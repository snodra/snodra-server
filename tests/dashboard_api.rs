//! Integration tests for the dashboard's JSON API (`/api/v1/`) against an in-process hub with
//! two fake services and some traffic.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, http_get, http_get_full};
use serde_json::Value;
use snodra_server::config::Config;
use snodra_server::proto::{ErrorCode, RequestKind, ServerMessage, server_message};
use snodra_server::topology::Topology;

const BOUND: Duration = Duration::from_secs(3);

fn config() -> Config {
    Config {
        otlp_endpoint: None,
        grafana_url: Some("http://grafana.local:3000".into()),
        ..Config::default()
    }
}

async fn start(config: Config) -> (SocketAddr, SocketAddr) {
    let (grpc, http) = common::start_hub_with_http(config).await;
    (grpc, http.expect("http listener"))
}

async fn json(http: SocketAddr, path: &str) -> Value {
    let r = http_get_full(http, path).await;
    assert_eq!(r.status, 200, "{path}: {}", r.body);
    assert_eq!(r.header("content-type"), Some("application/json"), "{path}");
    assert_eq!(r.header("cache-control"), Some("no-store"), "{path}");
    assert!(
        r.header("content-security-policy")
            .is_some_and(|csp| csp.contains("default-src 'self'")),
        "{path}: no CSP"
    );
    serde_json::from_str(&r.body).unwrap_or_else(|e| panic!("{path}: {e}: {}", r.body))
}

/// Polls `GET /api/v1/topology` until `done` holds (bounded).
async fn topology_until(http: SocketAddr, done: impl Fn(&Topology) -> bool) -> Topology {
    let deadline = tokio::time::Instant::now() + BOUND;
    loop {
        let (status, body) = http_get(http, "/api/v1/topology").await;
        assert_eq!(status, 200);
        let t: Topology = serde_json::from_str(&body).unwrap();
        if done(&t) {
            return t;
        }
        assert!(tokio::time::Instant::now() < deadline, "not reached: {t:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The next message that is neither a `RegisterAck` nor a heartbeat echo.
async fn next_message(client: &mut FakeClient) -> server_message::Message {
    loop {
        match client.next().await {
            Some(Ok(ServerMessage {
                message: Some(server_message::Message::RegisterAck(_)),
            }))
            | Some(Ok(ServerMessage {
                message: Some(server_message::Message::Heartbeat(_)),
            })) => continue,
            Some(Ok(ServerMessage { message: Some(m) })) => return m,
            other => panic!("unexpected: {other:?}"),
        }
    }
}

/// One request from `a` answered by `b` (ok or with an error).
async fn call(
    a: &mut FakeClient,
    b: &mut FakeClient,
    id: &str,
    kind: RequestKind,
    type_name: &str,
    fail: bool,
) {
    a.tx.send(FakeClient::request(id, kind, type_name, "", 0))
        .await
        .unwrap();
    let server_message::Message::Request(req) = next_message(b).await else {
        panic!("handler expected a request");
    };
    assert_eq!(req.id, id);
    let reply = if fail {
        FakeClient::reply_err(id, ErrorCode::HandlerError, "boom")
    } else {
        FakeClient::reply_ok(id, b"3")
    };
    b.tx.send(reply).await.unwrap();
    let answer = next_message(a).await;
    assert!(
        matches!(answer, server_message::Message::Reply(ref r) if r.request_id == id),
        "{answer:?}"
    );
}

/// service-a (sender only) and service-b (owns examples.CountGreetings and examples.Greet),
/// three counting calls, one failing greet and one call to a type nobody owns.
async fn with_traffic(http: SocketAddr, grpc: SocketAddr) -> (FakeClient, FakeClient) {
    let mut b = FakeClient::register(
        grpc,
        "service-b",
        &["examples.Greet"],
        &["examples.CountGreetings"],
    )
    .await;
    let mut a = FakeClient::register(grpc, "service-a", &[], &[]).await;
    topology_until(http, |t| t.totals.instances == 2 && t.types.len() == 2).await;
    for i in 0..3 {
        call(
            &mut a,
            &mut b,
            &format!("count-{i}"),
            RequestKind::Query,
            "examples.CountGreetings",
            false,
        )
        .await;
    }
    call(
        &mut a,
        &mut b,
        "greet-1",
        RequestKind::Command,
        "examples.Greet",
        true,
    )
    .await;
    a.tx.send(FakeClient::request(
        "nope-1",
        RequestKind::Command,
        "examples.Nope",
        "",
        0,
    ))
    .await
    .unwrap();
    let server_message::Message::Error(e) = next_message(&mut a).await else {
        panic!("expected NO_HANDLER");
    };
    assert_eq!(e.code(), ErrorCode::NoHandler);
    (a, b)
}

fn find<'a>(items: &'a Value, field: &str, value: &str) -> &'a Value {
    items
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v[field] == value)
        .unwrap_or_else(|| panic!("no {field}={value} in {items}"))
}

#[tokio::test]
async fn graph_shows_the_edge_with_counts_errors_and_latency() {
    let (grpc, http) = start(config()).await;
    let _clients = with_traffic(http, grpc).await;
    let g = json(http, "/api/v1/graph").await;

    assert_eq!(g["windowMs"], 300_000);
    assert_eq!(g["maxEdges"], 10_000);
    assert_eq!(g["evictedEdges"], 0);
    let totals = &g["totals"];
    assert_eq!(totals["services"], 2);
    assert_eq!(totals["instances"], 2);
    assert_eq!(totals["types"], 2);
    assert_eq!(totals["requests"], 5);
    assert_eq!(totals["errors"], 2);

    let a = find(&g["nodes"], "id", "service:service-a");
    assert_eq!(
        (a["kind"].as_str(), a["instances"].as_u64()),
        (Some("service"), Some(1))
    );
    let count = find(&g["nodes"], "id", "type:examples.CountGreetings");
    assert_eq!(count["kind"], "type");
    assert_eq!(count["typeKind"], "QUERY");
    assert_eq!(count["owners"], serde_json::json!(["service-b"]));
    let nope = find(&g["nodes"], "id", "type:examples.Nope");
    assert_eq!(nope["registered"], false);

    let edges = g["edges"].as_array().unwrap();
    let edge = |from: &str, to: &str| {
        edges
            .iter()
            .find(|e| e["from"] == from && e["to"] == to)
            .unwrap_or_else(|| panic!("no edge {from} -> {to}: {edges:?}"))
    };
    let send = edge("service:service-a", "type:examples.CountGreetings");
    assert_eq!(send["role"], "send");
    assert_eq!(
        (send["count"].as_u64(), send["errors"].as_u64()),
        (Some(3), Some(0))
    );
    assert!(send["p50Ms"].as_f64().unwrap() >= 0.0);
    assert!(send["p99Ms"].as_f64().unwrap() >= send["p50Ms"].as_f64().unwrap());
    let handle = edge("type:examples.CountGreetings", "service:service-b");
    assert_eq!(handle["role"], "handle");
    assert_eq!(handle["count"], 3);
    let greet = edge("service:service-a", "type:examples.Greet");
    assert_eq!(
        (greet["count"].as_u64(), greet["errors"].as_u64()),
        (Some(1), Some(1))
    );
    let nope = edge("service:service-a", "type:examples.Nope");
    assert_eq!(
        (nope["count"].as_u64(), nope["errors"].as_u64()),
        (Some(1), Some(1))
    );
    assert!(
        nope["p50Ms"].is_null(),
        "a NO_HANDLER answer has no latency"
    );
}

#[tokio::test]
async fn types_topology_settings_and_version_have_their_shape() {
    let (grpc, http) = start(config()).await;
    let (a, _b) = with_traffic(http, grpc).await;

    // topology: the snapshot the gRPC `Topology` returns.
    let t = json(http, "/api/v1/topology").await;
    for field in [
        "hubId",
        "hubStartedAtMs",
        "nowMs",
        "services",
        "types",
        "totals",
    ] {
        assert!(t.get(field).is_some(), "topology.{field} missing");
    }
    let _: Topology = serde_json::from_value(t).unwrap();

    // types: kind, owners, instances, orphaned, senders and traffic per type.
    let v = json(http, "/api/v1/types").await;
    assert_eq!(v["windowMs"], 300_000);
    let count = find(&v["types"], "type", "examples.CountGreetings");
    assert_eq!(count["kind"], "QUERY");
    assert_eq!(count["owners"], serde_json::json!(["service-b"]));
    assert_eq!(count["registered"], true);
    assert_eq!(count["liveInstances"], 1);
    assert_eq!(count["instances"].as_array().unwrap().len(), 1);
    assert_eq!(count["orphaned"], false);
    assert_eq!(count["senders"], serde_json::json!(["service-a"]));
    assert_eq!(count["traffic"]["count"], 3);
    let nope = find(&v["types"], "type", "examples.Nope");
    assert_eq!(nope["registered"], false);
    assert_eq!(nope["owners"], serde_json::json!([]));

    // settings: every variable, effective value and default, no secret values.
    let s = json(http, "/api/v1/settings").await;
    let grafana = find(&s["settings"], "name", "KRAKEN_HUB_GRAFANA_URL");
    assert_eq!(grafana["value"], "http://grafana.local:3000");
    assert_eq!(grafana["default"], "");
    let window = find(&s["settings"], "name", "KRAKEN_HUB_EDGE_STATS_WINDOW");
    assert_eq!(window["value"], "5m");
    for entry in s["settings"].as_array().unwrap() {
        let secret = entry["name"].as_str().unwrap().ends_with("_TOKEN");
        assert_eq!(entry["secret"], secret, "{entry}");
        if secret {
            assert!(entry["value"].is_null(), "{entry}");
        }
        assert!(entry["description"].as_str().is_some_and(|d| !d.is_empty()));
    }

    // version: hub id, version, start time, build, Grafana link.
    let v = json(http, "/api/v1/version").await;
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    assert!(v["hubId"].as_str().is_some_and(|id| !id.is_empty()));
    assert!(v["startedAtMs"].as_i64().unwrap() <= v["nowMs"].as_i64().unwrap());
    assert!(v["buildId"].as_str().is_some_and(|b| !b.is_empty()));
    assert_eq!(v["grafanaUrl"], "http://grafana.local:3000");
    drop(a);
}

/// Reads server-sent events from `/api/v1/stream`.
struct EventStream {
    stream: tokio::net::TcpStream,
    buf: String,
}

impl EventStream {
    async fn open(http: SocketAddr) -> Self {
        use tokio::io::AsyncWriteExt;
        let mut stream = tokio::net::TcpStream::connect(http).await.unwrap();
        let request = format!("GET /api/v1/stream HTTP/1.1\r\nHost: {http}\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        Self {
            stream,
            buf: String::new(),
        }
    }

    async fn head(&mut self) -> String {
        loop {
            if let Some((head, rest)) = self.buf.split_once("\r\n\r\n") {
                let head = head.to_string();
                self.buf = rest.to_string();
                return head;
            }
            self.fill().await;
        }
    }

    async fn fill(&mut self) {
        use tokio::io::AsyncReadExt;
        let mut chunk = [0u8; 4096];
        let n = tokio::time::timeout(BOUND, self.stream.read(&mut chunk))
            .await
            .expect("no event in time")
            .unwrap();
        assert!(n > 0, "stream closed");
        self.buf.push_str(&String::from_utf8_lossy(&chunk[..n]));
    }

    /// The data of the next `summary` event.
    async fn summary(&mut self) -> Value {
        loop {
            if let Some(start) = self.buf.find("event: summary\ndata: ") {
                let data = &self.buf[start + "event: summary\ndata: ".len()..];
                if let Some(end) = data.find('\n') {
                    let value = serde_json::from_str(&data[..end]).unwrap();
                    self.buf = data[end..].to_string();
                    return value;
                }
            }
            self.fill().await;
        }
    }
}

#[tokio::test]
async fn the_stream_pushes_summaries_and_a_new_revision_on_change() {
    let (grpc, http) = start(config()).await;
    let _b = FakeClient::register(grpc, "service-b", &[], &["examples.CountGreetings"]).await;
    topology_until(http, |t| t.totals.instances == 1).await;

    let mut events = EventStream::open(http).await;
    let head = events.head().await.to_ascii_lowercase();
    assert!(head.starts_with("http/1.1 200"), "{head}");
    assert!(head.contains("content-type: text/event-stream"), "{head}");
    assert!(head.contains("content-security-policy:"), "{head}");

    let first = events.summary().await;
    for field in [
        "hubId",
        "buildId",
        "nowMs",
        "totals",
        "staleInstances",
        "requests",
        "errors",
        "revision",
    ] {
        assert!(
            first.get(field).is_some(),
            "summary.{field} missing: {first}"
        );
    }
    assert_eq!(first["totals"]["instances"], 1);

    // A new instance changes the revision, and the change is pushed before the 2 s refresh.
    let started = tokio::time::Instant::now();
    let _a = FakeClient::register(grpc, "service-a", &[], &[]).await;
    loop {
        let next = events.summary().await;
        if next["revision"] != first["revision"] {
            assert_eq!(next["totals"]["instances"], 2);
            break;
        }
    }
    assert!(
        started.elapsed() < Duration::from_millis(1800),
        "{:?}",
        started.elapsed()
    );

    // Without changes a summary still arrives about every 2 s.
    let quiet = tokio::time::Instant::now();
    events.summary().await;
    assert!(
        quiet.elapsed() < Duration::from_millis(2600),
        "{:?}",
        quiet.elapsed()
    );
}

#[tokio::test]
async fn a_disabled_dashboard_is_404() {
    let (grpc, http) = start(Config {
        dashboard_enabled: false,
        ..config()
    })
    .await;
    let _b = FakeClient::register(grpc, "service-b", &[], &["examples.CountGreetings"]).await;
    for path in [
        "/api/v1/graph",
        "/api/v1/types",
        "/api/v1/topology",
        "/api/v1/settings",
        "/api/v1/version",
        "/api/v1/stream",
        "/api/v1/processors",
    ] {
        assert_eq!(http_get(http, path).await.0, 404, "{path}");
    }
}

// ---- assets ------------------------------------------------------------------------------

const CSP_PARTS: &[&str] = &[
    "default-src 'self'",
    "script-src 'self'",
    "style-src 'self'",
    "frame-ancestors 'none'",
    "object-src 'none'",
];

fn assert_strict_csp(r: &common::HttpResponse, what: &str) {
    let csp = r
        .header("content-security-policy")
        .unwrap_or_else(|| panic!("{what}: no CSP"));
    for part in CSP_PARTS {
        assert!(csp.contains(part), "{what}: CSP lacks {part}: {csp}");
    }
    assert!(
        !csp.contains("unsafe-inline") && !csp.contains("unsafe-eval"),
        "{csp}"
    );
    assert_eq!(
        r.header("x-content-type-options"),
        Some("nosniff"),
        "{what}"
    );
}

#[tokio::test]
async fn ui_redirects_to_the_page_and_the_page_is_not_cached() {
    let (_grpc, http) = start(config()).await;
    for path in ["/ui", "/"] {
        let r = http_get_full(http, path).await;
        assert_eq!(r.status, 307, "{path}");
        assert_eq!(r.header("location"), Some("ui/"), "{path}");
    }
    let page = http_get_full(http, "/ui/").await;
    assert_eq!(page.status, 200);
    assert_eq!(
        page.header("content-type"),
        Some("text/html; charset=utf-8")
    );
    assert_eq!(page.header("cache-control"), Some("no-cache"));
    assert_eq!(page.header("x-frame-options"), Some("DENY"));
    assert_strict_csp(&page, "/ui/");

    // Assets are referenced under this build's prefix; no inline script, style or handlers.
    let build = json(http, "/api/v1/version").await["buildId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(build, snodra_server::assets::build_id());
    assert!(
        page.body
            .contains(&format!(r#"<script type="module" src="v/{build}/app.js">"#)),
        "{}",
        page.body
    );
    assert!(page.body.contains(&format!(r#"href="v/{build}/app.css""#)));
    assert!(!page.body.contains("{{build}}"));
    assert!(!page.body.contains("<style"), "inline style");
    assert!(!page.body.contains(" style="), "inline style attribute");
    assert!(
        page.body.matches("<script").count()
            == page.body.matches("<script type=\"module\" src=").count(),
        "inline script in the page"
    );
    assert!(!page.body.contains(" onclick=") && !page.body.contains(" onload="));
}

#[tokio::test]
async fn assets_have_their_content_types_and_are_immutable_per_build() {
    let (_grpc, http) = start(config()).await;
    let build = snodra_server::assets::build_id();
    for (path, content_type) in [
        ("app.js", "text/javascript; charset=utf-8"),
        ("pages/overview.js", "text/javascript; charset=utf-8"),
        ("app.css", "text/css; charset=utf-8"),
        ("favicon.svg", "image/svg+xml"),
    ] {
        let r = http_get_full(http, &format!("/ui/v/{build}/{path}")).await;
        assert_eq!(r.status, 200, "{path}");
        assert_eq!(r.header("content-type"), Some(content_type), "{path}");
        assert_eq!(
            r.header("cache-control"),
            Some("public, max-age=31536000, immutable"),
            "{path}"
        );
        assert_strict_csp(&r, path);
        assert!(!r.body.is_empty(), "{path}");
    }
    // Another build's URL still answers (a stale tab), but is not cached.
    let stale = http_get_full(http, "/ui/v/000000000000/app.js").await;
    assert_eq!(stale.status, 200);
    assert_eq!(stale.header("cache-control"), Some("no-cache"));
    // The page only at /ui/; unknown assets are 404.
    for path in [
        format!("/ui/v/{build}/index.html"),
        format!("/ui/v/{build}/nope.js"),
        format!("/ui/v/{build}/../Cargo.toml"),
        "/ui/app.js".to_string(),
    ] {
        assert_eq!(http_get(http, &path).await.0, 404, "{path}");
    }
}

#[tokio::test]
async fn a_disabled_dashboard_serves_no_page() {
    let (_grpc, http) = start(Config {
        dashboard_enabled: false,
        ..config()
    })
    .await;
    let build = snodra_server::assets::build_id();
    for path in ["/", "/ui", "/ui/", &format!("/ui/v/{build}/app.js")] {
        assert_eq!(http_get(http, path).await.0, 404, "{path}");
    }
}
