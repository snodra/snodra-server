//! The events API over the event browser add-on (ADR 0027), against a fake browser
//! (`tests/common/browser.rs`): what the hub asks for each call, the neutral answers it passes
//! on, the payload rules and their audit lines (ADR 0025), the browser's errors as HTTP statuses,
//! the tail's server-sent events, and what happens without a browser or when it goes away.

mod common;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::browser::{
    self, Answer, FakeBrowser, GET, LAG, SEARCH, SOURCES, TAIL, defaults, page, refusal, summary,
};
use common::{FakeClient, http_send};
use kraken_hub::auth::Secret;
use kraken_hub::config::Config;
use kraken_hub::proto::{ErrorCode, RequestKind, server_message};
use serde_json::{Value, json};

const READ: &str = "a-dashboard-token-0123456789";
const WRITE: &str = "a-write-token-0123456789";

fn config() -> Config {
    Config {
        otlp_endpoint: None,
        ..Config::default()
    }
}

async fn hub(config: Config) -> (SocketAddr, SocketAddr) {
    let (grpc, http) = common::start_hub_with_http(config).await;
    (grpc, http.unwrap())
}

/// Opens a tail on `path`: the hub answers once the browser's `start` came, so the fake starts
/// it (`segments`) while the request waits.
async fn open_tail(
    http: SocketAddr,
    path: &str,
    fake: &mut FakeBrowser,
    segments: u64,
) -> (browser::Sse, browser::Tail) {
    let path = path.to_string();
    let sse = tokio::spawn(async move { browser::sse(http, &path, None).await });
    let mut tail = fake.next_tail().await;
    tail.start(segments).await;
    (sse.await.unwrap(), tail)
}

/// `GET path` (30 s at most) with `headers`: the response and its JSON body (`Null` without one).
async fn get_with(
    addr: SocketAddr,
    path: &str,
    token: Option<&str>,
    extra: &[(&str, &str)],
) -> (common::HttpResponse, Value) {
    let auth = token.map(|t| format!("Bearer {t}"));
    let mut headers: Vec<(&str, &str)> = extra.to_vec();
    if let Some(auth) = auth.as_deref() {
        headers.push(("Authorization", auth));
    }
    let r = http_send(addr, "GET", path, &headers, b"", Duration::from_secs(30)).await;
    let body = serde_json::from_str(&r.body).unwrap_or(Value::Null);
    (r, body)
}

async fn get(addr: SocketAddr, path: &str, token: Option<&str>) -> (u16, Value) {
    let (r, body) = get_with(addr, path, token, &[]).await;
    (r.status, body)
}

const ROUTES: [&str; 6] = [
    "/api/v1/events",
    "/api/v1/events/kraken.events/0/0",
    "/api/v1/events/sources",
    "/api/v1/events/lag",
    "/api/v1/events/tail",
    "/api/v1/events?q=x",
];

/// Waits (5 s at most) until the hub sees the browser (its Register landed).
async fn until_installed(http: SocketAddr) {
    until_installed_as(http, None).await;
}

async fn until_installed_as(http: SocketAddr, token: Option<&str>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (_, v) = get(http, "/api/v1/version", token).await;
        if v["eventBrowser"]["installed"] == true {
            return;
        }
        assert!(Instant::now() < deadline, "the browser never showed");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn events_api_404_without_browser() {
    let (grpc, http) = hub(config()).await;
    for path in ROUTES {
        let (status, body) = get(http, path, None).await;
        assert_eq!(
            (status, body),
            (404, json!({"error": "event browser not installed"})),
            "{path}"
        );
    }
    let (_, version) = get(http, "/api/v1/version", None).await;
    assert_eq!(
        version["eventBrowser"],
        json!({"installed": false, "payloads": false})
    );
    let (_, settings) = get(http, "/api/v1/settings", None).await;
    let names: Vec<&str> = settings["settings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"KRAKEN_HUB_EVENT_BROWSER_PAYLOADS"));
    assert!(names.contains(&"KRAKEN_HUB_EVENT_MAX_TAILS"));
    assert!(names.iter().all(|n| !n.contains("KAFKA")), "{names:?}");

    // A browser connects: the routes answer; it leaves: 404 again.
    let fake = FakeBrowser::start(grpc, defaults).await;
    until_installed(http).await;
    let (status, body) = get(http, "/api/v1/events/sources", None).await;
    assert_eq!(status, 200, "{body}");
    fake.disconnect();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, body) = get(http, "/api/v1/events", None).await;
        if status == 404 {
            assert_eq!(body["error"], "event browser not installed");
            break;
        }
        assert!(Instant::now() < deadline, "still {status}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn every_route_asks_its_query_and_passes_the_neutral_answer() {
    let (grpc, http) = hub(Config {
        dashboard_token: Some(Secret::new(READ)),
        ..config()
    })
    .await;
    let fake = FakeBrowser::start(grpc, |type_name, request| match type_name {
        SEARCH => {
            let mut hit = summary(
                "3",
                "42",
                1_730_000_000_000,
                "order-42",
                json!({"type": "orders.Placed", "id": "e-1", "x-private": "nope"}),
            );
            // A summary never carries a payload, whatever the browser sends.
            hit["payload"] = "secret".into();
            let mut p = page(vec![hit]);
            p["cursor"] = "eyJmIjoi".into();
            p["exhausted"] = false.into();
            p["stoppedBy"] = "limit".into();
            p["debug"] = "dropped".into();
            Answer::Json(p)
        }
        LAG => Answer::Json(
            json!({"source": request["source"].as_str().unwrap_or("kraken.events"),
            "subscriptions": [{"subscription": "orders.projector", "state": "Empty", "members": 0,
            "segments": [{"segment": "0", "committed": "4", "head": "10", "lag": 6},
                         {"segment": "1", "committed": null, "head": "3", "lag": null}],
            "lag": 6}], "truncated": false}),
        ),
        _ => defaults(type_name, request),
    })
    .await;
    until_installed_as(http, Some(READ)).await;

    let path = "/api/v1/events?source=kraken.events&from=2026-11-05T10:00:00Z&to=1730000000000\
&type=orders.*&key=order-42&service=service-b&correlationId=c-1&userId=u-1&eventId=&limit=20\
&cursor=abc";
    let (r, body) = get_with(http, path, Some(READ), &[("X-Correlation-Id", "corr-7")]).await;
    assert_eq!(r.status, 200, "{body}");
    assert_eq!(r.header("x-correlation-id"), Some("corr-7"));
    assert_eq!(
        body,
        json!({"source": "kraken.events", "results": [{"source": "kraken.events",
            "segment": "3", "position": "42", "timestamp": 1_730_000_000_000_i64,
            "key": "order-42", "headers": {"type": "orders.Placed", "id": "e-1"}, "size": 17}],
            "scanned": 2, "exhausted": false, "stoppedBy": "limit", "cursor": "eyJmIjoi"})
    );
    let seen = fake.last(SEARCH);
    let timeout = seen.request["timeoutMs"].as_u64().unwrap();
    assert!((1..30_000).contains(&timeout), "{timeout}");
    assert_eq!(
        seen.request,
        json!({"source": "kraken.events", "filters": {"from": "2026-11-05T10:00:00Z",
            "to": "1730000000000", "type": "orders.*", "key": "order-42",
            "source": "service-b", "correlationId": "c-1", "userId": "u-1"},
            "limit": 20, "cursor": "abc", "timeoutMs": timeout})
    );
    // Who asked and the correlation id travel with the query.
    assert_eq!(seen.headers["userId"], "dashboard-token");
    assert_eq!(seen.headers["correlationId"], "corr-7");
    assert!(!seen.headers.contains_key("authorization"));

    // Without a correlation id the hub makes one and answers it.
    let (r, _) = get_with(http, "/api/v1/events", Some(READ), &[]).await;
    let made = r.header("x-correlation-id").unwrap().to_string();
    assert_eq!(fake.last(SEARCH).headers["correlationId"], made);

    // Sources and lag, as the browser says them.
    let (status, sources) = get(http, "/api/v1/events/sources", Some(READ)).await;
    assert_eq!(status, 200);
    assert_eq!(sources["sources"][0]["segments"][0]["head"], "2");
    assert_eq!(fake.last(SOURCES).request, json!({}));
    let (status, lag) = get(
        http,
        "/api/v1/events/lag?source=kraken.events.dlt",
        Some(READ),
    )
    .await;
    assert_eq!(status, 200, "{lag}");
    assert_eq!(
        fake.last(LAG).request,
        json!({"source": "kraken.events.dlt"})
    );
    assert_eq!(lag["source"], "kraken.events.dlt");
    assert_eq!(
        lag["subscriptions"][0]["segments"][1]["committed"],
        Value::Null
    );
    assert_eq!(lag["subscriptions"][0]["lag"], 6);

    // What the hub can tell is wrong never reaches the browser.
    let asked = fake.seen().len();
    for (path, want) in [
        (
            "/api/v1/events?limit=ten",
            "limit must be a positive number",
        ),
        (
            "/api/v1/events?limit=1.5",
            "limit must be a positive number",
        ),
        (
            "/api/v1/events/tail?from=1",
            "a tail takes no from: it starts at the end",
        ),
    ] {
        let (status, body) = get(http, path, Some(READ)).await;
        assert_eq!(
            (status, body["error"].as_str()),
            (400, Some(want)),
            "{path}"
        );
    }
    let long = "x".repeat(1025);
    let (status, _) = get(http, &format!("/api/v1/events?type={long}"), Some(READ)).await;
    assert_eq!(status, 400);
    let (status, _) = get(
        http,
        &format!("/api/v1/events/kraken.events/{}/0", "1".repeat(129)),
        Some(READ),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(fake.seen().len(), asked);

    // The routes are dashboard reads: the token is needed.
    for path in ROUTES {
        let (status, _) = get(http, path, None).await;
        assert_eq!(status, 401, "{path}");
    }
}

#[tokio::test]
async fn payload_rules_stay_in_the_hub_and_are_audited() {
    common::browser::capture_logs();
    let secret = format!("s3cr3t-{}", uuid::Uuid::now_v7().simple());
    let record = |request: &Value, secret: &str| {
        let mut r = summary(
            "0",
            request["position"].as_str().unwrap(),
            1,
            "k\\xff",
            json!({"type": "a.Placed", "id": "e-1"}),
        );
        r["allHeaders"] = json!([{"name": "type", "value": "a.Placed"},
            {"name": "bin", "value": null, "valueHex": "00ff"}]);
        if request["payload"] == true {
            if request["position"] == "1" {
                r["payloadBase64"] = "//4=".into();
            } else {
                r["payload"] = format!("{{\"secret\":\"{secret}\"}}").into();
            }
        }
        r
    };
    let start = |grpc, secret: String| {
        FakeBrowser::start(grpc, move |type_name, request| match type_name {
            GET => Answer::Json(record(request, &secret)),
            _ => defaults(type_name, request),
        })
    };

    // The switch is off: hidden, whatever the token; the payload is not even asked.
    let (grpc, off) = hub(Config {
        dashboard_token: Some(Secret::new(READ)),
        ..config()
    })
    .await;
    let fake = start(grpc, secret.clone()).await;
    until_installed_as(off, Some(READ)).await;
    let (status, d) = get(off, "/api/v1/events/kraken.events/0/0", Some(READ)).await;
    assert_eq!(status, 200, "{d}");
    assert_eq!(fake.last(GET).request["payload"], false);
    assert_eq!(d["payload"]["hidden"], "payloads-off");
    assert_eq!(d["key"], "k\\xff");
    assert_eq!(d["allHeaders"][1]["valueHex"], "00ff");
    let asked = fake.seen().len();
    let (status, q) = get(off, &format!("/api/v1/events?q={secret}"), Some(READ)).await;
    assert_eq!((status, &q["hidden"]), (403, &Value::from("payloads-off")));
    assert_eq!(
        fake.seen().len(),
        asked,
        "a refused q never reaches the browser"
    );

    // On, but the hub has no token to present.
    let (grpc, open) = hub(Config {
        event_payloads: true,
        ..config()
    })
    .await;
    let _fake = start(grpc, secret.clone()).await;
    until_installed(open).await;
    let (_, d) = get(open, "/api/v1/events/kraken.events/0/0", None).await;
    assert_eq!(d["payload"]["hidden"], "no-token-configured");
    let (status, q) = get(open, &format!("/api/v1/events?q={secret}"), None).await;
    assert_eq!(
        (status, &q["hidden"]),
        (403, &Value::from("no-token-configured"))
    );

    // On, a write token configured (reads stay open), not presented; then presented.
    let (grpc, writer) = hub(Config {
        event_payloads: true,
        dashboard_write_token: Some(Secret::new(WRITE)),
        ..config()
    })
    .await;
    let fake = start(grpc, secret.clone()).await;
    until_installed(writer).await;
    let (_, d) = get(writer, "/api/v1/events/kraken.events/0/0", None).await;
    assert_eq!(d["payload"]["hidden"], "no-token-presented");
    assert!(!d.to_string().contains(&secret));
    let (_, d) = get(writer, "/api/v1/events/kraken.events/0/0", Some(WRITE)).await;
    assert_eq!(fake.last(GET).request["payload"], true);
    assert_eq!(d["payload"], format!("{{\"secret\":\"{secret}\"}}"));

    // On, both tokens: the dashboard token sees payloads and may search them.
    let (grpc, secure) = hub(Config {
        event_payloads: true,
        dashboard_token: Some(Secret::new(READ)),
        dashboard_write_token: Some(Secret::new(WRITE)),
        ..config()
    })
    .await;
    let fake = start(grpc, secret.clone()).await;
    until_installed_as(secure, Some(READ)).await;
    let (_, d) = get(secure, "/api/v1/events/kraken.events/0/0", Some(READ)).await;
    assert_eq!(d["payload"], format!("{{\"secret\":\"{secret}\"}}"));
    let (_, raw) = get(secure, "/api/v1/events/kraken.events/0/1", Some(READ)).await;
    assert_eq!(raw["payloadBase64"], "//4=");
    assert!(raw.get("payload").is_none(), "{raw}");
    let (status, _) = get(secure, &format!("/api/v1/events?q={secret}"), Some(READ)).await;
    assert_eq!(status, 200);
    assert_eq!(fake.last(SEARCH).request["filters"]["q"], secret.as_str());

    // One audit line per payload served (write token once, dashboard token twice), with the
    // caller and the record; one line for the payload search, without the text; never the
    // payload in any line.
    let lines = common::browser::logged();
    let audits: Vec<_> = lines
        .iter()
        .filter(|l| l.message == "event payload served")
        .collect();
    assert_eq!(audits.len(), 3, "{audits:#?}");
    assert_eq!(audits[0].field("caller"), Some("write-token"));
    assert_eq!(audits[1].field("caller"), Some("dashboard-token"));
    assert_eq!(
        (
            audits[1].field("source"),
            audits[1].field("segment"),
            audits[1].field("position"),
            audits[1].field("size"),
        ),
        (Some("kraken.events"), Some("0"), Some("0"), Some("17"))
    );
    assert!(audits[1].field("correlation_id").is_some());
    assert_eq!(audits[2].field("position"), Some("1"));
    let searches: Vec<_> = lines
        .iter()
        .filter(|l| l.message == "event payload search")
        .collect();
    assert_eq!(searches.len(), 1, "{searches:#?}");
    assert_eq!(searches[0].field("caller"), Some("dashboard-token"));
    assert!(
        lines.iter().all(|l| !l.text().contains(&secret)),
        "a payload or a searched text was logged"
    );
}

/// Even if a (buggy or hostile) browser sends payload fields the hub did not ask for, the hub
/// drops them: in a record, a search result and a tail record.
#[tokio::test]
async fn payload_dropped_when_not_asked() {
    let (grpc, http) = hub(config()).await;
    let hostile = |type_name: &str, request: &Value| {
        let leak = |mut v: Value| {
            v["payload"] = "SECRET".into();
            v["payloadBase64"] = "U0VDUkVU".into();
            v["payloadTruncated"] = true.into();
            v
        };
        match type_name {
            GET => {
                let mut r = summary("0", "0", 1, "k", json!({}));
                r["allHeaders"] = json!([]);
                Answer::Json(leak(r))
            }
            SEARCH => Answer::Json(page(vec![leak(summary("0", "0", 1, "k", json!({})))])),
            _ => defaults(type_name, request),
        }
    };
    let mut fake = FakeBrowser::start(grpc, hostile).await;
    until_installed(http).await;
    let (status, d) = get(http, "/api/v1/events/kraken.events/0/0", None).await;
    assert_eq!(status, 200, "{d}");
    assert_eq!(fake.last(GET).request["payload"], false);
    assert_eq!(d["payload"]["hidden"], "payloads-off");
    let (_, s) = get(http, "/api/v1/events", None).await;
    let (mut tail, mut t) = open_tail(http, "/api/v1/events/tail", &mut fake, 1).await;
    t.part(
        json!({"record": {"source": "kraken.events", "segment": "0", "position": "5",
        "headers": {}, "size": 6, "payload": "SECRET", "payloadBase64": "U0VDUkVU"}}),
    )
    .await;
    assert_eq!(tail.next(Duration::from_secs(5)).await.unwrap().0, "start");
    let (name, record) = tail.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(name, "record");
    for text in [d.to_string(), s.to_string(), record] {
        assert!(
            !text.contains("SECRET")
                && !text.contains("U0VDUkVU")
                && !text.contains("payloadTruncated"),
            "{text}"
        );
    }
}

#[tokio::test]
async fn browser_errors_map_to_http_statuses() {
    let (grpc, http) = hub(Config {
        timeout_overrides: [(GET.to_string(), Duration::from_millis(300))].into(),
        ..config()
    })
    .await;
    let _fake = FakeBrowser::start(grpc, |type_name, request| {
        let key = request["filters"]["key"].as_str().unwrap_or_default();
        match (type_name, key) {
            (SEARCH, "invalid") => refusal("invalid", "the cursor belongs to another search"),
            (SEARCH, "missing") => refusal("not-found", "no such source"),
            (SEARCH, "busy") => refusal("busy", "4 searches are running"),
            (SEARCH, "down") => refusal("unavailable", "the event store did not answer"),
            (SEARCH, "odd") => refusal("exploded", "stack trace here"),
            (SEARCH, "raw") => Answer::Error {
                code: ErrorCode::HandlerError,
                message: "internal: stack trace here".into(),
                details: b"not json".to_vec(),
            },
            (SEARCH, "undecodable") => Answer::Error {
                code: ErrorCode::InvalidRequest,
                message: "invalid type: string \"50\", expected u64".into(),
                details: vec![],
            },
            (SEARCH, "garbage") => Answer::Raw(b"{\"source\":".to_vec()),
            (SEARCH, "huge") => Answer::Raw(vec![b' '; 3 * 1024 * 1024]),
            (GET, _) => Answer::Silent,
            _ => defaults(type_name, request),
        }
    })
    .await;
    until_installed(http).await;
    for (key, status, message) in [
        ("invalid", 400, "the cursor belongs to another search"),
        ("missing", 404, "no such source"),
        ("busy", 429, "4 searches are running"),
        ("down", 503, "the event store did not answer"),
        ("odd", 502, "the event browser failed to answer"),
        ("raw", 502, "the event browser failed to answer"),
        (
            "undecodable",
            400,
            "the event browser could not read the request",
        ),
        ("garbage", 502, "the event browser failed to answer"),
        ("huge", 502, "the event browser failed to answer"),
    ] {
        let (r, body) = get_with(http, &format!("/api/v1/events?key={key}"), None, &[]).await;
        assert_eq!(
            (r.status, body["error"].as_str()),
            (status, Some(message)),
            "{key}"
        );
        assert!(!r.body.contains("stack trace"), "{key}: {}", r.body);
        if status == 429 {
            assert_eq!(r.header("retry-after"), Some("1"));
        }
    }
    let (status, body) = get(http, "/api/v1/events/kraken.events/0/0", None).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (504, Some("the event browser did not answer in time"))
    );
}

/// While a search waits for the browser, routing goes on as always.
#[tokio::test]
async fn routing_goes_on_while_a_search_waits() {
    let (grpc, http) = hub(Config {
        timeout_overrides: [(SEARCH.to_string(), Duration::from_secs(2))].into(),
        ..config()
    })
    .await;
    let _fake = FakeBrowser::start(grpc, |type_name, request| match type_name {
        SEARCH => Answer::Silent,
        _ => defaults(type_name, request),
    })
    .await;
    until_installed(http).await;
    let search = tokio::spawn(async move { get(http, "/api/v1/events", None).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut handler = FakeClient::register(grpc, "service-b", &[], &["b.Count"]).await;
    let mut sender = FakeClient::register(grpc, "service-a", &[], &[]).await;
    let routed = Instant::now();
    sender
        .tx
        .send(FakeClient::request(
            "r1",
            RequestKind::Query,
            "b.Count",
            "",
            0,
        ))
        .await
        .unwrap();
    loop {
        match handler.next().await {
            Some(Ok(m)) if matches!(m.message, Some(server_message::Message::Request(_))) => break,
            Some(Ok(_)) => continue,
            other => panic!("{other:?}"),
        }
    }
    handler
        .tx
        .send(FakeClient::reply_ok("r1", b"1"))
        .await
        .unwrap();
    loop {
        match sender.next().await {
            Some(Ok(m)) if matches!(m.message, Some(server_message::Message::Reply(_))) => break,
            Some(Ok(_)) => continue,
            other => panic!("{other:?}"),
        }
    }
    assert!(
        routed.elapsed() < Duration::from_secs(1),
        "{:?}",
        routed.elapsed()
    );
    assert!(!search.is_finished(), "the search still waits");
    let (status, _) = search.await.unwrap();
    assert_eq!(status, 504);
}

#[tokio::test]
async fn a_tail_streams_start_records_heartbeats_and_end() {
    let (grpc, http) = hub(config()).await;
    let mut fake = FakeBrowser::start(grpc, defaults).await;
    until_installed(http).await;
    // A payload filter without the payload rules is refused before any tail.
    let (status, body) = get(http, "/api/v1/events/tail?q=x", None).await;
    assert_eq!(
        (status, &body["hidden"]),
        (403, &Value::from("payloads-off"))
    );

    let (mut sse, mut tail) =
        open_tail(http, "/api/v1/events/tail?type=orders.*", &mut fake, 6).await;
    // The heartbeat is asked well below the internal stream's idle timeout (5 s), the duration
    // within the hub's longest stream (10 min).
    let heartbeat = tail.request["heartbeatMs"].as_u64().unwrap();
    assert!((100..5000).contains(&heartbeat), "{heartbeat}");
    assert!(tail.request["maxDurationMs"].as_u64().unwrap() < 600_000);
    assert_eq!(tail.request["filters"], json!({"type": "orders.*"}));
    assert!(tail.headers.contains_key("correlationId"));
    assert_eq!(sse.status, 200);
    let (name, data) = sse.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(name, "start");
    let start: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(
        start,
        json!({"source": "kraken.events", "segments": 6, "endsAtMs": 1_900_000_000_000_i64})
    );
    tail.part(json!({"record": summary("2", "9", 5, "k-1", json!({"type": "orders.Placed"}))}))
        .await;
    let (name, data) = sse.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(name, "record");
    let record: Value = serde_json::from_str(&data).unwrap();
    assert_eq!(
        (record["segment"].as_str(), record["position"].as_str()),
        (Some("2"), Some("9"))
    );
    tail.part(json!({"heartbeat": {"nowMs": 1}})).await;
    let (name, data) = sse.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(name, "heartbeat");
    let now: Value = serde_json::from_str(&data).unwrap();
    assert!(now["nowMs"].as_i64().unwrap() > 1, "the hub's clock: {now}");
    tail.part(json!({"end": {"reason": "maxDuration"}})).await;
    tail.close().await;
    let (name, data) = sse.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        (name.as_str(), data.as_str()),
        ("end", "{\"reason\":\"maxDuration\"}")
    );
    assert_eq!(
        sse.next(Duration::from_secs(5)).await,
        None,
        "nothing after the end"
    );

    // A reader that leaves cancels the browser's tail.
    let (sse, mut tail) = open_tail(http, "/api/v1/events/tail", &mut fake, 1).await;
    let id = tail.id.clone();
    drop(sse);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // Parts keep the hub writing, so it notices the reader is gone.
        tail.part(json!({"heartbeat": {"nowMs": 1}})).await;
        if fake.cancelled().contains(&id) {
            break;
        }
        assert!(Instant::now() < deadline, "the tail was not cancelled");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A refused tail is refused before any event.
    let (grpc, http) = hub(config()).await;
    let _busy = FakeBrowser::start(grpc, |type_name, request| match type_name {
        TAIL => refusal("busy", "4 tails are open"),
        _ => defaults(type_name, request),
    })
    .await;
    until_installed(http).await;
    let (r, body) = get_with(http, "/api/v1/events/tail", None, &[]).await;
    assert_eq!(
        (r.status, body["error"].as_str()),
        (429, Some("4 tails are open"))
    );
    assert_eq!(r.header("retry-after"), Some("1"));
}

#[tokio::test]
async fn tail_ends_when_browser_disconnects() {
    let (grpc, http) = hub(config()).await;
    let mut fake = FakeBrowser::start(grpc, defaults).await;
    until_installed(http).await;
    let (mut sse, tail) = open_tail(http, "/api/v1/events/tail", &mut fake, 2).await;
    assert_eq!(sse.next(Duration::from_secs(5)).await.unwrap().0, "start");
    drop(tail);
    fake.disconnect();
    let (name, data) = sse.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        (name.as_str(), data.as_str()),
        ("end", "{\"reason\":\"failed\"}")
    );
    assert_eq!(sse.next(Duration::from_secs(5)).await, None);
    let (status, body) = get(http, "/api/v1/events/tail", None).await;
    assert_eq!(
        (status, body["error"].as_str()),
        (404, Some("event browser not installed"))
    );
}

/// A tail part the hub cannot read ends the tail as failed and cancels the browser's stream.
#[tokio::test]
async fn a_malformed_tail_part_ends_the_tail() {
    let (grpc, http) = hub(config()).await;
    let mut fake = FakeBrowser::start(grpc, defaults).await;
    until_installed(http).await;
    let (mut sse, mut tail) = open_tail(http, "/api/v1/events/tail", &mut fake, 1).await;
    assert_eq!(sse.next(Duration::from_secs(5)).await.unwrap().0, "start");
    tail.raw(b"{\"record\": 42}").await;
    let (name, data) = sse.next(Duration::from_secs(5)).await.unwrap();
    assert_eq!(
        (name.as_str(), data.as_str()),
        ("end", "{\"reason\":\"failed\"}")
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !fake.cancelled().contains(&tail.id) {
        assert!(Instant::now() < deadline, "not cancelled");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // A first part that is not `start` is a failed call (502), not a stream.
    let sse_task = tokio::spawn(async move { get(http, "/api/v1/events/tail", None).await });
    let mut tail = fake.next_tail().await;
    tail.part(json!({"heartbeat": {"nowMs": 1}})).await;
    let (status, _) = sse_task.await.unwrap();
    assert_eq!(status, 502);
}

#[tokio::test]
async fn the_hub_bounds_its_open_tails() {
    let (grpc, http) = hub(Config {
        event_max_tails: 1,
        ..config()
    })
    .await;
    let mut fake = FakeBrowser::start(grpc, defaults).await;
    until_installed(http).await;
    let (_first, _tail) = open_tail(http, "/api/v1/events/tail", &mut fake, 1).await;
    let (r, body) = get_with(http, "/api/v1/events/tail", None, &[]).await;
    assert_eq!(r.status, 429, "{body}");
    assert_eq!(r.header("retry-after"), Some("1"));
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("KRAKEN_HUB_EVENT_MAX_TAILS")
    );
    assert_eq!(
        fake.seen().iter().filter(|s| s.type_name == TAIL).count(),
        1
    );
}
