//! The dashboard page as a browser loads it: every asset `index.html` references is served,
//! every `import` in every module resolves to a served module, no module is unreachable, and
//! no module uses HTML-string sinks or third-party code. When a Chrome or Chromium binary is
//! available, each page is also loaded headless and must render without a console message
//! (`KRAKEN_HUB_TEST_BROWSER` points at a binary, or `none` to skip).

mod common;

use std::collections::{BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use common::{FakeClient, http_get, http_get_full};
use kraken_hub::config::Config;
use kraken_hub::proto::{RequestKind, ServerMessage, server_message};

/// The fake instances never send a heartbeat: a long timeout keeps them registered however long
/// the headless loads take on a busy machine (ten pages load at once).
fn config() -> Config {
    Config {
        otlp_endpoint: None,
        heartbeat_timeout: Duration::from_secs(300),
        ..Config::default()
    }
}

/// `rel` against the directory `base` (which ends with `/`), with `.` and `..` segments.
fn resolve(base: &str, rel: &str) -> String {
    let joined = if rel.starts_with('/') {
        rel.to_string()
    } else {
        format!("{base}{rel}")
    };
    let mut out: Vec<&str> = Vec::new();
    for seg in joined.split('/') {
        match seg {
            "." => {}
            ".." => {
                out.pop();
            }
            s => out.push(s),
        }
    }
    let path = out.join("/");
    if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    }
}

fn dir_of(path: &str) -> &str {
    &path[..=path.rfind('/').unwrap()]
}

/// Values of `attr="..."` attributes in `html`.
fn attribute_values<'a>(html: &'a str, attr: &str) -> Vec<&'a str> {
    let needle = format!(" {attr}=\"");
    html.match_indices(&needle)
        .map(|(i, _)| {
            let rest = &html[i + needle.len()..];
            &rest[..rest.find('"').unwrap()]
        })
        .collect()
}

/// Module specifiers of static and dynamic imports and re-exports in `source`.
fn import_specifiers(source: &str) -> Vec<String> {
    let mut out = Vec::new();
    for marker in [
        "from '",
        "from \"",
        "import '",
        "import \"",
        "import('",
        "import(\"",
    ] {
        let quote = marker.chars().last().unwrap();
        for (i, _) in source.match_indices(marker) {
            let rest = &source[i + marker.len()..];
            out.push(rest[..rest.find(quote).unwrap()].to_string());
        }
    }
    out
}

#[tokio::test]
async fn every_referenced_asset_and_import_is_served() {
    let (_grpc, http) = common::start_hub_with_http(config()).await;
    let http = http.unwrap();
    let page = http_get_full(http, "/ui/").await;
    assert_eq!(page.status, 200);

    let mut references: Vec<String> = attribute_values(&page.body, "src")
        .into_iter()
        .chain(attribute_values(&page.body, "href"))
        .filter(|v| !v.starts_with('#'))
        .map(|v| {
            assert!(
                !v.contains("://") && !v.starts_with("//"),
                "page loads {v} from elsewhere"
            );
            resolve("/ui/", v)
        })
        .collect();
    references.sort();
    references.dedup();
    assert!(
        references.iter().any(|r| r.ends_with("/app.js")),
        "{references:?}"
    );

    let mut modules: VecDeque<String> = VecDeque::new();
    for path in &references {
        let r = http_get_full(http, path).await;
        assert_eq!(r.status, 200, "{path}");
        let content_type = r.header("content-type").unwrap_or_default().to_string();
        if path.ends_with(".js") {
            assert_eq!(content_type, "text/javascript; charset=utf-8", "{path}");
            modules.push_back(path.clone());
        } else if path.ends_with(".css") {
            assert_eq!(content_type, "text/css; charset=utf-8", "{path}");
            assert!(!r.body.contains("@import"), "{path} imports a stylesheet");
            for (i, _) in r.body.match_indices("url(") {
                let rest = &r.body[i + 4..];
                let url = rest[..rest.find(')').unwrap()].trim_matches(['"', '\'']);
                assert!(!url.contains("://"), "{path} loads {url} from elsewhere");
                let target = resolve(dir_of(path), url);
                assert_eq!(http_get(http, &target).await.0, 200, "{target}");
            }
        }
    }

    // Walk the module graph from the entry point.
    let mut reached = BTreeSet::new();
    while let Some(path) = modules.pop_front() {
        if !reached.insert(path.clone()) {
            continue;
        }
        let r = http_get_full(http, &path).await;
        assert_eq!(r.status, 200, "{path}");
        assert_eq!(
            r.header("content-type"),
            Some("text/javascript; charset=utf-8"),
            "{path}"
        );
        for sink in [
            "innerHTML",
            "outerHTML",
            "insertAdjacentHTML",
            "document.write",
            "eval(",
            "new Function",
        ] {
            assert!(!r.body.contains(sink), "{path} uses {sink}");
        }
        for spec in import_specifiers(&r.body) {
            assert!(
                spec.starts_with("./") || spec.starts_with("../"),
                "{path} imports {spec:?}: only relative imports of the hub's own modules"
            );
            modules.push_back(resolve(dir_of(&path), &spec));
        }
    }

    // Every embedded module is reachable (no dead or forgotten file), and only those exist.
    let build = kraken_hub::assets::build_id();
    let prefix = format!("/ui/v/{build}/");
    let reached: BTreeSet<String> = reached
        .iter()
        .map(|p| p.strip_prefix(&prefix).unwrap_or(p).to_string())
        .collect();
    let embedded: BTreeSet<String> = kraken_hub::assets::names()
        .into_iter()
        .filter(|n| n.ends_with(".js"))
        .collect();
    assert_eq!(reached, embedded);
}

// ---- headless browser --------------------------------------------------------------------

fn browser() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("KRAKEN_HUB_TEST_BROWSER") {
        return (!path.is_empty() && path != "none").then(|| path.into());
    }
    let mac = Path::new("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome");
    if mac.exists() {
        return Some(mac.into());
    }
    let paths = std::env::var_os("PATH")?;
    for name in [
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
    ] {
        for dir in std::env::split_paths(&paths) {
            let candidate = dir.join(name);
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

/// The DOM after load and the console lines of one headless load of `url`.
async fn load(browser: &Path, url: &str, profile: &Path) -> (String, Vec<String>) {
    load_with(browser, url, profile, &[]).await
}

/// [`load`] with extra browser arguments.
async fn load_with(
    browser: &Path,
    url: &str,
    profile: &Path,
    extra: &[&str],
) -> (String, Vec<String>) {
    let run = tokio::process::Command::new(browser)
        .args([
            "--headless=new",
            "--disable-gpu",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-extensions",
            "--enable-logging=stderr",
            "--v=0",
            "--dump-dom",
        ])
        .args(extra)
        .arg(format!("--user-data-dir={}", profile.display()))
        .arg(url)
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("browser timed out")
        .expect("browser did not start");
    let console = String::from_utf8_lossy(&output.stderr)
        .lines()
        .filter(|l| l.contains(":CONSOLE"))
        .map(str::to_string)
        .collect();
    (
        String::from_utf8_lossy(&output.stdout).into_owned(),
        console,
    )
}

/// The next message that is neither an ack nor a heartbeat echo.
async fn next_message(client: &mut FakeClient) -> server_message::Message {
    loop {
        match client.next().await {
            Some(Ok(ServerMessage {
                message: Some(server_message::Message::RegisterAck(_)),
            })) => continue,
            Some(Ok(ServerMessage { message: Some(m) })) => return m,
            other => panic!("unexpected: {other:?}"),
        }
    }
}

#[tokio::test]
async fn every_page_renders_in_a_headless_browser_without_console_messages() {
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped (static checks still run)");
        return;
    };
    let (grpc, http) = common::start_hub_with_http(config()).await;
    let http: SocketAddr = http.unwrap();
    let mut b = FakeClient::register(grpc, "service-b", &[], &["examples.CountGreetings"]).await;
    let mut a = FakeClient::register(grpc, "service-a", &[], &[]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    a.tx.send(FakeClient::request(
        "r1",
        RequestKind::Query,
        "examples.CountGreetings",
        "",
        0,
    ))
    .await
    .unwrap();
    let server_message::Message::Request(_) = next_message(&mut b).await else {
        panic!("expected the request");
    };
    b.tx.send(FakeClient::reply_ok("r1", b"1")).await.unwrap();
    assert!(matches!(
        next_message(&mut a).await,
        server_message::Message::Reply(_)
    ));

    let profiles = std::env::temp_dir().join(format!("kraken-hub-ui-test-{}", std::process::id()));
    let pages = [
        ("overview", "aria-label=\"Traffic graph"),
        ("services", "Instances of service-b"),
        ("types", "examples.CountGreetings"),
        ("settings", "KRAKEN_HUB_EDGE_STATS_WINDOW"),
        ("processors", "No service reports event processors"),
        ("dead-letters", "No processor keeps dead letters"),
    ];
    let mut checks = Vec::new();
    for (page, marker) in pages {
        let browser = browser.clone();
        let profile = profiles.join(page);
        checks.push(tokio::spawn(async move {
            let url = format!("http://{http}/ui/#/{page}");
            // --dump-dom prints the DOM after the load event, which may come before the page's
            // first fetch has answered: retry until the page says it rendered.
            for _ in 0..8 {
                let (dom, console) = load(&browser, &url, &profile).await;
                assert!(console.is_empty(), "{page}: console messages: {console:#?}");
                if dom.contains("data-ready=\"true\"") {
                    assert!(dom.contains(marker), "{page}: {marker} missing in {dom}");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            panic!("{page} never rendered");
        }));
    }
    for check in checks {
        check.await.unwrap();
    }
    let _ = std::fs::remove_dir_all(&profiles);
    drop((a, b));
}

/// A shared query type (ADR 0017): the Types page lists every owner with a "shared" badge and
/// the Overview graph draws an owner edge to each.
#[tokio::test]
async fn a_shared_query_type_shows_every_owner_and_a_shared_badge() {
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let (grpc, http) = common::start_hub_with_http(config()).await;
    let http: SocketAddr = http.unwrap();
    let mut owners = Vec::new();
    for service in ["service-a", "service-b"] {
        let client = FakeClient::connect(grpc).await;
        client
            .send_raw_register(kraken_hub::proto::Register {
                shared_query_types: vec!["search.FindAnything".to_string()],
                ..client.register_message(service, &[], &[])
            })
            .await;
        owners.push(client);
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let profiles =
        std::env::temp_dir().join(format!("kraken-hub-ui-shared-{}", std::process::id()));
    // Without a live stream the page keeps no connection open, so the virtual-time load ends.
    let _slots = take_stream_slots(http).await;
    let (dom, _) = rendered(&browser, http, "types", &profiles.join("types")).await;
    assert!(
        dom.contains("service-a, service-b"),
        "owners missing in {dom}"
    );
    assert!(
        dom.contains(">shared</span>"),
        "shared badge missing in {dom}"
    );
    let (dom, _) = rendered(&browser, http, "overview", &profiles.join("overview")).await;
    for owner in ["service-a", "service-b"] {
        assert!(
            dom.contains(&format!(
                "data-from=\"M:search.FindAnything\" data-to=\"R:{owner}\""
            )),
            "no owner edge to {owner} in {dom}"
        );
    }
    let _ = std::fs::remove_dir_all(&profiles);
    drop(owners);
}

/// Opens every `/api/v1/stream` slot (each answered `200`), so the next stream gets `503`.
async fn take_stream_slots(http: SocketAddr) -> Vec<tokio::net::TcpStream> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut open = Vec::new();
    for _ in 0..kraken_hub::http::MAX_STREAMS {
        let mut stream = tokio::net::TcpStream::connect(http).await.unwrap();
        let request = format!("GET /api/v1/stream HTTP/1.1\r\nHost: {http}\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut head = [0u8; 12];
        stream.read_exact(&mut head).await.unwrap();
        assert_eq!(&head, b"HTTP/1.1 200");
        open.push(stream);
    }
    let (status, _) = http_get(http, "/api/v1/stream").await;
    assert_eq!(status, 503);
    open
}

/// Loads `page` until it says it rendered; returns the DOM and how long the rendered load took.
async fn rendered(
    browser: &Path,
    http: SocketAddr,
    page: &str,
    profile: &Path,
) -> (String, Duration) {
    rendered_allowing(browser, http, page, profile, |_| false).await
}

/// [`rendered`], tolerating console lines `allow` accepts.
async fn rendered_allowing(
    browser: &Path,
    http: SocketAddr,
    page: &str,
    profile: &Path,
    allow: impl Fn(&str) -> bool,
) -> (String, Duration) {
    let url = format!("http://{http}/ui/#/{page}");
    for _ in 0..8 {
        let started = std::time::Instant::now();
        // The large graph takes longer to fetch than the load event: give the page virtual
        // time (fetches complete, timers fast-forward) before the DOM is dumped.
        let (dom, console) =
            load_with(browser, &url, profile, &["--virtual-time-budget=5000"]).await;
        assert!(
            console.iter().all(|l| allow(l)),
            "{page}: console messages: {console:#?}"
        );
        if dom.contains("data-ready=\"true\"") {
            return (dom, started.elapsed());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("{page} never rendered");
}

/// Any client can create thousands of edges and type nodes by sending distinct type names that
/// get `NO_HANDLER`. The overview must stay responsive: it draws a bounded number of type nodes
/// with a note about the rest, and the tables show a bounded number of rows.
#[tokio::test]
async fn a_flood_of_types_and_edges_renders_bounded_and_in_time() {
    const FLOOD: usize = 4000;
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let (grpc, http) = common::start_hub_with_http(config()).await;
    let http: SocketAddr = http.unwrap();
    let _owner = FakeClient::register(grpc, "service-b", &[], &["examples.CountGreetings"]).await;
    let mut flood = FakeClient::register(grpc, "flood", &[], &[]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let tx = flood.tx.clone();
    let sender = tokio::spawn(async move {
        for i in 0..FLOOD {
            tx.send(FakeClient::request(
                &format!("f{i}"),
                RequestKind::Query,
                &format!("flood.Type{i:05}"),
                "",
                0,
            ))
            .await
            .unwrap();
        }
    });
    let mut answered = 0;
    while answered < FLOOD {
        if let server_message::Message::Error(_) = next_message(&mut flood).await {
            answered += 1;
        }
    }
    sender.await.unwrap();
    let (status, body) = http_get(http, "/api/v1/graph").await;
    assert_eq!(status, 200);
    let graph: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(
        graph["edges"].as_array().unwrap().len() > FLOOD,
        "flood not recorded"
    );

    // The page's live stream is an endless fetch, which stops Chrome's virtual time; with every
    // stream slot taken the page falls back to polling (also covered here), so a virtual-time
    // budget lets the large first fetch finish before the DOM is dumped.
    let _slots = take_stream_slots(http).await;
    let profiles = std::env::temp_dir().join(format!("kraken-hub-ui-flood-{}", std::process::id()));
    let (dom, took) = rendered(&browser, http, "overview", &profiles.join("overview")).await;
    assert!(took < Duration::from_secs(15), "overview took {took:?}");
    assert!(
        dom.contains("more types hidden"),
        "no note about hidden types"
    );
    let type_nodes = dom.matches("class=\"g-node").count();
    assert!(type_nodes <= 120, "{type_nodes} graph nodes drawn");
    assert!(
        dom.contains("Showing the busiest 200 of"),
        "edges table not capped"
    );
    assert!(dom.matches("<tr>").count() < 260, "too many table rows");

    let (dom, took) = rendered(&browser, http, "types", &profiles.join("types")).await;
    assert!(took < Duration::from_secs(15), "types took {took:?}");
    assert!(
        dom.contains("Showing the first 200 of"),
        "types table not capped"
    );
    assert!(dom.matches("<tr>").count() < 260, "too many table rows");
    let _ = std::fs::remove_dir_all(&profiles);
}

/// Registers `instance_id` of `service` on the hub at `grpc`, waits for the ack and reports
/// `connected` hub ids.
async fn register_reporting(
    grpc: SocketAddr,
    service: &str,
    instance_id: &str,
    commands: &[&str],
    connected: &[&str],
) -> FakeClient {
    let mut client = FakeClient::connect(grpc).await;
    client.instance_id = instance_id.to_string();
    client.send_register(service, commands, &[]).await;
    client.expect_ack().await;
    client.status(connected, 0).await;
    client
}

/// Two in-process hubs list each other (and a third URL nobody answers) in KRAKEN_HUB_PEERS.
/// Hub A's page merges both: the instance registered on A only, the type owned by different
/// services on A and B and the instance that reports no connection to B are flagged; the dead
/// peer is shown as unreachable, not as a disagreement.
#[tokio::test]
async fn two_peer_hubs_merge_in_the_browser_and_flag_disagreements() {
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let bind = || tokio::net::TcpListener::bind("127.0.0.1:0");
    let (grpc_a, http_a, grpc_b, http_b) = (
        bind().await.unwrap(),
        bind().await.unwrap(),
        bind().await.unwrap(),
        bind().await.unwrap(),
    );
    let dead = bind().await.unwrap().local_addr().unwrap(); // closed again: connection refused
    let (ga, ha, gb, hb) = (
        grpc_a.local_addr().unwrap(),
        http_a.local_addr().unwrap(),
        grpc_b.local_addr().unwrap(),
        http_b.local_addr().unwrap(),
    );
    let peers = vec![
        format!("http://{ha}"),
        format!("http://{hb}"),
        format!("http://{dead}"),
    ];
    for (id, grpc, http) in [("hub-a", grpc_a, http_a), ("hub-b", grpc_b, http_b)] {
        let config = Config {
            hub_id: id.to_string(),
            peers: peers.clone(),
            ..config()
        };
        tokio::spawn(async move {
            kraken_hub::serve_with_shutdown(config, grpc, Some(http), std::future::pending())
                .await
                .unwrap();
        });
    }

    let both = ["hub-a", "hub-b"];
    let _agree_a = register_reporting(ga, "service-b", "b-both", &["b.Do"], &both).await;
    let _agree_b = register_reporting(gb, "service-b", "b-both", &["b.Do"], &both).await;
    let _only_a = register_reporting(ga, "service-x", "x-only-on-a", &[], &["hub-a"]).await;
    let _owner_a = register_reporting(ga, "service-y", "y-1", &["shared.Thing"], &both).await;
    let _owner_b = register_reporting(gb, "service-z", "z-1", &["shared.Thing"], &both).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // The live stream stops Chrome's virtual time; with every slot taken the page polls (see
    // the flood test), and a virtual-time budget lets the chained fetches finish.
    let _slots = take_stream_slots(ha).await;
    let profiles = std::env::temp_dir().join(format!("kraken-hub-ui-peers-{}", std::process::id()));
    let dead_port = dead.port().to_string();
    // The browser logs the refused connection to the dead peer; nothing else may be logged.
    let refused = |l: &str| l.contains("ERR_CONNECTION_REFUSED") || l.contains(&dead_port);
    let mut pages = Vec::new();
    for (page, markers) in [
        (
            "services",
            vec![
                "Seen by",
                "not registered on hub-b",
                "reports no connection to hub-b",
                "not on hub-b",
                "hubs disagree on 1",
                "b-both",
                "unreachable",
            ],
        ),
        (
            "overview",
            vec![
                "All hubs",
                "Owner differs",
                "owned by service-y on hub-a; service-z on hub-b",
                "Missing on a hub",
                "1 unreachable",
            ],
        ),
    ] {
        let (dom, _) = rendered_allowing(&browser, ha, page, &profiles.join(page), refused).await;
        for marker in markers {
            assert!(dom.contains(marker), "{page}: {marker:?} missing in {dom}");
        }
        pages.push(dom);
    }
    // The agreeing instance carries no flag.
    let services = &pages[0];
    let row = &services[services.find("b-both").unwrap()..];
    let row = &row[..row.find("</tr>").unwrap()];
    assert!(!row.contains("not registered"), "{row}");
    assert!(!row.contains("reports no connection"), "{row}");
    let _ = std::fs::remove_dir_all(&profiles);
}

// ---- processors (ADR 0022) ---------------------------------------------------------------

/// A segment with decimal positions, as the first-wave event stores report them.
fn segment(
    segment: i32,
    position: i64,
    committed: i64,
    head: i64,
) -> kraken_hub::proto::SegmentStatus {
    kraken_hub::proto::SegmentStatus {
        segment: segment.to_string(),
        position: position.to_string(),
        committed: committed.to_string(),
        head: head.to_string(),
        lag: head - committed,
        replay_until: String::new(),
    }
}

fn processor(
    name: &str,
    state: kraken_hub::proto::ProcessorState,
    segments: Vec<kraken_hub::proto::SegmentStatus>,
) -> kraken_hub::proto::ProcessorStatus {
    kraken_hub::proto::ProcessorStatus {
        name: name.to_string(),
        subscription: if name == "default" {
            "service-b".to_string()
        } else {
            format!("service-b.{name}")
        },
        source: "kraken.events".to_string(),
        event_store: "kafka".to_string(),
        capabilities: ["replay", "browse", "lag", "native-dead-letter"]
            .map(String::from)
            .to_vec(),
        state: state as i32,
        segments,
        dead_letter_mode: "collection".to_string(),
        ..Default::default()
    }
}

/// Registers `instance_id` of `service` and waits for the ack.
async fn register_as(grpc: SocketAddr, service: &str, instance_id: &str) -> FakeClient {
    let mut client = FakeClient::connect(grpc).await;
    client.instance_id = instance_id.to_string();
    client.send_register(service, &[], &[]).await;
    client.expect_ack().await;
    client
}

/// The Processors page: one row per (service, processor) with its aggregate, a row opened from
/// the URL shows every instance's segments; the Services page counts the processors and shows
/// the dead letters.
#[tokio::test]
async fn the_processors_page_aggregates_and_expands_to_segments() {
    use kraken_hub::proto::ProcessorState;
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let (grpc, http) = common::start_hub_with_http(config()).await;
    let http: SocketAddr = http.unwrap();
    let b1 = register_as(grpc, "service-b", "b-1").await;
    let b2 = register_as(grpc, "service-b", "b-2").await;
    let _a = FakeClient::register(grpc, "service-a", &[], &[]).await;
    let mut orders = processor(
        "orders",
        ProcessorState::ProcessorFailing,
        vec![segment(1, 10, 8, 20)],
    );
    orders.dead_letters = 2;
    orders.failed_total = 1;
    orders.last_error = "handler failed: boom".to_string();
    orders.last_error_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - 65_000;
    b1.status_with_processors(
        &["hub"],
        vec![
            processor(
                "default",
                ProcessorState::ProcessorRunning,
                vec![segment(0, 5, 5, 5)],
            ),
            orders,
        ],
    )
    .await;
    let mut other = processor(
        "orders",
        ProcessorState::ProcessorRunning,
        vec![segment(0, 3, 3, 4)],
    );
    other.dead_letters = 2;
    b2.status_with_processors(&["hub"], vec![other]).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let _slots = take_stream_slots(http).await;
    let profiles =
        std::env::temp_dir().join(format!("kraken-hub-ui-processors-{}", std::process::id()));
    let (dom, _) = rendered(
        &browser,
        http,
        "processors?open=service-b%2Forders",
        &profiles.join("processors"),
    )
    .await;
    for marker in [
        "aria-label=\"Processors\"",
        "service-b.orders",
        ">FAILING</span>",
        ">RUNNING</span>",
        "handler failed: boom",
        "Segments of orders on b-1",
        "Segments of orders on b-2",
        ">Head</th>",
        ">kafka</td>",
        "aria-expanded=\"true\"",
        "2 processors in 1 service",
    ] {
        assert!(
            dom.contains(marker),
            "processors: {marker:?} missing in {dom}"
        );
    }
    assert!(
        !dom.contains("Segments of default on"),
        "a closed row shows no segments"
    );
    // The error's age: 65 s before the report, plus however long the page took to load.
    let error = &dom[dom.find("handler failed: boom").unwrap()..];
    let error = &error[..error.find("</td>").unwrap()];
    assert!(
        error.contains(">1 m ") && error.contains(" s ago<"),
        "{error}"
    );
    // The orders row: 2 instances, 2 segments, lag 12 + 1, 2 dead letters (the max).
    let row = &dom[dom.find("service-b.orders").unwrap()..];
    let row = &row[..row.find("</tr>").unwrap()];
    for cell in [">2</td>", ">13</td>", ">2 dead letters</span>"] {
        assert!(row.contains(cell), "{cell:?} missing in {row}");
    }

    let (dom, _) = rendered(&browser, http, "services", &profiles.join("services")).await;
    for marker in ["2 processors", "2 dead letters"] {
        assert!(
            dom.contains(marker),
            "services: {marker:?} missing in {dom}"
        );
    }
    let _ = std::fs::remove_dir_all(&profiles);
    drop((b1, b2));
}

/// Two hubs: one instance reports to both, the report hub-b got later says FAILING, the older one
/// hub-a holds says STARTING. Hub-a's page merges them: the instance counts once and the fresher
/// report wins; a second instance only hub-b knows is added.
#[tokio::test]
async fn processors_merge_over_two_hubs_and_the_freshest_report_wins() {
    use kraken_hub::proto::ProcessorState;
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let bind = || tokio::net::TcpListener::bind("127.0.0.1:0");
    let (grpc_a, http_a, grpc_b, http_b) = (
        bind().await.unwrap(),
        bind().await.unwrap(),
        bind().await.unwrap(),
        bind().await.unwrap(),
    );
    let (ga, ha, gb, hb) = (
        grpc_a.local_addr().unwrap(),
        http_a.local_addr().unwrap(),
        grpc_b.local_addr().unwrap(),
        http_b.local_addr().unwrap(),
    );
    let peers = vec![format!("http://{ha}"), format!("http://{hb}")];
    for (id, grpc, http) in [("hub-a", grpc_a, http_a), ("hub-b", grpc_b, http_b)] {
        let config = Config {
            hub_id: id.to_string(),
            peers: peers.clone(),
            ..config()
        };
        tokio::spawn(async move {
            kraken_hub::serve_with_shutdown(config, grpc, Some(http), std::future::pending())
                .await
                .unwrap();
        });
    }
    let on_a = register_as(ga, "service-b", "b-both").await;
    let on_b = register_as(gb, "service-b", "b-both").await;
    let only_b = register_as(gb, "service-b", "b-only-on-b").await;
    on_a.status_with_processors(
        &["hub-a", "hub-b"],
        vec![processor(
            "orders",
            ProcessorState::ProcessorStarting,
            vec![],
        )],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let mut fresh = processor(
        "orders",
        ProcessorState::ProcessorFailing,
        vec![segment(0, 1, 1, 8)],
    );
    fresh.last_error = "fresh failure".to_string();
    on_b.status_with_processors(&["hub-a", "hub-b"], vec![fresh])
        .await;
    only_b
        .status_with_processors(
            &["hub-a", "hub-b"],
            vec![processor(
                "orders",
                ProcessorState::ProcessorRunning,
                vec![segment(1, 4, 4, 6)],
            )],
        )
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let _slots = take_stream_slots(ha).await;
    let profiles =
        std::env::temp_dir().join(format!("kraken-hub-ui-proc-peers-{}", std::process::id()));
    let (dom, _) = rendered(
        &browser,
        ha,
        "processors?open=service-b%2Forders",
        &profiles.join("processors"),
    )
    .await;
    for marker in [
        "on 2 reachable hubs",
        ">FAILING</span>",
        "fresh failure",
        "Segments of orders on b-both",
        "Segments of orders on b-only-on-b",
    ] {
        assert!(dom.contains(marker), "{marker:?} missing in {dom}");
    }
    assert!(
        !dom.contains(">STARTING</span>"),
        "the older report lost: {dom}"
    );
    let row = &dom[dom.find("service-b.orders").unwrap()..];
    let row = &row[..row.find("</tr>").unwrap()];
    // 2 instances (b-both once), 2 segments, lag 7 + 2.
    for cell in [">2</td>", ">9</td>"] {
        assert!(row.contains(cell), "{cell:?} missing in {row}");
    }
    let _ = std::fs::remove_dir_all(&profiles);
    drop((on_a, on_b, only_b));
}

// ---- processor control (ADR 0023) -------------------------------------------------------

/// The Processors page shows a paused processor, one stopped for a reset with its operation id
/// and one that replays (with the end of each segment's replay range); without a write
/// credential it offers the write-token field and no control buttons.
#[tokio::test]
async fn the_processors_page_shows_paused_stopped_and_replaying_and_asks_for_the_write_token() {
    use kraken_hub::proto::ProcessorState;
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let (grpc, http) = common::start_hub_with_http(config()).await;
    let http: SocketAddr = http.unwrap();
    let b1 = register_as(grpc, "service-b", "b-1").await;
    let mut replaying = processor(
        "default",
        ProcessorState::ProcessorRunning,
        vec![segment(0, 30, 30, 90), segment(1, 5, 5, 5)],
    );
    replaying.replaying = true;
    replaying.segments[0].replay_until = "77".to_string();
    let mut stopped = processor("orders", ProcessorState::ProcessorStopped, vec![]);
    stopped.operation_id = "0f3c9a1e-7d2b-4c1a-9e8f-5a6b7c8d9e0f".to_string();
    b1.status_with_processors(
        &["hub"],
        vec![
            processor("audit", ProcessorState::ProcessorPaused, vec![]),
            stopped,
            replaying,
        ],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _slots = take_stream_slots(http).await;
    let profiles =
        std::env::temp_dir().join(format!("kraken-hub-ui-controls-{}", std::process::id()));
    let (dom, _) = rendered(
        &browser,
        http,
        "processors?open=service-b%2Fdefault",
        &profiles.join("processors"),
    )
    .await;
    for marker in [
        ">PAUSED</span>",
        ">STOPPED</span>",
        ">replaying</span>",
        ">op 0f3c9a1e</span>",
        "Replay until",
        ">77</td>",
        "id=\"write-token\"",
        ">Write token</label>",
        "a control goes to the hub that served this page",
        "href=\"#/dead-letters?service=service-b&amp;processor=audit\"",
    ] {
        assert!(dom.contains(marker), "{marker:?} missing in {dom}");
    }
    for absent in [
        ">Pause</button>",
        ">Resume</button>",
        ">Reset…</button>",
        ">Actions</th>",
    ] {
        assert!(
            !dom.contains(absent),
            "{absent:?} without a write credential: {dom}"
        );
    }
    let _ = std::fs::remove_dir_all(&profiles);
    drop(b1);
}

/// Enters `token` in the Processors page's write-token field (as an operator does), once the page
/// rendered, and waits for the control buttons.
fn enable_controls(devtools: &mut Devtools, token: &str) {
    devtools.wait_for(
        "document.getElementById('write-token') ? 'field' : ''",
        "field",
        Duration::from_secs(30),
    );
    devtools.eval(&format!(
        "(() => {{ const i = document.getElementById('write-token'); i.value = {}; i.form.requestSubmit(); return 'ok'; }})()",
        serde_json::Value::from(token)
    ));
    devtools.wait_for(
        "document.querySelector('table.processors')?.outerHTML ?? ''",
        ">Pause</button>",
        Duration::from_secs(30),
    );
}

/// The text of each Processors row (one per processor), with its HTML.
fn processor_rows(devtools: &mut Devtools) -> Vec<(String, String)> {
    let rows = devtools.eval(
        "[...document.querySelectorAll('table.processors > tbody > tr:not(.detail)')].map((r) => [r.textContent, r.outerHTML])",
    );
    serde_json::from_value(rows).unwrap()
}

/// With a write credential the page offers a reset only for a processor whose event store
/// declares the `replay` capability (ADR 0027); pause and resume stay for every processor. The
/// event store shows in its column, with its capabilities in the title.
#[tokio::test]
async fn reset_offered_only_with_replay_capability() {
    use kraken_hub::proto::ProcessorState;
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let (grpc, http) = common::start_hub_with_http(config()).await;
    let http: SocketAddr = http.unwrap();
    let b1 = register_as(grpc, "service-b", "b-1").await;
    let mut queue = processor("audit", ProcessorState::ProcessorRunning, vec![]);
    queue.event_store = "queue-x".to_string();
    queue.capabilities = vec!["lag".to_string()];
    let mut silent = processor("billing", ProcessorState::ProcessorPaused, vec![]);
    silent.event_store = String::new();
    silent.capabilities = vec![];
    b1.status_with_processors(
        &["hub"],
        vec![
            processor("orders", ProcessorState::ProcessorRunning, vec![]),
            queue,
            silent,
        ],
    )
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let _slots = take_stream_slots(http).await;
    let profile =
        std::env::temp_dir().join(format!("kraken-hub-ui-replay-cap-{}", std::process::id()));
    let page = format!("http://{http}/ui/#/processors");
    let dir = profile.clone();
    let rows = tokio::task::spawn_blocking(move || {
        let mut devtools = Devtools::launch(&browser, &dir);
        devtools.open(&page);
        enable_controls(&mut devtools, "any-write-token");
        processor_rows(&mut devtools)
    })
    .await
    .unwrap();
    let row = |name: &str| {
        rows.iter()
            .find(|(text, _)| text.contains(name))
            .unwrap_or_else(|| panic!("no {name} row in {rows:?}"))
            .1
            .clone()
    };
    let orders = row("orders");
    assert!(orders.contains(">Reset…</button>"), "{orders}");
    assert!(orders.contains(">Pause</button>"), "{orders}");
    assert!(
        orders.contains("title=\"Capabilities: browse, lag, native-dead-letter, replay\""),
        "{orders}"
    );
    let audit = row("audit");
    assert!(!audit.contains(">Reset…</button>"), "{audit}");
    assert!(audit.contains(">no reset</span>"), "{audit}");
    assert!(audit.contains(">Pause</button>"), "{audit}");
    assert!(
        audit.contains("title=\"Capabilities: lag\">queue-x</td>"),
        "{audit}"
    );
    let billing = row("billing");
    assert!(!billing.contains(">Reset…</button>"), "{billing}");
    assert!(billing.contains(">Resume</button>"), "{billing}");
    let _ = std::fs::remove_dir_all(&profile);
    drop(b1);
}

/// An instance answers a reset its event store cannot do with `CONTROL_REFUSAL_UNSUPPORTED`
/// (ADR 0027): the API answers 409 with the code and the instance's reason, and the Processors
/// page shows both on its status line.
#[tokio::test]
async fn unsupported_refusal_reaches_the_dashboard() {
    use common::control::{ControlClient, Reaction, default_result, refused};
    use kraken_hub::proto::{ControlRefusal, control_request};
    const WRITE: &str = "write-token-0123456789abcdef";
    const WHY: &str = "this event store cannot seek by time";
    let (grpc, http) = common::start_hub_with_http(Config {
        dashboard_write_token: Some(kraken_hub::auth::Secret::new(WRITE)),
        ..config()
    })
    .await;
    let http: SocketAddr = http.unwrap();
    let b1 = ControlClient::new(
        grpc,
        "service-b",
        "b-1",
        &["audit"],
        std::sync::Arc::new(
            |req: &kraken_hub::proto::ControlRequest| match &req.action {
                Some(control_request::Action::ResetExecute(_)) => {
                    Reaction::Reply(refused(ControlRefusal::Unsupported, WHY))
                }
                _ => Reaction::Reply(default_result(req)),
            },
        ),
    )
    .await;

    let auth = format!("Bearer {WRITE}");
    let r = common::http_send(
        http,
        "POST",
        "/api/v1/services/service-b/processors/audit/reset",
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &auth),
        ],
        br#"{"to":{"timestampMs":1762250000000}}"#,
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(r.status, 409, "{}", r.body);
    let body: serde_json::Value = serde_json::from_str(&r.body).unwrap();
    assert_eq!(body["outcome"], "failed");
    assert_eq!(
        body["error"],
        serde_json::json!({
            "stage": "execute", "instanceId": "b-1", "result": "refused",
            "code": "UNSUPPORTED", "message": WHY,
        })
    );

    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let _slots = take_stream_slots(http).await;
    let profile =
        std::env::temp_dir().join(format!("kraken-hub-ui-unsupported-{}", std::process::id()));
    let page = format!("http://{http}/ui/#/processors");
    let dir = profile.clone();
    let line = tokio::task::spawn_blocking(move || {
        let mut devtools = Devtools::launch(&browser, &dir);
        devtools.open(&page);
        enable_controls(&mut devtools, WRITE);
        devtools.eval(
            "[...document.querySelectorAll('table.processors button')].find((b) => b.textContent === 'Reset…').click()",
        );
        devtools.wait_for(
            "document.querySelector('dialog.confirm') ? 'open' : ''",
            "open",
            Duration::from_secs(10),
        );
        devtools.eval("document.querySelector('dialog.confirm button.danger').click()");
        devtools.wait_for(
            "document.querySelector('.control-status')?.textContent ?? ''",
            "UNSUPPORTED",
            Duration::from_secs(30),
        )
    })
    .await
    .unwrap();
    assert_eq!(
        line,
        format!("HTTP 409: refused UNSUPPORTED: {WHY} (instance b-1)")
    );
    let _ = std::fs::remove_dir_all(&profile);
    drop(b1);
}

/// The Dead letters page: the processors that keep letters, and one processor's letters as an
/// instance lists them through the hub, without retry or discard buttons until a write
/// credential is held.
#[tokio::test]
async fn the_dead_letters_page_lists_a_processors_letters_through_the_hub() {
    use common::control::{ControlClient, Reaction};
    use kraken_hub::proto::{DeadLetterPage, DeadLetterSummary, control_reply, control_request};
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let (grpc, http) = common::start_hub_with_http(config()).await;
    let http: SocketAddr = http.unwrap();
    let letter = |id: &str, reason: &str, blocked_by: &str| DeadLetterSummary {
        id: id.to_string(),
        processor: "audit".to_string(),
        source: "kraken.events".to_string(),
        segment: "2".to_string(),
        position: "41".to_string(),
        key: "order-7".to_string(),
        event_id: "evt-1".to_string(),
        event_type: "examples.OrderPlaced".to_string(),
        reason: reason.to_string(),
        error_type: "IllegalStateException".to_string(),
        error_message: "boom <b>not markup</b>".to_string(),
        blocked_by: blocked_by.to_string(),
        attempts: 3,
        parked_at_ms: 1_762_250_000_000,
        last_attempt_at_ms: 1_762_250_001_000,
    };
    let b1 = ControlClient::new(
        grpc,
        "service-b",
        "b-1",
        &["audit"],
        std::sync::Arc::new(
            move |req: &kraken_hub::proto::ControlRequest| match &req.action {
                Some(control_request::Action::DeadLetterList(_)) => {
                    Reaction::Reply(control_reply::Result::DeadLetters(DeadLetterPage {
                        letters: vec![
                            letter("letter-1", "HANDLER_FAILED", ""),
                            letter("letter-2", "SEQUENCED", "letter-1"),
                        ],
                        total: 2,
                    }))
                }
                _ => Reaction::Reply(common::control::ok()),
            },
        ),
    )
    .await;
    let _slots = take_stream_slots(http).await;
    let profiles =
        std::env::temp_dir().join(format!("kraken-hub-ui-letters-{}", std::process::id()));
    let (dom, _) = rendered(
        &browser,
        http,
        "dead-letters?service=service-b&processor=audit",
        &profiles.join("letters"),
    )
    .await;
    for marker in [
        "aria-label=\"Dead letters\"",
        "audit of service-b",
        ">letter-1</td>",
        ">letter-2</td>",
        ">HANDLER_FAILED</span>",
        ">SEQUENCED</span>",
        "boom &lt;b&gt;not markup&lt;/b&gt;",
        "2 letters; listed by instance b-1",
        "id=\"write-token\"",
    ] {
        assert!(dom.contains(marker), "{marker:?} missing in {dom}");
    }
    for absent in [
        ">Retry</button>",
        ">Discard</button>",
        ">Retry all</button>",
        "<b>not markup",
    ] {
        assert!(!dom.contains(absent), "{absent:?}: {dom}");
    }
    let (dom, _) = rendered(&browser, http, "dead-letters", &profiles.join("index")).await;
    assert!(dom.contains("Show the letters"), "{dom}");
    assert!(
        dom.contains("1 processor with a dead-letter collection"),
        "{dom}"
    );
    let _ = std::fs::remove_dir_all(&profiles);
    drop(b1);
}

#[test]
fn path_resolution_handles_dot_segments() {
    assert_eq!(resolve("/ui/", "v/abc/app.js"), "/ui/v/abc/app.js");
    assert_eq!(resolve("/ui/v/abc/pages/", "../dom.js"), "/ui/v/abc/dom.js");
    assert_eq!(
        resolve("/ui/v/abc/", "./pages/x.js"),
        "/ui/v/abc/pages/x.js"
    );
    assert_eq!(
        import_specifiers(
            "import a from './a.js';\nimport { b } from \"../b.js\";\nimport('./c.js')"
        ),
        ["./a.js", "../b.js", "./c.js"]
    );
}

/// With a dashboard token the page loads, finds out from the first 401 that it needs one, and
/// shows the sign-in form instead of data (ADR 0018). The console shows the failed request; the
/// page itself must not break.
#[tokio::test]
async fn a_hub_with_a_dashboard_token_shows_the_sign_in_form() {
    let Some(browser) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let (_grpc, http) = common::start_hub_with_http(Config {
        dashboard_token: Some(kraken_hub::auth::Secret::new("dash-0123456789abcdef0123")),
        ..config()
    })
    .await;
    let http: SocketAddr = http.unwrap();
    let profile = std::env::temp_dir().join(format!("kraken-hub-ui-login-{}", std::process::id()));
    let url = format!("http://{http}/ui/");
    let mut last = String::new();
    for _ in 0..8 {
        let (dom, _console) = load(&browser, &url, &profile).await;
        if dom.contains("id=\"login-token\"") {
            assert!(dom.contains("type=\"password\""), "{dom}");
            assert!(dom.contains("Dashboard token"), "{dom}");
            // No data page rendered behind the form.
            assert!(!dom.contains("Traffic graph"), "{dom}");
            let _ = std::fs::remove_dir_all(&profile);
            return;
        }
        last = dom;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("the sign-in form never appeared: {last}");
}

/// The Events page (ADR 0025, ADR 0027) over a fake event browser: it lists the browser's
/// records newest first and opens one (its headers, the reason its payload is hidden), every
/// value as text. The Processors page lists a subscription no live instance reports with the
/// browser's lag. A hub without a browser has no Events page in its navigation.
#[tokio::test]
async fn the_events_page_renders_the_browsers_answers_as_text_and_hides_without_a_browser() {
    use common::browser::{self, Answer, FakeBrowser};
    use serde_json::json;
    let Some(chrome) = browser() else {
        eprintln!("no Chrome or Chromium found: headless check skipped");
        return;
    };
    let (grpc, http) = common::start_hub_with_http(Config {
        event_payloads: true,
        ..config()
    })
    .await;
    let http: SocketAddr = http.unwrap();
    let _fake = FakeBrowser::start(grpc, |type_name, request| match type_name {
        browser::SEARCH => Answer::Json(browser::page(vec![
            browser::summary(
                "1",
                "0",
                1_700_000_001_000,
                "order-2",
                json!({"type": "orders.Shipped", "source": "service-b"}),
            ),
            browser::summary(
                "0",
                "0",
                1_700_000_000_000,
                "order-1",
                json!({"type": "orders.<img src=x onerror=alert(1)>", "correlationId": "corr-ui"}),
            ),
        ])),
        browser::GET => {
            let mut r = browser::summary(
                "0",
                "0",
                1_700_000_000_000,
                "order-1",
                json!({"type": "orders.<img src=x onerror=alert(1)>"}),
            );
            r["allHeaders"] =
                json!([{"name": "type", "value": "orders.<img src=x onerror=alert(1)>"}]);
            Answer::Json(r)
        }
        browser::LAG => Answer::Json(json!({"source": "kraken.events", "subscriptions": [
            {"subscription": "service-b.audit", "state": "Empty", "members": 0,
             "segments": [{"segment": "0", "committed": "0", "head": "7", "lag": 7}], "lag": 7}],
            "truncated": false})),
        _ => browser::defaults(type_name, request),
    })
    .await;
    let _slots = take_stream_slots(http).await;
    let profiles =
        std::env::temp_dir().join(format!("kraken-hub-ui-events-{}", std::process::id()));

    let (dom, _) = rendered(
        &chrome,
        http,
        "events?record=kraken.events%2F0%2F0",
        &profiles.join("events"),
    )
    .await;
    for marker in [
        "aria-label=\"Search results\"",
        "Scanned 2 records; every segment searched.",
        "orders.Shipped",
        "orders.&lt;img src=x onerror=alert(1)&gt;",
        "href=\"#/events?source=kraken.events&amp;type=",
        "correlationId=corr-ui",
        ">corr-ui</a>",
        ">0 / 0</button>",
        "aria-label=\"Record\"",
        "kraken.events 0 / 0",
        "payload hidden",
        "no-token-configured",
        "Payloads are not served to this page",
        "<select aria-label=\"Source\"><option value=\"kraken.events\">kraken.events</option>",
        "href=\"#/events\"",
    ] {
        assert!(dom.contains(marker), "events: {marker:?} missing in {dom}");
    }
    assert!(
        !dom.contains("<img src=x"),
        "markup from a header became markup"
    );
    assert!(!dom.contains("consumer-groups"), "{dom}");
    // Newest first, as the browser answered.
    let results = &dom[dom.find("aria-label=\"Search results\"").unwrap()..];
    assert!(results.find(">order-2<").unwrap() < results.find(">order-1<").unwrap());

    let (dom, _) = rendered(&chrome, http, "processors", &profiles.join("processors")).await;
    for marker in [
        "Subscriptions without a live instance",
        "service-b.audit",
        ">no live instance</span>",
    ] {
        assert!(
            dom.contains(marker),
            "processors: {marker:?} missing in {dom}"
        );
    }

    // Without a browser the Events page is not in the navigation, and says why when opened.
    let (_grpc, plain) = common::start_hub_with_http(config()).await;
    let plain: SocketAddr = plain.unwrap();
    let _plain_slots = take_stream_slots(plain).await;
    let (dom, _) = rendered(&chrome, plain, "events", &profiles.join("plain")).await;
    assert!(dom.contains("href=\"#/processors\""), "{dom}");
    assert!(!dom.contains("href=\"#/events\""), "{dom}");
    assert!(
        dom.contains("No event browser is connected to this hub"),
        "{dom}"
    );
    let _ = std::fs::remove_dir_all(&profiles);
}

/// A headless browser driven over the DevTools protocol on a pipe (`--remote-debugging-pipe`:
/// the browser reads commands on fd 3 and writes answers on fd 4, each JSON message ended by a
/// NUL), for a page that must be watched while it runs rather than dumped once.
struct Devtools {
    child: std::process::Child,
    commands: std::fs::File,
    answers: std::io::BufReader<std::fs::File>,
    next: u64,
    session: Option<String>,
}

impl Devtools {
    /// Starts `browser` with its pipe on two FIFOs under `profile` (a shell sets fds 3 and 4).
    fn launch(browser: &Path, profile: &Path) -> Self {
        std::fs::create_dir_all(profile).unwrap();
        let (to, from) = (profile.join("devtools-in"), profile.join("devtools-out"));
        for fifo in [&to, &from] {
            let _ = std::fs::remove_file(fifo);
            let made = std::process::Command::new("mkfifo").arg(fifo).status();
            assert!(made.unwrap().success(), "mkfifo {}", fifo.display());
        }
        let child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("exec \"$0\" \"$@\" 3<\"$DEVTOOLS_IN\" 4>\"$DEVTOOLS_OUT\"")
            .arg(browser)
            .args([
                "--headless=new",
                "--disable-gpu",
                "--no-first-run",
                "--no-default-browser-check",
                "--disable-extensions",
                "--remote-debugging-pipe",
            ])
            .arg(format!("--user-data-dir={}", profile.display()))
            .arg("about:blank")
            .env("DEVTOOLS_IN", &to)
            .env("DEVTOOLS_OUT", &from)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("browser did not start");
        // The shell opens fd 3 (blocking until this side writes), then fd 4.
        let commands = std::fs::OpenOptions::new().write(true).open(&to).unwrap();
        let answers = std::io::BufReader::new(std::fs::File::open(&from).unwrap());
        Self {
            child,
            commands,
            answers,
            next: 0,
            session: None,
        }
    }

    /// Sends `method` (to the attached page, once there is one) and returns its `result`.
    fn call(&mut self, method: &str, params: serde_json::Value) -> serde_json::Value {
        use std::io::{BufRead, Write};
        self.next += 1;
        let mut message = serde_json::json!({"id": self.next, "method": method, "params": params});
        if let Some(session) = &self.session {
            message["sessionId"] = session.clone().into();
        }
        let mut bytes = serde_json::to_vec(&message).unwrap();
        bytes.push(0);
        self.commands.write_all(&bytes).unwrap();
        loop {
            let mut raw = Vec::new();
            let read = self.answers.read_until(0, &mut raw).unwrap();
            assert!(read > 0, "the browser closed its pipe");
            raw.pop();
            let answer: serde_json::Value = serde_json::from_slice(&raw).unwrap();
            if answer["id"] == self.next {
                assert!(answer.get("error").is_none(), "{method}: {answer}");
                return answer["result"].clone();
            }
        }
    }

    /// Opens `url` in a new page and talks to that page from now on.
    fn open(&mut self, url: &str) {
        let target = self.call("Target.createTarget", serde_json::json!({ "url": url }));
        let attached = self.call(
            "Target.attachToTarget",
            serde_json::json!({ "targetId": target["targetId"], "flatten": true }),
        );
        self.session = attached["sessionId"].as_str().map(str::to_string);
    }

    /// The value of the JavaScript `expression` in the page.
    fn eval(&mut self, expression: &str) -> serde_json::Value {
        let result = self.call(
            "Runtime.evaluate",
            serde_json::json!({ "expression": expression, "returnByValue": true }),
        );
        result["result"]["value"].clone()
    }

    /// Evaluates `expression` until it is a string containing `marker`, for at most `within`.
    fn wait_for(&mut self, expression: &str, marker: &str, within: Duration) -> String {
        let deadline = std::time::Instant::now() + within;
        loop {
            let value = self.eval(expression);
            let text = value.as_str().unwrap_or_default().to_string();
            if text.contains(marker) {
                return text;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{marker:?} never came: {text}"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

impl Drop for Devtools {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
