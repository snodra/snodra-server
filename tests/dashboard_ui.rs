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

fn config() -> Config {
    Config {
        otlp_endpoint: None,
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
        ("processors", "Available from phase 6."),
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

/// Registers `instance_id` of `service` on the hub at `grpc` as a v2 client with the
/// "instance-status" capability, reports `connected` hub ids, and waits for the ack.
async fn register_reporting(
    grpc: SocketAddr,
    service: &str,
    instance_id: &str,
    commands: &[&str],
    connected: &[&str],
) -> FakeClient {
    let mut client = FakeClient::connect(grpc).await;
    client.instance_id = instance_id.to_string();
    client
        .send_raw_register(kraken_hub::proto::Register {
            service_name: service.to_string(),
            instance_id: instance_id.to_string(),
            command_types: commands.iter().map(|c| c.to_string()).collect(),
            protocol_version: 2,
            capabilities: vec!["instance-status".to_string()],
            ..Default::default()
        })
        .await;
    match client.next().await {
        Some(Ok(ServerMessage {
            message: Some(server_message::Message::RegisterAck(_)),
        })) => {}
        other => panic!("expected the ack, got {other:?}"),
    }
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
