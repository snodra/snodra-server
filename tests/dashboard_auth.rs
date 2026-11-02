//! The dashboard token (roadmap P5a, ADR 0018): `/topology`, `/api/v1/*` and the event stream
//! need `Authorization: Bearer <token>` when `KRAKEN_HUB_DASHBOARD_TOKEN` is set; the page and its
//! assets stay public.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{http_get_full, http_request, start_hub_with_http};
use kraken_hub::auth::Secret;
use kraken_hub::config::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "dash-0123456789abcdef0123456789";

fn config() -> Config {
    Config {
        otlp_endpoint: None,
        dashboard_token: Some(Secret::new(TOKEN)),
        ..Config::default()
    }
}

async fn start(config: Config) -> SocketAddr {
    start_hub_with_http(config).await.1.unwrap()
}

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

const JSON_PATHS: &[&str] = &[
    "/topology",
    "/api/v1/topology",
    "/api/v1/types",
    "/api/v1/graph",
    "/api/v1/settings",
    "/api/v1/version",
];

#[tokio::test]
async fn every_data_endpoint_is_401_without_the_token_and_200_with_it() {
    let http = start(config()).await;
    for path in JSON_PATHS.iter().chain(&["/api/v1/stream"]) {
        let r = http_get_full(http, path).await;
        assert_eq!(r.status, 401, "{path}");
        assert_eq!(r.body, "", "{path}: no detail in the body");
        assert_eq!(r.header("www-authenticate"), Some("Bearer"), "{path}");
    }
    for path in JSON_PATHS {
        let r = http_request(http, "GET", path, &[("Authorization", &bearer(TOKEN))]).await;
        assert_eq!(r.status, 200, "{path}");
    }
    // The scheme is case-insensitive.
    let r = http_request(
        http,
        "GET",
        "/topology",
        &[("authorization", &format!("bearer {TOKEN}"))],
    )
    .await;
    assert_eq!(r.status, 200);
}

#[tokio::test]
async fn a_wrong_token_is_the_same_bare_401() {
    let http = start(config()).await;
    let missing = http_get_full(http, "/api/v1/topology").await;
    let wrong_values = [
        bearer("wrong-token-0123456789abcdef0123"),
        bearer(&TOKEN[..TOKEN.len() - 1]),
        bearer(&format!("{TOKEN}x")),
        bearer(""),
        TOKEN.to_string(),
        format!("Basic {TOKEN}"),
        "Bearer".to_string(),
    ];
    for value in wrong_values {
        let r = http_request(
            http,
            "GET",
            "/api/v1/topology",
            &[("Authorization", &value)],
        )
        .await;
        assert_eq!(r.status, 401, "{value}");
        assert_eq!(r.body, missing.body, "{value}");
        assert_eq!(
            r.header("www-authenticate"),
            missing.header("www-authenticate"),
            "{value}"
        );
    }
    // A token in the URL is never honoured.
    for path in [
        format!("/api/v1/topology?token={TOKEN}"),
        format!("/topology?access_token={TOKEN}"),
    ] {
        assert_eq!(http_get_full(http, &path).await.status, 401, "{path}");
    }
}

#[tokio::test]
async fn the_page_and_its_assets_stay_public() {
    let http = start(config()).await;
    let build = kraken_hub::assets::build_id();
    for path in [
        "/".to_string(),
        "/ui".to_string(),
        "/ui/".to_string(),
        format!("/ui/v/{build}/app.js"),
        format!("/ui/v/{build}/api.js"),
        format!("/ui/v/{build}/login.js"),
        format!("/ui/v/{build}/app.css"),
        format!("/ui/v/{build}/favicon.svg"),
    ] {
        let r = http_get_full(http, &path).await;
        assert!(matches!(r.status, 200 | 307), "{path}: {}", r.status);
    }
    // The page never carries the token.
    let page = http_get_full(http, "/ui/").await;
    assert!(!page.body.contains(TOKEN));
    assert!(page.body.contains("Sign out"));
}

#[tokio::test]
async fn the_event_stream_authenticates_with_the_header_and_never_a_url() {
    let http = start(config()).await;

    async fn open(
        http: SocketAddr,
        path: &str,
        header: Option<&str>,
    ) -> (String, tokio::net::TcpStream) {
        let mut stream = tokio::net::TcpStream::connect(http).await.unwrap();
        let auth = header
            .map(|v| format!("Authorization: {v}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: {http}\r\n{auth}Accept: text/event-stream\r\n\r\n"
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut seen = String::new();
        let mut chunk = [0u8; 4096];
        while !(seen.contains("event: summary")
            || seen.contains("\r\n\r\n") && seen.starts_with("HTTP/1.1 401"))
        {
            let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut chunk))
                .await
                .expect("no answer")
                .unwrap();
            if n == 0 {
                break;
            }
            seen.push_str(&String::from_utf8_lossy(&chunk[..n]));
        }
        (seen, stream)
    }

    let (seen, _s) = open(http, "/api/v1/stream", Some(&bearer(TOKEN))).await;
    assert!(seen.starts_with("HTTP/1.1 200"), "{seen}");
    assert!(
        seen.to_ascii_lowercase()
            .contains("content-type: text/event-stream"),
        "{seen}"
    );
    assert!(seen.contains("event: summary"), "{seen}");

    let (seen, _s) = open(http, "/api/v1/stream", None).await;
    assert!(seen.starts_with("HTTP/1.1 401"), "{seen}");
    // A URL token (what EventSource would have to use) does not open a stream.
    let (seen, _s) = open(http, &format!("/api/v1/stream?token={TOKEN}"), None).await;
    assert!(seen.starts_with("HTTP/1.1 401"), "{seen}");
}

#[tokio::test]
async fn a_peer_page_may_forward_the_token_cross_origin() {
    let peer_origin = "http://hub-a.example:7001";
    let http = start(Config {
        peers: vec![peer_origin.to_string()],
        ..config()
    })
    .await;

    // The preflight carries no Authorization header and must be answered without the token,
    // allowing that header.
    let r = http_request(
        http,
        "OPTIONS",
        "/api/v1/topology",
        &[
            ("Origin", peer_origin),
            ("Access-Control-Request-Method", "GET"),
            ("Access-Control-Request-Headers", "authorization"),
        ],
    )
    .await;
    assert_eq!(r.status, 204);
    assert!(
        r.header("access-control-allow-headers")
            .unwrap()
            .contains("authorization")
    );
    assert_eq!(r.header("access-control-allow-origin"), Some(peer_origin));

    // The peer's page then sends the same token and reads the answer...
    let r = http_request(
        http,
        "GET",
        "/api/v1/topology",
        &[("Origin", peer_origin), ("Authorization", &bearer(TOKEN))],
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("access-control-allow-origin"), Some(peer_origin));

    // ...and a wrong token is a 401 the page can read (it carries the CORS header), so it shows
    // "rejected" instead of "unreachable".
    let r = http_request(
        http,
        "GET",
        "/api/v1/topology",
        &[
            ("Origin", peer_origin),
            ("Authorization", &bearer("another-hubs-token-0123456789")),
        ],
    )
    .await;
    assert_eq!(r.status, 401);
    assert_eq!(r.header("access-control-allow-origin"), Some(peer_origin));

    // An origin that is not a peer gets no CORS header even with the right token.
    let r = http_request(
        http,
        "GET",
        "/api/v1/topology",
        &[
            ("Origin", "http://evil.example"),
            ("Authorization", &bearer(TOKEN)),
        ],
    )
    .await;
    assert!(r.header("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn topology_stays_protected_when_the_dashboard_is_off() {
    let http = start(Config {
        dashboard_enabled: false,
        ..config()
    })
    .await;
    assert_eq!(http_get_full(http, "/topology").await.status, 401);
    let r = http_request(
        http,
        "GET",
        "/topology",
        &[("Authorization", &bearer(TOKEN))],
    )
    .await;
    assert_eq!(r.status, 200);
    assert_eq!(
        http_request(
            http,
            "GET",
            "/api/v1/topology",
            &[("Authorization", &bearer(TOKEN))]
        )
        .await
        .status,
        404
    );
}

#[tokio::test]
async fn without_a_token_configured_nothing_changes() {
    let http = start(Config {
        otlp_endpoint: None,
        ..Config::default()
    })
    .await;
    for path in JSON_PATHS {
        assert_eq!(http_get_full(http, path).await.status, 200, "{path}");
    }
    // The settings view lists the token names, never a value.
    let r = http_get_full(http, "/api/v1/settings").await;
    assert!(r.body.contains("KRAKEN_HUB_DASHBOARD_TOKEN"));
}

#[tokio::test]
async fn the_settings_view_never_shows_the_token() {
    let http = start(config()).await;
    let r = http_request(
        http,
        "GET",
        "/api/v1/settings",
        &[("Authorization", &bearer(TOKEN))],
    )
    .await;
    assert_eq!(r.status, 200);
    assert!(!r.body.contains(TOKEN));
    let v: serde_json::Value = serde_json::from_str(&r.body).unwrap();
    let entry = v["settings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["name"] == "KRAKEN_HUB_DASHBOARD_TOKEN")
        .unwrap();
    assert_eq!(entry["secret"], true);
    assert!(entry["value"].is_null());
}

/// The dashboard's own client code (`ui/api.js`), run by Node against real hubs: the token goes
/// out as a header on the hub's own calls, on calls to a peer hub and on the event stream, and
/// is never part of a URL. Skipped when Node is not installed.
#[tokio::test]
async fn the_dashboard_client_sends_the_token_in_a_header_everywhere() {
    let hub_a = start(config()).await;
    let hub_b = start(config()).await;
    let dir = common::TempDir::new();
    let api = dir.path("api.mjs");
    std::fs::copy(concat!(env!("CARGO_MANIFEST_DIR"), "/ui/api.js"), &api).unwrap();
    let script = dir.write(
        "client.mjs",
        r#"
import assert from 'node:assert/strict';
const [hubA, hubB, token, apiPath] = process.argv.slice(2);
globalThis.document = { baseURI: `${hubA}/ui/` };
const store = new Map();
globalThis.sessionStorage = {
  getItem: (k) => (store.has(k) ? store.get(k) : null),
  setItem: (k, v) => store.set(k, v),
  removeItem: (k) => store.delete(k),
};
const urls = [];
const realFetch = globalThis.fetch;
globalThis.fetch = (url, init) => { urls.push(String(url)); return realFetch(url, init); };
const api = await import(apiPath);

let unauthorized = 0;
api.onUnauthorized(() => { unauthorized += 1; });
assert.equal(api.hasToken(), false);
await assert.rejects(api.get('version'), api.AuthError);
assert.equal(unauthorized, 1);

api.setToken(`  ${token}\n`);
assert.equal(api.hasToken(), true);
assert.equal(store.get('kraken-hub.token'), token, 'kept in sessionStorage, trimmed');
const version = await api.get('version');
assert.ok(version.hubId);

// A peer hub with the same token: the same header goes cross-origin.
const peer = await api.getFrom(hubB, 'version');
assert.ok(peer.hubId && peer.hubId !== version.hubId);

// The event stream is read with fetch and the header.
const controller = new AbortController();
let summary = null;
await api.readStream({
  signal: controller.signal,
  onOpen() {},
  onEvent(name, data) {
    if (name === 'summary') { summary = JSON.parse(data); controller.abort(); }
  },
}).catch((e) => { if (!controller.signal.aborted) throw e; });
assert.ok(summary && summary.hubId === version.hubId);

// A wrong token: the hub's own calls ask for a new one, a peer call is just an error.
api.setToken('not-the-token-not-the-token');
await assert.rejects(api.get('version'), api.AuthError);
assert.equal(unauthorized, 2);
await assert.rejects(api.getFrom(hubB, 'version'), (e) => !(e instanceof api.AuthError) && /401/.test(e.message));
assert.equal(unauthorized, 2, 'a peer 401 does not sign the page out');
await assert.rejects(api.readStream({ signal: new AbortController().signal, onOpen() {}, onEvent() {} }), api.AuthError);

api.clearToken();
assert.equal(api.hasToken(), false);
assert.equal(store.has('kraken-hub.token'), false);

for (const url of urls) assert.ok(!url.includes(token) && !url.includes('token='), `token in a URL: ${url}`);
console.log('ok');
"#,
    );
    let node = tokio::process::Command::new("node")
        .arg(&script)
        .arg(format!("http://{hub_a}"))
        .arg(format!("http://{hub_b}"))
        .arg(TOKEN)
        .arg(format!("file://{}", api.display()))
        .output()
        .await;
    let out = match node {
        Ok(out) => out,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("node not found: dashboard client check skipped");
            return;
        }
        Err(e) => panic!("{e}"),
    };
    assert!(
        out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "ok",
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}
