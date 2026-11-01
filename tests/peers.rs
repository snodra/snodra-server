//! Dashboard peers (ADR 0016): `KRAKEN_HUB_PEERS` reaches the page through `/api/v1/version` and
//! `/api/v1/settings`, the CSP's `connect-src` gains exactly the peer origins, and the API answers
//! CORS for those origins only (GET and its preflight, `/api/v1/*` only, never credentials).

mod common;

use std::net::SocketAddr;

use common::{HttpResponse, http_request, start_hub_with_http};
use kraken_hub::config::Config;
use serde_json::Value;

const PEER: &str = "http://localhost:7011";
const OTHER_PEER: &str = "http://127.0.0.1:9";

fn config(peers: &[&str]) -> Config {
    Config {
        hub_id: "hub-1".into(),
        otlp_endpoint: None,
        peers: peers.iter().map(|p| p.to_string()).collect(),
        ..Config::default()
    }
}

async fn start(peers: &[&str]) -> SocketAddr {
    start_hub_with_http(config(peers)).await.1.unwrap()
}

async fn get(http: SocketAddr, path: &str, origin: Option<&str>) -> HttpResponse {
    let headers: Vec<(&str, &str)> = origin.map(|o| ("Origin", o)).into_iter().collect();
    http_request(http, "GET", path, &headers).await
}

fn connect_src(r: &HttpResponse) -> String {
    let csp = r.header("content-security-policy").expect("no CSP");
    csp.split(';')
        .map(str::trim)
        .find(|d| d.starts_with("connect-src"))
        .unwrap_or_else(|| panic!("no connect-src in {csp}"))
        .to_string()
}

#[tokio::test]
async fn an_allowed_origin_is_echoed_on_every_api_get() {
    let http = start(&[PEER, OTHER_PEER]).await;
    for path in [
        "/api/v1/topology",
        "/api/v1/version",
        "/api/v1/types",
        "/api/v1/graph",
        "/api/v1/settings",
    ] {
        let r = get(http, path, Some(PEER)).await;
        assert_eq!(r.status, 200, "{path}");
        assert_eq!(
            r.header("access-control-allow-origin"),
            Some(PEER),
            "{path}"
        );
        assert!(
            r.header("vary")
                .is_some_and(|v| v.to_ascii_lowercase().contains("origin")),
            "{path}: Vary: Origin missing"
        );
        assert_eq!(r.header("access-control-allow-credentials"), None, "{path}");
    }
    let r = get(http, "/api/v1/topology", Some(OTHER_PEER)).await;
    assert_eq!(r.header("access-control-allow-origin"), Some(OTHER_PEER));
}

#[tokio::test]
async fn another_origin_gets_no_cors_header() {
    let http = start(&[PEER]).await;
    for origin in [
        "http://evil.example",
        "http://localhost:7012",
        "https://localhost:7011",
        "http://LOCALHOST:7011x",
        "null",
        "*",
    ] {
        let r = get(http, "/api/v1/topology", Some(origin)).await;
        assert_eq!(r.status, 200, "{origin}");
        assert_eq!(r.header("access-control-allow-origin"), None, "{origin}");
    }
    let r = get(http, "/api/v1/topology", None).await;
    assert_eq!(r.header("access-control-allow-origin"), None);
}

#[tokio::test]
async fn paths_outside_the_api_get_no_cors_header() {
    let http = start(&[PEER]).await;
    for path in ["/topology", "/ui/", "/"] {
        let r = get(http, path, Some(PEER)).await;
        assert_eq!(r.header("access-control-allow-origin"), None, "{path}");
    }
}

#[tokio::test]
async fn preflight_is_answered_for_peers_only() {
    let http = start(&[PEER]).await;
    let preflight = |origin: &'static str| async move {
        http_request(
            http,
            "OPTIONS",
            "/api/v1/topology",
            &[
                ("Origin", origin),
                ("Access-Control-Request-Method", "GET"),
                ("Access-Control-Request-Headers", "accept"),
            ],
        )
        .await
    };
    let ok = preflight(PEER).await;
    assert_eq!(ok.status, 204);
    assert_eq!(ok.header("access-control-allow-origin"), Some(PEER));
    assert_eq!(ok.header("access-control-allow-methods"), Some("GET"));
    assert_eq!(ok.header("access-control-allow-credentials"), None);
    let no = preflight("http://evil.example").await;
    assert_eq!(no.status, 403);
    assert_eq!(no.header("access-control-allow-origin"), None);
    // A preflight for another method is refused even from a peer.
    let post = http_request(
        http,
        "OPTIONS",
        "/api/v1/topology",
        &[("Origin", PEER), ("Access-Control-Request-Method", "POST")],
    )
    .await;
    assert_eq!(post.status, 403);
    assert_eq!(post.header("access-control-allow-origin"), None);
}

#[tokio::test]
async fn csp_connect_src_lists_exactly_the_peers() {
    let http = start(&[PEER, OTHER_PEER]).await;
    for path in ["/ui/", "/api/v1/version"] {
        let r = get(http, path, None).await;
        assert_eq!(
            connect_src(&r),
            format!("connect-src 'self' {PEER} {OTHER_PEER}"),
            "{path}"
        );
        let csp = r.header("content-security-policy").unwrap();
        assert!(csp.contains("default-src 'self'"), "{csp}");
        assert!(csp.contains("script-src 'self'"), "{csp}");
    }
}

#[tokio::test]
async fn without_peers_nothing_changes() {
    let http = start(&[]).await;
    let r = get(http, "/api/v1/topology", Some(PEER)).await;
    assert_eq!(r.header("access-control-allow-origin"), None);
    assert_eq!(connect_src(&r), "connect-src 'self'");
    let v: Value = serde_json::from_str(&get(http, "/api/v1/version", None).await.body).unwrap();
    assert_eq!(v["peers"], serde_json::json!([]));
}

#[tokio::test]
async fn the_page_learns_the_peers_from_version_and_settings() {
    let http = start(&[PEER, OTHER_PEER]).await;
    let v: Value = serde_json::from_str(&get(http, "/api/v1/version", None).await.body).unwrap();
    assert_eq!(v["peers"], serde_json::json!([PEER, OTHER_PEER]));
    let s: Value = serde_json::from_str(&get(http, "/api/v1/settings", None).await.body).unwrap();
    let peers = s["settings"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "KRAKEN_HUB_PEERS")
        .expect("KRAKEN_HUB_PEERS listed");
    assert_eq!(peers["value"], format!("{PEER},{OTHER_PEER}"));
    assert_eq!(peers["default"], "");
}
