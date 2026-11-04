//! Integration tests for the HTTP JSON mirror (`GET /topology`).

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, http_get, start_hub_with_http};
use kraken_hub::config::Config;
use kraken_hub::proto::TopologyRequest;
use kraken_hub::proto::hub_client::HubClient;
use kraken_hub::topology::Topology;
use tonic::transport::Endpoint;

fn config() -> Config {
    Config {
        otlp_endpoint: None,
        ..Config::default()
    }
}

async fn rpc_topology(addr: SocketAddr) -> Topology {
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let response = tokio::time::timeout(
        Duration::from_secs(3),
        HubClient::new(channel).topology(TopologyRequest {}),
    )
    .await
    .expect("topology call timed out")
    .unwrap()
    .into_inner();
    Topology::from_proto(&response)
}

/// Zeroes the fields that differ between two snapshots taken at different instants.
fn normalize(t: &mut Topology) {
    t.now_ms = 0;
    for s in &mut t.services {
        for i in &mut s.instances {
            i.last_heartbeat_age_ms = 0;
        }
    }
}

#[tokio::test]
async fn get_topology_returns_json_equal_to_rpc() {
    let (grpc, http) = start_hub_with_http(config()).await;
    let http = http.expect("http listener");
    let _a = FakeClient::register(grpc, "svc-a", &["a.Do"], &["a.Get"]).await;
    let _b = FakeClient::register(grpc, "svc-b", &["b.Do"], &[]).await;

    // Wait until both registrations are visible.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while rpc_topology(grpc).await.services.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "registration timed out"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let (status, body) = http_get(http, "/api/v1/topology").await;
    assert_eq!(status, 200);
    let mut from_http: Topology = serde_json::from_str(&body).expect("valid topology JSON");
    let mut from_rpc = rpc_topology(grpc).await;
    normalize(&mut from_http);
    normalize(&mut from_rpc);
    assert_eq!(from_http, from_rpc);
    assert_eq!(from_http.services.len(), 2);
}

#[tokio::test]
async fn unknown_path_is_404() {
    let (_grpc, http) = start_hub_with_http(config()).await;
    let http = http.expect("http listener");
    let (status, _) = http_get(http, "/nope").await;
    assert_eq!(status, 404);
}

#[tokio::test]
async fn hub_runs_without_http_listener() {
    let (grpc, http) = start_hub_with_http(Config {
        http_listen: None,
        ..config()
    })
    .await;
    assert!(http.is_none());
    let _a = FakeClient::register(grpc, "svc-a", &["a.Do"], &[]).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while rpc_topology(grpc).await.services.is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "registration timed out"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
