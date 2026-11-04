//! Multi-hub HA, hub side (ADR 0016): the latest `InstanceStatus` of a session shows in the
//! topology (gRPC and `GET /api/v1/topology`).

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, http_get, start_hub_with_http};
use kraken_hub::config::Config;
use kraken_hub::proto::hub_client::HubClient;
use kraken_hub::proto::{RegisterAck, TopologyRequest, server_message};
use kraken_hub::topology::{InstanceTopology, Topology};
use tonic::transport::Endpoint;

const SHORT: Duration = Duration::from_millis(1500);

fn config() -> Config {
    Config {
        hub_id: "hub-a".to_string(),
        otlp_endpoint: None,
        ..Config::default()
    }
}

async fn next_message(c: &mut FakeClient) -> server_message::Message {
    c.next_within(SHORT)
        .await
        .expect("stream ended")
        .expect("stream failed")
        .message
        .expect("empty server message")
}

async fn expect_ack(c: &mut FakeClient) -> RegisterAck {
    match next_message(c).await {
        server_message::Message::RegisterAck(ack) => ack,
        other => panic!("expected RegisterAck, got {other:?}"),
    }
}

/// Sends a heartbeat and waits for its echo: everything sent before it has been handled.
async fn sync(c: &mut FakeClient, n: i64) {
    c.heartbeat(n).await;
    loop {
        match next_message(c).await {
            server_message::Message::Heartbeat(h) if h.sent_at_ms == n => return,
            server_message::Message::Heartbeat(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
}

async fn rpc_instance(grpc: SocketAddr, service: &str) -> InstanceTopology {
    let channel = Endpoint::from_shared(format!("http://{grpc}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let t = HubClient::new(channel)
        .topology(TopologyRequest {})
        .await
        .unwrap()
        .into_inner();
    instance_of(&Topology::from_proto(&t), service)
}

async fn http_json(http: SocketAddr, path: &str) -> serde_json::Value {
    let (status, body) = http_get(http, path).await;
    assert_eq!(status, 200, "{path}: {body}");
    serde_json::from_str(&body).unwrap()
}

fn instance_of(t: &Topology, service: &str) -> InstanceTopology {
    t.services
        .iter()
        .find(|s| s.service_name == service)
        .unwrap_or_else(|| panic!("{service} missing"))
        .instances[0]
        .clone()
}

fn json_instance<'a>(t: &'a serde_json::Value, service: &str) -> &'a serde_json::Value {
    t["services"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["serviceName"] == service)
        .map(|s| &s["instances"][0])
        .unwrap_or_else(|| panic!("{service} missing in {t}"))
}

#[tokio::test]
async fn a_client_reports_status_and_topology_shows_it() {
    let (grpc, http) = start_hub_with_http(config()).await;
    let http = http.unwrap();
    let mut c = FakeClient::register_no_wait(grpc, "svc", &["a.Do"], &[]).await;
    let ack = expect_ack(&mut c).await;
    assert_eq!(ack.hub_id, "hub-a");

    // Registered, nothing reported yet.
    sync(&mut c, 1).await;
    let before = rpc_instance(grpc, "svc").await;
    assert!(before.connected_hub_ids.is_empty());
    assert_eq!(before.status_age_ms, -1);

    c.status(&["hub-a", "hub-b"], 3).await;
    sync(&mut c, 2).await;
    let after = rpc_instance(grpc, "svc").await;
    assert_eq!(after.connected_hub_ids, ["hub-a", "hub-b"]);
    assert!(
        (0..2000).contains(&after.status_age_ms),
        "{}",
        after.status_age_ms
    );

    let t = http_json(http, "/api/v1/topology").await;
    let i = json_instance(&t, "svc");
    assert_eq!(i["connectedHubIds"], serde_json::json!(["hub-a", "hub-b"]));
    assert!(i["statusAgeMs"].as_i64().unwrap() >= 0, "{i}");

    // The next report replaces the previous one.
    c.status(&["hub-a"], 0).await;
    sync(&mut c, 3).await;
    assert_eq!(rpc_instance(grpc, "svc").await.connected_hub_ids, ["hub-a"]);

    // A new stream of the same instance starts without a status.
    let mut again = FakeClient::connect(grpc).await;
    again.instance_id = c.instance_id.clone();
    again.send_register("svc", &["a.Do"], &[]).await;
    expect_ack(&mut again).await;
    sync(&mut again, 4).await;
    let fresh = rpc_instance(grpc, "svc").await;
    assert!(fresh.connected_hub_ids.is_empty());
    assert_eq!(fresh.status_age_ms, -1);
}
