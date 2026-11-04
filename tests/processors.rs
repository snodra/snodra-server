//! Event processors, hub side (ADR 0022): a client reports its Kafka event processors in
//! `InstanceStatus.processors`; the hub keeps the latest report per session (bounded, never an
//! error), shows it in the topology (gRPC, `/api/v1/topology`) and aggregates it per service in
//! `/api/v1/processors`.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, http_get, start_hub_with_http};
use kraken_hub::config::Config;
use kraken_hub::proto::hub_client::HubClient;
use kraken_hub::proto::{
    PartitionStatus, ProcessorState, ProcessorStatus, RegisterAck, TopologyRequest, server_message,
};
use kraken_hub::topology::Topology;
use serde_json::{Value, json};
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

/// Sends a heartbeat and waits for its echo: everything sent before it has been handled, and the
/// stream is still open.
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

async fn rpc_topology(grpc: SocketAddr) -> Topology {
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
    Topology::from_proto(&t)
}

async fn http_json(http: SocketAddr, path: &str) -> Value {
    let (status, body) = http_get(http, path).await;
    assert_eq!(status, 200, "{path}: {body}");
    serde_json::from_str(&body).unwrap()
}

fn part(partition: i32, position: i64, committed: i64, high_watermark: i64) -> PartitionStatus {
    PartitionStatus {
        partition,
        position,
        committed,
        high_watermark,
        lag: if high_watermark < 0 {
            -1
        } else if committed >= 0 {
            high_watermark - committed
        } else {
            high_watermark - position
        },
        replay_until: 0,
    }
}

fn processor(
    name: &str,
    state: ProcessorState,
    partitions: Vec<PartitionStatus>,
) -> ProcessorStatus {
    ProcessorStatus {
        name: name.to_string(),
        group_id: if name == "default" {
            "service-b".to_string()
        } else {
            format!("service-b.{name}")
        },
        topic: "kraken.events".to_string(),
        state: state as i32,
        partitions,
        dead_letter_mode: "collection".to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_client_reports_processors_and_every_view_shows_them() {
    let (grpc, http) = start_hub_with_http(config()).await;
    let http = http.unwrap();
    let mut b1 = FakeClient::register(grpc, "service-b", &["b.Do"], &[]).await;
    let mut b2 = FakeClient::register(grpc, "service-b", &["b.Do"], &[]).await;

    let mut failing = processor(
        "orders",
        ProcessorState::ProcessorFailing,
        vec![part(1, 10, 8, 20)],
    );
    failing.failed_total = 1;
    failing.dead_letters = 2;
    failing.last_error = "handler failed: boom".to_string();
    failing.last_error_at_ms = 1_700_000_000_000;
    failing.event_types = vec!["examples.OrderPlaced".to_string()];
    let mut default = processor(
        "default",
        ProcessorState::ProcessorRunning,
        vec![part(0, 5, 5, 5), part(2, -1, -1, -1)],
    );
    default.processed_total = 5;
    default.event_types = vec!["examples.Greeted".to_string()];
    b1.status_with_processors(&["hub-a"], vec![default.clone(), failing.clone()])
        .await;
    let mut other = processor(
        "orders",
        ProcessorState::ProcessorRunning,
        vec![part(0, 3, 3, 4)],
    );
    other.dead_letters = 2;
    b2.status_with_processors(&["hub-a"], vec![other]).await;
    sync(&mut b1, 1).await;
    sync(&mut b2, 2).await;

    // gRPC topology: the report as kept.
    let t = rpc_topology(grpc).await;
    let svc = &t.services[0];
    let i1 = svc
        .instances
        .iter()
        .find(|i| i.instance_id == b1.instance_id)
        .unwrap();
    assert_eq!(
        i1.processors
            .iter()
            .map(|p| p.name.as_str())
            .collect::<Vec<_>>(),
        ["default", "orders"]
    );
    assert_eq!(i1.processors[1].last_error, "handler failed: boom");

    // `/api/v1/topology`: the same, as JSON.
    let t = http_json(http, "/api/v1/topology").await;
    let instances = t["services"][0]["instances"].as_array().unwrap();
    let i1 = instances
        .iter()
        .find(|i| i["instanceId"] == b1.instance_id.as_str())
        .unwrap();
    assert_eq!(i1["processors"][1]["name"], "orders");
    assert_eq!(i1["processors"][1]["state"], "FAILING");
    assert_eq!(
        i1["processors"][0]["partitions"][1],
        json!({"partition": 2, "position": -1, "committed": -1, "highWatermark": -1, "lag": -1, "replayUntil": 0})
    );

    // `/api/v1/processors`: per service and processor, aggregated over the instances.
    let v = http_json(http, "/api/v1/processors").await;
    assert_eq!(v["hubId"], "hub-a");
    assert!(v["nowMs"].as_i64().unwrap() > 0);
    let services = v["services"].as_array().unwrap();
    assert_eq!(services.len(), 1);
    assert_eq!(services[0]["serviceName"], "service-b");
    assert_eq!(services[0]["deadLetters"], 2);
    let processors = services[0]["processors"].as_array().unwrap();
    let default = &processors[0];
    assert_eq!(default["name"], "default");
    assert_eq!(default["groupId"], "service-b");
    assert_eq!(default["state"], "RUNNING");
    assert_eq!(default["instancesReporting"], 1);
    assert_eq!(default["assignedPartitions"], 2);
    assert_eq!(default["lag"], 0, "partition 2's unknown lag is left out");
    assert_eq!(default["processedTotal"], 5);
    assert_eq!(default["lastError"], Value::Null);
    let orders = &processors[1];
    assert_eq!(orders["name"], "orders");
    assert_eq!(orders["groupId"], "service-b.orders");
    assert_eq!(orders["topic"], "kraken.events");
    assert_eq!(orders["deadLetterMode"], "collection");
    assert_eq!(
        orders["state"], "FAILING",
        "the worst of FAILING and RUNNING"
    );
    assert_eq!(orders["instancesReporting"], 2);
    assert_eq!(orders["assignedPartitions"], 2);
    assert_eq!(orders["lag"], 13, "12 on partition 1 plus 1 on partition 0");
    assert_eq!(orders["deadLetters"], 2, "the max, not the sum");
    assert_eq!(orders["failedTotal"], 1);
    assert_eq!(orders["eventTypes"], json!(["examples.OrderPlaced"]));
    assert_eq!(
        orders["lastError"],
        json!({"message": "handler failed: boom", "atMs": 1_700_000_000_000i64, "instanceId": b1.instance_id})
    );
    let members = orders["instances"].as_array().unwrap();
    assert_eq!(members.len(), 2);
    for m in members {
        assert!(m["statusAgeMs"].as_i64().unwrap() >= 0, "{m}");
        assert!(m["partitions"].is_array(), "{m}");
        assert!(m["state"].is_string(), "{m}");
    }
}

#[tokio::test]
async fn an_oversized_report_is_cut_and_the_stream_stays_open() {
    let (grpc, http) = start_hub_with_http(config()).await;
    let http = http.unwrap();
    let mut c = FakeClient::register(grpc, "service-b", &[], &[]).await;
    let mut many: Vec<ProcessorStatus> = (0..40)
        .map(|i| {
            processor(
                &format!("p{i:02}"),
                ProcessorState::ProcessorRunning,
                vec![],
            )
        })
        .collect();
    many[0].partitions = (0..1100).map(|p| part(p, 1, 1, 1)).collect();
    many[0].event_types = (0..300).map(|i| format!("t.E{i:03}")).collect();
    many[0].last_error = "é".repeat(400);
    many[0].name = "n".repeat(80);
    c.status_with_processors(&["hub-a"], many).await;
    sync(&mut c, 1).await;
    let t = rpc_topology(grpc).await;
    let kept = &t.services[0].instances[0].processors;
    assert_eq!(kept.len(), 32);
    assert_eq!(kept[0].name, "n".repeat(64));
    assert_eq!(kept[0].partitions.len(), 1024);
    assert_eq!(kept[0].event_types.len(), 256);
    assert_eq!(
        kept[0].last_error,
        "é".repeat(256),
        "512 bytes of two-byte characters"
    );
    let v = http_json(http, "/api/v1/processors").await;
    assert_eq!(v["services"][0]["processors"].as_array().unwrap().len(), 32);
}

#[tokio::test]
async fn a_new_report_replaces_the_list_and_a_reconnect_starts_without_one() {
    let (grpc, http) = start_hub_with_http(config()).await;
    let http = http.unwrap();
    let mut c = FakeClient::register(grpc, "service-b", &[], &[]).await;
    c.status_with_processors(
        &["hub-a"],
        vec![
            processor("default", ProcessorState::ProcessorStarting, vec![]),
            processor("orders", ProcessorState::ProcessorStarting, vec![]),
        ],
    )
    .await;
    sync(&mut c, 1).await;
    c.status_with_processors(
        &["hub-a"],
        vec![processor(
            "default",
            ProcessorState::ProcessorStopped,
            vec![],
        )],
    )
    .await;
    sync(&mut c, 2).await;
    let v = http_json(http, "/api/v1/processors").await;
    let processors = v["services"][0]["processors"].as_array().unwrap();
    assert_eq!(processors.len(), 1, "replaced, not merged: {v}");
    assert_eq!(processors[0]["state"], "STOPPED");

    // The same instance id on a new stream: no report until it sends one.
    let id = c.instance_id.clone();
    drop(c);
    let mut again = FakeClient::connect(grpc).await;
    again.instance_id = id;
    again.send_register("service-b", &[], &[]).await;
    expect_ack(&mut again).await;
    sync(&mut again, 3).await;
    let v = http_json(http, "/api/v1/processors").await;
    assert_eq!(v["services"], json!([]), "{v}");
}
