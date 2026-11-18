//! Event processors, hub side (ADR 0022, 0027): a client reports its event processors in
//! `InstanceStatus.processors`; the hub keeps the latest report per session (bounded, never an
//! error), shows it in the topology (gRPC, `/api/v1/topology`) and aggregates it per service in
//! `/api/v1/processors`.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, http_get, start_hub_with_http};
use serde_json::{Value, json};
use snodra_server::config::Config;
use snodra_server::proto::hub_client::HubClient;
use snodra_server::proto::{
    ProcessorState, ProcessorStatus, RegisterAck, SegmentStatus, TopologyRequest, server_message,
};
use snodra_server::topology::Topology;
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

/// A segment with decimal positions (as the first-wave event stores report them); -1 is unknown
/// (an empty position on the wire).
fn part(segment: i32, position: i64, committed: i64, head: i64) -> SegmentStatus {
    let text = |v: i64| if v < 0 { String::new() } else { v.to_string() };
    SegmentStatus {
        segment: segment.to_string(),
        position: text(position),
        committed: text(committed),
        head: text(head),
        lag: if head < 0 {
            -1
        } else if committed >= 0 {
            head - committed
        } else {
            head - position
        },
        replay_until: String::new(),
    }
}

fn processor(name: &str, state: ProcessorState, segments: Vec<SegmentStatus>) -> ProcessorStatus {
    ProcessorStatus {
        name: name.to_string(),
        subscription: if name == "default" {
            "service-b".to_string()
        } else {
            format!("service-b.{name}")
        },
        source: "snodra.events".to_string(),
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
        i1["processors"][0]["segments"][1],
        json!({"segment": "2", "position": "", "committed": "", "head": "", "lag": -1, "replayUntil": ""})
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
    assert_eq!(default["subscription"], "service-b");
    assert_eq!(default["state"], "RUNNING");
    assert_eq!(default["instancesReporting"], 1);
    assert_eq!(default["assignedSegments"], 2);
    assert_eq!(default["lag"], 0, "segment 2's unknown lag is left out");
    assert_eq!(default["processedTotal"], 5);
    assert_eq!(default["lastError"], Value::Null);
    let orders = &processors[1];
    assert_eq!(orders["name"], "orders");
    assert_eq!(orders["subscription"], "service-b.orders");
    assert_eq!(orders["source"], "snodra.events");
    assert_eq!(orders["deadLetterMode"], "collection");
    assert_eq!(
        orders["state"], "FAILING",
        "the worst of FAILING and RUNNING"
    );
    assert_eq!(orders["instancesReporting"], 2);
    assert_eq!(orders["assignedSegments"], 2);
    assert_eq!(orders["lag"], 13, "12 on segment 1 plus 1 on segment 0");
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
        assert!(m["segments"].is_array(), "{m}");
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
    many[0].segments = (0..1100).map(|p| part(p, 1, 1, 1)).collect();
    many[0].event_types = (0..300).map(|i| format!("t.E{i:03}")).collect();
    many[0].last_error = "é".repeat(400);
    many[0].name = "n".repeat(80);
    c.status_with_processors(&["hub-a"], many).await;
    sync(&mut c, 1).await;
    let t = rpc_topology(grpc).await;
    let kept = &t.services[0].instances[0].processors;
    assert_eq!(kept.len(), 32);
    assert_eq!(kept[0].name, "n".repeat(64));
    assert_eq!(kept[0].segments.len(), 1024);
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

/// The neutral status (ADR 0027): segments named and positioned by opaque strings (sorted with
/// decimal names by value), the subscription, the source, the event store and its capabilities,
/// bounded as the proto says (event_store 64 bytes, at most 16 capabilities of 64 bytes, cut at a
/// UTF-8 boundary), in the topology and in the per-service aggregate.
#[tokio::test]
async fn processor_status_reports_segments_and_capabilities() {
    let (grpc, http) = start_hub_with_http(config()).await;
    let http = http.unwrap();
    let mut c = FakeClient::register(grpc, "service-b", &[], &[]).await;
    let segment =
        |name: &str, position: &str, head: &str, lag: i64, replay_until: &str| SegmentStatus {
            segment: name.to_string(),
            position: position.to_string(),
            committed: position.to_string(),
            head: head.to_string(),
            lag,
            replay_until: replay_until.to_string(),
        };
    let streams = ProcessorStatus {
        name: "orders".to_string(),
        subscription: "service-b.orders".to_string(),
        source: "snodra.events".to_string(),
        event_store: "rabbitmq-streams".to_string(),
        capabilities: vec!["replay".to_string(), "lag".to_string()],
        state: ProcessorState::ProcessorRunning as i32,
        segments: vec![
            segment("snodra.events-10", "5", "9", 4, ""),
            segment("snodra.events-2", "3:abc", "", -1, "7"),
        ],
        replaying: true,
        dead_letter_mode: "event-store".to_string(),
        ..Default::default()
    };
    let mut numbered = processor(
        "default",
        ProcessorState::ProcessorRunning,
        vec![part(10, 1, 1, 2), part(2, 1, 1, 3)],
    );
    numbered.event_store = format!("{}é", "k".repeat(63));
    numbered.capabilities = (0..20).map(|i| format!("cap-{i:02}")).collect();
    numbered.capabilities.push(format!("{}x", "c".repeat(64)));
    c.status_with_processors(&["hub-a"], vec![streams, numbered])
        .await;
    sync(&mut c, 1).await;

    let t = rpc_topology(grpc).await;
    let kept = &t.services[0].instances[0].processors;
    let default = kept.iter().find(|p| p.name == "default").unwrap();
    assert_eq!(default.event_store, "k".repeat(63), "cut before the é");
    assert_eq!(default.capabilities.len(), 16, "the first 16 reported");
    assert_eq!(default.capabilities[0], "cap-00");
    assert_eq!(
        default
            .segments
            .iter()
            .map(|s| s.segment.as_str())
            .collect::<Vec<_>>(),
        ["2", "10"],
        "decimal segments by value"
    );

    let v = http_json(http, "/api/v1/processors").await;
    let processors = v["services"][0]["processors"].as_array().unwrap();
    let orders = processors.iter().find(|p| p["name"] == "orders").unwrap();
    assert_eq!(orders["eventStore"], "rabbitmq-streams");
    assert_eq!(orders["capabilities"], json!(["lag", "replay"]));
    assert_eq!(orders["subscription"], "service-b.orders");
    assert_eq!(orders["source"], "snodra.events");
    assert_eq!(orders["deadLetterMode"], "event-store");
    assert_eq!(orders["assignedSegments"], 2);
    assert_eq!(orders["lag"], 4, "the unknown lag is left out");
    assert_eq!(orders["replaying"], true);
    assert_eq!(
        orders["instances"][0]["segments"],
        json!([
            {"segment": "snodra.events-10", "position": "5", "committed": "5", "head": "9", "lag": 4, "replayUntil": ""},
            {"segment": "snodra.events-2", "position": "3:abc", "committed": "3:abc", "head": "", "lag": -1, "replayUntil": "7"},
        ]),
        "positions are opaque text; names that are not decimal sort by their bytes"
    );
    let default = processors.iter().find(|p| p["name"] == "default").unwrap();
    assert_eq!(default["eventStore"], "k".repeat(63));
    assert_eq!(default["capabilities"].as_array().unwrap().len(), 16);

    // A report without an event store (a library that does not say) is kept as it came.
    c.status_with_processors(
        &["hub-a"],
        vec![ProcessorStatus {
            name: "bare".to_string(),
            ..Default::default()
        }],
    )
    .await;
    sync(&mut c, 2).await;
    let v = http_json(http, "/api/v1/processors").await;
    let bare = &v["services"][0]["processors"][0];
    assert_eq!(
        (bare["eventStore"].clone(), bare["capabilities"].clone()),
        (json!(""), json!([]))
    );
}
