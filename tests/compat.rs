//! Compatibility suite (ADR 0013): a raw v1 client (protocol_version 0, as every client before P0)
//! against this hub, and the v2 handshake. The suite runs in every roadmap phase.
//!
//! - v1: no `RegisterAck`, the legacy heartbeat timeout, only v1 message types.
//! - v2: a `RegisterAck` with the configured values before the echo of the first heartbeat.

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, start_hub};
use kraken_hub::config::Config;
use kraken_hub::proto::hub_client::HubClient;
use kraken_hub::proto::{
    ErrorCode, RegisterAck, RequestKind, ServerMessage, TopologyRequest, TopologyResponse,
    server_message,
};
use tonic::transport::Endpoint;

const SHORT: Duration = Duration::from_millis(1500);

fn config() -> Config {
    Config {
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

async fn expect_echo(c: &mut FakeClient, sent_at_ms: i64) {
    match next_message(c).await {
        server_message::Message::Heartbeat(h) => assert_eq!(h.sent_at_ms, sent_at_ms),
        other => panic!("expected heartbeat echo, got {other:?}"),
    }
}

/// Drains whatever arrives within `quiet` and fails on any message a v1 client cannot know.
async fn assert_only_v1_messages(c: &mut FakeClient, quiet: Duration) -> Vec<ServerMessage> {
    let mut seen = Vec::new();
    while let Ok(Some(Ok(m))) = tokio::time::timeout(quiet, c.rx.message())
        .await
        .map(|r| r.transpose())
    {
        match &m.message {
            Some(server_message::Message::Request(_))
            | Some(server_message::Message::Reply(_))
            | Some(server_message::Message::Heartbeat(_)) => {}
            Some(server_message::Message::Error(e)) => assert!(
                e.code <= ErrorCode::PermissionDenied as i32,
                "v1 session got error code {}",
                e.code
            ),
            other => panic!("v1 session got a v2 message: {other:?}"),
        }
        seen.push(m);
    }
    seen
}

async fn topology(addr: SocketAddr) -> TopologyResponse {
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    HubClient::new(channel)
        .topology(TopologyRequest {})
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test]
async fn v1_client_gets_the_echo_and_never_an_ack() {
    let addr = start_hub(config()).await;
    let mut handler = FakeClient::register(addr, "svc-h", &["h.Do"], &[]).await;
    handler.heartbeat(11).await;
    // The first thing a v1 client sees is the echo: no RegisterAck in front of it.
    expect_echo(&mut handler, 11).await;
    let mut sender = FakeClient::register(addr, "svc-s", &[], &[]).await;
    sender.heartbeat(12).await;
    expect_echo(&mut sender, 12).await;

    // A full v1 exchange: request, reply, a hub-made error, re-register, heartbeats.
    sender
        .tx
        .send(FakeClient::request(
            "r1",
            RequestKind::Command,
            "h.Do",
            "",
            0,
        ))
        .await
        .unwrap();
    match next_message(&mut handler).await {
        server_message::Message::Request(r) => assert_eq!(r.id, "r1"),
        other => panic!("expected request, got {other:?}"),
    }
    handler
        .tx
        .send(FakeClient::reply_ok("r1", b"{}"))
        .await
        .unwrap();
    sender
        .tx
        .send(FakeClient::request(
            "r2",
            RequestKind::Query,
            "nobody.Q",
            "",
            0,
        ))
        .await
        .unwrap();
    handler
        .send_register("svc-h", &["h.Do", "h.More"], &[])
        .await;
    handler.heartbeat(13).await;
    sender.heartbeat(14).await;

    let handler_saw = assert_only_v1_messages(&mut handler, Duration::from_millis(300)).await;
    let sender_saw = assert_only_v1_messages(&mut sender, Duration::from_millis(300)).await;
    assert_eq!(handler_saw.len(), 1, "{handler_saw:?}"); // the echo of 13
    assert_eq!(sender_saw.len(), 3, "{sender_saw:?}"); // reply r1, NO_HANDLER r2, echo of 14
}

#[tokio::test]
async fn v1_session_keeps_the_legacy_heartbeat_timeout_and_v2_gets_the_short_one() {
    let addr = start_hub(Config {
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_timeout: Duration::from_millis(300),
        legacy_heartbeat_timeout: Duration::from_millis(1500),
        ..config()
    })
    .await;
    let mut v1 = FakeClient::register(addr, "svc-v1", &["v1.Do"], &[]).await;
    v1.heartbeat(1).await;
    expect_echo(&mut v1, 1).await;
    let mut v2 = FakeClient::register_v2(addr, "svc-v2", &["v2.Do"], &[]).await;
    v2.heartbeat(1).await;
    expect_ack(&mut v2).await;
    expect_echo(&mut v2, 1).await;

    // Both go silent for well over the v2 timeout but under the legacy one.
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(
        v2.next_within(SHORT).await.is_none(),
        "v2 session must be evicted after its 300 ms timeout"
    );
    v1.heartbeat(2).await;
    expect_echo(&mut v1, 2).await;

    // Silent past the legacy timeout: the v1 session goes too.
    assert!(
        v1.next_within(Duration::from_millis(2500)).await.is_none(),
        "v1 session must be evicted after the legacy timeout"
    );
}

#[tokio::test]
async fn v2_client_gets_the_ack_with_configured_values_before_the_echo() {
    let addr = start_hub(Config {
        hub_id: "hub-under-test".to_string(),
        heartbeat_interval: Duration::from_millis(1500),
        heartbeat_timeout: Duration::from_secs(7),
        default_timeout: Duration::from_millis(4000),
        max_timeout: Duration::from_millis(50_000),
        timeout_overrides: [("t.Long".to_string(), Duration::from_millis(90_000))].into(),
        ..config()
    })
    .await;
    let mut c = FakeClient::register_v2(addr, "svc", &["t.Long"], &[]).await;
    c.heartbeat(77).await;

    let ack = expect_ack(&mut c).await;
    assert_eq!(
        ack,
        RegisterAck {
            hub_id: "hub-under-test".to_string(),
            protocol_version: 2,
            capabilities: vec![], // the client's made-up capability is not shared
            heartbeat_interval_ms: 1500,
            heartbeat_timeout_ms: 7000,
            default_timeout_ms: 4000,
            max_timeout_ms: 50_000,
            context: "default".to_string(),
            type_timeouts_ms: HashMap::from([("t.Long".to_string(), 90_000)]),
        }
    );
    expect_echo(&mut c, 77).await;
}

#[tokio::test]
async fn every_accepted_v2_register_is_acked_and_versions_cannot_change() {
    let addr = start_hub(config()).await;
    let mut c = FakeClient::register_v2(addr, "svc", &["a.One"], &[]).await;
    c.heartbeat(1).await;
    expect_ack(&mut c).await;
    expect_echo(&mut c, 1).await;

    // A later Register that claims v1 does not turn the session into a v1 one.
    c.send_register("svc", &["a.One", "a.Two"], &[]).await;
    c.heartbeat(2).await;
    expect_ack(&mut c).await;
    expect_echo(&mut c, 2).await;

    let t = topology(addr).await;
    let instance = &t.services[0].instances[0];
    assert_eq!(instance.protocol_version, 2);
    assert_eq!(instance.client_version, "fake-client/1.0");
    assert_eq!(instance.command_types, ["a.One", "a.Two"]);
}

#[tokio::test]
async fn topology_reports_hub_id_and_each_session_version() {
    let addr = start_hub(Config {
        hub_id: "hub-7".to_string(),
        ..config()
    })
    .await;
    let mut v1 = FakeClient::register(addr, "svc-v1", &[], &[]).await;
    v1.heartbeat(1).await;
    expect_echo(&mut v1, 1).await;
    let mut v2 = FakeClient::register_v2(addr, "svc-v2", &[], &[]).await;
    v2.heartbeat(1).await;
    expect_ack(&mut v2).await;
    expect_echo(&mut v2, 1).await;

    let t = topology(addr).await;
    assert_eq!(t.hub_id, "hub-7");
    let by_service: HashMap<_, _> = t
        .services
        .iter()
        .map(|s| (s.service_name.as_str(), &s.instances[0]))
        .collect();
    assert_eq!(by_service["svc-v1"].protocol_version, 0);
    assert_eq!(by_service["svc-v1"].client_version, "");
    assert_eq!(by_service["svc-v2"].protocol_version, 2);
    assert_eq!(by_service["svc-v2"].client_version, "fake-client/1.0");
    assert_eq!(t.heartbeat_timeout_ms, 10_000);
}

#[tokio::test]
async fn zero_timeout_from_a_v2_sender_gets_the_per_type_override() {
    let addr = start_hub(Config {
        timeout_overrides: [("t.Quick".to_string(), Duration::from_millis(200))].into(),
        ..config()
    })
    .await;
    let mut handler = FakeClient::register_v2(addr, "svc-h", &["t.Quick"], &[]).await;
    handler.heartbeat(1).await;
    expect_ack(&mut handler).await;
    expect_echo(&mut handler, 1).await;
    let mut sender = FakeClient::register_v2(addr, "svc-s", &[], &[]).await;
    sender.heartbeat(1).await;
    expect_ack(&mut sender).await;
    expect_echo(&mut sender, 1).await;

    let started = tokio::time::Instant::now();
    sender
        .tx
        .send(FakeClient::request(
            "q1",
            RequestKind::Command,
            "t.Quick",
            "",
            0,
        ))
        .await
        .unwrap();
    match next_message(&mut sender).await {
        server_message::Message::Error(e) => {
            assert_eq!(e.code(), ErrorCode::Timeout);
            assert_eq!(e.message, "no reply within 200 ms");
        }
        other => panic!("expected TIMEOUT, got {other:?}"),
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
}

/// P1 (ADR 0014): a sender that did not declare flow-control (v1, or v2 without the capability)
/// never gets RESOURCE_EXHAUSTED: a full queue answers HANDLER_ERROR "handler busy" as before, and
/// the ack of such a v2 client lists no capability.
#[tokio::test]
async fn senders_without_flow_control_never_get_resource_exhausted() {
    let addr = start_hub(Config {
        queue_capacity_per_instance: 1,
        ..config()
    })
    .await;
    let mut handler = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 1, &[]).await;
    expect_ack(&mut handler).await;
    handler.heartbeat(1).await;
    expect_echo(&mut handler, 1).await;
    let mut v2 = FakeClient::register_v2(addr, "svc-v2", &[], &[]).await;
    v2.heartbeat(1).await;
    assert!(expect_ack(&mut v2).await.capabilities.is_empty());
    expect_echo(&mut v2, 1).await;
    let mut v1 = FakeClient::register(addr, "svc-v1", &[], &[]).await;
    v1.heartbeat(1).await;
    expect_echo(&mut v1, 1).await;

    for id in ["forwarded", "queued"] {
        v2.tx
            .send(FakeClient::request(
                id,
                RequestKind::Command,
                "h.Do",
                "",
                10_000,
            ))
            .await
            .unwrap();
    }
    v2.heartbeat(2).await;
    expect_echo(&mut v2, 2).await;
    for (c, id) in [(&mut v2, "v2-full"), (&mut v1, "v1-full")] {
        c.tx.send(FakeClient::request(
            id,
            RequestKind::Command,
            "h.Do",
            "",
            10_000,
        ))
        .await
        .unwrap();
        match next_message(c).await {
            server_message::Message::Error(e) => {
                assert_eq!(e.request_id, id);
                assert_eq!(e.code(), ErrorCode::HandlerError);
                assert_eq!(e.message, "handler busy");
            }
            other => panic!("expected the busy HANDLER_ERROR, got {other:?}"),
        }
    }
    let seen = assert_only_v1_messages(&mut v1, Duration::from_millis(200)).await;
    assert!(seen.is_empty(), "{seen:?}");
}
