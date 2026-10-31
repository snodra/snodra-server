//! Flow control end to end (ADR 0014): permits, per-instance queues, priority, capacity
//! rejections by sender capability, and unlimited v1 handlers.

mod common;

use std::time::Duration;

use common::{FakeClient, start_hub};
use kraken_hub::config::Config;
use kraken_hub::proto::{
    ClientMessage, ErrorCode, Request, RequestKind, client_message, server_message,
};

const SHORT: Duration = Duration::from_millis(1500);

fn config() -> Config {
    Config {
        otlp_endpoint: None,
        ..Config::default()
    }
}

async fn message(c: &mut FakeClient) -> server_message::Message {
    c.next_within(SHORT)
        .await
        .unwrap()
        .unwrap()
        .message
        .unwrap()
}

/// Sends a heartbeat and skips everything up to its echo (ack included).
async fn handshake(c: &mut FakeClient, n: i64) {
    c.heartbeat(n).await;
    loop {
        if let server_message::Message::Heartbeat(h) = message(c).await
            && h.sent_at_ms == n
        {
            return;
        }
    }
}

async fn expect_request(c: &mut FakeClient) -> Request {
    match message(c).await {
        server_message::Message::Request(r) => r,
        other => panic!("expected a request, got {other:?}"),
    }
}

async fn nothing_within(c: &mut FakeClient, d: Duration) {
    if let Ok(m) = tokio::time::timeout(d, c.rx.message()).await {
        panic!("expected nothing, got {m:?}");
    }
}

fn request(id: &str, priority: i32) -> ClientMessage {
    let mut m = FakeClient::request(id, RequestKind::Command, "h.Do", "", 10_000);
    if let Some(client_message::Message::Request(r)) = m.message.as_mut() {
        r.priority = priority;
    }
    m
}

#[tokio::test]
async fn one_permit_serialises_three_concurrent_requests() {
    let addr = start_hub(config()).await;
    let mut handler = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 1, &[]).await;
    match message(&mut handler).await {
        server_message::Message::RegisterAck(ack) => {
            assert_eq!(ack.capabilities, ["flow-control"]);
        }
        other => panic!("expected the ack, got {other:?}"),
    }
    handshake(&mut handler, 1).await;
    let mut sender = FakeClient::register_flow(addr, "svc-s", &[], 0, &[]).await;
    handshake(&mut sender, 1).await;

    for id in ["r1", "r2", "r3"] {
        sender.tx.send(request(id, 0)).await.unwrap();
    }
    for id in ["r1", "r2", "r3"] {
        assert_eq!(expect_request(&mut handler).await.id, id);
        nothing_within(&mut handler, Duration::from_millis(150)).await;
        handler
            .tx
            .send(FakeClient::reply_ok(id, b"{}"))
            .await
            .unwrap();
        handler.permits(1).await;
        match message(&mut sender).await {
            server_message::Message::Reply(r) => assert_eq!(r.request_id, id),
            other => panic!("expected the reply to {id}, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn queued_requests_leave_by_priority_then_age() {
    let addr = start_hub(config()).await;
    let mut handler = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 1, &[]).await;
    handshake(&mut handler, 1).await;
    let mut sender = FakeClient::register_flow(addr, "svc-s", &[], 0, &[]).await;
    handshake(&mut sender, 1).await;
    sender.tx.send(request("blocker", 0)).await.unwrap();
    assert_eq!(expect_request(&mut handler).await.id, "blocker");
    for (id, p) in [("low", 0), ("high", 9), ("mid", 3), ("high-2", 9)] {
        sender.tx.send(request(id, p)).await.unwrap();
    }
    // Round trip on the sender's stream: the four requests were routed (queued) by now.
    handshake(&mut sender, 2).await;
    // One permit back per finished request, like a real client (permits never exceed the grant).
    let mut got = Vec::new();
    for _ in 0..4 {
        handler.permits(1).await;
        got.push(expect_request(&mut handler).await.id);
    }
    assert_eq!(got, ["high", "high-2", "mid", "low"]);
}

#[tokio::test]
async fn a_v1_handler_stays_unlimited() {
    let addr = start_hub(config()).await;
    let mut handler = FakeClient::register(addr, "svc-h", &["h.Do"], &[]).await;
    handshake(&mut handler, 1).await;
    let mut sender = FakeClient::register_flow(addr, "svc-s", &[], 0, &[]).await;
    handshake(&mut sender, 1).await;
    for i in 0..5 {
        sender.tx.send(request(&format!("r{i}"), 0)).await.unwrap();
    }
    for i in 0..5 {
        assert_eq!(expect_request(&mut handler).await.id, format!("r{i}"));
    }
}

#[tokio::test]
async fn a_full_queue_is_resource_exhausted_for_flow_senders_and_busy_for_v1_senders() {
    let addr = start_hub(Config {
        queue_capacity_per_instance: 1,
        ..config()
    })
    .await;
    let mut handler = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 1, &[]).await;
    handshake(&mut handler, 1).await;
    let mut v2 = FakeClient::register_flow(addr, "svc-s2", &[], 0, &[]).await;
    handshake(&mut v2, 1).await;
    let mut v1 = FakeClient::register(addr, "svc-s1", &[], &[]).await;
    handshake(&mut v1, 1).await;

    v2.tx.send(request("forwarded", 0)).await.unwrap();
    assert_eq!(expect_request(&mut handler).await.id, "forwarded");
    v2.tx.send(request("queued", 0)).await.unwrap();
    handshake(&mut v2, 2).await;

    v2.tx.send(request("v2-rejected", 0)).await.unwrap();
    match message(&mut v2).await {
        server_message::Message::Error(e) => {
            assert_eq!(
                (e.request_id.as_str(), e.code()),
                ("v2-rejected", ErrorCode::ResourceExhausted)
            );
        }
        other => panic!("expected RESOURCE_EXHAUSTED, got {other:?}"),
    }
    v1.tx.send(request("v1-rejected", 0)).await.unwrap();
    match message(&mut v1).await {
        server_message::Message::Error(e) => {
            assert_eq!(e.request_id, "v1-rejected");
            assert_eq!(e.code(), ErrorCode::HandlerError);
            assert_eq!(e.message, "handler busy");
        }
        other => panic!("expected HANDLER_ERROR, got {other:?}"),
    }
}

/// The next request `c` gets, skipping acks and heartbeat echoes.
async fn next_request(c: &mut FakeClient) -> Request {
    loop {
        if let server_message::Message::Request(r) = message(c).await {
            return r;
        }
    }
}

async fn expect_error(c: &mut FakeClient) -> (String, ErrorCode) {
    match message(c).await {
        server_message::Message::Error(e) => (e.request_id.clone(), e.code()),
        other => panic!("expected an error, got {other:?}"),
    }
}

/// Only the forwarded request's outcome is unknown; the queued one never left the hub and,
/// with no other instance to take it, is NO_HANDLER (retryable for commands too).
#[tokio::test]
async fn a_disconnecting_handler_fails_what_it_had_and_its_queue_finds_no_handler() {
    let addr = start_hub(config()).await;
    let mut handler = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 1, &[]).await;
    handshake(&mut handler, 1).await;
    let mut sender = FakeClient::register_flow(addr, "svc-s", &[], 0, &[]).await;
    handshake(&mut sender, 1).await;
    sender.tx.send(request("forwarded", 0)).await.unwrap();
    sender.tx.send(request("queued", 0)).await.unwrap();
    assert_eq!(expect_request(&mut handler).await.id, "forwarded");
    drop(handler);
    let mut failed = vec![
        expect_error(&mut sender).await,
        expect_error(&mut sender).await,
    ];
    failed.sort();
    assert_eq!(
        failed,
        [
            ("forwarded".to_string(), ErrorCode::HandlerDisconnected),
            ("queued".to_string(), ErrorCode::NoHandler)
        ]
    );
}

#[tokio::test]
async fn queued_commands_survive_a_reconnect_of_their_handler() {
    let addr = start_hub(config()).await;
    let mut old = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 1, &[]).await;
    handshake(&mut old, 1).await;
    let mut sender = FakeClient::register_flow(addr, "svc-s", &[], 0, &[]).await;
    handshake(&mut sender, 1).await;
    for id in ["forwarded", "q1", "q2"] {
        sender.tx.send(request(id, 0)).await.unwrap();
    }
    assert_eq!(expect_request(&mut old).await.id, "forwarded");
    handshake(&mut sender, 2).await; // all three routed
    // A new stream with the same instance id replaces the old one.
    let mut new = FakeClient::register_flow_as(addr, &old.instance_id, "svc-h", &["h.Do"], 1).await;
    assert_eq!(
        expect_error(&mut sender).await,
        ("forwarded".to_string(), ErrorCode::HandlerDisconnected)
    );
    assert_eq!(next_request(&mut new).await.id, "q1");
    new.permits(1).await;
    assert_eq!(next_request(&mut new).await.id, "q2");
    nothing_within(&mut sender, Duration::from_millis(150)).await;
}

#[tokio::test]
async fn a_rolling_restart_hands_queued_commands_over_and_the_stopping_instance_draws_nothing() {
    let addr = start_hub(config()).await;
    let mut a = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 1, &[]).await;
    handshake(&mut a, 1).await;
    let mut sender = FakeClient::register_flow(addr, "svc-s", &[], 0, &[]).await;
    handshake(&mut sender, 1).await;
    for id in ["on-a", "q1", "q2"] {
        sender.tx.send(request(id, 0)).await.unwrap();
    }
    assert_eq!(expect_request(&mut a).await.id, "on-a");
    handshake(&mut sender, 2).await;
    let mut b = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 1, &[]).await;
    handshake(&mut b, 1).await;

    // A stops gracefully: an empty Register, then it finishes its request.
    a.send_register_flow("svc-h", &[], 1).await;
    handshake(&mut a, 2).await;
    assert_eq!(next_request(&mut b).await.id, "q1", "A's queue moves to B");
    a.tx.send(FakeClient::reply_ok("on-a", b"{}"))
        .await
        .unwrap();
    a.permits(1).await;
    match message(&mut sender).await {
        server_message::Message::Reply(r) => assert_eq!(r.request_id, "on-a"),
        other => panic!("expected the reply, got {other:?}"),
    }
    for id in ["new-1", "new-2"] {
        sender.tx.send(request(id, 0)).await.unwrap();
    }
    handshake(&mut sender, 3).await;
    nothing_within(&mut a, Duration::from_millis(200)).await;
    let mut on_b = Vec::new();
    for _ in 0..3 {
        b.permits(1).await;
        on_b.push(next_request(&mut b).await.id);
    }
    assert_eq!(on_b, ["q2", "new-1", "new-2"]);
}

#[tokio::test]
async fn unkeyed_requests_prefer_the_instance_with_matching_tags() {
    let addr = start_hub(config()).await;
    let mut z1 = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 0, &[("zone", "z1")]).await;
    handshake(&mut z1, 1).await;
    let mut z2 = FakeClient::register_flow(addr, "svc-h", &["h.Do"], 0, &[("zone", "z2")]).await;
    handshake(&mut z2, 1).await;
    let mut sender = FakeClient::register_flow(addr, "svc-s", &[], 0, &[("zone", "z2")]).await;
    handshake(&mut sender, 1).await;
    for i in 0..4 {
        sender.tx.send(request(&format!("r{i}"), 0)).await.unwrap();
    }
    for i in 0..4 {
        assert_eq!(expect_request(&mut z2).await.id, format!("r{i}"));
    }
    nothing_within(&mut z1, Duration::from_millis(150)).await;
}
