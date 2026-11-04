//! Streaming queries end to end (ADR 0017), over real gRPC with raw clients: credits, endings,
//! cancels, a handler's permit, shared types and the refusals.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, PERMITS, start_hub};
use kraken_hub::config::Config;
use kraken_hub::proto::{
    ClientMessage, ErrorCode, Register, ReplyEnd, ReplyPart, Request, RequestKind, RequestMode,
    client_message, server_message,
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

/// Sends a heartbeat and returns everything up to its echo (an ack included).
async fn handshake(c: &mut FakeClient, n: i64) -> Vec<server_message::Message> {
    c.heartbeat(n).await;
    let mut before = Vec::new();
    loop {
        match message(c).await {
            server_message::Message::Heartbeat(h) if h.sent_at_ms == n => return before,
            other => before.push(other),
        }
    }
}

async fn nothing_within(c: &mut FakeClient, d: Duration) {
    if let Ok(m) = tokio::time::timeout(d, c.rx.message()).await {
        panic!("expected nothing, got {m:?}");
    }
}

/// A client handling `queries` (exclusive) and `shared` queries, with `initial_permits`; does
/// not wait for the ack.
async fn register(
    addr: SocketAddr,
    service: &str,
    queries: &[&str],
    shared: &[&str],
    initial_permits: u32,
) -> FakeClient {
    let client = FakeClient::connect(addr).await;
    client
        .send_raw_register(Register {
            shared_query_types: shared.iter().map(|s| s.to_string()).collect(),
            initial_permits,
            ..client.register_message(service, &[], queries)
        })
        .await;
    client
}

/// A handler of the exclusive query `exp.Stream` and a sender, both handshaken.
async fn pair(addr: SocketAddr) -> (FakeClient, FakeClient) {
    let mut handler = register(addr, "svc-h", &["exp.Stream"], &[], PERMITS).await;
    handshake(&mut handler, 1).await;
    let mut sender = register(addr, "svc-web", &[], &[], PERMITS).await;
    handshake(&mut sender, 1).await;
    (handler, sender)
}

fn stream(
    id: &str,
    type_name: &str,
    timeout_ms: u32,
    credit: u32,
    max_results: u32,
) -> ClientMessage {
    let mut m = FakeClient::request(id, RequestKind::Query, type_name, "", timeout_ms);
    if let Some(client_message::Message::Request(r)) = m.message.as_mut() {
        r.mode = RequestMode::Streaming as i32;
        r.initial_credit = credit;
        r.max_results = max_results;
        r.payload = b"{\"q\":\"export\"}".to_vec();
    }
    m
}

async fn expect_request(c: &mut FakeClient) -> Request {
    match message(c).await {
        server_message::Message::Request(r) => r,
        other => panic!("expected a request, got {other:?}"),
    }
}

async fn expect_part(c: &mut FakeClient) -> ReplyPart {
    match message(c).await {
        server_message::Message::Part(p) => p,
        other => panic!("expected a part, got {other:?}"),
    }
}

async fn expect_end(c: &mut FakeClient) -> ReplyEnd {
    match message(c).await {
        server_message::Message::End(e) => e,
        other => panic!("expected an end, got {other:?}"),
    }
}

async fn expect_cancel(c: &mut FakeClient) -> (String, String) {
    match message(c).await {
        server_message::Message::Cancel(x) => (x.request_id, x.reason),
        other => panic!("expected a cancel, got {other:?}"),
    }
}

async fn expect_error(c: &mut FakeClient) -> (String, ErrorCode, String) {
    match message(c).await {
        server_message::Message::Error(e) => (e.request_id.clone(), e.code(), e.message),
        other => panic!("expected an error, got {other:?}"),
    }
}

fn end_code(e: &ReplyEnd) -> ErrorCode {
    e.error.as_ref().expect("an error on the end").code()
}

#[tokio::test]
async fn parts_flow_under_the_senders_credit_and_the_end_follows() {
    let addr = start_hub(config()).await;
    let (mut handler, mut sender) = pair(addr).await;
    sender
        .tx
        .send(stream("st1", "exp.Stream", 5_000, 2, 0))
        .await
        .unwrap();
    let r = expect_request(&mut handler).await;
    assert_eq!(r.id, "st1#1");
    assert_eq!(r.mode, RequestMode::Streaming as i32);
    assert_eq!((r.initial_credit, r.max_results), (2, 0));
    assert_eq!(r.payload, b"{\"q\":\"export\"}");
    // The handler sends what its credit allows, then waits for more.
    for seq in 0..2 {
        handler
            .tx
            .send(FakeClient::part_ok(
                "st1#1",
                seq,
                format!("row{seq}").as_bytes(),
            ))
            .await
            .unwrap();
    }
    for seq in 0..2 {
        let p = expect_part(&mut sender).await;
        assert_eq!((p.request_id.as_str(), p.seq), ("st1", seq));
        assert_eq!(p.payload, format!("row{seq}").as_bytes());
        assert_eq!(p.responder_service, "svc-h");
        assert_eq!(p.responder_instance, handler.instance_id);
    }
    nothing_within(&mut handler, Duration::from_millis(100)).await;
    sender.tx.send(FakeClient::flow("st1", 2)).await.unwrap();
    match message(&mut handler).await {
        server_message::Message::Flow(f) => {
            assert_eq!((f.request_id.as_str(), f.credits), ("st1#1", 2))
        }
        other => panic!("expected the flow, got {other:?}"),
    }
    for seq in 2..4 {
        handler
            .tx
            .send(FakeClient::part_ok("st1#1", seq, b"more"))
            .await
            .unwrap();
    }
    handler
        .tx
        .send(FakeClient::end("st1#1", None))
        .await
        .unwrap();
    assert_eq!(expect_part(&mut sender).await.seq, 2);
    assert_eq!(expect_part(&mut sender).await.seq, 3);
    let e = expect_end(&mut sender).await;
    assert_eq!(
        (e.request_id.as_str(), e.parts, e.failed, e.timed_out),
        ("st1", 4, 0, 0)
    );
    assert!(e.error.is_none());
    // Nothing follows the end; a late part goes nowhere; the handler was not cancelled.
    handler
        .tx
        .send(FakeClient::part_ok("st1#1", 4, b"late"))
        .await
        .unwrap();
    assert!(handshake(&mut sender, 2).await.is_empty());
    assert!(handshake(&mut handler, 2).await.is_empty());
}

#[tokio::test]
async fn a_part_beyond_the_credit_and_max_results_cancel_the_handler() {
    let addr = start_hub(config()).await;
    let (mut handler, mut sender) = pair(addr).await;
    sender
        .tx
        .send(stream("st1", "exp.Stream", 5_000, 1, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    for seq in 0..2 {
        handler
            .tx
            .send(FakeClient::part_ok("st1#1", seq, b"x"))
            .await
            .unwrap();
    }
    assert_eq!(expect_part(&mut sender).await.seq, 0);
    let e = expect_end(&mut sender).await;
    assert_eq!((end_code(&e), e.parts), (ErrorCode::InvalidRequest, 1));
    assert_eq!(e.error.unwrap().message, "part without credit");
    assert_eq!(
        expect_cancel(&mut handler).await,
        ("st1#1".to_string(), "part without credit".to_string())
    );

    sender
        .tx
        .send(stream("st2", "exp.Stream", 5_000, 0, 2))
        .await
        .unwrap();
    let r = expect_request(&mut handler).await;
    assert_eq!(
        (r.initial_credit, r.max_results),
        (64, 0),
        "the hub enforces max_results"
    );
    for seq in 0..3 {
        handler
            .tx
            .send(FakeClient::part_ok("st2#1", seq, b"x"))
            .await
            .unwrap();
    }
    assert_eq!(expect_part(&mut sender).await.seq, 0);
    assert_eq!(expect_part(&mut sender).await.seq, 1);
    let e = expect_end(&mut sender).await;
    assert!(e.error.is_none());
    assert_eq!(e.parts, 2);
    assert_eq!(
        expect_cancel(&mut handler).await,
        ("st2#1".to_string(), "max results".to_string())
    );
    assert!(
        handshake(&mut sender, 2).await.is_empty(),
        "the third part was dropped"
    );
}

#[tokio::test]
async fn an_error_part_or_end_relays_the_handlers_error() {
    let addr = start_hub(config()).await;
    let (mut handler, mut sender) = pair(addr).await;
    sender
        .tx
        .send(stream("st1", "exp.Stream", 5_000, 0, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    handler
        .tx
        .send(FakeClient::part_err(
            "st1#1",
            0,
            ErrorCode::HandlerError,
            "disk gone",
        ))
        .await
        .unwrap();
    let e = expect_end(&mut sender).await;
    assert_eq!((end_code(&e), e.parts), (ErrorCode::HandlerError, 0));
    let err = e.error.unwrap();
    assert_eq!(
        (err.request_id.as_str(), err.message.as_str()),
        ("st1", "disk gone")
    );

    sender
        .tx
        .send(stream("st2", "exp.Stream", 5_000, 0, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    handler
        .tx
        .send(FakeClient::part_ok("st2#1", 0, b"one"))
        .await
        .unwrap();
    handler
        .tx
        .send(FakeClient::end(
            "st2#1",
            Some((ErrorCode::PermissionDenied, "no more")),
        ))
        .await
        .unwrap();
    expect_part(&mut sender).await;
    let e = expect_end(&mut sender).await;
    assert_eq!((end_code(&e), e.parts), (ErrorCode::PermissionDenied, 1));
    assert!(
        handshake(&mut handler, 2).await.is_empty(),
        "no cancel: the handler ended"
    );
}

#[tokio::test]
async fn the_idle_timeout_and_the_max_duration_end_the_stream() {
    let addr = start_hub(Config {
        max_stream_duration: Duration::from_secs(1),
        ..config()
    })
    .await;
    let (mut handler, mut sender) = pair(addr).await;
    sender
        .tx
        .send(stream("idle", "exp.Stream", 300, 0, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    let e = expect_end(&mut sender).await;
    assert_eq!(end_code(&e), ErrorCode::Timeout);
    assert_eq!(expect_cancel(&mut handler).await.1, "idle timeout");

    sender
        .tx
        .send(stream("long", "exp.Stream", 500, 0, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    let started = std::time::Instant::now();
    let mut seq = 0;
    let end = loop {
        handler
            .tx
            .send(FakeClient::part_ok("long#1", seq, b"tick"))
            .await
            .unwrap();
        seq += 1;
        match message(&mut sender).await {
            server_message::Message::Part(p) => assert_eq!(p.seq, seq - 1),
            server_message::Message::End(e) => break e,
            other => panic!("expected a part or the end, got {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let lived = started.elapsed();
    assert!(
        lived >= Duration::from_millis(900) && lived < Duration::from_secs(3),
        "{lived:?}"
    );
    assert_eq!(end_code(&end), ErrorCode::Cancelled);
    assert_eq!(
        end.error.unwrap().message,
        "max stream duration (1s) exceeded"
    );
    assert!(end.parts >= 5, "{}", end.parts);
    let (_, reason) = loop {
        match message(&mut handler).await {
            server_message::Message::Cancel(c) => break (c.request_id, c.reason),
            server_message::Message::Flow(_) => {}
            other => panic!("expected the cancel, got {other:?}"),
        }
    };
    assert_eq!(reason, "max stream duration (1s) exceeded");
}

#[tokio::test]
async fn a_cancel_from_the_sender_and_the_disconnects() {
    let addr = start_hub(config()).await;
    let (mut handler, mut sender) = pair(addr).await;
    sender
        .tx
        .send(stream("st1", "exp.Stream", 5_000, 0, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    sender
        .tx
        .send(FakeClient::cancel("st1", "user left"))
        .await
        .unwrap();
    assert_eq!(
        expect_cancel(&mut handler).await,
        ("st1#1".to_string(), "user left".to_string())
    );
    assert!(
        handshake(&mut sender, 2).await.is_empty(),
        "nothing more to the sender"
    );

    // The handler leaves: HANDLER_DISCONNECTED as the end's error.
    sender
        .tx
        .send(stream("st2", "exp.Stream", 5_000, 0, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    handler
        .tx
        .send(FakeClient::part_ok("st2#1", 0, b"one"))
        .await
        .unwrap();
    expect_part(&mut sender).await;
    drop(handler);
    let e = expect_end(&mut sender).await;
    assert_eq!((end_code(&e), e.parts), (ErrorCode::HandlerDisconnected, 1));

    // The sender leaves: the handler is cancelled.
    let mut handler = register(addr, "svc-h", &["exp.Stream"], &[], PERMITS).await;
    handshake(&mut handler, 1).await;
    sender
        .tx
        .send(stream("st3", "exp.Stream", 5_000, 0, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    drop(sender);
    assert_eq!(expect_cancel(&mut handler).await.1, "sender disconnected");
}

/// A flow-controlled handler with one permit: the stream holds it until the end; the client
/// returns it with `Permits`, as for any request.
#[tokio::test]
async fn a_stream_holds_its_handlers_permit_until_it_ends() {
    let addr = start_hub(config()).await;
    let mut handler = register(addr, "svc-h", &["exp.Stream"], &[], 1).await;
    handshake(&mut handler, 1).await;
    let mut sender = register(addr, "svc-web", &[], &[], PERMITS).await;
    handshake(&mut sender, 1).await;
    sender
        .tx
        .send(stream("st1", "exp.Stream", 5_000, 0, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    sender
        .tx
        .send(FakeClient::request(
            "q1",
            RequestKind::Query,
            "exp.Stream",
            "",
            5_000,
        ))
        .await
        .unwrap();
    nothing_within(&mut handler, Duration::from_millis(150)).await;
    handler
        .tx
        .send(FakeClient::end("st1#1", None))
        .await
        .unwrap();
    assert_eq!(expect_end(&mut sender).await.parts, 0);
    nothing_within(&mut handler, Duration::from_millis(150)).await;
    handler.permits(1).await;
    assert_eq!(expect_request(&mut handler).await.id, "q1");
}

#[tokio::test]
async fn an_unknown_mode_is_invalid_request_and_a_type_without_owner_no_handler() {
    let addr = start_hub(config()).await;
    let (mut handler, mut sender) = pair(addr).await;
    let mut weird = stream("w1", "exp.Stream", 1_000, 0, 0);
    if let Some(client_message::Message::Request(r)) = weird.message.as_mut() {
        r.mode = 42;
    }
    sender.tx.send(weird).await.unwrap();
    let (id, code, _) = expect_error(&mut sender).await;
    assert_eq!((id.as_str(), code), ("w1", ErrorCode::InvalidRequest));
    sender
        .tx
        .send(stream("none", "nobody.Stream", 1_000, 0, 0))
        .await
        .unwrap();
    assert_eq!(expect_error(&mut sender).await.1, ErrorCode::NoHandler);
    assert!(handshake(&mut handler, 2).await.is_empty());
}

#[tokio::test]
async fn a_shared_type_streams_from_one_owner_instance_and_a_gather_cancels_the_rest() {
    let addr = start_hub(config()).await;
    let mut a1 = register(addr, "svc-a", &[], &["search.Stream"], PERMITS).await;
    let mut b1 = register(addr, "svc-b", &[], &["search.Stream"], PERMITS).await;
    let mut b2 = register(addr, "svc-b", &[], &["search.Stream"], PERMITS).await;
    for h in [&mut a1, &mut b1, &mut b2] {
        handshake(h, 1).await;
    }
    let mut sender = register(addr, "svc-web", &[], &[], PERMITS).await;
    handshake(&mut sender, 1).await;
    // Three streams: one instance each.
    let mut targets = Vec::new();
    for id in ["st1", "st2", "st3"] {
        sender
            .tx
            .send(stream(id, "search.Stream", 5_000, 0, 0))
            .await
            .unwrap();
        let got = tokio::select! {
            r = expect_request(&mut a1) => ("a1", r),
            r = expect_request(&mut b1) => ("b1", r),
            r = expect_request(&mut b2) => ("b2", r),
        };
        assert_eq!(got.1.id, format!("{id}#1"));
        targets.push(got.0);
    }
    targets.sort();
    assert_eq!(targets, ["a1", "b1", "b2"], "spread over the instances");
    // Whichever got which: each ends the stream it serves.
    for h in [&mut a1, &mut b1, &mut b2] {
        for id in ["st1#1", "st2#1", "st3#1"] {
            h.tx.send(FakeClient::end(id, None)).await.unwrap();
        }
    }
    let mut ends = vec![
        expect_end(&mut sender).await.request_id,
        expect_end(&mut sender).await.request_id,
        expect_end(&mut sender).await.request_id,
    ];
    ends.sort();
    assert_eq!(ends, ["st1", "st2", "st3"]);

    // A gather with max_results 1: b2 answers first; a1 and b1 are cancelled.
    let mut g = FakeClient::request("g1", RequestKind::Query, "search.Stream", "", 5_000);
    if let Some(client_message::Message::Request(r)) = g.message.as_mut() {
        r.mode = RequestMode::ScatterGather as i32;
        r.max_results = 1;
    }
    sender.tx.send(g).await.unwrap();
    let a1_target = expect_request(&mut a1).await.id;
    let b1_target = expect_request(&mut b1).await.id;
    let b2_target = expect_request(&mut b2).await.id;
    b2.tx
        .send(FakeClient::reply_ok(&b2_target, b"first"))
        .await
        .unwrap();
    expect_part(&mut sender).await;
    assert_eq!(expect_end(&mut sender).await.parts, 1);
    assert_eq!(
        expect_cancel(&mut a1).await,
        (a1_target, "max results".to_string())
    );
    assert_eq!(
        expect_cancel(&mut b1).await,
        (b1_target, "max results".to_string())
    );
    assert!(
        handshake(&mut b2, 3).await.is_empty(),
        "b2 answered: nothing to cancel"
    );
}

#[tokio::test]
async fn parts_and_the_end_reach_a_slow_sender_in_order() {
    let addr = start_hub(Config {
        session_buffer: 1,
        ..config()
    })
    .await;
    // With a one-slot buffer a heartbeat echo (best effort) may be dropped behind the ack, so
    // the handshake reads the ack itself.
    let mut handler = register(addr, "svc-h", &["exp.Stream"], &[], PERMITS).await;
    let mut sender = register(addr, "svc-web", &[], &[], PERMITS).await;
    for c in [&mut handler, &mut sender] {
        assert!(matches!(
            message(c).await,
            server_message::Message::RegisterAck(_)
        ));
    }
    sender
        .tx
        .send(stream("st1", "exp.Stream", 5_000, 0, 0))
        .await
        .unwrap();
    expect_request(&mut handler).await;
    for seq in 0..20 {
        handler
            .tx
            .send(FakeClient::part_ok("st1#1", seq, b"row"))
            .await
            .unwrap();
    }
    handler
        .tx
        .send(FakeClient::end("st1#1", None))
        .await
        .unwrap();
    for seq in 0..20 {
        assert_eq!(expect_part(&mut sender).await.seq, seq);
    }
    assert_eq!(expect_end(&mut sender).await.parts, 20);
}
