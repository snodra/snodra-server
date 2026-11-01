//! Subscription queries end to end (ADR 0017, P4c), over real gRPC with raw clients: a v2
//! sender that declared "subscription", v2 handlers with and without it, and the codes a
//! session without the capability may see.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, start_hub};
use kraken_hub::config::Config;
use kraken_hub::proto::{
    ClientMessage, ErrorCode, Register, ReplyEnd, ReplyPart, Request, RequestKind, RequestMode,
    client_message, server_message,
};

const SHORT: Duration = Duration::from_millis(1500);
const SUBSCRIPTION: &str = "subscription";
const STREAMING: &str = "streaming";

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

/// A v2 client declaring `capabilities`, handling `queries` (exclusive) and `shared` queries.
async fn register(
    addr: SocketAddr,
    service: &str,
    queries: &[&str],
    shared: &[&str],
    capabilities: &[&str],
) -> FakeClient {
    let client = FakeClient::connect(addr).await;
    let strings = |v: &[&str]| v.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    client
        .send_raw_register(Register {
            service_name: service.to_string(),
            instance_id: client.instance_id.clone(),
            query_types: strings(queries),
            shared_query_types: strings(shared),
            protocol_version: 2,
            capabilities: strings(capabilities),
            client_version: "fake-client/1.0".to_string(),
            ..Register::default()
        })
        .await;
    client
}

/// Two handlers of the shared query `acct.Watch` (services svc-a and svc-b, both with
/// "subscription") and a sender, all handshaken.
async fn trio(addr: SocketAddr) -> (FakeClient, FakeClient, FakeClient) {
    let mut a = register(addr, "svc-a", &[], &["acct.Watch"], &[SUBSCRIPTION]).await;
    let mut b = register(addr, "svc-b", &[], &["acct.Watch"], &[SUBSCRIPTION]).await;
    handshake(&mut a, 1).await;
    handshake(&mut b, 1).await;
    let mut sender = register(addr, "svc-web", &[], &[], &[SUBSCRIPTION, STREAMING]).await;
    match &handshake(&mut sender, 1).await[..] {
        [server_message::Message::RegisterAck(ack)] => {
            assert_eq!(ack.capabilities, [STREAMING, SUBSCRIPTION]);
        }
        other => panic!("expected the ack, got {other:?}"),
    }
    (a, b, sender)
}

fn subscribe(
    id: &str,
    type_name: &str,
    timeout_ms: u32,
    credit: u32,
    max_results: u32,
) -> ClientMessage {
    let mut m = FakeClient::request(id, RequestKind::Query, type_name, "", timeout_ms);
    if let Some(client_message::Message::Request(r)) = m.message.as_mut() {
        r.mode = RequestMode::Subscription as i32;
        r.initial_credit = credit;
        r.max_results = max_results;
        r.payload = b"{\"account\":7}".to_vec();
    }
    m
}

/// The requests `c` gets until nothing arrives for 150 ms, sorted by id.
async fn requests_of(c: &mut FakeClient) -> Vec<Request> {
    let mut out = Vec::new();
    while let Ok(m) = tokio::time::timeout(Duration::from_millis(150), c.rx.message()).await {
        match m.unwrap().unwrap().message.unwrap() {
            server_message::Message::Request(r) => out.push(r),
            other => panic!("expected a request, got {other:?}"),
        }
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
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

/// Opens subscription `id` on `acct.Watch`: both handlers get their update target, one of
/// them the initial request. Returns `(initial handler is a, a's target id, b's target id)`.
async fn open(
    sender: &mut FakeClient,
    a: &mut FakeClient,
    b: &mut FakeClient,
    id: &str,
    credit: u32,
    max_results: u32,
) -> (bool, String, String) {
    sender
        .tx
        .send(subscribe(id, "acct.Watch", 5_000, credit, max_results))
        .await
        .unwrap();
    let ra = requests_of(a).await;
    let rb = requests_of(b).await;
    let initial_id = format!("{id}#0");
    let initial_on_a = ra.iter().any(|r| r.id == initial_id);
    assert_ne!(
        initial_on_a,
        rb.iter().any(|r| r.id == initial_id),
        "exactly one initial request: a {ra:?}, b {rb:?}"
    );
    let target = |rs: &[Request]| {
        let t: Vec<&Request> = rs
            .iter()
            .filter(|r| r.mode == RequestMode::Subscription as i32)
            .collect();
        assert_eq!(t.len(), 1, "{rs:?}");
        assert_eq!((t[0].initial_credit, t[0].max_results), (0, 0));
        assert_eq!(t[0].payload, b"{\"account\":7}");
        t[0].id.clone()
    };
    (initial_on_a, target(&ra), target(&rb))
}

#[tokio::test]
async fn the_initial_result_then_the_updates_of_every_target_then_the_end() {
    let addr = start_hub(config()).await;
    let (mut a, mut b, mut sender) = trio(addr).await;
    let (initial_on_a, ta, tb) = open(&mut sender, &mut a, &mut b, "sub1", 0, 0).await;
    assert_eq!((ta.as_str(), tb.as_str()), ("sub1#1", "sub1#2"));
    let initial = if initial_on_a { &mut a } else { &mut b };
    initial
        .tx
        .send(FakeClient::reply_ok("sub1#0", b"balance 10"))
        .await
        .unwrap();
    let p = expect_part(&mut sender).await;
    assert_eq!((p.request_id.as_str(), p.seq), ("sub1", 1));
    assert_eq!(p.payload, b"balance 10");
    assert_eq!(
        p.responder_service,
        if initial_on_a { "svc-a" } else { "svc-b" }
    );
    a.tx.send(FakeClient::part_ok(&ta, 1, b"balance 11"))
        .await
        .unwrap();
    let p = expect_part(&mut sender).await;
    assert_eq!((p.seq, p.responder_service.as_str()), (2, "svc-a"));
    assert_eq!(p.responder_instance, a.instance_id);
    b.tx.send(FakeClient::part_ok(&tb, 1, b"balance 12"))
        .await
        .unwrap();
    let p = expect_part(&mut sender).await;
    assert_eq!((p.seq, p.responder_service.as_str()), (3, "svc-b"));
    a.tx.send(FakeClient::end(&ta, None)).await.unwrap();
    assert!(handshake(&mut sender, 2).await.is_empty(), "b still serves");
    b.tx.send(FakeClient::end(&tb, None)).await.unwrap();
    let e = expect_end(&mut sender).await;
    assert_eq!(
        (e.request_id.as_str(), e.parts, e.failed, e.timed_out),
        ("sub1", 3, 0, 0)
    );
    assert!(e.error.is_none());
    // Nothing follows the end; a late update goes nowhere; nobody was cancelled.
    a.tx.send(FakeClient::part_ok(&ta, 2, b"late"))
        .await
        .unwrap();
    assert!(handshake(&mut sender, 3).await.is_empty());
    assert!(handshake(&mut a, 2).await.is_empty());
    assert!(handshake(&mut b, 2).await.is_empty());
}

#[tokio::test]
async fn a_flow_drains_the_buffered_updates_and_a_cancel_reaches_every_target() {
    let addr = start_hub(config()).await;
    let (mut a, mut b, mut sender) = trio(addr).await;
    let (initial_on_a, ta, tb) = open(&mut sender, &mut a, &mut b, "sub1", 1, 0).await;
    // Updates before the initial result wait behind it; the initial part takes the credit.
    a.tx.send(FakeClient::part_ok(&ta, 1, b"u1")).await.unwrap();
    b.tx.send(FakeClient::part_ok(&tb, 1, b"u2")).await.unwrap();
    assert!(
        handshake(&mut sender, 2).await.is_empty(),
        "nothing before the initial"
    );
    let initial = if initial_on_a { &mut a } else { &mut b };
    initial
        .tx
        .send(FakeClient::reply_ok("sub1#0", b"initial"))
        .await
        .unwrap();
    assert_eq!(expect_part(&mut sender).await.seq, 1);
    assert!(handshake(&mut sender, 3).await.is_empty(), "no credit left");
    sender.tx.send(FakeClient::flow("sub1", 1)).await.unwrap();
    let p = expect_part(&mut sender).await;
    assert_eq!((p.seq, p.payload.as_slice()), (2, &b"u1"[..]));
    sender.tx.send(FakeClient::flow("sub1", 10)).await.unwrap();
    let p = expect_part(&mut sender).await;
    assert_eq!((p.seq, p.payload.as_slice()), (3, &b"u2"[..]));
    assert!(
        handshake(&mut a, 2).await.is_empty(),
        "no Flow reaches a target"
    );
    assert!(handshake(&mut b, 2).await.is_empty());
    sender
        .tx
        .send(FakeClient::cancel("sub1", "user left"))
        .await
        .unwrap();
    assert_eq!(expect_cancel(&mut a).await, (ta, "user left".to_string()));
    assert_eq!(expect_cancel(&mut b).await, (tb, "user left".to_string()));
    assert!(
        handshake(&mut sender, 4).await.is_empty(),
        "nothing more to the sender"
    );
}

#[tokio::test]
async fn the_buffer_limit_and_max_results_end_the_subscription() {
    let addr = start_hub(Config {
        subscription_buffer: 2,
        ..config()
    })
    .await;
    let (mut a, mut b, mut sender) = trio(addr).await;
    let (initial_on_a, ta, tb) = open(&mut sender, &mut a, &mut b, "sub1", 1, 0).await;
    let initial = if initial_on_a { &mut a } else { &mut b };
    initial
        .tx
        .send(FakeClient::reply_ok("sub1#0", b"initial"))
        .await
        .unwrap();
    assert_eq!(expect_part(&mut sender).await.seq, 1);
    for seq in 1..4 {
        a.tx.send(FakeClient::part_ok(&ta, seq, b"u"))
            .await
            .unwrap();
    }
    let e = expect_end(&mut sender).await;
    assert_eq!((end_code(&e), e.parts), (ErrorCode::ResourceExhausted, 1));
    assert_eq!(e.error.unwrap().message, "subscriber buffer (2) exceeded");
    assert_eq!(expect_cancel(&mut a).await.0, ta);
    assert_eq!(expect_cancel(&mut b).await.0, tb);

    let (initial_on_a, ta, tb) = open(&mut sender, &mut a, &mut b, "sub2", 0, 1).await;
    let initial = if initial_on_a { &mut a } else { &mut b };
    initial
        .tx
        .send(FakeClient::reply_ok("sub2#0", b"initial"))
        .await
        .unwrap();
    assert_eq!(
        expect_part(&mut sender).await.seq,
        1,
        "the initial part does not count"
    );
    b.tx.send(FakeClient::part_ok(&tb, 1, b"u")).await.unwrap();
    assert_eq!(expect_part(&mut sender).await.seq, 2);
    let e = expect_end(&mut sender).await;
    assert!(e.error.is_none());
    assert_eq!(e.parts, 2);
    assert_eq!(expect_cancel(&mut a).await, (ta, "max results".to_string()));
    assert_eq!(expect_cancel(&mut b).await, (tb, "max results".to_string()));
}

#[tokio::test]
async fn the_disconnects_cancel_the_targets_or_end_the_subscription() {
    let addr = start_hub(config()).await;
    let (mut a, mut b, mut sender) = trio(addr).await;
    // A target leaves: silently; the last one: HANDLER_DISCONNECTED.
    let (initial_on_a, _, tb) = open(&mut sender, &mut a, &mut b, "sub1", 0, 0).await;
    let initial = if initial_on_a { &mut a } else { &mut b };
    initial
        .tx
        .send(FakeClient::reply_ok("sub1#0", b"initial"))
        .await
        .unwrap();
    assert_eq!(expect_part(&mut sender).await.seq, 1);
    drop(a);
    assert!(handshake(&mut sender, 2).await.is_empty(), "b still serves");
    b.tx.send(FakeClient::part_ok(&tb, 1, b"u")).await.unwrap();
    assert_eq!(expect_part(&mut sender).await.seq, 2);
    drop(b);
    let e = expect_end(&mut sender).await;
    assert_eq!((end_code(&e), e.parts), (ErrorCode::HandlerDisconnected, 2));

    // The sender leaves: every target is cancelled.
    let mut a = register(addr, "svc-a", &[], &["acct.Watch"], &[SUBSCRIPTION]).await;
    let mut b = register(addr, "svc-b", &[], &["acct.Watch"], &[SUBSCRIPTION]).await;
    handshake(&mut a, 1).await;
    handshake(&mut b, 1).await;
    let (initial_on_a, ta, tb) = open(&mut sender, &mut a, &mut b, "sub2", 0, 0).await;
    drop(sender);
    let mut cancels = vec![expect_cancel(&mut a).await, expect_cancel(&mut b).await];
    let initial = if initial_on_a { &mut a } else { &mut b };
    cancels.push(expect_cancel(initial).await);
    cancels.sort();
    let mut expected = vec![
        ("sub2#0".to_string(), "sender disconnected".to_string()),
        (ta, "sender disconnected".to_string()),
        (tb, "sender disconnected".to_string()),
    ];
    expected.sort();
    assert_eq!(cancels, expected);
}

#[tokio::test]
async fn subscriptions_are_refused_with_codes_the_session_knows() {
    let addr = start_hub(config()).await;
    let mut legacy = register(addr, "svc-old", &["old.Watch"], &[], &[STREAMING]).await;
    handshake(&mut legacy, 1).await;
    let (mut a, mut b, mut sender) = trio(addr).await;

    // A v2 sender without the capability: UNSUPPORTED.
    let mut plain = register(addr, "svc-plain", &[], &[], &[STREAMING]).await;
    handshake(&mut plain, 1).await;
    plain
        .tx
        .send(subscribe("p1", "acct.Watch", 1_000, 0, 0))
        .await
        .unwrap();
    let (id, code, why) = expect_error(&mut plain).await;
    assert_eq!((id.as_str(), code), ("p1", ErrorCode::Unsupported));
    assert!(why.contains("\"subscription\""), "{why}");

    // A raw v1 client: INVALID_REQUEST, never a multi-part message.
    let mut v1 = FakeClient::register(addr, "svc-v1", &[], &[]).await;
    v1.heartbeat(1).await;
    v1.tx
        .send(subscribe("v1", "acct.Watch", 1_000, 0, 0))
        .await
        .unwrap();
    loop {
        match message(&mut v1).await {
            server_message::Message::Heartbeat(_) => {}
            server_message::Message::Error(e) => {
                assert_eq!(e.code(), ErrorCode::InvalidRequest);
                break;
            }
            other => panic!("a v1 session got {other:?}"),
        }
    }

    // No capable instance of the owner: UNSUPPORTED naming it; no owner: NO_HANDLER.
    sender
        .tx
        .send(subscribe("old", "old.Watch", 1_000, 0, 0))
        .await
        .unwrap();
    let (id, code, why) = expect_error(&mut sender).await;
    assert_eq!((id.as_str(), code), ("old", ErrorCode::Unsupported));
    assert!(
        why.contains("svc-old") && why.contains("subscription"),
        "{why}"
    );
    sender
        .tx
        .send(subscribe("none", "nobody.Watch", 1_000, 0, 0))
        .await
        .unwrap();
    assert_eq!(expect_error(&mut sender).await.1, ErrorCode::NoHandler);
    assert!(
        handshake(&mut legacy, 2).await.is_empty(),
        "nothing reached the old handler"
    );
    // A subscription-only session's multi-part messages are read (and ignored when unknown).
    a.tx.send(FakeClient::part_ok("ghost#1", 1, b"x"))
        .await
        .unwrap();
    a.tx.send(FakeClient::end("ghost#1", None)).await.unwrap();
    assert!(handshake(&mut a, 2).await.is_empty());
    assert!(handshake(&mut b, 2).await.is_empty());
}
