//! Scatter-gather queries and shared query types end to end (ADR 0017), over real gRPC with raw
//! clients: a v2 sender that declared "scatter-gather", v2 and v1 handler instances, and a raw v1
//! client that must never see a multi-part message or a code it does not know.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, start_hub};
use kraken_hub::config::Config;
use kraken_hub::proto::hub_client::HubClient;
use kraken_hub::proto::{
    ClientMessage, ErrorCode, Register, ReplyEnd, ReplyPart, Request, RequestKind, RequestMode,
    ServerMessage, TopologyRequest, client_message, server_message,
};
use tonic::transport::Endpoint;

const SHORT: Duration = Duration::from_millis(1500);
const SCATTER: &str = "scatter-gather";

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
async fn register_v2(
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

fn scatter(id: &str, type_name: &str, timeout_ms: u32, max_results: u32) -> ClientMessage {
    let mut m = FakeClient::request(id, RequestKind::Query, type_name, "", timeout_ms);
    if let Some(client_message::Message::Request(r)) = m.message.as_mut() {
        r.mode = RequestMode::ScatterGather as i32;
        r.max_results = max_results;
        r.payload = b"{\"q\":\"kraken\"}".to_vec();
    }
    m
}

async fn expect_request(c: &mut FakeClient) -> Request {
    match message(c).await {
        server_message::Message::Request(r) => r,
        other => panic!("expected a request, got {other:?}"),
    }
}

/// Parts until the end, then the end.
async fn gather(c: &mut FakeClient) -> (Vec<ReplyPart>, ReplyEnd) {
    let mut parts = Vec::new();
    loop {
        match message(c).await {
            server_message::Message::Part(p) => parts.push(p),
            server_message::Message::End(e) => return (parts, e),
            other => panic!("expected a part or the end, got {other:?}"),
        }
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
            Some(server_message::Message::Request(r)) => {
                assert_eq!(r.mode, 0, "a v1 handler got a request mode");
                assert_eq!(r.max_results, 0);
            }
            Some(server_message::Message::Reply(_))
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

#[tokio::test]
async fn a_scatter_gather_over_three_instances_of_two_owners_gets_three_parts_and_an_end() {
    let addr = start_hub(config()).await;
    let mut a1 = register_v2(addr, "svc-a", &[], &["search.Find"], &[]).await;
    let mut b1 = register_v2(addr, "svc-b", &[], &["search.Find"], &[]).await;
    let mut b2 = register_v2(addr, "svc-b", &[], &["search.Find"], &[]).await;
    for h in [&mut a1, &mut b1, &mut b2] {
        handshake(h, 1).await;
    }
    let mut sender = register_v2(addr, "svc-web", &[], &[], &[SCATTER]).await;
    match &handshake(&mut sender, 1).await[..] {
        [server_message::Message::RegisterAck(ack)] => {
            assert_eq!(ack.capabilities, [SCATTER]);
        }
        other => panic!("expected the ack, got {other:?}"),
    }

    sender
        .tx
        .send(scatter("g1", "search.Find", 2_000, 0))
        .await
        .unwrap();
    let mut target_ids = Vec::new();
    for (h, name) in [(&mut a1, "a"), (&mut b1, "b1"), (&mut b2, "b2")] {
        let r = expect_request(h).await;
        assert_eq!(
            r.mode,
            RequestMode::Unary as i32,
            "handlers see a unary request"
        );
        assert_eq!(r.payload, b"{\"q\":\"kraken\"}");
        h.tx.send(FakeClient::reply_ok(&r.id, name.as_bytes()))
            .await
            .unwrap();
        target_ids.push(r.id);
    }
    target_ids.sort();
    assert_eq!(target_ids, ["g1#0", "g1#1", "g1#2"]);

    let (parts, end) = gather(&mut sender).await;
    assert_eq!(parts.len(), 3);
    let mut seqs: Vec<u64> = parts.iter().map(|p| p.seq).collect();
    seqs.sort();
    assert_eq!(seqs, [0, 1, 2]);
    let mut answers: Vec<(String, String, String)> = parts
        .iter()
        .map(|p| {
            assert_eq!(p.request_id, "g1");
            assert!(p.error.is_none());
            (
                p.responder_service.clone(),
                String::from_utf8(p.payload.clone()).unwrap(),
                p.responder_instance.clone(),
            )
        })
        .collect();
    answers.sort();
    assert_eq!(
        answers
            .iter()
            .map(|(s, p, _)| (s.as_str(), p.as_str()))
            .collect::<Vec<_>>(),
        [("svc-a", "a"), ("svc-b", "b1"), ("svc-b", "b2")]
    );
    assert_eq!(answers[0].2, a1.instance_id);
    assert_eq!(
        (
            end.request_id.as_str(),
            end.parts,
            end.failed,
            end.timed_out
        ),
        ("g1", 3, 0, 0)
    );
    assert!(end.error.is_none());

    // max_results 1: one part, then the end; the other answers are dropped.
    sender
        .tx
        .send(scatter("g2", "search.Find", 2_000, 1))
        .await
        .unwrap();
    for h in [&mut a1, &mut b1, &mut b2] {
        let r = expect_request(h).await;
        h.tx.send(FakeClient::reply_ok(&r.id, b"x")).await.unwrap();
    }
    let (parts, end) = gather(&mut sender).await;
    assert_eq!((parts.len(), end.parts, end.timed_out), (1, 1, 0));
    assert!(
        handshake(&mut sender, 2).await.is_empty(),
        "nothing after the end"
    );

    // A point query on the shared type goes to one owner instance.
    sender
        .tx
        .send(FakeClient::request(
            "p1",
            RequestKind::Query,
            "search.Find",
            "",
            2_000,
        ))
        .await
        .unwrap();
    let got = tokio::select! {
        r = expect_request(&mut a1) => (r, &mut a1),
        r = expect_request(&mut b1) => (r, &mut b1),
        r = expect_request(&mut b2) => (r, &mut b2),
    };
    let (r, h) = got;
    assert_eq!(r.id, "p1");
    h.tx.send(FakeClient::reply_ok("p1", b"one")).await.unwrap();
    match message(&mut sender).await {
        server_message::Message::Reply(r) => assert_eq!(r.request_id, "p1"),
        other => panic!("expected the reply, got {other:?}"),
    }
}

#[tokio::test]
async fn v1_handlers_serve_scatter_targets_unchanged_and_the_deadline_ends_the_gather() {
    let addr = start_hub(config()).await;
    let mut h1 = FakeClient::register(addr, "svc-old", &[], &["old.Get"]).await;
    let mut h2 = FakeClient::register(addr, "svc-old", &[], &["old.Get"]).await;
    h1.heartbeat(1).await;
    h2.heartbeat(1).await;
    assert_only_v1_messages(&mut h1, Duration::from_millis(200)).await;
    assert_only_v1_messages(&mut h2, Duration::from_millis(200)).await;
    let mut sender = register_v2(addr, "svc-web", &[], &[], &[SCATTER]).await;
    handshake(&mut sender, 1).await;

    sender
        .tx
        .send(scatter("g1", "old.Get", 400, 0))
        .await
        .unwrap();
    let r = expect_request(&mut h1).await;
    assert_eq!(r.mode, 0);
    h1.tx
        .send(FakeClient::reply_ok(&r.id, b"h1"))
        .await
        .unwrap();
    // h2 never answers: the deadline ends the gather with one target timed out.
    let (parts, end) = gather(&mut sender).await;
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].responder_instance, h1.instance_id);
    assert_eq!((end.parts, end.failed, end.timed_out), (1, 0, 1));
    let h2_saw = assert_only_v1_messages(&mut h2, Duration::from_millis(200)).await;
    assert_eq!(h2_saw.len(), 1, "the target request only: {h2_saw:?}");
    // h2's late answer goes nowhere.
    let late = match &h2_saw[0].message {
        Some(server_message::Message::Request(r)) => r.id.clone(),
        other => panic!("expected the request, got {other:?}"),
    };
    h2.tx
        .send(FakeClient::reply_ok(&late, b"late"))
        .await
        .unwrap();
    assert!(handshake(&mut sender, 2).await.is_empty());
    assert_only_v1_messages(&mut h1, Duration::from_millis(200)).await;
}

#[tokio::test]
async fn a_target_that_disconnects_is_a_failed_part() {
    let addr = start_hub(config()).await;
    let mut a1 = register_v2(addr, "svc-a", &[], &["search.Find"], &[]).await;
    let mut b1 = register_v2(addr, "svc-b", &[], &["search.Find"], &[]).await;
    handshake(&mut a1, 1).await;
    handshake(&mut b1, 1).await;
    let mut sender = register_v2(addr, "svc-web", &[], &[], &[SCATTER]).await;
    handshake(&mut sender, 1).await;
    sender
        .tx
        .send(scatter("g1", "search.Find", 5_000, 0))
        .await
        .unwrap();
    let r = expect_request(&mut a1).await;
    expect_request(&mut b1).await;
    let b1_id = b1.instance_id.clone();
    drop(b1);
    let ((parts, end), ()) = tokio::join!(gather(&mut sender), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        a1.tx.send(FakeClient::reply_ok(&r.id, b"a")).await.unwrap();
    });
    assert_eq!((end.parts, end.failed, end.timed_out), (2, 1, 0));
    let failed: Vec<&ReplyPart> = parts.iter().filter(|p| p.error.is_some()).collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].responder_instance, b1_id);
    assert_eq!(
        failed[0].error.as_ref().unwrap().code(),
        ErrorCode::HandlerDisconnected
    );
}

#[tokio::test]
async fn senders_without_the_capability_and_raw_v1_clients_are_refused_with_codes_they_know() {
    let addr = start_hub(config()).await;
    let mut a1 = register_v2(addr, "svc-a", &[], &["search.Find"], &[]).await;
    handshake(&mut a1, 1).await;

    // A v2 sender that did not declare scatter-gather: UNSUPPORTED, no ack capability.
    let mut plain = register_v2(addr, "svc-plain", &[], &[], &["flow-control"]).await;
    match &handshake(&mut plain, 1).await[..] {
        [server_message::Message::RegisterAck(ack)] => {
            assert!(!ack.capabilities.iter().any(|c| c == SCATTER));
        }
        other => panic!("expected the ack, got {other:?}"),
    }
    plain
        .tx
        .send(scatter("p1", "search.Find", 1_000, 0))
        .await
        .unwrap();
    match message(&mut plain).await {
        server_message::Message::Error(e) => {
            assert_eq!(
                (e.request_id.as_str(), e.code()),
                ("p1", ErrorCode::Unsupported)
            );
        }
        other => panic!("expected UNSUPPORTED, got {other:?}"),
    }

    // A raw v1 client that sets the mode field anyway: INVALID_REQUEST, never a part or an end.
    let mut v1 = FakeClient::register(addr, "svc-v1", &[], &[]).await;
    v1.heartbeat(1).await;
    v1.tx
        .send(scatter("v1", "search.Find", 1_000, 0))
        .await
        .unwrap();
    let mut saw = assert_only_v1_messages(&mut v1, Duration::from_millis(300)).await;
    saw.retain(|m| matches!(m.message, Some(server_message::Message::Error(_))));
    match &saw[..] {
        [
            ServerMessage {
                message: Some(server_message::Message::Error(e)),
            },
        ] => assert_eq!(e.code(), ErrorCode::InvalidRequest),
        other => panic!("expected one INVALID_REQUEST, got {other:?}"),
    }
    // Nothing reached the handler.
    assert!(handshake(&mut a1, 2).await.is_empty());

    // A scatter-gather of a command: INVALID_REQUEST; of an unknown type: NO_HANDLER.
    let mut sender = register_v2(addr, "svc-web", &[], &[], &[SCATTER]).await;
    handshake(&mut sender, 1).await;
    let mut cmd = scatter("c1", "svc.Do", 1_000, 0);
    if let Some(client_message::Message::Request(r)) = cmd.message.as_mut() {
        r.kind = RequestKind::Command as i32;
    }
    sender.tx.send(cmd).await.unwrap();
    sender
        .tx
        .send(scatter("n1", "nobody.Find", 1_000, 0))
        .await
        .unwrap();
    for (id, code) in [
        ("c1", ErrorCode::InvalidRequest),
        ("n1", ErrorCode::NoHandler),
    ] {
        match message(&mut sender).await {
            server_message::Message::Error(e) => {
                assert_eq!((e.request_id.as_str(), e.code()), (id, code));
            }
            other => panic!("expected an error, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn exclusive_and_shared_registrants_conflict_and_the_topology_lists_every_owner() {
    let addr = start_hub(config()).await;
    let mut a1 = register_v2(addr, "svc-a", &[], &["search.Find"], &[]).await;
    let mut b1 = register_v2(addr, "svc-b", &["own.Get"], &["search.Find"], &[]).await;
    handshake(&mut a1, 1).await;
    handshake(&mut b1, 1).await;

    // An exclusive registrant of the shared type fails with ALREADY_EXISTS.
    let mut c1 = register_v2(addr, "svc-c", &["search.Find"], &[], &[]).await;
    match c1.next().await {
        Some(Err(status)) => {
            assert_eq!(status.code(), tonic::Code::AlreadyExists);
            assert!(status.message().contains("shared query type"), "{status:?}");
        }
        other => panic!("expected ALREADY_EXISTS, got {other:?}"),
    }

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let topo = HubClient::new(channel)
        .topology(TopologyRequest {})
        .await
        .unwrap()
        .into_inner();
    let find = topo
        .types
        .iter()
        .find(|t| t.r#type == "search.Find")
        .unwrap();
    assert!(find.shared);
    assert_eq!(find.owner_services, ["svc-a", "svc-b"]);
    assert_eq!(find.owner_service, "svc-a");
    assert_eq!(find.live_instances, 2);
    let own = topo.types.iter().find(|t| t.r#type == "own.Get").unwrap();
    assert!(!own.shared);
    assert_eq!(own.owner_services, ["svc-b"]);
    let b = topo
        .services
        .iter()
        .find(|s| s.service_name == "svc-b")
        .unwrap();
    assert_eq!(b.instances[0].query_types, ["own.Get", "search.Find"]);
    assert_eq!(b.instances[0].shared_query_types, ["search.Find"]);
}
