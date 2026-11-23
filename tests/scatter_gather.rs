//! Scatter-gather queries and shared query types end to end, over real gRPC with raw
//! clients.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, start_hub};
use snodra_server::config::Config;
use snodra_server::proto::hub_client::HubClient;
use snodra_server::proto::{
    ClientMessage, ErrorCode, Register, ReplyEnd, ReplyPart, Request, RequestKind, RequestMode,
    ServerMessage, TopologyRequest, client_message, server_message,
};
use tonic::transport::Endpoint;

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

/// A client handling `queries` (exclusive) and `shared` queries; does not wait for the ack.
async fn register(
    addr: SocketAddr,
    service: &str,
    queries: &[&str],
    shared: &[&str],
) -> FakeClient {
    let client = FakeClient::connect(addr).await;
    client
        .send_raw_register(Register {
            shared_query_types: shared.iter().map(|s| s.to_string()).collect(),
            ..client.register_message(service, &[], queries)
        })
        .await;
    client
}

fn scatter(id: &str, type_name: &str, timeout_ms: u32, max_results: u32) -> ClientMessage {
    let mut m = FakeClient::request(id, RequestKind::Query, type_name, "", timeout_ms);
    if let Some(client_message::Message::Request(r)) = m.message.as_mut() {
        r.mode = RequestMode::ScatterGather as i32;
        r.max_results = max_results;
        r.payload = b"{\"q\":\"Snodra\"}".to_vec();
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

/// Whatever arrives within `quiet`.
async fn drain(c: &mut FakeClient, quiet: Duration) -> Vec<ServerMessage> {
    let mut seen = Vec::new();
    while let Ok(Some(Ok(m))) = tokio::time::timeout(quiet, c.rx.message())
        .await
        .map(|r| r.transpose())
    {
        seen.push(m);
    }
    seen
}

#[tokio::test]
async fn a_scatter_gather_over_three_instances_of_two_owners_gets_three_parts_and_an_end() {
    let addr = start_hub(config()).await;
    let mut a1 = register(addr, "svc-a", &[], &["search.Find"]).await;
    let mut b1 = register(addr, "svc-b", &[], &["search.Find"]).await;
    let mut b2 = register(addr, "svc-b", &[], &["search.Find"]).await;
    for h in [&mut a1, &mut b1, &mut b2] {
        handshake(h, 1).await;
    }
    let mut sender = register(addr, "svc-web", &[], &[]).await;
    handshake(&mut sender, 1).await;

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
        assert_eq!(r.payload, b"{\"q\":\"Snodra\"}");
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
    // A target whose answer came after the end may have been sent a cancel for it.
    for h in [&mut a1, &mut b1, &mut b2] {
        for m in handshake(h, 2).await {
            assert!(matches!(m, server_message::Message::Cancel(_)), "{m:?}");
        }
    }

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
async fn targets_get_plain_unary_requests_and_the_deadline_ends_the_gather() {
    let addr = start_hub(config()).await;
    let mut h1 = FakeClient::register(addr, "svc-old", &[], &["old.Get"]).await;
    let mut h2 = FakeClient::register(addr, "svc-old", &[], &["old.Get"]).await;
    let mut sender = register(addr, "svc-web", &[], &[]).await;
    handshake(&mut sender, 1).await;

    sender
        .tx
        .send(scatter("g1", "old.Get", 400, 0))
        .await
        .unwrap();
    let r = expect_request(&mut h1).await;
    assert_eq!((r.mode, r.max_results), (0, 0));
    h1.tx
        .send(FakeClient::reply_ok(&r.id, b"h1"))
        .await
        .unwrap();
    // h2 never answers: the deadline ends the gather with one target timed out.
    let (parts, end) = gather(&mut sender).await;
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].responder_instance, h1.instance_id);
    assert_eq!((end.parts, end.failed, end.timed_out), (1, 0, 1));
    // h2 got its target request, then a cancel for it at the deadline.
    let h2_saw = drain(&mut h2, Duration::from_millis(200)).await;
    let late = match &h2_saw[..] {
        [
            ServerMessage {
                message: Some(server_message::Message::Request(r)),
            },
            ServerMessage {
                message: Some(server_message::Message::Cancel(c)),
            },
        ] if c.request_id == r.id && c.reason == "deadline" => r.id.clone(),
        other => panic!("expected the request and its cancel, got {other:?}"),
    };
    // h2's late answer goes nowhere.
    h2.tx
        .send(FakeClient::reply_ok(&late, b"late"))
        .await
        .unwrap();
    assert!(handshake(&mut sender, 2).await.is_empty());
    assert!(drain(&mut h1, Duration::from_millis(200)).await.is_empty());
}

#[tokio::test]
async fn a_target_that_disconnects_is_a_failed_part() {
    let addr = start_hub(config()).await;
    let mut a1 = register(addr, "svc-a", &[], &["search.Find"]).await;
    let mut b1 = register(addr, "svc-b", &[], &["search.Find"]).await;
    handshake(&mut a1, 1).await;
    handshake(&mut b1, 1).await;
    let mut sender = register(addr, "svc-web", &[], &[]).await;
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
async fn a_scatter_gather_of_a_command_or_an_unknown_type_is_refused() {
    let addr = start_hub(config()).await;
    // A scatter-gather of a command: INVALID_REQUEST; of an unknown type: NO_HANDLER.
    let mut sender = register(addr, "svc-web", &[], &[]).await;
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
    let mut a1 = register(addr, "svc-a", &[], &["search.Find"]).await;
    let mut b1 = register(addr, "svc-b", &["own.Get"], &["search.Find"]).await;
    handshake(&mut a1, 1).await;
    handshake(&mut b1, 1).await;

    // An exclusive registrant of the shared type fails with ALREADY_EXISTS.
    let mut c1 = register(addr, "svc-c", &["search.Find"], &[]).await;
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

/// A client handling `exclusive` and `shared` queries, declaring `subscriptions` among them as
/// subscription types; does not wait for the ack.
async fn register_subscriptions(
    addr: SocketAddr,
    service: &str,
    exclusive: &[&str],
    shared: &[&str],
    subscriptions: &[&str],
) -> FakeClient {
    let client = FakeClient::connect(addr).await;
    client
        .send_raw_register(Register {
            shared_query_types: shared.iter().map(|s| s.to_string()).collect(),
            subscription_query_types: subscriptions.iter().map(|s| s.to_string()).collect(),
            ..client.register_message(service, &[], exclusive)
        })
        .await;
    client
}

/// The first message of a Register the hub refused: the stream's status.
async fn refused(c: &mut FakeClient) -> tonic::Status {
    match c.next().await {
        Some(Err(status)) => status,
        other => panic!("expected the Register refused, got {other:?}"),
    }
}

#[tokio::test]
async fn a_scatter_gather_of_a_subscription_type_is_refused_and_no_target_gets_anything() {
    let addr = start_hub(config()).await;
    // Exclusive and shared subscription types: the handlers recognise a subscription's initial
    // request by its `#0` id, so a gather's target `#0` would be answered as one.
    let mut a1 = register_subscriptions(
        addr,
        "svc-a",
        &["acct.Watch"],
        &["prices.Watch"],
        &["acct.Watch", "prices.Watch"],
    )
    .await;
    let mut b1 =
        register_subscriptions(addr, "svc-b", &[], &["prices.Watch"], &["prices.Watch"]).await;
    handshake(&mut a1, 1).await;
    handshake(&mut b1, 1).await;
    let mut sender = register(addr, "svc-web", &[], &[]).await;
    handshake(&mut sender, 1).await;

    for (id, type_name) in [("g1", "prices.Watch"), ("g2", "acct.Watch")] {
        sender
            .tx
            .send(scatter(id, type_name, 2_000, 0))
            .await
            .unwrap();
    }
    let quiet = Duration::from_millis(300);
    for h in [&mut a1, &mut b1] {
        let seen = drain(h, quiet).await;
        assert!(seen.is_empty(), "no target gets anything: {seen:?}");
    }
    let seen = drain(&mut sender, quiet).await;
    let errors: Vec<_> = seen
        .iter()
        .map(|m| match m.message.as_ref() {
            Some(server_message::Message::Error(e)) => e,
            other => panic!("expected only terminal errors, got {other:?}"),
        })
        .collect();
    assert_eq!(errors.len(), 2, "one terminal error per gather: {seen:?}");
    for (e, id) in errors.iter().zip(["g1", "g2"]) {
        assert_eq!(
            (e.request_id.as_str(), e.code()),
            (id, ErrorCode::InvalidRequest)
        );
        assert!(e.message.contains("subscription type"), "{e:?}");
    }
    // The ids are free again: nothing was entered for them.
    sender
        .tx
        .send(scatter("g1", "search.Nope", 500, 0))
        .await
        .unwrap();
    match message(&mut sender).await {
        server_message::Message::Error(e) => assert_eq!(e.code(), ErrorCode::NoHandler),
        other => panic!("expected NO_HANDLER, got {other:?}"),
    }
}

#[tokio::test]
async fn a_shared_query_type_is_still_gathered_next_to_a_subscription_type() {
    let addr = start_hub(config()).await;
    // The same instances serve a plain shared query and a subscription type.
    let mut a1 = register_subscriptions(
        addr,
        "svc-a",
        &[],
        &["search.Find", "prices.Watch"],
        &["prices.Watch"],
    )
    .await;
    let mut b1 = register_subscriptions(
        addr,
        "svc-b",
        &[],
        &["search.Find", "prices.Watch"],
        &["prices.Watch"],
    )
    .await;
    handshake(&mut a1, 1).await;
    handshake(&mut b1, 1).await;
    let mut sender = register(addr, "svc-web", &[], &[]).await;
    handshake(&mut sender, 1).await;

    sender
        .tx
        .send(scatter("g1", "search.Find", 2_000, 0))
        .await
        .unwrap();
    let mut target_ids = Vec::new();
    for (h, name) in [(&mut a1, "a"), (&mut b1, "b")] {
        let r = expect_request(h).await;
        assert_eq!(r.mode, RequestMode::Unary as i32);
        h.tx.send(FakeClient::reply_ok(&r.id, name.as_bytes()))
            .await
            .unwrap();
        target_ids.push(r.id);
    }
    target_ids.sort();
    assert_eq!(target_ids, ["g1#0", "g1#1"]);
    let (parts, end) = gather(&mut sender).await;
    assert_eq!(parts.len(), 2);
    assert_eq!((end.parts, end.failed, end.timed_out), (2, 0, 0));
}

#[tokio::test]
async fn registrants_must_agree_on_a_subscription_type() {
    let addr = start_hub(config()).await;
    let mut a1 =
        register_subscriptions(addr, "svc-a", &[], &["prices.Watch"], &["prices.Watch"]).await;
    let mut b1 = register(addr, "svc-b", &[], &["search.Find"]).await;
    handshake(&mut a1, 1).await;
    handshake(&mut b1, 1).await;

    // A shared registrant that does not declare it a subscription type.
    let mut c1 = register(addr, "svc-c", &[], &["prices.Watch"]).await;
    let status = refused(&mut c1).await;
    assert_eq!(status.code(), tonic::Code::AlreadyExists, "{status:?}");
    assert!(
        status.message().contains("prices.Watch")
            && status.message().contains("a subscription type")
            && status.message().contains("svc-a"),
        "{status:?}"
    );

    // A registrant declaring a subscription type the others serve as a plain query.
    let mut d1 =
        register_subscriptions(addr, "svc-d", &[], &["search.Find"], &["search.Find"]).await;
    let status = refused(&mut d1).await;
    assert_eq!(status.code(), tonic::Code::AlreadyExists, "{status:?}");
    assert!(
        status.message().contains("not a subscription type"),
        "{status:?}"
    );

    // Another instance of the owner, and another shared owner, that agree are accepted.
    let mut a2 =
        register_subscriptions(addr, "svc-a", &[], &["prices.Watch"], &["prices.Watch"]).await;
    let mut e1 =
        register_subscriptions(addr, "svc-e", &[], &["prices.Watch"], &["prices.Watch"]).await;
    for h in [&mut a2, &mut e1] {
        let before = handshake(h, 1).await;
        assert!(
            matches!(before.as_slice(), [server_message::Message::RegisterAck(_)]),
            "{before:?}"
        );
    }
}

#[tokio::test]
async fn a_subscription_type_outside_the_query_lists_is_invalid_argument() {
    let addr = start_hub(config()).await;
    // In neither query list.
    let mut a1 = register_subscriptions(addr, "svc-a", &["acct.Get"], &[], &["acct.Watch"]).await;
    let status = refused(&mut a1).await;
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
    assert!(status.message().contains("acct.Watch"), "{status:?}");

    // A command is never a subscription type.
    let b1 = FakeClient::connect(addr).await;
    b1.send_raw_register(Register {
        subscription_query_types: vec!["acct.Do".to_string()],
        ..b1.register_message("svc-b", &["acct.Do"], &[])
    })
    .await;
    let mut b1 = b1;
    let status = refused(&mut b1).await;
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status:?}");
}

#[tokio::test]
async fn the_only_instance_re_registering_with_another_flag_is_refused_until_the_type_is_swept() {
    let config = Config {
        owner_grace: Duration::from_millis(400),
        heartbeat_timeout: Duration::from_millis(2_000),
        ..config()
    };
    let addr = start_hub(config).await;
    let mut a1 = register_subscriptions(addr, "svc-a", &["acct.Watch"], &[], &["acct.Watch"]).await;
    handshake(&mut a1, 1).await;

    // The same stream re-registers the type as a plain query: refused like a change between
    // exclusive and shared, as the check runs against the owner entry it holds itself.
    a1.send_raw_register(Register {
        subscription_query_types: Vec::new(),
        ..a1.register_message("svc-a", &[], &["acct.Watch"])
    })
    .await;
    let status = refused(&mut a1).await;
    assert_eq!(status.code(), tonic::Code::AlreadyExists, "{status:?}");
    assert!(
        status
            .message()
            .contains("is registered by service svc-a as a subscription type"),
        "{status:?}"
    );

    // Its stream closed, the type stays owned through the grace: a new instance with the
    // new flag is still refused.
    let mut a2 = register(addr, "svc-a", &["acct.Watch"], &[]).await;
    assert_eq!(refused(&mut a2).await.code(), tonic::Code::AlreadyExists);

    // Once the grace passed and the watchdog swept the type, the new flag is accepted.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut a3 = register(addr, "svc-a", &["acct.Watch"], &[]).await;
    let before = handshake(&mut a3, 1).await;
    assert!(
        matches!(before.as_slice(), [server_message::Message::RegisterAck(_)]),
        "{before:?}"
    );
}
