mod common;

use std::time::Duration;

use common::{FakeClient, start_hub};
use kraken_hub::config::Config;
use kraken_hub::proto::{
    ClientMessage, ErrorCode, Heartbeat, Request, RequestKind, ServerMessage, client_message,
    reply, server_message,
};
use tonic::Code;

const SHORT: Duration = Duration::from_millis(1500);

fn heartbeat(ms: i64) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Heartbeat(Heartbeat {
            sent_at_ms: ms,
        })),
    }
}

async fn expect_request(c: &mut FakeClient) -> Request {
    match c.next_within(SHORT).await.unwrap().unwrap().message {
        Some(server_message::Message::Request(r)) => r,
        other => panic!("expected request, got {other:?}"),
    }
}

async fn expect_error(c: &mut FakeClient) -> kraken_hub::proto::Error {
    match c.next_within(SHORT).await.unwrap().unwrap().message {
        Some(server_message::Message::Error(e)) => e,
        other => panic!("expected error, got {other:?}"),
    }
}

#[tokio::test]
async fn heartbeat_is_echoed_with_same_timestamp() {
    let addr = start_hub(Config::default()).await;
    let mut a = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    a.tx.send(heartbeat(123_456)).await.unwrap();
    match a.next_within(SHORT).await.unwrap().unwrap().message {
        Some(server_message::Message::Heartbeat(h)) => assert_eq!(h.sent_at_ms, 123_456),
        other => panic!("expected heartbeat, got {other:?}"),
    }
}

#[tokio::test]
async fn request_before_register_closes_with_failed_precondition() {
    let addr = start_hub(Config::default()).await;
    let mut c = FakeClient::connect(addr).await;
    c.tx.send(FakeClient::request(
        "r1",
        RequestKind::Command,
        "a.Do",
        "",
        0,
    ))
    .await
    .unwrap();
    let status = c.next_within(SHORT).await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::FailedPrecondition);
    assert_eq!(status.message(), "first message must be Register");
}

#[tokio::test]
async fn duplicate_type_owner_closes_with_already_exists() {
    let addr = start_hub(Config::default()).await;
    let mut a = FakeClient::register(addr, "svc-a", &["x.Do"], &[]).await;
    // Heartbeat echo proves A's Register was processed before the competing one is sent.
    a.tx.send(heartbeat(1)).await.unwrap();
    a.next_within(SHORT).await.unwrap().unwrap();
    let mut probe = FakeClient::register(addr, "svc-b", &["x.Do"], &[]).await;
    let status = match probe.next_within(SHORT).await.unwrap() {
        Err(s) => s,
        Ok(m) => panic!("expected error status, got {m:?}"),
    };
    assert_eq!(status.code(), Code::AlreadyExists);
    assert_eq!(status.message(), "type x.Do is owned by service svc-a");
}

#[tokio::test]
async fn command_round_trip_between_two_services() {
    let addr = start_hub(Config::default()).await;
    let mut a = FakeClient::register(addr, "a", &["b.Do"], &[]).await;
    // Heartbeat echo proves A's Register was processed.
    a.tx.send(heartbeat(1)).await.unwrap();
    a.next_within(SHORT).await.unwrap().unwrap();
    let mut b = FakeClient::register(addr, "b", &[], &[]).await;

    let mut sent = FakeClient::request("r1", RequestKind::Command, "b.Do", "", 0);
    if let Some(client_message::Message::Request(r)) = sent.message.as_mut() {
        r.payload = b"{\"x\":1}".to_vec();
        r.headers
            .insert("authorization".to_string(), "Bearer t".to_string());
    }
    let expected = match &sent.message {
        Some(client_message::Message::Request(r)) => r.clone(),
        _ => unreachable!(),
    };
    b.tx.send(sent).await.unwrap();

    let got = expect_request(&mut a).await;
    assert_eq!(got, expected);

    a.tx.send(FakeClient::reply_ok("r1", b"{\"ok\":true}"))
        .await
        .unwrap();
    match b.next_within(SHORT).await.unwrap().unwrap().message {
        Some(server_message::Message::Reply(r)) => {
            assert_eq!(r.request_id, "r1");
            assert_eq!(
                r.result,
                Some(reply::Result::Payload(b"{\"ok\":true}".to_vec()))
            );
        }
        other => panic!("expected reply, got {other:?}"),
    }
}

#[tokio::test]
async fn handler_error_reply_is_forwarded_unchanged() {
    let addr = start_hub(Config::default()).await;
    let mut a = FakeClient::register(addr, "a", &["b.Do"], &[]).await;
    a.tx.send(heartbeat(1)).await.unwrap();
    a.next_within(SHORT).await.unwrap().unwrap();
    let mut b = FakeClient::register(addr, "b", &[], &[]).await;

    b.tx.send(FakeClient::request(
        "r1",
        RequestKind::Command,
        "b.Do",
        "",
        0,
    ))
    .await
    .unwrap();
    expect_request(&mut a).await;
    a.tx.send(FakeClient::reply_err("r1", ErrorCode::HandlerError, "boom"))
        .await
        .unwrap();
    match b.next_within(SHORT).await.unwrap().unwrap().message {
        Some(server_message::Message::Reply(r)) => match r.result {
            Some(reply::Result::Error(e)) => {
                assert_eq!(e.code, ErrorCode::HandlerError as i32);
                assert_eq!(e.message, "boom");
                assert_eq!(e.request_id, "r1");
            }
            other => panic!("expected error result, got {other:?}"),
        },
        other => panic!("expected reply, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_type_gets_no_handler() {
    let addr = start_hub(Config::default()).await;
    let mut b = FakeClient::register(addr, "b", &[], &[]).await;
    b.tx.send(FakeClient::request(
        "r1",
        RequestKind::Query,
        "nope.Q",
        "",
        0,
    ))
    .await
    .unwrap();
    let e = expect_error(&mut b).await;
    assert_eq!(e.request_id, "r1");
    assert_eq!(e.code, ErrorCode::NoHandler as i32);
}

#[tokio::test]
async fn routing_key_sticks_to_one_instance_of_two() {
    let addr = start_hub(Config::default()).await;
    let mut a1 = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    let mut a2 = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    for a in [&mut a1, &mut a2] {
        a.tx.send(heartbeat(1)).await.unwrap();
        a.next_within(SHORT).await.unwrap().unwrap();
    }
    let b = FakeClient::register(addr, "b", &[], &[]).await;

    b.tx.send(FakeClient::request(
        "k0",
        RequestKind::Command,
        "a.Do",
        "agg-1",
        0,
    ))
    .await
    .unwrap();
    let a1_first = tokio::select! {
        r = a1.rx.message() => { r.unwrap().unwrap(); true }
        r = a2.rx.message() => { r.unwrap().unwrap(); false }
    };
    for i in 1..20 {
        b.tx.send(FakeClient::request(
            &format!("k{i}"),
            RequestKind::Command,
            "a.Do",
            "agg-1",
            0,
        ))
        .await
        .unwrap();
    }
    let (owner, other) = if a1_first {
        (&mut a1, &mut a2)
    } else {
        (&mut a2, &mut a1)
    };
    for i in 1..20 {
        let r = expect_request(owner).await;
        assert_eq!(r.id, format!("k{i}"));
    }
    let nothing: Result<_, _> =
        tokio::time::timeout(Duration::from_millis(100), other.rx.message()).await;
    assert!(nothing.is_err(), "other instance must receive nothing");
}

#[tokio::test]
async fn handler_stream_close_fails_in_flight_with_handler_disconnected() {
    let addr = start_hub(Config::default()).await;
    let mut a = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    a.tx.send(heartbeat(1)).await.unwrap();
    a.next_within(SHORT).await.unwrap().unwrap();
    let mut b = FakeClient::register(addr, "b", &[], &[]).await;

    b.tx.send(FakeClient::request(
        "r1",
        RequestKind::Command,
        "a.Do",
        "",
        0,
    ))
    .await
    .unwrap();
    expect_request(&mut a).await;
    drop(a);

    let e = expect_error(&mut b).await;
    assert_eq!(e.request_id, "r1");
    assert_eq!(e.code, ErrorCode::HandlerDisconnected as i32);
}

#[tokio::test]
async fn short_timeout_yields_timeout_error() {
    let addr = start_hub(Config::default()).await;
    let mut a = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    a.tx.send(heartbeat(1)).await.unwrap();
    a.next_within(SHORT).await.unwrap().unwrap();
    let mut b = FakeClient::register(addr, "b", &[], &[]).await;

    b.tx.send(FakeClient::request(
        "r1",
        RequestKind::Command,
        "a.Do",
        "",
        50,
    ))
    .await
    .unwrap();
    expect_request(&mut a).await;
    let e = expect_error(&mut b).await;
    assert_eq!(e.request_id, "r1");
    assert_eq!(e.code, ErrorCode::Timeout as i32);
}

#[tokio::test]
async fn health_service_reports_serving() {
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_client::HealthClient;

    let config = Config {
        heartbeat_timeout: Duration::from_millis(300),
        ..Config::default()
    };
    let addr = start_hub(config).await;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut health = HealthClient::new(channel);
    let resp = health
        .check(HealthCheckRequest {
            service: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, ServingStatus::Serving as i32);
    let resp = health
        .check(HealthCheckRequest {
            service: "kraken.hub.v1.Hub".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, ServingStatus::Serving as i32);
}

#[tokio::test]
async fn stale_instance_is_evicted_by_watchdog() {
    let config = Config {
        heartbeat_timeout: Duration::from_millis(300),
        ..Config::default()
    };
    let addr = start_hub(config).await;
    let mut a = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    // No heartbeats: the watchdog ends A's stream.
    assert!(a.next_within(Duration::from_millis(1200)).await.is_none());

    let mut b = FakeClient::register(addr, "b", &[], &[]).await;
    b.tx.send(FakeClient::request(
        "r1",
        RequestKind::Command,
        "a.Do",
        "",
        0,
    ))
    .await
    .unwrap();
    let e = expect_error(&mut b).await;
    assert_eq!(e.code, ErrorCode::NoHandler as i32);
}

#[tokio::test]
async fn reregister_replaces_type_list() {
    let addr = start_hub(Config::default()).await;
    let mut a = FakeClient::register(addr, "a", &["a.One"], &[]).await;
    a.send_register("a", &["a.Two"], &[]).await;
    // Heartbeat echo proves the re-Register (sent earlier on the same stream) was processed.
    a.tx.send(heartbeat(7)).await.unwrap();
    a.next_within(SHORT).await.unwrap().unwrap();
    let mut b = FakeClient::register(addr, "b", &[], &[]).await;

    b.tx.send(FakeClient::request(
        "r1",
        RequestKind::Command,
        "a.One",
        "",
        0,
    ))
    .await
    .unwrap();
    assert_eq!(expect_error(&mut b).await.code, ErrorCode::NoHandler as i32);

    b.tx.send(FakeClient::request(
        "r2",
        RequestKind::Command,
        "a.Two",
        "",
        0,
    ))
    .await
    .unwrap();
    assert_eq!(expect_request(&mut a).await.id, "r2");
}

#[allow(dead_code)]
fn _unused(_: ServerMessage) {}
