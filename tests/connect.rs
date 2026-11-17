mod common;

use std::collections::HashMap;
use std::time::Duration;

use common::{FakeClient, start_hub};
use snodra_server::config::Config;
use snodra_server::proto::{
    ClientMessage, ErrorCode, Heartbeat, Register, RegisterAck, Request, RequestKind,
    client_message, reply, server_message,
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

async fn expect_error(c: &mut FakeClient) -> snodra_server::proto::Error {
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

/// The hub's own request sessions use instance ids `snodra-server/<uuid>` (ADR 0027): a client may
/// not register one.
#[tokio::test]
async fn an_instance_id_with_the_hubs_reserved_prefix_is_refused() {
    let addr = start_hub(Config::default()).await;
    let mut client = FakeClient::connect(addr).await;
    client.instance_id = "snodra-server/1234".to_string();
    client.send_register("svc", &["svc.Do"], &[]).await;
    let status = client.expect_closed().await;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "Register.instance_id must not start with snodra-server/: the hub's own requests use it"
    );
    // Anything else is an ordinary instance id.
    let mut other = FakeClient::connect(addr).await;
    other.instance_id = "snodra-serverx/1".to_string();
    other.send_register("svc", &["svc.Do"], &[]).await;
    other.expect_ack().await;
}

/// The hub's own requests are sent as service `snodra-server`: no client may register as it.
#[tokio::test]
async fn the_hubs_own_service_name_is_refused() {
    let addr = start_hub(Config::default()).await;
    let mut client = FakeClient::connect(addr).await;
    client
        .send_register("snodra-server", &["svc.Do"], &[])
        .await;
    let status = client.expect_closed().await;
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "Register.service_name must not be snodra-server: the hub's own requests use it"
    );
    // Anything else is an ordinary service name.
    let mut other = FakeClient::connect(addr).await;
    other
        .send_register("snodra-server-tools", &["svc.Do"], &[])
        .await;
    other.expect_ack().await;
}

#[tokio::test]
async fn duplicate_type_owner_closes_with_already_exists() {
    let addr = start_hub(Config::default()).await;
    let mut a = FakeClient::register(addr, "svc-a", &["x.Do"], &[]).await;
    // Heartbeat echo proves A's Register was processed before the competing one is sent.
    sync(&mut a, 1).await;
    let mut probe = FakeClient::register_no_wait(addr, "svc-b", &["x.Do"], &[]).await;
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
    sync(&mut a, 1).await;
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
    sync(&mut a, 1).await;
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
    let first = tokio::time::timeout(SHORT, async {
        tokio::select! {
            r = a1.rx.message() => { r.unwrap().unwrap(); true }
            r = a2.rx.message() => { r.unwrap().unwrap(); false }
        }
    })
    .await
    .expect("no instance received the first request");
    let a1_first = first;
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
    sync(&mut a, 1).await;
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
    sync(&mut a, 1).await;
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
            service: "snodra.hub.v1.Hub".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.status, ServingStatus::Serving as i32);
}

/// Sends a heartbeat and skips everything up to its echo (acks included): the hub has handled
/// what this stream sent before it.
async fn sync(c: &mut FakeClient, n: i64) {
    c.tx.send(heartbeat(n)).await.unwrap();
    loop {
        if let Some(server_message::Message::Heartbeat(h)) =
            c.next_within(SHORT).await.unwrap().unwrap().message
            && h.sent_at_ms == n
        {
            return;
        }
    }
}

/// The next message, which must be the echo of the heartbeat `sent_at_ms`.
async fn expect_echo(c: &mut FakeClient, sent_at_ms: i64) {
    match c.next_within(SHORT).await.unwrap().unwrap().message {
        Some(server_message::Message::Heartbeat(h)) => assert_eq!(h.sent_at_ms, sent_at_ms),
        other => panic!("expected the echo of {sent_at_ms}, got {other:?}"),
    }
}

#[tokio::test]
async fn the_ack_carries_the_configured_values_before_the_echo() {
    let addr = start_hub(Config {
        hub_id: "hub-under-test".to_string(),
        heartbeat_interval: Duration::from_millis(1500),
        heartbeat_timeout: Duration::from_secs(7),
        default_timeout: Duration::from_millis(4000),
        max_timeout: Duration::from_millis(50_000),
        timeout_overrides: [("t.Long".to_string(), Duration::from_millis(90_000))].into(),
        ..Config::default()
    })
    .await;
    let mut c = FakeClient::register_no_wait(addr, "svc", &["t.Long"], &[]).await;
    c.heartbeat(77).await;
    assert_eq!(
        c.expect_ack().await,
        RegisterAck {
            hub_id: "hub-under-test".to_string(),
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
async fn every_accepted_register_is_acked_before_the_echo_and_the_client_version_stays() {
    let addr = start_hub(Config::default()).await;
    let mut c = FakeClient::register_no_wait(addr, "svc", &["a.One"], &[]).await;
    c.heartbeat(1).await;
    c.expect_ack().await;
    expect_echo(&mut c, 1).await;

    // A re-register is acked too; it cannot change the client version.
    c.send_raw_register(Register {
        client_version: "other/2.0".to_string(),
        ..c.register_message("svc", &["a.One", "a.Two"], &[])
    })
    .await;
    c.heartbeat(2).await;
    c.expect_ack().await;
    expect_echo(&mut c, 2).await;

    let t = snodra_server::topology_probe(addr).await.unwrap();
    let instance = &t.services[0].instances[0];
    assert_eq!(instance.client_version, "fake-client/1.0");
    assert_eq!(instance.command_types, ["a.One", "a.Two"]);
}

#[tokio::test]
async fn zero_timeout_gets_the_per_type_override() {
    let addr = start_hub(Config {
        timeout_overrides: [("t.Quick".to_string(), Duration::from_millis(200))].into(),
        ..Config::default()
    })
    .await;
    let _handler = FakeClient::register(addr, "svc-h", &["t.Quick"], &[]).await;
    let mut sender = FakeClient::register(addr, "svc-s", &[], &[]).await;
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
    let e = expect_error(&mut sender).await;
    assert_eq!(e.code(), ErrorCode::Timeout);
    assert_eq!(e.message, "no reply within 200 ms");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
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
    sync(&mut a, 7).await;
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

#[tokio::test]
async fn late_disconnect_of_old_stream_does_not_evict_reconnected_instance() {
    let addr = start_hub(Config::default()).await;
    let first = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    let mut second = FakeClient::connect(addr).await;
    second.instance_id = first.instance_id.clone();
    second.send_register("a", &["a.Do"], &[]).await;
    sync(&mut second, 1).await;

    // The first stream ends late; the replaced session's task must not touch the new one.
    drop(first);
    let mut b = FakeClient::register(addr, "b", &[], &[]).await;
    // Give the hub a round trip on b's stream so the old stream's close is likely processed.
    sync(&mut b, 2).await;

    b.tx.send(FakeClient::request(
        "r1",
        RequestKind::Command,
        "a.Do",
        "",
        0,
    ))
    .await
    .unwrap();
    assert_eq!(expect_request(&mut second).await.id, "r1");
    second
        .tx
        .send(FakeClient::reply_ok("r1", b"{}"))
        .await
        .unwrap();
    assert!(matches!(
        b.next_within(SHORT).await.unwrap().unwrap().message,
        Some(server_message::Message::Reply(_))
    ));
    // The second stream is still open: heartbeat still echoes.
    second.tx.send(heartbeat(3)).await.unwrap();
    assert!(second.next_within(SHORT).await.unwrap().is_ok());
}

#[tokio::test]
async fn rejected_reregister_closes_stream_and_unregisters() {
    let addr = start_hub(Config::default()).await;
    let mut other = FakeClient::register(addr, "other", &["o.Do"], &[]).await;
    sync(&mut other, 1).await;

    let mut a = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    sync(&mut a, 1).await;
    a.send_register("a", &["o.Do"], &[]).await;
    let status = a.next_within(SHORT).await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::AlreadyExists);
    assert!(a.next_within(SHORT).await.is_none());

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
    assert_eq!(expect_error(&mut b).await.code, ErrorCode::NoHandler as i32);
}

#[tokio::test]
async fn connect_without_register_times_out() {
    let config = Config {
        handshake_timeout: Duration::from_millis(200),
        ..Config::default()
    };
    let addr = start_hub(config).await;
    let mut c = FakeClient::connect(addr).await;
    let status = c.next_within(SHORT).await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::DeadlineExceeded);
    assert_eq!(status.message(), "no Register within 200ms");
    assert!(c.next_within(SHORT).await.is_none());
}

#[tokio::test]
async fn register_with_empty_identity_is_invalid_argument() {
    let addr = start_hub(Config::default()).await;
    let mut no_service = FakeClient::connect(addr).await;
    no_service.send_register("", &["a.Do"], &[]).await;
    let status = no_service.next_within(SHORT).await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    let mut no_instance = FakeClient::connect(addr).await;
    no_instance.instance_id = String::new();
    no_instance.send_register("a", &["a.Do"], &[]).await;
    let status = no_instance.next_within(SHORT).await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);

    // Neither registered anything.
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
    assert_eq!(expect_error(&mut b).await.code, ErrorCode::NoHandler as i32);
}

#[tokio::test]
async fn empty_request_id_is_invalid_request() {
    let addr = start_hub(Config::default()).await;
    let _a = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    let mut b = FakeClient::register(addr, "b", &[], &[]).await;
    b.tx.send(FakeClient::request("", RequestKind::Command, "a.Do", "", 0))
        .await
        .unwrap();
    let e = expect_error(&mut b).await;
    assert_eq!(e.code, ErrorCode::InvalidRequest as i32);
    assert_eq!(e.message, "invalid request: missing id");
}

#[tokio::test]
async fn health_probe_reports_serving() {
    let addr = start_hub(Config::default()).await;
    assert!(snodra_server::health_probe(addr).await.unwrap());
}

async fn shutdown_returns_promptly(with_http: bool) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let http = if with_http {
        Some(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap())
    } else {
        None
    };
    let http_addr = http.as_ref().map(|l| l.local_addr().unwrap());
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(snodra_server::serve_with_shutdown(
        Config::default(),
        listener,
        http,
        async move {
            let _ = stop_rx.await;
        },
    ));
    let mut a = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    sync(&mut a, 1).await;
    if let Some(http_addr) = http_addr {
        let (status, _) = common::http_get(http_addr, "/api/v1/topology").await;
        assert_eq!(status, 200);
    }

    stop_tx.send(()).unwrap();
    let status = a.next_within(SHORT).await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(status.message(), "hub shutting down");
    // Returns well before the 5 s drain bound: the open Connect stream was closed, not waited on.
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .expect("serve returned")
        .unwrap()
        .unwrap();
    if let Some(http_addr) = http_addr {
        assert!(
            tokio::net::TcpStream::connect(http_addr).await.is_err(),
            "http port still accepts connections after shutdown"
        );
    }
}

#[tokio::test]
async fn shutdown_closes_streams_and_returns_promptly() {
    shutdown_returns_promptly(false).await;
}

#[tokio::test]
async fn shutdown_with_http_returns_promptly_and_closes_http_port() {
    shutdown_returns_promptly(true).await;
}

#[tokio::test]
async fn register_with_type_in_both_lists_is_invalid_argument() {
    let addr = start_hub(Config::default()).await;
    let mut c = FakeClient::register_no_wait(addr, "svc", &["x.Do"], &["x.Do"]).await;
    let status = c.next_within(SHORT).await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "type x.Do listed as both command and query"
    );
}

#[tokio::test]
async fn register_kind_conflict_is_already_exists() {
    let addr = start_hub(Config::default()).await;
    let mut a = FakeClient::register(addr, "svc-a", &["x.Do"], &[]).await;
    sync(&mut a, 1).await;
    let mut b = FakeClient::register_no_wait(addr, "svc-b", &[], &["x.Do"]).await;
    let status = b.next_within(SHORT).await.unwrap().unwrap_err();
    assert_eq!(status.code(), Code::AlreadyExists);
    assert_eq!(
        status.message(),
        "type x.Do is owned as COMMAND by service svc-a"
    );
}

/// Review fix 2: a reconnect with the same instance id replaces the old session. Requests already
/// forwarded to the old stream can no longer be answered (clients reply only on the arrival
/// stream), so they must fail at once with HANDLER_DISCONNECTED instead of waiting for their
/// deadline.
#[tokio::test]
async fn replaced_session_fails_requests_forwarded_to_it() {
    let addr = start_hub(Config::default()).await;
    let mut old = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    sync(&mut old, 1).await;
    let mut b = FakeClient::register(addr, "b", &[], &[]).await;

    b.tx.send(FakeClient::request(
        "r1",
        RequestKind::Command,
        "a.Do",
        "",
        10_000,
    ))
    .await
    .unwrap();
    assert_eq!(expect_request(&mut old).await.id, "r1");

    // Same instance id on a new stream while the old one is still open.
    let mut new = FakeClient::connect(addr).await;
    new.instance_id = old.instance_id.clone();
    new.send_register("a", &["a.Do"], &[]).await;
    sync(&mut new, 2).await;

    let e = expect_error(&mut b).await;
    assert_eq!(e.request_id, "r1");
    assert_eq!(e.code(), ErrorCode::HandlerDisconnected);

    // The new session works normally.
    b.tx.send(FakeClient::request(
        "r2",
        RequestKind::Command,
        "a.Do",
        "",
        10_000,
    ))
    .await
    .unwrap();
    assert_eq!(expect_request(&mut new).await.id, "r2");
    drop(old);
}
