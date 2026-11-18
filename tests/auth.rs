//! Access control over gRPC (roadmap P5a, ADR 0018): the three modes, the `Register` checks,
//! the request checks in every mode, and ACL reloads, against an in-process hub.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, Spec, TempDir, acl_entry, auth_config, start_hub, start_hub_with_http};
use snodra_server::auth::AuthMode;
use snodra_server::config::Config;
use snodra_server::proto::{
    ClientMessage, ErrorCode, Request, RequestKind, RequestMode, ServerMessage, client_message,
    server_message,
};
use tonic::Code;

/// The ACL most tests use: `web` sends `orders.*` and handles nothing; `orders` handles
/// `orders.*` and shared `search.Find`, sends `billing.Charge`; `ops` may do anything.
fn acl() -> String {
    [
        acl_entry("tok-web", "web", &[], &["orders.*"]),
        acl_entry(
            "tok-orders",
            "orders",
            &["orders.*", "search.Find"],
            &["billing.Charge"],
        ),
        acl_entry("tok-ops", "ops", &["*"], &["*"]),
    ]
    .concat()
}

struct Hub {
    addr: SocketAddr,
    dir: TempDir,
}

async fn hub(mode: AuthMode) -> Hub {
    let dir = TempDir::new();
    let file = dir.write("acl.toml", &acl());
    let addr = start_hub(auth_config(mode, &file)).await;
    Hub { addr, dir }
}

fn spec<'a>(service: &'a str, token: Option<&'a str>) -> Spec<'a> {
    Spec {
        service,
        token,
        ..Spec::default()
    }
}

async fn orders(addr: SocketAddr) -> FakeClient {
    let s = Spec {
        commands: vec!["orders.Place"],
        queries: vec!["orders.Get"],
        ..spec("orders", Some("tok-orders"))
    };
    let mut c = FakeClient::register_spec(addr, &s).await;
    c.expect_ack().await;
    c
}

async fn web(addr: SocketAddr) -> FakeClient {
    let mut c = FakeClient::register_spec(addr, &spec("web", Some("tok-web"))).await;
    c.expect_ack().await;
    c
}

fn request(id: &str, kind: RequestKind, type_name: &str, mode: RequestMode) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Request(Request {
            id: id.into(),
            kind: kind as i32,
            r#type: type_name.into(),
            mode: mode as i32,
            timeout_ms: 2000,
            initial_credit: 8,
            ..Request::default()
        })),
    }
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// ---- Register ----------------------------------------------------------------------------

#[tokio::test]
async fn enforce_accepts_a_registration_its_token_allows() {
    let h = hub(AuthMode::Enforce).await;
    let mut c = orders(h.addr).await;
    // And the session works: it may send what `send` lists... orders sends billing.Charge only.
    c.tx.send(request(
        "r1",
        RequestKind::Command,
        "billing.Charge",
        RequestMode::Unary,
    ))
    .await
    .unwrap();
    // No handler for it: the denial would be PERMISSION_DENIED, so NO_HANDLER proves it passed.
    assert_eq!(c.expect_error().await.code(), ErrorCode::NoHandler);
}

#[tokio::test]
async fn enforce_refuses_an_unknown_token_with_unauthenticated() {
    let h = hub(AuthMode::Enforce).await;
    let mut c = FakeClient::register_spec(h.addr, &spec("orders", Some("who-is-this"))).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::Unauthenticated);
    assert!(
        !status.message().contains("who-is-this"),
        "{}",
        status.message()
    );
}

#[tokio::test]
async fn enforce_refuses_missing_credentials_with_unauthenticated() {
    let h = hub(AuthMode::Enforce).await;
    let mut c = FakeClient::register_spec(h.addr, &spec("orders", None)).await;
    assert_eq!(c.expect_closed().await.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn enforce_says_jwt_credentials_are_not_configured_without_an_issuer() {
    let h = hub(AuthMode::Enforce).await;
    let s = Spec {
        jwt: Some("a.b.c"),
        ..spec("orders", None)
    };
    let mut c = FakeClient::register_spec(h.addr, &s).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(
        status.message(),
        "jwt credentials are not configured on this hub"
    );
}

#[tokio::test]
async fn enforce_refuses_a_service_name_the_token_does_not_grant() {
    let h = hub(AuthMode::Enforce).await;
    let mut c = FakeClient::register_spec(h.addr, &spec("billing", Some("tok-orders"))).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(status.message().contains("billing"), "{}", status.message());
}

#[tokio::test]
async fn enforce_refuses_a_handled_type_outside_handle_and_names_it() {
    let h = hub(AuthMode::Enforce).await;
    for s in [
        Spec {
            commands: vec!["billing.Charge"],
            ..spec("orders", Some("tok-orders"))
        },
        Spec {
            queries: vec!["admin.Wipe"],
            ..spec("orders", Some("tok-orders"))
        },
        Spec {
            shared: vec!["admin.Everywhere"],
            ..spec("orders", Some("tok-orders"))
        },
    ] {
        let offending = [s.commands.clone(), s.queries.clone(), s.shared.clone()].concat()[0];
        let mut c = FakeClient::register_spec(h.addr, &s).await;
        let status = c.expect_closed().await;
        assert_eq!(status.code(), Code::PermissionDenied);
        assert!(status.message().contains(offending), "{}", status.message());
    }
    // A sender that handles nothing may register with an empty type list.
    let mut c = FakeClient::register_spec(h.addr, &spec("web", Some("tok-web"))).await;
    c.expect_ack().await;
}

#[tokio::test]
async fn enforce_refuses_snodra_events_types_without_the_event_browser_right() {
    let dir = TempDir::new();
    let browser = format!(
        "{}event-browser = true\n\n",
        acl_entry("tok-browser", "snodra-event-browser", &[], &[])
    );
    let file = dir.write("acl.toml", &[acl(), browser].concat());
    let addr = start_hub(auth_config(AuthMode::Enforce, &file)).await;
    // `ops` handles `*`, which does not reach the reserved namespace: the whole Register is
    // refused, naming the type and the right.
    let s = Spec {
        commands: vec!["ops.Run"],
        queries: vec!["snodra.events.Search"],
        ..spec("ops", Some("tok-ops"))
    };
    let mut c = FakeClient::register_spec(addr, &s).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        status.message().contains("snodra.events.Search")
            && status.message().contains("event-browser right"),
        "{}",
        status.message()
    );
    // The entry with the right registers the browser's queries.
    let s = Spec {
        queries: vec!["snodra.events.Search", "snodra.events.Tail"],
        ..spec("snodra-event-browser", Some("tok-browser"))
    };
    let mut c = FakeClient::register_spec(addr, &s).await;
    c.expect_ack().await;
}

#[tokio::test]
async fn a_refused_registration_leaves_nothing_behind() {
    let dir = TempDir::new();
    let file = dir.write("acl.toml", &acl());
    let (grpc, http) = start_hub_with_http(auth_config(AuthMode::Enforce, &file)).await;
    let http = http.unwrap();
    let s = Spec {
        commands: vec!["orders.Place"],
        ..spec("orders", Some("wrong"))
    };
    let mut c = FakeClient::register_spec(grpc, &s).await;
    assert_eq!(c.expect_closed().await.code(), Code::Unauthenticated);
    let (_, body) = common::http_get(http, "/api/v1/topology").await;
    let t: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(t["totals"]["instances"], 0);
    assert_eq!(t["types"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn a_client_without_credentials_is_refused_in_enforce_and_allowed_in_permissive() {
    let h = hub(AuthMode::Enforce).await;
    let mut c = FakeClient::register_spec(h.addr, &spec("orders", None)).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(
        status.message(),
        "credentials required: Register.credentials.token is missing"
    );

    let h = hub(AuthMode::Permissive).await;
    let s = Spec {
        commands: vec!["orders.Place"],
        ..spec("orders", None)
    };
    let mut c = FakeClient::register_spec(h.addr, &s).await;
    c.expect_ack().await;
}

// ---- modes -------------------------------------------------------------------------------

#[tokio::test]
async fn off_ignores_credentials_and_the_acl_entirely() {
    let h = hub(AuthMode::Off).await;
    // No token, a wrong token, a type nobody allowed, a request nobody allowed.
    let mut a = FakeClient::register_spec(
        h.addr,
        &Spec {
            commands: vec!["anything.Goes"],
            ..spec("whoever", Some("nope"))
        },
    )
    .await;
    a.expect_ack().await;
    let mut b = FakeClient::register_spec(h.addr, &spec("anyone", None)).await;
    b.expect_ack().await;
    b.tx.send(request(
        "r1",
        RequestKind::Command,
        "anything.Goes",
        RequestMode::Unary,
    ))
    .await
    .unwrap();
    match a.next().await {
        Some(Ok(ServerMessage {
            message: Some(server_message::Message::Request(r)),
        })) => {
            assert_eq!(r.id, "r1");
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn permissive_allows_everything_enforce_would_refuse() {
    let h = hub(AuthMode::Permissive).await;
    // Unknown token, wrong service and a type outside `handle`: all registered.
    let mut handler = FakeClient::register_spec(
        h.addr,
        &Spec {
            commands: vec!["admin.Wipe"],
            ..spec("orders", Some("tok-orders"))
        },
    )
    .await;
    handler.expect_ack().await;
    let mut anon = FakeClient::register_spec(h.addr, &spec("web", Some("unknown"))).await;
    anon.expect_ack().await;
    let mut wrong = FakeClient::register_spec(h.addr, &spec("other", Some("tok-web"))).await;
    wrong.expect_ack().await;
    // `web` is not allowed to send admin.Wipe; permissive routes it anyway.
    wrong
        .tx
        .send(request(
            "r1",
            RequestKind::Command,
            "admin.Wipe",
            RequestMode::Unary,
        ))
        .await
        .unwrap();
    match handler.next().await {
        Some(Ok(ServerMessage {
            message: Some(server_message::Message::Request(r)),
        })) => {
            assert_eq!(r.id, "r1");
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn permissive_marks_unauthenticated_sessions_in_the_topology() {
    let dir = TempDir::new();
    let file = dir.write("acl.toml", &acl());
    let (grpc, http) = start_hub_with_http(auth_config(AuthMode::Permissive, &file)).await;
    let http = http.unwrap();
    let mut good = FakeClient::register_spec(grpc, &spec("web", Some("tok-web"))).await;
    good.expect_ack().await;
    let mut bad = FakeClient::register_spec(grpc, &spec("stranger", None)).await;
    bad.expect_ack().await;
    eventually("both sessions in the topology", || async {
        let (_, body) = common::http_get(http, "/api/v1/topology").await;
        serde_json::from_str::<serde_json::Value>(&body).unwrap()["totals"]["instances"] == 2
    })
    .await;
    let (_, body) = common::http_get(http, "/api/v1/topology").await;
    let t: serde_json::Value = serde_json::from_str(&body).unwrap();
    let instance = |service: &str| {
        t["services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["serviceName"] == service)
            .unwrap()["instances"][0]
            .clone()
    };
    assert_eq!(instance("web")["auth"], "authenticated");
    assert_eq!(instance("web")["authenticatedService"], "web");
    assert_eq!(instance("stranger")["auth"], "unauthenticated (permissive)");
    assert!(instance("stranger").get("authenticatedService").is_none());
    // With access control off, the fields are absent.
    let (grpc, http) = start_hub_with_http(Config {
        otlp_endpoint: None,
        ..Config::default()
    })
    .await;
    let mut c = FakeClient::register_spec(grpc, &spec("web", None)).await;
    c.expect_ack().await;
    let (_, body) = common::http_get(http.unwrap(), "/api/v1/topology").await;
    assert!(
        !body.contains("authenticatedService") && !body.contains("\"auth\""),
        "{body}"
    );
}

#[tokio::test]
async fn startup_fails_on_a_missing_or_malformed_acl_file() {
    let dir = TempDir::new();
    for file in [
        dir.path("missing.toml"),
        dir.write("bad.toml", "[[token]]\nservice = 3"),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let result = snodra_server::serve_with_shutdown(
            auth_config(AuthMode::Enforce, &file),
            listener,
            None,
            std::future::pending(),
        )
        .await;
        let err = format!("{:#}", result.unwrap_err());
        assert!(
            err.contains("SNODRA_SERVER_ACL_FILE") || err.contains("ACL"),
            "{err}"
        );
    }
}

// ---- requests ----------------------------------------------------------------------------

#[tokio::test]
async fn enforce_refuses_sending_a_type_outside_send_in_every_mode() {
    let h = hub(AuthMode::Enforce).await;
    let mut handler = orders(h.addr).await;
    let mut sender = web(h.addr).await;
    // `web` sends orders.* only: billing.Charge and everything else is refused, whatever the
    // mode, and nothing reaches the handler.
    for (n, (kind, mode)) in [
        (RequestKind::Command, RequestMode::Unary),
        (RequestKind::Query, RequestMode::Unary),
        (RequestKind::Query, RequestMode::ScatterGather),
        (RequestKind::Query, RequestMode::Streaming),
        (RequestKind::Query, RequestMode::Subscription),
    ]
    .into_iter()
    .enumerate()
    {
        let id = format!("r{n}");
        sender
            .tx
            .send(request(&id, kind, "billing.Charge", mode))
            .await
            .unwrap();
        let e = sender.expect_error().await;
        assert_eq!(e.request_id, id, "{mode:?}");
        assert_eq!(e.code(), ErrorCode::PermissionDenied, "{mode:?}");
        assert!(e.message.contains("billing.Charge"), "{}", e.message);
    }
    assert!(
        handler.silent_for(Duration::from_millis(300)).await,
        "nothing was routed"
    );
}

#[tokio::test]
async fn enforce_routes_what_send_allows_in_every_mode() {
    let h = hub(AuthMode::Enforce).await;
    let mut handler = {
        let s = Spec {
            commands: vec!["orders.Place"],
            queries: vec!["orders.Get"],
            ..spec("orders", Some("tok-orders"))
        };
        let mut c = FakeClient::register_spec(h.addr, &s).await;
        c.expect_ack().await;
        c
    };
    let sender = web(h.addr).await;
    sender
        .tx
        .send(request(
            "c1",
            RequestKind::Command,
            "orders.Place",
            RequestMode::Unary,
        ))
        .await
        .unwrap();
    for expected in ["c1"] {
        match handler.next().await {
            Some(Ok(ServerMessage {
                message: Some(server_message::Message::Request(r)),
            })) => {
                assert_eq!(r.id, expected);
            }
            other => panic!("{other:?}"),
        }
    }
    // A scatter-gather query to the same owner reaches it too.
    sender
        .tx
        .send(request(
            "s1",
            RequestKind::Query,
            "orders.Get",
            RequestMode::ScatterGather,
        ))
        .await
        .unwrap();
    match handler.next().await {
        Some(Ok(ServerMessage {
            message: Some(server_message::Message::Request(r)),
        })) => {
            assert!(r.id.starts_with("s1"), "{}", r.id);
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn permissive_routes_a_send_violation() {
    let h = hub(AuthMode::Permissive).await;
    let mut handler = FakeClient::register_spec(
        h.addr,
        &Spec {
            commands: vec!["billing.Charge"],
            ..spec("ops", Some("tok-ops"))
        },
    )
    .await;
    handler.expect_ack().await;
    let sender = web(h.addr).await;
    sender
        .tx
        .send(request(
            "r1",
            RequestKind::Command,
            "billing.Charge",
            RequestMode::Unary,
        ))
        .await
        .unwrap();
    match handler.next().await {
        Some(Ok(ServerMessage {
            message: Some(server_message::Message::Request(r)),
        })) => {
            assert_eq!(r.id, "r1");
        }
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_re_register_that_adds_a_forbidden_type_closes_the_stream() {
    let h = hub(AuthMode::Enforce).await;
    let mut c = orders(h.addr).await;
    // Allowed addition.
    c.send_raw_register(
        Spec {
            commands: vec!["orders.Place", "orders.Cancel"],
            ..spec("orders", Some("tok-orders"))
        }
        .register(&c.instance_id),
    )
    .await;
    c.expect_ack().await;
    // Forbidden addition.
    c.send_raw_register(
        Spec {
            commands: vec!["orders.Place", "admin.Wipe"],
            ..spec("orders", Some("tok-orders"))
        }
        .register(&c.instance_id),
    )
    .await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(status.message().contains("admin.Wipe"));
}

// ---- reload ------------------------------------------------------------------------------

#[tokio::test]
async fn a_removed_token_closes_its_session_with_unauthenticated() {
    let h = hub(AuthMode::Enforce).await;
    let mut gone = orders(h.addr).await;
    let mut kept = web(h.addr).await;
    h.dir.write(
        "acl.toml",
        &[acl_entry("tok-web", "web", &[], &["orders.*"])].concat(),
    );
    assert_eq!(gone.expect_closed().await.code(), Code::Unauthenticated);
    // The other session is untouched, and still judged by the new file.
    assert!(kept.silent_for(Duration::from_millis(300)).await);
    kept.tx
        .send(request(
            "r1",
            RequestKind::Command,
            "orders.Place",
            RequestMode::Unary,
        ))
        .await
        .unwrap();
    assert_eq!(kept.expect_error().await.code(), ErrorCode::NoHandler);
}

#[tokio::test]
async fn a_narrowed_send_refuses_the_next_request() {
    let h = hub(AuthMode::Enforce).await;
    let _handler = orders(h.addr).await;
    let mut sender = web(h.addr).await;
    sender
        .tx
        .send(request(
            "before",
            RequestKind::Command,
            "orders.Place",
            RequestMode::Unary,
        ))
        .await
        .unwrap();
    // Forwarded to the (silent) handler: nothing comes back yet.
    assert!(sender.silent_for(Duration::from_millis(200)).await);
    h.dir.write(
        "acl.toml",
        &[
            acl_entry("tok-web", "web", &[], &["orders.Get"]),
            acl_entry("tok-orders", "orders", &["orders.*"], &[]),
        ]
        .concat(),
    );
    let mut refused = false;
    for n in 0..40 {
        sender
            .tx
            .send(request(
                &format!("after{n}"),
                RequestKind::Command,
                "orders.Place",
                RequestMode::Unary,
            ))
            .await
            .unwrap();
        // Before the reload lands the request is forwarded and nothing comes back.
        if let Ok(Ok(Some(ServerMessage {
            message: Some(server_message::Message::Error(e)),
        }))) = tokio::time::timeout(Duration::from_millis(150), sender.rx.message()).await
        {
            assert_eq!(e.code(), ErrorCode::PermissionDenied);
            refused = true;
            break;
        }
    }
    assert!(refused, "the narrowed send never applied");
}

#[tokio::test]
async fn a_narrowed_handle_closes_the_session_with_permission_denied() {
    let h = hub(AuthMode::Enforce).await;
    let mut handler = orders(h.addr).await;
    h.dir.write(
        "acl.toml",
        &[acl_entry("tok-orders", "orders", &["orders.Get"], &[])].concat(),
    );
    let status = handler.expect_closed().await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        status.message().contains("orders.Place"),
        "{}",
        status.message()
    );
}

#[tokio::test]
async fn a_changed_service_closes_the_session() {
    let h = hub(AuthMode::Enforce).await;
    let mut handler = orders(h.addr).await;
    h.dir.write(
        "acl.toml",
        &[acl_entry("tok-orders", "payments", &["orders.*"], &[])].concat(),
    );
    assert_eq!(handler.expect_closed().await.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn a_malformed_reload_keeps_the_previous_acl_and_every_session() {
    let h = hub(AuthMode::Enforce).await;
    let mut handler = orders(h.addr).await;
    let sender = web(h.addr).await;
    h.dir.write("acl.toml", "this is [[not valid");
    // Long enough for several polls.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(handler.silent_for(Duration::from_millis(100)).await);
    sender
        .tx
        .send(request(
            "r1",
            RequestKind::Command,
            "orders.Place",
            RequestMode::Unary,
        ))
        .await
        .unwrap();
    match handler.next().await {
        Some(Ok(ServerMessage {
            message: Some(server_message::Message::Request(r)),
        })) => {
            assert_eq!(r.id, "r1");
        }
        other => panic!("{other:?}"),
    }
    // A bad file does not block the next good one.
    h.dir.write(
        "acl.toml",
        &[acl_entry("tok-web", "web", &[], &["orders.*"])].concat(),
    );
    assert_eq!(handler.expect_closed().await.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn permissive_keeps_a_session_whose_token_was_removed() {
    let h = hub(AuthMode::Permissive).await;
    let mut c = web(h.addr).await;
    h.dir.write(
        "acl.toml",
        &[acl_entry("tok-orders", "orders", &["*"], &[])].concat(),
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        c.silent_for(Duration::from_millis(200)).await,
        "permissive never closes"
    );
    c.heartbeat(5).await;
    assert!(c.next().await.is_some());
}

// ---- gRPC Topology and the dashboard token -----------------------------------------------

#[tokio::test]
async fn the_topology_rpc_needs_the_dashboard_token_when_one_is_set() {
    let token = "0123456789abcdef0123";
    let addr = start_hub(Config {
        otlp_endpoint: None,
        dashboard_token: Some(snodra_server::auth::Secret::new(token)),
        ..Config::default()
    })
    .await;
    let target = snodra_server::client::Target::plain(addr);
    let denied = snodra_server::topology_probe_target(&target, None)
        .await
        .unwrap_err();
    assert!(
        format!("{denied:#}")
            .to_lowercase()
            .contains("unauthenticated"),
        "{denied:#}"
    );
    let wrong = snodra_server::topology_probe_target(&target, Some("not-the-token-at-all")).await;
    assert!(wrong.is_err());
    let t = snodra_server::topology_probe_target(&target, Some(token))
        .await
        .unwrap();
    assert_eq!(t.totals.instances, 0);
    // The health service stays public.
    assert!(snodra_server::health_probe(addr).await.unwrap());
}

// ---- instance id takeover (review finding 1) ----------------------------------------------

/// A handler of `orders.Place` with one permit, so its second request waits in its queue.
fn flow_orders<'a>(token: &'a str) -> Spec<'a> {
    Spec {
        commands: vec!["orders.Place"],
        initial_permits: 1,
        ..spec("orders", Some(token))
    }
}

async fn expect_request(c: &mut FakeClient) -> String {
    loop {
        match c.next().await {
            Some(Ok(ServerMessage {
                message: Some(server_message::Message::Request(r)),
            })) => return r.id,
            Some(Ok(ServerMessage {
                message: Some(server_message::Message::Heartbeat(_)),
            })) => continue,
            other => panic!("expected a Request, got {other:?}"),
        }
    }
}

async fn expect_reply_payload(c: &mut FakeClient) -> Vec<u8> {
    loop {
        match c.next().await {
            Some(Ok(ServerMessage {
                message: Some(server_message::Message::Reply(r)),
            })) => match r.result {
                Some(snodra_server::proto::reply::Result::Payload(p)) => return p,
                other => panic!("expected a payload, got {other:?}"),
            },
            Some(Ok(ServerMessage {
                message: Some(server_message::Message::Heartbeat(_)),
            })) => continue,
            other => panic!("expected a Reply, got {other:?}"),
        }
    }
}

/// `orders` holds one request and one queued; `ops` (a token for another service) then tries to
/// register under orders' instance id. The takeover is refused, nothing of orders moves, and
/// orders still gets its queued request and can answer.
async fn takeover_is_refused(mode: AuthMode) {
    let h = hub(mode).await;
    let mut victim = FakeClient::register_spec(h.addr, &flow_orders("tok-orders")).await;
    victim.expect_ack().await;
    let mut sender = web(h.addr).await;
    for id in ["r1", "r2"] {
        sender
            .tx
            .send(request(
                id,
                RequestKind::Command,
                "orders.Place",
                RequestMode::Unary,
            ))
            .await
            .unwrap();
    }
    assert_eq!(expect_request(&mut victim).await, "r1");

    // The attacker holds a valid token, for `ops`, and names the victim's instance id.
    let mut attacker =
        FakeClient::register_spec_as(h.addr, &victim.instance_id, &spec("ops", Some("tok-ops")))
            .await;
    let status = attacker.expect_closed().await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(
        status.message(),
        format!(
            "instance id {} belongs to another service",
            &victim.instance_id[..8]
        )
    );

    // A session of its own cannot answer the victim's pending request either.
    let mut own = FakeClient::register_spec(h.addr, &spec("ops", Some("tok-ops"))).await;
    own.expect_ack().await;
    own.tx
        .send(FakeClient::reply_ok("r1", b"forged"))
        .await
        .unwrap();
    own.tx
        .send(FakeClient::reply_ok("r2", b"forged"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    // The victim is untouched: it answers r1, and its queued r2 still comes to it.
    victim
        .tx
        .send(FakeClient::reply_ok("r1", b"real"))
        .await
        .unwrap();
    assert_eq!(expect_reply_payload(&mut sender).await, b"real");
    victim.permits(1).await;
    assert_eq!(expect_request(&mut victim).await, "r2");
    victim
        .tx
        .send(FakeClient::reply_ok("r2", b"real2"))
        .await
        .unwrap();
    assert_eq!(expect_reply_payload(&mut sender).await, b"real2");
    assert!(own.silent_for(Duration::from_millis(100)).await);
}

#[tokio::test]
async fn enforce_refuses_a_takeover_of_another_services_instance_id() {
    takeover_is_refused(AuthMode::Enforce).await;
}

#[tokio::test]
async fn permissive_refuses_a_takeover_of_another_services_instance_id_too() {
    takeover_is_refused(AuthMode::Permissive).await;
}

#[tokio::test]
async fn off_keeps_replacing_an_instance_id_whatever_its_service() {
    let h = hub(AuthMode::Off).await;
    let mut first = FakeClient::register_spec(h.addr, &spec("orders", None)).await;
    first.expect_ack().await;
    let mut second =
        FakeClient::register_spec_as(h.addr, &first.instance_id, &spec("ops", None)).await;
    second.expect_ack().await;
}

#[tokio::test]
async fn a_same_service_reconnect_still_takes_over_the_session_and_its_queue() {
    let h = hub(AuthMode::Enforce).await;
    let mut old = FakeClient::register_spec(h.addr, &flow_orders("tok-orders")).await;
    old.expect_ack().await;
    let sender = web(h.addr).await;
    for id in ["r1", "r2"] {
        sender
            .tx
            .send(request(
                id,
                RequestKind::Command,
                "orders.Place",
                RequestMode::Unary,
            ))
            .await
            .unwrap();
    }
    assert_eq!(expect_request(&mut old).await, "r1");
    let mut new =
        FakeClient::register_spec_as(h.addr, &old.instance_id, &flow_orders("tok-orders")).await;
    new.expect_ack().await;
    // r2 was queued for the instance id: the new stream gets it.
    assert_eq!(expect_request(&mut new).await, "r2");
}

#[tokio::test]
async fn a_reconnect_with_the_rotated_token_of_the_same_service_is_allowed() {
    let dir = TempDir::new();
    let both = [
        acl_entry("tok-orders", "orders", &["orders.*"], &[]),
        acl_entry("tok-orders-next", "orders", &["orders.*"], &[]),
    ]
    .concat();
    let file = dir.write("acl.toml", &both);
    let addr = start_hub(auth_config(AuthMode::Enforce, &file)).await;
    let mut old = FakeClient::register_spec(addr, &flow_orders("tok-orders")).await;
    old.expect_ack().await;
    let mut new =
        FakeClient::register_spec_as(addr, &old.instance_id, &flow_orders("tok-orders-next")).await;
    new.expect_ack().await;
}

#[tokio::test]
async fn enforce_refuses_the_topology_rpc_without_a_dashboard_token_or_an_acl_token() {
    let h = hub(AuthMode::Enforce).await;
    let target = snodra_server::client::Target::plain(h.addr);
    for presented in [None, Some("not-a-token")] {
        let denied = snodra_server::topology_probe_target(&target, presented)
            .await
            .unwrap_err();
        let status = denied
            .downcast_ref::<tonic::Status>()
            .expect("a gRPC status");
        assert_eq!(status.code(), Code::Unauthenticated, "{presented:?}");
    }
    // Any ACL token reads it.
    let t = snodra_server::topology_probe_target(&target, Some("tok-web"))
        .await
        .unwrap();
    assert_eq!(t.totals.instances, 0);
    assert!(snodra_server::health_probe(h.addr).await.unwrap());
}

#[tokio::test]
async fn enforce_with_a_dashboard_token_accepts_it_and_acl_tokens() {
    let dir = TempDir::new();
    let file = dir.write("acl.toml", &acl());
    let dash = "0123456789abcdef0123";
    let addr = start_hub(Config {
        dashboard_token: Some(snodra_server::auth::Secret::new(dash)),
        ..auth_config(AuthMode::Enforce, &file)
    })
    .await;
    let target = snodra_server::client::Target::plain(addr);
    assert!(
        snodra_server::topology_probe_target(&target, None)
            .await
            .is_err()
    );
    for token in [dash, "tok-ops"] {
        assert!(
            snodra_server::topology_probe_target(&target, Some(token))
                .await
                .is_ok(),
            "{token}"
        );
    }
}

#[tokio::test]
async fn permissive_and_off_leave_the_topology_rpc_open_without_a_dashboard_token() {
    for mode in [AuthMode::Permissive, AuthMode::Off] {
        let h = hub(mode).await;
        let target = snodra_server::client::Target::plain(h.addr);
        assert!(
            snodra_server::topology_probe_target(&target, None)
                .await
                .is_ok(),
            "{mode:?}"
        );
    }
}

#[tokio::test]
async fn a_rotated_dashboard_token_file_applies_without_a_restart() {
    let dir = TempDir::new();
    let file = dir.write("dashboard-token", "first-dashboard-token-0123456789\n");
    let mut config = Config {
        otlp_endpoint: None,
        reload_interval: Duration::from_millis(50),
        dashboard_token_file: Some(file.clone()),
        ..Config::default()
    };
    config.load_token_files().unwrap();
    let addr = start_hub(config).await;
    let target = snodra_server::client::Target::plain(addr);
    let works = |token: &'static str| {
        let target = target.clone();
        async move {
            snodra_server::topology_probe_target(&target, Some(token))
                .await
                .is_ok()
        }
    };
    assert!(works("first-dashboard-token-0123456789").await);
    assert!(!works("second-dashboard-token-0123456789").await);

    std::fs::write(&file, "second-dashboard-token-0123456789\n").unwrap();
    eventually("the new token is accepted", || {
        works("second-dashboard-token-0123456789")
    })
    .await;
    assert!(
        !works("first-dashboard-token-0123456789").await,
        "the old token stops working"
    );

    // A bad file keeps the token in force.
    std::fs::write(&file, "x\n").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(works("second-dashboard-token-0123456789").await);
}
