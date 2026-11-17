//! JWT credentials over gRPC (roadmap P5b, ADR 0019): admission against a JWKS served by an
//! in-process HTTP server, the `[[service]]` ACL, expiry, refresh, reloads and the topology.
//! Signing keys are generated here; nothing is checked in.

mod common;
#[path = "../src/jwt_test_support.rs"]
mod keys;

use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use common::{FakeClient, Spec, TempDir, acl_entry, auth_config};
use keys::{JwksServer, Signer, claims, forge_hs256, forge_none, now};
use serde_json::json;
use snodra_server::auth::AuthMode;
use snodra_server::config::Config;
use snodra_server::proto::{
    ClientMessage, Credentials, ErrorCode, Request, RequestKind, RequestMode, client_message,
    credentials,
};
use tonic::{Code, Status};

const ISS: &str = "https://sso.example/realms/kraken";
const AUD: &str = "snodra-server";

fn service_entry(name: &str, handle: &[&str], send: &[&str]) -> String {
    let list = |items: &[&str]| {
        items
            .iter()
            .map(|i| format!("\"{i}\""))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "[[service]]\nname = \"{name}\"\nhandle = [{}]\nsend = [{}]\n\n",
        list(handle),
        list(send)
    )
}

/// Token entries (for the kind-switching tests) and service tables. `orders` handles `orders.*`
/// and `search.Find` and sends `billing.Charge`; `web` sends `orders.*`; `probe` may do nothing.
fn acl() -> String {
    [
        acl_entry(
            "tok-orders",
            "orders",
            &["orders.*", "search.Find"],
            &["billing.Charge"],
        ),
        acl_entry("tok-orders-narrow", "orders", &["orders.Get"], &[]),
        acl_entry("tok-web", "web", &[], &["orders.*"]),
        service_entry("orders", &["orders.*", "search.Find"], &["billing.Charge"]),
        service_entry("web", &[], &["orders.*"]),
        service_entry("probe", &[], &[]),
    ]
    .concat()
}

struct Opts {
    mode: AuthMode,
    grace: Duration,
    audience: Option<&'static str>,
    /// `(interval, timeout)` of the heartbeat; the hub's defaults when unset.
    heartbeat: Option<(Duration, Duration)>,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            mode: AuthMode::Enforce,
            grace: Duration::from_secs(30),
            audience: None,
            heartbeat: None,
        }
    }
}

struct Hub {
    addr: SocketAddr,
    http: Option<SocketAddr>,
    dir: TempDir,
    key: Signer,
    jwks: JwksServer,
}

impl Hub {
    async fn start(opts: Opts) -> Self {
        let key = Signer::es256("k1");
        let jwks = JwksServer::start(&[&key]).await;
        Self::start_with(opts, key, jwks, true).await
    }

    async fn start_with(opts: Opts, key: Signer, jwks: JwksServer, wait: bool) -> Self {
        let dir = TempDir::new();
        let file = dir.write("acl.toml", &acl());
        let config = Config {
            http_listen: Some("127.0.0.1:0".parse().unwrap()),
            jwt_issuer: Some(ISS.into()),
            jwt_jwks_uri: Some(jwks.url()),
            jwt_audience: opts.audience.map(str::to_string),
            jwt_expiry_grace: opts.grace,
            jwks_refresh: Duration::from_secs(3600),
            jwks_refetch_interval: Duration::from_millis(100),
            ..auth_config(opts.mode, &file)
        };
        let config = match opts.heartbeat {
            Some((interval, timeout)) => Config {
                heartbeat_interval: interval,
                heartbeat_timeout: timeout,
                ..config
            },
            None => config,
        };
        let (addr, http) = common::start_hub_with_http(config).await;
        let hub = Self {
            addr,
            http,
            dir,
            key,
            jwks,
        };
        if wait {
            hub.wait_loaded().await;
        }
        hub
    }

    /// A token for `azp` that expires in `exp_in` seconds.
    fn jwt(&self, azp: &str, exp_in: i64) -> String {
        self.key.sign(&claims(ISS, azp, exp_in))
    }

    /// Returns once the hub's first fetch of the key set is done (a registration is not
    /// refused as "not loaded yet").
    async fn wait_loaded(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let t = self.jwt("probe", 60);
            let mut c = FakeClient::register_spec(self.addr, &spec("probe", &t)).await;
            match c.next().await {
                Some(Ok(_)) => return,
                Some(Err(status)) if status.code() == Code::Unavailable => {}
                // Any other answer means the keys are loaded (a hub with an audience refuses
                // the probe's token for another reason).
                Some(Err(_)) => return,
                other => panic!("the probe registration failed: {other:?}"),
            }
            assert!(Instant::now() < deadline, "the hub never loaded its keys");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn acl_file(&self) -> std::path::PathBuf {
        self.dir.path("acl.toml")
    }
}

fn spec<'a>(service: &'a str, jwt: &'a str) -> Spec<'a> {
    Spec {
        service,
        jwt: Some(jwt),
        ..Spec::default()
    }
}

fn orders_spec<'a>(jwt: &'a str) -> Spec<'a> {
    Spec {
        commands: vec!["orders.Place"],
        queries: vec!["orders.Get"],
        ..spec("orders", jwt)
    }
}

async fn registered(hub: &Hub, spec: &Spec<'_>) -> FakeClient {
    let mut c = FakeClient::register_spec(hub.addr, spec).await;
    c.expect_ack().await;
    c
}

fn refresh_jwt(token: &str) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Credentials(Credentials {
            kind: Some(credentials::Kind::Jwt(token.to_string())),
        })),
    }
}

fn refresh_token(token: &str) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Credentials(Credentials {
            kind: Some(credentials::Kind::Token(token.to_string())),
        })),
    }
}

fn request(id: &str, type_name: &str) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Request(Request {
            id: id.into(),
            kind: RequestKind::Command as i32,
            r#type: type_name.into(),
            mode: RequestMode::Unary as i32,
            timeout_ms: 2000,
            ..Request::default()
        })),
    }
}

/// True when the session answers a request for a type it may send (`orders` sends
/// `billing.Charge`; nobody handles it, so NO_HANDLER proves the request was let through).
async fn works(c: &mut FakeClient) -> bool {
    c.tx.send(request("alive", "billing.Charge")).await.unwrap();
    loop {
        match c.next().await {
            Some(Ok(m)) => match m.message {
                Some(snodra_server::proto::server_message::Message::Error(e)) => {
                    return e.code == ErrorCode::NoHandler as i32;
                }
                _ => continue,
            },
            _ => return false,
        }
    }
}

/// How the hub's topology says `service`'s first instance authenticated (waits for it to show).
async fn auth_kind(hub: &Hub, service: &str) -> String {
    let http = hub.http.expect("an HTTP listener");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (_, body) = common::http_get(http, "/api/v1/topology").await;
        let t: serde_json::Value = serde_json::from_str(&body).unwrap();
        let found = t["services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["serviceName"] == service)
            .map(|s| {
                s["instances"][0]["authKind"]
                    .as_str()
                    .unwrap_or("")
                    .to_string()
            });
        if let Some(kind) = found {
            return kind;
        }
        assert!(Instant::now() < deadline, "{service} never showed");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Waits up to `d` for the hub to close the stream.
async fn closed_within(c: &mut FakeClient, d: Duration) -> Status {
    let deadline = Instant::now() + d;
    loop {
        let left = deadline
            .checked_duration_since(Instant::now())
            .expect("the stream was not closed in time");
        match c.next_within(left).await {
            Some(Err(status)) => return status,
            Some(Ok(_)) => continue,
            None => panic!("the stream ended without a status"),
        }
    }
}

// ---- admission ----------------------------------------------------------------------------

#[tokio::test]
async fn a_valid_jwt_registers_and_its_rights_come_from_the_service_table() {
    let hub = Hub::start(Opts::default()).await;
    let t = hub.jwt("orders", 300);
    let mut c = FakeClient::register_spec(hub.addr, &orders_spec(&t)).await;
    c.expect_ack().await;
    // `send` is the service table's: billing.Charge passes (nobody handles it), admin.Wipe does not.
    assert!(works(&mut c).await);
    c.tx.send(request("r2", "admin.Wipe")).await.unwrap();
    assert_eq!(c.expect_error().await.code(), ErrorCode::PermissionDenied);
}

#[tokio::test]
async fn a_handled_type_outside_the_service_table_closes_the_stream() {
    let hub = Hub::start(Opts::default()).await;
    let t = hub.jwt("orders", 300);
    let s = Spec {
        commands: vec!["orders.Place", "admin.Wipe"],
        ..spec("orders", &t)
    };
    let mut c = FakeClient::register_spec(hub.addr, &s).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        status.message().contains("admin.Wipe"),
        "{}",
        status.message()
    );
}

#[tokio::test]
async fn a_token_for_another_service_or_without_a_table_is_refused() {
    let hub = Hub::start(Opts::default()).await;
    // azp = web, Register as orders.
    let t = hub.jwt("web", 300);
    let mut c = FakeClient::register_spec(hub.addr, &spec("orders", &t)).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "token azp web is not service orders");
    // A user's access token: azp is the frontend client, which has no [[service]] table.
    let t = hub.jwt("kraken-frontend", 300);
    let mut c = FakeClient::register_spec(hub.addr, &spec("kraken-frontend", &t)).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "no ACL entry for service kraken-frontend");
    // A [[token]] entry for the name does not help: token entries never apply to JWTs.
    let t = hub.jwt("tok-web", 300);
    let mut c = FakeClient::register_spec(hub.addr, &spec("tok-web", &t)).await;
    assert_eq!(c.expect_closed().await.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn an_invalid_jwt_is_refused_with_unauthenticated_and_a_reason() {
    let hub = Hub::start(Opts {
        audience: Some(AUD),
        ..Opts::default()
    })
    .await;
    let good_aud = |mut c: serde_json::Value| {
        c["aud"] = json!(["account", AUD]);
        c
    };
    let base = || good_aud(claims(ISS, "orders", 300));
    let mut expired = good_aud(claims(ISS, "orders", -120));
    expired["iat"] = json!(now() - 600);
    let mut nbf = base();
    nbf["nbf"] = json!(now() + 3600);
    let mut other_iss = base();
    other_iss["iss"] = json!("https://evil.example/realms/kraken");
    let mut wrong_aud = base();
    wrong_aud["aud"] = json!("someone-else");
    let mut id_token = base();
    id_token["typ"] = json!("ID");
    let no_aud = claims(ISS, "orders", 300);
    let cases: Vec<(&str, String, &str)> = vec![
        ("expired", hub.key.sign(&expired), "token expired"),
        (
            "nbf in the future",
            hub.key.sign(&nbf),
            "token not valid yet",
        ),
        (
            "wrong issuer",
            hub.key.sign(&other_iss),
            "wrong token issuer",
        ),
        (
            "wrong audience",
            hub.key.sign(&wrong_aud),
            "wrong token audience",
        ),
        ("no audience", hub.key.sign(&no_aud), "wrong token audience"),
        (
            "an ID token",
            hub.key.sign(&id_token),
            "not an access token",
        ),
        (
            "alg none",
            forge_none(&base()),
            "unsupported token algorithm",
        ),
        (
            "HS256",
            forge_hs256("k1", &base(), b"anything"),
            "unsupported token algorithm",
        ),
        (
            "another key's signature",
            Signer::es256("k1").sign(&base()),
            "invalid token signature",
        ),
        ("garbage", "not.a.jwt".to_string(), "malformed token"),
    ];
    for (what, token, message) in cases {
        let mut c = FakeClient::register_spec(hub.addr, &spec("orders", &token)).await;
        let status = c.expect_closed().await;
        assert_eq!(status.code(), Code::Unauthenticated, "{what}");
        assert_eq!(status.message(), message, "{what}");
        assert!(!status.message().contains(&token), "{what}");
    }
    // And the same token with the audience it needs registers.
    let t = hub.key.sign(&base());
    registered(&hub, &spec("orders", &t)).await;
}

#[tokio::test]
async fn jwt_credentials_are_refused_when_no_issuer_is_configured() {
    let dir = TempDir::new();
    let file = dir.write("acl.toml", &acl());
    let addr = common::start_hub(auth_config(AuthMode::Enforce, &file)).await;
    let mut c = FakeClient::register_spec(addr, &spec("orders", "a.b.c")).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(
        status.message(),
        "jwt credentials are not configured on this hub"
    );
}

#[tokio::test]
async fn a_rotated_signing_key_is_picked_up_on_the_first_unknown_kid() {
    let hub = Hub::start(Opts::default()).await;
    let rotated = Signer::es256("k2");
    hub.jwks.serve(&[&rotated]);
    let t = rotated.sign(&claims(ISS, "orders", 300));
    registered(&hub, &orders_spec(&t)).await;
    // The retired key: the fetch above was made for another kid and is inside the throttle
    // window, so the hub cannot say yet that the key is gone. A client retries at its normal
    // backoff on UNAVAILABLE; it must not read a registration rejection.
    let old = hub.jwt("orders", 300);
    let mut c = FakeClient::register_spec(hub.addr, &spec("orders", &old)).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(status.message(), "signing key not known yet");
    // Once a fetch has looked for the key and not found it, it is rejected for good.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let mut c = FakeClient::register_spec(hub.addr, &spec("orders", &old)).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(status.message(), "unknown signing key");
}

#[tokio::test]
async fn an_unknown_kid_is_unavailable_while_the_jwks_cannot_be_refetched() {
    let hub = Hub::start(Opts::default()).await;
    hub.jwks.set_status(500);
    let rotated = Signer::es256("k2");
    let t = rotated.sign(&claims(ISS, "orders", 300));
    let mut c = FakeClient::register_spec(hub.addr, &spec("orders", &t)).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(status.message(), "signing key not known yet");
    // The route recovers with the rotated key: the retry registers.
    hub.jwks.serve(&[&rotated]);
    tokio::time::sleep(Duration::from_millis(400)).await;
    registered(&hub, &orders_spec(&t)).await;
}

#[tokio::test]
async fn a_jwks_that_is_down_at_startup_refuses_with_unavailable_until_it_is_up() {
    let key = Signer::es256("k1");
    let addr = JwksServer::reserve().await;
    let hub = Hub::start_with(Opts::default(), key, JwksServer::unbound(addr), false).await;
    let t = hub.jwt("orders", 300);
    let mut c = FakeClient::register_spec(hub.addr, &orders_spec(&t)).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(status.message(), "jwks not loaded yet");
    // The identity provider comes up; the hub's retry loads the keys without a restart.
    let _server = JwksServer::start_at(addr, &[&hub.key]).await;
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let mut c = FakeClient::register_spec(hub.addr, &orders_spec(&t)).await;
        match c.next().await {
            Some(Ok(_)) => break,
            Some(Err(s)) if s.code() == Code::Unavailable => {}
            other => panic!("{other:?}"),
        }
        assert!(Instant::now() < deadline, "the keys never loaded");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ---- expiry -------------------------------------------------------------------------------

/// Sleeps until `at` (wall clock); returns at once when it has passed.
async fn sleep_until_wall(at: SystemTime) {
    if let Ok(left) = at.duration_since(SystemTime::now()) {
        tokio::time::sleep(left).await;
    }
}

#[tokio::test]
async fn enforce_closes_the_session_at_exp_plus_the_grace_and_not_before() {
    let grace = Duration::from_secs(3);
    let hub = Hub::start(Opts {
        grace,
        ..Opts::default()
    })
    .await;
    // Everything is measured against the token's own `exp` (whole seconds on the wall clock),
    // so a loaded machine can only slow the test down, never change what it asserts: the
    // checks sit 1 s or more from each edge and the close is compared with an absolute time.
    let c = claims(ISS, "orders", 4);
    let exp = UNIX_EPOCH + Duration::from_secs(c["exp"].as_u64().unwrap());
    let deadline = exp + grace;
    let mut client = registered(&hub, &orders_spec(&hub.key.sign(&c))).await;
    assert!(works(&mut client).await, "before exp");
    sleep_until_wall(exp + Duration::from_secs(1)).await;
    assert!(
        works(&mut client).await,
        "between exp and exp + grace requests are still allowed"
    );
    let status = closed_within(&mut client, Duration::from_secs(10)).await;
    assert_eq!(status.code(), Code::Unauthenticated);
    assert_eq!(status.message(), "credentials expired");
    // Not at exp, and not before exp + grace (a little less for the clock conversion).
    let closed_at = SystemTime::now();
    assert!(
        closed_at + Duration::from_millis(100) >= deadline,
        "closed {:?} before exp + grace",
        deadline.duration_since(closed_at).unwrap_or_default()
    );
}

#[tokio::test]
async fn permissive_keeps_an_expired_session() {
    let hub = Hub::start(Opts {
        mode: AuthMode::Permissive,
        grace: Duration::ZERO,
        ..Opts::default()
    })
    .await;
    let t = hub.jwt("orders", 1);
    let mut c = registered(&hub, &orders_spec(&t)).await;
    assert_eq!(
        auth_kind(&hub, "orders").await,
        "jwt",
        "the session is timed"
    );
    // exp is at most 1 s away; 3.5 s is well past it.
    tokio::time::sleep(Duration::from_millis(3500)).await;
    assert!(works(&mut c).await, "still open after exp + grace");
    assert!(works(&mut c).await);
}

#[tokio::test]
async fn off_ignores_the_credentials_and_their_expiry() {
    let hub = Hub::start(Opts {
        mode: AuthMode::Off,
        grace: Duration::ZERO,
        ..Opts::default()
    })
    .await;
    let t = hub.jwt("orders", -3600);
    let mut c = FakeClient::register_spec(hub.addr, &orders_spec(&t)).await;
    c.expect_ack().await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Off checks nothing: the refresh is ignored and the session lives on.
    c.tx.send(refresh_jwt("garbage")).await.unwrap();
    assert!(c.silent_for(Duration::from_millis(400)).await);
    c.tx.send(request("r1", "billing.Charge")).await.unwrap();
    assert_eq!(c.expect_error().await.code(), ErrorCode::NoHandler);
}

// ---- refresh ------------------------------------------------------------------------------

#[tokio::test]
async fn a_refresh_moves_the_expiry() {
    let hub = Hub::start(Opts {
        grace: Duration::from_secs(2),
        ..Opts::default()
    })
    .await;
    // exp is 2 to 3 s away, so the first token's deadline is 4 to 5 s away.
    let t = hub.jwt("orders", 3);
    let mut c = registered(&hub, &orders_spec(&t)).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    c.tx.send(refresh_jwt(&hub.jwt("orders", 600)))
        .await
        .unwrap();
    // 1.5 s or more past the first token's deadline.
    tokio::time::sleep(Duration::from_millis(6500)).await;
    assert!(works(&mut c).await, "the refresh moved the deadline");
}

#[tokio::test]
async fn a_refresh_naming_a_new_kid_never_stalls_the_stream_while_the_jwks_is_slow() {
    // The realm rotates its key and the JWKS route is slow (8 s; the hub's own fetch timeout is
    // 10 s). Verifying the refresh must not hold the stream's loop: heartbeats keep being
    // answered past the 4 s heartbeat timeout, and the session stays.
    let hub = Hub::start(Opts {
        heartbeat: Some((Duration::from_secs(1), Duration::from_secs(4))),
        ..Opts::default()
    })
    .await;
    let mut c = registered(&hub, &orders_spec(&hub.jwt("orders", 300))).await;
    let rotated = Signer::es256("k2");
    hub.jwks.serve(&[&rotated]);
    hub.jwks.set_delay(Duration::from_secs(8));
    let t = rotated.sign(&claims(ISS, "orders", 300));
    c.tx.send(refresh_jwt(&t)).await.unwrap();
    let until = Instant::now() + Duration::from_millis(6500);
    let mut sent = 0i64;
    while Instant::now() < until {
        sent += 1;
        c.heartbeat(sent).await;
        loop {
            // The refresh may hold the loop for the hub's bound (1.5 s), not more.
            match c.next_within(Duration::from_secs(3)).await {
                Some(Ok(m)) => {
                    if let Some(snodra_server::proto::server_message::Message::Heartbeat(h)) =
                        m.message
                        && h.sent_at_ms == sent
                    {
                        break;
                    }
                }
                other => panic!("heartbeat {sent} was not echoed: {other:?}"),
            }
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    // A key that is merely not known yet does not end the session: the old credentials stay.
    assert!(works(&mut c).await);
}

#[tokio::test]
async fn a_refresh_for_another_service_closes_the_stream_in_enforce() {
    let hub = Hub::start(Opts::default()).await;
    let mut c = registered(&hub, &orders_spec(&hub.jwt("orders", 300))).await;
    // A valid token of `web`, which has its own table: still not this session's service.
    c.tx.send(refresh_jwt(&hub.jwt("web", 300))).await.unwrap();
    let status = closed_within(&mut c, Duration::from_secs(3)).await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(status.message().contains("web"), "{}", status.message());
}

#[tokio::test]
async fn an_invalid_refresh_closes_the_stream_in_enforce() {
    let hub = Hub::start(Opts::default()).await;
    for bad in [hub.jwt("orders", -300), "nope".to_string()] {
        let mut c = registered(&hub, &orders_spec(&hub.jwt("orders", 300))).await;
        c.tx.send(refresh_jwt(&bad)).await.unwrap();
        let status = closed_within(&mut c, Duration::from_secs(3)).await;
        assert_eq!(status.code(), Code::Unauthenticated, "{bad}");
        assert!(!status.message().contains(&bad));
    }
    let mut c = registered(&hub, &orders_spec(&hub.jwt("orders", 300))).await;
    c.tx.send(refresh_token("who-is-this")).await.unwrap();
    assert_eq!(
        closed_within(&mut c, Duration::from_secs(3)).await.code(),
        Code::Unauthenticated
    );
}

#[tokio::test]
async fn permissive_keeps_the_session_and_its_old_credentials_on_an_invalid_refresh() {
    let hub = Hub::start(Opts {
        mode: AuthMode::Permissive,
        ..Opts::default()
    })
    .await;
    let mut c = registered(&hub, &orders_spec(&hub.jwt("orders", 300))).await;
    c.tx.send(refresh_jwt("nope")).await.unwrap();
    c.tx.send(refresh_jwt(&hub.jwt("web", 300))).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(works(&mut c).await);
}

#[tokio::test]
async fn refreshes_less_than_a_second_apart_are_ignored() {
    let hub = Hub::start(Opts::default()).await;
    let mut c = registered(&hub, &orders_spec(&hub.jwt("orders", 300))).await;
    c.tx.send(refresh_jwt(&hub.jwt("orders", 600)))
        .await
        .unwrap();
    // Within the second: an invalid one is not even looked at (it would close the stream).
    c.tx.send(refresh_jwt("nope")).await.unwrap();
    assert!(c.silent_for(Duration::from_millis(400)).await);
    assert!(works(&mut c).await, "the throttled refresh was ignored");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    c.tx.send(refresh_jwt("nope")).await.unwrap();
    assert_eq!(
        closed_within(&mut c, Duration::from_secs(3)).await.code(),
        Code::Unauthenticated
    );
}

#[tokio::test]
async fn a_token_session_may_move_to_a_jwt_and_back() {
    let hub = Hub::start(Opts {
        grace: Duration::from_secs(1),
        ..Opts::default()
    })
    .await;
    let token_spec = Spec {
        service: "orders",
        commands: vec!["orders.Place"],
        token: Some("tok-orders"),
        ..Spec::default()
    };
    // token -> jwt: from then on the JWT's expiry applies.
    let mut c = registered(&hub, &token_spec).await;
    assert!(works(&mut c).await);
    c.tx.send(refresh_jwt(&hub.jwt("orders", 1))).await.unwrap();
    let status = closed_within(&mut c, Duration::from_secs(10)).await;
    assert_eq!(status.message(), "credentials expired");
    // jwt -> token: the session no longer expires.
    let t = hub.jwt("orders", 1);
    let mut c = registered(&hub, &orders_spec(&t)).await;
    c.tx.send(refresh_token("tok-orders")).await.unwrap();
    // The JWT's deadline (exp 1 s, grace 1 s) is at most 2 s away; 4.5 s is far past it.
    tokio::time::sleep(Duration::from_millis(4500)).await;
    assert!(works(&mut c).await, "a token does not expire");
}

#[tokio::test]
async fn a_refresh_judges_the_handled_types_against_the_new_entry() {
    let hub = Hub::start(Opts::default()).await;
    let mut c = registered(&hub, &orders_spec(&hub.jwt("orders", 300))).await;
    // The new credential's entry grants orders.Get only; the session handles orders.Place.
    c.tx.send(refresh_token("tok-orders-narrow")).await.unwrap();
    let status = closed_within(&mut c, Duration::from_secs(3)).await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(
        status.message().contains("orders.Place"),
        "{}",
        status.message()
    );
}

// ---- reloads ------------------------------------------------------------------------------

#[tokio::test]
async fn removing_the_service_table_closes_the_jwt_session_in_enforce() {
    let hub = Hub::start(Opts::default()).await;
    let mut c = registered(&hub, &orders_spec(&hub.jwt("orders", 300))).await;
    assert!(works(&mut c).await);
    // `orders` loses its [[service]] table; the [[token]] entries stay.
    let without = [
        acl_entry("tok-orders", "orders", &["orders.*"], &["billing.Charge"]),
        service_entry("web", &[], &["orders.*"]),
        service_entry("probe", &[], &[]),
    ]
    .concat();
    std::fs::write(hub.acl_file(), without).unwrap();
    let status = closed_within(&mut c, Duration::from_secs(3)).await;
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), "no ACL entry for service orders");
}

#[tokio::test]
async fn permissive_keeps_a_jwt_session_whose_table_was_removed() {
    let hub = Hub::start(Opts {
        mode: AuthMode::Permissive,
        ..Opts::default()
    })
    .await;
    let mut c = registered(&hub, &orders_spec(&hub.jwt("orders", 300))).await;
    assert_eq!(auth_kind(&hub, "orders").await, "jwt");
    std::fs::write(hub.acl_file(), service_entry("probe", &[], &[])).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    c.tx.send(request("r1", "billing.Charge")).await.unwrap();
    // Allowed (and counted) by permissive: routed, so NO_HANDLER rather than PERMISSION_DENIED
    // or a closed stream.
    assert_eq!(c.expect_error().await.code(), ErrorCode::NoHandler);
}

// ---- topology -----------------------------------------------------------------------------

#[tokio::test]
async fn the_topology_shows_how_each_instance_authenticated() {
    let hub = Hub::start(Opts {
        mode: AuthMode::Permissive,
        ..Opts::default()
    })
    .await;
    let http = hub.http.unwrap();
    let t = hub.jwt("orders", 600);
    let exp_ms = (now() + 600) * 1000;
    let _jwt = registered(&hub, &orders_spec(&t)).await;
    let web = Spec {
        token: Some("tok-web"),
        ..spec("web", "")
    };
    let _token = registered(&hub, &web).await;
    let _none = registered(&hub, &spec("stranger", "")).await;
    let body = loop {
        let (_, body) = common::http_get(http, "/api/v1/topology").await;
        let t: serde_json::Value = serde_json::from_str(&body).unwrap();
        // Three sessions and the probe that registered first may still be going away.
        let n = t["services"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|s| s["serviceName"] != "probe")
            .count();
        if n == 3 {
            break t;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    let instance = |service: &str| {
        body["services"]
            .as_array()
            .unwrap()
            .iter()
            .find(|s| s["serviceName"] == service)
            .unwrap()["instances"][0]
            .clone()
    };
    let jwt = instance("orders");
    assert_eq!(jwt["authKind"], "jwt");
    assert_eq!(jwt["auth"], "authenticated");
    let got = jwt["expiresAt"].as_i64().unwrap();
    assert!((got - exp_ms).abs() <= 2000, "{got} vs {exp_ms}");
    let token = instance("web");
    assert_eq!(token["authKind"], "token");
    assert!(token.get("expiresAt").is_none());
    let none = instance("stranger");
    assert_eq!(none["authKind"], "none");
    assert!(none.get("expiresAt").is_none());
    // No token material anywhere.
    let text = body.to_string();
    for secret in [t.as_str(), "tok-web"] {
        assert!(!text.contains(secret), "{text}");
    }
}

#[tokio::test]
async fn the_topology_shows_nothing_about_authentication_when_it_is_off() {
    let hub = Hub::start(Opts {
        mode: AuthMode::Off,
        ..Opts::default()
    })
    .await;
    let _c = FakeClient::register_spec(hub.addr, &spec("orders", "x")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (_, body) = common::http_get(hub.http.unwrap(), "/api/v1/topology").await;
    assert!(
        !body.contains("authKind") && !body.contains("expiresAt"),
        "{body}"
    );
}

#[tokio::test]
async fn the_topology_rpc_takes_a_jwt_whose_service_has_a_table() {
    let hub = Hub::start(Opts::default()).await;
    let target = snodra_server::client::Target::plain(hub.addr);
    let ok = hub.jwt("web", 300);
    assert!(
        snodra_server::topology_probe_target(&target, Some(&ok))
            .await
            .is_ok()
    );
    for (what, bad) in [
        ("no table", hub.jwt("kraken-frontend", 300)),
        ("expired", hub.jwt("web", -300)),
        ("garbage", "a.b.c".to_string()),
    ] {
        let err = snodra_server::topology_probe_target(&target, Some(&bad))
            .await
            .unwrap_err();
        let status = err.downcast_ref::<Status>().expect("a gRPC status");
        assert_eq!(status.code(), Code::Unauthenticated, "{what}");
    }
}

// ---- logs ---------------------------------------------------------------------------------

static LOGS: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct Capture;

struct Line(String);

impl tracing::field::Visit for Line {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.0.push_str(&format!(" {}={value:?}", field.name()));
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut line = Line(event.metadata().target().to_string());
        event.record(&mut line);
        LOGS.lock().unwrap().push(line.0);
    }
}

#[tokio::test]
async fn no_token_reaches_a_log_line_in_any_flow() {
    use tracing_subscriber::layer::SubscriberExt;
    let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry().with(Capture));
    let hub = Hub::start(Opts::default()).await;
    let good = hub.jwt("orders", 300);
    let refreshed = hub.jwt("orders", 900);
    let expired = hub.jwt("orders", -300);
    let impostor = Signer::es256("k1").sign(&claims(ISS, "orders", 300));
    let wrong_service = hub.jwt("web", 300);
    let mut c = registered(&hub, &orders_spec(&good)).await;
    for t in [&expired, &impostor, &wrong_service] {
        let mut bad = FakeClient::register_spec(hub.addr, &spec("orders", t)).await;
        bad.expect_closed().await;
    }
    c.tx.send(refresh_jwt(&refreshed)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1100)).await;
    c.tx.send(refresh_jwt(&impostor)).await.unwrap();
    closed_within(&mut c, Duration::from_secs(3)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    let logs = LOGS.lock().unwrap().join("\n");
    assert!(logs.contains("jwks loaded"), "the capture works: {logs}");
    for token in [&good, &refreshed, &expired, &impostor, &wrong_service] {
        assert!(!logs.contains(token.as_str()), "a token is in the logs");
        for part in token.split('.') {
            assert!(
                !logs.contains(part),
                "a part of a token is in the logs: {part}"
            );
        }
    }
}
