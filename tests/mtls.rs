//! Mutual TLS on the gRPC listener (roadmap P5c, ADR 0020): the handshake with a client CA, the
//! client certificate as the session's identity, its expiry, reloads and the topology. CAs and
//! certificates are generated at test time; nothing is checked in.

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::{ClientSpec, FakeClient, Pki, Spec, TempDir, acl_entry, auth_config};
use kraken_hub::auth::AuthMode;
use kraken_hub::client::{ClientIdentity, Target, Trust};
use kraken_hub::config::Config;
use kraken_hub::tls::ClientAuthMode;

/// A hub with TLS, a client CA and (optionally) an ACL, plus what a client needs to talk to it.
struct Hub {
    addr: SocketAddr,
    http: SocketAddr,
    dir: TempDir,
    /// The CA that signed the hub's certificate: what clients trust.
    server_ca: PathBuf,
    /// The CA the hub accepts client certificates from.
    clients: Pki,
}

#[derive(Clone)]
struct Opts {
    client_auth: ClientAuthMode,
    auth: AuthMode,
    acl: String,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            client_auth: ClientAuthMode::Required,
            auth: AuthMode::Off,
            acl: String::new(),
        }
    }
}

impl Hub {
    async fn start(opts: Opts) -> Self {
        let dir = TempDir::new();
        let server = Pki::new("hub CA");
        let (server_ca, cert, key) = server.write(&dir, "hub");
        let clients = Pki::new("client CA");
        let client_ca = dir.write("client-ca.pem", &clients.ca_pem);
        let acl = dir.write("acl.toml", &opts.acl);
        let config = Config {
            http_listen: Some("127.0.0.1:0".parse().unwrap()),
            tls_cert: Some(cert),
            tls_key: Some(key),
            tls_client_ca: Some(client_ca),
            tls_client_auth: Some(opts.client_auth),
            ..auth_config(opts.auth, &acl)
        };
        let (addr, http) = common::start_hub_with_http(config).await;
        Self {
            addr,
            http: http.unwrap(),
            dir,
            server_ca,
            clients,
        }
    }

    /// Writes a client certificate and key into the temp dir; returns their paths.
    fn identity(&self, name: &str, spec: &ClientSpec) -> (PathBuf, PathBuf) {
        write_identity(&self.dir, name, &self.clients, spec)
    }

    fn acl_file(&self) -> PathBuf {
        self.dir.path("acl.toml")
    }

    /// The hub's topology JSON, over its TLS HTTP port.
    async fn topology(&self) -> serde_json::Value {
        let ca = std::fs::read_to_string(&self.server_ca).unwrap();
        let r = common::https_request(self.http, &ca, "GET", "/topology", &[])
            .await
            .unwrap();
        serde_json::from_str(&r.body).unwrap()
    }

    /// The first instance of `service` in the topology, once it shows.
    async fn instance(&self, service: &str) -> serde_json::Value {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let t = self.topology().await;
            if let Some(i) = t["services"]
                .as_array()
                .and_then(|s| s.iter().find(|s| s["serviceName"] == service))
                .map(|s| s["instances"][0].clone())
            {
                return i;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "{service} never showed"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// True when `service` is gone from the topology (within a few seconds).
    async fn gone(&self, service: &str) -> bool {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let t = self.topology().await;
            if !t["services"]
                .as_array()
                .is_some_and(|s| s.iter().any(|s| s["serviceName"] == service))
            {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Connects with `id` and registers `spec` (no credentials unless the spec has them).
    async fn register(&self, id: &(PathBuf, PathBuf), spec: &Spec<'_>) -> FakeClient {
        let client = self.connect(Some(id)).await.unwrap();
        client
            .send_raw_register(spec.register(&client.instance_id))
            .await;
        client
    }

    async fn connect(&self, identity: Option<&(PathBuf, PathBuf)>) -> Result<FakeClient, String> {
        FakeClient::connect_mtls(
            self.addr,
            &self.server_ca,
            identity.map(|(c, k)| (c.as_path(), k.as_path())),
        )
        .await
    }
}

fn write_identity(dir: &TempDir, name: &str, pki: &Pki, spec: &ClientSpec) -> (PathBuf, PathBuf) {
    let (cert, key) = pki.client(spec);
    (
        dir.write(&format!("{name}-cert.pem"), &cert),
        dir.write(&format!("{name}-key.pem"), &key),
    )
}

// ---- the handshake ---------------------------------------------------------------------------

#[tokio::test]
async fn a_required_hub_refuses_a_client_without_a_certificate_and_serves_one_with_it() {
    let hub = Hub::start(Opts::default()).await;
    assert!(hub.connect(None).await.is_err());
    let id = hub.identity("orders", &ClientSpec::service("orders"));
    let mut client = hub.connect(Some(&id)).await.unwrap();
    client.send_register_v2("orders", &[], &[]).await;
    client.expect_ack().await;
}

#[tokio::test]
async fn an_optional_hub_accepts_a_client_without_a_certificate() {
    let hub = Hub::start(Opts {
        client_auth: ClientAuthMode::Optional,
        ..Opts::default()
    })
    .await;
    let mut client = hub.connect(None).await.unwrap();
    client.send_register_v2("orders", &[], &[]).await;
    client.expect_ack().await;
}

#[tokio::test]
async fn a_certificate_from_another_ca_is_refused_in_both_modes() {
    for client_auth in [ClientAuthMode::Optional, ClientAuthMode::Required] {
        let hub = Hub::start(Opts {
            client_auth,
            ..Opts::default()
        })
        .await;
        let other = Pki::new("another CA");
        let id = write_identity(&hub.dir, "x", &other, &ClientSpec::service("orders"));
        assert!(hub.connect(Some(&id)).await.is_err(), "{client_auth:?}");
    }
}

#[tokio::test]
async fn the_client_ca_file_reloads_for_new_connections() {
    let hub = Hub::start(Opts::default()).await;
    let old = hub.identity("old", &ClientSpec::service("orders"));
    hub.connect(Some(&old)).await.unwrap();

    let next = Pki::new("next client CA");
    let fresh = write_identity(&hub.dir, "next", &next, &ClientSpec::service("orders"));
    assert!(hub.connect(Some(&fresh)).await.is_err(), "not trusted yet");
    std::fs::write(hub.dir.path("client-ca.pem"), &next.ca_pem).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if hub.connect(Some(&fresh)).await.is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the new CA never applied"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(hub.connect(Some(&old)).await.is_err(), "the old CA is gone");
}

// ---- the binary's probes ----------------------------------------------------------------------

#[tokio::test]
async fn the_health_probe_needs_a_client_certificate_on_a_required_hub() {
    let hub = Hub::start(Opts::default()).await;
    let plain = Target {
        address: hub.addr.to_string(),
        tls: Some(Trust::Ca(hub.server_ca.clone())),
        identity: None,
    };
    assert!(kraken_hub::health_probe_target(&plain).await.is_err());
    let id = hub.identity("probe", &ClientSpec::service("probe"));
    let with = Target {
        identity: Some(ClientIdentity::new(id.0.clone(), id.1.clone())),
        ..plain
    };
    assert!(kraken_hub::health_probe_target(&with).await.unwrap());
}

#[tokio::test]
async fn an_unusable_client_certificate_is_an_error_not_a_silent_downgrade() {
    let hub = Hub::start(Opts::default()).await;
    let target = Target {
        address: hub.addr.to_string(),
        tls: Some(Trust::Ca(hub.server_ca.clone())),
        identity: Some(ClientIdentity::new("/nope.pem".into(), "/nope.key".into())),
    };
    let e = kraken_hub::health_probe_target(&target).await.unwrap_err();
    assert!(format!("{e:#}").contains("/nope.pem"), "{e:#}");
}

/// The container's own probe (`--health-check` with the hub's environment) on a hub that requires
/// client certificates: with no flags it presents the hub's own certificate, which here is issued
/// by the client CA; with the flags it presents those.
#[tokio::test]
async fn the_binarys_own_health_check_works_against_a_required_hub() {
    let dir = TempDir::new();
    // One CA issues the hub's certificate and the clients', as a platform CA would.
    let pki = Pki::new("platform CA");
    let (ca, cert, key) = pki.write(&dir, "hub");
    let client_ca = dir.write("client-ca.pem", &pki.ca_pem);
    let acl = dir.write("acl.toml", "");
    let addr = common::start_hub(Config {
        tls_cert: Some(cert.clone()),
        tls_key: Some(key.clone()),
        tls_client_ca: Some(client_ca.clone()),
        tls_client_auth: Some(ClientAuthMode::Required),
        ..auth_config(AuthMode::Off, &acl)
    })
    .await;
    let bin = env!("CARGO_BIN_EXE_kraken-hub");
    let hub_env = |cmd: &mut tokio::process::Command| {
        cmd.env_clear()
            .env("KRAKEN_HUB_LISTEN", format!("127.0.0.1:{}", addr.port()))
            .env("KRAKEN_HUB_TLS_CERT", &cert)
            .env("KRAKEN_HUB_TLS_KEY", &key)
            .env("KRAKEN_HUB_TLS_CLIENT_CA", &client_ca)
            .env("KRAKEN_HUB_TLS_CLIENT_AUTH", "required");
    };
    let mut cmd = tokio::process::Command::new(bin);
    cmd.arg("--health-check");
    hub_env(&mut cmd);
    let out = cmd.output().await.unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("SERVING"));

    // --address probes another hub: no own certificate is presented, the flags are needed.
    let address = format!("https://localhost:{}", addr.port());
    let probe = |extra: &[&std::path::Path]| {
        let mut cmd = tokio::process::Command::new(bin);
        cmd.args(["--health-check", "--address", &address, "--tls-ca"])
            .arg(&ca);
        for (flag, path) in ["--tls-client-cert", "--tls-client-key"].iter().zip(extra) {
            cmd.arg(flag).arg(path);
        }
        cmd.env_clear();
        cmd
    };
    assert!(!probe(&[]).output().await.unwrap().status.success());
    let out = probe(&[&cert, &key]).output().await.unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
}

// ---- the certificate as the session's identity -------------------------------------------------

/// True when the hub answers a heartbeat on `c`: the session is open.
async fn echoed(c: &mut FakeClient) -> bool {
    c.heartbeat(7).await;
    matches!(
        c.next_within(Duration::from_secs(2)).await,
        Some(Ok(kraken_hub::proto::ServerMessage {
            message: Some(kraken_hub::proto::server_message::Message::Heartbeat(_))
        }))
    )
}

/// `orders` handles `orders.*` and sends `billing.Charge`; `billing` handles `billing.*`;
/// `orders` also has a token entry that may do anything, to show which table the certificate uses.
fn acl() -> String {
    format!(
        "{}[[service]]\nname = \"orders\"\nhandle = [\"orders.*\"]\nsend = [\"billing.Charge\"]\n\n\
         [[service]]\nname = \"billing\"\nhandle = [\"billing.*\"]\n",
        acl_entry("tok-orders", "orders", &["*"], &["*"])
    )
}

fn enforce() -> Opts {
    Opts {
        auth: AuthMode::Enforce,
        acl: acl(),
        ..Opts::default()
    }
}

fn orders<'a>() -> Spec<'a> {
    Spec {
        service: "orders",
        commands: vec!["orders.Place"],
        ..Spec::default()
    }
}

#[tokio::test]
async fn enforce_admits_a_certificate_with_no_credentials_and_the_rights_are_the_service_tables() {
    let hub = Hub::start(enforce()).await;
    let id = hub.identity("orders", &ClientSpec::service("orders"));
    let mut c = hub.register(&id, &orders()).await;
    c.expect_ack().await;

    // A type outside the table's `handle` is refused, though the token entry would allow it.
    let id = hub.identity("orders2", &ClientSpec::service("orders"));
    let mut wide = hub
        .register(
            &id,
            &Spec {
                commands: vec!["billing.Charge"],
                ..orders()
            },
        )
        .await;
    let status = wide.expect_closed().await;
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert!(
        status.message().contains("billing.Charge"),
        "{}",
        status.message()
    );
}

#[tokio::test]
async fn a_certificate_that_names_no_service_or_several_is_permission_denied_at_register() {
    let hub = Hub::start(enforce()).await;
    for (spec, text) in [
        (ClientSpec::default(), "certificate names no service"),
        (
            ClientSpec {
                dns: vec!["orders".into()],
                ..ClientSpec::default()
            },
            "certificate names no service",
        ),
        (
            ClientSpec {
                uris: vec!["spiffe://x/orders".into()],
                ..ClientSpec::default()
            },
            "certificate names no service",
        ),
        (
            ClientSpec {
                uris: vec![
                    "kraken://service/orders".into(),
                    "kraken://service/billing".into(),
                ],
                ..ClientSpec::default()
            },
            "certificate names more than one service",
        ),
    ] {
        let id = hub.identity("bad", &spec);
        let mut c = hub.register(&id, &orders()).await;
        let status = c.expect_closed().await;
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), text);
    }
}

#[tokio::test]
async fn a_certificate_cannot_register_as_another_service() {
    let hub = Hub::start(enforce()).await;
    let id = hub.identity("billing", &ClientSpec::service("billing"));
    let mut c = hub.register(&id, &orders()).await;
    let status = c.expect_closed().await;
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn credentials_next_to_a_certificate_must_name_the_same_service() {
    let hub = Hub::start(enforce()).await;
    let id = hub.identity("orders", &ClientSpec::service("orders"));
    // The same service by token: accepted, and the session is the certificate's.
    let mut ok = hub
        .register(
            &id,
            &Spec {
                token: Some("tok-orders"),
                ..orders()
            },
        )
        .await;
    ok.expect_ack().await;
    assert_eq!(hub.instance("orders").await["authKind"], "certificate");

    // A token for another service: refused.
    let hub = Hub::start(Opts {
        acl: format!(
            "{}{}",
            acl(),
            acl_entry("tok-billing", "billing", &["billing.*"], &[])
        ),
        ..enforce()
    })
    .await;
    let id = hub.identity("orders", &ClientSpec::service("orders"));
    let mut bad = hub
        .register(
            &id,
            &Spec {
                token: Some("tok-billing"),
                ..orders()
            },
        )
        .await;
    let status = bad.expect_closed().await;
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert_eq!(
        status.message(),
        "credentials and certificate name different services"
    );
}

#[tokio::test]
async fn permissive_lets_a_mismatch_through_and_the_certificate_wins() {
    let hub = Hub::start(Opts {
        auth: AuthMode::Permissive,
        acl: format!(
            "{}{}",
            acl(),
            acl_entry("tok-billing", "billing", &["billing.*"], &[])
        ),
        ..Opts::default()
    })
    .await;
    let id = hub.identity("orders", &ClientSpec::service("orders"));
    let mut c = hub
        .register(
            &id,
            &Spec {
                token: Some("tok-billing"),
                ..orders()
            },
        )
        .await;
    c.expect_ack().await;
    let i = hub.instance("orders").await;
    assert_eq!(i["authKind"], "certificate");
    assert_eq!(i["authenticatedService"], "orders");
}

#[tokio::test]
async fn the_token_path_is_unchanged_on_an_optional_hub() {
    let hub = Hub::start(Opts {
        client_auth: ClientAuthMode::Optional,
        ..enforce()
    })
    .await;
    let mut c = hub.connect(None).await.unwrap();
    c.send_raw_register(
        Spec {
            token: Some("tok-orders"),
            ..orders()
        }
        .register(&c.instance_id),
    )
    .await;
    c.expect_ack().await;
    assert_eq!(hub.instance("orders").await["authKind"], "token");
    // And no credentials at all is still refused.
    let mut anon = hub.connect(None).await.unwrap();
    anon.send_raw_register(orders().register(&anon.instance_id))
        .await;
    assert_eq!(
        anon.expect_closed().await.code(),
        tonic::Code::Unauthenticated
    );
}

#[tokio::test]
async fn mode_off_checks_the_handshake_and_nothing_else() {
    let hub = Hub::start(Opts::default()).await;
    // A certificate that names no service registers fine: no principal, no ACL.
    let id = hub.identity("anon", &ClientSpec::default());
    let mut c = hub.register(&id, &orders()).await;
    c.expect_ack().await;
    let i = hub.instance("orders").await;
    assert!(i.get("authKind").is_none(), "{i}");
}

#[tokio::test]
async fn the_topology_shows_the_kind_the_expiry_and_a_fingerprint_prefix_of_a_certificate() {
    let hub = Hub::start(enforce()).await;
    let id = hub.identity(
        "orders",
        &ClientSpec::service("orders").valid_for(Duration::from_secs(3600)),
    );
    let mut c = hub.register(&id, &orders()).await;
    c.expect_ack().await;
    let i = hub.instance("orders").await;
    assert_eq!(i["authKind"], "certificate");
    assert_eq!(i["auth"], "authenticated");
    let end = i["expiresAt"].as_i64().unwrap();
    let want = (SystemTime::now() + Duration::from_secs(3600))
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!((end - want).abs() < 5000, "{end} vs {want}");
    let fp = i["certFingerprint"].as_str().unwrap();
    assert!(
        fp.len() == 8 && fp.chars().all(|c| c.is_ascii_hexdigit()),
        "{fp}"
    );
    // Never the certificate.
    let pem = std::fs::read_to_string(&id.0).unwrap();
    let body = hub.topology().await.to_string();
    let b64: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    assert!(
        !body.contains(&b64[..40]) && !body.contains("BEGIN"),
        "{body}"
    );
}

#[tokio::test]
async fn a_certificate_session_is_closed_at_not_after_in_enforce() {
    let hub = Hub::start(enforce()).await;
    let id = hub.identity(
        "short",
        &ClientSpec::service("orders").valid_for(Duration::from_secs(3)),
    );
    let started = tokio::time::Instant::now();
    let mut c = hub.register(&id, &orders()).await;
    c.expect_ack().await;
    // Open until the certificate ends.
    let status = loop {
        match c.next_within(Duration::from_secs(8)).await {
            Some(Err(status)) => break status,
            Some(Ok(_)) => continue,
            None => panic!("the stream ended without a status"),
        }
    };
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert_eq!(status.message(), "certificate expired");
    let lived = started.elapsed();
    assert!(
        lived >= Duration::from_millis(1500) && lived < Duration::from_secs(5),
        "closed after {lived:?}"
    );
    assert!(hub.gone("orders").await);
}

#[tokio::test]
async fn permissive_keeps_a_certificate_session_past_not_after() {
    let hub = Hub::start(Opts {
        auth: AuthMode::Permissive,
        acl: acl(),
        ..Opts::default()
    })
    .await;
    let id = hub.identity(
        "short",
        &ClientSpec::service("orders").valid_for(Duration::from_secs(2)),
    );
    let mut c = hub.register(&id, &orders()).await;
    c.expect_ack().await;
    tokio::time::sleep(Duration::from_millis(3200)).await;
    assert!(echoed(&mut c).await, "the heartbeat is echoed");
    assert_eq!(hub.instance("orders").await["authKind"], "certificate");
}

#[tokio::test]
async fn mode_off_never_looks_at_the_expiry() {
    let hub = Hub::start(Opts::default()).await;
    let id = hub.identity(
        "short",
        &ClientSpec::service("orders").valid_for(Duration::from_secs(2)),
    );
    let mut c = hub.register(&id, &orders()).await;
    c.expect_ack().await;
    tokio::time::sleep(Duration::from_millis(3200)).await;
    assert!(echoed(&mut c).await);
}

#[tokio::test]
async fn an_acl_reload_closes_a_certificate_session_like_a_jwt_one() {
    let hub = Hub::start(enforce()).await;
    let id = hub.identity("orders", &ClientSpec::service("orders"));
    let mut c = hub.register(&id, &orders()).await;
    c.expect_ack().await;
    // The service table goes: the session no longer has an entry.
    std::fs::write(
        hub.acl_file(),
        "[[service]]\nname = \"billing\"\nhandle = [\"billing.*\"]\n",
    )
    .unwrap();
    let status = c.expect_closed().await;
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert!(
        status.message().contains("no ACL entry"),
        "{}",
        status.message()
    );
}

#[tokio::test]
async fn a_credentials_refresh_on_a_certificate_session_changes_nothing() {
    let hub = Hub::start(enforce()).await;
    let id = hub.identity("orders", &ClientSpec::service("orders"));
    let mut c = hub.register(&id, &orders()).await;
    c.expect_ack().await;
    for kind in [
        kraken_hub::proto::credentials::Kind::Token("tok-orders".into()),
        kraken_hub::proto::credentials::Kind::Token("junk".into()),
    ] {
        c.tx.send(kraken_hub::proto::ClientMessage {
            message: Some(kraken_hub::proto::client_message::Message::Credentials(
                kraken_hub::proto::Credentials { kind: Some(kind) },
            )),
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
    }
    assert!(echoed(&mut c).await, "still open");
    assert_eq!(hub.instance("orders").await["authKind"], "certificate");
}

#[tokio::test]
async fn the_topology_rpc_takes_a_certificate_whose_service_has_a_table() {
    let hub = Hub::start(enforce()).await;
    let probe = |identity: Option<&(PathBuf, PathBuf)>| Target {
        address: hub.addr.to_string(),
        tls: Some(Trust::Ca(hub.server_ca.clone())),
        identity: identity.map(|(c, k)| ClientIdentity::new(c.clone(), k.clone())),
    };
    let good = hub.identity("orders", &ClientSpec::service("orders"));
    assert!(
        kraken_hub::topology_probe_target(&probe(Some(&good)), None)
            .await
            .is_ok()
    );
    // A certificate whose service has no table, or names none: refused (UNAUTHENTICATED).
    for spec in [ClientSpec::service("nobody"), ClientSpec::default()] {
        let id = hub.identity("other", &spec);
        let e = kraken_hub::topology_probe_target(&probe(Some(&id)), None)
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("unauthenticated"), "{e:#}");
    }
}

#[tokio::test]
async fn the_topology_rpc_without_a_certificate_still_needs_a_token_on_an_optional_hub() {
    let hub = Hub::start(Opts {
        client_auth: ClientAuthMode::Optional,
        ..enforce()
    })
    .await;
    let target = Target {
        address: hub.addr.to_string(),
        tls: Some(Trust::Ca(hub.server_ca.clone())),
        identity: None,
    };
    assert!(
        kraken_hub::topology_probe_target(&target, None)
            .await
            .is_err()
    );
    assert!(
        kraken_hub::topology_probe_target(&target, Some("tok-orders"))
            .await
            .is_ok()
    );
}
