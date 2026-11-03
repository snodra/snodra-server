//! Mutual TLS on the gRPC listener (roadmap P5c, ADR 0020): the handshake with a client CA, the
//! client certificate as the session's identity, its expiry, reloads and the topology. CAs and
//! certificates are generated at test time; nothing is checked in.

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use common::{ClientSpec, FakeClient, Pki, TempDir, auth_config};
use kraken_hub::auth::AuthMode;
use kraken_hub::client::{ClientIdentity, Target, Trust};
use kraken_hub::config::Config;
use kraken_hub::tls::ClientAuthMode;

/// A hub with TLS, a client CA and (optionally) an ACL, plus what a client needs to talk to it.
struct Hub {
    addr: SocketAddr,
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
            tls_cert: Some(cert),
            tls_key: Some(key),
            tls_client_ca: Some(client_ca),
            tls_client_auth: Some(opts.client_auth),
            ..auth_config(opts.auth, &acl)
        };
        let addr = common::start_hub(config).await;
        Self {
            addr,
            dir,
            server_ca,
            clients,
        }
    }

    /// Writes a client certificate and key into the temp dir; returns their paths.
    fn identity(&self, name: &str, spec: &ClientSpec) -> (PathBuf, PathBuf) {
        write_identity(&self.dir, name, &self.clients, spec)
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
