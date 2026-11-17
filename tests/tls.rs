//! TLS on both listeners (roadmap P5a, ADR 0018): certificates are generated at test time with
//! rcgen; nothing is checked in.

mod common;

use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use common::{FakeClient, Pki, TempDir, https_request, start_hub_with_http};
use snodra_server::client::{Target, Trust};
use snodra_server::config::Config;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn config(cert: &Path, key: &Path) -> Config {
    Config {
        otlp_endpoint: None,
        tls_cert: Some(cert.to_path_buf()),
        tls_key: Some(key.to_path_buf()),
        reload_interval: Duration::from_millis(50),
        ..Config::default()
    }
}

fn ca_target(addr: SocketAddr, ca: &Path) -> Target {
    Target {
        address: addr.to_string(),
        tls: Some(Trust::Ca(ca.to_path_buf())),
        identity: None,
    }
}

async fn probe(addr: SocketAddr, ca: &Path) -> anyhow::Result<bool> {
    let target = ca_target(addr, ca);
    tokio::time::timeout(
        Duration::from_secs(3),
        snodra_server::health_probe_target(&target),
    )
    .await
    .map_err(|_| anyhow::anyhow!("timed out"))?
}

/// What a plain-text HTTP client gets from `addr`: whatever bytes the server answers with.
async fn plain_http(addr: SocketAddr) -> String {
    let fut = async {
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        let _ = s
            .write_all(b"GET /api/v1/topology HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await;
        let mut raw = Vec::new();
        let _ = s.read_to_end(&mut raw).await;
        String::from_utf8_lossy(&raw).into_owned()
    };
    tokio::time::timeout(Duration::from_secs(3), fut)
        .await
        .expect("the server neither answered nor closed")
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !check().await {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn grpc_serves_tls_only_and_verifies_against_the_ca() {
    let dir = TempDir::new();
    let pki = Pki::new("test CA");
    let (ca, cert, key) = pki.write(&dir, "hub");
    let (grpc, _) = start_hub_with_http(Config {
        http_listen: None,
        ..config(&cert, &key)
    })
    .await;

    // A client that trusts the CA registers and gets its ack over TLS.
    let mut client = FakeClient::connect_tls(grpc, &ca).await;
    client.send_register("svc", &["svc.Do"], &[]).await;
    client.expect_ack().await;
    assert!(probe(grpc, &ca).await.unwrap());

    // Plain text is refused: the health probe never connects.
    let plain =
        tokio::time::timeout(Duration::from_secs(3), snodra_server::health_probe(grpc)).await;
    assert!(
        !matches!(plain, Ok(Ok(true))),
        "plain text must not work: {plain:?}"
    );

    // A client that trusts another CA does not get through.
    let other = Pki::new("other CA");
    let other_ca = dir.write("other-ca.pem", &other.ca_pem);
    assert!(probe(grpc, &other_ca).await.is_err());
}

#[tokio::test]
async fn http_serves_tls_only() {
    let dir = TempDir::new();
    let pki = Pki::new("test CA");
    let (ca, cert, key) = pki.write(&dir, "hub");
    let (_grpc, http) = start_hub_with_http(config(&cert, &key)).await;
    let http = http.unwrap();
    let r = https_request(http, &pki.ca_pem, "GET", "/api/v1/topology", &[])
        .await
        .unwrap();
    assert_eq!(r.status, 200);
    assert!(r.body.contains("\"hubId\""), "{}", r.body);
    let r = https_request(http, &pki.ca_pem, "GET", "/ui/", &[])
        .await
        .unwrap();
    assert_eq!(r.status, 200);

    let plain = plain_http(http).await;
    assert!(
        !plain.contains("200 OK") && !plain.contains("hubId"),
        "{plain}"
    );
    let other = Pki::new("other CA");
    assert!(
        https_request(http, &other.ca_pem, "GET", "/api/v1/topology", &[])
            .await
            .is_err()
    );
    let _ = ca;
}

#[tokio::test]
async fn the_http_listener_can_use_its_own_pair() {
    let dir = TempDir::new();
    let grpc_pki = Pki::new("grpc CA");
    let http_pki = Pki::new("http CA");
    let (grpc_ca, cert, key) = grpc_pki.write(&dir, "grpc");
    let (_, http_cert, http_key) = http_pki.write(&dir, "http");
    let (grpc, http) = start_hub_with_http(Config {
        http_tls_cert: Some(http_cert),
        http_tls_key: Some(http_key),
        ..config(&cert, &key)
    })
    .await;
    let http = http.unwrap();
    assert!(probe(grpc, &grpc_ca).await.unwrap());
    assert_eq!(
        https_request(http, &http_pki.ca_pem, "GET", "/api/v1/topology", &[])
            .await
            .unwrap()
            .status,
        200
    );
    assert!(
        https_request(http, &grpc_pki.ca_pem, "GET", "/api/v1/topology", &[])
            .await
            .is_err()
    );
}

#[tokio::test]
async fn a_replaced_certificate_applies_to_new_connections_and_not_to_open_ones() {
    let dir = TempDir::new();
    let first = Pki::new("first CA");
    let (first_ca, cert, key) = first.write(&dir, "hub");
    let (grpc, http) = start_hub_with_http(config(&cert, &key)).await;
    let http = http.unwrap();

    // An open stream and an open-and-served HTTP exchange on the first certificate.
    let mut open = FakeClient::connect_tls(grpc, &first_ca).await;
    open.send_register("svc", &["svc.Do"], &[]).await;
    open.expect_ack().await;
    assert_eq!(
        https_request(http, &first.ca_pem, "GET", "/api/v1/topology", &[])
            .await
            .unwrap()
            .status,
        200
    );

    // Replace the pair in place: key first, then the certificate, as a deploy tool might.
    let second = Pki::new("second CA");
    let (second_cert, second_key) = second.server();
    std::fs::write(&key, second_key).unwrap();
    std::fs::write(&cert, second_cert).unwrap();
    let second_ca = dir.write("second-ca.pem", &second.ca_pem);

    // New connections get the new certificate, on both listeners.
    eventually("gRPC presents the new certificate", || async {
        probe(grpc, &second_ca).await.is_ok()
    })
    .await;
    eventually("HTTP presents the new certificate", || async {
        https_request(http, &second.ca_pem, "GET", "/api/v1/topology", &[])
            .await
            .is_ok()
    })
    .await;
    assert!(
        probe(grpc, &first_ca).await.is_err(),
        "the old certificate is gone"
    );

    // The connection opened before the swap was not dropped.
    open.heartbeat(7).await;
    match open.next().await {
        Some(Ok(_)) => {}
        other => panic!("the open stream broke: {other:?}"),
    }
}

#[tokio::test]
async fn a_certificate_that_does_not_load_keeps_the_previous_one() {
    let dir = TempDir::new();
    let pki = Pki::new("test CA");
    let (ca, cert, key) = pki.write(&dir, "hub");
    let (grpc, _http) = start_hub_with_http(Config {
        http_listen: None,
        ..config(&cert, &key)
    })
    .await;
    assert!(probe(grpc, &ca).await.unwrap());

    // A key that does not belong to the certificate, then garbage.
    let (_, other_key) = Pki::new("x").server();
    std::fs::write(&key, other_key).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        probe(grpc, &ca).await.unwrap(),
        "a mismatched key is ignored"
    );
    std::fs::write(&cert, "not a certificate").unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(probe(grpc, &ca).await.unwrap(), "garbage is ignored");
}

#[tokio::test]
async fn startup_fails_on_a_certificate_that_does_not_load() {
    let dir = TempDir::new();
    let pki = Pki::new("test CA");
    let (_, cert, _key) = pki.write(&dir, "hub");
    let (_, _, other_key) = Pki::new("other").write(&dir, "other");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let result = snodra_server::serve_with_shutdown(
        config(&cert, &other_key),
        listener,
        None,
        std::future::pending(),
    )
    .await;
    assert!(format!("{:#}", result.unwrap_err()).contains("does not match"));
}

#[tokio::test]
async fn the_probes_of_the_hubs_own_container_work_with_tls() {
    let dir = TempDir::new();
    let pki = Pki::new("test CA");
    let (ca, cert, key) = pki.write(&dir, "hub");
    let (grpc, _) = start_hub_with_http(Config {
        http_listen: None,
        ..config(&cert, &key)
    })
    .await;
    let mut client = FakeClient::connect_tls(grpc, &ca).await;
    client.send_register("billing", &["billing.Pay"], &[]).await;
    client.expect_ack().await;

    // No --address, no --tls-ca, the environment configures TLS: the hub's own certificate is
    // pinned (127.0.0.1 is not in a typical certificate).
    let local = Target::resolve(None, None, grpc.port(), Some(&cert));
    assert!(matches!(local.tls, Some(Trust::Pinned(_))));
    assert!(snodra_server::health_probe_target(&local).await.unwrap());
    let t = snodra_server::topology_probe_target(&local, None)
        .await
        .unwrap();
    assert_eq!(t.services[0].service_name, "billing");

    // --address https://... --tls-ca <pem>: verified against that CA, by name.
    let by_ca = Target::resolve(
        Some(&format!("https://localhost:{}", grpc.port())),
        Some(&ca),
        1,
        None,
    );
    let t = snodra_server::topology_probe_target(&by_ca, None)
        .await
        .unwrap();
    assert_eq!(t.totals.instances, 1);

    // https:// alone verifies against the system roots, which do not hold the test CA.
    let system = Target::resolve(
        Some(&format!("https://localhost:{}", grpc.port())),
        None,
        1,
        None,
    );
    assert_eq!(system.tls, Some(Trust::System));
    assert!(snodra_server::health_probe_target(&system).await.is_err());

    // Pinning a different certificate refuses this hub.
    let (_, other_cert, _) = Pki::new("other").write(&dir, "pin");
    let wrong = Target::resolve(None, None, grpc.port(), Some(&other_cert));
    assert!(snodra_server::health_probe_target(&wrong).await.is_err());
}

#[tokio::test]
async fn the_binary_speaks_tls_with_tls_ca_and_in_its_own_container() {
    let dir = TempDir::new();
    let pki = Pki::new("test CA");
    let (ca, cert, key) = pki.write(&dir, "hub");
    let (grpc, _) = start_hub_with_http(Config {
        http_listen: None,
        ..config(&cert, &key)
    })
    .await;
    let mut client = FakeClient::connect_tls(grpc, &ca).await;
    client.send_register("billing", &["billing.Pay"], &[]).await;
    client.expect_ack().await;
    let bin = env!("CARGO_BIN_EXE_snodra-server");
    let address = format!("https://localhost:{}", grpc.port());

    let out = tokio::process::Command::new(bin)
        .args(["--topology", "--address", &address, "--tls-ca"])
        .arg(&ca)
        .env_clear()
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("service billing (1 instances)"));

    let out = tokio::process::Command::new(bin)
        .args(["--health-check", "--address", &address, "--tls-ca"])
        .arg(&ca)
        .env_clear()
        .output()
        .await
        .unwrap();
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("SERVING"));

    // Without --tls-ca the system roots do not know the test CA.
    let out = tokio::process::Command::new(bin)
        .args(["--health-check", "--address", &address])
        .env_clear()
        .output()
        .await
        .unwrap();
    assert!(!out.status.success());

    // The container's own probe: only the environment says TLS.
    let out = tokio::process::Command::new(bin)
        .arg("--health-check")
        .env_clear()
        .env("SNODRA_SERVER_LISTEN", format!("127.0.0.1:{}", grpc.port()))
        .env("SNODRA_SERVER_TLS_CERT", &cert)
        .env("SNODRA_SERVER_TLS_KEY", &key)
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stdout)
    );
}
