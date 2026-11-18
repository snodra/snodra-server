//! `topology_probe` (the `--topology` mode's transport) against an in-process hub.

mod common;

use std::time::Duration;

use common::{FakeClient, start_hub};
use snodra_server::config::Config;

const BOUND: Duration = Duration::from_secs(3);

#[tokio::test]
async fn topology_probe_against_test_hub() {
    let config = Config {
        otlp_endpoint: None,
        ..Config::default()
    };
    let addr = start_hub(config).await;
    let _fake = FakeClient::register(addr, "billing", &["Pay"], &["GetInvoice"]).await;

    let topology = tokio::time::timeout(BOUND, async {
        loop {
            let t = snodra_server::topology_probe(addr).await.unwrap();
            if t.totals.instances == 1 {
                return t;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("instance never appeared in topology");

    assert_eq!(topology.services.len(), 1);
    let service = &topology.services[0];
    assert_eq!(service.service_name, "billing");
    assert_eq!(service.instances.len(), 1);
    assert_eq!(service.instances[0].command_types, ["Pay"]);
    assert_eq!(service.instances[0].query_types, ["GetInvoice"]);
    assert_eq!(topology.totals.instances, 1);
    assert!(snodra_server::cli::render_table(&topology).contains("service billing (1 instances)"));
}

/// The probe reads the processors over gRPC (`InstanceTopology.processors`, ADR 0022) and the
/// table shows them under their service.
#[tokio::test]
async fn topology_probe_shows_the_processors_an_instance_reports() {
    use snodra_server::proto::{ProcessorState, ProcessorStatus, SegmentStatus};
    let addr = start_hub(Config {
        otlp_endpoint: None,
        ..Config::default()
    })
    .await;
    let c = FakeClient::register(addr, "billing", &["Pay"], &[]).await;
    c.status_with_processors(
        &["hub"],
        vec![ProcessorStatus {
            name: "invoices".to_string(),
            subscription: "billing.invoices".to_string(),
            source: "snodra.events".to_string(),
            state: ProcessorState::ProcessorRunning as i32,
            segments: vec![SegmentStatus {
                segment: "0".to_string(),
                position: "4".to_string(),
                committed: "4".to_string(),
                head: "6".to_string(),
                lag: 2,
                replay_until: String::new(),
            }],
            dead_letter_mode: "collection".to_string(),
            ..Default::default()
        }],
    )
    .await;
    let topology = tokio::time::timeout(BOUND, async {
        loop {
            let t = snodra_server::topology_probe(addr).await.unwrap();
            if t.services
                .first()
                .is_some_and(|s| !s.instances[0].processors.is_empty())
            {
                return t;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("processors never appeared in topology");
    let text = snodra_server::cli::render_table(&topology);
    assert!(text.contains("processor | instance"), "{text}");
    assert!(
        text.contains("invoices  | ")
            && text.contains("| RUNNING | billing.invoices | snodra.events | 0        | 2 "),
        "{text}"
    );
}

#[tokio::test]
async fn acl_hash_prints_the_digest_and_never_the_token() {
    use tokio::io::AsyncWriteExt;
    let bin = env!("CARGO_BIN_EXE_snodra-server");
    let abc = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad\n";

    // From stdin (`-`): the token stays out of the process list.
    let mut child = tokio::process::Command::new(bin)
        .args(["acl", "hash", "-"])
        .env_clear()
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"abc\n")
        .await
        .unwrap();
    let out = child.wait_with_output().await.unwrap();
    assert!(out.status.success());
    assert_eq!(String::from_utf8_lossy(&out.stdout), abc);

    // From the argument.
    let out = tokio::process::Command::new(bin)
        .args(["acl", "hash", "abc"])
        .env_clear()
        .output()
        .await
        .unwrap();
    assert_eq!(String::from_utf8_lossy(&out.stdout), abc);
    assert!(!String::from_utf8_lossy(&out.stderr).contains("abc"));

    // An empty token is a usage error, and misuse prints the usage.
    let out = tokio::process::Command::new(bin)
        .args(["acl", "hash", "-"])
        .env_clear()
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    let out = tokio::process::Command::new(bin)
        .args(["acl", "nope"])
        .env_clear()
        .output()
        .await
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("acl hash"));
}

#[tokio::test]
async fn the_topology_mode_sends_the_dashboard_token_from_its_environment() {
    let token = "cli-0123456789abcdef0123";
    let addr = start_hub(Config {
        otlp_endpoint: None,
        dashboard_token: Some(snodra_server::auth::Secret::new(token)),
        ..Config::default()
    })
    .await;
    let bin = env!("CARGO_BIN_EXE_snodra-server");
    let run = |env: Option<&str>| {
        let mut cmd = tokio::process::Command::new(bin);
        cmd.args(["--topology", "--address", &addr.to_string()])
            .env_clear();
        if let Some(t) = env {
            cmd.env("SNODRA_SERVER_DASHBOARD_TOKEN", t);
        }
        async move { cmd.output().await.unwrap() }
    };
    let out = run(Some(token)).await;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = run(None).await;
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(stderr.contains("unauthenticated"), "{stderr}");
    let out = run(Some("wrong-wrong-wrong-wrong")).await;
    assert!(!out.status.success());
}

/// Review fix: a client certificate given for a plain-text target used to be dropped without a
/// word, so a probe the operator believed authenticated ran unauthenticated.
#[tokio::test]
async fn a_client_certificate_for_a_plain_text_target_is_refused() {
    use snodra_server::client::{ClientIdentity, Target};

    let addr = start_hub(Config {
        otlp_endpoint: None,
        ..Config::default()
    })
    .await;
    let identity = ClientIdentity::new("/nowhere/c.pem".into(), "/nowhere/k.pem".into());
    let target = Target::plain(addr).with_identity(Some(identity));
    let e = snodra_server::health_probe_target(&target)
        .await
        .unwrap_err();
    assert!(format!("{e:#}").contains("needs TLS"), "{e:#}");

    // The same through the binary: a failed probe that says why, not a SERVING.
    let bin = env!("CARGO_BIN_EXE_snodra-server");
    for mode in ["--health-check", "--topology"] {
        let out = tokio::process::Command::new(bin)
            .args([mode, "--address", &format!("http://{addr}")])
            .args(["--tls-client-cert", "/nowhere/c.pem"])
            .args(["--tls-client-key", "/nowhere/k.pem"])
            .env_clear()
            .output()
            .await
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(!out.status.success(), "{mode}: {text}");
        assert!(text.contains("needs TLS"), "{mode}: {text}");
    }
}
