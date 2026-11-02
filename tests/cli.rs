//! `topology_probe` (the `--topology` mode's transport) against an in-process hub.

mod common;

use std::time::Duration;

use common::{FakeClient, start_hub};
use kraken_hub::config::Config;

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
            let t = kraken_hub::topology_probe(addr).await.unwrap();
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
    assert!(kraken_hub::cli::render_table(&topology).contains("service billing (1 instances)"));
}

#[tokio::test]
async fn acl_hash_prints_the_digest_and_never_the_token() {
    use tokio::io::AsyncWriteExt;
    let bin = env!("CARGO_BIN_EXE_kraken-hub");
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
