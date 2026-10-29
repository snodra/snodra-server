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
