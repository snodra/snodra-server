//! Integration tests for the `Topology` RPC against an in-process hub.

mod common;

use std::net::SocketAddr;
use std::time::Duration;

use common::{FakeClient, start_hub};
use kraken_hub::config::Config;
use kraken_hub::proto::hub_client::HubClient;
use kraken_hub::proto::{RequestKind, TopologyRequest, TopologyResponse, server_message};
use tonic::transport::Endpoint;

const BOUND: Duration = Duration::from_secs(3);

fn config() -> Config {
    Config {
        otlp_endpoint: None,
        ..Config::default()
    }
}

async fn topology(addr: SocketAddr) -> TopologyResponse {
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    tokio::time::timeout(BOUND, HubClient::new(channel).topology(TopologyRequest {}))
        .await
        .expect("topology call timed out")
        .unwrap()
        .into_inner()
}

/// Polls the RPC until `done` holds (bounded by 3 s) and returns that snapshot.
async fn topology_until(
    addr: SocketAddr,
    done: impl Fn(&TopologyResponse) -> bool,
) -> TopologyResponse {
    let deadline = tokio::time::Instant::now() + BOUND;
    loop {
        let t = topology(addr).await;
        if done(&t) {
            return t;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met in time: {t:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn topology_lists_two_services_with_instances_and_types() {
    let addr = start_hub(config()).await;
    let _a1 = FakeClient::register(addr, "svc-a", &["a.Do"], &[]).await;
    let _a2 = FakeClient::register(addr, "svc-a", &["a.Do"], &[]).await;
    let _b = FakeClient::register(addr, "svc-b", &[], &["b.Get"]).await;

    let t = topology_until(addr, |t| {
        t.totals.as_ref().is_some_and(|x| x.instances == 3) && t.types.len() == 2
    })
    .await;

    let names: Vec<_> = t.services.iter().map(|s| s.service_name.as_str()).collect();
    assert_eq!(names, ["svc-a", "svc-b"]);
    assert_eq!(t.services[0].instances.len(), 2);
    assert_eq!(t.services[1].instances.len(), 1);
    let ids: Vec<_> = t.services[0]
        .instances
        .iter()
        .map(|i| &i.instance_id)
        .collect();
    assert!(ids[0] < ids[1]);
    assert_eq!(t.services[0].instances[0].command_types, ["a.Do"]);
    assert_eq!(t.services[1].instances[0].query_types, ["b.Get"]);

    assert_eq!(t.types[0].r#type, "a.Do");
    assert_eq!(t.types[0].kind(), RequestKind::Command);
    assert_eq!(t.types[0].owner_service, "svc-a");
    assert_eq!(t.types[0].live_instances, 2);
    assert!(!t.types[0].orphaned);
    assert_eq!(t.types[1].r#type, "b.Get");
    assert_eq!(t.types[1].kind(), RequestKind::Query);
    assert_eq!(t.types[1].live_instances, 1);

    let totals = t.totals.unwrap();
    assert_eq!(
        (
            totals.services,
            totals.instances,
            totals.command_types,
            totals.query_types
        ),
        (2, 3, 1, 1)
    );
    assert_eq!(totals.in_flight, 0);
    assert_eq!(t.heartbeat_timeout_ms, 30_000);
    assert!(t.now_ms >= t.hub_started_at_ms && t.hub_started_at_ms > 0);
    for s in &t.services {
        for i in &s.instances {
            assert!(
                i.connected_since_ms >= t.hub_started_at_ms && i.connected_since_ms <= t.now_ms
            );
            assert!(!i.stale);
        }
    }
}

#[tokio::test]
async fn topology_shows_orphaned_type_after_last_instance_leaves() {
    let addr = start_hub(Config {
        owner_grace: Duration::from_secs(60),
        ..config()
    })
    .await;
    let _a = FakeClient::register(addr, "svc-a", &["a.Do"], &[]).await;
    let b = FakeClient::register(addr, "svc-b", &["b.Do"], &[]).await;
    topology_until(addr, |t| t.types.len() == 2).await;
    drop(b);

    let t = topology_until(addr, |t| {
        t.services.len() == 1 && t.types.iter().any(|x| x.orphaned)
    })
    .await;
    assert_eq!(t.services[0].service_name, "svc-a");
    let orphan = t.types.iter().find(|x| x.r#type == "b.Do").unwrap();
    assert!(orphan.orphaned);
    assert_eq!(orphan.live_instances, 0);
    assert!(orphan.orphaned_for_ms >= 0);
    let live = t.types.iter().find(|x| x.r#type == "a.Do").unwrap();
    assert!(!live.orphaned);
    assert_eq!(live.orphaned_for_ms, 0);
}

#[tokio::test]
async fn topology_counts_in_flight_for_handler_and_sender() {
    let addr = start_hub(config()).await;
    let mut a = FakeClient::register(addr, "svc-a", &["a.Do"], &[]).await;
    let b = FakeClient::register(addr, "svc-b", &[], &[]).await;
    topology_until(addr, |t| {
        t.totals.as_ref().is_some_and(|x| x.instances == 2)
    })
    .await;

    b.tx.send(FakeClient::request(
        "r1",
        RequestKind::Command,
        "a.Do",
        "k",
        60_000,
    ))
    .await
    .unwrap();
    // The handler receives the request but does not reply: it stays pending.
    let msg = a.next_within(BOUND).await.unwrap().unwrap();
    assert!(matches!(
        msg.message,
        Some(server_message::Message::Request(_))
    ));

    let t = topology(addr).await;
    let inst = |svc: &str| {
        t.services
            .iter()
            .find(|s| s.service_name == svc)
            .unwrap()
            .instances[0]
            .clone()
    };
    assert_eq!(inst("svc-a").in_flight_as_handler, 1);
    assert_eq!(inst("svc-a").in_flight_as_sender, 0);
    assert_eq!(inst("svc-b").in_flight_as_sender, 1);
    assert_eq!(inst("svc-b").in_flight_as_handler, 0);
    assert_eq!(t.totals.unwrap().in_flight, 1);

    // Replying clears the counters.
    a.tx.send(FakeClient::reply_ok("r1", b"ok")).await.unwrap();
    let t = topology_until(addr, |t| {
        t.totals.as_ref().is_some_and(|x| x.in_flight == 0)
    })
    .await;
    assert!(t.services.iter().all(|s| {
        s.instances
            .iter()
            .all(|i| i.in_flight_as_handler == 0 && i.in_flight_as_sender == 0)
    }));
}

#[tokio::test]
async fn topology_reflects_eviction() {
    let addr = start_hub(Config {
        heartbeat_timeout: Duration::from_secs(1),
        ..config()
    })
    .await;
    // Never heartbeats, so the watchdog evicts it. Only the eventual empty state is asserted:
    // the intermediate "one instance" state is not polled, so a slow runner cannot flake it.
    let _quiet = FakeClient::register(addr, "svc-a", &["a.Do"], &[]).await;

    let t = topology_until(addr, |t| t.services.is_empty()).await;
    assert_eq!(t.totals.unwrap().instances, 0);
    assert_eq!(t.heartbeat_timeout_ms, 1000);
}
