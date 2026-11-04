//! Regression: an evicted stream whose client stalls (never reads, so the response body is never
//! drained) must not leave a "ghost" registration behind. Before the fix, the evicted stream's
//! task kept processing inbound messages; a late `Register` re-added the instance to the
//! registry with no session behind it, and a slice of keys routed to HANDLER_DISCONNECTED
//! forever.
//!
//! How the window is driven: A registers, then B floods A with 300 large requests that A never
//! reads, filling A's HTTP/2 flow-control window and outbound buffer so the response body cannot
//! drain. A stops heartbeating, so the watchdog evicts it (300 ms timeout). A then sends a late
//! `Register` on the same stream and goes away; after the owner grace (100 ms) and a sweep, the
//! type must have no owner: a request gets NO_HANDLER, not HANDLER_DISCONNECTED.

mod common;

use std::time::Duration;

use common::{FakeClient, start_hub};
use kraken_hub::config::Config;
use kraken_hub::proto::{
    ClientMessage, ErrorCode, Heartbeat, RequestKind, client_message, server_message,
};

fn heartbeat() -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Heartbeat(Heartbeat {
            sent_at_ms: 1,
        })),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_register_after_eviction_leaves_no_ghost() {
    let config = Config {
        heartbeat_timeout: Duration::from_millis(300),
        owner_grace: Duration::from_millis(100),
        otlp_endpoint: None,
        ..Config::default()
    };
    let addr = start_hub(config).await;
    let mut a = FakeClient::register(addr, "a", &["a.Do"], &[]).await;
    a.tx.send(heartbeat()).await.unwrap();
    a.next().await.unwrap().unwrap();
    let b = FakeClient::register(addr, "b", &[], &[]).await;
    // Fill A's h2 window and channel; A never reads again.
    for i in 0..300 {
        let mut m = FakeClient::request(&format!("r{i}"), RequestKind::Command, "a.Do", "", 60_000);
        if let Some(client_message::Message::Request(r)) = m.message.as_mut() {
            r.payload = vec![b'x'; 64 * 1024];
        }
        b.tx.send(m).await.unwrap();
    }
    tokio::time::sleep(Duration::from_millis(1200)).await; // the watchdog evicts A
    a.send_register("a", &["a.Do"], &[]).await; // late Register on the evicted stream
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(a);
    tokio::time::sleep(Duration::from_millis(1000)).await; // past the owner grace and a sweep

    let mut c = FakeClient::register(addr, "c", &[], &[]).await;
    c.tx.send(FakeClient::request(
        "z1",
        RequestKind::Command,
        "a.Do",
        "",
        0,
    ))
    .await
    .unwrap();
    match c.next().await.unwrap().unwrap().message {
        Some(server_message::Message::Error(e)) => {
            assert_eq!(
                e.code(),
                ErrorCode::NoHandler,
                "ghost instance: {}",
                e.message
            );
        }
        other => panic!("expected an error, got {other:?}"),
    }
}
