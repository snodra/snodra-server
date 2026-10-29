//! kraken-hub: routes commands and queries between kraken services over gRPC.
//! See kraken/docs/05-services/hub.md.

pub mod config;
pub mod registry;
pub mod router;
pub mod service;
pub mod session;
pub mod telemetry;

pub mod proto {
    tonic::include_proto!("kraken.hub.v1");
}

use std::sync::Arc;
use std::time::{Duration, Instant};

use config::Config;
use proto::hub_server::HubServer;
use registry::Registry;
use router::Router;
use service::HubService;
use session::Sessions;
use telemetry::Metrics;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::{info, warn};

/// Runs the hub on `listener` until the server stops.
pub async fn serve(config: Config, listener: TcpListener) -> anyhow::Result<()> {
    let config = Arc::new(config);
    let registry = Arc::new(Registry::new(config.owner_grace));
    let sessions = Arc::new(Sessions::new());
    let metrics = Metrics::noop();
    let router = Router::new(
        Arc::clone(&registry),
        Arc::clone(&sessions),
        Arc::clone(&config),
        metrics,
    );

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter.set_serving::<HubServer<HubService>>().await;

    let hub = HubService {
        registry: Arc::clone(&registry),
        sessions: Arc::clone(&sessions),
        router: Arc::clone(&router),
        config: Arc::clone(&config),
    };

    let watchdog = tokio::spawn(watchdog(
        Arc::clone(&registry),
        Arc::clone(&sessions),
        Arc::clone(&router),
        config.heartbeat_timeout,
    ));

    info!(addr = ?listener.local_addr().ok(), "hub listening");
    let result = tonic::transport::Server::builder()
        .add_service(health_service)
        .add_service(HubServer::new(hub))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await;
    watchdog.abort();
    result?;
    Ok(())
}

/// Evicts instances without a heartbeat for `timeout` and sweeps expired type owners.
async fn watchdog(
    registry: Arc<Registry>,
    sessions: Arc<Sessions>,
    router: Arc<Router>,
    timeout: Duration,
) {
    let period = (timeout / 3).max(Duration::from_millis(10));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let now = Instant::now();
        for (id, token) in sessions.stale_with_tokens(now, timeout) {
            warn!(instance = %id, "heartbeat timeout, evicting");
            service::disconnect(&registry, &sessions, &router, &id, token);
        }
        registry.sweep(now);
    }
}
