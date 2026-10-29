//! kraken-hub: routes commands and queries between kraken services over gRPC.
//! See kraken/docs/05-services/hub.md.

pub mod config;
pub mod registry;
pub mod router;
pub mod service;
pub mod session;
pub mod telemetry;
pub mod topology;

pub mod proto {
    tonic::include_proto!("kraken.hub.v1");
}

use std::future::Future;
use std::net::SocketAddr;
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
use tokio_util::sync::CancellationToken;
use tonic::transport::server::TcpIncoming;
use tonic_health::ServingStatus;
use tracing::{info, warn};

/// HTTP/2 PING interval; a peer that does not answer within [`HTTP2_KEEPALIVE_TIMEOUT`] is
/// dropped, which ends its `Connect` stream even when it holds no flow-control window.
const HTTP2_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const HTTP2_KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(20);
/// TCP keepalive idle time on accepted sockets.
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);
/// After the shutdown signal, connections get this long to drain before `serve` returns anyway.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// gRPC health service name the hub reports (and `--health-check` probes).
pub const HEALTH_SERVICE: &str = "kraken.hub.v1.Hub";

/// Runs the hub on `listener` until the server stops (no shutdown signal).
pub async fn serve(config: Config, listener: TcpListener) -> anyhow::Result<()> {
    serve_with_shutdown(config, listener, std::future::pending()).await
}

/// Runs the hub on `listener` until `shutdown` completes. Shutdown sequence: health goes
/// `NOT_SERVING`, the watchdog stops, every session is closed with `UNAVAILABLE` (its pending
/// requests fail as on any disconnect), then the server drains connections for at most
/// [`SHUTDOWN_GRACE`] and the function returns.
pub async fn serve_with_shutdown(
    config: Config,
    listener: TcpListener,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let anchor = topology::ClockAnchor::now();
    let config = Arc::new(config);
    let registry = Arc::new(Registry::new(config.owner_grace));
    let sessions = Arc::new(Sessions::new());
    let metrics = Metrics::new(opentelemetry::global::meter("kraken-hub"));
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
        anchor,
        started: anchor.instant,
        heartbeat_timeout: config.heartbeat_timeout,
    };

    let watchdog = tokio::spawn(watchdog(
        Arc::clone(&registry),
        Arc::clone(&sessions),
        Arc::clone(&router),
        config.heartbeat_timeout,
    ));

    let stopping = CancellationToken::new();
    let signal = {
        let stopping = stopping.clone();
        let watchdog = watchdog.abort_handle();
        let (registry, sessions, router) = (
            Arc::clone(&registry),
            Arc::clone(&sessions),
            Arc::clone(&router),
        );
        async move {
            shutdown.await;
            info!("shutdown requested");
            health_reporter
                .set_service_status(HEALTH_SERVICE, ServingStatus::NotServing)
                .await;
            health_reporter
                .set_service_status("", ServingStatus::NotServing)
                .await;
            watchdog.abort();
            for (id, token) in sessions.all_with_tokens() {
                if let Some(tx) = sessions.sender(&id) {
                    let _ = tx.try_send(Err(tonic::Status::unavailable("hub shutting down")));
                }
                service::disconnect(&registry, &sessions, &router, &id, token);
            }
            stopping.cancel();
        }
    };

    let incoming = TcpIncoming::from(listener).with_keepalive(Some(TCP_KEEPALIVE));
    info!(addr = ?incoming.local_addr().ok(), "hub listening");
    let server = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
        .add_service(health_service)
        .add_service(HubServer::new(hub))
        .serve_with_incoming_shutdown(incoming, signal);
    let result = tokio::select! {
        result = server => result,
        _ = async {
            stopping.cancelled().await;
            tokio::time::sleep(SHUTDOWN_GRACE).await;
        } => {
            warn!(grace = ?SHUTDOWN_GRACE, "connections did not drain in time; stopping anyway");
            Ok(())
        }
    };
    watchdog.abort();
    result?;
    info!("hub stopped");
    Ok(())
}

/// Resolves on SIGTERM (container stop; the binary is PID 1 in the image) or Ctrl-C.
pub async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(error = %e, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                sigterm.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

/// Calls `grpc.health.v1.Health/Check` for [`HEALTH_SERVICE`] on `addr`. `Ok(true)` when the
/// hub answers `SERVING`, `Ok(false)` for any other status; connection and RPC failures are
/// errors. The caller bounds the time (the `--health-check` mode uses 3 s).
pub async fn health_probe(addr: SocketAddr) -> anyhow::Result<bool> {
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus as PbStatus;
    use tonic_health::pb::health_client::HealthClient;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))?
        .connect()
        .await?;
    let response = HealthClient::new(channel)
        .check(HealthCheckRequest {
            service: HEALTH_SERVICE.to_string(),
        })
        .await?
        .into_inner();
    Ok(response.status() == PbStatus::Serving)
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
        router.refresh_gauges();
    }
}
