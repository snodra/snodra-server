//! kraken-hub: routes commands and queries between kraken services over gRPC.
//! See kraken/docs/05-services/hub.md.

pub mod cli;
pub mod config;
pub mod context;
pub mod protocol;
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

use anyhow::Context;
use axum::Json;
use axum::extract::State;
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

/// Runs the hub on `listener` until the server stops (no shutdown signal). The HTTP mirror is
/// bound here from `config.http_listen`; failing to bind it is fatal.
pub async fn serve(config: Config, listener: TcpListener) -> anyhow::Result<()> {
    let http = bind_http(&config).await?;
    serve_with_shutdown(config, listener, http, std::future::pending()).await
}

/// Binds the HTTP mirror listener from `config.http_listen` (`None` when disabled).
pub async fn bind_http(config: &Config) -> anyhow::Result<Option<TcpListener>> {
    match config.http_listen {
        Some(addr) => Ok(Some(
            TcpListener::bind(addr)
                .await
                .with_context(|| format!("bind HTTP listener {addr}"))?,
        )),
        None => Ok(None),
    }
}

/// Everything the `GET /topology` handler needs; the same `Arc`s the gRPC service holds.
#[derive(Clone)]
struct TopologyState {
    registry: Arc<Registry>,
    sessions: Arc<Sessions>,
    router: Arc<Router>,
    anchor: topology::ClockAnchor,
    started: Instant,
    heartbeat_timeout: Duration,
}

async fn get_topology(State(s): State<TopologyState>) -> Json<topology::Topology> {
    Json(topology::snapshot(
        &s.registry,
        &s.sessions,
        &s.router,
        s.anchor,
        s.started,
        s.heartbeat_timeout,
        Instant::now(),
    ))
}

/// Runs the hub on `listener` until `shutdown` completes. When `http` is given, `GET /topology`
/// is served there (JSON mirror of the `Topology` RPC) and stops on the same signal. Shutdown sequence: health goes
/// `NOT_SERVING`, the watchdog stops, every session is closed with `UNAVAILABLE` (its pending
/// requests fail as on any disconnect), then the server drains connections for at most
/// [`SHUTDOWN_GRACE`] and the function returns.
pub async fn serve_with_shutdown(
    config: Config,
    listener: TcpListener,
    http: Option<TcpListener>,
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

    let stopping = CancellationToken::new();
    // Set once, when shutdown begins; the gRPC drain and the HTTP join share one deadline from it.
    let stop_began: Arc<std::sync::OnceLock<Instant>> = Arc::default();
    let http_task = http.map(|listener| {
        let state = TopologyState {
            registry: Arc::clone(&registry),
            sessions: Arc::clone(&sessions),
            router: Arc::clone(&router),
            anchor,
            started: anchor.instant,
            heartbeat_timeout: config.heartbeat_timeout,
        };
        let app = axum::Router::new()
            .route("/topology", axum::routing::get(get_topology))
            .with_state(state);
        let stopping = stopping.clone();
        info!(addr = ?listener.local_addr().ok(), "http listening");
        tokio::spawn(async move {
            let token = stopping.clone();
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(async move { token.cancelled().await })
                .await;
            if !stopping.is_cancelled() {
                warn!("http server stopped before shutdown was requested");
            }
            info!("http stopped");
            result
        })
    });

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

    let signal = {
        let stopping = stopping.clone();
        let stop_began = Arc::clone(&stop_began);
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
            for (ctx, id, token) in sessions.all_with_tokens() {
                if let Some(tx) = sessions.sender(&ctx, &id) {
                    let _ = tx.try_send(Err(tonic::Status::unavailable("hub shutting down")));
                }
                service::disconnect(&registry, &sessions, &router, &ctx, &id, token);
            }
            let _ = stop_began.set(Instant::now());
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
            let began = stop_began.get().copied().unwrap_or_else(Instant::now);
            tokio::time::sleep_until(tokio::time::Instant::from_std(began + SHUTDOWN_GRACE)).await;
        } => {
            warn!(grace = ?SHUTDOWN_GRACE, "connections did not drain in time; stopping anyway");
            Ok(())
        }
    };
    watchdog.abort();
    // Also covers the gRPC server ending on its own: stop HTTP, then wait for it until the same
    // deadline the gRPC drain used (shutdown start + grace), so the total stays within one grace.
    stopping.cancel();
    if let Some(mut task) = http_task {
        let began = stop_began.get().copied().unwrap_or_else(Instant::now);
        let deadline = tokio::time::Instant::from_std(began + SHUTDOWN_GRACE);
        match tokio::time::timeout_at(deadline, &mut task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(e))) => warn!(error = %e, "http server failed"),
            Ok(Err(e)) => warn!(error = %e, "http task failed"),
            Err(_) => {
                warn!("http connections did not drain in time; aborting");
                task.abort();
            }
        }
    }
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

/// Calls the `Topology` RPC on `addr` and converts the response. The caller bounds the time
/// (the `--topology` mode uses 3 s).
pub async fn topology_probe(addr: SocketAddr) -> anyhow::Result<topology::Topology> {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))?
        .connect()
        .await?;
    let response = proto::hub_client::HubClient::new(channel)
        .topology(proto::TopologyRequest {})
        .await?
        .into_inner();
    Ok(topology::Topology::from_proto(&response))
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
        for (ctx, id, token) in sessions.stale_with_tokens(now, timeout) {
            warn!(instance = %id, "heartbeat timeout, evicting");
            service::disconnect(&registry, &sessions, &router, &ctx, &id, token);
        }
        registry.sweep(now);
        router.refresh_gauges();
    }
}
