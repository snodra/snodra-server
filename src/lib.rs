//! kraken-hub: routes commands and queries between kraken services over gRPC.
//! See kraken/docs/05-services/hub.md.

pub mod api;
pub mod assets;
pub mod auth;
pub mod cli;
pub mod client;
pub mod config;
pub mod context;
pub mod control;
pub mod edges;
pub mod http;
pub mod jwt;
pub mod mtls;
pub mod processors;
pub mod protocol;
pub mod queue;
pub mod registry;
pub mod reload;
pub mod router;
pub mod service;
pub mod session;
pub mod telemetry;
pub mod tls;
pub mod topology;

#[cfg(test)]
#[path = "pki_test_support.rs"]
mod pki_test_support;

pub mod proto {
    tonic::include_proto!("kraken.hub.v1");
}

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use auth::Authorizer;
use client::Target;
use config::Config;
use proto::hub_server::HubServer;
use registry::Registry;
use router::Router;
use service::HubService;
use session::Sessions;
use telemetry::Metrics;
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
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

/// Runs the hub on `listener` until `shutdown` completes. When `http` is given, the HTTP routes
/// ([`http::app`]: the dashboard and its API, unless disabled) are served there and stop
/// on the same signal. Shutdown sequence: health goes
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
    let auth = Authorizer::from_config(&config, Arc::clone(&metrics))?;
    info!(auth_mode = config.auth_mode.as_str(), "auth mode");
    let router = Router::with_auth(
        Arc::clone(&registry),
        Arc::clone(&sessions),
        Arc::clone(&config),
        Arc::clone(&metrics),
        Arc::clone(&auth),
    );

    let stopping = CancellationToken::new();
    // JWT credentials (ADR 0019): the signing keys load in the background, never blocking or
    // failing startup.
    auth.start_jwks(stopping.clone());
    // TLS and reloads (ADR 0018): load the certificates now (a bad pair is fatal), then watch
    // them and the ACL for changes until shutdown.
    let grpc_tls = config
        .grpc_tls()
        .map(|(cert, key)| tls::CertStore::load(cert, key, "grpc"))
        .transpose()?;
    let http_tls = match (config.http_tls(), config.grpc_tls(), &grpc_tls) {
        (Some(pair), Some(grpc_pair), Some(store)) if pair == grpc_pair => Some(Arc::clone(store)),
        (Some((cert, key)), ..) => Some(tls::CertStore::load(cert, key, "http")?),
        (None, ..) => None,
    };
    // Client certificates (ADR 0020, gRPC listener only): the client CAs are loaded now (a bad
    // bundle is fatal) and reloaded with the certificates.
    let client_auth = match (
        &grpc_tls,
        config.tls_client_ca.as_deref(),
        config.client_auth(),
    ) {
        (Some(_), Some(ca), Some(mode)) => {
            let cas = tls::ClientCaStore::load(ca, mode, Arc::clone(&metrics))?;
            tokio::spawn(reload::watch(
                cas.paths(),
                config.reload_interval,
                stopping.clone(),
                {
                    let cas = Arc::clone(&cas);
                    move || {
                        cas.reload();
                    }
                },
            ));
            tls::ClientAuth::from_store(cas)
        }
        (None, Some(_), _) => anyhow::bail!(
            "KRAKEN_HUB_TLS_CLIENT_CA needs KRAKEN_HUB_TLS_CERT and KRAKEN_HUB_TLS_KEY"
        ),
        _ => tls::ClientAuth::None,
    };
    // One watcher per distinct store (the HTTP port shares the gRPC store by default).
    let mut stores: Vec<Arc<tls::CertStore>> = Vec::new();
    for store in [&grpc_tls, &http_tls].into_iter().flatten() {
        if !stores.iter().any(|known| Arc::ptr_eq(known, store)) {
            stores.push(Arc::clone(store));
        }
    }
    for store in stores {
        tokio::spawn(reload::watch(
            store.paths(),
            config.reload_interval,
            stopping.clone(),
            move || {
                store.reload();
            },
        ));
    }
    // The dashboard token files are polled like the ACL; the live tokens are shared with the HTTP
    // guard and the gRPC Topology check, so a rotation applies without a restart.
    let token_files = config.token_files();
    if !token_files.is_empty() {
        let config = Arc::clone(&config);
        tokio::spawn(reload::watch(
            token_files,
            config.reload_interval,
            stopping.clone(),
            move || {
                config.reload_token_files();
            },
        ));
    }
    if let Some(path) = auth.path() {
        let (auth, registry, sessions, router) = (
            Arc::clone(&auth),
            Arc::clone(&registry),
            Arc::clone(&sessions),
            Arc::clone(&router),
        );
        tokio::spawn(reload::watch(
            vec![path.to_path_buf()],
            config.reload_interval,
            stopping.clone(),
            move || {
                if auth.reload() {
                    service::reevaluate_sessions(&auth, &registry, &sessions, &router);
                }
            },
        ));
    }
    // Set once, when shutdown begins; the gRPC drain and the HTTP join share one deadline from it.
    let stop_began: Arc<std::sync::OnceLock<Instant>> = Arc::default();
    let handshake_limits = config.handshake_limits();
    let http_task = http.map(|listener| {
        let http_metrics = Arc::clone(&metrics);
        let app = http::app(http::HttpState {
            registry: Arc::clone(&registry),
            sessions: Arc::clone(&sessions),
            router: Arc::clone(&router),
            config: Arc::clone(&config),
            auth: Arc::clone(&auth),
            anchor,
            started: anchor.instant,
            build_id: assets::build_id().to_string(),
            stopping: stopping.clone(),
            streams: Arc::default(),
            summaries: Arc::default(),
        });
        let stopping = stopping.clone();
        let local = listener.local_addr();
        info!(addr = ?local.as_ref().ok(), tls = http_tls.is_some(), "http listening");
        let tls_config = http_tls.as_ref().map(|store| {
            tls::server_config(
                Arc::clone(store),
                &[b"h2", b"http/1.1"],
                tls::ClientAuth::None,
            )
        });
        tokio::spawn(async move {
            let token = stopping.clone();
            let shutdown = async move { token.cancelled().await };
            let result = match (tls_config, local) {
                (Some(config), Ok(local)) => {
                    let incoming = TcpIncoming::from(listener).with_keepalive(Some(TCP_KEEPALIVE));
                    let rx = tls::accept(
                        incoming,
                        config,
                        stopping.clone(),
                        handshake_limits,
                        "http",
                        http_metrics,
                    );
                    axum::serve(tls::TlsListener::new(rx, local), app)
                        .with_graceful_shutdown(shutdown)
                        .await
                }
                _ => {
                    axum::serve(listener, app)
                        .with_graceful_shutdown(shutdown)
                        .await
                }
            };
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
        auth: Arc::clone(&auth),
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
    info!(addr = ?incoming.local_addr().ok(), tls = grpc_tls.is_some(), "hub listening");
    let routes = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(HTTP2_KEEPALIVE_INTERVAL))
        .http2_keepalive_timeout(Some(HTTP2_KEEPALIVE_TIMEOUT))
        .add_service(health_service)
        .add_service(HubServer::new(hub));
    type Served =
        std::pin::Pin<Box<dyn Future<Output = Result<(), tonic::transport::Error>> + Send>>;
    let server: Served = match &grpc_tls {
        Some(store) => {
            let config = tls::server_config(Arc::clone(store), &[b"h2"], client_auth);
            let accepted = ReceiverStream::new(tls::accept(
                incoming,
                config,
                stopping.clone(),
                handshake_limits,
                "grpc",
                Arc::clone(&metrics),
            ))
            .map(|stream| Ok::<_, std::io::Error>(tls::TlsConn(stream)));
            Box::pin(routes.serve_with_incoming_shutdown(accepted, signal))
        }
        None => Box::pin(routes.serve_with_incoming_shutdown(incoming, signal)),
    };
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

/// Makes SIGHUP reload the ACL and the TLS certificates at once, instead of at the next poll
/// (`KRAKEN_HUB_RELOAD_INTERVAL`). The binary calls it; in-process hubs (tests) do not, so they
/// never replace the process's SIGHUP disposition.
pub fn reload_on_sighup() {
    #[cfg(unix)]
    tokio::spawn(async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup()) {
            Ok(mut hangup) => {
                while hangup.recv().await.is_some() {
                    info!("SIGHUP: reloading the ACL and the TLS certificates");
                    reload::request_reload();
                }
            }
            Err(e) => warn!(error = %e, "cannot listen for SIGHUP"),
        }
    });
}

/// Calls `grpc.health.v1.Health/Check` for [`HEALTH_SERVICE`] on `addr`. `Ok(true)` when the
/// hub answers `SERVING`, `Ok(false)` for any other status; connection and RPC failures are
/// errors. The caller bounds the time (the `--health-check` mode uses 3 s).
pub async fn health_probe(addr: SocketAddr) -> anyhow::Result<bool> {
    health_probe_target(&Target::plain(addr)).await
}

/// Like [`health_probe`] against any [`Target`] (TLS included).
pub async fn health_probe_target(target: &Target) -> anyhow::Result<bool> {
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_check_response::ServingStatus as PbStatus;
    use tonic_health::pb::health_client::HealthClient;

    let channel = target.connect().await?;
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
    topology_probe_target(&Target::plain(addr), None).await
}

/// Like [`topology_probe`] against any [`Target`]; `token` is the hub's dashboard token, when it
/// has one (sent as `authorization: Bearer ...` metadata).
pub async fn topology_probe_target(
    target: &Target,
    token: Option<&str>,
) -> anyhow::Result<topology::Topology> {
    let channel = target.connect().await?;
    let mut request = tonic::Request::new(proto::TopologyRequest {});
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse()?);
    }
    let response = proto::hub_client::HubClient::new(channel)
        .topology(request)
        .await?
        .into_inner();
    Ok(topology::Topology::from_proto(&response))
}

/// How often the watchdog looks for silent sessions: a tenth of the heartbeat timeout, between
/// 10 ms and 1 s, so an eviction lands at most about a second after the timeout.
fn watchdog_period(heartbeat_timeout: Duration) -> Duration {
    (heartbeat_timeout / 10).clamp(Duration::from_millis(10), Duration::from_secs(1))
}

/// Evicts instances silent for longer than `heartbeat_timeout` and sweeps expired type owners.
async fn watchdog(
    registry: Arc<Registry>,
    sessions: Arc<Sessions>,
    router: Arc<Router>,
    heartbeat_timeout: Duration,
) {
    let mut ticker = tokio::time::interval(watchdog_period(heartbeat_timeout));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let now = Instant::now();
        for (ctx, id, token) in sessions.stale_with_tokens(now, heartbeat_timeout) {
            warn!(instance = %id, "heartbeat timeout, evicting");
            service::disconnect(&registry, &sessions, &router, &ctx, &id, token);
        }
        registry.sweep(now);
        router.refresh_gauges();
    }
}
