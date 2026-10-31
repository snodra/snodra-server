//! kraken-hub binary: configuration, telemetry, then `kraken_hub::serve_with_shutdown`.
//!
//! `kraken-hub --health-check` instead probes the local hub's gRPC health service (for the
//! image's `HEALTHCHECK`; the distroless image has no shell or curl) and exits 0 when it is
//! `SERVING`, 1 otherwise. `kraken-hub --topology [--json]` prints the local hub's topology.

use std::net::{Ipv4Addr, SocketAddr};
use std::process::ExitCode;
use std::time::Duration;

use kraken_hub::cli::{self, Mode};
use kraken_hub::config::Config;
use tokio::net::TcpListener;

const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    let mode = match cli::parse(std::env::args().skip(1)) {
        Ok(mode) => mode,
        Err(e) => {
            eprintln!("{e}\n{}", cli::USAGE);
            return Ok(ExitCode::from(2));
        }
    };
    let config = Config::from_env()?;
    match mode {
        Mode::HealthCheck => return Ok(health_check(&config).await),
        Mode::Topology { json } => return Ok(topology(&config, json).await),
        Mode::Serve => {}
    }
    let telemetry = kraken_hub::telemetry::init(&config)?;
    for warning in config.warnings() {
        tracing::warn!("{warning}");
    }
    let listener = TcpListener::bind(config.listen).await?;
    let http = kraken_hub::bind_http(&config).await?;
    kraken_hub::serve_with_shutdown(config, listener, http, kraken_hub::shutdown_signal()).await?;
    // Flushes pending spans and metrics.
    drop(telemetry);
    Ok(ExitCode::SUCCESS)
}

async fn health_check(config: &Config) -> ExitCode {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, config.listen.port()));
    match tokio::time::timeout(PROBE_TIMEOUT, kraken_hub::health_probe(addr)).await {
        Ok(Ok(true)) => {
            println!("kraken-hub at {addr}: SERVING");
            ExitCode::SUCCESS
        }
        Ok(Ok(false)) => {
            println!("kraken-hub at {addr}: NOT_SERVING");
            ExitCode::FAILURE
        }
        Ok(Err(e)) => {
            println!("kraken-hub at {addr}: health check failed: {e:#}");
            ExitCode::FAILURE
        }
        Err(_) => {
            println!("kraken-hub at {addr}: health check timed out after {PROBE_TIMEOUT:?}");
            ExitCode::FAILURE
        }
    }
}

async fn topology(config: &Config, json: bool) -> ExitCode {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, config.listen.port()));
    let result = match tokio::time::timeout(PROBE_TIMEOUT, kraken_hub::topology_probe(addr)).await {
        Ok(result) => result.map_err(|e| format!("{e:#}")),
        Err(_) => Err(format!("timed out after {PROBE_TIMEOUT:?}")),
    };
    let rendered = result.and_then(|t| {
        if json {
            serde_json::to_string_pretty(&t).map_err(|e| e.to_string())
        } else {
            Ok(cli::render_table(&t).trim_end().to_string())
        }
    });
    match rendered {
        Ok(text) => {
            println!("{text}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("kraken-hub at {addr}: topology failed: {e}");
            ExitCode::FAILURE
        }
    }
}
