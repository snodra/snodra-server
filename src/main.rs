//! kraken-hub binary: configuration, telemetry, then `kraken_hub::serve_with_shutdown`.
//!
//! `kraken-hub --health-check` instead probes the local hub's gRPC health service (for the
//! image's `HEALTHCHECK`; the distroless image has no shell or curl) and exits 0 when it is
//! `SERVING`, 1 otherwise.

use std::net::{Ipv4Addr, SocketAddr};
use std::process::ExitCode;
use std::time::Duration;

use kraken_hub::config::Config;
use tokio::net::TcpListener;

const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() -> anyhow::Result<ExitCode> {
    let config = Config::from_env()?;
    if std::env::args().skip(1).any(|a| a == "--health-check") {
        return Ok(health_check(&config).await);
    }
    let telemetry = kraken_hub::telemetry::init(&config)?;
    let listener = TcpListener::bind(config.listen).await?;
    kraken_hub::serve_with_shutdown(config, listener, kraken_hub::shutdown_signal()).await?;
    // Flushes pending spans and metrics.
    drop(telemetry);
    Ok(ExitCode::SUCCESS)
}

async fn health_check(config: &Config) -> ExitCode {
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, config.listen.port()));
    match tokio::time::timeout(HEALTH_CHECK_TIMEOUT, kraken_hub::health_probe(addr)).await {
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
            println!("kraken-hub at {addr}: health check timed out after {HEALTH_CHECK_TIMEOUT:?}");
            ExitCode::FAILURE
        }
    }
}
