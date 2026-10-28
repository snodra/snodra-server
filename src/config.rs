//! Environment-based configuration. Every variable is prefixed `KRAKEN_HUB_`.

use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    /// Address the gRPC server binds to. `KRAKEN_HUB_LISTEN`, default `0.0.0.0:7000`.
    pub listen: SocketAddr,
    /// OTLP gRPC endpoint for traces and metrics. `KRAKEN_HUB_OTLP_ENDPOINT`, optional.
    pub otlp_endpoint: Option<String>,
    /// An instance is dropped after this long without a heartbeat.
    /// `KRAKEN_HUB_HEARTBEAT_TIMEOUT` in seconds, default 30 (three missed 10 s heartbeats).
    pub heartbeat_timeout: Duration,
    /// Upper bound for `Request.timeout_ms`; larger hints are clamped.
    /// `KRAKEN_HUB_MAX_TIMEOUT_MS`, default 30000.
    pub max_timeout: Duration,
    /// Used when `Request.timeout_ms` is zero. Fixed at 5000 ms per docs/05-services/hub.md.
    pub default_timeout: Duration,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let listen = std::env::var("KRAKEN_HUB_LISTEN")
            .unwrap_or_else(|_| "0.0.0.0:7000".to_string())
            .parse::<SocketAddr>()?;
        let otlp_endpoint = std::env::var("KRAKEN_HUB_OTLP_ENDPOINT").ok().filter(|s| !s.is_empty());
        let heartbeat_timeout = Duration::from_secs(
            std::env::var("KRAKEN_HUB_HEARTBEAT_TIMEOUT")
                .ok()
                .map(|v| v.parse::<u64>())
                .transpose()?
                .unwrap_or(30),
        );
        let max_timeout = Duration::from_millis(
            std::env::var("KRAKEN_HUB_MAX_TIMEOUT_MS")
                .ok()
                .map(|v| v.parse::<u64>())
                .transpose()?
                .unwrap_or(30_000),
        );
        Ok(Self {
            listen,
            otlp_endpoint,
            heartbeat_timeout,
            max_timeout,
            default_timeout: Duration::from_millis(5_000),
        })
    }
}
