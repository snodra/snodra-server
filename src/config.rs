//! Environment-based configuration. Every variable is prefixed `KRAKEN_HUB_`.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;

#[derive(Debug, Clone)]
pub struct Config {
    /// Address the gRPC server binds to. `KRAKEN_HUB_LISTEN`, default `0.0.0.0:7000`.
    pub listen: SocketAddr,
    /// OTLP gRPC endpoint for traces and metrics. `KRAKEN_HUB_OTLP_ENDPOINT`, unset by default.
    pub otlp_endpoint: Option<String>,
    /// `tracing` filter. `KRAKEN_HUB_LOG`, default `info`.
    pub log: String,
    /// Used when `Request.timeout_ms` is zero. `KRAKEN_HUB_DEFAULT_TIMEOUT_MS`, default 5000.
    pub default_timeout: Duration,
    /// Upper bound for `Request.timeout_ms`; larger hints are clamped.
    /// `KRAKEN_HUB_MAX_TIMEOUT_MS`, default 60000.
    pub max_timeout: Duration,
    /// An instance is dropped after this long without a heartbeat.
    /// `KRAKEN_HUB_HEARTBEAT_TIMEOUT`, default 30 s.
    pub heartbeat_timeout: Duration,
    /// Type ownership is kept this long after the last instance leaves.
    /// `KRAKEN_HUB_OWNER_GRACE`, default 60 s.
    pub owner_grace: Duration,
    /// Per-session outbound buffer, in messages. `KRAKEN_HUB_SESSION_BUFFER`, default 1024.
    pub session_buffer: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 7000)),
            otlp_endpoint: None,
            log: "info".to_string(),
            default_timeout: Duration::from_millis(5_000),
            max_timeout: Duration::from_millis(60_000),
            heartbeat_timeout: Duration::from_secs(30),
            owner_grace: Duration::from_secs(60),
            session_buffer: 1024,
        }
    }
}

impl Config {
    /// Starts from [`Config::default`] and overrides from `KRAKEN_HUB_*` variables.
    pub fn from_env() -> anyhow::Result<Self> {
        let mut c = Self::default();
        if let Some(v) = var("KRAKEN_HUB_LISTEN") {
            c.listen = v.parse().context("KRAKEN_HUB_LISTEN")?;
        }
        c.otlp_endpoint = var("KRAKEN_HUB_OTLP_ENDPOINT");
        if let Some(v) = var("KRAKEN_HUB_LOG") {
            c.log = v;
        }
        if let Some(v) = var("KRAKEN_HUB_DEFAULT_TIMEOUT_MS") {
            c.default_timeout = millis(&v).context("KRAKEN_HUB_DEFAULT_TIMEOUT_MS")?;
        }
        if let Some(v) = var("KRAKEN_HUB_MAX_TIMEOUT_MS") {
            c.max_timeout = millis(&v).context("KRAKEN_HUB_MAX_TIMEOUT_MS")?;
        }
        if let Some(v) = var("KRAKEN_HUB_HEARTBEAT_TIMEOUT") {
            c.heartbeat_timeout = parse_duration(&v).context("KRAKEN_HUB_HEARTBEAT_TIMEOUT")?;
        }
        if let Some(v) = var("KRAKEN_HUB_OWNER_GRACE") {
            c.owner_grace = parse_duration(&v).context("KRAKEN_HUB_OWNER_GRACE")?;
        }
        if let Some(v) = var("KRAKEN_HUB_SESSION_BUFFER") {
            c.session_buffer = v.parse().context("KRAKEN_HUB_SESSION_BUFFER")?;
        }
        Ok(c)
    }
}

/// Reads a variable, treating unset and empty the same.
fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn millis(s: &str) -> anyhow::Result<Duration> {
    Ok(Duration::from_millis(s.trim().parse()?))
}

/// Parses `"30"` (seconds), `"30s"` or `"500ms"`.
pub fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    if let Some(ms) = s.strip_suffix("ms") {
        return Ok(Duration::from_millis(ms.trim().parse()?));
    }
    let secs = s.strip_suffix('s').unwrap_or(s);
    Ok(Duration::from_secs(secs.trim().parse()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_spec() {
        let c = Config::default();
        assert_eq!(c.listen, "0.0.0.0:7000".parse().unwrap());
        assert_eq!(c.otlp_endpoint, None);
        assert_eq!(c.default_timeout, Duration::from_millis(5000));
        assert_eq!(c.max_timeout, Duration::from_millis(60_000));
        assert_eq!(c.heartbeat_timeout, Duration::from_secs(30));
        assert_eq!(c.owner_grace, Duration::from_secs(60));
        assert_eq!(c.session_buffer, 1024);
        assert_eq!(c.log, "info");
    }

    #[test]
    fn parse_duration_accepts_seconds_suffix_and_millis() {
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert!(parse_duration("abc").is_err());
    }
}
