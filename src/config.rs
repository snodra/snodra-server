//! Environment-based configuration. Every variable is prefixed `KRAKEN_HUB_`.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;

#[derive(Debug, Clone)]
pub struct Config {
    /// Address the gRPC server binds to. `KRAKEN_HUB_LISTEN`, default `0.0.0.0:7000`.
    pub listen: SocketAddr,
    /// OTLP gRPC endpoint for traces and metrics. `KRAKEN_HUB_OTLP_ENDPOINT`, default `http://otel-lgtm:4317`; empty disables export.
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
    /// A new `Connect` stream must send `Register` within this long or it is closed with
    /// `DEADLINE_EXCEEDED`. `KRAKEN_HUB_HANDSHAKE_TIMEOUT`, default 10 s.
    pub handshake_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 7000)),
            otlp_endpoint: Some("http://otel-lgtm:4317".to_string()),
            log: "info".to_string(),
            default_timeout: Duration::from_millis(5_000),
            max_timeout: Duration::from_millis(60_000),
            heartbeat_timeout: Duration::from_secs(30),
            owner_grace: Duration::from_secs(60),
            session_buffer: 1024,
            handshake_timeout: Duration::from_secs(10),
        }
    }
}

impl Config {
    /// Starts from [`Config::default`] and overrides from `KRAKEN_HUB_*` variables.
    pub fn from_env() -> anyhow::Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    /// Like [`Config::from_env`] over an arbitrary variable lookup (hermetic in tests).
    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let var = |name: &str| lookup(name).filter(|s| !s.is_empty());
        let mut c = Self::default();
        if let Some(v) = var("KRAKEN_HUB_LISTEN") {
            c.listen = v.parse().context("KRAKEN_HUB_LISTEN")?;
        }
        // Unset keeps the default; an explicitly empty value disables export.
        if let Some(v) = lookup("KRAKEN_HUB_OTLP_ENDPOINT") {
            c.otlp_endpoint = Some(v).filter(|s| !s.is_empty());
        }
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
        if let Some(v) = var("KRAKEN_HUB_HANDSHAKE_TIMEOUT") {
            c.handshake_timeout = parse_duration(&v).context("KRAKEN_HUB_HANDSHAKE_TIMEOUT")?;
        }
        Ok(c)
    }
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
        assert_eq!(c.otlp_endpoint.as_deref(), Some("http://otel-lgtm:4317"));
        assert_eq!(c.default_timeout, Duration::from_millis(5000));
        assert_eq!(c.max_timeout, Duration::from_millis(60_000));
        assert_eq!(c.heartbeat_timeout, Duration::from_secs(30));
        assert_eq!(c.owner_grace, Duration::from_secs(60));
        assert_eq!(c.session_buffer, 1024);
        assert_eq!(c.handshake_timeout, Duration::from_secs(10));
        assert_eq!(c.log, "info");
    }

    #[test]
    fn parse_duration_accepts_seconds_suffix_and_millis() {
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert!(parse_duration("abc").is_err());
    }

    fn otlp(value: Option<&str>) -> Option<String> {
        let c = Config::from_lookup(|n| {
            (n == "KRAKEN_HUB_OTLP_ENDPOINT")
                .then(|| value.map(str::to_string))
                .flatten()
        })
        .unwrap();
        c.otlp_endpoint
    }

    #[test]
    fn otlp_endpoint_unset_keeps_default_empty_disables_value_overrides() {
        assert_eq!(otlp(None).as_deref(), Some("http://otel-lgtm:4317"));
        assert_eq!(otlp(Some("")), None);
        assert_eq!(
            otlp(Some("http://x:4317")).as_deref(),
            Some("http://x:4317")
        );
    }

    #[test]
    fn handshake_timeout_is_read_from_env() {
        let c = Config::from_lookup(|n| {
            (n == "KRAKEN_HUB_HANDSHAKE_TIMEOUT").then(|| "250ms".to_string())
        })
        .unwrap();
        assert_eq!(c.handshake_timeout, Duration::from_millis(250));
    }
}
