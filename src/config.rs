//! Environment-based configuration. Every variable is prefixed `KRAKEN_HUB_`.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use anyhow::{Context, bail};

use crate::protocol;

#[derive(Debug, Clone)]
pub struct Config {
    /// Identifies this hub process in `RegisterAck.hub_id` and the topology. `KRAKEN_HUB_ID`,
    /// default the hostname plus a random 8-hex-digit suffix (unique per process start).
    pub hub_id: String,
    /// Address the gRPC server binds to. `KRAKEN_HUB_LISTEN`, default `0.0.0.0:7000`.
    pub listen: SocketAddr,
    /// Address of the HTTP JSON mirror (`GET /topology`). `KRAKEN_HUB_HTTP_LISTEN`, default
    /// `0.0.0.0:7001`; an explicitly empty value disables the HTTP listener.
    pub http_listen: Option<SocketAddr>,
    /// OTLP gRPC endpoint for traces and metrics. `KRAKEN_HUB_OTLP_ENDPOINT`, default `http://otel-lgtm:4317`; empty disables export.
    pub otlp_endpoint: Option<String>,
    /// `tracing` filter. `KRAKEN_HUB_LOG`, default `info`.
    pub log: String,
    /// Used when `Request.timeout_ms` is zero. `KRAKEN_HUB_DEFAULT_TIMEOUT_MS`, default 5000.
    pub default_timeout: Duration,
    /// Upper bound for `Request.timeout_ms`; larger hints are clamped.
    /// `KRAKEN_HUB_MAX_TIMEOUT_MS`, default 60000.
    pub max_timeout: Duration,
    /// Heartbeat period announced to v2 sessions in `RegisterAck`.
    /// `KRAKEN_HUB_HEARTBEAT_INTERVAL`, default 3 s. `heartbeat_timeout` must be at least twice it.
    pub heartbeat_interval: Duration,
    /// A v2 session (`Register.protocol_version >= 2`) is dropped after this long without a
    /// heartbeat. `KRAKEN_HUB_HEARTBEAT_TIMEOUT`, default 10 s.
    pub heartbeat_timeout: Duration,
    /// A v1 session (`protocol_version` 0, clients that heartbeat every 10 s) is dropped after this
    /// long without a heartbeat. `KRAKEN_HUB_LEGACY_HEARTBEAT_TIMEOUT`, default 30 s.
    pub legacy_heartbeat_timeout: Duration,
    /// Per-type request timeouts. For a listed type the override replaces the default (requests
    /// with `timeout_ms == 0`) and the clamp (`max_timeout`). `KRAKEN_HUB_TIMEOUT_OVERRIDES`,
    /// `type=ms,type=ms`, default none.
    pub timeout_overrides: BTreeMap<String, Duration>,
    /// Requests the hub queues for one flow-controlled instance before rejecting new ones.
    /// `KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE`, default 10 000.
    pub queue_capacity_per_instance: usize,
    /// Encoded bytes the hub queues for one flow-controlled instance before rejecting new
    /// requests. `KRAKEN_HUB_QUEUE_BYTES_PER_INSTANCE`, default 64 MiB (`B`, `KiB`, `MiB`, `GiB`).
    pub queue_bytes_per_instance: usize,
    /// Encoded bytes the hub queues over all flow-controlled instances together before
    /// rejecting new requests (reason `queue_bytes_total`). `KRAKEN_HUB_QUEUE_BYTES_TOTAL`,
    /// default 512 MiB.
    pub queue_bytes_total: usize,
    /// Type ownership is kept this long after the last instance leaves.
    /// `KRAKEN_HUB_OWNER_GRACE`, default 60 s.
    pub owner_grace: Duration,
    /// Per-session outbound buffer, in messages. `KRAKEN_HUB_SESSION_BUFFER`, default 1024.
    pub session_buffer: usize,
    /// A new `Connect` stream must send `Register` within this long or it is closed with
    /// `DEADLINE_EXCEEDED`. `KRAKEN_HUB_HANDSHAKE_TIMEOUT`, default 10 s.
    pub handshake_timeout: Duration,
    /// Serve the read-only dashboard (`/ui/`) and its JSON API (`/api/v1/`) on the HTTP port and
    /// keep the edge statistics they show. `KRAKEN_HUB_DASHBOARD_ENABLED`, default true.
    /// `GET /topology` is served either way.
    pub dashboard_enabled: bool,
    /// Grafana base URL the dashboard links to (`http://` or `https://`).
    /// `KRAKEN_HUB_GRAFANA_URL`, default none (no link).
    pub grafana_url: Option<String>,
    /// Cap on `(sender service, type)` edges kept for the dashboard graph; at the cap the least
    /// recently used edges are dropped. `KRAKEN_HUB_EDGE_STATS_MAX_EDGES`, default 10 000.
    pub edge_stats_max_edges: usize,
    /// Sliding window of the edge statistics. `KRAKEN_HUB_EDGE_STATS_WINDOW`, default 5 minutes,
    /// at least 10 s.
    pub edge_stats_window: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            hub_id: default_hub_id(hostname().as_deref()),
            listen: SocketAddr::from(([0, 0, 0, 0], 7000)),
            http_listen: Some(SocketAddr::from(([0, 0, 0, 0], 7001))),
            otlp_endpoint: Some("http://otel-lgtm:4317".to_string()),
            log: "info".to_string(),
            default_timeout: Duration::from_millis(5_000),
            max_timeout: Duration::from_millis(60_000),
            heartbeat_interval: Duration::from_secs(3),
            heartbeat_timeout: Duration::from_secs(10),
            legacy_heartbeat_timeout: Duration::from_secs(30),
            timeout_overrides: BTreeMap::new(),
            queue_capacity_per_instance: 10_000,
            queue_bytes_per_instance: 64 * 1024 * 1024,
            queue_bytes_total: 512 * 1024 * 1024,
            owner_grace: Duration::from_secs(60),
            session_buffer: 1024,
            handshake_timeout: Duration::from_secs(10),
            dashboard_enabled: true,
            grafana_url: None,
            edge_stats_max_edges: 10_000,
            edge_stats_window: Duration::from_secs(300),
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
        if let Some(v) = var("KRAKEN_HUB_ID") {
            c.hub_id = v.trim().to_string();
        }
        if let Some(v) = var("KRAKEN_HUB_LISTEN") {
            c.listen = v.parse().context("KRAKEN_HUB_LISTEN")?;
        }
        // Unset keeps the default; an explicitly empty value disables the HTTP listener.
        if let Some(v) = lookup("KRAKEN_HUB_HTTP_LISTEN") {
            c.http_listen = if v.is_empty() {
                None
            } else {
                Some(v.parse().context("KRAKEN_HUB_HTTP_LISTEN")?)
            };
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
        if let Some(v) = var("KRAKEN_HUB_HEARTBEAT_INTERVAL") {
            c.heartbeat_interval = parse_duration(&v).context("KRAKEN_HUB_HEARTBEAT_INTERVAL")?;
        }
        if let Some(v) = var("KRAKEN_HUB_HEARTBEAT_TIMEOUT") {
            c.heartbeat_timeout = parse_duration(&v).context("KRAKEN_HUB_HEARTBEAT_TIMEOUT")?;
        }
        if let Some(v) = var("KRAKEN_HUB_LEGACY_HEARTBEAT_TIMEOUT") {
            c.legacy_heartbeat_timeout =
                parse_duration(&v).context("KRAKEN_HUB_LEGACY_HEARTBEAT_TIMEOUT")?;
        }
        if let Some(v) = var("KRAKEN_HUB_TIMEOUT_OVERRIDES") {
            c.timeout_overrides = parse_overrides(&v).context("KRAKEN_HUB_TIMEOUT_OVERRIDES")?;
        }
        if let Some(v) = var("KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE") {
            c.queue_capacity_per_instance = v
                .trim()
                .parse()
                .context("KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE")?;
        }
        if let Some(v) = var("KRAKEN_HUB_QUEUE_BYTES_PER_INSTANCE") {
            c.queue_bytes_per_instance =
                parse_size(&v).context("KRAKEN_HUB_QUEUE_BYTES_PER_INSTANCE")?;
        }
        if let Some(v) = var("KRAKEN_HUB_QUEUE_BYTES_TOTAL") {
            c.queue_bytes_total = parse_size(&v).context("KRAKEN_HUB_QUEUE_BYTES_TOTAL")?;
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
        if let Some(v) = var("KRAKEN_HUB_DASHBOARD_ENABLED") {
            c.dashboard_enabled = parse_bool(&v).context("KRAKEN_HUB_DASHBOARD_ENABLED")?;
        }
        if let Some(v) = var("KRAKEN_HUB_GRAFANA_URL") {
            c.grafana_url = Some(v.trim().to_string()).filter(|s| !s.is_empty());
        }
        if let Some(v) = var("KRAKEN_HUB_EDGE_STATS_MAX_EDGES") {
            c.edge_stats_max_edges = v
                .trim()
                .parse()
                .context("KRAKEN_HUB_EDGE_STATS_MAX_EDGES")?;
        }
        if let Some(v) = var("KRAKEN_HUB_EDGE_STATS_WINDOW") {
            c.edge_stats_window = parse_duration(&v).context("KRAKEN_HUB_EDGE_STATS_WINDOW")?;
        }
        c.validate()?;
        Ok(c)
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.hub_id.is_empty() {
            bail!("KRAKEN_HUB_ID must not be empty");
        }
        if self.queue_capacity_per_instance == 0 {
            bail!("KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE must be positive");
        }
        if self.queue_bytes_per_instance == 0 {
            bail!("KRAKEN_HUB_QUEUE_BYTES_PER_INSTANCE must be positive");
        }
        if self.queue_bytes_total == 0 {
            bail!("KRAKEN_HUB_QUEUE_BYTES_TOTAL must be positive");
        }
        if self.heartbeat_interval.is_zero() {
            bail!("KRAKEN_HUB_HEARTBEAT_INTERVAL must be positive");
        }
        if self.edge_stats_max_edges == 0 {
            bail!("KRAKEN_HUB_EDGE_STATS_MAX_EDGES must be positive");
        }
        if self.edge_stats_window < MIN_EDGE_STATS_WINDOW {
            bail!(
                "KRAKEN_HUB_EDGE_STATS_WINDOW ({:?}) must be at least {MIN_EDGE_STATS_WINDOW:?}",
                self.edge_stats_window
            );
        }
        if let Some(url) = &self.grafana_url {
            check_link_url(url).context("KRAKEN_HUB_GRAFANA_URL")?;
        }
        // At least two announced heartbeats must fit in the timeout, or one late heartbeat evicts.
        if self.heartbeat_interval * 2 > self.heartbeat_timeout {
            bail!(
                "KRAKEN_HUB_HEARTBEAT_TIMEOUT ({:?}) must be at least twice KRAKEN_HUB_HEARTBEAT_INTERVAL ({:?})",
                self.heartbeat_timeout,
                self.heartbeat_interval
            );
        }
        Ok(())
    }

    /// Settings that are valid but probably wrong; `main` logs them once telemetry is up.
    pub fn warnings(&self) -> Vec<String> {
        let mut out = Vec::new();
        if self.legacy_heartbeat_timeout < LEGACY_TIMEOUT_WARN_BELOW {
            out.push(format!(
                "KRAKEN_HUB_LEGACY_HEARTBEAT_TIMEOUT is {:?}: v1 clients heartbeat every 10 s, so below {:?} one late heartbeat evicts them",
                self.legacy_heartbeat_timeout, LEGACY_TIMEOUT_WARN_BELOW
            ));
        }
        out
    }

    /// The deadline the hub applies to a request of `type_name` that asked for `requested_ms`
    /// (`Request.timeout_ms`): `> 0` gives `min(requested, override or max)`, `0` gives
    /// `override or default`. An override therefore also moves the clamp for its type, up or down.
    pub fn effective_timeout(&self, type_name: &str, requested_ms: u32) -> Duration {
        let over = self.timeout_overrides.get(type_name).copied();
        if requested_ms == 0 {
            over.unwrap_or(self.default_timeout)
        } else {
            Duration::from_millis(requested_ms.into()).min(over.unwrap_or(self.max_timeout))
        }
    }

    /// The eviction threshold of a session that declared `protocol_version` in its first
    /// `Register`: v2 sessions are told the interval and get the short timeout, v1 sessions keep
    /// the timeout their 10 s heartbeats were designed for.
    pub fn heartbeat_timeout_for(&self, protocol_version: u32) -> Duration {
        if protocol::is_v2(protocol_version) {
            self.heartbeat_timeout
        } else {
            self.legacy_heartbeat_timeout
        }
    }
}

/// v1 clients heartbeat every 10 s; a legacy timeout below two of their intervals is suspicious.
const LEGACY_TIMEOUT_WARN_BELOW: Duration = Duration::from_secs(20);

/// Ten slots of at least a second each.
const MIN_EDGE_STATS_WINDOW: Duration = Duration::from_secs(10);

/// A URL the dashboard may put in a link: `http://` or `https://` with a host, no whitespace or
/// control characters (so it can never be a `javascript:` link or break out of an attribute).
fn check_link_url(url: &str) -> anyhow::Result<()> {
    let lower = url.to_ascii_lowercase();
    let rest = ["http://", "https://"]
        .iter()
        .find_map(|scheme| lower.strip_prefix(scheme))
        .with_context(|| format!("{url:?} must start with http:// or https://"))?;
    if rest.is_empty() || rest.starts_with('/') {
        bail!("{url:?} has no host");
    }
    if url.chars().any(|c| c.is_whitespace() || c.is_control()) {
        bail!("{url:?} contains whitespace or control characters");
    }
    Ok(())
}

/// `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`, any case.
pub fn parse_bool(s: &str) -> anyhow::Result<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => bail!("{other:?} is not a boolean (true or false)"),
    }
}

/// `HOSTNAME` (set by Docker and Kubernetes), else `/etc/hostname`.
fn hostname() -> Option<String> {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::fs::read_to_string("/etc/hostname").ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
}

/// `<host>-<8 hex digits>`; the suffix comes from the process's random hasher keys and the clock,
/// so two hubs on one host (or one host restarting) never share an id.
fn default_hub_id(host: Option<&str>) -> String {
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u128(
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos()),
    );
    hasher.write_u32(std::process::id());
    let suffix = hasher.finish() as u32;
    format!("{}-{suffix:08x}", host.unwrap_or("kraken-hub"))
}

/// `type=ms,type=ms`; blanks around entries are ignored, a trailing comma is allowed. Every value
/// is a positive number of milliseconds that fits `RegisterAck.type_timeouts_ms` (u32).
fn parse_overrides(s: &str) -> anyhow::Result<BTreeMap<String, Duration>> {
    let mut out = BTreeMap::new();
    for entry in s.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let Some((type_name, ms)) = entry.split_once('=') else {
            bail!("entry {entry:?} is not type=ms");
        };
        let type_name = type_name.trim();
        if type_name.is_empty() {
            bail!("entry {entry:?} has no type name");
        }
        let ms: u32 = ms
            .trim()
            .parse()
            .with_context(|| format!("entry {entry:?}: milliseconds must be a u32"))?;
        if ms == 0 {
            bail!("entry {entry:?}: timeout must be positive");
        }
        if out
            .insert(type_name.to_string(), Duration::from_millis(ms.into()))
            .is_some()
        {
            bail!("type {type_name} listed twice");
        }
    }
    Ok(out)
}

fn millis(s: &str) -> anyhow::Result<Duration> {
    Ok(Duration::from_millis(s.trim().parse()?))
}

/// Parses a byte size: a plain number of bytes, or a number with `B`, `KiB`, `MiB` or `GiB`.
pub fn parse_size(s: &str) -> anyhow::Result<usize> {
    let s = s.trim();
    let (digits, unit) = [
        ("GiB", 1usize << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("B", 1),
    ]
    .into_iter()
    .find_map(|(suffix, unit)| s.strip_suffix(suffix).map(|d| (d.trim(), unit)))
    .unwrap_or((s, 1));
    let n: usize = digits.parse().with_context(|| {
        format!("{s:?} is not a size (bytes, or a number with B, KiB, MiB, GiB)")
    })?;
    n.checked_mul(unit)
        .with_context(|| format!("{s:?} is too large"))
}

/// Parses `"30"` (seconds), `"30s"`, `"500ms"` or `"5m"`.
pub fn parse_duration(s: &str) -> anyhow::Result<Duration> {
    let s = s.trim();
    if let Some(ms) = s.strip_suffix("ms") {
        return Ok(Duration::from_millis(ms.trim().parse()?));
    }
    if let Some(m) = s.strip_suffix('m') {
        let minutes: u64 = m.trim().parse()?;
        return minutes
            .checked_mul(60)
            .map(Duration::from_secs)
            .with_context(|| format!("{s:?} is too large"));
    }
    let secs = s.strip_suffix('s').unwrap_or(s);
    Ok(Duration::from_secs(secs.trim().parse()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_env(vars: &[(&str, &str)]) -> anyhow::Result<Config> {
        let vars: Vec<(String, String)> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|n| vars.iter().find(|(k, _)| k == n).map(|(_, v)| v.clone()))
    }

    #[test]
    fn defaults_match_spec() {
        let c = Config::default();
        assert_eq!(c.listen, "0.0.0.0:7000".parse().unwrap());
        assert_eq!(c.otlp_endpoint.as_deref(), Some("http://otel-lgtm:4317"));
        assert_eq!(c.default_timeout, Duration::from_millis(5000));
        assert_eq!(c.max_timeout, Duration::from_millis(60_000));
        assert_eq!(c.heartbeat_interval, Duration::from_secs(3));
        assert_eq!(c.heartbeat_timeout, Duration::from_secs(10));
        assert_eq!(c.legacy_heartbeat_timeout, Duration::from_secs(30));
        assert!(c.timeout_overrides.is_empty());
        assert_eq!(c.owner_grace, Duration::from_secs(60));
        assert_eq!(c.session_buffer, 1024);
        assert_eq!(c.handshake_timeout, Duration::from_secs(10));
        assert_eq!(c.log, "info");
    }

    #[test]
    fn default_hub_id_is_hostname_plus_random_suffix() {
        let a = default_hub_id(Some("hub-host"));
        let b = default_hub_id(Some("hub-host"));
        let (host, suffix) = a.rsplit_once('-').unwrap();
        assert_eq!(host, "hub-host");
        assert_eq!(suffix.len(), 8);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()), "{a}");
        assert_ne!(a, b, "two hub processes must not share an id");
        assert!(default_hub_id(None).starts_with("kraken-hub-"));
    }

    #[test]
    fn hub_id_and_heartbeat_settings_are_read_from_env() {
        let c = with_env(&[
            ("KRAKEN_HUB_ID", "hub-1"),
            ("KRAKEN_HUB_HEARTBEAT_INTERVAL", "500ms"),
            ("KRAKEN_HUB_HEARTBEAT_TIMEOUT", "2s"),
            ("KRAKEN_HUB_LEGACY_HEARTBEAT_TIMEOUT", "45"),
        ])
        .unwrap();
        assert_eq!(c.hub_id, "hub-1");
        assert_eq!(c.heartbeat_interval, Duration::from_millis(500));
        assert_eq!(c.heartbeat_timeout, Duration::from_secs(2));
        assert_eq!(c.legacy_heartbeat_timeout, Duration::from_secs(45));
    }

    #[test]
    fn heartbeat_timeout_must_allow_at_least_two_intervals() {
        assert!(with_env(&[("KRAKEN_HUB_HEARTBEAT_INTERVAL", "10s")]).is_err());
        assert!(with_env(&[("KRAKEN_HUB_HEARTBEAT_INTERVAL", "0")]).is_err());
        // 6 s interval, 10 s timeout: a single late heartbeat would evict.
        let err = with_env(&[("KRAKEN_HUB_HEARTBEAT_INTERVAL", "6s")]).unwrap_err();
        assert!(err.to_string().contains("at least twice"), "{err:#}");
        assert!(with_env(&[("KRAKEN_HUB_HEARTBEAT_INTERVAL", "5s")]).is_ok());
    }

    #[test]
    fn short_legacy_timeout_is_a_warning_not_an_error() {
        assert!(Config::default().warnings().is_empty());
        let c = with_env(&[("KRAKEN_HUB_LEGACY_HEARTBEAT_TIMEOUT", "15s")]).unwrap();
        let warnings = c.warnings();
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("KRAKEN_HUB_LEGACY_HEARTBEAT_TIMEOUT"),
            "{warnings:?}"
        );
        assert!(
            with_env(&[("KRAKEN_HUB_LEGACY_HEARTBEAT_TIMEOUT", "20s")])
                .unwrap()
                .warnings()
                .is_empty()
        );
    }

    #[test]
    fn timeout_overrides_parse_type_equals_millis_lists() {
        let c = with_env(&[(
            "KRAKEN_HUB_TIMEOUT_OVERRIDES",
            " files.Upload=120000 , gym.Report=500,",
        )])
        .unwrap();
        assert_eq!(
            c.timeout_overrides,
            BTreeMap::from([
                ("files.Upload".to_string(), Duration::from_millis(120_000)),
                ("gym.Report".to_string(), Duration::from_millis(500)),
            ])
        );
        for bad in [
            "files.Upload",
            "=5",
            "a=b",
            "a=0",
            "a=1,a=2",
            "a=5000000000",
        ] {
            assert!(
                with_env(&[("KRAKEN_HUB_TIMEOUT_OVERRIDES", bad)]).is_err(),
                "{bad} accepted"
            );
        }
    }

    #[test]
    fn queue_caps_default_and_parse_sizes() {
        let c = Config::default();
        assert_eq!(c.queue_capacity_per_instance, 10_000);
        assert_eq!(c.queue_bytes_per_instance, 64 * 1024 * 1024);
        let c = with_env(&[
            ("KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE", "3"),
            ("KRAKEN_HUB_QUEUE_BYTES_PER_INSTANCE", "512KiB"),
        ])
        .unwrap();
        assert_eq!(
            (c.queue_capacity_per_instance, c.queue_bytes_per_instance),
            (3, 512 * 1024)
        );
        for (v, want) in [
            ("1048576", 1 << 20),
            ("2MiB", 2 << 20),
            ("1GiB", 1 << 30),
            ("10B", 10),
        ] {
            assert_eq!(parse_size(v).unwrap(), want, "{v}");
        }
        for bad in ["", "MiB", "1.5MiB", "-1", "1TB"] {
            assert!(parse_size(bad).is_err(), "{bad} accepted");
        }
        assert!(with_env(&[("KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE", "0")]).is_err());
        assert!(with_env(&[("KRAKEN_HUB_QUEUE_BYTES_PER_INSTANCE", "0")]).is_err());
    }

    #[test]
    fn total_queue_bytes_default_parse_and_validation() {
        assert_eq!(Config::default().queue_bytes_total, 512 * 1024 * 1024);
        let c = with_env(&[("KRAKEN_HUB_QUEUE_BYTES_TOTAL", "1GiB")]).unwrap();
        assert_eq!(c.queue_bytes_total, 1 << 30);
        assert!(with_env(&[("KRAKEN_HUB_QUEUE_BYTES_TOTAL", "0")]).is_err());
        assert!(with_env(&[("KRAKEN_HUB_QUEUE_BYTES_TOTAL", "lots")]).is_err());
    }

    /// The effective-timeout table from the design (P0, F4).
    #[test]
    fn effective_timeout_table() {
        let c = Config {
            default_timeout: Duration::from_millis(5_000),
            max_timeout: Duration::from_millis(60_000),
            timeout_overrides: BTreeMap::from([
                ("t.Long".to_string(), Duration::from_millis(120_000)),
                ("t.Short".to_string(), Duration::from_millis(1_000)),
            ]),
            ..Config::default()
        };
        let ms = |d: Duration| d.as_millis() as u64;
        // (type, requested timeout_ms, effective ms)
        let table = [
            ("t.Plain", 0, 5_000),        // 0 -> default
            ("t.Plain", 2_000, 2_000),    // > 0, below the clamp -> as requested
            ("t.Plain", 999_999, 60_000), // > 0 -> clamped at max
            ("t.Long", 0, 120_000),       // 0 -> override instead of default
            ("t.Long", 90_000, 90_000),   // the override raises the clamp for its type
            ("t.Long", 999_999, 120_000), // ... up to the override
            ("t.Short", 0, 1_000),        // 0 -> override
            ("t.Short", 3_000, 1_000),    // min(requested, override)
            ("t.Short", 500, 500),        // shorter requests stay shorter
        ];
        for (type_name, requested, want) in table {
            assert_eq!(
                ms(c.effective_timeout(type_name, requested)),
                want,
                "{type_name} timeout_ms={requested}"
            );
        }
    }

    /// Per-session heartbeat timeout: v2 sessions get the short timeout, v1 (version 0) and the
    /// never-defined version 1 keep the legacy one.
    #[test]
    fn heartbeat_timeout_is_chosen_per_protocol_version() {
        let c = Config::default();
        assert_eq!(c.heartbeat_timeout_for(0), Duration::from_secs(30));
        assert_eq!(c.heartbeat_timeout_for(1), Duration::from_secs(30));
        assert_eq!(c.heartbeat_timeout_for(2), Duration::from_secs(10));
        assert_eq!(c.heartbeat_timeout_for(7), Duration::from_secs(10));
    }

    #[test]
    fn parse_duration_accepts_seconds_suffix_and_millis() {
        assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("m").is_err());
    }

    #[test]
    fn dashboard_settings_default_and_parse() {
        let c = Config::default();
        assert!(c.dashboard_enabled);
        assert_eq!(c.grafana_url, None);
        assert_eq!(c.edge_stats_max_edges, 10_000);
        assert_eq!(c.edge_stats_window, Duration::from_secs(300));
        let c = with_env(&[
            ("KRAKEN_HUB_DASHBOARD_ENABLED", "false"),
            ("KRAKEN_HUB_GRAFANA_URL", " https://grafana.example/d/hub "),
            ("KRAKEN_HUB_EDGE_STATS_MAX_EDGES", "500"),
            ("KRAKEN_HUB_EDGE_STATS_WINDOW", "10m"),
        ])
        .unwrap();
        assert!(!c.dashboard_enabled);
        assert_eq!(
            c.grafana_url.as_deref(),
            Some("https://grafana.example/d/hub")
        );
        assert_eq!(c.edge_stats_max_edges, 500);
        assert_eq!(c.edge_stats_window, Duration::from_secs(600));
        for v in ["TRUE", "1", "yes", "On"] {
            assert!(parse_bool(v).unwrap(), "{v}");
        }
        for v in ["False", "0", "no", "off"] {
            assert!(!parse_bool(v).unwrap(), "{v}");
        }
        assert!(with_env(&[("KRAKEN_HUB_DASHBOARD_ENABLED", "maybe")]).is_err());
        assert!(with_env(&[("KRAKEN_HUB_EDGE_STATS_MAX_EDGES", "0")]).is_err());
        assert!(with_env(&[("KRAKEN_HUB_EDGE_STATS_WINDOW", "9s")]).is_err());
        assert!(with_env(&[("KRAKEN_HUB_EDGE_STATS_WINDOW", "10s")]).is_ok());
    }

    #[test]
    fn grafana_url_must_be_a_plain_http_link() {
        for good in [
            "http://localhost:3000",
            "HTTPS://grafana.example/d/x?orgId=1",
        ] {
            assert!(
                with_env(&[("KRAKEN_HUB_GRAFANA_URL", good)]).is_ok(),
                "{good} rejected"
            );
        }
        for bad in [
            "javascript:alert(1)",
            "grafana.example",
            "https://",
            "http:///path",
            "https://a b",
            "ftp://grafana",
        ] {
            assert!(
                with_env(&[("KRAKEN_HUB_GRAFANA_URL", bad)]).is_err(),
                "{bad} accepted"
            );
        }
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

    #[test]
    fn http_listen_defaults_to_7001_and_empty_disables() {
        let with = |value: Option<&str>| {
            Config::from_lookup(|n| {
                (n == "KRAKEN_HUB_HTTP_LISTEN")
                    .then(|| value.map(str::to_string))
                    .flatten()
            })
        };
        let default = Some("0.0.0.0:7001".parse().unwrap());
        assert_eq!(Config::default().http_listen, default);
        assert_eq!(with(None).unwrap().http_listen, default);
        assert_eq!(with(Some("")).unwrap().http_listen, None);
        assert_eq!(
            with(Some("127.0.0.1:9")).unwrap().http_listen,
            Some("127.0.0.1:9".parse().unwrap())
        );
        assert!(with(Some("nonsense")).is_err());
    }
}
