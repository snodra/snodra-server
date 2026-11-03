//! Environment-based configuration. Every variable is prefixed `KRAKEN_HUB_`.

use std::collections::BTreeMap;
use std::hash::{BuildHasher, Hasher};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, bail};

use crate::auth::{AuthMode, Secret};
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
    /// A streaming query (ADR 0017, P4b) open for longer than this ends with
    /// `ReplyEnd{error = CANCELLED}` and a `Cancel` to its handler, however active it is.
    /// `KRAKEN_HUB_MAX_STREAM_DURATION`, default 10 minutes, at least 1 s.
    pub max_stream_duration: Duration,
    /// Updates of a subscription query (ADR 0017, P4c) the hub holds for a sender whose credit
    /// is exhausted; one more ends the subscription with `RESOURCE_EXHAUSTED`.
    /// `KRAKEN_HUB_SUBSCRIPTION_BUFFER`, default 256, positive.
    pub subscription_buffer: usize,
    /// Subscription targets one handler instance serves at once; a subscription that would
    /// exceed it on any of its targets is refused with `RESOURCE_EXHAUSTED`.
    /// `KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE`, default 1024, positive.
    pub max_subscriptions_per_instance: u32,
    /// A subscription open for longer than this ends with `ReplyEnd{error = CANCELLED}` and a
    /// `Cancel` to every target. `KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION`, default `0` (`None`:
    /// unlimited); when set, at least 1 s.
    pub max_subscription_duration: Option<Duration>,
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
    /// The other hubs' dashboards, as their HTTP base URLs reachable from the operator's browser
    /// (ADR 0016). `KRAKEN_HUB_PEERS`, comma-separated, default none. Normalised to origins
    /// (`scheme://host[:port]`, lowercase, default port dropped), deduplicated, at most
    /// [`MAX_PEERS`]. The list may include this hub (the page skips it by hub id). The dashboard
    /// merges the peers in the browser; each peer answers CORS for these origins only.
    pub peers: Vec<String>,
    /// Access control for `Connect` (ADR 0018): `off` (default, no checks), `permissive`
    /// (evaluate, log and count violations, allow everything) or `enforce` (refuse violations).
    /// `KRAKEN_HUB_AUTH_MODE`.
    pub auth_mode: AuthMode,
    /// The ACL (TOML, token SHA-256 -> service, handle and send patterns); required unless the
    /// mode is `off`. Reloaded when it changes. `KRAKEN_HUB_ACL_FILE`.
    pub acl_file: Option<PathBuf>,
    /// PEM certificate chain and private key of the gRPC listener; when set it only accepts TLS.
    /// `KRAKEN_HUB_TLS_CERT` and `KRAKEN_HUB_TLS_KEY`, both or neither. Reloaded when they change.
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    /// The HTTP listener's own pair; default the gRPC pair. `KRAKEN_HUB_HTTP_TLS_CERT` and
    /// `KRAKEN_HUB_HTTP_TLS_KEY`, both or neither.
    pub http_tls_cert: Option<PathBuf>,
    pub http_tls_key: Option<PathBuf>,
    /// When set, `/api/*`, `/topology`, the event stream and the gRPC `Topology` call need
    /// `Authorization: Bearer <token>`. Only its digest is kept. `KRAKEN_HUB_DASHBOARD_TOKEN`,
    /// at least 16 characters. Unrelated to the ACL tokens.
    pub dashboard_token: Option<Secret>,
    /// Reserved for the processor controls of roadmap P6 (a second, stronger token for mutating
    /// calls); parsed and kept, nothing reads it yet. `KRAKEN_HUB_DASHBOARD_WRITE_TOKEN`.
    pub dashboard_write_token: Option<Secret>,
    /// The files the two tokens above are read from; each is read at startup and again on the
    /// reload poll, and wins over the variable when both are set (like the client libraries'
    /// `*_TOKEN_FILE`). `KRAKEN_HUB_DASHBOARD_TOKEN_FILE`,
    /// `KRAKEN_HUB_DASHBOARD_WRITE_TOKEN_FILE`.
    pub dashboard_token_file: Option<PathBuf>,
    pub dashboard_write_token_file: Option<PathBuf>,
    /// How often the ACL and the certificates are checked for changes.
    /// `KRAKEN_HUB_RELOAD_INTERVAL`, default 5 s.
    pub reload_interval: Duration,
    /// A TLS handshake that takes longer is dropped. `KRAKEN_HUB_TLS_HANDSHAKE_TIMEOUT`, default
    /// 5 s, positive.
    pub tls_handshake_timeout: Duration,
    /// TLS handshakes in progress at once from one peer IP; more are dropped at once. `0`
    /// disables the cap. `KRAKEN_HUB_TLS_HANDSHAKES_PER_IP`, default 32.
    pub tls_handshakes_per_ip: usize,
    /// The `iss` a JWT credential must carry, compared as an exact string (ADR 0019). Unset: JWT
    /// credentials are refused as "not configured". `KRAKEN_HUB_JWT_ISSUER`.
    pub jwt_issuer: Option<String>,
    /// Where the issuer's signing keys are fetched from; `None` exactly when [`Config::jwt_issuer`]
    /// is. Default `<issuer>/protocol/openid-connect/certs` (Keycloak). Separate from the issuer
    /// so a hub can fetch from an internal address and validate the public issuer.
    /// `KRAKEN_HUB_JWT_JWKS_URI`.
    pub jwt_jwks_uri: Option<String>,
    /// When set, a JWT's `aud` (a string or an array) must contain it. `KRAKEN_HUB_JWT_AUDIENCE`.
    pub jwt_audience: Option<String>,
    /// Clock skew tolerated on `exp`, `nbf` and `iat`. `KRAKEN_HUB_JWT_LEEWAY`, default 30 s.
    pub jwt_leeway: Duration,
    /// How long after `exp` a session stays open without a refresh. `KRAKEN_HUB_JWT_EXPIRY_GRACE`,
    /// default 30 s.
    pub jwt_expiry_grace: Duration,
    /// How often the signing keys are fetched again. `KRAKEN_HUB_JWKS_REFRESH`, default 5 m.
    pub jwks_refresh: Duration,
    /// An extra CA (PEM) trusted for the JWKS fetch, on top of the platform roots.
    /// `KRAKEN_HUB_JWT_JWKS_CA`.
    pub jwt_jwks_ca: Option<PathBuf>,
    /// At most one key-set fetch per this long is made because a token names an unknown `kid`.
    /// Not an environment variable: 30 s, shortened by tests.
    pub jwks_refetch_interval: Duration,
    /// A key-set fetch that takes longer fails. Not an environment variable: 10 s, shortened by
    /// tests.
    pub jwks_fetch_timeout: Duration,
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
            max_stream_duration: Duration::from_secs(600),
            subscription_buffer: 256,
            max_subscriptions_per_instance: 1024,
            max_subscription_duration: None,
            session_buffer: 1024,
            handshake_timeout: Duration::from_secs(10),
            dashboard_enabled: true,
            grafana_url: None,
            edge_stats_max_edges: 10_000,
            edge_stats_window: Duration::from_secs(300),
            peers: Vec::new(),
            auth_mode: AuthMode::Off,
            acl_file: None,
            tls_cert: None,
            tls_key: None,
            http_tls_cert: None,
            http_tls_key: None,
            dashboard_token: None,
            dashboard_write_token: None,
            dashboard_token_file: None,
            dashboard_write_token_file: None,
            reload_interval: Duration::from_secs(5),
            tls_handshake_timeout: Duration::from_secs(5),
            tls_handshakes_per_ip: 32,
            jwt_issuer: None,
            jwt_jwks_uri: None,
            jwt_audience: None,
            jwt_leeway: Duration::from_secs(30),
            jwt_expiry_grace: Duration::from_secs(30),
            jwks_refresh: Duration::from_secs(300),
            jwt_jwks_ca: None,
            jwks_refetch_interval: Duration::from_secs(30),
            jwks_fetch_timeout: Duration::from_secs(10),
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
        if let Some(v) = var("KRAKEN_HUB_MAX_STREAM_DURATION") {
            c.max_stream_duration = parse_duration(&v).context("KRAKEN_HUB_MAX_STREAM_DURATION")?;
        }
        if let Some(v) = var("KRAKEN_HUB_SUBSCRIPTION_BUFFER") {
            c.subscription_buffer = v.trim().parse().context("KRAKEN_HUB_SUBSCRIPTION_BUFFER")?;
        }
        if let Some(v) = var("KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE") {
            c.max_subscriptions_per_instance = v
                .trim()
                .parse()
                .context("KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE")?;
        }
        if let Some(v) = var("KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION") {
            c.max_subscription_duration =
                Some(parse_duration(&v).context("KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION")?)
                    .filter(|d| !d.is_zero());
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
        if let Some(v) = var("KRAKEN_HUB_PEERS") {
            c.peers = parse_peers(&v).context("KRAKEN_HUB_PEERS")?;
        }
        if let Some(v) = var("KRAKEN_HUB_AUTH_MODE") {
            c.auth_mode = v.parse().context("KRAKEN_HUB_AUTH_MODE")?;
        }
        c.acl_file = path_var(&var, "KRAKEN_HUB_ACL_FILE");
        c.tls_cert = path_var(&var, "KRAKEN_HUB_TLS_CERT");
        c.tls_key = path_var(&var, "KRAKEN_HUB_TLS_KEY");
        c.http_tls_cert = path_var(&var, "KRAKEN_HUB_HTTP_TLS_CERT");
        c.http_tls_key = path_var(&var, "KRAKEN_HUB_HTTP_TLS_KEY");
        // Secrets are trimmed (a mounted secret file ends with a newline) and kept as digests.
        let secret = |name: &str| {
            var(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        c.dashboard_token = secret("KRAKEN_HUB_DASHBOARD_TOKEN")
            .map(|t| {
                check_dashboard_token("KRAKEN_HUB_DASHBOARD_TOKEN", &t).map(|()| Secret::new(&t))
            })
            .transpose()?;
        c.dashboard_write_token = secret("KRAKEN_HUB_DASHBOARD_WRITE_TOKEN")
            .map(|t| {
                check_dashboard_token("KRAKEN_HUB_DASHBOARD_WRITE_TOKEN", &t)
                    .map(|()| Secret::new(&t))
            })
            .transpose()?;
        c.dashboard_token_file = path_var(&var, "KRAKEN_HUB_DASHBOARD_TOKEN_FILE");
        c.dashboard_write_token_file = path_var(&var, "KRAKEN_HUB_DASHBOARD_WRITE_TOKEN_FILE");
        c.load_token_files()?;
        if let Some(v) = var("KRAKEN_HUB_RELOAD_INTERVAL") {
            c.reload_interval = parse_duration(&v).context("KRAKEN_HUB_RELOAD_INTERVAL")?;
        }
        if let Some(v) = var("KRAKEN_HUB_TLS_HANDSHAKE_TIMEOUT") {
            c.tls_handshake_timeout =
                parse_duration(&v).context("KRAKEN_HUB_TLS_HANDSHAKE_TIMEOUT")?;
        }
        if let Some(v) = var("KRAKEN_HUB_TLS_HANDSHAKES_PER_IP") {
            c.tls_handshakes_per_ip = v
                .trim()
                .parse()
                .context("KRAKEN_HUB_TLS_HANDSHAKES_PER_IP")?;
        }
        c.jwt_issuer = secret("KRAKEN_HUB_JWT_ISSUER");
        let explicit_uri = secret("KRAKEN_HUB_JWT_JWKS_URI");
        c.jwt_audience = secret("KRAKEN_HUB_JWT_AUDIENCE");
        c.jwt_jwks_ca = path_var(&var, "KRAKEN_HUB_JWT_JWKS_CA");
        if let Some(v) = var("KRAKEN_HUB_JWT_LEEWAY") {
            c.jwt_leeway = parse_duration(&v).context("KRAKEN_HUB_JWT_LEEWAY")?;
        }
        if let Some(v) = var("KRAKEN_HUB_JWT_EXPIRY_GRACE") {
            c.jwt_expiry_grace = parse_duration(&v).context("KRAKEN_HUB_JWT_EXPIRY_GRACE")?;
        }
        if let Some(v) = var("KRAKEN_HUB_JWKS_REFRESH") {
            c.jwks_refresh = parse_duration(&v).context("KRAKEN_HUB_JWKS_REFRESH")?;
        }
        c.resolve_jwt(explicit_uri)?;
        c.validate()?;
        Ok(c)
    }

    /// Settles the JWKS URI: the explicit one, else Keycloak's path under the issuer. The JWT
    /// settings that only mean something with an issuer are an error without one, so a hub whose
    /// operator thinks JWT is on does not quietly refuse every JWT as "not configured".
    fn resolve_jwt(&mut self, explicit_uri: Option<String>) -> anyhow::Result<()> {
        let Some(issuer) = &self.jwt_issuer else {
            for (name, set) in [
                ("KRAKEN_HUB_JWT_JWKS_URI", explicit_uri.is_some()),
                ("KRAKEN_HUB_JWT_AUDIENCE", self.jwt_audience.is_some()),
                ("KRAKEN_HUB_JWT_JWKS_CA", self.jwt_jwks_ca.is_some()),
            ] {
                if set {
                    bail!(
                        "{name} needs KRAKEN_HUB_JWT_ISSUER (without an issuer JWT credentials are off)"
                    );
                }
            }
            return Ok(());
        };
        let uri = match explicit_uri {
            Some(uri) => uri,
            None => {
                if jwks_uri_host(issuer).is_err() {
                    bail!(
                        "KRAKEN_HUB_JWT_ISSUER is not an http(s) URL, so the JWKS URI cannot be derived from it; set KRAKEN_HUB_JWT_JWKS_URI"
                    );
                }
                format!(
                    "{}/protocol/openid-connect/certs",
                    issuer.trim_end_matches('/')
                )
            }
        };
        self.jwt_jwks_uri = Some(uri);
        Ok(())
    }

    /// Reads the token files into the tokens (the file wins over the variable). A file that
    /// cannot be read, or holds an invalid token, is an error that never echoes its content.
    pub fn load_token_files(&mut self) -> anyhow::Result<()> {
        for (var, file, slot) in [
            (
                "KRAKEN_HUB_DASHBOARD_TOKEN_FILE",
                &self.dashboard_token_file,
                &mut self.dashboard_token,
            ),
            (
                "KRAKEN_HUB_DASHBOARD_WRITE_TOKEN_FILE",
                &self.dashboard_write_token_file,
                &mut self.dashboard_write_token,
            ),
        ] {
            if let Some(file) = file {
                *slot = Some(Secret::new(&read_token_file(var, file)?));
            }
        }
        Ok(())
    }

    /// The token files, for the reload watcher.
    pub fn token_files(&self) -> Vec<PathBuf> {
        [&self.dashboard_token_file, &self.dashboard_write_token_file]
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }

    /// Re-reads the token files into the live tokens. A file that cannot be read or holds an
    /// invalid token keeps the previous token (an error is logged, never the content). Returns
    /// whether any token changed.
    pub fn reload_token_files(&self) -> bool {
        let mut changed = false;
        for (var, file, slot) in [
            (
                "KRAKEN_HUB_DASHBOARD_TOKEN_FILE",
                &self.dashboard_token_file,
                &self.dashboard_token,
            ),
            (
                "KRAKEN_HUB_DASHBOARD_WRITE_TOKEN_FILE",
                &self.dashboard_write_token_file,
                &self.dashboard_write_token,
            ),
        ] {
            let (Some(file), Some(secret)) = (file, slot) else {
                continue;
            };
            match read_token_file(var, file) {
                Ok(token) => {
                    if secret.set(&token) {
                        tracing::info!(file = %file.display(), "{var} reloaded");
                        changed = true;
                    }
                }
                Err(e) => {
                    tracing::error!(error = %format!("{e:#}"), "{var} reload failed; keeping the previous token");
                }
            }
        }
        changed
    }

    /// The bounds of the TLS handshakes of each listener.
    pub fn handshake_limits(&self) -> crate::tls::HandshakeLimits {
        crate::tls::HandshakeLimits {
            timeout: self.tls_handshake_timeout,
            per_ip: self.tls_handshakes_per_ip,
            ..Default::default()
        }
    }

    /// The gRPC listener's certificate and key, when it serves TLS.
    pub fn grpc_tls(&self) -> Option<(&Path, &Path)> {
        self.tls_cert.as_deref().zip(self.tls_key.as_deref())
    }

    /// The HTTP listener's certificate and key: its own pair, else the gRPC pair.
    pub fn http_tls(&self) -> Option<(&Path, &Path)> {
        self.http_tls_cert
            .as_deref()
            .zip(self.http_tls_key.as_deref())
            .or_else(|| self.grpc_tls())
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
        if self.tls_handshake_timeout.is_zero() {
            bail!("KRAKEN_HUB_TLS_HANDSHAKE_TIMEOUT must be positive");
        }
        if self.reload_interval.is_zero() {
            bail!("KRAKEN_HUB_RELOAD_INTERVAL must be positive");
        }
        if self.auth_mode != AuthMode::Off && self.acl_file.is_none() {
            bail!(
                "KRAKEN_HUB_AUTH_MODE={} needs KRAKEN_HUB_ACL_FILE",
                self.auth_mode.as_str()
            );
        }
        if self.jwks_refresh.is_zero() {
            bail!("KRAKEN_HUB_JWKS_REFRESH must be positive");
        }
        if let Some(uri) = &self.jwt_jwks_uri {
            jwks_uri_host(uri).context("KRAKEN_HUB_JWT_JWKS_URI")?;
        }
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            bail!("KRAKEN_HUB_TLS_CERT and KRAKEN_HUB_TLS_KEY must be set together");
        }
        if self.http_tls_cert.is_some() != self.http_tls_key.is_some() {
            bail!("KRAKEN_HUB_HTTP_TLS_CERT and KRAKEN_HUB_HTTP_TLS_KEY must be set together");
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
        if self.max_stream_duration < MIN_MAX_STREAM_DURATION {
            bail!(
                "KRAKEN_HUB_MAX_STREAM_DURATION ({:?}) must be at least {MIN_MAX_STREAM_DURATION:?}",
                self.max_stream_duration
            );
        }
        if self.subscription_buffer == 0 {
            bail!("KRAKEN_HUB_SUBSCRIPTION_BUFFER must be positive");
        }
        if self.max_subscriptions_per_instance == 0 {
            bail!("KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE must be positive");
        }
        if let Some(d) = self.max_subscription_duration
            && d < MIN_MAX_STREAM_DURATION
        {
            bail!(
                "KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION ({d:?}) must be 0 (unlimited) or at least {MIN_MAX_STREAM_DURATION:?}"
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

    /// Security posture worth shouting about, logged at startup. Every line is a `warn`.
    pub fn security_warnings(&self) -> Vec<String> {
        let mut out = Vec::new();
        let open = |a: SocketAddr| !a.ip().is_loopback();
        if self.auth_mode != AuthMode::Enforce && open(self.listen) {
            out.push(format!(
                "KRAKEN_HUB_AUTH_MODE={} while the gRPC port listens on {}: any peer that can reach it may register as any service and send any type; set KRAKEN_HUB_AUTH_MODE=enforce (ADR 0018) or keep the port on a private network",
                self.auth_mode.as_str(),
                self.listen
            ));
        }
        if self.auth_mode != AuthMode::Off && self.grpc_tls().is_none() && open(self.listen) {
            out.push(
                "access control is on but the gRPC port has no TLS (KRAKEN_HUB_TLS_CERT): tokens travel in clear text"
                    .to_string(),
            );
        }
        // Enforce protects the gRPC topology with ACL tokens, but the HTTP one only with the
        // dashboard token: without it, enforce leaves the same data open on the HTTP port.
        let enforce_open_http = self.auth_mode == AuthMode::Enforce
            && self.dashboard_token.is_none()
            && self.http_listen.is_some();
        if let (true, Some(http)) = (enforce_open_http, self.http_listen) {
            out.push(format!(
                "access control is enforced but KRAKEN_HUB_DASHBOARD_TOKEN is not set: GET /topology and the dashboard API on {http} are open to any peer that can reach it (only the gRPC Topology needs a token); set KRAKEN_HUB_DASHBOARD_TOKEN or KRAKEN_HUB_DASHBOARD_TOKEN_FILE"
            ));
        }
        if let Some(http) = self.http_listen
            && open(http)
        {
            if self.dashboard_token.is_none() {
                if !enforce_open_http {
                    out.push(format!(
                        "the HTTP port listens on {http} without KRAKEN_HUB_DASHBOARD_TOKEN: /topology and the dashboard API are open to any peer"
                    ));
                }
            } else if self.http_tls().is_none() {
                out.push("the dashboard token is set but the HTTP port has no TLS: the token travels in clear text".to_string());
            }
        }
        if self.jwt_issuer.is_some() && self.auth_mode == AuthMode::Off {
            out.push(
                "KRAKEN_HUB_JWT_ISSUER is set but KRAKEN_HUB_AUTH_MODE=off: no credentials are checked, JWT or token; set KRAKEN_HUB_AUTH_MODE=permissive or enforce (and the [[service]] tables of KRAKEN_HUB_ACL_FILE)"
                    .to_string(),
            );
        }
        // The key set decides who may register as what: over plain HTTP anyone on the path can
        // swap it. Loopback and a single-label name (a compose service) stay on the host or the
        // private network, so they are not worth a line.
        if let Some(uri) = &self.jwt_jwks_uri
            && let Ok((false, host)) = jwks_uri_host(uri)
            && !(host.eq_ignore_ascii_case("localhost")
                || host
                    .trim_start_matches('[')
                    .trim_end_matches(']')
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
                || !host.contains(['.', ':']))
        {
            out.push(format!(
                "KRAKEN_HUB_JWT_JWKS_URI uses plain http to {host}: whoever can alter that traffic can mint tokens the hub accepts; use https (KRAKEN_HUB_JWT_JWKS_CA adds a private CA)"
            ));
        }
        if self.dashboard_write_token.is_some() {
            out.push("KRAKEN_HUB_DASHBOARD_WRITE_TOKEN is reserved for the processor controls of roadmap P6 and has no effect yet".to_string());
        }
        out
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

/// Below a second a streaming query (or a subscription) could never deliver anything.
const MIN_MAX_STREAM_DURATION: Duration = Duration::from_secs(1);

/// A URL the dashboard may put in a link: `http://` or `https://` with a host, no user info, no
/// whitespace or control characters (so it can never be a `javascript:` link or break out of an attribute).
fn check_link_url(url: &str) -> anyhow::Result<()> {
    // Checked first, and without echoing the URL: the value may hold a password. The link is
    // served unauthenticated (`/api/v1/version`), so credentials do not belong in it.
    if let Some((_, rest)) = url.split_once("://") {
        let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
        if rest[..authority_end].contains('@') {
            bail!(
                "the URL carries user info (user:password@); the dashboard serves this link \
                 unauthenticated, so put no credentials in it (Grafana asks for its own login)"
            );
        }
    }
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

/// A path variable: unset or empty is `None`.
/// `(is_https, host)` of an http(s) URL with no user info. The error never echoes the URL (it
/// may hold credentials).
fn jwks_uri_host(url: &str) -> anyhow::Result<(bool, String)> {
    let uri: tonic::codegen::http::Uri = url
        .parse()
        .map_err(|_| anyhow::anyhow!("not a valid URL"))?;
    let https = match uri.scheme_str() {
        Some("https") => true,
        Some("http") => false,
        _ => bail!("must be an http:// or https:// URL"),
    };
    let authority = uri.authority().map(|a| a.as_str()).unwrap_or_default();
    if authority.contains('@') {
        bail!("must not carry user info");
    }
    match uri.host() {
        Some(host) if !host.is_empty() => Ok((https, host.to_string())),
        _ => bail!("has no host"),
    }
}

fn path_var(var: &impl Fn(&str) -> Option<String>, name: &str) -> Option<PathBuf> {
    var(name)
        .map(|v| PathBuf::from(v.trim()))
        .filter(|p| !p.as_os_str().is_empty())
}

/// The token in `file`, trimmed (a mounted secret ends with a newline) and checked like the
/// variable. The error names the variable and the file, never the content.
fn read_token_file(var: &str, file: &Path) -> anyhow::Result<String> {
    let text =
        std::fs::read_to_string(file).with_context(|| format!("{var}: read {}", file.display()))?;
    let token = text.trim().to_string();
    if token.is_empty() {
        bail!("{var}: {} is empty", file.display());
    }
    check_dashboard_token(var, &token).with_context(|| format!("{}", file.display()))?;
    Ok(token)
}

/// The shortest dashboard token accepted; `openssl rand -hex 32` makes 64.
const MIN_DASHBOARD_TOKEN: usize = 16;

fn check_dashboard_token(name: &str, token: &str) -> anyhow::Result<()> {
    // Never echo the value.
    if token.chars().count() < MIN_DASHBOARD_TOKEN {
        bail!(
            "{name} must be at least {MIN_DASHBOARD_TOKEN} characters (try `openssl rand -hex 32`)"
        );
    }
    if token.chars().any(char::is_whitespace) {
        bail!("{name} must not contain whitespace");
    }
    Ok(())
}

/// Peer hubs a dashboard merges at most (`KRAKEN_HUB_PEERS`).
pub const MAX_PEERS: usize = 16;

/// `KRAKEN_HUB_PEERS`: comma-separated base URLs, each normalised by [`peer_origin`], blanks and
/// duplicates dropped, at most [`MAX_PEERS`].
fn parse_peers(s: &str) -> anyhow::Result<Vec<String>> {
    let mut out: Vec<String> = Vec::new();
    for entry in s.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let origin = peer_origin(entry)?;
        if !out.contains(&origin) {
            out.push(origin);
        }
    }
    if out.len() > MAX_PEERS {
        bail!(
            "{} peers listed; at most {MAX_PEERS} are allowed",
            out.len()
        );
    }
    Ok(out)
}

/// The browser origin of a peer's base URL: `http://` or `https://`, a host (name, IPv4, or
/// `[IPv6]`), an optional port, and no path beyond `/`, no query, fragment or user info. Scheme
/// and host are lowercased and the scheme's default port dropped, as browsers serialise an
/// `Origin` header, so the CORS check can compare exactly.
pub fn peer_origin(url: &str) -> anyhow::Result<String> {
    let Some((scheme, rest)) = url.split_once("://") else {
        bail!("{url:?} must start with http:// or https://");
    };
    let scheme = scheme.to_ascii_lowercase();
    let default_port = match scheme.as_str() {
        "http" => 80,
        "https" => 443,
        _ => bail!("{url:?} must start with http:// or https://"),
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    // Checked before anything echoes the URL: the value may hold a password.
    if authority.contains('@') {
        bail!("a peer URL carries user info (user:password@); peers are plain base URLs");
    }
    if !tail.is_empty() && tail != "/" {
        bail!("{url:?} has a path, query or fragment; give the hub's base URL (http://host:port)");
    }
    let (host, port) = if let Some(after) = authority.strip_prefix('[') {
        let Some((v6, port)) = after.split_once(']') else {
            bail!("{url:?} has an unclosed IPv6 address");
        };
        if v6.is_empty()
            || !v6
                .chars()
                .all(|c| c.is_ascii_hexdigit() || c == ':' || c == '.')
        {
            bail!("{url:?} has an invalid IPv6 address");
        }
        (format!("[{}]", v6.to_ascii_lowercase()), port)
    } else {
        let end = authority.find(':').unwrap_or(authority.len());
        let (host, port) = authority.split_at(end);
        if host.is_empty()
            || !host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_'))
        {
            bail!("{url:?} has no valid host");
        }
        (host.to_ascii_lowercase(), port)
    };
    let port = match port {
        "" => None,
        p => {
            let n: u16 = p
                .strip_prefix(':')
                .and_then(|d| d.parse().ok())
                .filter(|n| *n > 0)
                .with_context(|| format!("{url:?} has an invalid port"))?;
            (n != default_port).then_some(n)
        }
    };
    Ok(match port {
        Some(p) => format!("{scheme}://{host}:{p}"),
        None => format!("{scheme}://{host}"),
    })
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
    fn max_stream_duration_defaults_to_ten_minutes_and_needs_at_least_a_second() {
        assert_eq!(
            Config::default().max_stream_duration,
            Duration::from_secs(600)
        );
        let c = with_env(&[("KRAKEN_HUB_MAX_STREAM_DURATION", "90s")]).unwrap();
        assert_eq!(c.max_stream_duration, Duration::from_secs(90));
        let c = with_env(&[("KRAKEN_HUB_MAX_STREAM_DURATION", "1s")]).unwrap();
        assert_eq!(c.max_stream_duration, Duration::from_secs(1));
        let err = with_env(&[("KRAKEN_HUB_MAX_STREAM_DURATION", "500ms")]).unwrap_err();
        assert!(
            err.to_string().contains("KRAKEN_HUB_MAX_STREAM_DURATION"),
            "{err:#}"
        );
        assert!(with_env(&[("KRAKEN_HUB_MAX_STREAM_DURATION", "soon")]).is_err());
    }

    #[test]
    fn subscription_settings_default_parse_and_validate() {
        let c = Config::default();
        assert_eq!(c.subscription_buffer, 256);
        assert_eq!(c.max_subscriptions_per_instance, 1024);
        assert_eq!(c.max_subscription_duration, None, "unlimited by default");
        let c = with_env(&[
            ("KRAKEN_HUB_SUBSCRIPTION_BUFFER", " 16 "),
            ("KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE", "3"),
            ("KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION", "2m"),
        ])
        .unwrap();
        assert_eq!(c.subscription_buffer, 16);
        assert_eq!(c.max_subscriptions_per_instance, 3);
        assert_eq!(c.max_subscription_duration, Some(Duration::from_secs(120)));
        let c = with_env(&[("KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION", "0")]).unwrap();
        assert_eq!(c.max_subscription_duration, None, "0 is unlimited");
        let c = with_env(&[("KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION", "1s")]).unwrap();
        assert_eq!(c.max_subscription_duration, Some(Duration::from_secs(1)));
        for (name, value) in [
            ("KRAKEN_HUB_SUBSCRIPTION_BUFFER", "0"),
            ("KRAKEN_HUB_SUBSCRIPTION_BUFFER", "lots"),
            ("KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE", "0"),
            ("KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE", "-1"),
            ("KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION", "500ms"),
            ("KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION", "later"),
        ] {
            let err = with_env(&[(name, value)]).unwrap_err();
            assert!(err.to_string().contains(name), "{name}={value}: {err:#}");
        }
    }

    #[test]
    fn peers_default_to_none() {
        assert!(Config::default().peers.is_empty());
        assert!(
            with_env(&[("KRAKEN_HUB_PEERS", "")])
                .unwrap()
                .peers
                .is_empty()
        );
        assert!(
            with_env(&[("KRAKEN_HUB_PEERS", " , ")])
                .unwrap()
                .peers
                .is_empty()
        );
    }

    #[test]
    fn peers_are_normalised_to_origins_and_deduplicated() {
        let c = with_env(&[(
            "KRAKEN_HUB_PEERS",
            " http://localhost:7001 ,HTTP://LocalHost:7011/, https://hub.example, \
             http://localhost:7001/, https://hub.example:443, http://10.0.0.5:80/, http://[::1]:7001",
        )])
        .unwrap();
        assert_eq!(
            c.peers,
            [
                "http://localhost:7001",
                "http://localhost:7011",
                "https://hub.example",
                "http://10.0.0.5",
                "http://[::1]:7001",
            ]
        );
    }

    #[test]
    fn peers_must_be_plain_http_origins() {
        for bad in [
            "localhost:7001",
            "ftp://hub.example",
            "javascript:alert(1)",
            "http://",
            "http:///x",
            "http://user:pw@hub.example",
            "http://hub.example/ui",
            "http://hub.example/?a=1",
            "http://hub.example?a=1",
            "http://hub.example#x",
            "http://hub.example:notaport",
            "http://hub.example:0",
            "http://hub.example:70000",
            "http://hub example",
            "http://hub.example\u{7}",
            "http://hub.example,https://ok.example/path",
        ] {
            let err =
                with_env(&[("KRAKEN_HUB_PEERS", bad)]).expect_err(&format!("{bad:?} accepted"));
            let msg = format!("{err:#}");
            assert!(msg.contains("KRAKEN_HUB_PEERS"), "{msg}");
            assert!(!msg.contains("pw@"), "the message echoes user info: {msg}");
        }
    }

    #[test]
    fn at_most_sixteen_peers() {
        let list = |n: u16| {
            (0..n)
                .map(|i| format!("http://hub-{i}:7001"))
                .collect::<Vec<_>>()
                .join(",")
        };
        assert_eq!(
            with_env(&[("KRAKEN_HUB_PEERS", &list(16))])
                .unwrap()
                .peers
                .len(),
            16
        );
        let err = with_env(&[("KRAKEN_HUB_PEERS", &list(17))]).unwrap_err();
        assert!(format!("{err:#}").contains("at most 16"), "{err:#}");
        // Duplicates count once.
        let twice = format!("{},{}", list(16), list(16));
        assert_eq!(
            with_env(&[("KRAKEN_HUB_PEERS", &twice)])
                .unwrap()
                .peers
                .len(),
            16
        );
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

    #[test]
    fn grafana_url_must_not_carry_user_info() {
        for bad in [
            "https://user:pass@grafana.example",
            "https://user@grafana.example",
            "http://admin:hunter2@grafana.example:3000/d/hub",
        ] {
            let err =
                with_env(&[("KRAKEN_HUB_GRAFANA_URL", bad)]).expect_err(&format!("{bad} accepted"));
            let msg = format!("{err:#}");
            assert!(msg.contains("KRAKEN_HUB_GRAFANA_URL"), "{msg}");
            assert!(msg.contains("user info"), "{msg}");
            assert!(!msg.contains(bad) && !msg.contains("hunter2"), "{msg}");
        }
        // An `@` after the host (path, query, fragment) is not user info.
        for good in [
            "https://grafana.example/d/x@y",
            "https://grafana.example/d/x?by=a@b",
            "https://grafana.example/#a@b",
        ] {
            assert!(
                with_env(&[("KRAKEN_HUB_GRAFANA_URL", good)]).is_ok(),
                "{good} rejected"
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

    #[test]
    fn jwt_is_off_by_default() {
        let c = with_env(&[]).unwrap();
        assert_eq!(c.jwt_issuer, None);
        assert_eq!(c.jwt_jwks_uri, None);
        assert_eq!(c.jwt_audience, None);
        assert_eq!(c.jwt_leeway, Duration::from_secs(30));
        assert_eq!(c.jwt_expiry_grace, Duration::from_secs(30));
        assert_eq!(c.jwks_refresh, Duration::from_secs(300));
        assert_eq!(c.jwt_jwks_ca, None);
        assert_eq!(c.jwks_refetch_interval, Duration::from_secs(30));
        assert_eq!(c.jwks_fetch_timeout, Duration::from_secs(10));
    }

    #[test]
    fn the_jwks_uri_defaults_to_keycloaks_certs_path_under_the_issuer() {
        let c =
            with_env(&[("KRAKEN_HUB_JWT_ISSUER", "https://sso.example/realms/kraken")]).unwrap();
        assert_eq!(
            c.jwt_issuer.as_deref(),
            Some("https://sso.example/realms/kraken")
        );
        assert_eq!(
            c.jwt_jwks_uri.as_deref(),
            Some("https://sso.example/realms/kraken/protocol/openid-connect/certs")
        );
        // A trailing slash does not double up in the derived URI; the issuer itself is kept exact.
        let c = with_env(&[(
            "KRAKEN_HUB_JWT_ISSUER",
            "https://sso.example/realms/kraken/",
        )])
        .unwrap();
        assert_eq!(
            c.jwt_issuer.as_deref(),
            Some("https://sso.example/realms/kraken/")
        );
        assert_eq!(
            c.jwt_jwks_uri.as_deref(),
            Some("https://sso.example/realms/kraken/protocol/openid-connect/certs")
        );
    }

    #[test]
    fn the_jwt_settings_are_read_from_the_environment() {
        let c = with_env(&[
            ("KRAKEN_HUB_JWT_ISSUER", "https://sso.example/realms/kraken"),
            (
                "KRAKEN_HUB_JWT_JWKS_URI",
                "http://keycloak:8080/realms/kraken/certs",
            ),
            ("KRAKEN_HUB_JWT_AUDIENCE", "kraken-hub"),
            ("KRAKEN_HUB_JWT_LEEWAY", "5s"),
            ("KRAKEN_HUB_JWT_EXPIRY_GRACE", "0"),
            ("KRAKEN_HUB_JWKS_REFRESH", "10m"),
            ("KRAKEN_HUB_JWT_JWKS_CA", "/etc/hub/ca.pem"),
        ])
        .unwrap();
        assert_eq!(
            c.jwt_jwks_uri.as_deref(),
            Some("http://keycloak:8080/realms/kraken/certs")
        );
        assert_eq!(c.jwt_audience.as_deref(), Some("kraken-hub"));
        assert_eq!(c.jwt_leeway, Duration::from_secs(5));
        assert_eq!(c.jwt_expiry_grace, Duration::ZERO);
        assert_eq!(c.jwks_refresh, Duration::from_secs(600));
        assert_eq!(c.jwt_jwks_ca.as_deref(), Some(Path::new("/etc/hub/ca.pem")));
    }

    #[test]
    fn jwt_settings_without_an_issuer_are_a_startup_error() {
        for (name, value) in [
            ("KRAKEN_HUB_JWT_JWKS_URI", "https://sso.example/certs"),
            ("KRAKEN_HUB_JWT_AUDIENCE", "kraken-hub"),
            ("KRAKEN_HUB_JWT_JWKS_CA", "/etc/hub/ca.pem"),
        ] {
            let err = with_env(&[(name, value)]).unwrap_err();
            assert!(
                format!("{err:#}").contains("KRAKEN_HUB_JWT_ISSUER"),
                "{name}: {err:#}"
            );
        }
    }

    #[test]
    fn bad_jwt_settings_are_startup_errors() {
        let issuer = ("KRAKEN_HUB_JWT_ISSUER", "https://sso.example/realms/kraken");
        // The derived JWKS URI needs an http(s) issuer; an explicit one lifts that.
        assert!(with_env(&[("KRAKEN_HUB_JWT_ISSUER", "kraken-sso")]).is_err());
        assert!(
            with_env(&[
                ("KRAKEN_HUB_JWT_ISSUER", "kraken-sso"),
                ("KRAKEN_HUB_JWT_JWKS_URI", "https://sso.example/certs")
            ])
            .is_ok()
        );
        for uri in [
            "ftp://sso.example/certs",
            "sso.example/certs",
            "https://user:secret@sso.example/certs",
            "https:///certs",
        ] {
            let err = with_env(&[issuer, ("KRAKEN_HUB_JWT_JWKS_URI", uri)]).unwrap_err();
            let text = format!("{err:#}");
            assert!(text.contains("KRAKEN_HUB_JWT_JWKS_URI"), "{uri}: {text}");
            assert!(!text.contains("secret"), "never echo user info: {text}");
        }
        assert!(with_env(&[issuer, ("KRAKEN_HUB_JWKS_REFRESH", "0")]).is_err());
        assert!(with_env(&[issuer, ("KRAKEN_HUB_JWT_LEEWAY", "soon")]).is_err());
        assert!(with_env(&[issuer, ("KRAKEN_HUB_JWT_EXPIRY_GRACE", "-1")]).is_err());
    }

    #[test]
    fn an_issuer_with_access_control_off_warns_that_nothing_is_checked() {
        let jwt_off = |mode: &str| {
            let mut env = vec![("KRAKEN_HUB_JWT_ISSUER", "https://sso.example/realms/kraken")];
            if mode != "off" {
                env.extend([
                    ("KRAKEN_HUB_AUTH_MODE", mode),
                    ("KRAKEN_HUB_ACL_FILE", "/acl"),
                ]);
            }
            with_env(&env)
                .unwrap()
                .security_warnings()
                .iter()
                .filter(|w| w.contains("KRAKEN_HUB_JWT_ISSUER"))
                .count()
        };
        assert_eq!(jwt_off("off"), 1);
        assert_eq!(jwt_off("permissive"), 0);
        assert_eq!(jwt_off("enforce"), 0);
        assert!(
            with_env(&[])
                .unwrap()
                .security_warnings()
                .iter()
                .all(|w| !w.contains("KRAKEN_HUB_JWT_ISSUER"))
        );
    }

    #[test]
    fn a_plain_http_jwks_warns_unless_it_stays_on_the_host() {
        let warns = |uri: &str| {
            let c = with_env(&[
                ("KRAKEN_HUB_JWT_ISSUER", "https://sso.example/realms/kraken"),
                ("KRAKEN_HUB_JWT_JWKS_URI", uri),
            ])
            .unwrap();
            c.security_warnings()
                .into_iter()
                .filter(|w| w.contains("KRAKEN_HUB_JWT_JWKS_URI"))
                .count()
        };
        assert_eq!(warns("http://sso.example/certs"), 1);
        assert_eq!(warns("http://10.0.0.7:8080/certs"), 1);
        assert_eq!(warns("http://[2001:db8::1]/certs"), 1);
        // Loopback and a single-label name (a compose service) are not worth a warning.
        for quiet in [
            "http://localhost:8180/certs",
            "http://127.0.0.1:8180/certs",
            "http://[::1]:8180/certs",
            "http://keycloak:8080/certs",
            "https://sso.example/certs",
        ] {
            assert_eq!(warns(quiet), 0, "{quiet}");
        }
        // One warning, however many times the hub asks.
        let c = with_env(&[("KRAKEN_HUB_JWT_ISSUER", "http://sso.example/realms/kraken")]).unwrap();
        assert_eq!(
            c.security_warnings()
                .iter()
                .filter(|w| w.contains("KRAKEN_HUB_JWT_JWKS_URI"))
                .count(),
            1,
            "a derived plain-http URI warns too"
        );
    }

    #[test]
    fn auth_mode_needs_an_acl_file_unless_off() {
        assert_eq!(with_env(&[]).unwrap().auth_mode, AuthMode::Off);
        for mode in ["permissive", "enforce"] {
            let err = with_env(&[("KRAKEN_HUB_AUTH_MODE", mode)]).unwrap_err();
            assert!(err.to_string().contains("KRAKEN_HUB_ACL_FILE"), "{err:#}");
            let c = with_env(&[
                ("KRAKEN_HUB_AUTH_MODE", mode),
                ("KRAKEN_HUB_ACL_FILE", "/etc/hub/acl.toml"),
            ])
            .unwrap();
            assert_eq!(c.auth_mode.as_str(), mode);
            assert_eq!(c.acl_file.as_deref(), Some(Path::new("/etc/hub/acl.toml")));
        }
        assert!(with_env(&[("KRAKEN_HUB_AUTH_MODE", "strict")]).is_err());
        // Off ignores a file that is named.
        assert!(with_env(&[("KRAKEN_HUB_ACL_FILE", "/x")]).is_ok());
    }

    #[test]
    fn tls_files_come_in_pairs_and_the_http_pair_defaults_to_the_grpc_pair() {
        assert!(with_env(&[("KRAKEN_HUB_TLS_CERT", "/c.pem")]).is_err());
        assert!(with_env(&[("KRAKEN_HUB_TLS_KEY", "/k.pem")]).is_err());
        assert!(with_env(&[("KRAKEN_HUB_HTTP_TLS_CERT", "/c.pem")]).is_err());
        let c = with_env(&[
            ("KRAKEN_HUB_TLS_CERT", "/c.pem"),
            ("KRAKEN_HUB_TLS_KEY", "/k.pem"),
        ])
        .unwrap();
        let pair = Some((Path::new("/c.pem"), Path::new("/k.pem")));
        assert_eq!(c.grpc_tls(), pair);
        assert_eq!(c.http_tls(), pair);
        let c = with_env(&[
            ("KRAKEN_HUB_TLS_CERT", "/c.pem"),
            ("KRAKEN_HUB_TLS_KEY", "/k.pem"),
            ("KRAKEN_HUB_HTTP_TLS_CERT", "/h.pem"),
            ("KRAKEN_HUB_HTTP_TLS_KEY", "/hk.pem"),
        ])
        .unwrap();
        assert_eq!(
            c.http_tls(),
            Some((Path::new("/h.pem"), Path::new("/hk.pem")))
        );
        // HTTP only.
        let c = with_env(&[
            ("KRAKEN_HUB_HTTP_TLS_CERT", "/h.pem"),
            ("KRAKEN_HUB_HTTP_TLS_KEY", "/hk.pem"),
        ])
        .unwrap();
        assert_eq!((c.grpc_tls(), c.http_tls().is_some()), (None, true));
        assert_eq!(with_env(&[]).unwrap().http_tls(), None);
    }

    /// A scratch directory with `files` written into it; removed by the caller.
    fn scratch(files: &[(&str, &str)]) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("kraken-cfg-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        for (name, content) in files {
            std::fs::write(dir.join(name), content).unwrap();
        }
        dir
    }

    #[test]
    fn a_token_file_is_read_trimmed_and_wins_over_the_variable() {
        let dir = scratch(&[
            ("dash", "file-token-0123456789abcdef\n"),
            ("write", "write-token-0123456789abcdef\n"),
        ]);
        let path = |n: &str| dir.join(n).display().to_string();
        let c = with_env(&[
            ("KRAKEN_HUB_DASHBOARD_TOKEN", "variable-token-0123456789"),
            ("KRAKEN_HUB_DASHBOARD_TOKEN_FILE", &path("dash")),
            ("KRAKEN_HUB_DASHBOARD_WRITE_TOKEN_FILE", &path("write")),
        ])
        .unwrap();
        let dash = c.dashboard_token.as_ref().unwrap();
        assert!(dash.matches(Some("file-token-0123456789abcdef")));
        assert!(
            !dash.matches(Some("variable-token-0123456789")),
            "the file wins"
        );
        assert!(
            c.dashboard_write_token
                .as_ref()
                .unwrap()
                .matches(Some("write-token-0123456789abcdef"))
        );
        assert_eq!(c.dashboard_token_file, Some(dir.join("dash")));
        // Without the file variable the plain variable still works.
        let c = with_env(&[("KRAKEN_HUB_DASHBOARD_TOKEN", "variable-token-0123456789")]).unwrap();
        assert!(
            c.dashboard_token
                .unwrap()
                .matches(Some("variable-token-0123456789"))
        );
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_bad_token_file_stops_startup_without_echoing_its_content() {
        let dir = scratch(&[
            ("short", "tiny\n"),
            ("spaces", "has a space in it, 16+\n"),
            ("empty", "\n"),
        ]);
        let path = |n: &str| dir.join(n).display().to_string();
        for name in ["short", "spaces", "empty", "missing"] {
            let err = with_env(&[("KRAKEN_HUB_DASHBOARD_TOKEN_FILE", &path(name))]).unwrap_err();
            let text = format!("{err:#}");
            assert!(
                text.contains("KRAKEN_HUB_DASHBOARD_TOKEN_FILE"),
                "{name}: {text}"
            );
            assert!(
                !text.contains("tiny") && !text.contains("has a space"),
                "{name}: {text}"
            );
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_reload_picks_up_a_rotated_token_file_and_keeps_the_old_token_when_the_file_goes_bad() {
        let dir = scratch(&[("dash", "first-token-0123456789abcdef\n")]);
        let file = dir.join("dash");
        let c = with_env(&[(
            "KRAKEN_HUB_DASHBOARD_TOKEN_FILE",
            &file.display().to_string(),
        )])
        .unwrap();
        // A clone (as the HTTP guard holds) sees the rotation too.
        let held = c.dashboard_token.clone().unwrap();
        assert!(held.matches(Some("first-token-0123456789abcdef")));

        std::fs::write(&file, "second-token-0123456789abcdef\n").unwrap();
        assert!(c.reload_token_files());
        assert!(held.matches(Some("second-token-0123456789abcdef")));
        assert!(
            !held.matches(Some("first-token-0123456789abcdef")),
            "the old token stops working"
        );
        assert!(!c.reload_token_files(), "unchanged");

        for bad in ["", "short", "has a space in it, 16+"] {
            std::fs::write(&file, bad).unwrap();
            assert!(!c.reload_token_files(), "{bad:?}");
            assert!(
                held.matches(Some("second-token-0123456789abcdef")),
                "{bad:?} keeps the previous token"
            );
        }
        std::fs::remove_file(&file).unwrap();
        assert!(!c.reload_token_files());
        assert!(held.matches(Some("second-token-0123456789abcdef")));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn the_dashboard_tokens_are_trimmed_checked_and_never_shown() {
        let token = "0123456789abcdef-token\n";
        let c = with_env(&[("KRAKEN_HUB_DASHBOARD_TOKEN", token)]).unwrap();
        let secret = c.dashboard_token.as_ref().unwrap();
        assert!(secret.matches(Some("0123456789abcdef-token")));
        assert!(!secret.matches(Some("0123456789abcdef-token\n")));
        assert!(!format!("{c:?}").contains("0123456789abcdef"));
        let err = with_env(&[("KRAKEN_HUB_DASHBOARD_TOKEN", "short")]).unwrap_err();
        assert!(err.to_string().contains("at least 16"), "{err:#}");
        assert!(!format!("{err:#}").contains("short\""));
        assert!(with_env(&[("KRAKEN_HUB_DASHBOARD_TOKEN", "has a space in it, 16+")]).is_err());
        // The write token is parsed and kept; nothing reads it yet.
        let c = with_env(&[
            ("KRAKEN_HUB_DASHBOARD_TOKEN", token),
            ("KRAKEN_HUB_DASHBOARD_WRITE_TOKEN", "fedcba9876543210-write"),
        ])
        .unwrap();
        assert!(
            c.dashboard_write_token
                .unwrap()
                .matches(Some("fedcba9876543210-write"))
        );
        assert!(with_env(&[]).unwrap().dashboard_token.is_none());
    }

    #[test]
    fn open_ports_without_protection_are_security_warnings() {
        let open = Config::default();
        let w = open.security_warnings();
        assert!(
            w.iter().any(|m| m.contains("KRAKEN_HUB_AUTH_MODE=off")),
            "{w:?}"
        );
        assert!(
            w.iter().any(|m| m.contains("KRAKEN_HUB_DASHBOARD_TOKEN")),
            "{w:?}"
        );
        let permissive = Config {
            auth_mode: AuthMode::Permissive,
            ..Config::default()
        };
        assert!(
            permissive
                .security_warnings()
                .iter()
                .any(|m| m.contains("KRAKEN_HUB_AUTH_MODE=permissive"))
        );
        let local = Config {
            listen: "127.0.0.1:7000".parse().unwrap(),
            http_listen: Some("127.0.0.1:7001".parse().unwrap()),
            ..Config::default()
        };
        assert!(local.security_warnings().is_empty());
        let enforced = Config {
            auth_mode: AuthMode::Enforce,
            acl_file: Some("/acl".into()),
            ..Config::default()
        };
        let w = enforced.security_warnings();
        assert!(!w.iter().any(|m| m.contains("AUTH_MODE")), "{w:?}");
        assert!(w.iter().any(|m| m.contains("no TLS")), "{w:?}");
    }

    #[test]
    fn enforce_without_a_dashboard_token_warns_that_the_http_topology_is_open() {
        // Even on loopback: enforce is meant to protect the topology, and HTTP does not use ACL tokens.
        let enforced = Config {
            auth_mode: AuthMode::Enforce,
            acl_file: Some("/acl".into()),
            listen: "127.0.0.1:7000".parse().unwrap(),
            http_listen: Some("127.0.0.1:7001".parse().unwrap()),
            ..Config::default()
        };
        let w = enforced.security_warnings();
        assert_eq!(
            w.iter()
                .filter(|m| m.contains("KRAKEN_HUB_DASHBOARD_TOKEN"))
                .count(),
            1,
            "{w:?}"
        );
        assert!(
            w.iter()
                .any(|m| m.contains("127.0.0.1:7001") && m.contains("enforce")),
            "{w:?}"
        );
        let with_token = Config {
            dashboard_token: Some(crate::auth::Secret::new("0123456789abcdef0123")),
            ..enforced
        };
        assert!(
            !with_token
                .security_warnings()
                .iter()
                .any(|m| m.contains("KRAKEN_HUB_DASHBOARD_TOKEN")),
        );
    }

    #[test]
    fn the_tls_handshake_bounds_default_parse_and_validate() {
        let d = with_env(&[]).unwrap();
        assert_eq!(d.tls_handshake_timeout, Duration::from_secs(5));
        assert_eq!(d.tls_handshakes_per_ip, 32);
        let c = with_env(&[
            ("KRAKEN_HUB_TLS_HANDSHAKE_TIMEOUT", "750ms"),
            ("KRAKEN_HUB_TLS_HANDSHAKES_PER_IP", "0"),
        ])
        .unwrap();
        assert_eq!(c.tls_handshake_timeout, Duration::from_millis(750));
        assert_eq!(c.tls_handshakes_per_ip, 0);
        let limits = c.handshake_limits();
        assert_eq!(limits.timeout, Duration::from_millis(750));
        assert_eq!(limits.per_ip, 0);
        assert_eq!(limits.slots, crate::tls::MAX_HANDSHAKES);
        assert!(with_env(&[("KRAKEN_HUB_TLS_HANDSHAKE_TIMEOUT", "0")]).is_err());
        assert!(with_env(&[("KRAKEN_HUB_TLS_HANDSHAKES_PER_IP", "many")]).is_err());
    }

    #[test]
    fn the_reload_interval_defaults_to_five_seconds_and_must_be_positive() {
        assert_eq!(
            with_env(&[]).unwrap().reload_interval,
            Duration::from_secs(5)
        );
        assert_eq!(
            with_env(&[("KRAKEN_HUB_RELOAD_INTERVAL", "250ms")])
                .unwrap()
                .reload_interval,
            Duration::from_millis(250)
        );
        assert!(with_env(&[("KRAKEN_HUB_RELOAD_INTERVAL", "0")]).is_err());
    }
}
