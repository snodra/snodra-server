//! JWT credentials (roadmap P5b, ADR 0019): validation of a Keycloak client-credentials access
//! token against the issuer's signing keys, and the cache of those keys.
//!
//! * [`Jwt`] holds the settings and the key cache. The keys are fetched in the background
//!   ([`Jwt::spawn`]) at startup and every `SNODRA_SERVER_JWKS_REFRESH`, and when a token names a
//!   `kid` the cache lacks ([`Jwt::ensure_key`], at most once per `jwks_refetch_interval`). A
//!   failed fetch keeps the last good set.
//! * [`Jwt::verify`] is synchronous over the cache: signature, algorithm, `iss`, `aud`, `exp`,
//!   `nbf`, `iat`, `typ`, and `azp`. It never logs or returns the token.
//!
//! Crypto is `jsonwebtoken` with `rust_crypto` (pure Rust, no C toolchain); the fetch is a small
//! hyper HTTP/1 client over the rustls (ring) the hub already uses.

use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Context as _;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http_body_util::{BodyExt, Empty, Limited};
use hyper::body::Bytes;
use hyper::header::{ACCEPT, CONNECTION, HOST};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, Jwk, PublicKeyUse};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use rustls::RootCertStore;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use serde::Deserialize;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_rustls::TlsConnector;
use tokio_util::sync::CancellationToken;
use tonic::codegen::http::Uri;
use tracing::{debug, info, warn};

use crate::auth::clip;
use crate::config::Config;
use crate::telemetry::Metrics;

#[cfg(test)]
#[path = "jwt_test_support.rs"]
pub(crate) mod test_support;

/// The most bytes a JWKS response may have.
pub const MAX_JWKS_BYTES: usize = 1 << 20;

/// The longest token that is looked at; a Keycloak access token is a few kilobytes.
pub const MAX_TOKEN_BYTES: usize = 16 * 1024;

/// Longest wait between two fetches while the last one failed.
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// A year `exp` is clamped to, so converting it to a time cannot overflow.
const MAX_EXP_SECS: f64 = 253_402_300_799.0;

/// What a valid token says.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verified {
    /// The service the token was issued to (`azp`).
    pub azp: String,
    /// The `kid` of the key that signed it, when the header named one.
    pub kid: Option<String>,
    /// `exp`.
    pub expires_at: SystemTime,
}

/// Why a token was refused. [`JwtError::label`] is the `result` label of
/// `snodra_server_jwt_validations_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JwtError {
    /// The first fetch of the key set has not succeeded yet.
    NotLoaded,
    Malformed,
    BadAlg,
    UnknownKid,
    /// The `kid` is not in the key set, and the hub cannot yet say whether it ever will be.
    KeyPending,
    BadSignature,
    Expired,
    NotYetValid,
    WrongIssuer,
    WrongAudience,
    WrongType,
    MissingClaim,
}

impl JwtError {
    /// A fixed label, never text a client chose.
    pub fn label(self) -> &'static str {
        match self {
            Self::NotLoaded => "not_loaded",
            Self::Malformed => "malformed",
            Self::BadAlg => "bad_alg",
            Self::UnknownKid => "unknown_kid",
            Self::KeyPending => "key_pending",
            Self::BadSignature => "bad_signature",
            Self::Expired => "expired",
            Self::NotYetValid => "not_yet_valid",
            Self::WrongIssuer => "wrong_issuer",
            Self::WrongAudience => "wrong_audience",
            Self::WrongType => "wrong_type",
            Self::MissingClaim => "missing_claim",
        }
    }

    /// What the client is told: no claim, key or token in it.
    pub fn message(self) -> &'static str {
        match self {
            Self::NotLoaded => "jwks not loaded yet",
            Self::Malformed => "malformed token",
            Self::BadAlg => "unsupported token algorithm",
            Self::UnknownKid => "unknown signing key",
            Self::KeyPending => "signing key not known yet",
            Self::BadSignature => "invalid token signature",
            Self::Expired => "token expired",
            Self::NotYetValid => "token not valid yet",
            Self::WrongIssuer => "wrong token issuer",
            Self::WrongAudience => "wrong token audience",
            Self::WrongType => "not an access token",
            Self::MissingClaim => "token lacks a required claim",
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Keys

#[derive(Clone, Copy, PartialEq, Eq)]
enum Family {
    Rsa,
    Ec(Algorithm),
}

struct Key {
    kid: Option<String>,
    /// The `alg` the key declares, when it does.
    alg: Option<Algorithm>,
    family: Family,
    decoding: DecodingKey,
}

impl Key {
    /// True when this key may verify a token signed with `alg`.
    fn fits(&self, alg: Algorithm) -> bool {
        let family_ok = match (self.family, alg) {
            (Family::Rsa, Algorithm::RS256 | Algorithm::RS384 | Algorithm::RS512) => true,
            (Family::Rsa, Algorithm::PS256 | Algorithm::PS384 | Algorithm::PS512) => true,
            // An EC key is for the one algorithm its curve names.
            (Family::Ec(own), alg) => own == alg,
            _ => false,
        };
        family_ok && self.alg.is_none_or(|declared| declared == alg)
    }
}

/// The signing keys of a JWKS document, and how many entries were skipped. Entries that are not
/// signing keys (encryption keys, `oct`, `OKP`, unknown types, junk) are skipped, not errors:
/// Keycloak serves an RSA-OAEP key next to the signing key.
fn parse_jwks(body: &[u8]) -> Result<(Vec<Key>, usize), FetchError> {
    #[derive(Deserialize)]
    struct Doc {
        keys: Vec<serde_json::Value>,
    }
    let doc: Doc = serde_json::from_slice(body).map_err(|_| FetchError::Invalid)?;
    let mut keys = Vec::new();
    let mut skipped = 0;
    for entry in doc.keys {
        match serde_json::from_value::<Jwk>(entry).ok().and_then(into_key) {
            Some(key) => keys.push(key),
            None => skipped += 1,
        }
    }
    if keys.is_empty() {
        return Err(FetchError::Invalid);
    }
    Ok((keys, skipped))
}

fn into_key(jwk: Jwk) -> Option<Key> {
    if jwk
        .common
        .public_key_use
        .as_ref()
        .is_some_and(|u| *u != PublicKeyUse::Signature)
    {
        return None;
    }
    let family = match &jwk.algorithm {
        AlgorithmParameters::RSA(_) => Family::Rsa,
        AlgorithmParameters::EllipticCurve(ec) => match ec.curve {
            EllipticCurve::P256 => Family::Ec(Algorithm::ES256),
            EllipticCurve::P384 => Family::Ec(Algorithm::ES384),
            _ => return None,
        },
        _ => return None,
    };
    let alg = match jwk.common.key_algorithm {
        None => None,
        Some(declared) => Some(declared.to_string().parse::<Algorithm>().ok()?),
    };
    Some(Key {
        kid: jwk.common.key_id.clone(),
        alg,
        family,
        decoding: DecodingKey::from_jwk(&jwk).ok()?,
    })
}

// ---------------------------------------------------------------------------------------------
// The token's header

const ALLOWED: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
    Algorithm::ES256,
    Algorithm::ES384,
];

struct TokenHeader {
    alg: Algorithm,
    kid: Option<String>,
}

/// Reads the header of `token` without trusting anything in it: the shape (three segments), the
/// algorithm (only [`ALLOWED`]) and the `kid`.
fn read_header(token: &str) -> Result<TokenHeader, JwtError> {
    #[derive(Deserialize)]
    struct Raw {
        alg: Option<String>,
        kid: Option<String>,
    }
    if token.len() > MAX_TOKEN_BYTES {
        return Err(JwtError::Malformed);
    }
    let mut parts = token.split('.');
    let (Some(head), Some(_), Some(_), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(JwtError::Malformed);
    };
    let raw: Raw = URL_SAFE_NO_PAD
        .decode(head)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .ok_or(JwtError::Malformed)?;
    let name = raw.alg.ok_or(JwtError::Malformed)?;
    let alg = ALLOWED
        .iter()
        .copied()
        .find(|a| format!("{a:?}") == name)
        .ok_or(JwtError::BadAlg)?;
    Ok(TokenHeader { alg, kid: raw.kid })
}

#[derive(Deserialize)]
struct Claims {
    azp: Option<String>,
    typ: Option<String>,
    exp: Option<f64>,
    iat: Option<f64>,
}

// ---------------------------------------------------------------------------------------------
// Fetching

#[derive(Debug, Clone, PartialEq, Eq)]
enum FetchError {
    /// Connecting, or the HTTP exchange, failed (the I/O error's text).
    Network(String),
    /// The TLS handshake failed: the certificate is not trusted or does not match, or the peer
    /// does not speak TLS (rustls's text).
    Tls(String),
    Timeout,
    /// Anything but 200.
    Status(u16),
    TooLarge,
    Invalid,
}

impl FetchError {
    /// The `result` label of `snodra_server_jwks_fetches_total`: fixed strings.
    fn label(&self) -> &'static str {
        match self {
            Self::Network(_) => "network",
            Self::Tls(_) => "tls",
            Self::Timeout => "timeout",
            Self::Status(_) => "status",
            Self::TooLarge => "too_large",
            Self::Invalid => "invalid",
        }
    }

    /// What the log line adds to the label: the OS or rustls error text, the status code. None of
    /// it is secret (the response body, which is not logged, is the only thing the peer chose).
    fn detail(&self) -> String {
        match self {
            Self::Network(why) | Self::Tls(why) => why.clone(),
            Self::Status(code) => format!("HTTP status {code}"),
            Self::Timeout => "no complete response in time".into(),
            Self::TooLarge => "response over the size cap".into(),
            Self::Invalid => "not a JWKS document with a signing key".into(),
        }
    }
}

/// Fetches the key set: HTTP/1.1 GET, no redirects, a bounded body, a bounded time.
struct Fetcher {
    uri: Uri,
    https: bool,
    host: String,
    port: u16,
    tls: Option<TlsConnector>,
    timeout: Duration,
}

impl Fetcher {
    fn new(uri: &str, ca: Option<&Path>, timeout: Duration) -> anyhow::Result<Self> {
        let uri: Uri = uri.parse().context("SNODRA_SERVER_JWT_JWKS_URI")?;
        let https = uri.scheme_str() == Some("https");
        // `Uri::host` keeps the brackets of an IPv6 literal; neither connecting nor a TLS server
        // name takes them.
        let host = uri
            .host()
            .context("SNODRA_SERVER_JWT_JWKS_URI has no host")?
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
        let tls = if https {
            Some(TlsConnector::from(Arc::new(tls_config(ca)?)))
        } else {
            None
        };
        if !https && ca.is_some() {
            // Unused, but still checked: a CA that cannot be read is a mistake the operator
            // must hear about now, not when the URI later becomes https.
            tls_config(ca)?;
            warn!(
                "SNODRA_SERVER_JWT_JWKS_CA is set but the JWKS URI is plain http: the CA is unused"
            );
        }
        Ok(Self {
            uri,
            https,
            host,
            port,
            tls,
            timeout,
        })
    }

    async fn get(&self) -> Result<Vec<u8>, FetchError> {
        match tokio::time::timeout(self.timeout, self.exchange()).await {
            Ok(result) => result,
            Err(_) => Err(FetchError::Timeout),
        }
    }

    async fn exchange(&self) -> Result<Vec<u8>, FetchError> {
        let tcp = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|e| FetchError::Network(e.to_string()))?;
        match &self.tls {
            Some(connector) if self.https => {
                let name = ServerName::try_from(self.host.clone())
                    .map_err(|e| FetchError::Network(format!("server name: {e}")))?;
                let tls = connector.connect(name, tcp).await.map_err(|e| {
                    // rustls errors travel inside the io::Error; a reset in the middle of the
                    // handshake is the network's.
                    match e.get_ref().and_then(|i| i.downcast_ref::<rustls::Error>()) {
                        Some(tls) => FetchError::Tls(tls.to_string()),
                        None => FetchError::Network(e.to_string()),
                    }
                })?;
                self.request(tls).await
            }
            _ => self.request(tcp).await,
        }
    }

    async fn request<Io>(&self, io: Io) -> Result<Vec<u8>, FetchError>
    where
        Io: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
            .await
            .map_err(|e| FetchError::Network(e.to_string()))?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        let path = self
            .uri
            .path_and_query()
            .map_or("/", |p| p.as_str())
            .to_string();
        let request = Request::builder()
            .method(Method::GET)
            .uri(path)
            .header(HOST, self.uri.authority().map_or("", |a| a.as_str()))
            .header(ACCEPT, "application/json")
            .header(CONNECTION, "close")
            .body(Empty::<Bytes>::new())
            .map_err(|e| FetchError::Network(e.to_string()))?;
        let outcome = async {
            let response = sender
                .send_request(request)
                .await
                .map_err(|e| FetchError::Network(e.to_string()))?;
            if response.status() != StatusCode::OK {
                return Err(FetchError::Status(response.status().as_u16()));
            }
            let body = Limited::new(response.into_body(), MAX_JWKS_BYTES)
                .collect()
                .await
                .map_err(|e| {
                    if e.downcast_ref::<http_body_util::LengthLimitError>()
                        .is_some()
                    {
                        FetchError::TooLarge
                    } else {
                        FetchError::Network(e.to_string())
                    }
                })?;
            Ok(body.to_bytes().to_vec())
        }
        .await;
        driver.abort();
        outcome
    }
}

/// The platform roots plus `ca`, TLS 1.2 and 1.3, HTTP/1.1 only; rustls with `ring`.
fn tls_config(ca: Option<&Path>) -> anyhow::Result<rustls::ClientConfig> {
    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    roots.add_parsable_certificates(native.certs);
    if let Some(path) = ca {
        let mut added = 0;
        for cert in CertificateDer::pem_file_iter(path)
            .with_context(|| format!("SNODRA_SERVER_JWT_JWKS_CA: read {}", path.display()))?
        {
            let cert = cert
                .with_context(|| format!("SNODRA_SERVER_JWT_JWKS_CA: parse {}", path.display()))?;
            roots.add(cert).context("SNODRA_SERVER_JWT_JWKS_CA")?;
            added += 1;
        }
        anyhow::ensure!(
            added > 0,
            "SNODRA_SERVER_JWT_JWKS_CA: no certificate in {}",
            path.display()
        );
    }
    let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

// ---------------------------------------------------------------------------------------------
// The verifier

struct State {
    keys: Arc<Vec<Key>>,
    loaded: bool,
    /// When an unknown `kid` last forced a fetch.
    last_forced: Option<Instant>,
    /// `kid`s that a fetch made for them found missing from the key set (bounded; cleared when
    /// the set is replaced by a fetch nobody asked for). Only these are `UnknownKid`: any other
    /// `kid` the set lacks is [`JwtError::KeyPending`], because no fetch has looked for it yet.
    absent: Vec<String>,
}

/// How many `kid`s [`State::absent`] remembers: only the one a forced fetch (at most one per
/// refetch interval) was made for gets in, so this is a bound, not a working size.
const MAX_ABSENT: usize = 8;

/// What [`Jwt`] needs from the configuration.
struct Settings {
    issuer: String,
    audience: Option<String>,
    leeway: Duration,
    expiry_grace: Duration,
    refresh: Duration,
    refetch_interval: Duration,
}

pub struct Jwt {
    settings: Settings,
    fetcher: Fetcher,
    metrics: Arc<Metrics>,
    state: RwLock<State>,
    /// One fetch at a time: a second caller waits for the first and then looks at the keys.
    fetching: tokio::sync::Mutex<()>,
    /// Keeps the log of one failure streak to one line (see [`Jwt::fetch_once`]).
    failing: Mutex<bool>,
}

impl Jwt {
    /// The verifier `config` asks for; `None` when no issuer is set (JWT credentials are off).
    /// Does not fetch anything and does not need the network: see [`Jwt::spawn`]. An unreadable
    /// CA file is an error (startup).
    pub fn from_config(
        config: &Config,
        metrics: Arc<Metrics>,
    ) -> anyhow::Result<Option<Arc<Self>>> {
        let (Some(issuer), Some(uri)) = (&config.jwt_issuer, &config.jwt_jwks_uri) else {
            return Ok(None);
        };
        let fetcher = Fetcher::new(
            uri,
            config.jwt_jwks_ca.as_deref(),
            config.jwks_fetch_timeout,
        )?;
        Ok(Some(Arc::new(Self {
            settings: Settings {
                issuer: issuer.clone(),
                audience: config.jwt_audience.clone(),
                leeway: config.jwt_leeway,
                expiry_grace: config.jwt_expiry_grace,
                refresh: config.jwks_refresh,
                refetch_interval: config.jwks_refetch_interval,
            },
            fetcher,
            metrics,
            state: RwLock::new(State {
                keys: Arc::default(),
                loaded: false,
                last_forced: None,
                absent: Vec::new(),
            }),
            fetching: tokio::sync::Mutex::new(()),
            failing: Mutex::new(false),
        })))
    }

    /// Fetches the key set now and then every `SNODRA_SERVER_JWKS_REFRESH`, until `stopping`. It
    /// never blocks or fails the hub's startup: while the keys are not loaded a JWT is refused
    /// with `UNAVAILABLE` and the fetch is retried with a growing delay (1 s, 2 s, ... capped at
    /// 30 s and at the refetch interval); a failed refresh keeps the last good keys and retries
    /// the same way.
    pub fn spawn(self: &Arc<Self>, stopping: CancellationToken) -> JoinHandle<()> {
        let jwt = Arc::clone(self);
        tokio::spawn(async move {
            let mut failures = 0u32;
            loop {
                let ok = {
                    let _one_at_a_time = jwt.fetching.lock().await;
                    jwt.fetch_once().await
                };
                failures = if ok { 0 } else { failures.saturating_add(1) };
                let wait = if ok {
                    jwt.settings.refresh
                } else {
                    Duration::from_secs(1 << (failures - 1).min(5))
                        .min(MAX_RETRY_BACKOFF)
                        .min(jwt.settings.refetch_interval)
                };
                tokio::select! {
                    _ = stopping.cancelled() => return,
                    _ = tokio::time::sleep(wait) => {}
                }
            }
        })
    }

    /// How long after `exp` a session stays open without a refresh.
    pub fn expiry_grace(&self) -> Duration {
        self.settings.expiry_grace
    }

    /// True once a fetch has succeeded.
    pub fn is_loaded(&self) -> bool {
        self.state.read().unwrap().loaded
    }

    /// Signing keys in the current set.
    pub fn key_count(&self) -> usize {
        self.state.read().unwrap().keys.len()
    }

    fn has_kid(&self, kid: &str) -> bool {
        self.state
            .read()
            .unwrap()
            .keys
            .iter()
            .any(|k| k.kid.as_deref() == Some(kid))
    }

    /// When `token` names a `kid` the loaded keys lack, fetches the key set (a rotation), at most
    /// once per refetch interval; concurrent callers share the fetch. Does nothing otherwise
    /// (nothing is loaded yet, no `kid`, a known `kid`, a token that is not a JWT). Call before
    /// [`Jwt::verify`].
    pub async fn ensure_key(self: &Arc<Self>, token: &str) {
        let Ok(TokenHeader { kid: Some(kid), .. }) = read_header(token) else {
            return;
        };
        if !self.is_loaded() || self.has_kid(&kid) || self.throttled() {
            return;
        }
        // The fetch runs as a task of its own, so a caller that stops waiting (a bounded wait,
        // a client that went away) does not cancel it: its result and its place in the throttle
        // stay honest, and the next caller finds the key.
        let jwt = Arc::clone(self);
        let _ = tokio::spawn(async move { jwt.refetch_for(kid).await }).await;
    }

    /// True while an unknown-`kid` refetch is not allowed yet.
    fn throttled(&self) -> bool {
        self.state.read().unwrap().last_forced.is_some_and(|at| {
            Instant::now().saturating_duration_since(at) < self.settings.refetch_interval
        })
    }

    async fn refetch_for(&self, kid: String) {
        let _one_at_a_time = self.fetching.lock().await;
        if self.has_kid(&kid) {
            return;
        }
        {
            let mut state = self.state.write().unwrap();
            let now = Instant::now();
            if state.last_forced.is_some_and(|at| {
                now.saturating_duration_since(at) < self.settings.refetch_interval
            }) {
                return;
            }
            state.last_forced = Some(now);
        }
        debug!(kid = %clip(&kid), "unknown kid: fetching the key set");
        if self.fetch_once().await && !self.has_kid(&kid) {
            let mut state = self.state.write().unwrap();
            if state.absent.len() >= MAX_ABSENT {
                state.absent.remove(0);
            }
            state.absent.push(kid);
        }
    }

    /// One fetch; true when it replaced the key set. A failure keeps the last good set (logged
    /// once per streak of failures, counted every time).
    async fn fetch_once(&self) -> bool {
        let outcome = self.fetcher.get().await.and_then(|body| parse_jwks(&body));
        match outcome {
            Ok((keys, skipped)) => {
                self.metrics.jwks_fetch("ok");
                self.metrics.jwks_keys_set(keys.len() as u64);
                let count = keys.len();
                let mut state = self.state.write().unwrap();
                let first = !state.loaded;
                state.keys = Arc::new(keys);
                state.loaded = true;
                state.absent.clear();
                drop(state);
                let recovered = std::mem::take(&mut *self.failing.lock().unwrap());
                if first || recovered {
                    info!(keys = count, skipped, uri = %self.fetcher.uri, "jwks loaded");
                } else {
                    debug!(keys = count, skipped, "jwks refreshed");
                }
                true
            }
            Err(e) => {
                self.metrics.jwks_fetch(e.label());
                let first_failure = !std::mem::replace(&mut *self.failing.lock().unwrap(), true);
                if first_failure {
                    warn!(
                        reason = e.label(),
                        detail = %e.detail(),
                        uri = %self.fetcher.uri,
                        loaded = self.is_loaded(),
                        "jwks fetch failed; keeping the last good keys (further failures are counted, not logged, until one succeeds)"
                    );
                }
                false
            }
        }
    }

    /// Validates `token` against the loaded keys and counts the result. Never logs the token.
    pub fn verify(&self, token: &str) -> Result<Verified, JwtError> {
        let result = self.validate(token);
        self.metrics
            .jwt_validation(result.as_ref().map_or_else(|e| e.label(), |_| "ok"));
        match &result {
            Ok(v) => debug!(
                azp = %clip(&v.azp),
                kid = %v.kid.as_deref().map(clip).unwrap_or_default(),
                exp = v.expires_at.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs()),
                "jwt accepted"
            ),
            Err(e) => debug!(reason = e.label(), "jwt refused"),
        }
        result
    }

    fn validate(&self, token: &str) -> Result<Verified, JwtError> {
        let header = read_header(token)?;
        let (keys, loaded, absent) = {
            let state = self.state.read().unwrap();
            let absent = header
                .kid
                .as_ref()
                .is_some_and(|kid| state.absent.contains(kid));
            (Arc::clone(&state.keys), state.loaded, absent)
        };
        if !loaded {
            return Err(JwtError::NotLoaded);
        }
        let candidates: Vec<&Key> = match &header.kid {
            Some(kid) => {
                let named: Vec<&Key> = keys
                    .iter()
                    .filter(|k| k.kid.as_deref() == Some(kid.as_str()))
                    .collect();
                if named.is_empty() {
                    // Missing for sure only when a fetch made for this `kid` has found it
                    // missing; otherwise the set may predate a rotation (ADR 0019, amendment 1).
                    return Err(if absent {
                        JwtError::UnknownKid
                    } else {
                        JwtError::KeyPending
                    });
                }
                named
            }
            None => keys.iter().collect(),
        };
        let mut validation = Validation::new(header.alg);
        validation.leeway = self.settings.leeway.as_secs();
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.set_issuer(&[&self.settings.issuer]);
        validation.required_spec_claims = ["exp", "iss"].map(String::from).into();
        match &self.settings.audience {
            Some(audience) => {
                validation.set_audience(&[audience]);
                validation.required_spec_claims.insert("aud".into());
            }
            None => validation.validate_aud = false,
        }
        for key in candidates.into_iter().filter(|k| k.fits(header.alg)) {
            match jsonwebtoken::decode::<Claims>(token, &key.decoding, &validation) {
                // The signature is checked before the claims: any other error means a key of
                // the set signed this token.
                Err(e) if matches!(e.kind(), ErrorKind::InvalidSignature) => {}
                Err(e) => return Err(claims_error(e.kind())),
                Ok(data) => return self.accept(data.claims, header.kid),
            }
        }
        Err(JwtError::BadSignature)
    }

    /// The checks on the claims of a token whose signature, `iss`, `aud`, `exp` and `nbf` passed.
    fn accept(&self, claims: Claims, kid: Option<String>) -> Result<Verified, JwtError> {
        if let Some(iat) = claims.iat {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0.0, |d| d.as_secs_f64());
            if iat > now + self.settings.leeway.as_secs_f64() {
                return Err(JwtError::NotYetValid);
            }
        }
        if claims.typ.as_deref().is_some_and(|t| t != "Bearer") {
            return Err(JwtError::WrongType);
        }
        let azp = claims
            .azp
            .filter(|a| !a.is_empty())
            .ok_or(JwtError::MissingClaim)?;
        let exp = claims.exp.ok_or(JwtError::MissingClaim)?;
        Ok(Verified {
            azp,
            kid,
            expires_at: UNIX_EPOCH + Duration::from_secs_f64(exp.clamp(0.0, MAX_EXP_SECS)),
        })
    }
}

fn claims_error(kind: &ErrorKind) -> JwtError {
    match kind {
        ErrorKind::ExpiredSignature => JwtError::Expired,
        ErrorKind::ImmatureSignature => JwtError::NotYetValid,
        ErrorKind::InvalidIssuer => JwtError::WrongIssuer,
        ErrorKind::InvalidAudience => JwtError::WrongAudience,
        ErrorKind::MissingRequiredClaim(claim) if claim == "aud" => JwtError::WrongAudience,
        ErrorKind::MissingRequiredClaim(_) => JwtError::MissingClaim,
        ErrorKind::InvalidAlgorithm | ErrorKind::InvalidAlgorithmName => JwtError::BadAlg,
        _ => JwtError::Malformed,
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use crate::telemetry::test_support::{TestMetrics, test_metrics};
    use jsonwebtoken::Algorithm;
    use serde_json::json;

    const ISS: &str = "https://sso.example/realms/snodra";

    struct Fx {
        jwt: Arc<Jwt>,
        server: JwksServer,
        m: TestMetrics,
        stop: CancellationToken,
    }

    impl Drop for Fx {
        fn drop(&mut self) {
            self.stop.cancel();
        }
    }

    impl Fx {
        /// `ensure_key` then `verify`, as the stream does.
        async fn check(&self, token: &str) -> Result<Verified, JwtError> {
            self.jwt.ensure_key(token).await;
            self.jwt.verify(token)
        }

        fn validations(&self, result: &str) -> Option<u64> {
            self.m
                .counter("snodra_server_jwt_validations_total", "result", result)
        }

        fn fetches(&self, result: &str) -> Option<u64> {
            self.m
                .counter("snodra_server_jwks_fetches_total", "result", result)
        }
    }

    fn config(uri: &str) -> Config {
        Config {
            jwt_issuer: Some(ISS.into()),
            jwt_jwks_uri: Some(uri.into()),
            jwt_leeway: Duration::from_secs(30),
            jwks_refresh: Duration::from_secs(3600),
            jwks_refetch_interval: Duration::from_millis(400),
            jwks_fetch_timeout: Duration::from_secs(2),
            ..Config::default()
        }
    }

    /// A hub-side verifier over a server serving `signers`, loaded (its first fetch is done).
    async fn loaded(signers: &[&Signer]) -> Fx {
        loaded_with(signers, |_| {}).await
    }

    async fn loaded_with(signers: &[&Signer], tweak: impl FnOnce(&mut Config)) -> Fx {
        let server = JwksServer::start(signers).await;
        let fx = started(server, tweak);
        eventually("the key set loads", || fx.jwt.is_loaded()).await;
        fx
    }

    fn started(server: JwksServer, tweak: impl FnOnce(&mut Config)) -> Fx {
        let mut config = config(&server.url());
        tweak(&mut config);
        let m = test_metrics();
        let jwt = Jwt::from_config(&config, m.metrics.clone())
            .unwrap()
            .expect("JWT is configured");
        let stop = CancellationToken::new();
        jwt.spawn(stop.clone());
        Fx {
            jwt,
            server,
            m,
            stop,
        }
    }

    async fn eventually(what: &str, mut ready: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !ready() {
            assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    // ---- what a valid token is ----

    #[tokio::test]
    async fn a_valid_es256_token_verifies_and_reports_azp_kid_and_expiry() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let claims = claims(ISS, "orders", 300);
        let exp = claims["exp"].as_i64().unwrap() as u64;
        let v = fx.check(&key.sign(&claims)).await.unwrap();
        assert_eq!(v.azp, "orders");
        assert_eq!(v.kid.as_deref(), Some("k1"));
        assert_eq!(
            v.expires_at,
            SystemTime::UNIX_EPOCH + Duration::from_secs(exp)
        );
        assert_eq!(fx.validations("ok"), Some(1));
    }

    #[tokio::test]
    async fn rs256_and_ps256_and_es384_verify() {
        let rs = Signer::rs256("rs");
        let ps = Signer::rsa("ps", Algorithm::PS256);
        let es = Signer::es384("es");
        let fx = loaded(&[&rs, &ps, &es]).await;
        for key in [&rs, &ps, &es] {
            let v = fx.check(&key.sign(&claims(ISS, "orders", 60))).await;
            assert_eq!(v.map(|v| v.azp), Ok("orders".to_string()), "{:?}", key.alg);
        }
        assert_eq!(fx.validations("ok"), Some(3));
    }

    #[tokio::test]
    async fn a_token_without_kid_is_tried_against_every_key_of_its_family() {
        let a = Signer::es256("a");
        let b = Signer::es256("b");
        let fx = loaded(&[&a, &b]).await;
        assert!(
            fx.check(&b.sign_without_kid(&claims(ISS, "orders", 60)))
                .await
                .is_ok()
        );
        // A key that is not in the set signs nothing it accepts.
        let stranger = Signer::es256("a");
        assert_eq!(
            fx.check(&stranger.sign_without_kid(&claims(ISS, "orders", 60)))
                .await,
            Err(JwtError::BadSignature)
        );
        // The family matters: an RSA-signed token without kid is not tried against EC keys.
        let rsa = Signer::rs256("r");
        assert_eq!(
            fx.check(&rsa.sign_without_kid(&claims(ISS, "orders", 60)))
                .await,
            Err(JwtError::BadSignature)
        );
    }

    #[tokio::test]
    async fn keys_that_cannot_sign_are_skipped() {
        let key = Signer::es256("k1");
        let server = JwksServer::start(&[]).await;
        let mut enc = Signer::rs256("enc").jwk();
        enc["use"] = json!("enc");
        server.set_body(jwks_document(&[
            json!({"kty": "oct", "kid": "hmac", "k": "c2VjcmV0"}),
            json!({"kty": "banana", "kid": "weird"}),
            json!("not even an object"),
            enc,
            key.jwk(),
        ]));
        let fx = started(server, |_| {});
        eventually("loaded", || fx.jwt.is_loaded()).await;
        assert_eq!(fx.jwt.key_count(), 1);
        assert!(
            fx.check(&key.sign(&claims(ISS, "orders", 60)))
                .await
                .is_ok()
        );
    }

    // ---- refused tokens ----

    #[tokio::test]
    async fn alg_none_and_hmac_tokens_are_refused() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let c = claims(ISS, "orders", 60);
        assert_eq!(fx.check(&forge_none(&c)).await, Err(JwtError::BadAlg));
        // HS256 keyed with the public key's coordinates, the algorithm-confusion attack.
        let public = serde_json::to_vec(&key.jwk()).unwrap();
        assert_eq!(
            fx.check(&forge_hs256("k1", &c, &public)).await,
            Err(JwtError::BadAlg)
        );
        assert_eq!(fx.validations("bad_alg"), Some(2));
        assert_eq!(fx.validations("ok"), None);
    }

    #[tokio::test]
    async fn a_token_from_the_wrong_issuer_is_refused() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let t = key.sign(&claims("https://evil.example/realms/snodra", "orders", 60));
        assert_eq!(fx.check(&t).await, Err(JwtError::WrongIssuer));
        // Exact match: a trailing slash is a different issuer.
        let t = key.sign(&claims(&format!("{ISS}/"), "orders", 60));
        assert_eq!(fx.check(&t).await, Err(JwtError::WrongIssuer));
        assert_eq!(fx.validations("wrong_issuer"), Some(2));
    }

    #[tokio::test]
    async fn a_token_without_an_issuer_or_expiry_or_azp_is_refused() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let mut no_iss = claims(ISS, "orders", 60);
        no_iss.as_object_mut().unwrap().remove("iss");
        let mut no_exp = claims(ISS, "orders", 60);
        no_exp.as_object_mut().unwrap().remove("exp");
        let mut no_azp = claims(ISS, "orders", 60);
        no_azp.as_object_mut().unwrap().remove("azp");
        let empty_azp = claims(ISS, "", 60);
        for c in [no_iss, no_exp, no_azp, empty_azp] {
            assert_eq!(
                fx.check(&key.sign(&c)).await,
                Err(JwtError::MissingClaim),
                "{c}"
            );
        }
        assert_eq!(fx.validations("missing_claim"), Some(4));
    }

    #[tokio::test]
    async fn the_audience_is_checked_only_when_one_is_configured() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let mut c = claims(ISS, "orders", 60);
        c["aud"] = json!("someone-else");
        assert!(fx.check(&key.sign(&c)).await.is_ok(), "no audience set");

        let fx = loaded_with(&[&key], |c| c.jwt_audience = Some("snodra-server".into())).await;
        for (aud, ok) in [
            (json!("snodra-server"), true),
            (json!(["account", "snodra-server"]), true),
            (json!("someone-else"), false),
            (json!(["account", "other"]), false),
            (json!([]), false),
        ] {
            let mut c = claims(ISS, "orders", 60);
            c["aud"] = aud.clone();
            let r = fx.check(&key.sign(&c)).await;
            assert_eq!(r.is_ok(), ok, "aud {aud}: {r:?}");
            if !ok {
                assert_eq!(r, Err(JwtError::WrongAudience), "aud {aud}");
            }
        }
        // A token with no aud at all does not have the audience either.
        assert_eq!(
            fx.check(&key.sign(&claims(ISS, "orders", 60))).await,
            Err(JwtError::WrongAudience)
        );
    }

    #[tokio::test]
    async fn expiry_and_not_before_and_issued_at_honour_the_leeway() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        // Expired 10 s ago, leeway 30 s: still good. Expired 60 s ago: not.
        assert!(
            fx.check(&key.sign(&claims(ISS, "orders", -10)))
                .await
                .is_ok()
        );
        assert_eq!(
            fx.check(&key.sign(&claims(ISS, "orders", -60))).await,
            Err(JwtError::Expired)
        );
        for (claim, offset, ok) in [
            ("nbf", 10, true),
            ("nbf", 60, false),
            ("iat", 10, true),
            ("iat", 60, false),
        ] {
            let mut c = claims(ISS, "orders", 300);
            c[claim] = json!(now() + offset);
            let r = fx.check(&key.sign(&c)).await;
            assert_eq!(r.is_ok(), ok, "{claim} +{offset}s: {r:?}");
            if !ok {
                assert_eq!(r, Err(JwtError::NotYetValid), "{claim} +{offset}s");
            }
        }
        // A tighter leeway is honoured.
        let strict = loaded_with(&[&key], |c| c.jwt_leeway = Duration::ZERO).await;
        assert_eq!(
            strict.check(&key.sign(&claims(ISS, "orders", -10))).await,
            Err(JwtError::Expired)
        );
    }

    #[tokio::test]
    async fn only_an_access_token_is_accepted() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        for typ in ["ID", "Refresh", "Offline", "bearer"] {
            let mut c = claims(ISS, "orders", 60);
            c["typ"] = json!(typ);
            assert_eq!(
                fx.check(&key.sign(&c)).await,
                Err(JwtError::WrongType),
                "{typ}"
            );
        }
        // No typ claim at all is fine.
        let mut c = claims(ISS, "orders", 60);
        c.as_object_mut().unwrap().remove("typ");
        assert!(fx.check(&key.sign(&c)).await.is_ok());
        assert_eq!(fx.validations("wrong_type"), Some(4));
    }

    #[tokio::test]
    async fn a_forged_or_tampered_token_fails_its_signature() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let c = claims(ISS, "orders", 60);
        // Same kid, another key.
        let impostor = Signer::es256("k1");
        assert_eq!(
            fx.check(&impostor.sign(&c)).await,
            Err(JwtError::BadSignature)
        );
        // The payload swapped for another service's, the signature kept.
        let good = key.sign(&c);
        let other = key.sign(&claims(ISS, "billing", 60));
        let (head, rest) = good.split_once('.').unwrap();
        let (_, sig) = rest.split_once('.').unwrap();
        let (_, other_payload) = other.split_once('.').unwrap();
        let other_payload = other_payload.split_once('.').unwrap().0;
        let tampered = format!("{head}.{other_payload}.{sig}");
        assert_eq!(fx.check(&tampered).await, Err(JwtError::BadSignature));
        // An RSA key id used with an EC algorithm does not verify.
        let rs = Signer::rs256("k1");
        assert_eq!(fx.check(&rs.sign(&c)).await, Err(JwtError::BadSignature));
        assert_eq!(fx.validations("bad_signature"), Some(3));
    }

    #[tokio::test]
    async fn garbage_is_malformed_and_counted_under_a_fixed_label() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let giant = format!("{}.{}.{}", "a".repeat(10_000), "b".repeat(10_000), "c");
        for junk in [
            "",
            "not a jwt",
            "a.b",
            "a.b.c",
            "a.b.c.d",
            "e30.e30.sig",
            giant.as_str(),
        ] {
            let r = fx.check(junk).await;
            assert!(
                matches!(r, Err(JwtError::Malformed)),
                "{:?} gave {r:?}",
                &junk[..junk.len().min(20)]
            );
        }
        assert_eq!(fx.validations("malformed"), Some(7));
        // No label is made of anything the client sent.
        assert_eq!(
            fx.m.counter_total("snodra_server_jwt_validations_total"),
            Some(7)
        );
    }

    // ---- the key cache ----

    #[tokio::test]
    async fn before_the_first_fetch_succeeds_nothing_verifies() {
        let key = Signer::es256("k1");
        let addr = JwksServer::reserve().await;
        // Nothing listens on `addr` yet: the first fetch is refused.
        let fx = started_at(addr, |_| {});
        tokio::time::sleep(Duration::from_millis(300)).await;
        let t = key.sign(&claims(ISS, "orders", 60));
        assert_eq!(fx.check(&t).await, Err(JwtError::NotLoaded));
        assert_eq!(fx.validations("not_loaded"), Some(1));
        assert!(!fx.jwt.is_loaded());
        assert!(fx.fetches("network").is_some_and(|n| n >= 1));
        // The server comes up: the background retry loads the keys without a restart.
        let _server = JwksServer::start_at(addr, &[&key]).await;
        eventually("the keys load once the server is up", || fx.jwt.is_loaded()).await;
        assert!(fx.check(&t).await.is_ok());
        assert_eq!(fx.fetches("ok"), Some(1));
    }

    fn started_at(addr: std::net::SocketAddr, tweak: impl FnOnce(&mut Config)) -> Fx {
        // A server object that is not bound: only its URL is used.
        let server = JwksServer::unbound(addr);
        started(server, tweak)
    }

    #[tokio::test]
    async fn an_unknown_kid_refetches_once_and_succeeds_after_a_rotation() {
        let old = Signer::es256("old");
        let fx = loaded(&[&old]).await;
        assert_eq!(fx.server.hits(), 1);
        let new = Signer::es256("new");
        // The realm rotates: the next fetch has the new key (and the old one is gone).
        fx.server.serve(&[&new]);
        let t = new.sign(&claims(ISS, "orders", 60));
        assert!(fx.check(&t).await.is_ok());
        assert_eq!(fx.server.hits(), 2, "one extra fetch");
        // The old key left with the rotation. The fetch above was for another kid and is
        // inside the throttle window, so it is not known to be gone yet...
        let gone = old.sign(&claims(ISS, "orders", 60));
        assert_eq!(fx.check(&gone).await, Err(JwtError::KeyPending));
        // ...until a fetch made for it has looked.
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert_eq!(fx.check(&gone).await, Err(JwtError::UnknownKid));
        assert_eq!(fx.validations("unknown_kid"), Some(1));
        assert_eq!(fx.fetches("ok"), Some(3));
    }

    #[tokio::test]
    async fn unknown_kid_refetches_are_throttled() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let stranger = Signer::es256("nope");
        let t = stranger.sign(&claims(ISS, "orders", 60));
        for _ in 0..5 {
            assert_eq!(fx.check(&t).await, Err(JwtError::UnknownKid));
        }
        assert_eq!(
            fx.server.hits(),
            2,
            "the startup fetch and one forced refetch"
        );
        // After the interval one more is allowed (the throttle is per interval, not forever).
        tokio::time::sleep(Duration::from_millis(450)).await;
        for _ in 0..5 {
            assert_eq!(fx.check(&t).await, Err(JwtError::UnknownKid));
        }
        assert_eq!(fx.server.hits(), 3);
    }

    #[tokio::test]
    async fn an_unknown_kid_is_pending_until_a_refetch_has_looked_for_it() {
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let signer = |kid: &str| Signer::es256(kid).sign(&claims(ISS, "orders", 60));
        // This call's own refetch succeeded and the key is still not there: definitive.
        assert_eq!(fx.check(&signer("first")).await, Err(JwtError::UnknownKid));
        // Another kid inside the throttle window: no refetch looked for it, so it may be a
        // rotation that the last fetch (made for another kid) predates.
        assert_eq!(fx.check(&signer("second")).await, Err(JwtError::KeyPending));
        assert_eq!(fx.validations("key_pending"), Some(1));
        // Once the window has passed, its own refetch settles it.
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert_eq!(fx.check(&signer("second")).await, Err(JwtError::UnknownKid));
        // A refetch that fails settles nothing.
        fx.server.set_status(500);
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert_eq!(fx.check(&signer("third")).await, Err(JwtError::KeyPending));
        // Nor does one a refetch already answered stay definitive for a kid that then appears.
        let third = Signer::es256("third");
        fx.server.serve(&[&key, &third]);
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert!(
            fx.check(&third.sign(&claims(ISS, "orders", 60)))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn concurrent_unknown_kids_share_one_fetch() {
        let old = Signer::es256("old");
        let fx = Arc::new(loaded(&[&old]).await);
        let new = Signer::es256("new");
        fx.server.serve(&[&new]);
        fx.server.set_delay(Duration::from_millis(200));
        let t = new.sign(&claims(ISS, "orders", 60));
        let calls: Vec<_> = (0..8)
            .map(|_| {
                let (fx, t) = (Arc::clone(&fx), t.clone());
                tokio::spawn(async move { fx.check(&t).await })
            })
            .collect();
        for call in calls {
            assert!(
                call.await.unwrap().is_ok(),
                "waiters see the fetch's result"
            );
        }
        assert_eq!(fx.server.hits(), 2);
    }

    #[tokio::test]
    async fn the_keys_are_fetched_again_on_the_refresh_interval() {
        let a = Signer::es256("a");
        let fx = loaded_with(&[&a], |c| c.jwks_refresh = Duration::from_millis(150)).await;
        let b = Signer::es256("b");
        fx.server.serve(&[&a, &b]);
        eventually("the refresh picks up the new key", || {
            fx.jwt.key_count() == 2
        })
        .await;
        assert_eq!(fx.m.gauge_plain("snodra_server_jwks_keys"), Some(2));
    }

    #[tokio::test]
    async fn a_failed_fetch_keeps_the_last_good_keys() {
        let key = Signer::es256("k1");
        let fx = loaded_with(&[&key], |c| c.jwks_refresh = Duration::from_millis(100)).await;
        let t = key.sign(&claims(ISS, "orders", 60));
        for (what, break_it, label) in [
            (
                "a 503",
                Box::new(|s: &JwksServer| s.set_status(503)) as Box<dyn Fn(&JwksServer)>,
                "status",
            ),
            (
                "not json",
                Box::new(|s: &JwksServer| s.set_body(b"<html>".to_vec())),
                "invalid",
            ),
            (
                "no keys",
                Box::new(|s: &JwksServer| s.set_body(jwks_document(&[]))),
                "invalid",
            ),
            (
                "only junk",
                Box::new(|s: &JwksServer| s.set_body(jwks_document(&[json!({"kty": "oct"})]))),
                "invalid",
            ),
        ] {
            let before = fx.fetches(label).unwrap_or(0);
            break_it(&fx.server);
            eventually(what, || fx.fetches(label).unwrap_or(0) > before).await;
            assert!(fx.jwt.is_loaded(), "{what}");
            assert_eq!(fx.jwt.key_count(), 1, "{what}");
            assert!(fx.check(&t).await.is_ok(), "{what}");
        }
        // And it recovers.
        let new = Signer::es256("k2");
        fx.server.serve(&[&new]);
        eventually("recovery", || {
            fx.jwt.verify(&new.sign(&claims(ISS, "orders", 60))).is_ok()
        })
        .await;
    }

    #[tokio::test]
    async fn a_failed_status_is_logged_with_its_code() {
        crate::telemetry::test_support::capture_logs();
        let key = Signer::es256("k1");
        let server = JwksServer::start(&[&key]).await;
        server.set_status(503);
        let fx = started(server, |_| {});
        eventually("the 503 is counted", || fx.fetches("status").is_some()).await;
        let warn = crate::telemetry::test_support::logged()
            .into_iter()
            .find(|l| l.message.contains("jwks fetch failed"))
            .expect("the failure is logged");
        assert_eq!(warn.field("reason"), Some("status"));
        assert!(
            warn.field("detail").is_some_and(|d| d.contains("503")),
            "{warn:?}"
        );
    }

    #[tokio::test]
    async fn an_oversized_or_slow_response_is_a_failed_fetch() {
        let key = Signer::es256("k1");
        let fx = loaded_with(&[&key], |c| {
            c.jwks_refresh = Duration::from_millis(100);
            c.jwks_fetch_timeout = Duration::from_millis(300);
        })
        .await;
        // Over the cap (the cap is on the bytes, whatever the document says).
        let filler = "x".repeat(MAX_JWKS_BYTES);
        fx.server.set_body(
            json!({"keys": [key.jwk()], "pad": filler})
                .to_string()
                .into_bytes(),
        );
        eventually("too large", || fx.fetches("too_large").is_some()).await;
        assert_eq!(fx.jwt.key_count(), 1);
        // Slower than the fetch time limit.
        fx.server.serve(&[&key]);
        fx.server.set_delay(Duration::from_secs(2));
        eventually("timeout", || fx.fetches("timeout").is_some()).await;
        assert!(fx.jwt.is_loaded());
    }

    #[tokio::test]
    async fn the_keys_can_be_fetched_over_tls_with_an_extra_ca() {
        let key = Signer::es256("k1");
        let (ca, cert, private) = tls_pair();
        let server = JwksServer::start_tls(&cert, &private, &[&key]).await;
        let dir = std::env::temp_dir().join(format!("kraken-jwks-ca-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca_file = dir.join("ca.pem");
        std::fs::write(&ca_file, ca).unwrap();
        // Without the CA the platform roots do not trust it.
        let untrusted = started(
            JwksServer::unbound(server.addr).with_scheme("https"),
            |_| {},
        );
        crate::telemetry::test_support::capture_logs();
        eventually("an untrusted certificate fails", || {
            untrusted.fetches("tls").is_some()
        })
        .await;
        assert_eq!(
            untrusted.fetches("network"),
            None,
            "a trust failure is not the network"
        );
        let warn = crate::telemetry::test_support::logged()
            .into_iter()
            .find(|l| l.message.contains("jwks fetch failed"))
            .expect("the failure is logged");
        assert_eq!(warn.field("reason"), Some("tls"));
        // The rustls error says why (an unknown issuer here), and the URI is in the line.
        assert!(
            warn.field("detail")
                .is_some_and(|d| d.contains("UnknownIssuer")),
            "{warn:?}"
        );
        assert!(!untrusted.jwt.is_loaded());
        let trusted = started(JwksServer::unbound(server.addr).with_scheme("https"), |c| {
            c.jwt_jwks_ca = Some(ca_file.clone());
        });
        eventually("the extra CA is trusted", || trusted.jwt.is_loaded()).await;
        assert!(
            trusted
                .check(&key.sign(&claims(ISS, "orders", 60)))
                .await
                .is_ok()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn the_keys_can_be_fetched_from_an_ipv6_literal() {
        let key = Signer::es256("k1");
        let Some(server) = JwksServer::start_ipv6(&[&key]).await else {
            eprintln!("no IPv6 loopback on this host: skipped");
            return;
        };
        assert!(server.url().contains("[::1]"), "{}", server.url());
        let fx = started(server, |_| {});
        eventually("the keys load from [::1]", || fx.jwt.is_loaded()).await;
        assert_eq!(fx.fetches("ok"), Some(1));
    }

    #[tokio::test]
    async fn the_keys_can_be_fetched_over_tls_from_an_ipv6_literal() {
        let key = Signer::es256("k1");
        let (ca, cert, private) = tls_pair();
        let Some(server) = JwksServer::start_tls_on("[::1]:0", &cert, &private, &[&key]).await
        else {
            eprintln!("no IPv6 loopback on this host: skipped");
            return;
        };
        let dir = std::env::temp_dir().join(format!("kraken-jwks-ca-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let ca_file = dir.join("ca.pem");
        std::fs::write(&ca_file, ca).unwrap();
        let fx = started(server, |c| c.jwt_jwks_ca = Some(ca_file.clone()));
        eventually("the keys load over TLS from [::1]", || fx.jwt.is_loaded()).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_missing_ca_file_is_a_startup_error() {
        let config = Config {
            jwt_jwks_ca: Some("/nonexistent/ca.pem".into()),
            ..config("https://sso.example/certs")
        };
        let err = Jwt::from_config(&config, test_metrics().metrics.clone())
            .err()
            .expect("an error");
        assert!(
            format!("{err:#}").contains("SNODRA_SERVER_JWT_JWKS_CA"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn an_unreadable_ca_file_is_a_startup_error_even_with_an_http_uri() {
        // hub.md: "an unreadable file is a startup error"; the scheme must not hide it.
        for ca in ["/nonexistent/ca.pem", "/dev/null"] {
            let config = Config {
                jwt_jwks_ca: Some(ca.into()),
                ..config("http://keycloak:8080/certs")
            };
            let err = Jwt::from_config(&config, test_metrics().metrics.clone())
                .err()
                .unwrap_or_else(|| panic!("{ca}: an unusable CA over http is accepted"));
            assert!(
                format!("{err:#}").contains("SNODRA_SERVER_JWT_JWKS_CA"),
                "{err:#}"
            );
        }
    }

    #[tokio::test]
    async fn without_an_issuer_there_is_no_verifier() {
        let jwt = Jwt::from_config(&Config::default(), test_metrics().metrics.clone()).unwrap();
        assert!(jwt.is_none());
    }

    #[tokio::test]
    async fn a_token_is_never_logged() {
        crate::telemetry::test_support::capture_logs();
        let key = Signer::es256("k1");
        let fx = loaded(&[&key]).await;
        let good = key.sign(&claims(ISS, "orders", 60));
        let bad = Signer::es256("k1").sign(&claims(ISS, "orders", 60));
        let _ = fx.check(&good).await;
        let _ = fx.check(&bad).await;
        let body = good.split('.').nth(1).unwrap().to_string();
        for line in crate::telemetry::test_support::logged() {
            let text = format!("{} {:?}", line.message, line.fields);
            for secret in [&good, &bad, &body] {
                assert!(!text.contains(secret.as_str()), "{text}");
            }
        }
    }
}
