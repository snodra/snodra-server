//! Hub access control (roadmap P5a, ADR 0018): who may register as which service, which types a
//! session may handle and which it may send.
//!
//! Three pieces:
//!
//! * [`Acl`], parsed from the TOML file `KRAKEN_HUB_ACL_FILE`. Entries are keyed by the SHA-256
//!   of a static token, so the file never holds a token. Lookups compare digests with
//!   [`ct::eq`] against every entry, with no early exit.
//! * [`Authorizer`]: the mode (`off`, `permissive`, `enforce`), the current ACL (swapped when the
//!   file changes, see [`crate::reload`]) and the checks: [`Authorizer::admit`] for a `Register`,
//!   [`Authorizer::check_send`] for every request, [`Authorizer::recheck`] for a session that
//!   already exists when the ACL changes.
//! * [`Secret`]: a token the hub compares but never keeps (the dashboard token).
//!
//! Tokens are never logged and never stored: a token is hashed as soon as it is read, and logs
//! carry at most [`short`] (the first 8 hex characters of the digest). Every credential kind
//! produces a [`Principal`]: who the credential speaks for and the ACL entry it resolves through
//! (a token's digest, or a service name for a JWT, ADR 0019). A session keeps its principal
//! ([`crate::session::SessionMeta::principal`]) and every later check looks the entry up again,
//! so a reload takes effect on live sessions. A client certificate (P5c, ADR 0020) is the third
//! kind: the handshake proves it, [`crate::mtls::identify`] reads the service from it, and
//! [`Authorizer::admit`] resolves that service through its `[[service]]` table.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

use anyhow::Context;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::jwt::{Jwt, JwtError};
use crate::mtls::{CertError, ClientCert};
use crate::proto::{Credentials, credentials};
use crate::telemetry::Metrics;

/// SHA-256 of a token.
pub type Digest = [u8; 32];

/// The only context contexts-aware ACL files may name (contexts are deferred, roadmap P8).
pub const DEFAULT_CONTEXT: &str = "default";

/// The reserved namespace of the event browser's query types (ADR 0027): only a session whose
/// ACL entry has `event-browser = true` may register a type in it, whatever its `handle` says.
pub const EVENTS_NAMESPACE: &str = "kraken.events.";

/// True when `type_name` is in [`EVENTS_NAMESPACE`] (the namespace itself is not a type).
pub fn is_events_type(type_name: &str) -> bool {
    type_name.len() > EVENTS_NAMESPACE.len() && type_name.starts_with(EVENTS_NAMESPACE)
}

/// Constant-time comparison, the only way this module (and the dashboard token check) compares
/// secret-derived bytes.
pub mod ct {
    use subtle::ConstantTimeEq;

    #[cfg(test)]
    thread_local! {
        /// Comparisons made on this thread; lets a test prove the lookups go through [`eq`].
        pub static COMPARISONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    /// True when `a == b`, in time that does not depend on where they differ.
    pub fn eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
        #[cfg(test)]
        COMPARISONS.with(|c| c.set(c.get() + 1));
        a.ct_eq(b).into()
    }
}

/// SHA-256 of `token`.
pub fn digest_of(token: &str) -> Digest {
    Sha256::digest(token.as_bytes()).into()
}

/// Lowercase hex of a digest.
pub fn to_hex(digest: &Digest) -> String {
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// The hex digest `kraken-hub acl hash` prints.
pub fn hash_token(token: &str) -> String {
    to_hex(&digest_of(token))
}

/// The 8-hex-character prefix of a digest: the only form of a token's identity that may be logged.
pub fn short(digest: &Digest) -> String {
    to_hex(digest)[..8].to_string()
}

/// The longest client-chosen name (a service name, a type) that is logged or sent back.
pub const MAX_LOGGED_CHARS: usize = 128;

/// `s` cut to [`MAX_LOGGED_CHARS`] characters. A client chooses the service name of a `Register`
/// before anything is checked, so a log line or an error message never carries it whole.
pub fn clip(s: &str) -> String {
    match s.char_indices().nth(MAX_LOGGED_CHARS) {
        Some((end, _)) => s[..end].to_string(),
        None => s.to_string(),
    }
}

/// Refusal lines allowed per reason in one [`LOG_WINDOW`].
const LOG_BURST: u32 = 10;
/// The window of the refusal log rate limit.
const LOG_WINDOW: Duration = Duration::from_secs(10);

/// What to do with one refusal log line (see [`LogLimiter::gate`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Gate {
    /// Log it.
    Log,
    /// The first line over the budget: log one notice instead, then stay quiet.
    Notice,
    /// Over the budget, already announced: log nothing.
    Suppress,
    /// A new window begins: log a summary of how many lines the last one dropped, then the line.
    LogAfterSummary(u64),
}

struct LogWindow {
    started: Instant,
    logged: u32,
    suppressed: u64,
}

/// At most [`LOG_BURST`] refusal lines per reason per [`LOG_WINDOW`], so a client that retries a
/// refused `Register` in a loop cannot fill the disk or drown the other lines. The metrics keep
/// counting every refusal; only the log lines are dropped.
#[derive(Default)]
struct LogLimiter {
    windows: Mutex<HashMap<&'static str, LogWindow>>,
}

impl LogLimiter {
    fn gate(&self, reason: &'static str, now: Instant) -> Gate {
        let mut windows = self.windows.lock().unwrap();
        let w = windows.entry(reason).or_insert(LogWindow {
            started: now,
            logged: 0,
            suppressed: 0,
        });
        if now.saturating_duration_since(w.started) >= LOG_WINDOW {
            let dropped = w.suppressed;
            *w = LogWindow {
                started: now,
                logged: 1,
                suppressed: 0,
            };
            return if dropped > 0 {
                Gate::LogAfterSummary(dropped)
            } else {
                Gate::Log
            };
        }
        if w.logged < LOG_BURST {
            w.logged += 1;
            Gate::Log
        } else if w.suppressed == 0 {
            w.suppressed = 1;
            Gate::Notice
        } else {
            w.suppressed += 1;
            Gate::Suppress
        }
    }
}

fn from_hex(s: &str) -> Option<Digest> {
    if s.len() != 64 || !s.is_ascii() {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

// ---------------------------------------------------------------------------------------------
// Mode

/// `KRAKEN_HUB_AUTH_MODE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    /// No checks.
    Off,
    /// Every rule is evaluated; violations are logged and counted, everything is allowed.
    Permissive,
    /// Violations are refused.
    Enforce,
}

impl AuthMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Permissive => "permissive",
            Self::Enforce => "enforce",
        }
    }
}

impl FromStr for AuthMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" => Ok(Self::Off),
            "permissive" => Ok(Self::Permissive),
            "enforce" => Ok(Self::Enforce),
            other => {
                anyhow::bail!("unknown auth mode {other:?} (expected off, permissive or enforce)")
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Secrets

/// A token the hub only compares (the dashboard token). Holds its digest, never the token;
/// `Debug` shows nothing. Clones share one digest cell, so replacing it with [`Secret::set`]
/// (when the token file changes) reaches every holder, such as the HTTP guard built at startup.
#[derive(Clone)]
pub struct Secret {
    digest: Arc<RwLock<Digest>>,
}

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        self.current() == other.current()
    }
}

impl Eq for Secret {}

impl Secret {
    pub fn new(token: &str) -> Self {
        Self {
            digest: Arc::new(RwLock::new(digest_of(token))),
        }
    }

    fn current(&self) -> Digest {
        *self.digest.read().unwrap()
    }

    /// Replaces the token (every clone sees the new one). Returns whether it changed.
    pub fn set(&self, token: &str) -> bool {
        let new = digest_of(token);
        let mut current = self.digest.write().unwrap();
        let changed = !ct::eq(&current, &new);
        *current = new;
        changed
    }

    /// True when `presented` is the token. Both sides are hashed first, so the comparison time
    /// depends neither on the token's length nor on where it differs, and an absent token costs
    /// the same as a wrong one.
    pub fn matches(&self, presented: Option<&str>) -> bool {
        let equal = ct::eq(&self.current(), &digest_of(presented.unwrap_or("")));
        equal & presented.is_some()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(..)")
    }
}

// ---------------------------------------------------------------------------------------------
// ACL

/// A type pattern: an exact name, `prefix.*` (every type that starts with `prefix.`) or `*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pattern {
    Any,
    /// Stored with its trailing dot: `files.`.
    Prefix(String),
    Exact(String),
}

impl Pattern {
    pub fn parse(s: &str) -> Result<Self, String> {
        if s.is_empty() {
            return Err("empty pattern".into());
        }
        if s.chars().any(char::is_whitespace) {
            return Err(format!("pattern {s:?} contains whitespace"));
        }
        if s == "*" {
            return Ok(Self::Any);
        }
        if let Some(prefix) = s.strip_suffix('*') {
            if prefix.len() < 2 || !prefix.ends_with('.') || prefix.contains('*') {
                return Err(format!(
                    "pattern {s:?}: a wildcard must be `*` or `prefix.*`"
                ));
            }
            return Ok(Self::Prefix(prefix.to_string()));
        }
        if s.contains('*') {
            return Err(format!(
                "pattern {s:?}: a wildcard must be `*` or `prefix.*`"
            ));
        }
        Ok(Self::Exact(s.to_string()))
    }

    pub fn matches(&self, type_name: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Prefix(p) => type_name.len() > p.len() && type_name.starts_with(p.as_str()),
            Self::Exact(e) => e == type_name,
        }
    }
}

fn any_matches(patterns: &[Pattern], type_name: &str) -> bool {
    patterns.iter().any(|p| p.matches(type_name))
}

/// What one token may do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclEntry {
    pub digest: Digest,
    /// The service name the token may register as.
    pub service: String,
    /// Types it may register as a handler: commands, queries and shared queries.
    pub handle: Vec<Pattern>,
    /// Types it may send, in every request mode.
    pub send: Vec<Pattern>,
    /// The token may also call the hub's processor controls (ADR 0023).
    pub control: bool,
    /// The token may register the event browser's `kraken.events.*` types (ADR 0027).
    pub event_browser: bool,
    /// Free text for the operator ("rotation 2026-11"); never used for decisions.
    pub label: Option<String>,
}

impl AclEntry {
    fn rights(&self) -> Rights<'_> {
        Rights {
            service: &self.service,
            handle: &self.handle,
            send: &self.send,
            control: self.control,
            event_browser: self.event_browser,
        }
    }
}

/// What an ACL entry grants, whichever table it came from.
#[derive(Debug, Clone, Copy)]
pub struct Rights<'a> {
    /// The service the credential may act as.
    pub service: &'a str,
    pub handle: &'a [Pattern],
    pub send: &'a [Pattern],
    /// The processor controls of the HTTP API (ADR 0023).
    pub control: bool,
    /// Registering the event browser's `kraken.events.*` types (ADR 0027); `handle` never
    /// grants them.
    pub event_browser: bool,
}

/// The ACL entry a credential resolves through.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AclKey {
    /// A static token: the digest of a `[[token]]` entry.
    Token(Digest),
    /// A JWT: the service name of a `[[service]]` entry (the token's `azp`).
    Service(String),
}

/// How a credential was presented.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CredentialKind {
    Token,
    Jwt,
    /// A client certificate presented in the TLS handshake (ADR 0020).
    Certificate,
}

impl CredentialKind {
    /// `token`, `jwt` or `certificate`, as the topology shows it.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Token => "token",
            Self::Jwt => "jwt",
            Self::Certificate => "certificate",
        }
    }
}

/// Who a valid credential speaks for: what every credential kind produces, and what a session
/// keeps instead of the credential (which the hub never stores).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    /// The service the credential authenticates: the `service` of a token's entry, or a JWT's
    /// `azp`.
    pub service: String,
    /// The entry later checks look up in the current ACL.
    pub key: AclKey,
    /// When a JWT expires (`exp`) or a certificate ends (`notAfter`); `None` for a token, which
    /// does not.
    pub expires_at: Option<SystemTime>,
    /// How the credential was presented.
    kind: CredentialKind,
    /// The first 8 hex characters of a certificate's SHA-256 fingerprint.
    pub fingerprint: Option<String>,
}

impl Principal {
    /// The principal of a token whose entry grants `service`.
    pub fn token(digest: Digest, service: &str) -> Self {
        Self {
            service: service.to_string(),
            key: AclKey::Token(digest),
            expires_at: None,
            kind: CredentialKind::Token,
            fingerprint: None,
        }
    }

    /// The principal of a JWT whose `azp` is `service`, valid until `expires_at`.
    pub fn jwt(service: &str, expires_at: SystemTime) -> Self {
        Self {
            service: service.to_string(),
            key: AclKey::Service(service.to_string()),
            expires_at: Some(expires_at),
            kind: CredentialKind::Jwt,
            fingerprint: None,
        }
    }

    /// The principal of a client certificate (ADR 0020): its service's `[[service]]` table is
    /// its entry, and it ends at `notAfter`.
    pub fn certificate(cert: &ClientCert) -> Self {
        Self {
            service: cert.service.clone(),
            key: AclKey::Service(cert.service.clone()),
            expires_at: Some(cert.not_after),
            kind: CredentialKind::Certificate,
            fingerprint: Some(cert.short()),
        }
    }

    pub fn kind(&self) -> CredentialKind {
        self.kind
    }

    /// How logs name the credential: the first 8 hex characters of a token's digest, the service
    /// of a JWT's entry (clipped), or `cert:` and the first 8 hex characters of a certificate's
    /// fingerprint. Never the credential itself.
    pub fn label(&self) -> String {
        match (&self.key, &self.fingerprint) {
            (AclKey::Token(digest), _) => short(digest),
            (AclKey::Service(_), Some(fingerprint)) => format!("cert:{fingerprint}"),
            (AclKey::Service(name), None) => clip(name),
        }
    }
}

/// What one service's JWTs may do (ADR 0019): the `[[service]]` table named after the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceEntry {
    /// The service name, which is the `azp` of its tokens.
    pub name: String,
    pub handle: Vec<Pattern>,
    pub send: Vec<Pattern>,
    /// The service's JWTs may also call the hub's processor controls (ADR 0023).
    pub control: bool,
    /// The service may register the event browser's `kraken.events.*` types (ADR 0027).
    pub event_browser: bool,
}

impl ServiceEntry {
    fn rights(&self) -> Rights<'_> {
        Rights {
            service: &self.name,
            handle: &self.handle,
            send: &self.send,
            control: self.control,
            event_browser: self.event_browser,
        }
    }
}

/// The parsed ACL file.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Acl {
    entries: Vec<AclEntry>,
    services: Vec<ServiceEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAcl {
    #[serde(default)]
    token: Vec<RawEntry>,
    #[serde(default)]
    service: Vec<RawService>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawService {
    name: String,
    #[serde(default)]
    handle: Vec<String>,
    #[serde(default)]
    send: Vec<String>,
    #[serde(default)]
    control: bool,
    #[serde(default, rename = "event-browser")]
    event_browser: bool,
    contexts: Option<Vec<String>>,
}

/// `contexts` other than `["default"]` is an error: contexts are deferred indefinitely.
fn check_contexts(at: &str, contexts: Option<&Vec<String>>) -> anyhow::Result<()> {
    if let Some(contexts) = contexts
        && !(contexts.is_empty() || contexts.iter().all(|c| c == DEFAULT_CONTEXT))
    {
        anyhow::bail!(
            "{at}: contexts other than [\"default\"] are not supported (contexts are deferred)"
        );
    }
    Ok(())
}

fn parse_patterns(at: &str, what: &str, list: &[String]) -> anyhow::Result<Vec<Pattern>> {
    list.iter()
        .map(|p| Pattern::parse(p).map_err(|m| anyhow::anyhow!("{at}: {what}: {m}")))
        .collect()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawEntry {
    token_sha256: String,
    service: String,
    #[serde(default)]
    handle: Vec<String>,
    #[serde(default)]
    send: Vec<String>,
    #[serde(default)]
    control: bool,
    #[serde(default, rename = "event-browser")]
    event_browser: bool,
    contexts: Option<Vec<String>>,
    label: Option<String>,
}

impl Acl {
    /// Parses the TOML text of an ACL file. Unknown keys are errors (a misspelt `send` must not
    /// silently grant nothing, or something else); so are duplicate token digests, an empty
    /// service name, a bad pattern and `contexts` other than `["default"]` (contexts are deferred
    /// indefinitely, roadmap P8).
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let raw: RawAcl = toml::from_str(text).context("invalid ACL file")?;
        let mut entries = Vec::with_capacity(raw.token.len());
        let mut seen = HashSet::new();
        for (n, e) in raw.token.into_iter().enumerate() {
            let at = format!("token #{}", n + 1);
            let hex = e.token_sha256.trim().to_ascii_lowercase();
            let digest = from_hex(&hex)
                .with_context(|| format!("{at}: token_sha256 must be 64 hex characters"))?;
            if !seen.insert(digest) {
                anyhow::bail!("{at}: duplicate token_sha256 {}", short(&digest));
            }
            let at = format!("token {} ({})", short(&digest), e.service);
            if e.service.trim().is_empty() {
                anyhow::bail!("{at}: service must not be empty");
            }
            check_contexts(&at, e.contexts.as_ref())?;
            entries.push(AclEntry {
                digest,
                handle: parse_patterns(&at, "handle", &e.handle)?,
                send: parse_patterns(&at, "send", &e.send)?,
                control: e.control,
                event_browser: e.event_browser,
                label: e.label,
                service: e.service,
            });
        }
        let mut services = Vec::with_capacity(raw.service.len());
        let mut names = HashSet::new();
        for (n, e) in raw.service.into_iter().enumerate() {
            let at = format!("service #{} ({})", n + 1, clip(&e.name));
            if e.name.trim().is_empty() {
                anyhow::bail!("{at}: name must not be empty");
            }
            if !names.insert(e.name.clone()) {
                anyhow::bail!("{at}: duplicate [[service]] name");
            }
            check_contexts(&at, e.contexts.as_ref())?;
            services.push(ServiceEntry {
                handle: parse_patterns(&at, "handle", &e.handle)?,
                send: parse_patterns(&at, "send", &e.send)?,
                control: e.control,
                event_browser: e.event_browser,
                name: e.name,
            });
        }
        Ok(Self { entries, services })
    }

    pub fn entries(&self) -> &[AclEntry] {
        &self.entries
    }

    /// The entry of `digest`. Every entry is compared in constant time and the loop never exits
    /// early, so neither a match nor its position shows in the time taken.
    pub fn lookup(&self, digest: &Digest) -> Option<&AclEntry> {
        let mut found = None;
        for entry in &self.entries {
            if ct::eq(&entry.digest, digest) {
                found = Some(entry);
            }
        }
        found
    }

    /// The `[[service]]` tables.
    pub fn services(&self) -> &[ServiceEntry] {
        &self.services
    }

    /// True when some entry, of either kind, is for `service`.
    pub fn has_service(&self, service: &str) -> bool {
        self.entries.iter().any(|e| e.service == service)
            || self.services.iter().any(|e| e.name == service)
    }

    /// What the entry `key` names grants now, `None` when the file no longer has it.
    pub fn rights(&self, key: &AclKey) -> Option<Rights<'_>> {
        match key {
            AclKey::Token(digest) => self.lookup(digest).map(AclEntry::rights),
            AclKey::Service(name) => self
                .services
                .iter()
                .find(|e| e.name == *name)
                .map(ServiceEntry::rights),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Decisions

/// Why a check failed; the `reason` label of `kraken_hub_auth_violations_total`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denied {
    pub reason: &'static str,
    pub code: DeniedCode,
    /// Safe to send to the client: names the service or type, never a token.
    pub message: String,
}

/// The gRPC status of a refused `Register` and the `ErrorCode` of a refused request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeniedCode {
    Unauthenticated,
    PermissionDenied,
    /// The hub cannot decide yet (the JWKS is not loaded): the client retries.
    Unavailable,
}

impl Denied {
    fn new(reason: &'static str, code: DeniedCode, message: impl Into<String>) -> Self {
        Self {
            reason,
            code,
            message: message.into(),
        }
    }

    /// The status a stream is closed with.
    pub fn to_status(&self) -> Status {
        match self.code {
            DeniedCode::Unauthenticated => Status::unauthenticated(self.message.clone()),
            DeniedCode::PermissionDenied => Status::permission_denied(self.message.clone()),
            DeniedCode::Unavailable => Status::unavailable(self.message.clone()),
        }
    }

    /// The `Error.code` a refused request is answered with.
    pub fn error_code(&self) -> crate::proto::ErrorCode {
        match self.code {
            DeniedCode::Unauthenticated => crate::proto::ErrorCode::Unauthenticated,
            DeniedCode::PermissionDenied => crate::proto::ErrorCode::PermissionDenied,
            // Only a Register or a refresh is ever unavailable, never a request; the nearest
            // code a request could carry.
            DeniedCode::Unavailable => crate::proto::ErrorCode::HandlerError,
        }
    }
}

/// What [`Authorizer::refresh`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refresh {
    /// Adopt the new principal and status; the session's expiry moves with it.
    Accepted {
        principal: Arc<Principal>,
        status: AuthStatus,
    },
    /// Permissive mode: the refresh was refused, counted and logged; the session keeps what it
    /// had.
    Kept,
    /// Enforce mode: close the stream with it (already counted and logged).
    Close(Denied),
}

/// What [`Authorizer::expired`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expiry {
    /// Enforce mode: close the stream with it (already counted and logged).
    Close(Denied),
    /// Permissive mode: counted and logged, the session stays; ask once.
    Keep,
    /// Not expired by the wall clock yet (the clock stepped back, or the timer only woke to look
    /// again): nothing counted; arm the timer again.
    NotYet,
}

/// At most one refresh in [`REFRESH_MIN_INTERVAL`] is looked at per session, so a client cannot
/// make the hub verify signatures in a loop.
#[derive(Debug, Default)]
pub struct RefreshGate {
    last: Option<Instant>,
}

/// A JWT whose expiry is further away than this is never timed (nothing runs that long, and
/// the timer's arithmetic stays far from overflow).
const MAX_EXPIRY_WAIT: Duration = Duration::from_secs(10 * 365 * 24 * 3600);

/// The longest the expiry timer waits before comparing `exp` with the wall clock again.
const EXPIRY_RECHECK: Duration = Duration::from_secs(60);

/// The shortest time between two refreshes of one session that are both looked at.
pub const REFRESH_MIN_INTERVAL: Duration = Duration::from_secs(1);

impl RefreshGate {
    /// True when a refresh arriving at `now` is to be looked at; a throttled one does not count
    /// as the previous one.
    pub fn allow(&mut self, now: Instant) -> bool {
        if self
            .last
            .is_some_and(|last| now.saturating_duration_since(last) < REFRESH_MIN_INTERVAL)
        {
            return false;
        }
        self.last = Some(now);
        true
    }
}

/// What [`Authorizer::report`] did about a session's violations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// The session was closed (counted).
    Closed,
    /// A re-register was allowed in permissive mode (counted).
    Allowed,
    /// The session was kept because the mode is permissive (logged, not counted again).
    Kept,
}

/// True when `credentials` carries a token or a JWT (an empty token is none).
fn credentials_present(credentials: Option<&Credentials>) -> bool {
    match credentials.and_then(|c| c.kind.as_ref()) {
        Some(credentials::Kind::Token(token)) => !token.is_empty(),
        Some(credentials::Kind::Jwt(_)) => true,
        None => false,
    }
}

/// Three non-empty dot-separated segments: worth verifying as a JWT. Anything else is a static
/// token, which is not counted as a malformed JWT.
fn looks_like_jwt(token: &str) -> bool {
    let mut parts = token.split('.');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(a), Some(b), Some(c), None) if !a.is_empty() && !b.is_empty() && !c.is_empty()
    )
}

/// The token of an `Authorization: Bearer <token>` value (the scheme is case-insensitive).
pub fn bearer(value: &str) -> Option<&str> {
    let (scheme, token) = value.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| token.trim())
        .filter(|t| !t.is_empty())
}

/// How a session authenticated; shown in the topology and the dashboard.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum AuthStatus {
    /// Mode `off`: nothing was checked.
    #[default]
    Off,
    /// A known token; `service` is the one the ACL grants it.
    Authenticated { service: String },
    /// Mode `permissive` and no valid credentials: allowed, but `enforce` would refuse it.
    UnauthenticatedPermissive,
}

impl AuthStatus {
    /// `authenticated`, `unauthenticated (permissive)`; `None` when nothing was checked.
    pub fn label(&self) -> Option<&'static str> {
        match self {
            Self::Off => None,
            Self::Authenticated { .. } => Some("authenticated"),
            Self::UnauthenticatedPermissive => Some("unauthenticated (permissive)"),
        }
    }

    pub fn service(&self) -> Option<&str> {
        match self {
            Self::Authenticated { service } => Some(service),
            _ => None,
        }
    }
}

/// The result of [`Authorizer::admit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    /// Who the credentials speak for when their entry is in the ACL; what later request checks
    /// look up.
    pub principal: Option<Arc<Principal>>,
    pub status: AuthStatus,
    /// Set only in mode `enforce`: close the stream with it.
    pub refused: Option<Denied>,
}

/// What a bearer credential may do with the hub's processor controls (ADR 0023).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlRight {
    /// An ACL credential whose entry has `control = true`; the service it names.
    Granted(String),
    /// A valid ACL credential whose entry does not grant control; the service it names.
    Denied(String),
    /// Not an ACL credential this hub accepts (or access control is off).
    Unknown,
}

// ---------------------------------------------------------------------------------------------
// Authorizer

pub struct Authorizer {
    mode: AuthMode,
    acl: RwLock<Arc<Acl>>,
    generation: AtomicU64,
    path: Option<PathBuf>,
    metrics: Arc<Metrics>,
    limiter: LogLimiter,
    /// Validates JWT credentials; `None` when `KRAKEN_HUB_JWT_ISSUER` is unset.
    jwt: Option<Arc<Jwt>>,
}

impl Authorizer {
    /// An authorizer that checks nothing.
    pub fn off(metrics: Arc<Metrics>) -> Arc<Self> {
        Self::with_acl(AuthMode::Off, Acl::default(), metrics)
    }

    /// An authorizer over an ACL held in memory (no file to reload).
    pub fn with_acl(mode: AuthMode, acl: Acl, metrics: Arc<Metrics>) -> Arc<Self> {
        Self::with_acl_and_jwt(mode, acl, metrics, None)
    }

    /// Like [`Authorizer::with_acl`], validating JWT credentials with `jwt`.
    pub fn with_acl_and_jwt(
        mode: AuthMode,
        acl: Acl,
        metrics: Arc<Metrics>,
        jwt: Option<Arc<Jwt>>,
    ) -> Arc<Self> {
        metrics.acl_generation_set(1);
        Arc::new(Self {
            mode,
            acl: RwLock::new(Arc::new(acl)),
            generation: AtomicU64::new(1),
            path: None,
            metrics,
            limiter: LogLimiter::default(),
            jwt,
        })
    }

    /// Reads the mode and the ACL file named by `config`; a bad file is fatal at startup.
    pub fn from_config(config: &Config, metrics: Arc<Metrics>) -> anyhow::Result<Arc<Self>> {
        let Some(path) = config
            .acl_file
            .clone()
            .filter(|_| config.auth_mode != AuthMode::Off)
        else {
            return Ok(Self::off(metrics));
        };
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("read KRAKEN_HUB_ACL_FILE {}", path.display()))?;
        let acl =
            Acl::parse(&text).with_context(|| format!("KRAKEN_HUB_ACL_FILE {}", path.display()))?;
        info!(
            mode = config.auth_mode.as_str(),
            file = %path.display(),
            tokens = acl.entries().len(),
            "access control"
        );
        if acl.entries().is_empty() {
            warn!("the ACL has no tokens: in enforce mode no service can register");
        }
        let jwt = Jwt::from_config(config, Arc::clone(&metrics))?;
        if jwt.is_some() {
            info!(
                issuer = config.jwt_issuer.as_deref().unwrap_or_default(),
                jwks_uri = config.jwt_jwks_uri.as_deref().unwrap_or_default(),
                audience = config.jwt_audience.as_deref().unwrap_or("-"),
                services = acl.services().len(),
                "jwt credentials"
            );
            if acl.services().is_empty() {
                warn!(
                    "JWT credentials are configured but the ACL has no [[service]] tables: no JWT can register until one is added"
                );
            }
        }
        metrics.acl_generation_set(1);
        Ok(Arc::new(Self {
            mode: config.auth_mode,
            acl: RwLock::new(Arc::new(acl)),
            generation: AtomicU64::new(1),
            path: Some(path),
            jwt,
            metrics,
            limiter: LogLimiter::default(),
        }))
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
    }

    /// [`Authorizer::prepare`] for a bearer token that may be a JWT (the gRPC `Topology` call).
    pub async fn prepare_bearer(&self, presented: Option<&str>) {
        if let (Some(jwt), Some(token)) = (&self.jwt, presented)
            && self.mode != AuthMode::Off
            && looks_like_jwt(token)
        {
            jwt.ensure_key(token).await;
        }
    }

    /// Makes the key cache ready for the JWT in `credentials` (a rotated key is fetched), so the
    /// synchronous [`Authorizer::admit`] and [`Authorizer::check_topology`] can judge it. Call it
    /// first for credentials that came from a client; it does nothing for a token.
    pub async fn prepare(&self, credentials: Option<&Credentials>) {
        if let (Some(jwt), Some(credentials::Kind::Jwt(token))) =
            (&self.jwt, credentials.and_then(|c| c.kind.as_ref()))
            && self.mode != AuthMode::Off
        {
            jwt.ensure_key(token).await;
        }
    }

    /// [`Authorizer::prepare`], waiting at most `bound` for it. True when it finished, false
    /// when the bound passed first; the fetch of a rotated key goes on in the background either
    /// way, so a later call finds the key.
    pub async fn prepare_within(&self, credentials: Option<&Credentials>, bound: Duration) -> bool {
        tokio::time::timeout(bound, self.prepare(credentials))
            .await
            .is_ok()
    }

    /// A credentials refresh was ignored because the session's principal is its client
    /// certificate, which no message can replace (ADR 0020).
    pub fn refresh_ignored_for_certificate(&self) {
        self.metrics
            .credentials_refresh("ignored_certificate_session");
    }

    /// A refresh was ignored because it came less than a second after the last one looked at.
    pub fn refresh_throttled(&self) {
        self.metrics.credentials_refresh("throttled");
    }

    /// Starts the background fetch of the JWKS, when JWT credentials are configured.
    pub fn start_jwks(&self, stopping: CancellationToken) {
        if let Some(jwt) = &self.jwt {
            jwt.spawn(stopping);
        }
    }

    /// The ACL file being watched, if any.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Starts at 1 and grows by one per successful reload that changed the ACL.
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn acl(&self) -> Arc<Acl> {
        Arc::clone(&self.acl.read().unwrap())
    }

    /// Re-reads the ACL file. A file that cannot be read or parsed keeps the previous ACL (an
    /// error is logged and counted); an unchanged ACL does not bump the generation. Returns
    /// whether the ACL changed.
    pub fn reload(&self) -> bool {
        let Some(path) = &self.path else {
            return false;
        };
        let parsed = std::fs::read_to_string(path)
            .with_context(|| format!("read {}", path.display()))
            .and_then(|text| Acl::parse(&text));
        match parsed {
            Err(e) => {
                error!(error = %format!("{e:#}"), file = %path.display(), "ACL reload failed; keeping the previous ACL");
                self.metrics.acl_reload_failed();
                false
            }
            Ok(acl) => {
                let mut current = self.acl.write().unwrap();
                if **current == acl {
                    return false;
                }
                let tokens = acl.entries().len();
                *current = Arc::new(acl);
                let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
                drop(current);
                self.metrics.acl_generation_set(generation);
                info!(generation, tokens, "ACL reloaded");
                true
            }
        }
    }

    /// The `service` label of a violation: the claimed name only when the ACL knows it, so a
    /// client that has not authenticated cannot mint label values.
    fn label_service(acl: &Acl, claimed: &str) -> String {
        if acl.has_service(claimed) {
            clip(claimed)
        } else {
            "unknown".to_string()
        }
    }

    /// True when a refusal line for `reason` may be logged now; writes the notice or the summary
    /// of the rate limit itself when one is due.
    fn may_log(&self, reason: &'static str) -> bool {
        match self.limiter.gate(reason, Instant::now()) {
            Gate::Log => true,
            Gate::Suppress => false,
            Gate::Notice => {
                warn!(
                    reason,
                    "refusal log rate limit: more than {LOG_BURST} refusals in {LOG_WINDOW:?}; further lines for this reason are not logged until the window ends (kraken_hub_auth_violations_total still counts them)"
                );
                false
            }
            Gate::LogAfterSummary(dropped) => {
                warn!(
                    reason,
                    dropped, "refusal lines were not logged in the last window (rate limit)"
                );
                true
            }
        }
    }

    fn count(&self, acl: &Acl, d: &Denied, service: &str) {
        self.metrics
            .auth_violation(d.reason, &Self::label_service(acl, service));
    }

    /// Judges a `Register`: the credentials (`None`: the client sent none), the service name it
    /// claims and every type it handles. Every violation is logged and counted; in `enforce` the
    /// first is returned in [`Admission::refused`], in `permissive` the registration goes ahead
    /// and `status` says it is unauthenticated.
    pub fn admit(
        &self,
        credentials: Option<&Credentials>,
        service: &str,
        handled: &[String],
    ) -> Admission {
        self.admit_with(None, credentials, service, handled)
    }

    /// [`Authorizer::admit`] for a stream whose TLS peer presented a client certificate (`peer`:
    /// its DER encoding; `None` when it presented none). The certificate, when there is one, is
    /// the session's principal; see [`Authorizer::admit_identified`].
    pub fn admit_peer(
        &self,
        peer: Option<&[u8]>,
        credentials: Option<&Credentials>,
        service: &str,
        handled: &[String],
    ) -> Admission {
        let identified = match self.mode {
            AuthMode::Off => None,
            _ => peer.map(crate::mtls::identify),
        };
        self.admit_with(identified, credentials, service, handled)
    }

    /// [`Authorizer::admit`] for a certificate already read. A certificate that names exactly one
    /// service is the principal (rights come from that service's `[[service]]` table, and the
    /// service must be the one registered); `credentials`, when present, must resolve to the same
    /// service (`cert_mismatch`) and add nothing else. A certificate that cannot be used (it names
    /// no service or several, or it ended) is a violation of its own and, where the mode lets the
    /// session go on, the credentials are judged as if there were no certificate.
    pub fn admit_identified(
        &self,
        cert: Result<ClientCert, CertError>,
        credentials: Option<&Credentials>,
        service: &str,
        handled: &[String],
    ) -> Admission {
        self.admit_with(Some(cert), credentials, service, handled)
    }

    fn admit_with(
        &self,
        cert: Option<Result<ClientCert, CertError>>,
        credentials: Option<&Credentials>,
        service: &str,
        handled: &[String],
    ) -> Admission {
        if self.mode == AuthMode::Off {
            return Admission {
                principal: None,
                status: AuthStatus::Off,
                refused: None,
            };
        }
        let acl = self.acl();
        let (matched, mut denials) = self.resolve_principal(&acl, cert, credentials, service);
        if let Some((principal, rights)) = &matched {
            denials.extend(judge(*rights, principal, service, handled));
        }
        let key = matched.as_ref().map(|(p, _)| p.label());
        for d in &denials {
            self.count(&acl, d, service);
        }
        // One line per refused `Register`, however many rules it broke; the metric has each.
        if let Some(first) = denials.first()
            && self.may_log(first.reason)
        {
            let reasons = denials
                .iter()
                .map(|d| d.reason)
                .collect::<Vec<_>>()
                .join(",");
            let service = clip(service);
            let key = key.as_deref().unwrap_or("-");
            if self.mode == AuthMode::Enforce {
                warn!(
                    reason = first.reason,
                    reasons, service, key, "register refused: {}", first.message
                );
            } else {
                warn!(
                    reason = first.reason,
                    reasons,
                    service,
                    key,
                    "auth violation (permissive, allowed): {}",
                    first.message
                );
            }
        }
        let status = match &matched {
            Some((_, rights)) => AuthStatus::Authenticated {
                service: rights.service.to_string(),
            },
            None => AuthStatus::UnauthenticatedPermissive,
        };
        Admission {
            principal: matched.map(|(p, _)| Arc::new(p)),
            status,
            refused: (self.mode == AuthMode::Enforce)
                .then(|| denials.into_iter().next())
                .flatten(),
        }
    }

    /// Who a `Register` speaks for: the principal and what its entry grants (`None` when nothing
    /// resolved), and the denials found on the way, in the order they are reported. A usable
    /// certificate wins over `credentials`; see [`Authorizer::admit_identified`].
    fn resolve_principal<'a>(
        &self,
        acl: &'a Acl,
        cert: Option<Result<ClientCert, CertError>>,
        credentials: Option<&Credentials>,
        service: &str,
    ) -> (Option<(Principal, Rights<'a>)>, Vec<Denied>) {
        let from_credentials =
            |side: Vec<Denied>| match self.resolve_credential(acl, credentials, service) {
                Ok(found) => (Some(found), side),
                Err(denials) => (None, [side, denials].concat()),
            };
        let Some(cert) = cert else {
            return from_credentials(Vec::new());
        };
        let cert = match cert {
            Err(e) => {
                self.metrics.client_certificate(e.result());
                return from_credentials(vec![Denied::new(
                    e.reason(),
                    DeniedCode::PermissionDenied,
                    e.message(),
                )]);
            }
            Ok(c) if c.not_after <= SystemTime::now() => {
                self.metrics.client_certificate("expired");
                return from_credentials(vec![Denied::new(
                    "expired",
                    DeniedCode::Unauthenticated,
                    "certificate expired",
                )]);
            }
            Ok(c) => c,
        };
        let principal = Principal::certificate(&cert);
        let mut side = Vec::new();
        let mut result = "ok";
        if credentials_present(credentials) {
            match self.resolve_credential(acl, credentials, service) {
                Ok((other, _)) if other.service != cert.service => {
                    result = "mismatch";
                    side.push(Denied::new(
                        "cert_mismatch",
                        DeniedCode::PermissionDenied,
                        "credentials and certificate name different services",
                    ));
                }
                Ok(_) => {}
                // A credential that does not resolve is refused as it always is.
                Err(denials) => side.extend(denials),
            }
        }
        self.metrics.client_certificate(result);
        match acl.rights(&principal.key) {
            Some(rights) => (Some((principal, rights)), side),
            None => {
                if cert.service != service {
                    side.push(Denied::new(
                        "wrong_service",
                        DeniedCode::PermissionDenied,
                        format!(
                            "certificate service {} is not service {}",
                            clip(&cert.service),
                            clip(service)
                        ),
                    ));
                }
                side.push(entry_gone(&principal.key));
                (None, side)
            }
        }
    }

    /// What `credentials` resolve to: the principal and what its entry grants, or the denials
    /// (one for a missing, unknown or invalid credential; a JWT for another service names both
    /// services). The entry's own checks (service name, handled types) are [`judge`]'s.
    fn resolve_credential<'a>(
        &self,
        acl: &'a Acl,
        credentials: Option<&Credentials>,
        service: &str,
    ) -> Result<(Principal, Rights<'a>), Vec<Denied>> {
        match credentials.and_then(|c| c.kind.as_ref()) {
            Some(credentials::Kind::Token(token)) if !token.is_empty() => {
                let digest = digest_of(token);
                match acl.lookup(&digest) {
                    Some(entry) => Ok((Principal::token(digest, &entry.service), entry.rights())),
                    None => Err(vec![Denied::new(
                        "unknown_token",
                        DeniedCode::Unauthenticated,
                        "unknown token",
                    )]),
                }
            }
            Some(credentials::Kind::Jwt(token)) => self.resolve_jwt(acl, token, service),
            Some(credentials::Kind::Token(_)) | None => Err(vec![Denied::new(
                "no_credentials",
                DeniedCode::Unauthenticated,
                "credentials required: Register.credentials.token is missing",
            )]),
        }
    }

    /// What a JWT credential resolves to: the principal and the entry's rights when it is valid
    /// and its service has an entry (the entry's own checks follow in [`Authorizer::admit`]), the
    /// denials otherwise. A token for service A presented by a `Register` for service B names
    /// both, before it says whether A has an entry.
    fn resolve_jwt<'a>(
        &self,
        acl: &'a Acl,
        token: &str,
        service: &str,
    ) -> Result<(Principal, Rights<'a>), Vec<Denied>> {
        let Some(jwt) = &self.jwt else {
            return Err(vec![Denied::new(
                "jwt_not_configured",
                DeniedCode::Unauthenticated,
                "jwt credentials are not configured on this hub",
            )]);
        };
        let verified = jwt.verify(token).map_err(|e| {
            vec![match e {
                JwtError::NotLoaded | JwtError::KeyPending => {
                    Denied::new("jwks_unavailable", DeniedCode::Unavailable, e.message())
                }
                _ => Denied::new("jwt_invalid", DeniedCode::Unauthenticated, e.message()),
            }]
        })?;
        let principal = Principal::jwt(&verified.azp, verified.expires_at);
        match acl.rights(&principal.key) {
            Some(rights) => Ok((principal, rights)),
            None => {
                let mut denials = Vec::new();
                if principal.service != service {
                    denials.push(Denied::new(
                        "wrong_service",
                        DeniedCode::PermissionDenied,
                        format!(
                            "token azp {} is not service {}",
                            clip(&principal.service),
                            clip(service)
                        ),
                    ));
                }
                denials.push(entry_gone(&principal.key));
                Err(denials)
            }
        }
    }

    /// Judges a `ClientMessage.credentials`: the new credential must resolve to the session's own
    /// `service`, and the session's `handled` types are judged against its entry. See [`Refresh`].
    pub fn refresh(
        &self,
        credentials: Option<&Credentials>,
        service: &str,
        handled: &[String],
    ) -> Refresh {
        let acl = self.acl();
        let enforce = self.mode == AuthMode::Enforce;
        let disposition = if enforce {
            Disposition::Closed
        } else {
            Disposition::Allowed
        };
        let (principal, rights) = match self.resolve_credential(&acl, credentials, service) {
            Ok(found) => found,
            Err(denials) if denials[0].code == DeniedCode::Unavailable => {
                // The credential is not bad: the hub cannot judge it yet (the key set is not
                // loaded, or a rotated key is still being fetched). The session keeps what it
                // has until the client's next refresh or the expiry decides (ADR 0019,
                // amendment 2); closing it would punish a client for the identity provider.
                self.metrics.credentials_refresh("unavailable");
                if self.may_log(denials[0].reason) {
                    warn!(
                        service = %clip(service),
                        "credentials refresh not judged: {}; the session keeps its credentials",
                        denials[0].message
                    );
                }
                return Refresh::Kept;
            }
            Err(denials) => {
                // A credential that does not resolve is never adopted.
                self.refresh_refused(&denials, service, None, disposition);
                return if enforce {
                    Refresh::Close(denials[0].clone())
                } else {
                    Refresh::Kept
                };
            }
        };
        let denials = judge(rights, &principal, service, handled);
        let Some(first) = denials.first() else {
            self.metrics.credentials_refresh("ok");
            return Refresh::Accepted {
                status: AuthStatus::Authenticated {
                    service: rights.service.to_string(),
                },
                principal: Arc::new(principal),
            };
        };
        self.refresh_refused(&denials, service, Some(&principal), disposition);
        if enforce {
            Refresh::Close(first.clone())
        } else if first.reason == "wrong_service" {
            // Permissive counts a credential for another service and keeps the old one; a type
            // the new entry does not grant is counted too, and the new credential is adopted
            // (a reload would keep the session in the same way).
            Refresh::Kept
        } else {
            Refresh::Accepted {
                status: AuthStatus::Authenticated {
                    service: rights.service.to_string(),
                },
                principal: Arc::new(principal),
            }
        }
    }

    /// Counts and logs a refresh that broke rules: `result` tells a credential for another
    /// service from one that is simply refused.
    fn refresh_refused(
        &self,
        denials: &[Denied],
        service: &str,
        principal: Option<&Principal>,
        disposition: Disposition,
    ) {
        let wrong = denials.first().is_some_and(|d| d.reason == "wrong_service");
        self.metrics
            .credentials_refresh(if wrong { "wrong_service" } else { "refused" });
        self.report(denials, service, principal, disposition);
    }

    /// What to do with a session whose expiry timer fired: [`Expiry::NotYet`] when the wall clock
    /// has not reached `exp` plus the grace, else the session expired without a refresh (counted
    /// and logged here; the caller asks once per session).
    pub fn expired(&self, principal: &Principal, service: &str) -> Expiry {
        if self.mode == AuthMode::Off {
            return Expiry::Keep;
        }
        // The timer is monotonic; the wall clock decides (amended in H1).
        if self
            .expires_at(principal)
            .is_none_or(|at| at > SystemTime::now())
        {
            return Expiry::NotYet;
        }
        let certificate = principal.kind() == CredentialKind::Certificate;
        if certificate {
            self.metrics.client_certificate("expired");
        }
        let denied = Denied::new(
            "expired",
            DeniedCode::Unauthenticated,
            if certificate {
                "certificate expired"
            } else {
                "credentials expired"
            },
        );
        if self.mode == AuthMode::Enforce {
            self.report(
                std::slice::from_ref(&denied),
                service,
                Some(principal),
                Disposition::Closed,
            );
            Expiry::Close(denied)
        } else {
            self.report(
                std::slice::from_ref(&denied),
                service,
                Some(principal),
                Disposition::Allowed,
            );
            Expiry::Keep
        }
    }

    /// When, by the wall clock, a session holding `principal` is expired: `exp` plus the grace.
    /// `None` when it never is (a token).
    fn expires_at(&self, principal: &Principal) -> Option<SystemTime> {
        let exp = principal.expires_at?;
        // A TLS session cannot be refreshed on a live stream: a certificate ends at `notAfter`.
        let grace = match principal.kind() {
            CredentialKind::Certificate => Duration::ZERO,
            _ => self
                .jwt
                .as_ref()
                .map_or(Duration::ZERO, |j| j.expiry_grace()),
        };
        exp.checked_add(grace)
    }

    /// When the expiry timer of a session holding `principal` is to fire: at `exp` plus the
    /// grace, or after [`EXPIRY_RECHECK`] when that is further away, so a wall-clock step
    /// forward or a suspend is noticed within it; [`Authorizer::expired`] compares with the wall
    /// clock when it fires. `None` when it never fires (mode `off`, a token, an expiry beyond any
    /// process's life).
    pub fn expiry_deadline(&self, principal: Option<&Principal>) -> Option<tokio::time::Instant> {
        if self.mode == AuthMode::Off {
            return None;
        }
        let remaining = self
            .expires_at(principal?)?
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO);
        (remaining <= MAX_EXPIRY_WAIT)
            .then(|| tokio::time::Instant::now() + remaining.min(EXPIRY_RECHECK))
    }

    /// True when `token` is a valid JWT of a service that has a `[[service]]` table: a service
    /// may read the topology, like any ACL token (ADR 0019). A user's access token (`azp` is a
    /// frontend client) has no table, so it never does.
    fn jwt_has_entry(&self, token: &str) -> bool {
        let Some(jwt) = &self.jwt else { return false };
        looks_like_jwt(token)
            && jwt
                .verify(token)
                .is_ok_and(|v| self.acl().rights(&AclKey::Service(v.azp)).is_some())
    }

    /// True when `der` is a client certificate that names a service with a `[[service]]` table and
    /// has not ended.
    fn certificate_has_entry(&self, der: &[u8]) -> bool {
        crate::mtls::identify(der).is_ok_and(|cert| {
            cert.not_after > SystemTime::now()
                && self.acl().rights(&AclKey::Service(cert.service)).is_some()
        })
    }

    /// Judges a gRPC `Topology` call carrying `presented` (the bearer token, if any) against the
    /// dashboard token `dashboard` (if one is set) and the ACL. The topology lists every service,
    /// type and instance id, so:
    ///
    /// * `enforce`: the dashboard token or any ACL token; with neither configured, or neither
    ///   presented, the call is `UNAUTHENTICATED`.
    /// * `permissive`: the same, except that an unauthenticated call is allowed when no dashboard
    ///   token is set (it is logged and counted as `topology_unauthenticated`). A dashboard token
    ///   that is set stays required.
    /// * `off`: only the dashboard token, when set.
    pub fn check_topology(
        &self,
        presented: Option<&str>,
        dashboard: Option<&Secret>,
    ) -> Result<(), Status> {
        self.check_topology_peer(presented, None, dashboard)
    }

    /// [`Authorizer::check_topology`] for a call that came over a TLS connection whose peer
    /// presented the client certificate `peer` (DER): in `permissive` and `enforce` a certificate
    /// that names a service with a `[[service]]` table, and has not ended, is accepted like an ACL
    /// token (ADR 0020).
    pub fn check_topology_peer(
        &self,
        presented: Option<&str>,
        peer: Option<&[u8]>,
        dashboard: Option<&Secret>,
    ) -> Result<(), Status> {
        let unauthenticated = || Status::unauthenticated("unauthenticated");
        let dashboard_ok = dashboard.is_some_and(|d| d.matches(presented));
        if self.mode == AuthMode::Off {
            return if dashboard.is_none() || dashboard_ok {
                Ok(())
            } else {
                Err(unauthenticated())
            };
        }
        // Looked up even when the dashboard token matched, so the time does not tell which
        // of the two the caller presented.
        let acl_ok = presented.is_some_and(|t| self.acl().lookup(&digest_of(t)).is_some());
        if dashboard_ok
            || acl_ok
            || presented.is_some_and(|t| self.jwt_has_entry(t))
            || peer.is_some_and(|der| self.certificate_has_entry(der))
        {
            return Ok(());
        }
        if self.mode == AuthMode::Enforce || dashboard.is_some() {
            return Err(unauthenticated());
        }
        self.metrics
            .auth_violation("topology_unauthenticated", "unknown");
        if self.may_log("topology_unauthenticated") {
            warn!(
                reason = "topology_unauthenticated",
                "auth violation (permissive, allowed): Topology without a valid token"
            );
        }
        Ok(())
    }

    /// True when some ACL entry grants the processor controls (ADR 0023). Never in `off`, which
    /// reads no ACL. Asked per call, so a reload that adds or removes `control` applies at once.
    pub fn controls_configured(&self) -> bool {
        if self.mode == AuthMode::Off {
            return false;
        }
        let acl = self.acl();
        acl.entries().iter().any(|e| e.control) || acl.services().iter().any(|e| e.control)
    }

    /// What the bearer credential `presented` may do with the processor controls: a static token
    /// or, with JWT credentials configured, a JWT, resolved as `Register` resolves it (a rotated
    /// key must have been fetched first, see [`Authorizer::prepare_bearer`]). The token lookup
    /// compares every entry in constant time.
    pub fn control_right(&self, presented: &str) -> ControlRight {
        if self.mode == AuthMode::Off || presented.is_empty() {
            return ControlRight::Unknown;
        }
        let acl = self.acl();
        if let Some(entry) = acl.lookup(&digest_of(presented)) {
            return if entry.control {
                ControlRight::Granted(entry.service.clone())
            } else {
                ControlRight::Denied(entry.service.clone())
            };
        }
        let Some(jwt) = &self.jwt else {
            return ControlRight::Unknown;
        };
        if !looks_like_jwt(presented) {
            return ControlRight::Unknown;
        }
        let Ok(verified) = jwt.verify(presented) else {
            return ControlRight::Unknown;
        };
        match acl.rights(&AclKey::Service(verified.azp.clone())) {
            Some(rights) if rights.control => ControlRight::Granted(verified.azp),
            Some(_) => ControlRight::Denied(verified.azp),
            None => ControlRight::Unknown,
        }
    }

    /// The denial for a `Register` that names an instance id held by a session of another
    /// service. Counted (`instance_takeover`) and logged in `permissive` and `enforce` alike:
    /// handing one service's queue and pending replies to another is never legitimate, so
    /// `permissive` refuses it too (the one violation it does not merely observe).
    pub fn instance_takeover(&self, service: &str, instance_id: &str) -> Denied {
        let denied = Denied::new(
            "instance_takeover",
            DeniedCode::PermissionDenied,
            format!(
                "instance id {} belongs to another service",
                instance_id.chars().take(8).collect::<String>()
            ),
        );
        self.count(&self.acl(), &denied, service);
        if self.may_log(denied.reason) {
            warn!(
                reason = denied.reason,
                service = clip(service),
                instance = instance_id.chars().take(8).collect::<String>(),
                mode = self.mode.as_str(),
                "register refused: {}",
                denied.message
            );
        }
        denied
    }

    /// Judges a session that exists against the current ACL, without logging or counting: the
    /// violations (empty when the session is still allowed). Used right after a registration
    /// (a reload may have run between the check and the insert) and when the ACL changes. A
    /// session without a principal is only judged in `enforce` (it cannot exist there).
    pub fn recheck(
        &self,
        principal: Option<&Principal>,
        service: &str,
        handled: &[String],
    ) -> Vec<Denied> {
        if self.mode == AuthMode::Off {
            return Vec::new();
        }
        let Some(principal) = principal else {
            return if self.mode == AuthMode::Enforce {
                vec![Denied::new(
                    "no_credentials",
                    DeniedCode::Unauthenticated,
                    "credentials required",
                )]
            } else {
                Vec::new()
            };
        };
        match self.acl().rights(&principal.key) {
            None => vec![entry_gone(&principal.key)],
            Some(rights) => judge(rights, principal, service, handled),
        }
    }

    /// Logs (and, for `Closed` and `Allowed`, counts) what [`Authorizer::recheck`] found.
    pub fn report(
        &self,
        denials: &[Denied],
        service: &str,
        principal: Option<&Principal>,
        disposition: Disposition,
    ) {
        let acl = self.acl();
        let service = clip(service);
        for d in denials {
            let key = principal.map_or_else(|| "-".to_string(), Principal::label);
            if disposition != Disposition::Kept {
                self.count(&acl, d, &service);
            }
            if !self.may_log(d.reason) {
                continue;
            }
            match disposition {
                Disposition::Closed => warn!(
                    reason = d.reason,
                    service, key, "session closed: {}", d.message
                ),
                Disposition::Allowed => warn!(
                    reason = d.reason,
                    service, key, "auth violation (permissive, allowed): {}", d.message
                ),
                Disposition::Kept => warn!(
                    reason = d.reason,
                    service, key, "auth violation (permissive, session kept): {}", d.message
                ),
            }
        }
    }

    /// Judges a request of type `type_name` from a session registered as `sender_service` with
    /// `principal`, against the current ACL. `Err` only in `enforce` (the request is refused);
    /// in `permissive` a violation is logged and counted and the request goes on. A session
    /// without a principal is not judged here: its `Register` was already reported.
    pub fn check_send(
        &self,
        principal: Option<&Principal>,
        sender_service: &str,
        type_name: &str,
    ) -> Result<(), Denied> {
        if self.mode == AuthMode::Off {
            return Ok(());
        }
        let Some(principal) = principal else {
            return Ok(());
        };
        let acl = self.acl();
        let denied = match acl.rights(&principal.key) {
            None => Some(entry_gone(&principal.key)),
            Some(rights) if rights.service != sender_service => Some(Denied::new(
                "wrong_service",
                DeniedCode::PermissionDenied,
                format!(
                    "token is no longer allowed to act as service {}",
                    clip(sender_service)
                ),
            )),
            Some(rights) if !any_matches(rights.send, type_name) => Some(Denied::new(
                "send_denied",
                DeniedCode::PermissionDenied,
                format!(
                    "service {} is not allowed to send type {}",
                    clip(sender_service),
                    clip(type_name)
                ),
            )),
            Some(_) => None,
        };
        let Some(denied) = denied else {
            return Ok(());
        };
        self.count(&acl, &denied, sender_service);
        let enforce = self.mode == AuthMode::Enforce;
        if self.may_log(denied.reason) {
            let (service, r#type) = (clip(sender_service), clip(type_name));
            if enforce {
                warn!(
                    reason = denied.reason,
                    service,
                    r#type,
                    key = principal.label(),
                    "request refused"
                );
            } else {
                warn!(
                    reason = denied.reason,
                    service,
                    r#type,
                    key = principal.label(),
                    "auth violation (permissive, allowed)"
                );
            }
        }
        if enforce { Err(denied) } else { Ok(()) }
    }
}

/// Forbidden types a denial names; the rest is "and N more".
const MAX_LISTED_TYPES: usize = 10;

/// The denial for a principal whose ACL entry is gone: the token was removed from the file, or
/// the service's table was.
fn entry_gone(key: &AclKey) -> Denied {
    match key {
        AclKey::Token(_) => Denied::new(
            "token_revoked",
            DeniedCode::Unauthenticated,
            "token no longer accepted",
        ),
        AclKey::Service(name) => Denied::new(
            "no_acl_entry",
            DeniedCode::PermissionDenied,
            format!("no ACL entry for service {}", clip(name)),
        ),
    }
}

/// `type A` or `types A, B` and, past [`MAX_LISTED_TYPES`], `and N more`: one denial names all
/// the offending types of a `Register` (a client may offer thousands).
fn listed(types: &[&String]) -> String {
    let shown: Vec<String> = types
        .iter()
        .take(MAX_LISTED_TYPES)
        .map(|t| clip(t))
        .collect();
    let more = match types.len().saturating_sub(MAX_LISTED_TYPES) {
        0 => String::new(),
        n => format!(" and {n} more"),
    };
    let noun = if types.len() == 1 { "type" } else { "types" };
    format!("{noun} {}{more}", shown.join(", "))
}

/// The checks of an entry that matched: the service name, every handled type against `handle`,
/// then the event browser's types against the `event-browser` right.
fn judge(
    rights: Rights<'_>,
    principal: &Principal,
    service: &str,
    handled: &[String],
) -> Vec<Denied> {
    let mut out = Vec::new();
    if rights.service != service {
        out.push(Denied::new(
            "wrong_service",
            DeniedCode::PermissionDenied,
            match (&principal.key, principal.kind()) {
                (AclKey::Token(digest), _) => format!(
                    "token {} is not allowed to register as service {}",
                    short(digest),
                    clip(service)
                ),
                (AclKey::Service(name), CredentialKind::Certificate) => format!(
                    "certificate service {} is not service {}",
                    clip(name),
                    clip(service)
                ),
                (AclKey::Service(azp), _) => {
                    format!("token azp {} is not service {}", clip(azp), clip(service))
                }
            },
        ));
    }
    // The event browser's namespace is the `event-browser` right's alone (ADR 0027): `handle`
    // never grants it, so `handle = ["*"]` cannot register a browser by accident.
    let (events, others): (Vec<&String>, Vec<&String>) =
        handled.iter().partition(|t| is_events_type(t));
    let offending: Vec<&String> = others
        .into_iter()
        .filter(|t| !any_matches(rights.handle, t))
        .collect();
    if !offending.is_empty() {
        out.push(Denied::new(
            "handle_denied",
            DeniedCode::PermissionDenied,
            format!(
                "service {} is not allowed to handle {}",
                clip(service),
                listed(&offending)
            ),
        ));
    }
    if !rights.event_browser && !events.is_empty() {
        out.push(Denied::new(
            "event_browser_denied",
            DeniedCode::PermissionDenied,
            format!(
                "service {} may not handle {}: {EVENTS_NAMESPACE}* types need the event-browser right",
                clip(service),
                listed(&events)
            ),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::test_support::test_metrics;

    fn hex(token: &str) -> String {
        hash_token(token)
    }

    fn acl_text() -> String {
        format!(
            r#"
[[token]]
token_sha256 = "{a}"
service = "billing"
handle = ["billing.*", "shared.Ping"]
send = ["orders.*"]
control = true
label = "first"

[[token]]
token_sha256 = "{b}"
service = "orders"
handle = ["*"]
contexts = ["default"]
"#,
            a = hex("tok-billing"),
            b = hex("tok-orders")
        )
    }

    fn token(t: &str) -> Credentials {
        Credentials {
            kind: Some(credentials::Kind::Token(t.to_string())),
        }
    }

    fn authorizer(
        mode: AuthMode,
    ) -> (Arc<Authorizer>, crate::telemetry::test_support::TestMetrics) {
        let m = test_metrics();
        let a = Authorizer::with_acl(mode, Acl::parse(&acl_text()).unwrap(), m.metrics.clone());
        (a, m)
    }

    fn types(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// The principal of the test token `t` (granted `service`).
    fn tok(t: &str, service: &str) -> Principal {
        Principal::token(digest_of(t), service)
    }

    // ---- gRPC Topology (review finding 2) ----

    #[test]
    fn enforce_topology_needs_the_dashboard_token_or_an_acl_token() {
        let (a, m) = authorizer(AuthMode::Enforce);
        let dash = Secret::new("dash-token-0123456789");
        let refused =
            |r: Result<(), Status>| assert_eq!(r.unwrap_err().code(), tonic::Code::Unauthenticated);
        // Neither configured nor presented, or a token that is neither.
        refused(a.check_topology(None, None));
        refused(a.check_topology(Some("not-a-token"), None));
        refused(a.check_topology(None, Some(&dash)));
        refused(a.check_topology(Some("not-a-token"), Some(&dash)));
        // The dashboard token, or any ACL token.
        assert!(
            a.check_topology(Some("dash-token-0123456789"), Some(&dash))
                .is_ok()
        );
        assert!(a.check_topology(Some("tok-billing"), None).is_ok());
        assert!(a.check_topology(Some("tok-orders"), Some(&dash)).is_ok());
        // Enforce refuses; it does not count a violation of its own.
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "topology_unauthenticated"
            ),
            None
        );
    }

    #[test]
    fn permissive_topology_allows_and_counts_an_unauthenticated_call() {
        let (a, m) = authorizer(AuthMode::Permissive);
        assert!(a.check_topology(None, None).is_ok());
        assert!(a.check_topology(Some("nope"), None).is_ok());
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "topology_unauthenticated"
            ),
            Some(2)
        );
        // An authenticated call is not a violation.
        assert!(a.check_topology(Some("tok-orders"), None).is_ok());
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "topology_unauthenticated"
            ),
            Some(2)
        );
        // A dashboard token that is set stays required.
        let dash = Secret::new("dash-token-0123456789");
        assert_eq!(
            a.check_topology(None, Some(&dash)).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn off_topology_is_open_unless_a_dashboard_token_is_set() {
        let (a, m) = authorizer(AuthMode::Off);
        assert!(a.check_topology(None, None).is_ok());
        let dash = Secret::new("dash-token-0123456789");
        assert!(a.check_topology(None, Some(&dash)).is_err());
        assert!(
            a.check_topology(Some("dash-token-0123456789"), Some(&dash))
                .is_ok()
        );
        assert_eq!(m.counter_total("kraken_hub_auth_violations_total"), None);
    }

    // ---- refusal logging (review finding 6) ----

    use crate::telemetry::test_support::{LogLine, capture_logs, logged};

    /// The refusal lines logged on this thread (they all carry a `reason`).
    fn refusals() -> Vec<LogLine> {
        logged()
            .into_iter()
            .filter(|l| l.field("reason").is_some())
            .collect()
    }

    #[test]
    fn clip_cuts_at_128_characters_on_a_char_boundary() {
        assert_eq!(clip("orders"), "orders");
        let long = "é".repeat(300);
        let clipped = clip(&long);
        assert_eq!(clipped.chars().count(), 128);
        assert!(long.starts_with(&clipped));
        assert_eq!(clip(&"x".repeat(128)).len(), 128);
    }

    #[test]
    fn an_attacker_chosen_service_name_is_cut_in_the_log_and_the_message_and_never_a_label() {
        capture_logs();
        let (a, m) = authorizer(AuthMode::Enforce);
        let name = "s".repeat(5000);
        let admission = a.admit(Some(&token("nope")), &name, &[]);
        let denied = admission.refused.unwrap();
        assert!(denied.message.len() < 200, "{}", denied.message.len());
        let lines = refusals();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0].field("service").unwrap().chars().count(), 128);
        // The metric label is `unknown`: the ACL does not hold the name.
        assert_eq!(
            m.counter("kraken_hub_auth_violations_total", "service", "unknown"),
            Some(1)
        );
        // A wrong-service denial quotes the name too, cut.
        let admission = a.admit(Some(&token("tok-orders")), &name, &[]);
        let message = admission.refused.unwrap().message;
        assert!(message.len() < 200, "{}", message.len());
    }

    #[test]
    fn a_refused_register_logs_one_line_however_many_rules_it_breaks() {
        capture_logs();
        let (a, m) = authorizer(AuthMode::Enforce);
        // The wrong service and three types outside `handle`: two denials, one line.
        let admission = a.admit(
            Some(&token("tok-billing")),
            "orders",
            &types(&["x.A", "x.B", "x.C"]),
        );
        assert!(admission.refused.is_some());
        let lines = refusals();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].message.contains("register refused"), "{lines:?}");
        assert_eq!(
            lines[0].field("reasons"),
            Some("wrong_service,handle_denied")
        );
        // Every rule broken is still counted, once.
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "wrong_service"
            ),
            Some(1)
        );
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "handle_denied"
            ),
            Some(1)
        );
    }

    #[test]
    fn many_forbidden_types_collapse_into_one_denial_listing_ten() {
        let (a, _) = authorizer(AuthMode::Enforce);
        let many: Vec<String> = (0..25).map(|n| format!("x.T{n:02}")).collect();
        let denials = a.recheck(Some(&tok("tok-orders", "orders")), "orders", &many);
        // tok-orders handles `*`: nothing is denied. tok-billing handles billing.* and shared.Ping.
        assert!(denials.is_empty());
        let denials = a.recheck(Some(&tok("tok-billing", "billing")), "billing", &many);
        assert_eq!(denials.len(), 1, "{denials:?}");
        let message = &denials[0].message;
        assert_eq!(denials[0].reason, "handle_denied");
        assert!(
            message.contains("x.T00") && message.contains("x.T09"),
            "{message}"
        );
        assert!(!message.contains("x.T10"), "{message}");
        assert!(message.ends_with("and 15 more"), "{message}");
        // One forbidden type is named alone, without a count of more.
        let one = a.recheck(
            Some(&tok("tok-billing", "billing")),
            "billing",
            &types(&["x.A"]),
        );
        assert_eq!(
            one[0].message,
            "service billing is not allowed to handle type x.A"
        );
        // A long type name is cut too.
        let long = format!("x.{}", "n".repeat(1000));
        let cut = a.recheck(Some(&tok("tok-billing", "billing")), "billing", &[long]);
        assert!(cut[0].message.len() < 300, "{}", cut[0].message.len());
    }

    #[test]
    fn the_log_gate_allows_ten_lines_per_reason_per_window_then_one_notice() {
        let limiter = LogLimiter::default();
        let t0 = Instant::now();
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        for n in 0..10 {
            assert_eq!(limiter.gate("a", at(n)), Gate::Log, "line {n}");
        }
        assert_eq!(limiter.gate("a", at(10)), Gate::Notice);
        for n in 11..50 {
            assert_eq!(limiter.gate("a", at(n)), Gate::Suppress, "line {n}");
        }
        // Another reason has its own budget.
        assert_eq!(limiter.gate("b", at(50)), Gate::Log);
        // Window over: the suppressed count is reported with the first line of the next one.
        assert_eq!(
            limiter.gate("a", at(10_000)),
            Gate::LogAfterSummary(40),
            "1 notice-triggering + 39 suppressed"
        );
        assert_eq!(limiter.gate("a", at(10_001)), Gate::Log);
        // A quiet window leaves no summary.
        assert_eq!(limiter.gate("b", at(30_000)), Gate::Log);
    }

    #[test]
    fn a_flood_of_refused_registers_logs_a_bounded_number_of_lines() {
        capture_logs();
        let (a, m) = authorizer(AuthMode::Enforce);
        for _ in 0..200 {
            let admission = a.admit(Some(&token("nope")), "orders", &[]);
            assert!(admission.refused.is_some());
        }
        let lines = refusals();
        assert_eq!(lines.len(), 11, "10 lines and one notice: {}", lines.len());
        assert!(
            lines[10].message.contains("rate limit"),
            "{:?}",
            lines[10].message
        );
        // Nothing is lost from the metric.
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "unknown_token"
            ),
            Some(200)
        );
    }

    #[test]
    fn hashing_matches_the_known_sha256_vector() {
        assert_eq!(
            hash_token("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(short(&digest_of("abc")), "ba7816bf");
    }

    #[test]
    fn patterns_match_exact_prefix_and_any() {
        let p = |s| Pattern::parse(s).unwrap();
        assert!(p("*").matches("anything.At.All"));
        assert!(p("files.*").matches("files.Attach"));
        assert!(p("files.*").matches("files.sub.Attach"));
        assert!(!p("files.*").matches("files."));
        assert!(!p("files.*").matches("filesX.Attach"));
        assert!(!p("files.*").matches("files"));
        assert!(p("files.Attach").matches("files.Attach"));
        assert!(!p("files.Attach").matches("files.AttachMore"));
        for bad in ["", "*.x", "a*", "a.*.b", "a.**", "a b"] {
            assert!(Pattern::parse(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn the_acl_parses_every_field() {
        let acl = Acl::parse(&acl_text()).unwrap();
        assert_eq!(acl.entries().len(), 2);
        let e = acl.lookup(&digest_of("tok-billing")).unwrap();
        assert_eq!(e.service, "billing");
        assert_eq!(e.handle.len(), 2);
        assert_eq!(e.send, vec![Pattern::Prefix("orders.".into())]);
        assert!(e.control);
        assert_eq!(e.label.as_deref(), Some("first"));
        assert!(!acl.lookup(&digest_of("tok-orders")).unwrap().control);
        assert!(acl.lookup(&digest_of("nope")).is_none());
        assert!(Acl::parse("").unwrap().entries().is_empty());
    }

    #[test]
    fn contexts_must_be_default_or_absent() {
        let with = |contexts: &str| {
            format!(
                "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"s\"\ncontexts = {contexts}\n",
                hex("t")
            )
        };
        assert!(Acl::parse(&with("[\"default\"]")).is_ok());
        assert!(Acl::parse(&with("[]")).is_ok());
        let err = Acl::parse(&with("[\"tenant-a\"]")).unwrap_err().to_string();
        assert!(err.contains("contexts"), "{err}");
        assert!(Acl::parse(&with("[\"default\", \"tenant-a\"]")).is_err());
    }

    #[test]
    fn malformed_acl_files_are_errors() {
        let h = hex("t");
        for (name, text) in [
            ("not toml", "[[token".to_string()),
            (
                "short hash",
                "[[token]]\ntoken_sha256 = \"abcd\"\nservice = \"s\"".into(),
            ),
            (
                "non-hex hash",
                format!(
                    "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"s\"",
                    "z".repeat(64)
                ),
            ),
            ("no service", format!("[[token]]\ntoken_sha256 = \"{h}\"")),
            (
                "empty service",
                format!("[[token]]\ntoken_sha256 = \"{h}\"\nservice = \" \""),
            ),
            (
                "unknown key",
                format!("[[token]]\ntoken_sha256 = \"{h}\"\nservice = \"s\"\nsned = [\"x\"]"),
            ),
            (
                "bad pattern",
                format!("[[token]]\ntoken_sha256 = \"{h}\"\nservice = \"s\"\nsend = [\"a*b\"]"),
            ),
            (
                "duplicate",
                format!(
                    "[[token]]\ntoken_sha256 = \"{h}\"\nservice = \"s\"\n[[token]]\ntoken_sha256 = \"{h}\"\nservice = \"t\""
                ),
            ),
            ("unknown table", "[[tokens]]\nx = 1".into()),
        ] {
            assert!(Acl::parse(&text).is_err(), "{name} should be rejected");
        }
    }

    #[test]
    fn two_tokens_may_share_a_service_for_rotation() {
        let text = format!(
            "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"s\"\n[[token]]\ntoken_sha256 = \"{}\"\nservice = \"s\"\n",
            hex("old"),
            hex("new")
        );
        let acl = Acl::parse(&text).unwrap();
        assert!(acl.lookup(&digest_of("old")).is_some());
        assert!(acl.lookup(&digest_of("new")).is_some());
    }

    #[test]
    fn off_allows_everything_and_counts_nothing() {
        let (a, m) = authorizer(AuthMode::Off);
        let adm = a.admit(None, "whatever", &types(&["x.Y"]));
        assert_eq!(adm.refused, None);
        assert_eq!(adm.status, AuthStatus::Off);
        assert!(a.check_send(None, "whatever", "x.Y").is_ok());
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "no_credentials"
            ),
            None
        );
    }

    #[test]
    fn enforce_admits_a_valid_registration() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let adm = a.admit(
            Some(&token("tok-billing")),
            "billing",
            &types(&["billing.Pay", "shared.Ping"]),
        );
        assert_eq!(adm.refused, None);
        assert_eq!(
            adm.principal.as_deref(),
            Some(&tok("tok-billing", "billing"))
        );
        assert_eq!(
            adm.status,
            AuthStatus::Authenticated {
                service: "billing".into()
            }
        );
    }

    #[test]
    fn enforce_refuses_each_kind_of_violation_with_its_status() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let refused = |creds: Option<Credentials>, service: &str, handled: &[&str]| {
            a.admit(creds.as_ref(), service, &types(handled))
                .refused
                .expect("refused")
        };
        let d = refused(None, "billing", &[]);
        assert_eq!(
            (d.reason, d.code),
            ("no_credentials", DeniedCode::Unauthenticated)
        );
        let d = refused(Some(token("")), "billing", &[]);
        assert_eq!(d.reason, "no_credentials");
        let d = refused(Some(token("who?")), "billing", &[]);
        assert_eq!(
            (d.reason, d.code),
            ("unknown_token", DeniedCode::Unauthenticated)
        );
        assert!(!d.message.contains("who?"));
        let jwt = Credentials {
            kind: Some(credentials::Kind::Jwt("a.b.c".into())),
        };
        let d = refused(Some(jwt), "billing", &[]);
        assert_eq!(
            (d.reason, d.code),
            ("jwt_not_configured", DeniedCode::Unauthenticated)
        );
        assert_eq!(d.message, "jwt credentials are not configured on this hub");
        let d = refused(Some(token("tok-billing")), "orders", &[]);
        assert_eq!(
            (d.reason, d.code),
            ("wrong_service", DeniedCode::PermissionDenied)
        );
        let d = refused(
            Some(token("tok-billing")),
            "billing",
            &["billing.Pay", "orders.Place", "x.Y"],
        );
        assert_eq!(
            (d.reason, d.code),
            ("handle_denied", DeniedCode::PermissionDenied)
        );
        // The forbidden types are listed in one denial (up to ten, then "and N more").
        assert_eq!(
            d.message,
            "service billing is not allowed to handle types orders.Place, x.Y"
        );
        assert_eq!(d.to_status().code(), tonic::Code::PermissionDenied);
        assert_eq!(
            refused(None, "billing", &[]).to_status().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn permissive_allows_every_violation_but_counts_it() {
        let (a, m) = authorizer(AuthMode::Permissive);
        let adm = a.admit(None, "billing", &types(&["billing.Pay"]));
        assert_eq!(adm.refused, None);
        assert_eq!(adm.principal, None);
        assert_eq!(adm.status, AuthStatus::UnauthenticatedPermissive);
        assert_eq!(adm.status.label(), Some("unauthenticated (permissive)"));
        let adm = a.admit(
            Some(&token("tok-billing")),
            "billing",
            &types(&["orders.Place"]),
        );
        assert_eq!(adm.refused, None);
        assert_eq!(
            adm.status,
            AuthStatus::Authenticated {
                service: "billing".into()
            }
        );
        assert_eq!(adm.status.label(), Some("authenticated"));
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "no_credentials"
            ),
            Some(1)
        );
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "handle_denied"
            ),
            Some(1)
        );
        // The service label of the first violation is a name the ACL knows.
        assert_eq!(
            m.counter("kraken_hub_auth_violations_total", "service", "billing"),
            Some(2)
        );
    }

    #[test]
    fn unauthenticated_clients_cannot_mint_service_labels() {
        let (a, m) = authorizer(AuthMode::Enforce);
        for n in 0..5 {
            let _ = a.admit(None, &format!("random-{n}"), &[]);
        }
        assert_eq!(
            m.counter("kraken_hub_auth_violations_total", "service", "unknown"),
            Some(5)
        );
        assert_eq!(
            m.counter("kraken_hub_auth_violations_total", "service", "random-1"),
            None
        );
    }

    #[test]
    fn requests_are_checked_against_send_patterns() {
        let (a, m) = authorizer(AuthMode::Enforce);
        let d = tok("tok-billing", "billing");
        assert!(a.check_send(Some(&d), "billing", "orders.Place").is_ok());
        let denied = a.check_send(Some(&d), "billing", "admin.Wipe").unwrap_err();
        assert_eq!(
            (denied.reason, denied.code),
            ("send_denied", DeniedCode::PermissionDenied)
        );
        assert!(denied.message.contains("admin.Wipe"));
        assert_eq!(
            m.counter("kraken_hub_auth_violations_total", "reason", "send_denied"),
            Some(1)
        );
        // A token the ACL no longer holds is unauthenticated; a changed service is denied.
        let gone = tok("tok-gone", "billing");
        assert_eq!(
            a.check_send(Some(&gone), "billing", "x.Y")
                .unwrap_err()
                .reason,
            "token_revoked"
        );
        assert_eq!(
            a.check_send(Some(&d), "other", "orders.Place")
                .unwrap_err()
                .reason,
            "wrong_service"
        );
    }

    #[test]
    fn permissive_request_violations_are_counted_and_allowed() {
        let (a, m) = authorizer(AuthMode::Permissive);
        let d = tok("tok-billing", "billing");
        assert!(a.check_send(Some(&d), "billing", "admin.Wipe").is_ok());
        assert_eq!(
            m.counter("kraken_hub_auth_violations_total", "reason", "send_denied"),
            Some(1)
        );
        // No principal: reported at Register, not per request.
        assert!(a.check_send(None, "billing", "admin.Wipe").is_ok());
        assert_eq!(
            m.counter("kraken_hub_auth_violations_total", "reason", "send_denied"),
            Some(1)
        );
    }

    #[test]
    fn recheck_finds_revoked_tokens_and_narrowed_handles() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let d = tok("tok-billing", "billing");
        assert!(
            a.recheck(Some(&d), "billing", &types(&["billing.Pay"]))
                .is_empty()
        );
        assert_eq!(
            a.recheck(Some(&d), "billing", &types(&["x.Y"]))[0].reason,
            "handle_denied"
        );
        assert_eq!(
            a.recheck(Some(&tok("x", "billing")), "billing", &[])[0].reason,
            "token_revoked"
        );
        assert_eq!(a.recheck(None, "billing", &[])[0].reason, "no_credentials");
        let (p, _m) = authorizer(AuthMode::Permissive);
        assert!(p.recheck(None, "billing", &[]).is_empty());
    }

    #[test]
    fn a_principal_names_its_entry_and_never_the_credential() {
        let t = tok("tok-billing", "billing");
        assert_eq!(t.kind(), CredentialKind::Token);
        assert_eq!(t.label(), short(&digest_of("tok-billing")));
        assert_eq!(t.expires_at, None);
        let j = Principal::jwt("billing", SystemTime::UNIX_EPOCH);
        assert_eq!(j.kind(), CredentialKind::Jwt);
        assert_eq!(j.label(), "billing");
        assert_eq!(CredentialKind::Jwt.as_str(), "jwt");
        // The entry of a token resolves through the digest, not through the service name.
        let acl = Acl::parse(&acl_text()).unwrap();
        assert_eq!(acl.rights(&t.key).unwrap().service, "billing");
        assert!(acl.rights(&tok("nope", "billing").key).is_none());
    }

    #[test]
    fn lookups_compare_every_entry_in_constant_time() {
        let acl = Acl::parse(&acl_text()).unwrap();
        let before = ct::COMPARISONS.with(|c| c.get());
        // The first entry matches, and the second is still compared: no early exit.
        assert!(acl.lookup(&digest_of("tok-billing")).is_some());
        assert_eq!(ct::COMPARISONS.with(|c| c.get()) - before, 2);
        let before = ct::COMPARISONS.with(|c| c.get());
        assert!(acl.lookup(&digest_of("unknown")).is_none());
        assert_eq!(ct::COMPARISONS.with(|c| c.get()) - before, 2);
    }

    #[test]
    fn admit_and_the_dashboard_secret_use_the_constant_time_compare() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let before = ct::COMPARISONS.with(|c| c.get());
        let _ = a.admit(Some(&token("tok-orders")), "orders", &[]);
        assert_eq!(ct::COMPARISONS.with(|c| c.get()) - before, 2);

        let secret = Secret::new("s3cret-dashboard-token");
        let before = ct::COMPARISONS.with(|c| c.get());
        assert!(secret.matches(Some("s3cret-dashboard-token")));
        assert!(!secret.matches(Some("s3cret-dashboard-tokeN")));
        assert!(!secret.matches(None));
        assert!(!secret.matches(Some("")));
        // One comparison each, absent and empty tokens included.
        assert_eq!(ct::COMPARISONS.with(|c| c.get()) - before, 4);
    }

    #[test]
    fn secrets_do_not_show_in_debug_output() {
        let secret = Secret::new("hunter2");
        assert_eq!(format!("{secret:?}"), "Secret(..)");
    }

    #[test]
    fn reload_keeps_the_previous_acl_when_the_file_is_bad() {
        let dir = std::env::temp_dir().join(format!("kraken-acl-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("acl.toml");
        std::fs::write(&file, acl_text()).unwrap();
        let m = test_metrics();
        let config = Config {
            auth_mode: AuthMode::Enforce,
            acl_file: Some(file.clone()),
            ..Config::default()
        };
        let a = Authorizer::from_config(&config, m.metrics.clone()).unwrap();
        assert_eq!(a.generation(), 1);
        assert!(!a.reload(), "an unchanged file changes nothing");
        assert_eq!(a.generation(), 1);

        std::fs::write(&file, "this is = not [valid").unwrap();
        assert!(!a.reload());
        assert_eq!(
            a.generation(),
            1,
            "a bad file keeps the previous generation"
        );
        assert!(
            a.acl().lookup(&digest_of("tok-billing")).is_some(),
            "and the previous ACL"
        );
        assert_eq!(
            m.counter_total("kraken_hub_acl_reload_errors_total"),
            Some(1)
        );

        std::fs::write(&file, "").unwrap();
        assert!(a.reload());
        assert_eq!(a.generation(), 2);
        assert!(a.acl().lookup(&digest_of("tok-billing")).is_none());
        assert_eq!(m.gauge_plain("kraken_hub_acl_generation"), Some(2));
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn startup_fails_on_a_missing_or_bad_file() {
        let missing = Config {
            auth_mode: AuthMode::Enforce,
            acl_file: Some("/nonexistent/acl.toml".into()),
            ..Config::default()
        };
        assert!(Authorizer::from_config(&missing, Metrics::noop()).is_err());
    }

    #[test]
    fn modes_parse() {
        assert_eq!("off".parse::<AuthMode>().unwrap(), AuthMode::Off);
        assert_eq!(" Enforce ".parse::<AuthMode>().unwrap(), AuthMode::Enforce);
        assert_eq!(
            "permissive".parse::<AuthMode>().unwrap(),
            AuthMode::Permissive
        );
        assert!("strict".parse::<AuthMode>().is_err());
    }

    // ---- processor control (ADR 0023) ----

    #[test]
    fn only_an_acl_entry_with_control_may_control_processors() {
        for mode in [AuthMode::Enforce, AuthMode::Permissive] {
            let (a, _m) = authorizer(mode);
            assert!(a.controls_configured());
            assert_eq!(
                a.control_right("tok-billing"),
                ControlRight::Granted("billing".into())
            );
            assert_eq!(
                a.control_right("tok-orders"),
                ControlRight::Denied("orders".into()),
                "an entry without control = true"
            );
            assert_eq!(a.control_right("tok-unknown"), ControlRight::Unknown);
            assert_eq!(a.control_right(""), ControlRight::Unknown);
        }
        // `off` reads no ACL, so no ACL credential controls anything.
        let off = Authorizer::off(test_metrics().metrics);
        assert!(!off.controls_configured());
        assert_eq!(off.control_right("tok-billing"), ControlRight::Unknown);
        // An ACL whose entries all lack `control`: nothing to grant with.
        let plain = format!(
            "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"orders\"\n",
            hex("tok-orders")
        );
        let a = Authorizer::with_acl(
            AuthMode::Enforce,
            Acl::parse(&plain).unwrap(),
            test_metrics().metrics,
        );
        assert!(!a.controls_configured());
        assert_eq!(
            a.control_right("tok-orders"),
            ControlRight::Denied("orders".into())
        );
    }

    #[test]
    fn the_control_right_lookup_compares_every_entry_in_constant_time() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let before = ct::COMPARISONS.with(|c| c.get());
        a.control_right("tok-unknown");
        assert_eq!(
            ct::COMPARISONS.with(|c| c.get()) - before,
            a.acl().entries().len()
        );
    }

    // ---- [[service]] tables (ADR 0019) ----

    fn service_acl_text() -> String {
        format!(
            r#"{tokens}
[[service]]
name = "billing"
handle = ["billing.*", "shared.Ping"]
send = ["orders.*"]
control = true
contexts = ["default"]

[[service]]
name = "orders"
handle = ["*"]
"#,
            tokens = acl_text()
        )
    }

    #[test]
    fn service_tables_parse_with_the_token_pattern_semantics() {
        let acl = Acl::parse(&service_acl_text()).unwrap();
        let rights = acl.rights(&AclKey::Service("billing".into())).unwrap();
        assert_eq!(rights.service, "billing");
        assert!(any_matches(rights.handle, "billing.Pay"));
        assert!(any_matches(rights.handle, "shared.Ping"));
        assert!(!any_matches(rights.handle, "billing"));
        assert!(any_matches(rights.send, "orders.Place"));
        assert!(!any_matches(rights.send, "admin.Wipe"));
        assert!(acl.rights(&AclKey::Service("nobody".into())).is_none());
        // A file of service tables alone is a valid ACL.
        let only = Acl::parse("[[service]]\nname = \"x\"\nsend = [\"*\"]\n").unwrap();
        assert!(only.entries().is_empty());
        assert!(only.rights(&AclKey::Service("x".into())).is_some());
        // Two ACLs that differ only in a service table differ (a reload must notice).
        assert_ne!(Acl::parse(&acl_text()).unwrap(), acl);
    }

    // ---- the event-browser right (ADR 0027) ----

    /// A browser token with the right, a token handling `*` without it, and the same pair as
    /// `[[service]]` tables.
    fn event_browser_acl_text() -> String {
        format!(
            r#"
[[token]]
token_sha256 = "{browser}"
service = "kraken-event-browser"
event-browser = true

[[token]]
token_sha256 = "{orders}"
service = "orders"
handle = ["*"]

[[service]]
name = "kraken-event-browser"
event-browser = true

[[service]]
name = "orders"
handle = ["*"]
event-browser = false
"#,
            browser = hex("tok-browser"),
            orders = hex("tok-orders")
        )
    }

    #[test]
    fn event_browser_right_parses_in_acl() {
        let acl = Acl::parse(&event_browser_acl_text()).unwrap();
        let browser = acl.lookup(&digest_of("tok-browser")).unwrap();
        assert!(browser.event_browser);
        assert!(!acl.lookup(&digest_of("tok-orders")).unwrap().event_browser);
        assert!(
            acl.rights(&tok("tok-browser", "kraken-event-browser").key)
                .unwrap()
                .event_browser
        );
        let service = |name: &str| {
            acl.rights(&AclKey::Service(name.into()))
                .unwrap()
                .event_browser
        };
        assert!(service("kraken-event-browser"));
        assert!(!service("orders"));
        // Absent is false, in both tables.
        let plain = Acl::parse(&service_acl_text()).unwrap();
        assert!(
            !plain
                .lookup(&digest_of("tok-billing"))
                .unwrap()
                .event_browser
        );
        assert!(
            !plain
                .rights(&AclKey::Service("billing".into()))
                .unwrap()
                .event_browser
        );
        // A value that is not a boolean, or the right misspelt, is an error.
        let with = |line: &str| {
            format!(
                "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"s\"\n{line}\n",
                hex("t")
            )
        };
        assert!(Acl::parse(&with("event-browser = \"yes\"")).is_err());
        assert!(Acl::parse(&with("event_browser = true")).is_err());
        assert!(Acl::parse("[[service]]\nname = \"s\"\nevent-browser = 1\n").is_err());
        // Two ACLs that differ only in the right differ (a reload must notice).
        let without = event_browser_acl_text().replace("event-browser = true", "");
        assert_ne!(Acl::parse(&without).unwrap(), acl);
    }

    #[test]
    fn events_types_need_the_event_browser_right() {
        let m = test_metrics();
        let acl = Acl::parse(&event_browser_acl_text()).unwrap();
        let a = Authorizer::with_acl(AuthMode::Enforce, acl.clone(), m.metrics.clone());
        let events = types(&["kraken.events.Search", "kraken.events.Tail"]);

        // With the right: the reserved types register, and need no `handle` pattern.
        let ok = a.admit(Some(&token("tok-browser")), "kraken-event-browser", &events);
        assert_eq!(ok.refused, None);
        // The right grants the reserved namespace only.
        let other = a.admit(
            Some(&token("tok-browser")),
            "kraken-event-browser",
            &types(&["kraken.events.Get", "orders.Get"]),
        );
        assert_eq!(other.refused.unwrap().reason, "handle_denied");

        // Without it, even `handle = ["*"]` does not cover them: the whole Register is refused,
        // naming the right and the type.
        let refused = a
            .admit(
                Some(&token("tok-orders")),
                "orders",
                &types(&["orders.Get", "kraken.events.Search"]),
            )
            .refused
            .expect("refused");
        assert_eq!(refused.reason, "event_browser_denied");
        assert_eq!(refused.code, DeniedCode::PermissionDenied);
        assert_eq!(
            refused.message,
            "service orders may not handle type kraken.events.Search: kraken.events.* types need the event-browser right"
        );
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "event_browser_denied"
            ),
            Some(1)
        );
        // Several are listed as `handle_denied` lists them.
        let many = a.recheck(Some(&tok("tok-orders", "orders")), "orders", &events);
        assert_eq!(many.len(), 1, "{many:?}");
        assert!(
            many[0]
                .message
                .contains("types kraken.events.Search, kraken.events.Tail"),
            "{}",
            many[0].message
        );
        // Only the namespace is reserved: neighbours are ordinary types.
        assert!(
            a.recheck(
                Some(&tok("tok-orders", "orders")),
                "orders",
                &types(&["kraken.eventsX.Search", "kraken.Search"]),
            )
            .is_empty()
        );

        // JWT and certificate sessions resolve through `[[service]]` tables: the same rule.
        let jwt =
            |service: &str| Principal::jwt(service, SystemTime::now() + Duration::from_secs(3600));
        assert!(
            a.recheck(
                Some(&jwt("kraken-event-browser")),
                "kraken-event-browser",
                &events
            )
            .is_empty()
        );
        assert_eq!(
            a.recheck(Some(&jwt("orders")), "orders", &events)[0].reason,
            "event_browser_denied"
        );

        // A reload that takes the right away closes the browser's session.
        let without = event_browser_acl_text().replace("event-browser = true", "");
        let reloaded = Authorizer::with_acl(
            AuthMode::Enforce,
            Acl::parse(&without).unwrap(),
            m.metrics.clone(),
        );
        let denials = reloaded.recheck(
            Some(&tok("tok-browser", "kraken-event-browser")),
            "kraken-event-browser",
            &events,
        );
        assert_eq!(denials[0].reason, "event_browser_denied");

        // Permissive counts and allows; off checks nothing.
        let p = Authorizer::with_acl(AuthMode::Permissive, acl.clone(), m.metrics.clone());
        assert_eq!(
            p.admit(Some(&token("tok-orders")), "orders", &events)
                .refused,
            None
        );
        assert_eq!(
            m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "event_browser_denied"
            ),
            Some(2)
        );
        let off = Authorizer::with_acl(AuthMode::Off, acl, m.metrics.clone());
        assert_eq!(off.admit(None, "orders", &events).refused, None);
    }

    #[test]
    fn token_entries_never_apply_to_jwts_and_service_entries_never_to_tokens() {
        let acl = Acl::parse(&service_acl_text()).unwrap();
        // `billing` is both a token entry's service and a [[service]] name; each key reaches
        // only its own table.
        let by_token = acl.rights(&tok("tok-billing", "billing").key).unwrap();
        assert!(any_matches(by_token.handle, "billing.Pay"));
        let token_only = Acl::parse(&acl_text()).unwrap();
        assert!(
            token_only
                .rights(&AclKey::Service("billing".into()))
                .is_none(),
            "a token entry does not make a service entry"
        );
        let service_only = Acl::parse("[[service]]\nname = \"billing\"\n").unwrap();
        assert!(
            service_only
                .rights(&tok("tok-billing", "billing").key)
                .is_none()
        );
    }

    #[test]
    fn malformed_service_tables_are_errors() {
        assert!(
            Acl::parse("[[service]]\nname = \"a\"\n").is_ok(),
            "the control parses"
        );
        for (bad, why) in [
            ("[[service]]\n", "a name is required"),
            ("[[service]]\nname = \"\"\n", "an empty name"),
            ("[[service]]\nname = \"  \"\n", "a blank name"),
            (
                "[[service]]\nname = \"a\"\n[[service]]\nname = \"a\"\n",
                "duplicates",
            ),
            (
                "[[service]]\nname = \"a\"\nsnd = [\"x\"]\n",
                "an unknown key",
            ),
            (
                "[[service]]\nname = \"a\"\nlabel = \"x\"\n",
                "no label on a service",
            ),
            (
                "[[service]]\nname = \"a\"\ntoken_sha256 = \"00\"\n",
                "no token key",
            ),
            (
                "[[service]]\nname = \"a\"\nsend = [\"a*\"]\n",
                "a bad pattern",
            ),
            (
                "[[service]]\nname = \"a\"\nhandle = [\"x y\"]\n",
                "whitespace",
            ),
            (
                "[[service]]\nname = \"a\"\ncontexts = [\"other\"]\n",
                "contexts",
            ),
        ] {
            assert!(Acl::parse(bad).is_err(), "{why} should be an error");
        }
        // A `[[token]]` stays strict about a service-only key.
        assert!(Acl::parse("[[token]]\nname = \"a\"\n").is_err());
    }

    // ---- JWT admission ----

    use crate::jwt::test_support::{JwksServer, Signer, claims};

    const ISS: &str = "https://sso.example/realms/kraken";

    struct JwtFx {
        auth: Arc<Authorizer>,
        m: crate::telemetry::test_support::TestMetrics,
        server: JwksServer,
        key: Signer,
        _stop: tokio_util::sync::DropGuard,
    }

    async fn jwt_fx(mode: AuthMode) -> JwtFx {
        let key = Signer::es256("k1");
        let server = JwksServer::start(&[&key]).await;
        let config = Config {
            jwt_issuer: Some(ISS.into()),
            jwt_jwks_uri: Some(server.url()),
            jwks_refetch_interval: Duration::from_millis(100),
            ..Config::default()
        };
        let m = test_metrics();
        let jwt = Jwt::from_config(&config, m.metrics.clone()).unwrap();
        let stop = CancellationToken::new();
        jwt.as_ref().unwrap().spawn(stop.clone());
        let a = Authorizer::with_acl_and_jwt(
            mode,
            Acl::parse(&service_acl_text()).unwrap(),
            m.metrics.clone(),
            jwt.clone(),
        );
        let jwt = jwt.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !jwt.is_loaded() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the keys never loaded"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        JwtFx {
            auth: a,
            m,
            server,
            key,
            _stop: stop.drop_guard(),
        }
    }

    fn jwt_creds(token: String) -> Credentials {
        Credentials {
            kind: Some(credentials::Kind::Jwt(token)),
        }
    }

    impl JwtFx {
        fn token(&self, azp: &str, exp_in: i64) -> Credentials {
            jwt_creds(self.key.sign(&claims(ISS, azp, exp_in)))
        }
    }

    #[tokio::test]
    async fn a_valid_jwt_registers_with_the_rights_of_its_service_entry() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let creds = fx.token("billing", 300);
        let adm = fx.auth.admit(
            Some(&creds),
            "billing",
            &types(&["billing.Pay", "shared.Ping"]),
        );
        assert_eq!(adm.refused, None);
        let p = adm.principal.expect("a principal");
        assert_eq!(p.kind(), CredentialKind::Jwt);
        assert_eq!(p.key, AclKey::Service("billing".into()));
        assert_eq!(p.service, "billing");
        assert!(p.expires_at.is_some_and(|t| t > SystemTime::now()));
        assert_eq!(
            adm.status,
            AuthStatus::Authenticated {
                service: "billing".into()
            }
        );
        // The rights are the [[service]] table's, not the [[token]] table's: the token entry
        // for `billing` has `billing.*` too, so tell them apart by a type only one grants.
        let adm = fx.auth.admit(Some(&creds), "billing", &types(&["x.Only"]));
        assert_eq!(adm.refused.expect("refused").reason, "handle_denied");
    }

    #[tokio::test]
    async fn a_jwt_controls_with_the_right_of_its_service_table() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let bearer = |azp: &str, exp_in: i64| match fx.token(azp, exp_in).kind {
            Some(credentials::Kind::Jwt(t)) => t,
            _ => unreachable!(),
        };
        assert!(fx.auth.controls_configured());
        for (azp, exp_in, want) in [
            ("billing", 300, ControlRight::Granted("billing".into())),
            ("orders", 300, ControlRight::Denied("orders".into())),
            ("ghost", 300, ControlRight::Unknown),
            ("billing", -600, ControlRight::Unknown),
        ] {
            let token = bearer(azp, exp_in);
            fx.auth.prepare_bearer(Some(&token)).await;
            assert_eq!(fx.auth.control_right(&token), want, "{azp} {exp_in}");
        }
    }

    #[tokio::test]
    async fn a_jwt_for_another_service_is_refused_naming_both() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let adm = fx
            .auth
            .admit(Some(&fx.token("orders", 300)), "billing", &[]);
        let d = adm.refused.expect("refused");
        assert_eq!(
            (d.reason, d.code),
            ("wrong_service", DeniedCode::PermissionDenied)
        );
        assert_eq!(d.message, "token azp orders is not service billing");
        // An azp the ACL does not know says so, after the mismatch.
        let adm = fx.auth.admit(Some(&fx.token("ghost", 300)), "billing", &[]);
        assert_eq!(adm.refused.unwrap().reason, "wrong_service");
    }

    #[tokio::test]
    async fn a_jwt_for_a_service_without_an_entry_is_refused() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        // `web` has a token elsewhere in the file but no [[service]] table; the user-token case
        // (azp = the frontend client) is the same refusal.
        for service in ["ghost", "kraken-frontend"] {
            let adm = fx.auth.admit(Some(&fx.token(service, 300)), service, &[]);
            let d = adm.refused.expect("refused");
            assert_eq!(
                (d.reason, d.code),
                ("no_acl_entry", DeniedCode::PermissionDenied)
            );
            assert_eq!(d.message, format!("no ACL entry for service {service}"));
            assert!(adm.principal.is_none());
        }
    }

    #[tokio::test]
    async fn an_invalid_jwt_is_unauthenticated_and_never_echoed() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let expired = fx.token("billing", -120);
        let d = fx
            .auth
            .admit(Some(&expired), "billing", &[])
            .refused
            .unwrap();
        assert_eq!(
            (d.reason, d.code),
            ("jwt_invalid", DeniedCode::Unauthenticated)
        );
        assert_eq!(d.message, "token expired");
        let wrong_key = jwt_creds(Signer::es256("k1").sign(&claims(ISS, "billing", 300)));
        let d = fx
            .auth
            .admit(Some(&wrong_key), "billing", &[])
            .refused
            .unwrap();
        assert_eq!(d.message, "invalid token signature");
        let junk = jwt_creds("a.b.c".into());
        let d = fx.auth.admit(Some(&junk), "billing", &[]).refused.unwrap();
        assert_eq!(d.message, "malformed token");
        assert_eq!(d.to_status().code(), tonic::Code::Unauthenticated);
        assert_eq!(
            fx.m.counter("kraken_hub_jwt_validations_total", "result", "expired"),
            Some(1)
        );
        // Labels are fixed strings.
        assert_eq!(
            fx.m.counter("kraken_hub_auth_violations_total", "reason", "jwt_invalid"),
            Some(3)
        );
    }

    #[tokio::test]
    async fn before_the_keys_load_a_jwt_is_unavailable() {
        let server = JwksServer::unbound(JwksServer::reserve().await);
        let config = Config {
            jwt_issuer: Some(ISS.into()),
            jwt_jwks_uri: Some(server.url()),
            ..Config::default()
        };
        let m = test_metrics();
        let jwt = Jwt::from_config(&config, m.metrics.clone()).unwrap();
        let a = Authorizer::with_acl_and_jwt(
            AuthMode::Enforce,
            Acl::parse(&service_acl_text()).unwrap(),
            m.metrics.clone(),
            jwt,
        );
        let key = Signer::es256("k1");
        let creds = jwt_creds(key.sign(&claims(ISS, "billing", 300)));
        a.prepare(Some(&creds)).await;
        let d = a.admit(Some(&creds), "billing", &[]).refused.unwrap();
        assert_eq!(
            (d.reason, d.code),
            ("jwks_unavailable", DeniedCode::Unavailable)
        );
        assert_eq!(d.message, "jwks not loaded yet");
        assert_eq!(d.to_status().code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn a_rotated_key_is_fetched_before_the_register_is_judged() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let rotated = Signer::es256("k2");
        fx.server.serve(&[&rotated]);
        let creds = jwt_creds(rotated.sign(&claims(ISS, "billing", 300)));
        // Without the async step the sync check cannot know the key: the client is told to
        // retry (UNAVAILABLE), not that its registration is rejected.
        let d = fx.auth.admit(Some(&creds), "billing", &[]).refused.unwrap();
        assert_eq!(d.message, "signing key not known yet");
        assert_eq!(d.code, DeniedCode::Unavailable);
        tokio::time::sleep(Duration::from_millis(150)).await;
        fx.auth.prepare(Some(&creds)).await;
        assert_eq!(fx.auth.admit(Some(&creds), "billing", &[]).refused, None);
    }

    #[tokio::test]
    async fn permissive_counts_a_bad_jwt_and_lets_the_session_in_unauthenticated() {
        let fx = jwt_fx(AuthMode::Permissive).await;
        let adm = fx
            .auth
            .admit(Some(&fx.token("billing", -120)), "billing", &[]);
        assert_eq!(adm.refused, None);
        assert_eq!(adm.principal, None);
        assert_eq!(adm.status, AuthStatus::UnauthenticatedPermissive);
        assert_eq!(
            fx.m.counter("kraken_hub_auth_violations_total", "reason", "jwt_invalid"),
            Some(1)
        );
        // A valid one for the right service is authenticated.
        let adm = fx
            .auth
            .admit(Some(&fx.token("billing", 300)), "billing", &[]);
        assert!(adm.principal.is_some());
    }

    #[tokio::test]
    async fn requests_and_reloads_judge_a_jwt_session_by_its_service_entry() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let adm = fx
            .auth
            .admit(Some(&fx.token("billing", 300)), "billing", &[]);
        let p = adm.principal.unwrap();
        assert!(
            fx.auth
                .check_send(Some(&p), "billing", "orders.Place")
                .is_ok()
        );
        let d = fx
            .auth
            .check_send(Some(&p), "billing", "admin.Wipe")
            .unwrap_err();
        assert_eq!(d.reason, "send_denied");
        assert!(
            fx.auth
                .recheck(Some(&p), "billing", &types(&["billing.Pay"]))
                .is_empty()
        );
        assert_eq!(
            fx.auth.recheck(Some(&p), "billing", &types(&["x.Y"]))[0].reason,
            "handle_denied"
        );
        // The entry leaves the file: PERMISSION_DENIED, whatever the token still says.
        let without = Authorizer::with_acl(
            AuthMode::Enforce,
            Acl::parse(&acl_text()).unwrap(),
            fx.m.metrics.clone(),
        );
        let gone = without.recheck(Some(&p), "billing", &[]);
        assert_eq!(
            (gone[0].reason, gone[0].code),
            ("no_acl_entry", DeniedCode::PermissionDenied)
        );
        assert_eq!(gone[0].message, "no ACL entry for service billing");
        assert_eq!(
            without
                .check_send(Some(&p), "billing", "orders.Place")
                .unwrap_err()
                .reason,
            "no_acl_entry"
        );
    }

    #[tokio::test]
    async fn the_service_label_knows_the_names_of_service_tables() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let _ = fx
            .auth
            .admit(Some(&fx.token("billing", 300)), "billing", &types(&["x.Y"]));
        assert_eq!(
            fx.m.counter("kraken_hub_auth_violations_total", "service", "billing"),
            Some(1)
        );
        let _ = fx.auth.admit(Some(&fx.token("ghost", 300)), "ghost", &[]);
        assert_eq!(
            fx.m.counter("kraken_hub_auth_violations_total", "service", "unknown"),
            Some(1)
        );
    }

    // ---- refresh and expiry ----

    fn token_for(creds: &str) -> Credentials {
        token(creds)
    }

    fn adopted(r: Refresh) -> Arc<Principal> {
        match r {
            Refresh::Accepted { principal, .. } => principal,
            other => panic!("expected an accepted refresh, got {other:?}"),
        }
    }

    fn closed(r: Refresh) -> Denied {
        match r {
            Refresh::Close(d) => d,
            other => panic!("expected a close, got {other:?}"),
        }
    }

    fn refreshes(fx: &JwtFx, result: &str) -> Option<u64> {
        fx.m.counter("kraken_hub_credentials_refreshes_total", "result", result)
    }

    #[tokio::test]
    async fn a_refresh_with_a_jwt_for_the_same_service_is_accepted() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let r = fx.auth.refresh(
            Some(&fx.token("billing", 900)),
            "billing",
            &types(&["billing.Pay"]),
        );
        let Refresh::Accepted { principal, status } = r else {
            panic!("{r:?}")
        };
        assert_eq!(principal.key, AclKey::Service("billing".into()));
        assert!(principal.expires_at.unwrap() > SystemTime::now() + Duration::from_secs(800));
        assert_eq!(
            status,
            AuthStatus::Authenticated {
                service: "billing".into()
            }
        );
        assert_eq!(refreshes(&fx, "ok"), Some(1));
    }

    #[tokio::test]
    async fn a_refresh_whose_key_is_not_known_yet_keeps_the_old_credentials_even_in_enforce() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let rotated = Signer::es256("k2");
        // Not prepared: the key set does not have k2 and nothing has looked for it.
        let creds = jwt_creds(rotated.sign(&claims(ISS, "billing", 300)));
        assert_eq!(fx.auth.refresh(Some(&creds), "billing", &[]), Refresh::Kept);
        assert_eq!(refreshes(&fx, "unavailable"), Some(1));
        assert_eq!(refreshes(&fx, "refused"), None);
        // Not a violation: the credential is not bad, the hub just cannot judge it yet.
        assert_eq!(fx.m.counter_total("kraken_hub_auth_violations_total"), None);
    }

    #[tokio::test]
    async fn preparing_a_rotated_key_can_be_bounded_and_the_fetch_goes_on() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let rotated = Signer::es256("k2");
        fx.server.serve(&[&rotated]);
        fx.server.set_delay(Duration::from_millis(600));
        let creds = jwt_creds(rotated.sign(&claims(ISS, "billing", 300)));
        let t0 = tokio::time::Instant::now();
        let finished = fx
            .auth
            .prepare_within(Some(&creds), Duration::from_millis(100))
            .await;
        assert!(!finished, "the fetch is slower than the bound");
        assert!(
            t0.elapsed() < Duration::from_millis(450),
            "{:?}",
            t0.elapsed()
        );
        // The fetch was not cancelled with the wait: the key shows up and the refresh passes.
        tokio::time::sleep(Duration::from_millis(900)).await;
        assert!(matches!(
            fx.auth.refresh(Some(&creds), "billing", &[]),
            Refresh::Accepted { .. }
        ));
    }

    #[tokio::test]
    async fn a_refresh_may_change_the_credential_kind_of_the_same_service() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let p = adopted(
            fx.auth
                .refresh(Some(&token_for("tok-billing")), "billing", &[]),
        );
        assert_eq!(p.kind(), CredentialKind::Token);
        assert_eq!(p.expires_at, None);
        let p = adopted(
            fx.auth
                .refresh(Some(&fx.token("billing", 60)), "billing", &[]),
        );
        assert_eq!(p.kind(), CredentialKind::Jwt);
        assert_eq!(refreshes(&fx, "ok"), Some(2));
    }

    #[tokio::test]
    async fn a_refresh_for_another_service_closes_in_enforce_and_is_counted_in_permissive() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        for creds in [fx.token("orders", 60), token_for("tok-orders")] {
            let d = closed(fx.auth.refresh(Some(&creds), "billing", &[]));
            assert_eq!(
                (d.reason, d.code),
                ("wrong_service", DeniedCode::PermissionDenied)
            );
        }
        assert_eq!(refreshes(&fx, "wrong_service"), Some(2));
        assert_eq!(refreshes(&fx, "ok"), None);

        let fx = jwt_fx(AuthMode::Permissive).await;
        let r = fx
            .auth
            .refresh(Some(&fx.token("orders", 60)), "billing", &[]);
        assert_eq!(r, Refresh::Kept, "the old principal stays");
        assert_eq!(refreshes(&fx, "wrong_service"), Some(1));
        assert_eq!(
            fx.m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "wrong_service"
            ),
            Some(1)
        );
    }

    #[tokio::test]
    async fn an_invalid_refresh_closes_in_enforce_and_keeps_the_old_principal_in_permissive() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        for (creds, reason, code) in [
            (
                fx.token("billing", -120),
                "jwt_invalid",
                DeniedCode::Unauthenticated,
            ),
            (
                token_for("nope"),
                "unknown_token",
                DeniedCode::Unauthenticated,
            ),
            (token_for(""), "no_credentials", DeniedCode::Unauthenticated),
            (
                fx.token("ghost", 60),
                "wrong_service",
                DeniedCode::PermissionDenied,
            ),
        ] {
            let d = closed(fx.auth.refresh(Some(&creds), "billing", &[]));
            assert_eq!((d.reason, d.code), (reason, code));
        }
        assert!(matches!(
            fx.auth.refresh(None, "billing", &[]),
            Refresh::Close(_)
        ));
        assert_eq!(refreshes(&fx, "refused"), Some(4));
        assert_eq!(refreshes(&fx, "wrong_service"), Some(1));
        let fx = jwt_fx(AuthMode::Permissive).await;
        assert_eq!(
            fx.auth
                .refresh(Some(&fx.token("billing", -120)), "billing", &[]),
            Refresh::Kept
        );
        assert_eq!(refreshes(&fx, "refused"), Some(1));
        assert_eq!(
            fx.m.counter("kraken_hub_auth_violations_total", "reason", "jwt_invalid"),
            Some(1)
        );
    }

    #[tokio::test]
    async fn a_refresh_judges_the_handled_types_against_the_new_entry() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        // `orders` handles `*` as a token and as a service; `billing`'s service table does not
        // grant x.Only.
        let d = closed(fx.auth.refresh(
            Some(&fx.token("billing", 60)),
            "billing",
            &types(&["x.Only"]),
        ));
        assert_eq!(d.reason, "handle_denied");
        assert_eq!(refreshes(&fx, "refused"), Some(1));
        let fx = jwt_fx(AuthMode::Permissive).await;
        let p = adopted(fx.auth.refresh(
            Some(&fx.token("billing", 60)),
            "billing",
            &types(&["x.Only"]),
        ));
        assert_eq!(
            p.kind(),
            CredentialKind::Jwt,
            "permissive adopts it and counts"
        );
        assert_eq!(
            fx.m.counter(
                "kraken_hub_auth_violations_total",
                "reason",
                "handle_denied"
            ),
            Some(1)
        );
    }

    #[tokio::test]
    async fn a_refresh_where_jwt_is_not_configured_is_refused() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let jwt = jwt_creds("a.b.c".into());
        let d = closed(a.refresh(Some(&jwt), "billing", &[]));
        assert_eq!(d.reason, "jwt_not_configured");
        assert_eq!(d.message, "jwt credentials are not configured on this hub");
    }

    #[tokio::test]
    async fn an_expired_session_is_closed_in_enforce_and_counted_once_in_permissive() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let p = Principal::jwt("billing", SystemTime::now() - Duration::from_secs(31));
        let Expiry::Close(d) = fx.auth.expired(&p, "billing") else {
            panic!("enforce closes")
        };
        assert_eq!(
            (d.reason, d.code, d.message.as_str()),
            (
                "expired",
                DeniedCode::Unauthenticated,
                "credentials expired"
            )
        );
        assert_eq!(
            fx.m.counter("kraken_hub_auth_violations_total", "reason", "expired"),
            Some(1)
        );
        let fx = jwt_fx(AuthMode::Permissive).await;
        let p = Principal::jwt("billing", SystemTime::now() - Duration::from_secs(31));
        assert_eq!(fx.auth.expired(&p, "billing"), Expiry::Keep);
        assert_eq!(
            fx.m.counter("kraken_hub_auth_violations_total", "reason", "expired"),
            Some(1)
        );
        assert_eq!(
            fx.m.counter("kraken_hub_auth_violations_total", "service", "billing"),
            Some(1)
        );
    }

    /// The timer is monotonic, armed from the wall clock once: a wall-clock step back (or a
    /// look-again wake-up) makes it fire before `exp` plus the grace by the wall clock. That is
    /// not an expiry: nothing is counted and the caller arms the timer again (amended in H1).
    #[tokio::test]
    async fn a_timer_that_fires_before_exp_plus_the_grace_by_the_wall_clock_expires_nothing() {
        for mode in [AuthMode::Enforce, AuthMode::Permissive] {
            let fx = jwt_fx(mode).await;
            let p = Principal::jwt("billing", SystemTime::now() + Duration::from_secs(100));
            assert_eq!(fx.auth.expired(&p, "billing"), Expiry::NotYet, "{mode:?}");
            // Within the grace is not expired either.
            let p = Principal::jwt("billing", SystemTime::now() - Duration::from_secs(10));
            assert_eq!(fx.auth.expired(&p, "billing"), Expiry::NotYet, "{mode:?}");
            assert_eq!(
                fx.m.counter_total("kraken_hub_auth_violations_total"),
                None,
                "{mode:?}"
            );
        }
    }

    /// A wall-clock step forward or a suspend leaves a monotonic timer late; it never waits more
    /// than [`EXPIRY_RECHECK`] before comparing `exp` with the wall clock again (amended in H1).
    #[tokio::test]
    async fn a_distant_expiry_is_looked_at_again_within_the_recheck() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let p = Principal::jwt("billing", SystemTime::now() + Duration::from_secs(2 * 3600));
        let left = fx
            .auth
            .expiry_deadline(Some(&p))
            .expect("a deadline")
            .saturating_duration_since(tokio::time::Instant::now());
        assert!(
            left > EXPIRY_RECHECK - Duration::from_secs(1) && left <= EXPIRY_RECHECK,
            "{left:?}"
        );
    }

    #[tokio::test]
    async fn the_expiry_deadline_is_exp_plus_the_grace() {
        let fx = jwt_fx(AuthMode::Enforce).await;
        let in_secs = |exp_in: i64| {
            let p = Principal::jwt(
                "billing",
                SystemTime::now()
                    .checked_add(Duration::from_secs(exp_in as u64))
                    .unwrap(),
            );
            fx.auth.expiry_deadline(Some(&p))
        };
        // The default grace is 30 s.
        let d = in_secs(20).expect("a deadline");
        let left = d.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            left > Duration::from_secs(45) && left <= Duration::from_secs(50),
            "{left:?}"
        );
        // Already past: due now.
        let past = Principal::jwt("billing", SystemTime::UNIX_EPOCH);
        assert!(fx.auth.expiry_deadline(Some(&past)).unwrap() <= tokio::time::Instant::now());
        // A token never expires; neither does a JWT whose exp is beyond any process's life.
        assert_eq!(
            fx.auth
                .expiry_deadline(Some(&tok("tok-billing", "billing"))),
            None
        );
        assert_eq!(fx.auth.expiry_deadline(None), None);
        assert_eq!(in_secs(60 * 60 * 24 * 365 * 50), None);
        // Mode off never looks.
        let off = jwt_fx(AuthMode::Off).await;
        let p = Principal::jwt("billing", SystemTime::UNIX_EPOCH);
        assert_eq!(off.auth.expiry_deadline(Some(&p)), None);
    }

    #[test]
    fn refreshes_less_than_a_second_apart_are_throttled() {
        let mut gate = RefreshGate::default();
        let t0 = Instant::now();
        assert!(gate.allow(t0));
        assert!(!gate.allow(t0 + Duration::from_millis(100)));
        // A throttled refresh does not restart the wait: one is looked at per second even
        // when the client keeps sending.
        assert!(!gate.allow(t0 + Duration::from_millis(900)));
        assert!(gate.allow(t0 + Duration::from_millis(1001)));
        assert!(!gate.allow(t0 + Duration::from_millis(1500)));
        assert!(gate.allow(t0 + Duration::from_millis(2002)));
    }
}

/// Client certificates as credentials (roadmap P5c, ADR 0020).
#[cfg(test)]
mod cert_tests {
    use super::*;
    use crate::pki_test_support::{ClientSpec, Pki};
    use crate::telemetry::test_support::{TestMetrics, capture_logs, logged, test_metrics};

    fn acl_text() -> String {
        format!(
            r#"
[[token]]
token_sha256 = "{orders}"
service = "orders"
handle = ["*"]
send = ["*"]

[[token]]
token_sha256 = "{billing}"
service = "billing"
handle = ["billing.*"]

[[service]]
name = "orders"
handle = ["orders.*"]
send = ["billing.Charge"]

[[service]]
name = "billing"
handle = ["billing.*"]
"#,
            orders = hash_token("tok-orders"),
            billing = hash_token("tok-billing"),
        )
    }

    fn authorizer(mode: AuthMode) -> (Arc<Authorizer>, TestMetrics) {
        let m = test_metrics();
        let a = Authorizer::with_acl(mode, Acl::parse(&acl_text()).unwrap(), m.metrics.clone());
        (a, m)
    }

    fn der(spec: &ClientSpec) -> Vec<u8> {
        Pki::new("test CA").client_der(spec).0
    }

    fn svc(name: &str) -> Vec<u8> {
        der(&ClientSpec::service(name))
    }

    fn token(t: &str) -> Credentials {
        Credentials {
            kind: Some(credentials::Kind::Token(t.to_string())),
        }
    }

    fn types(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn results(m: &TestMetrics, result: &str) -> Option<u64> {
        m.counter("kraken_hub_client_certificates_total", "result", result)
    }

    fn violations(m: &TestMetrics, reason: &str) -> Option<u64> {
        m.counter("kraken_hub_auth_violations_total", "reason", reason)
    }

    #[test]
    fn enforce_admits_a_certificate_whose_service_has_a_table() {
        let (a, m) = authorizer(AuthMode::Enforce);
        let bytes = svc("orders");
        let adm = a.admit_peer(Some(&bytes), None, "orders", &types(&["orders.Place"]));
        assert_eq!(adm.refused, None);
        let p = adm.principal.expect("a principal");
        assert_eq!(p.kind(), CredentialKind::Certificate);
        assert_eq!(p.service, "orders");
        assert_eq!(p.key, AclKey::Service("orders".into()));
        assert!(p.expires_at.is_some_and(|t| t > SystemTime::now()));
        assert_eq!(p.fingerprint.as_deref().map(str::len), Some(8));
        assert_eq!(
            adm.status,
            AuthStatus::Authenticated {
                service: "orders".into()
            }
        );
        assert_eq!(results(&m, "ok"), Some(1));
        assert_eq!(CredentialKind::Certificate.as_str(), "certificate");
    }

    #[test]
    fn a_certificate_that_names_no_service_is_refused_with_permission_denied() {
        let (a, m) = authorizer(AuthMode::Enforce);
        for (spec, result, reason, text) in [
            (
                ClientSpec::default(),
                "no_service",
                "cert_no_service",
                "certificate names no service",
            ),
            (
                ClientSpec {
                    uris: vec!["kraken://service/a".into(), "kraken://service/b".into()],
                    ..ClientSpec::default()
                },
                "multiple_services",
                "cert_multiple_services",
                "certificate names more than one service",
            ),
        ] {
            let adm = a.admit_peer(Some(&der(&spec)), None, "orders", &[]);
            let denied = adm.refused.expect("refused");
            assert_eq!(denied.to_status().code(), tonic::Code::PermissionDenied);
            assert_eq!(denied.message, text);
            assert_eq!(denied.reason, reason);
            assert!(adm.principal.is_none());
            assert_eq!(results(&m, result), Some(1), "{result}");
            assert_eq!(violations(&m, reason), Some(1), "{reason}");
        }
        assert_eq!(results(&m, "ok"), None);
    }

    #[test]
    fn permissive_counts_a_certificate_that_names_no_service_and_falls_back_to_the_credentials() {
        let (a, m) = authorizer(AuthMode::Permissive);
        let bytes = der(&ClientSpec::default());
        let adm = a.admit_peer(Some(&bytes), None, "orders", &[]);
        assert_eq!(adm.refused, None);
        assert_eq!(adm.status, AuthStatus::UnauthenticatedPermissive);
        assert_eq!(results(&m, "no_service"), Some(1));
        // With a valid token next to it, the token is the principal.
        let adm = a.admit_peer(Some(&bytes), Some(&token("tok-orders")), "orders", &[]);
        assert_eq!(adm.principal.unwrap().kind(), CredentialKind::Token);
        assert_eq!(results(&m, "no_service"), Some(2));
    }

    #[test]
    fn a_certificate_for_another_service_than_the_one_registered_is_refused() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let denied = a
            .admit_peer(Some(&svc("billing")), None, "orders", &[])
            .refused
            .unwrap();
        assert_eq!(denied.reason, "wrong_service");
        assert_eq!(denied.to_status().code(), tonic::Code::PermissionDenied);
        assert!(denied.message.contains("certificate"), "{}", denied.message);
        assert!(denied.message.contains("billing") && denied.message.contains("orders"));
    }

    #[test]
    fn a_certificate_whose_service_has_no_table_is_refused_and_a_token_entry_does_not_count() {
        let m = test_metrics();
        // `orders` has a [[token]] entry only.
        let acl = format!(
            "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"orders\"\nhandle = [\"*\"]\n",
            hash_token("tok-orders")
        );
        let a = Authorizer::with_acl(
            AuthMode::Enforce,
            Acl::parse(&acl).unwrap(),
            m.metrics.clone(),
        );
        let denied = a
            .admit_peer(Some(&svc("orders")), None, "orders", &[])
            .refused
            .unwrap();
        assert_eq!(denied.reason, "no_acl_entry");
        assert_eq!(denied.to_status().code(), tonic::Code::PermissionDenied);
        // The certificate itself was fine: the ACL refused it.
        assert_eq!(results(&m, "ok"), Some(1));
    }

    #[test]
    fn the_rights_come_from_the_service_table() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let bytes = svc("orders");
        // The service table allows orders.*; the token entry of the same service allows * but
        // is not consulted.
        assert_eq!(
            a.admit_peer(Some(&bytes), None, "orders", &types(&["orders.Place"]))
                .refused,
            None
        );
        let denied = a
            .admit_peer(Some(&bytes), None, "orders", &types(&["billing.Charge"]))
            .refused
            .unwrap();
        assert_eq!(denied.reason, "handle_denied");
        let p = Principal::certificate(&crate::mtls::identify(&bytes).unwrap());
        assert!(a.check_send(Some(&p), "orders", "billing.Charge").is_ok());
        assert_eq!(
            a.check_send(Some(&p), "orders", "other.Thing")
                .unwrap_err()
                .reason,
            "send_denied"
        );
    }

    #[test]
    fn credentials_next_to_a_certificate_must_name_the_same_service() {
        let (a, m) = authorizer(AuthMode::Enforce);
        let bytes = svc("orders");
        // The same service: the certificate is the principal, and the token adds nothing.
        let adm = a.admit_peer(Some(&bytes), Some(&token("tok-orders")), "orders", &[]);
        assert_eq!(adm.refused, None);
        assert_eq!(adm.principal.unwrap().kind(), CredentialKind::Certificate);
        assert_eq!(results(&m, "ok"), Some(1));
        // Another service: PERMISSION_DENIED.
        let adm = a.admit_peer(Some(&bytes), Some(&token("tok-billing")), "orders", &[]);
        let denied = adm.refused.unwrap();
        assert_eq!(denied.to_status().code(), tonic::Code::PermissionDenied);
        assert_eq!(
            denied.message,
            "credentials and certificate name different services"
        );
        assert_eq!(denied.reason, "cert_mismatch");
        assert_eq!(results(&m, "mismatch"), Some(1));
        assert_eq!(violations(&m, "cert_mismatch"), Some(1));
        // A credential that does not resolve at all is refused as it always is.
        let denied = a
            .admit_peer(Some(&bytes), Some(&token("nope")), "orders", &[])
            .refused
            .unwrap();
        assert_eq!(denied.reason, "unknown_token");
        assert_eq!(denied.to_status().code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn permissive_counts_a_mismatch_allows_it_and_the_certificate_wins() {
        let (a, m) = authorizer(AuthMode::Permissive);
        let adm = a.admit_peer(
            Some(&svc("orders")),
            Some(&token("tok-billing")),
            "orders",
            &[],
        );
        assert_eq!(adm.refused, None);
        let p = adm.principal.unwrap();
        assert_eq!(
            (p.kind(), p.service.as_str()),
            (CredentialKind::Certificate, "orders")
        );
        assert_eq!(results(&m, "mismatch"), Some(1));
        assert_eq!(violations(&m, "cert_mismatch"), Some(1));
    }

    #[test]
    fn off_ignores_a_certificate() {
        let (a, m) = authorizer(AuthMode::Off);
        let adm = a.admit_peer(Some(&svc("billing")), None, "orders", &[]);
        assert_eq!(adm.principal, None);
        assert_eq!(adm.status, AuthStatus::Off);
        assert_eq!(adm.refused, None);
        assert_eq!(
            m.counter_total("kraken_hub_client_certificates_total"),
            None
        );
        assert_eq!(a.expiry_deadline(None), None);
    }

    #[test]
    fn a_certificate_expires_at_not_after_with_no_grace() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let bytes = der(&ClientSpec::service("orders").valid_for(Duration::from_secs(40)));
        let p = Principal::certificate(&crate::mtls::identify(&bytes).unwrap());
        let left = a
            .expiry_deadline(Some(&p))
            .unwrap()
            .saturating_duration_since(tokio::time::Instant::now());
        assert!(
            left > Duration::from_secs(35) && left <= Duration::from_secs(40),
            "{left:?}"
        );
        let off = authorizer(AuthMode::Off).0;
        assert_eq!(off.expiry_deadline(Some(&p)), None);
    }

    #[test]
    fn an_already_expired_certificate_is_refused_at_register() {
        let (a, m) = authorizer(AuthMode::Enforce);
        let mut p = crate::mtls::identify(&svc("orders")).unwrap();
        p.not_after = SystemTime::now() - Duration::from_secs(1);
        let adm = a.admit_identified(Ok(p), None, "orders", &[]);
        let denied = adm.refused.unwrap();
        assert_eq!(denied.reason, "expired");
        assert_eq!(denied.to_status().code(), tonic::Code::Unauthenticated);
        assert_eq!(denied.message, "certificate expired");
        assert_eq!(results(&m, "expired"), Some(1));
    }

    #[test]
    fn the_expiry_of_a_certificate_session_closes_in_enforce_and_counts_once_in_permissive() {
        let mut cert = crate::mtls::identify(&svc("orders")).unwrap();
        cert.not_after = SystemTime::now() - Duration::from_secs(1);
        let p = Principal::certificate(&cert);
        let (a, m) = authorizer(AuthMode::Enforce);
        let Expiry::Close(denied) = a.expired(&p, "orders") else {
            panic!("enforce closes")
        };
        assert_eq!(denied.to_status().code(), tonic::Code::Unauthenticated);
        assert_eq!(denied.message, "certificate expired");
        assert_eq!(results(&m, "expired"), Some(1));
        assert_eq!(violations(&m, "expired"), Some(1));

        let (a, m) = authorizer(AuthMode::Permissive);
        assert!(matches!(a.expired(&p, "orders"), Expiry::Keep));
        assert_eq!(results(&m, "expired"), Some(1));
        let (a, m) = authorizer(AuthMode::Off);
        assert!(matches!(a.expired(&p, "orders"), Expiry::Keep));
        assert_eq!(results(&m, "expired"), None);
    }

    #[test]
    fn a_reload_that_removes_the_service_table_denies_the_certificate_session() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let p = Principal::certificate(&crate::mtls::identify(&svc("orders")).unwrap());
        assert!(
            a.recheck(Some(&p), "orders", &types(&["orders.Place"]))
                .is_empty()
        );
        let m = test_metrics();
        let without = Authorizer::with_acl(
            AuthMode::Enforce,
            Acl::parse("[[service]]\nname = \"billing\"\n").unwrap(),
            m.metrics.clone(),
        );
        let denials = without.recheck(Some(&p), "orders", &[]);
        assert_eq!(denials[0].reason, "no_acl_entry");
        let narrowed = Authorizer::with_acl(
            AuthMode::Enforce,
            Acl::parse("[[service]]\nname = \"orders\"\nhandle = [\"orders.Get\"]\n").unwrap(),
            m.metrics.clone(),
        );
        assert_eq!(
            narrowed.recheck(Some(&p), "orders", &types(&["orders.Place"]))[0].reason,
            "handle_denied"
        );
    }

    #[test]
    fn the_grpc_topology_accepts_a_certificate_whose_service_has_a_table() {
        let (a, _m) = authorizer(AuthMode::Enforce);
        let refused =
            |r: Result<(), Status>| assert_eq!(r.unwrap_err().code(), tonic::Code::Unauthenticated);
        assert!(
            a.check_topology_peer(None, Some(&svc("orders")), None)
                .is_ok()
        );
        // No service, an unknown service, an expired certificate: refused.
        refused(a.check_topology_peer(None, Some(&der(&ClientSpec::default())), None));
        refused(a.check_topology_peer(None, Some(&svc("nobody")), None));
        refused(a.check_topology_peer(None, Some(b"junk"), None));
        refused(a.check_topology_peer(None, None, None));
        // Permissive allows an unauthenticated call (counted) but a good certificate is no violation.
        let (p, pm) = authorizer(AuthMode::Permissive);
        assert!(
            p.check_topology_peer(None, Some(&svc("orders")), None)
                .is_ok()
        );
        assert_eq!(violations(&pm, "topology_unauthenticated"), None);
        // Off: a certificate means nothing; the dashboard token decides.
        let (o, _) = authorizer(AuthMode::Off);
        let dash = Secret::new("dash-token-0123456789");
        refused(o.check_topology_peer(None, Some(&svc("orders")), Some(&dash)));
        assert!(
            o.check_topology_peer(Some("dash-token-0123456789"), None, Some(&dash))
                .is_ok()
        );
    }

    #[test]
    fn logs_name_a_certificate_by_the_fingerprint_prefix_only() {
        capture_logs();
        let (a, _m) = authorizer(AuthMode::Enforce);
        let bytes = svc("orders");
        let adm = a.admit_peer(Some(&bytes), None, "orders", &types(&["billing.Charge"]));
        assert!(adm.refused.is_some());
        let cert = crate::mtls::identify(&bytes).unwrap();
        let full: String = cert
            .fingerprint
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let lines = logged();
        let text = format!("{lines:?}");
        assert!(text.contains(&format!("cert:{}", cert.short())), "{text}");
        assert!(
            !text.contains(&full),
            "the whole fingerprint is never logged"
        );
        assert!(!text.contains("BEGIN"), "no certificate material");
        let p = Principal::certificate(&cert);
        assert_eq!(p.label(), format!("cert:{}", cert.short()));
    }
}
