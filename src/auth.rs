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
//! carry at most [`short`] (the first 8 hex characters of the digest). P5b adds a JWT credential
//! and P5c a client certificate; both plug into [`Authorizer::admit`] in the place of the token
//! lookup. P5a has no `Principal` type: a session keeps the digest of its token
//! ([`crate::session::SessionMeta::principal`]) and every later check looks it up in the ACL. A
//! `Principal` (service name, handle and send patterns) that every credential kind produces is
//! deferred to P5b, which will change what a session stores (ADR 0018, "What P5b and P5c add").

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use anyhow::Context;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tonic::Status;
use tracing::{error, info, warn};

use crate::config::Config;
use crate::proto::{Credentials, credentials};
use crate::telemetry::Metrics;

/// SHA-256 of a token.
pub type Digest = [u8; 32];

/// The only context contexts-aware ACL files may name (contexts are deferred, roadmap P8).
pub const DEFAULT_CONTEXT: &str = "default";

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
    /// No checks (the behaviour before P5a).
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
    /// Reserved for processor control (roadmap P6); parsed and kept, not used yet.
    pub control: bool,
    /// Free text for the operator ("rotation 2026-11"); never used for decisions.
    pub label: Option<String>,
}

/// The parsed ACL file.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Acl {
    entries: Vec<AclEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAcl {
    #[serde(default)]
    token: Vec<RawEntry>,
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
            if let Some(contexts) = &e.contexts
                && !(contexts.is_empty() || contexts.iter().all(|c| c == DEFAULT_CONTEXT))
            {
                anyhow::bail!(
                    "{at}: contexts other than [\"default\"] are not supported (contexts are deferred)"
                );
            }
            let patterns = |what: &str, list: &[String]| -> anyhow::Result<Vec<Pattern>> {
                list.iter()
                    .map(|p| Pattern::parse(p).map_err(|m| anyhow::anyhow!("{at}: {what}: {m}")))
                    .collect()
            };
            entries.push(AclEntry {
                digest,
                handle: patterns("handle", &e.handle)?,
                send: patterns("send", &e.send)?,
                control: e.control,
                label: e.label,
                service: e.service,
            });
        }
        Ok(Self { entries })
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

    /// True when some entry is for `service`.
    pub fn has_service(&self, service: &str) -> bool {
        self.entries.iter().any(|e| e.service == service)
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
        }
    }

    /// The `Error.code` a refused request is answered with.
    pub fn error_code(&self) -> crate::proto::ErrorCode {
        match self.code {
            DeniedCode::Unauthenticated => crate::proto::ErrorCode::Unauthenticated,
            DeniedCode::PermissionDenied => crate::proto::ErrorCode::PermissionDenied,
        }
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
    /// The digest of the token when it is in the ACL; what later request checks look up.
    pub principal: Option<Digest>,
    pub status: AuthStatus,
    /// Set only in mode `enforce`: close the stream with it.
    pub refused: Option<Denied>,
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
}

impl Authorizer {
    /// An authorizer that checks nothing.
    pub fn off(metrics: Arc<Metrics>) -> Arc<Self> {
        Self::with_acl(AuthMode::Off, Acl::default(), metrics)
    }

    /// An authorizer over an ACL held in memory (no file to reload).
    pub fn with_acl(mode: AuthMode, acl: Acl, metrics: Arc<Metrics>) -> Arc<Self> {
        metrics.acl_generation_set(1);
        Arc::new(Self {
            mode,
            acl: RwLock::new(Arc::new(acl)),
            generation: AtomicU64::new(1),
            path: None,
            metrics,
            limiter: LogLimiter::default(),
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
        metrics.acl_generation_set(1);
        Ok(Arc::new(Self {
            mode: config.auth_mode,
            acl: RwLock::new(Arc::new(acl)),
            generation: AtomicU64::new(1),
            path: Some(path),
            metrics,
            limiter: LogLimiter::default(),
        }))
    }

    pub fn mode(&self) -> AuthMode {
        self.mode
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
    /// claims and every type it handles. `v2` says whether the client speaks protocol v2 (a v1
    /// client cannot send credentials; the refusal says so). Every violation is logged and
    /// counted; in `enforce` the first is returned in [`Admission::refused`], in `permissive`
    /// the registration goes ahead and `status` says it is unauthenticated.
    pub fn admit(
        &self,
        credentials: Option<&Credentials>,
        v2: bool,
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
        let mut denials = Vec::new();
        let mut matched: Option<(Digest, &AclEntry)> = None;
        match credentials.and_then(|c| c.kind.as_ref()) {
            Some(credentials::Kind::Token(token)) if !token.is_empty() => {
                let digest = digest_of(token);
                match acl.lookup(&digest) {
                    Some(entry) => matched = Some((digest, entry)),
                    None => denials.push(Denied::new(
                        "unknown_token",
                        DeniedCode::Unauthenticated,
                        "unknown token",
                    )),
                }
            }
            Some(credentials::Kind::Jwt(_)) => denials.push(Denied::new(
                "jwt_unsupported",
                DeniedCode::Unauthenticated,
                "jwt credentials are not supported by this hub yet",
            )),
            Some(credentials::Kind::Token(_)) | None => denials.push(Denied::new(
                "no_credentials",
                DeniedCode::Unauthenticated,
                if v2 {
                    "credentials required: Register.credentials.token is missing"
                } else {
                    "credentials required: this client speaks protocol v1 and sends none"
                },
            )),
        }
        if let Some((digest, entry)) = matched {
            denials.extend(judge(entry, &digest, service, handled));
        }
        let key = matched.map(|(d, _)| short(&d));
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
        let status = match matched {
            Some((_, entry)) => AuthStatus::Authenticated {
                service: entry.service.clone(),
            },
            None => AuthStatus::UnauthenticatedPermissive,
        };
        Admission {
            principal: matched.map(|(d, _)| d),
            status,
            refused: (self.mode == AuthMode::Enforce)
                .then(|| denials.into_iter().next())
                .flatten(),
        }
    }

    /// Judges a gRPC `Topology` call carrying `presented` (the bearer token, if any) against the
    /// dashboard token `dashboard` (if one is set) and the ACL. The topology lists every service,
    /// type and instance id, so:
    ///
    /// * `enforce`: the dashboard token or any ACL token; with neither configured, or neither
    ///   presented, the call is `UNAUTHENTICATED`.
    /// * `permissive`: the same, except that an unauthenticated call is allowed when no dashboard
    ///   token is set (it is logged and counted as `topology_unauthenticated`). A dashboard token
    ///   that is set stays required, as before access control existed.
    /// * `off`: only the dashboard token, when set.
    pub fn check_topology(
        &self,
        presented: Option<&str>,
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
        if dashboard_ok || acl_ok {
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
        principal: Option<&Digest>,
        service: &str,
        handled: &[String],
    ) -> Vec<Denied> {
        if self.mode == AuthMode::Off {
            return Vec::new();
        }
        let Some(digest) = principal else {
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
        match self.acl().lookup(digest) {
            None => vec![Denied::new(
                "token_revoked",
                DeniedCode::Unauthenticated,
                "token no longer accepted",
            )],
            Some(entry) => judge(entry, digest, service, handled),
        }
    }

    /// Logs (and, for `Closed` and `Allowed`, counts) what [`Authorizer::recheck`] found.
    pub fn report(
        &self,
        denials: &[Denied],
        service: &str,
        principal: Option<&Digest>,
        disposition: Disposition,
    ) {
        let acl = self.acl();
        let service = clip(service);
        for d in denials {
            let key = principal.map_or_else(|| "-".to_string(), short);
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
        principal: Option<&Digest>,
        sender_service: &str,
        type_name: &str,
    ) -> Result<(), Denied> {
        if self.mode == AuthMode::Off {
            return Ok(());
        }
        let Some(digest) = principal else {
            return Ok(());
        };
        let acl = self.acl();
        let denied = match acl.lookup(digest) {
            None => Some(Denied::new(
                "token_revoked",
                DeniedCode::Unauthenticated,
                "token no longer accepted",
            )),
            Some(entry) if entry.service != sender_service => Some(Denied::new(
                "wrong_service",
                DeniedCode::PermissionDenied,
                format!(
                    "token is no longer allowed to act as service {}",
                    clip(sender_service)
                ),
            )),
            Some(entry) if !any_matches(&entry.send, type_name) => Some(Denied::new(
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
                    key = short(digest),
                    "request refused"
                );
            } else {
                warn!(
                    reason = denied.reason,
                    service,
                    r#type,
                    key = short(digest),
                    "auth violation (permissive, allowed)"
                );
            }
        }
        if enforce { Err(denied) } else { Ok(()) }
    }
}

/// Forbidden types a denial names; the rest is "and N more".
const MAX_LISTED_TYPES: usize = 10;

/// The checks of an entry that matched: the service name, then every handled type.
fn judge(entry: &AclEntry, digest: &Digest, service: &str, handled: &[String]) -> Vec<Denied> {
    let mut out = Vec::new();
    if entry.service != service {
        out.push(Denied::new(
            "wrong_service",
            DeniedCode::PermissionDenied,
            format!(
                "token {} is not allowed to register as service {}",
                short(digest),
                clip(service)
            ),
        ));
    }
    let offending: Vec<&String> = handled
        .iter()
        .filter(|t| !any_matches(&entry.handle, t))
        .collect();
    if !offending.is_empty() {
        // One denial for all of them: a client may offer thousands of types.
        let shown: Vec<String> = offending
            .iter()
            .take(MAX_LISTED_TYPES)
            .map(|t| clip(t))
            .collect();
        let more = match offending.len().saturating_sub(MAX_LISTED_TYPES) {
            0 => String::new(),
            n => format!(" and {n} more"),
        };
        out.push(Denied::new(
            "handle_denied",
            DeniedCode::PermissionDenied,
            format!(
                "service {} is not allowed to handle {} {}{more}",
                clip(service),
                if offending.len() == 1 {
                    "type"
                } else {
                    "types"
                },
                shown.join(", ")
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
        // A dashboard token that is set keeps being required, as before the ACL existed.
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
        let admission = a.admit(Some(&token("nope")), true, &name, &[]);
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
        let admission = a.admit(Some(&token("tok-orders")), true, &name, &[]);
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
            true,
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
        let denials = a.recheck(Some(&digest_of("tok-orders")), "orders", &many);
        // tok-orders handles `*`: nothing is denied. tok-billing handles billing.* and shared.Ping.
        assert!(denials.is_empty());
        let denials = a.recheck(Some(&digest_of("tok-billing")), "billing", &many);
        assert_eq!(denials.len(), 1, "{denials:?}");
        let message = &denials[0].message;
        assert_eq!(denials[0].reason, "handle_denied");
        assert!(
            message.contains("x.T00") && message.contains("x.T09"),
            "{message}"
        );
        assert!(!message.contains("x.T10"), "{message}");
        assert!(message.ends_with("and 15 more"), "{message}");
        // One forbidden type reads as before.
        let one = a.recheck(Some(&digest_of("tok-billing")), "billing", &types(&["x.A"]));
        assert_eq!(
            one[0].message,
            "service billing is not allowed to handle type x.A"
        );
        // A long type name is cut too.
        let long = format!("x.{}", "n".repeat(1000));
        let cut = a.recheck(Some(&digest_of("tok-billing")), "billing", &[long]);
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
            let admission = a.admit(Some(&token("nope")), true, "orders", &[]);
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
        let adm = a.admit(None, false, "whatever", &types(&["x.Y"]));
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
            true,
            "billing",
            &types(&["billing.Pay", "shared.Ping"]),
        );
        assert_eq!(adm.refused, None);
        assert_eq!(adm.principal, Some(digest_of("tok-billing")));
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
        let refused = |creds: Option<Credentials>, v2: bool, service: &str, handled: &[&str]| {
            a.admit(creds.as_ref(), v2, service, &types(handled))
                .refused
                .expect("refused")
        };
        let d = refused(None, true, "billing", &[]);
        assert_eq!(
            (d.reason, d.code),
            ("no_credentials", DeniedCode::Unauthenticated)
        );
        let d = refused(None, false, "billing", &[]);
        assert!(d.message.contains("protocol v1"), "{}", d.message);
        let d = refused(Some(token("")), true, "billing", &[]);
        assert_eq!(d.reason, "no_credentials");
        let d = refused(Some(token("who?")), true, "billing", &[]);
        assert_eq!(
            (d.reason, d.code),
            ("unknown_token", DeniedCode::Unauthenticated)
        );
        assert!(!d.message.contains("who?"));
        let jwt = Credentials {
            kind: Some(credentials::Kind::Jwt("a.b.c".into())),
        };
        let d = refused(Some(jwt), true, "billing", &[]);
        assert_eq!(d.reason, "jwt_unsupported");
        assert_eq!(
            d.message,
            "jwt credentials are not supported by this hub yet"
        );
        let d = refused(Some(token("tok-billing")), true, "orders", &[]);
        assert_eq!(
            (d.reason, d.code),
            ("wrong_service", DeniedCode::PermissionDenied)
        );
        let d = refused(
            Some(token("tok-billing")),
            true,
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
            refused(None, true, "billing", &[]).to_status().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[test]
    fn permissive_allows_every_violation_but_counts_it() {
        let (a, m) = authorizer(AuthMode::Permissive);
        let adm = a.admit(None, false, "billing", &types(&["billing.Pay"]));
        assert_eq!(adm.refused, None);
        assert_eq!(adm.principal, None);
        assert_eq!(adm.status, AuthStatus::UnauthenticatedPermissive);
        assert_eq!(adm.status.label(), Some("unauthenticated (permissive)"));
        let adm = a.admit(
            Some(&token("tok-billing")),
            true,
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
            let _ = a.admit(None, true, &format!("random-{n}"), &[]);
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
        let d = digest_of("tok-billing");
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
        let gone = digest_of("tok-gone");
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
        let d = digest_of("tok-billing");
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
        let d = digest_of("tok-billing");
        assert!(
            a.recheck(Some(&d), "billing", &types(&["billing.Pay"]))
                .is_empty()
        );
        assert_eq!(
            a.recheck(Some(&d), "billing", &types(&["x.Y"]))[0].reason,
            "handle_denied"
        );
        assert_eq!(
            a.recheck(Some(&digest_of("x")), "billing", &[])[0].reason,
            "token_revoked"
        );
        assert_eq!(a.recheck(None, "billing", &[])[0].reason, "no_credentials");
        let (p, _m) = authorizer(AuthMode::Permissive);
        assert!(p.recheck(None, "billing", &[]).is_empty());
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
        let _ = a.admit(Some(&token("tok-orders")), true, "orders", &[]);
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
}
