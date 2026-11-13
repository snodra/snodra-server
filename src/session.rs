//! Outbound side of every connected instance's gRPC stream.
//!
//! The router never touches a stream directly: it calls `Sessions::send`, which uses a
//! non-blocking `try_send` so a slow instance reports `Busy` instead of stalling the router,
//! and a vanished instance reports `Gone`.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::info;

use crate::auth::{AuthStatus, Principal};
use crate::context::ContextId;
use crate::processors::{self, ProcessorReport};
use crate::proto::{InstanceStatus, ServerMessage};
use crate::registry::{InstanceId, Placement};

#[derive(Debug, PartialEq, Eq)]
pub enum SendError {
    /// Unknown instance, or its stream has closed.
    Gone,
    /// The instance's outbound buffer is full.
    Busy,
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Gone => write!(f, "instance is gone"),
            Self::Busy => write!(f, "instance outbound buffer is full"),
        }
    }
}

/// Hub ids kept from one `InstanceStatus`; further ids are ignored.
pub const MAX_STATUS_HUB_IDS: usize = 64;
/// Longest hub id kept from an `InstanceStatus`, in bytes; a longer id is ignored (not cut, so it
/// can never match another hub's id by accident).
pub const MAX_STATUS_HUB_ID_BYTES: usize = 128;

/// The latest `InstanceStatus` of a session (ADR 0016), bounded; replaced by each report and
/// dropped with the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusDetail {
    /// `connected_hub_ids` as reported: empty and over-long ids and duplicates removed, at most
    /// [`MAX_STATUS_HUB_IDS`], in the reported order.
    pub connected_hub_ids: Vec<String>,
    pub handlers_in_flight: u32,
    /// `processors` as reported, bounded ([`crate::processors::bound`], ADR 0022).
    pub processors: Vec<ProcessorReport>,
    /// When the hub received the report.
    pub at: Instant,
}

impl StatusDetail {
    fn from_report(report: &InstanceStatus, at: Instant) -> Self {
        let mut ids: Vec<String> = Vec::new();
        for id in &report.connected_hub_ids {
            if ids.len() == MAX_STATUS_HUB_IDS {
                break;
            }
            if id.is_empty() || id.len() > MAX_STATUS_HUB_ID_BYTES || ids.contains(id) {
                continue;
            }
            ids.push(id.clone());
        }
        Self {
            connected_hub_ids: ids,
            handlers_in_flight: report.handlers_in_flight,
            processors: processors::bound(&report.processors),
            at,
        }
    }
}

/// A newer session already holds the instance id: an older stream never replaces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Superseded;

/// What the first `Register` of a session's stream fixed for the stream's life.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionMeta {
    /// `Register.client_version` as sent.
    pub client_version: String,
    /// Load factor and tags from the first `Register`, re-applied on every re-register.
    pub placement: Placement,
    /// Who the session's credentials speak for, when their ACL entry existed (ADR 0018, 0019);
    /// every later request is judged against the current ACL entry this names.
    pub principal: Option<Arc<Principal>>,
    /// How the session authenticated, for the topology and the dashboard.
    pub auth: AuthStatus,
}

pub struct Session {
    /// The context the session registered in (always `default`; contexts are deferred).
    pub context: ContextId,
    pub service: String,
    pub meta: SessionMeta,
    pub tx: mpsc::Sender<Result<ServerMessage, Status>>,
    pub last_heartbeat: Instant,
    /// When the stream registered (set at `insert`).
    pub connected_since: Instant,
    /// Unique per `insert`; lets a stream remove only its own session.
    pub token: u64,
    /// Cancelled when the session is removed or replaced; the stream task exits on it at once
    /// instead of waiting for the client to drain the response body.
    pub cancel: CancellationToken,
    /// The latest `InstanceStatus`, if the session reported one (ADR 0016).
    pub status: Option<StatusDetail>,
    /// Every type of the latest accepted `Register` (commands, queries, shared queries), for
    /// re-checking the registration when the ACL changes.
    pub handled: Vec<String>,
}

/// What the router needs to know about the sender of a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SenderInfo {
    pub token: u64,
    pub service: String,
    /// See [`SessionMeta::principal`].
    pub principal: Option<Arc<Principal>>,
}

/// Read-only copy of one session's observable state, for the topology view.
#[derive(Debug, Clone)]
pub struct SessionDetail {
    pub context: ContextId,
    pub instance_id: InstanceId,
    pub service: String,
    pub client_version: String,
    pub connected_since: Instant,
    pub last_heartbeat: Instant,
    /// The latest `InstanceStatus`, `None` when never reported.
    pub status: Option<StatusDetail>,
    /// How the session authenticated (ADR 0018).
    pub auth: AuthStatus,
    /// Who its credentials speak for: the kind and, for a JWT, the expiry the topology shows.
    pub principal: Option<Arc<Principal>>,
}

/// A session whose latest status lists an event processor: where a control request for it can go
/// (ADR 0023).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessorHolder {
    pub context: ContextId,
    pub instance_id: InstanceId,
    pub token: u64,
    /// When the hub received the status that lists the processor.
    pub reported_at: Instant,
    /// The event store that status names for the processor (ADR 0027); empty when it names none.
    pub event_store: String,
    /// That event store declares the `replay` capability (a reset is offered only then).
    pub replay: bool,
}

/// What the ACL check of a reload needs to know about a session.
#[derive(Debug, Clone)]
pub struct AuthView {
    pub context: ContextId,
    pub instance_id: InstanceId,
    pub token: u64,
    pub service: String,
    pub principal: Option<Arc<Principal>>,
    pub handled: Vec<String>,
}

/// Instance id -> session. Every lookup also matches the session's [`ContextId`], so a session is
/// only ever found in the context it registered in. Lock order: the sessions lock may be held while the registry lock is
/// taken (inside [`Sessions::with_current`] and [`Sessions::remove_if_then`] closures), never
/// the reverse. Closures must not call back into `Sessions` (the mutex is not reentrant).
#[derive(Default)]
pub struct Sessions {
    inner: Mutex<HashMap<InstanceId, Session>>,
    next_token: AtomicU64,
}

impl Sessions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a session, replacing (and cancelling) any existing one with the same id. Returns the
    /// session's token.
    pub fn insert(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        service: &str,
        meta: SessionMeta,
        tx: mpsc::Sender<Result<ServerMessage, Status>>,
        cancel: CancellationToken,
    ) -> u64 {
        self.insert_replacing(ctx, instance_id, service, meta, tx, cancel)
            .0
    }

    /// A fresh session token, for a stream that must open state keyed by it (its flow) before
    /// the session becomes current ([`Sessions::insert_with_token`]).
    pub fn allocate_token(&self) -> u64 {
        self.next_token.fetch_add(1, Ordering::Relaxed)
    }

    /// Inserts the session under a token from [`Sessions::allocate_token`], replacing (and
    /// cancelling) an older session of the instance id; a newer one is never replaced.
    #[allow(clippy::too_many_arguments)] // the fields of a Session, as `insert` takes them
    pub fn insert_with_token(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        service: &str,
        meta: SessionMeta,
        tx: mpsc::Sender<Result<ServerMessage, Status>>,
        cancel: CancellationToken,
        token: u64,
    ) -> Result<Option<u64>, Superseded> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();
        // Tokens grow with every stream, so the newest stream of an instance id always wins, in
        // the sessions map and (opened first, ADR 0014) in the router's flow state alike.
        if inner.get(instance_id).is_some_and(|s| s.token > token) {
            return Err(Superseded);
        }
        let session = Session {
            context: ctx.clone(),
            service: service.to_string(),
            meta,
            tx,
            last_heartbeat: now,
            connected_since: now,
            token,
            cancel,
            status: None,
            handled: Vec::new(),
        };
        let old = inner.insert(instance_id.to_string(), session);
        drop(inner);
        Ok(old.map(|old| {
            old.cancel.cancel();
            info!(
                instance = %instance_id,
                service = %service,
                old_service = %old.service,
                "session replaced by a new stream"
            );
            old.token
        }))
    }

    /// Like [`Sessions::insert`], also returning the token of the session it replaced, so the
    /// caller can end that session's requests (its stream task finds itself replaced and does not).
    pub fn insert_replacing(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        service: &str,
        meta: SessionMeta,
        tx: mpsc::Sender<Result<ServerMessage, Status>>,
        cancel: CancellationToken,
    ) -> (u64, Option<u64>) {
        let token = self.allocate_token();
        let replaced = self
            .insert_with_token(ctx, instance_id, service, meta, tx, cancel, token)
            .expect("a fresh token is the newest");
        (token, replaced)
    }

    /// The current session's token, its service and who it speaks for.
    pub fn sender_info(&self, ctx: &ContextId, instance_id: &str) -> Option<SenderInfo> {
        self.inner
            .lock()
            .unwrap()
            .get(instance_id)
            .filter(|s| s.context == *ctx)
            .map(|s| SenderInfo {
                token: s.token,
                service: s.service.clone(),
                principal: s.meta.principal.clone(),
            })
    }

    /// Records the types of the session's latest accepted `Register`; ignored unless `token` is
    /// still the current session of `instance_id`.
    pub fn set_handled(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        token: u64,
        handled: Vec<String>,
    ) {
        if let Some(s) = self
            .inner
            .lock()
            .unwrap()
            .get_mut(instance_id)
            .filter(|s| s.context == *ctx && s.token == token)
        {
            s.handled = handled;
        }
    }

    /// Every session's identity and registration, for the ACL check of a reload.
    pub fn auth_views(&self) -> Vec<AuthView> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, s)| AuthView {
                context: s.context.clone(),
                instance_id: id.clone(),
                token: s.token,
                service: s.service.clone(),
                principal: s.meta.principal.clone(),
                handled: s.handled.clone(),
            })
            .collect()
    }

    /// The token of the current session of `instance_id`, if any.
    pub fn token_of(&self, ctx: &ContextId, instance_id: &str) -> Option<u64> {
        self.inner
            .lock()
            .unwrap()
            .get(instance_id)
            .filter(|s| s.context == *ctx)
            .map(|s| s.token)
    }

    /// Like [`Sessions::send`], but only to the session identified by `token`: a replaced (or
    /// removed) session is `Gone` even if a newer one uses the same instance id.
    pub fn send_if_current(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        token: u64,
        msg: ServerMessage,
    ) -> Result<(), SendError> {
        let tx = self
            .inner
            .lock()
            .unwrap()
            .get(instance_id)
            .filter(|s| s.context == *ctx && s.token == token)
            .map(|s| s.tx.clone())
            .ok_or(SendError::Gone)?;
        tx.try_send(Ok(msg)).map_err(|e| match e {
            TrySendError::Full(_) => SendError::Busy,
            TrySendError::Closed(_) => SendError::Gone,
        })
    }

    /// Runs `f` under the sessions lock if the session `token` is still the current one for
    /// `instance_id`; `None` (and `f` not run) otherwise. Used for registry registration so a
    /// stream can only register while it provably owns the instance id.
    pub fn with_current<R>(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        token: u64,
        f: impl FnOnce() -> R,
    ) -> Option<R> {
        let inner = self.inner.lock().unwrap();
        if inner
            .get(instance_id)
            .is_some_and(|s| s.token == token && s.context == *ctx)
        {
            Some(f())
        } else {
            None
        }
    }

    /// Removes the session only if it is still the one identified by `token`, cancels it, and
    /// runs `f` with it while still holding the sessions lock (so no newer session for the same
    /// id can be inserted before `f`, typically the registry unregister, has run).
    pub fn remove_if_then<R>(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        token: u64,
        f: impl FnOnce(Session) -> R,
    ) -> Option<R> {
        let mut inner = self.inner.lock().unwrap();
        if !inner
            .get(instance_id)
            .is_some_and(|s| s.token == token && s.context == *ctx)
        {
            return None;
        }
        let session = inner.remove(instance_id).expect("checked above");
        session.cancel.cancel();
        Some(f(session))
    }

    /// Removes the session only if it is still the one identified by `token`.
    pub fn remove_if(&self, ctx: &ContextId, instance_id: &str, token: u64) -> Option<Session> {
        self.remove_if_then(ctx, instance_id, token, |s| s)
    }

    /// Every current session as `(context, instance id, token)`, for a disconnect sweep (shutdown).
    pub fn all_with_tokens(&self) -> Vec<(ContextId, InstanceId, u64)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, s)| (s.context.clone(), id.clone(), s.token))
            .collect()
    }

    /// Every session's observable state, cloned out under one brief lock.
    pub fn details(&self) -> Vec<SessionDetail> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, s)| SessionDetail {
                context: s.context.clone(),
                instance_id: id.clone(),
                service: s.service.clone(),
                client_version: s.meta.client_version.clone(),
                connected_since: s.connected_since,
                last_heartbeat: s.last_heartbeat,
                status: s.status.clone(),
                auth: s.meta.auth.clone(),
                principal: s.meta.principal.clone(),
            })
            .collect()
    }

    /// Keeps `report` (bounded, ADR 0016 and 0022) as the latest `InstanceStatus` of the session
    /// `token`, replacing the previous one. Ignored (`false`) unless that session is still the
    /// current one for `instance_id`.
    pub fn set_status(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        token: u64,
        report: &InstanceStatus,
        now: Instant,
    ) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let Some(session) = inner
            .get_mut(instance_id)
            .filter(|s| s.context == *ctx && s.token == token)
        else {
            return false;
        };
        session.status = Some(StatusDetail::from_report(report, now));
        true
    }

    /// Replaces the credentials of the session `token` after a refresh: who it speaks for and
    /// how it authenticated. Ignored (`false`) unless that session is still the current one.
    pub fn set_credentials(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        token: u64,
        principal: Option<Arc<Principal>>,
        auth: AuthStatus,
    ) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let Some(session) = inner
            .get_mut(instance_id)
            .filter(|s| s.context == *ctx && s.token == token)
        else {
            return false;
        };
        session.meta.principal = principal;
        session.meta.auth = auth;
        true
    }

    /// A weak handle on the outbound channel of `instance_id`'s session `token` (`None` when that
    /// session is no longer current). Holding it does not keep the stream open.
    pub fn weak_sender_if_current(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        token: u64,
    ) -> Option<mpsc::WeakSender<Result<ServerMessage, Status>>> {
        self.inner
            .lock()
            .unwrap()
            .get(instance_id)
            .filter(|s| s.context == *ctx && s.token == token)
            .map(|s| s.tx.downgrade())
    }

    /// A clone of the instance's outbound sender, for a delivery that must outlive `try_send`.
    pub fn sender(
        &self,
        ctx: &ContextId,
        instance_id: &str,
    ) -> Option<mpsc::Sender<Result<ServerMessage, Status>>> {
        self.inner
            .lock()
            .unwrap()
            .get(instance_id)
            .filter(|s| s.context == *ctx)
            .map(|s| s.tx.clone())
    }

    /// try_send: full buffer -> Busy, closed or unknown -> Gone.
    pub fn send(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        msg: ServerMessage,
    ) -> Result<(), SendError> {
        let tx = self.sender(ctx, instance_id).ok_or(SendError::Gone)?;
        tx.try_send(Ok(msg)).map_err(|e| match e {
            TrySendError::Full(_) => SendError::Busy,
            TrySendError::Closed(_) => SendError::Gone,
        })
    }

    pub fn touch(&self, ctx: &ContextId, instance_id: &str, now: Instant) {
        if let Some(s) = self
            .inner
            .lock()
            .unwrap()
            .get_mut(instance_id)
            .filter(|s| s.context == *ctx)
        {
            s.last_heartbeat = now;
        }
    }

    /// Instances whose last heartbeat is strictly older than `timeout`.
    pub fn stale(&self, now: Instant, timeout: Duration) -> Vec<InstanceId> {
        let mut ids: Vec<InstanceId> = self
            .stale_with_tokens(now, timeout)
            .into_iter()
            .map(|(_, id, _)| id)
            .collect();
        ids.sort();
        ids
    }

    /// Like [`Sessions::stale`], with each session's context and token for [`Sessions::remove_if`].
    pub fn stale_with_tokens(
        &self,
        now: Instant,
        timeout: Duration,
    ) -> Vec<(ContextId, InstanceId, u64)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, s)| s.last_heartbeat + timeout < now)
            .map(|(id, s)| (s.context.clone(), id.clone(), s.token))
            .collect()
    }

    /// The sessions of `service` in `ctx` whose latest status lists `processor`; `None` when the
    /// service has no session at all.
    pub fn processor_holders(
        &self,
        ctx: &ContextId,
        service: &str,
        processor: &str,
    ) -> Option<Vec<ProcessorHolder>> {
        let inner = self.inner.lock().unwrap();
        let mut known = false;
        let mut holders = Vec::new();
        for (id, s) in inner.iter() {
            if s.context != *ctx || s.service != service {
                continue;
            }
            known = true;
            let Some(status) = &s.status else { continue };
            if let Some(p) = status.processors.iter().find(|p| p.name == processor) {
                holders.push(ProcessorHolder {
                    context: s.context.clone(),
                    instance_id: id.clone(),
                    token: s.token,
                    reported_at: status.at,
                    event_store: p.event_store.clone(),
                    replay: p.can(crate::processors::CAPABILITY_REPLAY),
                });
            }
        }
        known.then_some(holders)
    }

    /// Every session's latest processor reports as `(service, report)`, for the processor
    /// gauges: only the processors are cloned, under one brief lock.
    pub fn processor_reports(&self, now: Instant) -> Vec<(String, processors::ProcessorInstance)> {
        let inner = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for (id, s) in inner.iter() {
            let Some(status) = &s.status else { continue };
            let age = now.saturating_duration_since(status.at);
            let status_age_ms = i64::try_from(age.as_millis()).unwrap_or(i64::MAX);
            for p in &status.processors {
                out.push((
                    s.service.clone(),
                    processors::ProcessorInstance {
                        instance_id: id.clone(),
                        status_age_ms,
                        report: p.clone(),
                    },
                ));
            }
        }
        out
    }

    pub fn count_by_service(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for s in self.inner.lock().unwrap().values() {
            *counts.entry(s.service.clone()).or_insert(0) += 1;
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::InstanceStatus;

    static D: std::sync::LazyLock<ContextId> = std::sync::LazyLock::new(ContextId::default);

    fn meta() -> SessionMeta {
        SessionMeta::default()
    }
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;

    #[test]
    fn an_older_session_never_replaces_a_newer_one() {
        let sessions = Sessions::new();
        let older = sessions.allocate_token();
        let newer = sessions.allocate_token();
        let (tx, _rx) = mpsc::channel(1);
        let insert = |token| {
            sessions.insert_with_token(
                &D,
                "a",
                "svc",
                meta(),
                tx.clone(),
                CancellationToken::new(),
                token,
            )
        };
        assert_eq!(insert(newer), Ok(None));
        assert_eq!(insert(older), Err(Superseded));
        assert_eq!(sessions.token_of(&D, "a"), Some(newer));
        let newest = sessions.allocate_token();
        assert_eq!(insert(newest), Ok(Some(newer)), "a newer one replaces");
    }

    #[tokio::test]
    async fn a_refresh_replaces_the_principal_and_the_auth_status_of_the_current_session() {
        let sessions = Sessions::new();
        let (tx, _rx) = mpsc::channel(1);
        let mut meta = meta();
        meta.principal = Some(Arc::new(Principal::token([1; 32], "svc")));
        meta.auth = AuthStatus::Authenticated {
            service: "svc".into(),
        };
        let token = sessions.insert(&D, "a", "svc", meta, tx, CancellationToken::new());
        let jwt = Arc::new(Principal::jwt("svc", std::time::SystemTime::UNIX_EPOCH));
        let status = AuthStatus::Authenticated {
            service: "svc".into(),
        };
        // A stale token (an older stream of the instance) changes nothing.
        assert!(!sessions.set_credentials(&D, "a", token + 1, Some(jwt.clone()), status.clone()));
        assert_eq!(
            sessions.auth_views()[0]
                .principal
                .as_deref()
                .map(Principal::label),
            Some(Principal::token([1; 32], "svc").label())
        );
        assert!(sessions.set_credentials(&D, "a", token, Some(jwt.clone()), status.clone()));
        let view = &sessions.auth_views()[0];
        assert_eq!(view.principal.as_deref(), Some(&*jwt));
        let detail = &sessions.details()[0];
        assert_eq!(
            detail.principal.as_deref(),
            Some(&*jwt),
            "the topology reads it too"
        );
        assert_eq!(
            sessions.sender_info(&D, "a").unwrap().principal.as_deref(),
            Some(&*jwt)
        );
        assert!(!sessions.set_credentials(&D, "gone", token, None, status));
    }

    #[tokio::test]
    async fn send_to_unknown_instance_is_gone() {
        let sessions = Sessions::new();
        assert_eq!(
            sessions.send(&D, "nope", ServerMessage::default()),
            Err(SendError::Gone)
        );
    }

    #[tokio::test]
    async fn send_with_full_buffer_is_busy() {
        let sessions = Sessions::new();
        let (tx, _rx) = mpsc::channel(1);
        sessions.insert(&D, "a", "svc", meta(), tx, CancellationToken::new());
        assert_eq!(sessions.send(&D, "a", ServerMessage::default()), Ok(()));
        assert_eq!(
            sessions.send(&D, "a", ServerMessage::default()),
            Err(SendError::Busy)
        );
    }

    #[tokio::test]
    async fn send_to_dropped_receiver_is_gone() {
        let sessions = Sessions::new();
        let (tx, rx) = mpsc::channel(1);
        sessions.insert(&D, "a", "svc", meta(), tx, CancellationToken::new());
        drop(rx);
        assert_eq!(
            sessions.send(&D, "a", ServerMessage::default()),
            Err(SendError::Gone)
        );
    }

    #[test]
    fn stale_returns_only_instances_past_the_timeout() {
        let sessions = Sessions::new();
        let now = Instant::now();
        let mut senders = Vec::new();
        for id in ["ok", "late"] {
            let (tx, rx) = mpsc::channel(1);
            senders.push(rx);
            sessions.insert(&D, id, "svc", meta(), tx, CancellationToken::new());
        }
        sessions.touch(&D, "ok", now - Duration::from_secs(9));
        sessions.touch(&D, "late", now - Duration::from_secs(12));
        assert_eq!(sessions.stale(now, Duration::from_secs(10)), ["late"]);
        assert_eq!(sessions.stale(now, Duration::from_secs(8)), ["late", "ok"]);
    }

    #[test]
    fn remove_if_ignores_replaced_session() {
        let sessions = Sessions::new();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let old = sessions.insert(&D, "a", "svc", meta(), tx1, CancellationToken::new());
        let new = sessions.insert(&D, "a", "svc", meta(), tx2, CancellationToken::new());
        assert_ne!(old, new);
        assert!(sessions.remove_if(&D, "a", old).is_none());
        assert!(sessions.remove_if(&D, "a", new).is_some());
        assert!(sessions.remove_if(&D, "a", new).is_none());
    }

    #[test]
    fn replacing_or_removing_a_session_cancels_it() {
        let sessions = Sessions::new();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let c1 = CancellationToken::new();
        let c2 = CancellationToken::new();
        sessions.insert(&D, "a", "svc", meta(), tx1, c1.clone());
        let new = sessions.insert(&D, "a", "svc", meta(), tx2, c2.clone());
        assert!(c1.is_cancelled());
        assert!(!c2.is_cancelled());
        sessions.remove_if(&D, "a", new).unwrap();
        assert!(c2.is_cancelled());
    }

    fn status(ids: &[&str], handlers: u32) -> InstanceStatus {
        InstanceStatus {
            connected_hub_ids: ids.iter().map(|s| s.to_string()).collect(),
            handlers_in_flight: handlers,
            processors: vec![],
        }
    }

    fn status_of(sessions: &Sessions, id: &str) -> Option<StatusDetail> {
        sessions
            .details()
            .into_iter()
            .find(|d| d.instance_id == id)
            .and_then(|d| d.status)
    }

    #[test]
    fn instance_status_is_stored_replaced_and_dropped_with_the_session() {
        let sessions = Sessions::new();
        let (tx, _rx) = mpsc::channel(1);
        let token = sessions.insert(&D, "a", "svc", meta(), tx, CancellationToken::new());
        assert_eq!(status_of(&sessions, "a"), None, "nothing before a report");
        let t0 = Instant::now();
        assert!(sessions.set_status(&D, "a", token, &status(&["h1", "h2"], 3), t0));
        let first = status_of(&sessions, "a").unwrap();
        assert_eq!(first.connected_hub_ids, ["h1", "h2"]);
        assert_eq!((first.handlers_in_flight, first.at), (3, t0));

        let t1 = t0 + Duration::from_secs(5);
        assert!(sessions.set_status(&D, "a", token, &status(&["h2"], 0), t1));
        let second = status_of(&sessions, "a").unwrap();
        assert_eq!(second.connected_hub_ids, ["h2"], "replaced, not merged");
        assert_eq!((second.handlers_in_flight, second.at), (0, t1));

        // A stale token (an older stream) never writes; the new session starts without status.
        let (tx2, _rx2) = mpsc::channel(1);
        let newer = sessions.insert(&D, "a", "svc", meta(), tx2, CancellationToken::new());
        assert_eq!(status_of(&sessions, "a"), None, "dropped with the session");
        assert!(!sessions.set_status(&D, "a", token, &status(&["old"], 0), t1));
        assert_eq!(status_of(&sessions, "a"), None);
        sessions.remove_if(&D, "a", newer).unwrap();
        assert!(!sessions.set_status(&D, "a", newer, &status(&["h1"], 0), t1));
    }

    #[test]
    fn instance_status_is_kept_for_every_session() {
        let sessions = Sessions::new();
        let (tx, _rx) = mpsc::channel(1);
        let token = sessions.insert(&D, "a", "svc", meta(), tx, CancellationToken::new());
        assert!(sessions.set_status(&D, "a", token, &status(&["h1"], 1), Instant::now()));
        assert_eq!(status_of(&sessions, "a").unwrap().connected_hub_ids, ["h1"]);
    }

    #[test]
    fn instance_status_is_bounded() {
        let sessions = Sessions::new();
        let (tx, _rx) = mpsc::channel(1);
        let token = sessions.insert(&D, "a", "svc", meta(), tx, CancellationToken::new());
        let long = "x".repeat(MAX_STATUS_HUB_ID_BYTES + 1);
        let edge = "y".repeat(MAX_STATUS_HUB_ID_BYTES);
        let mut ids: Vec<String> = vec![long.clone(), String::new(), edge.clone(), "h0".into()];
        ids.extend((0..200).map(|i| format!("hub-{i}")));
        let report = InstanceStatus {
            connected_hub_ids: ids,
            handlers_in_flight: 7,
            processors: vec![],
        };
        assert!(sessions.set_status(&D, "a", token, &report, Instant::now()));
        let kept = status_of(&sessions, "a").unwrap().connected_hub_ids;
        assert_eq!(kept.len(), MAX_STATUS_HUB_IDS);
        assert!(!kept.contains(&long), "an over-long id is ignored, not cut");
        assert!(!kept.contains(&String::new()), "an empty id is ignored");
        assert_eq!(kept[0], edge, "an id of exactly the limit is kept");
        assert_eq!(kept[1], "h0");
        assert_eq!(
            kept[MAX_STATUS_HUB_IDS - 1],
            format!("hub-{}", MAX_STATUS_HUB_IDS - 3)
        );
    }

    fn processor(name: &str) -> crate::proto::ProcessorStatus {
        crate::proto::ProcessorStatus {
            name: name.to_string(),
            subscription: "svc".into(),
            source: "kraken.events".into(),
            state: crate::proto::ProcessorState::ProcessorRunning as i32,
            dead_letter_mode: "collection".into(),
            ..Default::default()
        }
    }

    #[test]
    fn processors_are_kept_bounded() {
        let sessions = Sessions::new();
        let (tx, _rx) = mpsc::channel(1);
        let token = sessions.insert(&D, "p6a", "svc", meta(), tx, CancellationToken::new());
        let mut report = status(&["h1"], 0);
        report.processors = (0..40).map(|i| processor(&format!("p{i}"))).collect();
        assert!(sessions.set_status(&D, "p6a", token, &report, Instant::now()));
        let kept = status_of(&sessions, "p6a").unwrap();
        assert_eq!(kept.connected_hub_ids, ["h1"]);
        assert_eq!(kept.processors.len(), crate::processors::MAX_PROCESSORS);
        assert_eq!(kept.processors[0].name, "p0");

        // The next report replaces the list (an empty one clears it).
        assert!(sessions.set_status(&D, "p6a", token, &status(&["h1"], 0), Instant::now()));
        assert!(status_of(&sessions, "p6a").unwrap().processors.is_empty());
    }

    #[test]
    fn the_holders_of_a_processor_are_the_sessions_whose_latest_status_lists_it() {
        let sessions = Sessions::new();
        let mut rxs = Vec::new();
        let mut insert = |id: &str, service: &str, names: &[&str]| {
            let (tx, rx) = mpsc::channel(1);
            rxs.push(rx);
            let token = sessions.insert(&D, id, service, meta(), tx, CancellationToken::new());
            let mut report = status(&["h1"], 0);
            report.processors = names.iter().map(|n| processor(n)).collect();
            assert!(sessions.set_status(&D, id, token, &report, Instant::now()));
            token
        };
        let a = insert("a", "svc", &["audit", "default"]);
        insert("b", "svc", &["audit"]);
        insert("c", "svc", &["default"]);
        insert("d", "other", &["audit"]);
        let mut holders = sessions.processor_holders(&D, "svc", "audit").unwrap();
        holders.sort_by(|x, y| x.instance_id.cmp(&y.instance_id));
        let seen: Vec<&str> = holders.iter().map(|h| h.instance_id.as_str()).collect();
        assert_eq!(seen, ["a", "b"]);
        assert_eq!(holders[0].token, a);
        assert_eq!(
            sessions.processor_holders(&D, "svc", "orders"),
            Some(vec![]),
            "a known service, an unknown processor"
        );
        assert_eq!(sessions.processor_holders(&D, "ghost", "audit"), None);
    }

    #[test]
    fn instance_status_drops_duplicate_ids() {
        let sessions = Sessions::new();
        let (tx, _rx) = mpsc::channel(1);
        let token = sessions.insert(&D, "a", "svc", meta(), tx, CancellationToken::new());
        let report = status(&["h2", "h1", "h2"], 0);
        assert!(sessions.set_status(&D, "a", token, &report, Instant::now()));
        assert_eq!(
            status_of(&sessions, "a").unwrap().connected_hub_ids,
            ["h2", "h1"]
        );
    }

    #[test]
    fn with_current_runs_only_for_the_current_token() {
        let sessions = Sessions::new();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let old = sessions.insert(&D, "a", "svc", meta(), tx1, CancellationToken::new());
        let new = sessions.insert(&D, "a", "svc", meta(), tx2, CancellationToken::new());
        assert_eq!(sessions.with_current(&D, "a", old, || 1), None);
        assert_eq!(sessions.with_current(&D, "a", new, || 2), Some(2));
        assert_eq!(sessions.with_current(&D, "b", new, || 3), None);
        assert_eq!(sessions.remove_if_then(&D, "a", old, |_| 4), None);
        assert_eq!(
            sessions.remove_if_then(&D, "a", new, |s| s.token),
            Some(new)
        );
        assert_eq!(sessions.with_current(&D, "a", new, || 5), None);
    }
}
