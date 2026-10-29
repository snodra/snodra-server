//! Outbound side of every connected instance's gRPC stream.
//!
//! The router never touches a stream directly: it calls `Sessions::send`, which uses a
//! non-blocking `try_send` so a slow instance reports `Busy` instead of stalling the router,
//! and a vanished instance reports `Gone`.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{self, error::TrySendError};
use tokio_util::sync::CancellationToken;
use tonic::Status;
use tracing::info;

use crate::proto::ServerMessage;
use crate::registry::InstanceId;

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

pub struct Session {
    pub service: String,
    pub tx: mpsc::Sender<Result<ServerMessage, Status>>,
    pub last_heartbeat: Instant,
    /// When the stream registered (set at `insert`).
    pub connected_since: Instant,
    /// Unique per `insert`; lets a stream remove only its own session.
    pub token: u64,
    /// Cancelled when the session is removed or replaced; the stream task exits on it at once
    /// instead of waiting for the client to drain the response body.
    pub cancel: CancellationToken,
}

/// Read-only copy of one session's observable state, for the topology view.
#[derive(Debug, Clone)]
pub struct SessionDetail {
    pub instance_id: InstanceId,
    pub service: String,
    pub connected_since: Instant,
    pub last_heartbeat: Instant,
}

/// Instance id -> session. Lock order: the sessions lock may be held while the registry lock is
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
        instance_id: &str,
        service: &str,
        tx: mpsc::Sender<Result<ServerMessage, Status>>,
        cancel: CancellationToken,
    ) -> u64 {
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let session = Session {
            service: service.to_string(),
            tx,
            last_heartbeat: now,
            connected_since: now,
            token,
            cancel,
        };
        let old = self
            .inner
            .lock()
            .unwrap()
            .insert(instance_id.to_string(), session);
        if let Some(old) = old {
            old.cancel.cancel();
            info!(
                instance = %instance_id,
                service = %service,
                old_service = %old.service,
                "session replaced by a new stream"
            );
        }
        token
    }

    /// Runs `f` under the sessions lock if the session `token` is still the current one for
    /// `instance_id`; `None` (and `f` not run) otherwise. Used for registry registration so a
    /// stream can only register while it provably owns the instance id.
    pub fn with_current<R>(
        &self,
        instance_id: &str,
        token: u64,
        f: impl FnOnce() -> R,
    ) -> Option<R> {
        let inner = self.inner.lock().unwrap();
        if inner.get(instance_id).is_some_and(|s| s.token == token) {
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
        instance_id: &str,
        token: u64,
        f: impl FnOnce(Session) -> R,
    ) -> Option<R> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.get(instance_id).is_some_and(|s| s.token == token) {
            return None;
        }
        let session = inner.remove(instance_id).expect("checked above");
        session.cancel.cancel();
        Some(f(session))
    }

    /// Removes the session only if it is still the one identified by `token`.
    pub fn remove_if(&self, instance_id: &str, token: u64) -> Option<Session> {
        self.remove_if_then(instance_id, token, |s| s)
    }

    /// Every current session as `(instance id, token)`, for a disconnect sweep (shutdown).
    pub fn all_with_tokens(&self) -> Vec<(InstanceId, u64)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, s)| (id.clone(), s.token))
            .collect()
    }

    /// Every session's observable state, cloned out under one brief lock.
    pub fn details(&self) -> Vec<SessionDetail> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .map(|(id, s)| SessionDetail {
                instance_id: id.clone(),
                service: s.service.clone(),
                connected_since: s.connected_since,
                last_heartbeat: s.last_heartbeat,
            })
            .collect()
    }

    /// A clone of the instance's outbound sender, for a delivery that must outlive `try_send`.
    pub fn sender(&self, instance_id: &str) -> Option<mpsc::Sender<Result<ServerMessage, Status>>> {
        self.inner
            .lock()
            .unwrap()
            .get(instance_id)
            .map(|s| s.tx.clone())
    }

    /// try_send: full buffer -> Busy, closed or unknown -> Gone.
    pub fn send(&self, instance_id: &str, msg: ServerMessage) -> Result<(), SendError> {
        let tx = self.sender(instance_id).ok_or(SendError::Gone)?;
        tx.try_send(Ok(msg)).map_err(|e| match e {
            TrySendError::Full(_) => SendError::Busy,
            TrySendError::Closed(_) => SendError::Gone,
        })
    }

    pub fn touch(&self, instance_id: &str, now: Instant) {
        if let Some(s) = self.inner.lock().unwrap().get_mut(instance_id) {
            s.last_heartbeat = now;
        }
    }

    /// Instances whose last heartbeat is strictly older than `now - timeout`.
    pub fn stale(&self, now: Instant, timeout: Duration) -> Vec<InstanceId> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, s)| s.last_heartbeat + timeout < now)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Like [`Sessions::stale`], with each session's token for [`Sessions::remove_if`].
    pub fn stale_with_tokens(&self, now: Instant, timeout: Duration) -> Vec<(InstanceId, u64)> {
        self.inner
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, s)| s.last_heartbeat + timeout < now)
            .map(|(id, s)| (id.clone(), s.token))
            .collect()
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
    use std::time::{Duration, Instant};
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn send_to_unknown_instance_is_gone() {
        let sessions = Sessions::new();
        assert_eq!(
            sessions.send("nope", ServerMessage::default()),
            Err(SendError::Gone)
        );
    }

    #[tokio::test]
    async fn send_with_full_buffer_is_busy() {
        let sessions = Sessions::new();
        let (tx, _rx) = mpsc::channel(1);
        sessions.insert("a", "svc", tx, CancellationToken::new());
        assert_eq!(sessions.send("a", ServerMessage::default()), Ok(()));
        assert_eq!(
            sessions.send("a", ServerMessage::default()),
            Err(SendError::Busy)
        );
    }

    #[tokio::test]
    async fn send_to_dropped_receiver_is_gone() {
        let sessions = Sessions::new();
        let (tx, rx) = mpsc::channel(1);
        sessions.insert("a", "svc", tx, CancellationToken::new());
        drop(rx);
        assert_eq!(
            sessions.send("a", ServerMessage::default()),
            Err(SendError::Gone)
        );
    }

    #[test]
    fn stale_returns_only_instances_past_timeout() {
        let sessions = Sessions::new();
        let now = Instant::now();
        let (tx_a, _rx_a) = mpsc::channel(1);
        let (tx_b, _rx_b) = mpsc::channel(1);
        sessions.insert("a", "svc", tx_a, CancellationToken::new());
        sessions.insert("b", "svc", tx_b, CancellationToken::new());
        sessions.touch("a", now);
        sessions.touch("b", now - Duration::from_secs(31));
        assert_eq!(
            sessions.stale(now, Duration::from_secs(30)),
            vec!["b".to_string()]
        );
    }

    #[test]
    fn remove_if_ignores_replaced_session() {
        let sessions = Sessions::new();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let old = sessions.insert("a", "svc", tx1, CancellationToken::new());
        let new = sessions.insert("a", "svc", tx2, CancellationToken::new());
        assert_ne!(old, new);
        assert!(sessions.remove_if("a", old).is_none());
        assert!(sessions.remove_if("a", new).is_some());
        assert!(sessions.remove_if("a", new).is_none());
    }

    #[test]
    fn replacing_or_removing_a_session_cancels_it() {
        let sessions = Sessions::new();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let c1 = CancellationToken::new();
        let c2 = CancellationToken::new();
        sessions.insert("a", "svc", tx1, c1.clone());
        let new = sessions.insert("a", "svc", tx2, c2.clone());
        assert!(c1.is_cancelled());
        assert!(!c2.is_cancelled());
        sessions.remove_if("a", new).unwrap();
        assert!(c2.is_cancelled());
    }

    #[test]
    fn with_current_runs_only_for_the_current_token() {
        let sessions = Sessions::new();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(1);
        let old = sessions.insert("a", "svc", tx1, CancellationToken::new());
        let new = sessions.insert("a", "svc", tx2, CancellationToken::new());
        assert_eq!(sessions.with_current("a", old, || 1), None);
        assert_eq!(sessions.with_current("a", new, || 2), Some(2));
        assert_eq!(sessions.with_current("b", new, || 3), None);
        assert_eq!(sessions.remove_if_then("a", old, |_| 4), None);
        assert_eq!(sessions.remove_if_then("a", new, |s| s.token), Some(new));
        assert_eq!(sessions.with_current("a", new, || 5), None);
    }
}
