//! Outbound side of every connected instance's gRPC stream.
//!
//! The router never touches a stream directly: it calls `Sessions::send`, which uses a
//! non-blocking `try_send` so a slow instance reports `Busy` instead of stalling the router,
//! and a vanished instance reports `Gone`.

use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{self, error::TrySendError};
use tonic::Status;

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
}

#[derive(Default)]
pub struct Sessions {
    inner: Mutex<HashMap<InstanceId, Session>>,
}

impl Sessions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a session, replacing any existing one with the same id.
    pub fn insert(
        &self,
        instance_id: &str,
        service: &str,
        tx: mpsc::Sender<Result<ServerMessage, Status>>,
    ) {
        let session = Session {
            service: service.to_string(),
            tx,
            last_heartbeat: Instant::now(),
        };
        self.inner
            .lock()
            .unwrap()
            .insert(instance_id.to_string(), session);
    }

    pub fn remove(&self, instance_id: &str) -> Option<Session> {
        self.inner.lock().unwrap().remove(instance_id)
    }

    /// try_send: full buffer -> Busy, closed or unknown -> Gone.
    pub fn send(&self, instance_id: &str, msg: ServerMessage) -> Result<(), SendError> {
        let tx = self
            .inner
            .lock()
            .unwrap()
            .get(instance_id)
            .map(|s| s.tx.clone())
            .ok_or(SendError::Gone)?;
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
        sessions.insert("a", "svc", tx);
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
        sessions.insert("a", "svc", tx);
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
        sessions.insert("a", "svc", tx_a);
        sessions.insert("b", "svc", tx_b);
        sessions.touch("a", now);
        sessions.touch("b", now - Duration::from_secs(31));
        assert_eq!(
            sessions.stale(now, Duration::from_secs(30)),
            vec!["b".to_string()]
        );
    }
}
