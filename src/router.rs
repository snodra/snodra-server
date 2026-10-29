//! Forwards requests to handlers and relays replies back to senders.
//!
//! Invariant: exactly one terminal message (`Reply` or `Error`) per request id. Whoever removes
//! the `Pending` entry from the map is the only one allowed to send it. The map lock is never
//! held across an await (it is never held across anything but short synchronous work).
//!
//! Metrics outcomes: a `Reply` carrying an error from a handler is always `HandlerError`;
//! `outcome_of` maps only errors the hub itself produces (NO_HANDLER, TIMEOUT,
//! HANDLER_DISCONNECTED, INVALID_REQUEST, and busy as HANDLER_ERROR).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::config::Config;
use crate::proto::{ErrorCode, Reply, Request, RequestKind, ServerMessage, reply, server_message};
use crate::registry::{InstanceId, Kind, PickError, Registry};
use crate::session::{SendError, Sessions};
use crate::telemetry::{Metrics, Outcome, kind_label, set_parent_from_headers};

/// How long a terminal message waits for room in a full sender buffer before it is given up.
const TERMINAL_SEND_TIMEOUT: Duration = Duration::from_secs(5);

struct Pending {
    sender: InstanceId,
    handler: InstanceId,
    kind: Kind,
    type_name: String,
    started: Instant,
    cancel: AbortHandle,
    /// The `hub.route` span; closes (and exports) when the entry is dropped.
    span: tracing::Span,
}

pub struct Router {
    registry: Arc<Registry>,
    sessions: Arc<Sessions>,
    pending: Mutex<HashMap<String, Pending>>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
}

pub fn error_message(
    request_id: &str,
    code: ErrorCode,
    message: impl Into<String>,
) -> ServerMessage {
    ServerMessage {
        message: Some(server_message::Message::Error(crate::proto::Error {
            request_id: request_id.to_string(),
            code: code as i32,
            message: message.into(),
            details: vec![],
        })),
    }
}

fn outcome_of(code: ErrorCode) -> Outcome {
    match code {
        ErrorCode::NoHandler => Outcome::NoHandler,
        ErrorCode::InvalidRequest => Outcome::InvalidRequest,
        ErrorCode::Timeout => Outcome::Timeout,
        ErrorCode::HandlerDisconnected => Outcome::HandlerDisconnected,
        ErrorCode::HandlerError | ErrorCode::Unauthenticated | ErrorCode::Unspecified => {
            Outcome::HandlerError
        }
    }
}

fn record_outcome(span: &tracing::Span, outcome: Outcome) {
    span.record("kraken.outcome", outcome.as_str());
}

/// One structured line per error outcome: hub-produced errors at warn, a handler's own error
/// reply (`relayed`) at info. Identifiers only, never the payload or error details.
fn log_outcome(
    request_id: &str,
    type_name: &str,
    kind: Option<Kind>,
    sender: &str,
    handler: Option<&str>,
    outcome: Outcome,
    relayed: bool,
) {
    let kind = kind.map_or("unspecified", kind_label);
    let handler = handler.unwrap_or("");
    let outcome_label = outcome.as_str();
    if relayed {
        tracing::info!(
            request_id,
            kraken.type = type_name,
            kraken.kind = kind,
            kraken.sender = sender,
            kraken.handler = handler,
            kraken.outcome = outcome_label,
            "request ended with error"
        );
    } else {
        tracing::warn!(
            request_id,
            kraken.type = type_name,
            kraken.kind = kind,
            kraken.sender = sender,
            kraken.handler = handler,
            kraken.outcome = outcome_label,
            "request ended with error"
        );
    }
}

impl Router {
    pub fn new(
        registry: Arc<Registry>,
        sessions: Arc<Sessions>,
        config: Arc<Config>,
        metrics: Arc<Metrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            registry,
            sessions,
            pending: Mutex::new(HashMap::new()),
            config,
            metrics,
        })
    }

    /// Called for every Request a sender wrote. Never returns an error to the caller: every
    /// failure becomes a `ServerMessage::Error` sent to `sender`.
    pub fn route(self: &Arc<Self>, sender: &str, req: Request) {
        let id = req.id.clone();
        let type_name = req.r#type.clone();
        let req_kind = match req.kind() {
            RequestKind::Command => Some(Kind::Command),
            RequestKind::Query => Some(Kind::Query),
            RequestKind::Unspecified => None,
        };
        let span = tracing::info_span!(
            "hub.route",
            kraken.type = %req.r#type,
            kraken.kind = req_kind.map_or("unspecified", kind_label),
            kraken.sender = %sender,
            kraken.handler = tracing::field::Empty,
            kraken.outcome = tracing::field::Empty,
        );
        set_parent_from_headers(&span, &req.headers);
        let Some(req_kind) = req_kind.filter(|_| !type_name.is_empty() && !id.is_empty()) else {
            let message = if id.is_empty() {
                "invalid request: missing id"
            } else {
                "invalid request: missing kind or type"
            };
            // Without a kind there is no metrics label, so only kind-bearing requests are counted.
            if let Some(kind) = req_kind {
                self.record_rejected(kind, &type_name, Outcome::InvalidRequest);
            }
            record_outcome(&span, Outcome::InvalidRequest);
            log_outcome(
                &id,
                &type_name,
                req_kind,
                sender,
                None,
                Outcome::InvalidRequest,
                false,
            );
            self.send_to(
                sender,
                error_message(&id, ErrorCode::InvalidRequest, message),
            );
            return;
        };

        let (kind, handler) = match self.registry.pick(&type_name, &req.routing_key) {
            Ok(picked) => picked,
            Err(e) => {
                let message = match e {
                    PickError::UnknownType => {
                        format!("no handler registered for type {type_name}")
                    }
                    PickError::NoLiveInstance { service } => {
                        format!("owner known, no instance for service {service}")
                    }
                };
                self.record_rejected(req_kind, &type_name, Outcome::NoHandler);
                record_outcome(&span, Outcome::NoHandler);
                log_outcome(
                    &id,
                    &type_name,
                    Some(req_kind),
                    sender,
                    None,
                    Outcome::NoHandler,
                    false,
                );
                self.send_to(sender, error_message(&id, ErrorCode::NoHandler, message));
                return;
            }
        };

        span.record("kraken.handler", handler.as_str());
        let timeout = if req.timeout_ms == 0 {
            self.config.default_timeout
        } else {
            Duration::from_millis(req.timeout_ms.into()).min(self.config.max_timeout)
        };

        // Insert before forwarding so a fast reply always finds its entry. The lock is held
        // while spawning so an immediate timeout cannot run before the entry exists.
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.contains_key(&id) {
                drop(pending);
                self.record_rejected(kind, &type_name, Outcome::InvalidRequest);
                record_outcome(&span, Outcome::InvalidRequest);
                log_outcome(
                    &id,
                    &type_name,
                    Some(kind),
                    sender,
                    None,
                    Outcome::InvalidRequest,
                    false,
                );
                self.send_to(
                    sender,
                    error_message(
                        &id,
                        ErrorCode::InvalidRequest,
                        "invalid request: duplicate id",
                    ),
                );
                return;
            }
            self.metrics.request_started(kind);
            let router = Arc::clone(self);
            let task_id = id.clone();
            let cancel = tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                router.on_timeout(&task_id, timeout.as_millis());
            })
            .abort_handle();
            let entry = Pending {
                sender: sender.to_string(),
                handler: handler.clone(),
                kind,
                type_name,
                started: Instant::now(),
                cancel,
                span,
            };
            pending.insert(id.clone(), entry);
        }

        let forwarded = ServerMessage {
            message: Some(server_message::Message::Request(req)),
        };
        match self.sessions.send(&handler, forwarded) {
            Ok(()) => {}
            Err(SendError::Busy) => {
                let service = self
                    .registry
                    .service_of(&handler)
                    .unwrap_or_else(|| "unknown".to_string());
                self.metrics.buffer_full(&service);
                self.fail(&id, ErrorCode::HandlerError, "handler busy".to_string());
            }
            Err(SendError::Gone) => {
                self.fail(
                    &id,
                    ErrorCode::HandlerDisconnected,
                    format!("handler instance {handler} disconnected"),
                );
            }
        }
    }

    /// Reply from a handler; relays to the recorded sender if the id is pending and `from` is
    /// the recorded handler. Unknown or mismatched ids are ignored.
    pub fn on_reply(&self, from: &str, reply: Reply) {
        let id = reply.request_id.clone();
        let entry = {
            let mut pending = self.pending.lock().unwrap();
            match pending.get(&id) {
                None => {
                    tracing::debug!(request_id = %id, "reply for unknown request ignored");
                    return;
                }
                Some(p) if p.handler != from => {
                    tracing::debug!(request_id = %id, from, "reply from non-handler ignored");
                    return;
                }
                Some(_) => pending.remove(&id).expect("entry checked above"),
            }
        };
        entry.cancel.abort();
        let outcome = match &reply.result {
            Some(reply::Result::Error(_)) => Outcome::HandlerError,
            _ => Outcome::Ok,
        };
        record_outcome(&entry.span, outcome);
        if outcome == Outcome::HandlerError {
            log_outcome(
                &id,
                &entry.type_name,
                Some(entry.kind),
                &entry.sender,
                Some(&entry.handler),
                outcome,
                true,
            );
        }
        self.metrics.request_finished(
            entry.kind,
            &entry.type_name,
            outcome,
            entry.started.elapsed(),
        );
        self.send_to(
            &entry.sender,
            ServerMessage {
                message: Some(server_message::Message::Reply(reply)),
            },
        );
    }

    /// Fails pending requests handled by this instance with HANDLER_DISCONNECTED; drops pending
    /// requests it sent.
    pub fn on_disconnect(&self, instance_id: &str) {
        let (failed, dropped) = {
            let mut pending = self.pending.lock().unwrap();
            let ids: Vec<String> = pending
                .iter()
                .filter(|(_, p)| p.handler == instance_id || p.sender == instance_id)
                .map(|(id, _)| id.clone())
                .collect();
            let mut failed = Vec::new();
            let mut dropped = Vec::new();
            for id in ids {
                let entry = pending.remove(&id).expect("id collected under same lock");
                if entry.handler == instance_id {
                    failed.push((id, entry));
                } else {
                    dropped.push(entry);
                }
            }
            (failed, dropped)
        };
        for entry in dropped {
            entry.cancel.abort();
            // The sender is gone: nobody to answer, so no outcome metric, but it is no longer in flight.
            self.metrics.in_flight_add(entry.kind, -1);
            entry.span.record("kraken.outcome", "sender_disconnected");
        }
        for (id, entry) in failed {
            entry.cancel.abort();
            self.finish_with_error(
                &id,
                entry,
                ErrorCode::HandlerDisconnected,
                format!("handler instance {instance_id} disconnected"),
            );
        }
    }

    /// Refreshes the `kraken_hub_sessions` and `kraken_hub_registered_types` gauges from the
    /// current session and registry state. Call after any change to either.
    pub fn refresh_gauges(&self) {
        self.metrics.sessions_set(self.sessions.count_by_service());
        self.metrics.registered_types_set(&self.registry.snapshot());
    }

    pub fn in_flight(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    fn on_timeout(&self, id: &str, timeout_ms: u128) {
        self.fail(
            id,
            ErrorCode::Timeout,
            format!("no reply within {timeout_ms} ms"),
        );
    }

    /// Removes the pending entry (if still present) and sends it a terminal error.
    fn fail(&self, id: &str, code: ErrorCode, message: String) {
        let entry = self.pending.lock().unwrap().remove(id);
        if let Some(entry) = entry {
            entry.cancel.abort();
            self.finish_with_error(id, entry, code, message);
        }
    }

    /// `entry` must already have been removed from the map by the caller.
    fn finish_with_error(&self, id: &str, entry: Pending, code: ErrorCode, message: String) {
        let outcome = outcome_of(code);
        record_outcome(&entry.span, outcome);
        log_outcome(
            id,
            &entry.type_name,
            Some(entry.kind),
            &entry.sender,
            Some(&entry.handler),
            outcome,
            false,
        );
        self.metrics.request_finished(
            entry.kind,
            &entry.type_name,
            outcome_of(code),
            entry.started.elapsed(),
        );
        self.send_to(&entry.sender, error_message(id, code, message));
    }

    fn record_rejected(&self, kind: Kind, type_name: &str, outcome: Outcome) {
        self.metrics.request_started(kind);
        self.metrics
            .request_finished(kind, type_name, outcome, Duration::ZERO);
    }

    /// Sends a terminal message (`Reply` or `Error`) to `instance`. `try_send` is the fast path;
    /// when the buffer is full the message is handed to a background delivery that waits up to
    /// [`TERMINAL_SEND_TIMEOUT`] for room, so a terminal message is never dropped just because
    /// the sender is momentarily slow. At most one such task exists per removed pending entry,
    /// so their number is bounded by the in-flight requests.
    fn send_to(&self, instance: &str, msg: ServerMessage) {
        let Some(tx) = self.sessions.sender(instance) else {
            tracing::debug!(instance, "sender gone, dropping terminal message");
            return;
        };
        match tx.try_send(Ok(msg)) {
            Ok(()) => {}
            Err(TrySendError::Closed(_)) => {
                tracing::debug!(instance, "sender stream closed, dropping terminal message");
            }
            Err(TrySendError::Full(msg)) => {
                let service = self
                    .registry
                    .service_of(instance)
                    .unwrap_or_else(|| "unknown".to_string());
                let metrics = Arc::clone(&self.metrics);
                let instance = instance.to_string();
                tokio::spawn(async move {
                    if let Err(e) = tx.send_timeout(msg, TERMINAL_SEND_TIMEOUT).await {
                        let reason = match e {
                            SendTimeoutError::Timeout(_) => "buffer still full",
                            SendTimeoutError::Closed(_) => "stream closed",
                        };
                        tracing::warn!(instance, service, reason, "terminal message not delivered");
                        metrics.buffer_full(&service);
                    }
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tonic::Status;

    type Rx = mpsc::Receiver<Result<ServerMessage, Status>>;

    fn fake(sessions: &Sessions, id: &str, service: &str, cap: usize) -> Rx {
        let (tx, rx) = mpsc::channel(cap);
        sessions.insert(id, service, tx, CancellationToken::new());
        rx
    }

    struct Fixture {
        router: Arc<Router>,
        sessions: Arc<Sessions>,
        sender_rx: Rx,
        handler_rx: Rx,
    }

    fn fixture_with(config: Config, handler_cap: usize) -> Fixture {
        let registry = Arc::new(Registry::new(config.owner_grace));
        let sessions = Arc::new(Sessions::new());
        registry
            .register("h1", "orders", &["t.Cmd".into()], &["t.Qry".into()])
            .unwrap();
        let handler_rx = fake(&sessions, "h1", "orders", handler_cap);
        let sender_rx = fake(&sessions, "s1", "web", 16);
        let router = Router::new(
            registry,
            sessions.clone(),
            Arc::new(config),
            Metrics::noop(),
        );
        Fixture {
            router,
            sessions,
            sender_rx,
            handler_rx,
        }
    }

    fn fixture() -> Fixture {
        fixture_with(Config::default(), 16)
    }

    fn request(id: &str, type_name: &str, timeout_ms: u32) -> Request {
        Request {
            id: id.into(),
            kind: RequestKind::Command as i32,
            r#type: type_name.into(),
            routing_key: "k".into(),
            payload: vec![1, 2, 3],
            headers: HashMap::from([("h".to_string(), "v".to_string())]),
            timeout_ms,
        }
    }

    fn payload_reply(id: &str) -> Reply {
        Reply {
            request_id: id.into(),
            result: Some(reply::Result::Payload(vec![9])),
        }
    }

    fn expect_error(rx: &mut Rx, id: &str, code: ErrorCode) -> String {
        match rx.try_recv().expect("message expected").unwrap().message {
            Some(server_message::Message::Error(e)) => {
                assert_eq!(e.request_id, id);
                assert_eq!(e.code(), code);
                e.message
            }
            other => panic!("expected error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unknown_type_sends_no_handler_to_sender() {
        let mut f = fixture();
        f.router.route("s1", request("r1", "t.Nope", 0));
        let msg = expect_error(&mut f.sender_rx, "r1", ErrorCode::NoHandler);
        assert_eq!(msg, "no handler registered for type t.Nope");
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn forwards_request_unchanged_to_picked_handler() {
        let mut f = fixture();
        let req = request("r1", "t.Cmd", 0);
        f.router.route("s1", req.clone());
        let got = f.handler_rx.try_recv().unwrap().unwrap();
        assert_eq!(got.message, Some(server_message::Message::Request(req)));
        assert_eq!(f.router.in_flight(), 1);
    }

    #[tokio::test]
    async fn relays_reply_to_sender_and_clears_pending() {
        let mut f = fixture();
        f.router.route("s1", request("r1", "t.Cmd", 0));
        f.router.on_reply("h1", payload_reply("r1"));
        let got = f.sender_rx.try_recv().unwrap().unwrap();
        assert_eq!(
            got.message,
            Some(server_message::Message::Reply(payload_reply("r1")))
        );
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn reply_from_wrong_instance_is_ignored() {
        let mut f = fixture();
        f.router.route("s1", request("r1", "t.Cmd", 0));
        f.router.on_reply("intruder", payload_reply("r1"));
        assert!(f.sender_rx.try_recv().is_err());
        assert_eq!(f.router.in_flight(), 1);
    }

    #[tokio::test]
    async fn unknown_reply_id_is_ignored() {
        let mut f = fixture();
        f.router.on_reply("h1", payload_reply("ghost"));
        assert!(f.sender_rx.try_recv().is_err());
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_sends_timeout_once_and_drops_late_reply() {
        let mut f = fixture();
        f.router.route("s1", request("r1", "t.Cmd", 10));
        tokio::time::sleep(Duration::from_millis(11)).await;
        let msg = expect_error(&mut f.sender_rx, "r1", ErrorCode::Timeout);
        assert_eq!(msg, "no reply within 10 ms");
        assert!(f.sender_rx.try_recv().is_err());
        f.router.on_reply("h1", payload_reply("r1"));
        assert!(f.sender_rx.try_recv().is_err());
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_timeout_uses_default_and_large_is_clamped() {
        let mut f = fixture();
        f.router.route("s1", request("r1", "t.Cmd", 0));
        tokio::time::sleep(Duration::from_millis(4999)).await;
        assert!(f.sender_rx.try_recv().is_err());
        tokio::time::sleep(Duration::from_millis(2)).await;
        expect_error(&mut f.sender_rx, "r1", ErrorCode::Timeout);

        f.router.route("s1", request("r2", "t.Cmd", 999_999));
        tokio::time::sleep(Duration::from_millis(60_000 - 1)).await;
        assert!(f.sender_rx.try_recv().is_err());
        tokio::time::sleep(Duration::from_millis(2)).await;
        expect_error(&mut f.sender_rx, "r2", ErrorCode::Timeout);
    }

    #[tokio::test]
    async fn duplicate_request_id_is_rejected_without_touching_pending() {
        let mut f = fixture();
        let mut s2_rx = fake(&f.sessions, "s2", "web", 16);
        f.router.route("s1", request("r1", "t.Cmd", 0));
        f.router.route("s2", request("r1", "t.Cmd", 0));
        let msg = expect_error(&mut s2_rx, "r1", ErrorCode::InvalidRequest);
        assert_eq!(msg, "invalid request: duplicate id");
        assert_eq!(f.router.in_flight(), 1);
        assert!(f.handler_rx.try_recv().is_ok());
        assert!(f.handler_rx.try_recv().is_err());
        f.router.on_reply("h1", payload_reply("r1"));
        assert!(matches!(
            f.sender_rx.try_recv().unwrap().unwrap().message,
            Some(server_message::Message::Reply(_))
        ));
        assert!(s2_rx.try_recv().is_err());
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn reply_just_before_deadline_sends_no_timeout() {
        let mut f = fixture();
        f.router.route("s1", request("r1", "t.Cmd", 10));
        tokio::time::sleep(Duration::from_millis(9)).await;
        f.router.on_reply("h1", payload_reply("r1"));
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(matches!(
            f.sender_rx.try_recv().unwrap().unwrap().message,
            Some(server_message::Message::Reply(_))
        ));
        assert!(f.sender_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn handler_disconnect_fails_its_pending_requests() {
        let mut f = fixture();
        f.router.route("s1", request("r1", "t.Cmd", 0));
        f.router.on_disconnect("h1");
        let msg = expect_error(&mut f.sender_rx, "r1", ErrorCode::HandlerDisconnected);
        assert_eq!(msg, "handler instance h1 disconnected");
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn sender_disconnect_drops_its_pending_requests() {
        let mut f = fixture();
        f.router.route("s1", request("r1", "t.Cmd", 0));
        f.router.on_disconnect("s1");
        assert_eq!(f.router.in_flight(), 0);
        f.router.on_reply("h1", payload_reply("r1"));
        assert!(f.sender_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn full_handler_buffer_yields_handler_error_busy() {
        let mut f = fixture_with(Config::default(), 1);
        f.sessions.send("h1", ServerMessage::default()).unwrap();
        f.router.route("s1", request("r1", "t.Cmd", 0));
        let msg = expect_error(&mut f.sender_rx, "r1", ErrorCode::HandlerError);
        assert_eq!(msg, "handler busy");
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn gone_handler_yields_handler_disconnected() {
        let mut f = fixture();
        drop(f.handler_rx);
        f.router.route("s1", request("r1", "t.Cmd", 0));
        expect_error(&mut f.sender_rx, "r1", ErrorCode::HandlerDisconnected);
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn empty_request_id_is_invalid_request() {
        let mut f = fixture();
        f.router.route("s1", request("", "t.Cmd", 0));
        let msg = expect_error(&mut f.sender_rx, "", ErrorCode::InvalidRequest);
        assert_eq!(msg, "invalid request: missing id");
        assert!(f.handler_rx.try_recv().is_err());
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn reply_to_busy_sender_is_delivered_once_there_is_room() {
        let f = fixture();
        let mut s2_rx = fake(&f.sessions, "s2", "web", 1);
        f.router.route("s2", request("r1", "t.Cmd", 0));
        // Fill s2's only slot so the reply's try_send hits a full buffer.
        f.sessions.send("s2", ServerMessage::default()).unwrap();
        f.router.on_reply("h1", payload_reply("r1"));
        assert_eq!(f.router.in_flight(), 0);
        assert_eq!(
            s2_rx.recv().await.unwrap().unwrap(),
            ServerMessage::default()
        );
        let got = tokio::time::timeout(Duration::from_secs(1), s2_rx.recv())
            .await
            .expect("reply delivered after the buffer drained")
            .unwrap()
            .unwrap();
        assert_eq!(
            got.message,
            Some(server_message::Message::Reply(payload_reply("r1")))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn error_to_busy_sender_is_given_up_after_the_delivery_timeout() {
        let f = fixture();
        let mut s2_rx = fake(&f.sessions, "s2", "web", 1);
        f.sessions.send("s2", ServerMessage::default()).unwrap();
        f.router.route("s2", request("r1", "t.Nope", 0));
        tokio::time::sleep(TERMINAL_SEND_TIMEOUT + Duration::from_millis(1)).await;
        assert_eq!(
            s2_rx.recv().await.unwrap().unwrap(),
            ServerMessage::default()
        );
        assert!(s2_rx.try_recv().is_err());
    }
}
