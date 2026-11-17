//! Hub-originated requests (ADR 0027): the hub asks a handler a query, or opens a streaming
//! query, itself, as the dashboard's event pages ask the `snodra-event-browser` add-on.
//!
//! No bypass: each request gets a session of its own in [`crate::session::Sessions`] (hidden
//! from the topology, the ACL reload, the heartbeat sweep and the instance counts) and goes
//! through [`Router::route`] like a client's, so routing, the handler's permits and queue,
//! timeouts, stream credits and cancellation are the router's, unchanged. Dropping the caller's
//! side ends it as a client that goes away would: a dropped query future drops the pending
//! request, a dropped stream receiver sends the handler `Cancel`.
//!
//! The hub acts as itself: the sender is service [`HUB_SERVICE`] (metrics, edges, logs) and its
//! session has no principal (the ACL judges clients, not the hub). The caller's metadata (for the
//! dashboard: `userId` and `correlationId`, so the handler can log who asked) travels as the
//! request's headers; the hub never forwards an `authorization` credential.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{self, OwnedPermit};
use tonic::Status;

use super::{DEFAULT_STREAM_CREDIT, Router};
use crate::context::ContextId;
use crate::proto::{
    Cancel, ErrorCode, Flow as FlowMsg, Request, RequestKind, RequestMode, ServerMessage, reply,
    server_message,
};
use crate::registry::InstanceId;

/// The service the hub sends its own requests as.
pub const HUB_SERVICE: &str = "snodra-server";

/// The instance id prefix of the hub's request sessions (`snodra-server/<uuid>`); a client may not
/// register an instance id that starts with it.
pub const INSTANCE_PREFIX: &str = "snodra-server/";

/// The most credit an internal stream holds: the receiver's buffer is that large.
pub const MAX_INTERNAL_STREAM_CREDIT: u32 = 1024;

/// Why a hub-originated request failed: the `Error` the router answers a client with, or a
/// handler's own error with its machine-readable `details` (empty for the router's own).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteError {
    pub code: ErrorCode,
    pub message: String,
    pub details: Vec<u8>,
}

impl RouteError {
    fn from_proto(e: crate::proto::Error) -> Self {
        Self {
            code: ErrorCode::try_from(e.code).unwrap_or(ErrorCode::Unspecified),
            message: e.message,
            details: e.details,
        }
    }

    /// The request's session closed before an answer (the hub is shutting down).
    fn session_closed() -> Self {
        Self {
            code: ErrorCode::Cancelled,
            message: "the hub ended the request".to_string(),
            details: Vec::new(),
        }
    }
}

impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code.as_str_name(), self.message)
    }
}

impl std::error::Error for RouteError {}

type Outbound = mpsc::Receiver<Result<ServerMessage, Status>>;
type Parts = mpsc::Sender<Result<Vec<u8>, RouteError>>;

/// The hub as the sender of one request: a session of its own, removed on drop together with
/// whatever the router still holds for it (as for a client that left).
struct InternalSender {
    router: Arc<Router>,
    ctx: ContextId,
    instance: InstanceId,
    token: u64,
    /// What the router sends the hub: the answer, or the parts and the end.
    rx: Outbound,
}

impl InternalSender {
    /// A session whose channel holds `capacity` messages.
    fn open(router: &Arc<Router>, capacity: usize) -> Self {
        let ctx = ContextId::default();
        let instance = format!("{INSTANCE_PREFIX}{}", uuid::Uuid::new_v4());
        let (tx, rx) = mpsc::channel(capacity);
        let token = router
            .sessions
            .insert_internal(&ctx, &instance, HUB_SERVICE, tx);
        Self {
            router: Arc::clone(router),
            ctx,
            instance,
            token,
            rx,
        }
    }

    fn route(&self, req: Request) {
        self.router.route(&self.ctx, &self.instance, req);
    }

    /// The next message for request `id`; `None` when the session closed.
    async fn next(&mut self, id: &str) -> Option<server_message::Message> {
        loop {
            let msg = self.rx.recv().await?.ok()?.message?;
            let for_id = match &msg {
                server_message::Message::Reply(r) => r.request_id == id,
                server_message::Message::Error(e) => e.request_id == id,
                server_message::Message::Part(p) => p.request_id == id,
                server_message::Message::End(e) => e.request_id == id,
                _ => false,
            };
            if for_id {
                return Some(msg);
            }
        }
    }
}

impl Drop for InternalSender {
    fn drop(&mut self) {
        self.router
            .sessions
            .remove_if(&self.ctx, &self.instance, self.token);
        self.router
            .on_disconnect(&self.ctx, &self.instance, self.token);
    }
}

/// `timeout` as `Request.timeout_ms`: at least 1 ms (0 would mean the default), clamped by the
/// router as any client's.
fn timeout_ms(timeout: Duration) -> u32 {
    u32::try_from(timeout.as_millis())
        .unwrap_or(u32::MAX)
        .max(1)
}

impl Router {
    /// Asks the handler of query `type_name` and waits for its answer: routed, flow-controlled
    /// and timed (`timeout`, under the hub's per-type override and maximum) like a client's
    /// unary query; `headers` are the request's metadata. `Err` carries the router's error
    /// (`NO_HANDLER`, `TIMEOUT`, ...) or the handler's. Dropping the future drops the request, as
    /// a client that leaves does.
    pub async fn query_internal(
        self: &Arc<Self>,
        type_name: &str,
        payload: Vec<u8>,
        timeout: Duration,
        headers: HashMap<String, String>,
    ) -> Result<Vec<u8>, RouteError> {
        let mut sender = InternalSender::open(self, 2);
        let id = uuid::Uuid::new_v4().to_string();
        sender.route(Request {
            id: id.clone(),
            kind: RequestKind::Query as i32,
            r#type: type_name.to_string(),
            payload,
            headers,
            timeout_ms: timeout_ms(timeout),
            mode: RequestMode::Unary as i32,
            ..Request::default()
        });
        match sender.next(&id).await {
            Some(server_message::Message::Reply(r)) => match r.result {
                Some(reply::Result::Payload(payload)) => Ok(payload),
                Some(reply::Result::Error(e)) => Err(RouteError::from_proto(e)),
                None => Ok(Vec::new()),
            },
            Some(server_message::Message::Error(e)) => Err(RouteError::from_proto(e)),
            _ => Err(RouteError::session_closed()),
        }
    }

    /// Opens streaming query `type_name` with `credit` (0: the default of a client's stream; at
    /// most [`MAX_INTERNAL_STREAM_CREDIT`]): its parts arrive on the receiver in order, then the
    /// channel closes, after an `Err` when the stream failed (the router's error, such as
    /// `NO_HANDLER` or the idle `TIMEOUT`, or the handler's); `headers` are the request's
    /// metadata. The credit is the receiver's buffer: the slots the reader frees go back to the
    /// handler in batches of half the credit (at least one), so it never runs more than `credit`
    /// parts ahead of the reader and a busy stream costs a `Flow` per batch, not per part.
    /// Dropping the receiver cancels the stream at the handler (`Cancel`, reason
    /// `receiver dropped`).
    pub fn stream_internal(
        self: &Arc<Self>,
        type_name: &str,
        payload: Vec<u8>,
        credit: u32,
        headers: HashMap<String, String>,
    ) -> mpsc::Receiver<Result<Vec<u8>, RouteError>> {
        let credit = match credit {
            0 => DEFAULT_STREAM_CREDIT,
            n => n.min(MAX_INTERNAL_STREAM_CREDIT),
        };
        let (out, rx) = mpsc::channel(credit as usize);
        // Every slot of the receiver's buffer is held for a part the handler may send.
        let held = (0..credit)
            .map(|_| out.clone().try_reserve_owned().ok())
            .collect::<Option<Vec<_>>>()
            .expect("a new channel has room for its capacity");
        // Room for every part the credit allows and the end.
        let sender = InternalSender::open(self, credit as usize + 2);
        let id = uuid::Uuid::new_v4().to_string();
        sender.route(Request {
            id: id.clone(),
            kind: RequestKind::Query as i32,
            r#type: type_name.to_string(),
            payload,
            headers,
            mode: RequestMode::Streaming as i32,
            initial_credit: credit,
            ..Request::default()
        });
        tokio::spawn(relay_stream(sender, id, out, held, (credit / 2).max(1)));
        rx
    }
}

/// Moves the parts of internal stream `id` to the receiver, each into a slot held for it, and
/// grants the handler the slots the reader frees, `batch` at a time.
async fn relay_stream(
    mut sender: InternalSender,
    id: String,
    out: Parts,
    mut held: Vec<OwnedPermit<Result<Vec<u8>, RouteError>>>,
    batch: u32,
) {
    // Slots freed by the reader and not granted yet.
    let mut freed = 0;
    let failed = loop {
        tokio::select! {
            msg = sender.next(&id) => match msg {
                Some(server_message::Message::Part(part)) => {
                    // The router ends a stream whose handler outruns its credit, so a slot is
                    // always held for a part it relays.
                    match held.pop() {
                        Some(slot) => {
                            slot.send(Ok(part.payload));
                        }
                        None => tracing::warn!(
                            request = %id,
                            "internal stream part without a held slot dropped"
                        ),
                    }
                }
                Some(server_message::Message::End(end)) => {
                    break end.error.map(RouteError::from_proto);
                }
                Some(server_message::Message::Error(e)) => break Some(RouteError::from_proto(e)),
                Some(_) => {}
                None => break Some(RouteError::session_closed()),
            },
            slot = out.clone().reserve_owned() => match slot {
                Ok(slot) => {
                    held.push(slot);
                    freed += 1;
                    if freed >= batch {
                        let flow = FlowMsg { request_id: id.clone(), credits: freed };
                        freed = 0;
                        sender
                            .router
                            .on_flow_from(&sender.ctx, &sender.instance, sender.token, flow);
                    }
                }
                Err(_) => {
                    let cancel = Cancel {
                        request_id: id.clone(),
                        reason: "receiver dropped".to_string(),
                    };
                    sender
                        .router
                        .on_cancel_from(&sender.ctx, &sender.instance, sender.token, cancel);
                    return;
                }
            },
        }
    };
    // The stream ended: the session goes first, so nothing is left in the router while the
    // error waits for the reader.
    drop(sender);
    if let Some(error) = failed {
        drop(held);
        let _ = out.send(Err(error)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Acl, AuthMode, Authorizer};
    use crate::config::Config;
    use crate::proto::{Error as ProtoError, Reply, ReplyEnd, ReplyPart, RequestMode, reply};
    use crate::registry::Registry;
    use crate::session::{SessionMeta, Sessions};
    use crate::telemetry::Metrics;
    use std::collections::HashMap;
    use tokio_util::sync::CancellationToken;

    static D: std::sync::LazyLock<ContextId> = std::sync::LazyLock::new(ContextId::default);

    const SEARCH: &str = "snodra.events.Search";
    const TAIL: &str = "snodra.events.Tail";
    const PERMITS: u32 = 1024;

    /// A router with one handler session `e1` (service `snodra-event-browser`) for [`SEARCH`]
    /// and [`TAIL`], opened with `permits`.
    struct Fx {
        router: Arc<Router>,
        registry: Arc<Registry>,
        sessions: Arc<Sessions>,
        handler: mpsc::Receiver<Result<ServerMessage, tonic::Status>>,
        token: u64,
    }

    fn fixture(permits: u32) -> Fx {
        fixture_with(permits, |registry, sessions, config, metrics| {
            Router::new(registry, sessions, config, metrics)
        })
    }

    fn fixture_with(
        permits: u32,
        router: impl FnOnce(Arc<Registry>, Arc<Sessions>, Arc<Config>, Arc<Metrics>) -> Arc<Router>,
    ) -> Fx {
        let config = Config::default();
        let registry = Arc::new(Registry::new(config.owner_grace));
        let sessions = Arc::new(Sessions::new());
        let router = router(
            Arc::clone(&registry),
            Arc::clone(&sessions),
            Arc::new(config),
            Metrics::noop(),
        );
        let service = "snodra-event-browser";
        let token = sessions.allocate_token();
        assert!(
            router
                .open_session(&D, "e1", token, service, permits)
                .is_ok()
        );
        let (tx, handler) = mpsc::channel(64);
        let meta = SessionMeta::default();
        sessions
            .insert_with_token(&D, "e1", service, meta, tx, CancellationToken::new(), token)
            .unwrap();
        router.start_session(&D, "e1", token);
        registry
            .register(&D, "e1", service, &[], &[SEARCH.into(), TAIL.into()])
            .unwrap();
        Fx {
            router,
            registry,
            sessions,
            handler,
            token,
        }
    }

    /// No request metadata.
    fn none() -> HashMap<String, String> {
        HashMap::new()
    }

    /// The metadata the dashboard's calls carry: who asked and a correlation id.
    fn meta() -> HashMap<String, String> {
        HashMap::from([
            ("userId".to_string(), "dashboard-token".to_string()),
            ("correlationId".to_string(), "c-1".to_string()),
        ])
    }

    /// Lets the spawned tasks (the hub's side of an internal stream) run.
    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    impl Fx {
        /// The next message the handler got.
        async fn next(&mut self) -> server_message::Message {
            let msg = tokio::time::timeout(Duration::from_secs(5), self.handler.recv())
                .await
                .expect("a message for the handler")
                .expect("the handler's channel is open")
                .expect("not a status");
            msg.message.expect("a message")
        }

        async fn next_request(&mut self) -> Request {
            match self.next().await {
                server_message::Message::Request(r) => r,
                other => panic!("expected a request, got {other:?}"),
            }
        }

        fn nothing_for_the_handler(&mut self) {
            assert!(self.handler.try_recv().is_err(), "nothing for the handler");
        }

        fn reply(&self, id: &str, result: reply::Result) {
            let reply = Reply {
                request_id: id.into(),
                result: Some(result),
            };
            self.router.on_reply_from(&D, "e1", self.token, reply);
        }

        fn part(&self, target: &str, seq: u64, payload: &[u8]) {
            let part = ReplyPart {
                request_id: target.into(),
                payload: payload.to_vec(),
                seq,
                ..ReplyPart::default()
            };
            self.router.on_stream_part_from(&D, "e1", self.token, part);
        }

        fn end(&self, target: &str) {
            let end = ReplyEnd {
                request_id: target.into(),
                ..ReplyEnd::default()
            };
            self.router.on_stream_end_from(&D, "e1", self.token, end);
        }

        /// Sessions of every kind, the hub's own included (only `e1` once a request ended).
        fn sessions(&self) -> usize {
            self.sessions.all_with_tokens().len()
        }
    }

    #[tokio::test]
    async fn internal_query_reaches_handler_and_returns() {
        // No permit yet: the hub's request waits in the handler's queue like any other.
        let mut f = fixture(0);
        let router = Arc::clone(&f.router);
        let call = tokio::spawn(async move {
            router
                .query_internal(
                    SEARCH,
                    b"{\"q\":1}".to_vec(),
                    Duration::from_secs(3),
                    meta(),
                )
                .await
        });
        settle().await;
        f.nothing_for_the_handler();
        assert_eq!(f.router.queued_total(), 1);
        assert_eq!(f.router.in_flight(), 1);
        f.router.on_permits(&D, "e1", f.token, 1, 1);
        let r = f.next_request().await;
        assert_eq!(r.r#type, SEARCH);
        assert_eq!(
            (r.kind(), r.mode()),
            (RequestKind::Query, RequestMode::Unary)
        );
        assert_eq!(r.payload, b"{\"q\":1}");
        assert_eq!(r.timeout_ms, 3000);
        // The metadata travels as request headers; the hub still forwards no credential.
        assert_eq!(r.headers, meta());
        f.reply(&r.id, reply::Result::Payload(b"[1]".to_vec()));
        assert_eq!(call.await.unwrap(), Ok(b"[1]".to_vec()));
        assert_eq!(f.router.in_flight(), 0);
        assert_eq!(f.sessions(), 1, "the hub's session ended with its request");

        // A handler's error comes back as it is.
        f.router.on_permits(&D, "e1", f.token, 1, 1);
        let router = Arc::clone(&f.router);
        let call = tokio::spawn(async move {
            router
                .query_internal(SEARCH, vec![], Duration::from_secs(3), none())
                .await
        });
        let r = f.next_request().await;
        let error = ProtoError {
            request_id: r.id.clone(),
            code: ErrorCode::HandlerError as i32,
            message: "boom".into(),
            details: vec![],
        };
        f.reply(&r.id, reply::Result::Error(error));
        let err = call.await.unwrap().unwrap_err();
        assert_eq!(
            (err.code, err.message.as_str()),
            (ErrorCode::HandlerError, "boom")
        );

        // No handler: the router's own answer.
        let err = f
            .router
            .query_internal("snodra.events.Lag", vec![], Duration::from_secs(3), none())
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::NoHandler);
        assert_eq!(
            err.to_string(),
            "NO_HANDLER: no handler registered for type snodra.events.Lag"
        );
        assert_eq!(f.sessions(), 1);
    }

    #[tokio::test]
    async fn internal_requests_act_as_the_hub_under_access_control() {
        // Enforce with an empty ACL: no client could send anything, the hub's own requests go.
        let mut f = fixture_with(PERMITS, |registry, sessions, config, metrics| {
            let auth =
                Authorizer::with_acl(AuthMode::Enforce, Acl::default(), Arc::clone(&metrics));
            Router::with_auth(registry, sessions, config, metrics, auth)
        });
        let router = Arc::clone(&f.router);
        let call = tokio::spawn(async move {
            router
                .query_internal(SEARCH, vec![], Duration::from_secs(3), none())
                .await
        });
        let r = f.next_request().await;
        f.reply(&r.id, reply::Result::Payload(b"ok".to_vec()));
        assert_eq!(call.await.unwrap(), Ok(b"ok".to_vec()));
    }

    #[tokio::test]
    async fn internal_stream_respects_credit() {
        let mut f = fixture(PERMITS);
        let mut rx = f.router.stream_internal(TAIL, b"{}".to_vec(), 2, none());
        let r = f.next_request().await;
        assert_eq!(r.r#type, TAIL);
        assert_eq!(
            (r.kind(), r.mode()),
            (RequestKind::Query, RequestMode::Streaming)
        );
        assert_eq!(r.initial_credit, 2);
        let target = r.id;
        f.part(&target, 0, b"a");
        f.part(&target, 1, b"b");
        settle().await;
        // Two parts wait for the reader: the credit is spent and nothing is granted.
        f.nothing_for_the_handler();
        // The reader takes one: one credit back to the handler.
        assert_eq!(rx.recv().await, Some(Ok(b"a".to_vec())));
        settle().await;
        match f.next().await {
            server_message::Message::Flow(flow) => {
                assert_eq!(
                    (flow.request_id.as_str(), flow.credits),
                    (target.as_str(), 1)
                );
            }
            other => panic!("expected a flow, got {other:?}"),
        }
        f.nothing_for_the_handler();
        f.part(&target, 2, b"c");
        f.end(&target);
        assert_eq!(rx.recv().await, Some(Ok(b"b".to_vec())));
        assert_eq!(rx.recv().await, Some(Ok(b"c".to_vec())));
        assert_eq!(rx.recv().await, None, "a normal end closes the channel");
        settle().await;
        assert_eq!(f.router.in_flight(), 0);
        assert_eq!(f.sessions(), 1);

        // A handler that ignores the credit is stopped by the router, as for any sender.
        let mut rx = f.router.stream_internal(TAIL, vec![], 1, none());
        let target = f.next_request().await.id;
        f.part(&target, 0, b"a");
        f.part(&target, 1, b"over");
        assert_eq!(rx.recv().await, Some(Ok(b"a".to_vec())));
        let err = rx.recv().await.unwrap().unwrap_err();
        assert_eq!(err.code, ErrorCode::InvalidRequest);
        assert_eq!(rx.recv().await, None);
        match f.next().await {
            server_message::Message::Cancel(c) => assert_eq!(c.request_id, target),
            other => panic!("expected a cancel, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn internal_stream_receiver_drop_cancels_the_handler() {
        let mut f = fixture(PERMITS);
        // A credit of two goes back one at a time.
        let mut rx = f.router.stream_internal(TAIL, vec![], 2, none());
        let target = f.next_request().await.id;
        f.part(&target, 0, b"a");
        assert_eq!(rx.recv().await, Some(Ok(b"a".to_vec())));
        settle().await;
        assert!(matches!(f.next().await, server_message::Message::Flow(_)));
        assert_eq!(f.router.in_flight(), 1);
        drop(rx);
        settle().await;
        match f.next().await {
            server_message::Message::Cancel(c) => {
                assert_eq!(
                    (c.request_id.as_str(), c.reason.as_str()),
                    (target.as_str(), "receiver dropped")
                );
            }
            other => panic!("expected a cancel, got {other:?}"),
        }
        assert_eq!(f.router.in_flight(), 0);
        assert_eq!(f.sessions(), 1);
        // A late part is ignored.
        f.part(&target, 1, b"late");
        f.nothing_for_the_handler();
    }

    #[tokio::test]
    async fn a_dropped_internal_query_leaves_nothing_behind() {
        let mut f = fixture(PERMITS);
        let router = Arc::clone(&f.router);
        let call = tokio::spawn(async move {
            router
                .query_internal(SEARCH, vec![], Duration::from_secs(3), none())
                .await
        });
        let r = f.next_request().await;
        assert_eq!((f.router.in_flight(), f.sessions()), (1, 2));
        call.abort();
        settle().await;
        assert_eq!((f.router.in_flight(), f.sessions()), (0, 1));
        // The handler's late answer goes nowhere.
        f.reply(&r.id, reply::Result::Payload(vec![1]));
        settle().await;
        assert_eq!((f.router.in_flight(), f.sessions()), (0, 1));
        f.nothing_for_the_handler();
    }

    #[tokio::test(start_paused = true)]
    async fn internal_request_times_out() {
        let mut f = fixture(PERMITS);
        let err = f
            .router
            .query_internal(SEARCH, vec![], Duration::from_millis(100), none())
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Timeout);
        assert_eq!(f.next_request().await.timeout_ms, 100);
        assert_eq!((f.router.in_flight(), f.sessions()), (0, 1));

        // A stream with no part within the timeout ends with TIMEOUT and cancels the handler.
        let mut rx = f.router.stream_internal(TAIL, vec![], 4, none());
        let target = f.next_request().await.id;
        let err = rx.recv().await.unwrap().unwrap_err();
        assert_eq!(err.code, ErrorCode::Timeout);
        assert_eq!(rx.recv().await, None);
        match f.next().await {
            server_message::Message::Cancel(c) => assert_eq!(c.request_id, target),
            other => panic!("expected a cancel, got {other:?}"),
        }
        settle().await;
        assert_eq!((f.router.in_flight(), f.sessions()), (0, 1));
    }
    #[tokio::test]
    async fn internal_stream_credits_go_back_in_batches_of_half_the_credit() {
        let mut f = fixture(PERMITS);
        let mut rx = f.router.stream_internal(TAIL, vec![], 4, none());
        let target = f.next_request().await.id;
        for seq in 0..4 {
            f.part(&target, seq, b"p");
        }
        // The first part read frees one slot: not yet half the credit, so no Flow.
        assert_eq!(rx.recv().await, Some(Ok(b"p".to_vec())));
        settle().await;
        f.nothing_for_the_handler();
        // The second makes two: one Flow of two credits.
        assert_eq!(rx.recv().await, Some(Ok(b"p".to_vec())));
        settle().await;
        match f.next().await {
            server_message::Message::Flow(flow) => assert_eq!(flow.credits, 2),
            other => panic!("expected a flow, got {other:?}"),
        }
        f.nothing_for_the_handler();
        drop(rx);
    }

    #[tokio::test]
    async fn internal_stream_carries_the_handlers_error_after_its_parts() {
        let mut f = fixture(PERMITS);
        let mut rx = f.router.stream_internal(TAIL, vec![], 4, none());
        let target = f.next_request().await.id;
        f.part(&target, 0, b"a");
        f.part(&target, 1, b"b");
        let end = ReplyEnd {
            request_id: target.clone(),
            error: Some(ProtoError {
                request_id: target.clone(),
                code: ErrorCode::HandlerError as i32,
                message: "the feed died".into(),
                details: b"{\"error\":\"unavailable\"}".to_vec(),
            }),
            ..ReplyEnd::default()
        };
        f.router.on_stream_end_from(&D, "e1", f.token, end);
        assert_eq!(rx.recv().await, Some(Ok(b"a".to_vec())));
        assert_eq!(rx.recv().await, Some(Ok(b"b".to_vec())));
        let err = rx.recv().await.unwrap().unwrap_err();
        assert_eq!(
            (err.code, err.message.as_str(), err.details.as_slice()),
            (
                ErrorCode::HandlerError,
                "the feed died",
                b"{\"error\":\"unavailable\"}".as_slice()
            )
        );
        assert_eq!(rx.recv().await, None);
        settle().await;
        assert_eq!((f.router.in_flight(), f.sessions()), (0, 1));
    }

    #[tokio::test]
    async fn internal_stream_without_a_handler_fails_at_once() {
        let f = fixture(PERMITS);
        let mut rx = f
            .router
            .stream_internal("snodra.events.Nothing", vec![], 4, none());
        let err = rx.recv().await.unwrap().unwrap_err();
        assert_eq!(err.code, ErrorCode::NoHandler);
        assert_eq!(rx.recv().await, None);
        settle().await;
        assert_eq!((f.router.in_flight(), f.sessions()), (0, 1));
    }

    #[tokio::test]
    async fn internal_stream_credit_defaults_and_is_clamped() {
        let mut f = fixture(PERMITS);
        let rx = f.router.stream_internal(TAIL, vec![], 0, none());
        assert_eq!(f.next_request().await.initial_credit, DEFAULT_STREAM_CREDIT);
        drop(rx);
        settle().await;
        assert!(matches!(f.next().await, server_message::Message::Cancel(_)));
        let rx = f
            .router
            .stream_internal(TAIL, vec![], MAX_INTERNAL_STREAM_CREDIT + 1000, none());
        assert_eq!(
            f.next_request().await.initial_credit,
            MAX_INTERNAL_STREAM_CREDIT
        );
        drop(rx);
    }

    #[tokio::test]
    async fn the_shutdown_sweep_ends_internal_requests_as_cancelled() {
        let mut f = fixture(PERMITS);
        let router = Arc::clone(&f.router);
        let call = tokio::spawn(async move {
            router
                .query_internal(SEARCH, vec![], Duration::from_secs(3), none())
                .await
        });
        let mut rx = f.router.stream_internal(TAIL, vec![], 4, none());
        f.next_request().await;
        f.next_request().await;
        // What the hub does at shutdown (`serve_with_shutdown`) to every session, here to the
        // hub's own only (a handler swept first would answer HANDLER_DISCONNECTED instead).
        for (ctx, id, token) in f.sessions.all_with_tokens() {
            if id == "e1" {
                continue;
            }
            if let Some(tx) = f.sessions.sender(&ctx, &id) {
                let _ = tx.try_send(Err(tonic::Status::unavailable("hub shutting down")));
            }
            crate::service::disconnect(&f.registry, &f.sessions, &f.router, &ctx, &id, token);
        }
        let ended = RouteError {
            code: ErrorCode::Cancelled,
            message: "the hub ended the request".to_string(),
            details: Vec::new(),
        };
        assert_eq!(call.await.unwrap(), Err(ended.clone()));
        assert_eq!(rx.recv().await, Some(Err(ended)));
        assert_eq!(rx.recv().await, None);
        assert_eq!((f.router.in_flight(), f.sessions()), (0, 1));
    }
}
