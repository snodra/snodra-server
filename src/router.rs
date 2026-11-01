//! Forwards requests to handlers and relays replies back to senders.
//!
//! Invariant: exactly one terminal message (`Reply` or `Error`) per request id. Whoever removes
//! the `Pending` entry from the map is the only one allowed to send it. The map lock is never
//! held across an await (it is never held across anything but short synchronous work).
//!
//! Metrics outcomes: a `Reply` carrying an error from a handler is always `HandlerError`;
//! `outcome_of` maps only errors the hub itself produces (NO_HANDLER, TIMEOUT,
//! HANDLER_DISCONNECTED, INVALID_REQUEST); a capacity rejection is `ResourceExhausted` whichever
//! code the sender gets.
//!
//! Scatter-gather (ADR 0017): a [`Pending::Scatter`] keyed by the sender's request id holds the
//! gather; each target is an ordinary [`Pending::Unary`] keyed `<id>#<n>` whose `part_of` names
//! the gather, so flow control, queues, priority and disconnects apply per target unchanged. A
//! target's terminal message becomes a `ReplyPart` to the sender instead; the gather's own
//! terminal message is its `ReplyEnd`, sent by whoever removes the gather from the map.
//!
//! Streaming queries (ADR 0017, P4b): a [`Pending::Stream`] keyed by the sender's request id
//! holds the stream; its one target is a [`Pending::Unary`] keyed `<id>#1` with `part_of` set,
//! forwarded (with `mode = STREAMING` and the credit) through the same flow control as any
//! request and kept in the map, holding its permit, until the stream ends. The handler's parts
//! and end for `<id>#1` are relayed to the sender under the sender's credits; every ending path
//! (end, error, over-credit, `max_results`, cancel, idle timeout, max duration, disconnects)
//! removes both entries, sends the sender its `ReplyEnd` where one is due and the handler a
//! `Cancel` where it still streams.
//!
//! Flow control (ADR 0014): every session has a [`Flow`] keyed by instance id and session token.
//! A flow-controlled session (permits `Some`) is forwarded a request only while it has a permit;
//! further requests wait in its [`InstanceQueue`] (priority, then age) until `Permits` arrive or
//! their own deadline passes. Lock order: the pending lock and the flows lock are never held
//! together.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::WeakSender;
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::Instant;
use tonic::Status;

use crate::config::Config;
use crate::context::ContextId;
use crate::edges::EdgeStats;
use crate::proto::{
    Cancel, ErrorCode, Flow as FlowMsg, Reply, ReplyEnd, ReplyPart, Request, RequestKind,
    RequestMode, ServerMessage, reply, server_message,
};
use crate::protocol;
use crate::queue::{InstanceQueue, Queued, TotalBytes};
use crate::registry::{Candidate, InstanceId, Kind, PickError, Registry, Target};
use crate::session::{SendError, SenderInfo, Sessions};
use crate::telemetry::{Metrics, Outcome, kind_label, set_parent_from_headers};

/// How long a terminal message waits for room in a full sender buffer before it is given up.
const TERMINAL_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Every request the hub has not answered yet, keyed by request id. P4c adds subscriptions.
enum Pending {
    /// A request forwarded to one handler: a unary request, or the target of a scatter-gather or
    /// of a streaming query.
    Unary(Unary),
    /// A scatter-gather in progress; its targets are `Unary` entries with `part_of` set.
    Scatter(Scatter),
    /// A streaming query in progress; its target is the `Unary` entry `<id>#1`.
    Stream(Stream),
}

/// Credits the handler of a streaming query starts with when the sender sent none.
pub const DEFAULT_STREAM_CREDIT: u32 = 64;

/// The scatter-gather a forwarded request is a target of.
#[derive(Debug, Clone)]
struct PartOf {
    /// The sender's request id: the gather's key.
    parent: String,
    /// The target's service, for `ReplyPart.responder_service`.
    service: String,
}

struct Unary {
    /// Context of both sender and handler (requests never cross contexts).
    context: ContextId,
    sender: InstanceId,
    /// The sender's session when the request was routed (`None` if it was already gone).
    sender_token: Option<u64>,
    /// The sender's service, for the edge statistics (`None` if its session was already gone).
    sender_service: Option<String>,
    handler: InstanceId,
    /// The handler session the request was forwarded to. A reconnect with the same instance id
    /// is a different session: it cannot answer requests forwarded to the old stream.
    handler_token: u64,
    /// The request was written to the handler (set under the pending lock right after the
    /// write): only such a target is sent a `Cancel` when its stream or gather ends; one whose
    /// entry ended between the pop and the write is cancelled by [`Router::note_forwarded`].
    forwarded: bool,
    /// The sender shares "flow-control": capacity rejections are RESOURCE_EXHAUSTED for it,
    /// HANDLER_ERROR "handler busy" otherwise.
    sender_flow_control: bool,
    kind: Kind,
    type_name: String,
    started: Instant,
    /// The deadline timer; `None` for a scatter-gather target (its gather has the deadline).
    cancel: Option<AbortHandle>,
    /// The `hub.route` span; closes (and exports) when the entry is dropped.
    span: tracing::Span,
    /// `Some` for a scatter-gather target: its answer becomes a part of that gather.
    part_of: Option<PartOf>,
}

impl Unary {
    fn abort_timer(&self) {
        if let Some(cancel) = &self.cancel {
            cancel.abort();
        }
    }
}

/// A scatter-gather in progress (ADR 0017).
struct Scatter {
    context: ContextId,
    sender: InstanceId,
    sender_token: Option<u64>,
    sender_service: Option<String>,
    kind: Kind,
    type_name: String,
    started: Instant,
    /// The deadline timer of the whole gather.
    cancel: AbortHandle,
    span: tracing::Span,
    /// Ordered delivery of the parts and the end to the sender.
    relay: Relay,
    /// Target request ids (`<id>#<n>`) that have not answered.
    outstanding: HashSet<String>,
    /// Parts sent, successful ones and failed ones.
    parts: u32,
    ok: u32,
    failed: u32,
    /// `Request.max_results`: end after this many successful parts (0 = all).
    max_results: u32,
    /// The outcome of the last failed part, for the metrics when nothing succeeded.
    last_failure: Option<Outcome>,
}

/// A streaming query in progress (ADR 0017, P4b).
struct Stream {
    context: ContextId,
    sender: InstanceId,
    sender_token: u64,
    sender_service: String,
    /// The handler chosen for the whole stream, and its service (`ReplyPart.responder_*`).
    handler: InstanceId,
    handler_service: String,
    kind: Kind,
    type_name: String,
    started: Instant,
    /// The target's request id (`<id>#1`): the `Unary` entry forwarded to the handler.
    target_id: String,
    /// Parts the handler may still send: the initial credit plus every `Flow` of the sender,
    /// less the parts relayed.
    credit: u32,
    /// Parts relayed to the sender.
    parts: u32,
    /// `Request.max_results`: end after this many parts (0 = no limit).
    max_results: u32,
    /// When the stream was last active: routed, forwarded to the handler, a part relayed, or
    /// a `Flow` that raised an exhausted credit. The idle watch (`timeout_ms` under the usual
    /// rules) ends the stream when it gets old; it does not run while `credit` is 0 (nothing
    /// can flow until the sender grants more: a slow consumer is no idle handler).
    last_activity: Instant,
    /// The idle watch and the max-duration timer.
    idle: AbortHandle,
    max_duration: AbortHandle,
    span: tracing::Span,
    /// Ordered delivery of the parts and the end to the sender.
    relay: Relay,
}

/// How a stream ends: what the sender is told (if anything) and what the handler is told (if
/// it was forwarded the request).
struct StreamEnd {
    /// The `ReplyEnd.error`; `None` for a normal end.
    error: Option<crate::proto::Error>,
    /// `false` when the sender gets no `ReplyEnd` (it cancelled).
    tell_sender: bool,
    /// The `Cancel.reason` for the handler; `None` when the handler ended the stream itself or
    /// is gone.
    cancel: Option<String>,
    outcome: Outcome,
    /// The error is the handler's own (logged at info, not warn).
    relayed: bool,
}

/// A stream taken out of the map: its `ReplyEnd` is sent, the rest is bookkeeping.
struct ClosedStream {
    stream: Stream,
    /// The target, if it was still in the map (it is not after a handler disconnect).
    target: Option<Unary>,
    end: StreamEnd,
}

/// Ordered delivery of the multi-part messages of one request to its sender.
struct Relay {
    /// The sender's outbound channel (weak: it never keeps a closed stream open).
    out: WeakSender<Result<ServerMessage, Status>>,
    /// The delivery still waiting for room in the sender's buffer, if any: later messages of
    /// the request queue behind it, so the sender sees the parts and the end in order.
    delivery: Option<JoinHandle<()>>,
    /// The sender's service and instance, for the log line and the buffer-full counter.
    service: String,
    instance: InstanceId,
}

impl Relay {
    fn new(
        out: WeakSender<Result<ServerMessage, Status>>,
        service: Option<&str>,
        instance: &str,
    ) -> Self {
        Self {
            out,
            delivery: None,
            service: service.unwrap_or("unknown").to_string(),
            instance: instance.to_string(),
        }
    }

    /// Sends `msg` to the sender in order: at once when nothing is still waiting and the buffer
    /// has room, otherwise behind the waiting delivery, which waits up to
    /// [`TERMINAL_SEND_TIMEOUT`] for room (as terminal messages do).
    fn deliver(&mut self, msg: ServerMessage, metrics: &Arc<Metrics>) {
        let Some(tx) = self.out.upgrade() else {
            return;
        };
        let behind = self.delivery.as_ref().is_some_and(|h| !h.is_finished());
        let item = if behind {
            Ok(msg)
        } else {
            match tx.try_send(Ok(msg)) {
                Ok(()) => return,
                Err(TrySendError::Closed(_)) => return,
                Err(TrySendError::Full(item)) => item,
            }
        };
        let previous = self.delivery.take();
        let metrics = Arc::clone(metrics);
        let service = self.service.clone();
        let instance = self.instance.clone();
        self.delivery = Some(tokio::spawn(async move {
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            if let Err(e) = tx.send_timeout(item, TERMINAL_SEND_TIMEOUT).await {
                let reason = match e {
                    SendTimeoutError::Timeout(_) => "buffer still full",
                    SendTimeoutError::Closed(_) => "stream closed",
                };
                tracing::warn!(
                    instance,
                    service,
                    reason,
                    "multi-part message not delivered"
                );
                metrics.buffer_full(&service);
            }
        }));
    }
}

impl Pending {
    fn unary(&self) -> Option<&Unary> {
        match self {
            Pending::Unary(u) => Some(u),
            Pending::Scatter(_) | Pending::Stream(_) => None,
        }
    }

    fn unary_mut(&mut self) -> Option<&mut Unary> {
        match self {
            Pending::Unary(u) => Some(u),
            Pending::Scatter(_) | Pending::Stream(_) => None,
        }
    }

    fn sender(&self) -> &str {
        match self {
            Pending::Unary(u) => &u.sender,
            Pending::Scatter(g) => &g.sender,
            Pending::Stream(s) => &s.sender,
        }
    }

    fn sender_token(&self) -> Option<u64> {
        match self {
            Pending::Unary(u) => u.sender_token,
            Pending::Scatter(g) => g.sender_token,
            Pending::Stream(s) => Some(s.sender_token),
        }
    }

    fn context(&self) -> &ContextId {
        match self {
            Pending::Unary(u) => &u.context,
            Pending::Scatter(g) => &g.context,
            Pending::Stream(s) => &s.context,
        }
    }

    /// A request its sender waits for (a scatter-gather target is part of its gather instead).
    fn is_request(&self) -> bool {
        !matches!(
            self,
            Pending::Unary(Unary {
                part_of: Some(_),
                ..
            })
        )
    }
}

/// A gather taken out of the map: its `ReplyEnd` is sent, the rest is bookkeeping.
struct Closed {
    gather: Scatter,
    /// Targets that had not answered, removed from the map with the gather.
    rest: Vec<(String, Unary)>,
    timed_out: u32,
}

/// Label of `Request.mode` for the `mode` metric attribute.
pub(crate) fn mode_label(mode: i32) -> &'static str {
    match RequestMode::try_from(mode) {
        Ok(RequestMode::Unary) => "unary",
        Ok(RequestMode::ScatterGather) => "scatter_gather",
        Ok(RequestMode::Streaming) => "streaming",
        Ok(RequestMode::Subscription) => "subscription",
        Err(_) => "unknown",
    }
}

/// Pending requests touching one instance, by role.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InFlight {
    pub as_handler: u32,
    pub as_sender: u32,
}

/// Flow state of one session.
struct Flow {
    token: u64,
    service: String,
    /// Permits left; `None` is unlimited (no flow control, the v1 behaviour).
    permits: Option<u32>,
    /// Permits never exceed it, whatever a client sends back. Starts at
    /// `Register.initial_permits`; only a `Permits.grant` from the session raises it (multi-hub
    /// top-up, ADR 0016), and nothing lowers it.
    cap: u32,
    queue: InstanceQueue,
    /// Ids of the requests assigned to this session and not ended yet: queued plus forwarded.
    /// A set, so ending a request twice (or before it was assigned) cannot skew the count.
    assigned: HashSet<String>,
}

/// One session's flow state, for the topology view.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FlowView {
    pub flow_controlled: bool,
    pub permits_available: u32,
    pub queued: u32,
}

/// How busy a routing candidate is, for [`choose`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Load {
    /// Unlimited, or has a permit and nothing queued: a request would be forwarded at once.
    pub can_take: bool,
    /// Queued plus forwarded requests.
    pub assigned: u32,
}

/// Index of the candidate an unkeyed request goes to: the best tag-match score against the
/// sender's tags (number of equal key/value pairs), then the candidates that can take a request
/// now, then the least `assigned / load_factor`, then `rotation` among the remaining ties.
pub(crate) fn choose(
    candidates: &[Candidate],
    sender_tags: &BTreeMap<String, String>,
    rotation: usize,
    load: impl Fn(&str) -> Load,
) -> usize {
    let score = |c: &Candidate| {
        sender_tags
            .iter()
            .filter(|(k, v)| c.tags.get(*k) == Some(*v))
            .count()
    };
    let loads: Vec<Load> = candidates.iter().map(|c| load(&c.id)).collect();
    let best_score = candidates.iter().map(score).max().unwrap_or(0);
    let mut pool: Vec<usize> = (0..candidates.len())
        .filter(|&i| score(&candidates[i]) == best_score)
        .collect();
    if pool.iter().any(|&i| loads[i].can_take) {
        pool.retain(|&i| loads[i].can_take);
    }
    // a / lf_a < b / lf_b  <=>  a * lf_b < b * lf_a (no floats, load factors are >= 1).
    let weighted = |i: usize| {
        (
            u64::from(loads[i].assigned),
            u64::from(candidates[i].load_factor.max(1)),
        )
    };
    let lighter = |a: usize, b: usize| {
        let ((la, fa), (lb, fb)) = (weighted(a), weighted(b));
        (la * fb).cmp(&(lb * fa))
    };
    let min = *pool
        .iter()
        .min_by(|&&a, &&b| lighter(a, b))
        .expect("at least one candidate");
    pool.retain(|&i| lighter(i, min).is_eq());
    pool[rotation % pool.len()]
}

pub struct Router {
    registry: Arc<Registry>,
    sessions: Arc<Sessions>,
    pending: Mutex<HashMap<String, Pending>>,
    flows: Mutex<HashMap<InstanceId, Flow>>,
    /// Bytes queued over all flows (`KRAKEN_HUB_QUEUE_BYTES_TOTAL`).
    queued_bytes: Arc<TotalBytes>,
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    /// `(sender service, type)` statistics for the dashboard; `None` with the dashboard off.
    edges: Option<EdgeStats>,
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

fn no_handler_message(type_name: &str, e: PickError) -> String {
    match e {
        PickError::UnknownType => format!("no handler registered for type {type_name}"),
        PickError::NoLiveInstance { service } => {
            format!("owner known, no instance for service {service}")
        }
    }
}

fn outcome_of(code: ErrorCode) -> Outcome {
    match code {
        ErrorCode::NoHandler => Outcome::NoHandler,
        ErrorCode::InvalidRequest => Outcome::InvalidRequest,
        // Handler-produced: the receiving service denied an authenticated caller. Counted as a
        // handler error like every other Reply{error} relayed by the hub.
        ErrorCode::PermissionDenied => Outcome::HandlerError,
        ErrorCode::Timeout => Outcome::Timeout,
        ErrorCode::HandlerDisconnected => Outcome::HandlerDisconnected,
        ErrorCode::ResourceExhausted => Outcome::ResourceExhausted,
        // The hub cancels a stream that outlived KRAKEN_HUB_MAX_STREAM_DURATION.
        ErrorCode::Cancelled => Outcome::Cancelled,
        ErrorCode::HandlerError
        | ErrorCode::Unauthenticated
        | ErrorCode::Unsupported
        | ErrorCode::Unspecified => Outcome::HandlerError,
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
            flows: Mutex::new(HashMap::new()),
            queued_bytes: TotalBytes::new(config.queue_bytes_total),
            edges: config
                .dashboard_enabled
                .then(|| EdgeStats::new(config.edge_stats_window, config.edge_stats_max_edges)),
            config,
            metrics,
        })
    }

    /// The edge statistics, `None` when the dashboard is disabled.
    pub fn edges(&self) -> Option<&EdgeStats> {
        self.edges.as_ref()
    }

    /// Counts a request that reached its terminal state on its `(sender service, type)` edge.
    /// `latency` is `None` for a request answered without being routed.
    fn record_edge(
        &self,
        sender_service: Option<&str>,
        type_name: &str,
        kind: Kind,
        outcome: Outcome,
        latency: Option<Duration>,
    ) {
        self.record_edge_error(
            sender_service,
            type_name,
            kind,
            outcome != Outcome::Ok,
            latency,
        );
    }

    /// Like [`Router::record_edge`] with the error decided by the caller (a stream the sender
    /// cancelled is no error, whatever its outcome label).
    fn record_edge_error(
        &self,
        sender_service: Option<&str>,
        type_name: &str,
        kind: Kind,
        error: bool,
        latency: Option<Duration>,
    ) {
        if let (Some(edges), Some(sender)) = (&self.edges, sender_service)
            && !type_name.is_empty()
        {
            edges.record(sender, type_name, kind, error, latency, Instant::now());
        }
    }

    /// Starts the flow state of a session (replacing an older session's): `permits` `Some` makes
    /// it flow-controlled, `None` unlimited. Call before the session becomes current
    /// ([`Sessions::insert_with_token`]), so a request routed to the session always finds its flow.
    /// Returns false, and changes nothing, when a newer session of the instance already opened
    /// its flow (a concurrent, older handshake must give up).
    pub fn open_session(
        &self,
        _ctx: &ContextId,
        instance_id: &str,
        token: u64,
        service: &str,
        permits: Option<u32>,
    ) -> bool {
        let flow = Flow {
            token,
            service: service.to_string(),
            permits,
            cap: permits.unwrap_or(0),
            queue: InstanceQueue::with_total(
                self.config.queue_capacity_per_instance,
                self.config.queue_bytes_per_instance,
                Arc::clone(&self.queued_bytes),
            ),
            assigned: HashSet::new(),
        };
        let mut flow = flow;
        let mut moved = Vec::new();
        let mut overflow = Vec::new();
        let replaced = {
            let mut flows = self.flows.lock().unwrap();
            if flows.get(instance_id).is_some_and(|f| f.token > token) {
                return false;
            }
            let replaced = flows.remove(instance_id).map(|mut old| {
                // Requests the replaced session never got wait for this one instead of failing
                // with it (ADR 0014): same order, same deadlines.
                for item in old.queue.drain() {
                    let id = item.id.clone();
                    match flow.queue.push(item) {
                        Ok(()) => {
                            flow.assigned.insert(id.clone());
                            moved.push(id);
                        }
                        Err(_) => overflow.push(id),
                    }
                }
                old.token
            });
            flows.insert(instance_id.to_string(), flow);
            replaced
        };
        if let Some(old) = replaced {
            let mut pending = self.pending.lock().unwrap();
            for id in &moved {
                if let Some(p) = pending
                    .get_mut(id)
                    .and_then(Pending::unary_mut)
                    .filter(|p| p.handler == instance_id && p.handler_token == old)
                {
                    p.handler_token = token;
                }
            }
        }
        for id in overflow {
            self.reject_busy(&id, "queue full");
        }
        true
    }

    /// Forwards what a new session can take of the queue it took over from the session it
    /// replaced. Call once the session is current (after its ack).
    pub fn start_session(&self, ctx: &ContextId, instance_id: &str, token: u64) {
        let (service, ready) = {
            let mut flows = self.flows.lock().unwrap();
            let Some(f) = flows.get_mut(instance_id).filter(|f| f.token == token) else {
                return;
            };
            let ready = if f.permits.is_none() {
                f.queue.drain()
            } else {
                drain(f)
            };
            (f.service.clone(), ready)
        };
        if !ready.is_empty() {
            self.forward_ready(ctx, instance_id, token, &service, ready);
        }
    }

    /// The instance re-registered without types (a graceful stop): the registry no longer
    /// offers it as a target, and the requests still waiting for it are routed again, so a
    /// stopping instance draws no new work. What it was already forwarded it still answers.
    pub fn on_stopping(&self, ctx: &ContextId, instance_id: &str, token: u64) {
        let waiting = {
            let mut flows = self.flows.lock().unwrap();
            let Some(f) = flows.get_mut(instance_id).filter(|f| f.token == token) else {
                return;
            };
            let waiting = f.queue.drain();
            for item in &waiting {
                f.assigned.remove(&item.id);
            }
            waiting
        };
        for item in waiting {
            self.reroute(ctx, item);
        }
    }

    /// `Permits{count, grant}` from session `token`: raises the session's cap to `grant` when
    /// above it (a grant never lowers the cap), then adds `count` permits, clamped at the cap, and
    /// forwards queued requests. Ignored for an unlimited or replaced session.
    pub fn on_permits(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        token: u64,
        count: u32,
        grant: u32,
    ) {
        let (service, ready) = {
            let mut flows = self.flows.lock().unwrap();
            let Some(flow) = flows.get_mut(instance_id).filter(|f| f.token == token) else {
                return;
            };
            let Some(permits) = flow.permits.as_mut() else {
                return;
            };
            flow.cap = flow.cap.max(grant);
            *permits = permits.saturating_add(count).min(flow.cap);
            (flow.service.clone(), drain(flow))
        };
        self.forward_ready(ctx, instance_id, token, &service, ready);
    }

    /// Flow state per instance (current sessions only).
    pub fn flow_by_instance(&self) -> HashMap<InstanceId, FlowView> {
        self.flows
            .lock()
            .unwrap()
            .iter()
            .map(|(id, f)| {
                (
                    id.clone(),
                    FlowView {
                        flow_controlled: f.permits.is_some(),
                        permits_available: f.permits.unwrap_or(0),
                        queued: f.queue.len() as u32,
                    },
                )
            })
            .collect()
    }

    /// Requests waiting in all queues.
    pub fn queued_total(&self) -> usize {
        self.flows
            .lock()
            .unwrap()
            .values()
            .map(|f| f.queue.len())
            .sum()
    }

    /// Load information of `candidates` for [`choose`].
    fn loads(&self, candidates: &[Candidate]) -> HashMap<InstanceId, Load> {
        let flows = self.flows.lock().unwrap();
        candidates
            .iter()
            .map(|c| {
                let load = match flows.get(&c.id) {
                    Some(f) => Load {
                        can_take: f.permits.is_none_or(|p| p > 0) && f.queue.is_empty(),
                        assigned: f.assigned.len() as u32,
                    },
                    None => Load {
                        can_take: true,
                        assigned: 0,
                    },
                };
                (c.id.clone(), load)
            })
            .collect()
    }

    /// Ends the flow bookkeeping of a pending entry that was just removed from the map: out of
    /// its handler's queue if it was still queued, and no longer assigned. (Whether the handler
    /// got it is `entry.forwarded`, not "was it queued": a popped entry may not be written yet.)
    fn release(&self, id: &str, entry: &Unary) {
        if let Some(f) = self
            .flows
            .lock()
            .unwrap()
            .get_mut(&entry.handler)
            .filter(|f| f.token == entry.handler_token)
        {
            f.queue.remove(id);
            f.assigned.remove(id);
        }
    }

    /// A forward that was not written gives its permit back.
    fn return_permit(&self, handler: &str, token: u64) {
        if let Some(f) = self
            .flows
            .lock()
            .unwrap()
            .get_mut(handler)
            .filter(|f| f.token == token)
            && let Some(p) = f.permits.as_mut()
        {
            *p = (*p + 1).min(f.cap);
        }
    }

    /// The request `id` is still pending and assigned to `handler`'s session `token`.
    fn is_pending_for(&self, id: &str, handler: &str, token: u64) -> bool {
        self.pending
            .lock()
            .unwrap()
            .get(id)
            .and_then(Pending::unary)
            .is_some_and(|p| p.handler == handler && p.handler_token == token)
    }

    /// Removes and returns the forwarded request `id`, if it is still pending.
    fn take_unary(&self, id: &str) -> Option<Unary> {
        let mut pending = self.pending.lock().unwrap();
        if !matches!(pending.get(id), Some(Pending::Unary(_))) {
            return None;
        }
        match pending.remove(id) {
            Some(Pending::Unary(u)) => Some(u),
            _ => None,
        }
    }

    /// Sends requests popped from `handler`'s queue (one permit each). An entry whose request
    /// already ended (a race with its timeout, or a queue taken over from a replaced session)
    /// is not sent: its permit goes to the next queued request. A failed write returns the
    /// permit and fails the request.
    fn forward_ready(
        &self,
        ctx: &ContextId,
        handler: &str,
        token: u64,
        service: &str,
        mut ready: Vec<Queued>,
    ) {
        while !ready.is_empty() {
            let mut ended = Vec::new();
            for item in ready {
                if !self.is_pending_for(&item.id, handler, token) {
                    ended.push(item.id);
                    continue;
                }
                self.metrics
                    .queue_waited(service, &item.type_name, item.enqueued_at.elapsed());
                match self.sessions.send_if_current(ctx, handler, token, item.msg) {
                    Ok(()) => self.note_forwarded(ctx, &item.id, handler, token, service),
                    Err(e) => {
                        self.return_permit(handler, token);
                        self.forward_failed(&item.id, handler, service, e);
                    }
                }
            }
            if ended.is_empty() {
                break;
            }
            ready = {
                let mut flows = self.flows.lock().unwrap();
                let Some(f) = flows.get_mut(handler).filter(|f| f.token == token) else {
                    break;
                };
                for id in &ended {
                    f.assigned.remove(id);
                }
                if let Some(p) = f.permits.as_mut() {
                    *p = p.saturating_add(ended.len() as u32).min(f.cap);
                }
                drain(f)
            };
        }
    }

    fn forward_failed(&self, id: &str, handler: &str, service: &str, e: SendError) {
        match e {
            SendError::Busy => {
                self.metrics.buffer_full(service);
                self.metrics.rejected(service, "buffer_full");
                self.reject_busy(id, "outbound buffer full");
            }
            SendError::Gone => self.fail(
                id,
                ErrorCode::HandlerDisconnected,
                format!("handler instance {handler} disconnected"),
            ),
        }
    }

    /// Fails a pending request for capacity: RESOURCE_EXHAUSTED to a sender that shares
    /// flow-control, HANDLER_ERROR "handler busy" (the v1 answer) to any other sender.
    fn reject_busy(&self, id: &str, why: &str) {
        if let Some(entry) = self.take_unary(id) {
            entry.abort_timer();
            self.release(id, &entry);
            let (code, message) = if entry.sender_flow_control {
                (ErrorCode::ResourceExhausted, format!("handler busy: {why}"))
            } else {
                (ErrorCode::HandlerError, "handler busy".to_string())
            };
            self.finish_with_outcome(id, entry, code, message, Outcome::ResourceExhausted);
        }
    }

    /// Called for every Request a sender wrote. Never returns an error to the caller: every
    /// failure becomes a `ServerMessage::Error` sent to `sender`.
    pub fn route(self: &Arc<Self>, ctx: &ContextId, sender: &str, req: Request) {
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
        let sender_info = self.sessions.sender_info(ctx, sender);
        let sender_service = sender_info.as_ref().map(|s| s.service.as_str());
        let Some(req_kind) = req_kind.filter(|_| !type_name.is_empty() && !id.is_empty()) else {
            let message = if id.is_empty() {
                "invalid request: missing id"
            } else {
                "invalid request: missing kind or type"
            };
            // Without a kind there is no metrics label, so only kind-bearing requests are counted.
            if let Some(kind) = req_kind {
                self.record_rejected(sender_service, kind, &type_name, Outcome::InvalidRequest);
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
                ctx,
                sender,
                error_message(&id, ErrorCode::InvalidRequest, message),
            );
            return;
        };
        if req.mode != RequestMode::Unary as i32 {
            self.route_mode(ctx, sender, req, req_kind, sender_info, span);
            return;
        }

        let (kind, handler) = match self.pick_handler(ctx, sender, &type_name, &req.routing_key) {
            Ok(picked) => picked,
            Err(e) => {
                let message = no_handler_message(&type_name, e);
                self.record_rejected(sender_service, req_kind, &type_name, Outcome::NoHandler);
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
                self.send_to(
                    ctx,
                    sender,
                    error_message(&id, ErrorCode::NoHandler, message),
                );
                return;
            }
        };

        span.record("kraken.handler", handler.as_str());
        let (sender_token, sender_flow_control) = match &sender_info {
            Some(info) => (Some(info.token), info.flow_control),
            None => (None, false),
        };
        // `None`: the handler's session vanished since the pick; the send below reports it Gone.
        let handler_token = self.sessions.token_of(ctx, &handler).unwrap_or(u64::MAX);
        let timeout = self.config.effective_timeout(&type_name, req.timeout_ms);

        // Insert before forwarding so a fast reply always finds its entry. The lock is held
        // while spawning so an immediate timeout cannot run before the entry exists.
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.contains_key(&id) {
                drop(pending);
                self.record_rejected(sender_service, kind, &type_name, Outcome::InvalidRequest);
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
                    ctx,
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
            let entry = Unary {
                context: ctx.clone(),
                sender: sender.to_string(),
                sender_token,
                sender_service: sender_info.map(|s| s.service),
                handler: handler.clone(),
                handler_token,
                forwarded: false,
                sender_flow_control,
                kind,
                type_name,
                started: Instant::now(),
                cancel: Some(cancel),
                span,
                part_of: None,
            };
            pending.insert(id.clone(), Pending::Unary(entry));
        }

        self.dispatch(ctx, &id, &handler, handler_token, req);
    }

    /// The instance a request goes to: the ring owner of a keyed command, else the best of the
    /// owner's instances for `sender` ([`choose`]).
    fn pick_handler(
        &self,
        ctx: &ContextId,
        sender: &str,
        type_name: &str,
        routing_key: &str,
    ) -> Result<(Kind, InstanceId), PickError> {
        let (kind, target) = self.registry.pick_target(ctx, type_name, routing_key)?;
        let handler = match target {
            Target::Keyed(id) => id,
            Target::Candidates {
                mut candidates,
                rotation,
            } => {
                let sender_tags = self
                    .registry
                    .placement_of(ctx, sender)
                    .map(|p| p.tags)
                    .unwrap_or_default();
                let loads = self.loads(&candidates);
                let i = choose(&candidates, &sender_tags, rotation, |id| loads[id]);
                candidates.swap_remove(i).id
            }
        };
        Ok((kind, handler))
    }

    /// Routes a request that never left the hub again, because the instance it waited for left
    /// or stopped: same id, same deadline (its timer keeps running), normal routing (a keyed
    /// command goes to the new ring owner); no instance left is NO_HANDLER.
    fn reroute(&self, ctx: &ContextId, item: Queued) {
        let Some(server_message::Message::Request(req)) = item.msg.message else {
            return;
        };
        let id = item.id;
        let Some((sender, left, target)) = self
            .pending
            .lock()
            .unwrap()
            .get(&id)
            .and_then(Pending::unary)
            .map(|p| (p.sender.clone(), p.handler.clone(), p.part_of.is_some()))
        else {
            return; // ended meanwhile
        };
        if target {
            // A scatter-gather target is that instance: another one already has its own target.
            self.fail(
                &id,
                ErrorCode::HandlerDisconnected,
                format!("handler instance {left} left before the request reached it"),
            );
            return;
        }
        let handler = match self.pick_handler(ctx, &sender, &req.r#type, &req.routing_key) {
            Ok((_, handler)) => handler,
            Err(e) => {
                let message = no_handler_message(&req.r#type, e);
                self.fail(&id, ErrorCode::NoHandler, message);
                return;
            }
        };
        let token = self.sessions.token_of(ctx, &handler).unwrap_or(u64::MAX);
        {
            let mut pending = self.pending.lock().unwrap();
            let Some(p) = pending.get_mut(&id).and_then(Pending::unary_mut) else {
                return;
            };
            p.handler = handler.clone();
            p.handler_token = token;
            p.span.record("kraken.handler", handler.as_str());
        }
        tracing::debug!(request_id = %id, handler = %handler, "queued request routed again");
        self.dispatch(ctx, &id, &handler, token, req);
    }

    /// Forwards a routed request (its pending entry exists) to `handler`'s session `token`: at
    /// once when the session is unlimited, else through its queue and permits. Only to the
    /// session the entry names: if it was replaced meanwhile the request fails (Gone) instead of
    /// reaching a stream whose requests the replacement already failed.
    fn dispatch(&self, ctx: &ContextId, id: &str, handler: &str, token: u64, req: Request) {
        // The request may have ended (timeout) between its pending insert and this dispatch.
        if !self.is_pending_for(id, handler, token) {
            return;
        }
        let priority = req.priority;
        let type_name = req.r#type.clone();
        let msg = ServerMessage {
            message: Some(server_message::Message::Request(req)),
        };
        enum Next {
            Send(ServerMessage, String),
            /// With one of the session's permits.
            Forward(ServerMessage, String),
            Drained(Vec<Queued>, String),
            Rejected(&'static str, String),
            Closed,
        }
        let next = {
            let mut flows = self.flows.lock().unwrap();
            match flows.get_mut(handler).filter(|f| f.token == token) {
                // No flow state for this session: it ended (or was replaced, or never became
                // current). Sending would bypass its permits, so the request fails instead.
                None => Next::Closed,
                Some(flow) => {
                    flow.assigned.insert(id.to_string());
                    let service = flow.service.clone();
                    if flow.permits.is_none() {
                        Next::Send(msg, service)
                    } else if flow.queue.is_empty()
                        && let Some(p) = flow.permits.as_mut().filter(|p| **p > 0)
                    {
                        // A permit and nobody waiting: forward at once. The queue caps bound
                        // what waits, so a request that can go now is not measured against them.
                        *p -= 1;
                        Next::Forward(msg, service)
                    } else {
                        let bytes = prost::Message::encoded_len(&msg);
                        let item = Queued {
                            id: id.to_string(),
                            type_name,
                            priority,
                            bytes,
                            enqueued_at: Instant::now().into_std(),
                            msg,
                        };
                        match flow.queue.push(item) {
                            Ok(()) => Next::Drained(drain(flow), service),
                            Err((full, _)) => Next::Rejected(full.reason(), service),
                        }
                    }
                }
            }
        };
        match next {
            Next::Send(msg, service) => {
                match self.sessions.send_if_current(ctx, handler, token, msg) {
                    Ok(()) => self.note_forwarded(ctx, id, handler, token, &service),
                    Err(e) => self.forward_failed(id, handler, &service, e),
                }
            }
            Next::Forward(msg, service) => {
                match self.sessions.send_if_current(ctx, handler, token, msg) {
                    Ok(()) => self.note_forwarded(ctx, id, handler, token, &service),
                    Err(e) => {
                        self.return_permit(handler, token);
                        self.forward_failed(id, handler, &service, e);
                    }
                }
            }
            Next::Drained(ready, service) => {
                // Ended meanwhile: take it back out, so no orphan entry or assignment remains.
                // (If it was already drained, `forward_ready` skips it.)
                if !self.is_pending_for(id, handler, token)
                    && let Some(f) = self
                        .flows
                        .lock()
                        .unwrap()
                        .get_mut(handler)
                        .filter(|f| f.token == token)
                {
                    f.queue.remove(id);
                    f.assigned.remove(id);
                }
                self.forward_ready(ctx, handler, token, &service, ready);
            }
            Next::Rejected(reason, service) => {
                self.metrics.rejected(&service, reason);
                self.reject_busy(id, "queue full");
            }
            Next::Closed => self.fail(
                id,
                ErrorCode::HandlerDisconnected,
                format!("handler instance {handler} disconnected"),
            ),
        }
    }

    /// Reply from a handler; relays to the recorded sender if the id is pending and `from` is
    /// the recorded handler. Unknown or mismatched ids are ignored.
    pub fn on_reply(&self, ctx: &ContextId, from: &str, reply: Reply) {
        let id = reply.request_id.clone();
        let entry = {
            let mut pending = self.pending.lock().unwrap();
            match pending.get(&id).and_then(Pending::unary) {
                None => {
                    tracing::debug!(request_id = %id, "reply for unknown request ignored");
                    return;
                }
                Some(p) if p.handler != from || p.context != *ctx => {
                    tracing::debug!(request_id = %id, from, "reply from non-handler ignored");
                    return;
                }
                Some(_) => match pending.remove(&id) {
                    Some(Pending::Unary(u)) => u,
                    _ => unreachable!("entry checked above"),
                },
            }
        };
        entry.abort_timer();
        self.release(&id, &entry);
        if entry.part_of.is_some() {
            let result = match reply.result {
                Some(reply::Result::Error(e)) => {
                    log_outcome(
                        &id,
                        &entry.type_name,
                        Some(entry.kind),
                        &entry.sender,
                        Some(&entry.handler),
                        Outcome::HandlerError,
                        true,
                    );
                    Err((e, Outcome::HandlerError))
                }
                Some(reply::Result::Payload(payload)) => Ok(payload),
                None => Ok(Vec::new()),
            };
            self.on_part(&id, entry, result);
            return;
        }
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
        let elapsed = entry.started.elapsed();
        self.metrics
            .request_finished(entry.kind, &entry.type_name, outcome, elapsed);
        self.record_edge(
            entry.sender_service.as_deref(),
            &entry.type_name,
            entry.kind,
            outcome,
            Some(elapsed),
        );
        self.send_to(
            &entry.context,
            &entry.sender,
            ServerMessage {
                message: Some(server_message::Message::Reply(reply)),
            },
        );
    }

    /// Ends the session `token` of `instance_id` (stream closed, evicted or replaced by a
    /// reconnect): fails the pending requests forwarded to that session with
    /// HANDLER_DISCONNECTED and drops the pending requests it sent. Entries of a newer session
    /// with the same instance id are untouched.
    pub fn on_disconnect(&self, ctx: &ContextId, instance_id: &str, token: u64) {
        // The flow state goes first: a request routed concurrently then either finds no flow and
        // fails at once, or was assigned before and is among the pending entries collected below.
        let waiting = {
            let mut flows = self.flows.lock().unwrap();
            match flows.get(instance_id) {
                Some(f) if f.token == token => flows
                    .remove(instance_id)
                    .map(|mut f| f.queue.drain())
                    .unwrap_or_default(),
                _ => Vec::new(),
            }
        };
        // Queued requests never reached the instance: they are routed again, not failed.
        let waiting_ids: HashSet<&str> = waiting.iter().map(|q| q.id.as_str()).collect();
        let handled_by = |p: &Pending| {
            p.unary()
                .is_some_and(|u| u.handler == instance_id && u.handler_token == token)
        };
        let sent_by = |p: &Pending| p.sender() == instance_id && p.sender_token() == Some(token);
        let (failed, dropped) = {
            let mut pending = self.pending.lock().unwrap();
            let ids: Vec<String> = pending
                .iter()
                .filter(|(_, p)| p.context() == ctx && (handled_by(p) || sent_by(p)))
                .map(|(id, _)| id.clone())
                .collect();
            let mut failed = Vec::new();
            let mut dropped = Vec::new();
            for id in ids {
                let sent = sent_by(&pending[&id]);
                if !sent && waiting_ids.contains(id.as_str()) {
                    continue; // stays pending, routed again below
                }
                match pending.remove(&id).expect("id collected under same lock") {
                    entry if sent => dropped.push((id, entry)),
                    Pending::Unary(entry) => failed.push((id, entry)),
                    Pending::Scatter(_) | Pending::Stream(_) => {
                        unreachable!("a gather or stream has no handler of its own")
                    }
                }
            }
            (failed, dropped)
        };
        // The sender is gone: nobody to answer, so no outcome metric, but it is no longer in
        // flight. A gather's or stream's targets have the same sender, so they are dropped with
        // it; a target still streaming (or gathering, with a capable handler) is cancelled.
        for (id, entry) in dropped {
            match entry {
                Pending::Unary(entry) => {
                    entry.abort_timer();
                    self.release(&id, &entry);
                    if entry.part_of.is_none() {
                        self.metrics.in_flight_add(entry.kind, -1);
                    } else if entry.forwarded {
                        self.cancel_target(ctx, &id, &entry, "sender disconnected");
                    }
                    entry.span.record("kraken.outcome", "sender_disconnected");
                }
                Pending::Scatter(gather) => {
                    gather.cancel.abort();
                    self.metrics.in_flight_add(gather.kind, -1);
                    gather.span.record("kraken.outcome", "sender_disconnected");
                }
                Pending::Stream(stream) => {
                    stream.idle.abort();
                    stream.max_duration.abort();
                    self.metrics.in_flight_add(stream.kind, -1);
                    self.metrics
                        .stream_closed(&stream.handler_service, &stream.handler);
                    stream.span.record("kraken.outcome", "sender_disconnected");
                }
            }
        }
        for item in waiting {
            self.reroute(ctx, item); // a dropped one is no longer pending: skipped
        }
        for (id, entry) in failed {
            entry.abort_timer();
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
        self.metrics.flows_set(self.flow_gauges());
    }

    /// Requests queued and permits left per service, over flow-controlled instances: one pass
    /// for the whole hub, run by the watchdog every tick (at most a second) instead of per
    /// request, so the gauges are right after timeouts, departures and returned permits too.
    fn flow_gauges(&self) -> HashMap<String, (u64, u64)> {
        let flows = self.flows.lock().unwrap();
        let mut by_service: HashMap<String, (u64, u64)> = HashMap::new();
        for f in flows.values().filter(|f| f.permits.is_some()) {
            let entry = by_service.entry(f.service.clone()).or_default();
            entry.0 += f.queue.len() as u64;
            entry.1 += u64::from(f.permits.unwrap_or(0));
        }
        by_service
    }

    /// Pending requests per instance (as handler and as sender) and the total pending count,
    /// from one pass under the pending lock.
    pub fn in_flight_by_instance(&self) -> (HashMap<InstanceId, InFlight>, u32) {
        let pending = self.pending.lock().unwrap();
        let mut by_instance: HashMap<InstanceId, InFlight> = HashMap::new();
        let mut requests = 0;
        for p in pending.values() {
            if let Some(u) = p.unary() {
                by_instance.entry(u.handler.clone()).or_default().as_handler += 1;
            }
            if p.is_request() {
                by_instance
                    .entry(p.sender().to_string())
                    .or_default()
                    .as_sender += 1;
                requests += 1;
            }
        }
        (by_instance, requests)
    }

    /// Requests in flight as their senders see them: a scatter-gather counts once, not per
    /// target.
    pub fn in_flight(&self) -> usize {
        self.pending
            .lock()
            .unwrap()
            .values()
            .filter(|p| p.is_request())
            .count()
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
        if let Some(entry) = self.take_unary(id) {
            entry.abort_timer();
            self.release(id, &entry);
            self.finish_with_error(id, entry, code, message);
        }
    }

    /// `entry` must already have been removed from the map (and released) by the caller.
    fn finish_with_error(&self, id: &str, entry: Unary, code: ErrorCode, message: String) {
        self.finish_with_outcome(id, entry, code, message, outcome_of(code));
    }

    fn finish_with_outcome(
        &self,
        id: &str,
        entry: Unary,
        code: ErrorCode,
        message: String,
        outcome: Outcome,
    ) {
        record_outcome(&entry.span, outcome);
        // A stream target's failure is the stream's: `finish_stream` logs it once, for `<id>`.
        let stream_target = entry.part_of.as_ref().is_some_and(|p| {
            matches!(
                self.pending.lock().unwrap().get(&p.parent),
                Some(Pending::Stream(_))
            )
        });
        if !stream_target {
            log_outcome(
                id,
                &entry.type_name,
                Some(entry.kind),
                &entry.sender,
                Some(&entry.handler),
                outcome,
                false,
            );
        }
        if entry.part_of.is_some() {
            let error = crate::proto::Error {
                request_id: id.to_string(),
                code: code as i32,
                message,
                details: vec![],
            };
            self.on_part(id, entry, Err((error, outcome)));
            return;
        }
        let elapsed = entry.started.elapsed();
        self.metrics
            .request_finished(entry.kind, &entry.type_name, outcome, elapsed);
        self.record_edge(
            entry.sender_service.as_deref(),
            &entry.type_name,
            entry.kind,
            outcome,
            Some(elapsed),
        );
        self.send_to(
            &entry.context,
            &entry.sender,
            error_message(id, code, message),
        );
    }

    /// A request answered before it was routed (no pending entry): counted in the metrics and
    /// on its edge, without a latency.
    fn record_rejected(
        &self,
        sender_service: Option<&str>,
        kind: Kind,
        type_name: &str,
        outcome: Outcome,
    ) {
        self.record_rejected_mode(sender_service, kind, type_name, outcome, "unary");
    }

    fn record_rejected_mode(
        &self,
        sender_service: Option<&str>,
        kind: Kind,
        type_name: &str,
        outcome: Outcome,
        mode: &'static str,
    ) {
        self.metrics.request_started(kind);
        self.metrics
            .request_finished_mode(kind, type_name, outcome, Duration::ZERO, mode);
        self.record_edge(sender_service, type_name, kind, outcome, None);
    }

    /// A request with a mode other than UNARY (ADR 0017). Gates, in this order: a v1 sender
    /// cannot know the newer codes (INVALID_REQUEST); a v2 sender gets UNSUPPORTED for a mode
    /// this hub does not implement or its session did not declare; the modes are for queries
    /// only (INVALID_REQUEST); no live target is NO_HANDLER (a streaming query whose owners have
    /// no capable instance: UNSUPPORTED); a duplicate id is INVALID_REQUEST.
    fn route_mode(
        self: &Arc<Self>,
        ctx: &ContextId,
        sender: &str,
        req: Request,
        req_kind: Kind,
        sender_info: Option<SenderInfo>,
        span: tracing::Span,
    ) {
        let id = req.id.clone();
        let type_name = req.r#type.clone();
        let mode = mode_label(req.mode);
        let sender_service = sender_info.as_ref().map(|s| s.service.clone());
        let m = ModeContext {
            ctx,
            sender,
            id: &id,
            type_name: &type_name,
            sender_service: sender_service.as_deref(),
            mode,
            span: &span,
        };
        let Some(info) = sender_info.filter(|s| protocol::is_v2(s.protocol_version)) else {
            let message = format!("invalid request: request mode {mode} needs protocol v2");
            return self.reject_mode(&m, req_kind, ErrorCode::InvalidRequest, message);
        };
        match RequestMode::try_from(req.mode) {
            Ok(RequestMode::ScatterGather) => {}
            Ok(RequestMode::Streaming) => {
                return self.route_stream(ctx, sender, req, req_kind, info, span);
            }
            _ => {
                let message = format!("request mode {mode} is not supported by this hub");
                return self.reject_mode(&m, req_kind, ErrorCode::Unsupported, message);
            }
        }
        if !info.scatter_gather {
            let message = format!(
                "request mode {mode} needs the \"{}\" capability, not declared by this session",
                protocol::SCATTER_GATHER
            );
            return self.reject_mode(&m, req_kind, ErrorCode::Unsupported, message);
        }
        let reject = |kind: Kind, code: ErrorCode, _outcome: Outcome, message: String| {
            self.reject_mode(&m, kind, code, message);
        };
        let for_queries = |type_name: &str| {
            format!("invalid request: {type_name} is a command; {mode} is for queries")
        };
        if req_kind == Kind::Command {
            let message = for_queries(&type_name);
            return reject(
                req_kind,
                ErrorCode::InvalidRequest,
                Outcome::InvalidRequest,
                message,
            );
        }
        let (kind, targets) = match self.registry.scatter_targets(ctx, &type_name) {
            Ok(found) => found,
            Err(e) => {
                let message = no_handler_message(&type_name, e);
                return reject(req_kind, ErrorCode::NoHandler, Outcome::NoHandler, message);
            }
        };
        if kind == Kind::Command {
            let message = for_queries(&type_name);
            return reject(
                kind,
                ErrorCode::InvalidRequest,
                Outcome::InvalidRequest,
                message,
            );
        }
        // The sender's own stream, weakly: the gather never keeps a closed stream open.
        let Some(out) = self
            .sessions
            .weak_sender_if_current(ctx, sender, info.token)
        else {
            return; // the sender left meanwhile: nobody to answer
        };
        let relay = Relay::new(out, Some(&info.service), sender);
        let timeout = self.config.effective_timeout(&type_name, req.timeout_ms);
        let targets: Vec<(String, crate::registry::ScatterTarget, u64)> = targets
            .into_iter()
            .enumerate()
            .map(|(n, t)| {
                let token = self.sessions.token_of(ctx, &t.id).unwrap_or(u64::MAX);
                (format!("{id}#{n}"), t, token)
            })
            .collect();
        span.record(
            "kraken.handler",
            format!("{} targets", targets.len()).as_str(),
        );
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.contains_key(&id) || targets.iter().any(|(t, _, _)| pending.contains_key(t))
            {
                drop(pending);
                let message = "invalid request: duplicate id".to_string();
                return reject(
                    kind,
                    ErrorCode::InvalidRequest,
                    Outcome::InvalidRequest,
                    message,
                );
            }
            self.metrics.request_started(kind);
            let router = Arc::clone(self);
            let task_id = id.clone();
            let cancel = tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                router.on_scatter_deadline(&task_id);
            })
            .abort_handle();
            let now = Instant::now();
            for (target_id, target, token) in &targets {
                let entry = Unary {
                    context: ctx.clone(),
                    sender: sender.to_string(),
                    sender_token: Some(info.token),
                    sender_service: Some(info.service.clone()),
                    handler: target.id.clone(),
                    handler_token: *token,
                    forwarded: false,
                    sender_flow_control: info.flow_control,
                    kind,
                    type_name: type_name.clone(),
                    started: now,
                    cancel: None,
                    span: tracing::Span::none(),
                    part_of: Some(PartOf {
                        parent: id.clone(),
                        service: target.service.clone(),
                    }),
                };
                pending.insert(target_id.clone(), Pending::Unary(entry));
            }
            let gather = Scatter {
                context: ctx.clone(),
                sender: sender.to_string(),
                sender_token: Some(info.token),
                sender_service: Some(info.service.clone()),
                kind,
                type_name: type_name.clone(),
                started: now,
                cancel,
                span,
                relay,
                outstanding: targets.iter().map(|(t, _, _)| t.clone()).collect(),
                parts: 0,
                ok: 0,
                failed: 0,
                max_results: req.max_results,
                last_failure: None,
            };
            pending.insert(id.clone(), Pending::Scatter(gather));
        }
        self.metrics.scatter_targets(&type_name, targets.len());
        // Each target gets an ordinary unary request with its own id: v1 handlers serve it
        // unchanged, and its answer is matched to its target.
        for (target_id, target, token) in targets {
            let forwarded = Request {
                id: target_id.clone(),
                mode: RequestMode::Unary as i32,
                max_results: 0,
                initial_credit: 0,
                ..req.clone()
            };
            self.dispatch(ctx, &target_id, &target.id, token, forwarded);
        }
    }

    /// The answer (or hub-side failure) of target `id` (already removed from the map and
    /// released). For a scatter-gather target: a `ReplyPart` to the sender, and the `ReplyEnd`
    /// when it was the last one needed. For a streaming target it ends the stream: a failure
    /// (the handler left, or the hub could not forward) as `ReplyEnd{error}`; a unary `Reply`
    /// from the handler as one part and the end, or `ReplyEnd{error}` for `Reply{error}`.
    fn on_part(
        &self,
        id: &str,
        entry: Unary,
        result: Result<Vec<u8>, (crate::proto::Error, Outcome)>,
    ) {
        let Some(part_of) = entry.part_of else {
            return;
        };
        if matches!(
            self.pending.lock().unwrap().get(&part_of.parent),
            Some(Pending::Stream(_))
        ) {
            self.on_stream_target_result(&part_of.parent, result);
            return;
        }
        let closed = {
            let mut pending = self.pending.lock().unwrap();
            let Some(Pending::Scatter(gather)) = pending.get_mut(&part_of.parent) else {
                return; // the gather ended (deadline, max_results, sender gone)
            };
            if !gather.outstanding.remove(id) {
                return;
            }
            let seq = u64::from(gather.parts);
            gather.parts += 1;
            let (payload, error) = match result {
                Ok(payload) => {
                    gather.ok += 1;
                    (payload, None)
                }
                Err((mut error, outcome)) => {
                    gather.failed += 1;
                    gather.last_failure = Some(outcome);
                    error.request_id = part_of.parent.clone();
                    (Vec::new(), Some(error))
                }
            };
            let part = ServerMessage {
                message: Some(server_message::Message::Part(ReplyPart {
                    request_id: part_of.parent.clone(),
                    payload,
                    error,
                    responder_service: part_of.service,
                    responder_instance: entry.handler,
                    seq,
                })),
            };
            gather.relay.deliver(part, &self.metrics);
            let done = gather.outstanding.is_empty()
                || (gather.max_results > 0 && gather.ok >= gather.max_results);
            if done {
                self.close_gather(&mut pending, &part_of.parent, false)
            } else {
                None
            }
        };
        if let Some(closed) = closed {
            self.finish_scatter(&part_of.parent, closed);
        }
    }

    /// The deadline of gather `id`: it ends with the targets that did not answer as timed out.
    fn on_scatter_deadline(&self, id: &str) {
        let closed = {
            let mut pending = self.pending.lock().unwrap();
            self.close_gather(&mut pending, id, true)
        };
        if let Some(closed) = closed {
            self.finish_scatter(id, closed);
        }
    }

    /// Takes gather `id` and its unanswered targets out of the map and sends its `ReplyEnd`
    /// (under the pending lock, so it follows every part). `deadline`: the unanswered targets
    /// timed out (otherwise `max_results` was reached, or nobody is left), so that
    /// `parts + timed_out` is the number of targets.
    fn close_gather(
        &self,
        pending: &mut HashMap<String, Pending>,
        id: &str,
        deadline: bool,
    ) -> Option<Closed> {
        if !matches!(pending.get(id), Some(Pending::Scatter(_))) {
            return None;
        }
        let Some(Pending::Scatter(mut gather)) = pending.remove(id) else {
            return None;
        };
        // Every outstanding target had no part: counted here, before the drain, because one
        // whose answer is between its two locks (taken out of the map, `on_part` not yet run) is
        // no longer in the map and its answer is dropped when it finds no gather.
        let timed_out = if deadline {
            gather.outstanding.len() as u32
        } else {
            0
        };
        let rest: Vec<(String, Unary)> = gather
            .outstanding
            .drain()
            .filter_map(|target| match pending.remove(&target) {
                Some(Pending::Unary(u)) => Some((target, u)),
                Some(other) => {
                    pending.insert(target, other);
                    None
                }
                None => None,
            })
            .collect();
        let end = ServerMessage {
            message: Some(server_message::Message::End(ReplyEnd {
                request_id: id.to_string(),
                error: None,
                parts: gather.parts,
                failed: gather.failed,
                timed_out,
            })),
        };
        gather.relay.deliver(end, &self.metrics);
        Some(Closed {
            gather,
            rest,
            timed_out,
        })
    }

    /// Bookkeeping of a closed gather: flow state of its unanswered targets (a forwarded one
    /// whose handler negotiated "streaming" is told to cancel), metrics and edge (one request;
    /// an error when no part succeeded).
    fn finish_scatter(&self, id: &str, closed: Closed) {
        let Closed {
            gather,
            rest,
            timed_out,
        } = closed;
        gather.cancel.abort();
        let reason = if timed_out > 0 {
            "deadline"
        } else {
            "max results"
        };
        for (target, entry) in &rest {
            self.release(target, entry);
            if entry.forwarded {
                self.cancel_target(&gather.context, target, entry, reason);
            }
        }
        let outcome = if gather.ok > 0 {
            Outcome::Ok
        } else if timed_out > 0 {
            Outcome::Timeout
        } else {
            gather.last_failure.unwrap_or(Outcome::HandlerError)
        };
        record_outcome(&gather.span, outcome);
        if outcome != Outcome::Ok {
            log_outcome(
                id,
                &gather.type_name,
                Some(gather.kind),
                &gather.sender,
                None,
                outcome,
                false,
            );
        }
        let elapsed = gather.started.elapsed();
        self.metrics.request_finished_mode(
            gather.kind,
            &gather.type_name,
            outcome,
            elapsed,
            mode_label(RequestMode::ScatterGather as i32),
        );
        self.record_edge(
            gather.sender_service.as_deref(),
            &gather.type_name,
            gather.kind,
            outcome,
            Some(elapsed),
        );
    }

    /// Answers a non-unary request before it was routed.
    fn reject_mode(&self, m: &ModeContext<'_>, kind: Kind, code: ErrorCode, message: String) {
        let outcome = outcome_of(code);
        let outcome = if code == ErrorCode::Unsupported {
            Outcome::Unsupported
        } else {
            outcome
        };
        self.record_rejected_mode(m.sender_service, kind, m.type_name, outcome, m.mode);
        record_outcome(m.span, outcome);
        log_outcome(
            m.id,
            m.type_name,
            Some(kind),
            m.sender,
            None,
            outcome,
            false,
        );
        self.send_to(m.ctx, m.sender, error_message(m.id, code, message));
    }

    // ------------------------------------------------------------------------------------
    // Streaming queries (ADR 0017, P4b)

    /// A query with mode STREAMING from a v2 sender: one capable instance gets the request with
    /// the credit, as the target `<id>#1` of a [`Pending::Stream`].
    fn route_stream(
        self: &Arc<Self>,
        ctx: &ContextId,
        sender: &str,
        req: Request,
        req_kind: Kind,
        info: SenderInfo,
        span: tracing::Span,
    ) {
        let id = req.id.clone();
        let type_name = req.r#type.clone();
        let mode = mode_label(req.mode);
        let m = ModeContext {
            ctx,
            sender,
            id: &id,
            type_name: &type_name,
            sender_service: Some(&info.service),
            mode,
            span: &span,
        };
        if !info.streaming {
            let message = format!(
                "request mode {mode} needs the \"{}\" capability, not declared by this session",
                protocol::STREAMING
            );
            return self.reject_mode(&m, req_kind, ErrorCode::Unsupported, message);
        }
        let for_queries = |type_name: &str| {
            format!("invalid request: {type_name} is a command; {mode} is for queries")
        };
        if req_kind == Kind::Command {
            let message = for_queries(&type_name);
            return self.reject_mode(&m, req_kind, ErrorCode::InvalidRequest, message);
        }
        let pool = match self.registry.stream_candidates(ctx, &type_name) {
            Ok(pool) => pool,
            Err(e) => {
                let message = no_handler_message(&type_name, e);
                return self.reject_mode(&m, req_kind, ErrorCode::NoHandler, message);
            }
        };
        let kind = pool.kind;
        if kind == Kind::Command {
            let message = for_queries(&type_name);
            return self.reject_mode(&m, kind, ErrorCode::InvalidRequest, message);
        }
        // Only an instance that negotiated "streaming" understands the multi-part messages.
        let (mut capable, incapable): (Vec<Candidate>, Vec<Candidate>) = pool
            .candidates
            .into_iter()
            .partition(|c| self.sessions.negotiated_streaming(ctx, &c.id));
        if capable.is_empty() {
            let mut services: Vec<String> = incapable
                .iter()
                .filter_map(|c| self.registry.service_of(ctx, &c.id))
                .collect();
            services.sort();
            services.dedup();
            let message = format!(
                "no live instance of {} serving {type_name} negotiated the \"{}\" capability",
                services.join(", "),
                protocol::STREAMING
            );
            return self.reject_mode(&m, kind, ErrorCode::Unsupported, message);
        }
        let sender_tags = self
            .registry
            .placement_of(ctx, sender)
            .map(|p| p.tags)
            .unwrap_or_default();
        let loads = self.loads(&capable);
        let i = choose(&capable, &sender_tags, pool.rotation, |id| loads[id]);
        let handler = capable.swap_remove(i).id;
        let handler_service = self.registry.service_of(ctx, &handler).unwrap_or_default();
        span.record("kraken.handler", handler.as_str());
        // The sender's own stream, weakly: the stream never keeps a closed stream open.
        let Some(out) = self
            .sessions
            .weak_sender_if_current(ctx, sender, info.token)
        else {
            return; // the sender left meanwhile: nobody to answer
        };
        // `None`: the handler's session vanished since the pick; the dispatch reports it Gone.
        let handler_token = self.sessions.token_of(ctx, &handler).unwrap_or(u64::MAX);
        let timeout = self.config.effective_timeout(&type_name, req.timeout_ms);
        let credit = if req.initial_credit == 0 {
            DEFAULT_STREAM_CREDIT
        } else {
            req.initial_credit
        };
        let target_id = format!("{id}#1");
        {
            let mut pending = self.pending.lock().unwrap();
            if pending.contains_key(&id) || pending.contains_key(&target_id) {
                drop(pending);
                let message = "invalid request: duplicate id".to_string();
                return self.reject_mode(&m, kind, ErrorCode::InvalidRequest, message);
            }
            self.metrics.request_started(kind);
            self.metrics.stream_opened(&handler_service, &handler);
            let now = Instant::now();
            let router = Arc::clone(self);
            let idle = tokio::spawn(idle_watch(router, id.clone(), timeout)).abort_handle();
            let router = Arc::clone(self);
            let task_id = id.clone();
            let max = self.config.max_stream_duration;
            let max_duration = tokio::spawn(async move {
                tokio::time::sleep(max).await;
                router.on_stream_max_duration(&task_id, max);
            })
            .abort_handle();
            let target = Unary {
                context: ctx.clone(),
                sender: sender.to_string(),
                sender_token: Some(info.token),
                sender_service: Some(info.service.clone()),
                handler: handler.clone(),
                handler_token,
                forwarded: false,
                sender_flow_control: info.flow_control,
                kind,
                type_name: type_name.clone(),
                started: now,
                cancel: None,
                span: tracing::Span::none(),
                part_of: Some(PartOf {
                    parent: id.clone(),
                    service: handler_service.clone(),
                }),
            };
            pending.insert(target_id.clone(), Pending::Unary(target));
            let stream = Stream {
                context: ctx.clone(),
                sender: sender.to_string(),
                sender_token: info.token,
                sender_service: info.service.clone(),
                handler: handler.clone(),
                handler_service,
                kind,
                type_name,
                started: now,
                target_id: target_id.clone(),
                credit,
                parts: 0,
                max_results: req.max_results,
                last_activity: now,
                idle,
                max_duration,
                span,
                relay: Relay::new(out, Some(&info.service), sender),
            };
            pending.insert(id, Pending::Stream(stream));
        }
        // The handler gets the request with its own id and the credit; the hub enforces
        // `max_results` itself.
        let forwarded = Request {
            id: target_id.clone(),
            initial_credit: credit,
            max_results: 0,
            ..req
        };
        self.dispatch(ctx, &target_id, &handler, handler_token, forwarded);
    }

    /// The request `id` was just written to `handler`'s session `token`: the entry is marked
    /// forwarded (its ending paths now owe the handler a `Cancel`), and a streaming target's
    /// idle timeout starts over, so the handler gets the whole timeout for its first part
    /// whatever the queue wait was. An entry that ended between the pre-write check and the
    /// write (a close that found it neither queued nor forwarded, so sent no `Cancel`) gets its
    /// `Cancel{id, "closed before forward"}` here, if the handler negotiated "streaming": it
    /// must not serve a request the hub forgot.
    fn note_forwarded(&self, ctx: &ContextId, id: &str, handler: &str, token: u64, service: &str) {
        let mut pending = self.pending.lock().unwrap();
        let parent = match pending.get_mut(id).and_then(Pending::unary_mut) {
            Some(u) if u.handler == handler && u.handler_token == token => {
                u.forwarded = true;
                u.part_of.as_ref().map(|p| p.parent.clone())
            }
            _ => {
                drop(pending);
                if self.sessions.negotiated_streaming(ctx, handler) {
                    let msg = ServerMessage {
                        message: Some(server_message::Message::Cancel(Cancel {
                            request_id: id.to_string(),
                            reason: "closed before forward".to_string(),
                        })),
                    };
                    self.send_to_handler(ctx, handler, token, service, msg);
                }
                return;
            }
        };
        if let Some(parent) = parent
            && let Some(Pending::Stream(stream)) = pending.get_mut(&parent)
        {
            stream.last_activity = Instant::now();
        }
    }

    /// The stream whose target `target_id` is, if the target is pending for `from` in `ctx`.
    fn stream_of_target(
        pending: &HashMap<String, Pending>,
        target_id: &str,
        from: &str,
        ctx: &ContextId,
        what: &str,
    ) -> Option<String> {
        let Some(target) = pending.get(target_id).and_then(Pending::unary) else {
            tracing::debug!(request_id = %target_id, from, "{what} for unknown request ignored");
            return None;
        };
        if target.handler != from || target.context != *ctx {
            tracing::debug!(request_id = %target_id, from, "{what} from non-handler ignored");
            return None;
        }
        let parent = target.part_of.as_ref().map(|p| p.parent.clone());
        match parent.filter(|p| matches!(pending.get(p), Some(Pending::Stream(_)))) {
            Some(parent) => Some(parent),
            None => {
                tracing::debug!(request_id = %target_id, from, "{what} for a request that is not streaming ignored");
                None
            }
        }
    }

    /// `ReplyPart` from a handler for a streaming target: relayed to the sender (its id, the
    /// responder, the handler's `seq`) under the sender's credits. A part beyond the credit, or
    /// one carrying an error, ends the stream; so does the `max_results`-th part.
    pub fn on_stream_part(&self, ctx: &ContextId, from: &str, part: ReplyPart) {
        let closed = {
            let mut pending = self.pending.lock().unwrap();
            let Some(id) = Self::stream_of_target(&pending, &part.request_id, from, ctx, "part")
            else {
                return;
            };
            let Some(Pending::Stream(stream)) = pending.get_mut(&id) else {
                return;
            };
            if let Some(mut error) = part.error {
                error.request_id = id.clone();
                let end = StreamEnd {
                    error: Some(error),
                    tell_sender: true,
                    cancel: None,
                    outcome: Outcome::HandlerError,
                    relayed: true,
                };
                self.close_stream(&mut pending, &id, end)
            } else if stream.credit == 0 {
                let end = StreamEnd {
                    error: Some(crate::proto::Error {
                        request_id: id.clone(),
                        code: ErrorCode::InvalidRequest as i32,
                        message: "part without credit".to_string(),
                        details: vec![],
                    }),
                    tell_sender: true,
                    cancel: Some("part without credit".to_string()),
                    outcome: Outcome::InvalidRequest,
                    relayed: false,
                };
                self.close_stream(&mut pending, &id, end)
            } else {
                stream.credit -= 1;
                stream.parts += 1;
                stream.last_activity = Instant::now();
                let relayed = ServerMessage {
                    message: Some(server_message::Message::Part(ReplyPart {
                        request_id: id.clone(),
                        payload: part.payload,
                        error: None,
                        responder_service: stream.handler_service.clone(),
                        responder_instance: stream.handler.clone(),
                        seq: part.seq,
                    })),
                };
                stream.relay.deliver(relayed, &self.metrics);
                self.metrics
                    .stream_part(&stream.handler_service, &stream.type_name);
                if stream.max_results > 0 && stream.parts >= stream.max_results {
                    let end = StreamEnd {
                        error: None,
                        tell_sender: true,
                        cancel: Some("max results".to_string()),
                        outcome: Outcome::Ok,
                        relayed: false,
                    };
                    self.close_stream(&mut pending, &id, end)
                } else {
                    None
                }
            }
        };
        if let Some(closed) = closed {
            self.finish_stream(closed);
        }
    }

    /// `ReplyEnd` from a handler for a streaming target: the stream ends, with the handler's
    /// error if it carries one.
    pub fn on_stream_end(&self, ctx: &ContextId, from: &str, end: ReplyEnd) {
        let closed = {
            let mut pending = self.pending.lock().unwrap();
            let Some(id) = Self::stream_of_target(&pending, &end.request_id, from, ctx, "end")
            else {
                return;
            };
            let error = end.error.map(|mut e| {
                e.request_id = id.clone();
                e
            });
            let relayed = error.is_some();
            let end = StreamEnd {
                outcome: if relayed {
                    Outcome::HandlerError
                } else {
                    Outcome::Ok
                },
                error,
                tell_sender: true,
                cancel: None,
                relayed,
            };
            self.close_stream(&mut pending, &id, end)
        };
        if let Some(closed) = closed {
            self.finish_stream(closed);
        }
    }

    /// `Flow{request_id, credits}` from the sender of stream `request_id`: the credit grows and
    /// the handler gets `Flow{<id>#1, credits}`. Ignored for an unknown id, from anyone but the
    /// sender, and when it grants nothing.
    pub fn on_flow(&self, ctx: &ContextId, from: &str, flow: FlowMsg) {
        let id = flow.request_id;
        if flow.credits == 0 {
            tracing::debug!(request_id = %id, from, "flow without credits ignored");
            return;
        }
        let (handler, token, service, target_id) = {
            let mut pending = self.pending.lock().unwrap();
            let Some(Pending::Stream(stream)) = pending
                .get_mut(&id)
                .filter(|p| p.sender() == from && p.context() == ctx)
            else {
                tracing::debug!(request_id = %id, from, "flow for unknown stream ignored");
                return;
            };
            if stream.credit == 0 {
                // The idle clock was paused while nothing could flow: it starts over now.
                stream.last_activity = Instant::now();
            }
            stream.credit = stream.credit.saturating_add(flow.credits);
            let target_id = stream.target_id.clone();
            let token = pending
                .get(&target_id)
                .and_then(Pending::unary)
                .map_or(u64::MAX, |t| t.handler_token);
            let Some(Pending::Stream(stream)) = pending.get(&id) else {
                return;
            };
            (
                stream.handler.clone(),
                token,
                stream.handler_service.clone(),
                target_id,
            )
        };
        let msg = ServerMessage {
            message: Some(server_message::Message::Flow(FlowMsg {
                request_id: target_id,
                credits: flow.credits,
            })),
        };
        self.send_to_handler(ctx, &handler, token, &service, msg);
    }

    /// `Cancel{request_id, reason}` from the sender of stream `request_id`: the handler gets
    /// `Cancel{<id>#1, reason}` (if it was forwarded the request), the stream ends without a
    /// `ReplyEnd`. Ignored for an unknown id and from anyone but the sender.
    pub fn on_cancel(&self, ctx: &ContextId, from: &str, cancel: Cancel) {
        let id = cancel.request_id;
        let closed = {
            let mut pending = self.pending.lock().unwrap();
            if !matches!(
                pending.get(&id),
                Some(p @ Pending::Stream(_)) if p.sender() == from && p.context() == ctx
            ) {
                tracing::debug!(request_id = %id, from, "cancel for unknown stream ignored");
                return;
            }
            let reason = if cancel.reason.is_empty() {
                "cancelled by the sender".to_string()
            } else {
                cancel.reason
            };
            let end = StreamEnd {
                error: None,
                tell_sender: false,
                cancel: Some(reason),
                outcome: Outcome::Cancelled,
                relayed: false,
            };
            self.close_stream(&mut pending, &id, end)
        };
        if let Some(closed) = closed {
            self.finish_stream(closed);
        }
    }

    /// The target of stream `id` ended without a part or an end of its own: a hub-side failure
    /// (`Err`) ends the stream with that error; a unary `Reply{payload}` is relayed as one part
    /// and the end.
    fn on_stream_target_result(
        &self,
        id: &str,
        result: Result<Vec<u8>, (crate::proto::Error, Outcome)>,
    ) {
        let closed = {
            let mut pending = self.pending.lock().unwrap();
            let Some(Pending::Stream(stream)) = pending.get_mut(id) else {
                return;
            };
            let end = match result {
                Ok(payload) => {
                    let seq = u64::from(stream.parts);
                    stream.parts += 1;
                    let part = ServerMessage {
                        message: Some(server_message::Message::Part(ReplyPart {
                            request_id: id.to_string(),
                            payload,
                            error: None,
                            responder_service: stream.handler_service.clone(),
                            responder_instance: stream.handler.clone(),
                            seq,
                        })),
                    };
                    stream.relay.deliver(part, &self.metrics);
                    self.metrics
                        .stream_part(&stream.handler_service, &stream.type_name);
                    StreamEnd {
                        error: None,
                        tell_sender: true,
                        cancel: None,
                        outcome: Outcome::Ok,
                        relayed: false,
                    }
                }
                Err((mut error, outcome)) => {
                    error.request_id = id.to_string();
                    StreamEnd {
                        error: Some(error),
                        tell_sender: true,
                        cancel: None,
                        outcome,
                        relayed: outcome == Outcome::HandlerError,
                    }
                }
            };
            self.close_stream(&mut pending, id, end)
        };
        if let Some(closed) = closed {
            self.finish_stream(closed);
        }
    }

    /// No part within the idle timeout of stream `id`.
    fn on_stream_idle(&self, id: &str, timeout: Duration) {
        let end = StreamEnd {
            error: Some(crate::proto::Error {
                request_id: id.to_string(),
                code: ErrorCode::Timeout as i32,
                message: format!("no part within {} ms", timeout.as_millis()),
                details: vec![],
            }),
            tell_sender: true,
            cancel: Some("idle timeout".to_string()),
            outcome: Outcome::Timeout,
            relayed: false,
        };
        self.end_stream(id, end);
    }

    /// Stream `id` outlived `KRAKEN_HUB_MAX_STREAM_DURATION`.
    fn on_stream_max_duration(&self, id: &str, max: Duration) {
        let message = format!(
            "max stream duration ({}) exceeded",
            crate::api::fmt_duration(max)
        );
        let end = StreamEnd {
            error: Some(crate::proto::Error {
                request_id: id.to_string(),
                code: ErrorCode::Cancelled as i32,
                message: message.clone(),
                details: vec![],
            }),
            tell_sender: true,
            cancel: Some(message),
            outcome: Outcome::Cancelled,
            relayed: false,
        };
        self.end_stream(id, end);
    }

    /// Ends stream `id` (if still pending) the way `end` says.
    fn end_stream(&self, id: &str, end: StreamEnd) {
        let closed = {
            let mut pending = self.pending.lock().unwrap();
            self.close_stream(&mut pending, id, end)
        };
        if let Some(closed) = closed {
            self.finish_stream(closed);
        }
    }

    /// Takes stream `id` and its target out of the map and, when due, sends the sender its
    /// `ReplyEnd` (under the pending lock, so it follows every part).
    fn close_stream(
        &self,
        pending: &mut HashMap<String, Pending>,
        id: &str,
        end: StreamEnd,
    ) -> Option<ClosedStream> {
        if !matches!(pending.get(id), Some(Pending::Stream(_))) {
            return None;
        }
        let Some(Pending::Stream(mut stream)) = pending.remove(id) else {
            return None;
        };
        let target = match pending.remove(&stream.target_id) {
            Some(Pending::Unary(u)) if u.part_of.as_ref().is_some_and(|p| p.parent == id) => {
                Some(u)
            }
            Some(other) => {
                pending.insert(stream.target_id.clone(), other);
                None
            }
            None => None,
        };
        if end.tell_sender {
            let msg = ServerMessage {
                message: Some(server_message::Message::End(ReplyEnd {
                    request_id: id.to_string(),
                    error: end.error.clone(),
                    parts: stream.parts,
                    failed: 0,
                    timed_out: 0,
                })),
            };
            stream.relay.deliver(msg, &self.metrics);
        }
        Some(ClosedStream {
            stream,
            target,
            end,
        })
    }

    /// Bookkeeping of a closed stream: timers, the target's flow state (and its `Cancel` when
    /// the handler still streams), metrics, edge, span and log line.
    fn finish_stream(&self, closed: ClosedStream) {
        let ClosedStream {
            stream,
            target,
            end,
        } = closed;
        stream.idle.abort();
        stream.max_duration.abort();
        if let Some(target) = &target {
            self.release(&stream.target_id, target);
            if let Some(reason) = &end.cancel
                && target.forwarded
            {
                self.cancel_target(&stream.context, &stream.target_id, target, reason);
            }
        }
        self.metrics
            .stream_closed(&stream.handler_service, &stream.handler);
        let id = stream.target_id.trim_end_matches("#1");
        record_outcome(&stream.span, end.outcome);
        if end.outcome != Outcome::Ok {
            log_outcome(
                id,
                &stream.type_name,
                Some(stream.kind),
                &stream.sender,
                Some(&stream.handler),
                end.outcome,
                end.relayed,
            );
        }
        let elapsed = stream.started.elapsed();
        self.metrics.request_finished_mode(
            stream.kind,
            &stream.type_name,
            end.outcome,
            elapsed,
            mode_label(RequestMode::Streaming as i32),
        );
        self.record_edge_error(
            Some(&stream.sender_service),
            &stream.type_name,
            stream.kind,
            end.error.is_some(),
            Some(elapsed),
        );
    }

    /// Tells the handler of target `target_id` (forwarded, not answered) to stop, if its session
    /// negotiated "streaming" (a scatter-gather target's handler may not have; a stream's always
    /// has).
    fn cancel_target(&self, ctx: &ContextId, target_id: &str, target: &Unary, reason: &str) {
        if !self.sessions.negotiated_streaming(ctx, &target.handler) {
            return;
        }
        let service = target
            .part_of
            .as_ref()
            .map(|p| p.service.as_str())
            .unwrap_or_default();
        let msg = ServerMessage {
            message: Some(server_message::Message::Cancel(Cancel {
                request_id: target_id.to_string(),
                reason: reason.to_string(),
            })),
        };
        self.send_to_handler(ctx, &target.handler, target.handler_token, service, msg);
    }

    /// Sends a `Flow` or `Cancel` to the handler session `token` of `handler`: `try_send`, and
    /// when its buffer is full a background delivery that waits up to
    /// [`TERMINAL_SEND_TIMEOUT`] for room (a lost `Flow` would stall the stream, a lost `Cancel`
    /// would leave the handler working for nobody).
    fn send_to_handler(
        &self,
        ctx: &ContextId,
        handler: &str,
        token: u64,
        service: &str,
        msg: ServerMessage,
    ) {
        let Some(tx) = self
            .sessions
            .weak_sender_if_current(ctx, handler, token)
            .and_then(|w| w.upgrade())
        else {
            tracing::debug!(
                instance = handler,
                "handler gone, dropping multi-part message"
            );
            return;
        };
        match tx.try_send(Ok(msg)) {
            Ok(()) | Err(TrySendError::Closed(_)) => {}
            Err(TrySendError::Full(msg)) => {
                let metrics = Arc::clone(&self.metrics);
                let service = service.to_string();
                let instance = handler.to_string();
                tokio::spawn(async move {
                    if let Err(e) = tx.send_timeout(msg, TERMINAL_SEND_TIMEOUT).await {
                        let reason = match e {
                            SendTimeoutError::Timeout(_) => "buffer still full",
                            SendTimeoutError::Closed(_) => "stream closed",
                        };
                        tracing::warn!(
                            instance,
                            service,
                            reason,
                            "multi-part message not delivered"
                        );
                        metrics.buffer_full(&service);
                    }
                });
            }
        }
    }

    /// Sends a terminal message (`Reply` or `Error`) to `instance`. `try_send` is the fast path;
    /// when the buffer is full the message is handed to a background delivery that waits up to
    /// [`TERMINAL_SEND_TIMEOUT`] for room, so a terminal message is never dropped just because
    /// the sender is momentarily slow. At most one such task exists per removed pending entry,
    /// so their number is bounded by the in-flight requests.
    fn send_to(&self, ctx: &ContextId, instance: &str, msg: ServerMessage) {
        let Some(tx) = self.sessions.sender(ctx, instance) else {
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
                    .service_of(ctx, instance)
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

/// What every rejection of a non-unary request needs: the metrics labels, the log line, the
/// span and the sender to answer.
struct ModeContext<'a> {
    ctx: &'a ContextId,
    sender: &'a str,
    id: &'a str,
    type_name: &'a str,
    sender_service: Option<&'a str>,
    mode: &'static str,
    span: &'a tracing::Span,
}

/// Ends stream `id` with TIMEOUT once `timeout` passed without activity (a forward, a relayed
/// part, or a `Flow` that raised an exhausted credit); one task per stream, re-armed from the
/// stream's `last_activity`. While the credit is 0 the clock is paused: the watch looks again
/// after `timeout` (`on_flow` restarts `last_activity` when the credit comes back).
async fn idle_watch(router: Arc<Router>, id: String, timeout: Duration) {
    let mut deadline = Instant::now() + timeout;
    loop {
        tokio::time::sleep_until(deadline).await;
        let next = {
            let pending = router.pending.lock().unwrap();
            match pending.get(&id) {
                Some(Pending::Stream(s)) if s.credit == 0 => None,
                Some(Pending::Stream(s)) => Some(s.last_activity + timeout),
                _ => return,
            }
        };
        let Some(next) = next else {
            deadline = Instant::now() + timeout;
            continue;
        };
        if next <= Instant::now() {
            router.on_stream_idle(&id, timeout);
            return;
        }
        deadline = next;
    }
}

/// Pops queued requests while the session has permits, one permit each.
fn drain(flow: &mut Flow) -> Vec<Queued> {
    let mut ready = Vec::new();
    while let Some(permits) = flow.permits.as_mut().filter(|p| **p > 0) {
        let Some(item) = flow.queue.pop() else { break };
        *permits -= 1;
        ready.push(item);
    }
    ready
}

#[cfg(test)]
mod tests {
    use super::*;

    static D: std::sync::LazyLock<ContextId> = std::sync::LazyLock::new(ContextId::default);
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tonic::Status;

    type Rx = mpsc::Receiver<Result<ServerMessage, Status>>;

    fn fake(sessions: &Sessions, id: &str, service: &str, cap: usize) -> Rx {
        let (tx, rx) = mpsc::channel(cap);
        sessions.insert(
            &D,
            id,
            service,
            crate::session::SessionMeta::v1(Duration::from_secs(30)),
            tx,
            CancellationToken::new(),
        );
        rx
    }

    struct Fixture {
        router: Arc<Router>,
        sessions: Arc<Sessions>,
        sender_rx: Rx,
        handler_rx: Rx,
    }

    /// Like [`fixture_with`], with the flow state of h1 and s1 opened (unlimited), as the stream
    /// task does before a session becomes current.
    fn fixture_with(config: Config, handler_cap: usize) -> Fixture {
        let f = fixture_unopened(config, handler_cap);
        for (id, service) in [("h1", "orders"), ("s1", "web")] {
            let token = f.sessions.token_of(&D, id).unwrap();
            let _ = f.router.open_session(&D, id, token, service, None);
        }
        f
    }

    impl Fixture {
        /// A new (unlimited) session for `id`, flow state first, as the stream task opens it.
        fn connect(&self, id: &str, service: &str, cap: usize) -> Rx {
            let rx = fake(&self.sessions, id, service, cap);
            let token = self.sessions.token_of(&D, id).unwrap();
            let _ = self.router.open_session(&D, id, token, service, None);
            rx
        }
    }

    /// h1 and s1 with sessions but no flow state yet.
    fn fixture_unopened(config: Config, handler_cap: usize) -> Fixture {
        let registry = Arc::new(Registry::new(config.owner_grace));
        let sessions = Arc::new(Sessions::new());
        registry
            .register(&D, "h1", "orders", &["t.Cmd".into()], &["t.Qry".into()])
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
            ..Request::default()
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

    /// The `(sender service, type)` edge of a request, from the router's statistics.
    fn edge_of(router: &Router, sender: &str, type_name: &str) -> crate::edges::EdgeWindow {
        let snap = router
            .edges()
            .expect("dashboard on")
            .snapshot(Instant::now());
        snap.edges
            .into_iter()
            .find(|e| e.sender == sender && e.type_name == type_name)
            .unwrap_or_else(|| panic!("no edge {sender} -> {type_name}"))
    }

    #[tokio::test(start_paused = true)]
    async fn terminal_requests_are_counted_on_their_sender_service_edge() {
        let mut f = fixture();
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        tokio::time::advance(Duration::from_millis(40)).await;
        f.router.on_reply(&D, "h1", payload_reply("r1"));
        let e = edge_of(&f.router, "web", "t.Cmd");
        assert_eq!((e.count, e.errors, e.kind), (1, 0, Kind::Command));
        let p50 = e.latency.quantile(0.5).unwrap().as_secs_f64() * 1000.0;
        assert!((p50 - 40.0).abs() <= 2.0, "{p50}");

        // A handler error counts as an error, with its latency.
        f.router.route(&D, "s1", request("r2", "t.Cmd", 0));
        f.router.on_reply(
            &D,
            "h1",
            Reply {
                request_id: "r2".into(),
                result: Some(reply::Result::Error(crate::proto::Error {
                    request_id: "r2".into(),
                    code: ErrorCode::HandlerError as i32,
                    message: "boom".into(),
                    details: vec![],
                })),
            },
        );
        let e = edge_of(&f.router, "web", "t.Cmd");
        assert_eq!((e.count, e.errors, e.latency.samples()), (2, 1, 2));

        // A timeout is an error with the deadline as its latency.
        f.router.route(&D, "s1", request("r3", "t.Cmd", 100));
        tokio::time::sleep(Duration::from_millis(101)).await;
        let e = edge_of(&f.router, "web", "t.Cmd");
        assert_eq!((e.count, e.errors, e.latency.samples()), (3, 2, 3));
        let max = e.latency.quantile(1.0).unwrap().as_secs_f64() * 1000.0;
        assert!((max - 100.0).abs() <= 5.0, "{max}");

        // An immediate NO_HANDLER is counted without a latency.
        f.router.route(&D, "s1", request("r4", "t.Nope", 0));
        let e = edge_of(&f.router, "web", "t.Nope");
        assert_eq!((e.count, e.errors, e.latency.samples()), (1, 1, 0));
        while f.sender_rx.try_recv().is_ok() {}
    }

    #[tokio::test]
    async fn no_edge_statistics_without_the_dashboard() {
        let mut f = fixture_with(
            Config {
                dashboard_enabled: false,
                ..Config::default()
            },
            16,
        );
        assert!(f.router.edges().is_none());
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        f.router.on_reply(&D, "h1", payload_reply("r1"));
        assert!(f.sender_rx.try_recv().is_ok());
    }

    #[tokio::test]
    async fn unknown_type_sends_no_handler_to_sender() {
        let mut f = fixture();
        f.router.route(&D, "s1", request("r1", "t.Nope", 0));
        let msg = expect_error(&mut f.sender_rx, "r1", ErrorCode::NoHandler);
        assert_eq!(msg, "no handler registered for type t.Nope");
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn forwards_request_unchanged_to_picked_handler() {
        let mut f = fixture();
        let req = request("r1", "t.Cmd", 0);
        f.router.route(&D, "s1", req.clone());
        let got = f.handler_rx.try_recv().unwrap().unwrap();
        assert_eq!(got.message, Some(server_message::Message::Request(req)));
        assert_eq!(f.router.in_flight(), 1);
    }

    #[tokio::test]
    async fn relays_reply_to_sender_and_clears_pending() {
        let mut f = fixture();
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        f.router.on_reply(&D, "h1", payload_reply("r1"));
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
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        f.router.on_reply(&D, "intruder", payload_reply("r1"));
        assert!(f.sender_rx.try_recv().is_err());
        assert_eq!(f.router.in_flight(), 1);
    }

    #[tokio::test]
    async fn unknown_reply_id_is_ignored() {
        let mut f = fixture();
        f.router.on_reply(&D, "h1", payload_reply("ghost"));
        assert!(f.sender_rx.try_recv().is_err());
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_sends_timeout_once_and_drops_late_reply() {
        let mut f = fixture();
        f.router.route(&D, "s1", request("r1", "t.Cmd", 10));
        tokio::time::sleep(Duration::from_millis(11)).await;
        let msg = expect_error(&mut f.sender_rx, "r1", ErrorCode::Timeout);
        assert_eq!(msg, "no reply within 10 ms");
        assert!(f.sender_rx.try_recv().is_err());
        f.router.on_reply(&D, "h1", payload_reply("r1"));
        assert!(f.sender_rx.try_recv().is_err());
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn zero_timeout_uses_default_and_large_is_clamped() {
        let mut f = fixture();
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        tokio::time::sleep(Duration::from_millis(4999)).await;
        assert!(f.sender_rx.try_recv().is_err());
        tokio::time::sleep(Duration::from_millis(2)).await;
        expect_error(&mut f.sender_rx, "r1", ErrorCode::Timeout);

        f.router.route(&D, "s1", request("r2", "t.Cmd", 999_999));
        tokio::time::sleep(Duration::from_millis(60_000 - 1)).await;
        assert!(f.sender_rx.try_recv().is_err());
        tokio::time::sleep(Duration::from_millis(2)).await;
        expect_error(&mut f.sender_rx, "r2", ErrorCode::Timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn per_type_override_replaces_default_and_clamp() {
        let config = Config {
            timeout_overrides: [("t.Cmd".to_string(), Duration::from_millis(90_000))].into(),
            ..Config::default()
        };
        let mut f = fixture_with(config, 16);
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        f.router.route(&D, "s1", request("r2", "t.Cmd", 999_999));
        tokio::time::sleep(Duration::from_millis(90_000 - 1)).await;
        assert!(f.sender_rx.try_recv().is_err(), "override not applied");
        tokio::time::sleep(Duration::from_millis(2)).await;
        let first = expect_error(&mut f.sender_rx, "r1", ErrorCode::Timeout);
        assert_eq!(first, "no reply within 90000 ms");
        expect_error(&mut f.sender_rx, "r2", ErrorCode::Timeout);
    }

    #[tokio::test]
    async fn duplicate_request_id_is_rejected_without_touching_pending() {
        let mut f = fixture();
        let mut s2_rx = fake(&f.sessions, "s2", "web", 16);
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        f.router.route(&D, "s2", request("r1", "t.Cmd", 0));
        let msg = expect_error(&mut s2_rx, "r1", ErrorCode::InvalidRequest);
        assert_eq!(msg, "invalid request: duplicate id");
        assert_eq!(f.router.in_flight(), 1);
        assert!(f.handler_rx.try_recv().is_ok());
        assert!(f.handler_rx.try_recv().is_err());
        f.router.on_reply(&D, "h1", payload_reply("r1"));
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
        f.router.route(&D, "s1", request("r1", "t.Cmd", 10));
        tokio::time::sleep(Duration::from_millis(9)).await;
        f.router.on_reply(&D, "h1", payload_reply("r1"));
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
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        let token = f.sessions.token_of(&D, "h1").unwrap();
        f.router.on_disconnect(&D, "h1", token);
        let msg = expect_error(&mut f.sender_rx, "r1", ErrorCode::HandlerDisconnected);
        assert_eq!(msg, "handler instance h1 disconnected");
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn ending_an_old_session_leaves_the_newer_session_of_the_same_id_alone() {
        let mut f = fixture();
        let old = f.sessions.token_of(&D, "h1").unwrap();
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        // h1 reconnects: a new session with the same instance id.
        let mut new_rx = f.connect("h1", "orders", 16);
        f.router.route(&D, "s1", request("r2", "t.Cmd", 0));
        assert!(new_rx.try_recv().is_ok(), "r2 goes to the new session");
        f.router.on_disconnect(&D, "h1", old);
        let msg = expect_error(&mut f.sender_rx, "r1", ErrorCode::HandlerDisconnected);
        assert_eq!(msg, "handler instance h1 disconnected");
        assert!(f.sender_rx.try_recv().is_err(), "r2 must stay pending");
        assert_eq!(f.router.in_flight(), 1);
        f.router.on_reply(&D, "h1", payload_reply("r2"));
        assert!(matches!(
            f.sender_rx.try_recv().unwrap().unwrap().message,
            Some(server_message::Message::Reply(_))
        ));
        drop(f.handler_rx);
    }

    #[tokio::test]
    async fn sender_disconnect_drops_its_pending_requests() {
        let mut f = fixture();
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        let token = f.sessions.token_of(&D, "s1").unwrap();
        f.router.on_disconnect(&D, "s1", token);
        assert_eq!(f.router.in_flight(), 0);
        f.router.on_reply(&D, "h1", payload_reply("r1"));
        assert!(f.sender_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn full_handler_buffer_yields_handler_error_busy() {
        let mut f = fixture_with(Config::default(), 1);
        f.sessions.send(&D, "h1", ServerMessage::default()).unwrap();
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        let msg = expect_error(&mut f.sender_rx, "r1", ErrorCode::HandlerError);
        assert_eq!(msg, "handler busy");
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn gone_handler_yields_handler_disconnected() {
        let mut f = fixture();
        drop(f.handler_rx);
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        expect_error(&mut f.sender_rx, "r1", ErrorCode::HandlerDisconnected);
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn empty_request_id_is_invalid_request() {
        let mut f = fixture();
        f.router.route(&D, "s1", request("", "t.Cmd", 0));
        let msg = expect_error(&mut f.sender_rx, "", ErrorCode::InvalidRequest);
        assert_eq!(msg, "invalid request: missing id");
        assert!(f.handler_rx.try_recv().is_err());
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn reply_to_busy_sender_is_delivered_once_there_is_room() {
        let f = fixture();
        let mut s2_rx = f.connect("s2", "web", 1);
        f.router.route(&D, "s2", request("r1", "t.Cmd", 0));
        // Fill s2's only slot so the reply's try_send hits a full buffer.
        f.sessions.send(&D, "s2", ServerMessage::default()).unwrap();
        f.router.on_reply(&D, "h1", payload_reply("r1"));
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
        let mut s2_rx = f.connect("s2", "web", 1);
        f.sessions.send(&D, "s2", ServerMessage::default()).unwrap();
        f.router.route(&D, "s2", request("r1", "t.Nope", 0));
        tokio::time::sleep(TERMINAL_SEND_TIMEOUT + Duration::from_millis(1)).await;
        assert_eq!(
            s2_rx.recv().await.unwrap().unwrap(),
            ServerMessage::default()
        );
        assert!(s2_rx.try_recv().is_err());
    }

    // ---- flow control (P1) ----

    struct FlowFx {
        router: Arc<Router>,
        sender_rx: Rx,
        handler_rx: Rx,
        handler_token: u64,
    }

    fn meta(flow_control: bool) -> crate::session::SessionMeta {
        crate::session::SessionMeta {
            protocol_version: 2,
            flow_control,
            ..crate::session::SessionMeta::v1(Duration::from_secs(10))
        }
    }

    /// h1 (service "orders", t.Cmd and t.Qry) with `permits` (None = unlimited); sender s1 shares
    /// flow-control when `sender_flow`.
    fn flow_fixture(config: Config, permits: Option<u32>, sender_flow: bool) -> FlowFx {
        let registry = Arc::new(Registry::new(config.owner_grace));
        let sessions = Arc::new(Sessions::new());
        registry
            .register(&D, "h1", "orders", &["t.Cmd".into()], &["t.Qry".into()])
            .unwrap();
        let (tx, handler_rx) = mpsc::channel(64);
        let handler_token = sessions.insert(
            &D,
            "h1",
            "orders",
            meta(permits.is_some()),
            tx,
            CancellationToken::new(),
        );
        let (tx, sender_rx) = mpsc::channel(64);
        sessions.insert(
            &D,
            "s1",
            "web",
            meta(sender_flow),
            tx,
            CancellationToken::new(),
        );
        let router = Router::new(
            registry,
            sessions.clone(),
            Arc::new(config),
            Metrics::noop(),
        );
        router.open_session(&D, "h1", handler_token, "orders", permits);
        FlowFx {
            router,
            sender_rx,
            handler_rx,
            handler_token,
        }
    }

    fn unkeyed(id: &str, priority: i32, timeout_ms: u32) -> Request {
        Request {
            routing_key: String::new(),
            priority,
            ..request(id, "t.Cmd", timeout_ms)
        }
    }

    /// Ids of the requests forwarded to `rx` so far.
    fn forwarded(rx: &mut Rx) -> Vec<String> {
        let mut out = Vec::new();
        while let Ok(Ok(m)) = rx.try_recv() {
            if let Some(server_message::Message::Request(r)) = m.message {
                out.push(r.id);
            }
        }
        out
    }

    #[tokio::test]
    async fn permits_limit_forwarding_and_permits_drain_the_queue() {
        let mut f = flow_fixture(Config::default(), Some(1), true);
        for id in ["r1", "r2", "r3"] {
            f.router.route(&D, "s1", unkeyed(id, 0, 0));
        }
        assert_eq!(forwarded(&mut f.handler_rx), ["r1"]);
        assert_eq!(f.router.queued_total(), 2);
        f.router.on_permits(&D, "h1", f.handler_token, 1, 0);
        assert_eq!(forwarded(&mut f.handler_rx), ["r2"]);
        f.router.on_permits(&D, "h1", f.handler_token, 1, 0);
        assert_eq!(forwarded(&mut f.handler_rx), ["r3"]);
        assert_eq!(f.router.queued_total(), 0);
        f.router.on_permits(&D, "h1", f.handler_token, 1, 0);
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!(
            view,
            FlowView {
                flow_controlled: true,
                permits_available: 1,
                queued: 0
            }
        );
        assert!(f.sender_rx.try_recv().is_err(), "nothing failed");
    }

    #[tokio::test]
    async fn queue_orders_by_priority_then_age() {
        let mut f = flow_fixture(Config::default(), Some(1), true);
        f.router.route(&D, "s1", unkeyed("first", 0, 0));
        for (id, p) in [
            ("low-1", 0),
            ("high-1", 5),
            ("low-2", 0),
            ("high-2", 5),
            ("neg", -3),
        ] {
            f.router.route(&D, "s1", unkeyed(id, p, 0));
        }
        assert_eq!(forwarded(&mut f.handler_rx), ["first"]);
        let mut order = Vec::new();
        for _ in 0..5 {
            f.router.on_permits(&D, "h1", f.handler_token, 1, 0);
            order.extend(forwarded(&mut f.handler_rx));
        }
        assert_eq!(order, ["high-1", "high-2", "low-1", "low-2", "neg"]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_queued_request_keeps_its_own_deadline() {
        let mut f = flow_fixture(Config::default(), Some(1), true);
        f.router.route(&D, "s1", unkeyed("busy", 0, 10_000));
        f.router.route(&D, "s1", unkeyed("waits", 0, 100));
        assert_eq!(forwarded(&mut f.handler_rx), ["busy"]);
        tokio::time::sleep(Duration::from_millis(101)).await;
        let msg = expect_error(&mut f.sender_rx, "waits", ErrorCode::Timeout);
        assert_eq!(msg, "no reply within 100 ms");
        assert_eq!(
            f.router.queued_total(),
            0,
            "the timed-out request left the queue"
        );
        f.router.on_permits(&D, "h1", f.handler_token, 1, 0);
        assert!(forwarded(&mut f.handler_rx).is_empty());
    }

    #[tokio::test]
    async fn a_full_queue_answers_resource_exhausted_to_a_flow_control_sender() {
        let config = Config {
            queue_capacity_per_instance: 1,
            ..Config::default()
        };
        let mut f = flow_fixture(config, Some(1), true);
        for id in ["r1", "r2", "r3"] {
            f.router.route(&D, "s1", unkeyed(id, 0, 0));
        }
        let msg = expect_error(&mut f.sender_rx, "r3", ErrorCode::ResourceExhausted);
        assert!(msg.contains("queue full"), "{msg}");
        assert_eq!(f.router.in_flight(), 2);
    }

    #[tokio::test]
    async fn a_full_queue_answers_handler_busy_to_a_sender_without_flow_control() {
        let config = Config {
            queue_capacity_per_instance: 1,
            ..Config::default()
        };
        let mut f = flow_fixture(config, Some(1), false);
        for id in ["r1", "r2", "r3"] {
            f.router.route(&D, "s1", unkeyed(id, 0, 0));
        }
        let msg = expect_error(&mut f.sender_rx, "r3", ErrorCode::HandlerError);
        assert_eq!(msg, "handler busy");
    }

    #[tokio::test]
    async fn the_byte_cap_rejects_a_request_that_does_not_fit() {
        let config = Config {
            queue_bytes_per_instance: 200,
            ..Config::default()
        };
        let mut f = flow_fixture(config, Some(1), true);
        f.router.route(&D, "s1", unkeyed("r1", 0, 0));
        f.router.route(&D, "s1", unkeyed("small", 0, 0));
        let mut big = unkeyed("big", 0, 0);
        big.payload = vec![b'x'; 300];
        f.router.route(&D, "s1", big);
        let msg = expect_error(&mut f.sender_rx, "big", ErrorCode::ResourceExhausted);
        assert!(msg.contains("queue full"), "{msg}");
        assert_eq!(f.router.queued_total(), 1);
    }

    /// The caps bound what waits; a request that can go at once is never measured against them.
    #[tokio::test]
    async fn an_idle_instance_with_a_permit_takes_a_request_larger_than_its_queue_cap() {
        let config = Config {
            queue_bytes_per_instance: 200,
            ..Config::default()
        };
        let mut f = flow_fixture(config, Some(1), true);
        let mut big = unkeyed("big", 0, 0);
        big.payload = vec![b'x'; 300];
        f.router.route(&D, "s1", big);
        assert_eq!(forwarded(&mut f.handler_rx), ["big"]);
        assert!(f.sender_rx.try_recv().is_err(), "not rejected");
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!((view.permits_available, view.queued), (0, 0));
    }

    #[tokio::test]
    async fn an_unlimited_instance_gets_everything_at_once() {
        let mut f = flow_fixture(Config::default(), None, true);
        for i in 0..20 {
            f.router.route(&D, "s1", unkeyed(&format!("r{i}"), 0, 0));
        }
        assert_eq!(forwarded(&mut f.handler_rx).len(), 20);
        // Permits for a session without flow control change nothing.
        f.router.on_permits(&D, "h1", f.handler_token, 3, 0);
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!(
            view,
            FlowView {
                flow_controlled: false,
                permits_available: 0,
                queued: 0
            }
        );
    }

    /// Service "orders" (t.Cmd, t.Qry) with handler instances added by [`Cluster::join`]; sender
    /// s1 ("web") shares flow-control.
    struct Cluster {
        registry: Arc<Registry>,
        sessions: Arc<Sessions>,
        router: Arc<Router>,
        sender_rx: Rx,
        handlers: HashMap<&'static str, (Rx, u64)>,
    }

    impl Cluster {
        fn new() -> Self {
            Self::with_metrics(Metrics::noop())
        }

        fn with_metrics(metrics: Arc<Metrics>) -> Self {
            Self::with(Config::default(), metrics)
        }

        fn with(config: Config, metrics: Arc<Metrics>) -> Self {
            let registry = Arc::new(Registry::new(config.owner_grace));
            let sessions = Arc::new(Sessions::new());
            let router = Router::new(
                Arc::clone(&registry),
                Arc::clone(&sessions),
                Arc::new(config),
                metrics,
            );
            let (tx, sender_rx) = mpsc::channel(64);
            let token = sessions.insert(&D, "s1", "web", meta(true), tx, CancellationToken::new());
            let _ = router.open_session(&D, "s1", token, "web", None);
            Cluster {
                registry,
                sessions,
                router,
                sender_rx,
                handlers: HashMap::new(),
            }
        }

        /// A handler instance connects like the stream task does: flow, session, registration.
        fn join(&mut self, id: &'static str, permits: Option<u32>) {
            let token = self.sessions.allocate_token();
            assert!(self.router.open_session(&D, id, token, "orders", permits));
            let (tx, rx) = mpsc::channel(64);
            let replaced = self
                .sessions
                .insert_with_token(
                    &D,
                    id,
                    "orders",
                    meta(permits.is_some()),
                    tx,
                    CancellationToken::new(),
                    token,
                )
                .unwrap();
            if let Some(old) = replaced {
                self.router.on_disconnect(&D, id, old);
            }
            self.registry
                .register(&D, id, "orders", &["t.Cmd".into()], &["t.Qry".into()])
                .unwrap();
            self.router.start_session(&D, id, token);
            self.handlers.insert(id, (rx, token));
        }

        /// The instance's stream ends (the disconnect sequence of the stream task).
        fn leave(&self, id: &str) {
            let token = self.handlers[id].1;
            crate::service::disconnect(&self.registry, &self.sessions, &self.router, &D, id, token);
        }

        fn forwarded(&mut self, id: &str) -> Vec<String> {
            forwarded(&mut self.handlers.get_mut(id).unwrap().0)
        }

        fn permit(&self, id: &str) {
            self.router.on_permits(&D, id, self.handlers[id].1, 1, 0);
        }

        /// Errors the sender got so far, sorted by request id.
        fn errors(&mut self) -> Vec<(String, ErrorCode)> {
            let mut out = Vec::new();
            while let Ok(Ok(m)) = self.sender_rx.try_recv() {
                if let Some(server_message::Message::Error(e)) = m.message {
                    out.push((e.request_id.clone(), e.code()));
                }
            }
            out.sort();
            out
        }
    }

    #[tokio::test]
    async fn queued_requests_of_a_departed_instance_go_to_another_one() {
        let mut c = Cluster::new();
        c.join("h1", Some(1));
        c.router.route(&D, "s1", unkeyed("sent", 0, 0));
        c.router.route(&D, "s1", request("q-keyed", "t.Cmd", 0));
        c.router.route(&D, "s1", unkeyed("q-unkeyed", 0, 0));
        assert_eq!(c.forwarded("h1"), ["sent"]);
        c.join("h2", Some(1));
        c.leave("h1");
        assert_eq!(
            c.errors(),
            [("sent".to_string(), ErrorCode::HandlerDisconnected)],
            "only the forwarded request's outcome is unknown"
        );
        assert_eq!(c.forwarded("h2"), ["q-keyed"], "same id, new ring owner");
        c.permit("h2");
        assert_eq!(c.forwarded("h2"), ["q-unkeyed"]);
        assert_eq!(c.router.in_flight(), 2);
    }

    #[tokio::test]
    async fn queued_requests_without_another_instance_end_with_no_handler() {
        let mut c = Cluster::new();
        c.join("h1", Some(1));
        c.router.route(&D, "s1", unkeyed("sent", 0, 0));
        c.router.route(&D, "s1", unkeyed("queued", 0, 0));
        c.leave("h1");
        assert_eq!(
            c.errors(),
            [
                ("queued".to_string(), ErrorCode::NoHandler),
                ("sent".to_string(), ErrorCode::HandlerDisconnected)
            ]
        );
        assert_eq!(c.router.queued_total(), 0);
        assert!(c.router.flow_by_instance().keys().all(|id| id != "h1"));
    }

    #[tokio::test]
    async fn a_replacement_takes_over_the_queue_of_the_session_it_replaced() {
        let mut c = Cluster::new();
        c.join("h1", Some(1));
        c.router.route(&D, "s1", unkeyed("sent", 0, 0));
        c.router.route(&D, "s1", unkeyed("q1", 0, 0));
        c.router.route(&D, "s1", unkeyed("q2", 0, 0));
        assert_eq!(c.forwarded("h1"), ["sent"]);
        c.join("h1", Some(1)); // reconnect: a new stream with the same instance id
        assert_eq!(
            c.errors(),
            [("sent".to_string(), ErrorCode::HandlerDisconnected)]
        );
        assert_eq!(c.forwarded("h1"), ["q1"], "the new stream's permit");
        c.permit("h1");
        assert_eq!(c.forwarded("h1"), ["q2"]);
    }

    #[tokio::test]
    async fn a_stopping_instance_hands_its_queue_over_and_draws_nothing_new() {
        let mut c = Cluster::new();
        c.join("h1", Some(1));
        c.router.route(&D, "s1", unkeyed("sent", 0, 0));
        c.router.route(&D, "s1", unkeyed("queued", 0, 0));
        c.join("h2", Some(1));
        // h1 stops gracefully: an empty Register, then it finishes what it has.
        let h1 = c.handlers["h1"].1;
        c.registry.register(&D, "h1", "orders", &[], &[]).unwrap();
        c.router.on_stopping(&D, "h1", h1);
        assert_eq!(c.forwarded("h2"), ["queued"]);
        c.router.on_reply(&D, "h1", payload_reply("sent"));
        c.permit("h1");
        for i in 0..4 {
            c.router.route(&D, "s1", unkeyed(&format!("new-{i}"), 0, 0));
        }
        assert_eq!(c.forwarded("h1"), ["sent"], "nothing new for h1");
        assert!(c.errors().is_empty());
    }

    #[tokio::test]
    async fn permits_of_a_stale_token_are_ignored() {
        let mut f = flow_fixture(Config::default(), Some(1), true);
        f.router.route(&D, "s1", unkeyed("r1", 0, 0));
        f.router.route(&D, "s1", unkeyed("r2", 0, 0));
        f.router.on_permits(&D, "h1", f.handler_token + 1000, 5, 0);
        assert_eq!(forwarded(&mut f.handler_rx), ["r1"]);
    }

    // ---- flow state and session lifecycle (review fixes) ----

    /// The old replacement window: the session is current but its flow state is not open yet.
    #[tokio::test]
    async fn a_session_without_flow_state_is_never_sent_an_unmetered_request() {
        let mut f = fixture_unopened(Config::default(), 16);
        f.router.route(&D, "s1", request("r1", "t.Cmd", 0));
        expect_error(&mut f.sender_rx, "r1", ErrorCode::HandlerDisconnected);
        assert!(f.handler_rx.try_recv().is_err(), "nothing forwarded");
        assert_eq!(f.router.in_flight(), 0);
    }

    /// A request routed while its handler disconnects must not land in the dying flow.
    #[tokio::test]
    async fn a_request_routed_after_the_flow_closed_fails_at_once() {
        let mut f = flow_fixture(Config::default(), Some(1), true);
        f.router.on_disconnect(&D, "h1", f.handler_token);
        f.router.route(&D, "s1", unkeyed("late", 0, 0));
        expect_error(&mut f.sender_rx, "late", ErrorCode::HandlerDisconnected);
        assert!(forwarded(&mut f.handler_rx).is_empty());
        assert_eq!(f.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn an_older_handshake_never_replaces_a_newer_flow() {
        let f = flow_fixture(Config::default(), Some(3), true);
        let _ = f.router.open_session(&D, "h1", 1_000, "orders", Some(1));
        // A concurrent, older handshake of the same instance id opens its flow late.
        let _ = f.router.open_session(&D, "h1", 999, "orders", Some(7));
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!(view.permits_available, 1, "the newer flow stays");
    }

    #[tokio::test]
    async fn permits_never_exceed_the_initial_grant() {
        let f = flow_fixture(Config::default(), Some(2), true);
        f.router.on_permits(&D, "h1", f.handler_token, 5, 0);
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!(view.permits_available, 2);
    }

    /// A grant (multi-hub top-up) raises the cap first, so the added permits are kept.
    #[tokio::test]
    async fn a_grant_raises_the_cap_and_the_permits() {
        let f = flow_fixture(Config::default(), Some(4), true);
        f.router.on_permits(&D, "h1", f.handler_token, 4, 8);
        let permits = |f: &FlowFx| {
            f.router
                .flow_by_instance()
                .remove("h1")
                .unwrap()
                .permits_available
        };
        assert_eq!(permits(&f), 8);
        // The raised cap is the new invariant: further permits stay within it.
        f.router.on_permits(&D, "h1", f.handler_token, 5, 0);
        assert_eq!(permits(&f), 8);
    }

    #[tokio::test]
    async fn a_grant_below_the_cap_changes_nothing() {
        let mut f = flow_fixture(Config::default(), Some(4), true);
        for id in ["r1", "r2"] {
            f.router.route(&D, "s1", unkeyed(id, 0, 0));
        }
        assert_eq!(forwarded(&mut f.handler_rx), ["r1", "r2"]);
        // A grant of 2 never lowers the cap of 4: the two permits come back to 4, not 2.
        f.router.on_permits(&D, "h1", f.handler_token, 2, 2);
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!(view.permits_available, 4);
        f.router.on_permits(&D, "h1", f.handler_token, 3, 3);
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!(view.permits_available, 4, "cap still 4");
    }

    #[tokio::test]
    async fn a_grant_is_ignored_for_an_unlimited_session() {
        let f = flow_fixture(Config::default(), None, true);
        f.router.on_permits(&D, "h1", f.handler_token, 3, 8);
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!(
            view,
            FlowView {
                flow_controlled: false,
                permits_available: 0,
                queued: 0
            }
        );
    }

    /// The raised cap is what the hub forwards up to: grant 3 with count 2 on top of the one
    /// initial permit lets three requests through at once.
    #[tokio::test]
    async fn a_raised_cap_lets_that_many_requests_through() {
        let mut f = flow_fixture(Config::default(), Some(1), true);
        f.router.on_permits(&D, "h1", f.handler_token, 2, 3);
        for id in ["r1", "r2", "r3", "r4"] {
            f.router.route(&D, "s1", unkeyed(id, 0, 0));
        }
        assert_eq!(forwarded(&mut f.handler_rx), ["r1", "r2", "r3"]);
        assert_eq!(f.router.queued_total(), 1);
    }

    fn assigned_to(router: &Router, id: &str) -> u32 {
        router.loads(&[candidate(id, 100, &[])])[id].assigned
    }

    fn queued_item(id: &str) -> Queued {
        Queued {
            id: id.to_string(),
            type_name: "t.Cmd".into(),
            priority: 0,
            bytes: 10,
            enqueued_at: Instant::now().into_std(),
            msg: ServerMessage {
                message: Some(server_message::Message::Request(unkeyed(id, 0, 0))),
            },
        }
    }

    /// The request ended (its timeout fired) between the pending insert and the dispatch.
    #[tokio::test]
    async fn a_request_that_ended_before_its_dispatch_leaves_no_queue_entry_or_assignment() {
        let mut f = flow_fixture(Config::default(), Some(1), true);
        f.router.route(&D, "s1", unkeyed("busy", 0, 0));
        f.router
            .dispatch(&D, "ghost", "h1", f.handler_token, unkeyed("ghost", 0, 0));
        assert_eq!(f.router.queued_total(), 0, "no orphan queue entry");
        f.router.route(&D, "s1", unkeyed("real", 0, 0));
        f.router.on_permits(&D, "h1", f.handler_token, 1, 0);
        assert_eq!(forwarded(&mut f.handler_rx), ["busy", "real"]);
        assert_eq!(assigned_to(&f.router, "h1"), 2, "busy and real only");
    }

    /// An entry still queued after its request ended (a race with the timeout, or a queue
    /// moved to a new session) is dropped when it comes up, and its permit stays.
    #[tokio::test]
    async fn a_dequeued_entry_whose_request_ended_is_not_sent() {
        let mut f = flow_fixture(Config::default(), Some(1), true);
        f.router.route(&D, "s1", unkeyed("busy", 0, 0));
        assert_eq!(forwarded(&mut f.handler_rx), ["busy"]);
        f.router
            .flows
            .lock()
            .unwrap()
            .get_mut("h1")
            .unwrap()
            .queue
            .push(queued_item("ghost"))
            .unwrap();
        f.router.on_permits(&D, "h1", f.handler_token, 1, 0);
        assert!(forwarded(&mut f.handler_rx).is_empty(), "ghost not sent");
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!((view.permits_available, view.queued), (1, 0));
    }

    /// The flow gauges follow the queues even when no further request comes: they are
    /// refreshed by the watchdog's sweep (`refresh_gauges`), not per request.
    #[tokio::test(start_paused = true)]
    async fn flow_gauges_are_right_after_queued_timeouts_departures_and_permits() {
        let m = crate::telemetry::test_support::test_metrics();
        let mut c = Cluster::with_metrics(Arc::clone(&m.metrics));
        c.join("h1", Some(1));
        let depth = |m: &crate::telemetry::test_support::TestMetrics| {
            (
                m.gauge("kraken_hub_queue_depth", "orders"),
                m.gauge("kraken_hub_permits_available", "orders"),
            )
        };
        c.router.route(&D, "s1", unkeyed("busy", 0, 10_000));
        c.router.route(&D, "s1", unkeyed("waits", 0, 100));
        c.router.refresh_gauges();
        assert_eq!(depth(&m), (Some(1), Some(0)));

        tokio::time::sleep(Duration::from_millis(101)).await; // "waits" times out in the queue
        c.router.refresh_gauges();
        assert_eq!(depth(&m), (Some(0), Some(0)), "after a queued timeout");

        // A sender leaves with a request still queued.
        let (tx, _rx2) = mpsc::channel(64);
        let s2 = c
            .sessions
            .insert(&D, "s2", "web", meta(true), tx, CancellationToken::new());
        let _ = c.router.open_session(&D, "s2", s2, "web", None);
        c.router.route(&D, "s2", unkeyed("orphan", 0, 10_000));
        c.router.refresh_gauges();
        assert_eq!(depth(&m), (Some(1), Some(0)));
        c.router.on_disconnect(&D, "s2", s2);
        c.router.refresh_gauges();
        assert_eq!(depth(&m), (Some(0), Some(0)), "after a sender departure");

        c.router.on_reply(&D, "h1", payload_reply("busy"));
        c.permit("h1");
        c.router.refresh_gauges();
        assert_eq!(depth(&m), (Some(0), Some(1)), "after a returned permit");

        c.leave("h1");
        c.router.refresh_gauges();
        assert_eq!(
            depth(&m),
            (Some(0), Some(0)),
            "a service without flows reads 0"
        );
    }

    #[tokio::test]
    async fn the_total_queue_cap_rejects_across_instances_with_its_own_reason() {
        let m = crate::telemetry::test_support::test_metrics();
        let big = |id: &str| {
            let mut r = unkeyed(id, 0, 0);
            r.payload = vec![b'x'; 100];
            r
        };
        let size = prost::Message::encoded_len(&ServerMessage {
            message: Some(server_message::Message::Request(big("q1"))),
        });
        // Room for one queued request, not two.
        let config = Config {
            queue_bytes_total: size * 3 / 2,
            ..Config::default()
        };
        let mut c = Cluster::with(config, Arc::clone(&m.metrics));
        c.join("h1", Some(1));
        c.join("h2", Some(1));
        for id in ["busy-1", "busy-2", "q1", "q2"] {
            c.router.route(&D, "s1", big(id));
        }
        // The busy ones take both permits; q1 waits on one instance; q2 goes to the other,
        // whose own queue is empty, but the hub-wide cap is reached.
        assert_eq!(
            c.errors(),
            [("q2".to_string(), ErrorCode::ResourceExhausted)]
        );
        assert_eq!(c.router.queued_total(), 1);
        assert_eq!(
            m.counter("kraken_hub_rejected_total", "reason", "queue_bytes_total"),
            Some(1)
        );
    }

    fn candidate(id: &str, load_factor: u32, tags: &[(&str, &str)]) -> Candidate {
        Candidate {
            id: id.to_string(),
            load_factor,
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    fn tags(t: &[(&str, &str)]) -> BTreeMap<String, String> {
        t.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn choose_prefers_the_best_tag_match() {
        let cs = [
            candidate("a", 100, &[("zone", "z1")]),
            candidate("b", 100, &[("zone", "z2"), ("disk", "ssd")]),
            candidate("c", 100, &[("zone", "z2")]),
        ];
        let idle = |_: &str| Load {
            can_take: true,
            assigned: 0,
        };
        let sender = tags(&[("zone", "z2"), ("disk", "ssd")]);
        assert_eq!(choose(&cs, &sender, 0, idle), 1);
        // A heavily loaded best match still wins over a lighter worse match.
        let loads = |id: &str| Load {
            can_take: true,
            assigned: if id == "b" { 50 } else { 0 },
        };
        assert_eq!(choose(&cs, &sender, 0, loads), 1);
        // No sender tags: every candidate scores 0, the load decides.
        assert_eq!(choose(&cs, &BTreeMap::new(), 0, loads), 0);
    }

    #[test]
    fn choose_prefers_instances_that_can_take_a_request_now_then_the_least_loaded() {
        let cs = [
            candidate("a", 100, &[]),
            candidate("b", 200, &[]),
            candidate("c", 100, &[]),
        ];
        let none = BTreeMap::new();
        // a: 2 / 100, b: 3 / 200 = 1.5 / 100, c: 1 / 100 but out of permits.
        let load = |id: &str| match id {
            "a" => Load {
                can_take: true,
                assigned: 2,
            },
            "b" => Load {
                can_take: true,
                assigned: 3,
            },
            _ => Load {
                can_take: false,
                assigned: 1,
            },
        };
        assert_eq!(choose(&cs, &none, 0, load), 1);
        // Nobody can take one now: the least loaded queue wins (c).
        let all_full = |id: &str| Load {
            can_take: false,
            ..load(id)
        };
        assert_eq!(choose(&cs, &none, 0, all_full), 2);
    }

    #[test]
    fn choose_breaks_ties_by_rotation() {
        let cs = [
            candidate("a", 100, &[]),
            candidate("b", 100, &[]),
            candidate("c", 100, &[]),
        ];
        let idle = |_: &str| Load {
            can_take: true,
            assigned: 0,
        };
        let picks: Vec<usize> = (0..6)
            .map(|r| choose(&cs, &BTreeMap::new(), r, idle))
            .collect();
        assert_eq!(picks, [0, 1, 2, 0, 1, 2]);
    }

    #[tokio::test]
    async fn unkeyed_requests_go_to_the_least_loaded_instance() {
        let registry = Arc::new(Registry::new(Duration::from_secs(60)));
        let sessions = Arc::new(Sessions::new());
        let mut rxs = HashMap::new();
        for id in ["h1", "h2"] {
            registry
                .register(&D, id, "orders", &["t.Cmd".into()], &[])
                .unwrap();
            let (tx, rx) = mpsc::channel(64);
            let token =
                sessions.insert(&D, id, "orders", meta(false), tx, CancellationToken::new());
            rxs.insert(id, (rx, token));
        }
        let (tx, _sender_rx) = mpsc::channel(64);
        sessions.insert(&D, "s1", "web", meta(false), tx, CancellationToken::new());
        let router = Router::new(
            registry,
            sessions,
            Arc::new(Config::default()),
            Metrics::noop(),
        );
        for (id, (_, token)) in &rxs {
            router.open_session(&D, id, *token, "orders", None);
        }
        // Six requests nobody answers: round-robin while equal, so three each.
        for i in 0..6 {
            router.route(&D, "s1", unkeyed(&format!("r{i}"), 0, 0));
        }
        let (rx1, _) = rxs.get_mut("h1").unwrap();
        let h1 = forwarded(rx1);
        assert_eq!(h1.len(), 3, "{h1:?}");
        // h1 answers all of its requests: the next ones go to h1 until the loads are equal again.
        for id in &h1 {
            router.on_reply(&D, "h1", payload_reply(id));
        }
        for i in 6..9 {
            router.route(&D, "s1", unkeyed(&format!("r{i}"), 0, 0));
        }
        let (rx1, _) = rxs.get_mut("h1").unwrap();
        assert_eq!(forwarded(rx1).len(), 3);
    }

    // ------------------------------------------------------------------------------------
    // Scatter-gather (ADR 0017)

    use crate::proto::{ReplyEnd, ReplyPart, RequestMode};
    use crate::registry::RegisteredTypes;

    fn scatter_meta(protocol_version: u32, scatter: bool) -> crate::session::SessionMeta {
        crate::session::SessionMeta {
            protocol_version,
            scatter_gather: scatter,
            ..crate::session::SessionMeta::v1(Duration::from_secs(10))
        }
    }

    /// Owners A (a1) and B (b1, b2) of the shared query "q.Find"; X (x1) owns command "c.Do" and
    /// exclusive query "q.Own"; sender s1 ("web", v2, scatter-gather), s-v1 (v1) and s-plain
    /// (v2 without the capability).
    struct Gather {
        registry: Arc<Registry>,
        sessions: Arc<Sessions>,
        router: Arc<Router>,
        rx: HashMap<&'static str, Rx>,
        tokens: HashMap<&'static str, u64>,
    }

    impl Gather {
        fn new() -> Self {
            Self::with(Config::default(), Metrics::noop())
        }

        fn with(config: Config, metrics: Arc<Metrics>) -> Self {
            let registry = Arc::new(Registry::new(config.owner_grace));
            let sessions = Arc::new(Sessions::new());
            let router = Router::new(
                Arc::clone(&registry),
                Arc::clone(&sessions),
                Arc::new(config),
                metrics,
            );
            let mut g = Gather {
                registry,
                sessions,
                router,
                rx: HashMap::new(),
                tokens: HashMap::new(),
            };
            g.connect("s1", "web", scatter_meta(2, true), None);
            g.connect("s-v1", "old", scatter_meta(0, false), None);
            g.connect("s-plain", "plain", scatter_meta(2, false), None);
            for (id, service) in [("a1", "A"), ("b1", "B"), ("b2", "B")] {
                g.connect(id, service, scatter_meta(0, false), None);
                g.shared(id, service, &["q.Find"]);
            }
            g.connect("x1", "X", scatter_meta(0, false), None);
            g.registry
                .register(&D, "x1", "X", &["c.Do".into()], &["q.Own".into()])
                .unwrap();
            g
        }

        fn connect(
            &mut self,
            id: &'static str,
            service: &str,
            meta: crate::session::SessionMeta,
            permits: Option<u32>,
        ) {
            self.connect_with_buffer(id, service, meta, permits, 64);
        }

        /// [`Gather::connect`] with an outbound buffer of `cap` messages.
        fn connect_with_buffer(
            &mut self,
            id: &'static str,
            service: &str,
            meta: crate::session::SessionMeta,
            permits: Option<u32>,
            cap: usize,
        ) {
            let token = self.sessions.allocate_token();
            assert!(self.router.open_session(&D, id, token, service, permits));
            let (tx, rx) = mpsc::channel(cap);
            self.sessions
                .insert_with_token(&D, id, service, meta, tx, CancellationToken::new(), token)
                .unwrap();
            self.router.start_session(&D, id, token);
            self.rx.insert(id, rx);
            self.tokens.insert(id, token);
        }

        fn shared(&self, id: &str, service: &str, types: &[&str]) {
            self.registry
                .register_types(
                    &D,
                    id,
                    service,
                    &RegisteredTypes {
                        shared_query_types: types.iter().map(|t| t.to_string()).collect(),
                        ..RegisteredTypes::default()
                    },
                    crate::registry::Placement::default(),
                )
                .unwrap();
        }

        fn leave(&self, id: &str) {
            crate::service::disconnect(
                &self.registry,
                &self.sessions,
                &self.router,
                &D,
                id,
                self.tokens[id],
            );
        }

        fn requests(&mut self, id: &str) -> Vec<Request> {
            let mut out = Vec::new();
            while let Ok(Ok(m)) = self.rx.get_mut(id).unwrap().try_recv() {
                if let Some(server_message::Message::Request(r)) = m.message {
                    out.push(r);
                }
            }
            out
        }

        fn messages(&mut self, id: &str) -> Vec<server_message::Message> {
            let mut out = Vec::new();
            while let Ok(Ok(m)) = self.rx.get_mut(id).unwrap().try_recv() {
                out.extend(m.message);
            }
            out
        }

        fn reply(&self, from: &str, id: &str, payload: &[u8]) {
            self.router.on_reply(
                &D,
                from,
                Reply {
                    request_id: id.into(),
                    result: Some(reply::Result::Payload(payload.to_vec())),
                },
            );
        }

        fn reply_error(&self, from: &str, id: &str) {
            self.router.on_reply(
                &D,
                from,
                Reply {
                    request_id: id.into(),
                    result: Some(reply::Result::Error(crate::proto::Error {
                        request_id: id.into(),
                        code: ErrorCode::HandlerError as i32,
                        message: "boom".into(),
                        details: vec![],
                    })),
                },
            );
        }
    }

    fn scatter(id: &str, type_name: &str, timeout_ms: u32, max_results: u32) -> Request {
        Request {
            id: id.into(),
            kind: RequestKind::Query as i32,
            r#type: type_name.into(),
            payload: b"{\"q\":1}".to_vec(),
            headers: HashMap::from([("traceparent".to_string(), "x".to_string())]),
            timeout_ms,
            mode: RequestMode::ScatterGather as i32,
            max_results,
            priority: 3,
            ..Request::default()
        }
    }

    fn part(m: &server_message::Message) -> &ReplyPart {
        match m {
            server_message::Message::Part(p) => p,
            other => panic!("expected a part, got {other:?}"),
        }
    }

    fn end(m: &server_message::Message) -> &ReplyEnd {
        match m {
            server_message::Message::End(e) => e,
            other => panic!("expected an end, got {other:?}"),
        }
    }

    fn error_code(m: &server_message::Message) -> (String, ErrorCode) {
        match m {
            server_message::Message::Error(e) => (e.request_id.clone(), e.code()),
            other => panic!("expected an error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn every_target_gets_an_ordinary_request_with_its_own_id() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 0));
        let mut ids = Vec::new();
        for h in ["a1", "b1", "b2"] {
            let got = g.requests(h);
            assert_eq!(got.len(), 1, "{h}");
            let r = &got[0];
            assert_eq!(
                r.mode,
                RequestMode::Unary as i32,
                "handlers see a unary request"
            );
            assert_eq!((r.max_results, r.priority), (0, 3));
            assert_eq!(r.payload, b"{\"q\":1}");
            assert_eq!(r.headers["traceparent"], "x");
            ids.push(r.id.clone());
        }
        assert_eq!(ids, ["g1#0", "g1#1", "g1#2"]);
        assert!(g.requests("x1").is_empty());
        assert_eq!(
            g.router.in_flight(),
            1,
            "a scatter-gather is one request in flight"
        );
        let (by_instance, total) = g.router.in_flight_by_instance();
        assert_eq!(total, 1);
        assert_eq!(by_instance["s1"].as_sender, 1);
        for h in ["a1", "b1", "b2"] {
            assert_eq!(by_instance[h].as_handler, 1, "{h}");
        }
        // An exclusive query: every instance of its owner.
        g.router.route(&D, "s1", scatter("g2", "q.Own", 0, 0));
        assert_eq!(g.requests("x1")[0].id, "g2#0");
    }

    #[tokio::test]
    async fn answers_are_relayed_as_parts_in_arrival_order_then_one_end() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 0));
        g.reply("b2", "g1#2", b"two");
        g.reply_error("a1", "g1#0");
        g.reply("b1", "g1#1", b"one");
        let got = g.messages("s1");
        assert_eq!(got.len(), 4, "{got:?}");
        let p0 = part(&got[0]);
        assert_eq!(
            (
                p0.request_id.as_str(),
                p0.seq,
                p0.responder_service.as_str()
            ),
            ("g1", 0, "B")
        );
        assert_eq!(
            (p0.responder_instance.as_str(), p0.payload.as_slice()),
            ("b2", &b"two"[..])
        );
        assert!(p0.error.is_none());
        let p1 = part(&got[1]);
        assert_eq!((p1.seq, p1.responder_instance.as_str()), (1, "a1"));
        let e = p1.error.as_ref().expect("a failed part");
        assert_eq!(
            (e.request_id.as_str(), e.code()),
            ("g1", ErrorCode::HandlerError)
        );
        assert!(p1.payload.is_empty());
        assert_eq!(part(&got[2]).seq, 2);
        let end = end(&got[3]);
        assert_eq!(
            (
                end.request_id.as_str(),
                end.parts,
                end.failed,
                end.timed_out
            ),
            ("g1", 3, 1, 0)
        );
        assert!(end.error.is_none());
        assert_eq!(g.router.in_flight(), 0);
        assert!(g.router.in_flight_by_instance().0.is_empty());
        // A late or repeated answer changes nothing.
        g.reply("b1", "g1#1", b"again");
        assert!(g.messages("s1").is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parts_and_the_end_reach_a_slow_sender_in_order() {
        let mut g = Gather::new();
        // A sender whose buffer holds one message: the first part fills it, the rest wait.
        g.connect_with_buffer("s-slow", "web", scatter_meta(2, true), None, 1);
        g.router.route(&D, "s-slow", scatter("g1", "q.Find", 0, 0));
        assert_eq!(
            g.requests("a1").len() + g.requests("b1").len() + g.requests("b2").len(),
            3
        );
        g.reply("a1", "g1#0", b"zero");
        g.reply_error("b1", "g1#1");
        g.reply("b2", "g1#2", b"two");
        assert_eq!(
            g.router.in_flight(),
            0,
            "the gather ended before the sender drained"
        );
        // Drain one message at a time, letting the waiting deliveries run in between.
        let mut got = Vec::new();
        for _ in 0..4 {
            let m = tokio::time::timeout(
                Duration::from_secs(1),
                g.rx.get_mut("s-slow").unwrap().recv(),
            )
            .await
            .expect("message within the delivery timeout")
            .unwrap()
            .unwrap();
            got.extend(m.message);
            tokio::task::yield_now().await;
        }
        let seqs: Vec<u64> = got[..3].iter().map(|m| part(m).seq).collect();
        assert_eq!(seqs, [0, 1, 2], "{got:?}");
        assert_eq!(part(&got[0]).payload, b"zero");
        assert!(part(&got[1]).error.is_some());
        assert_eq!(part(&got[2]).payload, b"two");
        let e = end(&got[3]);
        assert_eq!((e.parts, e.failed, e.timed_out), (3, 1, 0));
        tokio::task::yield_now().await;
        assert!(g.messages("s-slow").is_empty(), "nothing follows the end");
    }

    #[tokio::test]
    async fn max_results_ends_after_that_many_successful_parts() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 1));
        g.reply_error("b1", "g1#1");
        g.reply("a1", "g1#0", b"first");
        g.reply("b2", "g1#2", b"dropped");
        let got = g.messages("s1");
        assert_eq!(got.len(), 3, "{got:?}");
        assert!(
            part(&got[0]).error.is_some(),
            "a failed part does not count"
        );
        assert_eq!(part(&got[1]).payload, b"first");
        let end = end(&got[2]);
        assert_eq!((end.parts, end.failed, end.timed_out), (2, 1, 0));
        assert_eq!(g.router.in_flight(), 0);
        assert!(
            g.router.in_flight_by_instance().0.is_empty(),
            "the remaining targets' entries ended"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_deadline_ends_with_the_targets_that_did_not_answer() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Find", 100, 0));
        // Let the deadline timer start before the clock moves.
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(30)).await;
        g.reply("b1", "g1#1", b"one");
        tokio::time::sleep(Duration::from_millis(80)).await;
        let got = g.messages("s1");
        assert_eq!(got.len(), 2, "{got:?}");
        let end = end(&got[1]);
        assert_eq!((end.parts, end.failed, end.timed_out), (1, 0, 2));
        assert!(end.error.is_none(), "partial answers are answers");
        assert_eq!(g.router.in_flight(), 0);
        g.reply("a1", "g1#0", b"late");
        assert!(g.messages("s1").is_empty(), "a late answer is dropped");
        let e = edge_of(&g.router, "web", "q.Find");
        assert_eq!((e.count, e.errors), (1, 0), "one request on the edge");
        let max = e.latency.quantile(1.0).unwrap().as_secs_f64() * 1000.0;
        assert!((max - 100.0).abs() <= 5.0, "latency to the ReplyEnd: {max}");

        // Nobody answers: the edge counts an error.
        g.router.route(&D, "s1", scatter("g2", "q.Find", 100, 0));
        tokio::time::sleep(Duration::from_millis(101)).await;
        let got = g.messages("s1");
        assert_eq!(end_of(&got), (0, 0, 3));
        let e = edge_of(&g.router, "web", "q.Find");
        assert_eq!((e.count, e.errors), (2, 1));
    }

    /// The two locks of an answer: `on_reply` takes the target out of the map, then `on_part`
    /// takes the gather. A deadline between the two finds the target outstanding but no longer
    /// in the map; it still had no part, so it timed out.
    #[tokio::test]
    async fn a_target_answered_as_the_deadline_closes_the_gather_is_timed_out() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Find", 100, 0));
        let entry = g.router.take_unary("g1#1").expect("a forwarded target");
        g.router.release("g1#1", &entry);
        g.router.on_scatter_deadline("g1");
        g.router.on_part("g1#1", entry, Ok(b"too late".to_vec()));
        let got = g.messages("s1");
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(
            end_of(&got),
            (0, 0, 3),
            "parts + timed_out = targets (failed parts are parts): every target counts once"
        );
        assert_eq!(g.router.in_flight(), 0);
        assert!(g.router.in_flight_by_instance().0.is_empty());
    }

    fn end_of(got: &[server_message::Message]) -> (u32, u32, u32) {
        let e = end(got.last().expect("messages"));
        (e.parts, e.failed, e.timed_out)
    }

    #[tokio::test]
    async fn a_target_that_disconnects_is_a_failed_part() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 0));
        g.reply("a1", "g1#0", b"a");
        g.leave("b1");
        g.reply("b2", "g1#2", b"b");
        let got = g.messages("s1");
        assert_eq!(got.len(), 4, "{got:?}");
        let p = part(&got[1]);
        assert_eq!(p.responder_instance, "b1");
        assert_eq!(
            p.error.as_ref().unwrap().code(),
            ErrorCode::HandlerDisconnected
        );
        assert_eq!(end_of(&got), (3, 1, 0));
        let e = edge_of(&g.router, "web", "q.Find");
        assert_eq!((e.count, e.errors), (1, 0));
    }

    #[tokio::test]
    async fn every_part_failed_counts_as_an_error_on_the_edge() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 0));
        for (h, id) in [("a1", "g1#0"), ("b1", "g1#1"), ("b2", "g1#2")] {
            g.reply_error(h, id);
        }
        assert_eq!(end_of(&g.messages("s1")), (3, 3, 0));
        let e = edge_of(&g.router, "web", "q.Find");
        assert_eq!((e.count, e.errors), (1, 1));
    }

    #[tokio::test]
    async fn zero_targets_is_an_ordinary_no_handler_error() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Nope", 0, 0));
        for h in ["a1", "b1", "b2"] {
            g.leave(h);
        }
        g.router.route(&D, "s1", scatter("g2", "q.Find", 0, 0));
        let got = g.messages("s1");
        let codes: Vec<_> = got.iter().map(error_code).collect();
        assert_eq!(
            codes,
            [
                ("g1".to_string(), ErrorCode::NoHandler),
                ("g2".to_string(), ErrorCode::NoHandler)
            ]
        );
        assert_eq!(g.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn modes_are_gated_by_version_capability_mode_and_kind() {
        let mut g = Gather::new();
        // A v1 session cannot know UNSUPPORTED: INVALID_REQUEST.
        g.router.route(&D, "s-v1", scatter("v1", "q.Find", 0, 0));
        assert_eq!(
            g.messages("s-v1")
                .iter()
                .map(error_code)
                .collect::<Vec<_>>(),
            [("v1".to_string(), ErrorCode::InvalidRequest)]
        );
        // A v2 session that did not declare scatter-gather.
        g.router.route(&D, "s-plain", scatter("p1", "q.Find", 0, 0));
        assert_eq!(
            error_code(&g.messages("s-plain")[0]),
            ("p1".to_string(), ErrorCode::Unsupported)
        );
        // Modes this hub does not implement yet.
        for (id, mode) in [
            ("st", RequestMode::Streaming),
            ("sub", RequestMode::Subscription),
        ] {
            g.router.route(
                &D,
                "s1",
                Request {
                    mode: mode as i32,
                    ..scatter(id, "q.Find", 0, 0)
                },
            );
        }
        g.router.route(
            &D,
            "s1",
            Request {
                mode: 42,
                ..scatter("weird", "q.Find", 0, 0)
            },
        );
        // A command, by request kind or by the type's registered kind.
        g.router.route(
            &D,
            "s1",
            Request {
                kind: RequestKind::Command as i32,
                ..scatter("cmd", "c.Do", 0, 0)
            },
        );
        g.router.route(&D, "s1", scatter("cmd-q", "c.Do", 0, 0));
        let codes: Vec<_> = g.messages("s1").iter().map(error_code).collect();
        assert_eq!(
            codes,
            [
                ("st".to_string(), ErrorCode::Unsupported),
                ("sub".to_string(), ErrorCode::Unsupported),
                ("weird".to_string(), ErrorCode::Unsupported),
                ("cmd".to_string(), ErrorCode::InvalidRequest),
                ("cmd-q".to_string(), ErrorCode::InvalidRequest),
            ]
        );
        for h in ["a1", "b1", "b2", "x1"] {
            assert!(g.requests(h).is_empty(), "{h} got nothing");
        }
        assert_eq!(g.router.in_flight(), 0);
    }

    #[tokio::test]
    async fn a_duplicate_scatter_id_is_rejected_and_the_first_continues() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 0));
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 0));
        g.router.route(&D, "s1", request("g1", "c.Do", 0));
        let codes: Vec<_> = g.messages("s1").iter().map(error_code).collect();
        assert_eq!(
            codes,
            [
                ("g1".to_string(), ErrorCode::InvalidRequest),
                ("g1".to_string(), ErrorCode::InvalidRequest)
            ]
        );
        assert_eq!(g.router.in_flight(), 1);
        for (h, id) in [("a1", "g1#0"), ("b1", "g1#1"), ("b2", "g1#2")] {
            g.reply(h, id, b"x");
        }
        assert_eq!(end_of(&g.messages("s1")), (3, 0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn targets_are_flow_controlled_and_prioritised_like_unary_requests() {
        let mut g = Gather::new();
        g.leave("b2");
        // b3 has one permit, already taken by a unary query.
        g.connect("b3", "B", scatter_meta(2, false), Some(1));
        g.shared("b3", "B", &["q.Find"]);
        // A unary point query goes to one of a1, b1, b3; send until b3 holds its permit.
        let mut n = 0;
        while g.router.flow_by_instance()["b3"].permits_available > 0 {
            g.router.route(
                &D,
                "s1",
                Request {
                    mode: 0,
                    priority: 0,
                    ..scatter(&format!("u{n}"), "q.Find", 10_000, 0)
                },
            );
            n += 1;
        }
        g.router.route(
            &D,
            "s1",
            Request {
                mode: 0,
                priority: 0,
                ..scatter("waiting", "q.Find", 10_000, 0)
            },
        );
        let waiting_on_b3 = g.router.flow_by_instance()["b3"].queued == 1;
        g.requests("b3");
        g.router.route(&D, "s1", scatter("g1", "q.Find", 100, 0));
        let queued = g.router.flow_by_instance()["b3"].queued;
        assert_eq!(queued, if waiting_on_b3 { 2 } else { 1 });
        // A permit: the scatter target (priority 3) goes before the unary one (priority 0).
        g.router.on_permits(&D, "b3", g.tokens["b3"], 1, 0);
        let ids: Vec<String> = g.requests("b3").into_iter().map(|r| r.id).collect();
        assert_eq!(ids, ["g1#2"]);
        // The deadline takes a still-queued target out of the queue.
        g.router.route(&D, "s1", scatter("g2", "q.Find", 100, 0));
        assert!(g.router.flow_by_instance()["b3"].queued >= 1);
        tokio::time::sleep(Duration::from_millis(101)).await;
        let q = g.router.flow_by_instance()["b3"].queued;
        assert_eq!(
            q,
            u32::from(waiting_on_b3),
            "only the unary request still waits"
        );
    }

    #[tokio::test]
    async fn a_departing_sender_takes_its_gather_and_targets_with_it() {
        let mut g = Gather::new();
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 0));
        g.leave("s1");
        assert_eq!(g.router.in_flight(), 0);
        assert!(g.router.in_flight_by_instance().0.is_empty());
        g.reply("a1", "g1#0", b"x");
        for h in ["a1", "b1", "b2"] {
            assert!(
                g.messages(h)
                    .iter()
                    .all(|m| matches!(m, server_message::Message::Request(_)))
            );
        }
    }

    #[tokio::test]
    async fn a_queued_target_whose_instance_leaves_fails_instead_of_moving() {
        let mut g = Gather::new();
        g.leave("b2");
        g.connect("b3", "B", scatter_meta(2, false), Some(0));
        g.shared("b3", "B", &["q.Find"]);
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 0));
        assert_eq!(g.router.flow_by_instance()["b3"].queued, 1);
        g.leave("b3");
        let got = g.messages("s1");
        let p = part(&got[0]);
        assert_eq!(p.responder_instance, "b3");
        assert_eq!(
            p.error.as_ref().unwrap().code(),
            ErrorCode::HandlerDisconnected
        );
        assert_eq!(g.requests("a1").len(), 1, "a1 got only its own target");
        assert_eq!(g.requests("b1").len(), 1, "b1 got only its own target");
    }

    #[tokio::test]
    async fn scatter_metrics_count_one_request_by_mode_and_the_targets() {
        let m = crate::telemetry::test_support::test_metrics();
        let mut g = Gather::with(Config::default(), Arc::clone(&m.metrics));
        g.router.route(&D, "s1", scatter("g1", "q.Find", 0, 0));
        for (h, id) in [("a1", "g1#0"), ("b1", "g1#1"), ("b2", "g1#2")] {
            g.reply(h, id, b"x");
        }
        g.router.route(&D, "s-plain", scatter("p1", "q.Find", 0, 0));
        g.messages("s1");
        assert_eq!(
            m.counter("kraken_hub_requests_total", "mode", "scatter_gather"),
            Some(2)
        );
        assert_eq!(
            m.counter("kraken_hub_requests_total", "outcome", "unsupported"),
            Some(1)
        );
        assert_eq!(
            m.counter("kraken_hub_requests_total", "mode", "unary"),
            None
        );
        assert_eq!(
            m.histogram_sum("kraken_hub_scatter_targets"),
            Some((1, 3.0))
        );
    }
}

#[cfg(test)]
mod stream_tests {
    //! Streaming queries (ADR 0017, P4b).
    use super::*;
    use crate::proto::{Cancel, Flow, ReplyEnd, ReplyPart, RequestMode};
    use crate::registry::RegisteredTypes;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;
    use tonic::Status;

    static D: std::sync::LazyLock<ContextId> = std::sync::LazyLock::new(ContextId::default);

    type Rx = mpsc::Receiver<Result<ServerMessage, Status>>;

    fn meta(protocol_version: u32, streaming: bool) -> crate::session::SessionMeta {
        crate::session::SessionMeta {
            protocol_version,
            scatter_gather: protocol_version >= 2,
            streaming,
            flow_control: protocol_version >= 2,
            ..crate::session::SessionMeta::v1(Duration::from_secs(10))
        }
    }

    /// Owners A (a1 streaming, a2 without) and B (b1 streaming) of the shared query "q.Find";
    /// X (x1 streaming, x2 without) owns command "c.Do" and exclusive query "q.Own"; N (n1,
    /// without streaming) owns "q.Legacy". Senders: s1 ("web", v2, streaming), s-v1 (v1) and
    /// s-plain (v2 with scatter-gather but without streaming).
    struct Streams {
        registry: Arc<Registry>,
        sessions: Arc<Sessions>,
        router: Arc<Router>,
        rx: HashMap<&'static str, Rx>,
        tokens: HashMap<&'static str, u64>,
    }

    impl Streams {
        fn new() -> Self {
            Self::with(Config::default(), Metrics::noop())
        }

        fn with(config: Config, metrics: Arc<Metrics>) -> Self {
            let registry = Arc::new(Registry::new(config.owner_grace));
            let sessions = Arc::new(Sessions::new());
            let router = Router::new(
                Arc::clone(&registry),
                Arc::clone(&sessions),
                Arc::new(config),
                metrics,
            );
            let mut f = Streams {
                registry,
                sessions,
                router,
                rx: HashMap::new(),
                tokens: HashMap::new(),
            };
            f.connect("s1", "web", meta(2, true), None, 64);
            f.connect("s-v1", "old", meta(0, false), None, 64);
            f.connect("s-plain", "plain", meta(2, false), None, 64);
            for (id, service, streaming) in
                [("a1", "A", true), ("a2", "A", false), ("b1", "B", true)]
            {
                f.connect(id, service, meta(2, streaming), None, 64);
                f.shared(id, service, &["q.Find"]);
            }
            for (id, streaming) in [("x1", true), ("x2", false)] {
                f.connect(id, "X", meta(2, streaming), None, 64);
                f.registry
                    .register(&D, id, "X", &["c.Do".into()], &["q.Own".into()])
                    .unwrap();
            }
            f.connect("n1", "N", meta(2, false), None, 64);
            f.registry
                .register(&D, "n1", "N", &[], &["q.Legacy".into()])
                .unwrap();
            f
        }

        /// A session as the stream task opens it: flow state, session, started.
        fn connect(
            &mut self,
            id: &'static str,
            service: &str,
            meta: crate::session::SessionMeta,
            permits: Option<u32>,
            cap: usize,
        ) {
            let token = self.sessions.allocate_token();
            assert!(self.router.open_session(&D, id, token, service, permits));
            let (tx, rx) = mpsc::channel(cap);
            self.sessions
                .insert_with_token(&D, id, service, meta, tx, CancellationToken::new(), token)
                .unwrap();
            self.router.start_session(&D, id, token);
            self.rx.insert(id, rx);
            self.tokens.insert(id, token);
        }

        fn shared(&self, id: &str, service: &str, types: &[&str]) {
            self.registry
                .register_types(
                    &D,
                    id,
                    service,
                    &RegisteredTypes {
                        shared_query_types: types.iter().map(|t| t.to_string()).collect(),
                        ..RegisteredTypes::default()
                    },
                    crate::registry::Placement::default(),
                )
                .unwrap();
        }

        fn leave(&self, id: &str) {
            crate::service::disconnect(
                &self.registry,
                &self.sessions,
                &self.router,
                &D,
                id,
                self.tokens[id],
            );
        }

        fn messages(&mut self, id: &str) -> Vec<server_message::Message> {
            let mut out = Vec::new();
            while let Ok(Ok(m)) = self.rx.get_mut(id).unwrap().try_recv() {
                out.extend(m.message);
            }
            out
        }

        fn requests(&mut self, id: &str) -> Vec<Request> {
            self.messages(id)
                .into_iter()
                .filter_map(|m| match m {
                    server_message::Message::Request(r) => Some(r),
                    _ => None,
                })
                .collect()
        }

        /// The handler that got the target of stream `id` (`<id>#1`), with the request.
        fn target_of(&mut self, id: &str) -> (&'static str, Request) {
            let target = format!("{id}#1");
            let handlers: Vec<&'static str> = self.rx.keys().copied().collect();
            for h in handlers {
                if let Some(r) = self.requests(h).into_iter().find(|r| r.id == target) {
                    return (h, r);
                }
            }
            panic!("no handler got {target}");
        }

        fn part(&self, from: &str, target: &str, seq: u64, payload: &[u8]) {
            self.router.on_stream_part(
                &D,
                from,
                ReplyPart {
                    request_id: target.into(),
                    payload: payload.to_vec(),
                    seq,
                    ..ReplyPart::default()
                },
            );
        }

        fn part_error(&self, from: &str, target: &str, seq: u64) {
            self.router.on_stream_part(
                &D,
                from,
                ReplyPart {
                    request_id: target.into(),
                    error: Some(crate::proto::Error {
                        request_id: target.into(),
                        code: ErrorCode::HandlerError as i32,
                        message: "boom".into(),
                        details: vec![],
                    }),
                    seq,
                    ..ReplyPart::default()
                },
            );
        }

        fn end(&self, from: &str, target: &str, error: Option<ErrorCode>) {
            self.router.on_stream_end(
                &D,
                from,
                ReplyEnd {
                    request_id: target.into(),
                    error: error.map(|code| crate::proto::Error {
                        request_id: target.into(),
                        code: code as i32,
                        message: "failed".into(),
                        details: vec![],
                    }),
                    ..ReplyEnd::default()
                },
            );
        }

        fn flow(&self, from: &str, id: &str, credits: u32) {
            self.router.on_flow(
                &D,
                from,
                Flow {
                    request_id: id.into(),
                    credits,
                },
            );
        }

        fn cancel(&self, from: &str, id: &str, reason: &str) {
            self.router.on_cancel(
                &D,
                from,
                Cancel {
                    request_id: id.into(),
                    reason: reason.into(),
                },
            );
        }
    }

    fn stream(
        id: &str,
        type_name: &str,
        timeout_ms: u32,
        credit: u32,
        max_results: u32,
    ) -> Request {
        Request {
            id: id.into(),
            kind: RequestKind::Query as i32,
            r#type: type_name.into(),
            payload: b"{\"q\":1}".to_vec(),
            headers: HashMap::from([("traceparent".to_string(), "x".to_string())]),
            timeout_ms,
            mode: RequestMode::Streaming as i32,
            initial_credit: credit,
            max_results,
            priority: 3,
            ..Request::default()
        }
    }

    fn part(m: &server_message::Message) -> &ReplyPart {
        match m {
            server_message::Message::Part(p) => p,
            other => panic!("expected a part, got {other:?}"),
        }
    }

    fn end(m: &server_message::Message) -> &ReplyEnd {
        match m {
            server_message::Message::End(e) => e,
            other => panic!("expected an end, got {other:?}"),
        }
    }

    fn cancel(m: &server_message::Message) -> &Cancel {
        match m {
            server_message::Message::Cancel(c) => c,
            other => panic!("expected a cancel, got {other:?}"),
        }
    }

    fn flow(m: &server_message::Message) -> &Flow {
        match m {
            server_message::Message::Flow(f) => f,
            other => panic!("expected a flow, got {other:?}"),
        }
    }

    fn error_code(m: &server_message::Message) -> (String, ErrorCode, String) {
        match m {
            server_message::Message::Error(e) => {
                (e.request_id.clone(), e.code(), e.message.clone())
            }
            other => panic!("expected an error, got {other:?}"),
        }
    }

    /// The `(sender service, type)` edge of a request, from the router's statistics.
    fn edge_of(router: &Router, sender: &str, type_name: &str) -> crate::edges::EdgeWindow {
        let snap = router
            .edges()
            .expect("dashboard on")
            .snapshot(Instant::now());
        snap.edges
            .into_iter()
            .find(|e| e.sender == sender && e.type_name == type_name)
            .unwrap_or_else(|| panic!("no edge {sender} -> {type_name}"))
    }

    /// `(code, message)` of a `ReplyEnd.error`.
    fn end_error(e: &ReplyEnd) -> (ErrorCode, String) {
        let err = e.error.as_ref().expect("an error on the end");
        (err.code(), err.message.clone())
    }

    #[tokio::test]
    async fn a_streaming_query_goes_to_one_capable_instance_with_its_credit() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, r) = f.target_of("st1");
        assert!(
            ["a1", "b1"].contains(&h),
            "a capable owner instance, got {h}"
        );
        assert_eq!(r.mode, RequestMode::Streaming as i32);
        assert_eq!(r.initial_credit, 64, "64 when the sender sent 0");
        assert_eq!((r.max_results, r.priority), (0, 3));
        assert_eq!(r.payload, b"{\"q\":1}");
        assert_eq!(r.headers["traceparent"], "x");
        assert_eq!(r.r#type, "q.Find");
        assert!(
            f.requests("a2").is_empty(),
            "a2 did not negotiate streaming"
        );
        assert_eq!(f.router.in_flight(), 1, "a stream is one request in flight");
        let (by_instance, total) = f.router.in_flight_by_instance();
        assert_eq!(total, 1);
        assert_eq!(by_instance["s1"].as_sender, 1);
        assert_eq!(by_instance[h].as_handler, 1);

        // The sender's credit is forwarded as is; the capable instances take turns.
        f.router.route(&D, "s1", stream("st2", "q.Find", 0, 5, 0));
        let (h2, r2) = f.target_of("st2");
        assert_eq!(r2.initial_credit, 5);
        assert_ne!(h, h2, "round-robin among the capable instances");

        // An exclusive type: the owner's capable instance.
        f.router.route(&D, "s1", stream("st3", "q.Own", 0, 0, 0));
        assert_eq!(f.target_of("st3").0, "x1");
        assert!(f.requests("x2").is_empty());
        assert!(f.messages("s1").is_empty(), "nothing to the sender yet");
    }

    #[tokio::test]
    async fn streaming_is_gated_by_version_capability_kind_and_targets() {
        let mut f = Streams::new();
        f.router.route(&D, "s-v1", stream("v1", "q.Find", 0, 0, 0));
        let got = f.messages("s-v1");
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(error_code(&got[0]).1, ErrorCode::InvalidRequest);

        f.router
            .route(&D, "s-plain", stream("p1", "q.Find", 0, 0, 0));
        let (_, code, message) = error_code(&f.messages("s-plain")[0]);
        assert_eq!(code, ErrorCode::Unsupported);
        assert!(message.contains("\"streaming\""), "{message}");

        f.router
            .route(&D, "s1", stream("in-flight", "q.Find", 0, 0, 0));
        f.router.route(
            &D,
            "s1",
            Request {
                kind: RequestKind::Command as i32,
                ..stream("cmd", "c.Do", 0, 0, 0)
            },
        );
        f.router.route(&D, "s1", stream("cmd-q", "c.Do", 0, 0, 0));
        f.router
            .route(&D, "s1", stream("legacy", "q.Legacy", 0, 0, 0));
        f.router.route(&D, "s1", stream("nope", "q.Nope", 0, 0, 0));
        f.router
            .route(&D, "s1", stream("in-flight", "q.Find", 0, 0, 0));
        f.router
            .route(&D, "s1", stream("in-flight#1", "q.Find", 0, 0, 0));
        f.router.route(
            &D,
            "s1",
            Request {
                mode: RequestMode::Subscription as i32,
                ..stream("sub", "q.Find", 0, 0, 0)
            },
        );
        let got = f.messages("s1");
        let codes: Vec<(String, ErrorCode)> = got
            .iter()
            .map(|m| {
                let (id, code, _) = error_code(m);
                (id, code)
            })
            .collect();
        assert_eq!(
            codes,
            [
                ("cmd".to_string(), ErrorCode::InvalidRequest),
                ("cmd-q".to_string(), ErrorCode::InvalidRequest),
                ("legacy".to_string(), ErrorCode::Unsupported),
                ("nope".to_string(), ErrorCode::NoHandler),
                ("in-flight".to_string(), ErrorCode::InvalidRequest),
                ("in-flight#1".to_string(), ErrorCode::InvalidRequest),
                ("sub".to_string(), ErrorCode::Unsupported),
            ]
        );
        let legacy = error_code(&got[2]).2;
        assert!(
            legacy.contains('N') && legacy.contains("streaming"),
            "names the service and the capability: {legacy}"
        );
        for h in ["a2", "x1", "x2", "n1"] {
            assert!(f.requests(h).is_empty(), "{h} got nothing");
        }
        assert_eq!(f.router.in_flight(), 1, "only the first stream");
        f.target_of("in-flight");
    }

    #[tokio::test]
    async fn parts_are_relayed_in_order_with_the_handlers_seq_then_the_end() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 3, 0));
        let (h, _) = f.target_of("st1");
        f.part(h, "st1#1", 0, b"a");
        f.part(h, "st1#1", 7, b"b");
        f.end(h, "st1#1", None);
        let got = f.messages("s1");
        assert_eq!(got.len(), 3, "{got:?}");
        let p0 = part(&got[0]);
        assert_eq!(
            (p0.request_id.as_str(), p0.seq, p0.payload.as_slice()),
            ("st1", 0, &b"a"[..])
        );
        assert_eq!(p0.responder_instance, h);
        assert_eq!(p0.responder_service, if h == "a1" { "A" } else { "B" });
        assert!(p0.error.is_none());
        let p1 = part(&got[1]);
        assert_eq!((p1.seq, p1.payload.as_slice()), (7, &b"b"[..]));
        let e = end(&got[2]);
        assert_eq!(
            (e.request_id.as_str(), e.parts, e.failed, e.timed_out),
            ("st1", 2, 0, 0)
        );
        assert!(e.error.is_none());
        assert_eq!(f.router.in_flight(), 0);
        assert!(f.router.in_flight_by_instance().0.is_empty());
        assert!(f.messages(h).is_empty(), "no cancel: the handler ended");
        // A late part or end changes nothing.
        f.part(h, "st1#1", 8, b"late");
        f.end(h, "st1#1", None);
        assert!(f.messages("s1").is_empty());
    }

    #[tokio::test]
    async fn an_end_with_an_error_is_relayed_as_the_streams_error() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        f.part(h, "st1#1", 0, b"a");
        f.end(h, "st1#1", Some(ErrorCode::HandlerError));
        let got = f.messages("s1");
        assert_eq!(got.len(), 2, "{got:?}");
        let e = end(&got[1]);
        assert_eq!(e.parts, 1);
        assert_eq!(
            end_error(e),
            (ErrorCode::HandlerError, "failed".to_string())
        );
        assert_eq!(
            e.error.as_ref().unwrap().request_id,
            "st1",
            "the sender's id"
        );
        assert_eq!(f.router.in_flight(), 0);
        let edge = edge_of(&f.router, "web", "q.Find");
        assert_eq!((edge.count, edge.errors), (1, 1));
    }

    #[tokio::test]
    async fn a_part_carrying_an_error_ends_the_stream_with_that_error() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        f.part(h, "st1#1", 0, b"a");
        f.part_error(h, "st1#1", 1);
        let got = f.messages("s1");
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(part(&got[0]).seq, 0);
        let e = end(&got[1]);
        assert_eq!(e.parts, 1, "the error part is not a part");
        assert_eq!(end_error(e), (ErrorCode::HandlerError, "boom".to_string()));
        assert!(f.messages(h).is_empty(), "no cancel: the handler ended");
        assert_eq!(f.router.in_flight(), 0);
        assert!(f.router.in_flight_by_instance().0.is_empty());
    }

    #[tokio::test]
    async fn a_part_beyond_the_credit_ends_the_stream_and_cancels_the_handler() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 2, 0));
        let (h, _) = f.target_of("st1");
        f.part(h, "st1#1", 0, b"a");
        f.part(h, "st1#1", 1, b"b");
        f.part(h, "st1#1", 2, b"c");
        let got = f.messages("s1");
        assert_eq!(got.len(), 3, "{got:?}");
        let e = end(&got[2]);
        assert_eq!(e.parts, 2);
        assert_eq!(
            end_error(e),
            (ErrorCode::InvalidRequest, "part without credit".to_string())
        );
        let got = f.messages(h);
        assert_eq!(got.len(), 1, "{got:?}");
        let c = cancel(&got[0]);
        assert_eq!(
            (c.request_id.as_str(), c.reason.as_str()),
            ("st1#1", "part without credit")
        );
        assert_eq!(f.router.in_flight(), 0);
        f.part(h, "st1#1", 3, b"d");
        assert!(f.messages("s1").is_empty(), "the stream is gone");
    }

    #[tokio::test]
    async fn a_flow_from_the_sender_adds_credit_and_reaches_the_handler() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 1, 0));
        let (h, _) = f.target_of("st1");
        f.part(h, "st1#1", 0, b"a");
        f.flow("s1", "st1", 2);
        let got = f.messages(h);
        assert_eq!(got.len(), 1, "{got:?}");
        let fl = flow(&got[0]);
        assert_eq!((fl.request_id.as_str(), fl.credits), ("st1#1", 2));
        f.part(h, "st1#1", 1, b"b");
        f.part(h, "st1#1", 2, b"c");
        assert_eq!(f.messages("s1").len(), 3, "three parts within the credit");
        // Flows that are not the sender's, for an unknown stream, or empty are ignored.
        f.flow("s-plain", "st1", 5);
        f.flow(h, "st1", 5);
        f.flow("s1", "ghost", 5);
        f.flow("s1", "st1", 0);
        assert!(f.messages(h).is_empty(), "nothing reached the handler");
        f.part(h, "st1#1", 3, b"d");
        let got = f.messages("s1");
        assert_eq!(
            end_error(end(&got[0])).0,
            ErrorCode::InvalidRequest,
            "no credit was added: {got:?}"
        );
    }

    #[tokio::test]
    async fn max_results_ends_after_that_many_parts_and_cancels_the_handler() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 2));
        let (h, r) = f.target_of("st1");
        assert_eq!(r.max_results, 0, "the hub enforces it");
        f.part(h, "st1#1", 0, b"a");
        assert!(f.messages(h).is_empty());
        f.part(h, "st1#1", 1, b"b");
        let got = f.messages("s1");
        assert_eq!(got.len(), 3, "{got:?}");
        let e = end(&got[2]);
        assert_eq!(e.parts, 2);
        assert!(e.error.is_none());
        let got = f.messages(h);
        assert_eq!(cancel(&got[0]).reason, "max results");
        assert_eq!(f.router.in_flight(), 0);
        assert!(f.router.in_flight_by_instance().0.is_empty());
        f.part(h, "st1#1", 2, b"c");
        assert!(f.messages("s1").is_empty(), "a late part is dropped");
        let edge = edge_of(&f.router, "web", "q.Find");
        assert_eq!((edge.count, edge.errors), (1, 0));
    }

    /// The hub does not know streaming handlers that answer with a unary `Reply`, but relays
    /// one as a single part and the end rather than letting the stream idle out.
    #[tokio::test]
    async fn a_unary_reply_to_a_streaming_target_is_one_part_and_the_end() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        f.part(h, "st1#1", 0, b"a");
        f.router.on_reply(
            &D,
            h,
            Reply {
                request_id: "st1#1".into(),
                result: Some(reply::Result::Payload(b"whole".to_vec())),
            },
        );
        let got = f.messages("s1");
        assert_eq!(got.len(), 3, "{got:?}");
        assert_eq!(
            (part(&got[1]).seq, part(&got[1]).payload.as_slice()),
            (1, &b"whole"[..])
        );
        assert_eq!(end(&got[2]).parts, 2);
        assert_eq!(f.router.in_flight(), 0);
        // A Reply{error} ends it with that error.
        f.router.route(&D, "s1", stream("st2", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st2");
        f.router.on_reply(
            &D,
            h,
            Reply {
                request_id: "st2#1".into(),
                result: Some(reply::Result::Error(crate::proto::Error {
                    request_id: "st2#1".into(),
                    code: ErrorCode::PermissionDenied as i32,
                    message: "no".into(),
                    details: vec![],
                })),
            },
        );
        let got = f.messages("s1");
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(end_error(end(&got[0])).0, ErrorCode::PermissionDenied);
    }

    #[tokio::test]
    async fn parts_and_ends_for_unknown_ids_or_from_non_targets_are_ignored() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        let other = if h == "a1" { "b1" } else { "a1" };
        f.part(other, "st1#1", 0, b"intruder");
        f.end(other, "st1#1", None);
        f.part(h, "ghost#1", 0, b"ghost");
        f.end(h, "ghost#1", None);
        f.part(h, "st1", 0, b"the sender's id, not the target's");
        assert!(f.messages("s1").is_empty());
        assert_eq!(f.router.in_flight(), 1, "the stream is untouched");
        // A part for a scatter-gather target: not a streaming request.
        f.router.route(
            &D,
            "s1",
            Request {
                mode: RequestMode::ScatterGather as i32,
                ..stream("g1", "q.Find", 0, 0, 0)
            },
        );
        let targets: Vec<Request> = ["a1", "a2", "b1"]
            .iter()
            .flat_map(|h| f.requests(h))
            .filter(|r| r.id.starts_with("g1#"))
            .collect();
        assert_eq!(targets.len(), 3);
        f.part("a1", "g1#0", 0, b"not a stream");
        f.end("a1", "g1#0", None);
        assert!(f.messages("s1").is_empty());
        assert_eq!(f.router.in_flight(), 2, "the gather continues");
        f.part(h, "st1#1", 0, b"real");
        assert_eq!(f.messages("s1").len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parts_and_the_end_reach_a_slow_sender_in_order() {
        let mut f = Streams::new();
        // A sender whose buffer holds one message: the first part fills it, the rest wait.
        f.connect("s-slow", "web", meta(2, true), None, 1);
        f.router
            .route(&D, "s-slow", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        for seq in 0..3 {
            f.part(h, "st1#1", seq, format!("p{seq}").as_bytes());
        }
        f.end(h, "st1#1", None);
        assert_eq!(f.router.in_flight(), 0, "ended before the sender drained");
        let mut got = Vec::new();
        for _ in 0..4 {
            let m = tokio::time::timeout(
                Duration::from_secs(1),
                f.rx.get_mut("s-slow").unwrap().recv(),
            )
            .await
            .expect("message within the delivery timeout")
            .unwrap()
            .unwrap();
            got.extend(m.message);
            tokio::task::yield_now().await;
        }
        let seqs: Vec<u64> = got[..3].iter().map(|m| part(m).seq).collect();
        assert_eq!(seqs, [0, 1, 2], "{got:?}");
        assert_eq!(part(&got[2]).payload, b"p2");
        assert_eq!(end(&got[3]).parts, 3);
        tokio::task::yield_now().await;
        assert!(f.messages("s-slow").is_empty(), "nothing follows the end");
    }

    #[tokio::test]
    async fn a_cancel_from_the_sender_reaches_the_handler_and_ends_the_stream_silently() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        f.part(h, "st1#1", 0, b"a");
        // Cancels that are not the sender's or for an unknown stream are ignored.
        f.cancel("s-plain", "st1", "not mine");
        f.cancel(h, "st1", "not the handler's to cancel");
        f.cancel("s1", "ghost", "nothing");
        assert!(f.messages(h).is_empty());
        assert_eq!(f.router.in_flight(), 1);
        f.cancel("s1", "st1", "enough");
        let got = f.messages(h);
        assert_eq!(got.len(), 1, "{got:?}");
        let c = cancel(&got[0]);
        assert_eq!(
            (c.request_id.as_str(), c.reason.as_str()),
            ("st1#1", "enough")
        );
        let got = f.messages("s1");
        assert_eq!(got.len(), 1, "the part only, no end: {got:?}");
        assert!(matches!(got[0], server_message::Message::Part(_)));
        assert_eq!(f.router.in_flight(), 0);
        assert!(f.router.in_flight_by_instance().0.is_empty());
        f.part(h, "st1#1", 1, b"late");
        assert!(f.messages("s1").is_empty());
        let edge = edge_of(&f.router, "web", "q.Find");
        assert_eq!((edge.count, edge.errors), (1, 0), "a cancel is no error");
        // An empty reason gets a default one.
        f.router.route(&D, "s1", stream("st2", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st2");
        f.cancel("s1", "st2", "");
        assert!(!cancel(&f.messages(h)[0]).reason.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn no_part_within_the_timeout_ends_the_stream_and_cancels_the_handler() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 100, 0, 0));
        let (h, _) = f.target_of("st1");
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(80)).await;
        f.part(h, "st1#1", 0, b"a");
        // The part reset the idle timeout: nothing at 100 ms from the start.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(f.messages("s1").len(), 1, "the part only");
        tokio::time::sleep(Duration::from_millis(51)).await;
        let got = f.messages("s1");
        assert_eq!(got.len(), 1, "{got:?}");
        let e = end(&got[0]);
        assert_eq!(e.parts, 1);
        assert_eq!(
            end_error(e),
            (ErrorCode::Timeout, "no part within 100 ms".to_string())
        );
        let got = f.messages(h);
        assert_eq!(cancel(&got[0]).reason, "idle timeout");
        assert_eq!(f.router.in_flight(), 0);
        let edge = edge_of(&f.router, "web", "q.Find");
        assert_eq!((edge.count, edge.errors), (1, 1));
        let max = edge.latency.quantile(1.0).unwrap().as_secs_f64() * 1000.0;
        assert!((max - 180.0).abs() <= 5.0, "latency to the end: {max}");
    }

    #[tokio::test(start_paused = true)]
    async fn the_max_stream_duration_ends_an_active_stream_with_cancelled() {
        let config = Config {
            max_stream_duration: Duration::from_secs(1),
            ..Config::default()
        };
        let mut f = Streams::with(config, Metrics::noop());
        f.router
            .route(&D, "s1", stream("st1", "q.Find", 60_000, 0, 0));
        let (h, _) = f.target_of("st1");
        tokio::task::yield_now().await;
        for seq in 0..9 {
            tokio::time::advance(Duration::from_millis(100)).await;
            f.part(h, "st1#1", seq, b"tick");
        }
        tokio::time::sleep(Duration::from_millis(101)).await;
        let got = f.messages("s1");
        assert_eq!(got.len(), 10, "{got:?}");
        let e = end(&got[9]);
        assert_eq!(e.parts, 9);
        assert_eq!(
            end_error(e),
            (
                ErrorCode::Cancelled,
                "max stream duration (1s) exceeded".to_string()
            )
        );
        assert_eq!(
            cancel(&f.messages(h)[0]).reason,
            "max stream duration (1s) exceeded"
        );
        assert_eq!(f.router.in_flight(), 0);
    }

    /// The target of a stream takes one of its handler's permits for the whole stream, and
    /// every ending path gives the flow state back (the client returns the permit itself).
    #[tokio::test(start_paused = true)]
    async fn a_stream_holds_a_permit_until_it_ends_whichever_way() {
        let mut f = Streams::new();
        for h in ["a1", "a2", "b1"] {
            f.leave(h);
        }
        f.connect("b3", "B", meta(2, true), Some(1), 64);
        f.shared("b3", "B", &["q.Find"]);
        let assigned = |f: &Streams| f.router.loads(&[candidate("b3")])["b3"].assigned;
        let unary = |id: &str| Request {
            mode: 0,
            priority: 0,
            ..stream(id, "q.Find", 10_000, 0, 0)
        };
        type EndIt = Box<dyn Fn(&mut Streams)>;
        let end_paths: Vec<(&str, EndIt)> = vec![
            ("end", Box::new(|f| f.end("b3", "st#1", None))),
            ("error part", Box::new(|f| f.part_error("b3", "st#1", 0))),
            ("cancel", Box::new(|f| f.cancel("s1", "st", "done"))),
            (
                "over credit",
                Box::new(|f| {
                    f.part("b3", "st#1", 0, b"a");
                    f.part("b3", "st#1", 1, b"b");
                }),
            ),
            ("max results", Box::new(|f| f.part("b3", "st#1", 0, b"a"))),
            (
                "idle timeout",
                Box::new(|_| {
                    // advanced below
                }),
            ),
        ];
        for (name, end_it) in end_paths {
            let max_results = u32::from(name == "max results");
            f.router
                .route(&D, "s1", stream("st", "q.Find", 200, 1, max_results));
            assert_eq!(f.target_of("st").0, "b3", "{name}");
            f.router.route(&D, "s1", unary("waits"));
            assert_eq!(
                f.router.flow_by_instance()["b3"].queued,
                1,
                "{name}: no permit left"
            );
            assert_eq!(assigned(&f), 2, "{name}");
            tokio::task::yield_now().await;
            if name == "idle timeout" {
                tokio::time::sleep(Duration::from_millis(201)).await;
            } else {
                end_it(&mut f);
            }
            assert_eq!(f.router.in_flight(), 1, "{name}: the unary request stays");
            assert_eq!(assigned(&f), 1, "{name}: the stream's target is released");
            assert_eq!(
                f.router.flow_by_instance()["b3"].queued,
                1,
                "{name}: still no permit"
            );
            // The handler returns the permit when its stream handler finishes.
            f.router.on_permits(&D, "b3", f.tokens["b3"], 1, 0);
            assert_eq!(
                f.requests("b3")
                    .iter()
                    .map(|r| r.id.as_str())
                    .collect::<Vec<_>>(),
                ["waits"],
                "{name}"
            );
            f.router.on_reply(
                &D,
                "b3",
                Reply {
                    request_id: "waits".into(),
                    result: Some(reply::Result::Payload(vec![])),
                },
            );
            f.router.on_permits(&D, "b3", f.tokens["b3"], 1, 0);
            f.messages("s1");
            f.messages("b3");
            assert_eq!(assigned(&f), 0, "{name}");
        }
        // A stream still queued when it ends never took a permit: the queue entry goes.
        f.router.route(&D, "s1", unary("busy"));
        assert_eq!(f.requests("b3").len(), 1);
        f.router
            .route(&D, "s1", stream("queued", "q.Find", 100, 0, 0));
        assert_eq!(f.router.flow_by_instance()["b3"].queued, 1);
        tokio::time::sleep(Duration::from_millis(101)).await;
        let got = f.messages("s1");
        assert_eq!(end_error(end(&got[0])).0, ErrorCode::Timeout, "{got:?}");
        assert_eq!(
            f.router.flow_by_instance()["b3"].queued,
            0,
            "left the queue"
        );
        assert!(f.messages("b3").is_empty(), "never forwarded: no cancel");
        assert_eq!(assigned(&f), 1, "busy only");
    }

    /// The idle timeout starts over when the target is forwarded: a queue wait leaves the
    /// handler the whole timeout for its first part, and is itself bounded by the timeout.
    #[tokio::test(start_paused = true)]
    async fn the_idle_timeout_starts_over_at_the_forward() {
        let mut f = Streams::new();
        for h in ["a1", "a2", "b1"] {
            f.leave(h);
        }
        f.connect("b3", "B", meta(2, true), Some(1), 64);
        f.shared("b3", "B", &["q.Find"]);
        f.router.route(
            &D,
            "s1",
            Request {
                mode: 0,
                ..stream("busy", "q.Find", 10_000, 0, 0)
            },
        );
        f.requests("b3");
        f.router.route(&D, "s1", stream("st1", "q.Find", 100, 0, 0));
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(60)).await;
        f.router.on_reply(
            &D,
            "b3",
            Reply {
                request_id: "busy".into(),
                result: Some(reply::Result::Payload(vec![])),
            },
        );
        f.router.on_permits(&D, "b3", f.tokens["b3"], 1, 0);
        assert_eq!(f.target_of("st1").0, "b3");
        f.messages("s1");
        tokio::time::sleep(Duration::from_millis(90)).await;
        assert!(
            f.messages("s1").is_empty(),
            "150 ms after the route, 90 after the forward"
        );
        tokio::time::sleep(Duration::from_millis(11)).await;
        let got = f.messages("s1");
        assert_eq!(end_error(end(&got[0])).0, ErrorCode::Timeout, "{got:?}");
    }

    /// A consumer slower than the window is no idle handler: the idle clock does not run while
    /// the sender's credit is exhausted, and starts over when a `Flow` raises it.
    #[tokio::test(start_paused = true)]
    async fn the_idle_clock_is_paused_while_the_credit_is_exhausted() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 100, 2, 0));
        let (h, _) = f.target_of("st1");
        tokio::task::yield_now().await;
        f.part(h, "st1#1", 1, b"a");
        f.part(h, "st1#1", 2, b"b");
        assert_eq!(f.messages("s1").len(), 2, "the window");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let got = f.messages("s1");
        assert!(
            got.is_empty(),
            "no TIMEOUT while the sender owes credit: {got:?}"
        );
        f.flow("s1", "st1", 2);
        tokio::time::sleep(Duration::from_millis(90)).await;
        let got = f.messages("s1");
        assert!(got.is_empty(), "the clock starts over at the flow: {got:?}");
        tokio::time::sleep(Duration::from_millis(11)).await;
        let got = f.messages("s1");
        assert_eq!(got.len(), 1, "{got:?}");
        let e = end(&got[0]);
        assert_eq!(e.parts, 2);
        assert_eq!(
            end_error(e),
            (ErrorCode::Timeout, "no part within 100 ms".to_string())
        );
        let got = f.messages(h);
        assert_eq!(flow(&got[0]).credits, 2);
        assert_eq!(cancel(&got[1]).reason, "idle timeout");
        assert_eq!(f.router.in_flight(), 0);
    }

    /// The stream closes between the pop of its queued target and the write to the handler
    /// (`forward_ready` checked the entry, then lost the race): the handler must not serve a
    /// stream the hub forgot, so it gets the `Request` and then one `Cancel`, and never a
    /// `Cancel` before the `Request` (it would ignore that one as unknown).
    #[tokio::test]
    async fn a_stream_closed_between_the_pop_and_the_forward_is_cancelled_after_the_request() {
        let mut f = Streams::new();
        for h in ["a1", "a2", "b1"] {
            f.leave(h);
        }
        f.connect("b3", "B", meta(2, true), Some(1), 64);
        f.shared("b3", "B", &["q.Find"]);
        let token = f.tokens["b3"];
        f.router.route(
            &D,
            "s1",
            Request {
                mode: 0,
                ..stream("busy", "q.Find", 10_000, 0, 0)
            },
        );
        assert_eq!(f.requests("b3").len(), 1);
        f.router
            .route(&D, "s1", stream("st1", "q.Find", 10_000, 0, 0));
        assert!(f.requests("b3").is_empty(), "queued behind busy");
        // A permit comes back and the target is popped ...
        let ready = {
            let mut flows = f.router.flows.lock().unwrap();
            let flow = flows.get_mut("b3").unwrap();
            flow.permits = Some(1);
            drain(flow)
        };
        let [item]: [Queued; 1] = ready.try_into().expect("the target");
        assert!(f.router.is_pending_for(&item.id, "b3", token));
        // ... the sender cancels before the write ...
        f.cancel("s1", "st1", "changed my mind");
        let got = f.messages("b3");
        assert!(got.is_empty(), "no Cancel before the Request: {got:?}");
        // ... and the write goes through.
        f.sessions
            .send_if_current(&D, "b3", token, item.msg)
            .unwrap();
        f.router.note_forwarded(&D, &item.id, "b3", token, "B");
        let got = f.messages("b3");
        assert_eq!(got.len(), 2, "{got:?}");
        assert!(
            matches!(&got[0], server_message::Message::Request(r) if r.id == "st1#1"),
            "{got:?}"
        );
        let c = cancel(&got[1]);
        assert_eq!(
            (c.request_id.as_str(), c.reason.as_str()),
            ("st1#1", "closed before forward")
        );
        assert!(
            f.messages("s1").is_empty(),
            "the sender cancelled: nothing more"
        );
        assert_eq!(f.router.in_flight(), 1, "busy only");
    }

    fn candidate(id: &str) -> Candidate {
        Candidate {
            id: id.to_string(),
            load_factor: 100,
            tags: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn stream_metrics_count_one_request_the_open_streams_and_the_parts() {
        let m = crate::telemetry::test_support::test_metrics();
        let mut f = Streams::with(Config::default(), Arc::clone(&m.metrics));
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        f.router.route(&D, "s1", stream("st2", "q.Own", 0, 0, 0));
        assert_eq!(m.up_down("kraken_hub_streams_open", "instance", h), Some(1));
        assert_eq!(
            m.up_down("kraken_hub_streams_open", "instance", "x1"),
            Some(1)
        );
        assert_eq!(
            m.up_down("kraken_hub_streams_open", "service", "X"),
            Some(1)
        );
        f.part(h, "st1#1", 0, b"a");
        f.part(h, "st1#1", 1, b"b");
        f.end(h, "st1#1", None);
        f.cancel("s1", "st2", "bye");
        f.router
            .route(&D, "s-plain", stream("p1", "q.Find", 0, 0, 0));
        assert_eq!(m.up_down("kraken_hub_streams_open", "instance", h), Some(0));
        assert_eq!(
            m.up_down("kraken_hub_streams_open", "instance", "x1"),
            Some(0)
        );
        assert_eq!(
            m.counter("kraken_hub_stream_parts_total", "type", "q.Find"),
            Some(2)
        );
        assert_eq!(
            m.counter("kraken_hub_requests_total", "mode", "streaming"),
            Some(3)
        );
        assert_eq!(
            m.counter("kraken_hub_requests_total", "outcome", "cancelled"),
            Some(1)
        );
        assert_eq!(
            m.counter("kraken_hub_requests_total", "outcome", "unsupported"),
            Some(1)
        );
        assert_eq!(
            m.counter("kraken_hub_requests_total", "outcome", "ok"),
            Some(1)
        );
        assert_eq!(
            m.counter("kraken_hub_requests_total", "mode", "unary"),
            None
        );
    }

    /// A gather that ends early tells the targets still working for it to stop, when their
    /// session understands `Cancel` (a1 and b1 negotiated streaming, a2 did not).
    #[tokio::test(start_paused = true)]
    async fn a_gather_that_ends_early_cancels_its_capable_targets() {
        let mut f = Streams::new();
        let scatter = |id: &str, timeout_ms: u32, max_results: u32| Request {
            mode: RequestMode::ScatterGather as i32,
            ..stream(id, "q.Find", timeout_ms, 0, max_results)
        };
        let cancels = |f: &mut Streams, h: &str| -> Vec<(String, String)> {
            f.messages(h)
                .iter()
                .filter_map(|m| match m {
                    server_message::Message::Cancel(c) => {
                        Some((c.request_id.clone(), c.reason.clone()))
                    }
                    _ => None,
                })
                .collect()
        };
        // max_results: a2 answers first, the others are cancelled.
        f.router.route(&D, "s1", scatter("g1", 0, 1));
        let a2_target = f.requests("a2")[0].id.clone();
        f.router.on_reply(
            &D,
            "a2",
            Reply {
                request_id: a2_target,
                result: Some(reply::Result::Payload(b"first".to_vec())),
            },
        );
        assert_eq!(f.messages("s1").len(), 2, "a part and the end");
        assert_eq!(
            cancels(&mut f, "a1"),
            [("g1#0".to_string(), "max results".to_string())]
        );
        assert_eq!(
            cancels(&mut f, "b1"),
            [("g1#2".to_string(), "max results".to_string())]
        );
        assert!(f.messages("a2").is_empty(), "a2 cannot understand a cancel");
        // The deadline: every unanswered capable target.
        f.router.route(&D, "s1", scatter("g2", 100, 0));
        tokio::time::sleep(Duration::from_millis(101)).await;
        assert_eq!(end(&f.messages("s1")[0]).timed_out, 3);
        assert_eq!(
            cancels(&mut f, "a1"),
            [("g2#0".to_string(), "deadline".to_string())]
        );
        assert_eq!(
            cancels(&mut f, "b1"),
            [("g2#2".to_string(), "deadline".to_string())]
        );
        assert_eq!(f.messages("a2").len(), 1, "the request only");
        // The sender leaves.
        f.router.route(&D, "s1", scatter("g3", 0, 0));
        f.leave("s1");
        assert_eq!(
            cancels(&mut f, "a1"),
            [("g3#0".to_string(), "sender disconnected".to_string())]
        );
        assert_eq!(
            cancels(&mut f, "b1"),
            [("g3#2".to_string(), "sender disconnected".to_string())]
        );
        assert_eq!(f.messages("a2").len(), 1);
    }

    thread_local! {
        /// The `request_id` of every "request ended with error" line logged on this thread
        /// (a test and its current-thread runtime share one thread).
        static ENDED: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
    }

    /// Records the "request ended with error" lines into [`ENDED`].
    struct EndedLog;

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for EndedLog {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            struct Fields {
                ended: bool,
                request_id: Option<String>,
            }
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    match field.name() {
                        "message" => {
                            self.ended = format!("{value:?}") == "request ended with error";
                        }
                        "request_id" => {
                            self.request_id =
                                Some(format!("{value:?}").trim_matches('"').to_string());
                        }
                        _ => {}
                    }
                }
            }
            let mut fields = Fields {
                ended: false,
                request_id: None,
            };
            event.record(&mut fields);
            if fields.ended
                && let Some(id) = fields.request_id
            {
                ENDED.with_borrow_mut(|lines| lines.push(id));
            }
        }
    }

    /// Installs [`EndedLog`] as the global subscriber (once per test binary: a scoped one is
    /// racy, as any thread's first hit of a callsite rebuilds the global interest cache and the
    /// max level against the global default) and clears this thread's lines.
    fn capture_ended_log() {
        use tracing_subscriber::layer::SubscriberExt;
        static INSTALL: std::sync::Once = std::sync::Once::new();
        INSTALL.call_once(|| {
            let subscriber = tracing_subscriber::registry().with(EndedLog);
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
        ENDED.with_borrow_mut(Vec::clear);
    }

    fn ended_log() -> Vec<String> {
        ENDED.with_borrow(Vec::clone)
    }

    /// The target `<id>#1` failing hub-side (here: its handler left) ends the stream with that
    /// error; it is logged once, as the stream's, not again as the target's.
    #[tokio::test]
    async fn a_hub_side_failure_of_a_forwarded_target_is_logged_once_for_the_stream() {
        capture_ended_log();
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        f.leave(h);
        let got = f.messages("s1");
        assert_eq!(end_error(end(&got[0])).0, ErrorCode::HandlerDisconnected);
        assert_eq!(ended_log(), ["st1"], "one line, the stream's");
    }

    #[tokio::test]
    async fn a_handler_that_leaves_ends_the_stream_with_handler_disconnected() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        f.leave(h);
        let got = f.messages("s1");
        assert_eq!(got.len(), 1, "{got:?}");
        let e = end(&got[0]);
        assert_eq!(e.request_id, "st1");
        assert_eq!(end_error(e).0, ErrorCode::HandlerDisconnected);
        assert_eq!(e.parts, 0);
        assert_eq!(f.router.in_flight(), 0);
        assert!(f.router.in_flight_by_instance().0.is_empty());
    }

    #[tokio::test]
    async fn a_sender_that_leaves_cancels_the_handler() {
        let mut f = Streams::new();
        f.router.route(&D, "s1", stream("st1", "q.Find", 0, 0, 0));
        let (h, _) = f.target_of("st1");
        f.leave("s1");
        let got = f.messages(h);
        assert_eq!(got.len(), 1, "{got:?}");
        let c = cancel(&got[0]);
        assert_eq!(c.request_id, "st1#1");
        assert!(c.reason.contains("sender"), "{}", c.reason);
        assert_eq!(f.router.in_flight(), 0);
        assert!(f.router.in_flight_by_instance().0.is_empty());
        // Late parts and ends are ignored.
        f.part(h, "st1#1", 0, b"late");
        f.end(h, "st1#1", None);
        assert!(f.messages(h).is_empty());
    }
}
