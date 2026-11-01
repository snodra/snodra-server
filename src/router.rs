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
    ErrorCode, Reply, ReplyEnd, ReplyPart, Request, RequestKind, RequestMode, ServerMessage, reply,
    server_message,
};
use crate::protocol;
use crate::queue::{InstanceQueue, Queued, TotalBytes};
use crate::registry::{Candidate, InstanceId, Kind, PickError, Registry, Target};
use crate::session::{SendError, SenderInfo, Sessions};
use crate::telemetry::{Metrics, Outcome, kind_label, set_parent_from_headers};

/// How long a terminal message waits for room in a full sender buffer before it is given up.
const TERMINAL_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Every request the hub has not answered yet, keyed by request id. P4b and P4c add streams and
/// subscriptions.
enum Pending {
    /// A request forwarded to one handler: a unary request, or one target of a scatter-gather.
    Unary(Unary),
    /// A scatter-gather in progress; its targets are `Unary` entries with `part_of` set.
    Scatter(Scatter),
}

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
    /// The sender's outbound channel (weak: it never keeps a closed stream open).
    out: WeakSender<Result<ServerMessage, Status>>,
    /// The delivery still waiting for room in the sender's buffer, if any: later messages of
    /// this gather queue behind it, so the sender sees the parts and the end in order.
    delivery: Option<JoinHandle<()>>,
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

impl Pending {
    fn unary(&self) -> Option<&Unary> {
        match self {
            Pending::Unary(u) => Some(u),
            Pending::Scatter(_) => None,
        }
    }

    fn unary_mut(&mut self) -> Option<&mut Unary> {
        match self {
            Pending::Unary(u) => Some(u),
            Pending::Scatter(_) => None,
        }
    }

    fn sender(&self) -> &str {
        match self {
            Pending::Unary(u) => &u.sender,
            Pending::Scatter(g) => &g.sender,
        }
    }

    fn sender_token(&self) -> Option<u64> {
        match self {
            Pending::Unary(u) => u.sender_token,
            Pending::Scatter(g) => g.sender_token,
        }
    }

    fn context(&self) -> &ContextId {
        match self {
            Pending::Unary(u) => &u.context,
            Pending::Scatter(g) => &g.context,
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

impl Scatter {
    /// Sends `msg` to the sender in this gather's order: at once when nothing of the gather is
    /// still waiting and the buffer has room, otherwise behind the waiting delivery, which waits
    /// up to [`TERMINAL_SEND_TIMEOUT`] for room (as terminal messages do).
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
        let service = self
            .sender_service
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        let instance = self.sender.clone();
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
                    "scatter-gather message not delivered"
                );
                metrics.buffer_full(&service);
            }
        }));
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
        // CANCELLED is allocated for P4b and not produced by this hub yet.
        ErrorCode::HandlerError
        | ErrorCode::Unauthenticated
        | ErrorCode::Unsupported
        | ErrorCode::Cancelled
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
        if let (Some(edges), Some(sender)) = (&self.edges, sender_service)
            && !type_name.is_empty()
        {
            edges.record(
                sender,
                type_name,
                kind,
                outcome != Outcome::Ok,
                latency,
                Instant::now(),
            );
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
    /// its handler's queue if it was still queued, and no longer assigned.
    fn release(&self, id: &str, entry: &Unary) {
        let mut flows = self.flows.lock().unwrap();
        if let Some(f) = flows
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
                if let Err(e) = self.sessions.send_if_current(ctx, handler, token, item.msg) {
                    self.return_permit(handler, token);
                    self.forward_failed(&item.id, handler, service, e);
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
                if let Err(e) = self.sessions.send_if_current(ctx, handler, token, msg) {
                    self.forward_failed(id, handler, &service, e);
                }
            }
            Next::Forward(msg, service) => {
                if let Err(e) = self.sessions.send_if_current(ctx, handler, token, msg) {
                    self.return_permit(handler, token);
                    self.forward_failed(id, handler, &service, e);
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
                    Pending::Scatter(_) => unreachable!("a gather has no handler"),
                }
            }
            (failed, dropped)
        };
        // The sender is gone: nobody to answer, so no outcome metric, but it is no longer in
        // flight. A gather's targets have the same sender, so they are dropped with it.
        for (id, entry) in dropped {
            match entry {
                Pending::Unary(entry) => {
                    entry.abort_timer();
                    self.release(&id, &entry);
                    if entry.part_of.is_none() {
                        self.metrics.in_flight_add(entry.kind, -1);
                    }
                    entry.span.record("kraken.outcome", "sender_disconnected");
                }
                Pending::Scatter(gather) => {
                    gather.cancel.abort();
                    self.metrics.in_flight_add(gather.kind, -1);
                    gather.span.record("kraken.outcome", "sender_disconnected");
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
        log_outcome(
            id,
            &entry.type_name,
            Some(entry.kind),
            &entry.sender,
            Some(&entry.handler),
            outcome,
            false,
        );
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

    /// A request with a mode other than UNARY (ADR 0017). Gates: a v1 sender cannot know the
    /// newer codes (INVALID_REQUEST); a v2 sender gets UNSUPPORTED for a mode this hub does not
    /// implement or its session did not declare; scatter-gather is for queries only
    /// (INVALID_REQUEST); no live target is NO_HANDLER.
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
        let reject = |kind: Kind, code: ErrorCode, outcome: Outcome, message: String| {
            self.record_rejected_mode(sender_service.as_deref(), kind, &type_name, outcome, mode);
            record_outcome(&span, outcome);
            log_outcome(&id, &type_name, Some(kind), sender, None, outcome, false);
            self.send_to(ctx, sender, error_message(&id, code, message));
        };
        let Some(info) = sender_info.filter(|s| protocol::is_v2(s.protocol_version)) else {
            let message = format!("invalid request: request mode {mode} needs protocol v2");
            return reject(
                req_kind,
                ErrorCode::InvalidRequest,
                Outcome::InvalidRequest,
                message,
            );
        };
        if req.mode != RequestMode::ScatterGather as i32 {
            let message = format!("request mode {mode} is not supported by this hub");
            return reject(
                req_kind,
                ErrorCode::Unsupported,
                Outcome::Unsupported,
                message,
            );
        }
        if !info.scatter_gather {
            let message = format!(
                "request mode {mode} needs the \"{}\" capability, not declared by this session",
                protocol::SCATTER_GATHER
            );
            return reject(
                req_kind,
                ErrorCode::Unsupported,
                Outcome::Unsupported,
                message,
            );
        }
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
                out,
                delivery: None,
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

    /// The answer of scatter-gather target `id` (already removed from the map and released):
    /// a `ReplyPart` to the sender, and the `ReplyEnd` when it was the last one needed.
    fn on_part(
        &self,
        id: &str,
        entry: Unary,
        result: Result<Vec<u8>, (crate::proto::Error, Outcome)>,
    ) {
        let Some(part_of) = entry.part_of else {
            return;
        };
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
            gather.deliver(part, &self.metrics);
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
    /// timed out (otherwise `max_results` was reached, or nobody is left).
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
        let timed_out = if deadline { rest.len() as u32 } else { 0 };
        let end = ServerMessage {
            message: Some(server_message::Message::End(ReplyEnd {
                request_id: id.to_string(),
                error: None,
                parts: gather.parts,
                failed: gather.failed,
                timed_out,
            })),
        };
        gather.deliver(end, &self.metrics);
        Some(Closed {
            gather,
            rest,
            timed_out,
        })
    }

    /// Bookkeeping of a closed gather: flow state of its unanswered targets, metrics and edge
    /// (one request; an error when no part succeeded).
    fn finish_scatter(&self, id: &str, closed: Closed) {
        let Closed {
            gather,
            rest,
            timed_out,
        } = closed;
        gather.cancel.abort();
        for (target, entry) in &rest {
            self.release(target, entry);
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
            let token = self.sessions.allocate_token();
            assert!(self.router.open_session(&D, id, token, service, permits));
            let (tx, rx) = mpsc::channel(64);
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
