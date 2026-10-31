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
//! Flow control (ADR 0014): every session has a [`Flow`] keyed by instance id and session token.
//! A flow-controlled session (permits `Some`) is forwarded a request only while it has a permit;
//! further requests wait in its [`InstanceQueue`] (priority, then age) until `Permits` arrive or
//! their own deadline passes. Lock order: the pending lock and the flows lock are never held
//! together.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use tokio::task::AbortHandle;
use tokio::time::Instant;

use crate::config::Config;
use crate::context::ContextId;
use crate::proto::{ErrorCode, Reply, Request, RequestKind, ServerMessage, reply, server_message};
use crate::queue::{InstanceQueue, Queued};
use crate::registry::{Candidate, InstanceId, Kind, PickError, Registry, Target};
use crate::session::{SendError, Sessions};
use crate::telemetry::{Metrics, Outcome, kind_label, set_parent_from_headers};

/// How long a terminal message waits for room in a full sender buffer before it is given up.
const TERMINAL_SEND_TIMEOUT: Duration = Duration::from_secs(5);

struct Pending {
    /// Context of both sender and handler (requests never cross contexts).
    context: ContextId,
    sender: InstanceId,
    /// The sender's session when the request was routed (`None` if it was already gone).
    sender_token: Option<u64>,
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
    cancel: AbortHandle,
    /// The `hub.route` span; closes (and exports) when the entry is dropped.
    span: tracing::Span,
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
    /// `Register.initial_permits`: permits never exceed it, whatever a client sends back.
    initial: u32,
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
        // UNSUPPORTED is declared by protocol v2 but not produced by this hub yet.
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
            config,
            metrics,
        })
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
            initial: permits.unwrap_or(0),
            queue: InstanceQueue::new(
                self.config.queue_capacity_per_instance,
                self.config.queue_bytes_per_instance,
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

    /// `Permits{count}` from session `token`: adds permits and forwards queued requests. Ignored
    /// for an unlimited or replaced session.
    pub fn on_permits(&self, ctx: &ContextId, instance_id: &str, token: u64, count: u32) {
        let (service, ready) = {
            let mut flows = self.flows.lock().unwrap();
            let Some(flow) = flows.get_mut(instance_id).filter(|f| f.token == token) else {
                return;
            };
            let Some(permits) = flow.permits.as_mut() else {
                return;
            };
            *permits = permits.saturating_add(count).min(flow.initial);
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
    fn release(&self, id: &str, entry: &Pending) {
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
            *p = (*p + 1).min(f.initial);
        }
    }

    /// The request `id` is still pending and assigned to `handler`'s session `token`.
    fn is_pending_for(&self, id: &str, handler: &str, token: u64) -> bool {
        self.pending
            .lock()
            .unwrap()
            .get(id)
            .is_some_and(|p| p.handler == handler && p.handler_token == token)
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
                    *p = p.saturating_add(ended.len() as u32).min(f.initial);
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
        let entry = self.pending.lock().unwrap().remove(id);
        if let Some(entry) = entry {
            entry.cancel.abort();
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
                ctx,
                sender,
                error_message(&id, ErrorCode::InvalidRequest, message),
            );
            return;
        };

        let (kind, handler) = match self.pick_handler(ctx, sender, &type_name, &req.routing_key) {
            Ok(picked) => picked,
            Err(e) => {
                let message = no_handler_message(&type_name, e);
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
                self.send_to(
                    ctx,
                    sender,
                    error_message(&id, ErrorCode::NoHandler, message),
                );
                return;
            }
        };

        span.record("kraken.handler", handler.as_str());
        let (sender_token, sender_flow_control) = match self.sessions.sender_info(ctx, sender) {
            Some((token, flow)) => (Some(token), flow),
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
            let entry = Pending {
                context: ctx.clone(),
                sender: sender.to_string(),
                sender_token,
                handler: handler.clone(),
                handler_token,
                sender_flow_control,
                kind,
                type_name,
                started: Instant::now(),
                cancel,
                span,
            };
            pending.insert(id.clone(), entry);
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
        let Some(sender) = self
            .pending
            .lock()
            .unwrap()
            .get(&id)
            .map(|p| p.sender.clone())
        else {
            return; // ended meanwhile
        };
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
            let Some(p) = pending.get_mut(&id) else {
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
            match pending.get(&id) {
                None => {
                    tracing::debug!(request_id = %id, "reply for unknown request ignored");
                    return;
                }
                Some(p) if p.handler != from || p.context != *ctx => {
                    tracing::debug!(request_id = %id, from, "reply from non-handler ignored");
                    return;
                }
                Some(_) => pending.remove(&id).expect("entry checked above"),
            }
        };
        entry.cancel.abort();
        self.release(&id, &entry);
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
        let handled_by = |p: &Pending| p.handler == instance_id && p.handler_token == token;
        let sent_by = |p: &Pending| p.sender == instance_id && p.sender_token == Some(token);
        let (failed, dropped) = {
            let mut pending = self.pending.lock().unwrap();
            let ids: Vec<String> = pending
                .iter()
                .filter(|(_, p)| p.context == *ctx && (handled_by(p) || sent_by(p)))
                .map(|(id, _)| id.clone())
                .collect();
            let mut failed = Vec::new();
            let mut dropped = Vec::new();
            for id in ids {
                let sent = sent_by(&pending[&id]);
                if !sent && waiting_ids.contains(id.as_str()) {
                    continue; // stays pending, routed again below
                }
                let entry = pending.remove(&id).expect("id collected under same lock");
                if sent {
                    dropped.push((id, entry));
                } else {
                    failed.push((id, entry));
                }
            }
            (failed, dropped)
        };
        for (id, entry) in dropped {
            entry.cancel.abort();
            self.release(&id, &entry);
            // The sender is gone: nobody to answer, so no outcome metric, but it is no longer in flight.
            self.metrics.in_flight_add(entry.kind, -1);
            entry.span.record("kraken.outcome", "sender_disconnected");
        }
        for item in waiting {
            self.reroute(ctx, item); // a dropped one is no longer pending: skipped
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
        for p in pending.values() {
            by_instance.entry(p.handler.clone()).or_default().as_handler += 1;
            by_instance.entry(p.sender.clone()).or_default().as_sender += 1;
        }
        (by_instance, pending.len() as u32)
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
            self.release(id, &entry);
            self.finish_with_error(id, entry, code, message);
        }
    }

    /// `entry` must already have been removed from the map (and released) by the caller.
    fn finish_with_error(&self, id: &str, entry: Pending, code: ErrorCode, message: String) {
        self.finish_with_outcome(id, entry, code, message, outcome_of(code));
    }

    fn finish_with_outcome(
        &self,
        id: &str,
        entry: Pending,
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
        self.metrics.request_finished(
            entry.kind,
            &entry.type_name,
            outcome,
            entry.started.elapsed(),
        );
        self.send_to(
            &entry.context,
            &entry.sender,
            error_message(id, code, message),
        );
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
            priority: 0,
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
        f.router.on_permits(&D, "h1", f.handler_token, 1);
        assert_eq!(forwarded(&mut f.handler_rx), ["r2"]);
        f.router.on_permits(&D, "h1", f.handler_token, 1);
        assert_eq!(forwarded(&mut f.handler_rx), ["r3"]);
        assert_eq!(f.router.queued_total(), 0);
        f.router.on_permits(&D, "h1", f.handler_token, 1);
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
            f.router.on_permits(&D, "h1", f.handler_token, 1);
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
        f.router.on_permits(&D, "h1", f.handler_token, 1);
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
        f.router.on_permits(&D, "h1", f.handler_token, 3);
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
            let config = Config::default();
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
            self.router.on_permits(&D, id, self.handlers[id].1, 1);
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
        f.router.on_permits(&D, "h1", f.handler_token + 1000, 5);
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
        f.router.on_permits(&D, "h1", f.handler_token, 5);
        let view = f.router.flow_by_instance().remove("h1").unwrap();
        assert_eq!(view.permits_available, 2);
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
        f.router.on_permits(&D, "h1", f.handler_token, 1);
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
        f.router.on_permits(&D, "h1", f.handler_token, 1);
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
}
