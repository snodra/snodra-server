//! gRPC service implementation: the `Hub.Connect` stream.
//!
//! Each connected stream is driven by one task (`run_stream`). The task owns the inbound half;
//! the outbound half is the receiver of a per-session channel whose only long-lived sender is the
//! one stored in [`Sessions`]. Every session carries a [`CancellationToken`]: removing or
//! replacing the session cancels it, and the task exits at once, without waiting for the client
//! to drain the response body (a stalled client may never do that). Dropping the response body
//! (client went away) cancels the same token.
//!
//! Lock order is sessions -> registry, never the reverse: registry registration and
//! unregistration for an instance run inside [`Sessions::with_current`] /
//! [`Sessions::remove_if_then`], so only the stream that currently owns an instance id can change
//! its registration.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::auth::{
    AuthMode, Authorizer, CredentialKind, Denied, Disposition, Expiry, Principal, Refresh,
    RefreshGate,
};
use crate::config::Config;
use crate::context::ContextId;
use crate::mtls::PeerCertificate;
use crate::proto::hub_server::Hub;
use crate::proto::{
    ClientMessage, Heartbeat, Register, ServerMessage, TopologyRequest, TopologyResponse,
    client_message, server_message,
};
use crate::protocol;
use crate::registry::{Placement, RegisterError, RegisteredTypes, Registry, effective_load_factor};
use crate::router::{OpenRefused, Router};
use crate::session::{SessionMeta, Sessions, Superseded};
use crate::topology::{ClockAnchor, snapshot};

/// How long a credentials refresh may wait for a rotated signing key to be fetched. The stream
/// loop also answers heartbeats (timeout 10 s by default) and the fetch itself may take 10 s.
const REFRESH_KEY_WAIT: Duration = Duration::from_millis(1500);

pub struct HubService {
    pub registry: Arc<Registry>,
    pub sessions: Arc<Sessions>,
    pub router: Arc<Router>,
    pub config: Arc<Config>,
    /// Access control for `Register`; the router holds the same one for requests.
    pub auth: Arc<Authorizer>,
    /// Anchor for converting monotonic instants to wall-clock time in topology snapshots.
    pub anchor: ClockAnchor,
    /// When this hub started serving (monotonic).
    pub started: Instant,
    pub heartbeat_timeout: Duration,
}

type ServerStream = Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;
type Outbound = mpsc::Sender<Result<ServerMessage, Status>>;

/// The one disconnect sequence: stream end, inbound error, rejected register, watchdog eviction
/// and shutdown all go through here. It only acts if the session identified by `token` is still
/// the registered one, so a late disconnect of an old stream never touches a newer stream that
/// reused the instance id.
///
/// Removing the session cancels it (the stream task exits) and drops its sender (the client's
/// inbound stream ends once drained). The registry unregister runs under the same sessions lock,
/// so no newer stream for the id can register in between and be unregistered by this one.
/// `router.on_disconnect` runs right after the lock is released, because it sends terminal
/// errors through [`Sessions`] (whose mutex is not reentrant).
pub fn disconnect(
    registry: &Registry,
    sessions: &Sessions,
    router: &Router,
    ctx: &ContextId,
    instance_id: &str,
    token: u64,
) -> bool {
    let Some(service) = sessions.remove_if_then(ctx, instance_id, token, |session| {
        registry.unregister(ctx, instance_id, Instant::now());
        session.service
    }) else {
        return false;
    };
    router.on_disconnect(ctx, instance_id, token);
    router.refresh_gauges();
    info!(instance = %instance_id, service = %service, "disconnect");
    true
}

/// Re-judges every session against the current ACL after a reload. In `enforce`, a
/// session whose token is gone or whose registration is no longer allowed is closed with the
/// matching status (`UNAUTHENTICATED` or `PERMISSION_DENIED`) and its pending requests end as on
/// any disconnect; in `permissive` the violation is only logged. Returns the sessions closed.
pub fn reevaluate_sessions(
    auth: &Authorizer,
    registry: &Registry,
    sessions: &Sessions,
    router: &Router,
) -> usize {
    if auth.mode() == AuthMode::Off {
        return 0;
    }
    let mut closed = 0;
    for view in sessions.auth_views() {
        let denials = auth.recheck(view.principal.as_deref(), &view.service, &view.handled);
        let Some(first) = denials.first() else {
            continue;
        };
        if auth.mode() != AuthMode::Enforce {
            auth.report(
                &denials,
                &view.service,
                view.principal.as_deref(),
                Disposition::Kept,
            );
            continue;
        }
        auth.report(
            &denials,
            &view.service,
            view.principal.as_deref(),
            Disposition::Closed,
        );
        if let Some(tx) = sessions.sender(&view.context, &view.instance_id) {
            let _ = tx.try_send(Err(first.to_status()));
        }
        if disconnect(
            registry,
            sessions,
            router,
            &view.context,
            &view.instance_id,
            view.token,
        ) {
            closed += 1;
        }
    }
    closed
}

/// Every type a `Register` handles: commands, queries and shared queries.
fn handled_types(reg: &Register) -> Vec<String> {
    reg.command_types
        .iter()
        .chain(&reg.query_types)
        .chain(&reg.shared_query_types)
        .cloned()
        .collect()
}

/// Response stream; dropping it (the client went away, or hyper finished the body) cancels the
/// stream task.
struct ResponseStream {
    inner: ReceiverStream<Result<ServerMessage, Status>>,
    _cancel_on_drop: DropGuard,
}

impl Stream for ResponseStream {
    type Item = Result<ServerMessage, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

#[tonic::async_trait]
impl Hub for HubService {
    type ConnectStream = ServerStream;

    async fn connect(
        &self,
        request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        // The client certificate of the connection, when there is one.
        let peer = request
            .extensions()
            .get::<PeerCertificate>()
            .and_then(|p| p.0.clone());
        let inbound = request.into_inner();
        let mut buffer = self.config.session_buffer;
        if buffer == 0 {
            warn!("session_buffer is 0; using 1");
            buffer = 1;
        }
        let (tx, rx) = mpsc::channel(buffer);
        let cancel = CancellationToken::new();
        info!("connect");

        let ctx = StreamCtx {
            registry: Arc::clone(&self.registry),
            sessions: Arc::clone(&self.sessions),
            router: Arc::clone(&self.router),
            config: Arc::clone(&self.config),
            auth: Arc::clone(&self.auth),
            context: ContextId::default(),
        };
        tokio::spawn(run_stream(ctx, inbound, peer, tx, cancel.clone()));

        let stream = ResponseStream {
            inner: ReceiverStream::new(rx),
            _cancel_on_drop: cancel.drop_guard(),
        };
        Ok(Response::new(Box::pin(stream) as ServerStream))
    }

    async fn topology(
        &self,
        request: Request<TopologyRequest>,
    ) -> Result<Response<TopologyResponse>, Status> {
        // The same data as `GET /api/v1/topology`, so the same dashboard token (the CLI sends it
        // as metadata); in permissive and enforce mode an ACL token reads it too.
        let presented = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(crate::auth::bearer);
        // A service's JWT reads it like an ACL token; its rotated key is fetched first.
        self.auth.prepare_bearer(presented).await;
        // A client certificate of the connection counts too.
        let peer = request.extensions().get::<PeerCertificate>();
        self.auth.check_topology_peer(
            presented,
            peer.and_then(PeerCertificate::der),
            self.config.dashboard_token.as_ref(),
        )?;
        debug!("topology requested");
        let topology = snapshot(
            &self.registry,
            &self.sessions,
            &self.router,
            self.anchor,
            self.started,
            &self.config.hub_id,
            self.heartbeat_timeout,
            Instant::now(),
        );
        Ok(Response::new(topology.to_proto()))
    }
}

struct StreamCtx {
    registry: Arc<Registry>,
    sessions: Arc<Sessions>,
    router: Arc<Router>,
    config: Arc<Config>,
    auth: Arc<Authorizer>,
    /// Every stream lives in the default context (contexts are deferred; the API keeps the door open).
    context: ContextId,
}

impl StreamCtx {
    /// Registers `reg` while the session `token` is provably the current one for its instance
    /// id. `None` means the session was removed or replaced meanwhile (nothing was changed).
    /// `placement` is the session's (from its first `Register`), not `reg`'s.
    fn register(
        &self,
        token: u64,
        reg: &Register,
        placement: &Placement,
    ) -> Option<Result<(), Status>> {
        let result = self
            .sessions
            .with_current(&self.context, &reg.instance_id, token, || {
                self.registry.register_types(
                    &self.context,
                    &reg.instance_id,
                    &reg.service_name,
                    &RegisteredTypes {
                        command_types: reg.command_types.clone(),
                        query_types: reg.query_types.clone(),
                        shared_query_types: reg.shared_query_types.clone(),
                    },
                    placement.clone(),
                )
            })?;
        Some(match result {
            Ok(()) => {
                self.sessions.set_handled(
                    &self.context,
                    &reg.instance_id,
                    token,
                    handled_types(reg),
                );
                // Never log `reg` itself: it may carry credentials.
                info!(
                    service = %reg.service_name,
                    instance = %reg.instance_id,
                    commands = reg.command_types.len(),
                    queries = reg.query_types.len(),
                    shared_queries = reg.shared_query_types.len(),
                    client_version = %reg.client_version,
                    "register"
                );
                Ok(())
            }
            Err(e) => {
                warn!(error = %e, service = %reg.service_name, instance = %reg.instance_id, "register rejected");
                Err(match e {
                    RegisterError::DuplicateTypeAcrossKinds { .. }
                    | RegisterError::SharedAndExclusive { .. } => {
                        Status::invalid_argument(e.to_string())
                    }
                    RegisterError::TypeOwnedByOtherService { .. }
                    | RegisterError::KindConflict { .. }
                    | RegisterError::SharingConflict { .. } => {
                        Status::already_exists(e.to_string())
                    }
                })
            }
        })
    }

    /// Judges a registration once more after it was recorded, in `enforce` mode: the status to
    /// close the session with when the current ACL no longer allows it, already reported and
    /// counted. A reload that ran between the first judgement and the recording could not judge
    /// types that were not recorded yet (it uses the session's recorded types), so every path
    /// that records a registration, the first `Register` and each re-register, calls this after.
    fn judge_again(
        &self,
        principal: Option<&Principal>,
        service: &str,
        handled: &[String],
    ) -> Option<Status> {
        if self.auth.mode() != AuthMode::Enforce {
            return None;
        }
        let late = self.auth.recheck(principal, service, handled);
        let first = late.first()?;
        self.auth
            .report(&late, service, principal, Disposition::Closed);
        Some(first.to_status())
    }

    fn disconnect(&self, instance_id: &str, token: u64) -> bool {
        disconnect(
            &self.registry,
            &self.sessions,
            &self.router,
            &self.context,
            instance_id,
            token,
        )
    }
}

/// Points `timer` at the expiry of `principal` (`exp` plus the grace); false, and nothing to
/// wait for, when it never expires.
fn arm_expiry(
    auth: &Authorizer,
    principal: Option<&Principal>,
    timer: Pin<&mut tokio::time::Sleep>,
) -> bool {
    match auth.expiry_deadline(principal) {
        Some(deadline) => {
            timer.reset(deadline);
            true
        }
        None => false,
    }
}

/// Closes a handshake that failed access control: the status goes to the client and the stream
/// ends. No session exists yet. The authorizer has already logged and counted the refusal (once
/// per `Register`, rate-limited), so nothing is logged here.
async fn refuse(tx: &Outbound, denied: &Denied) {
    let _ = tx.send(Err(denied.to_status())).await;
}

async fn run_stream(
    ctx: StreamCtx,
    mut inbound: Streaming<ClientMessage>,
    peer: Option<Arc<[u8]>>,
    tx: Outbound,
    cancel: CancellationToken,
) {
    // Handshake: the first message must be a Register, within the handshake timeout. No session
    // state exists yet, so every failure here just reports the status and ends the stream.
    let handshake_timeout = ctx.config.handshake_timeout;
    let first = tokio::select! {
        biased;
        _ = cancel.cancelled() => return,
        first = tokio::time::timeout(handshake_timeout, inbound.next()) => first,
    };
    let first = match first {
        Err(_elapsed) => {
            warn!(timeout = ?handshake_timeout, "no Register within handshake timeout");
            let _ = tx
                .send(Err(Status::deadline_exceeded(format!(
                    "no Register within {:?}",
                    handshake_timeout
                ))))
                .await;
            return;
        }
        Ok(Some(Ok(ClientMessage {
            message: Some(client_message::Message::Register(reg)),
        }))) => reg,
        Ok(other) => {
            if let Some(Err(status)) = &other {
                warn!(%status, "client stream error before register");
            } else {
                warn!("first message was not Register");
            }
            let _ = tx
                .send(Err(Status::failed_precondition(
                    "first message must be Register",
                )))
                .await;
            return;
        }
    };
    if first.instance_id.is_empty() || first.service_name.is_empty() {
        warn!(service = %crate::auth::clip(&first.service_name), instance = %crate::auth::clip(&first.instance_id), "register with empty identity");
        let _ = tx
            .send(Err(Status::invalid_argument(
                "Register.instance_id and Register.service_name must not be empty",
            )))
            .await;
        return;
    }
    // The hub's own request sessions are `snodra-server/<uuid>`: a client's id must not
    // be able to collide with one.
    if first
        .instance_id
        .starts_with(crate::router::HUB_INSTANCE_PREFIX)
    {
        warn!(service = %crate::auth::clip(&first.service_name), instance = %crate::auth::clip(&first.instance_id), "register with the hub's reserved instance prefix");
        let _ = tx
            .send(Err(Status::invalid_argument(format!(
                "Register.instance_id must not start with {}: the hub's own requests use it",
                crate::router::HUB_INSTANCE_PREFIX
            ))))
            .await;
        return;
    }
    // ...and the hub sends them as service `snodra-server`: no client may claim that name.
    if first.service_name == crate::router::HUB_SERVICE {
        warn!(service = %crate::auth::clip(&first.service_name), instance = %crate::auth::clip(&first.instance_id), "register with the hub's own service name");
        let _ = tx
            .send(Err(Status::invalid_argument(format!(
                "Register.service_name must not be {}: the hub's own requests use it",
                crate::router::HUB_SERVICE
            ))))
            .await;
        return;
    }
    if first.initial_permits == 0 {
        warn!(service = %crate::auth::clip(&first.service_name), instance = %crate::auth::clip(&first.instance_id), "register without permits");
        let _ = tx
            .send(Err(Status::invalid_argument(
                "Register.initial_permits must be at least 1",
            )))
            .await;
        return;
    }
    let instance_id = first.instance_id.clone();
    let service = first.service_name.clone();
    // Access control: the credentials and the claimed service and types are judged
    // before any session state exists. Like the client version, the permits and the placement,
    // the credentials are read from the first Register only; a later one cannot change who the
    // session is.
    let mut handled = handled_types(&first);
    // A rotated signing key is fetched before the credentials are judged.
    ctx.auth.prepare(first.credentials.as_ref()).await;
    let admission = ctx.auth.admit_peer(
        peer.as_deref(),
        first.credentials.as_ref(),
        &service,
        &handled,
    );
    if let Some(denied) = &admission.refused {
        refuse(&tx, denied).await;
        return;
    }
    let mut principal = admission.principal;
    let ack = protocol::register_ack(&ctx.config, &ctx.context);
    // So is the placement.
    let placement = Placement {
        load_factor: effective_load_factor(first.load_factor),
        tags: first
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    };
    let meta = SessionMeta {
        client_version: first.client_version.clone(),
        placement: placement.clone(),
        principal: principal.clone(),
        auth: admission.status.clone(),
    };
    // Flow state, then the session, then the registration under it: a request routed
    // to the session always finds its flow, and a late disconnect of an older stream with the
    // same instance id can never unregister this stream's registration. Tokens grow per stream,
    // so of two concurrent handshakes of one instance id the newer wins in both maps.
    let token = ctx.sessions.allocate_token();
    match ctx.router.open_session(
        &ctx.context,
        &instance_id,
        token,
        &service,
        first.initial_permits,
    ) {
        Ok(()) => {}
        Err(OpenRefused::Superseded) => {
            debug!(instance = %instance_id, "superseded by a newer stream during handshake");
            return;
        }
        Err(OpenRefused::OtherService) => {
            // Nothing was changed: the holder keeps its session, its queue and its pending
            // entries. Refused in `permissive` too.
            let denied = ctx.auth.instance_takeover(&service, &instance_id);
            refuse(&tx, &denied).await;
            return;
        }
    }
    let replaced = match ctx.sessions.insert_with_token(
        &ctx.context,
        &instance_id,
        &service,
        meta,
        tx.clone(),
        cancel.clone(),
        token,
    ) {
        Ok(replaced) => replaced,
        Err(Superseded) => {
            // The newer stream opened its flow after ours (replacing it); nothing to undo, but
            // end anything that may still be keyed by our token.
            ctx.router.on_disconnect(&ctx.context, &instance_id, token);
            debug!(instance = %instance_id, "superseded by a newer stream during handshake");
            return;
        }
    };
    // The replaced stream's task sees itself replaced and skips the disconnect sequence, so its
    // requests are ended here: nobody can answer requests forwarded to the old stream.
    if let Some(old) = replaced {
        ctx.router.on_disconnect(&ctx.context, &instance_id, old);
    }
    match ctx.register(token, &first, &placement) {
        Some(Ok(())) => {}
        Some(Err(status)) => {
            // The replaced session's registration (if any) may still be in the registry; the
            // full disconnect removes it along with this session.
            ctx.disconnect(&instance_id, token);
            // Best effort: the buffer may hold requests routed to this id meanwhile.
            let _ = tx.try_send(Err(status));
            return;
        }
        None => {
            debug!(instance = %instance_id, "session replaced during handshake");
            return;
        }
    }
    // A reload may have run between the check above and the insert, when no session existed for
    // it to re-judge; judge once more now that the session and its types are recorded.
    if let Some(status) = ctx.judge_again(principal.as_deref(), &service, &handled) {
        ctx.disconnect(&instance_id, token);
        let _ = tx.try_send(Err(status));
        return;
    }
    // The ack is queued before this task reads the next inbound message, so it precedes the echo
    // of the heartbeat that follows the Register (the stream's messages are handled in order):
    // the client waits for it. Requests routed to this instance meanwhile may come first;
    // clients handle them anyway.
    let sent = tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        sent = tx.send(Ok(ack.clone())) => sent.is_ok(),
    };
    if !sent {
        ctx.disconnect(&instance_id, token);
        return;
    }
    ctx.router.refresh_gauges();
    // Requests taken over from a replaced session go out now that the ack is queued.
    ctx.router.start_session(&ctx.context, &instance_id, token);
    // From here on the sessions map holds the only strong sender, so removing the session ends
    // the outbound stream; this task keeps a weak handle for its own replies.
    let out = tx.downgrade();
    drop(tx);

    // A JWT session is judged expired at `exp` plus the grace unless a refresh moved it. One
    // timer per stream, re-armed by a refresh: no sweeper, and a token session or a hub in `off`
    // never arms it.
    let expiry = tokio::time::sleep(Duration::ZERO);
    tokio::pin!(expiry);
    let mut expiry_armed = arm_expiry(&ctx.auth, principal.as_deref(), expiry.as_mut());
    let mut refresh_gate = RefreshGate::default();

    loop {
        let msg = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = &mut expiry, if expiry_armed => {
                // Asked once: permissive counts and keeps the session, enforce closes it. Not
                // expired by the wall clock yet: the timer is armed again.
                expiry_armed = false;
                match principal.as_deref().map(|p| ctx.auth.expired(p, &service)) {
                    Some(Expiry::Close(denied)) => {
                        if let Some(tx) = out.upgrade() {
                            let _ = tx.try_send(Err(denied.to_status()));
                        }
                        break;
                    }
                    Some(Expiry::NotYet) => {
                        expiry_armed =
                            arm_expiry(&ctx.auth, principal.as_deref(), expiry.as_mut());
                    }
                    Some(Expiry::Keep) | None => {}
                }
                continue;
            }
            msg = inbound.next() => msg,
        };
        let msg = match msg {
            None => break,
            Some(Err(status)) => {
                warn!(%status, instance = %instance_id, "client stream error");
                break;
            }
            Some(Ok(m)) => m,
        };
        match msg.message {
            Some(client_message::Message::Register(mut reg)) => {
                if reg.instance_id != instance_id || reg.service_name != service {
                    warn!(instance = %instance_id, "re-register changed identity; keeping original");
                    reg.instance_id = instance_id.clone();
                    reg.service_name = service.clone();
                }
                // A re-register may add types: they are judged against the current ACL too.
                let denials =
                    ctx.auth
                        .recheck(principal.as_deref(), &service, &handled_types(&reg));
                if let Some(first) = denials.first() {
                    if ctx.auth.mode() == AuthMode::Enforce {
                        ctx.auth.report(
                            &denials,
                            &service,
                            principal.as_deref(),
                            Disposition::Closed,
                        );
                        if let Some(tx) = out.upgrade() {
                            let _ = tx.try_send(Err(first.to_status()));
                        }
                        break;
                    }
                    ctx.auth.report(
                        &denials,
                        &service,
                        principal.as_deref(),
                        Disposition::Allowed,
                    );
                }
                match ctx.register(token, &reg, &placement) {
                    Some(Ok(())) => {
                        // The same late judgement as after the first `Register`: a reload may
                        // have overtaken the check above (the recorded types it judged were the
                        // previous ones).
                        if let Some(status) =
                            ctx.judge_again(principal.as_deref(), &service, &handled_types(&reg))
                        {
                            if let Some(tx) = out.upgrade() {
                                let _ = tx.try_send(Err(status));
                            }
                            break;
                        }
                        handled = handled_types(&reg);
                        // No types left: a graceful stop. Its queue goes to other instances.
                        if reg.command_types.is_empty()
                            && reg.query_types.is_empty()
                            && reg.shared_query_types.is_empty()
                        {
                            ctx.router.on_stopping(&ctx.context, &instance_id, token);
                        }
                        ctx.router.refresh_gauges();
                        // Best effort like the heartbeat echo: a full buffer drops this ack.
                        if let Some(tx) = out.upgrade()
                            && let Err(mpsc::error::TrySendError::Full(_)) =
                                tx.try_send(Ok(ack.clone()))
                        {
                            debug!(instance = %instance_id, "outbound buffer full, re-register ack dropped");
                        }
                    }
                    Some(Err(status)) => {
                        // Best effort: a full buffer must not keep this task alive.
                        if let Some(tx) = out.upgrade() {
                            let _ = tx.try_send(Err(status));
                        }
                        break;
                    }
                    None => break,
                }
            }
            Some(client_message::Message::Heartbeat(hb)) => {
                ctx.sessions
                    .touch(&ctx.context, &instance_id, Instant::now());
                let Some(tx) = out.upgrade() else { break };
                let echo = ServerMessage {
                    message: Some(server_message::Message::Heartbeat(Heartbeat {
                        sent_at_ms: hb.sent_at_ms,
                    })),
                };
                match tx.try_send(Ok(echo)) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        debug!(instance = %instance_id, "outbound buffer full, heartbeat echo dropped");
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => break,
                }
            }
            Some(client_message::Message::Request(req)) => {
                ctx.router.route(&ctx.context, &instance_id, req)
            }
            Some(client_message::Message::Reply(reply)) => {
                ctx.router
                    .on_reply_from(&ctx.context, &instance_id, token, reply)
            }
            Some(client_message::Message::Permits(p)) => {
                ctx.router
                    .on_permits(&ctx.context, &instance_id, token, p.count, p.grant)
            }
            Some(client_message::Message::Status(status)) => {
                ctx.sessions
                    .set_status(&ctx.context, &instance_id, token, &status, Instant::now());
            }
            // Multi-part messages from a client: a handler's streamed parts and end (or
            // pushed updates), a sender's credits and cancellation.
            Some(client_message::Message::Part(part)) => {
                ctx.router
                    .on_stream_part_from(&ctx.context, &instance_id, token, part)
            }
            Some(client_message::Message::End(end)) => {
                ctx.router
                    .on_stream_end_from(&ctx.context, &instance_id, token, end)
            }
            Some(client_message::Message::Flow(flow)) => {
                ctx.router
                    .on_flow_from(&ctx.context, &instance_id, token, flow)
            }
            Some(client_message::Message::Cancel(cancel)) => {
                ctx.router
                    .on_cancel_from(&ctx.context, &instance_id, token, cancel)
            }
            Some(client_message::Message::ControlReply(reply)) => {
                ctx.router
                    .control()
                    .on_reply(&ctx.context, &instance_id, token, reply)
            }
            Some(client_message::Message::Credentials(new)) => {
                // `off` checks nothing, so there is nothing to refresh.
                if ctx.auth.mode() == AuthMode::Off {
                    debug!(instance = %instance_id, "credentials refresh ignored: access control is off");
                    continue;
                }
                // A TLS session cannot be refreshed on a live stream: the certificate is the
                // principal until the connection ends, and no message replaces it. It
                // costs no verification, so it needs no throttle.
                if principal
                    .as_deref()
                    .is_some_and(|p| p.kind() == CredentialKind::Certificate)
                {
                    ctx.auth.refresh_ignored_for_certificate();
                    debug!(instance = %instance_id, "credentials refresh ignored: the session is authenticated by its certificate");
                    continue;
                }
                // One refresh per second is looked at: verifying a signature is work a client
                // must not be able to ask for in a loop.
                if !refresh_gate.allow(Instant::now()) {
                    ctx.auth.refresh_throttled();
                    debug!(instance = %instance_id, "credentials refresh throttled");
                    continue;
                }
                // The loop answers heartbeats too: waiting here for a slow key route would get
                // the session evicted. A key not known after the bound is judged as not known
                // yet (the session keeps its credentials); the fetch goes on without us.
                ctx.auth.prepare_within(Some(&new), REFRESH_KEY_WAIT).await;
                match ctx.auth.refresh(Some(&new), &service, &handled) {
                    Refresh::Accepted {
                        principal: fresh,
                        status,
                    } => {
                        principal = Some(fresh);
                        ctx.sessions.set_credentials(
                            &ctx.context,
                            &instance_id,
                            token,
                            principal.clone(),
                            status,
                        );
                        expiry_armed = arm_expiry(&ctx.auth, principal.as_deref(), expiry.as_mut());
                        // A reload that ran while the refresh was judged saw the old principal.
                        if let Some(status) =
                            ctx.judge_again(principal.as_deref(), &service, &handled)
                        {
                            if let Some(tx) = out.upgrade() {
                                let _ = tx.try_send(Err(status));
                            }
                            break;
                        }
                    }
                    Refresh::Kept => {}
                    Refresh::Close(denied) => {
                        if let Some(tx) = out.upgrade() {
                            let _ = tx.try_send(Err(denied.to_status()));
                        }
                        break;
                    }
                }
            }
            None => {}
        }
    }
    ctx.disconnect(&instance_id, token);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Acl, AuthStatus, Principal, digest_of, hash_token};
    use crate::session::SessionMeta;
    use crate::telemetry::test_support::test_metrics;

    fn acl(service: &str, handle: &str) -> Acl {
        Acl::parse(&format!(
            "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"{service}\"\nhandle = [\"{handle}\"]\n",
            hash_token("tok")
        ))
        .unwrap()
    }

    /// One session of `orders` handling `orders.Place`, registered with the token `tok`.
    fn session(sessions: &Sessions) -> mpsc::Receiver<Result<ServerMessage, Status>> {
        let ctx = ContextId::default();
        let (tx, rx) = mpsc::channel(4);
        let meta = SessionMeta {
            principal: Some(Arc::new(Principal::token(digest_of("tok"), "orders"))),
            auth: AuthStatus::Authenticated {
                service: "orders".into(),
            },
            ..SessionMeta::default()
        };
        let token = sessions.insert(&ctx, "i1", "orders", meta, tx, CancellationToken::new());
        sessions.set_handled(&ctx, "i1", token, vec!["orders.Place".into()]);
        rx
    }

    fn parts(
        mode: AuthMode,
        acl: Acl,
    ) -> (
        Arc<Authorizer>,
        Arc<Registry>,
        Arc<Sessions>,
        Arc<Router>,
        crate::telemetry::test_support::TestMetrics,
    ) {
        let m = test_metrics();
        let auth = Authorizer::with_acl(mode, acl, m.metrics.clone());
        let registry = Arc::new(Registry::new(Duration::from_secs(60)));
        let sessions = Arc::new(Sessions::new());
        let router = Router::with_auth(
            Arc::clone(&registry),
            Arc::clone(&sessions),
            Arc::new(Config::default()),
            m.metrics.clone(),
            Arc::clone(&auth),
        );
        (auth, registry, sessions, router, m)
    }

    #[tokio::test]
    async fn a_reload_closes_sessions_the_acl_no_longer_allows_and_counts_them() {
        let (auth, registry, sessions, router, m) =
            parts(AuthMode::Enforce, acl("orders", "orders.*"));
        let mut rx = session(&sessions);
        // Unchanged rules: nothing is closed.
        assert_eq!(reevaluate_sessions(&auth, &registry, &sessions, &router), 0);
        assert!(rx.try_recv().is_err());

        // The reload narrows `handle`: the session is closed with PERMISSION_DENIED.
        let narrowed = Authorizer::with_acl(
            AuthMode::Enforce,
            acl("orders", "orders.Get"),
            m.metrics.clone(),
        );
        assert_eq!(
            reevaluate_sessions(&narrowed, &registry, &sessions, &router),
            1
        );
        let status = rx.try_recv().expect("a closing status").unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(status.message().contains("orders.Place"));
        assert_eq!(
            m.counter(
                "snodra_server_auth_violations_total",
                "reason",
                "handle_denied"
            ),
            Some(1)
        );
        assert!(sessions.auth_views().is_empty(), "the session is gone");

        // A removed token: UNAUTHENTICATED, counted as token_revoked.
        let mut rx = session(&sessions);
        let empty = Authorizer::with_acl(AuthMode::Enforce, Acl::default(), m.metrics.clone());
        assert_eq!(
            reevaluate_sessions(&empty, &registry, &sessions, &router),
            1
        );
        assert_eq!(
            rx.try_recv().unwrap().unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            m.counter(
                "snodra_server_auth_violations_total",
                "reason",
                "token_revoked"
            ),
            Some(1)
        );
    }

    /// A `StreamCtx` over `parts`-like state, for the registration interleaving tests.
    fn stream_ctx(
        auth: &Arc<Authorizer>,
        registry: &Arc<Registry>,
        sessions: &Arc<Sessions>,
        router: &Arc<Router>,
    ) -> StreamCtx {
        StreamCtx {
            registry: Arc::clone(registry),
            sessions: Arc::clone(sessions),
            router: Arc::clone(router),
            config: Arc::new(Config::default()),
            auth: Arc::clone(auth),
            context: ContextId::default(),
        }
    }

    /// The re-register of review finding 4, step by step. The stream judges the new types against
    /// the ACL, a reload then narrows the ACL, and only afterwards does the stream record the
    /// registration. The reload judged the session by its old types (the new ones were not
    /// recorded yet), so it kept the session; the late judgement after `register` must close it.
    #[tokio::test]
    async fn a_re_register_that_a_reload_overtook_is_judged_again_once_recorded() {
        let dir = std::env::temp_dir().join(format!("snodra-server-race-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("acl.toml");
        let entry = |handle: &str| {
            format!(
                "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"orders\"\nhandle = [{handle}]\n",
                hash_token("tok")
            )
        };
        std::fs::write(&file, entry("\"orders.*\"")).unwrap();
        let m = test_metrics();
        let config = Config {
            auth_mode: AuthMode::Enforce,
            acl_file: Some(file.clone()),
            ..Config::default()
        };
        let auth = Authorizer::from_config(&config, m.metrics.clone()).unwrap();
        let registry = Arc::new(Registry::new(Duration::from_secs(60)));
        let sessions = Arc::new(Sessions::new());
        let router = Router::with_auth(
            Arc::clone(&registry),
            Arc::clone(&sessions),
            Arc::new(Config::default()),
            m.metrics.clone(),
            Arc::clone(&auth),
        );
        let ctx = stream_ctx(&auth, &registry, &sessions, &router);
        let principal = Principal::token(digest_of("tok"), "orders");

        // The session registered with orders.Place.
        let (tx, mut rx) = mpsc::channel(4);
        let meta = SessionMeta {
            principal: Some(Arc::new(principal.clone())),
            ..SessionMeta::default()
        };
        let token = sessions.insert(
            &ctx.context,
            "i1",
            "orders",
            meta,
            tx,
            CancellationToken::new(),
        );
        let first = Register {
            instance_id: "i1".into(),
            service_name: "orders".into(),
            command_types: vec!["orders.Place".into()],
            ..Register::default()
        };
        assert!(matches!(
            ctx.register(token, &first, &Placement::default()),
            Some(Ok(()))
        ));

        // Step 1: a re-register adds orders.Cancel; the ACL of the moment allows it.
        let second = Register {
            command_types: vec!["orders.Place".into(), "orders.Cancel".into()],
            ..first.clone()
        };
        assert!(
            auth.recheck(Some(&principal), "orders", &handled_types(&second))
                .is_empty(),
            "allowed by the ACL as it is"
        );
        // Step 2: the ACL is narrowed and reloaded; the reload judges the session by the types
        // it has recorded so far (orders.Place), which are still allowed.
        std::fs::write(&file, entry("\"orders.Place\"")).unwrap();
        assert!(auth.reload());
        assert_eq!(reevaluate_sessions(&auth, &registry, &sessions, &router), 0);
        assert!(rx.try_recv().is_err());
        // Step 3: the stream records the registration it judged in step 1.
        assert!(matches!(
            ctx.register(token, &second, &Placement::default()),
            Some(Ok(()))
        ));
        // Step 4: the late judgement sees what the reload could not.
        let status = ctx
            .judge_again(Some(&principal), "orders", &handled_types(&second))
            .expect("the registration is no longer allowed");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert!(
            status.message().contains("orders.Cancel"),
            "{}",
            status.message()
        );
        assert_eq!(
            m.counter(
                "snodra_server_auth_violations_total",
                "reason",
                "handle_denied"
            ),
            Some(1)
        );
        // And what the ACL allows is not judged at all.
        assert!(
            ctx.judge_again(Some(&principal), "orders", &handled_types(&first))
                .is_none()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn closing_a_refused_handshake_adds_no_log_line_of_its_own() {
        // The authorizer logged the refusal once (review finding 6); the stream only reports the
        // status.
        crate::telemetry::test_support::capture_logs();
        let m = test_metrics();
        let auth = Authorizer::with_acl(AuthMode::Enforce, Acl::default(), m.metrics.clone());
        let denied = auth.admit(None, "orders", &[]).refused.expect("refused");
        let logged_by_admit = crate::telemetry::test_support::logged()
            .iter()
            .filter(|l| l.field("reason").is_some())
            .count();
        assert_eq!(logged_by_admit, 1);
        let (tx, mut rx) = mpsc::channel(1);
        refuse(&tx, &denied).await;
        assert_eq!(
            rx.try_recv().unwrap().unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        let after = crate::telemetry::test_support::logged()
            .iter()
            .filter(|l| l.field("reason").is_some())
            .count();
        assert_eq!(after, 1, "refuse() logs nothing more");
    }

    #[tokio::test]
    async fn permissive_and_off_never_close_a_session_on_reload() {
        for mode in [AuthMode::Permissive, AuthMode::Off] {
            let (auth, registry, sessions, router, m) = parts(mode, Acl::default());
            let mut rx = session(&sessions);
            assert_eq!(reevaluate_sessions(&auth, &registry, &sessions, &router), 0);
            assert!(rx.try_recv().is_err());
            assert_eq!(sessions.auth_views().len(), 1);
            assert_eq!(
                m.counter(
                    "snodra_server_auth_violations_total",
                    "reason",
                    "token_revoked"
                ),
                None
            );
        }
    }
}
