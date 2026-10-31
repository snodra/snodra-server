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

use crate::config::Config;
use crate::context::ContextId;
use crate::proto::hub_server::Hub;
use crate::proto::{
    ClientMessage, Heartbeat, Register, ServerMessage, TopologyRequest, TopologyResponse,
    client_message, server_message,
};
use crate::protocol;
use crate::registry::{RegisterError, Registry};
use crate::router::Router;
use crate::session::{SessionMeta, Sessions};
use crate::topology::{ClockAnchor, snapshot};

pub struct HubService {
    pub registry: Arc<Registry>,
    pub sessions: Arc<Sessions>,
    pub router: Arc<Router>,
    pub config: Arc<Config>,
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
            context: ContextId::default(),
        };
        tokio::spawn(run_stream(ctx, inbound, tx, cancel.clone()));

        let stream = ResponseStream {
            inner: ReceiverStream::new(rx),
            _cancel_on_drop: cancel.drop_guard(),
        };
        Ok(Response::new(Box::pin(stream) as ServerStream))
    }

    async fn topology(
        &self,
        _request: Request<TopologyRequest>,
    ) -> Result<Response<TopologyResponse>, Status> {
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
    /// Every stream lives in the default context (contexts are deferred; the API keeps the door open).
    context: ContextId,
}

impl StreamCtx {
    /// Registers `reg` while the session `token` is provably the current one for its instance
    /// id. `None` means the session was removed or replaced meanwhile (nothing was changed).
    fn register(&self, token: u64, reg: &Register) -> Option<Result<(), Status>> {
        let result = self
            .sessions
            .with_current(&self.context, &reg.instance_id, token, || {
                self.registry.register(
                    &self.context,
                    &reg.instance_id,
                    &reg.service_name,
                    &reg.command_types,
                    &reg.query_types,
                )
            })?;
        Some(match result {
            Ok(()) => {
                // Never log `reg` itself: it may carry credentials.
                info!(
                    service = %reg.service_name,
                    instance = %reg.instance_id,
                    commands = reg.command_types.len(),
                    queries = reg.query_types.len(),
                    protocol_version = reg.protocol_version,
                    client_version = %reg.client_version,
                    "register"
                );
                Ok(())
            }
            Err(e) => {
                warn!(error = %e, service = %reg.service_name, instance = %reg.instance_id, "register rejected");
                Err(match e {
                    RegisterError::DuplicateTypeAcrossKinds { .. } => {
                        Status::invalid_argument(e.to_string())
                    }
                    RegisterError::TypeOwnedByOtherService { .. }
                    | RegisterError::KindConflict { .. } => Status::already_exists(e.to_string()),
                })
            }
        })
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

async fn run_stream(
    ctx: StreamCtx,
    mut inbound: Streaming<ClientMessage>,
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
        warn!(service = %first.service_name, instance = %first.instance_id, "register with empty identity");
        let _ = tx
            .send(Err(Status::invalid_argument(
                "Register.instance_id and Register.service_name must not be empty",
            )))
            .await;
        return;
    }
    let instance_id = first.instance_id.clone();
    let service = first.service_name.clone();
    // Version, capabilities and client version are fixed by the first Register (ADR 0013).
    let v2 = protocol::is_v2(first.protocol_version);
    let ack = v2.then(|| {
        protocol::register_ack(
            &ctx.config,
            &ctx.context,
            first.protocol_version,
            &first.capabilities,
        )
    });
    let meta = SessionMeta {
        protocol_version: first.protocol_version,
        capabilities: first.capabilities.clone(),
        client_version: first.client_version.clone(),
        heartbeat_timeout: ctx.config.heartbeat_timeout_for(first.protocol_version),
    };
    // Session first, then the registration under it: a late disconnect of an older stream with
    // the same instance id can then never unregister this stream's registration.
    let (token, replaced) = ctx.sessions.insert_replacing(
        &ctx.context,
        &instance_id,
        &service,
        meta,
        tx.clone(),
        cancel.clone(),
    );
    // The replaced stream's task sees itself replaced and skips the disconnect sequence, so its
    // requests are ended here: nobody can answer requests forwarded to the old stream.
    if let Some(old) = replaced {
        ctx.router.on_disconnect(&ctx.context, &instance_id, old);
    }
    match ctx.register(token, &first) {
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
    // The ack is queued before this task reads the next inbound message, so it precedes the echo
    // of the heartbeat that follows the Register (the stream's messages are handled in order).
    // Requests routed to this instance meanwhile may come first; clients handle them anyway.
    if let Some(ack) = &ack {
        let sent = tokio::select! {
            biased;
            _ = cancel.cancelled() => false,
            sent = tx.send(Ok(ack.clone())) => sent.is_ok(),
        };
        if !sent {
            ctx.disconnect(&instance_id, token);
            return;
        }
    }
    ctx.router.refresh_gauges();
    // From here on the sessions map holds the only strong sender, so removing the session ends
    // the outbound stream; this task keeps a weak handle for its own replies.
    let out = tx.downgrade();
    drop(tx);

    loop {
        let msg = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
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
                match ctx.register(token, &reg) {
                    Some(Ok(())) => {
                        ctx.router.refresh_gauges();
                        // Best effort like the heartbeat echo: a full buffer drops this ack.
                        if let (Some(ack), Some(tx)) = (&ack, out.upgrade())
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
                ctx.router.on_reply(&ctx.context, &instance_id, reply)
            }
            // Flow control is not implemented yet: permits are ignored.
            Some(client_message::Message::Permits(_)) => {}
            None => {}
        }
    }
    ctx.disconnect(&instance_id, token);
}
