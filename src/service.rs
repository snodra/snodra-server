//! gRPC service implementation: the `Hub.Connect` stream.
//!
//! Each connected stream is driven by one task (`run_stream`). The task owns the inbound half;
//! the outbound half is the receiver of a per-session channel whose only long-lived sender is the
//! one stored in [`Sessions`], so removing the session (disconnect, watchdog eviction) ends the
//! client's inbound stream and wakes the task.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use tokio::sync::{mpsc, oneshot};
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use crate::config::Config;
use crate::proto::hub_server::Hub;
use crate::proto::{
    ClientMessage, Heartbeat, Register, ServerMessage, client_message, server_message,
};
use crate::registry::Registry;
use crate::router::Router;
use crate::session::Sessions;

pub struct HubService {
    pub registry: Arc<Registry>,
    pub sessions: Arc<Sessions>,
    pub router: Arc<Router>,
    pub config: Arc<Config>,
}

type ServerStream = Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;
type Outbound = mpsc::Sender<Result<ServerMessage, Status>>;

/// The one disconnect sequence: stream end, inbound error, rejected re-register and watchdog
/// eviction all go through here. Removing the session drops its sender, which ends the client's
/// inbound stream.
pub fn disconnect(registry: &Registry, sessions: &Sessions, router: &Router, instance_id: &str) {
    router.on_disconnect(instance_id);
    let service = sessions.remove(instance_id).map(|s| s.service);
    registry.unregister(instance_id, Instant::now());
    info!(instance = %instance_id, service = service.as_deref().unwrap_or("unknown"), "disconnect");
}

/// Response stream that tells the stream task when it ended because every sender is gone (the
/// session was removed by someone else). If it is merely dropped (client went away) the task sees
/// the channel close without a value and runs the disconnect itself.
struct EndSignal {
    inner: ReceiverStream<Result<ServerMessage, Status>>,
    signal: Option<oneshot::Sender<()>>,
}

impl Stream for EndSignal {
    type Item = Result<ServerMessage, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = Pin::new(&mut self.inner).poll_next(cx);
        if matches!(poll, Poll::Ready(None))
            && let Some(signal) = self.signal.take()
        {
            let _ = signal.send(());
        }
        poll
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
        let (end_tx, end_rx) = oneshot::channel();
        info!("connect");

        let ctx = StreamCtx {
            registry: Arc::clone(&self.registry),
            sessions: Arc::clone(&self.sessions),
            router: Arc::clone(&self.router),
        };
        tokio::spawn(run_stream(ctx, inbound, tx, end_rx));

        let stream = EndSignal {
            inner: ReceiverStream::new(rx),
            signal: Some(end_tx),
        };
        Ok(Response::new(Box::pin(stream) as ServerStream))
    }
}

struct StreamCtx {
    registry: Arc<Registry>,
    sessions: Arc<Sessions>,
    router: Arc<Router>,
}

impl StreamCtx {
    fn register(&self, reg: &Register) -> Result<(), Status> {
        self.registry
            .register(
                &reg.instance_id,
                &reg.service_name,
                &reg.command_types,
                &reg.query_types,
            )
            .map_err(|e| {
                warn!(error = %e, service = %reg.service_name, instance = %reg.instance_id, "register rejected");
                Status::already_exists(e.to_string())
            })?;
        info!(
            service = %reg.service_name,
            instance = %reg.instance_id,
            commands = reg.command_types.len(),
            queries = reg.query_types.len(),
            "register"
        );
        Ok(())
    }
}

async fn run_stream(
    ctx: StreamCtx,
    mut inbound: Streaming<ClientMessage>,
    tx: Outbound,
    mut ended: oneshot::Receiver<()>,
) {
    // Handshake: the first message must be a successful Register. No session state exists yet,
    // so every failure here just reports the status and ends the stream.
    let first = match inbound.next().await {
        Some(Ok(ClientMessage {
            message: Some(client_message::Message::Register(reg)),
        })) => reg,
        other => {
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
    if let Err(status) = ctx.register(&first) {
        let _ = tx.send(Err(status)).await;
        return;
    }
    let instance_id = first.instance_id.clone();
    let service = first.service_name.clone();
    ctx.sessions.insert(&instance_id, &service, tx.clone());
    // From here on the sessions map holds the only strong sender, so removing the session ends
    // the outbound stream; this task keeps a weak handle for its own replies.
    let out = tx.downgrade();
    drop(tx);

    // `evicted`: the session was removed by someone else (watchdog, or replaced by a newer
    // stream with the same instance id); its cleanup already happened and must not run again.
    let mut evicted = false;
    loop {
        let msg = tokio::select! {
            biased;
            ended = &mut ended => { evicted = ended.is_ok(); break; }
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
                if let Err(status) = ctx.register(&reg) {
                    match out.upgrade() {
                        Some(tx) => {
                            let _ = tx.send(Err(status)).await;
                        }
                        None => evicted = true,
                    }
                    break;
                }
            }
            Some(client_message::Message::Heartbeat(hb)) => {
                ctx.sessions.touch(&instance_id, Instant::now());
                let Some(tx) = out.upgrade() else {
                    evicted = true;
                    break;
                };
                let echo = ServerMessage {
                    message: Some(server_message::Message::Heartbeat(Heartbeat {
                        sent_at_ms: hb.sent_at_ms,
                    })),
                };
                if tx.send(Ok(echo)).await.is_err() {
                    break;
                }
            }
            Some(client_message::Message::Request(req)) => ctx.router.route(&instance_id, req),
            Some(client_message::Message::Reply(reply)) => ctx.router.on_reply(&instance_id, reply),
            None => {}
        }
    }
    if !evicted {
        disconnect(&ctx.registry, &ctx.sessions, &ctx.router, &instance_id);
    }
}
