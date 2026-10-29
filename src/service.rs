//! gRPC service implementation. Skeleton: handles Register and Heartbeat, answers every Request
//! with `Error{NO_HANDLER}`. Forwarding to the owning instance and reply correlation are TODO.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use crate::config::Config;
use crate::proto::hub_server::Hub;
use crate::proto::{
    ClientMessage, Error, ErrorCode, Heartbeat, ServerMessage, client_message, server_message,
};
use crate::registry::Registry;

pub struct HubService {
    pub registry: Arc<Registry>,
    pub config: Config,
}

type ServerStream = Pin<Box<dyn Stream<Item = Result<ServerMessage, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl Hub for HubService {
    type ConnectStream = ServerStream;

    async fn connect(
        &self,
        request: Request<Streaming<ClientMessage>>,
    ) -> Result<Response<Self::ConnectStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<ServerMessage, Status>>(256);
        let registry = Arc::clone(&self.registry);

        tokio::spawn(async move {
            let mut instance_id: Option<String> = None;
            while let Some(msg) = inbound.next().await {
                let msg = match msg {
                    Ok(m) => m,
                    Err(status) => {
                        warn!(%status, "client stream error");
                        break;
                    }
                };
                match msg.message {
                    Some(client_message::Message::Register(reg)) => {
                        info!(service = %reg.service_name, instance = %reg.instance_id, "register");
                        if let Err(e) = registry.register(
                            &reg.instance_id,
                            &reg.service_name,
                            &reg.command_types,
                            &reg.query_types,
                        ) {
                            warn!(error = %e, "register rejected");
                        }
                        instance_id = Some(reg.instance_id);
                    }
                    Some(client_message::Message::Heartbeat(hb)) => {
                        let echo = ServerMessage {
                            message: Some(server_message::Message::Heartbeat(Heartbeat {
                                sent_at_ms: hb.sent_at_ms,
                            })),
                        };
                        if tx.send(Ok(echo)).await.is_err() {
                            break;
                        }
                    }
                    Some(client_message::Message::Request(req)) => {
                        // TODO: look up registry.pick_owner(&req.type, &req.routing_key), forward
                        // over the owner's stream, correlate the Reply by request id, enforce
                        // timeout_ms. Until then every request fails fast.
                        let err = ServerMessage {
                            message: Some(server_message::Message::Error(Error {
                                request_id: req.id,
                                code: ErrorCode::NoHandler as i32,
                                message: format!(
                                    "routing not implemented; no handler for {}",
                                    req.r#type
                                ),
                                details: Vec::new(),
                            })),
                        };
                        if tx.send(Ok(err)).await.is_err() {
                            break;
                        }
                    }
                    Some(client_message::Message::Reply(_reply)) => {
                        // TODO: correlate with a pending forwarded request and relay to its sender.
                    }
                    None => {}
                }
            }
            if let Some(id) = instance_id {
                info!(instance = %id, "stream closed, unregistering");
                registry.unregister(&id, Instant::now());
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as ServerStream
        ))
    }
}
