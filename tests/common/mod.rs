//! Shared helpers for the integration tests: an in-process hub and a fake service instance.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use kraken_hub::config::Config;
use kraken_hub::proto::hub_client::HubClient;
use kraken_hub::proto::{
    ClientMessage, Error, ErrorCode, Register, Reply, Request, RequestKind, ServerMessage,
    client_message, reply,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;
use tonic::{Status, Streaming};

/// Binds `127.0.0.1:0`, spawns `serve` and returns the bound address.
pub async fn start_hub(config: Config) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        kraken_hub::serve(config, listener).await.unwrap();
    });
    addr
}

pub struct FakeClient {
    pub tx: mpsc::Sender<ClientMessage>,
    pub rx: Streaming<ServerMessage>,
    pub instance_id: String,
}

impl FakeClient {
    /// Opens `Connect` without registering.
    pub async fn connect(addr: SocketAddr) -> Self {
        let channel = Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = HubClient::new(channel);
        let (tx, rx) = mpsc::channel(64);
        let rx = client
            .connect(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        Self {
            tx,
            rx,
            instance_id: uuid::Uuid::now_v7().to_string(),
        }
    }

    pub async fn register(
        addr: SocketAddr,
        service: &str,
        commands: &[&str],
        queries: &[&str],
    ) -> Self {
        let client = Self::connect(addr).await;
        client.send_register(service, commands, queries).await;
        client
    }

    /// Sends a `Register` for this client's instance id (first or re-register).
    pub async fn send_register(&self, service: &str, commands: &[&str], queries: &[&str]) {
        let msg = ClientMessage {
            message: Some(client_message::Message::Register(Register {
                service_name: service.to_string(),
                instance_id: self.instance_id.clone(),
                command_types: commands.iter().map(|s| s.to_string()).collect(),
                query_types: queries.iter().map(|s| s.to_string()).collect(),
            })),
        };
        self.tx.send(msg).await.unwrap();
    }

    pub async fn next(&mut self) -> Option<Result<ServerMessage, Status>> {
        self.next_within(Duration::from_secs(2)).await
    }

    /// Next message, or panics when nothing arrives within `d`.
    pub async fn next_within(&mut self, d: Duration) -> Option<Result<ServerMessage, Status>> {
        tokio::time::timeout(d, self.rx.message_or_status())
            .await
            .expect("timed out waiting for a server message")
    }

    pub fn request(
        id: &str,
        kind: RequestKind,
        type_name: &str,
        key: &str,
        timeout_ms: u32,
    ) -> ClientMessage {
        ClientMessage {
            message: Some(client_message::Message::Request(Request {
                id: id.to_string(),
                kind: kind as i32,
                r#type: type_name.to_string(),
                routing_key: key.to_string(),
                payload: Vec::new(),
                headers: Default::default(),
                timeout_ms,
            })),
        }
    }

    pub fn reply_ok(request_id: &str, payload: &[u8]) -> ClientMessage {
        ClientMessage {
            message: Some(client_message::Message::Reply(Reply {
                request_id: request_id.to_string(),
                result: Some(reply::Result::Payload(payload.to_vec())),
            })),
        }
    }

    pub fn reply_err(request_id: &str, code: ErrorCode, message: &str) -> ClientMessage {
        ClientMessage {
            message: Some(client_message::Message::Reply(Reply {
                request_id: request_id.to_string(),
                result: Some(reply::Result::Error(Error {
                    request_id: request_id.to_string(),
                    code: code as i32,
                    message: message.to_string(),
                    details: Vec::new(),
                })),
            })),
        }
    }
}

trait MessageOrStatus {
    async fn message_or_status(&mut self) -> Option<Result<ServerMessage, Status>>;
}

impl MessageOrStatus for Streaming<ServerMessage> {
    async fn message_or_status(&mut self) -> Option<Result<ServerMessage, Status>> {
        match self.message().await {
            Ok(Some(m)) => Some(Ok(m)),
            Ok(None) => None,
            Err(s) => Some(Err(s)),
        }
    }
}
