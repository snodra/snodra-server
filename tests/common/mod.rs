//! Shared helpers for the integration tests: an in-process hub and a fake service instance.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::time::Duration;

use kraken_hub::config::Config;
use kraken_hub::proto::hub_client::HubClient;
use kraken_hub::proto::{
    ClientMessage, Credentials, Error, ErrorCode, Heartbeat, Permits, Register, Reply, Request,
    RequestKind, ServerMessage, client_message, credentials, reply,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;
use tonic::{Status, Streaming};

/// Binds `127.0.0.1:0` for gRPC, runs the hub without an HTTP listener and returns the
/// bound address.
pub async fn start_hub(config: Config) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        kraken_hub::serve_with_shutdown(config, listener, None, std::future::pending())
            .await
            .unwrap();
    });
    addr
}

/// Like [`start_hub`], plus an ephemeral HTTP listener when `config.http_listen` is `Some`
/// (the configured port is ignored). Returns `(grpc_addr, http_addr)`.
pub async fn start_hub_with_http(config: Config) -> (SocketAddr, Option<SocketAddr>) {
    let grpc = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let grpc_addr = grpc.local_addr().unwrap();
    let http = match config.http_listen {
        Some(_) => Some(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap()),
        None => None,
    };
    let http_addr = http.as_ref().map(|l| l.local_addr().unwrap());
    tokio::spawn(async move {
        kraken_hub::serve_with_shutdown(config, grpc, http, std::future::pending())
            .await
            .unwrap();
    });
    (grpc_addr, http_addr)
}

/// Minimal HTTP/1.1 GET over a raw socket (bounded to 3 s). Returns `(status, body)`.
pub async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let fut = async {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.unwrap();
        String::from_utf8(raw).unwrap()
    };
    let raw = tokio::time::timeout(Duration::from_secs(3), fut)
        .await
        .expect("http_get timed out");
    let (head, body) = raw.split_once("\r\n\r\n").expect("malformed HTTP response");
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("malformed status line");
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        dechunk(body)
    } else {
        body.to_string()
    };
    (status, body)
}

fn dechunk(mut rest: &str) -> String {
    let mut out = String::new();
    while let Some((size, tail)) = rest.split_once("\r\n") {
        let n = usize::from_str_radix(size.trim(), 16).expect("bad chunk size");
        if n == 0 {
            break;
        }
        out.push_str(&tail[..n]);
        rest = &tail[n + 2..];
    }
    out
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

    /// Like [`FakeClient::register`] for a protocol v2 client: `protocol_version` 2, a client
    /// version and (ignored) credentials.
    pub async fn register_v2(
        addr: SocketAddr,
        service: &str,
        commands: &[&str],
        queries: &[&str],
    ) -> Self {
        let client = Self::connect(addr).await;
        client.send_register_v2(service, commands, queries).await;
        client
    }

    fn register_message(&self, service: &str, commands: &[&str], queries: &[&str]) -> Register {
        Register {
            service_name: service.to_string(),
            instance_id: self.instance_id.clone(),
            command_types: commands.iter().map(|s| s.to_string()).collect(),
            query_types: queries.iter().map(|s| s.to_string()).collect(),
            ..Register::default()
        }
    }

    /// Sends a v1 `Register` (no version fields) for this client's instance id (first or
    /// re-register).
    pub async fn send_register(&self, service: &str, commands: &[&str], queries: &[&str]) {
        let reg = self.register_message(service, commands, queries);
        self.send_raw_register(reg).await;
    }

    /// Sends a v2 `Register` for this client's instance id.
    pub async fn send_register_v2(&self, service: &str, commands: &[&str], queries: &[&str]) {
        let reg = Register {
            protocol_version: 2,
            capabilities: vec!["not-a-real-capability".to_string()],
            client_version: "fake-client/1.0".to_string(),
            credentials: Some(Credentials {
                kind: Some(credentials::Kind::Token("s3cret".to_string())),
            }),
            ..self.register_message(service, commands, queries)
        };
        self.send_raw_register(reg).await;
    }

    /// A v2 client that shares flow-control and starts with `initial_permits` (0 = unlimited),
    /// with the given load factor and tags.
    pub async fn register_flow(
        addr: SocketAddr,
        service: &str,
        commands: &[&str],
        initial_permits: u32,
        tags: &[(&str, &str)],
    ) -> Self {
        let client = Self::connect(addr).await;
        let reg = Register {
            protocol_version: 2,
            capabilities: vec!["flow-control".to_string()],
            client_version: "fake-client/1.0".to_string(),
            initial_permits,
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..client.register_message(service, commands, &[])
        };
        client.send_raw_register(reg).await;
        client
    }

    pub async fn permits(&self, count: u32) {
        let msg = ClientMessage {
            message: Some(client_message::Message::Permits(Permits { count })),
        };
        self.tx.send(msg).await.unwrap();
    }

    pub async fn send_raw_register(&self, reg: Register) {
        let msg = ClientMessage {
            message: Some(client_message::Message::Register(reg)),
        };
        self.tx.send(msg).await.unwrap();
    }

    pub async fn heartbeat(&self, sent_at_ms: i64) {
        let msg = ClientMessage {
            message: Some(client_message::Message::Heartbeat(Heartbeat { sent_at_ms })),
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
                ..Request::default()
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
