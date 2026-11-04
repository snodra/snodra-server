//! Shared helpers for the integration tests: an in-process hub and a fake service instance.
#![allow(dead_code)]

pub mod control;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use kraken_hub::config::Config;
use kraken_hub::proto::hub_client::HubClient;
use kraken_hub::proto::{
    Cancel, ClientMessage, Credentials, Error, ErrorCode, Flow, Heartbeat, InstanceStatus, Permits,
    Register, Reply, ReplyEnd, ReplyPart, Request, RequestKind, ServerMessage, client_message,
    credentials, reply,
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
    let r = http_get_full(addr, path).await;
    (r.status, r.body)
}

/// Status, headers (names lowercased) and body of an HTTP response.
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl HttpResponse {
    /// The first header named `name` (lowercase), if any.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Like [`http_get`], with the response headers.
pub async fn http_get_full(addr: SocketAddr, path: &str) -> HttpResponse {
    http_request(addr, "GET", path, &[]).await
}

/// One HTTP/1.1 request with extra headers (`name`, `value`), bounded to 3 s.
pub async fn http_request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> HttpResponse {
    http_send(addr, method, path, headers, b"", Duration::from_secs(3)).await
}

/// [`http_request`] with a body (sent with its `Content-Length` when not empty), bounded to
/// `bound`.
pub async fn http_send(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    bound: Duration,
) -> HttpResponse {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut extra: String = headers
        .iter()
        .map(|(n, v)| format!("{n}: {v}\r\n"))
        .collect();
    if !body.is_empty() {
        extra.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    let fut = async {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request =
            format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\n{extra}Connection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        // The server may answer (and close) before it read a body it refuses.
        let _ = stream.write_all(body).await;
        let mut raw = Vec::new();
        let _ = stream.read_to_end(&mut raw).await;
        String::from_utf8(raw).unwrap()
    };
    let raw = tokio::time::timeout(bound, fut)
        .await
        .expect("http request timed out");
    let (head, body) = raw.split_once("\r\n\r\n").expect("malformed HTTP response");
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .expect("malformed status line");
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let chunked = headers
        .iter()
        .any(|(n, v)| n == "transfer-encoding" && v.eq_ignore_ascii_case("chunked"));
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_string()
    };
    HttpResponse {
        status,
        headers,
        body,
    }
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

    /// Like [`FakeClient::register_flow`] for a given instance id: a reconnect of that instance.
    pub async fn register_flow_as(
        addr: SocketAddr,
        instance_id: &str,
        service: &str,
        commands: &[&str],
        initial_permits: u32,
    ) -> Self {
        let mut client = Self::connect(addr).await;
        client.instance_id = instance_id.to_string();
        client
            .send_register_flow(service, commands, initial_permits)
            .await;
        client
    }

    /// A v2 `Register` sharing flow-control (first or re-register; an empty `commands` list is
    /// what a client sends when it stops gracefully).
    pub async fn send_register_flow(&self, service: &str, commands: &[&str], initial_permits: u32) {
        let reg = Register {
            protocol_version: 2,
            capabilities: vec!["flow-control".to_string()],
            client_version: "fake-client/1.0".to_string(),
            initial_permits,
            ..self.register_message(service, commands, &[])
        };
        self.send_raw_register(reg).await;
    }

    /// A v2 client declaring exactly `capabilities`.
    pub async fn register_v2_with(
        addr: SocketAddr,
        service: &str,
        commands: &[&str],
        capabilities: &[&str],
    ) -> Self {
        let client = Self::connect(addr).await;
        let reg = Register {
            protocol_version: 2,
            capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
            client_version: "fake-client/1.0".to_string(),
            ..client.register_message(service, commands, &[])
        };
        client.send_raw_register(reg).await;
        client
    }

    /// Sends an `InstanceStatus` (multi-hub HA, ADR 0016).
    pub async fn status(&self, connected_hub_ids: &[&str], handlers_in_flight: u32) {
        let msg = ClientMessage {
            message: Some(client_message::Message::Status(InstanceStatus {
                connected_hub_ids: connected_hub_ids.iter().map(|s| s.to_string()).collect(),
                handlers_in_flight,
                processors: vec![],
            })),
        };
        self.tx.send(msg).await.unwrap();
    }

    /// Sends an `InstanceStatus` with event processors (ADR 0022).
    pub async fn status_with_processors(
        &self,
        connected_hub_ids: &[&str],
        processors: Vec<kraken_hub::proto::ProcessorStatus>,
    ) {
        let msg = ClientMessage {
            message: Some(client_message::Message::Status(InstanceStatus {
                connected_hub_ids: connected_hub_ids.iter().map(|s| s.to_string()).collect(),
                handlers_in_flight: 0,
                processors,
            })),
        };
        self.tx.send(msg).await.unwrap();
    }

    pub async fn permits(&self, count: u32) {
        self.permits_with_grant(count, 0).await;
    }

    /// `Permits{count, grant}`: a multi-hub top-up that raises the session's cap to `grant`.
    pub async fn permits_with_grant(&self, count: u32, grant: u32) {
        let msg = ClientMessage {
            message: Some(client_message::Message::Permits(Permits { count, grant })),
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

    /// A handler's streamed part (ADR 0017, P4b).
    pub fn part_ok(request_id: &str, seq: u64, payload: &[u8]) -> ClientMessage {
        ClientMessage {
            message: Some(client_message::Message::Part(ReplyPart {
                request_id: request_id.to_string(),
                payload: payload.to_vec(),
                seq,
                ..ReplyPart::default()
            })),
        }
    }

    /// A handler's part carrying an error (it ends the stream).
    pub fn part_err(request_id: &str, seq: u64, code: ErrorCode, message: &str) -> ClientMessage {
        ClientMessage {
            message: Some(client_message::Message::Part(ReplyPart {
                request_id: request_id.to_string(),
                error: Some(Error {
                    request_id: request_id.to_string(),
                    code: code as i32,
                    message: message.to_string(),
                    details: Vec::new(),
                }),
                seq,
                ..ReplyPart::default()
            })),
        }
    }

    /// A handler's end of stream, with an error when `error` is given.
    pub fn end(request_id: &str, error: Option<(ErrorCode, &str)>) -> ClientMessage {
        ClientMessage {
            message: Some(client_message::Message::End(ReplyEnd {
                request_id: request_id.to_string(),
                error: error.map(|(code, message)| Error {
                    request_id: request_id.to_string(),
                    code: code as i32,
                    message: message.to_string(),
                    details: Vec::new(),
                }),
                ..ReplyEnd::default()
            })),
        }
    }

    /// A sender's credits for a stream.
    pub fn flow(request_id: &str, credits: u32) -> ClientMessage {
        ClientMessage {
            message: Some(client_message::Message::Flow(Flow {
                request_id: request_id.to_string(),
                credits,
            })),
        }
    }

    /// A sender's cancellation of a stream.
    pub fn cancel(request_id: &str, reason: &str) -> ClientMessage {
        ClientMessage {
            message: Some(client_message::Message::Cancel(Cancel {
                request_id: request_id.to_string(),
                reason: reason.to_string(),
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

// ---- access control and TLS (P5a) ---------------------------------------------------------

/// A scratch directory under the system temp dir, removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("kraken-hub-test-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    pub fn write(&self, name: &str, content: &str) -> PathBuf {
        let path = self.path(name);
        std::fs::write(&path, content).unwrap();
        path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// One `[[token]]` entry of an ACL file for `token` (hashed here, like `acl hash` does).
pub fn acl_entry(token: &str, service: &str, handle: &[&str], send: &[&str]) -> String {
    let list = |items: &[&str]| {
        items
            .iter()
            .map(|i| format!("\"{i}\""))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "[[token]]\ntoken_sha256 = \"{}\"\nservice = \"{service}\"\nhandle = [{}]\nsend = [{}]\n\n",
        kraken_hub::auth::hash_token(token),
        list(handle),
        list(send)
    )
}

/// A config with access control `mode` over the ACL file `acl`, polling for changes every 50 ms.
pub fn auth_config(mode: kraken_hub::auth::AuthMode, acl: &std::path::Path) -> Config {
    Config {
        otlp_endpoint: None,
        auth_mode: mode,
        acl_file: Some(acl.to_path_buf()),
        reload_interval: Duration::from_millis(50),
        ..Config::default()
    }
}

/// Every capability a test sender may need.
pub const ALL_CAPS: &[&str] = &["scatter-gather", "streaming", "subscription"];

/// What a test `Register` carries.
#[derive(Default, Clone)]
pub struct Spec<'a> {
    pub service: &'a str,
    pub commands: Vec<&'a str>,
    pub queries: Vec<&'a str>,
    pub shared: Vec<&'a str>,
    pub token: Option<&'a str>,
    pub jwt: Option<&'a str>,
    /// A protocol v1 client: no version, capabilities or credentials field.
    pub v1: bool,
    pub caps: Vec<&'a str>,
    /// `Register.initial_permits` (needs the "flow-control" capability to take effect).
    pub initial_permits: u32,
}

impl Spec<'_> {
    pub fn register(&self, instance_id: &str) -> Register {
        let credentials = match (self.token, self.jwt) {
            (Some(t), _) => Some(credentials::Kind::Token(t.to_string())),
            (None, Some(j)) => Some(credentials::Kind::Jwt(j.to_string())),
            (None, None) => None,
        }
        .map(|kind| Credentials { kind: Some(kind) });
        Register {
            service_name: self.service.to_string(),
            instance_id: instance_id.to_string(),
            command_types: self.commands.iter().map(|s| s.to_string()).collect(),
            query_types: self.queries.iter().map(|s| s.to_string()).collect(),
            shared_query_types: self.shared.iter().map(|s| s.to_string()).collect(),
            protocol_version: if self.v1 { 0 } else { 2 },
            capabilities: self.caps.iter().map(|c| c.to_string()).collect(),
            client_version: if self.v1 {
                String::new()
            } else {
                "fake-client/1.0".into()
            },
            credentials: if self.v1 { None } else { credentials },
            initial_permits: self.initial_permits,
            ..Register::default()
        }
    }
}

impl FakeClient {
    /// Connects and sends the `Register` described by `spec`.
    pub async fn register_spec(addr: SocketAddr, spec: &Spec<'_>) -> Self {
        let client = Self::connect(addr).await;
        client
            .send_raw_register(spec.register(&client.instance_id))
            .await;
        client
    }

    /// Like [`FakeClient::register_spec`] for a given instance id: a reconnect of that instance
    /// (or an attempt to take it over).
    pub async fn register_spec_as(addr: SocketAddr, instance_id: &str, spec: &Spec<'_>) -> Self {
        let mut client = Self::connect(addr).await;
        client.instance_id = instance_id.to_string();
        client
            .send_raw_register(spec.register(&client.instance_id))
            .await;
        client
    }

    /// Like [`FakeClient::connect`] over TLS, trusting the CA in `ca_pem`.
    pub async fn connect_tls(addr: SocketAddr, ca_pem: &std::path::Path) -> Self {
        let target = kraken_hub::client::Target {
            address: addr.to_string(),
            tls: Some(kraken_hub::client::Trust::Ca(ca_pem.to_path_buf())),
            identity: None,
        };
        let channel = target.connect().await.unwrap();
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

    /// Like [`FakeClient::connect_tls`], presenting the client certificate and key (PEM files)
    /// in `identity`. `Err` when the hub refuses the handshake (a TLS 1.3 client learns it when
    /// the stream opens).
    pub async fn connect_mtls(
        addr: SocketAddr,
        ca_pem: &std::path::Path,
        identity: Option<(&std::path::Path, &std::path::Path)>,
    ) -> Result<Self, String> {
        let target = kraken_hub::client::Target {
            address: addr.to_string(),
            tls: Some(kraken_hub::client::Trust::Ca(ca_pem.to_path_buf())),
            identity: identity.map(|(cert, key)| {
                kraken_hub::client::ClientIdentity::new(cert.to_path_buf(), key.to_path_buf())
            }),
        };
        let channel = target.connect().await.map_err(|e| format!("{e:#}"))?;
        let mut client = HubClient::new(channel);
        let (tx, rx) = mpsc::channel(64);
        let rx = client
            .connect(ReceiverStream::new(rx))
            .await
            .map_err(|e| e.to_string())?
            .into_inner();
        Ok(Self {
            tx,
            rx,
            instance_id: uuid::Uuid::now_v7().to_string(),
        })
    }

    /// The next message, which must be a `RegisterAck` (the session was accepted).
    pub async fn expect_ack(&mut self) -> kraken_hub::proto::RegisterAck {
        match self.next().await {
            Some(Ok(ServerMessage {
                message: Some(kraken_hub::proto::server_message::Message::RegisterAck(ack)),
            })) => ack,
            other => panic!("expected a RegisterAck, got {other:?}"),
        }
    }

    /// Waits for the stream to end with a status (the hub closed it) and returns it.
    pub async fn expect_closed(&mut self) -> Status {
        loop {
            match self.next().await {
                Some(Err(status)) => return status,
                Some(Ok(_)) => continue,
                None => panic!("the stream ended without a status"),
            }
        }
    }

    /// The next message, which must be an `Error`.
    pub async fn expect_error(&mut self) -> Error {
        loop {
            match self.next().await {
                Some(Ok(ServerMessage {
                    message: Some(kraken_hub::proto::server_message::Message::Error(e)),
                })) => return e,
                Some(Ok(ServerMessage {
                    message: Some(kraken_hub::proto::server_message::Message::Heartbeat(_)),
                })) => continue,
                other => panic!("expected an Error, got {other:?}"),
            }
        }
    }

    /// True when no message arrives within `d` (heartbeat echoes aside).
    pub async fn silent_for(&mut self, d: Duration) -> bool {
        tokio::time::timeout(d, async {
            loop {
                match self.rx.message().await {
                    Ok(Some(ServerMessage {
                        message: Some(kraken_hub::proto::server_message::Message::Heartbeat(_)),
                    })) => continue,
                    _ => return,
                }
            }
        })
        .await
        .is_err()
    }
}

#[path = "../../src/pki_test_support.rs"]
mod pki_support;
#[allow(unused_imports)]
pub use pki_support::{ClientSpec, Pki};

impl Pki {
    /// Writes `ca.pem`, `cert.pem` and `key.pem` named with `prefix` into `dir`; returns the
    /// paths `(ca, cert, key)`.
    pub fn write(&self, dir: &TempDir, prefix: &str) -> (PathBuf, PathBuf, PathBuf) {
        let (cert, key) = self.server();
        (
            dir.write(&format!("{prefix}-ca.pem"), &self.ca_pem),
            dir.write(&format!("{prefix}-cert.pem"), &cert),
            dir.write(&format!("{prefix}-key.pem"), &key),
        )
    }
}

/// One HTTP/1.1 request over TLS (trusting `ca_pem`, server name `localhost`), bounded to 3 s.
pub async fn https_request(
    addr: SocketAddr,
    ca_pem: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<HttpResponse, String> {
    use rustls::pki_types::{ServerName, pem::PemObject};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    let extra: String = headers
        .iter()
        .map(|(n, v)| format!("{n}: {v}\r\n"))
        .collect();
    let fut = async move {
        let tcp = tokio::net::TcpStream::connect(addr)
            .await
            .map_err(|e| e.to_string())?;
        let mut stream = connector
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .map_err(|e| e.to_string())?;
        let request = format!(
            "{method} {path} HTTP/1.1\r\nHost: localhost\r\n{extra}Connection: close\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|e| e.to_string())?;
        let mut raw = Vec::new();
        // A close without close_notify is how this server ends a `Connection: close` exchange.
        let _ = stream.read_to_end(&mut raw).await;
        Ok::<_, String>(String::from_utf8_lossy(&raw).into_owned())
    };
    let raw = tokio::time::timeout(Duration::from_secs(3), fut)
        .await
        .map_err(|_| "timed out".to_string())??;
    let (head, body) = raw.split_once("\r\n\r\n").ok_or("no HTTP response")?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .ok_or("malformed status line")?;
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(n, v)| (n.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let chunked = headers
        .iter()
        .any(|(n, v)| n == "transfer-encoding" && v.eq_ignore_ascii_case("chunked"));
    let body = if chunked {
        dechunk(body)
    } else {
        body.to_string()
    };
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}
