//! A fake event browser for the hub's tests: a client that registers as service
//! `snodra-event-browser` with the five `snodra.events.*` query types and answers each request
//! as the test says (canned JSON, the browser's errors, garbage, nothing), records what it was
//! asked (type, JSON, headers) and hands every tail to the test to stream by hand. Plus a reader
//! for the tail's server-sent events and a capture of the hub's log lines.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use http_body_util::BodyExt;
use serde_json::Value;
use snodra_server::proto::{
    ClientMessage, Error, ErrorCode, Reply, ReplyEnd, client_message, reply, server_message,
};
use tokio::sync::mpsc;

use super::FakeClient;

pub const SERVICE: &str = "snodra-event-browser";
pub const SEARCH: &str = "snodra.events.Search";
pub const GET: &str = "snodra.events.Get";
pub const SOURCES: &str = "snodra.events.Sources";
pub const LAG: &str = "snodra.events.Lag";
pub const TAIL: &str = "snodra.events.Tail";
pub const TYPES: [&str; 5] = [SEARCH, GET, SOURCES, LAG, TAIL];

/// How the fake answers one request.
pub enum Answer {
    /// A reply with this JSON.
    Json(Value),
    /// A reply with these bytes.
    Raw(Vec<u8>),
    /// A handler error with `details` (a tail: an end with that error, before any part).
    Error {
        code: ErrorCode,
        message: String,
        details: Vec<u8>,
    },
    /// No answer at all.
    Silent,
    /// A tail the test streams by hand ([`FakeBrowser::next_tail`]).
    Stream,
}

/// The browser's own error (`{"error": kind, "message": message}` in the details).
pub fn refusal(kind: &str, message: &str) -> Answer {
    Answer::Error {
        code: ErrorCode::HandlerError,
        message: message.to_string(),
        details: serde_json::json!({ "error": kind, "message": message })
            .to_string()
            .into_bytes(),
    }
}

/// One request the fake was asked.
#[derive(Debug, Clone)]
pub struct Seen {
    pub type_name: String,
    pub request: Value,
    pub headers: HashMap<String, String>,
}

/// A tail the test streams: parts, then an end.
pub struct Tail {
    pub id: String,
    pub request: Value,
    pub headers: HashMap<String, String>,
    tx: mpsc::Sender<ClientMessage>,
    seq: u64,
}

impl Tail {
    /// Sends one part.
    pub async fn part(&mut self, part: Value) {
        let msg = FakeClient::part_ok(&self.id, self.seq, part.to_string().as_bytes());
        self.seq += 1;
        let _ = self.tx.send(msg).await;
    }

    /// Sends raw bytes as a part.
    pub async fn raw(&mut self, bytes: &[u8]) {
        let msg = FakeClient::part_ok(&self.id, self.seq, bytes);
        self.seq += 1;
        let _ = self.tx.send(msg).await;
    }

    /// `start` with `segments`.
    pub async fn start(&mut self, segments: u64) {
        self.part(
            serde_json::json!({"start": {"source": "snodra.events", "segments": segments,
            "endsAtMs": 1_900_000_000_000_i64}}),
        )
        .await;
    }

    /// Ends the stream normally (after an `end` part, as the browser does).
    pub async fn close(&self) {
        let _ = self.tx.send(FakeClient::end(&self.id, None)).await;
    }
}

pub struct FakeBrowser {
    seen: Arc<Mutex<Vec<Seen>>>,
    cancels: Arc<Mutex<Vec<String>>>,
    tails: mpsc::UnboundedReceiver<Tail>,
    tx: Option<mpsc::Sender<ClientMessage>>,
    task: tokio::task::JoinHandle<()>,
}

impl FakeBrowser {
    /// Registers with the hub at `addr` and answers every request with `answer(type, request)`.
    pub async fn start(
        addr: SocketAddr,
        answer: impl Fn(&str, &Value) -> Answer + Send + Sync + 'static,
    ) -> Self {
        let client = FakeClient::register(addr, SERVICE, &[], &TYPES).await;
        let FakeClient { tx, mut rx, .. } = client;
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::default();
        let cancels: Arc<Mutex<Vec<String>>> = Arc::default();
        let (tails_tx, tails) = mpsc::unbounded_channel();
        let task = {
            let (seen, cancels, tx) = (Arc::clone(&seen), Arc::clone(&cancels), tx.clone());
            tokio::spawn(async move {
                while let Ok(Some(msg)) = rx.message().await {
                    let r = match msg.message {
                        Some(server_message::Message::Request(r)) => r,
                        Some(server_message::Message::Cancel(c)) => {
                            cancels.lock().unwrap().push(c.request_id);
                            continue;
                        }
                        _ => continue,
                    };
                    let request: Value = serde_json::from_slice(&r.payload).unwrap_or(Value::Null);
                    let headers: HashMap<String, String> = r.headers.clone().into_iter().collect();
                    seen.lock().unwrap().push(Seen {
                        type_name: r.r#type.clone(),
                        request: request.clone(),
                        headers: headers.clone(),
                    });
                    let _ = tx.send(permits(1)).await;
                    let streaming = r.r#type == TAIL;
                    let out = match answer(&r.r#type, &request) {
                        Answer::Json(v) => reply_ok(&r.id, v.to_string().into_bytes()),
                        Answer::Raw(bytes) => reply_ok(&r.id, bytes),
                        Answer::Error {
                            code,
                            message,
                            details,
                        } if streaming => end_err(&r.id, code, &message, details),
                        Answer::Error {
                            code,
                            message,
                            details,
                        } => reply_err(&r.id, code, &message, details),
                        Answer::Silent => continue,
                        Answer::Stream => {
                            let tail = Tail {
                                id: r.id.clone(),
                                request,
                                headers,
                                tx: tx.clone(),
                                seq: 0,
                            };
                            let _ = tails_tx.send(tail);
                            continue;
                        }
                    };
                    let _ = tx.send(out).await;
                }
            })
        };
        Self {
            seen,
            cancels,
            tails,
            tx: Some(tx),
            task,
        }
    }

    /// Every request so far.
    pub fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }

    /// The last request of `type_name`.
    pub fn last(&self, type_name: &str) -> Seen {
        self.seen()
            .into_iter()
            .rev()
            .find(|s| s.type_name == type_name)
            .unwrap_or_else(|| panic!("no {type_name} request"))
    }

    /// The ids of the streams the hub cancelled.
    pub fn cancelled(&self) -> Vec<String> {
        self.cancels.lock().unwrap().clone()
    }

    /// The next tail the hub opened (5 s at most).
    pub async fn next_tail(&mut self) -> Tail {
        tokio::time::timeout(Duration::from_secs(5), self.tails.recv())
            .await
            .expect("no tail in time")
            .expect("the fake is running")
    }

    /// Goes away: the gRPC stream ends as a crashed browser's would.
    pub fn disconnect(mut self) {
        self.task.abort();
        self.tx = None;
        self.tails.close();
    }
}

fn permits(count: u32) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Permits(
            snodra_server::proto::Permits { count, grant: 0 },
        )),
    }
}

fn reply_ok(id: &str, payload: Vec<u8>) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Reply(Reply {
            request_id: id.to_string(),
            result: Some(reply::Result::Payload(payload)),
        })),
    }
}

fn error(id: &str, code: ErrorCode, message: &str, details: Vec<u8>) -> Error {
    Error {
        request_id: id.to_string(),
        code: code as i32,
        message: message.to_string(),
        details,
    }
}

fn reply_err(id: &str, code: ErrorCode, message: &str, details: Vec<u8>) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::Reply(Reply {
            request_id: id.to_string(),
            result: Some(reply::Result::Error(error(id, code, message, details))),
        })),
    }
}

fn end_err(id: &str, code: ErrorCode, message: &str, details: Vec<u8>) -> ClientMessage {
    ClientMessage {
        message: Some(client_message::Message::End(ReplyEnd {
            request_id: id.to_string(),
            error: Some(error(id, code, message, details)),
            ..ReplyEnd::default()
        })),
    }
}

/// A summary as the browser writes one.
pub fn summary(segment: &str, position: &str, ts: i64, key: &str, headers: Value) -> Value {
    serde_json::json!({"source": "snodra.events", "segment": segment, "position": position,
        "timestamp": ts, "key": key, "headers": headers, "size": 17})
}

/// A search page with `results`.
pub fn page(results: Vec<Value>) -> Value {
    serde_json::json!({"source": "snodra.events", "results": results, "scanned": 2,
        "exhausted": true, "stoppedBy": null, "cursor": null})
}

/// What a fake answers when the test does not care: an empty page, no record, one source, no
/// lag, a tail to stream by hand.
pub fn defaults(type_name: &str, _request: &Value) -> Answer {
    match type_name {
        SEARCH => Answer::Json(page(vec![])),
        GET => refusal("not-found", "no record at that position"),
        SOURCES => Answer::Json(serde_json::json!({"sources": [{"source": "snodra.events",
            "segments": [{"segment": "0", "first": "0", "head": "2"}]}]})),
        LAG => Answer::Json(
            serde_json::json!({"source": "snodra.events", "subscriptions": [],
            "truncated": false}),
        ),
        _ => Answer::Stream,
    }
}

// ---- server-sent events ----------------------------------------------------------------------

/// A response read as server-sent events.
pub struct Sse {
    pub status: u16,
    body: hyper::body::Incoming,
    buffer: String,
    _sender: hyper::client::conn::http1::SendRequest<http_body_util::Empty<axum::body::Bytes>>,
}

/// `GET path` on `addr` with `token` as the bearer, read as events.
pub async fn sse(addr: SocketAddr, path: &str, token: Option<&str>) -> Sse {
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(conn);
    let mut request = hyper::Request::get(path)
        .header("host", addr.to_string())
        .header("accept", "text/event-stream");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = tokio::time::timeout(
        Duration::from_secs(30),
        sender.send_request(request.body(http_body_util::Empty::new()).unwrap()),
    )
    .await
    .expect("no answer in time")
    .unwrap();
    Sse {
        status: response.status().as_u16(),
        body: response.into_body(),
        buffer: String::new(),
        _sender: sender,
    }
}

impl Sse {
    /// The next event `(name, data)`, `None` when the stream ended; panics when nothing comes
    /// within `within`.
    pub async fn next(&mut self, within: Duration) -> Option<(String, String)> {
        let deadline = tokio::time::Instant::now() + within;
        loop {
            while let Some(end) = self.buffer.find("\n\n") {
                let block: String = self.buffer.drain(..end + 2).collect();
                let mut name = "message".to_string();
                let mut data = Vec::new();
                for line in block.lines() {
                    if let Some(v) = line.strip_prefix("event:") {
                        name = v.trim().to_string();
                    } else if let Some(v) = line.strip_prefix("data:") {
                        data.push(v.strip_prefix(' ').unwrap_or(v).to_string());
                    }
                }
                if !data.is_empty() {
                    return Some((name, data.join("\n")));
                }
            }
            let frame = tokio::time::timeout_at(deadline, self.body.frame())
                .await
                .expect("no event in time");
            match frame {
                None => return None,
                Some(Ok(frame)) => {
                    if let Ok(data) = frame.into_data() {
                        self.buffer.push_str(&String::from_utf8_lossy(&data));
                    }
                }
                Some(Err(_)) => return None,
            }
        }
    }
}

// ---- logs ----------------------------------------------------------------------------------

/// One log event: its message and its fields.
#[derive(Debug, Clone, Default)]
pub struct Line {
    pub message: String,
    pub fields: Vec<(String, String)>,
}

impl Line {
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }

    /// The whole line as text, for "never logged" checks.
    pub fn text(&self) -> String {
        format!("{} {:?}", self.message, self.fields)
    }
}

static LINES: Mutex<Vec<Line>> = Mutex::new(Vec::new());

impl tracing::field::Visit for Line {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "message" => self.message = value.to_string(),
            name => self.fields.push((name.to_string(), value.to_string())),
        }
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let text = format!("{value:?}");
        match field.name() {
            "message" => self.message = text,
            name => self.fields.push((name.to_string(), text)),
        }
    }
}

struct Capture;

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let mut line = Line::default();
        event.record(&mut line);
        LINES.lock().unwrap().push(line);
    }
}

/// Records every log event of this test binary, on every thread, from now on.
pub fn capture_logs() {
    use tracing_subscriber::layer::SubscriberExt;
    static INSTALL: std::sync::Once = std::sync::Once::new();
    INSTALL.call_once(|| {
        let subscriber = tracing_subscriber::registry().with(Capture);
        let _ = tracing::subscriber::set_global_default(subscriber);
    });
}

/// Every event logged since [`capture_logs`] (by every test of the binary).
pub fn logged() -> Vec<Line> {
    LINES.lock().unwrap().clone()
}
