//! The event browser's test support (ADR 0025): a Kafka broker in a container (the image the
//! libraries' tests run), topics, records with chosen keys, headers and timestamps, consumer
//! groups, a reader for the tail's server-sent events, and a capture of the hub's log lines.
//!
//! The containers have no reaper here: keep the [`Broker`] in a local of the test, so it is
//! dropped (and the container removed) when the test ends.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::Duration;

use http_body_util::BodyExt;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, FutureProducer, FutureRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{self, Kafka};

pub struct Broker {
    pub container: ContainerAsync<Kafka>,
    pub bootstrap: String,
}

pub async fn kafka() -> Broker {
    let container = Kafka::default().start().await.expect("kafka container");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("kafka port");
    Broker {
        container,
        bootstrap: format!("127.0.0.1:{port}"),
    }
}

/// A topic name no other test uses.
pub fn unique(prefix: &str) -> String {
    format!("{prefix}-{}", uuid::Uuid::now_v7().simple())
}

pub async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new().operation_timeout(Some(Duration::from_secs(20))),
        )
        .await
        .unwrap();
    for r in results {
        r.unwrap();
    }
}

/// One record to write.
#[derive(Clone, Default)]
pub struct Rec {
    pub partition: i32,
    pub key: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub payload: Option<Vec<u8>>,
    pub timestamp: Option<i64>,
}

impl Rec {
    pub fn on(partition: i32) -> Self {
        Self {
            partition,
            ..Self::default()
        }
    }

    pub fn key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn header(mut self, name: &str, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.to_string(), value.into()));
        self
    }

    pub fn payload(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.payload = Some(payload.into());
        self
    }

    pub fn at(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
}

/// Writes `records` in order and returns where each landed, `(partition, offset)`.
pub async fn produce(bootstrap: &str, topic: &str, records: &[Rec]) -> Vec<(i32, i64)> {
    produce_compressed(bootstrap, topic, records, "none").await
}

/// [`produce`] with the producer's `compression.type` (`gzip`, `snappy`, `lz4`, `zstd`).
pub async fn produce_compressed(
    bootstrap: &str,
    topic: &str,
    records: &[Rec],
    compression: &str,
) -> Vec<(i32, i64)> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("message.timeout.ms", "20000")
        .set("compression.type", compression)
        .create()
        .unwrap();
    let mut out = Vec::new();
    for r in records {
        let mut headers = OwnedHeaders::new();
        for (name, value) in &r.headers {
            headers = headers.insert(Header {
                key: name,
                value: Some(value),
            });
        }
        let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic)
            .partition(r.partition)
            .headers(headers);
        if let Some(key) = &r.key {
            record = record.key(key);
        }
        if let Some(payload) = &r.payload {
            record = record.payload(payload);
        }
        if let Some(ts) = r.timestamp {
            record = record.timestamp(ts);
        }
        let delivery = producer
            .send(record, Duration::from_secs(20))
            .await
            .map_err(|(e, _)| e)
            .unwrap();
        out.push((delivery.partition, delivery.offset));
    }
    out
}

/// Writes `count` small records to `partition`, as fast as the producer batches them; record `i`
/// has key `k-<i>` and timestamp `first_ts + i`.
pub fn produce_many(bootstrap: &str, topic: &str, partition: i32, count: usize, first_ts: i64) {
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("linger.ms", "20")
        .set("batch.num.messages", "10000")
        .set("queue.buffering.max.messages", "1000000")
        .create()
        .unwrap();
    for i in 0..count {
        let key = format!("k-{i}");
        let mut record = BaseRecord::<str, [u8]>::to(topic)
            .partition(partition)
            .key(&key)
            .payload(b"{}".as_slice())
            .timestamp(first_ts + i as i64);
        loop {
            match producer.send(record) {
                Ok(()) => break,
                Err((_, back)) => {
                    producer.poll(Duration::from_millis(10));
                    record = back;
                }
            }
        }
    }
    producer.flush(Duration::from_secs(60)).unwrap();
}

/// Every consumer group the broker knows.
pub fn groups(bootstrap: &str) -> BTreeSet<String> {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .unwrap();
    c.fetch_group_list(None, Duration::from_secs(10))
        .unwrap()
        .groups()
        .iter()
        .map(|g| g.name().to_string())
        .collect()
}

/// Commits `offsets` (`(partition, offset)` of `topic`) for `group` without joining it: the group
/// then has committed offsets and no member.
pub fn commit(bootstrap: &str, group: &str, topic: &str, offsets: &[(i32, i64)]) {
    let c: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    for (p, o) in offsets {
        tpl.add_partition_offset(topic, *p, Offset::Offset(*o))
            .unwrap();
    }
    commit_retrying(&c, &tpl);
}

/// Commits `tpl`, retrying while a fresh broker is still electing the group coordinator.
fn commit_retrying(c: &BaseConsumer, tpl: &TopicPartitionList) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match c.commit(tpl, CommitMode::Sync) {
            Ok(()) => return,
            Err(e) if std::time::Instant::now() < deadline => {
                eprintln!("commit retried: {e}");
                std::thread::sleep(Duration::from_millis(300));
            }
            Err(e) => panic!("commit failed: {e}"),
        }
    }
}

/// A member of `group` subscribed to `topic`, polling on its own thread until dropped; it commits
/// `offsets` once it has its partitions.
pub struct Member {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Member {
    pub fn join(bootstrap: &str, group: &str, topic: &str, offsets: &[(i32, i64)]) -> Self {
        let c: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "latest")
            .create()
            .unwrap();
        c.subscribe(&[topic]).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while c.assignment().unwrap().count() == 0 {
            assert!(std::time::Instant::now() < deadline, "no assignment");
            c.poll(Duration::from_millis(100));
        }
        let mut tpl = TopicPartitionList::new();
        for (p, o) in offsets {
            tpl.add_partition_offset(topic, *p, Offset::Offset(*o))
                .unwrap();
        }
        commit_retrying(&c, &tpl);
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let thread = {
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    c.poll(Duration::from_millis(100));
                }
            })
        };
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for Member {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
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
