//! The event browser's Kafka reads (ADR 0025): search, one record, the tail and the consumer
//! groups with their lag, over `KRAKEN_HUB_KAFKA_BOOTSTRAP`, on the topics of
//! `KRAKEN_HUB_EVENT_TOPICS` only.
//!
//! The hub reads and never writes: every consumer is short-lived, `assign`s and seeks, never
//! subscribes and never commits (`enable.auto.commit=false`), so no consumer group exists for the
//! hub; its `group.id` (`kraken-hub-browser-<hub id>`) is only what librdkafka requires, and the
//! consumer-group view reads another group's committed offsets with an assign-only consumer of that
//! group that does neither either. Every librdkafka call runs on the blocking pool under a deadline
//! (the search budget), so a slow or absent broker never holds the gRPC router or another HTTP
//! handler; the hub needs Kafka for nothing else and starts without it. Calls are bounded in number
//! ([`Config::event_max_searches`], [`Config::event_max_tails`]: one more is 429), in records and in
//! time, and payloads only ever leave through [`Detail`] under [`crate::events::payload_access`].

use std::ffi::CStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::{Offset, TopicPartitionList};
use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::events::{
    Budget, CHUNK, Chunks, Cursor, Detail, Examined, Filters, Hidden, Range, RecordView, StoppedBy,
    Summary, Want, merge,
};
use crate::telemetry::Metrics;

/// The prefix of the `group.id` every browser consumer carries (never joined, never committed).
pub const BROWSER_GROUP_PREFIX: &str = "kraken-hub-browser-";
/// How far before a later-stamped record `to` still looks for a record of its range, in ms:
/// producers set `CreateTime`, so a partition's timestamps need not rise.
pub const TO_SKEW: i64 = 5 * 60 * 1000;
/// The consumer-group view lists at most this many groups (`truncated` beyond).
pub const MAX_GROUPS: usize = 500;
/// Summaries a tail holds for a reader that is behind; one more ends it (`slowReader`).
pub const TAIL_BUFFER: usize = 256;
/// The answer of every browser route while `KRAKEN_HUB_KAFKA_BOOTSTRAP` is unset.
pub const DISABLED: &str = "event browser disabled: set KRAKEN_HUB_KAFKA_BOOTSTRAP";
/// Kafka client warnings are logged at most this often (the others are counted in the next).
const WARN_EVERY: Duration = Duration::from_secs(30);
/// One `poll` waits at most this long, so deadlines and cancellation are seen.
const POLL: Duration = Duration::from_millis(100);
/// Time a blocking call gets beyond its deadline to close its consumer before the request gives
/// up on it (the call still ends on its own, holding its slot until then); also the longest a
/// single Kafka request may wait for its answer.
pub const CLOSE_GRACE: Duration = Duration::from_secs(5);

/// Why a browser call answered no data; the HTTP layer maps each to a status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// `KRAKEN_HUB_KAFKA_BOOTSTRAP` is unset: 404.
    Disabled,
    /// 400.
    BadRequest(String),
    /// A payload filter (`q`) without the payload rules: 403.
    Forbidden(Hidden),
    /// 404.
    NotFound(String),
    /// Every slot of its kind is taken: 429.
    TooMany(String),
    /// Kafka did not answer, or answered an error: 503.
    Unavailable(String),
}

impl Refusal {
    /// The `outcome` label of `kraken_hub_event_browser_requests_total`.
    pub fn outcome(&self) -> &'static str {
        match self {
            Refusal::Disabled => "disabled",
            Refusal::BadRequest(_) => "bad_request",
            Refusal::Forbidden(_) => "forbidden",
            Refusal::NotFound(_) => "not_found",
            Refusal::TooMany(_) => "too_many",
            Refusal::Unavailable(_) => "unavailable",
        }
    }
}

fn unavailable(e: KafkaError) -> Refusal {
    Refusal::Unavailable(format!("Kafka is unavailable: {e}"))
}

fn late() -> Refusal {
    Refusal::Unavailable("Kafka did not answer in time".to_string())
}

/// What is left until `deadline`; [`late`] when nothing is.
fn left(deadline: Instant) -> Result<Duration, Refusal> {
    let left = deadline.saturating_duration_since(Instant::now());
    if left.is_zero() {
        Err(late())
    } else {
        Ok(left)
    }
}

/// Kafka client warnings, at most one line per [`WARN_EVERY`].
#[derive(Default)]
struct Warnings {
    /// When the last line was written, and how many were held back since.
    state: Mutex<(Option<Instant>, u64)>,
}

impl Warnings {
    fn warn(&self, facility: &str, detail: &str) {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        if state
            .0
            .is_some_and(|last| now.duration_since(last) < WARN_EVERY)
        {
            state.1 += 1;
            return;
        }
        let suppressed = std::mem::take(&mut state.1);
        state.0 = Some(now);
        drop(state);
        tracing::warn!(
            facility,
            error = detail,
            suppressed,
            "Kafka client error (event browser)"
        );
    }
}

/// The librdkafka context of every browser client: its log lines and errors go through
/// [`Warnings`] (they never carry a payload).
struct Context {
    warnings: Arc<Warnings>,
}

impl ClientContext for Context {
    fn log(&self, level: RDKafkaLogLevel, facility: &str, message: &str) {
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error
            | RDKafkaLogLevel::Warning => self.warnings.warn(facility, message),
            _ => tracing::debug!(facility, message, "librdkafka (event browser)"),
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.warnings.warn("error", &format!("{error}: {reason}"));
    }
}

impl ConsumerContext for Context {}

type Client = BaseConsumer<Context>;

/// How to reach Kafka; cloned into every blocking call.
#[derive(Clone)]
struct Kafka {
    bootstrap: String,
    client_id: String,
    group: String,
    warnings: Arc<Warnings>,
}

impl Kafka {
    /// The settings of a consumer of `group` that only ever assigns: no commit, no subscription,
    /// no topic creation; fetches of about a chunk, so a search holds little memory. No request to
    /// Kafka waits longer than [`CLOSE_GRACE`] (librdkafka's default is 60 s), so a consumer
    /// closed while a broker hangs is gone within the grace and its call's slot with it.
    fn settings(&self, group: &str) -> ClientConfig {
        let request_timeout = CLOSE_GRACE.as_millis().to_string();
        let mut settings = ClientConfig::new();
        settings
            .set("bootstrap.servers", &self.bootstrap)
            .set("client.id", &self.client_id)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "true")
            .set("allow.auto.create.topics", "false")
            .set("auto.offset.reset", "earliest")
            // A fetch brings a partition about one chunk, and the consumer keeps about one chunk
            // ahead per partition: a search's broker traffic follows the records it examines.
            .set("max.partition.fetch.bytes", "262144")
            .set("queued.max.messages.kbytes", "256")
            .set("queued.min.messages", CHUNK.to_string())
            .set("fetch.max.bytes", "4194304")
            .set("fetch.wait.max.ms", "100")
            .set("socket.timeout.ms", &request_timeout)
            .set("socket.connection.setup.timeout.ms", &request_timeout)
            .set("api.version.request.timeout.ms", &request_timeout)
            .set_log_level(RDKafkaLogLevel::Warning);
        settings
    }

    /// A consumer of `group` with [`Kafka::settings`].
    fn consumer(&self, group: &str) -> Result<Client, Refusal> {
        self.settings(group)
            .create_with_context(Context {
                warnings: Arc::clone(&self.warnings),
            })
            .map_err(|e| Refusal::Unavailable(format!("cannot create a Kafka client: {e}")))
    }

    /// The browser's own consumer.
    fn browser(&self) -> Result<Client, Refusal> {
        self.consumer(&self.group)
    }
}

/// The partitions of `topic`, sorted; `None` when the topic does not exist.
fn partitions(c: &Client, topic: &str, deadline: Instant) -> Result<Option<Vec<i32>>, Refusal> {
    let metadata = c
        .fetch_metadata(Some(topic), left(deadline)?)
        .map_err(unavailable)?;
    let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) else {
        return Ok(None);
    };
    match t.error().map(RDKafkaErrorCode::from) {
        Some(RDKafkaErrorCode::UnknownTopicOrPartition) => Ok(None),
        Some(code) => Err(Refusal::Unavailable(format!(
            "Kafka is unavailable: topic {topic}: {code}"
        ))),
        None if t.partitions().is_empty() => Ok(None),
        None => {
            let mut ids: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
            ids.sort_unstable();
            Ok(Some(ids))
        }
    }
}

/// `(low, high)` watermarks of `partition`.
fn watermarks(
    c: &Client,
    topic: &str,
    partition: i32,
    deadline: Instant,
) -> Result<(i64, i64), Refusal> {
    c.fetch_watermarks(topic, partition, left(deadline)?)
        .map_err(unavailable)
}

/// For every partition, the first offset whose record timestamp is at least `ts` (`None` when
/// there is none).
fn offsets_for(
    c: &Client,
    topic: &str,
    parts: &[i32],
    ts: i64,
    deadline: Instant,
) -> Result<Vec<(i32, Option<i64>)>, Refusal> {
    let mut tpl = TopicPartitionList::new();
    for p in parts {
        tpl.add_partition_offset(topic, *p, Offset::Offset(ts))
            .map_err(unavailable)?;
    }
    let found = c
        .offsets_for_times(tpl, left(deadline)?)
        .map_err(unavailable)?;
    Ok(parts
        .iter()
        .map(|p| {
            let offset = found
                .find_partition(topic, *p)
                .and_then(|e| match e.offset() {
                    Offset::Offset(o) => Some(o),
                    _ => None,
                });
            (*p, offset)
        })
        .collect())
}

/// Every header of `m`, names as bytes. rdkafka's `Headers` reads a name as `&str` and panics on
/// one that is not UTF-8 (any client may write one), so they are read from librdkafka directly.
fn headers_of<'a>(m: &'a BorrowedMessage<'_>) -> Vec<(&'a [u8], Option<&'a [u8]>)> {
    use rdkafka::bindings as rd;
    use rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_NO_ERROR as NO_ERROR;
    let mut out = Vec::new();
    let mut headers: *mut rd::rd_kafka_headers_t = std::ptr::null_mut();
    // SAFETY: `m.ptr()` is the live message `m` borrows. librdkafka owns its headers, and the
    // name and value bytes they point to, until the message is destroyed, which cannot happen
    // while `m` (and so `'a`) lives. Names are NUL-terminated C strings; values are `size` bytes.
    unsafe {
        if rd::rd_kafka_message_headers(m.ptr(), &mut headers) != NO_ERROR || headers.is_null() {
            return out;
        }
        for idx in 0.. {
            let mut name: *const std::ffi::c_char = std::ptr::null();
            let mut value: *const std::ffi::c_void = std::ptr::null();
            let mut size = 0usize;
            if rd::rd_kafka_header_get_all(headers, idx, &mut name, &mut value, &mut size)
                != NO_ERROR
            {
                break;
            }
            let name = CStr::from_ptr(name).to_bytes();
            let value = (!value.is_null()).then(|| std::slice::from_raw_parts(value.cast(), size));
            out.push((name, value));
        }
    }
    out
}

/// Calls `f` with the [`RecordView`] of `m`.
fn with_view<T>(m: &BorrowedMessage<'_>, f: impl FnOnce(&RecordView<'_>) -> T) -> T {
    let headers = headers_of(m);
    f(&RecordView {
        partition: m.partition(),
        offset: m.offset(),
        timestamp: m.timestamp().to_millis(),
        key: m.key(),
        headers: &headers,
        payload: m.payload(),
    })
}

/// Assigns `partition` of `topic` from `offset`.
fn assign(c: &Client, topic: &str, partition: i32, offset: i64) -> Result<(), Refusal> {
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(offset))
        .map_err(unavailable)?;
    c.assign(&tpl).map_err(unavailable)
}

/// Reads the chunks of a search: one `assign` of every chunk's partition at the chunk's start,
/// then the records as the broker sends them, until every chunk reached its end.
struct Reader<'a> {
    consumer: &'a Client,
    topic: &'a str,
    filters: &'a Filters,
    warnings: &'a Warnings,
}

impl Chunks for Reader<'_> {
    type Error = Refusal;

    fn read(
        &mut self,
        wants: &[Want],
        deadline: Instant,
    ) -> Result<Vec<Option<Vec<Examined>>>, Refusal> {
        let mut tpl = TopicPartitionList::new();
        for w in wants {
            tpl.add_partition_offset(self.topic, w.partition, Offset::Offset(w.start))
                .map_err(unavailable)?;
        }
        self.consumer.assign(&tpl).map_err(unavailable)?;
        let mut chunks: Vec<Vec<Examined>> = wants
            .iter()
            .map(|w| Vec::with_capacity((w.end - w.start).clamp(0, CHUNK as i64) as usize))
            .collect();
        let mut complete = vec![false; wants.len()];
        let mut open = wants.len();
        let at = |partition: i32| wants.iter().position(|w| w.partition == partition);
        while open > 0 {
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break;
            }
            let ended = match self.consumer.poll(wait.min(POLL)) {
                None => None,
                Some(Ok(m)) => match at(m.partition()) {
                    Some(i) if !complete[i] && m.offset() >= wants[i].start => {
                        if m.offset() < wants[i].end {
                            chunks[i].push(with_view(&m, |r| Examined {
                                offset: r.offset,
                                timestamp: r.timestamp.unwrap_or(-1),
                                hit: self.filters.matches(r).then(|| Summary::of(self.topic, r)),
                            }));
                        }
                        (m.offset() >= wants[i].end - 1).then_some(i)
                    }
                    _ => None,
                },
                Some(Err(KafkaError::PartitionEOF(p))) => at(p).filter(|i| !complete[*i]),
                Some(Err(e)) => {
                    self.warnings.warn("fetch", &e.to_string());
                    None
                }
            };
            if let Some(i) = ended {
                complete[i] = true;
                open -= 1;
            }
        }
        Ok(chunks
            .into_iter()
            .zip(complete)
            .map(|(chunk, complete)| complete.then_some(chunk))
            .collect())
    }
}

/// `GET /api/v1/events`.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchView {
    pub topic: String,
    /// The topic is configured but does not exist.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub missing: bool,
    pub results: Vec<Summary>,
    pub scanned: u64,
    pub exhausted: bool,
    pub stopped_by: Option<StoppedBy>,
    /// Continues this search (send it with the same topic and filters); `null` when exhausted.
    pub cursor: Option<String>,
}

/// One partition of a consumer group.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GroupPartition {
    pub partition: i32,
    /// `null` when the group committed nothing on this partition.
    pub committed: Option<i64>,
    pub high_watermark: i64,
    /// `highWatermark - committed`; `null` without a committed offset.
    pub lag: Option<i64>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GroupView {
    pub group_id: String,
    /// As the broker reports it (`Stable`, `Empty`, `PreparingRebalance`, ...).
    pub state: String,
    pub members: usize,
    pub topic: String,
    pub partitions: Vec<GroupPartition>,
    /// The sum of the known lags.
    pub lag: i64,
}

/// `GET /api/v1/consumer-groups`.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GroupsView {
    pub topic: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub missing: bool,
    /// Every group with a committed offset on the topic, by lag (descending), then name.
    pub groups: Vec<GroupView>,
    /// More than [`MAX_GROUPS`] groups, or the time budget ran out: some are not listed.
    pub truncated: bool,
}

/// An open tail: its records as they come, and why it ended.
pub struct Tail {
    pub partitions: usize,
    pub records: mpsc::Receiver<Summary>,
    /// `maxDuration`, `slowReader` or `shutdown`; dropped without a value when the reader left.
    pub end: oneshot::Receiver<&'static str>,
    pub ends_at_ms: i64,
}

/// The event browser of one hub.
pub struct Browser {
    config: Arc<Config>,
    metrics: Arc<Metrics>,
    kafka: Option<Kafka>,
    reads: Arc<Semaphore>,
    tails: Arc<Semaphore>,
}

impl Browser {
    pub fn new(config: Arc<Config>, metrics: Arc<Metrics>) -> Arc<Self> {
        let kafka = config.kafka_bootstrap.as_ref().map(|bootstrap| Kafka {
            bootstrap: bootstrap.clone(),
            client_id: format!("kraken-hub-{}", config.hub_id),
            group: format!("{BROWSER_GROUP_PREFIX}{}", config.hub_id),
            warnings: Arc::default(),
        });
        Arc::new(Self {
            reads: Arc::new(Semaphore::new(config.event_max_searches)),
            tails: Arc::new(Semaphore::new(config.event_max_tails)),
            config,
            metrics,
            kafka,
        })
    }

    pub fn enabled(&self) -> bool {
        self.kafka.is_some()
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Counts one call (`kraken_hub_event_browser_requests_total`).
    pub fn count(&self, kind: &'static str, outcome: &'static str) {
        self.metrics.event_request(kind, outcome);
    }

    /// One payload served: the audit line and the counter, never the payload.
    pub fn payload_served(
        &self,
        caller: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        size: usize,
    ) {
        self.metrics.event_payload_view();
        tracing::info!(
            caller,
            topic,
            partition,
            offset,
            size,
            "event payload served"
        );
    }

    fn kafka(&self) -> Result<&Kafka, Refusal> {
        self.kafka.as_ref().ok_or(Refusal::Disabled)
    }

    /// The topic a call reads: `requested`, which must be configured, else the first configured.
    pub fn topic(&self, requested: Option<&str>) -> Result<String, Refusal> {
        self.kafka()?;
        match requested.filter(|t| !t.is_empty()) {
            None => Ok(self.config.event_topics[0].clone()),
            Some(t) if self.config.event_topics.iter().any(|c| c == t) => Ok(t.to_string()),
            Some(t) => Err(Refusal::BadRequest(format!(
                "topic {t:?} is not browsable here; KRAKEN_HUB_EVENT_TOPICS lists {}",
                self.config.event_topics.join(", ")
            ))),
        }
    }

    /// A slot for a search, a record read or a group listing.
    fn read_slot(&self) -> Result<OwnedSemaphorePermit, Refusal> {
        Arc::clone(&self.reads).try_acquire_owned().map_err(|_| {
            Refusal::TooMany(format!(
                "{} event searches are running on this hub (KRAKEN_HUB_EVENT_MAX_SEARCHES); try again shortly",
                self.config.event_max_searches
            ))
        })
    }

    /// Runs `f` on the blocking pool, holding `slot` until it returns, and waits for it at most
    /// `budget` plus [`CLOSE_GRACE`].
    async fn blocking<T: Send + 'static>(
        slot: OwnedSemaphorePermit,
        budget: Duration,
        f: impl FnOnce() -> Result<T, Refusal> + Send + 'static,
    ) -> Result<T, Refusal> {
        let task = tokio::task::spawn_blocking(move || {
            let _slot = slot;
            f()
        });
        match tokio::time::timeout(budget + CLOSE_GRACE, task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(Refusal::Unavailable("the Kafka read failed".to_string())),
            Err(_) => Err(late()),
        }
    }

    /// One page of a search of `topic`, newest first.
    pub async fn search(
        &self,
        topic: String,
        filters: Filters,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<SearchView, Refusal> {
        let kafka = self.kafka()?.clone();
        let slot = self.read_slot()?;
        let budget = self.config.event_search_timeout;
        let max_scanned = self.config.event_search_max_scanned;
        let view = Self::blocking(slot, budget, move || {
            let deadline = Instant::now() + budget;
            let c = kafka.browser()?;
            let Some(parts) = partitions(&c, &topic, deadline)? else {
                return Ok(SearchView {
                    topic,
                    missing: true,
                    results: vec![],
                    scanned: 0,
                    exhausted: true,
                    stopped_by: None,
                    cursor: None,
                });
            };
            let lowest = filters
                .from
                .map(|ts| offsets_for(&c, &topic, &parts, ts, deadline))
                .transpose()?;
            // The first record stamped after `to` + TO_SKEW bounds the range; every record is
            // still compared with `to` itself.
            let highest = filters
                .to
                .map(|ts| {
                    let bound = ts.saturating_add(TO_SKEW).saturating_add(1);
                    offsets_for(&c, &topic, &parts, bound, deadline)
                })
                .transpose()?;
            let mut ranges = Vec::with_capacity(parts.len());
            for (i, p) in parts.iter().enumerate() {
                let (low, high) = watermarks(&c, &topic, *p, deadline)?;
                let lower = match &lowest {
                    None => low,
                    Some(found) => found[i].1.map_or(high, |o| o.max(low)),
                };
                let mut upper = match &highest {
                    None => high,
                    Some(found) => found[i].1.map_or(high, |o| o.min(high)),
                };
                if let Some(cursor) = &cursor {
                    // A partition the cursor does not name was not there on the first page.
                    upper = cursor
                        .positions
                        .iter()
                        .find(|(cp, _)| cp == p)
                        .map_or(lower, |(_, o)| (*o).min(upper));
                }
                ranges.push(Range {
                    partition: *p,
                    lower,
                    upper,
                });
            }
            let mut reader = Reader {
                consumer: &c,
                topic: &topic,
                filters: &filters,
                warnings: &kafka.warnings,
            };
            let page = merge(
                &mut reader,
                &ranges,
                Budget {
                    limit,
                    max_scanned,
                    deadline,
                },
            )?;
            let cursor = (!page.exhausted)
                .then(|| Cursor::new(filters.digest(&topic), page.positions).encode());
            Ok(SearchView {
                topic,
                missing: false,
                results: page.results,
                scanned: page.scanned,
                exhausted: page.exhausted,
                stopped_by: page.stopped_by,
                cursor,
            })
        })
        .await?;
        self.metrics.event_scanned(view.scanned);
        Ok(view)
    }

    /// The record at `offset` of `partition`, its payload under `access`.
    pub async fn record(
        &self,
        topic: String,
        partition: i32,
        offset: i64,
        access: Result<(), Hidden>,
    ) -> Result<Detail, Refusal> {
        let kafka = self.kafka()?.clone();
        let slot = self.read_slot()?;
        let budget = self.config.event_search_timeout;
        Self::blocking(slot, budget, move || {
            let deadline = Instant::now() + budget;
            let c = kafka.browser()?;
            let Some(parts) = partitions(&c, &topic, deadline)? else {
                return Err(Refusal::NotFound(format!("topic {topic} does not exist")));
            };
            if !parts.contains(&partition) {
                return Err(Refusal::NotFound(format!(
                    "topic {topic} has no partition {partition}"
                )));
            }
            let (low, high) = watermarks(&c, &topic, partition, deadline)?;
            if !(low..high).contains(&offset) {
                return Err(Refusal::NotFound(format!(
                    "offset {offset} is outside partition {partition} of {topic} (offsets {low} to {})",
                    high - 1
                )));
            }
            assign(&c, &topic, partition, offset)?;
            let gone = || {
                Refusal::NotFound(format!(
                    "no record at offset {offset} of partition {partition} of {topic} (compacted or deleted)"
                ))
            };
            loop {
                match c.poll(left(deadline)?.min(POLL)) {
                    None => {}
                    Some(Ok(m)) if m.partition() == partition && m.offset() == offset => {
                        return Ok(with_view(&m, |r| Detail::of(&topic, r, access)));
                    }
                    Some(Ok(m)) if m.partition() == partition && m.offset() > offset => {
                        return Err(gone());
                    }
                    Some(Ok(_)) => {}
                    Some(Err(KafkaError::PartitionEOF(p))) if p == partition => {
                        return Err(gone());
                    }
                    Some(Err(KafkaError::PartitionEOF(_))) => {}
                    Some(Err(e)) => kafka.warnings.warn("fetch", &e.to_string()),
                }
            }
        })
        .await
    }

    /// Starts a tail of `topic` at the high watermark of every partition. Its records come as
    /// summaries (never a payload) for at most [`Config::event_tail_duration`]; a reader that
    /// falls [`TAIL_BUFFER`] records behind ends it, and so does `stopping`.
    pub async fn tail(
        &self,
        topic: String,
        filters: Filters,
        stopping: CancellationToken,
    ) -> Result<Tail, Refusal> {
        let kafka = self.kafka()?.clone();
        let slot = Arc::clone(&self.tails).try_acquire_owned().map_err(|_| {
            Refusal::TooMany(format!(
                "{} event tails are open on this hub (KRAKEN_HUB_EVENT_MAX_TAILS); try again later",
                self.config.event_max_tails
            ))
        })?;
        let budget = self.config.event_search_timeout;
        let duration = self.config.event_tail_duration;
        let (started_tx, started) = oneshot::channel();
        let (tx, records) = mpsc::channel(TAIL_BUFFER);
        let (end_tx, end) = oneshot::channel();
        let metrics = Arc::clone(&self.metrics);
        // One blocking call holds the slot and the consumer from the setup to the end, so a
        // request that leaves (or gives up waiting) during the setup never frees a slot while
        // Kafka work still runs for it, and the consumer is closed on this thread.
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            let setup = || {
                let deadline = Instant::now() + budget;
                let c = kafka.browser()?;
                let Some(parts) = partitions(&c, &topic, deadline)? else {
                    return Err(Refusal::NotFound(format!("topic {topic} does not exist")));
                };
                let mut tpl = TopicPartitionList::new();
                for p in &parts {
                    let (_, high) = watermarks(&c, &topic, *p, deadline)?;
                    tpl.add_partition_offset(&topic, *p, Offset::Offset(high))
                        .map_err(unavailable)?;
                }
                c.assign(&tpl).map_err(unavailable)?;
                Ok((c, parts.len()))
            };
            let c = match setup() {
                Ok((c, parts)) => {
                    let ends = std::time::SystemTime::now() + duration;
                    let ends_at_ms = ends
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_millis() as i64);
                    if started_tx.send(Ok((parts, ends_at_ms))).is_err() {
                        // The request left during the setup.
                        return;
                    }
                    c
                }
                Err(refusal) => {
                    let _ = started_tx.send(Err(refusal));
                    return;
                }
            };
            metrics.event_tails_add(1);
            let deadline = Instant::now() + duration;
            let reason = loop {
                if stopping.is_cancelled() {
                    break Some("shutdown");
                }
                if tx.is_closed() {
                    break None;
                }
                if Instant::now() >= deadline {
                    break Some("maxDuration");
                }
                match c.poll(POLL) {
                    None | Some(Err(KafkaError::PartitionEOF(_))) => {}
                    Some(Err(e)) => kafka.warnings.warn("fetch", &e.to_string()),
                    Some(Ok(m)) => {
                        let Some(summary) =
                            with_view(&m, |r| filters.matches(r).then(|| Summary::of(&topic, r)))
                        else {
                            continue;
                        };
                        match tx.try_send(summary) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => break Some("slowReader"),
                            Err(mpsc::error::TrySendError::Closed(_)) => break None,
                        }
                    }
                }
            };
            if let Some(reason) = reason {
                let _ = end_tx.send(reason);
            }
            drop(c);
            metrics.event_tails_add(-1);
        });
        let (parts, ends_at_ms) = match tokio::time::timeout(budget + CLOSE_GRACE, started).await {
            Ok(Ok(started)) => started?,
            Ok(Err(_)) => return Err(Refusal::Unavailable("the Kafka read failed".into())),
            Err(_) => return Err(late()),
        };
        Ok(Tail {
            partitions: parts,
            records,
            end,
            ends_at_ms,
        })
    }

    /// Every consumer group with a committed offset on `topic`, with its lag.
    pub async fn groups(&self, topic: String) -> Result<GroupsView, Refusal> {
        let kafka = self.kafka()?.clone();
        let slot = self.read_slot()?;
        let budget = self.config.event_search_timeout;
        Self::blocking(slot, budget, move || {
            let deadline = Instant::now() + budget;
            let c = kafka.browser()?;
            let Some(parts) = partitions(&c, &topic, deadline)? else {
                return Ok(GroupsView {
                    topic,
                    missing: true,
                    groups: vec![],
                    truncated: false,
                });
            };
            let mut highs = Vec::with_capacity(parts.len());
            for p in &parts {
                highs.push(watermarks(&c, &topic, *p, deadline)?.1);
            }
            let list = c
                .fetch_group_list(None, left(deadline)?)
                .map_err(unavailable)?;
            let mut known: Vec<(String, String, usize)> = list
                .groups()
                .iter()
                .filter(|g| !g.name().starts_with(BROWSER_GROUP_PREFIX))
                .map(|g| {
                    (
                        g.name().to_string(),
                        g.state().to_string(),
                        g.members().len(),
                    )
                })
                .collect();
            drop(c);
            known.sort();
            let mut truncated = known.len() > MAX_GROUPS;
            known.truncate(MAX_GROUPS);
            let mut groups = Vec::new();
            for (name, state, members) in known {
                let Ok(wait) = left(deadline) else {
                    truncated = true;
                    break;
                };
                let member = kafka.consumer(&name)?;
                let mut tpl = TopicPartitionList::new();
                for p in &parts {
                    tpl.add_partition(&topic, *p);
                }
                let committed = match member.committed_offsets(tpl, wait) {
                    Ok(committed) => committed,
                    Err(e) => {
                        kafka.warnings.warn("offsets", &format!("{name}: {e}"));
                        continue;
                    }
                };
                let partitions: Vec<GroupPartition> = parts
                    .iter()
                    .zip(&highs)
                    .map(|(p, high)| {
                        let committed = committed.find_partition(&topic, *p).and_then(|e| match e
                            .offset()
                        {
                            Offset::Offset(o) if o >= 0 => Some(o),
                            _ => None,
                        });
                        GroupPartition {
                            partition: *p,
                            committed,
                            high_watermark: *high,
                            lag: committed.map(|c| (high - c).max(0)),
                        }
                    })
                    .collect();
                if partitions.iter().all(|p| p.committed.is_none()) {
                    continue;
                }
                groups.push(GroupView {
                    lag: partitions.iter().filter_map(|p| p.lag).sum(),
                    group_id: name,
                    state,
                    members,
                    topic: topic.clone(),
                    partitions,
                });
            }
            groups.sort_by(|a, b| b.lag.cmp(&a.lag).then_with(|| a.group_id.cmp(&b.group_id)));
            Ok(GroupsView {
                topic,
                missing: false,
                groups,
                truncated,
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kafka_warnings_are_rate_limited_and_count_what_they_held_back() {
        crate::telemetry::test_support::capture_logs();
        let warnings = Warnings::default();
        for _ in 0..5 {
            warnings.warn("fail", "Connect to ipv4#127.0.0.1:1 failed");
        }
        let lines = crate::telemetry::test_support::logged();
        let lines: Vec<_> = lines
            .iter()
            .filter(|l| l.message == "Kafka client error (event browser)")
            .collect();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert_eq!(lines[0].field("suppressed"), Some("0"));
        assert_eq!(warnings.state.lock().unwrap().1, 4);
    }

    #[tokio::test]
    async fn a_disabled_browser_refuses_everything_without_touching_kafka() {
        let browser = Browser::new(Arc::new(Config::default()), Metrics::noop());
        assert!(!browser.enabled());
        assert_eq!(browser.topic(None), Err(Refusal::Disabled));
        let search = browser
            .search("kraken.events".into(), Filters::default(), 10, None)
            .await;
        assert_eq!(search, Err(Refusal::Disabled));
        assert_eq!(
            browser.groups("kraken.events".into()).await,
            Err(Refusal::Disabled)
        );
    }

    #[test]
    fn only_configured_topics_are_browsable_and_the_first_is_the_default() {
        let config = Config {
            kafka_bootstrap: Some("kafka:9092".into()),
            event_topics: vec!["a".into(), "a.dlt".into()],
            ..Config::default()
        };
        let browser = Browser::new(Arc::new(config), Metrics::noop());
        assert_eq!(browser.topic(None).as_deref(), Ok("a"));
        assert_eq!(browser.topic(Some("")).as_deref(), Ok("a"));
        assert_eq!(browser.topic(Some("a.dlt")).as_deref(), Ok("a.dlt"));
        assert!(matches!(
            browser.topic(Some("b")),
            Err(Refusal::BadRequest(_))
        ));
    }

    /// The broker sends a partition about one chunk per fetch, and the consumer keeps about one
    /// chunk ahead per partition, so a search's broker traffic follows the records it examines.
    #[test]
    fn a_consumer_fetches_about_one_chunk_per_partition() {
        let kafka = Kafka {
            bootstrap: "127.0.0.1:1".into(),
            client_id: "kraken-hub-test".into(),
            group: format!("{BROWSER_GROUP_PREFIX}test"),
            warnings: Arc::default(),
        };
        let settings = kafka.settings(&kafka.group);
        assert_eq!(settings.get("max.partition.fetch.bytes"), Some("262144"));
        assert_eq!(settings.get("queued.max.messages.kbytes"), Some("256"));
        assert_eq!(
            settings.get("queued.min.messages"),
            Some(CHUNK.to_string().as_str())
        );
        assert!(kafka.browser().is_ok(), "librdkafka takes the settings");
    }

    #[test]
    fn read_slots_are_bounded_and_given_back() {
        let config = Config {
            kafka_bootstrap: Some("kafka:9092".into()),
            event_max_searches: 2,
            ..Config::default()
        };
        let browser = Browser::new(Arc::new(config), Metrics::noop());
        let a = browser.read_slot().unwrap();
        let _b = browser.read_slot().unwrap();
        assert!(matches!(browser.read_slot(), Err(Refusal::TooMany(_))));
        drop(a);
        assert!(browser.read_slot().is_ok());
    }
}
