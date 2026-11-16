//! The dashboard's event pages over the event browser add-on (ADR 0027): the hub reads no event
//! store itself. Each `/api/v1/events*` call becomes one hub-internal query
//! ([`crate::router::Router::query_internal`], `stream_internal` for the tail) to a
//! `kraken-event-browser` instance: `kraken.events.Search`, `Get`, `Sources`, `Lag` and `Tail`.
//!
//! This module builds those requests from the HTTP call, reads the browser's answers as untrusted
//! input (bounded in size, parsed into the hub's own types: whatever the hub does not know is
//! dropped, malformed JSON is an error, never a panic), maps the browser's errors to HTTP
//! statuses, and keeps the payload rules of ADR 0025: a payload (and the `q` filter) is asked for
//! only when the caller may see it, and a payload the hub did not ask for is dropped.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value, json};

use crate::config::Config;
use crate::context::ContextId;
use crate::proto::ErrorCode;
use crate::registry::Registry;
use crate::router::RouteError;

/// The event browser's query types (ADR 0027).
pub const SEARCH: &str = "kraken.events.Search";
pub const GET: &str = "kraken.events.Get";
pub const SOURCES: &str = "kraken.events.Sources";
pub const LAG: &str = "kraken.events.Lag";
/// The streaming one.
pub const TAIL: &str = "kraken.events.Tail";

/// The answer of every `/api/v1/events*` call while no browser is connected.
pub const NOT_INSTALLED: &str = "event browser not installed";

/// What the hub asks as the timeout of a point query (`Request.timeout_ms`); the router clamps
/// it as any client's (`KRAKEN_HUB_MAX_TIMEOUT_MS`, `KRAKEN_HUB_TIMEOUT_OVERRIDES`).
pub const QUERY_TIMEOUT: Duration = Duration::from_secs(30);
/// A search's own time budget (`timeoutMs`) ends this long before the hub stops waiting, so the
/// browser answers what it found rather than the hub timing out.
pub const ANSWER_MARGIN: Duration = Duration::from_secs(2);
/// The longest answer of a point query the hub reads: a record with a 256 KiB payload fits even
/// fully JSON-escaped (the gRPC transport itself stops a message at 4 MiB).
pub const MAX_ANSWER: usize = 2 * 1024 * 1024;
/// The longest tail part the hub reads (a summary is a few KiB at most).
pub const MAX_PART: usize = 64 * 1024;
/// A browser's error message is cut to this many bytes.
pub const MAX_MESSAGE: usize = 512;
/// The longest correlation id the hub takes from a request (`X-Correlation-Id`).
pub const MAX_CORRELATION_ID: usize = 128;
/// A filter value and a source are at most this many bytes (the browser's bound, checked
/// before asking).
pub const MAX_FILTER: usize = 1024;
/// A cursor is at most this many bytes (the browser's bound: it holds a place per segment).
pub const MAX_CURSOR: usize = 64 * 1024;
/// A segment and a position are at most this many bytes (the browser's bound).
pub const MAX_PLACE: usize = 128;
/// The heartbeat the hub asks of a tail at most (its SSE heartbeat of ADR 0025)...
pub const MAX_HEARTBEAT: Duration = Duration::from_secs(15);
/// ...and at least (the browser's own floor).
pub const MIN_HEARTBEAT: Duration = Duration::from_millis(100);

/// The record headers the libraries write, the only ones a summary carries.
pub const LIBRARY_HEADERS: [&str; 7] = [
    "type",
    "id",
    "correlationId",
    "timestamp",
    "source",
    "userId",
    "traceparent",
];

/// True while an event browser is connected: a live instance handles [`SEARCH`] (not a type
/// kept through its owner's grace after the last browser left, when every call is a 404).
pub fn installed(registry: &Registry) -> bool {
    registry.has_live_handler(&ContextId::default(), SEARCH)
}

/// The request's correlation id when it brings a usable one (1 to [`MAX_CORRELATION_ID`] visible
/// ASCII characters), else a fresh one.
pub fn correlation_id(presented: Option<&str>) -> String {
    match presented {
        Some(id)
            if !id.is_empty()
                && id.len() <= MAX_CORRELATION_ID
                && id.bytes().all(|b| b.is_ascii_graphic()) =>
        {
            id.to_string()
        }
        _ => uuid::Uuid::new_v4().to_string(),
    }
}

/// The time budget a search asks of the browser within the hub's `deadline`.
pub fn search_budget(deadline: Duration) -> Duration {
    if deadline > ANSWER_MARGIN * 2 {
        deadline - ANSWER_MARGIN
    } else {
        (deadline / 2).max(Duration::from_millis(1))
    }
}

/// The heartbeat a tail asks of the browser: two fifths of the idle timeout of the hub's
/// internal stream (a quiet tail must never idle out; the browser sends one only after that long
/// without a part), within [`MIN_HEARTBEAT`] and [`MAX_HEARTBEAT`].
pub fn tail_heartbeat(config: &Config) -> Duration {
    (config.effective_timeout(TAIL, 0) * 2 / 5).clamp(MIN_HEARTBEAT, MAX_HEARTBEAT)
}

/// `value` when it is at most `max` bytes, else the reason for a 400.
fn bounded<'a>(name: &str, value: &'a str, max: usize) -> Result<&'a str, String> {
    if value.len() > max {
        Err(format!("{name} is longer than {max} bytes"))
    } else {
        Ok(value)
    }
}

/// The longest tail the hub asks for: a little under `KRAKEN_HUB_MAX_STREAM_DURATION`, so the
/// browser ends it (`maxDuration`) before the router would.
pub fn tail_duration(config: &Config) -> Duration {
    let max = config.max_stream_duration;
    if max > ANSWER_MARGIN * 2 {
        max - ANSWER_MARGIN
    } else {
        max / 2
    }
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX).max(1)
}

// ---------------------------------------------------------------------------------------------
// Payload rules (ADR 0025)

/// Why a payload is not served: the first condition of ADR 0025 that does not hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Hidden {
    /// `KRAKEN_HUB_EVENT_BROWSER_PAYLOADS` is not on.
    Off,
    /// The hub has neither a dashboard token nor a write token.
    NoToken,
    /// The request presented neither.
    NotPresented,
}

impl Hidden {
    /// The stable code the API answers (`payload.hidden`, and `hidden` in a refused `q`).
    pub fn code(self) -> &'static str {
        match self {
            Hidden::Off => "payloads-off",
            Hidden::NoToken => "no-token-configured",
            Hidden::NotPresented => "no-token-presented",
        }
    }

    pub fn reason(self) -> &'static str {
        match self {
            Hidden::Off => "payloads are off on this hub (KRAKEN_HUB_EVENT_BROWSER_PAYLOADS)",
            Hidden::NoToken => {
                "this hub has no dashboard or write token, and payloads are only served to a request that presents one"
            }
            Hidden::NotPresented => {
                "payloads are only served to a request that presents the dashboard token or the write token"
            }
        }
    }
}

/// The token `presented` is, as the audit lines name it: `dashboard-token` or `write-token`
/// (compared in constant time); `None` for no token or another one.
pub fn caller(config: &Config, presented: Option<&str>) -> Option<&'static str> {
    let read = config
        .dashboard_token
        .as_ref()
        .is_some_and(|t| t.matches(presented));
    let write = config
        .dashboard_write_token
        .as_ref()
        .is_some_and(|t| t.matches(presented));
    if read {
        Some("dashboard-token")
    } else if write {
        Some("write-token")
    } else {
        None
    }
}

/// Who may see payloads: `Ok` with the caller as the audit line names it when the switch is on,
/// a token is configured and `presented` is one of them.
pub fn payload_access(config: &Config, presented: Option<&str>) -> Result<&'static str, Hidden> {
    if !config.event_payloads {
        return Err(Hidden::Off);
    }
    if config.dashboard_token.is_none() && config.dashboard_write_token.is_none() {
        return Err(Hidden::NoToken);
    }
    caller(config, presented).ok_or(Hidden::NotPresented)
}

// ---------------------------------------------------------------------------------------------
// Requests

/// The query string of a search and of a tail, as sent. Empty values count as absent (a form
/// sends every field). `source` names the event store's source (a topic, a stream); `service`
/// filters on the publishing service (the record header `source`).
#[derive(Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub source: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    #[serde(rename = "type")]
    pub type_name: Option<String>,
    pub key: Option<String>,
    pub service: Option<String>,
    pub correlation_id: Option<String>,
    pub user_id: Option<String>,
    pub event_id: Option<String>,
    pub q: Option<String>,
    pub limit: Option<String>,
    pub cursor: Option<String>,
}

fn present(v: &Option<String>) -> Option<&str> {
    v.as_deref().filter(|s| !s.is_empty())
}

impl Params {
    /// True when the request filters on the payload (`q`): the payload rules decide.
    pub fn wants_payload(&self) -> bool {
        present(&self.q).is_some()
    }

    /// The `filters` of the browser's request; `q` only when `with_q` (the caller may search
    /// payloads). A tail refuses `from`, `to` and `cursor`. Values are the browser's to check.
    fn filters(&self, tail: bool, with_q: bool) -> Result<Map<String, Value>, String> {
        if tail {
            for (name, v) in [
                ("from", &self.from),
                ("to", &self.to),
                ("cursor", &self.cursor),
            ] {
                if present(v).is_some() {
                    return Err(format!("a tail takes no {name}: it starts at the end"));
                }
            }
        }
        let mut filters = Map::new();
        for (name, v) in [
            ("from", &self.from),
            ("to", &self.to),
            ("type", &self.type_name),
            ("key", &self.key),
            ("source", &self.service),
            ("correlationId", &self.correlation_id),
            ("userId", &self.user_id),
            ("eventId", &self.event_id),
        ] {
            if let Some(value) = present(v) {
                let value = bounded(query_name(name), value, MAX_FILTER)?;
                filters.insert(name.to_string(), Value::from(value));
            }
        }
        if with_q && let Some(q) = present(&self.q) {
            filters.insert("q".to_string(), bounded("q", q, MAX_FILTER)?.into());
        }
        Ok(filters)
    }

    fn source_into(&self, request: &mut Map<String, Value>) -> Result<(), String> {
        if let Some(source) = present(&self.source) {
            let source = bounded("source", source, MAX_FILTER)?;
            request.insert("source".to_string(), Value::from(source));
        }
        Ok(())
    }

    /// `kraken.events.Search`: the source, the filters, the page size, the cursor and the time
    /// budget the hub leaves the browser.
    pub fn search_request(&self, with_q: bool, budget: Duration) -> Result<Value, String> {
        let mut request = Map::new();
        self.source_into(&mut request)?;
        request.insert("filters".to_string(), self.filters(false, with_q)?.into());
        if let Some(limit) = present(&self.limit) {
            let limit = limit
                .trim()
                .parse::<u64>()
                .ok()
                .filter(|limit| *limit > 0)
                .ok_or_else(|| "limit must be a positive number".to_string())?;
            request.insert("limit".to_string(), limit.into());
        }
        if let Some(cursor) = present(&self.cursor) {
            let cursor = bounded("cursor", cursor, MAX_CURSOR)?;
            request.insert("cursor".to_string(), Value::from(cursor));
        }
        request.insert("timeoutMs".to_string(), ms(budget).into());
        Ok(request.into())
    }

    /// `kraken.events.Tail`: the source, the filters, the heartbeat and the longest duration.
    pub fn tail_request(
        &self,
        with_q: bool,
        heartbeat: Duration,
        duration: Duration,
    ) -> Result<Value, String> {
        let mut request = Map::new();
        self.source_into(&mut request)?;
        request.insert("filters".to_string(), self.filters(true, with_q)?.into());
        request.insert("heartbeatMs".to_string(), ms(heartbeat).into());
        request.insert("maxDurationMs".to_string(), ms(duration).into());
        Ok(request.into())
    }
}

/// The query-string name of a filter (the publishing service's filter is `service`).
fn query_name(filter: &str) -> &str {
    if filter == "source" {
        "service"
    } else {
        filter
    }
}

/// `kraken.events.Get`: one record, with its payload only when `payload`; `Err` with the reason
/// for a 400 when a part of its place is longer than the browser takes.
pub fn get_request(
    source: &str,
    segment: &str,
    position: &str,
    payload: bool,
) -> Result<Value, String> {
    let source = bounded("source", source, MAX_FILTER)?;
    let segment = bounded("segment", segment, MAX_PLACE)?;
    let position = bounded("position", position, MAX_PLACE)?;
    Ok(json!({ "source": source, "segment": segment, "position": position, "payload": payload }))
}

/// `kraken.events.Lag` of `source` (the browser's default when absent); `Err` with the reason
/// for a 400 when the source is longer than the browser takes.
pub fn lag_request(source: Option<&str>) -> Result<Value, String> {
    Ok(match source.filter(|s| !s.is_empty()) {
        Some(source) => json!({ "source": bounded("source", source, MAX_FILTER)? }),
        None => json!({}),
    })
}

// ---------------------------------------------------------------------------------------------
// Answers (the browser's JSON, ADR 0027): read into the hub's types and written back

/// A record in a search result or the tail: where it is, its key, the library headers and the
/// payload's size. Never the payload: a summary has no field for one.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub source: String,
    pub segment: String,
    pub position: String,
    #[serde(default)]
    pub timestamp: Option<i64>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub key_truncated: bool,
    /// Only the [`LIBRARY_HEADERS`] (see [`Summary::trimmed`]).
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub size: u64,
}

impl Summary {
    /// Keeps only the library headers: a summary never carries another header (the detail lists
    /// them all).
    fn trimmed(mut self) -> Self {
        self.headers
            .retain(|name, _| LIBRARY_HEADERS.contains(&name.as_str()));
        self
    }
}

/// `GET /api/v1/events`: one page of a search, newest first.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SearchPage {
    pub source: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub missing: bool,
    pub results: Vec<Summary>,
    #[serde(default)]
    pub scanned: u64,
    #[serde(default)]
    pub exhausted: bool,
    #[serde(default)]
    pub stopped_by: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
}

impl SearchPage {
    fn trimmed(mut self) -> Self {
        self.results = self.results.into_iter().map(Summary::trimmed).collect();
        self
    }
}

/// One header of the detail view.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HeaderView {
    pub name: String,
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_hex: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

/// A JSON key that may be absent (`None`), `null` (`Some(None)`) or a value.
fn nullable<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<Option<T>>, D::Error> {
    Option::<T>::deserialize(d).map(Some)
}

/// The browser's answer to `kraken.events.Get`.
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RecordAnswer {
    #[serde(flatten)]
    pub summary: Summary,
    #[serde(default)]
    pub all_headers: Vec<HeaderView>,
    #[serde(default)]
    pub headers_omitted: u64,
    #[serde(default, deserialize_with = "nullable")]
    pub payload: Option<Option<String>>,
    #[serde(default)]
    pub payload_base64: Option<String>,
    #[serde(default)]
    pub payload_truncated: bool,
}

/// The payload field of the detail view.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(untagged)]
pub enum PayloadView {
    /// The payload as UTF-8 text.
    Text(String),
    /// Why it is not shown.
    Hidden {
        hidden: &'static str,
        reason: &'static str,
    },
}

/// `GET /api/v1/events/{source}/{segment}/{position}`: the summary, every header, and the payload
/// or why it is hidden.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Detail {
    #[serde(flatten)]
    pub summary: Summary,
    pub all_headers: Vec<HeaderView>,
    #[serde(skip_serializing_if = "is_zero")]
    pub headers_omitted: u64,
    /// The payload text, `null` for a record without one (a tombstone), or the reason it is
    /// hidden. Absent when the payload is served but is not UTF-8 (see `payload_base64`).
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "nullable_view"
    )]
    pub payload: Option<Option<PayloadView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub payload_truncated: bool,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

fn nullable_view<S: serde::Serializer>(
    value: &Option<Option<PayloadView>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(Some(view)) => view.serialize(s),
        _ => s.serialize_none(),
    }
}

impl Detail {
    /// The detail of the browser's `answer`. With `access` `Ok` the hub asked for the payload and
    /// shows what came (a record without `payload` or `payloadBase64` is a tombstone); otherwise
    /// it says why the payload is hidden and drops whatever payload the browser sent anyway.
    pub fn of(answer: RecordAnswer, access: Result<&str, Hidden>) -> Self {
        let mut detail = Self {
            summary: answer.summary.trimmed(),
            all_headers: answer.all_headers,
            headers_omitted: answer.headers_omitted,
            payload: None,
            payload_base64: None,
            payload_truncated: false,
        };
        match access {
            Err(why) => {
                detail.payload = Some(Some(PayloadView::Hidden {
                    hidden: why.code(),
                    reason: why.reason(),
                }));
            }
            Ok(_) => {
                detail.payload_truncated = answer.payload_truncated;
                match (answer.payload_base64, answer.payload) {
                    (Some(base64), _) => detail.payload_base64 = Some(base64),
                    (None, Some(Some(text))) => {
                        detail.payload = Some(Some(PayloadView::Text(text)));
                    }
                    (None, _) => {
                        detail.payload = Some(None);
                        detail.payload_truncated = false;
                    }
                }
            }
        }
        detail
    }

    /// True when the detail carries a payload (text or base64), not a tombstone or a reason.
    pub fn serves_payload(&self) -> bool {
        self.payload_base64.is_some() || matches!(self.payload, Some(Some(PayloadView::Text(_))))
    }
}

/// `GET /api/v1/events/sources`: every source the browser reads, the default first.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct SourcesView {
    pub sources: Vec<SourceView>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SourceView {
    pub source: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub missing: bool,
    #[serde(default)]
    pub segments: Vec<SegmentBounds>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SegmentBounds {
    pub segment: String,
    pub first: String,
    pub head: String,
}

/// `GET /api/v1/events/lag`: every subscription with a committed position on the source, live
/// or not, by lag.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LagView {
    pub source: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub missing: bool,
    pub subscriptions: Vec<SubscriptionLag>,
    #[serde(default)]
    pub truncated: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionLag {
    pub subscription: String,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub members: u64,
    #[serde(default)]
    pub segments: Vec<SegmentLag>,
    #[serde(default)]
    pub lag: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SegmentLag {
    pub segment: String,
    #[serde(default)]
    pub committed: Option<String>,
    pub head: String,
    #[serde(default)]
    pub lag: Option<u64>,
}

/// The first part of a tail.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TailStart {
    pub source: String,
    #[serde(default)]
    pub segments: u64,
    pub ends_at_ms: i64,
}

/// Why a tail ended, as the browser says it; any other reason reads as `failed`.
pub const TAIL_END_REASONS: [&str; 4] = ["maxDuration", "slowReader", "shutdown", "failed"];

#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct TailEnd {
    pub reason: String,
}

/// A heartbeat part: the hub sends its own clock, so nothing of it is read.
#[derive(Deserialize, Debug, Clone, PartialEq)]
pub struct Beat {}

/// One part of `kraken.events.Tail`.
#[derive(Deserialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum TailEvent {
    Start(TailStart),
    Record(Summary),
    Heartbeat(Beat),
    End(TailEnd),
}

/// A point query's answer: at most [`MAX_ANSWER`] bytes of JSON of the expected shape, else
/// [`Refusal::Failed`].
pub fn read<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, Refusal> {
    read_bounded(bytes, MAX_ANSWER)
}

fn read_bounded<T: DeserializeOwned>(bytes: &[u8], max: usize) -> Result<T, Refusal> {
    if bytes.len() > max {
        tracing::warn!(
            size = bytes.len(),
            max,
            "event browser answer too large: dropped"
        );
        return Err(Refusal::Failed);
    }
    serde_json::from_slice(bytes).map_err(|e| {
        tracing::warn!(
            error = %crate::auth::clip(&e.to_string()),
            "event browser answer malformed: dropped"
        );
        Refusal::Failed
    })
}

/// [`read`] for a search page, its summaries trimmed to the library headers.
pub fn read_page(bytes: &[u8]) -> Result<SearchPage, Refusal> {
    read::<SearchPage>(bytes).map(SearchPage::trimmed)
}

/// One tail part: at most [`MAX_PART`] bytes; a record's summary trimmed to the library headers.
pub fn read_part(bytes: &[u8]) -> Result<TailEvent, Refusal> {
    read_bounded::<TailEvent>(bytes, MAX_PART).map(|event| match event {
        TailEvent::Record(summary) => TailEvent::Record(summary.trimmed()),
        TailEvent::End(end) if !TAIL_END_REASONS.contains(&end.reason.as_str()) => {
            TailEvent::End(TailEnd {
                reason: "failed".to_string(),
            })
        }
        other => other,
    })
}

// ---------------------------------------------------------------------------------------------
// Errors

/// Why a call has no data: the hub's own refusal or the browser's, mapped to an HTTP status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// No browser is connected (404 [`NOT_INSTALLED`]).
    NotInstalled,
    /// 400: a parameter the hub or the browser cannot honour.
    BadRequest(String),
    /// 403: `q` without the payload rules.
    Forbidden(Hidden),
    /// 404: a record, segment or source that does not exist.
    NotFound(String),
    /// 429 with `Retry-After: 1`: too many searches or tails at once.
    Busy(String),
    /// 503: the event store did not answer the browser.
    Unavailable(String),
    /// 504: the browser did not answer in time.
    Timeout,
    /// 502: anything else (the browser left, broke the contract, answered garbage).
    Failed,
}

/// The browser's error, as it puts it in `Error.details` (task E1-R).
#[derive(Deserialize)]
struct BrowseError {
    error: String,
    message: String,
}

/// The longest prefix of `s` of at most `max` bytes that ends at a character boundary.
fn cut(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

impl Refusal {
    /// What the router or the browser answered. Only the browser's `details` of a known kind
    /// pass its message on; anything else gets a short message of the hub's.
    pub fn from_route(e: RouteError) -> Self {
        match e.code {
            ErrorCode::NoHandler => return Refusal::NotInstalled,
            ErrorCode::InvalidRequest => return Refusal::BadRequest(UNREADABLE.to_string()),
            ErrorCode::Timeout => return Refusal::Timeout,
            ErrorCode::ResourceExhausted => {
                return Refusal::Busy(
                    "the event browser has too many requests waiting; try again shortly"
                        .to_string(),
                );
            }
            _ => {}
        }
        let Ok(browse) = serde_json::from_slice::<BrowseError>(&e.details) else {
            return Refusal::Failed;
        };
        let message = cut(&browse.message, MAX_MESSAGE).to_string();
        match browse.error.as_str() {
            "invalid" => Refusal::BadRequest(message),
            "not-found" => Refusal::NotFound(message),
            "busy" => Refusal::Busy(message),
            "unavailable" => Refusal::Unavailable(message),
            _ => Refusal::Failed,
        }
    }

    /// The `outcome` of `kraken_hub_event_browser_requests_total`.
    pub fn outcome(&self) -> &'static str {
        match self {
            Refusal::NotInstalled => "not_installed",
            Refusal::BadRequest(_) => "bad_request",
            Refusal::Forbidden(_) => "forbidden",
            Refusal::NotFound(_) => "not_found",
            Refusal::Busy(_) => "too_many",
            Refusal::Unavailable(_) => "unavailable",
            Refusal::Timeout => "timeout",
            Refusal::Failed => "failed",
        }
    }
}

/// The message of a [`Refusal::Failed`]: never the browser's own words.
pub const FAILED: &str = "the event browser failed to answer";
/// The message of a 400 for a request the browser could not decode (`INVALID_REQUEST`).
pub const UNREADABLE: &str = "the event browser could not read the request";
/// The message of a [`Refusal::Timeout`].
pub const TIMED_OUT: &str = "the event browser did not answer in time";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Secret;

    /// The parameters of a query string with `pairs`.
    fn params(pairs: &[(&str, &str)]) -> Params {
        let map: Map<String, Value> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), Value::from(*v)))
            .collect();
        serde_json::from_value(Value::Object(map)).unwrap()
    }

    #[test]
    fn a_search_request_carries_the_filters_and_drops_empty_values() {
        let p = params(&[
            ("source", "kraken.events.dlt"),
            ("from", "2026-11-05T10:00:00Z"),
            ("to", "1730000000000"),
            ("type", "orders.*"),
            ("key", "order-42"),
            ("service", "service-b"),
            ("correlationId", "c-1"),
            ("userId", "u-1"),
            ("eventId", ""),
            ("q", "\"total\":42"),
            ("limit", "20"),
            ("cursor", "eyJ"),
        ]);
        assert!(p.wants_payload());
        let request = p.search_request(true, Duration::from_secs(8)).unwrap();
        assert_eq!(
            request,
            json!({
                "source": "kraken.events.dlt",
                "filters": {
                    "from": "2026-11-05T10:00:00Z", "to": "1730000000000", "type": "orders.*",
                    "key": "order-42", "source": "service-b", "correlationId": "c-1",
                    "userId": "u-1", "q": "\"total\":42",
                },
                "limit": 20,
                "cursor": "eyJ",
                "timeoutMs": 8000,
            })
        );
        // `q` only under the payload rules.
        let without = p.search_request(false, Duration::from_secs(8)).unwrap();
        assert!(without["filters"].get("q").is_none(), "{without}");
        // Nothing set: the browser's defaults.
        assert_eq!(
            Params::default()
                .search_request(false, Duration::from_millis(1500))
                .unwrap(),
            json!({"filters": {}, "timeoutMs": 1500})
        );
        for limit in ["ten", "-1", "1.5", "0", " 0 "] {
            let bad = params(&[("limit", limit)]).search_request(false, Duration::from_secs(1));
            assert_eq!(
                bad.unwrap_err(),
                "limit must be a positive number",
                "{limit}"
            );
        }
        // A cursor holds a place per segment: up to the browser's 64 KiB, passed verbatim.
        let cursor = "c".repeat(4096);
        let request = params(&[("cursor", &cursor)])
            .search_request(false, Duration::from_secs(1))
            .unwrap();
        assert_eq!(request["cursor"], cursor.as_str());
        assert!(
            params(&[("cursor", &"c".repeat(MAX_CURSOR))])
                .search_request(false, Duration::from_secs(1))
                .is_ok()
        );
        assert_eq!(
            params(&[("cursor", &"c".repeat(MAX_CURSOR + 1))])
                .search_request(false, Duration::from_secs(1))
                .unwrap_err(),
            format!("cursor is longer than {MAX_CURSOR} bytes")
        );
        // The browser's bounds, checked before asking: a filter value of at most 1 KiB.
        let long = "x".repeat(MAX_FILTER + 1);
        for name in ["from", "type", "service", "q", "source"] {
            let err = params(&[(name, &long)])
                .search_request(true, Duration::from_secs(1))
                .unwrap_err();
            assert_eq!(err, format!("{name} is longer than {MAX_FILTER} bytes"));
        }
        let longest = "x".repeat(MAX_FILTER);
        assert!(
            params(&[("key", &longest)])
                .search_request(false, Duration::from_secs(1))
                .is_ok()
        );
    }

    #[test]
    fn a_record_request_bounds_its_place() {
        assert_eq!(
            get_request("kraken.events", "3", "42", true).unwrap(),
            json!({"source": "kraken.events", "segment": "3", "position": "42", "payload": true})
        );
        let long = "1".repeat(MAX_PLACE + 1);
        assert_eq!(
            get_request("kraken.events", &long, "1", false).unwrap_err(),
            format!("segment is longer than {MAX_PLACE} bytes")
        );
        assert_eq!(
            get_request("kraken.events", "1", &long, false).unwrap_err(),
            format!("position is longer than {MAX_PLACE} bytes")
        );
        assert!(get_request(&"s".repeat(MAX_FILTER + 1), "1", "1", false).is_err());
    }

    #[test]
    fn a_lag_request_bounds_its_source() {
        assert_eq!(lag_request(None).unwrap(), json!({}));
        assert_eq!(lag_request(Some("")).unwrap(), json!({}));
        assert_eq!(
            lag_request(Some("kraken.events")).unwrap(),
            json!({"source": "kraken.events"})
        );
        assert!(lag_request(Some(&"s".repeat(MAX_FILTER))).is_ok());
        assert_eq!(
            lag_request(Some(&"s".repeat(MAX_FILTER + 1))).unwrap_err(),
            format!("source is longer than {MAX_FILTER} bytes")
        );
    }

    /// serde's error text quotes what it could not read; the warning about a malformed answer
    /// carries it cut, never a browser-chosen string of any length.
    #[test]
    fn a_malformed_answer_is_logged_cut() {
        crate::telemetry::test_support::capture_logs();
        let marker = "m".repeat(10);
        let answer = format!("{{\"source\":\"{marker}{}\"}}", "y".repeat(10_000));
        assert_eq!(read::<SearchPage>(answer.as_bytes()), Err(Refusal::Failed));
        let answer = format!(
            "{{\"source\":\"s\",\"results\":[],\"scanned\":\"{marker}{}\"}}",
            "y".repeat(10_000)
        );
        assert_eq!(read::<SearchPage>(answer.as_bytes()), Err(Refusal::Failed));
        let lines: Vec<_> = crate::telemetry::test_support::logged()
            .into_iter()
            .filter(|l| l.message.contains("malformed"))
            .collect();
        assert_eq!(
            lines.len(),
            2,
            "the malformed answers were not logged: {lines:?}"
        );
        for line in lines {
            let error = line.field("error").unwrap_or_default();
            assert!(
                error.chars().count() <= crate::auth::MAX_LOGGED_CHARS,
                "{error}"
            );
        }
    }

    #[test]
    fn a_tail_request_refuses_a_range_and_asks_a_heartbeat() {
        for name in ["from", "to", "cursor"] {
            let err = params(&[(name, "1")])
                .tail_request(false, Duration::from_secs(1), Duration::from_secs(60))
                .unwrap_err();
            assert_eq!(err, format!("a tail takes no {name}: it starts at the end"));
        }
        let request = params(&[("type", "orders.*"), ("q", "x"), ("limit", "5")])
            .tail_request(false, Duration::from_millis(2500), Duration::from_secs(598))
            .unwrap();
        assert_eq!(
            request,
            json!({"filters": {"type": "orders.*"}, "heartbeatMs": 2500, "maxDurationMs": 598000})
        );
    }

    #[test]
    fn the_hub_asks_budgets_within_its_own_bounds() {
        assert_eq!(
            search_budget(Duration::from_secs(30)),
            Duration::from_secs(28)
        );
        assert_eq!(
            search_budget(Duration::from_secs(1)),
            Duration::from_millis(500)
        );
        let config = Config::default();
        // The internal stream idles out after the default timeout (5 s): 2 s, well below it.
        assert_eq!(tail_heartbeat(&config), Duration::from_secs(2));
        let slow = Config {
            default_timeout: Duration::from_secs(60),
            ..Config::default()
        };
        assert_eq!(tail_heartbeat(&slow), MAX_HEARTBEAT);
        let overridden = Config {
            timeout_overrides: [(TAIL.to_string(), Duration::from_millis(100))].into(),
            ..Config::default()
        };
        assert_eq!(tail_heartbeat(&overridden), MIN_HEARTBEAT);
        assert_eq!(tail_duration(&config), Duration::from_secs(598));
    }

    #[test]
    fn a_correlation_id_is_taken_when_usable_else_made() {
        assert_eq!(correlation_id(Some("c-7f1e")), "c-7f1e");
        for bad in [None, Some(""), Some("has space"), Some("é")] {
            let made = correlation_id(bad);
            assert!(uuid::Uuid::parse_str(&made).is_ok(), "{bad:?}: {made}");
        }
        let long = "x".repeat(MAX_CORRELATION_ID + 1);
        assert_ne!(correlation_id(Some(&long)), long);
    }

    #[test]
    fn a_page_carries_a_long_cursor_back_intact() {
        let cursor = "c".repeat(MAX_CURSOR);
        let answer = format!(
            "{{\"source\":\"kraken.events\",\"results\":[],\"scanned\":1,\"exhausted\":false,\"stoppedBy\":\"limit\",\"cursor\":\"{cursor}\"}}"
        );
        let page = read_page(answer.as_bytes()).unwrap();
        assert_eq!(page.cursor.as_deref(), Some(cursor.as_str()));
    }

    #[test]
    fn answers_keep_only_what_the_hub_knows() {
        let page = read_page(
            br#"{"source":"kraken.events","results":[{"source":"kraken.events","segment":"3",
                "position":"42","timestamp":17,"key":"k","headers":{"type":"a.B","x-other":"y"},
                "size":9,"payload":"secret","payloadBase64":"c2VjcmV0"}],"scanned":5,
                "exhausted":true,"stoppedBy":null,"cursor":null,"extra":1}"#,
        )
        .unwrap();
        let out = serde_json::to_value(&page).unwrap();
        assert_eq!(
            out,
            json!({"source": "kraken.events", "results": [{"source": "kraken.events",
                "segment": "3", "position": "42", "timestamp": 17, "key": "k",
                "headers": {"type": "a.B"}, "size": 9}], "scanned": 5, "exhausted": true,
                "stoppedBy": null, "cursor": null})
        );
        // Garbage, the wrong shape and an oversized answer are failures, never a panic.
        for bad in [&b"not json"[..], b"[]", b"{\"source\":1}", b""] {
            assert_eq!(read_page(bad), Err(Refusal::Failed), "{bad:?}");
        }
        let huge = format!(
            "{{\"sources\":[{{\"source\":\"{}\"}}]}}",
            "x".repeat(MAX_ANSWER)
        );
        assert_eq!(read::<SourcesView>(huge.as_bytes()), Err(Refusal::Failed));
    }

    fn record(extra: &str) -> RecordAnswer {
        let text = format!(
            r#"{{"source":"kraken.events","segment":"0","position":"7","timestamp":1,"key":null,
                "headers":{{"type":"a.B"}},"size":6,"allHeaders":[{{"name":"type","value":"a.B"}},
                {{"name":"bin","value":null,"valueHex":"00ff","truncated":true}}]{extra}}}"#
        );
        serde_json::from_str(&text).unwrap()
    }

    #[test]
    fn a_detail_serves_the_payload_only_when_asked() {
        // Asked: text, base64, a tombstone (null or nothing at all), and the cut marker.
        let text = Detail::of(record(r#","payload":"{\"a\":1}""#), Ok("dashboard-token"));
        assert_eq!(
            text.payload,
            Some(Some(PayloadView::Text("{\"a\":1}".to_string())))
        );
        assert!(text.serves_payload());
        let raw = Detail::of(
            record(r#","payloadBase64":"/wA=","payloadTruncated":true"#),
            Ok("write-token"),
        );
        let out = serde_json::to_value(&raw).unwrap();
        assert_eq!(out["payloadBase64"], "/wA=");
        assert_eq!(out["payloadTruncated"], true);
        assert!(out.get("payload").is_none(), "{out}");
        for tombstone in [r#","payload":null"#, ""] {
            let d = Detail::of(record(tombstone), Ok("dashboard-token"));
            let out = serde_json::to_value(&d).unwrap();
            assert_eq!(out["payload"], Value::Null, "{tombstone}");
            assert!(out.as_object().unwrap().contains_key("payload"));
            assert!(!d.serves_payload());
        }
        // Not asked: the reason, whatever the browser sent.
        let hidden = Detail::of(
            record(r#","payload":"secret","payloadBase64":"c2VjcmV0","payloadTruncated":true"#),
            Err(Hidden::NotPresented),
        );
        let out = serde_json::to_value(&hidden).unwrap();
        assert_eq!(
            out["payload"],
            json!({"hidden": "no-token-presented", "reason": Hidden::NotPresented.reason()})
        );
        assert!(!out.to_string().contains("secret") && !out.to_string().contains("c2VjcmV0"));
        assert!(out.get("payloadBase64").is_none() && out.get("payloadTruncated").is_none());
        assert_eq!(out["allHeaders"][1]["valueHex"], "00ff");
    }

    #[test]
    fn tail_parts_are_read_and_bounded() {
        assert_eq!(
            read_part(br#"{"start":{"source":"s","segments":6,"endsAtMs":9}}"#),
            Ok(TailEvent::Start(TailStart {
                source: "s".into(),
                segments: 6,
                ends_at_ms: 9
            }))
        );
        assert!(matches!(
            read_part(br#"{"heartbeat":{"nowMs":1}}"#),
            Ok(TailEvent::Heartbeat(_))
        ));
        let Ok(TailEvent::Record(summary)) = read_part(
            br#"{"record":{"source":"s","segment":"0","position":"1","headers":{"id":"e","p":"x"}}}"#,
        ) else {
            panic!("a record");
        };
        assert_eq!(summary.headers.keys().collect::<Vec<_>>(), ["id"]);
        assert_eq!(
            read_part(br#"{"end":{"reason":"maxDuration"}}"#),
            Ok(TailEvent::End(TailEnd {
                reason: "maxDuration".into()
            }))
        );
        assert_eq!(
            read_part(br#"{"end":{"reason":"<script>"}}"#),
            Ok(TailEvent::End(TailEnd {
                reason: "failed".into()
            }))
        );
        assert_eq!(read_part(b"{\"nope\":1}"), Err(Refusal::Failed));
        let big = format!(
            r#"{{"record":{{"source":"{}","segment":"0","position":"1"}}}}"#,
            "s".repeat(MAX_PART)
        );
        assert_eq!(read_part(big.as_bytes()), Err(Refusal::Failed));
    }

    fn route(code: ErrorCode, details: &str) -> RouteError {
        RouteError {
            code,
            message: "raw handler message".to_string(),
            details: details.as_bytes().to_vec(),
        }
    }

    #[test]
    fn browser_errors_map_to_refusals_and_nothing_else_leaks() {
        let h = ErrorCode::HandlerError;
        for (details, want) in [
            (
                r#"{"error":"invalid","message":"bad cursor"}"#,
                Refusal::BadRequest("bad cursor".into()),
            ),
            (
                r#"{"error":"not-found","message":"no such position"}"#,
                Refusal::NotFound("no such position".into()),
            ),
            (
                r#"{"error":"busy","message":"4 searches"}"#,
                Refusal::Busy("4 searches".into()),
            ),
            (
                r#"{"error":"unavailable","message":"store down"}"#,
                Refusal::Unavailable("store down".into()),
            ),
            (r#"{"error":"other","message":"x"}"#, Refusal::Failed),
            (r#"{"oops":true}"#, Refusal::Failed),
            ("not json", Refusal::Failed),
            ("", Refusal::Failed),
        ] {
            assert_eq!(Refusal::from_route(route(h, details)), want, "{details}");
        }
        assert_eq!(
            Refusal::from_route(route(ErrorCode::NoHandler, "")),
            Refusal::NotInstalled
        );
        assert_eq!(
            Refusal::from_route(route(ErrorCode::Timeout, "")),
            Refusal::Timeout
        );
        assert!(matches!(
            Refusal::from_route(route(ErrorCode::ResourceExhausted, "")),
            Refusal::Busy(_)
        ));
        // A request the browser could not decode: the hub's own fault, said neutrally.
        assert_eq!(
            Refusal::from_route(route(ErrorCode::InvalidRequest, "")),
            Refusal::BadRequest(UNREADABLE.to_string())
        );
        for code in [ErrorCode::HandlerDisconnected, ErrorCode::Cancelled] {
            assert_eq!(Refusal::from_route(route(code, "")), Refusal::Failed);
        }
        // A long message is cut at a character boundary.
        let long = format!(
            r#"{{"error":"invalid","message":"{}"}}"#,
            "é".repeat(MAX_MESSAGE)
        );
        let Refusal::BadRequest(m) = Refusal::from_route(route(h, &long)) else {
            panic!("invalid");
        };
        assert!(m.len() <= MAX_MESSAGE && m.chars().all(|c| c == 'é'));
    }

    #[test]
    fn payloads_need_the_switch_a_configured_token_and_a_presented_one() {
        let config = |payloads, read: Option<&str>, write: Option<&str>| Config {
            event_payloads: payloads,
            dashboard_token: read.map(Secret::new),
            dashboard_write_token: write.map(Secret::new),
            ..Config::default()
        };
        let (read, write) = ("dashboard-token-0123456789", "write-token-0123456789");
        assert_eq!(
            payload_access(&config(false, Some(read), None), Some(read)),
            Err(Hidden::Off)
        );
        assert_eq!(
            payload_access(&config(true, None, None), Some(read)),
            Err(Hidden::NoToken)
        );
        assert_eq!(
            payload_access(&config(true, None, Some(write)), None),
            Err(Hidden::NotPresented)
        );
        assert_eq!(
            payload_access(&config(true, Some(read), Some(write)), Some("other")),
            Err(Hidden::NotPresented)
        );
        assert_eq!(
            payload_access(&config(true, Some(read), Some(write)), Some(read)),
            Ok("dashboard-token")
        );
        assert_eq!(
            payload_access(&config(true, Some(read), Some(write)), Some(write)),
            Ok("write-token")
        );
        assert_eq!(
            payload_access(&config(true, None, Some(write)), Some(write)),
            Ok("write-token")
        );
        // Who asked, whatever the switch.
        assert_eq!(
            caller(&config(false, Some(read), Some(write)), Some(write)),
            Some("write-token")
        );
        assert_eq!(caller(&config(false, Some(read), None), None), None);
    }
}
