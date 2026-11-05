//! The event browser's rules, without Kafka (ADR 0025): the filters of a search and how a record
//! matches them, the newest-first merge of the partitions with its cursor, what a record looks
//! like in a result (a summary, never a payload) and in the detail view, and when a payload may
//! be served. [`crate::browser`] reads Kafka and feeds records through these.

use std::collections::BTreeMap;
use std::time::Instant;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};

use crate::config::Config;

/// The record headers the libraries write (`kafka/headers.rs`, `EventHeaders.kt`), in the order a
/// summary lists them.
pub const LIBRARY_HEADERS: [&str; 7] = [
    "type",
    "id",
    "correlationId",
    "timestamp",
    "source",
    "userId",
    "traceparent",
];
/// A summary's key is cut to this many bytes (at a character boundary).
pub const MAX_KEY: usize = 128;
/// A summary's header values are cut to this many bytes.
pub const MAX_SUMMARY_HEADER: usize = 256;
/// The detail view lists at most this many headers...
pub const MAX_HEADERS: usize = 64;
/// ...names cut to this many bytes...
pub const MAX_HEADER_NAME: usize = 128;
/// ...and values to this many.
pub const MAX_HEADER_VALUE: usize = 1024;
/// A payload served in the detail view is cut to this many bytes.
pub const MAX_PAYLOAD: usize = 256 * 1024;
/// A filter value (and `q`) is at most this many bytes.
pub const MAX_FILTER: usize = 1024;
/// Results a search page returns when it names no `limit`.
pub const DEFAULT_LIMIT: usize = 50;
/// Records a partition is read backwards by, per Kafka read.
pub const CHUNK: usize = 500;
/// The longest cursor accepted, in characters.
pub const MAX_CURSOR: usize = 64 * 1024;

/// `bytes` as text: valid UTF-8 as it is, every byte that is not part of valid UTF-8 as `\xNN`.
pub fn escaped(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for chunk in bytes.utf8_chunks() {
        out.push_str(chunk.valid());
        for b in chunk.invalid() {
            out.push_str(&format!("\\x{b:02x}"));
        }
    }
    out
}

/// The longest prefix of `s` of at most `max` bytes that ends at a character boundary, and
/// whether anything was cut.
pub fn cut(s: &str, max: usize) -> (&str, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

/// Lowercase hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

// ---------------------------------------------------------------------------------------------
// Records

/// One Kafka record as the browser sees it, borrowed from the consumer.
#[derive(Debug, Clone, Copy)]
pub struct RecordView<'a> {
    pub partition: i32,
    pub offset: i64,
    /// The record timestamp in epoch ms; `None` when the record has none.
    pub timestamp: Option<i64>,
    pub key: Option<&'a [u8]>,
    /// Every header in record order: name and value (`None`: a null value).
    pub headers: &'a [(&'a [u8], Option<&'a [u8]>)],
    pub payload: Option<&'a [u8]>,
}

impl<'a> RecordView<'a> {
    /// The value of the last header named `name` (Kafka's `lastHeader`), when it has one.
    pub fn header(&self, name: &str) -> Option<&'a [u8]> {
        self.headers
            .iter()
            .rev()
            .find(|(n, _)| *n == name.as_bytes())
            .and_then(|(_, v)| *v)
    }
}

/// A record in a search result or the tail: where it is, its key, the library headers and the
/// payload's size. Never the payload.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    /// The key as text ([`escaped`]), cut to [`MAX_KEY`] bytes; `None` without a key.
    pub key: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub key_truncated: bool,
    /// The library headers the record carries ([`LIBRARY_HEADERS`]), as text cut to
    /// [`MAX_SUMMARY_HEADER`] bytes.
    pub headers: BTreeMap<&'static str, String>,
    /// Payload bytes (0 without a payload).
    pub size: usize,
}

impl Summary {
    pub fn of(topic: &str, r: &RecordView<'_>) -> Self {
        let (key, key_truncated) = match r.key {
            Some(k) => {
                let text = escaped(k);
                let (shown, cut_off) = cut(&text, MAX_KEY);
                (Some(shown.to_string()), cut_off)
            }
            None => (None, false),
        };
        let headers = LIBRARY_HEADERS
            .iter()
            .filter_map(|name| {
                let text = escaped(r.header(name)?);
                Some((*name, cut(&text, MAX_SUMMARY_HEADER).0.to_string()))
            })
            .collect();
        Self {
            topic: topic.to_string(),
            partition: r.partition,
            offset: r.offset,
            timestamp: r.timestamp,
            key,
            key_truncated,
            headers,
            size: r.payload.map_or(0, <[u8]>::len),
        }
    }
}

/// One header of the detail view.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HeaderView {
    /// The name as text ([`escaped`]), cut to [`MAX_HEADER_NAME`] bytes.
    pub name: String,
    /// The value when it is UTF-8 (cut to [`MAX_HEADER_VALUE`] bytes); `None` for a null value
    /// or one that is not UTF-8.
    pub value: Option<String>,
    /// The value in hex when it is not UTF-8 (its first [`MAX_HEADER_VALUE`] bytes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value_hex: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

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

/// Who may see payloads: `Ok` with the caller as the audit line names it (`dashboard-token` or
/// `write-token`) when the switch is on, a token is configured and `presented` is one of them.
pub fn payload_access(config: &Config, presented: Option<&str>) -> Result<&'static str, Hidden> {
    if !config.event_payloads {
        return Err(Hidden::Off);
    }
    let read = config.dashboard_token.as_ref();
    let write = config.dashboard_write_token.as_ref();
    if read.is_none() && write.is_none() {
        return Err(Hidden::NoToken);
    }
    if read.is_some_and(|t| t.matches(presented)) {
        return Ok("dashboard-token");
    }
    if write.is_some_and(|t| t.matches(presented)) {
        return Ok("write-token");
    }
    Err(Hidden::NotPresented)
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

/// The record detail: the summary, every header, and the payload when it may be served.
#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Detail {
    #[serde(flatten)]
    pub summary: Summary,
    /// Every header in record order, at most [`MAX_HEADERS`].
    pub all_headers: Vec<HeaderView>,
    /// Headers beyond [`MAX_HEADERS`], not listed.
    #[serde(skip_serializing_if = "is_zero")]
    pub headers_omitted: usize,
    /// The payload text, `null` for a record without a payload (a tombstone), or the reason it is
    /// hidden. Absent when the payload is served but is not UTF-8 (see `payload_base64`).
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "nullable")]
    pub payload: Option<Option<PayloadView>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub payload_truncated: bool,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

fn nullable<S: serde::Serializer>(
    value: &Option<Option<PayloadView>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    match value {
        Some(Some(view)) => view.serialize(s),
        _ => s.serialize_none(),
    }
}

impl Detail {
    /// The detail of `r`; the payload is included only with `access` `Ok` (the caller audits it).
    pub fn of(topic: &str, r: &RecordView<'_>, access: Result<(), Hidden>) -> Self {
        let all_headers = r
            .headers
            .iter()
            .take(MAX_HEADERS)
            .map(|(name, value)| {
                let name = escaped(name);
                let (name, name_cut) = cut(&name, MAX_HEADER_NAME);
                let (value, value_hex, value_cut) = match value {
                    None => (None, None, false),
                    Some(v) => match std::str::from_utf8(v) {
                        Ok(text) => {
                            let (shown, cut_off) = cut(text, MAX_HEADER_VALUE);
                            (Some(shown.to_string()), None, cut_off)
                        }
                        Err(_) => {
                            let shown = &v[..v.len().min(MAX_HEADER_VALUE)];
                            (None, Some(hex(shown)), v.len() > MAX_HEADER_VALUE)
                        }
                    },
                };
                HeaderView {
                    name: name.to_string(),
                    value,
                    value_hex,
                    truncated: name_cut || value_cut,
                }
            })
            .collect();
        let mut detail = Self {
            summary: Summary::of(topic, r),
            all_headers,
            headers_omitted: r.headers.len().saturating_sub(MAX_HEADERS),
            payload: None,
            payload_base64: None,
            payload_truncated: false,
        };
        match (access, r.payload) {
            (Err(why), _) => {
                detail.payload = Some(Some(PayloadView::Hidden {
                    hidden: why.code(),
                    reason: why.reason(),
                }));
            }
            (Ok(()), None) => detail.payload = Some(None),
            (Ok(()), Some(bytes)) => match std::str::from_utf8(bytes) {
                Ok(text) => {
                    let (shown, cut_off) = cut(text, MAX_PAYLOAD);
                    detail.payload = Some(Some(PayloadView::Text(shown.to_string())));
                    detail.payload_truncated = cut_off;
                }
                Err(_) => {
                    let shown = &bytes[..bytes.len().min(MAX_PAYLOAD)];
                    detail.payload_base64 = Some(STANDARD.encode(shown));
                    detail.payload_truncated = bytes.len() > MAX_PAYLOAD;
                }
            },
        }
        detail
    }
}

// ---------------------------------------------------------------------------------------------
// Filters

/// The query of a search and of a tail, as sent. Empty values count as absent (a form sends
/// every field).
#[derive(Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct SearchQuery {
    pub topic: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    #[serde(rename = "type")]
    pub type_name: Option<String>,
    pub key: Option<String>,
    pub source: Option<String>,
    pub correlation_id: Option<String>,
    pub user_id: Option<String>,
    pub event_id: Option<String>,
    pub q: Option<String>,
    pub limit: Option<String>,
    pub cursor: Option<String>,
}

/// The `type` filter: exact, or a prefix when it ends with `*`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeFilter {
    Exact(String),
    Prefix(String),
}

/// What a record must match; every set field must hold.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filters {
    /// The record timestamp is at least this (epoch ms)...
    pub from: Option<i64>,
    /// ...and at most this.
    pub to: Option<i64>,
    pub type_name: Option<TypeFilter>,
    /// The key as [`escaped`] shows it, untruncated: the bytes for a UTF-8 key.
    pub key: Option<String>,
    /// Header `source`, `correlationId`, `userId`, `id`: the value as [`escaped`] shows it.
    pub source: Option<String>,
    pub correlation_id: Option<String>,
    pub user_id: Option<String>,
    pub event_id: Option<String>,
    /// A substring of the payload bytes (case-sensitive).
    pub q: Option<String>,
}

fn present(v: &Option<String>) -> Option<&str> {
    v.as_deref().filter(|s| !s.is_empty())
}

impl Filters {
    /// The filters of `query`; `Err` with the reason for a 400. `tail` refuses `from`, `to` and
    /// `cursor`.
    pub fn parse(query: &SearchQuery, tail: bool) -> Result<Self, String> {
        if tail {
            for (name, v) in [
                ("from", &query.from),
                ("to", &query.to),
                ("cursor", &query.cursor),
            ] {
                if present(v).is_some() {
                    return Err(format!("a tail takes no {name}: it starts at the end"));
                }
            }
        }
        let text = |name: &str, v: &Option<String>| -> Result<Option<String>, String> {
            match present(v) {
                Some(s) if s.len() > MAX_FILTER => {
                    Err(format!("{name} is longer than {MAX_FILTER} bytes"))
                }
                other => Ok(other.map(str::to_string)),
            }
        };
        let time = |name: &str, v: &Option<String>| -> Result<Option<i64>, String> {
            present(v)
                .map(|s| parse_time(s).map_err(|e| format!("{name}: {e}")))
                .transpose()
        };
        let filters = Self {
            from: time("from", &query.from)?,
            to: time("to", &query.to)?,
            type_name: text("type", &query.type_name)?.map(|t| match t.strip_suffix('*') {
                Some(prefix) => TypeFilter::Prefix(prefix.to_string()),
                None => TypeFilter::Exact(t),
            }),
            key: text("key", &query.key)?,
            source: text("source", &query.source)?,
            correlation_id: text("correlationId", &query.correlation_id)?,
            user_id: text("userId", &query.user_id)?,
            event_id: text("eventId", &query.event_id)?,
            q: text("q", &query.q)?,
        };
        if let (Some(from), Some(to)) = (filters.from, filters.to)
            && from > to
        {
            return Err("from is after to".to_string());
        }
        Ok(filters)
    }

    /// True when `r` matches every filter.
    pub fn matches(&self, r: &RecordView<'_>) -> bool {
        let ts = r.timestamp.unwrap_or(-1);
        if self.from.is_some_and(|from| ts < from) || self.to.is_some_and(|to| ts > to) {
            return false;
        }
        let equal = |want: &Option<String>, got: Option<&[u8]>| match want {
            None => true,
            Some(want) => got.is_some_and(|g| {
                g == want.as_bytes() || (std::str::from_utf8(g).is_err() && escaped(g) == *want)
            }),
        };
        if !equal(&self.key, r.key)
            || !equal(&self.source, r.header("source"))
            || !equal(&self.correlation_id, r.header("correlationId"))
            || !equal(&self.user_id, r.header("userId"))
            || !equal(&self.event_id, r.header("id"))
        {
            return false;
        }
        if let Some(t) = &self.type_name {
            let Some(got) = r.header("type") else {
                return false;
            };
            let ok = match t {
                TypeFilter::Exact(want) => got == want.as_bytes(),
                TypeFilter::Prefix(prefix) => got.starts_with(prefix.as_bytes()),
            };
            if !ok {
                return false;
            }
        }
        match &self.q {
            None => true,
            // Never empty: an empty `q` is no filter.
            Some(q) => r
                .payload
                .is_some_and(|p| p.windows(q.len()).any(|w| w == q.as_bytes())),
        }
    }

    /// A digest of the topic and the filters, which a cursor carries: a cursor only continues the
    /// search it came from.
    pub fn digest(&self, topic: &str) -> u64 {
        let mut text = String::new();
        let mut put = |name: &str, v: Option<&str>| {
            text.push_str(name);
            match v {
                Some(v) => {
                    text.push('=');
                    text.push_str(v);
                }
                None => text.push('-'),
            }
            text.push('\u{0}');
        };
        put("topic", Some(topic));
        put("from", self.from.map(|v| v.to_string()).as_deref());
        put("to", self.to.map(|v| v.to_string()).as_deref());
        let type_name = self.type_name.as_ref().map(|t| match t {
            TypeFilter::Exact(t) => format!("exact:{t}"),
            TypeFilter::Prefix(p) => format!("prefix:{p}"),
        });
        put("type", type_name.as_deref());
        put("key", self.key.as_deref());
        put("source", self.source.as_deref());
        put("correlationId", self.correlation_id.as_deref());
        put("userId", self.user_id.as_deref());
        put("eventId", self.event_id.as_deref());
        put("q", self.q.as_deref());
        xxhash_rust::xxh64::xxh64(text.as_bytes(), 0)
    }
}

/// Epoch milliseconds (digits only) or an RFC 3339 date-time (`2026-11-05T10:00:00Z`,
/// `2026-11-05T10:00:00.250+01:00`).
pub fn parse_time(s: &str) -> Result<i64, String> {
    let s = s.trim();
    if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        return s.parse::<i64>().map_err(|_| format!("{s:?} is too large"));
    }
    rfc3339(s).ok_or_else(|| format!("{s:?} is neither epoch milliseconds nor an RFC 3339 time"))
}

fn rfc3339(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let digits = |from: usize, len: usize| -> Option<i64> {
        let part = b.get(from..from + len)?;
        part.iter()
            .all(u8::is_ascii_digit)
            .then(|| std::str::from_utf8(part).ok()?.parse().ok())?
    };
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !matches!(b[10], b'T' | b't' | b' ') {
        return None;
    }
    if b[13] != b':' || b[16] != b':' {
        return None;
    }
    let (year, month, day) = (digits(0, 4)?, digits(5, 2)?, digits(8, 2)?);
    let (hour, minute, second) = (digits(11, 2)?, digits(14, 2)?, digits(17, 2)?);
    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&month)
        || day < 1
        || day > month_days[(month - 1) as usize]
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let mut i = 19;
    let mut millis = 0;
    if b[i] == b'.' {
        let start = i + 1;
        i = start;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return None;
        }
        let frac = &s[start..i.min(start + 3)];
        millis = frac.parse::<i64>().ok()? * 10i64.pow(3 - frac.len() as u32);
    }
    let offset_minutes = match &b[i..] {
        [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), _, _, b':', _, _] => {
            let (h, m) = (digits(i + 1, 2)?, digits(i + 4, 2)?);
            if h > 23 || m > 59 {
                return None;
            }
            let minutes = h * 60 + m;
            if *sign == b'-' { -minutes } else { minutes }
        }
        _ => return None,
    };
    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3600 + minute * 60 + second.min(59) - offset_minutes * 60;
    Some(seconds * 1000 + millis)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (H. Hinnant's algorithm).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The `limit` of a search: [`DEFAULT_LIMIT`] when absent, else 1 to `max`.
pub fn parse_limit(limit: Option<&str>, max: usize) -> Result<usize, String> {
    match limit.filter(|s| !s.is_empty()) {
        None => Ok(DEFAULT_LIMIT.min(max)),
        Some(s) => match s.trim().parse::<usize>() {
            Ok(n) if (1..=max).contains(&n) => Ok(n),
            _ => Err(format!("limit must be a number from 1 to {max}")),
        },
    }
}

// ---------------------------------------------------------------------------------------------
// Cursor

/// Where a search goes on: for every partition, the offset below which the next page reads (the
/// lowest examined so far), and the digest of the search ([`Filters::digest`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(rename = "f")]
    pub digest: String,
    #[serde(rename = "p")]
    pub positions: Vec<(i32, i64)>,
}

impl Cursor {
    pub fn new(digest: u64, positions: Vec<(i32, i64)>) -> Self {
        Self {
            digest: format!("{digest:016x}"),
            positions,
        }
    }

    /// Opaque base64url (no padding) of its JSON.
    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).expect("a cursor serializes"))
    }

    /// The cursor `s` of the search whose digest is `digest`; `Err` with the reason for a 400.
    pub fn decode(s: &str, digest: u64) -> Result<Self, String> {
        const MALFORMED: &str = "the cursor is malformed";
        if s.len() > MAX_CURSOR {
            return Err(MALFORMED.to_string());
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(s.trim())
            .map_err(|_| MALFORMED.to_string())?;
        let cursor: Cursor = serde_json::from_slice(&bytes).map_err(|_| MALFORMED.to_string())?;
        if cursor
            .positions
            .iter()
            .any(|(partition, offset)| *partition < 0 || *offset < 0)
        {
            return Err(MALFORMED.to_string());
        }
        if cursor.digest != format!("{digest:016x}") {
            return Err(
                "the cursor belongs to another search: send the topic and filters it came with"
                    .to_string(),
            );
        }
        Ok(cursor)
    }
}

// ---------------------------------------------------------------------------------------------
// Newest-first merge

/// One partition's offsets to search: from `lower` (inclusive) up to `upper` (exclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub partition: i32,
    pub lower: i64,
    pub upper: i64,
}

/// A record a search read: its place, its timestamp (-1 without one) and its summary when it
/// matched the filters.
#[derive(Debug, Clone, PartialEq)]
pub struct Examined {
    pub offset: i64,
    pub timestamp: i64,
    pub hit: Option<Summary>,
}

/// Reads records for a search.
pub trait Chunks {
    type Error;
    /// The records of `partition` from `start` up to (not including) `end` in offset order, each
    /// examined against the filters; offsets with no record (compacted, transaction markers) are
    /// simply absent. `Ok(None)` when `deadline` passed before the chunk was complete.
    fn read(
        &mut self,
        partition: i32,
        start: i64,
        end: i64,
        deadline: Instant,
    ) -> Result<Option<Vec<Examined>>, Self::Error>;
}

/// Why a search page ended before every partition reached its lower bound.
#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum StoppedBy {
    Limit,
    ScanBudget,
    TimeBudget,
}

/// The result of [`merge`].
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    /// The matching records, newest first.
    pub results: Vec<Summary>,
    /// Records read from Kafka.
    pub scanned: u64,
    /// Every partition reached its lower bound: there is no next page.
    pub exhausted: bool,
    /// Why the page ended early; `None` when exhausted.
    pub stopped_by: Option<StoppedBy>,
    /// Per partition, the offset below which the next page reads.
    pub positions: Vec<(i32, i64)>,
}

/// The bounds of one [`merge`].
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub limit: usize,
    pub max_scanned: u64,
    pub deadline: Instant,
}

struct Lane {
    range: Range,
    /// Every offset at or above it was examined (it is the next page's upper bound).
    next: i64,
    /// The start of the chunk in `buffer`.
    loaded_from: i64,
    /// The chunk not yet examined, oldest first (popped from the end).
    buffer: Vec<Examined>,
}

impl Lane {
    fn done(&self) -> bool {
        self.buffer.is_empty() && self.next <= self.range.lower
    }

    /// Newest first; ties by partition, then offset, both descending.
    fn head(&self) -> Option<(i64, i32, i64)> {
        self.buffer
            .last()
            .map(|e| (e.timestamp, self.range.partition, e.offset))
    }
}

/// Reads `ranges` backwards in chunks of at most [`CHUNK`] records and merges them newest first
/// (record timestamp; ties by partition, then offset, descending) until `budget.limit` results,
/// `budget.max_scanned` records read or `budget.deadline`, whichever comes first.
pub fn merge<C: Chunks>(
    reader: &mut C,
    ranges: &[Range],
    budget: Budget,
) -> Result<Page, C::Error> {
    let mut lanes: Vec<Lane> = ranges
        .iter()
        .map(|r| Lane {
            range: *r,
            next: r.upper.max(r.lower),
            loaded_from: r.upper.max(r.lower),
            buffer: Vec::new(),
        })
        .collect();
    // Enough per chunk that one round over the partitions fits the scan budget.
    let chunk = (budget.max_scanned / (lanes.len().max(1) as u64)).clamp(1, CHUNK as u64) as i64;
    let mut results = Vec::new();
    let mut scanned = 0u64;
    let mut stopped_by = None;
    'pages: loop {
        // Every partition that is not done needs a record in hand before the newest is known.
        for lane in lanes
            .iter_mut()
            .filter(|l| l.buffer.is_empty() && !l.done())
        {
            if scanned >= budget.max_scanned {
                stopped_by = Some(StoppedBy::ScanBudget);
                break 'pages;
            }
            if Instant::now() >= budget.deadline {
                stopped_by = Some(StoppedBy::TimeBudget);
                break 'pages;
            }
            let left = (budget.max_scanned - scanned).min(chunk as u64) as i64;
            let start = (lane.next - left).max(lane.range.lower);
            let Some(read) =
                reader.read(lane.range.partition, start, lane.next, budget.deadline)?
            else {
                stopped_by = Some(StoppedBy::TimeBudget);
                break 'pages;
            };
            scanned += read.len() as u64;
            lane.loaded_from = start;
            lane.buffer = read
                .into_iter()
                .filter(|e| (start..lane.next).contains(&e.offset))
                .collect();
            if lane.buffer.is_empty() {
                // Nothing in the chunk: on to the next one down.
                lane.next = start;
            }
            // This page emits at most `wanted` more results, so no partition needs a summary
            // beyond its newest `wanted` hits: the older part of the chunk is let go (and read
            // again by the next page), which bounds a page's memory by partitions x limit.
            let wanted = budget.limit - results.len();
            if let Some(oldest_kept) = lane
                .buffer
                .iter()
                .enumerate()
                .rev()
                .filter(|(_, e)| e.hit.is_some())
                .nth(wanted.saturating_sub(1))
                .map(|(i, _)| i)
                && oldest_kept > 0
            {
                lane.loaded_from = lane.buffer[oldest_kept].offset;
                lane.buffer.drain(..oldest_kept);
            }
        }
        if lanes.iter().any(|l| l.buffer.is_empty() && !l.done()) {
            continue;
        }
        let Some(lane) = lanes
            .iter_mut()
            .filter(|l| !l.buffer.is_empty())
            .max_by_key(|l| l.head())
        else {
            break;
        };
        let examined = lane.buffer.pop().expect("a lane with a head");
        lane.next = if lane.buffer.is_empty() {
            lane.loaded_from
        } else {
            examined.offset
        };
        if let Some(hit) = examined.hit {
            results.push(hit);
            if results.len() >= budget.limit {
                stopped_by = Some(StoppedBy::Limit);
                break;
            }
        }
    }
    let exhausted = lanes.iter().all(Lane::done);
    Ok(Page {
        results,
        scanned,
        exhausted,
        stopped_by: if exhausted { None } else { stopped_by },
        positions: lanes
            .iter()
            .map(|l| (l.range.partition, l.next.max(l.range.lower)))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Secret;
    use std::time::Duration;

    fn view<'a>(
        partition: i32,
        offset: i64,
        key: Option<&'a [u8]>,
        headers: &'a [(&'a [u8], Option<&'a [u8]>)],
        payload: Option<&'a [u8]>,
    ) -> RecordView<'a> {
        RecordView {
            partition,
            offset,
            timestamp: Some(1_000),
            key,
            headers,
            payload,
        }
    }

    #[test]
    fn escaped_keeps_utf8_and_escapes_every_other_byte() {
        assert_eq!(escaped(b"order-1"), "order-1");
        assert_eq!(escaped("caf\u{e9}".as_bytes()), "caf\u{e9}");
        assert_eq!(escaped(b"a\xff\xfeb"), "a\\xff\\xfeb");
        assert_eq!(escaped(b"\xc3"), "\\xc3", "a truncated sequence");
    }

    #[test]
    fn cut_stops_at_a_character_boundary() {
        assert_eq!(cut("abc", 3), ("abc", false));
        assert_eq!(cut("abcd", 3), ("abc", true));
        // "é" is two bytes: a cut in its middle keeps it out.
        assert_eq!(cut("a\u{e9}", 2), ("a", true));
        assert_eq!(cut("", 0), ("", false));
    }

    #[test]
    fn times_are_epoch_millis_or_rfc3339() {
        assert_eq!(parse_time("1730000000000"), Ok(1_730_000_000_000));
        assert_eq!(parse_time("1970-01-01T00:00:00Z"), Ok(0));
        assert_eq!(parse_time("2026-11-05T10:00:00Z"), Ok(1_793_872_800_000));
        assert_eq!(
            parse_time("2026-11-05T11:00:00.25+01:00"),
            Ok(1_793_872_800_250)
        );
        assert_eq!(
            parse_time("2026-11-05t09:30:00.123456-00:30"),
            Ok(1_793_872_800_123)
        );
        assert_eq!(parse_time("2024-02-29T00:00:00Z"), Ok(1_709_164_800_000));
        for bad in [
            "",
            "yesterday",
            "2026-11-05",
            "2026-11-05T10:00:00",
            "2026-13-01T00:00:00Z",
            "2026-02-29T00:00:00Z",
            "2026-11-05T24:00:00Z",
            "2026-11-05T10:00:00.Z",
            "2026-11-05T10:00:00+1:00",
            "-5",
            "99999999999999999999",
        ] {
            assert!(parse_time(bad).is_err(), "{bad:?}");
        }
    }

    fn query(pairs: &[(&str, &str)]) -> SearchQuery {
        let encoded: String = pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}&"))
            .collect::<String>();
        serde_urlencoded_like(&encoded)
    }

    /// The query as axum's `Query` would read it (the names are what the API documents).
    fn serde_urlencoded_like(s: &str) -> SearchQuery {
        let value: serde_json::Map<String, serde_json::Value> = s
            .split('&')
            .filter(|p| !p.is_empty())
            .map(|p| {
                let (k, v) = p.split_once('=').unwrap();
                (k.to_string(), serde_json::Value::String(v.to_string()))
            })
            .collect();
        serde_json::from_value(serde_json::Value::Object(value)).unwrap()
    }

    #[test]
    fn filters_parse_types_as_exact_or_prefix_and_refuse_what_they_cannot_honour() {
        let f = Filters::parse(
            &query(&[
                ("type", "orders.*"),
                ("key", "k-1"),
                ("from", "1000"),
                ("to", "1970-01-01T00:00:02Z"),
                ("correlationId", ""),
            ]),
            false,
        )
        .unwrap();
        assert_eq!(f.type_name, Some(TypeFilter::Prefix("orders.".into())));
        assert_eq!((f.from, f.to), (Some(1000), Some(2000)));
        assert_eq!(f.key.as_deref(), Some("k-1"));
        assert_eq!(f.correlation_id, None, "an empty value is no filter");
        let exact = Filters::parse(&query(&[("type", "orders.Placed")]), false).unwrap();
        assert_eq!(
            exact.type_name,
            Some(TypeFilter::Exact("orders.Placed".into()))
        );

        let long = "x".repeat(MAX_FILTER + 1);
        for bad in [
            query(&[("from", "2000"), ("to", "1000")]),
            query(&[("from", "soon")]),
            query(&[("key", &long)]),
            query(&[("q", &long)]),
        ] {
            assert!(Filters::parse(&bad, false).is_err(), "{bad:?}");
        }
        for name in ["from", "to", "cursor"] {
            let err = Filters::parse(&query(&[(name, "1")]), true).unwrap_err();
            assert!(err.contains(name), "{err}");
        }
        assert!(Filters::parse(&query(&[("type", "a.*")]), true).is_ok());
    }

    #[test]
    fn the_limit_defaults_to_fifty_and_stays_within_the_maximum() {
        assert_eq!(parse_limit(None, 100), Ok(50));
        assert_eq!(parse_limit(Some(""), 100), Ok(50));
        assert_eq!(parse_limit(None, 20), Ok(20));
        assert_eq!(parse_limit(Some("100"), 100), Ok(100));
        for bad in ["0", "101", "ten", "-1"] {
            assert!(parse_limit(Some(bad), 100).is_err(), "{bad}");
        }
    }

    #[test]
    fn filters_match_the_key_and_the_library_headers_exactly_and_types_by_prefix() {
        let headers: &[(&[u8], Option<&[u8]>)] = &[
            (b"type", Some(b"orders.OrderPlaced")),
            (b"id", Some(b"e-1")),
            (b"correlationId", Some(b"c-old")),
            (b"correlationId", Some(b"c-1")),
            (b"source", Some(b"service-b")),
            (b"userId", Some(b"u\xff")),
            (b"traceparent", None),
        ];
        let r = view(0, 7, Some(b"k\xfe"), headers, Some(b"{\"total\":42}"));
        let matches =
            |pairs: &[(&str, &str)]| Filters::parse(&query(pairs), false).unwrap().matches(&r);
        assert!(matches(&[]));
        assert!(matches(&[("type", "orders.OrderPlaced")]));
        assert!(matches(&[("type", "orders.*")]));
        assert!(matches(&[("type", "*")]));
        assert!(
            !matches(&[("type", "orders")]),
            "exact unless it ends with *"
        );
        assert!(!matches(&[("type", "orders.OrderPlaced.*")]));
        assert!(matches(&[("eventId", "e-1")]));
        assert!(matches(&[("correlationId", "c-1")]), "the last header wins");
        assert!(!matches(&[("correlationId", "c-old")]));
        assert!(matches(&[("source", "service-b")]));
        assert!(!matches(&[("source", "service")]), "exact, not a substring");
        assert!(
            matches(&[("userId", "u\\xff")]),
            "a value that is not UTF-8 matches as shown"
        );
        assert!(matches(&[("key", "k\\xfe")]));
        assert!(!matches(&[("key", "k")]));
        assert!(matches(&[("q", "\"total\":42")]));
        assert!(!matches(&[("q", "TOTAL")]), "case-sensitive");
        assert!(matches(&[("from", "1000"), ("to", "1000")]));
        assert!(!matches(&[("from", "1001")]));
        assert!(!matches(&[("to", "999")]));

        // A header that is absent (or null) matches no value; a record without a key no key.
        let bare = view(0, 8, None, &[], None);
        let f = |pairs: &[(&str, &str)]| Filters::parse(&query(pairs), false).unwrap();
        assert!(!f(&[("key", "k")]).matches(&bare));
        assert!(!f(&[("type", "*")]).matches(&bare));
        assert!(!f(&[("q", "x")]).matches(&bare));
        let null_type: &[(&[u8], Option<&[u8]>)] = &[(b"type", None)];
        assert!(!f(&[("type", "*")]).matches(&view(0, 9, None, null_type, None)));
    }

    #[test]
    fn a_cursor_round_trips_and_only_continues_its_own_search() {
        let f = Filters::parse(&query(&[("type", "a.*")]), false).unwrap();
        let digest = f.digest("kraken.events");
        let cursor = Cursor::new(digest, vec![(0, 120), (1, 0), (5, 9_000_000_000)]);
        let text = cursor.encode();
        assert!(
            text.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "{text}"
        );
        assert_eq!(Cursor::decode(&text, digest), Ok(cursor));

        let other = Filters::parse(&query(&[("type", "a.Placed")]), false).unwrap();
        assert_ne!(other.digest("kraken.events"), digest);
        assert_ne!(f.digest("kraken.events.dlt"), digest, "the topic counts");
        let err = Cursor::decode(&text, other.digest("kraken.events")).unwrap_err();
        assert!(err.contains("another search"), "{err}");
        for bad in [
            "%%%".to_string(),
            URL_SAFE_NO_PAD.encode("{}"),
            URL_SAFE_NO_PAD.encode(format!("{{\"f\":\"{digest:016x}\",\"p\":[[-1,5]]}}")),
            URL_SAFE_NO_PAD.encode(format!("{{\"f\":\"{digest:016x}\",\"p\":[[0,-5]]}}")),
            "A".repeat(MAX_CURSOR + 1),
        ] {
            assert!(Cursor::decode(&bad, digest).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_summary_cuts_the_key_and_headers_and_never_carries_the_payload() {
        let long_type = "t".repeat(300);
        let key = vec![b'k'; 200];
        let headers: &[(&[u8], Option<&[u8]>)] = &[
            (b"type", Some(long_type.as_bytes())),
            (b"x-custom", Some(b"not listed")),
            (b"source", Some(b"svc")),
        ];
        let s = Summary::of(
            "kraken.events",
            &view(3, 42, Some(&key), headers, Some(b"secret")),
        );
        assert_eq!(s.key.as_deref().map(str::len), Some(MAX_KEY));
        assert!(s.key_truncated);
        assert_eq!(s.headers["type"].len(), MAX_SUMMARY_HEADER);
        assert_eq!(s.headers["source"], "svc");
        assert!(!s.headers.contains_key("x-custom"));
        assert_eq!(s.size, 6);
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("secret"), "{json}");
        assert!(json.contains("\"keyTruncated\":true"), "{json}");
        let short = Summary::of("t", &view(0, 0, Some(b"k"), &[], None));
        let json = serde_json::to_string(&short).unwrap();
        assert!(!json.contains("keyTruncated"), "{json}");
        assert_eq!(short.size, 0);
    }

    #[test]
    fn a_detail_bounds_every_header_and_serves_the_payload_only_when_allowed() {
        let long_name = "n".repeat(200);
        let long_value = "v".repeat(2000);
        let mut headers: Vec<(&[u8], Option<&[u8]>)> = vec![
            (b"type", Some(b"a.B")),
            (long_name.as_bytes(), Some(long_value.as_bytes())),
            (b"bin", Some(b"\x00\xff")),
            (b"\xffname", None),
        ];
        for _ in 0..70 {
            headers.push((b"filler", Some(b"x")));
        }
        let r = view(1, 2, None, &headers, Some(b"{\"a\":1}"));
        let d = Detail::of("kraken.events", &r, Ok(()));
        assert_eq!(d.all_headers.len(), MAX_HEADERS);
        assert_eq!(d.headers_omitted, headers.len() - MAX_HEADERS);
        let long = &d.all_headers[1];
        assert_eq!(
            (
                long.name.len(),
                long.value.as_ref().unwrap().len(),
                long.truncated
            ),
            (MAX_HEADER_NAME, MAX_HEADER_VALUE, true)
        );
        assert_eq!(
            (
                &d.all_headers[2].value,
                d.all_headers[2].value_hex.as_deref()
            ),
            (&None, Some("00ff"))
        );
        assert_eq!(d.all_headers[3].name, "\\xffname");
        assert_eq!(d.payload, Some(Some(PayloadView::Text("{\"a\":1}".into()))));
        let json = serde_json::to_value(&d).unwrap();
        assert_eq!(json["payload"], "{\"a\":1}");
        assert_eq!(json["headers"]["type"], "a.B");

        for why in [Hidden::Off, Hidden::NoToken, Hidden::NotPresented] {
            let d = Detail::of("kraken.events", &r, Err(why));
            let json = serde_json::to_value(&d).unwrap();
            assert_eq!(json["payload"]["hidden"], why.code());
            assert!(!json.to_string().contains("\\\"a\\\":1"), "{json}");
            assert!(json.get("payloadBase64").is_none());
        }

        let big = "p".repeat(MAX_PAYLOAD + 10);
        let d = Detail::of("t", &view(0, 0, None, &[], Some(big.as_bytes())), Ok(()));
        assert!(d.payload_truncated);
        assert_eq!(
            d.payload,
            Some(Some(PayloadView::Text("p".repeat(MAX_PAYLOAD))))
        );
        let d = Detail::of("t", &view(0, 0, None, &[], Some(b"\xff\x00")), Ok(()));
        let json = serde_json::to_value(&d).unwrap();
        assert!(json.get("payload").is_none(), "{json}");
        assert_eq!(json["payloadBase64"], "/wA=");
        let d = Detail::of("t", &view(0, 0, None, &[], None), Ok(()));
        let json = serde_json::to_value(&d).unwrap();
        assert!(
            json["payload"].is_null() && json.get("payload").is_some(),
            "{json}"
        );
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
    }

    // ---- merge ---------------------------------------------------------------------------

    /// Records of one partition: `(offset, timestamp, matches)`; offsets may have gaps.
    type Records = Vec<(i64, i64, bool)>;

    /// Partitions in memory.
    struct Fake {
        partitions: BTreeMap<i32, Records>,
        reads: usize,
        slow_after: Option<usize>,
    }

    impl Fake {
        fn new(partitions: &[(i32, Records)]) -> Self {
            Self {
                partitions: partitions.iter().cloned().collect(),
                reads: 0,
                slow_after: None,
            }
        }

        fn ranges(&self) -> Vec<Range> {
            self.partitions
                .iter()
                .map(|(p, recs)| Range {
                    partition: *p,
                    lower: recs.first().map_or(0, |r| r.0),
                    upper: recs.last().map_or(0, |r| r.0 + 1),
                })
                .collect()
        }
    }

    impl Chunks for Fake {
        type Error = ();
        fn read(
            &mut self,
            partition: i32,
            start: i64,
            end: i64,
            _deadline: Instant,
        ) -> Result<Option<Vec<Examined>>, ()> {
            self.reads += 1;
            if self.slow_after.is_some_and(|n| self.reads > n) {
                return Ok(None);
            }
            Ok(Some(
                self.partitions[&partition]
                    .iter()
                    .filter(|(o, ..)| (start..end).contains(o))
                    .map(|(offset, ts, hit)| Examined {
                        offset: *offset,
                        timestamp: *ts,
                        hit: hit.then(|| Summary {
                            topic: "t".into(),
                            partition,
                            offset: *offset,
                            timestamp: Some(*ts),
                            key: None,
                            key_truncated: false,
                            headers: BTreeMap::new(),
                            size: 0,
                        }),
                    })
                    .collect(),
            ))
        }
    }

    /// One page over every record of `fake`.
    fn run(fake: &mut Fake, budget: Budget) -> Page {
        let ranges = fake.ranges();
        merge(fake, &ranges, budget).unwrap()
    }

    fn budget(limit: usize, max_scanned: u64) -> Budget {
        Budget {
            limit,
            max_scanned,
            deadline: Instant::now() + Duration::from_secs(60),
        }
    }

    fn places(page: &Page) -> Vec<(i32, i64)> {
        page.results
            .iter()
            .map(|s| (s.partition, s.offset))
            .collect()
    }

    #[test]
    fn partitions_merge_newest_first_with_ties_by_partition_then_offset() {
        // Partition 0 is busy and old, 1 sparse and new, 2 has gaps and ties with 0.
        let mut fake = Fake::new(&[
            (0, (0..5).map(|o| (o, 100 + o * 10, true)).collect()),
            (1, vec![(0, 500, true), (1, 900, true)]),
            (2, vec![(3, 120, true), (8, 140, true), (9, 140, true)]),
        ]);
        let page = run(&mut fake, budget(100, 1000));
        assert_eq!(
            places(&page),
            vec![
                (1, 1),
                (1, 0),
                (2, 9),
                (2, 8),
                (0, 4),
                (0, 3),
                // Both at 120: the higher partition first.
                (2, 3),
                (0, 2),
                (0, 1),
                (0, 0)
            ]
        );
        let times: Vec<i64> = page.results.iter().map(|s| s.timestamp.unwrap()).collect();
        assert!(times.windows(2).all(|w| w[0] >= w[1]), "{times:?}");
        assert!(page.exhausted);
        assert_eq!(page.stopped_by, None);
        assert_eq!(page.scanned, 10);
    }

    #[test]
    fn pages_continue_from_the_cursor_without_skipping_or_repeating_a_record() {
        // Uneven partitions, gaps, non-matching records and timestamps that interleave.
        let mut data = Vec::new();
        for p in 0..4i32 {
            let records: Vec<(i64, i64, bool)> = (0..(40 + p as i64 * 37))
                .filter(|o| (o + p as i64) % 7 != 0)
                .map(|o| {
                    (
                        o,
                        (o * (p as i64 + 3) * 11) % 1500 + o * 5,
                        (o + p as i64) % 3 != 0,
                    )
                })
                .collect();
            data.push((p, records));
        }
        let mut whole = Fake::new(&data);
        let all = run(&mut whole, budget(10_000, 1_000_000));
        assert!(all.exhausted);
        let expected = places(&all);
        assert!(expected.len() > 100);

        for (limit, max_scanned) in [(7, 1_000_000), (100, 23), (3, 9), (50, 4)] {
            let mut fake = Fake::new(&data);
            let mut ranges = fake.ranges();
            let mut got = Vec::new();
            for _ in 0..10_000 {
                let page = merge(&mut fake, &ranges, budget(limit, max_scanned)).unwrap();
                assert!(page.results.len() <= limit);
                got.extend(places(&page));
                if page.exhausted {
                    assert_eq!(page.stopped_by, None);
                    break;
                }
                assert!(page.stopped_by.is_some());
                // The next page: the same lower bounds, the cursor's upper bounds.
                ranges = ranges
                    .iter()
                    .map(|r| Range {
                        upper: page
                            .positions
                            .iter()
                            .find(|(p, _)| *p == r.partition)
                            .unwrap()
                            .1,
                        ..*r
                    })
                    .collect();
            }
            assert_eq!(got, expected, "limit {limit}, scan budget {max_scanned}");
        }
    }

    /// A chunk full of matches keeps only the summaries the page can still return (here 3 of
    /// 400 per partition); the rest is read again by the next page, so nothing is lost.
    #[test]
    fn a_chunk_full_of_matches_is_trimmed_without_losing_a_record() {
        let mut fake = Fake::new(&[
            (0, (0..400).map(|o| (o, o * 2, true)).collect()),
            (1, (0..400).map(|o| (o, o * 2 + 1, true)).collect()),
        ]);
        let page = run(&mut fake, budget(3, 10_000));
        assert_eq!(places(&page), vec![(1, 399), (0, 399), (1, 398)]);
        assert_eq!(page.positions, vec![(0, 399), (1, 398)]);
        let next = merge(
            &mut fake,
            &[
                Range {
                    partition: 0,
                    lower: 0,
                    upper: 399,
                },
                Range {
                    partition: 1,
                    lower: 0,
                    upper: 398,
                },
            ],
            budget(3, 10_000),
        )
        .unwrap();
        assert_eq!(places(&next), vec![(0, 398), (1, 397), (0, 397)]);
    }

    #[test]
    fn the_scan_budget_stops_a_search_and_counts_what_was_read() {
        let mut fake = Fake::new(&[
            (0, (0..300).map(|o| (o, o, false)).collect()),
            (1, (0..300).map(|o| (o, o, false)).collect()),
        ]);
        let page = run(&mut fake, budget(10, 100));
        assert_eq!(page.stopped_by, Some(StoppedBy::ScanBudget));
        assert!(!page.exhausted);
        assert_eq!(page.scanned, 100);
        assert!(page.results.is_empty());
        // Progress: the positions moved down from 300.
        assert!(
            page.positions.iter().all(|(_, o)| *o < 300),
            "{:?}",
            page.positions
        );
    }

    #[test]
    fn the_time_budget_stops_a_search_with_what_it_found() {
        let mut fake = Fake::new(&[(0, (0..2000).map(|o| (o, o, true)).collect())]);
        fake.slow_after = Some(1);
        let page = run(&mut fake, budget(5000, 1_000_000));
        assert_eq!(page.stopped_by, Some(StoppedBy::TimeBudget));
        assert_eq!(page.results.len(), CHUNK);
        assert_eq!(page.positions, vec![(0, 2000 - CHUNK as i64)]);

        let mut fake = Fake::new(&[(0, vec![(0, 0, true)])]);
        let past = Budget {
            deadline: Instant::now(),
            ..budget(10, 10)
        };
        let page = run(&mut fake, past);
        assert_eq!(page.stopped_by, Some(StoppedBy::TimeBudget));
        assert_eq!(
            page.positions,
            vec![(0, 1)],
            "nothing read, nothing skipped"
        );
    }

    #[test]
    fn a_search_hitting_the_limit_on_its_last_record_is_exhausted() {
        let mut fake = Fake::new(&[(0, vec![(0, 0, true), (1, 1, true)])]);
        let page = run(&mut fake, budget(2, 100));
        assert!(page.exhausted);
        assert_eq!(page.stopped_by, None);
        let mut fake = Fake::new(&[(0, vec![(0, 0, true), (1, 1, true), (2, 2, true)])]);
        let page = run(&mut fake, budget(2, 100));
        assert!(!page.exhausted);
        assert_eq!(page.stopped_by, Some(StoppedBy::Limit));
        assert_eq!(page.positions, vec![(0, 1)]);
    }

    #[test]
    fn empty_ranges_are_exhausted_at_once() {
        let mut fake = Fake::new(&[(0, vec![]), (1, vec![])]);
        let page = run(&mut fake, budget(10, 10));
        assert!(page.exhausted);
        assert_eq!(page.scanned, 0);
        assert_eq!(fake.reads, 0);
        // A range that is empty because from/to resolved past the end.
        let page = merge(
            &mut fake,
            &[Range {
                partition: 0,
                lower: 50,
                upper: 10,
            }],
            budget(10, 10),
        )
        .unwrap();
        assert!(page.exhausted);
    }
}
