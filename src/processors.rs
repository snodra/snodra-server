//! Event processors (ADR 0022): what each instance reports about its event processors in
//! `InstanceStatus.processors`, bounded as the hub keeps it, and the per-service view the
//! dashboard and `GET /api/v1/processors` show. A report also says whether the processor replays
//! and which reset holds its lock (ADR 0023), and which event store it reads with which
//! capabilities (ADR 0027).
//!
//! The hub only relays and aggregates what the libraries report: it reads no event store, keeps
//! no history and stores nothing beyond the latest report of each live session (in the session's
//! [`crate::session::StatusDetail`], dropped with the session). Never payloads, keys or headers.
//! Segments and positions are opaque strings only the event-store add-on reads; the hub shows
//! them as text and sorts segments in [`segment_order`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::proto;
use crate::topology::Topology;

/// Processors kept from one report; further entries are dropped.
pub const MAX_PROCESSORS: usize = 32;
/// Segments kept per processor.
pub const MAX_SEGMENTS: usize = 1024;
/// Event types kept per processor.
pub const MAX_EVENT_TYPES: usize = 256;
/// Longest processor name kept, in bytes (longer names are cut at a UTF-8 boundary).
pub const MAX_NAME_BYTES: usize = 64;
/// Longest subscription and source kept, in bytes.
pub const MAX_SUBSCRIPTION_SOURCE_BYTES: usize = 256;
/// Longest event-store name kept, in bytes.
pub const MAX_EVENT_STORE_BYTES: usize = 64;
/// Capabilities kept per processor.
pub const MAX_CAPABILITIES: usize = 16;
/// Longest capability kept, in bytes.
pub const MAX_CAPABILITY_BYTES: usize = 64;
/// Longest segment name or position kept, in bytes. Never cut (a cut position is a wrong one):
/// a longer segment is dropped, a longer position reads as unknown.
pub const MAX_SEGMENT_BYTES: usize = 128;
/// Longest last error kept, in bytes.
pub const MAX_LAST_ERROR_BYTES: usize = 512;
/// Longest event type kept, in bytes.
pub const MAX_EVENT_TYPE_BYTES: usize = 256;
/// Longest dead-letter mode kept, in bytes (the defined modes are at most 11).
pub const MAX_MODE_BYTES: usize = 32;
/// Longest reset operation id kept, in bytes (the hub's are UUIDs, 36).
pub const MAX_OPERATION_ID_BYTES: usize = 64;

/// The capability that offers a reset (and so a replay) of a processor (ADR 0027).
pub const CAPABILITY_REPLAY: &str = "replay";
/// `ProcessorState` as the hub shows it (JSON: `"RUNNING"` and so on, without the
/// `PROCESSOR_` prefix). A number this hub does not know reads as `Unspecified`.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProcessorStateName {
    Starting,
    Running,
    Failing,
    Stopped,
    Paused,
    #[default]
    #[serde(other)]
    Unspecified,
}

impl ProcessorStateName {
    pub fn from_proto(value: i32) -> Self {
        match proto::ProcessorState::try_from(value) {
            Ok(proto::ProcessorState::ProcessorStarting) => Self::Starting,
            Ok(proto::ProcessorState::ProcessorRunning) => Self::Running,
            Ok(proto::ProcessorState::ProcessorFailing) => Self::Failing,
            Ok(proto::ProcessorState::ProcessorStopped) => Self::Stopped,
            Ok(proto::ProcessorState::ProcessorPaused) => Self::Paused,
            Ok(proto::ProcessorState::Unspecified) | Err(_) => Self::Unspecified,
        }
    }

    pub fn to_proto(self) -> proto::ProcessorState {
        match self {
            Self::Unspecified => proto::ProcessorState::Unspecified,
            Self::Starting => proto::ProcessorState::ProcessorStarting,
            Self::Running => proto::ProcessorState::ProcessorRunning,
            Self::Failing => proto::ProcessorState::ProcessorFailing,
            Self::Stopped => proto::ProcessorState::ProcessorStopped,
            Self::Paused => proto::ProcessorState::ProcessorPaused,
        }
    }

    /// The JSON name, also used by the CLI table.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Starting => "STARTING",
            Self::Running => "RUNNING",
            Self::Failing => "FAILING",
            Self::Stopped => "STOPPED",
            Self::Paused => "PAUSED",
        }
    }

    /// Every state, for a gauge that exports one series per state.
    pub const ALL: [Self; 6] = [
        Self::Starting,
        Self::Running,
        Self::Failing,
        Self::Stopped,
        Self::Paused,
        Self::Unspecified,
    ];

    /// The `state` label of the processor state gauge (ADR 0026): the name in lowercase.
    pub fn label(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Failing => "failing",
            Self::Stopped => "stopped",
            Self::Paused => "paused",
        }
    }

    /// Higher is worse: FAILING, unknown, STARTING, PAUSED, STOPPED, RUNNING.
    fn severity(self) -> u8 {
        match self {
            Self::Failing => 5,
            Self::Unspecified => 4,
            Self::Starting => 3,
            Self::Paused => 2,
            Self::Stopped => 1,
            Self::Running => 0,
        }
    }
}

/// The digits of a decimal segment name without its leading zeros (`""` for zero); `None` when
/// the name is not decimal (empty, or anything but ASCII digits: a sign included).
fn decimal_digits(name: &str) -> Option<&str> {
    (!name.is_empty() && name.bytes().all(|b| b.is_ascii_digit()))
        .then(|| name.trim_start_matches('0'))
}

/// The order of segment names: decimal names (only ASCII digits) by value, of any length
/// (`2` before `10`), before any other name; other names, and equal values (`007` and `7`), by
/// their bytes. `ui/processors.js` `segmentOrder` is the same rule.
pub fn segment_order(a: &str, b: &str) -> Ordering {
    match (decimal_digits(a), decimal_digits(b)) {
        (Some(x), Some(y)) => x
            .len()
            .cmp(&y.len())
            .then_with(|| x.cmp(y))
            .then_with(|| a.cmp(b)),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => a.cmp(b),
    }
}

/// One assigned segment as reported (`SegmentStatus`): positions are opaque strings, empty when
/// unknown or none; a lag of -1 is unknown.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct SegmentReport {
    pub segment: String,
    pub position: String,
    pub committed: String,
    pub head: String,
    pub lag: i64,
    /// The end (exclusive) of the segment's open replay range (ADR 0023); empty when none.
    pub replay_until: String,
}

/// One processor of one instance as the hub keeps it (`ProcessorStatus`, bounded).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ProcessorReport {
    pub name: String,
    /// The event store's name for the processor's subscription.
    pub subscription: String,
    /// The source the processor reads.
    pub source: String,
    /// The event store the processor reads (`kafka`, `rabbitmq-streams`, `memory`, ...).
    pub event_store: String,
    /// What the event store supports (`replay`, `browse`, `lag`, `native-dead-letter`, ...),
    /// sorted, distinct.
    pub capabilities: Vec<String>,
    pub state: ProcessorStateName,
    /// Sorted in [`segment_order`], distinct.
    pub segments: Vec<SegmentReport>,
    pub processed_total: u64,
    pub failed_total: u64,
    pub dead_letters: u64,
    /// Empty when none.
    pub last_error: String,
    /// Epoch ms; 0 when none.
    pub last_error_at_ms: i64,
    /// Sorted, distinct.
    pub event_types: Vec<String>,
    /// `collection`, `event-store` or `log` as reported.
    pub dead_letter_mode: String,
    /// A replay range is open on this instance's segments (ADR 0023).
    pub replaying: bool,
    /// The reset operation holding the processor's lock; empty when none.
    pub operation_id: String,
}

/// `s` cut to at most `max` bytes at a UTF-8 character boundary.
pub fn truncate_utf8(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

/// A lag as kept: anything below -1 reads as -1 (unknown).
fn lag(v: i64) -> i64 {
    v.max(-1)
}

/// A position (or a segment name where it is only shown) as kept: never cut, one longer than
/// [`MAX_SEGMENT_BYTES`] reads as unknown (empty).
pub fn kept_whole(p: &str) -> String {
    if p.len() > MAX_SEGMENT_BYTES {
        String::new()
    } else {
        p.to_string()
    }
}

/// At most `max` distinct non-empty strings of `list`, each cut to `bytes`, sorted.
fn distinct_bounded(list: &[String], max: usize, bytes: usize) -> Vec<String> {
    let mut kept = BTreeSet::new();
    for item in list {
        if kept.len() == max {
            break;
        }
        let item = truncate_utf8(item, bytes);
        if !item.is_empty() {
            kept.insert(item);
        }
    }
    kept.into_iter().collect()
}

impl ProcessorReport {
    /// Bounds one reported processor: strings cut, segments, capabilities and event types
    /// capped, an empty or too long segment dropped, a too long position and a lag below -1
    /// read as unknown, an unknown state as `Unspecified`.
    fn bounded(p: &proto::ProcessorStatus, name: String) -> Self {
        let mut segments: Vec<SegmentReport> = Vec::new();
        let mut seen = BTreeSet::new();
        for seg in &p.segments {
            if segments.len() == MAX_SEGMENTS {
                break;
            }
            if seg.segment.is_empty()
                || seg.segment.len() > MAX_SEGMENT_BYTES
                || !seen.insert(seg.segment.as_str())
            {
                continue;
            }
            segments.push(SegmentReport {
                segment: seg.segment.clone(),
                position: kept_whole(&seg.position),
                committed: kept_whole(&seg.committed),
                head: kept_whole(&seg.head),
                lag: lag(seg.lag),
                replay_until: kept_whole(&seg.replay_until),
            });
        }
        segments.sort_by(|a, b| segment_order(&a.segment, &b.segment));
        Self {
            name,
            subscription: truncate_utf8(&p.subscription, MAX_SUBSCRIPTION_SOURCE_BYTES),
            source: truncate_utf8(&p.source, MAX_SUBSCRIPTION_SOURCE_BYTES),
            event_store: truncate_utf8(&p.event_store, MAX_EVENT_STORE_BYTES),
            capabilities: distinct_bounded(&p.capabilities, MAX_CAPABILITIES, MAX_CAPABILITY_BYTES),
            state: ProcessorStateName::from_proto(p.state),
            segments,
            processed_total: p.processed_total,
            failed_total: p.failed_total,
            dead_letters: p.dead_letters,
            last_error: truncate_utf8(&p.last_error, MAX_LAST_ERROR_BYTES),
            last_error_at_ms: p.last_error_at_ms.max(0),
            event_types: distinct_bounded(&p.event_types, MAX_EVENT_TYPES, MAX_EVENT_TYPE_BYTES),
            dead_letter_mode: truncate_utf8(&p.dead_letter_mode, MAX_MODE_BYTES),
            replaying: p.replaying,
            operation_id: truncate_utf8(&p.operation_id, MAX_OPERATION_ID_BYTES),
        }
    }

    /// The event store declares `capability`.
    pub fn can(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }

    pub fn to_proto(&self) -> proto::ProcessorStatus {
        proto::ProcessorStatus {
            name: self.name.clone(),
            subscription: self.subscription.clone(),
            source: self.source.clone(),
            state: self.state.to_proto() as i32,
            segments: self
                .segments
                .iter()
                .map(|s| proto::SegmentStatus {
                    segment: s.segment.clone(),
                    position: s.position.clone(),
                    committed: s.committed.clone(),
                    head: s.head.clone(),
                    lag: s.lag,
                    replay_until: s.replay_until.clone(),
                })
                .collect(),
            processed_total: self.processed_total,
            failed_total: self.failed_total,
            dead_letters: self.dead_letters,
            last_error: self.last_error.clone(),
            last_error_at_ms: self.last_error_at_ms,
            event_types: self.event_types.clone(),
            dead_letter_mode: self.dead_letter_mode.clone(),
            replaying: self.replaying,
            operation_id: self.operation_id.clone(),
            event_store: self.event_store.clone(),
            capabilities: self.capabilities.clone(),
        }
    }
}

/// Bounds a reported `InstanceStatus.processors`: at most [`MAX_PROCESSORS`] processors in the
/// reported order; an empty name and a name already kept (after cutting) are dropped, so each
/// name is one processor. Never fails.
pub fn bound(reported: &[proto::ProcessorStatus]) -> Vec<ProcessorReport> {
    let mut out: Vec<ProcessorReport> = Vec::new();
    for p in reported {
        if out.len() == MAX_PROCESSORS {
            break;
        }
        let name = truncate_utf8(&p.name, MAX_NAME_BYTES);
        if name.is_empty() || out.iter().any(|kept| kept.name == name) {
            continue;
        }
        out.push(ProcessorReport::bounded(p, name));
    }
    out
}

// ---------------------------------------------------------------------------------------------
// GET /api/v1/processors

/// Every service's processors as this hub sees them.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessorsView {
    pub hub_id: String,
    pub now_ms: i64,
    /// Services with at least one reported processor, sorted by name.
    pub services: Vec<ServiceProcessors>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ServiceProcessors {
    pub service_name: String,
    /// The sum of the processors' `deadLetters`.
    pub dead_letters: u64,
    /// Sorted by name.
    pub processors: Vec<ProcessorAggregate>,
}

/// One processor of one service, aggregated over the instances that report it.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessorAggregate {
    pub name: String,
    /// Subscription, source, event store, capabilities and dead-letter mode of the instance with
    /// the freshest report.
    pub subscription: String,
    pub source: String,
    pub event_store: String,
    pub capabilities: Vec<String>,
    pub dead_letter_mode: String,
    /// The worst state of its instances (FAILING, unknown, STARTING, PAUSED, STOPPED, RUNNING).
    pub state: ProcessorStateName,
    pub instances_reporting: u32,
    /// Distinct segments over the instances (a segment two instances report during a
    /// reassignment counts once, as the freshest report has it).
    pub assigned_segments: u32,
    /// The sum of the known lags of those segments; `None` when no lag is known.
    pub lag: Option<i64>,
    /// The highest `deadLetters` of its instances (each samples the service-wide count).
    pub dead_letters: u64,
    /// Sums over the instances (each counts since its own start).
    pub processed_total: u64,
    pub failed_total: u64,
    /// The union of its instances' event types, sorted.
    pub event_types: Vec<String>,
    /// The most recent last error of its instances; `None` when none reported one.
    pub last_error: Option<LastError>,
    /// Some instance replays (ADR 0023).
    pub replaying: bool,
    /// The reset operation of the freshest report that names one; empty when none does.
    pub operation_id: String,
    /// Sorted by instance id.
    pub instances: Vec<ProcessorInstance>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LastError {
    pub message: String,
    /// Epoch ms; 0 when the instance did not say.
    pub at_ms: i64,
    pub instance_id: String,
}

/// One instance's report of the processor, with the age of that report.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ProcessorInstance {
    pub instance_id: String,
    /// Milliseconds since the hub received the report.
    pub status_age_ms: i64,
    #[serde(flatten)]
    pub report: ProcessorReport,
}

/// Freshest first (lowest report age), then by instance id.
fn freshness(i: &ProcessorInstance) -> (i64, &str) {
    (i.status_age_ms, i.instance_id.as_str())
}

/// Aggregates one processor over the instances that report it (`instances` not empty).
pub fn aggregate(name: &str, mut instances: Vec<ProcessorInstance>) -> ProcessorAggregate {
    instances.sort_by(|a, b| a.instance_id.cmp(&b.instance_id));
    let freshest = instances
        .iter()
        .min_by(|a, b| freshness(a).cmp(&freshness(b)))
        .map(|i| &i.report)
        .cloned()
        .unwrap_or_default();
    let state = instances
        .iter()
        .map(|i| i.report.state)
        .max_by_key(|s| s.severity())
        .unwrap_or_default();
    // Each segment as the freshest instance that reports it has it.
    let mut by_freshness: Vec<&ProcessorInstance> = instances.iter().collect();
    by_freshness.sort_by(|a, b| freshness(a).cmp(&freshness(b)));
    let mut segments: BTreeMap<&str, &SegmentReport> = BTreeMap::new();
    for i in &by_freshness {
        for s in &i.report.segments {
            segments.entry(s.segment.as_str()).or_insert(s);
        }
    }
    let known: Vec<i64> = segments
        .values()
        .map(|p| p.lag)
        .filter(|l| *l >= 0)
        .collect();
    let lag = (!known.is_empty()).then(|| known.iter().fold(0i64, |a, l| a.saturating_add(*l)));
    let last_error = instances
        .iter()
        .filter(|i| !i.report.last_error.is_empty())
        .max_by(|a, b| {
            (a.report.last_error_at_ms, std::cmp::Reverse(&a.instance_id))
                .cmp(&(b.report.last_error_at_ms, std::cmp::Reverse(&b.instance_id)))
        })
        .map(|i| LastError {
            message: i.report.last_error.clone(),
            at_ms: i.report.last_error_at_ms,
            instance_id: i.instance_id.clone(),
        });
    let event_types: BTreeSet<&String> = instances
        .iter()
        .flat_map(|i| &i.report.event_types)
        .collect();
    let operation_id = by_freshness
        .iter()
        .map(|i| &i.report.operation_id)
        .find(|op| !op.is_empty())
        .cloned()
        .unwrap_or_default();
    ProcessorAggregate {
        name: name.to_string(),
        subscription: freshest.subscription,
        source: freshest.source,
        event_store: freshest.event_store,
        capabilities: freshest.capabilities,
        dead_letter_mode: freshest.dead_letter_mode,
        state,
        instances_reporting: instances.len() as u32,
        assigned_segments: segments.len() as u32,
        lag,
        dead_letters: instances
            .iter()
            .map(|i| i.report.dead_letters)
            .max()
            .unwrap_or(0),
        processed_total: instances
            .iter()
            .fold(0u64, |a, i| a.saturating_add(i.report.processed_total)),
        failed_total: instances
            .iter()
            .fold(0u64, |a, i| a.saturating_add(i.report.failed_total)),
        event_types: event_types.into_iter().cloned().collect(),
        last_error,
        replaying: instances.iter().any(|i| i.report.replaying),
        operation_id,
        instances,
    }
}

/// The per-service processors view of a topology snapshot.
pub fn view(t: &Topology) -> ProcessorsView {
    let reports = t.services.iter().flat_map(|s| {
        s.instances.iter().flat_map(move |i| {
            i.processors.iter().map(move |p| {
                (
                    s.service_name.clone(),
                    ProcessorInstance {
                        instance_id: i.instance_id.clone(),
                        status_age_ms: i.status_age_ms,
                        report: p.clone(),
                    },
                )
            })
        })
    });
    ProcessorsView {
        hub_id: t.hub_id.clone(),
        now_ms: t.now_ms,
        services: by_service(reports),
    }
}

/// Every service's processors, each aggregated over the instances that report it
/// ([`aggregate`]), from `(service, one instance's report of one processor)`; services sorted
/// by name. The Processors page and the processor gauges (ADR 0026) both read this.
pub fn by_service(
    reports: impl IntoIterator<Item = (String, ProcessorInstance)>,
) -> Vec<ServiceProcessors> {
    let mut grouped: BTreeMap<String, BTreeMap<String, Vec<ProcessorInstance>>> = BTreeMap::new();
    for (service, instance) in reports {
        grouped
            .entry(service)
            .or_default()
            .entry(instance.report.name.clone())
            .or_default()
            .push(instance);
    }
    grouped
        .into_iter()
        .map(|(service_name, by_name)| {
            let processors: Vec<ProcessorAggregate> = by_name
                .into_iter()
                .map(|(name, instances)| aggregate(&name, instances))
                .collect();
            ServiceProcessors {
                service_name,
                dead_letters: processors
                    .iter()
                    .fold(0u64, |a, p| a.saturating_add(p.dead_letters)),
                processors,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(
        segment: &str,
        position: &str,
        committed: &str,
        head: &str,
        lag: i64,
    ) -> proto::SegmentStatus {
        proto::SegmentStatus {
            segment: segment.to_string(),
            position: position.to_string(),
            committed: committed.to_string(),
            head: head.to_string(),
            lag,
            replay_until: String::new(),
        }
    }

    fn status(name: &str, state: proto::ProcessorState) -> proto::ProcessorStatus {
        proto::ProcessorStatus {
            name: name.to_string(),
            subscription: "svc".to_string(),
            source: "kraken.events".to_string(),
            event_store: "kafka".to_string(),
            capabilities: vec!["lag".to_string(), "replay".to_string()],
            state: state as i32,
            dead_letter_mode: "collection".to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn truncation_keeps_whole_characters() {
        assert_eq!(truncate_utf8("abc", 3), "abc");
        assert_eq!(truncate_utf8("abcd", 3), "abc");
        // "é" is two bytes: cutting inside it drops it whole.
        assert_eq!(truncate_utf8("aé", 2), "a");
        assert_eq!(truncate_utf8("aé", 3), "aé");
        assert_eq!(truncate_utf8("🦑🦑", 5), "🦑");
        assert_eq!(truncate_utf8("", 0), "");
    }

    #[test]
    fn decimal_segments_sort_by_value_before_other_names() {
        let mut names = vec!["b", "10", "a", "2", "02", "0", "kraken.events-1"];
        names.sort_by(|a, b| segment_order(a, b));
        assert_eq!(names, ["0", "02", "2", "10", "a", "b", "kraken.events-1"]);
        // Only ASCII digits are decimal (no sign); any length compares by value, no overflow;
        // equal values by their bytes. ui/processors.js sorts this list the same way.
        let mut names = vec![
            "b",
            "+5",
            "7",
            "99999999999999999999999",
            "007",
            "18446744073709551616",
            "10",
            "a",
            "0",
            "",
            "00",
            "5",
        ];
        names.sort_by(|a, b| segment_order(a, b));
        assert_eq!(
            names,
            [
                "0",
                "00",
                "5",
                "007",
                "7",
                "10",
                "18446744073709551616",
                "99999999999999999999999",
                "",
                "+5",
                "a",
                "b",
            ]
        );
    }

    #[test]
    fn a_report_is_bounded_and_never_refused() {
        let mut reported: Vec<proto::ProcessorStatus> = (0..40)
            .map(|i| status(&format!("p{i:02}"), proto::ProcessorState::ProcessorRunning))
            .collect();
        // An empty name and a duplicate are dropped and do not count towards the cap.
        reported.insert(1, status("", proto::ProcessorState::ProcessorRunning));
        reported.insert(2, status("p00", proto::ProcessorState::ProcessorFailing));
        let kept = bound(&reported);
        assert_eq!(kept.len(), MAX_PROCESSORS);
        assert_eq!(kept[0].name, "p00");
        assert_eq!(kept[0].state, ProcessorStateName::Running, "first p00 wins");
        assert_eq!(kept[1].name, "p01");
        assert_eq!(kept[31].name, "p31");

        let mut big = status(&"n".repeat(100), proto::ProcessorState::ProcessorRunning);
        big.subscription = "g".repeat(300);
        big.source = format!("{}é", "t".repeat(255));
        big.event_store = format!("{}é", "k".repeat(63));
        big.capabilities = (0..20).map(|i| format!("cap-{i:02}")).collect();
        big.capabilities.insert(0, "c".repeat(100));
        big.last_error = "e".repeat(600);
        big.dead_letter_mode = "m".repeat(40);
        big.segments = (0..2000)
            .rev()
            .map(|p| seg(&p.to_string(), "1", "1", "1", 0))
            .collect();
        big.event_types = (0..300).map(|i| format!("ev.T{i:03}")).collect();
        big.event_types.push(format!("{}x", "x".repeat(300)));
        let kept = bound(&[big]);
        let p = &kept[0];
        assert_eq!(p.name.len(), MAX_NAME_BYTES);
        assert_eq!(p.subscription.len(), MAX_SUBSCRIPTION_SOURCE_BYTES);
        assert_eq!(p.source, "t".repeat(255), "cut before the two-byte é");
        assert_eq!(p.event_store, "k".repeat(63), "cut before the two-byte é");
        assert_eq!(p.capabilities.len(), MAX_CAPABILITIES);
        // The first 16 reported (the long one, then cap-00 to cap-14), cut and sorted.
        assert_eq!(p.capabilities[0], "cap-00");
        assert_eq!(p.capabilities[14], "cap-14");
        assert_eq!(p.capabilities[15], "c".repeat(MAX_CAPABILITY_BYTES));
        assert_eq!(p.last_error.len(), MAX_LAST_ERROR_BYTES);
        assert_eq!(p.dead_letter_mode.len(), MAX_MODE_BYTES);
        assert_eq!(p.segments.len(), MAX_SEGMENTS);
        // The first 1024 reported (1999 down to 976), sorted by value.
        assert_eq!(p.segments[0].segment, "976");
        assert_eq!(p.segments[1023].segment, "1999");
        assert_eq!(p.event_types.len(), MAX_EVENT_TYPES);
        assert_eq!(p.event_types[0], "ev.T000");
        assert_eq!(p.event_types[255], "ev.T255");
    }

    #[test]
    fn positions_states_and_segments_are_normalised() {
        let mut p = status("default", proto::ProcessorState::ProcessorRunning);
        let long = "9".repeat(MAX_SEGMENT_BYTES + 1);
        p.segments = vec![
            seg("10", "10", "8", "12", 4),
            seg("", "0", "0", "0", 0),
            seg(&long, "0", "0", "0", 0),
            seg("2", "", "", &long, -3),
            seg("10", "99", "99", "99", 99),
            seg("events-b", "7", "", "9", 2),
        ];
        p.last_error_at_ms = -5;
        p.event_types = vec!["b".into(), "".into(), "a".into(), "b".into()];
        p.capabilities = vec!["replay".into(), "".into(), "lag".into(), "replay".into()];
        let kept = &bound(&[p])[0];
        let report =
            |segment: &str, position: &str, committed: &str, head: &str, lag: i64| SegmentReport {
                segment: segment.into(),
                position: position.into(),
                committed: committed.into(),
                head: head.into(),
                lag,
                replay_until: String::new(),
            };
        assert_eq!(
            kept.segments,
            [
                report("2", "", "", "", -1),
                report("10", "10", "8", "12", 4),
                report("events-b", "7", "", "9", 2),
            ],
            "an empty, a too long and a repeated segment dropped, a too long position and a lag \
             below -1 read as unknown, decimal segments by value before other names"
        );
        assert_eq!(kept.last_error_at_ms, 0);
        assert_eq!(kept.event_types, ["a", "b"]);
        assert_eq!(kept.capabilities, ["lag", "replay"]);
        assert!(kept.can(CAPABILITY_REPLAY) && !kept.can("browse"));

        for (value, want) in [
            (0, ProcessorStateName::Unspecified),
            (1, ProcessorStateName::Starting),
            (2, ProcessorStateName::Running),
            (3, ProcessorStateName::Failing),
            (4, ProcessorStateName::Stopped),
            (5, ProcessorStateName::Paused),
            (42, ProcessorStateName::Unspecified),
        ] {
            let mut s = status("default", proto::ProcessorState::Unspecified);
            s.state = value;
            assert_eq!(bound(&[s])[0].state, want, "{value}");
        }
    }

    #[test]
    fn the_control_fields_are_kept_bounded() {
        let mut p = status("audit", proto::ProcessorState::ProcessorStopped);
        p.replaying = true;
        p.operation_id = format!("{}é", "o".repeat(MAX_OPERATION_ID_BYTES - 1));
        let mut replaying = seg("0", "5", "4", "9", 5);
        replaying.replay_until = "450".into();
        let mut odd = seg("1", "1", "1", "1", 0);
        odd.replay_until = "x".repeat(MAX_SEGMENT_BYTES + 1);
        p.segments = vec![replaying, odd];
        let kept = &bound(&[p])[0];
        assert!(kept.replaying);
        assert_eq!(kept.operation_id, "o".repeat(MAX_OPERATION_ID_BYTES - 1));
        assert_eq!(kept.segments[0].replay_until, "450");
        assert_eq!(
            kept.segments[1].replay_until, "",
            "too long reads as not replaying"
        );
        let json = serde_json::to_value(kept).unwrap();
        assert_eq!(json["replaying"], true);
        assert_eq!(json["operationId"], kept.operation_id.as_str());
        assert_eq!(json["segments"][0]["replayUntil"], "450");
    }

    #[test]
    fn a_processor_replays_when_any_instance_does_and_shows_the_freshest_operation() {
        use ProcessorStateName::{Running, Stopped};
        let mut old = report(Stopped, &[]);
        old.operation_id = "op-old".into();
        let mut fresh = report(Stopped, &[]);
        fresh.operation_id = "op-fresh".into();
        let mut replaying = report(Running, &[]);
        replaying.replaying = true;
        let a = aggregate(
            "default",
            vec![
                instance("a", 3000, old),
                instance("b", 100, fresh),
                instance("c", 50, replaying),
            ],
        );
        assert!(a.replaying);
        assert_eq!(
            a.operation_id, "op-fresh",
            "the freshest report that names one"
        );
        let quiet = aggregate("default", vec![instance("a", 0, report(Running, &[]))]);
        assert!(!quiet.replaying);
        assert_eq!(quiet.operation_id, "");
        let json = serde_json::to_value(&a).unwrap();
        assert_eq!(
            (json["replaying"].clone(), json["operationId"].clone()),
            (true.into(), "op-fresh".into())
        );
    }

    #[test]
    fn proto_round_trip_keeps_a_bounded_report() {
        let mut p = status("orders", proto::ProcessorState::ProcessorFailing);
        p.segments = vec![seg("0", "5", "4", "9", 5), seg("1", "", "", "", -1)];
        p.processed_total = 10;
        p.failed_total = 2;
        p.dead_letters = 3;
        p.last_error = "boom".into();
        p.last_error_at_ms = 1234;
        p.event_types = vec!["a.B".into()];
        p.replaying = true;
        p.operation_id = "op-1".into();
        p.segments[0].replay_until = "9".into();
        let kept = bound(std::slice::from_ref(&p));
        assert_eq!(kept[0].to_proto(), p);
        assert_eq!(bound(&[kept[0].to_proto()]), kept);
    }

    fn instance(id: &str, age: i64, report: ProcessorReport) -> ProcessorInstance {
        ProcessorInstance {
            instance_id: id.to_string(),
            status_age_ms: age,
            report,
        }
    }

    fn report(state: ProcessorStateName, segments: &[(&str, i64)]) -> ProcessorReport {
        ProcessorReport {
            name: "default".into(),
            subscription: "svc".into(),
            source: "kraken.events".into(),
            event_store: "kafka".into(),
            capabilities: vec!["lag".into(), "replay".into()],
            state,
            segments: segments
                .iter()
                .map(|(s, lag)| SegmentReport {
                    segment: s.to_string(),
                    position: "0".into(),
                    committed: "0".into(),
                    head: "0".into(),
                    lag: *lag,
                    replay_until: String::new(),
                })
                .collect(),
            dead_letter_mode: "collection".into(),
            ..Default::default()
        }
    }

    #[test]
    fn the_worst_state_wins() {
        use ProcessorStateName::*;
        for (states, want) in [
            (vec![Running, Running], Running),
            (vec![Running, Stopped], Stopped),
            (vec![Stopped, Paused], Paused),
            (vec![Paused, Starting, Running], Starting),
            (vec![Starting, Unspecified], Unspecified),
            (vec![Running, Failing, Starting, Unspecified], Failing),
        ] {
            let instances = states
                .iter()
                .enumerate()
                .map(|(n, s)| instance(&format!("i{n}"), 0, report(*s, &[])))
                .collect();
            assert_eq!(aggregate("default", instances).state, want, "{states:?}");
        }
    }

    #[test]
    fn lag_is_the_sum_of_known_lags_and_unknown_when_none_is_known() {
        use ProcessorStateName::Running;
        let a = aggregate(
            "default",
            vec![
                instance("i1", 0, report(Running, &[("0", 3), ("1", -1)])),
                instance("i2", 0, report(Running, &[("2", 4)])),
            ],
        );
        assert_eq!((a.assigned_segments, a.lag), (3, Some(7)));
        let unknown = aggregate(
            "default",
            vec![instance("i1", 0, report(Running, &[("0", -1), ("1", -1)]))],
        );
        assert_eq!((unknown.assigned_segments, unknown.lag), (2, None));
        let none = aggregate("default", vec![instance("i1", 0, report(Running, &[]))]);
        assert_eq!((none.assigned_segments, none.lag), (0, None));
    }

    #[test]
    fn a_segment_two_instances_report_counts_once_as_the_freshest_has_it() {
        use ProcessorStateName::Running;
        let mut stale = report(Running, &[("0", 100), ("1", 1)]);
        stale.subscription = "old-subscription".into();
        let mut fresh = report(Running, &[("0", 2)]);
        fresh.subscription = "svc".into();
        fresh.source = "kraken.events".into();
        let a = aggregate(
            "default",
            vec![
                instance("a-stale", 4000, stale),
                instance("b-fresh", 200, fresh),
            ],
        );
        assert_eq!(a.assigned_segments, 2);
        assert_eq!(
            a.lag,
            Some(3),
            "segment 0 from the fresh report (2), 1 from the other"
        );
        assert_eq!(a.subscription, "svc", "subscription of the freshest report");
        assert_eq!(a.instances_reporting, 2);
        assert_eq!(
            a.instances
                .iter()
                .map(|i| i.instance_id.as_str())
                .collect::<Vec<_>>(),
            ["a-stale", "b-fresh"]
        );
    }

    #[test]
    fn dead_letters_take_the_max_totals_the_sum_and_the_latest_error_wins() {
        use ProcessorStateName::{Failing, Running};
        let mut one = report(Running, &[]);
        one.dead_letters = 4;
        one.processed_total = 10;
        one.failed_total = 1;
        one.last_error = "older".into();
        one.last_error_at_ms = 1000;
        one.event_types = vec!["a.X".into(), "a.Y".into()];
        let mut two = report(Failing, &[]);
        two.dead_letters = 6;
        two.processed_total = 5;
        two.failed_total = 2;
        two.last_error = "newer".into();
        two.last_error_at_ms = 2000;
        two.event_types = vec!["a.Y".into(), "a.Z".into()];
        let three = report(Running, &[]);
        let a = aggregate(
            "default",
            vec![
                instance("i1", 0, one),
                instance("i2", 0, two),
                instance("i3", 0, three),
            ],
        );
        assert_eq!(a.dead_letters, 6);
        assert_eq!((a.processed_total, a.failed_total), (15, 3));
        assert_eq!(
            a.last_error,
            Some(LastError {
                message: "newer".into(),
                at_ms: 2000,
                instance_id: "i2".into()
            })
        );
        assert_eq!(a.event_types, ["a.X", "a.Y", "a.Z"]);
        let quiet = aggregate("default", vec![instance("i3", 0, report(Running, &[]))]);
        assert_eq!(quiet.last_error, None);
    }

    #[test]
    fn the_view_groups_by_service_and_name_and_skips_silent_services() {
        use crate::topology::{InstanceTopology, ServiceTopology, Totals};
        let inst = |id: &str, age: i64, processors: Vec<ProcessorReport>| InstanceTopology {
            instance_id: id.into(),
            connected_since_ms: 0,
            last_heartbeat_age_ms: 0,
            stale: false,
            in_flight_as_handler: 0,
            in_flight_as_sender: 0,
            command_types: vec![],
            query_types: vec![],
            client_version: String::new(),
            permits_available: 0,
            queued: 0,
            load_factor: 100,
            tags: Default::default(),
            connected_hub_ids: vec![],
            status_age_ms: age,
            shared_query_types: vec![],
            auth: None,
            authenticated_service: None,
            auth_kind: None,
            expires_at: None,
            cert_fingerprint: None,
            processors,
        };
        let named = |name: &str, dead: u64| ProcessorReport {
            name: name.into(),
            dead_letters: dead,
            ..report(ProcessorStateName::Running, &[("0", 1)])
        };
        let t = Topology {
            hub_id: "hub-a".into(),
            hub_started_at_ms: 0,
            now_ms: 99,
            heartbeat_timeout_ms: 10_000,
            services: vec![
                ServiceTopology {
                    service_name: "quiet".into(),
                    instances: vec![inst("q1", -1, vec![])],
                },
                ServiceTopology {
                    service_name: "svc".into(),
                    instances: vec![
                        inst("s1", 100, vec![named("orders", 2), named("default", 1)]),
                        inst("s2", 200, vec![named("default", 1)]),
                    ],
                },
            ],
            types: vec![],
            totals: Totals {
                services: 2,
                instances: 3,
                command_types: 0,
                query_types: 0,
                in_flight: 0,
                queued: 0,
            },
        };
        let v = view(&t);
        assert_eq!((v.hub_id.as_str(), v.now_ms), ("hub-a", 99));
        assert_eq!(
            v.services.len(),
            1,
            "a service without processors is not listed"
        );
        let svc = &v.services[0];
        assert_eq!(svc.service_name, "svc");
        assert_eq!(svc.dead_letters, 3);
        let names: Vec<&str> = svc.processors.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["default", "orders"]);
        assert_eq!(svc.processors[0].instances_reporting, 2);
        assert_eq!(svc.processors[0].instances[1].status_age_ms, 200);

        let json = serde_json::to_value(&v).unwrap();
        let p = &json["services"][0]["processors"][0];
        for key in [
            "name",
            "subscription",
            "source",
            "eventStore",
            "capabilities",
            "deadLetterMode",
            "state",
            "instancesReporting",
            "assignedSegments",
            "lag",
            "deadLetters",
            "processedTotal",
            "failedTotal",
            "eventTypes",
            "lastError",
            "instances",
            "replaying",
            "operationId",
        ] {
            assert!(p.get(key).is_some(), "{key} missing in {p}");
        }
        assert_eq!(p["state"], "RUNNING");
        assert_eq!(p["lag"], 1);
        assert_eq!(p["lastError"], serde_json::Value::Null);
        let i = &p["instances"][0];
        for key in [
            "instanceId",
            "statusAgeMs",
            "state",
            "segments",
            "processedTotal",
            "failedTotal",
            "deadLetters",
            "lastError",
            "lastErrorAtMs",
            "eventTypes",
            "subscription",
            "source",
            "eventStore",
            "capabilities",
            "deadLetterMode",
            "replaying",
            "operationId",
        ] {
            assert!(i.get(key).is_some(), "{key} missing in {i}");
        }
        assert_eq!(
            i["segments"][0],
            serde_json::json!({"segment": "0", "position": "0", "committed": "0", "head": "0", "lag": 1, "replayUntil": ""})
        );
        assert_eq!(serde_json::from_value::<ProcessorsView>(json).unwrap(), v);
    }

    #[test]
    fn an_unknown_state_name_in_json_reads_as_unspecified() {
        let s: ProcessorStateName = serde_json::from_str("\"HIBERNATING\"").unwrap();
        assert_eq!(s, ProcessorStateName::Unspecified);
        assert_eq!(
            serde_json::to_string(&ProcessorStateName::Failing).unwrap(),
            "\"FAILING\""
        );
    }
}
