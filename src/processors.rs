//! Event processors (ADR 0022, roadmap P6a): what each instance reports about its Kafka event
//! processors in `InstanceStatus.processors` (capability "processors"), bounded as the hub keeps
//! it, and the per-service view the dashboard and `GET /api/v1/processors` show.
//!
//! The hub only relays and aggregates what the libraries report: it reads no Kafka, keeps no
//! history and stores nothing beyond the latest report of each live session (in the session's
//! [`crate::session::StatusDetail`], dropped with the session). Never payloads, keys or headers.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::proto;
use crate::topology::Topology;

/// Processors kept from one report; further entries are dropped.
pub const MAX_PROCESSORS: usize = 32;
/// Partitions kept per processor.
pub const MAX_PARTITIONS: usize = 1024;
/// Event types kept per processor.
pub const MAX_EVENT_TYPES: usize = 256;
/// Longest processor name kept, in bytes (longer names are cut at a UTF-8 boundary).
pub const MAX_NAME_BYTES: usize = 64;
/// Longest consumer group id and topic kept, in bytes.
pub const MAX_GROUP_TOPIC_BYTES: usize = 256;
/// Longest last error kept, in bytes.
pub const MAX_LAST_ERROR_BYTES: usize = 512;
/// Longest event type kept, in bytes.
pub const MAX_EVENT_TYPE_BYTES: usize = 256;
/// Longest dead-letter mode kept, in bytes (the defined modes are at most 10).
pub const MAX_MODE_BYTES: usize = 32;

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

/// One assigned partition as reported (`PartitionStatus`); -1 is unknown or none.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct PartitionReport {
    pub partition: i32,
    pub position: i64,
    pub committed: i64,
    pub high_watermark: i64,
    pub lag: i64,
}

/// One processor of one instance as the hub keeps it (`ProcessorStatus`, bounded).
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct ProcessorReport {
    pub name: String,
    pub group_id: String,
    pub topic: String,
    pub state: ProcessorStateName,
    /// Sorted by partition, distinct.
    pub partitions: Vec<PartitionReport>,
    pub processed_total: u64,
    pub failed_total: u64,
    pub dead_letters: u64,
    /// Empty when none.
    pub last_error: String,
    /// Epoch ms; 0 when none.
    pub last_error_at_ms: i64,
    /// Sorted, distinct.
    pub event_types: Vec<String>,
    /// `collection`, `topic` or `log` as reported.
    pub dead_letter_mode: String,
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

/// An offset as kept: anything below -1 reads as -1 (unknown).
fn offset(v: i64) -> i64 {
    v.max(-1)
}

impl ProcessorReport {
    /// Bounds one reported processor: strings cut, partitions and event types capped, offsets
    /// below -1 and negative lags read as unknown (-1), an unknown state as `Unspecified`.
    fn bounded(p: &proto::ProcessorStatus, name: String) -> Self {
        let mut partitions: Vec<PartitionReport> = Vec::new();
        let mut seen = BTreeSet::new();
        for part in &p.partitions {
            if partitions.len() == MAX_PARTITIONS {
                break;
            }
            if part.partition < 0 || !seen.insert(part.partition) {
                continue;
            }
            partitions.push(PartitionReport {
                partition: part.partition,
                position: offset(part.position),
                committed: offset(part.committed),
                high_watermark: offset(part.high_watermark),
                lag: offset(part.lag),
            });
        }
        partitions.sort_by_key(|p| p.partition);
        let mut event_types = BTreeSet::new();
        for t in &p.event_types {
            if event_types.len() == MAX_EVENT_TYPES {
                break;
            }
            let t = truncate_utf8(t, MAX_EVENT_TYPE_BYTES);
            if !t.is_empty() {
                event_types.insert(t);
            }
        }
        Self {
            name,
            group_id: truncate_utf8(&p.group_id, MAX_GROUP_TOPIC_BYTES),
            topic: truncate_utf8(&p.topic, MAX_GROUP_TOPIC_BYTES),
            state: ProcessorStateName::from_proto(p.state),
            partitions,
            processed_total: p.processed_total,
            failed_total: p.failed_total,
            dead_letters: p.dead_letters,
            last_error: truncate_utf8(&p.last_error, MAX_LAST_ERROR_BYTES),
            last_error_at_ms: p.last_error_at_ms.max(0),
            event_types: event_types.into_iter().collect(),
            dead_letter_mode: truncate_utf8(&p.dead_letter_mode, MAX_MODE_BYTES),
        }
    }

    pub fn to_proto(&self) -> proto::ProcessorStatus {
        proto::ProcessorStatus {
            name: self.name.clone(),
            group_id: self.group_id.clone(),
            topic: self.topic.clone(),
            state: self.state.to_proto() as i32,
            partitions: self
                .partitions
                .iter()
                .map(|p| proto::PartitionStatus {
                    partition: p.partition,
                    position: p.position,
                    committed: p.committed,
                    high_watermark: p.high_watermark,
                    lag: p.lag,
                })
                .collect(),
            processed_total: self.processed_total,
            failed_total: self.failed_total,
            dead_letters: self.dead_letters,
            last_error: self.last_error.clone(),
            last_error_at_ms: self.last_error_at_ms,
            event_types: self.event_types.clone(),
            dead_letter_mode: self.dead_letter_mode.clone(),
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
    /// Group id, topic and mode of the instance with the freshest report.
    pub group_id: String,
    pub topic: String,
    pub dead_letter_mode: String,
    /// The worst state of its instances (FAILING, unknown, STARTING, PAUSED, STOPPED, RUNNING).
    pub state: ProcessorStateName,
    pub instances_reporting: u32,
    /// Distinct partitions over the instances (a partition two instances report during a
    /// rebalance counts once, as the freshest report has it).
    pub assigned_partitions: u32,
    /// The sum of the known lags of those partitions; `None` when no lag is known.
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
    // Each partition as the freshest instance that reports it has it.
    let mut by_freshness: Vec<&ProcessorInstance> = instances.iter().collect();
    by_freshness.sort_by(|a, b| freshness(a).cmp(&freshness(b)));
    let mut partitions: BTreeMap<i32, &PartitionReport> = BTreeMap::new();
    for i in &by_freshness {
        for p in &i.report.partitions {
            partitions.entry(p.partition).or_insert(p);
        }
    }
    let known: Vec<i64> = partitions
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
    ProcessorAggregate {
        name: name.to_string(),
        group_id: freshest.group_id,
        topic: freshest.topic,
        dead_letter_mode: freshest.dead_letter_mode,
        state,
        instances_reporting: instances.len() as u32,
        assigned_partitions: partitions.len() as u32,
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
        instances,
    }
}

/// The per-service processors view of a topology snapshot.
pub fn view(t: &Topology) -> ProcessorsView {
    let mut services = Vec::new();
    for s in &t.services {
        let mut by_name: BTreeMap<&str, Vec<ProcessorInstance>> = BTreeMap::new();
        for i in &s.instances {
            for p in &i.processors {
                by_name
                    .entry(p.name.as_str())
                    .or_default()
                    .push(ProcessorInstance {
                        instance_id: i.instance_id.clone(),
                        status_age_ms: i.status_age_ms,
                        report: p.clone(),
                    });
            }
        }
        if by_name.is_empty() {
            continue;
        }
        let processors: Vec<ProcessorAggregate> = by_name
            .into_iter()
            .map(|(name, instances)| aggregate(name, instances))
            .collect();
        services.push(ServiceProcessors {
            service_name: s.service_name.clone(),
            dead_letters: processors
                .iter()
                .fold(0u64, |a, p| a.saturating_add(p.dead_letters)),
            processors,
        });
    }
    ProcessorsView {
        hub_id: t.hub_id.clone(),
        now_ms: t.now_ms,
        services,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(
        partition: i32,
        position: i64,
        committed: i64,
        hw: i64,
        lag: i64,
    ) -> proto::PartitionStatus {
        proto::PartitionStatus {
            partition,
            position,
            committed,
            high_watermark: hw,
            lag,
        }
    }

    fn status(name: &str, state: proto::ProcessorState) -> proto::ProcessorStatus {
        proto::ProcessorStatus {
            name: name.to_string(),
            group_id: "svc".to_string(),
            topic: "kraken.events".to_string(),
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
        big.group_id = "g".repeat(300);
        big.topic = format!("{}é", "t".repeat(255));
        big.last_error = "e".repeat(600);
        big.dead_letter_mode = "m".repeat(40);
        big.partitions = (0..2000).rev().map(|p| part(p, 1, 1, 1, 0)).collect();
        big.event_types = (0..300).map(|i| format!("ev.T{i:03}")).collect();
        big.event_types.push(format!("{}x", "x".repeat(300)));
        let kept = bound(&[big]);
        let p = &kept[0];
        assert_eq!(p.name.len(), MAX_NAME_BYTES);
        assert_eq!(p.group_id.len(), MAX_GROUP_TOPIC_BYTES);
        assert_eq!(p.topic, "t".repeat(255), "cut before the two-byte é");
        assert_eq!(p.last_error.len(), MAX_LAST_ERROR_BYTES);
        assert_eq!(p.dead_letter_mode.len(), MAX_MODE_BYTES);
        assert_eq!(p.partitions.len(), MAX_PARTITIONS);
        // The first 1024 reported (1999 down to 976), sorted.
        assert_eq!(p.partitions[0].partition, 976);
        assert_eq!(p.partitions[1023].partition, 1999);
        assert_eq!(p.event_types.len(), MAX_EVENT_TYPES);
        assert_eq!(p.event_types[0], "ev.T000");
        assert_eq!(p.event_types[255], "ev.T255");
    }

    #[test]
    fn offsets_states_and_partitions_are_normalised() {
        let mut p = status("default", proto::ProcessorState::ProcessorRunning);
        p.partitions = vec![
            part(2, 10, 8, 12, 4),
            part(-1, 0, 0, 0, 0),
            part(0, -7, -1, -9, -3),
            part(2, 99, 99, 99, 99),
        ];
        p.last_error_at_ms = -5;
        p.event_types = vec!["b".into(), "".into(), "a".into(), "b".into()];
        let kept = &bound(&[p])[0];
        assert_eq!(
            kept.partitions,
            [
                PartitionReport {
                    partition: 0,
                    position: -1,
                    committed: -1,
                    high_watermark: -1,
                    lag: -1
                },
                PartitionReport {
                    partition: 2,
                    position: 10,
                    committed: 8,
                    high_watermark: 12,
                    lag: 4
                },
            ],
            "a negative partition and a repeated one dropped, below -1 reads as unknown"
        );
        assert_eq!(kept.last_error_at_ms, 0);
        assert_eq!(kept.event_types, ["a", "b"]);

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
    fn proto_round_trip_keeps_a_bounded_report() {
        let mut p = status("orders", proto::ProcessorState::ProcessorFailing);
        p.partitions = vec![part(0, 5, 4, 9, 5), part(1, -1, -1, -1, -1)];
        p.processed_total = 10;
        p.failed_total = 2;
        p.dead_letters = 3;
        p.last_error = "boom".into();
        p.last_error_at_ms = 1234;
        p.event_types = vec!["a.B".into()];
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

    fn report(state: ProcessorStateName, partitions: &[(i32, i64)]) -> ProcessorReport {
        ProcessorReport {
            name: "default".into(),
            group_id: "svc".into(),
            topic: "kraken.events".into(),
            state,
            partitions: partitions
                .iter()
                .map(|(p, lag)| PartitionReport {
                    partition: *p,
                    position: 0,
                    committed: 0,
                    high_watermark: 0,
                    lag: *lag,
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
                instance("i1", 0, report(Running, &[(0, 3), (1, -1)])),
                instance("i2", 0, report(Running, &[(2, 4)])),
            ],
        );
        assert_eq!((a.assigned_partitions, a.lag), (3, Some(7)));
        let unknown = aggregate(
            "default",
            vec![instance("i1", 0, report(Running, &[(0, -1), (1, -1)]))],
        );
        assert_eq!((unknown.assigned_partitions, unknown.lag), (2, None));
        let none = aggregate("default", vec![instance("i1", 0, report(Running, &[]))]);
        assert_eq!((none.assigned_partitions, none.lag), (0, None));
    }

    #[test]
    fn a_partition_two_instances_report_counts_once_as_the_freshest_has_it() {
        use ProcessorStateName::Running;
        let mut stale = report(Running, &[(0, 100), (1, 1)]);
        stale.group_id = "old-group".into();
        let mut fresh = report(Running, &[(0, 2)]);
        fresh.group_id = "svc".into();
        fresh.topic = "kraken.events".into();
        let a = aggregate(
            "default",
            vec![
                instance("a-stale", 4000, stale),
                instance("b-fresh", 200, fresh),
            ],
        );
        assert_eq!(a.assigned_partitions, 2);
        assert_eq!(
            a.lag,
            Some(3),
            "partition 0 from the fresh report (2), 1 from the other"
        );
        assert_eq!(a.group_id, "svc", "group of the freshest report");
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
            protocol_version: 2,
            client_version: String::new(),
            flow_controlled: false,
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
            ..report(ProcessorStateName::Running, &[(0, 1)])
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
            "groupId",
            "topic",
            "deadLetterMode",
            "state",
            "instancesReporting",
            "assignedPartitions",
            "lag",
            "deadLetters",
            "processedTotal",
            "failedTotal",
            "eventTypes",
            "lastError",
            "instances",
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
            "partitions",
            "processedTotal",
            "failedTotal",
            "deadLetters",
            "lastError",
            "lastErrorAtMs",
            "eventTypes",
            "groupId",
            "topic",
            "deadLetterMode",
        ] {
            assert!(i.get(key).is_some(), "{key} missing in {i}");
        }
        assert_eq!(
            i["partitions"][0],
            serde_json::json!({"partition": 0, "position": 0, "committed": 0, "highWatermark": 0, "lag": 1})
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
