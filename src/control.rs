//! Processor control: the hub as a stateless control plane for the event
//! processors of the libraries, whatever their event store.
//!
//! The hub asks instances to act (`ServerMessage.control`) and
//! matches their answers (`ClientMessage.control_reply`) to the requests by `control_id`. It keeps
//! nothing but the requests in flight and, per (service, processor), the reset running on this
//! hub: a pause lives in the service's `processor_control` collection, which every instance reads,
//! so it reaches instances that start later and outlives any hub. A request whose session leaves
//! fails at once (`instance left`); a reply nobody waits for is dropped and counted.
//!
//! A reset is a leased operation: prepare every instance that runs the processor (each leaves the
//! subscription and holds the operation lock for the lease), renew the lease while it runs,
//! execute on one, finish on all. A hub that dies mid-way stops renewing: the leases expire and
//! the instances resume by themselves.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use crate::auth::clip;
use crate::config::Config;
use crate::context::ContextId;
use crate::processors::{
    MAX_NAME_BYTES, MAX_SEGMENT_BYTES, MAX_SEGMENTS, MAX_SUBSCRIPTION_SOURCE_BYTES, kept_whole,
    segment_order, truncate_utf8,
};
use crate::proto::{
    self, ControlRefusal, ControlReply, ServerMessage, control_reply, control_request,
};
use crate::session::{ProcessorHolder, Sessions};
use crate::telemetry::Metrics;

/// Letters one list asks for at most; also what the hub keeps of a longer page.
pub const MAX_LETTERS: u32 = 100;
/// Letters a list asks for when the caller names no limit.
pub const DEFAULT_LETTERS: u32 = 50;
/// Segment positions one reset may name.
pub const MAX_POSITIONS: usize = MAX_SEGMENTS;
/// Longest message, detail or error text kept from a reply, in bytes.
pub const MAX_TEXT_BYTES: usize = 512;
/// Longest letter id, key, event id or operation id kept or accepted, in bytes.
pub const MAX_ID_BYTES: usize = 128;
/// Longest event or error type kept, in bytes.
const MAX_TYPE_BYTES: usize = 256;
/// Longest dead-letter reason or retry result kept, in bytes (the defined ones are at most 14).
const MAX_WORD_BYTES: usize = 32;

/// How long the hub waits for the answers of one control call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeouts {
    /// Every request but `reset_execute` (`SNODRA_SERVER_CONTROL_TIMEOUT`).
    pub request: Duration,
    /// `reset_execute` (`SNODRA_SERVER_RESET_TIMEOUT`).
    pub reset: Duration,
    /// The lease a `reset_prepare` asks for.
    pub lease: Duration,
    /// How often a running reset renews the lease.
    pub renew: Duration,
}

impl Timeouts {
    pub fn from_config(config: &Config) -> Self {
        Self {
            request: config.control_timeout,
            reset: config.reset_timeout,
            lease: config.control_lease,
            renew: config.control_lease_renew,
        }
    }
}

/// What an operator asked for; the `action` label of `snodra_server_control_total` and the audit
/// line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Pause,
    Resume,
    Reset,
    DeadLetterList,
    DeadLetterRetry,
    DeadLetterRetryAll,
    DeadLetterDiscard,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pause => "pause",
            Self::Resume => "resume",
            Self::Reset => "reset",
            Self::DeadLetterList => "dead_letter_list",
            Self::DeadLetterRetry => "dead_letter_retry",
            Self::DeadLetterRetryAll => "dead_letter_retry_all",
            Self::DeadLetterDiscard => "dead_letter_discard",
        }
    }
}

/// How a control call ended, as the HTTP API reports it (each maps to one status code) and as the
/// `outcome` label counts it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Ok,
    BadRequest,
    NotFound,
    Conflict,
    Failed,
    Timeout,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::BadRequest => "bad_request",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
        }
    }
}

/// Why the hub refused a call before asking any instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// No session of the service is connected to this hub.
    UnknownService,
    /// No session of the service reports the processor.
    UnknownProcessor,
    /// A reset of the processor already runs on this hub (its operation id).
    Running(String),
    /// A reset of a processor whose event store does not declare the `replay` capability, as the
    /// freshest report has it; the event store's name, empty when unnamed.
    Unsupported(String),
}

impl Refusal {
    pub fn verdict(&self) -> Verdict {
        match self {
            Self::UnknownService | Self::UnknownProcessor => Verdict::NotFound,
            Self::Running(_) | Self::Unsupported(_) => Verdict::Conflict,
        }
    }

    /// The refusal code the API shows next to the message, as an instance's refusal would.
    pub fn code(&self) -> Option<&'static str> {
        match self {
            Self::Unsupported(_) => Some("UNSUPPORTED"),
            _ => None,
        }
    }

    pub fn message(&self, service: &str, processor: &str) -> String {
        let (service, processor) = (clip(service), clip(processor));
        match self {
            Self::UnknownService => format!("no instance of service {service} is connected"),
            Self::UnknownProcessor => {
                format!("no instance of service {service} reports processor {processor}")
            }
            Self::Running(op) => {
                format!("a reset of {service}/{processor} is running on this hub (operation {op})")
            }
            Self::Unsupported(store) => {
                let store = if store.is_empty() {
                    "an unnamed event store".to_string()
                } else {
                    format!("event store {}", clip(store))
                };
                format!(
                    "{service}/{processor} reads {store}, which does not declare the replay capability: it cannot be reset"
                )
            }
        }
    }
}

/// How one instance answered one request, bounded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Answer {
    Ok(String),
    Refused {
        /// The `ControlRefusal` name without its prefix (`LOCKED`); `UNSPECIFIED` for a value
        /// this hub does not know.
        code: &'static str,
        message: String,
    },
    Reset(ResetResult),
    Letters(LetterPage),
    Retry(RetryView),
    RetryAll(RetryAllView),
    /// No reply within the timeout.
    Timeout,
    /// The session ended before it replied.
    Left,
    /// The request could not be sent (the session is gone, or its outbound buffer is full).
    Unsent(&'static str),
    /// A reply that does not answer this kind of request (or carries no result).
    Unexpected,
}

/// What the instance that executed a reset did.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResetResult {
    /// Sorted by segment ([`segment_order`]), at most [`MAX_SEGMENTS`].
    pub ranges: Vec<RangeView>,
    pub markers_deleted: u64,
    pub reset_handlers: u32,
}

/// The records of `segment` at a position `from <= position < until` are handled again.
/// Positions are opaque strings; empty means none.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RangeView {
    pub segment: String,
    pub from: String,
    pub until: String,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LetterPage {
    /// Every letter of the processor.
    pub total: u64,
    /// At most [`MAX_LETTERS`], in the instance's order (oldest first).
    pub letters: Vec<LetterView>,
}

/// One dead letter without payload or headers, bounded.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LetterView {
    pub id: String,
    pub processor: String,
    /// The source the letter's record came from.
    pub source: String,
    /// Its segment; empty when unknown (or longer than the hub keeps).
    pub segment: String,
    /// The record's position in its segment; empty when unknown.
    pub position: String,
    pub key: String,
    pub event_id: String,
    pub event_type: String,
    pub reason: String,
    pub error_type: String,
    pub error_message: String,
    pub blocked_by: String,
    pub attempts: u32,
    pub parked_at_ms: i64,
    pub last_attempt_at_ms: i64,
}

impl LetterView {
    fn bounded(l: &proto::DeadLetterSummary) -> Self {
        Self {
            id: truncate_utf8(&l.id, MAX_ID_BYTES),
            processor: truncate_utf8(&l.processor, MAX_NAME_BYTES),
            source: truncate_utf8(&l.source, MAX_SUBSCRIPTION_SOURCE_BYTES),
            // Never cut: a cut name would be another segment's.
            segment: kept_whole(&l.segment),
            position: kept_whole(&l.position),
            key: truncate_utf8(&l.key, MAX_ID_BYTES),
            event_id: truncate_utf8(&l.event_id, MAX_ID_BYTES),
            event_type: truncate_utf8(&l.event_type, MAX_TYPE_BYTES),
            reason: truncate_utf8(&l.reason, MAX_WORD_BYTES),
            error_type: truncate_utf8(&l.error_type, MAX_TYPE_BYTES),
            error_message: truncate_utf8(&l.error_message, MAX_TEXT_BYTES),
            blocked_by: truncate_utf8(&l.blocked_by, MAX_ID_BYTES),
            attempts: l.attempts,
            parked_at_ms: l.parked_at_ms,
            last_attempt_at_ms: l.last_attempt_at_ms,
        }
    }
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetryView {
    /// `Retried`, `Duplicate`, `Failed`, `Blocked`, `NotRetriable` or `NotFound`, as the instance
    /// said.
    pub result: String,
    pub detail: String,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RetryAllView {
    pub retried: u64,
    pub failed: u64,
    pub blocked: u64,
    pub skipped: u64,
}

/// `ControlRefusal` as the API names it.
fn refusal_name(code: i32) -> &'static str {
    match ControlRefusal::try_from(code) {
        Ok(ControlRefusal::UnknownProcessor) => "UNKNOWN_PROCESSOR",
        Ok(ControlRefusal::Locked) => "LOCKED",
        Ok(ControlRefusal::NotPrepared) => "NOT_PREPARED",
        Ok(ControlRefusal::SubscribersActive) => "SUBSCRIBERS_ACTIVE",
        Ok(ControlRefusal::ResetHandlerFailed) => "RESET_HANDLER_FAILED",
        Ok(ControlRefusal::Invalid) => "INVALID",
        Ok(ControlRefusal::Failed) => "FAILED",
        Ok(ControlRefusal::NotFound) => "NOT_FOUND",
        Ok(ControlRefusal::Unsupported) => "UNSUPPORTED",
        Ok(ControlRefusal::Unspecified) | Err(_) => "UNSPECIFIED",
    }
}

impl Answer {
    /// The reply's result, bounded: texts cut at a UTF-8 boundary, at most [`MAX_LETTERS`]
    /// letters and [`MAX_SEGMENTS`] ranges.
    fn from_reply(result: Option<control_reply::Result>) -> Self {
        use control_reply::Result as R;
        match result {
            None => Self::Unexpected,
            Some(R::Ok(ok)) => Self::Ok(truncate_utf8(&ok.detail, MAX_TEXT_BYTES)),
            Some(R::Refused(r)) => Self::Refused {
                code: refusal_name(r.code),
                message: truncate_utf8(&r.message, MAX_TEXT_BYTES),
            },
            Some(R::Reset(r)) => {
                let mut ranges: Vec<RangeView> = r
                    .ranges
                    .iter()
                    // A segment name is never cut: a range of a longer one is dropped.
                    .filter(|x| x.segment.len() <= MAX_SEGMENT_BYTES)
                    .take(MAX_SEGMENTS)
                    .map(|x| RangeView {
                        segment: x.segment.clone(),
                        from: kept_whole(&x.from),
                        until: kept_whole(&x.until),
                    })
                    .collect();
                ranges.sort_by(|a, b| segment_order(&a.segment, &b.segment));
                Self::Reset(ResetResult {
                    ranges,
                    markers_deleted: r.markers_deleted,
                    reset_handlers: r.reset_handlers,
                })
            }
            Some(R::DeadLetters(page)) => Self::Letters(LetterPage {
                total: page.total,
                letters: page
                    .letters
                    .iter()
                    .take(MAX_LETTERS as usize)
                    .map(LetterView::bounded)
                    .collect(),
            }),
            Some(R::Retry(r)) => Self::Retry(RetryView {
                result: truncate_utf8(&r.result, MAX_WORD_BYTES),
                detail: truncate_utf8(&r.detail, MAX_TEXT_BYTES),
            }),
            Some(R::RetryAll(r)) => Self::RetryAll(RetryAllView {
                retried: r.retried,
                failed: r.failed,
                blocked: r.blocked,
                skipped: r.skipped,
            }),
        }
    }

    /// The answer, or [`Answer::Unexpected`] when it is a result of another kind of request.
    fn expected_by(self, action: &control_request::Action) -> Self {
        use control_request::Action as A;
        let fits = match (&self, action) {
            (
                Self::Ok(_),
                A::Pause(_)
                | A::Resume(_)
                | A::ResetPrepare(_)
                | A::ResetFinish(_)
                | A::DeadLetterDiscard(_),
            ) => true,
            (Self::Reset(_), A::ResetExecute(_)) => true,
            (Self::Letters(_), A::DeadLetterList(_)) => true,
            (Self::Retry(_), A::DeadLetterRetry(_)) => true,
            (Self::RetryAll(_), A::DeadLetterRetryAll(_)) => true,
            (
                Self::Ok(_)
                | Self::Reset(_)
                | Self::Letters(_)
                | Self::Retry(_)
                | Self::RetryAll(_),
                _,
            ) => false,
            // A refusal, and what the hub concluded itself, answer any request.
            _ => true,
        };
        if fits { self } else { Self::Unexpected }
    }

    /// A refusal with this code.
    fn refused_with(&self, wanted: &str) -> bool {
        matches!(self, Self::Refused { code, .. } if *code == wanted)
    }

    /// How the call ends when this is the answer that decides it.
    pub fn verdict(&self) -> Verdict {
        match self {
            Self::Ok(_) | Self::Reset(_) | Self::Letters(_) | Self::RetryAll(_) => Verdict::Ok,
            Self::Retry(r) => match r.result.as_str() {
                "Retried" | "Duplicate" | "Failed" => Verdict::Ok,
                "Blocked" | "NotRetriable" => Verdict::Conflict,
                "NotFound" => Verdict::NotFound,
                _ => Verdict::Failed,
            },
            Self::Refused { code, .. } => match *code {
                "UNKNOWN_PROCESSOR" | "NOT_FOUND" => Verdict::NotFound,
                // UNSUPPORTED: the event store lacks the capability; asking again will not help.
                "LOCKED"
                | "NOT_PREPARED"
                | "SUBSCRIBERS_ACTIVE"
                | "RESET_HANDLER_FAILED"
                | "UNSUPPORTED" => Verdict::Conflict,
                "INVALID" => Verdict::BadRequest,
                _ => Verdict::Failed,
            },
            Self::Timeout => Verdict::Timeout,
            Self::Left | Self::Unsent(_) | Self::Unexpected => Verdict::Failed,
        }
    }

    /// The answer as the API shows it (results with data show as `ok`).
    pub fn view(&self) -> AnswerView {
        let (result, code, message) = match self {
            Self::Ok(detail) => ("ok", None, detail.clone()),
            Self::Reset(_) | Self::Letters(_) | Self::RetryAll(_) => ("ok", None, String::new()),
            Self::Retry(r) => ("ok", None, r.detail.clone()),
            Self::Refused { code, message } => ("refused", Some(*code), message.clone()),
            Self::Timeout => ("timeout", None, "no answer in time".to_string()),
            Self::Left => ("left", None, "instance left".to_string()),
            Self::Unsent(why) => ("unsent", None, (*why).to_string()),
            Self::Unexpected => ("unexpected", None, "an answer of another kind".to_string()),
        };
        AnswerView {
            result,
            code,
            message,
        }
    }
}

/// One answer as the API shows it.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AnswerView {
    /// `ok`, `refused`, `timeout`, `left` (the session ended first), `unsent` (the request could
    /// not be sent) or `unexpected` (an answer of another kind).
    pub result: &'static str,
    /// The refusal code (`LOCKED`, ...) when `result` is `refused`.
    pub code: Option<&'static str>,
    pub message: String,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InstanceAnswer {
    pub instance_id: String,
    #[serde(flatten)]
    pub answer: AnswerView,
}

/// The answer of a pause or resume.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FanOutReport {
    pub service: String,
    pub processor: String,
    pub action: &'static str,
    /// Instances that answered `ok` (they persisted it).
    pub acknowledged: usize,
    /// Instances that did not.
    pub failed: usize,
    /// Every instance asked, sorted by instance id.
    pub instances: Vec<InstanceAnswer>,
    #[serde(skip)]
    pub verdict: Verdict,
}

/// Where a reset moves the subscription's committed positions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    Earliest,
    Latest,
    Timestamp(i64),
    /// `(segment, position)`, distinct segments sorted in [`segment_order`], at most
    /// [`MAX_POSITIONS`]; both opaque strings the event-store add-on reads.
    Positions(Vec<(String, String)>),
}

/// A segment name or position a reset may name: 1 to [`MAX_SEGMENT_BYTES`] bytes, no control
/// characters.
fn valid_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= MAX_SEGMENT_BYTES && !s.chars().any(char::is_control)
}

impl Target {
    /// Parses the body of `POST .../reset`: `{"to":"earliest"}`, `{"to":"latest"}`,
    /// `{"to":{"timestampMs":n}}` or `{"to":{"positions":[{"segment":"0","position":"12"}]}}`.
    /// The error is safe to show the caller.
    pub fn parse(body: &[u8]) -> Result<Self, String> {
        use serde_json::Value;
        let value: Value =
            serde_json::from_slice(body).map_err(|e| format!("the body is not JSON: {e}"))?;
        let Value::Object(fields) = value else {
            return Err("the body must be an object with \"to\"".into());
        };
        if let Some(other) = fields.keys().find(|k| *k != "to") {
            return Err(format!("unknown field {:?}", clip(other)));
        }
        let to = fields
            .get("to")
            .ok_or("the body must name the target in \"to\"")?;
        match to {
            Value::String(s) if s == "earliest" => Ok(Self::Earliest),
            Value::String(s) if s == "latest" => Ok(Self::Latest),
            Value::Object(o) if o.len() == 1 && o.contains_key("timestampMs") => {
                match o["timestampMs"].as_i64() {
                    Some(ms) if ms >= 0 => Ok(Self::Timestamp(ms)),
                    _ => Err("timestampMs must be a non-negative integer (epoch ms)".into()),
                }
            }
            Value::Object(o) if o.len() == 1 && o.contains_key("positions") => {
                let Some(list) = o["positions"].as_array() else {
                    return Err("positions must be an array".into());
                };
                if list.is_empty() || list.len() > MAX_POSITIONS {
                    return Err(format!("positions must list 1 to {MAX_POSITIONS} segments"));
                }
                let mut positions: Vec<(String, String)> = Vec::with_capacity(list.len());
                for entry in list {
                    let segment = entry
                        .get("segment")
                        .and_then(Value::as_str)
                        .filter(|s| valid_name(s));
                    let position = entry
                        .get("position")
                        .and_then(Value::as_str)
                        .filter(|p| valid_name(p));
                    let (Some(segment), Some(position)) = (segment, position) else {
                        return Err(format!(
                            "each position must be {{\"segment\": \"...\", \"position\": \"...\"}}, \
                             both strings of 1 to {MAX_SEGMENT_BYTES} bytes"
                        ));
                    };
                    if entry.as_object().is_some_and(|e| e.len() != 2) {
                        return Err("a position has only segment and position".into());
                    }
                    if positions.iter().any(|(s, _)| s == segment) {
                        return Err(format!("segment {:?} is listed twice", clip(segment)));
                    }
                    positions.push((segment.to_string(), position.to_string()));
                }
                positions.sort_by(|a, b| segment_order(&a.0, &b.0));
                Ok(Self::Positions(positions))
            }
            _ => Err(
                "to must be \"earliest\", \"latest\", {\"timestampMs\": n} or {\"positions\": [...]}"
                    .into(),
            ),
        }
    }

    /// The target as the body that names it.
    pub fn to_json(&self) -> serde_json::Value {
        use serde_json::json;
        match self {
            Self::Earliest => json!({ "to": "earliest" }),
            Self::Latest => json!({ "to": "latest" }),
            Self::Timestamp(ms) => json!({ "to": { "timestampMs": ms } }),
            Self::Positions(list) => json!({ "to": { "positions": list
                .iter()
                .map(|(s, p)| json!({ "segment": s, "position": p }))
                .collect::<Vec<_>>() } }),
        }
    }

    fn to_proto(&self) -> proto::ResetTarget {
        use proto::reset_target::To;
        let to = match self {
            Self::Earliest => To::Earliest(true),
            Self::Latest => To::Latest(true),
            Self::Timestamp(ms) => To::TimestampMs(*ms),
            Self::Positions(list) => To::Positions(proto::SegmentPositions {
                positions: list
                    .iter()
                    .map(|(segment, position)| proto::SegmentPosition {
                        segment: segment.clone(),
                        position: position.clone(),
                    })
                    .collect(),
            }),
        };
        proto::ResetTarget { to: Some(to) }
    }
}

/// One instance in a reset: how it answered the prepare and, when it was involved, the finish.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResetInstance {
    pub instance_id: String,
    pub prepare: AnswerView,
    /// `None` when it was not involved (it does not run the processor) or was never sent one.
    pub finish: Option<AnswerView>,
}

/// The answer to a reset that got as far as asking the instances.
#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ResetReport {
    pub service: String,
    pub processor: String,
    pub action: &'static str,
    pub operation_id: String,
    pub target: serde_json::Value,
    /// `ok` (executed), `aborted` (a prepare failed; nothing changed) or `failed` (the execute
    /// failed; every instance was finished as aborted).
    pub outcome: &'static str,
    /// The instance that executed (or was asked to), `None` when the prepare failed.
    pub executed_by: Option<String>,
    /// Some segment replays (the target was before its committed position).
    pub replay: bool,
    pub ranges: Vec<RangeView>,
    pub markers_deleted: u64,
    pub reset_handlers: u32,
    /// The answer that ended the operation, when it did not succeed.
    pub error: Option<StageError>,
    /// Every instance asked, sorted by instance id.
    pub instances: Vec<ResetInstance>,
    #[serde(skip)]
    pub verdict: Verdict,
}

#[derive(Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct StageError {
    /// `prepare` or `execute`.
    pub stage: &'static str,
    pub instance_id: Option<String>,
    #[serde(flatten)]
    pub answer: AnswerView,
}

/// A dead-letter call: what the instance that answered said.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LetterOutcome {
    pub instance_id: String,
    pub answer: Answer,
}

/// What an operator does with a processor's dead letters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LetterCall {
    List(u32),
    Retry(String),
    RetryAll,
    Discard(String),
}

impl LetterCall {
    pub fn action(&self) -> Action {
        match self {
            Self::List(_) => Action::DeadLetterList,
            Self::Retry(_) => Action::DeadLetterRetry,
            Self::RetryAll => Action::DeadLetterRetryAll,
            Self::Discard(_) => Action::DeadLetterDiscard,
        }
    }

    fn to_proto(&self) -> control_request::Action {
        use control_request::Action as A;
        match self {
            Self::List(limit) => A::DeadLetterList(proto::DeadLetterList {
                limit: (*limit).clamp(1, MAX_LETTERS),
            }),
            Self::Retry(id) => A::DeadLetterRetry(proto::DeadLetterRetry {
                letter_id: id.clone(),
            }),
            Self::RetryAll => A::DeadLetterRetryAll(proto::DeadLetterRetryAll {}),
            Self::Discard(id) => A::DeadLetterDiscard(proto::DeadLetterDiscard {
                letter_id: id.clone(),
            }),
        }
    }
}

/// A request waiting for its reply.
struct Waiter {
    context: ContextId,
    instance_id: String,
    /// The session the request went to: a reply from a later session of the same instance id is
    /// not its answer.
    token: u64,
    tx: oneshot::Sender<Answer>,
}

/// The lease renewals of one reset: set `stopped` (under the lock) and no renewal follows;
/// `replies` are the waits of those sent and not yet answered.
#[derive(Default)]
struct Renewals {
    stopped: bool,
    replies: Vec<tokio::task::JoinHandle<()>>,
}

/// The lease renewals of a running reset, stopped when it is dropped: a reset that ends without
/// reaching its finish (its caller went away, a panic) never renews the lease again.
struct Renewing(Arc<Mutex<Renewals>>);

impl Renewing {
    /// Stops the renewals: none is sent after this returns. The waits of those in flight are
    /// returned.
    fn stop(&self) -> Vec<tokio::task::JoinHandle<()>> {
        let mut renewals = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        renewals.stopped = true;
        std::mem::take(&mut renewals.replies)
    }
}

impl Drop for Renewing {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Removes the reset of its (service, processor) from [`Control::resets`] when the operation
/// ends, however it ends.
struct Running {
    control: Arc<Control>,
    key: (String, String),
}

impl Drop for Running {
    fn drop(&mut self) {
        self.control.resets.lock().unwrap().remove(&self.key);
    }
}

/// The control plane of one hub: the requests in flight and the resets it runs.
pub struct Control {
    sessions: Arc<Sessions>,
    metrics: Arc<Metrics>,
    timeouts: Timeouts,
    pending: Mutex<HashMap<String, Waiter>>,
    /// (service, processor) -> the operation id of the reset running on this hub.
    resets: Mutex<HashMap<(String, String), String>>,
}

/// A fresh control or operation id.
fn new_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// The holder with the freshest status (the latest report, ties to the lowest instance id).
fn freshest<'a>(
    holders: impl IntoIterator<Item = &'a ProcessorHolder>,
) -> Option<&'a ProcessorHolder> {
    holders.into_iter().max_by(|a, b| {
        a.reported_at
            .cmp(&b.reported_at)
            .then_with(|| b.instance_id.cmp(&a.instance_id))
    })
}

impl Control {
    pub fn new(sessions: Arc<Sessions>, metrics: Arc<Metrics>, timeouts: Timeouts) -> Arc<Self> {
        Arc::new(Self {
            sessions,
            metrics,
            timeouts,
            pending: Mutex::new(HashMap::new()),
            resets: Mutex::new(HashMap::new()),
        })
    }

    pub fn timeouts(&self) -> Timeouts {
        self.timeouts
    }

    /// Requests waiting for a reply.
    pub fn in_flight(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    /// Sends one request to `holder` and registers its waiter (before the send, so a fast reply
    /// is never missed). `Err` is the answer when it could not be sent.
    fn send(
        &self,
        holder: &ProcessorHolder,
        processor: &str,
        action: control_request::Action,
    ) -> Result<(String, oneshot::Receiver<Answer>), Answer> {
        let control_id = new_id();
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(
            control_id.clone(),
            Waiter {
                context: holder.context.clone(),
                instance_id: holder.instance_id.clone(),
                token: holder.token,
                tx,
            },
        );
        let msg = ServerMessage {
            message: Some(crate::proto::server_message::Message::Control(
                proto::ControlRequest {
                    control_id: control_id.clone(),
                    processor: processor.to_string(),
                    action: Some(action),
                },
            )),
        };
        match self
            .sessions
            .send_if_current(&holder.context, &holder.instance_id, holder.token, msg)
        {
            Ok(()) => Ok((control_id, rx)),
            Err(e) => {
                self.pending.lock().unwrap().remove(&control_id);
                Err(Answer::Unsent(match e {
                    crate::session::SendError::Gone => "instance is gone",
                    crate::session::SendError::Busy => "instance outbound buffer is full",
                }))
            }
        }
    }

    /// Waits for the reply of a sent request, at most `timeout`; a request that times out stops
    /// waiting (its late reply is then unknown).
    async fn wait(
        &self,
        control_id: &str,
        rx: oneshot::Receiver<Answer>,
        timeout: Duration,
    ) -> Answer {
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(answer)) => answer,
            // The waiter was dropped: the session left.
            Ok(Err(_)) => Answer::Left,
            Err(_) => {
                self.pending.lock().unwrap().remove(control_id);
                Answer::Timeout
            }
        }
    }

    /// Asks `holder` to act on `processor` and waits for its answer, at most `timeout`.
    pub async fn ask(
        &self,
        holder: &ProcessorHolder,
        processor: &str,
        action: control_request::Action,
        timeout: Duration,
    ) -> Answer {
        let answer = match self.send(holder, processor, action.clone()) {
            Err(answer) => answer,
            Ok((id, rx)) => self.wait(&id, rx, timeout).await,
        };
        answer.expected_by(&action)
    }

    /// Asks every holder at once; the answers in the holders' order.
    async fn ask_all(
        self: &Arc<Self>,
        holders: &[ProcessorHolder],
        processor: &str,
        action: control_request::Action,
        timeout: Duration,
    ) -> Vec<Answer> {
        let asks: Vec<_> = holders
            .iter()
            .map(|holder| {
                let (this, holder, processor, action) = (
                    Arc::clone(self),
                    holder.clone(),
                    processor.to_string(),
                    action.clone(),
                );
                tokio::spawn(async move { this.ask(&holder, &processor, action, timeout).await })
            })
            .collect();
        let mut answers = Vec::with_capacity(asks.len());
        for ask in asks {
            answers.push(ask.await.unwrap_or(Answer::Unexpected));
        }
        answers
    }

    /// A `ControlReply` from the session `token` of `instance_id`. Handed to the request it
    /// answers; a reply with an unknown id, or from another session than the one asked, is
    /// dropped and counted.
    pub fn on_reply(&self, ctx: &ContextId, instance_id: &str, token: u64, reply: ControlReply) {
        let waiter = {
            let mut pending = self.pending.lock().unwrap();
            match pending.get(&reply.control_id) {
                Some(w)
                    if w.context == *ctx && w.instance_id == instance_id && w.token == token =>
                {
                    pending.remove(&reply.control_id)
                }
                _ => None,
            }
        };
        match waiter {
            Some(w) => {
                let _ = w.tx.send(Answer::from_reply(reply.result));
            }
            None => {
                self.metrics.control_unknown_reply();
                debug!(instance = %instance_id, control_id = %clip(&reply.control_id), "control reply nobody waits for, dropped");
            }
        }
    }

    /// The session `token` of `instance_id` ended: every request it was asked fails at once
    /// (`instance left`).
    pub fn on_disconnect(&self, ctx: &ContextId, instance_id: &str, token: u64) {
        let mut pending = self.pending.lock().unwrap();
        let gone: Vec<String> = pending
            .iter()
            .filter(|(_, w)| w.context == *ctx && w.instance_id == instance_id && w.token == token)
            .map(|(id, _)| id.clone())
            .collect();
        for id in gone {
            if let Some(w) = pending.remove(&id) {
                let _ = w.tx.send(Answer::Left);
            }
        }
    }

    /// The sessions of `service` that report `processor` (at least one), sorted by instance id.
    fn holders(
        &self,
        ctx: &ContextId,
        service: &str,
        processor: &str,
    ) -> Result<Vec<ProcessorHolder>, Refusal> {
        let mut holders = self
            .sessions
            .processor_holders(ctx, service, processor)
            .ok_or(Refusal::UnknownService)?;
        if holders.is_empty() {
            return Err(Refusal::UnknownProcessor);
        }
        holders.sort_by(|a, b| a.instance_id.cmp(&b.instance_id));
        Ok(holders)
    }

    /// Pauses (`paused`) or resumes `processor` on every instance of `service` that reports it.
    pub async fn pause(
        self: &Arc<Self>,
        ctx: &ContextId,
        service: &str,
        processor: &str,
        paused: bool,
    ) -> Result<FanOutReport, Refusal> {
        let holders = self.holders(ctx, service, processor)?;
        let action = if paused {
            control_request::Action::Pause(proto::ProcessorPause {})
        } else {
            control_request::Action::Resume(proto::ProcessorResume {})
        };
        let answers = self
            .ask_all(&holders, processor, action, self.timeouts.request)
            .await;
        let acknowledged = answers
            .iter()
            .filter(|a| matches!(a, Answer::Ok(_)))
            .count();
        let verdict = if acknowledged > 0 {
            Verdict::Ok
        } else if answers.iter().all(|a| *a == Answer::Timeout) {
            Verdict::Timeout
        } else if answers.iter().all(|a| a.refused_with("UNKNOWN_PROCESSOR")) {
            Verdict::NotFound
        } else {
            Verdict::Failed
        };
        Ok(FanOutReport {
            service: service.to_string(),
            processor: processor.to_string(),
            action: if paused { "pause" } else { "resume" },
            acknowledged,
            failed: answers.len() - acknowledged,
            instances: holders
                .iter()
                .zip(&answers)
                .map(|(h, a)| InstanceAnswer {
                    instance_id: h.instance_id.clone(),
                    answer: a.view(),
                })
                .collect(),
            verdict,
        })
    }

    /// Runs a dead-letter call on the instance with the freshest status.
    pub async fn dead_letters(
        self: &Arc<Self>,
        ctx: &ContextId,
        service: &str,
        processor: &str,
        call: &LetterCall,
    ) -> Result<LetterOutcome, Refusal> {
        let holders = self.holders(ctx, service, processor)?;
        let holder = freshest(&holders).ok_or(Refusal::UnknownProcessor)?;
        let answer = self
            .ask(holder, processor, call.to_proto(), self.timeouts.request)
            .await;
        Ok(LetterOutcome {
            instance_id: holder.instance_id.clone(),
            answer,
        })
    }

    /// Takes this hub's slot for a reset of (service, processor).
    fn begin(
        self: &Arc<Self>,
        service: &str,
        processor: &str,
        operation: &str,
    ) -> Result<Running, Refusal> {
        let key = (service.to_string(), processor.to_string());
        let mut resets = self.resets.lock().unwrap();
        if let Some(held) = resets.get(&key) {
            return Err(Refusal::Running(held.clone()));
        }
        resets.insert(key.clone(), operation.to_string());
        Ok(Running {
            control: Arc::clone(self),
            key,
        })
    }

    /// Resets `processor` of `service` to `target` as a leased operation: prepare every instance
    /// that reports it, renew the lease every [`Timeouts::renew`] while the operation runs,
    /// execute on the prepared instance with the freshest status, finish on every prepared one
    /// (aborted when the execute failed). Refused up front while this hub runs a reset of it
    /// already.
    pub async fn reset(
        self: &Arc<Self>,
        ctx: &ContextId,
        service: &str,
        processor: &str,
        target: Target,
    ) -> Result<ResetReport, Refusal> {
        let holders = self.holders(ctx, service, processor)?;
        // The hub offers only what the event store declares, as the freshest report
        // has it (the rule of the dashboard's aggregate): no instance is asked otherwise.
        let newest = freshest(&holders).ok_or(Refusal::UnknownProcessor)?;
        if !newest.replay {
            return Err(Refusal::Unsupported(newest.event_store.clone()));
        }
        let operation_id = new_id();
        let _running = self.begin(service, processor, &operation_id)?;
        info!(
            service = %clip(service),
            processor = %clip(processor),
            operation_id = %operation_id,
            instances = holders.len(),
            "reset: preparing"
        );
        let prepare = control_request::Action::ResetPrepare(proto::ResetPrepare {
            operation_id: operation_id.clone(),
            lease_ms: ms(self.timeouts.lease),
        });
        let prepared_at = tokio::time::Instant::now();
        let prepares = self
            .ask_all(&holders, processor, prepare.clone(), self.timeouts.request)
            .await;
        let mut report = ResetReport {
            service: service.to_string(),
            processor: processor.to_string(),
            action: "reset",
            operation_id: operation_id.clone(),
            target: target.to_json(),
            outcome: "aborted",
            executed_by: None,
            replay: false,
            ranges: Vec::new(),
            markers_deleted: 0,
            reset_handlers: 0,
            error: None,
            instances: holders
                .iter()
                .zip(&prepares)
                .map(|(h, a)| ResetInstance {
                    instance_id: h.instance_id.clone(),
                    prepare: a.view(),
                    finish: None,
                })
                .collect(),
            verdict: Verdict::Ok,
        };
        // An instance that does not run the processor is not involved; any other answer but ok
        // aborts. A refusal says the most, then a timeout, then anything else.
        let not_involved = |a: &Answer| a.refused_with("UNKNOWN_PROCESSOR");
        let failure = prepares
            .iter()
            .position(|a| matches!(a, Answer::Refused { .. }) && !not_involved(a))
            .or_else(|| prepares.iter().position(|a| *a == Answer::Timeout))
            .or_else(|| {
                prepares
                    .iter()
                    .position(|a| !matches!(a, Answer::Ok(_)) && !not_involved(a))
            });
        let involved: Vec<usize> = (0..holders.len())
            .filter(|&i| matches!(prepares[i], Answer::Ok(_)))
            .collect();
        let finish = |aborted: bool| {
            control_request::Action::ResetFinish(proto::ResetFinish {
                operation_id: operation_id.clone(),
                aborted,
            })
        };
        if failure.is_some() || involved.is_empty() {
            // Every instance that may hold the lock (a late prepare included) is released.
            let asked: Vec<usize> = (0..holders.len())
                .filter(|&i| !not_involved(&prepares[i]))
                .collect();
            self.finish_all(&holders, &asked, processor, finish(true), &mut report)
                .await;
            report.error = Some(match failure {
                Some(i) => StageError {
                    stage: "prepare",
                    instance_id: Some(holders[i].instance_id.clone()),
                    answer: prepares[i].view(),
                },
                None => StageError {
                    stage: "prepare",
                    instance_id: None,
                    answer: AnswerView {
                        result: "refused",
                        code: Some("UNKNOWN_PROCESSOR"),
                        message: "no instance runs the processor".to_string(),
                    },
                },
            });
            report.verdict = failure.map_or(Verdict::NotFound, |i| prepares[i].verdict());
            warn!(service = %clip(service), processor = %clip(processor), operation_id = %operation_id, "reset aborted: an instance was not prepared");
            return Ok(report);
        }
        let prepared: Vec<ProcessorHolder> = involved.iter().map(|&i| holders[i].clone()).collect();
        let renewing = self.renew(prepared.clone(), processor, prepare, prepared_at);
        let executor = freshest(&prepared)
            .expect("at least one instance is prepared")
            .clone();
        report.executed_by = Some(executor.instance_id.clone());
        let execute = control_request::Action::ResetExecute(proto::ResetExecute {
            operation_id: operation_id.clone(),
            target: Some(target.to_proto()),
        });
        let executed = self
            .ask(&executor, processor, execute, self.timeouts.reset)
            .await;
        // No renewal may be sent after a finish, nor be in flight while it is sent: a renewal
        // that reached an instance after its finish would find the lock free.
        let in_flight = renewing.stop();
        let _ = tokio::time::timeout(self.timeouts.request, async {
            for reply in in_flight {
                let _ = reply.await;
            }
        })
        .await;
        let aborted = !matches!(executed, Answer::Reset(_));
        self.finish_all(&holders, &involved, processor, finish(aborted), &mut report)
            .await;
        match executed {
            Answer::Reset(done) => {
                report.outcome = "ok";
                report.replay = !done.ranges.is_empty();
                report.ranges = done.ranges;
                report.markers_deleted = done.markers_deleted;
                report.reset_handlers = done.reset_handlers;
                info!(
                    service = %clip(service),
                    processor = %clip(processor),
                    operation_id = %operation_id,
                    executed_by = %executor.instance_id,
                    replay = report.replay,
                    "reset done"
                );
            }
            failed => {
                report.outcome = "failed";
                report.verdict = failed.verdict();
                let mut answer = failed.view();
                if matches!(failed, Answer::Timeout | Answer::Left) {
                    // Unlike a refusal, no answer does not mean nothing changed.
                    answer.message = format!(
                        "{}: the executor may still finish the reset, or may already have run reset handlers or deleted markers; check the processor before resetting again",
                        answer.message
                    );
                }
                report.error = Some(StageError {
                    stage: "execute",
                    instance_id: Some(executor.instance_id.clone()),
                    answer,
                });
                warn!(service = %clip(service), processor = %clip(processor), operation_id = %operation_id, "reset failed: the execute did not succeed");
            }
        }
        Ok(report)
    }

    /// Sends `finish` to the holders at `which` and records their answers in `report`.
    async fn finish_all(
        self: &Arc<Self>,
        holders: &[ProcessorHolder],
        which: &[usize],
        processor: &str,
        finish: control_request::Action,
        report: &mut ResetReport,
    ) {
        let targets: Vec<ProcessorHolder> = which.iter().map(|&i| holders[i].clone()).collect();
        let answers = self
            .ask_all(&targets, processor, finish, self.timeouts.request)
            .await;
        for (&i, answer) in which.iter().zip(&answers) {
            report.instances[i].finish = Some(answer.view());
            if !matches!(answer, Answer::Ok(_)) {
                warn!(instance = %holders[i].instance_id, answer = ?answer.view(), "reset: finish not acknowledged; the lease releases the instance");
            }
        }
    }

    /// Renews the lease of `prepared` every [`Timeouts::renew`] from `since` until the returned
    /// [`Renewing`] is stopped or dropped. The flag is checked under its lock while the renewals
    /// are sent and their reply waits recorded, so once it is set no renewal follows and the
    /// waits of those in flight are known (a renewal after the finish would take the lock again).
    fn renew(
        self: &Arc<Self>,
        prepared: Vec<ProcessorHolder>,
        processor: &str,
        prepare: control_request::Action,
        since: tokio::time::Instant,
    ) -> Renewing {
        let stopped = Arc::new(Mutex::new(Renewals::default()));
        let (this, flag, processor) = (
            Arc::clone(self),
            Arc::clone(&stopped),
            processor.to_string(),
        );
        let every = self.timeouts.renew;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(since + every, every);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let mut renewals = flag.lock().unwrap();
                if renewals.stopped {
                    return;
                }
                renewals.replies.retain(|reply| !reply.is_finished());
                for h in &prepared {
                    let (id, rx) = match this.send(h, &processor, prepare.clone()) {
                        Ok(sent) => sent,
                        Err(answer) => {
                            warn!(instance = %h.instance_id, answer = ?answer.view(), "reset: lease renewal not sent");
                            continue;
                        }
                    };
                    let (this, instance) = (Arc::clone(&this), h.instance_id.clone());
                    renewals.replies.push(tokio::spawn(async move {
                        let answer = this.wait(&id, rx, this.timeouts.request).await;
                        if !matches!(answer, Answer::Ok(_)) {
                            warn!(instance = %instance, answer = ?answer.view(), "reset: lease renewal not acknowledged");
                        }
                    }));
                }
            }
        });
        Renewing(stopped)
    }

    /// The audit line of one control call and its count in
    /// `snodra_server_control_total{action,outcome}`. `caller` is `write-token`, the ACL service
    /// name, or for a read the read credential (`dashboard-token`, `write-token`, `anonymous`).
    #[allow(clippy::too_many_arguments)] // the fields of the audit line
    pub fn audit(
        &self,
        caller: &str,
        service: &str,
        processor: &str,
        action: Action,
        outcome: &'static str,
        started: Instant,
        operation_id: Option<&str>,
    ) {
        self.metrics.control(action.as_str(), outcome);
        info!(
            caller = %clip(caller),
            service = %clip(service),
            processor = %clip(processor),
            action = action.as_str(),
            outcome,
            duration_ms = started.elapsed().as_millis() as u64,
            operation_id = operation_id.unwrap_or("-"),
            "control"
        );
    }

    /// Counts a control call refused before it was judged (no or a wrong credential, no right,
    /// controls disabled); such calls get no audit line, so a flood of them cannot fill the log.
    pub fn refused(&self, action: Action, outcome: &'static str) {
        self.metrics.control(action.as_str(), outcome);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{
        ControlRequest, InstanceStatus, ProcessorState, ProcessorStatus, server_message,
    };
    use crate::session::SessionMeta;
    use crate::telemetry::test_support::{TestMetrics, capture_logs, logged, test_metrics};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    static D: std::sync::LazyLock<ContextId> = std::sync::LazyLock::new(ContextId::default);

    fn timeouts() -> Timeouts {
        Timeouts {
            request: Duration::from_millis(300),
            reset: Duration::from_millis(600),
            lease: Duration::from_secs(30),
            renew: Duration::from_secs(10),
        }
    }

    fn parts() -> (Arc<Sessions>, Arc<Control>, TestMetrics) {
        let m = test_metrics();
        let sessions = Arc::new(Sessions::new());
        let control = Control::new(Arc::clone(&sessions), m.metrics.clone(), timeouts());
        (sessions, control, m)
    }

    type Rx = mpsc::Receiver<Result<ServerMessage, tonic::Status>>;

    /// A session of `svc` that reports `audit`, on an event store that declares `replay`.
    fn session(sessions: &Sessions, id: &str) -> (u64, Rx) {
        let (tx, rx) = mpsc::channel(16);
        let meta = SessionMeta::default();
        let token = sessions.insert(&D, id, "svc", meta, tx, CancellationToken::new());
        let report = InstanceStatus {
            connected_hub_ids: vec!["h".into()],
            handlers_in_flight: 0,
            processors: vec![ProcessorStatus {
                name: "audit".into(),
                state: ProcessorState::ProcessorRunning as i32,
                event_store: "kafka".into(),
                capabilities: vec!["replay".into()],
                ..Default::default()
            }],
        };
        assert!(sessions.set_status(&D, id, token, &report, Instant::now()));
        (token, rx)
    }

    async fn request(rx: &mut Rx) -> ControlRequest {
        match rx.recv().await {
            Some(Ok(ServerMessage {
                message: Some(server_message::Message::Control(c)),
            })) => c,
            other => panic!("expected a control request, got {other:?}"),
        }
    }

    fn ok(id: &str) -> ControlReply {
        ControlReply {
            control_id: id.to_string(),
            result: Some(control_reply::Result::Ok(proto::ControlOk {
                detail: "done".into(),
            })),
        }
    }

    fn holder(sessions: &Sessions, id: &str) -> ProcessorHolder {
        sessions
            .processor_holders(&D, "svc", "audit")
            .unwrap()
            .into_iter()
            .find(|h| h.instance_id == id)
            .unwrap()
    }

    fn pause() -> control_request::Action {
        control_request::Action::Pause(proto::ProcessorPause {})
    }

    #[tokio::test]
    async fn a_reply_reaches_the_request_it_answers_and_nothing_else() {
        let (sessions, control, m) = parts();
        let (token, mut rx) = session(&sessions, "i1");
        let h = holder(&sessions, "i1");
        let ask = {
            let control = Arc::clone(&control);
            tokio::spawn(async move {
                control
                    .ask(&h, "audit", pause(), Duration::from_secs(2))
                    .await
            })
        };
        let req = request(&mut rx).await;
        assert_eq!(req.processor, "audit");
        assert!(matches!(
            req.action,
            Some(control_request::Action::Pause(_))
        ));
        // The same id from another session of the instance (a reconnect), or an id nobody sent,
        // is not the answer: dropped and counted.
        control.on_reply(&D, "i1", token + 99, ok(&req.control_id));
        control.on_reply(&D, "other", token, ok(&req.control_id));
        control.on_reply(&D, "i1", token, ok("made-up"));
        assert_eq!(
            m.counter_total("snodra_server_control_unknown_replies_total"),
            Some(3)
        );
        assert_eq!(control.in_flight(), 1);
        control.on_reply(&D, "i1", token, ok(&req.control_id));
        assert_eq!(ask.await.unwrap(), Answer::Ok("done".into()));
        assert_eq!(control.in_flight(), 0);
        // A second reply with that id is unknown now.
        control.on_reply(&D, "i1", token, ok(&req.control_id));
        assert_eq!(
            m.counter_total("snodra_server_control_unknown_replies_total"),
            Some(4)
        );
    }

    #[tokio::test]
    async fn a_request_times_out_and_its_late_reply_is_unknown() {
        let (sessions, control, m) = parts();
        let (token, mut rx) = session(&sessions, "i1");
        let h = holder(&sessions, "i1");
        let started = Instant::now();
        let answer = control
            .ask(&h, "audit", pause(), Duration::from_millis(150))
            .await;
        assert_eq!(answer, Answer::Timeout);
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert_eq!(answer.verdict(), Verdict::Timeout);
        assert_eq!(control.in_flight(), 0, "the waiter is gone");
        let req = request(&mut rx).await;
        control.on_reply(&D, "i1", token, ok(&req.control_id));
        assert_eq!(
            m.counter_total("snodra_server_control_unknown_replies_total"),
            Some(1)
        );
    }

    #[tokio::test]
    async fn a_session_that_leaves_fails_its_pending_requests_at_once() {
        let (sessions, control, _m) = parts();
        let (token, _rx) = session(&sessions, "i1");
        let (_other_token, _rx2) = session(&sessions, "i2");
        let h1 = holder(&sessions, "i1");
        let h2 = holder(&sessions, "i2");
        let (c1, c2) = (Arc::clone(&control), Arc::clone(&control));
        let a1 =
            tokio::spawn(
                async move { c1.ask(&h1, "audit", pause(), Duration::from_secs(5)).await },
            );
        let a2 = tokio::spawn(async move {
            c2.ask(&h2, "audit", pause(), Duration::from_millis(300))
                .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(control.in_flight(), 2);
        let started = Instant::now();
        // An older session of the instance leaving changes nothing.
        control.on_disconnect(&D, "i1", token + 1);
        assert_eq!(control.in_flight(), 2);
        control.on_disconnect(&D, "i1", token);
        let answer = a1.await.unwrap();
        assert_eq!(answer, Answer::Left);
        assert_eq!(answer.view().message, "instance left");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "at once, not at the timeout"
        );
        // The other instance's request is untouched (it times out on its own).
        assert_eq!(a2.await.unwrap(), Answer::Timeout);
    }

    #[tokio::test]
    async fn a_request_to_a_gone_session_is_unsent() {
        let (sessions, control, _m) = parts();
        let (token, rx) = session(&sessions, "i1");
        let h = holder(&sessions, "i1");
        sessions.remove_if(&D, "i1", token);
        drop(rx);
        let answer = control
            .ask(&h, "audit", pause(), Duration::from_secs(1))
            .await;
        assert_eq!(answer, Answer::Unsent("instance is gone"));
        assert_eq!(answer.verdict(), Verdict::Failed);
        assert_eq!(control.in_flight(), 0);
    }

    #[test]
    fn a_reply_of_another_kind_is_unexpected() {
        let reset = control_request::Action::ResetExecute(proto::ResetExecute::default());
        assert_eq!(
            Answer::Ok("x".into()).expected_by(&reset),
            Answer::Unexpected
        );
        assert_eq!(
            Answer::Letters(LetterPage {
                total: 0,
                letters: vec![]
            })
            .expected_by(&pause()),
            Answer::Unexpected
        );
        assert_eq!(Answer::Timeout.expected_by(&reset), Answer::Timeout);
        assert_eq!(Answer::from_reply(None), Answer::Unexpected);
        let refused = Answer::Refused {
            code: "LOCKED",
            message: String::new(),
        };
        assert_eq!(refused.clone().expected_by(&reset), refused);
    }

    #[test]
    fn replies_are_bounded_as_a_status_is() {
        use control_reply::Result as R;
        let long = format!("{}é", "x".repeat(MAX_TEXT_BYTES - 1));
        let refused = Answer::from_reply(Some(R::Refused(proto::ControlRefused {
            code: 42,
            message: long.clone(),
        })));
        let Answer::Refused { code, message } = refused else {
            panic!()
        };
        assert_eq!(code, "UNSPECIFIED", "an unknown code");
        for (code, name) in [
            (ControlRefusal::SubscribersActive, "SUBSCRIBERS_ACTIVE"),
            (ControlRefusal::Unsupported, "UNSUPPORTED"),
        ] {
            let answer = Answer::from_reply(Some(R::Refused(proto::ControlRefused {
                code: code as i32,
                message: String::new(),
            })));
            assert!(answer.refused_with(name), "{answer:?}");
        }
        assert_eq!(message, "x".repeat(MAX_TEXT_BYTES - 1), "cut before the é");

        let letter = proto::DeadLetterSummary {
            id: "i".repeat(300),
            error_message: "e".repeat(2000),
            event_type: "t".repeat(1000),
            reason: "HANDLER_FAILED".into(),
            segment: "s".repeat(MAX_SEGMENT_BYTES + 1),
            position: "9".repeat(MAX_SEGMENT_BYTES + 1),
            ..Default::default()
        };
        let page = Answer::from_reply(Some(R::DeadLetters(proto::DeadLetterPage {
            letters: vec![letter; 250],
            total: 250,
        })));
        let Answer::Letters(page) = page else {
            panic!()
        };
        assert_eq!(page.total, 250);
        assert_eq!(page.letters.len(), MAX_LETTERS as usize);
        let l = &page.letters[0];
        assert_eq!(l.id.len(), MAX_ID_BYTES);
        assert_eq!(l.error_message.len(), MAX_TEXT_BYTES);
        assert_eq!(l.event_type.len(), MAX_TYPE_BYTES);
        assert_eq!(l.reason, "HANDLER_FAILED");
        assert_eq!(
            (l.segment.as_str(), l.position.as_str()),
            ("", ""),
            "a too long segment or position is never cut: it reads as unknown"
        );

        let mut ranges: Vec<proto::ReplayRange> = (0..2000)
            .rev()
            .map(|p| proto::ReplayRange {
                segment: p.to_string(),
                from: "0".into(),
                until: if p == 1999 {
                    "9".repeat(MAX_SEGMENT_BYTES + 1)
                } else {
                    "1".into()
                },
            })
            .collect();
        // A range of a segment named longer than the hub keeps is dropped, never cut, and does
        // not count towards the cap.
        ranges.insert(
            0,
            proto::ReplayRange {
                segment: "1".repeat(MAX_SEGMENT_BYTES + 1),
                from: "0".into(),
                until: "1".into(),
            },
        );
        let Answer::Reset(r) = Answer::from_reply(Some(R::Reset(proto::ResetOutcome {
            ranges,
            markers_deleted: 7,
            reset_handlers: 1,
        }))) else {
            panic!()
        };
        assert_eq!(r.ranges.len(), MAX_SEGMENTS);
        assert!(
            r.ranges
                .windows(2)
                .all(|w| segment_order(&w[0].segment, &w[1].segment).is_lt()),
            "sorted by value"
        );
        assert_eq!(
            (
                r.ranges[0].segment.as_str(),
                r.ranges[1023].segment.as_str()
            ),
            ("976", "1999")
        );
        assert_eq!(
            r.ranges[1023].until, "",
            "a too long position reads as none"
        );

        let Answer::Retry(retry) =
            Answer::from_reply(Some(R::Retry(proto::DeadLetterRetryOutcome {
                result: "R".repeat(100),
                detail: "d".repeat(900),
            })))
        else {
            panic!()
        };
        assert_eq!(
            (retry.result.len(), retry.detail.len()),
            (MAX_WORD_BYTES, MAX_TEXT_BYTES)
        );
    }

    #[test]
    fn verdicts_follow_the_answer() {
        let refused = |code| Answer::Refused {
            code,
            message: String::new(),
        };
        for (answer, want) in [
            (Answer::Ok(String::new()), Verdict::Ok),
            (refused("UNKNOWN_PROCESSOR"), Verdict::NotFound),
            (refused("NOT_FOUND"), Verdict::NotFound),
            (refused("LOCKED"), Verdict::Conflict),
            (refused("NOT_PREPARED"), Verdict::Conflict),
            (refused("SUBSCRIBERS_ACTIVE"), Verdict::Conflict),
            (refused("UNSUPPORTED"), Verdict::Conflict),
            (refused("RESET_HANDLER_FAILED"), Verdict::Conflict),
            (refused("INVALID"), Verdict::BadRequest),
            (refused("FAILED"), Verdict::Failed),
            (refused("UNSPECIFIED"), Verdict::Failed),
            (Answer::Timeout, Verdict::Timeout),
            (Answer::Left, Verdict::Failed),
            (Answer::Unexpected, Verdict::Failed),
        ] {
            assert_eq!(answer.verdict(), want, "{answer:?}");
        }
        let retry = |result: &str| {
            Answer::Retry(RetryView {
                result: result.into(),
                detail: String::new(),
            })
        };
        for (result, want) in [
            ("Retried", Verdict::Ok),
            ("Duplicate", Verdict::Ok),
            ("Failed", Verdict::Ok),
            ("Blocked", Verdict::Conflict),
            ("NotRetriable", Verdict::Conflict),
            ("NotFound", Verdict::NotFound),
            ("Exploded", Verdict::Failed),
        ] {
            assert_eq!(retry(result).verdict(), want, "{result}");
        }
    }

    #[test]
    fn reset_targets_parse_from_the_documented_bodies_only() {
        assert_eq!(Target::parse(br#"{"to":"earliest"}"#), Ok(Target::Earliest));
        assert_eq!(Target::parse(br#"{"to":"latest"}"#), Ok(Target::Latest));
        assert_eq!(
            Target::parse(br#"{"to":{"timestampMs":1762250000000}}"#),
            Ok(Target::Timestamp(1_762_250_000_000))
        );
        assert_eq!(
            Target::parse(
                br#"{"to":{"positions":[{"segment":"10","position":"9"},{"segment":"2","position":"12"}]}}"#
            ),
            Ok(Target::Positions(vec![
                ("2".into(), "12".into()),
                ("10".into(), "9".into())
            ]))
        );
        for bad in [
            &b"not json"[..],
            br#"[]"#,
            br#"{}"#,
            br#"{"to":"yesterday"}"#,
            br#"{"to":"earliest","extra":1}"#,
            br#"{"to":{"timestampMs":-1}}"#,
            br#"{"to":{"timestampMs":"soon"}}"#,
            br#"{"to":{"timestampMs":1,"positions":[]}}"#,
            br#"{"to":{"positions":[]}}"#,
            br#"{"to":{"positions":[{"segment":"","position":"0"}]}}"#,
            br#"{"to":{"positions":[{"segment":"0","position":""}]}}"#,
            br#"{"to":{"positions":[{"segment":0,"position":"1"}]}}"#,
            br#"{"to":{"positions":[{"segment":"0","position":12}]}}"#,
            br#"{"to":{"positions":[{"segment":"0"}]}}"#,
            br#"{"to":{"positions":[{"segment":"0","position":"1","x":2}]}}"#,
            br#"{"to":{"positions":[{"segment":"0","position":"1"},{"segment":"0","position":"2"}]}}"#,
            br#"{"to":{"positions":[{"segment":"0\n","position":"1"}]}}"#,
            br#"{"to":{"offsets":[{"partition":0,"offset":1}]}}"#,
        ] {
            assert!(
                Target::parse(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
        let long = "9".repeat(MAX_SEGMENT_BYTES + 1);
        for (segment, position) in [(long.as_str(), "1"), ("0", long.as_str())] {
            let body = serde_json::json!({"to": {"positions": [{"segment": segment, "position": position}]}});
            assert!(
                Target::parse(body.to_string().as_bytes()).is_err(),
                "{body}"
            );
        }
        let many: Vec<String> = (0..=MAX_POSITIONS)
            .map(|p| format!("{{\"segment\":\"{p}\",\"position\":\"0\"}}"))
            .collect();
        let body = format!("{{\"to\":{{\"positions\":[{}]}}}}", many.join(","));
        assert!(
            Target::parse(body.as_bytes())
                .unwrap_err()
                .contains("1 to 1024")
        );
        let body = format!(
            "{{\"to\":{{\"positions\":[{}]}}}}",
            many[..MAX_POSITIONS].join(",")
        );
        assert!(Target::parse(body.as_bytes()).is_ok());
        // The body echoes back as it was given.
        for body in [
            r#"{"to":"earliest"}"#,
            r#"{"to":{"timestampMs":5}}"#,
            r#"{"to":{"positions":[{"segment":"0","position":"12"}]}}"#,
        ] {
            let target = Target::parse(body.as_bytes()).unwrap();
            assert_eq!(
                target.to_json(),
                serde_json::from_str::<serde_json::Value>(body).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn the_freshest_instance_answers_a_dead_letter_call() {
        let (sessions, control, _m) = parts();
        let (_t1, mut rx1) = session(&sessions, "a-old");
        tokio::time::sleep(Duration::from_millis(5)).await;
        let (t2, mut rx2) = session(&sessions, "b-fresh");
        let call = {
            let control = Arc::clone(&control);
            tokio::spawn(async move {
                control
                    .dead_letters(&D, "svc", "audit", &LetterCall::List(500))
                    .await
            })
        };
        let req = request(&mut rx2).await;
        let Some(control_request::Action::DeadLetterList(list)) = &req.action else {
            panic!("{req:?}")
        };
        assert_eq!(list.limit, MAX_LETTERS, "the hub clamps the limit");
        control.on_reply(
            &D,
            "b-fresh",
            t2,
            ControlReply {
                control_id: req.control_id,
                result: Some(control_reply::Result::DeadLetters(proto::DeadLetterPage {
                    letters: vec![],
                    total: 0,
                })),
            },
        );
        let outcome = call.await.unwrap().unwrap();
        assert_eq!(outcome.instance_id, "b-fresh");
        assert!(rx1.try_recv().is_err(), "nobody else was asked");
        assert_eq!(
            Control::new(
                Arc::new(Sessions::new()),
                test_metrics().metrics,
                timeouts()
            )
            .dead_letters(&D, "svc", "audit", &LetterCall::RetryAll)
            .await,
            Err(Refusal::UnknownService)
        );
        assert_eq!(
            control
                .dead_letters(&D, "svc", "orders", &LetterCall::RetryAll)
                .await,
            Err(Refusal::UnknownProcessor)
        );
    }

    #[tokio::test]
    async fn a_second_reset_of_the_same_processor_is_refused_while_one_runs() {
        let (sessions, control, _m) = parts();
        let _s = session(&sessions, "i1");
        let running = control.begin("svc", "audit", "op-1").unwrap();
        assert_eq!(
            control.reset(&D, "svc", "audit", Target::Latest).await,
            Err(Refusal::Running("op-1".into()))
        );
        // Another processor is not held.
        assert!(control.begin("svc", "orders", "op-2").is_ok());
        drop(running);
        assert!(
            control.begin("svc", "audit", "op-3").is_ok(),
            "released when it ends"
        );
    }

    /// A reset that ends without reaching its finish (its caller went away, a panic) stops
    /// renewing the lease: the instances are released when it runs out (H-4).
    #[tokio::test]
    async fn a_reset_dropped_mid_way_renews_the_lease_no_more() {
        let m = test_metrics();
        let sessions = Arc::new(Sessions::new());
        let control = Control::new(
            Arc::clone(&sessions),
            m.metrics.clone(),
            Timeouts {
                request: Duration::from_millis(300),
                reset: Duration::from_secs(5),
                lease: Duration::from_millis(500),
                renew: Duration::from_millis(50),
            },
        );
        let (token, mut rx) = session(&sessions, "i1");
        let reset = {
            let control = Arc::clone(&control);
            tokio::spawn(async move { control.reset(&D, "svc", "audit", Target::Latest).await })
        };
        let prepare = request(&mut rx).await;
        assert!(matches!(
            prepare.action,
            Some(control_request::Action::ResetPrepare(_))
        ));
        control.on_reply(&D, "i1", token, ok(&prepare.control_id));
        let execute = request(&mut rx).await;
        assert!(matches!(
            execute.action,
            Some(control_request::Action::ResetExecute(_))
        ));
        // A renewal goes out while the execute runs.
        let renewal = request(&mut rx).await;
        assert!(matches!(
            renewal.action,
            Some(control_request::Action::ResetPrepare(_))
        ));
        reset.abort();
        assert!(reset.await.unwrap_err().is_cancelled());
        while rx.try_recv().is_ok() {}
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(rx.try_recv().is_err(), "a renewal after the reset ended");
    }

    #[tokio::test]
    async fn the_audit_line_names_the_caller_the_call_and_its_outcome() {
        capture_logs();
        let (_sessions, control, m) = parts();
        control.audit(
            "write-token",
            "service-b",
            "audit",
            Action::Reset,
            Verdict::Conflict.as_str(),
            Instant::now(),
            Some("op-1"),
        );
        control.refused(Action::Pause, "unauthenticated");
        let lines: Vec<_> = logged()
            .into_iter()
            .filter(|l| l.message == "control")
            .collect();
        assert_eq!(lines.len(), 1, "a refused call has no line");
        let line = &lines[0];
        for (field, value) in [
            ("caller", "write-token"),
            ("service", "service-b"),
            ("processor", "audit"),
            ("action", "reset"),
            ("outcome", "conflict"),
            ("operation_id", "op-1"),
        ] {
            assert_eq!(line.field(field), Some(value), "{field}");
        }
        assert!(line.field("duration_ms").is_some());
        assert_eq!(
            m.counter("snodra_server_control_total", "action", "reset"),
            Some(1)
        );
        assert_eq!(
            m.counter("snodra_server_control_total", "outcome", "unauthenticated"),
            Some(1)
        );
    }
}
