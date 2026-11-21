//! The topology snapshot: one consistent-enough view of services, instances, type ownership and
//! in-flight requests, shared by the gRPC handler, the HTTP endpoint and the CLI.
//!
//! The snapshot reads sessions, then registry, then router, each under its own lock exactly once
//! (never nested, never across an await). Never contains payloads or headers.

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::processors::ProcessorReport;
use crate::proto;
use crate::registry::{DEFAULT_LOAD_FACTOR, Kind, Registry};
use crate::router::Router;
use crate::session::Sessions;

/// A `(Instant, SystemTime)` pair taken once at startup; every wall-clock value in a snapshot is
/// derived from it, so they stay mutually consistent under wall-clock jumps.
#[derive(Debug, Clone, Copy)]
pub struct ClockAnchor {
    pub instant: Instant,
    pub system: SystemTime,
}

impl ClockAnchor {
    pub fn now() -> Self {
        Self {
            instant: Instant::now(),
            system: SystemTime::now(),
        }
    }

    /// Epoch milliseconds of the monotonic instant `at` (before the anchor: anchor minus the
    /// difference).
    pub fn to_epoch_ms(&self, at: Instant) -> i64 {
        let base = self
            .system
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        if at >= self.instant {
            base.saturating_add((at - self.instant).as_millis() as i64)
        } else {
            base.saturating_sub((self.instant - at).as_millis() as i64)
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone, Copy)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum KindName {
    Command,
    Query,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Topology {
    pub hub_id: String,
    pub hub_started_at_ms: i64,
    pub now_ms: i64,
    pub heartbeat_timeout_ms: i64,
    pub services: Vec<ServiceTopology>,
    pub types: Vec<TypeOwnership>,
    pub totals: Totals,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServiceTopology {
    pub service_name: String,
    pub instances: Vec<InstanceTopology>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InstanceTopology {
    pub instance_id: String,
    pub connected_since_ms: i64,
    pub last_heartbeat_age_ms: i64,
    pub stale: bool,
    pub in_flight_as_handler: u32,
    pub in_flight_as_sender: u32,
    pub command_types: Vec<String>,
    pub query_types: Vec<String>,
    /// `Register.client_version`, empty when the client sent none.
    pub client_version: String,
    /// Permits left.
    pub permits_available: u32,
    /// Requests waiting in the hub's queue for this instance.
    pub queued: u32,
    /// As applied (0 in `Register` reads as 100).
    pub load_factor: u32,
    pub tags: BTreeMap<String, String>,
    /// `InstanceStatus.connected_hub_ids` as last reported; empty when never reported.
    pub connected_hub_ids: Vec<String>,
    /// Milliseconds since the last `InstanceStatus`; -1 when never reported.
    pub status_age_ms: i64,
    /// The query types (of `query_types`) registered as shared.
    pub shared_query_types: Vec<String>,
    /// How the session authenticated: `authenticated` or `unauthenticated
    /// (permissive)`. Absent when access control is off. JSON only: the gRPC response has no
    /// field for it, so a topology read over gRPC never carries it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,
    /// The service the instance's token is granted in the ACL, when it authenticated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authenticated_service: Option<String>,
    /// How the credentials were presented: `token`, `jwt` or `none`. Absent when
    /// access control is off; JSON only, like `auth`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_kind: Option<String>,
    /// When a JWT session's token expires (`exp`), milliseconds since the epoch; absent for a
    /// token or no credentials. Never any token material.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    /// The first 8 hex characters of the SHA-256 of a certificate session's client certificate
    ///; absent otherwise. Never the certificate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cert_fingerprint: Option<String>,
    /// `InstanceStatus.processors` of the latest report, bounded; empty when never
    /// reported.
    pub processors: Vec<ProcessorReport>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TypeOwnership {
    #[serde(rename = "type")]
    pub type_name: String,
    pub kind: KindName,
    pub live_instances: u32,
    pub orphaned: bool,
    pub orphaned_for_ms: i64,
    /// A shared query type: many services may own it.
    pub shared: bool,
    /// Every owning service, sorted (one for an exclusive type).
    pub owner_services: Vec<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Totals {
    pub services: u32,
    pub instances: u32,
    pub command_types: u32,
    pub query_types: u32,
    pub in_flight: u32,
    /// Requests waiting in per-instance queues (counted in `in_flight` too).
    pub queued: u32,
}

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// Builds the snapshot. Sessions are the base set; registry types and in-flight counts are
/// attached by instance id (empty / zero when absent).
#[allow(clippy::too_many_arguments)] // plain inputs of one snapshot; a struct would only rename them
pub fn snapshot(
    registry: &Registry,
    sessions: &Sessions,
    router: &Router,
    anchor: ClockAnchor,
    started: Instant,
    hub_id: &str,
    heartbeat_timeout: Duration,
    now: Instant,
) -> Topology {
    let details = sessions.details();
    let reg = registry.topology(now);
    let (in_flight, in_flight_total) = router.in_flight_by_instance();
    let flows = router.flow_by_instance();

    let mut by_service: BTreeMap<String, Vec<InstanceTopology>> = BTreeMap::new();
    for d in details {
        let age = now.saturating_duration_since(d.last_heartbeat);
        let (commands, queries, shared_queries, load_factor, tags) = reg
            .instances
            .get(&d.instance_id)
            .map(|i| {
                (
                    i.command_types.clone(),
                    i.query_types.clone(),
                    i.shared_query_types.clone(),
                    i.load_factor,
                    i.tags.clone(),
                )
            })
            .unwrap_or_else(|| (vec![], vec![], vec![], DEFAULT_LOAD_FACTOR, BTreeMap::new()));
        let flow = flows.get(&d.instance_id).copied().unwrap_or_default();
        let (connected_hub_ids, status_age_ms, processors) = match d.status {
            Some(st) => (
                st.connected_hub_ids,
                ms(now.saturating_duration_since(st.at)),
                st.processors,
            ),
            None => (vec![], -1, vec![]),
        };
        let flight = in_flight.get(&d.instance_id).copied().unwrap_or_default();
        by_service
            .entry(d.service)
            .or_default()
            .push(InstanceTopology {
                connected_since_ms: anchor.to_epoch_ms(d.connected_since),
                last_heartbeat_age_ms: ms(age),
                stale: age > heartbeat_timeout / 3,
                in_flight_as_handler: flight.as_handler,
                in_flight_as_sender: flight.as_sender,
                command_types: commands,
                query_types: queries,
                client_version: d.client_version,
                permits_available: flow.permits_available,
                queued: flow.queued,
                load_factor,
                tags,
                connected_hub_ids,
                status_age_ms,
                shared_query_types: shared_queries,
                auth: d.auth.label().map(str::to_string),
                authenticated_service: d.auth.service().map(str::to_string),
                auth_kind: d.auth.label().map(|_| {
                    d.principal
                        .as_deref()
                        .map_or("none", |p| p.kind().as_str())
                        .to_string()
                }),
                expires_at: d
                    .principal
                    .as_deref()
                    .and_then(|p| p.expires_at)
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX)),
                cert_fingerprint: d.principal.as_deref().and_then(|p| p.fingerprint.clone()),
                processors,
                instance_id: d.instance_id,
            });
    }
    let services: Vec<ServiceTopology> = by_service
        .into_iter()
        .map(|(service_name, mut instances)| {
            instances.sort_by(|a, b| a.instance_id.cmp(&b.instance_id));
            ServiceTopology {
                service_name,
                instances,
            }
        })
        .collect();

    let types: Vec<TypeOwnership> = reg
        .types
        .iter()
        .map(|t| TypeOwnership {
            type_name: t.type_name.clone(),
            kind: match t.kind {
                Kind::Command => KindName::Command,
                Kind::Query => KindName::Query,
            },
            live_instances: t.live_instances,
            orphaned: t.orphaned_for.is_some(),
            orphaned_for_ms: t.orphaned_for.map_or(0, ms),
            shared: t.shared,
            owner_services: t.owner_services.clone(),
        })
        .collect();

    let distinct = |kind: KindName| {
        types
            .iter()
            .filter(|t| t.kind == kind)
            .map(|t| t.type_name.as_str())
            .collect::<HashSet<_>>()
            .len() as u32
    };
    let totals = Totals {
        services: services.len() as u32,
        instances: services.iter().map(|s| s.instances.len() as u32).sum(),
        command_types: distinct(KindName::Command),
        query_types: distinct(KindName::Query),
        in_flight: in_flight_total,
        queued: flows.values().map(|f| f.queued).sum(),
    };

    Topology {
        hub_id: hub_id.to_string(),
        hub_started_at_ms: anchor.to_epoch_ms(started),
        now_ms: anchor.to_epoch_ms(now),
        heartbeat_timeout_ms: ms(heartbeat_timeout),
        services,
        types,
        totals,
    }
}

impl Topology {
    pub fn to_proto(&self) -> proto::TopologyResponse {
        proto::TopologyResponse {
            hub_started_at_ms: self.hub_started_at_ms,
            now_ms: self.now_ms,
            heartbeat_timeout_ms: self.heartbeat_timeout_ms,
            services: self
                .services
                .iter()
                .map(|s| proto::ServiceTopology {
                    service_name: s.service_name.clone(),
                    instances: s
                        .instances
                        .iter()
                        .map(|i| proto::InstanceTopology {
                            instance_id: i.instance_id.clone(),
                            connected_since_ms: i.connected_since_ms,
                            last_heartbeat_age_ms: i.last_heartbeat_age_ms,
                            stale: i.stale,
                            in_flight_as_handler: i.in_flight_as_handler,
                            in_flight_as_sender: i.in_flight_as_sender,
                            command_types: i.command_types.clone(),
                            query_types: i.query_types.clone(),
                            client_version: i.client_version.clone(),
                            permits_available: i.permits_available,
                            queued: i.queued,
                            load_factor: i.load_factor,
                            tags: i.tags.clone().into_iter().collect(),
                            connected_hub_ids: i.connected_hub_ids.clone(),
                            status_age_ms: i.status_age_ms,
                            shared_query_types: i.shared_query_types.clone(),
                            processors: i.processors.iter().map(|p| p.to_proto()).collect(),
                        })
                        .collect(),
                })
                .collect(),
            types: self
                .types
                .iter()
                .map(|t| proto::TypeOwnership {
                    r#type: t.type_name.clone(),
                    kind: match t.kind {
                        KindName::Command => proto::RequestKind::Command,
                        KindName::Query => proto::RequestKind::Query,
                    } as i32,
                    live_instances: t.live_instances,
                    orphaned: t.orphaned,
                    orphaned_for_ms: t.orphaned_for_ms,
                    shared: t.shared,
                    owner_services: t.owner_services.clone(),
                })
                .collect(),
            totals: Some(proto::TopologyTotals {
                services: self.totals.services,
                instances: self.totals.instances,
                command_types: self.totals.command_types,
                query_types: self.totals.query_types,
                in_flight: self.totals.in_flight,
                queued: self.totals.queued,
            }),
            hub_id: self.hub_id.clone(),
        }
    }

    /// Inverse of [`Topology::to_proto`]. An unspecified kind (never produced by the hub) reads
    /// as `Command`; a missing totals message reads as zeros.
    pub fn from_proto(p: &proto::TopologyResponse) -> Topology {
        let totals = p.totals.unwrap_or_default();
        Topology {
            hub_id: p.hub_id.clone(),
            hub_started_at_ms: p.hub_started_at_ms,
            now_ms: p.now_ms,
            heartbeat_timeout_ms: p.heartbeat_timeout_ms,
            services: p
                .services
                .iter()
                .map(|s| ServiceTopology {
                    service_name: s.service_name.clone(),
                    instances: s
                        .instances
                        .iter()
                        .map(|i| InstanceTopology {
                            instance_id: i.instance_id.clone(),
                            connected_since_ms: i.connected_since_ms,
                            last_heartbeat_age_ms: i.last_heartbeat_age_ms,
                            stale: i.stale,
                            in_flight_as_handler: i.in_flight_as_handler,
                            in_flight_as_sender: i.in_flight_as_sender,
                            command_types: i.command_types.clone(),
                            query_types: i.query_types.clone(),
                            client_version: i.client_version.clone(),
                            permits_available: i.permits_available,
                            queued: i.queued,
                            load_factor: i.load_factor,
                            tags: i.tags.clone().into_iter().collect(),
                            connected_hub_ids: i.connected_hub_ids.clone(),
                            status_age_ms: i.status_age_ms,
                            shared_query_types: i.shared_query_types.clone(),
                            auth: None,
                            authenticated_service: None,
                            auth_kind: None,
                            expires_at: None,
                            cert_fingerprint: None,
                            // Data from another hub is bounded like a report.
                            processors: crate::processors::bound(&i.processors),
                        })
                        .collect(),
                })
                .collect(),
            types: p
                .types
                .iter()
                .map(|t| TypeOwnership {
                    type_name: t.r#type.clone(),
                    kind: match t.kind() {
                        proto::RequestKind::Query => KindName::Query,
                        proto::RequestKind::Command => KindName::Command,
                        proto::RequestKind::Unspecified => {
                            debug!(type_name = %t.r#type, "unspecified kind in topology; reading as COMMAND");
                            KindName::Command
                        }
                    },
                    live_instances: t.live_instances,
                    orphaned: t.orphaned,
                    orphaned_for_ms: t.orphaned_for_ms,
                    shared: t.shared,
                    owner_services: t.owner_services.clone(),
                })
                .collect(),
            totals: Totals {
                services: totals.services,
                instances: totals.instances,
                command_types: totals.command_types,
                query_types: totals.query_types,
                in_flight: totals.in_flight,
                queued: totals.queued,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ContextId;
    use crate::session::SessionMeta;

    static D: std::sync::LazyLock<ContextId> = std::sync::LazyLock::new(ContextId::default);
    use crate::config::Config;
    use crate::telemetry::Metrics;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    struct Fx {
        registry: Arc<Registry>,
        sessions: Arc<Sessions>,
        router: Arc<Router>,
        anchor: ClockAnchor,
    }

    fn fx() -> Fx {
        let config = Arc::new(Config::default());
        let registry = Arc::new(Registry::new(config.owner_grace));
        let sessions = Arc::new(Sessions::new());
        let router = Router::new(
            Arc::clone(&registry),
            Arc::clone(&sessions),
            config,
            Metrics::noop(),
        );
        Fx {
            registry,
            sessions,
            router,
            anchor: ClockAnchor::now(),
        }
    }

    fn connect(f: &Fx, id: &str, service: &str) {
        connect_with(f, id, service, SessionMeta::default());
    }

    fn connect_with(f: &Fx, id: &str, service: &str, meta: SessionMeta) {
        let (tx, rx) = mpsc::channel(4);
        std::mem::forget(rx);
        f.sessions
            .insert(&D, id, service, meta, tx, CancellationToken::new());
    }

    fn snap(f: &Fx, started: Instant, timeout: Duration, now: Instant) -> Topology {
        snapshot(
            &f.registry,
            &f.sessions,
            &f.router,
            f.anchor,
            started,
            "hub-x",
            timeout,
            now,
        )
    }

    #[tokio::test]
    async fn snapshot_reconciles_session_without_registry_entry_as_empty_lists() {
        let f = fx();
        connect(&f, "i1", "svc");
        let now = Instant::now();
        let t = snap(&f, f.anchor.instant, Duration::from_secs(30), now);
        assert_eq!(t.services.len(), 1);
        let inst = &t.services[0].instances[0];
        assert_eq!(inst.instance_id, "i1");
        assert!(inst.command_types.is_empty() && inst.query_types.is_empty());
        assert_eq!(
            (inst.in_flight_as_handler, inst.in_flight_as_sender),
            (0, 0)
        );
        assert_eq!(t.totals.services, 1);
        assert_eq!(t.totals.instances, 1);
        assert_eq!(t.totals.in_flight, 0);
        assert!(t.types.is_empty());
    }

    #[tokio::test]
    async fn snapshot_marks_stale_when_heartbeat_older_than_a_third_of_timeout() {
        let f = fx();
        connect(&f, "fresh", "svc");
        connect(&f, "old", "svc");
        let base = Instant::now();
        f.sessions.touch(&D, "fresh", base + Duration::from_secs(9));
        f.sessions.touch(&D, "old", base);
        let t = snap(
            &f,
            f.anchor.instant,
            Duration::from_secs(30),
            base + Duration::from_secs(11),
        );
        let by_id = |id: &str| {
            t.services[0]
                .instances
                .iter()
                .find(|i| i.instance_id == id)
                .unwrap()
                .clone()
        };
        assert_eq!(by_id("fresh").last_heartbeat_age_ms, 2000);
        assert!(!by_id("fresh").stale);
        assert_eq!(by_id("old").last_heartbeat_age_ms, 11000);
        assert!(by_id("old").stale);
        assert_eq!(t.heartbeat_timeout_ms, 30000);
    }

    #[tokio::test]
    async fn snapshot_reports_the_client_version() {
        let f = fx();
        connect(&f, "none", "svc");
        connect_with(
            &f,
            "lib",
            "svc",
            SessionMeta {
                client_version: "lib/1".into(),
                ..SessionMeta::default()
            },
        );
        let t = snap(
            &f,
            f.anchor.instant,
            Duration::from_secs(10),
            Instant::now(),
        );
        let by_id = |id: &str| {
            t.services[0]
                .instances
                .iter()
                .find(|i| i.instance_id == id)
                .unwrap()
        };
        assert_eq!(by_id("lib").client_version, "lib/1");
        assert_eq!(by_id("none").client_version, "");
        assert_eq!(t.hub_id, "hub-x");
    }

    #[tokio::test]
    async fn snapshot_reports_flow_state_load_factor_and_tags() {
        let f = fx();
        let placement = crate::registry::Placement {
            load_factor: 250,
            tags: [("zone".to_string(), "z1".to_string())].into(),
        };
        f.registry
            .register_with(&D, "i1", "a", &["a.Do".into()], &[], placement)
            .unwrap();
        connect(&f, "i1", "a");
        let token = f.sessions.token_of(&D, "i1").unwrap();
        let _ = f.router.open_session(&D, "i1", token, "a", 4);
        connect(&f, "i2", "b");
        let t2 = f.sessions.token_of(&D, "i2").unwrap();
        let _ = f.router.open_session(&D, "i2", t2, "b", 1);
        let t = snap(
            &f,
            f.anchor.instant,
            Duration::from_secs(10),
            Instant::now(),
        );
        let i1 = &t.services[0].instances[0];
        assert_eq!(
            (i1.permits_available, i1.queued, i1.load_factor),
            (4, 0, 250)
        );
        assert_eq!(i1.tags["zone"], "z1");
        let i2 = &t.services[1].instances[0];
        assert_eq!((i2.permits_available, i2.load_factor), (1, 100));
        assert_eq!(t.totals.queued, 0);
        let json = serde_json::to_string(&t).unwrap();
        for field in [
            "permitsAvailable",
            "loadFactor",
            "\"tags\":{\"zone\":\"z1\"}",
        ] {
            assert!(json.contains(field), "{field} missing: {json}");
        }
        assert_eq!(Topology::from_proto(&t.to_proto()), t);
    }

    #[tokio::test]
    async fn snapshot_reports_the_latest_instance_status() {
        let f = fx();
        connect(&f, "reported", "svc");
        connect(&f, "silent", "svc");
        let token = f.sessions.token_of(&D, "reported").unwrap();
        let at = Instant::now();
        let report = proto::InstanceStatus {
            connected_hub_ids: vec!["hub-x".into(), "hub-y".into()],
            handlers_in_flight: 2,
            processors: vec![],
        };
        assert!(f.sessions.set_status(&D, "reported", token, &report, at));
        let t = snap(
            &f,
            f.anchor.instant,
            Duration::from_secs(10),
            at + Duration::from_millis(1500),
        );
        let by_id = |id: &str| {
            t.services[0]
                .instances
                .iter()
                .find(|i| i.instance_id == id)
                .unwrap()
                .clone()
        };
        let reported = by_id("reported");
        assert_eq!(reported.connected_hub_ids, ["hub-x", "hub-y"]);
        assert_eq!(reported.status_age_ms, 1500);
        let silent = by_id("silent");
        assert!(silent.connected_hub_ids.is_empty());
        assert_eq!(silent.status_age_ms, -1, "never reported");

        let json = serde_json::to_value(&t).unwrap();
        let instances = &json["services"][0]["instances"];
        assert_eq!(
            instances[0]["connectedHubIds"],
            serde_json::json!(["hub-x", "hub-y"])
        );
        assert_eq!(instances[0]["statusAgeMs"], 1500);
        assert_eq!(instances[1]["statusAgeMs"], -1);
        assert_eq!(Topology::from_proto(&t.to_proto()), t);
        let p = t.to_proto();
        assert_eq!(
            p.services[0].instances[0].connected_hub_ids,
            ["hub-x", "hub-y"]
        );
        assert_eq!(p.services[0].instances[1].status_age_ms, -1);
    }

    #[tokio::test]
    async fn snapshot_reports_the_latest_processors() {
        let f = fx();
        connect(&f, "p", "svc");
        connect(&f, "silent", "svc");
        let token = f.sessions.token_of(&D, "p").unwrap();
        let report = proto::InstanceStatus {
            connected_hub_ids: vec!["hub-x".into()],
            handlers_in_flight: 0,
            processors: vec![proto::ProcessorStatus {
                name: "default".into(),
                subscription: "svc".into(),
                source: "snodra.events".into(),
                state: proto::ProcessorState::ProcessorFailing as i32,
                segments: vec![proto::SegmentStatus {
                    segment: "0".into(),
                    position: "7".into(),
                    committed: "5".into(),
                    head: "9".into(),
                    lag: 4,
                    replay_until: "8".into(),
                }],
                processed_total: 6,
                failed_total: 1,
                dead_letters: 1,
                last_error: "boom".into(),
                last_error_at_ms: 42,
                event_types: vec!["a.Happened".into()],
                dead_letter_mode: "collection".into(),
                replaying: true,
                operation_id: "op-1".into(),
                event_store: "kafka".into(),
                capabilities: vec!["lag".into(), "replay".into()],
            }],
        };
        assert!(
            f.sessions
                .set_status(&D, "p", token, &report, Instant::now())
        );
        let t = snap(
            &f,
            f.anchor.instant,
            Duration::from_secs(10),
            Instant::now(),
        );
        let by_id = |id: &str| {
            t.services[0]
                .instances
                .iter()
                .find(|i| i.instance_id == id)
                .unwrap()
                .clone()
        };
        let p = by_id("p");
        assert_eq!(p.processors.len(), 1);
        assert_eq!(p.processors[0].name, "default");
        assert_eq!(
            p.processors[0].state,
            crate::processors::ProcessorStateName::Failing
        );
        assert!(by_id("silent").processors.is_empty());

        let wire = t.to_proto();
        assert_eq!(wire.services[0].instances[0].processors, report.processors);
        assert!(wire.services[0].instances[1].processors.is_empty());
        assert_eq!(Topology::from_proto(&wire), t);

        let json = serde_json::to_value(&t).unwrap();
        let reported = &json["services"][0]["instances"][0]["processors"][0];
        assert_eq!(reported["name"], "default");
        assert_eq!(reported["state"], "FAILING");
        assert_eq!(reported["subscription"], "svc");
        assert_eq!(reported["eventStore"], "kafka");
        assert_eq!(
            reported["capabilities"],
            serde_json::json!(["lag", "replay"])
        );
        assert_eq!(reported["segments"][0]["head"], "9");
        assert_eq!(reported["lastErrorAtMs"], 42);
        assert_eq!(reported["deadLetterMode"], "collection");
        // The control plane's fields ride along.
        assert_eq!(reported["replaying"], true);
        assert_eq!(reported["operationId"], "op-1");
        assert_eq!(reported["segments"][0]["replayUntil"], "8");
        assert_eq!(
            json["services"][0]["instances"][1]["processors"],
            serde_json::json!([])
        );
        assert_eq!(serde_json::from_value::<Topology>(json).unwrap(), t);
    }

    fn sample() -> Topology {
        let f = fx();
        f.registry
            .register(&D, "i1", "a", &["a.Do".into()], &["a.Get".into()])
            .unwrap();
        connect(&f, "i1", "a");
        connect_with(
            &f,
            "i2",
            "b",
            SessionMeta {
                client_version: "lib/1".into(),
                ..SessionMeta::default()
            },
        );
        f.registry.register(&D, "i2", "b", &[], &[]).unwrap();
        snap(
            &f,
            f.anchor.instant,
            Duration::from_secs(30),
            Instant::now() + Duration::from_secs(1),
        )
    }

    #[tokio::test]
    async fn proto_round_trip_preserves_everything() {
        let t = sample();
        assert_eq!(t.types.len(), 2);
        assert_eq!(Topology::from_proto(&t.to_proto()), t);
    }

    #[tokio::test]
    async fn json_uses_camel_case_and_kind_names() {
        let t = sample();
        let json = serde_json::to_string(&t).unwrap();
        assert!(json.contains("\"kind\":\"COMMAND\""), "{json}");
        assert!(json.contains("\"kind\":\"QUERY\""), "{json}");
        assert!(json.contains("connectedSinceMs"), "{json}");
        assert!(json.contains("lastHeartbeatAgeMs"), "{json}");
        assert!(json.contains("heartbeatTimeoutMs"), "{json}");
        assert!(json.contains("\"hubId\":\"hub-x\""), "{json}");
        assert!(json.contains("\"clientVersion\":\"lib/1\""), "{json}");
        assert_eq!(serde_json::from_str::<Topology>(&json).unwrap(), t);
    }
}
