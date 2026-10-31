//! The topology snapshot: one consistent-enough view of services, instances, type ownership and
//! in-flight requests, shared by the gRPC handler, the HTTP endpoint and the CLI.
//!
//! The snapshot reads sessions, then registry, then router, each under its own lock exactly once
//! (never nested, never across an await). Never contains payloads or headers.

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::debug;

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
    /// `Register.protocol_version` of the session (0 = v1).
    pub protocol_version: u32,
    /// `Register.client_version`, empty for v1 clients.
    pub client_version: String,
    /// The hub forwards to this instance only with permits.
    pub flow_controlled: bool,
    /// Permits left (0 when not flow-controlled).
    pub permits_available: u32,
    /// Requests waiting in the hub's queue for this instance.
    pub queued: u32,
    /// As applied (0 in `Register` reads as 100).
    pub load_factor: u32,
    pub tags: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TypeOwnership {
    #[serde(rename = "type")]
    pub type_name: String,
    pub kind: KindName,
    pub owner_service: String,
    pub live_instances: u32,
    pub orphaned: bool,
    pub orphaned_for_ms: i64,
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
        let (commands, queries, load_factor, tags) = reg
            .instances
            .get(&d.instance_id)
            .map(|i| {
                (
                    i.command_types.clone(),
                    i.query_types.clone(),
                    i.load_factor,
                    i.tags.clone(),
                )
            })
            .unwrap_or_else(|| (vec![], vec![], DEFAULT_LOAD_FACTOR, BTreeMap::new()));
        let flow = flows.get(&d.instance_id).copied().unwrap_or_default();
        let flight = in_flight.get(&d.instance_id).copied().unwrap_or_default();
        by_service
            .entry(d.service)
            .or_default()
            .push(InstanceTopology {
                connected_since_ms: anchor.to_epoch_ms(d.connected_since),
                last_heartbeat_age_ms: ms(age),
                // Each session against its own threshold (v2 and v1 sessions differ).
                stale: age > d.heartbeat_timeout / 3,
                in_flight_as_handler: flight.as_handler,
                in_flight_as_sender: flight.as_sender,
                command_types: commands,
                query_types: queries,
                protocol_version: d.protocol_version,
                client_version: d.client_version,
                flow_controlled: flow.flow_controlled,
                permits_available: flow.permits_available,
                queued: flow.queued,
                load_factor,
                tags,
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
            owner_service: t.owner_service.clone(),
            live_instances: t.live_instances,
            orphaned: t.orphaned_for.is_some(),
            orphaned_for_ms: t.orphaned_for.map_or(0, ms),
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
                            protocol_version: i.protocol_version,
                            client_version: i.client_version.clone(),
                            permits_available: i.permits_available,
                            queued: i.queued,
                            load_factor: i.load_factor,
                            tags: i.tags.clone().into_iter().collect(),
                            flow_controlled: i.flow_controlled,
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
                    owner_service: t.owner_service.clone(),
                    live_instances: t.live_instances,
                    orphaned: t.orphaned,
                    orphaned_for_ms: t.orphaned_for_ms,
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
                            protocol_version: i.protocol_version,
                            client_version: i.client_version.clone(),
                            flow_controlled: i.flow_controlled,
                            permits_available: i.permits_available,
                            queued: i.queued,
                            load_factor: i.load_factor,
                            tags: i.tags.clone().into_iter().collect(),
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
                    owner_service: t.owner_service.clone(),
                    live_instances: t.live_instances,
                    orphaned: t.orphaned,
                    orphaned_for_ms: t.orphaned_for_ms,
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
        connect_with(f, id, service, SessionMeta::v1(Duration::from_secs(30)));
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
    async fn stale_uses_each_session_own_timeout() {
        let f = fx();
        connect(&f, "v1", "svc");
        connect_with(
            &f,
            "v2",
            "svc",
            SessionMeta {
                protocol_version: 2,
                client_version: "lib/1".into(),
                ..SessionMeta::v1(Duration::from_secs(10))
            },
        );
        let base = Instant::now();
        f.sessions.touch(&D, "v1", base);
        f.sessions.touch(&D, "v2", base);
        // 5 s of silence: more than 10 s / 3, less than 30 s / 3.
        let t = snap(
            &f,
            f.anchor.instant,
            Duration::from_secs(10),
            base + Duration::from_secs(5),
        );
        let by_id = |id: &str| {
            t.services[0]
                .instances
                .iter()
                .find(|i| i.instance_id == id)
                .unwrap()
        };
        assert!(by_id("v2").stale);
        assert!(!by_id("v1").stale);
        assert_eq!(
            (
                by_id("v2").protocol_version,
                by_id("v2").client_version.as_str()
            ),
            (2, "lib/1")
        );
        assert_eq!(
            (
                by_id("v1").protocol_version,
                by_id("v1").client_version.as_str()
            ),
            (0, "")
        );
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
        f.router.open_session(&D, "i1", token, "a", Some(4));
        connect(&f, "i2", "b");
        let t2 = f.sessions.token_of(&D, "i2").unwrap();
        f.router.open_session(&D, "i2", t2, "b", None);
        let t = snap(
            &f,
            f.anchor.instant,
            Duration::from_secs(10),
            Instant::now(),
        );
        let i1 = &t.services[0].instances[0];
        assert!(i1.flow_controlled);
        assert_eq!(
            (i1.permits_available, i1.queued, i1.load_factor),
            (4, 0, 250)
        );
        assert_eq!(i1.tags["zone"], "z1");
        let i2 = &t.services[1].instances[0];
        assert!(!i2.flow_controlled);
        assert_eq!((i2.permits_available, i2.load_factor), (0, 100));
        assert_eq!(t.totals.queued, 0);
        let json = serde_json::to_string(&t).unwrap();
        for field in [
            "permitsAvailable",
            "flowControlled",
            "loadFactor",
            "\"tags\":{\"zone\":\"z1\"}",
        ] {
            assert!(json.contains(field), "{field} missing: {json}");
        }
        assert_eq!(Topology::from_proto(&t.to_proto()), t);
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
                protocol_version: 2,
                client_version: "lib/1".into(),
                ..SessionMeta::v1(Duration::from_secs(10))
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
        assert!(json.contains("\"protocolVersion\":2"), "{json}");
        assert!(json.contains("\"clientVersion\":\"lib/1\""), "{json}");
        assert_eq!(serde_json::from_str::<Topology>(&json).unwrap(), t);
    }
}
