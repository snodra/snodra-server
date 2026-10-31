//! The dashboard's JSON API (`/api/v1/`, roadmap P2): read-only views over the topology
//! snapshot, the edge statistics and the effective configuration. Never payloads, headers or
//! secrets. `/api/v1/topology` is the [`Topology`] snapshot itself; the other views are built
//! here so they can be tested without a server.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde::Serialize;
use xxhash_rust::xxh64::xxh64;

use crate::config::Config;
use crate::edges::{EdgeSnapshot, Latency, WindowTotals};
use crate::protocol;
use crate::registry::Kind;
use crate::topology::{KindName, Topology, Totals};

fn ms(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

/// Milliseconds with microsecond precision, for latencies.
fn ms_f64(d: Duration) -> f64 {
    (d.as_secs_f64() * 1_000_000.0).round() / 1000.0
}

fn kind_name(k: Kind) -> KindName {
    match k {
        Kind::Command => KindName::Command,
        Kind::Query => KindName::Query,
    }
}

/// Requests over the edge-statistics window.
#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Traffic {
    pub count: u64,
    pub errors: u64,
    /// `None` when no routed request contributed a latency.
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
}

impl Traffic {
    fn of(count: u64, errors: u64, latency: &Latency) -> Self {
        Self {
            count,
            errors,
            p50_ms: latency.quantile(0.5).map(ms_f64),
            p99_ms: latency.quantile(0.99).map(ms_f64),
        }
    }
}

/// Traffic per type, merged over its senders, and the senders.
#[derive(Default)]
struct TypeTraffic {
    count: u64,
    errors: u64,
    latency: Latency,
    kind: Option<Kind>,
    senders: BTreeSet<String>,
}

fn by_type(edges: &EdgeSnapshot) -> BTreeMap<&str, TypeTraffic> {
    let mut out: BTreeMap<&str, TypeTraffic> = BTreeMap::new();
    for e in &edges.edges {
        let t = out.entry(e.type_name.as_str()).or_default();
        t.count += e.count;
        t.errors += e.errors;
        t.latency.merge(&e.latency);
        t.kind = Some(e.kind);
        t.senders.insert(e.sender.clone());
    }
    out
}

// ---------------------------------------------------------------------------------------------
// GET /api/v1/version

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Version {
    pub hub_id: String,
    /// The hub crate version.
    pub version: String,
    /// The highest protocol version the hub speaks.
    pub protocol_version: u32,
    pub capabilities: Vec<String>,
    pub started_at_ms: i64,
    pub now_ms: i64,
    /// Content hash of the embedded dashboard assets; a page whose own build differs is stale.
    pub build_id: String,
    /// `KRAKEN_HUB_GRAFANA_URL`, `None` when unset.
    pub grafana_url: Option<String>,
    /// The edge-statistics window.
    pub window_ms: i64,
}

pub fn version(config: &Config, t: &Topology, build_id: &str) -> Version {
    Version {
        hub_id: t.hub_id.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_version: protocol::HUB_PROTOCOL_VERSION,
        capabilities: protocol::HUB_CAPABILITIES
            .iter()
            .map(|c| c.to_string())
            .collect(),
        started_at_ms: t.hub_started_at_ms,
        now_ms: t.now_ms,
        build_id: build_id.to_string(),
        grafana_url: config.grafana_url.clone(),
        window_ms: ms(config.edge_stats_window),
    }
}

// ---------------------------------------------------------------------------------------------
// GET /api/v1/types

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TypesView {
    pub now_ms: i64,
    pub window_ms: i64,
    pub types: Vec<TypeView>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TypeView {
    #[serde(rename = "type")]
    pub type_name: String,
    pub kind: KindName,
    /// Owning services: one today; a list so shared query types (roadmap P4a) fit.
    pub owners: Vec<String>,
    /// False for a type only seen in traffic (sent, but nobody registered it: `NO_HANDLER`).
    pub registered: bool,
    /// Live instances that registered the type.
    pub instances: Vec<String>,
    pub live_instances: u32,
    pub orphaned: bool,
    pub orphaned_for_ms: i64,
    /// Services that sent the type in the window.
    pub senders: Vec<String>,
    pub traffic: Traffic,
}

pub fn types(t: &Topology, edges: &EdgeSnapshot) -> TypesView {
    let mut traffic = by_type(edges);
    let mut instances: BTreeMap<&str, Vec<String>> = BTreeMap::new();
    for s in &t.services {
        for i in &s.instances {
            for name in i.command_types.iter().chain(&i.query_types) {
                instances
                    .entry(name.as_str())
                    .or_default()
                    .push(i.instance_id.clone());
            }
        }
    }
    let mut out: Vec<TypeView> = t
        .types
        .iter()
        .map(|o| {
            let tt = traffic.remove(o.type_name.as_str()).unwrap_or_default();
            TypeView {
                type_name: o.type_name.clone(),
                kind: o.kind,
                owners: vec![o.owner_service.clone()],
                registered: true,
                instances: instances.remove(o.type_name.as_str()).unwrap_or_default(),
                live_instances: o.live_instances,
                orphaned: o.orphaned,
                orphaned_for_ms: o.orphaned_for_ms,
                senders: tt.senders.iter().cloned().collect(),
                traffic: Traffic::of(tt.count, tt.errors, &tt.latency),
            }
        })
        .collect();
    out.extend(traffic.into_iter().map(|(name, tt)| TypeView {
        type_name: name.to_string(),
        kind: tt.kind.map_or(KindName::Command, kind_name),
        owners: vec![],
        registered: false,
        instances: vec![],
        live_instances: 0,
        orphaned: false,
        orphaned_for_ms: 0,
        senders: tt.senders.iter().cloned().collect(),
        traffic: Traffic::of(tt.count, tt.errors, &tt.latency),
    }));
    out.sort_by(|a, b| a.type_name.cmp(&b.type_name));
    TypesView {
        now_ms: t.now_ms,
        window_ms: ms(edges.window),
        types: out,
    }
}

// ---------------------------------------------------------------------------------------------
// GET /api/v1/graph

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Graph {
    pub now_ms: i64,
    pub window_ms: i64,
    /// `KRAKEN_HUB_EDGE_STATS_MAX_EDGES`.
    pub max_edges: usize,
    /// Edges dropped at the cap since the hub started.
    pub evicted_edges: u64,
    pub totals: GraphTotals,
    /// Services (`service:<name>`) and types (`type:<name>`), each sorted by name.
    pub nodes: Vec<Node>,
    /// `send` edges (sender service -> type, traffic in the window) and `handle` edges (type ->
    /// owner service, every owned type, traffic merged over its senders).
    pub edges: Vec<GraphEdge>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GraphTotals {
    pub services: u32,
    pub instances: u32,
    pub stale_instances: u32,
    pub types: u32,
    pub command_types: u32,
    pub query_types: u32,
    pub in_flight: u32,
    pub queued: u32,
    /// Requests and errors over the window, all edges.
    pub requests: u64,
    pub errors: u64,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Node {
    #[serde(rename_all = "camelCase")]
    Service {
        id: String,
        name: String,
        /// Live instances; 0 for an owner whose instances all left (orphaned types) or a sender
        /// that left within the window.
        instances: u32,
        stale_instances: u32,
    },
    #[serde(rename_all = "camelCase")]
    Type {
        id: String,
        name: String,
        type_kind: KindName,
        owners: Vec<String>,
        registered: bool,
        orphaned: bool,
    },
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum EdgeRole {
    Send,
    Handle,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GraphEdge {
    pub from: String,
    pub to: String,
    pub role: EdgeRole,
    #[serde(flatten)]
    pub traffic: Traffic,
}

pub fn service_id(name: &str) -> String {
    format!("service:{name}")
}

pub fn type_id(name: &str) -> String {
    format!("type:{name}")
}

pub fn graph(t: &Topology, edges: &EdgeSnapshot) -> Graph {
    let traffic = by_type(edges);

    // Services: connected ones, owners (also without instances) and senders in the window.
    let mut services: BTreeMap<String, (u32, u32)> = t
        .services
        .iter()
        .map(|s| {
            let stale = s.instances.iter().filter(|i| i.stale).count() as u32;
            (s.service_name.clone(), (s.instances.len() as u32, stale))
        })
        .collect();
    for o in &t.types {
        services.entry(o.owner_service.clone()).or_default();
    }
    for e in &edges.edges {
        services.entry(e.sender.clone()).or_default();
    }

    let mut nodes: Vec<Node> = services
        .iter()
        .map(|(name, &(instances, stale_instances))| Node::Service {
            id: service_id(name),
            name: name.clone(),
            instances,
            stale_instances,
        })
        .collect();
    let mut type_nodes: BTreeMap<&str, Node> = t
        .types
        .iter()
        .map(|o| {
            (
                o.type_name.as_str(),
                Node::Type {
                    id: type_id(&o.type_name),
                    name: o.type_name.clone(),
                    type_kind: o.kind,
                    owners: vec![o.owner_service.clone()],
                    registered: true,
                    orphaned: o.orphaned,
                },
            )
        })
        .collect();
    for (name, tt) in &traffic {
        type_nodes.entry(name).or_insert_with(|| Node::Type {
            id: type_id(name),
            name: name.to_string(),
            type_kind: tt.kind.map_or(KindName::Command, kind_name),
            owners: vec![],
            registered: false,
            orphaned: false,
        });
    }
    nodes.extend(type_nodes.into_values());

    let mut graph_edges: Vec<GraphEdge> = edges
        .edges
        .iter()
        .map(|e| GraphEdge {
            from: service_id(&e.sender),
            to: type_id(&e.type_name),
            role: EdgeRole::Send,
            traffic: Traffic::of(e.count, e.errors, &e.latency),
        })
        .collect();
    graph_edges.extend(t.types.iter().map(|o| {
        let tt = traffic.get(o.type_name.as_str());
        GraphEdge {
            from: type_id(&o.type_name),
            to: service_id(&o.owner_service),
            role: EdgeRole::Handle,
            traffic: tt.map_or_else(Traffic::default, |tt| {
                Traffic::of(tt.count, tt.errors, &tt.latency)
            }),
        }
    }));

    let totals = GraphTotals {
        services: t.totals.services,
        instances: t.totals.instances,
        stale_instances: stale_instances(t),
        types: t.totals.command_types + t.totals.query_types,
        command_types: t.totals.command_types,
        query_types: t.totals.query_types,
        in_flight: t.totals.in_flight,
        queued: t.totals.queued,
        requests: edges.edges.iter().map(|e| e.count).sum(),
        errors: edges.edges.iter().map(|e| e.errors).sum(),
    };
    Graph {
        now_ms: t.now_ms,
        window_ms: ms(edges.window),
        max_edges: edges.max_edges,
        evicted_edges: edges.evicted,
        totals,
        nodes,
        edges: graph_edges,
    }
}

fn stale_instances(t: &Topology) -> u32 {
    t.services
        .iter()
        .flat_map(|s| &s.instances)
        .filter(|i| i.stale)
        .count() as u32
}

// ---------------------------------------------------------------------------------------------
// GET /api/v1/settings

/// Whether a setting's value may be shown. A `Secret` setting is listed by name only: its value
/// and default are never serialized. No hub setting is secret today; tokens and key paths
/// (roadmap P5) must be declared `Secret`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sensitivity {
    Plain,
    Secret,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Setting {
    pub name: &'static str,
    /// The effective value in the variable's own syntax; `None` for a secret.
    pub value: Option<String>,
    /// The built-in default; `None` for a secret or a generated default (`KRAKEN_HUB_ID`).
    pub default: Option<String>,
    pub secret: bool,
    pub description: &'static str,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SettingsView {
    pub settings: Vec<Setting>,
}

fn setting(
    name: &'static str,
    value: String,
    default: Option<String>,
    sensitivity: Sensitivity,
    description: &'static str,
) -> Setting {
    match sensitivity {
        Sensitivity::Plain => Setting {
            name,
            value: Some(value),
            default,
            secret: false,
            description,
        },
        Sensitivity::Secret => Setting {
            name,
            value: None,
            default: None,
            secret: true,
            description,
        },
    }
}

/// `5m`, `10s` or `500ms`: the shortest exact form `parse_duration` reads back.
pub fn fmt_duration(d: Duration) -> String {
    let ms = d.as_millis();
    if ms >= 60_000 && ms.is_multiple_of(60_000) {
        format!("{}m", ms / 60_000)
    } else if ms.is_multiple_of(1000) {
        format!("{}s", ms / 1000)
    } else {
        format!("{ms}ms")
    }
}

/// `64MiB`, `512KiB`, or plain bytes: the largest exact unit `parse_size` reads back.
pub fn fmt_size(bytes: usize) -> String {
    for (unit, size) in [("GiB", 1usize << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)] {
        if bytes >= size && bytes.is_multiple_of(size) {
            return format!("{}{unit}", bytes / size);
        }
    }
    format!("{bytes}B")
}

/// A URL without the `user:password@` part, if it has one.
pub fn strip_userinfo(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    match rest[..authority_end].rfind('@') {
        Some(at) => format!("{scheme}://***@{}", &rest[at + 1..]),
        None => url.to_string(),
    }
}

fn overrides(c: &Config) -> String {
    c.timeout_overrides
        .iter()
        .map(|(t, d)| format!("{t}={}", d.as_millis()))
        .collect::<Vec<_>>()
        .join(",")
}

/// Every `KRAKEN_HUB_*` setting with its effective value and default, in the order of the
/// configuration table in hub.md. Values use the variable's own syntax; an empty value means
/// disabled or none.
pub fn settings(c: &Config) -> SettingsView {
    use Sensitivity::Plain;
    let d = Config::default();
    let addr = |a: Option<std::net::SocketAddr>| a.map_or_else(String::new, |a| a.to_string());
    let url = |u: &Option<String>| u.as_deref().map_or_else(String::new, strip_userinfo);
    let s = vec![
        setting(
            "KRAKEN_HUB_ID",
            c.hub_id.clone(),
            None,
            Plain,
            "identifies this hub in RegisterAck and the topology (default: hostname and a random suffix)",
        ),
        setting(
            "KRAKEN_HUB_LISTEN",
            c.listen.to_string(),
            Some(d.listen.to_string()),
            Plain,
            "gRPC bind address",
        ),
        setting(
            "KRAKEN_HUB_HTTP_LISTEN",
            addr(c.http_listen),
            Some(addr(d.http_listen)),
            Plain,
            "HTTP bind address (topology, dashboard, API); empty disables it",
        ),
        setting(
            "KRAKEN_HUB_OTLP_ENDPOINT",
            url(&c.otlp_endpoint),
            Some(url(&d.otlp_endpoint)),
            Plain,
            "OTLP gRPC endpoint for traces and metrics; empty disables export",
        ),
        setting(
            "KRAKEN_HUB_LOG",
            c.log.clone(),
            Some(d.log.clone()),
            Plain,
            "tracing filter",
        ),
        setting(
            "KRAKEN_HUB_DEFAULT_TIMEOUT_MS",
            c.default_timeout.as_millis().to_string(),
            Some(d.default_timeout.as_millis().to_string()),
            Plain,
            "deadline for requests with timeout_ms 0 and no override",
        ),
        setting(
            "KRAKEN_HUB_MAX_TIMEOUT_MS",
            c.max_timeout.as_millis().to_string(),
            Some(d.max_timeout.as_millis().to_string()),
            Plain,
            "upper clamp for timeout_ms on types without an override",
        ),
        setting(
            "KRAKEN_HUB_TIMEOUT_OVERRIDES",
            overrides(c),
            Some(overrides(&d)),
            Plain,
            "per-type timeouts (type=ms), replacing default and clamp",
        ),
        setting(
            "KRAKEN_HUB_HEARTBEAT_INTERVAL",
            fmt_duration(c.heartbeat_interval),
            Some(fmt_duration(d.heartbeat_interval)),
            Plain,
            "heartbeat period announced to v2 clients",
        ),
        setting(
            "KRAKEN_HUB_HEARTBEAT_TIMEOUT",
            fmt_duration(c.heartbeat_timeout),
            Some(fmt_duration(d.heartbeat_timeout)),
            Plain,
            "dead-instance threshold, v2 sessions",
        ),
        setting(
            "KRAKEN_HUB_LEGACY_HEARTBEAT_TIMEOUT",
            fmt_duration(c.legacy_heartbeat_timeout),
            Some(fmt_duration(d.legacy_heartbeat_timeout)),
            Plain,
            "dead-instance threshold, v1 sessions",
        ),
        setting(
            "KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE",
            c.queue_capacity_per_instance.to_string(),
            Some(d.queue_capacity_per_instance.to_string()),
            Plain,
            "requests queued per flow-controlled instance",
        ),
        setting(
            "KRAKEN_HUB_QUEUE_BYTES_PER_INSTANCE",
            fmt_size(c.queue_bytes_per_instance),
            Some(fmt_size(d.queue_bytes_per_instance)),
            Plain,
            "encoded bytes queued per flow-controlled instance",
        ),
        setting(
            "KRAKEN_HUB_QUEUE_BYTES_TOTAL",
            fmt_size(c.queue_bytes_total),
            Some(fmt_size(d.queue_bytes_total)),
            Plain,
            "encoded bytes queued over all instances",
        ),
        setting(
            "KRAKEN_HUB_OWNER_GRACE",
            fmt_duration(c.owner_grace),
            Some(fmt_duration(d.owner_grace)),
            Plain,
            "type ownership kept after the last instance leaves",
        ),
        setting(
            "KRAKEN_HUB_SESSION_BUFFER",
            c.session_buffer.to_string(),
            Some(d.session_buffer.to_string()),
            Plain,
            "per-session outbound buffer, in messages",
        ),
        setting(
            "KRAKEN_HUB_HANDSHAKE_TIMEOUT",
            fmt_duration(c.handshake_timeout),
            Some(fmt_duration(d.handshake_timeout)),
            Plain,
            "time allowed for the first Register on a new stream",
        ),
        setting(
            "KRAKEN_HUB_DASHBOARD_ENABLED",
            c.dashboard_enabled.to_string(),
            Some(d.dashboard_enabled.to_string()),
            Plain,
            "serve this dashboard and /api/v1 and keep edge statistics",
        ),
        setting(
            "KRAKEN_HUB_GRAFANA_URL",
            url(&c.grafana_url),
            Some(url(&d.grafana_url)),
            Plain,
            "Grafana base URL the dashboard links to; empty: no link",
        ),
        setting(
            "KRAKEN_HUB_EDGE_STATS_MAX_EDGES",
            c.edge_stats_max_edges.to_string(),
            Some(d.edge_stats_max_edges.to_string()),
            Plain,
            "cap on (sender service, type) edges; the least recently used go first",
        ),
        setting(
            "KRAKEN_HUB_EDGE_STATS_WINDOW",
            fmt_duration(c.edge_stats_window),
            Some(fmt_duration(d.edge_stats_window)),
            Plain,
            "sliding window of the edge statistics",
        ),
    ];
    SettingsView { settings: s }
}

// ---------------------------------------------------------------------------------------------
// GET /api/v1/stream (event `summary`)

#[derive(Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Summary {
    pub hub_id: String,
    pub build_id: String,
    pub now_ms: i64,
    pub totals: Totals,
    pub stale_instances: u32,
    /// Requests and errors over the edge-statistics window.
    pub requests: u64,
    pub errors: u64,
    /// Changes when services, instances, registrations, flow-control or staleness flags, type
    /// ownership or the set of edges change; not with counters or ages. A page refreshes its
    /// view at once when it changes.
    pub revision: String,
}

/// The structural fingerprint behind [`Summary::revision`].
pub fn revision(t: &Topology, edges: &WindowTotals) -> String {
    let mut s = String::new();
    for svc in &t.services {
        s.push_str(&svc.service_name);
        s.push('\u{1}');
        for i in &svc.instances {
            s.push_str(&format!(
                "{}|{}|{}|{}|{:?}|{:?}\u{2}",
                i.instance_id,
                i.protocol_version,
                i.flow_controlled,
                i.stale,
                i.command_types,
                i.query_types
            ));
        }
    }
    for o in &t.types {
        s.push_str(&format!(
            "{}|{:?}|{}|{}|{}\u{3}",
            o.type_name, o.kind, o.owner_service, o.orphaned, o.live_instances
        ));
    }
    format!("{:016x}", xxh64(s.as_bytes(), edges.keys_hash))
}

pub fn summary(t: &Topology, edges: &WindowTotals, build_id: &str) -> Summary {
    Summary {
        hub_id: t.hub_id.clone(),
        build_id: build_id.to_string(),
        now_ms: t.now_ms,
        totals: t.totals.clone(),
        stale_instances: stale_instances(t),
        requests: edges.requests,
        errors: edges.errors,
        revision: revision(t, edges),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edges::EdgeStats;
    use crate::topology::{InstanceTopology, ServiceTopology, TypeOwnership};
    use tokio::time::Instant;

    fn instance(id: &str, commands: &[&str], queries: &[&str], stale: bool) -> InstanceTopology {
        InstanceTopology {
            instance_id: id.into(),
            connected_since_ms: 1,
            last_heartbeat_age_ms: 0,
            stale,
            in_flight_as_handler: 0,
            in_flight_as_sender: 0,
            command_types: commands.iter().map(|s| s.to_string()).collect(),
            query_types: queries.iter().map(|s| s.to_string()).collect(),
            protocol_version: 2,
            client_version: "lib/1".into(),
            flow_controlled: false,
            permits_available: 0,
            queued: 0,
            load_factor: 100,
            tags: BTreeMap::new(),
        }
    }

    fn owned(name: &str, kind: KindName, owner: &str, live: u32) -> TypeOwnership {
        TypeOwnership {
            type_name: name.into(),
            kind,
            owner_service: owner.into(),
            live_instances: live,
            orphaned: live == 0,
            orphaned_for_ms: if live == 0 { 1500 } else { 0 },
        }
    }

    /// web (sender only), orders (owns orders.Place, orders.Get), billing (left: orphaned type).
    fn topology() -> Topology {
        Topology {
            hub_id: "hub-1".into(),
            hub_started_at_ms: 1_000,
            now_ms: 5_000,
            heartbeat_timeout_ms: 10_000,
            services: vec![
                ServiceTopology {
                    service_name: "orders".into(),
                    instances: vec![
                        instance("o-1", &["orders.Place"], &["orders.Get"], false),
                        instance("o-2", &["orders.Place"], &["orders.Get"], true),
                    ],
                },
                ServiceTopology {
                    service_name: "web".into(),
                    instances: vec![instance("w-1", &[], &[], false)],
                },
            ],
            types: vec![
                owned("billing.Charge", KindName::Command, "billing", 0),
                owned("orders.Get", KindName::Query, "orders", 2),
                owned("orders.Place", KindName::Command, "orders", 2),
            ],
            totals: Totals {
                services: 2,
                instances: 3,
                command_types: 2,
                query_types: 1,
                in_flight: 4,
                queued: 1,
            },
        }
    }

    fn edges() -> EdgeSnapshot {
        let stats = EdgeStats::new(Duration::from_secs(300), 100);
        let now = Instant::now();
        let rec = |sender: &str, t: &str, kind, error, ms: Option<u64>| {
            stats.record(sender, t, kind, error, ms.map(Duration::from_millis), now)
        };
        for _ in 0..9 {
            rec("web", "orders.Place", Kind::Command, false, Some(10));
        }
        rec("web", "orders.Place", Kind::Command, true, Some(10));
        rec("web", "orders.Get", Kind::Query, false, Some(2));
        rec("orders", "orders.Get", Kind::Query, false, Some(2));
        rec("web", "orders.Typo", Kind::Query, true, None);
        stats.snapshot(now)
    }

    #[tokio::test(start_paused = true)]
    async fn graph_has_service_and_type_nodes_and_both_edge_roles() {
        let g = graph(&topology(), &edges());
        let ids: Vec<String> = g
            .nodes
            .iter()
            .map(|n| match n {
                Node::Service { id, .. } | Node::Type { id, .. } => id.clone(),
            })
            .collect();
        assert_eq!(
            ids,
            [
                "service:billing",
                "service:orders",
                "service:web",
                "type:billing.Charge",
                "type:orders.Get",
                "type:orders.Place",
                "type:orders.Typo",
            ]
        );
        let edge = |from: &str, to: &str| {
            g.edges
                .iter()
                .find(|e| e.from == from && e.to == to)
                .unwrap_or_else(|| panic!("no edge {from} -> {to}"))
        };
        let send = edge("service:web", "type:orders.Place");
        assert_eq!(send.role, EdgeRole::Send);
        assert_eq!((send.traffic.count, send.traffic.errors), (10, 1));
        let p50 = send.traffic.p50_ms.unwrap();
        assert!((p50 - 10.0).abs() < 0.5, "{p50}");
        // The handle edge merges every sender of the type.
        let handle = edge("type:orders.Get", "service:orders");
        assert_eq!(handle.role, EdgeRole::Handle);
        assert_eq!(handle.traffic.count, 2);
        // An owned type without traffic still has its handle edge, with zero traffic.
        let idle = edge("type:billing.Charge", "service:billing");
        assert_eq!(idle.traffic, Traffic::default());
        // A type nobody registered has a send edge but no handle edge.
        assert!(g.edges.iter().all(|e| e.from != "type:orders.Typo"));
        let typo = edge("service:web", "type:orders.Typo");
        assert_eq!(typo.traffic.p99_ms, None);
        assert_eq!(g.edges.len(), 4 + 3);
        assert_eq!(
            (g.totals.requests, g.totals.errors, g.totals.stale_instances),
            (13, 2, 1)
        );
        assert_eq!(
            (g.totals.types, g.totals.in_flight, g.totals.queued),
            (3, 4, 1)
        );
        assert_eq!(g.window_ms, 300_000);

        let json = serde_json::to_value(&g).unwrap();
        let billing = &json["nodes"][0];
        assert_eq!(billing["kind"], "service");
        assert_eq!(billing["instances"], 0);
        let typo = &json["nodes"][6];
        assert_eq!(typo["kind"], "type");
        assert_eq!(typo["typeKind"], "QUERY");
        assert_eq!(typo["registered"], false);
        let first_edge = &json["edges"][0];
        for field in ["from", "to", "role", "count", "errors", "p50Ms", "p99Ms"] {
            assert!(
                first_edge.get(field).is_some(),
                "{field} missing: {first_edge}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn types_list_owners_instances_senders_and_traffic() {
        let v = types(&topology(), &edges());
        let names: Vec<_> = v.types.iter().map(|t| t.type_name.as_str()).collect();
        assert_eq!(
            names,
            [
                "billing.Charge",
                "orders.Get",
                "orders.Place",
                "orders.Typo"
            ]
        );
        let get = &v.types[1];
        assert_eq!(get.owners, ["orders"]);
        assert_eq!(get.instances, ["o-1", "o-2"]);
        assert_eq!(get.senders, ["orders", "web"]);
        assert_eq!(get.kind, KindName::Query);
        assert_eq!(get.traffic.count, 2);
        let billing = &v.types[0];
        assert!(billing.orphaned && billing.registered);
        assert_eq!(billing.orphaned_for_ms, 1500);
        assert!(billing.instances.is_empty());
        let typo = &v.types[3];
        assert!(!typo.registered);
        assert!(typo.owners.is_empty());
        assert_eq!(typo.kind, KindName::Query);
        assert_eq!((typo.traffic.count, typo.traffic.errors), (1, 1));
        let json = serde_json::to_value(&v).unwrap();
        assert_eq!(json["types"][1]["type"], "orders.Get");
        assert_eq!(json["types"][1]["liveInstances"], 2);
    }

    #[test]
    fn settings_list_every_variable_with_effective_value_and_default() {
        let c = Config {
            hub_id: "hub-7".into(),
            default_timeout: Duration::from_millis(2500),
            timeout_overrides: [("files.Upload".to_string(), Duration::from_secs(120))].into(),
            grafana_url: Some("https://admin:hunter2@grafana.example/d/hub".into()),
            otlp_endpoint: None,
            ..Config::default()
        };
        let v = settings(&c);
        let get = |name: &str| {
            v.settings
                .iter()
                .find(|s| s.name == name)
                .unwrap_or_else(|| panic!("{name} missing"))
        };
        assert_eq!(v.settings.len(), 21);
        assert_eq!(get("KRAKEN_HUB_ID").value.as_deref(), Some("hub-7"));
        assert_eq!(get("KRAKEN_HUB_ID").default, None);
        let t = get("KRAKEN_HUB_DEFAULT_TIMEOUT_MS");
        assert_eq!(
            (t.value.as_deref(), t.default.as_deref()),
            (Some("2500"), Some("5000"))
        );
        assert_eq!(
            get("KRAKEN_HUB_TIMEOUT_OVERRIDES").value.as_deref(),
            Some("files.Upload=120000")
        );
        assert_eq!(get("KRAKEN_HUB_OTLP_ENDPOINT").value.as_deref(), Some(""));
        assert_eq!(
            get("KRAKEN_HUB_QUEUE_BYTES_PER_INSTANCE").value.as_deref(),
            Some("64MiB")
        );
        assert_eq!(
            get("KRAKEN_HUB_EDGE_STATS_WINDOW").value.as_deref(),
            Some("5m")
        );
        assert_eq!(
            get("KRAKEN_HUB_HEARTBEAT_INTERVAL").value.as_deref(),
            Some("3s")
        );
        let json = serde_json::to_string(&v).unwrap();
        assert!(!json.contains("hunter2"), "{json}");
        assert!(json.contains("https://***@grafana.example/d/hub"), "{json}");
        assert!(v.settings.iter().all(|s| !s.secret));
    }

    #[test]
    fn a_secret_setting_never_carries_its_value() {
        let s = setting(
            "KRAKEN_HUB_TOKEN",
            "s3cret".into(),
            Some("default-s3cret".into()),
            Sensitivity::Secret,
            "a token",
        );
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("s3cret"), "{json}");
        assert!(json.contains("\"secret\":true"), "{json}");
        assert!(json.contains("\"value\":null"), "{json}");
    }

    #[test]
    fn durations_and_sizes_print_in_the_syntax_they_parse_from() {
        use crate::config::{parse_duration, parse_size};
        for d in [
            Duration::from_millis(500),
            Duration::from_secs(3),
            Duration::from_secs(90),
            Duration::from_secs(300),
        ] {
            assert_eq!(parse_duration(&fmt_duration(d)).unwrap(), d);
        }
        assert_eq!(fmt_duration(Duration::from_secs(90)), "90s");
        for n in [10usize, 1536, 512 << 10, 64 << 20, 1 << 30] {
            assert_eq!(parse_size(&fmt_size(n)).unwrap(), n);
        }
        assert_eq!(fmt_size(1536), "1536B");
    }

    #[test]
    fn userinfo_is_stripped_from_urls() {
        assert_eq!(strip_userinfo("http://otel:4317"), "http://otel:4317");
        assert_eq!(strip_userinfo("https://u:p@h/x@y"), "https://***@h/x@y");
        assert_eq!(strip_userinfo("no-scheme"), "no-scheme");
    }

    #[tokio::test(start_paused = true)]
    async fn the_revision_ignores_counters_and_follows_structure() {
        let t = topology();
        let w = WindowTotals::default();
        let r = revision(&t, &w);
        let mut busier = t.clone();
        busier.totals.in_flight = 99;
        busier.now_ms += 2000;
        busier.services[0].instances[0].last_heartbeat_age_ms = 1234;
        assert_eq!(revision(&busier, &w), r);
        let mut stale = t.clone();
        stale.services[0].instances[0].stale = true;
        assert_ne!(revision(&stale, &w), r);
        let mut fewer = t.clone();
        fewer.services[1].instances.clear();
        assert_ne!(revision(&fewer, &w), r);
        let more_edges = WindowTotals { keys_hash: 42, ..w };
        assert_ne!(revision(&t, &more_edges), r);
        let s = summary(&t, &w, "b1");
        assert_eq!((s.stale_instances, s.build_id.as_str()), (1, "b1"));
    }
}
