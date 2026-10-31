//! In-memory registry of connected service instances and the message types they handle.
//!
//! Model and routing rules (docs/05-services/hub.md):
//! - a message type is owned by exactly one service; another service registering it is rejected;
//! - when the last instance listing a type goes away the service keeps ownership for `owner_grace`
//!   so a restarting service does not lose its types; `sweep` then drops it;
//! - commands with a routing key go to one instance of the owning service chosen by consistent
//!   hashing over a ring weighted by each instance's load factor (same key, same instance while
//!   membership is stable; identical on every hub);
//! - commands without a key and all queries return every live instance of the owning service
//!   ([`Target::Candidates`]); the router picks one by tag match and load (ADR 0014). [`Registry::pick`]
//!   keeps the plain round-robin choice for callers without load information.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use xxhash_rust::xxh64::xxh64;

use crate::context::ContextId;

pub type InstanceId = String;

/// Virtual nodes of an instance with load factor 100.
const VNODES_PER_INSTANCE: usize = 128;
const MIN_VNODES: usize = 8;
const MAX_VNODES: usize = 1024;
/// `Register.load_factor` 0 reads as this.
pub const DEFAULT_LOAD_FACTOR: u32 = 100;

/// The load factor as applied: 0 reads as [`DEFAULT_LOAD_FACTOR`].
pub fn effective_load_factor(load_factor: u32) -> u32 {
    if load_factor == 0 {
        DEFAULT_LOAD_FACTOR
    } else {
        load_factor
    }
}

/// `128 * load_factor / 100` virtual nodes, clamped to 8..=1024.
pub fn vnodes_for(load_factor: u32) -> usize {
    (VNODES_PER_INSTANCE as u64 * effective_load_factor(load_factor) as u64 / 100)
        .clamp(MIN_VNODES as u64, MAX_VNODES as u64) as usize
}

/// Where an instance sits for routing: its relative capacity and its labels. Fixed by the first
/// `Register` of the session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// As applied (never 0).
    pub load_factor: u32,
    pub tags: BTreeMap<String, String>,
}

impl Default for Placement {
    fn default() -> Self {
        Self {
            load_factor: DEFAULT_LOAD_FACTOR,
            tags: BTreeMap::new(),
        }
    }
}

/// One live instance a request may go to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub id: InstanceId,
    pub load_factor: u32,
    pub tags: BTreeMap<String, String>,
}

/// What the registry decided for a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A keyed command: the ring's instance (affinity, never rerouted).
    Keyed(InstanceId),
    /// Every live instance of the owner, sorted by id, and a per-service rotation counter for
    /// round-robin tie-breaks (it advances on every pick).
    Candidates {
        candidates: Vec<Candidate>,
        rotation: usize,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum Kind {
    Command,
    Query,
}

#[derive(Debug, PartialEq, Eq)]
pub enum RegisterError {
    TypeOwnedByOtherService {
        type_name: String,
        owner: String,
    },
    /// The type is already owned (by any service) under a different kind.
    KindConflict {
        type_name: String,
        owner: String,
        owner_kind: Kind,
    },
    /// The request lists the same type as both command and query.
    DuplicateTypeAcrossKinds {
        type_name: String,
    },
}

impl Kind {
    fn label(self) -> &'static str {
        match self {
            Kind::Command => "COMMAND",
            Kind::Query => "QUERY",
        }
    }
}

impl fmt::Display for RegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TypeOwnedByOtherService { type_name, owner } => {
                write!(f, "type {type_name} is owned by service {owner}")
            }
            Self::KindConflict {
                type_name,
                owner,
                owner_kind,
            } => write!(
                f,
                "type {type_name} is owned as {} by service {owner}",
                owner_kind.label()
            ),
            Self::DuplicateTypeAcrossKinds { type_name } => {
                write!(f, "type {type_name} listed as both command and query")
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PickError {
    UnknownType,
    NoLiveInstance { service: String },
}

impl fmt::Display for PickError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownType => write!(f, "no service handles this type"),
            Self::NoLiveInstance { service } => {
                write!(f, "service {service} has no live instance for this type")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSnapshot {
    pub service: String,
    pub instances: usize,
    pub command_types: usize,
    pub query_types: usize,
}

/// Types one instance lists, sorted by name, and its placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceTypes {
    pub service: String,
    pub command_types: Vec<String>,
    pub query_types: Vec<String>,
    pub load_factor: u32,
    pub tags: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeInfo {
    pub type_name: String,
    pub kind: Kind,
    pub owner_service: String,
    pub live_instances: u32,
    /// `Some(age)` while no live instance lists the type (until `sweep` drops it).
    pub orphaned_for: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryTopology {
    pub instances: HashMap<InstanceId, InstanceTypes>,
    /// Sorted by type name.
    pub types: Vec<TypeInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistrySnapshot {
    pub services: Vec<ServiceSnapshot>,
}

/// Consistent-hash ring with virtual nodes.
#[derive(Default)]
struct Ring {
    points: BTreeMap<u64, InstanceId>,
}

impl Ring {
    /// Adds `vnodes` points. On the (64-bit, so rare) collision of two instances' points the
    /// smaller instance id keeps it, so the ring does not depend on the registration order.
    fn add(&mut self, instance_id: &str, vnodes: usize) {
        for i in 0..vnodes {
            let point = xxh64(format!("{instance_id}#{i}").as_bytes(), 0);
            self.points
                .entry(point)
                .and_modify(|held| {
                    if instance_id < held.as_str() {
                        *held = instance_id.to_string();
                    }
                })
                .or_insert_with(|| instance_id.to_string());
        }
    }

    fn remove(&mut self, instance_id: &str) {
        self.points.retain(|_, id| id != instance_id);
    }

    fn lookup(&self, hash: u64) -> Option<&InstanceId> {
        self.points
            .range(hash..)
            .next()
            .or_else(|| self.points.iter().next())
            .map(|(_, id)| id)
    }
}

struct Owner {
    service: String,
    kind: Kind,
    /// Number of live instances currently listing the type.
    holders: usize,
    /// Set when `holders` dropped to zero; ownership is released after the grace period.
    orphaned_at: Option<Instant>,
}

#[derive(Default)]
struct ServiceEntry {
    /// Sorted so round-robin order is deterministic.
    instances: BTreeSet<InstanceId>,
    ring: Ring,
    rr: usize,
}

struct InstanceEntry {
    service: String,
    types: Vec<(String, Kind)>,
    placement: Placement,
}

#[derive(Default)]
struct Inner {
    types: HashMap<String, Owner>,
    services: HashMap<String, ServiceEntry>,
    instances: HashMap<InstanceId, InstanceEntry>,
}

impl Inner {
    /// Removes an instance, releasing its type holds. Returns the service it belonged to.
    fn remove_instance(&mut self, instance_id: &str, now: Instant) -> Option<String> {
        let entry = self.instances.remove(instance_id)?;
        for (t, _) in &entry.types {
            if let Some(owner) = self.types.get_mut(t) {
                owner.holders = owner.holders.saturating_sub(1);
                if owner.holders == 0 {
                    owner.orphaned_at = Some(now);
                }
            }
        }
        if let Some(svc) = self.services.get_mut(&entry.service) {
            svc.instances.remove(instance_id);
            svc.ring.remove(instance_id);
            if svc.instances.is_empty() {
                self.services.remove(&entry.service);
            }
        }
        Some(entry.service)
    }
}

/// One registry partition. Every method that routes or (un)registers takes the [`ContextId`] of
/// the session; contexts are deferred, so the only context is `default` and the registry has a
/// single partition.
pub struct Registry {
    owner_grace: Duration,
    inner: Mutex<Inner>,
}

fn check_context(ctx: &ContextId) {
    debug_assert!(
        ctx.is_default(),
        "context {ctx} used before contexts are supported"
    );
}

impl Registry {
    pub fn new(owner_grace: Duration) -> Self {
        Self {
            owner_grace,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// [`Registry::register_with`] with the default placement (load factor 100, no tags).
    pub fn register(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        service: &str,
        command_types: &[String],
        query_types: &[String],
    ) -> Result<(), RegisterError> {
        self.register_with(
            ctx,
            instance_id,
            service,
            command_types,
            query_types,
            Placement::default(),
        )
    }

    /// Atomic: on error nothing changes. Replaces a previous registration of the same instance.
    /// A load factor of 0 in `placement` reads as 100.
    pub fn register_with(
        &self,
        ctx: &ContextId,
        instance_id: &str,
        service: &str,
        command_types: &[String],
        query_types: &[String],
        mut placement: Placement,
    ) -> Result<(), RegisterError> {
        check_context(ctx);
        placement.load_factor = effective_load_factor(placement.load_factor);
        let mut inner = self.inner.lock().unwrap();

        let mut wanted: Vec<(&String, Kind)> = Vec::new();
        let mut seen: HashMap<&String, Kind> = HashMap::new();
        let commands = command_types.iter().map(|t| (t, Kind::Command));
        let queries = query_types.iter().map(|t| (t, Kind::Query));
        for (t, kind) in commands.chain(queries) {
            match seen.insert(t, kind) {
                None => wanted.push((t, kind)),
                Some(prev) if prev != kind => {
                    return Err(RegisterError::DuplicateTypeAcrossKinds {
                        type_name: t.clone(),
                    });
                }
                Some(_) => {}
            }
        }

        for (t, kind) in &wanted {
            if let Some(owner) = inner.types.get(*t) {
                if owner.kind != *kind {
                    return Err(RegisterError::KindConflict {
                        type_name: (*t).clone(),
                        owner: owner.service.clone(),
                        owner_kind: owner.kind,
                    });
                }
                if owner.service != service {
                    return Err(RegisterError::TypeOwnedByOtherService {
                        type_name: (*t).clone(),
                        owner: owner.service.clone(),
                    });
                }
            }
        }

        let now = Instant::now();
        inner.remove_instance(instance_id, now);

        for (t, kind) in &wanted {
            let owner = inner.types.entry((*t).clone()).or_insert_with(|| Owner {
                service: service.to_string(),
                kind: *kind,
                holders: 0,
                orphaned_at: None,
            });
            owner.holders += 1;
            owner.orphaned_at = None;
        }

        let svc = inner.services.entry(service.to_string()).or_default();
        svc.instances.insert(instance_id.to_string());
        // An instance without types (a sender only, or one stopping gracefully: its last
        // Register is empty) is no routing target: not on the ring, not a candidate.
        if !wanted.is_empty() {
            svc.ring.add(instance_id, vnodes_for(placement.load_factor));
        }
        inner.instances.insert(
            instance_id.to_string(),
            InstanceEntry {
                service: service.to_string(),
                types: wanted.into_iter().map(|(t, k)| (t.clone(), k)).collect(),
                placement,
            },
        );
        Ok(())
    }

    /// Returns the service name the instance belonged to, if it was registered.
    pub fn unregister(&self, ctx: &ContextId, instance_id: &str, now: Instant) -> Option<String> {
        check_context(ctx);
        self.inner.lock().unwrap().remove_instance(instance_id, now)
    }

    /// The plain choice: the ring for keyed commands, else round-robin over the candidates.
    pub fn pick(
        &self,
        ctx: &ContextId,
        type_name: &str,
        routing_key: &str,
    ) -> Result<(Kind, InstanceId), PickError> {
        let (kind, target) = self.pick_target(ctx, type_name, routing_key)?;
        let id = match target {
            Target::Keyed(id) => id,
            Target::Candidates {
                mut candidates,
                rotation,
            } => {
                let i = rotation % candidates.len();
                candidates.swap_remove(i).id
            }
        };
        Ok((kind, id))
    }

    /// The ring target of a keyed command, or every live instance of the owner.
    pub fn pick_target(
        &self,
        ctx: &ContextId,
        type_name: &str,
        routing_key: &str,
    ) -> Result<(Kind, Target), PickError> {
        check_context(ctx);
        let mut inner = self.inner.lock().unwrap();
        let owner = inner.types.get(type_name).ok_or(PickError::UnknownType)?;
        let (kind, holders, service) = (owner.kind, owner.holders, owner.service.clone());
        let no_live = || PickError::NoLiveInstance {
            service: service.clone(),
        };
        if holders == 0 {
            return Err(no_live());
        }
        let Inner {
            services,
            instances,
            ..
        } = &mut *inner;
        let svc = services.get_mut(&service).ok_or_else(no_live)?;
        if svc.instances.is_empty() {
            return Err(no_live());
        }
        if kind == Kind::Command && !routing_key.is_empty() {
            let hash = xxh64(format!("{service}:{routing_key}").as_bytes(), 0);
            let id = svc.ring.lookup(hash).cloned().ok_or_else(no_live)?;
            return Ok((kind, Target::Keyed(id)));
        }
        let candidates: Vec<Candidate> = svc
            .instances
            .iter()
            .filter_map(|id| {
                let entry = instances.get(id).filter(|e| !e.types.is_empty())?;
                Some(Candidate {
                    id: id.clone(),
                    load_factor: entry.placement.load_factor,
                    tags: entry.placement.tags.clone(),
                })
            })
            .collect();
        if candidates.is_empty() {
            return Err(no_live());
        }
        let rotation = svc.rr;
        svc.rr = svc.rr.wrapping_add(1);
        Ok((
            kind,
            Target::Candidates {
                candidates,
                rotation,
            },
        ))
    }

    /// The placement an instance registered with (the sender's tags, for tag matching).
    pub fn placement_of(&self, ctx: &ContextId, instance_id: &str) -> Option<Placement> {
        check_context(ctx);
        let inner = self.inner.lock().unwrap();
        inner
            .instances
            .get(instance_id)
            .map(|e| e.placement.clone())
    }

    /// Drops ownership of types that have had no live instance for longer than `owner_grace`.
    /// Returns the number of types dropped.
    pub fn sweep(&self, now: Instant) -> usize {
        let grace = self.owner_grace;
        let mut inner = self.inner.lock().unwrap();
        let before = inner.types.len();
        inner.types.retain(|_, o| match o.orphaned_at {
            Some(at) if o.holders == 0 => now.saturating_duration_since(at) <= grace,
            _ => true,
        });
        before - inner.types.len()
    }

    pub fn service_of(&self, ctx: &ContextId, instance_id: &str) -> Option<String> {
        check_context(ctx);
        let inner = self.inner.lock().unwrap();
        inner.instances.get(instance_id).map(|e| e.service.clone())
    }

    /// Read-only view of instances, their types by kind and type ownership state.
    pub fn topology(&self, now: Instant) -> RegistryTopology {
        // Collect under the lock (the routing path shares it); sort after releasing it.
        let inner = self.inner.lock().unwrap();
        let mut instances: HashMap<String, InstanceTypes> = inner
            .instances
            .iter()
            .map(|(id, e)| {
                let of_kind = |k: Kind| -> Vec<String> {
                    e.types
                        .iter()
                        .filter(|(_, tk)| *tk == k)
                        .map(|(t, _)| t.clone())
                        .collect()
                };
                (
                    id.clone(),
                    InstanceTypes {
                        service: e.service.clone(),
                        command_types: of_kind(Kind::Command),
                        query_types: of_kind(Kind::Query),
                        load_factor: e.placement.load_factor,
                        tags: e.placement.tags.clone(),
                    },
                )
            })
            .collect();
        let mut types: Vec<TypeInfo> = inner
            .types
            .iter()
            .map(|(name, o)| TypeInfo {
                type_name: name.clone(),
                kind: o.kind,
                owner_service: o.service.clone(),
                live_instances: o.holders as u32,
                orphaned_for: match o.orphaned_at {
                    Some(at) if o.holders == 0 => Some(now.saturating_duration_since(at)),
                    _ => None,
                },
            })
            .collect();
        drop(inner);
        for it in instances.values_mut() {
            it.command_types.sort();
            it.query_types.sort();
        }
        types.sort_by(|a, b| a.type_name.cmp(&b.type_name));
        RegistryTopology { instances, types }
    }

    /// Per-service counts for metrics, sorted by service name.
    pub fn snapshot(&self) -> RegistrySnapshot {
        let inner = self.inner.lock().unwrap();
        let mut by_service: BTreeMap<&str, ServiceSnapshot> = BTreeMap::new();
        for (name, svc) in &inner.services {
            by_service.insert(
                name,
                ServiceSnapshot {
                    service: name.clone(),
                    instances: svc.instances.len(),
                    command_types: 0,
                    query_types: 0,
                },
            );
        }
        for owner in inner.types.values() {
            if let Some(s) = by_service.get_mut(owner.service.as_str()) {
                match owner.kind {
                    Kind::Command => s.command_types += 1,
                    Kind::Query => s.query_types += 1,
                }
            }
        }
        RegistrySnapshot {
            services: by_service.into_values().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static D: std::sync::LazyLock<ContextId> = std::sync::LazyLock::new(ContextId::default);

    fn reg() -> Registry {
        Registry::new(Duration::from_secs(60))
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn two_instances(r: &Registry, types: &[&str]) {
        r.register(&D, "a", "svc", &s(types), &[]).unwrap();
        r.register(&D, "b", "svc", &s(types), &[]).unwrap();
    }

    fn placed(load_factor: u32, tags: &[(&str, &str)]) -> Placement {
        Placement {
            load_factor,
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn vnodes_scale_with_the_load_factor_and_are_clamped() {
        assert_eq!(vnodes_for(0), 128); // 0 reads as 100
        assert_eq!(vnodes_for(100), 128);
        assert_eq!(vnodes_for(50), 64);
        assert_eq!(vnodes_for(300), 384);
        assert_eq!(vnodes_for(1), 8);
        assert_eq!(vnodes_for(5_000), 1024);
    }

    #[test]
    fn weighted_ring_distributes_keys_by_load_factor_within_five_percent() {
        let r = reg();
        r.register_with(&D, "a", "svc", &s(&["t.X"]), &[], placed(100, &[]))
            .unwrap();
        r.register_with(&D, "b", "svc", &s(&["t.X"]), &[], placed(300, &[]))
            .unwrap();
        let n = 20_000;
        let b = (0..n)
            .filter(|i| r.pick(&D, "t.X", &format!("order-{i}")).unwrap().1 == "b")
            .count();
        let share = b as f64 / n as f64;
        assert!(
            (share - 0.75).abs() <= 0.05,
            "b got {share:.3} of the keys, want 0.75"
        );
    }

    #[test]
    fn ring_is_deterministic_whatever_the_registration_order() {
        let one = reg();
        let two = reg();
        for (id, lf) in [("a", 100), ("b", 250), ("c", 40)] {
            one.register_with(&D, id, "svc", &s(&["t.X"]), &[], placed(lf, &[]))
                .unwrap();
        }
        for (id, lf) in [("c", 40), ("a", 100), ("b", 250)] {
            two.register_with(&D, id, "svc", &s(&["t.X"]), &[], placed(lf, &[]))
                .unwrap();
        }
        for i in 0..2_000 {
            let key = format!("k{i}");
            assert_eq!(
                one.pick(&D, "t.X", &key).unwrap(),
                two.pick(&D, "t.X", &key).unwrap(),
                "{key}"
            );
        }
    }

    #[test]
    fn unkeyed_pick_returns_sorted_candidates_with_placement_and_a_rotation() {
        let r = reg();
        r.register_with(
            &D,
            "b",
            "svc",
            &s(&["t.X"]),
            &[],
            placed(200, &[("zone", "z1")]),
        )
        .unwrap();
        r.register_with(&D, "a", "svc", &s(&["t.X"]), &[], placed(0, &[]))
            .unwrap();
        let (
            kind,
            Target::Candidates {
                candidates,
                rotation,
            },
        ) = r.pick_target(&D, "t.X", "").unwrap()
        else {
            panic!("expected candidates");
        };
        assert_eq!(kind, Kind::Command);
        let ids: Vec<_> = candidates.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(candidates[0].load_factor, 100);
        assert_eq!(candidates[1].tags["zone"], "z1");
        let (_, Target::Candidates { rotation: next, .. }) = r.pick_target(&D, "t.X", "").unwrap()
        else {
            panic!("expected candidates");
        };
        assert_eq!(next, rotation.wrapping_add(1));
        // Keyed commands get one ring target; queries ignore the key.
        assert!(matches!(
            r.pick_target(&D, "t.X", "k").unwrap().1,
            Target::Keyed(_)
        ));
        r.register_with(&D, "q", "svc2", &[], &s(&["q.Y"]), placed(100, &[]))
            .unwrap();
        assert!(matches!(
            r.pick_target(&D, "q.Y", "k").unwrap().1,
            Target::Candidates { .. }
        ));
        assert_eq!(
            r.placement_of(&D, "b").unwrap(),
            placed(200, &[("zone", "z1")])
        );
    }

    #[test]
    fn topology_reports_load_factor_and_tags() {
        let r = reg();
        r.register_with(&D, "a", "svc", &[], &[], placed(0, &[("zone", "z1")]))
            .unwrap();
        let topo = r.topology(Instant::now());
        assert_eq!(topo.instances["a"].load_factor, 100);
        assert_eq!(topo.instances["a"].tags["zone"], "z1");
    }

    /// A graceful stop re-registers without types: the instance stops drawing new work.
    #[test]
    fn an_instance_registered_without_types_is_no_routing_target() {
        let r = reg();
        two_instances(&r, &["t.Cmd"]);
        r.register(&D, "b", "svc", &[], &[]).unwrap();
        for key in 0..200 {
            let (_, id) = r.pick(&D, "t.Cmd", &format!("key-{key}")).unwrap();
            assert_eq!(
                id, "a",
                "keyed command {key} went to the stopping instance b"
            );
        }
        let (_, target) = r.pick_target(&D, "t.Cmd", "").unwrap();
        let Target::Candidates { candidates, .. } = target else {
            panic!("expected candidates");
        };
        let ids: Vec<&str> = candidates.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, ["a"]);
        r.register(&D, "a", "svc", &[], &[]).unwrap();
        assert!(
            r.pick(&D, "t.Cmd", "").is_err(),
            "no instance left to route to"
        );
    }

    #[test]
    fn pick_unknown_type_is_unknown_type() {
        assert_eq!(reg().pick(&D, "nope", ""), Err(PickError::UnknownType));
    }

    #[test]
    fn command_with_key_is_sticky_across_many_picks() {
        let r = reg();
        two_instances(&r, &["t.X"]);
        let first = r.pick(&D, "t.X", "k1").unwrap();
        assert_eq!(first.0, Kind::Command);
        for _ in 0..50 {
            assert_eq!(r.pick(&D, "t.X", "k1").unwrap(), first);
        }
    }

    #[test]
    fn command_with_key_distributes_over_keys() {
        let r = reg();
        two_instances(&r, &["t.X"]);
        let mut a = 0;
        let mut b = 0;
        for i in 0..1000 {
            match r.pick(&D, "t.X", &format!("key-{i}")).unwrap().1.as_str() {
                "a" => a += 1,
                _ => b += 1,
            }
        }
        assert!(a >= 300 && b >= 300, "a={a} b={b}");
    }

    #[test]
    fn command_without_key_round_robins() {
        let r = reg();
        two_instances(&r, &["t.X"]);
        let ids: Vec<_> = (0..4).map(|_| r.pick(&D, "t.X", "").unwrap().1).collect();
        assert_eq!(ids, ["a", "b", "a", "b"]);
    }

    #[test]
    fn query_ignores_routing_key_and_round_robins() {
        let r = reg();
        r.register(&D, "a", "svc", &[], &s(&["q.X"])).unwrap();
        r.register(&D, "b", "svc", &[], &s(&["q.X"])).unwrap();
        let first = r.pick(&D, "q.X", "same").unwrap();
        let second = r.pick(&D, "q.X", "same").unwrap();
        assert_eq!(first, (Kind::Query, "a".to_string()));
        assert_eq!(second, (Kind::Query, "b".to_string()));
    }

    #[test]
    fn second_service_registering_owned_type_is_rejected_atomically() {
        let r = reg();
        r.register(&D, "a1", "A", &s(&["t.X"]), &[]).unwrap();
        let err = r
            .register(&D, "b1", "B", &s(&["t.X", "t.Y"]), &[])
            .unwrap_err();
        assert_eq!(
            err,
            RegisterError::TypeOwnedByOtherService {
                type_name: "t.X".into(),
                owner: "A".into()
            }
        );
        assert_eq!(r.pick(&D, "t.Y", ""), Err(PickError::UnknownType));
        assert_eq!(r.service_of(&D, "b1"), None);
    }

    #[test]
    fn same_service_second_instance_is_allowed() {
        let r = reg();
        r.register(&D, "a1", "A", &s(&["t.X"]), &[]).unwrap();
        r.register(&D, "a2", "A", &s(&["t.X"]), &[]).unwrap();
        assert_eq!(r.snapshot().services[0].instances, 2);
    }

    #[test]
    fn reregister_with_smaller_list_removes_type() {
        let r = reg();
        let now = Instant::now();
        r.register(&D, "a", "A", &s(&["t.X", "t.Y"]), &[]).unwrap();
        r.register(&D, "a", "A", &s(&["t.X"]), &[]).unwrap();
        assert!(r.pick(&D, "t.X", "").is_ok());
        assert_eq!(
            r.pick(&D, "t.Y", ""),
            Err(PickError::NoLiveInstance {
                service: "A".into()
            })
        );
        r.sweep(now + Duration::from_secs(61));
        assert_eq!(r.pick(&D, "t.Y", ""), Err(PickError::UnknownType));
        assert!(r.pick(&D, "t.X", "").is_ok());
    }

    #[test]
    fn last_instance_leaving_keeps_ownership_during_grace() {
        let r = reg();
        r.register(&D, "a", "A", &s(&["t.X"]), &[]).unwrap();
        let t0 = Instant::now();
        assert_eq!(r.unregister(&D, "a", t0), Some("A".to_string()));
        let no_live = Err(PickError::NoLiveInstance {
            service: "A".into(),
        });
        assert_eq!(r.pick(&D, "t.X", ""), no_live);
        assert_eq!(r.sweep(t0 + Duration::from_secs(59)), 0);
        assert_eq!(r.pick(&D, "t.X", ""), no_live);
        assert_eq!(r.sweep(t0 + Duration::from_secs(61)), 1);
        assert_eq!(r.pick(&D, "t.X", ""), Err(PickError::UnknownType));
    }

    #[test]
    fn instance_rejoining_within_grace_restores_routing() {
        let r = reg();
        r.register(&D, "a", "A", &s(&["t.X"]), &[]).unwrap();
        let t0 = Instant::now();
        r.unregister(&D, "a", t0);
        r.register(&D, "a2", "A", &s(&["t.X"]), &[]).unwrap();
        assert_eq!(r.sweep(t0 + Duration::from_secs(61)), 0);
        assert_eq!(r.pick(&D, "t.X", "").unwrap().1, "a2");
    }

    #[test]
    fn removing_one_instance_moves_only_its_keys() {
        let r = reg();
        for id in ["a", "b", "c"] {
            r.register(&D, id, "svc", &s(&["t.X"]), &[]).unwrap();
        }
        let keys: Vec<String> = (0..1000).map(|i| format!("k{i}")).collect();
        let before: Vec<_> = keys
            .iter()
            .map(|k| r.pick(&D, "t.X", k).unwrap().1)
            .collect();
        r.unregister(&D, "c", Instant::now());
        for (k, was) in keys.iter().zip(before) {
            let now = r.pick(&D, "t.X", k).unwrap().1;
            if was != "c" {
                assert_eq!(now, was, "key {k} moved");
            } else {
                assert_ne!(now, "c");
            }
        }
    }

    #[test]
    fn topology_lists_instance_types_by_kind_sorted() {
        let r = reg();
        r.register(&D, "a", "svc", &s(&["b.X", "a.Y"]), &s(&["q.Z"]))
            .unwrap();
        let topo = r.topology(Instant::now());
        let it = &topo.instances["a"];
        assert_eq!(it.service, "svc");
        assert_eq!(it.command_types, ["a.Y", "b.X"]);
        assert_eq!(it.query_types, ["q.Z"]);
        let names: Vec<_> = topo.types.iter().map(|t| t.type_name.as_str()).collect();
        assert_eq!(names, ["a.Y", "b.X", "q.Z"]);
    }

    #[test]
    fn topology_reports_live_instances_and_orphaned_age() {
        let r = reg();
        two_instances(&r, &["t.X"]);
        let t0 = Instant::now();
        r.unregister(&D, "a", t0);
        let info = r.topology(t0).types.remove(0);
        assert_eq!((info.live_instances, info.orphaned_for), (1, None));
        r.unregister(&D, "b", t0);
        let now = t0 + Duration::from_secs(5);
        let info = r.topology(now).types.remove(0);
        assert_eq!(info.live_instances, 0);
        assert_eq!(info.orphaned_for, Some(now - t0));
        assert_eq!(info.kind, Kind::Command);
        assert_eq!(info.owner_service, "svc");
    }

    #[test]
    fn topology_keeps_orphaned_type_past_grace_until_sweep() {
        let r = reg();
        r.register(&D, "a", "svc", &s(&["t.X"]), &[]).unwrap();
        let t0 = Instant::now();
        r.unregister(&D, "a", t0);
        let late = t0 + Duration::from_secs(120);
        let types = r.topology(late).types;
        assert_eq!(types.len(), 1);
        assert_eq!(types[0].orphaned_for, Some(Duration::from_secs(120)));
        r.sweep(late);
        assert!(r.topology(late).types.is_empty());
    }

    #[test]
    fn register_same_type_under_both_kinds_is_rejected() {
        let r = reg();
        let err = r
            .register(&D, "a", "A", &s(&["t.X"]), &s(&["t.X"]))
            .unwrap_err();
        assert_eq!(
            err,
            RegisterError::DuplicateTypeAcrossKinds {
                type_name: "t.X".into()
            }
        );
        assert_eq!(r.service_of(&D, "a"), None);
        assert!(r.topology(Instant::now()).types.is_empty());
    }

    #[test]
    fn register_kind_conflict_with_owner_is_rejected() {
        let r = reg();
        r.register(&D, "a", "A", &s(&["t.X"]), &[]).unwrap();
        let err = r.register(&D, "b", "B", &[], &s(&["t.X"])).unwrap_err();
        assert_eq!(
            err,
            RegisterError::KindConflict {
                type_name: "t.X".into(),
                owner: "A".into(),
                owner_kind: Kind::Command
            }
        );
        assert_eq!(err.to_string(), "type t.X is owned as COMMAND by service A");
        assert_eq!(r.service_of(&D, "b"), None);
    }

    #[test]
    fn same_service_reregister_with_other_kind_is_rejected() {
        let r = reg();
        r.register(&D, "a", "A", &s(&["t.X"]), &[]).unwrap();
        let err = r.register(&D, "a2", "A", &[], &s(&["t.X"])).unwrap_err();
        assert!(matches!(err, RegisterError::KindConflict { .. }));
        assert_eq!(r.service_of(&D, "a2"), None);
    }
}
