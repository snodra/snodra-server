//! In-memory registry of connected service instances and the message types they handle.
//!
//! Model and routing rules (docs/05-services/hub.md):
//! - a message type is owned by exactly one service; another service registering it is rejected;
//! - when the last instance listing a type goes away the service keeps ownership for `owner_grace`
//!   so a restarting service does not lose its types; `sweep` then drops it;
//! - commands with a routing key go to one instance of the owning service chosen by consistent
//!   hashing (same key, same instance while membership is stable);
//! - commands without a key and all queries go to one instance chosen round-robin.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use xxhash_rust::xxh64::xxh64;

pub type InstanceId = String;

const VNODES_PER_INSTANCE: usize = 128;

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

/// Types one instance lists, sorted by name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceTypes {
    pub service: String,
    pub command_types: Vec<String>,
    pub query_types: Vec<String>,
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
    fn add(&mut self, instance_id: &str) {
        for i in 0..VNODES_PER_INSTANCE {
            let point = xxh64(format!("{instance_id}#{i}").as_bytes(), 0);
            self.points.insert(point, instance_id.to_string());
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

pub struct Registry {
    owner_grace: Duration,
    inner: Mutex<Inner>,
}

impl Registry {
    pub fn new(owner_grace: Duration) -> Self {
        Self {
            owner_grace,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Atomic: on error nothing changes. Replaces a previous registration of the same instance.
    pub fn register(
        &self,
        instance_id: &str,
        service: &str,
        command_types: &[String],
        query_types: &[String],
    ) -> Result<(), RegisterError> {
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
        svc.ring.add(instance_id);
        inner.instances.insert(
            instance_id.to_string(),
            InstanceEntry {
                service: service.to_string(),
                types: wanted.into_iter().map(|(t, k)| (t.clone(), k)).collect(),
            },
        );
        Ok(())
    }

    /// Returns the service name the instance belonged to, if it was registered.
    pub fn unregister(&self, instance_id: &str, now: Instant) -> Option<String> {
        self.inner.lock().unwrap().remove_instance(instance_id, now)
    }

    pub fn pick(
        &self,
        type_name: &str,
        routing_key: &str,
    ) -> Result<(Kind, InstanceId), PickError> {
        let mut inner = self.inner.lock().unwrap();
        let owner = inner.types.get(type_name).ok_or(PickError::UnknownType)?;
        let (kind, holders, service) = (owner.kind, owner.holders, owner.service.clone());
        let no_live = || PickError::NoLiveInstance {
            service: service.clone(),
        };
        if holders == 0 {
            return Err(no_live());
        }
        let svc = inner.services.get_mut(&service).ok_or_else(no_live)?;
        if svc.instances.is_empty() {
            return Err(no_live());
        }
        let id = if kind == Kind::Command && !routing_key.is_empty() {
            let hash = xxh64(format!("{service}:{routing_key}").as_bytes(), 0);
            svc.ring.lookup(hash).cloned().ok_or_else(no_live)?
        } else {
            let idx = svc.rr % svc.instances.len();
            svc.rr = svc.rr.wrapping_add(1);
            svc.instances.iter().nth(idx).cloned().ok_or_else(no_live)?
        };
        Ok((kind, id))
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

    pub fn service_of(&self, instance_id: &str) -> Option<String> {
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

    fn reg() -> Registry {
        Registry::new(Duration::from_secs(60))
    }

    fn s(v: &[&str]) -> Vec<String> {
        v.iter().map(|x| x.to_string()).collect()
    }

    fn two_instances(r: &Registry, types: &[&str]) {
        r.register("a", "svc", &s(types), &[]).unwrap();
        r.register("b", "svc", &s(types), &[]).unwrap();
    }

    #[test]
    fn pick_unknown_type_is_unknown_type() {
        assert_eq!(reg().pick("nope", ""), Err(PickError::UnknownType));
    }

    #[test]
    fn command_with_key_is_sticky_across_many_picks() {
        let r = reg();
        two_instances(&r, &["t.X"]);
        let first = r.pick("t.X", "k1").unwrap();
        assert_eq!(first.0, Kind::Command);
        for _ in 0..50 {
            assert_eq!(r.pick("t.X", "k1").unwrap(), first);
        }
    }

    #[test]
    fn command_with_key_distributes_over_keys() {
        let r = reg();
        two_instances(&r, &["t.X"]);
        let mut a = 0;
        let mut b = 0;
        for i in 0..1000 {
            match r.pick("t.X", &format!("key-{i}")).unwrap().1.as_str() {
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
        let ids: Vec<_> = (0..4).map(|_| r.pick("t.X", "").unwrap().1).collect();
        assert_eq!(ids, ["a", "b", "a", "b"]);
    }

    #[test]
    fn query_ignores_routing_key_and_round_robins() {
        let r = reg();
        r.register("a", "svc", &[], &s(&["q.X"])).unwrap();
        r.register("b", "svc", &[], &s(&["q.X"])).unwrap();
        let first = r.pick("q.X", "same").unwrap();
        let second = r.pick("q.X", "same").unwrap();
        assert_eq!(first, (Kind::Query, "a".to_string()));
        assert_eq!(second, (Kind::Query, "b".to_string()));
    }

    #[test]
    fn second_service_registering_owned_type_is_rejected_atomically() {
        let r = reg();
        r.register("a1", "A", &s(&["t.X"]), &[]).unwrap();
        let err = r.register("b1", "B", &s(&["t.X", "t.Y"]), &[]).unwrap_err();
        assert_eq!(
            err,
            RegisterError::TypeOwnedByOtherService {
                type_name: "t.X".into(),
                owner: "A".into()
            }
        );
        assert_eq!(r.pick("t.Y", ""), Err(PickError::UnknownType));
        assert_eq!(r.service_of("b1"), None);
    }

    #[test]
    fn same_service_second_instance_is_allowed() {
        let r = reg();
        r.register("a1", "A", &s(&["t.X"]), &[]).unwrap();
        r.register("a2", "A", &s(&["t.X"]), &[]).unwrap();
        assert_eq!(r.snapshot().services[0].instances, 2);
    }

    #[test]
    fn reregister_with_smaller_list_removes_type() {
        let r = reg();
        let now = Instant::now();
        r.register("a", "A", &s(&["t.X", "t.Y"]), &[]).unwrap();
        r.register("a", "A", &s(&["t.X"]), &[]).unwrap();
        assert!(r.pick("t.X", "").is_ok());
        assert_eq!(
            r.pick("t.Y", ""),
            Err(PickError::NoLiveInstance {
                service: "A".into()
            })
        );
        r.sweep(now + Duration::from_secs(61));
        assert_eq!(r.pick("t.Y", ""), Err(PickError::UnknownType));
        assert!(r.pick("t.X", "").is_ok());
    }

    #[test]
    fn last_instance_leaving_keeps_ownership_during_grace() {
        let r = reg();
        r.register("a", "A", &s(&["t.X"]), &[]).unwrap();
        let t0 = Instant::now();
        assert_eq!(r.unregister("a", t0), Some("A".to_string()));
        let no_live = Err(PickError::NoLiveInstance {
            service: "A".into(),
        });
        assert_eq!(r.pick("t.X", ""), no_live);
        assert_eq!(r.sweep(t0 + Duration::from_secs(59)), 0);
        assert_eq!(r.pick("t.X", ""), no_live);
        assert_eq!(r.sweep(t0 + Duration::from_secs(61)), 1);
        assert_eq!(r.pick("t.X", ""), Err(PickError::UnknownType));
    }

    #[test]
    fn instance_rejoining_within_grace_restores_routing() {
        let r = reg();
        r.register("a", "A", &s(&["t.X"]), &[]).unwrap();
        let t0 = Instant::now();
        r.unregister("a", t0);
        r.register("a2", "A", &s(&["t.X"]), &[]).unwrap();
        assert_eq!(r.sweep(t0 + Duration::from_secs(61)), 0);
        assert_eq!(r.pick("t.X", "").unwrap().1, "a2");
    }

    #[test]
    fn removing_one_instance_moves_only_its_keys() {
        let r = reg();
        for id in ["a", "b", "c"] {
            r.register(id, "svc", &s(&["t.X"]), &[]).unwrap();
        }
        let keys: Vec<String> = (0..1000).map(|i| format!("k{i}")).collect();
        let before: Vec<_> = keys.iter().map(|k| r.pick("t.X", k).unwrap().1).collect();
        r.unregister("c", Instant::now());
        for (k, was) in keys.iter().zip(before) {
            let now = r.pick("t.X", k).unwrap().1;
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
        r.register("a", "svc", &s(&["b.X", "a.Y"]), &s(&["q.Z"]))
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
        r.unregister("a", t0);
        let info = r.topology(t0).types.remove(0);
        assert_eq!((info.live_instances, info.orphaned_for), (1, None));
        r.unregister("b", t0);
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
        r.register("a", "svc", &s(&["t.X"]), &[]).unwrap();
        let t0 = Instant::now();
        r.unregister("a", t0);
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
            .register("a", "A", &s(&["t.X"]), &s(&["t.X"]))
            .unwrap_err();
        assert_eq!(
            err,
            RegisterError::DuplicateTypeAcrossKinds {
                type_name: "t.X".into()
            }
        );
        assert_eq!(r.service_of("a"), None);
        assert!(r.topology(Instant::now()).types.is_empty());
    }

    #[test]
    fn register_kind_conflict_with_owner_is_rejected() {
        let r = reg();
        r.register("a", "A", &s(&["t.X"]), &[]).unwrap();
        let err = r.register("b", "B", &[], &s(&["t.X"])).unwrap_err();
        assert_eq!(
            err,
            RegisterError::KindConflict {
                type_name: "t.X".into(),
                owner: "A".into(),
                owner_kind: Kind::Command
            }
        );
        assert_eq!(err.to_string(), "type t.X is owned as COMMAND by service A");
        assert_eq!(r.service_of("b"), None);
    }

    #[test]
    fn same_service_reregister_with_other_kind_is_rejected() {
        let r = reg();
        r.register("a", "A", &s(&["t.X"]), &[]).unwrap();
        let err = r.register("a2", "A", &[], &s(&["t.X"])).unwrap_err();
        assert!(matches!(err, RegisterError::KindConflict { .. }));
        assert_eq!(r.service_of("a2"), None);
    }
}
