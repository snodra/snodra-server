//! In-memory registry of connected service instances and the message types they handle.
//!
//! Routing rules (docs/05-services/hub.md):
//! - commands with a routing key go to one instance of the owning service chosen by consistent
//!   hashing of the key (same key, same instance while membership is stable);
//! - commands without a key and all queries go to one instance chosen round-robin.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

pub type InstanceId = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instance {
    pub instance_id: InstanceId,
    pub service_name: String,
}

#[derive(Default)]
pub struct Registry {
    /// message type name -> instances that registered it
    by_type: Mutex<HashMap<String, Vec<Instance>>>,
    rr_counter: AtomicUsize,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers (or re-registers) an instance for the given types. A later call for the same
    /// instance replaces its previous registration.
    pub fn register(&self, instance: Instance, types: impl IntoIterator<Item = String>) {
        self.unregister(&instance.instance_id);
        let mut by_type = self.by_type.lock().unwrap();
        for t in types {
            by_type.entry(t).or_default().push(instance.clone());
        }
    }

    /// Removes an instance from every type. Called on stream close or missed heartbeats.
    pub fn unregister(&self, instance_id: &str) {
        self.by_type.lock().unwrap().retain(|_, instances| {
            instances.retain(|i| i.instance_id != instance_id);
            !instances.is_empty()
        });
    }

    /// Picks the instance that should handle `message_type`, or `None` when nobody registered it.
    pub fn pick_owner(&self, message_type: &str, routing_key: &str) -> Option<Instance> {
        let by_type = self.by_type.lock().unwrap();
        let instances = by_type.get(message_type)?;
        if instances.is_empty() {
            return None;
        }
        let idx = if routing_key.is_empty() {
            self.rr_counter.fetch_add(1, Ordering::Relaxed) % instances.len()
        } else {
            let mut hasher = DefaultHasher::new();
            routing_key.hash(&mut hasher);
            (hasher.finish() as usize) % instances.len()
        };
        instances.get(idx).cloned()
    }

    pub fn registered_types(&self) -> Vec<String> {
        self.by_type.lock().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inst(id: &str) -> Instance {
        Instance {
            instance_id: id.into(),
            service_name: "svc".into(),
        }
    }

    #[test]
    fn same_key_same_instance() {
        let r = Registry::new();
        r.register(inst("a"), ["t".to_string()]);
        r.register(inst("b"), ["t".to_string()]);
        let first = r.pick_owner("t", "k1").unwrap();
        for _ in 0..10 {
            assert_eq!(r.pick_owner("t", "k1").unwrap(), first);
        }
    }

    #[test]
    fn unknown_type_has_no_owner() {
        let r = Registry::new();
        assert!(r.pick_owner("nope", "").is_none());
    }

    #[test]
    fn unregister_removes_type() {
        let r = Registry::new();
        r.register(inst("a"), ["t".to_string()]);
        r.unregister("a");
        assert!(r.pick_owner("t", "").is_none());
    }
}
