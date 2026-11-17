//! Per-instance request queue for flow-controlled instances (roadmap P1, ADR 0014).
//!
//! Ordered by priority (higher first), then arrival (FIFO within a priority). Bounded by a
//! number of entries and a number of bytes per instance, and by the bytes queued over all
//! instances of the hub ([`TotalBytes`]); entries can be removed by request id (a queued request
//! that times out, or whose sender left).

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::proto::ServerMessage;

/// A request waiting for a permit of its target instance.
#[derive(Debug)]
pub struct Queued {
    pub id: String,
    pub type_name: String,
    pub priority: i32,
    /// Encoded size, counted against the byte cap.
    pub bytes: usize,
    pub enqueued_at: Instant,
    /// The `ServerMessage::Request` to forward once a permit is available.
    pub msg: ServerMessage,
}

/// Which cap a rejected push hit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Full {
    Count,
    Bytes,
    /// The hub-wide cap (`SNODRA_SERVER_QUEUE_BYTES_TOTAL`).
    TotalBytes,
}

impl Full {
    /// Label of the `reason` attribute of `snodra_server_rejected_total`.
    pub fn reason(self) -> &'static str {
        match self {
            Full::Count => "queue_full",
            Full::Bytes => "queue_bytes",
            Full::TotalBytes => "queue_bytes_total",
        }
    }
}

/// Bytes queued over every instance queue of the hub, bounded by one cap, so many busy
/// instances cannot together take more memory than the hub is given.
#[derive(Debug)]
pub struct TotalBytes {
    used: AtomicUsize,
    max: usize,
}

impl TotalBytes {
    pub fn new(max: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            max,
        })
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    fn try_add(&self, n: usize) -> bool {
        self.used
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |u| {
                u.checked_add(n).filter(|t| *t <= self.max)
            })
            .is_ok()
    }

    fn sub(&self, n: usize) {
        self.used.fetch_sub(n, Ordering::SeqCst);
    }
}

type Key = (Reverse<i32>, u64);

/// Priority queue with removal by id and count and byte caps.
#[derive(Debug)]
pub struct InstanceQueue {
    entries: BTreeMap<Key, Queued>,
    keys: HashMap<String, Key>,
    /// Entries per type, for the depth gauge (a type with none is removed).
    types: HashMap<String, usize>,
    bytes: usize,
    next_seq: u64,
    max_len: usize,
    max_bytes: usize,
    total: Arc<TotalBytes>,
}

impl InstanceQueue {
    /// A queue that only has its own caps.
    pub fn new(max_len: usize, max_bytes: usize) -> Self {
        Self::with_total(max_len, max_bytes, TotalBytes::new(usize::MAX))
    }

    /// A queue that also counts against (and is bounded by) the hub-wide `total`.
    pub fn with_total(max_len: usize, max_bytes: usize, total: Arc<TotalBytes>) -> Self {
        Self {
            entries: BTreeMap::new(),
            keys: HashMap::new(),
            types: HashMap::new(),
            bytes: 0,
            next_seq: 0,
            max_len,
            max_bytes,
            total,
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Entries queued per type.
    pub fn by_type(&self) -> impl Iterator<Item = (&str, usize)> {
        self.types.iter().map(|(t, n)| (t.as_str(), *n))
    }

    fn count_type(&mut self, type_name: &str) {
        match self.types.get_mut(type_name) {
            Some(n) => *n += 1,
            None => {
                self.types.insert(type_name.to_string(), 1);
            }
        }
    }

    fn uncount_type(&mut self, type_name: &str) {
        if let Some(n) = self.types.get_mut(type_name) {
            *n -= 1;
            if *n == 0 {
                self.types.remove(type_name);
            }
        }
    }

    /// Appends behind every entry of the same or higher priority; on a full queue returns the
    /// entry back with the cap it hit and leaves the queue unchanged.
    pub fn push(&mut self, entry: Queued) -> Result<(), (Full, Box<Queued>)> {
        if self.entries.len() >= self.max_len {
            return Err((Full::Count, Box::new(entry)));
        }
        if self.bytes.saturating_add(entry.bytes) > self.max_bytes {
            return Err((Full::Bytes, Box::new(entry)));
        }
        if !self.total.try_add(entry.bytes) {
            return Err((Full::TotalBytes, Box::new(entry)));
        }
        let key = (Reverse(entry.priority), self.next_seq);
        self.next_seq += 1;
        self.bytes += entry.bytes;
        self.count_type(&entry.type_name);
        self.keys.insert(entry.id.clone(), key);
        self.entries.insert(key, entry);
        Ok(())
    }

    /// The highest-priority, oldest entry.
    pub fn pop(&mut self) -> Option<Queued> {
        let (_, entry) = self.entries.pop_first()?;
        self.keys.remove(&entry.id);
        self.uncount_type(&entry.type_name);
        self.bytes -= entry.bytes;
        self.total.sub(entry.bytes);
        Some(entry)
    }

    pub fn remove(&mut self, id: &str) -> Option<Queued> {
        let key = self.keys.remove(id)?;
        let entry = self.entries.remove(&key)?;
        self.uncount_type(&entry.type_name);
        self.bytes -= entry.bytes;
        self.total.sub(entry.bytes);
        Some(entry)
    }

    /// Empties the queue, in dequeue order.
    pub fn drain(&mut self) -> Vec<Queued> {
        std::iter::from_fn(|| self.pop()).collect()
    }
}

impl Drop for InstanceQueue {
    /// A queue dropped with entries (its session's flow state went) gives their bytes back.
    fn drop(&mut self) {
        self.total.sub(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Request, server_message};

    fn item(id: &str, priority: i32, bytes: usize) -> Queued {
        Queued {
            id: id.to_string(),
            type_name: "t.X".to_string(),
            priority,
            bytes,
            enqueued_at: Instant::now(),
            msg: ServerMessage {
                message: Some(server_message::Message::Request(Request {
                    id: id.to_string(),
                    ..Request::default()
                })),
            },
        }
    }

    fn ids(q: &mut InstanceQueue) -> Vec<String> {
        std::iter::from_fn(|| q.pop()).map(|e| e.id).collect()
    }

    #[test]
    fn higher_priority_first_fifo_within_a_priority() {
        let mut q = InstanceQueue::new(100, 1 << 20);
        for (id, p) in [("a", 0), ("b", 5), ("c", 0), ("d", 5), ("e", -1), ("f", 9)] {
            q.push(item(id, p, 10)).unwrap();
        }
        assert_eq!(q.len(), 6);
        assert_eq!(ids(&mut q), ["f", "b", "d", "a", "c", "e"]);
        assert_eq!((q.len(), q.bytes()), (0, 0));
    }

    #[test]
    fn remove_by_id_keeps_the_order_of_the_rest() {
        let mut q = InstanceQueue::new(100, 1 << 20);
        for (id, p) in [("a", 1), ("b", 1), ("c", 2)] {
            q.push(item(id, p, 10)).unwrap();
        }
        assert_eq!(q.remove("b").map(|e| e.id), Some("b".to_string()));
        assert!(q.remove("b").is_none());
        assert_eq!(q.bytes(), 20);
        assert_eq!(ids(&mut q), ["c", "a"]);
    }

    #[test]
    fn count_cap_rejects_without_changing_the_queue() {
        let mut q = InstanceQueue::new(2, 1 << 20);
        q.push(item("a", 0, 1)).unwrap();
        q.push(item("b", 0, 1)).unwrap();
        let rejected = q.push(item("c", 9, 1)).unwrap_err();
        assert_eq!(rejected.0, Full::Count);
        assert_eq!(rejected.1.id, "c");
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn byte_cap_rejects_an_entry_that_would_exceed_it() {
        let mut q = InstanceQueue::new(100, 100);
        q.push(item("a", 0, 60)).unwrap();
        assert_eq!(q.push(item("b", 0, 41)).unwrap_err().0, Full::Bytes);
        q.push(item("c", 0, 40)).unwrap();
        assert_eq!(q.bytes(), 100);
        // A single entry larger than the whole cap is rejected even on an empty queue.
        let mut empty = InstanceQueue::new(100, 100);
        assert_eq!(empty.push(item("big", 0, 101)).unwrap_err().0, Full::Bytes);
    }

    #[test]
    fn the_total_byte_cap_spans_all_queues_and_is_released_by_pop_remove_and_drop() {
        let total = TotalBytes::new(100);
        let mut a = InstanceQueue::with_total(100, 1 << 20, Arc::clone(&total));
        let mut b = InstanceQueue::with_total(100, 1 << 20, Arc::clone(&total));
        a.push(item("a1", 0, 60)).unwrap();
        assert_eq!(b.push(item("b1", 0, 41)).unwrap_err().0, Full::TotalBytes);
        b.push(item("b2", 0, 40)).unwrap();
        assert_eq!(total.used(), 100);
        a.pop().unwrap();
        b.remove("b2").unwrap();
        assert_eq!(total.used(), 0);
        b.push(item("b3", 0, 70)).unwrap();
        drop(b);
        assert_eq!(total.used(), 0, "a dropped queue releases its bytes");
        assert_eq!(Full::TotalBytes.reason(), "queue_bytes_total");
    }

    /// The queue keeps its count per type, so the depth gauge never walks the entries.
    #[test]
    fn counts_per_type_follow_push_pop_remove_and_rejections() {
        let typed = |id: &str, t: &str| Queued {
            type_name: t.to_string(),
            ..item(id, 0, 1)
        };
        let mut q = InstanceQueue::new(3, 1 << 20);
        q.push(typed("a", "t.A")).unwrap();
        q.push(typed("b", "t.B")).unwrap();
        q.push(typed("c", "t.A")).unwrap();
        assert!(q.push(typed("d", "t.C")).is_err());
        let mut counts: Vec<(&str, usize)> = q.by_type().collect();
        counts.sort();
        assert_eq!(counts, [("t.A", 2), ("t.B", 1)]);
        q.pop().unwrap();
        q.remove("b").unwrap();
        assert_eq!(q.by_type().collect::<Vec<_>>(), [("t.A", 1)]);
        q.drain();
        assert_eq!(q.by_type().count(), 0, "a type with nothing queued is gone");
    }

    #[test]
    fn drain_returns_everything_in_order() {
        let mut q = InstanceQueue::new(100, 1 << 20);
        q.push(item("a", 0, 1)).unwrap();
        q.push(item("b", 3, 1)).unwrap();
        let drained: Vec<String> = q.drain().into_iter().map(|e| e.id).collect();
        assert_eq!(drained, ["b", "a"]);
        assert_eq!(q.len(), 0);
    }
}
