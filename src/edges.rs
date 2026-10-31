//! Edge statistics for the dashboard graph (roadmap P2).
//!
//! An edge is `(sender service, type)`: every request that reaches a terminal state adds one
//! to its edge's count (and to its errors when the outcome is not `ok`) and, when the request
//! was routed, its latency to a small log-scale sketch. The owner side of the graph
//! (type -> owner service) is the merge of all edges of the type, so it is not stored.
//!
//! Window: [`SLOTS`] slots of `window / SLOTS` each (30 s for the default 5 minutes). A view
//! covers the current slot and the nine before it, so it reaches between 9/10 of the window and
//! the whole window back. An edge without a request in the window is dropped.
//!
//! Bound: at most `max_edges` edges (`KRAKEN_HUB_EDGE_STATS_MAX_EDGES`, default 10 000).
//! Expired edges are swept once per slot; a new edge at the cap then drops the least recently
//! used edges, 1 % of the cap at a time (at least one), and counts them in
//! [`EdgeSnapshot::evicted`]. Sender and type names longer than [`MAX_NAME_BYTES`] are cut.
//!
//! Everything sits behind one mutex that is taken for short synchronous work only, never
//! together with the router's other locks. Time is `tokio::time::Instant`, like the router's,
//! so tests run on paused time.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use tokio::time::Instant;
use xxhash_rust::xxh64::xxh64;

use crate::registry::Kind;

/// Slots per window: the window slides in tenths.
pub const SLOTS: u64 = 10;

/// Longest sender-service or type name kept in an edge key, in bytes. A longer name is cut at a
/// character boundary and ends with `…`, so a client cannot grow the map with huge names.
pub const MAX_NAME_BYTES: usize = 200;

/// Sketch resolution: bins per power of two. A quantile is reported as the geometric middle of
/// its bin, so it is off by at most `2^(1/16) - 1`, about 4.4 %.
const BINS_PER_OCTAVE: f64 = 8.0;

/// Bin of a latency: 0 for anything below a microsecond, else `1 + floor(8 * log2(us))`.
fn bin_of(d: Duration) -> u16 {
    let us = d.as_micros();
    if us == 0 {
        return 0;
    }
    let bin = ((us as f64).log2() * BINS_PER_OCTAVE).floor() as i64 + 1;
    bin.clamp(1, i64::from(u16::MAX)) as u16
}

/// The latency a bin stands for: the geometric middle of its range.
fn value_of(bin: u16) -> Duration {
    if bin == 0 {
        return Duration::ZERO;
    }
    let us = 2f64.powf((f64::from(bin) - 0.5) / BINS_PER_OCTAVE);
    Duration::from_secs_f64(us / 1e6)
}

/// Sparse log-scale histogram of one slot: `(bin, count)` sorted by bin. An edge whose latency
/// stays within one order of magnitude holds about 27 bins per slot.
#[derive(Debug, Clone, Default)]
struct Sketch {
    bins: Vec<(u16, u32)>,
}

impl Sketch {
    fn record(&mut self, d: Duration) {
        let bin = bin_of(d);
        match self.bins.binary_search_by_key(&bin, |(b, _)| *b) {
            Ok(i) => self.bins[i].1 = self.bins[i].1.saturating_add(1),
            Err(i) => self.bins.insert(i, (bin, 1)),
        }
    }
}

/// Latency distribution over several slots or edges, for quantiles.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Latency {
    bins: BTreeMap<u16, u64>,
    samples: u64,
}

impl Latency {
    fn add_sketch(&mut self, s: &Sketch) {
        for &(bin, n) in &s.bins {
            *self.bins.entry(bin).or_default() += u64::from(n);
            self.samples += u64::from(n);
        }
    }

    /// Adds every sample of `other`.
    pub fn merge(&mut self, other: &Latency) {
        for (&bin, &n) in &other.bins {
            *self.bins.entry(bin).or_default() += n;
        }
        self.samples += other.samples;
    }

    /// Requests that contributed a latency (routed ones; immediate rejections do not).
    pub fn samples(&self) -> u64 {
        self.samples
    }

    /// The `q` quantile (0..=1), `None` without samples.
    pub fn quantile(&self, q: f64) -> Option<Duration> {
        if self.samples == 0 {
            return None;
        }
        let rank = ((q.clamp(0.0, 1.0) * self.samples as f64).ceil() as u64).clamp(1, self.samples);
        let mut seen = 0;
        for (&bin, &n) in &self.bins {
            seen += n;
            if seen >= rank {
                return Some(value_of(bin));
            }
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EdgeKey {
    pub sender: String,
    pub type_name: String,
}

#[derive(Debug, Clone)]
struct Slot {
    index: u64,
    count: u64,
    errors: u64,
    sketch: Sketch,
}

struct Edge {
    /// Kind of the latest request on the edge.
    kind: Kind,
    last_seen: Instant,
    /// Oldest first; only slots with requests.
    slots: VecDeque<Slot>,
}

impl Edge {
    /// Drops slots older than `oldest`; true while any slot is left.
    fn trim(&mut self, oldest: u64) -> bool {
        while self.slots.front().is_some_and(|s| s.index < oldest) {
            self.slots.pop_front();
        }
        !self.slots.is_empty()
    }
}

#[derive(Default)]
struct Inner {
    edges: HashMap<EdgeKey, Edge>,
    /// The slot of the last full sweep (one sweep per slot at most).
    swept_at: u64,
    evicted: u64,
}

impl Inner {
    fn sweep(&mut self, oldest: u64) {
        self.edges.retain(|_, e| e.trim(oldest));
    }

    /// Drops the `n` least recently used edges.
    fn evict(&mut self, n: usize) {
        let n = n.min(self.edges.len());
        if n == 0 {
            return;
        }
        let mut by_age: Vec<(Instant, EdgeKey)> = self
            .edges
            .iter()
            .map(|(k, e)| (e.last_seen, k.clone()))
            .collect();
        by_age.select_nth_unstable_by(n - 1, |a, b| a.cmp(b));
        for (_, key) in by_age.into_iter().take(n) {
            self.edges.remove(&key);
        }
        self.evicted += n as u64;
    }
}

/// One edge over the window.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeWindow {
    pub sender: String,
    pub type_name: String,
    pub kind: Kind,
    pub count: u64,
    pub errors: u64,
    pub latency: Latency,
}

/// All edges over the window, sorted by sender then type.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeSnapshot {
    pub window: Duration,
    pub max_edges: usize,
    /// Edges dropped at the cap since the hub started.
    pub evicted: u64,
    pub edges: Vec<EdgeWindow>,
}

/// Request and error counts over the window, without latencies (cheap, for the live summary).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WindowTotals {
    pub requests: u64,
    pub errors: u64,
    pub edges: usize,
    /// Order-independent hash of the edge keys: changes when an edge appears or goes.
    pub keys_hash: u64,
}

pub struct EdgeStats {
    window: Duration,
    slot: Duration,
    max_edges: usize,
    origin: Instant,
    inner: Mutex<Inner>,
}

/// Cuts `name` to at most [`MAX_NAME_BYTES`] bytes (plus the ellipsis).
fn clip(name: &str) -> String {
    if name.len() <= MAX_NAME_BYTES {
        return name.to_string();
    }
    let mut end = MAX_NAME_BYTES;
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &name[..end])
}

impl EdgeStats {
    /// `window` is split into [`SLOTS`] slots (at least a millisecond each); `max_edges` is at
    /// least 1.
    pub fn new(window: Duration, max_edges: usize) -> Self {
        let slot = (window / SLOTS as u32).max(Duration::from_millis(1));
        Self {
            window: slot * SLOTS as u32,
            slot,
            max_edges: max_edges.max(1),
            origin: Instant::now(),
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn window(&self) -> Duration {
        self.window
    }

    fn slot_index(&self, now: Instant) -> u64 {
        (now.saturating_duration_since(self.origin).as_nanos() / self.slot.as_nanos()) as u64
    }

    /// The oldest slot a view at slot `current` still covers.
    fn oldest_live(current: u64) -> u64 {
        current.saturating_sub(SLOTS - 1)
    }

    /// One request of `kind` from `sender` (service name) for `type_name` reached its terminal
    /// state at `now`. `error` is any outcome but `ok`. `latency` is `None` for a request the
    /// hub answered without routing it (counted, but kept out of the percentiles).
    pub fn record(
        &self,
        sender: &str,
        type_name: &str,
        kind: Kind,
        error: bool,
        latency: Option<Duration>,
        now: Instant,
    ) {
        let current = self.slot_index(now);
        let oldest = Self::oldest_live(current);
        let key = EdgeKey {
            sender: clip(sender),
            type_name: clip(type_name),
        };
        let mut inner = self.inner.lock().unwrap();
        if inner.swept_at != current {
            inner.sweep(oldest);
            inner.swept_at = current;
        }
        // Slots only expire at slot boundaries, so after this slot's sweep every edge left is
        // live: at the cap, the least recently used ones go.
        if !inner.edges.contains_key(&key) && inner.edges.len() >= self.max_edges {
            let batch = (self.max_edges / 100).max(1);
            inner.evict(batch);
        }
        let edge = inner.edges.entry(key).or_insert_with(|| Edge {
            kind,
            last_seen: now,
            slots: VecDeque::new(),
        });
        edge.kind = kind;
        edge.last_seen = edge.last_seen.max(now);
        // A caller that read the clock just before another one took the lock lands in the
        // newer slot: the slots stay ordered.
        if edge.slots.back().is_none_or(|s| s.index < current) {
            edge.slots.push_back(Slot {
                index: current,
                count: 0,
                errors: 0,
                sketch: Sketch::default(),
            });
        }
        edge.trim(oldest);
        let slot = edge.slots.back_mut().expect("slot pushed above");
        slot.count += 1;
        if error {
            slot.errors += 1;
        }
        if let Some(latency) = latency {
            slot.sketch.record(latency);
        }
    }

    /// Every edge with a request in the window at `now`. The slots are copied under the lock
    /// and merged after it is released.
    pub fn snapshot(&self, now: Instant) -> EdgeSnapshot {
        let oldest = Self::oldest_live(self.slot_index(now));
        let (raw, evicted) = {
            let inner = self.inner.lock().unwrap();
            let raw: Vec<(EdgeKey, Kind, Vec<Slot>)> = inner
                .edges
                .iter()
                .filter_map(|(k, e)| {
                    let live: Vec<Slot> = e
                        .slots
                        .iter()
                        .filter(|s| s.index >= oldest)
                        .cloned()
                        .collect();
                    (!live.is_empty()).then(|| (k.clone(), e.kind, live))
                })
                .collect();
            (raw, inner.evicted)
        };
        let mut edges: Vec<EdgeWindow> = raw
            .into_iter()
            .map(|(key, kind, slots)| {
                let mut latency = Latency::default();
                for s in &slots {
                    latency.add_sketch(&s.sketch);
                }
                EdgeWindow {
                    sender: key.sender,
                    type_name: key.type_name,
                    kind,
                    count: slots.iter().map(|s| s.count).sum(),
                    errors: slots.iter().map(|s| s.errors).sum(),
                    latency,
                }
            })
            .collect();
        edges.sort_by(|a, b| {
            (a.sender.as_str(), a.type_name.as_str())
                .cmp(&(b.sender.as_str(), b.type_name.as_str()))
        });
        EdgeSnapshot {
            window: self.window,
            max_edges: self.max_edges,
            evicted,
            edges,
        }
    }

    /// Requests and errors over the window, and a hash of the live edge keys.
    pub fn totals(&self, now: Instant) -> WindowTotals {
        let oldest = Self::oldest_live(self.slot_index(now));
        let inner = self.inner.lock().unwrap();
        let mut t = WindowTotals::default();
        for (key, edge) in &inner.edges {
            let mut live = edge.slots.iter().filter(|s| s.index >= oldest).peekable();
            if live.peek().is_none() {
                continue;
            }
            for s in live {
                t.requests += s.count;
                t.errors += s.errors;
            }
            t.edges += 1;
            let mut bytes = Vec::with_capacity(key.sender.len() + key.type_name.len() + 1);
            bytes.extend_from_slice(key.sender.as_bytes());
            bytes.push(0);
            bytes.extend_from_slice(key.type_name.as_bytes());
            t.keys_hash ^= xxh64(&bytes, 0);
        }
        t
    }

    /// Edges currently held (expired ones included until the next sweep).
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().edges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(300);

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn rec(stats: &EdgeStats, sender: &str, type_name: &str, error: bool, latency: Option<u64>) {
        stats.record(
            sender,
            type_name,
            Kind::Command,
            error,
            latency.map(ms),
            Instant::now(),
        );
    }

    fn edge<'a>(snap: &'a EdgeSnapshot, sender: &str, type_name: &str) -> &'a EdgeWindow {
        snap.edges
            .iter()
            .find(|e| e.sender == sender && e.type_name == type_name)
            .unwrap_or_else(|| panic!("no edge {sender} -> {type_name}: {snap:?}"))
    }

    #[tokio::test(start_paused = true)]
    async fn counts_requests_and_errors_per_sender_and_type() {
        let stats = EdgeStats::new(WINDOW, 100);
        for _ in 0..3 {
            rec(&stats, "web", "orders.Place", false, Some(5));
        }
        rec(&stats, "web", "orders.Place", true, Some(7));
        rec(&stats, "web", "orders.Get", false, Some(1));
        rec(&stats, "batch", "orders.Place", true, None);
        let snap = stats.snapshot(Instant::now());
        assert_eq!(snap.edges.len(), 3);
        let place = edge(&snap, "web", "orders.Place");
        assert_eq!((place.count, place.errors), (4, 1));
        assert_eq!(place.latency.samples(), 4);
        let get = edge(&snap, "web", "orders.Get");
        assert_eq!((get.count, get.errors), (1, 0));
        // An immediate rejection counts, but has no latency.
        let batch = edge(&snap, "batch", "orders.Place");
        assert_eq!(
            (batch.count, batch.errors, batch.latency.samples()),
            (1, 1, 0)
        );
        assert_eq!(batch.latency.quantile(0.5), None);
        // Sorted by sender, then type.
        let order: Vec<_> = snap
            .edges
            .iter()
            .map(|e| (e.sender.as_str(), e.type_name.as_str()))
            .collect();
        assert_eq!(
            order,
            [
                ("batch", "orders.Place"),
                ("web", "orders.Get"),
                ("web", "orders.Place")
            ]
        );
        let totals = stats.totals(Instant::now());
        assert_eq!((totals.requests, totals.errors, totals.edges), (6, 2, 3));
    }

    #[tokio::test(start_paused = true)]
    async fn percentiles_are_within_five_percent() {
        let stats = EdgeStats::new(WINDOW, 100);
        // 1 ms .. 1000 ms, uniformly: p50 = 500 ms, p99 = 990 ms.
        for n in 1..=1000 {
            rec(&stats, "web", "t.X", false, Some(n));
        }
        let snap = stats.snapshot(Instant::now());
        let latency = &edge(&snap, "web", "t.X").latency;
        let within = |got: Duration, want: f64| {
            let got = got.as_secs_f64() * 1000.0;
            assert!(
                (got - want).abs() <= want * 0.05,
                "got {got} ms, want {want} ms +- 5 %"
            );
        };
        within(latency.quantile(0.5).unwrap(), 500.0);
        within(latency.quantile(0.99).unwrap(), 990.0);
        within(latency.quantile(1.0).unwrap(), 1000.0);
        within(latency.quantile(0.0).unwrap(), 1.0);
        // Sub-microsecond latencies are one bin, reported as zero.
        assert_eq!(value_of(bin_of(Duration::from_nanos(300))), Duration::ZERO);
    }

    #[tokio::test]
    async fn every_bin_is_within_its_error_bound() {
        for us in [
            1u64, 2, 3, 7, 10, 999, 1000, 1001, 65_432, 1_000_000, 59_999_999,
        ] {
            let d = Duration::from_micros(us);
            let got = value_of(bin_of(d)).as_secs_f64();
            let want = d.as_secs_f64();
            assert!(
                (got - want).abs() <= want * 0.045,
                "{us} us reads as {got} s"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_window_slides_and_expired_edges_go() {
        let stats = EdgeStats::new(WINDOW, 100);
        rec(&stats, "web", "t.Old", false, Some(1));
        tokio::time::advance(Duration::from_secs(200)).await;
        rec(&stats, "web", "t.New", true, Some(1));
        let snap = stats.snapshot(Instant::now());
        assert_eq!(snap.edges.len(), 2);
        assert_eq!(snap.window, WINDOW);

        // 330 s after the first request its slot (0-30 s) is out of the window; the second
        // request (200 s) is still in.
        tokio::time::advance(Duration::from_secs(130)).await;
        let snap = stats.snapshot(Instant::now());
        assert_eq!(snap.edges.len(), 1, "{snap:?}");
        assert_eq!(snap.edges[0].type_name, "t.New");
        assert_eq!(stats.totals(Instant::now()).requests, 1);

        // The next request sweeps the map: only live edges remain.
        rec(&stats, "web", "t.Other", false, Some(1));
        assert_eq!(stats.len(), 2);

        tokio::time::advance(WINDOW).await;
        assert!(stats.snapshot(Instant::now()).edges.is_empty());
        assert_eq!(stats.totals(Instant::now()), WindowTotals::default());
        rec(&stats, "web", "t.Fresh", false, Some(1));
        assert_eq!(stats.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn counts_within_one_edge_expire_slot_by_slot() {
        let stats = EdgeStats::new(WINDOW, 100);
        rec(&stats, "web", "t.X", false, Some(1));
        tokio::time::advance(Duration::from_secs(60)).await;
        rec(&stats, "web", "t.X", true, Some(1));
        rec(&stats, "web", "t.X", false, Some(1));
        let count = |s: &EdgeStats| {
            let snap = s.snapshot(Instant::now());
            snap.edges.first().map_or((0, 0), |e| (e.count, e.errors))
        };
        assert_eq!(count(&stats), (3, 1));
        tokio::time::advance(Duration::from_secs(250)).await; // t = 310 s
        assert_eq!(count(&stats), (2, 1));
        tokio::time::advance(Duration::from_secs(60)).await; // t = 370 s
        assert_eq!(count(&stats), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn at_the_cap_the_least_recently_used_edge_is_dropped() {
        let stats = EdgeStats::new(WINDOW, 3);
        for t in ["t.A", "t.B", "t.C"] {
            rec(&stats, "web", t, false, Some(1));
            tokio::time::advance(ms(10)).await;
        }
        // A is the oldest edge, but it is used again: B becomes the least recently used.
        rec(&stats, "web", "t.A", false, Some(1));
        tokio::time::advance(ms(10)).await;
        rec(&stats, "web", "t.D", false, Some(1));
        let snap = stats.snapshot(Instant::now());
        let types: Vec<_> = snap.edges.iter().map(|e| e.type_name.as_str()).collect();
        assert_eq!(types, ["t.A", "t.C", "t.D"]);
        assert_eq!(snap.evicted, 1);
        assert_eq!(snap.max_edges, 3);
        // An existing edge never evicts.
        rec(&stats, "web", "t.C", false, Some(1));
        assert_eq!(stats.snapshot(Instant::now()).evicted, 1);
        tokio::time::advance(ms(10)).await;
        rec(&stats, "web", "t.E", false, Some(1));
        let snap = stats.snapshot(Instant::now());
        let types: Vec<_> = snap.edges.iter().map(|e| e.type_name.as_str()).collect();
        assert_eq!(types, ["t.C", "t.D", "t.E"]);
        assert_eq!(snap.evicted, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn at_the_cap_expired_edges_go_before_live_ones() {
        let stats = EdgeStats::new(WINDOW, 2);
        rec(&stats, "web", "t.A", false, None);
        tokio::time::advance(Duration::from_secs(301)).await;
        rec(&stats, "web", "t.B", false, None);
        tokio::time::advance(ms(10)).await;
        rec(&stats, "web", "t.C", false, None);
        let snap = stats.snapshot(Instant::now());
        assert_eq!(snap.edges.len(), 2);
        assert_eq!(snap.evicted, 0, "an expired edge is swept, not evicted");
    }

    #[tokio::test(start_paused = true)]
    async fn a_large_cap_drops_one_percent_at_a_time() {
        let stats = EdgeStats::new(WINDOW, 200);
        for i in 0..200 {
            rec(&stats, "web", &format!("t.{i:03}"), false, None);
            tokio::time::advance(ms(1)).await;
        }
        rec(&stats, "web", "t.new", false, None);
        let snap = stats.snapshot(Instant::now());
        assert_eq!(snap.evicted, 2);
        assert_eq!(snap.edges.len(), 199);
        assert!(
            snap.edges
                .iter()
                .all(|e| e.type_name != "t.000" && e.type_name != "t.001")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn long_names_are_cut_at_a_char_boundary() {
        let stats = EdgeStats::new(WINDOW, 10);
        let long = "é".repeat(150); // 300 bytes
        rec(&stats, &long, &long, false, None);
        rec(&stats, &long, &long, false, None);
        let snap = stats.snapshot(Instant::now());
        assert_eq!(snap.edges.len(), 1);
        let e = &snap.edges[0];
        assert_eq!(e.count, 2);
        assert!(e.type_name.ends_with('…'));
        assert!(e.type_name.len() <= MAX_NAME_BYTES + '…'.len_utf8());
        assert_eq!(e.sender, e.type_name);
    }

    #[tokio::test(start_paused = true)]
    async fn merged_latency_combines_senders() {
        let stats = EdgeStats::new(WINDOW, 10);
        for _ in 0..99 {
            rec(&stats, "a", "t.X", false, Some(10));
        }
        rec(&stats, "b", "t.X", false, Some(1000));
        let snap = stats.snapshot(Instant::now());
        let mut merged = Latency::default();
        for e in &snap.edges {
            merged.merge(&e.latency);
        }
        assert_eq!(merged.samples(), 100);
        let p99 = merged.quantile(0.99).unwrap().as_secs_f64() * 1000.0;
        assert!((p99 - 10.0).abs() < 0.5, "{p99}");
        let max = merged.quantile(1.0).unwrap().as_secs_f64() * 1000.0;
        assert!((max - 1000.0).abs() < 45.0, "{max}");
    }

    #[tokio::test(start_paused = true)]
    async fn the_key_hash_changes_with_the_edge_set_only() {
        let stats = EdgeStats::new(WINDOW, 10);
        rec(&stats, "a", "t.X", false, None);
        let h1 = stats.totals(Instant::now()).keys_hash;
        rec(&stats, "a", "t.X", true, None);
        assert_eq!(stats.totals(Instant::now()).keys_hash, h1);
        rec(&stats, "a", "t.Y", false, None);
        assert_ne!(stats.totals(Instant::now()).keys_hash, h1);
    }
}
