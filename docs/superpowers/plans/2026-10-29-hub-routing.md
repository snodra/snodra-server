# kraken-hub Routing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the kraken-hub skeleton into the working command/query router described in the umbrella docs: registration with ownership rules, consistent-hash and round-robin routing, reply relay, deadlines, heartbeat eviction, health service, metrics and traces.

**Architecture:** One tonic bidirectional stream per service instance. A `Registry` (types to owner, services to instances plus hash ring) and a `Sessions` map (instance to outbound channel) are shared behind mutexes. A `Router` forwards each `Request` to a picked instance, records it in a `pending` map, spawns a deadline task, and relays the matching `Reply` or an `Error`. The crate becomes lib + bin so integration tests drive a real server with fake clients built from the generated tonic client.

**Tech Stack:** Rust 1.99.1 (edition 2024), tonic 0.14.8, prost 0.14.4, tokio 1.54.1, tonic-health 0.14, xxhash-rust 0.8 (xxh64), opentelemetry 0.33.1 / opentelemetry-otlp 0.33.1 / tracing-opentelemetry 0.34.0, tracing-subscriber 0.3.

**Spec:** `../kraken/docs/05-services/hub.md` (behaviour, timing, config, metrics), `../kraken/docs/02-messaging.md` section "Hub protocol walkthrough" and "Error semantics summary", `proto/hub.proto` (copy of `../kraken/protocol/hub.proto`).

## Global Constraints

- Rust toolchain pinned by `rust-toolchain.toml` to `1.99.1`; `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` must pass (CI runs both).
- Listen default `0.0.0.0:7000`; all config via `KRAKEN_HUB_*` env vars with the defaults in the spec table: `DEFAULT_TIMEOUT_MS=5000`, `MAX_TIMEOUT_MS=60000`, `HEARTBEAT_TIMEOUT=30s`, `OWNER_GRACE=60s`, `SESSION_BUFFER=1024`, `LOG=info`, `OTLP_ENDPOINT=http://otel-lgtm:4317`.
- Payload bytes are never logged and never inspected.
- The hub retries nothing. Exactly one terminal message (`Reply` or `Error`) per `Request.id` reaches the sender.
- Error codes produced by the hub: `NO_HANDLER`, `TIMEOUT`, `HANDLER_DISCONNECTED`; the "handler busy" case is `HANDLER_ERROR` with message `handler busy`.
- Commit `Cargo.lock` (binary crate) and build the image with `cargo build --release --locked`.
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Builder call (this plan): the hub compiles the proto itself and does not depend on `kraken-messaging-rs`; the skeleton already does this and it keeps the hub buildable alone.
- Builder call (this plan): shared state uses `std::sync::Mutex` around plain maps instead of `DashMap`. Lock sections are tiny and the single-mutex model is easier to reason about; `dashmap` is removed from `Cargo.toml`.

## Review Focus

1. A client that sends `Request` or `Reply` before `Register` must get status `FAILED_PRECONDITION` and a closed stream, not a panic or a silent drop. Test in Task 5.
2. A `Reply` whose `request_id` is unknown (late reply after timeout, or garbage) must be ignored and logged at debug, never crash or be relayed. Test in Task 4.
3. A sender that disconnects while its own request is pending must have the pending entry dropped so the later reply is not sent to a dead channel and the entry does not leak. Test in Task 4.
4. `Request.timeout_ms` above the max must be clamped, zero must mean the default; a request with `timeout_ms = 1` must still produce exactly one `TIMEOUT` error even if the reply races it. Test in Task 4.
5. Re-`Register` on the same stream with a smaller type list must remove the dropped types for that instance (and the type ownership when it was the last instance), so routing to a removed type yields `NO_HANDLER` after the grace period. Test in Task 2.

---

### Task 1: Lib + bin split, config per spec, dependencies

**Files:**
- Create: `src/lib.rs`
- Modify: `src/main.rs`, `src/config.rs`, `Cargo.toml`, `Dockerfile`, `README.md`
- Test: `src/config.rs` (unit tests module)

**Interfaces:**
- Produces: `kraken_hub::config::Config { listen: SocketAddr, otlp_endpoint: Option<String>, log: String, default_timeout: Duration, max_timeout: Duration, heartbeat_timeout: Duration, owner_grace: Duration, session_buffer: usize }`, `Config::from_env() -> anyhow::Result<Config>`, `Config::default() -> Config` (spec defaults, `otlp_endpoint = None`), `pub fn parse_duration(s: &str) -> anyhow::Result<Duration>` accepting `"30"` (seconds), `"30s"`, `"500ms"`.
- Produces: `kraken_hub::proto` module (generated), re-exported from `lib.rs`.
- Produces: `pub async fn serve(config: Config, listener: tokio::net::TcpListener) -> anyhow::Result<()>` in `src/lib.rs` (body filled in Task 6; this task leaves it returning `Ok(())` after binding so the crate compiles).

- [ ] **Step 1: Install the toolchain and compile the skeleton once**

Run: `cargo build 2>&1 | tail -30`
Expected: compile errors from the skeleton (acceptable); record them, do not fix yet.

- [ ] **Step 2: Write failing config tests in `src/config.rs`**

```rust
#[test] fn defaults_match_spec() {
    let c = Config::default();
    assert_eq!(c.listen, "0.0.0.0:7000".parse().unwrap());
    assert_eq!(c.default_timeout, Duration::from_millis(5000));
    assert_eq!(c.max_timeout, Duration::from_millis(60_000));
    assert_eq!(c.heartbeat_timeout, Duration::from_secs(30));
    assert_eq!(c.owner_grace, Duration::from_secs(60));
    assert_eq!(c.session_buffer, 1024);
    assert_eq!(c.log, "info");
}
#[test] fn parse_duration_accepts_seconds_suffix_and_millis() {
    assert_eq!(parse_duration("30").unwrap(), Duration::from_secs(30));
    assert_eq!(parse_duration("45s").unwrap(), Duration::from_secs(45));
    assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
    assert!(parse_duration("abc").is_err());
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --lib config`
Expected: FAIL (missing `Default`, `parse_duration`, new fields).

- [ ] **Step 4: Implement**

Move `mod config; mod registry; mod service; pub mod proto {...}` into `src/lib.rs` as `pub mod`; make `src/main.rs` call `kraken_hub::telemetry::init(&config)` (stub until Task 7: plain fmt JSON subscriber) then `kraken_hub::serve(config, TcpListener::bind(config.listen).await?)`. In `Cargo.toml` add `tonic-health = "0.14"`, `xxhash-rust = { version = "0.8", features = ["xxh64"] }`, remove `dashmap`, `serde`, `serde_json` (unused by the hub), add `[lib] name = "kraken_hub"` and `[[bin]] name = "kraken-hub" path = "src/main.rs"`. Fix the skeleton compile errors reported in Step 1. In `Dockerfile` replace the `|| cargo build --release` fallback with `--locked` only. Update the README config table to the spec values.

- [ ] **Step 5: Run tests and lints**

Run: `cargo test --lib config && cargo clippy --all-targets -- -D warnings && cargo fmt --check`
Expected: PASS, no warnings.

- [ ] **Step 6: Commit (including `Cargo.lock`)**

```bash
git add -A && git commit -m "feat: lib/bin split, spec config defaults, commit Cargo.lock"
```

---

### Task 2: Registry with ownership, grace period and consistent-hash ring

**Files:**
- Modify: `src/registry.rs` (rewrite)
- Test: `src/registry.rs` unit tests

**Interfaces:**
- Produces:
  ```rust
  pub type InstanceId = String;
  #[derive(Clone, Copy, PartialEq, Eq, Debug)] pub enum Kind { Command, Query }
  #[derive(Debug, PartialEq, Eq)] pub enum RegisterError { TypeOwnedByOtherService { type_name: String, owner: String } }
  #[derive(Debug, PartialEq, Eq)] pub enum PickError { UnknownType, NoLiveInstance { service: String } }
  pub struct Registry { /* Mutex<Inner> */ }
  impl Registry {
      pub fn new(owner_grace: Duration) -> Self;
      /// Atomic: on error nothing changes. Replaces a previous registration of the same instance.
      pub fn register(&self, instance_id: &str, service: &str, command_types: &[String], query_types: &[String]) -> Result<(), RegisterError>;
      /// Returns the service name the instance belonged to, if it was registered.
      pub fn unregister(&self, instance_id: &str, now: Instant) -> Option<String>;
      pub fn pick(&self, type_name: &str, routing_key: &str) -> Result<(Kind, InstanceId), PickError>;
      /// Drops ownership of types whose service has had no instance for longer than owner_grace.
      pub fn sweep(&self, now: Instant) -> usize;
      pub fn service_of(&self, instance_id: &str) -> Option<String>;
      pub fn snapshot(&self) -> RegistrySnapshot; // for metrics: Vec<(service, instance_count, command_count, query_count)>
  }
  ```
- Ring: `struct Ring { points: BTreeMap<u64, InstanceId> }` with 128 virtual nodes per instance, key `xxh64(format!("{service}:{routing_key}"), 0)`, vnode point `xxh64(format!("{instance_id}#{i}"), 0)`; lookup = first point `>= hash`, wrapping to the first entry.
- Round-robin: a `usize` counter per service.

- [ ] **Step 1: Write the failing tests**

```rust
fn reg() -> Registry { Registry::new(Duration::from_secs(60)) }
#[test] fn pick_unknown_type_is_unknown_type()            // Err(PickError::UnknownType)
#[test] fn command_with_key_is_sticky_across_many_picks()  // two instances, 50 picks of "k1" all equal
#[test] fn command_with_key_distributes_over_keys()        // 1000 distinct keys, both instances receive >= 300
#[test] fn command_without_key_round_robins()               // instances a,b -> picks alternate a,b,a,b
#[test] fn query_ignores_routing_key_and_round_robins()     // Kind::Query, same key twice -> a then b
#[test] fn second_service_registering_owned_type_is_rejected_atomically()
    // svc A owns "t.X"; svc B registers ["t.X","t.Y"] -> Err(TypeOwnedByOtherService{type_name:"t.X", owner:"A"}); pick("t.Y") == Err(UnknownType)
#[test] fn same_service_second_instance_is_allowed()
#[test] fn reregister_with_smaller_list_removes_type()
    // a registers ["t.X","t.Y"]; a re-registers ["t.X"]; pick("t.Y") -> Err(NoLiveInstance{service}) before grace, Err(UnknownType) after sweep(now+61s)
#[test] fn last_instance_leaving_keeps_ownership_during_grace()
    // unregister(a, t0); pick -> NoLiveInstance; sweep(t0+59s)==0 and still NoLiveInstance; sweep(t0+61s)==1 -> UnknownType
#[test] fn instance_rejoining_within_grace_restores_routing()
#[test] fn removing_one_instance_moves_only_its_keys()
    // 3 instances, record owner for 1000 keys, unregister c; keys that were on a or b are unchanged
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test --lib registry`
Expected: FAIL to compile (new API).

- [ ] **Step 3: Implement the registry as specified in Interfaces**

Inner maps: `types: HashMap<String, Owner { service: String, kind: Kind }>`, `services: HashMap<String, ServiceEntry { instances: HashSet<InstanceId>, ring: Ring, rr: usize, orphaned_at: Option<Instant> }>`, `instances: HashMap<InstanceId, InstanceEntry { service: String, types: Vec<String> }>`. `register` validates every type first (owned by another service => error), then removes the instance's previous types that are not in the new list (deleting ownership only if no other instance of the service lists them), then inserts.

- [ ] **Step 4: Run tests and lints**

Run: `cargo test --lib registry && cargo clippy --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/registry.rs Cargo.toml Cargo.lock && git commit -m "feat(registry): ownership rules, owner grace, xxh64 consistent-hash ring"
```

---

### Task 3: Sessions

**Files:**
- Create: `src/session.rs`
- Modify: `src/lib.rs` (add `pub mod session;`)
- Test: `src/session.rs` unit tests

**Interfaces:**
- Produces:
  ```rust
  #[derive(Debug, PartialEq, Eq)] pub enum SendError { Gone, Busy }
  pub struct Sessions { /* Mutex<HashMap<InstanceId, Session>> */ }
  pub struct Session { pub service: String, pub tx: mpsc::Sender<Result<ServerMessage, Status>>, pub last_heartbeat: Instant }
  impl Sessions {
      pub fn new() -> Self;
      pub fn insert(&self, instance_id: &str, service: &str, tx: mpsc::Sender<Result<ServerMessage, Status>>);
      pub fn remove(&self, instance_id: &str) -> Option<Session>;
      /// try_send: full buffer -> Busy, closed -> Gone
      pub fn send(&self, instance_id: &str, msg: ServerMessage) -> Result<(), SendError>;
      pub fn touch(&self, instance_id: &str, now: Instant);
      /// instances whose last_heartbeat is older than `now - timeout`
      pub fn stale(&self, now: Instant, timeout: Duration) -> Vec<InstanceId>;
      pub fn count_by_service(&self) -> HashMap<String, usize>;
  }
  ```

- [ ] **Step 1: Write failing tests**

```rust
#[tokio::test] async fn send_to_unknown_instance_is_gone()
#[tokio::test] async fn send_with_full_buffer_is_busy()        // channel capacity 1, fill it, second send -> Busy
#[tokio::test] async fn send_to_dropped_receiver_is_gone()
#[test] fn stale_returns_only_instances_past_timeout()         // a touched now, b touched now-31s, timeout 30s -> ["b"]
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test --lib session` — Expected: compile FAIL.

- [ ] **Step 3: Implement per Interfaces**

- [ ] **Step 4: Run tests** — `cargo test --lib session` Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/session.rs src/lib.rs && git commit -m "feat(session): outbound session map with busy/gone semantics"
```

---

### Task 4: Router core (forward, reply relay, NO_HANDLER, busy)

**Files:**
- Create: `src/router.rs`
- Modify: `src/lib.rs`
- Test: `src/router.rs` unit tests (no network: fake sessions via mpsc receivers)

**Interfaces:**
- Produces:
  ```rust
  pub struct Router { registry: Arc<Registry>, sessions: Arc<Sessions>, pending: Mutex<HashMap<String, Pending>>, config: Arc<Config>, metrics: Arc<Metrics> /* Task 7, stub struct until then */ }
  struct Pending { sender: InstanceId, handler: InstanceId, kind: Kind, type_name: String, started: Instant, cancel: tokio::task::AbortHandle }
  impl Router {
      pub fn new(registry: Arc<Registry>, sessions: Arc<Sessions>, config: Arc<Config>, metrics: Arc<Metrics>) -> Arc<Self>;
      /// Called for every Request a sender wrote. Never returns an error to the caller: every failure becomes a ServerMessage::Error sent to `sender`.
      pub fn route(self: &Arc<Self>, sender: &str, req: Request);
      /// Reply from a handler; relays to the recorded sender if the id is pending and `from` is the recorded handler. Unknown or mismatched ids are ignored (debug log).
      pub fn on_reply(&self, from: &str, reply: Reply);
      /// Fails pending requests handled by this instance with HANDLER_DISCONNECTED; drops pending requests it sent.
      pub fn on_disconnect(&self, instance_id: &str);
      pub fn in_flight(&self) -> usize;
  }
  pub fn error_message(request_id: &str, code: ErrorCode, message: impl Into<String>) -> ServerMessage;
  ```
- Timeout: `route` computes `timeout = if req.timeout_ms == 0 { default } else { min(timeout_ms, max) }`, spawns `tokio::spawn(async { sleep(timeout).await; router.on_timeout(id) })` and stores its `AbortHandle`; `on_reply` aborts it. Messages: `NO_HANDLER` with `no handler registered for type {type}` or `owner known, no instance for service {service}`; `TIMEOUT` with `no reply within {ms} ms`; `HANDLER_DISCONNECTED` with `handler instance {id} disconnected`; busy: `HANDLER_ERROR` with `handler busy`.

- [ ] **Step 1: Write failing tests**

Helper: `fn fake(sessions: &Sessions, id: &str, service: &str, cap: usize) -> mpsc::Receiver<Result<ServerMessage, Status>>`.

```rust
#[tokio::test] async fn unknown_type_sends_no_handler_to_sender()
#[tokio::test] async fn forwards_request_unchanged_to_picked_handler()   // handler rx gets ServerMessage::Request equal to the input Request
#[tokio::test] async fn relays_reply_to_sender_and_clears_pending()      // in_flight() back to 0
#[tokio::test] async fn reply_from_wrong_instance_is_ignored()
#[tokio::test] async fn unknown_reply_id_is_ignored()
#[tokio::test(start_paused = true)] async fn timeout_sends_timeout_once_and_drops_late_reply()
    // timeout_ms=10; advance 11ms; sender gets exactly one Error{TIMEOUT}; then on_reply -> sender rx empty
#[tokio::test(start_paused = true)] async fn zero_timeout_uses_default_and_large_is_clamped()
    // default 5000: advance 4999 -> nothing, advance 2 -> TIMEOUT; timeout_ms=999_999 with max 60000: TIMEOUT at 60_001ms
#[tokio::test] async fn handler_disconnect_fails_its_pending_requests()
#[tokio::test] async fn sender_disconnect_drops_its_pending_requests()    // in_flight()==0, later reply ignored
#[tokio::test] async fn full_handler_buffer_yields_handler_error_busy()   // cap 1, pre-filled
#[tokio::test] async fn gone_handler_yields_handler_disconnected()        // receiver dropped before route
```

- [ ] **Step 2: Run to verify failure** — `cargo test --lib router` Expected: compile FAIL.

- [ ] **Step 3: Implement per Interfaces**

Use `Metrics` as an empty struct with no-op methods for now (`src/telemetry.rs` created with `pub struct Metrics; impl Metrics { pub fn noop() -> Arc<Self> }`), filled in Task 7.

- [ ] **Step 4: Run tests and lints** — `cargo test --lib && cargo clippy --all-targets -- -D warnings` Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/router.rs src/telemetry.rs src/lib.rs && git commit -m "feat(router): forward, relay, deadlines, disconnect handling"
```

---

### Task 5: Connect stream service and heartbeat watchdog

**Files:**
- Modify: `src/service.rs` (rewrite), `src/lib.rs` (`serve` body)
- Test: `tests/common/mod.rs`, `tests/connect.rs`

**Interfaces:**
- Produces: `pub struct HubService { pub registry: Arc<Registry>, pub sessions: Arc<Sessions>, pub router: Arc<Router>, pub config: Arc<Config> }` implementing `proto::hub_server::Hub`.
- Produces: `serve(config, listener)` builds registry, sessions, metrics, router, adds `HubServer` and `tonic_health` service (`kraken.hub.v1.Hub` set `Serving`), spawns the watchdog, serves with `Server::builder().serve_with_incoming(TcpListenerStream)`.
- Stream rules: first `ClientMessage` must be `Register`, else the session task sends `Status::failed_precondition("first message must be Register")` and ends the stream. `Register` conflict => `Status::already_exists(format!("type {t} is owned by service {owner}"))` and end. On successful `Register` (first or later): `sessions.insert` (first only), `registry.register`, log `register` with counts. `Heartbeat` => `sessions.touch` and echo. `Request` => `router.route(instance_id, req)`. `Reply` => `router.on_reply(instance_id, reply)`. Stream end or error => `router.on_disconnect`, `sessions.remove`, `registry.unregister(now)`.
- Watchdog: `tokio::spawn` loop every `heartbeat_timeout / 3` (10 s at defaults): for each `sessions.stale(now, heartbeat_timeout)` do the disconnect sequence above and drop the session's `tx` (which ends the client's inbound stream); also `registry.sweep(now)`.
- Test helper (`tests/common/mod.rs`):
  ```rust
  pub async fn start_hub(config: Config) -> SocketAddr;   // binds 127.0.0.1:0, spawns serve
  pub struct FakeClient { pub tx: mpsc::Sender<ClientMessage>, pub rx: Streaming<ServerMessage>, pub instance_id: String }
  impl FakeClient {
      pub async fn connect(addr: SocketAddr) -> Self;            // opens Connect without registering
      pub async fn register(addr: SocketAddr, service: &str, commands: &[&str], queries: &[&str]) -> Self;
      pub async fn next(&mut self) -> Option<Result<ServerMessage, Status>>;
      pub async fn next_within(&mut self, d: Duration) -> Option<Result<ServerMessage, Status>>;
      pub fn request(id: &str, kind: RequestKind, type_name: &str, key: &str, timeout_ms: u32) -> ClientMessage;
      pub fn reply_ok(request_id: &str, payload: &[u8]) -> ClientMessage;
      pub fn reply_err(request_id: &str, code: ErrorCode, message: &str) -> ClientMessage;
  }
  ```

- [ ] **Step 1: Write failing integration tests in `tests/connect.rs`**

```rust
#[tokio::test] async fn heartbeat_is_echoed_with_same_timestamp()
#[tokio::test] async fn request_before_register_closes_with_failed_precondition()
#[tokio::test] async fn duplicate_type_owner_closes_with_already_exists()
#[tokio::test] async fn command_round_trip_between_two_services()
    // A registers cmd "b.Do"; B sends Request{id:"r1",...payload:b"{\"x\":1}", headers:{authorization:"Bearer t"}}; A receives identical Request; A replies payload b"{\"ok\":true}"; B receives Reply with same payload
#[tokio::test] async fn handler_error_reply_is_forwarded_unchanged()
#[tokio::test] async fn unknown_type_gets_no_handler()
#[tokio::test] async fn routing_key_sticks_to_one_instance_of_two()        // 20 requests with key "agg-1": all land on the same A instance
#[tokio::test] async fn handler_stream_close_fails_in_flight_with_handler_disconnected()
#[tokio::test] async fn short_timeout_yields_timeout_error()               // timeout_ms=50, handler never replies
#[tokio::test] async fn health_service_reports_serving()                    // tonic_health client Check("") == Serving
#[tokio::test] async fn stale_instance_is_evicted_by_watchdog()
    // Config{heartbeat_timeout: 300ms}; A registers, sends no heartbeats; after 1.2s B's request -> NO_HANDLER and A's stream ends
#[tokio::test] async fn reregister_replaces_type_list()
```

- [ ] **Step 2: Run to verify failure** — `cargo test --test connect` Expected: FAIL (service unimplemented).

- [ ] **Step 3: Implement `service.rs`, `serve`, and `tests/common/mod.rs`**

- [ ] **Step 4: Run all tests and lints** — `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check` Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(service): Connect stream, registration rules, watchdog, health"
```

---

### Task 6: Telemetry: structured logs, OTLP traces and metrics

**Files:**
- Modify: `src/telemetry.rs`, `src/router.rs`, `src/service.rs`, `src/main.rs`
- Test: `src/telemetry.rs` unit test (metrics with an in-memory `ManualReader`)

**Interfaces:**
- Produces: `pub fn init(config: &Config) -> anyhow::Result<TelemetryGuard>`: JSON `tracing_subscriber` fmt layer with `EnvFilter::new(&config.log)`; when `config.otlp_endpoint` is `Some`, adds `tracing_opentelemetry::layer()` with an OTLP gRPC span exporter and installs a periodic OTLP metric reader; `TelemetryGuard::drop` shuts providers down.
- Produces: `pub struct Metrics` built from an `opentelemetry::metrics::Meter`, with methods `request_finished(kind: Kind, type_name: &str, outcome: Outcome, duration: Duration)`, `in_flight_add(kind, delta: i64)`, `sessions_set(by_service: HashMap<String, usize>)`, `registered_types_set(snapshot)`, `buffer_full(service: &str)`; `enum Outcome { Ok, NoHandler, Timeout, HandlerDisconnected, HandlerError }` labelled exactly `ok|no_handler|timeout|handler_disconnected|handler_error`. Instrument names exactly as the spec table: `kraken_hub_requests_total`, `kraken_hub_request_duration_seconds`, `kraken_hub_in_flight`, `kraken_hub_sessions`, `kraken_hub_registered_types`, `kraken_hub_session_buffer_full_total`.
- Router: a `tracing::info_span!("hub.route", kraken.type, kraken.kind, kraken.sender, kraken.handler = field::Empty, kraken.outcome = field::Empty)` per request, parented from `headers["traceparent"]` via the W3C `TraceContextPropagator`; the forwarded `Request` headers are untouched.

- [ ] **Step 1: Write failing test**

```rust
#[test] fn request_finished_records_counter_with_outcome_label()
    // SdkMeterProvider with ManualReader; Metrics::new(meter); request_finished(Command,"a.B",Outcome::Ok, 10ms); collect; find kraken_hub_requests_total with attributes kind="command", type="a.B", outcome="ok", value 1
```

- [ ] **Step 2: Run to verify failure** — `cargo test --lib telemetry` Expected: compile FAIL.

- [ ] **Step 3: Implement; wire `Metrics` into `Router` (replace the no-op) and `HubService` (sessions/types gauges updated on register/unregister)**

- [ ] **Step 4: Run everything** — `cargo test && cargo clippy --all-targets -- -D warnings` Expected: PASS.

- [ ] **Step 5: Smoke run** — `KRAKEN_HUB_LOG=debug cargo run` in background, then `grpcurl -plaintext localhost:7000 grpc.health.v1.Health/Check` if `grpcurl` exists, else rely on Task 5's health test. Expected: `"status": "SERVING"`. Stop the process.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(telemetry): JSON logs, OTLP traces and metrics"
```

---

### Task 7: Image build and docs

**Files:**
- Modify: `README.md`, `Dockerfile`, `.gitlab-ci.yml` (fix the `kraken-hubatest` typo to `kraken-hub:latest`)

- [ ] **Step 1: Build the image** — `docker build -t kraken-hub:dev .` Expected: success; `docker run --rm -p 7000:7000 kraken-hub:dev` logs `kraken-hub starting`.
- [ ] **Step 2: Update README**: remove "skeleton" status, document `cargo test`, the test layout, the env table with spec defaults, and that `kraken-hub:dev` is the image name the Kotlin starter's integration tests expect (see kraken-messaging plan).
- [ ] **Step 3: Commit** — `git add -A && git commit -m "docs: hub ready; fix CI image tag"`
