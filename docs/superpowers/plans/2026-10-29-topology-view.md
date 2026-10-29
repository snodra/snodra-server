# kraken-hub Topology View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the hub a read-only topology view (gRPC `Topology` RPC, HTTP `GET /topology` JSON on a second port, `kraken-hub --topology` CLI) showing services, instances, handled command/query types, liveness and orphaned types.

**Architecture:** A `topology` module assembles one `Topology` struct from three single-lock reads (`Sessions::details`, `Registry::topology`, `Router::in_flight_by_instance`) using a startup `(Instant, SystemTime)` anchor for wall-clock fields. Proto and JSON (serde) conversions live in that module; the gRPC handler, the axum HTTP handler and the CLI renderer all consume the same struct. The registry stores the kind per instance type so the per-instance lists are exact.

**Tech Stack:** Rust 1.99.1, tonic 0.14.8 + tonic-prost, axum 0.8.9, serde 1 + serde_json 1, tokio 1.54, existing test harness in `tests/common`.

**Spec:** `docs/superpowers/specs/2026-10-29-topology-view-design.md` (this repo). Protocol source of truth: `../kraken/protocol/hub.proto`.

## Global Constraints

- Proto changes are made in `/Users/damoreira/.projects/kraken-workspace/kraken/protocol/hub.proto` first and copied verbatim to `proto/hub.proto` here and to `/Users/damoreira/.projects/kraken-workspace/kraken-messaging/kraken-messaging/src/main/proto/hub.proto`; additive only; `RequestKind` reused; JSON `kind` is the enum name.
- Snapshot shape exactly as the spec (camelCase JSON names listed there), plus `heartbeatTimeoutMs` and per-instance `stale` (resolved question 1).
- `KRAKEN_HUB_HTTP_LISTEN` default `0.0.0.0:7001`; empty disables HTTP; bind failure is fatal; `serve_with_shutdown(config, grpc_listener, http_listener: Option<TcpListener>, shutdown)`.
- Lock discipline: each of the three maps is read under its own lock exactly once; no nested locks; no lock across an await. Routing behaviour unchanged except the two new `Register` rejections (same name under both kinds → `INVALID_ARGUMENT`; kind conflict with the owner → `ALREADY_EXISTS`).
- No payload bytes or headers in the view or logs. No new metrics.
- `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` green; image builds with `--locked`.
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## Review Focus

1. A `Register` listing the same type under both `command_types` and `query_types` must be rejected, not silently deduplicated. Test in Task 2.
2. A `Topology` call while an instance is mid-registration (session exists, registry not yet updated) must return that instance with empty type lists, never panic or omit the session. Test in Task 3.
3. `GET /topology` must return `kind` as `"COMMAND"`/`"QUERY"` strings and camelCase keys; a consumer comparing it with the RPC must see identical data. Test in Task 4.
4. `--json` without `--topology`, or `--topology --health-check`, must exit 2 with usage text, not run anything. Test in Task 5.
5. With `KRAKEN_HUB_HTTP_LISTEN=` (empty) the hub must start with gRPC only and the CLI/RPC still work. Test in Task 4.

---

### Task 1: Protocol: `Topology` RPC and messages

**Files:**
- Modify: `/Users/damoreira/.projects/kraken-workspace/kraken/protocol/hub.proto` (umbrella, source of truth), `proto/hub.proto` (copy), `/Users/damoreira/.projects/kraken-workspace/kraken-messaging/kraken-messaging/src/main/proto/hub.proto` (copy)
- Test: `tests/connect.rs` (compile-level: the generated client exposes `topology`)

**Interfaces:**
- Produces (proto, additive):
  ```proto
  rpc Topology(TopologyRequest) returns (TopologyResponse);
  message TopologyRequest {}
  message TopologyResponse { int64 hub_started_at_ms = 1; int64 now_ms = 2; int64 heartbeat_timeout_ms = 3; repeated ServiceTopology services = 4; repeated TypeOwnership types = 5; TopologyTotals totals = 6; }
  message ServiceTopology { string service_name = 1; repeated InstanceTopology instances = 2; }
  message InstanceTopology { string instance_id = 1; int64 connected_since_ms = 2; int64 last_heartbeat_age_ms = 3; bool stale = 4; uint32 in_flight_as_handler = 5; uint32 in_flight_as_sender = 6; repeated string command_types = 7; repeated string query_types = 8; }
  message TypeOwnership { string type = 1; RequestKind kind = 2; string owner_service = 3; uint32 live_instances = 4; bool orphaned = 5; int64 orphaned_for_ms = 6; }
  message TopologyTotals { uint32 services = 1; uint32 instances = 2; uint32 command_types = 3; uint32 query_types = 4; uint32 in_flight = 5; }
  ```
  with comments mirroring the spec's field descriptions, and a comment on `Register` stating the two new rejections.

- [ ] **Step 1: Edit the umbrella proto, copy to both repos; in the umbrella repo commit on a branch `docs/topology-proto` in a worktree (`.worktrees/docs-topology-proto`), do not merge.**
- [ ] **Step 2: Hub compiles** — `cargo build` Expected: success; `HubServer` trait now requires `topology` (stub returning `Status::unimplemented` for now in `service.rs`).
- [ ] **Step 3: Kotlin starter regenerates** — in `/Users/damoreira/.projects/kraken-workspace/kraken-messaging` run `./gradlew :kraken-messaging:compileKotlin -q` Expected: success (generated `topology` stub, no Kotlin changes). Commit there on `main` as `chore(proto): sync hub.proto (Topology RPC)`.
- [ ] **Step 4: Commit hub** — `git add proto/hub.proto src/service.rs && git commit -m "feat(proto): Topology RPC and messages"`

---

### Task 2: Registry: kind per instance type, new rejections, topology read

**Files:**
- Modify: `src/registry.rs`, `src/service.rs` (map the new errors to statuses)
- Test: `src/registry.rs` unit tests, `tests/connect.rs`

**Interfaces:**
- Produces:
  ```rust
  pub enum RegisterError { TypeOwnedByOtherService { type_name, owner }, KindConflict { type_name, owner, owner_kind: Kind }, DuplicateTypeAcrossKinds { type_name } }
  pub struct InstanceTypes { pub service: String, pub command_types: Vec<String>, pub query_types: Vec<String> } // sorted
  pub struct TypeInfo { pub type_name: String, pub kind: Kind, pub owner_service: String, pub live_instances: u32, pub orphaned_for: Option<Duration> } // orphaned_for = Some(now - orphaned_at) while holders == 0
  pub struct RegistryTopology { pub instances: HashMap<InstanceId, InstanceTypes>, pub types: Vec<TypeInfo> /* sorted by type_name */ }
  impl Registry { pub fn topology(&self, now: Instant) -> RegistryTopology; }
  ```
- `service.rs`: `KindConflict` → `Status::already_exists("type {t} is owned as {KIND} by service {owner}")`; `DuplicateTypeAcrossKinds` → `Status::invalid_argument("type {t} listed as both command and query")`; both close the stream like the existing conflict path.

- [ ] **Step 1: Write failing tests**
```rust
#[test] fn topology_lists_instance_types_by_kind_sorted()      // a: cmds [b.X, a.Y], qry [q.Z] → command_types == ["a.Y","b.X"], query_types == ["q.Z"]
#[test] fn topology_reports_live_instances_and_orphaned_age()  // two instances hold t; unregister one → live 1, orphaned None; unregister both at t0 → live 0, orphaned_for == Some(now - t0)
#[test] fn topology_keeps_orphaned_type_past_grace_until_sweep()
#[test] fn register_same_type_under_both_kinds_is_rejected()   // Err(DuplicateTypeAcrossKinds), nothing registered
#[test] fn register_kind_conflict_with_owner_is_rejected()     // A owns t as Command; B lists t as query → Err(KindConflict{owner:"A", owner_kind: Command})
#[test] fn same_service_reregister_with_other_kind_is_rejected() // also KindConflict (owner kind is fixed while owned)
// tests/connect.rs
#[tokio::test] async fn register_with_type_in_both_lists_is_invalid_argument()
#[tokio::test] async fn register_kind_conflict_is_already_exists()
```
- [ ] **Step 2: Run** — `cargo test --lib registry` Expected: compile FAIL.
- [ ] **Step 3: Implement** (`InstanceEntry.types: Vec<(String, Kind)>`; validation before mutation stays atomic).
- [ ] **Step 4: Run** — `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check` Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(registry): kind per instance type, kind conflicts rejected, topology read"`

---

### Task 3: Sessions and router reads, snapshot assembly, gRPC handler

**Files:**
- Create: `src/topology.rs`
- Modify: `src/session.rs` (`Session.connected_since: Instant`, `details()`), `src/router.rs` (`in_flight_by_instance()`), `src/lib.rs` (clock anchor, pass `heartbeat_timeout`, wire `HubService`), `src/service.rs` (`topology` RPC)
- Test: `src/topology.rs` unit test (assembly from fixtures), `tests/topology.rs` (integration)

**Interfaces:**
- Produces:
  ```rust
  // session.rs
  pub struct SessionDetail { pub instance_id: InstanceId, pub service: String, pub connected_since: Instant, pub last_heartbeat: Instant }
  impl Sessions { pub fn details(&self) -> Vec<SessionDetail>; }
  // router.rs
  pub struct InFlight { pub as_handler: u32, pub as_sender: u32 }
  impl Router { pub fn in_flight_by_instance(&self) -> (HashMap<InstanceId, InFlight>, u32 /* total pending */); }
  // topology.rs
  #[derive(Clone, Copy)] pub struct ClockAnchor { pub instant: Instant, pub system: SystemTime } impl ClockAnchor { pub fn now() -> Self; pub fn to_epoch_ms(&self, at: Instant) -> i64; }
  #[derive(Serialize, Deserialize, PartialEq, Debug)] #[serde(rename_all = "camelCase")] pub struct Topology { hub_started_at_ms: i64, now_ms: i64, heartbeat_timeout_ms: i64, services: Vec<ServiceTopology>, types: Vec<TypeOwnership>, totals: Totals }
  // ServiceTopology, InstanceTopology { ..., stale: bool, ... }, TypeOwnership { kind: KindName /* serializes as "COMMAND"/"QUERY" */, ... }, Totals — fields exactly as the spec
  pub fn snapshot(registry: &Registry, sessions: &Sessions, router: &Router, anchor: ClockAnchor, started: Instant, heartbeat_timeout: Duration, now: Instant) -> Topology;
  impl Topology { pub fn to_proto(&self) -> proto::TopologyResponse; pub fn from_proto(p: &proto::TopologyResponse) -> Topology; }
  ```
- Assembly rules: sessions are the base set; registry types attached by instance id; `stale = age > heartbeat_timeout / 3`; services sorted by name, instances by id; `totals.in_flight` = pending-map size.
- `HubService` gains `anchor`, `started`, `heartbeat_timeout` (from `Config`); `topology()` RPC returns `snapshot(...).to_proto()`.

- [ ] **Step 1: Write failing tests**
```rust
// src/topology.rs unit (fixtures built via Registry/Sessions/Router directly)
#[test] fn snapshot_reconciles_session_without_registry_entry_as_empty_lists()
#[test] fn snapshot_marks_stale_when_heartbeat_older_than_a_third_of_timeout()
#[test] fn proto_round_trip_preserves_everything()      // from_proto(to_proto(t)) == t
#[test] fn json_uses_camel_case_and_kind_names()         // serde_json contains "\"kind\":\"COMMAND\"" and "connectedSinceMs"
// tests/topology.rs (FakeClient from tests/common)
#[tokio::test] async fn topology_lists_two_services_with_instances_and_types()  // A x2 instances cmd a.Do; B x1 qry b.Get → services sorted, live_instances 2 and 1, totals
#[tokio::test] async fn topology_shows_orphaned_type_after_last_instance_leaves()
#[tokio::test] async fn topology_counts_in_flight_for_handler_and_sender()      // B sends to A, A does not reply yet → A as_handler 1, B as_sender 1, totals.in_flight 1
#[tokio::test] async fn topology_reflects_eviction()                             // heartbeat_timeout 300ms; stale instance disappears after watchdog
```
- [ ] **Step 2: Run** — `cargo test --lib topology --test topology` Expected: compile FAIL.
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** — `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check` Expected: PASS (run `--test topology` three times).
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(topology): snapshot assembly and Topology RPC"`

---

### Task 4: HTTP JSON mirror

**Files:**
- Modify: `Cargo.toml` (`axum = "0.8.9"`, `serde = { version = "1", features = ["derive"] }`, `serde_json = "1"`), `src/config.rs` (`http_listen: Option<SocketAddr>`, env `KRAKEN_HUB_HTTP_LISTEN`, default `0.0.0.0:7001`, empty → `None`), `src/lib.rs` (`serve_with_shutdown(config, grpc: TcpListener, http: Option<TcpListener>, shutdown)`; axum router `GET /topology`, 404 otherwise; same shutdown signal), `src/main.rs` (bind HTTP when configured), `tests/common/mod.rs` (`start_hub_with_http(config) -> (SocketAddr, Option<SocketAddr>)`), `Dockerfile` (`EXPOSE 7001`), README env table
- Test: `src/config.rs`, `tests/http.rs`

**Interfaces:**
- Produces: `Config.http_listen: Option<SocketAddr>`; `pub async fn serve_with_shutdown(config: Config, grpc: TcpListener, http: Option<TcpListener>, shutdown: impl Future<Output=()>)`; existing `serve(config, listener)` keeps working by binding HTTP from `config.http_listen` (or `None`).

- [ ] **Step 1: Write failing tests**
```rust
// config.rs
#[test] fn http_listen_defaults_to_7001_and_empty_disables()
// tests/http.rs
#[tokio::test] async fn get_topology_returns_json_equal_to_rpc()   // reqwest-free: use hyper client or std TcpStream + minimal HTTP/1.1; parse with serde_json; compare Topology::from_json == Topology::from_proto(rpc) ignoring now_ms
#[tokio::test] async fn unknown_path_is_404()
#[tokio::test] async fn hub_runs_without_http_listener()           // start_hub_with_http(config with http_listen None): RPC works, no HTTP port
```
- [ ] **Step 2: Run** — Expected: compile FAIL.
- [ ] **Step 3: Implement** (test HTTP client: add `reqwest` as a dev-dependency only if hyper's client is more code; prefer a tiny `tokio::net::TcpStream` request helper in `tests/common`).
- [ ] **Step 4: Run** — `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check` Expected: PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(http): GET /topology JSON mirror on KRAKEN_HUB_HTTP_LISTEN"`

---

### Task 5: CLI `--topology [--json]`

**Files:**
- Create: `src/cli.rs` (argument parsing `enum Mode { Serve, HealthCheck, Topology { json: bool } }`, `fn parse(args) -> Result<Mode, UsageError>`, `fn render_table(t: &Topology) -> String`)
- Modify: `src/main.rs` (dispatch), `src/lib.rs` (`pub async fn topology_probe(addr) -> anyhow::Result<Topology>` with 3 s timeout), README
- Test: `src/cli.rs` unit tests, `tests/cli.rs`

**Interfaces:**
- Produces: `cli::parse(&[String]) -> Result<Mode, UsageError>` where `--json` without `--topology` or `--topology` with `--health-check` → `Err(UsageError)` (main prints usage to stderr, exit 2); `render_table` output: one block per service with header `service <name> (<n> instances)` and rows `instance | connected for | heartbeat age | stale | in-flight h/s | commands | queries`, then `orphaned types:` rows if any, then `totals: ...`; durations formatted `1h02m`, `45s`, `800ms` from `now_ms`.

- [ ] **Step 1: Write failing tests**
```rust
#[test] fn parse_modes_and_usage_errors()               // 5 cases incl. the two Review Focus errors
#[test] fn render_table_fixture_snapshot()              // fixed Topology → exact expected string
#[tokio::test] async fn topology_probe_against_test_hub()   // tests/cli.rs: start_hub, register one fake, probe returns it
```
- [ ] **Step 2: Run** — Expected: compile FAIL.
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** — `cargo test && cargo clippy --all-targets -- -D warnings && cargo fmt --check` Expected: PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(cli): kraken-hub --topology [--json]"`

---

### Task 6: Image, compose, docs

**Files:**
- Modify: `README.md` (topology section: RPC, endpoint, CLI, snapshot fields), `Dockerfile` (verify `EXPOSE 7000 7001`), umbrella worktree from Task 1: `infra/compose.yaml` (hub `ports` + `"7001:7001"`), `docs/05-services/hub.md` (new "Topology view" section + config row), `docs/07-local-development.md` (port 7001)

- [ ] **Step 1: Image** — `docker build -t kraken-hub:dev .`; `docker run --rm -d -p 7000:7000 -p 7001:7001 -e KRAKEN_HUB_OTLP_ENDPOINT= --name hub-topo kraken-hub:dev`; `curl -s localhost:7001/topology` returns JSON with `"services":[]`; `docker exec hub-topo /app/kraken-hub --topology` prints the empty table; `docker stop hub-topo`. (If port 7000 is taken by AirPlay on the host, map `7002:7000` instead.)
- [ ] **Step 2: Docs** — write the sections; `docker compose -f infra/compose.yaml config -q` in the umbrella worktree passes.
- [ ] **Step 3: Commit** hub (`docs: topology view usage; expose 7001`) and umbrella worktree branch (`docs: hub topology view, port 7001`); leave the umbrella branch unmerged for the integration decision.
