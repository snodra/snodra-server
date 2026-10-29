# kraken-hub topology view: design

Date: 2026-10-29. Status: builder check done, ready to plan.

## Intent

Operators and developers want one place that shows what the hub currently knows: which services and
instances are connected, which command and query types each instance handles, how alive each instance
is, and which types are temporarily ownerless. Today that knowledge exists only in the hub's memory,
metrics and logs. The view is read-only and informational; routing does not change.

## Goals

- A `Topology` gRPC RPC on the existing `kraken.hub.v1.Hub` service, port 7000, returning one
  consistent snapshot.
- The same snapshot as JSON over HTTP (`GET /topology`) on a second port, for curl, browsers and the
  future frontend.
- A `kraken-hub --topology` CLI mode that prints the snapshot as a readable table (and `--json` for raw
  JSON), usable inside the distroless image like `--health-check`.
- Umbrella docs, proto and compose updated; the Kotlin starter's proto copy regenerated (no Kotlin
  behaviour change).

## Non-goals

- Events are not declared to the hub and do not appear in the view.
- Senders are not tracked statically; the view shows handlers only. In-flight request counts are the
  only runtime trace of senders.
- No history, persistence, authentication or push/streaming of changes. Clients poll.
- No change to routing, registration rules, metrics names or the stream protocol.

## Anti-goals

- The snapshot must not slow the routing path: it takes each internal lock once, briefly, and never
  nests locks (the hub's lock order is sessions then registry; the snapshot does not hold one while
  taking another).
- No payload bytes or headers ever appear in the view.

## Snapshot shape

One snapshot is taken at a point in time; the three internal maps are read one after another, so a
service may appear in one and not the other for a few microseconds. The view reconciles by instance
id and shows what it found.

```text
Topology
  hubStartedAtMs      int64   wall clock, when this hub process started
  nowMs               int64   wall clock at snapshot time
  services[]          ServiceTopology
  types[]             TypeOwnership, sorted by type name
  totals              { services, instances, commandTypes, queryTypes, inFlight }

ServiceTopology
  serviceName         string
  instances[]         InstanceTopology, sorted by instanceId

InstanceTopology
  instanceId          string
  connectedSinceMs    int64   wall clock when the stream registered
  lastHeartbeatAgeMs  int64   now minus last heartbeat (or registration if none yet)
  inFlightAsHandler   uint32  pending requests forwarded to this instance
  inFlightAsSender    uint32  pending requests this instance sent
  commandTypes[]      string  sorted
  queryTypes[]        string  sorted

TypeOwnership
  type                string
  kind                RequestKind (COMMAND | QUERY)
  ownerService        string
  liveInstances       uint32  instances currently listing the type
  orphaned            bool    true while liveInstances == 0 and the owner grace period has not expired
  orphanedForMs       int64   0 unless orphaned
```

The gRPC messages mirror this shape in snake_case (`TopologyRequest {}`, `TopologyResponse` with the
fields above). The JSON mirror uses the camelCase names above. Both are produced from the same Rust
struct so they cannot drift.

## Interfaces

### gRPC

```proto
service Hub {
  rpc Connect(stream ClientMessage) returns (stream ServerMessage);
  rpc Topology(TopologyRequest) returns (TopologyResponse);
}
```

Additive change in `kraken/protocol/hub.proto` (source of truth), copied verbatim to `kraken-hub/proto/hub.proto`
and `kraken-messaging/kraken-messaging/src/main/proto/hub.proto` (regenerated stubs; no Kotlin code
uses the new RPC yet).

### HTTP

`GET /topology` returns `200 application/json` with the camelCase snapshot. Any other path returns
404. Bind address `KRAKEN_HUB_HTTP_LISTEN`, default `0.0.0.0:7001`; an empty value disables the HTTP
listener. No TLS, no auth (same trust model as port 7000: private network only).

**Builder call:** HTTP is served by axum 0.8 on its own listener and tokio task, started and stopped
with the gRPC server (same graceful shutdown signal). A separate port keeps the gRPC listener
untouched and makes the compose mapping explicit (`7001:7001`).

### CLI

`kraken-hub --topology [--json]` connects to `127.0.0.1:<port of KRAKEN_HUB_LISTEN>` over gRPC (same
convention as `--health-check`), calls `Topology`, and prints:

- default: a table per service (instance id, connected for, heartbeat age, in-flight handler/sender,
  types), then a table of orphaned types if any, then the totals line;
- `--json`: the JSON snapshot, pretty-printed.

Exit code 0 on success, 1 on connection or RPC failure (message on stderr). A 3 s timeout, like the
health probe.

## Internals

- `Registry::topology() -> RegistryTopology` (one lock): per instance its service and type list; per
  type its `Owner { service, kind, holders, orphaned_at }`. Grace period and `now` are known to the
  registry, so `orphaned`/`orphanedForMs` are computed there.
- `Sessions::details() -> Vec<SessionDetail { instance_id, service, connected_since: Instant,
  last_heartbeat: Instant }>` (one lock). `Session` gains `connected_since`, set at `insert`.
- `Router::in_flight_by_instance() -> HashMap<InstanceId, (as_handler, as_sender)>` (pending lock).
- `topology::snapshot(registry, sessions, router, started_at)` assembles the struct; monotonic
  `Instant`s are converted to wall-clock epoch milliseconds using the hub's `(Instant, SystemTime)`
  anchor taken at startup, so the view shows real times without the hub storing wall clocks per event.
- The gRPC handler, the HTTP handler and the CLI all consume the same `Topology` struct; proto and
  JSON conversions live in one module.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `KRAKEN_HUB_HTTP_LISTEN` | `0.0.0.0:7001` | HTTP bind address for `/topology`; empty disables HTTP |

Everything else is unchanged.

## Observability

- Log one `info` line when the HTTP listener starts and stops. No per-request logging of topology
  calls (debug level at most).
- No new metrics.

## Testing

- Unit: `Registry::topology()` reports holders, orphaned flag and grace age; `Sessions::details()`;
  `Router::in_flight_by_instance()` counts both roles.
- Integration (existing `tests/common` fake clients): two services register, one with two instances;
  `Topology` lists both with sorted types and `liveInstances`; after dropping one instance the
  snapshot shows the remaining one and, for a type only it held, `orphaned = true` within the grace
  period; a request in flight raises the handler's and the sender's counters.
- HTTP: `GET /topology` on the second listener returns the same content as the RPC (compare after
  JSON round trip); unknown path returns 404; empty `KRAKEN_HUB_HTTP_LISTEN` starts no listener.
- CLI: the table renderer is a pure function tested with a fixture snapshot; `--topology` is exercised
  against a running test hub.
- Docker: image still builds; `docker run` with both ports; `curl :7001/topology` returns JSON.

## Deployment and docs

- `kraken/infra/compose.yaml`: hub `ports` adds `"7001:7001"`.
- `kraken/docs/05-services/hub.md`: new "Topology view" section (RPC, HTTP, CLI, snapshot fields);
  configuration row; port table in `07-local-development.md` gains 7001.
- `kraken-hub/README.md`: usage of the RPC, endpoint and CLI.

## Left to the builder

- Exact table layout and column widths of the CLI renderer.
- Field order in JSON (serde default order from the struct).
- Whether HTTP uses axum or raw hyper (axum preferred for brevity).

## Resolved builder questions (all builder calls)

1. **Staleness flag.** The snapshot carries `heartbeatTimeoutMs` (the hub's eviction threshold) at the
   top level, and each instance has `stale: bool` = `lastHeartbeatAgeMs > heartbeatTimeoutMs / 3`
   (at least one expected heartbeat missed; the hub assumes clients send every `timeout / 3`).
   Consumers may still apply their own threshold to the raw age.
2. **Kind per type per instance.** `InstanceEntry` stores `(type, Kind)` pairs instead of bare names so
   `commandTypes`/`queryTypes` are exact. A `Register` that lists the same name under both kinds is
   rejected with `INVALID_ARGUMENT`; a `Register` whose kind for a type differs from the existing
   owner's kind is rejected with `ALREADY_EXISTS` ("type X is owned as COMMAND by service Y"). This
   closes a previously deferred minor (first registrant's kind silently won). Routing is unchanged.
3. **Past grace, before sweep.** A type whose owner has no live instance stays in `types[]` with
   `orphaned = true` and its real `orphanedForMs`, even after the grace period has elapsed, until the
   watchdog sweeps it. Nothing is hidden.
4. **In-flight counters.** `inFlightAsSender` is attached only to instances present in the snapshot
   (a sender's pending entries are dropped on its disconnect). `totals.inFlight` is the size of the
   pending map, taken in the same lock as the per-instance counts, and is canonical.
5. **Reconciliation.** The sessions map is the base set: an instance exists in the view if it has a
   session. Registry data (types) is attached when present, otherwise the lists are empty. Registry
   instances without a session are not shown (they disappear within microseconds). No nullable fields.
6. **Proto.** `RequestKind` already exists in `hub.proto` (`REQUEST_KIND_UNSPECIFIED`, `COMMAND`,
   `QUERY`) and is reused. Field numbers are assigned by the builder (additive, starting at 1 per new
   message). Wall-clock values are `int64` epoch milliseconds. JSON renders `kind` as the enum name
   (`"COMMAND"`/`"QUERY"`), never a number.
7. **HTTP failures.** A bind failure or an unparsable `KRAKEN_HUB_HTTP_LISTEN` fails startup, like the
   gRPC listener. For tests, `serve_with_shutdown` takes the HTTP listener as `Option<TcpListener>`
   (pre-bound, ephemeral port in tests); `main` binds it from config, `None` when the variable is empty.
8. **CLI arguments.** `--topology` and `--health-check` are mutually exclusive and `--json` requires
   `--topology`; violations print usage to stderr and exit 2. Durations shown by the renderer are
   computed from the response's `nowMs`, never the CLI's own clock. Loopback probing mirrors
   `--health-check`.
9. **Clock anchor.** The hub takes one `(Instant, SystemTime)` anchor when `serve_with_shutdown`
   starts; `hubStartedAtMs`, `connectedSinceMs` and `nowMs` are all derived from that anchor, so they
   stay mutually consistent under wall-clock jumps.
