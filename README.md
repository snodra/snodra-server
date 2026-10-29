# kraken-hub

Axon-Server-like gRPC router for kraken. Services open one bidirectional stream (`Hub.Connect`),
register the command and query types they handle, and the hub forwards each request to a live
instance of the owning service and relays the reply. Events never pass through here; they go
straight to Kafka.

Design and protocol: see the umbrella docs `../kraken/docs/05-services/hub.md` and
`../kraken/docs/02-messaging.md`. The proto in `proto/hub.proto` is a copy of
`../kraken/protocol/hub.proto` (see `proto/README.md`). Items marked **Builder call** below are
implementation decisions that are not yet in the umbrella spec; the spec is updated separately.

## Build and test

Requires Rust 1.99.1 (`rust-toolchain.toml` pins it) and `protoc`. `cmake` is not needed.

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Unit tests live next to the code in `src/`; integration tests in `tests/` start a real hub on an
ephemeral port and drive it over gRPC.

## Running locally

```sh
cargo run
```

| Variable | Default | Meaning |
|---|---|---|
| `KRAKEN_HUB_LISTEN` | `0.0.0.0:7000` | gRPC bind address |
| `KRAKEN_HUB_HTTP_LISTEN` | `0.0.0.0:7001` | HTTP bind address for `GET /topology`; empty disables the HTTP listener |
| `KRAKEN_HUB_OTLP_ENDPOINT` | `http://otel-lgtm:4317` | OTLP gRPC endpoint for traces and metrics |
| `KRAKEN_HUB_LOG` | `info` | `tracing` filter, for example `kraken_hub=debug,info` |
| `KRAKEN_HUB_DEFAULT_TIMEOUT_MS` | `5000` | used when `Request.timeout_ms` is 0 |
| `KRAKEN_HUB_MAX_TIMEOUT_MS` | `60000` | upper clamp for `Request.timeout_ms` |
| `KRAKEN_HUB_HEARTBEAT_TIMEOUT` | `30s` | dead-instance threshold (`30`, `30s` or `500ms`) |
| `KRAKEN_HUB_OWNER_GRACE` | `60s` | keep type ownership after the last instance leaves |
| `KRAKEN_HUB_SESSION_BUFFER` | `1024` | per-session outbound buffer, in messages |
| `KRAKEN_HUB_HANDSHAKE_TIMEOUT` | `10s` | a new `Connect` stream must send `Register` within this (`10`, `10s` or `500ms`) |

**Topology view.** `GET /topology` on `KRAKEN_HUB_HTTP_LISTEN` returns the same snapshot as the
`Topology` RPC as JSON (camelCase fields, `kind` as `COMMAND` or `QUERY`); every other path is 404.
`kraken-hub --topology` prints the local hub's topology as a table (`--topology --json` as JSON);
it probes `127.0.0.1` on the port of `KRAKEN_HUB_LISTEN` with a 3 s timeout and exits 1 on
failure. Unknown or conflicting flags print the usage to stderr and exit 2.
The full section is written with the topology view's documentation task.

- **Builder call:** `KRAKEN_HUB_OTLP_ENDPOINT` defaults to `http://otel-lgtm:4317`. Leaving it
  unset keeps that default; setting it to an empty string disables OTLP export, which is useful
  for `cargo run` without a collector. (For every other variable an empty value means "unset".)
- **Builder call:** `KRAKEN_HUB_HANDSHAKE_TIMEOUT` (default `10s`): a stream that sends nothing,
  or no `Register`, within it is closed with `DEADLINE_EXCEEDED` `no Register within 10s`. A
  `Register` with an empty `instance_id` or `service_name` is closed with `INVALID_ARGUMENT`.
- **Builder call:** on macOS, port 7000 is held by Control Center (AirPlay Receiver). Either disable
  AirPlay Receiver in System Settings or set `KRAKEN_HUB_LISTEN=0.0.0.0:7001` (and map the compose
  host port accordingly).

## Protocol summary

A service connects with `Hub.Connect`, sends `Register` (service name, instance id, handled
command and query types) and then `Heartbeat` messages. A `Request` (kind, type, id, payload,
optional `timeout_ms`) is routed to one live instance of the service that owns the type; the
handler answers with a `Reply` carrying the same id, which the hub relays to the sender. Each
request id gets exactly one terminal message: a `Reply` or a hub-produced error. The hub forwards
the `Request` (headers included) unchanged and parents its `hub.route` span on the request's
trace headers.

Error codes:

- Hub-produced: `NO_HANDLER`, `TIMEOUT`, `HANDLER_DISCONNECTED`, and **Builder call:**
  `INVALID_REQUEST` (malformed request: missing id, kind or type, or a duplicate in-flight
  `Request.id`). When the handler's outbound buffer is full, the hub answers `HANDLER_ERROR` with
  the message `handler busy`.
- Handler-produced codes in a `Reply{error}` are forwarded unchanged.

Metrics outcome labels are `ok`, `no_handler`, `timeout`, `handler_disconnected`, `handler_error`
and `invalid_request`. **Builder call:** any handler `Reply{error}` counts as `handler_error`,
whatever its code.

The generated client stubs are built with `build_transport(false)`; Rust clients create the stub
with `HubClient::new(channel)`.

## Routing and limitations

- **Builder call:** routing is per service. The hub routes to any live instance of the owning
  service and assumes all instances of a service handle the same types (Axon-style). During a
  mixed-version rollout a request may reach an instance that dropped the type; that instance's
  library answers `NO_HANDLER` and the sender sees a transient error.
- Single hub instance; no clustering or failover.
- No authentication or authorization on the gRPC port.
- Not durable: registrations, in-flight requests and sessions are in memory only. Clients
  re-register after a reconnect.
- No scatter-gather: every request goes to exactly one handler.

## Connection lifecycle

- **Builder call:** HTTP/2 keepalive PINGs every 10 s, the connection is dropped when a PING is
  not answered within 20 s; TCP keepalive is 30 s. These are constants in `src/lib.rs`.
- A stream that misses heartbeats for `KRAKEN_HUB_HEARTBEAT_TIMEOUT` is evicted: its session is
  removed, its stream task stops at once (even if the client no longer reads), its registration is
  dropped and its in-flight requests fail with `HANDLER_DISCONNECTED`.
- A `Register` with an instance id that already has a live stream replaces the old stream (logged
  at `info` as `session replaced by a new stream`); the old stream is closed.
- **Builder call:** when a sender's buffer is full, a terminal `Reply` or `Error` is retried in
  the background for up to 5 s before it is dropped (logged at `warn`, counted in
  `kraken_hub_session_buffer_full_total{service}`).
- Graceful shutdown on SIGTERM or Ctrl-C: health turns `NOT_SERVING`, every stream is closed with
  `UNAVAILABLE` `hub shutting down` (in-flight requests fail as on a disconnect), connections get
  at most 5 s to drain, then telemetry is flushed and the process exits.

## Observability

- Traces: one span `hub.route` per request, with the outcome in `kraken.outcome`; parent context
  is taken from the request's trace headers. Exported over OTLP.
- Metrics (OTLP): `kraken_hub_requests_total`, `kraken_hub_request_duration_seconds`,
  `kraken_hub_in_flight`, `kraken_hub_sessions`, `kraken_hub_registered_types`,
  `kraken_hub_session_buffer_full_total`.
- Health: the standard `grpc.health.v1.Health` service on the same port (service
  `kraken.hub.v1.Hub`, and `""` for the whole server). `kraken-hub --health-check` probes it on
  `127.0.0.1:<port of KRAKEN_HUB_LISTEN>` within 3 s, prints one line and exits 0 when
  `SERVING`, 1 otherwise.
- Logs: JSON on stdout, filtered by `KRAKEN_HUB_LOG`.

## Image

```sh
docker build -t kraken-hub:dev .
docker run --rm -p 7000:7000 -e KRAKEN_HUB_OTLP_ENDPOINT= kraken-hub:dev
```

The image is distroless (no shell, no curl), so its `HEALTHCHECK` runs the binary's own probe:
`HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD ["/app/kraken-hub", "--health-check"]`.
In compose, `depends_on` can wait for it:

```yaml
services:
  kraken-hub:
    image: kraken-hub:dev
    environment:
      KRAKEN_HUB_OTLP_ENDPOINT: http://otel-lgtm:4317
    # The image already declares this healthcheck; repeat it only to change the timings.
    healthcheck:
      test: ["CMD", "/app/kraken-hub", "--health-check"]
      interval: 10s
      timeout: 3s
      retries: 3
  orders:
    depends_on:
      kraken-hub:
        condition: service_healthy
```

`docker stop` sends SIGTERM to the hub (PID 1), which shuts down gracefully (see above).

`kraken-hub:dev` is the image name the Kotlin starter's integration tests use. CI publishes tagged
releases (`vX.Y.Z`) to `registry.gitlab.damoreira.ml/kraken/kraken-hub:<tag>` and `:latest`.

## Layout

```
src/lib.rs        crate root, proto module, serve()
src/main.rs       thin binary: config, telemetry, serve
src/config.rs     KRAKEN_HUB_* parsing
src/registry.rs   service/type ownership and instance selection
src/session.rs    per-connection outbound buffers
src/router.rs     request forwarding, timeouts, reply relay, one terminal message per id
src/service.rs    Hub.Connect stream handling
src/telemetry.rs  tracing subscriber, OTLP export, metrics
tests/            integration tests over a real gRPC connection
proto/hub.proto   protocol, synced from the umbrella repo
```
