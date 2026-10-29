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
| `KRAKEN_HUB_OTLP_ENDPOINT` | `http://otel-lgtm:4317` | OTLP gRPC endpoint for traces and metrics |
| `KRAKEN_HUB_LOG` | `info` | `tracing` filter, for example `kraken_hub=debug,info` |
| `KRAKEN_HUB_DEFAULT_TIMEOUT_MS` | `5000` | used when `Request.timeout_ms` is 0 |
| `KRAKEN_HUB_MAX_TIMEOUT_MS` | `60000` | upper clamp for `Request.timeout_ms` |
| `KRAKEN_HUB_HEARTBEAT_TIMEOUT` | `30s` | dead-instance threshold (`30`, `30s` or `500ms`) |
| `KRAKEN_HUB_OWNER_GRACE` | `60s` | keep type ownership after the last instance leaves |
| `KRAKEN_HUB_SESSION_BUFFER` | `1024` | per-session outbound buffer, in messages |

- **Builder call:** `KRAKEN_HUB_OTLP_ENDPOINT` defaults to `http://otel-lgtm:4317`. Setting it to an
  empty string disables OTLP export, which is useful for `cargo run` without a collector.
- **Builder call:** on macOS, port 7000 is held by Control Center (AirPlay Receiver). Either disable
  AirPlay Receiver in System Settings or set `KRAKEN_HUB_LISTEN=0.0.0.0:7001` (and map the compose
  host port accordingly).

## Protocol summary

A service connects with `Hub.Connect`, sends `Register` (service name, instance id, handled
command and query types) and then `Heartbeat` messages. A `Request` (kind, type, id, payload,
optional `timeout_ms`) is routed to one live instance of the service that owns the type; the
handler answers with a `Reply` carrying the same id, which the hub relays to the sender. Each
request id gets exactly one terminal message: a `Reply` or a hub-produced error. The hub
propagates W3C trace headers from the `Request` to the handler.

Error codes:

- Hub-produced: `NO_HANDLER`, `TIMEOUT`, `HANDLER_DISCONNECTED`, and **Builder call:**
  `INVALID_REQUEST` (malformed request: missing kind or type, or a duplicate in-flight
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

## Observability

- Traces: one span `hub.route` per request, with the outcome in `kraken.outcome`; parent context
  is taken from the request's trace headers. Exported over OTLP.
- Metrics (OTLP): `kraken_hub_requests_total`, `kraken_hub_request_duration_seconds`,
  `kraken_hub_in_flight`, `kraken_hub_sessions`, `kraken_hub_registered_types`,
  `kraken_hub_session_buffer_full_total`.
- Health: the standard `grpc.health.v1.Health` service on the same port.
- Logs: JSON on stdout, filtered by `KRAKEN_HUB_LOG`.

## Image

```sh
docker build -t kraken-hub:dev .
docker run --rm -p 7000:7000 -e KRAKEN_HUB_OTLP_ENDPOINT= kraken-hub:dev
```

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
