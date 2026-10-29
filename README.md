# kraken-hub

Axon-Server-like gRPC router for kraken. Services open one bidirectional stream, register the
command and query types they handle, and the hub forwards each request to one owning instance and
relays the reply. Events never pass through here; they go straight to Kafka.

Status: **skeleton, no business logic.** Register and Heartbeat are handled; every Request is
answered with `NO_HANDLER` until routing is implemented.

Design and protocol: see the umbrella repository
[`../kraken/docs/05-services/hub.md`](../kraken/docs/05-services/hub.md) and
[`../kraken/docs/02-messaging.md`](../kraken/docs/02-messaging.md). The proto in `proto/hub.proto`
is a copy of `../kraken/protocol/hub.proto` (see `proto/README.md`).

## Build and run

Requires Rust 1.99.1 (`rust-toolchain.toml` pins it), `protoc`, and `cmake` is not needed here.

```sh
cargo build
KRAKEN_HUB_LISTEN=0.0.0.0:7000 cargo run
cargo test
docker build -t kraken-hub .
```

## Configuration

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

## Layout

```
src/lib.rs       crate root, proto module, serve()
src/main.rs      thin binary: config, telemetry, serve
src/telemetry.rs tracing subscriber
src/config.rs    KRAKEN_HUB_* parsing
src/registry.rs  type -> instances, consistent hash / round-robin owner selection (unit tested)
src/service.rs   Hub.Connect stream handling (Register, Heartbeat, Request -> NO_HANDLER)
proto/hub.proto  protocol, synced from the umbrella repo
```
