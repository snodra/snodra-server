# syntax=docker/dockerfile:1
FROM rust:1.99-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY ui ./ui
RUN cargo build --release --locked

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder /app/target/release/snodra-server /app/snodra-server
ENV KRAKEN_HUB_LISTEN=0.0.0.0:7000
EXPOSE 7000 7001
USER nonroot
# Distroless has no shell or curl: the binary probes its own gRPC health service.
HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD ["/app/snodra-server", "--health-check"]
ENTRYPOINT ["/app/snodra-server"]
