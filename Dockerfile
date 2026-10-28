# syntax=docker/dockerfile:1
FROM rust:1.99-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked || cargo build --release

FROM gcr.io/distroless/cc-debian12:nonroot
WORKDIR /app
COPY --from=builder /app/target/release/kraken-hub /app/kraken-hub
ENV KRAKEN_HUB_LISTEN=0.0.0.0:7000
EXPOSE 7000
USER nonroot
ENTRYPOINT ["/app/kraken-hub"]
