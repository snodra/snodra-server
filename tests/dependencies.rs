//! The hub reads no event store and no message store (ADR 0027, 0028): no store client is built
//! into it or its image. A cheap stand-in for inspecting the image: the lock file the image is
//! built from (`cargo build --locked`) has no such crate, and the builder installs no C toolchain
//! for one.

/// Crates of store clients (and their native libraries) the hub must not link.
const STORE_CLIENTS: [&str; 12] = [
    "rdkafka",
    "rdkafka-sys",
    "zstd-sys",
    "rabbitmq-stream-client",
    "lapin",
    "mongodb",
    "bson",
    "sqlx",
    "sqlx-core",
    "sqlx-postgres",
    "sqlx-mysql",
    "testcontainers",
];

#[test]
fn hub_links_no_store_client() {
    let lock = include_str!("../Cargo.lock");
    for name in STORE_CLIENTS {
        assert!(
            !lock.contains(&format!("name = \"{name}\"")),
            "Cargo.lock has {name}"
        );
    }
    let dockerfile = include_str!("../Dockerfile");
    for word in ["cmake", "librdkafka", "libsasl2", "libzstd"] {
        assert!(!dockerfile.contains(word), "the Dockerfile mentions {word}");
    }
}
