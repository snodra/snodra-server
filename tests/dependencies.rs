//! The hub reads no event store (ADR 0027): no event-store client is built into it or its image.
//! A cheap stand-in for inspecting the image: the lock file the image is built from (`cargo build
//! --locked`) has no such crate, and the builder installs no C toolchain for one.

/// Crates of event-store clients (and their native libraries) the hub must not link.
const EVENT_STORE_CLIENTS: [&str; 4] = ["rdkafka", "rdkafka-sys", "zstd-sys", "testcontainers"];

#[test]
fn hub_image_has_no_librdkafka() {
    let lock = include_str!("../Cargo.lock");
    for name in EVENT_STORE_CLIENTS {
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
