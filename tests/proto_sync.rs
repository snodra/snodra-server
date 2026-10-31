//! The vendored proto must stay byte-identical to the umbrella's `kraken/protocol/hub.proto`.
use std::path::Path;

#[test]
fn vendored_proto_matches_umbrella() {
    let candidates = [
        "../kraken/protocol/hub.proto",
        "../../../kraken/protocol/hub.proto",
    ];
    let Some(umbrella) = candidates.iter().map(Path::new).find(|p| p.exists()) else {
        eprintln!("umbrella checkout not found next to this repo, skipping proto drift check");
        return;
    };
    let vendored = std::fs::read("proto/hub.proto").expect("vendored proto");
    let source = std::fs::read(umbrella).expect("umbrella proto");
    assert!(
        vendored == source,
        "proto/hub.proto drifted from {}: run `cp {} proto/hub.proto`",
        umbrella.display(),
        umbrella.display()
    );
}
