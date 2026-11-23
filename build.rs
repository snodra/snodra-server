fn main() -> Result<(), Box<dyn std::error::Error>> {
    // `PROTOC` wins (the release images and the developer's own protoc); otherwise use the binary
    // bundled with the build dependency, so a cross-compiled release needs no system package.
    if std::env::var_os("PROTOC").is_none()
        && let Ok(path) = protoc_bin_vendored::protoc_bin_path()
    {
        // SAFETY: a build script is single-threaded until it spawns, and this runs first.
        unsafe { std::env::set_var("PROTOC", path) };
    }
    // The crate version is the only source: CI writes it into Cargo.toml when it cuts a release, so
    // every build of a tag (the image, each binary, a local `cargo build`) reports the same version.
    let version = std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".into());
    println!("cargo:rustc-env=SNODRA_SERVER_BUILD_VERSION={version}");
    println!("cargo:rerun-if-changed=proto/hub.proto");
    // rust-embed reads ui/ at compile time; a new or changed asset must rebuild the crate.
    println!("cargo:rerun-if-changed=ui");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .build_transport(false)
        // Credentials (and its oneof, which prost skips with its message) carry a bearer token or
        // a JWT: their `Debug` is written by hand (src/protocol.rs) and prints the kind and
        // `***`, so no `{:?}` of a Register, a ClientMessage or a Credentials can leak it.
        .skip_debug([".snodra.hub.v1.Credentials"])
        .compile_protos(&["proto/hub.proto"], &["proto"])?;
    Ok(())
}
