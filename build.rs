fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        .skip_debug([".kraken.hub.v1.Credentials"])
        .compile_protos(&["proto/hub.proto"], &["proto"])?;
    Ok(())
}
