fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/hub.proto");
    // rust-embed reads ui/ at compile time; a new or changed asset must rebuild the crate.
    println!("cargo:rerun-if-changed=ui");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .build_transport(false)
        .compile_protos(&["proto/hub.proto"], &["proto"])?;
    Ok(())
}
