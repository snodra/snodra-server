fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/hub.proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .build_transport(false)
        .compile_protos(&["proto/hub.proto"], &["proto"])?;
    Ok(())
}
