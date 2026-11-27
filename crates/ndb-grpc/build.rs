fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true) // stress runner + integration tests drive a real client
        .compile_protos(&["../../proto/dbns/v1/ndb.proto"], &["../../proto"])?;
    Ok(())
}
