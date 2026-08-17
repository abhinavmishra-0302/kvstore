// ============================================================
// kvstore - binary entry point.
//
// All the actual logic lives in the library (src/lib.rs and its
// submodules) - this file just reads CLI flags and starts the server.
// See src/lib.rs for the module map.
// ============================================================

fn main() -> std::io::Result<()> {
    let config = kvstore::ReplicationConfig::from_env();
    kvstore::run(config)
}
