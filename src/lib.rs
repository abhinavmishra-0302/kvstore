// ============================================================
// kvstore - a small, durable, replicated key-value store.
//
// The crate is organized in layers, each one built on the one above:
//   wal          Stage 2: durable append-only log
//   store        the in-memory data + the WAL-then-store commit path
//   commands     the client protocol: parsing, dispatch, replication hooks
//   replication  Stage 3: node config, leader-side fan-out, follower intake
//   server       process bootstrap and the TCP connection loop
//
// `src/main.rs` is a thin binary shim over `run()` below. Exposing the
// server as a library (rather than folding everything into main.rs)
// is what lets `tests/integration_test.rs` spin up real nodes and
// talk to them over real sockets, the same way a client or another
// node would.
// ============================================================

pub mod commands;
pub mod replication;
pub mod server;
pub mod store;
pub mod wal;

pub use replication::ReplicationConfig;
pub use server::run;
