// ============================================================
// kvstore - a tiny in-memory key-value server (Week 1 build)
//
// This file is written to show the week's progression. Read the
// "DAY N" comments top to bottom - each one is the feature that
// day added. By the end (Day 5) this is a complete, working,
// concurrent single-node KV store.
//
// STAGE 2 adds durability: SET/DEL are written to a write-ahead
// log (wal.rs) before they touch the in-memory store, and that
// log is replayed on startup so a restart doesn't lose data.
//
// STAGE 3 adds replication: a leader forwards every committed
// SET/DEL to registered followers (replication.rs); a follower
// applies whatever it receives to its own store + WAL, so it has
// an independently recoverable copy. Run with no flags and this
// is still a plain standalone node, same as Stage 1/2.
// ============================================================

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

mod replication;
mod wal;

use replication::{
    register_follower, FollowerListener, ReplicationConfig, Replicator, SharedReplicator,
};
use wal::WriteAheadLog;

// DAY 3: Shared state across all client connections.
// - HashMap<String, String> is our actual data store.
// - Mutex protects it so only one thread touches it at a time.
// - Arc lets many threads hold a reference to the *same* mutex
//   (Arc = "atomically reference counted" - a thread-safe
//   shared pointer). Without Arc, Rust's ownership rules won't
//   let multiple threads own the same HashMap.
type SharedStore = Arc<Mutex<HashMap<String, String>>>;

// STAGE 2: same Arc<Mutex<...>> pattern, but for the WAL's file
// handle - every client thread needs to be able to append to it.
type SharedWal = Arc<Mutex<WriteAheadLog>>;

const WAL_PATH: &str = "data.log";

// STAGE 2: how many WAL writes we let accumulate before paying for
// an fsync. Bigger = higher throughput, but more writes are at risk
// of being lost (not corrupted - just not yet durable) if the
// process dies between syncs. See spec's "Performance Considerations".
const WAL_SYNC_THRESHOLD: u64 = 100;

// STAGE 3: everything a client-handling thread needs. Bundled into
// one Clone-able struct instead of four loose parameters, since every
// connection thread needs its own handle to all of it.
#[derive(Clone)]
struct ServerContext {
    store: SharedStore,
    wal: SharedWal,
    // Some(...) only on a leader; used to fan writes out to followers.
    replicator: Option<SharedReplicator>,
    // true only on a follower: it may serve GET/PING, but SET/DEL are
    // rejected since the leader is the only source of truth for writes.
    read_only: bool,
}

// STAGE 3: node-id-scoped WAL path, so a leader and follower running
// on the same machine (the common case for local testing) don't both
// try to write to the same data.log. `--node-id` is optional, so a
// plain `cargo run` with no flags keeps behaving exactly like Stage 2.
fn wal_path_for(node_id: &str) -> String {
    if node_id == "default" {
        WAL_PATH.to_string()
    } else {
        format!("data-{node_id}.log")
    }
}

fn main() -> std::io::Result<()> {
    let config = ReplicationConfig::from_env();
    if config.is_leader && config.leader_addr.is_some() {
        eprintln!("warning: --is-leader and --leader-addr both given; running as leader, ignoring --leader-addr");
    }
    // A leader is never also a follower, no matter what --leader-addr says.
    let leader_addr = if config.is_leader {
        None
    } else {
        config.leader_addr.clone()
    };
    let read_only = leader_addr.is_some();

    // DAY 1: bind a TCP listener - this is the socket clients connect to.
    let listener = TcpListener::bind(("127.0.0.1", config.listen_port))?;

    // DAY 3: create the shared store once, before accepting any clients.
    let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));

    // STAGE 2: open the WAL and replay it into the store BEFORE we
    // accept any client connections, so every client sees the fully
    // recovered state from the first command it sends.
    let wal_path = wal_path_for(&config.node_id);
    let wal = WriteAheadLog::new(&wal_path)?;
    let mut replayed = 0usize;
    {
        // Nothing else is running yet, so take the lock once for the
        // whole replay rather than re-acquiring it per entry.
        let mut map = store.lock().unwrap();
        for entry in wal.iter_entries() {
            replay_entry(&entry, &mut map);
            replayed += 1;
        }
    }
    if replayed > 0 {
        println!(
            "[{}] recovered {replayed} entries from {wal_path}",
            config.node_id
        );
    }
    let wal: SharedWal = Arc::new(Mutex::new(wal));

    // STAGE 3: if we're a leader, accept follower registrations on a
    // separate port and hand each one to a Replicator. If we're a
    // follower, connect out to the leader and apply whatever it sends.
    // A standalone node (no --is-leader, no --leader-addr) does neither.
    let replicator: Option<SharedReplicator> = if config.is_leader {
        let replicator: SharedReplicator = Arc::new(Mutex::new(Replicator::new()));
        let repl_listener = TcpListener::bind(("127.0.0.1", config.repl_port))?;
        println!(
            "[{}] accepting follower registrations on 127.0.0.1:{}",
            config.node_id, config.repl_port
        );
        let replicator_ref = Arc::clone(&replicator);
        thread::spawn(move || {
            for stream in repl_listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let replicator_ref = Arc::clone(&replicator_ref);
                        thread::spawn(move || {
                            if let Err(e) = register_follower(stream, &replicator_ref) {
                                eprintln!("replication: follower registration failed: {e}");
                            }
                        });
                    }
                    Err(e) => eprintln!("replication: failed to accept follower connection: {e}"),
                }
            }
        });
        Some(replicator)
    } else {
        None
    };

    if let Some(leader_addr) = leader_addr {
        println!(
            "[{}] replicating from leader at {leader_addr}",
            config.node_id
        );
        let store_ref = Arc::clone(&store);
        let wal_ref = Arc::clone(&wal);
        let node_id = config.node_id.clone();
        thread::spawn(move || {
            let follower = FollowerListener::new(leader_addr, node_id);
            follower.connect_and_listen(|entry| replicate_apply(&store_ref, &wal_ref, entry));
        });
    }

    println!(
        "[{}] listening for clients on 127.0.0.1:{}{}",
        config.node_id,
        config.listen_port,
        if read_only {
            " (read-only follower)"
        } else {
            ""
        }
    );

    let ctx = ServerContext {
        store,
        wal,
        replicator,
        read_only,
    };

    // DAY 1 (loop) + DAY 4 (thread-per-connection):
    // incoming() gives us each new connection as it arrives.
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Cheap clone (bumps the inner Arcs' refcounts), so this
                // thread gets its own handle to the SAME store/WAL/replicator.
                let ctx = ctx.clone();

                // DAY 4: spawn a new OS thread per client so multiple
                // clients can be connected and issuing commands at once.
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, ctx) {
                        eprintln!("connection error: {e}");
                    }
                });
            }
            Err(e) => eprintln!("failed to accept connection: {e}"),
        }
    }

    Ok(())
}

// Handles a single client connection for its entire lifetime.
fn handle_client(stream: TcpStream, ctx: ServerContext) -> std::io::Result<()> {
    let peer = stream.peer_addr()?;
    println!("client connected: {peer}");

    // A strict request/response protocol never wants Nagle's algorithm:
    // it holds small writes back waiting for more data to coalesce, but
    // there is no more data coming until the client sees this reply.
    stream.set_nodelay(true)?;

    let reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    // DAY 1: read the connection line by line (each command is one line,
    // terminated by \n - e.g. what you'd type into `nc localhost 6380`).
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            // DAY 5: if the client disconnects mid-read, `lines()` will
            // error out. We break the loop instead of panicking, so one
            // bad/dropped client can't take down the whole server.
            Err(_) => break,
        };

        if line.trim().is_empty() {
            continue;
        }

        // DAY 2: parse the line into a command + arguments.
        let mut response = handle_command(&line, &ctx);

        // Write the reply back with its terminating newline in ONE write.
        // Sending the newline as a second write makes it a tiny trailing
        // segment that Nagle holds until the peer ACKs the first one -
        // and the peer's delayed-ACK timer sits on that for ~40ms, which
        // capped this server at roughly 25 responses/sec.
        response.push('\n');
        writer.write_all(response.as_bytes())?;
    }

    println!("client disconnected: {peer}");
    Ok(())
}

// DAY 2 (parsing) + DAY 3 (actual storage logic) + DAY 5 (error handling)
// + STAGE 2 (WAL write-before-apply for mutations)
// + STAGE 3 (read-only rejection + replication fan-out on the leader).
fn handle_command(line: &str, ctx: &ServerContext) -> String {
    // split_whitespace() gives us command + args without extra allocation
    // pain. We only split into at most 3 parts so that a value containing
    // spaces (e.g. `SET name "abhinav singh"`) doesn't get chopped up.
    let mut parts = line.trim().splitn(3, ' ');
    let cmd = parts.next().unwrap_or("").to_uppercase();

    match cmd.as_str() {
        "SET" => {
            // STAGE 3: a follower's store is only ever supposed to change
            // via replicated commands, never a direct client write.
            if ctx.read_only {
                return "ERR write commands are not allowed on a read-only follower".to_string();
            }
            let key = parts.next();
            let value = parts.next();
            match (key, value) {
                (Some(k), Some(v)) => {
                    // STAGE 2 / transactional semantics: the WAL write
                    // must succeed BEFORE we touch the in-memory store.
                    // If it fails, the client is told and nothing changes -
                    // we never want the store ahead of what's durable.
                    let entry = format!("SET {k} {v}");
                    match commit(&ctx.store, &ctx.wal, &entry, |map| apply_set(map, k, v)) {
                        Ok(()) => {
                            replicate(&ctx.replicator, &entry);
                            "OK".to_string()
                        }
                        Err(e) => format!("ERR failed to persist write: {e}"),
                    }
                }
                // DAY 5: malformed command - don't crash, just tell the client.
                _ => "ERR wrong number of arguments for 'SET'".to_string(),
            }
        }
        "GET" => {
            // Reads never touch the WAL - there's nothing to make durable.
            let key = parts.next();
            match key {
                Some(k) => {
                    let map = ctx.store.lock().unwrap();
                    match map.get(k) {
                        Some(v) => v.clone(),
                        // DAY 5: missing key is not an error, it's a normal
                        // case - return a sentinel value like Redis does.
                        None => "(nil)".to_string(),
                    }
                }
                None => "ERR wrong number of arguments for 'GET'".to_string(),
            }
        }
        "DEL" => {
            if ctx.read_only {
                return "ERR write commands are not allowed on a read-only follower".to_string();
            }
            let key = parts.next();
            match key {
                Some(k) => {
                    // Log the delete before applying it, same as SET - and
                    // unconditionally, even if the key turns out not to
                    // exist, so replay stays a faithful redo of history.
                    let entry = format!("DEL {k}");
                    match commit(&ctx.store, &ctx.wal, &entry, |map| apply_del(map, k)) {
                        Ok(deleted) => {
                            replicate(&ctx.replicator, &entry);
                            if deleted {
                                "1".to_string() // 1 key was deleted
                            } else {
                                "0".to_string() // nothing to delete
                            }
                        }
                        Err(e) => format!("ERR failed to persist write: {e}"),
                    }
                }
                None => "ERR wrong number of arguments for 'DEL'".to_string(),
            }
        }
        "PING" => "PONG".to_string(),
        // STAGE 2 (optional, per spec): let a client force an immediate
        // fsync instead of waiting for the batch threshold.
        "SYNC" => {
            let mut log = ctx.wal.lock().unwrap();
            match log.sync() {
                Ok(()) => "OK".to_string(),
                Err(e) => format!("ERR sync failed: {e}"),
            }
        }
        "" => "ERR empty command".to_string(),
        other => format!("ERR unknown command '{other}'"),
    }
}

// STAGE 3: fan a just-committed entry out to every registered follower.
// No-op on a standalone node or a follower (neither has a Replicator).
fn replicate(replicator: &Option<SharedReplicator>, entry: &str) {
    if let Some(repl) = replicator {
        repl.lock().unwrap().replicate_to_all(entry);
    }
}

// STAGE 3: applies one entry received from the leader. Reuses `commit`
// so a follower gets the exact same WAL-then-store durability guarantee
// as a leader handling a live client write - it just never replicates
// further (no multi-hop replication) and never rejects on `read_only`
// (that check is for client-facing writes, not the replication stream).
fn replicate_apply(store: &SharedStore, wal: &SharedWal, entry: &str) {
    let mut parts = entry.splitn(3, ' ');
    let cmd = parts.next().unwrap_or("").to_uppercase();

    let result = match cmd.as_str() {
        "SET" => match (parts.next(), parts.next()) {
            (Some(k), Some(v)) => commit(store, wal, entry, |map| apply_set(map, k, v)),
            _ => {
                eprintln!("replication: skipping malformed SET entry: {entry:?}");
                return;
            }
        },
        "DEL" => match parts.next() {
            Some(k) => commit(store, wal, entry, |map| {
                apply_del(map, k);
            }),
            None => {
                eprintln!("replication: skipping malformed DEL entry: {entry:?}");
                return;
            }
        },
        _ => {
            eprintln!("replication: skipping unrecognized entry: {entry:?}");
            return;
        }
    };

    if let Err(e) = result {
        eprintln!("replication: failed to persist replicated entry: {e}");
    }
}

// STAGE 2: append one entry to the WAL, then apply it to the store -
// as a SINGLE atomic step.
//
// Holding the WAL lock across the in-memory update is the whole point.
// An earlier version released it first, which let two threads append in
// one order and apply in the opposite one: memory would serve the value
// from the last writer to grab the store lock, while the log recorded
// the other. Nobody noticed until a restart replayed the log and the
// server silently started serving a different value.
//
// Deadlock safety: this is the only place that holds both locks, and it
// always takes them in the same order (WAL, then store). Every other
// path takes just one - GET and replay take the store, SYNC takes the
// WAL - so there is no cycle to deadlock on.
//
// A sync failure is logged but NOT treated as a write failure: the
// entry is appended either way, it just isn't durable yet.
fn commit<T>(
    store: &SharedStore,
    wal: &SharedWal,
    entry: &str,
    apply: impl FnOnce(&mut HashMap<String, String>) -> T,
) -> std::io::Result<T> {
    let mut log = wal.lock().unwrap();
    log.append(entry)?;
    if let Err(e) = log.maybe_sync(WAL_SYNC_THRESHOLD) {
        eprintln!("WAL sync failed: {e}");
    }
    let mut map = store.lock().unwrap();
    Ok(apply(&mut map))
}

// The actual store mutations, factored out so both the live client
// path (inside `commit`, after a successful WAL write) and startup
// replay (which must NOT re-append to the WAL) share the same logic.
// They take the map directly rather than the mutex, so the caller
// decides how long the lock is held.
fn apply_set(map: &mut HashMap<String, String>, key: &str, value: &str) {
    map.insert(key.to_string(), value.to_string());
}

fn apply_del(map: &mut HashMap<String, String>, key: &str) -> bool {
    map.remove(key).is_some()
}

// STAGE 2: replays a single WAL line into the store at startup.
// Malformed entries are logged and skipped rather than aborting
// recovery - a single corrupt line shouldn't lose the rest of history.
fn replay_entry(entry: &str, map: &mut HashMap<String, String>) {
    let mut parts = entry.trim().splitn(3, ' ');
    let cmd = parts.next().unwrap_or("").to_uppercase();

    match cmd.as_str() {
        "SET" => match (parts.next(), parts.next()) {
            (Some(k), Some(v)) => apply_set(map, k, v),
            _ => eprintln!("WAL replay: skipping malformed SET entry: {entry:?}"),
        },
        "DEL" => match parts.next() {
            Some(k) => {
                apply_del(map, k);
            }
            None => eprintln!("WAL replay: skipping malformed DEL entry: {entry:?}"),
        },
        _ => eprintln!("WAL replay: skipping unrecognized entry: {entry:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every test gets its own WAL file (named after the test) since
    // cargo runs tests in parallel and a shared file would race. The
    // guard removes the file on drop so repeated runs start clean.
    struct TestEnv {
        ctx: ServerContext,
        wal_path: String,
    }

    impl TestEnv {
        fn new(name: &str) -> Self {
            let wal_path = format!("test_main_{name}.log");
            std::fs::remove_file(&wal_path).ok();
            let wal = WriteAheadLog::new(&wal_path).expect("failed to open test WAL");
            let ctx = ServerContext {
                store: Arc::new(Mutex::new(HashMap::new())),
                wal: Arc::new(Mutex::new(wal)),
                replicator: None,
                read_only: false,
            };
            TestEnv { ctx, wal_path }
        }

        fn read_only(name: &str) -> Self {
            let mut env = Self::new(name);
            env.ctx.read_only = true;
            env
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            std::fs::remove_file(&self.wal_path).ok();
        }
    }

    #[test]
    fn test_set_get() {
        let env = TestEnv::new("set_get");
        let response = handle_command("SET name alice", &env.ctx);
        assert_eq!(response, "OK");

        let response = handle_command("GET name", &env.ctx);
        assert_eq!(response, "alice");
    }

    #[test]
    fn test_get_missing() {
        let env = TestEnv::new("get_missing");
        let response = handle_command("GET missing", &env.ctx);
        assert_eq!(response, "(nil)");
    }

    #[test]
    fn test_del() {
        let env = TestEnv::new("del");
        handle_command("SET key value", &env.ctx);
        let response = handle_command("DEL key", &env.ctx);
        assert_eq!(response, "1");

        let response = handle_command("DEL key", &env.ctx);
        assert_eq!(response, "0");
    }

    #[test]
    fn test_malformed_command() {
        let env = TestEnv::new("malformed_command");
        let response = handle_command("SET key", &env.ctx);
        assert!(response.contains("ERR"));
    }

    #[test]
    fn test_ping() {
        let env = TestEnv::new("ping");
        let response = handle_command("PING", &env.ctx);
        assert_eq!(response, "PONG");
    }

    #[test]
    fn test_unknown_command() {
        let env = TestEnv::new("unknown_command");
        let response = handle_command("FOO bar", &env.ctx);
        assert!(response.contains("ERR"));
    }

    #[test]
    fn test_set_overwrites_existing_key() {
        let env = TestEnv::new("set_overwrites_existing_key");
        handle_command("SET key first", &env.ctx);
        handle_command("SET key second", &env.ctx);
        let response = handle_command("GET key", &env.ctx);
        assert_eq!(response, "second");
    }

    #[test]
    fn test_sync_command() {
        let env = TestEnv::new("sync_command");
        handle_command("SET key value", &env.ctx);
        let response = handle_command("SYNC", &env.ctx);
        assert_eq!(response, "OK");
    }

    // STAGE 2's whole point: a fresh store, replaying a WAL written by
    // real command handling, ends up with the same data.
    #[test]
    fn test_restart_recovers_data_via_replay() {
        let env = TestEnv::new("restart_recovers_data_via_replay");
        handle_command("SET name alice", &env.ctx);
        handle_command("SET city nyc", &env.ctx);
        handle_command("DEL city", &env.ctx);
        env.ctx.wal.lock().unwrap().sync().unwrap();

        // Simulate a restart: fresh store, replay the same WAL file.
        let map = replay_file(&env.wal_path);
        assert_eq!(map.get("name"), Some(&"alice".to_string()));
        assert_eq!(map.get("city"), None);
    }

    // Rebuilds a store from a WAL file the way a restart would.
    fn replay_file(path: &str) -> HashMap<String, String> {
        let mut map = HashMap::new();
        let wal = WriteAheadLog::new(path).expect("reopen WAL");
        for entry in wal.iter_entries() {
            replay_entry(&entry, &mut map);
        }
        map
    }

    // REGRESSION (the real guard): `commit` must still hold the WAL lock
    // at the moment it updates the store. The previous code released the
    // WAL lock first, which let two writers append in one order and apply
    // in the opposite one - memory then served one writer's value while
    // the log recorded another's, and the server silently changed its
    // answer after a restart.
    //
    // This asserts the invariant directly rather than racing for it, so
    // it fails 100% of the time if the ordering regresses. A probabilistic
    // version needed thousands of iterations and only reproduced when an
    // fsync happened to widen the window - far too flaky to rely on.
    #[test]
    fn commit_holds_wal_lock_while_updating_store() {
        let env = TestEnv::new("commit_atomicity");
        let probe_target = Arc::clone(&env.ctx.wal);

        let wal_was_locked = commit(&env.ctx.store, &env.ctx.wal, "SET k v", |map| {
            apply_set(map, "k", "v");
            // Probe from a separate thread so "already locked" is
            // unambiguous - re-locking from the holding thread is not
            // well-defined behaviour.
            thread::spawn(move || probe_target.try_lock().is_err())
                .join()
                .expect("probe thread panicked")
        })
        .expect("commit failed");

        assert!(
            wal_was_locked,
            "commit released the WAL lock before updating the store: concurrent \
             writers can interleave, leaving memory disagreeing with the log"
        );
    }

    // Companion smoke test: under real concurrent writers, the in-memory
    // value must be the one a restart would reconstruct. This alone is
    // too timing-dependent to catch the bug reliably (see above), but it
    // exercises the genuine multi-threaded path end to end.
    #[test]
    fn concurrent_writes_keep_store_and_wal_in_agreement() {
        for attempt in 0..500 {
            let env = TestEnv::new(&format!("race_{attempt}"));

            let handles: Vec<_> = (0..8)
                .map(|i| {
                    let ctx = env.ctx.clone();
                    thread::spawn(move || {
                        handle_command(&format!("SET k v{i}"), &ctx);
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("writer thread panicked");
            }

            let in_memory = env.ctx.store.lock().unwrap().get("k").cloned();
            let after_restart = replay_file(&env.wal_path).get("k").cloned();
            assert_eq!(
                in_memory, after_restart,
                "attempt {attempt}: store says {in_memory:?} but a restart would give {after_restart:?}"
            );
        }
    }

    // STAGE 3
    #[test]
    fn test_read_only_context_rejects_writes_but_allows_reads() {
        let env = TestEnv::read_only("read_only_rejects_writes");
        assert!(handle_command("SET k v", &env.ctx).contains("ERR"));
        assert!(handle_command("DEL k", &env.ctx).contains("ERR"));
        assert_eq!(handle_command("GET k", &env.ctx), "(nil)");
        assert_eq!(handle_command("PING", &env.ctx), "PONG");
    }

    #[test]
    fn test_leader_replicates_committed_write_to_follower() {
        let mut env = TestEnv::new("replicates_to_follower");

        // Stand in for a follower: accept one connection and read what
        // gets written to it.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (accepted, _) = listener.accept().unwrap();

        let mut replicator = Replicator::new();
        replicator.add_follower("f1".to_string(), accepted);
        env.ctx.replicator = Some(Arc::new(Mutex::new(replicator)));

        let response = handle_command("SET name alice", &env.ctx);
        assert_eq!(response, "OK");

        let mut reader = BufReader::new(client);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "SET name alice");
    }

    #[test]
    fn test_replicate_apply_updates_store_and_wal_without_reentrant_replication() {
        let env = TestEnv::new("replicate_apply");
        replicate_apply(&env.ctx.store, &env.ctx.wal, "SET name alice");
        replicate_apply(&env.ctx.store, &env.ctx.wal, "DEL name");

        assert_eq!(env.ctx.store.lock().unwrap().get("name"), None);
        let map = replay_file(&env.wal_path);
        assert_eq!(map.get("name"), None);
    }
}
