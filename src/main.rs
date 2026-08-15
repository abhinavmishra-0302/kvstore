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
// ============================================================

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

mod wal;
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

fn main() -> std::io::Result<()> {
    // DAY 1: bind a TCP listener - this is the socket clients connect to.
    let listener = TcpListener::bind("127.0.0.1:6380")?;
    println!("kvstore listening on 127.0.0.1:6380");

    // DAY 3: create the shared store once, before accepting any clients.
    let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));

    // STAGE 2: open the WAL and replay it into the store BEFORE we
    // accept any client connections, so every client sees the fully
    // recovered state from the first command it sends.
    let wal = WriteAheadLog::new(WAL_PATH)?;
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
        println!("recovered {replayed} entries from {WAL_PATH}");
    }
    let wal: SharedWal = Arc::new(Mutex::new(wal));

    // DAY 1 (loop) + DAY 4 (thread-per-connection):
    // incoming() gives us each new connection as it arrives.
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Clone the Arcs (cheap - just bumps a reference count),
                // so this thread gets its own handle to the SAME store
                // and the SAME WAL.
                let store_ref = Arc::clone(&store);
                let wal_ref = Arc::clone(&wal);

                // DAY 4: spawn a new OS thread per client so multiple
                // clients can be connected and issuing commands at once.
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, store_ref, wal_ref) {
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
fn handle_client(stream: TcpStream, store: SharedStore, wal: SharedWal) -> std::io::Result<()> {
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
        let mut response = handle_command(&line, &store, &wal);

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
// + STAGE 2 (WAL write-before-apply for mutations).
fn handle_command(line: &str, store: &SharedStore, wal: &SharedWal) -> String {
    // split_whitespace() gives us command + args without extra allocation
    // pain. We only split into at most 3 parts so that a value containing
    // spaces (e.g. `SET name "abhinav singh"`) doesn't get chopped up.
    let mut parts = line.trim().splitn(3, ' ');
    let cmd = parts.next().unwrap_or("").to_uppercase();

    match cmd.as_str() {
        "SET" => {
            let key = parts.next();
            let value = parts.next();
            match (key, value) {
                (Some(k), Some(v)) => {
                    // STAGE 2 / transactional semantics: the WAL write
                    // must succeed BEFORE we touch the in-memory store.
                    // If it fails, the client is told and nothing changes -
                    // we never want the store ahead of what's durable.
                    match commit(store, wal, &format!("SET {k} {v}"), |map| apply_set(map, k, v)) {
                        Ok(()) => "OK".to_string(),
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
                    let map = store.lock().unwrap();
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
            let key = parts.next();
            match key {
                Some(k) => {
                    // Log the delete before applying it, same as SET - and
                    // unconditionally, even if the key turns out not to
                    // exist, so replay stays a faithful redo of history.
                    match commit(store, wal, &format!("DEL {k}"), |map| apply_del(map, k)) {
                        Ok(true) => "1".to_string(),  // 1 key was deleted
                        Ok(false) => "0".to_string(), // nothing to delete
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
            let mut log = wal.lock().unwrap();
            match log.sync() {
                Ok(()) => "OK".to_string(),
                Err(e) => format!("ERR sync failed: {e}"),
            }
        }
        "" => "ERR empty command".to_string(),
        other => format!("ERR unknown command '{other}'"),
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
        store: SharedStore,
        wal: SharedWal,
        wal_path: String,
    }

    impl TestEnv {
        fn new(name: &str) -> Self {
            let wal_path = format!("test_main_{name}.log");
            std::fs::remove_file(&wal_path).ok();
            let wal = WriteAheadLog::new(&wal_path).expect("failed to open test WAL");
            TestEnv {
                store: Arc::new(Mutex::new(HashMap::new())),
                wal: Arc::new(Mutex::new(wal)),
                wal_path,
            }
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
        let response = handle_command("SET name alice", &env.store, &env.wal);
        assert_eq!(response, "OK");

        let response = handle_command("GET name", &env.store, &env.wal);
        assert_eq!(response, "alice");
    }

    #[test]
    fn test_get_missing() {
        let env = TestEnv::new("get_missing");
        let response = handle_command("GET missing", &env.store, &env.wal);
        assert_eq!(response, "(nil)");
    }

    #[test]
    fn test_del() {
        let env = TestEnv::new("del");
        handle_command("SET key value", &env.store, &env.wal);
        let response = handle_command("DEL key", &env.store, &env.wal);
        assert_eq!(response, "1");

        let response = handle_command("DEL key", &env.store, &env.wal);
        assert_eq!(response, "0");
    }

    #[test]
    fn test_malformed_command() {
        let env = TestEnv::new("malformed_command");
        let response = handle_command("SET key", &env.store, &env.wal);
        assert!(response.contains("ERR"));
    }

    #[test]
    fn test_ping() {
        let env = TestEnv::new("ping");
        let response = handle_command("PING", &env.store, &env.wal);
        assert_eq!(response, "PONG");
    }

    #[test]
    fn test_unknown_command() {
        let env = TestEnv::new("unknown_command");
        let response = handle_command("FOO bar", &env.store, &env.wal);
        assert!(response.contains("ERR"));
    }

    #[test]
    fn test_set_overwrites_existing_key() {
        let env = TestEnv::new("set_overwrites_existing_key");
        handle_command("SET key first", &env.store, &env.wal);
        handle_command("SET key second", &env.store, &env.wal);
        let response = handle_command("GET key", &env.store, &env.wal);
        assert_eq!(response, "second");
    }

    #[test]
    fn test_sync_command() {
        let env = TestEnv::new("sync_command");
        handle_command("SET key value", &env.store, &env.wal);
        let response = handle_command("SYNC", &env.store, &env.wal);
        assert_eq!(response, "OK");
    }

    // STAGE 2's whole point: a fresh store, replaying a WAL written by
    // real command handling, ends up with the same data.
    #[test]
    fn test_restart_recovers_data_via_replay() {
        let env = TestEnv::new("restart_recovers_data_via_replay");
        handle_command("SET name alice", &env.store, &env.wal);
        handle_command("SET city nyc", &env.store, &env.wal);
        handle_command("DEL city", &env.store, &env.wal);
        env.wal.lock().unwrap().sync().unwrap();

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
        let probe_target = Arc::clone(&env.wal);

        let wal_was_locked = commit(&env.store, &env.wal, "SET k v", |map| {
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
                    let store = Arc::clone(&env.store);
                    let wal = Arc::clone(&env.wal);
                    thread::spawn(move || {
                        handle_command(&format!("SET k v{i}"), &store, &wal);
                    })
                })
                .collect();
            for h in handles {
                h.join().expect("writer thread panicked");
            }

            let in_memory = env.store.lock().unwrap().get("k").cloned();
            let after_restart = replay_file(&env.wal_path).get("k").cloned();
            assert_eq!(
                in_memory, after_restart,
                "attempt {attempt}: store says {in_memory:?} but a restart would give {after_restart:?}"
            );
        }
    }
}
