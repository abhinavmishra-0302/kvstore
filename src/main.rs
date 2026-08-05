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
    for entry in wal.iter_entries() {
        replay_entry(&entry, &store);
        replayed += 1;
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
        let response = handle_command(&line, &store, &wal);

        // Write the reply back, followed by a newline so simple clients
        // (like `nc`) render it cleanly.
        writer.write_all(response.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
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
                    if let Err(e) = write_and_sync(wal, &format!("SET {k} {v}")) {
                        return format!("ERR failed to persist write: {e}");
                    }
                    // DAY 3: lock the mutex, insert, then the lock is
                    // automatically released when `map` goes out of scope.
                    apply_set(store, k, v);
                    "OK".to_string()
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
                    if let Err(e) = write_and_sync(wal, &format!("DEL {k}")) {
                        return format!("ERR failed to persist write: {e}");
                    }
                    if apply_del(store, k) {
                        "1".to_string() // 1 key was deleted
                    } else {
                        "0".to_string() // nothing to delete
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

// STAGE 2: append one entry to the WAL and opportunistically sync.
// A sync failure is logged but NOT treated as a write failure - the
// entry is safely appended either way, it just isn't durable yet.
fn write_and_sync(wal: &SharedWal, entry: &str) -> std::io::Result<()> {
    let mut log = wal.lock().unwrap();
    log.append(entry)?;
    if let Err(e) = log.maybe_sync(WAL_SYNC_THRESHOLD) {
        eprintln!("WAL sync failed: {e}");
    }
    Ok(())
}

// The actual store mutations, factored out so both the live client
// path (after a successful WAL write) and startup replay (which must
// NOT re-append to the WAL) share the same logic.
fn apply_set(store: &SharedStore, key: &str, value: &str) {
    store.lock().unwrap().insert(key.to_string(), value.to_string());
}

fn apply_del(store: &SharedStore, key: &str) -> bool {
    store.lock().unwrap().remove(key).is_some()
}

// STAGE 2: replays a single WAL line into the store at startup.
// Malformed entries are logged and skipped rather than aborting
// recovery - a single corrupt line shouldn't lose the rest of history.
fn replay_entry(entry: &str, store: &SharedStore) {
    let mut parts = entry.trim().splitn(3, ' ');
    let cmd = parts.next().unwrap_or("").to_uppercase();

    match cmd.as_str() {
        "SET" => match (parts.next(), parts.next()) {
            (Some(k), Some(v)) => apply_set(store, k, v),
            _ => eprintln!("WAL replay: skipping malformed SET entry: {entry:?}"),
        },
        "DEL" => match parts.next() {
            Some(k) => {
                apply_del(store, k);
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
        let fresh_store: SharedStore = Arc::new(Mutex::new(HashMap::new()));
        let wal_path = env.wal_path.clone();
        let recovery_wal = WriteAheadLog::new(&wal_path).expect("reopen WAL");
        for entry in recovery_wal.iter_entries() {
            replay_entry(&entry, &fresh_store);
        }

        let map = fresh_store.lock().unwrap();
        assert_eq!(map.get("name"), Some(&"alice".to_string()));
        assert_eq!(map.get("city"), None);
    }
}