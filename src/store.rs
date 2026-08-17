// ============================================================
// store.rs - the in-memory data store and its durability contract.
//
// Owns the shared HashMap and WAL types, the single `commit` path
// that keeps them in agreement, the raw store mutations, and the
// replay logic used both at startup and (from commands.rs) when a
// follower applies a replicated entry.
// ============================================================

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::wal::WriteAheadLog;

// DAY 3: Shared state across all client connections.
// - HashMap<String, String> is our actual data store.
// - Mutex protects it so only one thread touches it at a time.
// - Arc lets many threads hold a reference to the *same* mutex
//   (Arc = "atomically reference counted" - a thread-safe
//   shared pointer). Without Arc, Rust's ownership rules won't
//   let multiple threads own the same HashMap.
pub type SharedStore = Arc<Mutex<HashMap<String, String>>>;

// STAGE 2: same Arc<Mutex<...>> pattern, but for the WAL's file
// handle - every client thread needs to be able to append to it.
pub type SharedWal = Arc<Mutex<WriteAheadLog>>;

/// Default WAL filename for a node that wasn't given a `--node-id`.
pub const WAL_PATH: &str = "data.log";

// STAGE 2: how many WAL writes we let accumulate before paying for
// an fsync. Bigger = higher throughput, but more writes are at risk
// of being lost (not corrupted - just not yet durable) if the
// process dies between syncs. See spec's "Performance Considerations".
pub const WAL_SYNC_THRESHOLD: u64 = 100;

// STAGE 3: node-id-scoped WAL path, so a leader and follower running
// on the same machine (the common case for local testing) don't both
// try to write to the same data.log. `--node-id` is optional, so a
// plain `cargo run` with no flags keeps behaving exactly like Stage 2.
pub fn wal_path_for(node_id: &str) -> String {
    if node_id == "default" {
        WAL_PATH.to_string()
    } else {
        format!("data-{node_id}.log")
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
pub fn commit<T>(
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
pub fn apply_set(map: &mut HashMap<String, String>, key: &str, value: &str) {
    map.insert(key.to_string(), value.to_string());
}

pub fn apply_del(map: &mut HashMap<String, String>, key: &str) -> bool {
    map.remove(key).is_some()
}

// STAGE 2: replays a single WAL line into the store. Used both at
// startup (via `replay_into`) and directly by tests. Malformed
// entries are logged and skipped rather than aborting recovery - a
// single corrupt line shouldn't lose the rest of history.
pub fn replay_entry(entry: &str, map: &mut HashMap<String, String>) {
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

// STAGE 2: replays every entry in `wal` into `store`, in order. Called
// once at startup, before any client or replication connection is
// accepted, so the very first command handled already sees recovered
// state. Returns how many entries were applied, purely for logging.
pub fn replay_into(wal: &WriteAheadLog, store: &SharedStore) -> usize {
    let mut map = store.lock().unwrap();
    let mut replayed = 0usize;
    for entry in wal.iter_entries() {
        replay_entry(&entry, &mut map);
        replayed += 1;
    }
    replayed
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every test gets its own WAL file (named after the test) since
    // cargo runs tests in parallel and a shared file would race. The
    // guard removes the file on drop so repeated runs start clean.
    struct TempWal {
        path: String,
        wal: SharedWal,
        store: SharedStore,
    }

    impl TempWal {
        fn new(name: &str) -> Self {
            let path = format!("test_store_{name}.log");
            std::fs::remove_file(&path).ok();
            let wal = WriteAheadLog::new(&path).expect("open WAL");
            TempWal {
                path,
                wal: Arc::new(Mutex::new(wal)),
                store: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    impl Drop for TempWal {
        fn drop(&mut self) {
            std::fs::remove_file(&self.path).ok();
        }
    }

    #[test]
    fn test_apply_set_and_del() {
        let mut map = HashMap::new();
        apply_set(&mut map, "k", "v");
        assert_eq!(map.get("k"), Some(&"v".to_string()));
        assert!(apply_del(&mut map, "k"));
        assert!(!apply_del(&mut map, "k"));
    }

    #[test]
    fn test_replay_entry_applies_set_and_del() {
        let mut map = HashMap::new();
        replay_entry("SET name alice", &mut map);
        replay_entry("SET city nyc", &mut map);
        replay_entry("DEL city", &mut map);
        assert_eq!(map.get("name"), Some(&"alice".to_string()));
        assert_eq!(map.get("city"), None);
    }

    #[test]
    fn test_replay_entry_skips_malformed_lines() {
        let mut map = HashMap::new();
        replay_entry("SET onlykey", &mut map); // missing value
        replay_entry("NOPE whatever", &mut map); // unrecognized command
        assert!(map.is_empty());
    }

    #[test]
    fn test_commit_appends_to_wal_and_applies_to_store() {
        let t = TempWal::new("commit_basic");
        let result = commit(&t.store, &t.wal, "SET k v", |map| apply_set(map, "k", "v"));
        assert!(result.is_ok());
        assert_eq!(t.store.lock().unwrap().get("k"), Some(&"v".to_string()));

        let entries: Vec<_> = t.wal.lock().unwrap().iter_entries().collect();
        assert_eq!(entries, vec!["SET k v".to_string()]);
    }

    #[test]
    fn test_replay_into_recovers_store_from_wal() {
        let t = TempWal::new("replay_into");
        commit(&t.store, &t.wal, "SET name alice", |map| {
            apply_set(map, "name", "alice")
        })
        .unwrap();
        commit(&t.store, &t.wal, "DEL name", |map| apply_del(map, "name")).unwrap();

        let fresh_store: SharedStore = Arc::new(Mutex::new(HashMap::new()));
        let wal_for_replay = WriteAheadLog::new(&t.path).expect("reopen WAL");
        let replayed = replay_into(&wal_for_replay, &fresh_store);

        assert_eq!(replayed, 2);
        assert_eq!(fresh_store.lock().unwrap().get("name"), None);
    }

    // REGRESSION (the real guard): `commit` must still hold the WAL lock
    // at the moment it updates the store. An earlier version released the
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
        let t = TempWal::new("commit_atomicity");
        let probe_target = Arc::clone(&t.wal);

        let wal_was_locked = commit(&t.store, &t.wal, "SET k v", |map| {
            apply_set(map, "k", "v");
            // Probe from a separate thread so "already locked" is
            // unambiguous - re-locking from the holding thread is not
            // well-defined behaviour.
            std::thread::spawn(move || probe_target.try_lock().is_err())
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
}
