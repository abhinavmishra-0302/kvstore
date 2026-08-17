// ============================================================
// commands.rs - the client-facing protocol: parsing, dispatch, and
// the leader/follower behaviors layered on top of the store.
// ============================================================

use crate::replication::SharedReplicator;
use crate::store::{apply_del, apply_set, commit, SharedStore, SharedWal};

// STAGE 3: everything a client-handling thread needs. Bundled into
// one Clone-able struct instead of four loose parameters, since every
// connection thread needs its own handle to all of it.
#[derive(Clone)]
pub struct ServerContext {
    pub store: SharedStore,
    pub wal: SharedWal,
    // Some(...) only on a leader; used to fan writes out to followers.
    pub replicator: Option<SharedReplicator>,
    // true only on a follower: it may serve GET/PING, but SET/DEL are
    // rejected since the leader is the only source of truth for writes.
    pub read_only: bool,
}

// DAY 2 (parsing) + DAY 3 (actual storage logic) + DAY 5 (error handling)
// + STAGE 2 (WAL write-before-apply for mutations)
// + STAGE 3 (read-only rejection + replication fan-out on the leader).
pub fn handle_command(line: &str, ctx: &ServerContext) -> String {
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
pub fn replicate_apply(store: &SharedStore, wal: &SharedWal, entry: &str) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::Replicator;
    use crate::store::replay_entry;
    use crate::wal::WriteAheadLog;
    use std::collections::HashMap;
    use std::io::{BufRead, BufReader};
    use std::net::TcpStream;
    use std::sync::{Arc, Mutex};
    use std::thread;

    // Every test gets its own WAL file (named after the test) since
    // cargo runs tests in parallel and a shared file would race. The
    // guard removes the file on drop so repeated runs start clean.
    struct TestEnv {
        ctx: ServerContext,
        wal_path: String,
    }

    impl TestEnv {
        fn new(name: &str) -> Self {
            let wal_path = format!("test_commands_{name}.log");
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

    // Rebuilds a store from a WAL file the way a restart would.
    fn replay_file(path: &str) -> HashMap<String, String> {
        let mut map = HashMap::new();
        let wal = WriteAheadLog::new(path).expect("reopen WAL");
        for entry in wal.iter_entries() {
            replay_entry(&entry, &mut map);
        }
        map
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

    // Companion smoke test: under real concurrent writers, the in-memory
    // value must be the one a restart would reconstruct. This alone is
    // too timing-dependent to catch the underlying commit-ordering bug
    // reliably - see `store::commit_holds_wal_lock_while_updating_store`
    // for the deterministic regression guard - but it exercises the
    // genuine multi-threaded client path end to end.
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
