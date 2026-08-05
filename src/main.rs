// ============================================================
// kvstore - a tiny in-memory key-value server (Week 1 build)
//
// This file is written to show the week's progression. Read the
// "DAY N" comments top to bottom - each one is the feature that
// day added. By the end (Day 5) this is a complete, working,
// concurrent single-node KV store.
// ============================================================

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

// DAY 3: Shared state across all client connections.
// - HashMap<String, String> is our actual data store.
// - Mutex protects it so only one thread touches it at a time.
// - Arc lets many threads hold a reference to the *same* mutex
//   (Arc = "atomically reference counted" - a thread-safe
//   shared pointer). Without Arc, Rust's ownership rules won't
//   let multiple threads own the same HashMap.
type SharedStore = Arc<Mutex<HashMap<String, String>>>;

fn main() -> std::io::Result<()> {
    // DAY 1: bind a TCP listener - this is the socket clients connect to.
    let listener = TcpListener::bind("127.0.0.1:6380")?;
    println!("kvstore listening on 127.0.0.1:6380");

    // DAY 3: create the shared store once, before accepting any clients.
    let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));

    // DAY 1 (loop) + DAY 4 (thread-per-connection):
    // incoming() gives us each new connection as it arrives.
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // Clone the Arc (cheap - just bumps a reference count),
                // so this thread gets its own handle to the SAME store.
                let store_ref = Arc::clone(&store);

                // DAY 4: spawn a new OS thread per client so multiple
                // clients can be connected and issuing commands at once.
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream, store_ref) {
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
fn handle_client(stream: TcpStream, store: SharedStore) -> std::io::Result<()> {
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
        let response = handle_command(&line, &store);

        // Write the reply back, followed by a newline so simple clients
        // (like `nc`) render it cleanly.
        writer.write_all(response.as_bytes())?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    }

    println!("client disconnected: {peer}");
    Ok(())
}

// DAY 2 (parsing) + DAY 3 (actual storage logic) + DAY 5 (error handling).
fn handle_command(line: &str, store: &SharedStore) -> String {
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
                    // DAY 3: lock the mutex, insert, then the lock is
                    // automatically released when `map` goes out of scope.
                    let mut map = store.lock().unwrap();
                    map.insert(k.to_string(), v.to_string());
                    "OK".to_string()
                }
                // DAY 5: malformed command - don't crash, just tell the client.
                _ => "ERR wrong number of arguments for 'SET'".to_string(),
            }
        }
        "GET" => {
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
                    let mut map = store.lock().unwrap();
                    match map.remove(k) {
                        Some(_) => "1".to_string(), // 1 key was deleted
                        None => "0".to_string(),    // nothing to delete
                    }
                }
                None => "ERR wrong number of arguments for 'DEL'".to_string(),
            }
        }
        "PING" => "PONG".to_string(),
        "" => "ERR empty command".to_string(),
        other => format!("ERR unknown command '{other}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));
        let response = handle_command("SET name alice", &store);
        assert_eq!(response, "OK");

        let response = handle_command("GET name", &store);
        assert_eq!(response, "alice");
    }

    #[test]
    fn test_get_missing() {
        let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));
        let response = handle_command("GET missing", &store);
        assert_eq!(response, "(nil)");
    }

    #[test]
    fn test_del() {
        let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));
        handle_command("SET key value", &store);
        let response = handle_command("DEL key", &store);
        assert_eq!(response, "1");

        let response = handle_command("DEL key", &store);
        assert_eq!(response, "0");
    }

    #[test]
    fn test_malformed_command() {
        let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));
        let response = handle_command("SET key", &store);
        assert!(response.contains("ERR"));
    }

    #[test]
    fn test_ping() {
        let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));
        let response = handle_command("PING", &store);
        assert_eq!(response, "PONG");
    }

    #[test]
    fn test_unknown_command() {
        let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));
        let response = handle_command("FOO bar", &store);
        assert!(response.contains("ERR"));
    }

    #[test]
    fn test_set_overwrites_existing_key() {
        let store: SharedStore = Arc::new(Mutex::new(HashMap::new()));
        handle_command("SET key first", &store);
        handle_command("SET key second", &store);
        let response = handle_command("GET key", &store);
        assert_eq!(response, "second");
    }
}
