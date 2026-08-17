// ============================================================
// replication.rs - Stage 3: leader-follower replication.
//
// A leader accepts client writes (same as Stage 1/2) and, after each
// one is durably committed locally, forwards it to every registered
// follower. A follower connects out to the leader, registers itself,
// and applies whatever it receives to its own store + WAL - so it
// ends up with its own durable, independently recoverable copy.
//
// Consistency model: eventually consistent. A follower can lag behind
// the leader by however long the network + its own commit takes;
// there's no acknowledgement or backpressure from follower to leader.
// ============================================================

use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Which role this process plays and where to find the ports it needs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationConfig {
    pub node_id: String,
    pub is_leader: bool,
    /// Port clients connect to (SET/GET/DEL/...).
    pub listen_port: u16,
    /// Leader-only: the port followers register on.
    pub repl_port: u16,
    /// Some(addr) if this node is a follower (address of the leader's
    /// `repl_port`); None for a leader or a standalone node.
    pub leader_addr: Option<String>,
}

impl ReplicationConfig {
    pub fn from_env() -> Self {
        let args: Vec<String> = std::env::args().skip(1).collect();
        Self::parse(&args)
    }

    /// Parses `--flag value` / `--flag` style arguments. Split out from
    /// `from_env` so it's testable without touching real process argv.
    pub fn parse(args: &[String]) -> Self {
        let mut node_id = "default".to_string();
        let mut is_leader = false;
        let mut listen_port: u16 = 6380;
        let mut repl_port: u16 = 6381;
        let mut leader_addr: Option<String> = None;

        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--node-id" => {
                    i += 1;
                    node_id = args.get(i).expect("--node-id requires a value").clone();
                }
                "--is-leader" => is_leader = true,
                "--listen-port" => {
                    i += 1;
                    listen_port = args
                        .get(i)
                        .expect("--listen-port requires a value")
                        .parse()
                        .expect("--listen-port must be a number");
                }
                "--repl-port" => {
                    i += 1;
                    repl_port = args
                        .get(i)
                        .expect("--repl-port requires a value")
                        .parse()
                        .expect("--repl-port must be a number");
                }
                "--leader-addr" => {
                    i += 1;
                    leader_addr =
                        Some(args.get(i).expect("--leader-addr requires a value").clone());
                }
                other => eprintln!("warning: unrecognized argument '{other}', ignoring"),
            }
            i += 1;
        }

        ReplicationConfig {
            node_id,
            is_leader,
            listen_port,
            repl_port,
            leader_addr,
        }
    }
}

struct FollowerConnection {
    follower_id: String,
    stream: TcpStream,
}

/// Lives on the leader. Holds one open, writable connection per
/// registered follower and fans out committed commands to all of them.
pub struct Replicator {
    followers: Vec<FollowerConnection>,
}

pub type SharedReplicator = Arc<Mutex<Replicator>>;

impl Replicator {
    pub fn new() -> Self {
        Replicator {
            followers: Vec::new(),
        }
    }

    pub fn add_follower(&mut self, follower_id: String, stream: TcpStream) {
        println!("replication: follower '{follower_id}' registered");
        self.followers.push(FollowerConnection {
            follower_id,
            stream,
        });
    }

    // Only exercised by tests today, but a legitimate part of the public
    // API (e.g. for a future STATUS/health command) - not dead code.
    #[allow(dead_code)]
    pub fn follower_count(&self) -> usize {
        self.followers.len()
    }

    /// Sends `command` to every follower. A follower whose connection
    /// has died is dropped from the list rather than treated as a fatal
    /// error - one slow/dead follower must never block or crash writes
    /// on the leader.
    pub fn replicate_to_all(&mut self, command: &str) {
        let mut line = command.to_string();
        if !line.ends_with('\n') {
            line.push('\n');
        }
        self.followers
            .retain_mut(|f| match f.stream.write_all(line.as_bytes()) {
                Ok(()) => true,
                Err(e) => {
                    eprintln!("replication: follower '{}' dropped: {e}", f.follower_id);
                    false
                }
            });
    }
}

impl Default for Replicator {
    fn default() -> Self {
        Self::new()
    }
}

/// Handles one incoming connection on the leader's replication port:
/// reads the `REGISTER <id>` handshake line and, on success, hands the
/// still-open stream to the `Replicator` to keep writing to forever.
pub fn register_follower(mut stream: TcpStream, replicator: &SharedReplicator) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let line = line.trim();

    let mut parts = line.splitn(2, ' ');
    match parts.next() {
        Some("REGISTER") => {
            let follower_id = parts.next().unwrap_or("unknown").to_string();
            stream.write_all(b"OK\n")?;
            replicator.lock().unwrap().add_follower(follower_id, stream);
            Ok(())
        }
        _ => {
            stream.write_all(b"ERR expected REGISTER <id>\n")?;
            Ok(())
        }
    }
}

/// Lives on a follower. Connects to the leader's replication port,
/// registers, and streams commands to `on_command` until the
/// connection drops - then reconnects with a fixed backoff.
pub struct FollowerListener {
    leader_addr: String,
    node_id: String,
}

impl FollowerListener {
    pub fn new(leader_addr: String, node_id: String) -> Self {
        FollowerListener {
            leader_addr,
            node_id,
        }
    }

    pub fn connect_and_listen(&self, mut on_command: impl FnMut(&str)) {
        loop {
            match self.run_once(&mut on_command) {
                Ok(()) => eprintln!("replication: leader closed the connection, reconnecting..."),
                Err(e) => eprintln!("replication: connection to leader failed ({e}), retrying..."),
            }
            thread::sleep(Duration::from_secs(1));
        }
    }

    /// Connects once, registers, and processes lines until the
    /// connection ends. No retry loop here, so tests can call it
    /// directly against a single fake leader connection.
    pub fn run_once(&self, on_command: &mut impl FnMut(&str)) -> io::Result<()> {
        let mut stream = TcpStream::connect(&self.leader_addr)?;
        stream.write_all(format!("REGISTER {}\n", self.node_id).as_bytes())?;

        let mut reader = BufReader::new(stream.try_clone()?);
        let mut ack = String::new();
        reader.read_line(&mut ack)?;
        println!(
            "replication: connected to leader at {} (handshake: {})",
            self.leader_addr,
            ack.trim()
        );

        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if !line.is_empty() {
                on_command(line);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    fn args(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_config_defaults_to_standalone() {
        let cfg = ReplicationConfig::parse(&args(&[]));
        assert_eq!(cfg.node_id, "default");
        assert!(!cfg.is_leader);
        assert_eq!(cfg.listen_port, 6380);
        assert_eq!(cfg.repl_port, 6381);
        assert_eq!(cfg.leader_addr, None);
    }

    #[test]
    fn test_config_parses_leader_flags() {
        let cfg = ReplicationConfig::parse(&args(&[
            "--node-id",
            "leader1",
            "--is-leader",
            "--listen-port",
            "7000",
            "--repl-port",
            "7001",
        ]));
        assert_eq!(cfg.node_id, "leader1");
        assert!(cfg.is_leader);
        assert_eq!(cfg.listen_port, 7000);
        assert_eq!(cfg.repl_port, 7001);
        assert_eq!(cfg.leader_addr, None);
    }

    #[test]
    fn test_config_parses_follower_flags() {
        let cfg = ReplicationConfig::parse(&args(&[
            "--node-id",
            "follower1",
            "--leader-addr",
            "127.0.0.1:6381",
            "--listen-port",
            "6390",
        ]));
        assert_eq!(cfg.node_id, "follower1");
        assert!(!cfg.is_leader);
        assert_eq!(cfg.listen_port, 6390);
        assert_eq!(cfg.leader_addr, Some("127.0.0.1:6381".to_string()));
    }

    #[test]
    fn test_replicate_to_all_delivers_to_follower() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (accepted, _) = listener.accept().unwrap();

        let mut replicator = Replicator::new();
        replicator.add_follower("f1".to_string(), accepted);
        replicator.replicate_to_all("SET k v");

        let mut reader = BufReader::new(client);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "SET k v");
    }

    #[test]
    fn test_replicate_to_all_drops_dead_follower() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (accepted, _) = listener.accept().unwrap();
        drop(client); // peer is gone; writes to `accepted` should eventually fail

        let mut replicator = Replicator::new();
        replicator.add_follower("f1".to_string(), accepted);

        // A TCP write doesn't always fail on the very first attempt after
        // the peer disappears (the first write can land in the local send
        // buffer before the RST arrives), so retry a bounded number of
        // times until the connection is actually pruned.
        for _ in 0..50 {
            if replicator.follower_count() == 0 {
                break;
            }
            replicator.replicate_to_all("SET k v");
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            replicator.follower_count(),
            0,
            "dead follower was never pruned"
        );
    }

    #[test]
    fn test_follower_listener_registers_and_applies_commands() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let fake_leader = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut writer = stream;

            let mut register_line = String::new();
            reader.read_line(&mut register_line).unwrap();
            assert_eq!(register_line.trim(), "REGISTER follower1");

            writer.write_all(b"OK\n").unwrap();
            writer.write_all(b"SET name alice\n").unwrap();
            writer.write_all(b"DEL name\n").unwrap();
            // Dropping `writer` closes the connection, which ends run_once.
        });

        let received = Arc::new(Mutex::new(Vec::new()));
        let received_ref = Arc::clone(&received);
        let fl = FollowerListener::new(addr.to_string(), "follower1".to_string());
        fl.run_once(&mut |line| received_ref.lock().unwrap().push(line.to_string()))
            .expect("run_once should end cleanly when the leader closes the connection");

        fake_leader.join().unwrap();
        assert_eq!(
            *received.lock().unwrap(),
            vec!["SET name alice", "DEL name"]
        );
    }
}
