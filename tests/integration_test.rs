// ============================================================
// integration_test.rs - black-box tests: spawn a real node (via the
// library's public `run`/`ReplicationConfig` API) and talk to it over
// a real TCP socket, exactly like a client or another node would.
// Unlike the unit tests under src/*.rs, nothing here reaches into
// internals - only the wire protocol is used, matching the spec's
// "Level 2: Integration Tests" (spawn a real server, connect a real
// client) as opposed to the in-process unit tests next to each module.
// ============================================================

use kvstore::{run, ReplicationConfig};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

// `run` never returns under normal operation (it loops accepting
// connections forever), so each node gets its own leaked background
// thread - fine for a test binary, which exits at the end of the run.
fn spawn_node(config: ReplicationConfig) {
    thread::spawn(move || {
        run(config).expect("server exited with an error");
    });
    // Give the listener(s) time to bind before the test starts connecting.
    thread::sleep(Duration::from_millis(200));
}

fn send(port: u16, cmds: &[&str]) -> Vec<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect to node");
    for cmd in cmds {
        writeln!(stream, "{cmd}").unwrap();
    }
    let mut reader = BufReader::new(stream);
    cmds.iter()
        .map(|_| {
            let mut line = String::new();
            reader.read_line(&mut line).expect("read response");
            line.trim().to_string()
        })
        .collect()
}

fn standalone_config(node_id: &str, listen_port: u16) -> ReplicationConfig {
    ReplicationConfig {
        node_id: node_id.to_string(),
        is_leader: false,
        listen_port,
        repl_port: listen_port + 1, // unused (not a leader), just kept distinct
        leader_addr: None,
    }
}

#[test]
fn full_client_protocol_over_real_tcp() {
    spawn_node(standalone_config("it_full_protocol", 17380));

    let responses = send(
        17380,
        &[
            "SET name abhinav",
            "GET name",
            "PING",
            "DEL name",
            "DEL name",
            "GET missing",
        ],
    );
    assert_eq!(responses, vec!["OK", "abhinav", "PONG", "1", "0", "(nil)"]);

    std::fs::remove_file("data-it_full_protocol.log").ok();
}

#[test]
fn restart_recovers_data_from_its_own_wal() {
    let node_id = "it_restart_recovery";
    std::fs::remove_file(format!("data-{node_id}.log")).ok();

    spawn_node(standalone_config(node_id, 17382));
    assert_eq!(send(17382, &["SET name abhinav"]), vec!["OK"]);

    // Simulate a restart: a second `run()` call reopens the same WAL
    // path and replays it, exactly like relaunching the process would.
    // (The first node's listener thread is simply abandoned - harmless
    // in a test process, since nothing else will use port 17382 again.)
    spawn_node(standalone_config(node_id, 17383));
    assert_eq!(send(17383, &["GET name"]), vec!["abhinav"]);

    std::fs::remove_file(format!("data-{node_id}.log")).ok();
}

#[test]
fn leader_replicates_to_follower_over_real_tcp() {
    spawn_node(ReplicationConfig {
        node_id: "it_leader".to_string(),
        is_leader: true,
        listen_port: 17390,
        repl_port: 17391,
        leader_addr: None,
    });
    spawn_node(ReplicationConfig {
        node_id: "it_follower".to_string(),
        is_leader: false,
        listen_port: 17392,
        repl_port: 17393, // unused on a follower
        leader_addr: Some("127.0.0.1:17391".to_string()),
    });

    assert_eq!(send(17390, &["SET name abhinav"]), vec!["OK"]);

    // Replication is asynchronous - give it a moment to arrive.
    thread::sleep(Duration::from_millis(300));
    assert_eq!(send(17392, &["GET name"]), vec!["abhinav"]);

    // A follower must not accept direct writes.
    let rejected = send(17392, &["SET x y"]);
    assert!(
        rejected[0].contains("ERR"),
        "follower accepted a direct write: {rejected:?}"
    );

    std::fs::remove_file("data-it_leader.log").ok();
    std::fs::remove_file("data-it_follower.log").ok();
}
