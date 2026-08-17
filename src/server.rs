// ============================================================
// server.rs - process bootstrap and the TCP connection loop.
//
// `run` wires together the store, the WAL, and (depending on
// ReplicationConfig) either a leader's follower-registration server
// or a follower's connection to its leader, then accepts client
// connections forever, one OS thread per connection.
// ============================================================

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::commands::{handle_command, replicate_apply, ServerContext};
use crate::replication::{
    register_follower, FollowerListener, ReplicationConfig, Replicator, SharedReplicator,
};
use crate::store::{replay_into, wal_path_for, SharedStore, SharedWal};
use crate::wal::WriteAheadLog;

pub fn run(config: ReplicationConfig) -> std::io::Result<()> {
    if config.is_leader && config.leader_addr.is_some() {
        eprintln!(
            "warning: --is-leader and --leader-addr both given; running as leader, ignoring --leader-addr"
        );
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
    let replayed = replay_into(&wal, &store);
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
