# kvstore

A small key-value server, built in Rust from scratch. Week 1 was an
in-memory TCP server; Week 2 added durability via a write-ahead log.
Replication is coming in a later stage.

## What it does

Starts a TCP server on `127.0.0.1:6380`. Clients connect and send
line-based commands:

```
SET key value
GET key
DEL key
PING
SYNC
```

Data lives in a `HashMap` shared across all connections via
`Arc<Mutex<HashMap<String, String>>>`, so multiple clients can read
and write concurrently, safely.

Every `SET`/`DEL` is first appended to a write-ahead log (`data.log`)
before it touches the in-memory store - if the process crashes or is
restarted, the log is replayed on startup to rebuild the exact same
state. Fsyncs are batched (every 100 writes) rather than done on
every single command, since fsync is the expensive part; `SYNC` lets
a client force one immediately.

## Running it

```
cargo run
```

## Testing it

In another terminal:

```
nc localhost 6380
```

Then type:

```
SET name abhinav
GET name
GET missing
DEL name
DEL name
```

Expected output:

```
OK
abhinav
(nil)
1
0
```

Open a second `nc` terminal at the same time and set/get keys from
both to see the shared state and concurrency in action.

To see persistence in action: `SET` a key, stop the server
(Ctrl+C), then `cargo run` again - the startup log prints how many
WAL entries it recovered, and a `GET` for that key returns the value
without you having to `SET` it again.

## What this taught me (fill in as you go)

- Why shared mutable state across threads needs `Arc<Mutex<...>>`
  in Rust, and what the compiler stops you from doing without it.
- The difference between `String` and `&str` when parsing input.
- Handling a disconnecting client without panicking the whole server.
- Why the WAL write has to succeed *before* the in-memory store is
  touched (transactional ordering) - and that ordering the two writes
  is not enough on its own. Releasing the WAL lock before taking the
  store lock let two threads append in one order and apply in the
  other, so memory and the log disagreed and the server silently
  changed its answer after a restart. Both locks have to be held
  across the whole mutation; taking them in a consistent order
  (WAL, then store) is what keeps that deadlock-free.
- That a request/response server must never split one reply into two
  socket writes: Nagle's algorithm holds the second small write until
  the peer ACKs the first, and the peer's delayed-ACK timer sits on it
  for ~40ms. That single detail capped this server at ~25 ops/sec.
- Why fsync is batched instead of called on every write: it's the
  expensive part of durability, so committing every N writes trades
  a little recovery risk (writes since the last sync are still in
  the log, just not yet forced to disk) for a lot of throughput.

## Next up (Stage 3)

- Leader-follower replication so a second node has the data too.
