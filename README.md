# kvstore

A tiny in-memory key-value server, built in Rust. Week 1 of a larger
project (persistence + replication coming in later weeks).

## What it does

Starts a TCP server on `127.0.0.1:6380`. Clients connect and send
line-based commands:

```
SET key value
GET key
DEL key
```

Data lives in a `HashMap` shared across all connections via
`Arc<Mutex<HashMap<String, String>>>`, so multiple clients can read
and write concurrently, safely.

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

## What this taught me (fill in as you go)

- Why shared mutable state across threads needs `Arc<Mutex<...>>`
  in Rust, and what the compiler stops you from doing without it.
- The difference between `String` and `&str` when parsing input.
- Handling a disconnecting client without panicking the whole server.

## Next up (Week 2)

- Write-ahead log (WAL) so data survives a restart.
