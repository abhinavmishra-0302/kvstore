# kvstore

A small, durable key-value server written in Rust from scratch — no
external crates, just the standard library. It speaks a simple
line-based protocol over TCP, keeps data in memory, and writes every
mutation to a write-ahead log so a restart doesn't lose anything.

This is a learning project built in stages. The source is written to be
read top to bottom: `src/main.rs` carries `DAY N` comments marking what
each day of the build added, and `STAGE N` comments marking the larger
milestones.

| Stage | What it added | Status |
|-------|---------------|--------|
| 1 | In-memory TCP server, concurrent clients | Done |
| 2 | Write-ahead log, crash recovery on startup | Done |
| 3 | Leader-follower replication | Done |

## Quick start

```sh
cargo run --release
```

```
kvstore listening on 127.0.0.1:6380
```

Then connect from another terminal and type commands:

```sh
nc localhost 6380
```

```
SET name abhinav
OK
GET name
abhinav
GET missing
(nil)
DEL name
1
DEL name
0
```

If you don't have `nc` installed (`openbsd-netcat` or `nmap-ncat` on
most distros), any TCP client will do:

```sh
python3 -c "
import socket
f = socket.create_connection(('127.0.0.1', 6380)).makefile('rwb')
for cmd in [b'SET name abhinav', b'GET name', b'PING']:
    f.write(cmd + b'\n'); f.flush()
    print(f.readline().decode().strip())
"
```

Open a second client at the same time and set keys from one, read them
from the other — the store is shared across all connections.

## Protocol

One command per line, terminated by `\n`. Every command gets exactly
one line back, also terminated by `\n`.

| Command | Reply | On failure |
|---------|-------|------------|
| `SET <key> <value>` | `OK` | `ERR wrong number of arguments for 'SET'`<br>`ERR failed to persist write: <reason>` |
| `GET <key>` | the value, or `(nil)` if absent | `ERR wrong number of arguments for 'GET'` |
| `DEL <key>` | `1` if a key was removed, `0` if it wasn't there | `ERR wrong number of arguments for 'DEL'` |
| `PING` | `PONG` | — |
| `SYNC` | `OK` once the log is on disk | `ERR sync failed: <reason>` |

Anything else returns `ERR unknown command '<NAME>'`.

Parsing details worth knowing:

- **Command names are case-insensitive.** `set`, `SET`, and `sEt` are
  the same command.
- **Keys and values are case-sensitive** and stored exactly as sent.
- **Values may contain spaces.** The line is split into at most three
  parts, so `SET greeting hello there world` stores `hello there world`,
  interior spacing included.
- **Keys may not contain spaces**, since the key is the second field.
- **Use exactly one space between fields.** `SET  key value` (two
  spaces) parses the key as the empty string — see Known limitations.

## How durability works

Every `SET` and `DEL` is appended to a write-ahead log (`data.log` in
the working directory) *before* it touches the in-memory map. If the
append fails, the client gets an error and the in-memory state is left
untouched — the store is never ahead of what's been logged.

The log is plain text, one entry per line, in the same shape as the
wire protocol, so you can read it with `cat` and understand it:

```
SET greeting hello there world
SET lower case
DEL greeting
```

Deletes are logged even when the key didn't exist, so replaying the log
is a faithful redo of history rather than a summary of it.

**Batched fsync.** Appending to the log hands the bytes straight to the
kernel, but forcing them onto the physical disk (`fsync`) is the
expensive part, so that happens once every 100 writes rather than on
every command.

This is worth being precise about, because the two failure modes differ:

- **The process dies** (crash, `kill`, Ctrl+C): nothing is lost. The
  bytes are already with the kernel, which writes them out regardless
  of what happened to the process.
- **The machine loses power** (or the kernel panics): up to the last
  100 writes can be lost, because they hadn't been forced to disk yet.

A client that needs a specific write on disk *now* can issue `SYNC` to
force one immediately, rather than waiting for the batch to fill.

**Recovery.** On startup the log is replayed into the store before the
server accepts any connections, so the first client to connect already
sees the fully recovered state:

```
kvstore listening on 127.0.0.1:6380
recovered 3 entries from data.log
```

A malformed line is reported on stderr and skipped rather than aborting
recovery — one bad line shouldn't cost you the rest of your history.

To see it work: `SET` a key, stop the server with Ctrl+C, start it
again, and `GET` the key without setting it first.

## How it's put together

```
src/main.rs          server loop, connection handling, command parsing, replay
src/wal.rs           the write-ahead log: append, fsync, iterate for replay
src/replication.rs   CLI config, leader-side Replicator, follower-side FollowerListener
```

The server spawns one OS thread per connection. All threads share a
single `Arc<Mutex<HashMap<String, String>>>` for the data and an
`Arc<Mutex<WriteAheadLog>>` for the log.

Mutations go through `commit()`, which holds the WAL lock *across* the
in-memory update so that appending and applying are a single atomic
step. This matters: an earlier version released the WAL lock first,
which let two threads append in one order and apply in the opposite
one, leaving memory disagreeing with the log — invisible until a
restart, when the server would silently start serving a different
value. `commit()` is the only path that takes both locks and it always
takes them in the same order (WAL, then store), so it can't deadlock
against `GET` or `SYNC`, which each take only one.

## Replication

A leader forwards every write it commits to its followers. A follower
connects out to the leader, registers itself, and applies whatever it
receives to its own store *and its own WAL* — so a follower is
independently durable, not just a cache of the leader.

```sh
# terminal 1: leader
cargo run -- --node-id leader1 --is-leader --listen-port 6380 --repl-port 6381

# terminal 2: follower
cargo run -- --node-id follower1 --leader-addr 127.0.0.1:6381 --listen-port 6390
```

```
SET on leader (port 6380):   SET name abhinav  ->  OK
GET on follower (port 6390): GET name          ->  abhinav   (shortly after)
SET on follower (port 6390): SET x y           ->  ERR write commands are not allowed on a read-only follower
```

How it fits together:

- **Two ports on the leader.** `--listen-port` (default `6380`) is for
  clients; `--repl-port` (default `6381`) is a separate port that only
  followers connect to, so a follower can never accidentally show up as
  a regular client and vice versa.
- **Handshake.** A follower opens a connection to the leader's
  `repl-port` and sends `REGISTER <node-id>`; the leader replies `OK`
  and keeps that connection open as a one-way command feed.
- **Fan-out, not consensus.** After a write commits locally (WAL, then
  store — same as Stage 2), the leader writes the same line to every
  registered follower's socket. There's no acknowledgement and no
  quorum: a follower that's behind or disconnected doesn't block or
  fail the client's write. This is deliberately eventually consistent.
- **A dead follower is dropped, not retried.** If a write to a
  follower's socket fails, that follower is removed from the leader's
  list. It has to reconnect (and re-register) to start receiving
  writes again.
- **A follower reconnects on its own.** If the leader connection drops,
  the follower retries once a second until it succeeds, re-registering
  each time.
- **Read-only, not read-blocked.** A follower still answers `GET` and
  `PING` from clients on its own `--listen-port`; it only rejects `SET`
  and `DEL`, since the leader is the only source of truth for writes.
- **Separate WAL per node.** `--node-id` scopes the log file to
  `data-<node-id>.log`, so a leader and follower running on the same
  machine (the normal way to test this locally) don't collide on the
  same file. Omit every flag and you get the Stage 1/2 behavior back
  exactly: a single standalone node writing to `data.log`.
- **Restart a follower and it doesn't need the leader.** It replays its
  own WAL first, then reconnects and picks up from wherever the leader
  is — try `SET` on the leader, stop the follower, restart it, and
  `GET` the key before the leader sends anything new.

## Configuration

Command-line flags, all optional — omit them all and you get a single
standalone node on `127.0.0.1:6380`, same as Stage 1/2:

| Flag | Default | Meaning |
|------|---------|---------|
| `--node-id <id>` | `default` | identifies this node in logs and scopes its WAL filename |
| `--is-leader` | off | accept follower registrations and replicate writes |
| `--listen-port <port>` | `6380` | port clients connect to |
| `--repl-port <port>` | `6381` | leader-only: port followers register on |
| `--leader-addr <host:port>` | none | run as a follower of the leader's `--repl-port` |

A few values are still compile-time constants in `src/main.rs`:

| Value | Default | Meaning |
|-------|---------|---------|
| `WAL_PATH` | `data.log` | log filename used when `--node-id` is left at `default` |
| `WAL_SYNC_THRESHOLD` | `100` | writes to buffer before forcing an fsync |

## Testing

```sh
cargo test
```

24 tests covering command handling, WAL append and replay, concurrency,
and replication. A few are worth calling out:

- `commit_holds_wal_lock_while_updating_store` asserts the atomicity
  invariant directly, by probing from another thread that the WAL lock
  is still held while the store is being updated. It fails 100% of the
  time if that ordering regresses.
- `concurrent_writes_keep_store_and_wal_in_agreement` runs concurrent
  writers and checks the in-memory value matches what a restart would
  reconstruct. It's a smoke test, not the real guard — reproducing the
  race by racing for it turned out to be far too timing-dependent to
  rely on.
- `test_replicate_to_all_drops_dead_follower` closes one end of a real
  loopback socket and asserts the `Replicator` prunes it. TCP doesn't
  always surface a broken connection on the first write after the peer
  disappears, so it retries a bounded number of times rather than
  asserting on the first attempt.
- `test_leader_replicates_committed_write_to_follower` and
  `test_follower_listener_registers_and_applies_commands` exercise the
  replication wire protocol end to end over real loopback sockets
  (registration handshake, then command fan-out), not just the parsing
  logic in isolation.

Tests write their own `test_*.log` files and clean up after themselves.

## Performance

Measured on loopback with a single connection, release build, one
command at a time:

| Operation | Throughput |
|-----------|------------|
| `SET` (WAL append + in-memory insert) | ~81,000 ops/sec |
| `GET` | ~88,000 ops/sec |

Replaying a 500,000-entry log at startup takes about 72ms. The
benchmark client was the bottleneck at these rates, so the server's
real ceiling is higher.

One detail worth recording, because it cost three orders of magnitude:
replies must be written to the socket in a **single** write. Sending
the payload and its trailing newline as two writes makes the newline a
tiny follow-up segment that Nagle's algorithm holds back until the peer
ACKs the first one — and the peer's delayed-ACK timer sits on that for
~40ms. That alone capped this server at roughly 23 ops/sec.

## Known limitations

These are real and known, not hidden:

- **Repeated spaces break parsing.** `SET  key value` stores the empty
  string as the key. Fields are split on single spaces, not runs of
  whitespace.
- **Keys can't contain spaces**, and no value can contain a newline —
  the protocol is line-based with no quoting or escaping.
- **The log never compacts.** It grows forever, so startup time scales
  with every write ever made, not with the size of the dataset.
- **Log entries aren't checksummed.** A line left half-written by a
  power failure is skipped during replay if it no longer parses — but a
  truncated line that still *looks* valid (`SET k valu` instead of
  `SET k value`) would be replayed as-is, silently. A per-entry
  checksum would catch that.
- **No authentication or TLS.** It binds to `127.0.0.1` and should stay
  there.
- **One OS thread per connection**, spawned without limit. Fine for a
  handful of clients, not for thousands of mostly-idle ones.
- **Replication has no consensus or failover.** If the leader dies, a
  follower does not promote itself — it just keeps retrying a dead
  address. There is no leader election, and a follower's data can lag
  the leader's by an unbounded amount if it's been disconnected.
- **A follower's WAL can diverge from a from-scratch replay of the
  leader's**, since it only records what it actually received, in the
  order it received it — a follower that missed a window of writes
  while disconnected has gaps, not corruption, but its log is not a
  byte-for-byte copy of the leader's.
- **No authentication between leader and follower.** Anything that can
  reach `--repl-port` and speaks the `REGISTER` handshake gets a
  replication feed.

## Roadmap

Stage 4 (stretch) replaces the `HashMap` + full-file WAL with an
LSM-tree-style storage engine. Known efficiency work queued up before
that:

- Buffer WAL writes through a `BufWriter` (~10x on appends, at the cost
  of a slightly wider crash window).
- Use an `RwLock` for the store so reads don't serialize against
  each other.
- Compare command names without allocating (`eq_ignore_ascii_case`
  instead of `to_uppercase`).
- Log compaction or snapshotting, to stop startup cost growing forever.
- Leader election / failover, so a follower can take over automatically.
