# fedis Redis Compatibility Roadmap

## Goal
Ship a fast, lightweight Redis-compatible server in focused milestones while tracking command and protocol parity.

## Current Status

### Implemented
- Networking: RESP2 over TCP (`redis://` config support)
- Auth: `AUTH` with multi-user env config, per-user command ACL, enabled/disabled users
- Core KV: `GET`, `GETSET`, `GETDEL`, `GETEX`, `GETRANGE`, `SET`, `SETNX`, `SETEX`, `PSETEX`, `SETRANGE`, `UPDATE`, `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `DBSIZE`, `MGET`, `MSET`, `MSETNX`, `STRLEN`, `APPEND`
- Numeric: `INCR`, `DECR`, `INCRBY`, `DECRBY`
- Expiry: `EX`, `PX`, `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `PERSIST`, `TTL`, `PTTL`
- Introspection: `PING`, `INFO` (including `persistence` and `commandstats`), `SELECT` (DB 0), `QUIT`
- Utility: `ECHO`, `TIME`
- Client compatibility shims: `HELLO`, `CLIENT SETINFO|SETNAME|GETNAME|ID|TRACKING`, `MEMORY USAGE|STATS`, `OBJECT ENCODING|IDLETIME|FREQ|REFCOUNT`
- Meta compatibility shims: `COMMAND`, `CONFIG GET|RESETSTAT`, `LATENCY`, `SLOWLOG`
- Key discovery: `KEYS`, `SCAN`
- Persistence: append-only log replay at startup, fsync policy (`always|everysec|no`), basic AOF rewrite (`BGREWRITEAOF`)

### Compatibility Notes
- `SCAN` is stateless and cursor is an index into a sorted snapshot of matching keys
- Pattern matching currently supports `*` and `?`
- Only database `0` is supported

## Milestones

## M1 - Command Parity (Near Term)
- Improve exact Redis-like error texts and argument validation

## M2 - Protocol and Client Compatibility
- Add selective RESP3 support where required by popular clients
- Expand `CLIENT` subcommand coverage (`LIST`, `INFO`, `PAUSE`, `UNPAUSE` as needed)
- Add optional command table exposure (`COMMAND`) for discovery

## M3 - Storage and Durability
- Optional RDB-like snapshot support
- Crash recovery validation suite

## M4 - Performance
- Sharded store to reduce write contention
- Parser and response encoding allocation reductions
- Batched AOF writes and background flush tuning
- Benchmarks and regression thresholds

## M5 - Operations
- Structured logs and metrics export endpoint
- Config file support in addition to env vars
- Graceful shutdown and signal handling

## Tracking
Use this legend when adding new commands:
- `[x]` implemented
- `[~]` partial compatibility
- `[ ]` not implemented
