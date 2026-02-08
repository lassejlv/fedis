# fedis

A fast, lightweight Redis-compatible server written in Rust + Tokio.

## Status

`fedis` supports a growing subset of Redis commands, RESP2 protocol, user/password auth, ACL-style command allowlists, AOF persistence, background AOF rewrite, and compatibility shims for common GUI/client startup commands.

See `ROADMAP.md` for compatibility tracking.

## Run

```bash
cargo run
```

Defaults to `127.0.0.1:6379`.

## Common Environment Variables

- `FEDIS_HOST` / `FEDIS_PORT` / `FEDIS_LISTEN`
- `FEDIS_USERNAME` / `FEDIS_PASSWORD`
- `FEDIS_USERS` (multi-user auth + permissions)
- `FEDIS_USER_COMMANDS` (allowlist for default user)
- `FEDIS_DATA_PATH` / `FEDIS_AOF_PATH`
- `FEDIS_AOF_FSYNC=always|everysec|no`
- `FEDIS_URL=redis://user:pass@127.0.0.1:6379/0`
- `FEDIS_LOG=info|debug|warn|error`
- `FEDIS_NON_REDIS_MODE=1` + `FEDIS_DEBUG_RESPONSE_ID=1` (debug response wrapper)

## Example

```bash
FEDIS_PASSWORD=secret FEDIS_AOF_FSYNC=everysec cargo run
```

Then connect with:

```bash
redis-cli -a secret
```

## Implemented Command Groups

- Core strings: `GET`, `SET`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX`, `GETRANGE`, `SETRANGE`, `STRLEN`, `APPEND`, `MGET`, `MSET`, `MSETNX`
- Numeric: `INCR`, `DECR`, `INCRBY`, `DECRBY`
- Keyspace: `DEL`, `UNLINK`, `EXISTS`, `TYPE`, `DBSIZE`, `KEYS`, `SCAN`
- Expiry: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`
- Server/info: `PING`, `ECHO`, `TIME`, `INFO`, `SELECT`, `QUIT`, `BGREWRITEAOF`
- Compatibility shims: `HELLO`, `CLIENT`, `COMMAND`, `CONFIG`, `LATENCY`, `SLOWLOG`, `MEMORY`, `OBJECT`

## Notes

- RESP2 is the primary protocol target.
- DB index `0` only.
- AOF rewrite is implemented via `BGREWRITEAOF`.
- `INFO persistence` and `INFO commandstats` are supported, including per-command `calls`, `usec`, and `usec_per_call`.
