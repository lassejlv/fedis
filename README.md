# fedis

Fast Redis-compatible server in Rust.

## Quick start

```bash
cargo run
```

Default address: `127.0.0.1:6379`

Set a password:

```bash
FEDIS_PASSWORD=secret cargo run
```

Connect:

```bash
redis-cli -a secret
```

## Docker

```bash
docker build -t fedis .
docker run --rm -p 6379:6379 fedis
```

## Useful env vars

- `FEDIS_HOST` / `FEDIS_PORT` / `FEDIS_LISTEN`
- `FEDIS_PASSWORD`, `FEDIS_USERNAME`, `FEDIS_USERS`
- `FEDIS_DATA_PATH`, `FEDIS_AOF_PATH`, `FEDIS_AOF_FSYNC=always|everysec|no`
- `FEDIS_SNAPSHOT_PATH`, `FEDIS_SNAPSHOT_INTERVAL_SEC`
- `FEDIS_METRICS_ADDR` (Prometheus-style text endpoint)
- `FEDIS_CONFIG` (`KEY=VALUE` file)
- `FEDIS_LOG=info|debug|warn|error`

## Commands (high level)

- Strings: `GET`, `SET`, `MGET`, `MSET`, `INCR`, `DECR`, `APPEND`, `GETRANGE`, `SETRANGE`
- JSON v1: `JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.TYPE` (root path only)
- Keyspace/expiry: `DEL`, `UNLINK`, `EXISTS`, `KEYS`, `SCAN`, `EXPIRE`, `TTL`, `PERSIST`
- Server: `INFO`, `PING`, `ECHO`, `BGREWRITEAOF`, `BGSAVE`, `SAVE`, `LASTSAVE`

## Notes

- DB `0` only
- RESP2 primary, RESP3 map response for `HELLO 3`
- Persistence: AOF + optional snapshots

## Benchmarks

```bash
python3 benchmarks/run_bench.py
python3 benchmarks/run_bench_concurrent.py
python3 benchmarks/run_suite.py
python3 benchmarks/check_regression.py
```

See `ROADMAP.md` for compatibility tracking.
