# fedis roadmap

## now

- keep Redis compatibility improving
- keep performance fast under concurrency
- keep persistence reliable

## done

- RESP2 server with selective RESP3 (`HELLO 3` map)
- auth + users + command allowlists
- core string, numeric, keyspace, expiry commands
- JSON v1 root-path commands (`JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.TYPE`)
- AOF persistence with fsync modes and `BGREWRITEAOF`
- snapshot persistence with `SAVE` / `BGSAVE` / `LASTSAVE`
- production hardening controls (max connections, request size, idle timeout, maxmemory guard)
- metrics endpoint + structured logging + graceful shutdown
- extra compatibility commands (`ACL WHOAMI|LIST`, `MODULE LIST`, extended `CLIENT` subcommands)
- benchmark scripts (single and concurrent)

## next

- tighten Redis error text parity in edge cases
- keep tuning throughput and lock contention
- add more restart/recovery stress tests

## nice-to-have later

- fuller RESP3 coverage
- multi-db support
- replication primitives
