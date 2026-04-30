# Resonate Server — YugabyteDB Fork


A research fork of [resonatehq/resonate](https://github.com/resonatehq/resonate) that adds a **distributed YugabyteDB storage backend**, a benchmarking harness, fault-injection tests, and a Prometheus + Grafana monitoring stack.

This is the codebase for a master's thesis at the intersection of durable execution and distributed SQL: the goal is to show that a multi-instance Resonate deployment can run on top of a clustered storage backend and survive node failures without sacrificing correctness.

> Looking for the upstream project, SDKs, or production docs? See [resonatehq/resonate](https://github.com/resonatehq/resonate) and [docs.resonatehq.io](https://docs.resonatehq.io).

---

## What this fork adds

- **YugabyteDB store backend** at `internal/app/subsystems/aio/store/yugabyte/`, implementing the same store interface as the existing SQLite and Postgres backends. Built on `github.com/yugabyte/pgx/v5` over `pgxpool`, with cluster-aware DSN, multi-host fallback, batched transactions (`pgx.Batch` + `tx.SendBatch`), and exponential-backoff retry on retryable sqlstates.
- **Per-backend migrations** under `internal/migrationfiles/migrations/yugabyte/` — initial schema with `SPLIT INTO N TABLETS`, sequence cache tuning, and parked index experiments.
- **Fault-tolerance test suite** — six scenarios (secondary kill, primary kill, SIGSTOP partition, data integrity, concurrent claim race, recovery latency) driven against a real 3-node cluster via Docker.
- **Benchmark framework** at `internal/app/subsystems/aio/store/bench/` plus side-by-side Postgres / single-node YB / 3-node YB compose files.
- **`dbprobe` CLI** (`cmd/dbprobe/`) — a standalone load generator that classifies pgx errors (sqlstate, retryable vs non-retryable, network vs deadline) and is used to characterise the DB independently of Resonate during fault-injection runs.
- **Monitoring stack** — Prometheus scrape config, alert rules, and Grafana dashboards for the Resonate server, the YugaByte cluster, the YB store path, and the fault-tolerance tests.
- **New Prometheus metrics** for the YB backend: `yugabyte_tx_total`, `yugabyte_tx_retries_total`, `yugabyte_tx_duration_seconds`, `yugabyte_batch_duration_seconds`, `yugabyte_command_total`, `yugabyte_pool_connections`, `yugabyte_errors_total`.

---

## Architecture (where the new backend fits)

The Resonate request flow is unchanged:

```
HTTP → API subsystem → kernel coroutine → AIO submission → store/router/sender → completion → HTTP response
```

The YugaByte backend lives at the **store** layer alongside `sqlite/` and `postgres/`. Internally:

- A `pgxpool.Pool` is configured via `NewConn` with cluster-aware DSN, multi-host fallback, optional smart-driver load balancing, and `PrepareConn` ping validation.
- A `YugabyteStoreWorker` runs per background slot. Inside a transaction it queues every statement into a `pgx.Batch` and calls `tx.SendBatch` once at commit — collapsing N round-trips per transaction into one.
- `Execute` retries the whole transaction on retryable errors (`40001`, `40P01`, `XX000`, `08006`, `57P01`, EOF/ECONNRESET) with exponential backoff (10ms, 20ms, 40ms, …).
- A goroutine periodically deletes completed promises/tasks older than `CleanupRetention` (configurable; `0` disables).

---

## Performance & resilience improvements

Each item below is a concrete change made to make the YugaByte backend viable under cluster load and node failures.

### 1. pgx batch processing — fewer round-trips per transaction

All SQL commands inside a transaction are queued into a `pgx.Batch` and sent in a single network round-trip. YugabyteDB nodes can be spread across availability zones, so each round-trip adds latency. Batching collapses N round-trips per transaction into 1, regardless of how many statements the transaction contains.

```
Before:  client → SELECT      → DB     (round-trip 1)
         client → INSERT      → DB     (round-trip 2)
         client → UPDATE      → DB     (round-trip 3)

After:   client → SELECT+INSERT+UPDATE → DB   (1 round-trip)
```

### 2. Cluster-aware connection pool

The pool knows the full cluster topology, not just a single contact node. New config flags:

| Option | Description |
|---|---|
| `fallback-hosts` | Additional cluster nodes (comma-separated) |
| `load-balance` | Enables YugaByte smart-driver load balancing |
| `topology-keys` | Topology-aware routing (e.g. prefer local AZ) |
| `refresh-interval` | How often the driver refreshes the cluster node list |
| `max-open-conns` / `max-idle-conns` | Pool sizing (default to worker count) |
| `conn-max-lifetime` / `conn-max-idle-time` | Recycle thresholds |
| `health-check-period` | Idle-connection ping cadence |

Without `fallback-hosts`, a dead primary contact node takes the whole pool down with it.

### 3. Fast dead-node detection (TCP keepalives + connect timeout)

A custom `DialFunc` sets OS-level TCP keepalives on every connection:

```go
d := &net.Dialer{KeepAlive: 3 * time.Second, Timeout: 1 * time.Second}
```

Combined with `connect_timeout=1` in the DSN, dead nodes are detected in seconds instead of the OS default (~2 minutes for a stale TCP connection, ~2 minutes for a SYN to a black-holed IP).

### 4. Transaction retry with exponential backoff

`Execute` retries the whole transaction on retryable errors:

| Error code | Cause |
|---|---|
| `40001` | Serialisation failure (concurrent write conflict) |
| `40P01` | Deadlock detected |
| `XX000` | Internal error during Raft leader election |
| `08006` | Connection failure mid-transaction |
| `57P01` | Admin shutdown (rolling upgrade / restart) |
| `EOF`, `ECONNRESET` | Network-level connection drop |

Backoff doubles each attempt (10 ms, 20 ms, 40 ms, …; configurable via `max-retries`). Most leader-election and rolling-restart blips are invisible to callers.

### 5. Connection validation before use

`PrepareConn` pings every connection before handing it to a worker:

```go
poolConfig.PrepareConn = func(ctx context.Context, conn *pgx.Conn) (bool, error) {
    return conn.Ping(ctx) == nil, nil
}
```

Stale connections to a dead node are evicted on acquire instead of failing mid-transaction.

### 6. Sequence cache (×100)

Migration `002_optimize_sequences.sql`:

```sql
ALTER SEQUENCE promises_sort_id_seq  CACHE 100;
ALTER SEQUENCE schedules_sort_id_seq CACHE 100;
ALTER SEQUENCE tasks_sort_id_seq     CACHE 100;
```

Without this, every `nextval()` requires a distributed Raft round-trip to allocate the next value. With `CACHE 100`, each TServer pre-allocates 100 values locally — sequence coordination drops from once per insert to once per 100 inserts.

### 7. Tablet split count

Migration `001_initial_schema.sql` declares `SPLIT INTO N TABLETS` for `promises`, `callbacks`, `schedules`, `tasks`. Higher tablet counts distribute write load across more Raft groups; the right value is bench-driven and currently 6 on a 3-node cluster.

### 8. Hash + composite indexes (parked, see status)

Replacing single-column range indexes on `sort_id` with `(state HASH, sort_id ASC)` distributes writes across tablets by state value while still supporting the `ORDER BY sort_id ASC LIMIT N` cursor pagination queries:

```sql
CREATE INDEX idx_promises_state_sort_id ON promises (state HASH, sort_id ASC);
```

Migrations `003`–`005` explore this and other index changes; they are currently parked (see *Status & roadmap*).

### 9. Prometheus observability

| Metric | Type | Description |
|---|---|---|
| `yugabyte_tx_total` | Counter (ok/failed) | Transaction outcomes |
| `yugabyte_tx_retries_total` | Counter | Transaction retry count |
| `yugabyte_tx_duration_seconds` | Histogram | End-to-end transaction latency (buckets up to 60 s) |
| `yugabyte_batch_duration_seconds` | Histogram (by command type) | Time spent in `SendBatch` |
| `yugabyte_command_total` | Counter (by command type) | Commands processed per type |
| `yugabyte_pool_connections` | Gauge (acquired/idle/total/constructing) | Pool state |
| `yugabyte_errors_total` | Counter (by error class) | Errors by category |

---

## Quickstart with YugabyteDB

### 1. Build

```bash
go build -o resonate
```

> Per repo convention, do not run executables out of the repo root for development — bring services up via Docker Compose.

### 2. Bring up a 3-node YB cluster + monitoring + Resonate

```bash
docker-compose -f docker/docker-compose.fault-tolerance.yml up
```

For faster iteration on a single-node cluster:

```bash
docker-compose -f docker/docker-compose.bench-yugabyte-single.yml up
```

For an apples-to-apples Postgres baseline:

```bash
docker-compose -f docker/docker-compose.bench-postgres.yml up
```

### 3. Endpoints

- HTTP API: `http://localhost:8001`
- Long-poll: `http://localhost:8002`
- Resonate metrics: `http://localhost:9090/metrics`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (default credentials)

### 4. Run the standalone DB probe

```bash
./dbprobe --host yb-node1 --max-conns 4 --interval 50ms
```

---

## Tests

```bash
# Whole tree
make test

# YugaByte store unit tests + micro-benchmarks
go test -v ./internal/app/subsystems/aio/store/yugabyte/...

# Fault-tolerance scenarios (require a running YB cluster — see compose files above)
go test -v ./internal/app/subsystems/aio/store/yugabyte/ -run FaultTolerance
go test -v ./benchmarks/ -run FaultTolerance
```

---

## Code generation

```bash
make deps         # install oapi-codegen and mockgen
make gen-openapi  # regenerate pkg/client/ from api/openapi.yml
make gen-mock     # regenerate test mocks
```

---

## Status & roadmap

- [x] YugabyteDB store backend (full store interface).
- [x] Per-backend migrations + migration runner integration.
- [x] Fault-tolerance harness (6 scenarios) and Prometheus-aware variant.
- [x] Benchmark framework with single-node YB / 3-node YB / Postgres targets.
- [x] Grafana dashboards (server, YB cluster, YB store, fault-tolerance).
- [ ] **Distributed locking across multi-instance Resonate deployments.** The `locks` table exists in migration 001 but the runtime path is not yet wired up — this is the next thesis milestone.

---

## Upstream

This fork tracks [resonatehq/resonate](https://github.com/resonatehq/resonate). For SDK usage (TypeScript, Python), language examples, hosted offerings, and the original quickstart, refer to the upstream README and [docs.resonatehq.io](https://docs.resonatehq.io).
