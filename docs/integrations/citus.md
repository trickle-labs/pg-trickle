# Citus Distributed Tables

pg_trickle supports Citus distributed tables as **sources** for incremental
view maintenance and as **output targets** for stream tables.

## Prerequisites

- PostgreSQL 17 or 18 with `wal_level = logical` on **every** node
  (coordinator and all workers).
- Citus 12.x or 13.x installed on the coordinator and all workers.
- The `dblink` extension installed on the coordinator
  (`CREATE EXTENSION IF NOT EXISTS dblink`).
- pg_trickle installed at the **same version** on every node.
- Each source distributed table must have `REPLICA IDENTITY FULL`:
  ```sql
  ALTER TABLE my_distributed_table REPLICA IDENTITY FULL;
  ```

## Architecture Overview

```
┌───────────────────────────────────────────────────────────┐
│  Citus Coordinator                                         │
│                                                            │
│  pg_trickle scheduler                                      │
│    ├─ reads coordinator WAL slot (local sources)           │
│    └─ polls worker WAL slots via dblink (distributed)      │
│                                                            │
│  pgtrickle.pgt_worker_slots   ← tracks per-worker slots   │
│  pgtrickle.citus_status       ← observability view        │
└─────────────┬────────────┬────────────────────────────────┘
              │            │
        dblink│      dblink│
              ▼            ▼
┌─────────────────┐  ┌─────────────────┐
│  Citus Worker 1 │  │  Citus Worker 2 │
│  WAL slot:      │  │  WAL slot:      │
│  pgtrickle_...  │  │  pgtrickle_...  │
└─────────────────┘  └─────────────────┘
```

pg_trickle creates a logical replication slot on each worker for every
distributed source table. The coordinator scheduler polls these slots via
`dblink` on every tick, merges the decoded changes into the coordinator-local
change buffer, and then applies them to the stream table output.

## Installation

### 1. Verify prerequisites on every node

```sql
-- Run on coordinator AND each worker:
SHOW wal_level;            -- must be 'logical'
SELECT extname, extversion FROM pg_extension WHERE extname IN ('citus', 'pg_trickle', 'dblink');
```

### 2. Create the extension on the coordinator

```sql
CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS pg_trickle;
```

### 3. Run pre-flight checks

pg_trickle provides two pre-flight helpers that verify worker readiness:

```sql
-- COORD-7: Verify pg_trickle version matches on all workers
SELECT pgtrickle.source_stable_name(0::oid);  -- triggers version check on startup

-- COORD-8: Verify wal_level=logical on all workers
-- (checked automatically when a distributed CDC source is set up)
```

### 4. Prepare your distributed source table

```sql
-- Distribute your source table if not already distributed
SELECT create_distributed_table('orders', 'customer_id');

-- REPLICA IDENTITY FULL is required for CDC on distributed tables
ALTER TABLE orders REPLICA IDENTITY FULL;
```

### 5. Create a stream table over a distributed source

```sql
-- Basic stream table (output stored on coordinator)
CALL pgtrickle.create_stream_table(
    name  => 'orders_summary',
    query => 'SELECT customer_id, count(*) AS order_count, sum(amount) AS total
              FROM orders GROUP BY customer_id'
);

-- Distributed output: co-locate the stream table with the source
CALL pgtrickle.create_stream_table(
    name                       => 'orders_summary',
    query                      => 'SELECT customer_id, count(*) AS order_count,
                                           sum(amount) AS total
                                   FROM orders GROUP BY customer_id',
    output_distribution_column => 'customer_id'
);
```

The `output_distribution_column` parameter converts the
output storage table into a Citus distributed table on that column immediately
after creation.  If Citus is not loaded and you pass this parameter, an error
is raised.

## Placement Options

| Placement | When to use | Created by |
|-----------|-------------|------------|
| `local` (default) | Small result sets, coordinator-only queries | `create_stream_table()` without `output_distribution_column` |
| `distributed` | Large result sets, co-location with source shards | `output_distribution_column => 'col'` |
| `reference` | Lookup tables replicated to all workers | `create_distributed_table(st, 'col', colocate_with => 'none')` after creation |

## Monitoring

The `pgtrickle.citus_status` view shows per-worker CDC slot health:

```sql
SELECT
    pgt_schema || '.' || pgt_name AS stream_table,
    source_stable_name,
    source_placement,
    worker_name,
    worker_port,
    worker_slot,
    worker_frontier,
    last_polled_at,
    lease_health        -- 'unlocked' | 'locked' | 'expired'
FROM pgtrickle.citus_status
ORDER BY pgt_name, worker_name;
```

| Column | Description |
|--------|-------------|
| `coordinator_slot` | Local WAL slot name on the coordinator |
| `source_placement` | `distributed`, `reference`, or `local` |
| `worker_name` | Hostname of the Citus worker |
| `worker_port` | Port of the Citus worker |
| `worker_slot` | WAL slot name on the worker |
| `worker_frontier` | Last consumed LSN on the worker |
| `last_polled_at` | Timestamp of the last successful poll for each worker slot |
| `lease_holder` | Session that currently holds the `pgt_st_locks` lease, if any |
| `lease_acquired_at` | When the current lease was acquired |
| `lease_expires_at` | When the current lease expires |
| `lease_health` | `'unlocked'`, `'locked'`, or `'expired'` |

### Worker-failure alerting GUC

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.citus_worker_retry_ticks` | `5` | Consecutive per-worker poll failures before raising a WARNING in the PostgreSQL log. Set to `0` to disable. |
| `pg_trickle.citus_st_lock_lease_ms` | `60000` | Duration (ms) of the `pgt_st_locks` distributed-refresh lease. Must be ≥ `pg_ripple.merge_fence_timeout_ms` when pg_ripple is in use. |

## Failure Modes

### Worker unreachable

If a worker becomes unreachable, `poll_worker_slot_changes()` returns an error.
pg_trickle logs the failure and skips that worker's changes for the current
tick. Refresh resumes automatically once the worker is reachable again.

**Action**: Monitor `pgtrickle.citus_status` and alert on gaps in
`worker_frontier`.

### WAL slot recycled (slot missing or lag too high)

If the coordinator stops polling a worker slot for too long, PostgreSQL may
recycle the WAL and invalidate the slot. pg_trickle will log a
`WalTransitionError` and fall back to a full refresh for that stream table.

**Prevention**: Set `pg_trickle.citus_slot_max_lag_bytes` (default: 1 GB)
and ensure the coordinator restarts within the slot retention window.

**Recovery**:
```sql
-- Drop the stale slot on the worker (via dblink if needed)
SELECT pg_drop_replication_slot('pgtrickle_<stable_name>');
-- pg_trickle will re-create it on the next scheduler tick
```

### Shard rebalance

Citus shard rebalancing changes which worker holds which shards.
pg_trickle detects a topology change automatically (by comparing `pg_dist_node`
active primaries against `pgt_worker_slots`) and recovers without operator
intervention:

1. Stale slot entries for removed workers are dropped.
2. New `pgt_worker_slots` rows are inserted for the incoming workers.
3. The affected stream table is marked for a full refresh on the next tick.

No manual `DROP + CREATE` of stream tables is required after a rebalance.

### Version mismatch across nodes

If pg_trickle versions differ between the coordinator and workers,
`check_citus_version_compat()` raises an error during CDC setup. Install the
same pg_trickle version on all nodes before creating distributed stream tables.

## Known Limitations

- **MERGE** is not supported for distributed stream tables. pg_trickle
  automatically uses the `DELETE + INSERT … ON CONFLICT DO UPDATE` path for
  distributed output tables.

- **Cross-shard JOINs** in the stream table query follow normal Citus pushdown
  rules. If the plan is not pushable, the query runs on the coordinator.

- Citus reference tables work as sources with trigger-based CDC only
  (per-worker WAL slots are not needed for reference tables).

- **Worker failure alerting** — configure `pg_trickle.citus_worker_retry_ticks`
  (default 5) to control how many consecutive poll failures trigger a WARNING.
  Set to `0` to disable the alert entirely.

## pg_ripple Integration

pg_trickle and pg_ripple v0.58.0 can be deployed together on a Citus
cluster.  pg_ripple stores its RDF triples in _Vertical Partitioning (VP)_
tables that are distributed by subject hash (`s BIGINT`).  pg_trickle can track
changes to these tables and materialize downstream stream tables.

### Co-location Contract

VP tables are distributed on `s` (the XXH3-128 subject ID encoded as BIGINT).
Downstream stream tables consuming VP data should use the same distribution
column to avoid coordinator fan-out:

```sql
CALL pgtrickle.create_stream_table(
    name                       => 'rdf_subjects',
    query                      => 'SELECT s, count(*) AS triple_count
                                   FROM _pg_ripple.vp_42_delta GROUP BY s',
    output_distribution_column => 's'   -- co-locate with VP shards
);
```

The natural row identity for such a stream table is `(s, predicate_hash, g)` —
the triple's encoded subject, predicate, and named-graph.  Configure pg_trickle
with this composite key so the `DELETE WHERE row_id IN (…)` apply path targets
the correct shard.

### VP Table Promotion Notifications

When pg_ripple distributes a new VP table it emits a
`pg_ripple.vp_promoted` NOTIFY with the following JSON payload:

```json
{
  "table":             "_pg_ripple.vp_42_delta",
  "shard_count":       32,
  "shard_table_prefix":"_pg_ripple.vp_42_delta_",
  "predicate_id":      42
}
```

pg_trickle ships a helper function that processes this payload.  Use it from
any regular backend session that LISTENs to the channel:

```sql
LISTEN "pg_ripple.vp_promoted";
-- … wait for pg_notify …
-- (in application code: call handle_vp_promoted with the notification payload)
SELECT pgtrickle.handle_vp_promoted(pg_notification_queue_transfer());
-- or pass the payload directly:
SELECT pgtrickle.handle_vp_promoted(
  '{"table":"_pg_ripple.vp_42_delta","shard_count":32,'
  '"shard_table_prefix":"_pg_ripple.vp_42_delta_","predicate_id":42}'
);
```

`handle_vp_promoted()` logs the promotion and, when the VP table is already
tracked as a distributed CDC source, signals the scheduler that worker-slot
probing should run on the next tick.

### Merge Fencing and `pgt_st_locks` Lease Alignment

pg_ripple's merge worker emits `pg_ripple.merge_start` / `merge_end` NOTIFY
signals as **observability hints** — the TRUNCATE+INSERT merge is a single 2PC
transaction so no inconsistent state is visible to pg_trickle's per-worker WAL
decoders even without these signals.

pg_trickle uses `pgtrickle.pgt_st_locks` (catalog-based leases) for cross-node
coordination.  Set the `pgt_st_locks` lease expiry **≥**
`pg_ripple.merge_fence_timeout_ms` to prevent a lease from expiring mid-merge:

```sql
-- pg_ripple side (postgresql.conf or SET):
SET pg_ripple.merge_fence_timeout_ms = 30000;   -- 30 seconds

-- pg_trickle side:
SET pg_trickle.citus_st_lock_lease_ms = 45000;  -- 45 seconds (≥ 30s fence)
```

Monitor both together:

```sql
SELECT
    r.predicate_id,
    r.cycle_duration_ms,
    c.stream_table,
    c.worker_frontier
FROM pg_ripple.merge_status()   r
JOIN pgtrickle.citus_status      c
  ON c.source_stable_name LIKE '_pg_ripple_vp_' || r.predicate_id || '_%';
```

### Prerequisites

- pg_ripple ≥ 0.58.0 (Citus support)
- pg_trickle ≥ 0.33.0 (distributed CDC + stream tables)
- Citus 12.x on all nodes
- `pg_ripple.citus_sharding_enabled = on`
- `pg_ripple.citus_trickle_compat = on` (sets `colocate_with='none'` on VP tables,
  avoiding cross-shard tombstone deletes during CDC apply)

## Performance Considerations

- `dblink` polling adds round-trip latency per worker per tick.  On a loopback
  network, throughput exceeds 50 k rows/s (see `benches/bench_remote_slot_poll`).
  If your workload requires higher throughput, consider batching slot polls or
  increasing the scheduler poll interval.
- For large distributed stream tables, co-locating the output with the source
  shards (`output_distribution_column`) avoids data movement during apply.

## See Also

- [SQL Reference — `create_stream_table`](../SQL_REFERENCE.md)
- [Architecture — Citus distributed CDC](../ARCHITECTURE.md)
- [Monitoring — `citus_status` view](../CONFIGURATION.md)
- [CHANGELOG — v0.32.0](https://github.com/trickle-labs/pg-trickle/blob/main/CHANGELOG.md) (stable naming and frontier foundations)
- [CHANGELOG — v0.33.0](https://github.com/trickle-labs/pg-trickle/blob/main/CHANGELOG.md) (distributed CDC and stream tables)
- [CHANGELOG — v0.34.0](https://github.com/trickle-labs/pg-trickle/blob/main/CHANGELOG.md) (automated distributed CDC scheduler & shard rebalance auto-recovery)
