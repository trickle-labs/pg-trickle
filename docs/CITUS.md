# Citus Distributed Tables

pg_trickle supports [Citus](https://www.citusdata.com/) distributed
tables as **sources** for incremental view maintenance and as
**output targets** for stream tables. Once configured, distribution
is mostly invisible: you create stream tables exactly as you would
on single-node PostgreSQL, and pg_trickle handles per-worker change
capture and merging on the coordinator.

> **Citus support available** — distributed sources, output targets, and fully automated per-worker scheduling.

> This page is the canonical entry point for Citus support. The
> long-form reference (worker-slot lifecycle, troubleshooting, and
> internal architecture) lives at
> [integrations/citus.md](integrations/citus.md).

---

## What you get

- **Distributed sources.** Define a stream table whose source is a
  Citus-distributed table. pg_trickle creates a logical replication
  slot on each worker, polls all slots from the coordinator via
  `dblink`, and merges the changes into the stream table's storage.
- **Distributed output.** Pass `output_distribution_column` to
  `create_stream_table()` and the resulting stream table is itself a
  Citus distributed table, co-located with your source shards.
- **Automated scheduler.** the per-worker slot lifecycle
  (`ensure_worker_slot`, `poll_worker_slot_changes`, lease
  management) runs automatically — no manual wiring required.
- **Shard-rebalance auto-recovery.** Topology changes detected by
  comparing `pg_dist_node` against `pgt_worker_slots`; stale slots
  are pruned and new ones inserted without operator intervention.
- **Worker failure isolation.** Per-worker poll failures are logged
  and skipped; healthy workers keep running. After
  `pg_trickle.citus_worker_retry_ticks` (default 5) consecutive
  failures, a `WARNING` is raised.

---

## Prerequisites

- PostgreSQL 17 or 18 with `wal_level = logical` on **every** node
  (coordinator and workers).
- Citus 12.x or 13.x on the coordinator and all workers.
- The `dblink` extension on the coordinator.
- pg_trickle installed at the **same version** on every node.
- Each source distributed table must have `REPLICA IDENTITY FULL`.

---

## Quickstart

### 1. Verify prerequisites

```sql
-- Run on coordinator AND each worker:
SHOW wal_level;            -- must be 'logical'
SELECT extname, extversion FROM pg_extension
 WHERE extname IN ('citus', 'pg_trickle', 'dblink');
```

### 2. Create extensions on the coordinator

```sql
CREATE EXTENSION IF NOT EXISTS dblink;
CREATE EXTENSION IF NOT EXISTS pg_trickle;
```

### 3. Prepare a distributed source table

```sql
-- Distribute (or co-locate) the source
SELECT create_distributed_table('orders', 'customer_id');

-- Required for logical decoding to capture old values on UPDATE / DELETE
ALTER TABLE orders REPLICA IDENTITY FULL;
```

### 4. Create a stream table over distributed sources

```sql
SELECT pgtrickle.create_stream_table(
    'order_totals',
    $$SELECT customer_id, SUM(amount) AS total
      FROM orders GROUP BY customer_id$$,
    schedule => '5s'
);
```

That is it on the user side. pg_trickle:

1. Detects that `orders` is distributed.
2. Creates a per-worker logical replication slot.
3. Records each slot in `pgtrickle.pgt_worker_slots`.
4. Polls every slot on each scheduler tick via `dblink`.
5. Merges decoded changes into the coordinator-local change buffer.
6. Applies the delta to the stream table.

### 5. (Optional) make the output distributed too

```sql
SELECT pgtrickle.create_stream_table(
    'order_totals',
    $$SELECT customer_id, SUM(amount) AS total
      FROM orders GROUP BY customer_id$$,
    schedule                     => '5s',
    output_distribution_column   => 'customer_id'
);
```

The result table is now itself distributed on `customer_id` and
co-located with the source shards.

---

## Observability

| Helper | Purpose |
|---|---|
| `SELECT * FROM pgtrickle.citus_status;` | Per-worker slot summary |
| `SELECT * FROM pgtrickle.pgt_worker_slots;` | Raw slot catalogue |
| `SELECT * FROM pgtrickle.check_cdc_health();` | WAL slot health (lag, status) |
| `SELECT * FROM pgtrickle.health_check();` | Whole-extension triage |

---

## Caveats

- **DDL on distributed sources** is more involved than on local
  tables; see the long-form guide.
- **Foreign keys across shards** are restricted by Citus, not by
  pg_trickle.
- **Co-location:** if your stream table joins distributed tables,
  the join columns must be the distribution columns (a Citus
  requirement).

---

**See also:**
[Long-form Citus reference (worker slots, lifecycle, internals)](integrations/citus.md) ·
[CDC Modes](CDC_MODES.md) ·
[Configuration – `pg_trickle.citus_*`](CONFIGURATION.md) ·
[CloudNativePG integration](integrations/cloudnativepg.md)
