# Capacity Planning

This page helps you estimate the resources pg_trickle will need
*before* you put it in production: disk for change buffers and WAL,
memory for refresh execution, CPU for the scheduler and refresh
workers, and connection budget for background workers.

The rules of thumb here are starting points. The
[Performance Cookbook](PERFORMANCE_COOKBOOK.md) and
[Scaling Guide](SCALING.md) cover how to tune once you have real
data to work with.

---

## Quick sizing table

| Deployment size | Stream tables | Source tables | Sustained write rate | Recommended starting config |
|---|---|---|---|---|
| **Small** | 1–20 | 1–20 | < 100/s | All defaults |
| **Medium** | 20–100 | 20–100 | 100–1,000/s | `parallel_refresh_mode=on`, `max_dynamic_refresh_workers=4` |
| **Large** | 100–500 | 50–500 | 1,000–10,000/s | `tiered_scheduling=on`, `max_dynamic_refresh_workers=8`, WAL CDC |
| **Very large** | 500+ | 500+ | > 10,000/s | Add per-database quotas; consider Citus |

---

## Disk: change buffers

Each source table referenced by a stream table gets its own change
buffer (`pgtrickle_changes.changes_<oid>`). One row per captured
change.

**Per-row size estimate (trigger CDC):**

```
~ row_overhead (24 B)
+ key_columns (≈ 2 × avg_key_size)
+ referenced_columns (sum of referenced col sizes)
+ bitmap (1–2 B for narrow tables, ~ ncols/8 otherwise)
```

**Rule of thumb:** budget **~1.5 KB per captured change** for a
typical wide-row OLTP table, **~150 B** for a narrow lookup table.

**Steady-state size:**

```
buffer_bytes ≈ writes_per_second × refresh_interval × per_row_size × (1 - compaction_ratio)
```

Compaction collapses cancelling INSERT/DELETE pairs and successive
updates to the same row. Typical compaction ratios:

| Workload | Compaction ratio |
|---|---|
| Append-only event log | 0% |
| Mixed OLTP | 30–60% |
| High-churn (frequent UPDATEs to same key) | 70–95% |

**Worked example.** A source table doing 5,000 writes/s, refreshed
every 5 s, with 50% compaction and 1 KB rows:

```
5000 × 5 × 1024 × 0.5 ≈ 12.8 MiB per refresh cycle
```

That is the *peak* size of the buffer between refreshes. After the
refresh, bounded `DELETE` removes consumed rows from ordinary buffers.
Dead tuples require vacuuming. Partitioned buffers use partition cleanup.

**Alerts.** Set `pg_trickle.buffer_alert_threshold` (default
`100000` rows) so a `WARNING` is logged before a buffer becomes
unbounded.

---

## Disk: WAL retention (WAL CDC mode)

If you use `pg_trickle.cdc_mode = 'auto'` or `'wal'`, each source
table gets a logical replication slot. PostgreSQL retains WAL until
every active slot has consumed it.

**Worst-case retention** = `slot_lag_critical_threshold_mb` (default
`1024 MB`). Add this *per source* to your WAL disk budget if you
expect occasional refresh delays.

**Recommended monitoring:**

```sql
SELECT * FROM pgtrickle.check_cdc_health()
WHERE severity != 'OK';
```

---

## Memory: refresh execution

Each refresh runs a `MERGE` (DIFFERENTIAL) or full `INSERT … SELECT`
(FULL). Memory usage is dominated by hash tables and sorts:

```
peak_memory ≈ work_mem × (number of hash/sort nodes in the plan)
```

For most stream tables, `work_mem = 64 MB` is comfortable. Wide
joins or large `GROUP BY` may benefit from `256 MB`. Use
`pg_trickle.merge_work_mem_mb` to set it per-refresh without
affecting the rest of the database.

`pg_trickle.spill_threshold_blocks` controls when intermediate
results spill to disk; raise it on memory-rich servers.

---

## CPU and worker processes

The scheduler is one background worker per database. Refresh work
is either inline (default) or dispatched to a dynamic worker pool
(`pg_trickle.parallel_refresh_mode = on`).

```
max_worker_processes  ≥  1 (launcher)
                       + N (one scheduler per pg_trickle database)
                       + max_dynamic_refresh_workers
                       + autovacuum_max_workers
                       + max_parallel_workers
                       + other extensions
```

A typical safe starting point:

```ini
# postgresql.conf
max_worker_processes              = 32
max_parallel_workers              = 8
pg_trickle.max_dynamic_refresh_workers = 4
```

Defaults of 8 are usually too low — the [Pre-Deployment
Checklist](PRE_DEPLOYMENT.md) calls this out as the most common
silent misconfiguration.

---

## DAG topology and scheduling overhead

The scheduler walks the dependency DAG every tick (default 1 s).
Per-stream-table overhead is small (sub-millisecond) but not zero.
For very large DAGs:

| Stream tables | Recommended scheduler interval |
|---|---|
| < 50 | `1000 ms` (default) |
| 50–200 | `1000–2000 ms`, plus `tiered_scheduling=on` |
| 200–1000 | `2000–5000 ms` + Hot/Warm/Cold tiers |
| 1000+ | Consider splitting across databases |

The scheduler's zero-change overhead is documented in
[README – Zero-Change Latency](https://github.com/trickle-labs/pg-trickle/blob/main/README.md#zero-change-latency)
(target < 10 ms).

---

## Connection budget

Each parallel-refresh worker uses one PostgreSQL backend slot. The
scheduler uses one. The launcher uses one. So:

```
backends_used_by_pg_trickle = 1 + databases + max_dynamic_refresh_workers
```

If you front PostgreSQL with PgBouncer, this is **separate** from
your application's pool — pg_trickle's background workers connect
directly, not through the pooler.

---

## Network (Citus & multi-node)

In Citus deployments, the coordinator polls each worker's WAL slot
on every scheduler tick via `dblink`. Bandwidth scales with the
*delta volume*, not the source-table size, but you still need a
fast and reliable coordinator-to-worker network.

Plan for:

- One TCP connection per worker per polling cycle.
- Short, frequent reads.
- Tolerance for individual worker failures —
  `pg_trickle.citus_worker_retry_ticks` controls when failures
  escalate to `WARNING`.

---

## Forecasting growth

A workable rough model for a year of growth:

```
year_1_disk = current_buffer_peak × growth_factor
            + current_storage × growth_factor × number_of_stream_tables
            + WAL_retention_budget
```

Stream-table storage itself is just an ordinary heap table — its
size is the size of the result set, no different from a materialized
view.

---

## Sanity-check queries

```sql
-- Per-source change-buffer size
SELECT * FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;

-- Per-stream-table refresh stats
SELECT pgt_name, last_full_ms, last_diff_ms, p95_ms
FROM pgtrickle.st_refresh_stats()
ORDER BY p95_ms DESC NULLS LAST;

-- Worker-pool saturation
SELECT * FROM pgtrickle.worker_pool_status();
```

---

**See also:**
[Scaling Guide](SCALING.md) ·
[Performance Cookbook](PERFORMANCE_COOKBOOK.md) ·
[Configuration](CONFIGURATION.md) ·
[Pre-Deployment Checklist](PRE_DEPLOYMENT.md)
