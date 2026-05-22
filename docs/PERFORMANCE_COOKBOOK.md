# Performance Tuning Cookbook

This document is a practical, recipe-oriented guide to squeezing the best
throughput and latency out of pg_trickle stream tables.  Each recipe
describes *why* a problem occurs, *when* to apply it, and *how* to implement
the fix.

---

## Table of Contents

1. [Choosing the Right Refresh Mode](#1-choosing-the-right-refresh-mode)
2. [Tuning the Scheduler Interval](#2-tuning-the-scheduler-interval)
3. [Controlling Change-Buffer Growth](#3-controlling-change-buffer-growth)
4. [Accelerating Wide-Join Queries](#4-accelerating-wide-join-queries)
5. [Reducing Lock Contention](#5-reducing-lock-contention)
6. [Managing Spill-to-Disk in Large Deltas](#6-managing-spill-to-disk-in-large-deltas)
7. [Speeding Up FULL Refresh with Parallelism](#7-speeding-up-full-refresh-with-parallelism)
8. [Monitoring with Prometheus](#8-monitoring-with-prometheus)
9. [Partition-Aware Stream Tables](#9-partition-aware-stream-tables)
10. [Adaptive Threshold Tuning](#10-adaptive-threshold-tuning)
11. [Canary Testing Query Changes](#11-canary-testing-query-changes)
12. [Recovering from Stale Stream Tables](#12-recovering-from-stale-stream-tables)

---

## 1. Choosing the Right Refresh Mode

**Problem:** DIFFERENTIAL refresh is slower than expected, or FULL refresh
keeps being chosen by the adaptive engine when you expect DIFFERENTIAL.

**Diagnosis:** Run the diagnostics helper:

```sql
SELECT * FROM pgtrickle.diagnose_stream_table('public.orders_mv');
```

Look at `recommended_mode`, `composite_score`, and `change_ratio_current`.

**Recipe — Force DIFFERENTIAL for low-churn tables:**

```sql
SELECT pgtrickle.alter_stream_table(
    'public.orders_mv',
    refresh_mode => 'DIFFERENTIAL'
);
```

Use this when:
- `change_ratio_current < 0.05` (less than 5% of rows change per tick)
- The query has no DISTINCT, EXCEPT, or INTERSECT at the top level
- The table has a suitable covering index on the join/group-by columns

**Recipe — Force FULL for high-churn or complex queries:**

```sql
SELECT pgtrickle.alter_stream_table(
    'public.summary_mv',
    refresh_mode => 'FULL'
);
```

Use this when:
- `change_ratio_current > 0.30`
- The query contains `WITH RECURSIVE`, complex GROUPING SETS, or multiple
  correlated subqueries

**Recipe — Use AUTO (recommended default):**

```sql
SELECT pgtrickle.alter_stream_table(
    'public.orders_mv',
    refresh_mode => 'AUTO'
);
```

AUTO switches between FULL and DIFFERENTIAL each cycle based on the
adaptive cost model (`pg_trickle.cost_model_safety_margin`).

---

## 2. Tuning the Scheduler Interval

**Problem:** Stream tables are falling behind the source; refreshes are not
running often enough.  Or conversely, the scheduler is running too
frequently, creating unnecessary load.

**Diagnosis:**

```sql
-- Check average staleness across all active stream tables
SELECT pgt_name, staleness_seconds
FROM pgtrickle.st_refresh_stats()
ORDER BY staleness_seconds DESC NULLS LAST;
```

**Recipe — Reduce the poll interval for fresher data:**

```sql
-- In postgresql.conf or via ALTER SYSTEM:
ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 250;
SELECT pg_reload_conf();
```

Minimum safe value: `250` ms.  Below this, CPU overhead from the scheduler
loop becomes noticeable.

**Recipe — Set a per-table schedule:**

```sql
-- Refresh every 30 seconds
SELECT pgtrickle.alter_stream_table('public.orders_mv', schedule => '30s');

-- Refresh using a cron expression (every 5 minutes)
SELECT pgtrickle.alter_stream_table('public.daily_agg', schedule => '*/5 * * * *');
```

Per-table schedules override the global poll interval for that stream table.

---

## 3. Controlling Change-Buffer Growth

**Problem:** The change buffer schema (`pgtrickle_changes.*`) keeps growing
and consuming disk space.

**Diagnosis:**

```sql
SELECT schemaname, tablename,
       pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = current_setting('pg_trickle.change_buffer_schema', true)
                   ::text
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 20;
```

**Recipe — Reduce the WAL-to-buffer retention window:**

```sql
-- Advance the frontier faster by refreshing more frequently
SELECT pgtrickle.alter_stream_table('public.orders_mv', schedule => '5s');
```

pg_trickle deletes change-buffer rows once every stream table that
references the source has consumed them.  Slow stream tables block cleanup.

**Recipe — Enable truncate-based cleanup (faster for large buffers):**

```sql
ALTER SYSTEM SET pg_trickle.cleanup_use_truncate = on;
SELECT pg_reload_conf();
```

Uses `TRUNCATE` instead of `DELETE` when cleaning up entire partitioned
change-buffer tables.  Avoids bloat from frequent deletes.

---

## 4. Accelerating Wide-Join Queries

**Problem:** DIFFERENTIAL refresh on a query with 5+ table joins is slow.

**Diagnosis:**

```sql
-- Check the join scan count
SELECT pgtrickle.validate_query($$ SELECT … FROM a JOIN b JOIN c … $$);
```

**Recipe — Enable planner hints for wide joins:**

```sql
ALTER SYSTEM SET pg_trickle.planner_aggressive = on;
ALTER SYSTEM SET pg_trickle.merge_planner_hints = on;
SELECT pg_reload_conf();
```

This sets `SET LOCAL enable_seqscan = off` and `SET LOCAL join_collapse_limit = 1`
before the MERGE execution, forcing the planner to use indexes.

**Recipe — Limit differential join depth:**

```sql
SELECT pgtrickle.alter_stream_table(
    'public.complex_mv',
    max_differential_joins => 4
);
```

When join count exceeds `max_differential_joins`, pg_trickle falls back to
FULL refresh instead of failing with a planning error.

**Recipe — Add covering indexes on join keys:**

```sql
-- The differential engine joins on __pgt_row_id; ensure the join keys
-- are indexed in both the storage table and source tables.
CREATE INDEX CONCURRENTLY ON orders (customer_id, order_date);
CREATE INDEX CONCURRENTLY ON customers (id) INCLUDE (name, region);
```

---

## 5. Reducing Lock Contention

**Problem:** `lock timeout` errors appear in `pgt_refresh_history`, or
queries against the stream table are blocked during refresh.

**Diagnosis:**

```sql
SELECT * FROM pgtrickle.diagnose_errors('public.orders_mv') LIMIT 10;
```

Look for `error_type = 'performance'` with `lock timeout` in `error_message`.

**Recipe — Increase lock timeout:**

```sql
ALTER SYSTEM SET pg_trickle.lock_timeout = '5s';
SELECT pg_reload_conf();
```

**Recipe — Use APPEND_ONLY mode for insert-only pipelines:**

```sql
SELECT pgtrickle.alter_stream_table(
    'public.events_mv',
    append_only => true
);
```

APPEND_ONLY skips the MERGE and uses a fast `INSERT … SELECT` which
holds locks for a much shorter time.

**Recipe — Use pooler compatibility mode:**

```sql
SELECT pgtrickle.alter_stream_table(
    'public.orders_mv',
    pooler_compatibility_mode => true
);
```

Disables prepared-statement reuse, which can cause issues with PgBouncer
in transaction-pool mode.

---

## 6. Managing Spill-to-Disk in Large Deltas

**Problem:** Differential refresh writes large amounts of temp data, causing
performance degradation.

**Diagnosis:**

```sql
SELECT pgt_name, last_temp_blks_written
FROM pgtrickle.st_refresh_stats();
```

**Recipe — Increase work_mem for MERGE operations:**

```sql
ALTER SYSTEM SET pg_trickle.merge_work_mem_mb = 256;
SELECT pg_reload_conf();
```

**Recipe — Set a spill threshold to auto-switch to FULL:**

```sql
-- Force FULL refresh after 3 consecutive spilling differentials
ALTER SYSTEM SET pg_trickle.spill_threshold_blocks = 10000;
ALTER SYSTEM SET pg_trickle.spill_consecutive_limit = 3;
SELECT pg_reload_conf();
```

After `spill_consecutive_limit` consecutive differential refreshes that
write more than `spill_threshold_blocks` temp blocks, pg_trickle switches
to FULL refresh for that stream table.

---

## 7. Speeding Up FULL Refresh with Parallelism

**Problem:** FULL refresh is slow due to large source tables.

**Recipe — Enable parallel query for FULL refresh:**

```sql
-- Allow more parallel workers
ALTER SYSTEM SET max_parallel_workers_per_gather = 8;
ALTER SYSTEM SET parallel_tuple_cost = 0.01;
SELECT pg_reload_conf();
```

pg_trickle uses `INSERT INTO … SELECT …` which respects the standard
PostgreSQL parallel query settings.

**Recipe — Enable partition-parallel refresh:**

```sql
SELECT pgtrickle.alter_stream_table(
    'public.orders_mv',
    partition_by => 'region'
);
```

With `partition_by`, pg_trickle dispatches one refresh worker per partition,
running them in parallel.

---

## 8. Monitoring with Prometheus

**Problem:** You want to monitor pg_trickle metrics with Prometheus.

**Recipe — Enable the built-in metrics endpoint:**

```sql
ALTER SYSTEM SET pg_trickle.metrics_port = 9188;
SELECT pg_reload_conf();
```

Then configure Prometheus to scrape:

```yaml
scrape_configs:
  - job_name: pg_trickle
    static_configs:
      - targets: ['localhost:9188']
    metrics_path: /metrics
```

Available metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `pg_trickle_refreshes_total` | counter | Successful refreshes per stream table |
| `pg_trickle_refresh_failures_total` | counter | Failed refreshes per stream table |
| `pg_trickle_rows_changed_total` | counter | Rows inserted + deleted per table |
| `pg_trickle_consecutive_errors` | gauge | Current error streak per table |
| `pg_trickle_active` | gauge | 1 if ACTIVE, 0 otherwise |

**Recipe — Check staleness via SQL (for custom alerting):**

```sql
SELECT pgt_name, staleness_seconds, stale
FROM pgtrickle.st_refresh_stats()
WHERE stale = true;
```

---

## 9. Partition-Aware Stream Tables

**Problem:** A stream table over a large partitioned source is slow to refresh.

**Recipe — Mirror source partitioning:**

```sql
-- If the source is RANGE partitioned by order_date:
SELECT pgtrickle.create_stream_table(
    'public.orders_by_region',
    'SELECT customer_id, SUM(total) FROM orders GROUP BY customer_id',
    partition_by => 'customer_id'
);
```

**Recipe — Per-partition MERGE for HASH-partitioned targets:**

pg_trickle automatically uses per-partition MERGE when the stream table
is HASH-partitioned.  No additional configuration is needed; the optimizer
routes each row to the correct partition.

---

## 10. Adaptive Threshold Tuning

**Problem:** The adaptive engine keeps switching between FULL and DIFFERENTIAL
unexpectedly.

**Recipe — Widen the dead zone (less switching):**

```sql
-- Require a 30% score difference before switching (default: 20%)
ALTER SYSTEM SET pg_trickle.cost_model_safety_margin = 0.30;
SELECT pg_reload_conf();
```

**Recipe — Use self-monitoring analytics to auto-tune:**

```sql
-- Let pg_trickle automatically apply threshold recommendations
ALTER SYSTEM SET pg_trickle.self_monitoring_auto_apply = 'threshold_only';
SELECT pg_reload_conf();
```

With `threshold_only`, pg_trickle applies `max_delta_fraction` changes
from `pgtrickle.df_threshold_advice` when confidence is HIGH.

---

## 11. Canary Testing Query Changes

**Problem:** You want to change a stream table's defining query safely without
impacting production.

**Recipe — Use canary/shadow mode:**

```sql
-- 1. Create a canary table with the new query
SELECT pgtrickle.canary_begin(
    'public.orders_mv',
    'SELECT customer_id, COUNT(*), SUM(total) FROM orders GROUP BY customer_id'
);

-- 2. Wait for the canary to populate (check status)
SELECT status FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = '__pgt_canary_orders_mv';

-- 3. Compare live vs canary output
SELECT * FROM pgtrickle.canary_diff('public.orders_mv');

-- 4. If diff is empty (or acceptable), promote the canary
SELECT pgtrickle.canary_promote('public.orders_mv');
```

The `canary_diff` result will be empty when both the old and new queries
produce identical output for the current source data.

---

## 12. Recovering from Stale Stream Tables

**Problem:** A stream table is SUSPENDED or has a large backlog of changes.

**Recipe — Pause all tables, catch up, then resume:**

```sql
SELECT pgtrickle.pause_all();

-- Investigate
SELECT pgt_name, status, consecutive_errors
FROM pgtrickle.pgt_stream_tables
ORDER BY consecutive_errors DESC;

-- Fix the root cause, then resume
SELECT pgtrickle.resume_all();
```

**Recipe — Force immediate refresh on stale tables:**

```sql
-- Refresh only if older than 10 minutes
SELECT pgtrickle.refresh_if_stale('public.orders_mv', '10 minutes');
```

**Recipe — Full reinitialization after schema change:**

```sql
-- If source schema changed, reinitialize to rebuild column metadata
SELECT pgtrickle.reinitialize_stream_table('public.orders_mv');
```

---

## 13. DVM Query Complexity Limits

**Problem:** Differential refresh is slower than full refresh for complex
queries, especially at scale. Understanding when DIFFERENTIAL mode breaks
down helps you choose the right strategy.

### Three Failure Mode Categories

| Category | SQL Pattern | Symptom | Root Cause |
|----------|-------------|---------|------------|
| **Threshold Collapse** | 4+ table JOINs with cascading EXCEPT ALL | Fast at small scale, 100–260× slower per data decade | Intermediate CTE cardinality blowup: O(n²) row generation from L₀ snapshot expansion |
| **Early Collapse** | EXISTS anti-join with non-equi predicates | 140× jump at first 10× scale step, then stable | Equi-join key filter not applied correctly; R_old EXCEPT ALL scans full table |
| **Structural Bug** | Doubly-nested correlated EXISTS / NOT EXISTS | Slow at all scales (constant ~2s overhead) | Inner R_old re-materialized per outer delta row: O(Δ_outer × n_inner) |

### Which SQL Patterns Trigger Each Category

**Threshold Collapse** (queries like TPC-H Q05, Q07, Q08, Q09):
- Multi-table joins (4+ tables) using the cascading `EXCEPT ALL` delta strategy
- Queries with many intermediate join nodes generate exponential intermediate rows
- Diagnosis: `pgtrickle.log_delta_sql = on` + `EXPLAIN (ANALYZE, BUFFERS)`

**Early Collapse** (queries like TPC-H Q04):
- `WHERE EXISTS (SELECT 1 FROM t WHERE t.key = outer.key AND t.col < t.col2)`
- The non-equi predicates in the EXISTS clause can prevent key-filter extraction
- Diagnosis: Check if `R_old` CTE scans the full right table

**Structural Bug** (queries like TPC-H Q20):
- `WHERE EXISTS (SELECT 1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE ...))`
- Inner snapshot CTEs are re-evaluated per outer row instead of shared
- Diagnosis: Look for repeated CTE evaluations in `EXPLAIN ANALYZE`

### Recommended Scale Factors

| Pattern | Safe for DIFF | Use FULL above |
|---------|---------------|----------------|
| Simple scan/filter | Any scale | — |
| 2-table JOIN | Up to ~10M rows | — |
| 3-table JOIN | Up to ~1M rows | ~10M rows |
| 4+ table JOIN | Up to ~100K rows | ~1M rows |
| EXISTS anti-join | Up to ~100K rows | ~1M rows |
| Nested EXISTS | Use FULL mode | — |

### Diagnosing Your Query

```sql
-- 1. Enable delta SQL logging
SET pg_trickle.log_delta_sql = on;

-- 2. Trigger a manual refresh
SELECT pgtrickle.refresh_stream_table('my_stream_table');

-- 3. Check the PostgreSQL log for the generated delta SQL
-- 4. Run EXPLAIN (ANALYZE, BUFFERS) on the captured SQL
-- 5. Look for:
--    - Nested Loop joins on large tables (threshold collapse)
--    - Sequential scans on R_old CTEs (early collapse)
--    - Repeated CTE evaluations (structural bug)

-- Use explain_diff_sql() to inspect without executing:
SELECT pgtrickle.explain_diff_sql('my_stream_table');
```

### Mitigation GUCs

```sql
-- Increase work_mem for delta execution
SET pg_trickle.delta_work_mem = 256;  -- MB

-- Disable nested loops for delta execution
SET pg_trickle.delta_enable_nestloop = off;

-- Run ANALYZE on change buffers (enabled by default)
SET pg_trickle.analyze_before_delta = on;
```

### Worked Example A — `max_diff_ctes` Hit and Recovery

**Symptom:** `EXPLAIN ANALYZE` shows more than `pg_trickle.max_diff_ctes`
(default 64) CTEs in the generated delta SQL, and the refresh falls back to
FULL mode with a warning in `pgt_refresh_history`.

**Diagnosis:**

```sql
-- Check the warning in the refresh history
SELECT pgt_name, refresh_mode, rows_in_last_refresh, warning_message
FROM pgtrickle.pgt_refresh_history
WHERE pgt_name = 'my_complex_view'
ORDER BY started_at DESC
LIMIT 5;

-- Inspect the generated delta SQL
SELECT pgtrickle.explain_diff_sql('my_complex_view');
-- Count the CTE blocks in the output
```

**Recovery steps:**

```sql
-- Option 1: Raise the limit (accept higher delta execution cost)
ALTER SYSTEM SET pg_trickle.max_diff_ctes = 128;
SELECT pg_reload_conf();

-- Option 2: Simplify the query — split a complex view into two stream tables
-- First level: join + filter
SELECT pgtrickle.create_stream_table(
    'orders_with_products',
    'SELECT o.*, p.name AS product_name FROM orders o JOIN products p ON p.id = o.product_id',
    '10s', 'DIFFERENTIAL'
);
-- Second level: aggregate over the first
SELECT pgtrickle.create_stream_table(
    'revenue_summary',
    'SELECT product_name, SUM(amount) AS total FROM orders_with_products GROUP BY product_name',
    '15s', 'DIFFERENTIAL'
);

-- Option 3: Force FULL mode for queries that genuinely exceed complexity budget
SELECT pgtrickle.alter_stream_table('my_complex_view', refresh_mode => 'FULL');
```

### Worked Example B — Detecting When FULL Beats DIFFERENTIAL

**Symptom:** The AUTO cost model keeps switching between FULL and DIFFERENTIAL
every few cycles, or `diff_speedup` from `refresh_efficiency()` is below 1.5×.

**Diagnosis using `recommend_refresh_mode()`:**

```sql
-- Get the weighted signal breakdown for the table
SELECT
    pgt_name,
    current_mode,
    recommended_mode,
    confidence,
    reason,
    jsonb_pretty(signals) AS signals
FROM pgtrickle.recommend_refresh_mode('my_table');
```

Examine the `signals` output.  Key indicators that FULL is better:

| Signal | Value that favours FULL |
|--------|-------------------------|
| `change_ratio_avg` | > 0.30 (>30% of rows change per tick) |
| `empirical_timing` | DIFF and FULL latency within 10% |
| `latency_variance` | p95/p50 > 3 for DIFFERENTIAL |
| `query_complexity` | Score < 0 (many joins / CTEs) |

**Apply the recommendation:**

```sql
-- Switch to FULL when composite_score < -0.15
SELECT pgtrickle.alter_stream_table('my_table', refresh_mode => 'FULL');

-- Switch to AUTO and let the cost model decide going forward
SELECT pgtrickle.alter_stream_table('my_table', refresh_mode => 'AUTO');

-- Set the switching dead-zone wider to reduce oscillation
ALTER SYSTEM SET pg_trickle.cost_model_safety_margin = 0.25;
SELECT pg_reload_conf();
```

### Worked Example C — Deep-Join Chain and `max_differential_joins`

**Symptom:** A stream table with a 6-way JOIN is slow in DIFFERENTIAL mode,
even though individual tables are small.

**Diagnosis:**

```sql
-- Enable delta SQL logging and trigger a refresh
SET pg_trickle.log_delta_sql = on;
SELECT pgtrickle.refresh_stream_table('deep_join_view');

-- Check effective join count reported by the engine
SELECT pgt_name, query_join_depth, last_refresh_mode_reason
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'deep_join_view';
```

**Understanding the GUC:**

`pg_trickle.max_differential_joins` (default: 4) sets the maximum number of
right-side scan expansions the delta engine will attempt before falling back
to FULL mode.  Each additional join roughly doubles the number of delta CTE
branches generated.

**Tuning steps:**

```sql
-- Option 1: Allow deeper join differentiation (accept higher delta cost)
ALTER SYSTEM SET pg_trickle.max_differential_joins = 6;
SELECT pg_reload_conf();

-- Option 2: Intermediate stream table to break the join chain
-- Split a 6-way join into two 3-way steps:
SELECT pgtrickle.create_stream_table(
    'join_layer_1',
    $$SELECT a.*, b.val AS b_val, c.val AS c_val
      FROM table_a a
      JOIN table_b b ON b.id = a.b_id
      JOIN table_c c ON c.id = a.c_id$$,
    '5s', 'DIFFERENTIAL'
);
SELECT pgtrickle.create_stream_table(
    'join_layer_2',
    $$SELECT l1.*, d.val AS d_val, e.val AS e_val, f.val AS f_val
      FROM join_layer_1 l1
      JOIN table_d d ON d.id = l1.d_id
      JOIN table_e e ON e.id = l1.e_id
      JOIN table_f f ON f.id = l1.f_id$$,
    '10s', 'DIFFERENTIAL'
);

-- Option 3: Verify the deep-join fast-path is eligible for a given query
SELECT pgtrickle.validate_query(
    $$<your deep-join query>$$
);
```

Breaking the join chain into two stream tables reduces each step to ≤3
right-side expansions, well within the default `max_differential_joins = 4`
limit.

---

## See Also

- [docs/CONFIGURATION.md](CONFIGURATION.md) — full GUC reference
- [docs/SQL_REFERENCE.md](SQL_REFERENCE.md) — SQL function reference
- [docs/TROUBLESHOOTING.md](TROUBLESHOOTING.md) — common error messages and fixes
- [docs/BENCHMARK.md](BENCHMARK.md) — benchmark results and methodology
- [docs/SCALING.md](SCALING.md) — guidance for large deployments
