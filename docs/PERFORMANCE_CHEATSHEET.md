# Performance Cheat Sheet

Quick reference for pg_trickle performance tuning. For in-depth explanations,
see [CONFIGURATION.md](CONFIGURATION.md) and
[PERFORMANCE_COOKBOOK.md](PERFORMANCE_COOKBOOK.md).

---

## Three Golden Rules

1. **Measure before tuning.** Use `pgtrickle.explain_st('my_table')` and
   `pgtrickle.refresh_history('my_table')` to understand where time is spent
   before adjusting any GUC.

2. **DIFFERENTIAL is almost always faster for Δ-small changes.** Only switch
   to FULL mode when the change-to-table ratio is high (> 15%) or the delta
   SQL is genuinely slower than a full scan on your data distribution.

3. **The bottleneck is usually the change buffer or `work_mem`.** If refreshes
   are slow, check `pg_stat_statements` for temp block writes before adjusting
   schedule frequency.

---

## Top-10 GUC Quick Wins

| GUC | Default | Tune when... | Recommended value |
|-----|---------|-------------|------------------|
| `pg_trickle.parallel_refresh_mode` | `'on'` | You have many independent stream tables | Keep `'on'`; set `'off'` only to debug |
| `pg_trickle.max_concurrent_refreshes` | `4` | Parallelism is bottlenecked or over-saturating I/O | Set to number of independent DAG branches (2–8 typical) |
| `pg_trickle.differential_max_change_ratio` | `0.15` | DIFF is slower than FULL at peak write rates | Raise to `0.30`–`0.50` on high-churn workloads |
| `pg_trickle.delta_work_mem` | `0` (inherit) | Refresh spills temp blocks | Set to `128` (MB) for complex joins; `256`+ for large aggregates |
| `pg_trickle.analyze_before_delta` | `true` | Planner picks bad plans on stale stats | Keep `true`; set `false` only if ANALYZE overhead is measurable |
| `pg_trickle.aggregate_fast_path` | `true` | Aggregation refreshes are slow | Keep `true` (uses explicit DML instead of MERGE for simple aggregates) |
| `pg_trickle.scheduler_interval_ms` | `1000` | Scheduler CPU overhead is high | Raise to `5000`–`10000` on clusters with 100+ stream tables |
| `pg_trickle.cleanup_use_truncate` | `true` | Compatibility setting only | Both values use bounded DELETE for ordinary buffers; monitor dead tuples and autovacuum |
| `pg_trickle.tiered_scheduling` | `true` | Cold stream tables waste CPU cycles | Keep `true` (prevents cold STs from refreshing at full speed) |
| `pg_trickle.max_delta_estimate_rows` | `0` | OOM or excessive temp spill on large deltas | Set to `100000`–`500000` to cap delta size and trigger FULL fallback |

---

## 5 FULL-Fallback Patterns and How to Fix Them

These patterns cause pg_trickle to fall back to FULL refresh automatically.
Each can often be rewritten for DIFFERENTIAL support.

### Pattern 1: Volatile function in SELECT

```sql
-- ❌ Forces FULL: now() is volatile
SELECT id, created_at, now() - created_at AS age FROM orders;

-- ✅ DIFFERENTIAL: compute age in the source table or exclude it
SELECT id, created_at FROM orders;
-- Then compute age in the application or in a wrapper view
```

### Pattern 2: ORDER BY without LIMIT

```sql
-- ❌ Forces FULL: full sort on every refresh
SELECT customer_id, SUM(amount) AS total
FROM orders GROUP BY customer_id
ORDER BY total DESC;

-- ✅ DIFFERENTIAL: remove ORDER BY (sort in the query layer)
SELECT customer_id, SUM(amount) AS total
FROM orders GROUP BY customer_id;
-- Sort in the SELECT: SELECT * FROM my_stream ORDER BY total DESC
```

### Pattern 3: Non-equi join

```sql
-- ❌ Forces FULL: range join cannot be differentiated
SELECT e.*, s.salary_band
FROM employees e
JOIN salary_bands s ON e.salary BETWEEN s.min AND s.max;

-- ✅ DIFFERENTIAL: pre-classify salary_band in the source table
ALTER TABLE employees ADD COLUMN salary_band TEXT;
-- Update via trigger or background job
SELECT e.*, e.salary_band FROM employees e;
```

### Pattern 4: ARRAY_AGG / STRING_AGG

```sql
-- ❌ Forces FULL: order-dependent aggregate
SELECT customer_id, STRING_AGG(product, ', ') AS products
FROM order_lines GROUP BY customer_id;

-- ✅ DIFFERENTIAL: use COUNT or a separate denormalized column
SELECT customer_id, COUNT(*) AS product_count
FROM order_lines GROUP BY customer_id;
-- If you need the array: maintain it in the source table
```

### Pattern 5: Window function in output

```sql
-- ❌ Forces FULL: window function requires global ordering
SELECT customer_id, amount,
       RANK() OVER (ORDER BY amount DESC) AS rank
FROM orders;

-- ✅ Use a Top-N stream table instead
SELECT pgtrickle.create_stream_table(
    name    => 'top_orders',
    query   => 'SELECT customer_id, amount FROM orders ORDER BY amount DESC LIMIT 100',
    refresh_mode => 'DIFFERENTIAL'  -- pg_trickle supports ORDER BY LIMIT
);
```

---

## Refresh Latency Quick Diagnostics

```sql
-- See last N refresh durations for a stream table
SELECT started_at, duration_ms, mode, rows_changed
FROM pgtrickle.refresh_history('my_stream_table', limit => 20)
ORDER BY started_at DESC;

-- Check if delta is spilling to disk
SELECT query, temp_blks_written
FROM pg_stat_statements
WHERE query LIKE '%pgtrickle_changes%'
ORDER BY temp_blks_written DESC
LIMIT 10;

-- See the generated delta SQL
SELECT pgtrickle.explain_diff_sql('my_stream_table');

-- Check change buffer size
SELECT schemaname, tablename, n_live_tup
FROM pg_stat_user_tables
WHERE schemaname = 'pgtrickle_changes'
ORDER BY n_live_tup DESC
LIMIT 10;
```

---

## See Also

- [CONFIGURATION.md](CONFIGURATION.md) — Full GUC reference
- [PERFORMANCE_COOKBOOK.md](PERFORMANCE_COOKBOOK.md) — In-depth recipes
- [MENTAL_MODEL.md](MENTAL_MODEL.md) — Why DIFFERENTIAL is fast
- [LIMITATIONS.md](LIMITATIONS.md) — What forces FULL mode
