# Tutorial: Migrating from Materialized Views

This guide shows how to incrementally migrate existing PostgreSQL
`MATERIALIZED VIEW` + manual `REFRESH` workflows to pg_trickle stream
tables.

## Coming from a different background?

The step-by-step guide below covers the PostgreSQL materialized view path.
If you are migrating from a different system, start here:

| You are migrating from | Jump to |
|---|---|
| PostgreSQL `MATERIALIZED VIEW` + `REFRESH` | This guide |
| `pg_ivm` | [Migrating from pg_ivm](MIGRATING_FROM_PG_IVM.md) |
| Cron-based `REFRESH` (`pg_cron`, OS cron) | [Step 6 — Remove external refresh jobs](#6-remove-external-refresh-jobs) |
| Application-level refresh (manual SQL in code) | [Step 2 — Create the stream table](#2-create-the-stream-table) |
| Debezium + Materialize / RisingWave | Port your queries to PostgreSQL SQL, then follow this guide. See [Comparisons](../COMPARISONS.md) for feature mapping. |
| Looker PDTs (Persistent Derived Tables) | PDTs map closely to stream tables. Translate the PDT SQL to a `create_stream_table()` call; the schedule replaces the PDT caching strategy. |
| Snowflake Dynamic Tables | The concepts are nearly identical. Map `TARGET_LAG` to a pg_trickle `schedule`; `DOWNSTREAM` is `schedule => 'calculated'`. See [Comparisons](../COMPARISONS.md). |
| Homemade ETL pipeline (INSERT ... SELECT) | Replace the periodic ETL job with a stream table using the same SELECT query. |

## Why Migrate?

| | Materialized View | Stream Table |
|-|-------------------|-------------|
| Refresh | Manual (`REFRESH MATERIALIZED VIEW`) | Automatic (scheduler) or manual |
| Differential refresh | Not supported | Built-in differential mode |
| Blocking reads | `REFRESH` without `CONCURRENTLY` blocks readers | Never blocks readers |
| Dependency ordering | Manual | Automatic (DAG-aware topological refresh) |
| Monitoring | None | Built-in views, stats, NOTIFY alerts |
| Scheduling | External (cron, pg_cron) | Native (duration, cron, CALCULATED) |

## Step-by-Step Migration

### 1. Identify materialized views to migrate

```sql
-- List all materialized views with their defining queries
SELECT schemaname, matviewname, definition
FROM pg_matviews
ORDER BY schemaname, matviewname;
```

### 2. Create the stream table

Take the materialized view's defining query and pass it to
`create_stream_table()`:

**Before (materialized view):**
```sql
CREATE MATERIALIZED VIEW order_totals AS
SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id;

-- Refreshed via cron or pg_cron:
-- */5 * * * * psql -c "REFRESH MATERIALIZED VIEW CONCURRENTLY order_totals"
```

**After (stream table):**
```sql
SELECT pgtrickle.create_stream_table(
    name     => 'order_totals',
    query    => 'SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count
                 FROM orders GROUP BY customer_id',
    schedule => '5m'
);
```

### 3. Update application queries

Stream tables live in the `pgtrickle` schema by default. Update your
application queries to reference the new location:

```sql
-- Before:
SELECT * FROM public.order_totals WHERE total > 1000;

-- After:
SELECT * FROM pgtrickle.order_totals WHERE total > 1000;
```

Or create a view in the original schema for backward compatibility:

```sql
CREATE VIEW public.order_totals AS
SELECT customer_id, total, order_count
FROM pgtrickle.order_totals;
```

### 4. Recreate indexes

Stream tables are regular heap tables — you can add indexes just like
any other table. Recreate the indexes your queries depend on:

```sql
-- Before (on materialized view):
CREATE UNIQUE INDEX ON order_totals (customer_id);

-- After (on stream table):
CREATE INDEX ON pgtrickle.order_totals (customer_id);
```

> **Note:** The `__pgt_row_id` column is the primary key on stream tables.
> You cannot add a separate `UNIQUE` primary key, but you can add regular
> or unique indexes on your business columns.

### 5. Remove the old materialized view

Once you've verified the stream table is working correctly:

```sql
DROP MATERIALIZED VIEW IF EXISTS public.order_totals;
```

### 6. Remove external refresh jobs

Delete any cron jobs, pg_cron entries, or application-level refresh
triggers that were maintaining the old materialized view.

## Migrating Concurrent Refresh Patterns

If you use `REFRESH MATERIALIZED VIEW CONCURRENTLY` (which requires a
unique index), the stream table equivalent is simpler — differential
refresh never blocks readers and doesn't require a unique index:

**Before:**
```sql
CREATE MATERIALIZED VIEW active_users AS
SELECT user_id, MAX(login_at) AS last_login
FROM logins
WHERE login_at > NOW() - INTERVAL '30 days'
GROUP BY user_id;

CREATE UNIQUE INDEX ON active_users (user_id);
REFRESH MATERIALIZED VIEW CONCURRENTLY active_users;
```

**After:**
```sql
SELECT pgtrickle.create_stream_table(
    name     => 'active_users',
    query    => 'SELECT user_id, MAX(login_at) AS last_login
                 FROM logins
                 WHERE login_at > NOW() - INTERVAL ''30 days''
                 GROUP BY user_id',
    schedule => '1m'
);
-- No unique index needed. No manual refresh needed.
```

## Migrating Cascading Materialized Views

If you have materialized views that depend on other materialized views,
the migration is straightforward — pg_trickle handles dependency ordering
automatically:

**Before:**
```sql
CREATE MATERIALIZED VIEW order_totals AS
SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id;

CREATE MATERIALIZED VIEW big_customers AS
SELECT customer_id, total FROM order_totals WHERE total > 1000;

-- Must refresh in order:
REFRESH MATERIALIZED VIEW order_totals;
REFRESH MATERIALIZED VIEW big_customers;
```

**After:**
```sql
SELECT pgtrickle.create_stream_table(
    name     => 'order_totals',
    query    => 'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    schedule => '1m'
);

SELECT pgtrickle.create_stream_table(
    name     => 'big_customers',
    query    => 'SELECT customer_id, total FROM pgtrickle.order_totals WHERE total > 1000',
    schedule => '1m'
);
-- Dependency ordering is automatic. No manual refresh needed.
```

## Idempotent Deployment

For CI/CD pipelines, use `create_or_replace_stream_table()` so your
migration scripts are safe to re-run:

```sql
SELECT pgtrickle.create_or_replace_stream_table(
    name         => 'order_totals',
    query        => 'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    schedule     => '5m',
    refresh_mode => 'DIFFERENTIAL'
);
```

## Choosing the Right Refresh Mode

| Scenario | Mode |
|----------|------|
| Most migrations (default) | `DIFFERENTIAL` — only processes changes |
| Volatile functions (`NOW()`, `RANDOM()`) in the query | `FULL` — the query result changes even without source DML |
| Need real-time consistency within a transaction | `IMMEDIATE` |
| Unsure | `AUTO` (default) — pg_trickle picks the best mode per cycle |

## Migration Checklist

- [ ] Identify all materialized views and their refresh schedules
- [ ] Create equivalent stream tables with matching queries
- [ ] Recreate any required indexes on the stream tables
- [ ] Update application queries to reference the `pgtrickle` schema
- [ ] Verify data correctness (compare stream table vs. materialized view)
- [ ] Remove external refresh jobs (cron, pg_cron)
- [ ] Drop the old materialized views
- [ ] Set up monitoring ([Prometheus/Grafana](../integrations/prometheus.md) or built-in views)

## Further Reading

- [Getting Started](../GETTING_STARTED.md)
- [SQL Reference — create_stream_table()](../SQL_REFERENCE.md#pgtricklecreate_stream_table)
- [SQL Reference — create_or_replace_stream_table()](../SQL_REFERENCE.md#pgtricklecreate_or_replace_stream_table)
- [FAQ — Materialized View vs Stream Table](../FAQ.md#what-is-the-difference-between-a-stream-table-and-a-regular-materialized-view-in-practice)
