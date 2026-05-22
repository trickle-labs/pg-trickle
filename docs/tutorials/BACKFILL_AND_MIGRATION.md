# Tutorial: Zero-Downtime Migration from Materialized Views

> DOC-NEW-26 — Step-by-step guide: migrate a manually-maintained
> materialized view to a stream table with zero downtime.

## Overview

This tutorial walks through migrating an existing `REFRESH MATERIALIZED VIEW`
workflow to a pg_trickle stream table without any downtime or data loss.
The process runs the old view and the new stream table in parallel so you can
verify correctness before cutting over consumers.

---

## Prerequisites

- PostgreSQL 18 with pg_trickle installed (see [Installation](../installation.md))
- An existing `MATERIALIZED VIEW` with a known refresh schedule
- At least `SELECT` access to the materialized view

---

## Step 1 — Pre-Migration Assessment

Before migrating, assess whether the defining query is IVM-eligible.

```sql
-- Check the current materialized view definition
SELECT schemaname, matviewname, definition
FROM pg_matviews
WHERE matviewname = 'my_view';

-- Validate the query against pg_trickle's IVM compatibility checker
SELECT pgtrickle.validate_query(
    $$<paste your view definition here>$$
);
```

**Example output:**

```
 result  | detail
---------+--------------------------------------------------------------
 ok      | Query is IVM-eligible. Recommended mode: DIFFERENTIAL
```

If `validate_query` returns `ok`, the migration is straightforward.
If it returns warnings or `not_eligible`, check the detail column —
common reasons include volatile functions or unsupported SQL patterns.
See [LIMITATIONS.md](../LIMITATIONS.md) for the full list.

**For non-eligible queries**, use `refresh_mode => 'FULL'` — pg_trickle
will maintain a full-refresh stream table automatically, eliminating the
manual `REFRESH MATERIALIZED VIEW` calls.

---

## Step 2 — Create the Stream Table in Parallel

Do **not** drop the old materialized view yet. Create the stream table
alongside it, pointing at the same source tables.

```sql
-- Example: migrating this materialized view
-- CREATE MATERIALIZED VIEW orders_summary AS
--     SELECT region, COUNT(*) AS order_count, SUM(amount) AS total
--     FROM orders GROUP BY region;

-- Create the equivalent stream table
SELECT pgtrickle.create_stream_table(
    name         => 'orders_summary_st',
    query        => $$
        SELECT region,
               COUNT(*)   AS order_count,
               SUM(amount) AS total
        FROM orders
        GROUP BY region
    $$,
    schedule     => '10s',
    refresh_mode => 'DIFFERENTIAL'
);
```

The stream table will populate within one refresh cycle. Check status:

```sql
SELECT pgt_name, status, last_refresh_at, rows_in_last_refresh
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'orders_summary_st';
```

---

## Step 3 — Verify Output Parity

Compare the outputs of both the old view and the new stream table:

```sql
-- Rows in old view but not in stream table
SELECT * FROM orders_summary
EXCEPT
SELECT * FROM orders_summary_st;

-- Rows in stream table but not in old view
SELECT * FROM orders_summary_st
EXCEPT
SELECT * FROM orders_summary;
```

Both queries should return zero rows. If there are differences:

1. Check that the stream table has had at least one full refresh cycle.
2. Verify the defining query is identical (column order and aliases matter).
3. Run `SELECT pgtrickle.reinitialize_stream_table('orders_summary_st')` to
   force a clean full refresh if there is any doubt.

For long-running parallel validation, write DML to the source table and
verify both targets update correctly:

```sql
INSERT INTO orders (region, amount) VALUES ('TEST', 99.99);

-- Both should show the TEST region
SELECT * FROM orders_summary     WHERE region = 'TEST';
SELECT * FROM orders_summary_st  WHERE region = 'TEST';
```

---

## Step 4 — Create a Compatibility View (Optional)

If consumers reference `orders_summary` by name and you cannot update them
before cutover, create a compatibility view that points to the stream table:

```sql
-- 1. Rename the old materialized view
ALTER MATERIALIZED VIEW orders_summary RENAME TO orders_summary_old;

-- 2. Create a regular view with the original name, reading from the ST
CREATE VIEW orders_summary AS SELECT * FROM orders_summary_st;
```

Consumers now read from the stream table transparently. The old materialized
view remains as a fallback.

---

## Step 5 — Consumer Cutover

Once parallel validation passes, cut over consumers:

**Option A — Direct table reference (recommended):**
Update consumer queries/code to reference `orders_summary_st` directly.
This is the cleanest path and exposes pg_trickle's automatic freshness.

**Option B — Keep the compatibility view:**
If you created the compatibility view in Step 4, consumers already read
from the stream table. No further changes needed.

---

## Step 6 — Remove the Old Materialized View

After confirming all consumers use the stream table:

```sql
-- Remove the old materialized view
DROP MATERIALIZED VIEW orders_summary_old;

-- If you kept the compatibility view, optionally rename the stream table
-- to match the original name:
--   SELECT pgtrickle.drop_stream_table('orders_summary_st');
--   SELECT pgtrickle.create_stream_table('orders_summary', ...)
-- Or rename the view to align naming conventions.
```

Also remove any cron jobs or application code that called
`REFRESH MATERIALIZED VIEW orders_summary`.

---

## Step 7 — Rollback Procedure

If problems arise after cutover:

```sql
-- 1. Stop the stream table from refreshing
SELECT pgtrickle.pause_stream_table('orders_summary_st');

-- 2. Revert consumers to the old materialized view
--    (if you renamed it in Step 4)
ALTER MATERIALIZED VIEW orders_summary_old RENAME TO orders_summary;

-- 3. Drop the compatibility view if you created one
DROP VIEW IF EXISTS orders_summary;

-- 4. Resume your manual refresh schedule
-- (add back the cron job / pg_cron entry for REFRESH MATERIALIZED VIEW)

-- 5. Optionally drop the stream table
SELECT pgtrickle.drop_stream_table('orders_summary_st');
```

---

## Common Migration Patterns

### Non-IVM-eligible queries (use FULL mode)

```sql
-- Query uses a volatile function; pg_trickle will use FULL refresh
SELECT pgtrickle.create_stream_table(
    name         => 'hourly_snapshot',
    query        => $$
        SELECT *, now() AS snapshot_at FROM large_table
    $$,
    schedule     => '1h',
    refresh_mode => 'FULL'
);
```

### Concurrently-refreshed materialized views

If the old view used `REFRESH MATERIALIZED VIEW CONCURRENTLY`, note that
pg_trickle's MERGE-based update is also non-blocking for readers. No special
configuration is needed.

### Views with `WITH DATA` at creation

pg_trickle always populates the stream table on the first cycle, equivalent
to `WITH DATA`. The `WITHOUT DATA` option does not apply.

---

## Post-Migration Checklist

- [ ] `pgtrickle.validate_query()` returned `ok` or migration is in FULL mode
- [ ] Stream table reached `status = 'ACTIVE'`
- [ ] `EXCEPT` diff queries return zero rows
- [ ] Manual `REFRESH MATERIALIZED VIEW` calls removed from cron/pg_cron
- [ ] Old materialized view dropped (or retained as read-only archive)
- [ ] Consumer queries point to the stream table

---

## Next Steps

- Tune the refresh interval and mode — see [tuning-refresh-mode.md](tuning-refresh-mode.md)
- Add monitoring and alerts — see [MONITORING_AND_ALERTING.md](MONITORING_AND_ALERTING.md)
- Performance optimisation — see [PERFORMANCE_COOKBOOK.md](../PERFORMANCE_COOKBOOK.md)
