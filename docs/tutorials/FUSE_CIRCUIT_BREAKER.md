# Tutorial: Fuse Circuit Breaker

The fuse circuit breaker suspends differential refreshes when
the incoming change volume exceeds a threshold. This protects your database
from runaway refresh cycles during bulk data loads, accidental mass-deletes,
or migration scripts.

## When to Use It

- **Bulk ETL loads** — loading millions of rows that would overwhelm a
  differential refresh
- **Data migration scripts** — large schema or data changes that temporarily
  spike the change buffer
- **Protection against accidents** — an errant `DELETE FROM orders`
  shouldn't silently cascade through all downstream stream tables

## How It Works

```
Normal operation           Fuse blows               After reset
─────────────────         ─────────────────        ─────────────────
Source DML ──▶ CDC ──▶ Refresh   Source DML ──▶ CDC ──▶ BLOCKED   Source DML ──▶ CDC ──▶ Refresh
                                  │                                    (resumed)
                                  ▼
                           NOTIFY alert
                           (fuse_blown)
```

1. Each refresh cycle, the scheduler counts pending changes in the buffer.
2. If the count exceeds `fuse_ceiling` for `fuse_sensitivity` consecutive
   cycles, the fuse **blows**.
3. The stream table enters a paused state — no refreshes occur.
4. A `fuse_blown` alert is emitted via `NOTIFY pg_trickle_alert`.
5. An operator investigates and calls `reset_fuse()` to resume.

## Step-by-Step Example

### 1. Create a stream table with fuse protection

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'category_summary',
    query        => 'SELECT category, COUNT(*) AS cnt, SUM(price) AS total
                     FROM products GROUP BY category',
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);

-- Arm the fuse: blow when pending changes exceed 50,000 rows
SELECT pgtrickle.alter_stream_table(
    'category_summary',
    fuse           => 'on',
    fuse_ceiling   => 50000,
    fuse_sensitivity => 3    -- require 3 consecutive over-ceiling cycles
);
```

### 2. Observe normal operation

```sql
-- Insert a small batch — well under the ceiling
INSERT INTO products (name, category, price)
SELECT 'Product ' || i, 'Electronics', 9.99
FROM generate_series(1, 100) i;

-- After the next refresh cycle, the stream table is updated normally
SELECT * FROM pgtrickle.category_summary;
```

### 3. Trigger a bulk load

```sql
-- Simulate a large ETL load — 100,000 rows
INSERT INTO products (name, category, price)
SELECT 'Bulk ' || i, 'Imported', 4.99
FROM generate_series(1, 100000) i;
```

After `fuse_sensitivity` scheduler cycles (3 in our example), the fuse
blows. The stream table stops refreshing.

### 4. Inspect the fuse state

```sql
SELECT name, fuse_mode, fuse_state, fuse_ceiling, blown_at, blow_reason
FROM pgtrickle.fuse_status();
```

```
     name          | fuse_mode | fuse_state | fuse_ceiling |          blown_at          |       blow_reason
-------------------+-----------+------------+--------------+----------------------------+---------------------------
 category_summary  | on        | blown      |        50000 | 2026-03-31 14:22:01.123+00 | change_count_exceeded
```

### 5. Decide how to recover

You have three options:

```sql
-- Option A: Apply the changes (process the bulk load normally)
SELECT pgtrickle.reset_fuse('category_summary', action => 'apply');

-- Option B: Skip the changes (discard the batch, resume from current state)
SELECT pgtrickle.reset_fuse('category_summary', action => 'skip_changes');

-- Option C: Reinitialize (full rebuild from the defining query)
SELECT pgtrickle.reset_fuse('category_summary', action => 'reinitialize');
```

After resetting, the fuse returns to `'armed'` state and the scheduler
resumes.

## Fuse Modes

| Mode | Behavior |
|------|----------|
| `'off'` | No fuse protection (default) |
| `'on'` | Always armed — blows when changes exceed `fuse_ceiling` |
| `'auto'` | Blows only when a FULL refresh would be cheaper than DIFFERENTIAL |

`'auto'` mode is recommended for most use cases — it protects against
bulk loads while allowing large-but-efficient differential refreshes to
proceed.

## Using with dbt

In dbt models, configure the fuse via the `stream_table` materialization:

```sql
-- models/marts/category_summary.sql
{{ config(
    materialized='stream_table',
    schedule='5m',
    refresh_mode='DIFFERENTIAL',
    fuse='auto',
    fuse_ceiling=50000,
    fuse_sensitivity=3
) }}

SELECT category, COUNT(*) AS cnt, SUM(price) AS total
FROM {{ source('raw', 'products') }}
GROUP BY category
```

## Global Defaults

Set a cluster-wide default ceiling via the `pg_trickle.fuse_default_ceiling`
GUC. Stream tables with `fuse_ceiling = NULL` inherit this value:

```sql
ALTER SYSTEM SET pg_trickle.fuse_default_ceiling = 100000;
SELECT pg_reload_conf();
```

## Monitoring

- `pgtrickle.fuse_status()` — inspect fuse state for all stream tables
- `LISTEN pg_trickle_alert` — receive real-time `fuse_blown` notifications
- `pgtrickle.dedup_stats()` — includes fuse-related counters
- `pgtrickle.pgt_stream_tables.fuse_state` — direct catalog query

## Further Reading

- [SQL Reference — fuse_status()](../SQL_REFERENCE.md#pgtricklefuse_status)
- [SQL Reference — reset_fuse()](../SQL_REFERENCE.md#pgtricklereset_fuse)
- [Configuration — fuse_default_ceiling](../CONFIGURATION.md#pg_tricklefuse_default_ceiling)
- [Tutorial: ETL & Bulk Load Patterns](ETL_BULK_LOAD.md)
