# Tutorial 2: O(Δ) Refresh on DuckLake Tables

*How pg_trickle's change-feed adapter reads only the rows that changed \u2014 even when the source is a DuckLake foreign table with 10 million rows*

---

## What you'll build

You'll create a pg_trickle stream table whose **source is a DuckLake table**
(not a regular PostgreSQL table). You'll see that pg_trickle uses DuckLake's
`table_changes()` API to read only the rows that changed since the last
refresh cycle \u2014 an O(\u0394) scan instead of an O(N) full-table scan.

---

## Background: What is DuckLake? What is an FDW?

**DuckLake** is a data lake catalog that stores metadata in PostgreSQL while
keeping actual data in Parquet files on S3. DuckLake 1.x ships a PostgreSQL
Foreign Data Wrapper (FDW) so you can query DuckLake tables using ordinary SQL
from inside PostgreSQL.

**FDW (Foreign Data Wrapper)** is a PostgreSQL mechanism that lets you access
external data sources \u2014 including DuckLake tables, files on S3, or remote
databases \u2014 as if they were regular PostgreSQL tables. They appear in
`pg_class` with `relkind = 'f'` (foreign table).

**IVM (Incremental View Maintenance)** means maintaining a precomputed result
set by applying only the *changes* (\u0394) that occurred since the last refresh,
rather than recomputing from scratch every time.

**The problem pg_trickle solves here:** before v0.65.0, the only way to detect
changes on a DuckLake FDW table was to scan the entire table and compare with a
snapshot (`EXCEPT ALL`). That is O(N) \u2014 it gets slower as the table grows,
regardless of how few rows actually changed. DuckLake 1.x provides
`table_changes(table, from_snapshot, to_snapshot)` which returns only the delta
rows. pg_trickle v0.65.0+ uses this API automatically.

| Approach | Work per refresh | When to use |
|----------|-----------------|-------------|
| `EXCEPT ALL` polling (\u2264 v0.64.0) | O(N) \u2014 full table scan | DuckLake without `table_changes` |
| Change-feed adapter (v0.65.0+) | O(\u0394) \u2014 only changed rows | DuckLake 1.x with `table_changes` |

---

## Prerequisites

Before starting you need:

- **PostgreSQL 18** with **pg_trickle v0.65.0+** installed and in
  `shared_preload_libraries`
- **DuckLake 1.x** installed in your PostgreSQL instance:
  ```sql
  CREATE EXTENSION IF NOT EXISTS ducklake;
  ```
- A DuckLake catalog initialized \u2014 see Step 1 below

---

## Architecture

```
DuckLake catalog (PostgreSQL)
  \u2502  raw_events table  \u2190 batch loads land here
  \u2502  (exposed as FDW foreign table in pg_class)
  \u2502
  \u2514\u2500 pg_trickle stream table: event_summary
       \u2502  5-minute DIFFERENTIAL refresh
       \u2502  CDC mode: DUCKLAKE_CHANGE_FEED
       \u2502  pg_trickle calls table_changes(raw_events, prev_snapshot, latest)
       \u2502  and processes only the \u0394 rows
       \u25bc
  event_summary (PostgreSQL table)
       \u2514\u2500 SELECT * FROM event_summary  \u2190 always fresh, always fast
```

---

## Step 1: Initialize a DuckLake catalog and create a source table

First, initialize a DuckLake catalog. This creates the catalog metadata tables
in your PostgreSQL database and registers the FDW:

```sql
-- Initialize the DuckLake catalog
-- The path is where DuckLake will store its internal metadata database
SELECT ducklake_init('/var/lib/ducklake/events.db');
```

Now create a DuckLake table. This table will be the *source* for our stream
table \u2014 the data that pg_trickle watches for changes:

```sql
-- Create a DuckLake table (this executes DuckLake DDL via the FDW)
CREATE TABLE lake.raw_events (
    event_id    BIGINT      PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    user_id     BIGINT      NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload     JSONB
);
```

DuckLake automatically exposes this as a foreign table in PostgreSQL. Verify
it's visible:

```sql
-- relkind = 'f' means foreign table (not a regular heap table)
SELECT relname, relkind
FROM pg_class
WHERE relname = 'raw_events' AND relkind = 'f';
```

Seed it with some test data:

```sql
INSERT INTO lake.raw_events
SELECT
    i                                           AS event_id,
    (ARRAY['click','view','purchase'])[1 + (i % 3)] AS event_type,
    (i % 1000)                                 AS user_id,
    now() - (i * interval '1 second')          AS occurred_at,
    '{}'::jsonb                                AS payload
FROM generate_series(1, 10000000) AS i;  -- 10 million rows
```

---

## Step 2: Create a stream table over the DuckLake source

pg_trickle inspects the source table at `create_stream_table` time and
automatically detects that `lake.raw_events` is a DuckLake FDW table. It
selects the `DUCKLAKE_CHANGE_FEED` CDC mode without any extra configuration
from you.

```sql
SELECT pgtrickle.create_stream_table(
    'event_summary',
    query        => $$
        SELECT
            date_trunc('hour', occurred_at) AS hour,
            event_type,
            COUNT(*)                        AS event_count,
            COUNT(DISTINCT user_id)         AS unique_users
        FROM lake.raw_events
        GROUP BY 1, 2
    $$,
    schedule     => '5m',
    refresh_mode => 'DIFFERENTIAL'
);
```

Confirm that pg_trickle chose the change-feed CDC mode automatically:

```sql
SELECT
    d.source_relid::regclass AS source,
    d.cdc_mode
FROM pgtrickle.pgt_dependencies d
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
WHERE st.table_name = 'event_summary';

-- Expected output:
-- source          | cdc_mode
-- ----------------+----------------------
-- lake.raw_events | DUCKLAKE_CHANGE_FEED
```

If you see `POLLING` instead of `DUCKLAKE_CHANGE_FEED`, your DuckLake version
does not have the `table_changes()` function (pre-1.x). The tutorial still
works but refreshes will be O(N) instead of O(\u0394).

---

## Step 3: Observe O(\u0394) refresh behaviour

Now insert just 100 new rows into the 10-million-row table:

```sql
INSERT INTO lake.raw_events
SELECT
    10000000 + i                                    AS event_id,
    (ARRAY['click','view','purchase'])[1 + (i % 3)] AS event_type,
    (i % 1000)                                      AS user_id,
    now()                                           AS occurred_at,
    '{}'::jsonb                                     AS payload
FROM generate_series(1, 100) AS i;
```

Trigger a manual refresh and check how many rows were processed:

```sql
SELECT pgtrickle.force_refresh('event_summary');

SELECT
    refresh_mode,
    delta_rows_in,
    delta_rows_out,
    duration_ms
FROM pgtrickle.pgt_refresh_history
WHERE pgt_id = (
    SELECT pgt_id FROM pgtrickle.pgt_stream_tables
    WHERE table_name = 'event_summary'
)
ORDER BY started_at DESC
LIMIT 1;
```

Expected output:

```
refresh_mode | delta_rows_in | delta_rows_out | duration_ms
-------------+---------------+----------------+-------------
DIFFERENTIAL |           100 |             68 |           3
```

`delta_rows_in = 100` means pg_trickle read exactly the 100 new rows via
`table_changes()` \u2014 not the 10 million rows in the full table. The
`delta_rows_out = 68` is the number of rows in `event_summary` that changed
as a result (some `hour` + `event_type` combinations received their first row,
others updated an existing aggregate).

---

## Step 4: Understand and configure the compaction policy

DuckLake periodically **compacts** old Parquet files \u2014 merging many small
delta files into one large file and deleting the originals. This is a routine
maintenance operation that improves query performance.

The problem: pg_trickle tracks which DuckLake snapshot it last processed using
a **snapshot frontier** (a snapshot ID stored in the stream table's metadata).
If DuckLake compacts away a snapshot that pg_trickle's frontier references, the
change-feed call `table_changes(raw_events, old_snapshot, latest)` can no
longer find the starting point.

You have two options for how pg_trickle handles this:

```sql
-- Option A (default): fall back to a full DIFFERENTIAL refresh automatically.
-- The stream table continues working; you just pay for one extra full scan.
ALTER SYSTEM SET pg_trickle.ducklake_compaction_policy = 'fallback';
SELECT pg_reload_conf();

-- Option B: raise an error and stop refreshing until you reinitialize manually.
-- Use this when a silent full re-scan is unacceptable (e.g., SLA-critical tables).
SELECT pgtrickle.alter_stream_table(
    'event_summary',
    ducklake_compaction_policy => 'error'
);
```

For most use cases, `'fallback'` (the default) is the right choice.

---

## Step 5: Inspect the snapshot frontier

pg_trickle stores the last-processed DuckLake snapshot ID in the stream table
metadata. You can see it here:

```sql
SELECT
    table_name,
    frontier
FROM pgtrickle.pgt_stream_tables
WHERE table_name = 'event_summary';
```

The `frontier` column is a JSON object. The `snapshot_id` field inside it
advances by 1 after each successful refresh. On the next refresh pg_trickle
will call `table_changes(lake.raw_events, <snapshot_id>, <latest>)` to fetch
only the rows that appeared after that snapshot.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `cdc_mode` is `POLLING` not `DUCKLAKE_CHANGE_FEED` | DuckLake version < 1.0 | Upgrade DuckLake or accept O(N) polling |
| `delta_rows_in` equals the full table size | Compaction cleared the frontier | The `'fallback'` policy triggered a full scan; this is expected after compaction |
| `pgt_refresh_history` shows `ERROR` | Compaction policy is `'error'` and frontier was lost | Run `SELECT pgtrickle.reinitialize('event_summary')` to reset the frontier |
| `CREATE TABLE lake.raw_events` fails | DuckLake FDW not installed | Verify `CREATE EXTENSION ducklake` ran successfully |

---

## What you've built

- A stream table that aggregates a 10-million-row DuckLake FDW table into
  an hourly event summary \u2014 refreshing in milliseconds by reading only the
  rows that changed.
- Automatic compaction resilience: if DuckLake compacts away old snapshots,
  pg_trickle falls back to a full scan and then resumes O(\u0394) operation.

---

## Next steps

- **[Tutorial 3](tutorial-modern-data-stack-one-box.md)** \u2014 use DuckLake as a
  *sink* (write stream table deltas out to Parquet on S3) in a full docker
  compose stack.
- **[Tutorial 6](tutorial-sub-millisecond-inlined-cdc.md)** \u2014 sub-millisecond
  CDC on small DuckLake tables that are stored inline in PostgreSQL.
