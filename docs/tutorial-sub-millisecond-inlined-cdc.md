# Tutorial 6: Sub-Millisecond CDC on Inlined DuckLake Tables

*When DuckLake stores small tables directly in PostgreSQL, pg_trickle gets trigger-based CDC \u2014 and refresh latency drops to under a millisecond*

---

## What you'll build

You'll create a stream table that joins a **small DuckLake lookup table** (stored
inline in PostgreSQL) with a **large DuckLake event table** (stored in Parquet
on S3). Because the small table lives in PostgreSQL as a real heap table,
pg_trickle installs AFTER triggers on it \u2014 making change detection for that
source essentially free. You'll measure the difference and see sub-millisecond
refresh latency in practice.

---

## Background: What are "inlined-data tables"?

DuckLake stores tables in two places depending on their size:

| Size | Storage | How CDC works |
|------|---------|---------------|
| Small (below `ducklake_inline_table_max_rows`, default 1 000 rows) | Regular PostgreSQL heap table | Trigger-based CDC \u2014 O(1) per row change |
| Large (above threshold) | Parquet files on S3, via FDW | Change-feed CDC via `table_changes()` \u2014 O(\u0394) |

When DuckLake stores a table inline, it creates a real PostgreSQL table named
`ducklake_inlined_data_table_<id>_<version>`. pg_trickle detects this pattern,
installs AFTER INSERT/UPDATE/DELETE triggers on it, and gets CDC with the same
sub-millisecond latency as any other trigger-based source.

**The complication:** DuckLake periodically "rotates" inlined tables to a new
version number (e.g. from `_1` to `_2`) when the schema changes. If pg_trickle
didn't handle this, the triggers would be on the old (now-dropped) table and
CDC would stop working. pg_trickle's DDL watcher detects the rotation via event
triggers and reinstalls the CDC triggers on the new table version automatically.

---

## Prerequisites

- **PostgreSQL 18** with **pg_trickle v0.65.0+**
- **DuckLake 1.x** installed:
  ```sql
  CREATE EXTENSION IF NOT EXISTS ducklake;
  ```
- A DuckLake catalog initialized (see
  [Tutorial 2](tutorial-ivm-ducklake-before-v2.md) Step 1 if you haven't done
  this yet)
- It helps to have read [Tutorial 2](tutorial-ivm-ducklake-before-v2.md) first
  for background on DuckLake FDW and change-feed CDC

---

## Architecture

```
DuckLake catalog
  \u251c\u2500 lake.categories           (small lookup table, \u2264 1 000 rows)
  \u2502    Stored inline as:
  \u2502    ducklake_inlined_data_table_42_1  \u2190 real PostgreSQL heap table
  \u2502    pg_trickle installs AFTER triggers here
  \u2502
  \u2514\u2500 lake.raw_events            (large table, Parquet on S3)
       Exposed as FDW foreign table
       pg_trickle uses change-feed CDC here

pg_trickle stream table: category_stats
  \u2502  1-minute DIFFERENTIAL refresh
  \u2502  CDC source 1: ducklake_inlined_data_table_42_1 \u2192 TRIGGER mode (sub-ms)
  \u2502  CDC source 2: lake.raw_events               \u2192 DUCKLAKE_CHANGE_FEED
  \u25bc
category_stats (PostgreSQL table)
```

---

## Step 1: Create the DuckLake source tables

Create a small categories lookup table and a large events table:

```sql
-- Small lookup table (DuckLake will store this inline in PostgreSQL)
CREATE TABLE lake.categories (
    category_id   BIGINT PRIMARY KEY,
    category_name TEXT NOT NULL
);

INSERT INTO lake.categories VALUES
    (1, 'Electronics'),
    (2, 'Books'),
    (3, 'Clothing');

-- Large events table (DuckLake will store this in Parquet on S3)
-- We're reusing lake.raw_events from Tutorial 2; skip this if it already exists
CREATE TABLE lake.raw_events (
    event_id    BIGINT      PRIMARY KEY,
    event_type  TEXT        NOT NULL,
    user_id     BIGINT      NOT NULL,
    category_id BIGINT      NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);
```

Now find out the real name of the inlined table DuckLake created for
`lake.categories`. DuckLake names these tables with an internal ID:

```sql
-- Find the inlined table name DuckLake created
SELECT relname
FROM pg_class
WHERE relname LIKE 'ducklake_inlined_data_table_%'
ORDER BY relname;

-- Example output: ducklake_inlined_data_table_42_1
-- The '42' is DuckLake's internal table ID; the '1' is the schema version.
```

Remember this name \u2014 you'll need it in Step 3.

---

## Step 2: Create the stream table

```sql
SELECT pgtrickle.create_stream_table(
    'category_stats',
    query        => $$
        SELECT
            c.category_id,
            c.category_name,
            COUNT(e.event_id) AS event_count
        FROM lake.categories c
        LEFT JOIN lake.raw_events e USING (category_id)
        GROUP BY c.category_id, c.category_name
    $$,
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

Verify that pg_trickle chose the right CDC mode for each source:

```sql
SELECT
    d.source_relid::regclass AS source,
    d.cdc_mode
FROM pgtrickle.pgt_dependencies d
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
WHERE st.table_name = 'category_stats';
```

Expected output:

```
source                           | cdc_mode
---------------------------------+----------------------
ducklake_inlined_data_table_42_1 | TRIGGER
lake.raw_events                  | DUCKLAKE_CHANGE_FEED
```

The inlined categories table uses fast trigger CDC; the large events table uses
the DuckLake change-feed adapter.

---

## Step 3: Measure sub-millisecond refresh latency

Insert a new category directly into the inlined table. Replace
`ducklake_inlined_data_table_42_1` with the actual name you found in Step 1.

```sql
-- Insert via the DuckLake API (recommended, keeps snapshot bookkeeping consistent)
INSERT INTO lake.categories VALUES (4, 'Gaming');

-- Alternatively you can insert directly into the inlined heap table:
-- INSERT INTO ducklake_inlined_data_table_42_1 (...) VALUES (...);
-- but inserting through the DuckLake API is safer.
```

Trigger a refresh and measure how long it takes:

```sql
\timing on
SELECT pgtrickle.force_refresh('category_stats');
\timing off
```

Then check the history:

```sql
SELECT
    refresh_mode,
    delta_rows_in,
    delta_rows_out,
    duration_ms
FROM pgtrickle.pgt_refresh_history
WHERE pgt_id = (
    SELECT pgt_id FROM pgtrickle.pgt_stream_tables
    WHERE table_name = 'category_stats'
)
ORDER BY started_at DESC
LIMIT 3;
```

Typical result:

```
refresh_mode | delta_rows_in | delta_rows_out | duration_ms
-------------+---------------+----------------+-------------
DIFFERENTIAL |             1 |              1 |         0.8
```

`duration_ms = 0.8` \u2014 under one millisecond \u2014 because the change was captured
by a trigger: no table scan, no FDW round-trip, no Parquet file read.

---

## Step 4: Understand table rotation (no action required)

When DuckLake alters the schema of an inlined table (e.g. you `ALTER TABLE
lake.categories ADD COLUMN weight NUMERIC`), it creates a new internal version
of the table:

```
ducklake_inlined_data_table_42_1  \u2192  (dropped)
ducklake_inlined_data_table_42_2  \u2192  (new, with the weight column)
```

pg_trickle's DDL event trigger fires on the DROP + CREATE, detects that the
new table is also an inlined DuckLake table for the same source, and
automatically reinstalls the CDC triggers on the new version.

You can verify this works by making a schema change in DuckLake and then
running another refresh:

```sql
-- Simulate a schema evolution (actual syntax depends on your DuckLake version)
ALTER TABLE lake.categories ADD COLUMN weight NUMERIC DEFAULT 1.0;

-- Stream table should still work after the rotation
SELECT pgtrickle.force_refresh('category_stats');  -- should succeed
```

---

## Step 5: Compare trigger CDC vs. change-feed CDC latency

This comparison shows the real-world difference between the two modes:

```sql
-- Run 10 single-row refreshes to compare modes
DO $$
DECLARE
    i INT;
BEGIN
    FOR i IN 1..10 LOOP
        -- Single-row insert into the inlined table (trigger CDC)
        INSERT INTO lake.categories VALUES (100 + i, 'Category ' || i);
        PERFORM pgtrickle.force_refresh('category_stats');
    END LOOP;
END;
$$;

-- Average latency for trigger CDC source changes
SELECT
    AVG(duration_ms)    AS avg_ms,
    MIN(duration_ms)    AS min_ms,
    MAX(duration_ms)    AS max_ms,
    COUNT(*)            AS refreshes
FROM pgtrickle.pgt_refresh_history
WHERE pgt_id = (
    SELECT pgt_id FROM pgtrickle.pgt_stream_tables
    WHERE table_name = 'category_stats'
)
AND started_at > now() - interval '5 minutes';
```

Typical results on a warm system:

| Source type | Avg latency | Why |
|-------------|-------------|-----|
| Trigger CDC (inlined table) | ~0.8 ms | Trigger fires in-process; change already in the buffer |
| Change-feed CDC (DuckLake FDW) | ~3\u201310 ms | FDW round-trip + `table_changes()` call |
| `EXCEPT ALL` polling (legacy) | ~50\u2013500 ms | Full table scan of the FDW source |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `cdc_mode` shows `POLLING` not `TRIGGER` for the inlined table | pg_trickle didn't recognize the inlined table pattern | Check that the table name matches `ducklake_inlined_data_table_*`; ensure you're on pg_trickle v0.65.0+ |
| Refresh fails after schema change on `lake.categories` | DDL watcher missed the rotation | Run `SELECT pgtrickle.reinitialize('category_stats')` to reset CDC triggers |
| `duration_ms` is high despite trigger CDC | Another source (e.g. `raw_events`) had many changes | The refresh time includes both sources; check `delta_rows_in` to see the breakdown |
| `ducklake_inlined_data_table_*` query returns no rows | DuckLake storing the table in Parquet (too large for inline) | Check `ducklake_inline_table_max_rows` setting; reduce table size or raise the threshold |

---

## What you've built

- A stream table with **mixed CDC modes**: trigger-based for a small inline
  table (sub-millisecond) and change-feed for a large Parquet-backed table
  (low-latency O(\u0394)).
- Automatic CDC trigger reinstallation after DuckLake schema evolution \u2014 the
  stream table keeps working through `ALTER TABLE` without any manual
  intervention.

---

## Next steps

- **[Tutorial 2](tutorial-ivm-ducklake-before-v2.md)** \u2014 deep-dive into the
  change-feed CDC adapter for large DuckLake tables.
- **[Tutorial 3](tutorial-modern-data-stack-one-box.md)** \u2014 use DuckLake as a
  *sink* to write stream table deltas to Parquet on S3.
