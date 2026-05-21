# Demo: DuckLake Time-Travel Debugging

*Rewind a stream table to any past DuckLake snapshot and replay forward — deterministic debugging for live pipelines*

---

## What you'll build

You'll run a pg_trickle stream table that processes DuckLake change-feed events
and computes a running event summary. Then you'll deliberately introduce a data
quality issue ("bug"), observe it corrupt the stream table, rewind the stream
table's frontier to a snapshot taken before the bug arrived, and watch it
deterministically restore to the clean state.

This is a core operational technique for diagnosing data quality issues in live
stream pipelines without taking the system offline or discarding accumulated
history.

**Concepts demonstrated:**
- DuckLake change-feed as a stream table source
- Differential O(Δ) refresh over a DuckLake snapshot chain
- Stream table frontier control (pause → rewind → replay)
- Deterministic time-travel debugging

---

## Background: What is DuckLake time-travel?

Every time data is written to a DuckLake table, a new **snapshot** is created.
Each snapshot has a monotonically increasing integer ID. DuckDB can query any
past snapshot with `AT (SNAPSHOT N)`.

pg_trickle stream tables track *which snapshot they last processed* as the
**frontier**. The frontier is stored in `pgtrickle.pgt_stream_tables.frontier`
as a JSON object like:
```json
{"sources": {"ducklake:lake.events": {"snapshot_id": 42}}}
```

To rewind a stream table to a past state:
1. **Pause** the scheduler so no more refreshes happen.
2. **Update the frontier** to point to a past snapshot ID.
3. **Force a FULL refresh** — pg_trickle discards the current result and
   rebuilds from the rewound snapshot.
4. **Resume** the scheduler — it will replay forward from the rewound snapshot,
   processing only the deltas since that point.

The result is a completely deterministic, reproducible rewind: the stream table
at snapshot N will always have the same rows, no matter when you perform the
operation.

---

## Prerequisites

- **Docker Engine 24+** and **Docker Compose v2**
  Verify: `docker compose version`
- **`psql`** installed locally (or use `docker compose exec postgres psql ...`)
- No pg_trickle installation needed — it runs inside the Docker container

---

## Architecture

```
Event generator (Python)
  │  Batches of 50 events every 2 seconds
  │  event_type: 'click' | 'view' | 'purchase'
  ▼
PostgreSQL 18 + pg_trickle  (port 5432, db: timetravel_demo)
  │  DuckLake FDW foreign table: lake.events
  │
  │  pg_trickle monitors the DuckLake snapshot chain
  │  and processes each new snapshot as a delta:
  │     table_changes(from_snapshot, to_snapshot)
  │
  └─ stream table: public.event_summary
       query: SELECT event_type, COUNT(*), SUM(value)
              FROM lake.events GROUP BY event_type
       5-second DIFFERENTIAL refresh
       frontier: { sources: { "ducklake:lake.events": { snapshot_id: N } } }

                                ┌────────────────────────────────────────┐
  Time-travel debugging flow:   │  1. Record good_snapshot               │
  ─────────────────────────────→│  2. Generator inserts "bad" events     │
                                │  3. Pause scheduler                    │
                                │  4. SET frontier to good_snapshot      │
                                │  5. FULL refresh → rewind              │
                                │  6. Resume → replay forward            │
                                └────────────────────────────────────────┘
```

---

## Step 1: Start the demo

```bash
cd demos/ducklake-time-travel
docker compose up
```

Two containers start:

| Container | Port | Purpose |
|-----------|------|---------|
| `ducklake_timetravel_postgres` | 5432 | PostgreSQL 18 + pg_trickle |
| `ducklake_timetravel_generator` | — | Python script inserting 50 events every 2 s |

Wait until the stream table is populated (≈ 10 seconds):
```
ducklake_timetravel_postgres  | LOG: database system is ready to accept connections
```

---

## Step 2: Verify the stream table is running

Connect to PostgreSQL:

```bash
psql postgresql://postgres:postgres@localhost:5432/timetravel_demo
```

Check the stream table:

```sql
SELECT table_name, status, frontier, last_refreshed_at
FROM pgtrickle.pgt_stream_tables
WHERE table_name = 'event_summary';
```

Check the current event summary:

```sql
SELECT event_type, event_count, total_value
FROM event_summary
ORDER BY event_type;
```

You should see rows like:
```
 event_type | event_count | total_value
------------+-------------+-------------
 click      |         312 |       15600
 purchase   |          41 |       20500
 view       |         198 |        9900
```

---

## Step 3: Record a "good" snapshot

Before introducing the bug, save the current snapshot ID:

```sql
SELECT
    (frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id')::bigint
        AS good_snapshot,
    (SELECT COUNT(*) FROM event_summary) AS current_row_count
FROM pgtrickle.pgt_stream_tables
WHERE table_name = 'event_summary';
```

Note down the `good_snapshot` value. We'll call it `42` in the examples below.

---

## Step 4: Introduce a "bug"

A common real-world scenario: a misconfigured batch job inserts events with the
wrong event type, inflating the counts. Simulate this:

```sql
-- Simulate a bad batch job inserting events with a bogus type
INSERT INTO lake.events (event_id, event_type, value, created_at)
SELECT
    gen_random_uuid(),
    'CORRUPTED_EVENT',       -- this type should not exist
    999999,                  -- implausible value
    now()
FROM generate_series(1, 1000);
```

Wait one refresh cycle (≈ 5–10 seconds), then check the damage:

```sql
SELECT event_type, event_count, total_value
FROM event_summary
ORDER BY event_type;
```

You'll see a new row:
```
 event_type       | event_count | total_value
------------------+-------------+---------------
 CORRUPTED_EVENT  |        1000 |   999999000
 click            |         318 |       15900
 ...
```

Note the new `good_snapshot` would be what we had before — the snapshot
just before the `INSERT` was committed.

---

## Step 5: Pause the scheduler and rewind

First, pause all refreshes so no more deltas are applied:

```sql
SELECT pgtrickle.pause_scheduler(ARRAY['event_summary']);
```

Verify the stream table is paused:

```sql
SELECT table_name, status
FROM pgtrickle.pgt_stream_tables
WHERE table_name = 'event_summary';
-- status should be 'PAUSED'
```

Now rewind the frontier to the good snapshot (replace `42` with your actual
`good_snapshot` value from Step 3):

```sql
UPDATE pgtrickle.pgt_stream_tables
SET frontier = jsonb_set(
    frontier,
    '{sources,ducklake:lake.events,snapshot_id}',
    '42'::jsonb    -- ← replace with your good_snapshot value
)
WHERE table_name = 'event_summary';
```

Force a FULL refresh from the rewound frontier:

```sql
SELECT pgtrickle.refresh('event_summary', 'FULL');
```

---

## Step 6: Verify the rewind

```sql
SELECT event_type, event_count, total_value
FROM event_summary
ORDER BY event_type;
```

The `CORRUPTED_EVENT` row should be gone and the counts should match what they
were at snapshot `good_snapshot`. The rewind is deterministic: if you repeat
it, you'll get identical results every time.

Confirm the frontier:

```sql
SELECT
    (frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id')::bigint
        AS current_snapshot
FROM pgtrickle.pgt_stream_tables
WHERE table_name = 'event_summary';
-- Should show 42 (your good_snapshot value)
```

---

## Step 7: Resume from the rewound point

```sql
SELECT pgtrickle.resume_scheduler(ARRAY['event_summary']);
```

The scheduler will now replay forward from snapshot `42`. It will process each
snapshot between `42` and the current snapshot in order, applying the deltas
differentially. The corrupted events (inserted after snapshot 42) will re-appear
unless you also delete them from `lake.events` — in a real incident, you'd fix
the source data first, then resume.

To remove the corrupted data before resuming:

```sql
DELETE FROM lake.events
WHERE event_type = 'CORRUPTED_EVENT';

-- Now resume — the corrupted events won't be replayed
SELECT pgtrickle.resume_scheduler(ARRAY['event_summary']);
```

After resuming, the stream table catches up to the current snapshot with the
corrected data.

---

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `BATCH_SIZE` | `50` | Events inserted per generator tick |
| `INTERVAL_MS` | `2000` | Milliseconds between generator ticks (2 s = ~25 events/s) |
| `POSTGRES_PORT` | `5432` | Host port for PostgreSQL |

Change these in a `.env` file:

```bash
echo "BATCH_SIZE=100" > .env
echo "INTERVAL_MS=1000" >> .env
docker compose up
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `event_summary` is empty after 30 s | Generator not running or init failed | Check `docker compose logs generator` and `docker compose logs postgres` |
| `pause_scheduler` has no effect | Stream table name typo | Verify with `SELECT table_name FROM pgtrickle.pgt_stream_tables` |
| After rewind, row count is higher than expected | Snapshot ID was after the bug | Repeat with a lower `good_snapshot` value |
| `FULL` refresh takes a long time | Large event table | Normal — FULL refresh scans all data from the frontier snapshot |
| Stream table shows status `ERROR` | Underlying DuckLake FDW unreachable | Check `docker compose logs postgres` for FDW errors |

---

## What you've built

- A **time-travel debugging workflow** for stream tables: pause → rewind
  frontier → FULL refresh → resume → replay.
- Direct experience of DuckLake **snapshot determinism**: rewinding to snapshot N
  always produces the same result, regardless of when you do it.
- A practical mental model for dealing with **data quality incidents** in live
  pipelines: you can always roll back to a known-good state, fix the source data,
  and replay forward cleanly.

---

## Stop and clean up

```bash
docker compose down -v
```

---

## Related resources

- [Tutorial 2: IVM on DuckLake Before v2](../../docs/tutorial-ivm-ducklake-before-v2.md)
- [Tutorial 4: Streaming PostgreSQL to a Data Lake](../../docs/tutorial-streaming-postgres-to-data-lake.md)
- [Blog: DuckLake Table Changes and the DVM Engine](../../blog/ducklake-table-changes-dvm.md)


> **Roll back a stream table to any past snapshot and watch it rewind deterministically.**

---

## What This Demo Shows

This demo starts a pg_trickle stream table over a DuckLake change-feed source. A synthetic
event generator continuously inserts data into DuckLake. The stream table refreshes every few
seconds via the O(Δ) change-feed adapter.

At any point you can:

1. **Pause** the scheduler.
2. **Rewind** `last_consumed_snapshot_id` to a past snapshot.
3. **Replay** from that snapshot — the stream table rewinds deterministically to its past state.

This is a powerful debugging tool: if you notice a data quality issue in a stream table,
you can rewind to the snapshot just before the problem appeared and inspect the data.

---

## Quick Start

```bash
docker compose up -d
```

Wait for the stream table to be populated (≈ 10 seconds), then open a `psql` session:

```bash
docker compose exec postgres psql -U postgres -d timetravel_demo
```

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│ PostgreSQL (pg_trickle v0.65.0+)                    │
│                                                       │
│  DuckLake FDW foreign table: lake.events              │
│      ↓ table_changes(from_snap, to_snap)              │
│  Change buffer: pgtrickle_changes.changes_<oid>       │
│      ↓ O(Δ) differential refresh                     │
│  Stream table: public.event_summary                   │
│      ↓ snapshot frontier: { snapshot_id: N }          │
│  Queryable at any past snapshot                       │
└──────────────────────────────────────────────────────┘
         ↑
         │ INSERT batches every 2 s
┌────────────────────┐
│ Event generator    │
└────────────────────┘
```

---

## Time-Travel Walkthrough

### 1. Check the Current Snapshot

```sql
SELECT
    frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id' AS current_snapshot,
    (SELECT COUNT(*) FROM event_summary) AS current_row_count
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'event_summary';
```

### 2. Record a "Good" Snapshot ID

```sql
-- Save the current snapshot ID before introducing a "bug"
SELECT (frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id')::bigint AS good_snapshot
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'event_summary';
-- Example: good_snapshot = 42
```

### 3. Wait for More Data (Simulate Bug Window)

Wait 10–20 seconds for more data to arrive, then:

```sql
-- Note the new (bugged) state
SELECT COUNT(*) AS bugged_row_count FROM event_summary;
```

### 4. Pause the Scheduler and Rewind

```sql
-- Pause all refreshes
SELECT pgtrickle.pause_scheduler(ARRAY['event_summary']);

-- Rewind the frontier snapshot_id to the "good" snapshot
UPDATE pgtrickle.pgt_stream_tables
SET frontier = jsonb_set(
    frontier,
    '{sources,ducklake:lake.events,snapshot_id}',
    '42'::jsonb  -- replace 42 with your good_snapshot value
)
WHERE pgt_name = 'event_summary';

-- Trigger a FULL refresh to rebuild from the rewound frontier
SELECT pgtrickle.refresh('event_summary', 'FULL');
```

### 5. Verify the Rewind

```sql
-- The stream table is now back at the state it had at snapshot 42
SELECT COUNT(*) AS rewound_row_count FROM event_summary;
SELECT (frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id')::bigint AS snapshot
FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'event_summary';
```

### 6. Resume from the Rewound Point

```sql
SELECT pgtrickle.resume_scheduler(ARRAY['event_summary']);
-- The stream table will now replay forwards from snapshot 42.
```

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `BATCH_SIZE` | `50` | Rows inserted per generator tick |
| `INTERVAL_MS` | `2000` | Milliseconds between generator ticks |
| `POSTGRES_PORT` | `5432` | Host port for PostgreSQL |

---

## Stopping the Demo

```bash
docker compose down -v
```
