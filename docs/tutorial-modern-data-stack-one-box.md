# Tutorial 3: The Modern Data Stack in One Box

*PostgreSQL + pg_trickle + DuckLake + DuckDB — no Kafka, no Airflow, no separate stream processor*

---

## What you'll build

You'll spin up a complete analytics pipeline — from OLTP writes to queryable
Parquet files on object storage — using a single `docker compose up` command.
No Kafka. No Airflow. No infrastructure team required.

By the end you will have:

- A PostgreSQL `orders` table accepting writes at full OLTP speed.
- A stream table that recomputes `revenue_by_region` every 5 seconds using
  only the rows that changed (differential refresh).
- Each delta automatically written to MinIO as a Parquet file via the DuckLake
  sink.
- A DuckDB session that queries the historical Parquet snapshots for trend
  analysis — including time-travel back to any previous snapshot.

**Total setup time: under 10 minutes.**

---

## Overview

This tutorial uses a pre-built `docker compose` environment that wires
everything together for you. All S3 credentials, bucket setup, and PostgreSQL
configuration are handled automatically by the containers — you just run the
SQL.

This tutorial walks you through running a complete modern data stack in a single
`docker compose up` environment:

| Layer | Technology | Role |
|-------|-----------|------|
| OLTP | PostgreSQL 18 + pg_trickle | Write orders; compute aggregations incrementally |
| Object storage | MinIO (S3-compatible) | Parquet files for the data lake |
| Lake catalog | DuckLake | Manages the Parquet files and snapshots |
| Analytics | DuckDB | Ad-hoc queries over the historical Parquet data |

The result: a stream table computes `revenue_by_region` on every 5-second
refresh cycle. Each delta is written to DuckLake as a new Parquet snapshot on
MinIO. DuckDB queries the historical snapshots for trend analysis.

**Total setup time: under 10 minutes.**

---

## Prerequisites

You need the following installed on your machine:

- **Docker Engine 24+** and **Docker Compose v2**
  Verify: `docker compose version` — should print `Docker Compose version v2.x`
- **4 GB free RAM** for the containers
- **DuckDB CLI or Python** (optional — only needed to run the analytics queries in Step 4)
  Install: `pip install duckdb` or download from [duckdb.org](https://duckdb.org)

You do **not** need pg_trickle installed locally — it runs inside the Docker
container.

---

## Architecture

```
PostgreSQL 18
  │  orders table  ← INSERT new orders here
  │
  └─ stream table: revenue_by_region
       │  5-second DIFFERENTIAL refresh
       │  DuckLake sink (append mode)
       ▼
  MinIO (S3-compatible)
       │  Parquet delta files
       ▼
  DuckLake catalog (PostgreSQL)
       │  ducklake_snapshot records each delta
       │  ducklake_view exposes revenue_by_region as a native DuckLake object
       ▼
  DuckDB
       └─ SELECT * FROM ducklake.revenue_by_region
          (reads latest Parquet snapshots from MinIO)
```

---

## Step 0: Start the environment

Clone the repo (if you haven't already) and start the docker compose stack:

```bash
cd demos/ducklake-oltp-lake
docker compose up
```

This starts four containers:

| Container | Purpose |
|-----------|--------|
| `postgres` | PostgreSQL 18 + pg_trickle, listening on `localhost:5432` |
| `minio` | S3-compatible object storage, console at http://localhost:9001 |
| `minio-setup` | One-shot container that creates the `pg-trickle-demo` bucket |
| `jupyter` | JupyterLab with DuckDB, at http://localhost:8888 (token: `demo`) |

Wait until you see:
```
postgres  | LOG:  database system is ready to accept connections
```

Then open a second terminal and connect to PostgreSQL:

```bash
psql postgresql://postgres:postgres@localhost:5432/postgres
```

> **MinIO credentials are pre-configured.** The `postgres` container's
> `postgresql.conf` already contains:
> ```
> pg_trickle.ducklake_sink_s3_endpoint   = 'http://minio:9000'
> pg_trickle.ducklake_sink_s3_access_key = 'minioadmin'
> pg_trickle.ducklake_sink_s3_secret_key = 'minioadmin'
> pg_trickle.ducklake_sink_s3_region     = 'us-east-1'
> ```
> You do not need to run any `ALTER SYSTEM` commands for this tutorial.

---

## Step 1: Create the source table and stream table

Connect to PostgreSQL and run:

```sql
-- Source table
CREATE TABLE orders (
    order_id   BIGSERIAL PRIMARY KEY,
    region     TEXT NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stream table with DuckLake sink
SELECT pgtrickle.create_stream_table(
    'revenue_by_region',
    query => $$
        SELECT region,
               SUM(amount) AS total_revenue,
               COUNT(*)    AS order_count
        FROM orders
        GROUP BY region
    $$,
    schedule             => '5s',
    refresh_mode         => 'DIFFERENTIAL',
    sink                 => 'ducklake',
    ducklake_sink_path   => 's3://pg-trickle-demo/revenue_by_region/'
);
```

After this call, pg_trickle:

1. Creates the `revenue_by_region` storage table inside PostgreSQL.
2. Installs CDC triggers on `orders` to track every INSERT/UPDATE/DELETE.
3. Registers a `ducklake_view` entry so DuckDB sees `revenue_by_region` as a
   native catalog object.
4. Schedules the first refresh for 5 seconds from now.

Verify it was created:

```sql
SELECT table_name, refresh_mode, schedule, sink, ducklake_sink_path
FROM pgtrickle.pgt_stream_tables
WHERE table_name = 'revenue_by_region';
```

---

## Step 2: Insert some orders

```sql
INSERT INTO orders (region, amount) VALUES
    ('us-east', 149.99),
    ('us-west', 89.50),
    ('eu-west', 220.00),
    ('ap-south', 55.75);
```

Within 5 seconds pg_trickle's scheduler fires, detects the new rows via CDC,
runs the `GROUP BY` query differentially (only over the 4 new rows, not a full
table scan), and writes a Parquet delta to the `pg-trickle-demo` bucket on MinIO.

Verify the delta was written:

```sql
SELECT stream_table_name, delta_row_count, written_at
FROM pgtrickle.pgt_ducklake_provenance
ORDER BY written_at DESC
LIMIT 5;
```

You should see one row with `delta_row_count = 4`.

You can also browse the Parquet files directly in the MinIO console:
http://localhost:9001 (login: `minioadmin` / `minioadmin`)
Navigate to `pg-trickle-demo` → `revenue_by_region/`.

---

## Step 3: Generate continuous load

The `generator` container is already running and inserting ~10 orders per second
automatically. You can watch the stream table grow:

```sql
-- Count the rows accumulating in the stream table
SELECT * FROM revenue_by_region ORDER BY total_revenue DESC;

-- Watch the refresh history — one row per 5-second cycle
SELECT
    started_at,
    refresh_mode,
    delta_rows_in,
    delta_rows_out,
    duration_ms
FROM pgtrickle.pgt_refresh_history
WHERE pgt_id = (
    SELECT pgt_id FROM pgtrickle.pgt_stream_tables
    WHERE table_name = 'revenue_by_region'
)
ORDER BY started_at DESC
LIMIT 10;
```

Notice that `delta_rows_in` is always small (only the orders from the last 5-second
window), not the full table size.

---

## Step 4: Query from DuckDB

Open your browser to http://localhost:8888 (token: `demo`) and open the
`oltp_lake_analysis.ipynb` notebook — it has pre-written DuckDB queries ready
to run.

Alternatively, from the DuckDB CLI or a Python session:

```python
import duckdb

con = duckdb.connect()
```

```sql
-- Attach the DuckLake catalog that lives in PostgreSQL
ATTACH 'ducklake:postgresql://postgres:postgres@localhost:5432/postgres'
    AS my_lake (TYPE DUCKLAKE);

-- Current revenue by region (DuckDB merges all the Parquet deltas)
SELECT * FROM my_lake.revenue_by_region ORDER BY total_revenue DESC;
```

You should see the same totals you would see from `SELECT * FROM revenue_by_region`
in PostgreSQL — the Parquet deltas and the PostgreSQL storage stay in sync.

```sql
-- Historical trend: how many rows were written per refresh cycle
SELECT
    date_trunc('minute', s.snapshot_time) AS minute,
    p.stream_table_name,
    SUM(p.delta_row_count)                AS deltas_written
FROM ducklake_snapshot s
JOIN pgtrickle.pgt_ducklake_provenance p
    ON s.snapshot_id = p.ducklake_snapshot_id
GROUP BY 1, 2
ORDER BY 1 DESC
LIMIT 20;
```

This query joins two PostgreSQL tables — `ducklake_snapshot` (managed by DuckLake)
and `pgt_ducklake_provenance` (managed by pg_trickle) — to trace every Parquet
file back to the exact refresh cycle that produced it.

---

## Step 5: Explore the provenance trail

pg_trickle records every sink write in `pgtrickle.pgt_ducklake_provenance`.
This gives you end-to-end lineage: from the PostgreSQL INSERT to the Parquet
file on MinIO.

```sql
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at
FROM pgtrickle.pgt_ducklake_provenance
ORDER BY written_at DESC
LIMIT 10;
```

Each row links a pg_trickle refresh cycle to the DuckLake snapshot it produced.
You can join this table to `ducklake_snapshot` (to get the Parquet file path)
or to `pgtrickle.pgt_refresh_history` (to get timing and latency data) to build
complete audit trails or SLA dashboards.

---

## Step 6: Stop and clean up

To stop the containers:

```bash
docker compose down
```

To also remove the MinIO volume (deletes all Parquet files):

```bash
docker compose down -v
```

If you want to drop the stream table while the stack is running:

```sql
SELECT pgtrickle.drop_stream_table('revenue_by_region');
```

This automatically removes the `ducklake_view` entry so the table no longer
appears in the DuckLake catalog. Existing Parquet files on MinIO are not deleted.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `docker compose up` hangs at MinIO | Docker resource limits | Ensure Docker has ≥ 4 GB RAM assigned |
| Can't connect to PostgreSQL | Container not ready yet | Wait for `database system is ready to accept connections` in the logs |
| `pgt_ducklake_provenance` empty after 30 s | S3 write failed | Run `docker compose logs postgres` and look for S3 errors |
| MinIO console shows empty bucket | MinIO setup container failed | Run `docker compose up minio-setup` to re-run the bucket creation |
| JupyterLab unreachable | Port conflict | Check if port 8888 is already in use: `lsof -i :8888` |

---

## What you've built

With a single `create_stream_table` call and `docker compose up` you now have a
complete modern data stack:

- **OLTP-speed writes** — orders land in PostgreSQL at full heap speed; no
  Kafka producer, no connector, no stream processor overhead.
- **Incremental maintenance** — only the rows that changed flow through the DVM
  engine on each 5-second cycle; no full table scans.
- **Durable data lake** — every delta is written to MinIO as Parquet and tracked
  by DuckLake, so the history is preserved even if pg_trickle is restarted.
- **Bidirectional discoverability** — `revenue_by_region` is visible in both
  PostgreSQL (as a regular table) and DuckLake (as a native view).
- **End-to-end lineage** — every Parquet snapshot is traceable back to the
  exact refresh cycle that produced it.

---

## Next steps

- **[Tutorial 4](tutorial-streaming-postgres-to-data-lake.md)** — set up the
  DuckLake sink manually (without docker compose) and learn how to handle
  credentials for AWS S3 and MinIO.
- **[Tutorial 5](tutorial-pg-tide-ducklake-pipeline.md)** — add transactional
  at-least-once delivery guarantees using the pg-tide relay.
- **[Demo E](../demos/ducklake-oltp-lake/)** — the full self-contained
  `docker compose` demo with a live DuckDB trend-analysis notebook.
