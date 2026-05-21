# Demo: OLTP-to-Lake Loop

*The full modern data stack in a single `docker compose up` — PostgreSQL writes to a DuckLake in MinIO, DuckDB reads it*

---

## What you'll build

You'll run a complete OLTP-to-data-lake pipeline that turns raw e-commerce orders
into a continuously-updated `revenue_by_region` analytics table queryable from
DuckDB — all on your laptop, with no cloud accounts, no Kafka, and no Spark.

A Python order generator inserts 10 orders per second into PostgreSQL. A
pg_trickle stream table aggregates those orders by region and minute,
differentially — touching only the rows that changed since the last cycle. Every
5 seconds, each delta is written as a Parquet file to MinIO (your local S3). A
JupyterLab notebook queries the growing history with DuckDB and plots revenue
trends.

**End-to-end latency from ORDER INSERT to DuckLake snapshot: typically under 2 seconds.**

---

## Background: The OLTP-to-lake problem

Most businesses run PostgreSQL (or another OLTP database) for transactional
workloads. But PostgreSQL is not designed for analytical queries that scan
millions of rows. The classic solution is to replicate data to a separate OLAP
system — but that requires CDC pipelines, stream processors, schema registries,
and careful consistency management.

pg_trickle collapses this stack. It sits inside PostgreSQL and maintains an
incrementally-refreshed aggregate (the stream table) that is both:

- **Queryable directly from PostgreSQL** in sub-millisecond reads (operational use)
- **Continuously published to a DuckLake** as Parquet deltas (analytical use)

There's one source of truth, one maintenance mechanism, and no consistency gap
between the operational and analytical views.

---

## Prerequisites

- **Docker Engine 24+** and **Docker Compose v2**
  Verify: `docker compose version`
- **4 GB free RAM** for the four containers
- No local installation of pg_trickle, DuckDB, or Python needed

---

## Architecture

```
Python order generator
  │  10 orders/second
  │  columns: order_id, region, amount, created_at
  ▼
PostgreSQL 18 + pg_trickle  (port 5432, db: postgres)
  │  orders table  ← trigger-based CDC on INSERT
  │
  └─ stream table: revenue_by_region
       query: SELECT region, date_trunc('minute', created_at) AS minute,
                     SUM(amount) AS total_revenue, COUNT(*) AS order_count
              FROM orders GROUP BY region, date_trunc('minute', created_at)
       5-second DIFFERENTIAL refresh
       DuckLake sink → pgtrickle.pgt_ducklake_provenance (provenance log)
       ▼
MinIO  (port 9000 / console 9001)
  s3://pg-trickle-demo/revenue_by_region/
  │  One Parquet delta file per refresh cycle
  │  Only the (region, minute) rows that changed in the last 5 seconds
  ▼
DuckLake catalog  (stored in PostgreSQL)
  ducklake_snapshot, ducklake_view, ducklake_data_file
  ▼
JupyterLab  (port 8888, token: demo)
  oltp_lake_analysis.ipynb
  DuckDB queries the Parquet history for trend analysis
```

---

## Step 1: Start the demo

```bash
cd demos/ducklake-oltp-lake
docker compose up
```

Four containers start:

| Container | Port | Purpose |
|-----------|------|---------|
| `postgres` | 5432 | PostgreSQL 18 + pg_trickle + DuckLake catalog |
| `minio` | 9000 / 9001 | S3-compatible object storage |
| `jupyter` | 8888 | JupyterLab with DuckDB analytics notebook |
| `generator` | — | Python script producing 10 orders/s |

Wait until all containers are healthy:
```
postgres  | LOG: database system is ready to accept connections
jupyter   | [JupyterLab] http://127.0.0.1:8888/lab?token=demo
```

---

## Step 2: Open JupyterLab

Go to **http://localhost:8888/lab?token=demo** and open `oltp_lake_analysis.ipynb`.

The notebook has three sections:

1. **Connect** — attaches DuckDB to the DuckLake catalog in PostgreSQL
2. **Query current revenue** — reads from the stream table directly for
   sub-millisecond results
3. **Analyse the Parquet history** — time-travel queries over the snapshot chain
   to plot how revenue by region evolved over the last N minutes

Run all cells top to bottom with **Run → Run All Cells**.

---

## Step 3: Connect to PostgreSQL and explore the pipeline

In a second terminal:

```bash
psql postgresql://postgres:postgres@localhost:5432/postgres
```

See the stream table:

```sql
SELECT table_name, refresh_mode, schedule, sink, ducklake_sink_path
FROM pgtrickle.pgt_stream_tables;
```

Query the current revenue by region:

```sql
SELECT region, minute, total_revenue, order_count
FROM revenue_by_region
ORDER BY minute DESC, total_revenue DESC;
```

Watch the provenance trail — one row per Parquet file written to MinIO:

```sql
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at,
    written_at - LAG(written_at) OVER (ORDER BY written_at) AS cycle_duration
FROM pgtrickle.pgt_ducklake_provenance
WHERE stream_table_name = 'revenue_by_region'
ORDER BY written_at DESC
LIMIT 10;
```

`delta_row_count` shows how many (region, minute) rows were updated in each 5-
second cycle. When the generator is running at steady state, this is typically
1–4 rows (only the current minute's region aggregates change), regardless of
how many total orders have been inserted.

---

## Step 4: Explore the Parquet files in MinIO

Open the MinIO console at **http://localhost:9001**.

- Login: `minioadmin` / `minioadmin`
- Navigate to `pg-trickle-demo` → `revenue_by_region/`

A new `.parquet` file appears roughly every 5 seconds (whenever orders have
arrived in the last cycle). Each file is a delta — not a full snapshot of
`revenue_by_region`. The DuckLake catalog in PostgreSQL tracks which files belong
to which snapshot, so DuckDB assembles the correct view when you query.

---

## Step 5: Understand the stream table SQL

The stream table is defined by this call (pre-loaded by `postgres/init.sql`):

```sql
SELECT pgtrickle.create_stream_table(
    'revenue_by_region',
    query => $$
        SELECT
            region,
            date_trunc('minute', created_at) AS minute,
            SUM(amount)                       AS total_revenue,
            COUNT(*)                          AS order_count
        FROM orders
        GROUP BY region, date_trunc('minute', created_at)
    $$,
    schedule           => '5s',
    refresh_mode       => 'DIFFERENTIAL',
    sink               => 'ducklake',
    ducklake_sink_path => 's3://pg-trickle-demo/revenue_by_region/'
);
```

**How DIFFERENTIAL refresh works for `SUM`:**
When new orders arrive, pg_trickle's DVM engine computes:
```
Δ_total_revenue(region, minute) = SUM(amount) over NEW_ORDERS
```
for each `(region, minute)` bucket that received new orders. It adds this delta
to the existing `revenue_by_region` values. Only the affected buckets are updated
— not a full `GROUP BY` recomputation over all orders.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `revenue_by_region` is empty after 30 s | Generator not running | Check `docker compose logs generator` |
| MinIO console shows empty bucket | DuckLake sink S3 write failed | Check `docker compose logs postgres` for S3 errors |
| JupyterLab shows "No such file or directory" on notebook | Jupyter started before volume was ready | Refresh the browser and re-open the notebook |
| `pgt_ducklake_provenance` has no rows | DuckLake sink not writing | Verify `sink = 'ducklake'` in `pgt_stream_tables` |
| DuckDB notebook gives "catalog not found" | pg_trickle catalog tables not initialised | Check `docker compose logs postgres` for init errors |
| Port 5432 conflict | Another PostgreSQL already running | Stop the local instance: `brew services stop postgresql` |
| Port 8888 conflict | Another Jupyter instance running | Stop it and re-run `docker compose up` |

---

## What you've built

- A **full OLTP-to-lake pipeline** in one `docker compose up`:
  write to PostgreSQL → aggregate differentially → publish Parquet deltas to
  MinIO → query history with DuckDB.
- **Sub-2-second end-to-end latency** from INSERT to DuckLake snapshot with no
  Kafka, no Flink, no Debezium.
- **Both operational and analytical access** from the same stream table: fast
  row reads from PostgreSQL, time-travel analytics from DuckDB.

---

## Stop and clean up

```bash
docker compose down -v
```

The `-v` flag removes the PostgreSQL and MinIO data volumes for a clean restart.

---

## Related resources

- [Tutorial 3: The Modern Data Stack in One Box](../../docs/tutorial-modern-data-stack-one-box.md)
- [Tutorial 4: Streaming PostgreSQL to a Data Lake](../../docs/tutorial-streaming-postgres-to-data-lake.md)
- [Demo: DuckLake Observability in a Box](../ducklake-observability/README.md)


A self-contained `docker compose up` demo of the full OLTP-to-data-lake loop:

1. Synthetic e-commerce orders land in a PostgreSQL `orders` table.
2. A pg_trickle stream table computes `revenue_by_region` incrementally.
3. The DuckLake sink publishes each delta to MinIO as a Parquet file.
4. A DuckDB notebook queries the historical Parquet files for trend analysis.

**End-to-end latency from order INSERT to DuckLake snapshot: typically under 2 seconds.**

---

## Quick Start

```bash
cd demos/ducklake-oltp-lake
docker compose up
```

- **JupyterLab**: http://localhost:8888 (token: `demo`) — open `oltp_lake_analysis.ipynb`
- **MinIO console**: http://localhost:9001 (minioadmin/minioadmin)

---

## Architecture

```
Order generator (Python)
  │  10 orders/second
  ▼
PostgreSQL 18 + pg_trickle
  │  orders table
  └─ stream table: revenue_by_region
       │  5-second DIFFERENTIAL refresh
       │  DuckLake sink (append mode)
       │  Provenance → pgtrickle.pgt_ducklake_provenance
       ▼
  MinIO (S3-compatible object store)
       │  Parquet delta files
       ▼
  DuckLake catalog
       │  ducklake_snapshot, ducklake_view
       ▼
  DuckDB (JupyterLab)
       └─ Trend analysis notebook
```

---

## Services

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL 18 + pg_trickle | 5432 | OLTP source, stream table, DuckLake catalog |
| MinIO | 9000/9001 | S3-compatible object storage |
| JupyterLab | 8888 | DuckDB analytics notebook |
| Order generator | — | Python script producing 10 orders/s |

---

## The stream table

```sql
SELECT pgtrickle.create_stream_table(
    'revenue_by_region',
    query => $$
        SELECT
            region,
            date_trunc('minute', created_at) AS minute,
            SUM(amount)                       AS total_revenue,
            COUNT(*)                          AS order_count
        FROM orders
        GROUP BY region, date_trunc('minute', created_at)
    $$,
    schedule           => '5s',
    refresh_mode       => 'DIFFERENTIAL',
    sink               => 'ducklake',
    ducklake_sink_path => 's3://pg-trickle-demo/revenue_by_region/'
);
```

---

## Provenance query

```sql
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at,
    written_at - LAG(written_at) OVER (ORDER BY written_at) AS cycle_duration
FROM pgtrickle.pgt_ducklake_provenance
WHERE stream_table_name = 'revenue_by_region'
ORDER BY written_at DESC
LIMIT 20;
```
