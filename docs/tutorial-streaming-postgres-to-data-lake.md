# Tutorial 4: Streaming PostgreSQL to a Data Lake without Kafka

*Use pg_trickle's DuckLake sink to replicate OLTP data into Parquet on S3 — incrementally, with no external streaming infrastructure*

---

## Overview

Traditional approaches to replicating a PostgreSQL table into a data lake involve:

1. A Kafka cluster to capture changes.
2. A Kafka Connect connector (Debezium) to read WAL.
3. A stream processor (Flink, Spark Streaming) to transform and write Parquet.
4. A data lake catalog (Iceberg, Hudi, or DuckLake) to manage snapshots.

pg_trickle replaces steps 1–3 with a single SQL statement. The DuckLake sink
writes Parquet deltas directly from the PostgreSQL process, eliminating the
entire streaming infrastructure.

| Approach | Infra required | Latency | Setup time |
|----------|--------------|---------|------------|
| Kafka + Debezium + Flink | 3 separate systems | ~seconds | Days |
| pg_trickle DuckLake sink | PostgreSQL only | ~seconds | Minutes |

---

## Background: What is DuckLake?

DuckLake is a lightweight data lake catalog that runs **inside PostgreSQL**. It
stores metadata (file paths, schema, snapshots) as ordinary PostgreSQL tables
while the actual data lives in Parquet files on S3-compatible object storage.

DuckDB can attach to a DuckLake catalog and query the Parquet files as if they
were native tables, including time-travel queries over historical snapshots.

You do not need a separate DuckLake server. The catalog is just a PostgreSQL
extension.

---

## Prerequisites

Before starting you need:

- **PostgreSQL 18** with **pg_trickle v0.67.0+** installed and loaded via
  `shared_preload_libraries`.
- The **DuckLake PostgreSQL extension** installed in the same database:
  ```sql
  CREATE EXTENSION IF NOT EXISTS ducklake;
  ```
- An **S3-compatible object store** reachable from the PostgreSQL server:
  - AWS S3 (production)
  - MinIO (self-hosted; see the local MinIO setup note below)
- **DuckDB** CLI or Python library (optional — only needed to query results).
- A **superuser** PostgreSQL role (needed to run `ALTER SYSTEM`).

> **Don't have an S3 bucket yet?** See the
> [Demo E](../demos/ducklake-oltp-lake/) `docker compose` environment — it
> includes a local MinIO container pre-configured with the correct bucket and
> credentials, so you can skip the credential setup entirely and jump straight
> to Step 1.

---

## Architecture of this tutorial

```
PostgreSQL 18 + pg_trickle
  │  customers table  ← INSERT / UPDATE / DELETE here
  │
  └─ stream table: customers_lake
       │  30-second DIFFERENTIAL refresh
       │  DuckLake sink (append mode)
       ▼
  S3 / MinIO
       │  Parquet delta files
       ▼
  DuckLake catalog (PostgreSQL tables)
       │  ducklake_snapshot  — one row per Parquet write
       │  ducklake_view      — exposes customers_lake to DuckDB
       ▼
  DuckDB
       └─ SELECT * FROM lake.customers_lake
          (reads and merges the Parquet snapshots)
```

On each 30-second refresh cycle pg_trickle:

1. Reads only the rows that changed since the last refresh (CDC).
2. Applies the SELECT query differentially using the DVM engine — no full table
   scan.
3. Serialises the delta as a Parquet file (Snappy-compressed by default).
4. Uploads the file to your S3 path.
5. Atomically records `ducklake_data_file` + `ducklake_snapshot` rows in the
   catalog.
6. Writes a provenance record to `pgtrickle.pgt_ducklake_provenance` for
   end-to-end lineage.

If the S3 upload fails, no catalog rows are written and the next cycle retries
automatically.

---

## Step 1: Create the source table and insert test data

Connect to your PostgreSQL database as a superuser and run:

```sql
-- Source table that your application writes to
CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,
    email       TEXT NOT NULL UNIQUE,
    region      TEXT NOT NULL,
    tier        TEXT NOT NULL DEFAULT 'free',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed with a few rows so the first refresh has data to write
INSERT INTO customers (email, region, tier) VALUES
    ('alice@example.com', 'us-east', 'pro'),
    ('bob@example.com',   'eu-west', 'free'),
    ('carol@example.com', 'ap-south', 'enterprise');
```

pg_trickle uses DIFFERENTIAL refresh, so it tracks which rows changed since the
last cycle and only processes those rows — not the whole table. Even a
`customers` table with millions of rows is fine.

---

## Step 2: Configure S3 credentials and create the stream table

### 2a. Configure S3 credentials

pg_trickle needs to know how to reach your object store. There are three
scenarios depending on your setup. Pick the one that matches yours.

---

**Scenario A — AWS S3 with explicit access keys**

Run these commands as a PostgreSQL superuser. `ALTER SYSTEM` writes the values
to `postgresql.auto.conf`; `pg_reload_conf()` makes PostgreSQL re-read it
without a restart.

```sql
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_region     = 'us-east-1';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_access_key = 'AKIAIOSFODNN7EXAMPLE';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_secret_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY';
SELECT pg_reload_conf();
```

Replace the region, key, and secret with your actual AWS credentials. The
access key and secret key are stored in `postgresql.auto.conf` on the server
filesystem — treat that file with the same care as any credentials file.

> **Tip:** Prefer IAM instance roles (Scenario B) over explicit keys wherever
> possible to avoid storing long-lived credentials on disk.

---

**Scenario B — AWS S3 with an IAM role or environment-variable credential chain**

Leave `ducklake_sink_s3_access_key` and `ducklake_sink_s3_secret_key` unset
(their default is empty). pg_trickle will fall back to the standard AWS
credential chain:

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables set
   in the PostgreSQL server process environment.
2. An IAM instance profile attached to the EC2 / ECS task running PostgreSQL.
3. An IAM role assumed via `AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE`.

You still need to set the region:

```sql
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_region = 'eu-west-1';
SELECT pg_reload_conf();
```

---

**Scenario C — MinIO or another S3-compatible store**

MinIO exposes an S3-compatible API but uses a custom endpoint URL instead of
the AWS endpoint. You must tell pg_trickle where to find it.

```sql
-- Point to your MinIO instance (replace host/port as needed)
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_endpoint   = 'http://minio:9000';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_access_key = 'minioadmin';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_secret_key = 'minioadmin';
-- Region is ignored when an endpoint override is set, but must be non-empty
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_region     = 'us-east-1';
SELECT pg_reload_conf();
```

If you are using the [Demo E](../demos/ducklake-oltp-lake/) docker compose
environment, these values are already pre-configured in the container's
`postgresql.conf` — you do not need to run any `ALTER SYSTEM` commands.

---

### 2b. Create the stream table

Once credentials are in place, create the stream table:

```sql
SELECT pgtrickle.create_stream_table(
    'customers_lake',
    query => $$
        SELECT
            customer_id,
            email,
            region,
            tier,
            created_at,
            updated_at
        FROM customers
    $$,
    schedule             => '30s',
    refresh_mode         => 'DIFFERENTIAL',
    sink                 => 'ducklake',
    ducklake_sink_path   => 's3://my-data-lake/customers/',
    ducklake_sink_mode   => 'append'
);
```

**What each parameter means:**

| Parameter | Value | Meaning |
|-----------|-------|---------|
| `schedule` | `'30s'` | Refresh every 30 seconds. |
| `refresh_mode` | `'DIFFERENTIAL'` | Only process rows that changed since the last cycle — not the full table. |
| `sink` | `'ducklake'` | Write each delta to DuckLake / S3 instead of keeping it only in PostgreSQL. |
| `ducklake_sink_path` | S3 URI | Where Parquet files land. Must be a bucket that pg_trickle can write to with the credentials above. |
| `ducklake_sink_mode` | `'append'` | Each refresh cycle appends a new Parquet delta file. DuckLake merges the history. Use `'replace'` to overwrite a single full-snapshot file each cycle instead. |

After this call pg_trickle:

1. Creates the `customers_lake` storage table inside PostgreSQL.
2. Installs CDC triggers on `customers` to capture changes.
3. Registers a `ducklake_view` entry so DuckDB sees `customers_lake` as a
   native catalog object.
4. Schedules the first refresh for 30 seconds from now.

> **Verify the stream table was created:**
> ```sql
> SELECT pgt_id, table_name, refresh_mode, schedule, sink, ducklake_sink_path
> FROM pgtrickle.pgt_stream_tables
> WHERE table_name = 'customers_lake';
> ```

---

## Step 3: Verify the first write

Wait 30 seconds (or trigger an immediate refresh with
`SELECT pgtrickle.force_refresh('customers_lake')`), then check the provenance
table:

```sql
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at
FROM pgtrickle.pgt_ducklake_provenance
WHERE stream_table_name = 'customers_lake'
ORDER BY written_at DESC;
```

You should see one row with `delta_row_count = 3` (the three customers you
inserted in Step 1).

Also confirm the DuckLake catalog recorded the snapshot:

```sql
SELECT snapshot_id, snapshot_time, schema_version
FROM ducklake_snapshot
ORDER BY snapshot_time DESC
LIMIT 5;
```

If no rows appear, check the PostgreSQL log for errors. The most common causes
are wrong S3 credentials, a missing bucket, or a network connectivity issue
between the PostgreSQL server and the object store.

---

## Step 4: Make a change and watch it propagate

Insert another customer and update an existing one:

```sql
INSERT INTO customers (email, region, tier)
VALUES ('dan@example.com', 'us-west', 'pro');

UPDATE customers
SET tier = 'enterprise', updated_at = now()
WHERE email = 'bob@example.com';
```

After the next 30-second cycle, the provenance table will have a new row. The
`delta_row_count` will be 2 (one insert, one update represented as a
delete-then-insert pair in the DVM engine's output).

---

## Step 5: Handle deletes (tombstone pattern)

DuckLake accumulates Parquet delta files over time. When a row is deleted from
`customers`, pg_trickle records the deletion in the delta, but the row still
exists in the older Parquet files that DuckLake has already written.

There are two strategies:

### Strategy A — Soft deletes with an `is_deleted` flag (recommended for DIFFERENTIAL mode)

Add a boolean flag column to the stream table's defining query. Downstream
consumers filter on `is_deleted = FALSE` to get the current view.

```sql
-- Update the stream table's defining query to include the flag
SELECT pgtrickle.alter_stream_table(
    'customers_lake',
    query => $$
        SELECT
            customer_id,
            email,
            region,
            tier,
            created_at,
            updated_at,
            FALSE AS is_deleted
        FROM customers
    $$
);
```

When a row is physically deleted from `customers`, it disappears from the
stream table's PostgreSQL storage. It will not reappear in future Parquet
deltas. The old Parquet files with `is_deleted = FALSE` for that row remain
unchanged — they represent the historical record. DuckDB time-travel queries
can still see the row as it existed before deletion.

> **Note:** `pgtrickle.alter_stream_table` reinitializes the stream table —
> the next refresh will be a FULL refresh to rebuild the storage with the new
> column. After that, DIFFERENTIAL resumes.

### Strategy B — Periodic full snapshots with `'replace'` mode

If you need DuckLake consumers to always see a clean, delete-aware view
without old Parquet history accumulating, use `replace` mode on a longer
schedule. Each cycle writes one full-snapshot Parquet file that overwrites the
previous one:

```sql
SELECT pgtrickle.alter_stream_table(
    'customers_lake',
    ducklake_sink_mode => 'replace',
    schedule           => '1h'
);
```

DuckDB time-travel is still available across the hourly snapshots, but deleted
rows vanish from the latest snapshot immediately.

---

## Step 6: Query from DuckDB

In a DuckDB session (CLI or Python), attach to the DuckLake catalog stored in
your PostgreSQL database:

```sql
-- Attach the DuckLake catalog
-- Replace the connection string with your actual PostgreSQL host/credentials
ATTACH 'ducklake:postgresql://postgres:postgres@localhost:5432/postgres'
    AS lake (TYPE DUCKLAKE);
```

Now query the stream table as a native DuckLake view:

```sql
-- Current state of all non-deleted customers
SELECT *
FROM lake.customers_lake
WHERE is_deleted = FALSE
ORDER BY customer_id;
```

**Time-travel** — read the table as it looked one hour ago:

```sql
-- The AT (SNAPSHOT => ...) clause selects the DuckLake snapshot
-- that was current at a given point in time
SELECT *
FROM lake.customers_lake
AT (SNAPSHOT => (
    SELECT MAX(snapshot_id)
    FROM ducklake_snapshot
    WHERE snapshot_time <= now() - INTERVAL '1 hour'
))
ORDER BY customer_id;
```

**Trend analysis** — how many rows were written per refresh cycle:

```sql
SELECT
    date_trunc('minute', p.written_at) AS minute,
    p.stream_table_name,
    SUM(p.delta_row_count)             AS rows_written
FROM pgtrickle.pgt_ducklake_provenance p
WHERE p.stream_table_name = 'customers_lake'
GROUP BY 1, 2
ORDER BY 1 DESC;
```

---

## Step 7: Optional — tune compression

The default Parquet compression codec is **Snappy**, which gives a good balance
of speed and file size. To switch to Zstandard (better compression, slightly
slower):

```sql
ALTER SYSTEM SET pg_trickle.ducklake_sink_compression = 'zstd';
SELECT pg_reload_conf();
```

Supported values: `'snappy'` (default), `'zstd'`, `'gzip'`, `'none'`.

---

## Cleanup

```sql
-- Remove the stream table and its DuckLake catalog entry
SELECT pgtrickle.drop_stream_table('customers_lake');
```

`drop_stream_table` automatically removes the `ducklake_view` entry so the
table no longer appears in the DuckLake catalog. Existing Parquet files on S3
are **not** deleted — manage those directly in your object store if needed.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| No rows in `pgt_ducklake_provenance` after 60 s | S3 write failed | Check PostgreSQL log for `ERROR` lines mentioning `ducklake_sink` or S3 |
| `AccessDenied` in log | Wrong access key / secret, or bucket policy | Verify credentials and that the IAM policy allows `s3:PutObject` on the bucket |
| `NoSuchBucket` in log | Bucket does not exist | Create the bucket before creating the stream table |
| `connection refused` to endpoint | MinIO not reachable from PostgreSQL container | Check `ducklake_sink_s3_endpoint` and that the containers are on the same Docker network |
| `pgt_stream_tables` shows `status = 'ERROR'` | Refresh failed and hit the consecutive-error limit | Run `SELECT pgtrickle.reinitialize('customers_lake')` to reset |
| DuckDB sees no data | `ducklake_view` not registered | Run `SELECT * FROM ducklake_view` in PostgreSQL to confirm the view exists |

---

## Reference: DuckLake sink GUCs

| GUC | Default | Description |
|-----|---------|-------------|
| `pg_trickle.ducklake_sink_compression` | `'snappy'` | Parquet compression codec. |
| `pg_trickle.ducklake_sink_s3_endpoint` | (none) | Endpoint URL override for MinIO or other S3-compatible stores. Leave empty for AWS S3. |
| `pg_trickle.ducklake_sink_s3_region` | `'us-east-1'` | AWS region. Ignored when `s3_endpoint` is set. |
| `pg_trickle.ducklake_sink_s3_access_key` | (none) | Access key ID. Empty = use credential chain. |
| `pg_trickle.ducklake_sink_s3_secret_key` | (none) | Secret access key. Empty = use credential chain. |

All GUCs take effect after `SELECT pg_reload_conf()`. No server restart is
required.

---

## Next steps

- **[Tutorial 3](tutorial-modern-data-stack-one-box.md)** — the full modern
  data stack (OLTP + pg_trickle + DuckLake + DuckDB) in a single
  `docker compose up`.
- **[Demo E](../demos/ducklake-oltp-lake/)** — a self-contained `docker compose`
  demo of this OLTP-to-lake loop with a live DuckDB notebook.
- **[INT-10 tutorial](tutorial-pg-tide-ducklake-pipeline.md)** — use pg-tide's
  relay to stream data to DuckLake with transactional guarantees.
