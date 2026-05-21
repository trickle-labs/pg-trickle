# Tutorial 5: Guaranteed Delivery to DuckLake with pg-tide

*Add a transactional outbox between pg_trickle and DuckLake so every delta reaches S3 \u2014 even through network failures and restarts*

---

## What you'll build

You'll wire a pg_trickle stream table to DuckLake via **pg-tide's transactional
relay**. Every differential delta is atomically enqueued in a pg-tide outbox
*in the same database transaction as the refresh*. A background relay worker
then picks up the outbox messages and writes Parquet files to S3. If the S3
write fails, the message stays in the outbox and the relay retries
automatically \u2014 giving you at-least-once delivery with no message loss.

**When should you use this instead of the direct sink?**

| Pattern | Best for |
|---------|---------|
| Direct sink (`sink => 'ducklake'`) | Simple pipelines; best-effort delivery is fine |
| pg-tide relay (this tutorial) | Production pipelines; S3 can be unreliable; you need fan-out to multiple sinks or exactly-once semantics |

---

## Background: What is pg-tide?

[pg-tide](https://github.com/trickle-labs/pg-tide) is a PostgreSQL extension
that implements the **transactional outbox pattern**: instead of writing
directly to an external system (S3, Kafka, SQS) inside a database transaction,
you write to an outbox table in the same transaction. A separate relay process
reads the outbox and delivers messages to the destination, retrying on failure.

The critical guarantee: **the outbox write and the pg_trickle refresh are in
the same transaction**. Either both succeed or neither does. This eliminates
the "refresh succeeded but S3 write failed silently" failure mode that can
happen with the direct sink.

---

## Prerequisites

- **PostgreSQL 18** with **pg_trickle v0.67.0+**
- **pg-tide v0.22.0+** installed:
  ```sql
  CREATE EXTENSION IF NOT EXISTS pg_tide;
  ```
  Verify: `SELECT tide.version();`
- **DuckLake 1.x** installed:
  ```sql
  CREATE EXTENSION IF NOT EXISTS ducklake;
  ```
- An S3-compatible object store (AWS S3 or MinIO). See
  [Tutorial 4](tutorial-streaming-postgres-to-data-lake.md) for credential
  setup guidance.
- A bucket already created (e.g. `my-lake`)

---

## Architecture

```
Application
  \u2502  INSERT into orders
  \u25bc
PostgreSQL
  \u251c\u2500 orders table
  \u2502
  \u251c\u2500 pg_trickle: revenue_by_region stream table
  \u2502    \u2502  5-second DIFFERENTIAL refresh
  \u2502    \u25bc
  \u2514\u2500 pg-tide outbox: revenue_by_region_outbox
       \u2502  Written in the same transaction as the refresh
       \u2502  Survives pg_trickle restarts and S3 outages
       \u25bc
  pg-tide relay (background worker)
       \u2502  Polls outbox with SKIP LOCKED
       \u2502  Serializes delta to Parquet
       \u2502  Uploads to S3
       \u2502  On success: marks message delivered
       \u2502  On failure: leaves message for retry
       \u25bc
  S3 / MinIO
       \u2502  Parquet delta files
       \u25bc
  DuckLake catalog
       \u2514\u2500 ducklake_snapshot, ducklake_view
```

---

## Step 1: Install extensions and verify

Connect as a superuser and install both extensions:

```sql
CREATE EXTENSION IF NOT EXISTS pg_tide;
CREATE EXTENSION IF NOT EXISTS pg_trickle;
CREATE EXTENSION IF NOT EXISTS ducklake;

-- Verify versions
SELECT tide.version()      AS pg_tide_version;
SELECT pgtrickle.version() AS pg_trickle_version;
```

---

## Step 2: Configure S3 credentials for the relay

The pg-tide relay needs credentials to write to S3. Set these as PostgreSQL
GUCs (same approach as Tutorial 4):

**For AWS S3:**
```sql
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_region     = 'us-east-1';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_access_key = 'AKIAIOSFODNN7EXAMPLE';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_secret_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY';
SELECT pg_reload_conf();
```

**For MinIO:**
```sql
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_endpoint   = 'http://minio:9000';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_access_key = 'minioadmin';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_secret_key = 'minioadmin';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_region     = 'us-east-1';
SELECT pg_reload_conf();
```

---

## Step 3: Create the source and stream tables

```sql
-- Source OLTP table
CREATE TABLE orders (
    order_id   BIGSERIAL    PRIMARY KEY,
    region     TEXT         NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Stream table (no sink parameter yet — we'll attach pg-tide instead)
SELECT pgtrickle.create_stream_table(
    'revenue_by_region',
    query        => $$
        SELECT
            region,
            SUM(amount)  AS total_revenue,
            COUNT(*)     AS order_count
        FROM orders
        GROUP BY region
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## Step 4: Attach the pg-tide outbox

This is the key step. `attach_outbox()` does three things:

1. Creates a pg-tide outbox table that will hold pending delta messages.
2. Wires the stream table's refresh pipeline so every differential delta is
   written to the outbox **atomically** (same transaction as the refresh).
3. Registers the outbox with the pg-tide relay background worker.

```sql
SELECT pgtrickle.attach_outbox('revenue_by_region');
```

Verify the outbox was created:

```sql
SELECT outbox_name, status, pending_count
FROM tide.outboxes
WHERE outbox_name = 'revenue_by_region';
```

---

## Step 5: Configure the relay destination

Tell the pg-tide relay where to send the messages \u2014 in this case, to DuckLake
on S3:

```sql
SELECT tide.relay_set_outbox(
    outbox_name  => 'revenue_by_region',
    sink_type    => 'ducklake',
    sink_options => jsonb_build_object(
        'catalog_db',    'postgres',        -- PostgreSQL database where DuckLake catalog lives
        'data_path',     's3://my-lake/revenue_by_region/',
        's3_region',     'us-east-1',       -- or leave empty to use the GUC value
        's3_access_key', '',                -- empty = use the GUC / credential chain
        's3_secret_key', ''
    )
);
```

> **Tip:** Set `s3_access_key` and `s3_secret_key` to empty strings to reuse
> the GUC values you set in Step 2 \u2014 that way you only store credentials in
> one place.

---

## Step 6: Verify end-to-end delivery

Insert some orders and watch the full pipeline:

```sql
INSERT INTO orders (region, amount) VALUES
    ('us-east', 99.00),
    ('eu-west', 155.50),
    ('ap-south', 42.00);

-- Wait 5 seconds for the first refresh cycle, then check:

-- 1. Did pg_trickle refresh?
SELECT refresh_mode, delta_rows_in, delta_rows_out, duration_ms
FROM pgtrickle.pgt_refresh_history
WHERE pgt_id = (
    SELECT pgt_id FROM pgtrickle.pgt_stream_tables
    WHERE table_name = 'revenue_by_region'
)
ORDER BY started_at DESC LIMIT 3;

-- 2. Did pg-tide pick up the outbox message?
SELECT outbox_name, delivered_count, failed_count, pending_count
FROM tide.outboxes
WHERE outbox_name = 'revenue_by_region';

-- 3. Did the Parquet file land in DuckLake?
SELECT stream_table_name, ducklake_snapshot_id, delta_row_count, written_at
FROM pgtrickle.pgt_ducklake_provenance
WHERE stream_table_name = 'revenue_by_region'
ORDER BY written_at DESC
LIMIT 5;
```

`delivered_count` should increment by 1 for each refresh cycle that had
changes. `pending_count` should be 0 (meaning all messages have been
delivered).

---

## Step 7: Understand the retry behaviour

To simulate an S3 outage, stop MinIO (or temporarily set an invalid endpoint):

```sql
-- Simulate bad credentials to trigger a delivery failure
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_secret_key = 'wrong-key';
SELECT pg_reload_conf();
```

Now insert more orders and wait for a refresh cycle. The outbox message will
be written (the refresh succeeded) but the relay will fail to deliver it to S3:

```sql
INSERT INTO orders (region, amount) VALUES ('us-west', 75.00);

-- Wait for refresh, then check outbox status
SELECT outbox_name, pending_count, failed_count
FROM tide.outboxes
WHERE outbox_name = 'revenue_by_region';
-- pending_count > 0 and failed_count incremented
```

Restore the correct credentials:

```sql
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_secret_key = 'correct-secret-key';
SELECT pg_reload_conf();
```

The relay will automatically retry and deliver the pending messages. No data
was lost \u2014 the deltas are safely in the outbox.

---

## How this differs from the direct sink

With `attach_outbox()` + pg-tide relay versus `sink => 'ducklake'`:

| Aspect | Direct sink | pg-tide relay |
|--------|------------|---------------|
| S3 write timing | During the refresh transaction | After the refresh, in a separate relay transaction |
| Refresh blocked by S3 | Yes \u2014 if S3 is slow, the refresh cycle takes longer | No \u2014 refresh completes as soon as the outbox write succeeds |
| Retry on S3 failure | Automatic (next refresh cycle) | Automatic (relay retry loop, independent of refresh schedule) |
| Fan-out to multiple sinks | Not supported | Supported: add multiple relay destinations to the same outbox |
| Delivery guarantee | Best-effort | At-least-once (outbox survives crashes) |

---

## Cleanup

```sql
-- Remove the relay destination first
SELECT tide.relay_remove_outbox('revenue_by_region');

-- Then drop the stream table (also removes the outbox)
SELECT pgtrickle.drop_stream_table('revenue_by_region');
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `tide.outboxes` returns no rows after `attach_outbox()` | pg-tide not installed or wrong schema | Verify `CREATE EXTENSION pg_tide` ran; check `\dx` |
| `pending_count` never decreases | pg-tide relay background worker not running | Check `pg_stat_activity` for `pg_tide_relay` processes; restart PostgreSQL |
| `failed_count` keeps incrementing | S3 credentials wrong or bucket inaccessible | Check `tide.relay_failures` for the error message |
| Refresh succeeds but no provenance row | Relay not yet delivered | `pending_count > 0` means the relay is still processing; wait a few seconds |

---

## Next steps

- **[pg-tide documentation](https://github.com/trickle-labs/pg-tide)** \u2014 fan-out
  configuration, dead-letter queues, and monitoring.
- **[Tutorial 3](tutorial-modern-data-stack-one-box.md)** \u2014 the full modern data
  stack with a `docker compose` environment.
- **[Tutorial 4](tutorial-streaming-postgres-to-data-lake.md)** \u2014 using the
  direct DuckLake sink without pg-tide.
