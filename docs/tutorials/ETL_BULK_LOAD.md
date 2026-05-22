# Tutorial: ETL & Bulk Load Patterns

pg_trickle provides **source gating** and **watermark gating**
to coordinate stream table refreshes with ETL pipelines and bulk
data loads. This tutorial covers common patterns for pausing refreshes
during loads and resuming them safely afterward.

## The Problem

When you bulk-load data into a source table (e.g., a nightly ETL job), the
change buffer fills rapidly. Without coordination:

- A differential refresh mid-load sees a **partial** batch, producing
  incomplete results
- The adaptive fallback may trigger repeated FULL refreshes during the load
- The fuse circuit breaker may blow, requiring manual intervention

**Source gating** solves this by telling pg_trickle to skip refreshes for
gated sources until the load completes.

## Recipe 1 — Single Source Bulk Load

The simplest pattern: gate the source, load data, ungate.

```sql
-- 1. Gate the source table — all dependent stream tables pause
SELECT pgtrickle.gate_source('public.orders');

-- 2. Perform the bulk load
COPY orders FROM '/data/orders_20260331.csv' WITH (FORMAT csv, HEADER);
-- or: INSERT INTO orders SELECT ... FROM staging_orders;

-- 3. Ungate — stream tables resume and process the full batch
SELECT pgtrickle.ungate_source('public.orders');
```

While gated, the scheduler skips all stream tables that depend on the gated
source. Changes still accumulate in the CDC buffer and are processed in a
single batch after ungating.

## Recipe 2 — Coordinated Multi-Source Load

When your ETL loads multiple tables that feed into the same stream table:

```sql
-- Gate all sources involved in the load
SELECT pgtrickle.gate_source('public.orders');
SELECT pgtrickle.gate_source('public.customers');
SELECT pgtrickle.gate_source('public.products');

-- Load all tables
COPY orders FROM '/data/orders.csv' WITH (FORMAT csv, HEADER);
COPY customers FROM '/data/customers.csv' WITH (FORMAT csv, HEADER);
COPY products FROM '/data/products.csv' WITH (FORMAT csv, HEADER);

-- Ungate all at once — stream tables see a consistent snapshot
SELECT pgtrickle.ungate_source('public.orders');
SELECT pgtrickle.ungate_source('public.customers');
SELECT pgtrickle.ungate_source('public.products');
```

## Recipe 3 — Gate + Deferred Stream Table Creation

For initial deployments where data must be loaded before stream tables
are created:

```sql
-- 1. Gate the source before any stream tables exist
SELECT pgtrickle.gate_source('public.orders');

-- 2. Load the initial data
COPY orders FROM '/data/historical_orders.csv' WITH (FORMAT csv, HEADER);

-- 3. Create stream tables — they won't refresh yet (source is gated)
SELECT pgtrickle.create_stream_table(
    name     => 'order_totals',
    query    => 'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    schedule => '1m'
);

-- 4. Ungate — the first refresh processes all data cleanly
SELECT pgtrickle.ungate_source('public.orders');
```

## Recipe 4 — Nightly Batch Pattern

A common production pattern using a scheduled batch job:

```sql
-- Run nightly at 02:00 UTC

-- Step 1: Gate all ETL sources
DO $$
DECLARE
    src TEXT;
BEGIN
    FOR src IN SELECT DISTINCT source_table
               FROM pgtrickle.list_sources('daily_report')
    LOOP
        PERFORM pgtrickle.gate_source(src);
    END LOOP;
END;
$$;

-- Step 2: Run the ETL pipeline
CALL etl.load_daily_data();

-- Step 3: Ungate all sources
DO $$
DECLARE
    gated RECORD;
BEGIN
    FOR gated IN SELECT source_name FROM pgtrickle.source_gates()
                 WHERE is_gated = true
    LOOP
        PERFORM pgtrickle.ungate_source(gated.source_name);
    END LOOP;
END;
$$;
```

## Monitoring During a Gated Load

While sources are gated, verify the gate status:

```sql
-- Check which sources are currently gated
SELECT * FROM pgtrickle.source_gates();

-- Bootstrap gate status
SELECT * FROM pgtrickle.bootstrap_gate_status();
```

## Combining with the Fuse Circuit Breaker

For extra safety, combine gating with the fuse circuit breaker:

```sql
-- Arm the fuse as a safety net
SELECT pgtrickle.alter_stream_table('order_totals',
    fuse         => 'on',
    fuse_ceiling => 500000
);

-- Gate for controlled loads
SELECT pgtrickle.gate_source('public.orders');
-- ... load data ...
SELECT pgtrickle.ungate_source('public.orders');

-- The fuse catches any unexpected bulk changes outside the gated window
```

## Watermark Gating

Watermark gating extends source gating with **LSN-based coordination**
for more precise control:

```sql
-- Set a watermark — refreshes only consume changes up to this LSN
SELECT pgtrickle.set_watermark('public.orders', pg_current_wal_lsn());

-- Load new data (changes accumulate beyond the watermark)
COPY orders FROM '/data/new_orders.csv' WITH (FORMAT csv, HEADER);

-- Advance the watermark to include the new data
SELECT pgtrickle.advance_watermark('public.orders', pg_current_wal_lsn());

-- Or clear the watermark entirely
SELECT pgtrickle.clear_watermark('public.orders');
```

See the [SQL Reference — Watermark Gating](../SQL_REFERENCE.md#watermark-gating-v070)
for the complete API.

## Further Reading

- [SQL Reference — Bootstrap Source Gating](../SQL_REFERENCE.md#bootstrap-source-gating-v050)
- [SQL Reference — Watermark Gating](../SQL_REFERENCE.md#watermark-gating-v070)
- [SQL Reference — ETL Coordination Cookbook](../SQL_REFERENCE.md#etl-coordination-cookbook-v060)
- [Tutorial: Fuse Circuit Breaker](FUSE_CIRCUIT_BREAKER.md)
