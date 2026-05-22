# Partitioned Tables as Sources

This tutorial shows how pg_trickle works with PostgreSQL's declarative table
partitioning. It covers RANGE, LIST, and HASH partitioned source tables,
explains what happens when you add or remove partitions, and documents
known caveats.

## Background

PostgreSQL lets you split large tables into smaller "partitions" — for
example one partition per month for an `orders` table. This is a common
technique for managing very large datasets. pg_trickle handles partitioned
source tables transparently:

- **CDC triggers fire on all partitions.** pg_trickle installs statement-level
  AFTER triggers directly on the partitioned root table. PostgreSQL routes all
  DML — whether targeted at the root or at a child partition directly — through
  the root's triggers. The trigger's `REFERENCING NEW TABLE / OLD TABLE`
  transition tables contain all affected rows regardless of which partition was
  written to. All captured changes are recorded in a single change buffer keyed
  by the parent table's OID.

- **ATTACH PARTITION is detected automatically.** When you add a new
  partition with pre-existing data, pg_trickle's DDL event trigger detects
  the change and marks affected stream tables for reinitialization. No
  manual intervention required.

- **WAL-based CDC works correctly.** When using WAL mode, publications are
  created with `publish_via_partition_root = true` so all partition changes
  appear under the parent table's identity.

## Example: Monthly Sales Partitions (RANGE)

```sql
-- Create a RANGE-partitioned source table
CREATE TABLE sales (
    id         SERIAL,
    sale_date  DATE    NOT NULL,
    region     TEXT    NOT NULL,
    amount     NUMERIC NOT NULL,
    PRIMARY KEY (id, sale_date)
) PARTITION BY RANGE (sale_date);

-- Create partitions for each half of the year
CREATE TABLE sales_h1_2025 PARTITION OF sales
    FOR VALUES FROM ('2025-01-01') TO ('2025-07-01');
CREATE TABLE sales_h2_2025 PARTITION OF sales
    FOR VALUES FROM ('2025-07-01') TO ('2026-01-01');

-- Insert data across partitions
INSERT INTO sales (sale_date, region, amount) VALUES
    ('2025-02-15', 'US', 100.00),
    ('2025-05-20', 'EU', 250.00),
    ('2025-08-10', 'US', 175.00),
    ('2025-11-30', 'EU', 300.00);

-- Create a stream table over the partitioned source
SELECT pgtrickle.create_stream_table(
    name  => 'regional_sales',
    query => $$
        SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
        FROM sales
        GROUP BY region
    $$,
    schedule     => '1 minute',
    refresh_mode => 'DIFFERENTIAL'
);

-- Refresh to populate
SELECT pgtrickle.refresh_stream_table('regional_sales');

-- Verify — aggregates span all partitions:
SELECT * FROM regional_sales ORDER BY region;
--  region | total  | cnt
-- --------+--------+-----
--  EU     | 550.00 |   2
--  US     | 275.00 |   2
```

## Adding New Partitions

When you add a new partition, any new rows inserted through the parent are
automatically captured by CDC triggers. The trigger on the parent is cloned
to the new partition by PostgreSQL.

```sql
-- Add a new partition for 2026
CREATE TABLE sales_h1_2026 PARTITION OF sales
    FOR VALUES FROM ('2026-01-01') TO ('2026-07-01');

-- Inserts into the new partition are captured normally
INSERT INTO sales (sale_date, region, amount)
    VALUES ('2026-03-15', 'US', 400.00);

-- Next refresh picks up the new row
SELECT pgtrickle.refresh_stream_table('regional_sales');

SELECT * FROM regional_sales ORDER BY region;
--  region | total  | cnt
-- --------+--------+-----
--  EU     | 550.00 |   2
--  US     | 675.00 |   3
```

## ATTACH PARTITION with Pre-Existing Data

The most important edge case: attaching a table that already contains rows.
These rows were never seen by CDC triggers, so the stream table would be
stale. pg_trickle detects this automatically.

```sql
-- Create a standalone table with existing data
CREATE TABLE sales_h2_2026 (
    id        SERIAL,
    sale_date DATE    NOT NULL,
    region    TEXT    NOT NULL,
    amount    NUMERIC NOT NULL,
    PRIMARY KEY (id, sale_date)
);
INSERT INTO sales_h2_2026 (sale_date, region, amount) VALUES
    ('2026-08-01', 'EU', 500.00),
    ('2026-09-15', 'US', 200.00);

-- Attach it to the partitioned table
ALTER TABLE sales ATTACH PARTITION sales_h2_2026
    FOR VALUES FROM ('2026-07-01') TO ('2027-01-01');

-- pg_trickle detects the partition change and marks the stream table
-- for reinitialize. Check:
SELECT pgt_name, needs_reinit
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'regional_sales';
--  pgt_name        | needs_reinit
-- -----------------+--------------
--  regional_sales  | t

-- The next refresh reinitializes — re-reading all data from scratch:
SELECT pgtrickle.refresh_stream_table('regional_sales');

SELECT * FROM regional_sales ORDER BY region;
--  region | total   | cnt
-- --------+---------+-----
--  EU     | 1050.00 |   3
--  US     |  875.00 |   4
```

## DETACH PARTITION

When you detach a partition, the detached table's data is no longer visible
through the parent. pg_trickle detects this too and marks stream tables for
reinitialize.

```sql
-- Archive the old partition
ALTER TABLE sales DETACH PARTITION sales_h1_2025;

-- Stream table is marked for reinit:
SELECT pgt_name, needs_reinit
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'regional_sales';
--  pgt_name        | needs_reinit
-- -----------------+--------------
--  regional_sales  | t

-- After refresh, the detached partition's rows are gone:
SELECT pgtrickle.refresh_stream_table('regional_sales');
SELECT * FROM regional_sales ORDER BY region;
-- (only rows from remaining partitions)
```

## LIST Partitioning

LIST partitioning splits rows by discrete values. It works identically:

```sql
CREATE TABLE events (
    id      SERIAL,
    region  TEXT NOT NULL,
    payload TEXT,
    PRIMARY KEY (id, region)
) PARTITION BY LIST (region);

CREATE TABLE events_us PARTITION OF events FOR VALUES IN ('US');
CREATE TABLE events_eu PARTITION OF events FOR VALUES IN ('EU');
CREATE TABLE events_ap PARTITION OF events FOR VALUES IN ('AP');

SELECT pgtrickle.create_stream_table(
    name  => 'event_counts',
    query => 'SELECT region, count(*) AS cnt FROM events GROUP BY region',
    schedule => '1 minute'
);
```

## HASH Partitioning

HASH partitioning distributes rows across a fixed number of partitions.
Useful for spreading write load evenly:

```sql
CREATE TABLE metrics (
    id        SERIAL PRIMARY KEY,
    sensor_id INT    NOT NULL,
    value     DOUBLE PRECISION
) PARTITION BY HASH (id);

CREATE TABLE metrics_0 PARTITION OF metrics
    FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE metrics_1 PARTITION OF metrics
    FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE metrics_2 PARTITION OF metrics
    FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE metrics_3 PARTITION OF metrics
    FOR VALUES WITH (MODULUS 4, REMAINDER 3);

SELECT pgtrickle.create_stream_table(
    name  => 'sensor_avg',
    query => $$
        SELECT sensor_id, AVG(value) AS avg_val, COUNT(*) AS cnt
        FROM metrics GROUP BY sensor_id
    $$,
    schedule => '1 minute'
);
```

## Foreign Tables

Tables from other databases (via `postgres_fdw`) can be used as sources,
but with restrictions:

- **No trigger-based CDC** — foreign tables don't support row-level triggers.
- **No WAL-based CDC** — foreign tables don't generate local WAL.
- **FULL refresh works** — `SELECT *` executes a remote query each time.
- **Polling-based CDC works** — when `pg_trickle.foreign_table_polling` is
  enabled, pg_trickle creates a local snapshot table and detects changes via
  `EXCEPT ALL` comparison.

When you use a foreign table as a source, pg_trickle emits an info message
explaining the limitations:

```sql
CREATE EXTENSION postgres_fdw;

CREATE SERVER remote_db
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'remote-host', dbname 'analytics');

CREATE USER MAPPING FOR CURRENT_USER
    SERVER remote_db OPTIONS (user 'reader');

CREATE FOREIGN TABLE remote_orders (
    id     INT,
    amount NUMERIC
) SERVER remote_db OPTIONS (table_name 'orders');

-- Only FULL refresh is available:
SELECT pgtrickle.create_stream_table(
    name  => 'remote_totals',
    query => 'SELECT SUM(amount) AS total FROM remote_orders',
    schedule     => '5 minutes',
    refresh_mode => 'FULL'
);
-- INFO: pg_trickle: source table remote_orders is a foreign table.
-- Foreign tables cannot use trigger-based or WAL-based CDC —
-- only FULL refresh mode or polling-based change detection is supported.
```

## Known Caveats

| Caveat | Description |
|--------|-------------|
| **PostgreSQL 13+ required** | Parent-table triggers only propagate to child partitions on PG 13+. pg_trickle targets PostgreSQL 18, so this is always satisfied. |
| **Partition key in PRIMARY KEY** | PostgreSQL requires the partition key to be part of any unique constraint. This means your `PRIMARY KEY` must include the partition column. |
| **ATTACH with data = reinitialize** | Attaching a partition with pre-existing rows triggers a full reinitialize on the next refresh. For very large tables, this may be slow. Consider gating the source with `pgtrickle.gate_source()` during bulk partition operations. |
| **Sub-partitioning** | Multi-level partitioning (partitions of partitions) works in principle because triggers propagate through the entire hierarchy, but it is not extensively tested. |
| **pg_partman compatibility** | `pg_partman` dynamically creates and drops partitions. Since pg_trickle detects ATTACH/DETACH via DDL event triggers, it should work, but this combination is not yet tested. |
| **Partitioned storage tables** | Using a partitioned table as the stream table's *storage* is not supported. This is tracked for a future release. |
| **DETACH PARTITION CONCURRENTLY** | `DETACH PARTITION ... CONCURRENTLY` is a two-phase operation. The DDL event trigger fires after the first phase; the partition is not fully detached until the second phase commits. The stream table may briefly reflect the old partition count. |
