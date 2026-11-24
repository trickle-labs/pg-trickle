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

- **CDC captures writes routed through the source.** With statement-level CDC,
  PostgreSQL fires the trigger for the relation named by the statement; a
  statement directed at a child does not fire its parent's statement trigger.
  Retention-enabled sources use row-level capture, whose triggers PostgreSQL
  clones to partitions, so direct child writes are captured as well. Changes
  use a single change buffer keyed by the source table's OID.

- **ATTACH PARTITION is detected automatically.** When you attach a
  partition with pre-existing data, pg_trickle marks affected stream tables
  for reinitialization; an empty attach stays active. A partition created with
  `CREATE TABLE ... PARTITION OF` is also recorded immediately so that a
  later direct DROP cannot silently leave stale aggregates.

- **Partitioned sources currently use trigger CDC.** Although PostgreSQL
  supports `publish_via_partition_root`, the WAL cutover recheck does not
  yet admit partitioned parents. An explicit `cdc_mode => 'wal'` request
  retains trigger capture and reports the eligibility reason through
  `pgtrickle.check_cdc_health()`.

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

## Retention policies

Explicit retention policies are disabled by default. A newly initialized or
protectedly reinitialized partition-aligned managed DIFFERENTIAL stream can automatically delete groups
when their source partition is removed, without configuring `TRIM` or a cutoff.
The initial FULL baseline must establish complete partition CDC, truncate guards
and a suitable output index under source locks. Streams that do not meet these
requirements use the protected full-reinitialization fallback instead. This
fast path is internal: `get_stream_table_partition_retention` reports
`enabled: false` and `automatic_delete_with_source: true` when it is armed.
A caller may still explicitly configure `TRIM` or `RETAIN`, optionally
with a cutoff. Setting the policy to `OFF` disables automatic delete-with-source,
including across subsequent protected rebuilds. When a cutoff is provided, a partition is
eligible only when its entire half-open range `[lower, upper)` is below the
cutoff. Advancing the cutoff does not remove a partition; it makes a subsequent
`DETACH`, concurrent `DETACH`, or direct `DROP TABLE` eligible for retention
processing. Omitting the cutoff opts into **source-driven removal**: the
partition's disappearance is the signal, and any removable, aligned partition
can be trimmed or sealed regardless of its timestamp. Existing policies with
explicit cutoffs retain their cutoff checks for compatibility.

This example uses the **same** `occurred` column for partitioning and hourly
grouping. It does not infer any relationship between two timestamps. Run it in
a database with pg_trickle installed:

```sql
CREATE TABLE retention_events (
    occurred timestamp NOT NULL,
    account_id integer NOT NULL,
    amount numeric NOT NULL
) PARTITION BY RANGE (occurred);
CREATE TABLE retention_events_old PARTITION OF retention_events
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE retention_events_live PARTITION OF retention_events
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
INSERT INTO retention_events VALUES
    ('2025-01-15 12:05', 1, 10), ('2025-01-15 12:30', 1, 5),
    ('2025-02-15 09:10', 1, 20), ('2025-02-15 09:20', 2, 7);

SELECT pgtrickle.create_stream_table_with_retention(
    'hourly_trim',
    'SELECT pg_catalog.date_trunc(''hour'', occurred) AS hour, account_id,
            sum(amount) AS total, count(*) AS n
       FROM retention_events GROUP BY 1, 2',
    'TRIM', 'occurred', '2025-02-01', '1h'
);
SELECT pgtrickle.create_stream_table_with_retention(
    'hourly_history',
    'SELECT pg_catalog.date_trunc(''hour'', occurred) AS hour, account_id,
            sum(amount) AS total, count(*) AS n
       FROM retention_events GROUP BY 1, 2',
    'RETAIN', 'occurred', '2025-02-01', '1h'
);

ALTER TABLE retention_events DETACH PARTITION retention_events_old;
SELECT hour, account_id, total, n FROM hourly_trim ORDER BY 1, 2;
-- 2025-02-15 09:00:00 | 1 | 20 | 1
-- 2025-02-15 09:00:00 | 2 |  7 | 1
SELECT hour, account_id, total, n FROM hourly_history ORDER BY 1, 2;
-- The same two rows plus 2025-01-15 12:00:00 | 1 | 15 | 2.
SELECT pgtrickle.get_stream_table_partition_retention('hourly_history');
```

For `TRIM`, pg_trickle first brings the stream current under its existing CDC
locking protocol, then removes the affected output groups using an index. The
remaining groups continue to receive differential updates. An ineligible
removal, including a partition that crosses the cutoff, falls back to normal
reinitialization. A stream with user triggers or output-delta consumers cannot
currently enable TRIM.
During retention catch-up and TRIM deletion, pg_trickle temporarily enables
partition pruning, index and bitmap scans, discourages sequential scans, and disables JIT. It
then restores the caller's planner settings.
If stream storage is partitioned solely by the proven retention group key,
aggregate, DML, and downstream capture lookups include that key so PostgreSQL
can prune output partitions. The retention planner scope forces parameterized
nested-loop index probes for this DML and restores the caller's planner
settings on success or error. Other storage keys keep the ordinary identity
probe and may visit more output partitions.

For `RETAIN`, removal seals the expired range and preserves its aggregate
rows. Later differential refreshes continue to update live groups. A protected
FULL refresh or reinitialization can rebuild **only unsealed groups** from the
surviving source partitions while leaving sealed aggregate rows in place.
The persisted list of sealed ranges, **not the retention cutoff**, defines
which groups must be preserved: a partition below the cutoff that has not
been detached or dropped is still reconstructible. The scoped replacement
keeps the protected source-lock/frontier protocol and emits output changes
for downstream stream tables.

Sealed history cannot be reconstructed from surviving raw rows. Rebuilds fail
closed when the defining-query contract, partition alignment, capture guards,
or durable storage proof cannot be verified or a partition detach is pending.
Only a narrowly compatible query change is permitted after sealing: the
source, grouping, aggregate expressions, projected output columns and CDC
capture coverage must remain identical, while the source-level `WHERE`
predicate may change. `alter_stream_table(..., query => ...)` then performs
an in-place, protected rebuild of **unsealed** groups. The historical rows
keep their original values even if the new filter would have excluded them:
this is a prospective change to the live range, **not** a retroactive edit to
sealed aggregates. An output-schema, aggregate, grouping, or source change,
policy removal, destructive storage replacement or incompatible restore
continues to fail closed. Save raw events externally if you must correct
sealed aggregates; deleting and recreating a stream deliberately discards
its retained history.

An initialized DIFFERENTIAL stream with complete CDC capture can be paused
without invalidating its frontier. Capture remains active while it is
suspended; `resume_stream_table` keeps the frontier and processes pending
changes in the next differential refresh. If a source DDL or lost CDC
invalidates replay, resumption instead requires a protected rebuild of the
reconstructible live range. Ineligible operations fail closed rather than
discarding sealed rows. Monitoring and sizing the change buffer is important
during long pauses. The whole-table TRUNCATE optimization is never used on
a stream with sealed history.

The contract is intentionally narrow:

- The source is one logged, trigger-CDC RANGE-partitioned table with one
  non-null `date`, `timestamp`, or `timestamptz` partition column, the default
  btree operator class, finite bounds, and no default partition. Descendants
  must be durable local tables.
- The defining query must admit **partition-separable** grouping: the
  supported single-source grouped aggregate has a uniquely projected partition
  key (or a bucket of that very key aligned to source partition boundaries).
  Other grouping expressions may involve other source columns, including
  hourly buckets of an event timestamp different from the date partition key;
  they cannot combine output groups across partitions. Constant or calculated
  SELECT items do not change this proof. Query operators with cross-partition
  dependencies, such as joins, window functions, global limits and unrelated
  subqueries, are still rejected by the retention proof.
- Other grouping expressions, projections and PostgreSQL built-in aggregates
  must be supported by ordinary DIFFERENTIAL maintenance, including its
  immutability and typed row-identity requirements. Floating-point `SUM` is
  rejected because rounding depends on change batching. Grouping columns
  using nondeterministic collations are rejected. The defining search path must
  leave `pg_catalog` ahead of other schemas, and the stream owner must not be
  subject to source row-level security. The output partition identity remains
  a direct projected group column so that the retention delete can use a typed
  half-open range predicate.
- Hourly `timestamp` partition-key buckets use
  `pg_catalog.date_trunc('hour', key)`; `timestamptz` partition-key buckets
  must use `pg_catalog.date_trunc('hour', key, 'UTC')`. Partition bounds must
  align with the bucket boundaries. A bucket of an *unrelated* timestamp
  column is allowed only as an **additional** group key when the partition
  key itself is also projected.
- An optional source-level `WHERE` filter is allowed; `HAVING` and
  predicates that mix groups across partitions are not. An ALTER of this
  filter must not require newly uncaptured source columns.
- The source workload may append rows, but grouped output rows are updated;
  this feature does not require the stream's append-only output fast path.
- Retention cuts require the configured stream to be active and initialized
  with no pending reinitialization. Resume and repair it before removing a
  partition if it has been paused or suspended.
- Keep the stream table's valid, non-partial btree row-identity index on `__pgt_row_id` or
  `pgtrickle.row_probe_v1(__pgt_row_id)`. A missing or invalid index rejects
  the retention cut; restore its original definition, including uniqueness
  when required, before retrying. Keyed, unpartitioned streams require an
  immediate unique index whose only key is `__pgt_row_id`; keyless or
  partitioned streams can use a non-unique identity index.
- `RETAIN` rejects overlapping historical attachments, including empty
  partitions, and nonempty attachments that would require rebuilding sealed
  history. Late arrivals or backfills into a sealed range therefore require a
  deliberate external rebuild. Ordinary writes to still-attached ranges remain
  supported. Eligible `DETACH PARTITION CONCURRENTLY` operations suspend the
  affected streams while PostgreSQL's detach is pending, then replay captured
  changes and apply the retention cut in its second transaction. If that
  transaction is interrupted, run PostgreSQL's `DETACH PARTITION ... FINALIZE`
  to complete the cut; the pending range is visible as `pending_detach` in the
  policy JSON. Partition attachments and retention configuration changes are
  blocked while the cut is pending. A dependent remains suspended if another
  upstream still requires recovery; repair that upstream before resuming it.
  A nested child can be removed when every
  intermediate partition level is RANGE-partitioned
  on the same key with finite, bucket-aligned bounds; the effective range is
  the intersection of its bounds with all ancestor bounds. For other nested
  layouts, TRIM falls back and RETAIN rejects the removal. Removing an entire
  direct child that itself contains subpartitions is also supported.

Enabling retention switches the shared source to row-level CDC and performs a
protected baseline reinitialization. This captures any earlier direct-child
writes that statement-level CDC could have missed. It may scan the full source
once during configuration. Refreshes fail closed if the required capture
triggers or partition TRUNCATE guards are missing or disabled. While retention
is configured, TRUNCATE of an attached partition is rejected; use supported
row-level DELETEs or partition retention instead. Writes are rejected while
CDC is paused in discard mode, which would otherwise lose their changes. A
detached table can be modified independently.

If an existing policy used a nondeterministic grouping collation or a
floating-point `SUM`, refresh now fails closed. Its existing groups may already
be incorrect; changing the definition alone does not repair them. Restore the
complete raw history and build a replacement stream with deterministic
grouping and exact aggregate inputs. If RETAIN has sealed history, do not
discard the old stream until the replacement has been checked.

The optional retention cutoff is parsed as the source column type. Include a
timezone offset for `timestamptz` cutoffs to avoid dependence on the session
timezone. A missing cutoff is represented as `null` by
`get_stream_table_partition_retention`; the internal typed sentinel is not a
time boundary and is never used as an expiry gate.
For an existing initialized managed differential stream, use
`pgtrickle.set_stream_table_partition_retention(name, policy, column, cutoff)`;
the API validates the existing source, query, storage and partition layout.
Use the same function with a later cutoff to advance it. `OFF` disables a
policy only before any RETAIN range has been sealed. Upgrading leaves existing
streams disabled until explicitly configured. Inspect the JSON status and
refresh history when diagnosing a rejected operation or fallback.

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

## DROP TABLE of an attached partition

A direct `DROP TABLE` of a partition does not generate row-level DELETE
events. pg_trickle records the partition OIDs in its source dependency
snapshots and detects their removal at the `sql_drop` event. The parent
stream table is marked `needs_reinit = true`; the next refresh rebuilds
its results from the surviving source rows, including downstream streams.

```sql
DROP TABLE sales_h1_2025; -- no preceding DETACH is needed

SELECT pgt_name, status, needs_reinit
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'regional_sales';
-- status ACTIVE, needs_reinit true

SELECT pgtrickle.refresh_stream_table('regional_sales');
```

This detection also covers nested partitions. Partition DDL is not a
row-level change, so materialized results may remain from the previous
refresh until the rebuild completes; monitor `needs_reinit` when performing
retention jobs. **IMMEDIATE-mode streams are suspended instead**: they have
no scheduled refresh worker, so explicitly call
`pgtrickle.resume_stream_table('name')` after the partition operation to
rebuild their state under source locks.

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
| **DETACH PARTITION CONCURRENTLY** | This is a two-phase operation. Without retention, dependent stream tables are suspended and must be resumed explicitly after detach; resume schedules a full reinitialization. Eligible retention-enabled streams replay CDC and complete their cut in the second phase. If interrupted, run `DETACH PARTITION ... FINALIZE`. |
