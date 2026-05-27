# pg_trickle × DuckLake Integration Plan

Date: 2026-05-27 (revised)
Status: REMOVED — all DuckLake integration code was removed in v0.76.0.

## Decision (v0.76.0)

All DuckLake-specific code has been removed from pg_trickle. Rationale:

1. **pg_ducklake uses native table AM, not FDW.** The `DUCKLAKE_CHANGE_FEED`
   detection heuristic checked `pg_foreign_data_wrapper.fdwname LIKE '%ducklake%'`
   but pg_ducklake creates tables via `CREATE TABLE ... USING ducklake` (native
   table AM), which never appear in `pg_foreign_table`. The source-side CDC
   adapter was architecturally obsolete.

2. **pg_duckpipe covers the outbound direction.** The pg_duckpipe extension
   (relytcloud) provides dedicated WAL-based PostgreSQL→DuckLake CDC with
   backpressure, per-table flush threads, and sync groups.

3. **Sink code is orthogonal to IVM.** The 1,258-line `ducklake_sink.rs` added
   five heavy Cargo dependencies and an async-in-sync tokio shim that fights
   PostgreSQL's signal handling — all orthogonal to IVM performance.

Users who need DuckLake egress should evaluate `pg_duckpipe` or `pg_tide`.

---

## Historical Context (preserved for reference)

## Executive Summary (for everyone, including non-technical readers)

Imagine that your company keeps two very different kinds of data. Some of it
lives in PostgreSQL — the operational database that runs your application, holds
the orders your customers place, and powers the screens employees stare at all
day. The rest of it lives in a "data lake" — a giant, cheap pile of files on
Amazon S3 that your analytics team queries with tools like DuckDB, Spark, Trino,
or whatever the data team happens to love this quarter. These two worlds rarely
talk to each other in real time. Stitching them together is traditionally an
expensive engineering project involving Kafka, Debezium, Flink, Airflow, and a
small army of people who keep that pipeline alive.

**DuckLake**, released as v1.0 in April 2026 by the DuckDB team, changes the
shape of the problem. It is a new "lakehouse format" — a way to describe data
that lives in cheap Parquet files on S3 — but with one radically simple twist:
all the bookkeeping (which files exist, what schemas they have, who wrote them
when, what the running totals are) lives in a normal SQL database. Most often,
that SQL database is **PostgreSQL**. Suddenly, the catalog of a data lake is
just another set of tables in the same Postgres instance you probably already
run.

**pg_trickle** is a PostgreSQL extension that does something equally simple and
equally radical: it lets you write a SQL query once and have PostgreSQL keep the
result up to date automatically, with sub-second latency, by computing only the
*difference* every time underlying data changes. No external streaming cluster,
no schedulers, no glue code. Just `CREATE STREAM TABLE … AS SELECT …` and your
result table is always fresh.

Put these two together inside the same PostgreSQL instance and a remarkable
thing happens. pg_trickle can *watch* the DuckLake catalog tables, see exactly
which rows in your data lake just changed, and incrementally update aggregations,
joins, dashboards, machine-learning features, and search indexes — all in
PostgreSQL, all in milliseconds, all available to any DuckDB / Spark / Trino
client that already speaks DuckLake. We can also flip the direction: pg_trickle
can publish its incrementally maintained results back into DuckLake as a
brand-new Parquet table, instantly readable by every analytics engine in the
ecosystem.

The DuckLake team's own published roadmap lists "Materialized views and
incremental maintenance" as a future feature they are "looking for funding" to
build. pg_trickle already has a production-grade incremental view maintenance
engine. **This document lays out how the two projects can fit together — what
features we should add to pg_trickle, what tutorials and demos we should write,
and how we can show the DuckLake and broader data-lake community that the
missing piece they have been waiting for already exists.**

---

## Table of Contents

- [Why This Matters: A Plain-English Primer](#why-this-matters-a-plain-english-primer)
- [Background: What Is DuckLake?](#background-what-is-ducklake)
- [Who Is Already Using DuckLake?](#who-is-already-using-ducklake)
- [Architectural Synergies](#architectural-synergies)
- [Integration Opportunities](#integration-opportunities)
  - [INT-1: DuckLake as Stream Table Source (Foreign Table Path)](#int-1-ducklake-as-stream-table-source-foreign-table-path)
  - [INT-2: DuckLake Inlined Data as Native CDC Source](#int-2-ducklake-inlined-data-as-native-cdc-source)
  - [INT-3: DuckLake Change Feed Polling](#int-3-ducklake-change-feed-polling)
  - [INT-4: Stream Table Results Exported to DuckLake](#int-4-stream-table-results-exported-to-ducklake)
  - [INT-5: IVM Engine for DuckLake (v2.0 Collaboration)](#int-5-ivm-engine-for-ducklake-v20-collaboration)
  - [INT-6: DuckLake Metadata Monitoring via Stream Tables](#int-6-ducklake-metadata-monitoring-via-stream-tables)
  - [INT-7: Hybrid OLTP/OLAP Pipeline (One-Box Modern Stack)](#int-7-hybrid-oltpolap-pipeline-one-box-modern-stack)
  - [INT-8: Stream Tables Surfaced as DuckLake Views](#int-8-stream-tables-surfaced-as-ducklake-views)
  - [INT-9: Row-Lineage-Driven Differential Refresh](#int-9-row-lineage-driven-differential-refresh)
  - [INT-10: pg-tide DuckLake Integration (Already Available)](#int-10-pg-tide-ducklake-integration-already-available)
  - [INT-11: Snapshot Provenance & Audit Trails](#int-11-snapshot-provenance--audit-trails)
- [Feature Ideas for pg_trickle](#feature-ideas-for-pg_trickle)
- [Blog Post & Tutorial Ideas](#blog-post--tutorial-ideas)
- [Demo Ideas: Engaging, Shareable, Memorable](#demo-ideas-engaging-shareable-memorable)
- [Implementation Roadmap](#implementation-roadmap)
- [Risks & Considerations](#risks--considerations)
- [Competitive Landscape](#competitive-landscape)
- [Community Engagement Strategy](#community-engagement-strategy)
- [Summary](#summary)

---

## Why This Matters: A Plain-English Primer

Modern analytics architectures suffer from what could politely be called a
"system zoo problem." You start with PostgreSQL because every backend developer
knows it. Then somebody needs reports, so a data warehouse appears. Then they
want fresh data, so Kafka and Debezium are added. Then a stream processor like
Flink is added to compute rolling aggregations. Then a data lake is added to
keep history cheap. Then a catalog service is added to make the data lake
queryable from multiple engines. Then a workflow scheduler is added to glue all
the pieces together. Each of these systems has its own dialect, its own
operations team, its own failure modes, and its own bill from a cloud vendor.

DuckLake and pg_trickle are part of a small but growing movement that says:
maybe we do not need most of that. PostgreSQL is genuinely good. Parquet on S3
is genuinely good. If we use Postgres as both the operational store *and* the
catalog for the data lake, and we put a tiny but extremely fast incremental
compute engine right next to it, we can collapse most of the zoo into a single
boring rectangle on the architecture diagram.

For a non-technical reader, the headline is this: **with pg_trickle and DuckLake
running together, you get sub-second freshness on metrics derived from a data
lake the size of Snowflake, using infrastructure no more exotic than a
PostgreSQL server and an S3 bucket.** That is the story. The rest of this
document is the engineering plan for telling it convincingly and backing it up
with real code.

---

## Background: What Is DuckLake?

DuckLake is a lakehouse format — a specification that describes how to organize
columnar Parquet files on object storage so that they collectively behave like a
real database table, complete with transactions, time travel, schema evolution,
and concurrent multi-engine reads. Where it differs from older lakehouse formats
like Apache Iceberg and Delta Lake is in *where the bookkeeping lives*. Iceberg
and Delta encode their metadata as a maze of JSON and Avro files on the same
blob store as the data, then bolt a catalog database on top to atomically swap
table-version pointers. DuckLake skips the maze entirely and stores all metadata
directly in a standard SQL database. That database can be PostgreSQL, SQLite,
DuckDB, MySQL, or even Google Spanner. The "DuckLake manifesto" sums up the
philosophy in a single line: **SQL as a Lakehouse Format**.

In practice, a DuckLake deployment with PostgreSQL as catalog looks like this:

```
┌──────────────────────────────────────────────────────┐
│              DuckDB / Spark / Trino / DataFusion      │  ← Compute
├──────────────────────────────────────────────────────┤
│                    DuckLake Format                    │
├──────────────┬───────────────────────────────────────┤
│  PostgreSQL  │          Object Storage (S3)          │
│  (Catalog)   │          (Parquet files)              │
│  ~28 tables  │                                       │
└──────────────┴───────────────────────────────────────┘
```

Every committed transaction in DuckLake is represented as a "snapshot" — a few
rows added to the metadata database that say "as of this moment, these are the
files that make up this table." Because snapshots are cheap (just a handful of
rows, not megabytes of JSON), DuckLake can keep millions of them around at the
same time and offer time-travel queries like *"show me the table as it was last
Tuesday at 3pm."*

DuckLake's key features that matter most for the pg_trickle integration are:

- **The `table_changes()` function.** Given a table and two snapshot IDs (or two
  timestamps), DuckLake returns the rows that were inserted, deleted, or updated
  between those two points in time, tagged with `insert`, `delete`,
  `update_preimage`, or `update_postimage`. There are also two cheaper
  specialised variants, `table_insertions()` and `table_deletions()`, when only
  one direction is needed. This is, in effect, a built-in change data capture
  feed sitting on top of the data lake — and it is exactly the shape that
  pg_trickle's differential refresh engine wants to consume.
- **Row lineage via a stable `rowid` virtual column.** DuckLake assigns every
  inserted row a unique identifier that survives updates, compactions, and file
  movements. This is gold for incremental view maintenance: it gives pg_trickle
  a reliable way to recognise the "same row" across snapshots without needing
  primary keys.
- **Data inlining.** When a write touches fewer than ten rows (configurable),
  DuckLake skips writing a Parquet file entirely and stores those rows directly
  in a regular PostgreSQL table named
  `ducklake_inlined_data_table_<id>_<version>`. This means small DuckLake writes
  are literally just `INSERT INTO some_pg_table` — which means pg_trickle's
  standard trigger-based change capture works on them out of the box, with no
  Parquet reading required.
- **Lightweight snapshots.** Each snapshot is a handful of rows. There is no
  expensive snapshot pruning, no manifest rewrite, no compaction cycle required
  just to keep the catalog healthy. This makes snapshot IDs an excellent stable
  pointer for resuming a stream — exactly the role that WAL LSNs play in
  pg_trickle's existing WAL-based CDC path.
- **Built-in SQL views.** DuckLake supports `CREATE VIEW` and persists the
  definition in the `ducklake_view` metadata table. This is interesting because
  it gives us a way to publish pg_trickle stream tables as native DuckLake
  objects — see INT-8.
- **Transparent encryption.** DuckLake can encrypt every Parquet file with a
  per-file key, with the keys stored in the catalog database. If pg_trickle ever
  writes data into DuckLake, it needs to participate in this key management.
- **Multi-engine reads.** Anything DuckLake catalogs can be read by DuckDB,
  Spark, Trino, DataFusion, Pandas, and a growing list of other tools. Whatever
  pg_trickle produces and publishes to DuckLake instantly becomes available to
  the entire ecosystem.

---

## Who Is Already Using DuckLake?

The DuckLake homepage lists a striking number of named production deployments
already — Summer Forever, AlterTable, Windmill, Decision Computing, locals.com,
Summation, Media Cluster Norway, Ascend.io, **PostHog**, Sliplane, the Austrian
Supply Chain Intelligence Institute, and Irion, among others. This list matters
for two reasons. First, it tells us that DuckLake is not a research toy — it is
already running real workloads less than two months after v1.0. Second, almost
every name on that list represents a company whose business depends on
*aggregating events in close-to-real-time* and serving those aggregations to
either internal dashboards or customers.

PostHog, for example, builds product analytics — counting events per user per
funnel step. Windmill is a workflow engine that wants to expose run statistics.
locals.com runs a community platform that surfaces trending content. These are
exactly the use cases where incremental view maintenance is dramatically more
efficient than the alternatives (recompute-from-scratch or pay-Snowflake). If we
can credibly tell each of these companies that pg_trickle gives them
millisecond-fresh metrics over their existing DuckLake without adding a single
new piece of infrastructure, we have a very compelling pitch.

---

## Architectural Synergies

The two projects share an enormous amount of design DNA, and that overlap is
what makes the integration feel almost preordained rather than forced. The
table below maps each dimension where their abstractions meet.

| Dimension | DuckLake | pg_trickle | Synergy |
|-----------|----------|------------|---------|
| **Runs inside PostgreSQL** | Catalog lives in PG | Extension runs in PG | Same process, zero-copy metadata access |
| **Change tracking** | `table_changes()` returns insert/delete/update deltas between snapshots | Trigger / WAL CDC captures row-level changes | DuckLake's change feed is a natural delta source for pg_trickle |
| **Stable row identity** | `rowid` virtual column, preserved across updates and compactions | Differential refresh needs row identity to reconcile deltas | DuckLake's `rowid` is a drop-in replacement for derived row identity, no PK required |
| **Data inlining** | Small changes stored in PG tables (`ducklake_inlined_data_table_*`) | Triggers watch any PG table | pg_trickle can track inlined DuckLake data natively, sub-millisecond latency |
| **Incremental materialized views** | Roadmap item for a future release, currently *"looking for funding"* | Core competency, production-ready | pg_trickle can *be* the IVM engine for DuckLake |
| **Snapshot semantics** | Lightweight, millions supported, integer IDs | Frontier-based version tracking | Snapshot IDs map naturally to frontier positions |
| **Multi-engine reads** | DuckDB, Spark, Trino, DataFusion, Pandas | Exposes results as PostgreSQL tables | Stream table results queryable by every DuckLake client via `postgres_scanner` or via published DuckLake tables |
| **SQL views** | `CREATE VIEW`, stored in `ducklake_view` | Stream tables expose materialised results | We can register a stream table as a DuckLake view that points at the PG result |
| **Statistics** | Table/column/file-level stats in catalog | Uses PG stats for planning | DuckLake stats can guide pg_trickle cost model and refresh-mode selection |
| **ACID** | Via PG transactions | Via PG transactions | Both participate in the same transaction boundaries |
| **Transactional DDL** | Schema changes are snapshots | Stream-table DDL is transactional | Schema evolution can be coordinated atomically |
| **Encryption** | Per-file Parquet encryption, keys in catalog | Currently unaware | Future writer must integrate with key catalog |
| **Iceberg compatibility** | Data files written by DuckLake are Iceberg-readable | — | Anything we ship to DuckLake is also indirectly Iceberg-compatible |

The deepest insight buried in this table is the row at the top: **both systems
already live inside the same PostgreSQL instance.** Every other integration
benefit cascades from that single architectural fact. There are no network
hops, no serialisation costs, no separate authentication, no version-skew
problems between catalog and compute. When DuckLake commits a snapshot,
pg_trickle can see it in the same transaction context. When pg_trickle
publishes a stream table, DuckDB can read it via the same Postgres connection
its catalog already uses.

---

## Integration Opportunities

The integrations below are ordered roughly from "works today with no code" to
"requires substantial new engineering and community alignment." Each one
delivers standalone value, so the roadmap can pick any subset without
sacrificing coherence.

### INT-1: DuckLake as Stream Table Source (Foreign Table Path)

The first integration is the one that already works without changing a single
line of pg_trickle code. Using DuckDB's `postgres_scanner` in reverse —
exposing DuckLake-managed Parquet files as PostgreSQL foreign tables through
`parquet_fdw` or `duckdb_fdw` — we can immediately wrap any DuckLake table in a
pg_trickle stream table. The polling-based CDC path picks up changes by
doing a full scan plus `EXCEPT ALL` diff each cycle.

```sql
-- 1. Install a Parquet-aware FDW
CREATE EXTENSION parquet_fdw;
CREATE SERVER duckdb_server FOREIGN DATA WRAPPER parquet_fdw;

-- 2. Map the DuckLake table as a foreign table
CREATE FOREIGN TABLE events (
    user_id   INT,
    event     TEXT,
    timestamp TIMESTAMPTZ
) SERVER duckdb_server
  OPTIONS (filename 's3://my-lake/data_files/*.parquet');

-- 3. Enable foreign-table polling and build an incremental aggregation
SET pg_trickle.foreign_table_polling = on;
SELECT pgtrickle.create_stream_table(
    name  => 'events_per_user',
    query => $$
        SELECT user_id, COUNT(*) AS event_count
        FROM events
        GROUP BY user_id
    $$,
    schedule     => '30s',
    refresh_mode => 'DIFFERENTIAL'
);
```

This is genuinely useful as-is for read-heavy use cases where the DuckLake table
is updated less often than the dashboard polls. The natural improvement is to
stop relying on full-scan polling and instead consume DuckLake's
`table_changes()` directly — see INT-3.

**Status:** Works today. **Effort:** Low (a tutorial and a documentation page).

### INT-2: DuckLake Inlined Data as Native CDC Source

When DuckLake is configured for data inlining (the default), small writes never
touch Parquet at all. They land in a regular PostgreSQL table named
`ducklake_inlined_data_table_<table_id>_<schema_version>` with this structure:

```sql
ducklake_inlined_data_table_id_schema_version (
    row_id         BIGINT,
    begin_snapshot BIGINT,           -- snapshot when the row was inserted
    end_snapshot   BIGINT,           -- snapshot when the row was deleted, or NULL
    -- one column per user column, matching the table schema
    …
)
```

Because these are *just PostgreSQL heap tables*, pg_trickle's existing
trigger-based CDC mechanism can attach AFTER triggers to them and capture every
write with sub-millisecond latency. A pg_trickle stream table over an inlined
DuckLake table is effectively the same as a stream table over a regular
PostgreSQL table — the catalog database just happens to be the storage layer
for the lake too.

```sql
SELECT pgtrickle.create_stream_table(
    name  => 'active_lake_orders',
    query => $$
        SELECT order_id, customer_id, total
        FROM ducklake_inlined_data_table_5_1
        WHERE end_snapshot IS NULL  -- only live rows
    $$,
    schedule => 'immediate'         -- real-time as DuckLake commits
);
```

The catch is that this fast path only applies while the data remains inlined.
When DuckLake's `ducklake_flush_inlined_data` function (or a `CHECKPOINT`) runs,
the rows move into Parquet files on S3 and the inlined table is truncated.
A complete adapter therefore needs to also consume the flush event and fall back
to either the change-feed adapter (INT-3) or a full re-read of the affected
files. The schema-version suffix also changes whenever the DuckLake table is
altered, so we need to track which inlined table is current.

**Effort:** Medium — a specialised trigger adapter that understands the
`begin_snapshot` / `end_snapshot` convention plus a small DDL watcher.

### INT-3: DuckLake Change Feed Polling

DuckLake's signature feature for change-driven workloads is the family of
`table_changes(tbl, start, end)`, `table_insertions(tbl, start, end)`, and
`table_deletions(tbl, start, end)` functions. They return precisely the deltas
between two snapshots (or two timestamps), with each row tagged as `insert`,
`delete`, `update_preimage`, or `update_postimage`. The output shape is
strikingly close to pg_trickle's internal change buffer format, which means the
translation layer is mostly mechanical.

```
snapshot_id | rowid | change_type        | col1 | col2 | …
-----------+-------+--------------------+------+------+----
3          | 0     | insert             | 42   | foo  |
4          | 1     | update_preimage    | 43   | bar  |
4          | 1     | update_postimage   | 43   | baz  |
5          | 2     | delete             | 44   | qux  |
```

A new CDC adapter — call it `DuckLakeChangeFeed` — would do the following on
each refresh cycle:

1. Look up the last consumed snapshot ID for the source table in a tracking
   table.
2. Invoke `table_changes(tbl, last_consumed_snapshot + 1, current_snapshot())`
   either through `duckdb_fdw`, through an embedded DuckDB instance in a
   background worker, or — most elegantly — by reading the DuckLake metadata
   tables directly with pure SQL (since they all live in the same Postgres
   instance, we can reconstruct the `table_changes()` result without going
   through DuckDB at all).
3. Translate each output row into the appropriate insert/delete/update entries
   in `pgtrickle_changes.changes_<oid>`.
4. Advance the frontier.
5. Let the DVM engine do its normal differential refresh.

The huge win versus the polling approach in INT-1 is that the cost becomes
O(delta) per refresh rather than O(full table). For a DuckLake table with a
billion rows where only a hundred rows changed in the last minute, this is
literally seven orders of magnitude less work. DuckLake also gives us
pre-computed update semantics (the preimage/postimage pair), which removes the
need for pg_trickle to do its own row-matching.

**Effort:** High — a new adapter component with careful snapshot-window
management, but well within reach because we can avoid the DuckDB dependency by
querying the catalog tables directly.

### INT-4: Stream Table Results Exported to DuckLake

Reverse the direction: instead of (or in addition to) materialising a stream
table into a PostgreSQL heap table, write each refresh cycle's delta out as a
Parquet file on S3 and commit a corresponding snapshot in the DuckLake catalog.
The end state is that every DuckLake client — DuckDB, Spark, Trino, DataFusion,
or whatever the analytics team brings next — automatically sees a continuously
updating DuckLake table whose rows are the result of an incremental computation
done inside PostgreSQL.

```
Source Tables (PG)
    │  CDC triggers
    ▼
pg_trickle DVM Engine
    │  computes delta
    ▼
┌─────────────────────────────────┐
│  DuckLake Sink Adapter          │  ← New component
│                                 │
│  • Receives INSERT/DELETE delta │
│  • Writes new Parquet file to   │
│    object storage               │
│  • Commits new snapshot to      │
│    DuckLake catalog (PG tables) │
└─────────────────────────────────┘
    │
    ▼
DuckLake (Parquet on S3 + PG catalog)
    │
    ▼
DuckDB / Spark / Trino consumers
```

The API would look something like this:

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'revenue_by_region',
    query        => $$
        SELECT region, SUM(amount) AS total
        FROM orders JOIN customers USING (customer_id)
        GROUP BY region
    $$,
    schedule     => '5s',
    sink         => 'ducklake',
    sink_options => '{
        "catalog_db": "ducklake_catalog",
        "data_path": "s3://analytics-lake/stream_tables/",
        "table_name": "revenue_by_region",
        "inline_small_changes": true
    }'
);
```

When `inline_small_changes` is true and the delta is below DuckLake's inlining
threshold, the sink writes directly into the catalog rather than uploading a
new Parquet file. This avoids the well-known "small files problem" for
fast-refresh stream tables.

The headline value proposition is direct and memorable: **compute incrementally
in PostgreSQL, serve at data-lake scale.** Each pg_trickle refresh cycle creates
a new DuckLake snapshot, which means downstream consumers can in turn use
`table_changes()` on the stream table's DuckLake representation to drive
*their* incremental processing. The whole architecture starts to compose
recursively.

**Effort:** Very High — needs a Parquet writer (`arrow-rs` is the obvious
choice), an object-store client (`opendal` or `aws-sdk-rust`), encryption
integration, and careful transaction coordination with the DuckLake catalog.
This is multi-month work but unlocks an enormous amount of value.

### INT-5: IVM Engine for DuckLake (v2.0 Collaboration)

DuckLake's roadmap page explicitly lists "Materialized views and incremental
maintenance" under "Future Work / Looking for Funding." This is not a future
they have started building yet — they are inviting somebody to fund or
contribute the work. pg_trickle is uniquely positioned to be that somebody, and
doing so cements pg_trickle as a foundational component of any PostgreSQL-backed
DuckLake deployment rather than just a clever third-party add-on.

There are two viable collaboration models, and they are not mutually exclusive.

#### Option A: pg_trickle becomes the IVM backend for PG-catalog DuckLakes

```
DuckDB client
    │  CREATE MATERIALIZED VIEW … WITH INCREMENTAL MAINTENANCE
    ▼
DuckLake catalog (PG) ──▶ pg_trickle extension
    │                          │
    │  data files (Parquet)    │  monitors ducklake_snapshot_changes
    │                          │  computes delta via DVM engine
    ▼                          │  writes result back to DuckLake
Object Storage                 │
    ▲                          │
    └──────────────────────────┘
```

In this model, when DuckDB writes data to a DuckLake table, pg_trickle (running
in the same PostgreSQL catalog database) detects the snapshot, reads the delta,
applies its DVM operator tree, and writes the result as a new DuckLake snapshot
on a materialised-view table. This is architecturally elegant because both
systems are already coordinated through the same Postgres transaction layer —
the integration is largely about extending the SQL surface of the DuckDB
extension to delegate IVM operations to pg_trickle.

#### Option B: A DuckDB extension that ports the algorithms

The DVM algorithms in pg_trickle — operator-by-operator differentiation rules,
frontier tracking, DAG management — are general. They could be ported to C++ as
a native DuckDB extension, which would work for non-PostgreSQL DuckLake
deployments (SQLite-backed, DuckDB-backed) too. This is a separate project but
would share the same theory and operator design.

| | Option A (PG-side) | Option B (DuckDB extension) |
|---|---|---|
| Leverages existing pg_trickle code | ✅ Directly | ❌ Requires C++ rewrite |
| Works with PG-catalog DuckLakes | ✅ Native | ✅ Via extension |
| Works without PostgreSQL | ❌ | ✅ |
| Performance for huge analytical scans | Moderate (PG storage) | High (DuckDB columnar engine) |
| Time to a working prototype | 3–6 months | 12+ months |

**Recommendation:** pursue Option A first as a proof of concept and a
relationship-builder with the DuckDB Labs team. If the demand and the
collaboration prove fruitful, Option B becomes the natural next step.

### INT-6: DuckLake Metadata Monitoring via Stream Tables

DuckLake's ~28 metadata tables are themselves a rich source of operational
intelligence. Stream tables over them give DuckLake operators something that
does not exist anywhere else today: real-time, SQL-defined observability into
the lake without bolting on Prometheus, Grafana exporters, or a custom polling
script.

```sql
-- Real-time snapshot rate monitoring
SELECT pgtrickle.create_stream_table(
    name  => 'ducklake_snapshot_rate',
    query => $$
        SELECT date_trunc('minute', snapshot_time) AS minute,
               COUNT(*)                            AS snapshots_per_minute,
               COUNT(DISTINCT table_id)            AS tables_changed
        FROM ducklake_snapshot
        GROUP BY 1
    $$,
    schedule => '10s'
);

-- Compaction candidates (tables accumulating many small files)
SELECT pgtrickle.create_stream_table(
    name  => 'ducklake_compaction_candidates',
    query => $$
        SELECT t.table_name,
               COUNT(*)                       AS active_files,
               AVG(ds.row_count)              AS avg_rows_per_file,
               SUM(ds.file_size_bytes)        AS total_size
        FROM ducklake_data_file ds
        JOIN ducklake_table    t  ON t.table_id = ds.table_id
        WHERE ds.deletion_snapshot_id IS NULL
        GROUP BY t.table_name
        HAVING COUNT(*) > 100
           AND AVG(ds.row_count) < 10000
    $$,
    schedule => '5m'
);

-- Per-tenant write activity for multi-tenant DuckLakes
SELECT pgtrickle.create_stream_table(
    name  => 'ducklake_tenant_activity',
    query => $$
        SELECT split_part(t.table_name, '_', 1) AS tenant,
               COUNT(DISTINCT s.snapshot_id)    AS commits_last_hour,
               SUM(df.file_size_bytes)          AS bytes_written
        FROM ducklake_snapshot         s
        JOIN ducklake_snapshot_changes sc USING (snapshot_id)
        JOIN ducklake_data_file        df ON df.added_snapshot_id = s.snapshot_id
        JOIN ducklake_table            t  ON t.table_id = df.table_id
        WHERE s.snapshot_time > NOW() - INTERVAL '1 hour'
        GROUP BY 1
    $$,
    schedule => '30s'
);
```

This is especially powerful for multi-tenant DuckLake deployments where
hundreds of clients are committing concurrently. Operators get a live, queryable
view of who is writing, how much, and where the small-files problem is
brewing.

**Effort:** Low — these are stream tables over already-existing PG tables.
The remaining work is curating a library of useful queries and publishing them
as documentation and a Grafana dashboard pack.

### INT-7: Hybrid OLTP/OLAP Pipeline (One-Box Modern Stack)

The previous integrations combine into a single reference architecture that is
genuinely novel: an OLTP database, a streaming compute engine, and a lakehouse
catalog all running inside one PostgreSQL instance, with Parquet on S3 as the
only external dependency.

```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL                                │
│                                                                 │
│  ┌──────────────┐     ┌──────────────┐     ┌────────────────┐  │
│  │  OLTP Tables │────▶│  pg_trickle  │────▶│ Stream Tables  │  │
│  │  (orders,    │ CDC │  DVM Engine  │delta│ (aggregations) │  │
│  │   products)  │     │              │     │                │  │
│  └──────────────┘     └──────────────┘     └───────┬────────┘  │
│                                                     │           │
│  ┌──────────────────────────────────────────────────┼───────┐  │
│  │            DuckLake Catalog (PG tables)          │       │  │
│  │  ducklake_snapshot, ducklake_data_file, …        │       │  │
│  └──────────────────────────────────────────────────┼───────┘  │
└─────────────────────────────────────────────────────┼──────────┘
                                                      │
                              ┌────────────────────────┘
                              │  Export delta as Parquet
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Object Storage (S3)                           │
│  s3://lake/stream_tables/revenue_by_region/                     │
│    ├── data_files/ducklake-abc123.parquet                       │
│    └── data_files/ducklake-def456.parquet                       │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         ┌─────────┐    ┌─────────┐    ┌─────────┐
         │ DuckDB  │    │  Spark  │    │  Trino  │
         │ (local) │    │(cluster)│    │  (BI)   │
         └─────────┘    └─────────┘    └─────────┘
```

The user story is plain: *"I have an OLTP PostgreSQL database. I want real-time
aggregated views that are also queryable from my data lake by BI tools like
Metabase, Superset, or dbt running on DuckDB."* Today that requires a
five-system pipeline. With pg_trickle and DuckLake it requires one extension
and an S3 bucket.

This is the **killer demo** for the integration — see the Demo Ideas section
below.

### INT-8: Stream Tables Surfaced as DuckLake Views

DuckLake supports first-class SQL views: `CREATE VIEW v AS SELECT …` and the
view definition is persisted in the `ducklake_view` metadata table, after which
every DuckLake client sees `v` as a queryable object. We can exploit this to
make pg_trickle stream tables *look like* native DuckLake objects from the
outside, without any of the consumers needing to know that the underlying
storage is a PostgreSQL heap rather than a Parquet file.

The idea is straightforward. Whenever a stream table is created (or its
definition changes), pg_trickle automatically writes a corresponding row into
`ducklake_view` whose SQL body is something like:

```sql
SELECT * FROM postgres_scan('postgresql://…', 'pgtrickle', 'stream_<name>')
```

A DuckDB client doing `SELECT * FROM my_ducklake.revenue_by_region` then
transparently round-trips through `postgres_scanner` into the live pg_trickle
result table. Combined with INT-4 (which writes Parquet exports), the user has
two options per stream table:

1. **Live view mode** (cheap, always fresh): the DuckLake view points at the PG
   stream table. Reads pull through `postgres_scanner`.
2. **Materialised mode** (cheap for analytics, slightly stale): the DuckLake
   table is real Parquet on S3, refreshed by pg_trickle on every cycle.

The user picks per-stream-table which mode they want. Both modes appear in
DuckLake as ordinary catalog objects.

This is the integration that makes pg_trickle stream tables feel like a native
part of the DuckLake ecosystem rather than an external system that DuckLake
clients have to know about.

**Effort:** Low–Medium — requires writing rows into `ducklake_view` and keeping
them in sync with stream-table DDL.

### INT-9: Row-Lineage-Driven Differential Refresh

One of pg_trickle's harder challenges in differential mode is determining
**row identity** — figuring out which "new" row corresponds to which "old" row
when a row is updated. Without a primary key, the engine has to fall back to
content-hashing or treat updates as delete+insert pairs, both of which are more
expensive than they need to be.

DuckLake solves this for us. Every row in DuckLake has a stable `rowid` that
**survives updates, file movements, and compactions**. When pg_trickle consumes
a delta from `table_changes()`, the `rowid` column gives it exactly the row
identity it needs — for free, with full correctness, and without requiring the
user to declare a primary key (which DuckLake tables today cannot even enforce
yet, since `PRIMARY KEY` constraints are on the roadmap but not implemented).

This makes DuckLake one of the easiest CDC sources pg_trickle could ever
consume from a differential-mode correctness perspective. We should design the
DuckLake CDC adapter (INT-3) to plumb `rowid` straight through into the DVM
engine's row-identity column rather than recomputing it.

**Effort:** Folds naturally into INT-3.

### INT-10: pg-tide DuckLake Integration (Already Available)

**This integration already exists.** The relay functionality that was originally
part of this repository has been extracted into a fully independent project,
[pg-tide](https://github.com/trickle-labs/pg-tide) (`trickle-labs/pg-tide`),
which is a standalone PostgreSQL extension plus a `pg-tide` relay binary for
transactional outbox, idempotent inbox, and relay pipelines. Crucially,
pg-tide's relay already ships with **DuckLake as a named sink backend** in its
object-storage connector tier (alongside Apache Iceberg v2 and Delta Lake v2).

The integration story is therefore not a future engineering project — it is a
configuration exercise that can be demonstrated today:

- **Forward (sink) mode**: use `pgtrickle.attach_outbox()` (available from
  pg_trickle ≥ v0.46.0) to wire a stream table's delta to a pg-tide outbox,
  then configure a pg-tide relay pipeline with the DuckLake sink backend.
  The relay batches rows, writes Parquet to object storage, and commits a
  DuckLake snapshot — all without a single line of custom code.
- **Reverse (source) mode**: configure a pg-tide pipeline pointing at a
  DuckLake source. The relay polls `table_changes()`, transforms the deltas
  into pg-tide's wire format, and delivers them into a pg_trickle inbox table,
  where they are indistinguishable from any other CDC event.

The sidecar architecture retains the two key advantages noted in the original
design. First, expensive operations — Parquet encoding, S3 uploads, network
retries — run in the pg-tide process rather than inside PostgreSQL, so they
cannot affect the shared memory budget or connection pool. Second, the relay
is a clean horizontal scale unit: more throughput means running more pg-tide
replicas, not reconfiguring the database.

The pg-tide project also brings capabilities that the original embedded-relay
concept did not contemplate: OpenTelemetry spans, per-pipeline Prometheus
metrics with tenant labels, a Grafana dashboard, schema-evolution guards with
configurable policies, a dead-letter queue with replay workbench, and an
AsyncAPI 3.0 export for documenting the pipeline contract. All of these are
automatically available to any pg_trickle × DuckLake pipeline that uses pg-tide
as its relay layer.

```sql
-- Wire up the pg_trickle × DuckLake pipeline in four SQL calls:
CREATE EXTENSION pg_tide;
CREATE EXTENSION pg_trickle;

-- 1. Attach an outbox to the stream table
SELECT pgtrickle.attach_outbox('revenue_by_region', retention_hours => 48);

-- 2. Configure the DuckLake sink pipeline
SELECT tide.relay_set_outbox(
    'revenue-to-lake',
    'revenue_by_region',
    'ducklake',
    '{"catalog_db": "ducklake_catalog",
      "data_path": "s3://analytics-lake/stream_tables/",
      "table_name": "revenue_by_region"}'::jsonb
);
-- Start the relay binary:
-- pg-tide --postgres-url "postgres://user:pass@localhost:5432/mydb"
```

**Status:** Works today via pg-tide v0.22.0+. **Effort:** Documentation only —
configuration guide plus an end-to-end tutorial.

### INT-11: Snapshot Provenance & Audit Trails

DuckLake supports per-snapshot commit messages, authors, and arbitrary JSON
`extra_info` via the `set_commit_message` function. When pg_trickle writes a
stream-table delta into DuckLake, it should populate these fields with
operational provenance:

```json
{
  "source_stream_table": "revenue_by_region",
  "pg_trickle_version":  "0.63.0",
  "refresh_mode":        "DIFFERENTIAL",
  "refresh_cycle_id":    "01HXYZ…",
  "delta_rows":          {"insert": 4321, "delete": 12, "update": 7},
  "source_snapshots":    {"orders": 42, "customers": 38},
  "wall_clock_ms":       17
}
```

This gives DuckLake operators an audit trail per snapshot that traces every
materialised change back to the pg_trickle refresh cycle that produced it,
which source-table snapshots fed in, and how long the computation took. It is
trivial to add but transforms DuckLake from "a series of opaque commits" into
"a fully documented history of how every value got there."

**Effort:** Trivial once the sink writer (INT-4 / INT-10) exists.

---

## Feature Ideas for pg_trickle

Building on the integration opportunities, the concrete new features we should
add to pg_trickle are:

### F-1: DuckLake-Aware Polling Adapter (`CdcMode::DuckLakeChangeFeed`)

Replace the generic `EXCEPT ALL` polling for DuckLake foreign tables with a
snapshot-aware adapter that calls `table_changes()` (or queries the equivalent
metadata tables directly) and produces O(delta) work per refresh. Track
`last_consumed_snapshot_id` instead of LSN.

### F-2: DuckLake Sink Output Mode

Add a `sink => 'ducklake'` parameter to `create_stream_table` that writes
results into DuckLake-managed storage. See INT-4.

### F-3: Snapshot-Based Frontier

Extend the frontier model to carry DuckLake snapshot IDs alongside WAL LSNs
and clock-based markers:

```json
{
  "ducklake:lake.events":   {"snapshot_id": 42},
  "ducklake:lake.users":    {"snapshot_id": 38},
  "wal:postgres":           {"lsn": "0/16A4F08"}
}
```

This lets a single stream table mix data from PostgreSQL OLTP tables and
DuckLake tables and still have a coherent consistency story.

### F-4: Parquet Delta Export

For any stream table, allow exporting the computed delta as a Parquet file on
each refresh cycle. This is a building block for both F-2 and INT-10:

```sql
SELECT pgtrickle.alter_stream_table(
    'revenue_by_region',
    delta_export_path => 's3://lake/deltas/revenue_by_region/'
);
```

### F-5: DuckLake Inlined Table Trigger Adapter

A specialised trigger function that understands DuckLake's inlined-data table
schema conventions (`row_id`, `begin_snapshot`, `end_snapshot`) and translates
the begin/end columns into standard INSERT/DELETE change-buffer rows. Includes
a DDL watcher that follows schema-version increments.

### F-6: DuckLake View Registration

When a stream table is created or altered, automatically register (or update) a
matching row in `ducklake_view` so the result is queryable from every DuckLake
client as a native catalog object. See INT-8.

### F-7: Row-ID Plumbing

Extend the DVM engine's row-identity layer to accept a caller-supplied stable
identifier instead of always deriving one. When consuming from a DuckLake
source, plug the DuckLake `rowid` straight through. See INT-9.

### F-8: Snapshot-Window Awareness for Compaction Safety

DuckLake compaction can expire snapshots, after which their changes are no
longer readable via `table_changes()`. The CDC adapter must therefore detect
when the requested start snapshot has been compacted away and either fall back
to a full refresh or raise a clear error so the user can extend the
snapshot-retention window. Expose this as a configurable policy
(`pg_trickle.ducklake_compaction_policy = 'fallback' | 'error'`).

### F-9: Encryption Key Pass-Through

If the source DuckLake is encrypted (per-file Parquet encryption with keys in
`ducklake_data_file.encryption_key`), the writer side of any pg_trickle
integration must generate fresh keys and store them in the catalog the same
way DuckLake itself does. This is a small but essential piece for any real
production user — many lakes will be encrypted from day one.

---

## Blog Post & Tutorial Ideas

Each item below is a self-contained story with a clear audience, a clear
take-away, and an opportunity to publish on Hacker News, /r/dataengineering,
the DuckDB blog cross-link, and the dbt Slack.

### Tutorial 1: "Real-Time Dashboards on Your Data Lake with pg_trickle + DuckLake"

**Audience:** Data engineers using DuckLake who want fresh aggregations.

Start with an empty DuckLake catalog in PostgreSQL. Spin up a DuckDB client that
writes 10 000 synthetic events per second. Show that a naive
`SELECT COUNT(*) GROUP BY user_id` query on DuckLake takes seconds even on a
small dataset. Then add a single `pgtrickle.create_stream_table(…)` call and
watch the same aggregation return in two milliseconds, refreshed automatically
every second. Connect Grafana directly to the PostgreSQL stream table. End with
a screen capture of the dashboard updating in real time as events flow in.

### Tutorial 2: "Incremental Materialized Views for DuckLake — Before v2.0"

**Audience:** DuckLake users who want IVM today and cannot wait for the DuckDB
team to ship native support.

Lead with the DuckLake roadmap quote: *"Materialized views and incremental
maintenance — looking for funding."* Explain in plain English what IVM is and
why DuckLake does not have it natively yet. Walk through a full example: a
DuckLake events table → pg_trickle stream table over it → result published back
as a DuckLake view (INT-8) → queried from DuckDB exactly as if it were a
regular table. Include a benchmark comparing a full refresh against the
differential path on a 100M-row source with a 1 000-row delta.

### Tutorial 3: "The Modern Data Stack in One Box: PostgreSQL + pg_trickle + DuckLake"

**Audience:** Startups and SMBs who want a complete analytics stack without
Kafka, Spark, Airflow, or a vendor bill that doubles every year.

Build out the INT-7 reference architecture end-to-end on a single Postgres
instance. Show OLTP writes, real-time aggregations, time-travel queries, and
multi-engine reads. Close with an architecture-diagram comparison: the
traditional five-system pipeline next to the one-box equivalent, and a list of
ops responsibilities each one removes.

### Tutorial 4: "Streaming PostgreSQL Changes to Your Data Lake — Without Kafka"

**Audience:** Engineers currently running or contemplating a CDC-to-lake
pipeline (Debezium → Kafka → Flink → Iceberg).

Show the same outcome with a single PostgreSQL instance plus pg_trickle's
DuckLake sink. Emphasise the operational footprint (one ALTER vs five clusters),
the failure modes that go away, and the cost difference. Provide a migration
guide for users coming from Debezium.

### Tutorial 5: "Monitoring Your DuckLake with pg_trickle"

**Audience:** DuckLake operators managing multi-tenant data lakes.

Build out a complete Grafana dashboard powered by stream tables over DuckLake
metadata: snapshot rates, storage growth, compaction candidates, tenant
activity, schema-evolution events. Include alert rules for "tenant X writing
abnormally fast" and "table Y accumulating small files."

### Tutorial 6: "Sub-Millisecond DuckLake CDC: The Inlined-Data Fast Path"

**Audience:** Performance-curious engineers who love a deep technical dive.

Walk through how DuckLake inlining works, why the inlined data tables are just
PostgreSQL heap tables, and how pg_trickle's trigger-based CDC achieves
sub-millisecond latency on small DuckLake writes. Show benchmarks against the
Parquet-only path.

### Blog Post: "Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM"

**Audience:** The data-infrastructure community (Hacker News, the dbt Slack,
/r/dataengineering, the Lakehouse Lakehouse newsletter).

Frame the lakehouse-IVM gap, walk through how Materialize, RisingWave, and
Flink each try to solve it with separate infrastructure, and then show how
pg_trickle + DuckLake solve it by *not* adding new infrastructure. Include the
benchmark from Tutorial 2.

### Blog Post: "DuckLake's `table_changes()` Meets pg_trickle's DVM Engine"

**Audience:** Technical readers interested in incremental computation theory.

Deep dive into DuckLake's change-feed format and pg_trickle's change-buffer
format, with the row-by-row translation laid out. Show why the row-lineage
`rowid` column makes DuckLake one of the most natural CDC sources you could
imagine.

### Blog Post: "From Trigger to Snapshot: A pg_trickle × DuckLake Wire-Level Tour"

**Audience:** Distributed-systems-curious engineers.

Trace one INSERT all the way through: trigger fires → change buffer → DVM →
stream-table refresh → Parquet writer → DuckLake snapshot commit → DuckDB
client read. Include a sequence diagram and a Wireshark-style annotated trace.

---

## Demo Ideas: Engaging, Shareable, Memorable

Documentation tells people what is possible. Demos make them want it. Each
demo below is designed to be small enough to fit on a developer's laptop, fun
enough to share on social media, and educational enough to teach the audience
something real along the way.

### Demo A: "The Five-Second Funnel"

Build a tiny e-commerce demo that streams page-view events into a DuckLake
table at high throughput. A pg_trickle stream table computes the classic
funnel — visits → product views → cart adds → checkouts → purchases — with
five-second freshness. A single-page React dashboard shows the funnel updating
live as fake users click around in a side-by-side iframe. The whole thing
fits in a single `docker-compose up` and runs on a laptop. Perfect for
conference talks and the PostHog crowd.

### Demo B: "Time-Travel Debugging for Stream Tables"

Show off DuckLake's time-travel feature in combination with pg_trickle stream
tables. The demo writes a stream of orders, occasionally injects a "bad" order
that breaks a downstream invariant, and then lets the user rewind the stream
table to the snapshot just before the bad order arrived. Highlights the unique
combination of incremental freshness and full historical reproducibility — a
combination no other system on the market offers.

### Demo C: "Multi-Engine Live Leaderboard"

A single stream table maintains the top-100 leaderboard for a fake gaming
service. The leaderboard is exposed simultaneously as (a) a PostgreSQL view,
(b) a DuckLake view (INT-8) read by a DuckDB client, (c) a Parquet snapshot
on S3 read by a Spark notebook, and (d) a Trino query for the BI team. The
audience watches all four interfaces update in lock-step in real time.
Visceral demonstration of the "compute once, serve everywhere" promise.

### Demo D: "DuckLake Observability in a Box"

A pre-packaged Grafana dashboard plus stream tables over DuckLake metadata
(INT-6). The user attaches an existing DuckLake catalog, runs one SQL script,
and immediately gets a production-quality observability dashboard. The pitch
is "five minutes from `git clone` to operational visibility."

### Demo E: "The OLTP-to-Lake Loop Without Kafka"

A reference deployment of the INT-7 hybrid architecture. An app writes orders
to PostgreSQL. pg_trickle computes per-region revenue. The DuckLake sink
publishes the result as a continuously updating DuckLake table. A
Metabase-style dashboard reads the DuckLake table via DuckDB. Compare the
ops checklist to the equivalent Debezium-Kafka-Flink-Iceberg pipeline.

### Demo F: "Bring Your Own DuckLake"

A `pg_trickle-on-DuckLake` template repo — fork it, point it at any existing
PG-catalog DuckLake, run `just demo`, and watch a curated set of example stream
tables come online. Designed for DuckLake users to try the integration on
their *own* data in minutes, without writing any SQL.

### Demo G: "Live Vector Search Over a Lake"

Combines pg_trickle's existing pgvector support with DuckLake. Documents are
ingested into a DuckLake table; an embedding model writes vector columns to a
pg_trickle stream table; queries run against the stream table for low-latency
ANN search. Show how the search index stays fresh in real time as the lake
grows. Perfect for the RAG and AI crowd.

---

## Implementation Roadmap

The roadmap below is intentionally arranged so that every phase delivers
standalone value. We can stop after any phase and still have shipped something
useful to the community.

### Phase 1: Documentation & Awareness (v0.63)

| Item | Type | Effort |
|------|------|--------|
| Blog post: INT-1 (DuckLake as foreign-table source) | Tutorial | 1 day |
| Blog post: INT-6 (monitoring DuckLake metadata) | Tutorial | 1 day |
| Add DuckLake examples to foreign-table-sources.md | Docs | 0.5 day |
| Blog post: "One-box data stack" walkthrough | Tutorial | 2 days |
| Demo A: Five-Second Funnel (containerised) | Demo | 3 days |
| Demo D: DuckLake observability dashboard pack | Demo | 2 days |
| Conference talk submission (DuckCon, PGConf) | Community | 1 day |

### Phase 2: DuckLake-Optimised Polling (v0.64–v0.65)

| Item | Type | Effort |
|------|------|--------|
| F-1: DuckLake change-feed adapter (prototype) | Feature | 1 week |
| F-3: Snapshot-based frontier for DuckLake sources | Feature | 3 days |
| F-5: Inlined-data trigger adapter | Feature | 3 days |
| F-7: Row-ID plumbing | Feature | 3 days |
| F-8: Snapshot-window compaction safety | Feature | 2 days |
| Integration tests with DuckDB + ducklake extension | Tests | 3 days |
| Tutorial 2: "IVM for DuckLake before v2.0" | Tutorial | 2 days |
| Tutorial 6: "Sub-millisecond inlined-data CDC" | Tutorial | 2 days |
| Demo B: Time-travel debugging | Demo | 3 days |

### Phase 3: DuckLake Sink and View Registration (v0.66–v0.68)

| Item | Type | Effort |
|------|------|--------|
| F-4: Parquet delta export (via `arrow-rs`) | Feature | 2 weeks |
| F-2: DuckLake sink mode for stream tables | Feature | 2 weeks |
| F-6: DuckLake view registration | Feature | 1 week |
| F-9: Encryption key pass-through | Feature | 1 week |
| S3 / object-store upload integration | Feature | 1 week |
| DuckLake catalog transaction writer | Feature | 1 week |
| INT-10: pg-tide DuckLake pipeline tutorial (already works) | Tutorial | 2 days |
| INT-11: Snapshot provenance | Feature | 2 days |
| Tutorial 3: "Modern data stack in one box" | Tutorial | 2 days |
| Tutorial 4: "Streaming PG to data lake without Kafka" | Tutorial | 2 days |
| Demo C: Multi-engine leaderboard | Demo | 4 days |
| Demo E: OLTP-to-lake loop | Demo | 3 days |
| E2E tests with DuckLake sink | Tests | 1 week |

### Phase 4: DuckLake v2.0 IVM Partnership (v0.70+)

| Item | Type | Effort |
|------|------|--------|
| INT-5 Option A: Full DuckLake IVM backend | Feature | 2–3 months |
| Engage DuckDB Labs for specification alignment | Community | Ongoing |
| Benchmark suite: IVM over DuckLake at scale | Perf | 2 weeks |
| Joint announcement / blog post / podcast | Community | 1 week |
| DuckCon talk (post-implementation) | Community | 1 week |

---

## Risks & Considerations

### Technical Risks

1. **DuckDB connectivity from PostgreSQL.** Calling DuckLake's `table_changes()`
   normally requires a DuckDB process. Our options, in increasing order of
   independence: use `duckdb_fdw` (exists but maturity varies); embed DuckDB in
   a background worker via the C API (complex but doable); run an external
   sidecar process via pg-tide (see INT-10, already available); or — most elegantly —
   query the DuckLake metadata tables directly with pure SQL, since they all
   live in the same Postgres instance. The pure-SQL path is the most attractive
   long-term answer because it minimises the dependency surface.
2. **Parquet writing from inside Postgres.** The sink integrations require a
   Parquet writer in the extension. The Rust `parquet` crate (part of
   `arrow-rs`) is mature, async-friendly, and works inside pgrx with care.
   Object-store uploads are handled cleanly by `opendal` or `aws-sdk-rust`.
3. **DuckLake schema evolution.** When a DuckLake table is altered, a new
   inlined-data table is created with a fresh `schema_version` suffix. The
   trigger adapter must follow the version change and rewrite its trigger plan
   transactionally. Our existing DDL hook framework already handles this kind
   of transition for native PG tables.
4. **Snapshot retention windows.** DuckLake compaction expires snapshots and
   drops the change-feed history for them. A pg_trickle stream table that
   pauses for longer than the retention window will lose the ability to
   resume incrementally. We need a clear policy (F-8) and operator-facing
   monitoring.
5. **Multi-writer conflicts.** DuckLake uses optimistic concurrency control
   with retry. A pg_trickle writer competing with DuckDB writers for the same
   table must implement the standard retry loop. The DuckLake extension already
   has tunable retry configuration that we should mirror.
6. **Circular dependencies.** If pg_trickle both reads from and writes back to
   DuckLake tables (e.g. a stream table that materialises a roll-up of another
   stream table's DuckLake output), we need clear DAG boundaries to prevent
   refresh loops. pg_trickle's DAG already enforces acyclicity for native
   sources; we extend the same model to DuckLake snapshot dependencies.
7. **Encryption.** Sink writes must mint per-file keys and store them in the
   catalog. Forgetting this would silently break encrypted DuckLakes.
8. **Catalog-database load.** DuckLake's design pushes a great deal of work
   onto the catalog database. pg_trickle running in the same instance must be
   careful with connection pool consumption, lock contention, and bgworker
   priorities. The existing scheduler is a sound starting point but will need
   tuning for the combined workload.

### Strategic Risks

1. **DuckLake v2.0 timeline.** If DuckLake ships native IVM before we deliver
   INT-5, the deepest collaboration window closes. Mitigated by delivering
   Phases 1–3 first, since they are useful regardless of whether DuckLake
   eventually ships native IVM.
2. **Competing IVM implementations.** Materialize, RisingWave, and Feldera all
   have streaming SQL engines. None of them sit inside the catalog database
   today, but any of them could write a DuckLake adapter. Our defensible moat
   is co-location: nothing else lives *in* the catalog the way pg_trickle does.
3. **DuckDB Labs collaboration openness.** INT-5 Option A specifically benefits
   from DuckDB Labs treating pg_trickle as a recommended IVM provider. We
   should engage early through GitHub Discussions, the DuckDB Slack, and the
   DuckCon CFP. The DuckLake roadmap's explicit "looking for funding"
   language for IVM is an open door.
4. **Brand confusion.** The pitch "PostgreSQL extension for data lakes" can
   sound to some audiences like "yet another lake catalog." We need clear
   messaging: pg_trickle is the *incremental compute engine*, DuckLake is the
   *lakehouse format*, together they are *streaming compute on a data lake
   without the streaming infrastructure.*

### Mitigation Strategy

The mitigation strategy follows directly from the phased roadmap: start with
zero-code integrations that ship value immediately (Phase 1), then layer in
optimised adapters that produce big wins on existing setups (Phase 2), then
ship the sink that enables the killer cross-engine story (Phase 3), and only
then attempt the deep IVM-backend collaboration (Phase 4). Each phase ships
working code that is useful on its own. We never bet the project on a single
multi-month deliverable.

---

## Competitive Landscape

| Solution | Approach | Versus pg_trickle + DuckLake |
|----------|----------|------------------------------|
| **Materialize** | Streaming SQL engine | Separate cluster, no lakehouse story, commercial |
| **RisingWave** | Streaming SQL engine | Separate cluster, needs Kafka/Pulsar for ingest |
| **Feldera** | Streaming SQL engine | Separate cluster, no native PG/DuckLake hook |
| **Flink SQL** | Stream processor | JVM cluster, complex ops, no PG-native story |
| **dbt incremental** | Batch incremental | Not true IVM — full scan on a schedule |
| **Iceberg + Flink + catalog** | Lakehouse + stream + service | 5+ systems to operate |
| **Delta Live Tables (Databricks)** | Managed streaming | Proprietary, locked to Databricks |
| **Snowflake Dynamic Tables** | Managed streaming | Proprietary, Snowflake-only |
| **pg_ivm** | In-PG IVM | No lakehouse story, narrower IVM coverage |
| **pg_trickle + DuckLake** | IVM in the catalog DB | Zero additional infra, open formats end-to-end |

The unique positioning is straightforward: **pg_trickle + DuckLake is the only
combination that delivers true differential IVM inside the lakehouse catalog
database itself**, with the entire data plane in open formats. Every other
option on the list either requires running another distributed system or locks
the user into a proprietary cloud platform.

---

## Community Engagement Strategy

Technical integration on its own is not enough — the integration has to be
visible to the people who would benefit from it.

1. **Engage DuckDB Labs and the DuckLake maintainers early.** The roadmap's
   "looking for funding" language on IVM is an explicit invitation. Open a
   GitHub Discussion on `duckdb/ducklake` summarising this plan and asking for
   feedback. Offer to co-author a "DuckLake + pg_trickle reference
   architecture" page on the DuckLake website.
2. **Target the named production users.** PostHog, Windmill, Ascend.io,
   Sliplane, locals.com, and Media Cluster Norway are all listed as production
   DuckLake users on the DuckLake homepage. Reach out individually with a tailored
   pitch ("your funnel/dashboard/workflow workload would benefit from
   millisecond IVM on your existing DuckLake — would you like a hands-on demo?")
3. **Speak at the conferences.** DuckCon (run by the DuckDB Foundation),
   PGConf, PGCon, P99 CONF, and Data Engineering Open Source Summit are all
   natural venues. Submit one talk per phase: "Stream tables on DuckLake" for
   Phase 1, "Differential IVM over DuckLake change feeds" for Phase 2, "The
   one-box modern data stack" for Phase 3.
4. **Cross-publish blog posts.** The DuckDB blog regularly features ecosystem
   integrations. Offer to co-author the "pg_trickle on DuckLake" launch post
   alongside DuckDB Labs.
5. **Sponsor a small DuckLake community moment.** A "pg_trickle × DuckLake
   month" with a daily blog post, a weekly demo livestream, and a community
   Discord/Slack channel would establish credibility quickly.
6. **Maintain a "DuckLake integration" badge on the README.** Make it visible
   that this is a first-class integration, not an afterthought.

---

## Summary

The pg_trickle × DuckLake integration is unusually well-aligned across every
axis that matters — architectural, philosophical, and strategic. DuckLake chose
PostgreSQL as its catalog precisely because PostgreSQL is a great place to
manage transactional metadata at scale. pg_trickle lives in PostgreSQL
precisely because PostgreSQL is a great place to do incremental compute at
scale. The two projects have already, independently, arrived at the same
fundamental bet: that the modern data stack can be much simpler if PostgreSQL
is allowed to take on a larger share of the load.

This document proposes a four-phase roadmap that turns that latent alignment
into shipped software. **Phase 1** publishes documentation, blog posts, and
demos that show what already works — establishing pg_trickle as a recognised
DuckLake collaborator with zero new code. **Phase 2** ships specialised CDC
adapters that consume DuckLake's change feed and inlined data with O(delta)
work per refresh, dramatically outperforming any general-purpose approach.
**Phase 3** introduces the DuckLake sink so that pg_trickle's incrementally
maintained results flow back into the lake as native DuckLake tables, instantly
visible to every analytics engine in the ecosystem. **Phase 4** pursues the
deepest collaboration of all: pg_trickle as the IVM engine that DuckLake's
roadmap is openly inviting somebody to build.

Along the way, the integration unlocks a string of headline stories: real-time
dashboards on data-lake tables; sub-millisecond CDC over DuckLake's inlined
fast path; a one-box modern data stack that replaces the
Debezium-Kafka-Flink-Iceberg pipeline; cross-engine live leaderboards;
time-travel-aware stream tables; and a credible answer to the
"materialised-views-on-the-lake" question that every analytics team eventually
asks.

The fundamental insight that ties it all together remains the simplest possible
sentence: **DuckLake chose PostgreSQL as its catalog. pg_trickle lives in
PostgreSQL. This co-location is a superpower — and the time to exploit it is
now, while DuckLake is still on the v1.0 → v2.0 ramp and the door to deep
collaboration is wide open.**
