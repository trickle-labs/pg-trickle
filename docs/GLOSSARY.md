# Glossary

A plain-language reference for the terms used throughout the pg_trickle
documentation. If a term isn't here, check the
[FAQ](FAQ.md) — and please [open an issue](https://github.com/trickle-labs/pg-trickle/issues)
so we can add it.

> **How to use this page.** Most pg_trickle pages link the *first* use of a
> jargon term back to the matching entry below. You can also search this page
> directly (the entries are alphabetised within each section).

---

## Core concepts

### Stream table
A table whose contents are defined by a SQL query, and that pg_trickle keeps
up to date automatically as the underlying data changes. Think of it as a
materialized view that maintains itself — without you ever calling
`REFRESH MATERIALIZED VIEW`.

### Defining query
The SQL `SELECT` statement you give to `pgtrickle.create_stream_table()`.
It can use joins, aggregates, CTEs, window functions, and most of standard
SQL. The defining query is what pg_trickle differentiates to compute deltas.

### Source table
A regular PostgreSQL table that a stream table reads from. Source tables
are written to in the normal way (`INSERT`, `UPDATE`, `DELETE`); pg_trickle
captures those writes and propagates them downstream.

### Base table
Synonym for *source table*. Used interchangeably in older docs.

### Schedule
How often a stream table refreshes. May be a duration (`'5s'`, `'10m'`),
a cron expression (`'@hourly'`, `'0 * * * *'`), the special value
`'CALCULATED'` (derived from downstream consumers), or `NULL` (only refresh
when called manually, or for `IMMEDIATE` mode).

### Refresh
A single round of bringing a stream table up to date with its sources. Each
refresh either rewrites the whole result (FULL) or applies only the
incremental change (DIFFERENTIAL).

### Refresh mode
Tells pg_trickle *how* to refresh. The four modes:

- **AUTO** — pick the cheapest mode each cycle (the default).
- **DIFFERENTIAL** — incremental: only changed rows are processed.
- **FULL** — re-run the entire defining query.
- **IMMEDIATE** — refresh inside the same transaction as the source DML
  (no scheduler involved).

### Incremental View Maintenance (IVM)
The technique of updating a materialized view by computing only the
*change* induced by recent edits, rather than re-running the whole query.
pg_trickle is an IVM engine for PostgreSQL.

### Differential
Synonym for "incremental" in the IVM sense. Also the name of the refresh
mode that uses incremental computation. Inspired by *differential dataflow*
and the [DBSP framework](research/DBSP_COMPARISON.md) — see also
[delta query](#delta-query-deltaq) below.

### Delta query (ΔQ)
The SQL pg_trickle generates internally to compute the change in a stream
table given a change in its inputs. ΔQ is derived automatically from the
defining query's operator tree. You can inspect it with
`pgtrickle.explain_st(name)`.

### Operator tree
The internal representation of your defining query — a tree of nodes like
*scan*, *filter*, *join*, *aggregate*. pg_trickle differentiates this tree
operator by operator to derive the delta query.

### DAG (directed acyclic graph)
The shape of your stream-table dependencies. If stream table B reads from
stream table A, there is an edge A → B. pg_trickle refreshes the DAG in
topological order so that downstream tables always see consistent upstream
state.

### Diamond (in a DAG)
A pattern where two parallel branches both depend on a common ancestor and
both feed into a common descendant (A → B → D and A → C → D). pg_trickle
refreshes diamonds atomically to prevent the descendant from seeing one
branch updated and the other not.

### SCC (strongly connected component)
A cycle in the dependency graph — a group of stream tables that all
transitively depend on each other. pg_trickle supports cycles only for
*monotone* queries and only when explicitly enabled
(`pg_trickle.allow_circular`). See
[Circular Dependencies](tutorials/CIRCULAR_DEPENDENCIES.md).

---

## Change capture

### CDC (Change Data Capture)
The mechanism that records every `INSERT`, `UPDATE`, and `DELETE` on a
source table so pg_trickle knows what changed since the last refresh.
pg_trickle has two CDC backends — see [CDC Modes](CDC_MODES.md).

### Trigger-based CDC
The default initial backend. Lightweight `AFTER` statement-level triggers with
transition tables write changed rows to a per-source *change buffer* inside the
writing transaction. Row-level triggers are available with
`pg_trickle.cdc_trigger_mode = 'row'` for legacy or diagnostic use.

### WAL-based CDC
The optional backend that uses PostgreSQL's logical replication to read
changes from the write-ahead log instead of via triggers. Requires
`wal_level = logical`. Adds near-zero write-side cost.

### Hybrid CDC
The default behaviour: pg_trickle starts with triggers (which always work),
and if `wal_level = logical` is available, transitions automatically to WAL
once the first refresh succeeds. If anything goes wrong, it falls back to
triggers.

### Change buffer
A small per-source table in the `pgtrickle_changes.*` schema that holds
captured changes between refreshes. Each refresh drains the relevant rows.

### Compaction
Collapsing redundant entries in a change buffer — for example an
`INSERT` followed by a matching `DELETE` cancels out. pg_trickle compacts
buffers automatically when refreshes are batched.

### Watermark
A timestamp or position published by an external loader that tells
pg_trickle "you can safely consider data through here as complete".
Downstream refreshes wait until all relevant watermarks are aligned. Used
in ETL bootstrap patterns.

### Frontier
A set of per-source positions (LSN or logical timestamps) that records
*where each input was up to* at the moment of the last refresh. The next
refresh reads from frontier→now. Frontiers are how pg_trickle guarantees
correctness across multiple sources.

### LSN (Log Sequence Number)
A PostgreSQL identifier for a position in the write-ahead log. Looks like
`16/B374D8A0`. pg_trickle uses LSNs to record frontiers in WAL CDC mode.

---

## Refresh & scheduling

### CALCULATED schedule
The default. Set an explicit refresh interval only on the *consumer-facing*
stream tables (the ones your application queries). Upstream stream tables
inherit the tightest cadence among their downstream dependents
automatically.

### Tier (Hot / Warm / Cold / Frozen)
A coarse refresh cadence bucket used for very large deployments
(`pg_trickle.tiered_scheduling = on`). Hot tables refresh as scheduled;
Frozen tables never refresh until manually thawed.

### Adaptive fallback
The engine's automatic switch from DIFFERENTIAL to FULL when the change
ratio exceeds a threshold (default 50%). It switches back when the rate
drops.

### Fuse (circuit breaker)
A safety mechanism: a stream table that fails repeatedly is automatically
suspended so it cannot block the scheduler. You re-enable it with
`pgtrickle.reset_fuse()`. See
[Fuse Circuit Breaker](tutorials/FUSE_CIRCUIT_BREAKER.md).

### MERGE
PostgreSQL's `MERGE` statement, which lets pg_trickle apply a delta
(insert / update / delete in one go) to the stream table's storage.
Most of a refresh's wall-clock time is spent in `MERGE` — i.e., in
PostgreSQL itself, not in pg_trickle.

### Scoped recomputation
A delta-application strategy used for `MIN`, `MAX`, and TopK aggregates:
re-aggregate just the *affected groups* (rather than the whole result) by
reading only the rows that match the changed keys.

### Group-rescan
Similar to scoped recomputation, used for "holistic" aggregates like
`STRING_AGG`, `ARRAY_AGG`, `MODE`, `PERCENTILE_*`.

### Predicate pushdown
The optimisation that injects `WHERE` clauses from the defining query
directly into change-buffer scans, so irrelevant changes are filtered out
at read time.

### Columnar tracking
A capture-side optimisation: CDC records only the *columns referenced by
the defining query*, encoded as a bitmask. Updates that touch only
unreferenced columns are skipped entirely.

---

## Background workers

### Launcher
The single per-server background worker that scans `pg_database` every few
seconds and spawns a *scheduler* in every database where pg_trickle is
installed.

### Scheduler
The per-database background worker that wakes periodically, decides which
stream tables are due for refresh, and (in parallel mode) dispatches refresh
jobs to a worker pool.

### BGW (BackGround Worker)
A PostgreSQL concept — a long-running process spawned by the postmaster.
pg_trickle uses BGWs for the launcher, schedulers, and parallel refresh
workers.

### Parallel refresh
An execution mode (`pg_trickle.parallel_refresh_mode = 'on'`) where
independent stream tables in the DAG are refreshed concurrently across a
pool of dynamic background workers.

---

## Aggregates

### Algebraic aggregate
An aggregate that can be maintained from previous state plus a delta
(SUM, COUNT, AVG by tracking sum + count). Cheapest possible IVM.

### Semi-algebraic aggregate
An aggregate that can be maintained on inserts cheaply, but on a delete may
need to rescan the affected group (MIN, MAX). pg_trickle handles this with
*scoped recomputation*.

### Holistic aggregate
An aggregate that has no incremental form (PERCENTILE_*, MODE, STRING_AGG,
ARRAY_AGG). pg_trickle re-aggregates the affected groups from source.

---

## Stream-table features

### Snapshot
A point-in-time copy of a stream table's contents. Useful for
backups, replica bootstrap, or test fixtures. See [Snapshots](SNAPSHOTS.md).

### Outbox
A stream-table-backed implementation of the *transactional outbox*
pattern: write events in the same transaction as your business data;
external systems consume them with at-least-once delivery. See
[Transactional Outbox](OUTBOX.md).

### Inbox
The mirror of the outbox: receive events idempotently from an external
system, with stream tables giving you live views of pending work and a
dead-letter queue. See [Transactional Inbox](INBOX.md).

### Publication (downstream)
A regular PostgreSQL logical-replication publication automatically
created over a stream table's storage. Lets Debezium, Kafka Connect,
Spark, etc. subscribe to stream-table changes without an extra pipeline.
See [Downstream Publications](PUBLICATIONS.md).

### Relay
A standalone Rust binary (`pg-tide-relay`) that bridges outbox/inbox
tables with external messaging systems (NATS, Kafka, Redis Streams,
SQS, RabbitMQ, webhooks). Extracted to the
[`pg_tide`](https://github.com/trickle-labs/pg-tide) project.

### TopK
Stream tables of the form `SELECT … ORDER BY x LIMIT N` (optionally with
`OFFSET M`). pg_trickle stores only the top N rows and recomputes them
incrementally when the changes affect the leaderboard.

### IMMEDIATE mode
A refresh mode that maintains the stream table inside the same transaction
as the source DML — no scheduler, no change buffers. Gives
read-your-writes consistency at the cost of slightly heavier writes.

---

## Engine internals

### DVM (Differential View Maintenance)
The engine inside pg_trickle that turns operator trees into delta queries.
The name is used informally; the academic name for the underlying technique
is *differential dataflow*.

### DBSP
The academic framework that pg_trickle's differentiation rules are based
on. See the [DBSP Comparison](research/DBSP_COMPARISON.md) for the
relationship between pg_trickle and the original DBSP runtime.

### Bilinear expansion
The expansion of `Δ(A ⋈ B) = ΔA ⋈ B + A ⋈ ΔB + ΔA ⋈ ΔB` for joins. This
is the formal recipe for incrementally maintaining a join. You don't need
to know it to use pg_trickle.

### Semi-naive evaluation
A classical algorithm for incremental recursive queries (`WITH RECURSIVE`).
pg_trickle uses it in IMMEDIATE and DIFFERENTIAL modes for insert-only
recursion.

### DRed (Delete-and-Rederive)
A companion algorithm to semi-naive evaluation that handles deletions
inside recursive queries. Also used in IMMEDIATE mode.

### `__pgt_row_id`
A hidden column pg_trickle adds to every stream table to give each row a
stable identity, even if the defining query has no primary key. You can
ignore it in your queries; it is replicated correctly to logical-replication
subscribers.

### Auto-rewrite
A pipeline of small rewrites pg_trickle applies to your defining query
before differentiation — for example expanding `DISTINCT ON` to
`ROW_NUMBER()`, inlining views, or splitting `GROUPING SETS` into
`UNION ALL`. See
[Auto-Rewrite Pipeline](SQL_REFERENCE.md#auto-rewrite-pipeline).

---

## Operations & infrastructure

### GUC (Grand Unified Configuration)
A PostgreSQL configuration variable, set in `postgresql.conf`, by
`ALTER SYSTEM`, or per-session with `SET`. All pg_trickle GUCs start with
`pg_trickle.*`. The full list is in [Configuration](CONFIGURATION.md).

### Advisory lock
A lightweight lock you ask PostgreSQL to keep on your behalf. pg_trickle
uses advisory locks to coordinate refreshes across processes — including
across PgBouncer-pooled sessions, which is why pg_trickle is
pooler-friendly.

### Pooler
Connection pooler such as PgBouncer or pgcat, often deployed in front of
PostgreSQL. pg_trickle's background workers connect directly (not through
the pooler), so your app's pooler does not interact with refresh activity.

### CNPG
[CloudNativePG](https://cloudnative-pg.io/), a Kubernetes operator for
PostgreSQL. pg_trickle ships a minimal `scratch`-based OCI image suitable
for CNPG's Image Volume Extensions feature.

### pgrx
The Rust framework pg_trickle is built with. Provides safe wrappers around
PostgreSQL internals.

### `wal_level`
The PostgreSQL setting that controls how much information the write-ahead
log contains. Values: `minimal`, `replica` (default), `logical`. WAL-based
CDC requires `logical`.

### Replication slot
A PostgreSQL object that retains WAL until a consumer has read it. WAL CDC
mode creates one slot per source table; trigger CDC mode does not.

---

## Acronym key

| Acronym | Meaning |
|---|---|
| BGW | Background worker |
| CDC | Change Data Capture |
| CNPG | CloudNativePG |
| CTE | Common Table Expression (`WITH …`) |
| DAG | Directed Acyclic Graph |
| DBSP | Database Stream Processor (the framework pg_trickle is inspired by) |
| DDL | Data Definition Language (`CREATE`, `ALTER`, `DROP`) |
| DLQ | Dead-Letter Queue |
| DML | Data Manipulation Language (`INSERT`, `UPDATE`, `DELETE`) |
| DRed | Delete-and-Rederive (recursive query algorithm) |
| DVM | Differential View Maintenance (the engine inside pg_trickle) |
| GUC | Grand Unified Configuration (PostgreSQL setting) |
| IVM | Incremental View Maintenance |
| LSN | Log Sequence Number |
| OID | Object Identifier (PostgreSQL row ID for catalog objects) |
| ΔQ | Delta query — the SQL pg_trickle generates to compute changes |
| RLS | Row-Level Security |
| SCC | Strongly Connected Component (a cycle in the DAG) |
| SLA | Service Level Agreement |
| SLO | Service Level Objective |
| SPI | Server Programming Interface (PostgreSQL's in-process query API) |
| SRF | Set-Returning Function |
| ST | Stream Table |
| WAL | Write-Ahead Log |

---

**See also:** [FAQ](FAQ.md) · [SQL Reference](SQL_REFERENCE.md) ·
[Architecture](ARCHITECTURE.md) · [Configuration](CONFIGURATION.md)
