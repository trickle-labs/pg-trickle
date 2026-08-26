# Frequently Asked Questions

This FAQ covers everything from core concepts and getting started, through SQL
support details, to operational topics like deployment, monitoring, and
troubleshooting. Use the table of contents below to jump to a specific topic.

---

## New User FAQ — Top 15 Questions

New to pg_trickle? Start here. Each answer is a short summary with a link to
the full explanation further down.

### 1. What is pg_trickle?

A PostgreSQL 18 extension that adds **stream tables** — materialized views that
refresh themselves incrementally, processing only changed rows instead of
re-running the entire query. [Full answer →](#what-is-pg_trickle)

### 2. How is this different from a materialized view?

Stream tables refresh automatically on a schedule, support incremental
(differential) refresh, track changes via CDC triggers, and propagate updates
through dependency chains — none of which `REFRESH MATERIALIZED VIEW` provides.
[Full answer →](#what-is-the-difference-between-a-stream-table-and-a-regular-materialized-view-in-practice)

### 3. How do I install pg_trickle?

Install from the Docker image, PGXN, or build from source. Add
`shared_preload_libraries = 'pg_trickle'` to `postgresql.conf`, then
`CREATE EXTENSION pg_trickle;` in each database. [Full answer →](#how-do-i-install-pg_trickle)

### 4. How do I create my first stream table?

One function call: `SELECT pgtrickle.create_stream_table(name => 'my_st', query => 'SELECT ...', schedule => '5s');`
See the [Getting Started guide](GETTING_STARTED.md) for a walkthrough.
[Full answer →](#how-do-i-create-a-stream-table)

### 5. What is the difference between FULL and DIFFERENTIAL refresh?

**FULL** re-runs the entire defining query. **DIFFERENTIAL** reads only the
changed rows from the change buffer and computes the delta — orders of magnitude
faster for small changes on large tables. AUTO mode picks the best strategy per
cycle. [Full answer →](#what-is-the-difference-between-full-and-differential-refresh-mode)

### 6. Which refresh mode should I use?

Use **AUTO** (the default) — it selects DIFFERENTIAL when possible and falls
back to FULL when needed. Use **IMMEDIATE** for same-transaction consistency.
Use **FULL** only when the defining query uses volatile functions or is not
IVM-eligible. [Full answer →](#when-should-i-use-full-vs-differential-vs-immediate)

### 7. What SQL features are supported?

Joins (INNER, LEFT, RIGHT, FULL OUTER, CROSS, LATERAL), aggregates (60+
functions including SUM, COUNT, AVG, array_agg, jsonb_agg), CTEs (including
recursive), window functions, UNION/INTERSECT/EXCEPT, subqueries, CASE, COALESCE,
DISTINCT, GROUP BY with ROLLUP/CUBE/GROUPING SETS, and more.
[Full answer →](#what-sql-features-are-supported-in-defining-queries)

### 8. How fresh is my stream table data?

As fresh as the refresh schedule allows. With a `1s` schedule, data is typically
< 2 seconds stale. With IMMEDIATE mode, data is updated within the same
transaction as the source write. [Full answer →](#how-stale-can-a-stream-table-be)

### 9. Can I chain stream tables (ST reads from another ST)?

Yes — stream tables can reference other stream tables. pg_trickle builds a
dependency DAG and refreshes them in topological order automatically.
[Full answer →](#can-a-stream-table-reference-another-stream-table)

### 10. How does change data capture work?

Trigger-based CDC captures every INSERT, UPDATE, and DELETE into per-table
change buffers. Statement-level triggers are the default, with row-level
triggers available via `pg_trickle.cdc_trigger_mode = 'row'`. If
`wal_level = logical` is available,
pg_trickle can automatically transition to WAL-based CDC for near-zero
write-path overhead. [Full answer →](#change-data-capture-cdc)

### 11. Do I need `wal_level = logical`?

No. pg_trickle works with the default `wal_level = replica` using trigger-based
CDC. WAL-based CDC is optional and provides lower write-path overhead.
[Full answer →](#does-pg_trickle-require-wal_level--logical)

### 12. Can I use pg_trickle with PgBouncer / connection poolers?

Yes. pg_trickle's background workers use direct connections, not pooled ones.
Your application can use any pooler for reads and writes — the scheduler
operates independently. [Full answer →](#interoperability)

### 13. How do I monitor stream table health?

Built-in views (`pgtrickle.pgt_status`, `pgtrickle.pgt_refresh_history`),
Prometheus metrics endpoint, Grafana dashboard, and NOTIFY-based alerts.
[Full answer →](#monitoring--alerting)

### 14. What happens if a refresh fails?

The stream table is automatically suspended after `pg_trickle.max_consecutive_errors`
consecutive failures (default **3**). Data already in the change buffer is
preserved. After fixing the root cause, resume with:

```sql
ALTER STREAM TABLE my_st RESUME;
```

See [Stream Table Lifecycle](SQL_REFERENCE.md#stream-table-lifecycle) and [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for step-by-step guidance.
[Full answer →](#troubleshooting)

### 15. Can I use pg_trickle with dbt?

Yes — the `dbt-pgtrickle` package provides a `stream_table` materialization.
`dbt run` creates/alters stream tables, `dbt source freshness` checks staleness.
[Full answer →](#dbt-integration)

---

## Table of Contents

**Getting started**
- [General](#general) — What pg_trickle is, how IVM works, key concepts
- [Installation & Setup](#installation--setup) — Installing, configuring, uninstalling
- [Creating & Managing Stream Tables](#creating--managing-stream-tables) — Create, alter, drop, schedules

**Consistency & refresh modes**
- [Data Freshness & Consistency](#data-freshness--consistency) — Staleness, read-your-writes, DVS
- [IMMEDIATE Mode (Transactional IVM)](#immediate-mode-transactional-ivm) — Same-transaction refresh

**SQL features**
- [SQL Support](#sql-support) — Supported and unsupported SQL constructs
- [Aggregates & Group-By](#aggregates--group-by) — Incremental aggregates, HAVING, auxiliary columns
- [Joins](#joins) — Multi-table delta computation, FULL OUTER JOIN
- [CTEs & Recursive Queries](#ctes--recursive-queries) — Semi-naive, DRed, recomputation strategies
- [Window Functions & LATERAL](#window-functions--lateral) — Partition-based recomputation, SRFs
- [TopK (ORDER BY … LIMIT)](#topk-order-by--limit) — Bounded result sets
- [Tables Without Primary Keys](#tables-without-primary-keys) — Content-based row identity

**Internals & architecture**
- [Change Data Capture (CDC)](#change-data-capture-cdc) — Triggers, WAL transition, why `auto` is the default, change buffers
- [Diamond Dependencies & DAG Scheduling](#diamond-dependencies--dag-scheduling) — Topological ordering, atomic groups
- [Schema Changes & DDL Events](#schema-changes--ddl-events) — Reinitialize, event triggers

**Operations**
- [Performance & Tuning](#performance--tuning) — Scheduler tuning, min schedule risks, disk space, adaptive fallback
- [Interoperability](#interoperability) — Views, replication, connection poolers, triggers, pgvector
- [dbt Integration](#dbt-integration) — Materialization, commands, freshness checks
- [Row-Level Security (RLS)](#row-level-security-rls) — Source vs stream table policies, SECURITY DEFINER triggers
- [Deployment & Operations](#deployment--operations) — Workers, upgrades, replicas, Kubernetes
- [Monitoring & Alerting](#monitoring--alerting) — Views, NOTIFY alerts, failure handling
- [Configuration Reference](#configuration-reference) — All GUC parameters

**Troubleshooting & reference**
- [Troubleshooting](#troubleshooting) — Common problems and debugging
- [Why Are These SQL Features Not Supported?](#why-are-these-sql-features-not-supported) — Technical explanations for each limitation
- [Why Are These Stream Table Operations Restricted?](#why-are-these-stream-table-operations-restricted) — Why direct DML, ALTER TABLE, and TRUNCATE are disallowed

---

## General

These questions cover fundamental concepts — what pg_trickle is, how incremental
view maintenance works, and the key building blocks (frontiers, row IDs, the
auto-rewrite pipeline) that power the extension.

### What is pg_trickle?

pg_trickle is a PostgreSQL 18 extension that implements **stream tables** — declarative, automatically-refreshing materialized views with **Differential View Maintenance (DVM)**. You define a SQL query and a refresh schedule; the extension handles change capture, delta computation, and differential refresh automatically.

It is inspired by the [DBSP](https://arxiv.org/abs/2203.16684) differential dataflow framework. See [DBSP_COMPARISON.md](research/DBSP_COMPARISON.md) for a detailed comparison.

### What is incremental view maintenance (IVM) and why does it matter?

**Incremental View Maintenance** means updating a materialized view by processing only the *changes* (deltas) to the source data, rather than re-executing the entire defining query from scratch.

Consider a stream table defined as `SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id` over a 10-million-row `orders` table. When you insert 5 new rows:

- **Without IVM (FULL refresh):** Re-scans all 10 million rows and recomputes every group. Cost: O(total rows).
- **With IVM (DIFFERENTIAL refresh):** Reads only the 5 new rows from the change buffer, identifies the affected groups, and updates just those groups. Cost: O(changed rows × affected groups).

pg_trickle's DVM engine implements IVM using differentiation rules for each SQL operator (Scan, Filter, Join, Aggregate, etc.), generating a *delta query* that computes the exact changes to the stream table from the exact changes to the source.

### What is the difference between a stream table and a regular materialized view, in practice?

| Feature | Materialized Views | Stream Tables |
|---|---|---|
| Refresh | Manual (`REFRESH MATERIALIZED VIEW`) | Automatic (scheduler) or manual |
| Differential refresh | Not supported natively | Built-in differential mode |
| Change detection | None — always full recompute | CDC triggers track row-level changes |
| Dependency ordering | None | DAG-aware topological refresh |
| Monitoring | None | Built-in views, stats, NOTIFY alerts |
| Schedule | None | Duration strings (`5m`) or cron (`*/5 * * * *`) |
| Transactional IVM | No | Yes (IMMEDIATE mode) |

In practice, stream tables are regular PostgreSQL heap tables under the hood — you can query them, create indexes on them, join them with other tables, and reference them from views. The key difference is that pg_trickle manages their contents automatically.

### What happens behind the scenes when I INSERT a row into a table tracked by a stream table?

The full data flow for a DIFFERENTIAL-mode stream table:

1. **Your INSERT completes normally.** The row is written to the source table.
2. **A CDC trigger fires** (row-level `AFTER INSERT`). It writes a change record (action=`I`, the new row data as JSONB, the current WAL LSN) into the source's change buffer table (`pgtrickle_changes.changes_<oid>`). This happens within your transaction — if you roll back, the change record is also rolled back.
3. **You commit.** Both the source row and the change record become visible.
4. **The scheduler wakes up** (every `pg_trickle.scheduler_interval_ms`, default 1 second). It checks whether the stream table's schedule says a refresh is due.
5. **If due, the refresh engine runs.** It reads the change buffer for rows with LSN > the stream table's current frontier, generates a delta query from the DVM operator tree, and applies the result via `MERGE`.
6. **Frontier advances.** The stream table's frontier is updated to the new LSN, and the consumed change buffer rows are cleaned up.

For IMMEDIATE-mode stream tables, steps 2–6 are replaced: a statement-level AFTER trigger computes and applies the delta **within your transaction**, so the stream table is updated before your transaction commits.

### What does "differential" mean in the context of pg_trickle?

"Differential" refers to the mathematical approach of computing **differences (deltas)** rather than absolute values. Given a query Q and a set of changes ΔR to source table R, the DVM engine computes ΔQ(R, ΔR) — the *change to the query result* caused by the *change to the source*. This delta is then applied (merged) into the stream table.

Each SQL operator has its own differentiation rule. For example:
- **Filter:** ΔFilter(R, ΔR) = Filter(ΔR) — just apply the filter to the changes.
- **Join:** ΔJoin(R, S, ΔR) = Join(ΔR, S) — join the changes against the other side's current state.
- **Aggregate:** Recompute only the groups whose keys appear in the changes.

See [DVM_OPERATORS.md](DVM_OPERATORS.md) for the complete set of differentiation rules.

### What is a frontier, and why does pg_trickle track LSNs?

A **frontier** is a per-source map of `{source_oid → LSN}` that records exactly how far each stream table has consumed changes from each of its source tables. It is stored as JSONB in the `pgtrickle.pgt_stream_tables` catalog.

**Why LSNs?** PostgreSQL's Write-Ahead Log Sequence Number (LSN) provides a globally ordered, monotonically increasing position in the change stream. By recording the LSN at which each source was last consumed, the frontier ensures:

- **No missed changes.** The next refresh reads changes with LSN > frontier, ensuring contiguous, non-overlapping windows.
- **No duplicate processing.** Changes at or below the frontier are never re-read.
- **Consistent snapshots.** When a stream table depends on multiple source tables, the frontier tracks each source independently, enabling consistent multi-source delta computation.

**Lifecycle:** Created on first full refresh → Advanced on each differential refresh → Reset on reinitialize.

### What is the `__pgt_row_id` column and why does it appear in my stream tables?

Every stream table has a `__pgt_row_id BIGINT PRIMARY KEY` column. It stores a 64-bit xxHash of the row's group-by key (for aggregate queries) or all output columns (for non-aggregate queries). The refresh engine uses it to match incoming deltas against existing rows during the `MERGE` operation.

**You should ignore this column in your queries.** It is an implementation detail. If it bothers you, exclude it explicitly:

```sql
SELECT customer_id, total FROM order_totals;  -- omit __pgt_row_id
```

### What is the auto-rewrite pipeline and how does it affect my queries?

Before parsing a defining query into the DVM operator tree, pg_trickle runs six automatic rewrite passes:

| # | Pass | What it does |
|---|------|-------------|
| 0 | View inlining | Replaces view references with `(view_definition) AS alias` subqueries (fixpoint, max depth 10) |
| 1 | DISTINCT ON | Converts to `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) = 1` subquery |
| 2 | GROUPING SETS / CUBE / ROLLUP | Decomposes into `UNION ALL` of separate `GROUP BY` queries |
| 3 | Scalar subquery in WHERE | Rewrites `WHERE col > (SELECT …)` to `CROSS JOIN` |
| 4 | Correlated scalar subquery in SELECT | Rewrites to `LEFT JOIN` with grouped inline view |
| 5 | SubLinks in OR | Splits `WHERE a OR EXISTS (…)` into `UNION` branches |

The rewrites are transparent — your original query is preserved in the catalog (`original_query` column) while the rewritten version is stored in `defining_query`. The DVM engine only sees standard SQL operators after rewriting.

See [ARCHITECTURE.md](ARCHITECTURE.md) for details on each pass.

### How does pg_trickle compare to DBSP (the academic framework)?

pg_trickle is inspired by [DBSP](https://arxiv.org/abs/2203.16684) but is not a direct implementation. Key differences:

- **DBSP** is a general-purpose differential dataflow framework with a Rust runtime (Feldera). It models computation as circuits over Z-sets (multisets with integer weights).
- **pg_trickle** implements the same mathematical principles (delta queries, frontier tracking) but embedded inside PostgreSQL as an extension. It generates SQL delta queries rather than running a separate computation engine.
- **Trade-off:** pg_trickle leverages PostgreSQL's optimizer, indexes, and storage engine but is limited to what can be expressed as SQL queries. DBSP can implement arbitrary dataflow computations.

See [DBSP_COMPARISON.md](research/DBSP_COMPARISON.md) for a detailed comparison.

### How does pg_trickle compare to pg_ivm?

| Feature | pg_ivm | pg_trickle |
|---|---|---|
| Refresh timing | Immediate (same transaction) only | Immediate, Deferred (scheduled), or Manual |
| Incremental strategy | Transition tables + query rewriting | DVM operator tree + delta SQL generation |
| Supported SQL | Inner joins, simple outer joins, COUNT/SUM/AVG/MIN/MAX, EXISTS, DISTINCT | All of the above + window functions, recursive CTEs, LATERAL, UNION/INTERSECT/EXCEPT, 37 aggregates, TopK, GROUPING SETS |
| Cascading (view-on-view) | No | Yes (DAG-aware topological refresh) |
| Scheduling | None (always immediate) | Duration, cron, CALCULATED, or NULL |
| Monitoring | None | Built-in views, stats, NOTIFY alerts |
| PostgreSQL version | 14–17 | 18 only |

pg_trickle's IMMEDIATE mode is designed as a migration path for pg_ivm users — it uses the same statement-level trigger approach with transition tables.

### What PostgreSQL versions are supported?

**PostgreSQL 18.x** exclusively. pg_trickle uses PostgreSQL 18 features such as enhanced `MERGE` syntax with `NOT MATCHED BY SOURCE` and improved event trigger payloads. These features are not available in earlier versions.

Backward compatibility with PostgreSQL 16–17 is planned for a future release (tracked in the roadmap).

### Does pg_trickle require `wal_level = logical`?

**No.** By default, pg_trickle starts with trigger-based CDC instead of logical replication. Statement-level triggers are used unless you explicitly set `pg_trickle.cdc_trigger_mode = 'row'`. This means you do not need to set `wal_level = logical`, configure `max_replication_slots`, or create publications.

With the default hybrid CDC mode (`pg_trickle.cdc_mode = 'auto'`), WAL-based capture becomes active only when `wal_level = logical` is available and the transition succeeds. It is not required for normal operation.

### Is pg_trickle production-ready?

pg_trickle is under active development and approaching production readiness. It has a comprehensive test suite with 700+ unit tests and 290+ end-to-end tests covering correctness, failure recovery, and concurrency scenarios.

That said, as with any new extension, you should evaluate it against your specific workloads before deploying to production. Start with non-critical dashboards or reporting tables, monitor refresh performance and data correctness, and gradually expand usage as confidence grows.

---

## Installation & Setup

### How do I install pg_trickle?

1. Add `pg_trickle` to `shared_preload_libraries` in `postgresql.conf`:
   ```ini
   shared_preload_libraries = 'pg_trickle'
   ```
2. Restart PostgreSQL.
3. Run:
   ```sql
   CREATE EXTENSION pg_trickle;
   ```

See [Installation](installation.md) for platform-specific instructions and pre-built release artifacts.

### What are the minimum configuration requirements?

The only mandatory setting is adding pg_trickle to `shared_preload_libraries` in `postgresql.conf` (this requires a PostgreSQL restart):

```ini
shared_preload_libraries = 'pg_trickle'
```

All other GUC parameters have sensible defaults and can be tuned later. However, **`max_worker_processes` often needs to be raised** from its default of 8 — see the next question.

### Can I install pg_trickle on a managed PostgreSQL service (RDS, Cloud SQL, etc.)?

It depends on whether the service allows custom extensions and `shared_preload_libraries` modifications. Many managed services restrict these. However, pg_trickle has one advantage over replication-based extensions: it does **not** require `wal_level = logical`, which avoids one of the most common restrictions on managed PostgreSQL services.

Check your provider's documentation for custom extension support. Services that support custom extensions (e.g., some tiers of Azure Flexible Server, Supabase, Neon) are more likely to work.

### How do I uninstall pg_trickle?

1. Drop all stream tables first (or they will be cascade-dropped):
   ```sql
   SELECT pgtrickle.drop_stream_table(pgt_name) FROM pgtrickle.pgt_stream_tables;
   ```
2. Drop the extension:
   ```sql
   DROP EXTENSION pg_trickle CASCADE;
   ```
3. Remove `pg_trickle` from `shared_preload_libraries` and restart PostgreSQL.

---

## Creating & Managing Stream Tables

### Do I need to choose a refresh mode?

No. The default mode (`'AUTO'`) is adaptive: it uses differential (delta-only)
maintenance when efficient, and automatically falls back to full
recomputation when the change volume is high or the query cannot be
differentiated. This works well for the vast majority of queries.

You only need to specify a mode explicitly when:
- You want **FULL** mode to force recomputation every time (rare).
- You want **IMMEDIATE** mode for sub-second, in-transaction updates
  (adds overhead to every write on source tables).
- You want strict **DIFFERENTIAL** mode and prefer an error over silent
  fallback when the query isn't differentiable.

### How do I create a stream table?

```sql
-- Minimal: just name and query. Refreshes on a calculated schedule
-- using adaptive differential maintenance.
SELECT pgtrickle.create_stream_table(
    'order_totals',
    'SELECT customer_id, SUM(amount) AS total
     FROM orders GROUP BY customer_id'
);

-- With custom schedule:
SELECT pgtrickle.create_stream_table(
    name     => 'order_totals',
    query    => 'SELECT customer_id, SUM(amount) AS total
     FROM orders GROUP BY customer_id',
    schedule => '5m'
);
```

### What is the difference between FULL and DIFFERENTIAL refresh mode?

- **FULL** — Truncates the stream table and re-runs the entire defining query every refresh cycle. Simple but expensive for large result sets.
- **DIFFERENTIAL** — Computes only the delta (changes since the last refresh) using the DVM engine and applies it via a `MERGE` statement. Much faster when only a small fraction of source data changes between refreshes. When the change ratio exceeds `pg_trickle.differential_max_change_ratio` (default 15%), DIFFERENTIAL automatically falls back to FULL for that cycle.
- **IMMEDIATE** — Maintains the stream table synchronously within the same transaction as the base table DML. Uses statement-level triggers with transition tables — no change buffers, no scheduler. The stream table is always up-to-date.

### Why does FULL mode exist if DIFFERENTIAL can fall back to it automatically?

DIFFERENTIAL mode with adaptive fallback covers most user needs — it uses incremental deltas when changes are small and automatically switches to a full recompute when the change ratio is high. However, explicit FULL mode still has its place:

1. **No CDC overhead.** FULL mode installs CDC triggers on source tables (for DAG tracking), but the refresh itself ignores the change buffers entirely. If your workload has very high write throughput and you know you'll always do a full recompute, FULL mode avoids the per-row trigger overhead of writing change records that will never be consumed incrementally.

2. **Simpler debugging.** When investigating data correctness issues, FULL mode is a clean baseline — it re-runs the defining query with no delta computation, no frontier tracking, and no MERGE logic. If FULL produces correct results but DIFFERENTIAL doesn't, the bug is in the delta pipeline.

3. **Predictable performance.** DIFFERENTIAL refresh time varies with the number of changes, which can be unpredictable. FULL refresh time is proportional to the total result set size, which is stable. For SLA-sensitive workloads where you'd rather have consistent 500ms refreshes than variable 5ms–500ms refreshes, FULL provides that predictability.

4. **Unsupported-but-planned constructs.** Some queries may parse correctly in DIFFERENTIAL mode but produce suboptimal deltas. Using FULL mode explicitly is a safe fallback while the DVM engine matures.

For most users, **DIFFERENTIAL is the right default**. Use FULL when you have a specific reason.

### When should I use FULL vs. DIFFERENTIAL vs. IMMEDIATE?

Use **DIFFERENTIAL** (default) when:
- Source tables are large and changes between refreshes are small
- The defining query uses supported operators (most common SQL is supported)
- Some staleness (seconds to minutes) is acceptable

Use **FULL** when:
- The defining query uses unsupported aggregates (`CORR`, `COVAR_*`, `REGR_*`)
- Source tables are small and a full recompute is cheap
- You see frequent adaptive fallbacks to FULL (check refresh history)

Use **IMMEDIATE** when:
- The stream table must always reflect the latest committed data
- You need transactional consistency (reads within the same transaction see updated data)
- Write-side overhead per DML statement is acceptable
- The defining query is relatively simple (no TopK, no materialized view sources)

### What are the advantages and disadvantages of IMMEDIATE vs. deferred (FULL/DIFFERENTIAL) refresh modes?

**IMMEDIATE mode**

| | Detail |
|---|---|
| ✅ Read-your-writes consistency | The stream table is updated within the same transaction as the base table DML — always current from the writer's perspective. |
| ✅ No lag | No background worker, no schedule interval. The view is never stale. |
| ✅ No change buffers | `pgtrickle_changes.*` tables are not used, reducing write overhead on source tables. |
| ✅ pg_ivm compatibility | Drop-in migration path for existing pg_ivm / IMMV users. |
| ❌ Write amplification | Every DML statement on a base table also executes IVM trigger logic, adding latency to the original transaction. |
| ❌ Serialized concurrent writes | An `ExclusiveLock` is taken on the stream table during maintenance, serializing writers. |
| ❌ Limited SQL support | Window functions, `LATERAL` joins, scalar subqueries, and TopK (`ORDER BY … LIMIT`) are not supported — use `DIFFERENTIAL` instead. Recursive CTEs are supported with bounded semi-naive / DRed maintenance. |
| ❌ Cascading limitations | Cascading IMMEDIATE stream tables work but may require manual refresh for deep chains. |
| ❌ No throttling | The refresh cannot be delayed or rate-limited. |

**Deferred mode (`FULL` / `DIFFERENTIAL`)**

| | Detail |
|---|---|
| ✅ Decoupled write path | Base table writes are fast; view maintenance runs later via the scheduler or manual refresh. |
| ✅ Broadest SQL support | Window functions, recursive CTEs, `LATERAL`, `UNION`, user-defined aggregates, TopK, cascading stream tables, and more. |
| ✅ Adaptive cost control | `DIFFERENTIAL` automatically falls back to `FULL` when the change ratio exceeds `pg_trickle.differential_max_change_ratio`. |
| ✅ Concurrency-friendly | Writers never block on view maintenance. |
| ❌ Staleness | The stream table lags by up to one schedule interval (e.g. `1m`). |
| ❌ No read-your-writes | A writer querying the stream table immediately after a write may see the pre-change data. |
| ❌ Infrastructure overhead | Requires change buffer tables, a background worker, and frontier tracking. |

**Rule of thumb:** use `IMMEDIATE` when the query is simple and freshness within the transaction matters. Use `DIFFERENTIAL` (or `FULL`) for complex queries, high concurrency, or when you want to decouple write latency from view maintenance.

### What happens if I have an IMMEDIATE stream table between two DIFFERENTIAL stream tables in a dependency chain?

Consider the chain: `source → ST_A (DIFFERENTIAL) → ST_B (IMMEDIATE) → ST_C (DIFFERENTIAL)`. This is a valid but unusual configuration with important behavioral consequences:

- **ST_A** refreshes on its schedule (e.g., every 1 minute) via the background scheduler.
- **ST_B** is IMMEDIATE, so it has no CDC triggers on ST_A — it uses statement-level IVM triggers. But ST_A is updated by the *scheduler* (not by user DML), and the scheduler's `MERGE` operation *does* fire statement-level triggers on ST_A's dependents. So ST_B updates within the scheduler's transaction when ST_A refreshes.
- **ST_C** is DIFFERENTIAL and depends on ST_B. Since ST_B is a stream table, ST_C's CDC triggers fire when ST_B is modified. The scheduler refreshes ST_C on its own schedule.

The practical concern: **write latency stacking.** When the scheduler refreshes ST_A, ST_B's IVM triggers fire synchronously within that same transaction, adding IVM overhead to ST_A's refresh. If ST_B's delta computation is expensive, it slows down the entire scheduler cycle.

**Recommendation:** Avoid mixing IMMEDIATE into the middle of a deferred chain. Either make the entire chain IMMEDIATE (for small, simple queries) or keep it entirely DIFFERENTIAL. If you need read-your-writes for one specific step, consider making that the terminal (leaf) stream table in the chain.

### What schedule formats are supported?

**Duration strings:**

| Unit | Suffix | Example |
|---|---|---|
| Seconds | `s` | `30s` |
| Minutes | `m` | `5m` |
| Hours | `h` | `2h` |
| Days | `d` | `1d` |
| Weeks | `w` | `1w` |
| Compound | — | `1h30m` |

**Cron expressions:**

| Format | Example | Description |
|---|---|---|
| 5-field | `*/5 * * * *` | Every 5 minutes |
| Aliases | `@hourly`, `@daily` | Built-in shortcuts |

**CALCULATED mode:** Pass `NULL` as the schedule to inherit the schedule from downstream dependents.

### How do cron schedules handle timezones? What does `@daily` really mean?

pg_trickle evaluates cron expressions in **UTC**. The underlying `croner` crate computes the next occurrence from a UTC timestamp, and the scheduler compares this against `chrono::Utc::now()`. There is no per-stream-table timezone setting.

This means:
- `@daily` (equivalent to `0 0 * * *`) fires at **midnight UTC**, not midnight in your local timezone.
- `@hourly` (equivalent to `0 * * * *`) fires at the top of each UTC hour.
- `0 9 * * 1-5` fires at 09:00 UTC on weekdays — if your server is in `America/New_York`, that's 04:00 or 05:00 local time depending on DST.

If you need a schedule aligned to a local timezone, convert the desired local time to UTC and write the cron expression accordingly. For example, to refresh at 08:00 `Europe/Oslo` (UTC+1 in winter, UTC+2 in summer), use `0 6 * * *` in summer and `0 7 * * *` in winter — or accept the 1-hour seasonal shift and pick one.

**Tip:** For most analytics workloads, UTC-based schedules are preferable because they don't shift with daylight saving transitions.

### What is the minimum allowed schedule?

The `pg_trickle.min_schedule_seconds` GUC (default: `60` seconds) sets the shortest allowed refresh schedule. Any `create_stream_table` or `alter_stream_table` call with a schedule shorter than this floor is rejected with a clear error message.

This guard exists to prevent accidentally creating stream tables that refresh too frequently, which could overload the scheduler or the source tables. During development and testing, you can lower it:

```sql
ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1;
SELECT pg_reload_conf();
```

### What happens if all stream tables in the DAG have a CALCULATED schedule?

When every stream table uses a CALCULATED schedule (`schedule => 'calculated'`), there
are no explicit schedules for the resolution algorithm to derive from. The
CALCULATED logic works by propagating `MIN(effective_schedule)` from downstream
dependents upward through the DAG. If **no** node has an explicit duration:

1. **Leaf nodes** (no downstream dependents) have no schedules to take the
   minimum of, so they fall back to the `pg_trickle.min_schedule_seconds` GUC
   (default: **60 seconds**).
2. **Upstream nodes** then resolve to `MIN(fallback) = fallback`.
3. The result: **every stream table in the DAG gets the fallback schedule**
   (60 s by default).

This is safe but usually not what you want — the whole DAG refreshes at the
same generic interval. Best practice is to set an explicit schedule on at least
the leaf (most-downstream) stream tables so that upstream CALCULATED schedules
resolve to something meaningful:

```sql
-- Leaf ST with an explicit schedule
SELECT pgtrickle.create_stream_table(
    name     => 'daily_summary',
    query    => 'SELECT region, SUM(total) FROM pgtrickle.order_totals GROUP BY region',
    schedule => '10m'
);

-- Upstream ST inherits that 10 m schedule via CALCULATED
SELECT pgtrickle.create_stream_table(
    name     => 'order_totals',
    query    => 'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    schedule => 'calculated'
);
```

You can inspect the resolved effective schedules with:

```sql
SELECT pgt_name, schedule, effective_schedule
FROM pgtrickle.pgt_stream_tables;
```

### Can a stream table reference another stream table?

**Yes.** Stream tables can depend on other stream tables. The scheduler automatically refreshes them in topological order (upstream first). Circular dependencies are detected and rejected at creation time.

```sql
-- ST1: aggregates orders
SELECT pgtrickle.create_stream_table(
    name         => 'order_totals',
    query        => 'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);

-- ST2: filters ST1
SELECT pgtrickle.create_stream_table(
    name         => 'big_customers',
    query        => 'SELECT customer_id, total FROM pgtrickle.order_totals WHERE total > 1000',
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

### How do I change a stream table's schedule or mode?

```sql
-- Change schedule
SELECT pgtrickle.alter_stream_table('order_totals', schedule => '10m');

-- Switch refresh mode
SELECT pgtrickle.alter_stream_table('order_totals', refresh_mode => 'FULL');

-- Suspend
SELECT pgtrickle.alter_stream_table('order_totals', status => 'SUSPENDED');

-- Resume
SELECT pgtrickle.alter_stream_table('order_totals', status => 'ACTIVE');
```

### Can I change the defining query of a stream table?

Yes — use the `query` parameter of `alter_stream_table()`:

```sql
SELECT pgtrickle.alter_stream_table('order_totals',
    query => 'SELECT customer_id, SUM(amount) AS total, COUNT(*) AS order_count
              FROM orders GROUP BY customer_id');
```

The ALTER QUERY operation validates the new query, migrates the storage table schema if needed, updates catalog entries and source dependencies, and runs a full refresh — all within a single transaction. Concurrent readers see either the old data or the new data, never an empty table.

**Schema migration behavior:**

| Schema change | Behavior |
|---|---|
| Same columns | Fast path — no storage DDL, just catalog update + full refresh |
| Columns added or removed | Compatible migration via `ALTER TABLE ADD/DROP COLUMN` — storage table OID preserved |
| Column type incompatible | Full rebuild — storage table dropped and recreated (OID changes, `WARNING` emitted) |

You can also change the query and other parameters simultaneously:

```sql
SELECT pgtrickle.alter_stream_table('order_totals',
    query => 'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    refresh_mode => 'FULL');
```

### How do I deploy stream tables idempotently?

Use `create_or_replace_stream_table()` — one function call that does the right
thing automatically:

```sql
-- Safe to run on every deploy — creates, updates, or no-ops as needed:
SELECT pgtrickle.create_or_replace_stream_table(
    name         => 'order_totals',
    query        => 'SELECT region, SUM(amount) AS total FROM orders GROUP BY region',
    schedule     => '2m',
    refresh_mode => 'DIFFERENTIAL'
);
```

**What happens on each deploy:**

| Situation | Action |
|---|---|
| First deploy (stream table doesn't exist) | Creates it, populates data |
| Nothing changed since last deploy | No-op — logs INFO, returns instantly |
| You changed the schedule or mode | Updates config in place (no data loss) |
| You changed the query | Migrates storage schema + runs a full refresh |

This mirrors PostgreSQL's `CREATE OR REPLACE VIEW` / `CREATE OR REPLACE FUNCTION` pattern.

**When to use which function:**

| Function | Use case |
|---|---|
| `create_or_replace_stream_table()` | **Recommended for most deployments.** Declarative, idempotent — handles all cases automatically. |
| `create_stream_table_if_not_exists()` | Safe re-run, but **never modifies** an existing definition. Good for one-time seed migrations. |
| `create_stream_table()` | **Strict mode** — errors if the stream table already exists. Use when you want an explicit failure on duplicates. |

### How do I trigger a manual refresh?

Call `refresh_stream_table()` to immediately refresh a stream table without waiting for the next scheduled cycle:

```sql
SELECT pgtrickle.refresh_stream_table('order_totals');
```

This runs a synchronous refresh in your current session and returns when complete. It works even when the background scheduler is disabled (`pg_trickle.enabled = false`), making it useful for testing, debugging, or one-off data refreshes.

To force a full refresh regardless of the stream table's configured mode, temporarily change the refresh mode:

```sql
SELECT pgtrickle.alter_stream_table('order_totals', refresh_mode => 'FULL');
SELECT pgtrickle.refresh_stream_table('order_totals');
-- Switch back to the original mode when done:
SELECT pgtrickle.alter_stream_table('order_totals', refresh_mode => 'DIFFERENTIAL');
```

---

## Data Freshness & Consistency

Understanding when and how stream tables become current is the #1 conceptual
hurdle for users coming from synchronous materialized views. This section
explains staleness guarantees, read-your-writes behavior, and Delayed View
Semantics (DVS).

### How stale can a stream table be?

For **deferred** modes (FULL / DIFFERENTIAL): A stream table can be at most **one schedule interval** behind the source data, plus the time it takes to execute the refresh itself. For example, with `schedule => '1m'`, the maximum staleness is approximately 1 minute + refresh duration.

In practice, staleness is often less than the schedule interval because the scheduler continuously checks for due refreshes at `pg_trickle.scheduler_interval_ms` (default: 1 second).

For **IMMEDIATE** mode: The stream table is **always current** within the transaction that modified the source data. There is zero staleness.

Check current staleness:

```sql
SELECT pgtrickle.get_staleness('order_totals');  -- returns seconds, NULL if never refreshed

-- Or check all stream tables:
SELECT pgt_name, staleness, stale FROM pgtrickle.stream_tables_info;
```

### Can I read my own writes immediately after an INSERT?

**It depends on the refresh mode:**

- **IMMEDIATE mode:** Yes. The stream table is updated within the same transaction as your INSERT. You can query it immediately and see the updated data.
- **DIFFERENTIAL / FULL mode:** No. The stream table is updated by the background scheduler in a separate transaction. Your INSERT is captured by the CDC trigger, but the stream table won't reflect it until the next scheduled refresh (or a manual `refresh_stream_table()` call).

If read-your-writes consistency is a requirement, use `refresh_mode => 'IMMEDIATE'`.

### What consistency guarantees does pg_trickle provide?

pg_trickle provides **Delayed View Semantics (DVS):** the contents of every stream table are logically equivalent to evaluating its defining query at some past point in time — the `data_timestamp`. This means:

- The data is always **internally consistent** — it corresponds to a valid snapshot of the source data.
- The data may be **stale** — it reflects the source state at `data_timestamp`, not necessarily the current state.
- For cascading stream tables, the scheduler refreshes in **topological order** so that when ST B references upstream ST A, A has already been refreshed before B runs its delta query against A's contents.

For IMMEDIATE mode, the guarantee is stronger: the stream table always reflects the state of the source data **as of the current transaction**.

### What are "Delayed View Semantics" (DVS)?

DVS is the formal consistency guarantee: a stream table's contents are equivalent to evaluating its defining query at a specific past time (the `data_timestamp`). This is analogous to how a materialized view captured at a point in time is always internally consistent, even if the source data has since changed.

The `data_timestamp` is recorded in the catalog and advanced after each successful refresh:

```sql
SELECT pgt_name, data_timestamp FROM pgtrickle.pgt_stream_tables;
```

### What happens if the scheduler is behind — does data get lost?

**No.** Change data is never lost, even if the scheduler falls behind. Changes accumulate in the change buffer tables (`pgtrickle_changes.changes_<oid>`) until consumed by a refresh. The frontier ensures that each refresh picks up exactly where the last one left off.

However, a growing change buffer increases:
- Disk usage (change buffer tables grow)
- Refresh time (more changes to process per cycle)
- Risk of adaptive fallback to FULL (if the change ratio exceeds `pg_trickle.differential_max_change_ratio`)

The monitoring system emits a `buffer_growth_warning` NOTIFY alert if buffers grow unexpectedly.

### How does pg_trickle ensure deltas are applied in the right order across cascading stream tables?

The scheduler uses **topological ordering** from the dependency DAG. When ST B depends on ST A:

1. ST A is refreshed first — its data is brought up to date and its frontier advances.
2. ST A's refresh writes are captured by CDC triggers (since ST A is a source for ST B).
3. ST B is refreshed next — its delta query reads ST A's current (just-refreshed) data and the change buffer.

This ensures that downstream stream tables always see consistent upstream data. Circular dependencies are rejected at creation time.

---

## IMMEDIATE Mode (Transactional IVM)

IMMEDIATE mode maintains the stream table synchronously — within the same
transaction as the source DML. This section covers when to use it, what SQL it
supports, locking behavior, and how to switch between modes.

### When should I use IMMEDIATE mode instead of DIFFERENTIAL?

Use IMMEDIATE when:
- Your application requires **read-your-writes consistency** — e.g., a user inserts an order and immediately queries a dashboard that must include that order.
- The defining query is relatively simple (single-table aggregation, joins, filters).
- The source table write rate is moderate (IMMEDIATE adds latency to every DML statement).

Stick with DIFFERENTIAL when:
- Staleness of a few seconds to minutes is acceptable.
- The defining query uses unsupported IMMEDIATE constructs (materialized-view sources, foreign-table sources).
- Write-side performance is critical (high-throughput OLTP).
- You need to decouple write latency from view maintenance.

### What SQL features are NOT supported in IMMEDIATE mode?

IMMEDIATE mode supports **all** constructs that DIFFERENTIAL supports, with two source-type exceptions:

| Feature | Status | Notes |
|---|---|---|
| `WITH RECURSIVE` | ✅ Supported (IM1) | Semi-naive evaluation inside the trigger. A depth counter guards against infinite loops (`pg_trickle.ivm_recursive_max_depth`, default 100). A warning is emitted at create time for very deep hierarchies. |
| TopK (`ORDER BY … LIMIT N [OFFSET M]`) | ✅ Supported (IM2) | Micro-refresh: recomputes the top-N rows on every DML statement. Gated by `pg_trickle.ivm_topk_max_limit` to prevent unbounded scans. |
| Materialized views as sources | ❌ Rejected | Stale-snapshot prevents trigger-based capture — use the underlying query instead. |
| Foreign tables as sources | ❌ Rejected | No triggers on foreign tables — use FULL mode instead. |

Attempting to create or switch to IMMEDIATE mode with an unsupported construct produces a clear error message.

### What happens when I TRUNCATE a source table in IMMEDIATE mode?

A statement-level `AFTER TRUNCATE` trigger fires and **truncates the stream table, then re-populates it** by executing a full refresh from the defining query — all within the same transaction. The stream table remains consistent.

### Can I have cascading IMMEDIATE stream tables (ST A → ST B)?

**Yes.** When ST A is IMMEDIATE and ST B depends on ST A and is also IMMEDIATE, changes propagate through the chain within the same transaction. The IVM triggers on the base table update ST A, and since that write is visible within the transaction, ST B's triggers fire and update ST B.

### What locking does IMMEDIATE mode use?

IMMEDIATE mode acquires **statement-level locks** on the stream table during delta application:

- **Simple queries** (single-table scan/filter without aggregates or DISTINCT): `RowExclusiveLock` — allows concurrent readers, blocks other writers.
- **Complex queries** (joins, aggregates, DISTINCT, window functions): `ExclusiveLock` — blocks both readers and writers to ensure delta consistency.

This means concurrent writes to the same base table are **serialized** through the stream table lock. For high-concurrency write workloads, DIFFERENTIAL mode avoids this bottleneck.

### How do I switch an existing DIFFERENTIAL stream table to IMMEDIATE?

```sql
SELECT pgtrickle.alter_stream_table('order_totals', refresh_mode => 'IMMEDIATE');
```

This:
1. Validates the defining query against IMMEDIATE mode restrictions.
2. Removes the row-level CDC triggers from source tables.
3. Installs statement-level IVM triggers (BEFORE + AFTER with transition tables).
4. Clears the schedule (IMMEDIATE mode has no schedule).
5. Performs a full refresh to establish a consistent baseline.

To switch back:

```sql
SELECT pgtrickle.alter_stream_table('order_totals', refresh_mode => 'DIFFERENTIAL');
```

This reverses the process: removes IVM triggers, installs CDC triggers, restores the schedule (default `1m`), and performs a full refresh.

### What happens to IMMEDIATE mode during a manual `refresh_stream_table()` call?

For IMMEDIATE mode stream tables, `refresh_stream_table()` performs a **FULL refresh** — truncates and re-populates from the defining query. This is useful for recovering from edge cases or forcing a clean baseline. It is equivalent to pg_ivm's `refresh_immv(name, true)`.

### How much write-side overhead does IMMEDIATE mode add?

Each DML statement on a base table tracked by an IMMEDIATE stream table incurs:

- **BEFORE trigger:** Advisory lock acquisition + pre-state setup (~0.1–0.5 ms).
- **AFTER trigger:** Transition table copy to temp tables + delta SQL generation + delta application (~1–50 ms depending on query complexity and delta size).

For a simple single-table aggregate, expect **2–10 ms overhead per statement**. For multi-table joins or window functions, overhead is higher. The overhead scales with the number of IMMEDIATE stream tables that depend on the same source table.

---

## SQL Support

pg_trickle supports a broad range of SQL in defining queries. This section
covers what’s supported, what’s rejected (with rewrites), and how specific
constructs like aggregates and `ORDER BY` are handled. The subsections that
follow dive deeper into aggregates, joins, CTEs, window functions, and TopK.

### What SQL features are supported in defining queries?

Most common SQL is supported in both FULL and DIFFERENTIAL modes:

- Table scans, projections, `WHERE`/`HAVING` filters
- `INNER`, `LEFT`, `RIGHT`, `FULL OUTER JOIN` (including multi-table joins)
- `GROUP BY` with 25+ aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `BOOL_AND`/`OR`, `STRING_AGG`, `ARRAY_AGG`, `JSON_AGG`, `JSONB_AGG`, `BIT_AND`/`OR`/`XOR`, `STDDEV`, `VARIANCE`, `MODE`, `PERCENTILE_CONT`/`DISC`, and more)
- `FILTER (WHERE ...)` on aggregates
- `DISTINCT`
- Set operations: `UNION ALL`, `UNION`, `INTERSECT`, `INTERSECT ALL`, `EXCEPT`, `EXCEPT ALL`
- Subqueries: `EXISTS`, `NOT EXISTS`, `IN (subquery)`, `NOT IN (subquery)`, scalar subqueries
- Non-recursive and recursive CTEs
- Window functions (`ROW_NUMBER`, `RANK`, `SUM OVER`, etc.)
- `LATERAL` joins with set-returning functions and correlated subqueries
- `CASE`, `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`, `BETWEEN`, `IS DISTINCT FROM`

See [DVM Operators](DVM_OPERATORS.md) for the complete list.

### What SQL features are NOT supported?

The following are rejected with clear error messages and suggested rewrites:

| Feature | Reason | Suggested Rewrite |
|---|---|---|
| `TABLESAMPLE` | Stream tables materialize the full result set | Use `WHERE random() < fraction` in consuming query |
| Window functions in expressions | Cannot be differentially maintained | Move window function to a separate column |
| `LIMIT` / `OFFSET` (without `ORDER BY`) | Stream tables materialize the full result set; `ORDER BY … LIMIT N [OFFSET M]` *is* supported as [TopK](#topk-order-by--limit) | Apply when querying the stream table, or add `ORDER BY` + `LIMIT` to use the TopK pattern |
| `FOR UPDATE` / `FOR SHARE` | Row-level locking not applicable | Remove the locking clause |
| `RANGE_AGG` / `RANGE_INTERSECT_AGG` | No incremental delta decomposition exists for range aggregates | Use FULL mode, or compute range unions in the consuming query |

Each rejected feature is explained in detail in the [Why Are These SQL Features Not Supported?](#why-are-these-sql-features-not-supported) section below.

### What happens to `ORDER BY` in defining queries?

`ORDER BY` in the defining query is **accepted but silently discarded**. This is consistent with how PostgreSQL handles `CREATE MATERIALIZED VIEW AS SELECT ... ORDER BY ...` — the ordering only affects the initial INSERT, not the stored data.

Stream tables are heap tables with no guaranteed row order. Apply `ORDER BY` when **querying** the stream table instead:

```sql
-- Don't rely on ORDER BY in the defining query:
-- 'SELECT region, SUM(amount) AS total FROM orders GROUP BY region ORDER BY total DESC'

-- Instead, order when reading:
SELECT * FROM regional_totals ORDER BY total DESC;
```

**Exception:** When `ORDER BY` is paired with `LIMIT N` (with or without `OFFSET M`), pg_trickle recognizes the [TopK pattern](#topk-order-by--limit) and preserves the ordering, limit, and offset.

### Which aggregates support DIFFERENTIAL mode?

**Algebraic** (O(changes), fully incremental): `COUNT`, `SUM`, `AVG`

**Semi-algebraic** (incremental with occasional group rescan): `MIN`, `MAX`

**Group-rescan** (affected groups re-aggregated from source): `STRING_AGG`, `ARRAY_AGG`, `JSON_AGG`, `JSONB_AGG`, `BOOL_AND`, `BOOL_OR`, `BIT_AND`, `BIT_OR`, `BIT_XOR`, `JSON_OBJECT_AGG`, `JSONB_OBJECT_AGG`, `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, `VAR_SAMP`, `MODE`, `PERCENTILE_CONT`, `PERCENTILE_DISC`, `CORR`, `COVAR_POP`, `COVAR_SAMP`, `REGR_AVGX`, `REGR_AVGY`, `REGR_COUNT`, `REGR_INTERCEPT`, `REGR_R2`, `REGR_SLOPE`, `REGR_SXX`, `REGR_SXY`, `REGR_SYY`

**37 aggregate function variants** are supported in total.

---

## Aggregates & Group-By

Aggregate handling is one of the most complex parts of incremental view
maintenance. This section explains how pg_trickle categorizes aggregates by
their incremental cost, how hidden auxiliary columns work, and what happens
when groups are created or destroyed.

### Which aggregates are fully incremental (O(1) per change) vs. group-rescan?

pg_trickle categorizes aggregates into three tiers:

| Tier | Cost per change | Aggregates | Mechanism |
|---|---|---|---|
| **Algebraic** | O(1) | `COUNT`, `SUM`, `AVG` | Hidden auxiliary columns (`__pgt_count`, `__pgt_sum_x`) track running totals. Delta updates these columns arithmetically. |
| **Semi-algebraic** | O(1) normally, O(group) on extremum deletion | `MIN`, `MAX` | Maintained via `LEAST`/`GREATEST`. If the current MIN/MAX is deleted, the group is rescanned to find the new extremum. |
| **Group-rescan** | O(group size) per affected group | All others (35 functions) | Affected groups are re-aggregated from source data. A NULL sentinel marks stale groups for rescan. |

For most workloads, the algebraic tier (COUNT/SUM/AVG) covers the majority of aggregations and is the fastest.

### Why do some aggregates have hidden auxiliary columns?

For algebraic aggregates (COUNT, SUM, AVG), the DVM engine adds hidden `__pgt_count` and `__pgt_sum_x` columns to the stream table's storage. These store running totals that can be updated with O(1) arithmetic per change instead of rescanning the entire group.

For example, a stream table defined as `SELECT dept, AVG(salary) FROM employees GROUP BY dept` internally stores:
- `dept` — the group-by key
- `avg` — the user-visible average (computed as `__pgt_sum_x / __pgt_count`)
- `__pgt_count` — running count of rows in the group
- `__pgt_sum_x` — running sum of salary values
- `__pgt_row_id` — row identity hash

When a new employee is inserted, the refresh updates `__pgt_count += 1`, `__pgt_sum_x += new_salary`, and recomputes `avg`. No rescan of the source table is needed.

### How does HAVING work with differential refresh?

`HAVING` is fully supported in DIFFERENTIAL mode. The DVM engine tracks **threshold transitions** — groups entering or exiting the HAVING condition:

- **Group crosses threshold upward:** A previously excluded group (e.g., `HAVING COUNT(*) > 5`) gains enough members → the group is **inserted** into the stream table.
- **Group crosses threshold downward:** A group that was included drops below the threshold → the group is **deleted** from the stream table.
- **Group stays above threshold:** Normal delta update (adjust aggregate values).

This means the stream table always reflects only the groups that satisfy the HAVING clause, even as group membership changes.

### What happens to a group when all its rows are deleted?

When the last row of a group is deleted from the source table, the DVM engine detects that `__pgt_count` drops to zero and **deletes the group row** from the stream table. The hidden auxiliary columns are cleaned up along with it.

If a new row for the same group-by key is later inserted, a fresh group row is created from scratch.

### Why are `CORR`, `COVAR_*`, and `REGR_*` limited to FULL mode?

Regression aggregates like `CORR`, `COVAR_POP`, `COVAR_SAMP`, and the `REGR_*` family require maintaining running sums of products and squares across the entire group. Unlike `COUNT`/`SUM`/`AVG` (where deltas can be computed from the change alone), regression aggregates:

1. **Lack algebraic delta rules.** There is no closed-form way to update a correlation coefficient from a single row change without access to the full group's data.
2. **Would degrade to group-rescan anyway.** Even if supported, the implementation would need to rescan the full group from source — identical to FULL mode for most practical group sizes.

These aggregates work fine in **FULL** refresh mode, which re-runs the entire query from scratch each cycle.

---

## Joins

Join delta computation can produce surprising results when both sides change
simultaneously. This section covers the standard IVM join rule, FULL OUTER JOIN
support, and known edge cases.

### How does a DIFFERENTIAL refresh handle a join when both sides changed?

When both tables in a join have changes since the last refresh, the DVM engine computes the join delta using the **standard IVM join rule:**

$$\Delta(R \bowtie S) = (\Delta R \bowtie S) \cup (R \bowtie \Delta S) \cup (\Delta R \bowtie \Delta S)$$

In practice, this means:
1. Join the **changes from the left** against the **current state of the right**.
2. Join the **current state of the left** against the **changes from the right**.
3. Join the **changes from both sides** (handles simultaneous changes to matching keys).

All three parts are combined into a single CTE-based delta query that PostgreSQL executes in one pass.

### Does pg_trickle support FULL OUTER JOIN incrementally?

**Yes.** FULL OUTER JOIN is supported in DIFFERENTIAL mode with an 8-part delta computation. This handles all four cases: matched rows on both sides, left-only rows, right-only rows, and rows that transition between matched and unmatched states as data changes.

The 8 parts cover: new left matches, removed left matches, new right matches, removed right matches, newly matched from left-only, newly matched from right-only, newly unmatched to left-only, and newly unmatched to right-only.

### What happens when a join key is updated and the joined row is simultaneously deleted?

This is a known edge case. When a join key column is updated in the same refresh cycle as the joined-side row is deleted, the delta may miss the required DELETE, potentially leaving a stale row in the stream table.

**Mitigations:**
- The **adaptive FULL fallback** (triggered when the change ratio exceeds `pg_trickle.differential_max_change_ratio`) catches most high-change-rate scenarios where this is likely.
- You can stagger changes across refresh cycles.
- Use FULL mode for tables where this pattern is common.

### How does NATURAL JOIN work?

`NATURAL JOIN` is fully supported. At parse time, pg_trickle resolves the common columns between the two tables and synthesizes explicit equi-join conditions. The internal `__pgt_row_id` column is excluded from common column resolution, so NATURAL JOINs between stream tables also work correctly.

---

## CTEs & Recursive Queries

Recursive CTE support is a key differentiator for pg_trickle. This section
explains the three maintenance strategies (semi-naive, DRed, recomputation)
and when each is used.

### Do recursive CTEs work in DIFFERENTIAL mode?

**Yes.** pg_trickle supports `WITH RECURSIVE` in DIFFERENTIAL mode with three auto-selected strategies:

| Strategy | When used | How it works |
|---|---|---|
| **Semi-naive evaluation** | INSERT-only changes to the base case | Iteratively evaluates new derivations from the inserted rows without touching existing rows. Fastest path. |
| **Delete-and-Rederive (DRed)** | Mixed changes (INSERT + DELETE/UPDATE) | Deletes potentially affected derived rows, then rederives them from scratch to determine the true delta. |
| **Recomputation fallback** | Column mismatch or non-monotone recursive terms | Falls back to full recomputation of the recursive CTE. Used when the recursive term contains EXCEPT, Aggregate, Window, DISTINCT, AntiJoin, or INTERSECT SET operators. |

The strategy is selected automatically based on the type of changes and the recursive term's structure.

### What are the three strategies for recursive CTE maintenance?

See the table above. In brief:

- **Semi-naive** is the fast path for append-only workloads (e.g., adding nodes to a tree). It's O(new derivations) — much cheaper than a full re-evaluation.
- **DRed** handles deletions and updates correctly by first removing potentially invalidated rows and then rederiving them. More expensive than semi-naive, but still incremental.
- **Recomputation** is the safe fallback that re-executes the entire recursive CTE. Used when the recursive term's structure is too complex for incremental processing.

### What triggers a fallback from semi-naive to recomputation?

A recomputation fallback is triggered when:

1. **The recursive term contains non-monotone operators** — `EXCEPT`, `Aggregate`, `Window`, `DISTINCT`, `AntiJoin`, or `INTERSECT SET`. These operators can "un-derive" rows when inputs change, which semi-naive evaluation cannot handle.
2. **Column mismatch** — the CTE's output columns don't match the stream table's storage schema (e.g., after a schema change).
3. **Mixed DML with non-monotone terms** — DELETE or UPDATE changes combined with non-monotone recursive terms always trigger recomputation.

Check which strategy was used in the refresh history:

```sql
SELECT action, rows_inserted, rows_deleted
FROM pgtrickle.get_refresh_history('my_recursive_st', 5);
```

### What happens when a CTE is referenced multiple times in the same query?

When a non-recursive CTE is referenced more than once, pg_trickle uses **shared delta computation** — the CTE's delta is computed once and cached, then reused by each reference. This is tracked via `CteScan` operator nodes that look up the shared delta from an internal CTE registry.

For single-reference CTEs, pg_trickle simply inlines them as subqueries (no overhead).

---

## Window Functions & LATERAL

Window functions are maintained via partition-based recomputation rather than
row-level deltas. This section covers what’s supported, the expression
restriction, and LATERAL constructs.

### How are window functions maintained incrementally?

pg_trickle uses **partition-based recomputation** for window functions. When source data changes, the DVM engine:

1. Identifies which **partitions** are affected by the changes (based on the `PARTITION BY` key).
2. Recomputes the window function for **only the affected partitions**.
3. Replaces the old partition results with the new ones in the stream table.

This is more efficient than a full recomputation when changes affect a small number of partitions.

### Why can't I use a window function inside a CASE or COALESCE expression?

Window functions like `ROW_NUMBER() OVER (…)` are supported as **standalone columns** but cannot be embedded in expressions (e.g., `CASE WHEN ROW_NUMBER() OVER (...) = 1 THEN ...`).

This restriction exists because the DVM engine handles window functions by recomputing entire partitions. When a window function is buried inside an expression, the engine cannot isolate the window computation from the surrounding expression.

**Rewrite:** Move the window function to a separate column in one stream table, then reference it in a second stream table:

```sql
-- ST1: compute the window function
SELECT id, dept, salary,
       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
FROM employees

-- ST2: use it in an expression (references ST1)
SELECT id, CASE WHEN rn = 1 THEN 'top' ELSE 'other' END AS rank_label
FROM st1
```

### What LATERAL constructs are supported?

pg_trickle supports three kinds of `LATERAL` constructs:

| Construct | Example | Delta strategy |
|---|---|---|
| **Set-returning functions** | `LATERAL jsonb_array_elements(data)` | Row-scoped recomputation — only affected parent rows are re-expanded |
| **Correlated subqueries** | `LATERAL (SELECT ... WHERE t.id = s.id)` | Row-scoped recomputation |
| **JSON_TABLE** (PG 17+) | `JSON_TABLE(data, '$.items[*]' ...)` | Modeled as `LateralFunction` |

Additional supported SRFs: `jsonb_each`, `jsonb_each_text`, `unnest`, `generate_series`, and others.

### What happens when a row moves between window partitions during a refresh?

When a row's `PARTITION BY` key changes (e.g., an employee moves departments), the DVM engine recomputes **both** the old partition (to remove the row) and the new partition (to add it). Both partitions are re-evaluated from the source data, ensuring window function results are correct.

---

## TopK (ORDER BY … LIMIT)

TopK queries (`ORDER BY ... LIMIT N`, optionally with `OFFSET M`) are handled via a
specialized MERGE-based strategy that re-executes the bounded query each cycle.
This section explains how it works and its limitations.

### How does `ORDER BY … LIMIT N` work in a stream table?

When a defining query has a top-level `ORDER BY … LIMIT N` (with a constant integer N), pg_trickle recognizes it as a **TopK pattern**. An optional `OFFSET M` (constant integer) selects a "page" within the ranked result. The stream table stores exactly N rows and is refreshed via a MERGE-based scoped-recomputation strategy:

1. On each refresh, the full query (with ORDER BY + LIMIT, and OFFSET if present) is re-executed against the source tables.
2. The result is merged into the stream table using `MERGE` with `NOT MATCHED BY SOURCE` for deletes.
3. The catalog records `topk_limit`, `topk_order_by`, and optionally `topk_offset` for the stream table.

TopK bypasses the DVM delta pipeline — it always re-executes the bounded query. This is efficient because the result set is bounded by N.

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'top_customers',
    query        => 'SELECT customer_id, total FROM order_totals ORDER BY total DESC LIMIT 100',
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);

-- With OFFSET — "page 2" of the leaderboard (rows 101–200):
SELECT pgtrickle.create_stream_table(
    name         => 'next_customers',
    query        => 'SELECT customer_id, total FROM order_totals ORDER BY total DESC LIMIT 100 OFFSET 100',
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Does OFFSET work with TopK?

**Yes.** `ORDER BY … LIMIT N OFFSET M` is fully supported. The stream table stores exactly N rows starting from position M+1 in the ranked result. This is useful for:

- **Paginated dashboards:** Each page is a separate stream table with a different OFFSET.
- **Excluding outliers:** `OFFSET 5 LIMIT 50` skips the top 5 and shows the next 50.
- **Windowed leaderboards:** `OFFSET 10 LIMIT 10` shows the "second tier."

**Caveat:** When source data changes, the "page" can shift — a row on page 3 may move to page 2 or 4. The stream table always reflects the current state of the page at the time of the last refresh.

`OFFSET 0` is treated as no offset.

### What happens when a row below the top-N cutoff rises above it?

On the next refresh, the full `ORDER BY … LIMIT N` query is re-executed. The newly qualifying row appears in the result, and the row that fell out of the top-N is removed. The MERGE operation handles this by:

- **INSERT** the newly qualifying row
- **DELETE** the row that fell below the cutoff
- **UPDATE** any rows whose values changed but remained in the top-N

Since TopK always re-executes the bounded query, it correctly detects all ranking changes.

### Can I use TopK with aggregates or joins?

**Yes.** The defining query can contain any SQL that pg_trickle supports, plus `ORDER BY … LIMIT N`:

```sql
-- TopK over an aggregate
SELECT dept, SUM(salary) AS total_salary
FROM employees GROUP BY dept
ORDER BY total_salary DESC LIMIT 10

-- TopK over a join
SELECT e.name, d.name AS dept, e.salary
FROM employees e JOIN departments d ON e.dept_id = d.id
ORDER BY e.salary DESC LIMIT 20
```

The only restriction is that TopK cannot be combined with set operations (`UNION`/`INTERSECT`/`EXCEPT`) or `GROUPING SETS`/`CUBE`/`ROLLUP`.

---

## Tables Without Primary Keys

While primary keys are not required, their absence changes how pg_trickle
identifies rows. This section explains the content-based hashing fallback and
its limitations with duplicate rows.

### Do source tables need a primary key?

**No, but it is strongly recommended.** When a source table has a primary key, pg_trickle uses it to generate a deterministic `__pgt_row_id` for each row — this is the most reliable way to track row identity across refreshes.

Without a primary key, pg_trickle falls back to **content-based hashing** — an xxHash of all column values. This works correctly for tables where every row is unique, but has known issues with exact duplicate rows. See [What are the risks of using tables without primary keys?](#what-are-the-risks-of-using-tables-without-primary-keys) for details.

### What are the risks of using tables without primary keys?

Content-based row identity has known limitations with **exact duplicate rows** (rows where every column value is identical):

1. **INSERT as no-op:** If a row identical to an existing one is inserted, both have the same `__pgt_row_id` hash, so the MERGE treats it as a no-op (the row already exists).
2. **DELETE removes all copies:** Deleting one of N identical rows generates a DELETE delta, but the MERGE removes all rows with that `__pgt_row_id`.
3. **Aggregate drift:** Over time, these mismatches can cause aggregate values to drift from the true result.

**Recommendation:** Add a primary key or unique constraint to source tables, or use FULL mode for tables with frequent exact-duplicate rows.

### How does content-based row identity work for duplicate rows?

For tables without a primary key, `__pgt_row_id` is computed as `pg_trickle_hash_multi(ARRAY[col1::text, col2::text, ...])` — an xxHash of all column values. Rows with identical content produce identical hashes.

The hash uses `\x1E` (record separator) between values and `\x00NULL\x00` for NULL values, minimizing collision risk for rows with different content. However, truly identical rows (same values in every column) will always hash to the same value — this is inherent to content-based identity.

---

## Change Data Capture (CDC)

This section explains how pg_trickle captures changes to your source tables,
the trade-offs between trigger-based and WAL-based CDC, and operational topics
like backup/restore and buffer inspection.

### How does pg_trickle capture changes to source tables?

pg_trickle installs **statement-level** `AFTER INSERT`, `AFTER UPDATE`, and `AFTER DELETE` PL/pgSQL triggers on each source table referenced by a stream table (`pg_trickle.cdc_trigger_mode = 'statement'` is the default). Each trigger uses `REFERENCING NEW TABLE / OLD TABLE` transition tables so it fires once per statement and can batch all affected rows in one pass.

For the step-by-step walkthrough, see [WHAT_HAPPENS_ON_INSERT.md](tutorials/WHAT_HAPPENS_ON_INSERT.md) and [ARCHITECTURE.md](ARCHITECTURE.md#change-data-capture).

Each change record contains:
- **Action** — `I` (insert), `U` (update), `D` (delete), or `T` (truncate marker)
- **Row data** — old and/or new row values serialized as JSONB
- **LSN** — the current WAL log sequence number, used for frontier tracking
- **Transaction ID** — links the change to its originating transaction

The trigger fires within your transaction, so if you roll back, the change record is also rolled back. This guarantees that only committed changes appear in the buffer.

### What is the overhead of CDC triggers?

With statement-level triggers (the default), the overhead is amortized across all rows in a statement. For a single-row INSERT the overhead is approximately **20–55 μs** (PL/pgSQL dispatch + transition table scan + buffer INSERT). For a bulk INSERT of N rows, the trigger fires once and writes N buffer rows in a single batched INSERT — dramatically lower per-row cost than the legacy row-level mode.

At typical write rates (fewer than 1,000 writes per second per source table), this adds **less than 5% additional DML latency**. For most OLTP workloads, the overhead is negligible — a single network round-trip to the database is usually 10–100× more expensive.

If you have very high-throughput source tables (>10K writes/sec), consider enabling the hybrid CDC mode (`pg_trickle.cdc_mode = 'auto'`) which can automatically transition to WAL-based capture for near-zero write-path overhead.

### What happens when I `TRUNCATE` a source table?

TRUNCATE is captured via a statement-level `AFTER TRUNCATE` trigger that writes a `T` marker row to the change buffer. When the differential refresh engine detects this marker, it automatically falls back to a full refresh for that cycle, ensuring the stream table stays consistent. Both FULL and DIFFERENTIAL mode stream tables handle TRUNCATE correctly.

### Are CDC triggers automatically cleaned up?

**Yes.** pg_trickle tracks which source tables are referenced by which stream tables in the `pgt_dependencies` catalog. When the last stream table referencing a particular source table is dropped, pg_trickle automatically:

1. Removes the CDC triggers from the source table.
2. Drops the associated change buffer table (`pgtrickle_changes.changes_<oid>`).

You do not need to manually clean up triggers or buffer tables.

### What happens if a source table is dropped or altered?

pg_trickle has DDL event triggers that listen for `ALTER TABLE` and `DROP TABLE` on source tables. When a change is detected, pg_trickle responds automatically:

1. All stream tables that depend on the altered source are marked with `needs_reinit = true` in the catalog.
2. On the next scheduler cycle, each affected stream table is **reinitialized** — the existing storage table is dropped, recreated from the current defining query schema, and re-populated with a full refresh.
3. A `reinitialize_needed` NOTIFY alert is sent so your monitoring can detect the event.

If the DDL change breaks the defining query (e.g., a column referenced in the query was dropped), the reinitialization will fail and the stream table will enter ERROR status. In that case, you need to drop and recreate the stream table with an updated query.

Dependency inspection is deliberately **fail-closed**. PostgreSQL event
triggers are database-wide, so pg_trickle must read
`pgtrickle.pgt_dependencies` before it can determine whether an altered or
dropped table is tracked. If that lookup cannot complete, for example because
an `ACCESS EXCLUSIVE` lock causes `lock_timeout`, the originating DDL is
aborted rather than risk missing a schema change and leaving CDC or a stream
table inconsistent. This can temporarily block DDL on an unrelated table;
retry the statement after the catalog contention clears.

### How do I check if a source table has switched from trigger-based CDC to WAL-based CDC?

When you enable hybrid CDC (`pg_trickle.cdc_mode = 'auto'`), pg_trickle starts capturing changes with triggers and can automatically transition to WAL-based logical replication once conditions are met. There are several ways to check the current CDC mode for each source table:

**1. Query the dependency catalog directly:**

```sql
SELECT d.source_relid, c.relname AS source_table, d.cdc_mode,
       d.slot_name, d.decoder_confirmed_lsn, d.transition_started_at
FROM pgtrickle.pgt_dependencies d
JOIN pg_class c ON c.oid = d.source_relid;
```

The `cdc_mode` column shows one of three values:
- `TRIGGER` — changes are captured via trigger-based CDC (statement-level by default)
- `TRANSITIONING` — the system is in the process of switching from triggers to WAL
- `WAL` — changes are captured via logical replication

**2. Use the built-in health check function:**

```sql
SELECT source_table, cdc_mode, slot_name, lag_bytes, alert
FROM pgtrickle.check_cdc_health();
```

This returns a row per source table with the current mode, replication slot lag (for WAL-mode sources), and any alert conditions such as `slot_lag_exceeds_threshold` or `replication_slot_missing`.

**3. Listen for real-time transition notifications:**

```sql
LISTEN pg_trickle_cdc_transition;
```

pg_trickle sends a `NOTIFY` with a JSON payload whenever a transition starts, completes, or is rolled back. Example payload:

```json
{
  "event": "transition_complete",
  "source_table": "public.orders",
  "old_mode": "TRANSITIONING",
  "new_mode": "WAL",
  "slot_name": "pg_trickle_slot_16384"
}
```

This lets you integrate CDC mode changes into your monitoring stack without polling.

**4. Check the global GUC setting:**

```sql
SHOW pg_trickle.cdc_mode;
```

This shows the *desired* global behavior (`trigger`, `auto`, or `wal`), not the per-table actual state. The per-table state lives in `pgt_dependencies.cdc_mode` as described above.

See [CONFIGURATION.md](CONFIGURATION.md) for details on the `pg_trickle.cdc_mode`, `pg_trickle.wal_transition_timeout`, `pg_trickle.slot_lag_warning_threshold_mb`, and `pg_trickle.slot_lag_critical_threshold_mb` GUCs.

### Is it safe to add triggers to a stream table while the source table is switching CDC modes?

**Yes, this is completely safe.** CDC mode transitions and user-defined triggers operate on different tables and do not interfere with each other:

- **CDC transitions** affect how changes are captured from **source tables** (e.g., `orders`). The transition switches the capture mechanism from row-level triggers on the source table to WAL-based logical replication.
- **User-defined triggers** live on **stream tables** (e.g., `order_totals`) and control how the refresh engine *applies* changes to the materialized output.

Because these are independent concerns, you can freely add, modify, or remove triggers on a stream table at any point — including during an active CDC transition on its source tables.

**How it works in practice:**

1. The refresh engine checks for user-defined triggers on the stream table at the start of each refresh cycle (via a fast `pg_trigger` lookup, <0.1 ms).
2. If user triggers are detected, the engine uses explicit `DELETE` / `UPDATE` / `INSERT` statements instead of `MERGE`, so your triggers fire with correct `TG_OP`, `OLD`, and `NEW` values.
3. The change data consumed by the refresh engine has the same format regardless of whether it came from CDC triggers or WAL decoding — so the trigger detection and the CDC mode are fully decoupled.

A trigger added between two refresh cycles will simply be picked up on the next cycle. The only (theoretical) edge case is adding a trigger in the tiny window *during* a single refresh transaction, between the trigger-detection check and the MERGE execution — but since both happen within the same transaction, this is virtually impossible in practice.

### Why does pg_trickle use triggers instead of logical replication for initial CDC?

pg_trickle always bootstraps CDC with triggers because they provide **single-transaction atomicity** — the change record is written in the same transaction as the source DML, so:

1. **No commit-order ambiguity.** The change buffer always reflects committed data; rolled-back transactions never produce partial change records.
2. **No replication slot management at creation time.** Logical replication requires creating and monitoring replication slots, which can bloat WAL if the subscriber falls behind. Trigger-based bootstrap avoids this complexity.
3. **Works on all hosting providers.** Some managed PostgreSQL services restrict `wal_level = logical` or limit the number of replication slots. Trigger bootstrap works everywhere, with no configuration changes.
4. **Simpler initial deployment.** No need for `wal_level = logical`, no publication/subscription setup, and no extra connections for WAL senders.

With `pg_trickle.cdc_mode = 'auto'` (the default), pg_trickle uses triggers initially and then transparently transitions to WAL-based CDC if `wal_level = logical` is available. If WAL is not available, triggers are kept permanently — no degradation, no errors. Set `pg_trickle.cdc_mode = 'trigger'` if you want to disable WAL transitions entirely. See ADR-001 and ADR-002 in the architecture documentation for the full rationale.

### Why is `auto` the default `pg_trickle.cdc_mode`?

`auto` is the default CDC mode. It was changed from `trigger` based on the following considerations:

**1. Safe no-op on standard installs.**
PostgreSQL ships with `wal_level = replica` by default. In this configuration, `auto` simply stays on trigger-based CDC permanently — it does not create replication slots, publications, or any WAL infrastructure. There is no error, warning, or user-visible difference from the old `trigger` default. `auto` only activates the WAL transition path when `wal_level = logical` is explicitly configured by the operator.

**2. Automatic fallback hardening.**
The WAL transition and steady-state polling now include robust automatic fallback:
- Consecutive poll errors (5 failures) trigger automatic revert to triggers.
- `check_decoder_health()` validates slot existence, WAL lag, and `wal_level` on every tick.
- The `TRANSITIONING` phase has a progressive timeout with informative warnings.
- Post-restart health checks (`check_cdc_transition_health()`) automatically clean up stale transitions.

**3. Zero overhead for trigger-only deployments.**
When `wal_level != logical`, the `auto` scheduler branch takes a fast-path exit after a single GUC check and `pg_replication_slots` query. The overhead compared to `trigger` mode is negligible (<1 ms per scheduler tick).

**4. Progressive optimisation without config changes.**
When an operator later enables `wal_level = logical` (e.g., for other replication needs), pg_trickle automatically benefits from lower per-row CDC overhead (~5–15 μs vs ~20–55 μs) without any configuration change. This aligns with the principle of least surprise.

**When to use `trigger` instead:** Set `pg_trickle.cdc_mode = 'trigger'` if you want fully deterministic trigger-only behaviour, need to minimize any replication slot management, or are on a restricted managed PostgreSQL that caps replication slots. This reverts to the legacy trigger-only default.

**Caveats to be aware of in `auto` mode:**
- Keyless tables (no PRIMARY KEY) stay on triggers permanently — WAL mode requires a PK for `pk_hash` computation.
- Replication slots prevent WAL recycling: if the decoder falls behind, WAL accumulates. pg_trickle now warns at `pg_trickle.slot_lag_warning_threshold_mb` (default 100 MB) and marks per-source CDC health unhealthy at `pg_trickle.slot_lag_critical_threshold_mb` (default 1024 MB).
- The `TRANSITIONING` phase runs both trigger and WAL decoder simultaneously; LSN-based deduplication handles correctness. If anything goes wrong, the system rolls back to triggers.

### How does the trigger-to-WAL automatic transition work?

When `pg_trickle.cdc_mode = 'auto'`, pg_trickle monitors each source table's write rate. When the rate exceeds an internal threshold, the transition proceeds in three phases:

1. **Slot creation.** A logical replication slot is created for the source table's OID (e.g., `pg_trickle_slot_16384`).
2. **Dual capture.** For a brief period, both triggers and WAL decoding capture changes. The system uses LSN comparison to deduplicate, ensuring no changes are lost or double-counted.
3. **Trigger removal.** Once the WAL decoder has confirmed it is caught up (its confirmed LSN ≥ the frontier LSN), the trigger-based CDC objects are dropped and the source transitions fully to WAL mode.

The transition is tracked in `pgt_dependencies.cdc_mode` (values: `TRIGGER` → `TRANSITIONING` → `WAL`). If the transition times out (`pg_trickle.wal_transition_timeout`, default 5 minutes), it is rolled back and triggers are kept.

### What happens to CDC if I restore a database backup?

After restoring a backup (pg_dump, pg_basebackup, or PITR), the CDC state depends on the backup type:

| Backup type | Triggers | Change buffers | Frontier | Action needed |
|---|---|---|---|---|
| **pg_dump (logical)** | Preserved (in DDL) | Buffer rows included | Catalog restored | Usually none — next refresh detects stale frontier and does a full refresh |
| **pg_basebackup (physical)** | Preserved | Buffer rows preserved (committed at backup time) | Catalog restored | Replication slots may be invalid — WAL-mode sources may need manual transition back to TRIGGER mode |
| **PITR (point-in-time)** | Preserved | Only committed buffer rows at the recovery target | Catalog restored | Similar to pg_basebackup; frontier may point ahead of actual buffer content → first refresh does a full refresh to reconcile |

In all cases, the pg_trickle scheduler automatically detects frontier inconsistencies and falls back to a full refresh for the first cycle after restore. No manual intervention is required for trigger-mode sources.

For full guidelines on disaster recovery strategies, see our dedicated [Backup and Restore](BACKUP_AND_RESTORE.md) chapter.

For WAL-mode sources, replication slots created after the backup point will not exist in the restored state. Set `pg_trickle.cdc_mode = 'trigger'` temporarily, or let the auto transition recreate slots.

### Do CDC triggers fire for rows inserted via logical replication (subscribers)?

**Yes.** PostgreSQL fires row-level triggers on the subscriber side for rows applied via logical replication. This means if you have a subscriber database with pg_trickle installed, the CDC triggers will capture replicated changes into the local change buffers.

**Implication:** You can run stream tables on a subscriber database that tracks replicated tables — the change capture works transparently. However, be careful about:

- **Double-counting.** If the same table is tracked by pg_trickle on both the publisher and subscriber, changes are captured twice (once on each side). This is fine if the stream tables are independent, but confusing if you expect them to be identical.
- **Replication lag.** The stream table on the subscriber will be delayed by both the replication lag and the pg_trickle refresh schedule.

### Can I inspect the change buffer tables directly?

**Yes.** Change buffers are ordinary tables in the `pgtrickle_changes` schema, named `changes_<source_oid>`:

```sql
-- List all change buffer tables
SELECT tablename FROM pg_tables WHERE schemaname = 'pgtrickle_changes';

-- Inspect recent changes for a source table (find OID first)
SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'orders' AND n.nspname = 'public';

-- Then query the buffer
SELECT action, lsn, txid, old_data, new_data
FROM pgtrickle_changes.changes_16384
ORDER BY lsn DESC LIMIT 10;
```

The `action` column contains: `I` (insert), `U` (update), `D` (delete), or `T` (truncate).

**Warning:** Do not modify buffer tables directly. The refresh engine manages buffer cleanup (truncation) after each successful refresh. Manual changes will corrupt the frontier tracking.

### How does pg_trickle prevent its own refresh writes from re-triggering CDC?

When the refresh engine writes to a stream table (via MERGE or explicit DML), it does **not** trigger CDC capture on that stream table, even if the stream table is itself a source for a downstream stream table. This is because:

1. **CDC triggers are only installed on source tables**, not on stream tables. The refresh engine writes directly to the stream table's storage without going through any change-capture mechanism.
2. **Downstream change propagation uses a different path.** When stream table A is a source for stream table B, changes to A are detected at B's refresh time by re-reading A's data (not via triggers on A). The topological ordering ensures A is refreshed before B.

This design prevents infinite loops (A triggers B triggers A) and avoids the overhead of capturing changes to materialized output that will be recomputed anyway.

---

## Diamond Dependencies & DAG Scheduling

When multiple stream tables form a diamond-shaped dependency graph, careful
coordination is needed to avoid inconsistent snapshots. This section covers
atomic consistency, schedule policies, and topological ordering.

### What is a diamond dependency and why does it matter?

A **diamond dependency** occurs when two (or more) intermediate stream tables both depend on the same source, and a downstream stream table depends on both of them:

```
       Source: orders
       /             \
  ST: totals      ST: counts
       \             /
    ST: combined_report
```

Without coordination, `combined_report` might be refreshed after `totals` is updated but before `counts` is updated (or vice versa), producing a temporarily inconsistent snapshot — `totals` reflects the latest data but `counts` is stale.

### What does `diamond_consistency = 'atomic'` do?

When `diamond_consistency = 'atomic'` is set on the downstream stream table (e.g., `combined_report`), pg_trickle ensures that **all upstream stream tables in the diamond are refreshed within the same scheduler cycle** before the downstream table is refreshed. This guarantees a consistent point-in-time snapshot.

If any upstream refresh in the atomic group fails, the downstream refresh is **skipped** for that cycle to avoid inconsistency. The failed upstream will be retried on the next cycle.

```sql
SELECT pgtrickle.alter_stream_table('combined_report',
    diamond_consistency => 'atomic');
```

### What is the difference between `'fastest'` and `'slowest'` schedule policy?

When a stream table has multiple upstream dependencies with different schedules, pg_trickle needs a policy for when to refresh the downstream table:

| Policy | Behavior | Best for |
|---|---|---|
| `fastest` | Refresh downstream whenever **any** upstream refreshes | Low-latency dashboards where partial freshness is acceptable |
| `slowest` | Refresh downstream only after **all** upstreams have refreshed | Reports requiring all-or-nothing consistency |

The default is `fastest`. Use `slowest` with `diamond_consistency = 'atomic'` for the strongest consistency guarantees.

### What happens when an atomic diamond group partially fails?

When `diamond_consistency = 'atomic'` is set and one upstream stream table in the diamond fails to refresh:

1. The **downstream** refresh is **skipped** for that cycle (it reads stale-but-consistent data from the previous successful cycle).
2. The **failed upstream** follows the normal retry logic (exponential backoff, up to `max_consecutive_errors`).
3. Other **non-failing upstreams** in the diamond are still refreshed normally — their data is fresh, but the downstream won't consume it until all upstreams succeed.
4. A `NOTIFY pg_trickle_alert` with event `diamond_partial_failure` is sent so your monitoring can detect the situation.

### How does pg_trickle determine topological refresh order?

The scheduler builds a **directed acyclic graph (DAG)** of stream table dependencies at startup and after any `create_stream_table` / `drop_stream_table` call. The algorithm:

1. **Edge discovery.** For each stream table, the defining query's source tables are extracted. If a source table is itself a stream table, a dependency edge is added.
2. **Cycle detection.** The DAG is checked for cycles. If a cycle is detected, the offending `create_stream_table` call is rejected with a clear error message listing the cycle path.
3. **Topological sort.** A Kahn's algorithm topological sort produces the refresh order — leaf nodes (no stream table dependencies) are refreshed first, then their dependents, and so on.
4. **Level assignment.** Each stream table is assigned a "level" (0 for leaves, max(parent levels) + 1 for dependents). Stream tables at the same level are refreshed concurrently when `pg_trickle.parallel_refresh_mode = 'on'`.

The topological order is recalculated whenever the DAG changes. You can inspect it with:

```sql
SELECT pgt_name, depends_on, topo_level
FROM pgtrickle.stream_tables_info
ORDER BY topo_level, pgt_name;
```

---

## Schema Changes & DDL Events

pg_trickle detects source table schema changes via PostgreSQL’s DDL event
trigger system and reacts automatically. This section explains what happens
for various DDL operations and how to handle them.

### What happens when I add a column to a source table?

Adding a column to a source table is **safe and non-disruptive** if the stream table's defining query does not use `SELECT *`:

- **Named columns:** If the defining query explicitly lists columns (e.g., `SELECT id, name, amount FROM orders`), the new column is simply not captured by CDC and has no effect on the stream table.
- **`SELECT *`:** If the defining query uses `SELECT *`, pg_trickle detects the schema mismatch at the next refresh and marks the stream table with `needs_reinit = true`. The next scheduler cycle performs a full reinitialization — drops the storage table, recreates it with the new column set, and does a full refresh.

CDC triggers capture the full row as JSONB regardless of which columns the stream table uses, so no trigger changes are needed.

### What happens when I drop a column used in a stream table's query?

Dropping a column that is referenced in a stream table's defining query will cause the next refresh to **fail** because the column no longer exists in the source table. pg_trickle handles this via:

1. **DDL event trigger** detects the `ALTER TABLE ... DROP COLUMN` and marks all affected stream tables with `needs_reinit = true`.
2. On the **next refresh cycle**, the scheduler attempts reinitialization — but the defining query will fail with a PostgreSQL error (e.g., `column "amount" does not exist`).
3. The stream table moves to **ERROR** status after `max_consecutive_errors` failures.
4. A `reinitialize_needed` NOTIFY alert is sent.

**Resolution:** Drop and recreate the stream table with an updated defining query:

```sql
SELECT pgtrickle.drop_stream_table('order_totals');
SELECT pgtrickle.create_stream_table(
    name         => 'order_totals',
    query        => 'SELECT id, name FROM orders',  -- updated query without dropped column
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

### What happens when I `CREATE OR REPLACE` a view used by a stream table?

PostgreSQL event triggers fire on `CREATE OR REPLACE VIEW`, so pg_trickle detects the change and marks dependent stream tables with `needs_reinit = true`. On the next refresh:

- If the **new view definition** is compatible (same output columns, same types), reinitialization succeeds transparently — the stream table is repopulated with the new query logic.
- If the new view definition **changes the output schema** (different columns or types), the delta query will fail and the stream table enters ERROR status.

**Tip:** To avoid disruption, use `pgtrickle.alter_stream_table()` to pause the stream table before replacing the view, then resume after verifying compatibility.

### What happens when I alter or drop a function used in a stream table's query?

If a stream table's defining query calls a user-defined function (e.g., `SELECT my_func(amount) FROM orders`) and that function is altered or dropped:

- **ALTER FUNCTION** (changing the body): pg_trickle does **not** detect this automatically — PostgreSQL does not fire DDL event triggers for function body changes. The stream table continues refreshing with the new function behavior. If this is intentional, no action is needed. If you want a full rebase to the new logic, temporarily switch to FULL mode and refresh:
  ```sql
  SELECT pgtrickle.alter_stream_table('my_st', refresh_mode => 'FULL');
  SELECT pgtrickle.refresh_stream_table('my_st');
  SELECT pgtrickle.alter_stream_table('my_st', refresh_mode => 'DIFFERENTIAL');
  ```
- **DROP FUNCTION**: The next refresh fails because the function no longer exists. The stream table enters ERROR status. Recreate the function or drop and recreate the stream table.

### What is reinitialize and when does it trigger?

**Reinitialize** is pg_trickle's mechanism for handling structural changes to source tables. When a stream table is marked with `needs_reinit = true`, the next scheduler cycle performs:

1. **Drop** the existing storage table (the physical heap table backing the stream table).
2. **Recreate** the storage table from the defining query's current output schema.
3. **Full refresh** — run the defining query against current source data and populate the new storage table.
4. **Reset** the frontier to the current LSN.
5. **Clear** the `needs_reinit` flag.

Reinitialize triggers automatically when:
- DDL event triggers detect `ALTER TABLE`, `DROP TABLE`, or `CREATE OR REPLACE VIEW` on source tables or intermediate views.
- A `needs_reinit` NOTIFY alert is sent.
- You can also trigger it manually:
  ```sql
  UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = true WHERE pgt_name = 'my_st';
  ```

### Can I block DDL on tracked source tables?

pg_trickle does not currently block DDL on source tables — it only **reacts** to DDL changes via event triggers. If you want to prevent accidental schema changes on critical source tables, use PostgreSQL's built-in mechanisms:

```sql
-- Revoke ALTER/DROP from application roles
REVOKE ALL ON TABLE orders FROM app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE orders TO app_user;
-- Only the table owner (or superuser) can now ALTER/DROP
```

Alternatively, create a custom event trigger that raises an exception when DDL targets tracked source tables:

```sql
CREATE OR REPLACE FUNCTION prevent_source_ddl() RETURNS event_trigger AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_event_trigger_ddl_commands() cmd
        JOIN pgtrickle.pgt_dependencies d ON d.source_relid = cmd.objid
    ) THEN
        RAISE EXCEPTION 'Cannot ALTER/DROP a table tracked by pg_trickle';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE EVENT TRIGGER guard_source_ddl ON ddl_command_end
EXECUTE FUNCTION prevent_source_ddl();
```

### What happens if I run DDL on a source table during an active refresh? {#ec-17}

**PostgreSQL's locking mechanism prevents most conflicts.** The refresh transaction acquires a `ShareLock` on source tables before reading them. Since `ALTER TABLE` (including `ADD COLUMN`, `DROP COLUMN`, `ALTER TYPE`) requires an `AccessExclusiveLock`, the DDL statement **blocks** until the refresh transaction completes.

In practice:
- **During a refresh:** The ALTER TABLE waits for the refresh to finish, then proceeds. pg_trickle's DDL event trigger then detects the change and marks the stream table for reinitialization.
- **Between refreshes:** DDL proceeds immediately. The next refresh picks up the reinitialization flag.

There is a tiny theoretical window between lock acquisition and the first read where DDL could sneak in, but this is prevented by PostgreSQL's MVCC — the refresh's snapshot was taken before the DDL committed, so it reads the old schema regardless.

**If `pg_trickle.block_source_ddl = true`:** Column-affecting DDL on tracked source tables is rejected entirely with an ERROR, regardless of whether a refresh is running.

### Do stream tables work with logical replication? {#ec-21-22-23}

**Stream tables are replicated to standbys via physical (streaming) replication** like any other heap table. However, they are **not** automatically maintained by pg_trickle on the subscriber:

| Aspect | Primary | Physical standby | Logical subscriber |
|---|---|---|---|
| **Scheduler runs** | Yes | No (read-only) | No (no pg_trickle catalog) |
| **Stream tables readable** | Yes | Yes (replicated) | Only if published |
| **Refreshes occur** | Yes | No (standby is read-only) | No |
| **Change buffers** | Managed by pg_trickle | Replicated but not consumed | Not available |

**Key limitations:**
- Change buffer tables (`pgtrickle_changes.*`) are **not published** through logical replication — they are internal transient data.
- The pg_trickle catalog (`pgtrickle.pgt_stream_tables`) is not replicated through logical replication.
- On a physical standby, stream tables receive updates through streaming replication with the usual replication lag.

**Recommended pattern:** Run pg_trickle on the primary only. Read stream tables from any physical standby.

---

## Performance & Tuning

This section covers scheduler tuning, the adaptive FULL fallback, disk space
management, and guidance on when to use DIFFERENTIAL vs. FULL mode.

### How do I tune the scheduler interval?

The `pg_trickle.scheduler_interval_ms` GUC controls how often the scheduler checks for stale stream tables (default: 1000 ms).

| Workload | Recommended Value |
|---|---|
| Low-latency (near real-time) | `100`–`500` |
| Standard | `1000` (default) |
| Low-overhead (many STs, long schedules) | `5000`–`10000` |

### Is there any risk in setting `min_schedule_seconds` very low?

Yes. `pg_trickle.min_schedule_seconds` (default: 60) is a safety guardrail, not an arbitrary limit. Setting it very low — especially in production — can cause several problems:

**WAL amplification.** Every differential refresh writes a `MERGE` to the WAL. At 1-second intervals across many stream tables, WAL generation rises sharply, increasing replication lag and storage costs.

**Lock contention.** Each refresh acquires locks on the change buffer table. With `cleanup_use_truncate = true` (the default), this is an `AccessExclusiveLock`. Sub-second schedules can starve concurrent `INSERT`/`UPDATE`/`DELETE` statements on the source tables.

**Cascading refresh load.** If a refresh takes longer than the schedule interval (e.g., an 800 ms refresh on a 1-second schedule), the next refresh fires almost immediately upon completion. With chained or diamond-shaped ST graphs, the entire topological chain must complete within the interval to avoid falling behind.

**Autovacuum pressure.** Rapid `MERGE` operations produce dead tuples in the stream table faster than autovacuum can clean them up, bloating the table and degrading query performance over time.

**Adaptive fallback triggering.** At high change rates, `pg_trickle.differential_max_change_ratio` may trigger a FULL refresh instead of DIFFERENTIAL. A FULL refresh at 1-second intervals is very expensive and defeats the purpose of differential maintenance.

**Practical guidance:**

| Environment | Recommended minimum |
|---|---|
| Development / testing | `1` s — fine for fast iteration |
| Lightly loaded production | `10`–`30` s |
| Standard production | `60` s (default) |
| High-throughput OLTP | `120`+ s — let change buffers accumulate for efficient batch merging |

If you need near-real-time results, consider **IMMEDIATE mode** (`refresh_mode => 'DIFFERENTIAL'` with same-transaction refresh) instead of a very short schedule — it avoids the scheduler overhead entirely and updates the stream table within your transaction.

### What is the adaptive fallback to FULL?

When the number of pending changes exceeds `pg_trickle.differential_max_change_ratio` (default: 15%) of the source table size, DIFFERENTIAL mode automatically falls back to FULL for that refresh cycle. This prevents pathological delta queries on bulk changes.

- Set to `0.0` to always use DIFFERENTIAL (even on large change sets)
- Set to `1.0` to effectively always use FULL
- Default `0.15` (15%) is a good balance

### How many concurrent refreshes can run?

By default (`parallel_refresh_mode = 'on'`) the scheduler can dispatch independent refresh units to dynamic background workers. Set `pg_trickle.parallel_refresh_mode = 'off'` when you want sequential refreshes within each per-database scheduler for a resource-constrained or diagnostic deployment.

**True parallel refresh** is available via:

```sql
ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'on';
ALTER SYSTEM SET pg_trickle.max_dynamic_refresh_workers = 4;  -- cluster-wide cap
ALTER SYSTEM SET pg_trickle.max_concurrent_refreshes = 4;    -- per-database cap
SELECT pg_reload_conf();
```

When enabled, independent stream tables at the same DAG level are refreshed concurrently in separate dynamic background workers. Each worker uses one `max_worker_processes` slot — see the [worker-budget formula](CONFIGURATION.md#pg_tricklemax_dynamic_refresh_workers) before enabling.

Monitor parallel refresh with:

```sql
SELECT * FROM pgtrickle.worker_pool_status();
SELECT * FROM pgtrickle.parallel_job_status(60);
```

For most deployments with fewer than 100 stream tables, sequential processing is still efficient (each differential refresh typically takes 5–50 ms).

### How do I check if my stream tables are keeping up?

```sql
-- Quick overview
SELECT pgt_name, status, staleness, stale
FROM pgtrickle.stream_tables_info;

-- Detailed statistics
SELECT pgt_name, total_refreshes, avg_duration_ms, consecutive_errors, stale
FROM pgtrickle.pg_stat_stream_tables;

-- Recent refresh history for a specific ST
SELECT * FROM pgtrickle.get_refresh_history('order_totals', 10);
```

### What is `__pgt_row_id`?

Every stream table has a `__pgt_row_id BIGINT PRIMARY KEY` column that stores a 64-bit xxHash of the row's identity key. The refresh engine uses it to match incoming deltas against existing rows during `MERGE` operations.

For a detailed explanation of how this column is computed and why it exists, see [What is the `__pgt_row_id` column and why does it appear in my stream tables?](#what-is-the-__pgt_row_id-column-and-why-does-it-appear-in-my-stream-tables) in the General section.

**You should ignore this column in your queries.** It is an implementation detail.

### How much disk space do change buffer tables consume?

Each change buffer table stores one row per source-table change (INSERT, UPDATE, DELETE, or TRUNCATE marker). The row size depends on the source table's column count and data types:

| Component | Approximate size |
|---|---|
| `action` column (char) | 1 byte |
| `old_data` / `new_data` (JSONB) | 1–10 KB per row (depends on source columns) |
| `lsn` (pg_lsn) | 8 bytes |
| `txid` (xid8) | 8 bytes |
| **Index** (on lsn) | ~40 bytes per row |

**Rule of thumb:** Buffer tables consume roughly **2–3× the raw row size** of the source change, because both OLD and NEW values are stored as JSONB.

Buffer tables are cleaned up (truncated or deleted) after each successful refresh. If you suspect buffer bloat, check:

```sql
SELECT relname, pg_size_pretty(pg_total_relation_size(oid)) AS size
FROM pg_class
WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'pgtrickle_changes')
ORDER BY pg_total_relation_size(oid) DESC;
```

### What determines whether DIFFERENTIAL or FULL is faster for a given workload?

The breakeven point depends on the **change ratio** — the number of changed rows relative to the total source table size:

| Change ratio | Recommended mode | Why |
|---|---|---|
| < 5% | DIFFERENTIAL | Delta query touches few rows; much cheaper than re-reading everything |
| 5–15% | DIFFERENTIAL (usually) | Still faster, but approaching the crossover |
| 15–50% | FULL | The delta query scans a large fraction of the source anyway; FULL avoids the overhead of delta computation |
| > 50% | FULL | Bulk load scenario — TRUNCATE + INSERT is simpler and faster |

Additional factors:
- **Query complexity:** Queries with many joins or window functions have more expensive delta computation. The crossover shifts lower.
- **Source table size:** For small tables (<10K rows), FULL is nearly always faster because the overhead is negligible.
- **Index presence:** DIFFERENTIAL uses indexes to look up changed rows. Missing indexes on join keys or GROUP BY columns can make delta queries slow.

The adaptive fallback (`pg_trickle.differential_max_change_ratio`, default 0.15) automates this decision per-cycle.

### What are the planner hints and when should I disable them?

Before executing a delta query, pg_trickle sets several session-level planner parameters to guide PostgreSQL toward efficient delta plans:

```sql
SET LOCAL enable_seqscan = off;     -- Prefer index scans for small deltas
SET LOCAL enable_nestloop = on;     -- Nested loops are good for small delta × large table joins
SET LOCAL enable_mergejoin = off;   -- Merge joins are worse for skewed delta sizes
```

These hints are active only during the refresh transaction and are reset afterward.

**When to disable hints:** If you notice that a particular stream table's refresh is slow (check `avg_duration_ms` in `pg_stat_stream_tables`), the planner hints may be suboptimal for that specific query. You can disable them by setting:

```sql
SET pg_trickle.planner_hints = off;
```

This allows PostgreSQL's planner to choose its own strategy. Test both settings and compare `avg_duration_ms`.

### How do prepared statements help refresh performance?

The refresh engine uses PostgreSQL prepared statements (`PREPARE` / `EXECUTE`) for the delta and MERGE queries. On the first refresh, the statement is prepared; subsequent refreshes reuse the cached plan. Benefits:

- **Reduced planning overhead.** For complex delta queries with many joins and CTEs, planning can take 5–50 ms. Prepared statements skip this on subsequent refreshes.
- **Stable plans.** The planner uses generic plans after the 5th execution (PostgreSQL default), avoiding plan instability from statistic fluctuations.

Prepared statements are stored per-session and are invalidated when:
- The stream table is reinitialized (schema change)
- The shared cache generation advances after DDL or stream-table metadata changes
- The PostgreSQL connection is recycled
- The session ends

### How does the adaptive FULL fallback threshold work in practice?

The `pg_trickle.differential_max_change_ratio` GUC (default: 0.15) is evaluated **per source table, per refresh cycle:**

1. Before each differential refresh, the engine counts pending changes in the buffer table: `pending_changes = COUNT(*) FROM pgtrickle_changes.changes_<oid>`.
2. It estimates the source table size from `pg_class.reltuples`.
3. If `pending_changes / reltuples > differential_max_change_ratio`, the engine falls back to FULL for that cycle.

**Edge cases:**
- If the source table has `reltuples = 0` (freshly created, no ANALYZE yet), the engine always uses FULL until statistics are available.
- For multi-source stream tables (joins), each source is evaluated independently. If **any** source exceeds the threshold, the entire refresh falls back to FULL.
- The threshold applies to the current cycle only — the next cycle re-evaluates.

### How many stream tables can a single PostgreSQL instance handle?

There is no hard limit. Practical limits depend on:

| Factor | Guideline |
|---|---|
| **Scheduler overhead** | Each cycle iterates all STs; at 1000 STs with 1ms overhead per check, the cycle takes ~1s |
| **Background connections** | 1 per database (the scheduler) + 1 per manual refresh call |
| **Change buffer bloat** | Each source table gets its own buffer table — many sources = many tables in `pgtrickle_changes` |
| **Catalog size** | `pgt_stream_tables` and `pgt_dependencies` grow linearly |
| **Refresh throughput** | Sequential processing means total cycle time = sum of individual refresh times |

**Tested benchmarks:** Up to 500 stream tables on a single instance with <2s total cycle time for DIFFERENTIAL refreshes averaging 3ms each.

### What is the TRUNCATE vs DELETE cleanup trade-off for change buffers?

After each successful refresh, the engine cleans up processed change records from the buffer table. The `pg_trickle.cleanup_use_truncate` GUC (default: `true`) controls the method:

| Method | Pros | Cons |
|---|---|---|
| `TRUNCATE` (default) | Instant — O(1) regardless of row count. Reclaims disk space immediately. | Takes an `ACCESS EXCLUSIVE` lock on the buffer table, briefly blocking concurrent INSERTs from CDC triggers (~0.1 ms typical). |
| `DELETE` | Row-level lock only — no blocking of concurrent CDC writes. | O(N) — proportional to the number of processed rows. Dead tuples require VACUUM to reclaim space. |

**When to switch to DELETE:** If your source table has extremely high write throughput (>10K writes/sec) and you observe brief stalls in DML latency during refresh cleanup, switch to DELETE:

```sql
ALTER SYSTEM SET pg_trickle.cleanup_use_truncate = false;
SELECT pg_reload_conf();
```

For most workloads, TRUNCATE is the better choice because buffer tables are typically emptied completely after each refresh.

---

## Interoperability

Stream tables are standard PostgreSQL heap tables, which means they work with
most PostgreSQL features. This section clarifies what’s compatible (views,
replication, triggers) and what’s not (direct DML, foreign keys).

### Can PostgreSQL views reference stream tables?

**Yes.** Since stream tables are standard PostgreSQL heap tables, you can create views on top of them just like any other table. The view will return whatever data is currently in the stream table, reflecting the most recent refresh:

```sql
CREATE VIEW high_value_customers AS
SELECT customer_id, total FROM pgtrickle.order_totals WHERE total > 1000;
```

This is a common pattern for adding per-user filters or formatting on top of a shared stream table.

### Can materialized views reference stream tables?

**Yes**, though this is usually redundant — both materialized views and stream tables are physical snapshots of query results. The key difference is that the materialized view requires its own manual `REFRESH MATERIALIZED VIEW` call; it does not auto-refresh when the underlying stream table refreshes.

A more idiomatic approach is to create a **second stream table** that references the first one. This way, pg_trickle handles the dependency ordering and refresh scheduling for both automatically.

### Can I replicate stream tables with logical replication?

**Yes.** Stream tables can be published like any ordinary table:

```sql
CREATE PUBLICATION my_pub FOR TABLE pgtrickle.order_totals;
```

**Important caveats:**
- The `__pgt_row_id` column is replicated (it is the primary key)
- Subscribers receive materialized data, not the defining query
- Do **not** install pg_trickle on the subscriber and attempt to refresh the replicated table — it will have no CDC triggers or catalog entries
- Internal change buffer tables are not published by default

### Can I `INSERT`, `UPDATE`, or `DELETE` rows in a stream table directly?

**No.** Stream table contents are managed exclusively by the refresh engine, and direct DML will corrupt the internal state (row IDs, frontier tracking, and change buffer consistency). See [Why can't I INSERT, UPDATE, or DELETE rows in a stream table?](#why-cant-i-insert-update-or-delete-rows-in-a-stream-table) for a detailed explanation of what goes wrong.

If you need to post-process stream table data, create a **view** or a **second stream table** that references the first one.

### Can I add foreign keys to or from stream tables?

**No.** Foreign key constraints are incompatible with how the refresh engine operates. The engine uses bulk `MERGE` operations that apply inserts and deletes atomically, without guaranteeing the row-by-row ordering that foreign key checks require. Full refreshes also use `TRUNCATE` + `INSERT`, which bypasses cascade logic entirely.

See [Why can't I add foreign keys?](#why-cant-i-add-foreign-keys-to-or-from-a-stream-table) for details. If you need referential integrity, enforce it in your application or in a view that joins the stream tables.

### Can I add my own triggers to stream tables?

**Yes, for DIFFERENTIAL mode stream tables.** When user-defined row-level triggers are detected, the refresh engine automatically switches from `MERGE` to explicit `DELETE` + `UPDATE` + `INSERT` statements. This ensures triggers fire with the correct `TG_OP`, `OLD`, and `NEW` values. Legacy configs that still set `pg_trickle.user_triggers = 'on'` are treated the same as `auto`.

**Limitations:**
- Row-level triggers do **not** fire during FULL refresh (they are automatically suppressed via `DISABLE TRIGGER USER`). Use `REFRESH MODE DIFFERENTIAL` for stream tables with triggers.
- The `IS DISTINCT FROM` guard prevents no-op `UPDATE` triggers when the aggregate result is unchanged.
- `BEFORE` triggers that modify `NEW` will affect the stored value — the next refresh may "correct" it back, causing oscillation.

See the `pg_trickle.user_triggers` GUC in [CONFIGURATION.md](CONFIGURATION.md) for control options.

### Can I `ALTER TABLE` a stream table directly?

**No.** Direct `ALTER TABLE` would change the physical table without updating pg_trickle's catalog, causing column mismatches and `__pgt_row_id` invalidation on the next refresh. See [Why can't I ALTER TABLE a stream table directly?](#why-cant-i-alter-table-a-stream-table-directly) for details.

Instead, use the pg_trickle API:

```sql
-- Change schedule, mode, or status:
SELECT pgtrickle.alter_stream_table('order_totals', schedule => '10m');

-- To change the defining query or column structure, drop and recreate:
SELECT pgtrickle.drop_stream_table('order_totals');
SELECT pgtrickle.create_stream_table(
    name         => 'order_totals',
    query        => '...',
    schedule     => '5m',
    refresh_mode => 'DIFFERENTIAL'
);
```

### Does pg_trickle work with PgBouncer or other connection poolers?

**It depends on the pooling mode.** pg_trickle's background scheduler uses session-level features that are incompatible with **transaction-mode** connection pooling:

| Feature | Issue with Transaction-Mode Pooling |
|---|---|
| `pg_advisory_lock()` | Session-level lock released when connection returns to pool — concurrent refreshes possible |
| `PREPARE` / `EXECUTE` | Prepared statements are session-scoped — "does not exist" errors on different connections |
| `LISTEN` / `NOTIFY` | Notifications lost when listeners change connections |

**Recommended configurations:**

- **Session-mode pooling** (`pool_mode = session`): Fully compatible. The scheduler holds a dedicated connection.
- **Direct connection** (no pooler for the scheduler): Fully compatible. Application queries can still go through a pooler.
- **Transaction-mode pooling** (`pool_mode = transaction`): **Not supported.** The scheduler requires a persistent session.

**Tip:** If your infrastructure requires transaction-mode pooling (e.g., AWS RDS Proxy, Supabase), route the pg_trickle background worker through a direct connection while keeping application traffic on the pooler. Most connection poolers support per-database or per-user routing rules.

### Does pg_trickle work with pgvector?

**Partially — it depends on the refresh mode and what the defining query does.**

**What works:**

- **Source tables with `vector` columns.** CDC triggers are generated using PostgreSQL's `format_type()`, which returns the full type name (e.g. `vector(1536)`). Change buffer tables mirror the source schema correctly, so inserts, updates, and deletes on pgvector tables are captured and replayed without issue.
- **Passing vector columns through in DIFFERENTIAL mode.** Stream tables that select, filter (on non-vector columns), or join sources that happen to contain `vector` columns work correctly — the vector data is treated as an opaque value and copied through unchanged.
- **FULL mode with any pgvector expression.** Because FULL mode re-executes the entire defining query, all pgvector operators (`<->`, `<=>`, `<#>`) and functions (`cosine_distance`, `l2_normalize`, etc.) work exactly as they do in a regular query.

**What does not work:**

- **DIFFERENTIAL mode with pgvector distance operators in the query.** The DVM engine needs a differentiation rule for every SQL operator it encounters. Custom operators like `<->` (L2 distance) or `<=>` (cosine distance) are not in the built-in rule set. The engine will fall back automatically to FULL mode if such operators appear in the delta query path. Set `refresh_mode => 'FULL'` explicitly to make this intent clear.
- **Incremental aggregation over vector columns.** There is no meaningful incremental form for aggregates over `vector` values (e.g. averaging embeddings). Use FULL mode for any aggregate that involves vector arithmetic.

**Recommended pattern** for a nearest-neighbour cache or semantic search result set:

```sql
CREATE EXTENSION IF NOT EXISTS vector;

SELECT pgtrickle.create_stream_table(
    name         => 'top_similar_docs',
    query        => $$
        SELECT d.id, d.title, d.embedding,
               d.embedding <=> '[0.1, 0.2, 0.3]'::vector AS distance
        FROM documents d
        ORDER BY distance
        LIMIT 100
    $$,
    schedule     => '5m',
    refresh_mode => 'FULL'
);
```

For use-cases that only carry vector columns through without computing on them, DIFFERENTIAL mode works fine:

```sql
-- Vectors are not used in the delta computation — DIFFERENTIAL is safe here
SELECT pgtrickle.create_stream_table(
    name         => 'active_doc_embeddings',
    query        => $$
        SELECT id, embedding
        FROM documents
        WHERE status = 'published'
    $$,
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## dbt Integration

The **dbt-pgtrickle** package provides a `stream_table` materialization that
lets you manage stream tables through dbt’s standard workflow. This section
covers setup, commands, freshness checks, and query change handling.

### How do I use pg_trickle with dbt?

Install the **dbt-pgtrickle** package (a pure Jinja SQL macro package — no Python dependencies):

```yaml
# packages.yml
packages:
  - package: pg_trickle/dbt_pgtrickle
    version: ">=0.2.0"
```

Then define a stream table model using the `stream_table` materialization:

```sql
-- models/order_totals.sql
{{ config(
    materialized='stream_table',
    schedule='1m',
    refresh_mode='DIFFERENTIAL'
) }}

SELECT customer_id, SUM(amount) AS total
FROM {{ source('public', 'orders') }}
GROUP BY customer_id
```

The `stream_table` materialization calls `pgtrickle.create_stream_table()` on the first run and `pgtrickle.alter_stream_table()` on subsequent runs (if the schedule or mode changes).

### What dbt commands work with stream tables?

| Command | Behavior |
|---|---|
| `dbt run` | Creates stream tables that don't exist; updates schedule/mode if changed; **does not** alter the defining query of existing STs |
| `dbt run --full-refresh` | Drops and recreates all stream tables from scratch (new defining query, fresh data) |
| `dbt test` | Works normally — tests query the stream table as a regular table |
| `dbt source freshness` | Works if you configure a freshness block on the stream table source |
| `dbt docs generate` | Documents stream tables like any other model |

### How does `dbt run --full-refresh` work with stream tables?

When `--full-refresh` is passed, the `stream_table` materialization:

1. Calls `pgtrickle.drop_stream_table('model_name')` to remove the existing stream table, CDC triggers, and change buffers.
2. Calls `pgtrickle.create_stream_table(...)` with the current defining query from the model file.
3. The new stream table starts in INITIALIZING status and performs its first full refresh.

This is the correct way to update a stream table's defining query in dbt. Without `--full-refresh`, dbt will **not** detect query changes (it only compares schedule and mode).

### How do I check stream table freshness in dbt?

Use dbt's built-in `source freshness` feature by adding a freshness block to your source definition:

```yaml
# models/sources.yml
sources:
  - name: pgtrickle
    schema: pgtrickle
    tables:
      - name: order_totals
        loaded_at_field: "last_refreshed_at"  # from stream_tables_info
        freshness:
          warn_after: {count: 5, period: minute}
          error_after: {count: 15, period: minute}
```

Then run `dbt source freshness` to check.

Alternatively, query the pg_trickle monitoring views directly in a dbt test:

```sql
-- tests/check_freshness.sql
SELECT pgt_name FROM pgtrickle.stream_tables_info WHERE stale = true
```

### What happens when the defining query changes in dbt?

If you modify the SQL in a stream table model file and run `dbt run` **without** `--full-refresh`:

- The `stream_table` materialization detects that the stream table already exists.
- It compares the schedule and refresh mode — if either changed, it calls `alter_stream_table()` to update them.
- It does **not** compare the defining query text. The existing defining query remains in effect.

To apply a new defining query, you must run `dbt run --full-refresh`. This drops and recreates the stream table with the new query.

**Recommendation:** After changing a model's SQL, always run `dbt run --full-refresh -s model_name` to apply the change.

### Can I use `dbt snapshot` with stream tables?

**Yes, with caveats.** dbt snapshots work by tracking changes to a source table over time using `updated_at` or `check` strategies. You can snapshot a stream table like any other table.

However, keep in mind:
- Stream tables are refreshed periodically, not on every write. The snapshot will only capture changes at refresh boundaries, not at the granularity of individual source-table writes.
- The `__pgt_row_id` column will appear in the snapshot. You may want to exclude it with `check_cols` or a `select` in the snapshot configuration.
- FULL refresh mode replaces all rows each cycle, which will appear as "updates" to the snapshot strategy even if the data hasn't changed. Use DIFFERENTIAL mode for stream tables that are snapshotted.

### What dbt versions are supported?

dbt-pgtrickle is a pure Jinja SQL macro package that works with:

- **dbt-core** 1.7+ (the `stream_table` materialization uses standard Jinja patterns)
- **dbt-postgres** adapter (required for PostgreSQL connection)

There are no Python dependencies beyond dbt-core and dbt-postgres. The package is tested against dbt 1.7.x and 1.8.x in CI.

---

## Row-Level Security (RLS)

### Does RLS on source tables affect stream table content?

**Yes.** Every refresh evaluates defining SQL as the stream-table owner with
`row_security = on`. The owner's source grants and applicable policies define
the materialized result. The manual caller, scheduler role, or DML issuer does
not change that result.

### Can I use RLS on a stream table to filter reads per role?

**Yes.** Stream tables are regular PostgreSQL tables, so `ALTER TABLE …
ENABLE ROW LEVEL SECURITY` and `CREATE POLICY` work exactly as expected.
This is the **recommended pattern** for multi-tenant filtering:

```sql
ALTER TABLE pgtrickle.order_totals ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation ON pgtrickle.order_totals
    USING (tenant_id = current_setting('app.tenant_id')::INT);
```

Target-table RLS can further filter reads. If source policies materialize only
an owner-specific subset, use separate stream owners/tables for different
source-visible subsets.

### What happens when I ENABLE or DISABLE RLS on a source table?

pg_trickle's DDL event trigger detects `ALTER TABLE … ENABLE ROW LEVEL
SECURITY`, `DISABLE ROW LEVEL SECURITY`, `FORCE ROW LEVEL SECURITY`, and
`NO FORCE ROW LEVEL SECURITY` on source tables and marks all dependent
stream tables for reinitialisation. The same applies to `CREATE POLICY`,
`ALTER POLICY`, and `DROP POLICY`.

### Why are IVM trigger functions SECURITY DEFINER?

In IMMEDIATE mode, the IVM trigger fires in the DML-issuing user's context.
`SECURITY DEFINER` permits the trigger to perform private bookkeeping, while
definition-derived delta SQL explicitly switches to the stream owner. The DML
itself remains subject to the issuing user's policies.

The trigger functions also set `search_path = pg_catalog, pgtrickle,
pgtrickle_changes, public` to prevent search_path hijacking — a security best
practice for all `SECURITY DEFINER` functions. The `public` schema is included
because the delta SQL references user tables that typically reside there.

---

## Deployment & Operations

This section covers the operational aspects of running pg_trickle in
production: background workers, upgrades, restarts, replicas, Kubernetes,
partitioned tables, and multi-database deployments.

### How many background workers does pg_trickle use?

pg_trickle uses a **two-tier background worker model**:

1. **Launcher** (`pg_trickle launcher`) — one per cluster, static. Scans `pg_database` every ~10 seconds and spawns a per-database scheduler for every database where pg_trickle is installed. Automatically re-spawns schedulers that exit.
2. **Per-database scheduler** (`pg_trickle scheduler`) — one dynamic worker per database with pg_trickle installed.

| Component | Workers | Notes |
|---|---|---|
| Launcher | 1 (static) | Cluster-wide; connects to `postgres` database |
| Scheduler | 1 per database (dynamic) | Persistent per database; drives all refreshes |
| Parallel refresh workers | 0–N per database | Only when `pg_trickle.parallel_refresh_mode = 'on'` |
| WAL decoder | 0 (shared) | Shares the scheduler's SPI connection |
| Manual refresh | 0 | Runs in the caller's session |

### How do I size `max_worker_processes`?

When `max_worker_processes` is too low, the launcher silently fails to spawn schedulers for some databases and retries every **5 minutes**. Those databases stop refreshing with no error in the stream table itself — you only see it in the PostgreSQL log:

```
WARNING:  pg_trickle launcher: could not spawn scheduler for database 'mydb'
```

The minimum formula:

```
max_worker_processes ≥
  1 (pg_trickle launcher)
  + N  (one scheduler per database with pg_trickle installed)
  + max_dynamic_refresh_workers  (only if parallel_refresh_mode = 'on'; default 4)
  + autovacuum_max_workers        (default 3)
  + parallel query workers        (max_parallel_workers_per_gather × concurrent queries)
  + slots for other extensions    (logical replication launcher, etc.)
```

A practical starting point for a cluster with a handful of databases:

```ini
max_worker_processes = 32
```

This value requires a full PostgreSQL restart (not just reload).

### How do I upgrade pg_trickle to a new version?

1. **Install the new shared library** (replace the `.so`/`.dylib` file in PostgreSQL's `lib` directory).
2. **Run the upgrade SQL:**
   ```sql
   ALTER EXTENSION pg_trickle UPDATE;
   ```
   This applies migration scripts (e.g., `pg_trickle--0.2.1--0.2.2.sql`) that update catalog tables, add new functions, and migrate data as needed.
3. **Restart PostgreSQL** if the shared library changed (required for `shared_preload_libraries` changes).
4. **Verify:**
   ```sql
   SELECT pgtrickle.version();
   ```

**Zero-downtime upgrades** are possible for minor versions (patch releases) that don't change the shared library. Just run `ALTER EXTENSION pg_trickle UPDATE` — no restart needed.

For detailed instructions, version-specific notes, rollback procedures, and troubleshooting, see the full [Upgrading Guide](UPGRADING.md).

### How do I know if my shared library and SQL extension versions match?

The background worker checks for version mismatches at startup and logs a
WARNING if the compiled `.so` version differs from the installed SQL extension
version. You can also check manually:

```sql
-- Compiled .so version:
SELECT pgtrickle.version();

-- Installed SQL extension version:
SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle';
```

If these differ, run `ALTER EXTENSION pg_trickle UPDATE;` and restart
PostgreSQL if prompted.

### Are stream tables preserved during an upgrade?

Yes. `ALTER EXTENSION pg_trickle UPDATE` applies only additive schema
migrations (new columns, updated function signatures). Existing stream tables,
their data, refresh history, and CDC infrastructure are preserved. The
scheduler resumes normal operation after the upgrade completes.

For version-specific migration notes, see the
[Upgrading Guide — Version-Specific Notes](UPGRADING.md#version-specific-notes).

### What happens to stream tables during a PostgreSQL restart?

During a restart:
1. **The scheduler stops.** No refreshes occur while PostgreSQL is down.
2. **CDC triggers are inactive.** Source table writes during the restart window are captured when PostgreSQL comes back up (triggers are persistent DDL objects).
3. **On startup**, the scheduler background worker starts, reads the catalog, rebuilds the DAG, and resumes refresh cycles from where it left off.
4. **Frontier reconciliation.** The scheduler re-establishes a visibility-safe bound before dispatching work. Required CDC buffers must still validate their registered sentinel; missing or crash-cleared state holds the old frontier and requires repair plus a protected FULL refresh.

**Net effect:** Stream tables may be stale for the duration of the downtime, but no data is lost. The first refresh cycle after restart catches up automatically.

### Can I use pg_trickle on a read replica / standby?

**The scheduler does not run on standby servers.** When pg_trickle detects it is running in recovery mode (`pg_is_in_recovery() = true`), the background worker enters a sleep loop and does not attempt any refreshes.

However, stream tables replicated from the primary are **readable** on the standby — they are regular heap tables and are replicated via physical (streaming) replication like any other table.

**Pattern for read-heavy workloads:**
- Run pg_trickle on the **primary** — it performs all refreshes.
- Query stream tables on the **standby** — read replicas get the latest refreshed data via streaming replication, with replication lag as the only additional delay.

### How does pg_trickle work with CloudNativePG / Kubernetes?

pg_trickle is compatible with CloudNativePG. The `cnpg/` directory in the repository contains example manifests:

- [Dockerfile.ext](https://github.com/trickle-labs/pg-trickle/blob/main/cnpg/Dockerfile.ext) — builds a PostgreSQL image with pg_trickle pre-installed
- [cluster-example.yaml](https://github.com/trickle-labs/pg-trickle/blob/main/cnpg/cluster-example.yaml) — CloudNativePG Cluster manifest with `shared_preload_libraries = 'pg_trickle'`

Key considerations:
- Include `pg_trickle` in `shared_preload_libraries` in the Cluster's `postgresql` configuration.
- The scheduler runs on the **primary** pod only. Replica pods detect recovery mode and sleep.
- Pod restarts are handled the same way as PostgreSQL restarts (see above).
- Persistent volume claims preserve catalog and change buffers across pod rescheduling.

### Does pg_trickle work with partitioned source tables?

**Yes.** pg_trickle installs CDC triggers on the **partitioned parent table**, which PostgreSQL automatically propagates to all existing and future partitions. When a row is inserted into any partition, the trigger fires and writes the change to the buffer table.

**Caveats:**
- `TRUNCATE` on individual partitions fires the partition-level trigger, which is also captured.
- Attaching or detaching partitions (`ALTER TABLE ... ATTACH/DETACH PARTITION`) fires DDL event triggers, which may mark the stream table for reinitialization.
- Row movement between partitions (when the partition key is updated) is captured as a DELETE from the old partition and an INSERT into the new partition.

### Can I run pg_trickle in multiple databases on the same cluster?

**Yes.** Each database gets its own independent scheduler background worker, its own catalog tables, and its own change buffers. Stream tables in different databases do not interact.

**Resource planning:** Each database with stream tables requires 1 background worker slot in `max_worker_processes`. If you have many databases, the default of 8 is easily exhausted.

> **Important:** When `max_worker_processes` is exhausted, the launcher silently skips databases it cannot spawn a scheduler for and retries every **5 minutes**. This means stream tables in those databases stop refreshing with no visible error — they just go stale. Check the PostgreSQL log for:
> ```
> WARNING:  pg_trickle launcher: could not spawn scheduler for database 'mydb'
> ```
> If you see this, increase `max_worker_processes` and restart PostgreSQL.

See [How do I size `max_worker_processes`?](#how-do-i-size-max_worker_processes) for the full formula.

```sql
-- On each database where you want pg_trickle:
CREATE EXTENSION pg_trickle;
```

The extension must be created separately in each database — `shared_preload_libraries` loads the shared library cluster-wide, but the SQL objects (catalog tables, functions) are per-database.

---

## Monitoring & Alerting

pg_trickle provides built-in monitoring views and NOTIFY-based alerting.
This section explains the available views, alert events, and failure handling.

### How do I list all stream tables in my database?

Several options depending on how much detail you need:

```sql
-- Quickest: name + status + mode + staleness
SELECT name, status, refresh_mode, is_populated, staleness
FROM pgtrickle.stream_tables_info;

-- Full stats: refresh counts, rows inserted/deleted, avg duration, error streaks
SELECT * FROM pgtrickle.pg_stat_stream_tables;

-- Live status including consecutive_errors and data_timestamp
SELECT * FROM pgtrickle.pgt_status();

-- Raw catalog (all persisted properties, no computed fields)
SELECT * FROM pgtrickle.pgt_stream_tables;
```

### How do I inspect what pg_trickle is doing right now?

**Quick status snapshot:**

```sql
SELECT name, status, refresh_mode, consecutive_errors, staleness
FROM pgtrickle.pgt_status();
```

**Deep dive into a specific stream table** — shows the defining query, DVM
operator tree, source tables, generated delta SQL, and current WAL frontier:

```sql
SELECT * FROM pgtrickle.explain_st('my_table');
```

Key properties returned:

| Property | Description |
|---|---|
| `dvm_supported` | Whether differential maintenance is possible for this query |
| `operator_tree` | How the DVM engine has decomposed the query |
| `delta_query` | The actual SQL executed during a differential refresh |
| `frontier` | Per-source LSN positions flushed at last refresh |

**Recent refresh activity:**

```sql
-- Last 10 refreshes for a stream table (action, status, rows, duration):
SELECT * FROM pgtrickle.get_refresh_history('my_table', 10);

-- Aggregate refresh stats for all stream tables:
SELECT * FROM pgtrickle.st_refresh_stats();
```

**CDC and slot health:**

```sql
-- Per-source CDC mode, WAL lag, and alerts:
SELECT * FROM pgtrickle.check_cdc_health();

-- Replication slot health (slot_name, active, lag_bytes):
SELECT * FROM pgtrickle.slot_health();
```

**Real-time event stream:**

```sql
LISTEN pg_trickle_alert;
-- Receives JSON payloads for: stale_data, auto_suspended, resumed,
-- reinitialize_needed, buffer_growth_warning, refresh_completed, refresh_failed
```

**Pending change buffers** (rows not yet consumed by a differential refresh):

```sql
SELECT stream_table, source_table, cdc_mode, pending_rows, buffer_bytes
FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;
```

### Are there convenience functions for inspecting source tables and CDC buffers?

Yes. pg_trickle provides two functions added to complement the existing monitoring suite:

**`pgtrickle.list_sources(name)`** — shows every source table a stream table depends on, the CDC mode each uses, and any column-level usage metadata:

```sql
SELECT * FROM pgtrickle.list_sources('order_totals');
-- Returns: source_table, source_oid, source_type, cdc_mode, columns_used
```

**`pgtrickle.change_buffer_sizes()`** — shows, for every tracked source table, how many CDC rows are pending (not yet consumed by a differential refresh) and the estimated on-disk size of the change buffer:

```sql
SELECT * FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;
-- Returns: stream_table, source_table, source_oid, cdc_mode, pending_rows, buffer_bytes
```

A large `pending_rows` value for a source table means a differential refresh is overdue or stalled — use `pgtrickle.get_refresh_history()` to investigate.

### Can I see a tree view of all stream table dependencies?

Yes. `pgtrickle.dependency_tree()` walks the dependency DAG and renders it as an indented ASCII tree:

```sql
SELECT tree_line, status, refresh_mode
FROM pgtrickle.dependency_tree();
```

Example output:

```
tree_line                                 | status | refresh_mode
------------------------------------------+--------+--------------
report_summary                            | ACTIVE | DIFFERENTIAL
├── orders_by_region                      | ACTIVE | DIFFERENTIAL
│   ├── public.orders [src]              |        |
│   └── public.customers [src]           |        |
└── revenue_totals                        | ACTIVE | DIFFERENTIAL
    └── public.orders [src]              |        |
```

Each row has `node` (qualified name), `node_type` (`stream_table` or `source_table`), `depth`, `status`, and `refresh_mode`. Source tables are shown as leaves tagged with `[src]`.

### What monitoring views are available?

| View | Description |
|---|---|
| `pgtrickle.stream_tables_info` | Status overview with computed staleness |
| `pgtrickle.pg_stat_stream_tables` | Comprehensive stats (refresh counts, avg duration, error streaks) |

### How do I get alerted when something goes wrong?

pg_trickle sends PostgreSQL `NOTIFY` messages on the `pg_trickle_alert` channel with JSON payloads:

| Event | When |
|---|---|
| `stale_data` | Staleness exceeds 2× the schedule |
| `auto_suspended` | Stream table suspended after max consecutive errors |
| `reinitialize_needed` | Upstream DDL change detected |
| `buffer_growth_warning` | Change buffer growing unexpectedly |
| `refresh_completed` | Refresh completed successfully |
| `refresh_failed` | Refresh failed |

Listen with:
```sql
LISTEN pg_trickle_alert;
```

### What happens when a stream table keeps failing?

After `pg_trickle.max_consecutive_errors` (default: 3) consecutive failures, the stream table moves to `ERROR` status and automatic refreshes stop. An `auto_suspended` NOTIFY alert is sent.

To recover:
```sql
-- Fix the underlying issue (e.g., restore a dropped source table), then:
SELECT pgtrickle.alter_stream_table('my_table', status => 'ACTIVE');
```

Retries use exponential backoff (base 1s, max 60s, ±25% jitter, up to 5 retries before counting as a real failure).

---

## Configuration Reference

All pg_trickle settings are configured via PostgreSQL GUC parameters. The table
below lists the most-used parameters; for the complete reference see
[CONFIGURATION.md](CONFIGURATION.md).

| GUC | Type | Default | Description |
|---|---|---|---|
| [`pg_trickle.enabled`](CONFIGURATION.md#pg_trickle-enabled) | bool | `true` | Enable/disable the scheduler. Manual refreshes still work when `false`. |
| [`pg_trickle.scheduler_interval_ms`](CONFIGURATION.md#pg_trickle-scheduler_interval_ms) | int | `1000` | Scheduler wake interval in milliseconds (100–60000) |
| [`pg_trickle.min_schedule_seconds`](CONFIGURATION.md#pg_trickle-min_schedule_seconds) | int | `60` | Minimum allowed schedule duration (1–86400) |
| [`pg_trickle.max_consecutive_errors`](CONFIGURATION.md#pg_trickle-max_consecutive_errors) | int | `3` | Failures before auto-suspending (1–100) |
| [`pg_trickle.change_buffer_schema`](CONFIGURATION.md#pg_trickle-change_buffer_schema) | text | `pgtrickle_changes` | Schema for CDC buffer tables |
| [`pg_trickle.max_concurrent_refreshes`](CONFIGURATION.md#pg_trickle-max_concurrent_refreshes) | int | `4` | Max parallel refresh workers (1–32) |
| [`pg_trickle.user_triggers`](CONFIGURATION.md#pg_trickle-user_triggers) | text | `auto` | User trigger handling: `auto` (detect), `off` (suppress), `on` (deprecated alias for `auto`) |
| [`pg_trickle.differential_max_change_ratio`](CONFIGURATION.md#pg_trickle-differential_max_change_ratio) | float | `0.15` | Change ratio threshold for adaptive FULL fallback (0.0–1.0) |
| [`pg_trickle.cleanup_use_truncate`](CONFIGURATION.md#pg_trickle-cleanup_use_truncate) | bool | `true` | Use TRUNCATE instead of DELETE for buffer cleanup |

All GUCs are `SUSET` context (superuser SET) and take effect without restart,
except `shared_preload_libraries` which requires a PostgreSQL restart.

For SQL function signatures and descriptions see [SQL_REFERENCE.md](SQL_REFERENCE.md).

---

## Troubleshooting

This section covers common problems and how to debug them. If your issue isn’t
listed here, check the [refresh history](#how-do-i-interpret-the-refresh-history)
for error messages and the [monitoring views](#what-monitoring-views-are-available)
for status information.

### How do I diagnose stalled data flow through stream tables?

> **See also:** [Error Reference](ERRORS.md) — comprehensive guide to all
> pg_trickle error variants with causes and fixes.

If data seems to have stopped flowing -- stream tables show stale results
despite DML on the source tables -- follow this systematic diagnostic workflow.
Each step narrows the problem from broad health checks down to specific root
causes.

**Step 0 -- Verify GUC configuration:**

Misconfigured GUCs are a common and easy-to-miss cause of stalled or
severely throttled data flow. Check all pg_trickle settings in one query:

```sql
SELECT name, setting, unit
FROM pg_settings
WHERE name LIKE 'pg_trickle.%'
   OR name = 'max_worker_processes'
ORDER BY name;
```

Key values to check:

| GUC | Safe value | Problem if set to |
|---|---|---|
| `pg_trickle.enabled` | `on` | `off` -- stops all automatic refreshes |
| `pg_trickle.tiered_scheduling` | `on` (fine) | `on` with all STs at `tier = 'frozen'` -- silently skips them |
| `pg_trickle.max_consecutive_errors` | `3`-`10` | `1` -- one transient error suspends the ST permanently |
| `pg_trickle.scheduler_interval_ms` | `100`-`1000` | Very high (e.g. `60000`) -- scheduler only wakes every 60 s |
| `pg_trickle.auto_backoff` | `on` | Fine normally, but if refreshes take >95% of schedule it silently stretches intervals up to 8x |
| `pg_trickle.default_schedule_seconds` | `1`-`60` | Very high -- isolated CALCULATED tables refresh very infrequently |
| `max_worker_processes` | `>= 16` (typical) | Too low -- workers cannot be spawned; parallel mode silently stalls |

Also check whether any stream tables are frozen:

```sql
SELECT pgt_name, refresh_tier
FROM pgtrickle.pgt_stream_tables
WHERE refresh_tier = 'frozen';
```

**Step 1 -- Quick health overview:**

```sql
SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK';
```

This single call checks scheduler status, error tables, stale tables, buffer
growth, replication slot lag, and the worker pool. Any `WARN` or `ERROR` row
tells you where to look next.

**Step 2 -- Check stream table status and staleness:**

```sql
SELECT name, status, refresh_mode, consecutive_errors, staleness
FROM pgtrickle.pgt_status()
ORDER BY staleness DESC NULLS FIRST;
```

Look for `SUSPENDED` status (auto-suspended after repeated errors), high
`consecutive_errors`, or unexpectedly large `staleness`.

**Step 3 -- Check recent refresh activity:**

```sql
SELECT start_time, stream_table, action, status, duration_ms, error_message
FROM pgtrickle.refresh_timeline(20);
```

If no recent rows appear, the scheduler may not be running. If rows show
`ERROR`, the error messages explain why refreshes are failing.

**Step 4 -- Inspect errors for a specific stream table:**

```sql
SELECT * FROM pgtrickle.diagnose_errors('my_stream_table');
```

Returns the last 5 `FAILED` refresh events with error classification and
suggested remediation steps.

**Step 5 -- Check the CDC pipeline (are changes being captured?):**

```sql
SELECT stream_table, source_table, cdc_mode, pending_rows, buffer_bytes
FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;
```

- `pending_rows = 0` everywhere: either no DML is happening on the source
  tables, or CDC triggers are missing.
- `pending_rows` growing but stream tables are not refreshing: scheduler or
  refresh problem (go back to Steps 1-3).

**Step 6 -- Verify CDC triggers exist and are enabled:**

```sql
SELECT source_table, trigger_type, trigger_name
FROM pgtrickle.trigger_inventory()
WHERE NOT present OR NOT enabled;
```

Any rows returned here mean change capture is broken for that source table --
DML changes are not being recorded.

**Step 7 -- Check CDC slot health (WAL mode only):**

```sql
SELECT * FROM pgtrickle.check_cdc_health();
```

Look for `alert` values like `slot_lag_exceeds_threshold` or
`replication_slot_missing`.

**Step 8 -- Verify the dependency DAG:**

```sql
SELECT tree_line, status, refresh_mode
FROM pgtrickle.dependency_tree();
```

Confirms the dependency graph is wired as expected. A missing edge means
upstream changes will not propagate to downstream stream tables.

**Step 9 -- Check the parallel worker pool (if using parallel mode):**

```sql
SELECT * FROM pgtrickle.worker_pool_status();

SELECT job_id, unit_key, status, duration_ms
FROM pgtrickle.parallel_job_status(300)
WHERE status NOT IN ('SUCCEEDED');
```

**Common root causes at a glance:**

| Symptom | Diagnostic function | Likely root cause |
|---|---|---|
| No refreshes happening at all | `health_check` -> `scheduler_running` | Background worker not running or `pg_trickle.enabled = off` |
| Stream table in `SUSPENDED` status | `pgt_status` | Repeated errors hit `max_consecutive_errors` threshold |
| Zero pending changes despite DML | `trigger_inventory` | CDC trigger was dropped or disabled by DDL |
| WAL slot missing or lagging | `check_cdc_health`, `slot_health` | Replication slot dropped, or WAL retention exceeded |
| Buffers growing but no refreshes | `change_buffer_sizes` + `refresh_timeline` | Scheduler stalled, refresh failing, or lock contention |
| Upstream changes not propagating | `dependency_tree` | Upstream ST not connected in the DAG |

### Unit tests crash with `symbol not found in flat namespace` on macOS 26+

macOS 26 (Tahoe) changed the dynamic linker (`dyld`) to eagerly resolve all
flat-namespace symbols at binary load time. pgrx extensions link PostgreSQL
server symbols (e.g. `CacheMemoryContext`, `SPI_connect`) with
`-Wl,-undefined,dynamic_lookup`, which previously resolved lazily. Since
`cargo test --lib` runs outside the postgres process, those symbols are
missing and the test binary aborts:

```
dyld[66617]: symbol not found in flat namespace '_CacheMemoryContext'
```

**Use `just test-unit`** — it automatically detects macOS 26+ and injects a
stub library (`libpg_stub.dylib`) via `DYLD_INSERT_LIBRARIES`. The stub
provides NULL/no-op definitions for the ~28 PostgreSQL symbols; they are never
called during unit tests (pure Rust logic only).

This does **not** affect integration tests, E2E tests, `just lint`,
`just build`, or the extension running inside PostgreSQL.

See the [Installation Guide](installation.md#unit-tests-crash-on-macos-26-symbol-not-found-in-flat-namespace) for details and manual usage.

### My stream table is stuck in INITIALIZING status

The initial full refresh may have failed. Check:
```sql
SELECT * FROM pgtrickle.get_refresh_history('my_table', 5);
```
If the error is transient, retry with:
```sql
SELECT pgtrickle.refresh_stream_table('my_table');
```

### My stream table shows stale data but the scheduler is running

Common causes:
1. **TRUNCATE on source table** — bypasses CDC triggers. Manual refresh needed.
2. **Too many errors** — check `consecutive_errors` in `pgtrickle.pg_stat_stream_tables`. Resume with `ALTER ... status => 'ACTIVE'`.
3. **Long-running refresh** — check for lock contention or slow defining queries.
4. **Scheduler disabled** — verify `SHOW pg_trickle.enabled;` returns `on`.

### I get "cycle detected" when creating a stream table

Stream tables cannot have circular dependencies. If stream table A depends on stream table B and B depends on A (either directly or through a chain of intermediate stream tables), pg_trickle rejects the creation with a clear error message listing the cycle path.

To resolve this, restructure your queries to eliminate the circular reference. Common patterns:
- Extract the shared logic into a single base stream table that both A and B reference.
- Use a regular view instead of a stream table for one side of the dependency.
- Merge the two queries into a single stream table if possible.

### A source table was altered and my stream table stopped refreshing

pg_trickle detects DDL changes (column additions, drops, type changes) via event triggers and marks affected stream tables with `needs_reinit = true`. The next scheduler cycle will attempt to **reinitialize** the stream table — drop the storage table, recreate it from the current defining query schema, and perform a full refresh.

If the schema change breaks the defining query (e.g., a column referenced in the query was dropped or renamed), the reinitialization will fail repeatedly until the stream table hits `max_consecutive_errors` and enters ERROR status.

**To fix it:** Update the defining query and recreate the stream table:

```sql
SELECT pgtrickle.drop_stream_table('order_totals');
SELECT pgtrickle.create_stream_table(
    name         => 'order_totals',
    query        => 'SELECT id, name FROM orders',  -- updated query reflecting new schema
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

Check the refresh history for the specific error message:

```sql
SELECT * FROM pgtrickle.get_refresh_history('order_totals', 5);
```

### How do I see the delta query generated for a stream table?

```sql
SELECT pgtrickle.explain_st('order_totals');
```

This shows the DVM operator tree, source tables, and the generated delta SQL.

### How do I interpret the refresh history?

The `pgtrickle.get_refresh_history()` function returns the most recent refresh records for a stream table:

```sql
SELECT * FROM pgtrickle.get_refresh_history('order_totals', 10);
```

Key columns:

| Column | Meaning |
|---|---|
| `action` | Refresh type: `FULL`, `DIFFERENTIAL`, `TOPK`, `IMMEDIATE`, or `REINITIALIZE` |
| `rows_inserted` | Rows added to the stream table in this cycle |
| `rows_deleted` | Rows removed from the stream table in this cycle |
| `rows_updated` | Rows modified in the stream table (for explicit DML path) |
| `duration_ms` | Wall-clock time for the refresh |
| `error_message` | NULL for success; error text for failures |
| `source_changes` | Number of pending change records processed |
| `fallback_reason` | If DIFFERENTIAL fell back to FULL: `change_ratio_exceeded`, `truncate_detected`, or `reinitialize` |

**Patterns to look for:**
- High `rows_inserted` + `rows_deleted` with low `source_changes` → possible duplicate rows (keyless source tables)
- `fallback_reason = 'change_ratio_exceeded'` frequently → consider lowering the threshold or switching to FULL mode
- Increasing `duration_ms` over time → index maintenance or buffer bloat; consider VACUUM or checking for missing indexes

### How can I tell if the scheduler is running?

Several ways to verify:

**1. Check the background worker:**
```sql
SELECT pid, datname, backend_type, state, query
FROM pg_stat_activity
WHERE backend_type = 'pg_trickle scheduler';
```

If no rows are returned, the scheduler is not running. Common causes:
- `pg_trickle.enabled = false`
- Extension not in `shared_preload_libraries`
- `max_worker_processes` exhausted — the launcher silently skips databases it cannot accommodate and retries every **5 minutes**. Check the PostgreSQL log for `WARNING: pg_trickle launcher: could not spawn scheduler for database '...'`.

**2. Check recent refresh activity:**
```sql
SELECT MAX(refreshed_at) AS last_refresh
FROM pgtrickle.pgt_stream_tables
WHERE status = 'ACTIVE';
```

If the last refresh was long ago relative to the shortest schedule, the scheduler may be stuck.

**3. Check PostgreSQL logs:**
The scheduler logs startup and shutdown messages at `LOG` level:
```
LOG:  pg_trickle scheduler started for database "mydb"
LOG:  pg_trickle scheduler shutting down (SIGTERM)
```

### How do I debug a stream table that shows stale data?

Follow this diagnostic checklist:

1. **Is the scheduler running?** (See above)
2. **Is the stream table active?**
   ```sql
   SELECT pgt_name, status, consecutive_errors FROM pgtrickle.pg_stat_stream_tables
   WHERE pgt_name = 'my_st';
   ```
   If status is `ERROR` or `SUSPENDED`, the stream table has been auto-suspended after repeated failures.
3. **Are there pending changes?**
   ```sql
   SELECT COUNT(*) FROM pgtrickle_changes.changes_<source_oid>;
   ```
   If zero, the source table may not have CDC triggers (check `SELECT tgname FROM pg_trigger WHERE tgrelid = '<source_oid>'`).
4. **Is the refresh failing silently?**
   ```sql
   SELECT * FROM pgtrickle.get_refresh_history('my_st', 5);
   ```
   Check for error messages.
5. **Is there lock contention?** Long-running transactions holding locks on the source or stream table can block refreshes. Check `pg_locks` and `pg_stat_activity`.

### What does the `needs_reinit` flag mean and how do I clear it?

The `needs_reinit` flag in `pgtrickle.pgt_stream_tables` indicates that the stream table's physical storage needs to be rebuilt — typically because a source table's schema changed.

When `needs_reinit = true`:
- The scheduler **skips** normal differential/full refresh.
- Instead, it performs a **reinitialize**: drop the storage table, recreate it from the current defining query schema, and populate with a full refresh.
- If reinitialization succeeds, `needs_reinit` is cleared automatically.

If reinitialization keeps failing (e.g., the defining query references a dropped column):
```sql
-- Fix the underlying issue first, then clear manually:
UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = false WHERE pgt_name = 'my_st';
-- Or drop and recreate:
SELECT pgtrickle.drop_stream_table('my_st');
SELECT pgtrickle.create_stream_table(
    name         => 'my_st',
    query        => 'SELECT ...',
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## Why Are These SQL Features Not Supported?

This section gives detailed technical explanations for each SQL limitation.
pg_trickle follows the principle of **"fail loudly rather than produce wrong
data"** — every unsupported feature is detected at stream-table creation time
and rejected with a clear error message and a suggested rewrite.

For all of these, returning an explicit error is a deliberate design choice:
the alternative would be silently producing incorrect results after a refresh,
which is far harder to diagnose.

### How does `NATURAL JOIN` work?

`NATURAL JOIN` is fully supported. At parse time, pg_trickle resolves the common columns between the two tables (using `OpTree::output_columns()`) and synthesizes explicit equi-join conditions. This supports `INNER`, `LEFT`, `RIGHT`, and `FULL` NATURAL JOIN variants.

Internally, `NATURAL JOIN` is converted to an explicit `JOIN ... ON` before the DVM engine builds its operator tree, so delta computation works identically to a manually specified equi-join.

**Note:** The internal `__pgt_row_id` column is excluded from common column resolution, so NATURAL JOINs between stream tables work correctly.

### How do `GROUPING SETS`, `CUBE`, and `ROLLUP` work?

`GROUPING SETS`, `CUBE`, and `ROLLUP` are fully supported via an automatic parse-time rewrite. pg_trickle decomposes these constructs into a `UNION ALL` of separate `GROUP BY` queries before the DVM engine processes the query.

> **Explosion guard:** `CUBE(N)` generates $2^N$ branches. pg_trickle rejects
> CUBE/ROLLUP combinations that would produce more than **64 branches** to
> prevent runaway memory usage. Use explicit `GROUPING SETS(...)` instead.

For example:
```sql
-- This defining query:
SELECT dept, region, SUM(amount) FROM sales GROUP BY CUBE(dept, region)

-- Is automatically rewritten to:
SELECT dept, region, SUM(amount) FROM sales GROUP BY dept, region
UNION ALL
SELECT dept, NULL::text, SUM(amount) FROM sales GROUP BY dept
UNION ALL
SELECT NULL::text, region, SUM(amount) FROM sales GROUP BY region
UNION ALL
SELECT NULL::text, NULL::text, SUM(amount) FROM sales
```

`GROUPING()` function calls are replaced with integer literal constants corresponding to the grouping level. The rewrite is transparent — the DVM engine sees only standard `GROUP BY` + `UNION ALL` operators and can apply incremental delta computation to each branch independently.

### How does `DISTINCT ON (…)` work?

`DISTINCT ON` is fully supported via an automatic parse-time rewrite. pg_trickle transparently transforms `DISTINCT ON` into a `ROW_NUMBER()` window function subquery:

```sql
-- This defining query:
SELECT DISTINCT ON (dept) dept, employee, salary
FROM employees ORDER BY dept, salary DESC

-- Is automatically rewritten to:
SELECT dept, employee, salary FROM (
    SELECT dept, employee, salary,
           ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
    FROM employees
) sub WHERE rn = 1
```

The rewrite happens before DVM parsing, so the operator tree sees a standard window function query and can apply partition-based recomputation for incremental delta maintenance.

### Why is `TABLESAMPLE` rejected?

`TABLESAMPLE` returns a random subset of rows from a table (e.g., `FROM orders TABLESAMPLE BERNOULLI(10)` gives ~10% of rows).

Stream tables materialize the **complete** result set of the defining query and keep it up-to-date across refreshes. Baking a random sample into the defining query is not meaningful because:

1. **Non-determinism.** Each refresh would sample different rows, making the stream table contents unstable and unpredictable. The delta between refreshes would be dominated by sampling noise, not actual data changes.

2. **CDC incompatibility.** The trigger-based change-capture system tracks specific row-level changes (inserts, updates, deletes). A `TABLESAMPLE` defining query has no stable row identity — the "changed rows" concept doesn't apply when the entire sample shifts each cycle.

**Rewrite:**
```sql
-- Instead of sampling in the defining query:
SELECT * FROM orders TABLESAMPLE BERNOULLI(10)

-- Materialize the full result and sample when querying:
SELECT * FROM order_stream_table WHERE random() < 0.1
```

### Why is `LIMIT` / `OFFSET` rejected?

Stream tables materialize the complete result set and keep it synchronized with source data. Bare `LIMIT`/`OFFSET` (without a recognized pattern) would truncate the result:

1. **Undefined ordering.** `LIMIT` without `ORDER BY` returns an arbitrary subset.

2. **Delta instability.** When source rows change, the boundary between "in the LIMIT" and "out of the LIMIT" shifts. A single INSERT could evict one row and admit another, requiring the refresh to track the full ordered position of every row.

3. **Semantic mismatch.** Users who write `LIMIT 100` typically want to limit what they *read*, not what is *stored*.

**Exception — TopK pattern:** When the defining query has a top-level `ORDER BY … LIMIT N` (constant integer, optionally with `OFFSET M`), pg_trickle recognizes this as a **TopK** query and **accepts it**. The `ORDER BY` clause is required — bare `LIMIT` without `ORDER BY` is always rejected because it selects an arbitrary subset. With `ORDER BY`, the top-N boundary is well-defined and the stream table stores exactly those N rows (starting from position M+1 if OFFSET is specified). See the [TopK section](#topk-order-by--limit) for details.

**Rewrite (when TopK doesn't apply):**
```sql
-- Instead of:
'SELECT * FROM orders ORDER BY created_at DESC LIMIT 100'

-- Omit LIMIT from the defining query, apply when reading:
SELECT * FROM orders_stream_table ORDER BY created_at DESC LIMIT 100
```

### Why are window functions in expressions rejected?

Window functions like `ROW_NUMBER() OVER (…)` are supported as **standalone columns** in stream tables. However, embedding a window function inside an expression — such as `CASE WHEN ROW_NUMBER() OVER (...) = 1 THEN ...` or `SUM(x) OVER (...) + 1` — is rejected.

This restriction exists because:

1. **Partition-based recomputation.** pg_trickle's differential mode handles window functions by recomputing entire partitions that were affected by changes. When a window function is buried inside an expression, the DVM engine cannot isolate the window computation from the surrounding expression, making it impossible to correctly identify which partitions to recompute.

2. **Expression tree ambiguity.** The DVM parser would need to differentiate the outer expression (arithmetic, `CASE`, etc.) while treating the inner window function specially. This creates a combinatorial explosion of differentiation rules for every possible expression type × window function combination.

**Rewrite:**
```sql
-- Instead of:
SELECT id, CASE WHEN ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) = 1
                THEN 'top' ELSE 'other' END AS rank_label
FROM employees

-- Move window function to a separate column, then use a wrapping stream table:
-- ST1:
SELECT id, dept, salary,
       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
FROM employees

-- ST2 (references ST1):
SELECT id, CASE WHEN rn = 1 THEN 'top' ELSE 'other' END AS rank_label
FROM pgtrickle.employees_ranked
```

### Why is `FOR UPDATE` / `FOR SHARE` rejected?

`FOR UPDATE` and related locking clauses (`FOR SHARE`, `FOR NO KEY UPDATE`, `FOR KEY SHARE`) acquire row-level locks on selected rows. This is incompatible with stream tables because:

1. **Refresh semantics.** Stream table contents are managed by the refresh engine using bulk `MERGE` operations. Row-level locks taken during the defining query would conflict with the refresh engine's own locking strategy.

2. **No direct DML.** Since users cannot directly modify stream table rows, there is no use case for locking rows inside the defining query. The locks would be held for the duration of the refresh transaction and then released, serving no purpose.

### How does `ALL (subquery)` work?

`ALL (subquery)` comparisons (e.g., `WHERE x > ALL (SELECT y FROM t)`) are supported via an automatic rewrite to `NOT EXISTS`. For example, `x > ALL (SELECT y FROM t)` is rewritten to `NOT EXISTS (SELECT 1 FROM t WHERE y >= x)`, which pg_trickle handles via its anti-join operator.

### Why is `ORDER BY` silently discarded?

`ORDER BY` in the defining query is **accepted but ignored**. This is consistent with how PostgreSQL treats `CREATE MATERIALIZED VIEW AS SELECT ... ORDER BY ...` — the ordering is not preserved in the stored data.

Stream tables are heap tables with no guaranteed row order. The `ORDER BY` in the defining query would only affect the order of the initial `INSERT`, which has no lasting effect. Apply ordering when **querying** the stream table:

```sql
-- This ORDER BY is meaningless in the defining query:
'SELECT region, SUM(amount) FROM orders GROUP BY region ORDER BY total DESC'

-- Instead, order when reading:
SELECT * FROM regional_totals ORDER BY total DESC
```

### Why are unsupported aggregates (`CORR`, `COVAR_*`, `REGR_*`) limited to FULL mode?

Regression aggregates like `CORR`, `COVAR_POP`, `COVAR_SAMP`, and the `REGR_*` family require maintaining running sums of products and squares across the entire group. Unlike `COUNT`/`SUM`/`AVG` (where deltas can be computed from the change alone) or group-rescan aggregates (where only affected groups are re-read), regression aggregates:

1. **Lack algebraic delta rules.** There is no closed-form way to update a correlation coefficient from a single row change without access to the full group's data.

2. **Would degrade to group-rescan anyway.** Even if supported, the implementation would need to rescan the full group from source — identical to FULL mode for most practical group sizes.

These aggregates work fine in **FULL** refresh mode, which re-runs the entire query from scratch each cycle.

---

## Why Are These Stream Table Operations Restricted?

Stream tables are regular PostgreSQL heap tables under the hood, but their
contents are managed exclusively by the refresh engine. This section explains
why certain operations that work on ordinary tables are disallowed or
unsupported on stream tables, and what to do instead.

### Why can't I `INSERT`, `UPDATE`, or `DELETE` rows in a stream table?

Stream table contents are the **output** of the refresh engine — they represent the materialized result of the defining query at a specific point in time. Direct DML would corrupt this contract in several ways:

1. **Row ID integrity.** Every row has a `__pgt_row_id` (a 64-bit xxHash of the group-by key or all columns). The refresh engine uses this for delta `MERGE` — matching incoming deltas against existing rows. A manually inserted row with an incorrect or duplicate `__pgt_row_id` would cause the next differential refresh to produce wrong results (double-counting, missed deletes, or merge conflicts).

2. **Frontier inconsistency.** Each refresh records a *frontier* — a set of per-source LSN positions that represent "data up to this point has been materialized." A manual DML change is not tracked by any frontier. The next differential refresh would either overwrite the change (if the delta touches the same row) or leave the stream table in a state that doesn't match any consistent point-in-time snapshot of the source data.

3. **Change buffer desync.** The CDC triggers on source tables write changes to buffer tables. The refresh engine reads these buffers and advances the frontier. Manual DML on the stream table bypasses this pipeline entirely — the buffer and frontier have no record of the change, so future refreshes cannot account for it.

If you need to post-process stream table data, create a **view** or a **second stream table** that references the first one.

### Why can't I add foreign keys to or from a stream table?

Foreign key constraints require that referenced/referencing rows exist at the time of each DML statement. The refresh engine violates this assumption:

1. **Bulk `MERGE` ordering.** A differential refresh executes a single `MERGE INTO` statement that applies all deltas (inserts and deletes) atomically. PostgreSQL evaluates FK constraints row-by-row within this `MERGE`. If a parent row is deleted and a new parent inserted in the same delta batch, the child FK check may fail because it sees the delete before the insert — even though the final state would be consistent.

2. **Full refresh uses `TRUNCATE` + `INSERT`.** In FULL mode, the refresh engine truncates the stream table and re-inserts all rows. `TRUNCATE` does not fire individual `DELETE` triggers and bypasses FK cascade logic, which would leave referencing tables with dangling references.

3. **Cross-table refresh ordering.** If stream table A has an FK referencing stream table B, both tables refresh independently (in topological order, but in separate transactions). There is no guarantee that A's refresh sees B's latest data — the FK constraint could transiently fail between refreshes.

**Workaround:** Enforce referential integrity in the consuming application or use a view that joins the stream tables and validates the relationship.

### How do user-defined triggers work on stream tables?

When a DIFFERENTIAL mode stream table has user-defined row-level triggers, the refresh engine uses **explicit DML decomposition** instead of `MERGE`:

1. **Delta materialized once.** The delta query result is stored in a temporary table (`__pgt_delta_<id>`) to avoid evaluating it three times.

2. **DELETE removed rows.** Rows in the stream table whose `__pgt_row_id` is absent from the delta are deleted. `AFTER DELETE` triggers fire with correct `OLD` values.

3. **UPDATE changed rows.** Rows whose `__pgt_row_id` exists in both the stream table and delta but whose values differ (checked via `IS DISTINCT FROM`) are updated. `AFTER UPDATE` triggers fire with correct `OLD` and `NEW`. No-op updates (where values are identical) are skipped, preventing spurious triggers.

4. **INSERT new rows.** Rows in the delta whose `__pgt_row_id` is absent from the stream table are inserted. `AFTER INSERT` triggers fire with correct `NEW` values.

**FULL refresh behavior:** Row-level user triggers are automatically suppressed during FULL refresh via `DISABLE TRIGGER USER` / `ENABLE TRIGGER USER`. A `NOTIFY pgtrickle_refresh` is emitted so listeners know a FULL refresh occurred. Use `REFRESH MODE DIFFERENTIAL` for stream tables that need per-row trigger semantics.

**Performance:** The explicit DML path adds ~25–60% overhead compared to MERGE for triggered stream tables. Stream tables without user triggers have zero overhead (only a fast `pg_trigger` check, <0.1 ms).

**Control:** The `pg_trickle.user_triggers` GUC controls this behavior:
- `auto` (default): detect user triggers automatically
- `off`: always use MERGE, suppressing triggers
- `on`: deprecated compatibility alias for `auto`

### Why can't I `ALTER TABLE` a stream table directly?

Stream table metadata (defining query, schedule, refresh mode) is stored in the pg_trickle catalog (`pgtrickle.pgt_stream_tables`). A direct `ALTER TABLE` would change the physical table without updating the catalog, causing:

1. **Column mismatch.** If you add or remove columns, the refresh engine's cached delta query and `MERGE` statement would reference columns that no longer exist (or miss new ones), causing runtime errors.

2. **`__pgt_row_id` invalidation.** The row ID hash is computed from the defining query's output columns. Altering the table schema without updating the defining query would make existing row IDs inconsistent with the new column set.

Use `pgtrickle.alter_stream_table()` to change schedule, refresh mode, or status. To change the defining query or column structure, drop and recreate the stream table.

### Why can't I `TRUNCATE` a stream table?

`TRUNCATE` removes all rows instantly but does not update the pg_trickle frontier or change buffers. After a `TRUNCATE`:

1. **Differential refresh sees no changes.** The frontier still records the last-processed LSN. No new source changes may have occurred, so the next differential refresh produces an empty delta — leaving the stream table empty even though the source still has data.

2. **No recovery path for differential mode.** The refresh engine has no way to detect that the stream table was externally truncated. It assumes the current contents match the frontier.

Use `pgtrickle.refresh_stream_table('my_table')` to force a full re-materialization, or drop and recreate the stream table if you need a clean slate.

### What are the memory limits for delta processing?

The differential refresh path executes the delta query as a **single SQL statement**. For large batches (e.g., a bulk UPDATE of 10M rows), PostgreSQL may attempt to materialize the entire delta result set in memory. If the delta exceeds `work_mem`, PostgreSQL will spill to temporary files on disk, which is slower but safe. In extreme cases, OOM (out of memory) can occur if `work_mem` is set very high and the delta is enormous.

**Mitigations:**

1. **Adaptive fallback.** The `pg_trickle.differential_max_change_ratio` GUC (default 0.15) automatically triggers a FULL refresh when the ratio of pending changes to total rows exceeds the threshold. This prevents large deltas from consuming excessive memory.

2. **`work_mem` tuning.** PostgreSQL's `work_mem` setting controls how much memory each sort/hash operation uses before spilling to disk. For pg_trickle workloads, 64–256 MB is typical. Monitor `temp_blks_written` in `pg_stat_statements` to detect spilling.

3. **`pg_trickle.merge_work_mem_mb` GUC.** Sets a session-level `work_mem` override during the MERGE execution (default: 0 = use global `work_mem`). This allows higher memory for refresh without affecting other queries.

4. **Monitoring.** If `pg_stat_statements` is installed, pg_trickle logs a warning when the MERGE query writes temporary blocks to disk.

### Why are refreshes processed sequentially by default?

The current default (`parallel_refresh_mode = 'on'`) enables parallel refresh for independent execution units while preserving topological ordering. Set `parallel_refresh_mode = 'off'` for sequential refreshes when minimizing worker usage matters more than refresh throughput.

**When to consider enabling parallel refresh:**

- Your database has many **independent** stream tables (no shared dependencies).
- Total cycle time = sum of all refresh durations and some refreshes are visibly blocking unrelated ones.
- You have enough `max_worker_processes` headroom (each parallel worker uses one slot).

**Enabling parallel refresh:**

```sql
ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'on';
SELECT pg_reload_conf();
```

With `parallel_refresh_mode = 'on'`, the scheduler builds an execution-unit DAG and dispatches independent units to dynamic background workers. Atomic consistency groups and IMMEDIATE-trigger closures remain single-worker for correctness.

See [CONFIGURATION.md — Parallel Refresh](CONFIGURATION.md#parallel-refresh) for tuning guidance including the worker-budget formula.

### How many connections does pg_trickle use?

pg_trickle uses the following PostgreSQL connections:

| Component | Connections | When |
|-----------|-------------|------|
| Background scheduler | 1 | Always (per database with STs) |
| WAL decoder polling | 0 (shared) | Uses the scheduler's SPI connection |
| Manual refresh | 1 | Per-call (uses caller's session) |

**Total**: 1 persistent connection per database. WAL decoder polling shares the scheduler's SPI connection rather than opening separate connections.

**`max_worker_processes`**: pg_trickle registers 1 background worker per database during `_PG_init()`. Ensure `max_worker_processes` (default 8) has room for the pg_trickle worker plus any other extensions.

**Advisory locks**: The scheduler holds a session-level advisory lock per actively-refreshing ST. These are released immediately after each refresh completes.

---

**See also:**
[Troubleshooting](TROUBLESHOOTING.md) ·
[Error Reference](ERRORS.md) ·
[Configuration](CONFIGURATION.md) ·
[SQL Reference](SQL_REFERENCE.md) ·
[Glossary](GLOSSARY.md)
