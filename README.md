# pg_trickle

[![Build](https://github.com/trickle-labs/pg-trickle/actions/workflows/build.yml/badge.svg)](https://github.com/trickle-labs/pg-trickle/actions/workflows/build.yml)
[![CI](https://github.com/trickle-labs/pg-trickle/actions/workflows/ci.yml/badge.svg)](https://github.com/trickle-labs/pg-trickle/actions/workflows/ci.yml)
[![Release](https://github.com/trickle-labs/pg-trickle/actions/workflows/release.yml/badge.svg)](https://github.com/trickle-labs/pg-trickle/actions/workflows/release.yml)
[![Coverage](https://codecov.io/gh/trickle-labs/pg-trickle/branch/main/graph/badge.svg)](https://codecov.io/gh/trickle-labs/pg-trickle)
[![Benchmarks](https://img.shields.io/badge/Benchmarks-view-blue)](docs/BENCHMARK.md)
[![Roadmap](https://img.shields.io/badge/Roadmap-view-informational)](ROADMAP.md)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![PostgreSQL 18](https://img.shields.io/badge/PostgreSQL-18-blue?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![pgrx 0.18](https://img.shields.io/badge/pgrx-0.18-orange)](https://github.com/pgcentralfoundation/pgrx)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/trickle-labs/pg-trickle)

> **Pre-1.0 Notice:** pg_trickle is under active development and has not yet reached a stable 1.0 release. The core feature set is extensive and thoroughly tested, but APIs and configuration options may still change between minor versions. See [ROADMAP.md](ROADMAP.md) for the planned path to 1.0.

> For a plain-language description of the problem pg_trickle solves, the differential dataflow approach, and the hybrid CDC architecture, read **[ESSENCE.md](ESSENCE.md)**.

## Stream Tables for PostgreSQL 18

pg_trickle brings declarative, automatically-refreshing materialized views to PostgreSQL, inspired by the [DBSP](https://arxiv.org/abs/2203.16684) differential dataflow framework ([comparison](docs/research/DBSP_COMPARISON.md)). Define a SQL query and a schedule; the extension handles the rest.

A **stream table** is a table defined by a SQL query that stays up to date automatically as the underlying data changes. You write to your base tables normally — `INSERT`, `UPDATE`, `DELETE` — and pg_trickle propagates the changes downstream. No polling, no manual refresh calls, no application-level orchestration.

The key difference from `REFRESH MATERIALIZED VIEW` is that pg_trickle uses **Incremental View Maintenance (IVM)**: when a row changes, only the effect of that row on the query result is computed. If you insert one row into a ten-million-row table, pg_trickle processes one row — not ten million. At high change rates it falls back to a full recompute automatically; at low change rates (the common case) the speedup is substantial. The [TPC-H benchmarks](#tpc-h-validation-22-queries-sf001) show 5–90× measured improvements across the 22 standard analytical queries at a 1% change rate.

Stream tables support the full range of SQL — `GROUP BY`, `JOIN`, `WINDOW`, `EXISTS`, `WITH RECURSIVE`, CTEs, LATERAL, subqueries — and can depend on other stream tables, forming a DAG that is refreshed in topological order. Changes propagate through the entire graph automatically.

```sql
-- Create a stream table that stays up to date with no manual work
SELECT pgtrickle.create_stream_table(
    name     => 'sales_by_region',
    query    => $$
        SELECT c.region,
               COUNT(*)                   AS order_count,
               SUM(o.quantity * p.price)  AS total_revenue
        FROM orders o
        JOIN customers c ON c.id = o.customer_id
        JOIN products  p ON p.id = o.product_id
        GROUP BY c.region
    $$,
    schedule => '1s'
);

-- Query it like any table — always fresh
SELECT * FROM sales_by_region ORDER BY total_revenue DESC;
```

## Try it in 30 seconds — no installation needed

Start a PostgreSQL 18 container with pg_trickle pre-installed and sample data loaded:

```bash
cd playground && docker compose up -d
```

Then connect with psql:

```bash
psql postgresql://postgres:playground@localhost:5432/playground
```

The [`playground/`](playground/) directory is a self-contained Docker environment with sample
tables, pre-loaded data, and five stream tables demonstrating key pg_trickle patterns — basic
aggregates, window functions, multi-table joins, time-series, and EXISTS subqueries. See
[`playground/README.md`](playground/README.md) or the [Playground docs page](docs/PLAYGROUND.md).

## Choose your path

| Persona | Start here |
|---|---|
| **Curious / evaluator** | [What is pg_trickle?](ESSENCE.md) → [Use Cases](docs/USE_CASES.md) → [Comparisons](docs/COMPARISONS.md) → [Playground](playground/) |
| **Application developer** | [5-Minute Quickstart](docs/QUICKSTART_5MIN.md) → [Getting Started tutorial](docs/GETTING_STARTED.md) → [Patterns](docs/PATTERNS.md) → [SQL Reference](docs/SQL_REFERENCE.md) |
| **DBA / SRE** | [Pre-Deployment Checklist](docs/PRE_DEPLOYMENT.md) → [Configuration](docs/CONFIGURATION.md) → [Troubleshooting](docs/TROUBLESHOOTING.md) → [Capacity Planning](docs/CAPACITY_PLANNING.md) |
| **Data / analytics engineer** | [Use Cases](docs/USE_CASES.md) → [dbt integration](docs/integrations/dbt.md) → [Migrating from materialized views](docs/tutorials/MIGRATING_FROM_MATERIALIZED_VIEWS.md) |
| **Confused by jargon** | [Glossary](docs/GLOSSARY.md) |

> **Curious about how pg_trickle came to be — and the role coding agents played in its development?**
> See [Project History](docs/PROJECT_HISTORY.md).

## Key Features

### Core IVM engine

- **Incremental by default** — delta queries are derived automatically from the defining query's operator tree; only changed rows are processed.
- **Four refresh modes** — `AUTO` (smart default: DIFFERENTIAL when possible, FULL fallback), `DIFFERENTIAL` (incremental), `FULL` (complete recomputation), and `IMMEDIATE` (synchronous, in-transaction IVM).
- **Transactional IVM** — `IMMEDIATE` mode maintains stream tables within the same transaction as the base-table DML, giving read-your-writes consistency with no background worker required.
- **Change buffer compaction** — cancelling INSERT/DELETE pairs and sequential changes to the same row are collapsed automatically, reducing delta scan overhead 50–90% on high-churn tables.
- **Adaptive fallback** — when change rate exceeds the `pg_trickle.adaptive_full_threshold` (default 50%), the engine switches to FULL automatically and switches back when the rate drops.
- **DVM depth guard & CTE cap** — `diff_node()` enforces a configurable recursion depth limit (`pg_trickle.max_parse_depth`, default 64) and a CTE count cap (`pg_trickle.max_diff_ctes`, default 1000) to prevent stack overflows and OOM on pathological queries.
- **O(V+E) diamond detection** — the DAG diamond-detection algorithm runs in linear time (single forward topological pass) for all graph sizes.
- **Multi-column `IN` rewrite** — row expressions and multi-target sub-selects in `IN`/`NOT IN` predicates are automatically rewritten to AND-chained equality.

### SQL coverage

- **Joins** — `INNER`, `LEFT`, `RIGHT`, `FULL OUTER`, `NATURAL`, nested (3+ tables), non-equi-joins.
- **Aggregation** — fully algebraic `COUNT`/`SUM`/`AVG`; semi-algebraic `MIN`/`MAX`; group-rescan for 30+ other aggregates (`STRING_AGG`, `ARRAY_AGG`, `STDDEV`, `PERCENTILE_*`, `CORR`, …).
- **Window functions** — `ROW_NUMBER`, `RANK`, `SUM OVER`, `RANGE`/`ROWS`/`GROUPS` frames, named `WINDOW` clauses, multiple partitions.
- **Set operations** — `UNION ALL`, `UNION`, `INTERSECT`, `EXCEPT`.
- **Subqueries** — `EXISTS`/`NOT EXISTS`, `IN`/`NOT IN`, scalar subqueries in `SELECT` and `WHERE`, subqueries in `FROM`.
- **CTEs** — non-recursive `WITH` (inlined, shared delta); `WITH RECURSIVE` in FULL, DIFFERENTIAL, and IMMEDIATE modes.
- **TopK** — `ORDER BY … LIMIT N [OFFSET M]` via scoped recomputation; server-side pagination with `OFFSET`.
- **LATERAL / SRFs** — `jsonb_array_elements`, `unnest`, `jsonb_each`, `JSON_TABLE`, and other set-returning functions.
- **DDL** — `ALTER QUERY` changes the defining query online; the engine migrates storage and reruns a full refresh with no downtime.

### Change data capture

- **Trigger-based CDC** — lightweight `AFTER` row-level triggers; no `wal_level = logical`, no replication slots required.
- **Hybrid CDC** — when `wal_level = logical` is available, pg_trickle transitions from triggers to WAL-based capture automatically (`pg_trickle.cdc_mode = auto`), reducing write-path overhead to near-zero.
- **Watermark gating** — external loaders publish per-source watermarks; downstream refreshes wait until all sources are aligned before proceeding.

### Scheduling & dependency management

- **DAG-aware** — stream tables that depend on other stream tables are refreshed in topological order; `CALCULATED` schedule propagation is supported.
- **Diamond consistency** — diamond-shaped DAGs (A→B→D, A→C→D) are refreshed atomically to prevent split-version reads.
- **Circular dependencies** — monotone cycles can be enabled explicitly and are driven to a fixed point with convergence guardrails.
- **Tiered scheduling** — Hot / Warm / Cold / Frozen tiers control effective refresh rates in large deployments; Frozen tables are skipped until manually thawed.
- **Multi-database** — a single launcher worker auto-discovers every database with the extension installed and spawns a scheduler for each.

### Citus distributed tables

- **Distributed sources** — stream tables can be defined over Citus distributed source tables; the scheduler polls per-worker WAL slots via `dblink` and merges changes on the coordinator.
- **Distributed output** — pass `output_distribution_column` to `create_stream_table()` to co-locate the result table with the source shards.
- **Automated scheduler** — the scheduler drives the full per-worker slot lifecycle automatically (no manual wiring required): `ensure_worker_slot`, `poll_worker_slot_changes`, and `pgt_st_locks` lease management.
- **Shard rebalance auto-recovery** — topology changes are detected by comparing `pg_dist_node` against `pgt_worker_slots`; stale slots are pruned and new ones inserted without operator intervention.
- **Worker failure isolation** — per-worker poll failures are logged and skipped; after `pg_trickle.citus_worker_retry_ticks` (default 5) consecutive failures a WARNING is raised while healthy workers continue uninterrupted.

### DuckLake lakehouse integration

pg_trickle is positioning itself as the incremental view maintenance engine for [DuckLake](https://ducklake.select) — the PostgreSQL-catalog-backed lakehouse format from the DuckDB team.

- **Phase 1** — tutorials, blog posts, and containerised demos for running pg_trickle as the IVM layer for DuckLake-backed data lakes using the existing foreign-table path; zero new extension code required.
- **Phase 2** — native `CdcMode::DuckLakeChangeFeed` adapter calling DuckLake's `table_changes()` API for O(Δ) change consumption; snapshot-based frontier model; inlined-data trigger adapter; row-ID plumbing for O(1) delta application; compaction-window safety policy.
- **Phase 3a** — DuckLake sink output mode (`sink => 'ducklake'`): stream table results serialised as Parquet (Snappy/ZSTD) via `arrow-rs`/`parquet` crates, uploaded to `file://` or `s3://`, and registered in the DuckLake catalog (data files, stats, snapshot) — queryable by DuckDB, Spark, and Trino with no custom export code.
- **Phase 3b** — automatic DuckLake view registration on `create_stream_table` / `drop_stream_table`; snapshot provenance table (`pgtrickle.pgt_ducklake_provenance`) recording every sink run with `created_by` identity, snapshot ID, and delta row count for full end-to-end lineage.

See [DuckLake Integration Plan](plans/ecosystem/PLAN_DUCKLAKE.md) and [ROADMAP.md](ROADMAP.md) for the full roadmap.

### Production & operations

- **PgBouncer / connection-pool compatible** — works behind PgBouncer in transaction-pool mode (Supabase, Railway, Neon, etc.); row-level locking replaces session locks; per-table `pooler_compatibility_mode` available.
- **Crash-safe** — row-level locks prevent concurrent refreshes; in-flight refreshes are marked failed and resumed cleanly on recovery.
- **Online ALTER QUERY** — schema changes are classified (same / compatible / incompatible), storage migrated, and the dependency graph updated without dropping and recreating the stream table.
- **Self-healing repair** — `pgtrickle.repair_stream_table(name)` rebuilds any missing CDC triggers and change buffer tables, resets the refresh frontier, and clears fuse state — use after PITR restores or operator-level DDL.
- **CNPG / Kubernetes ready** — purpose-built Docker images and CloudNativePG manifests included; a `lifecycle.preStop` drain hook (`pgtrickle.drain(timeout_s => 120)`) ensures clean rolling upgrades.
- **SQL-injection safe Citus paths** — all `dblink` call sites escape connection strings and queries via `pg_catalog.quote_literal`, eliminating injection risk through attacker-controlled hostnames or slot names.
- **Supply-chain hardened Docker images** — all Dockerfiles pin `postgres:18.4-bookworm` to an exact SHA256 digest for reproducible builds; `scripts/update_base_image_digests.sh` automates quarterly refreshes.

### Observability

- `pgtrickle.health_check()` — overall extension health at a glance.
- `pgtrickle.pgt_status()` — status, staleness, and refresh mode for every stream table.
- `pgtrickle.refresh_timeline(n)` — last _n_ refresh events with timings.
- `pgtrickle.change_buffer_sizes()` — pending CDC entries per source table.
- `pgtrickle.dependency_tree()` — full DAG with topological order.
- `pgtrickle.explain_st(name)` — the delta SQL pg_trickle will run on next refresh.
- `pgtrickle.repair_stream_table(name)` — rebuild missing CDC triggers, recreate missing change buffers, and reset state after PITR or operator-level DDL.
- `pgtrickle.reliability_counters()` — template cache stale evictions, DAG cycles detected, and hash-mismatch evictions as a single-row table; exposed via the Prometheus `/metrics` endpoint.
- `NOTIFY`-based alerting, SCC status, watermark status, trigger inventory, diamond groups, and more.

## SQL Support

Every operator listed here works in `DIFFERENTIAL` mode (incremental delta computation) unless noted otherwise. `FULL` mode always works — it re-runs the entire query on each refresh.

| Category | Feature | Support | Notes |
|---|---|---|---|
| **Core** | Table scan | ✅ Full | |
| **Core** | Projection (`SELECT` expressions) | ✅ Full | |
| **Filtering** | `WHERE` clause | ✅ Full | |
| **Filtering** | `HAVING` clause | ✅ Full | Applied as a filter on top of the aggregate delta |
| **Joins** | `INNER JOIN` | ✅ Full | Equi-joins are optimal; non-equi-joins also work |
| **Joins** | `LEFT OUTER JOIN` | ✅ Full | |
| **Joins** | `RIGHT OUTER JOIN` | ✅ Full | Automatically converted to `LEFT JOIN` with swapped operands |
| **Joins** | `FULL OUTER JOIN` | ✅ Full | 8-part delta; may be slower than inner joins on high-churn data |
| **Joins** | `NATURAL JOIN` | ✅ Full | Resolved at parse time; explicit equi-join synthesized |
| **Joins** | Nested joins (3+ tables) | ✅ Full | `a JOIN b JOIN c` — snapshot subqueries with disambiguated columns |
| **Aggregation** | `GROUP BY` + `COUNT`, `SUM`, `AVG` | ✅ Full | Fully algebraic — no rescan needed |
| **Aggregation** | `GROUP BY` + `MIN`, `MAX` | ✅ Full | Semi-algebraic — `LEAST`/`GREATEST` merge; per-group rescan on extremum deletion |
| **Aggregation** | 30+ other aggregates (`STRING_AGG`, `ARRAY_AGG`, `BOOL_AND/OR`, `JSON[B]_AGG`, `STDDEV`, `VARIANCE`, `MODE`, `PERCENTILE_*`, `CORR`, `COVAR_*`, `REGR_*`, etc.) | ✅ Full | Group-rescan — affected groups re-aggregated from source |
| **Aggregation** | `FILTER (WHERE …)` on aggregates | ✅ Full | Filter predicate applied within delta computation |
| **Deduplication** | `DISTINCT` | ✅ Full | Reference-counted multiplicity tracking |
| **Deduplication** | `DISTINCT ON (…)` | ✅ Full | Auto-rewritten to `ROW_NUMBER()` window subquery |
| **Set operations** | `UNION ALL` | ✅ Full | |
| **Set operations** | `UNION` (deduplicated) | ✅ Full | Composed as `UNION ALL` + `DISTINCT` |
| **Set operations** | `INTERSECT` / `EXCEPT` | ✅ Full | Dual-count multiplicity tracking with LEAST / GREATEST boundary crossing |
| **Subqueries** | Subquery in `FROM` | ✅ Full | `(SELECT …) AS alias` |
| **Subqueries** | `EXISTS` / `NOT EXISTS` in `WHERE` | ✅ Full | Semi-join / anti-join delta operators |
| **Subqueries** | `IN` / `NOT IN` (subquery) in `WHERE` | ✅ Full | Rewritten to semi-join / anti-join |
| **Subqueries** | `ALL` (subquery) in `WHERE` | ✅ Full | Rewritten to anti-join via `NOT EXISTS` |
| **Subqueries** | Scalar subquery in `SELECT` | ✅ Full | `(SELECT max(x) FROM t)` in target list |
| **Subqueries** | Scalar subquery in `WHERE` | ✅ Full | Auto-rewritten to `CROSS JOIN` |
| **CTEs** | Non-recursive `WITH` | ✅ Full | Single & multi-reference; shared delta computation |
| **CTEs** | Recursive `WITH RECURSIVE` | ✅ Full | Both `FULL` and `DIFFERENTIAL` modes (semi-naive + DRed) |
| **Window functions** | `ROW_NUMBER`, `RANK`, `SUM OVER`, etc. | ✅ Full | Partition-based recomputation |
| **Window functions** | Window frame clauses | ✅ Full | `ROWS`, `RANGE`, `GROUPS` with `BETWEEN` bounds and `EXCLUDE` |
| **Window functions** | Named `WINDOW` clauses | ✅ Full | `WINDOW w AS (...)` resolved from query-level window definitions |
| **Window functions** | Multiple `PARTITION BY` clauses | ✅ Full | Same partition key used directly; different keys fall back to full recomputation |
| **LATERAL SRFs** | `jsonb_array_elements`, `unnest`, `jsonb_each`, etc. | ✅ Full | Row-scoped recomputation in DIFFERENTIAL mode |
| **JSON_TABLE** | `JSON_TABLE(expr, path COLUMNS (...))` | ✅ Full | PostgreSQL 17+; modeled as lateral function |
| **Expressions** | `CASE WHEN … THEN … ELSE … END` | ✅ Full | Both searched and simple CASE |
| **Expressions** | `COALESCE`, `NULLIF`, `GREATEST`, `LEAST` | ✅ Full | |
| **Expressions** | `IN (list)`, `BETWEEN`, `IS DISTINCT FROM` | ✅ Full | Including `NOT IN`, `NOT BETWEEN`, `IS NOT DISTINCT FROM` |
| **Expressions** | `IS TRUE/FALSE/UNKNOWN` | ✅ Full | All boolean test variants |
| **Expressions** | `SIMILAR TO`, `ANY(array)`, `ALL(array)` | ✅ Full | |
| **Expressions** | `ARRAY[…]`, `ROW(…)` | ✅ Full | |
| **Expressions** | `CURRENT_DATE`, `CURRENT_TIMESTAMP`, etc. | ✅ Full | All SQL value functions |
| **Expressions** | Array subscript, field access | ✅ Full | `arr[1]`, `(rec).field`, `(data).*` |
| **Grouping** | `GROUPING SETS` / `CUBE` / `ROLLUP` | ✅ Full | Auto-rewritten to `UNION ALL` of `GROUP BY` queries |
| **Safety** | Volatile function/operator detection | ✅ Full | VOLATILE expressions rejected in DIFFERENTIAL and IMMEDIATE; STABLE functions warned |
| **Source tables** | Tables without primary key | ✅ Full | Content-hash row identity via all columns |
| **Source tables** | Views as sources | ✅ Full | Auto-inlined as subqueries; CDC triggers land on base tables |
| **Source tables** | Materialized views | ⚠️ DIFF / ✅ FULL | DIFFERENTIAL requires `pg_trickle.matview_polling = on` (snapshot comparison); FULL always works |
| **TopK** | `ORDER BY ... LIMIT N [OFFSET M]` | ✅ Full | TopK stream tables store only N rows (optionally from position M+1); scoped recomputation via MERGE |
| **Ordering** | `ORDER BY` (without LIMIT) | ⚠️ Ignored | Accepted but silently discarded; storage row order is undefined |
| **Ordering** | `LIMIT` / `OFFSET` (without ORDER BY) | ❌ Rejected | Not supported — stream tables materialize the full result set |

See [docs/DVM_OPERATORS.md](docs/DVM_OPERATORS.md) for the full differentiation rules and CTE tiers.

## Performance

pg_trickle is designed for low-latency, high-throughput incremental view maintenance. Differential refresh processes only changed rows — not the entire base table — yielding significant speedups over full recomputation.

### When Differential Wins

DIFFERENTIAL processes only changed rows; FULL re-executes the entire query. The gap between them grows with table size and shrinks with change rate — at 50% churn the two modes converge.

DIFFERENTIAL is the right default for **scans, joins, filtered projections, and high-cardinality aggregates** (`GROUP BY customer_id` with thousands of distinct groups): FULL must TRUNCATE and re-insert or re-aggregate the entire result set, while DIFFERENTIAL touches only the 1–2% of rows that changed. The TPC-H validation section below shows 15.9x measured speedup for joins at 1% change rate.

**Aggregate queries with few distinct groups** (e.g. `GROUP BY region` with 5 regions from 100K rows) are the exception: FULL re-aggregates into 5 output rows in a single cheap hash pass, so DIFFERENTIAL's delta overhead is not recovered. Use `refresh_mode = 'full'` explicitly for these cases, or rely on the adaptive fallback (`pg_trickle.adaptive_full_threshold`, default 50%) which switches to FULL automatically when the change ratio is high. `create_stream_table` emits a WARNING when a low-cardinality aggregate pattern is detected.

For a detailed per-query breakdown across all 22 TPC-H queries see the [TPC-H Validation](#tpc-h-validation-22-queries-sf001) section below, and [docs/BENCHMARK.md](docs/BENCHMARK.md) for methodology and how to run your own benchmarks.

### Zero-Change Latency

When no data has changed, the scheduler's per-cycle overhead is minimal:

| Metric | Value |
|---|---|
| Average | 3.2 ms |
| Max | 5.1 ms |
| Target | < 10 ms |

**Tip:** For tables that change rarely, assign them to the **Cold** or **Frozen** tier (`pg_trickle.tiered_scheduling = on`) to reduce polling frequency without removing the stream table. Frozen tables are skipped by the scheduler entirely until manually refreshed or explicitly thawed.

### Where Time Is Spent

Rust-side delta SQL generation takes **< 1%** of total refresh time (sub-microsecond to ~50 µs depending on operator complexity). PostgreSQL's MERGE execution dominates at **70–97%** of wall-clock time — the extension gets out of the way and lets the database do what it does best. Placeholder resolution uses a single-pass [Aho-Corasick](https://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_algorithm) multi-pattern replacer (O(template_length) regardless of the number of source tables), and the volatility lookup cache eliminates repeated SPI round-trips per refresh cycle.

**Tip:** Since MERGE dominates, standard PostgreSQL tuning applies: ensure the stream table has an index on its primary key (created automatically) and that source tables have indexes on any JOIN or WHERE columns in the defining query. Increasing `work_mem` and `maintenance_work_mem` can also reduce sort and hash costs within the MERGE plan.

### DAG Propagation

Changes propagate through multi-level stream table DAGs efficiently:

| Topology | Stream Tables | Propagation Time |
|---|---|---|
| Linear chain (depth 10) | 10 | ~820 ms |
| Wide DAG (3 levels × 20 wide) | 60 | ~2,430 ms |
| Diamond (4-way fan-out + join) | 5 | ~200 ms |

PARALLEL refresh mode processes independent branches concurrently, reducing wall-clock time for wide DAGs. The scheduler composes all per-node delta SQL into a single CTE-chain statement (**fused multi-node refresh**) — eliminating round-trips between nodes and achieving a ≥20% wall-time improvement on complex DAGs.

**Tips:** Enable `PARALLEL` refresh mode (`ALTER STREAM TABLE ... SET refresh_mode = 'parallel'`) to process independent DAG branches concurrently. For deep linear chains (> 5 levels), consider consolidating intermediate steps into a single defining query to reduce propagation hops and transaction overhead.

### IMMEDIATE Mode

For use cases that require read-your-writes consistency, IMMEDIATE mode maintains the stream table **within the same transaction** as the DML — no scheduler, no change buffers, no additional latency. The delta is computed from PostgreSQL's transition tables and applied inline.

### Change Buffer Compaction

High-churn tables benefit from automatic compaction of CDC change buffers, which collapses cancelling INSERT/DELETE pairs and sequential changes to the same row, reducing delta scan overhead by **50–90%**.

**Tip:** Compaction is most effective when refreshes are batched. Increasing `refresh_interval` gives the compactor more time to accumulate and collapse changes between cycles, amplifying the reduction. For workloads with periodic bulk writes (e.g., hourly ETL), align `refresh_interval` with the load cadence so the compactor runs after each batch settles.

### Write-Path Overhead

Incremental view maintenance is not free on the write side. CDC triggers add overhead to every INSERT, UPDATE, and DELETE on source tables. The trade-off is intentional: atomic, transactional change tracking with zero data loss, in exchange for modest write-side cost.

**Per-row trigger overhead: 20–55 µs.** At typical OLTP write rates (< 1,000 writes/sec per source), this adds **< 5%** to DML latency — well below network round-trip time.

**Write amplification** (measured via [E2E CDC overhead benchmarks](docs/BENCHMARK.md)):

| Operation | Write Amplification |
|---|---|
| Single-row INSERT | ~2.0x |
| Bulk INSERT (10K rows) | ~2.1x |
| Bulk UPDATE (10K rows) | ~2.2x |
| Bulk DELETE (10K rows) | ~2.3x |

Several layers reduce this cost automatically:

- **Hybrid CDC** — triggers bootstrap change capture with zero config; when `wal_level = logical` is available, the system transitions to WAL-based capture for lower write-side overhead (~5 µs/row). Controlled by `pg_trickle.cdc_mode` (default: `auto`).
- **Columnar change tracking** — CDC records only the columns referenced by the defining query, using a VARBIT bitmask. UPDATEs that touch only unreferenced columns are skipped entirely, reducing delta volume by **50–90%** for wide tables.
- **Delta predicate pushdown** — WHERE predicates from the defining query are injected into change buffer scans, filtering irrelevant changes at read time (**5–10x** delta volume reduction for selective queries).
- **Event-driven scheduler wake** — CDC triggers emit `pg_notify()` to wake the scheduler immediately instead of polling, reducing propagation latency from ~515 ms to ~15 ms median.
- **Adaptive FULL fallback** — when the change ratio exceeds a threshold (default: 50%), the engine automatically switches to FULL refresh for that cycle, avoiding the case where differential is slower than recomputation.

For write-heavy workloads where trigger overhead is a concern, FULL refresh mode bypasses CDC entirely — no triggers are installed, and each refresh re-executes the full query.

**If overhead is still a concern:**

- **Force WAL-based CDC** — if `wal_level = logical` is confirmed available on your instance, set `pg_trickle.cdc_mode = 'wal'` to bypass trigger overhead entirely, dropping per-row cost from 20–55 µs to ~5 µs.
- **Batch writes** — prefer multi-row `INSERT` or `COPY` over single-row statements. Per-row trigger cost is constant, so batching amortizes it across fewer transactions and reduces change buffer pressure.
- **Narrow the defining query** — referencing fewer source columns lets columnar filtering discard more UPDATE events at capture time. UPDATEs that touch only unreferenced columns are skipped entirely, with no entry written to the change buffer.
- **Increase `refresh_interval`** — less frequent refreshes allow the compactor to collapse more cancelling changes per cycle, reducing the total delta volume the engine must process.
- **Switch to FULL refresh** — `ALTER STREAM TABLE ... SET refresh_mode = 'full'` removes all CDC triggers on the source table. Each refresh re-executes the full query; no change buffers, no trigger overhead.

### TPC-H Validation (22 queries, SF=0.01)

pg_trickle is validated against the full TPC-H benchmark suite — all 22 standard queries across all three refresh modes. The test suite runs 15 test scenarios and passes completely:

| Test | Queries | Result |
|---|---|---|
| Differential correctness | 22/22 | ✅ pass |
| Immediate correctness | 22/22 | ✅ pass |
| Full correctness | 22/22 | ✅ pass |
| Differential == Immediate | 22/22 | ✅ identical results |
| Full == Differential | 22/22 | ✅ identical results |
| Rollback correctness | INSERT / UPDATE / DELETE | ✅ pass |
| Savepoint rollback | INSERT + DELETE | ✅ pass |
| Single-row mutations | 3 queries | ✅ pass |
| Combined-delete regression (q07, q08, q09) | 3 queries | ✅ pass |

**Sustained churn (T1-C, 7 stream tables, 50 cycles, SF=0.01):**

| Metric | Value |
|---|---|
| Avg cycle time | 150.7 ms |
| Min / Max | 115.9 / 537.9 ms |
| Drift detected | 0 |
| Refresh failures | 0 |

For full benchmark methodology and how to run your own benchmarks, see [docs/BENCHMARK.md](docs/BENCHMARK.md).

## Quick Start

### Prerequisites

- PostgreSQL 18.x
- Rust 1.85+ (edition 2024) with [pgrx](https://github.com/pgcentralfoundation/pgrx) 0.18.x

### Install

```bash
# Build and install the extension
cargo pgrx install --release

# Or package for deployment
cargo pgrx package
```

Add to `postgresql.conf`:

```ini
shared_preload_libraries = 'pg_trickle'
max_worker_processes = 8
```

> **Note:** `wal_level = logical` and `max_replication_slots` are **not** required by default. CDC uses lightweight row-level triggers unless you opt in to WAL-based capture via `pg_trickle.cdc_mode = 'auto'` (see [CONFIGURATION.md](docs/CONFIGURATION.md)).

Restart PostgreSQL, then:

```sql
CREATE EXTENSION pg_trickle;
```

### Kubernetes (CloudNativePG)

pg_trickle is distributed as a minimal OCI extension image for [CloudNativePG Image Volume Extensions](https://cloudnative-pg.io/docs/1.28/imagevolume_extensions/). The image is `scratch`-based (< 10 MB) and contains only the extension files — no PostgreSQL server, no OS.

```bash
docker pull ghcr.io/trickle-labs/pg_trickle-ext:latest
```

Deploy with the official CNPG PostgreSQL 18 operand image:

```yaml
# In your Cluster resource
spec:
  imageName: ghcr.io/cloudnative-pg/postgresql:18
  postgresql:
    shared_preload_libraries: [pg_trickle]
    extensions:
      - name: pg-trickle
        image:
          reference: ghcr.io/trickle-labs/pg_trickle-ext:latest
```

See [cnpg/cluster-example.yaml](cnpg/cluster-example.yaml) and [cnpg/database-example.yaml](cnpg/database-example.yaml) for complete examples. Requires Kubernetes 1.33+ and CNPG 1.28+.

### Usage

```sql
-- Create base tables
CREATE TABLE orders (
    id    INT PRIMARY KEY,
    region TEXT,
    amount NUMERIC
);

INSERT INTO orders VALUES
    (1, 'US', 100), (2, 'EU', 200),
    (3, 'US', 300), (4, 'APAC', 50);

-- Create a stream table — AUTO mode (default): DIFFERENTIAL when possible, FULL fallback.
-- Schedule defaults to 'calculated' (derived from consumer refresh cycles).
SELECT pgtrickle.create_stream_table(
    'regional_totals',
    'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
     FROM orders GROUP BY region'
);

-- Explicit IMMEDIATE mode: updated within the same transaction as DML
SELECT pgtrickle.create_stream_table(
    'regional_totals_live',
    'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
     FROM orders GROUP BY region',
    schedule     => NULL,
    refresh_mode => 'IMMEDIATE'
);

-- Explicit schedule and mode
SELECT pgtrickle.create_stream_table(
    'hourly_totals',
    'SELECT region, SUM(amount) AS total FROM orders GROUP BY region',
    schedule     => '@hourly',
    refresh_mode => 'FULL'
);

-- Query the stream table like any regular table
SELECT * FROM regional_totals;

-- Manual refresh (scheduler also refreshes automatically)
SELECT pgtrickle.refresh_stream_table('regional_totals');

-- Change the defining query online (ALTER QUERY)
SELECT pgtrickle.alter_stream_table(
    'regional_totals',
    query => 'SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
              FROM orders WHERE active GROUP BY region'
);

-- Check status and overall health
SELECT * FROM pgtrickle.pgt_status();
SELECT * FROM pgtrickle.health_check();  -- OK/WARN/ERROR triage

-- View monitoring stats
SELECT * FROM pgtrickle.pg_stat_stream_tables;
SELECT * FROM pgtrickle.dependency_tree();  -- ASCII DAG view
SELECT * FROM pgtrickle.change_buffer_sizes();  -- CDC buffer health

-- Repair after a PITR restore or out-of-band DDL
SELECT pgtrickle.repair_stream_table('regional_totals');

-- Drop when no longer needed
SELECT pgtrickle.drop_stream_table('regional_totals');
```

## Terminal UI & CLI

pg_trickle ships a standalone terminal tool (`pgtrickle`) for managing and
monitoring stream tables from outside SQL. It works as both an interactive
dashboard and a scriptable CLI.

```bash
# Interactive dashboard (auto-refresh every 2s)
pgtrickle --url postgres://user:pass@host:5432/mydb

# One-shot CLI (for scripts, CI, monitoring)
pgtrickle list --format json
pgtrickle health                        # exit code 1 on critical
pgtrickle refresh order_totals
pgtrickle explain order_totals --analyze
pgtrickle watch -n 5                    # continuous output
```

18 subcommands: `list`, `status`, `refresh`, `create`, `drop`, `alter`,
`export`, `diag`, `cdc`, `graph`, `config`, `health`, `workers`, `fuse`,
`watermarks`, `explain`, `watch`, `completions`.

13 interactive views: Dashboard, Detail, Dependencies, Refresh Log,
Diagnostics, CDC Health, Configuration, Health Checks, Alerts, Workers,
Fuse, Watermarks, and Delta Inspector.

## dbt Integration

The `dbt-pgtrickle` package provides a custom `stream_table` materialization for
[dbt](https://www.getdbt.com/). No custom adapter needed — works with the standard
`dbt-postgres` adapter.

```yaml
# packages.yml
packages:
  - git: "https://github.com/trickle-labs/pg-trickle.git"
    revision: main
    subdirectory: "dbt-pgtrickle"
```

```sql
-- models/marts/order_totals.sql
{{ config(materialized='stream_table', schedule='5m', refresh_mode='DIFFERENTIAL') }}
SELECT customer_id, SUM(amount) AS total FROM {{ source('raw', 'orders') }} GROUP BY customer_id
```

See [dbt-pgtrickle/README.md](dbt-pgtrickle/README.md) for the full configuration
reference, operations, and testing guide.

## Documentation

The documentation is organised into reading paths so you can find what you
need without scrolling. Start with **[ESSENCE.md](ESSENCE.md)** if you want
the plain-language overview, or jump straight to a topic below.

### Discover

| Document | Description |
|---|---|
| [What is pg_trickle?](ESSENCE.md) | Plain-language overview |
| [Mental Model](docs/MENTAL_MODEL.md) | How IVM works — delta semantics, change capture, DAG chaining |
| [Use Cases](docs/USE_CASES.md) | What people build with stream tables |
| [Comparisons](docs/COMPARISONS.md) | vs. pg_ivm, Materialize, Flink, Debezium, … |
| [Glossary](docs/GLOSSARY.md) | Decoder for every term used in the docs |

### Get started

| Document | Description |
|---|---|
| [5-Minute Quickstart](docs/QUICKSTART_5MIN.md) | Shortest possible introduction |
| [Getting Started Tutorial](docs/GETTING_STARTED.md) | Hands-on tutorial building an org-chart with stream tables |
| [INSTALL.md](INSTALL.md) | Detailed installation and configuration guide |

### Build with stream tables

| Document | Description |
|---|---|
| [Patterns](docs/PATTERNS.md) | Bronze/Silver/Gold, SCDs, dashboards, outbox/inbox |
| [Performance Cookbook](docs/PERFORMANCE_COOKBOOK.md) | Tuning recipes |
| [Performance Cheatsheet](docs/PERFORMANCE_CHEATSHEET.md) | Three golden rules, top-10 GUC quick wins, single-page reference |
| [Limitations](docs/LIMITATIONS.md) | Unsupported SQL, DIFFERENTIAL constraints, decision tree |
| [SQL Reference](docs/SQL_REFERENCE.md) | Every function, view, and option |
| [Configuration](docs/CONFIGURATION.md) | All GUC variables |
| [Predictive Cost Model](docs/COST_MODEL.md) | How AUTO mode chooses |

### Operate

| Document | Description |
|---|---|
| [Pre-Deployment Checklist](docs/PRE_DEPLOYMENT.md) | Before you go live |
| [Scaling Guide](docs/SCALING.md) | Worker pool sizing, tiered scheduling |
| [Capacity Planning](docs/CAPACITY_PLANNING.md) | Disk, memory, connections |
| [Multi-Database Deployments](docs/MULTI_DATABASE.md) | One server, many databases |
| [Backup and Restore](docs/BACKUP_AND_RESTORE.md) | pg_dump, pgBackRest, PITR |
| [Snapshots](docs/SNAPSHOTS.md) | Point-in-time copies of stream tables |
| [HA & Replication](docs/HA_AND_REPLICATION.md) | Failover, logical replicas, CNPG |
| [Upgrading](docs/UPGRADING.md) | Version-to-version upgrade procedure |
| [Security Guide](docs/SECURITY_GUIDE.md) | Roles, grants, RLS, auditing |
| [Troubleshooting](docs/TROUBLESHOOTING.md) | Symptoms → diagnosis → fix |
| [Error Reference](docs/ERRORS.md) | Every PgTrickleError variant with SQLSTATE |
| [GUC Catalog](docs/GUC_CATALOG.md) | All 115 configuration parameters with types and defaults |

### Distributed & streaming

| Document | Description |
|---|---|
| [Citus](docs/CITUS.md) | Distributed sources and outputs |
| [CDC Modes](docs/CDC_MODES.md) | Trigger, WAL, hybrid |
| [SLA-based Smart Scheduling](docs/SLA_SCHEDULING.md) | Set freshness goals, not intervals |
| [Downstream Publications](docs/PUBLICATIONS.md) | Replicate stream tables to Debezium, Kafka, … |
| [Transactional Outbox](docs/OUTBOX.md) | Reliable event emission |
| [Transactional Inbox](docs/INBOX.md) | Idempotent event consumption |

### Tutorials & deep dives

[What Happens on INSERT](docs/tutorials/WHAT_HAPPENS_ON_INSERT.md) ·
[UPDATE](docs/tutorials/WHAT_HAPPENS_ON_UPDATE.md) ·
[DELETE](docs/tutorials/WHAT_HAPPENS_ON_DELETE.md) ·
[TRUNCATE](docs/tutorials/WHAT_HAPPENS_ON_TRUNCATE.md) ·
[Build Your First Dashboard](docs/tutorials/FIRST_DASHBOARD.md) ·
[Event Sourcing & CQRS](docs/tutorials/EVENT_SOURCING.md) ·
[Backfill & Zero-Downtime Migration](docs/tutorials/BACKFILL_AND_MIGRATION.md) ·
[Security Hardening](docs/tutorials/SECURITY_HARDENING.md) ·
[Tiered Scheduling](docs/tutorials/TIERED_SCHEDULING.md) ·
[Fuse Circuit Breaker](docs/tutorials/FUSE_CIRCUIT_BREAKER.md) ·
[Circular Dependencies](docs/tutorials/CIRCULAR_DEPENDENCIES.md) ·
[Migrating from Materialized Views](docs/tutorials/MIGRATING_FROM_MATERIALIZED_VIEWS.md) ·
[Migrating from pg_ivm](docs/tutorials/MIGRATING_FROM_PG_IVM.md) ·
[Row-Level Security](docs/tutorials/ROW_LEVEL_SECURITY.md) ·
[Partitioned Tables](docs/tutorials/PARTITIONED_TABLES.md) ·
[Foreign Table Sources](docs/tutorials/FOREIGN_TABLE_SOURCES.md) ·
[Tuning Refresh Mode](docs/tutorials/tuning-refresh-mode.md) ·
[Monitoring & Alerting](docs/tutorials/MONITORING_AND_ALERTING.md) ·
[ETL & Bulk Load](docs/tutorials/ETL_BULK_LOAD.md)

### Reference

[FAQ](docs/FAQ.md) ·
[What's New](docs/WHATS_NEW.md) ·
[Changelog](CHANGELOG.md) ·
[Roadmap](ROADMAP.md) ·
[Project History](docs/PROJECT_HISTORY.md) ·
[Architecture](docs/ARCHITECTURE.md) ·
[DVM Operators](docs/DVM_OPERATORS.md) ·
[DVM Rewrite Rules](docs/DVM_REWRITE_RULES.md) ·
[Benchmarks](docs/BENCHMARK.md)

## Known Limitations

The following SQL features are **rejected with clear error messages** explaining the limitation and suggesting rewrites. See [FAQ — Why Are These SQL Features Not Supported?](docs/FAQ.md#why-are-these-sql-features-not-supported) for detailed technical explanations.

| Feature | Reason | Suggested Rewrite |
|---|---|---|
| Materialized views in DIFFERENTIAL | CDC triggers cannot track `REFRESH MATERIALIZED VIEW` | Use the underlying query directly, or use FULL mode |
| `TABLESAMPLE` | Stream tables materialize the complete result set; sampling at define-time is not meaningful | Use `WHERE random() < fraction` in the consuming query |
| Window functions in expressions | Window functions inside `CASE`, `COALESCE`, arithmetic, etc. cannot be differentially maintained | Move window function to a separate column |
| `LIMIT` / `OFFSET` without `ORDER BY` | Stream tables materialize the full result set; arbitrary row subsets are non-deterministic | Use `ORDER BY ... LIMIT N` for TopK, or apply LIMIT when querying the stream table |
| `FOR UPDATE` / `FOR SHARE` | Row-level locking not applicable | Remove the locking clause |

### Stream Table Restrictions

Stream tables are regular PostgreSQL heap tables, but their contents are managed exclusively by the refresh engine. See [FAQ — Why Are These Stream Table Operations Restricted?](docs/FAQ.md#why-are-these-stream-table-operations-restricted) for detailed explanations.

| Operation | Allowed? | Notes |
|---|---|---|
| ST references other STs | ✅ Yes | DAG-ordered refresh; monotone cycles supported via `pg_trickle.allow_circular` |
| Views reference STs | ✅ Yes | Standard PostgreSQL views work normally |
| Materialized views reference STs | ✅ Yes | Requires separate `REFRESH MATERIALIZED VIEW` |
| Logical replication of STs | ✅ Yes | `__pgt_row_id` column is replicated; subscribers receive materialized data only |
| Direct DML on STs | ❌ No | Contents managed by the refresh engine |
| Foreign keys on STs | ❌ No | Bulk `MERGE` during refresh does not respect FK ordering |
| User triggers on STs | ✅ Supported | Supported in DIFFERENTIAL mode; suppressed during FULL refresh (see `pg_trickle.user_triggers` GUC) |

See [SQL Reference — Restrictions & Interoperability](docs/SQL_REFERENCE.md#restrictions--interoperability) for details and examples.

## How It Works

### DIFFERENTIAL / FULL Mode (scheduled)

1. **Create** — `pgtrickle.create_stream_table()` parses the defining query into an operator tree, creates a storage table, installs lightweight CDC triggers on source tables, and registers the ST in the catalog.

2. **Capture** — Changes to base tables are captured via the hybrid CDC layer. By default, `AFTER INSERT/UPDATE/DELETE` row-level triggers write to per-source change buffer tables in the `pgtrickle_changes` schema. With `pg_trickle.cdc_mode = 'auto'`, the system transitions to WAL-based capture (logical replication) after the first successful refresh for lower write-side overhead.

3. **Schedule** — A background worker wakes periodically (default: 1s) and checks which STs have exceeded their schedule (or whose cron schedule has fired). STs are scheduled for refresh in topological order.

4. **Differentiate** — The DVM engine differentiates the defining query's operator tree to produce a delta query (ΔQ) that computes only the changes since the last refresh.

5. **Apply** — The delta query is executed and its results (INSERT/DELETE actions with row IDs) are merged into the storage table.

6. **Version** — Each refresh records a frontier (per-source LSN positions) and a data timestamp, implementing a simplified Data Versioning System (DVS).

### IMMEDIATE Mode (transactional)

When `refresh_mode = 'IMMEDIATE'`, the stream table is maintained **within the same transaction** as the base table DML:

1. **BEFORE triggers** acquire an advisory lock on the stream table.
2. **AFTER triggers** (with `REFERENCING NEW TABLE AS ... OLD TABLE AS ...`) copy transition tables to temp tables.
3. The DVM engine computes the delta from the transition tables (via `DeltaSource::TransitionTable`).
4. The delta is applied to the stream table via DELETE + INSERT ON CONFLICT.

No change buffer tables, no scheduler, no WAL infrastructure. The stream table is always up-to-date within the current transaction.

Supported SQL features include all DIFFERENTIAL-mode constructs: JOINs, aggregates, window functions, LATERAL, scalar subqueries, `WITH RECURSIVE`, TopK, and more. `WITH RECURSIVE` in IMMEDIATE mode uses semi-naive evaluation (INSERT-only) or Delete-and-Rederive (DELETE/UPDATE), bounded by `pg_trickle.ivm_recursive_max_depth` (default 100). TopK stream tables in IMMEDIATE mode use micro-refresh (recompute top-K on every DML), gated by `pg_trickle.ivm_topk_max_limit`.

## Architecture

```
┌─────────────────────────────────────────────┐
│              PostgreSQL 18                  │
│                                             │
│  ┌─────────┐    ┌──────────┐   ┌─────────┐  │
│  │  Source │    │  Stream  │   │pg_trickle│  │
│  │  Tables │───▸│  Tables  │   │ Catalog │  │
│  └────┬────┘    └──────────┘   └─────────┘  │
│       │                                     │
│  ┌────▼─────────────────────────────┐       │
│  │     Hybrid CDC Layer              │       │
│  │     Triggers (default) or WAL     │       │
│  └────┬─────────────────────────────┘       │
│       │                                     │
│  ┌────▼─────────────────────────────┐       │
│  │     Change Buffer Tables         │       │
│  │     pgtrickle_changes schema      │       │
│  └────┬─────────────────────────────┘       │
│       │                                     │
│  ┌────▼─────────────────────────────┐       │
│  │     DVM Engine                   │       │
│  │  ┌────────┐ ┌─────────────────┐  │       │
│  │  │ Parser │ │ Differentiation │  │       │
│  │  │ OpTree │ │  Delta Query ΔQ │  │       │
│  │  └────────┘ └─────────────────┘  │       │
│  └────┬─────────────────────────────┘       │
│       │                                     │
│  ┌────▼─────────────────────────────┐       │
│  │     Refresh Executor             │       │
│  │  Full / Differential / Immediate │       │
│  └────┬─────────────────────────────┘       │
│       │                                     │
│  ┌────▼─────────────────────────────┐       │
│  │     Background Scheduler         │       │
│  │     DAG-aware, schedule-based    │       │
│  └──────────────────────────────────┘       │
└─────────────────────────────────────────────┘
```

## Testing

```bash
just test-unit           # Pure Rust unit tests (no Docker needed)
just test-integration    # Testcontainers-based integration tests
just test-light-e2e      # Light E2E (stock postgres container, fast)
just test-e2e            # Full E2E (custom Docker image)
just test-all            # All of the above + pgrx tests
just bench               # Criterion benchmarks
```

**Test counts:** Current test counts are reported by CI on every push to `main`.
Run `just test-all` locally or check the latest
[GitHub Actions run](../../actions/workflows/ci.yml) for the live numbers.

## Contributors

- [Geir O. Grønmo](https://github.com/grove)
- [Baard H. Rehn Johansen](https://github.com/BaardBouvet)
- [GitHub Copilot](https://github.com/features/copilot) (AI pair programmer)

## License

[Apache License 2.0](LICENSE)
