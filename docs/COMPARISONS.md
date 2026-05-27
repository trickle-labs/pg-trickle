# Comparisons

This page compares pg_trickle to adjacent tools so you can decide
whether it's the right fit. Each comparison is a short, honest
summary — strengths, weaknesses, and "use this instead if…".

> If you are evaluating pg_trickle from a specific tool you already
> run, jump to the relevant section. If you want a deeper academic
> comparison, see also
> [DBSP Comparison](research/DBSP_COMPARISON.md),
> [pg_ivm Comparison](research/PG_IVM_COMPARISON.md), and
> [Prior Art](research/PRIOR_ART.md).

---

## At a glance

| Tool | Lives in PostgreSQL? | Incremental? | External infra? | Best for |
|---|---|---|---|---|
| **pg_trickle** | ✅ | ✅ | ✕ | Self-maintaining materialized views inside one PostgreSQL |
| `REFRESH MATERIALIZED VIEW` | ✅ | ✕ | ✕ | Periodic full recomputation, no automation |
| [pg_ivm](https://github.com/sraoss/pg_ivm) | ✅ | ✅ (limited) | ✕ | Incremental views with a smaller SQL surface |
| [Materialize](https://materialize.com/) | ✕ (own engine) | ✅ | Whole new database | Cross-source streaming SQL |
| [Feldera](https://www.feldera.com/) | ✕ (own engine) | ✅ | Separate service | DBSP-based incremental SQL for data pipelines |
| [RisingWave](https://risingwave.com/) | ✕ (own engine) | ✅ | Whole new database | Streaming SQL with PostgreSQL wire compat |
| [DuckDB + DuckLake](https://ducklake.select/) | ✕ | ✅ (via pg_trickle) | Object storage | Analytical queries on a Parquet/Iceberg lake |
| [Apache Flink](https://flink.apache.org/) | ✕ | ✅ | JVM cluster + state backend | Stateful event processing at scale |
| [Debezium](https://debezium.io/) + sink | ✕ | (CDC only) | Kafka + Connect | Replicating change events out of PostgreSQL |
| [ksqlDB](https://ksqldb.io/) | ✕ | ✅ | Kafka cluster | Streaming SQL on top of Kafka |
| [Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about) | ✕ | ✅ | Snowflake | Auto-refreshing tables in Snowflake |
| Custom cron + materialized view | ✅ | ✕ | ✕ | What teams build before they find pg_trickle |

---

## vs. PostgreSQL `REFRESH MATERIALIZED VIEW`

**The question this answers:** "I'm already using materialized
views — what would I gain?"

| | `REFRESH MATERIALIZED VIEW` | pg_trickle stream table |
|---|---|---|
| Refresh trigger | Manual (or your cron) | Schedule, transition, or in-transaction (IMMEDIATE) |
| Refresh cost | Always full recomputation | Incremental (delta only) for most queries |
| Cross-table dependencies | Manual coordination | DAG-aware topological refresh |
| Concurrency | `CONCURRENTLY` requires unique index | Always non-blocking; advisory locks coordinate |
| Read-your-writes | Not possible | `IMMEDIATE` mode |
| Operator coverage | Anything PostgreSQL supports | A large but explicit subset (see [SQL Reference](SQL_REFERENCE.md)) |

**Use vanilla materialized views if:** you only refresh occasionally,
your data is small, and you do not have a chain of dependent views.

**Switch to pg_trickle if:** any of those things stop being true.

---

## vs. pg_ivm

**The question this answers:** "There's another PostgreSQL extension
in this space — how do they relate?"

[pg_ivm](https://github.com/sraoss/pg_ivm) is an open-source IVM
extension that pioneered much of the relevant work in PostgreSQL
land. The two projects have different scopes.

| | pg_ivm | pg_trickle |
|---|---|---|
| Maturity | First released 2022 | First released 2024 |
| Refresh model | Trigger-driven, statement-by-statement | Trigger or WAL CDC + scheduler + DAG |
| SQL coverage | Aggregates, simple joins, sub-queries | Full DBSP-style coverage incl. WITH RECURSIVE, window functions, FULL OUTER JOIN, LATERAL, GROUPING SETS, scalar subqueries |
| Cross-table chains | Manual | DAG with topological refresh and CALCULATED schedules |
| Modes | Always immediate | AUTO / DIFFERENTIAL / FULL / IMMEDIATE |
| Distributed | — | Citus integration |
| Operations | Minimal tooling | Health-check, fuse, parallel refresh, snapshots, dbt |

There is a more thorough side-by-side at
[research/PG_IVM_COMPARISON.md](research/PG_IVM_COMPARISON.md).

If your queries are simple aggregates and you want the smallest
possible install footprint, pg_ivm is a perfectly good choice. If
you want broader SQL, multi-layer DAGs, or operational tooling,
pg_trickle is closer to that shape.

---

## vs. Materialize

[Materialize](https://materialize.com/) is a cloud-native database
built specifically for incremental view maintenance. It is the
inspiration for much of this space.

| | Materialize | pg_trickle |
|---|---|---|
| Deployment | Separate cloud database (or self-hosted server) | Extension inside PostgreSQL |
| Source coverage | PostgreSQL, Kafka, S3, MySQL, … | PostgreSQL tables (incl. Citus, foreign tables) |
| Latency | Streaming, sub-second | Sub-second with `1s` schedule; in-transaction with IMMEDIATE |
| Joins / aggregates / recursion | Yes, very mature | Yes |
| Pricing | Commercial cloud product | Open-source, runs anywhere PostgreSQL runs |
| Operational footprint | Managed service or significant self-hosted commitment | Add-on to existing PostgreSQL |

**Use Materialize if:** you want one engine to materialise across
many heterogeneous sources, you want true streaming semantics, and
you are happy operating a separate database.

**Use pg_trickle if:** your data lives in PostgreSQL and you want
the materialisation to live there too.

---

## vs. RisingWave

[RisingWave](https://risingwave.com/) is a PostgreSQL-wire-compatible
streaming database in Rust. Like Materialize, it is its own engine
that you deploy alongside (or instead of) PostgreSQL.

The same trade-off applies: RisingWave is a richer streaming
engine; pg_trickle is the answer if you do not want to operate a
second database.

---

## vs. Apache Flink (or Spark Structured Streaming)

Flink is a general stateful stream processor. It can do everything
pg_trickle can and a lot more — including state-machine workflows,
event-time semantics, and complex windowing.

The trade-off is operational. Flink wants a JVM cluster, a state
backend (RocksDB / S3), checkpointing, savepoint management, a
schema registry, and so on. For "I want my materialized views to
update themselves", that is overkill.

**Use Flink if:** you have stateful event processing that goes
beyond derived tables — state machines, complex CEP, multi-source
joins at high throughput.

**Use pg_trickle if:** you want stream-table semantics and you are
already running PostgreSQL.

---

## vs. Debezium + sink (Kafka Connect, etc.)

Debezium captures changes from PostgreSQL and emits them onto Kafka
(or another stream). It is *only* the change-capture half of the
problem — you still need a downstream consumer that turns those
changes into a derived table.

| | Debezium | pg_trickle |
|---|---|---|
| Captures changes from PostgreSQL | ✅ | ✅ (built-in CDC) |
| Computes derived tables | ✕ (you write that) | ✅ |
| Kafka required | ✅ | ✕ |
| Downstream sinks | Many | Logical replication via [downstream publications](PUBLICATIONS.md) |

**Use Debezium if:** you need to fan changes out to many
heterogeneous downstream systems (Elasticsearch, S3, Snowflake, a
data lake).

**Use pg_trickle if:** you want the derived table to live in
PostgreSQL itself. You can still expose stream-table changes via
[downstream publications](PUBLICATIONS.md) — and even use
Debezium to read those.

---

## vs. ksqlDB

[ksqlDB](https://ksqldb.io/) gives you streaming SQL on top of
Kafka. Same trade-off as Materialize/RisingWave: another engine,
another set of operational concerns.

If your data already lives in Kafka and you want SQL on it, ksqlDB
is a fine choice. If your data lives in PostgreSQL, pg_trickle is
closer to where it already is.

---

## vs. Snowflake Dynamic Tables

[Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
are auto-refreshing tables inside Snowflake. They occupy almost
exactly the same conceptual slot as pg_trickle — but in a different
database.

Use whichever matches the database you have.

---

## vs. "cron + `REFRESH MATERIALIZED VIEW`"

This is what most teams build before they find a real IVM tool.
It works, until:

- Refreshes start to overlap.
- A long refresh blocks readers.
- The refresh becomes too expensive to run as often as you'd like.
- A second view depends on the first and you start writing
  ordering logic.
- A failure leaves stale data and nobody notices.

When that happens, [pg_trickle's quick start](QUICKSTART_5MIN.md) is
~5 minutes of setup.

---

## vs. Feldera

[Feldera](https://www.feldera.com/) is an open-source incremental SQL
engine built on [DBSP](https://arxiv.org/abs/2203.16684) — the same
Z-set / differential dataflow theory that underlies pg_trickle's DVM
engine. Both systems are implementations of the same mathematical
model, but they occupy very different deployment shapes.

| | Feldera | pg_trickle |
|---|---|---|
| Deployment | Separate service (Docker / cloud) | Extension inside PostgreSQL |
| Data lives in | Feldera's own storage / external connectors | PostgreSQL |
| Source coverage | Kafka, PostgreSQL CDC, S3, HTTP, and more via connectors | PostgreSQL tables (incl. Citus, foreign tables, DuckLake) |
| SQL surface | SQL:2016 subset (aggregates, joins, window functions, UDFs) | Very similar surface plus WITH RECURSIVE, LATERAL, GROUPING SETS |
| Theoretical basis | DBSP Z-sets (same as pg_trickle) | DBSP Z-sets |
| Latency | Sub-second streaming (push model) | Sub-second with `1s` schedule; in-transaction with IMMEDIATE |
| Consistency model | Epoch-based snapshot consistency | PostgreSQL MVCC + per-refresh transaction |
| Operational model | API-driven pipeline management | SQL DDL (`CREATE STREAM TABLE`, `ALTER STREAM TABLE`) |
| Pricing | Open-source + managed cloud offering | Open-source, runs anywhere PostgreSQL runs |
| Ecosystem | Kafka-native; Rust/Python SDK | PostgreSQL-native; dbt, CNPG, Grafana |

**Shared DNA:** Both systems use Z-set multiset semantics, support
nested deltas, and handle the same set of SQL operators (INNER JOIN,
LEFT JOIN, aggregates, subqueries, etc.) in an incremental manner.
Feldera's open-source codebase and pg_trickle have independently
implemented the same core delta rules from the DBSP paper.

**Use Feldera if:** you want a dedicated streaming pipeline service
with first-class Kafka integration, and you are comfortable managing
a separate service. Feldera shines when your data arrives from
multiple heterogeneous sources and you want a unified pipeline API.

**Use pg_trickle if:** your data already lives in PostgreSQL and you
want the incremental computation to live there too — no extra service
to deploy, no Kafka required.

---

## vs. DuckDB / DuckLake

[DuckDB](https://duckdb.org/) is an in-process OLAP engine.
[DuckLake](https://ducklake.select/) is a PostgreSQL-catalog-backed
Delta Lake / Iceberg format that DuckDB can write and query.

These two projects occupy a *complementary* position to pg_trickle,
not a competing one. The combination `PostgreSQL + pg_trickle + DuckLake + DuckDB`
gives you a full modern data stack in a surprisingly small footprint:

| Layer | Tool | Role |
|---|---|---|
| OLTP storage | PostgreSQL | Transactional writes |
| Incremental aggregation | pg_trickle | Keeps derived tables fresh |
| Historical / analytical archive | DuckLake (on S3) | Long-retention Parquet store |
| Ad-hoc OLAP queries | DuckDB | Fast analytical queries |

Pg_trickle v0.66+ supports a **DuckLake sink** that writes stream-table
results directly into DuckLake as Parquet snapshots, closing the full
bidirectional loop.

| | DuckDB/DuckLake alone | pg_trickle + DuckLake |
|---|---|---|
| Incremental maintenance | ✕ (DuckDB re-reads full snapshot) | ✅ (O(Δ) differential refresh) |
| Lives in PostgreSQL | ✕ | ✅ |
| Writes to object storage | ✅ (DuckLake native) | ✅ (via DuckLake sink, v0.66+) |
| Change capture from PostgreSQL | ✕ | ✅ (trigger CDC or WAL CDC) |
| SQL coverage for IVM | N/A | Full DBSP coverage |

---

## Comprehensive IVM comparison matrix

This table covers the five most commonly evaluated IVM systems across
eight axes. Ratings are approximate and based on publicly available
information as of 2026.

### SQL coverage

| Feature | pg_ivm | Materialize | Feldera | DuckDB (MV) | pg_trickle |
|---|---|---|---|---|---|
| INNER JOIN | ✅ | ✅ | ✅ | ✅ | ✅ |
| LEFT / RIGHT JOIN | ✅ (basic) | ✅ | ✅ | ✅ | ✅ |
| FULL OUTER JOIN | ✕ | ✅ | ✅ | ✅ | ✅ |
| Aggregates (COUNT, SUM, AVG…) | ✅ | ✅ | ✅ | ✅ | ✅ (39 aggregate functions) |
| DISTINCT | ✕ | ✅ | ✅ | ✅ | ✅ |
| HAVING | ✕ | ✅ | ✅ | ✅ | ✅ |
| Scalar subqueries | ✕ | ✅ | ✕ | ✅ | ✅ |
| EXISTS / NOT EXISTS | ✕ | ✅ | ✅ | ✅ | ✅ |
| NOT IN / anti-join | ✕ | ✅ | ✅ | ✅ | ✅ |
| WITH RECURSIVE | ✕ | ✕ | ✅ | ✅ | ✅ |
| Window functions | ✕ | ✅ | ✅ | ✅ | ✅ |
| LATERAL | ✕ | ✅ | ✕ | ✅ | ✅ |
| GROUPING SETS / CUBE / ROLLUP | ✕ | ✕ | ✕ | ✅ | ✅ |
| JSON aggregates | ✕ | ✕ | ✕ | ✅ | ✅ |

### Consistency model

| | pg_ivm | Materialize | Feldera | DuckDB (MV) | pg_trickle |
|---|---|---|---|---|---|
| Isolation level | PostgreSQL MVCC | Linearizable (own store) | Epoch-based snapshot | Per-query snapshot | PostgreSQL MVCC |
| Results always fresh? | Yes (immediate) | Yes (streaming) | Yes (epoch-based) | No (manual refresh) | Configurable (IMMEDIATE / scheduled) |
| Read-your-writes | ✅ | ✅ | ✕ | ✕ | ✅ (IMMEDIATE mode) |
| Atomic multi-view refresh | ✕ | ✕ | ✅ (pipeline epoch) | ✕ | ✅ (diamond consistency groups) |

### Change data capture (CDC)

| | pg_ivm | Materialize | Feldera | DuckDB (MV) | pg_trickle |
|---|---|---|---|---|---|
| Capture mechanism | Statement-level triggers | Logical replication slot | External connectors (Kafka, HTTP, …) | Full re-scan | Row-level AFTER triggers OR WAL CDC |
| Multi-source | ✕ | ✅ | ✅ | ✕ | ✅ (Citus, foreign tables, DuckLake) |
| Kafka source | ✕ | ✅ | ✅ | ✕ | ✕ (use Debezium + pg_trickle) |
| Cost of capture | Medium (trigger overhead) | Medium (replication lag) | Low (connector-based) | High (full scan) | Low to medium (trigger or WAL) |

### Performance profile

| | pg_ivm | Materialize | Feldera | DuckDB (MV) | pg_trickle |
|---|---|---|---|---|---|
| Refresh cost per change | O(Δ) | O(Δ) | O(Δ) | O(full table) | O(Δ) |
| Minimum refresh latency | In-transaction | Sub-second | Sub-second | Manual trigger | In-transaction (IMMEDIATE) or ≥ 1 s (scheduled) |
| Throughput at scale | Medium (single PG) | High (distributed) | High (parallel pipeline) | High (columnar OLAP) | Medium-high (parallel refresh pool) |
| TPC-H 22/22 differentially | ✕ | ✅ | ✅ | ✕ | ✅ (v0.23.0+) |

### Operational model

| | pg_ivm | Materialize | Feldera | DuckDB (MV) | pg_trickle |
|---|---|---|---|---|---|
| Deployment unit | PostgreSQL extension | Separate database service | Separate service (Docker / cloud) | In-process library | PostgreSQL extension |
| Admin surface | SQL DDL | Platform API + SQL | Pipeline YAML + SQL | SQL | SQL (`CREATE STREAM TABLE`) |
| Monitoring | None built-in | Materialize console | Metrics endpoint | None built-in | 30+ SQL functions, Prometheus, Grafana |
| Backup / restore | PostgreSQL pg_dump | Managed (cloud) | External | DuckDB export | PostgreSQL pg_dump (v0.8.0+) |
| High availability | PostgreSQL HA | Built-in (cloud) | External | External | CNPG / Patroni + pg_trickle HA guide |
| Multi-tenant | RLS on base tables | Isolated environments | Isolated pipelines | File-level isolation | RLS on stream tables (v0.5.0+) |
| dbt integration | ✕ | ✅ (dbt-materialize) | ✕ | ✅ (dbt-duckdb) | ✅ (dbt-pgtrickle, v0.15.0+) |

> **Disclaimer:** Feature support changes rapidly in all these projects.
> Verify current capabilities in each project's own documentation before
> making a production architecture decision.

---

**See also:**
[Use Cases](USE_CASES.md) ·
[Migrating from materialized views](tutorials/MIGRATING_FROM_MATERIALIZED_VIEWS.md) ·
[Migrating from pg_ivm](tutorials/MIGRATING_FROM_PG_IVM.md) ·
[Research and prior art](research/PRIOR_ART.md)
