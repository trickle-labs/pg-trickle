# pg_trickle Roadmap

> **Audience:** Product managers, stakeholders, and technically curious readers
> who want to understand what each release delivers and why it matters —
> without needing to read Rust code or SQL specifications.

## Versions

### Foundation (v0.1.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.1.0](roadmap/v0.1.0.md) | The complete foundation — differential engine, CDC, scheduling, monitoring | ✅ Released | Very Large | [Full details](roadmap/v0.1.x.md-full.md) |
| [v0.1.1](roadmap/v0.1.1.md) | Change capture correctness fixes (WAL decoder, UPDATE handling) | ✅ Released | Patch | [Full details](roadmap/v0.1.x.md-full.md) |
| [v0.1.2](roadmap/v0.1.2.md) | DDL tracking improvements and PgBouncer compatibility | ✅ Released | Patch | [Full details](roadmap/v0.1.x.md-full.md) |
| [v0.1.3](roadmap/v0.1.3.md) | SQL coverage completion, WAL hardening, TPC-H 22/22 | ✅ Released | Patch | [Full details](roadmap/v0.1.x.md-full.md) |

### Early Feature Development (v0.2.x – v0.5.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.2.0](roadmap/v0.2.0.md) | Top-N views, IMMEDIATE refresh mode, diamond dependency safety | ✅ Released | Medium | [Full details](roadmap/v0.2.0.md-full.md) |
| [v0.2.1](roadmap/v0.2.1.md) | Upgrade infrastructure and documentation expansion | ✅ Released | Small | [Full details](roadmap/v0.2.1.md-full.md) |
| [v0.2.2](roadmap/v0.2.2.md) | Paginated top-N, AUTO mode default, ALTER QUERY | ✅ Released | Medium | [Full details](roadmap/v0.2.2.md-full.md) |
| [v0.2.3](roadmap/v0.2.3.md) | Non-determinism detection and operational polish | ✅ Released | Small | [Full details](roadmap/v0.2.3.md-full.md) |
| [v0.3.0](roadmap/v0.3.0.md) | Correctness for HAVING, FULL OUTER JOIN, and correlated subqueries | ✅ Released | Medium | [Full details](roadmap/v0.3.0.md-full.md) |
| [v0.4.0](roadmap/v0.4.0.md) | Parallel refresh, statement-level CDC triggers, cross-source consistency | ✅ Released | Medium | [Full details](roadmap/v0.4.0.md-full.md) |
| [v0.5.0](roadmap/v0.5.0.md) | Row-level security, ETL bootstrap gating, API polish | ✅ Released | Medium | [Full details](roadmap/v0.5.0.md-full.md) |

### Scalability and Robustness (v0.6.x – v0.9.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.6.0](roadmap/v0.6.0.md) | Partitioned source tables, idempotent DDL, circular dependency foundation | ✅ Released | Medium | [Full details](roadmap/v0.6.0.md-full.md) |
| [v0.7.0](roadmap/v0.7.0.md) | Circular DAG execution, watermarks, Prometheus/Grafana observability | ✅ Released | Large | [Full details](roadmap/v0.7.0.md-full.md) |
| [v0.8.0](roadmap/v0.8.0.md) | pg_dump backup support and multiset invariant testing | ✅ Released | Small | [Full details](roadmap/v0.8.0.md-full.md) |
| [v0.9.0](roadmap/v0.9.0.md) | Algebraic aggregate maintenance — AVG, STDDEV, COUNT(DISTINCT) | ✅ Released | Medium | [Full details](roadmap/v0.9.0.md-full.md) |

### Production Readiness (v0.10.x – v0.14.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.10.0](roadmap/v0.10.0.md) | DVM hardening, PgBouncer compatibility, "No Surprises" UX | ✅ Released | Medium | [Full details](roadmap/v0.10.0.md-full.md) |
| [v0.11.0](roadmap/v0.11.0.md) | Partitioned stream tables, event-driven scheduler (34× latency), circuit breaker | ✅ Released | Large | [Full details](roadmap/v0.11.0.md-full.md) |
| [v0.12.0](roadmap/v0.12.0.md) | Three-table join fix (EC-01), developer tools, SQLancer fuzzing | ✅ Released | Medium | [Full details](roadmap/v0.12.0.md-full.md) |
| [v0.13.0](roadmap/v0.13.0.md) | Columnar change tracking, shared buffers, TPC-H 22/22 DIFFERENTIAL | ✅ Released | Large | [Full details](roadmap/v0.13.0.md-full.md) |
| [v0.14.0](roadmap/v0.14.0.md) | Tiered scheduling, UNLOGGED buffers, diagnostics | ✅ Released | Medium | [Full details](roadmap/v0.14.0.md-full.md) |

### Performance and Integration (v0.15.x – v0.19.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.15.0](roadmap/v0.15.0.md) | Nexmark benchmark, bulk create API, watermark hold-back, dbt Hub | ✅ Released | Medium | [Full details](roadmap/v0.15.0.md-full.md) |
| [v0.16.0](roadmap/v0.16.0.md) | Append-only fast path, algebraic aggregates, auto-indexing, benchmark CI | ✅ Released | Medium | [Full details](roadmap/v0.16.0.md-full.md) |
| [v0.17.0](roadmap/v0.17.0.md) | Cost-based refresh strategy, incremental DAG rebuild, pg_ivm migration guide | ✅ Released | Large | [Full details](roadmap/v0.17.0.md-full.md) |
| [v0.18.0](roadmap/v0.18.0.md) | Z-set delta engine, consistency enforcement, safety hardening | ✅ Released | Large | [Full details](roadmap/v0.18.0.md-full.md) |
| [v0.19.0](roadmap/v0.19.0.md) | Security hardening, packaging (PGXN, Docker Hub, apt/rpm) | ✅ Released | Medium | [Full details](roadmap/v0.19.0.md-full.md) |

### Self-Monitoring and Deep Correctness (v0.20.x – v0.27.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.20.0](roadmap/v0.20.0.md) | pg_trickle monitors itself using its own stream tables | ✅ Released | Large | [Full details](roadmap/v0.20.0.md-full.md) |
| [v0.21.0](roadmap/v0.21.0.md) | Correctness hardening, zero-crash guarantee, shadow/canary mode | ✅ Released | Large | [Full details](roadmap/v0.21.0.md-full.md) |
| [v0.22.0](roadmap/v0.22.0.md) | Downstream CDC publication, parallel refresh pool, SLA tier auto-assignment | ✅ Released | Large | [Full details](roadmap/v0.22.0.md-full.md) |
| [v0.23.0](roadmap/v0.23.0.md) | TPC-H DVM scaling performance — all 22 queries at O(Δ) | ✅ Released | Large | [Full details](roadmap/v0.23.0.md-full.md) |
| [v0.24.0](roadmap/v0.24.0.md) | Join correctness complete fix, two-phase frontier, TOAST-aware CDC | ✅ Released | Large | [Full details](roadmap/v0.24.0.md-full.md) |
| [v0.25.0](roadmap/v0.25.0.md) | Thousands of stream tables, pooler cold-start fix, predictive model | ✅ Released | Large | [Full details](roadmap/v0.25.0.md-full.md) |
| [v0.26.0](roadmap/v0.26.0.md) | Concurrency testing, fuzz targets, refresh engine modularisation | ✅ Released | Large | [Full details](roadmap/v0.26.0.md-full.md) |
| [v0.27.0](roadmap/v0.27.0.md) | Snapshot/PITR, schedule recommendations, cluster observability | ✅ Released | Medium | [Full details](roadmap/v0.27.0.md-full.md) |

### Toward Stable (v0.28.x – v1.0)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.28.0](roadmap/v0.28.0.md) | Reliable event messaging built into PostgreSQL | ✅ Released | Large | [Full details](roadmap/v0.28.0.md-full.md) |
| [v0.29.0](roadmap/v0.29.0.md) | Off-the-shelf connector to Kafka, NATS, SQS, and more | ✅ Released | Large | [Full details](roadmap/v0.29.0.md-full.md) |
| [v0.30.0](roadmap/v0.30.0.md) | Quality gate before 1.0 — correctness, stability, and docs | ✅ Released | Medium | [Full details](roadmap/v0.30.0.md-full.md) |
| [v0.31.0](roadmap/v0.31.0.md) | Smarter scheduling and faster hot paths | ✅ Released | Medium | [Full details](roadmap/v0.31.0.md-full.md) |
| [v0.32.0](roadmap/v0.32.0.md) | Citus: stable object naming and per-source frontier foundation | ✅ Released | Medium | [Full details](roadmap/v0.32.0.md-full.md) |
| [v0.33.0](roadmap/v0.33.0.md) | Citus: world-class distributed source CDC and stream table support | ✅ Released | Large | [Full details](roadmap/v0.33.0.md-full.md) |
| [v0.34.0](roadmap/v0.34.0.md) | Citus: automated distributed CDC scheduler wiring and shard rebalance auto-recovery | ✅ Released | Medium | [Full details](roadmap/v0.34.0.md-full.md) |
| [v0.35.0](roadmap/v0.35.0.md) | EC-01 correctness closeout, Citus chaos hardening, reactive subscriptions, zero-downtime schema changes | ✅ Released | Large | [Full details](roadmap/v0.35.0.md-full.md) |
| [v0.36.0](roadmap/v0.36.0.md) | Structural hardening, L0 cache, WAL backpressure, temporal IVM, columnar storage | ✅ Released | Large | [Full details](roadmap/v0.36.0.md-full.md) |
| [v0.37.0](roadmap/v0.37.0.md) | Scheduler & merge modularisation, pgVectorMV (vector_avg/sum), OpenTelemetry trace propagation | ✅ Released | Medium | [Full details](roadmap/v0.37.0.md-full.md) |
| [v0.38.0](roadmap/v0.38.0.md) | EC-01 Correctness Sprint (Hard Gate): join phantom rows, property-test convergence proof — BLOCKING release gate | ✅ Released | Medium | [Full details](roadmap/v0.38.0.md-full.md) |
| [v0.39.0](roadmap/v0.39.0.md) | Operational Truthfulness & Distributed Hardening: backpressure/wake fix, generated docs, Citus chaos, SQLSTATE rollout, diagnostics | ✅ Released | Large | [Full details](roadmap/v0.39.0.md-full.md) |
| [v0.40.0](roadmap/v0.40.0.md) | Operator trust and maintainability: generated references, alerting, drain-mode proof, secret hygiene, unsafe gating | ✅ Released | Large | [Full details](roadmap/v0.40.0.md-full.md) |
| [v0.41.0](roadmap/v0.41.0.md) | DVM correctness: structural cache keys, placeholder safety, WAL transition guards | ✅ Released | Medium | [Full details](roadmap/v0.41.0.md-full.md) |
| [v0.42.0](roadmap/v0.42.0.md) | Documentation truthfulness + test quality: repair_stream_table, catalog generator, SQL reference, sleep removal, fuzz CI | ✅ Released | Large | [Full details](roadmap/v0.42.0.md-full.md) |
| [v0.43.0](roadmap/v0.43.0.md) | Performance tunability: deep-join GUCs, GROUP_RESCAN improvement, explain_stream_table diagnostics, D+I change buffer refactor | ✅ Released | Large | [Full details](roadmap/v0.43.0.md-full.md) |
| [v0.44.0](roadmap/v0.44.0.md) | Security hardening: IVM search_path fix, centralized SQL builder, RLS warnings, module decomposition | ✅ Released | Large | [Full details](roadmap/v0.44.0.md-full.md) |
| [v0.45.0](roadmap/v0.45.0.md) | Operational readiness: preflight functions, scalability infrastructure, CI completeness, CNPG production examples | ✅ Released | Large | [Full details](roadmap/v0.45.0.md-full.md) |

### `pg_tide` Extraction (v0.46.0)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.46.0](roadmap/v0.46.0.md) | Extract `pg_tide`: standalone transactional outbox, inbox, and relay into `trickle-labs/pg-tide` | ✅ Released | Large | [Full details](roadmap/v0.46.0.md-full.md) |

### Embedding & AI Programme (v0.47.x – v0.48.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v0.47.0](roadmap/v0.47.0.md) | Embedding pipeline infrastructure: post-refresh hooks, drift-based reindex, vector monitoring | ✅ Released | Medium | [Full details](roadmap/v0.47.0.md-full.md) |
| [v0.48.0](roadmap/v0.48.0.md) | Complete embedding programme: sparse/half-precision vector aggregates, hybrid search, embedding_stream_table() API, per-tenant ANN, embedding outbox | ✅ Released | Large | [Full details](roadmap/v0.48.0.md-full.md) |

### v1.0 Readiness Arc (v0.49.x – v0.51.x)

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.49.0](roadmap/v0.49.0.md) | Test infrastructure hardening: concurrency synchronization overhaul, 10-module unit test sweep, merge/row_id fuzz targets, DDL-during-refresh E2E, scheduler decomposition, CI smoke breadth | ✅ Released | Large | [Full details](roadmap/v0.49.0.md-full.md) |
| [v0.49.1](roadmap/v0.49.1.md) | Repository migration to trickle-labs/pg-trickle: updated CI/CD, Docker, PGXN, dbt Hub, and CloudNativePG artifact publishing | ✅ Released | Patch | — |
| [v0.50.0](roadmap/v0.50.0.md) | Performance, security & operational hardening: SPI batching in differential refresh, dblink escaping fix, CNPG graceful-drain preStop hook, Docker image digest pinning, invalidation ring observability, deep-join drift monitoring, Prometheus secondary metrics | ✅ Released | Large | [Full details](roadmap/v0.50.0.md-full.md) |
| [v0.51.0](roadmap/v0.51.0.md) | Citus chaos resilience & documentation truth: chaos test rig (node kill/rebalance/partition), deprecated GUC removal, ARCHITECTURE.md pg_tide boundary, recursive CTE strategy docs, CDC-enabled-flag documentation | ✅ Released | Large | [Full details](roadmap/v0.51.0.md-full.md) |

### Assessment-Driven Final Hardening Arc (v0.52.x – v0.55.x)

Driven by the findings in the v0.51.0 overall assessment
([plans/PLAN_OVERALL_ASSESSMENT_11.md](plans/PLAN_OVERALL_ASSESSMENT_11.md)).
The assessment found 0 critical, 2 HIGH, and 22 MEDIUM findings across
correctness, performance, scalability, test coverage, code quality,
security, and feature completeness — all resolved in this four-release arc
before v1.0.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.52.0](roadmap/v0.52.0.md) | DVM hot-path performance: O(1) placeholder resolution (aho-corasick), thread-local volatility cache, lazy DiffContext allocations, O(1) template LRU eviction | ✅ Released | Large | [Full details](roadmap/v0.52.0.md-full.md) |
| [v0.53.0](roadmap/v0.53.0.md) | Unit test depth sweep: dag, scheduler, CDC, parser, config — eleven modules with zero inline coverage — plus proptest extension and buffer-growth sleep removal | ✅ Released | Large | [Full details](roadmap/v0.53.0.md-full.md) |
| [v0.54.0](roadmap/v0.54.0.md) | DVM engine hardening: diff_node depth limit, DiffContext CTE cap (OOM guard), snapshot fingerprint caching, Expr::to_sql() caching, view inlining fixpoint + batched relkind, ST source frontier validation, O(V+E) diamond detection | ✅ Released | Large | [Full details](roadmap/v0.54.0.md-full.md) |
| [v0.55.0](roadmap/v0.55.0.md) | Final pre-1.0 polish: GUC-configurable invalidation ring, api/mod.rs and monitor.rs module decomposition, serde_json NOTIFY payloads, multi-column IN rewrite to EXISTS, DVM parse metrics, reserved-prefix docs, GUC rationale comments, PR coverage gate | ✅ Released | Large | [Full details](roadmap/v0.55.0.md-full.md) |

### Documentation Excellence Arc (v0.56.x – v0.57.x)

Driven by the findings in the Round 2 documentation audit
([plans/PLAN_DOCUMENTATION_GAPS_2.md](plans/PLAN_DOCUMENTATION_GAPS_2.md),
2026-05-11). The audit found 3 P0 blockers (corrupted GUC_CATALOG.md, 54%-complete
ERRORS.md, wrong GUC default), 8 P1 items, 7 P2 items, 5 P3 items, and 7 new
documents that should exist before v1.0. This two-release arc resolves all
findings and delivers the world-class documentation standard planned for the
stable release.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|-------------- |
| [v0.56.0](roadmap/v0.56.0.md) | Documentation Foundation: fix GUC_CATALOG corruption, complete ERRORS.md (all 44 variants), correct parallel_refresh_mode default, complete SQL_REFERENCE outbox/inbox, add MENTAL_MODEL.md, LIMITATIONS.md, PERFORMANCE_CHEATSHEET.md | ✅ Released | Large | [Full details](roadmap/v0.56.0.md-full.md) |
| [v0.57.0](roadmap/v0.57.0.md) | Documentation Excellence: four new tutorials (first dashboard, event sourcing, backfill/migration, security hardening), P2/P3 quality polish, full 83-file consistency sweep | ✅ Released | Large | [Full details](roadmap/v0.57.0.md-full.md) |

### Assessment-Driven Hardening Arc (v0.58.x – v0.61.x)

Driven by the findings in the v0.57.0 overall assessment
([plans/PLAN_OVERALL_ASSESSMENT_12.md](plans/PLAN_OVERALL_ASSESSMENT_12.md)).
The assessment found 0 critical, 4 HIGH, 23 MEDIUM, and 20 LOW findings across
security (ownership bypass in outbox/publication APIs), correctness (recursive-CTE
depth guard in DIFFERENTIAL mode, multi-column NOT IN + NULL semantics, WAL decoder
TOCTOU race), performance (per-source SPI fan-out in monitor, merge-template clone
overhead, WAL decoder allocation patterns), observability (missing CDC-lag
percentiles, worker queue-depth, WAL decoder queue, refresh-mode ratio counters),
code quality (scheduler log levels, codegen decomposition, cdc.rs split), and test
coverage (refresh orchestrator, CDC, hooks, remaining fixed sleeps). This four-release
arc resolves all findings before v1.0.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.58.0](roadmap/v0.58.0.md) | Security & Correctness Hardening: ownership checks for outbox/publication APIs, multi-column NOT IN + NULL fix, recursive CTE depth guard in DIFFERENTIAL mode, WAL decoder TOCTOU advisory lock, DDL hook escalation on SPI failure | ✅ Released | Medium | [Full details](roadmap/v0.58.0.md-full.md) |
| [v0.59.0](roadmap/v0.59.0.md) | Performance & Observability: batched monitor buffer-growth SPI, query-hash caching, Arc<str> merge templates, WAL decoder Vec pre-allocation, frontier borrow not clone, CDC-lag percentile metrics, worker queue-depth, WAL decoder queue, refresh-mode ratio counters, application_name in BGW, backup/restore docs | ✅ Released | Large | [Full details](roadmap/v0.59.0.md-full.md) |
| [v0.60.0](roadmap/v0.60.0.md) | Code Quality, Test Coverage & CI: scheduler log levels, codegen decomposition, cdc.rs 4-way split, refresh orchestrator/merge/CDC/hooks unit tests, differential idempotence proptest, sleep removal, WAL OID filter, partition-attach rebuild, path-filtered full E2E on PRs, Dockerfile non-root, codecov module thresholds | ✅ Released | Large | [Full details](roadmap/v0.60.0.md-full.md) |
| [v0.61.0](roadmap/v0.61.0.md) | DX, Documentation & Final Pre-1.0 Polish: health_check() foreign-owner row, SQL_REFERENCE completeness, snapshot cache secondary equality, cte_counter reset, outbox name collision fix, sublinks.rs decomposition, ctid invariant comment, 3 foundational ADRs, LIMITATIONS.md NOT IN + NULL section, SEARCH/CYCLE clear error, LATERAL+DIFFERENTIAL docs | ✅ Released | Large | [Full details](roadmap/v0.61.0.md-full.md) |

### Scheduler Throughput Arc (v0.62.x – v0.63.x)

Two releases targeting scheduler throughput: eliminating redundant change-buffer
scans via fan-out, adding the `pause_scheduler` / `resume_scheduler` /
`stream_table_spec` SQL API required by the planned `pg_aqueduct` migration tool
([pg-aqueduct plan](https://github.com/trickle-labs/pg-aqueduct/blob/main/plans/pg-aqueduct-plan.md)), and implementing
fused CTE refresh to reduce per-tick statement overhead for multi-node DAGs.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.62.0](roadmap/v0.62.0.md) | Scheduler throughput: change-buffer fan-out (O(N)→O(1) scans for multi-consumer DAGs), `pause_scheduler` / `resume_scheduler` per-node SQL functions, `stream_table_spec(oid)` stable JSON projection | ✅ Released | Medium | [Full details](roadmap/v0.62.0.md-full.md) |
| [v0.63.0](roadmap/v0.63.0.md) | Fused multi-node refresh: CTE-chain composition of per-node delta SQL in a single statement, correctness property test, benchmark regression gate (≥ 20 % wall-time reduction on TPC-H 22-node DAG) | ✅ Released | Large | [Full details](roadmap/v0.63.0.md-full.md) |

### DuckLake Ecosystem Arc (v0.64.x)

Phase 1 of the [DuckLake integration plan](plans/ecosystem/PLAN_DUCKLAKE.md): publish
tutorials, blog posts, containerised demos, and reference architectures that
demonstrate pg_trickle working with DuckLake's PostgreSQL catalog today — zero new
extension code required. This establishes pg_trickle as the incremental view
maintenance layer for data lakes, creates thought leadership ahead of the v1.0
stable release, and seeds demand signals that will guide whether Phase 2
(DuckLake-optimised change-feed polling) is worth engineering investment.
Community outreach to named DuckLake production users (PostHog, Windmill,
Ascend.io, Sliplane, locals.com, Media Cluster Norway) is explicitly part of this
release.

Nine deliverables, all documentation / community / demo:

1. **Tutorial: "Real-Time Dashboards on Your Data Lake"** — DuckDB writes events to DuckLake; pg_trickle stream tables compute per-minute aggregations; Grafana dashboard powered by PostgreSQL.
2. **Tutorial: "The Modern Data Stack in One Box"** — OLTP in PostgreSQL + pg_trickle aggregations + DuckLake for historical analytics + DuckDB for ad-hoc queries, all from one instance and an S3 bucket — no Kafka, no Airflow.
3. **Tutorial: "Monitoring Your DuckLake with pg_trickle"** — stream tables over DuckLake's 28 metadata tables; real-time alerts for small-file proliferation, snapshot rate spikes, and storage growth.
4. **Blog post: "Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM"** — thought-leadership post for Hacker News / r/dataengineering positioning pg_trickle as the IVM layer DuckLake's v2.0 roadmap explicitly calls for.
5. **Blog post: "DuckLake's `table_changes()` Meets pg_trickle's DVM Engine"** — technical deep-dive on how DuckLake's change-feed format maps directly to pg_trickle's change-buffer model; builds credibility with the systems-programming audience.
6. **Docs: DuckLake examples in `foreign-table-sources.md`** — concrete code samples for using DuckLake-backed foreign tables as stream table sources.
7. **Demo A: "Five-Second Funnel"** — self-contained `docker-compose up` demo that streams fake e-commerce events into DuckLake and displays a live pg_trickle-powered funnel dashboard; shareable for conference talks and social media.
8. **Demo D: "DuckLake Observability in a Box"** — pre-packaged Grafana dashboard powered by stream tables over DuckLake metadata; five minutes from `git clone` to operational visibility.
9. **Community: Named-user outreach + DuckCon/PGConf talk submission** — direct pitches to the named DuckLake production users identified in research, plus CFP submissions to DuckCon and PGConf EU.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|-------------- |
| [v0.64.0](roadmap/v0.64.0.md) | DuckLake ecosystem (Phase 1): 3 tutorials + 2 blog posts + docs + 2 containerised demos + community outreach — no extension code changes | ✅ Released | Small | [Full details](roadmap/v0.64.0.md-full.md) |

### DuckLake Phase 2 Arc (v0.65.x)

**Note:** All DuckLake integration code was removed in v0.76.0. The items below are historical.

Phase 2 ships the first engineering that makes pg_trickle a first-class DuckLake
citizen at the code level. The centrepiece is a purpose-built change-feed adapter
(`CdcMode::DuckLakeChangeFeed`) that calls DuckLake's `table_changes()` API and
processes O(Δ) rows instead of re-scanning the foreign table on every refresh
cycle. A snapshot-based frontier model lets a single stream table mix PostgreSQL
CDC events and DuckLake snapshot IDs in one coherent consistency story. An
inlined-data trigger adapter covers the fast path for tables small enough to live
in PostgreSQL, and row-ID plumbing wires DuckLake's `rowid` virtual column
directly into the DVM engine for O(1) delta application. Compaction-safety logic
handles the case where a DuckLake snapshot expires before pg_trickle can consume
it, with a configurable `fallback | error` policy. An integration test suite
built on DuckDB validates end-to-end correctness, and two new tutorials ship
alongside the code.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.65.0](roadmap/v0.65.0.md) | DuckLake Phase 2: change-feed adapter, snapshot frontier, inlined-data CDC, row-ID plumbing, compaction safety, integration tests, 2 tutorials, 1 demo | ✅ Released | Large | [Full details](roadmap/v0.65.0.md) |

### DuckLake Phase 3 Arc (v0.66.x – v0.67.x)

**Note:** All DuckLake integration code was removed in v0.76.0. The items below are historical.

Phase 3 implements the DuckLake *sink*: the ability for any pg_trickle stream
table to write its incrementally computed results into a DuckLake-managed Parquet
table on object storage, making those results immediately queryable from DuckDB,
Spark, Trino, and every other engine that speaks DuckLake. This closes the full
bidirectional loop — pg_trickle can now both *read* from DuckLake and *publish*
back into it.

v0.66.0 delivers the infrastructure layer: Parquet delta serialisation via
`arrow-rs`, S3 and object-store upload integration, a DuckLake catalog
transaction writer that atomically records new data files in the PostgreSQL
catalog, and per-file encryption key pass-through so encrypted lakes work from
day one. A full E2E test suite validates the write path. v0.67.0 completes the
arc with discoverability and ecosystem polish: DuckLake view registration
auto-inserts a `ducklake_view` entry for every stream table so results are
visible to every DuckLake client as a native object, snapshot provenance (INT-11)
records which stream table produced each snapshot for end-to-end lineage, and
four tutorials plus two containerised demos ship with the code.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.66.0](roadmap/v0.66.0.md) | DuckLake Phase 3a: Parquet delta export (`arrow-rs`), DuckLake sink output mode, S3 upload, catalog transaction writer, encryption key pass-through, E2E tests | ✅ Released | Large | [Full details](roadmap/v0.66.0.md) |
| [v0.67.0](roadmap/v0.67.0.md) | DuckLake Phase 3b: view registration, snapshot provenance (INT-11), pg-tide tutorial, 2 tutorials, 2 containerised demos | ✅ Released | Medium | [Full details](roadmap/v0.67.0.md-full.md) |

### Assessment-13-Driven Hardening Arc (v0.68.x – v0.71.x)

Driven by the findings in the v0.67.0 overall assessment
([plans/PLAN_OVERALL_ASSESSMENT_13.md](plans/PLAN_OVERALL_ASSESSMENT_13.md)).
The assessment found 0 critical, 10 HIGH, 19 MEDIUM, and 7 LOW findings across
correctness (fused refresh audit trail, LATERAL validation bypass, durability GUC
not wired, DuckLake timestamp NULL serialisation), reliability (DuckLake sink
warning-only delivery), scalability (stale scheduler pool code, launcher fan-out),
performance (per-source SPI storm, fused eligibility O(N×M) cost, history prune
GUC ignored), security (publication name-parser inconsistency, unqualified
DuckLake schema resolution), observability (sink metrics absent, prune failures
invisible), test coverage (no LATERAL volatile tests, stale test harness schema),
CI/CD (fuzz smoke incomplete, `fuzz-all` masks crashes, coverage schedule wrong),
code quality (SQL API catalog generator truncates return types, Tarjan SCC
panics), and documentation (PLAN.md obsolete, INDEX.md stale). This four-release
arc resolves every finding before v1.0.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.68.0](roadmap/v0.68.0.md) | Correctness & Durability Sprint: fused refresh audit trail (COR-001), wire `change_buffer_durability` into CDC (ARCH-001/COR-003), DuckLake timestamp NULL fix (COR-004), stale pool path deleted (SCAL-001), scheduler fused E2E audit test (TEST-003), durability mode tests (TEST-004) | ✅ Released | Medium | [Full details](roadmap/v0.68.0.md-full.md) |
| [v0.69.0](roadmap/v0.69.0.md) | DuckLake Sink Reliability & Security: delivery state machine with retry/backoff (ARCH-002/REL-001), view registration on query-only ALTER (COR-005), snapshot ID advisory lock (COR-006), qualified schema resolution (SEC-002), sink health metrics & Prometheus (OBS-001), dependency policy docs (DEP-002) | ✅ Released | Large | [Full details](roadmap/v0.69.0.md-full.md) |
| [v0.70.0](roadmap/v0.70.0.md) | Scheduler, Validator & Security Hardening: LATERAL body volatility scanning (COR-002), batched monitor buffer health (PERF-001), batched fused eligibility loads (PERF-002), history prune GUC wired + start_time index (PERF-003), work-mem cap conservative default (PERF-004), launcher DB cache (SCAL-002), publication name-parser unified (SEC-001), prune failure visibility (OBS-002), LATERAL volatile tests (TEST-001), cache_stats() E2E tests (TEST-002) | ✅ Released | Large | [Full details](roadmap/v0.70.0.md-full.md) |
| [v0.71.0](roadmap/v0.71.0.md) | CI Truthfulness, Test Harness & Documentation Cleanup: fuzz smoke covers all 9 targets (CI-001), fuzz-all failure propagation (CI-002), E2E coverage schedule (CI-003), docs-lint in just lint (CI-004), advisory expiry metadata (DEP-001), SQL API catalog generator rewritten (DOC-001/CODE-001), Tarjan SCC unwrap→error (CODE-002), generated test harness schema (TEST-005), PLAN.md archived (ARCH-003/DOC-002), INDEX.md regenerated (DOC-003) | ✅ Released | Medium | [Full details](roadmap/v0.71.0.md-full.md) |

### Assessment-14-Driven Hardening Arc (v0.72.x – v0.75.x)

Driven by the findings in the v0.71.0 overall assessment
([plans/PLAN_OVERALL_ASSESSMENT_14.md](plans/PLAN_OVERALL_ASSESSMENT_14.md)).
The assessment found 0 critical, 6 HIGH, 28 MEDIUM, and 8 LOW findings across
correctness and data integrity (scheduler frontier persistence best-effort only,
outbox catalog key stores `pgt_id` instead of stream-table OID, dead DUR-1
tentative-frontier recovery code with invalid SQL, WAL transition handoff race,
pristine-transaction precondition unenforced for slot creation), performance and
scalability (monitoring functions O(N×history) per stream table, multi-SPI
cleanup loops per source OID, holdback probe every tick, launcher dual catalog
scans, Aho-Corasick automaton rebuilt per resolution, no byte-size cap on
template cache), reliability (cleanup failures log-only with no durable state,
full E2E schedule/manual only, dead recovery code), security (cargo audit vs.
CI advisory split, IVM AFTER trigger includes `public` in SECURITY DEFINER
search path), test coverage (outbox key invariant untested, frontier recovery
untested, fixed stabilization sleeps, no path-filtered full E2E on risky PRs,
no per-module coverage summary), API ergonomics (missing `metrics_summary()`
SQL reference, Rust return types in generated catalog, inconsistent parameter
naming), documentation (corrupted PLAN.md index, stale README GUC count,
missing metrics section), architecture (two competing frontier durability
designs, cleanup without backpressure, launcher state memory-only, no IVM
comparison matrix), and developer experience (benchmark workflow disabled,
`just lint` narrower than CI gates, stale version tags in Dockerfile examples,
advisory policy split). This four-release arc resolves every finding.

**v0.72.0 is the hard correctness gate for this arc.** It will not ship until
the outbox `stream_table_oid` schema mismatch is fixed or explicitly migrated
with a deprecation path, the DUR-1 tentative-frontier recovery is either wired
into every refresh path or cleanly removed with a documented decision, the WAL
transition handoff acquires an explicit serialization gate, and the pristine-
transaction precondition for logical slot creation is enforced at runtime. These
are catalog contract and durability invariants that must be proven before v1.0.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.72.0](roadmap/v0.72.0.md) | Frontier Durability & Catalog Correctness: fix `pgt_outbox_config.stream_table_oid` catalog key (COR-002/API-001), wire or remove DUR-1 tentative-frontier recovery (COR-001/REL-001/ARCH-001), remove invalid recovery SQL, WAL transition explicit handoff gate (COR-003), runtime pristine-transaction guard before slot creation (COR-004), outbox OID invariant tests (TEST-001), frontier recovery tests (TEST-003), ADR documenting chosen frontier durability model (CODE-001) | ✅ Released | Large | [Full details](roadmap/v0.72.0.md-full.md) |
| [v0.73.0](roadmap/v0.73.0.md) | Monitoring Scalability & Operational Resilience: incremental refresh-history summary table (PERF-001), batched per-OID frontier cleanup (PERF-002), holdback probe caching and cost metrics (PERF-003), combined launcher database+activity discovery (PERF-004), cached Aho-Corasick automata with delta templates (PERF-005), byte-size cap and `cache_stats()` memory column (PERF-006), consolidated per-stream-table scheduler state struct (PERF-007), persistent `pgt_cleanup_status` table with retry schedule and backpressure policy (ARCH-002/REL-002), launcher state in shared memory with health metrics (ARCH-003) | ✅ Released | Large | [Full details](roadmap/v0.73.0.md-full.md) |
| [v0.74.0](roadmap/v0.74.0.md) | Test Coverage, CI Integrity & Security Hardening: replace fixed WAL/safety stabilization sleeps with condition-based polling (TEST-002), path-filtered full E2E + reduced TPC-H slice on risky PRs (TEST-004/REL-003), `just coverage-summary` recipe with per-module risk output (TEST-005), `#[cfg(test)]` unit tests for `src/refresh/merge/mod.rs`, `src/refresh/codegen.rs`, `src/api/metrics_ext.rs` (CODE-002), centralize advisory ignores in `deny.toml` and make `just security` reproduce CI (SEC-001/DEVEX-004), restrict IVM AFTER trigger search path or add targeted shadowing tests (SEC-002), SQL builder helpers audit and lint for raw `format!()` SQL (SEC-003), re-enable push-to-main benchmark baselines (DEVEX-001), add `just lint-ci` recipe covering generated doc/schema/version/docs-truth checks (DEVEX-002), replace stale version tags in Dockerfile examples and justfile (DEVEX-003), upgrade deps: sqlx 0.9.0 (query safety), lru 0.18.0, object_store 0.13.2 (DuckLake E2E) (DEP-001/002/003) | ✅ Released | Large | [Full details](roadmap/v0.74.0.md) |
| [v0.75.0](roadmap/v0.75.0.md) | API Polish, Documentation Excellence & Developer Experience: add `pgtrickle.metrics_summary` full SQL reference section with columns, examples, and cost caveats (API-002/DOC-003), normalize SQL function parameter naming convention and document it (API-003), convert generated API catalog return types to SQL-facing forms (API-004), add schedule-mode comparison table to SQL reference (API-005), introduce typed `PgtId`/`StreamTableOid` wrappers to prevent cross-domain casts (CODE-003), repair corrupted `plans/PLAN.md` architecture-doc table and add fragment-corruption lint (DOC-001), update README GUC count to generated phrase and add stale-version scanner (DOC-002/DOC-004), add `docs/COMPARISONS.md` covering pg_ivm, Materialize, Feldera, DuckDB/DuckLake, and pg_trickle across SQL coverage, consistency, CDC, performance, and operational model (ARCH-004) | ✅ Released | Large | [Full details](roadmap/v0.75.0.md) |

### DuckLake Integration Removal Arc (v0.76.x)

This arc completes the removal of all DuckLake-specific integration code from
pg_trickle. The decision is driven by three architectural insights:

1. **pg_ducklake uses native table AM, not FDW.** The `DUCKLAKE_CHANGE_FEED`
   detection heuristic (`is_ducklake_foreign_table()`) checks `pg_foreign_data_wrapper.fdwname
   LIKE '%ducklake%'` — but pg_ducklake creates tables via `CREATE TABLE ... USING ducklake`,
   which never appear in `pg_foreign_table`. The entire source-side CDC adapter was
   architecturally obsolete from the start.

2. **pg_duckpipe covers the outbound direction.** The pg_duckpipe extension
   (relytcloud) provides dedicated WAL-based PostgreSQL-to-DuckLake CDC with
   backpressure, per-table flush threads, and sync groups. Maintaining a parallel
   Parquet/S3 sink inside an IVM extension is redundant.

3. **Sink code is orthogonal to IVM.** The 1,258-line `ducklake_sink.rs` added
   three heavy Cargo dependencies (`arrow-array`, `arrow-schema`, `parquet`,
   `object_store`, `bytes`) and an async-in-sync tokio shim. Removing it cuts
   compile time and attack surface.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.76.0](roadmap/v0.76.0.md) | Complete DuckLake Integration Removal: delete `src/ducklake_sink.rs` + all sink infrastructure; remove `DUCKLAKE_CHANGE_FEED` CDC mode and all polling/trigger functions; drop `ducklake_*` columns, `pgt_ducklake_provenance`, `pgt_ducklake_sink_delivery` tables; remove `arrow-array`, `arrow-schema`, `parquet`, `object_store`, `bytes` Cargo deps; remove all DuckLake GUCs and accessors; update upgrade SQL | ✅ Released | Medium | [Full details](roadmap/v0.76.0.md) |

### Assessment-15-Driven Hardening Arc (v0.77.x – v0.80.x)

Driven by the findings in the v0.76.0 overall assessment
([plans/PLAN_OVERALL_ASSESSMENT_15.md](plans/PLAN_OVERALL_ASSESSMENT_15.md)).
The assessment found 1 critical, 5 HIGH, 13 MEDIUM, and 8 LOW findings across
correctness (TRUNCATE CDC captures the wrong WAL position, q12 CASE/IN-list DVM
drift excluded from churn tests, q20 correlated subquery O(delta×table) at
scale, placeholder source-coverage validation gap), data durability (multi-consumer
change-buffer cleanup has no explicit per-source lock around min-frontier plus
DELETE, IMMEDIATE mode SAVEPOINT coverage absent), performance (correlated scalar
subqueries in WHERE can be O(delta×table) at high scale factors, regex-only query
complexity classifier can misclassify complex forms, cost model queries recent
rows from `pgt_refresh_history` per stream table instead of using the precomputed
summary table, placeholder resolver cache uses a 64-bit hash key with no collision
guard), code quality (28 consecutive unused-import suppressions in SQL codegen
modules, too-many-arguments suppressions across create/alter paths, global
`#![allow(dead_code)]` weakens cleanup pressure, deprecated `consume_slot_changes()`
retained with a full count path), API ergonomics (parameter-heavy create/alter
surfaces, no first-class pause/resume verbs), test coverage (DVM aggregate/join/
window algebra property tests absent, SF-10 TPC-H breadth intentionally limited,
fuzz smoke too short, TRUNCATE LSN regression test missing, dbt adapter matrix
incomplete), security (dynamic SQL semgrep enforcement absent, RLS warning not
emitted on create, search_path tests should remain wired), observability (no DVM
fallback reason codes, no invalidation ring overflow threshold alert in
`health_check()`, cleanup backlog trends not exposed as metrics), documentation
(SQL reference drift risk, DVM support matrix missing), dependencies (CI gate docs
absent, cargo-deny review dates need attention), and upgrade safety (rollback
runbook less visible than forward upgrade, upgrade E2E cutoff policy not prominently
documented). This four-release arc resolves every finding before v1.0.

**v0.77.0 is the critical correctness gate for this arc.** It will not ship until
the TRUNCATE LSN bug is fixed and regression-tested, the q12 and q20 TPC-H
limitations have explicit fallback paths with reason codes, the multi-consumer
change-buffer cleanup invariant is proven or locked, and the DVM delta invariant
validator GUC is in place and running in the CI test tiers. These are correctness
and durability foundations that must be proven before v1.0.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.77.0](roadmap/v0.77.0.md) | Correctness Stop-the-Line & DVM Proof Infrastructure: fix TRUNCATE LSN (C-1, pg_current_wal_insert_lsn), TRUNCATE marker E2E regression test (T-4), q12 CASE/IN-list forced FULL fallback + minimized regression (C-3/DVM-1), q20 correlated subquery forced fallback with reason code (DVM-2/P-1), multi-consumer cleanup advisory lock or formal proof + overlap E2E (D-1), IMMEDIATE mode SAVEPOINT/rollback E2E tests (D-2), source-placeholder coverage assertion in codegen (C-2), `pg_trickle.validate_delta_invariants` GUC (DVM-3), DVM algebra property generators comparing DIFFERENTIAL vs FULL after every cycle covering aggregate/join/CASE/window (T-1), semgrep CI rule blocking unwrap/expect/panic outside test modules (C-4) | ✅ Released | Large | [Full details](roadmap/v0.77.0.md-full.md) |
| [v0.78.0](roadmap/v0.78.0.md) | DVM Engine Root-Cause Fixes + Scheduler Intelligence: root-cause fix for CASE/IN-list aggregate drift or definitive rejection path (DVM-1), correlated aggregate subquery pre-aggregation rewrite into CTEs joined once per refresh for safe patterns (DVM-2), FULL fallback with CORRELATED_SUBQUERY_DELTA_QUADRATIC reason for unsafe patterns (P-1), OpTree-based query complexity classifier replacing regex with parsed OpTree structure stored in catalog and compared in logs (P-2), cost model history lookups moved from per-ST pgt_refresh_history rows to precomputed rolling summary with batch lookup per scheduler tick (P-3), placeholder resolver cache collision guard storing canonical key with verification before automaton reuse (P-4), rotating SF-10 TPC-H subset with per-query EXPLAIN latency regression thresholds (T-2), nightly extended fuzz workflow at 300 seconds per target with corpus size tracking (T-3) | ✅ Released | Large | [Full details](roadmap/v0.78.0.md-full.md) |
| [v0.79.0](roadmap/v0.79.0.md) | Code Quality, API Ergonomics & Security: remove unused-import suppressions in src/refresh/codegen.rs and src/refresh/merge/mod.rs module-by-module (Q-1), convert internal create/alter API implementations to typed parameter structs eliminating too-many-arguments in business logic (Q-2), replace global #![allow(dead_code)] with narrower per-module allowances on pgrx/export boundaries (Q-3), remove or #[deprecated] consume_slot_changes() replacing with clearly named status function (Q-4), add SQL convenience helpers create_stream_table_fast_append_only/set_stream_table_refresh_policy/set_stream_table_storage_policy (A-1), add first-class pause_stream_table/resume_stream_table wrappers (A-2), add/strengthen semgrep CI rules for dynamic SQL distinguishing identifier/literal/OID boundaries (S-1), emit runtime WARNING when source has RLS enabled at create_stream_table time (S-2), CI test inspecting SECURITY DEFINER trigger functions for SET search_path (S-3), cleanup chaos test forcing three consecutive DELETE failures with alert and status verification (D-3), dbt adapter compatibility matrix with alter/drop/rebuild flow and version matrix tests (T-5) | ✅ Released | Large | [Full details](roadmap/v0.79.0.md-full.md) |
| [v0.80.0](roadmap/v0.80.0.md) | Operational Excellence, Documentation Completeness & Final v1.0 Gate: add DVM fallback/performance reason codes to refresh history and health output — CORRELATED_SUBQUERY_DELTA_QUADRATIC, CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK, REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN (O-1), add health_check() threshold alert when invalidation ring overflow count increases in recent time window (O-2), add cleanup backlog trend metrics integrated into pgt_metrics_summary (O-3), docs lint comparing #[pg_extern] exports with SQL_REFERENCE.md entries (DOC-1), create docs/DVM_SUPPORT_MATRIX.md with every query pattern, fallback behavior, IMMEDIATE restrictions, and known-unsupported forms including q12/q20 entries (DOC-2), operational rollback runbook (backup requirements, snapshot recommendation, restore path, why downgrades are unsafe) (U-1), document upgrade E2E cutoff policy prominently in CHANGELOG and release notes (U-2), CI gate documentation in CONTRIBUTING.md describing which workflows gate PRs (B-1), review-by dates on cargo-deny advisory suppressions and require cargo-deny in PR gates (B-2), fuzz test for DVM snapshot fingerprint cache stability under OpTree refactoring (P-5), document internal event trigger functions in ARCHITECTURE comments (A-3) | ✅ Released | Large | [Full details](roadmap/v0.80.0.md-full.md) |

### Assessment-16-Driven Scaling Arc (v0.81.x – v0.87.x)

Driven by the findings in the v0.80.0 vision audit
([plans/PLAN_OVERALL_ASSESSMENT_16.md](plans/PLAN_OVERALL_ASSESSMENT_16.md)).
The assessment identified 7 HIGH, 18 MEDIUM, and 12 LOW findings across
architecture (single-process compute ceiling, change buffer I/O amplification,
no state externalization), performance (no vectorized batch processing, SPI
overhead per refresh, no incremental MERGE), ergonomics (no declarative DDL,
no auto-schema-evolution, no configuration advisor), and observability (no
OpenTelemetry traces, no commit-to-visible latency metric). This seven-release
arc implements the "Blueprint for Infinite Scaling" — transitioning pg_trickle
from a world-class single-node extension to a distributed, auto-scaling IVM
system capable of running on a developer laptop or a Kubernetes cluster.

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|-------|--------------|
| [v0.81.0](roadmap/v0.81.0.md) | Observability, Self-Tuning & Quick Wins: commit-to-visible latency metric using pg_xact_commit_timestamp (QW-1), configuration advisor function pgtrickle.tune_recommendations() (QW-2), preview/dry-run mode pgtrickle.preview_stream_table() (QW-3), OpenTelemetry trace spans on scheduler_tick/refresh_cycle/delta_execute/merge_apply with OTLP export (QW-4), bounded LRU eviction on thread-local L0/L1 template caches (QW-5), DeltaOperator trait for extensible operator dispatch (QW-6), split config.rs into config/scheduler.rs + config/cdc.rs + config/dvm.rs + config/monitoring.rs (QW-7), self-healing circuit breaker with auto-remediation for OOM/lock-timeout/sustained-lag (QW-8), chunked MERGE for large deltas with configurable merge_batch_size GUC (QW-9), stream table presets ('real-time'/'batch'/'cost-optimized') (QW-10) | ✅ Released | Large | [Full details](roadmap/v0.81.0.md) |
| [v0.82.0](roadmap/v0.82.0.md) | External Worker Foundation: extract DVM + MERGE into standalone pg_trickle_worker binary connecting via tokio-postgres (MT-1), advisory-lock-based work claiming with heartbeat monitoring and automatic failover (MT-2), DiffContext decomposition into CdcContext/CacheContext/OptimizationContext (MT-7), pg_stat_pgtrickle virtual system view for per-ST cumulative statistics (MT-9), adaptive worker pool sizing based on CPU utilization and refresh queue depth (MT-10), worker registration table pgt_worker_registry with health monitoring | Planned | Large | [Full details](roadmap/v0.82.0.md) |
| [v0.83.0](roadmap/v0.83.0.md) | Performance Pipeline & CDC Extraction: pipelined refresh execution with portal-based streaming overlapping delta SQL and MERGE application (MT-3), external CDC consumer binary pg_trickle_cdc subscribing to logical replication with zero trigger overhead (MT-4), shared-memory change buffer ring for hot sources >10K writes/s with table-based overflow (MT-5), auto-schema-evolution via DDL event trigger for additive source changes (MT-6), pg_trickle.cdc_mode='external' GUC for zero-write-path-impact mode | Planned | Large | [Full details](roadmap/v0.83.0.md) |
| [v0.84.0](roadmap/v0.84.0.md) | Vectorized Compute & Adaptive Engine: vectorized aggregate path using Arrow-compatible columnar batches with SIMD operations for pure-aggregate STs (MT-8), incremental window function computation replacing partition-based recomputation with proper incremental algorithms for ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD (LT-7), cost-based operator scheduling reordering operators within delta queries based on estimated cardinality (LT-9), parallel delta computation fan-out for multi-source joins (PERF-O4) | Planned | Large | [Full details](roadmap/v0.84.0.md) |
| [v0.85.0](roadmap/v0.85.0.md) | Kubernetes-Native Deployment: Kubernetes operator with StreamTableCluster CRD managing worker Deployments and HPA policies (LT-1), external change log backend supporting Kafka/NATS JetStream/local WAL files as intermediate storage (LT-2), declarative SQL syntax CREATE STREAM TABLE name AS SELECT via ProcessUtility_hook (LT-6), CDC consumer StatefulSet with exactly-once delivery, metrics adapter exposing CDC lag as Kubernetes custom metric for HPA | Planned | Very Large | [Full details](roadmap/v0.85.0.md) |
| [v0.86.0](roadmap/v0.86.0.md) | Distributed Delta Computation: partitioned delta computation by source key ranges across multiple workers (LT-3), read-replica DVM execution running delta computation on streaming replicas and applying results to primary (LT-4), zero-copy change buffer protocol using shared memory and UNIX domain sockets for same-host transfer (LT-10), worker affinity and data locality optimization for partition-aligned scheduling | Planned | Very Large | [Full details](roadmap/v0.86.0.md) |
| [v0.87.0](roadmap/v0.87.0.md) | Enterprise Federation & State Management: multi-cluster federation coordinating stream tables across PostgreSQL clusters with global DAG and cross-cluster frontier synchronization (LT-5), state checkpointing to object storage (S3/GCS) for disaster recovery independent of pg_basebackup (LT-8), global priority queue and resource governor for multi-database clusters, zero-config adaptive mode detection (laptop/server/production/kubernetes) | Planned | Very Large | [Full details](roadmap/v0.87.0.md) |

### Toward v1.0

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v1.0.0](roadmap/v1.0.0.md-full.md) | Stable release — PostgreSQL 19, package registries, signed artifacts, SBOMs, zero breaking changes | Planned | Large | [Full details](roadmap/v1.0.0.md-full.md) |

### Beyond v1.0

| Version | Theme | Status | Scope | Full details |
|---------|-------|--------|------- |---------- |
| [v1.1.0](roadmap/v1.1.0.md-full.md) | PostgreSQL 17 support; WITH RECURSIVE … SEARCH/CYCLE clause; auto_explain integration hook | Planned | Medium | [Full details](roadmap/v1.1.0.md-full.md) |
| [v1.2.0](roadmap/v1.2.0.md-full.md) | PGlite proof of concept; pg_partman automated partition scheduling integration | Planned | Medium | [Full details](roadmap/v1.2.0.md-full.md) |
| [v1.3.0](roadmap/v1.3.0.md-full.md) | Core extraction (`pg_trickle_core`) | Planned | Large | [Full details](roadmap/v1.3.0.md-full.md) |
| [v1.4.0](roadmap/v1.4.0.md-full.md) | PGlite WASM extension | Planned | Medium | [Full details](roadmap/v1.4.0.md-full.md) |
| [v1.5.0](roadmap/v1.5.0.md-full.md) | PGlite reactive integration | Planned | Medium | [Full details](roadmap/v1.5.0.md-full.md) |

## How these versions fit together

```
v0.1.0   ─── Foundation: differential engine, CDC, scheduling, 1300+ tests
    │
v0.2–0.5 ─── TopK, IMMEDIATE mode, RLS, partitioned sources, parallel refresh
    │
v0.6–0.9 ─── Circular DAGs, watermarks, Prometheus, algebraic aggregates
    │
v0.10–14 ─── PgBouncer compat, 34× latency, partitioned outputs, tiered scheduling
    │
v0.15–19 ─── Nexmark, append-only fast path, cost model, security, packaging
    │
v0.20–23 ─── Self-monitoring, zero-crash guarantee, downstream CDC, TPC-H at scale
    │
v0.24–27 ─── Join correctness complete, thousands of STs, snapshot/PITR
    │
v0.28–29 ─── Reliable event messaging (outbox + inbox) + relay CLI
    │
v0.30    ─── Quality gate: correctness, stability, docs (required for 1.0)
    │
v0.31    ─── Scheduler intelligence and performance hot paths
    │
v0.32    ─── Citus: stable naming foundation (additive, safe for all users)
    │
v0.33    ─── Citus: distributed CDC and stream table support
    │
v0.35    ─── EC-01 fix, Citus chaos rig, reactive subscriptions, shadow-ST, relay hardening
    │
v0.36    ─── L0 cache, WAL backpressure, api split, temporal IVM, columnar, RowIdSchema
    │
v0.37    ─── Scheduler split, pgVectorMV, OpenTelemetry, pg_partman compat
    │
v0.38    ─── Correctness closeout and truthfulness: EC-01, RowIdSchema planning, backpressure, wake/docs repair
    │
v0.39    ─── Distributed hardening and diagnostics: Citus chaos, durable CDC hold, TPC-H explain artifacts, fuzzing
    │
v0.40    ─── Operator trust and maintainability: generated docs, alerting, drain proof, secret hygiene, unsafe gating
    │
v0.41    ─── DVM correctness: structural cache keys, placeholder safety, WAL transition guards
    │
v0.42    ─── Docs truthfulness + test quality: repair_stream_table, catalog generator, sleep removal, fuzz CI
    │
v0.43    ─── Performance tunability: deep-join GUCs, GROUP_RESCAN improvement, explain diagnostics, D+I CB refactor
    │
v0.44    ─── Security hardening: IVM search_path, SQL builder, RLS warnings, module decomposition
    │
v0.45    ─── Operational readiness: preflight, scalability, CI completeness, CNPG production
    │
v0.46    ─── Extract pg_tide: standalone outbox/inbox/relay → trickle-labs/pg-tide; attach_outbox() integration
    │
v0.47    ─── Embedding infrastructure: post-refresh actions, drift-based reindex, vector monitoring
    │
v0.48    ─── Complete embedding programme: sparse vectors, hybrid search, embedding_stream_table(), per-tenant ANN
    │
v0.49    ─── Test infrastructure hardening: concurrency sync overhaul, 10-module unit sweep, merge fuzz, DDL E2E, scheduler split
    │
v0.50    ─── Performance, security & ops hardening: SPI batching, dblink fix, CNPG drain hook, digest pinning, ring observability
    │
v0.51    ─── Citus chaos resilience & doc truth: chaos rig, deprecated GUC removal, pg_tide boundary, CTE strategy docs
    │
v0.52    ─── DVM hot-path perf: O(1) placeholder resolution, volatility cache, lazy DiffContext, O(1) LRU eviction
    │
v0.53    ─── Unit test depth: dag/scheduler/CDC/parser/config sweep, proptest extension, sleep removal
    │
v0.54    ─── DVM hardening: diff_node depth limit, DiffContext OOM cap, snapshot fingerprint cache, view inlining fixpoint, O(V+E) diamond detection
    │
v0.55    ─── Final pre-1.0 polish: configurable ring, module decomposition, serde_json NOTIFY, multi-column IN rewrite, DVM metrics, docs
    │
v0.56    ─── Documentation Foundation: GUC_CATALOG fix, ERRORS.md complete (44 variants), MENTAL_MODEL.md, LIMITATIONS.md, PERFORMANCE_CHEATSHEET.md
    │
v0.57    ─── Documentation Excellence: 4 new tutorials, P2/P3 polish, full 83-file consistency sweep
    │
v0.58    ─── Security & correctness hardening: ownership checks (outbox/publication APIs), NOT IN + NULL fix, recursive CTE depth guard, WAL decoder TOCTOU lock, DDL hook escalation
    │
v0.59    ─── Performance & observability: batched monitor SPI, query-hash cache, Arc<str> templates, WAL decoder Vec pre-alloc, CDC-lag percentiles, worker queue metrics, app_name BGW, backup docs
    │
v0.60    ─── Code quality, test coverage & CI: cdc.rs split, codegen decompose, refresh/CDC/hooks unit tests, idempotence proptest, sleep removal, WAL OID filter, partition-attach rebuild, path-filtered E2E on PRs
    │
v0.61    ─── DX, docs & pre-1.0 polish: health_check foreign-owner row, SQL_REFERENCE complete, snapshot secondary equality, cte_counter reset, outbox name fix, sublinks decompose, 3 ADRs, LATERAL docs
    │
v0.64    ─── DuckLake Phase 1: 3 tutorials + 2 blog posts + 2 containerised demos + named-user outreach (no extension code)
    │
v0.65    ─── DuckLake Phase 2: change-feed adapter, snapshot frontier, inlined-data CDC, row-ID plumbing, compaction safety
    │
v0.66    ─── DuckLake Phase 3a: Parquet delta export, DuckLake sink output mode, S3 upload, catalog writer, encryption
    │
v0.67    ─── DuckLake Phase 3b: view registration, snapshot provenance, pg-tide tutorial, tutorials & demos
    │
v0.68    ─── Assessment-13 sprint: fused refresh audit trail, change_buffer_durability wired, DuckLake timestamp fix, stale pool deleted
    │
v0.69    ─── DuckLake sink reliability: delivery state machine, view-on-ALTER, snapshot lock, qualified schema, sink metrics
    │
v0.70    ─── Scheduler/validator hardening: LATERAL validation, batched monitor, fused eligibility, prune GUC, work-mem cap, launcher cache
    │
v0.71    ─── CI truth + doc cleanup: fuzz all 9 targets, catalog generator rewrite, test harness schema generated, PLAN.md archived
    │
v0.72    ─── Frontier durability & catalog correctness: outbox OID fix, DUR-1 recovery wire-or-remove, WAL handoff gate, slot pristine guard
    │
v0.73    ─── Monitoring scalability & operational resilience: O(Δ) history table, batched cleanup, Aho-Corasick cache, persistent cleanup queue, launcher shmem
    │
v0.74    ─── Test coverage, CI integrity & security: path-filtered full E2E, per-module coverage, IVM search_path, unified advisory policy, re-enable benchmarks
    │
v0.75    ─── API polish & documentation excellence: metrics_summary reference, typed PgtId wrappers, IVM comparison matrix, stale-tag scanner
    │
v0.76.0  ─── Complete DuckLake integration removal: sink + source + CDC mode + catalog tables/columns + GUCs + deps
    │
v0.77    ─── Correctness stop-the-line: TRUNCATE LSN fix, q12 FULL fallback, q20 reason code, cleanup lock proof, IMMEDIATE SAVEPOINT tests, DVM property algebra, validate_delta_invariants GUC
    │
v0.78    ─── DVM root-cause fixes + scheduler intelligence: CASE/IN-list fix, correlated subquery pre-aggregation, OpTree complexity classifier, cost model summary table, cache collision guard, extended fuzz
    │
v0.79    ─── Code quality, API ergonomics & security: remove lint suppressions, typed param structs, dead_code narrowed, pause/resume, semgrep dynamic SQL, RLS warning, dbt matrix, cleanup chaos test
    │
v0.80    ─── Operational excellence & final v1.0 gate: DVM reason codes, ring overflow alert, backlog trends, pg_extern docs lint, DVM_SUPPORT_MATRIX, rollback runbook, upgrade cutoff docs, CI gate docs
    │
v0.81    ─── Observability, self-tuning & quick wins: OTel traces, commit-to-visible metric, config advisor, chunked MERGE, self-healing, presets, DeltaOperator trait
    │
v0.82    ─── External worker foundation: pg_trickle_worker binary, advisory lock coordination, adaptive pool sizing, pg_stat_pgtrickle view, DiffContext split
    │
v0.83    ─── Performance pipeline & CDC extraction: pipelined refresh, external CDC consumer, shared-memory ring buffers, auto-schema-evolution
    │
v0.84    ─── Vectorized compute & adaptive engine: Arrow columnar batches, incremental window functions, cost-based operator scheduling, parallel fan-out
    │
v0.85    ─── Kubernetes-native deployment: K8s operator + CRD, external change log (Kafka/NATS), declarative SQL syntax, HPA metrics adapter
    │
v0.86    ─── Distributed delta computation: partitioned deltas across workers, read-replica execution, zero-copy shared-memory protocol
    │
v0.87    ─── Enterprise federation & state management: multi-cluster federation, S3/GCS state checkpointing, global resource governor, zero-config adaptive mode
    │
v1.0.0   ─── Stable release, PostgreSQL 19, package registries, signed artifacts, SBOMs
```

v0.1.0 through v0.27.0 build the complete core engine and harden it for
production use. v0.28.0 and v0.29.0 deliver the event-driven integration
story. v0.30.0 is a mandatory correctness and polish gate before 1.0.
v0.31.0 sharpens scheduler intelligence before new features are added.
v0.32.0 is the first of two Citus releases, shipping stable object naming
and detection helpers as an additive, non-breaking foundation. v0.33.0
delivers the full Citus integration immediately after — per-worker slot CDC,
distributed ST placement, cross-node coordination, and the Citus test suite.
Pulling v0.33.0 forward means users with Citus topologies (including
billion-row all-distributed deployments) are unblocked two releases earlier.
v0.35.0 was intended to be the single most important release before v1.0, but
the v0.37.0 overall assessment shows several of those closeout items remain
partially open or insufficiently proven. v0.36.0 and v0.37.0 still delivered
substantial structural gains: L0 cache construction, temporal IVM,
`RowIdSchema`, scheduler and merge splits, pgVectorMV, and OpenTelemetry trace
capture. The next three releases now form a hardening programme rather than an
immediate feature expansion.

**v0.38.0 is a dedicated EC-01 correctness sprint with a hard release gate:**
This release will NOT ship until join phantom rows are proven closed with a
comprehensive DIFF-vs-FULL property test suite covering Q07/Q15-style joins.
EC-01 has been labeled critical since v0.20.0 (6+ releases) and deferred multiple
times; v0.38.0 breaks that pattern by making EC-01 closure the sole release
objective. No other features, no operational docs, no SQLSTATE rollout — just the
join phantom-row fix and its proof.

**v0.39.0 absorbs the operational truthfulness items** that were originally planned
for v0.38.0: backpressure hysteresis or deprecation, wake-truthfulness repair,
generated configuration and upgrade docs, OpenTelemetry collector proof, SQLSTATE
rollout on hot paths, and the full distributed/diagnostic coverage (Citus chaos
testing, durable CDC hold semantics, per-query TPC-H explain artifacts, SQLancer
light PR mode, targeted fuzzing, and inbox/outbox reliability tests).

**v0.40.0** then focuses on operator trust and maintainability: generated SQL/GUC
references, drain-mode proof, monitoring/alert rules, security-model and
secret-handling docs, upgrade-gate coverage, unsafe-inventory PR gating, and
continued decomposition of the largest files.

**v0.41.0 through v0.45.0 form a second hardening arc** driven by the findings
in the v0.40 overall assessment (plans/PLAN_OVERALL_ASSESSMENT_9.md). These
five releases systematically close every gap identified across 10 dimensions:
correctness (P0 cache-key and placeholder fixes), documentation truthfulness
(repair function implementation, catalog generator rewrite), test quality
(sleep removal, property tests, fuzz CI — merged into v0.42.0), performance
tunability (GUC-exposed thresholds, explain diagnostics), security
(search_path hardening, centralized SQL building), and operational readiness
(preflight functions, scalability infrastructure, CI completeness). Only after
this arc does the roadmap resume the embedding programme in v0.47.0–v0.48.0,
preserving the pgvector work while aligning the release order with the
assessment's conclusion that closing correctness and operational gaps matters
more than adding new surface area. The embedding programme itself is
consolidated into two releases: v0.47.0 for infrastructure and ANN maintenance,
and v0.48.0 completing the full feature set (sparse/half-precision aggregates,
hybrid search, the ergonomic `embedding_stream_table()` API, per-tenant ANN
patterns, and outbox-emitted embedding events). v0.46.0 precedes this arc
with the extraction of `pg_tide` — moving the outbox, inbox, and relay
subsystems into a standalone extension at `trickle-labs/pg-tide`.

**v0.49.0 through v0.51.0 form the v1.0 readiness arc**, driven by the findings
in the v0.48.0 overall assessment (plans/PLAN_OVERALL_ASSESSMENT_10.md). The
assessment confirmed that every P0 correctness issue from prior assessments is
closed — EC-01 phantom rows, snapshot cache-key weakness, placeholder resolution,
and WAL transition TOCTOU are all fixed. The project has transitioned from a
capability problem to a coverage confidence problem. These three releases
systematically close the remaining gaps across test reliability, performance,
security hardening, operational polish, and documentation truth before v1.0.

**v0.58.0 through v0.61.0 form the final assessment-driven hardening arc before
v1.0**, driven by the findings in the v0.57.0 overall assessment
(plans/PLAN_OVERALL_ASSESSMENT_12.md). The assessment found 0 critical findings,
4 HIGH severity issues (ownership-check bypass in the outbox and publication APIs,
recursive-CTE depth guard not applied in DIFFERENTIAL mode, multi-column NOT IN
with NULL row semantics, and per-source SPI fan-out in the monitor health check),
plus 23 MEDIUM and 20 LOW items spanning performance, observability, code quality,
test coverage, and documentation. v0.58.0 closes all HIGH findings as a hard gate.
v0.59.0 eliminates the performance and observability gaps. v0.60.0 completes the
code quality and test coverage sweep. v0.61.0 delivers the final developer-experience
and documentation polish, closing the last remaining items so that v1.0 is a clean,
fully verified stable release.

**v0.72.0 through v0.75.0 form the Assessment-14-Driven Hardening Arc**, driven
by the findings in the v0.71.0 overall assessment
(plans/PLAN_OVERALL_ASSESSMENT_14.md). The assessment found 0 critical, 6 HIGH,
28 MEDIUM, and 8 LOW findings. v0.72.0 is the hardest gate: it closes every
finding that threatens catalog correctness or durability invariants — the
outbox `stream_table_oid` schema mismatch, the dead DUR-1 tentative-frontier
code, the WAL transition handoff race, and the unenforced slot precondition.
v0.73.0 eliminates the scalability and reliability gaps introduced by
per-stream-table SPI fan-out patterns: the monitoring O(N×history) aggregation
is replaced with an incremental summary table, cleanup gains a persistent retry
queue with backpressure, Aho-Corasick automata are cached across refresh cycles,
and the launcher's in-memory-only state migrates into shared memory. v0.74.0
closes the test-coverage, CI-integrity, and security gaps: path-filtered full
E2E on risky PRs, advisory-policy consolidation, IVM trigger search-path
restriction, benchmark CI re-enablement, and a `just lint-ci` recipe that
surfaces every CI gate locally. v0.75.0 completes the arc with API polish,
documentation excellence, and the long-overdue IVM comparison matrix —
leaving v1.0 with nothing remaining but the package-registry and signature
infrastructure.
 All concurrency tests currently rely on
`sleep(50ms)` for synchronization, which provides false confidence: tests may
pass locally while missing real race conditions on slow CI runners or under
load. This release replaces sleep-based synchronization with `pg_locks`-polling
patterns throughout `tests/e2e_concurrent_tests.rs`. Alongside, ten source
modules that have zero `#[cfg(test)]` unit test coverage are systematically
addressed: `catalog.rs`, `template_cache.rs`, `ivm.rs`, `cdc/polling.rs`,
`cdc/rebuild.rs`, `diagnostics.rs`, `logging.rs`, `metrics_server.rs`, and
`otel.rs`. New fuzz targets are added for the refresh merge SQL codegen
(`src/refresh/merge/`) and row identity tracking (`src/dvm/row_id.rs`) — two
high-value surfaces with no current fuzz coverage. An E2E test for concurrent
DDL during active refresh (`ALTER STREAM TABLE` + in-flight refresh) is added.
The `src/scheduler/mod.rs` monolith (6,700+ lines) is decomposed into focused
submodule files: scheduling loop, parallel dispatch state, and cost model
each become separate files. The e2e-smoke CI filter is widened to cover join,
aggregate, and window operator regressions on every PR, and a consolidated
`just fuzz-all` recipe is added to the justfile.

**v0.50.0 targets performance, security, and operational hardening.** The
differential refresh hot path currently makes 3–4 separate SPI round-trips per
refresh cycle — buffer existence check, change count per source, and table row
estimate — that are consolidated into a single CTE query, saving 10–15ms per
multi-source refresh. The CDC trigger SQL generation loop is tightened using
`String::with_capacity()` to eliminate per-column heap allocations. The
watermark computation in the scheduler tick is consolidated into a single
compound query. On the security side, the `src/citus.rs` dblink calls that
use manual single-quote doubling for escaping are replaced with
`pg_escape_literal()` SPI calls for defense-in-depth. Operational gaps are
closed: the CNPG `cluster-production.yaml` gains a preStop lifecycle hook
that calls `pgtrickle.drain(timeout_s => 120)` before pod termination,
preventing interrupted in-flight refreshes during rolling upgrades. All Docker
base images are pinned to SHA256 digests for reproducible builds. The shared
memory invalidation ring capacity limit (1,024 entries) is documented in
`docs/CONFIGURATION.md` with a new `pg_trickle_invalidation_ring_overflow`
Prometheus counter. Two additional Prometheus metrics are added:
`pg_trickle_dag_cycles_detected` and `pg_trickle_cache_stale_evictions`.
The deep join chain Part 3 correction threshold GUC and its trade-off
between SQL complexity and correctness at >6 join tables is documented in the
configuration reference with an associated soak-test assertion.

**v0.51.0 closes the Citus resilience gap and brings documentation into full
truth** — the chaos test rig (node kill, rebalance, and network-partition
scenarios) proves that every Citus failure mode is handled, while deprecated
GUC removals, ARCHITECTURE.md boundary updates, recursive CTE strategy
documentation, and CDC-enabled-flag documentation eliminate the last
documentation inaccuracies identified in the v10 assessment.

**v0.52.0 through v0.55.0 form the final pre-1.0 hardening arc**, driven by
the findings in the v0.51.0 overall assessment
(plans/PLAN_OVERALL_ASSESSMENT_11.md). The assessment found no critical
issues, two HIGH findings (both performance-class), and 22 MEDIUM findings
across correctness, performance, scalability, test coverage, code quality,
security, and feature completeness. These four releases close every one of
them in priority order.

**v0.52.0 targets the two HIGH-severity performance gaps on the DVM hot
path.** `resolve_delta_template()` currently resolves LSN placeholders by
calling `.replace()` twice per source OID — an O(k×n) scan for k source
tables in a SQL string of length n. This is replaced with a single
`aho-corasick` pass that resolves all placeholders in one traversal, cutting
multi-source refresh latency proportionally. Alongside, `lookup_function_volatility()`
currently makes one SPI round-trip to `pg_proc` for every unknown function in
a query — up to 50 ms overhead for function-heavy queries. A thread-local
`HashMap<String, char>` cache pre-populated with all PostgreSQL built-in
functions eliminates these trips on the hot path. Two further allocator
improvements close the LOW-rated findings: `DiffContext::new()` switches from
12 unconditional `HashMap::new()` calls to `Option<HashMap>` with lazy
initialization (saving 5–10 µs per refresh for simple queries), and the
template cache eviction path is replaced with a proper LRU data structure for
O(1) eviction instead of O(N) scanning.

**v0.53.0 is the eleven-module unit test depth sweep.** Five source modules
that are responsible for core algorithmic logic — `dag.rs` (cycle detection,
topological sort, diamond detection, schedule resolution), the eight
`scheduler/` submodules (cost model, tier transitions, watermark computation),
`cdc.rs`/`cdc/polling.rs`/`cdc/rebuild.rs` (buffer naming, column escaping,
trigger SQL), all five `dvm/parser/` files (Expr::to_sql(), AggFunc
classification, strip_qualifier()), and `config.rs` (mode parsing, threshold
conversion) — have zero inline `#[cfg(test)]` unit tests and are only
exercised through full-stack E2E tests. This release adds focused
`#[cfg(test)]` modules to every one of them using mock structures that require
no PostgreSQL backend. `proptest!` coverage is extended to DAG cycle detection
and schedule resolution. The two remaining fixed-sleep tests in
`e2e_buffer_growth_tests.rs` (7s and 20s sleeps) are replaced with adaptive
`pg_locks`-polling helpers.

**v0.54.0 hardens the DVM engine against pathological queries and slow
parsing.** `diff_node()` gains a depth counter that errors on breach of
`max_parse_depth` (default 64), preventing stack overflow on extreme nesting.
`DiffContext` gains a configurable CTE count ceiling (default 1,000) that
returns a clean error before OOM can occur. The snapshot cache fingerprint is
computed and stored at `OpTree` construction time instead of re-traversing the
tree on every diff cycle, and `Expr::to_sql()` caches its result string to
eliminate redundant allocations. View inlining (`rewrite_views_inline()`) is
refactored to batch all `relkind` lookups into a single SPI query and use a
fixpoint check (no changes this iteration) instead of a hard counter, cutting
3-level view hierarchies from ~15 ms to a single parse + one SPI call. The
ST-to-ST frontier resolver is hardened to return `PgTrickleError::SourceNotFound`
instead of silently defaulting to `"0/0"` when a required source is missing.
Finally, diamond detection is reimplemented with a BFS-based visited-set merge
algorithm, reducing complexity from O(V²) pairwise comparisons to O(V+E)
— critical for deployments with 500+ stream tables.

**v0.55.0 delivers the final pre-1.0 polish pass** across scalability,
module structure, security, documentation, and one new SQL feature. The
shared-memory invalidation ring capacity (currently hardcoded at 1,024) becomes
a GUC with a default of 1,024 and a maximum of 4,096, preventing excessive full
DAG rebuilds in DDL-burst environments. `src/api/mod.rs` (7,600+ lines) is
decomposed into focused submodules (`api/create.rs`, `api/alter.rs`,
`api/refresh.rs`), and `src/monitor.rs` (4,000+ lines) is split into
`monitor/alert.rs`, `monitor/health.rs`, and `monitor/tree.rs`. NOTIFY alert
payloads are switched from manual string escaping to `serde_json::json!()`
to guarantee correct JSON for error messages containing backslashes or control
characters. The DVM parser gains automatic rewriting of `WHERE (a, b) IN
(SELECT x, y FROM ...)` multi-column row IN subqueries to equivalent
`EXISTS` form, closing the last user-visible SQL coverage gap. DVM parse
timing metrics (`pg_trickle_dvm_parse_ms`, `pg_trickle_delta_query_size_bytes`)
are added to the Prometheus metrics endpoint. The `__PGS_`/`__PGT_` reserved
column-name prefixes are documented in `docs/SQL_REFERENCE.md`, rationale
comments are added to all magic-number GUC defaults in `src/config.rs`, and
coverage reporting is added to the PR gate so regressions are visible before
merge.
truth.** The Citus distributed support shipped in v0.32–v0.34 has never had
a chaos test suite — there are zero tests validating behaviour under node
failure, shard rebalance, or network partition. This release delivers a
docker-compose-based chaos rig with three scenarios: coordinator restart,
worker node kill with automatic reconnect, and rolling shard rebalance during
active refresh. The deprecated `pg_trickle.event_driven_wake` GUC (non-functional
since background workers cannot use `LISTEN`) is removed entirely along with
all associated code paths and the runtime warning it emits. Documentation is
brought to full truth: `docs/ARCHITECTURE.md` is updated to clearly describe
the pg_tide boundary after v0.46.0 extraction; `docs/CONFIGURATION.md` gains
a deprecation header on the removed GUC entry; the recursive CTE strategy
selection heuristic (semi-naive vs. DRed vs. recomputation fallback) is
documented for the first time with an example EXPLAIN output; and a note is
added to `docs/CONFIGURATION.md` clarifying that CDC triggers fire even when
`pg_trickle.enabled = false` (by design, to keep buffers ready for re-enable).

**v0.77.0 through v0.80.0 form the Assessment-15-Driven Hardening Arc**,
the final hardening programme before v1.0, driven by the findings in the
v0.76.0 overall assessment (plans/PLAN_OVERALL_ASSESSMENT_15.md). The
assessment found one critical issue (TRUNCATE CDC captures the wrong WAL
position), five HIGH issues (q12 CASE/IN-list DVM drift, q20 O(delta×table)
correlated subquery scalability cliff, multi-consumer cleanup has no explicit
per-source lock, regex-only query complexity classifier, DVM algebra property
tests absent), thirteen MEDIUM issues, and eight LOW issues spanning code
quality, API ergonomics, security, observability, documentation, and upgrade
safety.

**v0.77.0 is the correctness hard gate.** It will not ship until the TRUNCATE
LSN bug is fixed and verified with an E2E regression, q12 drift has an
explicit fallback with a minimized regression test, q20's correlated subquery
cliff is surfaced with a reason code and fallback, multi-consumer cleanup
correctness is proven or locked with an overlap test, and the DVM algebra
property test suite is generating aggregate/join/CASE/window differential
workloads compared against FULL recomputation. The `validate_delta_invariants`
GUC and semgrep unwrap/panic CI rule are also required to ship this release.

**v0.78.0 delivers the DVM root-cause fixes** that v0.77.0 gates behind
fallbacks: the CASE/IN-list aggregate drift is resolved at the rewrite/delta
rule level, correlated aggregate subqueries are pre-aggregated into CTEs for
safe patterns, and the full O(Δ) treatment for q20-style queries is either
implemented or a clean rejection path is documented. The regex query
complexity classifier is replaced by an OpTree-based classifier stored in
the catalog, cost model lookups shift from per-ST history scanning to the
precomputed summary table with batched scheduler reads, and the placeholder
resolver cache gains a collision guard. SF-10 TPC-H gains a rotating subset
with per-query latency regression thresholds, and the fuzz budget is raised
to a nightly 300-second extended workflow.

**v0.79.0 sweeps code quality, API ergonomics, and security** — the
maintainability debt that accumulates between hardening arcs. Unused-import
suppressions in the SQL codegen modules are removed, internal API
implementations switch from many-argument functions to typed parameter
structs, the global `#![allow(dead_code)]` is replaced with narrower
per-boundary allowances, and `consume_slot_changes()` is removed. User-facing
convenience goes up: first-class `pause_stream_table()` / `resume_stream_table()`
wrappers are added, along with mode-preset helper functions for common stream
table configurations. Security enforcement tightens with semgrep CI rules for
dynamic SQL identifier boundaries, a runtime RLS warning on create, and an
automated SECURITY DEFINER search_path test. A cleanup chaos test and dbt
adapter matrix round out this release.

**v0.80.0 is the final v1.0 gate**, completing every remaining observability,
documentation, and operational reliability item. DVM fallback reason codes
surface in refresh history and `health_check()`, the invalidation ring gains
a threshold alert for overflow increases within a time window, and cleanup
backlog trends are integrated into `pgt_metrics_summary`. Documentation
receives its final pass: a `pg_extern` export lint guards against SQL reference
drift, and `docs/DVM_SUPPORT_MATRIX.md` consolidates every query pattern,
fallback behavior, and known limitation into a single user-facing document.
Operational runbooks for upgrade rollback and the upgrade E2E cutoff policy,
CI gate documentation for contributors, and cargo-deny advisory review-by
dates complete the pre-v1.0 checklist.

**v0.81.0 through v0.87.0 form the Assessment-16-Driven Scaling Arc**, driven
by the findings in the v0.80.0 vision audit
(plans/PLAN_OVERALL_ASSESSMENT_16.md). With correctness, documentation, and
operational readiness proven across 15 prior assessment arcs, the project is
architecturally sound for single-node deployment but structurally limited to
one PostgreSQL backend process. This arc implements the "Blueprint for Infinite
Scaling" — a backward-compatible transition from a world-class single-node
extension to a distributed, auto-scaling IVM system.

v0.81.0 delivers immediate observability and ergonomic wins: OpenTelemetry
trace spans across the refresh hot path, a commit-to-visible latency metric
for SLA compliance, a configuration advisor function that analyzes workloads
and suggests optimal GUC values, chunked MERGE to reduce peak memory on large
deltas, self-healing circuit breakers, and stream table presets for progressive
disclosure. v0.82.0 introduces the external worker foundation: a standalone
`pg_trickle_worker` binary that connects via tokio-postgres, claims work via
advisory locks, and computes deltas outside the PostgreSQL process. The
in-process scheduler becomes a coordinator that dispatches to external workers
when available. v0.83.0 eliminates write-path impact with an external CDC
consumer binary (`pg_trickle_cdc`) that subscribes to logical replication
without triggers, plus shared-memory ring buffers for hot sources and pipelined
refresh execution. v0.84.0 introduces vectorized compute using Arrow-compatible
columnar batches and proper incremental window function algorithms. v0.85.0
delivers Kubernetes-native deployment with a CRD-based operator, external
change log backends (Kafka/NATS), and declarative SQL syntax. v0.86.0 enables
distributed delta computation across workers with partition-aligned scheduling
and read-replica execution. v0.87.0 completes the arc with multi-cluster
federation, object-storage state checkpointing, and the zero-config adaptive
mode that detects deployment context (laptop/server/kubernetes) and configures
behavior accordingly.

Each phase is backward-compatible: a developer laptop continues running
pg_trickle as a single in-process extension with zero additional infrastructure.
A production Kubernetes cluster deploys the full distributed topology with
auto-scaling workers.

