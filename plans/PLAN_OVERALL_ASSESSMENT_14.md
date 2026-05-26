# pg_trickle - Overall Assessment Report
*Generated: 2026-05-26*
*Scope: Static source audit, documentation/CI audit, targeted command verification, and comparison against Overall Assessment 13*

## Executive Summary

pg_trickle is in substantially better shape than the previous assessment baseline. Several Assessment 13 findings are now fixed or guarded: `pg_trickle.change_buffer_durability` is wired and tested, DuckLake timestamp serialization and sink status/retry handling are present, generated catalog drift checks pass, generated test schema drift checks pass, version synchronization passes, docs truth checks pass, and `just lint` completes with zero warnings. The project is actively hardening, not merely accumulating roadmap text.

The remaining risks are concentrated in a few places. The highest correctness concern is frontier durability: the catalog contains a two-phase tentative-frontier design, but current scheduler paths still write frontiers after refresh using simple best-effort calls, and the tentative-frontier recovery functions are not called anywhere. A second high-value bug is the outbox catalog key: the public schema and docs say `stream_table_oid`, but the implementation stores `pgt_id::oid` and tests do not assert the column actually equals `pgt_relid`. That is a user-visible catalog contract mismatch.

The main scalability risks are query fan-out and per-stream-table aggregation: monitoring functions scan `pgt_refresh_history` through `LEFT JOIN LATERAL` per stream table, cleanup still performs multiple SPI queries per source OID, the holdback probe scans `pg_stat_activity`/`pg_prepared_xacts` every scheduler tick, and the launcher still performs separate database and activity scans. These are not immediate correctness failures, but they are exactly the kind of O(N) and O(N * history) patterns that keep a database extension from feeling world-class at thousands of stream tables.

The test and delivery story is strong at the fast tiers but still has residual risk: full E2E and TPC-H remain schedule/manual only, WAL CDC tests retain fixed stabilization sleeps, and critical outbox/frontier semantics are not directly covered. Dependency security policy also needs cleanup: plain `cargo audit` reports one vulnerability and two warnings, while CI suppresses advisories in workflow YAML and `deny.toml` tracks a different set.

## Checks Run

| Check | Result | Notes |
|---|---:|---|
| `just lint` | PASS | `cargo fmt --check`, `cargo clippy --all-targets --features pg18 -- -D warnings`, security-definer check, docs-lint |
| `cargo audit` | FAIL | Reports RUSTSEC-2023-0071 (`rsa` via `sqlx-mysql`) plus unmaintained `paste` and `serde_cbor` warnings |
| `python3 scripts/gen_catalogs.py --check` | PASS | 130 GUCs, 124 SQL functions, return-type quality gate passes |
| `python3 scripts/gen_plans_index.py --check` | PASS | `plans/INDEX.md` up to date |
| `python3 scripts/gen_test_schema.py --check` | PASS | test harness schema up to date |
| `./scripts/check_version_sync.sh` | PASS | version 0.71.0 synchronized |
| `python3 scripts/check_docs_truth.py` | PASS | 102 docs checked, no issues |

## Severity Summary Table

| Area | Critical | High | Medium | Low | Total |
|---|---:|---:|---:|---:|---:|
| Correctness & Data Integrity | 0 | 2 | 2 | 0 | 4 |
| Performance & Scalability | 0 | 1 | 5 | 1 | 7 |
| Reliability & Stability | 0 | 1 | 2 | 0 | 3 |
| Code Quality & Maintainability | 0 | 0 | 2 | 1 | 3 |
| API Ergonomics & User Experience | 0 | 1 | 3 | 1 | 5 |
| Test Coverage & Test Quality | 0 | 1 | 3 | 1 | 5 |
| Security | 0 | 0 | 2 | 1 | 3 |
| Architectural Gaps & Missing Features | 0 | 0 | 3 | 1 | 4 |
| Build, CI/CD & DevEx | 0 | 0 | 3 | 1 | 4 |
| Documentation Completeness & Accuracy | 0 | 0 | 3 | 1 | 4 |
| **Total** | **0** | **6** | **28** | **8** | **42** |

## Area 1: Correctness & Data Integrity

### Critical Findings

None confirmed in this pass. I did not find a direct, currently proven path that silently corrupts the primary stream-table contents under ordinary committed DML. The high findings below are still serious because they affect refresh frontiers and catalog truth.

### High Findings

#### COR-001: Scheduler refresh paths update frontiers best-effort after data changes

- **Severity:** High
- **Location:** `src/scheduler/mod.rs:3380-3525`, `src/api/refresh_ops.rs:688-714`, `src/catalog.rs:719-747`, `src/catalog.rs:783-880`
- **Description:** The scheduler refresh path calls `execute_full_refresh()` / `execute_differential_refresh()` and then stores the new frontier with `StreamTableMeta::store_frontier(...)`. On failure, the scheduler logs but continues returning `Ok((ins, del))`. Manual refresh uses `store_frontier_and_complete_refresh(...)` and propagates errors, but the scheduler hot path does not. The catalog contains `prepare_frontier()`, `finalize_frontier_and_complete_refresh()`, and `reconcile_tentative_frontiers()`, but `rg` found only definitions and no call sites.
- **Recommendation:** Make frontier persistence part of the same success boundary as refresh completion. Wire the DUR-1 tentative frontier API into scheduler and manual paths, or remove it and replace it with a simpler atomic transaction design. Treat frontier-store failure as refresh failure, not a log-only event. Add crash/restart tests that simulate MERGE success followed by catalog update failure.

#### COR-002: `pgt_outbox_config.stream_table_oid` stores `pgt_id::oid`, not the stream table OID

- **Severity:** High
- **Location:** `src/api/outbox.rs:61-76`, `src/api/outbox.rs:148-156`, `src/api/outbox.rs:189-191`, `src/api/outbox.rs:311-316`, `sql/archive/pg_trickle--0.71.0.sql:358-365`, `docs/SQL_REFERENCE.md:4659-4669`, `tests/e2e_outbox_tests.rs:89-128`
- **Description:** The catalog schema and SQL reference define `stream_table_oid OID` as the PostgreSQL OID of the stream table. The implementation inserts `Oid::from(meta.pgt_id as u32)` and all lookups use `pgt_id` cast to `oid`. The E2E tests assert only `stream_table_name` and `tide_outbox_name`, so the mismatch is invisible. This makes the catalog misleading for users and can produce incorrect joins against `pg_class`.
- **Recommendation:** Either change the column to `stream_table_pgt_id BIGINT` and migrate docs/tests, or store `meta.pgt_relid` as the schema promises. Add an E2E assertion that `pgt_outbox_config.stream_table_oid = pgt_stream_tables.pgt_relid` if the OID contract remains.

### Medium Findings

#### COR-003: WAL transition completion has no explicit source-table handoff gate

- **Severity:** Medium
- **Location:** `src/wal_decoder.rs:1285-1313`
- **Description:** `complete_wal_transition()` drops the CDC trigger first and then updates catalog mode to WAL. The code relies on the prior catch-up decision but does not show a source-table lock, trigger-side gate, or catalog state transition that makes the final handoff atomic with concurrent DML. A narrow race here can create duplicate or missing change-buffer rows depending on exact WAL/trigger timing.
- **Recommendation:** Complete transition under an advisory lock or table lock shared with trigger writes. Mark a handoff epoch, verify WAL caught up under that gate, then atomically switch catalog mode and drop the trigger. Add concurrent DML tests during transition completion.

#### COR-004: Replication slot creation depends on an unenforced pristine-transaction precondition

- **Severity:** Medium
- **Location:** `src/wal_decoder.rs:226-239`, `src/wal_decoder.rs:311-328`
- **Description:** `create_replication_slot_pristine()` documents that it must run in a transaction with no prior SPI/catalog access because `CreateInitDecodingContext` rejects XID-assigned transactions. There is no runtime check such as `GetCurrentTransactionIdIfAny()` before calling the C API.
- **Recommendation:** Add a guard that detects assigned XID and returns a descriptive `PgTrickleError` before slot creation. Add a unit/integration test that intentionally performs a SPI read before calling the slot creation path and verifies the actionable error.

### Low Findings

No low-only correctness findings recorded. The correctness gaps found in this pass deserve Medium or High priority.

## Area 2: Performance & Scalability

### Critical Findings

None confirmed.

### High Findings

#### PERF-001: Monitoring functions aggregate refresh history per stream table

- **Severity:** High
- **Location:** `src/monitor/mod.rs:43-120`, `src/api/metrics_ext.rs:27-88`
- **Description:** `st_refresh_stats()` and `metrics_summary()` aggregate `pgt_refresh_history` through `LEFT JOIN LATERAL` subqueries for each stream table. At high stream-table count and long retention, this becomes O(stream tables * history rows) work per call. These functions are natural dashboard targets, so polling them can amplify catalog load.
- **Recommendation:** Maintain an incremental summary table updated after each refresh, with bounded history windows for dashboard views. Keep the detailed history query available behind a time-range parameter.

### Medium Findings

#### PERF-002: Frontier cleanup still performs multiple SPI probes per source OID

- **Severity:** Medium
- **Location:** `src/refresh/codegen.rs:430-540`
- **Description:** `cleanup_change_buffers_by_frontier()` loops over source OIDs. For each one it checks table existence, computes min frontier, checks for stale rows, checks truncation eligibility, and deletes/truncates. A stream table with many sources or a DAG with many shared sources can turn each refresh into many SPI round-trips.
- **Recommendation:** Batch source cleanup into a single query that computes table existence, min frontier, and stale thresholds for all source OIDs. For partitioned buffers, batch metadata discovery before per-partition detach/drop.

#### PERF-003: The xmin holdback probe scans backend state every scheduler tick

- **Severity:** Medium
- **Location:** `src/scheduler/watermark.rs:38-92`, `src/cdc/mod.rs:2585-2624`, `docs/GUC_CATALOG.md:68`
- **Description:** In default `frontier_holdback_mode = xmin`, the coordinator tick calls `compute_safe_upper_bound()`, which scans `pg_stat_activity` and `pg_prepared_xacts`. This is now one compound query, which is good, but it remains O(active sessions) on every scheduler tick.
- **Recommendation:** Cache the safe upper bound for a short interval, expose cost/latency metrics for the probe, and consider a native helper that reads transaction state without repeated SQL catalog scans.

#### PERF-004: The launcher scans databases and active schedulers separately

- **Severity:** Medium
- **Location:** `src/scheduler/scheduler_loop.rs:138-180`, `src/scheduler/scheduler_loop.rs:210-278`
- **Description:** Each launcher loop performs one query over `pg_database` and one over `pg_stat_activity`, then keeps `last_attempt`/`had_scheduler` only in memory. The v0.70 install epoch logic reduces polling frequency, but large multi-database installations still pay repeated catalog scans and lose launcher memory state on restart.
- **Recommendation:** Combine discovery into one query where possible, cache stable database state in shared memory or a lightweight catalog table, and expose launcher scan duration/database count metrics.

#### PERF-005: Aho-Corasick placeholder automata are rebuilt on every delta-template resolution

- **Severity:** Medium
- **Location:** `src/dvm/mod.rs:265-310`
- **Description:** `resolve_delta_template()` builds the pattern list and constructs a new Aho-Corasick automaton for every refresh. Source OID sets are stable per template, so this repeats work in the hot path for complex stream tables.
- **Recommendation:** Cache the automaton and replacement pattern metadata with the cached delta template. Invalidate it with the same generation as the SQL template.

#### PERF-006: The template cache has entry count bounds but no byte-size cap

- **Severity:** Medium
- **Location:** `src/refresh/codegen.rs:90-115`
- **Description:** `pg_trickle.template_cache_max_entries = 0` maps to a practical capacity of 65,536 entries. Large generated SQL templates can consume significant backend memory before entry-count eviction becomes meaningful.
- **Recommendation:** Add byte-size accounting to template caches, expose current bytes in `cache_stats()`, and support a memory cap GUC independent of entry count.

### Low Findings

#### PERF-007: Per-stream-table scheduler state uses many separate maps

- **Severity:** Low
- **Location:** `src/scheduler/mod.rs:438-456`
- **Description:** Retry/backoff/drift/spill/backpressure state is kept in separate maps. This is not a correctness issue, but it has avoidable allocation and cache-locality overhead for thousands of stream tables.
- **Recommendation:** Consolidate into a single per-stream-table state struct keyed by `pgt_id`.

## Area 3: Reliability & Stability

### Critical Findings

None confirmed.

### High Findings

#### REL-001: Tentative-frontier recovery is dead code and contains invalid dynamic table SQL

- **Severity:** High
- **Location:** `src/catalog.rs:783-880`
- **Description:** `prepare_frontier()`, `finalize_frontier_and_complete_refresh()`, and `reconcile_tentative_frontiers()` document a crash-recovery design, but no call sites exist. The recovery query also attempts `FROM {change_schema}.changes_ || s.pgt_relid::text`, which is not valid SQL relation-name construction. This means the documented recovery mechanism is neither wired nor executable as written.
- **Recommendation:** Decide whether the DUR-1 design is still intended. If yes, implement dynamic buffer-table probing safely with generated SQL per relation or a helper view, call recovery on scheduler startup, and cover it with tests. If no, delete the dead functions and comments so operators are not given false assurance.

### Medium Findings

#### REL-002: Cleanup failures are visible only as warnings/debug logs, with no durable operator state

- **Severity:** Medium
- **Location:** `src/refresh/codegen.rs:348-415`, `src/refresh/codegen.rs:512-540`
- **Description:** Deferred cleanup logs warnings after repeated failures, and frontier cleanup logs failures at debug level. There is no persistent cleanup-failure table, no backoff state, no SQL function to list stuck buffers, and no hard backpressure when cleanup cannot keep up.
- **Recommendation:** Add a persistent `pgt_cleanup_status`/`pgt_cleanup_queue` table keyed by source OID, last error, attempt count, and next retry. Surface it through SQL and Prometheus, and add configurable backpressure when buffer size exceeds a safe threshold.

#### REL-003: Full E2E is schedule/manual only, leaving PRs dependent on smoke/light tiers

- **Severity:** Medium
- **Location:** `.github/workflows/ci.yml:276-340`
- **Description:** Full E2E runs only on schedule or manual dispatch, with PRs covered by lighter/smoke tiers. This is an intentional cost trade-off, but it means changes to DVM, CDC, or scheduler behavior can merge before the full Docker-based matrix exercises them.
- **Recommendation:** Add path-filtered full E2E slices for risky areas (`src/dvm/**`, `src/refresh/**`, `src/cdc/**`, `src/wal_decoder.rs`, `src/scheduler/**`) using cached GHCR builder images. Keep complete TPC-H nightly if PR cost is too high.

### Low Findings

No low-only reliability findings recorded.

## Area 4: Code Quality & Maintainability

### Critical Findings

None confirmed.

### High Findings

None confirmed. `just lint` passes, and the security-definer checker reports 22 locations checked with zero errors.

### Medium Findings

#### CODE-001: Dead recovery APIs hide correctness assumptions

- **Severity:** Medium
- **Location:** `src/catalog.rs:783-880`, `src/scheduler/mod.rs:3380-3525`
- **Description:** The codebase contains a polished two-phase frontier API and comments, but the live refresh path uses a different model. This creates a maintainability hazard: future changes may rely on the documented invariant even though it is not active.
- **Recommendation:** Convert the frontier flow into one explicit abstraction with tests. Remove stale comments or make them accurate.

#### CODE-002: Core refresh modules have limited local unit-test anchoring

- **Severity:** Medium
- **Location:** `src/refresh/merge/mod.rs`, `src/refresh/codegen.rs`, `src/api/metrics_ext.rs`
- **Description:** Static markers show no inline `#[cfg(test)]` blocks in these files, despite heavy SQL generation and hot-path logic. Some coverage exists elsewhere, but the most complex functions are harder to regression-test in isolation.
- **Recommendation:** Add pure Rust unit tests for SQL-builder fragments, cleanup batching decisions, frontier-update decisions, and metrics query construction. Keep E2E tests for behavior, but put invariants close to the code.

### Low Findings

#### CODE-003: `pgt_id` cast to `Oid` indicates a domain-type smell

- **Severity:** Low
- **Location:** `src/api/outbox.rs:61-76`, `src/api/outbox.rs:148-156`, `src/api/outbox.rs:311-316`
- **Description:** Casting `pgt_id` into `Oid` blurs two unrelated identifier domains. Even if retained for compatibility, this should be made explicit rather than hidden under a column named `stream_table_oid`.
- **Recommendation:** Introduce typed wrappers or helper functions for `PgtId` and `StreamTableOid` so cross-domain casts become hard to write accidentally.

## Area 5: API Ergonomics & User Experience

### Critical Findings

None confirmed.

### High Findings

#### API-001: Outbox catalog contract is misleading for users and tooling

- **Severity:** High
- **Location:** `sql/archive/pg_trickle--0.71.0.sql:358-365`, `docs/SQL_REFERENCE.md:4659-4669`, `src/api/outbox.rs:61-76`
- **Description:** A user reading the catalog docs will naturally join `pgt_outbox_config.stream_table_oid` to `pg_class.oid` or `pgt_stream_tables.pgt_relid`. The implementation instead stores the internal `pgt_id` cast to OID, so user queries return no rows or wrong rows.
- **Recommendation:** Fix the schema/data contract and add a migration note. If backward compatibility requires keeping the column, add a new correctly typed column and deprecate the old one.

### Medium Findings

#### API-002: `metrics_summary()` lacks a detailed SQL reference section

- **Severity:** Medium
- **Location:** `src/api/metrics_ext.rs:1-88`, `docs/SQL_API_CATALOG.md:74`, `docs/SQL_REFERENCE.md`
- **Description:** `metrics_summary()` appears in the generated API catalog but has no detailed section in `docs/SQL_REFERENCE.md`, unlike `cache_stats()` and `history_prune_status()`. Users do not get column descriptions, examples, or operational interpretation.
- **Recommendation:** Add a `pgtrickle.metrics_summary` section with signature, columns, example output, Grafana guidance, and caveats about history aggregation cost.

#### API-003: SQL function parameter naming is inconsistent

- **Severity:** Medium
- **Location:** `src/api/create.rs:27-55`, `src/api/outbox.rs:93-103`, `src/api/spec.rs:58-75`
- **Description:** Core functions use bare names (`name`, `query`), while newer APIs use `p_name`, `p_retention_hours`, or overload-specific names. This is minor in Rust but visible in named-argument SQL calls and generated docs.
- **Recommendation:** Document the convention for new SQL APIs. Prefer bare user-facing argument names, keep `p_` only for internal wrappers or where migration compatibility requires it.

#### API-004: Generated SQL API catalog exposes Rust return types for several SQL functions

- **Severity:** Medium
- **Location:** `docs/SQL_API_CATALOG.md:17-125`
- **Description:** The catalog quality gate now passes, but many rows still show return types like `Result<(), PgTrickleError>` or `pgrx::JsonB (nullable)`. That is useful to developers but not ideal as a SQL contract for operators.
- **Recommendation:** Convert generated return types into SQL-facing forms (`void`, `jsonb`, `SETOF record`, etc.) and keep Rust return metadata in a developer appendix if needed.

### Low Findings

#### API-005: Schedule-mode documentation can be more workflow-oriented

- **Severity:** Low
- **Location:** `docs/SQL_REFERENCE.md:200-230`
- **Description:** The SQL reference lists duration, cron, and `calculated`, but users would benefit from a single comparison table that explains which mode to choose and how IMMEDIATE ignores scheduler timing.
- **Recommendation:** Add a compact table for Duration / Cron / CALCULATED / IMMEDIATE with use case, example, and refresh trigger semantics.

## Area 6: Test Coverage & Test Quality

### Critical Findings

None confirmed.

### High Findings

#### TEST-001: Outbox tests do not assert the key catalog invariant

- **Severity:** High
- **Location:** `tests/e2e_outbox_tests.rs:89-128`, `src/api/outbox.rs:148-156`
- **Description:** Current outbox E2E tests verify that a row exists and that `tide_outbox_name` is correct, but they do not verify that `stream_table_oid` equals the stream table's real `pgt_relid`. This is why the pgt_id-as-OID bug survives.
- **Recommendation:** Add a test that joins `pgt_outbox_config.stream_table_oid` to `pgtrickle.pgt_stream_tables.pgt_relid` and fails on mismatch.

### Medium Findings

#### TEST-002: Fixed sleeps remain in timing-sensitive E2E tests

- **Severity:** Medium
- **Location:** `tests/e2e_wal_cdc_tests.rs:80`, `tests/e2e_wal_cdc_tests.rs:326`, `tests/e2e_wal_cdc_tests.rs:858-907`, `tests/e2e_safety_tests.rs:53-76`, `tests/e2e_safety_tests.rs:158`, `tests/common/mod.rs:196-215`
- **Description:** There are still many sleep calls in E2E suites. Some are polling helper intervals, which are fine, but WAL CDC and safety tests also include fixed stabilization sleeps before assertions or DDL. These can be flaky on slow CI and unnecessarily slow on fast machines.
- **Recommendation:** Replace fixed waits with condition-based helpers that poll catalog state, slot state, transition mode, or refresh history. Keep short sleeps only inside reusable poll loops.

#### TEST-003: Tentative-frontier recovery has no call-site or coverage

- **Severity:** Medium
- **Location:** `src/catalog.rs:804-880`, `tests/`
- **Description:** `prepare_frontier()`, `finalize_frontier_and_complete_refresh()`, and `reconcile_tentative_frontiers()` are not referenced by production code or tests. This leaves the intended crash-recovery invariant untested.
- **Recommendation:** Add recovery tests that create a tentative frontier, simulate buffer empty/non-empty states, run reconciliation, and verify frontier promotion/discard behavior.

#### TEST-004: TPC-H and full E2E remain outside PR gating

- **Severity:** Medium
- **Location:** `.github/workflows/ci.yml:276-340`, `tests/e2e_tpch_tests.rs`
- **Description:** The fast test matrix is broad, but the highest-risk SQL coverage remains scheduled/manual. This is reasonable for cost, but the risk should be made explicit in PR policy.
- **Recommendation:** Gate risky path changes with a reduced TPC-H subset (join-heavy, subquery-heavy, aggregate-heavy) and run the full matrix nightly.

### Low Findings

#### TEST-005: Coverage signal is not summarized by module in normal developer output

- **Severity:** Low
- **Location:** `codecov.yml`, `.github/workflows/coverage.yml`, `src/refresh/merge/mod.rs`, `src/refresh/codegen.rs`
- **Description:** The project has coverage infrastructure, but local developer commands do not surface a concise module risk summary after `just test-unit`/`just lint`.
- **Recommendation:** Add a `just coverage-summary` recipe or CI artifact comment that lists coverage for high-risk modules (`dvm`, `refresh`, `cdc`, `scheduler`, `api`).

## Area 7: Security

### Critical Findings

None confirmed.

### High Findings

None confirmed. The explicit security-definer checker passes, and ownership checks are present in the inspected outbox API.

### Medium Findings

#### SEC-001: Plain `cargo audit` fails while CI suppresses advisories elsewhere

- **Severity:** Medium
- **Location:** `.github/workflows/security.yml:32-80`, `deny.toml:13-67`, `Cargo.lock`
- **Description:** Running plain `cargo audit` reports RUSTSEC-2023-0071 (`rsa` 0.9.10 via `sqlx-mysql`) as a vulnerability, plus unmaintained `paste` and `serde_cbor` warnings. CI ignores RUSTSEC-2023-0071 in workflow YAML, while `deny.toml` tracks a different ignore set. This split makes local and CI security posture disagree.
- **Recommendation:** Centralize advisory suppressions in `deny.toml` or a single policy file, include reachability rationale and review dates for every ignored advisory, and make local `just security` reproduce CI behavior.

#### SEC-002: IMMEDIATE-mode AFTER trigger functions include `public` in `SECURITY DEFINER` search path

- **Severity:** Medium
- **Location:** `src/ivm.rs:315-406`, `docs/SQL_REFERENCE.md:2912-2917`
- **Description:** The code intentionally includes `public` so user delta SQL can resolve unqualified source references. The trigger bodies call schema-qualified pgtrickle functions, but user SQL resolution under a security-definer function remains a sharp edge.
- **Recommendation:** Prefer schema-qualifying user query objects during create/alter time, then keep trigger search paths restricted to `pgtrickle_changes, pgtrickle, pg_catalog, pg_temp`. If `public` must remain, add targeted tests for search_path shadowing and document the exact risk trade-off.

### Low Findings

#### SEC-003: Some dynamic SQL still relies on internal identifier provenance

- **Severity:** Low
- **Location:** `src/refresh/codegen.rs:250-345`, `src/refresh/codegen.rs:430-540`
- **Description:** Cleanup SQL interpolates schema and generated buffer names directly. The values are internal and currently low risk, but central SQL-builder helpers exist and should be used consistently.
- **Recommendation:** Route all identifier construction through the shared SQL builder/quoting helpers and add lints for new dynamic `format!()` SQL in SQL-facing paths.

## Area 8: Architectural Gaps & Missing Features

### Critical Findings

None confirmed.

### High Findings

None recorded as purely architectural; the most severe architecture-adjacent items are tracked under correctness and reliability.

### Medium Findings

#### ARCH-001: Frontier durability has two competing designs

- **Severity:** Medium
- **Location:** `src/catalog.rs:719-880`, `src/scheduler/mod.rs:3380-3525`
- **Description:** The codebase has a single-call frontier completion path, simple `store_frontier()` calls, and a dead tentative-frontier path. This fragments the durability model.
- **Recommendation:** Choose one canonical frontier state machine and document it in `docs/ARCHITECTURE.md` plus an ADR.

#### ARCH-002: Cleanup has no persistent queue/backpressure model

- **Severity:** Medium
- **Location:** `src/refresh/codegen.rs:141-148`, `src/refresh/codegen.rs:230-415`, `src/refresh/codegen.rs:430-540`
- **Description:** Deferred cleanup is thread-local, with a frontier-based compensating pass on the next refresh. That prevents total loss of cleanup work, but there is no durable queue, no retry schedule, and no write-side backpressure for runaway buffers.
- **Recommendation:** Add a persistent cleanup/backpressure model before claiming world-class sustained throughput under failure.

#### ARCH-003: Multi-database launcher state is memory-only

- **Severity:** Medium
- **Location:** `src/scheduler/scheduler_loop.rs:116-135`, `src/scheduler/scheduler_loop.rs:210-278`
- **Description:** `last_attempt` and `had_scheduler` reset on launcher restart. That is acceptable for small installations, but creates unnecessary re-probe delay and catalog churn at scale.
- **Recommendation:** Persist minimal launcher state in shared memory or a cluster-local catalog table and expose launcher health metrics.

### Low Findings

#### ARCH-004: Comparison material for state-of-the-art IVM systems is not centralized

- **Severity:** Low
- **Location:** `docs/`, `plans/`, `ROADMAP.md`
- **Description:** The project references pg_ivm, Materialize, DuckLake, Citus, and DBSP concepts across many docs, but there is no current comparison matrix for feature completeness and differentiator gaps.
- **Recommendation:** Add a living `docs/COMPARISONS.md` covering pg_ivm, Materialize, Feldera, DuckDB/DuckLake, and pg_trickle across SQL coverage, consistency, CDC, performance, and operational model.

## Area 9: Build, CI/CD & DevEx

### Critical Findings

None confirmed.

### High Findings

None confirmed. Version sync, generated schema, generated catalog, docs truth, and lint checks all passed in this pass.

### Medium Findings

#### DEVEX-001: Benchmark workflow is manual-only

- **Severity:** Medium
- **Location:** `.github/workflows/benchmarks.yml:1-20`
- **Description:** The benchmark workflow says it tracks Criterion benchmarks with Bencher, but the workflow is explicitly disabled except for `workflow_dispatch`.
- **Recommendation:** Re-enable push-to-main benchmark baseline tracking. For PRs, use a path-filtered or sampled regression gate so performance-sensitive changes get signal without making every PR slow.

#### DEVEX-002: Local `just lint` is narrower than CI's generated-doc/version gates

- **Severity:** Medium
- **Location:** `justfile:42-70`, `.github/workflows/ci.yml:390-406`, `.github/workflows/docs-drift.yml:1-45`
- **Description:** `just lint` runs formatting, clippy, security-definer, and stale-term docs lint. CI additionally runs version sync, SQL reference generation, generated test schema check, generated catalog check, plan index check, and docs truth. Developers can pass local lint and still fail CI.
- **Recommendation:** Add `just lint-ci` or extend `just lint` with fast generated checks. At minimum, document the complete local pre-PR command sequence.

#### DEVEX-003: Docker and justfile examples contain stale version tags

- **Severity:** Medium
- **Location:** `justfile:21`, `Dockerfile.hub:13-23`, `Dockerfile.ghcr:13-17`, `README.md:558`
- **Description:** Version sync validates Dockerfile `ARG VERSION=0.71.0`, but examples still mention older tags (`0.11.0`, `0.13.0`, and `just build-hub` tags `0.19.0-pg18`). These examples are easy to copy.
- **Recommendation:** Replace hard-coded example versions with `<version>` or current generated version substitution. Add stale-version scanning to docs lint.

### Low Findings

#### DEVEX-004: Advisory policy is split between workflow YAML and `deny.toml`

- **Severity:** Low
- **Location:** `.github/workflows/security.yml:32-80`, `deny.toml:13-67`, `.github/workflows/dependency-policy.yml:37-64`
- **Description:** `cargo audit` ignores are embedded in the security workflow, while `cargo-deny` ignores live in `deny.toml` with review metadata. This is maintainable today but easy to drift.
- **Recommendation:** Generate both CI configurations from one advisory policy file, or call `cargo audit` with the same ignore metadata used by `cargo-deny`.

## Area 10: Documentation Completeness & Accuracy

### Critical Findings

None confirmed.

### High Findings

None confirmed. Generated catalogs, docs truth, and plan index checks passed.

### Medium Findings

#### DOC-001: `plans/PLAN.md` active index is visibly corrupted

- **Severity:** Medium
- **Location:** `plans/PLAN.md:19-24`
- **Description:** The key architecture-doc table contains duplicated fragments and corrupted text such as repeated `docs/COST_MODEL.md` fragments and broken `LIMITATIONS` text. This is an active entrypoint file, so corruption here undermines trust in planning docs.
- **Recommendation:** Regenerate or manually repair `plans/PLAN.md`, then add a docs lint that catches repeated broken-link fragments or non-word corruption in small index files.

#### DOC-002: README GUC count is stale

- **Severity:** Medium
- **Location:** `README.md:558`, `docs/GUC_CATALOG.md:7`
- **Description:** README says the GUC catalog contains all 115 configuration parameters, while the generated catalog reports 130. This is a small but visible truthfulness issue on the project front page.
- **Recommendation:** Replace hard-coded counts with phrasing such as "all generated configuration parameters", or add a generated badge/snippet for the count.

#### DOC-003: `metrics_summary()` is missing from detailed SQL reference

- **Severity:** Medium
- **Location:** `docs/SQL_REFERENCE.md`, `docs/SQL_API_CATALOG.md:74`, `src/api/metrics_ext.rs:1-88`
- **Description:** The function is generated into the API catalog but lacks a detailed SQL reference section. Operators using dashboards need the column meanings and cost caveats.
- **Recommendation:** Add a full section adjacent to `cache_stats()` and `history_prune_status()`.

### Low Findings

#### DOC-004: Dockerfile examples use stale release numbers

- **Severity:** Low
- **Location:** `Dockerfile.hub:13-23`, `Dockerfile.ghcr:13-17`
- **Description:** Comments show old example tags. The build ARGs are current, but comments are still copy/paste hazards.
- **Recommendation:** Use `<version>` placeholders or update examples as part of release automation.

## Prioritized Action Plan

| Rank | Item | Area | Impact | Effort |
|---:|---|---|---:|---:|
| 1 | Fix `pgt_outbox_config.stream_table_oid` to store real `pgt_relid`, or rename/migrate it to `stream_table_pgt_id` | Correctness/API | Very high | Medium |
| 2 | Wire or remove the tentative-frontier recovery design; make frontier persistence part of refresh success | Correctness/Reliability | Very high | Large |
| 3 | Add tests for outbox catalog OID semantics and tentative-frontier recovery | Tests | High | Medium |
| 4 | Rework `st_refresh_stats()` / `metrics_summary()` around incremental summaries | Performance | High | Large |
| 5 | Add a durable cleanup status/queue and buffer backpressure policy | Reliability/Architecture | High | Large |
| 6 | Gate WAL transition completion with an explicit lock/epoch and concurrent DML tests | Correctness | High | Medium |
| 7 | Batch frontier cleanup SPI queries across source OIDs | Performance | Medium-high | Medium |
| 8 | Centralize dependency advisory ignores and make local security checks match CI | Security/DevEx | Medium-high | Small |
| 9 | Add path-filtered full E2E/TPC-H slices for risky PRs | CI/Test | Medium-high | Medium |
| 10 | Re-enable push-to-main benchmark baselines | CI/Performance | Medium | Small |
| 11 | Cache placeholder replacement automata with delta templates | Performance | Medium | Medium |
| 12 | Add a byte-size cap and observability for template caches | Performance | Medium | Medium |
| 13 | Replace fixed WAL/safety sleeps with condition-based test waits | Test Quality | Medium | Medium |
| 14 | Repair `plans/PLAN.md` corruption | Docs | Medium | Small |
| 15 | Document `metrics_summary()` in SQL reference | Docs/API | Medium | Small |
| 16 | Update README GUC count and stale Docker/just examples | Docs/DevEx | Medium | Small |
| 17 | Add runtime check for pristine transaction before logical slot creation | Reliability | Medium | Small |
| 18 | Consolidate launcher state and expose launcher metrics | Scalability | Medium | Medium |
| 19 | Add local `just lint-ci` for generated docs/schema/version/docs-truth checks | DevEx | Medium | Small |
| 20 | Add `docs/COMPARISONS.md` for state-of-the-art IVM feature comparison | Architecture/Product | Low-medium | Medium |

## Positive Signals To Preserve

- `just lint` passes with zero warnings.
- Generated GUC and SQL API catalogs pass quality checks.
- `plans/INDEX.md`, generated test schema, version sync, and docs-truth checks pass.
- `pg_trickle.change_buffer_durability` appears fixed and tested in v0.68+.
- DuckLake timestamp roundtrip and sink status/retry handling appear fixed and tested.
- Security-definer checking is wired into `just lint` and currently passes.
- `fuzz-all` now accumulates failures instead of masking them, with an explicit best-effort variant.
- Advisory ignores in `deny.toml` include review metadata and expiry checking.

## Appendix: Files Audited

### Orientation and Planning

- `AGENTS.md`
- `docs/ARCHITECTURE.md`
- `docs/SQL_REFERENCE.md`
- `docs/CONFIGURATION.md`
- `docs/GUC_CATALOG.md`
- `docs/SQL_API_CATALOG.md`
- `docs/ERRORS.md`
- `ROADMAP.md`
- `plans/PLAN.md`
- `plans/PLAN_OVERALL_ASSESSMENT_13.md`
- `plans/adrs/PLAN_ADRS.md`
- `INSTALL.md`
- `CHANGELOG.md`
- `README.md`

### Source Files

- `src/api/create.rs`
- `src/api/outbox.rs`
- `src/api/spec.rs`
- `src/api/metrics_ext.rs`
- `src/api/refresh_ops.rs`
- `src/api/alter.rs`
- `src/api/helpers.rs`
- `src/catalog.rs`
- `src/cdc/mod.rs`
- `src/cdc/polling.rs`
- `src/cdc/triggers.rs`
- `src/dvm/mod.rs`
- `src/dvm/diff.rs`
- `src/dvm/operators/aggregate.rs`
- `src/dvm/operators/join.rs`
- `src/dvm/operators/scan.rs`
- `src/dvm/parser/mod.rs`
- `src/dvm/parser/validation.rs`
- `src/ducklake_sink.rs`
- `src/ivm.rs`
- `src/monitor/mod.rs`
- `src/refresh/codegen.rs`
- `src/refresh/merge/mod.rs`
- `src/scheduler/mod.rs`
- `src/scheduler/scheduler_loop.rs`
- `src/scheduler/watermark.rs`
- `src/wal_decoder.rs`
- `src/config.rs`
- `src/shmem.rs`

### Tests, CI, Build, and Scripts

- `tests/e2e_outbox_tests.rs`
- `tests/e2e_wal_cdc_tests.rs`
- `tests/e2e_safety_tests.rs`
- `tests/e2e_ducklake_tests.rs`
- `tests/e2e_tpch_tests.rs`
- `tests/common/mod.rs`
- `tests/generated/schema.rs`
- `.github/workflows/ci.yml`
- `.github/workflows/docs-drift.yml`
- `.github/workflows/benchmarks.yml`
- `.github/workflows/security.yml`
- `.github/workflows/dependency-policy.yml`
- `justfile`
- `build.rs`
- `scripts/gen_catalogs.py`
- `scripts/gen_plans_index.py`
- `scripts/gen_test_schema.py`
- `scripts/check_docs_truth.py`
- `scripts/check_version_sync.sh`
- `scripts/check_deny_expiry.py`
- `Dockerfile.hub`
- `Dockerfile.ghcr`
- `deny.toml`
- `Cargo.toml`
- `Cargo.lock`

### Repository Memory Consulted

- `/memories/repo/dvm-correctness-audit.md`
- `/memories/repo/join-delta-phantom-rows.md`
- `/memories/repo/tpch-dvm-scaling-analysis.md`
