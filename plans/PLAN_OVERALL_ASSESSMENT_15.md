# pg_trickle Overall Assessment 15

Date: 2026-05-28
Scope: Read-only deep assessment of correctness, durability, performance, scalability, code quality, API ergonomics, security, operations, documentation, dependencies, DVM correctness, tests, and upgrade safety.

## Executive Summary

pg_trickle is already a serious, high-ambition PostgreSQL extension. The project has strong architectural separation, a broad SQL-facing feature set, mature test infrastructure, meaningful observability, and unusually good upgrade discipline for a fast-moving extension. The current codebase is much closer to a production system than to a prototype: it has explicit frontier tracking, trigger-based CDC, stream-to-stream dependency machinery, DVM operator decomposition, upgrade completeness checks, TPC-H coverage, fuzz targets, and health diagnostics.

The biggest remaining risks are concentrated in three areas. First, there is one concrete correctness bug in TRUNCATE CDC capture: the TRUNCATE marker uses `pg_current_wal_lsn()` even though the surrounding code explicitly requires `pg_current_wal_insert_lsn()` for captured change LSNs. Second, the DVM still has known performance and correctness gaps around complex TPC-H-style subqueries: q20 is skipped at SF-10 because correlated scalar subqueries in WHERE become O(delta x table), and q12 is excluded from sustained churn because of known DVM drift. Third, several scalability paths still rely on per-stream-table SPI history queries, regex query complexity classification, and broad SQL code-generation modules with many lint suppressions.

Overall rating: strong and improving, but not yet world-class across all dimensions. Correctness and durability look robust for the common INSERT/UPDATE/DELETE differential path, but v1.0 readiness should require fixing the TRUNCATE LSN bug, turning the q12/q20 TPC-H limitations into first-class roadmap items, hardening multi-consumer cleanup concurrency with tests, and shrinking the remaining high-complexity code surfaces.

## Top 20 Most Important Issues

1. CRITICAL - TRUNCATE CDC marker captures the wrong WAL position. See Correctness C-1.
2. HIGH - TPC-H q12 has known DVM drift and is excluded from sustained churn. See DVM DVM-1.
3. HIGH - TPC-H q20 is skipped at SF-10 due to O(delta x table) correlated subquery delta SQL. See Performance P-1 and DVM DVM-2.
4. HIGH - Change-buffer cleanup has no explicit per-source cleanup lock around min-frontier computation plus DELETE/TRUNCATE. See Durability D-1.
5. HIGH - Regex-only query complexity classification can make poor AUTO-mode decisions for nested joins, comments, LATERAL, or subqueries. See Performance P-2.
6. HIGH - DVM complex-query coverage is still partly example-driven; property tests do not cover aggregate/join/window delta algebra. See Test Coverage T-1.
7. MEDIUM - Cost model still queries recent rows from `pgt_refresh_history` per ST instead of using the existing summary table for crossover decisions. See Performance P-3.
8. MEDIUM - TPC-H SF-10 intentionally validates only one differential correctness test because full coverage would take too long. See Test Coverage T-2.
9. MEDIUM - Fuzz smoke duration is too short for parser and SQL code-generation confidence. See Test Coverage T-3.
10. MEDIUM - Placeholder resolver cache uses a 64-bit hash key without a collision guard on cached automata. See Performance P-4.
11. MEDIUM - `resolve_delta_template()` validates unresolved tokens but does not separately assert that every expected source placeholder occurred in the template. See Correctness C-2.
12. MEDIUM - Public SQL APIs carry many optional arguments and rely on `#[allow(clippy::too_many_arguments)]`, making extension ergonomics and implementation testing harder. See API A-1 and Code Quality Q-2.
13. MEDIUM - Large SQL-generation modules suppress many unused imports, masking stale decomposition boundaries. See Code Quality Q-1.
14. MEDIUM - Global `#![allow(dead_code)]` weakens the value of dead-code cleanup in production modules. See Code Quality Q-3.
15. MEDIUM - `consume_slot_changes()` is retained as dead diagnostic code and performs a full count if used. See Code Quality Q-4.
16. MEDIUM - Manual dynamic SQL remains widespread; safe helpers exist, but semgrep/CI rules should enforce identifier/value boundaries continuously. See Security S-1.
17. MEDIUM - IMMEDIATE-mode SAVEPOINT / partial rollback behavior needs explicit E2E coverage. See Durability D-2 and Test Coverage T-4.
18. LOW - Upgrade rollback strategy is not documented as clearly as upgrade-forward safety. See Upgrade U-1.
19. LOW - dbt coverage is much better than a minimal smoke test, but still lacks broader incremental semantics and adapter matrix coverage. See Test Coverage T-5.
20. LOW - Documentation is generally strong, but advanced feature docs should keep being generated or checked from SQL exports to avoid drift. See Documentation DOC-1.

## 1. Correctness & Bug Analysis

Summary: Core INSERT/UPDATE/DELETE CDC and many DVM operator paths are well covered, but TRUNCATE handling has a concrete LSN bug, and some placeholder and complex-query validation should be tightened.

### C-1 - TRUNCATE trigger violates CDC LSN contract

- Severity: CRITICAL
- Reference: [src/cdc/mod.rs](../src/cdc/mod.rs#L191), [src/cdc/mod.rs](../src/cdc/mod.rs#L320)
- Problem: `create_cdc_trigger()` documents that all builders must use `pg_current_wal_insert_lsn()` rather than `pg_current_wal_lsn()` because the write position can lag inside an uncommitted transaction. The TRUNCATE trigger then inserts `(pg_current_wal_lsn(), 'T')` into the change buffer.
- Risk: A TRUNCATE marker can be captured at a stale LSN. Because refresh windows are LSN bounded, this can produce a silent no-op or wrong ordering around TRUNCATE plus subsequent writes in the same scheduling window.
- Recommended fix: Change the TRUNCATE marker to `pg_current_wal_insert_lsn()`. Add a regression test that inspects the generated trigger function body and an E2E transaction that performs TRUNCATE plus post-TRUNCATE INSERTs before refresh.

### C-2 - Placeholder validation is token-based, not source-coverage-based

- Severity: MEDIUM
- Reference: [src/dvm/mod.rs](../src/dvm/mod.rs#L156), [src/dvm/mod.rs](../src/dvm/mod.rs#L207), [src/dvm/mod.rs](../src/dvm/mod.rs#L285)
- Problem: `check_no_remaining_placeholders()` verifies that unresolved `__PGS_*__` / `__PGT_*__` tokens are gone after replacement. It does not independently verify that the final template contained every source OID expected by the parsed dependency set.
- Risk: If code generation accidentally omits a source placeholder, the final SQL can pass token validation while using an incomplete LSN window. This is less likely than an unresolved token bug, but the failure mode is correctness-sensitive.
- Recommended fix: Track expected placeholder families during codegen and assert every source OID has both previous/new LSN placeholders or a documented exemption. Add unit tests for omitted-source placeholders.

### C-3 - TPC-H q12 drift is documented in test comments but not promoted to a first-class correctness issue

- Severity: HIGH
- Reference: [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L1939), [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L1946)
- Problem: Sustained TPC-H churn excludes q12 with the comment "known DVM drift (CASE WHEN IN-list produces non-deterministic incremental results)." This is exactly the kind of multi-operator correctness gap the DVM should make visible in release planning.
- Risk: Similar CASE/IN-list aggregate patterns in user queries may drift under incremental maintenance.
- Recommended fix: Create a dedicated failing or ignored regression for the smallest q12-like query, document the unsupported pattern in SQL reference until fixed, and implement the correct normalization/delta rule or force FULL fallback.

### C-4 - Production panic/unwrap posture is strong, but grep should stay enforced

- Severity: LOW
- Reference: [src/lib.rs](../src/lib.rs#L19), [src/lib.rs](../src/lib.rs#L21), [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs#L5506)
- Problem: The crate denies `clippy::unwrap_used` outside tests, and prior risky rewrite code has a comment noting that a production `expect()` was avoided. Remaining `expect()` sites found in `rewrites.rs` are in tests.
- Risk: Low today. The bigger issue is regression risk as SQL parsing grows.
- Recommended fix: Keep clippy denial in CI and add a narrow grep/semgrep check for `unwrap`, `expect`, and `panic!` outside `#[cfg(test)]` modules.

## 2. Data Durability & Transaction Safety

Summary: Trigger-based CDC gives strong transactional behavior for ordinary DML because change-buffer writes participate in the source transaction. The most important open questions are around cleanup ordering, multi-consumer concurrency, and IMMEDIATE-mode savepoint semantics.

### D-1 - Multi-consumer cleanup lacks an explicit source-level lock around min-frontier plus delete

- Severity: HIGH
- Reference: [src/refresh/codegen.rs](../src/refresh/codegen.rs#L289), [src/refresh/codegen.rs](../src/refresh/codegen.rs#L342), [src/refresh/codegen.rs](../src/refresh/codegen.rs#L465), [src/refresh/codegen.rs](../src/refresh/codegen.rs#L548)
- Problem: Cleanup computes the minimum frontier across consumers and then deletes or truncates rows at or below that LSN. That is the right safety rule, but the computation plus DML is not protected by an explicit per-source advisory lock.
- Risk: A concurrent refresh that has selected its LSN range but not persisted its frontier could race with cleanup based on older catalog state. This needs verification with the actual transaction boundaries, but the code should make the invariant mechanically obvious.
- Recommended fix: Use a per-source advisory lock for cleanup and refresh range selection, or prove and document the existing transaction ordering. Add an E2E test with two stream tables consuming the same source while refreshes overlap.

### D-2 - IMMEDIATE mode needs SAVEPOINT and partial rollback tests

- Severity: MEDIUM
- Reference: [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L123), [src/dvm/parser/validation.rs](../src/dvm/parser/validation.rs#L1064)
- Problem: IMMEDIATE mode is heavily constrained by supported query shapes, but the test matrix should explicitly cover SAVEPOINT, ROLLBACK TO SAVEPOINT, repeated UPDATE to the same key, and mixed INSERT/DELETE inside one transaction.
- Risk: Trigger maintenance may be transactionally rolled back by PostgreSQL, but row-level intermediate deltas can still expose subtle bugs in key-change and aggregate paths.
- Recommended fix: Add dedicated E2E tests for IMMEDIATE mode transaction semantics independent of TPC-H.

### D-3 - Cleanup failures are tracked, which is a strength

- Severity: LOW
- Reference: [src/refresh/codegen.rs](../src/refresh/codegen.rs#L151), [src/refresh/codegen.rs](../src/refresh/codegen.rs#L175), [src/lib.rs](../src/lib.rs#L386)
- Problem: Not a bug. `pgt_cleanup_status` plus retry/backpressure status is a good durability/operability pattern.
- Recommended follow-up: Add a chaos test that forces cleanup DELETE failure for three attempts and verifies alert/status behavior.

## 3. Performance & Scalability

Summary: The project has made real performance investments: summary tables, DVM caches, planner hints, DAG invalidation rings, and TPC-H scale testing. Remaining gaps are in cost-model accuracy, complex subquery rewrites, and the cleanup/history paths.

### P-1 - Correlated scalar subqueries in WHERE can be O(delta x table)

- Severity: HIGH
- Reference: [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L91), [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L102), [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs#L1680)
- Problem: q20 is pre-skipped at SF-10 because DVM delta SQL re-evaluates a correlated scalar subquery per delta row. The rewrite path skips dot-qualified correlations and only decorrelates some bare-column cases.
- Risk: This is a real scalability cliff for analytics-style queries with correlated thresholds or semi-join filters.
- Recommended fix: Add a rewrite that turns supported correlated aggregate subqueries into pre-aggregated CTEs joined once per refresh. Until then, detect this pattern and force FULL/AUTO fallback with a clear reason.

### P-2 - Query complexity classification is regex-only

- Severity: HIGH
- Reference: [src/refresh/mod.rs](../src/refresh/mod.rs#L114), [src/refresh/mod.rs](../src/refresh/mod.rs#L118)
- Problem: `classify_query_complexity()` uppercases SQL and checks strings such as `" JOIN "`, `"GROUP BY"`, and `" WHERE "`.
- Risk: Comments, nested subqueries, LATERAL forms, comma joins, CTEs, or unusual formatting can misclassify a stream table and bias AUTO-mode thresholds incorrectly.
- Recommended fix: Prefer an OpTree-based classifier where parsing is already available. Store complexity in the catalog with the query hash and invalidate on query changes.

### P-3 - Cost model still scans recent refresh history per stream table

- Severity: MEDIUM
- Reference: [src/refresh/orchestrator.rs](../src/refresh/orchestrator.rs#L153), [src/refresh/orchestrator.rs](../src/refresh/orchestrator.rs#L244), [src/lib.rs](../src/lib.rs#L375), [src/catalog.rs](../src/catalog.rs#L2215)
- Problem: `query_refresh_history_stats()` and `estimate_cost_based_threshold()` query recent rows from `pgt_refresh_history` per ST. The project already maintains `pgt_refresh_summary`, but the cost path does not appear to use it for crossover estimates.
- Risk: At hundreds or thousands of stream tables, scheduler decision-making can become catalog-SPI heavy.
- Recommended fix: Extend `pgt_refresh_summary` or add a small rolling stats table for differential/full timing by pgt_id. Batch lookup for all due STs per scheduler tick.

### P-4 - Placeholder resolver cache key has no collision guard

- Severity: MEDIUM
- Reference: [src/dvm/mod.rs](../src/dvm/mod.rs#L213), [src/dvm/mod.rs](../src/dvm/mod.rs#L222), [src/dvm/mod.rs](../src/dvm/mod.rs#L239)
- Problem: The placeholder resolver cache key is a `DefaultHasher` u64 over template plus OIDs. Unlike the snapshot CTE cache, it does not store the original canonical key for collision verification.
- Risk: Collision probability is low, but using the wrong Aho-Corasick automaton for SQL template replacement is correctness-sensitive.
- Recommended fix: Use the full `(template_hash, source_oids, st_source_pgt_ids)` structure as the key or store the original key string next to the cached resolver and verify before reuse.

### P-5 - DVM snapshot fingerprint pointer cache is acceptable as scoped optimization

- Severity: LOW
- Reference: [src/dvm/diff.rs](../src/dvm/diff.rs#L221), [src/dvm/diff.rs](../src/dvm/diff.rs#L900)
- Problem: Not an immediate issue. The pointer-keyed fingerprint cache is scoped to one `DiffContext`, and comments state the OpTree is borrowed immutably for that lifetime.
- Recommended follow-up: Keep the secondary structural equality guard and fuzz fingerprint stability when refactoring OpTree allocation.

## 4. Code Quality & Maintainability

Summary: The codebase has good modular boundaries at the top level, but some generated/refactored modules still carry broad lint suppressions and large SQL-building surfaces. This is now mostly maintainability debt, not architectural failure.

### Q-1 - SQL codegen/merge modules suppress many unused imports

- Severity: MEDIUM
- Reference: [src/refresh/codegen.rs](../src/refresh/codegen.rs#L6), [src/refresh/merge/mod.rs](../src/refresh/merge/mod.rs#L6)
- Problem: Several consecutive `#[allow(unused_imports)]` attributes hide stale imports and make code movement harder to verify.
- Risk: Dead imports are not dangerous by themselves, but broad suppressions reduce lint signal in the highest-risk SQL generation modules.
- Recommended fix: Remove unused-import suppressions module by module, keeping only documented conditional imports where necessary.

### Q-2 - SQL APIs and helpers use many argument suppressions

- Severity: MEDIUM
- Reference: [src/api/alter.rs](../src/api/alter.rs#L1582), [src/api/create.rs](../src/api/create.rs#L24), [src/api/mod.rs](../src/api/mod.rs#L1259)
- Problem: `#[allow(clippy::too_many_arguments)]` appears across create/alter API paths. pgrx SQL functions need many parameters, but internal implementations should not have to carry the same shape everywhere.
- Risk: Harder testing, error-prone parameter forwarding, and less discoverable ergonomics.
- Recommended fix: Keep the SQL wrapper signature, but convert immediately into typed parameter structs for internal logic.

### Q-3 - Global dead-code allowance weakens cleanup pressure

- Severity: MEDIUM
- Reference: [src/lib.rs](../src/lib.rs#L19)
- Problem: The crate has `#![allow(dead_code)]`. This is understandable for pgrx/exported symbols, but it suppresses useful maintenance feedback across all modules.
- Risk: Stale compatibility code and unused helpers can linger in sensitive paths.
- Recommended fix: Replace global allowance with narrower allowances on pgrx/export boundary modules or generated code.

### Q-4 - Deprecated `consume_slot_changes()` remains in CDC module

- Severity: MEDIUM
- Reference: [src/cdc/mod.rs](../src/cdc/mod.rs#L2206), [src/cdc/mod.rs](../src/cdc/mod.rs#L2211)
- Problem: `consume_slot_changes()` is explicitly deprecated, no longer used by the refresh pipeline, and performs a full `count(*)` if called.
- Risk: Confuses the trigger-based CDC model and can encourage inefficient diagnostics.
- Recommended fix: Remove it if private, or mark it `#[deprecated]` and move diagnostics to a clearly named status function.

## 5. API Ergonomics & SQL Interface

Summary: The SQL interface is broad and generally documented. The biggest ergonomic issue is not missing functions but the size and complexity of create/alter surfaces.

### A-1 - Create/alter surfaces are feature-rich but parameter-heavy

- Severity: MEDIUM
- Reference: [docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md#L168), [src/api/alter.rs](../src/api/alter.rs#L1582)
- Problem: `create_stream_table()` and `alter_stream_table()` expose many knobs. That is powerful, but harder to discover and easy to misuse.
- Risk: Users may set advanced options without understanding interactions; internal forwarding becomes brittle.
- Recommended fix: Add helper functions or structured presets for common modes: `create_stream_table_fast_append_only`, `set_stream_table_refresh_policy`, `set_stream_table_storage_policy`, or equivalent documented recipes.

### A-2 - Pause/resume should be explicitly first-class if not already

- Severity: LOW
- Reference: [src/api/alter.rs](../src/api/alter.rs#L1588)
- Problem: `alter_stream_table(... status => ...)` appears to provide status changes, but users will look for obvious pause/resume verbs.
- Risk: Operational UX friction.
- Recommended fix: Add or document `pause_stream_table(name)` / `resume_stream_table(name)` wrappers over status transitions.

### A-3 - Internal event trigger functions are marked `sql = false`, which is good

- Severity: LOW
- Reference: [src/hooks.rs](../src/hooks.rs#L51), [src/hooks.rs](../src/hooks.rs#L1094)
- Problem: No immediate action. `_on_ddl_end` and `_on_sql_drop` use `sql = false`, so they should not be exposed as normal SQL API objects by pgrx generation.
- Recommended follow-up: Keep internal hook functions documented in comments but out of public SQL reference.

## 6. Test Coverage Analysis

Summary: Test coverage is broad and unusually mature. The main gap is that complex DVM algebra is still better covered by curated E2E examples than by property/fuzz generation.

### T-1 - Property tests do not cover DVM algebra deeply enough

- Severity: HIGH
- Reference: [tests/property_tests.rs](../tests/property_tests.rs#L1), [tests/property_tests.rs](../tests/property_tests.rs#L28)
- Problem: Property tests cover LSN ordering, frontier roundtrips, DAG invariants, SQL identifier quoting, and helper-level behavior. They do not yet generate aggregate/join/window delta scenarios and compare incremental results against full recomputation.
- Risk: Combinatorial DVM bugs can survive curated examples.
- Recommended fix: Add property generators for small schemas and operation sequences. For each generated workload, compare FULL result with differential ST result after every cycle.

### T-2 - TPC-H coverage is strong, but SF-10 breadth is intentionally limited

- Severity: MEDIUM
- Reference: [.github/workflows/tpch-nightly.yml](../.github/workflows/tpch-nightly.yml#L38), [.github/workflows/tpch-nightly.yml](../.github/workflows/tpch-nightly.yml#L57), [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L102)
- Problem: SF-1 runs broad coverage, but SF-10 runs only differential correctness and pre-skips q20. That is a reasonable cost tradeoff, but it means high-scale plan changes for the rest of TPC-H are not continuously validated.
- Recommended fix: Add a rotating SF-10 subset across weekdays or maintain per-query EXPLAIN/latency artifacts with regression thresholds.

### T-3 - Fuzz smoke budget is short

- Severity: MEDIUM
- Reference: [.github/workflows/fuzz-smoke.yml](../.github/workflows/fuzz-smoke.yml#L51), [.github/workflows/fuzz-smoke.yml](../.github/workflows/fuzz-smoke.yml#L75)
- Problem: PR seed smoke defaults to 30 seconds, and schedule/manual fuzz defaults to 60 seconds per target.
- Risk: Good crash replay, limited discovery.
- Recommended fix: Keep PR smoke short but add a nightly extended fuzz workflow or raise scheduled target time to 300 seconds. Track corpus size and coverage growth.

### T-4 - TRUNCATE LSN semantics need a targeted regression

- Severity: HIGH
- Reference: [tests/e2e_cdc_tests.rs](../tests/e2e_cdc_tests.rs#L14), [tests/e2e_wake_tests.rs](../tests/e2e_wake_tests.rs#L106)
- Problem: Existing tests verify TRUNCATE behavior and wake notification, but no grep/test asserts the TRUNCATE trigger uses `pg_current_wal_insert_lsn()`.
- Recommended fix: Add an E2E inspection test for the trigger function source and a transactional edge case around TRUNCATE marker LSN ordering.

### T-5 - dbt integration exists but should grow beyond core marts

- Severity: LOW
- Reference: [dbt-pgtrickle/integration_tests/models/marts/schema.yml](../dbt-pgtrickle/integration_tests/models/marts/schema.yml#L1)
- Problem: dbt tests cover several marts, stream table health, partitioning, AUTO mode, and fused config. Missing coverage areas include version matrix, failure recovery, and adapter behavior across dbt-postgres versions.
- Recommended fix: Add an adapter compatibility matrix and tests for alter/drop/rebuild flows from dbt.

## 7. Security Analysis

Summary: Security foundations look solid: RLS behavior is documented, change buffers use SECURITY DEFINER where needed, internal event hooks are not generated as public SQL, and the codebase has SQL builder helpers. The main recommendation is to keep dynamic SQL enforcement automatic.

### S-1 - Dynamic SQL safety should be enforced by CI rules, not convention

- Severity: MEDIUM
- Reference: [src/sql_builder.rs](../src/sql_builder.rs), [src/refresh/codegen.rs](../src/refresh/codegen.rs#L342), [src/cdc/mod.rs](../src/cdc/mod.rs#L2217)
- Problem: The extension necessarily builds a large amount of dynamic SQL. Safe helper APIs exist, and many values are internal OIDs/stable names, but manual `format!` remains widespread.
- Risk: Future edits can accidentally interpolate user-controlled identifiers or values without quoting.
- Recommended fix: Add or strengthen semgrep rules that distinguish identifiers, literals, OIDs, and internal stable names. Require `quote_identifier`, `sql_builder::ident`, parameters, or documented internal-only values.

### S-2 - RLS behavior is clearly documented; keep warning prominent

- Severity: LOW
- Reference: [docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md#L2988), [docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md#L2999), [docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md#L3004)
- Problem: Not a gap now. SQL reference explicitly states that source-table RLS is ignored during refresh and stream tables contain all rows.
- Recommended follow-up: Emit a runtime warning on create when a source has RLS enabled, if not already done.

### S-3 - Search-path pinning should remain part of security tests

- Severity: LOW
- Reference: [src/cdc/mod.rs](../src/cdc/mod.rs#L303), [docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md#L3004)
- Problem: Trigger functions pin search_path. This is a strength.
- Recommended follow-up: Keep a test that inspects SECURITY DEFINER functions for explicit `SET search_path`.

## 8. Operational & Observability Gaps

Summary: Observability is better than expected. `health_check`, `metrics_summary`, `wal_source_status`, cleanup status, and invalidation overflow counters exist. The next step is to make high-risk DVM performance cliffs visible before operators hit them.

### O-1 - Add operator-visible DVM fallback/performance reason codes

- Severity: MEDIUM
- Reference: [src/refresh/mod.rs](../src/refresh/mod.rs#L114), [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L102)
- Problem: Known complex-query cliffs such as q20 should surface as explicit reason codes in refresh history, status, and health output.
- Risk: Operators see slow refreshes or FULL fallback without knowing whether the cause is query shape, delta size, missing indexes, or planner spill.
- Recommended fix: Add reason codes like `CORRELATED_SUBQUERY_DELTA_QUADRATIC`, `CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK`, `REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN`.

### O-2 - Invalidation ring observability is a strength

- Severity: LOW
- Reference: [src/shmem.rs](../src/shmem.rs#L148), [src/monitor/mod.rs](../src/monitor/mod.rs#L739), [src/api/diagnostics.rs](../src/api/diagnostics.rs#L1707), [docs/CONFIGURATION.md](../docs/CONFIGURATION.md#L1503)
- Problem: Earlier risk around silent ring overflow appears addressed: the project has a counter, monitoring output, diagnostics, and documentation.
- Recommended follow-up: Add a threshold alert in `health_check()` if overflow count increases within a recent time window, not just since startup.

### O-3 - Cleanup status is good; expose backlog trends

- Severity: LOW
- Reference: [src/lib.rs](../src/lib.rs#L386), [src/refresh/codegen.rs](../src/refresh/codegen.rs#L175)
- Problem: `pgt_cleanup_status` tracks failures and backlog at attempts. A trend view would help capacity planning.
- Recommended fix: Add a lightweight historical cleanup metric or integrate backlog into existing metrics summary.

## 9. Documentation Gaps

Summary: Documentation is significantly better than many codebases of this size. The biggest docs risk is drift: the SQL API is large and still growing.

### DOC-1 - Keep SQL reference generated or checked against pg_externs

- Severity: LOW
- Reference: [docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md#L168), [docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md#L4007), [docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md#L4019)
- Problem: SQL reference already documents advanced parameters such as `temporal`, `storage_backend`, `cluster_worker_summary`, and `wal_source_status`. That is good, but manual drift risk remains high.
- Recommended fix: Add a docs lint that compares `#[pg_extern]` exports with SQL reference entries and reports undocumented public functions.

### DOC-2 - Document known DVM unsupported/fallback patterns in one place

- Severity: MEDIUM
- Reference: [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L91), [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L1946)
- Problem: Important known limitations are currently visible in tests and comments. Users need a concise compatibility table for patterns that force FULL, are unsupported in IMMEDIATE, or are correctness-sensitive.
- Recommended fix: Add `docs/DVM_SUPPORT_MATRIX.md` or expand SQL reference with query pattern support, fallback behavior, and examples.

## 10. Dependency & Build Health

Summary: Build and dependency hygiene is strong. The project has a justfile, cargo-deny policy, upgrade completeness scripts, fuzz workflows, and multiple CI lanes.

### B-1 - Build workflow is split between automatic and manual paths

- Severity: LOW
- Reference: [.github/workflows/build.yml](../.github/workflows/build.yml#L20), [.github/workflows/lint.yml](../.github/workflows/lint.yml#L25)
- Problem: Manual-only build workflow is fine if lint/test workflows cover automatic gates. Keep this intentional split documented so contributors do not assume build.yml is the PR gate.
- Recommended fix: Add a short comment in CONTRIBUTING or CI docs describing which workflows gate PRs.

### B-2 - Dependency policy appears deliberate

- Severity: LOW
- Reference: [deny.toml](../deny.toml)
- Problem: No immediate issue found in this pass. The important ongoing work is keeping advisory suppressions reviewed.
- Recommended fix: Keep review-by dates and require cargo-deny in PR gates.

## 11. Differential Dataflow Engine Correctness Deep Dive

Summary: The DVM engine is sophisticated and has many prior fixes for joins, snapshots, placeholders, aggregates, keyless tables, vector aggregates, and LATERAL support. The remaining world-class gap is not lack of ambition; it is proving complex SQL constructs through property tests and removing known TPC-H exceptions.

### DVM-1 - CASE/IN-list aggregate drift needs root-cause fix or forced fallback

- Severity: HIGH
- Reference: [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L1946), [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs)
- Problem: q12 is excluded from churn because of known drift around CASE WHEN IN-list behavior.
- Recommended fix: Minimize q12 to a dedicated DVM unit/E2E test. Either implement the correct rewrite/delta rule or reject/fallback before incremental maintenance.

### DVM-2 - Correlated scalar subquery rewrite coverage is incomplete

- Severity: HIGH
- Reference: [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs#L1680), [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs#L2075), [tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs#L102)
- Problem: Some correlated scalar subqueries are decorrelated, but dot-qualified correlations are skipped and LIMIT/OFFSET cases become LATERAL. q20 demonstrates that at least one correlated aggregate WHERE pattern remains too expensive.
- Recommended fix: Add support matrix entries for scalar subquery forms, implement pre-aggregation for safe aggregate correlations, and set explicit fallback for unsafe ones.

### DVM-3 - Runtime delta-dedup validation would be a valuable safety valve

- Severity: MEDIUM
- Reference: [src/dvm/diff.rs](../src/dvm/diff.rs#L96), [src/dvm/diff.rs](../src/dvm/diff.rs#L728), [src/dvm/mod.rs](../src/dvm/mod.rs#L344)
- Problem: The merge path relies on DVM metadata such as `is_deduplicated`. A bug in an operator flag can become a data correctness issue.
- Recommended fix: Add a debug GUC such as `pg_trickle.validate_delta_invariants` that checks row-id uniqueness, action validity, and negative-count invariants before merge in tests/staging.

## 12. Upgrade & Migration Safety

Summary: Upgrade safety is one of the project's strengths. There are many SQL upgrade scripts, CI discovery for upgrade pairs, and generated schema checks. The main gap is rollback/runbook clarity.

### U-1 - Rollback strategy is less visible than forward upgrade strategy

- Severity: LOW
- Reference: [.github/workflows/ci.yml](../.github/workflows/ci.yml#L1200), [sql](../sql)
- Problem: Forward upgrade paths are heavily tested. Downgrade/rollback guidance is not equally prominent.
- Risk: Operators may improvise after a failed extension upgrade.
- Recommended fix: Add a short operational runbook: backup requirements, snapshot/export recommendation, restore path, and why PostgreSQL extension downgrades are not generally supported.

### U-2 - Upgrade E2E cutoff is intentional but should stay documented

- Severity: LOW
- Reference: [.github/workflows/ci.yml](../.github/workflows/ci.yml#L548)
- Problem: CI comments state the support cutoff is v0.40.0+. That is a reasonable policy.
- Recommended fix: Mirror the policy in release docs and CHANGELOG.

## Quick Wins

1. Fix TRUNCATE LSN from `pg_current_wal_lsn()` to `pg_current_wal_insert_lsn()` and add a trigger-body regression.
2. Add q12 minimized regression and mark it as expected failure or FULL fallback until fixed.
3. Add a DVM support matrix row for correlated scalar subqueries in WHERE and q20-style patterns.
4. Replace regex complexity classification with an optional OpTree classifier behind a GUC, then compare both in logs.
5. Remove unused-import suppressions in `src/refresh/codegen.rs` and `src/refresh/merge/mod.rs`.
6. Convert internal create/alter implementations to typed parameter structs.
7. Add per-source advisory locking or a proof comment plus concurrency E2E around change-buffer cleanup.
8. Raise scheduled fuzz time to 300 seconds per target or create a nightly extended fuzz workflow.
9. Add a debug GUC for delta invariant validation.
10. Add docs lint comparing public pg_extern functions with SQL reference entries.

## Missing Features

| Feature | Priority | Rationale |
| --- | --- | --- |
| OpTree-based query complexity classifier | HIGH | AUTO mode should reason from parsed structure, not string patterns. |
| Correlated aggregate subquery pre-aggregation rewrite | HIGH | Needed for q20-style analytics queries at scale. |
| CASE/IN-list DVM fallback or exact delta rule | HIGH | q12 drift shows a correctness-sensitive gap. |
| Debug delta invariant validator | MEDIUM | Gives staging and CI a runtime guard for row-id/action/count invariants. |
| First-class pause/resume wrappers | LOW | Improves operational ergonomics over generic status alteration. |
| Cleanup backlog trend metrics | LOW | Helps operators see buffer growth before it becomes urgent. |
| Upgrade rollback runbook | LOW | Helps DBAs handle failed upgrades safely. |
| Public DVM support matrix | MEDIUM | Converts scattered test comments into user-facing guidance. |

## Test Coverage Matrix

| Module / Area | Unit | Integration | Light E2E | Full E2E / TPC-H | Property / Fuzz | Quality |
| --- | --- | --- | --- | --- | --- | --- |
| `src/api/` | Partial | Good | Good | Good | Missing | Partial |
| `src/catalog.rs` | Partial | Good | Good | Good | Missing | Good |
| `src/cdc/` | Partial | Good | Good | Good | Fuzz target exists | Good, with TRUNCATE LSN gap |
| `src/config.rs` | Partial | Good | Good | Partial | GUC fuzz target | Good |
| `src/dag.rs` | Good | Good | Good | Partial | Property/fuzz target | Good |
| `src/dvm/parser/` | Good | Good | Good | TPC-H broad | Parser fuzz target | Good |
| `src/dvm/operators/` | Partial | Good | Good | TPC-H broad | Needs algebra properties | Partial |
| `src/dvm/diff.rs` | Good | Good | Good | TPC-H broad | Needs invariant checks | Partial-Good |
| `src/dvm/row_id.rs` | Partial | Good | Good | TPC-H broad | Row-id fuzz target | Partial |
| `src/refresh/` | Good | Good | Good | Good | Merge SQL fuzz target | Good |
| `src/scheduler/` | Good | Good | Good | Stability tests | Property coverage | Good |
| `src/shmem.rs` | Unit tests for ring | Partial | Indirect | Stability tests | Missing | Partial |
| `src/monitor/` | Partial | Good | Good | Partial | Missing | Good |
| `src/wal_decoder.rs` | Partial | Partial | Partial | Partial | WAL fuzz target | Partial |
| SQL upgrade scripts | N/A | Upgrade completeness | Upgrade slice | Scheduled upgrade E2E | Missing | Good |
| dbt integration | N/A | dbt project tests | Scheduled/manual | N/A | Missing | Partial-Good |

## Recommended Next Milestones

### Milestone 1 - Correctness Stop-the-Line Fixes

1. Fix TRUNCATE LSN capture.
2. Add targeted regression for TRUNCATE marker LSN ordering.
3. Create minimized q12 drift test and decide fix vs forced FULL fallback.
4. Add explicit q20 support/fallback reason.

### Milestone 2 - DVM World-Class Proof Work

1. Implement correlated aggregate subquery pre-aggregation where mathematically safe.
2. Add property-based differential-vs-full tests for aggregate, join, anti-join, CASE, IN-list, and window patterns.
3. Add `pg_trickle.validate_delta_invariants` and run it in CI/staging tiers.

### Milestone 3 - Scheduler and Cost Model Scale-Out

1. Replace regex complexity classifier with OpTree/cached catalog classification.
2. Move cost-model history lookups to precomputed rolling summaries.
3. Add cleanup concurrency E2E with shared sources and overlapping refreshes.

### Milestone 4 - Maintainability and API Polish

1. Remove broad unused-import and dead-code allowances where possible.
2. Convert internal API implementation arguments to typed params.
3. Add convenience pause/resume and policy-setting wrappers.

### Milestone 5 - Operational Trust and Docs

1. Publish DVM support/fallback matrix.
2. Add upgrade rollback runbook.
3. Add docs/API export lint.
4. Extend fuzz runtime and corpus tracking.

## Closing Assessment

The project is on a strong trajectory. It already contains many mechanisms expected from a serious production extension: transactional trigger CDC, frontier-based cleanup, health checks, metrics, upgrade validation, and nontrivial TPC-H coverage. The path to world-class is now less about adding surface area and more about proving the hardest behaviors: exact DVM semantics under complex SQL, predictable performance at scale, and simple operational explanations when the engine chooses fallback or hits a limitation.