# PLAN_OVERALL_ASSESSMENT.md — Deep Project Assessment

**Status:** Assessment report
**Type:** REPORT
**Last updated:** 2026-04-17
**Scope:** Repository-wide review of pg_trickle at v0.20.0 (main @ `cfacdc0`)
**Target audience:** Maintainers planning v0.21+ and v1.0

> **Roadmap cross-reference:** All items from §10 (Prioritised Action Plan) have been
> assigned to milestones in [ROADMAP.md](../ROADMAP.md). P0 items land in **v0.21.0**;
> P1 #8 and #11 land in **v0.22.0** (Production Scalability & Downstream Integration);
> remaining P1 items and selected P2 items are distributed across **v0.21.0** and
> **v0.22.0**. P3 items are tracked in Post-1.0. See §10 for per-item milestone tags.

---

## Executive Summary

pg_trickle is a mature, well-documented, aggressively tested
PostgreSQL 18 IVM extension (~102k LOC in `src/`, 1,855 in-crate
`#[test]` / `#[pg_test]` functions, 134 integration test files, 100+
planning documents, TPC-H & SQLancer nightly gates). Correctness,
performance and operational ergonomics are clearly the top three
priorities and have all received real investment.

The highest-impact risks right now are:

1. **A known-critical correctness bug in JOIN delta computation** (EC-01
   residual — phantom rows from split Part 1a / Part 1b row-id hashing).
   TPC-H Q07 and Q15 regress across cycles. Already documented in repo
   memory; not yet fully fixed.
2. **A very large unsafe surface** (547 `unsafe` sites) with only a
   *Proposed* plan to reduce it.
3. **Monolithic hot-path modules** — `refresh.rs` (8.4k LOC),
   `scheduler.rs` (6.7k LOC), `dag.rs` (4.3k LOC), parser
   (`sublinks.rs` 6.4k, `rewrites.rs` 5.9k, `mod.rs` 5.5k). Change-risk
   is concentrated in a handful of files.
4. **Unit-test gaps in three of the biggest files**
   (`api/helpers.rs` 2.5k LOC, `api/diagnostics.rs` 1.5k LOC,
   `dvm/parser/rewrites.rs` 5.9k LOC all lack `mod tests`).
5. **No true in-database parallel refresh**, no multi-database support,
   no downstream CDC emitter — three of the most frequently-requested
   productionisation features.

The remainder of this document breaks each risk down with code
citations, then proposes 12 new feature ideas, and finishes with a
prioritised action table.

---

## Table of Contents

1. [Method & Scope](#1-method--scope)
2. [Findings — Code Quality & Bug Risk](#2-findings--code-quality--bug-risk)
3. [Findings — Architecture & Design](#3-findings--architecture--design)
4. [Findings — Performance & Scalability](#4-findings--performance--scalability)
5. [Findings — SQL & API Surface](#5-findings--sql--api-surface)
6. [Findings — Testing & Reliability](#6-findings--testing--reliability)
7. [Findings — Documentation & Usability](#7-findings--documentation--usability)
8. [Findings — Operational Aspects](#8-findings--operational-aspects)
9. [New Feature Proposals](#9-new-feature-proposals)
10. [Prioritised Action Plan](#10-prioritised-action-plan)
11. [Appendix — Repository Metrics](#11-appendix--repository-metrics)

---

## 1. Method & Scope

**Inputs reviewed**

- [ROADMAP.md](../ROADMAP.md), [CHANGELOG.md](../CHANGELOG.md),
  [AGENTS.md](../AGENTS.md), [ESSENCE.md](../ESSENCE.md)
- [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md), [docs/ERRORS.md](../docs/ERRORS.md),
  [docs/CONFIGURATION.md](../docs/CONFIGURATION.md),
  [docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md)
- All top-level source modules in [src/](../src/) (20 files, ~102k LOC)
- The [plans/INDEX.md](INDEX.md) catalogue and ~15 key plan documents
- Repo memory: [df-cdc-buffer-trends-fix](../.memory) and
  [join-delta-phantom-rows](../.memory) notes
- `git log` — last 30 commits and recent tags (v0.19.0, v0.20.0)
- Integration test directory structure (134 files)
- Regex scans for `unwrap`, `panic!`, `unreachable!`, `todo!`, `unsafe`,
  `TODO|FIXME|HACK|XXX`

**Out of scope**

- Bytecode-level audit of `unsafe` blocks (deferred — 547 sites)
- Running the full test suite (Docker-based E2E)
- dbt adapter internals (dbt-pgtrickle/)

---

## 2. Findings — Code Quality & Bug Risk

### 2.1 CRITICAL — JOIN delta phantom rows (EC-01 residual)

**Evidence:** Repo memory `/memories/repo/join-delta-phantom-rows.md`,
[src/dvm/operators/join.rs](../src/dvm/operators/join.rs) L234–245, L657–663,
[src/refresh.rs](../src/refresh.rs) L1774–1820, L4991–5005,
[tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs) L2153, L3214.

Root cause: when EC-01 splits Part 1 into 1a (Δ⋈R₁) and 1b (Δ⋈R₀),
the `__pgt_row_id` hash depends on the right-side PK. For a co-deleted
right row, `R₀ ≠ R₁`, so the two halves emit *different* row ids for
the same logical row; weight aggregation cannot cancel them across
cycles because the GROUP BY is keyed on `__pgt_row_id`. The PH-D1
delete path also deletes only the row ids present in the current delta,
leaving prior-cycle phantoms in place.

Observable failures:

- `test_tpch_q07_ec01b_combined_delete` — revenue drift after
  combined LEFT+RIGHT DELETE.
- `test_tpch_immediate_correctness` — Q15 extra supplier row after
  UPDATE (IMMEDIATE mode); Q07 extra row after RF1 INSERT.

**Severity:** P0 (silent wrong results).
**Recommendation:** keep Q15 off the IMMEDIATE allowlist and pursue
one of two fixes: (a) hash only the *left* PK on Part 1b so the two
halves converge on a single row id; (b) recompute Part 1b via the
pre-image columns already captured by the CDC trigger (see EC-01
"Deferred — pre-image capture" note in
[PLAN_EDGE_CASES.md](PLAN_EDGE_CASES.md)). Either approach keeps the
EXCEPT ALL snapshot logic that ships today.

> **→ Roadmap:** [v0.21.0 §EC-01 Fix](../roadmap/v0.21.0.md-full.md) — items EC01-0 through EC01-4.

### 2.2 HIGH — `.unwrap()` in production parser hot paths

`src/` contains 513 `.unwrap()` calls total. Most are in `#[cfg(test)]`
modules, but a non-trivial subset runs on the SQL parsing path:

- [src/dvm/parser/sublinks.rs](../src/dvm/parser/sublinks.rs) — 28
  non-test unwraps (e.g. `from_list.head().unwrap()`, `fields.get_ptr(0).unwrap()`).
  These rely on PostgreSQL list invariants; a malformed `Node*` from a
  future PG minor release or an ALTER EXTENSION in-flight could crash the
  backend.
- [src/dag.rs](../src/dag.rs) L1093, L1100, L1112, L1795 — SCC
  invariant-based unwraps, tagged `nosemgrep: rust.panic-in-sql-path`.
  If any future mutation violates the invariant, the bgworker panics.

**Recommendation:** convert the parser unwraps to `?`
(`ok_or(PgTrickleError::UnsupportedPattern)`) — cost is a one-line
wrapper per site; do it opportunistically when touching each function.
Add a clippy `deny(clippy::unwrap_used)` lint gate outside of test
modules, with an `allow` on `dag.rs` invariant-justified lines.

> **→ Roadmap:** [v0.21.0 §Safety & Code Quality](../roadmap/v0.21.0.md-full.md) — items SAF-1 and SAF-3.

### 2.3 HIGH — 547 `unsafe` blocks, reduction plan still "Proposed"

[plans/safety/PLAN_REDUCED_UNSAFE.md](safety/PLAN_REDUCED_UNSAFE.md) has
been in *Proposed* state since before v0.10. Most unsafe is pgrx FFI,
but the scan is very coarse because `mod tests` in the same file inflate
the counter. A targeted pass to:

1. Replace `unsafe { pg_sys::list_head(...) }` patterns with safe
   `PgList<T>::iter()` helpers (where they don't already exist).
2. Wrap `get_ptr(i).unwrap()` patterns in a single `list_nth_safe<T>()`
   helper that returns `Option<PgBox<T>>`.
3. Group repeated `unsafe { pg_sys::* }` FFI calls into module-level
   safe façades in `src/dvm/parser/types.rs`.

would likely halve the count and create a reviewable audit unit.

> **→ Roadmap:** [v0.21.0 §Safety & Code Quality](../roadmap/v0.21.0.md-full.md) — item SAF-2.

### 2.4 MEDIUM — Monolithic modules invite silent coupling

| File | LOC | Risk |
|---|---|---|
| [src/refresh.rs](../src/refresh.rs) | 8,451 | Refresh orchestration, delta SQL codegen, PH-D1, MERGE path — four concerns in one file |
| [src/scheduler.rs](../src/scheduler.rs) | 6,706 | Dispatch loop, tier logic, quota, interference detection |
| [src/dvm/parser/sublinks.rs](../src/dvm/parser/sublinks.rs) | 6,361 | Sublink lowering + correlated rewrites |
| [src/api/mod.rs](../src/api/mod.rs) | 6,009 | Public SQL API surface + validation + orchestration |
| [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs) | 5,918 | 7 rewrite passes chained |
| [src/dvm/parser/mod.rs](../src/dvm/parser/mod.rs) | 5,495 | OpTree parsing |
| [src/dvm/operators/aggregate.rs](../src/dvm/operators/aggregate.rs) | 5,399 | 12 aggregate families + Welford |
| [src/dag.rs](../src/dag.rs) | 4,288 | DAG, SCC, topo, cycle detection, refresh groups |
| [src/dvm/operators/recursive_cte.rs](../src/dvm/operators/recursive_cte.rs) | 4,041 | Non-monotone detection + semi-naive codegen |

These are natural "change-magnets" — most recent fix PRs touch one or
more. Splitting them into focused submodules (e.g. `refresh/{merge.rs,
phd1.rs, codegen.rs, orchestrator.rs}`) reduces reviewer cognitive load
and merge-conflict friction.

> **→ Roadmap:** [v0.21.0 §Architecture](../roadmap/v0.21.0.md-full.md) — item ARCH-1 (`refresh.rs` split into 4 sub-modules).

### 2.5 MEDIUM — Incremental recursive CTE refresh falls back to FULL

[plans/PLAN.md](PLAN.md) records P2-1 (Recursive CTE DRed for
DIFFERENTIAL mode) as **deferred to v0.10.0** and it has not been
re-planned since. Any defining query containing WITH RECURSIVE silently
recomputes from scratch on every cycle — acceptable for small graphs,
latency death for transitive closures over anything nontrivial.

**Recommendation:** re-open a slim v0.22+ item to at least document
the recomputation cost in `EXPLAIN` output and add a Prometheus metric
tagged `refresh_reason="recursive_cte_fallback"` so operators can see
what is happening.

> **→ Roadmap:** [v0.21.0 §Architecture](../roadmap/v0.21.0.md-full.md) — item ARCH-2 (recursive CTE fallback observability).

### 2.6 MEDIUM — `PLAN_NON_DETERMINISM.md` is still "Not started"

[plans/sql/PLAN_NON_DETERMINISM.md](sql/PLAN_NON_DETERMINISM.md) has
status *Not started*. Queries using `now()`, `random()`, `clock_timestamp()`,
and volatile user functions are accepted today without warning, and
incremental refresh will produce different answers than a full refresh
would. This is a data-correctness footgun.

**Recommendation (low effort):** during
`create_stream_table`, inspect the parsed tree for
`pg_proc.provolatile = 'v'` and either (a) reject with a clear error
or (b) emit a `WARNING` and force `FULL` mode unless the user passes
`non_deterministic => true`.

> **→ Roadmap:** [v0.21.0 §Safety & Code Quality](../roadmap/v0.21.0.md-full.md) — item OP-6.

### 2.7 LOW — One `FIXME`, zero `TODO`/`HACK`/`XXX`

Scan found only one `TODO/FIXME/HACK/XXX` in `src/`. Discipline is
strong — recommend adopting a clippy lint to keep it that way.

### 2.8 LOW — `delete_insert` merge strategy deprecation shim

Removed in v0.19.0 with a warning
([config.rs](../src/config.rs) L687). Plan to remove the warning in
v1.0.0 and drop the GUC parsing entirely.

---

## 3. Findings — Architecture & Design

### 3.1 Gap — No in-database parallel refresh worker pool

[plans/sql/PLAN_PARALLELISM.md](sql/PLAN_PARALLELISM.md) is *Proposed*;
the scheduler is single-threaded and dispatches one refresh at a time
(bounded by `max_concurrent_refreshes`, but that spawns background
*transactions*, not worker processes with shared frontier state).

**Impact:** single-node throughput is capped below what modern hardware
can provide. Deployments with 200+ stream tables see serialised tails.

**Recommendation:** promote PLAN_PARALLELISM to v0.22 candidate.
Minimal viable slice: dynamic bgworker pool per database, with
coordinator owning the DAG and the pool owning refresh execution.

> **→ Roadmap:** [v0.22.0 §In-Database Parallel Refresh Worker Pool](../roadmap/v0.22.0.md-full.md) — items PAR-1 through PAR-5.

### 3.2 Gap — No downstream CDC emitter

pg_trickle consumes CDC (trigger or WAL) but cannot *emit* changes to
downstream consumers. `NOTIFY pg_trickle_refresh` is the only signal.
Three plans hint at this ([REPORT_DOWNSTREAM_CONSUMERS.md](infra/REPORT_DOWNSTREAM_CONSUMERS.md),
[PLAN_FUSE.md](sql/PLAN_FUSE.md), the new transactional outbox patterns)
but none is in flight.

**Impact:** cannot be used as the authoritative source for Kafka,
Redis Streams, or event sourcing without a separate replication slot.

**Recommendation:** see §9.2 (feature proposal "ST→logical publication").

> **→ Roadmap:** [v0.22.0 §Downstream CDC Publication](../roadmap/v0.22.0.md-full.md) — items CDC-PUB-1 through CDC-PUB-5.

### 3.3 Gap — Multi-database / multi-tenant isolation

[plans/infra/PLAN_MULTI_DATABASE.md](infra/PLAN_MULTI_DATABASE.md) is
*Draft*. The shmem state (`src/shmem.rs`) currently tracks stream
tables globally; the bgworker attaches per-database but cross-DB
accounting is absent. For managed offerings this is a blocker.

> **→ Roadmap:** Post-1.0 — [Scale §S3](../ROADMAP.md#beyond-v10) (deferred; scope is large).

### 3.4 Gap — Cost model does not include network/disk pressure

The adaptive threshold (P3-4 / B-4) compares `last_full_ms` to a
measured DIFF cost, but only in CPU units. Under `work_mem` pressure
the spill detector
([CHANGELOG "Early warning when refreshes spill to disk"](../CHANGELOG.md))
reacts *after* spill. Ideally the cost estimator also considers
buffer-cache miss rate and per-worker RAM.

### 3.5 Gap — WAL decoder cannot resume across restarts for partial replays

`pgt_change_tracking.decoder_confirmed_lsn` is persisted, but an
in-flight decode batch is not checkpointed. A bgworker crash mid-batch
forces re-decode from `confirmed_lsn`, which is correct but can be
expensive if batches are large. A progress record every *N* rows would
bound the worst case.

### 3.6 Gap — Dog-feeding is reactive, not predictive

v0.20's self-monitoring identifies anomalies *after* they occur (3×
baseline duration spike, error burst). A predictive model — linear
regression on the last hour of delta sizes vs. duration — could preempt
spills and recommend mode switches before a single bad refresh fires.

> **→ Roadmap:** [v0.22.0 §Predictive Refresh Cost Model](../roadmap/v0.22.0.md-full.md) — items PRED-1 through PRED-4.

### 3.7 Strength — Hybrid CDC is well separated

`src/cdc.rs` (3.5k LOC) and `src/wal_decoder.rs` (2.1k LOC) are cleanly
split; the TRIGGER → TRANSITIONING → WAL state machine is explicit in
`pgt_dependencies.cdc_mode`. This is a healthy part of the codebase.

### 3.8 Strength — Operator tree is well-factored

21 operators, each with its own file under
[src/dvm/operators/](../src/dvm/operators/), with a shared
`test_helpers.rs` (719 LOC) providing reusable differential-invariant
assertions. New operators follow a clear template.

---

## 4. Findings — Performance & Scalability

### 4.1 Strength — Scheduler dispatch is already O(1)

v0.19 dropped scheduler poll from ~650 µs → ~45 µs at 500 STs
(CHANGELOG). This removes the most obvious quadratic.

### 4.2 Bottleneck — Per-source change detection issues one query per change buffer

Mitigated in v0.19 by "single-query change detection" — confirm this
path is used on the `IMMEDIATE` hot path, not just scheduled refreshes.

### 4.3 Bottleneck — `diff_project` string concat in hot path

[src/dvm/operators/project.rs](../src/dvm/operators/project.rs) builds
SQL via repeated `format!` inside a per-refresh loop. For stream
tables with 50+ projected columns this allocates dozens of `String`s
per refresh. A reusable `String` buffer (with `write!`) would cut
allocations by ~70% on wide-column STs.

### 4.4 Bottleneck — Template cache is per-backend, not shared

[src/template_cache.rs](../src/template_cache.rs) is only 158 LOC and
lives in backend-local memory. Each PostgreSQL connection warms its
own cache. A `dshash` in shmem would let all backends share one
compiled template set and dramatically improve first-query latency in
`pg_trickle.connection_pooler_mode` deployments.

### 4.5 Bottleneck — Change buffer reads always full-scan the buffer

Compaction (C-4) eliminates net-zero rows, but between compactions the
buffer is scanned end-to-end. A partial index on
`(__pgt_action, __pgt_ts)` would let the DVM pipeline skip ranges that
have already been consumed by the current delta.

### 4.6 Opportunity — Parallel aggregate merge for large SUM/AVG/COVAR groups

The Welford path (P3-2) is elegant but strictly single-threaded. For
stream tables with >1M groups, a parallel merge via
`pg_parallel_safe` helpers could cut latency in half with no
correctness cost (all ops are commutative on Welford aux columns).

### 4.7 Opportunity — Missing SIMD-friendly hashing

[src/hash.rs](../src/hash.rs) (119 LOC) computes `pg_trickle_hash_multi`
via a scalar loop. On AVX2/NEON hosts a SIMD batch hash (xxhash3 batched)
would reduce row-id computation cost, which is on the hot path for
every delta.

---

## 5. Findings — SQL & API Surface

### 5.1 Ergonomic inconsistency — `pg_trickle.` vs `pgtrickle.`

- GUCs use `pg_trickle.*` (e.g. `pg_trickle.enabled`).
- Schema and functions use `pgtrickle.*` (e.g. `pgtrickle.health_summary()`).
- Notification channel was fixed in v0.19 (`pgtrickle_refresh` → `pg_trickle_refresh`),
  but the channel is the *one* place that now uses an underscore where
  the schema does not.

**Recommendation:** document the rule explicitly in `SQL_REFERENCE.md`:
"GUC prefix = `pg_trickle.` (PG convention); everything else = `pgtrickle`."
Do not change either; both shipped. Just clarify.

### 5.2 Missing API — No `pgtrickle.pause_all()` / `resume_all()`

Operators routinely need to suspend all STs during maintenance (e.g.
pg_dump of source tables). Today this is N calls to `alter_stream_table`.
A single wrapper is trivial and high-value.

### 5.3 Missing API — No `pgtrickle.refresh_if_stale(name, max_age)`

Application-level staleness gating is a common pattern. The building
blocks exist (`data_timestamp`, `refresh_stream_table`); the wrapper is
five lines.

### 5.4 Missing API — No `pgtrickle.stream_table_definition(name)`

For auditing and blue-green migrations users want to fetch the exact
`original_query` + `refresh_mode` + `schedule` in one row. Today they
must join three catalog tables.

### 5.5 Error messages — Good after v0.18

v0.18's error-code / DETAIL / HINT pass is thorough and should be
preserved as a contract — worth adding an `ERRORS.md` gate in CI that
fails if any new `ereport!`/`error!` lacks a hint.

---

## 6. Findings — Testing & Reliability

### 6.1 Gap — Three large files without any unit tests

| File | LOC | Tests |
|---|---|---|
| [src/api/helpers.rs](../src/api/helpers.rs) | 2,527 | 0 |
| [src/api/diagnostics.rs](../src/api/diagnostics.rs) | 1,452 | 0 |
| [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs) | 5,918 | 0 |

All three are on the SQL parsing / API orchestration path. Parser
rewrites in particular are deeply nested pure functions — ideal unit
test targets.

**Recommendation:** budget a two-day campaign to land 50+ unit tests
covering each rewrite pass (view inlining, DISTINCT ON, GROUPING SETS,
scalar subquery in WHERE, correlated SSQ in SELECT, SubLinks in OR,
multi-PARTITION BY windows).

> **→ Roadmap:** [v0.21.0 §Test Coverage](../roadmap/v0.21.0.md-full.md) — items TEST-1, TEST-2, TEST-3.

### 6.2 Gap — No fuzz target for the parser

`cargo-fuzz` is not configured. Given that `PLAN_NON_DETERMINISM` is
*Not started* and the parser is 18k LOC, a week of differential
fuzzing (pg_trickle parser vs. plain `SELECT`) would likely uncover
rejection bugs and panic-on-malformed cases.

> **→ Roadmap:** [v0.21.0 §Test Coverage](../roadmap/v0.21.0.md-full.md) — item TEST-4.

### 6.3 Gap — No kill-switch / crash-recovery test for bgworker

`resilience_tests.rs` is a single file (~1). A chaos-style test that
`pg_ctl stop -m immediate` mid-refresh and verifies:

- No unfinalised `pgt_refresh_history` entries.
- WAL decoder advances from the last confirmed_lsn on restart.
- Change buffer state is consistent.

…would close a class of "what happens if…" questions.

> **→ Roadmap:** [v0.21.0 §Test Coverage](../roadmap/v0.21.0.md-full.md) — item TEST-5.

### 6.4 Gap — TPC-H IMMEDIATE mode correctness still gated by allowlist

The skip-allowlist in
[tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs) L92–L104 already
excludes q05/q07/q08/q09 (join size). Repo memory identifies q15 as
*also* failing under IMMEDIATE mode but *not* in the allowlist — either
add it (test hygiene) or fix the underlying EC-01 issue (correctness).

> **→ Roadmap:** [v0.21.0 §EC-01 Fix](../roadmap/v0.21.0.md-full.md) — item EC01-0 (Q15 stop-gap) and EC01-3 (permanent fix).

### 6.5 Strength — Property tests + TPC-H nightly + SQLancer

`proptest` regressions are committed
([proptest-regressions/](../proptest-regressions/)), TPC-H nightly runs
([.github/workflows/tpch-nightly.yml](../.github/workflows/tpch-nightly.yml) per commit `da0e5ef`),
SQLancer driver is in [scripts/run_sqlancer.sh](../scripts/run_sqlancer.sh).
Coverage is strong.

---

## 7. Findings — Documentation & Usability

### 7.1 Strength — `docs/` is exceptional

25+ structured markdown files, every new feature in v0.18–v0.20
shipped with doc updates, dedicated troubleshooting / getting-started /
upgrading / benchmarking guides.

### 7.2 Gap — Missing "Performance Tuning Cookbook"

`docs/SCALING.md` covers knob selection; a companion "Cookbook" with
*symptom → likely cause → GUC to tune → measurement* rows would round
out the operator experience. Most of the content already exists
distributed across FAQ / TROUBLESHOOTING.

> **→ Roadmap:** [v0.21.0 §Documentation](../roadmap/v0.21.0.md-full.md) — item DOC-1.

### 7.3 Gap — Dog-feeding recipes are new; examples light

v0.20 introduced `setup_self_monitoring()` but the examples mostly show
the setup line itself. A "day in the life" walkthrough — ingest some
data, wait, read `df_threshold_advice`, apply, observe — would lift
adoption.

### 7.4 Gap — No migration guide between refresh modes

Switching a stream table from `FULL` to `DIFFERENTIAL` mid-life is
supported (`alter_stream_table`) but the safety implications
(watermark reset, first-cycle full rebuild) are documented only in
passing in `SQL_REFERENCE.md`.

---

## 8. Findings — Operational Aspects

### 8.1 Strength — v0.19 schema_version table

`pgtrickle.pgt_schema_version` and the
`check_upgrade_completeness.sh` gate close a real gap; upgrade
completeness is now a CI precondition, not a release-time manual step.

### 8.2 Gap — No downgrade / rollback SQL scripts

Only forward `sql/pg_trickle--X--Y.sql` exists. Users who roll back
(e.g. v0.20 → v0.19 after a bad deploy) must do so manually. A
policy decision (we support forward-only) should be documented, or
reverse migrations should be generated from the upgrade script.

### 8.3 Gap — No Prometheus exporter binary

`docs/BENCHMARK.md` and `monitoring/grafana/` describe a Grafana
dashboard, but Prometheus consumes metrics either via
`pg_stat_statements` + textfile or via an external exporter. A tiny
HTTP endpoint on a bgworker exposing OpenMetrics
(`/metrics`) would remove the "bring your own exporter" hurdle.

> **→ Roadmap:** [v0.21.0 §Operational Features](../roadmap/v0.21.0.md-full.md) — item OP-2.

### 8.4 Gap — No canary / shadow mode

When changing a defining query via `alter_stream_table`, the stream
table is rebuilt and exposed immediately. A "shadow" mode that writes
to a hidden sibling table and diffs against the live table would make
migrations of critical STs safer.

> **→ Roadmap:** [v0.21.0 §Operational Features](../roadmap/v0.21.0.md-full.md) — item OPS-1.

### 8.5 Gap — `explain_dag()` is Mermaid/DOT; no runtime profile

v0.20 renders the DAG topology. The natural next step is to overlay
per-node *refresh latency* and *change volume* so operators can see
where time is being spent at a glance. Data already exists in
`pgt_refresh_history`.

> **→ Roadmap:** [v0.23.0 §CLI Visualization Polish](../roadmap/v0.23.0.md-full.md) — item OP-1.

---

## 9. New Feature Proposals

Ordered by expected value-to-effort ratio. All include the rationale
and a minimal viable slice.

### 9.1 `pgtrickle.refresh_if_stale(name text, max_age interval)` (XS effort — Days)

Trivial wrapper on existing building blocks. Unblocks every
application that wants "refresh-on-read with a grace period" semantics
without writing their own procedural code. Also useful in dbt hooks.

> **→ Roadmap:** [v0.21.0 §Operational Features](../roadmap/v0.21.0.md-full.md) — item OP-4.

### 9.2 Downstream CDC publication — `stream_table_to_publication()` (M effort — 1–2 weeks)

Emit changes applied to each stream table as a PostgreSQL logical
replication publication so downstream consumers (Kafka Connect,
Debezium, pgpool) subscribe with zero code. Builds on the existing
MERGE output (`pgt_inserted_rows` / `pgt_deleted_rows`) — no new
capture layer needed.

**Value:** unlocks streaming-to-Kafka / event sourcing use cases that
are currently blocked on users setting up a second slot.

> **→ Roadmap:** [v0.22.0 §Downstream CDC Publication](../roadmap/v0.22.0.md-full.md) — items CDC-PUB-1 through CDC-PUB-5.

### 9.3 Predictive refresh cost model (M effort — 1–2 weeks)

Extend self-monitoring to forecast `duration_ms` from
`rows_inserted + rows_deleted` using linear regression over the last
hour. When the forecast exceeds `last_full_ms × 1.5`, pre-emptively
switch to FULL. Keeps adaptive fallback from *reacting* to a slow
diff and instead *predicts* it.

> **→ Roadmap:** [v0.22.0 §Predictive Refresh Cost Model](../roadmap/v0.22.0.md-full.md) — items PRED-1 through PRED-4.

### 9.4 Stream-table checkpoint / snapshot restore (M effort — 2–3 weeks)

Persist `(pgt_id, frontier, content_hash)` tuples to an archival
table so a newly-joined replica can bootstrap from a `pg_basebackup`
+ replay instead of full recomputation. Complements the existing
`restore_stream_tables()` function.

### 9.5 Shadow / canary mode for `alter_stream_table` (S effort — 1 week)

Optional `dry_run_shadow => true` parameter: the new defining query
is materialised into `pgt_shadow_<name>` on the same schedule; a
diff function (`pgtrickle.canary_diff(name)`) compares against the
live table. Flip atomically when confident.

> **→ Roadmap:** [v0.21.0 §Operational Features](../roadmap/v0.21.0.md-full.md) — item OPS-1.

### 9.6 Prometheus HTTP endpoint in bgworker (S effort — 1 week)

Tiny `tiny_http` (or `hyper`) server bound to a port configured via
`pg_trickle.metrics_port` (default `0` = off). Emits all existing
monitor metrics in OpenMetrics format. No sidecar needed.

> **→ Roadmap:** [v0.21.0 §Operational Features](../roadmap/v0.21.0.md-full.md) — item OP-2.

### 9.7 SLA-driven tier auto-assignment (M effort — 1–2 weeks)

`alter_stream_table(name, sla => interval '30 seconds')` — the
scheduler assigns the ST to the tier whose worst-case dispatch gap is
≤ SLA, considering current queue depth. Makes the tiered scheduler
usable without operator expertise.

> **→ Roadmap:** [v0.22.0 §SLA-Driven Tier Auto-Assignment](../roadmap/v0.22.0.md-full.md) — items SLA-1 through SLA-4.

### 9.8 Native streaming SQL dialect — `CREATE STREAM TABLE … WATERMARK …` (L effort — 4–6 weeks)

Already drafted in [PLAN_NATIVE_SYNTAX.md](sql/PLAN_NATIVE_SYNTAX.md).
Customers asking for "Materialize-compatible syntax" get a familiar
API; the function layer remains as a compatibility shim.

> **→ Roadmap:** Post-1.0 — [PG Backward Compatibility & Native DDL](../roadmap/v1.1.0.md-full.md) (deferred; large scope).

### 9.9 pgvector stream-table operator (M effort — 2–3 weeks)

Support `ORDER BY embedding <-> $1 LIMIT k` via the existing TopK path
plus an HNSW index maintained as a side effect. Opens RAG / semantic
search use cases where the "source of truth" is a pg_trickle ST.

### 9.10 Auto-denormalisation advisor (M effort — 2 weeks)

Analyse `pg_stat_statements` for expensive join queries and suggest
a stream table that would materialise them incrementally. Surfaces as
`pgtrickle.suggest_stream_tables()`. Pure analytical function —
zero runtime risk.

### 9.11 DAG node runtime overlay in `explain_dag()` (XS effort — 2 days)

Colour each node by *p95 latency* and width by *rows per refresh*
using existing `pgt_refresh_history` data. One SQL aggregate plus a
Mermaid template tweak.

> **→ Roadmap:** [v0.23.0 §CLI Visualization Polish](../roadmap/v0.23.0.md-full.md) — item OP-1.

### 9.12 Transactional outbox helper (S effort — 1 week)

Already planned in
[plans/patterns/](patterns/). Small, high-leverage helper: a table
`pgtrickle.outbox_<st>` receives a row per refresh cycle with JSON
payload `{inserted:[…], deleted:[…]}`. Eliminates the dual-write
problem for downstream event buses.

> **→ Roadmap:** [v0.28.0 §Transactional Outbox Helper](../roadmap/v0.28.0.md-full.md) — items OUTBOX-1 through OUTBOX-4.

---

## 10. Prioritised Action Plan

**Legend:** Impact (1–5) × Effort (S=1, M=2, L=3) ⇒ Priority.

### P0 — Land before v1.0

| # | Item | Kind | Impact | Effort | Notes | Milestone |
|---|---|---|---|---|---|---|
| 1 | Fix EC-01 Part 1 row-id hash (single-left-pk hash OR pre-image reconstruction) | Bug | 5 | L | §2.1 — correctness gate for TPC-H Q07/Q15 | [v0.21.0 EC01-1/2](../roadmap/v0.21.0.md-full.md) |
| 2 | Reject or warn on volatile functions in defining query | Bug | 4 | S | §2.6 — silent wrong-answer risk | [v0.21.0 OP-6](../roadmap/v0.21.0.md-full.md) |
| 3 | Add Q15 to `IMMEDIATE_SKIP_ALLOWLIST` (interim) | Test | 3 | XS | §6.4 — until #1 ships | [v0.21.0 EC01-0](../roadmap/v0.21.0.md-full.md) |
| 4 | Convert remaining production `.unwrap()` sites in `sublinks.rs` to `?` | Bug | 3 | S | §2.2 — 28 sites | [v0.21.0 SAF-1](../roadmap/v0.21.0.md-full.md) |

### P1 — Next two releases

| # | Item | Kind | Impact | Effort | Notes | Milestone |
|---|---|---|---|---|---|---|
| 5 | Shard `refresh.rs` (8.4k LOC) into 4 submodules | Arch | 4 | M | §2.4 | [v0.21.0 ARCH-1](../roadmap/v0.21.0.md-full.md) |
| 6 | `PLAN_REDUCED_UNSAFE` half-pass (list helpers + façades) | Safety | 4 | M | §2.3 | [v0.21.0 SAF-2](../roadmap/v0.21.0.md-full.md) |
| 7 | Prometheus HTTP endpoint in bgworker | Ops | 4 | S | §9.6 | [v0.21.0 OP-2](../roadmap/v0.21.0.md-full.md) |
| 8 | Downstream CDC publication (`stream_table_to_publication`) | Feature | 5 | M | §9.2 | [v0.22.0 CDC-PUB](../roadmap/v0.22.0.md-full.md) |
| 9 | Unit-test campaign for `parser/rewrites.rs` + `api/helpers.rs` + `api/diagnostics.rs` | Test | 3 | M | §6.1 | [v0.21.0 TEST-1/2/3](../roadmap/v0.21.0.md-full.md) |
| 10 | Parser fuzz target (`cargo-fuzz`) | Test | 3 | S | §6.2 | [v0.21.0 TEST-4](../roadmap/v0.21.0.md-full.md) |
| 11 | In-database parallel refresh pool (PLAN_PARALLELISM minimal slice) | Arch | 5 | L | §3.1 | [v0.22.0 PAR](../roadmap/v0.22.0.md-full.md) |

### P2 — Opportunistic

| # | Item | Kind | Impact | Effort | Milestone |
|---|---|---|---|---|---|
| 12 | `refresh_if_stale`, `pause_all`, `resume_all`, `stream_table_definition` | API | 2 | XS | [v0.21.0 OP-3/4/5](../roadmap/v0.21.0.md-full.md) |
| 13 | Predictive refresh cost model | Feature | 3 | M | [v0.22.0 PRED](../roadmap/v0.22.0.md-full.md) |
| 14 | Shared-memory template cache | Perf | 3 | M | Post-1.0 |
| 15 | Shadow/canary `alter_stream_table` | Ops | 3 | S | [v0.21.0 OPS-1](../roadmap/v0.21.0.md-full.md) |
| 16 | DAG runtime overlay in `explain_dag()` | Ops | 2 | XS | [v0.23.0 OP-1](../roadmap/v0.23.0.md-full.md) |
| 17 | SLA-driven tier assignment | Feature | 3 | M | [v0.22.0 SLA](../roadmap/v0.22.0.md-full.md) |
| 18 | Stream-table checkpoint | Feature | 3 | M | Post-1.0 |
| 19 | Native `CREATE STREAM TABLE` syntax | Feature | 4 | L | Post-1.0 |
| 20 | pgvector TopK support | Feature | 3 | M | Post-1.0 |
| 21 | Auto-denormalisation advisor | Feature | 2 | M | Post-1.0 |
| 22 | Transactional outbox helper | Feature | 3 | S | [v0.28.0 OUTBOX](../roadmap/v0.28.0.md-full.md) |
| 23 | Performance Tuning Cookbook doc | Docs | 2 | S | [v0.21.0 DOC-1](../roadmap/v0.21.0.md-full.md) |
| 24 | Chaos/crash-recovery test for bgworker | Test | 3 | M | [v0.21.0 TEST-5](../roadmap/v0.21.0.md-full.md) |

### P3 — Track, don't build yet

| # | Item | Notes | Milestone |
|---|---|---|---|
| 25 | Multi-database support (PLAN_MULTI_DATABASE Draft) | Scope is large; stays Draft until a customer demand forces prioritisation | Post-1.0 S3 |
| 26 | Recursive CTE DRed (P2-1 from PLAN.md) | Documented fallback; instrument, don't build | [v0.21.0 ARCH-2](../roadmap/v0.21.0.md-full.md) (observability only) |
| 27 | Downgrade SQL scripts | Policy decision first (support forward-only?) | Post-1.0 |

---

## 11. Appendix — Repository Metrics

### Code volume (2026-04-17)

- **Source (`src/`):** 54 files, ~102,118 LOC.
- **Tests (`tests/`):** 134 integration files; 108 tagged `e2e_*`.
- **In-crate tests:** 1,855 `#[test]` / `#[pg_test]` functions.
- **Plans:** 100+ markdown files under [plans/](.).
- **Docs:** 25+ structured files under [docs/](../docs/).

### Hot files by line count

```
src/refresh.rs                    8,451
src/scheduler.rs                  6,706
src/dvm/parser/sublinks.rs        6,361
src/api/mod.rs                    6,009
src/dvm/parser/rewrites.rs        5,918
src/dvm/parser/mod.rs             5,495
src/dvm/operators/aggregate.rs    5,399
src/dag.rs                        4,288
src/dvm/operators/recursive_cte.rs 4,041
src/cdc.rs                        3,535
src/monitor.rs                    3,239
src/dvm/parser/types.rs           3,082
src/api/helpers.rs                2,527
src/config.rs                     2,493
src/wal_decoder.rs                2,117
```

### Risk-pattern scan

| Pattern | Count | Notes |
|---|---|---|
| `.unwrap()` (all) | 513 | Mostly in `mod tests` |
| `.unwrap()` (production) | ~34 | Most in parser on PG-list invariants (§2.2) |
| `panic!` / `unreachable!` / `todo!` | 51 | Most are `unreachable!` in exhaustive matches |
| `unsafe` blocks | 547 | Largely pgrx FFI; no reduction plan in flight (§2.3) |
| `TODO`/`FIXME`/`HACK`/`XXX` | 1 | Excellent hygiene |

### Recent release cadence

| Tag | Date | Major theme |
|---|---|---|
| v0.20.0 | 2026-04-? | Dog-feeding (self-monitoring STs) |
| v0.19.0 | 2026-04-13 | Prod gap closure; 10–15× scheduler speedup |
| v0.18.0 | 2026-04-12 | NULL-in-GROUP-BY fix; delta memory safety net |
| v0.17.0 | 2026-04-08 | Query intelligence & stability |
| v0.16.0 | 2026-04-06 | Performance & refresh optimisation |

Release cadence is weekly — healthy but also means each window has to
be carefully scoped. Backporting fixes to two releases back (v0.18,
v0.19) is practical given the upgrade test matrix.

---

## References

- [AGENTS.md](../AGENTS.md) — project conventions
- [ROADMAP.md](../ROADMAP.md) — canonical roadmap
- [PLAN.md](PLAN.md) — master implementation plan
- [PLAN_EDGE_CASES.md](PLAN_EDGE_CASES.md) — edge-case register (EC-01…)
- [PLAN_REDUCED_UNSAFE.md](safety/PLAN_REDUCED_UNSAFE.md)
- [PLAN_PARALLELISM.md](sql/PLAN_PARALLELISM.md)
- [PLAN_NON_DETERMINISM.md](sql/PLAN_NON_DETERMINISM.md)
- [PLAN_NATIVE_SYNTAX.md](sql/PLAN_NATIVE_SYNTAX.md)
- [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md)
- Repo memory notes: `join-delta-phantom-rows`, `df-cdc-buffer-trends-fix`
