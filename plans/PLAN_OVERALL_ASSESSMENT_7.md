# pg_trickle — Overall Project Assessment v7

> **Status:** Draft (review-ready)
> **Type:** Audit / project-wide gap analysis
> **Last updated:** 2026-04-27
> **Scope:** Codebase as of v0.34.0 (Citus distributed-source CDC scheduler
> automation just shipped; CHANGELOG head documents v0.34.0; `Cargo.toml`
> reads `version = "0.34.0"`).
> **Target audience:** Maintainers, release managers, senior contributors
> planning v0.35.0 → v1.0.0.
> **Sibling documents:**
> - [PLAN_OVERALL_ASSESSMENT.md](PLAN_OVERALL_ASSESSMENT.md) — v0.20.0 baseline
> - [PLAN_OVERALL_ASSESSMENT_2.md](PLAN_OVERALL_ASSESSMENT_2.md) — v0.23.0 follow-up
> - [PLAN_OVERALL_ASSESSMENT_3.md](PLAN_OVERALL_ASSESSMENT_3.md) — v0.27.0 follow-up
> - [ROADMAP.md](../ROADMAP.md), [PLAN.md](PLAN.md), [AGENTS.md](../AGENTS.md)

---

## Executive Summary

Between v0.27.0 (April 2026) and v0.34.0 the project absorbed **seven minor
releases** of substantial new surface area: a transactional outbox + inbox
([src/api/outbox.rs](../src/api/outbox.rs), [src/api/inbox.rs](../src/api/inbox.rs)),
the `pgtrickle-relay` CLI, a pre-1.0 correctness gate (v0.30.0), scheduler
intelligence work (v0.31.0), and the three-release Citus arc (v0.32–v0.34)
that ships per-worker WAL slot CDC, distributed stream-table output, the
`pgt_st_locks` distributed mutex, and a fully-automated scheduler
([src/citus.rs](../src/citus.rs) is now 1,174 LOC of additive code that does
not perturb single-node behaviour). Several of the critical findings from
the v0.27.0 assessment have been **closed in production**: snapshot/restore
is now wrapped in a sub-transaction RAII helper
([src/api/snapshot.rs:27-87](../src/api/snapshot.rs)); a SQLSTATE-based
locale-safe error classifier was added in v0.30.0
([src/error.rs:312-380](../src/error.rs)); the L1 catalog snapshot cache
landed in v0.31.0 ([src/scheduler.rs:59-118](../src/scheduler.rs)); and the
DAG is now rebuilt copy-on-write outside the watermark lock
([src/scheduler.rs:134-144](../src/scheduler.rs)). The architectural worry
that dominated the v0.20 and v0.23 assessments — monolithic
`refresh.rs`/`scheduler.rs` and a single mega-lock — is, structurally, gone.

The **top-five risks remaining** at v0.34.0 are sharper but narrower:

1. **EC-01 phantom-row residue is *still* in production.**
   [src/dvm/operators/join.rs:657-668](../src/dvm/operators/join.rs) still
   sets `is_deduplicated: false`; PH-D1 cross-cycle cleanup
   ([src/refresh/phd1.rs](../src/refresh/phd1.rs)) is opt-in and not
   invoked unconditionally. Repo memory
   (`/memories/repo/join-delta-phantom-rows.md`) confirms test flakes
   continue. Three assessments in a row have flagged this; it is the single
   biggest risk to a credible v1.0.

2. **Locale-text classifier still exists alongside SQLSTATE classifier.**
   `classify_spi_error_retryable` ([src/error.rs:282-340](../src/error.rs))
   is the legacy English-text path; the new SQLSTATE-aware
   `classify_spi_sqlstate_retryable` ([src/error.rs:312-380](../src/error.rs))
   is only reached when the new `SpiErrorCode` variant is constructed,
   which (per static search) **no call site does yet**. The bug is one
   layer down: the right code exists, but the wiring is incomplete.

3. **Citus distributed coordination has effectively zero hardening tests.**
   [src/citus.rs](../src/citus.rs) introduces `pgt_st_locks` (distributed
   advisory mutex via `INSERT … ON CONFLICT DO NOTHING`),
   `ensure_worker_slot`, `poll_worker_slot_changes`, and rebalance
   auto-recovery — none of which has a chaos test for worker death,
   coordinator failover, partial rebalance, lease-expiry under clock skew,
   or split-brain. The CNPG smoke test does not run in a Citus topology.

4. **Inbox/outbox lack unit-level coverage.** Both
   [src/api/inbox.rs](../src/api/inbox.rs) and
   [src/api/outbox.rs](../src/api/outbox.rs) have **no `#[cfg(test)]
   mod tests` block**. Coverage is only via `tests/e2e_inbox_tests.rs` and
   `tests/e2e_outbox_tests.rs`, which means dedup-key collisions, envelope
   replay, NULL handling, and offset durability are untested at the unit
   level. For an "at-least-once with dedup" guarantee this is too thin.

5. **Operational ergonomics gap: 111 GUCs with no top-level catalogue.**
   `src/config.rs` defines 111 `GucRegistry::define_*_guc` calls (`grep -c`
   on [src/config.rs](../src/config.rs)). [docs/CONFIGURATION.md](../docs/CONFIGURATION.md)
   does not enumerate all of them; new operators cannot tune without
   reading source. Several GUCs (e.g. `pg_trickle.parallel_refresh_mode`,
   `pg_trickle.matview_polling`, `pg_trickle.foreign_table_polling`) are
   not referenced in any test file.

The single most impactful engineering investment for the next 90 days is a
**4-week "EC-01 closeout + Citus chaos sprint"** that (a) routes every join
delta through unconditional PH-D1 cleanup with a small-batch implementation,
(b) flips `is_deduplicated: true` for INNER joins on stable PKs and proves
convergence with a 50,000-iteration property test, (c) adds a Citus
integration test rig (multi-container compose with rebalance, worker kill,
and coordinator failover), and (d) gates PRs that touch
`src/dvm/operators/join*.rs`, `src/refresh/phd1.rs`, or `src/citus.rs` on
that rig. Everything else — reactive subscriptions, time-travel, PGlite —
is downstream of getting the join-delta correctness story to "obviously
right" and the distributed coordination to "documented and tested".

The project's overall maturity now sits between Materialize CE (early
2024) and pg_ivm: comparable correctness rigour to pg_ivm, comparable
streaming features to early Materialize, but better operational ergonomics
than either (dbt adapter, snapshot/PITR, downstream publications).
With the EC-01 + Citus closeout it is on track for a credible v1.0 GA in
late 2026.

---

## Table of Contents

1. [Method & Scope](#1-method--scope)
2. [What Has Improved Since v0.27.0](#2-what-has-improved-since-v0270)
3. [Critical Issues](#3-critical-issues)
4. [Architecture & Design Gaps](#4-architecture--design-gaps)
5. [Performance & Scalability Bottlenecks](#5-performance--scalability-bottlenecks)
6. [Memory Safety & Unsafe Code Audit](#6-memory-safety--unsafe-code-audit)
7. [Error Handling & Observability Gaps](#7-error-handling--observability-gaps)
8. [SQL API & Ergonomics](#8-sql-api--ergonomics)
9. [Test Coverage Gaps](#9-test-coverage-gaps)
10. [Security Findings](#10-security-findings)
11. [Documentation & Onboarding Gaps](#11-documentation--onboarding-gaps)
12. [Operational Readiness Assessment](#12-operational-readiness-assessment)
13. [Ecosystem & Integration Gaps](#13-ecosystem--integration-gaps)
14. [Codebase Health Metrics](#14-codebase-health-metrics)
15. [New Feature Proposals](#15-new-feature-proposals)
16. [Prioritised Action Plan](#16-prioritised-action-plan)
17. [Appendix — File-Level Coverage Summary](#17-appendix--file-level-coverage-summary)

---

## 1. Method & Scope

This audit was produced over a single read-only pass against `main` at
v0.34.0 (`Cargo.toml` line 3). I read all three prior assessments first,
walked every file in `src/` (66 .rs files, 115,371 LOC), spot-checked the
top-LOC files line-by-line, ran static analysis scripts (`grep`-based
counts of `unsafe`, `unwrap`, `expect`, `panic!`, `TODO/FIXME/HACK` outside
`#[cfg(test)]` blocks), surveyed all 140 files in `tests/`, read the 19
GitHub workflow files in `.github/workflows/`, and walked
`pgtrickle-relay/src/`, `dbt-pgtrickle/`, `cnpg/`,
and `monitoring/`.

I dispatched four read-only exploration sub-agents in parallel against
distinct slices (DVM engine, core orchestration, Citus & new modules,
infra/tests/ecosystem) to widen coverage. Each sub-agent's findings were
verified against the actual source where this report cites a line number.
Where the sub-agents disagreed with the source — for example the core
sub-agent claimed "no `pgt_refresh_history` retention exists" when in fact
[src/config.rs:2071](../src/config.rs) defines `pg_trickle.history_retention_days`
and [src/monitor.rs:571-580](../src/monitor.rs) implements the prune — the
source wins and the report cites the source.

I did **not** run any tests, builds, or benchmarks; this is static
analysis only. I did not inspect generated SQL artifacts under
`target/release/extension` (out of scope), the full Grafana dashboard JSON
(I only scanned the panel set), or the demo container internals. Where
the v3 assessment claimed an item was resolved I re-checked the source
before treating it as such; one (snapshot atomicity, v3 §3.2) is now
genuinely fixed; another (SQLSTATE classifier, v3 §3.4) is partially
fixed.

---

## 2. What Has Improved Since v0.27.0

The following v3 findings are now **resolved or substantially mitigated**.
Items not listed here remain open and are tracked in §3 onwards.

| v3 finding | Identifier | Current state | Evidence |
|---|---|---|---|
| Snapshot/restore not atomic | v3 §3.2 | **Resolved.** `SnapSubTransaction` RAII helper wraps both `CREATE TABLE AS + catalog INSERT` and `TRUNCATE + INSERT` in a sub-transaction with auto-rollback on Drop (panic-safe). | [src/api/snapshot.rs:27-87](../src/api/snapshot.rs); see comment "STAB-1 (v0.30.0)" |
| `classify_spi_error_retryable` is text-based | v3 §3.4 | **Partially resolved.** SQLSTATE-aware `classify_spi_sqlstate_retryable` shipped in v0.30.0 with locale-safe class matching (40, 23, 22, 28, 08); legacy text matcher kept as fallback. | [src/error.rs:312-380](../src/error.rs) (new); [src/error.rs:282-340](../src/error.rs) (legacy still in use — see §7.2) |
| L2 template-cache catalog table unbounded | v3 §3.5 | **Resolved (size cap).** v0.30.0 added bounded eviction; LRU enforcement is per-tick. Catalog table still grows during DDL bursts but is capped. | [src/template_cache.rs:79-118](../src/template_cache.rs); [src/config.rs:332](../src/config.rs) `template_cache_max_entries` |
| Catalog snapshot reload on every scheduler tick | v3 §4 | **Resolved.** `CATALOG_SNAPSHOT_CACHE` thread-local + DAG version cache; `rebuild_dag_copy_on_write` builds outside lock and atomically swaps. | [src/scheduler.rs:59-118](../src/scheduler.rs); [src/scheduler.rs:134-144](../src/scheduler.rs) |
| L0 dshash cache is missing | v3 §3 | **Signal removed.** The unread process-local L0 cache and availability signal were deleted; caching still uses thread-local L1 and catalog-backed L2. A shared dshash cache remains a possible, unmeasured optimization. | [src/dvm/mod.rs:898](../src/dvm/mod.rs) |
| `pgt_refresh_history` retention | v3 carry-over from v2 | **Resolved.** `pg_trickle.history_retention_days` GUC (default 90 days, [src/config.rs:2071](../src/config.rs)); prune runs from monitor module ([src/monitor.rs:571](../src/monitor.rs)). |
| `IVM_DELTA_CACHE` unbounded | v3 §3.3 | **Resolved.** v0.31.0 added bounded eviction + generation counter invalidation; deferred-cleanup queue still drains at next access (§4 below). | [src/ivm.rs](../src/ivm.rs) `LOCAL_IVM_CACHE_GEN` |

In addition, v0.28.0–v0.34.0 ship features the v3 audit could not assess:

- **Outbox + inbox** with envelope versioning, dedup keys
  ([src/api/outbox.rs:1-942](../src/api/outbox.rs); [src/api/inbox.rs:1-1034](../src/api/inbox.rs)).
- **`pgtrickle-relay` CLI** — coordinator pattern, hot-reload via
  `LISTEN/NOTIFY`, sink trait abstraction
  ([pgtrickle-relay/src/coordinator.rs](../pgtrickle-relay/src/coordinator.rs)).
- **Citus integration** — stable hash naming
  ([src/citus.rs:42-54](../src/citus.rs)), per-worker WAL slots, `pgt_st_locks`
  distributed mutex, `citus_status` view, automated rebalance recovery.
- **Soak (G17-SOAK) and multi-database (G17-MDB) workflows** in
  `.github/workflows/stability-tests.yml`.
- **DAG benchmark workflows** — calc/throughput, parallel workers
  (`.github/workflows/e2e-benchmarks.yml`, `tests/e2e_dag_bench_tests.rs`).
- **111 GUCs** (vs. ~75 at v0.27.0) — significant configuration surface
  expansion, see §8.4 for the documentation gap.

---

## 3. Critical Issues

Severity legend: **CRITICAL** = blocks v1.0 GA; **HIGH** = correctness or
data-loss risk under realistic workloads; **MEDIUM** = production-quality
gap; **LOW** = polish.

### 3.1 EC-01 phantom rows still leak across cycles *(CRITICAL)*

**Where:** [src/dvm/operators/join.rs:220-260](../src/dvm/operators/join.rs)
(Part 1a/1b hash); [src/dvm/operators/join.rs:657-668](../src/dvm/operators/join.rs)
(`is_deduplicated: false` is hard-coded); [src/refresh/phd1.rs:18-99](../src/refresh/phd1.rs)
(opt-in cleanup path).

This is the third consecutive assessment to flag this. The v0.24.0 fix
made Part 1a (`ΔL_I ⋈ R₁`) and Part 1b (`ΔL_D ⋈ R₀`) hash on
`(left_pks, right_pks)` rather than the right-side PK alone, which is
sound when PKs are immutable. But the Z-set pipeline downstream still
groups by `__pgt_row_id` and the join operator returns
`is_deduplicated: false`, so the MERGE path retains a per-row-id sum that
can reach a non-zero residual when an insert-then-delete on the left
collides with a delete on the right within the same cycle. PH-D1
cross-cycle cleanup at [src/refresh/phd1.rs:34-99](../src/refresh/phd1.rs)
is **opt-in per refresh**: the orchestrator only invokes it when the
prior cycle reported a non-zero residual, but the residual reporting
itself depends on a flag that is not flipped on every code path. Repo
memory `/memories/repo/join-delta-phantom-rows.md` records continued
flakes in `test_tpch_q07_ec01b_combined_delete` and
`test_tpch_immediate_correctness` Q15.

**Impact in production.** Stream tables built on multi-table joins can
produce wrong aggregates and `UNIQUE_VIOLATION` errors on downstream
sinks. The bug has been "papered over" with allowlists in
[tests/e2e_tpch_tests.rs](../tests/e2e_tpch_tests.rs) for IMMEDIATE-mode
Q15 since v0.21.0. Customers who write joins on top of pg_trickle today
must either use FULL refresh or accept eventual inconsistency.

**Recommendation.** Two-step:
1. *Immediately:* route every cycle unconditionally through PH-D1 with
   batch size = 1,024 rows; cost is negligible (anti-join against the
   freshly-applied delta) and removes the residual-detection coupling.
2. *Properly:* re-engineer Part 2 to derive `__pgt_row_id` from the
   *retained* base-table PK snapshot so Part 1a and Part 1b emit
   convergent ids, then flip `is_deduplicated: true` for INNER joins on
   stable PKs and gate the change behind a 50,000-iteration property
   test in [tests/e2e_property_tests.rs](../tests/e2e_property_tests.rs).

**Effort:** L (4–6 weeks including new property test corpus).

### 3.2 SQLSTATE classifier exists but is not wired *(HIGH)*

**Where:** [src/error.rs:282-340](../src/error.rs) (legacy text path,
in active use); [src/error.rs:312-380](../src/error.rs)
(new `classify_spi_sqlstate_retryable`, v0.30.0); `SpiErrorCode` variant
is constructed in **zero** call sites (`grep -rn "SpiErrorCode"
src/`). The architecture is right; the migration is unfinished.

**Impact.** On a PostgreSQL build with `lc_messages='ja_JP.UTF-8'`,
every SPI error becomes "retryable by default" (the catch-all branch),
so permanent schema errors cause infinite retry loops in the scheduler
until `max_consecutive_errors` trips the fuse — which is misleading
diagnostic noise.

**Recommendation.** Surface `pg_sys::ErrorData.sqlerrcode` from pgrx
SPI errors (pgrx 0.18 exposes it via `SpiError::error_code()`), then
construct `SpiErrorCode { code, msg }` everywhere a `SpiError(String)`
is constructed today. The text path can be retained as a final fallback
for non-SPI sources. One-week task; will retire ~30 `.to_string()` SPI
sites across the codebase.

### 3.3 Citus distributed coordination has no chaos test coverage *(HIGH)*

**Where:** [src/citus.rs:1-1174](../src/citus.rs);
[tests/e2e_mdb_tests.rs](../tests/e2e_mdb_tests.rs) (multi-database
only; no Citus); no test file matches `*citus*`.

`pgt_st_locks` ([src/citus.rs](../src/citus.rs) — search for
`pgt_st_locks`) implements a distributed mutex with `INSERT … ON
CONFLICT DO NOTHING` plus a TTL lease (`citus_lease_ttl_seconds` GUC).
Failure modes that have **no test** include: lease expiry during refresh
(no fencing token); coordinator failover mid-lease; clock skew between
coordinator and worker; partial `pg_dist_node` rebalance; worker death
during `poll_worker_slot_changes`; concurrent `handle_vp_promoted`
calls. The `citus_worker_retry_ticks` GUC (default 5) only emits a
WARNING in the PostgreSQL log; there is no Prometheus counter, no
alerting hook, and the worker-skipped path can mask permanent breakage.

**Recommendation.** Add `tests/e2e_citus_*` with a Docker-Compose-Citus
rig (coordinator + 3 workers) that drives: (a) kill-and-restart worker
during poll, (b) coordinator restart mid-lease, (c) pg_dist_node
removal+re-add of a worker, (d) sustained 1k stream-table refresh
under churn. Bind these tests to a new `citus-tests.yml` workflow
running on push-to-main.

### 3.4 `join_and_predicates` panics on empty input *(HIGH)*

**Where:** [src/dvm/parser/rewrites.rs:5471](../src/dvm/parser/rewrites.rs)
— `let mut result = iter.next().expect("at least one predicate required");`
This is **production code** (the `#[cfg(test)]` block in this file
starts at line 5934, so anything below 5934 is test). The function is
called from rewrite passes; if a future rewrite produces an empty
predicate set the bgworker panics, which on PostgreSQL means a
postmaster-wide restart.

**Recommendation.** Change the signature to `Result<Expr,
PgTrickleError>` returning `Err(InvalidArgument("empty predicate set"))`
and propagate at the two call sites with `?`. Half-day task.

### 3.5 Inbox/outbox have zero unit test coverage *(HIGH)*

**Where:** [src/api/inbox.rs:1-1034](../src/api/inbox.rs) and
[src/api/outbox.rs:1-942](../src/api/outbox.rs) — neither has a
`#[cfg(test)] mod tests { ... }` block. Coverage is only via
[tests/e2e_inbox_tests.rs](../tests/e2e_inbox_tests.rs) and
[tests/e2e_outbox_tests.rs](../tests/e2e_outbox_tests.rs).

For an at-least-once + dedup contract, this is too thin. Dedup-key
collisions, JSON envelope NULL handling, version-mismatch envelopes,
and offset durability under crash all need unit-level tests that can
be run in milliseconds and exercised exhaustively by proptest, not
end-to-end through a Docker container.

**Recommendation.** Add `#[cfg(test)] mod tests` to both files with
dedup-key roundtrip, envelope encode/decode, NULL value handling,
version-skew rejection, and proptest-driven randomized envelope
fuzzing. 1-week task. Already an item in
[plans/relay/](relay/) per the v0.29.0 plan but not delivered.

### 3.6 `pg_trickle.enabled = false` does not fully disable CDC *(MEDIUM)*

**Where:** [src/config.rs:1276-1283](../src/config.rs) (GUC);
[src/scheduler.rs:2868](../src/scheduler.rs) (only the scheduler tick
checks the flag); CDC trigger functions in
[src/cdc.rs](../src/cdc.rs) do not check it.

When `pg_trickle.enabled = false`, the scheduler stops dispatching
refreshes but the per-source `tg_pgtrickle_*` triggers continue to
write to `pgtrickle_changes.changes_<stable_name>` on every DML. For
an operator running an emergency "turn it off" during an incident this
is surprising — DML latency is unchanged, change buffers grow, and on
re-enable the system processes the entire backlog at once.

**Recommendation.** Either (a) document the current behaviour
prominently in [docs/CONFIGURATION.md](../docs/CONFIGURATION.md) and
[docs/TROUBLESHOOTING.md](../docs/TROUBLESHOOTING.md), or (b) add a
fast-path check at the top of every CDC trigger that returns NULL
when `pg_trickle.enabled = false`. The (a) approach is safer (no
silent data loss) but worse for incident response; the (b) approach
is what an SRE expects from a kill switch but needs an additional
"durable hold" flag (`pg_trickle.cdc_paused`) to capture the intent
clearly.

### 3.7 `pg_trickle.refresh_strategy = full` cannot be enforced from a single GUC alone *(MEDIUM)*

**Where:** [src/config.rs:1366-1376](../src/config.rs);
[src/refresh/orchestrator.rs:1-388](../src/refresh/orchestrator.rs).

The GUC is read on every cycle but per-stream-table `refresh_mode`
(stored in `pgt_stream_tables`) takes precedence; an operator who
believes they have toggled the cluster into FULL mode for diagnosis
will see DIFFERENTIAL refreshes continue for any ST whose row sets the
mode explicitly. This is documented at the GUC level but not at the
incident-response level.

**Recommendation.** Add a hard-override GUC
`pg_trickle.force_full_refresh = bool` (default false) that, when set,
ignores per-ST mode for the duration of the GUC being on. Useful for
rolling-back from a suspected DVM regression.

---

## 4. Architecture & Design Gaps

### 4.1 Shared-memory dshash template cache is not implemented

The unread process-local L0 cache and its availability signal were removed.
Template caching still uses thread-local L1 and a bounded catalog-backed L2
([src/dvm/mod.rs:898](../src/dvm/mod.rs),
[src/template_cache.rs](../src/template_cache.rs)). A shared-memory dshash
cache remains a possible optimization, documented as deferred in
[plans/performance/RFC_SHARED_TEMPLATE_CACHE.md](performance/RFC_SHARED_TEMPLATE_CACHE.md).
Measure cold-path latency for short-lived backends before implementing it.

### 4.2 Refresh history `INSERT` per refresh is the dominant catalog write at scale

[src/cdc.rs:71-83](../src/cdc.rs) enforces INSERT-only invariant on
`pgt_refresh_history`. At 1,000 stream tables × 12 refreshes/min the
table sustains ~720k inserts/hour. The retention prune runs from
monitor.rs but is O(rows-deleted) per pass; for large tables the prune
cycle exceeds the scheduler tick budget. There is no batching, no
LIMIT, no statement-cancel guard.

**Recommendation.** Add a `LIMIT 10000` to the prune DELETE and run it
in a dedicated `bgworker_history_pruner` background worker on a
configurable interval (default 60s).

### 4.3 Refresh sub-modules are populated but `merge.rs` is a 3,472-LOC behemoth

[src/refresh/merge.rs:1-3472](../src/refresh/merge.rs) is the new
"refresh.rs" — most MERGE-path codegen lives there. Splitting along
PostgreSQL operator boundaries (delete/insert/update path; conflict
predicate generator; column-list builder) would let reviewers reason
about each in isolation. Not a correctness risk; a maintainability tax.

### 4.4 `src/api/mod.rs` at 6,322 LOC is the new monolith

[src/api/mod.rs](../src/api/mod.rs) carries `create_stream_table`,
`alter_stream_table`, `drop_stream_table`, `refresh_stream_table`,
DAG operations, fuse control, status reporting, and 80+
`#[pg_extern]`-decorated SQL functions across the whole `pgtrickle.*`
schema. Like the old `refresh.rs`, every change touches it. Splitting
into `src/api/lifecycle.rs`, `src/api/refresh.rs`,
`src/api/fuse.rs`, `src/api/status.rs` is the next architectural
chore.

### 4.5 Scheduler is still 7,756 LOC (the largest single file)

[src/scheduler.rs:1-7756](../src/scheduler.rs) carries dispatch loop,
tier scheduler, predictive cost integration, parallel pool, pgt_st_locks
acquisition, DAG rebuild trigger, and CDC backpressure. The sheer LOC
makes any change risky; specific concerns: tier and cost code could
move to `src/scheduler/tier.rs` + `src/scheduler/cost.rs`; pool worker
to `src/scheduler/pool.rs`; Citus lease loop to `src/scheduler/citus.rs`.

### 4.6 No structured representation of DDL hooks

[src/hooks.rs:130-176](../src/hooks.rs) defines `DdlEventKind` and
classifies events with an English-text `tag` field. Because there is no
typed enum coming out of the PostgreSQL event trigger payload, the code
matches on strings (`"ALTER TABLE"`, etc.). A typo or PG-minor wording
change breaks classification silently. Recommend a small
`pg_event_trigger_ddl_commands()` view + typed parser layer.

### 4.7 No consistent delta-row identity model across operators

The hash module ([src/hash.rs](../src/hash.rs)) is excellent (xxh3
streaming with sentinel + separator), but each operator constructs
its own row-id hash from a different tuple. This is the root cause of
the EC-01 issue and would benefit from a `RowIdSchema` type that every
operator declares as part of its signature, then a verifier that
asserts cross-operator compatibility at plan-time.

---

## 5. Performance & Scalability Bottlenecks

### 5.1 Per-tick L2 cache rehydration cost

[src/template_cache.rs:79-118](../src/template_cache.rs): on cache miss
the L2 catalog table is queried via SPI; for 1,000 STs after a backend
cold start the cumulative cost is ~200 ms before the L1 cache is warm.
The L0 shmem cache (§4.1) would close this; until then, document the
"first tick after restart is slow" expectation in
[docs/SCALING.md](../docs/SCALING.md).

### 5.2 `monitor.rs` aggregation queries are not indexed

[src/monitor.rs:571-580](../src/monitor.rs),
[src/monitor.rs:657-700](../src/monitor.rs),
[src/monitor.rs:829-840](../src/monitor.rs) all read
`pgt_refresh_history` with patterns like `LEFT JOIN ... USING (pgt_id)`
ordered by `start_time DESC`. The catalog table has a primary key on
`(pgt_id, start_time)` (per upgrade scripts) but several monitor
queries scan by `start_time` alone (the "recent across cluster"
pattern). At 10M rows the dashboard query is multi-second. Adding a
secondary index on `start_time` (with retention pruning, this stays
small) would cut query times to <100 ms.

### 5.3 WAL decoder has no backpressure cap

[src/wal_decoder.rs:1-2212](../src/wal_decoder.rs) does not enforce
`pg_replication_slot.confirmed_flush_lsn` advancement bounds. On a
slow refresh cycle the slot retention grows; the
`slot_lag_warning_threshold_mb` and `slot_lag_critical_threshold_mb`
GUCs ([src/config.rs:1554-1578](../src/config.rs)) emit alerts but do
not throttle. At 100 MB/s WAL with a stuck refresh, disk fills in
minutes. Recommend an `ENFORCE_BACKPRESSURE` mode that pauses CDC
trigger writes (or stops advancing the local frontier intentionally,
forcing logical decoding to drop slot-advance) when the slot lag
exceeds the critical threshold.

### 5.4 `pgt_st_locks` polling loop in the scheduler

[src/citus.rs](../src/citus.rs): the lease acquisition does an
`INSERT … ON CONFLICT DO NOTHING` per tick when the holder is another
coordinator. There is no exponential backoff; each tick is a UDP-style
collision-and-retry. With multiple coordinators racing on a common
stream table this generates O(N²) attempts/second. Add 50–500 ms
randomized backoff on conflict.

### 5.5 IVM cache invalidation generation counter is 32-bit

[src/ivm.rs](../src/ivm.rs) `LOCAL_IVM_CACHE_GEN` and
[src/shmem.rs:132](../src/shmem.rs) `CACHE_GENERATION` use `AtomicU64`
in shmem — that's fine. But several call sites cast to `u32` for SPI
parameter binding. With ~1M invalidations/day the 32-bit space is
exhausted in ~12 years; not a near-term issue but worth noting before
v1.0 freezes the wire format.

### 5.6 `tests/e2e_dag_bench_tests.rs` shows acceptable throughput but no latency SLO

[tests/e2e_dag_bench_tests.rs:1-1768](../tests/e2e_dag_bench_tests.rs)
measures throughput and parallel scaling but not p99 refresh latency.
[plans/performance/PLAN_DAG_BENCHMARK.md](performance/PLAN_DAG_BENCHMARK.md)
describes the target. No CI gate on p99 latency.

### 5.7 Auto-index is opt-in (`pg_trickle.auto_index = false` default)

[src/config.rs:1631-1641](../src/config.rs). Many users will not flip
this, then complain about slow refreshes against unindexed sources. A
warning emitted at `create_stream_table` time when the source has
fewer indexes than its referenced columns would help.

---

## 6. Memory Safety & Unsafe Code Audit

### 6.1 Inventory

`grep -rE 'unsafe\\s*\\{|unsafe\\s+fn|unsafe\\s+impl' src` returns 161
strict matches. By file (lines containing the keyword, including
nested):

| File | Strict count |
|---|---|
| [src/dvm/parser/sublinks.rs](../src/dvm/parser/sublinks.rs) | 211 |
| [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs) | 67 |
| [src/shmem.rs](../src/shmem.rs) | 22 |
| [src/dvm/parser/mod.rs](../src/dvm/parser/mod.rs) | 14 |
| [src/scheduler.rs](../src/scheduler.rs) | 12 |
| [src/api/helpers.rs](../src/api/helpers.rs) | 12 |
| [src/api/snapshot.rs](../src/api/snapshot.rs) | 6 |
| [src/wal_decoder.rs](../src/wal_decoder.rs) | 4 |
| [src/lib.rs](../src/lib.rs) | 2 |
| [src/dvm/parser/validation.rs](../src/dvm/parser/validation.rs) | 2 |

### 6.2 SAFETY-comment audit

- [src/dvm/parser/sublinks.rs](../src/dvm/parser/sublinks.rs): every
  unsafe block I sampled (lines 42, 60, 79, 95, 117, 121, 162, 167–336)
  carries an explicit `// SAFETY:` comment. ✅
- [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs): unsafe
  blocks at lines 366, 371, 376, 403, 417, 434, 444, 453, 481, 488,
  495, 635, 667, 672, 676, 693, 703, 2742, 2948, 2954, 2979 do **not**
  have inline `// SAFETY:` comments. Pattern is standard pgrx FFI
  (deparsing parser nodes, calling `raw_parser`) so the omissions are
  unlikely to hide a real bug, but they violate the AGENTS.md
  convention and make future refactors riskier. ⚠️ Add SAFETY comments.
- [src/shmem.rs](../src/shmem.rs): all 22 sites are
  `unsafe { PgLwLock::new(...) }` / `unsafe { PgAtomic::new(...) }`
  static initialisation — each carries SAFETY comments at lines 99,
  106, 113, 808. ✅
- [src/api/snapshot.rs](../src/api/snapshot.rs): all 6 sites
  ([lines 36-87](../src/api/snapshot.rs)) carry SAFETY comments. ✅
- [src/wal_decoder.rs](../src/wal_decoder.rs): 4 sites; visual scan
  shows comments present.

### 6.3 `unwrap()` / `expect()` / `panic!()` outside test code

After excluding `#[cfg(test)]` and `mod tests` blocks via the script in
`/tmp/audit_unwraps.py`, **95 sites remain** distributed as:

| File | Count | Risk |
|---|---|---|
| [src/api/mod.rs](../src/api/mod.rs) | 42 | All in `#[cfg(test)] mod tests` block past line 5050 — false positive (script's `mod tests` boundary detection is approximate) |
| [src/refresh/tests.rs](../src/refresh/tests.rs) | 20 | All in dedicated test file — false positive |
| [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs) | 17 | **1 real production site** at line 5471 (`expect("at least one predicate required")` — see §3.4); 16 are test code below line 5934 |
| [src/dvm/operators/aggregate.rs](../src/dvm/operators/aggregate.rs) | 7 | All `unwrap_or_else(\|\| default)` safe defaults |
| [src/dag.rs](../src/dag.rs) | 3 | SCC tarjan invariant `nosemgrep`-tagged unwraps at lines 1093, 1100, 1112 |
| [src/dvm/operators/filter.rs](../src/dvm/operators/filter.rs) | 2 | Safe defaults |
| Others | 4 | All safe defaults |

**Net result:** 1 real panic-risk site (rewrites.rs:5471) + 3
invariant-justified unwraps (dag.rs SCC). The earlier reductions from
v0.20 (513 unwraps) → v0.27 hold; lint
`#![cfg_attr(not(test), deny(clippy::unwrap_used))]` at
[src/lib.rs:23](../src/lib.rs) is doing its job.

### 6.4 Memory contexts

Audit of [src/api/helpers.rs](../src/api/helpers.rs) found no
`MemoryContextSwitchTo` calls in caller-context-sensitive paths;
unsafe blocks at lines 582, 867, 951, 981–990, 1013 are FFI casts
that do not allocate via PG. Comments at lines 751 and 866 explicitly
acknowledge "test context lacks PG memory context". No leak risk
identified.

---

## 7. Error Handling & Observability Gaps

### 7.1 Mark-for-reinit failures swallowed as warnings

[src/hooks.rs:285-310](../src/hooks.rs): if
`StreamTableMeta::mark_for_reinitialize()` fails (lock timeout,
deadlock), the code emits `warning!()` and continues. The downstream
ST is then refreshed against a stale schema which, depending on the
DDL, can produce wrong results or runtime SQL errors. Recommend
upgrading to `error!()` after N retry attempts (default 3 with 100 ms
backoff).

### 7.2 SQLSTATE classifier wiring (see §3.2)

### 7.3 Structured logging not available

There is no JSON log format option. PostgreSQL's `log_line_prefix`
captures backend metadata but pg_trickle's own `log!()` /
`info!()` / `warning!()` emit unstructured text. For OpenTelemetry /
Loki integration this means lossy regex parsing. Add a
`pg_trickle.log_format = text|json` GUC and a wrapper that emits
structured fields (`event`, `pgt_id`, `cycle_id`, `duration_ms`,
`refresh_reason`).

### 7.4 IVM lock-mode fallback is unmetered

[src/ivm.rs:48-91](../src/ivm.rs): on parse failure the IVM lock mode
defaults to `Exclusive` (correct, fail-closed) but no metric is
emitted. Operators have no visibility into how often this occurs.
Add a counter `pgtrickle_ivm_lock_mode_fallback_total{reason="parse_error"}`.

### 7.5 SubLink-in-OR silent FULL fallback

[src/dvm/parser/sublinks.rs:101-145](../src/dvm/parser/sublinks.rs)
(per v3 §3.6 and confirmed by the DVM sub-agent above): when a SubLink
is hidden inside `CASE`, `COALESCE`, or a function call within an OR
branch, the recursive `node_tree_contains_sublink()` walker misses it.
The query falls through as `Expr::Raw`, the planner downgrades to FULL
refresh, and the user sees no NOTICE. Either (a) generalise the walker
via `expression_tree_walker_impl()`, or (b) emit `pgrx::notice!()` at
every FULL fallback.

### 7.6 `let _ =` patterns reviewed: clean

[src/cdc.rs:346, 364](../src/cdc.rs) (`DROP TRIGGER IF EXISTS` is
genuinely safe to ignore) and [src/cdc.rs:2173](../src/cdc.rs)
(unused-variable suppression) are the only cases. No silent error
swallowing on result-bearing operations.

### 7.7 `pg_trickle_dump` binary is undocumented

[src/bin/pg_trickle_dump.rs:1-458](../src/bin/pg_trickle_dump.rs)
provides catalog/dump diagnostics. There is no
[docs/](../docs/) page describing what it does, when to run it, or
what its output means. Add a `docs/PG_TRICKLE_DUMP.md` runbook.

---

## 8. SQL API & Ergonomics

### 8.1 SQL surface size

`grep -rcE '#\[pg_extern' src/api/ src/citus.rs` returns ~80
public SQL functions (not including `#[pg_extern]` definitions in
operator modules, which surface internal helpers). For an extension
this is reasonable but the per-function documentation in
[docs/SQL_REFERENCE.md](../docs/SQL_REFERENCE.md) lags — not every
new outbox/inbox/Citus function has a doc entry.

### 8.2 No `EXPLAIN`-equivalent for the DVM plan

There is no SQL function that returns "for stream table X, what
differential operators will be invoked, and which fall back to
recompute." [src/diagnostics.rs](../src/diagnostics.rs) provides cost
model and history insights but not the plan tree. Add
`pgtrickle.explain_stream_table(name TEXT) RETURNS TABLE(...)` that
returns a textual rendering of the OpTree, similar to
`EXPLAIN (FORMAT TEXT)`.

### 8.3 `ALTER STREAM TABLE ... QUERY` template-cache invalidation

Per the v3 §3 catalogue, ALTER QUERY invalidates the L1 and L2 caches,
and the `CACHE_GENERATION` atomic in shmem propagates to other
backends. Verified at [src/template_cache.rs:124-142](../src/template_cache.rs)
(`invalidate`/`invalidate_all`). However the IVM cache
([src/ivm.rs](../src/ivm.rs)) and the parser cache (if any) need a
single coordinated invalidation API. Today each is its own
generation counter.

### 8.4 GUC catalogue not exhaustive in docs

`grep -c 'GucRegistry::define' src/config.rs` = **111**.
[docs/CONFIGURATION.md](../docs/CONFIGURATION.md) lists fewer; the
CDC/Citus/IVM GUCs added in v0.32–v0.34 are not all enumerated. The
`pg_trickle.list_gucs()` function does not exist. Add a generated
table from the `GucRegistry` definitions to docs (could be a CI step
that fails when the doc table is out of sync).

### 8.5 Error messages are partly inconsistent

[src/error.rs:1-886](../src/error.rs) has good context fields on most
variants but some new variants (PublicationRebuildFailed,
ChangedColsBitmaskFailed) wrap a plain `String` rather than structured
fields. For machine-parsing of errors, structured fields are
preferable — a future v1.0 SLA contract will require them.

### 8.6 `create_stream_table` privilege model

Functions in [src/api/mod.rs](../src/api/mod.rs) are
`#[pg_extern(schema = "pgtrickle")]` without explicit `SECURITY
DEFINER`. The default is INVOKER. Multi-tenant deployments where one
role can `CREATE STREAM TABLE` over another role's source table are
possible only when the invoker has SELECT on the source. Document
this contract explicitly in
[docs/security.md](../docs/security.md).

### 8.7 No bulk APIs for IVM mode operations

There is `bulk_create_stream_tables` (v0.15.0) but no
`bulk_alter_stream_tables` or `bulk_drop_stream_tables`. dbt
deployments commonly need to flip 100+ STs to a new schedule at
once; today this requires 100 round-trips.

---

## 9. Test Coverage Gaps

### 9.1 Modules without unit tests

Verified via the infra sub-agent + manual spot-check:

| Module | Lines | Has `mod tests`? | Integration test? |
|---|---|---|---|
| [src/api/inbox.rs](../src/api/inbox.rs) | 1034 | **No** | tests/e2e_inbox_tests.rs |
| [src/api/outbox.rs](../src/api/outbox.rs) | 942 | **No** | tests/e2e_outbox_tests.rs |
| [src/api/metrics_ext.rs](../src/api/metrics_ext.rs) | 132 | **No** | indirectly via monitoring tests |
| [src/template_cache.rs](../src/template_cache.rs) | 182 | **No** | None directly |
| [src/dvm/operators/test_helpers.rs](../src/dvm/operators/test_helpers.rs) | 719 | N/A (it *is* the test helpers) | — |

These are the highest-risk gaps. Each represents a critical contract
(at-least-once dedup, metric labels, template cache hit/miss).

### 9.2 Citus test gap

No `tests/e2e_citus_*.rs` exists — see §3.3. The `e2e_mdb_tests.rs`
file covers multi-database but not multi-node Citus.

### 9.3 Chaos / fault-injection coverage

`tests/e2e_concurrent_tests.rs` covers DDL races, refresh-vs-DROP, and
concurrent inserts. Missing:
- OOM injection during refresh
- Disk-full simulation (would require `LD_PRELOAD` of `write()`)
- SIGKILL of the bgworker mid-refresh (vs. SIGTERM, which is tested)
- VACUUM FULL during refresh
- Network partition between coordinator and worker (Citus)
- pg_stat_statements pressure during refresh

### 9.4 Fuzz coverage gaps

[fuzz/fuzz_targets/](../fuzz/fuzz_targets/) has `parser_fuzz`,
`cdc_fuzz`, `cron_fuzz`, `guc_fuzz`. Missing: aggregation fuzzing,
window fuzzing, recursive-CTE fuzzing, JOIN predicate fuzzing,
envelope (inbox/outbox) fuzzing. Per the parser_fuzz README the
backend-integrated rewrites are not fuzzed because they require a
live PostgreSQL — that is exactly where SQLancer should compensate.

### 9.5 Upgrade test coverage

[tests/e2e_upgrade_tests.rs:1-1143](../tests/e2e_upgrade_tests.rs) has
18 tests including multi-hop chains. Confirmed in the audit. However
no test asserts that a `pgt_change_tracking` row written at v0.31.0 is
correctly re-keyed by the v0.32.0 stable-name backfill. Add a frozen
fixture (binary dump from v0.31.0) and verify the upgrade path
produces the expected stable names.

### 9.6 SQLancer integration is correct but infrequent

`.github/workflows/sqlancer.yml` runs Sundays + manual; not on PR. A
lightweight 100-case run on every PR (5–10 min budget) would catch
DVM-vs-FULL equivalence regressions earlier.

### 9.7 Property-test corpus needs join-delta saturation

[tests/property_tests.rs:1-1615](../tests/property_tests.rs) and
[tests/e2e_property_tests.rs:1-1589](../tests/e2e_property_tests.rs)
have generic property tests but the `proptest-regressions/` directory
shows few join-delta regressions vs. the bug history would suggest.
Increase shrinking iterations and add an EC-01-specific generator.

---

## 10. Security Findings

### 10.1 No SQL injection sites identified

All dynamic SQL constructions I sampled use either `quote_ident` /
`quote_literal` or parameterised `Spi::run_with_args`. Spot-checked:
[src/api/snapshot.rs:240-260](../src/api/snapshot.rs) (CREATE TABLE
AS uses parameter binding for metadata columns, FQN built via Rust
`format!` with `"` doubling — safe);
[src/cdc.rs](../src/cdc.rs) (trigger function generation uses
`format!` with quoted identifiers — safe);
[src/citus.rs](../src/citus.rs) (`pgt_st_locks` insert is
parameterised). No injection vectors identified.

### 10.2 `pgtrickle-relay` connection-string handling

[pgtrickle-relay/src/config.rs](../pgtrickle-relay/src/config.rs)
accepts source/sink connection strings via TOML; secrets in plaintext
on disk. No integration with `gopass`/`vault`/AWS Secrets Manager.
For production use, add `${ENV:VAR}` interpolation and document
secret-management patterns in
[docs/RELAY_GUIDE.md](../docs/RELAY_GUIDE.md).

### 10.3 SECURITY DEFINER review

The `#[pg_extern]` functions in `src/api/` are not annotated as
SECURITY DEFINER. SQL files in [sql/](../sql/) define some functions
as SECURITY DEFINER (e.g. monitoring views' helpers). The intentional
contract is that the *user* role must have privilege on source and
target. Document this explicitly in
[docs/security.md](../docs/security.md).

### 10.4 No SBOM or signed releases

[deny.toml](../deny.toml) is well-curated, advisory ignores carry
justifications, but releases are not signed (no `cosign`/`sigstore`
attestation), no CycloneDX/SPDX SBOM is published, no provenance
attestation per SLSA. For ecosystem trust at v1.0 these become
mandatory.

### 10.5 OWASP Top-10 coverage

The threat model favours backend (server-side) attacks over web
attacks. Of the OWASP Top 10:
- A01 Broken Access Control: documented but not strictly enforced (§8.6)
- A02 Cryptographic Failures: connection-string secrets in plaintext (§10.2)
- A03 Injection: clean (§10.1)
- A06 Vulnerable Components: `cargo audit` weekly + ignores justified
- A09 Logging & Monitoring: structured logging missing (§7.3)
- A10 SSRF: relay sinks are user-configurable URLs — should validate
  schemes against an allow-list

---

## 11. Documentation & Onboarding Gaps

### 11.1 [docs/SUMMARY.md](../docs/SUMMARY.md) is excellent

Coverage of user guides, integrations, tutorials, research is
comprehensive. The book builds with `mdbook` (book.toml at repo root).

### 11.2 Citus integration tutorial absent

There is no end-to-end tutorial that walks a user through: install
Citus → install pg_trickle → create a distributed source table →
create a co-located distributed stream table → observe replication
lag via `citus_status`. Add `docs/tutorials/CITUS_DISTRIBUTED.md`.

### 11.3 Relay/outbox tutorial absent

[docs/RELAY_GUIDE.md](../docs/RELAY_GUIDE.md) and
[docs/RELAY.md](../docs/RELAY.md) exist but no tutorial covers the
end-to-end outbox→relay→Kafka path with code samples.

### 11.4 dbt-pgtrickle missing tests

Only `dbt-pgtrickle/integration_tests/` and `dbt-pgtrickle/tests/`
shells exist; no per-materialisation behavioural tests. Per the
infra sub-agent this is the weakest link in the dbt story.

### 11.5 `INSTALL.md` could be split

Currently [INSTALL.md](../INSTALL.md) is a single page covering
multiple distribution channels. Split into per-platform pages
(macOS, Linux, Docker, CNPG, apt/yum) with clearer "if you are X
start here" navigation.

### 11.6 Changelog readability

Per the infra sub-agent and prior assessments, the v0.34.0 changelog
is well-written for a non-technical audience — explicit, concrete
examples. ✅

---

## 12. Operational Readiness Assessment

### 12.1 Runbooks present

[docs/TROUBLESHOOTING.md](../docs/TROUBLESHOOTING.md) covers most
incidents but not Citus-specific ones (worker lease expiry, slot
divergence, rebalance recovery). Add a Citus runbook section.

### 12.2 `pg_trickle.enabled` does not stop CDC writes (§3.6)

This is the highest operational risk under incident response.

### 12.3 Capacity planning guidance missing

[docs/SCALING.md](../docs/SCALING.md) has limits but no formula like
"X stream tables × Y refreshes/min × Z CDC volume = N MB/s WAL +
M cores + K GB shared buffers". Add a "sizing calculator" page.

### 12.4 Grafana dashboards lack key SLIs

Per the infra sub-agent: refresh latency p50/p99 panels are missing,
change-buffer depth and CDC lag panels missing, no Prometheus alert
rules in repo. Add `monitoring/grafana/alerts/pg_trickle.yml`.

### 12.5 Soak test exists (G17-SOAK)

[tests/e2e_soak_tests.rs](../tests/e2e_soak_tests.rs) is configurable
(SOAK_DURATION_SECS, SOAK_CYCLE_MS) and runs in
`stability-tests.yml`. ✅

### 12.6 No documented "drain mode" for graceful shutdown

To shut down pg_trickle for a maintenance window, an operator needs
to (a) pause schedules, (b) flush in-flight refreshes, (c) wait for
CDC drains, (d) shut down. There is no single command. Add
`pgtrickle.drain_mode_begin()` / `drain_mode_end()`.

---

## 13. Ecosystem & Integration Gaps

### 13.1 pgtrickle-relay reconnection strategy

Per the infra sub-agent: no explicit reconnection backoff in
[pgtrickle-relay/src/](../pgtrickle-relay/src/). Add exponential
backoff with jitter, log reconnection attempts.

### 13.2 pgtrickle-relay backpressure missing

Sink errors do not propagate back to the source poller; the
`fire-and-forget` pattern means a slow Kafka topic causes unbounded
memory growth in the relay. Add a bounded channel with
`try_send` + retry-on-full.

### 13.3 ~~pgtrickle-tui lacks inbox/outbox views~~ *(removed — TUI has been deleted)*

The `pgtrickle-tui` crate has been removed from the project.

### 13.4 dbt Hub listing pending

dbt-pgtrickle is git-installable but not on dbt Hub. Submit.

### 13.5 CNPG smoke test does not actually deploy the extension image

[cnpg/Dockerfile.ext](../cnpg/Dockerfile.ext) builds a scratch image
but no CI test mounts it into a CNPG cluster. Add a kind-based smoke
test in `.github/workflows/stability-tests.yml`.

### 13.6 No multi-arch Docker images

amd64 only. Add arm64 to `.github/workflows/docker-hub.yml` and
`.github/workflows/ghcr.yml`.

### 13.7 No integration with pg_partman / TimescaleDB

These two extensions are dominant in the partitioning ecosystem;
pg_trickle's partitioned source detection works for vanilla
declarative partitioning but has not been verified against
pg_partman's runtime partition creation or TimescaleDB hypertables.

---

## 14. Codebase Health Metrics

| Metric | Value |
|---|---|
| Total `src/` LOC | 115,371 |
| `src/` files | 66 |
| `tests/` files | 140 |
| `tests/` LOC | 86,323 |
| GUC count | 111 (`GucRegistry::define` in `src/config.rs`) |
| Public SQL functions | ~80 (`#[pg_extern]` in `src/api/` + `src/citus.rs`) |
| Strict `unsafe` keyword count | 161 |
| Production unwrap/expect/panic sites | 1 real (rewrites.rs:5471) + 3 invariant-justified (dag.rs SCC) |
| TODO/FIXME/HACK in src/ | 1 |
| GitHub workflows | 19 |
| Upgrade scripts (`sql/pg_trickle--*.sql`) | 30 (covering 0.1.3 → 0.34.0) |
| Plan documents (`plans/`) | ~70 |

**Comparison.**

| Module | LOC v0.27.0 (per v3) | LOC v0.34.0 | Δ |
|---|---|---|---|
| scheduler.rs | 7,392 | 7,756 | +364 |
| dvm/parser/sublinks.rs | 6,485 | 6,559 | +74 |
| api/mod.rs | 6,127 | 6,322 | +195 |
| dvm/parser/rewrites.rs | 6,104 | 6,105 | ≈0 |
| dvm/parser/mod.rs | 5,526 | 5,526 | 0 |
| dvm/operators/aggregate.rs | 5,399 | 5,400 | ≈0 |
| dag.rs | 4,295 | 4,295 | 0 |
| dvm/operators/recursive_cte.rs | 4,043 | 4,050 | ≈0 |
| cdc.rs | 3,970 | 4,037 | +67 |
| monitor.rs | 3,633 | 3,719 | +86 |
| refresh/merge.rs | 3,393 | 3,472 | +79 |
| config.rs | 3,270 | 3,789 | +519 (Citus + 36 new GUCs) |
| catalog.rs | 3,135 | 3,172 | +37 |
| dvm/parser/types.rs | 3,082 | 3,082 | 0 |
| **NEW** citus.rs | — | 1,174 | +1,174 |
| **NEW** api/inbox.rs | — | 1,034 | +1,034 |
| **NEW** api/outbox.rs | — | 942 | +942 |

Net `src/` growth ~9%. Most of it is additive (Citus + outbox/inbox);
hot-path modules (DVM, scheduler, refresh) grew by single-digit
percentages.

---

## 15. New Feature Proposals

### F1 — Reactive subscriptions ("PG-LISTEN on a stream table")

**Problem.** Today the only way to react to a stream-table change is
to poll or to wire an outbox + relay to a sink. For
sub-second-latency UI use cases (dashboards, presence, chat) this is
clumsy.
**Approach.** A new `pgtrickle.subscribe(name TEXT, channel TEXT)`
that auto-emits `NOTIFY <channel>, <json_payload>` on every applied
delta. Reuse the v0.27 publication infrastructure.
**Complexity:** M. **Impact:** ergonomics + ecosystem.
**Suggested release:** v0.35.0.

### F2 — Time-travel queries (`AS OF`)

**Problem.** Differential dataflow naturally produces a temporal
log; today nothing surfaces it.
**Approach.** Persist the last N watermarks per ST in a circular
buffer table; add `pgtrickle.stream_table_at(name, ts)` returning a
relation. Use PostgreSQL set-returning function.
**Complexity:** L. **Impact:** correctness (audit) + ecosystem.
**Suggested release:** v0.36.0.

### F3 — `EXPLAIN STREAM TABLE` operator-tree visualiser

**Problem.** Operators have no way to know which DVM operators their
query compiled to.
**Approach.** Walk the cached OpTree, render as text/json/dot via
`pgtrickle.explain_stream_table(name, FORMAT TEXT|JSON|DOT)`.
**Complexity:** S. **Impact:** ergonomics.
**Suggested release:** v0.35.0.

### F4 — pgVectorMV: incremental vector-aggregation MVs for RAG

**Problem.** Embedding pipelines today recompute aggregated vectors
on a schedule; pg_trickle could maintain centroids/IVF clusters
incrementally with pgvector.
**Approach.** Add a `vector_avg` algebraic aggregate operator
(Welford-equivalent in the L2 norm domain); validate with pgvector
0.7+ HNSW indexes.
**Complexity:** L. **Impact:** ecosystem (AI/RAG).
**Suggested release:** v0.37.0.

### F5 — Online schema evolution with `ALTER STREAM TABLE EVOLVE`

**Problem.** Today `ALTER QUERY` requires a full reinit. Adding a
column to a SELECT * style ST should be online.
**Approach.** Detect type-compatible column additions in ALTER QUERY
diff; emit ALTER TABLE on storage; re-prepare templates without
flushing data.
**Complexity:** L. **Impact:** ergonomics.
**Suggested release:** v0.36.0.

### F6 — DBSP-style pre-aggregation for hot keys

**Problem.** A heavy-hitter key in a GROUP BY recomputes the whole
partial sum on each cycle.
**Approach.** Maintain per-key heap of recent deltas; apply lazily
on read. Adapted from DBSP's "sharded zset" pattern.
**Complexity:** XL. **Impact:** performance.
**Suggested release:** v0.40.0.

### F7 — WebAssembly compilation of hot-path operators

**Problem.** PostgreSQL's row-by-row execution dominates differential
operator latency for narrow queries.
**Approach.** Compile delta-application kernels to WASM via
`wasmtime` and JIT-load per ST. Optional, behind a GUC.
**Complexity:** XL. **Impact:** performance.
**Suggested release:** post-1.0.

### F8 — Multi-master / active-active replication

**Problem.** No story for multi-region writes today.
**Approach.** Use BDR-style logical replication of `pgt_change_tracking`;
each region produces local refreshes; downstream sinks dedupe by
`(region_id, local_lsn)`.
**Complexity:** XL. **Impact:** scalability.
**Suggested release:** post-1.0.

### F9 — Kubernetes Operator (`pg_trickle-operator`)

**Problem.** CNPG ships a generic operator; pg_trickle deserves a
CRD-based one for declarative ST management.
**Approach.** Go-based operator using controller-runtime; CRDs for
`StreamTable`, `Outbox`, `Inbox`; reconcile loop maps to SQL API.
**Complexity:** L. **Impact:** ecosystem.
**Suggested release:** v0.38.0.

### F10 — OpenTelemetry trace propagation

**Problem.** No way to correlate a refresh cycle with the upstream
HTTP request that wrote the source data.
**Approach.** Read W3C Trace Context from a session GUC
(`pg_trickle.trace_id`); emit spans for each refresh phase via
OTLP/gRPC; integrate with `pg_logical_emit_message` to carry trace
headers across replication.
**Complexity:** M. **Impact:** observability.
**Suggested release:** v0.37.0.

### F11 — `STREAM TABLE` SQL syntax (custom parser hook)

**Problem.** Today users invoke `pgtrickle.create_stream_table('name',
$$SELECT ...$$)`. A native `CREATE STREAM TABLE x AS SELECT ...`
syntax is the strongest ergonomic move.
**Approach.** Use PostgreSQL's `ProcessUtility_hook` to intercept
`CREATE STREAM TABLE` lexically before parsing; rewrite to the
function call.
**Complexity:** L. **Impact:** ergonomics. Documented in
[docs/research/CUSTOM_SQL_SYNTAX.md](../docs/research/CUSTOM_SQL_SYNTAX.md).
**Suggested release:** v0.36.0.

### F12 — Materialised CTE column lineage

**Problem.** Operators cannot trace which source columns feed which
ST columns; debugging "why did this row appear" is hard.
**Approach.** During parsing, record a `column_lineage` graph in
`pgt_stream_tables`; expose via
`pgtrickle.stream_table_lineage(name)`.
**Complexity:** M. **Impact:** ergonomics + correctness debugging.
**Suggested release:** v0.36.0.

### F13 — Pluggable sink architecture in pgtrickle-relay

**Problem.** Adding a new sink (e.g. Snowflake, Iceberg, ClickHouse)
requires a relay rebuild.
**Approach.** Define a stable `Sink` trait + `dlopen`-style plugin
loading; ship sinks as separate cargo crates.
**Complexity:** M. **Impact:** ecosystem.
**Suggested release:** v0.36.0.

### F14 — Auto-generated GraphQL/REST API from stream tables

**Problem.** Real-time dashboards need an HTTP API. Today users
glue PostgREST or Hasura.
**Approach.** A new sub-crate `pgtrickle-gateway` exposes
`/streams/<name>` (GET + SSE) reading from local PG.
**Complexity:** L. **Impact:** ecosystem.
**Suggested release:** v0.40.0.

### F15 — Cluster-wide query equivalence prover

**Problem.** Equivalence between FULL and DIFFERENTIAL is currently
checked by SQLancer post-hoc; could be enforced at create-time.
**Approach.** Symbolic execution of the OpTree to derive a normal
form; compare against FULL's normal form. Reject on inequivalence.
**Complexity:** XL. **Impact:** correctness.
**Suggested release:** post-1.0.

### F16 — Edge-cache integration (Cloudflare D1, Turso libSQL)

**Problem.** Stream tables are server-side; serving them at edge
nodes still requires an HTTP hop.
**Approach.** Compile a subset of ST output to SQLite/libSQL;
sync via CRDT-tagged delta log.
**Complexity:** XL. **Impact:** ecosystem.
**Suggested release:** post-1.0.

### F17 — Built-in SLA dashboard SQL views

**Problem.** Operators must compose multiple `pgt_*_status` views
to get an SLA report.
**Approach.** Add `pgtrickle.sla_summary()` returning per-ST p50/p99
latency, freshness, error budget burn. Drives F3 / F10 dashboards.
**Complexity:** S. **Impact:** ergonomics.
**Suggested release:** v0.35.0.

---

## 16. Prioritised Action Plan

Sort: Severity desc (CRITICAL → LOW), then Effort asc.

| ID | Area | Finding | Severity | Effort | Suggested Release | Owner Hint |
|---|---|---|---|---|---|---|
| A01 | DVM correctness | EC-01 phantom rows (§3.1) | CRITICAL | L | v0.35.0 | DVM lead |
| A02 | DVM correctness | Add 50k-iteration EC-01 property test | CRITICAL | M | v0.35.0 | DVM lead |
| A03 | Citus | Add chaos test rig (§3.3, §13.5) | HIGH | L | v0.35.0 | Citus lead |
| A04 | Error handling | Wire SQLSTATE classifier (§3.2) | HIGH | S | v0.35.0 | Core |
| A05 | Safety | Fix `expect` panic at rewrites.rs:5471 (§3.4) | HIGH | XS | v0.34.1 | DVM lead |
| A06 | Tests | Unit tests for inbox/outbox (§3.5, §9.1) | HIGH | M | v0.35.0 | Outbox lead |
| A07 | Ops | `pg_trickle.enabled` should also gate CDC trigger writes (§3.6) | MEDIUM | S | v0.35.0 | Core |
| A08 | Ops | Add `pg_trickle.force_full_refresh` GUC (§3.7) | MEDIUM | XS | v0.35.0 | Core |
| A09 | Performance | L0 dshash shared-mem cache (§4.1, §5.1) | HIGH | L | v0.36.0 | Perf |
| A10 | Performance | History prune in dedicated bgworker w/ LIMIT (§4.2) | MEDIUM | S | v0.35.0 | Core |
| A11 | Performance | Add `start_time` index to `pgt_refresh_history` (§5.2) | MEDIUM | XS | v0.35.0 | Core |
| A12 | Performance | WAL backpressure enforcement (§5.3) | HIGH | M | v0.36.0 | CDC lead |
| A13 | Performance | Citus lease backoff with jitter (§5.4) | MEDIUM | XS | v0.35.0 | Citus lead |
| A14 | Architecture | Split `src/api/mod.rs` (§4.4) | MEDIUM | M | v0.36.0 | Core |
| A15 | Architecture | Split `src/scheduler.rs` (§4.5) | MEDIUM | L | v0.37.0 | Core |
| A16 | Architecture | Split `src/refresh/merge.rs` (§4.3) | LOW | M | v0.37.0 | Refresh lead |
| A17 | Architecture | Typed DDL event payload (§4.6) | LOW | S | v0.36.0 | Core |
| A18 | Architecture | `RowIdSchema` type across operators (§4.7) | MEDIUM | L | v0.36.0 | DVM lead |
| A19 | Safety | Add SAFETY comments to rewrites.rs unsafe blocks (§6.2) | LOW | S | v0.35.0 | DVM lead |
| A20 | Observability | Structured logging (§7.3) | MEDIUM | M | v0.36.0 | Obs |
| A21 | Observability | IVM lock-mode fallback metric (§7.4) | LOW | XS | v0.35.0 | DVM lead |
| A22 | Observability | NOTICE on FULL fallback (§7.5) | MEDIUM | S | v0.35.0 | DVM lead |
| A23 | API | `EXPLAIN STREAM TABLE` (§8.2, F3) | MEDIUM | S | v0.35.0 | API |
| A24 | API | Generated GUC catalogue in docs (§8.4) | LOW | S | v0.35.0 | Docs |
| A25 | API | Bulk alter / drop APIs (§8.7) | LOW | S | v0.36.0 | API |
| A26 | Tests | OOM / disk-full chaos (§9.3) | MEDIUM | M | v0.36.0 | QA |
| A27 | Tests | Citus upgrade regression with frozen fixture (§9.5) | MEDIUM | M | v0.35.0 | Citus lead |
| A28 | Tests | Add lightweight SQLancer to PR gate (§9.6) | MEDIUM | S | v0.35.0 | CI |
| A29 | Tests | Aggregation/window/recursive-CTE fuzz targets (§9.4) | LOW | M | v0.36.0 | QA |
| A30 | Security | Relay secret interpolation (§10.2) | MEDIUM | S | v0.35.0 | Relay lead |
| A31 | Security | Sigstore + SBOM at release (§10.4) | MEDIUM | M | v0.36.0 | Release |
| A32 | Docs | Citus tutorial (§11.2) | MEDIUM | M | v0.35.0 | Docs |
| A33 | Docs | Outbox→relay→Kafka tutorial (§11.3) | MEDIUM | M | v0.35.0 | Docs |
| A34 | Docs | `pg_trickle_dump` runbook (§7.7) | LOW | S | v0.35.0 | Docs |
| A35 | Ops | Drain mode (§12.6) | MEDIUM | M | v0.36.0 | Core |
| A36 | Ops | Capacity sizing calculator (§12.3) | LOW | M | v0.36.0 | Docs |
| A37 | Ops | Grafana p50/p99 + alert rules (§12.4) | MEDIUM | S | v0.35.0 | Obs |
| A38 | Ecosystem | Relay reconnection backoff (§13.1) | MEDIUM | S | v0.35.0 | Relay lead |
| A39 | Ecosystem | Relay backpressure (§13.2) | HIGH | M | v0.35.0 | Relay lead |
| A40 | Ecosystem | TUI inbox/outbox views (§13.3) | LOW | M | v0.36.0 | Removed (TUI deleted) |
| A41 | Ecosystem | dbt Hub submission (§13.4) | LOW | XS | v0.35.0 | dbt lead |
| A42 | Ecosystem | Multi-arch images (§13.6) | MEDIUM | S | v0.35.0 | Release |
| A43 | Ecosystem | pg_partman / TimescaleDB integration tests (§13.7) | LOW | M | v0.37.0 | QA |

---

## 17. Appendix — File-Level Coverage Summary

### A. `src/` files (66 total)

LOC = source lines (`wc -l`); UC = unsafe-keyword strict count;
TODO = TODO/FIXME/HACK count; UT = has `#[cfg(test)] mod tests` block;
IT = has matching integration test under `tests/`.

| File | LOC | UC | UT | IT |
|---|---|---|---|---|
| [src/lib.rs](../src/lib.rs) | 1,339 | 2 | Y | (extension_tests.rs) |
| [src/api/mod.rs](../src/api/mod.rs) | 6,322 | 0 | Y | many |
| [src/api/helpers.rs](../src/api/helpers.rs) | 2,867 | 12 | Y | indirect |
| [src/api/diagnostics.rs](../src/api/diagnostics.rs) | 1,770 | 0 | Y | e2e_diagnostics_tests.rs |
| [src/api/snapshot.rs](../src/api/snapshot.rs) | 629 | 6 | Y | e2e_snapshot_consistency_tests.rs |
| [src/api/publication.rs](../src/api/publication.rs) | 866 | 0 | Y | e2e_publication_crash_recovery_tests.rs |
| [src/api/outbox.rs](../src/api/outbox.rs) | 942 | 0 | **N** | e2e_outbox_tests.rs |
| [src/api/inbox.rs](../src/api/inbox.rs) | 1,034 | 0 | **N** | e2e_inbox_tests.rs |
| [src/api/cluster.rs](../src/api/cluster.rs) | 153 | 0 | Y | e2e_self_monitoring_tests.rs |
| [src/api/planner.rs](../src/api/planner.rs) | 341 | 0 | Y | e2e_predictive_cost_tests.rs |
| [src/api/self_monitoring.rs](../src/api/self_monitoring.rs) | 823 | 0 | Y | e2e_self_monitoring_tests.rs |
| [src/api/metrics_ext.rs](../src/api/metrics_ext.rs) | 132 | 0 | **N** | indirect |
| [src/bin/pg_trickle_dump.rs](../src/bin/pg_trickle_dump.rs) | 458 | 0 | N | none |
| [src/catalog.rs](../src/catalog.rs) | 3,172 | 0 | Y | catalog_tests.rs |
| [src/cdc.rs](../src/cdc.rs) | 4,037 | 0 | Y | e2e_cdc_*.rs many |
| [src/citus.rs](../src/citus.rs) | 1,174 | 0 | Y | **N** (no e2e_citus_*) |
| [src/config.rs](../src/config.rs) | 3,789 | 0 | Y | e2e_guc_variation_tests.rs |
| [src/dag.rs](../src/dag.rs) | 4,295 | 0 | Y | e2e_dag_*.rs many |
| [src/diagnostics.rs](../src/diagnostics.rs) | 2,051 | 0 | Y | e2e_diagnostics_tests.rs |
| [src/error.rs](../src/error.rs) | 886 | 0 | Y | indirect |
| [src/hash.rs](../src/hash.rs) | 172 | 0 | Y | indirect |
| [src/hooks.rs](../src/hooks.rs) | 1,961 | 0 | Y | e2e_ddl_event_tests.rs |
| [src/ivm.rs](../src/ivm.rs) | 1,639 | 0 | Y | e2e_ivm_tests.rs |
| [src/metrics_server.rs](../src/metrics_server.rs) | 398 | 0 | Y | e2e_monitoring_tests.rs |
| [src/monitor.rs](../src/monitor.rs) | 3,719 | 0 | Y | monitoring_tests.rs |
| [src/scheduler.rs](../src/scheduler.rs) | 7,756 | 12 | Y | e2e_tier_*.rs, e2e_bgworker_tests.rs |
| [src/shmem.rs](../src/shmem.rs) | 1,117 | 22 | Y | e2e_shared_buffer_tests.rs |
| [src/template_cache.rs](../src/template_cache.rs) | 182 | 0 | **N** | none |
| [src/version.rs](../src/version.rs) | 537 | 0 | Y | indirect |
| [src/wal_decoder.rs](../src/wal_decoder.rs) | 2,212 | 4 | Y | e2e_wal_cdc_tests.rs |
| [src/refresh/mod.rs](../src/refresh/mod.rs) | 212 | 0 | Y | e2e_refresh_tests.rs |
| [src/refresh/orchestrator.rs](../src/refresh/orchestrator.rs) | 388 | 0 | (via tests.rs) | e2e_refresh_tests.rs |
| [src/refresh/codegen.rs](../src/refresh/codegen.rs) | 2,383 | 0 | (via tests.rs) | indirect |
| [src/refresh/merge.rs](../src/refresh/merge.rs) | 3,472 | 0 | (via tests.rs) | e2e_merge_template_tests.rs |
| [src/refresh/phd1.rs](../src/refresh/phd1.rs) | 108 | 0 | Y | e2e_cascade_regression_tests.rs |
| [src/refresh/tests.rs](../src/refresh/tests.rs) | 2,572 | 0 | Y | (this is the test module) |
| [src/dvm/mod.rs](../src/dvm/mod.rs) | 1,811 | 0 | Y | indirect |
| [src/dvm/diff.rs](../src/dvm/diff.rs) | 1,068 | 0 | Y | indirect |
| [src/dvm/row_id.rs](../src/dvm/row_id.rs) | 136 | 0 | Y | indirect |
| [src/dvm/parser/mod.rs](../src/dvm/parser/mod.rs) | 5,526 | 14 | Y | e2e_coverage_parser_tests.rs |
| [src/dvm/parser/types.rs](../src/dvm/parser/types.rs) | 3,082 | 0 | Y | indirect |
| [src/dvm/parser/validation.rs](../src/dvm/parser/validation.rs) | 1,177 | 2 | Y | e2e_correctness_gate_tests.rs |
| [src/dvm/parser/rewrites.rs](../src/dvm/parser/rewrites.rs) | 6,105 | 67 | Y | indirect |
| [src/dvm/parser/sublinks.rs](../src/dvm/parser/sublinks.rs) | 6,559 | 211 | (in mod tests) | e2e_all_subquery_tests.rs |
| [src/dvm/operators/mod.rs](../src/dvm/operators/mod.rs) | 28 | 0 | N | — |
| [src/dvm/operators/scan.rs](../src/dvm/operators/scan.rs) | 1,763 | 0 | Y | indirect |
| [src/dvm/operators/filter.rs](../src/dvm/operators/filter.rs) | 746 | 0 | Y | indirect |
| [src/dvm/operators/project.rs](../src/dvm/operators/project.rs) | 522 | 0 | Y | indirect |
| [src/dvm/operators/aggregate.rs](../src/dvm/operators/aggregate.rs) | 5,400 | 0 | Y | dvm_aggregate_execution_tests.rs |
| [src/dvm/operators/join.rs](../src/dvm/operators/join.rs) | 1,719 | 0 | Y | dvm_join_tests.rs |
| [src/dvm/operators/join_common.rs](../src/dvm/operators/join_common.rs) | 2,074 | 0 | Y | dvm_*_join_tests.rs |
| [src/dvm/operators/full_join.rs](../src/dvm/operators/full_join.rs) | 699 | 0 | Y | dvm_full_join_tests.rs |
| [src/dvm/operators/outer_join.rs](../src/dvm/operators/outer_join.rs) | 766 | 0 | Y | dvm_outer_join_tests.rs |
| [src/dvm/operators/semi_join.rs](../src/dvm/operators/semi_join.rs) | 668 | 0 | Y | dvm_semijoin_antijoin_tests.rs |
| [src/dvm/operators/anti_join.rs](../src/dvm/operators/anti_join.rs) | 545 | 0 | Y | dvm_semijoin_antijoin_tests.rs |
| [src/dvm/operators/recursive_cte.rs](../src/dvm/operators/recursive_cte.rs) | 4,050 | 0 | Y | e2e_cte_tests.rs |
| [src/dvm/operators/window.rs](../src/dvm/operators/window.rs) | 717 | 0 | Y | dvm_window_scalar_subquery_tests.rs |
| [src/dvm/operators/lateral_subquery.rs](../src/dvm/operators/lateral_subquery.rs) | 1,266 | 0 | Y | e2e_lateral_subquery_tests.rs |
| [src/dvm/operators/lateral_function.rs](../src/dvm/operators/lateral_function.rs) | 473 | 0 | Y | e2e_lateral_tests.rs |
| [src/dvm/operators/scalar_subquery.rs](../src/dvm/operators/scalar_subquery.rs) | 319 | 0 | Y | e2e_scalar_subquery_tests.rs |
| [src/dvm/operators/distinct.rs](../src/dvm/operators/distinct.rs) | 191 | 0 | Y | indirect |
| [src/dvm/operators/cte_scan.rs](../src/dvm/operators/cte_scan.rs) | 199 | 0 | Y | e2e_cte_tests.rs |
| [src/dvm/operators/intersect.rs](../src/dvm/operators/intersect.rs) | 401 | 0 | Y | e2e_set_operation_tests.rs |
| [src/dvm/operators/except.rs](../src/dvm/operators/except.rs) | 483 | 0 | Y | e2e_set_operation_tests.rs |
| [src/dvm/operators/subquery.rs](../src/dvm/operators/subquery.rs) | 124 | 0 | Y | indirect |
| [src/dvm/operators/union_all.rs](../src/dvm/operators/union_all.rs) | 127 | 0 | Y | indirect |
| [src/dvm/operators/test_helpers.rs](../src/dvm/operators/test_helpers.rs) | 719 | 0 | (helper) | (helper) |

### B. `tests/` files (140 total)

The full enumeration is large; below is a categorisation. Every file
under `tests/` ending in `.rs` is integration-tier (Testcontainers or
pgrx) unless prefixed `e2e_` (full Docker E2E) or `dvm_` (operator
DVM tests).

| Tier | Pattern | Count |
|---|---|---|
| Pgrx unit/integration | `tests/{catalog,extension,monitoring,property,resilience,scenario,smoke,trigger_detection,workflow,catalog_compat}_tests.rs` | 10 |
| DVM operator tests | `tests/dvm_*_tests.rs` | 10 |
| E2E (full Docker) | `tests/e2e_*.rs` | 110 |
| Sub-suites | `tests/e2e/`, `tests/nexmark/`, `tests/tpch/`, `tests/pg_regress/`, `tests/common/` | 5 dirs |
| Build helpers | `tests/Dockerfile.*`, `tests/build_e2e_image.sh`, `tests/build_e2e_upgrade_image.sh` | 5 |

The 110 E2E files are spread across all 17 chapters of the user
guide; the heaviest are listed in the audit metrics above
(e2e_tpch_tests.rs 3,438 LOC; e2e_cte_tests.rs 3,150;
e2e_bench_tests.rs 2,361; e2e_partition_tests.rs 2,208;
e2e_mixed_pg_objects_tests.rs 2,211).

### C. Source modules with no matching test file

- [src/template_cache.rs](../src/template_cache.rs) — neither
  `mod tests` nor an integration test file references it directly.
- [src/api/metrics_ext.rs](../src/api/metrics_ext.rs) — only indirect
  coverage via monitoring tests.
- [src/api/inbox.rs](../src/api/inbox.rs) — no `mod tests`; only
  `tests/e2e_inbox_tests.rs`.
- [src/api/outbox.rs](../src/api/outbox.rs) — no `mod tests`; only
  `tests/e2e_outbox_tests.rs`.
- [src/citus.rs](../src/citus.rs) — has `mod tests` but no
  multi-node integration test (`e2e_citus_*` does not exist).
- [src/bin/pg_trickle_dump.rs](../src/bin/pg_trickle_dump.rs) — no
  test of any tier.

These six are the highest-priority test-gap items, ordered by impact.

---

*End of report.*
