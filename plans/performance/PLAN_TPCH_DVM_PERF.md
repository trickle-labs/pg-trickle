# PLAN: TPC-H DVM Scaling — Investigation and Fixes

**Date:** 2026-04-19
**Status:** Planning
**Predecessor:** [PLAN_DVM_IMPROVEMENTS.md](PLAN_DVM_IMPROVEMENTS.md) (DI-1 – DI-10, DI-2 partial ✅),
**Related:** PR #574 (SF-10 nightly timeout root cause)
**Scope:** Diagnose and fix the three DVM scaling failure modes identified by
running `test_tpch_performance_comparison` at SF=0.01 / SF=0.1 / SF=1.0
(April 19 2026). Queries fall into three categories: threshold collapse,
early collapse, and structural bugs. Most map onto known DI items in
`PLAN_DVM_IMPROVEMENTS.md`; this plan tracks the investigation-first
workflow needed to confirm hypotheses before coding.

> **Note on DI items:** `PLAN_DVM_IMPROVEMENTS.md` already contains
> detailed algebraic analysis of the DVM bottlenecks. This document
> does not duplicate that material. It focuses on what new evidence
> the scaling benchmarks provide, which DI items they confirm or
> invalidate, and what work remains.

---

## Table of Contents

1. [Benchmark Results Summary](#1-benchmark-results-summary)
2. [Failure Mode Classification](#2-failure-mode-classification)
3. [Phase 1: Diagnosis — Disambiguate Spill vs DVM SQL](#3-phase-1-diagnosis--disambiguate-spill-vs-dvm-sql)
4. [Phase 2: Fix Threshold-Collapse Queries (q05/q07/q08/q09)](#4-phase-2-fix-threshold-collapse-queries-q05q07q08q09)
5. [Phase 3: Fix Early-Collapse Query (q04)](#5-phase-3-fix-early-collapse-query-q04)
6. [Phase 4: Fix Structural Bug (q20)](#6-phase-4-fix-structural-bug-q20)
7. [Phase 5: Planner Hints and work_mem GUC](#7-phase-5-planner-hints-and-work_mem-guc)
8. [Effort Estimates](#8-effort-estimates)
9. [Verification](#9-verification)
10. [Appendix: Raw Benchmark Data](#10-appendix-raw-benchmark-data)

---

## 1. Benchmark Results Summary

Median DIFF refresh time (ms) across three scale factors:

| Query | SF=0.01 | SF=0.1 | SF=1.0 | 0.01→0.1 | 0.1→1.0 | Mode |
|-------|---------|--------|--------|----------|---------|------|
| q08   | 138     | 505    | 39,940 | 3.7×     | **79×** | Threshold collapse |
| q05   | 103     | 108    | 28,404 | 1.0×     | **264×**| Threshold collapse |
| q07   | 124     | 183    | 31,113 | 1.5×     | **170×**| Threshold collapse |
| q09   | 114     | 123    | 29,204 | 1.1×     | **237×**| Threshold collapse |
| q22   | 13      | 27     | 3,056  | 2.1×     | **113×**| Threshold collapse |
| q04   | 15      | 2,102  | 5,731  | **140×** | 2.7×    | Early collapse |
| q20   | 1,775   | 1,912  | 2,647  | 1.1×     | 1.4×    | Structural bug |
| q15   | 21      | 46     | 1,000  | 2.2×     | 21.6×   | Super-linear |
| q17   | 19      | 23     | 328    | 1.2×     | 14.3×   | Super-linear |
| q13   | 14      | 29     | 326    | 2.1×     | 11.2×   | Super-linear |
| q02   | 9       | 7      | 7      | ~1×      | ~1×     | ✅ Ideal (flat) |
| q11   | 5       | 5      | 5      | ~1×      | ~1×     | ✅ Ideal (flat) |
| q16   | 6       | 5      | 6      | ~1×      | ~1×     | ✅ Ideal (flat) |

At SF=1.0, 18 of 22 queries have DIFF slower than FULL re-evaluation.
The worst case (q09) is 2,246× slower than FULL.

---

## 2. Failure Mode Classification

### 2.1 Threshold Collapse (q05, q07, q08, q09, q22)

These queries appear fast at SF=0.01 (100–140ms) and are nearly flat from
SF=0.01→0.1, then explode 100–260× per data decade at SF=1.0.

**Hypothesis A — work_mem spill:** The DVM delta SQL generates hash joins
or sort nodes that fit in `work_mem` (default 4MB in Docker container) at
SF=0.1 but spill to disk at SF=1.0. This would manifest as near-linear DIFF
scaling after increasing `work_mem`.

**Hypothesis B — DVM cardinality blowup:** The intermediate CTE volume from
the L₀ snapshot expansion described in `PLAN_DVM_IMPROVEMENTS.md §3.1`
produces O(n²) intermediate rows. This is the root cause DI-1/DI-2 address.
Under this hypothesis, `work_mem` is not the bottleneck — the SQL itself is
generating too many rows.

**Connection to DI items:** These are exactly the Q05/Q09-class queries
identified in `PLAN_DVM_IMPROVEMENTS.md §1` (6-table join, cascading EXCEPT
ALL evaluated multiple times per join node). DI-1 (named CTE sharing) ✅ and
DI-2 (pre-image capture, partial ✅) address this. The SF=1.0 benchmark
results measure how much of the problem DI-2 partial has already fixed and
how much remains.

### 2.2 Early Collapse (q04)

q04 is 15ms at SF=0.01 then jumps 140× to 2.1s at SF=0.1 before
stabilizing. q04's WHERE clause is `WHERE o_orderdate >= date '[DATE]' AND
o_orderdate < date '[DATE]' + interval '3 month' AND EXISTS (SELECT 1 FROM
lineitem WHERE l_orderkey = o_orderkey AND l_commitdate < l_receiptdate)`.

The `EXISTS` maps to an anti-join node in the query plan (`diff_anti_join`
in `anti_join.rs`). The DI-6 key filter optimization (equi-join key pushed
into R_old) is implemented, but the SF=0.01→0.1 jump suggests the equi-join
key filter is either not being applied correctly for this correlated shape
or the R_old EXCEPT ALL is still too expensive at the first 10× scale step.

**Hypothesis:** The `r_old_key_filter` in `anti_join.rs` requires a simple
equi-join key (e.g. `l.key = r.key`) but q04's EXISTS condition correlates
on `l_orderkey = o_orderkey` while also scanning the full `lineitem` for
non-changed rows. At SF=0.1 (60K lineitems), the R_old EXCEPT ALL scans
600K rows for each changed order — the key filter prevents a cross-product
but cannot prevent the full scan.

### 2.3 Structural Bug (q20)

q20's DIFF time is ~1.8–2.6s at every scale factor while FULL is ~8–14ms.
The 1.4× increase from SF=0.01 to SF=1.0 (100× data) confirms the cost is
dominated by a fixed-overhead path that barely depends on table size.

q20 contains a doubly-nested correlated EXISTS:
```sql
WHERE p_partkey IN (
  SELECT ps_partkey FROM partsupp
  WHERE ps_suppkey IN (
    SELECT s_suppkey FROM supplier
    WHERE s_nationkey = (SELECT n_nationkey FROM nation WHERE n_name = :1)
  ) AND ps_availqty > (
    SELECT 0.5 * SUM(l_quantity) FROM lineitem
    WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey ...
  )
)
```

`PLAN_DVM_IMPROVEMENTS.md §1` explicitly identifies Q20 as "doubly-nested
correlated semi-join | R_old MATERIALIZED for both EXISTS levels; EXCEPT ALL
inside inner semi-join | 6824ms DIFF vs 15ms FULL". The flat scaling
confirms this is not a data-volume problem — the DVM SQL structure itself is
the issue.

---

## 3. Phase 1: Diagnosis — Disambiguate Spill vs DVM SQL

**Goal:** Confirm whether the threshold-collapse queries (q05/q07/q08/q09)
are bottlenecked by PostgreSQL sort/hash spill or by DVM intermediate
cardinality. The answer determines the fix cost.

### P1-1: work_mem Benchmark (½ day)

Run `test_tpch_performance_comparison` at SF=1.0 with `work_mem = '1GB'`
set before each refresh:

```bash
TPCH_SCALE=1.0 TPCH_BENCH=1 TPCH_CYCLES=2 \
  PGT_BENCH_PRE_SQL="SET work_mem = '1GB'" \
  ./scripts/run_e2e_tests.sh \
  --test e2e_tpch_tests --run-ignored all --no-capture \
  -E 'test(test_tpch_performance_comparison)'
```

**If q05/q07/q08/q09 drop to <500ms:** it's a spill problem. Fix is a
`work_mem` bump in the delta SQL execution path (Phase 5).

**If they stay above 5s:** it's a DVM SQL cardinality problem. Fix requires
completing DI-2 (Phase 2).

**Implementation:** Add `PGT_BENCH_PRE_SQL` env var support to
`test_tpch_performance_comparison` (or use `ALTER SYSTEM SET work_mem` before
the test run). No production code change required for this diagnostic.

### P1-2: Capture DVM-Generated Delta SQL (1 day)

For q04 and q20, capture the actual SQL that `diff_node` generates so we
can run `EXPLAIN (ANALYZE, BUFFERS)` on it directly.

The cleanest approach is adding a pgtrickle debug GUC:
```sql
SET pgtrickle.log_delta_sql = on;
```
that logs the generated SQL to PostgreSQL's server log at `DEBUG1` level
before execution. It already goes through `Spi::execute` — add one
`pgrx::log!("{}", delta_sql)` call gated on a GUC flag.

Expected output for q04 at SF=0.1 would show whether the
`r_old_key_filter` is being included and what the estimated rows are on the
EXCEPT ALL node.

---

## 4. Phase 2: Fix Threshold-Collapse Queries (q05/q07/q08/q09)

**Prerequisite:** Phase 1 diagnosis completed.

### Path A — DI-2 Completion (3–4 days, if hypothesis B confirmed)

`PLAN_DVM_IMPROVEMENTS.md §DI-2` describes replacing the L₀ EXCEPT ALL
inline expression with a pre-image captured from the change buffer's
`before_image` columns. This eliminates the multi-scan of unchanged base
tables and reduces intermediate row counts from O(n) to O(Δ).

DI-2 is "partial ✅" — pre-image capture works at the leaf level but the
aggregate-level UPDATE-split (§DI-2, paragraph "aggregate UPDATE-split")
has not been implemented. For q05/q09-class queries the leaf-level
pre-image already helps; the remaining gap is for UPDATE-heavy workloads.

**Scope of remaining DI-2 work:**
- Aggregate UPDATE-split (splits UPDATE rows into DELETE+INSERT for
  algebraic aggregate path): ~2 days
- Validation that 22/22 TPC-H queries pass after change: ~1–2 days
- Regression benchmark against SF=0.01 baseline: ½ day

### Path B — work_mem Bump (½ day, if hypothesis A confirmed)

Set `work_mem` to a configurable budget inside `execute_delta_sql` before
calling `Spi::execute`. See Phase 5 for the full GUC design.

### P2-1: EXPLAIN ANALYZE for q13/q15/q17/q22 Super-Linear Queries (½ day)

q13, q15, q17, q22 show 10–20× scaling per decade — better than the
collapse group but still super-linear. After P1-2, run EXPLAIN ANALYZE on
their generated delta SQL at SF=0.1 and SF=1.0 to determine whether these
also benefit from DI-2 or whether they have independent issues (e.g. q22
has a `NOT IN` correlated subquery that may generate a hash anti-join with
bad cardinality estimation).

---

## 5. Phase 3: Fix Early-Collapse Query (q04)

**Goal:** Reduce q04 from 2.1s (SF=0.1) to under 100ms.

### P3-1: Investigate DI-6 Filter Coverage for q04 (½ day)

`anti_join.rs` already implements the DI-6 key filter:

```rust
let r_old_equi_keys = extract_equijoin_keys_aliased(condition, left, "dl", right, right_alias);
```

Verify that `extract_equijoin_keys_aliased` extracts the q04 condition
`l_orderkey = o_orderkey` as an equi-join key. If the extraction fails
(e.g. because the condition is inside a correlated EXISTS with additional
non-equi predicates like `l_commitdate < l_receiptdate`), the key filter
is silently omitted.

### P3-2: Restrict R_old to Changed Keys Only (1–2 days, if P3-1 shows gap)

Even with the equi-join key filter, R_old for q04 scans all lineitem rows
matching any changed order key, which at SF=0.1 is O(60K). The correct
fix is to restrict R_old to rows correlated with the specific delta:

```sql
r_old AS MATERIALIZED (
  SELECT * FROM lineitem
  WHERE l_orderkey IN (SELECT o_orderkey FROM delta_orders)
  EXCEPT ALL
  SELECT * FROM delta_lineitem WHERE action = 'I'
  UNION ALL
  SELECT * FROM delta_lineitem WHERE action = 'D'
)
```

This turns an O(n) scan into O(Δ) — the same row count as the delta itself.

The implementation touches `anti_join.rs` and `semi_join.rs`: the key
filter construction needs to generate the `IN (SELECT ... FROM delta)` form
rather than the static value filter it currently uses.

---

## 6. Phase 4: Fix Structural Bug (q20)

**Goal:** Reduce q20 from ~2s to under 50ms across all scale factors.

### P4-1: Understand the Doubly-Nested EXISTS Path (½ day)

q20 contains two nested EXISTS/IN clauses each correlated on different
keys. From `PLAN_DVM_IMPROVEMENTS.md §1`:

> "R_old MATERIALIZED for both EXISTS levels; EXCEPT ALL inside inner
> semi-join"

The issue is that the inner EXISTS generates its own R_old, and the delta
for the outer EXISTS re-materializes the inner R_old on every row of the
outer delta. This is O(outer_Δ × n_inner) rather than O(outer_Δ + n_inner).

After P1-2 captures the generated SQL, verify this is happening and measure
the inner R_old row count at SF=0.1.

### P4-2: Hoist Inner R_old to Named CTE (1–2 days)

The fix is to hoist the inner EXISTS's R_old out of the correlated subquery
loop into a named CTE materialized once before the outer delta scan:

```sql
WITH inner_r_old AS MATERIALIZED (
  SELECT ps_partkey, ps_suppkey FROM partsupp
  WHERE ps_suppkey IN (...)
  EXCEPT ALL ...
)
SELECT ... FROM delta_outer
WHERE ... IN (SELECT ps_partkey FROM inner_r_old WHERE ...)
```

This is a special case of DI-1 (named CTE sharing) applied across nested
semi-join levels. The current DI-1 implementation materializes each node's
own R_old but may not hoist across nesting levels.

**Implementation:** Modify `DiffContext::add_cte` to detect when a CTE
from an inner semi-join/anti-join is referenced from an outer correlated
context and promote it to the outer level.

---

## 7. Phase 5: Planner Hints and work_mem GUC

**Goal:** Allow operators to tune PostgreSQL query planning for delta SQL
without requiring code changes.

### P5-1: `pgtrickle.delta_work_mem` GUC (½ day)

Add a GUC that sets `work_mem` inside `execute_delta_sql` before running
the generated SQL. Allows tuning without server restart:

```sql
ALTER SYSTEM SET pgtrickle.delta_work_mem = '256MB';
SELECT pg_reload_conf();
```

Default: `0` (inherit PostgreSQL's session `work_mem`). This is a
short-term mitigation while DI-2 completion (Phase 2) is in progress.

**Location:** `config.rs` (GUC definition) + `refresh.rs`
(`execute_delta_sql` wrapper).

### P5-2: `pgtrickle.delta_enable_nestloop` GUC (½ day, optional)

Some planner regressions on generated delta SQL come from nested loop joins
being chosen for large right sides. A per-refresh GUC to disable nestloop
(`SET enable_nestloop = off`) inside delta execution could help multi-join
queries. Useful diagnostic until planner stats are reliable.

---

## 8. Effort Estimates

| Phase | Item | Days | Confidence | Prerequisite |
|-------|------|------|------------|--------------|
| 1 | P1-1: work_mem benchmark | 0.5 | High | — |
| 1 | P1-2: delta SQL logging GUC | 1.0 | High | — |
| 2A | DI-2 agg UPDATE-split completion | 2.0 | Medium | P1-1 confirms hypothesis B |
| 2A | DI-2 validation (22/22 TPC-H) | 1.5 | Medium | above |
| 2B | work_mem bump in execute_delta_sql | 0.5 | High | P1-1 confirms hypothesis A |
| 2 | P2-1: EXPLAIN for q13/q15/q17/q22 | 0.5 | High | P1-2 |
| 3 | P3-1: DI-6 coverage check for q04 | 0.5 | High | P1-2 |
| 3 | P3-2: delta-key R_old restriction | 1.5 | Medium | P3-1 shows gap |
| 4 | P4-1: q20 nested EXISTS analysis | 0.5 | High | P1-2 |
| 4 | P4-2: hoist inner R_old to named CTE | 2.0 | Medium | P4-1 |
| 5 | P5-1: delta_work_mem GUC | 0.5 | High | — |
| 5 | P5-2: delta_enable_nestloop GUC | 0.5 | Low | P5-1 |

**Best case (hypothesis A: spill):** P1-1 + P1-2 + P2B + P5-1 = **2.5 days**
The queries are already generating correct delta SQL; PostgreSQL just needs
more sort memory. This path requires no DVM code changes.

**Likely case (hypothesis B: DVM cardinality):** P1-1 + P1-2 + P2A + P3-1
+ P3-2 + P4-1 + P4-2 + P5-1 = **~10 days**
DI-2 completion plus targeted fixes for q04 and q20. This is the path if
the work_mem benchmark shows no improvement.

**Key uncertainty:** The 0.01→0.1 plateau for q05/q07/q09 followed by the
0.1→1.0 explosion strongly suggests memory spill, but the existing DI-1
named-CTE work should have reduced intermediate volume. If DI-1 is correctly
sharing CTEs, the remaining volume growth is likely the non-algebraic
aggregate rescan path described in `PLAN_DVM_IMPROVEMENTS.md §2.4`.

---

## 9. Verification

After each phase:

```bash
# Phase 1 verification
TPCH_SCALE=1.0 TPCH_BENCH=1 TPCH_CYCLES=2 \
  ./scripts/run_e2e_tests.sh \
  --test e2e_tpch_tests --run-ignored all --no-capture \
  -E 'test(test_tpch_performance_comparison)'

# Check correctness still holds
TPCH_SCALE=1.0 TPCH_CYCLES=2 \
  ./scripts/run_e2e_tests.sh \
  --test e2e_tpch_tests --run-ignored all \
  -E 'test(test_tpch_differential_correctness)'
```

**Success criteria:**
- All 22 queries pass `test_tpch_differential_correctness` at SF=1.0
- q04 DIFF < 500ms at SF=1.0 (currently 5.7s)
- q20 DIFF < 100ms at SF=1.0 (currently 2.6s)
- q05/q07/q08/q09 DIFF < 2s at SF=1.0 (currently 28–40s)
- q22 DIFF < 200ms at SF=1.0 (currently 3.1s)
- No regression on queries that are currently fast (q02, q11, q16: stay < 20ms)

---

## 10. Appendix: Raw Benchmark Data

Collected 2026-04-19 on macOS, Docker Desktop, pg_trickle_e2e:latest,
PostgreSQL 18.3. `TPCH_BENCH=1 TPCH_CYCLES=2`. Median of 2 measured cycles.

### SF=0.01 (1,500 orders, 6,000 lineitems)

| Query | FULL med ms | DIFF med ms | Speedup |
|-------|-------------|-------------|---------|
| q01 | 9.7 | 11.1 | 0.87× |
| q02 | 12.2 | 8.8 | 1.39× |
| q03 | 5.6 | 5.1 | 1.10× |
| q04 | 7.5 | 15.0 | 0.50× |
| q05 | 9.8 | 103.2 | 0.10× |
| q06 | 6.7 | 9.4 | 0.72× |
| q07 | 10.7 | 123.6 | 0.09× |
| q08 | 11.4 | 137.8 | 0.08× |
| q09 | 10.4 | 114.0 | 0.09× |
| q10 | 5.6 | 5.5 | 1.03× |
| q11 | 6.9 | 5.4 | 1.29× |
| q12 | 8.2 | 13.8 | 0.59× |
| q13 | 6.8 | 14.2 | 0.48× |
| q14 | 6.9 | 12.3 | 0.56× |
| q15 | 8.8 | 21.0 | 0.42× |
| q16 | 7.6 | 5.7 | 1.34× |
| q17 | 7.8 | 18.9 | 0.41× |
| q18 | 6.2 | 5.6 | 1.11× |
| q19 | 7.6 | 11.6 | 0.65× |
| q20 | 7.1 | 1,774.6 | 0.00× |
| q21 | 7.3 | 7.7 | 0.95× |
| q22 | 6.8 | 13.0 | 0.53× |

### SF=0.1 (15,000 orders, 60,000 lineitems)

| Query | FULL med ms | DIFF med ms | Speedup |
|-------|-------------|-------------|---------|
| q01 | 32.6 | 14.2 | 2.29× |
| q02 | 10.9 | 6.7 | 1.63× |
| q03 | 8.7 | 7.0 | 1.26× |
| q04 | 12.3 | 2,101.8 | 0.01× |
| q05 | 12.7 | 107.7 | 0.12× |
| q06 | 9.0 | 11.4 | 0.79× |
| q07 | 13.2 | 182.6 | 0.07× |
| q08 | 13.6 | 505.4 | 0.03× |
| q09 | 10.0 | 123.0 | 0.08× |
| q10 | 13.4 | 11.0 | 1.22× |
| q11 | 8.9 | 5.1 | 1.72× |
| q12 | 12.1 | 25.2 | 0.48× |
| q13 | 9.4 | 29.0 | 0.33× |
| q14 | 10.1 | 18.8 | 0.54× |
| q15 | 15.8 | 46.4 | 0.34× |
| q16 | 8.8 | 5.0 | 1.77× |
| q17 | 8.4 | 23.0 | 0.36× |
| q18 | 17.4 | 16.3 | 1.07× |
| q19 | 7.7 | 18.0 | 0.43× |
| q20 | 11.4 | 1,911.6 | 0.01× |
| q21 | 13.0 | 13.3 | 0.97× |
| q22 | 8.3 | 27.1 | 0.31× |

### SF=1.0 (150,000 orders, 600,000 lineitems)

| Query | FULL med ms | DIFF med ms | Speedup |
|-------|-------------|-------------|---------|
| q01 | 288.8 | 75.3 | 3.83× |
| q02 | 10.8 | 6.8 | 1.58× |
| q03 | 30.4 | 29.1 | 1.04× |
| q04 | 53.3 | 5,730.6 | 0.01× |
| q05 | 29.1 | 28,403.5 | 0.00× |
| q06 | 40.4 | 51.0 | 0.79× |
| q07 | 55.3 | 31,112.5 | 0.00× |
| q08 | 44.8 | 39,939.5 | 0.00× |
| q09 | 13.2 | 29,203.5 | 0.00× |
| q10 | 56.3 | 53.8 | 1.05× |
| q11 | 20.8 | 5.4 | 3.84× |
| q12 | 65.4 | 170.2 | 0.38× |
| q13 | 43.4 | 326.1 | 0.13× |
| q14 | 47.4 | 102.3 | 0.46× |
| q15 | 98.1 | 1,000.4 | 0.10× |
| q16 | 17.9 | 5.8 | 3.09× |
| q17 | 11.3 | 328.1 | 0.03× |
| q18 | 149.0 | 143.1 | 1.04× |
| q19 | 10.5 | 31.0 | 0.34× |
| q20 | 9.5 | 2,646.5 | 0.00× |
| q21 | 61.6 | 60.5 | 1.02× |
| q22 | 22.6 | 3,055.8 | 0.01× |
