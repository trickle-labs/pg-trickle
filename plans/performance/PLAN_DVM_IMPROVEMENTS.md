# PLAN: DVM Engine Improvements — Reducing Delta SQL Intermediate Cardinality

**Date:** 2025-07-22  
**Status:** In Progress (DI-8 ✅, DI-9 ✅, DI-1 ✅, DI-3 ✅ (already implemented), DI-10 ✅, DI-6 ✅, DI-4 ✅ (subset of DI-1), DI-7 ✅, DI-5 ✅, DI-2 partial ✅)  
**Scope:** Reduce temporary data volume in generated delta SQL, targeting the
multi-GB temp file spill that blocks TPC-H Q05/Q09 in DIFFERENTIAL mode and
the O(n²) blowup on correlated semi-joins (Q20).

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Current Architecture](#2-current-architecture)
3. [Bottleneck Analysis](#3-bottleneck-analysis)
4. [Proposals](#4-proposals)
   - [DI-1: Materialize Per-Leaf L₀ Snapshots](#di-1-materialize-per-leaf-l₀-snapshots)
   - [DI-2: Pre-Image Capture from Change Buffer](#di-2-pre-image-capture-from-change-buffer)
   - [DI-3: Group-Key Filtered Aggregate Old Rescan](#di-3-group-key-filtered-aggregate-old-rescan)
   - [DI-4: Shared R₀ CTE Across Join Parts](#di-4-shared-r₀-cte-across-join-parts)
   - [DI-5: Part 3 Correction Consolidation](#di-5-part-3-correction-consolidation)
   - [DI-6: Lazy Semi-Join R_old Materialization](#di-6-lazy-semi-join-r_old-materialization)
   - [DI-7: Scan-Count-Aware Strategy Selector](#di-7-scan-count-aware-strategy-selector)
   - [DI-8: SUM(CASE WHEN …) Algebraic Drift Fix](#di-8-sumcase-when--algebraic-drift-fix)
   - [DI-9: Scheduler Skips IMMEDIATE-Mode Tables](#di-9-scheduler-skips-immediate-mode-tables)
   - [DI-10: SF=1 Benchmark Validation Target](#di-10-sf1-benchmark-validation-target)
5. [Dependency Graph](#5-dependency-graph)
6. [Priority & Schedule](#6-priority--schedule)
7. [Background: EC-01 and EC-01B](#7-background-ec-01-and-ec-01b)

---

## 1. Motivation

TPC-H at SF=0.01 demonstrates two classes of delta SQL cardinality problems:

| Query | Tables | Issue | Symptom |
|-------|--------|-------|---------|
| Q05 | 6-way join (supplier→lineitem→orders→customer→nation→region) | Per-leaf EXCEPT ALL computed 3× per join node; cascading CTEs spill to disk | `temp_file_limit (4194304kB)` exceeded |
| Q09 | 6-way join (nation→supplier→partsupp→lineitem→orders→part) | Same pattern as Q05 | `temp_file_limit (4194304kB)` exceeded |
| Q20 | Doubly-nested correlated semi-join | R_old MATERIALIZED for both EXISTS levels; EXCEPT ALL inside inner semi-join | 6824ms DIFF vs 15ms FULL (0.00× speedup) |
| Q07/Q08 | 5–6 way join | DIFF vs IMMEDIATE skipped | `temp_file_limit` exceeded in cross-mode comparison |

Raising `temp_file_limit` above 4 GB may allow these queries to complete but
at extreme I/O cost. The sustainable fix is reducing the intermediate data
volume the delta SQL produces.

### Impact

Solving Q05/Q09 would bring TPC-H DIFFERENTIAL correctness from 20/22 to
22/22, meeting the v0.13.0 exit criterion. Solving Q20 would remove the most
severe performance outlier from the benchmark suite.

---

## 2. Current Architecture

### 2.1 Join Delta Formula

For an inner join `J = L ⋈ R`, the delta is computed as:

```
ΔJ = (ΔL_ins ⋈ R₁) ∪ (ΔL_del ⋈ R₀)     -- Part 1a + 1b (EC-01 split)
   ∪ (L₀ ⋈ ΔR)                            -- Part 2
   - correction                            -- Part 3 (nested joins only)
```

Where:
- **R₁** = post-change snapshot (current table state)  
- **R₀** = pre-change snapshot = R₁ EXCEPT ALL ΔR_ins UNION ALL ΔR_del  
- **L₀** = pre-change snapshot of left child (same formula, recursive)  
- **Part 3 correction** = ΔL ⋈ ΔR (removes double-counted rows)

### 2.2 Pre-Change Snapshot Strategy (EC-01B)

Since v0.12.0, the `use_pre_change_snapshot` function in `join_common.rs`
applies per-leaf CTE reconstruction for **all** join depths—no scan-count
threshold. Each leaf Scan's L₀ is computed as:

```sql
(SELECT * FROM base_table
 EXCEPT ALL SELECT cols FROM delta WHERE action='I'
 UNION ALL SELECT cols FROM delta WHERE action='D')
```

SemiJoin/AntiJoin-containing subtrees fall back to post-change snapshots
(L₁/R₁) with correction terms to avoid the Q21 numwait regression.

### 2.3 CTE Volume for a 6-Table Join (Q05-like)

A 6-table inner join chain `((((A ⋈ B) ⋈ C) ⋈ D) ⋈ E) ⋈ F` where table C
changes produces approximately:

| CTE Category | Count | Notes |
|-------------|-------|-------|
| Delta capture (change buffer reads) | 2 | ins + del from `pgtrickle_changes` |
| Per-leaf L₀ snapshots | 5 | One per non-changed leaf (A, B, D, E, F) |
| Per-node Part 1a (ins ⋈ R₁) | 5 | One per join node on the delta path |
| Per-node Part 1b (del ⋈ R₀) | 5 | One per join node; each references per-leaf L₀ |
| Per-node Part 2 (L₀ ⋈ ΔR) | 5 | Each expands L₀ inline |
| Part 3 corrections | 2–4 | For shallow nested joins |
| UNION ALL assembly | 5 | Combining Part 1+2+3 per node |

**Total: ~22–30 CTEs**, most referencing the same per-leaf L₀ snapshot
expressions inline rather than as named CTEs. The planner may evaluate each
inline reference separately, causing the same EXCEPT ALL to execute multiple
times per join node.

### 2.4 Aggregate Rescan Paths

Two paths in `aggregate.rs`:

- **Algebraic path** (COUNT, SUM, AVG): `old = new - ins + del`. No EXCEPT ALL
  needed. Uses `new_rescan` CTE only (post-change aggregate).
- **Non-algebraic path** (MIN, MAX, BIT_AND, STRING_AGG, etc.): Computes
  `old_rescan` via full EXCEPT ALL on the child's data plus re-aggregation.
  This second `diff_node(child)` call duplicates the entire child CTE tree.

---

## 3. Bottleneck Analysis

Four locations cause intermediate cardinality explosion:

### 3.1 Repeated Per-Leaf L₀ Inline Expansion

Each join node's Part 1b and Part 2 reference L₀ of their respective children.
For a 6-table join, the innermost leaf's L₀ is:

```
L₀_A = A EXCEPT ALL delta_A_ins UNION ALL delta_A_del
```

This expression appears **inline** at every join node that needs the left
child's pre-change state. With 5 join nodes × 2 references (Part 1b + Part 2),
a single leaf's EXCEPT ALL can be evaluated up to 10 times.

**Estimated waste:** 5–10× redundant base-table scans for unchanged leaves.

### 3.2 R₀ Recomputation in Part 1b

Part 1b computes `ΔL_del ⋈ R₀`, where R₀ is the pre-change right child.
For a deep right subtree, R₀ involves cascading EXCEPT ALL operations.
Part 2 also needs L₀ which may share structure with R₀ at the parent level
but is computed independently.

**Current code:** `build_pre_change_snapshot_sql()` in `join_common.rs`
constructs the snapshot SQL string each time it's called, producing a fresh
inline subquery. Two calls for the same subtree produce textually identical
but separately-planned subqueries.

### 3.3 Non-Algebraic Aggregate Old Rescan

When `is_algebraically_invertible` returns false (MIN, MAX, BIT_AND,
STRING_AGG), the aggregate operator calls `ctx.diff_node(child)` a second
time to get delta column names, then wraps the entire child FROM with:

```sql
SELECT aggs FROM (
  SELECT * FROM child_source
  EXCEPT ALL SELECT cols FROM child_delta WHERE action='I'
  UNION ALL SELECT cols FROM child_delta WHERE action='D'
) old_source
GROUP BY group_cols
```

This rescans the full child extent to compute the old aggregate. For a join
child, this means re-executing the entire join on full data.

**Key issue:** The GROUP BY produces one row per group, but the EXCEPT ALL
inside operates on the full pre-GROUP-BY cardinality. If only a few groups
are affected, most of this work is wasted.

### 3.4 Semi-Join MATERIALIZED R_old

Semi-joins (`EXISTS (SELECT ...)`) always build a `MATERIALIZED` R_old CTE:

```sql
r_old AS MATERIALIZED (
  SELECT * FROM right_source
  EXCEPT ALL SELECT cols FROM right_delta WHERE action = 'I'
  UNION ALL SELECT cols FROM right_delta WHERE action = 'D'
)
```

For nested semi-joins (Q20: `EXISTS(... EXISTS(...))`) this materializes at
each level. The inner level's R_old is the full inner subquery result minus
deltas — potentially large.

---

## 4. Proposals

### DI-1: Materialize Per-Leaf L₀ Snapshots

**Problem:** Per-leaf L₀ (pre-change base table snapshot) is computed inline
at every reference site. PostgreSQL evaluates each inline subquery
independently, causing redundant EXCEPT ALL + full table scans.

**Proposal:** Emit each per-leaf L₀ as a **named CTE** with `NOT MATERIALIZED`
hint by default (letting the planner fold it into downstream scans when
beneficial). When the reference count exceeds a threshold (≥3), switch to
`MATERIALIZED` to force a single evaluation.

The default of `NOT MATERIALIZED` is chosen because:
- For small CTEs (e.g., 100 rows at SF=0.01), the planner may inline the CTE
  more efficiently than materializing it.
- For large CTEs at SF=1, the planner's cost model can decide whether to
  materialize based on actual row-count estimates.
- The ≥3 reference-count threshold is conservative — it triggers only for
  deep join chains (4+ tables) where redundant evaluation is demonstrably
  expensive. The threshold can be refined based on SF=1 benchmark results
  (DI-10).

**Implementation:**
1. In `build_pre_change_snapshot_sql()` (`join_common.rs:399`), instead of
   returning inline SQL, register a named CTE via `ctx.add_cte()` and return
   the CTE name.
2. Track reference counts per leaf. If a leaf's L₀ is referenced ≥3 times,
   mark it MATERIALIZED.
3. Unchanged leaves (no delta rows) can skip EXCEPT ALL entirely — their L₀
   equals the current table. Use delta-branch pruning (already implemented
   in B3-1) to detect this.

**Estimated impact:** 20–40% reduction in intermediate data for 4+ table joins.
For Q05/Q09 (6-way), eliminates ~5× redundant full-table scans per leaf.

**Effort:** Medium (2–3 days). Core change is in `build_pre_change_snapshot_sql`
and `diff_inner_join`. Must verify that named CTEs don't break column
disambiguation.

**Risk:** Low. CTE naming is well-established in the diff engine. The main
risk is column aliasing — the returned CTE must carry disambiguated column
names matching what inline SQL currently produces.

**Prerequisite:** None. Can be done independently.

---

### DI-2: Pre-Image Capture from Change Buffer (partial ✅)

**Status:** NOT EXISTS anti-join implemented for all Scan leaf snapshots.
Per-leaf conditional fallback implemented: when a source's delta exceeds
`max_delta_fraction`, that leaf switches from NOT EXISTS to EXCEPT ALL.
E2E validated: all diff-full-equivalence, multi-cycle, profiling, and
autorefresh tests pass after bootstrap DDL fix and snapshot CTE cache key
collision fix. Remaining: aggregate UPDATE-split (DI-8 band-aid removal
blocked on Q12 differential drift — root cause in join×aggregate
interaction), Q05/Q09 E2E verification.

**Problem:** `EXCEPT ALL` is fundamentally expensive — it requires sorting or
hashing the full base table to subtract delta inserts. For large tables
this dominates refresh time.

**Key finding:** The CDC change buffer **already stores `old_*` typed columns**
for every source column. Code inspection of `src/cdc.rs` confirms:
- `build_row_trigger_fn_sql()`: INSERT writes `new_*`; UPDATE writes both
  `new_*` and `old_*`; DELETE writes `old_*`.
- `build_stmt_trigger_fn_sql()`: same pattern via transition tables.
- `create_change_buffer_table()`: emits `"new_col" TYPE, "old_col" TYPE`
  for all source columns.

The CDC trigger change previously treated as "fundamental architecture work"
(justifying v1.x deferral) is therefore already complete. Only the delta SQL
generator needs to be updated to use this data.

**Proposal:** Replace per-leaf L₀ `EXCEPT ALL` reconstruction with a
pk-hash filter + direct pre-image read from the change buffer:

```sql
-- Old: full base table minus delta inserts (expensive)
SELECT * FROM base_table
EXCEPT ALL SELECT new_col1, ... FROM changes_NNN WHERE action IN ('I', 'U')
UNION ALL  SELECT old_col1, ... FROM changes_NNN WHERE action IN ('D', 'U')

-- New: filter unchanged rows + read pre-image directly (cheap)
SELECT col1, ... FROM base_table b
  WHERE NOT EXISTS (
    SELECT 1 FROM changes_NNN c WHERE c.pk_hash = b.pk_hash
  )
UNION ALL
SELECT old_col1, ... FROM changes_NNN WHERE action IN ('D', 'U')
```

The `NOT EXISTS` anti-join uses the existing `lsn_pk_cid` covering index and
returns only untouched rows. `NOT EXISTS` is preferred over `NOT IN` because:
- PostgreSQL's planner reliably chooses hash anti-join for `NOT EXISTS`,
  while `NOT IN` can degrade to nested-loop anti-join at higher cardinalities.
- `NOT EXISTS` is NULL-safe: if `pk_hash` is ever NULL (e.g., keyless tables),
  `NOT IN` silently returns zero rows; `NOT EXISTS` correctly handles NULLs.
- The semantic intent ("rows not touched by this delta") maps directly to
  an anti-join, which `NOT EXISTS` expresses precisely.

The `old_*` values come directly from the already-populated change buffer
columns.

**Implementation:**
1. Add `change_schema`, `lsn_range` access to `build_pre_change_snapshot_sql()`
   (currently it only receives the `OpTree`, not `DiffContext`). Thread the
   context through the call chain in `join_common.rs`.
2. For `Scan` nodes in that function, emit the `NOT EXISTS` anti-join + `old_*`
   UNION ALL instead of the current `EXCEPT ALL` inline expression.
3. **Per-leaf conditional fallback:** At generation time, if the change buffer
   row count for this leaf exceeds `max_delta_fraction × estimated_table_rows`
   (using stats already available in `DiffContext`), emit the old `EXCEPT ALL`
   formula for that leaf instead of the `NOT EXISTS` anti-join. This provides
   finer-grained control than the per-ST threshold in DI-7 — a single
   high-churn leaf can fall back independently while sibling leaves use the
   fast path.
4. For `Scan` nodes where the source has **no** delta rows in the current cycle
   (detected via existing delta-branch pruning), skip the UNION ALL entirely
   and emit the base table reference directly — L₀ = L₁ when no changes occurred.
5. Test all DML combinations: INSERT-only, UPDATE-only, DELETE-only,
   mixed UPDATE+DELETE, keyless tables (all-column content hash).

**Aggregate UPDATE-split (subsumes DI-8 band-aid):** In addition to the join-
level pre-image capture, DI-2 includes an aggregate-level UPDATE-split:
for UPDATE rows in the change buffer, the aggregate delta CTE evaluates the
aggregate expression using `old_*` column values for the 'D' side and `new_*`
for the 'I' side. This makes the algebraic formula correct even when CASE
conditions reference mutable columns — the 'D' side evaluates `SUM(CASE WHEN
old_o_orderpriority …)` and the 'I' side evaluates `SUM(CASE WHEN
new_o_orderpriority …)`. Once this lands, DI-8's `Expr::Raw` check in
`is_algebraically_invertible()` can be removed and SUM(CASE WHEN) restored
to the faster algebraic path.

**Estimated impact:** Eliminates `EXCEPT ALL` against full base tables for L₀
construction — for Q05/Q09 (6-table joins, ~100K rows/table at SF=1), this
replaces 5 full-table sorts/hashes with 5 indexed pk lookups. Expected 60–90%
reduction in intermediate data volume.

**Effort:** Medium (3.5–5.5 days).
- CDC trigger: no changes required.
- Schema migration: no changes required — `old_*` columns already exist.
- SQL generator: core change is ~50–80 lines in `build_pre_change_snapshot_sql()`
  plus context threading through 2–3 call sites.
- Per-leaf conditional fallback: ~0.5 day (change buffer stats lookup +
  EXCEPT ALL fallback emission).
- Aggregate UPDATE-split: ~0.5 day (modify `agg_delta_exprs()` in
  `aggregate.rs` to reference `old_*` columns for UPDATE rows on the 'D' side;
  add test coverage for SUM(CASE WHEN) with UPDATE mutations; remove DI-8
  band-aid check after verification).
- Tests: ~1 day for comprehensive DML coverage.

**Risk:** Medium.
- The per-leaf conditional fallback (step 3) mitigates the risk of `NOT EXISTS`
  degradation at very large delta sets (>25% of table rows). Unlike the per-ST
  `max_delta_fraction` in DI-7 which decides at the scheduler level, this
  fallback is per-leaf and decided at SQL generation time — protecting only
  the affected leaf while siblings use the fast path.
- Keyless tables use an all-column content hash as `pk_hash` — the anti-join
  still works but requires careful column ordering in the `old_*` SELECT
  to match the base table's column order expected by downstream CTEs.
- `DiffContext` threading into `build_pre_change_snapshot_sql()` affects
  several call sites; must verify no regression in non-join paths.
- The aggregate UPDATE-split adds ~0.5 day to the DI-2 scope: the
  `agg_delta_exprs()` function in `aggregate.rs` must be modified to emit
  `old_*` column references for the 'D' side when change buffer rows have
  `action = 'U'`.

**Prerequisite:** DI-1 (named CTE) remains beneficial even with DI-2
land — it deduplicates the pk-filter subqueries themselves across multiple
references. Implement DI-1 first, then DI-2 replaces the body of each
named CTE.

**Target:** v0.13.0 (promoted from v1.x — CDC capture already complete).

---

### DI-3: Group-Key Filtered Aggregate Old Rescan

**Problem:** Non-algebraic aggregates (MIN, MAX, STRING_AGG, etc.) compute
`old_rescan` by rescanning the **entire** child data through EXCEPT ALL +
GROUP BY. If only 3 groups out of 10,000 are affected by the current delta,
99.97% of the rescan work is wasted.

**Proposal:** Filter the old rescan to only groups that appear in the delta:

```sql
agg_old AS (
  SELECT aggs
  FROM (
    SELECT * FROM child_source
    WHERE (group_col1, group_col2) IN (
      SELECT group_col1, group_col2 FROM delta_cte
    )
    EXCEPT ALL
    SELECT cols FROM child_delta WHERE action='I'
    UNION ALL
    SELECT cols FROM child_delta WHERE action='D'
  ) old
  GROUP BY group_cols
)
```

The `WHERE ... IN (SELECT ... FROM delta_cte)` clause restricts the base table
scan to only rows belonging to affected groups before the EXCEPT ALL.

**Implementation:**
1. In `aggregate.rs`, non-algebraic branch (~line 750), add a WHERE clause
   to the FROM subquery that filters on group keys present in `delta_cte`.
2. Extract group column names from `group_output` and `delta_cte`.
3. For single-group (no GROUP BY) aggregates, skip the optimization (all rows
   are in one group).

**Estimated impact:** Proportional to the ratio of affected groups to total
groups. At 1% change rate with uniform distribution: ~99% reduction in
old_rescan volume. Real-world impact depends on data distribution.

**Effort:** Low (0.5–1 day). Small, localized change in the non-algebraic
branch of `diff_aggregate`.

**Risk:** Low. The group-key filter is a pure optimization — it doesn't change
the EXCEPT ALL semantics, just reduces its input. For NULLable group-by
columns, the filter must use `EXISTS` with `IS NOT DISTINCT FROM` instead of
`IN` to correctly include NULL groups:

```sql
WHERE EXISTS (
  SELECT 1 FROM delta_cte d
  WHERE d.group_col1 IS NOT DISTINCT FROM base.group_col1
    AND d.group_col2 IS NOT DISTINCT FROM base.group_col2
)
```

Standard `IN` on NULLable columns silently excludes NULL groups, producing
incorrect old aggregate values. `IS NOT DISTINCT FROM` treats `NULL = NULL`
as true, which is the correct semantics for group-key matching.

**Prerequisite:** None. Independent of all other proposals.

---

### DI-4: Shared R₀ CTE Across Join Parts

**Problem:** In `diff_inner_join`, Part 1b computes `ΔL_del ⋈ R₀` and Part 2
computes `L₀ ⋈ ΔR`. Both reference the pre-change snapshot of their
respective sides. When the **right** child is a join subtree (not a simple
Scan), R₀ is a complex inline subquery that gets emitted twice — once for
Part 1b and once for Part 2's usage of R₀ in the parent's perspective.

Actually the duplication is more subtle: at each join node, Part 1b needs
R₀ (pre-change right), and Part 2 needs L₀ (pre-change left). At the
**parent** join node, the current node's result is either L or R, and the
parent may compute its snapshot inline again.

**Proposal:** When `build_pre_change_snapshot_sql()` is called for a subtree
that has already been computed, return the existing CTE name instead of
generating a new inline expression. This requires a cache keyed by OpTree
node identity.

**Implementation:**
1. Add a `snapshot_cache: HashMap<usize, String>` to `DiffContext` (keyed by
   OpTree pointer or node ID).
2. In `build_pre_change_snapshot_sql()`, check the cache before generating SQL.
3. On first computation, register as a named CTE and cache the name.

**Note (2026-03-30):** The cache key was initially `op.alias()`, which returns
`"join"` for all `InnerJoin` nodes — causing 4+ table deep join chains to
reuse the wrong CTE. Fixed by `snapshot_cache_key()` in `src/dvm/diff.rs`
which collects leaf Scan aliases in tree order (e.g. `"t1+t2"` vs
`"t1+t2+t3"`).

**Estimated impact:** 10–20% reduction for 4+ table joins. Eliminates ~1
redundant snapshot computation per shared subtree.

**Effort:** Medium (1–2 days). Requires introducing a cache mechanism and
ensuring OpTree identity is stable across calls.

**Risk:** Low-Medium. The main risk is cache invalidation — the snapshot
must correspond to the correct point in the delta computation (pre-change
vs post-change). Since we only cache L₀/R₀ (pre-change), and the pre-change
state is fixed for the entire delta computation, this should be safe.

**Prerequisite:** DI-1 (named CTE for snapshots makes caching natural).

---

### DI-5: Part 3 Correction Consolidation ✅

**Status:** Completed (Part 3 correctness is independent of the inlining threshold).

**Problem:** Part 3 (correction for double-counted rows in nested joins)
generates separate CTEs for each join node in the chain. For a 6-table
join, this can produce 2–4 correction CTEs, each referencing deltas from
both left and right children.

**Analysis:** Part 3 correction is only emitted when `use_pre_change_snapshot`
returns `false` for the left child, which happens only when the left child
contains a semi-join or anti-join. For pure inner-join chains (TPC-H
Q05/Q07/Q08), `use_l0` is always `true` and Part 3 is never emitted —
making the original "consolidation across join levels" approach moot.

**Implementation:** Emit the correction whenever the nested left child and
snapshot strategy require it, regardless of scan count. The
`pg_trickle.part3_max_scan_count` setting controls whether the relevant delta
CTE is forced `NOT MATERIALIZED`; it does not suppress the correction. This
preserves exact results for deep semi-join chains while leaving PostgreSQL's
default CTE materialization choice available above the configured threshold.

---

### DI-6: Lazy Semi-Join R_old Materialization

**Problem:** Semi-join differentiation always materializes `R_old`:

```sql
r_old AS MATERIALIZED (
  SELECT * FROM right_source
  EXCEPT ALL SELECT cols FROM right_delta WHERE action='I'
  UNION ALL SELECT cols FROM right_delta WHERE action='D'
)
```

For EXISTS subqueries, the right side often has high cardinality but the
semi-join only needs to prove existence. Materializing the full R_old is
wasteful when only a few rows from the left delta need to be checked.

**Proposal:** Replace unconditional MATERIALIZED with a heuristic:
- If the right side has a delta (changes occurred), keep MATERIALIZED
  (correctness requires the pre-change snapshot).
- If the right side has **no** delta (no changes to the EXISTS subquery
  tables), skip the EXCEPT ALL entirely and use the current table directly.
  The existing delta-branch pruning (B3-1) should already handle this, but
  the materialization hint is still applied.

Additionally, for cases where R_old must be computed, add a semi-join push-down:

```sql
r_old AS (
  SELECT * FROM right_source
  WHERE right_key IN (SELECT left_key FROM left_delta)
  EXCEPT ALL SELECT cols FROM right_delta WHERE action='I'
  UNION ALL SELECT cols FROM right_delta WHERE action='D'
)
```

This restricts R_old to only rows that could potentially match the left delta,
reducing materialization volume.

**Implementation:**
1. In `semi_join.rs`, check if the right child has any changes (via delta
   source tracking). If not, emit R₁ directly without EXCEPT ALL.
2. For changed right children, extract the equi-join key from the semi-join
   condition and add a `WHERE key IN (...)` filter before EXCEPT ALL.
3. Remove the MATERIALIZED hint when the filtered R_old is expected to be
   small (< estimated threshold).

**Estimated impact:** For Q20-type queries, 50–80% reduction in R_old volume.
The semi-join key filter restricts materialization to only matching rows.

**Effort:** Medium (1–2 days). The equi-join key extraction is already
implemented for inner joins (EC-01). Porting to semi-joins is straightforward.

**Risk:** Low-Medium. The key filter is a pure restriction and doesn't change
correctness. Risk is in the semi-join condition parsing — if the condition
isn't a simple equi-join (e.g., `EXISTS (SELECT 1 WHERE correlated_expr)`),
the filter can't be applied. Need a fallback to the current behavior.

**Prerequisite:** None. Independent of other proposals.

---

### DI-7: Scan-Count-Aware Strategy Selector

**Problem:** The current DVM engine applies the same delta strategy regardless
of join tree complexity. A 2-table join and a 10-table join both use per-leaf
EXCEPT ALL reconstruction. The marginal cost of each additional table is
super-linear due to CTE explosion.

**Proposal:** Introduce a configurable complexity threshold that switches delta
strategy based on join tree characteristics:

| Scan Count | Strategy |
|-----------|----------|
| 1–3 | Full per-leaf EXCEPT ALL (current, optimal for small joins) |
| 4–6 | Named CTE L₀ with materialization (DI-1) + group-key filtering (DI-3) |
| 7+ | Automatic fallback to FULL refresh for the affected stream table |

Additionally, expose two per-stream-table catalog options:
```sql
SELECT pgtrickle.alter_stream_table('my_complex_view',
  refresh_mode => 'auto',        -- try DIFFERENTIAL, fall back
  max_differential_joins => 6,   -- above this, use FULL refresh
  max_delta_fraction => 0.25     -- if delta > 25% of table, use FULL refresh
);
```

The `max_delta_fraction` threshold addresses DI-2's high delta-rate
degradation: the `pk_hash NOT IN (changes)` filter in DI-2 performs well
at low delta rates (<25%) but becomes expensive when the NOT IN set is
large. When `COUNT(*) FROM change_buffer / ST row count > max_delta_fraction`,
auto-fallback to FULL refresh for that cycle.

**Implementation:**
1. Add `join_scan_count()` call at the start of `diff_node()` for join trees.
2. If count exceeds threshold, return a `DiffResult` that signals "use FULL".
3. Add a `max_differential_joins` column to `pgtrickle.pgt_stream_tables`.
4. Add a `max_delta_fraction` column to `pgtrickle.pgt_stream_tables` (default 0.25).
5. In the scheduler, before generating delta SQL, compare
   `change_buffer_count / last_known_row_count`; if above threshold, choose FULL.
6. Default threshold: 6 joins (covers Q07/Q08 but falls back for pathological cases).

**Estimated impact:** Prevents pathological blowup for very complex views.
The `max_delta_fraction` guard prevents DI-2 performance regression under
bulk-load workloads where delta rows rival base table size.

**Effort:** Low–Medium (1–2 days). `join_scan_count` already exists;
`max_delta_fraction` adds one catalog column and a pre-refresh check.

**Risk:** Low. Both are conservative fallbacks — they reduce blast radius
rather than introducing new correctness risk.

**Prerequisite:** DI-2 must land before `max_delta_fraction` is meaningful.
Otherwise most valuable **after** DI-1/DI-3/DI-6 raise the practical threshold.

---

### DI-8: SUM(CASE WHEN …) Algebraic Drift Fix

**Problem:** `is_algebraically_invertible()` in `src/dvm/operators/aggregate.rs`
returns `true` for any `AggFunc::Sum`, including `SUM(CASE WHEN col IN (…)
THEN 1 ELSE 0 END)` patterns (TPC-H Q12: `SUM(CASE WHEN o_orderpriority IN
('1-URGENT','2-HIGH') THEN 1 ELSE 0 END)`).

The algebraic formula `old = new − ins + del` computes both the 'D' and 'I'
halves using the change buffer row's *current* (post-UPDATE) column values.
When `o_orderpriority` changes via UPDATE, the 'D' side of the delta evaluates
the CASE condition with the **new** value rather than the old one — so
`__del_high_line_count` is miscounted and the running total drifts.

This is the same root cause class as EC-01 (join key change split): the
algebraic formula assumes the CASE condition is stable across the row's
lifecycle. It is not, when the condition references non-group-key columns
that can be mutated by UPDATE.

**Parser representation:** The `Expr` enum has no `Case` variant. CASE WHEN
expressions are deparsed to `Expr::Raw("CASE WHEN … END")` by `node_to_expr()`
in `parser.rs:12646`. Detection must therefore use string-prefix matching on
`Expr::Raw`, not structural pattern matching.

**Proposal:** In `is_algebraically_invertible()`, return `false` when the SUM
argument is an `Expr::Raw` whose deparsed SQL starts with `CASE`. The aggregate
falls back to `GROUP_RESCAN` (EXCEPT ALL), which correctly reads pre-change
state via the standard per-leaf EXCEPT ALL path and produces correct old/new
values.

```rust
fn is_algebraically_invertible(agg: &AggExpr) -> bool {
    if agg.is_distinct { return false; }
    // SUM(CASE WHEN …) can drift when the CASE condition references mutable
    // non-group-key columns — fall back to GROUP_RESCAN (EXCEPT ALL).
    if matches!(agg.function, AggFunc::Sum) {
        if let Some(Expr::Raw(s)) = &agg.argument {
            if s.trim_start().to_uppercase().starts_with("CASE") {
                return false;
            }
        }
    }
    matches!(agg.function, AggFunc::CountStar | AggFunc::Count | AggFunc::Sum)
}
```

**Q14 is unaffected:** Q14's target expression is `100.00 * SUM(CASE …) /
CASE WHEN SUM(…) = 0 THEN NULL ELSE SUM(…) END`. Because the top-level
target node is an arithmetic expression (not a bare `T_FuncCall`), the parser
stores it as `AggFunc::ComplexExpression(raw_sql)` with `argument = None` —
not as `AggFunc::Sum`. `ComplexExpression` never matches `is_algebraically_invertible()`
and already takes the GROUP_RESCAN path. DI-8's `Expr::Raw` check on
`AggFunc::Sum` arguments does not apply to Q14 at all.

**Why Q12 drifts but Q14 doesn't:** Q12 has bare `SUM(CASE WHEN ...)` as
the top-level target (parser: `AggFunc::Sum`, algebraic path). Q14's nested
`SUM(CASE WHEN ...)` is inside a multiplication/division (parser:
`AggFunc::ComplexExpression`, GROUP_RESCAN path). The drift is specific to
the algebraic path — GROUP_RESCAN always produces correct results because
it re-evaluates the full expression from the pre-change snapshot.

**Long-term elimination of the band-aid:** When DI-2 lands with aggregate
UPDATE-split support (see below), the algebraic path can evaluate the CASE
condition using `old_*` column values for the 'D' side and `new_*` values
for the 'I' side — making the algebraic formula correct even for mutable
CASE conditions. At that point, this `Expr::Raw` check can be removed and
`SUM(CASE WHEN …)` restored to the faster algebraic path. See DI-2
"Aggregate UPDATE-split" subsection for details.

**Estimated impact:** Q12 passes DIFFERENTIAL correctness; removed from
`DIFFERENTIAL_SKIP_ALLOWLIST`. TPC-H DIFFERENTIAL correctness gate: 20/22
→ 21/22 (combined with DI-1/DI-2 for Q05/Q09: 22/22).

**Effort:** Low (~0.5 day: 1h code + regression E2E run).

**Risk:** Very low. This is a pure restriction — the EXCEPT ALL path already
works correctly for all GROUP_RESCAN aggregates. Performance impact: Q12-style
bare `SUM(CASE WHEN …)` queries switch from algebraic to GROUP_RESCAN, which
is slightly slower but correct. Q14 and all other `ComplexExpression`
aggregates are wholly unaffected. After DI-2, the band-aid can be removed.

**Prerequisite:** None. Fully independent.

---

### DI-9: Scheduler Skips IMMEDIATE-Mode Tables

**Problem:** The scheduler background worker fires every `scheduler_interval_ms`
(hard-capped at 60,000 ms in `src/config.rs`). On every tick it acquires a lock
on *all* due stream tables, including those with `refresh_mode = IMMEDIATE`.
IMMEDIATE-mode stream tables are refreshed inline on every DML transaction by
BEFORE triggers — the scheduler has no incremental work to do for them. Yet
it still acquires the table lock, competing with IMMEDIATE transactions.

In the TPC-H DIFF+IMM comparison test (duration ~328 s), the scheduler fires
~5 times during the test window. Its AccessShareLock acquisitions conflict with
the BEFORE trigger's ExclusiveLock, producing `lock_timeout` cancellations that
the test records as "lock-timeout events" (Q12, Q17, Q19).

**Proposal — two-part fix:**

**(a) Short-term GUC cap lift.** Raise `scheduler_interval_ms` maximum from
`60_000` ms to `600_000` ms in `src/config.rs`. Set `scheduler_interval_ms =
600000` in the TPC-H test GUC configuration so the scheduler does not fire
during the ~328 s test window.

**(b) Semantic fix (correct long-term solution).** In the scheduler's
job-selection loop, skip any stream table whose `refresh_mode = IMMEDIATE` and
which has no DIFFERENTIAL-mode downstream dependants that require a scheduled
catchup. IMMEDIATE tables never accumulate a pending-change backlog that the
scheduler should process — refreshes happen synchronously in the user's
transaction. The scheduler's involvement is both unnecessary and a source of
lock contention.

Code change for (b): in the `is_refresh_due` / pending-change check path,
return `false` early when `st.refresh_mode == RefreshMode::Immediate`. The
scheduler will still manage topology-driven CALCULATED dependants of IMMEDIATE
roots if any exist.

**Estimated impact:** Elimination of lock-timeout events for Q12/Q17/Q19 in
the DIFF+IMM comparison test. Removes false negatives from the CI "deadlock"
counter. Also reduces unnecessary lock contention in production deployments
mixing IMMEDIATE and DIFFERENTIAL stream tables.

**Effort:** Low (0.5 day: config cap change + scheduler guard + test).

**Risk:** Very low. Skipping IMMEDIATE tables in the scheduler is semantically
correct — they are self-refreshing.

**CALCULATED dependant edge case (verified safe):** The concern is whether
skipping IMMEDIATE tables in the scheduler would starve downstream CALCULATED
tables that depend on them. Verification:

1. IMMEDIATE tables refresh synchronously in the user’s transaction, which
   **drains the TABLE-source change buffer** (`changes_{oid}`) as part of each
   refresh cycle.
2. When the scheduler evaluates a downstream CALCULATED table, it calls
   `check_upstream_changes()` → `has_table_source_changes()`, which checks
   `SELECT EXISTS(SELECT 1 FROM changes_{oid} LIMIT 1)`. Since the IMMEDIATE
   root already drained this buffer, the check returns `false`.
3. If the IMMEDIATE root’s refresh produces output changes, those are written
   to the root’s own `changes_pgt_{id}` buffer. Downstream CALCULATED tables
   detect these via `has_stream_table_source_changes()` and are scheduled
   normally — this path is part of the scheduler’s topology walk and is
   independent of whether the IMMEDIATE root itself is "due for refresh".
4. Therefore: skipping IMMEDIATE tables in `is_refresh_due()` does not affect
   downstream CALCULATED scheduling. The scheduler’s job-selection loop for
   CALCULATED tables operates on their own `check_upstream_changes()` result,
   not on their upstream’s refresh-due status.

**Prerequisite:** None. Fully independent.

---

### DI-10: SF=1 Benchmark Validation Target

**Problem:** All TPC-H Phase 10 correctness and speedup measurements are taken
at SF=0.01 (~10 MB data). Real OLAP workloads operate at SF≥1. Improvements
that look good at SF=0.01 may still spill, stall, or lose their speedup
advantage at SF=1 (~1 GB) due to:
- Buffer pool pressure (warm at SF=0.01; cold at SF=1)
- Planner row-count estimates changing join strategy at larger N
- `temp_file_limit` headroom consumed faster at SF=1

**Proposal:** Add a `bench-tpch-sf1` justfile target that runs the TPC-H
`TPCH_BENCH=1` suite with `TPCH_SF=1`. Record one baseline run before Phase 10
ships and one after. Gate v0.13.0 release on at least the correctness check
(22/22 queries pass) at SF=1 in addition to the SF=0.01 gate.

```makefile
bench-tpch-sf1:
    TPCH_BENCH=1 TPCH_SF=1 cargo test --test e2e_tpch_tests \\
        -- --ignored --test-threads=1 --nocapture 2>&1 | tee bench-sf1.log
```

The TPC-H data generation path already reads `TPCH_SF` (or can be parameterised
with a 1-line change). Docker volume requirement: ~1 GB vs. ~10 MB.

**CI placement:** The SF=1 run is too slow for PR checks or daily schedule
(expected runtime: 60–180 minutes including data generation on first run).
It should be triggered via **manual dispatch** (`gh workflow run ci.yml
--ref <branch>`) before cutting the v0.13.0 release, and optionally after
each DI-* item lands to track incremental progress. The CI job should have a
4-hour timeout and run on a dedicated large runner.

**Estimated impact:** SF=1 numbers are the definitive validation that Phase 10
improvements are production-grade. Any regression visible only at SF=1 is
caught before release.

**Effort:** Very low (30 min to add target; ~2–3 h to run and record first
baseline; one-time Docker volume expansion).

**Risk:** None beyond existing TPC-H infrastructure. The SF=1 run is additive;
it does not alter any existing tests.

**Prerequisite:** All DI-1 through DI-9 should be complete (or in progress)
before the SF=1 gate is evaluated for the release blocker.

---

## 5. Dependency Graph

```
DI-1 (Named CTE L₀)
 └──→ DI-2 (Pre-image capture, replaces L₀ CTE body)
 └──→ DI-4 (Shared R₀ cache, builds on named CTEs)

DI-3 (Group-key aggregate filter) — independent
DI-5 (Part 3 consolidation) — independent
DI-6 (Lazy semi-join R_old) — independent
DI-7 (Strategy selector + max_delta_fraction) — after DI-1, DI-2, DI-3, DI-6

DI-8 (SUM(CASE WHEN) drift fix) — independent
DI-9 (Scheduler IMMEDIATE skip) — independent
DI-10 (SF=1 benchmark target) — after DI-1 … DI-9
```

DI-1, DI-3, DI-5, DI-6, DI-8, DI-9 can all be developed in parallel.
DI-2 depends on DI-1 (the named CTEs become the mount point for the pre-image
formula). DI-4 also depends on DI-1. DI-7 should come last among the engine
changes. DI-10 (SF=1 gate) is a validation step, not a code change, and depends
on all others being complete.

---

## 6. Priority & Schedule

| Priority | Proposal | Impact | Effort | Target |
|----------|----------|--------|--------|--------|
| P0 | DI-1: Named CTE L₀ | High (Q05/Q09) | Medium | v0.13 |
| P0 | DI-3: Group-key aggregate filter | Medium-High | Low | v0.13 |
| P0 | DI-8: SUM(CASE WHEN) drift fix | High (Q12 correctness) | Very low | v0.13 |
| P0 | DI-9: Scheduler IMMEDIATE skip | High (Q12/Q17/Q19 lock contention) | Low | v0.13 |
| P1 | DI-2: Pre-image capture | Very high | Medium | v0.13 |
| P1 | DI-6: Lazy semi-join R_old | High (Q20) | Medium | v0.13 |
| P1 | DI-4: Shared R₀ cache | Medium | Medium | v0.13 |
| P2 | DI-7: Strategy selector + max_delta_fraction | Safety net | Low–Medium | v0.13 |
| P2 | DI-5: Part 3 consolidation | Low-Medium | Medium | v0.13 |
| P2 | DI-10: SF=1 benchmark gate | Validation | Very low | v0.13 |

> **Note on DI-2 promotion:** Previously listed as v1.x, requiring "fundamental
> CDC architecture changes". Code inspection of `src/cdc.rs` reveals that
> `old_*` typed columns are already captured in the change buffer for all
> UPDATE and DELETE rows — the CDC trigger change was already done as part
> of the typed-column CDC rewrite. Only the delta SQL generator needs updating.

**Recommended sequence for v0.13:**
1. DI-8 (one-line correctness fix — independent, closes Q12 drift immediately)
2. DI-9 (scheduler IMMEDIATE skip — independent, closes Q12/Q17/Q19 lock contention)
3. DI-1 (named CTEs — unblocks DI-2 and DI-4, quickest win for deep joins)
4. DI-3 (independent, small, high ROI for aggregate queries)
5. DI-2 (replaces EXCEPT ALL in DI-1 CTE bodies with pk-filter + old_* read)
6. DI-6 (semi-join optimization, improves Q20 and similar)
7. DI-4 (builds on DI-1's named CTE infrastructure)
8. DI-7 + max_delta_fraction (safety net after other optimizations raise the bar)
9. DI-5 (correction consolidation — lowest priority, validates last)
10. DI-10 (SF=1 benchmark run — validation gate before v0.13.0 release cut)

**Validation gate:** After DI-8 + DI-9, Q12 leaves `DIFFERENTIAL_SKIP_ALLOWLIST`
and lock-timeout events in the DIFF+IMM comparison test drop to zero. After
DI-1 + DI-3, re-run TPC-H at SF=0.01 with `temp_file_limit = '4GB'` — if
Q05/Q09 pass, DIFFERENTIAL correctness reaches 22/22 (the v0.13.0 gate). After
DI-2 lands, verify that intermediate CTE volume (e.g. via `EXPLAIN (BUFFERS)`)
is measurably reduced. After all items are complete, run DI-10 (SF=1 gate)
to confirm results hold at realistic scale before cutting the release.

---

## 7. Background: EC-01 and EC-01B

### EC-01: Join Key Change Split (v0.10.0) — DONE

Split Part 1 into Part 1a (inserts ⋈ R₁) and Part 1b (deletes ⋈ R₀) so
that updates changing the join key propagate the correct pre-change and
post-change join results.

**Code:** `diff_inner_join()` in `src/dvm/operators/join.rs:109+`

### EC-01B: Per-Leaf CTE Snapshot (v0.12.0) — DONE

Removed the `join_scan_count <= 2` threshold that previously limited
per-leaf snapshot reconstruction to small subtrees. Now uses per-leaf
EXCEPT ALL for **all** join depths, including deep chains (Q07/Q08/Q09).

SemiJoin/AntiJoin subtrees still fall back to post-change snapshots to
avoid the Q21 numwait regression.

**Code:** `use_pre_change_snapshot()` in `src/dvm/operators/join_common.rs:1342+`

### Pre-Image Capture (promoted from v1.x — CDC already complete)

`old_*` column capture in the CDC trigger was part of the typed-column CDC
rewrite (now in production). The DI-2 work is entirely in the delta SQL
generator: replacing `EXCEPT ALL` in `build_pre_change_snapshot_sql()` with
a pk-hash filter + direct `old_*` read. See DI-2 above for implementation
details. The ADR-001/ADR-002 decision to use trigger-based CDC (rather than
logical replication) in `plans/adrs/PLAN_ADRS.md` is unaffected.
