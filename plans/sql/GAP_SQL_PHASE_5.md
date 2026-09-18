# PLAN: SQL Gaps — Phase 5

**Status:** In Progress  
**Date:** 2026-02-24  
**Branch:** `main`  
**Scope:** Next priorities for SQL feature coverage — both new implementations and revisiting rejected constructs.  
**Current state:** 890 unit tests, 22 E2E test suites, 37 AggFunc variants, 21 OpTree variants, 20 diff operators. Zero P0 or P1 issues. All remaining gaps are P2+ with clear error messages.

### Completed Steps

- [x] **C-1: Populate `columns_used` + wire `detect_schema_change_kind()`** — Done 2026-02-24
  - `OpTree::source_columns_used()` + `ParseResult::source_columns_used()` collect per-source column names from Scan nodes
  - `StDependency::insert()` accepts and writes `columns_used` to `pgt_dependencies`
  - `get_for_st()` / `get_all()` read `columns_used` from DB (no longer hardcoded `None`)
  - `api.rs`: extracts column map from `ParseResult` during creation, passes to dependency insert
  - `handle_alter_table()` calls `detect_schema_change_kind()` — benign DDL skips reinit, only column changes trigger reinit + cascade

- [x] **S1: Volatile function detection (A1)** — Done 2026-02-25
  - `lookup_function_volatility()` — SPI query to `pg_proc.provolatile` (with `#[cfg(test)]` stub)
  - `collect_volatilities()` recursive Expr tree scanner
  - `tree_worst_volatility()` / `tree_worst_volatility_with_registry()` OpTree walkers
  - Wired into `create_stream_table_impl()`: DIFF+volatile → reject, DIFF+stable → warn, FULL+volatile → skip
  - 4 new unit tests (876 total, up from 872)

- [x] **S2: TRUNCATE capture in CDC (A6)** — Done 2026-02-25
  - Statement-level `AFTER TRUNCATE` trigger writes `action='T'` marker row to change buffer
  - `execute_differential_refresh()` detects 'T' rows in LSN range → falls back to full refresh
  - `drop_change_trigger()` now also cleans up TRUNCATE trigger + function

- [x] **S3: ALL subquery → AntiJoin (A3)** — Done 2026-02-25
  - Removed ALL_SUBLINK rejection from `check_where_for_unsupported_sublinks()`
  - Added `parse_all_sublink()`: `x op ALL (subq)` → `NOT EXISTS (... WHERE NOT (x op col))` → AntiJoin
  - Operator extracted from `operName` list

- [x] **S4: DISTINCT ON → ROW_NUMBER() (A2)** — Done 2026-02-25
  - `rewrite_distinct_on()` detects DISTINCT ON via raw parse tree, builds ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) = 1 subquery
  - Called BEFORE `validate_defining_query()` so all downstream sees rewritten form
  - Helper functions: `extract_from_clause_sql()`, `from_item_to_sql()`

- [x] **S5: Regression aggregates (A4)** — Done 2026-02-25
  - 12 new AggFunc variants: Corr, CovarPop, CovarSamp, RegrAvgx, RegrAvgy, RegrCount, RegrIntercept, RegrR2, RegrSlope, RegrSxx, RegrSxy, RegrSyy
  - All 12 are group-rescan — diff engine's catch-all handles them automatically
  - Updated `sql_name()`, `is_group_rescan()`, `check_ivm_support_inner()`, `extract_aggregates()`
  - Added `regression_agg()` test helper
  - 37 AggFunc variants total (up from 25)

- [x] **S6: Mixed UNION / UNION ALL (A5)** — Done 2026-02-25
  - `collect_union_children()` no longer rejects mixed trees
  - Children with different `all` flag parsed as separate set operations via `parse_set_operation()`
  - Respects PostgreSQL's nested `SetOperationStmt` tree structure

- [x] **S11: GROUPING SETS / CUBE / ROLLUP (A7/B4)** — Done 2026-02-25
  - `rewrite_grouping_sets()` parse-time rewrite: decomposes GROUPING SETS / CUBE / ROLLUP into UNION ALL of separate GROUP BY queries
  - `expand_grouping_set()` recursively expands CUBE (all 2^n subsets), ROLLUP (prefix subsets), and nested GROUPING SETS
  - Cross-product of multiple grouping set specifications (e.g., `GROUP BY a, ROLLUP(b), CUBE(c)`)
  - `GROUPING(col, …)` calls replaced with computed integer literals per branch
  - Non-grouped columns replaced with `NULL` per branch
  - Rejection removed from `check_select_unsupported()`
  - Wired into `api.rs` before `validate_defining_query()` (same pattern as DISTINCT ON rewrite)
  - 9 new unit tests for `compute_grouping_value` (890 total)

---

## Part A: Top 10 Gaps to Implement Next

These are unimplemented features or correctness gaps ordered by impact-to-effort
ratio. Items higher on the list provide the most value for the least work.

### A1. Non-Deterministic Function Detection

| Field | Value |
|-------|-------|
| **Gap** | Volatile functions (`random()`, `gen_random_uuid()`, `clock_timestamp()`) silently break DIFFERENTIAL delta computation |
| **Current behavior** | No detection — silently accepted, produces phantom changes and broken row hashes |
| **Severity** | **Correctness gap** — not P0 (no wrong SQL generated) but produces wrong *data* |
| **Effort** | 1–2 hours |
| **Impact** | High — prevents silent data corruption for any user using volatile functions |
| **Plan** | Completed non-determinism implementation |

**Implementation:**
- Add `lookup_function_volatility()` — SPI query to `pg_proc.provolatile`
- Add recursive `worst_volatility()` Expr tree scanner
- Add `tree_worst_volatility()` OpTree walker
- Enforce at `create_stream_table()` time:
  - DIFFERENTIAL + volatile → reject with clear error
  - DIFFERENTIAL + stable → warn (e.g., `now()` is safe within a single refresh)
  - FULL + volatile → warn
- ~110 lines of new code + 6 E2E tests

**Why #1:** This is the last remaining **silent correctness gap** in the system.
Every other unsupported construct is either working or rejected with a clear
error. Volatile functions are the only case where the system silently accepts a
query that will produce wrong results.

---

### A2. DISTINCT ON Auto-Rewrite

| Field | Value |
|-------|-------|
| **Gap** | `DISTINCT ON (expr)` rejected → user must manually rewrite to `ROW_NUMBER()` |
| **Current behavior** | Rejected with error suggesting DISTINCT or ROW_NUMBER() |
| **Severity** | P2 — rejected with clear error |
| **Effort** | 6–8 hours |
| **Impact** | Medium — common PostgreSQL idiom for "first row per group" |
| **Source** | Completed earlier SQL-gap work, item S2 |

**Implementation:**
- At parse time, detect `DISTINCT ON` with non-empty `distinctClause`
- Transparently rewrite to a subquery:
  ```sql
  -- Input:
  SELECT DISTINCT ON (customer_id) customer_id, order_id, created_at
  FROM orders ORDER BY customer_id, created_at DESC

  -- Rewrite to:
  SELECT customer_id, order_id, created_at FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY customer_id ORDER BY created_at DESC
    ) AS __pgt_rn FROM orders
  ) __pgt_don WHERE __pgt_rn = 1
  ```
- Reuses existing Window + Filter operators — no new OpTree variant needed
- ~150 lines in `parser.rs` + unit tests + E2E tests

**Why #2:** DISTINCT ON is one of the most frequently used PostgreSQL-specific
constructs. The auto-rewrite is clean, well-understood, and reuses existing
infrastructure.

---

### A3. ALL (subquery)

| Field | Value |
|-------|-------|
| **Gap** | `WHERE col > ALL (SELECT ...)` rejected |
| **Current behavior** | Rejected with error suggesting NOT EXISTS rewrite |
| **Severity** | P2 — rejected with clear error |
| **Effort** | 4–6 hours |
| **Impact** | Low-Medium — used in analytical queries |
| **Source** | Completed earlier SQL-gap work, item E3 |

**Implementation:**
- `ALL (subquery)` is the dual of `ANY (subquery)`:
  `col > ALL(S)` ≡ `NOT EXISTS (SELECT 1 FROM S WHERE NOT (col > S.val))`
- Rewrite `ALL_SUBLINK` to `AntiJoin` with negated condition
- Follows the exact same pattern as `IN/NOT IN → SemiJoin/AntiJoin`
- ~80 lines: parser rewrite + reuse existing AntiJoin operator

**Why #3:** Minimal effort — follows the established AntiJoin pattern exactly.
Covers the last missing subquery expression type.

---

### A4. Regression Aggregates (11 functions)

| Field | Value |
|-------|-------|
| **Gap** | `CORR`, `COVAR_POP`, `COVAR_SAMP`, `REGR_AVGX/AVGY/COUNT/INTERCEPT/R2/SLOPE/SXX/SXY` rejected |
| **Current behavior** | Recognized and rejected with error suggesting FULL mode |
| **Severity** | P2 — rejected with clear error |
| **Effort** | 4–6 hours |
| **Impact** | Low — niche statistical use, but covers 11 gap items at once |
| **Source** | Completed earlier SQL-gap work, item A3 |

**Implementation:**
- All 11 use the proven group-rescan pattern (copy-paste of existing aggregates)
- Add 11 new `AggFunc` enum variants
- Each returns NULL sentinel on group change → triggers re-aggregation from source
- Mechanical work — no new logic, no new operators
- ~200 lines (enum variants + match arms) + unit tests

**Why #4:** Closes the largest single batch of gap items (11 functions) with
minimal new code. All follow the exact same pattern as STDDEV/VARIANCE.

---

### A5. Mixed UNION / UNION ALL

| Field | Value |
|-------|-------|
| **Gap** | Queries mixing `UNION` and `UNION ALL` rejected |
| **Current behavior** | Rejected with error |
| **Severity** | P2 — rejected with clear error |
| **Effort** | 4–6 hours |
| **Impact** | Low — uncommon pattern, but a completeness gap |
| **Source** | Completed earlier SQL-gap work, item S3 |

**Implementation:**
- PostgreSQL's parser already produces a nested tree for mixed set ops:
  `A UNION B UNION ALL C` → `SetOperationStmt(UNION ALL, SetOperationStmt(UNION, A, B), C)`
- The DVM parser currently flattens this; instead, respect the nesting
- Each nested `SetOperationStmt` maps to the appropriate operator (UnionAll or
  Intersect/Except with dedup)
- ~100 lines in parser.rs set-operation handling

**Why #5:** Low effort, improves completeness. The fix is mostly about
respecting PostgreSQL's already-correct parse tree structure.

---

### A6. TRUNCATE Capture in CDC

| Field | Value |
|-------|-------|
| **Gap** | `TRUNCATE` on a source table not detected — stream table becomes silently stale |
| **Current behavior** | No detection, no error, stale data |
| **Severity** | **Correctness gap** — similar to A1 |
| **Effort** | 4–6 hours |
| **Impact** | Medium — TRUNCATE is common in ETL workflows |

**Implementation:**
- Add `AFTER TRUNCATE` statement-level trigger alongside the existing row-level
  triggers in `src/cdc.rs`
- The truncate trigger function marks all dependent stream tables for
  reinitialization (`needs_reinit = true`)
- For WAL-mode CDC, logical decoding naturally captures TRUNCATE messages —
  add handling in `src/wal_decoder.rs`
- ~80 lines in `cdc.rs` + ~30 lines in `wal_decoder.rs` + E2E tests

**Why #6:** Second remaining silent correctness gap (after volatile functions).
Simple to implement with `AFTER TRUNCATE` triggers.

---

### A7. GROUPING SETS / CUBE / ROLLUP

| Field | Value |
|-------|-------|
| **Gap** | Advanced aggregation groupings rejected |
| **Current behavior** | Rejected with error suggesting separate STs + UNION ALL |
| **Severity** | P2 — rejected with clear error |
| **Effort** | 10–15 hours |
| **Impact** | Medium — used in reporting and OLAP queries |
| **Source** | Completed earlier SQL-gap work, item S1 |

**Implementation (preferred: parse-time rewrite):**
- Decompose into multiple GROUP BY queries combined with UNION ALL
- `GROUPING SETS ((a), (b), (a,b))` →
  ```sql
  SELECT a, NULL AS b, SUM(x) FROM t GROUP BY a
  UNION ALL
  SELECT NULL AS a, b, SUM(x) FROM t GROUP BY b
  UNION ALL
  SELECT a, b, SUM(x) FROM t GROUP BY a, b
  ```
- Add `GROUPING()` function results as literal columns in the rewrite
- Reuses existing Aggregate + UnionAll operators
- ~250 lines (parser rewrite + GROUPING() synthesis)
- CUBE and ROLLUP are syntactic sugar generating specific grouping set
  combinations — handled by enumerating them

**Why #7:** High effort but important for analytical workloads. The UNION ALL
rewrite avoids needing a new OpTree variant.

---

### A8. Multiple PARTITION BY in Window Functions

| Field | Value |
|-------|-------|
| **Gap** | Window functions with different `PARTITION BY` clauses rejected |
| **Current behavior** | Rejected with error — must share same PARTITION BY |
| **Severity** | P2 — rejected with clear error |
| **Effort** | 8–10 hours |
| **Impact** | Low — edge case, but blocks legitimate analytics queries |
| **Source** | Completed earlier SQL-gap work, item S4 |

**Implementation (multi-pass recomputation):**
- Group window functions by their PARTITION BY clause
- For each distinct partitioning, run a separate recomputation pass
- The superset of affected partitions across all passes forms the final delta
- Requires splitting a single Window OpTree node into multiple passes with a
  final join to combine results
- ~200 lines: parser grouping + multi-pass window operator

**Why #8:** Medium-high effort. Legitimate use case but workaround (split into
separate stream tables) exists.

---

### A9. Recursive CTE in DIFFERENTIAL Mode (Incremental Fixpoint)

| Field | Value |
|-------|-------|
| **Gap** | WITH RECURSIVE rejected in DIFFERENTIAL mode → must use FULL |
| **Current behavior** | Rejected with clear error |
| **Severity** | P2 |
| **Effort** | 15–20 hours |
| **Impact** | Medium — recursive CTEs are common for graph traversal, hierarchies |

**Implementation:**
- Currently handled by recomputation-diff (full re-execution + anti-join against
  storage) which is functional but forces FULL mode
- Incremental approach: semi-naive evaluation with Delete-and-Rederive (DRed):
  1. Propagate source deltas through the non-recursive (base) term
  2. Iterate the recursive term with only new rows until fixpoint
  3. For deletions: over-delete, then rederive to restore rows with alternative
     derivations
- Requires monotonicity analysis to determine if convergence is guaranteed
- New operator: `IncrementalRecursiveCte` with iteration loop in delta SQL
- ~400 lines: new operator + monotonicity checker + iteration control

**Why #9:** High effort and complexity, but recursive CTEs are the only major
SQL feature category that is mode-restricted. Enabling them in DIFFERENTIAL mode
would significantly expand the extension's coverage.

---

### A10. Scalar Subquery in WHERE

| Field | Value |
|-------|-------|
| **Gap** | `WHERE col > (SELECT avg(x) FROM t)` rejected in DIFFERENTIAL mode |
| **Current behavior** | Rejected with error suggesting JOIN or CTE rewrite |
| **Severity** | P2 |
| **Effort** | 6–8 hours |
| **Impact** | Low — JOIN/CTE rewrite is straightforward |
| **Source** | Completed earlier SQL-gap work, item E1 |

**Implementation:**
- Auto-rewrite to a CROSS JOIN with the scalar subquery:
  ```sql
  -- Input:
  SELECT * FROM orders WHERE amount > (SELECT avg(amount) FROM orders)
  -- Rewrite to:
  SELECT o.* FROM orders o
  CROSS JOIN (SELECT avg(amount) AS __pgt_scalar FROM orders) __pgt_sq
  WHERE o.amount > __pgt_sq.__pgt_scalar
  ```
- The CROSS JOIN produces exactly one row from the scalar subquery
- Reuses existing InnerJoin operator for delta computation
- ~120 lines in parser.rs (SubLink detection + rewrite)

**Why #10:** Medium effort. The auto-rewrite approach avoids needing per-row
value-change tracking. Works for the common case of simple scalar comparisons.

---

## Part B: Top 10 Rejected Constructs to Revisit

These are constructs currently rejected with clear error messages that should be
reconsidered for implementation. Ordered by user impact and feasibility.

### B1. DISTINCT ON → Auto-Rewrite to Window Function

| Field | Value |
|-------|-------|
| **Current rejection** | *"DISTINCT ON is not supported. Use DISTINCT or ROW_NUMBER() OVER (...) = 1."* |
| **Recommendation** | **Implement** — auto-rewrite at parse time (same as A2 above) |
| **Effort** | 6–8 hours |
| **User impact** | High — extremely common PostgreSQL idiom |

The ROW_NUMBER() rewrite is clean and mechanical. Users shouldn't need to
manually restructure their queries for this common pattern.

---

### B2. ALL (subquery) → Anti-Join Rewrite

| Field | Value |
|-------|-------|
| **Current rejection** | *"ALL (subquery) is not supported. Use NOT EXISTS with negated condition."* |
| **Recommendation** | **Implement** — rewrite to AntiJoin with negated condition (same as A3 above) |
| **Effort** | 4–6 hours |
| **User impact** | Medium — completes subquery expression coverage |

Follows the exact same pattern as the existing `IN → SemiJoin` rewrite. The
NOT EXISTS alternative works but is unnecessarily verbose for users.

---

### B3. Mixed UNION / UNION ALL → Respect Nested Parse Tree

| Field | Value |
|-------|-------|
| **Current rejection** | *"Mixed UNION / UNION ALL not supported. Use all UNION or all UNION ALL."* |
| **Recommendation** | **Implement** — respect PostgreSQL's nested SetOperationStmt tree (same as A5) |
| **Effort** | 4–6 hours |
| **User impact** | Low-Medium — completeness improvement |

The parser already has the information; it just needs to stop flattening mixed
set operations into a single list.

---

### B4. GROUPING SETS / CUBE / ROLLUP → UNION ALL Decomposition

| Field | Value |
|-------|-------|
| **Current rejection** | *"GROUPING SETS is not supported. Use separate stream tables with UNION ALL, or FULL refresh mode."* |
| **Recommendation** | **Implement** — parse-time UNION ALL rewrite (same as A7) |
| **Effort** | 10–15 hours |
| **User impact** | Medium — important for OLAP and reporting |

The current workaround (manual decomposition) is tedious and error-prone for
CUBE (which generates $2^n$ grouping sets). Automatic decomposition is cleaner.

---

### B5. Recursive CTE in DIFFERENTIAL → Incremental Fixpoint

| Field | Value |
|-------|-------|
| **Current rejection** | *"Recursive CTE is not supported in DIFFERENTIAL mode. Use FULL refresh mode."* |
| **Recommendation** | **Implement for monotone cases** — semi-naive evaluation with DRed (same as A9) |
| **Effort** | 15–20 hours |
| **User impact** | Medium — graph traversal, hierarchies, transitive closure |

At minimum, monotone recursive CTEs (containing only JOINs, UNIONs, filters)
should be allowed. Non-monotone recursive CTEs (containing EXCEPT, aggregates)
can remain rejected or require explicit opt-in.

---

### B6. SubLinks Inside OR → OR-to-UNION Rewrite

| Field | Value |
|-------|-------|
| **Current rejection** | *"Subquery expressions (EXISTS/IN) inside OR conditions are not supported in DIFFERENTIAL mode. Use UNION or separate stream tables."* |
| **Recommendation** | **Implement with caution** — auto-rewrite `WHERE A OR EXISTS(...)` to `UNION` |
| **Effort** | 8–10 hours |
| **User impact** | Low-Medium — uncommon but catches users off guard |

**Rewrite approach:**
```sql
-- Input:
SELECT * FROM t WHERE status = 'active' OR EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id)
-- Rewrite to:
SELECT * FROM t WHERE status = 'active'
UNION
SELECT t.* FROM t JOIN vip ON vip.id = t.id
```

The OR-to-UNION rewrite is well-known from query optimization literature.
Challenge: ensuring the rewrite is equivalent when the OR arms are not
independent (overlapping results handled by UNION dedup).

---

### B7. Scalar Subquery in WHERE → CROSS JOIN Rewrite

| Field | Value |
|-------|-------|
| **Current rejection** | *"Scalar subquery in WHERE is not supported in DIFFERENTIAL mode. Use JOIN or CTE."* |
| **Recommendation** | **Implement** — auto-rewrite to CROSS JOIN (same as A10) |
| **Effort** | 6–8 hours |
| **User impact** | Low-Medium — common pattern in analytical queries |

The CROSS JOIN approach handles the typical case (`WHERE col > (SELECT
avg(...))`) cleanly. More complex correlated scalar subqueries in WHERE remain
harder but are exceedingly rare.

---

### B8. Multiple PARTITION BY → Multi-Pass Recomputation

| Field | Value |
|-------|-------|
| **Current rejection** | *"All window functions must share the same PARTITION BY clause."* |
| **Recommendation** | **Implement** — multi-pass approach (same as A8) |
| **Effort** | 8–10 hours |
| **User impact** | Low — workaround exists (separate stream tables) |

This restriction is more of an implementation limitation than a fundamental
design constraint. Multi-pass recomputation is correct and bounded.

---

### B9. NATURAL JOIN → Catalog-Resolved Rewrite

| Field | Value |
|-------|-------|
| **Current rejection** | *"NATURAL JOIN is not supported. Use explicit JOIN ... ON."* |
| **Recommendation** | **Keep rejection** — NATURAL JOIN is fragile and considered poor practice |
| **Effort** | 6–8 hours (if implemented) |
| **User impact** | Very low — explicit JOINs are universally preferred |

NATURAL JOIN silently changes semantics when columns are added to either table.
The rejection error already suggests the correct alternative. Implementation
would require catalog access at parse time (`pg_attribute` lookup) to resolve
common column names, adding complexity for minimal user value.

**Verdict:** Keep rejection. The error message is helpful and the alternative
is better practice.

---

### B10. Remaining Rejected Aggregates (17 functions)

| Field | Value |
|-------|-------|
| **Current rejection** | *"Aggregate function X() is not supported in DIFFERENTIAL mode. Use FULL refresh mode."* |
| **Recommendation** | **Implement regression aggregates (11)**, keep rejection for hypothetical-set (4) and XMLAGG (1) |
| **Effort** | 4–6 hours for regression; 4–6 hours for hypothetical-set if desired |
| **User impact** | Low — niche use cases |

**Breakdown:**
- **Regression aggregates** (CORR, COVAR_*, REGR_* — 11 functions): Same group-rescan
  pattern as existing aggregates. Implement on demand. (Same as A4.)
- **Hypothetical-set** (RANK, DENSE_RANK, PERCENT_RANK, CUME_DIST as aggregates):
  Almost always used as window functions, not aggregates. Keep rejection.
- **XMLAGG**: Extremely niche. Keep rejection.

---

## Priority Summary

### Recommended Execution Order

> **Note:** Session C-1 is complete. The table below reflects only remaining
> work. Items are ordered by impact-to-effort ratio, with correctness gaps
> first, then high-value features, then schema infrastructure, then
> diminishing-returns items.

| Session | Items | Total Effort | Cumulative Value |
|---------|-------|-------------|------------------|
| ~~**C-1**~~ | ~~Populate `columns_used` + wire `detect_schema_change_kind()`~~ | ~~4–5h~~ | ~~✅ DONE — benign DDL skips reinit~~ |
| **1** | A1 (volatile functions) + A3 (ALL subquery) | 5–8 hours | Closes last silent correctness gap + completes subquery coverage |
| **2** | A2 (DISTINCT ON) | 6–8 hours | Unlocks common PG idiom |
| **3** | A4 (regression aggregates) + A5 (mixed UNION) | 8–12 hours | Covers 12 gap items in one session |
| **4** | A6 (TRUNCATE capture) | 4–6 hours | Closes second correctness gap |
| **5** | A7 (GROUPING SETS) | 10–15 hours | Major OLAP feature |
| **6+** | A8–A10 (multi-PARTITION, recursive CTE, scalar WHERE) | 29–38 hours | Diminishing returns |

### Items to Keep as Rejections

These items are best left rejected with their current error messages:

| Item | Reason |
|------|--------|
| **NATURAL JOIN** | ~~Fragile, poor practice~~ → **Moved to S9** (implementable under schema policy, see Part C) |
| **LIMIT / OFFSET** | Fundamental design (stream tables are full result sets) |
| **FOR UPDATE / FOR SHARE** | No row-level locking on stream tables |
| **TABLESAMPLE** | Stream tables materialize complete result sets |
| **ROWS FROM() multi-function** | Extremely niche, single SRF covers all practical use |
| **Hypothetical-set aggregates** | Almost always used as window functions |
| **XMLAGG** | Extremely niche |
| **Window functions in expressions** | Architectural constraint; separate column is cleaner |
| **LATERAL with RIGHT/FULL JOIN** | PostgreSQL itself restricts this |

---

## Success Criteria (Parts A + B)

After Steps S1–S6 (highest-impact work):

- [x] Volatile functions rejected in DIFFERENTIAL mode with clear error (S1)
- [x] Stable functions produce a warning in DIFFERENTIAL mode (S1)
- [x] `DISTINCT ON` auto-rewritten to ROW_NUMBER() window function (S4)
- [x] `ALL (subquery)` supported via AntiJoin rewrite (S3)
- [x] 12 regression aggregates supported in DIFFERENTIAL mode (S5)
- [x] Mixed UNION / UNION ALL works correctly (S6)
- [x] TRUNCATE on source tables triggers full refresh fallback (S2)
- [x] 37 AggFunc variants (up from 25)
- [x] 876 unit tests (up from 872)
- [ ] Documentation updated across SQL_REFERENCE, DVM_OPERATORS, README

---

## Historical Progress Summary

| Plan | Sessions | Items Resolved | Test Growth |
|------|----------|---------------|-------------|
| PLAN_SQL_GAPS_1 | 1 | Window detection, CROSS JOIN, FOR UPDATE, report cleanup | 745 → 757 |
| PLAN_SQL_GAPS_2 | 1 | GROUPING SETS P0, GROUP BY hardening, TABLESAMPLE | 745 → 750 |
| PLAN_SQL_GAPS_3 | ~5 | 5 new aggregates + 3 subquery operators | 750 → 809 |
| PLAN_SQL_GAPS_4 | 1 | Report accuracy + 3 ordered-set aggregates | 809 → 826 |
| Hybrid CDC + user triggers + pgt_ rename | ~3 | Hybrid CDC, user triggers, 72-file rename | 826 → 872 |
| **SQL_GAPS_5 C-1** | **1** | **columns_used population + smart schema change detection** | **872 (no new tests yet)** |
| **SQL_GAPS_5** | **~10** | **Target: volatile detection, DISTINCT ON, ALL subquery, 11 regression aggs, mixed UNION, TRUNCATE, schema infra, NATURAL JOIN, keyless tables** | **872 → 920+** |

---

## Part C: Schema-Dependent Features Under Policy Change

Some features in Part B were rejected because they are fragile when users make
changes to the database schema (adding/dropping columns, changing types, etc.).
This section reassesses those features under two policy assumptions:

- **Policy 1 — DDL Blocking:** Prevent schema changes on tracked source tables
- **Policy 2 — Detection + Smart Reinit:** Capture schema state at creation
  time and reinitialize intelligently when changes are detected

Both policies build on partially-implemented infrastructure that already exists
in the codebase.

---

### C0. Current Infrastructure Audit

Three mechanisms were identified. **C0-a and C0-b are now complete** (session
C-1). C0-c remains for future work (session C-2).

> **Status after C-1:** `columns_used` is populated at creation time for
> DIFFERENTIAL-mode STs. `detect_schema_change_kind()` is wired into
> `handle_alter_table()`. Benign DDL (indexes, comments, statistics) and
> constraint-only changes no longer trigger unnecessary reinitialization.

#### C0-a. `columns_used TEXT[]` in `pgt_dependencies` — ✅ DONE

| Field | Value |
|-------|-------|
| **Schema column** | `pgt_dependencies.columns_used TEXT[]` ([src/lib.rs](../../src/lib.rs) table DDL) |
| **Rust struct** | `StDependency.columns_used: Option<Vec<String>>` ([src/catalog.rs L82](../../src/catalog.rs#L82)) |
| **Insert** | `StDependency::insert()` now accepts `Option<Vec<String>>` and writes to DB |
| **Read** | `get_for_st()` and `get_all()` now read `columns_used` from query results |
| **Source** | `ParseResult::source_columns_used()` + `OpTree::source_columns_used()` collect per-source column names from Scan nodes |
| **API** | `create_stream_table_impl()` extracts column map from `ParseResult` and passes to dependency insert |

#### C0-b. `detect_schema_change_kind()` — ✅ DONE

| Field | Value |
|-------|-------|
| **Location** | [src/hooks.rs L590](../../src/hooks.rs#L590) |
| **Purpose** | Classifies ALTER TABLE changes as `ColumnChange`, `ConstraintChange`, or `Benign` |
| **Mechanism** | Compares `columns_used` (from `pgt_dependencies`) against current `pg_attribute` |
| **Status** | **Wired in.** `handle_alter_table()` calls `detect_schema_change_kind()` per-ST and only reinits on `ColumnChange`. `Benign` and `ConstraintChange` skip reinit. |

#### C0-c. No Column Snapshot at Creation Time

`validate_defining_query()` at [src/api.rs L775](../../src/api.rs#L775) runs
`SELECT * FROM (query) sub LIMIT 0` to validate syntax and resolve output
columns, but this metadata is only used to build the storage table DDL — it is
**not persisted** in the catalog for later comparison.

Similarly, `resolve_columns()` at [src/dvm/parser.rs L3413](../../src/dvm/parser.rs#L3413)
queries `pg_attribute` for source column names, type OIDs, and nullability, but
this data is consumed by the OpTree builder and then discarded.

---

### C1. Policy Option 1: DDL Blocking

Add an event trigger that **ERRORs** on schema-altering DDL for any source
table used by a stream table.

| Field | Value |
|-------|-------|
| **Mechanism** | Extend `_on_ddl_end` at [src/hooks.rs L51](../../src/hooks.rs#L51) to `ERROR` instead of reinit when `pg_trickle.block_source_ddl` GUC is `true` |
| **New GUC** | `pg_trickle.block_source_ddl` (boolean, default `false`) in [src/config.rs](../../src/config.rs) |
| **Scope** | ALTER TABLE (ADD/DROP/RENAME/ALTER COLUMN, DROP CONSTRAINT) on tables referenced by any stream table |
| **Exempt** | CREATE INDEX, COMMENT ON, ALTER TABLE SET STATISTICS — benign ops that don't affect column structure |
| **Effort** | 3–4 hours |

**Implementation:**
```
_on_ddl_end():
  if cmd == ALTER TABLE && is_source_of_any_st(objid):
    if guc::block_source_ddl():
      let kind = detect_schema_change_kind(...)  // already built
      if kind == ColumnChange:
        error!("ALTER TABLE blocked: table is a source for stream tables. \
                Set pg_trickle.block_source_ddl = false to allow.")
      // ConstraintChange and Benign pass through
```

**What this unlocks:** Any schema-dependent feature becomes safe because the
source table columns are guaranteed not to change. NATURAL JOIN, `SELECT *`
expansion, keyless table PK detection — all can rely on creation-time catalog
state remaining stable.

**Drawback:** Blocks legitimate schema evolution. Users who need to `ALTER TABLE`
must temporarily disable the GUC, which defeats the protection. Best suited for
production workloads where source schemas are stable.

---

### C2. Policy Option 2: Detection + Smart Reinit

Wire in the existing infrastructure and add column snapshots so the system can
**detect** what changed and respond intelligently (targeted reinit, warning, or
error depending on the feature).

#### Step 1: Populate `columns_used` — ✅ DONE

During stream table creation (`pgtrickle.create()` → `StDependency::insert()`),
column names from `resolve_columns()` results are passed through the API layer
into catalog storage.

- `ParseResult::source_columns_used()` collects `(table_oid, Vec<column_name>)`
  from all Scan nodes in the OpTree and CTE registry
- `create_stream_table_impl()` extracts this map and passes to
  `StDependency::insert()` per source dependency
- `StDependency::insert()` writes `columns_used` into the SQL INSERT
- `get_for_st()` and `get_all()` read the column from query results

**Benefit:** `detect_schema_change_kind()` now produces accurate classifications
instead of always returning `ColumnChange`.

#### Step 2: Wire `detect_schema_change_kind()` — ✅ DONE

`handle_alter_table()` now calls `detect_schema_change_kind()` per affected ST:

- `Benign` → skip reinit (log at debug level)
- `ConstraintChange` → skip reinit (future S10 may reinit for PK changes)
- `ColumnChange` → reinit + rebuild CDC trigger + cascade to downstream STs
- WAL fallback only triggered on column changes
- Falls back to conservative `ColumnChange` on detection errors

**Benefit:** Benign DDL (adding indexes, comments, statistics) no longer
triggers unnecessary reinitialization.

#### Step 3: Store Column Snapshot (3–4 hours)

Add a new catalog table or extend `pgt_dependencies` with:

| Column | Type | Purpose |
|--------|------|---------|
| `column_snapshot` | `JSONB` | Array of `{name, type_oid, ordinal}` at creation time |
| `schema_fingerprint` | `TEXT` | SHA-256 of serialized column snapshot — fast equality check |

On each DDL event, compare the current `pg_attribute` state against the stored
snapshot. This enables precise change detection:

- Column added → warn, reinit (new column may affect NATURAL JOIN, SELECT *)
- Column dropped → error if column is in `columns_used`, benign otherwise
- Column type changed → reinit (type coercion paths may differ)
- Column renamed → reinit if column is in `columns_used`

#### Step 4: NATURAL JOIN Column Resolution Snapshot (2 hours)

For NATURAL JOIN specifically, store the resolved common-column names at
creation time in `pgt_dependencies` or the column snapshot. On reinit:

1. Re-resolve common columns from current `pg_attribute`
2. Compare against stored list
3. If changed: emit `WARNING` explaining semantic drift, then reinit with new
   column set
4. Optionally: set stream table to ERROR status instead, requiring user to
   explicitly `ALTER STREAM TABLE ... REINITIALIZE`

**Total effort for Policy 2:** 9–13 hours across Steps 1–4.

---

### C3. Feature Reassessment Under Policy Change

#### Features with True Schema Dependency

These features were rejected (or not yet implemented) specifically because they
depend on catalog state that could change after creation.

| Feature | Item | Without Policy | DDL Blocking (C1) | Detection + Reinit (C2) | Recommendation |
|---------|------|---------------|-------------------|------------------------|----------------|
| **NATURAL JOIN** | B9 | Fragile — adding a column silently changes join condition | **Safe** — columns can't change | **Safe** — reinit regenerates join condition; warns if semantics changed | **Implement** under either policy |
| **SELECT \*** | — | Implicit column set changes silently | **Safe** — can't add/drop columns | **Safe** — re-resolve `*` on reinit, propagate to storage table | **Implement** under C2 |
| **Volatile function detection** | A1 | `pg_proc.provolatile` could change if function is replaced | No impact (orthogonal to DDL) | Store `provolatile` at creation time, compare on reinit | **Implement regardless** — risk is extremely low |
| **Keyless tables** | ADR-072 | PK addition/removal changes row identity strategy | **Safe** — PK can't be added/dropped | **Safe** — detect PK change as `ConstraintChange`, reinit with new strategy | **Implement** under either policy |
| **Type coercion** | ADR-071 | Implicit casts from `pg_cast` change if types change | **Safe** | **Safe** — type OIDs in snapshot detect changes | **Implement regardless** — changing column types is already caught by reinit |

**NATURAL JOIN detail:** Implementation would require `pg_attribute` lookup at
parse time to resolve common column names, then synthesize an explicit equi-join.
~6–8 hours of parser work. With Policy C2 Step 4, the resolved column names
are stored, enabling semantic drift detection on reinit.

**Keyless tables detail:** Without a PK, row identity falls back to an
all-column content hash for `__pgt_row_id`. If a PK is later added, the row
identity strategy should switch. Under Policy C2 Step 2, `ConstraintChange` is
detected and triggers reinit. ~4–6 hours for the initial keyless implementation.

#### Features with Low or Zero Schema Dependency

These items from Parts A and B are implementable **regardless of policy choice**
because they reference only explicit query-level constructs, not implicit catalog
state:

| Feature | Items | Schema Dependency | Why Safe |
|---------|-------|-------------------|----------|
| DISTINCT ON auto-rewrite | A2/B1 | None | PARTITION BY and ORDER BY expressions are explicit in the query |
| ALL (subquery) | A3/B2 | None | Follows existing AntiJoin pattern — no catalog resolution |
| Regression aggregates | A4/B10 | None | Mechanical group-rescan pattern — same as existing aggregates |
| Mixed UNION / UNION ALL | A5/B3 | None | Respects PostgreSQL's nested `SetOperationStmt` tree — no catalog access |
| TRUNCATE capture | A6 | None | Trigger-based — detects the event, not schema state |
| GROUPING SETS / CUBE / ROLLUP | A7/B4 | None | GROUP BY columns are explicit in the query |
| Multiple PARTITION BY | A8/B8 | None | Partition keys are explicit in the query |
| Recursive CTE (incremental) | A9/B5 | None | Monotonicity analysis is on the OpTree structure, not live catalog |
| Scalar subquery in WHERE | A10/B7 | None | CROSS JOIN rewrite uses explicit column references |
| SubLinks inside OR | B6 | None | OR-to-UNION rewrite uses explicit predicate structure |

---

### C4. Master Implementation Order (All Remaining Steps)

With C-1 complete, the full implementation order for all remaining work across
Parts A, B, and C. Each step is self-contained and can be committed
independently. Steps are numbered sequentially for easy reference.

#### Tier 1 — Correctness Gaps (must-fix)

| Step | Item(s) | Effort | Delivers | Prereqs |
|------|---------|--------|----------|----------|
| ~~C-1~~ | ~~Populate `columns_used` + wire `detect_schema_change_kind()`~~ | ~~4–5h~~ | ~~✅ DONE~~ | — |
| ~~S1~~ | ~~A1: Volatile function detection~~ | ~~1–2h~~ | ~~✅ DONE~~ | — |
| ~~S2~~ | ~~A6: TRUNCATE capture in CDC~~ | ~~4–6h~~ | ~~✅ DONE~~ | — |

#### Tier 2 — High-Value SQL Features (best ROI)

| Step | Item(s) | Effort | Delivers | Prereqs |
|------|---------|--------|----------|----------|
| ~~S3~~ | ~~A3: ALL (subquery) → AntiJoin rewrite~~ | ~~4–6h~~ | ~~✅ DONE~~ | — |
| ~~S4~~ | ~~A2: DISTINCT ON → ROW_NUMBER() auto-rewrite~~ | ~~6–8h~~ | ~~✅ DONE~~ | — |
| ~~S5~~ | ~~A4: Regression aggregates (12 functions)~~ | ~~4–6h~~ | ~~✅ DONE~~ | — |
| ~~S6~~ | ~~A5: Mixed UNION / UNION ALL~~ | ~~4–6h~~ | ~~✅ DONE~~ | — |

#### Tier 3 — Schema Infrastructure (enables Tier 4)

| Step | Item(s) | Effort | Delivers | Prereqs |
|------|---------|--------|----------|----------|
| ~~**S7**~~ | ~~C-2: Column snapshot + schema fingerprint~~ | ~~3–4h~~ | ~~✅ DONE~~ | C-1 ✅ |
| ~~**S8**~~ | ~~C-3: `pg_trickle.block_source_ddl` GUC~~ | ~~3–4h~~ | ~~✅ DONE~~ | — |

#### Tier 4 — Schema-Dependent Features (newly unlocked)

| Step | Item(s) | Effort | Delivers | Prereqs |
|------|---------|--------|----------|----------|
| ~~**S9**~~ | ~~C-4: NATURAL JOIN with column snapshot~~ | ~~6–8h~~ | ~~✅ DONE~~ | S7 ✅ |
| ~~**S10**~~ | ~~C-5: Keyless table support (ADR-072)~~ | ~~4–6h~~ | ~~✅ DONE~~ | C-1 ✅ |

#### Tier 5 — OLAP & Advanced Features (diminishing returns)

| Step | Item(s) | Effort | Delivers | Prereqs |
|------|---------|--------|----------|---------|
| ~~**S11**~~ | ~~A7: GROUPING SETS / CUBE / ROLLUP~~ | ~~10–15h~~ | ~~✅ DONE~~ | — |
| ~~**S12**~~ | ~~A10: Scalar subquery in WHERE~~ | ~~6–8h~~ | ~~✅ DONE~~ | — |
| ~~**S13**~~ | ~~B6: SubLinks inside OR~~ | ~~8–10h~~ | ~~✅ DONE~~ | — |
| ~~**S14**~~ | ~~A8: Multiple PARTITION BY in windows~~ | ~~8–10h~~ | ~~✅ DONE~~ | — |
| ~~**S15**~~ | ~~A9: Recursive CTE in DIFFERENTIAL mode~~ | ~~15–20h~~ | ~~✅ DONE~~ | — |

#### Summary

| Tier | Steps | Total Effort | Cumulative Items Resolved |
|------|-------|-------------|---------------------------|
| ✅ Done | C-1, S1–S15 | ~68–90h | All Tier 1–5 items complete |
| ~~1 — Correctness~~ | ~~S1–S2~~ | ~~5–8h~~ | ~~✅ DONE~~ |
| ~~2 — High-Value~~ | ~~S3–S6~~ | ~~18–26h~~ | ~~✅ DONE~~ |
| ~~3 — Schema Infra~~ | ~~S7–S8~~ | ~~6–8h~~ | ~~✅ DONE~~ |
| ~~4 — Unlocked~~ | ~~S9–S10~~ | ~~10–14h~~ | ~~✅ DONE~~ |
| ~~5 — Advanced~~ | ~~S11–S15~~ | ~~47–73h~~ | ~~✅ DONE~~ |
| **Total remaining** | **None** | **0h** | **All steps complete** |

---

### C5. Decision: Detection + Smart Reinit (Option 2) — ACCEPTED

**Option 2 is the accepted approach.** Option 1 (DDL Blocking) is available as
an optional strict mode for conservative deployments, but the primary mechanism
is Detection + Smart Reinit.

**Rationale:**

1. **Pragmatic for real-world workflows.** Production databases evolve — columns
   get added, types get widened, indexes change. Blocking `ALTER TABLE` on any
   source table used by a stream table would force users to drop all dependent
   stream tables before any schema migration.

2. **Infrastructure already ~60% built.** `columns_used TEXT[]` exists in the
   catalog schema, `detect_schema_change_kind()` exists with tests, and
   `resolve_columns()` already resolves full column metadata. Completing the
   wiring is ~4–5 hours.

3. **Benefits the entire system.** Today, *any* `ALTER TABLE` on a source (even
   adding a comment or index) triggers full reinitialization of all dependent
   stream tables. With smart detection, benign changes skip reinit entirely —
   a performance win for all users regardless of schema-dependent features.

**Implementation order:**

1. **Detection + Smart Reinit first** (C-1 + C-2). Wire existing infrastructure,
   add column snapshots. ~7–9 hours total.

2. **DDL Blocking as opt-in strict mode** (C-3). Offer
   `pg_trickle.block_source_ddl = true` for production deployments where source
   schemas are stable. Default to `false`.

3. **Schema-dependent features** (C-4 + C-5). NATURAL JOIN with reinit-aware
   column resolution, keyless tables with PK-aware row identity.

This gives users a spectrum of safety:

| User Profile | Configuration | Behavior |
|-------------|---------------|----------|
| Development / experimentation | Default (`block_source_ddl = false`) | Schema changes trigger smart reinit; NATURAL JOIN warns on semantic drift |
| Production / stable schemas | `block_source_ddl = true` | Schema-altering DDL on tracked sources is blocked; guaranteed correctness |

---

### C6. Items That Remain Rejected Regardless of Policy

These items are **not** schema-dependent — they are rejected for fundamental
design reasons that no schema policy can address:

| Item | Reason | Policy Relevance |
|------|--------|-----------------|
| LIMIT / OFFSET | Stream tables are full result sets by design | None |
| FOR UPDATE / FOR SHARE | No row-level locking on materialized stream tables | None |
| TABLESAMPLE | Stream tables materialize complete result sets | None |
| ROWS FROM() multi-function | Extremely niche; single SRF covers all practical use | None |
| Hypothetical-set aggregates | Almost always used as window functions, not aggregates | None |
| XMLAGG | Extremely niche | None |
| Window functions in expressions | Architectural constraint; separate column is cleaner | None |
| LATERAL with RIGHT/FULL JOIN | PostgreSQL itself restricts this | None |

---

### C7. Success Criteria

#### Completed (C-1)

- [x] `columns_used` populated for all source dependencies at creation time
      (DIFFERENTIAL mode STs)
- [x] `detect_schema_change_kind()` wired into `handle_alter_table()` — benign
      DDL no longer triggers reinit
- [x] `get_for_st()` / `get_all()` read `columns_used` from DB

#### Remaining (S1–S15)

- [ ] Volatile functions rejected in DIFFERENTIAL mode with clear error (S1)
- [ ] Stable functions produce a warning in DIFFERENTIAL mode (S1)
- [ ] TRUNCATE on source tables triggers reinitialization (S2)
- [ ] `ALL (subquery)` supported via AntiJoin rewrite (S3)
- [ ] `DISTINCT ON` auto-rewritten to ROW_NUMBER() window function (S4)
- [ ] 11 regression aggregates supported in DIFFERENTIAL mode (S5)
- [ ] Mixed UNION / UNION ALL works correctly (S6)
- [x] Column snapshot stored per source dependency with schema fingerprint (S7)
- [x] `pg_trickle.block_source_ddl` GUC available, default false (S8)
- [x] NATURAL JOIN supported with catalog-resolved rewrite + column snapshot (S9)
- [x] Keyless tables supported with all-column content hash for `__pgt_row_id` (S10)
- [x] GROUPING SETS / CUBE / ROLLUP via UNION ALL decomposition (S11)
- [x] Scalar subquery in WHERE via CROSS JOIN rewrite (S12)
- [x] SubLinks inside OR via OR-to-UNION rewrite (S13)
- [x] Multiple PARTITION BY via multi-pass recomputation (S14)
- [x] Recursive CTE in DIFFERENTIAL mode via incremental fixpoint (S15)
- [ ] 36+ AggFunc variants (up from 25)
- [x] 896 unit tests (up from 890)
- [ ] E2E tests for: benign DDL skips reinit, column DDL triggers reinit,
      blocked DDL errors, NATURAL JOIN creation + reinit + column change
- [ ] Documentation updated across SQL_REFERENCE, DVM_OPERATORS, CONFIGURATION, README
