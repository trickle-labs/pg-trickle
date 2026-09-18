# SQL Support Gap Analysis

**Status:** Reference document (periodically updated)  
**Date:** 2025-02-21
**Branch:** `main`
**Scope:** All SQL constructs that produce incorrect results, broken delta SQL, or rejection errors in the pg_trickle parser and differential view maintenance engine.
**Last Updated:** 2026-02-24 — Hybrid CDC (trigger → WAL transition), user-defined trigger support on stream tables, `needs_pgt_count()` fix for HAVING + aggregates. 25 aggregate functions in DIFFERENTIAL mode. 872 unit tests, 22 E2E test suites passing.

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Severity Classification](#severity-classification)
3. [Gap 1: Silent Broken SQL — Expression Deparsing](#gap-1-silent-broken-sql--expression-deparsing)
4. [Gap 2: Join Types](#gap-2-join-types)
5. [Gap 3: Aggregate Functions](#gap-3-aggregate-functions)
6. [Gap 4: Subquery Expressions](#gap-4-subquery-expressions)
7. [Gap 5: LATERAL Subqueries](#gap-5-lateral-subqueries)
8. [Gap 6: Clause Features](#gap-6-clause-features)
9. [Gap 7: Window Function Limitations](#gap-7-window-function-limitations)
10. [Gap 8: Data Type Operations](#gap-8-data-type-operations)
11. [Gap 9: Documentation Inaccuracies](#gap-9-documentation-inaccuracies)
12. [Prioritized Fix Recommendations](#prioritized-fix-recommendations)
13. [Implementation Roadmap for Remaining Work](#implementation-roadmap-for-remaining-work)
14. [What Works Well](#what-works-well)

---

## Executive Summary

pg_trickle supports a substantial core of SQL for both FULL and DIFFERENTIAL refresh modes: table scans, projections, WHERE/HAVING filtering, INNER/LEFT/RIGHT/FULL OUTER JOIN (including nested 3+ table joins), GROUP BY with COUNT/SUM/AVG/MIN/MAX/BOOL_AND/BOOL_OR/STRING_AGG/ARRAY_AGG/JSON_AGG/JSONB_AGG/BIT_AND/BIT_OR/BIT_XOR/JSON_OBJECT_AGG/JSONB_OBJECT_AGG/STDDEV_POP/STDDEV_SAMP/VAR_POP/VAR_SAMP/MODE/PERCENTILE_CONT/PERCENTILE_DISC (including FILTER and WITHIN GROUP clauses), DISTINCT, set operations (UNION ALL/UNION/INTERSECT/EXCEPT), non-recursive and recursive CTEs, window functions (with frame clauses and named windows), LATERAL set-returning functions, and LATERAL subqueries.

**Priorities 1-4, Phases A, B, and C are fully complete**, resolving all silent data corruption from expression deparsing, semantic error detection gaps, and expanding support for RIGHT JOIN, FULL OUTER JOIN, nested joins (3+ tables), MIN/MAX in DIFFERENTIAL mode, 25 aggregate functions in DIFFERENTIAL mode (including FILTER and WITHIN GROUP clauses), DISTINCT, set operations (UNION ALL/UNION/INTERSECT/EXCEPT), non-recursive and recursive CTEs, window functions (with frame clauses and named windows), LATERAL set-returning functions, LATERAL subqueries, EXISTS/NOT EXISTS subqueries (SemiJoin operator), IN/NOT IN subqueries (SemiJoin/AntiJoin operators), scalar subqueries in SELECT (ScalarSubquery operator), proper rejection of GROUPING SETS/CUBE/ROLLUP, DISTINCT ON, NATURAL JOIN, and detection of window functions nested inside expressions.

Remaining gaps: additional aggregate functions (regression: CORR/COVAR_*/REGR_*), structural enhancements (GROUPING SETS full impl, DISTINCT ON full impl, Mixed UNION/UNION ALL), and NATURAL JOIN support (currently rejected with clear error).

**Total identified gaps: 52 distinct items across 9 categories. 49+ items resolved. 872 unit tests, 22 E2E test suites passing.**

---

## Severity Classification

| Severity | Meaning | Count |
|----------|---------|-------|
| **P0 — Silent Data Corruption** | Produces invalid or semantically wrong delta SQL without any error message. User believes results are correct when they are not. | 21 |
| **P1 — Incorrect Semantics** | Parses successfully but applies wrong logic. | 5 |
| **P2 — Missing with Error** | Properly rejected with an error message. User knows it doesn't work. | 13 |
| **P3 — Documentation** | Feature listed as supported but actually isn't, or vice versa. | 3 |
| **P4 — Low Priority** | Rare constructs or edge cases. | 9 |

---

## Gap 1: Silent Broken SQL — Expression Deparsing

**Severity: P0 — Silent Data Corruption**
**Impact: Affects any DIFFERENTIAL-mode stream table using these expressions**

### Root Cause

`node_to_expr()` ([parser.rs line 2226](../../src/dvm/parser.rs#L2226)) handles 7 node types with structured `Expr` variants:

| Node Type | Handled As |
|-----------|-----------|
| `T_ColumnRef` | `Expr::ColumnRef` |
| `T_A_Const` | `Expr::Raw` (via `deparse_node`, handles String/Integer/Float) |
| `T_A_Expr` with `AEXPR_OP` | `Expr::BinaryOp` |
| `T_BoolExpr` | `Expr::BinaryOp` (AND/OR) or `Expr::FuncCall` (NOT) |
| `T_FuncCall` | `Expr::FuncCall` |
| `T_TypeCast` | `Expr::Raw` (`CAST(x AS type)`) |
| `T_NullTest` | `Expr::Raw` (`x IS [NOT] NULL`) |

**All other node types** fall through to `Expr::Raw(deparse_node(node))`. The `deparse_node()` function ([parser.rs line 2431](../../src/dvm/parser.rs#L2431)) only handles `T_A_Const` and `T_ColumnRef` — everything else emits:

```
/* node T_CaseExpr */
```

This placeholder text is embedded directly into generated delta SQL, making it syntactically invalid. PostgreSQL will reject the delta query at execution time, but the error message will be confusing (a SQL syntax error citing an unexpected comment), with no indication that the expression type is unsupported.

### Affected Expression Types

#### 1.1 — CASE Expressions (P0) ✅ FIXED

```sql
-- ✅ Now properly deparsed to valid SQL
SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END AS is_active
FROM accounts
```

**Parse node:** `T_CaseExpr` contains `args` (list of `CaseWhen`), optional `arg` (simple CASE argument), and `defresult` (ELSE).

**Fix:** Add `T_CaseExpr` handling to `node_to_expr()` → `Expr::Raw(deparse_case_expr())`. The deparse function would walk `CaseWhen` nodes and reconstruct `CASE WHEN <cond> THEN <result> ... ELSE <default> END`.

#### 1.2 — COALESCE (P0) ✅ FIXED

```sql
-- ✅ Now properly deparsed to COALESCE(arg1, arg2, ...)
SELECT COALESCE(middle_name, '') AS middle
FROM users
```

**Parse node:** `T_CoalesceExpr` contains `args` (list of expressions).

**Fix:** Add `T_CoalesceExpr` → `Expr::Raw("COALESCE(arg1, arg2, ...)")`.

#### 1.3 — NULLIF (P0) ✅ FIXED

```sql
-- ✅ Now properly deparsed to NULLIF(a, b)
SELECT NULLIF(divisor, 0) AS safe_divisor
FROM calculations
```

**Parse node:** `T_NullIfExpr` contains `args` (exactly 2 expressions).

**Fix:** Add `T_NullIfExpr` → `Expr::Raw("NULLIF(a, b)")`.

#### 1.4 — GREATEST / LEAST (P0) ✅ FIXED

```sql
-- ✅ Now properly deparsed to GREATEST(a, b) / LEAST(a, b)
SELECT GREATEST(price_a, price_b) AS max_price
FROM products
```

**Parse node:** `T_MinMaxExpr` contains `args` and `op` (IS_GREATEST / IS_LEAST).

**Fix:** Add `T_MinMaxExpr` → `Expr::Raw("GREATEST(a, b)")` or `Expr::Raw("LEAST(a, b)")`.

#### 1.5 — BETWEEN (P0) ✅ FIXED

```sql
-- ✅ Now handles AEXPR_BETWEEN, AEXPR_NOT_BETWEEN, AEXPR_BETWEEN_SYM, AEXPR_NOT_BETWEEN_SYM
SELECT * FROM orders WHERE amount BETWEEN 100 AND 1000
```

**Parse node:** `T_A_Expr` with `kind = AEXPR_BETWEEN`. The `A_Expr` has `lexpr` (tested value) and `rexpr` (a List of two bounds).

**Also affects:** `NOT BETWEEN` (`AEXPR_NOT_BETWEEN`), `BETWEEN SYMMETRIC` (`AEXPR_BETWEEN_SYM`), `NOT BETWEEN SYMMETRIC`.

**Fix:** Handle `AEXPR_BETWEEN` → `Expr::Raw("x BETWEEN a AND b")` by extracting the `lexpr` and the two list items from `rexpr`.

#### 1.6 — IN (value list) (P0) ✅ FIXED

```sql
-- ✅ Now handles AEXPR_IN with IN and NOT IN detection
SELECT * FROM orders WHERE status IN ('pending', 'processing', 'shipped')
```

**Parse node:** `T_A_Expr` with `kind = AEXPR_IN`. The `rexpr` is a `List` of constant values.

**Fix:** Handle `AEXPR_IN` → `Expr::Raw("x IN (v1, v2, v3)")`.

#### 1.7 — SIMILAR TO (P0) ✅ FIXED

```sql
-- ✅ Now handles AEXPR_SIMILAR → 'x SIMILAR TO pattern'
SELECT * FROM products WHERE name SIMILAR TO '%widget%'
```

**Parse node:** `T_A_Expr` with `kind = AEXPR_SIMILAR`.

**Note:** `LIKE` and `ILIKE` are NOT affected — PostgreSQL's raw parser rewrites them to `AEXPR_OP` with operators `~~` / `~~*`, which correctly go through the `BinaryOp` path.

**Fix:** Handle `AEXPR_SIMILAR` → `Expr::Raw("x SIMILAR TO 'pattern'")`.

#### 1.8 — IS DISTINCT FROM (P0) ✅ FIXED

```sql
-- ✅ Now handles AEXPR_DISTINCT and AEXPR_NOT_DISTINCT
SELECT * FROM t WHERE a IS DISTINCT FROM b
```

**Parse node:** `T_A_Expr` with `kind = AEXPR_DISTINCT`.

**Also affects:** `IS NOT DISTINCT FROM` (`AEXPR_NOT_DISTINCT`).

**Fix:** Handle `AEXPR_DISTINCT` → `Expr::Raw("a IS DISTINCT FROM b")`.

#### 1.9 — CURRENT_TIMESTAMP and friends (P0) ✅ FIXED

```sql
-- ✅ All 15 SVFOP_* variants mapped to SQL keywords
SELECT CURRENT_TIMESTAMP AS ts, CURRENT_USER AS who FROM t
```

**Parse node:** `T_SQLValueFunction` with `op` field indicating which function (`SVFOP_CURRENT_TIMESTAMP`, `SVFOP_CURRENT_USER`, etc.).

**Affected functions:** `CURRENT_DATE`, `CURRENT_TIME`, `CURRENT_TIMESTAMP`, `LOCALTIME`, `LOCALTIMESTAMP`, `CURRENT_USER`, `CURRENT_ROLE`, `SESSION_USER`, `USER`, `CURRENT_CATALOG`, `CURRENT_SCHEMA`.

**Fix:** Map `SVFOp` enum values to their SQL keyword strings.

#### 1.10 — Boolean/type test expressions (P0) ✅ FIXED

```sql
-- ✅ BooleanTest: IS [NOT] TRUE/FALSE/UNKNOWN
SELECT * FROM t WHERE flag IS TRUE

-- ❌ Produces: /* node T_A_Expr */
SELECT * FROM t WHERE a IS NOT DISTINCT FROM b
```

**Parse node:** `T_BooleanTest` with `booltesttype` (IS_TRUE, IS_NOT_TRUE, IS_FALSE, IS_NOT_FALSE, IS_UNKNOWN, IS_NOT_UNKNOWN).

**Fix:** Add `T_BooleanTest` → `Expr::Raw("x IS [NOT] TRUE/FALSE/UNKNOWN")`.

#### 1.11 — Unary minus / prefix operators (P0) ✅ FIXED

```sql
-- Prefix operator: -1, -amount
-- T_A_Expr with AEXPR_OP but lexpr is NULL (unary prefix)
```

**Status:** `AEXPR_OP` is handled, but if `lexpr` is NULL (unary prefix), `node_to_expr(aexpr.lexpr)` will return an error. The error may or may not propagate correctly.

**Fix:** Check for NULL `lexpr` in `AEXPR_OP` handling and emit `Expr::Raw("-x")` for prefix operators.

### Impact Summary

These 11 expression categories cover the vast majority of SQL expressions beyond simple column references, arithmetic, and function calls. A query like:

```sql
SELECT
    customer_id,
    CASE WHEN type = 'premium' THEN amount * 1.1 ELSE amount END AS adj_amount,
    COALESCE(discount, 0) AS discount
FROM orders
WHERE status IN ('active', 'pending')
  AND amount BETWEEN 100 AND 5000
```

...contains **4 distinct broken expressions** (`CASE`, `COALESCE`, `IN`, `BETWEEN`). The stream table would be created successfully but DIFFERENTIAL refresh would fail at runtime with a confusing SQL syntax error.

---

## Gap 2: Join Types

### 2.1 — RIGHT OUTER JOIN (P2) ✅ FIXED

**Severity: P2 — Rejected with error**
**Status: FIXED — RIGHT JOIN is now automatically converted to LEFT JOIN with swapped operands.**

```sql
-- ❌ Error: "Join type JOIN_RIGHT not supported for differential mode"
SELECT * FROM a RIGHT JOIN b ON a.id = b.id
```

**Location:** [parser.rs line 1787](../../src/dvm/parser.rs#L1787)

**Fix complexity: Low.** A RIGHT JOIN is semantically equivalent to a LEFT JOIN with operands swapped. The parser can rewrite `RIGHT JOIN(A, B)` → `LeftJoin { left: B, right: A }`.

### 2.2 — FULL OUTER JOIN (P2) ✅ IMPLEMENTED

**Severity: P2 — Previously rejected with error**
**Status: IMPLEMENTED** — `OpTree::FullJoin` variant added with dedicated diff operator in `full_join.rs`. Uses an 8-part UNION ALL delta extending the LEFT JOIN 5-part formula with symmetric right-side anti-join and NULL-padding transitions. Pre-computes delta flags (`__has_ins_*`, `__has_del_*`) for efficiency.

```sql
-- ✅ Now supported in DIFFERENTIAL mode
SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id
```

### 2.3 — NATURAL JOIN (P1 — Incorrect Semantics) ✅ FIXED (REJECTED)

**Severity: P1 — Previously silent incorrect semantics**
**Status: FIXED (REJECTED) — NATURAL JOIN is detected and rejected with a clear error message directing users to use explicit JOIN ... ON conditions instead. Full implementation was prototyped but reverted due to complexity; rejection prevents silent wrong results.**

```sql
-- ✅ Now rejected with clear error: "NATURAL JOIN is not supported..."
SELECT * FROM orders NATURAL JOIN customers
```

The parser reads `T_JoinExpr` and sees `jointype = JOIN_INNER`, but does not inspect the `isNatural` flag. When `isNatural` is true, the `quals` field is NULL (PostgreSQL doesn't resolve natural join columns in the raw parse tree — that happens during analysis). The parser treats NULL quals as `Expr::Literal("TRUE")`, producing a cross join.

**Fix complexity: Medium.** When `isNatural` is true, resolve the common column names between left and right children and synthesize an equi-join condition on all shared names.

### 2.4 — Nested Joins in DIFFERENTIAL mode (P2) ✅ IMPLEMENTED

**Severity: P2 — Previously rejected with error**
**Status: IMPLEMENTED** — The nested join restriction has been removed. A new shared module `join_common.rs` provides `build_snapshot_sql()` and `rewrite_join_condition()` helpers that enable recursive delta computation over join trees with 3+ tables. All join operators (inner, left, full) delegate to these helpers for both simple (Scan) and nested (join-of-join) children.

```sql
-- ✅ Now supported in DIFFERENTIAL mode
SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id
```

For nested children, `build_snapshot_sql()` generates a parenthesized subquery with disambiguated column names (e.g., `a__id`, `b__name`), and `rewrite_join_condition()` rewrites ON-clause column references to use the correct alias prefixes.

### 2.5 — Explicit CROSS JOIN syntax (P4) ✅ VERIFIED

**Severity: P4 — Works correctly**
**Status: VERIFIED** — Explicit `CROSS JOIN` works in both FULL and DIFFERENTIAL modes. Dedicated test coverage added: simple CROSS JOIN, nested 3-table CROSS JOIN, CROSS JOIN with WHERE clause, and DIFFERENTIAL mode with insert delta verification. 3 unit tests + 4 E2E tests.

```sql
-- ✅ Works correctly in both FULL and DIFFERENTIAL modes
SELECT * FROM a CROSS JOIN b
```

PostgreSQL parses `CROSS JOIN` as a `JoinExpr` with `jointype = JOIN_INNER` and `quals = NULL`. The parser handles this as `InnerJoin` with condition `TRUE`, which is semantically correct.

---

## Gap 3: Aggregate Functions

### 3.1 — Unrecognized Aggregates (P1 — Incorrect Semantics) ✅ FIXED

**Severity: P1 — Silent semantic error**
### 3.1 — Additional Aggregate Functions (P1) ✅ IMPLEMENTED (Phase 1)

**Severity: P1 — Previously rejected with error**
**Status: IMPLEMENTED (Phase 1+2+3+4)** — ~45 known aggregate functions are recognized. The following 25 aggregates work in DIFFERENTIAL mode (including 3 ordered-set aggregates with WITHIN GROUP):

| Function | DVM Strategy | Status |
|----------|-------------|--------|
| `COUNT(*)` | Algebraic | ✅ Implemented |
| `COUNT(expr)` | Algebraic | ✅ Implemented |
| `COUNT(DISTINCT expr)` | Algebraic | ✅ Implemented |
| `SUM(expr)` | Algebraic | ✅ Implemented |
| `AVG(expr)` | Algebraic (SUM/COUNT) | ✅ Implemented |
| `MIN(expr)` | Semi-algebraic | ✅ Implemented |
| `MAX(expr)` | Semi-algebraic | ✅ Implemented |
| `BOOL_AND(expr)` / `EVERY(expr)` | Group-rescan | ✅ Implemented |
| `BOOL_OR(expr)` | Group-rescan | ✅ Implemented |
| `STRING_AGG(expr, sep)` | Group-rescan | ✅ Implemented |
| `ARRAY_AGG(expr)` | Group-rescan | ✅ Implemented |
| `JSON_AGG(expr)` | Group-rescan | ✅ Implemented |
| `JSONB_AGG(expr)` | Group-rescan | ✅ Implemented |
| `BIT_AND(expr)` | Group-rescan | ✅ Implemented |
| `BIT_OR(expr)` | Group-rescan | ✅ Implemented |
| `BIT_XOR(expr)` | Group-rescan | ✅ Implemented |
| `JSON_OBJECT_AGG(key, value)` | Group-rescan | ✅ Implemented |
| `JSONB_OBJECT_AGG(key, value)` | Group-rescan | ✅ Implemented |
| `STDDEV_POP(expr)` / `STDDEV(expr)` | Group-rescan | ✅ Implemented |
| `STDDEV_SAMP(expr)` | Group-rescan | ✅ Implemented |
| `VAR_POP(expr)` | Group-rescan | ✅ Implemented |
| `VAR_SAMP(expr)` / `VARIANCE(expr)` | Group-rescan | ✅ Implemented |
| `MODE() WITHIN GROUP (ORDER BY expr)` | Group-rescan | ✅ Implemented |
| `PERCENTILE_CONT(frac) WITHIN GROUP (ORDER BY expr)` | Group-rescan | ✅ Implemented |
| `PERCENTILE_DISC(frac) WITHIN GROUP (ORDER BY expr)` | Group-rescan | ✅ Implemented |

Group-rescan aggregates use a NULL sentinel approach: when any row in a group is inserted or deleted, the merge expression returns NULL, triggering the change-detection guard to flag the group for re-aggregation from source data.

Recognized-but-unsupported aggregates (ordered-set, regression, etc.) are still rejected with a clear error suggesting FULL mode.

**Remaining unsupported aggregate functions:**

| Category | Functions |
|----------|-----------|
| **String** | `CONCAT_AGG` |
| **Regression** | `CORR`, `COVAR_POP`, `COVAR_SAMP`, `REGR_*` (8 functions) |
| **XML** | `XMLAGG` |
| **Hypothetical** | `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST` (as aggregates) |

**Fix options:**

- **Option A (Recommended):** Detect unrecognized function names in an aggregate context (GROUP BY present + FuncCall in SELECT) and **reject with an error** rather than silently misclassifying. This is a small change with high safety value.

- **Option B:** Add algebraic diff support for additional aggregates. `BOOL_AND`/`BOOL_OR` could track TRUE/FALSE counts similar to COUNT. `ARRAY_AGG`/`STRING_AGG` would require group-rescan (like MIN/MAX).

### 3.2 — MIN/MAX Rejected in DIFFERENTIAL (P2) ✅ IMPLEMENTED

**Severity: P2 — Previously rejected with error**
**Status: IMPLEMENTED** — MIN/MAX now uses a semi-algebraic approach with `CASE`/`LEAST`/`GREATEST` merge expressions. When the deleted value equals the current extremum, returns a NULL sentinel triggering re-aggregation via MERGE DELETE+INSERT. Non-extremum deletions use `LEAST`/`GREATEST` directly, avoiding full group rescan in the common case.

```sql
-- ✅ Now supported in DIFFERENTIAL mode
SELECT department, MIN(salary) FROM employees GROUP BY department
```

### 3.3 — FILTER (WHERE ...) on Aggregates (P0) ✅ IMPLEMENTED

**Severity: P0 — Previously rejected with error**
**Status: IMPLEMENTED** — The `FILTER (WHERE …)` clause is now fully supported on all aggregate functions in DIFFERENTIAL mode. The filter predicate is applied within the delta computation — only rows matching the filter contribute to the aggregate delta.

```sql
-- ✅ Now supported in DIFFERENTIAL mode
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE status = 'active') AS active_count
FROM orders
GROUP BY region
```

Implementation details:
- The `agg_filter` field from `FuncCall` is parsed into an `Expr` tree stored on `AggExpr.filter`
- Filter expressions are resolved against child CTE columns via `resolve_expr_for_child()`
- The filter predicate is injected as `AND <filter>` in all delta CASE WHEN branches
- Filtered aggregates are excluded from the P5 direct-bypass optimization

### 3.4 — WITHIN GROUP (ORDER BY ...) / Ordered-Set Aggregates (P2) ✅ FIXED (REJECTED)

**Severity: P2 — Rejected with error** (downgraded from P0: aggregate recognition now covers ~45 functions)
**Status: FIXED (REJECTED)** — `PERCENTILE_CONT`, `PERCENTILE_DISC`, `MODE`, and other ordered-set aggregates are now recognized as aggregate functions but rejected with a clear error: *"Aggregate function PERCENTILE_CONT() is not supported in DIFFERENTIAL mode. Use FULL refresh mode instead."*

```sql
-- ✅ Now rejected with clear error (was previously silently broken)
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) FROM employees
```

The original P0 classification assumed these would silently produce wrong results. In practice, the ~45-function aggregate recognition whitelist causes unrecognized aggregates to be detected and rejected with a clear error, making this P2 (rejected with error) rather than P0 (silent corruption).

---

## Gap 4: Subquery Expressions

**Severity: P0 — Previously silent broken SQL**
**Status: ✅ IMPLEMENTED** — All major subquery expression types now have dedicated DVM operators with incremental delta computation.

### 4.1 — Scalar Subquery in SELECT ✅ IMPLEMENTED

```sql
-- ✅ Fully supported in DIFFERENTIAL mode via ScalarSubquery operator
SELECT
    name,
    (SELECT MAX(amount) FROM orders o WHERE o.customer_id = c.id) AS max_order
FROM customers c
```

**Parse node:** `T_SubLink` with `subLinkType = EXPR_SUBLINK`.
**Implementation:** New `OpTree::ScalarSubquery` variant with dedicated `diff_scalar_subquery()` operator. Two-part delta: (1) child delta with current scalar value appended, (2) when scalar value changes, emit DELETE for all old rows + INSERT with new value. Old scalar reconstructed via `EXCEPT ALL / UNION ALL` reversal.

### 4.2 — EXISTS Subquery in WHERE ✅ IMPLEMENTED

```sql
-- ✅ Fully supported in DIFFERENTIAL mode via SemiJoin/AntiJoin operators
SELECT * FROM customers c
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)

-- NOT EXISTS also supported via AntiJoin
SELECT * FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
```

**Parse node:** `T_SubLink` with `subLinkType = EXISTS_SUBLINK`.
**Implementation:** `EXISTS` → `OpTree::SemiJoin`, `NOT EXISTS` → `OpTree::AntiJoin`. Two-part delta: (1) filter changed outer rows against current inner snapshot, (2) detect existence changes in inner snapshot for existing outer rows. Old inner state reconstructed via `EXCEPT ALL / UNION ALL` reversal.

### 4.3 — IN (subquery) in WHERE ✅ IMPLEMENTED

```sql
-- ✅ Fully supported in DIFFERENTIAL mode
SELECT * FROM products p
WHERE p.category_id IN (SELECT id FROM active_categories)

-- NOT IN also supported
SELECT * FROM products p
WHERE p.category_id NOT IN (SELECT id FROM active_categories)
```

**Parse node:** `T_SubLink` with `subLinkType = ANY_SUBLINK`.
**Implementation:** `IN (subquery)` rewritten to `SemiJoin` with equality condition. `NOT IN (subquery)` rewritten to `AntiJoin`. Reuses the same delta operators as EXISTS/NOT EXISTS.

### 4.4 — ANY / ALL with Subquery ✅ PARTIALLY IMPLEMENTED

```sql
-- ✅ ANY (subquery) / IN (subquery) fully supported via SemiJoin
SELECT * FROM orders WHERE amount > ANY (SELECT threshold FROM limits)

-- ❌ ALL (subquery) still rejected with clear error
SELECT * FROM orders WHERE amount > ALL (SELECT threshold FROM limits)
```

**Parse node:** `T_SubLink` with `subLinkType = ANY_SUBLINK` or `ALL_SUBLINK`.
**Status:** `ANY_SUBLINK` is fully implemented (SemiJoin). `ALL_SUBLINK` remains rejected with a clear error suggesting rewrite to `NOT EXISTS` with negated condition.

### Implementation Summary

Three new OpTree variants and diff operators were added in PLAN_SQL_GAPS_3:

| Operator | Module | Delta Strategy |
|----------|--------|---------------|
| `SemiJoin` | `src/dvm/operators/semi_join.rs` | Two-part: outer delta + existence change detection |
| `AntiJoin` | `src/dvm/operators/anti_join.rs` | Two-part: inverse semi-join semantics |
| `ScalarSubquery` | `src/dvm/operators/scalar_subquery.rs` | Two-part: child delta + value-change broadcast |

Parser changes: `extract_where_sublinks()` walks WHERE clause to extract SubLinks into `SublinkWrapper` structs. `parse_exists_sublink()` and `parse_any_sublink()` build the OpTree. `deparse_select_to_sql()` converts scalar subqueries to raw SQL for the operator.

---

## Gap 5: LATERAL Subqueries

**Severity: P1 — Incorrect semantics (LATERAL flag ignored)**

```sql
-- ❌ lateral flag ignored; subquery treated as non-correlated
SELECT o.id, latest.amount
FROM orders o,
     LATERAL (
         SELECT amount FROM line_items li WHERE li.order_id = o.id
         ORDER BY created_at DESC LIMIT 1
     ) AS latest
```

**Location:** [parser.rs line 1791](../../src/dvm/parser.rs#L1791) — the `T_RangeSubselect` handler reads `sub.lateral` but does not use it.

The subquery is wrapped in `OpTree::Subquery` and its internal column references to the outer table (`o.id`) will fail to resolve during delta computation.

**Status:** A comprehensive implementation plan exists in the historical LATERAL-join design.

---

## Gap 6: Clause Features

### 6.1 — LIMIT / OFFSET (P2) — ✅ PARTIALLY RESOLVED

**Severity: P2 → Resolved for TopK; LIMIT without ORDER BY and OFFSET remain rejected**

**TopK (ORDER BY + LIMIT)** is now supported:
```sql
-- ✅ Accepted as TopK — stores only the top 100 rows, refreshed via MERGE
SELECT * FROM orders ORDER BY created_at DESC LIMIT 100
```

**LIMIT without ORDER BY** and **OFFSET** remain rejected:
```sql
-- ❌ Error: "LIMIT is not supported in defining queries without ORDER BY (TopK requires ORDER BY)."
SELECT * FROM orders LIMIT 100

-- ❌ Error: "OFFSET is not supported in defining queries."
SELECT * FROM orders ORDER BY created_at OFFSET 10
```

**Location:** [parser.rs `detect_topk_pattern()` + `reject_limit_offset()`](../../src/dvm/parser.rs)

TopK stream tables bypass the DVM delta pipeline and use scoped recomputation — the full ORDER BY + LIMIT query is re-executed on each refresh and merged into the storage table.

### 6.2 — GROUPING SETS / CUBE / ROLLUP (P0) ✅ FIXED (REJECTED)

**Severity: P0 — Previously silent broken behavior**
**Status: FIXED (REJECTED)** — GROUPING SETS, CUBE, and ROLLUP are now detected and rejected with a clear error message suggesting separate stream tables or UNION ALL as alternatives. The error message includes the specific construct name (GROUPING SETS, CUBE, or ROLLUP).

> **Note (PLAN_SQL_GAPS_2 audit):** This item was previously claimed as fixed, but an audit found the fix was incomplete. The GROUP BY parsing loops used `if let Ok(expr)` which **silently skipped** `T_GroupingSet` nodes rather than rejecting them. The actual fix adds an explicit `T_GroupingSet` detection loop in `check_select_unsupported()` *before* GROUP BY parsing begins, and also hardens the GROUP BY parsing loops to use `?` error propagation instead of `if let Ok`.

```sql
-- ✅ Now rejected with clear error: "GROUPING SETS is not supported..."
SELECT department, region, SUM(amount)
FROM sales
GROUP BY GROUPING SETS ((department), (region), (department, region))

-- ✅ Also rejected: ROLLUP and CUBE
SELECT dept, region, SUM(amount) FROM sales GROUP BY ROLLUP(dept, region)
SELECT dept, region, SUM(amount) FROM sales GROUP BY CUBE(dept, region)
```

Full implementation would require a new `OpTree` variant that models multiple aggregation groupings — each grouping set is effectively a separate aggregate computation. Deferred to a future session.

### 6.3 — DISTINCT ON (P1) ✅ FIXED

**Severity: P1 — Incorrect semantics**
**Status: FIXED — DISTINCT ON is now detected when `distinctClause` has non-empty items and rejected with a clear error suggesting plain DISTINCT or ROW_NUMBER() window functions.**

```sql
-- ❌ Treated as plain DISTINCT, ignoring the ON expression
SELECT DISTINCT ON (customer_id) customer_id, order_id, created_at
FROM orders
ORDER BY customer_id, created_at DESC
```

**Location:** [parser.rs ~line 1631](../../src/dvm/parser.rs#L1631) — checks if `distinctClause` is non-null but does not distinguish between `DISTINCT` (empty list) and `DISTINCT ON (expr, ...)` (non-empty list with specific expressions).

With plain `DISTINCT`, any duplicate row is removed. With `DISTINCT ON`, only duplicates on the specified columns are removed (keeping the first per ORDER BY). The current implementation treats both the same, which produces **more deduplication than intended**.

**Fix:** Detect non-empty `distinctClause` list items and either properly implement `DISTINCT ON` semantics or reject with an error.

### 6.4 — Mixed UNION / UNION ALL (P2)

**Severity: P2 — Rejected with error**

```sql
-- ❌ Error: "Mixed UNION / UNION ALL not supported"
SELECT * FROM a UNION SELECT * FROM b UNION ALL SELECT * FROM c
```

**Location:** [parser.rs line 1424](../../src/dvm/parser.rs#L1424)

All set operation arms must use the same dedup/non-dedup mode. This is a genuine implementation limitation documented with a clear error.

### 6.5 — Named WINDOW Clause (P0) ✅ FIXED

**Severity: P0 — Silent broken behavior**
**Status: FIXED — Named window references are now resolved from `select.windowClause`. When `OVER w` is used, the parser looks up the window definition and merges partition, order, and frame specifications.**

```sql
-- ❌ Named window reference not resolved
SELECT
    ROW_NUMBER() OVER w AS rn,
    SUM(amount) OVER w AS running_total
FROM orders
WINDOW w AS (PARTITION BY customer_id ORDER BY created_at)
```

The parser reads window specs inline from `FuncCall.over` (a `WindowDef`). When a named window is used, the `WindowDef.name` field is set (referencing `w`) but `partitionClause` and `orderClause` are empty — they live in the `WINDOW` clause in `select.windowClause` which is never read.

**Result:** Window functions using named windows get empty PARTITION BY and ORDER BY, producing wrong results.

**Fix:** When `wdef.name` is non-null but partition/order clauses are empty, look up the named window from `select.windowClause` and merge specifications.

### 6.6 — FOR UPDATE / FOR SHARE (P4) ✅ FIXED (REJECTED)

**Severity: P4 — Low priority**
**Status: FIXED (REJECTED)** — `FOR UPDATE`, `FOR SHARE`, `FOR NO KEY UPDATE`, and `FOR KEY SHARE` are now detected via `SelectStmt.lockingClause` and rejected with a clear error: *"FOR UPDATE/FOR SHARE is not supported in defining queries. Stream tables do not support row-level locking. Remove the FOR UPDATE/FOR SHARE clause."* 4 E2E tests added covering all four locking modes.

### 6.7 — TABLESAMPLE (P4) ✅ FIXED (REJECTED)

**Severity: P4 — Low priority**
**Status: FIXED (REJECTED)** — `T_RangeTableSample` is now explicitly detected in `parse_from_item()` and rejected with a clear, actionable error message: *"TABLESAMPLE is not supported in defining queries. Stream tables materialize the complete result set; use a WHERE condition with random() if sampling is needed."* Previously fell through to a generic "Unsupported FROM item" error with no guidance.

```sql
-- ✅ Now rejected with clear, actionable error mentioning TABLESAMPLE
SELECT * FROM orders TABLESAMPLE BERNOULLI(10)
SELECT * FROM orders TABLESAMPLE SYSTEM(50)
```

2 E2E tests added (BERNOULLI and SYSTEM variants).

### 6.8 — ROWS FROM (multiple functions) (P2)

**Severity: P2 — Rejected with error**

```sql
-- ❌ Error: "ROWS FROM() with multiple functions is not supported."
SELECT * FROM ROWS FROM(generate_series(1,3), generate_series(1,5))
```

**Location:** [parser.rs line 1854](../../src/dvm/parser.rs#L1854)

Single-function ROWS FROM works fine; only the multi-function variant is rejected.

---

## Gap 7: Window Function Limitations

### 7.1 — Window Frame Specification (P0) ✅ FIXED

**Severity: P0 — Silent dropped behavior**
**Status: FIXED — Window frame clauses are now fully parsed and included in SQL output. Supports ROWS/RANGE/GROUPS modes, all bound types, BETWEEN syntax, and EXCLUDE clauses.**

```sql
-- ❌ Frame clause is silently ignored
SELECT
    customer_id,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY created_at
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW  -- ← IGNORED
    ) AS rolling_sum
FROM orders
```

The `WindowDef.frameOptions` field is never read in `parse_window_func_call()` ([parser.rs line 2553](../../src/dvm/parser.rs#L2553)). The `WindowExpr` struct has no frame representation. The delta computation uses the default frame (unbounded preceding to current row for range, or the entire partition for rows without ORDER BY).

**Affected frame types:**
- `ROWS BETWEEN N PRECEDING AND M FOLLOWING`
- `RANGE BETWEEN ...`
- `GROUPS BETWEEN ...`
- `EXCLUDE CURRENT ROW / GROUP / TIES / NO OTHERS`

**Impact:** Any window function with an explicit frame clause gets the wrong result — the actual frame used is always the default, not the user-specified one.

**Fix:** Add `frame_options: Option<WindowFrame>` to `WindowExpr` and read `wdef.frameOptions`, `wdef.startOffset`, `wdef.endOffset`. The diff operator must include the frame specification when reconstructing `OVER (...)`.

### 7.2 — Single PARTITION BY Requirement (P2)

**Severity: P2 — Rejected with error**

```sql
-- ❌ Error: "All window functions must share the same PARTITION BY clause"
SELECT
    ROW_NUMBER() OVER (PARTITION BY department) AS dept_rank,
    ROW_NUMBER() OVER (PARTITION BY region) AS region_rank
FROM employees
```

**Location:** [parser.rs line 1548](../../src/dvm/parser.rs#L1548)

The current partition-based recomputation strategy requires a single partition key. Using multiple different partitions would need multiple recomputation passes.

### 7.3 — Named Window References (P0) ✅ FIXED

See Gap 6.5 above — named WINDOW clause is not resolved.

### 7.4 — Window Functions in Subexpressions (P1) ✅ FIXED (REJECTED)

**Severity: P1 — Incorrect semantics** (upgraded from P4: this was the last silent-wrong-result path)
**Status: FIXED (REJECTED)** — Window functions nested inside expressions (CASE, COALESCE, arithmetic, CAST, etc.) are now detected recursively and rejected with a clear error message directing users to move the window function to a separate column and reference it in outer queries.

```sql
-- ✅ Now rejected with clear error: "Window functions nested inside expressions..."
SELECT CASE WHEN ROW_NUMBER() OVER (ORDER BY id) <= 3 THEN 'top' ELSE 'other' END
FROM t
```

The recursive `node_contains_window_func()` helper walks into 13 node types (CaseExpr, CoalesceExpr, NullIfExpr, MinMaxExpr, BoolExpr, A_Expr, TypeCast, NullTest, BooleanTest, ArrayExpr, RowExpr, SubLink, FuncCall args) to detect window functions at any nesting depth. Top-level window functions (`SELECT ROW_NUMBER() OVER (...) AS rn`) continue to work normally.

---

## Gap 8: Data Type Operations

### 8.1 — Array Constructor (P0) ✅ FIXED

```sql
-- ✅ Now properly deparsed to ARRAY[a, b, c]
SELECT ARRAY[1, 2, 3] AS nums FROM t
```

**Fix:** Add `T_ArrayExpr` → `Expr::Raw("ARRAY[a, b, c]")`.

### 8.2 — Array Subscript / Field Access (P0) ✅ FIXED

```sql
-- ✅ Now properly deparsed with subscript, field access, and star indirection
SELECT arr[1] AS first, (rec).field AS f FROM t
```

**Parse node:** `T_A_Indirection` contains a base expression and a list of `A_Indices` (subscript) or `String` (field name) nodes.

**Fix:** Add `T_A_Indirection` → `Expr::Raw(deparse_indirection())`.

### 8.3 — Row Constructor (P0) ✅ FIXED

```sql
-- ✅ Now properly deparsed to ROW(a, b, c)
SELECT ROW(1, 'a', NULL) AS r FROM t
```

**Fix:** Add `T_RowExpr` → `Expr::Raw("ROW(a, b, c)")`.

### 8.4 — Schema-Qualified Column References (P2) ✅ FIXED

```sql
-- ✅ Now handles 3-field ColumnRef by dropping schema prefix
SELECT public.orders.amount FROM public.orders
```

**Location:** [parser.rs line 2263](../../src/dvm/parser.rs#L2263) — `node_to_expr()` for `T_ColumnRef` only handles 1-field and 2-field references. 3-field (schema.table.column) is rejected.

**Fix complexity: Low.** Add a `3` arm that extracts schema + table + column and uses `table.column` (dropping schema, since it's resolved during analysis).

### 8.5 — JSONB Operators (P4 — Mostly works)

```sql
-- ✅ Works: Simple JSONB operators via AEXPR_OP → BinaryOp
SELECT data->>'name' FROM items

-- ⚠️ May not work: Complex expressions like jsonb_path_query
```

Standard JSONB operators (`->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`) are regular operators that go through the `AEXPR_OP` → `BinaryOp` path. However, any expression that involves `T_A_Indirection` (e.g., `(data->'nested')::text`) will fail.

### 8.6 — Regex Operators (P4 — Works)

`~`, `~*`, `!~`, `!~*` are regular operators parsed via `AEXPR_OP`. These work correctly.

---

## Gap 9: Documentation Inaccuracies

### 9.1 — README Claims RIGHT/FULL OUTER JOIN Support (P3) ✅ FIXED

The [README.md](../../README.md) SQL Support table now splits outer join types into separate rows:
- `LEFT OUTER JOIN` — ✅ Full
- `RIGHT OUTER JOIN` — ✅ Full (automatically converted to LEFT JOIN with swapped operands)
- `FULL OUTER JOIN` — ✅ Full

### 9.2 — No Mention of Expression Limitations (P3) ✅ FIXED

Added comprehensive "Expression Support" section to [docs/SQL_REFERENCE.md](../../docs/SQL_REFERENCE.md) documenting all supported and unsupported expression types, and added "Known Limitations" section to README.

### 9.3 — ORDER BY Documentation (P3)

The README says `ORDER BY` is "⚠️ Ignored" which is accurate but could be more explicit about the implications: the stream table's row order is undefined regardless of ORDER BY.

---

## Prioritized Fix Recommendations

### Priority 1: Fix Silent Data Corruption (P0) — ✅ COMPLETED

All 14 expression types now produce either valid SQL or clear error messages. The catch-all in `node_to_expr()` returns `Err(PgTrickleError::UnsupportedOperator(...))` instead of `Expr::Raw(deparse_node())`.

Implemented expression deparsing:
- `T_CaseExpr` → `CASE WHEN ... THEN ... ELSE ... END` (both searched and simple CASE)
- `T_CoalesceExpr` → `COALESCE(a, b, ...)`
- `T_NullIfExpr` → `NULLIF(a, b)`
- `T_MinMaxExpr` → `GREATEST(...)` / `LEAST(...)`
- `T_A_Expr(AEXPR_IN)` → `x IN (v1, v2)` / `x NOT IN (...)`
- `T_A_Expr(AEXPR_BETWEEN)` → `x BETWEEN a AND b` (+ NOT BETWEEN, SYMMETRIC variants)
- `T_A_Expr(AEXPR_DISTINCT)` → `a IS [NOT] DISTINCT FROM b`
- `T_A_Expr(AEXPR_SIMILAR)` → `x SIMILAR TO pattern`
- `T_A_Expr(AEXPR_OP_ANY)` → `x op ANY(array)`
- `T_A_Expr(AEXPR_OP_ALL)` → `x op ALL(array)`
- `T_SQLValueFunction` → `CURRENT_TIMESTAMP`, `CURRENT_USER`, etc. (15 variants)
- `T_BooleanTest` → `IS [NOT] TRUE/FALSE/UNKNOWN`
- `T_ArrayExpr` → `ARRAY[a, b, c]`
- `T_RowExpr` → `ROW(a, b, c)`
- `T_A_Indirection` → `arr[1]`, `(rec).field`, `(data).*`
- `T_SubLink` → Detected and rejected with clear error + rewrite suggestion
- Unary prefix operators (`-x`) handled in `AEXPR_OP`

### Priority 2: Fix Silent Semantic Errors (P1) — ✅ COMPLETED

| Fix | Status |
|-----|--------|
| Reject unrecognized aggregate functions with error instead of silent misclassification | ✅ ~45 known aggregates recognized |
| Detect NATURAL JOIN `isNatural` flag and reject | ✅ Rejected with clear error |
| Detect `DISTINCT ON` and reject | ✅ Rejected with clear error |
| Read `agg_filter` and reject FILTER clause with error | ✅ Fully implemented — FILTER supported in delta computation |

### Priority 3: Expand Support (P2) — ✅ COMPLETED

| Fix | Status |
|-----|--------|
| RIGHT JOIN → swap to LEFT JOIN | ✅ Implemented |
| Named WINDOW clause resolution | ✅ Resolved from `select.windowClause` |
| Window frame specification parsing + deparsing | ✅ Full support (ROWS/RANGE/GROUPS, BETWEEN, EXCLUDE) |
| 3-field ColumnRef support | ✅ Drops schema prefix |

### Priority 4: Documentation (P3) — ✅ COMPLETED

| Fix | Status |
|-----|--------|
| Split RIGHT/FULL OUTER JOIN in README | ✅ Updated |
| Add "Expression Support" section to SQL_REFERENCE | ✅ Comprehensive section added |
| Add "Known Limitations" section to README | ✅ Added |

### Priority 5: Larger Features (Separate plans)

| Feature | Plan File | Estimated Effort |
|---------|-----------|-----------------|
| ~~LATERAL subqueries~~ | ✅ Completed | — |
| ~~Nested join support~~ | ✅ Completed (Phase A1) | — |
| ~~FULL OUTER JOIN diff operator~~ | ✅ Completed (Phase A2) | — |
| ~~MIN/MAX in DIFFERENTIAL~~ | ✅ Completed (Phase A3) | — |
| ~~GROUPING SETS / CUBE / ROLLUP~~ | ✅ Rejected with clear error (Phase C2) | — |
| ~~NATURAL JOIN~~ | ✅ Rejected with clear error (Phase C4) | — |
| ~~Subquery expressions (EXISTS, IN subquery, scalar)~~ | ✅ Completed (PLAN_SQL_GAPS_3 Tasks 6-10) | — |
| Additional aggregate functions | ✅ Phase 1+2+3 done (BOOL_AND/OR, STRING/ARRAY/JSON/JSONB_AGG, BIT_AND/OR/XOR, JSON/JSONB_OBJECT_AGG, STDDEV/STDDEV_POP/STDDEV_SAMP, VARIANCE/VAR_POP/VAR_SAMP) | Phase 4: regression, ordered-set |

---

## Implementation Roadmap for Remaining Work

With Priorities 1–4 complete, the remaining gaps fall into three phases organized by **user impact**, **dependency chains**, and **implementation complexity**. These focus on the Priority 5 items above plus any remaining lower-severity items.

### Phase A: Join & Multi-Table Expansion ✅ COMPLETED

These unlock the most-requested multi-table query patterns and have the highest user impact among remaining gaps.

| Order | Gap | Construct | Severity | Status | Implementation |
|-------|-----|-----------|----------|--------|----------------|
| **A1** | 2.4 | Nested joins (3+ tables) in DIFFERENTIAL | P2 | ✅ Done | `join_common.rs` — shared helpers (`build_snapshot_sql`, `rewrite_join_condition`) enable recursive delta computation over join trees with 3+ tables. All join operators (inner, left, full) delegate to these helpers. |
| **A2** | 2.2 | `FULL OUTER JOIN` diff operator | P2 | ✅ Done | `full_join.rs` — 8-part UNION ALL delta extending LEFT JOIN's 5-part with symmetric right-side anti-join and NULL-padding transitions. Pre-computes delta flags for efficiency. |
| **A3** | 3.2 | `MIN` / `MAX` in DIFFERENTIAL (full support) | P2 | ✅ Done | Semi-algebraic approach in `aggregate.rs` — uses `CASE`/`LEAST`/`GREATEST` merge expressions. When the deleted value equals the current extremum, returns NULL sentinel triggering re-aggregation via MERGE DELETE+INSERT. Non-extremum deletions use `LEAST`/`GREATEST` directly. |

**Completed in 1 session.** Added 37 new unit tests (712 total, up from 675).

### Phase B: Subquery & Advanced Aggregation (Sessions 5–8)

These address the P0 items that are currently detected-and-rejected with clear errors (from Priority 1–2 work) but still need full implementation.

| Order | Gap | Construct | Severity | Rationale |
|-------|-----|-----------|----------|-----------|
| **B1** | 4.2 | `EXISTS (SELECT ...)` subquery expression | P0 | ✅ **IMPLEMENTED** — SemiJoin/AntiJoin operators with two-part delta (outer delta + existence change detection). |
| **B2** | 4.3 | `IN (SELECT ...)` subquery expression | P0 | ✅ **IMPLEMENTED** — Rewritten to SemiJoin (IN) / AntiJoin (NOT IN) with equality condition. |
| **B3** | 4.1 | Scalar subquery (`(SELECT max(x) FROM ...)`) | P0 | ✅ **IMPLEMENTED** — ScalarSubquery operator with value-change detection via IS DISTINCT FROM. |
| **B4** | 3.1+ | Additional aggregate functions (`STRING_AGG`, `ARRAY_AGG`, `BOOL_AND/OR`, `JSON_AGG`, `JSONB_AGG`) | P1 | ✅ **IMPLEMENTED** — Group-rescan strategy: affected groups re-aggregated from source. |
| **B5** | 3.3 | `FILTER (WHERE ...)` clause on aggregates | P0 | ✅ **IMPLEMENTED** — Filter predicate applied within delta computation. |

**Phase B is fully complete.** All 5 items implemented. B1-B3 added in PLAN_SQL_GAPS_3 (Tasks 6-10): 3 new OpTree variants (SemiJoin, AntiJoin, ScalarSubquery), 3 new diff operators, 26 new unit tests. Phase 2 aggregates (BIT_AND/OR/XOR, JSON/JSONB_OBJECT_AGG) and Phase 3 aggregates (STDDEV/STDDEV_POP/STDDEV_SAMP, VARIANCE/VAR_POP/VAR_SAMP) also complete. 809 unit tests passing.

### Phase C: Structural & Advanced Features (Sessions 9–12)

These handle less common but still important SQL constructs.

| Order | Gap | Construct | Severity | Rationale |
|-------|-----|-----------|----------|-----------|
| **C1** | 5.0 | LATERAL subqueries (non-SRF) | P1 | ✅ **IMPLEMENTED** — Already fully supported in both FULL and DIFFERENTIAL modes. |
| **C2** | 6.2 | `GROUPING SETS` / `CUBE` / `ROLLUP` | P0 | ✅ **FIXED** — Detected and rejected with clear error (was silently broken). |
| **C3** | 6.3 | `DISTINCT ON (expr)` | P1 | ✅ **FIXED** — Rejected with clear error suggesting DISTINCT or ROW_NUMBER(). |
| **C4** | 2.3 | `NATURAL JOIN` | P1 | ✅ **FIXED (REJECTED)** — Detected and rejected with clear error suggesting explicit JOIN ... ON. Full implementation was prototyped but reverted. |

**Estimated effort:** Complete.
**Unlocks:** Full SQL coverage for analytical queries.
**Phase C is complete.** C1 (LATERAL subqueries) was already implemented. C2 (GROUPING SETS) now properly rejected. C3 (DISTINCT ON) properly rejected. C4 (NATURAL JOIN) properly rejected. 742 unit tests passing.

### Dependency Graph

```
Phase A (Joins)  ─────────────┐
  ✅ COMPLETED                ├──→ Phase C (Structural/Advanced) ✅ COMPLETED
Phase B (Subqueries & Aggs) ──┘
```

**All phases are complete.** Phase A (Joins), Phase B (Subqueries & Aggregation), and Phase C (Structural) are fully resolved. 872 unit tests passing.

### Recommended Next Step

**All planned phases are complete.** Remaining work is limited to Tier 3 structural enhancements and additional aggregate functions; see the completed SQL-gap work for the historical recommendations.

---

## What Works Well

For completeness, here's what's fully supported and correctly implemented:

| Category | Feature | FULL | DIFFERENTIAL |
|----------|---------|------|--------------|
| **Core** | Table scan (`FROM table`) | ✅ | ✅ |
| **Core** | Projection (`SELECT expr AS alias`) | ✅ | ✅ |
| **Core** | Column references (1-field and 2-field) | ✅ | ✅ |
| **Core** | Schema-qualified tables | ✅ | ✅ |
| **Filtering** | `WHERE` clause | ✅ | ✅ |
| **Filtering** | `HAVING` clause | ✅ | ✅ |
| **Joins** | `INNER JOIN` (equi + non-equi) | ✅ | ✅ |
| **Joins** | `LEFT OUTER JOIN` | ✅ | ✅ |
| **Joins** | `RIGHT OUTER JOIN` (auto-converted to LEFT) | ✅ | ✅ |
| **Joins** | `FULL OUTER JOIN` | ✅ | ✅ 8-part delta |
| **Joins** | Nested joins (3+ tables) | ✅ | ✅ Recursive delta |
| **Joins** | Cross join via comma syntax | ✅ | ✅ |
| **Joins** | Explicit `CROSS JOIN` | ✅ | ✅ |
| **Aggregation** | `COUNT(*)`, `COUNT(expr)`, `COUNT(DISTINCT)` | ✅ | ✅ Algebraic |
| **Aggregation** | `SUM(expr)` | ✅ | ✅ Algebraic |
| **Aggregation** | `AVG(expr)` | ✅ | ✅ Algebraic (SUM/COUNT) |
| **Aggregation** | `MIN(expr)`, `MAX(expr)` | ✅ | ✅ Semi-algebraic |
| **Aggregation** | `BOOL_AND(expr)`, `BOOL_OR(expr)` | ✅ | ✅ Group-rescan |
| **Aggregation** | `STRING_AGG(expr, sep)` | ✅ | ✅ Group-rescan |
| **Aggregation** | `ARRAY_AGG(expr)` | ✅ | ✅ Group-rescan |
| **Aggregation** | `JSON_AGG(expr)`, `JSONB_AGG(expr)` | ✅ | ✅ Group-rescan |
| **Aggregation** | `FILTER (WHERE …)` on aggregates | ✅ | ✅ |
| **Dedup** | `SELECT DISTINCT` | ✅ | ✅ Reference-counted |
| **Set ops** | `UNION ALL` | ✅ | ✅ |
| **Set ops** | `UNION` (dedup) | ✅ | ✅ |
| **Set ops** | `INTERSECT` / `INTERSECT ALL` | ✅ | ✅ Dual-count |
| **Set ops** | `EXCEPT` / `EXCEPT ALL` | ✅ | ✅ Dual-count |
| **Subqueries** | `(SELECT ...) AS alias` in FROM | ✅ | ✅ |
| **CTEs** | Non-recursive `WITH` (single + multi-ref) | ✅ | ✅ Shared delta |
| **CTEs** | `WITH RECURSIVE` | ✅ | ✅ Semi-naive + DRed |
| **Window** | Window functions with PARTITION BY + ORDER BY | ✅ | ✅ Partition recompute |
| **Window** | Window frame clauses (ROWS/RANGE/GROUPS) | ✅ | ✅ Partition recompute |
| **Window** | Named WINDOW clause resolution | ✅ | ✅ Partition recompute |
| **LATERAL SRF** | `jsonb_array_elements`, `unnest`, `jsonb_each`, etc. | ✅ | ✅ Row-scoped recompute |
| **Expressions** | `AND` / `OR` / `NOT` | ✅ | ✅ |
| **Expressions** | Binary operators (`=`, `>`, `<`, `+`, `-`, `*`, `/`, `~~`) | ✅ | ✅ |
| **Expressions** | Function calls (`func(args...)`) | ✅ | ✅ |
| **Expressions** | `CAST(x AS type)` / `x::type` | ✅ | ✅ |
| **Expressions** | `IS NULL` / `IS NOT NULL` | ✅ | ✅ |
| **Expressions** | `LIKE` / `ILIKE` (via `~~` / `~~*` operators) | ✅ | ✅ |
| **Expressions** | String / Integer / Float / NULL literals | ✅ | ✅ |
| **Expressions** | `CASE WHEN ... THEN ... ELSE ... END` | ✅ | ✅ |
| **Expressions** | `COALESCE`, `NULLIF`, `GREATEST`, `LEAST` | ✅ | ✅ |
| **Expressions** | `IN (list)`, `NOT IN`, `BETWEEN`, `NOT BETWEEN` | ✅ | ✅ |
| **Expressions** | `IS [NOT] DISTINCT FROM` | ✅ | ✅ |
| **Expressions** | `SIMILAR TO`, `ANY(array)`, `ALL(array)` | ✅ | ✅ |
| **Expressions** | `IS [NOT] TRUE/FALSE/UNKNOWN` | ✅ | ✅ |
| **Expressions** | `CURRENT_DATE/TIME/TIMESTAMP`, `CURRENT_USER`, etc. | ✅ | ✅ |
| **Expressions** | `ARRAY[...]`, `ROW(...)` | ✅ | ✅ |
| **Expressions** | Array subscript, field access, star indirection | ✅ | ✅ |
| **Subqueries** | `WHERE EXISTS (SELECT ...)` / `NOT EXISTS` | ✅ | ✅ Semi-join / Anti-join |
| **Subqueries** | `WHERE col IN (SELECT ...)` / `NOT IN` | ✅ | ✅ Semi-join / Anti-join |
| **Subqueries** | Scalar subquery in SELECT `(SELECT max(x) FROM t)` | ✅ | ✅ Scalar subquery operator |
| **Ordering** | `ORDER BY` (silently discarded) | ✅ | ✅ (no-op) |
| **LATERAL SRF** | `WITH ORDINALITY` | ✅ | ✅ |
