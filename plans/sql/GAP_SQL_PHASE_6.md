# PLAN: SQL Gaps — Phase 6

**Status:** Reference  
**Date:** 2026-02-25  
**Branch:** `main`  
**Scope:** Comprehensive gap analysis — PostgreSQL 18 SQL coverage vs pg_trickle implementation.  
**Current state:** 896 unit tests, 22 E2E test suites (350 E2E tests), 61 integration tests, 36 AggFunc variants, 21 OpTree variants, 21 diff operators, 5 auto-rewrite passes, 17 GUC variables.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Methodology](#2-methodology)
3. [Ground Truth: Current Implementation State](#3-ground-truth-current-implementation-state)
4. [Gap Category 1: Expression Node Types](#4-gap-category-1-expression-node-types)
5. [Gap Category 2: Source Object Types](#5-gap-category-2-source-object-types)
6. [Gap Category 3: Aggregate Functions](#6-gap-category-3-aggregate-functions)
7. [Gap Category 4: Type System & Data Types](#7-gap-category-4-type-system--data-types)
8. [Gap Category 5: PostgreSQL 16/17/18 New Features](#8-gap-category-5-postgresql-161718-new-features)
9. [Gap Category 6: CDC & Change Tracking Edge Cases](#9-gap-category-6-cdc--change-tracking-edge-cases)
10. [Gap Category 7: Correctness Blind Spots](#10-gap-category-7-correctness-blind-spots)
11. [Gap Category 8: Operational & Concurrency Gaps](#11-gap-category-8-operational--concurrency-gaps)
12. [Gap Category 9: Documentation Drift](#12-gap-category-9-documentation-drift)
13. [Gap Category 10: Intentional Rejections (Not Gaps)](#13-gap-category-10-intentional-rejections-not-gaps)
14. [Prioritized Implementation Roadmap](#14-prioritized-implementation-roadmap)
15. [Historical Progress Summary](#15-historical-progress-summary)

---

## 1. Executive Summary

pg_trickle now covers the vast majority of PostgreSQL SELECT syntax. Phases 1–5
resolved 52+ original gaps, implemented 5 transparent auto-rewrite passes
(DISTINCT ON, GROUPING SETS/CUBE/ROLLUP, scalar subquery in WHERE, SubLinks in
OR, multi-PARTITION BY windows), added 21 diff operators, and expanded aggregate
support from 5 to 36 functions.

**Zero P0 (silent data corruption) or P1 (incorrect semantics) issues remain in
the core SQL parsing and delta computation.** All unsupported constructs are
rejected with clear, actionable error messages.

However, deep analysis reveals a new class of gaps — not in SQL syntax coverage
but in the **operational boundary conditions** where pg_trickle intersects with
PostgreSQL's broader feature set:

| Category | P0 | P1 | P2 | P3 | P4 | Total |
|----------|----|----|----|----|----|----|
| Expression node types | 0 | 0 | 3 | 0 | 2 | 5 |
| Source object types | 1 | 1 | 1 | 0 | 0 | 3 |
| Aggregate functions | 0 | 1 | 2 | 0 | 1 | 4 |
| Type system & data types | 0 | 1 | 0 | 0 | 3 | 4 |
| PostgreSQL 16/17/18 features | 0 | 0 | 3 | 0 | 2 | 5 |
| CDC & change tracking | 1 | 1 | 1 | 0 | 1 | 4 |
| Correctness blind spots | 1 | 1 | 1 | 0 | 0 | 3 |
| Operational & concurrency | 0 | 0 | 2 | 0 | 2 | 4 |
| Documentation drift | 0 | 0 | 0 | 4 | 2 | 6 |
| **Total new gaps** | **3** | **5** | **13** | **4** | **13** | **38** |

The 3 new P0 items are:
1. **Views as sources in DIFFERENTIAL mode** — CDC triggers cannot be placed on views; changes are silently missed
2. **Volatile expressions inside `Expr::Raw`** — volatility checker cannot see functions wrapped in CASE/COALESCE/BETWEEN/etc.
3. **Partitioned tables as CDC sources** — row-level triggers on parent may miss changes routed to child partitions

---

## 2. Methodology

This analysis was performed by:

1. **Code audit** of all 11,024 lines in `src/dvm/parser.rs` — every `node_to_expr()` match arm, rejection function, and auto-rewrite pass
2. **Operator inventory** of all 21 diff operator modules in `src/dvm/operators/`
3. **Cross-reference** of PostgreSQL 18 parse tree node types (`pg_sys::NodeTag`) against handled types
4. **CDC system review** of `src/cdc.rs` trigger setup, `src/hooks.rs` DDL event handling, `src/refresh.rs` refresh engine
5. **Documentation comparison** across `SQL_REFERENCE.md`, `DVM_OPERATORS.md`, `ARCHITECTURE.md`, `CONFIGURATION.md` vs actual code
6. **Test coverage analysis** of 896 unit tests, 350 E2E tests, and 61 integration tests
7. **Review of previous gap analyses** (SQL_GAPS_1 through SQL_GAPS_5, GAP_SQL_OVERVIEW.md)

---

## 3. Ground Truth: Current Implementation State

### 3.1 Supported Features (DIFFERENTIAL mode)

| Category | Features | Count |
|----------|----------|-------|
| **OpTree variants** | Scan, Project, Filter, InnerJoin, LeftJoin, FullJoin, Aggregate, Distinct, UnionAll, Intersect, Except, Subquery, CteScan, RecursiveCte, RecursiveSelfRef, Window, LateralFunction, LateralSubquery, SemiJoin, AntiJoin, ScalarSubquery | 21 |
| **Diff operators** | scan, filter, project, join, outer_join, full_join, aggregate, distinct, union_all, intersect, except, subquery, cte_scan, recursive_cte, window, lateral_function, lateral_subquery, semi_join, anti_join, scalar_subquery, join_common | 21 |
| **Aggregate functions** | COUNT/COUNT(*), SUM, AVG, MIN, MAX, BOOL_AND/EVERY, BOOL_OR, STRING_AGG, ARRAY_AGG, JSON_AGG, JSONB_AGG, JSON_OBJECT_AGG, JSONB_OBJECT_AGG, BIT_AND, BIT_OR, BIT_XOR, STDDEV_POP/STDDEV, STDDEV_SAMP, VAR_POP, VAR_SAMP/VARIANCE, MODE, PERCENTILE_CONT, PERCENTILE_DISC, CORR, COVAR_POP, COVAR_SAMP, REGR_AVGX/AVGY/COUNT/INTERCEPT/R2/SLOPE/SXX/SXY/SYY | 36 |
| **Expression types** | ColumnRef, A_Const, BinaryOp (AEXPR_OP + unary), BoolExpr (AND/OR/NOT), FuncCall, TypeCast, NullTest, CaseExpr, CoalesceExpr, NullIfExpr, MinMaxExpr, SQLValueFunction (15 variants), BooleanTest (6 variants), SubLink (EXISTS/ANY/EXPR), ArrayExpr, RowExpr, A_Indirection, AEXPR_IN, AEXPR_BETWEEN (4 variants), AEXPR_DISTINCT/NOT_DISTINCT, AEXPR_SIMILAR, AEXPR_OP_ANY/ALL | 30+ |
| **Auto-rewrites** | DISTINCT ON → ROW_NUMBER(), GROUPING SETS/CUBE/ROLLUP → UNION ALL, Scalar subquery in WHERE → CROSS JOIN, SubLinks in OR → UNION, Multi-PARTITION BY windows → nested subqueries | 5 |
| **Join types** | INNER, LEFT, RIGHT (→LEFT swap), FULL, CROSS, NATURAL (catalog-resolved), LATERAL (INNER/LEFT) | 7 |
| **Set operations** | UNION ALL, UNION (dedup), INTERSECT [ALL], EXCEPT [ALL], mixed UNION/UNION ALL | 5 |
| **CTEs** | Non-recursive (single + multi-ref shared delta), WITH RECURSIVE (semi-naive/DRed/recomputation) | 2 |
| **Window** | PARTITION BY, ORDER BY, frame clauses (ROWS/RANGE/GROUPS + EXCLUDE), named WINDOW | Full |
| **Subqueries** | FROM subquery, EXISTS/NOT EXISTS in WHERE, IN/NOT IN (subquery), ALL (subquery), scalar in SELECT, scalar in WHERE (rewritten) | Full |

### 3.2 Auto-Rewrite Pipeline (Pre-Parse)

These functions transparently rewrite the raw parse tree before the main DVM parser runs:

| Order | Function | Trigger | Rewrite |
|-------|----------|---------|---------|
| 1 | `rewrite_distinct_on()` | `DISTINCT ON(expr)` in SELECT | → `ROW_NUMBER() OVER (PARTITION BY expr ORDER BY ...) = 1` subquery |
| 2 | `rewrite_grouping_sets()` | `GROUPING SETS`/`CUBE`/`ROLLUP` in GROUP BY | → `UNION ALL` of separate `GROUP BY` queries with `GROUPING()` literals |
| 3 | `rewrite_scalar_subquery_in_where()` | Scalar `SubLink` in WHERE | → `CROSS JOIN` with scalar subquery as inline view |
| 4 | `rewrite_sublinks_in_or()` | `EXISTS`/`IN` SubLinks inside `OR` | → `UNION` of separate filtered queries |
| 5 | `rewrite_multi_partition_windows()` | Multiple different `PARTITION BY` clauses | → Nested subqueries, one per distinct partitioning |

### 3.3 Infrastructure

| Component | Status |
|-----------|--------|
| **CDC triggers** | Row-level AFTER INSERT/UPDATE/DELETE + statement-level AFTER TRUNCATE |
| **WAL transition** | Hybrid trigger→WAL migration with configurable timeout |
| **DDL tracking** | Event trigger on `ddl_command_end`; classifies changes as Benign/ConstraintChange/ColumnChange |
| **Schema snapshots** | `columns_used` per dependency; `schema_fingerprint` for fast equality |
| **Block source DDL** | `pg_trickle.block_source_ddl` GUC (default: false) |
| **Keyless tables** | All-column content hash for `__pgt_row_id` |
| **Volatile detection** | `pg_proc.provolatile` lookup; rejects volatile in DIFF, warns for stable |
| **MERGE-based refresh** | Cached MERGE SQL templates, prepared statements, planner hints |
| **GUC count** | 17 settings in `pg_trickle.*` namespace |

---

## 4. Gap Category 1: Expression Node Types

These are PostgreSQL raw parse tree node types not handled in `node_to_expr()`.
The catch-all returns `PgTrickleError::UnsupportedOperator` with the node tag
name, so none are silent — they all surface as clear errors.

### G1.1 — `T_CollateExpr` (COLLATE clause on expressions)

| Field | Value |
|-------|-------|
| **PostgreSQL syntax** | `expr COLLATE "C"`, `column COLLATE "en_US"` |
| **Parse tree** | `T_CollateExpr { arg, collname }` |
| **Current behavior** | Rejected: `"Expression type T_CollateExpr is not supported"` |
| **Severity** | **P2** — rejected with error |
| **Impact** | Medium — affects internationalized text queries, case-insensitive comparisons |
| **Effort** | 1–2 hours |
| **Plan** | Deparse as `(expr) COLLATE "collation_name"` → `Expr::Raw` |

```sql
-- Currently rejected:
SELECT name COLLATE "C" AS sorted_name FROM users ORDER BY name COLLATE "C"
SELECT * FROM t WHERE name COLLATE "en_US" = 'café'
```

**Implementation:** Extract `collname` from the `CollateExpr` node, recursively
deparse the `arg` expression, emit `Expr::Raw("(arg_sql) COLLATE \"collname\"")`.

---

### G1.2 — `T_XmlExpr` (XML processing expressions)

| Field | Value |
|-------|-------|
| **PostgreSQL syntax** | `XMLELEMENT`, `XMLFOREST`, `XMLPARSE`, `XMLPI`, `XMLROOT`, `XMLSERIALIZE`, `XMLCONCAT` |
| **Current behavior** | Rejected: `"Expression type T_XmlExpr is not supported"` |
| **Severity** | **P4** — very niche |
| **Impact** | Very low — XML processing in PostgreSQL is rare |
| **Effort** | 4–6 hours (many sub-variants) |
| **Recommendation** | Defer indefinitely — XML features are extremely niche |

---

### G1.3 — `T_GroupingFunc` (GROUPING() outside auto-rewrite)

| Field | Value |
|-------|-------|
| **PostgreSQL syntax** | `GROUPING(col1, col2)` in SELECT list |
| **Current behavior** | Rejected when encountered outside the GROUPING SETS rewrite path |
| **Severity** | **P4** — edge case only |
| **Impact** | Very low — `GROUPING()` is only meaningful with GROUPING SETS, which is auto-rewritten |
| **Effort** | 1 hour |
| **Recommendation** | Defer — the auto-rewrite handles the standard case. Only trips if someone uses `GROUPING()` without GROUPING SETS, which is meaningless in PostgreSQL too. |

---

### G1.4 — `T_JsonIsPredicate` (PostgreSQL 16+)

| Field | Value |
|-------|-------|
| **PostgreSQL syntax** | `expr IS JSON`, `expr IS JSON SCALAR`, `expr IS JSON ARRAY`, `expr IS JSON OBJECT`, `expr IS NOT JSON` |
| **Current behavior** | Rejected: `"Expression type T_JsonIsPredicate is not supported"` |
| **Severity** | **P2** — growing in usage |
| **Impact** | Medium — JSON validation is increasingly common |
| **Effort** | 2–3 hours |
| **Plan** | Deparse as `Expr::Raw("(expr) IS [NOT] JSON [SCALAR|ARRAY|OBJECT] [WITH|WITHOUT UNIQUE KEYS]")` |

```sql
-- Currently rejected:
SELECT * FROM api_logs WHERE response IS JSON
SELECT * FROM events WHERE payload IS JSON OBJECT
```

---

### G1.5 — `T_JsonConstructorExpr` (PostgreSQL 16+ SQL/JSON constructors)

| Field | Value |
|-------|-------|
| **PostgreSQL syntax** | `JSON()`, `JSON_ARRAY()`, `JSON_OBJECT()`, `JSON_ARRAYAGG()`, `JSON_OBJECTAGG()`, `JSON_SCALAR()`, `JSON_SERIALIZE()` |
| **Current behavior** | Rejected as unrecognized expression type |
| **Severity** | **P2** — SQL standard JSON constructors growing in adoption |
| **Impact** | Medium — these are the SQL-standard way to construct JSON |
| **Effort** | 4–6 hours (several sub-variants) |
| **Plan** | Deparse each variant to its SQL representation via `Expr::Raw` |

```sql
-- Currently rejected:
SELECT JSON_OBJECT('name': name, 'age': age) FROM users
SELECT JSON_ARRAY(1, 2, 3)
SELECT JSON_ARRAYAGG(name ORDER BY name) FROM users GROUP BY dept
```

**Note:** `JSON_ARRAYAGG` and `JSON_OBJECTAGG` are aggregate functions that PostgreSQL
16+ implements via `T_JsonConstructorExpr` rather than `T_FuncCall`. They bypass
the regular aggregate recognition path entirely.

---

## 5. Gap Category 2: Source Object Types

### G2.1 — Views as Sources in DIFFERENTIAL Mode ✅ RESOLVED

> **Resolved by:** View inlining auto-rewrite. Views are
> transparently replaced with inline subqueries so CDC triggers land on base
> tables. Nested views fully expanded. Original query preserved for reinit.

| Field | Value |
|-------|-------|
| **Gap** | CDC triggers cannot be placed on views — changes to underlying tables are silently missed |
| **Current behavior** | View is resolved via `pg_class` OID lookup; stream table creation succeeds. But `create_change_trigger()` creates row-level triggers on the view's OID, which PostgreSQL silently ignores (row-level triggers on views require INSTEAD OF triggers). DIFFERENTIAL mode produces no deltas — the stream table becomes permanently stale after initial population. |
| **Severity** | **P0 — Silent data staleness** |
| **Impact** | High — views are commonly used as abstraction layers |
| **Effort** | 4–6 hours |
| **Detection** | None — no error, no warning. The stream table appears healthy but never updates. |

**Implementation options:**

| Option | Approach | Effort | Tradeoffs |
|--------|----------|--------|-----------|
| **A. Reject views in DIFF** | Check `relkind` in `resolve_table_oid()` — reject 'v'/'m' in FROM when mode is DIFFERENTIAL | 2 hours | Safe, clear error; limits flexibility |
| **B. Recursive source resolution** | Walk `pg_depend` + `pg_rewrite` to find base tables underlying the view, attach CDC triggers to those instead | 8–12 hours | Complex; multi-level view stacks; fragile to view redefinition |
| **C. Warn + force FULL** | Detect view sources and auto-downgrade to FULL mode with a WARNING | 3 hours | Pragmatic; no silent staleness; performance cost for views |

**Recommendation:** Option A (reject) for DIFFERENTIAL, with a clear error message
suggesting FULL mode. Option C as a follow-up enhancement.

```sql
-- A view used as a source:
CREATE VIEW active_orders AS SELECT * FROM orders WHERE status = 'active';

-- This should either reject or warn:
SELECT pgtrickle.create('order_summary', 'DIFFERENTIAL',
  'SELECT customer_id, COUNT(*) FROM active_orders GROUP BY customer_id');
```

---

### G2.2 — Materialized Views as Sources ✅ RESOLVED

> **Resolved by:** View inlining auto-rewrite.
> Materialized views are rejected in DIFFERENTIAL mode with a clear error.
> Allowed in FULL mode.

| Field | Value |
|-------|-------|
| **Gap** | Same as G2.1 — CDC triggers don't fire on `REFRESH MATERIALIZED VIEW` |
| **Current behavior** | Materialized view resolved as a regular source; CDC triggers attached but never fire on REFRESH |
| **Severity** | **P1 — Incorrect semantics** |
| **Impact** | Low-Medium — materialized views as sources are less common |
| **Effort** | Included in G2.1 fix (relkind 'm' check) |

---

### G2.3 — Foreign Tables as Sources

| Field | Value |
|-------|-------|
| **Gap** | Row-level triggers cannot be created on foreign tables (PostgreSQL restriction) |
| **Current behavior** | `create_change_trigger()` will fail with a PostgreSQL error, which surfaces to the user. This is not silent but the error message is confusing (a PG internal error about trigger creation). |
| **Severity** | **P2 — Confusing error** |
| **Impact** | Low — foreign tables as stream table sources are uncommon |
| **Effort** | 1 hour — detect `relkind = 'f'` and reject with clear message |

---

## 6. Gap Category 3: Aggregate Functions

### G3.1 — User-Defined Aggregates

| Field | Value |
|-------|-------|
| **Gap** | Custom aggregates created with `CREATE AGGREGATE` are not recognized |
| **Current behavior** | Treated as regular function calls → placed into `Expr::FuncCall` instead of `AggExpr`. The defining query is accepted but the aggregate semantics are lost — the function sees only the current row instead of the group. |
| **Severity** | **P1 — Incorrect semantics** |
| **Impact** | Medium — popular extensions define custom aggregates (e.g., `tdigest`, `hll_union_agg`, `topn_agg`) |
| **Effort** | 4–6 hours |
| **Detection** | None — the query parses successfully. Results are wrong. |

**Root cause:** `extract_aggregates()` checks function names against a hardcoded
`is_known_aggregate()` list. Functions not in this list are assumed to be scalar
functions, even when they appear in a GROUP BY context.

**Implementation:**
1. Query `pg_proc` for `prokind = 'a'` (aggregate) during aggregate extraction
2. If a function call in the SELECT list is an aggregate per `pg_proc` but not
   in the `AggFunc` enum: reject with clear error suggesting FULL mode
3. This prevents silent misclassification while preserving clear error messages

```sql
-- Example with a custom aggregate from an extension:
CREATE AGGREGATE my_median(numeric) (
  SFUNC = my_median_sfunc, STYPE = internal, FINALFUNC = my_median_final
);

-- pg_trickle silently treats this as a scalar function:
SELECT dept, my_median(salary) FROM employees GROUP BY dept;
-- → Wrong: my_median sees each row individually, not the group
```

---

### G3.2 — `ANY_VALUE` (PostgreSQL 16+)

| Field | Value |
|-------|-------|
| **Gap** | `ANY_VALUE(expr)` aggregate not recognized |
| **Current behavior** | Treated as scalar function → wrong results in GROUP BY context |
| **Severity** | **P2** — recognized as an aggregate by PostgreSQL 16+ |
| **Impact** | Low-Medium — growing in usage as a replacement for non-deterministic GROUP BY columns |
| **Effort** | 1 hour — add `AnyValue` variant using group-rescan pattern |

```sql
-- PostgreSQL 16+:
SELECT department, ANY_VALUE(manager_name) FROM employees GROUP BY department;
```

---

### G3.3 — `RANGE_AGG` / `RANGE_INTERSECT_AGG` (PostgreSQL 14+)

| Field | Value |
|-------|-------|
| **Gap** | Range aggregation functions not recognized |
| **Current behavior** | Treated as scalar functions |
| **Severity** | **P4** — very niche |
| **Impact** | Very low |
| **Effort** | 1 hour each — group-rescan pattern |

---

### G3.4 — `JSON_ARRAYAGG` / `JSON_OBJECTAGG` (SQL/JSON standard, PG 16+)

| Field | Value |
|-------|-------|
| **Gap** | SQL/JSON standard aggregate functions use `T_JsonConstructorExpr` instead of `T_FuncCall` |
| **Current behavior** | Not recognized as aggregates — parser never sees a `FuncCall` node for these |
| **Severity** | **P2** — SQL-standard JSON aggregates growing in adoption |
| **Impact** | Medium — blocks standard-compliant JSON aggregation |
| **Effort** | 4–6 hours — requires `T_JsonConstructorExpr` handling in addition to aggregate recognition |

**Note:** The existing `json_agg`/`jsonb_agg`/`json_object_agg`/`jsonb_object_agg`
PostgreSQL-specific aggregates work fine. This gap only affects the SQL-standard
syntax introduced in PostgreSQL 16.

---

## 7. Gap Category 4: Type System & Data Types

### G4.1 — Keyless Table `::text` Cast Lossyness

| Field | Value |
|-------|-------|
| **Gap** | Content hash for keyless tables uses `col::text` which is lossy for some types |
| **Current behavior** | Two distinct rows that produce identical `::text` representations get the same `__pgt_row_id` → one row silently overwrites the other in the MERGE |
| **Severity** | **P1 — Silent data loss** (only for keyless tables) |
| **Impact** | Low — requires both keyless tables AND problematic types |
| **Affected types** | `float8` (rounding: `0.1 + 0.2 → '0.30000000000000004'` but intermediate representations may collide), `bytea` (encoding-dependent), `bit varying` (truncation), composite types (may not have unique text output) |
| **Effort** | 4–6 hours |

**Mitigations considered:**
- Use `col::bytea` instead of `col::text` — more faithful but slower
- Use `pg_column_size()` as a tie-breaker — adds cost but eliminates collisions
- Store `ctid` as a fallback identifier — but `ctid` changes on UPDATE/VACUUM

**Note:** Tables with primary keys are not affected — PK columns provide unique
identity directly.

---

### G4.2 — Enum Type ALTER Inconsistency

| Field | Value |
|-------|-------|
| **Gap** | `ALTER TYPE ... ADD VALUE` on an enum used in a source table is not tracked |
| **Current behavior** | `detect_schema_change_kind()` classifies based on `pg_attribute` changes. Enum value additions don't change column attributes → classified as `Benign` → no reinit |
| **Severity** | **P4** — rarely causes actual problems |
| **Impact** | Very low — new enum values don't invalidate existing data or delta SQL |
| **Effort** | 2 hours — add `pg_enum` change detection in `handle_alter_type()` |

---

### G4.3 — Composite Type Column Access in Delta SQL

| Field | Value |
|-------|-------|
| **Gap** | `(record).field` access in delta SQL may produce wrong results when the composite type definition changes |
| **Current behavior** | Field access is deparsed as `Expr::Raw` — the field name is baked into the delta template. If `ALTER TYPE` changes the composite, the delta SQL silently uses the old structure. |
| **Severity** | **P4** — extremely rare scenario |
| **Impact** | Very low |
| **Effort** | 2 hours — add `ALTER TYPE` tracking for composite types |

---

### G4.4 — NULL Handling in Keyless Table Content Hash

| Field | Value |
|-------|-------|
| **Gap** | Rows differing only by NULL placement can collide in the content hash |
| **Current behavior** | `pg_trickle_hash_multi(ARRAY[col1::text, col2::text, ...])` converts all values to text. `NULL::text` is `NULL`, and `NULL` in an array may be handled inconsistently depending on the hash implementation. Two rows `(1, NULL, 3)` and `(1, 3, NULL)` could potentially hash to the same value if NULL handling in the array is position-insensitive. |
| **Severity** | **P4** — theoretical concern; depends on `pg_trickle_hash_multi` implementation |
| **Impact** | Very low — requires specific NULL patterns in keyless tables |
| **Effort** | 2 hours — add position-aware NULL sentinel values (e.g., `'__null_N__'` where N is the ordinal) |

---

## 8. Gap Category 5: PostgreSQL 16/17/18 New Features

### G5.1 — `JSON_TABLE()` (PostgreSQL 17)

| Field | Value |
|-------|-------|
| **PostgreSQL syntax** | `SELECT * FROM JSON_TABLE(data, '$.items[*]' COLUMNS (id INT PATH '$.id', name TEXT PATH '$.name'))` |
| **Parse tree** | `T_JsonTable` as a FROM item |
| **Current behavior** | Rejected: `"Unsupported FROM item node type: T_JsonTable"` |
| **Severity** | **P2** — new SQL-standard feature |
| **Impact** | Medium — JSON document processing is increasingly important |
| **Effort** | 8–12 hours |

**Why this is complex:** `JSON_TABLE` is a row-generating construct similar to
`LATERAL`, but its internal structure (columns, paths, nested paths) requires
dedicated parsing. The delta strategy would be row-scoped recomputation similar
to `LateralFunction`.

```sql
-- Currently rejected:
SELECT jt.*
FROM api_responses r,
     JSON_TABLE(r.body, '$.results[*]'
       COLUMNS (
         id INT PATH '$.id',
         name TEXT PATH '$.name',
         score NUMERIC PATH '$.score'
       )) AS jt;
```

---

### G5.2 — SQL/JSON Path Expressions (`@?`, `@@`)

| Field | Value |
|-------|-------|
| **Gap** | JSON path operators `@?` and `@@` work as `AEXPR_OP` — functional but opaque |
| **Current behavior** | Works correctly as binary operators |
| **Severity** | **P4** — actually works, just isn't treated intelligently |
| **Impact** | None — operational |
| **Note** | Not a real gap — listed for documentation completeness |

---

### G5.3 — `MERGE` Statement as Defining Query

| Field | Value |
|-------|-------|
| **Gap** | `MERGE INTO ... USING ... WHEN MATCHED/NOT MATCHED` cannot be used as a defining query |
| **Current behavior** | Only `SELECT` statements are accepted as defining queries |
| **Severity** | **P4** — MERGE is a DML statement, not a query |
| **Impact** | None — MERGE is used internally by pg_trickle for delta application |
| **Note** | Not a real gap — defining queries are SELECT statements by design |

---

### G5.4 — Virtual Generated Columns (PostgreSQL 18)

| Field | Value |
|-------|-------|
| **Gap** | Virtual generated columns have no storage but may appear in trigger `NEW` records |
| **Current behavior** | Untested. The CDC trigger references `NEW."col_name"` for all columns from `pg_attribute`. Virtual columns (PG 18: `attgenerated = 'v'`) have no stored value — reading them in a BEFORE trigger returns `NULL`, but in an AFTER trigger returns the computed value. |
| **Severity** | **P2** — could silently produce wrong CDC data |
| **Impact** | Low — virtual generated columns are new in PG 18 |
| **Effort** | 2–3 hours |
| **Plan** | Filter out virtual generated columns (`attgenerated = 'v'`) from CDC trigger column lists. Instead, recompute from the expression in the stream table if needed. |

---

### G5.5 — `RETURNING` Clause Improvements (PostgreSQL 18)

| Field | Value |
|-------|-------|
| **Gap** | `RETURNING` expressions on DML are not relevant |
| **Current behavior** | N/A — defining queries are SELECT-only |
| **Severity** | **P4** — not a gap |
| **Note** | Listed for completeness. PG 18's enhanced RETURNING is used internally by MERGE but transparent. |

---

## 9. Gap Category 6: CDC & Change Tracking Edge Cases

### G6.1 — Partitioned Tables as Sources

| Field | Value |
|-------|-------|
| **Gap** | Row-level triggers on a partitioned table may not fire for rows routed to child partitions |
| **Current behavior** | The trigger is attached to the table OID from `pg_class`. For declaratively partitioned tables, DML on the parent is routed to partitions. Before PostgreSQL 13, AFTER triggers on the parent did not fire for partition-routed rows. PostgreSQL 13+ fires parent triggers for partition-routed rows, but the trigger's `TG_TABLE_NAME` and `TG_RELID` still reference the parent. |
| **Severity** | **P0 — Silent data loss** (PostgreSQL 12 and earlier); **P4** in PostgreSQL 13+ |
| **Impact** | High for PG <13: changes silently missed. Low for PG 13+: likely works correctly. |
| **Effort** | 3–4 hours |
| **Target PG version** | pg_trickle targets PG 18, so this is likely **not a real problem** in practice |

**Verification needed:** Confirm that with PG 18 + pgrx 0.17, AFTER triggers on
partitioned table parents correctly fire for all routed DML. If so, close this
gap with a documentation note.

**For PG 13+:** The trigger fires correctly, but the change buffer table uses the
**parent's** OID for its name (`pgtrickle_changes.changes_<parent_oid>`). This
should be correct since the stream table's `source_oid` also references the parent.

---

### G6.2 — Logical Replication Targets

| Field | Value |
|-------|-------|
| **Gap** | Tables receiving data via logical replication do not fire normal triggers |
| **Current behavior** | No detection — if a source table is a logical replication target, changes arriving via replication are invisible to CDC triggers |
| **Severity** | **P1 — Silent data staleness** |
| **Impact** | Low-Medium — affects logical replication setups |
| **Effort** | 2 hours for detection + warning |
| **Plan** | At stream table creation, check if source tables have active subscriptions (`pg_subscription_rel`) and emit a WARNING if so |

---

### G6.3 — TOAST Column Bloat in Change Buffers

| Field | Value |
|-------|-------|
| **Gap** | Large text/bytea columns are stored in full in the change buffer table |
| **Current behavior** | CDC triggers copy `NEW."col"` values directly into the change buffer. For columns with large TOAST values (>8KB), this forces de-TOASTing and full storage, potentially causing significant buffer table bloat. |
| **Severity** | **P2** — performance/storage issue, not correctness |
| **Impact** | Medium — tables with large text/JSON columns will have oversized change buffers |
| **Effort** | 4–6 hours |
| **Mitigation** | Only store columns that are in `columns_used` for the dependency. Currently all columns are captured regardless. or use content hash of TOAST columns. |

---

### G6.4 — Row-Level Security on Change Buffer Tables

| Field | Value |
|-------|-------|
| **Gap** | If RLS is enabled on the `pgtrickle_changes` schema, CDC trigger inserts could fail |
| **Current behavior** | Change buffer tables are owned by the extension installer. RLS is not explicitly disabled on them. If a DBA enables RLS globally or on the `pgtrickle_changes` schema, trigger-based inserts may be blocked. |
| **Severity** | **P4** — extremely unlikely scenario |
| **Impact** | Very low |
| **Effort** | 1 hour — add `ALTER TABLE ... DISABLE ROW LEVEL SECURITY` to buffer table creation |

---

## 10. Gap Category 7: Correctness Blind Spots

### G7.1 — Volatile Expressions Inside `Expr::Raw`

| Field | Value |
|-------|-------|
| **Gap** | The volatility checker only walks `Expr::FuncCall` and `Expr::BinaryOp` nodes. Expressions that deparse to `Expr::Raw` (CASE, COALESCE, BETWEEN, ARRAY[], ROW(), etc.) are opaque to the checker. |
| **Current behavior** | A volatile function wrapped in CASE, COALESCE, or other complex expressions is silently accepted in DIFFERENTIAL mode |
| **Severity** | **P0 — Silent correctness gap** |
| **Impact** | Medium — `CASE WHEN random() > 0.5 THEN ...` or `COALESCE(gen_random_uuid(), ...)` bypass volatile detection |
| **Effort** | 6–8 hours |

**Root cause:** `collect_volatilities()` at `parser.rs` line ~1381 walks `Expr`
nodes recursively but has an implicit pass-through for `Expr::Raw` — it only
descends into `FuncCall` args and `BinaryOp` operands.

**Implementation options:**

| Option | Approach | Effort | Accuracy |
|--------|----------|--------|----------|
| **A. Re-parse `Expr::Raw` strings** | Run `pg_parse_query()` on the SQL text inside `Expr::Raw` to get back a parse tree, then walk that for `FuncCall` nodes | 6–8 hours | High — catches all functions |
| **B. Walk the original parse tree** | Move volatility checking to operate on the raw parse tree (before `Expr` conversion) | 8–10 hours | Perfect — operates on full information |
| **C. Regex scan for function calls** | Heuristic: scan `Expr::Raw` text for `funcname(` patterns and look those up in `pg_proc` | 2–3 hours | Low — misses edge cases, false positives |

**Recommendation:** Option A. Re-parsing `Expr::Raw` strings is reliable and
reuses existing infrastructure. The pg_parse_query API is already available via
pgrx's raw parse tree access.

```sql
-- Currently NOT detected as volatile:
SELECT CASE WHEN random() > 0.5 THEN 'heads' ELSE 'tails' END AS coin FROM t;
SELECT COALESCE(gen_random_uuid()::text, 'default') AS id FROM t;
SELECT ARRAY[random(), random()] AS arr FROM t;
```

---

### G7.2 — Operator Volatility Not Checked

| Field | Value |
|-------|-------|
| **Gap** | Custom operators have underlying `oprcode` functions with their own volatility, but these are never checked |
| **Current behavior** | Only function-call volatility is checked. Operators like PostGIS's `&&` (bounding box overlap) or custom volatile operators are assumed safe. |
| **Severity** | **P1 — Potential silent correctness gap** |
| **Impact** | Low — custom volatile operators are extremely rare |
| **Effort** | 3–4 hours |
| **Plan** | For `Expr::BinaryOp`, look up operator in `pg_operator` → `oprcode` → `pg_proc.provolatile` |

---

### G7.3 — Schema-Qualified Column Reference Ambiguity

| Field | Value |
|-------|-------|
| **Gap** | 3-field `ColumnRef` (`schema.table.column`) silently strips the schema qualifier |
| **Current behavior** | `node_to_expr()` for 3-field ColumnRef emits `Expr::ColumnRef { table_alias: table, column_name }`, dropping the schema prefix. If a query references `schema1.t.x` and `schema2.t.x`, both become `t.x` in the generated delta SQL. |
| **Severity** | **P2 — Potential ambiguity** |
| **Impact** | Very low — self-joins between same-named tables in different schemas are extremely rare |
| **Effort** | 2–3 hours |
| **Plan** | When multiple FROM items share the same table name but different schemas, use schema-qualified names or synthetic aliases in delta SQL |

---

## 11. Gap Category 8: Operational & Concurrency Gaps

### G8.1 — Cross-Session MERGE Cache Staleness

| Field | Value |
|-------|-------|
| **Gap** | MERGE template cache is thread-local (per PostgreSQL backend). If session A alters a stream table's defining query, session B's cached MERGE template is stale. |
| **Current behavior** | Session B continues to use the old MERGE SQL until its cache is invalidated by a refresh error or session reconnection |
| **Severity** | **P2** — affects multi-backend deployments |
| **Impact** | Low — `ALTER STREAM TABLE` is infrequent; the stale cache is invalidated on the next DDL event in session B's scope |
| **Effort** | 4–6 hours |
| **Plan** | Use `pg_notify` to broadcast cache invalidation events, or add a catalog version counter that backends check before each refresh |

---

### G8.2 — Missing `ALTER FUNCTION` / `ALTER OPERATOR` Tracking

| Field | Value |
|-------|-------|
| **Gap** | DDL hooks don't track changes to functions or operators referenced in defining queries |
| **Current behavior** | If a user replaces a function (`CREATE OR REPLACE FUNCTION`) or drops a custom operator used in a defining query, the stream table silently uses the new function semantics or fails on next refresh |
| **Severity** | **P2** — function replacement silently changes stream table semantics |
| **Impact** | Low — most functions in defining queries are PostgreSQL built-ins |
| **Effort** | 4–6 hours |
| **Plan** | Track function OIDs referenced in defining queries. On `ddl_command_end` for `CREATE/ALTER/DROP FUNCTION`, check if any tracked function OID was affected and trigger reinit. |

---

### G8.3 — Deferred Cleanup After Session Crash

| Field | Value |
|-------|-------|
| **Gap** | Change buffer cleanup is deferred to the next refresh cycle. If a session crashes between refresh and cleanup, the cleanup queue is lost. |
| **Current behavior** | The change buffer grows until the next successful refresh in any session, which then cleans up using LSN range predicates. Not a correctness issue — stale rows are harmless — but causes WAL and storage bloat. |
| **Severity** | **P4** — operational consideration |
| **Impact** | Low — self-healing on next refresh |
| **Effort** | 2–3 hours — add a background worker sweep or startup cleanup hook |

---

### G8.4 — Advisory Lock Contention

| Field | Value |
|-------|-------|
| **Gap** | No explicit advisory lock to prevent duplicate concurrent refreshes of the same stream table |
| **Current behavior** | The `shmem` coordination layer prevents most duplicate refreshes via shared memory flags, but there's a small race window between checking the flag and starting the refresh. |
| **Severity** | **P4** — theoretical; shmem coordination handles >99% of cases |
| **Impact** | Very low — double-refresh is idempotent (correct but wasteful) |
| **Effort** | 1–2 hours — add `pg_advisory_xact_lock()` at refresh entry point |

---

## 12. Gap Category 9: Documentation Drift

Since SQL_GAPS_5, several features have been implemented but documentation has
not been updated to reflect the new capabilities. These are **not code bugs**
but documentation inaccuracies that mislead users.

### D1. SQL_REFERENCE.md — Features Documented as Rejected That Are Now Supported

| Feature | Doc Says | Actual State | Priority |
|---------|----------|-------------|----------|
| **NATURAL JOIN** | "Rejected with rewrite suggestion" | **Supported** — parser resolves common columns | **P3** |
| **DISTINCT ON** | "Rejected with rewrite suggestion" | **Supported** — auto-rewritten to ROW_NUMBER() | **P3** |
| **GROUPING SETS/CUBE/ROLLUP** | "Rejected with rewrite suggestion" | **Supported** — auto-rewritten to UNION ALL | **P3** |
| **SubLinks in OR** | "Rejected" | **Supported** — auto-rewritten to UNION | **P3** |

### D2. All Docs — Missing 12 Regression/Correlation Aggregates

| Doc | Issue |
|-----|-------|
| SQL_REFERENCE.md | Lists 24 aggregates; missing CORR, COVAR_POP/SAMP, REGR_* (12 functions) |
| DVM_OPERATORS.md | Aggregate table lists 24; missing same 12 |

### D3. ARCHITECTURE.md — Stale Operator Table

| Issue | Detail |
|-------|--------|
| Operator count | Lists 14 operators; actual count is 21 |
| Missing operators | LateralSubquery, SemiJoin, AntiJoin, ScalarSubquery, FullJoin, RecursiveSelfRef, join_common |
| Module map | Missing 6+ operator source files |
| CDC description | Omits TRUNCATE trigger |

### D4. CONFIGURATION.md — Missing GUC Sections

| GUC | Status |
|-----|--------|
| `pg_trickle.differential_max_change_ratio` | Exists in code; no dedicated section in docs |
| `pg_trickle.cleanup_use_truncate` | Exists in code; no dedicated section in docs |
| `pg_trickle.merge_planner_hints` | Exists in code; no dedicated section in docs |
| `pg_trickle.merge_work_mem_mb` | Exists in code; no dedicated section in docs |
| `pg_trickle.merge_strategy` | Exists in code; no dedicated section in docs |
| `pg_trickle.use_prepared_statements` | Exists in code; no dedicated section in docs |

**Actual GUC count:** 17 (not the 12 documented).

### D5. Missing Feature Documentation

| Feature | Implemented | Not Documented |
|---------|------------|----------------|
| HAVING clause | Full support | Never mentioned |
| Keyless table support | All-column content hash | Not documented in SQL_REFERENCE |
| Volatile function detection | Rejects in DIFF, warns for stable | Not documented |
| Schema snapshots + fingerprints | columns_used, schema_fingerprint | Not in ARCHITECTURE |
| Auto-rewrite pipeline (5 passes) | Fully operational | Not documented as a system |
| Scalar subquery in WHERE rewrite | Auto-rewritten to CROSS JOIN | Not documented |
| Multi-PARTITION BY rewrite | Auto-rewritten to nested subqueries | Not documented |

### D6. DVM_OPERATORS.md — Cross-Document Contradiction

DVM_OPERATORS.md correctly states "NATURAL JOIN is supported — common columns
are resolved automatically" while SQL_REFERENCE.md says "Rejected." DVM_OPERATORS.md
is correct.

---

## 13. Gap Category 10: Intentional Rejections (Not Gaps)

These items are **permanently rejected by design** — they are not gaps to be
fixed. Listed here for completeness and to prevent re-evaluation.

| Construct | Reason | Error Message |
|-----------|--------|---------------|
| `LIMIT` without `ORDER BY` | Undefined ordering — rejected | "LIMIT is not supported in defining queries." |
| `OFFSET` | Stream tables materialize full result sets — rejected | "OFFSET is not supported in defining queries." |
| `ORDER BY` + `LIMIT` (TopK) | ✅ **Now supported** — scoped recomputation via MERGE | Accepted (not an error) |
| `FETCH FIRST` / `FETCH NEXT` | Same as LIMIT — TopK if ORDER BY present, rejected otherwise | Same as LIMIT |
| `FOR UPDATE` / `FOR SHARE` / `FOR NO KEY UPDATE` / `FOR KEY SHARE` | Row-level locking has no meaning on materialized stream tables | "FOR UPDATE/FOR SHARE is not supported." |
| `TABLESAMPLE` | Non-deterministic; stream tables are complete result sets | "TABLESAMPLE is not supported." |
| `ROWS FROM()` with multiple functions | Extremely niche | "ROWS FROM() with multiple functions is not supported." |
| `LATERAL` with `RIGHT`/`FULL JOIN` | PostgreSQL itself restricts this | "Only INNER JOIN LATERAL and LEFT JOIN LATERAL are supported." |
| `XMLAGG` | Extremely niche XML aggregation | Recognized but rejected for DIFFERENTIAL mode |
| Hypothetical-set aggregates | Almost always used as window functions, not aggregates | Recognized but rejected for DIFFERENTIAL mode |
| Window functions inside expressions | Architectural constraint; requires separate column | "Window functions nested inside expressions..." |
| `ORDER BY` | Accepted but silently discarded — row order is undefined | No error (intentional no-op) |
| Direct DML on stream tables | Refresh engine manages all writes | Triggers prevent manual DML |
| Direct DDL on storage tables | Managed by extension | DDL hooks warn/block |
| `SELECT` without `FROM` | Constant-only queries have no CDC source | "Defining query must have FROM clause" |

---

## 14. Prioritized Implementation Roadmap

### Tier 0 — Critical Correctness (Must Fix)

| Step | Gap | Description | Effort | Impact |
|------|-----|-------------|--------|--------|
| ~~**F1**~~ | ~~G2.1~~ | ~~Views as sources: view inlining auto-rewrite~~ | ~~Done~~ | ~~✅ Implemented~~ |
| ~~**F2**~~ | ~~G7.1~~ | ~~Volatile expressions in `Expr::Raw`: re-parse for volatility checking~~ | ~~Done~~ | ~~✅ Implemented~~ |
| ~~**F3**~~ | ~~G3.1~~ | ~~User-defined aggregates: detect via `pg_proc.prokind` and reject~~ | ~~Done~~ | ~~✅ Implemented~~ |

**Estimated effort:** 12–16 hours  
**Value:** Closes all remaining P0 and P1 silent correctness gaps.

### Tier 1 — High-Value Enhancements

| Step | Gap | Description | Effort | Impact |
|------|-----|-------------|--------|--------|
| ~~**F4**~~ | ~~G1.1~~ | ~~COLLATE expression support~~ | ~~Done~~ | ~~✅ Implemented~~ |
| **F5** | G1.4 | `IS JSON` predicate (PG 16+) | 2–3 hours | Enables JSON validation |
| ~~**F6**~~ | ~~G3.2~~ | ~~`ANY_VALUE` aggregate (PG 16+)~~ | ~~Done~~ | ~~✅ Implemented~~ |
| ~~**F7**~~ | ~~G5.4~~ | ~~Virtual generated columns (PG 18)~~ | ~~Done~~ | ~~✅ Implemented~~ |
| ~~**F8**~~ | ~~G2.3~~ | ~~Foreign tables: detect and reject clearly~~ | ~~Done~~ | ~~✅ Implemented~~ |
| ~~**F9**~~ | ~~G2.2~~ | ~~Materialized views: reject in DIFFERENTIAL~~ | ~~Incl. in F1~~ | ~~✅ Implemented~~ |

**Estimated effort:** 7–12 hours  
**Value:** Covers PostgreSQL 16–18 new features and improves error clarity.

### Tier 2 — SQL/JSON Ecosystem

| Step | Gap | Description | Effort | Impact |
|------|-----|-------------|--------|--------|
| **F10** | G1.5 | SQL/JSON constructors (JSON(), JSON_ARRAY(), etc.) | 4–6 hours | ~~✅ Implemented~~ |
| **F11** | G3.4 | JSON_ARRAYAGG / JSON_OBJECTAGG (SQL standard) | 4–6 hours | ~~✅ Implemented~~ |
| **F12** | G5.1 | JSON_TABLE() in FROM | 8–12 hours | ~~✅ Implemented~~ |

**Estimated effort:** 16–24 hours  
**Value:** Full SQL/JSON standard compliance (PostgreSQL 16+).

### Tier 3 — Operational Hardening

| Step | Gap | Description | Effort | Impact |
|------|-----|-------------|--------|--------|
| **F13** | G6.1 | Verify partitioned table CDC on PG 18 | 3–4 hours | ~~✅ Implemented~~ |
| **F14** | G6.2 | Detect logical replication targets | 2 hours | ~~✅ Implemented~~ |
| **F15** | G6.3 | Selective CDC column capture | 4–6 hours | Deferred (needs column lineage) |
| **F16** | G7.2 | Operator volatility checking | 3–4 hours | ~~✅ Implemented~~ |
| **F17** | G8.1 | Cross-session cache invalidation | 4–6 hours | ~~✅ Implemented~~ |
| **F18** | G8.2 | Function/operator DDL tracking | 4–6 hours | ~~✅ Implemented~~ |

**Estimated effort:** 20–28 hours  
**Value:** Production hardening for complex deployments.

### Tier 4 — Documentation Sync

| Step | Gap | Description | Effort |
|------|-----|-------------|--------|
| **F19** | D1 | Update SQL_REFERENCE.md — move 4 features from rejected to supported | 1 hour |
| **F20** | D2 | Document 12 regression/correlation aggregates across all docs | 1 hour |
| **F21** | D3 | Update ARCHITECTURE.md operator table and module map | 1 hour |
| **F22** | D4 | Add 7 missing GUC sections to CONFIGURATION.md | 2 hours |
| **F23** | D5 | Document HAVING, keyless tables, volatile detection, auto-rewrites | 2 hours |
| **F24** | D6 | Resolve DVM_OPERATORS.md / SQL_REFERENCE.md contradiction | 0.5 hours |

**Estimated effort:** 7–8 hours  
**Value:** Accurate documentation for users and contributors.

### Summary

| Tier | Steps | Effort | Cumulative |
|------|-------|--------|------------|
| 0 — Critical | F1–F3 | 12–16h | 12–16h |
| 1 — High Value | F4–F9 | 7–12h | 19–28h |
| 2 — SQL/JSON | F10–F12 | 16–24h | 35–52h |
| 3 — Operational | F13–F18 | 20–28h | 55–80h |
| 4 — Documentation | F19–F24 | 7–8h | 62–88h |
| **Total** | **24 steps** | **62–88h** | — |

### Recommended Execution Order

```
Session 1:  F1 (views) + F3 (user-defined aggs) + F8 (foreign tables)    ✅ Done
Session 2:  F7 (virtual gen cols) + F4 (COLLATE) + F6 (ANY_VALUE)        ✅ Done
Session 3:  F2 (volatile in Expr::Raw)                                    ✅ Done
Session 4:  F19–F24 (all documentation)                                   ✅ Done
Session 5:  F13 (partitioned tables) + F14 (replication targets)          ✅ Done
Session 6:  F5 (IS JSON) + F10 (SQL/JSON constructors)                   ✅ Done
Session 7:  F11 (JSON agg standard) + F12 (JSON_TABLE)                   ✅ Done
Session 8+: F15–F18 (operational hardening)                               ✅ Done (F15 implemented in v0.9.0)
```

**F15 (v0.9.0 implementation note):** F15 is now implemented end-to-end in v0.9.0.
`source_columns_used()` in the parser already walks all ColumnRef/expression nodes
and returns per-source column lists. `StDependency::union_referenced_columns_for_source()`
in `src/catalog.rs` computes the union across all downstream STs.
`cdc::resolve_referenced_column_defs()` filters the full column list to referenced ∪ PK,
and this is wired into `setup_cdc_for_source` (creation) and both `rebuild_cdc_trigger*`
functions (schema-change rebuild).

Two items remain before F15 is fully closed:
1. **Integration test** — verify the change buffer table has only the expected columns.
2. **Monitoring view** — expose `is_selective_capture_active` via the monitoring SQL API.

---

## 15. Historical Progress Summary

| Plan | Phase | Sessions | Items Resolved | Test Growth |
|------|-------|----------|---------------|-------------|
| SQL_GAPS_1 | Expression deparsing, CROSS JOIN, FOR UPDATE rejection | 1 | 6 | 745 → 757 |
| SQL_GAPS_2 | GROUPING SETS P0, GROUP BY hardening, TABLESAMPLE | 1 | 6 | 745 → 750 |
| SQL_GAPS_3 | Aggregates (5 new), subquery operators (Semi/Anti/Scalar Join) | ~5 | 10 | 750 → 809 |
| SQL_GAPS_4 | Report accuracy, ordered-set aggregates (MODE, PERCENTILE) | 1 | 7 | 809 → 826 |
| Hybrid CDC | Trigger→WAL transition, user triggers, pgt_ rename | ~3 | 12+ | 826 → 872 |
| SQL_GAPS_5 | 15 steps: volatile detection, DISTINCT ON, GROUPING SETS, ALL subquery, regression aggs, mixed UNION, TRUNCATE, schema infra, NATURAL JOIN, keyless tables, scalar WHERE, SubLinks-OR, multi-PARTITION, recursive CTE DIFF | ~10 | 15 | 872 → 896 |
| **SQL_GAPS_6** | **This plan: 38 new gaps identified, 24 steps proposed** | **8** | **23/24 done (F15 deferred)** | 896 → 1,138+ |

### Cumulative Scorecard

| Metric | SQL_GAPS_1 | SQL_GAPS_3 | SQL_GAPS_5 End | SQL_GAPS_6 (Now) |
|--------|-----------|-----------|----------------|-----------------|
| AggFunc variants | 5 | 10 | 36 | 39 |
| OpTree variants | 12 | 18 | 21 | 21 |
| Diff operators | 10 | 16 | 21 | 21 |
| Auto-rewrite passes | 0 | 0 | 5 | 5 |
| Unit tests | 745 | 809 | 896 | ~1,138 |
| E2E tests | ~100 | ~200 | 350 | 384 |
| E2E test files | ~15 | ~18 | 22 | 22 |
| P0 issues | 14 | 0 | 0 | 0 |
| P1 issues | 5 | 0 | 0 | 0 |
| Expression types | 7 | 15 | 30+ | 40+ |
| GUCs | ~6 | ~8 | 17 | 17 |

### What Changed Between SQL_GAPS_5 and SQL_GAPS_6

SQL_GAPS_5 focused on closing syntax-level gaps — every SQL construct that
pg_trickle could encounter was either handled or rejected with a clear error.
SQL_GAPS_6 shifts focus to **operational boundaries**:

- **Source object types**: Views, materialized views, foreign tables, partitioned tables
- **PostgreSQL version-specific features**: PG 16 SQL/JSON, PG 17 JSON_TABLE, PG 18 virtual generated columns
- **Type system edge cases**: Lossy `::text` casts, enum modifications, composite type changes
- **Correctness blind spots**: `Expr::Raw` opacity to volatility checking, operator volatility, cross-session cache coherence
- **CDC limitations**: TOAST bloat, replication targets, RLS interactions
- **Documentation drift**: 6 categories of stale docs from rapid feature development

This represents a maturation from "does the SQL parser handle this syntax?" to
"does the complete system behave correctly in all production scenarios?"
