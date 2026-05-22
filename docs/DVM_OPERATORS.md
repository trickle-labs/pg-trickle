# DVM Operators

This document describes the Differential View Maintenance (DVM) operators implemented by pgtrickle. Each operator transforms a stream of row-level changes (deltas) propagated from source tables through the operator tree.

## Quick Reference

| Operator | FULL | DIFF | IMMED | Section |
|----------|:----:|:----:|:-----:|---------|
| Simple `SELECT` / projection | ✅ | ✅ | ✅ | [Scan & Project](#scan--project) |
| `WHERE` filter | ✅ | ✅ | ✅ | [Filter](#filter) |
| `DISTINCT` | ✅ | ✅ | ✅ | [Distinct](#distinct) |
| `INNER JOIN` | ✅ | ✅ | ✅ | [Joins](#joins) |
| `LEFT / RIGHT OUTER JOIN` | ✅ | ✅ | ✅ | [Joins](#joins) |
| `FULL OUTER JOIN` | ✅ | ✅ | ✅ | [Joins](#joins) |
| `LATERAL JOIN` | ✅ | ✅ | ✅ | [Joins](#joins) |
| Multi-table join (≥3 right scans) | ✅ | ⚠️ | ⚠️ | [Joins](#joins) |
| `EXISTS` / `NOT EXISTS` | ✅ | ✅ | ✅ | [Subqueries](#subqueries) |
| Scalar subquery | ✅ | ✅ | ✅ | [Subqueries](#subqueries) |
| `UNION ALL` | ✅ | ✅ | ✅ | [Set Operations](#set-operations) |
| `INTERSECT` / `EXCEPT` | ✅ | ✅ | ✅ | [Set Operations](#set-operations) |
| `COUNT`, `SUM`, `AVG` | ✅ | ✅ | ✅ | [Aggregates](#aggregates) |
| `MIN` / `MAX` | ✅ | ✅ | ✅ | [Aggregates](#aggregates) |
| `COUNT(DISTINCT)` / `SUM(DISTINCT)` | ✅ | ✅ | ✅ | [Aggregates](#aggregates) |
| `STRING_AGG` / `ARRAY_AGG` | ✅ | ⚠️ | ⚠️ | [Aggregates](#aggregates) |
| `JSONB_AGG` / `JSONB_OBJECT_AGG` | ✅ | ⚠️ | ⚠️ | [Aggregates](#aggregates) |
| Window functions | ✅ | ⚠️ | ⚠️ | [Window Functions](#window-functions) |
| `ORDER BY … LIMIT` (TopK) | ✅ | ✅ | ✅ | [TopK](#topk) |
| `HAVING` | ✅ | ✅ | ✅ | [Having](#having) |
| `GROUP BY ROLLUP / CUBE` | ✅ | ✅ | ✅ | [Grouping Sets](#grouping-sets) |
| Recursive CTEs | ✅ | ⚠️ | ⚠️ | [CTEs](#ctes) |
| `vector_avg` / `halfvec_avg` | ✅ | ✅ | ✅ | [Vector Aggregates](#vector-aggregates) |

Legend: ✅ Supported · ⚠️ Partial (see section) · ❌ Not supported

---

## Prior Art

- Budiu, M. et al. (2023). "DBSP: Automatic Incremental View Maintenance." VLDB 2023. ([comparison](research/DBSP_COMPARISON.md))
- Gupta, A. & Mumick, I.S. (1999). *Materialized Views: Techniques, Implementations, and Applications.* MIT Press.
- Koch, C. et al. (2014). "DBToaster: Higher-order Delta Processing for Dynamic, Frequently Fresh Views." VLDB Journal.
- PostgreSQL 9.4+ — Materialized views with `REFRESH MATERIALIZED VIEW CONCURRENTLY`.

---

## Overview

When a stream table is created, the defining SQL query is parsed into a tree of **DVM operators**. During an differential refresh, changes flow bottom-up through this tree:

```
         Aggregate
            │
         Project
            │
          Filter
            │
    ┌───────┴───────┐
   Join             │
  ┌─┴─┐            │
Scan(A) Scan(B)   Scan(C)
```

Each operator implements a **differentiation rule**: given the delta (Δ) to its input(s), it produces the corresponding delta to its output. This is conceptually similar to automatic differentiation in calculus.

The general contract:

- Input: a set of `('+', row)` and `('-', row)` tuples (inserts and deletes)
- Output: a set of `('+', row)` and `('-', row)` tuples

Updates are modeled as a delete of the old row followed by an insert of the new row.

DIFFERENTIAL and IMMEDIATE maintenance require deterministic expressions. VOLATILE functions and custom operators such as `random()` or `clock_timestamp()` are rejected during stream table creation because re-evaluation would corrupt delta semantics. STABLE functions such as `now()` and `current_timestamp` are allowed with a warning; FULL mode accepts all volatility classes because it recomputes the full result on each refresh.

---

## Operator Support Matrix

The following table shows which SQL constructs are supported under each refresh mode.

| SQL Construct | FULL | DIFFERENTIAL | IMMEDIATE | Notes |
|---|:---:|:---:|:---:|---|
| **Basic** | | | | |
| Simple `SELECT` / projection | ✅ | ✅ | ✅ | |
| `WHERE` filter | ✅ | ✅ | ✅ | |
| Column expressions / aliases | ✅ | ✅ | ✅ | |
| `DISTINCT` | ✅ | ✅ | ✅ | Uses `__pgt_dup_count` reference counting |
| `DISTINCT ON` | ✅ | ✅ | ✅ | |
| **Joins** | | | | |
| `INNER JOIN` | ✅ | ✅ | ✅ | Hybrid delta strategy |
| `LEFT OUTER JOIN` | ✅ | ✅ | ✅ | NULL-padding transitions tracked |
| `RIGHT OUTER JOIN` | ✅ | ✅ | ✅ | |
| `FULL OUTER JOIN` | ✅ | ✅ | ✅ | 8-part UNION ALL delta |
| `CROSS JOIN` | ✅ | ✅ | ✅ | |
| `LATERAL JOIN` | ✅ | ✅ | ✅ | Row-scoped re-execution |
| Multi-table join (≤2 right scans) | ✅ | ✅ | ✅ | Full phantom-row-after-DELETE fix |
| Multi-table join (≥3 right scans) | ✅ | ⚠️ | ⚠️ | Falls back to post-change snapshot for right subtree (EC-01 boundary) |
| **Subqueries** | | | | |
| `EXISTS` / `IN` (semi-join) | ✅ | ✅ | ✅ | Delta-key pre-filter on left side |
| `NOT EXISTS` / `NOT IN` (anti-join) | ✅ | ✅ | ✅ | Inverted semantics; two-part delta |
| Scalar subquery (SELECT-list) | ✅ | ✅ | ✅ | Pre/post snapshot EXCEPT ALL diff |
| Correlated `LATERAL` subquery | ✅ | ✅ | ✅ | |
| **Set Operations** | | | | |
| `UNION ALL` | ✅ | ✅ | ✅ | Dual-branch merge |
| `INTERSECT` / `INTERSECT ALL` | ✅ | ✅ | ✅ | Dual-count tracking |
| `EXCEPT` / `EXCEPT ALL` | ✅ | ✅ | ✅ | |
| **Aggregates** | | | | |
| `COUNT`, `SUM`, `AVG` | ✅ | ✅ | ✅ | Algebraic — fully invertible delta |
| `MIN`, `MAX` | ✅ | ✅ | ✅ | Semi-algebraic — group rescan on ambiguous delete |
| `COUNT(DISTINCT)`, `SUM(DISTINCT)` | ✅ | ✅ | ✅ | Algebraic via auxiliary columns |
| `BOOL_AND`, `BOOL_OR`, `BIT_AND`, `BIT_OR` | ✅ | ✅ | ✅ | Algebraic via auxiliary columns |
| `EVERY` | ✅ | ✅ | ✅ | Algebraic via auxiliary columns |
| `STRING_AGG`, `ARRAY_AGG` | ✅ | ⚠️ | ⚠️ | Group-rescan strategy — warning emitted at creation time in DIFFERENTIAL mode |
| `STDDEV`, `VARIANCE`, `STDDEV_POP`, `VAR_POP` | ✅ | ✅ | ✅ | Algebraic via auxiliary M2/sum/count columns |
| `COVAR_SAMP`, `COVAR_POP`, `CORR` | ✅ | ✅ | ✅ | Algebraic via auxiliary columns |
| `REGR_*` (all 9 regression functions) | ✅ | ✅ | ✅ | Algebraic via auxiliary columns |
| `PERCENTILE_CONT`, `PERCENTILE_DISC` | ✅ | ⚠️ | ⚠️ | Group-rescan strategy |
| `MODE` | ✅ | ⚠️ | ⚠️ | Group-rescan strategy |
| `XMLAGG`, `JSON_AGG`, `JSONB_AGG` | ✅ | ⚠️ | ⚠️ | Group-rescan strategy |
| `JSON_OBJECT_AGG`, `JSONB_OBJECT_AGG` | ✅ | ⚠️ | ⚠️ | Group-rescan strategy |
| `GROUP BY` / `HAVING` | ✅ | ✅ | ✅ | |
| `GROUP BY ROLLUP` / `CUBE` / `GROUPING SETS` | ✅ | ✅ | ✅ | Branch count capped by `max_grouping_set_branches` (default 64) |
| **Window Functions** | | | | |
| `ROW_NUMBER`, `RANK`, `DENSE_RANK` | ✅ | ✅ | ✅ | Partition-scoped recompute |
| `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` | ✅ | ✅ | ✅ | Partition-scoped recompute |
| `NTILE`, `CUME_DIST`, `PERCENT_RANK` | ✅ | ✅ | ✅ | Partition-scoped recompute |
| Window `frame` clauses (`ROWS`, `RANGE`, `GROUPS`) | ✅ | ✅ | ✅ | |
| **CTEs** | | | | |
| Non-recursive `WITH` | ✅ | ✅ | ✅ | Inlined or delta-cached (multi-ref) |
| `WITH RECURSIVE` (INSERT-only workload) | ✅ | ✅ | ✅ | Semi-naive evaluation |
| `WITH RECURSIVE` (mixed INSERT/DELETE/UPDATE) | ✅ | ✅ | ✅ | Delete-and-Rederive (DRed) strategy |
| **TopK** | | | | |
| `ORDER BY … LIMIT N` | ✅ | ✅ | ✅ | Scoped recomputation; metadata validated each refresh |
| `ORDER BY … LIMIT N OFFSET M` | ✅ | ✅ | ✅ | |
| **Lateral / SRF** | | | | |
| `LATERAL` with set-returning function | ✅ | ✅ | ✅ | Row-scoped re-execution |
| `JSON_TABLE` | ✅ | ✅ | ✅ | Via lateral function operator |
| `generate_series()` | ✅ | ✅ | ✅ | |
| `unnest()` | ✅ | ✅ | ✅ | |
| **ST-to-ST Dependencies** | | | | |
| Stream table reading from another stream table | ✅ | ✅ | ✅ | Differential via `changes_pgt_` buffers; FULL upstream produces I/D diff so downstream stays differential |
| Multi-level ST chains | ✅ | ✅ | ✅ | Topological order; per-level delta propagation |
| **Function Volatility** | | | | |
| `IMMUTABLE` functions | ✅ | ✅ | ✅ | |
| `STABLE` functions (`now()`, `current_timestamp`) | ✅ | ⚠️ | ⚠️ | Allowed with warning — value may differ between initial load and delta evaluation |
| `VOLATILE` functions (`random()`, `clock_timestamp()`) | ✅ | ❌ | ❌ | Rejected at creation time — re-evaluation corrupts delta semantics |

**Legend:** ✅ = fully supported — ⚠️ = supported with caveats (see Notes column) — ❌ = not supported (blocked at creation time)

---

## Operators

### Scan

**Module:** `src/dvm/operators/scan.rs`

The leaf operator. Reads CDC changes from a source table's change buffer.

**Delta Rule:**

$$\Delta(\text{Scan}(R)) = \Delta R$$

The scan operator is a direct passthrough — inserts in the source become inserts in the output, deletes become deletes.

**SQL Generation:**
```sql
SELECT op, row_data FROM pgtrickle_changes.changes_<oid>
WHERE xid >= <last_consumed_xid>
```

**Notes:**
- Each source table has a dedicated change buffer table created by the CDC module.
- Row data is stored as JSONB with column names as keys.
- The `__pgt_row_id` column (xxHash of primary key) is included for deduplication.

---

### Filter

**Module:** `src/dvm/operators/filter.rs`

Applies a WHERE clause predicate to the delta stream.

**Delta Rule:**

$$\Delta(\sigma_p(R)) = \sigma_p(\Delta R)$$

Filtering is applied to the deltas in the same way as to the base data — only rows satisfying the predicate pass through.

**SQL Generation:**
```sql
SELECT * FROM (<input_delta>) AS d
WHERE <predicate>
```

**Example:**

If the defining query is:
```sql
SELECT * FROM orders WHERE status = 'shipped'
```

And a new row `(id=5, status='pending')` is inserted, it does **not** appear in the delta output. If `(id=3, status='shipped')` is inserted, it passes through.

**Edge Cases:**
- For updates that change the predicate column (e.g., `status` from `'pending'` to `'shipped'`), the CDC produces a delete of the old row and insert of the new row. The filter passes the insert (matches) and blocks the delete (doesn't match the old row against the predicate), correctly resulting in a net insert.

---

### Project

**Module:** `src/dvm/operators/project.rs`

Applies column projection from the target list.

**Delta Rule:**

$$\Delta(\pi_L(R)) = \pi_L(\Delta R)$$

Projects the same columns from the delta that the query projects from the base data.

**SQL Generation:**
```sql
SELECT <target_columns> FROM (<input_delta>) AS d
```

**Notes:**
- Projection is applied after filtering for efficiency.
- Computed expressions in the target list (e.g., `price * quantity AS total`) are evaluated on the delta rows.

---

### Join (Inner)

**Module:** `src/dvm/operators/join.rs`

Implements inner join between two inputs.

**Delta Rule:**

For $R \bowtie S$:

$$\Delta(R \bowtie S) = (\Delta R \bowtie S) \cup (R' \bowtie \Delta S)$$

Where $R' = R \cup \Delta R$ (the new state of R after applying deltas).

In practice, when only one side has changes (common case), the delta join simplifies to joining the changed rows against the current state of the other side.

**SQL Generation:**
```sql
-- Changes to left side joined with current right side
SELECT '+' AS op, l.*, r.*
FROM (<left_delta> WHERE op = '+') AS l
JOIN <right_table> AS r ON <join_condition>

UNION ALL

-- Current left side joined with changes to right side
SELECT '+' AS op, l.*, r.*
FROM <left_table> AS l
JOIN (<right_delta> WHERE op = '+') AS r ON <join_condition>
```

(And corresponding DELETE queries for `op = '-'`.)

**Notes:**
- The join uses the **current state** of the non-changed side, not the change buffer.
- For equi-joins, this is efficient — the join key narrows the scan.
- Non-equi joins (theta joins) may require broader scans.

---

### Outer Join

**Module:** `src/dvm/operators/outer_join.rs` (LEFT JOIN), `src/dvm/operators/full_join.rs` (FULL JOIN)

Implements LEFT, RIGHT, and FULL OUTER JOIN.

**RIGHT JOIN Handling:**

`RIGHT JOIN` is automatically converted to a `LEFT JOIN` with swapped left/right operands during query parsing. This normalization happens transparently — the user can write `RIGHT JOIN` and the parser rewrites it to an equivalent `LEFT JOIN` before the operator tree is constructed.

**Delta Rule:**

Similar to inner join, but additionally handles NULL-padded rows:

$$\Delta(R \text{ LEFT JOIN } S) = (\Delta R \bowtie_L S) \cup (R' \bowtie_L \Delta S)$$

With special handling for:
- Rows in ΔR that have no match in S → emit `('+', row, NULLs)`
- Rows in ΔS that create a first match for an R row → emit `('-', row, NULLs)` and `('+', row, s_data)`
- Rows in ΔS that remove the last match for an R row → emit `('-', row, s_data)` and `('+', row, NULLs)`

**SQL Generation (LEFT JOIN):**

Uses anti-join detection (via `NOT EXISTS`) to correctly handle the NULL padding transitions.

**FULL OUTER JOIN Delta Rule:**

FULL OUTER JOIN extends the LEFT JOIN delta with symmetric right-side handling. The delta is computed as an 8-part `UNION ALL`:

1. Parts 1–5: Same as LEFT JOIN delta (inserted/deleted rows from both sides, with NULL-padding transitions)
2. Parts 6–7: Symmetric anti-join transitions for the right side (rows in ΔL that remove/create the last/first match for an S row)
3. Part 8: Right-side insertions that have no match in the left side → emit `('+', NULLs, s_data)`

Each part uses pre-computed delta flags (`__has_ins_*`, `__has_del_*`) to efficiently detect first-match/last-match transitions without redundant subqueries.

**Nested Join Support:**

**Module:** `src/dvm/operators/join_common.rs`

All join operators (inner, left, full) support nested children — i.e., a join whose left or right operand is itself another join. The `join_common` module provides shared helpers:

- `build_snapshot_sql()` — returns the table reference for simple (Scan) operands, or a parenthesized subquery with disambiguated columns for nested join operands
- `rewrite_join_condition()` — rewrites column references in ON conditions to use the correct alias prefixes for nested children (e.g., `o.cust_id` → `dl.o__cust_id`)

This enables queries with 3 or more joined tables, e.g.:
```sql
SELECT o.id, c.name, p.title
FROM orders o
JOIN customers c ON o.cust_id = c.id
JOIN products p ON o.prod_id = p.id
```

**Limitations:**
- FULL OUTER JOIN delta computation can be expensive due to dual-side NULL tracking (8 UNION ALL parts).
- Performance degrades with high-cardinality join keys.
- `NATURAL JOIN` is supported — common columns are resolved automatically and synthesized into an explicit equi-join condition.
- **EC-01 pre-change snapshot boundary (SF-5):** The phantom-row-after-DELETE
  fix (EC-01) uses `EXCEPT ALL` to reconstruct the pre-change state of a join
  side. This is limited to join subtrees with **≤ 2 scan nodes** to avoid
  PostgreSQL temporary file exhaustion on wide join chains. For queries with
  ≥ 3 base tables on one side of a join (e.g. TPC-H Q7/Q8/Q9), a
  simultaneous DELETE on both join sides may leave a phantom row in the stream
  table until the next full refresh. See `use_pre_change_snapshot()` in
  `join_common.rs` for the full rationale.

---

### Aggregate

**Module:** `src/dvm/operators/aggregate.rs`

Handles GROUP BY with aggregate functions (COUNT, SUM, AVG, MIN, MAX, BOOL_AND, BOOL_OR, STRING_AGG, ARRAY_AGG, JSON_AGG, JSONB_AGG, BIT_AND, BIT_OR, BIT_XOR, JSON_OBJECT_AGG, JSONB_OBJECT_AGG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, MODE, PERCENTILE_CONT, PERCENTILE_DISC, JSON_ARRAYAGG, JSON_OBJECTAGG) and the `FILTER (WHERE …)` and `WITHIN GROUP (ORDER BY …)` clauses.

**Delta Rule:**

$$\Delta(\gamma_{G, \text{agg}}(R)) = \gamma_{G, \text{agg}}(R' \text{ WHERE } G \in \text{affected\_keys}) - \gamma_{G, \text{agg}}(R \text{ WHERE } G \in \text{affected\_keys})$$

Where:
- $G$ = grouping columns
- `affected_keys` = the set of group key values that appear in ΔR
- $R'$ = $R \cup \Delta R$ (the new state)

**Strategy:**

1. **Identify affected groups** — Collect all group key values that appear in the delta (either inserted or deleted rows).
2. **Recompute old values** — Query the storage table for current aggregate values of affected groups.
3. **Recompute new values** — Query the updated source for new aggregate values of affected groups.
4. **Diff** — For each affected group:
   - If old exists and new differs → emit `('-', old)` and `('+', new)`
   - If old exists and new is gone → emit `('-', old)` (group eliminated)
   - If no old and new exists → emit `('+', new)` (new group appeared)

**Supported Aggregate Functions:**

| Function | DVM Strategy | Notes |
|---|---|---|
| `COUNT(*)` | Algebraic | Fully differential |
| `COUNT(expr)` | Algebraic | Fully differential |
| `SUM(expr)` | Algebraic | Fully differential |
| `AVG(expr)` | Algebraic | Decomposed to SUM/COUNT internally |
| `MIN(expr)` | Semi-algebraic | Uses `LEAST` merge; falls back to per-group rescan when min row is deleted |
| `MAX(expr)` | Semi-algebraic | Uses `GREATEST` merge; falls back to per-group rescan when max row is deleted |
| `BOOL_AND(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `BOOL_OR(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `STRING_AGG(expr, sep)` | Group-rescan | Affected groups are re-aggregated from source data |
| `ARRAY_AGG(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `JSON_AGG(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `JSONB_AGG(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `BIT_AND(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `BIT_OR(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `BIT_XOR(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `JSON_OBJECT_AGG(key, value)` | Group-rescan | Affected groups are re-aggregated from source data |
| `JSONB_OBJECT_AGG(key, value)` | Group-rescan | Affected groups are re-aggregated from source data |
| `STDDEV_POP(expr)` / `STDDEV(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `STDDEV_SAMP(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `VAR_POP(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `VAR_SAMP(expr)` / `VARIANCE(expr)` | Group-rescan | Affected groups are re-aggregated from source data |
| `MODE() WITHIN GROUP (ORDER BY expr)` | Group-rescan | Ordered-set aggregate; affected groups re-aggregated |
| `PERCENTILE_CONT(frac) WITHIN GROUP (ORDER BY expr)` | Group-rescan | Ordered-set aggregate; affected groups re-aggregated |
| `PERCENTILE_DISC(frac) WITHIN GROUP (ORDER BY expr)` | Group-rescan | Ordered-set aggregate; affected groups re-aggregated |
| `CORR(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `COVAR_POP(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `COVAR_SAMP(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `REGR_AVGX(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `REGR_AVGY(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `REGR_COUNT(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `REGR_INTERCEPT(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `REGR_R2(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `REGR_SLOPE(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `REGR_SXX(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `REGR_SXY(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `REGR_SYY(Y, X)` | Group-rescan | Regression aggregate; affected groups re-aggregated |
| `ANY_VALUE(expr)` | Group-rescan | PostgreSQL 16+; affected groups re-aggregated |
| `JSON_ARRAYAGG(expr ...)` | Group-rescan | SQL-standard JSON aggregation (PostgreSQL 16+); full deparsed SQL preserved |
| `JSON_OBJECTAGG(key: value ...)` | Group-rescan | SQL-standard JSON aggregation (PostgreSQL 16+); full deparsed SQL preserved |
| User-defined aggregates (`CREATE AGGREGATE`) | Group-rescan | Any custom aggregate is supported via group-rescan; full aggregate call SQL preserved verbatim |

**FILTER Clause:**

All aggregate functions support the `FILTER (WHERE …)` clause:

```sql
SELECT COUNT(*) FILTER (WHERE status = 'active') AS active_count FROM orders GROUP BY region
```

The filter predicate is applied within the delta computation — only rows matching the filter contribute to the aggregate delta. Filtered aggregates are excluded from the P5 direct-bypass optimization.

**SQL Generation:**

The aggregate operator uses a 3-CTE pipeline:

1. **Merge CTE** — Joins affected group keys against old (storage) and new (source) aggregate values, producing `__pgt_meta_action` ('I' for new-only groups, 'D' for disappeared groups, 'U' for changed groups).
2. **LATERAL VALUES expansion** — A single-pass `LATERAL (VALUES ...)` clause expands each merge row into insert and delete actions, avoiding a 4-branch UNION ALL:

```sql
FROM merge_cte m,
LATERAL (VALUES
    ('I', m.new_count, m.new_total),
    ('D', m.old_count, m.old_total)
) v(action, count_val, val_total)
WHERE (m.__pgt_meta_action = 'I' AND v.action = 'I')
   OR (m.__pgt_meta_action = 'D' AND v.action = 'D')
   OR (m.__pgt_meta_action = 'U')
```

3. **Final projection** — Emits `('+', row)` and `('-', row)` tuples for the refresh engine.

**MIN/MAX Merge Strategy:**

`MIN` and `MAX` use a **semi-algebraic** strategy with two cases:

1. **Non-extremum deletion** — When the deleted row is NOT the current minimum (or maximum), the merge uses `LEAST(old_value, new_inserts)` for MIN or `GREATEST(old_value, new_inserts)` for MAX. This is fully algebraic and requires no rescan.

2. **Extremum deletion** — When the row holding the current minimum (or maximum) IS deleted, the new value cannot be computed from the delta alone. The merge expression returns `NULL` as a sentinel, which triggers the change-detection guard (`IS DISTINCT FROM`) to emit the group for re-aggregation. The MERGE layer treats this as a DELETE + INSERT pair, recomputing the group from source data. This is still more efficient than a full table refresh since only affected groups are rescanned.

---

### Distinct

**Module:** `src/dvm/operators/distinct.rs`

Implements `SELECT DISTINCT` using reference counting.

**Delta Rule:**

$$\Delta(\delta(R)) = \{ r \in \Delta R : \text{count}(r, R) = 0 \land \text{count}(r, R') > 0 \} - \{ r \in \Delta R : \text{count}(r, R) > 0 \land \text{count}(r, R') = 0 \}$$

In other words:
- A row enters the output when its count transitions from 0 to ≥1
- A row leaves the output when its count transitions from ≥1 to 0

**Strategy:**

Maintains a hidden `__pgt_dup_count` column in the storage table to track how many times each distinct row appears in the pre-distinct input.

1. On insert: increment count. If count was 0, emit `('+', row)`.
2. On delete: decrement count. If count becomes 0, emit `('-', row)`.

**Notes:**
- The duplicate count is not visible in user queries against the storage table (projected away by the view layer).
- Duplicate counting uses `__pgt_row_id` (xxHash) for efficient lookups.

---

### Union All

**Module:** `src/dvm/operators/union_all.rs`

Merges deltas from two branches.

**Delta Rule:**

$$\Delta(R \cup_{\text{all}} S) = \Delta R \cup_{\text{all}} \Delta S$$

Simply concatenates the delta streams from both branches.

**SQL Generation:**
```sql
SELECT * FROM (<left_delta>)
UNION ALL
SELECT * FROM (<right_delta>)
```

**Notes:**
- Column count and types must match between branches.
- Each branch is independently processed through its own operator sub-tree.
- This is the simplest operator since `UNION ALL` preserves all duplicates.

---

### Intersect

**Module:** `src/dvm/operators/intersect.rs`

Implements `INTERSECT` and `INTERSECT ALL` using dual-count per-branch multiplicity tracking.

**Delta Rule:**

$$\Delta(R \cap S): \text{emit rows where } \min(\text{count}_L, \text{count}_R) \text{ crosses the 0 boundary}$$

- **INTERSECT** (set): a row is present when both branches contain it.
- **INTERSECT ALL** (bag): a row appears $\min(\text{count}_L, \text{count}_R)$ times.

**SQL Generation (3-CTE chain):**

1. **Delta CTE** — tags rows from left/right child deltas with branch indicator (`'L'`/`'R'`) and computes per-row net_count.
2. **Merge CTE** — joins with the storage table to compute old and new per-branch counts (`__pgt_count_l`, `__pgt_count_r`).
3. **Final CTE** — detects boundary crossings using `LEAST(old_count_l, old_count_r)` vs `LEAST(new_count_l, new_count_r)`.

**Notes:**
- Storage table requires hidden columns `__pgt_count_l` and `__pgt_count_r` for multiplicity tracking.
- Both set and bag variants use the same 3-CTE structure; only the boundary logic stays the same (both use LEAST).

---

### Except

**Module:** `src/dvm/operators/except.rs`

Implements `EXCEPT` and `EXCEPT ALL` using dual-count per-branch multiplicity tracking.

**Delta Rule:**

$$\Delta(R - S): \text{emit rows where } \max(0, \text{count}_L - \text{count}_R) \text{ crosses the 0 boundary}$$

- **EXCEPT** (set): a row is present when it exists in the left but not the right branch.
- **EXCEPT ALL** (bag): a row appears $\max(0, \text{count}_L - \text{count}_R)$ times.

**SQL Generation (3-CTE chain):**

1. **Delta CTE** — same as Intersect: tags rows from both child deltas with branch indicator.
2. **Merge CTE** — joins with storage table for old/new per-branch counts.
3. **Final CTE** — detects boundary crossings using `GREATEST(0, old_count_l - old_count_r)` vs `GREATEST(0, new_count_l - new_count_r)`.

**Notes:**
- EXCEPT is **not** commutative — left branch is the positive input, right is subtracted.
- Storage table requires hidden columns `__pgt_count_l` and `__pgt_count_r`.
- Same 3-CTE structure as Intersect with different effective-count function.

---

### Subquery

**Module:** `src/dvm/operators/subquery.rs`

Handles both inlined CTEs and explicit subqueries in FROM (`(SELECT ...) AS alias`).

**Delta Rule:**

$$\Delta(\rho_{\text{alias}}(Q)) = \rho_{\text{alias}}(\Delta Q)$$

A subquery wrapper is transparent for differentiation — it delegates to its child's delta and optionally renames output columns to match the subquery's column aliases.

**SQL Generation:**
```sql
-- If column aliases differ from child output columns:
SELECT __pgt_row_id, __pgt_action, child_col1 AS alias_col1, child_col2 AS alias_col2
FROM (<child_delta>)
```

If the child columns already match the aliases, the subquery is a pure passthrough — no additional CTE is emitted.

**Notes:**
- This operator enables both CTE support (Tier 1) and standalone subqueries in FROM.
- Column aliases on subqueries (`FROM (...) AS x(a, b)`) are handled by emitting a thin renaming CTE.
- The subquery body is fully differentiated as a normal operator sub-tree.

---

### CTE Scan (Shared Delta)

**Module:** `src/dvm/operators/cte_scan.rs`

Handles multi-reference CTEs by computing the CTE body's delta once and reusing it across all references (Tier 2).

**Delta Rule:**

$$\Delta(\text{CteScan}(\text{id}, Q)) = \text{cache}[\text{id}] \quad \text{(computed once, reused)}$$

When a CTE is referenced multiple times in a query, each reference produces a `CteScan` node with the same `cte_id`. The diff engine differentiates the CTE body once and caches the result. Subsequent CteScan nodes for the same CTE reuse the cached delta.

**SQL Generation:**
```sql
-- First reference: differentiates the CTE body and stores result in cache
-- Subsequent references: point to the same system CTE name
SELECT __pgt_row_id, __pgt_action, <columns>
FROM __pgt_cte_<cte_name>_delta  -- shared across all references
```

If column aliases are present, a thin renaming CTE is added on top of the cached delta.

**Notes:**
- Without CteScan (Tier 1), multi-reference CTEs are inlined: each reference duplicates the full operator sub-tree. CteScan (Tier 2) eliminates this duplication.
- The CTE body is pre-differentiated in dependency order (earlier CTEs before later ones that reference them).
- Column alias support follows the same pattern as the Subquery operator.

---

### Recursive CTEs

Recursive CTEs (`WITH RECURSIVE`) are supported in FULL, DIFFERENTIAL, and IMMEDIATE modes, with different execution paths depending on the refresh mode:

#### FULL Mode

Recursive CTEs work out-of-the-box with `refresh_mode = 'FULL'`. The defining query is executed as-is via `INSERT INTO ... SELECT ...`, and PostgreSQL handles the iterative evaluation internally.

#### DIFFERENTIAL Mode (Three-Strategy Incremental Maintenance)

Recursive CTEs with `refresh_mode = 'DIFFERENTIAL'` use an automatic three-strategy approach, selected based on column compatibility and change type:

##### Strategy 1: Semi-Naive Evaluation (INSERT-only changes)

When only INSERT changes are present in the change buffer, pg_trickle uses **semi-naive evaluation** — the standard technique for incremental fixpoint computation. The base case is differentiated normally through the DVM operator tree, then the resulting delta is propagated through the recursive term using a nested `WITH RECURSIVE`:

```sql
WITH RECURSIVE
  __pgt_base_delta AS (
    -- Normal DVM differentiation of the base case (INSERT rows only)
    <differentiated base case>
  ),
  __pgt_rec_delta AS (
    -- Seed: base case delta rows
    SELECT cols FROM __pgt_base_delta WHERE __pgt_action = 'I'
    UNION ALL
    -- Seed: new base rows joining existing ST storage
    SELECT cols FROM <recursive term with self_ref = ST_storage, base = change_buffer>
    UNION ALL
    -- Propagation: recursive term applied to growing delta
    SELECT cols FROM <recursive term with self_ref = __pgt_rec_delta, base = full>
  )
SELECT pgtrickle.pg_trickle_hash(...) AS __pgt_row_id, 'I' AS __pgt_action, cols
FROM __pgt_rec_delta
```

The cost is proportional to the number of *new* rows produced by the change, not the full result set.

##### Strategy 2: Delete-and-Rederive / DRed (mixed INSERT/DELETE/UPDATE changes)

When the change buffer contains DELETE or UPDATE changes, simple propagation is insufficient — a deleted base row may have transitively derived many recursive rows, some of which may still be derivable from alternative paths. DRed handles this in four phases:

1. **Insert propagation** — semi-naive evaluation for the INSERT portion (same as Strategy 1)
2. **Over-deletion cascade** — propagate base-case deletions through the recursive term against ST storage to find all transitively-derived rows that *might* be invalidated
3. **Rederivation** — re-execute the recursive CTE from the remaining (non-deleted) base rows to restore any over-deleted rows that have alternative derivations
4. **Combine** — final delta = inserts + (over-deletions − rederived rows)

This avoids full recomputation while correctly handling deletions with alternative derivation paths.

#### IMMEDIATE Mode

Recursive CTEs with `refresh_mode = 'IMMEDIATE'` use the same semi-naive and
Delete-and-Rederive machinery as DIFFERENTIAL mode, but the base changes come
from PostgreSQL statement transition tables instead of the background change
buffer. This keeps the stream table transactionally up to date within the same
statement. To guard against cyclic data or unexpectedly deep recursion, the
semi-naive SQL injects a depth counter capped by
`pg_trickle.ivm_recursive_max_depth` (default `100`; set to `0` to disable the
guard).

##### Strategy 3: Recomputation Fallback

When the CTE defines more columns than the outer `SELECT` projects (column mismatch), the incremental strategies cannot be used because the ST storage table lacks columns needed for recursive self-joins. In this case, the full defining query is re-executed and anti-joined against current storage:

```sql
WITH __pgt_recomp_new AS (
    SELECT pgtrickle.pg_trickle_hash(row_to_json(sub)::text) AS __pgt_row_id, col1, col2, ...
    FROM (<defining_query>) sub
),
__pgt_recomp_ins AS (
    SELECT n.__pgt_row_id, 'I'::text AS __pgt_action, n.col1, n.col2, ...
    FROM __pgt_recomp_new n
    LEFT JOIN <storage_table> s ON s.__pgt_row_id = n.__pgt_row_id
    WHERE s.__pgt_row_id IS NULL
),
__pgt_recomp_del AS (
    SELECT s.__pgt_row_id, 'D'::text AS __pgt_action, s.col1, s.col2, ...
    FROM <storage_table> s
    LEFT JOIN __pgt_recomp_new n ON n.__pgt_row_id = s.__pgt_row_id
    WHERE n.__pgt_row_id IS NULL
)
SELECT * FROM __pgt_recomp_ins
UNION ALL
SELECT * FROM __pgt_recomp_del
```

The cost is proportional to the full result set size.

##### Strategy Selection

| CTE columns match ST? | Change type | `refresh_mode` / `DeltaSource` | Strategy |
|---|---|---|---|
| ✅ Match | INSERT-only | DIFFERENTIAL (ChangeBuffer) | Semi-naive (Strategy 1) |
| Match | Mixed (INSERT+DELETE/UPDATE) | DIFFERENTIAL (ChangeBuffer) | **DRed (Strategy 2)** |
| Match | INSERT-only | IMMEDIATE (TransitionTable) | Semi-naive (Strategy 1) |
| Match | Mixed (INSERT+DELETE/UPDATE) | IMMEDIATE (TransitionTable) | DRed (Strategy 2) |
| Mismatch | Any | Any | Recomputation (Strategy 3) |

> **DRed in DIFFERENTIAL mode (P2-1)**
>
> DRed is now active in both DIFFERENTIAL and IMMEDIATE modes when CTE output columns
> match ST storage columns. Phase 1 propagates inserts via semi-naive evaluation;
> Phase 2 cascades deletions through ST storage; Phase 3 rederives over-deleted rows
> that have alternative derivation paths; Phase 4 combines the results.
> DRed correctly handles derived-column changes such as path rebuilds under a renamed
> ancestor node. Column-mismatch cases still use recomputation fallback.

**Notes:**
- Non-linear recursion (multiple self-references in the recursive term) is rejected — PostgreSQL restricts the recursive term to reference the CTE at most once.
- The `__pgt_row_id` column (xxHash of the JSON-serialized row) is used for row identity.
- For write-heavy workloads on very large recursive result sets with frequent mixed changes, `refresh_mode = 'FULL'` may still be more efficient than DRed.

---

### Window Functions

**Module:** `src/dvm/operators/window.rs`

Handles window functions (`ROW_NUMBER`, `RANK`, `DENSE_RANK`, `SUM() OVER`, etc.) using partition-based recomputation.

**Delta Rule:**

When any row in a partition changes (insert, update, or delete), the entire partition's window function output is recomputed:

$$\Delta(\omega_{f, P}(R)) = \omega_{f, P}(R'|_{\text{affected partitions}}) - \omega_{f, P}(R|_{\text{affected partitions}})$$

Where $P$ is the PARTITION BY key and $f$ is the window function.

**Strategy:**

1. Identify affected partition keys from the child delta.
2. Delete old window function results for affected partitions from storage.
3. Build the current input for affected partitions by excluding changed rows via NOT EXISTS on pass-through columns.
4. Recompute the window function on the current input for affected partitions.
5. Compute unique row IDs via `row_to_json` + `row_number` (handles tied values in ranking functions).
6. Emit the recomputed rows as inserts.

**SQL Generation:**
```sql
-- CTE 1: Affected partition keys from delta
WITH affected_partitions AS (
    SELECT DISTINCT <partition_cols> FROM (<child_delta>)
),
-- CTE 2: Current input (surviving rows not in delta) for affected partitions
current_input AS (
    SELECT * FROM <child_snapshot>
    WHERE (<partition_cols>) IN (SELECT * FROM affected_partitions)
    AND NOT EXISTS (
        SELECT 1 FROM (<child_delta>) d
        WHERE d.<col1> IS NOT DISTINCT FROM <child_alias>.<col1>
        AND   d.<col2> IS NOT DISTINCT FROM <child_alias>.<col2> ...
    )
),
-- CTE 3: Recompute window function with unique row IDs
recomputed AS (
    SELECT *, pgtrickle.pg_trickle_hash(
        row_to_json(w)::text || '/' || row_number() OVER ()::text
    ) AS __pgt_row_id
    FROM (
        SELECT *, <window_func> OVER (PARTITION BY <partition_cols> ORDER BY <order_cols>) AS <alias>
        FROM current_input
    ) w
)
-- Delete old results + insert recomputed results
SELECT 'D' AS __pgt_action, ...  -- old rows from affected partitions
UNION ALL
SELECT 'I' AS __pgt_action, ...  -- recomputed rows
```

**Notes:**
- The cost is proportional to the size of affected partitions, not the full table. For workloads where changes spread across few partitions, this is efficient.
- When multiple window functions use different PARTITION BY clauses, the parser accepts all of them. If they share the same partition key it is used directly; otherwise the operator falls back to un-partitioned (full) recomputation.
- Without PARTITION BY, the entire table is treated as a single partition — any change triggers a full recomputation.
- Window functions wrapping aggregates (e.g., `RANK() OVER (ORDER BY SUM(x))`) are supported: the window diff rewrites ORDER BY / PARTITION BY expressions to reference aggregate output aliases via `build_agg_alias_map`.
- Row IDs are computed from the full row content (`row_to_json`) plus a positional disambiguator (`row_number`) to avoid hash collisions with tied ranking values (DENSE_RANK, RANK).

> **Known Limitation: O(partition_size) Recomputation Cost**
>
> Any single-row change within a window partition triggers recomputation of the *entire* partition.
> For queries with large partitions (e.g., `PARTITION BY region` where a region has 500K rows),
> a single INSERT into that partition causes all 500K rows to be recomputed and diffed. This is
> inherent to the partition-based delta strategy — window functions cannot be incrementally
> maintained at sub-partition granularity because a single row insertion can shift the rank,
> row number, or running aggregate of every other row in the same partition.
>
> **Mitigation strategies:**
> - Use more granular `PARTITION BY` keys to keep partition sizes small.
> - For queries without `PARTITION BY`, consider restructuring as a `GROUP BY` aggregate
>   if the window function is equivalent (e.g., `SUM(x) OVER ()` → `SUM(x)` as a scalar subquery).
> - Accept the cost for low-change-frequency partitions; the recomputation is still
>   cheaper than a full table refresh since only *affected* partitions are touched.
> - If partition sizes routinely exceed 100K rows and changes are frequent, consider
>   the FULL refresh mode which bypasses the per-partition delta entirely.

**Window Frame Clauses:**

Window frame specifications are fully supported:
- **Modes:** `ROWS`, `RANGE`, `GROUPS`
- **Bounds:** `UNBOUNDED PRECEDING`, `N PRECEDING`, `CURRENT ROW`, `N FOLLOWING`, `UNBOUNDED FOLLOWING`
- **Between syntax:** `BETWEEN <start> AND <end>`
- **Exclusion:** `EXCLUDE CURRENT ROW`, `EXCLUDE GROUP`, `EXCLUDE TIES`, `EXCLUDE NO OTHERS`

Example: `SUM(val) OVER (ORDER BY ts ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)`

**Named WINDOW Clauses:**

Named window definitions are resolved from the query-level `WINDOW` clause:

```sql
SELECT id, SUM(val) OVER w, AVG(val) OVER w
FROM data
WINDOW w AS (PARTITION BY category ORDER BY ts)
```

The parser resolves `OVER w` by looking up the window definition from the `WINDOW` clause and merging partition, order, and frame specifications.

---

### Lateral Function (Set-Returning Functions in FROM)

**Module:** `src/dvm/operators/lateral_function.rs`

Handles set-returning functions (SRFs) used in the FROM clause with implicit LATERAL semantics: `jsonb_array_elements`, `jsonb_each`, `jsonb_each_text`, `unnest`, etc.

**Delta Rule:**

When a source row changes (insert, update, or delete), the SRF expansion is re-evaluated only for that source row:

$$\Delta(R \ltimes f(R.\text{col})) = (R' \ltimes f(R'.\text{col}))|_{\text{changed rows}} - (R \ltimes f(R.\text{col}))|_{\text{changed rows}}$$

Where $R$ is the source table, $f$ is the SRF, and changed rows are identified via the child delta.

**Strategy (Row-Scoped Recomputation):**

1. Propagate the child delta to identify changed source rows.
2. Find all existing ST rows derived from changed source rows (via column matching).
3. Delete old SRF expansions for those source rows.
4. Re-expand the SRF for inserted/updated source rows.
5. Emit deletes + inserts as the final delta.

**SQL Generation (4-CTE chain):**

```sql
-- CTE 1: Changed source rows from child delta
WITH lat_changed AS (
    SELECT DISTINCT "__pgt_row_id", "__pgt_action", <child_cols>
    FROM <child_delta>
),
-- CTE 2: Old ST rows for changed source rows (to be deleted)
lat_old AS (
    SELECT st."__pgt_row_id", st.<all_output_cols>
    FROM <st_table> st
    WHERE EXISTS (
        SELECT 1 FROM lat_changed cs
        WHERE st.<col1> IS NOT DISTINCT FROM cs.<col1>
          AND st.<col2> IS NOT DISTINCT FROM cs.<col2>
          ...
    )
),
-- CTE 3: Re-expand SRF for inserted/updated source rows
lat_expand AS (
    SELECT pg_trickle_hash(<all_cols>::text) AS "__pgt_row_id",
           cs.<child_cols>, <srf_alias>.<srf_cols>
    FROM lat_changed cs,
         LATERAL <srf_function>(cs.<arg>) AS <srf_alias>
    WHERE cs."__pgt_action" = 'I'
),
-- CTE 4: Final delta
lat_final AS (
    SELECT "__pgt_row_id", 'D' AS "__pgt_action", <cols> FROM lat_old
    UNION ALL
    SELECT "__pgt_row_id", 'I' AS "__pgt_action", <cols> FROM lat_expand
)
```

**Row Identity:**

Content-based: `hash(child_columns || srf_result_columns)`. This is stable as long as the same source row produces the same expanded values.

**Supported SRFs:**

| Function | Output Columns | Notes |
|----------|---------------|-------|
| `jsonb_array_elements(jsonb)` | `value` (jsonb) | Expands JSONB array to rows |
| `jsonb_array_elements_text(jsonb)` | `value` (text) | Text variant |
| `jsonb_each(jsonb)` | `key` (text), `value` (jsonb) | Expands JSONB object to key-value pairs |
| `jsonb_each_text(jsonb)` | `key` (text), `value` (text) | Text variant |
| `unnest(anyarray)` | Element type | Unnests PostgreSQL arrays |
| Custom SRFs | User-provided column aliases | `AS alias(col1, col2)` |

**Notes:**
- The cost is proportional to the number of changed source rows × average SRF expansion size, not the full table.
- `WITH ORDINALITY` is supported — adds a `bigint` ordinality column to the output.
- `ROWS FROM()` with multiple functions is not supported (rejected at parse time).
- Column aliases (e.g., `AS child(value)`) are used to determine output column names; for known SRFs without aliases, the alias name becomes the column name.
- **JSON_TABLE** (PostgreSQL 17+) — `JSON_TABLE(expr, path COLUMNS (...))` is modeled as a `LateralFunction` and uses the same row-scoped recomputation strategy. Supported column types: regular, EXISTS, formatted, and nested columns with `ON ERROR`/`ON EMPTY` behaviors and `PASSING` clauses.

---

### Lateral Subquery (Correlated Subqueries in FROM)

**Module:** `src/dvm/operators/lateral_subquery.rs`

Handles correlated subqueries used in the FROM clause with explicit or implicit LATERAL semantics: `FROM t, LATERAL (SELECT ... WHERE ref = t.col) AS alias` or `FROM t LEFT JOIN LATERAL (...) AS alias ON true`.

**Delta Rule:**

When an outer row changes, the correlated subquery is re-executed only for that row:

$$\Delta(R \ltimes Q(R)) = (R' \ltimes Q(R'))|_{\text{changed rows}} - (R \ltimes Q(R))|_{\text{changed rows}}$$

Where $R$ is the outer table, $Q(R)$ is the correlated subquery, and changed rows are identified via the child delta.

**Strategy (Row-Scoped Recomputation):**

1. Propagate the child delta to identify changed outer rows.
2. Find all existing ST rows derived from changed outer rows (via column matching with `IS NOT DISTINCT FROM`).
3. Delete old subquery expansions for those outer rows.
4. Re-execute the subquery for inserted/updated outer rows using the original outer alias.
5. Emit deletes + inserts as the final delta.

**SQL Generation (4-CTE chain):**

```sql
-- CTE 1: Changed outer rows from child delta
WITH lat_sq_changed AS (
    SELECT DISTINCT "__pgt_row_id", "__pgt_action", <child_cols>
    FROM <child_delta>
),
-- CTE 2: Old ST rows for changed outer rows (to be deleted)
lat_sq_old AS (
    SELECT st."__pgt_row_id", st.<all_output_cols>
    FROM <st_table> st
    WHERE EXISTS (
        SELECT 1 FROM lat_sq_changed cs
        WHERE st.<col1> IS NOT DISTINCT FROM cs.<col1>
          AND st.<col2> IS NOT DISTINCT FROM cs.<col2>
          ...
    )
),
-- CTE 3: Re-execute subquery for inserted/updated outer rows
lat_sq_expand AS (
    SELECT pg_trickle_hash(<all_cols>::text) AS "__pgt_row_id",
           <outer_alias>.<child_cols>, <sub_alias>.<sub_cols>
    FROM lat_sq_changed AS <outer_alias>,      -- Original outer alias!
         LATERAL (<subquery_sql>) AS <sub_alias>
    WHERE <outer_alias>."__pgt_action" = 'I'
),
-- CTE 4: Final delta
lat_sq_final AS (
    SELECT "__pgt_row_id", 'D' AS "__pgt_action", <cols> FROM lat_sq_old
    UNION ALL
    SELECT "__pgt_row_id", 'I' AS "__pgt_action", <cols> FROM lat_sq_expand
)
```

**LEFT JOIN LATERAL Handling:**

For queries using `LEFT JOIN LATERAL (...) ON true`, the expand CTE uses `LEFT JOIN LATERAL` instead of comma syntax and wraps subquery columns in `COALESCE` for hash stability:

```sql
lat_sq_expand AS (
    SELECT pg_trickle_hash(<outer_cols>::text || '/' || COALESCE(<sub_cols>::text, '')) AS "__pgt_row_id",
           <outer_alias>.<child_cols>, <sub_alias>.<sub_cols>
    FROM lat_sq_changed AS <outer_alias>
    LEFT JOIN LATERAL (<subquery_sql>) AS <sub_alias> ON true
    WHERE <outer_alias>."__pgt_action" = 'I'
)
```

**Row Identity:**

Content-based: `hash(outer_columns || '/' || subquery_result_columns)`. For LEFT JOIN with NULL results, `COALESCE` ensures a stable hash.

**Supported Patterns:**

| Pattern | Syntax | Notes |
|---------|--------|-------|
| **Top-N per group** | `LATERAL (SELECT ... ORDER BY ... LIMIT N)` | Most common use case |
| **Correlated aggregate** | `LATERAL (SELECT SUM(x) FROM t WHERE t.fk = p.pk)` | Returns single row per outer row |
| **Existence with data** | `LEFT JOIN LATERAL (...) ON true` | Preserves outer rows with NULLs |
| **Multi-column lookup** | `LATERAL (SELECT a, b FROM t WHERE t.fk = p.pk LIMIT 1)` | Multiple derived values |
| **GROUP BY inside subquery** | `LATERAL (SELECT type, COUNT(*) FROM t WHERE t.fk = p.pk GROUP BY type)` | Multiple rows per outer row |

**Key Design Decision: Outer Alias Rewriting**

The subquery body contains column references to the outer table (e.g., `WHERE li.order_id = o.id`). In the expansion CTE, the changed-sources CTE is aliased with the **original outer table alias** (e.g., `lat_sq_changed AS o`) so that the subquery's column references resolve naturally without rewriting.

**Notes:**
- The cost is proportional to the number of changed outer rows × average subquery result size, not the full table.
- The subquery is stored as raw SQL (like `LateralFunction`) because it cannot be independently differentiated — it depends on outer row context.
- Source table OIDs referenced by the subquery body are extracted at parse time for CDC trigger setup.
- ORDER BY + LIMIT inside the subquery are valid (they apply per-outer-row, not to the stream table).

---

### Semi-Join (EXISTS / IN Subquery)

**Module:** `src/dvm/operators/semi_join.rs`

Handles `WHERE EXISTS (SELECT ... FROM ...)` and `WHERE col IN (SELECT ...)` patterns. The parser transforms these into a `SemiJoin` operator with a left (outer) child, a right (inner) child, and a join condition.

**Delta Rule:**

$$\Delta(L \ltimes R) = \Delta L|_{R} + L|_{\Delta R \text{ causes existence change}}$$

- Part 1: Outer rows that changed and still satisfy the semi-join condition.
- Part 2: Existing outer rows whose semi-join result flipped due to inner changes (a matching inner row was inserted or deleted).

**Strategy (Two-Part Delta):**

1. **Part 1 (outer delta):** Filter `delta_left` to rows that have at least one match in the current right-hand snapshot.
2. **Part 2 (inner delta):** For each row in the left snapshot, check whether the existence of matching right-hand rows changed between the old and current state. Emit `'I'` if a match appeared, `'D'` if all matches disappeared.

The "old" right-hand state is reconstructed from the current state by reversing the delta: `R_old = (R_current EXCEPT ALL delta_right(action='I')) UNION ALL delta_right(action='D')`.

**Row Identity:**

- Part 1: Uses `__pgt_row_id` from the left delta.
- Part 2: Content-based hash via `pg_trickle_hash_multi` on left-side columns.

**Supported Patterns:**

| Pattern | SQL | Notes |
|---------|-----|-------|
| `EXISTS` | `WHERE EXISTS (SELECT 1 FROM t WHERE t.fk = s.pk)` | Direct semi-join |
| `IN (subquery)` | `WHERE id IN (SELECT fk FROM t)` | Rewritten to `EXISTS` with equality |
| Multiple conditions | `WHERE EXISTS (... AND ...)` | Additional predicates in subquery WHERE |

---

### Anti-Join (NOT EXISTS / NOT IN Subquery)

**Module:** `src/dvm/operators/anti_join.rs`

Handles `WHERE NOT EXISTS (SELECT ... FROM ...)` and `WHERE col NOT IN (SELECT ...)` patterns. The inverse of the semi-join operator.

**Delta Rule:**

$$\Delta(L \triangleright R) = \Delta L|_{\neg R} + L|_{\Delta R \text{ causes existence change}}$$

- Part 1: Outer rows that changed and have no match in the right-hand snapshot.
- Part 2: Existing outer rows whose anti-join result flipped due to inner changes.

**Strategy (Two-Part Delta):**

1. **Part 1 (outer delta):** Filter `delta_left` to rows with `NOT EXISTS` in the current right snapshot.
2. **Part 2 (inner delta):** For each row in the left snapshot, detect existence changes. Emit `'D'` if a match appeared (row no longer qualifies), `'I'` if all matches disappeared (row now qualifies).

Note the inverted semantics compared to semi-join: a new match means deletion, losing all matches means insertion.

**Row Identity:** Same as semi-join.

**Supported Patterns:**

| Pattern | SQL | Notes |
|---------|-----|-------|
| `NOT EXISTS` | `WHERE NOT EXISTS (SELECT 1 FROM t WHERE t.fk = s.pk)` | Direct anti-join |
| `NOT IN (subquery)` | `WHERE id NOT IN (SELECT fk FROM t)` | Rewritten to `NOT EXISTS` with equality |

---

### Scalar Subquery (Correlated SELECT Subquery)

**Module:** `src/dvm/operators/scalar_subquery.rs`

Handles scalar subqueries appearing in the `SELECT` list, e.g., `SELECT a, (SELECT max(x) FROM t) AS mx FROM s`. The subquery must return exactly one row and one column.

**Delta Rule:**

$$\Delta(L \times q) = \Delta L \times q' + L \times (q' - q)$$

Where $q$ is the scalar subquery value and $q'$ is the updated value.

**Strategy (Two-Part Delta):**

1. **Part 1 (outer delta):** Propagate the child delta, appending the current scalar subquery value to each row.
2. **Part 2 (scalar value change):** When the scalar subquery's result changes, emit deletes for all existing outer rows (with the old scalar value) and re-inserts for all outer rows (with the new value). The old scalar value is reconstructed by reversing the inner delta.

**SQL Generation (3 or 4 CTEs):**

```sql
-- Part 1: child delta + current scalar value
WITH sq_outer AS (
    SELECT *, (<scalar_subquery>) AS "<alias>"
    FROM <child_delta>
),
-- Part 2a: DELETE all outer rows when scalar changed
sq_del AS (
    SELECT "__pgt_row_id", 'D' AS "__pgt_action", <cols>
    FROM <st_table>
    WHERE (<scalar_old>) IS DISTINCT FROM (<scalar_current>)
),
-- Part 2b: INSERT all outer rows with new scalar value
sq_ins AS (
    SELECT pg_trickle_hash_multi(...) AS "__pgt_row_id",
           'I' AS "__pgt_action", <cols>, (<scalar_current>) AS "<alias>"
    FROM <source_snapshot>
    WHERE (<scalar_old>) IS DISTINCT FROM (<scalar_current>)
)
-- Final: UNION ALL of all parts
SELECT * FROM sq_outer
UNION ALL SELECT * FROM sq_del
UNION ALL SELECT * FROM sq_ins
```

**Row Identity:**

- Part 1: `__pgt_row_id` from the child delta.
- Part 2: Content-based hash via `pg_trickle_hash_multi` on all output columns.

**Notes:**
- The scalar subquery is stored as raw SQL (deparsed from the parse tree).
- The old scalar value is approximated using the same `EXCEPT ALL / UNION ALL` reversal technique as semi/anti-join.
- If the scalar subquery references a table that changes, all outer rows must be re-evaluated — the delta can be large.
- Source OIDs used by the scalar subquery are captured at parse time for CDC trigger registration.

---

## Operator Tree Construction

The DVM engine builds the operator tree by analyzing the parsed query:

1. **WITH clause** → CTE definitions extracted into a name→body map (non-recursive) or CTE registry (multi-reference)
2. **FROM clause** → `Scan` nodes for physical tables; `Subquery` nodes for inlined CTEs and subqueries in FROM; `CteScan` nodes for multi-reference CTEs; `LateralFunction` nodes for SRFs and JSON_TABLE in FROM; `LateralSubquery` nodes for correlated subqueries in FROM
3. **JOIN** → `Join` or `OuterJoin` wrapping two sub-trees
4. **LATERAL SRFs** → `LateralFunction` wrapping the left-hand FROM item as its child
5. **LATERAL subqueries** → `LateralSubquery` wrapping the left-hand FROM item as its child (comma syntax or JOIN LATERAL)
5. **WHERE subqueries** → `SemiJoin` for `EXISTS`/`IN (subquery)`, `AntiJoin` for `NOT EXISTS`/`NOT IN (subquery)`, extracted from the WHERE clause
5. **Scalar subqueries** → `ScalarSubquery` for `(SELECT ...)` in the SELECT list, wrapping the child tree
5. **WHERE** → `Filter` wrapping the scan/join tree (remaining non-subquery predicates)
5. **SELECT list** → `Project` for column selection and expressions
6. **GROUP BY** → `Aggregate` wrapping the filtered/projected tree
7. **DISTINCT** → `Distinct` on top
8. **UNION ALL** → `UnionAll` combining two complete sub-trees
9. **INTERSECT / EXCEPT** → `Intersect` or `Except` combining two sub-trees with dual-count tracking
10. **Window functions** → `Window` wrapping the sub-tree with PARTITION BY / ORDER BY metadata
11. **ORDER BY** → silently discarded (storage row order is undefined)
12. **LIMIT / OFFSET** → `ORDER BY + LIMIT [+ OFFSET]` is accepted as TopK (scoped recomputation); standalone `LIMIT` or `OFFSET` without `ORDER BY` is rejected

For recursive CTEs (`WITH RECURSIVE`), the query is parsed into an OpTree with `RecursiveCte` operator nodes. In DIFFERENTIAL mode, the strategy (semi-naive, DRed, or recomputation) is selected automatically based on column compatibility and change type — see the Recursive CTEs section above for details.

The tree is then traversed bottom-up during delta generation: each operator's `generate_delta_sql()` method composes its SQL fragment around the output of its child operator(s).

---

## LATERAL Joins and DIFFERENTIAL Mode (FEAT-2)

The following table summarises which LATERAL patterns are supported in
`DIFFERENTIAL` mode and what behaviour to expect for each:

| LATERAL pattern | DIFFERENTIAL support | Notes |
|----------------|---------------------|-------|
| `LATERAL` SRF (set-returning function, e.g. `jsonb_array_elements`) | ✅ Full | Delta applied via `LateralFunction` operator |
| `LATERAL` subquery with simple correlated filter | ✅ Full | `LateralSubquery` operator; correlated columns from outer row |
| `, LATERAL (SELECT … WHERE ref = t.col)` comma syntax | ✅ Full | Rewritten to inner join semantics internally |
| `LEFT JOIN LATERAL (…) ON true` | ✅ Full (with EC-01 semantics) | Outer row preserved when subquery returns no rows; R₀ snapshot used for left side |
| `LEFT JOIN LATERAL` with outer-applied correlated aggregate | ⚠️ Supported with caveat | Aggregate re-evaluated over R₀ snapshot; see note below |
| `LATERAL` referencing a volatile SRF | ❌ Falls back to FULL | Detected at parse time via volatility check; `UnsupportedOperator` hint emitted |
| Nested `LATERAL` (LATERAL inside LATERAL) | ❌ Falls back to FULL | Not yet supported in delta rules |

### EC-01 Semantics for LEFT JOIN LATERAL

When the outer side of a `LEFT JOIN LATERAL` changes (e.g., a row is updated
that previously had a match in the lateral subquery), the DVM engine applies
the EC-01 snapshot-splitting rule:

1. The pre-update outer row is joined against the *current* (pre-delta) lateral
   result → generates a deletion delta.
2. The post-update outer row is joined against the *post-delta* lateral
   result → generates an insertion delta.

This ensures that phantom NULL-padded output rows from the previous refresh
cycle are correctly cancelled before the new rows are inserted.

### Correlated Aggregate Note

For patterns like:

```sql
SELECT p.id, lat.total
FROM products p
LEFT JOIN LATERAL (
    SELECT SUM(quantity) AS total
    FROM order_items oi
    WHERE oi.product_id = p.id
) lat ON true
```

The `total` aggregate is re-evaluated over the R₀ snapshot of `order_items`
for every outer row affected by the delta.  This is correct but involves a
full sub-scan of `order_items` for each changed `p.id`.  For large datasets
with many product changes per cycle, consider materialising the aggregate as a
separate stream table and joining against it instead:

```sql
-- Preferred for high-cardinality aggregates:
SELECT pgt_order_totals.product_id, pgt_order_totals.total
FROM pgt_order_totals   -- a separate stream table over order_items
```

---

## Further Reading

- [ARCHITECTURE.md](ARCHITECTURE.md) — System-wide component overview
- [SQL_REFERENCE.md](SQL_REFERENCE.md) — Complete function reference
- [CONFIGURATION.md](CONFIGURATION.md) — GUC tuning guide
