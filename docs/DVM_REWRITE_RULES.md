# DVM SQL Rewrite Rules

This document describes the transformation pipeline in
`src/dvm/parser/rewrites.rs` that prepares a defining query for
differentiation by the DVM (Differential View Maintenance) engine.

Each rewrite pass targets a specific SQL pattern, transforms it into a
form the DVM engine can differentiate, and has a formal algebraic
correctness argument.

---

## Rewrite Pipeline Order

The rewrite passes are applied in sequence. Each pass may be iterated
until a fixed point (no further changes) is reached.

String-rewrite pipeline (applied in this exact order by `run_query_rewrite_pipeline`):

1. **View Inlining** (`rewrite_views_inline`) — Replace view references with their definitions
2. **Nested Window Expression Lift** (`rewrite_nested_window_exprs`) — Lift window functions wrapped in expressions to an inner subquery
3. **DISTINCT ON Rewrite** (`rewrite_distinct_on`) — Convert `SELECT DISTINCT ON` to `ROW_NUMBER()` window subquery
4. **Grouping Sets Expansion** (`rewrite_grouping_sets`) — Expand CUBE/ROLLUP into UNION ALL
5. **Scalar Sublink in WHERE** (`rewrite_scalar_subquery_in_where`) — Hoist non-correlated scalar subqueries in WHERE to a CROSS JOIN CTE
6. **Correlated Scalar in SELECT** (`rewrite_correlated_scalar_in_select`) — Lift correlated scalar subqueries in SELECT list to LEFT JOINs
7. **De Morgan Normalisation** (`rewrite_demorgan_sublinks`) — Expose OR+sublink patterns hidden under NOT (repeated with step 8 up to 3 times)
8. **Sublinks-in-OR Expansion** (`rewrite_sublinks_in_or`) — Split OR conditions containing sublinks into UNION branches
9. **ROWS FROM Rewrite** (`rewrite_rows_from`) — Convert multi-function `ROWS FROM(...)` into a LATERAL join chain

Parse-time / differentiation-time operations (not string rewrites):

10. **EXISTS → Anti/Semi-Join** — Convert correlated EXISTS to `OpTree::SemiJoin` / `OpTree::AntiJoin` nodes during query parsing
11. **Multi-Partition Window Split** (`rewrite_multi_partition_windows`) — Split window functions with different PARTITION BY into separate subqueries (pre-parse utility)
12. **Delta Key Restriction** (DI-6) — Push join key filters into R_old snapshots at differentiation time

---

## 1. View Inlining (`rewrite_views_inline`)

**Input Pattern:** `SELECT ... FROM my_view v WHERE ...`

**Transformation:** Replace `my_view` with its `pg_get_viewdef()` body
as a subquery: `SELECT ... FROM (SELECT ... FROM base_tables) v WHERE ...`

**Correctness:** A view is semantically equivalent to its definition.
Inlining is required because the DVM engine needs to see the base
tables to generate per-table change buffer references.

**Before:**
```sql
-- Defining query referencing a view
SELECT o.customer_id, SUM(o.amount) AS total
FROM order_summary_view o
GROUP BY o.customer_id
```

**After:**
```sql
-- View inlined; base tables are now visible for CDC binding
SELECT o.customer_id, SUM(o.amount) AS total
FROM (
    SELECT orders.customer_id,
           orders.amount,
           orders.created_at
    FROM public.orders
    WHERE orders.status = 'completed'
) o
GROUP BY o.customer_id
```

The inlined form allows the DVM engine to bind `orders` as the CDC source
and generate delta SQL that reads from `pgtrickle_changes.changes_<orders_oid>`
instead of the whole table.

---

## 2. Grouping Sets Expansion (`rewrite_grouping_sets`)

**Input Pattern:** `SELECT ... GROUP BY CUBE(a, b)` or `GROUP BY ROLLUP(a, b)`

**Transformation:** Expand into a `UNION ALL` of individual `GROUP BY`
combinations. CUBE(a, b) → GROUP BY (a, b) UNION ALL GROUP BY (a)
UNION ALL GROUP BY (b) UNION ALL GROUP BY ().

**Correctness:** CUBE/ROLLUP is algebraically equivalent to the union of
all grouping combinations. The DVM engine differentiates each branch
independently, and the UNION ALL operator merges the deltas.

**Guard:** `pg_trickle.max_grouping_set_branches` (default 64) limits
explosion for high-dimensional CUBE expressions.

**Before:**
```sql
-- ROLLUP over region + product_type
SELECT region, product_type, SUM(revenue) AS total
FROM sales
GROUP BY ROLLUP(region, product_type)
```

**After:**
```sql
-- Expanded to three GROUP BY branches
SELECT region, product_type, SUM(revenue) AS total
FROM sales
GROUP BY region, product_type

UNION ALL

SELECT region, NULL AS product_type, SUM(revenue) AS total
FROM sales
GROUP BY region

UNION ALL

SELECT NULL AS region, NULL AS product_type, SUM(revenue) AS total
FROM sales
```

Each branch is an independent leaf node in the `OpTree`. The DVM engine
differentiates each branch by computing delta rows from the change buffer,
then merges the results via the UNION ALL parent node.

---

## 3. EXISTS → Anti/Semi-Join Conversion

**Input Pattern:**
```sql
SELECT ... FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.key = t1.key)
SELECT ... FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.key = t1.key)
```

**Transformation:** Convert to `OpTree::SemiJoin` or `OpTree::AntiJoin`
with the extracted condition as the join predicate.

**Correctness:** `EXISTS (correlated subquery)` is equivalent to a
semi-join; `NOT EXISTS` is equivalent to an anti-join. The DVM engine
has specialized delta operators for both.

---

## 4. Scalar Sublink Hoisting (`rewrite_scalar_subquery_in_where` / `rewrite_correlated_scalar_in_select`)

> **Note:** This section describes both scalar-subquery rewrite passes together. See also the dedicated
> sections for [Scalar Sublink in WHERE](#5-scalar-sublink-in-where-rewrite_scalar_subquery_in_where)
> and [Correlated Scalar in SELECT](#6-correlated-scalar-in-select-rewrite_correlated_scalar_in_select).

**Input Pattern:** Scalar subqueries in SELECT or WHERE:
```sql
SELECT a, (SELECT max(b) FROM t2 WHERE t2.key = t1.key) FROM t1
```

**Transformation:** Hoist the scalar subquery to a CTE and replace
with a reference:
```sql
WITH __pgt_scalar_1 AS (SELECT key, max(b) AS val FROM t2 GROUP BY key)
SELECT a, s.val FROM t1 LEFT JOIN __pgt_scalar_1 s ON s.key = t1.key
```

**Correctness:** A correlated scalar subquery is equivalent to a
left join to its grouped equivalent. The CTE form allows the DVM engine
to differentiate the subquery as a separate operator node.

---

## 5. Delta Key Restriction (DI-6)

**Input Pattern:** Anti-join / semi-join R_old snapshots that scan the
full right table.

**Transformation:** Push equi-join key filters from the delta into the
R_old snapshot to restrict it to only the changed keys.

**Correctness:** Only right-side rows matching changed keys can affect
the anti/semi-join output. Restricting R_old to changed keys preserves
correctness while reducing the scan from O(n) to O(Δ).

**Before:**
```sql
-- Anti-join delta: which left rows lost their right-side match?
-- R_old scans ALL of the right table (O(n))
SELECT l.*
FROM left_table l
WHERE NOT EXISTS (
    SELECT 1 FROM right_table r_old WHERE r_old.key = l.key
)
AND EXISTS (
    SELECT 1 FROM delta_right d WHERE d.key = l.key
)
```

**After:**
```sql
-- R_old restricted to only rows matching changed keys (O(Δ))
SELECT l.*
FROM left_table l
WHERE NOT EXISTS (
    SELECT 1 FROM right_table r_old
    WHERE r_old.key = l.key
      AND r_old.key IN (SELECT key FROM delta_right)  -- <-- restriction added
)
AND EXISTS (
    SELECT 1 FROM delta_right d WHERE d.key = l.key
)
```

This rewrite is critical for join-heavy queries: without it, every
anti-join delta scan reads the full right table regardless of how many
rows actually changed.

---

## 6. Scalar Sublink Hoisting in WHERE (`rewrite_scalar_subquery_in_where`)

**Input Pattern:** Scalar subqueries used as predicates in WHERE:
```sql
SELECT * FROM orders o WHERE o.amount > (SELECT avg(amount) FROM orders)
```

**Transformation:** Hoist the scalar subquery to a CTE and replace with a
join reference:
```sql
WITH __pgt_scalar_1 AS (SELECT avg(amount) AS __pgt_val FROM orders)
SELECT o.* FROM orders o
CROSS JOIN __pgt_scalar_1
WHERE o.amount > __pgt_scalar_1.__pgt_val
```

**Correctness:** A non-correlated scalar subquery in WHERE is equivalent to
a cross-joined constant. The CTE form allows the DVM engine to differentiate
the outer query as a filter over a stable value.

---

## 7. DISTINCT ON Rewrite (`rewrite_distinct_on`)

**Input Pattern:** `SELECT DISTINCT ON (expr_list) ...`
```sql
SELECT DISTINCT ON (region) region, amount FROM orders ORDER BY region, amount DESC
```

**Transformation:** Rewrite using `ROW_NUMBER()` OVER (PARTITION BY) inside
a subquery, keeping only rows where the row number equals 1:
```sql
SELECT region, amount FROM (
  SELECT region, amount,
         ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS __pgt_rn
  FROM orders
) __pgt_do WHERE __pgt_rn = 1
```

**Correctness:** `DISTINCT ON` is equivalent to the top-1 row per partition.
`ROW_NUMBER()` is a supported window function with a known differential. This
pass also rewrites `DISTINCT ON` inside CTE bodies.

---

## 8. Correlated Scalar in SELECT (`rewrite_correlated_scalar_in_select`)

**Input Pattern:** Correlated scalar subqueries in the SELECT list:
```sql
SELECT d.name,
       (SELECT MAX(e.salary) FROM emp e WHERE e.dept_id = d.id) AS max_sal
FROM dept d
```

**Transformation:** Hoist each correlated scalar to a LEFT JOIN subquery:
```sql
SELECT d.name, __pgt_sq_1.__pgt_scalar_1 AS max_sal
FROM dept d
LEFT JOIN (
    SELECT e.dept_id AS __pgt_corr_key_1, MAX(e.salary) AS __pgt_scalar_1
    FROM emp e GROUP BY e.dept_id
) AS __pgt_sq_1 ON d.id = __pgt_sq_1.__pgt_corr_key_1
```

**Correctness:** A correlated scalar subquery returning at most one row per
outer row is equivalent to a left join with grouping. The left join form
allows the DVM engine to differentiate using the standard join delta rules.
Non-correlated scalar subqueries are left untouched (handled by the
`ScalarSubquery` OpTree node).

---

## 9. De Morgan Normalisation (`rewrite_demorgan_sublinks`)

**Input Pattern:** Sublinks hidden under NOT-AND/NOT-OR:
```sql
SELECT * FROM t
WHERE NOT (x AND NOT EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id))
```

**Transformation:** Apply De Morgan's law to surface the sublink:
- `NOT (a AND b)` → `(NOT a) OR (NOT b)`
- `NOT (a OR b)` → `(NOT a) AND (NOT b)`
- `NOT NOT a` → `a`

```sql
SELECT * FROM t
WHERE (NOT x) OR EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id)
```

**Correctness:** De Morgan's law is a boolean identity. This pass runs before
`rewrite_sublinks_in_or` to expose OR+sublink patterns that the subsequent
UNION rewrite can handle.

---

## 10. Sublinks-in-OR Expansion (`rewrite_sublinks_in_or`)

**Input Pattern:** Sublinks (EXISTS/IN) inside OR conditions:
```sql
SELECT * FROM t WHERE status = 'active' OR EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id)
```

**Transformation:** Split each OR arm into a separate branch of a UNION:
```sql
SELECT * FROM t WHERE status = 'active'
UNION
SELECT t.* FROM t WHERE EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id)
```

**Correctness:** `A OR B` is equivalent to `A UNION B` (with deduplication).
Each UNION branch is an independent query that the DVM engine can differentiate
separately. UNION (not UNION ALL) handles rows matching multiple OR arms.

---

## 11. ROWS FROM Rewrite (`rewrite_rows_from`)

**Input Pattern:** PostgreSQL `ROWS FROM(f1(...), f2(...), ...)` multi-function syntax:
```sql
SELECT * FROM ROWS FROM (generate_series(1, 3), unnest(ARRAY['a', 'b', 'c']))
  AS t(n, v)
```

**Transformation:**
- **All-unnest optimisation:** when every function is `unnest`, merge into a
  single multi-argument `unnest(A, B, ...)` call.
- **General case:** rewrite to an ordinal-based LEFT JOIN LATERAL chain using
  `generate_series WITH ORDINALITY` as the driving series, LEFT JOIN LATERAL
  each SRF with an `ON ord = ord` predicate.

**Correctness:** `ROWS FROM` is PostgreSQL-specific syntax for zip-joining
multiple set-returning functions. The ordinal-join rewrite preserves the
NULL-padding semantics of unequal-length SRFs.

---

## 12. Multi-Partition Window Split (`rewrite_multi_partition_windows`)

**Input Pattern:** Window functions using different PARTITION BY clauses in
the same SELECT:
```sql
SELECT id, region, dept,
       SUM(amount) OVER (PARTITION BY region) AS region_sum,
       SUM(amount) OVER (PARTITION BY dept)   AS dept_sum
FROM orders
```

**Transformation:** Split into separate subqueries — one per distinct
PARTITION BY group — joined by an internal row marker:
```sql
SELECT __pgt_w1.id, __pgt_w1.region, __pgt_w1.dept,
       __pgt_w1.region_sum, __pgt_w2.dept_sum
FROM (
  SELECT id, region, dept, amount,
         SUM(amount) OVER (PARTITION BY region) AS region_sum
  FROM orders
) __pgt_w1
JOIN (
  SELECT id, region, dept, amount,
         SUM(amount) OVER (PARTITION BY dept) AS dept_sum
  FROM orders
) __pgt_w2 ON __pgt_w1.__pgt_row_marker = __pgt_w2.__pgt_row_marker
```

**Correctness:** Each subquery computes a self-contained window partition.
Because window functions are non-distributing aggregates, splitting by
PARTITION BY is correct: each subquery sees all rows needed for its partition.

---

## 13. Nested Window Expression Lift (`rewrite_nested_window_exprs`)

**Input Pattern:** Window functions wrapped inside expressions in the SELECT
list:
```sql
SELECT ABS(ROW_NUMBER() OVER (ORDER BY score) - 5) AS dist FROM t
```

**Transformation:** Lift window functions to an inner subquery and reference
them by alias in the outer SELECT:
```sql
SELECT "abs"("__pgt_wf_inner"."__pgt_wf_1" - 5) AS "dist"
FROM (
  SELECT *, ROW_NUMBER() OVER (ORDER BY score) AS "__pgt_wf_1"
  FROM t
) "__pgt_wf_inner"
```

**Correctness:** The window function computes the same result whether it
appears inline or as a subquery column. The lift is safe when there is no
GROUP BY (interaction between GROUP BY and window functions is excluded).

---

## Adding New Rewrite Passes

To add a new rewrite pass:

1. Add the function in `src/dvm/parser/rewrites.rs`
2. Add unit tests asserting the expected SQL output for a reference input
3. Insert the pass at the correct position in the pipeline
4. Document the pass in this file with input pattern, transformation,
   and correctness argument

---

## See Also

- [docs/DVM_OPERATORS.md](DVM_OPERATORS.md) — Per-operator differentiation rules
- [docs/PERFORMANCE_COOKBOOK.md](PERFORMANCE_COOKBOOK.md) — Performance tuning
- [src/dvm/parser/rewrites.rs](https://github.com/trickle-labs/pg-trickle/blob/main/src/dvm/parser/rewrites.rs) — Implementation
