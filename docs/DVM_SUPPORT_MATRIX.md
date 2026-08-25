# DVM Support Matrix

<!-- DOC-2 (v0.80.0): Complete reference for DVM query-pattern support, fallback behaviour,
     IMMEDIATE restrictions, and known-unsupported forms. -->

This document describes which SQL query patterns pg_trickle can maintain
incrementally via **Differential View Maintenance (DVM)**, which patterns
trigger an automatic fallback to FULL refresh, and which are blocked entirely
in IMMEDIATE mode.

---

## Table of Contents

1. [How DVM works (quick overview)](#how-dvm-works)
2. [Supported patterns](#supported-patterns)
3. [Fallback behaviour and reason codes](#fallback-behaviour-and-reason-codes)
4. [IMMEDIATE-mode restrictions](#immediate-mode-restrictions)
5. [Known-unsupported forms (q12 / q20)](#known-unsupported-forms)
6. [Pattern quick-reference table](#pattern-quick-reference-table)

---

## How DVM works

The DVM engine parses a stream table's defining query into an **OpTree** (an
algebraic operator tree), then generates a *delta SQL* template that computes
only the incremental change caused by new/updated/deleted rows in the CDC
buffer.  For each source table the engine produces:

- **Part 1** — rows to *remove* from the materialized result
- **Part 2** — rows to *add* to the materialized result

These parts are applied with a single `MERGE` statement.  When the engine
cannot safely compute a delta (e.g. the query uses a pattern with known
delta-algebra limitations), it falls back to a FULL recompute.

---

## Supported patterns

### Simple scan / filter

```sql
SELECT id, name, amount FROM orders WHERE status = 'OPEN'
```

- Full INSERT/UPDATE/DELETE incremental support.
- `refresh_reason` column: never set (always differential).

### Aggregates (GROUP BY)

```sql
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
```

- Supported via GROUP_RESCAN strategy (EXCEPT ALL on the group key).
- INSERT and DELETE: safe.
- UPDATE on a non-group-key column: safe.
- Update that changes the **group key itself**: triggers GROUP_RESCAN.

### Joins (inner, left, right, full)

```sql
SELECT o.id, c.name, o.amount
FROM orders o
JOIN customers c ON o.customer_id = c.id
```

- Up to `max_differential_joins` source tables (default: 8).
- Each join type (INNER, LEFT, RIGHT, FULL OUTER) is supported.
- Delta SQL generates L₀ snapshot CTEs for pre-change state of each leaf.

### Semi-join / anti-join (EXISTS / NOT EXISTS)

```sql
SELECT o.id FROM orders o
WHERE EXISTS (SELECT 1 FROM flagged f WHERE f.order_id = o.id)
```

- Supported.  DVM generates a separate `NOT EXISTS` delta branch.

### DISTINCT

```sql
SELECT DISTINCT customer_id FROM orders
```

- Supported via reference-counting strategy (distinct_ref_count).

### Window functions

```sql
SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at)
FROM orders
```

- **DIFFERENTIAL path**: not supported — falls back to GROUP_RESCAN.
- In GROUP_RESCAN mode the full group is recomputed for every changed row.
- `refresh_reason` column: never set (GROUP_RESCAN is the normal path, no explicit reason code).

### CTEs (WITH clause)

```sql
WITH ranked AS (
  SELECT *, RANK() OVER (ORDER BY amount DESC) AS rnk FROM orders
)
SELECT id, rnk FROM ranked WHERE rnk <= 10
```

- Non-recursive CTEs: supported as inlined (NOT MATERIALIZED) sub-queries.
- Recursive CTEs: fallback to FULL refresh with reason `recursive_cte_fallback`.

### Recursive CTEs

```sql
WITH RECURSIVE tree AS (
  SELECT id, parent_id FROM categories WHERE parent_id IS NULL
  UNION ALL
  SELECT c.id, c.parent_id FROM categories c JOIN tree t ON c.parent_id = t.id
)
SELECT * FROM tree
```

- **Not differentially maintainable**.
- Always falls back to FULL refresh.
- `refresh_reason`: `recursive_cte_fallback`

### UNION ALL

```sql
SELECT id, amount FROM domestic_orders
UNION ALL
SELECT id, amount FROM foreign_orders
```

- Supported. Each branch is differentiated independently.

### INTERSECT / EXCEPT

`INTERSECT`, `INTERSECT ALL`, `EXCEPT`, and `EXCEPT ALL` are materialized
directly in FULL mode. Their branch multiplicity state is not exposed in the
stream table. AUTO selects FULL; explicit DIFFERENTIAL and IMMEDIATE requests
are rejected until durable private state and mutation-by-mutation parity are
proven.

### Lateral joins

```sql
SELECT o.id, latest.event
FROM orders o
CROSS JOIN LATERAL (
  SELECT event FROM order_events e WHERE e.order_id = o.id
  ORDER BY created_at DESC LIMIT 1
) latest
```

- Supported for proven row-scoped forms with exact outer identity. AUTO falls
  back to FULL when identity/dependency inspection is incomplete. IMMEDIATE
  rejects mutable inner sources because transition-table coverage is not yet
  proven.

### Scalar subqueries (non-correlated)

```sql
SELECT id, amount,
       amount / (SELECT SUM(amount) FROM orders) AS pct
FROM orders
```

- Supported if the scalar subquery is not correlated (no outer reference).

### Volatility

Only IMMUTABLE expressions are admitted to incremental maintenance by default.
STABLE and VOLATILE expressions use FULL under AUTO and are rejected for
explicit DIFFERENTIAL or IMMEDIATE mode. PostgreSQL analysis resolves the exact
function, operator, and cast overload before pg_trickle checks its volatility.

---

## Fallback behaviour and reason codes

When the DVM engine detects a query pattern that cannot be safely maintained
incrementally, it sets a `refresh_reason` code in `pgt_refresh_history` and
falls back to a FULL refresh.  The `pgtrickle.health_check()` function emits
a `WARN` for any such code in the last hour.

| Reason code | Trigger condition | Version introduced |
|-------------|-------------------|--------------------|
| `CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK` | `SUM/COUNT(CASE…)` aggregate with an `IN (…)` literal list predicate **and** a mutable source (UPDATE possible). The delta algebra mis-counts CASE-condition flips via UPDATE. | v0.78.0 |
| `CORRELATED_SUBQUERY_DELTA_QUADRATIC` | Correlated aggregate scalar subquery in `WHERE` (e.g. `col > (SELECT SUM(…) FROM t WHERE t.k = outer.k)`) that cannot be safely pre-aggregated into a CTE. Produces O(delta × table) delta SQL. | v0.78.0 |
| `REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN` | `CASE` expression inside an aggregate contains a scalar subquery or `EXISTS` predicate — the string-based classifier cannot confirm DVM algebraic delta safety. | v0.80.0 |
| `recursive_cte_fallback` | `WITH RECURSIVE` — the recomputation strategy is always used for recursive queries. | v0.38.0 |
| `predicted_cost_exceeds_full` | The scheduler's adaptive cost model predicts the differential path would be more expensive than a FULL recompute (large delta, high cost factor). | v0.45.0 |

### Remediating a fallback

1. **`CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK`**  
   If the source table is append-only (no UPDATEs), set `is_append_only = true`:
   ```sql
   SELECT pgtrickle.alter_stream_table('myschema', 'my_st',
     is_append_only => true);
   ```
   Otherwise, rewrite the query to avoid the pattern or use FULL mode permanently.

2. **`CORRELATED_SUBQUERY_DELTA_QUADRATIC`**  
   Rewrite the correlated subquery as a pre-aggregated CTE:
   ```sql
   -- Instead of:
   SELECT * FROM orders o
   WHERE o.amount > (SELECT SUM(amount) FROM orders WHERE customer_id = o.customer_id)
   -- Use:
   WITH cust_total AS (
     SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY 1
   )
   SELECT o.* FROM orders o JOIN cust_total t ON o.customer_id = t.customer_id
   WHERE o.amount > t.total
   ```

3. **`REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN`**  
   Simplify the CASE predicate — remove subqueries or EXISTS from inside the CASE condition.

4. **`recursive_cte_fallback`**  
   Use FULL mode explicitly: no workaround — recursive queries are not differentially maintainable.

---

## IMMEDIATE-mode restrictions

IMMEDIATE mode refreshes the stream table *within the same transaction* that
caused the source change, before the transaction commits.  This imposes strict
restrictions on what the defining query can do:

| Pattern | Allowed in IMMEDIATE mode? | Notes |
|---------|---------------------------|-------|
| Simple scan / filter | ✅ Yes | |
| Aggregate (GROUP BY) | ✅ Yes | GROUP_RESCAN allowed |
| Inner/left join | ✅ Yes | Single-transaction lock safe |
| DISTINCT | ✅ Yes | |
| Non-correlated scalar subquery | ✅ Yes | |
| Lateral join | ⚠️ Limited | Only if lateral body is a simple scan |
| Window functions | ⚠️ Limited | GROUP_RESCAN allowed; large window may exceed lock timeout |
| Recursive CTE | ❌ Blocked | Causes lock acquisition cycle risk |
| Cross-database references | ❌ Blocked | SPI cannot span databases in one transaction |
| Long-running aggregate (cost > threshold) | ❌ Blocked | May exceed `lock_timeout` |

IMMEDIATE mode uses `FOR KEY SHARE` locking on scanned source tables to
prevent phantom reads.  If the lock cannot be acquired within the configured
timeout, the extension automatically downgrades to SCHEDULED mode and emits
an `ivm_lock_parse_error_count` increment.

---

## Known-unsupported forms

### q12-like: CASE aggregate with IN-list predicate (mutable source)

Pattern (from TPC-H Q12):
```sql
SELECT l_shipmode,
       SUM(CASE WHEN o_orderpriority IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS high_prio,
       SUM(CASE WHEN o_orderpriority NOT IN ('1-URGENT', '2-HIGH') THEN 1 ELSE 0 END) AS low_prio
FROM orders JOIN lineitem ON o_orderkey = l_orderkey
WHERE l_shipmode IN ('MAIL', 'SHIP')
GROUP BY l_shipmode
```

**Why it fails**: When an UPDATE changes the `o_orderpriority` column, the
CASE expression evaluates to different values for the 'D' (delete) and 'I'
(insert) sides of the delta.  The delta algebra double-counts or miscounts the
CASE-condition flip (DI-8).

**Workaround**: If the source is append-only, set `is_append_only = true`.  
Otherwise, use FULL refresh mode permanently.

**Reason code**: `CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK`

---

### q20-like: Correlated aggregate scalar subquery in WHERE

Pattern (from TPC-H Q20):
```sql
SELECT s_name, s_address FROM supplier
WHERE s_suppkey IN (
  SELECT ps_suppkey FROM partsupp
  WHERE ps_availqty > (
    SELECT 0.5 * SUM(l_quantity)
    FROM lineitem
    WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
      AND l_shipdate BETWEEN '1994-01-01' AND '1994-12-31'
  )
)
```

**Why it fails**: The inner `(SELECT 0.5 * SUM(l_quantity) … WHERE l_partkey = ps_partkey)` is
a correlated aggregate subquery evaluated once per outer row.  The DVM delta
SQL would re-evaluate this for every changed `partsupp` row, producing
O(delta × lineitem) work.

**Workaround**: Rewrite using a pre-aggregated CTE (see
[Remediating a fallback](#remediating-a-fallback) above).

**Reason code**: `CORRELATED_SUBQUERY_DELTA_QUADRATIC`

---

## Pattern quick-reference table

| SQL pattern | DVM path | Fallback | Reason code |
|-------------|----------|----------|-------------|
| Simple scan | DIFF | — | — |
| Filter (WHERE) | DIFF | — | — |
| Aggregate (GROUP BY) | DIFF / GROUP_RESCAN | — | — |
| Inner join | DIFF | — | — |
| Left / right / full join | DIFF | — | — |
| Semi-join (EXISTS) | DIFF | — | — |
| Anti-join (NOT EXISTS) | DIFF | — | — |
| DISTINCT | DIFF | — | — |
| Non-recursive CTE | DIFF (inlined) | — | — |
| Recursive CTE | FULL | Always | `recursive_cte_fallback` |
| UNION ALL | DIFF | — | — |
| Lateral join | DIFF (snapshot) | — | — |
| Window functions | GROUP_RESCAN | — | — |
| Non-correlated scalar subquery | DIFF | — | — |
| SUM/COUNT(CASE) + IN-list + mutable | FULL | On UPDATE | `CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK` |
| Correlated aggregate subquery in WHERE | FULL (if CTE rewrite fails) | Always | `CORRELATED_SUBQUERY_DELTA_QUADRATIC` |
| CASE aggregate with EXISTS / subquery inside | FULL | Always | `REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN` |

> **Legend**: DIFF = full incremental differential path; GROUP_RESCAN = aggregate
> group is recomputed for changed groups; FULL = full recompute of the entire
> result set.
