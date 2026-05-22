# pg_trickle Limitations

This document covers what pg_trickle cannot do, the constraints of
DIFFERENTIAL mode, source table restrictions, and operational anti-patterns.
Use the decision tree at the end to quickly determine if your use case is
supported.

---

## Unsupported SQL Constructs

The following SQL features are **not supported** in the defining query of a
stream table. Attempting to use them will produce an `UnsupportedOperator`
error at creation time, or at the first refresh attempt.

### In DIFFERENTIAL mode only

These constructs force a fallback to FULL refresh. The stream table is created
successfully, but every refresh performs a full table recomputation:

| Construct | Reason | Workaround |
|-----------|--------|------------|
| `ORDER BY` without `LIMIT` | Result ordering is non-deterministic as a delta | Remove `ORDER BY`, or use `ORDER BY ... LIMIT N` for Top-N views |
| `TABLESAMPLE` | Non-deterministic sampling cannot be differentiated | Use `FULL` mode explicitly |
| `VOLATILE` functions in SELECT | `random()`, `now()`, `clock_timestamp()`, `nextval()` change on every call | Pre-compute volatile values in the source table, or use `FULL` mode |
| `STABLE` functions in GROUP BY keys | Key values change between refresh cycles | Use stable or immutable functions only |
| Window functions in output | `ROW_NUMBER()`, `RANK()`, `LEAD()`, `LAG()` require global reordering | Use `FULL` mode, or pre-aggregate and use Top-N views |
| `FETCH FIRST` without `ORDER BY` | Non-deterministic selection | Add a deterministic `ORDER BY` and use Top-N via `LIMIT` |
| `GROUPING SETS` beyond branch limit | Explosion prevents O(Δ) maintenance | Reduce dimensions, or raise `pg_trickle.max_grouping_set_branches` |

### Supported with constraints

| Construct | Support | Notes |
|-----------|---------|-------|
| `WITH RECURSIVE` | ✅ Supported | DIFFERENTIAL and IMMEDIATE mode use bounded semi-naive evaluation for monotone insert-only changes, DRed for mixed changes, and full recomputation fallback when needed. Guarded by `pg_trickle.ivm_recursive_max_depth`. |

### Not supported at all (any mode)

| Construct | Reason |
|-----------|--------|
| DDL inside the defining query | `CREATE TABLE`, `CALL` etc. are not valid in `SELECT` |
| `RETURNING` clauses | Not applicable to `SELECT` queries |
| `FOR UPDATE` / `FOR SHARE` | Locking hints cannot be used in defining queries |
| Subqueries with side effects | `INSERT ... RETURNING` in subqueries |
| `pg_catalog` internal tables as sources | Internal catalog tables are not tracked by CDC |
| Temp tables as sources | Temporary tables are session-scoped; CDC triggers cannot be installed |

---

## DIFFERENTIAL Mode Constraints

### Source table requirements

For DIFFERENTIAL mode to work correctly, each source table must:

1. **Have a primary key or unique index** on the columns used as join keys.
   Without a reliable row identity, the MERGE step cannot match old and new
   versions of a row. pg_trickle can fall back to a hash-based row ID
   (`__pgt_row_id`) for sources without primary keys, but this adds overhead.

2. **Not use UNLOGGED or TEMPORARY storage** for the stream table output.
   The stream table must survive a crash-recovery cycle. Source tables can be
   UNLOGGED (changes are still captured by triggers).

3. **Not be altered concurrently** in ways that change column structure while a
   refresh is running. pg_trickle blocks source DDL by default
   (`pg_trickle.block_source_ddl = true`). Disabling this risks schema
   inconsistency between the change buffer and the stream table.

### Multi-source join constraints

When a defining query joins multiple source tables:

- **All join keys must be equi-joins** (e.g., `t1.id = t2.id`). Range joins
  (`t1.ts BETWEEN t2.start AND t2.end`) force FULL mode.
- **The number of delta CTEs grows with the number of sources.** Queries
  joining 5+ large tables may hit the `pg_trickle.max_diff_ctes` limit.
  The default limit is 200; raise it or simplify the query.
- **Left outer joins with nullable right-side keys** add correctness complexity.
  pg_trickle handles them correctly, but the delta SQL is larger.

### Aggregate constraints

| Aggregate | Supported? | Notes |
|-----------|-----------|-------|
| `COUNT(*)` | ✅ Yes | Fully algebraic |
| `SUM(x)` | ✅ Yes | Fully algebraic |
| `MIN(x)`, `MAX(x)` | ✅ Yes | With reference counting |
| `AVG(x)` | ✅ Yes | Via sum + count decomposition |
| `STDDEV(x)`, `VARIANCE(x)` | ✅ Yes | Via sum-of-squares decomposition |
| `COUNT(DISTINCT x)` | ✅ Yes | Via Z-set algebraic counting |
| `ARRAY_AGG(x)` | ❌ No | Order-dependent; use FULL mode |
| `STRING_AGG(x, sep)` | ❌ No | Order-dependent; use FULL mode |
| `JSON_AGG(x)` | ❌ No | Order-dependent; use FULL mode |
| `PERCENTILE_CONT(f) WITHIN GROUP (ORDER BY x)` | ❌ No | Requires global sort |
| `MODE()` | ❌ No | Requires global frequency computation |
| Custom user-defined aggregates | ⚠️ Maybe | Supported if the aggregate provides `sfunc` + `finalfunc` that pg_trickle can decompose; marked `STRICT` aggregates are rejected |

---

## Source Table Restrictions

### Supported source types

| Source type | Supported? | Notes |
|-------------|-----------|-------|
| Regular heap tables | ✅ Yes | Full CDC support |
| Partitioned tables (declarative) | ✅ Yes | Triggers installed on each partition |
| Foreign tables (postgres_fdw) | ✅ Yes | Snapshot-comparison mode |
| Materialized views | ✅ Yes | Snapshot-comparison mode |
| Other stream tables | ✅ Yes | DAG chaining supported |
| UNLOGGED tables | ✅ Yes (source) | Changes captured; stream table output must be logged |
| Temporary tables | ❌ No | Session-scoped; CDC triggers cannot persist |
| System catalogs (`pg_class`, etc.) | ❌ No | Not tracked by CDC |
| Views (non-materialized) | ❌ No | Automatically inlined; the base tables become the sources |
| Remote tables via dblink | ⚠️ Limited | Use foreign tables via postgres_fdw instead |

### Column type constraints

- **`text`-typed columns named as join keys** work, but are less efficient than
  integer or UUID keys. Use an index on the join key columns.
- **`jsonb` columns in GROUP BY** are supported but hash joins on JSONB are
  expensive. Consider extracting the key sub-field.
- **`bytea` columns** work in the output but cannot be used as GROUP BY keys
  in DIFFERENTIAL mode.

---

## Operational Anti-Patterns

### Anti-pattern 1: Very high write rates with low schedules

**Problem:** If a source table receives 100K inserts/second and the stream
table schedule is 1 second, the change buffer accumulates 100K rows per cycle.
The DIFFERENTIAL delta SQL must process all 100K rows on every refresh, which
may take longer than 1 second — causing the scheduler to fall behind.

**Fix:**
- Increase the schedule to allow batching: `schedule => '10s'`
- Enable the adaptive fallback: `pg_trickle.differential_max_change_ratio = 0.15`
  (default: fall back to FULL when > 15% of the source table changed)
- Use `pg_trickle.max_delta_estimate_rows` to cap delta size

### Anti-pattern 2: Unbounded DAG depth

**Problem:** A chain of 20+ stream tables where each depends on the previous
creates O(depth) sequential refresh latency on every cycle.

**Fix:** Flatten the DAG where possible. Use parallel refresh
(`pg_trickle.parallel_refresh_mode = 'on'`) for independent branches.
Consider whether intermediate stream tables are necessary.

### Anti-pattern 3: Schema changes on live sources

**Problem:** `ALTER TABLE ... DROP COLUMN` on a source table while pg_trickle
is running will break the change buffer schema and cause refresh errors.

**Fix:** Keep `pg_trickle.block_source_ddl = true` (the default). This causes
schema changes to fail with a descriptive error; you can then update the stream
table query explicitly before re-applying the schema change.

### Anti-pattern 4: Treating stream tables as application write targets

**Problem:** Inserting or updating rows directly in a stream table bypasses
pg_trickle's refresh logic. On the next refresh, the direct writes will be
overwritten.

**Fix:** Stream tables are **read-only from the application's perspective**.
All writes must go through the source tables. Use the
`pgtrickle.repair_stream_table()` function if a stream table gets into an
inconsistent state.

### Anti-pattern 5: Using `pg_trickle.enabled = false` in production

**Problem:** Setting `pg_trickle.enabled = false` globally stops all refreshes.
Change buffers accumulate indefinitely. Re-enabling causes a burst refresh of
all stream tables simultaneously.

**Fix:** Use `pgtrickle.suspend_stream_table()` to pause individual stream
tables, or `pg_trickle.drain_mode = true` to stop new work while completing
in-flight refreshes.

---

## "Will This Work?" Decision Tree

```
Does your query use window functions in the output?
  YES → Use FULL mode (refresh_mode => 'FULL')
  NO  ↓

Does your query use volatile functions (random(), now(), nextval())?
  YES → Use FULL mode, or pre-compute the volatile value in the source
  NO  ↓

Does your query use ORDER BY without LIMIT?
  YES → Remove ORDER BY, or use LIMIT N for a Top-N stream table
  NO  ↓

Does your query use WITH RECURSIVE?
  YES → Supported with bounded semi-naive / DRed maintenance; tune pg_trickle.ivm_recursive_max_depth.
  NO  ↓

Do all join keys use equi-join conditions (= not BETWEEN / >=)?
  NO  → Use FULL mode, or rewrite the join condition
  YES ↓

Does every source table have a primary key or unique index on the join key?
  NO  → pg_trickle will use hash-based row IDs (slightly less efficient, but works)
  YES ↓

✅ Your query is a good candidate for DIFFERENTIAL mode.
   Use: refresh_mode => 'DIFFERENTIAL' or 'AUTO' (default).
```

---

## Multi-Column NOT IN with Nullable Elements (COR-1)

When a defining query contains a multi-column `NOT IN` subquery such as:

```sql
SELECT a, b FROM t
WHERE (a, b) NOT IN (SELECT x, y FROM s)
```

pg_trickle introduced an optimisation that rewrites `(a, b) IN (SELECT x, y …)` as a
SemiJoin and `NOT IN` as an AntiJoin.  However, SQL semantics for `NOT IN` differ from
AntiJoin semantics when either side of the comparison can be `NULL`: SQL propagates
`UNKNOWN` (which excludes the outer row), whereas an AntiJoin keeps the outer row.

**Behaviour:** When any element on the left-hand side of the row constructor is a `NULL`
constant, or when any column in the subquery's SELECT list is a `NULL` literal,
pg_trickle detects this condition and falls back to the subquery-based
(FULL refresh) execution path, emitting a `NOTICE`:

```
NOTICE: pg_trickle: multi-column NOT IN with nullable elements cannot be
rewritten to an anti-join; falling back to subquery-based delta computation.
```

**Workaround:** Rewrite using `NOT EXISTS` or add explicit `IS NOT NULL` guards to avoid
NULL-producing expressions in the row constructor.

---

## Multi-Column NOT IN with Nullable Columns (DOC-2)

When using a multi-column `NOT IN` subquery where any of the left-hand side
columns or the subquery's corresponding output columns can be `NULL` at
runtime, the DVM parser cannot safely rewrite the predicate to an anti-join.

```sql
-- Example: order_items.category_id or product.category_id may be NULL
SELECT *
FROM orders
WHERE (customer_id, category_id) NOT IN (
    SELECT customer_id, category_id FROM blocked_combinations
)
```

In this case, pg_trickle falls back to subquery-based delta computation, which
is **correct** but uses a full subquery evaluation on every refresh cycle
rather than an incremental anti-join.  For large subqueries this can be
significantly slower.

**To restore anti-join performance**, add explicit `IS NOT NULL` predicates on
both sides of the comparison to guarantee that `NULL` values are excluded
before the join is evaluated:

```sql
SELECT *
FROM orders
WHERE customer_id IS NOT NULL
  AND category_id IS NOT NULL
  AND (customer_id, category_id) NOT IN (
      SELECT customer_id, category_id
      FROM blocked_combinations
      WHERE customer_id IS NOT NULL
        AND category_id IS NOT NULL
  )
```

With these guards in place, the DVM parser can safely use an anti-join rewrite
for both `IN` and `NOT IN` forms, restoring incremental performance.

**Alternatively**, rewrite using `NOT EXISTS`:

```sql
SELECT *
FROM orders o
WHERE NOT EXISTS (
    SELECT 1 FROM blocked_combinations b
    WHERE b.customer_id = o.customer_id
      AND b.category_id = o.category_id
)
```

`NOT EXISTS` with a correlated subquery always rewrites to an anti-join
regardless of nullability, because `NOT EXISTS` uses `FALSE` (not `UNKNOWN`)
when the subquery returns no rows.

---

## Known Future Improvements

| Limitation | Planned in |
|------------|-----------|
| Window functions in output | v1.1+ |
| `WITH RECURSIVE` support | v1.2+ |
| `STRING_AGG` / `ARRAY_AGG` incremental maintenance | Researching |
| Cross-database stream tables (without foreign tables) | Not planned |
| Nested `LATERAL` (LATERAL inside LATERAL) | v1.1+ |

---

## LATERAL Joins in DIFFERENTIAL Mode (FEAT-2)

Most `LATERAL` patterns are supported in `DIFFERENTIAL` mode.  The following
patterns have known limitations:

| LATERAL pattern | Status | Notes |
|----------------|--------|-------|
| `LATERAL` volatile SRF | ❌ Falls back to FULL | `random()`, `clock_timestamp()`, etc. cannot be differentiated |
| Nested `LATERAL` (LATERAL inside LATERAL) | ❌ Falls back to FULL | Not yet implemented in delta rules |
| `LEFT JOIN LATERAL` with correlated aggregate | ⚠️ Supported (suboptimal) | Re-scans sub-table for each changed outer row; see note below |

### Correlated Aggregate Performance Note

For `LEFT JOIN LATERAL` patterns that include a correlated aggregate, pg_trickle
correctly maintains the stream table but the per-cycle cost scales with the
number of changed outer rows × the size of the inner subquery.  For high-write
workloads, materialise the aggregate as a separate stream table.

See [DVM_OPERATORS.md](DVM_OPERATORS.md#lateral-joins-and-differential-mode) for
the full compatibility table and workaround guidance.

---

## See Also

- [MENTAL_MODEL.md](MENTAL_MODEL.md) — Conceptual overview of how IVM works
- [CONFIGURATION.md](CONFIGURATION.md) — GUC options to tune limits
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — Diagnosing refresh problems
- [ERRORS.md](ERRORS.md) — Error reference
