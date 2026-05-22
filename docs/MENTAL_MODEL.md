# Mental Model: How pg_trickle Works

This document explains the core concepts behind pg_trickle's differential
view maintenance engine for developers who know SQL but have not studied
incremental view maintenance (IVM) theory. Analogies come before formulas.

---

## 1. The Problem: Expensive Full Recomputation

A standard PostgreSQL materialized view is a snapshot. When the source data
changes, you call `REFRESH MATERIALIZED VIEW` and PostgreSQL re-runs the entire
defining query — scanning every row in every source table, applying all the
joins, filtering, and aggregating — every time.

For a billion-row orders table with 100 new orders since the last refresh,
this is the equivalent of re-reading an entire library to update one paragraph.

pg_trickle solves this with **differential maintenance**: compute only the
*change* in the view output caused by the *change* in the inputs.

---

## 2. The Key Insight: Deltas Are Just Rows

Think of a change to a table as a signed multiset of rows:

- `+1` for an inserted row
- `-1` for a deleted row
- `-1` for the old version of an updated row, `+1` for the new version

If the source table `T` changes by a delta `ΔT`, and the view `V = f(T)`,
then the view output changes by `ΔV = f(T + ΔT) - f(T)`.

For many SQL operators, `ΔV` can be computed without reading `T` at all —
only `ΔT` is needed. For a simple `SELECT * FROM orders WHERE status = 'active'`:

- `ΔV = { new rows in ΔT where status = 'active' } - { deleted rows in ΔT where status = 'active' }`

For a `COUNT(*)` aggregate, the delta is even simpler:

- `Δcount = (inserted active rows) - (deleted active rows)`

This is why pg_trickle can refresh a stream table in milliseconds even when
the source table has billions of rows.

---

## 3. Change Capture: The Change Buffer

Before pg_trickle can compute `ΔV`, it needs to know `ΔT`. It captures changes
using **trigger-based CDC** (statement-level triggers by default) or **WAL
decoding**.

Each source table gets a dedicated **change buffer** table:
`pgtrickle_changes.changes_<source_table_oid>` or a stable-name equivalent. The
trigger writes every inserted, updated, or deleted row into this buffer as part
of the *same transaction* as the DML. This gives you:

- **Atomicity**: A committed change is guaranteed to be in the buffer.
- **No missed changes**: There is no window between commit and capture.
- **Snapshot isolation**: The buffer holds the before/after images of each row.

The change buffer accumulates rows between refresh cycles. On each refresh,
the DVM engine reads the buffer, computes the delta SQL, applies it to the
stream table, and truncates the buffer.

---

## 4. The Delta SQL

For each stream table, pg_trickle pre-generates a **delta SQL template** at
creation time. This template is parameterized by the change buffer contents
and produces the `ΔV` rows to apply.

For a simple aggregation like:
```sql
SELECT customer_id, COUNT(*) AS order_count
FROM orders GROUP BY customer_id
```

The delta SQL looks roughly like:
```sql
-- Compute which customers changed in this refresh window
WITH changed_customers AS (
    SELECT DISTINCT customer_id FROM pgtrickle_changes.changes_<oid>
),
-- Recompute count for only the affected customers
new_counts AS (
    SELECT customer_id, COUNT(*) AS order_count
    FROM orders
    WHERE customer_id IN (SELECT customer_id FROM changed_customers)
    GROUP BY customer_id
)
-- Apply: delete old rows, insert new rows for changed customers
MERGE INTO stream_table AS t
USING new_counts AS s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET order_count = s.order_count
WHEN NOT MATCHED THEN INSERT VALUES (s.customer_id, s.order_count)
WHEN NOT MATCHED BY SOURCE AND t.customer_id IN (SELECT customer_id FROM changed_customers)
    THEN DELETE;
```

The key property: the `FROM orders` scan is filtered to **only the affected
customer IDs**, not the full table. When 10 customers out of 10 million changed,
only 10 customer IDs are scanned.

---

## 5. Algebraic Operators: What Can Be Maintained Incrementally?

Not all SQL operators can be maintained in O(Δ). pg_trickle classifies
them into categories:

### ✅ Fully Incremental (O(Δ))
- `SELECT` with filters, projections, casts
- `INNER JOIN`, `LEFT JOIN` (equi-join with indexed keys)
- `GROUP BY` with algebraic aggregates: `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`
- `DISTINCT` (with reference counting)
- `UNION ALL`
- `WHERE EXISTS` / `WHERE NOT EXISTS` (converted to semi/anti-join)
- `HAVING` (filter on aggregate result)

### ⚠️ Conditionally Incremental
- `COUNT(DISTINCT x)` — incremental with algebraic Z-set counting
- `STDDEV`, `VARIANCE` — incremental using sum-of-squares decomposition
- `TOP-N` (ORDER BY ... LIMIT) — incremental within the top-N window
- Multi-table joins — incremental, but delta SQL becomes larger with more tables
- `CUBE` / `ROLLUP` — expanded into UNION ALL branches, each incremental

### ❌ Not Incremental (falls back to FULL refresh)
- `TABLESAMPLE` — non-deterministic, cannot be differentiated
- `VOLATILE` functions (`random()`, `now()`, `nextval()`) in the SELECT list
- `ORDER BY` without `LIMIT` — full sort on every refresh
- `FETCH FIRST` without `ORDER BY` — non-deterministic
- Window functions in the output — planned for future support
- Recursive CTEs with `CYCLE` — non-terminating delta

When a query contains a non-incremental operator, pg_trickle automatically
uses **FULL refresh** — replacing the stream table contents entirely. This is
transparent to the application.

---

## 6. The Row Identity Problem

`MERGE` needs to know which rows in the stream table correspond to which rows
in the delta. This is the **row identity problem**.

For stream tables with a natural primary key in the output (e.g., `customer_id`),
the MERGE key is obvious. For aggregations without a natural key, or for queries
with complex output structures, pg_trickle computes a **row identity hash**
(`__pgt_row_id`) from the grouping keys or the query structure. This column is
maintained automatically and is invisible in normal `SELECT *` queries.

---

## 7. The Refresh Cycle

```
Source DML → CDC trigger → change buffer (same txn)
                                 ↓
                     Scheduler background worker (async)
                                 ↓
                     delta SQL → MERGE into stream table
                                 ↓
                     Truncate change buffer
```

The scheduler wakes every `pg_trickle.scheduler_interval_ms` (default 1s),
checks which stream tables are ready to refresh based on their `schedule`,
and runs the refresh in dependency (topological) order.

**Key property:** The application sees a consistent read of the stream table
at all times. The MERGE either has fully committed or not. There is no partial
update visible to readers.

---

## 8. DAG Chaining: Stream Tables as Sources

Stream tables can themselves be sources for other stream tables, forming a
**directed acyclic graph (DAG)** of dependencies:

```
orders → orders_by_customer → customer_top10
              ↑
         order_items
```

When `orders` changes, pg_trickle refreshes `orders_by_customer` first, then
uses its delta to refresh `customer_top10`. Each step is O(Δ), so the full
chain completes in time proportional to the number of changed rows — not the
total data size.

pg_trickle detects cycles and rejects stream table definitions that would
create them (unless `pg_trickle.allow_circular = true`, which enables fixpoint
iteration for convergent circular queries).

The scheduler runs refreshes in topological order and supports **parallel
refresh** (`pg_trickle.parallel_refresh_mode = 'on'`, the default) to execute
independent branches of the DAG concurrently.

---

## See Also

- [LIMITATIONS.md](LIMITATIONS.md) — What pg_trickle cannot do and why
- [ARCHITECTURE.md](ARCHITECTURE.md) — Internal module structure
- [DVM_REWRITE_RULES.md](DVM_REWRITE_RULES.md) — SQL rewrite passes
- [DVM_OPERATORS.md](DVM_OPERATORS.md) — Per-operator delta rules
- [PERFORMANCE_CHEATSHEET.md](PERFORMANCE_CHEATSHEET.md) — Quick performance guide
