# Tutorial: Migrating from pg_ivm to pg_trickle

This guide walks through migrating existing `pg_ivm` IMMVs (Incrementally
Maintained Materialized Views) to pg_trickle stream tables. It covers API
mapping, behavioral differences, and a step-by-step migration checklist.

> **See also:** [plans/ecosystem/GAP_PG_IVM_COMPARISON.md](https://github.com/trickle-labs/pg-trickle/blob/main/plans/ecosystem/GAP_PG_IVM_COMPARISON.md)
> for the full feature comparison and gap analysis between the two extensions.

---

## Why Migrate?

| | pg_ivm (IMMV) | pg_trickle (Stream Table) |
|-|---------------|--------------------------|
| Maintenance model | Immediate only (in-transaction) | Deferred (scheduler) **and** Immediate |
| Aggregate functions | 5 (COUNT, SUM, AVG, MIN, MAX) | 60+ (all built-in + user-defined) |
| Window functions | Not supported | Full support |
| CTEs (recursive) | Not supported | Semi-naive, DRed, recomputation |
| Subqueries | Very limited | Full (EXISTS, NOT EXISTS, IN, LATERAL, scalar) |
| Set operations | Not supported | UNION, INTERSECT, EXCEPT (bag + set) |
| HAVING clause | Not supported | Supported |
| GROUPING SETS / CUBE / ROLLUP | Not supported | Auto-rewritten to UNION ALL |
| DISTINCT ON | Not supported | Auto-rewritten to ROW_NUMBER |
| Views as sources | Not supported | Auto-inlined |
| Cascading views | Not supported | DAG-aware topological scheduling |
| Background scheduling | None (manual only) | Native cron, duration, CALCULATED |
| Monitoring | 1 catalog table | 15+ diagnostic functions |
| Concurrency | ExclusiveLock during maintenance | Advisory locks, non-blocking reads |
| Parallel refresh | Not supported | Worker pool with caps |

---

## Concept Mapping

| pg_ivm Concept | pg_trickle Equivalent | Notes |
|----------------|----------------------|-------|
| IMMV (Incrementally Maintained Materialized View) | Stream table | Same idea — a query result kept incrementally up to date |
| `pgivm.create_immv(name, query)` | `pgtrickle.create_stream_table(name, query)` | pg_trickle adds optional `schedule` and `refresh_mode` parameters |
| `pgivm.refresh_immv(name, true)` | `pgtrickle.refresh_stream_table(name)` | Manual refresh |
| `pgivm.refresh_immv(name, false)` | No direct equivalent | pg_trickle has `pgtrickle.alter_stream_table(name, enabled => false)` to suspend |
| `pgivm.pg_ivm_immv` catalog | `pgtrickle.pgt_stream_tables` | Plus `pgt_status()`, `refresh_timeline()`, etc. |
| `DROP TABLE immv_name` | `pgtrickle.drop_stream_table(name)` | Stream tables must be dropped via the API |
| `ALTER TABLE immv RENAME TO ...` | `pgtrickle.alter_stream_table(old, name => new)` | Rename via API |
| In-transaction maintenance (AFTER row triggers) | `refresh_mode => 'IMMEDIATE'` | Same model — triggers fire in the writing transaction |
| (not available) | `refresh_mode => 'DIFFERENTIAL'` | Deferred differential refresh via change buffers |
| (not available) | `refresh_mode => 'AUTO'` | Picks DIFFERENTIAL or FULL automatically |
| Auto-created indexes on GROUP BY / PK | Manual `CREATE INDEX` | pg_trickle auto-creates the primary key but not secondary indexes |

---

## Step-by-Step Migration

### 1. Inventory existing IMMVs

List all pg_ivm IMMVs in your database:

```sql
-- pg_ivm catalog
SELECT immvrelid::regclass AS immv_name,
       pgivm.get_immv_def(immvrelid) AS defining_query
FROM pgivm.pg_ivm_immv
ORDER BY immvrelid::regclass::text;
```

Record each IMMV's name, defining query, and any indexes you have created on it.

### 2. Check query compatibility

pg_trickle supports a superset of pg_ivm's SQL dialect, so any query that works
with pg_ivm will work with pg_trickle. However, there are a few things to verify:

- **Data types:** pg_ivm requires btree operator classes for all columns
  (excluding `json`, `xml`, `point`, etc.). pg_trickle has no such restriction.
- **Outer joins:** If your IMMV uses outer joins, pg_trickle removes pg_ivm's
  restrictions (single equijoin, no aggregates, no CASE). Your query may work
  unchanged or you may be able to simplify workarounds you added for pg_ivm.

### 3. Choose a refresh mode

For each IMMV, decide which pg_trickle refresh mode to use:

| pg_ivm behavior | pg_trickle refresh mode | When to choose |
|-----------------|------------------------|----------------|
| Zero staleness required | `IMMEDIATE` | Same in-transaction behavior as pg_ivm |
| Some staleness acceptable | `DIFFERENTIAL` with schedule | Lower write latency, batched refresh |
| Let pg_trickle decide | `AUTO` (default) | Recommended for most cases |

### 4. Create stream tables

For each IMMV, create the corresponding stream table:

**pg_ivm (before):**
```sql
SELECT pgivm.create_immv(
    'order_totals',
    'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id'
);
```

**pg_trickle — IMMEDIATE mode (same behavior as pg_ivm):**
```sql
SELECT pgtrickle.create_stream_table(
    'order_totals',
    'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    NULL,          -- no schedule needed for IMMEDIATE
    'IMMEDIATE'
);
```

**pg_trickle — deferred mode (lower write latency):**
```sql
SELECT pgtrickle.create_stream_table(
    'order_totals',
    'SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id',
    '30s'          -- refresh every 30 seconds; mode defaults to AUTO
);
```

### 5. Recreate indexes

pg_ivm auto-creates indexes on `GROUP BY`, `DISTINCT`, and primary key columns.
pg_trickle auto-creates the primary key (`pgt_row_id`) but not secondary indexes.

Recreate any indexes that your read queries depend on:

```sql
-- Example: index on the GROUP BY column for lookup queries
CREATE INDEX ON pgtrickle.order_totals (customer_id);
```

### 6. Update application queries

pg_ivm IMMVs live in the schema where they were created (usually `public`).
pg_trickle stream tables default to the `pgtrickle` schema.

```sql
-- Before (pg_ivm):
SELECT * FROM public.order_totals WHERE customer_id = 42;

-- After (pg_trickle):
SELECT * FROM pgtrickle.order_totals WHERE customer_id = 42;
```

To avoid changing application code, create a compatibility view:

```sql
CREATE VIEW public.order_totals AS
SELECT * FROM pgtrickle.order_totals;
```

### 7. Verify correctness

After creating the stream table and running a refresh, compare results:

```sql
-- Compare row counts
SELECT 'immv' AS source, COUNT(*) FROM public.order_totals_immv
UNION ALL
SELECT 'stream_table', COUNT(*) FROM pgtrickle.order_totals;

-- Full diff (should return zero rows)
(SELECT * FROM public.order_totals_immv EXCEPT SELECT * FROM pgtrickle.order_totals)
UNION ALL
(SELECT * FROM pgtrickle.order_totals EXCEPT SELECT * FROM public.order_totals_immv);
```

### 8. Drop the old IMMV

Once you have verified the stream table is correct and applications are updated:

```sql
DROP TABLE public.order_totals_immv;
```

### 9. (Optional) Remove pg_ivm

After all IMMVs are migrated:

```sql
DROP EXTENSION pg_ivm CASCADE;
```

Remove `pg_ivm` from `shared_preload_libraries` if it was listed there and
restart PostgreSQL.

---

## Behavioral Differences to Be Aware Of

### Locking

- **pg_ivm:** Holds `ExclusiveLock` on the IMMV during maintenance. In
  `REPEATABLE READ` / `SERIALIZABLE`, concurrent writes to the same IMMV's
  base tables may raise serialization errors.
- **pg_trickle (IMMEDIATE):** Uses advisory locks. Concurrent reads of the
  stream table are never blocked.
- **pg_trickle (deferred):** Base table writes only insert into change buffers
  (~2–50 μs). No lock contention with refresh.

### TRUNCATE

- **pg_ivm:** Synchronously truncates or fully refreshes the IMMV.
- **pg_trickle (IMMEDIATE):** Performs a full refresh within the same
  transaction.
- **pg_trickle (deferred):** Clears the change buffer and queues a full
  refresh on the next cycle.

### Logical Replication

- **pg_ivm:** Not compatible with logical replication — subscriber nodes do
  not have triggers that fire for replicated changes.
- **pg_trickle (deferred):** Supports WAL-based CDC (`pg_trickle.cdc_mode = 'wal'`)
  which reads from the WAL directly. Trigger-based CDC also works with
  logical replication if triggers are created on the subscriber.

### Schema Changes

- **pg_ivm:** No automatic DDL tracking. If a base table column is altered,
  the IMMV may break silently.
- **pg_trickle:** Event triggers detect DDL changes on source tables and
  automatically reinitialize affected stream tables.

---

## Upgrading Queries That pg_ivm Couldn't Handle

pg_ivm's SQL restrictions often force users to create workarounds. With
pg_trickle, many of these workarounds can be simplified:

### HAVING clauses

```sql
-- pg_ivm workaround: filter in application or wrap in a view
SELECT pgivm.create_immv('big_customers',
    'SELECT customer_id, SUM(amount) AS total
     FROM orders GROUP BY customer_id'
);
-- Then: SELECT * FROM big_customers WHERE total > 1000;

-- pg_trickle: use HAVING directly
SELECT pgtrickle.create_stream_table('big_customers',
    'SELECT customer_id, SUM(amount) AS total
     FROM orders GROUP BY customer_id
     HAVING SUM(amount) > 1000'
);
```

### NOT EXISTS / anti-joins

```sql
-- pg_ivm: not supported — manual workaround required

-- pg_trickle: works directly
SELECT pgtrickle.create_stream_table('orphan_orders',
    'SELECT o.* FROM orders o
     WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id)'
);
```

### Window functions

```sql
-- pg_ivm: not supported

-- pg_trickle: works directly
SELECT pgtrickle.create_stream_table('ranked_products',
    'SELECT product_id, category, revenue,
            RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS rnk
     FROM product_revenue'
);
```

### UNION ALL pipelines

```sql
-- pg_ivm: not supported — requires separate IMMVs + application-side UNION

-- pg_trickle: works directly
SELECT pgtrickle.create_stream_table('all_events',
    'SELECT id, ts, ''order'' AS type FROM order_events
     UNION ALL
     SELECT id, ts, ''return'' AS type FROM return_events'
);
```

---

## Monitoring After Migration

pg_trickle provides extensive monitoring that pg_ivm does not offer:

```sql
-- Overall health
SELECT * FROM pgtrickle.health_check();

-- Status of all stream tables (includes staleness, last refresh, error count)
SELECT * FROM pgtrickle.pgt_status();

-- Recent refresh history across all stream tables
SELECT * FROM pgtrickle.refresh_timeline(20);

-- CDC pipeline health
SELECT * FROM pgtrickle.change_buffer_sizes();

-- Diagnose errors for a specific stream table
SELECT * FROM pgtrickle.diagnose_errors('order_totals');
```

See [SQL Reference](../SQL_REFERENCE.md) for the complete list of monitoring
functions.
