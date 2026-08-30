# What Happens When You INSERT a Row?

> **v0.87.16 note:** The diagrams below use simplified legacy change-buffer labels to explain the lifecycle. Current buffers use flat typed columns and a complete `__pgt_row_id BYTEA` from the V2 encoder; `row_probe_v1` is only an index accelerator, never the identity.

This tutorial traces the complete lifecycle of a single `INSERT` statement on a base table that is referenced by a stream table — from the moment the row is written to the moment the stream table reflects the change.

## Setup: A Real-World Example

Suppose you run an e-commerce platform. You have an `orders` table and a stream table that maintains a running total per customer:

```sql
-- Base table
CREATE TABLE orders (
    id    SERIAL PRIMARY KEY,
    customer TEXT NOT NULL,
    amount   NUMERIC(10,2) NOT NULL
);

-- Stream table: always-fresh customer totals
SELECT pgtrickle.create_stream_table(
    name     => 'customer_totals',
    query    => $$
      SELECT customer, SUM(amount) AS total, COUNT(*) AS order_count
      FROM orders GROUP BY customer
    $$,
    schedule => '1m'  -- refresh when data is staler than 1 minute
    -- refresh_mode defaults to 'AUTO' (differential with full-refresh fallback)
);
```

After creation, `customer_totals` is a real PostgreSQL table:

```sql
SELECT * FROM customer_totals;
-- (empty — no orders yet)
```

## Phase 1: The INSERT

A new order arrives:

```sql
INSERT INTO orders (customer, amount) VALUES ('alice', 49.99);
```

### What happens inside PostgreSQL

When `create_stream_table()` was called, pg_trickle installed separate **statement-level AFTER triggers** for INSERT, UPDATE, and DELETE on the `orders` table — each with `REFERENCING` transition tables so the trigger function can read all affected rows at once. For INSERT, the trigger is:

```sql
CREATE TRIGGER pg_trickle_cdc_ins_<name>
  AFTER INSERT ON orders
  REFERENCING NEW TABLE AS __pgt_new
  FOR EACH STATEMENT EXECUTE FUNCTION
  pgtrickle_changes.pg_trickle_cdc_ins_fn_<name>();
```

This trigger fires automatically — the user's `INSERT` statement triggers it transparently.

The trigger function (`pgtrickle_changes.pg_trickle_cdc_ins_fn_<name>()`) executes inside the same transaction as the INSERT. It reads all inserted rows from the `__pgt_new` transition table and writes one change buffer row per inserted row into the **change buffer table**:

```
pgtrickle_changes.changes_16384    (where 16384 = orders table OID)
┌───────────┬─────────────┬────────┬─────────┬──────────┬──────────┬────────────┐
│ change_id │ lsn         │ action │ pk_hash  │ new_id   │ new_cust │ new_amount │
├───────────┼─────────────┼────────┼─────────┼──────────┼──────────┼────────────┤
│ 1         │ 0/1A3F2B80  │ I      │ -837291 │ 1        │ alice    │ 49.99      │
└───────────┴─────────────┴────────┴─────────┴──────────┴──────────┴────────────┘
```

Key details:
- **`lsn`**: The current WAL Log Sequence Number (`pg_current_wal_lsn()`), used to bound which changes belong to which refresh cycle.
- **`action`**: `'I'` for INSERT, `'U'` for UPDATE, `'D'` for DELETE.
- **`pk_hash`**: A pre-computed hash of the primary key (`orders.id`), used later for efficient row matching.
- **`new_*` columns**: The actual column values from `NEW`, stored as native PostgreSQL types (not JSONB). There are no `old_*` values for INSERTs.

The trigger adds **zero overhead to the user's transaction commit** beyond this single INSERT into the buffer table. There is no JSONB serialization, no logical replication slot, and no external process involved.

## Phase 2: The Scheduler Wakes Up

A background worker called the **scheduler** runs inside PostgreSQL (registered via `shared_preload_libraries`). It wakes up every `pg_trickle.scheduler_interval_ms` milliseconds (default: 1000ms) and performs a tick:

1. **Rebuild the DAG** (if any stream tables were created/dropped since last tick) — a dependency graph of all stream tables and their source tables.
2. **Topological sort** — determine the refresh order so that stream tables depending on other stream tables are refreshed after their dependencies.
3. **For each stream table**, check: has its staleness exceeded its schedule?

For `customer_totals` with a `'1m'` schedule, the scheduler compares:
- `now()` minus `data_timestamp` (the freshness watermark from the last refresh)
- Against the schedule: 60 seconds

If more than 60 seconds have elapsed and the stream table isn't already being refreshed, the scheduler begins a refresh.

## Phase 3: Frontier Advancement

Before executing the refresh, the scheduler creates a **new frontier** — a snapshot of how far to read changes from each source table:

```
Previous frontier: { orders(16384): lsn = 0/1A3F2A00 }
New frontier:      { orders(16384): lsn = 0/1A3F2C00 }
```

The frontier is a DBSP-inspired version vector. Each source table has its own LSN cursor. The refresh will process all changes in the buffer table where `lsn > previous_frontier_lsn AND lsn <= new_frontier_lsn`.

This means:
- Changes committed **before** the previous refresh are already reflected.
- Changes committed **after** the new frontier will be picked up in the next cycle.
- The INSERT we made (`lsn = 0/1A3F2B80`) falls within this window.

## Phase 4: Change Detection — Is There Anything to Do?

Before running the full delta query, the scheduler runs a **short-circuit check**: does the change buffer actually have any rows in the LSN window?

```sql
SELECT count(*)::bigint FROM (
    SELECT 1 FROM pgtrickle_changes.changes_16384
    WHERE lsn > '0/1A3F2A00'::pg_lsn
    AND lsn <= '0/1A3F2C00'::pg_lsn
    LIMIT <threshold>
) __pgt_capped
```

This query also checks the **adaptive threshold**: if the number of changes exceeds a percentage of the source table size (default: 10%), the scheduler falls back to a FULL refresh instead of DIFFERENTIAL, because applying thousands of individual deltas would be slower than a bulk reload.

For our single INSERT, the count is 1 — well below the threshold. The scheduler proceeds with a DIFFERENTIAL refresh.

## Phase 5: Delta Query Generation (DVM Engine)

This is where the Differential View Maintenance (DVM) engine does its work. The defining query:

```sql
SELECT customer, SUM(amount) AS total, COUNT(*) AS order_count
FROM orders GROUP BY customer
```

is parsed into an **operator tree**:

```
Aggregate(GROUP BY customer, SUM(amount), COUNT(*))
  └── Scan(orders)
```

The DVM engine **differentiates** each operator — converting it from "compute the full result" to "compute only what changed":

### Step 1: Differentiate the Scan

The `Scan(orders)` operator becomes a read from the change buffer:

```sql
-- Reads only changes in the LSN window, splitting UPDATEs into DELETE+INSERT
WITH __pgt_raw AS (
    SELECT c.pk_hash, c.action,
           c."new_customer", c."old_customer",
           c."new_amount", c."old_amount"
    FROM pgtrickle_changes.changes_16384 c
    WHERE c.lsn > '0/1A3F2A00'::pg_lsn
    AND   c.lsn <= '0/1A3F2C00'::pg_lsn
)
-- INSERT rows: take new_* values
SELECT pk_hash AS __pgt_row_id, 'I' AS __pgt_action,
       "new_customer" AS customer, "new_amount" AS amount
FROM __pgt_raw WHERE action IN ('I', 'U')
UNION ALL
-- DELETE rows: take old_* values
SELECT pk_hash AS __pgt_row_id, 'D' AS __pgt_action,
       "old_customer" AS customer, "old_amount" AS amount
FROM __pgt_raw WHERE action IN ('D', 'U')
```

For our single INSERT, this produces:

```
__pgt_row_id | __pgt_action | customer | amount
-------------|--------------|----------|-------
-837291      | I            | alice    | 49.99
```

### Step 2: Differentiate the Aggregate

The `Aggregate` differentiation is the heart of incremental maintenance. Instead of re-computing `SUM(amount)` over the entire `orders` table, it computes:

```sql
-- Delta for SUM: add new values, subtract deleted values
SELECT customer,
       SUM(CASE WHEN __pgt_action = 'I' THEN amount
                WHEN __pgt_action = 'D' THEN -amount END) AS total,
       SUM(CASE WHEN __pgt_action = 'I' THEN 1
                WHEN __pgt_action = 'D' THEN -1 END) AS order_count,
       pgtrickle.pg_trickle_hash(customer::text) AS __pgt_row_id,
       'I' AS __pgt_action
FROM <scan_delta>
GROUP BY customer
```

For our INSERT of `('alice', 49.99)`, this yields:

```
customer | total  | order_count | __pgt_row_id | __pgt_action
---------|--------|-------------|--------------|-------------
alice    | +49.99 | +1          | 7283194      | I
```

The stream table uses reference counting: it tracks `__pgt_count` (how many source rows contribute to each group). When `__pgt_count` reaches 0, the group row is deleted.

## Phase 6: MERGE Into the Stream Table

The delta is applied to the `customer_totals` storage table using a single SQL `MERGE` statement:

```sql
MERGE INTO public.customer_totals AS st
USING (<delta_query>) AS d
ON st.__pgt_row_id = d.__pgt_row_id
WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE
WHEN MATCHED AND d.__pgt_action = 'I' THEN
    UPDATE SET customer = d.customer, total = d.total, order_count = d.order_count
WHEN NOT MATCHED AND d.__pgt_action = 'I' THEN
    INSERT (__pgt_row_id, customer, total, order_count)
    VALUES (d.__pgt_row_id, d.customer, d.total, d.order_count)
```

Since `alice` didn't exist before, this is a `NOT MATCHED` → `INSERT`. The stream table now contains:

```sql
SELECT * FROM customer_totals;
 customer | total | order_count
----------|-------|------------
 alice    | 49.99 | 1
```

## Phase 7: Cleanup and Bookkeeping

After the MERGE succeeds:

1. **Consumed changes are deleted** from the buffer table:
   ```sql
   DELETE FROM pgtrickle_changes.changes_16384
   WHERE lsn > '0/1A3F2A00'::pg_lsn
   AND lsn <= '0/1A3F2C00'::pg_lsn
   ```

2. **The frontier is saved** to the catalog as JSONB, so the next refresh knows where to start.

3. **The refresh is recorded** in `pgtrickle.pgt_refresh_history`:
   ```
   refresh_id | pgt_id | action       | rows_inserted | rows_deleted | delta_row_count | status    | initiated_by
   1          | 1      | DIFFERENTIAL | 1             | 0            | 1               | COMPLETED | SCHEDULER
   ```

   The `delta_row_count` column records the total number of change buffer rows consumed during this refresh cycle.

4. **The data timestamp** on the stream table is advanced, resetting the staleness clock.

5. **The MERGE template is cached** in thread-local storage. The next refresh for this stream table skips SQL parsing, operator tree construction, and differentiation — it only substitutes LSN values into the cached template. This saves ~45ms per refresh cycle.

## What About UPDATE and DELETE?

### UPDATE

```sql
UPDATE orders SET amount = 59.99 WHERE id = 1;
```

The trigger writes a single row with `action = 'U'`, capturing both `OLD` and `NEW` values:

```
action | new_amount | old_amount | new_customer | old_customer
-------|------------|------------|--------------|-------------
U      | 59.99      | 49.99      | alice        | alice
```

The scan differentiation splits this into:
- **DELETE** old: `(alice, 49.99)` with action `'D'`
- **INSERT** new: `(alice, 59.99)` with action `'I'`

The aggregate differentiation computes: `+59.99 - 49.99 = +10.00` for alice's total. The MERGE updates the existing row.

### DELETE

```sql
DELETE FROM orders WHERE id = 1;
```

The trigger writes `action = 'D'` with the `OLD` values. The aggregate differentiation computes `-49.99` for the total and `-1` for the count. If the `__pgt_count` reaches 0 (no more orders for alice), the MERGE deletes alice's row from the stream table entirely.

## Performance: Why This Is Fast

| Step | What it avoids |
|------|---------------|
| Trigger-based CDC | No logical replication slot, no WAL parsing, no external process |
| Typed columns | No JSONB serialization in the trigger, no `jsonb_populate_record` in the delta query |
| Pre-computed pk_hash | No per-row hash computation during the delta query |
| LSN-bounded reads | Index scan on the change buffer, not a full table scan |
| Algebraic differentiation | Processes only changed rows — O(changes) not O(table size) |
| MERGE statement | Single SQL round-trip for all inserts, updates, and deletes |
| Cached templates | After the first refresh, delta SQL generation is skipped entirely |
| Adaptive fallback | Automatically switches to FULL refresh when changes exceed a threshold |

For a table with 10 million rows and 100 changed rows, a DIFFERENTIAL refresh processes only those 100 rows. A FULL refresh would need to scan all 10 million.

---

## What About IMMEDIATE Mode?

Everything described above applies to the default **AUTO** mode — changes accumulate in a buffer and are applied on a schedule using differential (delta-only) maintenance. pg_trickle also supports **IMMEDIATE** mode, which takes a fundamentally different path.

With IMMEDIATE mode, there are no change buffers, no scheduler, and no waiting:

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'customer_totals_live',
    query        => $$
      SELECT customer, SUM(amount) AS total, COUNT(*) AS order_count
      FROM orders GROUP BY customer
    $$,
    refresh_mode => 'IMMEDIATE'
);
```

### How IMMEDIATE Mode Differs for INSERT

| Phase | DIFFERENTIAL | IMMEDIATE |
|-------|-------------|----------|
| **Trigger type** | Row-level AFTER trigger | Statement-level AFTER trigger with `REFERENCING NEW TABLE` |
| **What's captured** | One buffer row per INSERT | A transition table containing all inserted rows |
| **When delta runs** | Next scheduler tick (up to schedule bound) | Immediately, in the same transaction |
| **Delta source** | Change buffer table (`pgtrickle_changes.*`) | Temp table copied from transition table |
| **Concurrency** | No locking between writers | Advisory lock per stream table |

When you run `INSERT INTO orders ...`:

1. A **BEFORE INSERT** statement-level trigger acquires an advisory lock on the stream table
2. The **AFTER INSERT** trigger captures the transition table (`NEW TABLE AS __pgt_newtable`) into a temp table
3. The DVM engine generates the same delta query, but reads from the temp table instead of the change buffer
4. The delta is applied to the stream table via INSERT/DELETE DML (not MERGE)
5. The stream table is immediately up-to-date — **within the same transaction**

```sql
BEGIN;
INSERT INTO orders (customer, amount) VALUES ('alice', 49.99);
-- customer_totals_live already shows alice with total=49.99 here!
SELECT * FROM customer_totals_live;
COMMIT;
```

The delta SQL template is cached per (pgt_id, source_oid, has_new, has_old) combination, so subsequent trigger invocations skip query parsing entirely.

---

## Next in This Series

- **[What Happens When You UPDATE a Row?](WHAT_HAPPENS_ON_UPDATE.md)** — D+I split, group key changes, net-effect for multiple UPDATEs
- **[What Happens When You DELETE a Row?](WHAT_HAPPENS_ON_DELETE.md)** — Reference counting, group deletion, INSERT+DELETE cancellation
- **[What Happens When You TRUNCATE a Table?](WHAT_HAPPENS_ON_TRUNCATE.md)** — Why TRUNCATE bypasses triggers and how to recover
