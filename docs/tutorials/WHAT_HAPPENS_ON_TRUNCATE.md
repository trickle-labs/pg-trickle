# What Happens When You TRUNCATE a Table?

This tutorial explains what happens when a `TRUNCATE` statement hits a base table that is referenced by a stream table. PostgreSQL does not provide per-row OLD records for TRUNCATE, so pg_trickle captures it with a statement-level marker and refreshes affected stream tables with a full recomputation.

> **Prerequisite:** Read [WHAT_HAPPENS_ON_INSERT.md](WHAT_HAPPENS_ON_INSERT.md) first — it introduces the 7-phase lifecycle. This tutorial explains why TRUNCATE takes a different path through that lifecycle.

## Setup

Same e-commerce example used throughout the series:

```sql
CREATE TABLE orders (
    id       SERIAL PRIMARY KEY,
    customer TEXT NOT NULL,
    amount   NUMERIC(10,2) NOT NULL
);

SELECT pgtrickle.create_stream_table(
    name     => 'customer_totals',
    query    => $$
      SELECT customer, SUM(amount) AS total, COUNT(*) AS order_count
      FROM orders GROUP BY customer
    $$,
    schedule => '1m'
);

-- Seed some data
INSERT INTO orders (customer, amount) VALUES
    ('alice', 50.00),
    ('alice', 30.00),
    ('bob',   75.00),
    ('bob',   25.00);
```

After the first refresh, the stream table contains:

```
customer | total  | order_count
---------|--------|------------
alice    | 80.00  | 2
bob      | 100.00 | 2
```

---

## Case 1: TRUNCATE the Base Table (DIFFERENTIAL Mode)

```sql
TRUNCATE orders;
```

All four rows are removed instantly.

### What Happens at the Trigger Level: TRUNCATE Marker

> pg_trickle installs a **statement-level AFTER TRUNCATE** trigger on tracked source tables. This trigger writes a single marker row to the change buffer with `action = 'T'`.

Unlike the per-row DML triggers, the TRUNCATE trigger cannot capture individual row data (PostgreSQL's `TRUNCATE` does not provide `OLD` records). Instead, it writes a sentinel:

```
pgtrickle_changes.changes_16384
┌───────────┬─────────────┬────────┬──────────┬──────────┐
│ change_id │ lsn         │ action │ new_*    │ old_*    │
├───────────┼─────────────┼────────┼──────────┼──────────┤
│ 5         │ 0/1A3F4000  │ T      │ NULL     │ NULL     │
└───────────┴─────────────┴────────┴──────────┴──────────┘
```

The `'T'` action marker tells the refresh engine: "a TRUNCATE happened — a full refresh is required."

### What Happens at the Scheduler: Automatic Full Refresh

On the next refresh cycle, the scheduler:
1. Checks the change buffer for rows in the LSN window
2. Finds the `action = 'T'` marker row
3. **Falls back to a FULL refresh** — regardless of the stream table's configured `refresh_mode`
4. TRUNCATEs the stream table
5. Re-executes the defining query against the current base table state
6. Inserts all results

Since the `orders` table is now empty, the defining query returns zero rows:

```sql
-- After the next scheduled refresh:
SELECT * FROM customer_totals;
 customer | total | order_count
----------|-------|------------
 (0 rows)                        ← correct: orders is empty
```

**No manual intervention required.** The TRUNCATE marker ensures the stream table is automatically brought back into consistency on the next refresh cycle.

> **Note:** In older versions, TRUNCATE was not captured at all — the change buffer stayed empty and the stream table became silently stale. If you're running an older version, you still need to call `pgtrickle.refresh_stream_table()` manually after a TRUNCATE.

---

## Case 2: Manual Refresh (Explicit Recovery)

Although TRUNCATE is now automatically handled on the next refresh cycle, you can force an immediate recovery without waiting:

```sql
SELECT pgtrickle.refresh_stream_table('customer_totals');
```

This executes a **full refresh** regardless of the stream table's configured refresh mode:

1. **TRUNCATE** the stream table itself (clearing the stale data)
2. **Re-execute** the defining query
3. **INSERT** the results into the stream table
4. **Update the frontier** so future differential refreshes start from the current LSN

This is useful when you can't wait for the next scheduled refresh cycle and need the stream table consistent immediately.

---

## Case 3: TRUNCATE Then INSERT (Common ETL Pattern)

A common data loading pattern is:

```sql
BEGIN;
TRUNCATE orders;
INSERT INTO orders (customer, amount) VALUES
    ('charlie', 100.00),
    ('charlie', 200.00),
    ('dave',    150.00);
COMMIT;
```

### What the Change Buffer Sees

- TRUNCATE: **1 marker event** (`action = 'T'`) — captured by the statement-level trigger
- INSERT charlie 100.00: **1 event** (captured)
- INSERT charlie 200.00: **1 event** (captured)
- INSERT dave 150.00: **1 event** (captured)

The change buffer has 4 rows — the TRUNCATE marker plus 3 INSERT events.

### What the Scheduler Does

The scheduler sees the `action = 'T'` marker and triggers a **full refresh**, ignoring the individual INSERT events. The full refresh re-executes the defining query against the current state of `orders`, which now contains only charlie and dave:

```sql
-- After the next scheduled refresh:
SELECT * FROM customer_totals;
 customer | total  | order_count
----------|--------|------------
 charlie  | 300.00 | 2            ← correct
 dave     | 150.00 | 1            ← correct
```

The old data (alice, bob) is gone because the full refresh recomputed from scratch. This is correct — the TRUNCATE marker ensures consistency regardless of what other changes occurred in the same window.

---

## Case 4: TRUNCATE a Dimension Table in a JOIN

Consider a stream table that joins two tables:

```sql
CREATE TABLE customers (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    tier TEXT NOT NULL DEFAULT 'standard'
);

CREATE TABLE orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(id),
    amount      NUMERIC(10,2)
);

SELECT pgtrickle.create_stream_table(
    name         => 'order_details',
    query        => $$
      SELECT c.name, c.tier, o.amount
      FROM orders o
      JOIN customers c ON o.customer_id = c.id
    $$,
    schedule => '1m'
);
```

Now truncate the dimension table:

```sql
TRUNCATE customers CASCADE;
```

The `CASCADE` also truncates `orders` (due to the foreign key). Both tables have TRUNCATE triggers installed, so both write a `'T'` marker to their respective change buffers.

On the next refresh cycle, the scheduler detects the TRUNCATE markers and performs a full refresh. The stream table is recomputed from the now-empty base tables:

```sql
-- After the next scheduled refresh:
SELECT * FROM order_details;
-- (0 rows) — correct
```

---

## Case 5: FULL Mode Stream Tables Are Immune

If the stream table uses `FULL` refresh mode instead of `DIFFERENTIAL`:

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'customer_totals_full',
    query        => $$
      SELECT customer, SUM(amount) AS total, COUNT(*) AS order_count
      FROM orders GROUP BY customer
    $$,
    schedule     => '1m',
    refresh_mode => 'FULL'
);
```

A FULL-mode stream table doesn't use the change buffer at all. Every refresh cycle:
1. TRUNCATEs the stream table
2. Re-executes the defining query
3. Inserts all results

So after a TRUNCATE of the base table, the **next scheduled refresh** automatically picks up the correct state — no manual intervention needed. The trade-off is that every refresh recomputes from scratch, which is more expensive for large result sets.

---

## Why PostgreSQL Doesn't Fire Row Triggers on TRUNCATE

Understanding the PostgreSQL internals helps explain why per-row capture is impossible:

| Operation | Mechanism | Row triggers fired? | Statement triggers fired? |
|-----------|-----------|-------------------|-------------------------|
| `DELETE FROM t` | Scans and removes rows one by one | Yes — AFTER DELETE per row | Yes |
| `TRUNCATE t` | Removes all heap files and reinitializes the table storage | No — no per-row processing | Yes — AFTER TRUNCATE |
| `DELETE FROM t WHERE true` | Same as `DELETE FROM t` (full scan) | Yes — AFTER DELETE per row | Yes |

`TRUNCATE` is fundamentally different from `DELETE`. It's an O(1) operation that replaces the table's storage files, while DELETE is O(N) — scanning every row and recording each removal in WAL.

pg_trickle uses a **statement-level** `AFTER TRUNCATE` trigger to detect the event and write a `'T'` marker to the change buffer. This marker does not contain per-row data (PostgreSQL's TRUNCATE trigger doesn't provide OLD records), but it's sufficient to signal that a full refresh is needed.

---

## Alternative: DELETE FROM Instead of TRUNCATE

For **DIFFERENTIAL** mode, `TRUNCATE` is now handled automatically (via the `'T'` marker and full refresh fallback). However, using `DELETE FROM` instead of `TRUNCATE` has its own advantages:

```sql
-- Instead of: TRUNCATE orders;
DELETE FROM orders;
```

This fires the row-level DELETE trigger for every row. The change buffer captures all removals, and the next differential refresh correctly decrements all reference counts through the standard algebraic delta path — avoiding the need for a full refresh fallback.

| Approach | Speed | Stream table consistent? | Refresh type |
|----------|-------|------------------------|-------------|
| `TRUNCATE orders` | O(1) — instant | Yes — automatic full refresh on next cycle | FULL (fallback) |
| `DELETE FROM orders` | O(N) — scans all rows | Yes — per-row triggers fire | DIFFERENTIAL |
| `TRUNCATE` + manual refresh | O(1) + O(query) | Yes — immediately | FULL (manual) |

For tables with millions of rows, `DELETE FROM` can be slow and generate significant WAL. TRUNCATE is generally the better choice — the automatic full refresh fallback makes it safe to use.

---

## Best Practices

### 1. TRUNCATE Is Safe to Use

TRUNCATE on tracked source tables is automatically detected and triggers a full refresh on the next scheduler cycle. No manual intervention is required for standard operation.

### 2. Use Manual Refresh for Immediate Consistency

If you need the stream table to be consistent immediately (not on the next cycle), call refresh explicitly:

```sql
TRUNCATE orders;
SELECT pgtrickle.refresh_stream_table('customer_totals');
```

### 3. Consider IMMEDIATE Mode for Real-Time Needs

For stream tables that need to reflect TRUNCATE instantly (within the same transaction), use IMMEDIATE mode. The TRUNCATE trigger automatically performs a full refresh synchronously.

### 4. Consider FULL Mode for ETL-Heavy Tables

If a table is routinely truncated and reloaded, FULL refresh mode may be simpler than DIFFERENTIAL — it naturally handles TRUNCATE because it recomputes from scratch every cycle.

### 5. Use `trigger_inventory()` to Verify Triggers

You can verify that both the DML trigger and the TRUNCATE trigger are installed and enabled:

```sql
SELECT * FROM pgtrickle.trigger_inventory();
```

This shows one row per (source table, trigger type) confirming both `pg_trickle_cdc_<oid>` (DML) and `pg_trickle_cdc_truncate_<oid>` (TRUNCATE) triggers are present.

---

## How TRUNCATE Compares to Other Operations

| Aspect | INSERT | UPDATE | DELETE | TRUNCATE |
|--------|--------|--------|--------|----------|
| **Row trigger fires?** | Yes (per row) | Yes (per row) | Yes (per row) | No |
| **Statement trigger fires?** | Yes | Yes | Yes | Yes (writes `'T'` marker) |
| **Change buffer** | 1 row per INSERT | 1 row per UPDATE | 1 row per DELETE | 1 marker row (`action='T'`) |
| **Stream table updated?** | Yes (next refresh) | Yes (next refresh) | Yes (next refresh) | Yes (full refresh on next cycle) |
| **Recovery** | Automatic (differential) | Automatic (differential) | Automatic (differential) | Automatic (full refresh fallback) |
| **FULL mode affected?** | N/A (recomputes) | N/A (recomputes) | N/A (recomputes) | N/A (recomputes) |
| **IMMEDIATE mode?** | Synchronous delta | Synchronous delta | Synchronous delta | Synchronous full refresh |
| **Speed** | O(1) per row | O(1) per row | O(1) per row | O(1) + O(query) for refresh |

---

## What About IMMEDIATE Mode?

In **IMMEDIATE** mode, TRUNCATE is handled synchronously within the same transaction:

1. The **BEFORE TRUNCATE** trigger acquires an advisory lock on the stream table
2. The **AFTER TRUNCATE** trigger calls `pgt_ivm_handle_truncate(pgt_id)`
3. This function TRUNCATEs the stream table and re-populates it by re-executing the defining query
4. The stream table is immediately consistent — within the same transaction

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'customer_totals_live',
    query        => $$
      SELECT customer, SUM(amount) AS total, COUNT(*) AS order_count
      FROM orders GROUP BY customer
    $$,
    refresh_mode => 'IMMEDIATE'
);

BEGIN;
TRUNCATE orders;
-- customer_totals_live is already empty here!
SELECT * FROM customer_totals_live;  -- (0 rows)
COMMIT;
```

No waiting for a scheduler cycle, no stale data — TRUNCATE is fully handled in real-time.

---

## Summary

TRUNCATE is fully tracked by pg_trickle across all three refresh modes. While it cannot be captured as per-row DELETE events (PostgreSQL's TRUNCATE doesn't process individual rows), pg_trickle uses a statement-level trigger to detect the event and respond appropriately.

The key takeaways:

1. **TRUNCATE is automatically handled** — a statement-level AFTER TRUNCATE trigger writes a `'T'` marker to the change buffer
2. **DIFFERENTIAL mode: automatic full refresh** — the scheduler detects the marker and falls back to a full refresh on the next cycle
3. **IMMEDIATE mode: synchronous full refresh** — the stream table is rebuilt within the same transaction
4. **FULL mode: naturally immune** — every refresh recomputes from scratch regardless
5. **Manual refresh for instant consistency** — call `pgtrickle.refresh_stream_table()` if you can't wait for the next cycle
6. **`DELETE FROM` remains an alternative** — fires per-row triggers, enabling incremental delta processing instead of full refresh fallback

---

## Next in This Series

- **[What Happens When You INSERT a Row?](WHAT_HAPPENS_ON_INSERT.md)** — The full 7-phase lifecycle (start here if you haven't already)
- **[What Happens When You UPDATE a Row?](WHAT_HAPPENS_ON_UPDATE.md)** — D+I split, group key changes, net-effect for multiple UPDATEs
- **[What Happens When You DELETE a Row?](WHAT_HAPPENS_ON_DELETE.md)** — Reference counting, group deletion, INSERT+DELETE cancellation
