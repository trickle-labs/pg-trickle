# What Happens When You DELETE a Row?

> **v0.87.16 note:** The diagrams below use simplified legacy change-buffer labels to explain the lifecycle. Current buffers use flat typed columns and a complete `__pgt_row_id BYTEA` from the V2 encoder; `row_probe_v1` is only an index accelerator, never the identity.

This tutorial traces what happens when a `DELETE` statement hits a base table that is referenced by a stream table. It covers the trigger capture, how the scan delta emits a single DELETE event, and how each DVM operator propagates the removal — including group deletion, partial group reduction, JOINs, cascading deletes within a single refresh window, and the important edge case where a DELETE cancels a prior INSERT.

> **Prerequisite:** Read [WHAT_HAPPENS_ON_INSERT.md](WHAT_HAPPENS_ON_INSERT.md) first — it introduces the full 7-phase lifecycle (trigger → scheduler → frontier → change detection → DVM delta → MERGE → cleanup). This tutorial focuses on how DELETE differs.

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

## Case 1: Delete One Row (Group Survives)

```sql
DELETE FROM orders WHERE id = 2;  -- alice's 30.00 order
```

Alice still has one remaining order (id=1, amount=50.00). The group shrinks but doesn't vanish.

### Phase 1: Trigger Capture

The AFTER DELETE trigger fires and writes **one row** to the change buffer with only OLD values:

```
pgtrickle_changes.changes_16384
┌───────────┬─────────────┬────────┬──────────┬──────────┬────────────┬──────────┬────────────┐
│ change_id │ lsn         │ action │ new_cust │ new_amt  │ old_cust   │ old_amt  │ pk_hash    │
├───────────┼─────────────┼────────┼──────────┼──────────┼────────────┼──────────┼────────────┤
│ 5         │ 0/1A3F3000  │ D      │ NULL     │ NULL     │ alice      │ 30.00    │ 4521038    │
└───────────┴─────────────┴────────┴──────────┴──────────┴────────────┴──────────┴────────────┘
```

Key difference from INSERT and UPDATE:
- **`new_*` columns are all NULL** — the row no longer exists, so there are no NEW values
- **`old_*` columns contain the deleted row's data** — this is what gets subtracted
- **`pk_hash`** is computed from `OLD.id` (the deleted row's primary key)

### Phase 2–4: Scheduler, Frontier, Change Detection

Identical to the INSERT flow. The scheduler detects one change row in the LSN window.

### Phase 5: Scan Differentiation — Pure DELETE

Unlike UPDATE (which splits into D+I), a DELETE produces a **single event**:

```
__pgt_row_id | __pgt_action | customer | amount
-------------|--------------|----------|-------
4521038      | D            | alice    | 30.00
```

The scan delta applies the net-effect filtering rule:
- `first_action = 'D'` → row existed before the refresh window
- `last_action = 'D'` → row does not exist after

Result: emit a DELETE using old values. **No INSERT is emitted** (because `last_action = 'D'`).

This is the simplest path through the scan delta — one change, one PK, one DELETE event.

### Phase 5 (continued): Aggregate Differentiation

The aggregate operator processes the DELETE event against the `alice` group:

```sql
-- DELETE event: subtract old values from alice's group
__ins_count = 0         -- no inserts
__del_count = 1         -- one deletion
__ins_total = 0         -- no amount added
__del_total = 30.00     -- 30.00 removed
```

The merge CTE joins this delta with the existing stream table state:

```
new_count = old_count + ins_count - del_count = 2 + 0 - 1 = 1  (still > 0)
```

Since `new_count > 0` and the group already existed (`old_count = 2`), the action is classified as **'U' (update)**. The aggregate emits the group with its new values:

```
customer | total | order_count | __pgt_row_id | __pgt_action
---------|-------|-------------|--------------|-------------
alice    | 50.00 | 1           | 7283194      | I
```

Note: the 'U' meta-action is emitted as `__pgt_action = 'I'` because the MERGE treats it as an update-via-INSERT (see aggregate final CTE: `CASE WHEN __pgt_meta_action = 'D' THEN 'D' ELSE 'I' END`).

### Phase 6: MERGE

The MERGE statement matches alice's existing row and updates it:

```sql
MERGE INTO customer_totals AS st
USING (...delta...) AS d
ON st.__pgt_row_id = d.__pgt_row_id
WHEN MATCHED AND d.__pgt_action = 'I' THEN
  UPDATE SET customer = d.customer, total = d.total, order_count = d.order_count, ...
```

Result:

```sql
SELECT * FROM customer_totals;
 customer | total  | order_count
----------|--------|------------
 alice    | 50.00  | 1            ← was 80.00 / 2
 bob      | 100.00 | 2
```

### Phase 7: Cleanup

The change buffer rows in the consumed LSN window are deleted:

```sql
DELETE FROM pgtrickle_changes.changes_16384
WHERE lsn > '0/1A3F2FFF'::pg_lsn AND lsn <= '0/1A3F3000'::pg_lsn;
```

---

## Case 2: Delete Last Row in Group (Group Vanishes)

```sql
-- Alice has one order left (id=1, amount=50.00). Delete it.
DELETE FROM orders WHERE id = 1;
```

### Trigger Capture

```
change_id | lsn         | action | old_cust | old_amt | pk_hash
6         | 0/1A3F3100  | D      | alice    | 50.00   | -837291
```

### Scan Delta

Single DELETE event:

```
__pgt_row_id | __pgt_action | customer | amount
-------------|--------------|----------|-------
-837291      | D            | alice    | 50.00
```

### Aggregate Delta

```
Group "alice":
  ins_count = 0
  del_count = 1
  new_count = old_count + 0 - 1 = 1 - 1 = 0  → group vanishes!
```

When `new_count` drops to 0 (or below), the aggregate classifies this as action **'D' (delete)**. The reference count has reached zero — no rows contribute to this group anymore.

The aggregate emits a DELETE for alice's group:

```
customer | __pgt_row_id | __pgt_action
---------|--------------|-------------
alice    | 7283194      | D
```

### MERGE

The MERGE matches alice's existing row and **deletes** it:

```sql
WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE
```

Result:

```sql
SELECT * FROM customer_totals;
 customer | total  | order_count
----------|--------|------------
 bob      | 100.00 | 2
```

Alice's row is completely removed from the stream table. This is the correct behavior — with zero contributing rows, the group should not exist.

---

## Case 3: Delete Multiple Rows (Same Group, Same Window)

```sql
-- Delete both of bob's orders before the next refresh
DELETE FROM orders WHERE id = 3;  -- bob, 75.00
DELETE FROM orders WHERE id = 4;  -- bob, 25.00
```

The change buffer has two rows with different pk_hash values (different PKs):

```
change_id | action | old_cust | old_amt | pk_hash
7         | D      | bob      | 75.00   | pk_hash_3
8         | D      | bob      | 25.00   | pk_hash_4
```

### Scan Delta

Each PK has exactly one change, so both take the single-change fast path:

```
__pgt_row_id | __pgt_action | customer | amount
-------------|--------------|----------|-------
pk_hash_3    | D            | bob      | 75.00
pk_hash_4    | D            | bob      | 25.00
```

Two DELETE events, both targeting bob's group.

### Aggregate Delta

The aggregate sums both deletions:

```
Group "bob":
  ins_count = 0
  del_count = 2
  del_total = 75.00 + 25.00 = 100.00
  new_count = 2 + 0 - 2 = 0  → group vanishes!
```

The aggregate emits a DELETE for bob's group.

### MERGE

Bob's row is deleted from the stream table. With both alice and bob gone (from Cases 1+2+3), the stream table is now empty.

---

## Case 4: INSERT + DELETE in Same Window (Cancellation)

What if a row is inserted and then deleted before the next refresh?

```sql
INSERT INTO orders (customer, amount) VALUES ('charlie', 200.00);
DELETE FROM orders WHERE customer = 'charlie';
```

The change buffer has:

```
change_id | action | new_cust | new_amt | old_cust | old_amt | pk_hash
9         | I      | charlie  | 200.00  | NULL     | NULL    | pk_hash_new
10        | D      | NULL     | NULL    | charlie  | 200.00  | pk_hash_new
```

### Net-Effect Computation

Both changes share the same pk_hash. The pk_stats CTE finds `cnt = 2`, so this goes through the multi-change path:

```sql
first_action = FIRST_VALUE(action) OVER (...) → 'I'
last_action  = LAST_VALUE(action)  OVER (...) → 'D'
```

The scan delta applies the net-effect filtering:
- **DELETE branch**: requires `first_action != 'I'` → FAILS (first_action = 'I')
- **INSERT branch**: requires `last_action != 'D'` → FAILS (last_action = 'D')

**Result: zero events emitted.** The INSERT and DELETE completely cancel each other out.

The aggregate never sees charlie. The stream table is unchanged. This is correct — the row was born and died within the same refresh window, so it should have no visible effect.

---

## Case 5: UPDATE + DELETE in Same Window

```sql
UPDATE orders SET amount = 999.99 WHERE id = 3;  -- bob: 75 → 999.99
DELETE FROM orders WHERE id = 3;
```

The change buffer:

```
change_id | action | old_amt | new_amt
11        | U      | 75.00   | 999.99
12        | D      | 999.99  | NULL
```

### Net-Effect Computation

Same pk_hash, `cnt = 2`:

```
first_action = 'U'  (row existed before this window)
last_action  = 'D'  (row no longer exists)
```

Filtering:
- **DELETE branch**: `first_action != 'I'` → OK. Emit DELETE with old values from the **earliest** change: `old_amt = 75.00`
- **INSERT branch**: `last_action != 'D'` → FAILS. No INSERT emitted.

Net delta:

```
__pgt_row_id | __pgt_action | amount
-------------|--------------|-------
pk_hash_3    | D            | 75.00
```

The intermediate value of 999.99 never appears. The aggregate sees only the removal of the **original** value (75.00), which is correct — that's the value that was previously accounted for in the stream table.

---

## Case 6: DELETE with JOINs

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

Seed data:

```sql
INSERT INTO customers VALUES (1, 'alice', 'premium'), (2, 'bob', 'standard');
INSERT INTO orders VALUES (1, 1, 50.00), (2, 1, 30.00), (3, 2, 75.00);
```

After refresh, the stream table has:

```
name  | tier     | amount
------|----------|-------
alice | premium  | 50.00
alice | premium  | 30.00
bob   | standard | 75.00
```

Now delete an order:

```sql
DELETE FROM orders WHERE id = 2;  -- alice's 30.00 order
```

### How the JOIN Delta Works

The join differentiation formula:

$$\Delta(L \bowtie R) = (\Delta L \bowtie R) \cup (L \bowtie \Delta R) - (\Delta L \bowtie \Delta R)$$

Since only the `orders` table changed:
- $\Delta L$ = changes to orders (one DELETE: order #2)
- $\Delta R$ = changes to customers (empty)

So:
- **Part 1**: $\Delta\text{orders} \bowtie \text{customers}$ = the deleted order joined with its customer
- **Part 2**: $\text{orders} \bowtie \Delta\text{customers}$ = empty (no customer changes)
- **Part 3**: $\Delta\text{orders} \bowtie \Delta\text{customers}$ = empty (customers unchanged)

Part 1 produces:
```
name  | tier    | amount | __pgt_action
------|---------|--------|-------------
alice | premium | 30.00  | D
```

The deleted order is joined with alice's customer record to produce a DELETE delta row with the complete joined values.

### MERGE

The MERGE matches the row `(alice, premium, 30.00)` and deletes it:

```sql
SELECT * FROM order_details;
 name  | tier     | amount
-------|----------|-------
 alice | premium  | 50.00      ← alice's remaining order
 bob   | standard | 75.00
```

### What About Deleting From the Dimension Table?

```sql
DELETE FROM customers WHERE id = 2;  -- remove bob entirely
```

Now $\Delta R$ has a DELETE for bob, while $\Delta L$ is empty:

- **Part 1**: $\Delta\text{orders} \bowtie \text{customers}$ = empty
- **Part 2**: $\text{orders} \bowtie \Delta\text{customers}$ = bob's order(s) joined with deleted customer record

Part 2 produces DELETE events for every order that referenced bob:

```
name | tier     | amount | __pgt_action
-----|----------|--------|-------------
bob  | standard | 75.00  | D
```

After MERGE, bob's rows vanish from the stream table.

> **Note**: This assumes referential integrity — if `orders` still references customer #2, a foreign key constraint would prevent the DELETE in practice. But from the IVM perspective, the join delta correctly handles the removal regardless.

---

## Case 7: Bulk DELETE

```sql
DELETE FROM orders WHERE amount < 50.00;
```

This deletes multiple rows across potentially multiple groups. The statement-level trigger fires once for the entire DELETE, reading all deleted rows from the `__pgt_old` transition table and writing one change buffer entry per deleted row:

```
change_id | action | old_cust | old_amt | pk_hash
13        | D      | alice    | 30.00   | pk_hash_2
14        | D      | bob      | 25.00   | pk_hash_4
```

### Scan Delta

Each deleted PK is independent (different pk_hash values), so each takes the single-change fast path. Two DELETE events:

```
__pgt_row_id | __pgt_action | customer | amount
-------------|--------------|----------|-------
pk_hash_2    | D            | alice    | 30.00
pk_hash_4    | D            | bob      | 25.00
```

### Aggregate Delta

The aggregate groups these by customer:

```
Group "alice":
  del_count = 1, del_total = 30.00
  new_count = 2 - 1 = 1  (survives)

Group "bob":
  del_count = 1, del_total = 25.00
  new_count = 2 - 1 = 1  (survives)
```

Both groups survive (count > 0), so the aggregate emits UPDATE (as 'I') events with new values:

```
customer | total | order_count
---------|-------|------------
alice    | 50.00 | 1
bob      | 75.00 | 1
```

The MERGE updates both rows. **All work is proportional to the number of deleted rows (2), not the total table size.**

---

## Case 8: TRUNCATE (Automatic Full Refresh)

```sql
TRUNCATE orders;
```

`TRUNCATE` does **not** fire row-level triggers. However, pg_trickle installs a **statement-level AFTER TRUNCATE** trigger that writes a `'T'` marker to the change buffer. On the next refresh cycle, the scheduler detects this marker and automatically performs a **full refresh** — truncating the stream table and recomputing from the defining query.

No manual intervention is required. For details on how TRUNCATE is handled across all three refresh modes (DIFFERENTIAL, IMMEDIATE, FULL), see [What Happens When You TRUNCATE a Table?](WHAT_HAPPENS_ON_TRUNCATE.md).

---

## How DELETE Differs From INSERT and UPDATE — A Summary

| Aspect | INSERT | UPDATE | DELETE |
|--------|--------|--------|--------|
| **Trigger writes** | `new_*` columns only | Both `new_*` and `old_*` | `old_*` columns only |
| **new_* columns** | Row values | New values | NULL |
| **old_* columns** | NULL | Old values | Row values |
| **pk_hash source** | `NEW.pk` | `NEW.pk` | `OLD.pk` |
| **Scan delta output** | 1 INSERT event | 2 events (D+I split) | 1 DELETE event |
| **Aggregate effect** | Adds to group count/sum | Subtracts old, adds new | Subtracts from group |
| **Can delete a group?** | No (only creates/grows) | Yes (if group key changes) | Yes (if count reaches 0) |
| **MERGE action** | INSERT new row | UPDATE existing row | DELETE matched row |

---

## The Reference Counting Principle

The core insight behind incremental DELETE handling is **reference counting**. Every aggregate group in the stream table maintains an internal counter (`__pgt_count`) that tracks how many source rows contribute to the group:

```
Stream table internal state:
customer | total | order_count | __pgt_count (hidden)
---------|-------|-------------|---------------------
alice    | 80.00 | 2           | 2
bob      | 100.00| 2           | 2
```

- **INSERT** → `__pgt_count += 1`
- **DELETE** → `__pgt_count -= 1`
- **UPDATE** → `__pgt_count += 0` (D cancels I for same-group updates)

When `__pgt_count` reaches 0:
- The group has zero contributing rows
- The aggregate emits a DELETE event
- The MERGE removes the row from the stream table

This is mathematically rigorous — the stream table always reflects the correct result of the defining query over the current base table contents, incrementally maintained through algebraic delta operations.

---

## Performance Summary

| Scenario | Buffer rows | Delta rows emitted | Work |
|----------|------------|-------------------|------|
| Single row DELETE (group survives) | 1 | 1 (D) | O(1) per group |
| Single row DELETE (group vanishes) | 1 | 1 (D) | O(1) |
| N deletes same group | N | N (D) → 1 group delta | O(N) scan, O(1) per group |
| INSERT+DELETE same window | 2 | 0 (cancels) | O(1) |
| UPDATE+DELETE same window | 2 | 1 (D original) | O(1) |
| Bulk DELETE across M groups | N | N (D) → M group deltas | O(N) scan, O(M) aggregate |
| JOIN table DELETE | 1 | K (one per matched join row) | O(K) join |

In all cases, the work is proportional to the number of **changed rows**, not the total table size. Deleting 3 rows from a billion-row table produces the same delta cost as from a 10-row table.

---

## What About IMMEDIATE Mode?

Everything above describes **DIFFERENTIAL** mode — changes accumulate in a buffer and are applied on a schedule. pg_trickle also supports **IMMEDIATE** mode, where the stream table is updated synchronously within the same transaction as your DELETE.

### How IMMEDIATE Mode Differs for DELETE

| Phase | DIFFERENTIAL | IMMEDIATE |
|-------|-------------|----------|
| **Trigger type** | Row-level AFTER trigger | Statement-level AFTER trigger with `REFERENCING OLD TABLE` |
| **What's captured** | One buffer row with old_* columns per deleted row | A transition table containing all deleted rows |
| **When delta runs** | Next scheduler tick | Immediately, in the same transaction |
| **Delta source** | Change buffer rows with action='D' | Temp table copied from transition table |
| **Concurrency** | No locking between writers | Advisory lock per stream table |

When you run `DELETE FROM orders WHERE id = 2`:

1. A **BEFORE DELETE** trigger acquires an advisory lock on the stream table
2. The **AFTER DELETE** trigger captures `OLD TABLE AS __pgt_oldtable` into a temp table
3. The DVM engine generates the same aggregate delta, reading deleted values from the old-table
4. The delta is applied to the stream table immediately — groups are decremented, and groups reaching count=0 are removed
5. Any query within the same transaction sees the updated stream table

```sql
BEGIN;
DELETE FROM orders WHERE id = 2;  -- alice's 30.00 order
-- customer_totals already reflects the deletion here!
SELECT * FROM customer_totals WHERE customer = 'alice';
-- Shows: alice | 50.00 | 1
COMMIT;
```

The same reference counting, group deletion, and net-effect logic applies — the only difference is the data source (transition tables vs change buffer) and timing (synchronous vs scheduled).

---

## Next in This Series

- **[What Happens When You INSERT a Row?](WHAT_HAPPENS_ON_INSERT.md)** — The full 7-phase lifecycle (start here if you haven't already)
- **[What Happens When You UPDATE a Row?](WHAT_HAPPENS_ON_UPDATE.md)** — D+I split, group key changes, net-effect for multiple UPDATEs
- **[What Happens When You TRUNCATE a Table?](WHAT_HAPPENS_ON_TRUNCATE.md)** — Why TRUNCATE bypasses triggers and how to recover
