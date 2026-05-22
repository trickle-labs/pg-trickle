# What Happens When You UPDATE a Row?

This tutorial traces what happens when an `UPDATE` statement hits a base table that is referenced by a stream table. It covers the trigger capture, the scan-level decomposition into DELETE + INSERT, and how each DVM operator propagates the change — including cases where the group key changes, where JOINs are involved, and where multiple UPDATEs happen within a single refresh window.

> **Prerequisite:** Read [WHAT_HAPPENS_ON_INSERT.md](WHAT_HAPPENS_ON_INSERT.md) first — it introduces the full 7-phase lifecycle. This tutorial focuses on how UPDATE differs.

## Setup

Same e-commerce example:

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
    ('alice', 49.99),
    ('alice', 30.00),
    ('bob',   75.00);
```

After the first refresh, the stream table contains:

```
customer | total | order_count
---------|-------|------------
alice    | 79.99 | 2
bob      | 75.00 | 1
```

---

## Case 1: Simple Value UPDATE (Same Group Key)

```sql
UPDATE orders SET amount = 59.99 WHERE id = 1;
```

Alice's first order changes from 49.99 to 59.99. The customer (group key) stays the same.

### Phase 1: Trigger Capture

The AFTER UPDATE trigger fires and writes **one row** to the change buffer with both OLD and NEW values:

```
pgtrickle_changes.changes_16384
┌───────────┬─────────────┬────────┬──────────┬──────────┬────────────┬──────────┬────────────┐
│ change_id │ lsn         │ action │ new_cust │ new_amt  │ old_cust   │ old_amt  │ pk_hash    │
├───────────┼─────────────┼────────┼──────────┼──────────┼────────────┼──────────┼────────────┤
│ 4         │ 0/1A3F3000  │ U      │ alice    │ 59.99    │ alice      │ 49.99    │ -837291    │
└───────────┴─────────────┴────────┴──────────┴──────────┴────────────┴──────────┴────────────┘
```

Key difference from INSERT: the trigger writes **both** `new_*` and `old_*` columns. The pk_hash is computed from `NEW.id`.

### Phase 2–4: Scheduler, Frontier, Change Detection

Identical to the INSERT flow. The scheduler detects one change row in the LSN window.

### Phase 5: Scan Differentiation — The U → D+I Split

This is where UPDATE handling diverges fundamentally. The scan delta operator **decomposes the UPDATE into two events**:

```
__pgt_row_id | __pgt_action | customer | amount
-------------|--------------|----------|-------
-837291      | D            | alice    | 49.99     ← old values (DELETE)
-837291      | I            | alice    | 59.99     ← new values (INSERT)
```

**Why split into D+I?** This is a core IVM principle. Downstream operators (aggregates, joins, filters) don't have special "update" logic — they only understand insertions and deletions. By decomposing the UPDATE:

- The **DELETE** event subtracts the old values from running aggregates
- The **INSERT** event adds the new values

This algebraic approach handles arbitrary operator trees without operator-specific update logic.

### Phase 5 (continued): Aggregate Differentiation

The aggregate operator processes both events against the `alice` group:

```sql
-- DELETE event: subtract old values
alice: total += CASE WHEN action='D' THEN -49.99 END  →  -49.99
alice: count += CASE WHEN action='D' THEN -1 END       →  -1

-- INSERT event: add new values
alice: total += CASE WHEN action='I' THEN +59.99 END  →  +59.99
alice: count += CASE WHEN action='I' THEN +1 END       →  +1
```

Net effect on alice's group:

```
total delta:  -49.99 + 59.99 = +10.00
count delta:  -1 + 1 = 0
```

The aggregate emits this as an INSERT (because the group still exists and its value changed):

```
customer | total  | order_count | __pgt_row_id | __pgt_action
---------|--------|-------------|--------------|-------------
alice    | +10.00 | 0           | 7283194      | I
```

### Phase 6: MERGE

The MERGE updates the existing row:

```sql
-- MERGE WHEN MATCHED AND action = 'I' THEN UPDATE:
-- alice's total: 79.99 + 10.00 = 89.99  (via reference counting)
-- alice's count: 2 + 0 = 2
```

Wait — that's not right. The MERGE doesn't add deltas; it **replaces** the row. The aggregate delta query actually computes the **new absolute value** by combining the stored state with the delta:

```sql
COALESCE(existing.total, 0) + delta.total  → 79.99 + 10.00 = 89.99
COALESCE(existing.__pgt_count, 0) + delta.__pgt_count → 2 + 0 = 2
```

Result:

```sql
SELECT * FROM customer_totals;
 customer | total | order_count
----------|-------|------------
 alice    | 89.99 | 2            ← was 79.99
 bob      | 75.00 | 1
```

---

## Case 2: Group Key Change (Customer Reassignment)

```sql
UPDATE orders SET customer = 'bob' WHERE id = 2;
```

Alice's second order (amount=30.00) is reassigned to Bob. The **group key itself changes**.

### Trigger Capture

```
change_id | lsn         | action | new_cust | new_amt | old_cust | old_amt | pk_hash
5         | 0/1A3F3100  | U      | bob      | 30.00   | alice    | 30.00   | 4521038
```

The old and new customer values differ.

### Scan Delta: D+I Split

```
__pgt_row_id | __pgt_action | customer | amount
-------------|--------------|----------|-------
4521038      | D            | alice    | 30.00    ← removes from alice's group
4521038      | I            | bob      | 30.00    ← adds to bob's group
```

### Aggregate Delta

The aggregate groups by `customer`, so the DELETE and INSERT land in **different groups**:

```
Group "alice":
  total delta:  -30.00
  count delta:  -1

Group "bob":
  total delta:  +30.00
  count delta:  +1
```

### After MERGE

```sql
SELECT * FROM customer_totals;
 customer | total  | order_count
----------|--------|------------
 alice    | 59.99  | 1            ← lost one order (-30.00)
 bob      | 105.00 | 2            ← gained one order (+30.00)
```

This is why the D+I decomposition is essential. Without it, you'd need special "move between groups" logic. With it, the standard aggregate differentiation handles group key changes naturally.

---

## Case 3: UPDATE That Deletes a Group

```sql
-- Alice only has one order left. Reassign it to bob.
UPDATE orders SET customer = 'bob' WHERE id = 1;
```

### Aggregate Delta

```
Group "alice":
  total delta:    -59.99
  count delta:    -1
  new __pgt_count: 1 - 1 = 0  → group vanishes!

Group "bob":
  total delta:    +59.99
  count delta:    +1
```

When `__pgt_count` reaches 0, the aggregate emits a **DELETE** for alice's group:

```
customer | total | __pgt_row_id | __pgt_action
---------|-------|--------------|-------------
alice    | —     | 7283194      | D             ← group removed
bob      | ...   | 9182734      | I             ← group updated
```

The MERGE deletes alice's row entirely:

```sql
SELECT * FROM customer_totals;
 customer | total  | order_count
----------|--------|------------
 bob      | 165.00 | 3
```

---

## Case 4: Multiple UPDATEs on the Same Row (Within One Refresh Window)

What if a row is updated multiple times before the next refresh?

```sql
UPDATE orders SET amount = 10.00 WHERE id = 3;  -- bob: 75 → 10
UPDATE orders SET amount = 20.00 WHERE id = 3;  -- bob: 10 → 20
UPDATE orders SET amount = 30.00 WHERE id = 3;  -- bob: 20 → 30
```

The change buffer now has 3 rows for pk_hash of order #3:

```
change_id | action | old_amt | new_amt
6         | U      | 75.00   | 10.00
7         | U      | 10.00   | 20.00
8         | U      | 20.00   | 30.00
```

### Net-Effect Computation

The scan delta uses a **split fast-path** design. Since order #3 has multiple changes (cnt > 1), it takes the multi-change path with window functions:

```sql
FIRST_VALUE(action) OVER (PARTITION BY pk_hash ORDER BY change_id)  → 'U'
LAST_VALUE(action) OVER (...)                                        → 'U'
```

Both first and last actions are 'U', so:
- **DELETE**: emits using old values from the **earliest** change (change_id=6): `old_amt = 75.00`
- **INSERT**: emits using new values from the **latest** change (change_id=8): `new_amt = 30.00`

Net delta:

```
__pgt_row_id | __pgt_action | amount
-------------|--------------|-------
pk_hash_3    | D            | 75.00    ← original value before all changes
pk_hash_3    | I            | 30.00    ← final value after all changes
```

The aggregate sees `-75.00 + 30.00 = -45.00`. This is correct regardless of the intermediate values. The intermediate rows (10.00, 20.00) are never seen.

---

## Case 5: INSERT + UPDATE in Same Window

```sql
INSERT INTO orders (customer, amount) VALUES ('charlie', 100.00);
UPDATE orders SET amount = 200.00 WHERE customer = 'charlie';
```

Both happen before the next refresh. The buffer has:

```
change_id | action | old_amt | new_amt
9         | I      | NULL    | 100.00
10        | U      | 100.00  | 200.00
```

Net-effect analysis:
- `first_action = 'I'` (row didn't exist before this window)
- `last_action = 'U'` (row exists after)

Result:
- **No DELETE** emitted (first_action = 'I' means the row was born in this window)
- **INSERT** with final values: `(charlie, 200.00)`

The aggregate sees a pure insertion of `(charlie, 200.00)` — the intermediate value of 100.00 never appears.

---

## Case 6: UPDATE + DELETE in Same Window

```sql
UPDATE orders SET amount = 999.99 WHERE id = 3;
DELETE FROM orders WHERE id = 3;
```

Net-effect:
- `first_action = 'U'` (row existed before)
- `last_action = 'D'` (row no longer exists)

Result:
- **DELETE** with original old values from the first change
- **No INSERT** (last_action = 'D')

The aggregate correctly sees only a removal.

---

## Case 7: UPDATE with JOINs

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

Now update a customer's tier:

```sql
UPDATE customers SET tier = 'premium' WHERE name = 'alice';
```

### How the JOIN Delta Works

The join differentiation follows the formula:

$$\Delta(L \bowtie R) = (\Delta L \bowtie R) \cup (L \bowtie \Delta R) - (\Delta L \bowtie \Delta R)$$

Since only the `customers` table changed:
- $\Delta L$ = changes to orders (empty)
- $\Delta R$ = changes to customers (alice's tier: standard → premium)

So:
- **Part 1**: $\Delta\text{orders} \bowtie \text{customers}$ = empty (no order changes)
- **Part 2**: $\text{orders} \bowtie \Delta\text{customers}$ = all of alice's orders joined with her tier change
- **Part 3**: $\Delta\text{orders} \bowtie \Delta\text{customers}$ = empty (no order changes)

Part 2 produces the delta: for each of alice's orders, DELETE the old row (with tier='standard') and INSERT a new row (with tier='premium').

The stream table is updated to reflect the new tier across all of alice's order rows.

---

## Performance Summary

| Scenario | Buffer rows | Delta rows emitted | Work |
|----------|------------|-------------------|------|
| Simple value change | 1 | 2 (D+I) | O(1) per group |
| Group key change | 1 | 2 (D+I, different groups) | O(1) per affected group |
| Group deletion | 1 | 1 (D) + 1 (I) or 1 (D) | O(1) |
| N updates same row | N | 2 (D first-old + I last-new) | O(N) scan, O(1) aggregate |
| INSERT+UPDATE same window | 2 | 1 (I only) | O(1) |
| UPDATE+DELETE same window | 2 | 1 (D only) | O(1) |

In all cases, the work is proportional to the number of **changed rows**, not the total table size. A single UPDATE on a billion-row table produces the same delta cost as on a 10-row table.

---

## What About IMMEDIATE Mode?

Everything above describes **DIFFERENTIAL** mode — changes accumulate in a buffer and are applied on a schedule. pg_trickle also supports **IMMEDIATE** mode, where the stream table is updated synchronously within the same transaction as your UPDATE.

### How IMMEDIATE Mode Differs for UPDATE

| Phase | DIFFERENTIAL | IMMEDIATE |
|-------|-------------|----------|
| **Trigger type** | Row-level AFTER trigger | Statement-level AFTER trigger with `REFERENCING OLD TABLE, NEW TABLE` |
| **What's captured** | One buffer row with old_* and new_* | Two transition tables: `__pgt_oldtable` and `__pgt_newtable` |
| **When delta runs** | Next scheduler tick | Immediately, in the same transaction |
| **D+I decomposition** | In the scan delta CTE | Same algebra, but reading from transition temp tables |
| **Concurrency** | No locking between writers | Advisory lock per stream table |

When you run `UPDATE orders SET amount = 59.99 WHERE id = 1`:

1. A **BEFORE UPDATE** trigger acquires an advisory lock on the stream table
2. The **AFTER UPDATE** trigger captures both `OLD TABLE AS __pgt_oldtable` and `NEW TABLE AS __pgt_newtable` into temp tables
3. The DVM engine generates the same D+I decomposition, reading old values from the old-table and new values from the new-table
4. The delta is applied to the stream table immediately
5. Any query within the same transaction sees the updated stream table

```sql
BEGIN;
UPDATE orders SET amount = 59.99 WHERE id = 1;
-- customer_totals already reflects the new amount here!
SELECT * FROM customer_totals WHERE customer = 'alice';
COMMIT;
```

The same D+I split, aggregate differentiation, and net-effect logic applies — the only difference is the data source (transition tables vs change buffer) and timing (synchronous vs scheduled).

---

## Next in This Series

- **[What Happens When You INSERT a Row?](WHAT_HAPPENS_ON_INSERT.md)** — The full 7-phase lifecycle (start here if you haven't already)
- **[What Happens When You DELETE a Row?](WHAT_HAPPENS_ON_DELETE.md)** — Reference counting, group deletion, INSERT+DELETE cancellation
- **[What Happens When You TRUNCATE a Table?](WHAT_HAPPENS_ON_TRUNCATE.md)** — Why TRUNCATE bypasses triggers and how to recover
