# Transactional Outbox

The **transactional outbox pattern** solves the dual-write problem: how to
atomically update your database _and_ publish an event to an external system
without risking inconsistency if one side fails.

pg_trickle's outbox implementation builds on top of stream tables. Every time a
stream table refresh produces a non-empty delta, a summary row is written to an
outbox table **in the same transaction** as the MERGE. Consumers are notified
via `pg_notify` the moment the commit lands.

---

## How it works

```
Source tables (INSERT / UPDATE / DELETE)
        │
        ▼
  CDC trigger fires → pgtrickle_changes buffer
        │
        ▼
  Stream table refresh (MERGE)
        │   ← same transaction ─────────────────────────────┐
        ▼                                                    │
  Delta rows applied to stream table               outbox row written
  (inserted_count / deleted_count recorded)        to pgtrickle.outbox_<st>
                                                             │
                                                    pg_notify fired
                                                             │
                                                    Consumer polls / listens
```

The outbox row is guaranteed to exist if and only if the stream table was
updated. There is no window where the stream table changes but no outbox row
exists, or an outbox row exists but the stream table did not change.

### Inline vs. claim-check mode

| Condition | Mode | What the consumer receives |
|-----------|------|---------------------------|
| `delta_rows ≤ outbox_inline_threshold_rows` (default: 1000) | **Inline** | Full delta serialized as JSONB in `payload` |
| `delta_rows > outbox_inline_threshold_rows` | **Claim-check** | `is_claim_check = true`, payload is NULL; delta rows in `pgtrickle.outbox_delta_rows_<st>` |

Inline mode is simpler — the consumer reads one row and gets everything.
Claim-check mode avoids storing very large payloads in the outbox table, at the
cost of an extra query to fetch the delta rows.

---

## Quickstart

### 1. Create a stream table

```sql
SELECT pgtrickle.create_stream_table(
    'public.order_totals',
    $$SELECT customer_id, SUM(amount) AS total
      FROM orders
      GROUP BY customer_id$$
);
```

### 2. Enable the outbox

```sql
SELECT pgtrickle.enable_outbox('public.order_totals');
```

This creates:
- `pgtrickle.outbox_order_totals` — outbox header table
- `pgtrickle.outbox_delta_rows_order_totals` — claim-check delta rows
- `pgtrickle.pgt_outbox_latest_order_totals` — convenience view pointing to the most recent outbox row

### 3. Create consumer groups

Each independent consumer needs its own group. Groups track their own offset
into the outbox table so they never interfere with each other.

```sql
SELECT pgtrickle.create_consumer_group(
    'shipping_service',
    'public.order_totals'
);

SELECT pgtrickle.create_consumer_group(
    'analytics_pipeline',
    'public.order_totals'
);
```

### 4. Poll for messages

A consumer loop looks like this:

```sql
-- Claim up to 50 unprocessed rows, hold the lease for 30 seconds
SELECT * FROM pgtrickle.poll_outbox(
    'public.order_totals',
    'shipping_service',
    batch_size    => 50,
    lease_seconds => 30
);
```

`poll_outbox` returns outbox rows that this consumer has not yet committed.
Each row is leased — no other worker sharing the same consumer group can claim
it until the lease expires.

### 5. Process and commit

After successfully processing each batch:

```sql
SELECT pgtrickle.commit_offset('shipping_service', 'public.order_totals', last_id);
```

`last_id` is the highest `id` value from the batch you just processed.
Committed rows are never returned by `poll_outbox` again.

---

## Reading the payload

### Inline mode

```sql
SELECT
    id,
    created_at,
    inserted_count,
    deleted_count,
    payload -> 'inserted' AS inserted_rows,
    payload -> 'deleted'  AS deleted_rows
FROM pgtrickle.outbox_order_totals
ORDER BY id DESC
LIMIT 5;
```

### Claim-check mode

```sql
-- Get the outbox row
SELECT id, is_claim_check FROM pgtrickle.pgt_outbox_latest_order_totals;

-- Fetch the actual delta rows for a claim-check outbox row
SELECT row_op, row_data
FROM pgtrickle.outbox_delta_rows_order_totals
WHERE outbox_id = <outbox_id>
ORDER BY row_num;
```

---

## Multiple workers (parallel consumption)

Multiple workers in the same consumer group share the workload. pg_trickle
assigns non-overlapping leases, so each row is processed by exactly one worker
at a time.

```sql
-- Worker 1
SELECT * FROM pgtrickle.poll_outbox('public.order_totals', 'shipping_service');

-- Worker 2 (concurrent, gets a different batch)
SELECT * FROM pgtrickle.poll_outbox('public.order_totals', 'shipping_service');
```

Workers should register their presence so the system can detect dead workers:

```sql
-- Call periodically (e.g. every 30 s) while the worker is alive
SELECT pgtrickle.consumer_heartbeat('shipping_service', 'worker-1');
```

Workers that miss their heartbeat deadline are removed from the consumer group.
Any leases held by a dead worker expire automatically after `lease_seconds`,
returning those rows to the available pool.

---

## Lease management

### Extending a lease

If processing is taking longer than expected:

```sql
SELECT pgtrickle.extend_lease(
    'shipping_service',
    'public.order_totals',
    outbox_id     => 42,
    extra_seconds => 60
);
```

### Seeking to a specific position

For replay or recovery scenarios:

```sql
-- Replay from the beginning
SELECT pgtrickle.seek_offset('shipping_service', 'public.order_totals', 0);

-- Skip ahead to the current tip
SELECT pgtrickle.seek_offset(
    'shipping_service', 'public.order_totals',
    (SELECT MAX(id) FROM pgtrickle.outbox_order_totals)
);
```

---

## Monitoring

### Check outbox health

```sql
SELECT pgtrickle.outbox_status('public.order_totals');
```

Returns JSONB:
```json
{
  "enabled": true,
  "stream_table": "public.order_totals",
  "outbox_table": "pgtrickle.outbox_order_totals",
  "row_count": 1247,
  "oldest_row": "2025-04-20T10:00:00Z",
  "newest_row": "2025-04-23T14:32:00Z",
  "retention_hours": 24
}
```

### Consumer lag

```sql
-- Per consumer group
SELECT pgtrickle.consumer_lag('shipping_service', 'public.order_totals');
```

Returns the number of outbox rows that the consumer group has not yet committed.
A large or growing lag means the consumer is falling behind.

### Global outbox overview

```sql
SELECT * FROM pgtrickle.pgt_outbox_config;
```

---

## Catalog tables

| Table | Contents |
|-------|---------|
| `pgtrickle.pgt_outbox_config` | One row per enabled outbox: ST OID, outbox table name, retention hours |
| `pgtrickle.pgt_consumer_groups` | One row per consumer group: name, stream table, created_at |
| `pgtrickle.pgt_consumer_offsets` | Per-group committed offsets and lease state |
| `pgtrickle.outbox_<st>` | Outbox header rows (auto-created per stream table) |
| `pgtrickle.outbox_delta_rows_<st>` | Claim-check delta rows (auto-created per stream table) |

---

## Retention and cleanup

Outbox rows are automatically deleted after `outbox_retention_hours` (default:
24). Claim-check delta rows are removed when `commit_offset` is called or when
the retention period expires.

Configure retention per stream table at enable time:

```sql
SELECT pgtrickle.enable_outbox('public.order_totals', p_retention_hours => 48);
```

Or globally in `postgresql.conf`:

```
pg_trickle.outbox_retention_hours = 48
```

---

## Disabling the outbox

```sql
SELECT pgtrickle.disable_outbox('public.order_totals');
```

This drops the outbox table, delta-rows table, and latest view, and removes the
catalog entry. Consumer groups must be dropped separately:

```sql
SELECT pgtrickle.drop_consumer_group('shipping_service', 'public.order_totals');
```

---

## Recommended configuration

| GUC | Recommended value | Notes |
|-----|------------------|-------|
| `pg_trickle.outbox_enabled` | `on` | Must be on for the outbox background worker to run |
| `pg_trickle.outbox_retention_hours` | `24`–`72` | Balance storage cost vs. replay window |
| `pg_trickle.outbox_drain_batch_size` | `500`–`2000` | Larger batches improve throughput |
| `pg_trickle.outbox_inline_threshold_rows` | `500`–`2000` | Tune based on typical delta size |
| `pg_trickle.outbox_skip_empty_delta` | `on` | Skip writing outbox rows when delta is empty |
| `pg_trickle.consumer_cleanup_enabled` | `on` | Auto-remove dead consumer workers |
| `pg_trickle.consumer_dead_threshold_hours` | `1` | Mark worker dead after 1 h of silence |

---

## Anti-patterns

**Do not poll without committing.** If your consumer processes messages but
never calls `commit_offset`, the lag grows unboundedly and messages are
replayed forever after a worker restart.

**Do not use a single consumer group for independent services.** Each service
that needs to process outbox events independently must have its own consumer
group. Sharing a group means one service blocking the other.

**Do not delete outbox rows manually.** Let the retention mechanism handle
cleanup. Manual deletes can cause consumer group offsets to point to
non-existent rows.

**Do not enable the outbox on IMMEDIATE-mode stream tables.** The outbox
requires DIFFERENTIAL or FULL refresh mode to detect which rows changed.

---

## See also

- [Transactional Inbox](INBOX.md) — receive events from external systems
- [SQL Reference: Transactional Outbox](SQL_REFERENCE.md#transactional-outbox--consumer-groups-v0280)
- [Configuration](CONFIGURATION.md#transactional-outbox-v0280)
- [Pattern 7: Transactional Outbox](PATTERNS.md#pattern-7-transactional-outbox-v0280)
