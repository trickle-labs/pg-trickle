# Transactional Inbox

The **transactional inbox pattern** solves the duplicate-processing problem:
when messages arrive from an external system, your service needs a guarantee
that each message is processed exactly once, even if the message broker
delivers it more than once or your service restarts mid-batch.

pg_trickle's inbox works by writing incoming messages to a PostgreSQL table and
using stream tables to present live views of pending work, dead letters, and
per-type statistics. Because the inbox table is ordinary PostgreSQL, your
application's processing step and the "mark as processed" step can be wrapped
in a single transaction — making the entire operation atomic.

---

## How it works

```
External system (Kafka / NATS / webhook / custom consumer)
        │
        ▼
  INSERT into pgtrickle.<inbox_name>
  (idempotent: ON CONFLICT DO NOTHING on event_id)
        │
        ▼
  Stream tables refresh automatically:
  ├─ <inbox_name>_pending   ← WHERE processed_at IS NULL AND retry_count < max_retries
  ├─ <inbox_name>_dlq       ← WHERE processed_at IS NULL AND retry_count >= max_retries
  └─ <inbox_name>_stats     ← GROUP BY event_type (counts)
        │
        ▼
  Your application queries <inbox_name>_pending,
  processes each message, then:
  UPDATE <inbox_name> SET processed_at = now() WHERE event_id = $1
```

The stream tables are differential: when a row's `processed_at` is set, the
change propagates to `_pending` and `_stats` in the next refresh cycle
(typically within 1 second).

---

## Quickstart

### 1. Create an inbox

```sql
SELECT pgtrickle.create_inbox('order_events');
```

This creates:
- `pgtrickle.order_events` — the inbox table (one row per message)
- `pgtrickle.order_events_pending` — stream table: unprocessed messages
- `pgtrickle.order_events_dlq` — stream table: messages that exhausted retries
- `pgtrickle.order_events_stats` — stream table: per-event-type counts

### 2. Write messages (sender side)

The inbox table has a standard schema:

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | TEXT PK | Globally unique message ID (idempotency key) |
| `event_type` | TEXT | Message type / topic (e.g. `order.placed`) |
| `source` | TEXT | Originating system or service |
| `aggregate_id` | TEXT | Business entity ID (e.g. order ID) |
| `payload` | JSONB | Message body |
| `received_at` | TIMESTAMPTZ | Set to `now()` on insert |
| `processed_at` | TIMESTAMPTZ | Set by your application after processing |
| `error` | TEXT | Last error message, if any |
| `retry_count` | INT | Number of failed attempts |
| `trace_id` | TEXT | Distributed trace ID for observability |

Write messages with conflict protection to guarantee idempotency:

```sql
INSERT INTO pgtrickle.order_events
    (event_id, event_type, source, aggregate_id, payload)
VALUES
    ('evt-001', 'order.placed', 'shop-api', 'ORD-123', '{"amount": 49.99}')
ON CONFLICT (event_id) DO NOTHING;
```

### 3. Process messages (receiver side)

```sql
-- Read pending messages
SELECT event_id, event_type, aggregate_id, payload
FROM pgtrickle.order_events_pending
LIMIT 100;
```

Process each message in a transaction:

```sql
BEGIN;

-- Do your business logic here
-- (e.g. publish to downstream service, update application tables)

-- Mark as processed atomically with your business logic
UPDATE pgtrickle.order_events
SET processed_at = now()
WHERE event_id = 'evt-001';

COMMIT;
```

If the transaction rolls back, `processed_at` stays NULL and the message
remains in `_pending` for retry.

---

## Using an existing table (bring-your-own-table)

If you already have a messages table, point pg_trickle at it instead of
creating a new one:

```sql
SELECT pgtrickle.enable_inbox_tracking(
    'my_inbox',                          -- logical name
    'app.incoming_events',               -- your existing table
    p_id_column          => 'msg_id',
    p_processed_at_column => 'done_at',
    p_event_type_column  => 'type'
);
```

pg_trickle validates that the required columns exist, then creates the standard
stream tables on top of your table. The underlying table is not modified.

---

## Ordering guarantees (per-aggregate)

By default, multiple workers can process messages for the same `aggregate_id`
concurrently. If your business logic requires strictly sequential processing per
aggregate (e.g. events for the same order must be handled in order), enable
ordering:

```sql
SELECT pgtrickle.enable_inbox_ordering(
    'order_events',
    p_aggregate_column => 'aggregate_id',
    p_sequence_column  => 'received_at'
);
```

This creates a fourth stream table:

- `pgtrickle.next_order_events` — one row per `aggregate_id`, always the
  _next_ unprocessed message for that aggregate (DISTINCT ON semantics)

Workers that need ordered processing should query `next_order_events` instead
of `order_events_pending`:

```sql
-- Only the next message per aggregate — safe for parallel workers
SELECT event_id, event_type, aggregate_id, payload
FROM pgtrickle.next_order_events
LIMIT 50;
```

A worker processing `aggregate_id = 'ORD-123'` blocks any other message for
that order until it commits. Different aggregates are processed in parallel.

### Checking for ordering gaps

```sql
-- Returns aggregate IDs where messages are out of sequence or missing
SELECT * FROM pgtrickle.inbox_ordering_gaps('order_events');
```

---

## Priority processing

If some message types should be processed before others, enable priority
scheduling:

```sql
SELECT pgtrickle.enable_inbox_priority(
    'order_events',
    p_priority_column => 'event_type',
    p_priority_map    => '{"order.cancelled": 1, "order.placed": 2, "order.shipped": 3}'::jsonb
);
```

Lower priority values are processed first. Messages without an entry in the
priority map default to priority 999 (processed last).

---

## Multi-worker partitioning

When many workers process the same inbox concurrently, you can partition the
workload by aggregate ID using consistent hashing:

```sql
-- Worker 0 of 4: only process messages assigned to partition 0
SELECT event_id, aggregate_id, payload
FROM pgtrickle.order_events_pending
WHERE pgtrickle.inbox_is_my_partition('order_events', aggregate_id, 0, 4);

-- Worker 1 of 4
SELECT event_id, aggregate_id, payload
FROM pgtrickle.order_events_pending
WHERE pgtrickle.inbox_is_my_partition('order_events', aggregate_id, 1, 4);
```

The hash function is deterministic — the same `aggregate_id` always maps to
the same partition — so you can scale the worker pool without rebalancing.

---

## Dead-letter queue

Messages that exceed `max_retries` (default: 3) are automatically visible in
the DLQ stream table:

```sql
-- View dead letters
SELECT event_id, event_type, aggregate_id, error, retry_count
FROM pgtrickle.order_events_dlq
ORDER BY received_at;
```

### Replaying DLQ messages

After fixing the root cause:

```sql
-- Reset retry count so the message is picked up again
SELECT pgtrickle.replay_inbox_messages(
    'order_events',
    p_event_ids => ARRAY['evt-001', 'evt-002']
);

-- Or replay all DLQ messages of a specific type
SELECT pgtrickle.replay_inbox_messages(
    'order_events',
    p_event_type => 'order.placed'
);
```

---

## Monitoring

### Health check

```sql
SELECT pgtrickle.inbox_health('order_events');
```

Returns a JSONB object:

```json
{
  "inbox": "order_events",
  "pending_count": 42,
  "dlq_count": 3,
  "oldest_pending_age_seconds": 12,
  "throughput_per_minute": 180,
  "status": "healthy"
}
```

A `status` of `"degraded"` means the DLQ count or pending age is above
configured thresholds.

### Detailed status

```sql
SELECT pgtrickle.inbox_status('order_events');
```

Returns richer JSONB including processing rates, error breakdown, and stream
table refresh counts.

### Global inbox overview

```sql
SELECT * FROM pgtrickle.pgt_inbox_config;
```

---

## Catalog tables

| Table | Contents |
|-------|---------|
| `pgtrickle.pgt_inbox_config` | One row per inbox: name, schema, max_retries, schedule |
| `pgtrickle.pgt_inbox_ordering_config` | Ordering settings per inbox |
| `pgtrickle.pgt_inbox_priority_config` | Priority map per inbox |
| `pgtrickle.<name>` | The inbox message table (auto-created) |
| `pgtrickle.<name>_pending` | Stream table: unprocessed messages |
| `pgtrickle.<name>_dlq` | Stream table: dead letters |
| `pgtrickle.<name>_stats` | Stream table: per-event-type counts |
| `pgtrickle.next_<name>` | Stream table: next message per aggregate (ordering only) |

---

## Retention and cleanup

Processed messages are automatically deleted after `inbox_processed_retention_hours`
(default: 72). DLQ rows are held for `inbox_dlq_retention_hours` (default: 168
= 7 days) to give operators time to inspect and replay them.

Configure globally in `postgresql.conf`:

```
pg_trickle.inbox_processed_retention_hours = 72
pg_trickle.inbox_dlq_retention_hours = 168
```

---

## Dropping an inbox

```sql
-- Drop the inbox and its stream tables, but keep the underlying table
SELECT pgtrickle.drop_inbox('order_events');

-- Drop everything including the backing table
SELECT pgtrickle.drop_inbox('order_events', p_cascade => true);
```

---

## Recommended configuration

| GUC | Recommended value | Notes |
|-----|------------------|-------|
| `pg_trickle.inbox_enabled` | `on` | Must be on for inbox background workers to run |
| `pg_trickle.inbox_processed_retention_hours` | `24`–`72` | Adjust based on audit requirements |
| `pg_trickle.inbox_dlq_retention_hours` | `168` | Keep DLQ items for at least 7 days |
| `pg_trickle.inbox_drain_batch_size` | `500`–`2000` | Tune for throughput vs. latency |
| `pg_trickle.inbox_dlq_alert_max_per_refresh` | `100` | Alert when DLQ grows rapidly |

---

## Anti-patterns

**Do not mark messages as processed outside a transaction with your business
logic.** The atomic combination of "do work + mark processed" is what prevents
duplicate processing. If you process first and then mark processed in a separate
transaction, a crash between the two steps causes duplicate processing.

**Do not share a single inbox across unrelated services.** Each service should
have its own inbox so they can fail, replay, and scale independently.

**Do not ignore the DLQ.** A growing DLQ is a signal that something is
consistently broken. Set up an alert on `inbox_dlq_alert_max_per_refresh` and
review DLQ items regularly.

**Do not delete inbox rows manually.** Let the retention mechanism handle
cleanup. Manual deletes can confuse the stream table refresh cycle.

---

## See also

- [Transactional Outbox](OUTBOX.md) — publish events from your database to external systems
- [SQL Reference: Transactional Inbox](SQL_REFERENCE.md#transactional-inbox-v0280)
- [Configuration](CONFIGURATION.md#transactional-inbox-v0280)
- [Pattern 8: Transactional Inbox](PATTERNS.md#pattern-8-transactional-inbox-v0280)
