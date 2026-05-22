# Inbox Pattern

The managed transactional inbox API (`create_inbox`, inbox DLQ helpers,
consumer partitioning helpers, and inbox retention GUCs) moved to the
standalone [`pg_tide`](https://github.com/trickle-labs/pg-tide) extension.
Current pg_trickle does not expose `pgtrickle.create_inbox()` or
`pg_trickle.inbox_*` GUCs.

You can still use pg_trickle to build the inbox pattern directly: store incoming
messages in a normal PostgreSQL table, then define stream tables over that table
for pending work, dead letters, and operational statistics.

---

## Manual Inbox with Stream Tables

### 1. Create the raw inbox table

```sql
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE app.order_events (
    event_id     text PRIMARY KEY,
    event_type   text NOT NULL,
    aggregate_id text NOT NULL,
    payload      jsonb NOT NULL,
    received_at  timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz,
    retry_count  int NOT NULL DEFAULT 0,
    last_error   text
);
```

External producers should insert with idempotency protection:

```sql
INSERT INTO app.order_events (event_id, event_type, aggregate_id, payload)
VALUES ('evt-001', 'order.placed', 'ORD-123', '{"amount": 49.99}'::jsonb)
ON CONFLICT (event_id) DO NOTHING;
```

### 2. Create pending and DLQ stream tables

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'app.order_events_pending',
    query    => $$
        SELECT event_id, event_type, aggregate_id, payload, received_at, retry_count
        FROM app.order_events
        WHERE processed_at IS NULL AND retry_count < 3
    $$,
    schedule => '1s'
);

SELECT pgtrickle.create_stream_table(
    name     => 'app.order_events_dlq',
    query    => $$
        SELECT event_id, event_type, aggregate_id, payload, retry_count, last_error
        FROM app.order_events
        WHERE processed_at IS NULL AND retry_count >= 3
    $$,
    schedule => '1s'
);
```

### 3. Create live inbox metrics

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'app.order_events_stats',
    query    => $$
        SELECT event_type,
               COUNT(*) FILTER (WHERE processed_at IS NULL AND retry_count < 3) AS pending,
               COUNT(*) FILTER (WHERE processed_at IS NULL AND retry_count >= 3) AS dlq,
               COUNT(*) FILTER (WHERE processed_at IS NOT NULL) AS processed
        FROM app.order_events
        GROUP BY event_type
    $$,
    schedule => '5s'
);
```

### 4. Process messages transactionally

```sql
BEGIN;

SELECT event_id, payload
FROM app.order_events_pending
ORDER BY received_at
LIMIT 100
FOR UPDATE SKIP LOCKED;

-- Perform application work here.

UPDATE app.order_events
SET processed_at = now()
WHERE event_id = 'evt-001'
  AND processed_at IS NULL;

COMMIT;
```

If processing fails, increment `retry_count` and store `last_error` in the same
transaction. The pending, DLQ, and stats stream tables update on the next
refresh cycle.

---

## When to Use pg_tide Instead

Use pg_tide when you want managed inbox lifecycle, retention, relay integration,
consumer partitioning helpers, or a packaged DLQ workflow. Use the manual
pg_trickle pattern when you want a small SQL-native inbox table and can manage
processing semantics in application code.

---

## See Also

- [Patterns](PATTERNS.md#pattern-8-transactional-inbox)
- [SQL Reference](SQL_REFERENCE.md#pg_tide-outbox-integration)
- [pg_tide + DuckLake tutorial](tutorial-pg-tide-ducklake-pipeline.md)
