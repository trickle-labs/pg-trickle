# pg_tide Outbox Integration

> **Status: Stable** — The `attach_outbox`, `detach_outbox`, and `attach_embedding_outbox` APIs are stable. The full outbox consumer pipeline is in [pg_tide](https://github.com/trickle-labs/pg-tide).

pg_trickle no longer implements the full transactional outbox, consumer-group,
inbox, or relay stack itself. That functionality moved to the standalone
[`pg_tide`](https://github.com/trickle-labs/pg-tide) extension in v0.46.0.

What remains in pg_trickle is the integration point that publishes stream-table
refresh summaries into a pg_tide outbox in the same transaction as the refresh.

---

## What pg_trickle Provides

pg_trickle exposes three outbox-related SQL functions:

| Function | Purpose |
|----------|---------|
| `pgtrickle.attach_outbox(name, retention_hours, inline_threshold_rows)` | Create or register a pg_tide outbox for a stream table |
| `pgtrickle.detach_outbox(name, if_exists)` | Remove the pg_trickle mapping without dropping pg_tide storage |
| `pgtrickle.attach_embedding_outbox(name, vector_column, retention_hours, inline_threshold_rows)` | Attach an outbox whose events are tagged as embedding changes |

After an outbox is attached, every non-empty refresh calls
`tide.outbox_publish()` inside the refresh transaction. If the refresh rolls
back, the outbox event rolls back too.

pg_trickle does not expose `poll_outbox`, `commit_offset`, consumer groups,
leases, inboxes, or relay configuration. Use pg_tide for those APIs.

---

## Quickstart

### 1. Install pg_tide

```sql
CREATE EXTENSION pg_tide;
```

`attach_outbox()` checks for `tide.outbox_create(text, integer, integer)` and
raises a clear error if pg_tide is missing.

### 2. Create a stream table

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'public.order_totals',
    query    => $$
        SELECT customer_id, SUM(amount) AS total
        FROM orders
        GROUP BY customer_id
    $$,
    schedule => '5s'
);
```

### 3. Attach an outbox

```sql
SELECT pgtrickle.attach_outbox(
    p_name                  => 'public.order_totals',
    p_retention_hours       => 48,
    p_inline_threshold_rows => 10000
);
```

This creates the corresponding pg_tide outbox via `tide.outbox_create()` and
records the mapping in `pgtrickle.pgt_outbox_config`.

### 4. Consume with pg_tide

Use pg_tide's polling, relay, consumer, and retention APIs to consume the
outbox. pg_trickle only publishes the event envelope.

---

## Event Envelope

The standard stream-table outbox payload is a compact refresh summary:

```json
{
  "v": 1,
  "refresh_id": "...",
  "inserted": 12,
  "updated": 4,
  "deleted": 3,
  "source": "public.order_totals"
}
```

Headers include:

```json
{
  "source": "public.order_totals",
  "version": 1
}
```

The payload reports counts, not full row data. Consumers that need row-level
changes need a separate CDC feed; pg_trickle's attached outbox event does not
include changed rows.

---

## Embedding Outbox

Vector pipelines can tag events as embedding changes:

```sql
SELECT pgtrickle.attach_embedding_outbox(
    p_name          => 'public.product_embeddings',
    p_vector_column => 'embedding'
);
```

Embedding events add `event_type = 'embedding_change'` and `vector_column` to
the payload and headers, making downstream routing simpler.

---

## Detaching

```sql
SELECT pgtrickle.detach_outbox('public.order_totals', p_if_exists => true);
```

Detaching removes only the row from `pgtrickle.pgt_outbox_config`. It does not
drop pg_tide's outbox table or delete published messages. Use pg_tide's storage
management APIs for that cleanup.

---

## Catalog

`pgtrickle.pgt_outbox_config` stores one row per attached stream table:

| Column | Type | Description |
|--------|------|-------------|
| `stream_table_oid` | `oid` | Stream table OID |
| `stream_table_name` | `text` | Qualified stream table name |
| `tide_outbox_name` | `text` | pg_tide outbox name |
| `embedding_vector_column` | `text` | Optional vector column for embedding events |
| `created_at` | `timestamptz` | Attachment time |

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `attach_outbox() requires the pg_tide extension` | pg_tide is not installed in this database | Run `CREATE EXTENSION pg_tide;` |
| `outbox already enabled` | The stream table already has a mapping | Use `detach_outbox()` first if you need to recreate it |
| No events appear in pg_tide | Refreshes are empty, the stream table is suspended, or the outbox was detached | Check `pgtrickle.pgt_outbox_config`, `pgtrickle.pgt_status()`, and refresh history |
| Old `enable_outbox()` examples fail | They refer to the pre-v0.46.0 pg_trickle API | Use `attach_outbox()` and pg_tide's current outbox API |

---

## See Also

- [SQL Reference](SQL_REFERENCE.md#pg_tide-outbox-integration)
- [pg_tide + DuckLake tutorial](tutorial-pg-tide-ducklake-pipeline.md)
- [Error Reference](ERRORS.md#outboxalreadyenabled)
