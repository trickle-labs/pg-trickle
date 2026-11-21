# INT-10: pg-tide DuckLake Pipeline Tutorial

This integration combines two **separate** paths: pg_trickle can publish refresh events transactionally to a pg_tide outbox, and pg_trickle's DuckLake sink can publish refresh deltas as Parquet files. The outbox does not itself write Parquet: a separate pg_tide consumer or relay must be configured for any additional downstream delivery.

For the authoritative APIs and division of responsibility, see [pg_tide Outbox Integration](OUTBOX.md). For a fully runnable DuckLake sink pipeline without an external relay, see [Tutorial 4](tutorial-streaming-postgres-to-data-lake.md).

## Prerequisites

- PostgreSQL 18 with pg_trickle installed and enabled
- The separate [pg_tide extension](https://github.com/trickle-labs/pg-tide) installed in the same database
- A source table with a primary key, for example an existing `orders` table

## 1. Enable pg_tide

```sql
CREATE EXTENSION IF NOT EXISTS pg_trickle;
CREATE EXTENSION IF NOT EXISTS pg_tide;
```

## 2. Create a stream table

Given `orders(customer_id, amount)`, create an incrementally maintained aggregate:

```sql
SELECT pgtrickle.create_stream_table(
    name => 'public.order_totals',
    query => $$
        SELECT customer_id, SUM(amount) AS total
        FROM orders
        GROUP BY customer_id
    $$,
    schedule => '5s'
);
```

Run this only once in a database where `orders` is already defined.

## 3. Attach a transactional outbox

```sql
SELECT pgtrickle.attach_outbox(
    p_name                  => 'public.order_totals',
    p_retention_hours       => 48,
    p_inline_threshold_rows => 10000
);
```

After each nonempty stream-table refresh, pg_trickle publishes a compact refresh summary through `tide.outbox_publish()` **in the same transaction**. If the refresh rolls back, the outbox event rolls back too.

The event is a refresh notification and counts envelope, **not** the stream-table rows or Parquet bytes. See [the event envelope](OUTBOX.md#event-envelope).

## 4. Consume or relay the events

Use the [pg_tide project](https://github.com/trickle-labs/pg-tide) to configure polling, consumer groups, offsets, and any relay destination. These APIs are owned by pg_tide rather than pg_trickle; do not call `pgtrickle.poll_outbox` or `pgtrickle.commit_offset`.

If the target is **DuckLake Parquet data**, use the separately configured pg_trickle DuckLake sink shown in [Tutorial 4](tutorial-streaming-postgres-to-data-lake.md). A pg_tide event alone is insufficient to materialize a DuckLake snapshot.

## 5. Clean up the integration

```sql
SELECT pgtrickle.detach_outbox('public.order_totals');
```

Detaching the mapping does not drop pg_tide's outbox storage. Consult [OUTBOX.md](OUTBOX.md) and pg_tide's documentation before deleting retained events or consumer state.

For a production workflow, decide explicitly whether you need the transactional event notification, the DuckLake data sink, or both.
