# Tutorial: Stream Tables as Event-Sourced Read Models

> DOC-NEW-25 — End-to-end tutorial: use stream tables as read-model
> projections over an event-sourced write model.  Models an order-processing
> domain with CQRS pattern and event-replay guidance.

## What You Will Build

An event-sourced order-processing system where:

- **Writes** go to an immutable `order_events` table (the event log)
- **Reads** are served by three stream tables (the read models):
  - `current_order_state` — current status of each order
  - `customer_lifetime_value` — rolling spend and order count per customer
  - `inventory_levels` — current stock count derived from reservation events

This is the **CQRS** (Command Query Responsibility Segregation) pattern:
the write model is append-only events; the read models are incrementally
maintained projections.

---

## Prerequisites

- PostgreSQL 18 with pg_trickle installed (see [Installation](../installation.md))
- Basic familiarity with event sourcing concepts

---

## Step 1 — The Event Log (Write Model)

The event log is a single append-only table. Every mutation to an order is
recorded as a row. The table is never updated or deleted from — only new
events are appended.

```sql
CREATE TYPE order_event_type AS ENUM (
    'ORDER_PLACED',
    'PAYMENT_RECEIVED',
    'PAYMENT_FAILED',
    'SHIPPED',
    'DELIVERED',
    'CANCELLED',
    'REFUNDED',
    'ITEM_RESERVED',
    'ITEM_RELEASED'
);

CREATE TABLE order_events (
    id           BIGSERIAL PRIMARY KEY,
    event_type   order_event_type NOT NULL,
    order_id     UUID NOT NULL,
    customer_id  UUID NOT NULL,
    product_id   BIGINT,
    quantity     INT,
    amount       NUMERIC(12,2),
    payload      JSONB,
    occurred_at  TIMESTAMPTZ DEFAULT now()
);

-- Immutability enforced: no UPDATE or DELETE allowed
CREATE RULE no_update_order_events AS ON UPDATE TO order_events DO INSTEAD NOTHING;
CREATE RULE no_delete_order_events AS ON DELETE TO order_events DO INSTEAD NOTHING;
```

---

## Step 2 — Enable pg_trickle

```sql
CREATE EXTENSION IF NOT EXISTS pg_trickle;
```

---

## Step 3 — Current Order State (Read Model)

This stream table folds all events for each order into its current state.
`FILTER (WHERE ...)` aggregates extract the latest relevant event data per
event type.

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'current_order_state',
    query    => $$
        SELECT
            order_id,
            customer_id,
            MAX(occurred_at)
                FILTER (WHERE event_type = 'ORDER_PLACED')      AS placed_at,
            MAX(occurred_at)
                FILTER (WHERE event_type = 'PAYMENT_RECEIVED')  AS paid_at,
            MAX(occurred_at)
                FILTER (WHERE event_type = 'SHIPPED')           AS shipped_at,
            MAX(occurred_at)
                FILTER (WHERE event_type = 'DELIVERED')         AS delivered_at,
            MAX(occurred_at)
                FILTER (WHERE event_type = 'CANCELLED')         AS cancelled_at,
            SUM(amount)
                FILTER (WHERE event_type = 'ORDER_PLACED')      AS order_total,
            CASE
                WHEN BOOL_OR(event_type = 'CANCELLED')   THEN 'cancelled'
                WHEN BOOL_OR(event_type = 'DELIVERED')   THEN 'delivered'
                WHEN BOOL_OR(event_type = 'SHIPPED')     THEN 'shipped'
                WHEN BOOL_OR(event_type = 'PAYMENT_RECEIVED') THEN 'paid'
                WHEN BOOL_OR(event_type = 'PAYMENT_FAILED')   THEN 'payment_failed'
                ELSE 'placed'
            END AS status
        FROM order_events
        WHERE event_type IN (
            'ORDER_PLACED', 'PAYMENT_RECEIVED', 'PAYMENT_FAILED',
            'SHIPPED', 'DELIVERED', 'CANCELLED'
        )
        GROUP BY order_id, customer_id
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);

CREATE INDEX ON current_order_state (order_id);
CREATE INDEX ON current_order_state (customer_id, placed_at DESC);
CREATE INDEX ON current_order_state (status, placed_at DESC);
```

**Read-model query — active orders for a customer:**

```sql
SELECT order_id,
       status,
       order_total,
       placed_at,
       shipped_at
FROM current_order_state
WHERE customer_id = $1
  AND status NOT IN ('delivered', 'cancelled')
ORDER BY placed_at DESC;
```

---

## Step 4 — Customer Lifetime Value (Read Model)

Track rolling spend and order count per customer.

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'customer_lifetime_value',
    query    => $$
        SELECT
            customer_id,
            COUNT(DISTINCT order_id)                              AS total_orders,
            SUM(amount)
                FILTER (WHERE event_type = 'PAYMENT_RECEIVED')   AS total_spent,
            SUM(amount)
                FILTER (WHERE event_type = 'REFUNDED')           AS total_refunded,
            SUM(amount)
                FILTER (WHERE event_type = 'PAYMENT_RECEIVED') -
            COALESCE(SUM(amount)
                FILTER (WHERE event_type = 'REFUNDED'), 0)       AS net_revenue,
            MIN(occurred_at)
                FILTER (WHERE event_type = 'ORDER_PLACED')       AS first_order_at,
            MAX(occurred_at)
                FILTER (WHERE event_type = 'ORDER_PLACED')       AS last_order_at
        FROM order_events
        WHERE event_type IN ('ORDER_PLACED', 'PAYMENT_RECEIVED', 'REFUNDED')
        GROUP BY customer_id
    $$,
    schedule     => '10s',
    refresh_mode => 'DIFFERENTIAL'
);

CREATE INDEX ON customer_lifetime_value (customer_id);
CREATE INDEX ON customer_lifetime_value (net_revenue DESC);
```

**Read-model query — top customers by net revenue:**

```sql
SELECT customer_id,
       total_orders,
       net_revenue,
       last_order_at
FROM customer_lifetime_value
ORDER BY net_revenue DESC
LIMIT 20;
```

---

## Step 5 — Inventory Levels (Read Model)

Derive current stock counts from `ITEM_RESERVED` and `ITEM_RELEASED` events.

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'inventory_levels',
    query    => $$
        SELECT
            product_id,
            SUM(CASE
                    WHEN event_type = 'ITEM_RESERVED' THEN -quantity
                    WHEN event_type = 'ITEM_RELEASED' THEN  quantity
                    ELSE 0
                END) AS reserved_delta,
            SUM(quantity)
                FILTER (WHERE event_type = 'ITEM_RESERVED') AS total_reserved,
            SUM(quantity)
                FILTER (WHERE event_type = 'ITEM_RELEASED') AS total_released,
            COUNT(DISTINCT order_id)
                FILTER (WHERE event_type = 'ITEM_RESERVED') AS active_reservations
        FROM order_events
        WHERE event_type IN ('ITEM_RESERVED', 'ITEM_RELEASED')
          AND product_id IS NOT NULL
        GROUP BY product_id
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);

CREATE INDEX ON inventory_levels (product_id);
```

---

## Step 6 — Try It with Sample Events

```sql
-- A customer places an order
INSERT INTO order_events (event_type, order_id, customer_id, product_id, quantity, amount) VALUES
    ('ORDER_PLACED',    'ord-001'::uuid, 'cust-A'::uuid, 1, 2, 2598.00),
    ('ITEM_RESERVED',   'ord-001'::uuid, 'cust-A'::uuid, 1, 2, NULL);

-- Payment succeeds
INSERT INTO order_events (event_type, order_id, customer_id, amount) VALUES
    ('PAYMENT_RECEIVED', 'ord-001'::uuid, 'cust-A'::uuid, 2598.00);

-- Order ships
INSERT INTO order_events (event_type, order_id, customer_id) VALUES
    ('SHIPPED', 'ord-001'::uuid, 'cust-A'::uuid);

-- Wait for the scheduler, then read the projections
SELECT * FROM current_order_state WHERE order_id = 'ord-001'::uuid;
SELECT * FROM customer_lifetime_value WHERE customer_id = 'cust-A'::uuid;
SELECT * FROM inventory_levels WHERE product_id = 1;
```

---

## Step 7 — CQRS Pattern Summary

```
Write path:                         Read path:
──────────────────                  ──────────────────────────────────
Application layer                   Dashboard / API queries
       │                                       │
       │ INSERT INTO order_events              │ SELECT FROM current_order_state
       │                                       │ SELECT FROM customer_lifetime_value
       ▼                                       │ SELECT FROM inventory_levels
order_events (event log)                       ▲
       │                                       │
       │ pg_trickle CDC triggers               │ pg_trickle differential refresh
       └──────────────────────────────────────►│
                    (incremental, per schedule)
```

The application layer writes **only** to `order_events`. pg_trickle handles
all projection maintenance automatically.

---

## Step 8 — Event Replay and Backfill

If you need to rebuild a projection from scratch (e.g., after changing the
defining query), use the reinitialize API:

```sql
-- Force a full rebuild of the current_order_state projection
SELECT pgtrickle.reinitialize_stream_table('current_order_state');
```

This triggers a FULL refresh from the event log, rebuilding the projection
from all historical events. Once complete, pg_trickle switches back to
differential maintenance automatically.

**Backfill workflow for a new projection:**

```sql
-- 1. Create the new projection with IMMEDIATE for the first cycle,
--    then switch to DIFFERENTIAL
SELECT pgtrickle.create_stream_table(
    name         => 'new_projection',
    query        => '...',
    schedule     => '5s',
    refresh_mode => 'FULL'       -- use FULL for initial backfill
);

-- 2. Wait for the first full cycle to complete
SELECT pgt_name, status, last_refresh_at
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'new_projection';

-- 3. Once status = 'ACTIVE', switch to DIFFERENTIAL
SELECT pgtrickle.alter_stream_table('new_projection',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## Monitor the Projections

```sql
SELECT pgt_name, status, refresh_mode,
       last_refresh_at,
       consecutive_errors,
       rows_in_last_refresh
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name IN ('current_order_state', 'customer_lifetime_value',
                   'inventory_levels')
ORDER BY pgt_name;
```

---

## Clean Up

```sql
SELECT pgtrickle.drop_stream_table('inventory_levels');
SELECT pgtrickle.drop_stream_table('customer_lifetime_value');
SELECT pgtrickle.drop_stream_table('current_order_state');

DROP TABLE order_events;
DROP TYPE order_event_type;
```

---

## Next Steps

- Chain projections for derived aggregates — see
  [FIRST_DASHBOARD.md](FIRST_DASHBOARD.md) Step 7
- Add downstream publication for external consumers — see
  [PUBLICATIONS.md](../PUBLICATIONS.md)
- Secure projections with RLS — see [ROW_LEVEL_SECURITY.md](ROW_LEVEL_SECURITY.md)
- Backfill and migration guide — see [BACKFILL_AND_MIGRATION.md](BACKFILL_AND_MIGRATION.md)
