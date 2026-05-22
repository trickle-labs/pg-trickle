# Tutorial: Build a Real-Time Analytics Dashboard

> DOC-NEW-24 — End-to-end tutorial: build the backend for a
> real-time analytics dashboard over a sample e-commerce dataset.

## What You Will Build

A real-time analytics backend that powers three dashboard panels:

1. **Revenue by region** — running totals updated within seconds of each order
2. **Hourly order counts** — time-bucketed activity feed for trend charts
3. **Top 10 products** — a continuously-maintained leaderboard by revenue

All three panels are backed by pg_trickle stream tables, so they refresh
incrementally — only the rows that actually changed are recomputed.

---

## Prerequisites

- PostgreSQL 18 with pg_trickle installed (see [Installation](../installation.md))
- `psql` or any SQL client

---

## Step 1 — Create the Source Tables

```sql
-- Orders: the core transaction table
CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    region      TEXT        NOT NULL,
    product_id  BIGINT      NOT NULL,
    amount      NUMERIC(12,2) NOT NULL,
    placed_at   TIMESTAMPTZ DEFAULT now()
);

-- Products: the product catalogue
CREATE TABLE products (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL
);

-- Seed products
INSERT INTO products (name, category) VALUES
    ('Laptop Pro 15',   'Electronics'),
    ('Wireless Keyboard', 'Electronics'),
    ('Standing Desk',   'Furniture'),
    ('Ergonomic Chair', 'Furniture'),
    ('USB-C Hub',       'Electronics');
```

---

## Step 2 — Enable pg_trickle

```sql
CREATE EXTENSION IF NOT EXISTS pg_trickle;
```

---

## Step 3 — Revenue by Region

This panel shows the total revenue in each region, updated automatically
as orders arrive.

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'revenue_by_region',
    query    => $$
        SELECT
            region,
            COUNT(*)             AS order_count,
            SUM(amount)          AS total_revenue,
            AVG(amount)          AS avg_order_value,
            MAX(placed_at)       AS last_order_at
        FROM orders
        GROUP BY region
    $$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Index for fast dashboard lookups
CREATE INDEX ON revenue_by_region (region);
```

**Dashboard query:**

```sql
SELECT region,
       order_count,
       total_revenue,
       avg_order_value,
       last_order_at
FROM revenue_by_region
ORDER BY total_revenue DESC;
```

---

## Step 4 — Hourly Order Counts

A time-series stream table that aggregates orders into one-hour buckets.
`date_trunc('hour', ...)` is a STABLE function, so DIFFERENTIAL mode works.

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'hourly_order_counts',
    query    => $$
        SELECT
            date_trunc('hour', placed_at)  AS hour,
            region,
            COUNT(*)                        AS order_count,
            SUM(amount)                     AS hourly_revenue
        FROM orders
        GROUP BY date_trunc('hour', placed_at), region
    $$,
    schedule => '10s',
    refresh_mode => 'DIFFERENTIAL'
);

CREATE INDEX ON hourly_order_counts (hour DESC, region);
```

**Dashboard query — last 24 hours:**

```sql
SELECT hour,
       region,
       order_count,
       hourly_revenue
FROM hourly_order_counts
WHERE hour >= now() - interval '24 hours'
ORDER BY hour DESC, region;
```

---

## Step 5 — Top 10 Products by Revenue

A leaderboard of the top-selling products. Joining `orders` to `products`
to include the product name.

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'top_products',
    query    => $$
        SELECT
            p.id                    AS product_id,
            p.name                  AS product_name,
            p.category,
            COUNT(o.id)             AS order_count,
            SUM(o.amount)           AS total_revenue
        FROM orders o
        JOIN products p ON p.id = o.product_id
        GROUP BY p.id, p.name, p.category
        ORDER BY SUM(o.amount) DESC
        LIMIT 10
    $$,
    schedule => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
```

**Dashboard query:**

```sql
SELECT product_name,
       category,
       order_count,
       total_revenue
FROM top_products
ORDER BY total_revenue DESC;
```

> **Note:** `LIMIT N` stream tables use differential TOP-K maintenance —
> pg_trickle tracks the rank boundary and recomputes only when a row
> enters or exits the top 10.

---

## Step 6 — Insert Some Sample Data and Watch It Update

```sql
-- Simulate a batch of orders
INSERT INTO orders (region, product_id, amount) VALUES
    ('US',   1, 1299.00),
    ('EU',   2,  149.99),
    ('US',   3,  799.00),
    ('APAC', 1, 1299.00),
    ('US',   4,  599.00),
    ('EU',   5,   49.99),
    ('US',   1, 1299.00);

-- Wait a few seconds, then query the stream tables
SELECT * FROM revenue_by_region ORDER BY total_revenue DESC;
SELECT * FROM top_products;
```

---

## Step 7 — Chain the Stream Tables (Optional)

You can build derived stream tables on top of other stream tables.
For example, compute a "daily summary" that reads from `hourly_order_counts`:

```sql
SELECT pgtrickle.create_stream_table(
    name     => 'daily_revenue_summary',
    query    => $$
        SELECT
            date_trunc('day', hour) AS day,
            region,
            SUM(order_count)        AS total_orders,
            SUM(hourly_revenue)     AS total_revenue
        FROM hourly_order_counts
        GROUP BY date_trunc('day', hour), region
    $$,
    schedule => '30s',
    refresh_mode => 'DIFFERENTIAL'
);
```

pg_trickle automatically builds a dependency DAG: when `orders` changes,
it refreshes `hourly_order_counts` first, then `daily_revenue_summary`
in topological order.

---

## Step 8 — Optional: Grafana Data-Source Configuration

If you use Grafana with the PostgreSQL data source plugin, point it at
the database and use the stream tables directly as query targets:

```sql
-- Grafana time-series panel query (hourly revenue per region)
SELECT
    $__timeGroupAlias(hour, '1h'),
    region,
    SUM(hourly_revenue) AS revenue
FROM hourly_order_counts
WHERE $__timeFilter(hour)
GROUP BY 1, 2
ORDER BY 1;
```

Set `Refresh` to `5s` in the Grafana panel options to poll for updates.

---

## Monitor the Stream Tables

```sql
-- Check that all three stream tables are ACTIVE and refreshing
SELECT pgt_name, status, refresh_mode,
       last_refresh_at,
       consecutive_errors
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name IN ('revenue_by_region', 'hourly_order_counts',
                   'top_products', 'daily_revenue_summary')
ORDER BY pgt_name;
```

---

## Clean Up

```sql
SELECT pgtrickle.drop_stream_table('daily_revenue_summary');
SELECT pgtrickle.drop_stream_table('top_products');
SELECT pgtrickle.drop_stream_table('hourly_order_counts');
SELECT pgtrickle.drop_stream_table('revenue_by_region');

DROP TABLE orders;
DROP TABLE products;
```

---

## Next Steps

- Add per-tenant isolation with RLS — see [ROW_LEVEL_SECURITY.md](ROW_LEVEL_SECURITY.md)
- Expose stream tables as a downstream publication — see [PUBLICATIONS.md](../PUBLICATIONS.md)
- Tune refresh intervals for your load — see [tuning-refresh-mode.md](tuning-refresh-mode.md)
- Performance optimisation — see [PERFORMANCE_COOKBOOK.md](../PERFORMANCE_COOKBOOK.md)
