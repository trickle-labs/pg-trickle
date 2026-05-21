-- Demo E: OLTP-to-Lake Loop — PostgreSQL initialization

CREATE EXTENSION IF NOT EXISTS pg_trickle;

CREATE TABLE IF NOT EXISTS orders (
    order_id   BIGSERIAL PRIMARY KEY,
    region     TEXT NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Create the revenue_by_region stream table
SELECT pgtrickle.create_stream_table(
    name     => 'revenue_by_region',
    query    => $$
        SELECT
            region,
            date_trunc('minute', created_at) AS minute,
            SUM(amount)                       AS total_revenue,
            COUNT(*)                          AS order_count
        FROM orders
        GROUP BY region, date_trunc('minute', created_at)
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
