-- T-5 (v0.79.0): Assert that the compatibility-matrix stream table produces
-- the correct data — same totals as a direct aggregate of the seed data.
-- An empty result set means the test passes.
WITH expected AS (
    SELECT
        customer_id,
        SUM(amount)  AS total_amount,
        COUNT(*)     AS order_count
    FROM {{ ref('raw_orders') }}
    GROUP BY customer_id
),
actual AS (
    SELECT customer_id, total_amount, order_count
    FROM {{ ref('order_totals_compat') }}
)
SELECT e.*
FROM expected e
LEFT JOIN actual a
  ON  e.customer_id  = a.customer_id
  AND e.total_amount = a.total_amount
  AND e.order_count  = a.order_count
WHERE a.customer_id IS NULL
