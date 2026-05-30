{{ config(
    materialized='stream_table',
    schedule='1m',
    refresh_mode='DIFFERENTIAL'
) }}

-- T-5 (v0.79.0): dbt adapter compatibility matrix — create/alter/drop/rebuild flows.
-- This model is used to test that ALTER STREAM TABLE is issued when the config
-- changes (rather than a full DROP + CREATE).
SELECT
    customer_id,
    SUM(amount)  AS total_amount,
    COUNT(*)     AS order_count
FROM {{ ref('raw_orders') }}
GROUP BY customer_id
