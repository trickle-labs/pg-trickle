# dbt-pgtrickle

**dbt-pgtrickle** is the official dbt adapter for pg_trickle. It lets
you define stream tables as dbt models using standard `{{ config() }}`
blocks, manage them through `dbt run` / `dbt build`, and run
incremental refreshes as part of your dbt pipeline.

## Quick example

```sql
-- models/orders_agg.sql
{{ config(
    materialized = 'stream_table',
    schedule     = '5m',
    refresh_mode = 'DIFFERENTIAL'
) }}

SELECT customer_id,
       COUNT(*)         AS order_count,
       SUM(amount)      AS total_spent
FROM {{ ref('orders') }}
GROUP BY customer_id
```

Run it:

```bash
dbt run --select orders_agg
```

The model is created as a stream table and refreshed automatically.
Subsequent `dbt run` invocations update the defining query if it changed
(via `ALTER QUERY`), without dropping and recreating the table.

## Installation

```bash
pip install dbt-pgtrickle
```

Requires dbt-postgres 1.7+ and pg_trickle.

> The full configuration reference, supported materializations, macros,
> testing guide, and CI setup are in the
> [dbt-pgtrickle README](https://github.com/trickle-labs/pg-trickle/blob/main/dbt-pgtrickle/README.md).

---

{{#include ../../dbt-pgtrickle/README.md}}
