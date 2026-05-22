# Foreign Table Sources

This tutorial shows how to use a `postgres_fdw` foreign table as a source for
a stream table. Foreign tables let you aggregate data from remote PostgreSQL
databases into a local stream table that refreshes automatically.

## Background

PostgreSQL's [Foreign Data Wrapper](https://www.postgresql.org/docs/current/postgres-fdw.html)
(`postgres_fdw`) lets you define tables that transparently query a remote
database. pg_trickle can use these foreign tables as stream table sources,
but with different change-detection semantics than regular tables.

**Key difference:** Foreign tables cannot use trigger-based or WAL-based CDC.
Changes are detected either by re-scanning the entire remote table (FULL
refresh) or by comparing a local snapshot to the remote data (polling-based
CDC).

## Step 1 — Set Up the Foreign Server

```sql
-- Enable the foreign data wrapper extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create a connection to the remote database
CREATE SERVER warehouse_db
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host 'warehouse.example.com', dbname 'analytics', port '5432');

-- Map the current user to a remote user
CREATE USER MAPPING FOR CURRENT_USER
    SERVER warehouse_db
    OPTIONS (user 'readonly_user', password 'secret');
```

## Step 2 — Define the Foreign Table

```sql
CREATE FOREIGN TABLE remote_orders (
    id          INT,
    customer_id INT,
    amount      NUMERIC(12,2),
    region      TEXT,
    created_at  TIMESTAMP
) SERVER warehouse_db
  OPTIONS (schema_name 'public', table_name 'orders');
```

Alternatively, import an entire remote schema:

```sql
IMPORT FOREIGN SCHEMA public
    LIMIT TO (orders, customers)
    FROM SERVER warehouse_db
    INTO public;
```

## Step 3 — Create a Stream Table with FULL Refresh

The simplest approach uses FULL refresh mode — pg_trickle re-executes the
query against the remote table on every refresh cycle:

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'orders_by_region',
    query        => $$
        SELECT
            region,
            COUNT(*)        AS order_count,
            SUM(amount)     AS total_revenue,
            AVG(amount)     AS avg_order_value
        FROM remote_orders
        GROUP BY region
    $$,
    schedule     => '5m',
    refresh_mode => 'FULL'
);
```

pg_trickle will emit an informational message:

```
INFO: pg_trickle: source table remote_orders is a foreign table.
Foreign tables cannot use trigger-based or WAL-based CDC —
only FULL refresh mode or polling-based change detection is supported.
```

**How FULL refresh works with foreign tables:**

1. Every 5 minutes, pg_trickle executes the defining query.
2. The query is sent to the remote database via `postgres_fdw`.
3. The complete result set replaces the stream table contents.
4. This is equivalent to a `MATERIALIZED VIEW` refresh, but automated.

## Step 4 — Polling-Based CDC (Optional)

If the remote table is large and changes are small, FULL refresh becomes
expensive because it transfers the entire result set every cycle. Polling-based
CDC provides a more efficient alternative:

```sql
-- Enable polling globally (or per-session)
SET pg_trickle.foreign_table_polling = on;

-- Now create with DIFFERENTIAL mode — pg_trickle will use polling
SELECT pgtrickle.create_stream_table(
    name         => 'orders_by_region_polling',
    query        => $$
        SELECT
            region,
            COUNT(*)        AS order_count,
            SUM(amount)     AS total_revenue,
            AVG(amount)     AS avg_order_value
        FROM remote_orders
        GROUP BY region
    $$,
    schedule     => '5m',
    refresh_mode => 'FULL'
);
```

**How polling works:**

1. On the first refresh, pg_trickle creates a local **snapshot table** that
   mirrors the remote table's data.
2. On subsequent refreshes, it fetches the current remote data and computes
   an `EXCEPT ALL` difference against the snapshot.
3. Only the changed rows are written to the change buffer and processed through
   the incremental delta pipeline.
4. The snapshot table is updated to reflect the new remote state.
5. When the stream table is dropped, the snapshot table is cleaned up.

**Trade-offs:**

| Aspect | FULL Refresh | Polling CDC |
|--------|-------------|-------------|
| **Network transfer** | Full result set every cycle | Full remote scan, but only diffs applied |
| **Local storage** | Stream table only | Stream table + snapshot table |
| **Best for** | Small remote tables | Large remote tables with small change rates |
| **GUC required** | No | `pg_trickle.foreign_table_polling = on` |

## Step 5 — Verify and Monitor

```sql
-- Check stream table status
SELECT * FROM pgtrickle.pgt_status('orders_by_region');

-- Check CDC health (will show foreign table constraints)
SELECT * FROM pgtrickle.check_cdc_health();

-- View refresh history
SELECT * FROM pgtrickle.get_refresh_history('orders_by_region', 5);

-- Monitor staleness
SELECT * FROM pgtrickle.get_staleness('orders_by_region');
```

## Worked Example — Remote Inventory Dashboard

This example aggregates inventory data from a remote warehouse database into a
local dashboard table:

```sql
-- Remote table definition
CREATE FOREIGN TABLE remote_inventory (
    sku         TEXT,
    warehouse   TEXT,
    quantity    INT,
    updated_at  TIMESTAMP
) SERVER warehouse_db
  OPTIONS (schema_name 'inventory', table_name 'stock_levels');

-- Dashboard: inventory summary by warehouse
SELECT pgtrickle.create_stream_table(
    name     => 'inventory_dashboard',
    query    => $$
        SELECT
            warehouse,
            COUNT(DISTINCT sku)  AS unique_products,
            SUM(quantity)        AS total_units,
            MIN(updated_at)      AS oldest_update,
            MAX(updated_at)      AS newest_update
        FROM remote_inventory
        GROUP BY warehouse
    $$,
    schedule     => '10m',
    refresh_mode => 'FULL'
);
```

After the first refresh:

```sql
SELECT * FROM inventory_dashboard;
```

```
 warehouse | unique_products | total_units | oldest_update       | newest_update
-----------+-----------------+-------------+---------------------+---------------------
 east      |             142 |       23500 | 2026-03-14 08:00:00 | 2026-03-14 09:15:00
 west      |              98 |       15200 | 2026-03-14 07:30:00 | 2026-03-14 09:10:00
 central   |             215 |       41000 | 2026-03-14 06:00:00 | 2026-03-14 09:20:00
```

## Constraints and Caveats

| Constraint | Details |
|------------|---------|
| **No trigger CDC** | Foreign tables do not support any PostgreSQL triggers (neither row-level nor statement-level). |
| **No WAL CDC** | Foreign tables don't generate local WAL entries. |
| **Network latency** | Each refresh cycle queries the remote database. Schedule accordingly. |
| **Remote availability** | If the remote database is down, the refresh will fail (logged in `pgt_refresh_history`). The stream table retains its last successful data. |
| **Authentication** | `CREATE USER MAPPING` credentials must remain valid. Use `.pgpass` or environment variables in production. |
| **Snapshot storage** | Polling CDC creates a snapshot table sized proportionally to the remote table. Monitor disk usage. |

## FAQ

**Q: Why does my foreign table stream table only work in FULL mode?**

Foreign tables cannot have any PostgreSQL triggers installed (neither row-level nor statement-level), so trigger-based CDC is impossible for foreign tables. They also don't generate local WAL records (used by WAL-based CDC). FULL refresh works because it simply re-executes the remote query.
Enable `pg_trickle.foreign_table_polling` if you need differential-style
change detection.

**Q: Can I mix foreign and local tables in the same defining query?**

Yes. If your query joins a foreign table with a local table, pg_trickle uses
trigger/WAL CDC for the local table and FULL-rescan or polling for the foreign
table. The refresh mode must be FULL unless polling is enabled for the foreign
table sources.

**Q: What happens if the remote database is temporarily unavailable?**

The refresh attempt fails, is logged in `pgt_refresh_history` with status
`FAILED`, and the `consecutive_errors` counter increments. The stream table
retains its last successful data. When the remote database recovers, the next
scheduled refresh succeeds and the error counter resets.
