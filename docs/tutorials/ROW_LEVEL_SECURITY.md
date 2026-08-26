# Row-Level Security (RLS) on Stream Tables

This tutorial shows how to apply PostgreSQL Row-Level Security to stream tables
so that different database roles see only the rows they are permitted to access.

## Background

Stream tables evaluate their defining query as the stream-table owner with
`row_security = on`. Source policies define what is materialized; stream-table
policies define who may read the materialized rows.

The recommended pattern is:

1. **Source tables**: grants and RLS are evaluated as the stream owner.
2. **Stream table**: enable RLS **on the stream table** and create per-role
   policies so each role sees only its permitted rows.

## Setup: Multi-Tenant Orders

```sql
-- Source table: all tenant orders
CREATE TABLE orders (
    id        SERIAL PRIMARY KEY,
    tenant_id INT    NOT NULL,
    product   TEXT   NOT NULL,
    amount    NUMERIC(10,2) NOT NULL
);

INSERT INTO orders (tenant_id, product, amount) VALUES
    (1, 'Widget A', 19.99),
    (1, 'Widget B',  9.50),
    (2, 'Gadget X', 49.00),
    (2, 'Gadget Y', 25.00),
    (3, 'Doohickey', 5.00);

-- Stream table: per-tenant spend summary
SELECT pgtrickle.create_stream_table(
    name  => 'tenant_spend',
    query => $$
      SELECT tenant_id,
             COUNT(*)       AS order_count,
             SUM(amount)    AS total_spend
      FROM orders
      GROUP BY tenant_id
    $$,
    schedule => '1m'
);
```

After the first refresh, `tenant_spend` contains all three tenants:

```sql
SELECT * FROM pgtrickle.tenant_spend ORDER BY tenant_id;
--  tenant_id | order_count | total_spend
-- -----------+-------------+-------------
--          1 |           2 |       29.49
--          2 |           2 |       74.00
--          3 |           1 |        5.00
```

## Step 1: Enable RLS on the Stream Table

```sql
ALTER TABLE pgtrickle.tenant_spend ENABLE ROW LEVEL SECURITY;
```

Once RLS is enabled, **non-superuser** roles see zero rows unless a policy
grants access. The superuser (table owner) bypasses RLS by default.

## Step 2: Create Per-Tenant Roles

```sql
CREATE ROLE tenant_1 LOGIN;
CREATE ROLE tenant_2 LOGIN;

GRANT USAGE  ON SCHEMA pgtrickle TO tenant_1, tenant_2;
GRANT SELECT ON pgtrickle.tenant_spend TO tenant_1, tenant_2;
```

## Step 3: Create RLS Policies

```sql
-- Tenant 1 sees only tenant_id = 1
CREATE POLICY tenant_1_policy ON pgtrickle.tenant_spend
    FOR SELECT
    TO tenant_1
    USING (tenant_id = 1);

-- Tenant 2 sees only tenant_id = 2
CREATE POLICY tenant_2_policy ON pgtrickle.tenant_spend
    FOR SELECT
    TO tenant_2
    USING (tenant_id = 2);
```

## Step 4: Verify Filtering

Connect as each tenant role and query:

```sql
-- As tenant_1:
SET ROLE tenant_1;
SELECT * FROM pgtrickle.tenant_spend;
--  tenant_id | order_count | total_spend
-- -----------+-------------+-------------
--          1 |           2 |       29.49

RESET ROLE;

-- As tenant_2:
SET ROLE tenant_2;
SELECT * FROM pgtrickle.tenant_spend;
--  tenant_id | order_count | total_spend
-- -----------+-------------+-------------
--          2 |           2 |       74.00

RESET ROLE;
```

Each tenant sees only their own data. The underlying stream table still
contains all rows — the filtering happens at query time via RLS.

## How Refresh Works with RLS

Initial, scheduled, manual, differential, full, and IMMEDIATE refreshes use the
same stored owner and defining path. This ensures:

- Source policies produce the same owner-visible result on every path.
- A `refresh_stream_table()` call produces the same result regardless of its caller.
- IMMEDIATE trigger bookkeeping stays privileged while defining SQL runs as the owner.

## Policy Change Detection

pg_trickle automatically detects RLS-related DDL on source tables:

| DDL on source table | Effect |
|---------------------|--------|
| `CREATE POLICY` / `ALTER POLICY` / `DROP POLICY` | Stream table marked for reinit |
| `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` | Stream table marked for reinit |
| `ALTER TABLE ... DISABLE ROW LEVEL SECURITY` | Stream table marked for reinit |
| `ALTER TABLE ... FORCE ROW LEVEL SECURITY` | Stream table marked for reinit |
| `ALTER TABLE ... NO FORCE ROW LEVEL SECURITY` | Stream table marked for reinit |

These reinits recompute the result under the updated owner-visible policy.

## Tips

- **One stream table, many roles**: A single stream table can serve all
  tenants. Each role's RLS policy filters at read time — no per-tenant
  duplication needed.
- **Write policies**: Stream tables are maintained by pg_trickle. Restrict
  writes to the pg_trickle system by only creating `FOR SELECT` policies.
- **Default deny**: Once RLS is enabled, roles without a matching policy see
  zero rows. Always test with a non-superuser role.
- **FORCE ROW LEVEL SECURITY**: By default, table owners bypass RLS. Use
  `ALTER TABLE ... FORCE ROW LEVEL SECURITY` if the owner should also be
  subject to policies.
