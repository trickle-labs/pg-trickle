# Tutorial: Security Hardening for pg_trickle

> DOC-NEW-27 — Step-by-step security hardening guide: dedicated
> roles, CDC trigger ownership, change-buffer protection, and audit logging.

## Overview

This guide hardens a pg_trickle installation following the principle of
least privilege. After completing these steps:

- Stream tables are owned by a dedicated non-superuser role.
- Application users can read (but not write) stream tables.
- Change buffers are protected from direct application access.
- DDL operations against stream tables are audit-logged.

---

## Prerequisites

- PostgreSQL 18 with pg_trickle installed as a superuser
- `psql` or an admin SQL client

---

## Step 1 — Create Dedicated Roles

Run these statements as a superuser (e.g., `postgres`).

```sql
-- ─── pgtrickle_admin ──────────────────────────────────────────────────────
-- Manages stream tables: create, alter, drop, reinitialize.
-- Intended for DBAs and data engineers.
CREATE ROLE pgtrickle_admin NOLOGIN NOINHERIT;

GRANT USAGE  ON SCHEMA pgtrickle         TO pgtrickle_admin;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA pgtrickle TO pgtrickle_admin;

-- Allow creating stream tables in the public schema
GRANT CREATE ON SCHEMA public TO pgtrickle_admin;

-- Allow reading source tables (add schemas as required)
GRANT USAGE  ON SCHEMA public TO pgtrickle_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pgtrickle_admin;

-- Future tables in public schema (run once per schema)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO pgtrickle_admin;

-- ─── pgtrickle_user ───────────────────────────────────────────────────────
-- Reads stream tables and calls monitoring functions.
-- Intended for application backends.
CREATE ROLE pgtrickle_user NOLOGIN NOINHERIT;

GRANT USAGE  ON SCHEMA pgtrickle  TO pgtrickle_user;

-- Read-only access to stream tables (granted per-table below)
-- Monitoring functions
GRANT EXECUTE ON FUNCTION pgtrickle.pgt_status()             TO pgtrickle_user;
GRANT EXECUTE ON FUNCTION pgtrickle.refresh_efficiency()     TO pgtrickle_user;
GRANT EXECUTE ON FUNCTION pgtrickle.health_check()           TO pgtrickle_user;

-- ─── pgtrickle_readonly ───────────────────────────────────────────────────
-- Pure read access to stream tables only; no extension function access.
-- Intended for reporting tools and BI consumers.
CREATE ROLE pgtrickle_readonly NOLOGIN NOINHERIT;

GRANT USAGE ON SCHEMA public TO pgtrickle_readonly;
-- Per-table GRANT added below after stream tables are created.
```

---

## Step 2 — Grant Roles to Login Roles

```sql
-- Example: your data engineer login
CREATE ROLE de_alice LOGIN PASSWORD '...';
GRANT pgtrickle_admin TO de_alice;

-- Example: your application backend login
CREATE ROLE app_backend LOGIN PASSWORD '...';
GRANT pgtrickle_user TO app_backend;

-- Example: your BI tool login
CREATE ROLE bi_tool LOGIN PASSWORD '...';
GRANT pgtrickle_readonly TO bi_tool;
```

---

## Step 3 — Create Stream Tables Under the Admin Role

Connect as the `pgtrickle_admin` role (or `SET ROLE pgtrickle_admin`) and
create stream tables. The admin role becomes the owner, not the superuser.

```sql
SET ROLE pgtrickle_admin;

SELECT pgtrickle.create_stream_table(
    name     => 'order_summary',
    query    => $$SELECT region, SUM(amount) AS total FROM orders GROUP BY region$$,
    schedule => '10s'
);

RESET ROLE;
```

Verify ownership:

```sql
SELECT tablename, tableowner
FROM pg_tables
WHERE tablename = 'order_summary';
```

---

## Step 4 — Grant Read Access to Consumer Roles

```sql
-- pgtrickle_user: reads stream tables and calls monitoring functions
GRANT SELECT ON order_summary TO pgtrickle_user;

-- pgtrickle_readonly: pure read access
GRANT SELECT ON order_summary TO pgtrickle_readonly;

-- For future stream tables, set default privileges so new tables are
-- automatically accessible:
ALTER DEFAULT PRIVILEGES FOR ROLE pgtrickle_admin IN SCHEMA public
    GRANT SELECT ON TABLES TO pgtrickle_user;

ALTER DEFAULT PRIVILEGES FOR ROLE pgtrickle_admin IN SCHEMA public
    GRANT SELECT ON TABLES TO pgtrickle_readonly;
```

---

## Step 5 — Protect Change Buffers

Change buffers in `pgtrickle_changes` should never be directly accessible
to application users. Revoke all access and grant only to the extension owner:

```sql
-- Revoke PUBLIC access (if not already revoked during extension install)
REVOKE ALL ON SCHEMA pgtrickle_changes FROM PUBLIC;

-- Application roles must not see change buffer tables
REVOKE ALL ON ALL TABLES IN SCHEMA pgtrickle_changes FROM pgtrickle_user;
REVOKE ALL ON ALL TABLES IN SCHEMA pgtrickle_changes FROM pgtrickle_readonly;
REVOKE ALL ON ALL TABLES IN SCHEMA pgtrickle_changes FROM pgtrickle_admin;

-- Verify: this query should return zero rows for non-superuser roles
SELECT table_name
FROM information_schema.role_table_grants
WHERE table_schema = 'pgtrickle_changes'
  AND grantee IN ('pgtrickle_user', 'pgtrickle_readonly', 'pgtrickle_admin');
```

---

## Step 6 — Secure CDC Trigger Functions

CDC trigger functions are owned by the extension installation role. This lets
them write to private change buffers while stream-table authors keep only their
source `SELECT` and target-schema `CREATE` privileges. Verify this:

```sql
-- CDC trigger functions should be owned by the extension installation role.
SELECT trigger_name, event_object_table, action_statement
FROM information_schema.triggers
WHERE trigger_name LIKE 'pgt_cdc_%'
ORDER BY event_object_table;

-- Verify trigger function ownership
SELECT proname, rolname AS owner
FROM pg_proc
JOIN pg_roles ON pg_roles.oid = pg_proc.proowner
WHERE proname LIKE 'pgt_cdc_%';
```

The generated functions must retain `SECURITY DEFINER` and their locked
`search_path`; do not change their ownership or grant application roles access
to `pgtrickle_changes`.

---

## Step 7 — Enable Audit Logging for Stream Table DDL

Use PostgreSQL's `log_statement` or `pg_audit` (if installed) to capture
DDL events against pg_trickle objects.

### Using `log_statement` (built-in)

```sql
-- Log all DDL operations (creates, alters, drops)
ALTER SYSTEM SET log_statement = 'ddl';
SELECT pg_reload_conf();
```

DDL against stream tables — including `pgtrickle.create_stream_table()`,
`pgtrickle.drop_stream_table()`, and `pgtrickle.alter_stream_table()` —
will appear in the PostgreSQL log.

### Using `pg_audit` (recommended for production)

```sql
-- Install pg_audit extension (if available)
CREATE EXTENSION IF NOT EXISTS pgaudit;

-- Audit all DDL and function calls in the pgtrickle schema
ALTER SYSTEM SET pgaudit.log = 'DDL, FUNCTION';
SELECT pg_reload_conf();
```

### Query the pg_trickle DDL history

pg_trickle records every refresh in `pgtrickle.pgt_refresh_history`.
For change-level audit trails:

```sql
-- All stream table DDL operations (create, alter, drop)
SELECT pgt_name, action, performed_by, performed_at
FROM pgtrickle.pgt_ddl_history
ORDER BY performed_at DESC
LIMIT 50;

-- Recent refresh failures
SELECT pgt_name, refresh_mode, started_at, error_message
FROM pgtrickle.pgt_refresh_history
WHERE NOT success
  AND started_at > now() - interval '24 hours'
ORDER BY started_at DESC;
```

---

## Step 8 — Disable Extension Behaviour in Non-Refresh Environments

If you have replica databases or analysis environments where you do not
want pg_trickle running refreshes:

```sql
-- Disable the scheduler without uninstalling the extension
ALTER SYSTEM SET pg_trickle.enabled = off;
SELECT pg_reload_conf();
```

---

## Verification Checklist

After completing all steps, verify the hardened state:

```sql
-- 1. pgtrickle_admin can create stream tables
SET ROLE pgtrickle_admin;
SELECT pgtrickle.validate_query('SELECT 1');
RESET ROLE;

-- 2. pgtrickle_user can read stream tables but cannot modify them
SET ROLE pgtrickle_user;
SELECT * FROM order_summary LIMIT 1;   -- should succeed
-- INSERT INTO order_summary VALUES (...);  -- should fail with permission denied
RESET ROLE;

-- 3. pgtrickle_readonly cannot call extension functions
SET ROLE pgtrickle_readonly;
-- SELECT pgtrickle.refresh_stream_table('order_summary');  -- should fail
RESET ROLE;

-- 4. No application role can see change buffers
SELECT COUNT(*)
FROM information_schema.role_table_grants
WHERE table_schema = 'pgtrickle_changes'
  AND grantee NOT IN ('postgres', 'pg_trickle');
-- Expected: 0
```

---

## Security Hardening Checklist

- [ ] `pgtrickle_admin` role created with NOLOGIN NOINHERIT
- [ ] `pgtrickle_user` role created for application backends
- [ ] `pgtrickle_readonly` role created for BI / reporting tools
- [ ] Stream tables owned by `pgtrickle_admin`, not a superuser
- [ ] `REVOKE ALL ON SCHEMA pgtrickle_changes FROM PUBLIC`
- [ ] Application roles have no access to `pgtrickle_changes.*`
- [ ] Audit logging enabled (`log_statement = 'ddl'` or `pg_audit`)
- [ ] `pg_trickle.allow_circular = off` (default)
- [ ] `pg_trickle.enabled = off` on replica / analysis environments

---

## Next Steps

- Full security model reference — see [SECURITY_MODEL.md](../SECURITY_MODEL.md)
- RLS patterns for per-tenant stream tables — see [ROW_LEVEL_SECURITY.md](ROW_LEVEL_SECURITY.md)
- Security Guide (threat model, CDC triggers, hardening checklist) — see
  [SECURITY_GUIDE.md](../SECURITY_GUIDE.md)
