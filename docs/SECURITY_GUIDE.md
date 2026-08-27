# Security Guide

This page is the practical security reference for operators of
pg_trickle. It covers roles and grants, what privileges the
extension needs, how stream tables interact with PostgreSQL Row-Level
Security (RLS), how triggers behave under SECURITY DEFINER vs
INVOKER, and what to lock down in production.

> **Reporting a vulnerability?** See
> [SECURITY.md](https://github.com/trickle-labs/pg-trickle/blob/main/SECURITY.md)
> in the repository root for the disclosure policy.

---

## Threat model in one paragraph

pg_trickle runs *inside* PostgreSQL. Anyone who can connect as a
superuser, or as a role that owns the relevant tables, can already
read, modify, or destroy the data the extension manages — they do
not need pg_trickle to do that. The threats this guide focuses on
are: privilege escalation through stream tables (e.g., a low-privilege
role gaining access to source data via a stream table), accidental
exposure of source data through CDC change buffers, and operational
mistakes (running everything as the postgres superuser).

---

## Roles & grants

### What pg_trickle needs

The extension installs into the `pgtrickle` and `pgtrickle_changes`
schemas. The role that runs `CREATE EXTENSION pg_trickle` must be a
**superuser** because the extension installs background workers, but
day-to-day usage can (and should) be done with a less-privileged
role.

The role that **creates a stream table** needs:

- `USAGE` on the schemas containing source tables.
- `SELECT` on the source tables referenced in the defining query.
- `CREATE` on the schema where the stream table will live.
- `EXECUTE` on the relevant `pgtrickle.*` functions.

The stream-table creation APIs use `SECURITY DEFINER` for private catalog,
CDC, and storage setup. They explicitly check the caller's source `SELECT` and
target-schema `CREATE` privileges, then transfer the completed stream table to
the caller. Authors do not need access to `pgtrickle_changes`.

### Publication privileges

Publication APIs preserve the caller's PostgreSQL privilege boundary. A caller
must own the stream table, have `USAGE` on its schema, have `CREATE` on the
current database, and have `EXECUTE` on the exact publication function.

```sql
GRANT CREATE ON DATABASE mydb TO st_author;
GRANT EXECUTE ON FUNCTION pgtrickle.stream_table_to_publication(text) TO st_author;
GRANT EXECUTE ON FUNCTION pgtrickle.drop_stream_table_publication(text) TO st_author;
```

The resulting publication is owned by the caller. Publication DDL does not run
with the extension owner's authority. The private binding is updated in the
same transaction and validates publication identity, ownership, the stream
relation, and the relation set before mutation. Publication callers do not
need access to `pgtrickle` or `pgtrickle_changes` tables.

### Recommended split

```sql
-- Owner of stream tables (your application's "data engineer" role)
CREATE ROLE st_author NOINHERIT;
GRANT USAGE       ON SCHEMA public TO st_author;
GRANT SELECT      ON ALL TABLES IN SCHEMA public TO st_author;
GRANT CREATE      ON SCHEMA public TO st_author;
-- Grant only the exact lifecycle overloads required by this role.
GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table(
    text, text, text, text, boolean, text, text, text, boolean, boolean,
    text, integer, double precision, text, boolean, text, integer, text
) TO st_author;
GRANT EXECUTE ON FUNCTION pgtrickle.alter_stream_table(
    text, text, text, text, text, text, text, text, boolean, boolean,
    text, text, bigint, integer, text, integer, double precision, text,
    double precision, text
) TO st_author;
GRANT EXECUTE ON FUNCTION pgtrickle.drop_stream_table(text, boolean) TO st_author;
GRANT USAGE       ON SCHEMA pgtrickle TO st_author;

-- Read-only consumer (your application)
CREATE ROLE app_reader;
GRANT USAGE       ON SCHEMA public TO app_reader;
GRANT SELECT      ON ALL TABLES IN SCHEMA public TO app_reader;
```

`app_reader` can read stream tables exactly as it reads any other
table — the extension does not require special privileges for
*reading* a stream table.

---

## Stream tables and Row-Level Security (RLS)

A stream table is the **materialized result** of its defining query.
RLS policies on **source** tables are evaluated **at the time the
defining query runs**, which is during refresh, under the **owner's**
identity (not the consumer's).

This has two important consequences:

1. **Stream-table contents do not honour the consumer's RLS context.**
   Two consumers with different RLS contexts will read the same rows
   from the stream table.
2. **You can apply RLS *to the stream table itself*** to filter rows
   per consumer. pg_trickle does not interfere with RLS policies on
   stream tables (they are ordinary heap tables under the hood).

The recommended pattern is therefore:

```sql
-- Define the ST without RLS at the source level
SELECT pgtrickle.create_stream_table(
    'order_summary',
    $$SELECT tenant_id, customer_id, SUM(amount) AS total
      FROM orders GROUP BY tenant_id, customer_id$$
);

-- Apply RLS to the stream table for per-tenant isolation
ALTER TABLE order_summary ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON order_summary
    FOR SELECT USING (tenant_id = current_setting('app.tenant_id')::int);
```

See the [Row-Level Security tutorial](tutorials/ROW_LEVEL_SECURITY.md)
for a complete worked example.

---

## CDC triggers — SECURITY DEFINER vs INVOKER

In trigger CDC mode, pg_trickle installs `AFTER` triggers on every source table
(statement-level by default, row-level if configured). Their functions run as
**SECURITY DEFINER** under the extension owner with a locked search path, so
they can write to `pgtrickle_changes.*` regardless of who issued the source-table
write.

**What this means for you:**

- Any role that can write to a source table will indirectly write to
  the corresponding change buffer. That is by design.
- The change buffer table is owned by the extension owner. Other roles get no
  implicit access.
- If you revoke `INSERT` on the change buffer, the trigger keeps
  working (it runs as the owner).

In WAL CDC mode, no triggers are installed; capture happens in
PostgreSQL's logical decoding pipeline and is governed by the
`max_replication_slots` and `wal_level` settings.

---

## What change buffers contain

`pgtrickle_changes.changes_<oid>` tables contain the **post-image**
of each changed row, restricted to the columns referenced by the
defining query (columnar tracking). Two consequences:

1. If your defining query references a sensitive column, that
   column ends up in the change buffer.
2. The change buffer table inherits the same `tablespace` and disk
   layout rules as ordinary tables. If you encrypt your data
   directory, the change buffers are encrypted at rest the same way.

You can lock change buffers down further:

```sql
REVOKE ALL ON ALL TABLES IN SCHEMA pgtrickle_changes FROM PUBLIC;
-- No pgtrickle_changes grant is required for a stream-table owner. The
-- owner-checked SECURITY DEFINER lifecycle and refresh paths access buffers
-- internally with the pinned extension boundary.
```

---

## Lock down circular dependencies

`pg_trickle.allow_circular` is `off` by default and should generally
stay that way. Cycles in the DAG are accepted only when this GUC is
on, and only for *monotone* queries — but enabling it widens the
class of queries pg_trickle accepts, which deserves explicit
attention. Set it via `ALTER SYSTEM` and require a superuser to
flip it.

---

## Audit & monitoring

pg_trickle records every refresh in
`pgtrickle.pgt_refresh_history`. For audit:

```sql
-- Last 100 refreshes across the whole installation
SELECT pgt_name, refresh_mode, started_at, finished_at,
       success, rows_in, rows_out, error_message
FROM pgtrickle.pgt_refresh_history
ORDER BY started_at DESC
LIMIT 100;

-- Failed refreshes in the last hour
SELECT * FROM pgtrickle.pgt_refresh_history
WHERE NOT success AND started_at > now() - interval '1 hour';
```

Combine with `pg_audit` for full DDL/DML coverage. The
[Monitoring & Alerting tutorial](tutorials/MONITORING_AND_ALERTING.md)
includes recommended Prometheus alerts.

---

## Copy-Paste Role Templates

The following SQL templates create the three standard pg_trickle roles and
grant the minimum required privileges.  Run these as a superuser immediately
after installing the extension.

### `pgtrickle_admin` — stream table author

```sql
CREATE ROLE pgtrickle_admin NOLOGIN NOINHERIT;

-- Extension function access
GRANT USAGE   ON SCHEMA pgtrickle          TO pgtrickle_admin;
-- Use the exact grants emitted by scripts/check_sql_api_policy.py.
-- Do not grant EXECUTE on every function: internal and trigger-entry
-- functions are intentionally excluded.
-- Example lifecycle grants:
GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table(
    text, text, text, text, boolean, text, text, text, boolean, boolean,
    text, integer, double precision, text, boolean, text, integer, text
) TO pgtrickle_admin;
-- v0.87.9: create_or_replace_stream_table, alter_stream_table, and
-- drop_stream_table are SECURITY DEFINER with a pinned search_path, so
-- these grants alone are sufficient — no pg_trickle private catalog or
-- pgtrickle_changes access is required.
GRANT EXECUTE ON FUNCTION pgtrickle.create_or_replace_stream_table(
    text, text, text, text, boolean, text, text, text, boolean, boolean,
    text, integer, double precision, text, boolean, text
) TO pgtrickle_admin;
GRANT EXECUTE ON FUNCTION pgtrickle.alter_stream_table(
    text, text, text, text, text, text, text, text, boolean, boolean,
    text, text, bigint, integer, text, integer, double precision, text,
    double precision, text
) TO pgtrickle_admin;
GRANT EXECUTE ON FUNCTION pgtrickle.drop_stream_table(text, boolean)
    TO pgtrickle_admin;

-- Create stream tables in the public schema
GRANT CREATE  ON SCHEMA public TO pgtrickle_admin;
GRANT USAGE   ON SCHEMA public TO pgtrickle_admin;
GRANT SELECT  ON ALL TABLES IN SCHEMA public TO pgtrickle_admin;

-- Automatically grant SELECT on new source tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO pgtrickle_admin;
```

### `pgtrickle_user` — application backend

```sql
CREATE ROLE pgtrickle_user NOLOGIN NOINHERIT;

GRANT USAGE   ON SCHEMA pgtrickle TO pgtrickle_user;

-- Monitoring functions (read-only)
GRANT EXECUTE ON FUNCTION pgtrickle.pgt_status()         TO pgtrickle_user;
GRANT EXECUTE ON FUNCTION pgtrickle.refresh_efficiency() TO pgtrickle_user;
GRANT EXECUTE ON FUNCTION pgtrickle.health_check()       TO pgtrickle_user;

-- Per-stream-table SELECT (run after each create_stream_table call):
-- GRANT SELECT ON <stream_table_name> TO pgtrickle_user;
```

### `pgtrickle_readonly` — BI and reporting tools

```sql
CREATE ROLE pgtrickle_readonly NOLOGIN NOINHERIT;

GRANT USAGE ON SCHEMA public TO pgtrickle_readonly;

-- Per-stream-table SELECT (run after each create_stream_table call):
-- GRANT SELECT ON <stream_table_name> TO pgtrickle_readonly;
```

### Assign roles to login roles

```sql
-- Data engineer
CREATE ROLE de_alice LOGIN PASSWORD '...';
GRANT pgtrickle_admin    TO de_alice;

-- Application backend
CREATE ROLE app_backend  LOGIN PASSWORD '...';
GRANT pgtrickle_user     TO app_backend;

-- BI tool
CREATE ROLE bi_tool      LOGIN PASSWORD '...';
GRANT pgtrickle_readonly TO bi_tool;
```

For a complete worked example including CDC trigger ownership verification,
see the
[Security Hardening tutorial](tutorials/SECURITY_HARDENING.md).

---

## Hardening checklist

- [ ] `pg_trickle.allow_circular = off` unless explicitly needed.
- [ ] Stream tables owned by a dedicated, non-superuser role.
- [ ] `REVOKE ... FROM PUBLIC` on `pgtrickle_changes` if change
      buffers contain sensitive columns.
- [ ] RLS policies applied to stream tables that present per-tenant
      data.
- [ ] Audit logging in place for `pgtrickle.pgt_refresh_history`.
- [ ] `pg_trickle.enabled = on` only in environments that should
      run refreshes (you can disable extension behaviour without
      uninstalling it).

---

**See also:**
[Row-Level Security tutorial](tutorials/ROW_LEVEL_SECURITY.md) ·
[Pre-Deployment Checklist](PRE_DEPLOYMENT.md) ·
[Configuration](CONFIGURATION.md) ·
[SECURITY policy](https://github.com/trickle-labs/pg-trickle/blob/main/SECURITY.md)
