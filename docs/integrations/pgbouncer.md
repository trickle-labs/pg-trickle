# PgBouncer & Connection Poolers

pg_trickle's background scheduler uses session-level PostgreSQL features.
This page explains how to configure pg_trickle alongside connection poolers
like PgBouncer, Supavisor (Supabase), and PgCat.

## Compatibility Matrix

| Pooling Mode | Compatible? | Notes |
|-------------|-------------|-------|
| **Session mode** (`pool_mode = session`) | ✅ Fully | All features work. |
| **Direct connection** (no pooler for scheduler) | ✅ Fully | Application queries can still go through a pooler. |
| **Transaction mode** (`pool_mode = transaction`) | ❌ Not supported | Advisory locks, prepared statements, and LISTEN/NOTIFY are session-scoped. |
| **Statement mode** (`pool_mode = statement`) | ❌ Not supported | Same session-scoped limitations. |

## Why Transaction Mode Breaks

The pg_trickle scheduler relies on three session-level features:

| Feature | Problem in Transaction Mode |
|---------|---------------------------|
| `pg_advisory_lock()` | Session lock released when connection returns to pool — concurrent refreshes become possible |
| `PREPARE` / `EXECUTE` | Prepared statements vanish on connection hop — "prepared statement does not exist" errors |
| `LISTEN` / `NOTIFY` | Listener loses notifications when assigned a different backend connection |

## Recommended Setup

Route the pg_trickle background worker through a **direct connection**
while keeping application traffic on the pooler:

```
┌─────────────────┐     ┌──────────────┐
│  Application    │────▶│  PgBouncer   │──┐
│  (transaction   │     │  (txn mode)  │  │
│   mode OK)      │     └──────────────┘  │
└─────────────────┘                       │
                                          ▼
┌─────────────────┐                ┌─────────────┐
│  pg_trickle     │───────────────▶│ PostgreSQL   │
│  scheduler      │  direct conn   │             │
│  (session mode) │                └─────────────┘
└─────────────────┘
```

The scheduler connects directly to PostgreSQL as a background worker — it
does not go through the pooler at all. No special configuration is needed
for this; the scheduler always uses an internal SPI connection.

**The pooler only matters for application queries** that read from stream
tables or call pg_trickle functions (e.g., `refresh_stream_table()`).

## Platform-Specific Notes

### Supabase

Supabase uses Supavisor in transaction mode by default. pg_trickle's
scheduler works because it runs as a background worker (bypasses the
pooler). Application queries against stream tables work normally through
the pooler since they are regular `SELECT` statements.

If you call `pgtrickle.refresh_stream_table()` from application code,
use the direct connection string (port 5432) rather than the pooled
connection (port 6543).

### Neon

Neon uses a custom proxy that supports both session and transaction modes.
Use the session-mode connection string for any pg_trickle management calls.
The scheduler runs as a background worker and is unaffected by the proxy.

### AWS RDS Proxy

RDS Proxy only supports transaction-mode pooling. The pg_trickle scheduler
runs as a background worker inside the RDS instance and is unaffected.
Application queries reading stream tables work normally through the proxy.

Manual `refresh_stream_table()` calls through the proxy may fail due to
advisory lock issues. Use a direct connection for management operations.

## Pooler Compatibility Mode

pg_trickle includes a `pooler_compatibility_mode` setting that
adjusts internal behavior for environments where the scheduler's SPI
connection may be affected by pooler-like middleware:

```sql
-- Usually not needed — the scheduler bypasses external poolers
SHOW pg_trickle.pooler_compatibility_mode;
```

This GUC is primarily for edge cases in managed PostgreSQL services.
For standard deployments, the default setting works correctly.

## Further Reading

- [Configuration Reference](../CONFIGURATION.md)
- [FAQ — Connection Poolers](../FAQ.md#does-pg_trickle-work-with-pgbouncer-or-other-connection-poolers)
- [Deployment & Operations](../FAQ.md#deployment--operations)
