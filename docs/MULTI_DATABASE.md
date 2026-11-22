# Multi-Database Deployments

pg_trickle is **multi-database aware**. A single PostgreSQL server
can host pg_trickle in any number of databases simultaneously, and
the extension's background workers handle the fan-out automatically.
You do not need to start anything per database; the launcher
discovers them.

---

## Architecture in one diagram

```
┌─────────────────────────────────────────────────────────────┐
│                  PostgreSQL 18 server                        │
│                                                              │
│  ┌────────────┐                                              │
│  │  Launcher  │  ── scans pg_database every ~10 s            │
│  └─────┬──────┘                                              │
│        │ spawns                                              │
│   ┌────┼────────────────────┬────────────────────┐           │
│   ▼    ▼                    ▼                    ▼           │
│ ┌──────────┐         ┌──────────┐         ┌──────────┐       │
│ │Scheduler │         │Scheduler │         │Scheduler │       │
│ │   db_a   │         │   db_b   │         │  db_etl  │       │
│ └────┬─────┘         └────┬─────┘         └────┬─────┘       │
│      │ refresh jobs       │ refresh jobs       │             │
│      ▼                    ▼                    ▼             │
│  ┌─────────────────────────────────────────────────┐         │
│  │ Shared dynamic refresh worker pool               │         │
│  │ (max_dynamic_refresh_workers, per-DB quotas)     │         │
│  └─────────────────────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

- **One launcher** per PostgreSQL server.
- **One scheduler** per database that has `pg_trickle` installed.
- **One shared dynamic worker pool** for parallel refreshes.

The launcher restarts crashed schedulers automatically. Each
scheduler is fully independent; failure in one database does not
affect the others.

---

## Enabling pg_trickle in additional databases

Just install the extension. The launcher will pick it up on the
next discovery cycle (within ~10 s).

```sql
\c db_b
CREATE EXTENSION pg_trickle;
```

Verify the scheduler started:

```sql
SELECT * FROM pgtrickle.cluster_worker_summary();
```

Expected: one row per database with a column showing the scheduler
PID and uptime.

---

## Resource budgeting across databases

Background-worker slots are a finite, server-wide resource. The
formula:

```
max_worker_processes ≥ launchers(1)
                     + schedulers(N_databases)
                     + max_dynamic_refresh_workers
                     + autovacuum_max_workers
                     + max_parallel_workers
                     + other_extensions
```

For 4 databases each running pg_trickle, with 8 dynamic refresh
workers and modest autovacuum:

```ini
max_worker_processes                    = 32
max_parallel_workers                    = 8
pg_trickle.max_dynamic_refresh_workers  = 8
pg_trickle.per_database_worker_quota    = 4
```

Without `per_database_worker_quota`, one busy database can starve
the others. Set it to `max_dynamic_refresh_workers / N_databases`
or higher.

---

## Cluster-wide observability

```sql
-- One row per database with per-database stats
SELECT * FROM pgtrickle.cluster_worker_summary();

-- Combined health across every database
SELECT datname, severity, message
FROM pgtrickle.cluster_health_check()    -- (where exposed)
WHERE severity != 'OK';
```

The `pgtrickle workers` command also aggregates
across databases when invoked against a server-level URL.

---

## Common patterns

### Per-tenant database

If you isolate tenants by database, each gets its own scheduler.
Combined with `tiered_scheduling = on`, low-traffic tenants pay
almost no scheduler cost.

### App database + analytics database

A common topology: `app_db` runs OLTP and a few `IMMEDIATE` stream
tables; `analytics_db` (often a logical replica) runs the heavy
DIFFERENTIAL aggregates. The launcher handles both.

### Tenant-of-tenants (Citus)

For very large multi-tenant deployments, see
[Citus](CITUS.md) — distributed sources can replace per-tenant
databases.

---

## Caveats

- pg_trickle does **not** create cross-database stream tables.
  Stream tables live in exactly one database; their sources must
  live there too. Use logical replication or
  [downstream publications](PUBLICATIONS.md) to bridge databases.
- `shared_preload_libraries = 'pg_trickle'` is server-wide. The
  launcher then discovers per-database state.
- A server-wide `pg_trickle.enabled = off` pauses automatic refreshes,
  **not** the launcher or the scheduler processes. A client-session `SET` does
  not affect these workers. Use `ALTER SYSTEM` plus `pg_reload_conf()` for a
  server-wide change; database-level overrides require their scheduler worker
  to restart. See [Configuration](CONFIGURATION.md#pg_trickleenabled).

---

**See also:**
[Scaling Guide](SCALING.md) ·
[Capacity Planning](CAPACITY_PLANNING.md) ·
[Configuration](CONFIGURATION.md)
