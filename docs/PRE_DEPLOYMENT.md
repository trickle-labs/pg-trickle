# Pre-Deployment Checklist

Complete this checklist before deploying pg_trickle to a new environment.
Each item links to the relevant documentation for details.

---

## 1. PostgreSQL Version

- [ ] **PostgreSQL 18.x** is required (pg_trickle is compiled against PG 18)
- [ ] Extension binary matches your exact PostgreSQL major version

```sql
SELECT version();  -- Must show PostgreSQL 18.x
```

---

## 2. shared_preload_libraries

pg_trickle **must** be loaded at server startup via `shared_preload_libraries`.
Without this, GUC variables and the background scheduler are not available.

```ini
# postgresql.conf
shared_preload_libraries = 'pg_trickle'
```

- [ ] `shared_preload_libraries` includes `pg_trickle`
- [ ] PostgreSQL has been **restarted** after changing this setting (reload is not sufficient)

```sql
SHOW shared_preload_libraries;  -- Must include pg_trickle
```

> **Managed PostgreSQL:** Some providers (Supabase, Neon) do not support
> custom `shared_preload_libraries`. Check your provider's extension
> compatibility list. AWS RDS and Google Cloud SQL support custom
> shared libraries via parameter groups.

---

## 3. WAL Configuration (Optional but Recommended)

pg_trickle works without `wal_level = logical` — it uses trigger-based
CDC by default. However, WAL-based CDC provides lower overhead on
write-heavy workloads.

```ini
# postgresql.conf (optional — for WAL-based CDC)
wal_level = logical
max_replication_slots = 10   # At least 1 per tracked source table
```

- [ ] Decide: trigger-based CDC (default) or WAL-based CDC
- [ ] If WAL: `wal_level = logical` and server restarted
- [ ] If WAL: `max_replication_slots` is sufficient for your source table count

> **Note:** CDC mode is configurable per stream table. The default
> `cdc_mode = 'auto'` starts with triggers and transitions to WAL
> automatically when `wal_level = logical` is detected. See
> [CONFIGURATION.md](CONFIGURATION.md#pg_tricklecdc_mode) for details.

---

## 4. Extension Installation

```sql
CREATE EXTENSION pg_trickle;

-- Verify installation
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_trickle';
```

- [ ] Extension created successfully
- [ ] Version matches expected release

---

## 5. Background Scheduler

The scheduler runs as a background worker and manages automatic refresh.
Verify it's running:

```sql
SELECT pid, backend_type, state
FROM pg_stat_activity
WHERE backend_type = 'pg_trickle scheduler';
```

- [ ] Scheduler process is visible in `pg_stat_activity`
- [ ] `pg_trickle.enabled = true` (default; set to `false` to disable)

---

## 6. Connection Pooler Compatibility

### PgBouncer (Transaction Mode)

PgBouncer in transaction pooling mode drops session state between
transactions. pg_trickle needs special handling:

- [ ] Enable `pooler_compatibility_mode` on affected stream tables:

```sql
SELECT pgtrickle.alter_stream_table('my_st',
    pooler_compatibility_mode => true);
```

- [ ] Or set globally via GUC:

```ini
pg_trickle.pooler_compatibility_mode = true
```

### PgBouncer (Session Mode)

Session mode preserves session state — no special configuration needed.

### Supavisor / Other Poolers

Some poolers (Supavisor, pgcat) have their own compatibility
characteristics. Test with `pgtrickle.validate_query()` before deploying.

---

## 7. Recommended GUC Starting Values

These are sensible defaults for most workloads. Adjust based on
monitoring data.

```ini
# Core settings (usually fine as defaults)
pg_trickle.enabled = true                    # Enable scheduler
pg_trickle.schedule_interval = '5s'          # Global default refresh interval
pg_trickle.max_concurrent_refreshes = 4      # Parallel refresh limit

# Performance tuning
pg_trickle.planner_aggressive = true         # Enable MERGE planner hints
pg_trickle.tiered_scheduling = true          # Tier-aware scheduling

# CDC mode
pg_trickle.cdc_mode = 'auto'                # auto | trigger | wal

# Safety
pg_trickle.change_buffer_durability = logged # sync for strongest commit durability
pg_trickle.fuse_default_ceiling = 10000      # Auto-fuse change threshold
```

- [ ] Review GUC values for your workload
- [ ] See [CONFIGURATION.md](CONFIGURATION.md) for the full reference

---

## 8. Resource Planning

### Memory

- Each background worker uses a separate PostgreSQL backend
- `work_mem` applies to each worker's delta SQL execution
- Monitor RSS growth via `pg_stat_activity` or OS-level tools

### Storage

- Change buffer tables (`pgtrickle_changes.changes_*`) grow between refreshes
- Buffer size depends on DML rate × refresh interval
- Monitor via `pgtrickle.shared_buffer_stats()`

### Connections

- The scheduler uses `pg_trickle.max_concurrent_refreshes` backend connections
- Ensure `max_connections` has headroom for workers + application

- [ ] `max_connections` is at least application connections + `pg_trickle.max_concurrent_refreshes` + 5

---

## 9. Monitoring Setup

### Essential Queries

```sql
-- Stream table health overview
SELECT pgt_name, status, staleness, refresh_mode
FROM pgtrickle.stream_tables_info
ORDER BY staleness DESC NULLS LAST;

-- Refresh efficiency
SELECT pgt_name, diff_speedup, avg_change_ratio
FROM pgtrickle.refresh_efficiency();

-- Error states
SELECT pgt_name, status, last_error_message, last_error_at
FROM pgtrickle.pgt_stream_tables
WHERE status IN ('ERROR', 'SUSPENDED');
```

### Grafana / Prometheus

See the [monitoring/](https://github.com/trickle-labs/pg-trickle/blob/main/monitoring/) directory for ready-to-use
Grafana dashboards and Prometheus configuration.

- [ ] Monitoring configured for stream table health
- [ ] Alerting on ERROR/SUSPENDED status

---

## 10. Backup & Restore

pg_trickle stream tables are standard PostgreSQL tables and are included
in `pg_dump` / `pg_restore`. See [BACKUP_AND_RESTORE.md](BACKUP_AND_RESTORE.md)
for details.

- [ ] Backup strategy accounts for both source tables and stream tables
- [ ] Restore procedure tested (stream tables may need re-initialization)

---

## Quick Validation Script

Run this after deployment to verify everything is working:

```sql
-- 1. Extension loaded
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_trickle';

-- 2. Scheduler running
SELECT COUNT(*) > 0 AS scheduler_alive
FROM pg_stat_activity
WHERE backend_type = 'pg_trickle scheduler';

-- 3. Create a test stream table
CREATE TABLE _deploy_test_src (id INT PRIMARY KEY, val INT);
INSERT INTO _deploy_test_src VALUES (1, 100), (2, 200);

SELECT pgtrickle.create_stream_table(
    '_deploy_test_st',
    'SELECT id, val FROM _deploy_test_src',
    refresh_mode => 'FULL'
);

SELECT pgtrickle.refresh_stream_table('_deploy_test_st');

-- 4. Verify data
SELECT * FROM _deploy_test_st ORDER BY id;
-- Expected: (1, 100), (2, 200)

-- 5. Cleanup
SELECT pgtrickle.drop_stream_table('_deploy_test_st');
DROP TABLE _deploy_test_src;
```

---

## Connection Pooler Compatibility

pg_trickle uses prepared statements and `NOTIFY` internally. These features
require special handling when a connection pooler sits between the application
and PostgreSQL.

### PgBouncer Transaction Mode

In PgBouncer **transaction pooling** mode, each transaction may land on a
different server-side connection. Prepared statements and LISTEN/NOTIFY do
not survive across transactions.

**Recommended configuration:**

```ini
# postgresql.conf
pg_trickle.connection_pooler_mode = 'transaction'
```

This cluster-wide GUC:
- Disables prepared-statement reuse for all stream tables.
- Suppresses `NOTIFY pg_trickle_refresh` emissions (listeners on other
  connections will not receive them anyway in transaction mode).

Alternatively, enable pooler compatibility per stream table:

```sql
SELECT pgtrickle.alter_stream_table('my_stream_table',
    pooler_compatibility_mode => true);
```

### PgBouncer Session Mode

Session pooling is fully compatible — no special configuration needed.

### pgcat / Supavisor

These poolers generally support prepared statements and NOTIFY. Set
`pg_trickle.connection_pooler_mode = 'off'` (the default).

### Kubernetes / CNPG

See [Scaling — CNPG](SCALING.md#cnpg--kubernetes-operations) for connection
pooler configuration in Kubernetes environments.

---

## Row-Level Security

> **Important:** pg_trickle background workers execute refresh queries with
> `SET LOCAL row_security = off`. This is intentional and matches the semantics
> of PostgreSQL's `REFRESH MATERIALIZED VIEW`.

### Implications

- Stream table output always contains the **full, unfiltered result set** regardless of RLS policies on source tables.
- Row-Level Security policies on source tables **do not filter** what ends up in a stream table.
- If the source table has RLS and the defining query selects `*`, all rows (including those that would be hidden by RLS for normal roles) will be included in the stream table.

### Mitigations

- [ ] Audit all stream table queries: ensure sensitive columns are excluded or aggregated.
- [ ] Do not expose stream tables directly to end-user roles if the source tables are protected by RLS.
- [ ] Use a per-role VIEW on top of the stream table to re-apply filtering: `CREATE VIEW orders_view AS SELECT * FROM order_totals_st WHERE user_id = current_user_id()`.
- [ ] Consider column-level masking extensions (e.g., `anon`) on the stream table output view.
- [ ] Review `pgtrickle.list_stream_tables()` output for any stream tables selecting from RLS-protected sources.

---

## Related Documentation

- [Getting Started](GETTING_STARTED.md) — First stream table in 5 minutes
- [Configuration Reference](CONFIGURATION.md) — All GUC variables
- [SQL Reference](SQL_REFERENCE.md) — Complete function reference
- [Best-Practice Patterns](PATTERNS.md) — Common data modeling patterns
- [Architecture](ARCHITECTURE.md) — How pg_trickle works internally
- [Backup & Restore](BACKUP_AND_RESTORE.md) — Backup considerations
