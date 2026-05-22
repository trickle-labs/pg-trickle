# Scaling Guide

This document provides guidance for scaling pg_trickle to hundreds of stream
tables and beyond. It covers worker pool sizing, scheduler tuning, and
diagnostic queries for identifying bottlenecks.

## Architecture Overview

pg_trickle uses a two-tier background worker model:

1. **Launcher** — one per server. Scans `pg_database` every 10 seconds, spawns
   per-database schedulers, and auto-restarts crashed workers.
2. **Per-database scheduler** — one per database. Wakes every
   `scheduler_interval_ms` (default: 1 s), reads DAG changes from shared memory,
   consumes CDC buffers, and dispatches refreshes.

When `parallel_refresh_mode = 'on'`, the scheduler dispatches refresh work to a
pool of dynamic background workers instead of running refreshes inline.

## Worker Pool Sizing

| Deployment Size | Stream Tables | Recommended `max_dynamic_refresh_workers` | Notes |
|-----------------|---------------|-------------------------------------------|-------|
| Small           | 1–20          | 2–4                                       | Default (4) is usually sufficient |
| Medium          | 20–100        | 4–8                                       | Monitor worker saturation |
| Large           | 100–200       | 8–16                                      | Enable tiered scheduling |
| Very Large      | 200+          | 16–32                                     | Tune per-database quotas |

### Budget Formula

Worker slots are drawn from `max_worker_processes`, which is shared with
autovacuum, parallel queries, and other extensions:

```
max_worker_processes >= launchers(1)
                      + schedulers(N_databases)
                      + max_dynamic_refresh_workers
                      + autovacuum_max_workers
                      + max_parallel_workers
                      + other_extensions
```

**Example for 200 STs across 2 databases with 16 workers:**

```ini
# postgresql.conf
max_worker_processes = 40
pg_trickle.max_dynamic_refresh_workers = 16
pg_trickle.max_concurrent_refreshes = 8
pg_trickle.per_database_worker_quota = 8
pg_trickle.parallel_refresh_mode = 'on'
```

## Tiered Scheduling

For deployments with 50+ stream tables, enable tiered scheduling to reduce
scheduler overhead:

```ini
pg_trickle.tiered_scheduling = on   -- default
```

The scheduler classifies stream tables into tiers based on change frequency:

| Tier | Schedule Multiplier | Behavior |
|------|---------------------|----------|
| Hot  | 1× (base interval)  | Tables with frequent changes |
| Warm | 2×                  | Tables with moderate changes |
| Cold | 10×                 | Tables with rare changes |
| Frozen | skip             | Tables with no recent changes |

This reduces the CPU cost of the scheduling loop itself, which can become a
bottleneck at 200+ STs when every table is polled every cycle.

## Dispatch Priority

When multiple stream tables are ready simultaneously, the scheduler dispatches
in priority order:

1. **IMMEDIATE closures** — time-critical refresh requests
2. **Atomic groups / Repeatable-read groups / Fused chains** — multi-ST units
3. **Singletons** — individual stream tables
4. **Cyclic SCCs** — strongly-connected components

Within each priority band, the tier sort applies (Hot > Warm > Cold).

## Per-Database Quotas and Burst

When `per_database_worker_quota > 0`, each database gets a guaranteed slice
of the worker pool:

- **Normal load** (cluster < 80% capacity): database can burst to 150% of its
  quota using idle capacity from other databases.
- **High load** (cluster ≥ 80% capacity): strict quota enforcement.

This prevents a single high-traffic database from starving others.

## Monitoring

### Worker Pool Status

```sql
SELECT * FROM pgtrickle.worker_pool_status();
-- Returns: active_workers, max_workers, per_db_cap, parallel_mode
```

### Active Job Details

```sql
SELECT * FROM pgtrickle.parallel_job_status(300);
-- Returns recent jobs (last 300s): status, duration, worker PID, etc.
```

### Health Summary

```sql
SELECT * FROM pgtrickle.health_summary();
-- Returns: total/active/error/suspended/stale counts, scheduler status, cache hit rate
```

### Buffer Backlog Check

```sql
SELECT * FROM pgtrickle.change_buffer_sizes()
ORDER BY row_count DESC
LIMIT 20;
```

### Identifying Bottlenecks

**Is the scheduler loop the bottleneck?**

```sql
-- If queue depth is consistently > 10 and workers are not saturated,
-- the scheduler loop is the bottleneck. Reduce scheduler_interval_ms.
SELECT active_workers, max_workers
FROM pgtrickle.worker_pool_status();
```

**Are workers saturated?**

```sql
-- If active_workers == max_workers consistently, increase the pool.
SELECT active_workers >= max_workers AS saturated
FROM pgtrickle.worker_pool_status();
```

**Which STs take the longest?**

```sql
SELECT st.pgt_schema, st.pgt_name,
       AVG(EXTRACT(EPOCH FROM (h.end_time - h.start_time))) AS avg_sec,
       MAX(EXTRACT(EPOCH FROM (h.end_time - h.start_time))) AS max_sec,
       COUNT(*) AS refreshes
FROM pgtrickle.pgt_refresh_history h
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
WHERE h.start_time > now() - interval '1 hour'
  AND h.status = 'COMPLETED'
GROUP BY st.pgt_schema, st.pgt_name
ORDER BY avg_sec DESC
LIMIT 20;
```

## Tuning Profiles

### Low-Latency (< 50 ms P99)

```ini
pg_trickle.scheduler_interval_ms = 200
pg_trickle.parallel_refresh_mode = 'on'
pg_trickle.max_dynamic_refresh_workers = 8
pg_trickle.tiered_scheduling = on
```

### High-Throughput (200+ STs)

```ini
pg_trickle.scheduler_interval_ms = 500
pg_trickle.parallel_refresh_mode = 'on'
pg_trickle.max_dynamic_refresh_workers = 16
pg_trickle.max_concurrent_refreshes = 8
pg_trickle.per_database_worker_quota = 8
pg_trickle.tiered_scheduling = on
pg_trickle.merge_work_mem_mb = 128
```

### Resource-Constrained (4 CPU / 8 GB RAM)

```ini
pg_trickle.scheduler_interval_ms = 2000
pg_trickle.parallel_refresh_mode = 'on'
pg_trickle.max_dynamic_refresh_workers = 2
pg_trickle.max_concurrent_refreshes = 2
pg_trickle.tiered_scheduling = on
pg_trickle.delta_work_mem_cap_mb = 256
pg_trickle.merge_work_mem_mb = 32
```

## Profiling Methodology

To profile worker utilization at scale, run a test with 200+ stream tables
and `max_workers` set to 4, 8, and 16 in turn. Collect the following metrics
at 1-second intervals:

```sql
-- Worker pool utilization over time
SELECT now() AS ts,
       (SELECT active_workers FROM pgtrickle.worker_pool_status()) AS active,
       (SELECT max_workers FROM pgtrickle.worker_pool_status()) AS pool_size,
       (SELECT COUNT(*) FROM pgtrickle.parallel_job_status(5)
        WHERE status = 'QUEUED') AS queue_depth;
```

Plot `active / pool_size` (utilization) and `queue_depth` over time.
If utilization is consistently > 90% with non-zero queue depth, the pool
is undersized. If utilization is < 50%, the pool is oversized and consuming
`max_worker_processes` slots unnecessarily.

## Known Scaling Limits

| Resource              | Practical Limit | Bottleneck |
|-----------------------|-----------------|------------|
| Stream tables per DB  | ~500            | Scheduler loop CPU |
| Worker pool size      | 64              | GUC max |
| Change buffer rows    | `max_buffer_rows` (default 1M) | Disk I/O |
| Template cache size   | 128 entries (L1) | Evictions increase at >128 STs |
| DAG depth             | ~20 levels      | Topological sort + cascade latency |

---

## Read Replicas & Hot Standby

pg_trickle is a **primary-only** extension. Stream tables are maintained
by the background scheduler through DML (INSERT, DELETE, MERGE), which is
only possible on the primary server.

### Behaviour on Replicas

When the pg_trickle shared library is loaded on a **read replica** (physical
standby or streaming replica):

1. The **launcher worker** detects `pg_is_in_recovery() = true` and enters
   a sleep loop, checking every 30 seconds for promotion.
2. Upon **promotion** (e.g. `pg_promote()`), the launcher resumes normal
   operation and spawns per-database schedulers.
3. **Manual refresh** calls (`pgtrickle.refresh_stream_table()`) on a replica
   are rejected with a clear error message.

### Recommended Setup

- Include `pg_trickle` in `shared_preload_libraries` on **both** primary and
  replicas. This ensures immediate availability after failover without a restart.
- Stream tables are read-queryable on replicas via physical replication —
  the storage tables are regular PostgreSQL tables that replicate normally.
- Monitor the replication lag to estimate stream table staleness on replicas.

---

## CNPG & Kubernetes Operations

[CloudNativePG (CNPG)](https://cloudnative-pg.io/) is the recommended Kubernetes
operator for running pg_trickle. The extension is packaged as a custom container
image that extends the official PostgreSQL image.

### Container Image

Build the pg_trickle image using the provided Dockerfiles:

```bash
# GHCR image (multi-stage build)
docker build -f Dockerfile.ghcr -t pg-trickle:latest .

# Or use the CNPG-specific Dockerfile
docker build -f cnpg/Dockerfile.ext -t pg-trickle-cnpg:latest .
```

### CNPG Cluster Configuration

```yaml
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: pg-trickle-cluster
spec:
  instances: 3
  imageName: your-registry/pg-trickle:0.19.0
  postgresql:
    shared_preload_libraries:
      - pg_trickle
    parameters:
      pg_trickle.enabled: "true"
      pg_trickle.scheduler_interval_ms: "1000"
      pg_trickle.max_concurrent_refreshes: "4"
      # STAB-1: If using PgBouncer sidecar in transaction mode:
      # pg_trickle.connection_pooler_mode: "transaction"
```

### Operational Notes

- **Failover**: pg_trickle detects promotion automatically (see Read Replicas
  above). After CNPG promotes a replica, the launcher starts within 30 seconds.
- **Scaling replicas**: Stream table data replicates to all replicas via
  physical replication. No pg_trickle-specific configuration needed on replicas.
- **Backup**: Use CNPG's built-in Barman backup. pg_trickle's catalog tables
  are included automatically. See [Backup & Restore](BACKUP_AND_RESTORE.md).
- **Monitoring**: The Prometheus endpoint (`pgtrickle.health_summary()`) is
  compatible with CNPG's monitoring sidecar. See the Grafana dashboards in
  `monitoring/grafana/`.

---

## Cluster-wide Worker Fairness

When pg_trickle is installed across multiple databases on the same PostgreSQL
instance, all scheduler background workers share a single worker pool bounded
by `pg_trickle.max_dynamic_refresh_workers`. Without care, high-throughput
databases can starve lower-priority databases of worker slots.

### Quota allocation

Use the quota formula to distribute workers fairly:

```
per_db_quota = ceil(max_dynamic_refresh_workers / N_databases)
```

For high-priority databases, increase their individual quota via `ALTER DATABASE SET`:

```sql
ALTER DATABASE tenant_high SET pg_trickle.max_dynamic_refresh_workers = 4;
```

### Monitoring cluster-wide allocation

Use `pgtrickle.cluster_worker_summary()` to monitor allocation in real time:

```sql
SELECT db_name, active_workers, total_active_workers
FROM pgtrickle.cluster_worker_summary()
ORDER BY active_workers DESC;
```

See [docs/integrations/multi-tenant.md](integrations/multi-tenant.md) for the
complete multi-tenant deployment guide, Prometheus configuration, and Grafana
dashboard snippets.

### Per-database Prometheus labels

All metrics include `db_oid` and `db_name` labels so Grafana
dashboards can filter by database without requiring separate scrape targets:

```promql
rate(pg_trickle_refreshes_total{db_name="tenant_a"}[5m])
```

---

**See also:**
[Capacity Planning](CAPACITY_PLANNING.md) ·
[Configuration](CONFIGURATION.md) ·
[Multi-Database Deployments](MULTI_DATABASE.md) ·
[Performance Cookbook](PERFORMANCE_COOKBOOK.md) ·
[Cost Model](COST_MODEL.md) ·
[Pre-Deployment Checklist](PRE_DEPLOYMENT.md)
