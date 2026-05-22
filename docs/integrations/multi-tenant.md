# Multi-tenant Deployment Guide

This guide covers recommended deployment patterns for running pg_trickle
across multiple PostgreSQL databases on the same instance, including worker
quota allocation, per-database observability, and Grafana dashboard configuration.

---

## Architecture Overview

In a multi-tenant setup, each PostgreSQL database gets its own pg_trickle
background worker scheduler. All schedulers share a single worker pool via
PostgreSQL shared memory (`ACTIVE_REFRESH_WORKERS` counter). The total number
of concurrent refresh workers is bounded by `pg_trickle.max_dynamic_refresh_workers`.

```
┌─────────────────────────────────────────────┐
│             PostgreSQL instance              │
│                                              │
│  ┌──────────────┐  ┌──────────────┐          │
│  │  tenant_a DB │  │  tenant_b DB │  ...     │
│  │  scheduler   │  │  scheduler   │          │
│  └──────┬───────┘  └──────┬───────┘          │
│         │                 │                  │
│         └────────┬────────┘                  │
│                  ▼                           │
│       Shared worker pool (shmem)             │
│       ACTIVE_REFRESH_WORKERS atomic          │
└─────────────────────────────────────────────┘
```

---

## Worker Quota Formula

When running `N` databases on the same instance, the recommended per-database
worker quota is:

```
per_db_quota = ceil(max_parallel_refresh_workers / N_databases)
```

For example, with `pg_trickle.max_dynamic_refresh_workers = 8` and 4 databases:

```
per_db_quota = ceil(8 / 4) = 2 workers per database
```

Set this in `postgresql.conf` or in each database's `ALTER DATABASE SET`:

```sql
-- Global limit (applies to all databases)
ALTER SYSTEM SET pg_trickle.max_dynamic_refresh_workers = 8;

-- Per-database override (optional, for high-priority tenants)
\c tenant_a
ALTER DATABASE tenant_a SET pg_trickle.max_dynamic_refresh_workers = 4;
```

---

## Monitoring with `cluster_worker_summary()`

The `pgtrickle.cluster_worker_summary()` function returns a real-time view of
worker allocation across all databases visible from the current connection:

```sql
SELECT * FROM pgtrickle.cluster_worker_summary();
```

**Example output:**

| db_oid | db_name | active_workers | scheduler_pid | scheduler_running | total_active_workers |
|--------|---------|---------------|---------------|------------------|---------------------|
| 16384  | tenant_a | 2             | 12345         | true              | 5                   |
| 16385  | tenant_b | 1             | 12346         | true              | 5                   |
| 16386  | tenant_c | 2             | 12347         | true              | 5                   |

The `total_active_workers` column shows the cluster-wide total from shared memory.

---

## Per-Database Prometheus Labels (CLUS-2)

All pg_trickle metrics emitted by the `/metrics` endpoint include `db_oid` and
`db_name` labels. This enables per-database Grafana panels
and alerting rules without requiring separate Prometheus scrape targets.

**Example metric with labels:**

```
pg_trickle_refreshes_total{schema="public",name="orders_agg",db_oid="16384",db_name="tenant_a"} 1247
```

### Configuring Prometheus scrape targets

In a multi-tenant setup, configure one scrape job per database, each pointing
to its own scheduler's metrics port:

```yaml
scrape_configs:
  - job_name: 'pg_trickle_tenant_a'
    static_configs:
      - targets: ['localhost:9101']
        labels:
          instance: 'pg-primary'

  - job_name: 'pg_trickle_tenant_b'
    static_configs:
      - targets: ['localhost:9102']
        labels:
          instance: 'pg-primary'
```

Configure each database's metrics port:

```sql
\c tenant_a
ALTER DATABASE tenant_a SET pg_trickle.metrics_port = 9101;

\c tenant_b
ALTER DATABASE tenant_b SET pg_trickle.metrics_port = 9102;
```

---

## Grafana Dashboard Snippets

### Per-tenant refresh rate panel

```promql
rate(pg_trickle_refreshes_total{db_name=~"$tenant"}[5m])
```

Variable `$tenant` should be a Grafana template variable sourcing from:

```promql
label_values(pg_trickle_refreshes_total, db_name)
```

### Cluster-wide worker utilisation

```promql
sum(pg_trickle_active_workers) by (db_name)
  / scalar(pg_trickle_max_concurrent_refreshes)
```

### Refresh failure rate heatmap

```promql
sum by (db_name, le) (
  rate(pg_trickle_refresh_failures_total{db_name=~"$tenant"}[1h])
)
```

### SLA breach prediction alerts (PLAN-3)

Add this Grafana alert rule to fire when a predicted breach is emitted via NOTIFY:

```sql
-- pg_trickle emits NOTIFY pg_trickle_alert with JSON payload.
-- Parse in your alerting system:
-- {"event":"predicted_sla_breach","pgt_schema":"...","pgt_name":"...",
--   "predicted_ms":...,"sla_ms":...,"pct_over":...}
LISTEN pg_trickle_alert;
```

---

## See Also

- [docs/SCALING.md](../SCALING.md) — cluster-wide fairness and worker budgeting
- [docs/CONFIGURATION.md](../CONFIGURATION.md) — GUC reference
- [docs/SQL_REFERENCE.md](../SQL_REFERENCE.md) — `cluster_worker_summary()` and `metrics_summary()` API
