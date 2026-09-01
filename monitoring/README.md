# pg_trickle Monitoring Stack

This directory provides a complete observability setup for pg_trickle using
**postgres_exporter**, **Prometheus**, and **Grafana**.

## Quick Start

```bash
docker compose up -d
```

Then open Grafana at <http://localhost:3000> (default credentials: `admin` /
`admin`). The **pg_trickle Overview** dashboard is pre-provisioned.

## Architecture

```
PostgreSQL + pg_trickle
        │
        │  custom SQL queries
        ▼
postgres_exporter (:9187)
        │
        │  /metrics (Prometheus format)
        ▼
   Prometheus (:9090)
        │
        │  data source
        ▼
    Grafana (:3000)
```

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Demo stack: PG + pg_trickle + exporter + Prometheus + Grafana |
| `prometheus/prometheus.yml` | Prometheus scrape configuration |
| `prometheus/pg_trickle_queries.yml` | postgres_exporter custom SQL queries (OBS-1) |
| `prometheus/alerts.yml` | Alerting rules: staleness, errors, CDC lag (OBS-2) |
| `grafana/provisioning/datasources/prometheus.yml` | Auto-provisioned data source |
| `grafana/provisioning/dashboards/provider.yml` | Dashboard provisioning config |
| `grafana/dashboards/pg_trickle_overview.json` | Overview dashboard (OBS-3) |

## Connecting to an Existing PostgreSQL Instance

If you already have PostgreSQL + pg_trickle running, set these environment
variables before `docker compose up`:

```bash
export PG_HOST=your-pg-host
export PG_PORT=5432
export PG_USER=postgres
export PG_PASSWORD=yourpassword
export PG_DATABASE=yourdb
```

Or edit the `DATA_SOURCE_NAME` in `docker-compose.yml` directly.

## Metrics Exposed

All metrics are prefixed `pg_trickle_`.

| Metric | Type | Description |
|--------|------|-------------|
| `pg_trickle_stream_tables_total` | gauge | Total stream tables by status |
| `pg_trickle_stale_tables_total` | gauge | Tables with data older than schedule |
| `pg_trickle_consecutive_errors` | gauge | Per-table consecutive error count |
| `pg_trickle_refresh_duration_ms` | gauge | Average refresh duration (ms) |
| `pg_trickle_total_refreshes` | counter | Total refresh count per table |
| `pg_trickle_failed_refreshes` | counter | Failed refresh count per table |
| `pg_trickle_rows_inserted_total` | counter | Rows inserted per table |
| `pg_trickle_rows_deleted_total` | counter | Rows deleted per table |
| `pg_trickle_staleness_seconds` | gauge | Seconds since last successful refresh |
| `pg_trickle_cdc_pending_rows` | gauge | Pending rows in CDC change buffer |
| `pg_trickle_cdc_buffer_bytes` | gauge | CDC change buffer size in bytes |
| `pg_trickle_scheduler_running` | gauge | 1 if scheduler background worker is alive |
| `pg_trickle_health_status` | gauge | Overall health: 0=OK, 1=WARNING, 2=CRITICAL |
| `pg_trickle_cache_l1_hits` | counter | Template cache L1 hits (avoids delta SQL regeneration) |
| `pg_trickle_cache_l1_evictions` | counter | Template cache L1 evictions |
| `pg_trickle_cache_delta_cache_entries` | gauge | Current delta template cache entries |
| `pg_trickle_health_summary_cache_hit_rate` | gauge | Template cache hit rate (0.0–1.0) |
| `pg_trickle_health_summary_p99_refresh_ms_1h` | gauge | P99 refresh latency over 1 hour (ms) |
| `pg_trickle_health_summary_avg_refresh_ms_1h` | gauge | Average refresh latency over 1 hour (ms) |
| `pg_trickle_health_summary_total_refreshes_1h` | gauge | Total refreshes in last hour |
| `pg_trickle_health_summary_failed_refreshes_1h` | gauge | Failed refreshes in last hour |
| `pg_trickle_target_freshness_seconds` | gauge | Declared interval freshness target per table |
| `pg_trickle_freshness_p50_seconds` | gauge | Exact settled source-commit-to-visible p50 |
| `pg_trickle_freshness_p95_seconds` | gauge | Exact settled source-commit-to-visible p95 |
| `pg_trickle_freshness_p99_seconds` | gauge | Exact settled source-commit-to-visible p99 |
| `pg_trickle_sla_breach_duration_seconds` | gauge | Current exact freshness SLA breach duration |
| `pg_trickle_sla_at_risk_tables` | gauge | Interval targets at risk or breaching |
| `pg_trickle_sla_infeasible_tables` | gauge | Interval targets proven infeasible |
| `pg_trickle_adaptive_worker_target` | gauge | Advisory worker target when enabled |

Per-table freshness series use only the bounded labels `db_oid`, `db_name`,
`schema`, and `name`. Status, reason, query text, source relation, and target
values are metric values, not labels. Percentiles are omitted until exact
settled evidence exists. The exporter query reads the stored controller
summary; it does not recompute it.

## Alerting

Alerts are defined in `prometheus/alerts.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| `PgTrickleTableStale` | Staleness > 5 minutes past schedule | warning |
| `PgTrickleConsecutiveErrors` | ≥ 3 consecutive refresh failures | warning |
| `PgTrickleTableSuspended` | Any table in SUSPENDED status | critical |
| `PgTrickleCdcBufferLarge` | CDC buffer > 1 GB | warning |
| `PgTrickleSchedulerDown` | Scheduler not running for > 2 minutes | critical |
| `PgTrickleHighRefreshDuration` | Avg refresh > 30 s | warning |
| `PgTrickleFreshnessBreach` | Exact breach duration > 0 for 5 minutes | warning |
| `PgTrickleFreshnessInfeasible` | Any interval target is infeasible | warning |

The dashboard’s v0.90 freshness row shows target versus exact p95, p50/p99,
breach duration, at-risk targets, infeasible targets, and the adaptive worker
target. `AT_RISK` is informational; only a sustained `BREACHING` state alerts.

## Requirements

- Docker 24+ with Compose v2
- pg_trickle 0.90.0+ installed in the target database
- PostgreSQL user with `SELECT` on `pgtrickle.*` schema
