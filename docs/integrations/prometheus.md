# Prometheus & Grafana Monitoring

pg_trickle ships with a complete observability stack based on
**postgres_exporter**, **Prometheus**, and **Grafana**. The `monitoring/`
directory in the repository contains everything you need.

## Quick Start

```bash
cd monitoring/
docker compose up -d
```

Open Grafana at <http://localhost:3000> (default: `admin` / `admin`).
The **pg_trickle Overview** dashboard is pre-provisioned.

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

`postgres_exporter` runs custom SQL queries defined in
`prometheus/pg_trickle_queries.yml` against the pg_trickle monitoring views
(`pgtrickle.stream_tables_info`, `pgtrickle.pg_stat_stream_tables`, etc.)
and exposes them as Prometheus metrics.

pg_trickle also provides a scheduler-safe native endpoint. Set
`pg_trickle.metrics_port` and, if needed, the literal IPv4/IPv6
`pg_trickle.metrics_bind_address` (default `127.0.0.1`) per database. The
endpoint is available at `/metrics`; `/health` is a lightweight liveness check.
Remote binding (`0.0.0.0` or `::`) should only be used behind network access
controls.

## Connecting to an Existing Database

If you already have PostgreSQL + pg_trickle running, configure the exporter
to point at your instance:

```bash
export PG_HOST=your-pg-host
export PG_PORT=5432
export PG_USER=postgres
export PG_PASSWORD=yourpassword
export PG_DATABASE=yourdb
docker compose up -d
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
| `pg_trickle_target_freshness_seconds` | gauge | Declared interval freshness target per table |
| `pg_trickle_freshness_p50_seconds` | gauge | Exact settled source-commit-to-visible p50 |
| `pg_trickle_freshness_p95_seconds` | gauge | Exact settled source-commit-to-visible p95 |
| `pg_trickle_freshness_p99_seconds` | gauge | Exact settled source-commit-to-visible p99 |
| `pg_trickle_sla_breach_duration_seconds` | gauge | Current exact freshness SLA breach duration |
| `pg_trickle_sla_at_risk_tables` | gauge | Number of interval targets at risk or breaching |
| `pg_trickle_sla_infeasible_tables` | gauge | Number of interval targets proven infeasible |
| `pg_trickle_adaptive_worker_target` | gauge | Advisory worker target when adaptive workers are enabled |

Freshness metrics are emitted only for interval-targeted tables. Per-table
series use the stable bounded labels `db_oid`, `db_name`, `schema`, and `name`.
They never add status, reason detail, query text, source relation, or target
values as labels. Percentiles are omitted when exact settled evidence is NULL;
the scrape reads the stored summary and does not recompute controller state.
Controller status remains available through `pgtrickle.freshness()` and
`pg_stat_pgtrickle`.

## Pre-configured Alerts

Alerting rules are defined in `prometheus/alerts.yml`:

| Alert | Condition | Severity |
|-------|-----------|----------|
| `PgTrickleTableStale` | Staleness > 5 min past schedule | warning |
| `PgTrickleConsecutiveErrors` | ≥ 3 consecutive refresh failures | warning |
| `PgTrickleTableSuspended` | Any table in SUSPENDED status | critical |
| `PgTrickleCdcBufferLarge` | CDC buffer > 1 GB | warning |
| `PgTrickleSchedulerDown` | Scheduler not running for > 2 min | critical |
| `PgTrickleHighRefreshDuration` | Avg refresh > 30 s | warning |
| `PgTrickleFreshnessBreach` | Exact breach duration > 0 for 5 min | warning |
| `PgTrickleFreshnessInfeasible` | Any interval target is infeasible | warning |

## NOTIFY-Based Alerting

In addition to Prometheus alerts, pg_trickle emits real-time PostgreSQL
`NOTIFY` events on the `pg_trickle_alert` channel:

```sql
LISTEN pg_trickle_alert;
```

Events include `stale_data`, `auto_suspended`, `reinitialize_needed`,
`buffer_growth_warning`, `fuse_blown`, `refresh_completed`, and
`refresh_failed`. Each notification carries a JSON payload with the stream
table name and relevant details.

You can bridge NOTIFY events to external alerting systems (PagerDuty, Slack,
etc.) using tools like [pgnotify](https://github.com/ps/pgnotify) or a
simple `LISTEN` loop in your application.

## Grafana Dashboard

The pre-provisioned **pg_trickle Overview** dashboard
(`grafana/dashboards/pg_trickle_overview.json`) includes panels for:

- Stream table status distribution (active / suspended / error)
- Refresh rate and duration over time
- Staleness heatmap
- CDC buffer sizes
- Consecutive error counts
- Scheduler uptime
- Freshness target versus exact p95 and p50/p99
- Sustained breach duration, at-risk and infeasible target counts
- Advisory adaptive worker target

The dashboard uses the same bounded database, schema, and stream-table labels
as the exporter query. `AT_RISK` remains visible without being treated as a
health failure; the breach alert is driven by exact breach duration.

## OpenTelemetry Metrics

When `pg_trickle.otel_endpoint` is configured, pg_trickle sends one bounded
OTLP/HTTP JSON metrics batch per monitoring cadence to
`{endpoint}/v1/metrics`. It exports `pg_trickle.target_freshness`,
`pg_trickle.freshness.p95`, and `pg_trickle.sla.breach_duration` in seconds,
using the same `db_oid`, `db_name`, `schema`, and `name` attributes plus the
controller status. The batch is best-effort and never blocks refreshes.

## Built-in SQL Monitoring Views

pg_trickle also provides built-in monitoring accessible without Prometheus:

```sql
-- Quick health overview (returns warnings and errors)
SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK';

-- Stream table status and staleness
SELECT name, status, refresh_mode, staleness
FROM pgtrickle.stream_tables_info;

-- Detailed refresh statistics
SELECT * FROM pgtrickle.pg_stat_stream_tables;

-- CDC health per source table
SELECT * FROM pgtrickle.check_cdc_health();

-- Change buffer sizes
SELECT * FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;
```

See the [SQL Reference](../SQL_REFERENCE.md#status--monitoring) for the
complete list of monitoring functions.

## Files Reference

| File | Purpose |
|------|---------|
| `monitoring/docker-compose.yml` | Demo stack: PG + exporter + Prometheus + Grafana |
| `monitoring/prometheus/prometheus.yml` | Prometheus scrape configuration |
| `monitoring/prometheus/pg_trickle_queries.yml` | Custom SQL queries for postgres_exporter |
| `monitoring/prometheus/alerts.yml` | Alerting rules |
| `monitoring/grafana/provisioning/` | Auto-provisioned data source + dashboard |
| `monitoring/grafana/dashboards/pg_trickle_overview.json` | Overview dashboard |

## Requirements

- Docker 24+ with Compose v2
- pg_trickle 0.90.0+ installed in the target database
- PostgreSQL user with `SELECT` on the `pgtrickle.*` schema
