# Operator Cheat Sheet

A single-page quick reference for the commands and GUCs you reach for most
often when running pg_trickle in production. Drill deeper with the links at
the end of each section.

---

## Top 5 Diagnostic Queries

```sql
-- 1. Overall health — returns rows only when something is wrong
SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK';

-- 2. Per-table status: refresh mode, last refresh, error count
SELECT * FROM pgtrickle.pgt_status();

-- 3. What would the next differential refresh actually run?
SELECT pgtrickle.explain_diff_sql('schema.stream_table_name');

-- 4. Staleness by table — see how many seconds behind each table is
SELECT st_name, staleness_seconds, last_refresh_at
FROM pgtrickle.get_staleness()
ORDER BY staleness_seconds DESC;

-- 5. Change-buffer sizes — spot tables with growing backlogs
SELECT source_table, buffer_rows, consumer_count
FROM pgtrickle.shared_buffer_stats()
ORDER BY buffer_rows DESC;
```

---

## Top 5 Production GUCs

| GUC | Recommended value | Why |
|---|---|---|
| `pg_trickle.max_concurrent_refreshes` | `4`–`8` | Prevent refresh storms from saturating I/O; tune up for large worker pools |
| `pg_trickle.differential_max_change_ratio` | `0.05`–`0.20` | Fraction of the table that must change before switching from DIFF to FULL refresh; lower = more aggressive DIFF |
| `pg_trickle.delta_work_mem_cap_mb` | `256` (default) | Cap per-refresh memory; raise to `512`–`1024` on large joins |
| `pg_trickle.scheduler_interval_ms` | `500`–`2000` | Refresh loop cadence; lower = fresher data, higher CPU; match to your latency SLA |
| `max_worker_processes` | `≥ 32` | PostgreSQL default of 8 is exhausted quickly; silent scheduler stalls result |

Full reference: [CONFIGURATION.md](CONFIGURATION.md) · [GUC_CATALOG.md](GUC_CATALOG.md)

---

## Lifecycle Commands

```sql
-- Create a stream table (1-second schedule, auto refresh mode)
SELECT pgtrickle.create_stream_table(
    name     => 'schema.my_summary',
    query    => 'SELECT region, SUM(amount) FROM orders GROUP BY region',
    schedule => '1s'
);

-- Force a refresh right now
SELECT pgtrickle.refresh_stream_table('schema.my_summary');

-- Inspect the refresh plan without running it
SELECT * FROM pgtrickle.explain_refresh_mode('schema.my_summary');

-- Suspend and resume (keeps CDC triggers alive)
SELECT pgtrickle.alter_stream_table('schema.my_summary', status => 'SUSPENDED');
SELECT pgtrickle.resume_stream_table('schema.my_summary');

-- Drop (removes storage table, triggers, and catalog entry)
SELECT pgtrickle.drop_stream_table('schema.my_summary');
```

---

## Monitoring & Alerting

```sql
-- Refresh rate and error counts per table (last 24 h)
SELECT st_name, total_refreshes, full_refreshes, diff_refreshes,
       avg_refresh_ms, error_count
FROM pgtrickle.st_refresh_stats();

-- WAL-based CDC slot health
SELECT * FROM pgtrickle.slot_health();

-- Reliability counters (scheduler errors, CDC pause events)
SELECT * FROM pgtrickle.reliability_counters();

-- Worker allocation across databases
SELECT * FROM pgtrickle.worker_allocation_status();
```

Alert rule: `staleness_seconds > 30` from `pgtrickle.get_staleness()` for
any table with a sub-second schedule indicates a stalled scheduler.

Full reference: [TROUBLESHOOTING.md](TROUBLESHOOTING.md) ·
[integrations/prometheus.md](integrations/prometheus.md) ·
[tutorials/MONITORING_AND_ALERTING.md](tutorials/MONITORING_AND_ALERTING.md)

---

## Maintenance Operations

```sql
-- Graceful quiesce before pg_upgrade or a rolling restart
SELECT pgtrickle.drain();
SELECT pgtrickle.is_drained();   -- wait until true

-- Pause CDC without dropping triggers (maintenance window)
SET pg_trickle.cdc_paused = on;
-- ... perform maintenance ...
SET pg_trickle.cdc_paused = off;

-- Convert change buffers to UNLOGGED for faster write path
-- (data in buffers is reconstructable via FULL refresh after crash)
SELECT pgtrickle.convert_buffers_to_unlogged();
```

Full reference: [RUNBOOK_DRAIN.md](RUNBOOK_DRAIN.md) ·
[UPGRADING.md](UPGRADING.md) · [PRE_DEPLOYMENT.md](PRE_DEPLOYMENT.md)

---

## Quick Links

| Need | Go to |
|---|---|
| Something is wrong right now | [TROUBLESHOOTING.md](TROUBLESHOOTING.md) |
| Error code lookup | [ERRORS.md](ERRORS.md) |
| All GUCs with defaults | [GUC_CATALOG.md](GUC_CATALOG.md) |
| All SQL functions | [SQL_REFERENCE.md](SQL_REFERENCE.md) |
| Capacity planning | [CAPACITY_PLANNING.md](CAPACITY_PLANNING.md) |
| Pre-production checklist | [PRE_DEPLOYMENT.md](PRE_DEPLOYMENT.md) |
| Scaling beyond one database | [SCALING.md](SCALING.md) |
| HA and replication | [HA_AND_REPLICATION.md](HA_AND_REPLICATION.md) |
