# Tutorial: Monitoring & Alerting

This guide consolidates all pg_trickle monitoring capabilities into a
single reference: built-in SQL views, NOTIFY-based alerts, and the
Prometheus/Grafana observability stack.

## Quick Health Check

The fastest way to verify pg_trickle is healthy:

```sql
SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK';
```

If this returns no rows, everything is working. Any `WARN` or `ERROR` rows
tell you where to investigate.

## Built-in Monitoring Views

### Stream table status

```sql
-- Overview: name, status, mode, staleness
SELECT name, status, refresh_mode, staleness, stale
FROM pgtrickle.stream_tables_info;

-- Detailed stats: refresh counts, duration, error streaks
SELECT pgt_name, total_refreshes, avg_duration_ms, consecutive_errors, stale
FROM pgtrickle.pg_stat_stream_tables;

-- Live status with error counts
SELECT * FROM pgtrickle.pgt_status();
```

### Refresh history

```sql
-- Last 10 refreshes for a specific stream table
SELECT start_time, action, status, duration_ms, rows_inserted, rows_deleted, error_message
FROM pgtrickle.get_refresh_history('order_totals', 10);

-- Global refresh timeline (last 20 events across all stream tables)
SELECT start_time, stream_table, action, status, duration_ms, error_message
FROM pgtrickle.refresh_timeline(20);

-- Aggregate refresh statistics
SELECT * FROM pgtrickle.st_refresh_stats();
```

### CDC pipeline health

```sql
-- Per-source CDC mode, WAL lag, and alerts
SELECT * FROM pgtrickle.check_cdc_health();

-- Change buffer sizes (pending changes not yet consumed)
SELECT stream_table, source_table, cdc_mode, pending_rows, buffer_bytes
FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;

-- Verify all CDC triggers are installed and enabled
SELECT source_table, trigger_type, trigger_name
FROM pgtrickle.trigger_inventory()
WHERE NOT present OR NOT enabled;
```

### Dependencies

```sql
-- ASCII tree view of the entire dependency graph
SELECT tree_line, status, refresh_mode
FROM pgtrickle.dependency_tree();

-- Diamond consistency groups
SELECT * FROM pgtrickle.diamond_groups();
```

### Fuse circuit breaker

```sql
-- Check fuse state for all stream tables
SELECT name, fuse_mode, fuse_state, fuse_ceiling, blown_at
FROM pgtrickle.fuse_status();
```

### Parallel workers

```sql
-- Worker pool status (when parallel_refresh_mode = 'on')
SELECT * FROM pgtrickle.worker_pool_status();

-- Recent parallel job history
SELECT job_id, unit_key, status, duration_ms
FROM pgtrickle.parallel_job_status(60);
```

## NOTIFY-Based Alerting

pg_trickle emits real-time events via PostgreSQL's `NOTIFY` system:

```sql
LISTEN pg_trickle_alert;
```

### Event Types

| Event | Trigger | Severity |
|-------|---------|----------|
| `stale_data` | Scheduler is also behind — view is genuinely out of date | Warning |
| `no_upstream_changes` | Scheduler is healthy but source tables have had no writes — view is correct | Info |
| `auto_suspended` | Stream table suspended after max consecutive errors | Critical |
| `resumed` | Stream table resumed after suspension | Info |
| `reinitialize_needed` | Upstream DDL change detected | Warning |
| `buffer_growth_warning` | Change buffer growing unexpectedly | Warning |
| `slot_lag_warning` | WAL replication slot retaining excessive data | Warning |
| `fuse_blown` | Circuit breaker tripped | Warning |
| `refresh_completed` | Refresh completed successfully | Info |
| `refresh_failed` | Refresh failed | Error |
| `diamond_partial_failure` | One member of an atomic diamond group failed | Warning |
| `scheduler_falling_behind` | Refresh duration approaching the schedule interval | Warning |
| `spill_threshold_exceeded` | Delta MERGE spilled to temp files for consecutive refreshes, forcing FULL | Warning |

### Notification Payload

Each notification carries a JSON payload:

```json
{
  "event": "auto_suspended",
  "stream_table": "order_totals",
  "consecutive_errors": 3,
  "last_error": "column \"deleted_column\" does not exist",
  "timestamp": "2026-03-31T14:22:01.123Z"
}
```

### Bridging to External Systems

To forward NOTIFY events to external alerting systems (PagerDuty, Slack,
OpsGenie), use a listener process:

```python
# Example: Python listener using psycopg
import psycopg
import json

conn = psycopg.connect("postgresql://user:pass@host/db", autocommit=True)
conn.execute("LISTEN pg_trickle_alert")

for notify in conn.notifies():
    payload = json.loads(notify.payload)
    event = payload["event"]
    # no_upstream_changes is informational — source tables are quiet but healthy.
    # Only page on actionable events.
    if event in ("auto_suspended", "fuse_blown", "refresh_failed"):
        send_to_pagerduty(payload)
    elif event == "stale_data":  # scheduler itself is falling behind
        send_to_pagerduty(payload)
```

## Prometheus & Grafana Stack

For the native scheduler endpoint, set `pg_trickle.metrics_port` and leave
`pg_trickle.metrics_bind_address` at its loopback default unless remote
scraping is explicitly required. Only literal IPv4/IPv6 bind addresses are
accepted; use `0.0.0.0` or `::` with appropriate network controls.

For production deployments, use the pre-built observability stack in the
`monitoring/` directory:

```bash
cd monitoring/
docker compose up -d
```

This gives you:
- **Prometheus** scraping pg_trickle metrics via postgres_exporter
- **Grafana** with a pre-provisioned dashboard
- **Alerting rules** for staleness, errors, CDC lag, and scheduler health

See [Prometheus & Grafana Integration](../integrations/prometheus.md) for
full setup details.

## Diagnostic Workflow

When something is wrong, follow this systematic workflow:

### Step 1 — Global health

```sql
SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK';
```

### Step 2 — Status and staleness

```sql
SELECT name, status, consecutive_errors, staleness
FROM pgtrickle.pgt_status()
ORDER BY staleness DESC NULLS FIRST;
```

### Step 3 — Recent refresh activity

```sql
SELECT start_time, stream_table, action, status, error_message
FROM pgtrickle.refresh_timeline(20);
```

### Step 4 — Error details for a specific stream table

```sql
SELECT * FROM pgtrickle.diagnose_errors('my_stream_table');
```

### Step 5 — CDC pipeline

```sql
SELECT stream_table, source_table, pending_rows, buffer_bytes
FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;
```

### Step 6 — Trigger verification

```sql
SELECT source_table, trigger_type, trigger_name
FROM pgtrickle.trigger_inventory()
WHERE NOT present OR NOT enabled;
```

## Common Alert Responses

| Alert | Likely Cause | Action |
|-------|-------------|--------|
| `stale_data` | Scheduler behind, long refresh, or lock contention | Check `pgt_status()` and `refresh_timeline()` |
| `auto_suspended` | Repeated refresh failures | Fix root cause, then `resume_stream_table()` |
| `fuse_blown` | Bulk load exceeded fuse ceiling | Investigate, then `reset_fuse()` |
| `buffer_growth_warning` | Scheduler not consuming buffers fast enough | Check scheduler status and refresh errors |
| `reinitialize_needed` | Source table DDL changed | Verify schema compatibility; scheduler handles automatically |

## Further Reading

- [Prometheus & Grafana Integration](../integrations/prometheus.md)
- [SQL Reference — Monitoring Functions](../SQL_REFERENCE.md#status--monitoring)
- [Configuration Reference](../CONFIGURATION.md)
- [FAQ — Troubleshooting](../FAQ.md#troubleshooting)
