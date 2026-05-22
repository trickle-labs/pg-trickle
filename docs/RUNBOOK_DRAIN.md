# Drain-Mode Runbook — pg_trickle

> **Audience:** Database administrators and on-call engineers performing
> controlled shutdowns, maintenance windows, and rolling upgrades.

---

## What is Drain Mode?

Drain mode is a controlled shutdown mechanism for the pg_trickle scheduler.
When drain is requested:

1. The scheduler **stops dispatching new refresh cycles** immediately.
2. Any **in-flight refresh workers** are allowed to complete normally.
3. Once all workers have finished, the scheduler signals drain completion.
4. `pgtrickle.drain(timeout)` returns `true` when all in-flight work is done,
   or `false` if the timeout is exceeded.

Drain mode does **not** pause CDC capture — changes from source tables continue
to accumulate in the change buffer while the scheduler is drained. Those
changes will be processed on the next refresh cycle after resuming.

---

## API

```sql
-- Enter drain mode and wait up to <timeout> seconds for in-flight work to finish.
-- Returns true if all workers finished, false if timed out.
SELECT pgtrickle.drain(timeout => 60);

-- Check whether the scheduler is currently in a drained (idle) state.
SELECT pgtrickle.is_drained();
```

**GUC:** `pg_trickle.drain_timeout` — default timeout for `drain()` in seconds.
Set this in `postgresql.conf` or `ALTER SYSTEM` to match your maintenance window SLAs.

---

## Step-by-Step: Controlled Scheduler Drain

### Pre-drain checklist

- [ ] Confirm the maintenance window is open and stakeholders are notified.
- [ ] Record the current scheduler state:
  ```sql
  SELECT * FROM pgtrickle.worker_pool_status();
  SELECT name, last_refresh_at, status FROM pgtrickle.pgt_stream_tables ORDER BY name;
  ```
- [ ] Note which stream tables have pending changes:
  ```sql
  SELECT * FROM pgtrickle.change_buffer_sizes() ORDER BY pending_rows DESC LIMIT 10;
  ```

### Step 1: Request drain

```sql
-- Request drain and wait up to 120 seconds for in-flight work to finish.
SELECT pgtrickle.drain(timeout => 120);
```

Expected output: `true` (all workers finished) or `false` (timed out).

If `false`: some workers are still running. You can either:
- Wait longer: `SELECT pgtrickle.drain(timeout => 300);`
- Force immediate shutdown by restarting the scheduler:
  `SELECT pg_reload_conf();` (restarts the background worker on the next
  `pg_trickle.scheduler_interval_ms` tick)

### Step 2: Verify drain state

```sql
SELECT pgtrickle.is_drained();
-- Expected: true
```

Also confirm no active refresh workers:

```sql
SELECT count(*) FROM pg_stat_activity
WHERE application_name LIKE 'pg_trickle%refresh%';
-- Expected: 0
```

### Step 3: Perform maintenance

With the scheduler drained, you can safely:

- Upgrade the pg_trickle extension:
  `ALTER EXTENSION pg_trickle UPDATE TO '0.40.0';`
- Perform `VACUUM FULL` or `REINDEX` on stream tables.
- Alter source table schemas (if compatible with the defining query).
- Restart PostgreSQL for a configuration change.

### Step 4: Resume

Drain mode is automatically cleared when the scheduler restarts. After
PostgreSQL restart or extension reload:

```sql
-- Verify the scheduler is running again.
SELECT count(*) FROM pg_stat_activity
WHERE application_name LIKE 'pg_trickle_scheduler%';
-- Expected: 1 (one scheduler per database with pg_trickle enabled)
```

Stream tables will resume normal scheduling. Any changes that accumulated
during the drain will be processed in the next refresh cycle.

---

## Step-by-Step: Rolling Upgrade

For a rolling upgrade with minimal staleness:

1. Drain the scheduler:
   ```sql
   SELECT pgtrickle.drain(120);
   ```
2. Verify drain:
   ```sql
   SELECT pgtrickle.is_drained();
   ```
3. Upgrade the extension:
   ```sql
   ALTER EXTENSION pg_trickle UPDATE;
   ```
4. Verify the upgrade:
   ```sql
   SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle';
   ```
5. The scheduler resumes automatically. Monitor freshness:
   ```sql
   SELECT name, last_refresh_at, status FROM pgtrickle.pgt_stream_tables
   WHERE status != 'ok'
   ORDER BY last_refresh_at;
   ```

---

## Drain Behavior Under Load

When a drain is requested while a heavy refresh (e.g., TPC-H Q01 on a large
table) is in progress:

| State | Behavior |
|-------|----------|
| No in-flight workers | `drain()` returns `true` immediately |
| 1–N in-flight workers | `drain()` waits up to `timeout` seconds |
| Workers exceed timeout | `drain()` returns `false`; workers continue |
| CDC triggers | Continue writing to the change buffer (not affected by drain) |
| New refresh cycles | Not dispatched after drain is signalled |

The change buffer **continues to grow** during drain. Plan for a brief catch-up
burst when the scheduler resumes.

---

## Observability During Drain

Check drain state from Prometheus metrics (if `pg_trickle.metrics_port` is set):

```
pg_trickle_scheduler_drain_active 1  # 1 = drained, 0 = running
pg_trickle_scheduler_active_workers  # should approach 0 during drain
```

From Grafana: the "Scheduler State" panel on the pg_trickle overview dashboard
shows drain status and active worker count in real time.

---

## Troubleshooting

### `drain()` returns `false` (timeout)

1. Check which workers are still running:
   ```sql
   SELECT pid, application_name, query, state, now() - query_start AS duration
   FROM pg_stat_activity
   WHERE application_name LIKE 'pg_trickle%';
   ```
2. If a worker appears stuck (duration > 5× expected refresh time), check for
   lock contention:
   ```sql
   SELECT blocking_locks.pid AS blocking_pid, blocked_locks.pid AS blocked_pid,
          blocked_activity.query AS blocked_query
   FROM pg_catalog.pg_locks blocked_locks
   JOIN pg_catalog.pg_stat_activity blocked_activity
     ON blocked_activity.pid = blocked_locks.pid
   JOIN pg_catalog.pg_locks blocking_locks
     ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.relation = blocked_locks.relation
    AND blocking_locks.pid != blocked_locks.pid
   WHERE NOT blocked_locks.granted;
   ```
3. Cancel the blocking session if safe to do so:
   ```sql
   SELECT pg_cancel_backend(<pid>);
   ```

### Scheduler does not resume after restart

1. Verify `pg_trickle.enabled = on`:
   ```sql
   SHOW pg_trickle.enabled;
   ```
2. Check PostgreSQL logs for background worker registration errors.
3. Verify the extension is loaded:
   ```sql
   SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_trickle';
   ```

### Stream tables stale after resuming from drain

This is expected if the drain was long. The scheduler will process accumulated
changes in order. Monitor `pgtrickle.health_check()` until all tables return
`status = 'ok'`.

---

## Kubernetes Rolling Upgrade

When a CNPG-managed pod is terminated (rolling upgrade, scale-down, or eviction),
any in-flight refresh workers are killed mid-execution. Stream tables recover
safely on the next startup (they reinitialize), but the full-refresh cycle adds
latency proportional to table size.

### Configuring a preStop hook (OPS-10-01)

The production cluster manifest (`cnpg/cluster-production.yaml`) includes a
preStop lifecycle hook that drains pg_trickle before the pod receives SIGTERM:

```yaml
lifecycle:
  preStop:
    exec:
      command:
        - /bin/sh
        - -c
        - psql -U postgres -c "SELECT pgtrickle.drain(timeout_s => 120)" || true
```

The `|| true` ensures pod termination even if the database is unavailable
(e.g., primary already down during a failover). The timeout of 120 seconds
should comfortably cover a CNPG `terminationGracePeriodSeconds` of 60–120.

### Verifying drain behaviour after upgrade

After a rolling upgrade completes:
```sql
-- All tables should return to ACTIVE with low staleness
SELECT pgt_schema, pgt_name, status, staleness
FROM pgtrickle.pg_stat_stream_tables
WHERE status != 'ACTIVE' OR staleness > interval '5 minutes'
ORDER BY staleness DESC;
```

If tables remain stale, trigger a manual refresh:
```sql
SELECT pgtrickle.refresh('myschema', 'my_stream_table');
```

---

## Related Documentation

- [docs/SECURITY_MODEL.md](SECURITY_MODEL.md) — security model and `cdc_paused` semantics
- [docs/CONFIGURATION.md](CONFIGURATION.md) — `pg_trickle.drain_timeout` GUC
- [docs/SQL_REFERENCE.md](SQL_REFERENCE.md) — `pgtrickle.drain()` and `pgtrickle.is_drained()`
