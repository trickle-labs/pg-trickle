# Troubleshooting & Failure Mode Runbook

This document covers common failure scenarios, their symptoms, diagnosis steps,
and resolution procedures. It is intended for operators and DBAs running
pg_trickle in production.

> **Quick start:** Run `SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK';`
> for a single-call triage of your installation.

> **See also:**
> - [Error Reference](ERRORS.md) — all `PgTrickleError` variants with causes and fixes
> - [FAQ — Troubleshooting section](FAQ.md#troubleshooting) — common user questions
> - [Pre-Deployment Checklist](PRE_DEPLOYMENT.md) — configuration verification
> - [Configuration](CONFIGURATION.md) — GUC reference

---

## Table of Contents

- [Diagnostic Toolkit](#diagnostic-toolkit)
- [Failure Scenarios](#failure-scenarios)
  - [1. Scheduler Not Running](#1-scheduler-not-running)
  - [2. Stream Table Stuck in SUSPENDED Status](#2-stream-table-stuck-in-suspended-status)
  - [3. CDC Triggers Missing or Disabled](#3-cdc-triggers-missing-or-disabled)
  - [4. WAL Replication Slot Lag or Missing](#4-wal-replication-slot-lag-or-missing)
  - [5. Stream Table Stuck in INITIALIZING](#5-stream-table-stuck-in-initializing)
  - [6. Change Buffers Growing Without Refresh](#6-change-buffers-growing-without-refresh)
  - [7. Lock Contention Blocking Refresh](#7-lock-contention-blocking-refresh)
  - [8. Out-of-Memory During Refresh](#8-out-of-memory-during-refresh)
  - [9. Disk Full / WAL Retention Exceeded](#9-disk-full--wal-retention-exceeded)
  - [10. Circular Pipeline Convergence Failure](#10-circular-pipeline-convergence-failure)
  - [11. Schema Change Broke Stream Table](#11-schema-change-broke-stream-table)
  - [12. Worker Pool Exhaustion](#12-worker-pool-exhaustion)
  - [13. Fuse Tripped (Circuit Breaker)](#13-fuse-tripped-circuit-breaker)
  - [14. Stream Table Appears Stuck Behind a Long Transaction](#14-stream-table-appears-stuck-behind-a-long-transaction)
  - [15. Stale Data After High-Concurrency Writes (Sequence Cache Inversion)](#15-stale-data-after-high-concurrency-writes-sequence-cache-inversion)

---

## Diagnostic Toolkit

These functions are your primary tools for diagnosing issues:

| Function | Purpose |
|----------|---------|
| `pgtrickle.health_check()` | Single-call overall health triage (OK/WARN/ERROR) |
| `pgtrickle.pgt_status()` | Status, staleness, error count for all stream tables |
| `pgtrickle.refresh_timeline(N)` | Last N refresh events across all stream tables |
| `pgtrickle.diagnose_errors('name')` | Last 5 failed events with classification and remediation |
| `pgtrickle.change_buffer_sizes()` | CDC pipeline: pending rows and buffer bytes per source |
| `pgtrickle.trigger_inventory()` | CDC trigger presence and enabled state |
| `pgtrickle.check_cdc_health()` | WAL replication slot health (WAL mode only) |
| `pgtrickle.dependency_tree()` | Dependency DAG visualization |
| `pgtrickle.worker_pool_status()` | Parallel refresh worker pool state |
| `pgtrickle.explain_st('name')` | DVM operator tree and generated delta SQL |

**Quick health check script:**

```sql
-- 1. Overall health
SELECT * FROM pgtrickle.health_check() WHERE severity != 'OK';

-- 2. Problem stream tables
SELECT name, status, refresh_mode, consecutive_errors, staleness
FROM pgtrickle.pgt_status()
WHERE status != 'ACTIVE' OR consecutive_errors > 0
ORDER BY consecutive_errors DESC;

-- 3. Recent failures
SELECT start_time, stream_table, action, status, duration_ms, error_message
FROM pgtrickle.refresh_timeline(20)
WHERE status = 'FAILED';
```

---

## Failure Scenarios

### 1. Scheduler Not Running

**Symptoms:**
- No stream tables are refreshing
- `health_check()` reports `scheduler_running = false`
- No `pg_trickle scheduler` process in `pg_stat_activity`

**Diagnosis:**

```sql
-- Check for the scheduler process
SELECT pid, datname, backend_type, state
FROM pg_stat_activity
WHERE backend_type = 'pg_trickle scheduler';

-- Check GUC
SHOW pg_trickle.enabled;

-- Check shared_preload_libraries
SHOW shared_preload_libraries;
```

**Resolution:**

| Cause | Fix |
|-------|-----|
| `pg_trickle.enabled = off` | `ALTER SYSTEM SET pg_trickle.enabled = on; SELECT pg_reload_conf();` |
| Not in `shared_preload_libraries` | Add `pg_trickle` to `shared_preload_libraries` in `postgresql.conf` and **restart** PostgreSQL |
| `max_worker_processes` exhausted | Increase `max_worker_processes` and restart. The launcher retries every 5 minutes — check PostgreSQL logs for `WARNING: pg_trickle launcher: could not spawn scheduler` |
| Scheduler crashed | Check PostgreSQL logs for crash details. The launcher will auto-restart it. If recurring, check for OOM or resource limits |

---

### 2. Stream Table Stuck in SUSPENDED Status

**Symptoms:**
- Stream table status shows `SUSPENDED`
- `consecutive_errors` is at or above `pg_trickle.max_consecutive_errors`
- No refreshes happening for this stream table

**Diagnosis:**

```sql
-- Check the stream table status
SELECT pgt_name, status, consecutive_errors, last_error_message
FROM pgtrickle.pg_stat_stream_tables
WHERE pgt_name = 'my_stream_table';

-- Get detailed error history
SELECT * FROM pgtrickle.diagnose_errors('my_stream_table');
```

**Resolution:**

1. Fix the underlying error (check `last_error_message` and `diagnose_errors`)
2. Resume the stream table:

```sql
SELECT pgtrickle.alter_stream_table('my_stream_table', enabled => true);
```

3. Trigger a manual refresh to verify:

```sql
SELECT pgtrickle.refresh_stream_table('my_stream_table');
```

**Prevention:** Increase `pg_trickle.max_consecutive_errors` (default 3) if
transient errors are common in your environment:

```sql
ALTER SYSTEM SET pg_trickle.max_consecutive_errors = 5;
SELECT pg_reload_conf();
```

---

### 3. CDC Triggers Missing or Disabled

**Symptoms:**
- Stream table refreshes succeed but shows no changes
- `change_buffer_sizes()` shows `pending_rows = 0` despite active DML
- Source tables have no pg_trickle triggers

**Diagnosis:**

```sql
-- Check trigger inventory
SELECT source_table, trigger_type, trigger_name, present, enabled
FROM pgtrickle.trigger_inventory()
WHERE NOT present OR NOT enabled;

-- Manual check on a specific source table
SELECT tgname, tgenabled
FROM pg_trigger
WHERE tgrelid = 'public.orders'::regclass
  AND tgname LIKE 'pgt_%';
```

**Resolution:**

| Cause | Fix |
|-------|-----|
| Triggers dropped by DDL (e.g., `pg_dump` + restore without triggers) | Drop and recreate the stream table, or reinitialize: `SELECT pgtrickle.refresh_stream_table('my_st');` |
| Triggers disabled (`ALTER TABLE ... DISABLE TRIGGER`) | `ALTER TABLE source_table ENABLE TRIGGER ALL;` |
| Source gating active | Check `SELECT * FROM pgtrickle.source_gates();` and ungate: `SELECT pgtrickle.ungate_source('source_table');` |
| WAL mode active but slot missing | See [WAL Replication Slot Lag or Missing](#4-wal-replication-slot-lag-or-missing) |

---

### 4. WAL Replication Slot Lag or Missing

**Symptoms:**
- `check_cdc_health()` shows `slot_lag_exceeds_threshold` or `replication_slot_missing`
- WAL disk usage growing unexpectedly
- Stream tables not receiving changes in WAL mode

**Diagnosis:**

```sql
-- Check CDC health
SELECT * FROM pgtrickle.check_cdc_health();

-- Check replication slots directly
SELECT slot_name, active, restart_lsn, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name LIKE 'pgt_%';
```

**Resolution:**

| Cause | Fix |
|-------|-----|
| Slot dropped externally | pg_trickle will auto-fallback to trigger-based CDC. To recreate: drop and recreate the stream table |
| Slot lagging (WAL accumulation) | Check for long-running transactions: `SELECT pid, age(backend_xmin) FROM pg_stat_replication;`. Kill idle-in-transaction sessions |
| `wal_level != logical` | WAL CDC requires `wal_level = logical`. Set it and restart PostgreSQL |
| `max_replication_slots` exhausted | Increase `max_replication_slots` and restart |

**Fallback:** Force trigger-based CDC mode if WAL mode is problematic:

```sql
ALTER SYSTEM SET pg_trickle.cdc_mode = 'trigger';
SELECT pg_reload_conf();
```

---

### 5. Stream Table Stuck in INITIALIZING

**Symptoms:**
- Stream table status is `INITIALIZING` for an extended period
- The initial full refresh may have failed or is still running

**Diagnosis:**

```sql
-- Check refresh history
SELECT * FROM pgtrickle.get_refresh_history('my_st', 5);

-- Check for active refresh
SELECT pid, state, query, now() - query_start AS running_for
FROM pg_stat_activity
WHERE query LIKE '%pgtrickle%' AND state = 'active';
```

**Resolution:**

| Cause | Fix |
|-------|-----|
| Initial refresh failed (check error in history) | Fix the error, then: `SELECT pgtrickle.refresh_stream_table('my_st');` |
| Defining query is very slow | Optimize the query, add indexes on source tables, or increase `work_mem` |
| Lock contention during initial refresh | See [Lock Contention](#7-lock-contention-blocking-refresh) |

---

### 6. Change Buffers Growing Without Refresh

**Symptoms:**
- `change_buffer_sizes()` shows large `pending_rows` and growing `buffer_bytes`
- Stream tables are stale
- Refreshes are not running or are failing

**Diagnosis:**

```sql
-- Check buffer sizes
SELECT stream_table, source_table, pending_rows, buffer_bytes
FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;

-- Check if refreshes are happening
SELECT * FROM pgtrickle.refresh_timeline(10);

-- Check for blocked refresh processes
SELECT pid, wait_event_type, wait_event, state, query
FROM pg_stat_activity
WHERE query LIKE '%pgtrickle%';
```

**Resolution:**

| Cause | Fix |
|-------|-----|
| Scheduler not running | See [Scheduler Not Running](#1-scheduler-not-running) |
| All refreshes failing | Check `diagnose_errors()` for each affected stream table |
| Lock contention | See [Lock Contention](#7-lock-contention-blocking-refresh) |
| Very large buffer causing slow MERGE | Consider lowering `pg_trickle.differential_change_ratio_threshold` to trigger FULL refresh for large batches |

**Emergency:** If buffers are dangerously large and you need immediate relief:

```sql
-- Force a full refresh (bypasses change buffers)
SELECT pgtrickle.refresh_stream_table('my_st', force_full => true);
```

---

### 7. Lock Contention Blocking Refresh

**Symptoms:**
- Refresh duration is much longer than usual
- `pg_stat_activity` shows refresh processes in `Lock` wait state
- Long-running transactions on source or stream tables

**Diagnosis:**

```sql
-- Find blocking locks
SELECT blocked.pid AS blocked_pid,
       blocked.query AS blocked_query,
       blocking.pid AS blocking_pid,
       blocking.query AS blocking_query
FROM pg_stat_activity blocked
JOIN pg_locks bl ON bl.pid = blocked.pid AND NOT bl.granted
JOIN pg_locks gl ON gl.locktype = bl.locktype
    AND gl.database IS NOT DISTINCT FROM bl.database
    AND gl.relation IS NOT DISTINCT FROM bl.relation
    AND gl.page IS NOT DISTINCT FROM bl.page
    AND gl.tuple IS NOT DISTINCT FROM bl.tuple
    AND gl.pid != bl.pid
    AND gl.granted
JOIN pg_stat_activity blocking ON blocking.pid = gl.pid
WHERE blocked.query LIKE '%pgtrickle%';
```

**Resolution:**

1. Identify and terminate the blocking session if appropriate:
   ```sql
   SELECT pg_terminate_backend(<blocking_pid>);
   ```
2. Investigate why the blocking transaction is long-running (idle in transaction, slow query, etc.)
3. Consider adding `statement_timeout` or `idle_in_transaction_session_timeout` to prevent future occurrences

---

### 8. Out-of-Memory During Refresh

**Symptoms:**
- Refresh processes killed by OS OOM killer
- PostgreSQL logs show `out of memory` errors
- Stream tables fail with system-category errors

**Diagnosis:**

```bash
# Check OS OOM killer logs
dmesg | grep -i "oom\|killed process" | tail -20

# Check PostgreSQL logs for memory errors
grep -i "out of memory\|oom" /var/log/postgresql/postgresql-*.log | tail -10
```

```sql
-- Check which stream tables have large source data
SELECT stream_table, source_table, pending_rows
FROM pgtrickle.change_buffer_sizes()
ORDER BY pending_rows DESC;
```

**Resolution:**

| Cause | Fix |
|-------|-----|
| Large FULL refresh on big table | Reduce `work_mem` or `maintenance_work_mem` to limit per-query memory |
| Large change buffer accumulation | Refresh more frequently (shorter schedule) to keep buffers small |
| Complex query with many joins | Simplify the defining query or break into cascading stream tables |
| Parallel refresh amplifies memory | Reduce `pg_trickle.max_concurrent_refreshes` |

**Tuning:**

```sql
-- Limit per-refresh memory
SET work_mem = '64MB';

-- Limit concurrent refreshes to reduce peak memory
ALTER SYSTEM SET pg_trickle.max_concurrent_refreshes = 2;
SELECT pg_reload_conf();
```

---

### 9. Disk Full / WAL Retention Exceeded

**Symptoms:**
- PostgreSQL logs `No space left on device` errors
- WAL directory consuming excessive disk
- Replication slots preventing WAL cleanup

**Diagnosis:**

```bash
# Check disk usage
df -h /var/lib/postgresql/data
du -sh /var/lib/postgresql/data/pg_wal/
```

```sql
-- Check replication slot WAL retention
SELECT slot_name, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots
ORDER BY pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) DESC;

-- Check change buffer table sizes
SELECT stream_table, source_table,
       pg_size_pretty(buffer_bytes::bigint) AS buffer_size
FROM pgtrickle.change_buffer_sizes()
ORDER BY buffer_bytes DESC;
```

**Resolution:**

| Cause | Fix |
|-------|-----|
| Inactive replication slot holding WAL | Drop the slot: `SELECT pg_drop_replication_slot('pgt_...');` |
| Change buffer tables too large | Force full refresh to clear buffers, or refresh more frequently |
| WAL accumulation from long transactions | Terminate idle-in-transaction sessions |
| `max_wal_size` too low | Increase `max_wal_size` in `postgresql.conf` |

**Emergency cleanup:**

```sql
-- Drop inactive pg_trickle replication slots
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name LIKE 'pgt_%' AND NOT active;
```

---

### 10. Circular Pipeline Convergence Failure

**Symptoms:**
- Stream tables in a circular dependency hit the maximum iteration limit
- Refresh history shows repeated cycles without convergence
- Error messages mention `fixed_point_max_iterations`

**Diagnosis:**

```sql
-- Check for circular dependencies
SELECT * FROM pgtrickle.dependency_tree();

-- Check refresh history for iteration patterns
SELECT start_time, stream_table, action, status, error_message
FROM pgtrickle.refresh_timeline(50)
WHERE stream_table IN ('st_a', 'st_b')  -- suspected cycle members
ORDER BY start_time DESC;
```

**Resolution:**

1. Verify the cycle is intentional (see [Circular Dependencies tutorial](tutorials/CIRCULAR_DEPENDENCIES.md))
2. Increase the iteration limit if convergence is slow:
   ```sql
   ALTER SYSTEM SET pg_trickle.fixed_point_max_iterations = 20;
   SELECT pg_reload_conf();
   ```
3. If the cycle never converges, the defining queries may not be monotone.
   Restructure to eliminate the cycle or ensure monotonicity

---

### 11. Schema Change Broke Stream Table

**Symptoms:**
- Stream table has `needs_reinit = true`
- Reinitialization keeps failing
- Error messages reference dropped or renamed columns

**Diagnosis:**

```sql
-- Check for pending reinit
SELECT pgt_name, needs_reinit, status, last_error_message
FROM pgtrickle.pg_stat_stream_tables
WHERE needs_reinit;

-- Get error details
SELECT * FROM pgtrickle.diagnose_errors('my_st');
```

**Resolution:**

If the defining query is still valid after the DDL change, force a reinit:

```sql
SELECT pgtrickle.refresh_stream_table('my_st');
```

If the defining query needs to be updated:

```sql
-- Option 1: Alter the defining query
SELECT pgtrickle.alter_stream_table('my_st',
    query => 'SELECT new_column, SUM(amount) FROM orders GROUP BY new_column'
);

-- Option 2: Drop and recreate
SELECT pgtrickle.drop_stream_table('my_st');
SELECT pgtrickle.create_stream_table(
    'my_st',
    'SELECT new_column, SUM(amount) FROM orders GROUP BY new_column',
    '1m'
);
```

---

### 12. Worker Pool Exhaustion

**Symptoms:**
- Refresh latency increases across the board
- Some stream tables refresh while others queue indefinitely
- `worker_pool_status()` shows all workers busy

**Diagnosis:**

```sql
-- Check worker pool
SELECT * FROM pgtrickle.worker_pool_status();

-- Check for long-running parallel jobs
SELECT job_id, unit_key, status, duration_ms
FROM pgtrickle.parallel_job_status(300)
WHERE status = 'RUNNING'
ORDER BY duration_ms DESC;
```

**Resolution:**

| Cause | Fix |
|-------|-----|
| Too few workers for workload | Increase `pg_trickle.max_concurrent_refreshes` and/or `max_worker_processes` |
| One stream table monopolizing workers | Check if a single slow refresh is blocking the pool. Consider splitting into smaller stream tables |
| Global worker cap reached | Increase `pg_trickle.max_dynamic_refresh_workers` |

---

### 13. Fuse Tripped (Circuit Breaker)

**Symptoms:**
- Stream table shows `fuse_state = 'BLOWN'` or refresh is paused
- `fuse_status()` reports a tripped fuse
- No refreshes happening despite active scheduler

**Diagnosis:**

```sql
-- Check fuse status
SELECT * FROM pgtrickle.fuse_status();
```

**Resolution:**

Reset the fuse after investigating the root cause:

```sql
SELECT pgtrickle.reset_fuse('my_stream_table');
```

See the [Fuse Circuit Breaker tutorial](tutorials/FUSE_CIRCUIT_BREAKER.md) for
details on fuse thresholds and configuration.

---

### 14. Stream Table Appears Stuck Behind a Long Transaction

**Symptoms:**
- A stream table's `data_timestamp` is not advancing even though the source
  table is receiving new inserts.
- The `pg_trickle_frontier_holdback_lsn_bytes` Prometheus gauge is non-zero.
- Server log contains: `pg_trickle: frontier holdback active — the oldest in-progress transaction is Ns old`.

**Cause:**
`frontier_holdback_mode = 'xmin'` (the default) prevents the scheduler from
advancing the frontier while any in-progress transaction exists that is older
than the previous tick's xmin baseline.  A long-running or forgotten session
holding an open transaction will pause frontier advancement for all stream
tables on that PostgreSQL server.

This is intentional: without the holdback, a transaction that inserts into a
tracked source table and commits *after* the scheduler ticks would have its
change permanently lost (see Issue #536 and `plans/safety/PLAN_FRONTIER_VISIBILITY_HOLDBACK.md`).

**Diagnosis:**

```sql
-- Find the oldest in-progress transaction
SELECT pid, usename, state, application_name,
       backend_xmin,
       EXTRACT(EPOCH FROM (now() - xact_start))::int AS xact_age_secs,
       query
FROM pg_stat_activity
WHERE backend_xmin IS NOT NULL
  AND state <> 'idle'
ORDER BY xact_start;

-- Check for prepared (2PC) transactions
SELECT gid, prepared,
       EXTRACT(EPOCH FROM (now() - prepared))::int AS age_secs,
       owner, database
FROM pg_prepared_xacts
ORDER BY prepared;
```

**Resolution:**

1. **Identify and terminate the blocking session:**

   ```sql
   SELECT pg_terminate_backend(pid)
   FROM pg_stat_activity
   WHERE state = 'idle in transaction'
     AND backend_xmin IS NOT NULL
   ORDER BY xact_start
   LIMIT 1;
   ```

2. **Rollback a forgotten 2PC transaction:**

   ```sql
   ROLLBACK PREPARED 'gid_from_pg_prepared_xacts';
   ```

3. **For benchmark or known-safe workloads only**, disable holdback to restore
   the pre-fix fast path (risks silent data loss):

   ```sql
   ALTER SYSTEM SET pg_trickle.frontier_holdback_mode = 'none';
   SELECT pg_reload_conf();
   ```

4. **Suppress the warning** (while keeping holdback active) by raising the
   threshold:

   ```sql
   ALTER SYSTEM SET pg_trickle.frontier_holdback_warn_seconds = 300;
   SELECT pg_reload_conf();
   ```

5. **On managed PostgreSQL (RDS, Cloud SQL, Aiven, etc.)** where
   `pg_stat_activity` is restricted to the current user's own sessions,
   the probe will silently see no other backends and never trigger a
   holdback. The server log will contain:
   `pg_trickle: frontier holdback probe cannot see other PostgreSQL backends`.

   Fix by granting the monitoring role to the pg_trickle service account:

   ```sql
   GRANT pg_monitor TO <pg_trickle_service_role>;
   ```

   Then restart the pg_trickle scheduler (or reload PostgreSQL) so the new
   privilege takes effect.

---

### 15. Stale Data After High-Concurrency Writes (Sequence Cache Inversion)

**Symptoms:**
- A stream table consistently shows an outdated value for a row that is
  being updated frequently by concurrent sessions.
- The issue is reproducible under high write concurrency but not with a
  single writer.
- `pgtrickle.check_cdc_health()` returns a `CRITICAL: change buffer
  sequence(s) have CACHE > 1` alert.

**Cause:**
Someone manually altered a change buffer sequence to use `CACHE > 1` (e.g.
to reduce sequence LWLock contention). The change buffer BIGSERIAL sequence
**must** use `CACHE 1` — this is a hard correctness invariant, not a
performance knob.

With `CACHE > 1`, PostgreSQL backends pre-allocate blocks of sequence
values. Two concurrent transactions modifying the same row can commit in an
order that inverts their pre-cached `change_id` values:

1. **Backend A** caches `[16, 31]` and starts updating row `id=1`.
2. **Backend B** caches `[33, 64]`, updates the same row with
   `change_id=33`, and commits first.
3. **Backend A** commits last (true final state), but its `change_id=16`.
4. The compaction/delta pipeline uses `ORDER BY change_id DESC` to find the
   final state → picks `change_id=33` (Backend B's **stale** data).

Silent data corruption. See [issue #536](https://github.com/trickle-labs/pg-trickle/issues/536)
for full analysis.

**Diagnosis:**

```sql
-- check_cdc_health() surfaces the problem:
SELECT source_table, cdc_mode, alert
FROM pgtrickle.check_cdc_health()
WHERE alert IS NOT NULL;

-- Directly inspect all change buffer sequences:
SELECT schemaname, sequencename, cache_size
FROM pg_sequences
WHERE schemaname = 'pgtrickle_changes'
  AND (sequencename LIKE 'changes_%_change_id_seq'
    OR sequencename LIKE 'changes_pgt_%_change_id_seq')
  AND cache_size > 1;
```

**Resolution:**

1. Reset every affected sequence back to `CACHE 1`:

   ```sql
   -- Replace <seq_name> with the sequencename from the query above.
   ALTER SEQUENCE pgtrickle_changes.<seq_name> CACHE 1;
   ```

2. Verify the alert is gone:

   ```sql
   SELECT alert FROM pgtrickle.check_cdc_health() WHERE alert IS NOT NULL;
   -- Should return zero rows.
   ```

3. **Do NOT increase CACHE to reduce LWLock contention.** The only
   structural solution for high-concurrency `change_id` contention is to
   switch to the WAL/logical-decoding CDC backend, which uses commit-LSN
   ordering and has no sequence at all:

   ```sql
   SELECT pgtrickle.alter_stream_table('my_st', cdc_mode => 'wal');
   ```

---

## General Diagnostic Workflow

When investigating any issue, follow this sequence:

```
1. health_check()          → identify which subsystem is unhealthy
2. pgt_status()            → find specific affected stream tables
3. diagnose_errors('name') → get root cause for failures
4. refresh_timeline(20)    → correlate with recent refresh events
5. change_buffer_sizes()   → check CDC pipeline health
6. trigger_inventory()     → verify change capture is working
7. dependency_tree()       → confirm DAG wiring
8. PostgreSQL logs         → low-level crash/resource details
```

## GUC Quick Reference for Troubleshooting

| GUC | Default | What to check |
|-----|---------|---------------|
| `pg_trickle.enabled` | `on` | Must be `on` for scheduler to run |
| `pg_trickle.max_consecutive_errors` | `3` | Stream tables suspend after this many failures |
| `pg_trickle.scheduler_interval_ms` | `100` | Very high values cause refresh lag |
| `pg_trickle.cdc_mode` | `auto` | `trigger` for reliable fallback |
| `pg_trickle.max_concurrent_refreshes` | `4` | Per-database parallel refresh cap |
| `pg_trickle.fixed_point_max_iterations` | `10` | Circular pipeline iteration limit |
| `pg_trickle.differential_change_ratio_threshold` | `0.5` | Falls back to FULL above this ratio |
| `pg_trickle.auto_backoff` | `on` | Stretches intervals up to 8x under load |
| `pg_trickle.frontier_holdback_mode` | `xmin` | `none` disables holdback (unsafe); `xmin` = safe default |
| `pg_trickle.frontier_holdback_warn_seconds` | `60` | Warn after holding back for this many seconds |

---

**See also:**
[Error Reference](ERRORS.md) ·
[Configuration](CONFIGURATION.md) ·
[FAQ](FAQ.md) ·
[Pre-Deployment Checklist](PRE_DEPLOYMENT.md) ·
[Capacity Planning](CAPACITY_PLANNING.md) ·
[CDC Modes](CDC_MODES.md)
