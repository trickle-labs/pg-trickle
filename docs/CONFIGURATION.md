# Configuration

Complete reference for all pg_trickle GUC (Grand Unified Configuration) variables.

---

## Quick-tuning by goal

Not sure which GUC to change? Start here.

| Goal | GUCs to adjust |
|---|---|
| **Lower refresh latency** | `scheduler_interval_ms`, `min_schedule_seconds` |
| **Reduce write overhead on busy tables** | `compact_threshold`, `max_buffer_rows`, `cleanup_use_truncate`, `user_triggers` |
| **Handle larger DAGs without timeouts** | `max_workers`, `max_dynamic_refresh_workers`, `scheduler_interval_ms` |
| **Connection-pooler compatibility (PgBouncer)** | `pooler_compatibility_mode`, `use_prepared_statements` |
| **Lower memory usage during refresh** | `merge_work_mem_mb`, `max_delta_estimate_rows` |
| **Improve cost-model accuracy** | `cost_model_safety_margin`, `planner_aggressive`, `differential_max_change_ratio` |
| **Enable WAL-based CDC** | `cdc_mode`, `wal_transition_timeout`, `slot_lag_warning_threshold_mb` |
| **Prevent a runaway stream table** | `max_consecutive_errors`, `fuse_threshold`, `buffer_alert_threshold` |

See the full reference below for each variable's defaults, valid values, and
notes. Use `pgtrickle.recommend_refresh_mode()` for per-table advice.

---

## Table of Contents

- [Overview](#overview)
- [GUC Variables](#guc-variables)
  - [Essential](#essential)
    - [pg\_trickle.enabled](#pg_trickleenabled)
    - [pg\_trickle.cdc\_mode](#pg_tricklecdc_mode)
    - [pg\_trickle.scheduler\_interval\_ms](#pg_tricklescheduler_interval_ms)
    - [pg\_trickle.min\_schedule\_seconds](#pg_tricklemin_schedule_seconds)
    - [pg\_trickle.default\_schedule\_seconds](#pg_trickledefault_schedule_seconds)
    - [pg\_trickle.max\_consecutive\_errors](#pg_tricklemax_consecutive_errors)
  - [WAL CDC](#wal-cdc)
    - [pg\_trickle.wal\_transition\_timeout](#pg_tricklewal_transition_timeout)
    - [pg\_trickle.slot\_lag\_warning\_threshold\_mb](#pg_trickleslot_lag_warning_threshold_mb)
    - [pg\_trickle.slot\_lag\_critical\_threshold\_mb](#pg_trickleslot_lag_critical_threshold_mb)
  - [Refresh Performance](#refresh-performance)
    - [pg\_trickle.differential\_max\_change\_ratio](#pg_trickledifferential_max_change_ratio)
    - [pg\_trickle.refresh\_strategy](#pg_tricklerefresh_strategy)
    - [pg\_trickle.cost\_model\_safety\_margin](#pg_tricklecost_model_safety_margin)
    - [pg\_trickle.max\_delta\_estimate\_rows](#pg_tricklemax_delta_estimate_rows)
    - [pg\_trickle.planner\_aggressive](#pg_trickleplanner_aggressive)
    - [pg\_trickle.merge\_join\_strategy](#pg_tricklemerge_join_strategy)
    - [pg\_trickle.merge\_strategy](#pg_tricklemerge_strategy)
    - [pg\_trickle.merge\_strategy\_threshold](#pg_tricklemerge_strategy_threshold)
    - [pg\_trickle.merge\_planner\_hints](#pg_tricklemerge_planner_hints) *(deprecated)*
    - [pg\_trickle.merge\_work\_mem\_mb](#pg_tricklemerge_work_mem_mb)
    - [pg\_trickle.merge\_seqscan\_threshold](#pg_tricklemerge_seqscan_threshold)
    - [pg\_trickle.auto\_backoff](#pg_trickleauto_backoff)
    - [pg\_trickle.tiered\_scheduling](#pg_trickletiered_scheduling)
    - [pg\_trickle.cleanup\_use\_truncate](#pg_tricklecleanup_use_truncate)
    - [pg\_trickle.use\_prepared\_statements](#pg_trickleuse_prepared_statements)
    - [pg\_trickle.user\_triggers](#pg_trickleuser_triggers)
  - [Guardrails & Limits](#guardrails--limits)
    - [pg\_trickle.block\_source\_ddl](#pg_trickleblock_source_ddl)
    - [pg\_trickle.buffer\_alert\_threshold](#pg_tricklebuffer_alert_threshold)
    - [pg\_trickle.compact\_threshold](#pg_tricklecompact_threshold)
    - [pg\_trickle.max\_buffer\_rows](#pg_tricklemax_buffer_rows)
    - [pg\_trickle.auto\_index](#pg_trickleauto_index)
    - [pg\_trickle.aggregate\_fast\_path](#pg_trickleaggregate_fast_path)
    - [pg\_trickle.template\_cache](#pg_trickletemplate_cache)
    - [pg\_trickle.buffer\_partitioning](#pg_tricklebuffer_partitioning)
    - [pg\_trickle.max\_grouping\_set\_branches](#pg_tricklemax_grouping_set_branches)
    - [pg\_trickle.max\_parse\_depth](#pg_tricklemax_parse_depth)
    - [pg\_trickle.ivm\_topk\_max\_limit](#pg_trickleivm_topk_max_limit)
    - [pg\_trickle.ivm\_recursive\_max\_depth](#pg_trickleivm_recursive_max_depth)
  - [Parallel Refresh](#parallel-refresh)
    - [pg\_trickle.parallel\_refresh\_mode](#pg_trickleparallel_refresh_mode)
    - [pg\_trickle.max\_dynamic\_refresh\_workers](#pg_tricklemax_dynamic_refresh_workers)
    - [pg\_trickle.max\_concurrent\_refreshes](#pg_tricklemax_concurrent_refreshes)
    - [pg\_trickle.per\_database\_worker\_quota](#pg_trickleper_database_worker_quota)
  - [Advanced / Internal](#advanced--internal)
    - [pg\_trickle.change\_buffer\_schema](#pg_tricklechange_buffer_schema)
    - [pg\_trickle.foreign\_table\_polling](#pg_trickleforeign_table_polling)
    - [pg\_trickle.matview\_polling](#pg_tricklematview_polling)
    - [pg\_trickle.cdc\_trigger\_mode](#pg_tricklecdc_trigger_mode)
    - [pg\_trickle.tick\_watermark\_enabled](#pg_trickletick_watermark_enabled)
    - [pg\_trickle.watermark\_holdback\_timeout](#pg_tricklewatermark_holdback_timeout)
    - [pg\_trickle.spill\_threshold\_blocks](#pg_tricklespill_threshold_blocks)
    - [pg\_trickle.spill\_consecutive\_limit](#pg_tricklespill_consecutive_limit)
    - [pg\_trickle.log\_merge\_sql](#pg_tricklelog_merge_sql)
  - [Guardrails & Diagnostics](#guardrails--diagnostics)
    - [pg\_trickle.fuse\_default\_ceiling](#pg_tricklefuse_default_ceiling)
    - [pg\_trickle.delta\_amplification\_threshold](#pg_trickledelta_amplification_threshold)
    - [pg\_trickle.algebraic\_drift\_reset\_cycles](#pg_tricklealgebraic_drift_reset_cycles)
    - [pg\_trickle.agg\_diff\_cardinality\_threshold](#pg_trickleagg_diff_cardinality_threshold)
  - [Connection Pooler](#connection-pooler)
    - [pg\_trickle.connection\_pooler\_mode](#pg_trickleconnection_pooler_mode)
  - [History & Retention](#history--retention)
    - [pg\_trickle.history\_retention\_days](#pg_tricklehistory_retention_days)
  - [Circular Dependencies](#circular-dependencies)
    - [pg\_trickle.allow\_circular](#pg_trickleallow_circular)
    - [pg\_trickle.max\_fixpoint\_iterations](#pg_tricklemax_fixpoint_iterations)
- [Scheduler Scalability](#scheduler-scalability-v0250)
  - [pg\_trickle.worker\_pool\_size](#pg_trickleworker_pool_size)
  - [pg\_trickle.template\_cache\_max\_entries](#pg_trickletemplate_cache_max_entries)
- [Operability, Observability & DR](#operability-observability--dr-v0270)
  - [pg\_trickle.metrics\_port](#pg_tricklemetrics_port)
  - [pg\_trickle.metrics\_request\_timeout\_ms](#pg_tricklemetrics_request_timeout_ms)
  - [pg\_trickle.frontier\_holdback\_mode](#pg_tricklefrontier_holdback_mode)
  - [pg\_trickle.frontier\_holdback\_warn\_seconds](#pg_tricklefrontier_holdback_warn_seconds)
  - [pg\_trickle.publication\_lag\_warn\_bytes](#pg_tricklepublication_lag_warn_bytes)
  - [pg\_trickle.schedule\_recommendation\_min\_samples](#pg_trickleschedule_recommendation_min_samples)
  - [pg\_trickle.schedule\_alert\_cooldown\_seconds](#pg_trickleschedule_alert_cooldown_seconds)
- [Transactional Outbox](#transactional-outbox-v0280)
  - [pg\_trickle.outbox\_enabled](#pg_trickleoutbox_enabled)
  - [pg\_trickle.outbox\_retention\_hours](#pg_trickleoutbox_retention_hours)
  - [pg\_trickle.outbox\_drain\_batch\_size](#pg_trickleoutbox_drain_batch_size)
  - [pg\_trickle.outbox\_drain\_interval\_seconds](#pg_trickleoutbox_drain_interval_seconds)
  - [pg\_trickle.outbox\_inline\_threshold\_rows](#pg_trickleoutbox_inline_threshold_rows)
  - [pg\_trickle.outbox\_skip\_empty\_delta](#pg_trickleoutbox_skip_empty_delta)
  - [pg\_trickle.outbox\_storage\_critical\_mb](#pg_trickleoutbox_storage_critical_mb)
  - [pg\_trickle.outbox\_force\_retention](#pg_trickleoutbox_force_retention)
  - [pg\_trickle.consumer\_dead\_threshold\_hours](#pg_trickleconsumer_dead_threshold_hours)
  - [pg\_trickle.consumer\_stale\_offset\_threshold\_days](#pg_trickleconsumer_stale_offset_threshold_days)
  - [pg\_trickle.consumer\_cleanup\_enabled](#pg_trickleconsumer_cleanup_enabled)
- [Transactional Inbox](#transactional-inbox-v0280)
  - [pg\_trickle.inbox\_enabled](#pg_trickleinbox_enabled)
  - [pg\_trickle.inbox\_processed\_retention\_hours](#pg_trickleinbox_processed_retention_hours)
  - [pg\_trickle.inbox\_dlq\_retention\_hours](#pg_trickleinbox_dlq_retention_hours)
  - [pg\_trickle.inbox\_drain\_batch\_size](#pg_trickleinbox_drain_batch_size)
  - [pg\_trickle.inbox\_drain\_interval\_seconds](#pg_trickleinbox_drain_interval_seconds)
  - [pg\_trickle.inbox\_dlq\_alert\_max\_per\_refresh](#pg_trickleinbox_dlq_alert_max_per_refresh)
- [Pre-GA Correctness & Stability](#pre-ga-correctness--stability-v0300)
  - [pg\_trickle.use\_sqlstate\_classification](#pg_trickleuse_sqlstate_classification)
  - [pg\_trickle.template\_cache\_max\_age\_hours](#pg_trickletemplate_cache_max_age_hours)
  - [pg\_trickle.max\_parse\_nodes](#pg_tricklemax_parse_nodes)
- [Citus Distributed Tables](#citus-distributed-tables-v0320)
  - [pg\_trickle.citus\_st\_lock\_lease\_ms](#pg_tricklecitus_st_lock_lease_ms)
  - [pg\_trickle.citus\_worker\_retry\_ticks](#pg_tricklecitus_worker_retry_ticks)
- [GUC Interaction Matrix](#guc-interaction-matrix)
- [Tuning Profiles](#tuning-profiles)
  - [Low-Latency Profile](#low-latency-profile)
  - [High-Throughput Profile](#high-throughput-profile)
  - [Resource-Constrained Profile](#resource-constrained-profile)
- [Complete postgresql.conf Example](#complete-postgresqlconf-example)
- [Runtime Configuration](#runtime-configuration)
- [Further Reading](#further-reading)

---

## Overview

pg_trickle exposes over forty configuration variables in the `pg_trickle` namespace. All can be set in `postgresql.conf` or at runtime via `SET` / `ALTER SYSTEM`.

**Required `postgresql.conf` settings:**

```ini
shared_preload_libraries = 'pg_trickle'
```

The extension **must** be loaded via `shared_preload_libraries` because it registers GUC variables and a background worker at startup.

> **Note:** `wal_level = logical` and `max_replication_slots` are recommended but **not** required. The default CDC mode (`auto`) uses lightweight row-level triggers initially and transparently transitions to WAL-based capture if `wal_level = logical` is available. If `wal_level` is not `logical`, pg_trickle stays on triggers permanently — no degradation, no errors. Set `pg_trickle.cdc_mode = 'trigger'` to disable WAL transitions entirely (see [pg_trickle.cdc_mode](#pg_tricklecdc_mode)).

---

## GUC Variables

### Essential

The settings most users configure at install time.

---

### pg_trickle.enabled

Enable or disable the pg_trickle extension.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `true` |
| Context | `SUSET` (superuser) |
| Restart Required | No |

When set to `false`, the background scheduler stops processing refreshes. Existing stream tables remain in the catalog but are not refreshed. Manual `pgtrickle.refresh_stream_table()` calls still work.

> **Note on CDC triggers**: Setting `enabled = false` stops the scheduler from
> refreshing stream tables but does **not** disable CDC trigger execution.
> Change buffers continue to accumulate. This is intentional: when the extension
> is re-enabled, stream tables can refresh immediately from the buffered changes
> rather than performing a full table scan.
>
> To fully quiesce CDC overhead during extended maintenance, use
> `pgtrickle.drain()` before disabling, then `DROP TRIGGER` the CDC triggers
> manually and recreate them via `pgtrickle.repair_stream_table()` when
> re-enabling.

```sql
-- Disable automatic refreshes
SET pg_trickle.enabled = false;

-- Re-enable
SET pg_trickle.enabled = true;
```

---

### pg_trickle.cdc_mode

CDC (Change Data Capture) mechanism selection.

| Value | Description |
|-------|-------------|
| `'auto'` | **(default)** Use triggers for creation; transition to WAL-based CDC if `wal_level = logical`. Falls back to triggers automatically on error. |
| `'trigger'` | Always use row-level triggers for change capture |
| `'wal'` | Require WAL-based CDC (fails if `wal_level != logical`) |

**Default:** `'auto'`

`pg_trickle.cdc_mode` only affects deferred refresh modes (`'AUTO'`, `'FULL'`,
and `'DIFFERENTIAL'`). `refresh_mode = 'IMMEDIATE'` bypasses CDC entirely and
always uses statement-level IVM triggers. If the GUC is set to `'wal'` when a
stream table is created or altered to `IMMEDIATE`, pg_trickle logs an INFO and
continues with IVM triggers instead of creating CDC triggers or WAL slots.

Per-stream-table overrides take precedence over the GUC when you pass
`cdc_mode => 'auto' | 'trigger' | 'wal'` to
`pgtrickle.create_stream_table(...)` or `pgtrickle.alter_stream_table(...)`.
The override is stored in `pgtrickle.pgt_stream_tables.requested_cdc_mode`.
For shared source tables, pg_trickle resolves the effective source-level CDC
mechanism conservatively: any dependent stream table that requests `'trigger'`
keeps the source on trigger CDC; otherwise `'wal'` wins over `'auto'`.

```sql
-- Enable automatic trigger → WAL transition (default)
SET pg_trickle.cdc_mode = 'auto';

-- Force trigger-only CDC (disable WAL transitions)
SET pg_trickle.cdc_mode = 'trigger';

-- Require WAL-based CDC (error if wal_level != logical)
SET pg_trickle.cdc_mode = 'wal';
```

---

### pg_trickle.scheduler_interval_ms

How often the background scheduler checks for stream tables that need refreshing.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `1000` (1 second) |
| Range | `100` – `60000` (100ms to 60s) |
| Context | `SUSET` |
| Restart Required | No |

**Tuning Guidance:**
- **Low-latency workloads** (sub-second schedule): Set to `100`–`500`.
- **Standard workloads** (minutes of schedule): Default `1000` is appropriate.
- **Low-overhead workloads** (many STs with long schedules): Increase to `5000`–`10000` to reduce scheduler overhead.

The scheduler interval does **not** determine refresh frequency — it determines how often the scheduler *checks* whether any ST's staleness exceeds its schedule (or whether a cron expression has fired). The actual refresh frequency is governed by `schedule` (duration or cron) and canonical period alignment.

```sql
SET pg_trickle.scheduler_interval_ms = 500;
```

---

### pg_trickle.event_driven_wake

> ⚠️ **Removed** — This GUC has been removed. It had no effect because PostgreSQL's `LISTEN` command is not permitted inside
> background worker processes. The scheduler always uses efficient latch-based
> polling regardless of this setting.
>
> **Migration:** Remove `pg_trickle.event_driven_wake` from `postgresql.conf`
> and any `ALTER SYSTEM` settings. The scheduler behavior is unchanged — it
> wakes at `pg_trickle.scheduler_interval_ms` intervals. To reduce latency,
> lower `scheduler_interval_ms` instead (e.g. `200` ms for sub-200 ms refresh
> latency).

---

### pg_trickle.wake_debounce_ms

> ⚠️ **Removed** — This GUC has been removed together with
> `event_driven_wake`. It had no effect since `event_driven_wake` was always
> non-functional in background workers.
>
> **Migration:** Remove `pg_trickle.wake_debounce_ms` from `postgresql.conf`
> and any `ALTER SYSTEM` settings. No replacement is needed.

---

### pg_trickle.min_schedule_seconds

Minimum allowed `schedule` value (in seconds) when creating or altering a stream table with a duration-based schedule. This limit does **not** apply to cron expressions.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `1` (1 second) |
| Range | `1` – `86400` (1 second to 24 hours) |
| Context | `SUSET` |
| Restart Required | No |

This acts as a safety guardrail to prevent users from setting impractically small schedules that would cause excessive refresh overhead.

**Tuning Guidance:**
- **Development/testing**: Default `1` allows sub-second testing.
- **Production**: Raise to `60` or higher to prevent excessive WAL consumption and CPU usage.

```sql
-- Restrict to 10-second minimum schedules
SET pg_trickle.min_schedule_seconds = 10;
```

---

### pg_trickle.default_schedule_seconds

Default effective schedule (in seconds) for isolated CALCULATED stream tables that have no downstream dependents.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `1` (1 second) |
| Range | `1` – `86400` (1 second to 24 hours) |
| Context | `SUSET` |
| Restart Required | No |

When a CALCULATED stream table (scheduled with `'calculated'`) has no downstream dependents to derive a schedule from, this value is used as its effective refresh interval. This is distinct from `min_schedule_seconds`, which is the validation **floor** for duration-based schedules.

**Tuning Guidance:**
- **Development/testing**: Default `1` allows rapid iteration.
- **Production standalone CALCULATED tables**: Raise to match your desired update cadence (e.g., `60` for once-per-minute).

```sql
-- Set default for isolated CALCULATED tables to 30 seconds
SET pg_trickle.default_schedule_seconds = 30;
```

---

### pg_trickle.max_consecutive_errors

Maximum consecutive refresh failures before a stream table is moved to `ERROR` status.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `3` |
| Range | `1` – `100` |
| Context | `SUSET` |
| Restart Required | No |

When a ST's `consecutive_errors` reaches this threshold:
1. The ST status changes to `ERROR`.
2. Automatic refreshes stop for this ST.
3. Manual intervention is required: `SELECT pgtrickle.alter_stream_table('...', status => 'ACTIVE')`.

**Tuning Guidance:**
- **Strict** (production): `3` — fail fast to surface issues.
- **Lenient** (development): `10`–`20` — tolerate transient errors.

```sql
SET pg_trickle.max_consecutive_errors = 5;
```

---

### WAL CDC

Settings specific to WAL-based CDC. Only relevant when `pg_trickle.cdc_mode = 'auto'` or `'wal'`.

---

### pg_trickle.wal_transition_timeout

> **Note:** This setting is only relevant when `pg_trickle.cdc_mode = 'auto'` or `'wal'`. See
> [ARCHITECTURE.md](ARCHITECTURE.md) for the full CDC transition lifecycle.

Maximum time (seconds) to wait for the WAL decoder to catch up during
the transition from trigger-based to WAL-based CDC. If the decoder has
not caught up within this timeout, the system falls back to triggers.

**Default:** `300` (5 minutes)  
**Range:** `10` – `3600`

```sql
SET pg_trickle.wal_transition_timeout = 300;
```

---

### pg_trickle.slot_lag_warning_threshold_mb

Warning threshold for retained WAL on pg_trickle replication slots.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `100` (MB) |
| Range | `1` – `1048576` |
| Context | `SUSET` |
| Restart Required | No |

When retained WAL for a pg_trickle replication slot exceeds this threshold:
- The scheduler emits a `slot_lag_warning` event on `LISTEN pg_trickle_alert`
- `pgtrickle.health_check()` reports `WARN` for the `slot_lag` check

Raise this on high-throughput systems that intentionally tolerate larger WAL retention. Lower it if you want earlier warning before slots risk invalidation.

```sql
SET pg_trickle.slot_lag_warning_threshold_mb = 256;
```

---

### pg_trickle.slot_lag_critical_threshold_mb

Critical threshold for retained WAL on pg_trickle replication slots.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `1024` (MB) |
| Range | `1` – `1048576` |
| Context | `SUSET` |
| Restart Required | No |

When retained WAL for a pg_trickle replication slot exceeds this threshold,
`pgtrickle.check_cdc_health()` returns a per-source
`slot_lag_exceeds_threshold` alert.

This threshold is intentionally higher than the warning threshold so operators can separate early warning from source-level unhealthy state.

```sql
SET pg_trickle.slot_lag_critical_threshold_mb = 2048;
```

---

### Refresh Performance

Fine-grained tuning for the differential refresh engine.

---

### pg_trickle.differential_max_change_ratio

Maximum change-to-table ratio before DIFFERENTIAL refresh falls back to FULL refresh.

| Property | Value |
|---|---|
| Type | `float` |
| Default | `0.15` (15%) |
| Range | `0.0` – `1.0` |
| Context | `SUSET` |
| Restart Required | No |

When the number of pending change buffer rows exceeds this fraction of the source table's estimated row count, the refresh engine switches from DIFFERENTIAL (which uses JSONB parsing and window functions) to FULL refresh. At high change rates FULL refresh is cheaper because it avoids the per-row JSONB overhead.

**Special Values:**
- **`0.0`**: Disable adaptive fallback — always use DIFFERENTIAL.
- **`1.0`**: Always fall back to FULL (effectively forces FULL mode).

**Tuning Guidance:**
- **OLTP with low change rates** (< 5%): Default `0.15` is appropriate.
- **Batch-load workloads** (bulk inserts): Lower to `0.05`–`0.10` so large batches trigger FULL refresh sooner.
- **Latency-sensitive** (want deterministic refresh time): Set to `0.0` to always use DIFFERENTIAL.

```sql
-- Lower threshold for batch-heavy workloads
SET pg_trickle.differential_max_change_ratio = 0.10;

-- Disable adaptive fallback
SET pg_trickle.differential_max_change_ratio = 0.0;
```

---

### pg_trickle.refresh_strategy

Cluster-wide refresh strategy override.

| Property | Value |
|---|---|
| Type | `string` |
| Default | `'auto'` |
| Values | `'auto'`, `'differential'`, `'full'` |
| Context | `SUSET` |
| Restart Required | No |

Controls the FULL vs. DIFFERENTIAL decision for all stream tables whose `refresh_mode` is `DIFFERENTIAL`:

- **`'auto'`** (default): Use the adaptive cost-based heuristic that considers `differential_max_change_ratio`, per-ST `auto_threshold`, refresh history, and spill detection to pick the optimal strategy per refresh cycle.
- **`'differential'`**: Always use DIFFERENTIAL refresh — skip the adaptive ratio check entirely. The BUF-LIMIT safety check (`max_buffer_rows`) still applies.
- **`'full'`**: Always use FULL refresh regardless of change volume. Useful for debugging or when you know DIFFERENTIAL is consistently slower for your workload.

**Important:** Per-ST `refresh_mode` in the catalog takes precedence. Stream tables explicitly configured as `refresh_mode = 'FULL'` always use FULL regardless of this GUC.

**Tuning Guidance:**
- **Most workloads**: Leave at `'auto'` — the adaptive heuristic learns from refresh history.
- **Known-low-churn workloads**: Set to `'differential'` to eliminate the per-source capped-count query overhead.
- **Debugging delta issues**: Temporarily set to `'full'` to compare behavior.

```sql
-- Force DIFFERENTIAL for all stream tables (skip ratio check)
SET pg_trickle.refresh_strategy = 'differential';

-- Force FULL for all stream tables (debugging)
SET pg_trickle.refresh_strategy = 'full';

-- Reset to adaptive heuristic
SET pg_trickle.refresh_strategy = 'auto';
```

---

### pg_trickle.cost_model_safety_margin

Safety margin for the predictive cost model that decides FULL vs. DIFFERENTIAL.

| Property | Value |
|---|---|
| Type | `float` |
| Default | `0.8` |
| Range | `0.1` – `2.0` |
| Context | `SUSET` |
| Restart Required | No |

When `refresh_strategy = 'auto'`, the cost model estimates DIFFERENTIAL and FULL costs from recent refresh history. DIFFERENTIAL is chosen when:

```
estimated_diff_cost < estimated_full_cost × safety_margin
```

A value below 1.0 biases toward DIFFERENTIAL (which has lower lock contention and is generally preferred). A value above 1.0 biases toward FULL.

The cost model also classifies each stream table's query complexity (scan, filter, aggregate, join, or join+aggregate) and uses per-class coefficients learned from historical data.

**Tuning Guidance:**
- **`0.8`** (default): Prefer DIFFERENTIAL unless it's nearly as expensive as FULL.
- **`0.5`**: Strongly prefer DIFFERENTIAL — only fall back when it's clearly more expensive.
- **`1.0`**: Neutral — pick whichever is estimated to be cheaper.
- **`1.2`**: Slightly prefer FULL — useful when FULL is very fast and DIFFERENTIAL lock contention is a concern.

```sql
-- Strongly prefer DIFFERENTIAL
SET pg_trickle.cost_model_safety_margin = 0.5;

-- Neutral (pick the estimated cheapest)
SET pg_trickle.cost_model_safety_margin = 1.0;
```

---

### pg_trickle.max_delta_estimate_rows

Maximum estimated delta output cardinality before falling back to FULL refresh.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `0` (disabled) |
| Range | `0` – `10,000,000` |
| Context | `SUSET` |
| Restart Required | No |

Before executing the MERGE, the refresh executor extracts the delta subquery and runs a capped `SELECT count(*) FROM (delta LIMIT N+1)`. If the count reaches the configured limit, the refresh emits a `NOTICE` and falls back to FULL refresh to prevent OOM or excessive temp-file spills from unexpectedly large delta output.

This is complementary to [`differential_max_change_ratio`](#pg_trickledifferential_max_change_ratio) which checks *input* change buffer size as a ratio of source table size. `max_delta_estimate_rows` checks *output* cardinality — catching cases where a small number of input changes produce a large delta output after JOINs.

**Special Values:**
- **`0`** (default): Disable the estimation check entirely.

**Tuning Guidance:**
- **Servers with 8–16 GB RAM**: Start with `100000` and adjust based on observed refresh behavior.
- **Large-memory servers** (32+ GB): `500000` or higher.
- **Complex multi-join queries**: Lower to `50000` since join fan-out can amplify small changes.

```sql
-- Enable delta output estimation with 100K row limit
SET pg_trickle.max_delta_estimate_rows = 100000;

-- Disable estimation (default)
SET pg_trickle.max_delta_estimate_rows = 0;
```

---

### pg_trickle.cleanup_use_truncate

Use `TRUNCATE` instead of per-row `DELETE` for change buffer cleanup when the entire buffer is consumed by a refresh.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `true` |
| Context | `SUSET` |
| Restart Required | No |

After a differential refresh consumes all rows from the change buffer, the engine must clean up the buffer table. `TRUNCATE` is O(1) regardless of row count, versus `DELETE` which must update indexes row-by-row. This saves 3–5 ms per refresh at 10%+ change rates.

**Trade-off:** `TRUNCATE` acquires an `AccessExclusiveLock` on the change buffer table. If concurrent DML on the source table is actively inserting into the same change buffer via triggers, this lock can cause brief contention.

**Tuning Guidance:**
- **Most workloads**: Leave at `true` — the performance benefit outweighs the brief lock.
- **High-concurrency OLTP** with continuous writes during refresh: Set to `false` if you observe lock-wait timeouts on the change buffer.
- **PgBouncer / connection poolers**: The `AccessExclusiveLock` acquired by `TRUNCATE` is held only on the change buffer table (not the source table), but in transaction-pooling mode with frequent refreshes, even brief exclusive locks can cause connection queuing. If you observe elevated `pg_stat_activity` wait events on change buffer tables, switch to `false`.

```sql
-- Use per-row DELETE for change buffer cleanup
SET pg_trickle.cleanup_use_truncate = false;
```

---

### pg_trickle.planner_aggressive

Consolidated switch for all MERGE planner hints. Replaces the deprecated `merge_planner_hints` and `merge_work_mem_mb` GUCs.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `true` |
| Context | `SUSET` |
| Restart Required | No |

When enabled, the refresh executor estimates the delta size and applies optimizer hints within the transaction:
- **Delta ≥ 100 rows**: `SET LOCAL enable_nestloop = off` — forces hash joins instead of nested-loop joins.
- **Delta ≥ 10,000 rows**: additionally `SET LOCAL work_mem = '<N>MB'` (see [pg_trickle.merge_work_mem_mb](#pg_tricklemerge_work_mem_mb)).

**Tuning Guidance:**
- **Most workloads**: Leave at `true` — the hints improve tail latency without affecting small deltas.
- **Custom plan overrides**: Set to `false` if you manage planner settings yourself or if the hints conflict with your `pg_hint_plan` configuration.
- **Memory-constrained environments**: When enabled, large deltas (≥ 10,000 rows) raise `work_mem` to 64 MB (configurable via [`merge_work_mem_mb`](#pg_tricklemerge_work_mem_mb)). If your server has limited RAM and runs many concurrent refreshes, this can cause unexpected memory pressure or temp-file spills. Monitor `temp_blks_written` in `pg_stat_statements` and consider lowering `merge_work_mem_mb` or disabling this GUC if spills are frequent.

```sql
-- Disable all planner hints
SET pg_trickle.planner_aggressive = false;
```

---

### pg_trickle.merge_join_strategy

Manual override for the join strategy used during MERGE execution.

| Property | Value |
|---|---|
| Type | `text` |
| Default | `'auto'` |
| Values | `auto`, `hash_join`, `nested_loop`, `merge_join` |
| Context | `SUSET` |
| Restart Required | No |

Controls which join strategy the refresh executor hints to PostgreSQL via `SET LOCAL` during differential refresh. Requires [`planner_aggressive`](#pg_trickleplanner_aggressive) to be enabled.

| Value | Behaviour |
|---|---|
| `auto` (default) | Delta-size heuristics choose: nested-loop for tiny deltas, hash-join for larger ones |
| `hash_join` | Always disable nested-loop joins and raise `work_mem` — best for medium-to-large deltas |
| `nested_loop` | Always disable hash-join and merge-join — best for very small deltas against indexed tables |
| `merge_join` | Always disable hash-join and nested-loop — useful if data is pre-sorted |

**Tuning Guidance:**
- **Most workloads**: Leave at `auto` — the built-in heuristic performs well.
- **Consistently large deltas** (1K+ rows): Setting to `hash_join` avoids heuristic overhead.
- **Troubleshooting**: If refresh is slow, try different strategies and compare with [`explain_st()`](SQL_REFERENCE.md#pgtrickleexplain_st).

```sql
-- Force hash joins for all MERGE operations
SET pg_trickle.merge_join_strategy = 'hash_join';

-- Revert to automatic heuristics
SET pg_trickle.merge_join_strategy = 'auto';
```

---

### pg_trickle.merge_strategy

Controls how differential refresh applies deltas to stream tables.

| Property | Value |
|---|---|
| Type | `text` |
| Default | `'auto'` |
| Values | `auto`, `merge` |
| Context | `SUSET` |
| Restart Required | No |

| Value | Behaviour |
|---|---|
| `auto` (default) | Use DELETE+INSERT when `delta_rows / target_rows` is below [`merge_strategy_threshold`](#pg_tricklemerge_strategy_threshold); MERGE otherwise |
| `merge` | Always use the PostgreSQL MERGE statement |

> **Breaking change:** The `delete_insert` value was removed in
> (CORR-1) because it was semantically unsafe for aggregate and
> DISTINCT queries. Setting it now logs a WARNING and falls back to `auto`.

The DELETE+INSERT strategy avoids the MERGE join cost by executing two targeted statements:
a DELETE for removed rows (matched by `__pgt_row_id`), then an INSERT for new rows.
This is significantly cheaper for sub-1% deltas against large tables because it avoids
scanning the entire target for the MERGE join.

**Tuning Guidance:**
- **Most workloads**: Leave at `auto` — the heuristic picks DELETE+INSERT for small deltas automatically.
- **Correctness concerns**: The `merge` setting preserves the the original behaviour.

```sql
-- Force MERGE for all differential refreshes
SET pg_trickle.merge_strategy = 'merge';

-- Revert to automatic heuristics
SET pg_trickle.merge_strategy = 'auto';
```

---

### pg_trickle.merge_strategy_threshold

Delta ratio threshold for the `auto` merge strategy.

| Property | Value |
|---|---|
| Type | `float` |
| Default | `0.01` (1%) |
| Range | `0.001` – `1.0` |
| Context | `SUSET` |
| Restart Required | No |

When [`merge_strategy`](#pg_tricklemerge_strategy) is `auto`, DELETE+INSERT is used instead of
MERGE when `delta_rows / target_rows` is below this threshold. The target row count is estimated
from `pg_class.reltuples`.

**Tuning Guidance:**
- **Default (0.01)**: DELETE+INSERT for deltas under 1% of the target table size.
- **Higher values (0.05–0.10)**: More aggressive use of DELETE+INSERT; useful for wide tables where MERGE join overhead is high.
- **Lower values (0.001)**: Only use DELETE+INSERT for very tiny deltas.

```sql
-- Use DELETE+INSERT for deltas under 5% of target size
SET pg_trickle.merge_strategy_threshold = 0.05;
```

---

### pg_trickle.merge_planner_hints

> **Deprecated.** Use [`pg_trickle.planner_aggressive`](#pg_trickleplanner_aggressive) instead. This GUC is still accepted for backward compatibility but is ignored at runtime.

Inject `SET LOCAL` planner hints before MERGE execution during differential refresh.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `true` |
| Context | `SUSET` |
| Restart Required | No |

When enabled, the refresh executor estimates the delta size and applies optimizer hints within the transaction:
- **Delta ≥ 100 rows**: `SET LOCAL enable_nestloop = off` — forces hash joins instead of nested-loop joins.
- **Delta ≥ 10,000 rows**: additionally `SET LOCAL work_mem = '<N>MB'` (see [pg_trickle.merge_work_mem_mb](#pg_tricklemerge_work_mem_mb)).

This reduces P95 latency spikes caused by PostgreSQL choosing nested-loop plans for medium/large delta sizes.

**Tuning Guidance:**
- **Most workloads**: Leave at `true` — the hints improve tail latency without affecting small deltas.
- **Custom plan overrides**: Set to `false` if you manage planner settings yourself or if the hints conflict with your `pg_hint_plan` configuration.

```sql
-- Disable planner hints
SET pg_trickle.merge_planner_hints = false;
```

---

### pg_trickle.merge_work_mem_mb

`work_mem` value (in MB) applied via `SET LOCAL` when the delta exceeds 10,000 rows and [planner hints](#pg_tricklemerge_planner_hints) are enabled.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `64` (64 MB) |
| Range | `8` – `4096` (8 MB to 4 GB) |
| Context | `SUSET` |
| Restart Required | No |

A higher value lets PostgreSQL use larger in-memory hash tables for the MERGE join, avoiding disk-spilling sort/merge strategies on large deltas. This setting is only applied when both `merge_planner_hints = true` and the delta exceeds 10,000 rows.

**Tuning Guidance:**
- **Servers with ample RAM** (32+ GB): Increase to `128`–`256` for faster large-delta refreshes.
- **Memory-constrained**: Lower to `16`–`32` or disable planner hints entirely.
- **Very large deltas** (100K+ rows): Consider `256`–`512` if refresh latency matters.

```sql
SET pg_trickle.merge_work_mem_mb = 128;
```

---

### pg_trickle.delta_work_mem_cap_mb

Maximum `work_mem` (in MB) that planner hints are allowed to set during delta MERGE execution. When the deep-join or large-delta path would set `work_mem` above this cap, the refresh falls back to FULL instead of risking OOM.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `0` (disabled — no cap) |
| Range | `0` – `8192` (0 to 8 GB) |
| Context | `SUSET` |
| Restart Required | No |

Set to `0` to disable the cap entirely (default). When enabled, the cap is checked before any `SET LOCAL work_mem` in `apply_planner_hints()`. If the configured or computed `work_mem` exceeds the cap, the refresh emits a `NOTICE` and falls back to FULL refresh.

**Tuning Guidance:**
- **Production servers with tight memory budgets**: Set to `256`–`512` to prevent runaway hash joins.
- **Servers with ample RAM** (64+ GB): Leave at `0` (disabled) or set high (`2048`+).
- **If you see SCAL-3 fallback notices**: Either raise the cap or investigate why delta sizes are unexpectedly large.

```sql
SET pg_trickle.delta_work_mem_cap_mb = 512;
```

---

### pg_trickle.merge_seqscan_threshold

Delta-to-ST row ratio below which sequential scans are disabled for the MERGE transaction. Requires [planner hints](#pg_tricklemerge_planner_hints) to be enabled.

| Property | Value |
|---|---|
| Type | `real` |
| Default | `0.001` |
| Range | `0.0` – `1.0` |
| Context | `SUSET` |
| Restart Required | No |

When the estimated delta row count divided by the stream table's `reltuples` falls below this threshold, the refresh executor issues `SET LOCAL enable_seqscan = off`, coercing PostgreSQL into using the `__pgt_row_id` B-tree index instead of a full sequential scan.

Set to `0.0` to disable the feature entirely.

**Tuning Guidance:**
- **Default (`0.001`)**: Suitable for most workloads. A 10M-row ST with fewer than 10K delta rows triggers the hint.
- **High-throughput / small STs**: Increase to `0.01` if your STs are small and you want more aggressive index usage.
- **Disable**: Set to `0.0` if index-only scans are not beneficial for your access pattern.

```sql
SET pg_trickle.merge_seqscan_threshold = 0.01;
```

---

### pg_trickle.auto_backoff

Automatically back off the refresh schedule when a stream table is consistently falling behind.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `on` |
| Context | `SUSET` |
| Restart Required | No |

When enabled (the default), the scheduler tracks a per-stream-table backoff factor. If a
refresh cycle takes more than **95%** of the scheduled interval, the backoff factor doubles
(capped at **8×**), effectively stretching the schedule to avoid runaway refresh storms.
The factor resets to 1× on the first on-time completion, and a `WARNING` is emitted whenever
the factor changes so you always know why a stream table is refreshing more slowly than expected.

The 95% trigger threshold means that brief jitter on developer machines (e.g. a 950 ms refresh
on a 1-second schedule) will correctly engage backoff, while a 900 ms refresh on the same
schedule will not. The EC-11 operator alert (`scheduler_falling_behind` NOTIFY) continues to
fire at the lower 80% threshold, giving you advance warning before the scheduler is actually stuck.

This is a safety net for overloaded systems — it prevents a single slow stream table from
monopolizing the background worker when operators are not available to intervene.

**Tuning Guidance:**
- **Leave on** (the default) for both production and development environments.
- **Disable** only if you are deliberately running stream tables at the limit of their schedule
  budget and want the scheduler to keep trying at full speed regardless.

```sql
-- Disable if you want no backoff (not recommended for production)
SET pg_trickle.auto_backoff = off;
```

---

### pg_trickle.tiered_scheduling

Enable tiered refresh scheduling (Hot/Warm/Cold/Frozen) for stream tables.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `on` |
| Context | `SUSET` |
| Restart Required | No |

When enabled, the scheduler applies a per-stream-table refresh tier multiplier
to duration-based schedules. Each stream table has a `refresh_tier` column
(default `'hot'`) that controls how often it is refreshed relative to its
configured schedule:

| Tier | Multiplier | Effect |
|------|-----------|--------|
| `hot` | 1× | Refresh at configured schedule (default) |
| `warm` | 2× | Refresh at 2× the configured interval |
| `cold` | 10× | Refresh at 10× the configured interval |
| `frozen` | skip | Never refreshed until manually promoted |

Cron-based schedules are not affected by the tier multiplier.

Set the tier via:
```sql
SELECT pgtrickle.alter_stream_table('my_table', tier => 'warm');
SELECT pgtrickle.alter_stream_table('my_table', tier => 'frozen');
```

**Design note:** Tiers are user-assigned only. Automatic classification from
`pg_stat_user_tables` was rejected because pg_trickle's own MERGE scans
pollute the read counters, making auto-classification unreliable.

#### Tier Thresholds Reference

The following table summarizes the effective refresh behavior for each tier.
All multipliers apply to **duration-based schedules** only — cron-based
schedules are always honored as-is. New stream tables default to `hot`.

| Tier | Multiplier | Effective Schedule (1 s base) | Use Case |
|------|-----------|-------------------------------|----------|
| `hot` | 1× | 1 s | Real-time dashboards, alerting tables, SLA-bound queries |
| `warm` | 2× | 2 s | Important but not latency-critical tables; reduces CPU by 50% |
| `cold` | 10× | 10 s | Reporting tables queried infrequently; saves significant CPU |
| `frozen` | skip | never (until promoted) | Archival tables, tables under maintenance, or seasonal reports |

**When to use each tier:**

- **Hot** — default for all new stream tables. Appropriate when downstream
  consumers expect near-real-time freshness.
- **Warm** — set for tables where a few seconds of staleness is acceptable.
  Halves the refresh CPU cost compared to Hot.
- **Cold** — set for tables queried only by batch jobs or low-frequency
  dashboards. 10× reduction in refresh overhead.
- **Frozen** — set when a table should not be refreshed at all (e.g., during
  a maintenance window or when the upstream source is being migrated).
  Promote back to Hot/Warm/Cold when ready.

```sql
-- Promote a frozen table back to warm
SELECT pgtrickle.alter_stream_table('seasonal_report', tier => 'warm');

-- Freeze a table during maintenance
SELECT pgtrickle.alter_stream_table('my_table', tier => 'frozen');
```

> **** The default for `pg_trickle.tiered_scheduling`
> changed from `off` to `on`. Set `pg_trickle.tiered_scheduling = off` in
> `postgresql.conf` to restore the previous behavior (all STs refresh at
> full speed regardless of tier assignment).

---

### Diamond Schedule Policy (per-stream-table)

Controls how the scheduler fires diamond consistency groups — sets of stream
tables that share upstream sources through a diamond-shaped DAG topology.

| Property | Value |
|---|---|
| Column | `diamond_schedule_policy` in `pgt_stream_tables` |
| Values | `'fastest'` (default), `'slowest'` |
| Set via | `create_stream_table(..., diamond_schedule_policy => 'slowest')` |
| Alter via | `alter_stream_table('name', diamond_schedule_policy => 'slowest')` |

Only meaningful when `diamond_consistency = 'atomic'` is also set.

**`fastest` (default):** The atomic group fires when **any** member is due.
This maximizes freshness but can cause CPU multiplication. In an asymmetric
diamond where stream table B refreshes every 1 s and stream table C every 5 s,
both feeding D with `diamond_consistency = 'atomic'`: C refreshes **5× more
often than its schedule** because B triggers the group every second. For N
members with schedules S₁ < S₂ < … < Sₙ, the total refresh count is
N × (cycle_time / S₁), meaning slower members do up to Sₙ/S₁ times more work
than their schedule implies.

**`slowest`:** The atomic group fires only when **all** members are due.
This minimizes CPU cost at the expense of freshness — faster members are held
back until the slowest member's schedule fires.

**Tuning Guidance:**
- Use `'fastest'` when freshness of the diamond tip matters and the cost of
  extra refreshes is acceptable.
- Use `'slowest'` when CPU budget is tight or members have very different
  schedules (e.g., 1 s vs 60 s) and the multiplication would be excessive.

```sql
-- Create with slowest policy to avoid CPU multiplication
SELECT pgtrickle.create_stream_table(
    'my_diamond_tip',
    'SELECT ... FROM a JOIN b ...',
    diamond_consistency => 'atomic',
    diamond_schedule_policy => 'slowest'
);
```

---

### pg_trickle.use_prepared_statements

Use SQL `PREPARE` / `EXECUTE` for MERGE statements during differential refresh.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `true` |
| Context | `SUSET` |
| Restart Required | No |

When enabled, the refresh executor issues `PREPARE __pgt_merge_{id}` on the first cache-hit cycle, then uses `EXECUTE` on subsequent cycles. After approximately 5 executions, PostgreSQL switches from a custom plan to a generic plan, saving 1–2 ms of parse/plan overhead per refresh.

**Tuning Guidance:**
- **Most workloads**: Leave at `true` — the cumulative parse/plan savings are significant for frequently-refreshed stream tables.
- **Highly skewed data**: Set to `false` if prepared-statement parameter sniffing produces poor plans (e.g., highly skewed LSN distributions causing bad join estimates).

```sql
-- Disable prepared statements
SET pg_trickle.use_prepared_statements = false;
```

---

### pg_trickle.user_triggers

Control how user-defined triggers on stream tables are handled during refresh.

| Property | Value |
|---|---|
| Type | `text` |
| Default | `'auto'` |
| Values | `'auto'`, `'off'` (`'on'` accepted as deprecated alias for `'auto'`) |
| Context | `SUSET` |
| Restart Required | No |

When a stream table has user-defined row-level triggers, the refresh engine can decompose the `MERGE` into explicit `DELETE` + `UPDATE` + `INSERT` statements so triggers fire with correct `TG_OP`, `OLD`, and `NEW` values.

**Values:**
- **`auto`** (default): Automatically detect user triggers on the stream table. If present, use the explicit DML path; otherwise use `MERGE`.
- **`off`**: Always use `MERGE`. User triggers are suppressed during refresh. This is the escape hatch if explicit DML causes issues.
- **`on`**: Deprecated compatibility alias for `auto`. Existing configs continue to work, but new configs should use `auto`.

**Notes:**
- Row-level triggers do **not** fire during FULL refresh regardless of this setting. FULL refresh uses `DISABLE TRIGGER USER` / `ENABLE TRIGGER USER` to suppress them.
- The explicit DML path adds ~25–60% overhead compared to MERGE for affected stream tables.
- Stream tables without user triggers have zero overhead when using `auto` (only a fast `pg_trigger` check).

```sql
-- Auto-detect (default)
SET pg_trickle.user_triggers = 'auto';

-- Suppress triggers, use MERGE
SET pg_trickle.user_triggers = 'off';

-- Backward-compatible legacy setting (treated the same as 'auto')
SET pg_trickle.user_triggers = 'on';
```

---

### Guardrails & Limits

Safety controls and hard limits.

---

### pg_trickle.block_source_ddl

When enabled, column-affecting DDL (e.g., `ALTER TABLE ... DROP COLUMN`,
`ALTER TABLE ... ALTER COLUMN ... TYPE`) on source tables tracked by stream
tables is **blocked** with an ERROR instead of silently marking stream tables
for reinitialization.

This is useful in production environments where you want to prevent accidental
schema changes that would trigger expensive full recomputation of downstream
stream tables.

**Default:** `false`  
**Context:** Superuser

```sql
-- Block column-affecting DDL on tracked source tables
SET pg_trickle.block_source_ddl = true;

-- Allow DDL (stream tables will be marked for reinit instead)
SET pg_trickle.block_source_ddl = false;
```

> **Note:** Only column-affecting changes are blocked. Benign DDL (adding
> indexes, comments, constraints) is always allowed regardless of this setting.

---

### pg_trickle.buffer_alert_threshold

When any source table's change buffer exceeds this number of rows, a
`BufferGrowthWarning` alert is emitted. Raise for high-throughput workloads,
lower for small tables.

**Default:** `1000000` (1 million rows)  
**Range:** `1000` – `100000000`

```sql
SET pg_trickle.buffer_alert_threshold = 500000;
```

---

### pg_trickle.compact_threshold

When a source table's pending change buffer exceeds this many rows,
compaction is triggered before the next refresh cycle. Compaction eliminates
net-zero INSERT+DELETE pairs (rows inserted then deleted within the same
refresh window) and collapses multi-change groups to first+last rows per
`pk_hash`, reducing delta scan overhead by 50–90% for high-churn tables.

Set to `0` to disable compaction.

**Default:** `100000` (100K rows)  
**Range:** `0` – `100000000`

```sql
-- Trigger compaction at 50K pending rows
SET pg_trickle.compact_threshold = 50000;

-- Disable compaction
SET pg_trickle.compact_threshold = 0;
```

---

### pg_trickle.max_buffer_rows

Hard limit on change buffer rows per source table. When
a source table's change buffer exceeds this limit at refresh time, pg_trickle
forces a FULL refresh and truncates the buffer, preventing unbounded disk
growth when differential refresh fails repeatedly.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `1000000` (1 million rows) |
| Range | `0` – `100000000` |
| Context | `SUSET` |
| Restart Required | No |

Set to `0` to disable the limit (not recommended for production).

**Tuning Guidance:**
- **Most workloads**: Leave at `1000000`. This accommodates high-throughput tables while preventing runaway growth.
- **High-throughput event tables**: Raise to `5000000`–`10000000` if your source tables regularly accumulate large change buffers between refresh cycles.
- **Small databases / tight disk budgets**: Lower to `100000`–`500000` to limit change buffer disk usage.

```sql
-- Set buffer limit to 5 million rows
SET pg_trickle.max_buffer_rows = 5000000;

-- Disable the limit (not recommended)
SET pg_trickle.max_buffer_rows = 0;
```

---

### pg_trickle.auto_index

Controls whether `create_stream_table()` automatically
creates performance indexes on stream tables.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `true` |
| Context | `SUSET` |
| Restart Required | No |

When enabled, the following indexes are created automatically:

1. **GROUP BY composite index** — for aggregate queries in DIFFERENTIAL mode,
   a composite index on the GROUP BY columns is created to speed up group
   lookups during MERGE.

2. **DISTINCT composite index** — for DISTINCT queries with ≤ 8 output columns,
   a composite index on all output columns is created.

3. **Covering `__pgt_row_id` index** — for stream tables with ≤ 8 output
   columns, the `__pgt_row_id` index includes all user columns via `INCLUDE`,
   enabling index-only scans during MERGE (20–50% faster for small deltas
   against large targets).

The `__pgt_row_id` index itself is always created regardless of this setting
(it is required for correctness).

**Tuning Guidance:**
- **Most workloads**: Leave at `true`.
- **Custom index strategies**: Set to `false` if you prefer to manage indexes
  manually or if the auto-created indexes conflict with your workload patterns.

```sql
-- Disable automatic index creation
SET pg_trickle.auto_index = false;
```

---

### pg_trickle.aggregate_fast_path

Controls whether stream tables with all-algebraic
aggregates use the explicit DML fast-path instead of MERGE.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `true` |
| Context | `SUSET` |
| Restart Required | No |

When enabled, stream tables whose aggregates are all algebraically invertible
(COUNT, SUM, AVG, STDDEV, VAR, CORR, REGR_*, etc.) use the explicit DML path
(DELETE + UPDATE + INSERT via a materialized temp table) instead of the generic
MERGE statement. This avoids the MERGE hash-join cost, which dominates for
aggregate queries with high group cardinality.

**Not eligible:**
- Queries with SEMI_ALGEBRAIC aggregates (MIN, MAX) — these may require
  group-rescan on extremum deletion
- Queries with GROUP_RESCAN aggregates (STRING_AGG, ARRAY_AGG, JSON_AGG, etc.)
- Queries with user-defined triggers on the stream table (already use explicit
  DML via the user-trigger path)

The `explain_st()` output shows the `aggregate_path` field:
- `explicit_dml` — fast-path is active
- `merge` — using the default MERGE path
- `merge (fast-path disabled)` — eligible but GUC is off

```sql
-- Disable aggregate fast-path
SET pg_trickle.aggregate_fast_path = false;

-- Check the current aggregate path for a stream table
SELECT * FROM pgtrickle.explain_st('my_agg_st');
```

---

### pg_trickle.template_cache

Controls the cross-backend delta template cache backed by
an UNLOGGED catalog table.

| Property | Value |
|---|---|
| Type | `bool` |
| Default | `true` |
| Context | `SUSET` |
| Restart Required | No |

When enabled, delta SQL templates generated by the DVM engine are persisted in
`pgtrickle.pgt_template_cache` so that new backends skip the ~45 ms
parse+differentiate step on their first refresh of each stream table (down to
~1 ms SPI lookup).

Templates are automatically invalidated when:
- A stream table's defining query changes (ALTER STREAM TABLE ... SET QUERY)
- A stream table is dropped
- A stream table is reinitialized

The `explain_st()` output includes `template_cache` (enabled/disabled) and
`template_cache_stats` with L2 hit and full miss counters.

```sql
-- Disable the template cache for debugging
SET pg_trickle.template_cache = false;

-- Check template cache stats
SELECT * FROM pgtrickle.explain_st('my_st')
WHERE property IN ('template_cache', 'template_cache_stats');
```

---

### pg_trickle.buffer_partitioning

Controls whether change buffer tables use `PARTITION BY RANGE (lsn)` for
O(1) cleanup via partition detach instead of O(n) DELETE.

| Value | Behaviour |
|-------|----------|
| `'off'` | **(Default)** Unpartitioned heap tables. Cleanup uses DELETE or TRUNCATE. Lowest DDL overhead per cycle. |
| `'on'` | Always create partitioned change buffers. Old partitions are detached and dropped after consumption — O(1) cleanup regardless of buffer size. Best for high-throughput sources where buffers routinely exceed `compact_threshold`. |
| `'auto'` | Start with unpartitioned buffers. If a buffer accumulates more rows than `compact_threshold` within a single refresh cycle, automatically promote it to RANGE(lsn) partitioned mode. Once promoted, the buffer stays partitioned. Combines low overhead for quiet sources with O(1) cleanup for hot ones. |

**Default:** `'off'`
**Context:** `SUSET` (superuser session-level)

```sql
-- Always partition change buffers
SET pg_trickle.buffer_partitioning = 'on';

-- Auto-promote based on throughput
SET pg_trickle.buffer_partitioning = 'auto';

-- Disable partitioning (default)
SET pg_trickle.buffer_partitioning = 'off';
```

> **Interaction with `compact_threshold`:** In `'auto'` mode, the
> `compact_threshold` value serves double duty — it triggers both compaction
> and the auto-promotion decision. Lowering `compact_threshold` makes
> auto-promotion more sensitive.

---

### pg_trickle.max_grouping_set_branches

Maximum allowed grouping set branches in `CUBE`/`ROLLUP` queries.
`CUBE(n)` produces $2^n$ branches — without a limit, large cubes cause
memory exhaustion during parsing. Users who genuinely need more than
64 branches can raise this GUC.

**Default:** `64`  
**Range:** `1` – `65536`

```sql
-- Allow up to 128 grouping set branches
SET pg_trickle.max_grouping_set_branches = 128;
```

---

### pg_trickle.volatile_function_policy

Controls how volatile functions in defining queries are handled for
DIFFERENTIAL and IMMEDIATE modes.

| Value | Behaviour |
|-------|----------|
| `reject` | **(Default)** Volatile functions cause an ERROR at stream table creation time. |
| `warn` | Volatile functions emit a WARNING but creation proceeds. Delta correctness is not guaranteed. |
| `allow` | Volatile functions are permitted silently. Use only when you understand that delta computation may produce incorrect results. |

**Default:** `reject`
**Context:** `SUSET` (superuser session-level)

```sql
-- Allow volatile functions with a warning
SET pg_trickle.volatile_function_policy = 'warn';

-- Allow volatile functions silently
SET pg_trickle.volatile_function_policy = 'allow';
```

> **Note:** Volatile functions (e.g., `random()`, `clock_timestamp()`) produce
> different values on each evaluation. In DIFFERENTIAL/IMMEDIATE modes, the
> delta computation assumes deterministic functions — volatile functions may
> cause stale or incorrect rows. FULL mode is unaffected since it recomputes
> from scratch every time.

---

### pg_trickle.unlogged_buffers

Create new change buffer tables as `UNLOGGED` to reduce WAL amplification
from CDC trigger inserts.

| Value | Behaviour |
|-------|----------|
| `false` | **(Default)** Change buffers are WAL-logged. Crash-safe — no data loss on crash recovery. |
| `true` | New change buffers are created as `UNLOGGED`. Eliminates WAL writes for trigger-inserted rows, reducing WAL amplification by ~30%. **Trade-off:** buffers are truncated on crash recovery; affected stream tables automatically receive a FULL refresh on the next scheduler cycle. |

**Default:** `false`
**Context:** `SUSET` (superuser session-level)

```sql
-- Enable UNLOGGED buffers for new stream tables
SET pg_trickle.unlogged_buffers = true;
```

> **Crash recovery:** After a PostgreSQL crash or standby restart, UNLOGGED
> buffer tables are automatically truncated by PostgreSQL. The pg_trickle
> scheduler detects this condition and enqueues a FULL refresh for each
> affected stream table on the next tick. During the window between crash
> recovery and FULL refresh completion, stream table data may be stale.

> **Standby replicas:** UNLOGGED tables are not replicated to standbys.
> Stream tables on read replicas will be stale after any standby restart
> until the next FULL refresh completes on the primary.

> **Converting existing buffers:** This GUC only affects *newly created*
> change buffer tables. To convert existing logged buffers, use:
> ```sql
> SELECT pgtrickle.convert_buffers_to_unlogged();
> ```
> This function acquires `ACCESS EXCLUSIVE` lock on each buffer table.
> Run it during a low-traffic maintenance window.

---

### pg_trickle.max_parse_depth

Maximum recursion depth for the query parser's tree visitors (G13-SD).
Prevents stack-overflow crashes on pathological queries with deeply nested
subqueries, CTEs, or set operations. When the limit is exceeded, the
parser returns a `QueryTooComplex` error instead of crashing.

**Default:** `64`
**Range:** `1` – `10000`

```sql
-- Raise the limit for deeply nested queries
SET pg_trickle.max_parse_depth = 128;
```

---

### pg_trickle.ivm_topk_max_limit

Maximum `LIMIT` value for TopK stream tables in **IMMEDIATE** mode.
TopK queries exceeding this threshold are rejected because the inline
micro-refresh (recomputing top-K rows on every DML statement) adds
latency proportional to `LIMIT`. Set to `0` to disable TopK in
IMMEDIATE mode entirely.

**Default:** `1000`  
**Range:** `0` – `1000000`

```sql
-- Allow TopK up to LIMIT 5000 in IMMEDIATE mode
SET pg_trickle.ivm_topk_max_limit = 5000;
```

---

### pg_trickle.ivm_recursive_max_depth

Maximum recursion depth for `WITH RECURSIVE` queries in **IMMEDIATE** mode.
The semi-naive evaluation injects a `__pgt_depth` counter column into the
recursive SQL; iteration stops when the counter reaches this limit. Protects
against infinite recursion in pathological graphs.

**Default:** `100`  
**Range:** `1` – `10000`

```sql
-- Allow deeper recursion for large hierarchies
SET pg_trickle.ivm_recursive_max_depth = 500;
```

---

## Invalidation Ring & Deep-Join Tuning

### SCAL-10-01 — Invalidation ring capacity ceiling

### pg_trickle.invalidation_ring_capacity

Controls the number of slots in the per-backend invalidation ring buffer.
When a source table DDL change (e.g. `ALTER TABLE`) or schema reload is
detected, the extension marks affected stream-table OIDs in this ring so
background refresh workers can schedule a full DAG rebuild.

**Default:** `128`  
**Range:** `1` – `1024`  
**Hard ceiling:** `1024` entries (enforced at registration time; values above
1024 are clamped to 1024)

```sql
-- Increase for deployments with many simultaneously-modified source tables
SET pg_trickle.invalidation_ring_capacity = 512;
```

#### Overflow behaviour

When more than `invalidation_ring_capacity` unique OIDs are invalidated in a
single burst (e.g. during a schema migration touching hundreds of tables at
once), the ring overflows. An overflow causes:

1. A full DAG rebuild to be triggered on the next scheduler tick, regardless
   of which individual OIDs were invalidated.
2. The `invalidation_ring_overflows` counter (visible via
   `pgtrickle.reliability_counters()` and the
   `pg_trickle_reliability_invalidation_ring_overflows_total` Prometheus
   metric) to be incremented by 1.

Overflows are safe but expensive — a full DAG rebuild scans all registered
stream tables. A sustained non-zero overflow rate indicates that `capacity`
should be increased.

#### Guidance for large deployments (1,000+ stream tables)

| ST count | Recommended capacity |
|----------|--------------------|
| < 200    | 128 (default)      |
| 200–500  | 256                |
| 500–1000 | 512                |
| > 1000   | 1024 (maximum)     |

> **Note:** Each ring slot consumes ~8 bytes of shared memory (allocated at
> `pg_trickle.max_shared_memory_kb`). Increasing capacity by 896 slots
> (128 → 1024) uses an extra ~7 KB of shared memory, which is negligible.

| Setting | Value |
|---------|-------|
| Default | `128` |
| Range | `1` – `1024` |
| Context | `postmaster` (requires server restart) |

---

### COR-10-01 — Deep join chain threshold

### pg_trickle.part3_max_scan_count

Maximum number of source-table rows the differential engine scans in
**Part 3** (direct scan strategy) before it escalates to a full deep-join
delta recomputation.

Part 3 applies when a join chain has depth ≤ `part3_max_scan_count` source
rows per side. Beyond this threshold the planner falls back to a more
expensive multi-pass join, which is correct at any depth but generates larger
SQL plans.

**Default:** `5`  
**Range:** `1` – `10000`

```sql
-- Tighten to Part 3 only for very small dimension tables (≤3 rows):
SET pg_trickle.part3_max_scan_count = 3;

-- Relax for moderately sized dimensions where Part 3 SQL is manageable:
SET pg_trickle.part3_max_scan_count = 20;
```

#### Trade-off: SQL complexity vs. delta correctness at depth

| Threshold | Effect |
|-----------|--------|
| Low (1–5) | Part 3 used rarely; deeper join strategy always chosen for non-trivial chains; correct but larger delta SQL and higher planning time. |
| Default (5) | Balanced; Part 3 applies only to tiny lookup tables (static enums, 1-row config rows). Recommended for most workloads. |
| High (50+) | Part 3 used aggressively; SQL is simpler but intermediate delta estimates may miss correlated rows at join depth > 6. |

#### Recommendation by join-chain depth

- **≤ 6 table join chains:** Default (`5`) is safe and near-optimal.
- **> 6 table join chains with small dimension tables:** Increase to `10`–`20`
  only if you observe excessive delta SQL plan sizes in `EXPLAIN` output.
- **Analytical workloads with 10+ table star schemas:** Leave at default `5`
  and rely on the planner's GROUP_RESCAN fallback for correctness.

> **Diagnostic:** Set `pg_trickle.log_format = 'verbose'` and look for
> `part3_direct_scan` tags in the scheduler log to see how often Part 3
> is being selected.

| Setting | Value |
|---------|-------|
| Default | `5` |
| Range | `1` – `10000` |
| Context | `superuser` |

---

## Parallel Refresh

These settings control whether and how the scheduler dispatches refresh
work to multiple dynamic background workers instead of processing
stream tables sequentially. See
[PLAN_PARALLELISM.md](https://github.com/trickle-labs/pg-trickle/blob/main/plans/sql/PLAN_PARALLELISM.md) for the design.

> **Note:** Parallel refresh has been the default (`on`). Use
> `pg_trickle.parallel_refresh_mode = 'off'` to revert to sequential execution.

### pg_trickle.parallel_refresh_mode

Controls whether the scheduler dispatches refresh work to dynamic
background workers.

| Property | Value |
|---|---|
| Type | `text` |
| Default | `'on'` |
| Values | `'off'`, `'dry_run'`, `'on'` |
| Context | `SUSET` |
| Restart Required | No |

- **`on`** (default): True parallel refresh. The coordinator builds an execution-unit
  DAG, dispatches ready units to dynamic background workers, and respects
  both the per-database cap (`max_concurrent_refreshes`) and the
  cluster-wide cap (`max_dynamic_refresh_workers`).
- **`dry_run`**: The scheduler computes execution units, logs dispatch
  decisions (unit keys, ready-queue contents, budget), but still executes
  refreshes inline. Useful for previewing parallel behaviour without
  actually spawning workers.
- **`off`**: Sequential execution. All stream tables are
  refreshed one at a time in topological order by the single scheduler
  background worker.

```sql
-- Preview parallel dispatch decisions without changing runtime behaviour
SET pg_trickle.parallel_refresh_mode = 'dry_run';

-- Enable parallel refresh
SET pg_trickle.parallel_refresh_mode = 'on';
```

---

### pg_trickle.max_dynamic_refresh_workers

Cluster-wide cap on concurrently active pg_trickle dynamic refresh workers.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `4` |
| Range | `0` – `64` |
| Context | `SUSET` |
| Restart Required | No |

This is distinct from `pg_trickle.max_concurrent_refreshes` (per-database
cap). When multiple databases each have their own scheduler, this GUC
prevents them from overcommitting the shared PostgreSQL
`max_worker_processes` budget.

**Worker-budget planning:** Each dynamic refresh worker consumes one
`max_worker_processes` slot. In addition, pg_trickle uses one slot for
the launcher and one per-database scheduler. Ensure:

```
max_worker_processes >= pg_trickle launchers (1)
                      + pg_trickle schedulers (1 per database)
                      + max_dynamic_refresh_workers
                      + autovacuum workers
                      + parallel query workers
                      + other extensions
```

A typical small deployment (1–2 databases, 4 parallel workers) needs at
least `max_worker_processes = 16`. The E2E test Docker image uses 128.

```sql
-- Allow up to 8 concurrent refresh workers cluster-wide
SET pg_trickle.max_dynamic_refresh_workers = 8;
```

---

### pg_trickle.max_concurrent_refreshes

Per-database dispatch cap for parallel refresh workers.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `4` |
| Range | `1` – `32` |
| Context | `SUSET` |
| Restart Required | No |

When `parallel_refresh_mode = 'on'`, this limits how many execution units
a single database coordinator may have in-flight at the same time. In
sequential mode (`parallel_refresh_mode = 'off'`), this setting has no
effect.

The effective concurrent refreshes for a database is:

```
min(max_concurrent_refreshes, max_dynamic_refresh_workers - workers_used_by_other_dbs)
```

```sql
-- Allow up to 8 concurrent refreshes in this database
SET pg_trickle.max_concurrent_refreshes = 8;
```

---

### pg_trickle.per_database_worker_quota

Per-database dynamic refresh worker quota for multi-tenant cluster isolation.

| Property | Value |
|---|---|
| Type | `int` |
| Default | `0` (disabled) |
| Range | `0` – `64` |
| Context | `SUSET` |
| Restart Required | No |

When greater than 0, each per-database scheduler limits itself to this many
concurrently active dynamic refresh workers drawn from the shared
`max_dynamic_refresh_workers` pool. This prevents a single busy database
from starving others in multi-tenant clusters.

**Burst capacity:** when the cluster is lightly loaded (active workers
< 80% of `max_dynamic_refresh_workers`), a database may temporarily
exceed its quota by up to 50% to absorb sudden change backlogs. The burst
is reclaimed automatically within 1 scheduler cycle once global load rises
back above the 80% threshold.

**Priority dispatch:** within each dispatch tick, IMMEDIATE-trigger closures
are dispatched before all other unit kinds, ensuring transactional
consistency requirements are always met first, even under quota pressure.

```sql
-- Limit the analytics DB to 4 base workers (bursts to 6 when cluster is idle)
ALTER DATABASE analytics SET pg_trickle.per_database_worker_quota = 4;
-- Give the reporting DB only 2 base workers
ALTER DATABASE reporting  SET pg_trickle.per_database_worker_quota = 2;
SELECT pg_reload_conf();
```

When `per_database_worker_quota = 0` (the default), this feature is
disabled and all databases share the `max_dynamic_refresh_workers` pool
on a first-come-first-served basis, bounded per coordinator by
`max_concurrent_refreshes`.

> **Note:** Set this GUC per-database with `ALTER DATABASE` rather than
> globally with `ALTER SYSTEM`, so different databases can have different
> quotas.

---

## Advanced / Internal

### pg_trickle.change_buffer_schema

Schema name for change-buffer tables created by the trigger-based CDC
pipeline.

**Default:** `'pgtrickle_changes'`

Change buffer tables are named `<schema>.changes_<oid>` where `<oid>` is
the source table's OID. Placing them in a dedicated schema keeps them out
of the `public` namespace.

```sql
SET pg_trickle.change_buffer_schema = 'my_change_buffers';
```

---

### pg_trickle.foreign_table_polling

Enable polling-based change detection for foreign table sources. When
enabled, the scheduler periodically re-executes the foreign table query
and computes deltas via snapshot comparison (`EXCEPT ALL`). Foreign tables
cannot use trigger or WAL-based CDC, so this is the only mechanism for
incremental maintenance.

**Default:** `false`

```sql
SET pg_trickle.foreign_table_polling = true;
```

---

### pg_trickle.matview_polling

Enable polling-based CDC for materialized views. When enabled, materialized
views referenced in defining queries are supported via snapshot-comparison
(the same mechanism as foreign table polling). A local shadow table stores
the previous state; `EXCEPT ALL` computes the delta on each refresh cycle.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `false` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
SET pg_trickle.matview_polling = true;
```

---

### pg_trickle.cdc_trigger_mode

Controls the CDC trigger granularity: `statement` (default) or `row`.

`statement` uses statement-level `AFTER` triggers with transition tables
(`NEW TABLE` / `OLD TABLE`). A single invocation per DML statement processes
all affected rows in one bulk `INSERT ... SELECT`, giving 50-80% less
write-side overhead for bulk `UPDATE`/`DELETE`. Single-row DML is unaffected.

`row` uses the legacy per-row trigger approach (pg_trickle < 0.4.0 behavior).

Changing this setting takes effect for newly installed CDC triggers. Call
`pgtrickle.rebuild_cdc_triggers()` to migrate existing stream tables.

| Property | Value |
|---|---|
| Type | `string` |
| Default | `'statement'` |
| Valid values | `statement`, `row` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Switch to statement-level triggers (default, recommended)
SET pg_trickle.cdc_trigger_mode = 'statement';

-- After changing, rebuild existing triggers:
SELECT pgtrickle.rebuild_cdc_triggers();
```

---

### pg_trickle.tick_watermark_enabled

Cap CDC consumption to the WAL LSN at scheduler tick start. When enabled
(default), each scheduler tick captures `pg_current_wal_lsn()` at its start
and prevents any refresh from consuming WAL changes beyond that LSN. This
bounds cross-source staleness without requiring user configuration.

Disable only if you need stream tables to always advance to the latest
available LSN.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `true` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Disable tick watermark bounding
SET pg_trickle.tick_watermark_enabled = false;
```

---

### pg_trickle.watermark_holdback_timeout

Maximum seconds a user-provided watermark may remain un-advanced before
being considered **stuck**. When a watermark group contains a source whose
watermark has not been advanced within this timeout, downstream stream
tables in that group are paused (refresh is skipped) and a
`pgtrickle_alert` NOTIFY with `watermark_stuck` event is emitted.

When the stuck watermark is advanced again (via `advance_watermark()`), the
pause is automatically lifted and a `watermark_resumed` event is emitted.

Set to `0` to disable stuck-watermark detection (default). Useful values
depend on your ETL pipeline cadence -- for a pipeline that loads every 5
minutes, a timeout of `600` (10 min) gives a safety margin.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (disabled) |
| Min | `0` |
| Max | `86400` (24 hours) |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Set stuck-watermark timeout to 10 minutes
ALTER SYSTEM SET pg_trickle.watermark_holdback_timeout = 600;
SELECT pg_reload_conf();
```

**NOTIFY payloads:**

```json
{"event":"watermark_stuck","group":"order_pipeline","source_oid":16385,"age_secs":620}
{"event":"watermark_resumed","source_oid":16385}
```

---

### pg_trickle.spill_threshold_blocks

Temp blocks written threshold for spill detection. After each differential
MERGE, pg\_trickle queries `pg_stat_statements` for the `temp_blks_written`
metric. If the value exceeds this threshold, the refresh is considered a
**spill**.

After `spill_consecutive_limit` consecutive spills, the scheduler forces a
FULL refresh for that stream table to prevent repeated expensive
differential merges.

Requires the `pg_stat_statements` extension to be installed. Set to `0` to
disable spill detection (default).

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (disabled) |
| Min | `0` |
| Max | `100000000` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Enable spill detection: flag > 1000 temp blocks as a spill
ALTER SYSTEM SET pg_trickle.spill_threshold_blocks = 1000;
SELECT pg_reload_conf();
```

---

### pg_trickle.spill_consecutive_limit

Number of consecutive spilling differential refreshes before the scheduler
automatically forces a FULL refresh. Resets after any non-spilling refresh.

Only effective when `spill_threshold_blocks > 0`.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `3` |
| Min | `1` |
| Max | `100` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Force FULL after 5 consecutive spills (default: 3)
ALTER SYSTEM SET pg_trickle.spill_consecutive_limit = 5;
SELECT pg_reload_conf();
```

---

### pg_trickle.log_merge_sql

Log the generated MERGE SQL template on every refresh cycle. When enabled,
the MERGE SQL template built during differential refresh is emitted to the
PostgreSQL server log at `LOG` level.

**Intended for debugging MERGE query generation only. Do not enable in
production** — the output is verbose and includes the full SQL for every
refresh.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `false` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
SET pg_trickle.log_merge_sql = true;
```

---

## Guardrails & Diagnostics

These GUCs control safety thresholds and diagnostic warnings.

### pg_trickle.fuse_default_ceiling

Global default change-count ceiling for the fuse circuit breaker. When a
stream table has `fuse_mode = 'on'` or `'auto'` and no per-ST `fuse_ceiling`,
this value is used. If pending changes exceed this count, the fuse blows
and the stream table is suspended (status = `SUSPENDED`).

Set to `0` to disable the global default (per-ST ceilings still apply).

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (disabled) |
| Range | 0 - 2,000,000,000 |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Set global fuse ceiling to 1 million rows
SET pg_trickle.fuse_default_ceiling = 1000000;
```

---

### pg_trickle.delta_amplification_threshold

Delta amplification detection threshold (output/input ratio). When a
`DIFFERENTIAL` refresh produces more than this multiple of the input delta
rows, a `WARNING` is emitted so operators can identify pathological join
fan-out or many-to-many amplification.

Set to `0.0` to disable.

| Property | Value |
|---|---|
| Type | `float` |
| Default | `0.0` (disabled) |
| Range | 0.0 - 100,000.0 |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Warn when delta output is 10x the input
SET pg_trickle.delta_amplification_threshold = 10.0;
```

---

### pg_trickle.algebraic_drift_reset_cycles

Differential cycles between automatic full recomputes for algebraic
aggregates. After this many differential refresh cycles, stream tables
with algebraic aggregates (`AVG`, `STDDEV`, `VAR`) are automatically
reinitialized to reset accumulated floating-point drift in auxiliary
columns.

Set to `0` to disable automatic resets.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (disabled) |
| Range | 0 - 100,000 |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Reset algebraic aggregates every 10,000 cycles
SET pg_trickle.algebraic_drift_reset_cycles = 10000;
```

---

### pg_trickle.agg_diff_cardinality_threshold

Estimated `GROUP BY` cardinality threshold for algebraic aggregate warnings.
At `create_stream_table` time, if the defining query uses algebraic
aggregates (`SUM`, `COUNT`, `AVG`) in `DIFFERENTIAL` mode and the estimated
group cardinality is below this threshold, a `WARNING` is emitted suggesting
`FULL` or `AUTO` mode.

Set to `0` to disable the warning.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (disabled) |
| Range | 0 - 100,000,000 |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Warn when GROUP BY cardinality is below 100
SET pg_trickle.agg_diff_cardinality_threshold = 100;
```

---

### Connection Pooler

#### pg_trickle.connection_pooler_mode

Cluster-wide connection pooler compatibility override.

| Property | Value |
|----------|-------|
| Type | `string` |
| Default | `'off'` |
| Valid values | `'off'`, `'transaction'`, `'session'` |
| Context | `SUSET` |

| Value | Behaviour |
|-------|-----------|
| `off` (default) | Per-ST `pooler_compatibility_mode` governs |
| `transaction` | Globally disable prepared-statement reuse and suppress NOTIFY emissions (PgBouncer transaction-pool compatibility) |
| `session` | Explicit opt-in to session mode (same as `off` today, reserved for future use) |

See [Connection Pooler Compatibility](PRE_DEPLOYMENT.md#connection-pooler-compatibility)
for deployment guidance.

```sql
-- Enable transaction-mode pooler compatibility globally
SET pg_trickle.connection_pooler_mode = 'transaction';
```

---

### History & Retention

#### pg_trickle.history_retention_days

Number of days to retain rows in `pgtrickle.pgt_refresh_history`.

| Property | Value |
|----------|-------|
| Type | `integer` |
| Default | `90` |
| Min | `0` (disabled) |
| Max | `36500` (~100 years) |
| Context | `SUSET` |

The scheduler runs a daily background cleanup that deletes rows older than
this many days. Set to `0` to disable automatic cleanup (history grows
unbounded — monitor disk usage).

```sql
-- Keep 30 days of refresh history
SET pg_trickle.history_retention_days = 30;
```

---

### Circular Dependencies

> Circular dependency support is available for safe
> monotone cycles in DIFFERENTIAL mode. These settings control whether cycles
> are allowed at all and how many fixpoint iterations the scheduler will try
> before surfacing a non-convergence error.

### pg_trickle.allow_circular

Master switch for circular (cyclic) stream table dependencies. When `false`
(default), creating a stream table that would introduce a cycle in the
dependency graph is rejected with a `CycleDetected` error. When `true`,
monotone cycles — those containing only safe operators (joins, filters,
projections, UNION ALL, INTERSECT, EXISTS) — are allowed.

Non-monotone operators (Aggregate, EXCEPT, Window functions, NOT EXISTS)
always block cycle creation regardless of this setting, because they cannot
guarantee convergence to a fixed point.

**Default:** `false`

```sql
SET pg_trickle.allow_circular = true;
```

### pg_trickle.max_fixpoint_iterations

Maximum number of iterations per strongly connected component (SCC) before
the scheduler declares non-convergence and marks all SCC members as ERROR.
Prevents runaway loops in circular dependency chains.

For most practical use cases (transitive closure, graph reachability),
convergence happens in 2–5 iterations. The default of 100 provides ample
headroom.

**Default:** `100`  
**Range:** `1` – `10000`

```sql
SET pg_trickle.max_fixpoint_iterations = 50;
```

---

### pg_trickle.self_monitoring_auto_apply

Controls whether the self-monitoring analytics stream tables can automatically
adjust stream table configuration.

| Value | Behaviour |
|-------|-----------|
| `off` (default) | Advisory only — no automatic changes. Dog-feeding stream tables produce analytics that operators and dashboards can read, but nothing is applied automatically. |
| `threshold_only` | After each 10-minute auto-apply cycle, reads `df_threshold_advice`. If a recommendation has HIGH confidence and the recommended threshold differs from the current threshold by more than 5%, applies `ALTER STREAM TABLE ... SET auto_threshold = <recommended>`. Changes are logged with `initiated_by = 'SELF_MONITOR'`. |
| `full` | Same as `threshold_only`, plus applies scheduling hints from `df_scheduling_interference` (future enhancement). |

**Default:** `off`

```sql
-- Enable threshold auto-apply.
SET pg_trickle.self_monitoring_auto_apply = 'threshold_only';

-- Check current setting.
SHOW pg_trickle.self_monitoring_auto_apply;
```

**Prerequisites:** Dog-feeding stream tables must be created first via
`SELECT pgtrickle.setup_self_monitoring()`. If the stream tables do not exist,
the auto-apply worker is a no-op.

**Rate limiting:** At most one threshold change per stream table per 10 minutes.

**Audit trail:** All auto-apply changes are recorded in `pgt_refresh_history`
with `initiated_by = 'SELF_MONITOR'` and a SKIP action describing the old and new
threshold values.

---

## Scheduler Scalability

### pg_trickle.worker_pool_size

Number of persistent background workers kept ready in a pool. When `> 0`,
the scheduler reuses these workers across refresh cycles instead of spawning
a new worker for each job, eliminating the ~2 ms per-worker startup cost.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (spawn-per-task) |
| Range | 0 – 64 |
| Context | `SUSET` (superuser) |
| Restart required | Yes |

```sql
-- Keep 4 persistent workers ready at all times
ALTER SYSTEM SET pg_trickle.worker_pool_size = 4;
SELECT pg_reload_conf();
```

Set to `0` to use the original spawn-per-task model (default, no change from
the previous behavior (spawn-per-task model)).

### pg_trickle.template_cache_max_entries

Maximum number of entries in the per-backend L1 delta SQL template cache. When
the cache reaches this limit, the least-recently-used entry is evicted.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (unbounded) |
| Range | 0 – 65536 |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Cap the template cache at 200 entries per backend
SET pg_trickle.template_cache_max_entries = 200;
```

---

## Operability, Observability & DR

### pg_trickle.metrics_port

TCP port for the Prometheus/OpenMetrics endpoint served by the per-database
background scheduler. When non-zero, `GET /metrics` returns all pg_trickle
monitoring metrics in Prometheus text format.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (disabled) |
| Range | 0 – 65535 |
| Context | `SUSET` (superuser) |
| Restart required | Yes |

```sql
-- Expose metrics on port 9188 (per database)
ALTER DATABASE mydb SET pg_trickle.metrics_port = 9188;
SELECT pg_reload_conf();
```

Use `0` (the default) to disable the HTTP endpoint. Each database runs its
own scheduler, so the port must be unique per database on the same host.

### pg_trickle.metrics_request_timeout_ms

Maximum milliseconds the metrics HTTP handler is allowed to run. If a slow
HTTP client holds the connection open longer, it is dropped. This protects the
scheduler tick loop from being blocked by unresponsive Prometheus scrapers.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `5000` (5 seconds) |
| Range | 0 (no timeout) – 600 000 (10 min) |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.frontier_holdback_mode

Controls how the scheduler prevents silent data loss from long-running
transactions. When an uncommitted transaction has written rows to a source
table, those change-buffer rows must not be included in a refresh until the
transaction commits (or rolls back).

| Property | Value |
|---|---|
| Type | `text` |
| Default | `'xmin'` |
| Values | `'xmin'`, `'none'`, `'lsn:<N>'` |
| Context | `SUSET` (superuser) |
| Restart required | No |

| Value | Behaviour |
|-------|-----------|
| `'xmin'` | (Default) Probes `pg_stat_activity` + `pg_prepared_xacts` once per tick; caps the frontier to exclude rows from uncommitted transactions. |
| `'none'` | No holdback — maximum performance but can skip rows from long-lived transactions. Not recommended for production. |
| `'lsn:<N>'` | Hold back by exactly N bytes. Debugging use only. |

### pg_trickle.frontier_holdback_warn_seconds

Emit a WARNING when a holdback-causing transaction has been blocking frontier
advancement for longer than this many seconds. The warning fires at most once
per minute to avoid log spam. Useful for identifying forgotten long-running
transactions.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `300` (5 minutes) |
| Range | 0 (disabled) – 3600 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.publication_lag_warn_bytes

Emit a WARNING and defer change-buffer truncation when a downstream logical
replication subscriber's confirmed WAL position lags behind the current
change buffer by more than this many bytes.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (disabled) |
| Range | 0 – 2 147 483 647 |
| Context | `SUSET` (superuser) |
| Restart required | No |

This prevents data loss for subscribers that rely on the change buffer as
part of their replication pipeline.

### pg_trickle.schedule_recommendation_min_samples

Minimum number of refresh-history observations before
`pgtrickle.recommend_schedule()` returns a recommendation with non-zero
confidence. Raise this for more reliable recommendations; lower it to get
early guidance on new stream tables.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `10` |
| Range | 1 – 1000 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.schedule_alert_cooldown_seconds

Minimum seconds between consecutive `predicted_sla_breach` alerts for the
same stream table. Prevents log spam when the cost model consistently predicts
an imminent SLA violation.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `300` (5 minutes) |
| Range | 0 (no cooldown) – 86 400 |
| Context | `SUSET` (superuser) |
| Restart required | No |

---

## Transactional Outbox

These GUCs control the transactional outbox subsystem. See the SQL Reference
for the `enable_outbox()`, `poll_outbox()`, and consumer group functions.

### pg_trickle.outbox_enabled

Master enable/disable switch for the outbox subsystem.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `true` |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.outbox_retention_hours

Default retention period (in hours) for outbox rows. Rows older than this
threshold are eligible for the background drain sweep. Can be overridden
per stream table via `enable_outbox(retention_hours => N)`.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `24` |
| Range | 1 – 87 600 (10 years) |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.outbox_drain_batch_size

Number of expired outbox rows deleted in a single background drain pass.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `1000` |
| Range | 1 – 1 000 000 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.outbox_drain_interval_seconds

Seconds between background outbox drain sweeps. Set to `0` to disable
automatic draining (you would then drain manually with `outbox_rows_consumed()`).

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `60` |
| Range | 0 (disabled) – 86 400 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.outbox_inline_threshold_rows

Maximum number of delta rows stored inline in the outbox payload. When a
refresh delta exceeds this count, pg_trickle switches to **claim-check** mode:
the payload is stored in a separate table and `poll_outbox()` returns
`is_claim_check = true` with a `NULL` payload.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `10000` |
| Range | 0 (always inline) – 10 000 000 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.outbox_skip_empty_delta

When `true`, no outbox row is written for refreshes that produce zero inserted
and zero deleted rows. This reduces outbox table growth for frequently-scheduled
stream tables with sparse updates.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `true` |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.outbox_storage_critical_mb

Size threshold (in MB) at which the outbox table is considered critically large.
When exceeded, a WARNING is emitted on each refresh cycle.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `1024` (1 GB) |
| Range | 1 – 10 000 000 (10 TB) |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.outbox_force_retention

When `true`, outbox rows are kept past their `retention_hours` expiry until
**all** consumer groups have committed an offset past them. Prevents consumers
that are temporarily offline from missing messages.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `false` |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.consumer_dead_threshold_hours

Hours of silence (no heartbeat) after which a consumer is marked as dead and
eligible for cleanup (when `consumer_cleanup_enabled = true`).

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `24` |
| Range | 1 – 87 600 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.consumer_stale_offset_threshold_days

Days of no offset progress after which a consumer's offset record is
considered stale and eligible for cleanup.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `7` |
| Range | 1 – 3650 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.consumer_cleanup_enabled

Enable automatic background cleanup of dead and stale consumer offsets
and leases. When disabled, old records must be removed manually.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `true` |
| Context | `SUSET` (superuser) |
| Restart required | No |

---

## Transactional Inbox

These GUCs control the transactional inbox subsystem. See the SQL Reference
for `create_inbox()` and related functions.

### pg_trickle.inbox_enabled

Master enable/disable switch for the inbox subsystem.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `true` |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.inbox_processed_retention_hours

Retention period (in hours) for successfully processed inbox messages
(`processed_at IS NOT NULL`). Rows older than this threshold are deleted
by the background drain sweep.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `72` (3 days) |
| Range | 1 – 87 600 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.inbox_dlq_retention_hours

Retention period (in hours) for dead-letter queue rows. Set to `0` to
keep DLQ rows indefinitely (useful for forensics and manual replay).

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (keep forever) |
| Range | 0 (keep forever) – 87 600 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.inbox_drain_batch_size

Number of expired inbox messages deleted in a single background drain pass.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `1000` |
| Range | 1 – 1 000 000 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.inbox_drain_interval_seconds

Seconds between inbox background drain sweeps. Set to `0` to disable
automatic draining.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `60` |
| Range | 0 (disabled) – 86 400 |
| Context | `SUSET` (superuser) |
| Restart required | No |

### pg_trickle.inbox_dlq_alert_max_per_refresh

Maximum number of DLQ alert events raised per refresh cycle. Limits log
volume when many messages are failing simultaneously. Set to `0` to disable
DLQ alerting.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `10` |
| Range | 0 (disabled) – 100 |
| Context | `SUSET` (superuser) |
| Restart required | No |

---

## Pre-GA Correctness & Stability

### pg_trickle.use_sqlstate_classification

When `true`, the retry classification for SPI errors uses the five-character
PostgreSQL SQLSTATE class rather than message-text pattern matching. This
makes retry decisions locale-independent (safe with `lc_messages=fr_FR` or
other non-English locales).

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `false` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Enable locale-safe SQLSTATE-based retry classification
ALTER SYSTEM SET pg_trickle.use_sqlstate_classification = true;
SELECT pg_reload_conf();
```

Set to `true` in any deployment where `lc_messages` is not `en_US.UTF-8`, or
in mixed-locale clusters.

### pg_trickle.template_cache_max_age_hours

Maximum age (in hours) for entries in the L2 catalog-backed template cache
(`pgtrickle.pgt_template_cache`). Entries older than this limit are purged
by the background scheduler on each tick. Keeping the cache fresh ensures
stale delta SQL templates do not persist after a stream table schema change.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `168` (7 days) |
| Range | 1 – 8760 (1 year) |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Purge L2 cache entries older than 24 hours
ALTER SYSTEM SET pg_trickle.template_cache_max_age_hours = 24;
SELECT pg_reload_conf();
```

Lower values reduce the risk of stale templates surviving a schema change.
Higher values improve performance by retaining warm cache entries across
long maintenance windows.

### pg_trickle.max_parse_nodes

Maximum estimated number of parse-tree nodes allowed in a stream table
defining query. When `> 0`, queries whose estimated node count exceeds this
limit are rejected with a `QueryTooComplex` error before the OpTree builder
allocates memory. This guards against pathological queries such as
`WHERE id IN (1, 2, …, 1 000 000)` that would otherwise exhaust per-backend
memory during delta SQL generation.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `0` (disabled) |
| Range | 0 (disabled) – 10,000,000 |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Reject defining queries with more than 100 000 estimated nodes
ALTER SYSTEM SET pg_trickle.max_parse_nodes = 100000;
SELECT pg_reload_conf();
```

Node count is estimated conservatively as `len(query) / 4`.  The default of
`0` disables the check for backward compatibility.  A value of `100000` is
recommended for production deployments.

---

## Citus Distributed Tables

Configuration for Citus distributed-table CDC and stream table support.
See [docs/integrations/citus.md](integrations/citus.md) for the full setup guide.

---

### pg_trickle.citus_st_lock_lease_ms

Duration in milliseconds of the `pgtrickle.pgt_st_locks` lease used for
cross-node refresh coordination in Citus clusters.

The lease prevents two coordinator nodes from applying changes to the same
distributed stream table simultaneously.  When pg_ripple is deployed alongside
pg_trickle, set this value to be **≥ `pg_ripple.merge_fence_timeout_ms`** to
prevent the lease from expiring mid-merge.

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `60000` (60 seconds) |
| Range | 0 (disabled) – 600,000 ms (10 minutes) |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Align with a 30-second pg_ripple merge fence
ALTER SYSTEM SET pg_trickle.citus_st_lock_lease_ms = 45000;
SELECT pg_reload_conf();
```

---

### pg_trickle.citus_worker_retry_ticks

Number of consecutive per-worker poll failures before the scheduler emits a
`WARNING` in the PostgreSQL log and flags the worker in `pgtrickle.citus_status`.
The warning is for operator attention — healthy workers continue refreshing
uninterrupted while a failed worker is skipped.

Set to `0` to disable the alerting entirely (failures are still logged at
`LOG` level per tick).

| Property | Value |
|---|---|
| Type | `integer` |
| Default | `5` |
| Range | 0 (disabled) – 100 |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Alert after 3 consecutive failures instead of 5
ALTER SYSTEM SET pg_trickle.citus_worker_retry_ticks = 3;
SELECT pg_reload_conf();

-- Disable alerting (failures logged at LOG level only)
ALTER SYSTEM SET pg_trickle.citus_worker_retry_ticks = 0;
SELECT pg_reload_conf();
```

---

## WAL Backpressure & Logging

### pg_trickle.enforce_backpressure

When `true`, CDC trigger writes are suppressed when the WAL replication slot
lag exceeds `pg_trickle.slot_lag_critical_threshold_mb`. Writes resume once
the lag drops below 50 % of the threshold (hysteresis).

When `false` (default), pg_trickle only emits `WARNING` log messages when slot
lag is critical but does **not** suppress writes. Use `enforce_backpressure =
true` only when disk exhaustion is an immediate risk.

> **Important:** `enforce_backpressure = true` operates in **discard mode** —
> changes that arrive while backpressure is active are **dropped** from the
> CDC buffer. Stream tables must be reinitialized after backpressure clears to
> recover from the data gap. See also `pg_trickle.cdc_capture_mode`.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `false` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Enable backpressure suppression (discard mode)
ALTER SYSTEM SET pg_trickle.enforce_backpressure = true;
SELECT pg_reload_conf();
-- After clearing: reinitialize affected stream tables
SELECT pgtrickle.refresh_stream_table('my_stream', 'FULL');
```

---

### pg_trickle.log_format

Log format for pg_trickle structured log events.

- `"text"` (default): Unstructured human-readable messages.
- `"json"`: Structured JSON with fields `event`, `pgt_id`, `cycle_id`,
  `duration_ms`, `refresh_reason`, `error_code`. Suitable for log aggregation
  pipelines (Loki, OpenTelemetry).

| Property | Value |
|---|---|
| Type | `string` |
| Default | `text` |
| Valid values | `text`, `json` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Switch to JSON structured logging
ALTER SYSTEM SET pg_trickle.log_format = 'json';
SELECT pg_reload_conf();
```

---

## pgVectorMV & OpenTelemetry

### pg_trickle.enable_vector_agg

When `true`, `avg(vector_col)` and `sum(vector_col)` in stream table defining
queries are handled by the DVM engine using incremental aggregate operators for
`vector`, `halfvec`, and `sparsevec` types. Requires the `pgvector` extension.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `false` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
ALTER SYSTEM SET pg_trickle.enable_vector_agg = true;
SELECT pg_reload_conf();
```

---

### pg_trickle.enable_trace_propagation

When `true`, pg_trickle reads `pg_trickle.trace_id` from the session GUC at
CDC capture time and stores the W3C traceparent in the change buffer column
`__pgt_trace_context`. At refresh time, spans are exported via OTLP/gRPC to
`pg_trickle.otel_endpoint`.

| Property | Value |
|---|---|
| Type | `boolean` |
| Default | `false` |
| Context | `SUSET` (superuser) |
| Restart required | No |

---

### pg_trickle.otel_endpoint

OTLP/gRPC endpoint for OpenTelemetry span export. Empty string (default)
disables span export.

| Property | Value |
|---|---|
| Type | `string` |
| Default | `''` (disabled) |
| Example | `'http://jaeger:4317'` |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Export spans to a local Jaeger instance
ALTER SYSTEM SET pg_trickle.otel_endpoint = 'http://localhost:4317';
ALTER SYSTEM SET pg_trickle.enable_trace_propagation = true;
SELECT pg_reload_conf();
```

---

### pg_trickle.trace_id

Session-level W3C traceparent header for trace context propagation. Set this
in the application session before DML so CDC capture links the changes to the
initiating trace. Requires `enable_trace_propagation = true`.

| Property | Value |
|---|---|
| Type | `string` |
| Default | `''` |
| Context | `USERSET` (any user) |
| Restart required | No |

```sql
-- Propagate a trace across CDC capture
SET pg_trickle.trace_id = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
INSERT INTO orders VALUES (42, 'widget', 5);
```

---

## Operational Truthfulness

### pg_trickle.cdc_capture_mode

Controls what happens to CDC writes when `pg_trickle.cdc_paused = on`.

- `"discard"` (default): CDC trigger bodies return `NULL`; changes arriving
  while paused are **dropped**. Stream tables must be reinitialized after
  un-pausing to recover from the data gap.
- `"hold"`: Reserved for a future durable capture-and-hold mode. Currently
  emits a `WARNING` and falls back to `"discard"`.

> **Operator checklist:** Before setting `cdc_paused = on`, check
> `pgtrickle.cdc_pause_status()` to confirm the active mode. After
> un-pausing in discard mode, run `pgtrickle.refresh_stream_table('<name>')`
> with `FULL` mode for each affected stream table.

| Property | Value |
|---|---|
| Type | `string` |
| Default | `discard` |
| Valid values | `discard`, `hold` (reserved) |
| Context | `SUSET` (superuser) |
| Restart required | No |

```sql
-- Check the current CDC pause status
SELECT * FROM pgtrickle.cdc_pause_status();

-- Pause CDC (discard mode — changes arriving now are DROPPED)
ALTER SYSTEM SET pg_trickle.cdc_paused = on;
SELECT pg_reload_conf();

-- After maintenance, un-pause and reinitialize affected tables
ALTER SYSTEM SET pg_trickle.cdc_paused = off;
SELECT pg_reload_conf();
-- Full refresh to recover from the gap:
SELECT pgtrickle.refresh_stream_table('public.my_stream_table', 'FULL');
```

---

## GUC Interaction Matrix

Some GUC variables interact with or depend on each other. The table below
documents these cross-dependencies to help avoid misconfiguration.

| GUC A | GUC B | Interaction |
|-------|-------|-------------|
| `auto_backoff` | `min_schedule_seconds` | `auto_backoff` stretches the effective interval up to 8× the configured schedule, but never below `min_schedule_seconds`. If `min_schedule_seconds` is high, backoff has limited room to operate. |
| `auto_backoff` | `default_schedule_seconds` | The backoff multiplier is applied to `default_schedule_seconds` (or the per-ST override); raising this value gives backoff a wider range. |
| `parallel_refresh_mode` | `max_concurrent_refreshes` | `parallel_refresh_mode = 'on'` dispatches independent STs to parallel workers, up to `max_concurrent_refreshes` per database. Setting `max_concurrent_refreshes = 1` effectively disables parallelism even when the mode is `'on'`. |
| `parallel_refresh_mode` | `max_dynamic_refresh_workers` | `max_dynamic_refresh_workers` is a cluster-wide cap across all databases. If you have 4 databases each wanting 4 concurrent refreshes, set this to ≥16 (or accept queuing). |
| `max_dynamic_refresh_workers` | `per_database_worker_quota` | When `per_database_worker_quota > 0`, each database claims at most that many workers from the shared `max_dynamic_refresh_workers` pool. Set `per_database_worker_quota` to `max_dynamic_refresh_workers / n_databases` for equal sharing. Burst to 150% is allowed when the cluster is < 80% loaded. |
| `differential_max_change_ratio` | `fuse_default_ceiling` | Both guard against large change batches but at different levels: `differential_max_change_ratio` triggers a FULL refresh fallback (proportional to table size), while `fuse_default_ceiling` halts refresh entirely (absolute row count). The fuse fires first if the change count exceeds it, regardless of the ratio. |
| `block_source_ddl` | DDL operations | When `true`, DDL on source tables (ALTER TABLE, DROP COLUMN) is blocked by an event trigger. Disable temporarily with `SET pg_trickle.block_source_ddl = false` before schema migrations, then re-enable. |
| `cdc_mode` | `cdc_trigger_mode` | `cdc_trigger_mode` (`'statement'` / `'row'`) only applies when CDC is trigger-based. When `cdc_mode = 'wal'` (or after auto-transition to WAL), `cdc_trigger_mode` is irrelevant. |
| `cdc_mode` | `wal_transition_timeout` | `wal_transition_timeout` only applies when `cdc_mode = 'auto'`. It controls how many seconds to wait for the first WAL-based refresh to succeed before falling back to triggers. |
| `cleanup_use_truncate` | `compact_threshold` | `cleanup_use_truncate = true` uses TRUNCATE to clear consumed change buffers (fastest, acquires AccessExclusiveLock briefly). `compact_threshold` controls when fully-consumed buffers are compacted via DELETE — only relevant when TRUNCATE is disabled. |
| `buffer_partitioning` | `compact_threshold` | In `'auto'` mode, `compact_threshold` serves as the promotion trigger: if a buffer exceeds this many rows in a single refresh cycle, it is promoted to RANGE(lsn) partitioned mode. Lowering `compact_threshold` makes auto-promotion more sensitive. |
| `allow_circular` | `max_fixpoint_iterations` | `max_fixpoint_iterations` is only evaluated when `allow_circular = true`. It caps the number of convergence iterations for circular dependency chains. |
| `ivm_topk_max_limit` | TopK queries | Queries with `LIMIT > ivm_topk_max_limit` fall back to FULL refresh instead of the optimized TopK path. Raise this if you have legitimate large TopK queries. |
| `ivm_recursive_max_depth` | Recursive CTEs | Recursive expansion beyond `ivm_recursive_max_depth` iterations is terminated with a warning and falls back to FULL refresh. Set to 0 to disable the guard (not recommended). |

---

## Tuning Profiles

Three named profiles for common deployment patterns. Copy the relevant
settings into your `postgresql.conf` and adjust to taste.

### Low-Latency Profile

**Goal:** Minimize end-to-end latency from base table write to stream table
update. Best for dashboards, real-time analytics, and operational monitoring.

```ini
# Fast scheduling (polling-based, sub-200ms median latency)
pg_trickle.scheduler_interval_ms = 200       # poll interval
pg_trickle.min_schedule_seconds = 1
pg_trickle.default_schedule_seconds = 1

# Parallel refresh for independent STs
pg_trickle.parallel_refresh_mode = 'on'
pg_trickle.max_concurrent_refreshes = 4

# Lean merge
pg_trickle.merge_planner_hints = true
pg_trickle.merge_work_mem_mb = 128           # more memory = fewer disk sorts
pg_trickle.cleanup_use_truncate = true
pg_trickle.use_prepared_statements = true

# Guardrails
pg_trickle.auto_backoff = true               # prevent CPU runaway
pg_trickle.fuse_default_ceiling = 0          # disabled — latency over safety
pg_trickle.block_source_ddl = true
```

### High-Throughput Profile

**Goal:** Maximize rows-per-second processed across many stream tables under
heavy write load. Accepts slightly higher latency in exchange for better
batching and resource efficiency.

```ini
# Batched scheduling
pg_trickle.scheduler_interval_ms = 2000      # 2-second poll interval
pg_trickle.min_schedule_seconds = 2
pg_trickle.default_schedule_seconds = 5

# Heavy parallelism
pg_trickle.parallel_refresh_mode = 'on'
pg_trickle.max_concurrent_refreshes = 8
pg_trickle.max_dynamic_refresh_workers = 8

# Aggressive performance
pg_trickle.merge_planner_hints = true
pg_trickle.merge_work_mem_mb = 256           # large work_mem for big deltas
pg_trickle.merge_seqscan_threshold = 0.01    # allow seq scans for >1% changes
pg_trickle.cleanup_use_truncate = true
pg_trickle.use_prepared_statements = true
pg_trickle.auto_backoff = true
pg_trickle.buffer_partitioning = 'auto'      # O(1) cleanup for hot buffers

# Safety for bulk workloads
pg_trickle.fuse_default_ceiling = 500000     # pause on >500K changes
pg_trickle.differential_max_change_ratio = 0.25  # FULL fallback at 25%
pg_trickle.block_source_ddl = true
```

### Resource-Constrained Profile

**Goal:** Minimize CPU and memory footprint for small instances, shared
hosting, or development environments. Accepts higher latency and slower
throughput.

```ini
# Poll-based scheduling (conservative)
pg_trickle.scheduler_interval_ms = 5000      # 5-second poll

# Conservative scheduling
pg_trickle.min_schedule_seconds = 5
pg_trickle.default_schedule_seconds = 10

# Minimal parallelism
pg_trickle.parallel_refresh_mode = 'off'     # single-threaded refresh
pg_trickle.max_concurrent_refreshes = 1
pg_trickle.max_dynamic_refresh_workers = 1

# Conservative memory
pg_trickle.merge_work_mem_mb = 32
pg_trickle.merge_planner_hints = true
pg_trickle.cleanup_use_truncate = true

# Tight guardrails
pg_trickle.auto_backoff = true
pg_trickle.fuse_default_ceiling = 100000
pg_trickle.differential_max_change_ratio = 0.10
pg_trickle.block_source_ddl = true
pg_trickle.buffer_alert_threshold = 500000
```

---

## Complete postgresql.conf Example

```ini
# Required
shared_preload_libraries = 'pg_trickle'

# Essential
pg_trickle.enabled = true
pg_trickle.cdc_mode = 'auto'
pg_trickle.scheduler_interval_ms = 1000
pg_trickle.min_schedule_seconds = 1
pg_trickle.default_schedule_seconds = 1
pg_trickle.max_consecutive_errors = 3

# WAL CDC
pg_trickle.wal_transition_timeout = 300
pg_trickle.slot_lag_warning_threshold_mb = 100
pg_trickle.slot_lag_critical_threshold_mb = 1024

# Refresh performance
pg_trickle.differential_max_change_ratio = 0.15
pg_trickle.merge_planner_hints = true
pg_trickle.merge_work_mem_mb = 64
pg_trickle.cleanup_use_truncate = true
pg_trickle.use_prepared_statements = true
pg_trickle.user_triggers = 'auto'

# Guardrails & limits
pg_trickle.block_source_ddl = false
pg_trickle.buffer_alert_threshold = 1000000
pg_trickle.compact_threshold = 100000
pg_trickle.buffer_partitioning = 'off'
pg_trickle.max_grouping_set_branches = 64
pg_trickle.max_parse_depth = 64
pg_trickle.ivm_topk_max_limit = 1000
pg_trickle.ivm_recursive_max_depth = 100

# Circular dependencies
pg_trickle.allow_circular = false                # master switch
pg_trickle.max_fixpoint_iterations = 100         # convergence limit

# Parallel refresh (default 'on')
pg_trickle.parallel_refresh_mode = 'on'         # 'off' | 'dry_run' | 'on'
pg_trickle.max_dynamic_refresh_workers = 4       # cluster-wide worker cap
pg_trickle.max_concurrent_refreshes = 4          # per-database dispatch cap
pg_trickle.max_parallel_workers = 0              # user-facing parallel cap (0 = use automatic sizing)

# Predictive cost model
pg_trickle.prediction_window = 60               # minutes of history for regression
pg_trickle.prediction_ratio = 1.5               # diff/full cost ratio threshold
pg_trickle.prediction_min_samples = 5           # minimum samples before model activates

# DVM scaling & diagnostics
pg_trickle.log_delta_sql = false                # log delta SQL at DEBUG1 (diagnostic only)
pg_trickle.delta_work_mem = 0                   # work_mem MB for delta execution (0 = inherit)
pg_trickle.delta_enable_nestloop = true         # allow nested-loop joins in delta SQL
pg_trickle.analyze_before_delta = true          # ANALYZE change buffers before delta SQL
pg_trickle.max_change_buffer_alert_rows = 0     # change buffer overflow alert threshold (0 = off)
pg_trickle.diff_output_format = 'split'         # 'split' (DI-2 pairs) | 'merged' (compat)

# Scheduler scalability
pg_trickle.worker_pool_size = 0                 # 0 = spawn-per-task; >0 = persistent pool
pg_trickle.template_cache_max_entries = 0       # 0 = unbounded

# Operability & observability
pg_trickle.metrics_port = 0                     # 0 = disabled; set per-database
pg_trickle.frontier_holdback_mode = 'xmin'      # xmin | none | lsn:<N>
pg_trickle.frontier_holdback_warn_seconds = 300 # warn after 5 min of blocked frontier
pg_trickle.publication_lag_warn_bytes = 0       # 0 = disabled

# Transactional outbox
pg_trickle.outbox_enabled = true
pg_trickle.outbox_retention_hours = 24
pg_trickle.outbox_inline_threshold_rows = 10000
pg_trickle.outbox_skip_empty_delta = true
pg_trickle.outbox_force_retention = false
pg_trickle.consumer_dead_threshold_hours = 24
pg_trickle.consumer_cleanup_enabled = true

# Transactional inbox
pg_trickle.inbox_enabled = true
pg_trickle.inbox_processed_retention_hours = 72
pg_trickle.inbox_dlq_retention_hours = 0       # 0 = keep forever
pg_trickle.inbox_dlq_alert_max_per_refresh = 10

# Citus distributed tables
pg_trickle.citus_st_lock_lease_ms = 60000      # lease duration for cross-node coordination
pg_trickle.citus_worker_retry_ticks = 5        # failures before WARNING in citus_status

# Advanced / internal
pg_trickle.change_buffer_schema = 'pgtrickle_changes'
pg_trickle.foreign_table_polling = false
```

---

## Runtime Configuration

All GUC variables can be changed at runtime by a superuser:

```sql
-- View current settings
SHOW pg_trickle.enabled;
SHOW pg_trickle.parallel_refresh_mode;

-- Enable parallel refresh for current session
SET pg_trickle.parallel_refresh_mode = 'on';

-- Change persistently (requires reload)
ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 500;
SELECT pg_reload_conf();
```

---

## Further Reading

- [Installation](installation.md) — Installation and initial configuration
- [ARCHITECTURE.md](ARCHITECTURE.md) — System architecture overview
- [SQL_REFERENCE.md](SQL_REFERENCE.md) — Complete function reference

---

## Appendix: Deprecated / Compatibility GUCs

The GUCs listed below are **deprecated** and retained only for backward
compatibility (to allow rolling upgrades without configuration errors). They
have no effect on extension behaviour and should be removed from new deployments.

### pg_trickle.event_driven_wake

> ⚠️ **Removed** — This GUC has been fully removed from the
> extension. If it appears in `postgresql.conf` after upgrading,
> PostgreSQL will emit an "unrecognized configuration parameter" warning at
> startup. Remove it to suppress the warning.
>
> **Migration:** Remove `pg_trickle.event_driven_wake` from `postgresql.conf`
> and any `ALTER SYSTEM` settings. No replacement is needed — the scheduler
> always uses efficient latch-based polling.

### pg_trickle.wake_debounce_ms

> ⚠️ **Removed** — This GUC has been fully removed together with
> `event_driven_wake`. Remove it from `postgresql.conf` to avoid an
> "unrecognized configuration parameter" warning at startup.
>
> **Migration:** Remove `pg_trickle.wake_debounce_ms` from `postgresql.conf`.
> No replacement is needed.

### pg_trickle.merge_planner_hints

> ⚠️ **Deprecated** — accepted for backwards compatibility but has no effect.
> Will be removed in a future major version.

### pg_trickle.user_triggers (value `'on'`)

> ⚠️ **Deprecated** — The value `'on'` is accepted as a deprecated alias for
> `'auto'` and has no distinct behaviour. Use `'auto'` (default) or `'off'`
> instead. The `'on'` alias will be removed in a future major version.
