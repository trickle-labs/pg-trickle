//! Scheduler, worker, dispatch, and refresh-strategy GUCs.

use pgrx::guc::*;

/// Scheduler wake interval in milliseconds.
///
/// Default: 1,000 ms (1 s). This provides sub-second refresh latency while
/// spending negligible CPU when all stream tables are idle. Raise to reduce
/// scheduler overhead on heavily loaded clusters; lower only if sub-100 ms
/// refresh latency is required and the cluster has spare CPU.
pub static PGS_SCHEDULER_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(1000);

/// Minimum allowed schedule in seconds.
///
/// Default: 1 s. Prevents runaway re-scheduling below the scheduler tick
/// interval.  Stream tables with CALCULATED schedules will never refresh
/// more frequently than once per second regardless of downstream demand.
pub static PGS_MIN_SCHEDULE_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(1);

/// Default effective schedule (in seconds) for isolated CALCULATED stream tables
/// that have no downstream dependents.
///
/// Default: 1 s. Isolated stream tables inherit their effective schedule from
/// downstream dependents; this value is used as a fallback when there are none.
/// Raise to reduce scheduler churn on workloads with many independent STs.
pub static PGS_DEFAULT_SCHEDULE_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(1);

/// Maximum consecutive errors before auto-suspending a stream table.
///
/// Default: 3. Three strikes gives a transient failure (e.g., a momentary
/// source lock conflict) a chance to self-recover without filling logs,
/// while reliably catching permanent failures before they cascade. Raise
/// on high-churn clusters where occasional lock timeouts are expected.
pub static PGS_MAX_CONSECUTIVE_ERRORS: GucSetting<i32> = GucSetting::<i32>::new(3);

/// Maximum change-to-table ratio before falling back to FULL refresh.
///
/// When the number of pending change buffer rows exceeds this fraction of
/// the source table's estimated row count, DIFFERENTIAL refresh automatically
/// falls back to FULL refresh to avoid the JSONB/window-function overhead
/// that makes DIFFERENTIAL slower than FULL at high change rates.
///
/// Set to 0.0 to disable adaptive fallback (always use DIFFERENTIAL).
/// Set to 1.0 to always fall back (effectively forcing FULL mode).
pub static PGS_DIFFERENTIAL_MAX_CHANGE_RATIO: GucSetting<f64> = GucSetting::<f64>::new(0.15);

/// PH-E1: Maximum estimated delta result rows before falling back to FULL refresh.
///
/// Before executing the MERGE, the refresh executor runs a capped
/// `SELECT count(*) FROM (delta_query LIMIT N+1)` to estimate the output
/// cardinality. If the count reaches this limit, a NOTICE is emitted and
/// the refresh downgrades to FULL to avoid OOM or excessive temp-file spills.
///
/// Set to 0 to disable the estimation check (default).
/// Recommended range: 50_000–500_000 depending on available memory.
pub static PGS_MAX_DELTA_ESTIMATE_ROWS: GucSetting<i32> = GucSetting::<i32>::new(0);

// Note: PGS_MERGE_SEQSCAN_THRESHOLD (P3-4) lives in dvm.rs; re-exported via pub use.

/// P3-5: Automatic schedule backoff for falling-behind stream tables.
///
/// When enabled and a stream table's refresh duration exceeds 80% of its
/// schedule interval (the falling-behind threshold), the scheduler doubles
/// the effective interval on each consecutive falling-behind cycle. The
/// backoff factor resets to 1.0 on the first on-time cycle.
///
/// This prevents CPU runaway when a stream table's refresh cost exceeds
/// its schedule budget and an operator is not available to respond manually.
pub static PGS_AUTO_BACKOFF: GucSetting<bool> = GucSetting::<bool>::new(true);

/// G-7: Enable tiered refresh scheduling (Hot/Warm/Cold/Frozen).
///
/// When enabled, per-ST `refresh_tier` controls the effective schedule
/// multiplier. Hot (1×), Warm (2×), Cold (10×), Frozen (skip entirely).
/// User-set via `ALTER STREAM TABLE ... SET (tier = 'warm')`.
/// Default tier for new STs is Hot (no change in behavior).
///
/// Default changed to `true` in v0.12.0 (PERF-3) — prevents large
/// deployments from wasting CPU refreshing cold STs at full speed.
pub static PGS_TIERED_SCHEDULING: GucSetting<bool> = GucSetting::<bool>::new(true);

/// CSS1: Cap CDC consumption to the WAL LSN captured at scheduler tick start.
///
/// When enabled (default), each scheduler tick calls `pg_current_wal_lsn()`
/// at its start to obtain a *tick watermark*. Every refresh within that tick
/// is prevented from consuming WAL changes beyond that watermark, ensuring
/// all stream tables in the same tick share the same consistent LSN view.
///
/// Disable only if you need stream tables to always advance to the very
/// latest available LSN regardless of cross-source consistency.
pub static PGS_TICK_WATERMARK_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);

/// CYC-4: Maximum iterations per SCC before declaring non-convergence.
///
/// When stream tables form a cyclic dependency (circular reference),
/// the scheduler iterates to a fixed point. If convergence is not
/// reached within this many iterations, all members of the SCC are
/// marked as ERROR.
pub static PGS_MAX_FIXPOINT_ITERATIONS: GucSetting<i32> = GucSetting::<i32>::new(100);

/// CYC-4: Master switch for circular dependency support.
///
/// When `false` (default), cycle detection rejects any stream table
/// creation that would introduce a cycle in the dependency graph.
/// When `true`, monotone cycles (those containing only safe operators)
/// are allowed and scheduled with fixed-point iteration.
pub static PGS_ALLOW_CIRCULAR: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Cluster-wide cap on concurrently active pg_trickle dynamic refresh workers.
///
/// This is distinct from `pg_trickle.max_concurrent_refreshes`, which is the
/// per-database dispatch cap. This GUC prevents multiple database coordinators
/// from overcommitting the shared PostgreSQL `max_worker_processes` budget.
pub static PGS_MAX_DYNAMIC_REFRESH_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(4);

/// C3-1: Per-database dynamic refresh worker quota.
///
/// When > 0, each per-database scheduler limits itself to this many
/// concurrently active dynamic refresh workers drawn from the cluster-wide
/// `max_dynamic_refresh_workers` pool. This prevents a single busy database
/// from starving other databases in multi-tenant clusters.
///
/// **Burst capacity:** when the cluster has spare capacity (active workers
/// < 80% of `max_dynamic_refresh_workers`), a database may temporarily
/// exceed its quota by up to 50% to absorb sudden backlogs. Burst is
/// reclaimed automatically within 1 scheduler cycle once global load rises.
///
/// **Priority dispatch:** within each dispatch tick, IMMEDIATE-trigger
/// closures are dispatched before other units, followed by atomic groups,
/// singletons, and cyclic SCCs — ensuring transactional consistency
/// requirements are always satisfied first.
///
/// Set to 0 (default) to disable per-database quotas — all databases share
/// `max_dynamic_refresh_workers` on a first-come-first-served basis,
/// bounded per coordinator by `max_concurrent_refreshes`.
pub static PGS_PER_DATABASE_WORKER_QUOTA: GucSetting<i32> = GucSetting::<i32>::new(0);

/// Parallel refresh mode — controls whether the scheduler dispatches
/// refresh work to dynamic background workers.
///
/// - `"on"` (default as of v0.11.0): Enable true parallel refresh via
///   dynamic workers. The feature has been stable since v0.4.0.
/// - `"off"`: Sequential refresh (pre-v0.11.0 default).
/// - `"dry_run"`: Compute execution units and log dispatch decisions,
///   but execute inline (no actual workers spawned).
pub static PGS_PARALLEL_REFRESH_MODE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"on"));

/// SCAL-5: Persistent worker pool size.
///
/// When set to > 0, the scheduler maintains a pool of persistent background
/// workers that loop on a shmem work queue instead of being registered and
/// deregistered on each refresh. This eliminates the ~2 ms per-worker spawn
/// cost at high task rates.
///
/// Set to 0 (default) to use the existing spawn-per-task model.
/// Recommended range: 2–8 for workloads with many short refreshes.
pub static PGS_WORKER_POOL_SIZE: GucSetting<i32> = GucSetting::<i32>::new(0);

/// PRED-1: Prediction window in minutes for the linear regression forecaster.
///
/// The forecaster fits `duration_ms ~ delta_rows` over this many minutes of
/// `pgt_refresh_history` data per stream table.
pub static PGS_PREDICTION_WINDOW: GucSetting<i32> = GucSetting::<i32>::new(60);

/// PRED-2: Prediction ratio threshold for pre-emptive FULL switch.
///
/// When `predicted_diff_ms > last_full_ms × prediction_ratio`, the
/// scheduler overrides the strategy to FULL refresh.
pub static PGS_PREDICTION_RATIO: GucSetting<f64> = GucSetting::<f64>::new(1.5);

/// PRED-3: Minimum number of history samples before the predictor is active.
///
/// When fewer than this many data points exist, the predictor falls back to
/// the existing fixed-threshold logic.
pub static PGS_PREDICTION_MIN_SAMPLES: GucSetting<i32> = GucSetting::<i32>::new(5);

/// SCAL-1 (v0.31.0): Number of consecutive refresh cycles a change buffer
/// must exceed `pg_trickle.buffer_alert_threshold` before a
/// `change_buffer_backpressure` alert is emitted.
///
/// A value of 1 fires on the first oversized cycle. Higher values suppress
/// transient spikes. Set to 0 to disable back-pressure alerting.
///
/// Default: 3 cycles.
pub static PGS_BACKPRESSURE_CONSECUTIVE_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(3);

/// B-4: Refresh strategy override.
///
/// Controls the FULL vs. DIFFERENTIAL decision for all stream tables:
/// - `"auto"` (default): Use the adaptive cost-based heuristic that
///   considers `differential_max_change_ratio`, per-ST `auto_threshold`,
///   refresh history, and spill detection to pick the optimal strategy.
/// - `"differential"`: Always use DIFFERENTIAL refresh (skip the adaptive
///   threshold check). Useful when operators know their workload has low
///   change rates and want to avoid any overhead from the ratio check.
/// - `"full"`: Always use FULL refresh. Useful for debugging or when
///   differential refresh is known to be slower for a specific workload.
///
/// This GUC is a cluster-wide override. Per-ST `refresh_mode` in the
/// catalog takes precedence: if a stream table is configured as
/// `refresh_mode = 'FULL'`, it will always use FULL regardless of this GUC.
/// This GUC only affects stream tables with `refresh_mode = 'DIFFERENTIAL'`.
pub static PGS_REFRESH_STRATEGY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"auto"));

/// B-4: Cost-model safety margin for the FULL vs. DIFFERENTIAL decision.
///
/// When `refresh_strategy = 'auto'`, the cost model compares the estimated
/// DIFFERENTIAL cost against `estimated_full_cost × safety_margin`.
/// A value below 1.0 biases toward DIFFERENTIAL (which has lower lock
/// contention), while a value above 1.0 biases toward FULL.
///
/// Default 0.8 — DIFFERENTIAL is chosen unless it's estimated to cost
/// more than 80% of FULL.
pub static PGS_COST_MODEL_SAFETY_MARGIN: GucSetting<f64> = GucSetting::<f64>::new(0.8);

/// B-4: Refresh strategy enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshStrategy {
    /// Adaptive cost-based heuristic (existing behavior).
    Auto,
    /// Always use DIFFERENTIAL (skip adaptive fallback to FULL).
    Differential,
    /// Always fall back to FULL refresh.
    Full,
}

impl RefreshStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            RefreshStrategy::Auto => "auto",
            RefreshStrategy::Differential => "differential",
            RefreshStrategy::Full => "full",
        }
    }
}

pub(crate) fn normalize_refresh_strategy(value: Option<String>) -> RefreshStrategy {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("differential") => RefreshStrategy::Differential,
        Some("full") => RefreshStrategy::Full,
        _ => RefreshStrategy::Auto,
    }
}

/// Parallel refresh operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelRefreshMode {
    /// Sequential execution — current behavior (default).
    Off,
    /// Compute execution units and log dispatch decisions, but execute inline.
    DryRun,
    /// Enable true parallel refresh via dynamic background workers.
    On,
}

impl ParallelRefreshMode {
    pub fn as_str(self) -> &'static str {
        match self {
            ParallelRefreshMode::Off => "off",
            ParallelRefreshMode::DryRun => "dry_run",
            ParallelRefreshMode::On => "on",
        }
    }
}

impl std::fmt::Display for ParallelRefreshMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub(crate) fn normalize_parallel_refresh_mode(value: Option<String>) -> ParallelRefreshMode {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("dry_run") => ParallelRefreshMode::DryRun,
        Some("off") => ParallelRefreshMode::Off,
        // Default to On for None (unset) and any unrecognised value.
        // On has been the stable parallel path since v0.4.0.
        _ => ParallelRefreshMode::On,
    }
}

/// PAR-2: Maximum parallel refresh workers for the coordinator/worker pool.
///
/// When > 0, the per-database scheduler dispatches independent same-level
/// stream tables to a pool of dynamic background workers for concurrent
/// refresh. At most `max_parallel_workers` refreshes execute simultaneously.
///
/// Default 0 = serial mode (existing behavior preserved).
pub static PGS_MAX_PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(0);

/// A46-10 (v0.45.0): Enable lag-aware cross-database worker quota adjustment.
///
/// When `on`, the per-database worker quota is boosted proportionally to the
/// maximum observed stream table lag in the current database. Databases with
/// more stale stream tables receive a higher quota, allowing lag to recover
/// faster at the cost of temporarily reducing other databases' entitlements.
///
/// Off by default — use static quotas. Enable when running many databases
/// with heterogeneous workloads where some routinely fall behind.
pub static PGS_LAG_AWARE_SCHEDULING: GucSetting<bool> = GucSetting::<bool>::new(false);

/// A44-4 (v0.43.0): Effective capacity of the shared cost-model cache.
///
/// The physical cache is allocated at startup with 256 slots (compile-time
/// constant). This GUC controls the effective number of slots used at runtime
/// by adjusting the modulo divisor in slot-address computation. Reducing it
/// concentrates the cache on low pgt_id values and reduces hash-collision
/// probability for small deployments.
///
/// Requires `shared_preload_libraries` restart to take effect on the
/// allocation side; a runtime change adjusts only the effective divisor.
///
/// Default: 256. Range: 16–256.
pub static PGS_COST_CACHE_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(256);

/// A46-7 (v0.45.0): Effective capacity of the invalidation ring buffer.
///
/// Controls how many stream-table pgt_ids can be queued for incremental DAG
/// re-evaluation between scheduler ticks. When the ring overflows, the scheduler
/// falls back to a full O(V+E) DAG rebuild. The compile-time maximum is 4,096;
/// this GUC can be tuned up to that limit for clusters with high DDL rates.
///
/// Must be set in `postgresql.conf` or via `ALTER SYSTEM` before loading
/// `shared_preload_libraries` (preload-time). Runtime changes have no effect
/// until restart.
///
/// Default: 1024. Range: 1–4096. Raised from 128/1024 to 1024/4096 in v0.55.0.
pub static PGS_INVALIDATION_RING_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(1024);

/// VP-2 (v0.47.0): Default drift threshold for drift-triggered REINDEX.
///
/// When a stream table has `post_refresh_action = 'reindex_if_drift'` and no
/// per-table `reindex_drift_threshold` is set, this global GUC is used.
///
/// A drift of 0.20 means "REINDEX when 20% of estimated rows have changed since
/// the last REINDEX".
///
/// Default: 0.20. Range: 0.01–1.0.
pub static PGS_REINDEX_DRIFT_THRESHOLD: GucSetting<f64> = GucSetting::<f64>::new(0.20);

/// PERF-2 (v0.63.0): Enable CTE-fused multi-node refresh.
///
/// When `true` (default), the scheduler composes the delta SQL for multiple
/// DIFFERENTIAL-mode stream tables in the same tick into a single
/// `WITH … MERGE; MERGE; …` CTE chain.  This reduces the number of
/// planner invocations, executor setups, and round-trips for multi-node DAGs.
///
/// Disable if a specific DAG shape causes unexpected planner behaviour.
/// Please file an issue with the problematic query if you need to disable
/// fusion — we want to fix planner interaction problems rather than leave
/// them as permanent opt-outs.
pub static PGS_ENABLE_FUSED_REFRESH: GucSetting<bool> = GucSetting::<bool>::new(true);

/// PERF-2 (v0.63.0): Maximum estimated delta rows for a node to be
/// fusion-eligible.
///
/// Nodes whose estimated pending change count exceeds this threshold are
/// excluded from the fused CTE chain and refreshed sequentially.  Very
/// large deltas produce very large CTEs; the planner may choose a worse
/// plan for the composed statement than for two independent statements.
///
/// Default: 500 000. Set to 0 to disable the cardinality gate (always fuse).
pub static PGS_FUSED_REFRESH_MAX_DELTA_ROWS: GucSetting<i32> = GucSetting::<i32>::new(500_000);

/// PERF-1 (v0.62.0): Deduplicate change-buffer scans across all stream tables
/// that share the same source within a single scheduler tick.
///
/// When `true` (default), the scheduler builds a per-tick cache of source OIDs
/// that have pending changes and uses it for every `has_table_source_changes`
/// call within that tick. This eliminates redundant SPI round-trips when many
/// stream tables share the same source table.
///
/// Disable only if the shared cache is producing incorrect change-detection
/// results (should not occur in practice).
pub static PGS_ENABLE_CHANGE_BUFFER_FANOUT: GucSetting<bool> = GucSetting::<bool>::new(true);

/// API-1/2 (v0.62.0): Maximum seconds to wait for in-flight refreshes to drain
/// when `pgtrickle.pause_scheduler(nodes)` is called.
///
/// After setting the pause flag for a node, `pause_scheduler` polls the refresh
/// status every 100 ms. If the node has not stopped refreshing within this many
/// seconds, the call returns without waiting further and logs a WARNING.
///
/// Default: 30 seconds. Range: 1–3600.
pub static PGS_SCHEDULER_DRAIN_TIMEOUT: GucSetting<i32> = GucSetting::<i32>::new(30);

/// PERF-1 (v0.31.0): Coalesce change-buffer scans across stream tables that
/// share the same source table within a single scheduler tick.
///
/// When true (default), the scheduler groups ready stream tables by their
/// source OIDs before the has-changes check and issues a single batched
/// EXISTS query per unique source table instead of one per ST. Expected
/// throughput improvement: 10–30% for deployments with many STs sharing
/// common source tables.
///
/// Disable if the batched query plan is unexpectedly slow (rare).
pub static PGS_ADAPTIVE_BATCH_COALESCING: GucSetting<bool> = GucSetting::<bool>::new(true);

/// PERF-2 (v0.31.0): Automatically select the `merge_strategy` for each
/// differential refresh based on the EXPLAIN plan instead of relying on the
/// fixed `pg_trickle.merge_strategy` GUC.
///
/// When true, after each differential refresh the scheduler inspects the
/// estimated cost ratio between the MERGE and DELETE+INSERT paths using
/// `EXPLAIN (FORMAT JSON)`. If the cheaper path differs from the current
/// strategy, the per-ST preference is updated for the next cycle.
///
/// Default `false` — the fixed `merge_strategy` GUC governs.
pub static PGS_ADAPTIVE_MERGE_STRATEGY: GucSetting<bool> = GucSetting::<bool>::new(false);

/// PLAN-1/PLAN-4 (v0.27.0): Minimum number of cost-model observations required
/// before `recommend_schedule()` returns a non-trivial recommendation.
///
/// When fewer samples are available, `confidence` is returned as 0.0 and
/// the recommendation fields are NULL or conservative defaults.
pub static PGS_SCHEDULE_RECOMMENDATION_MIN_SAMPLES: GucSetting<i32> = GucSetting::<i32>::new(20);

/// PLAN-3/PLAN-4 (v0.27.0): Minimum interval (seconds) between consecutive
/// `predicted_sla_breach` alerts for the same stream table.
///
/// Prevents alert spam when the cost model consistently predicts SLA breach.
/// Set to 0 to disable debouncing (fire on every tick).
pub static PGS_SCHEDULE_ALERT_COOLDOWN_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(300);

/// COORD-2 (v0.33.0): Duration in milliseconds for `pgt_st_locks` lease entries
/// acquired by the scheduler before coordinating a distributed stream table refresh.
///
/// Must be ≥ `pg_ripple.merge_fence_timeout_ms` (default 30 000) so that a
/// scheduling lease does not expire while a pg_ripple merge cycle is still in
/// progress.  Set to 0 to disable catalog-based scheduling locks (not recommended
/// for multi-worker Citus deployments).
///
/// Default: 60 000 ms (60 seconds).
pub static PGS_CITUS_ST_LOCK_LEASE_MS: GucSetting<i32> = GucSetting::<i32>::new(60_000);

/// COORD-15 (v0.34.0): Number of consecutive per-worker poll failures before
/// the stream table is flagged in `citus_status` for operator attention.
///
/// When `poll_worker_slot_changes()` fails for a worker on this many consecutive
/// scheduler ticks, the failure is surfaced as an alert in the `citus_status`
/// view.  Refreshes against healthy workers continue uninterrupted.
///
/// Set to 0 to disable the alert (not recommended for production).
///
/// Default: 5 ticks.
pub static PGS_CITUS_WORKER_RETRY_TICKS: GucSetting<i32> = GucSetting::<i32>::new(5);

/// A08 (v0.35.0): When `true`, overrides per-ST `refresh_mode` and forces
/// every stream table to use FULL refresh for the duration the GUC is set.
///
/// Useful for SRE diagnosis when a cluster-wide `refresh_strategy = 'full'`
/// still has DIFFERENTIAL STs due to explicit per-ST row values. Set to
/// `false` (default) to restore normal per-ST scheduling.
pub static PGS_FORCE_FULL_REFRESH: GucSetting<bool> = GucSetting::<bool>::new(false);

/// UX-GUC / CORR-SUB (v0.35.0): Debounce interval in milliseconds for NOTIFY
/// coalescing in the reactive subscription API.
///
/// When `pgtrickle.subscribe()` is active and the refresh interval is shorter
/// than the LISTEN client's poll loop, successive NOTIFY calls for the same
/// stream table within this window are coalesced into a single emission.
/// Set to 0 to disable coalescing (emit on every non-empty refresh).
///
/// Default: 250 ms.
pub static PGS_NOTIFY_COALESCE_MS: GucSetting<i32> = GucSetting::<i32>::new(250);

/// A10 (v0.35.0): Interval in seconds between history pruner sweeps.
///
/// The history pruner deletes rows from `pgtrickle.pgt_refresh_history`
/// older than `history_retention_days` in batches of 10,000 rows to
/// limit lock contention. Set to 0 to use the legacy behaviour (prune
/// once per `HISTORY_CLEANUP_INTERVAL_MS`).
///
/// Default: 60 seconds.
pub static PGS_HISTORY_PRUNE_INTERVAL_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(60);

/// A35 (v0.36.0): Drain timeout in seconds for `pgtrickle.drain()`.
///
/// Maximum seconds to wait for in-flight refreshes to complete during a drain
/// operation. When the timeout is exceeded, `drain()` returns `false` to
/// indicate that not all refreshes completed before the deadline.
///
/// Default: 60 seconds.
pub static PGS_DRAIN_TIMEOUT: GucSetting<i32> = GucSetting::<i32>::new(60);

/// FUSE-5: Global default change-count ceiling for the fuse circuit breaker.
///
/// When a stream table's fuse_mode is 'on' or 'auto' and no per-ST
/// `fuse_ceiling` is configured, this global ceiling is used. If the total
/// pending change buffer rows across all sources of an ST exceed this value,
/// the fuse blows and the ST is suspended.
///
/// Set to 0 to disable the global default ceiling (per-ST ceiling only).
pub static PGS_FUSE_DEFAULT_CEILING: GucSetting<i32> = GucSetting::<i32>::new(0);

/// AUTO-IDX: Automatic index creation on stream tables.
///
/// When enabled, `create_stream_table()` automatically creates indexes on
/// GROUP BY keys, DISTINCT columns, and adds INCLUDE clauses to the
/// `__pgt_row_id` index for stream tables with ≤ 8 output columns.
pub static PGS_AUTO_INDEX: GucSetting<bool> = GucSetting::<bool>::new(true);

/// SCAL-1 (v0.30.0): When true, classify SPI error retryability by SQLSTATE code
/// instead of English message-text patterns.
///
/// The SQLSTATE-based classification is locale-safe: it works correctly regardless
/// of `lc_messages`. Flipped to `true` (default) in v0.31.0 after the validation
/// window. Set to `false` to revert to message-text pattern matching.
pub static PGS_USE_SQLSTATE_CLASSIFICATION: GucSetting<bool> = GucSetting::<bool>::new(true);

/// DVM-3 (v0.77.0): After each DIFFERENTIAL refresh, compare stream table row
/// count against a full recomputation and emit a WARNING on any discrepancy.
///
/// When `true`, after every successful differential MERGE the scheduler runs
/// `SELECT count(*) FROM (defining_query) AS __pgt_validate` and compares the
/// result against `SELECT count(*) FROM stream_table`.  A mismatch indicates
/// that the differential delta produced an incorrect result.
///
/// **Performance impact:** significant — runs the full defining query after
/// every differential refresh.  Enable only for debugging or in CI validation.
///
/// Default: `false`.
pub static PGS_VALIDATE_DELTA_INVARIANTS: GucSetting<bool> = GucSetting::<bool>::new(false);

/// D-3 (v0.79.0): TEST-MODE only. When set to a stream table name, the
/// scheduler simulates a retryable refresh failure for that table on every
/// tick (by directly calling `increment_errors`) instead of running the
/// actual refresh. Used by the D-3 E2E test to reliably trigger auto-
/// suspension without depending on PostgreSQL exception handling from user
/// triggers. Default: `None` (disabled).
///
/// Activate with: `ALTER SYSTEM SET pg_trickle.test_chaos_for_table = 'name'`
/// followed by `SELECT pg_reload_conf()`.
pub static PGS_TEST_CHAOS_FOR_TABLE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

// ── v0.81.0 GUC statics ───────────────────────────────────────────────────

/// QW-8 (v0.81.0): Enable OOM-triggered self-healing.
///
/// When `true`, a refresh error containing "out of memory" causes the scheduler
/// to reduce the effective `merge_work_mem_mb` for the affected stream table by
/// 25% on the next tick and retry. Reverts after 3 consecutive successes.
pub static PGS_SELF_HEAL_OOM: GucSetting<bool> = GucSetting::<bool>::new(true);

/// QW-8 (v0.81.0): Enable lock-timeout-triggered interval backoff.
///
/// When `true`, a refresh error containing "lock timeout" doubles the effective
/// refresh interval for the affected stream table (exponential backoff).
/// Reverts after 3 consecutive successes.
pub static PGS_SELF_HEAL_LOCK_TIMEOUT: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Register all scheduler-related GUC variables.
pub fn register_scheduler_gucs() {
    GucRegistry::define_int_guc(
        c"pg_trickle.scheduler_interval_ms",
        c"Scheduler wake interval in milliseconds.",
        c"Controls how frequently the background scheduler checks for STs that need refresh.",
        &PGS_SCHEDULER_INTERVAL_MS,
        100,     // min
        600_000, // max (DI-9: raised from 60s to 600s for long-running benchmarks)
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.min_schedule_seconds",
        c"Minimum allowed schedule in seconds.",
        c"Stream tables cannot specify a schedule smaller than this value.",
        &PGS_MIN_SCHEDULE_SECONDS,
        1,      // min
        86_400, // max (1 day)
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.default_schedule_seconds",
        c"Default effective schedule (seconds) for isolated CALCULATED stream tables.",
        c"When a CALCULATED stream table has no downstream dependents, this value \
           is used as its effective refresh interval. Distinct from min_schedule_seconds \
           which is the validation floor for duration-based schedules.",
        &PGS_DEFAULT_SCHEDULE_SECONDS,
        1,      // min
        86_400, // max (1 day)
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.max_consecutive_errors",
        c"Maximum consecutive errors before auto-suspend.",
        c"After this many consecutive refresh failures, the stream table is automatically suspended.",
        &PGS_MAX_CONSECUTIVE_ERRORS,
        1,    // min
        100,  // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        c"pg_trickle.differential_max_change_ratio",
        c"Max change ratio before falling back to FULL refresh.",
        c"When pending changes exceed this fraction of the source table size, DIFFERENTIAL refresh falls back to FULL. Set to 0.0 to disable.",
        &PGS_DIFFERENTIAL_MAX_CHANGE_RATIO,
        0.0,  // min
        1.0,  // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // B-4: Refresh strategy override.
    GucRegistry::define_string_guc(
        c"pg_trickle.refresh_strategy",
        c"Refresh strategy override: auto, differential, or full.",
        c"Controls the FULL vs. DIFFERENTIAL decision for all stream tables. \
           'auto' (default) uses the adaptive cost-based heuristic. \
           'differential' always uses DIFFERENTIAL (skips ratio check). \
           'full' always uses FULL refresh. Per-ST refresh_mode takes precedence.",
        &PGS_REFRESH_STRATEGY,
        GucContext::Suset,
        GucFlags::default(),
    );

    // B-4: Cost-model safety margin.
    GucRegistry::define_float_guc(
        c"pg_trickle.cost_model_safety_margin",
        c"Safety margin for the cost-model FULL vs DIFFERENTIAL decision.",
        c"When refresh_strategy = 'auto', DIFFERENTIAL is chosen unless its \
           estimated cost exceeds estimated_full_cost × this margin. Values \
           below 1.0 bias toward DIFFERENTIAL (lower lock contention). \
           Default 0.8.",
        &PGS_COST_MODEL_SAFETY_MARGIN,
        0.1, // min
        2.0, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // PH-E1: Delta estimated output cardinality threshold.
    GucRegistry::define_int_guc(
        c"pg_trickle.max_delta_estimate_rows",
        c"Max estimated delta output rows before falling back to FULL (0 = disabled).",
        c"Before executing the MERGE, runs a capped COUNT on the delta subquery. \
           If the count reaches this limit, the refresh downgrades to FULL with a NOTICE \
           to prevent OOM or excessive temp-file spills from unexpectedly large deltas. \
           Set to 0 to disable the estimation check. Recommended: 50000–500000.",
        &PGS_MAX_DELTA_ESTIMATE_ROWS,
        0,          // min (0 = disabled)
        10_000_000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.auto_backoff",
        c"Automatically back off schedule for falling-behind stream tables (default on).",
        c"When enabled (the default), the scheduler doubles the effective interval \
           when a refresh takes more than 95% of the schedule window, capped at 8x. \
           Emits a WARNING when the factor changes. Resets on the first on-time cycle.",
        &PGS_AUTO_BACKOFF,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.tiered_scheduling",
        c"Enable tiered refresh scheduling (Hot/Warm/Cold/Frozen).",
        c"When enabled, per-ST refresh_tier controls the effective schedule \
           multiplier. Hot refreshes at configured interval, Warm at 2x, \
           Cold at 10x, Frozen skips entirely. Set per-ST tier via \
           ALTER STREAM TABLE ... SET (tier = 'warm'). Default is on \
           (changed in v0.12.0; set to off to restore pre-v0.12.0 behavior).",
        &PGS_TIERED_SCHEDULING,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.tick_watermark_enabled",
        c"Cap CDC consumption to the WAL LSN at scheduler tick start.",
        c"When on (default), each scheduler tick captures pg_current_wal_lsn() at its \
           start and prevents any refresh from consuming WAL changes beyond that LSN. \
           This bounds cross-source staleness without requiring user configuration. \
           Disable only if you need STs to always advance to the latest available LSN.",
        &PGS_TICK_WATERMARK_ENABLED,
        GucContext::Suset,
        GucFlags::default(),
    );

    // CYC-4: Circular dependency GUCs.
    GucRegistry::define_int_guc(
        c"pg_trickle.max_fixpoint_iterations",
        c"Maximum iterations per SCC before declaring non-convergence.",
        c"When circular stream table dependencies are iterated to a fixed point, \
           this limits the maximum number of iterations. If convergence is not \
           reached within this limit, all members of the SCC are marked ERROR. \
           Only meaningful when pg_trickle.allow_circular = true.",
        &PGS_MAX_FIXPOINT_ITERATIONS,
        1,      // min
        10_000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.allow_circular",
        c"Allow circular (cyclic) stream table dependencies.",
        c"When false (default), creating a stream table that would introduce a cycle \
           in the dependency graph is rejected. When true, monotone cycles \
           (containing only safe operators like joins, filters, and projections) \
           are allowed and refreshed via fixed-point iteration.",
        &PGS_ALLOW_CIRCULAR,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.max_dynamic_refresh_workers",
        c"Cluster-wide cap on pg_trickle dynamic refresh workers.",
        c"Limits the total number of concurrently active pg_trickle refresh workers \
           across all databases. Prevents overcommitting max_worker_processes.",
        &PGS_MAX_DYNAMIC_REFRESH_WORKERS,
        1,  // min
        64, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.per_database_worker_quota",
        c"Per-database dynamic refresh worker quota for multi-tenant isolation.",
        c"When > 0, limits each database's concurrent refresh workers to this count \
           from the shared cluster budget (max_dynamic_refresh_workers). Prevents one \
           busy database from starving others. Burst to 150% allowed when cluster has \
           spare capacity (active workers < 80% of max_dynamic_refresh_workers). \
           0 (default) disables per-DB quotas (first-come-first-served from pool). \
           Within each tick, IMMEDIATE closures are dispatched before other units.",
        &PGS_PER_DATABASE_WORKER_QUOTA,
        0,  // min: 0 (disabled)
        64, // max: matches max_dynamic_refresh_workers ceiling
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.parallel_refresh_mode",
        c"Parallel refresh mode: on (default), dry_run, or off.",
        c"'on' (default): enable true parallel refresh via dynamic background workers. \
           'dry_run': compute execution units and log dispatch decisions but execute inline. \
           'off': sequential refresh (pre-v0.11.0 default).",
        &PGS_PARALLEL_REFRESH_MODE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.worker_pool_size",
        c"SCAL-5: Persistent worker pool size (0 = spawn-per-task, default).",
        c"When > 0, the scheduler maintains a pool of this many persistent background \
           workers that loop on a shmem queue, eliminating ~2 ms per-worker spawn cost. \
           Set to 0 to use the existing spawn-per-task model.",
        &PGS_WORKER_POOL_SIZE,
        0,  // min
        64, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // PRED-1: Prediction window in minutes.
    GucRegistry::define_int_guc(
        c"pg_trickle.prediction_window",
        c"Prediction window in minutes for the linear regression forecaster.",
        c"The forecaster fits duration_ms ~ delta_rows over this many minutes of \
           pgt_refresh_history data per stream table.",
        &PGS_PREDICTION_WINDOW,
        5,    // min (5 minutes)
        1440, // max (24 hours)
        GucContext::Suset,
        GucFlags::default(),
    );

    // PRED-2: Prediction ratio threshold.
    GucRegistry::define_float_guc(
        c"pg_trickle.prediction_ratio",
        c"Prediction ratio threshold for pre-emptive FULL switch.",
        c"When predicted_diff_ms > last_full_ms × prediction_ratio, the scheduler \
           overrides the strategy to FULL refresh. Default 1.5.",
        &PGS_PREDICTION_RATIO,
        1.0,  // min
        10.0, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // PRED-3: Minimum number of history samples.
    GucRegistry::define_int_guc(
        c"pg_trickle.prediction_min_samples",
        c"Minimum samples before the predictive cost model activates (0 = disabled).",
        c"When fewer than this many data points exist for a stream table, the \
           predictor falls back to the existing fixed-threshold logic.",
        &PGS_PREDICTION_MIN_SAMPLES,
        0,    // min (0 = disabled)
        1000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.fuse_default_ceiling",
        c"Global default change-count ceiling for the fuse circuit breaker.",
        c"When a stream table has fuse_mode='on' or 'auto' and no per-ST fuse_ceiling, \
           this value is used. If pending changes exceed this count, the fuse blows \
           and the ST is suspended. Set to 0 to disable the global default.",
        &PGS_FUSE_DEFAULT_CEILING,
        0,             // min (disabled)
        2_000_000_000, // max (~2B rows)
        GucContext::Suset,
        GucFlags::default(),
    );

    // AUTO-IDX: Automatic index creation on stream tables.
    GucRegistry::define_bool_guc(
        c"pg_trickle.auto_index",
        c"Automatically create indexes on stream tables at creation time.",
        c"When true (default), create_stream_table() auto-creates indexes on GROUP BY keys, \
           DISTINCT columns, and adds INCLUDE clauses to the __pgt_row_id index for small \
           stream tables. Set to false to manage indexes manually.",
        &PGS_AUTO_INDEX,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.use_sqlstate_classification",
        c"SCAL-1: Use SQLSTATE codes for SPI error retry classification instead of message text.",
        c"When true, retry decisions use the numeric SQLSTATE code from pg_sys::ErrorData \
           rather than English message patterns. Locale-safe: works with any lc_messages. \
           Default false for v0.30.0 validation; will become true in v0.31.0.",
        &PGS_USE_SQLSTATE_CLASSIFICATION,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.adaptive_batch_coalescing",
        c"PERF-1: Coalesce change-buffer scans for STs sharing a source table.",
        c"When true (default), the scheduler groups ready stream tables by source OID \
           and issues one batched EXISTS check per unique source instead of one per ST. \
           Reduces SPI round-trips by up to N× for N stream tables sharing one source.",
        &PGS_ADAPTIVE_BATCH_COALESCING,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.adaptive_merge_strategy",
        c"PERF-2: Auto-select merge_strategy based on EXPLAIN plan after each refresh.",
        c"When true, after each differential refresh the scheduler inspects the EXPLAIN \
           cost ratio. If DELETE+INSERT is estimated to be cheaper than MERGE, the \
           per-ST strategy is switched for the next cycle. Default false — the \
           fixed pg_trickle.merge_strategy GUC governs.",
        &PGS_ADAPTIVE_MERGE_STRATEGY,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.backpressure_consecutive_limit",
        c"SCAL-1: Consecutive cycles above buffer_alert_threshold before emitting backpressure alert.",
        c"When a change buffer exceeds pg_trickle.buffer_alert_threshold for this many \
           consecutive refresh cycles, a change_buffer_backpressure alert is emitted on \
           the pg_trickle_alert NOTIFY channel. Set to 0 to disable. Default: 3.",
        &PGS_BACKPRESSURE_CONSECUTIVE_LIMIT,
        0,   // min (0 = disabled)
        100, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.schedule_recommendation_min_samples",
        c"PLAN-4: Minimum cost-model observations before recommend_schedule() returns a recommendation.",
        c"When fewer than this many refresh cycles have been recorded for a stream table, \
           recommend_schedule() returns confidence=0.0. Raise this for better accuracy; \
           lower it to get early recommendations.",
        &PGS_SCHEDULE_RECOMMENDATION_MIN_SAMPLES,
        1,    // min
        1000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.schedule_alert_cooldown_seconds",
        c"PLAN-3: Minimum seconds between consecutive predicted_sla_breach alerts for the same ST.",
        c"Debounces the spike-forecast alert so operators are not spammed when the cost model \
           consistently predicts an SLA breach. Set to 0 to disable debouncing.",
        &PGS_SCHEDULE_ALERT_COOLDOWN_SECONDS,
        0,      // min (0 = disabled)
        86_400, // max (1 day)
        GucContext::Suset,
        GucFlags::default(),
    );

    // COORD-2 (v0.33.0): Citus pgt_st_locks lease duration.
    pgrx::GucRegistry::define_int_guc(
        c"pg_trickle.citus_st_lock_lease_ms",
        c"COORD-2: Duration (ms) of pgt_st_locks lease for distributed refresh coordination. \
          Must be >= pg_ripple.merge_fence_timeout_ms to prevent lease expiry during a merge.",
        c"",
        &PGS_CITUS_ST_LOCK_LEASE_MS,
        0,       // min (0 = disabled)
        600_000, // max (10 minutes)
        GucContext::Suset,
        GucFlags::default(),
    );

    // COORD-15 (v0.34.0): Consecutive worker-poll failures before alerting in citus_status.
    pgrx::GucRegistry::define_int_guc(
        c"pg_trickle.citus_worker_retry_ticks",
        c"COORD-15: Consecutive worker-poll failures before flagging in citus_status. \
          0 = disabled.",
        c"",
        &PGS_CITUS_WORKER_RETRY_TICKS,
        0,   // min (0 = disabled)
        100, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // A08: Force-full-refresh override.
    GucRegistry::define_bool_guc(
        c"pg_trickle.force_full_refresh",
        c"A08: Force all stream tables to FULL refresh regardless of per-ST mode.",
        c"When true, overrides per-ST refresh_mode and forces every differential ST \
          to use FULL refresh. Useful for SRE diagnosis. Default false.",
        &PGS_FORCE_FULL_REFRESH,
        GucContext::Suset,
        GucFlags::default(),
    );

    // UX-GUC / CORR-SUB: Notify coalescing debounce interval.
    GucRegistry::define_int_guc(
        c"pg_trickle.notify_coalesce_ms",
        c"UX-GUC: Debounce window (ms) for NOTIFY coalescing in subscribe() API.",
        c"Successive NOTIFY emissions for the same stream table within this window \
          are coalesced into a single emission. 0 disables coalescing. Default 250.",
        &PGS_NOTIFY_COALESCE_MS,
        0,      // min (0 = disabled)
        60_000, // max (1 minute)
        GucContext::Suset,
        GucFlags::default(),
    );

    // A10: History pruner interval.
    GucRegistry::define_int_guc(
        c"pg_trickle.history_prune_interval_seconds",
        c"A10: Seconds between history pruner sweeps (0 = legacy mode).",
        c"The pruner deletes pgt_refresh_history rows older than history_retention_days \
          in batches of 10,000. 0 uses the legacy single-pass pruner.",
        &PGS_HISTORY_PRUNE_INTERVAL_SECONDS,
        0,      // min (0 = legacy)
        86_400, // max (1 day)
        GucContext::Suset,
        GucFlags::default(),
    );

    // A35: Drain timeout.
    GucRegistry::define_int_guc(
        c"pg_trickle.drain_timeout",
        c"A35: Maximum seconds to wait for in-flight refreshes during drain().",
        c"When pgtrickle.drain() is called, the scheduler stops accepting new cycles \
          and waits up to this many seconds for all in-flight refreshes to complete. \
          Returns false if not all refreshes complete before the deadline.",
        &PGS_DRAIN_TIMEOUT,
        1,     // min
        3_600, // max (1 hour)
        GucContext::Suset,
        GucFlags::default(),
    );

    // A44-4: Cost-cache capacity GUC.
    GucRegistry::define_int_guc(
        c"pg_trickle.cost_cache_capacity",
        c"A44-4: Effective number of slots in the shared cost-model cache.",
        c"The physical cache is allocated with 256 slots at startup. This GUC \
          controls the effective number of slots used at runtime by adjusting the \
          modulo divisor. Reducing it focuses the cache on low pgt_id values. \
          Requires restart to change physical allocation. Default: 256.",
        &PGS_COST_CACHE_CAPACITY,
        16,  // min
        256, // max (matches compile-time COST_CACHE_CAPACITY)
        GucContext::Suset,
        GucFlags::default(),
    );

    // A46-7: Invalidation ring capacity GUC.
    GucRegistry::define_int_guc(
        c"pg_trickle.invalidation_ring_capacity",
        c"A46-7: Effective capacity of the invalidation ring buffer.",
        c"Controls how many stream-table pgt_ids can be queued for incremental DAG \
          re-evaluation between scheduler ticks. When the ring overflows, the scheduler \
          falls back to a full O(V+E) DAG rebuild. Compile-time maximum is 4096. \
          Default: 1024. Range: 1-4096. Raised from 1024 max to 4096 in v0.55.0.",
        &PGS_INVALIDATION_RING_CAPACITY,
        1,    // min
        4096, // max (matches INVALIDATION_RING_MAX_CAPACITY in shmem.rs)
        GucContext::Sighup,
        GucFlags::default(),
    );

    // A46-10: Lag-aware cross-database scheduling.
    GucRegistry::define_bool_guc(
        c"pg_trickle.lag_aware_scheduling",
        c"A46-10: Enable lag-aware per-database worker quota adjustment.",
        c"When on, the per-database worker quota is boosted proportionally to observed \
          stream table lag, so databases with higher staleness receive more refresh capacity. \
          Off by default (use static quotas).",
        &PGS_LAG_AWARE_SCHEDULING,
        GucContext::Suset,
        GucFlags::default(),
    );

    // VP-2: Global default drift threshold for reindex_if_drift post-refresh action.
    GucRegistry::define_float_guc(
        c"pg_trickle.reindex_drift_threshold",
        c"VP-2: Default drift fraction for reindex_if_drift post-refresh action.",
        c"When a stream table has post_refresh_action='reindex_if_drift' and no per-table \
          reindex_drift_threshold is set, this global value is used. \
          A value of 0.20 means REINDEX when 20% of estimated rows have changed.",
        &PGS_REINDEX_DRIFT_THRESHOLD,
        0.01,
        1.0,
        GucContext::Suset,
        GucFlags::default(),
    );

    // PERF-1: Change-buffer fan-out deduplication.
    GucRegistry::define_bool_guc(
        c"pg_trickle.enable_change_buffer_fanout",
        c"PERF-1: Deduplicate change-buffer scans across STs sharing a source (v0.62.0).",
        c"When true (default), the scheduler maintains a per-tick cache of which source \
          OIDs have pending changes and reuses it across all has_table_source_changes \
          calls within the same tick. Eliminates redundant SPI round-trips when many \
          stream tables share the same source. Disable only for troubleshooting.",
        &PGS_ENABLE_CHANGE_BUFFER_FANOUT,
        GucContext::Suset,
        GucFlags::default(),
    );

    // API-1/2: Pause scheduler drain timeout.
    GucRegistry::define_int_guc(
        c"pg_trickle.scheduler_drain_timeout",
        c"API-1/2: Seconds to wait for in-flight refreshes when pause_scheduler() is called.",
        c"After setting the pause flag, pause_scheduler() polls every 100 ms. \
          If the node has not finished refreshing within this many seconds, \
          the call returns and logs a WARNING. Default: 30. Range: 1–3600.",
        &PGS_SCHEDULER_DRAIN_TIMEOUT,
        1,     // min
        3_600, // max (1 hour)
        GucContext::Suset,
        GucFlags::default(),
    );

    // PERF-2: CTE-fused multi-node refresh.
    GucRegistry::define_bool_guc(
        c"pg_trickle.enable_fused_refresh",
        c"PERF-2: Enable CTE-fused multi-node refresh (v0.63.0).",
        c"When true (default), the scheduler composes the delta SQL for multiple \
          DIFFERENTIAL-mode stream tables in the same tick into a single \
          WITH … MERGE CTE chain, reducing planner invocations and round-trips. \
          Disable if a specific DAG shape causes unexpected planner behaviour.",
        &PGS_ENABLE_FUSED_REFRESH,
        GucContext::Suset,
        GucFlags::default(),
    );

    // PERF-2: Fused refresh max delta rows cardinality gate.
    GucRegistry::define_int_guc(
        c"pg_trickle.fused_refresh_max_delta_rows",
        c"PERF-2: Max estimated pending rows for a node to be fusion-eligible (v0.63.0).",
        c"Nodes whose pending change count exceeds this threshold are excluded from \
          the fused CTE chain and refreshed sequentially. Very large deltas produce \
          very large CTEs where separate statements may plan better. \
          Default: 500000. Set to 0 to disable the cardinality gate.",
        &PGS_FUSED_REFRESH_MAX_DELTA_ROWS,
        0,          // min (0 = disabled)
        10_000_000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // PAR-2: Maximum parallel refresh workers.
    GucRegistry::define_int_guc(
        c"pg_trickle.max_parallel_workers",
        c"Maximum parallel refresh workers for the coordinator/worker pool (0 = serial).",
        c"When > 0, the per-database scheduler dispatches independent same-level \
           stream tables to a pool of dynamic background workers for concurrent \
           refresh. Default 0 = serial mode (existing behavior preserved).",
        &PGS_MAX_PARALLEL_WORKERS,
        0,  // min (0 = serial)
        32, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // DVM-3: Delta invariant validation GUC.
    GucRegistry::define_bool_guc(
        c"pg_trickle.validate_delta_invariants",
        c"DVM-3: Validate DIFFERENTIAL refresh results against full recomputation (v0.77.0).",
        c"When true, after every successful differential MERGE the scheduler compares \
          the stream table row count against a full recomputation of the defining query \
          and emits a WARNING on any discrepancy. Significant performance impact — \
          enable only for debugging or in CI validation. Default: false.",
        &PGS_VALIDATE_DELTA_INVARIANTS,
        GucContext::Suset,
        GucFlags::default(),
    );

    // D-3: Test-mode chaos injection GUC.
    GucRegistry::define_string_guc(
        c"pg_trickle.test_chaos_for_table",
        c"TEST-MODE: simulate refresh failure for named stream table (v0.79.0).",
        c"When set to a non-empty stream table name, the scheduler skips the actual \
          refresh for that table and directly increments consecutive_errors on every \
          tick, simulating repeated refresh failures. Used by the D-3 E2E test to \
          trigger auto-suspension without relying on PG exception handling from user \
          triggers. Requires SELECT pg_reload_conf() after ALTER SYSTEM SET. \
          Default: empty string (disabled). Do not set in production.",
        &PGS_TEST_CHAOS_FOR_TABLE,
        GucContext::Suset,
        GucFlags::default(),
    );

    // ── v0.81.0 GUCs ─────────────────────────────────────────────────────

    // QW-8: Self-healing circuit breaker.
    GucRegistry::define_bool_guc(
        c"pg_trickle.self_heal_oom",
        c"QW-8: Auto-reduce merge_work_mem_mb when OOM is detected during refresh.",
        c"When true, if a refresh fails with an 'out of memory' SPI error, the scheduler \
          temporarily reduces the effective merge_work_mem_mb for the affected stream table \
          by 25% and retries. Resets after 3 consecutive successes. Default true.",
        &PGS_SELF_HEAL_OOM,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.self_heal_lock_timeout",
        c"QW-8: Auto-backoff on lock timeout during refresh.",
        c"When true, if a refresh fails with a lock-timeout error, the scheduler \
          doubles the effective interval for the affected stream table (exponential \
          backoff) until 3 consecutive successes. Default true.",
        &PGS_SELF_HEAL_LOCK_TIMEOUT,
        GucContext::Suset,
        GucFlags::default(),
    );
}

// ── Accessor functions ────────────────────────────────────────────────────

/// Returns the scheduler interval in milliseconds.
pub fn pg_trickle_scheduler_interval_ms() -> i32 {
    PGS_SCHEDULER_INTERVAL_MS.get()
}

/// Returns the minimum schedule in seconds.
pub fn pg_trickle_min_schedule_seconds() -> i32 {
    PGS_MIN_SCHEDULE_SECONDS.get()
}

/// Returns the default effective schedule (in seconds) for isolated CALCULATED
/// stream tables that have no downstream dependents.
pub fn pg_trickle_default_schedule_seconds() -> i32 {
    PGS_DEFAULT_SCHEDULE_SECONDS.get()
}

/// Returns the max consecutive errors before auto-suspend.
pub fn pg_trickle_max_consecutive_errors() -> i32 {
    PGS_MAX_CONSECUTIVE_ERRORS.get()
}

/// Returns the max change ratio for adaptive FULL fallback.
pub fn pg_trickle_differential_max_change_ratio() -> f64 {
    PGS_DIFFERENTIAL_MAX_CHANGE_RATIO.get()
}

/// B-4: Returns the refresh strategy override.
pub fn pg_trickle_refresh_strategy() -> RefreshStrategy {
    normalize_refresh_strategy(
        PGS_REFRESH_STRATEGY
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// B-4: Returns the cost-model safety margin (default 0.8).
pub fn pg_trickle_cost_model_safety_margin() -> f64 {
    PGS_COST_MODEL_SAFETY_MARGIN.get()
}

/// PH-E1: Returns the max estimated delta output rows before FULL fallback.
/// Returns 0 when disabled.
pub fn pg_trickle_max_delta_estimate_rows() -> i32 {
    PGS_MAX_DELTA_ESTIMATE_ROWS.get()
}

/// Returns whether automatic schedule backoff is enabled for falling-behind STs.
pub fn pg_trickle_auto_backoff() -> bool {
    PGS_AUTO_BACKOFF.get()
}

/// G-7: Returns whether tiered refresh scheduling is enabled.
pub fn pg_trickle_tiered_scheduling() -> bool {
    PGS_TIERED_SCHEDULING.get()
}

/// Returns whether the tick watermark (CSS1) feature is enabled.
pub fn pg_trickle_tick_watermark_enabled() -> bool {
    PGS_TICK_WATERMARK_ENABLED.get()
}

/// Returns the maximum fixpoint iterations for SCC convergence (CYC-4).
pub fn pg_trickle_max_fixpoint_iterations() -> i32 {
    PGS_MAX_FIXPOINT_ITERATIONS.get()
}

/// Returns whether circular (cyclic) dependencies are allowed (CYC-4).
pub fn pg_trickle_allow_circular() -> bool {
    PGS_ALLOW_CIRCULAR.get()
}

/// Returns the cluster-wide cap on dynamic refresh workers.
pub fn pg_trickle_max_dynamic_refresh_workers() -> i32 {
    PGS_MAX_DYNAMIC_REFRESH_WORKERS.get()
}

/// C3-1: Returns the per-database worker quota (0 = disabled).
pub fn pg_trickle_per_database_worker_quota() -> i32 {
    PGS_PER_DATABASE_WORKER_QUOTA.get()
}

/// Returns the current parallel refresh mode.
pub fn pg_trickle_parallel_refresh_mode() -> ParallelRefreshMode {
    normalize_parallel_refresh_mode(
        PGS_PARALLEL_REFRESH_MODE
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// SCAL-5: Returns the persistent worker pool size (0 = spawn-per-task).
pub fn pg_trickle_worker_pool_size() -> i32 {
    PGS_WORKER_POOL_SIZE.get()
}

/// PRED-1: Returns the prediction window in minutes.
pub fn pg_trickle_prediction_window() -> i32 {
    PGS_PREDICTION_WINDOW.get()
}

/// PRED-2: Returns the prediction ratio threshold for pre-emptive FULL switch.
pub fn pg_trickle_prediction_ratio() -> f64 {
    PGS_PREDICTION_RATIO.get()
}

/// PRED-3: Returns the minimum number of history samples before prediction activates.
pub fn pg_trickle_prediction_min_samples() -> i32 {
    PGS_PREDICTION_MIN_SAMPLES.get()
}

/// SCAL-1 (v0.31.0): Returns the number of consecutive cycles before emitting
/// a back-pressure alert.
pub fn pg_trickle_backpressure_consecutive_limit() -> i32 {
    PGS_BACKPRESSURE_CONSECUTIVE_LIMIT.get()
}

/// FUSE-5: Returns the global default fuse ceiling (0 = disabled).
pub fn pg_trickle_fuse_default_ceiling() -> i64 {
    PGS_FUSE_DEFAULT_CEILING.get() as i64
}

/// Returns whether automatic index creation is enabled.
pub fn pg_trickle_auto_index() -> bool {
    PGS_AUTO_INDEX.get()
}

/// SCAL-1 (v0.30.0): Returns whether SQLSTATE-based SPI error classification is enabled.
pub fn pg_trickle_use_sqlstate_classification() -> bool {
    PGS_USE_SQLSTATE_CLASSIFICATION.get()
}

/// PERF-1 (v0.31.0): Returns whether adaptive batch coalescing is enabled.
pub fn pg_trickle_adaptive_batch_coalescing() -> bool {
    PGS_ADAPTIVE_BATCH_COALESCING.get()
}

/// PERF-2 (v0.31.0): Returns whether adaptive merge strategy selection is enabled.
pub fn pg_trickle_adaptive_merge_strategy() -> bool {
    PGS_ADAPTIVE_MERGE_STRATEGY.get()
}

/// PERF-1 (v0.62.0): Returns whether the change-buffer fan-out deduplication is enabled.
pub fn pg_trickle_enable_change_buffer_fanout() -> bool {
    PGS_ENABLE_CHANGE_BUFFER_FANOUT.get()
}

/// API-1/2 (v0.62.0): Returns the pause_scheduler drain timeout in seconds.
pub fn pg_trickle_scheduler_drain_timeout() -> i32 {
    PGS_SCHEDULER_DRAIN_TIMEOUT.get()
}

/// PERF-2 (v0.63.0): Returns whether CTE-fused multi-node refresh is enabled.
pub fn pg_trickle_enable_fused_refresh() -> bool {
    PGS_ENABLE_FUSED_REFRESH.get()
}

/// PERF-2 (v0.63.0): Returns the maximum delta-row cardinality for fusion eligibility.
/// Returns `None` when the limit is disabled (value == 0).
pub fn pg_trickle_fused_refresh_max_delta_rows() -> Option<i64> {
    let v = PGS_FUSED_REFRESH_MAX_DELTA_ROWS.get();
    if v > 0 { Some(v as i64) } else { None }
}

/// PAR-2: Returns the maximum parallel refresh workers (0 = serial).
pub fn pg_trickle_max_parallel_workers() -> i32 {
    PGS_MAX_PARALLEL_WORKERS.get()
}

/// A46-10: Returns whether lag-aware cross-database scheduling is enabled.
pub fn pg_trickle_lag_aware_scheduling() -> bool {
    #[cfg(test)]
    {
        false
    }
    #[cfg(not(test))]
    {
        PGS_LAG_AWARE_SCHEDULING.get()
    }
}

/// A44-4: Returns the effective cost-cache capacity (capped to 256).
pub fn pg_trickle_cost_cache_capacity() -> usize {
    #[cfg(test)]
    {
        256usize
    }
    #[cfg(not(test))]
    {
        (PGS_COST_CACHE_CAPACITY.get().clamp(16, 256)) as usize
    }
}

/// A46-7: Returns the effective invalidation ring capacity (clamped to 1–4096).
pub fn pg_trickle_invalidation_ring_capacity() -> usize {
    #[cfg(test)]
    {
        128usize
    }
    #[cfg(not(test))]
    {
        (PGS_INVALIDATION_RING_CAPACITY.get().clamp(1, 4096)) as usize
    }
}

/// VP-2 (v0.47.0): Returns the global default drift threshold for
/// drift-triggered REINDEX operations.
pub fn pg_trickle_reindex_drift_threshold() -> f64 {
    PGS_REINDEX_DRIFT_THRESHOLD.get()
}

/// Returns the `pgt_st_locks` lease duration in milliseconds for Citus coordination.
pub fn pg_trickle_citus_st_lock_lease_ms() -> i64 {
    PGS_CITUS_ST_LOCK_LEASE_MS.get() as i64
}

/// COORD-15: Returns the number of consecutive worker-poll failures before
/// flagging in `citus_status`.  Returns 0 when alerting is disabled.
pub fn pg_trickle_citus_worker_retry_ticks() -> i32 {
    PGS_CITUS_WORKER_RETRY_TICKS.get()
}

/// A08 (v0.35.0): Returns whether force-full-refresh override is active.
pub fn pg_trickle_force_full_refresh() -> bool {
    PGS_FORCE_FULL_REFRESH.get()
}

/// UX-GUC (v0.35.0): Returns the NOTIFY coalescing debounce interval in ms.
/// Returns 0 when coalescing is disabled.
pub fn pg_trickle_notify_coalesce_ms() -> i32 {
    PGS_NOTIFY_COALESCE_MS.get()
}

/// A10 (v0.35.0): Returns the history pruner interval in seconds (0 = legacy).
pub fn pg_trickle_history_prune_interval_seconds() -> i32 {
    PGS_HISTORY_PRUNE_INTERVAL_SECONDS.get()
}

/// A35 (v0.36.0): Returns the drain timeout in seconds.
pub fn pg_trickle_drain_timeout() -> i32 {
    PGS_DRAIN_TIMEOUT.get()
}

/// DVM-3 (v0.77.0): Returns whether differential refresh result validation is enabled.
pub fn pg_trickle_validate_delta_invariants() -> bool {
    PGS_VALIDATE_DELTA_INVARIANTS.get()
}

/// D-3 (v0.79.0): Returns the stream table name for which refresh failures
/// should be simulated (test-mode only). Returns empty string when disabled.
pub fn pg_trickle_test_chaos_for_table() -> String {
    PGS_TEST_CHAOS_FOR_TABLE
        .get()
        .and_then(|cs| cs.to_str().ok().map(|s| s.to_string()))
        .unwrap_or_default()
}

// ── v0.81.0 accessor functions ────────────────────────────────────────────

/// QW-8 (v0.81.0): Returns whether OOM self-healing is enabled.
pub fn pg_trickle_self_heal_oom() -> bool {
    PGS_SELF_HEAL_OOM.get()
}

/// QW-8 (v0.81.0): Returns whether lock-timeout interval backoff is enabled.
pub fn pg_trickle_self_heal_lock_timeout() -> bool {
    PGS_SELF_HEAL_LOCK_TIMEOUT.get()
}
