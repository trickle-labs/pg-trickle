//! GUC (Grand Unified Configuration) variables for pgtrickle.
//!
//! These are registered in `_PG_init()` and control the extension's behavior.
//! All GUC names are prefixed with `pgtrickle.`.

use pgrx::guc::*;

/// Master enable/disable switch for the extension.
pub static PGS_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);

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

/// Schema name for change buffer tables.
pub static PGS_CHANGE_BUFFER_SCHEMA: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"pgtrickle_changes"));

/// Maximum number of concurrent refresh workers.
///
/// Default: 4. A single worker typically sustains 200–500 refreshes/s on
/// an 8-core instance; 4 workers cover bursty diamond DAG parallelism
/// without saturating I/O.  Set equal to the number of parallel refresh
/// chains in your largest DAG for maximum throughput.
pub static PGS_MAX_CONCURRENT_REFRESHES: GucSetting<i32> = GucSetting::<i32>::new(4);

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

/// WM-7: Maximum seconds a watermark may remain un-advanced before being
/// considered "stuck". When a watermark group contains a stuck source,
/// downstream stream tables in that group are paused (skipped) and a
/// `pgtrickle_alert` NOTIFY with category `watermark_stuck` is emitted.
///
/// Set to 0 to disable stuck-watermark detection (default).
pub static PGS_WATERMARK_HOLDBACK_TIMEOUT: GucSetting<i32> = GucSetting::<i32>::new(0);

/// PH-E2: Temp blocks written threshold for spill detection.
///
/// After each differential MERGE, the refresh executor queries
/// `pg_stat_statements` for `temp_blks_written`. If the value exceeds
/// this threshold, the refresh is considered a "spill". When
/// `spill_consecutive_limit` consecutive spills are recorded for the
/// same stream table, the scheduler forces a FULL refresh on the next
/// cycle to avoid repeated temp-file overhead.
///
/// Set to 0 to disable spill detection (default).
/// Requires `pg_stat_statements` extension to be installed.
pub static PGS_SPILL_THRESHOLD_BLOCKS: GucSetting<i32> = GucSetting::<i32>::new(0);

/// PH-E2: Number of consecutive spills before auto-switching to FULL refresh.
///
/// When a stream table accumulates this many consecutive differential
/// refreshes where `temp_blks_written > spill_threshold_blocks`, the
/// scheduler marks the ST for reinitialization (FULL refresh) on the
/// next cycle. The counter resets after each non-spilling refresh.
pub static PGS_SPILL_CONSECUTIVE_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(3);

/// Whether to use TRUNCATE instead of DELETE for change buffer cleanup
/// when the entire buffer is consumed by a refresh.
///
/// TRUNCATE is O(1) regardless of row count, versus per-row DELETE which
/// must update indexes. This saves 3–5ms per refresh at 10%+ change rates.
///
/// Set to false if the TRUNCATE AccessExclusiveLock on the change buffer
/// is problematic for concurrent DML on the source table.
pub static PGS_CLEANUP_USE_TRUNCATE: GucSetting<bool> = GucSetting::<bool>::new(true);

/// C4: Consolidated planner aggressiveness switch.
///
/// When enabled (default), the refresh executor estimates the delta size and
/// applies `SET LOCAL` planner hints before MERGE execution:
/// - delta >= 100 rows: `SET LOCAL enable_nestloop = off` (favour hash joins)
/// - delta >= 10 000 rows: additionally `SET LOCAL work_mem = '<N>MB'`
///
/// Replaces the old `merge_planner_hints` and `merge_work_mem_mb` GUCs
/// (both still accepted but emit deprecation warnings).
pub static PGS_PLANNER_AGGRESSIVE: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Deprecated — use `pg_trickle.planner_aggressive` instead.
/// Kept for backward compatibility; emits a deprecation warning when read.
pub static PGS_MERGE_PLANNER_HINTS: GucSetting<bool> = GucSetting::<bool>::new(true);

/// `work_mem` (in MB) applied via `SET LOCAL` when the estimated delta
/// exceeds 10 000 rows and planner hints are enabled.
///
/// A higher value lets PostgreSQL use larger hash tables for the MERGE
/// join, avoiding disk-spilling sort/merge strategies on large deltas.
pub static PGS_MERGE_WORK_MEM_MB: GucSetting<i32> = GucSetting::<i32>::new(64);

/// SCAL-3: Maximum `work_mem` (in MB) allowed during delta MERGE execution.
///
/// When the planner hints would set `work_mem` above this cap (for deep
/// joins or large deltas), the refresh falls back to FULL instead. This
/// prevents OOM on unexpectedly large deltas where hash joins would
/// allocate unbounded memory.
///
/// PERF-004 (v0.70.0): Default changed from 0 (disabled) to 256 MB.
/// Deployments that rely on the old unlimited behaviour must set
/// `pg_trickle.delta_work_mem_cap_mb = 0` explicitly in `postgresql.conf`.
pub static PGS_DELTA_WORK_MEM_CAP_MB: GucSetting<i32> = GucSetting::<i32>::new(256);

/// Whether to use SQL PREPARE / EXECUTE for MERGE statements.
///
/// When enabled, the refresh executor issues `PREPARE __pgt_merge_{id}`
/// on the first cache-hit cycle, then uses `EXECUTE` on subsequent cycles.
/// After ~5 executions PostgreSQL switches from a custom plan to a generic
/// plan, saving 1–2ms of parse/plan overhead per refresh.
///
/// Disable if prepared-statement parameter sniffing produces poor plans
/// (e.g., highly skewed LSN distributions).
pub static PGS_USE_PREPARED_STATEMENTS: GucSetting<bool> = GucSetting::<bool>::new(true);

/// User-trigger handling mode for stream table refresh.
///
/// - `"auto"` (default): Detect user-defined row-level triggers on the
///   stream table and automatically use explicit DML (DELETE + UPDATE +
///   INSERT) so triggers fire with correct `TG_OP`, `OLD`, and `NEW`.
/// - `"off"`: Always use MERGE; user triggers will NOT fire correctly.
/// - `"on"`: Deprecated compatibility alias for `"auto"`.
pub static PGS_USER_TRIGGERS: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"auto"));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserTriggersMode {
    Auto,
    Off,
}

impl UserTriggersMode {
    pub fn as_str(self) -> &'static str {
        match self {
            UserTriggersMode::Auto => "auto",
            UserTriggersMode::Off => "off",
        }
    }
}

fn normalize_user_triggers_mode(value: Option<String>) -> UserTriggersMode {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("off") => UserTriggersMode::Off,
        _ => UserTriggersMode::Auto,
    }
}

fn threshold_mb_to_bytes(megabytes: i32) -> i64 {
    megabytes as i64 * 1024 * 1024
}

/// CDC mechanism selection.
///
/// - `"auto"` (default): Use triggers for creation, transition to WAL if
///   `wal_level = logical` is available. Falls back to triggers automatically.
/// - `"trigger"`: Always use row-level triggers for CDC.
/// - `"wal"`: Require WAL-based CDC (fail if `wal_level != logical`).
pub static PGS_CDC_MODE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"auto"));

/// Maximum time (seconds) to wait for the WAL decoder to catch up during
/// transition from triggers to WAL-based CDC before falling back to triggers.
pub static PGS_WAL_TRANSITION_TIMEOUT: GucSetting<i32> = GucSetting::<i32>::new(300);

/// Warning threshold (in MB) for retained WAL on pg_trickle replication slots.
///
/// When a WAL-mode source retains more than this amount of WAL, pg_trickle:
/// - emits a `slot_lag_warning` NOTIFY event from the scheduler, and
/// - reports a WARN row in `pgtrickle.health_check()`.
pub static PGS_SLOT_LAG_WARNING_THRESHOLD_MB: GucSetting<i32> = GucSetting::<i32>::new(100);

/// Critical threshold (in MB) for retained WAL on pg_trickle replication slots.
///
/// When a WAL-mode source retains more than this amount of WAL,
/// `pgtrickle.check_cdc_health()` reports a `slot_lag_exceeds_threshold` alert
/// for the source.
pub static PGS_SLOT_LAG_CRITICAL_THRESHOLD_MB: GucSetting<i32> = GucSetting::<i32>::new(1024);

/// When true, schema-altering DDL (column ADD/DROP/RENAME/ALTER TYPE) on
/// source tables used by stream tables is blocked with an ERROR instead of
/// triggering reinitialization.
///
/// Benign DDL (CREATE INDEX, COMMENT ON, ALTER TABLE SET STATISTICS) and
/// constraint-only changes are always allowed regardless of this setting.
///
/// Default is `true` (enabled) as of v0.11.0 — set to `false` to restore
/// the previous permissive behavior (DDL triggers reinitialization instead
/// of blocking).
pub static PGS_BLOCK_SOURCE_DDL: GucSetting<bool> = GucSetting::<bool>::new(true);

/// F46 (G9.3): Buffer growth alert threshold (number of pending change rows).
///
/// When any source table's change buffer exceeds this number of rows,
/// a `BufferGrowthWarning` alert is emitted. Configurable to accommodate
/// both high-throughput workloads (raise) and small tables (lower).
pub static PGS_BUFFER_ALERT_THRESHOLD: GucSetting<i32> = GucSetting::<i32>::new(1_000_000);

/// C-4: Change buffer compaction threshold (pending change row count).
///
/// When a source table's pending change buffer exceeds this many rows,
/// compaction is triggered before the next refresh cycle. Compaction
/// eliminates net-zero INSERT+DELETE pairs and collapses multi-change
/// groups to first+last rows per pk_hash.
///
/// Set to 0 to disable compaction. Typical values: 10_000–1_000_000.
pub static PGS_COMPACT_THRESHOLD: GucSetting<i32> = GucSetting::<i32>::new(100_000);

/// BUF-LIMIT: Hard limit on total change buffer rows per source table.
///
/// When a source table's change buffer exceeds this many rows at refresh
/// time, pg_trickle falls back to FULL refresh and truncates the buffer.
/// This prevents unbounded disk growth when differential refresh fails
/// repeatedly.
///
/// Set to 0 to disable the limit. Default: 1,000,000 rows.
pub static PGS_MAX_BUFFER_ROWS: GucSetting<i32> = GucSetting::<i32>::new(1_000_000);

/// AUTO-IDX: Automatic index creation on stream tables.
///
/// When enabled, `create_stream_table()` automatically creates indexes on
/// GROUP BY keys, DISTINCT columns, and adds INCLUDE clauses to the
/// `__pgt_row_id` index for stream tables with ≤ 8 output columns.
pub static PGS_AUTO_INDEX: GucSetting<bool> = GucSetting::<bool>::new(true);

/// B-1: Aggregate fast-path — use explicit DML instead of MERGE for
/// GROUP BY queries where all aggregates are algebraically invertible
/// (COUNT, SUM, AVG, etc.).  The explicit DML path (DELETE+UPDATE+INSERT)
/// avoids the MERGE hash-join cost, which is the dominant overhead for
/// aggregate stream tables with many groups.
pub static PGS_AGGREGATE_FAST_PATH: GucSetting<bool> = GucSetting::<bool>::new(true);

/// G14-SHC: Enable the cross-backend template cache backed by an UNLOGGED
/// catalog table (`pgtrickle.pgt_template_cache`).  When enabled, delta SQL
/// templates are persisted so that new backends avoid the ~45 ms DVM
/// parse+differentiate cost on their first refresh of each stream table.
///
/// **Cache architecture (O40-7):**
/// - **L0 — process-local `RwLock<HashMap>`** in each backend/worker process.
///   Fast (ns lookups), but **not shared across pooler connections**. A PgBouncer
///   transaction-pooling deployment will incur an L0 miss on every new connection
///   to the backend. The L0 hit rate is only high for long-lived or session-pinned
///   connections.
/// - **L1 — thread-local delta template** per Rust thread (`DELTA_TEMPLATE_CACHE`).
///   Fastest path; reset on each pgrx `SPI::connect()` context switch.
/// - **L2 — catalog table** (`pgtrickle.pgt_template_cache`, UNLOGGED).
///   Shared across all backends for the same stream table OID. Populated by the
///   first backend to compute a template; subsequent backends load from L2 and
///   promote to L0/L1.
///
/// In transaction-pooling mode, rely on L2 rather than L0 warm-up for
/// cross-connection performance.
pub static PGS_TEMPLATE_CACHE: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Maximum allowed grouping set branches for CUBE/ROLLUP expansion (EC-02).
pub static PGS_MAX_GROUPING_SET_BRANCHES: GucSetting<i32> = GucSetting::<i32>::new(64);

/// G13-SD: Maximum recursion depth for the query parser's tree visitors.
///
/// Prevents stack-overflow crashes on pathological queries with deeply
/// nested subqueries, CTEs, or set operations.  Returns
/// `PgTrickleError::QueryTooComplex` when the limit is exceeded.
pub static PGS_MAX_PARSE_DEPTH: GucSetting<i32> = GucSetting::<i32>::new(64);

/// C-7 / R-7 (v0.54.0): Maximum number of CTEs that the differential query
/// generator may produce for a single refresh cycle.
///
/// Complex queries with many operators, joins, and set operations can produce
/// hundreds of CTEs. This guard prevents unbounded memory growth from
/// pathological queries.  Returns `PgTrickleError::DiffCteCountExceeded`
/// when the limit is exceeded.  The default of 1000 is well above what
/// any realistic query requires (~10–60 CTEs for TPC-H queries).
pub static PGS_MAX_DIFF_CTES: GucSetting<i32> = GucSetting::<i32>::new(1000);

/// Number of differential refresh cycles after which algebraic aggregate
/// stream tables are automatically reinitialized (full recompute) to reset
/// accumulated floating-point drift in auxiliary sum/sum2 columns.
///
/// Set to 0 to disable periodic drift reset (default).
/// Typical values: 100–1000, depending on workload precision requirements.
pub static PGS_ALGEBRAIC_DRIFT_RESET_CYCLES: GucSetting<i32> = GucSetting::<i32>::new(0);

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

/// P3-4: Delta-to-ST-size ratio below which `SET LOCAL enable_seqscan = off`
/// is applied before MERGE execution.
///
/// For small deltas against large stream tables, PostgreSQL's planner often
/// chooses a sequential scan of the stream table for the MERGE join on
/// `__pgt_row_id`, yielding O(n) full-table I/O when an index lookup would
/// be O(log n). When the delta row count is below this fraction of the
/// stream table's estimated row count, the seqscan is disabled.
///
/// Set to 0.0 to disable this optimization.
pub static PGS_MERGE_SEQSCAN_THRESHOLD: GucSetting<f64> = GucSetting::<f64>::new(0.001);

/// Maximum LIMIT value for TopK stream tables in IMMEDIATE mode.
///
/// TopK queries with `LIMIT > threshold` are rejected in IMMEDIATE mode
/// because inline recomputation of large result sets adds unacceptable
/// latency to the trigger path. Set to 0 to disable TopK in IMMEDIATE mode.
pub static PGS_IVM_TOPK_MAX_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(1000);

/// Maximum recursion depth for `WITH RECURSIVE` CTEs in IMMEDIATE mode.
///
/// The semi-naive delta query generated for an IMMEDIATE-mode recursive
/// CTE includes a `__pgt_depth` counter.  Propagation stops when this
/// counter reaches the configured limit, preventing infinite loops caused
/// by cyclic data or deeply recursive hierarchies that would otherwise
/// exhaust PostgreSQL's `max_stack_depth` inside a trigger body.
///
/// Set to 0 to disable the depth guard (allow unlimited recursion).
/// The default (100) is sufficient for virtually all practical hierarchies.
pub static PGS_IVM_RECURSIVE_MAX_DEPTH: GucSetting<i32> = GucSetting::<i32>::new(100);

/// STAB-1: Cluster-wide connection pooler mode.
///
/// Overrides the per-ST `pooler_compatibility_mode` for all stream tables.
/// - `"off"` (default): per-ST setting governs (normal behaviour).
/// - `"transaction"`: globally disable prepared-statement reuse and suppress
///   NOTIFY emissions, matching PgBouncer transaction-pooling requirements.
/// - `"session"`: explicit opt-in to session mode — same as `"off"` today,
///   reserved for future session-pinning optimisations.
pub static PGS_CONNECTION_POOLER_MODE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"off"));

/// DB-5: History retention in days.
///
/// The scheduler runs a daily cleanup that deletes rows from
/// `pgtrickle.pgt_refresh_history` older than this many days.
/// Set to 0 to disable automatic cleanup (history grows unbounded).
pub static PGS_HISTORY_RETENTION_DAYS: GucSetting<i32> = GucSetting::<i32>::new(90);

// ── OP-2: Prometheus metrics HTTP port ──────────────────────────────────────

/// TCP port on which the per-database scheduler serves an OpenMetrics
/// (Prometheus) endpoint at `GET /metrics`.
///
/// Default `0` means the endpoint is disabled.  When set to a valid port
/// number (1–65535), the scheduler spawns a background thread that
/// handles exactly one connection per poll cycle.  The server is single-
/// threaded and designed for low-frequency scraping (≤ once per second).
///
/// Example:
/// ```sql
/// ALTER SYSTEM SET pg_trickle.metrics_port = 9188;
/// SELECT pg_reload_conf();
/// ```
pub static PGS_METRICS_PORT: GucSetting<i32> = GucSetting::<i32>::new(0);

/// OP-2: Returns the configured Prometheus metrics port.
/// Returns `0` when the endpoint is disabled.
pub fn pg_trickle_metrics_port() -> i32 {
    PGS_METRICS_PORT.get()
}

/// Buffer table partitioning mode (Task 3.3).
///
/// Controls whether change buffer tables use `PARTITION BY RANGE (lsn)`:
/// - `"off"` (default): Unpartitioned heap tables (current behaviour).
/// - `"on"`: Always partition. After each refresh cycle, old partitions
///   are detached and dropped (O(1), no VACUUM needed).
/// - `"auto"`: Enable partitioning for sources whose effective refresh
///   schedule is >= 30 s (below that, DDL overhead exceeds VACUUM savings).
pub static PGS_BUFFER_PARTITIONING: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"off"));

/// Enable polling-based change detection for foreign tables (EC-05).
///
/// When enabled, foreign tables used in DIFFERENTIAL / IMMEDIATE mode
/// defining queries will be supported via a snapshot-comparison approach:
/// before each refresh cycle the scheduler materializes a snapshot of
/// the foreign table into a local shadow table, then computes EXCEPT ALL
/// deltas against the previous snapshot.
pub static PGS_FOREIGN_TABLE_POLLING: GucSetting<bool> = GucSetting::<bool>::new(false);

/// When `true`, materialized views referenced in DIFFERENTIAL/IMMEDIATE
/// defining queries will be supported via a snapshot-comparison approach
/// (same mechanism as foreign table polling).
pub static PGS_MATVIEW_POLLING: GucSetting<bool> = GucSetting::<bool>::new(false);

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

/// Cluster-wide cap on concurrently active pg_trickle dynamic refresh workers.
///
/// This is distinct from `pg_trickle.max_concurrent_refreshes`, which is the
/// per-database dispatch cap. This GUC prevents multiple database coordinators
/// from overcommitting the shared PostgreSQL `max_worker_processes` budget.
pub static PGS_MAX_DYNAMIC_REFRESH_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(4);

/// CDC trigger granularity.
///
/// - `"statement"` (default): Use statement-level AFTER triggers with transition
///   tables (`NEW TABLE AS __pgt_new` / `OLD TABLE AS __pgt_old`). A single
///   trigger invocation per statement processes all affected rows via a bulk
///   `INSERT … SELECT FROM __pgt_new/old`, giving 50–80% less write-side
///   overhead for bulk DML. Zero change for single-row DML.
/// - `"row"`: Legacy per-row AFTER triggers — one trigger invocation and one
///   change-buffer INSERT per affected row. Equivalent to pg_trickle < 0.4.0.
///
/// Changing this GUC takes effect for newly created stream tables. To migrate
/// existing stream tables call `SELECT pgtrickle.rebuild_cdc_triggers()`.
pub static PGS_CDC_TRIGGER_MODE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"statement"));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcTriggerMode {
    Statement,
    Row,
}

impl CdcTriggerMode {
    pub fn as_str(self) -> &'static str {
        match self {
            CdcTriggerMode::Statement => "statement",
            CdcTriggerMode::Row => "row",
        }
    }
}

fn normalize_cdc_trigger_mode(value: Option<String>) -> CdcTriggerMode {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("row") => CdcTriggerMode::Row,
        _ => CdcTriggerMode::Statement,
    }
}

fn normalize_recursive_max_depth(value: i32) -> Option<i32> {
    if value > 0 { Some(value) } else { None }
}

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

/// QF-1: When `true`, the MERGE SQL template is emitted to the PostgreSQL
/// server log at `LOG` level on every refresh cycle.
///
/// Intended for debugging MERGE query generation only. **Do not enable in
/// production** — every refresh will emit potentially large SQL strings to
/// the server log.
pub static PGS_LOG_MERGE_SQL: GucSetting<bool> = GucSetting::<bool>::new(false);

/// FUSE-5: Global default change-count ceiling for the fuse circuit breaker.
///
/// When a stream table's fuse_mode is 'on' or 'auto' and no per-ST
/// `fuse_ceiling` is configured, this global ceiling is used. If the total
/// pending change buffer rows across all sources of an ST exceed this value,
/// the fuse blows and the ST is suspended.
///
/// Set to 0 to disable the global default ceiling (per-ST ceiling only).
pub static PGS_FUSE_DEFAULT_CEILING: GucSetting<i32> = GucSetting::<i32>::new(0);

/// DAG-3: Delta amplification detection threshold.
///
/// When a DIFFERENTIAL refresh produces `output_delta / input_delta` rows
/// exceeding this ratio, pg_trickle emits a WARNING indicating pathological
/// delta amplification (common with many-to-many joins or large fan-out).
/// The warning includes the stream table name, input/output counts, and
/// the computed ratio, helping operators identify and tune problematic hops.
///
/// Set to 0.0 to disable amplification detection.
pub static PGS_DELTA_AMPLIFICATION_THRESHOLD: GucSetting<f64> = GucSetting::<f64>::new(100.0);

/// DIAG-2: Estimated GROUP BY cardinality threshold for algebraic aggregate
/// DIFFERENTIAL mode warning.
///
/// At `create_stream_table` time, if the defining query uses algebraic
/// aggregates (SUM, COUNT, AVG) in DIFFERENTIAL mode and the estimated
/// group cardinality (from `pg_stats.n_distinct`) is below this threshold,
/// a WARNING is emitted suggesting FULL or AUTO mode instead.
///
/// Low-cardinality GROUP BY columns make DIFFERENTIAL aggregates maintain
/// auxiliary columns for very few groups, which may not justify the overhead.
///
/// Set to 0 to disable the cardinality warning.
pub static PGS_AGG_DIFF_CARDINALITY_THRESHOLD: GucSetting<i32> = GucSetting::<i32>::new(1000);

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

/// VOL-1: Volatile function policy for DIFFERENTIAL/IMMEDIATE mode.
///
/// Controls how volatile functions in defining queries are handled:
/// - `"reject"` (default): Error — volatile functions are rejected.
/// - `"warn"`: Allow creation with a WARNING.
/// - `"allow"`: Allow silently.
pub static PGS_VOLATILE_FUNCTION_POLICY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"reject"));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolatileFunctionPolicy {
    Reject,
    Warn,
    Allow,
}

impl VolatileFunctionPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            VolatileFunctionPolicy::Reject => "reject",
            VolatileFunctionPolicy::Warn => "warn",
            VolatileFunctionPolicy::Allow => "allow",
        }
    }
}

fn normalize_volatile_function_policy(value: Option<String>) -> VolatileFunctionPolicy {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("warn") => VolatileFunctionPolicy::Warn,
        Some("allow") => VolatileFunctionPolicy::Allow,
        _ => VolatileFunctionPolicy::Reject,
    }
}

/// PH-D2: Merge join strategy override.
///
/// Controls the join strategy hint applied via `SET LOCAL` during MERGE:
/// - `"auto"` (default): delta-size heuristics choose the strategy.
/// - `"hash_join"`: always prefer hash joins (disable nestloop, raise work_mem).
/// - `"nested_loop"`: always prefer nested loops (disable hashjoin + mergejoin).
/// - `"merge_join"`: always prefer merge joins (disable hashjoin + nestloop).
pub static PGS_MERGE_JOIN_STRATEGY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"auto"));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeJoinStrategy {
    /// Delta-size heuristics (existing behaviour).
    Auto,
    /// Force hash joins.
    HashJoin,
    /// Force nested-loop joins.
    NestedLoop,
    /// Force merge joins.
    MergeJoin,
}

impl MergeJoinStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            MergeJoinStrategy::Auto => "auto",
            MergeJoinStrategy::HashJoin => "hash_join",
            MergeJoinStrategy::NestedLoop => "nested_loop",
            MergeJoinStrategy::MergeJoin => "merge_join",
        }
    }
}

fn normalize_merge_join_strategy(value: Option<String>) -> MergeJoinStrategy {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("hash_join") => MergeJoinStrategy::HashJoin,
        Some("nested_loop") => MergeJoinStrategy::NestedLoop,
        Some("merge_join") => MergeJoinStrategy::MergeJoin,
        _ => MergeJoinStrategy::Auto,
    }
}

/// D-1a: Create new change buffer tables as UNLOGGED.
///
/// When `true`, newly created change buffer tables (`pgtrickle_changes.changes_*`)
/// are created with `CREATE UNLOGGED TABLE` instead of `CREATE TABLE`. This
/// eliminates WAL writes for trigger-inserted CDC rows, reducing WAL
/// amplification by ~30%.
///
/// **Trade-off:** UNLOGGED tables are truncated on crash recovery and are
/// not replicated to standbys. After a crash or standby restart, affected
/// stream tables will automatically receive a FULL refresh on the next
/// scheduler cycle to resynchronize.
///
/// Existing change buffer tables are not retroactively altered. Use
/// `pgtrickle.convert_buffers_to_unlogged()` to convert existing buffers.
///
/// Default `false` — change buffers remain WAL-logged and crash-safe.
///
/// **Deprecated (COR-003/ARCH-001, v0.68.0):** Use `pg_trickle.change_buffer_durability` instead.
/// Setting this GUC emits a deprecation WARNING at runtime.
pub static PGS_UNLOGGED_BUFFERS: GucSetting<bool> = GucSetting::<bool>::new(false);

/// DUR-2: Change buffer durability mode.
///
/// Controls the WAL-logging behavior of change buffer tables:
/// - `"logged"` (default): Change buffers are WAL-logged. Survives crashes
///   and is replicated to standbys. Preserves the pre-v0.68.0 default
///   behavior (equivalent to `pg_trickle.unlogged_buffers = false`).
/// - `"unlogged"`: Change buffers are UNLOGGED for maximum write throughput.
///   After a crash, buffers are lost and the ST receives a FULL refresh.
///   Equivalent to `pg_trickle.unlogged_buffers = true`.
/// - `"sync"`: WAL-logged + `synchronous_commit = on` for the change buffer
///   transaction. Maximum durability — no data loss even under OS crashes.
///
/// This GUC supersedes `pg_trickle.unlogged_buffers` (which is now a
/// compatibility alias: `true` maps to `"unlogged"`, `false` to `"logged"`).
pub static PGS_CHANGE_BUFFER_DURABILITY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"logged"));

/// DUR-2: Change buffer durability mode enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeBufferDurability {
    /// UNLOGGED tables — maximum performance, lost on crash.
    Unlogged,
    /// WAL-logged tables — survives crash, replicated.
    Logged,
    /// WAL-logged + synchronous_commit — maximum durability.
    Sync,
}

impl ChangeBufferDurability {
    pub fn as_str(self) -> &'static str {
        match self {
            ChangeBufferDurability::Unlogged => "unlogged",
            ChangeBufferDurability::Logged => "logged",
            ChangeBufferDurability::Sync => "sync",
        }
    }

    pub fn is_wal_logged(self) -> bool {
        matches!(
            self,
            ChangeBufferDurability::Logged | ChangeBufferDurability::Sync
        )
    }
}

fn normalize_change_buffer_durability(value: Option<String>) -> ChangeBufferDurability {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("logged") => ChangeBufferDurability::Logged,
        Some("sync") => ChangeBufferDurability::Sync,
        _ => ChangeBufferDurability::Unlogged,
    }
}

/// Return the current change buffer durability mode.
///
/// COR-003/ARCH-001 (v0.68.0): Also checks the legacy `unlogged_buffers` GUC
/// for backward compatibility.  When `unlogged_buffers = true`, a deprecation
/// WARNING is emitted and `Unlogged` is returned regardless of the
/// `change_buffer_durability` setting.
pub fn pg_trickle_change_buffer_durability() -> ChangeBufferDurability {
    // COR-003: Backward-compat shim — legacy GUC takes precedence with a
    // deprecation warning.
    if PGS_UNLOGGED_BUFFERS.get() {
        pgrx::warning!(
            "pg_trickle.unlogged_buffers is deprecated. \
             Use pg_trickle.change_buffer_durability = 'unlogged' instead."
        );
        return ChangeBufferDurability::Unlogged;
    }
    let raw = PGS_CHANGE_BUFFER_DURABILITY
        .get()
        .map(|c| c.to_string_lossy().into_owned());
    normalize_change_buffer_durability(raw)
}

/// PH-D1: MERGE strategy override.
///
/// Controls how differential refresh applies deltas to stream tables:
/// - `"auto"` (default): use DELETE+INSERT when `delta_rows / target_rows`
///   is below `merge_strategy_threshold`; MERGE otherwise.
/// - `"merge"`: always use the MERGE statement.
///
/// The former `"delete_insert"` value was removed in v0.19.0 (CORR-1).
/// Setting it now logs a WARNING and falls back to `"auto"`.
pub static PGS_MERGE_STRATEGY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"auto"));

/// PH-D1: Delta ratio threshold for the `auto` merge strategy.
///
/// When `merge_strategy = 'auto'`, DELETE+INSERT is used instead of MERGE
/// when `delta_rows / target_rows < merge_strategy_threshold`. This avoids
/// the MERGE join cost for sub-1% deltas against large tables.
///
/// Default: 0.01 (1%).
pub static PGS_MERGE_STRATEGY_THRESHOLD: GucSetting<f64> = GucSetting::<f64>::new(0.01);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeStrategy {
    /// Heuristic: DELETE+INSERT for small deltas, MERGE otherwise.
    Auto,
    /// Always MERGE.
    Merge,
}

impl MergeStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            MergeStrategy::Auto => "auto",
            MergeStrategy::Merge => "merge",
        }
    }
}

fn normalize_merge_strategy(value: Option<String>) -> MergeStrategy {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("merge") => MergeStrategy::Merge,
        Some("delete_insert") => {
            // CORR-1: The delete_insert strategy was removed in v0.19.0.
            // It was semantically unsafe for aggregate/DISTINCT queries.
            // Suppress the pgrx warning in unit tests — pgrx FFI is not
            // available outside a PostgreSQL backend process.
            #[cfg(not(test))]
            pgrx::warning!(
                "pg_trickle.merge_strategy = 'delete_insert' was removed in v0.19.0 \
                 (unsafe for aggregate/DISTINCT queries). Falling back to 'auto'. \
                 Update your postgresql.conf to use 'auto' or 'merge'."
            );
            MergeStrategy::Auto
        }
        _ => MergeStrategy::Auto,
    }
}

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

fn normalize_refresh_strategy(value: Option<String>) -> RefreshStrategy {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("differential") => RefreshStrategy::Differential,
        Some("full") => RefreshStrategy::Full,
        _ => RefreshStrategy::Auto,
    }
}

// ── Dog-feeding auto-apply GUC (DF-G1) ────────────────────────────────────

/// Dog-feeding auto-apply policy mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelfMonitoringAutoApply {
    /// No automatic configuration changes (default).
    Off,
    /// Apply only threshold recommendations from `df_threshold_advice`.
    ThresholdOnly,
    /// Apply threshold + scheduling hints from `df_scheduling_interference`.
    Full,
}

impl SelfMonitoringAutoApply {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::ThresholdOnly => "threshold_only",
            Self::Full => "full",
        }
    }
}

/// Automatic application mode for self-monitoring stream tables.
/// Controls whether pg_trickle acts on self-monitoring recommendations automatically.
/// - `'off'` (default): self-monitoring stream tables are not auto-applied
/// - `'threshold_only'`: apply only when health thresholds are exceeded
/// - `'full'`: always apply self-monitoring recommendations automatically
pub static PGS_SELF_MONITORING_AUTO_APPLY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"off"));

fn normalize_self_monitoring_auto_apply(value: Option<String>) -> SelfMonitoringAutoApply {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("threshold_only") => SelfMonitoringAutoApply::ThresholdOnly,
        Some("full") => SelfMonitoringAutoApply::Full,
        _ => SelfMonitoringAutoApply::Off,
    }
}

// ── PAR-2: Maximum parallel refresh workers GUC ────────────────────────────

/// PAR-2: Maximum parallel refresh workers for the coordinator/worker pool.
///
/// When > 0, the per-database scheduler dispatches independent same-level
/// stream tables to a pool of dynamic background workers for concurrent
/// refresh. At most `max_parallel_workers` refreshes execute simultaneously.
///
/// Default 0 = serial mode (existing behavior preserved).
pub static PGS_MAX_PARALLEL_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(0);

// ── PRED: Predictive cost model GUCs ───────────────────────────────────────

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

// ── v0.23.0: TPC-H DVM Scaling Performance GUCs ───────────────────────────

/// P1-2: Log delta SQL to server log at DEBUG1 level.
///
/// When enabled, the full delta SQL generated by the DVM engine is logged
/// before execution for each differential refresh cycle. Allows running
/// `EXPLAIN (ANALYZE, BUFFERS)` on captured delta SQL for diagnosis.
///
/// **Do not enable in production** — every refresh will emit potentially
/// large SQL strings to the server log.
pub static PGS_LOG_DELTA_SQL: GucSetting<bool> = GucSetting::<bool>::new(false);

/// P5-1: `work_mem` override (in MB) for delta SQL execution.
///
/// When non-zero, `SET LOCAL work_mem = '<N>MB'` is applied inside
/// `execute_delta_sql` before running the generated delta SQL. This
/// allows tuning delta execution memory independently of the session
/// `work_mem` without a server restart.
///
/// Set to 0 (default) to inherit the session `work_mem`.
pub static PGS_DELTA_WORK_MEM: GucSetting<i32> = GucSetting::<i32>::new(0);

/// P5-2: Disable nested-loop joins during delta SQL execution.
///
/// When enabled, `SET LOCAL enable_nestloop = off` is applied inside
/// `execute_delta_sql` before running the generated delta SQL. Useful
/// diagnostic for planner regressions on large right-side joins before
/// planner statistics are reliable.
pub static PGS_DELTA_ENABLE_NESTLOOP: GucSetting<bool> = GucSetting::<bool>::new(true);

/// PERF-5: Run ANALYZE on change buffer tables before delta SQL execution.
///
/// When enabled, `ANALYZE pgtrickle_changes.changes_<oid>` is run before
/// the delta SQL is executed. This ensures PostgreSQL has accurate row
/// count estimates for change buffer tables, which are truncated and
/// refilled every refresh cycle (auto-analyze never fires on them).
pub static PGS_ANALYZE_BEFORE_DELTA: GucSetting<bool> = GucSetting::<bool>::new(true);

/// SCAL-2: Maximum change buffer rows per source before emitting an alert.
///
/// When non-zero, the refresh executor checks the change buffer row count
/// and emits a `pg_trickle_alert change_buffer_overflow` event if it
/// exceeds this threshold. Prevents the WAL accumulation pattern from
/// going undetected in production.
///
/// Set to 0 to disable (default).
pub static PGS_MAX_CHANGE_BUFFER_ALERT_ROWS: GucSetting<i32> = GucSetting::<i32>::new(0);

/// UX-7: DIFF output row format for aggregate UPDATE-splits.
///
/// Controls how the DI-2 aggregate UPDATE-split surfaces changes:
/// - `"split"` (default): Emit DELETE+INSERT pairs for aggregate UPDATEs.
///   This is the correct algebraic form for O(Δ) performance.
/// - `"merged"`: Re-combine DELETE+INSERT pairs into a single UPDATE row
///   before writing to the stream table. Compatible with pre-v0.23.0
///   consumers that check `op = 'UPDATE'`.
pub static PGS_DIFF_OUTPUT_FORMAT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"split"));

/// UX-7: Diff output format enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiffOutputFormat {
    /// Emit DELETE+INSERT pairs for aggregate UPDATE-splits.
    Split,
    /// Re-combine into UPDATE rows for backward compatibility.
    Merged,
}

impl DiffOutputFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            DiffOutputFormat::Split => "split",
            DiffOutputFormat::Merged => "merged",
        }
    }
}

fn normalize_diff_output_format(value: Option<String>) -> DiffOutputFormat {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("merged") => DiffOutputFormat::Merged,
        _ => DiffOutputFormat::Split,
    }
}

// ── Issue #536: Frontier Visibility Holdback ───────────────────────────────

/// #536: Frontier holdback mode for the trigger-based CDC path.
///
/// Controls whether the scheduler holds back the frontier LSN to avoid
/// silently skipping change-buffer rows from long-running transactions
/// that committed after the previous tick captured the watermark.
///
/// | Value | Meaning |
/// |-------|---------|
/// | `"xmin"` (default) | Probe `pg_stat_activity` + `pg_prepared_xacts` once per tick and cap the frontier to the safe upper bound. |
/// | `"none"` | No holdback — current fast behaviour. Can silently lose rows under long-running transactions. |
/// | `"lsn:<N>"` | Hold back the frontier by exactly N bytes for debugging. |
pub static PGS_FRONTIER_HOLDBACK_MODE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"xmin"));

/// #536: Emit a WARNING when the frontier holdback has been active for
/// longer than this many seconds.
///
/// A holdback occurs when a long-running (or forgotten) transaction keeps
/// the scheduler from advancing the frontier. When this threshold is
/// exceeded, a WARNING is emitted at most once per minute so operators
/// can identify the blocking session.
///
/// Set to 0 to disable the warning (not recommended for production).
pub static PGS_FRONTIER_HOLDBACK_WARN_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(60);

// ── v0.25.0: Scheduler scalability & pooler performance ───────────────────

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

/// CACHE-2: Maximum number of entries in the per-backend L1 template cache.
///
/// When the cache reaches this size, the least-recently-used entry is evicted.
/// Set to 0 to use an unbounded cache (default, matching pre-v0.25.0 behavior).
/// Recommended range: 64–1024 depending on number of stream tables per database.
pub static PGS_TEMPLATE_CACHE_MAX_ENTRIES: GucSetting<i32> = GucSetting::<i32>::new(0);

/// PERF-006 (v0.73.0): Maximum memory (bytes) for the per-backend L1 template
/// cache.
///
/// Set to 0 to disable byte-based eviction and rely only on
/// `template_cache_max_entries`.
pub static PGS_TEMPLATE_CACHE_MAX_BYTES: GucSetting<i32> = GucSetting::<i32>::new(0);

/// PERF-003 (v0.73.0): Cache interval (milliseconds) for the xmin holdback
/// probe result.
///
/// Set to 0 to disable caching and probe on every scheduler tick.
pub static PGS_FRONTIER_HOLDBACK_PROBE_CACHE_MS: GucSetting<i32> = GucSetting::<i32>::new(250);

/// PUB-1: Warn when a publication subscriber lags behind the change buffer
/// by more than this many bytes of WAL.
///
/// When a subscriber's `confirmed_flush_lsn` is more than this many bytes
/// behind the change buffer's maximum LSN, a WARNING is emitted and the
/// change buffer truncation is deferred until the subscriber catches up.
///
/// Set to 0 to disable subscriber lag tracking (default).
/// Recommended value: 104857600 (100 MB).
pub static PGS_PUBLICATION_LAG_WARN_BYTES: GucSetting<i32> = GucSetting::<i32>::new(0);

// ── v0.30.0 GUCs ──────────────────────────────────────────────────────────

/// SCAL-1 (v0.30.0): When true, classify SPI error retryability by SQLSTATE code
/// instead of English message-text patterns.
///
/// The SQLSTATE-based classification is locale-safe: it works correctly regardless
/// of `lc_messages`. Flipped to `true` (default) in v0.31.0 after the validation
/// window. Set to `false` to revert to message-text pattern matching.
pub static PGS_USE_SQLSTATE_CLASSIFICATION: GucSetting<bool> = GucSetting::<bool>::new(true);

/// STAB-3 (v0.30.0): Maximum age (hours) of L2 catalog template cache entries
/// before they are eligible for deletion during the scheduler's launcher tick.
///
/// Prevents stale entries accumulating after ALTER QUERY without DROP or
/// source-OID renumbering. Set to 0 to disable age-based purging.
/// Default: 168 hours (7 days).
pub static PGS_TEMPLATE_CACHE_MAX_AGE_HOURS: GucSetting<i32> = GucSetting::<i32>::new(168);

/// PERF-2 (v0.30.0): Maximum number of parse tree nodes allowed in a single query.
///
/// Queries that exceed this limit are rejected with `QueryTooComplex` to prevent
/// unbounded memory allocation in the parse advisory warnings cache and CTE registry.
/// Set to 0 to disable the limit (default).
pub static PGS_MAX_PARSE_NODES: GucSetting<i32> = GucSetting::<i32>::new(0);

// ── v0.31.0 GUCs ──────────────────────────────────────────────────────────

/// PERF-4 (v0.31.0): Use ENR (Ephemeral Named Relations) directly in IVM trigger
/// bodies instead of copying transition data to temp tables.
///
/// When true (default), the AFTER trigger function bodies skip the
/// `CREATE TEMP TABLE ... AS SELECT * FROM __pgt_newtable` step and pass
/// the ENR names directly to the delta-apply function. This eliminates a
/// per-statement heap allocation for INSERT/UPDATE/DELETE on IMMEDIATE-mode
/// stream tables.
///
/// When false, the legacy temp-table copy behaviour is used.
/// Requires PostgreSQL 18+ (ENRs are only available in PG 18 trigger
/// contexts).
pub static PGS_IVM_USE_ENR: GucSetting<bool> = GucSetting::<bool>::new(false);

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

/// SCAL-1 (v0.31.0): Number of consecutive refresh cycles a change buffer
/// must exceed `pg_trickle.buffer_alert_threshold` before a
/// `change_buffer_backpressure` alert is emitted.
///
/// A value of 1 fires on the first oversized cycle. Higher values suppress
/// transient spikes. Set to 0 to disable back-pressure alerting.
///
/// Default: 3 cycles.
pub static PGS_BACKPRESSURE_CONSECUTIVE_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(3);

// ── v0.27.0 GUCs ──────────────────────────────────────────────────────────

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

/// METR-2 (v0.27.0): Maximum time (milliseconds) allowed for a single metrics
/// HTTP request handler to complete before the connection is closed.
///
/// Protects the scheduler from a slow client stalling the tick loop.
pub static PGS_METRICS_REQUEST_TIMEOUT_MS: GucSetting<i32> = GucSetting::<i32>::new(5000);

// ── v0.33.0: Citus / pg_ripple co-ordination GUCs ────────────────────────────

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

// ── v0.35.0 GUCs ──────────────────────────────────────────────────────────

/// A08 (v0.35.0): When `true`, overrides per-ST `refresh_mode` and forces
/// every stream table to use FULL refresh for the duration the GUC is set.
///
/// Useful for SRE diagnosis when a cluster-wide `refresh_strategy = 'full'`
/// still has DIFFERENTIAL STs due to explicit per-ST row values. Set to
/// `false` (default) to restore normal per-ST scheduling.
pub static PGS_FORCE_FULL_REFRESH: GucSetting<bool> = GucSetting::<bool>::new(false);

/// A07 (v0.35.0): When `true`, CDC trigger bodies return `NULL` (no-op) and
/// the change buffer is not written. Provides a durable hold that survives
/// session reconnects, unlike `pg_trickle.enabled = false` which only stops
/// the scheduler.
///
/// Default: `false` (CDC writes are enabled).
pub static PGS_CDC_PAUSED: GucSetting<bool> = GucSetting::<bool>::new(false);

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

/// F17 (v0.35.0): SLA reporting window in hours for `pgtrickle.sla_summary()`.
///
/// `sla_summary()` computes p50/p99 refresh latency, freshness lag, error rate,
/// and error-budget remaining over this many hours of `pgt_refresh_history`.
///
/// Default: 24 hours.
pub static PGS_SLA_WINDOW_HOURS: GucSetting<i32> = GucSetting::<i32>::new(24);

/// A10 (v0.35.0): Interval in seconds between history pruner sweeps.
///
/// The history pruner deletes rows from `pgtrickle.pgt_refresh_history`
/// older than `history_retention_days` in batches of 10,000 rows to
/// limit lock contention. Set to 0 to use the legacy behaviour (prune
/// once per `HISTORY_CLEANUP_INTERVAL_MS`).
///
/// Default: 60 seconds.
pub static PGS_HISTORY_PRUNE_INTERVAL_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(60);

/// #536: Frontier holdback mode enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontierHoldbackMode {
    /// Probe pg_stat_activity + pg_prepared_xacts and cap to safe LSN (default).
    Xmin,
    /// No holdback — fast but can lose rows under long transactions.
    None,
    /// Hold back the frontier by exactly N bytes (debugging only).
    LsnBytes(u64),
    /// Sentinel: `lsn:<value>` was present but the number failed to parse.
    /// The accessor converts this to `Xmin` after emitting a WARNING.
    InvalidLsn,
}

impl FrontierHoldbackMode {
    /// Return a human-readable representation of the mode.
    /// Unlike `as_str()` on simpler enums, this allocates for `LsnBytes`
    /// to include the actual byte count (e.g. `"lsn:1048576"`).
    pub fn display_string(&self) -> String {
        match self {
            FrontierHoldbackMode::Xmin => "xmin".to_string(),
            FrontierHoldbackMode::None => "none".to_string(),
            FrontierHoldbackMode::LsnBytes(n) => format!("lsn:{n}"),
            FrontierHoldbackMode::InvalidLsn => "invalid".to_string(),
        }
    }
}

pub fn normalize_frontier_holdback_mode(value: Option<String>) -> FrontierHoldbackMode {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("none") => FrontierHoldbackMode::None,
        Some(s) if s.starts_with("lsn:") => {
            let tail = &s["lsn:".len()..];
            match tail.parse::<u64>() {
                Ok(bytes) => FrontierHoldbackMode::LsnBytes(bytes),
                Err(_) => FrontierHoldbackMode::InvalidLsn,
            }
        }
        _ => FrontierHoldbackMode::Xmin,
    }
}

// ── v0.36.0 GUCs ──────────────────────────────────────────────────────────

/// A12 (v0.36.0): Enforce WAL backpressure when slot lag exceeds the critical threshold.
///
/// When `true`, CDC trigger writes are paused when the WAL slot lag exceeds
/// `pg_trickle.slot_lag_critical_threshold_mb`. Writes resume when lag drops
/// below 50% of the threshold. This prevents disk exhaustion at the cost of
/// temporary change-buffer growth.
///
/// Default: `false` (alerts only, no throttling).
pub static PGS_ENFORCE_BACKPRESSURE: GucSetting<bool> = GucSetting::<bool>::new(false);

/// A20 (v0.36.0): Log format for pg_trickle structured log events.
///
/// - `"text"` (default): Unstructured human-readable messages via `pgrx::log!()`.
/// - `"json"`: Structured JSON with fields `event`, `pgt_id`, `cycle_id`,
///   `duration_ms`, `refresh_reason`, `error_code`. Targets OpenTelemetry/Loki.
pub static PGS_LOG_FORMAT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"text"));

/// A35 (v0.36.0): Drain timeout in seconds for `pgtrickle.drain()`.
///
/// Maximum seconds to wait for in-flight refreshes to complete during a drain
/// operation. When the timeout is exceeded, `drain()` returns `false` to
/// indicate that not all refreshes completed before the deadline.
///
/// Default: 60 seconds.
pub static PGS_DRAIN_TIMEOUT: GucSetting<i32> = GucSetting::<i32>::new(60);

/// F5 (v0.36.0): Enable online schema evolution for `ALTER STREAM TABLE EVOLVE`.
///
/// When `true`, type-compatible column additions detected during `ALTER QUERY`
/// are handled by emitting `ALTER TABLE … ADD COLUMN` on the storage table,
/// then re-preparing templates — without a full data flush. Falls back to the
/// standard full reinit path when the evolution is not type-compatible.
///
/// Default: `false` (standard ALTER QUERY reinit behaviour).
pub static PGS_ONLINE_SCHEMA_EVOLUTION: GucSetting<bool> = GucSetting::<bool>::new(false);

/// CORR-2 / UX-3 (v0.36.0): Columnar storage backend for stream tables.
///
/// - `"none"` (default): Heap storage (standard PostgreSQL tables).
/// - `"citus"`: Citus columnar via `CREATE TABLE … USING columnar`.
/// - `"pg_mooncake"`: pg_mooncake columnar tables.
///
/// When set, `create_stream_table()` uses the specified columnar backend and
/// routes differential refresh to the `delete_insert` strategy (columnar
/// backends are append-only).
pub static PGS_COLUMNAR_BACKEND: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"none"));

/// CORR-1 / UX-1 (v0.36.0): Enable temporal IVM (SCD Type 2) for stream tables.
///
/// When `true`, stream tables created with `temporal := true` maintain a
/// two-dimensional frontier `(frontier_lsn, valid_from_ts)`. Each row carries
/// `__pgt_valid_from TIMESTAMPTZ` and `__pgt_valid_to TIMESTAMPTZ`. Rows are
/// never physically deleted; a "close" delta sets `valid_to`.
///
/// Default: `false` (standard non-temporal storage).
pub static PGS_TEMPORAL_STREAM_TABLES: GucSetting<bool> = GucSetting::<bool>::new(false);

/// F4 (v0.37.0): Enable pgVectorMV — incremental vector aggregate operators.
/// When `true`, `avg(vector_col)` and `sum(vector_col)` in stream table defining
/// queries are handled by the DVM engine using a group-rescan strategy so they
/// remain correct under differential refresh. Requires pgvector extension.
/// Default: `false`.
pub static PGS_ENABLE_VECTOR_AGG: GucSetting<bool> = GucSetting::<bool>::new(false);

/// F10 (v0.37.0): Enable W3C Trace Context propagation through the refresh pipeline.
/// When `true`, pg_trickle reads `pg_trickle.trace_id` from the session GUC at
/// CDC capture time and stores it in the change-buffer row. At refresh time, spans
/// are opened and exported via OTLP/gRPC to `pg_trickle.otel_endpoint`.
/// Default: `false`.
pub static PGS_ENABLE_TRACE_PROPAGATION: GucSetting<bool> = GucSetting::<bool>::new(false);

/// F10 (v0.37.0): OTLP/gRPC endpoint for OpenTelemetry span export.
/// Empty string = disabled (no spans emitted).
/// Example: `"http://jaeger:4317"`.
pub static PGS_OTEL_ENDPOINT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

/// F10 (v0.37.0): Session-level W3C traceparent header for trace context propagation.
/// Set in the application session before DML: `SET pg_trickle.trace_id = 'traceparent: ...'`.
pub static PGS_TRACE_ID: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

// ── v0.39.0 GUCs ──────────────────────────────────────────────────────────

/// O39-8 (v0.39.0): CDC capture mode enum.
///
/// Controls what happens when CDC is paused via `pg_trickle.cdc_paused = on`:
///
/// - `"discard"` (default): CDC trigger bodies return `NULL` (no-op); changes
///   that arrive while paused are **dropped**. The stream table must be
///   reinitialized after un-pausing to recover from the data gap. This is the
///   legacy `cdc_paused` behaviour.
///
/// - `"hold"`: Future mode — intended to keep CDC triggers active but pause
///   the scheduler from consuming the change buffer. Changes accumulate in the
///   buffer and are processed when the pause is lifted. **Not yet implemented;**
///   setting this emits a WARNING and falls back to `"discard"`.
///
/// Default: `"discard"`.
///
/// Use `pgtrickle.cdc_capture_mode()` to inspect the active mode at runtime.
pub static PGS_CDC_CAPTURE_MODE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"discard"));

/// O39-8 (v0.39.0): CDC capture mode enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcCaptureMode {
    /// Changes are discarded while paused. Reinit required after un-pause.
    Discard,
    /// (Future) Changes accumulate in the buffer while refreshes are paused.
    Hold,
}

impl CdcCaptureMode {
    pub fn as_str(self) -> &'static str {
        match self {
            CdcCaptureMode::Discard => "discard",
            CdcCaptureMode::Hold => "hold",
        }
    }
}

pub fn normalize_cdc_capture_mode(value: Option<String>) -> CdcCaptureMode {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("hold") => CdcCaptureMode::Hold,
        _ => CdcCaptureMode::Discard,
    }
}

/// A20 (v0.36.0): Log format enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Standard unstructured text messages.
    Text,
    /// Structured JSON with named fields.
    Json,
}

impl LogFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            LogFormat::Text => "text",
            LogFormat::Json => "json",
        }
    }
}

pub fn normalize_log_format(value: Option<String>) -> LogFormat {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("json") => LogFormat::Json,
        _ => LogFormat::Text,
    }
}

/// CORR-2 (v0.36.0): Columnar backend enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnarBackend {
    /// Standard heap storage (default).
    None,
    /// Citus columnar extension.
    Citus,
    /// pg_mooncake columnar tables.
    PgMooncake,
}

impl ColumnarBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnarBackend::None => "none",
            ColumnarBackend::Citus => "citus",
            ColumnarBackend::PgMooncake => "pg_mooncake",
        }
    }

    /// Returns `true` if this backend is append-only (requires `delete_insert` strategy).
    pub fn is_append_only(self) -> bool {
        matches!(self, ColumnarBackend::Citus | ColumnarBackend::PgMooncake)
    }
}

pub fn normalize_columnar_backend(value: Option<String>) -> ColumnarBackend {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("citus") => ColumnarBackend::Citus,
        Some("pg_mooncake") => ColumnarBackend::PgMooncake,
        _ => ColumnarBackend::None,
    }
}

// ── v0.43.0 GUCs ──────────────────────────────────────────────────────────

/// A44-1 (v0.43.0): Maximum number of Scan nodes in the left child for which
/// Part 3 correction term is emitted in inner-join differentiation.
///
/// Raising this value allows more complex join chains to use the algebraically
/// correct correction path rather than falling back to L₁ + coarser correction.
/// Lowering it reduces generated SQL complexity for deeply-nested queries.
///
/// Default: 5 (matches the previously hardcoded `PART3_MAX_SCAN_COUNT`).
/// Range: 1–32.
pub static PGS_PART3_MAX_SCAN_COUNT: GucSetting<i32> = GucSetting::<i32>::new(5);

/// A44-1 (v0.43.0): Maximum number of Scan nodes in a join child before
/// switching from per-leaf L₀/R₀ reconstruction to L₁/R₁ + Part 3 correction.
///
/// At depths above this threshold, per-leaf CTE snapshots generate excessive
/// temp files at large scale factors. The correction path is cheaper and
/// equally correct for pure inner-join chains.
///
/// Default: 4 (matches the previously hardcoded `DEEP_JOIN_L0_SCAN_THRESHOLD`).
/// Range: 1–32.
pub static PGS_DEEP_JOIN_L0_SCAN_THRESHOLD: GucSetting<i32> = GucSetting::<i32>::new(4);

/// A44-3 (v0.43.0): Maximum number of changes fetched per WAL poll cycle.
///
/// Controls the `max_changes` parameter passed to
/// `pg_logical_slot_get_changes()`. Increasing this value raises throughput
/// at the cost of larger per-tick memory usage; decreasing it reduces latency
/// for high-volume sources but increases poll overhead.
///
/// Default: 10 000. Range: 100–1 000 000.
pub static PGS_WAL_MAX_CHANGES_PER_POLL: GucSetting<i32> = GucSetting::<i32>::new(10_000);

/// A44-3 (v0.43.0): Maximum WAL lag bytes before emitting a warning.
///
/// When the decoded WAL lag (bytes between the slot's `restart_lsn` and the
/// current write LSN) exceeds this threshold, a WARNING is emitted and the
/// metric `wal_lag_bytes` is recorded. Set to 0 to disable the warning.
///
/// Default: 65 536 (64 KiB). Range: 0–2 147 483 647.
pub static PGS_WAL_MAX_LAG_BYTES: GucSetting<i32> = GucSetting::<i32>::new(65_536);

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

// ── v0.47.0 GUCs ──────────────────────────────────────────────────────────

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

// ── v0.63.0 GUCs ──────────────────────────────────────────────────────────

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

// ── v0.65.0 GUCs ─────────────────────────────────────────────────────────

/// CDC-6 (v0.65.0): Global default compaction policy for DuckLake change-feed sources.
///
/// Controls what happens when a DuckLake snapshot referenced by a stream table's
/// frontier has been compacted away and is no longer accessible:
///
/// - `"fallback"` (default): Fall back to a full refresh automatically.
/// - `"error"`: Raise an error and halt the refresh until the user reinitializes.
///
/// Individual stream tables may override this with the `ducklake_compaction_policy`
/// column in `pgtrickle.pgt_stream_tables`.
pub static PGS_DUCKLAKE_COMPACTION_POLICY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"fallback"));

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DucklakeCompactionPolicy {
    Fallback,
    Error,
}

impl DucklakeCompactionPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            DucklakeCompactionPolicy::Fallback => "fallback",
            DucklakeCompactionPolicy::Error => "error",
        }
    }
}

fn normalize_ducklake_compaction_policy(value: Option<String>) -> DucklakeCompactionPolicy {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("error") => DucklakeCompactionPolicy::Error,
        _ => DucklakeCompactionPolicy::Fallback,
    }
}

// ── v0.66.0: DuckLake sink GUCs ───────────────────────────────────────────

/// F-4 (v0.66.0): Parquet compression codec for the DuckLake sink.
/// Default: 'snappy'.
pub static PGS_DUCKLAKE_SINK_COMPRESSION: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"snappy"));

/// S3 endpoint URL override for the DuckLake sink.
/// Empty = use the default AWS endpoint.
pub static PGS_DUCKLAKE_SINK_S3_ENDPOINT: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

/// AWS S3 region for the DuckLake sink (default: 'us-east-1').
pub static PGS_DUCKLAKE_SINK_S3_REGION: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"us-east-1"));

/// AWS S3 access key ID for the DuckLake sink (empty = use credential chain).
pub static PGS_DUCKLAKE_SINK_S3_ACCESS_KEY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

/// AWS S3 secret access key for the DuckLake sink (empty = use credential chain).
pub static PGS_DUCKLAKE_SINK_S3_SECRET_KEY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

/// F-9 (v0.66.0): Key-name prefix for per-file Parquet encryption keys.
/// Empty = encryption disabled.
pub static PGS_DUCKLAKE_SINK_ENCRYPTION_KEY_PREFIX: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(None);

// ── v0.69.0: DuckLake sink reliability & security GUCs ───────────────────

/// ARCH-002/REL-001 (v0.69.0): Maximum number of retryable delivery attempts
/// for a DuckLake sink write before transitioning to FAILED_PERMANENT.
///
/// Default: 3. When `run_ducklake_sink()` fails with a transient error, the
/// delivery row status is set to `FAILED_RETRYABLE` and the attempt count is
/// incremented. Once `attempt_count >= ducklake_sink_max_retries`, the status
/// transitions to `FAILED_PERMANENT`.
pub static PGS_DUCKLAKE_SINK_MAX_RETRIES: GucSetting<i32> = GucSetting::<i32>::new(3);

/// ARCH-002/REL-001 (v0.69.0): What to do when a DuckLake sink delivery
/// reaches `FAILED_PERMANENT` status.
///
/// - `"warn"` (default): emit a PostgreSQL WARNING and continue. The stream
///   table stays ACTIVE so future refreshes still attempt delivery.
/// - `"error"`: propagate as a PostgreSQL error. The refresh cycle that
///   triggered the delivery will be marked FAILED.
pub static PGS_DUCKLAKE_SINK_FAILURE_MODE: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"warn"));

/// SEC-002 (v0.69.0): Schema name for DuckLake catalog tables.
///
/// Default: `"main"`. When DuckLake tables (`ducklake_view`, `ducklake_snapshot`,
/// `ducklake_data_file`, `ducklake_table_stats`) live in a schema other than
/// the current `search_path`, set this GUC to their fully-qualified schema so
/// that catalog writes are always directed to the correct namespace regardless
/// of `search_path` manipulation.
pub static PGS_DUCKLAKE_CATALOG_SCHEMA: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"main"));

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

/// Register all GUC variables. Called from `_PG_init()`.
pub fn register_gucs() {
    GucRegistry::define_bool_guc(
        c"pg_trickle.enabled",
        c"Master enable/disable switch for pgtrickle.",
        c"When false, the scheduler will not run and no refreshes will be triggered.",
        &PGS_ENABLED,
        GucContext::Suset,
        GucFlags::default(),
    );

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

    GucRegistry::define_string_guc(
        c"pg_trickle.change_buffer_schema",
        c"Schema name for change buffer tables.",
        c"CDC change data is stored in tables within this schema.",
        &PGS_CHANGE_BUFFER_SCHEMA,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.max_concurrent_refreshes",
        c"Maximum active refresh workers per database coordinator.",
        c"Limits the number of concurrent refresh operations within a single database. \
           In sequential mode (parallel_refresh_mode=off) this has no effect. \
           In parallel mode, the coordinator will not dispatch more than this many \
           concurrent refresh workers for one database.",
        &PGS_MAX_CONCURRENT_REFRESHES,
        1,  // min
        32, // max
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

    // WM-7: Watermark holdback timeout — seconds before a watermark is "stuck".
    GucRegistry::define_int_guc(
        c"pg_trickle.watermark_holdback_timeout",
        c"Seconds before an un-advanced watermark is considered stuck (0 = disabled).",
        c"When non-zero, the scheduler periodically checks all watermark sources. \
           If any source in a watermark group has not advanced within this many seconds, \
           downstream stream tables in that group are paused and a pgtrickle_alert \
           notification with category watermark_stuck is emitted. Set to 0 to disable.",
        &PGS_WATERMARK_HOLDBACK_TIMEOUT,
        0,      // min (0 = disabled)
        86_400, // max (24 hours)
        GucContext::Suset,
        GucFlags::default(),
    );

    // PH-E2: Spill detection threshold.
    GucRegistry::define_int_guc(
        c"pg_trickle.spill_threshold_blocks",
        c"Temp blocks written threshold for spill detection (0 = disabled).",
        c"After each differential MERGE, queries pg_stat_statements for temp_blks_written. \
           If the value exceeds this threshold, the refresh is a spill. After \
           spill_consecutive_limit consecutive spills, forces FULL refresh. \
           Requires pg_stat_statements. Set to 0 to disable.",
        &PGS_SPILL_THRESHOLD_BLOCKS,
        0,           // min (0 = disabled)
        100_000_000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // PH-E2: Consecutive spill limit before FULL fallback.
    GucRegistry::define_int_guc(
        c"pg_trickle.spill_consecutive_limit",
        c"Consecutive spilling refreshes before auto-switching to FULL (default 3).",
        c"When a stream table has this many consecutive differential refreshes with \
           temp_blks_written exceeding spill_threshold_blocks, the scheduler forces \
           a FULL refresh on the next cycle. Resets after any non-spilling refresh.",
        &PGS_SPILL_CONSECUTIVE_LIMIT,
        1,   // min
        100, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.cleanup_use_truncate",
        c"Use TRUNCATE for change buffer cleanup when all rows are consumed.",
        c"When true and the entire change buffer is consumed by a refresh, uses TRUNCATE (O(1)) instead of per-row DELETE. Disable if the AccessExclusiveLock is problematic.",
        &PGS_CLEANUP_USE_TRUNCATE,
        GucContext::Suset,
        GucFlags::default(),
    );

    // C4: Consolidated planner aggressiveness switch (v0.14.0).
    GucRegistry::define_bool_guc(
        c"pg_trickle.planner_aggressive",
        c"Enable all planner hints for MERGE execution (consolidates merge_planner_hints + merge_work_mem_mb).",
        c"When true (default), disables nested-loop joins and raises work_mem for medium/large \
           delta sizes to stabilise P95 latency. Replaces the deprecated merge_planner_hints \
           and merge_work_mem_mb GUCs.",
        &PGS_PLANNER_AGGRESSIVE,
        GucContext::Suset,
        GucFlags::default(),
    );

    // Deprecated: kept for backward compatibility.
    GucRegistry::define_bool_guc(
        c"pg_trickle.merge_planner_hints",
        c"Deprecated — use pg_trickle.planner_aggressive instead.",
        c"Deprecated in v0.14.0. When explicitly set, emits a deprecation warning. \
           Use pg_trickle.planner_aggressive instead.",
        &PGS_MERGE_PLANNER_HINTS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.merge_work_mem_mb",
        c"work_mem (MB) for large-delta MERGE execution.",
        c"Applied via SET LOCAL when planner_aggressive is enabled and the delta exceeds 10 000 rows.",
        &PGS_MERGE_WORK_MEM_MB,
        8,    // min
        4096, // max (4 GB)
        GucContext::Suset,
        GucFlags::default(),
    );

    // SCAL-3: Delta working-set memory cap.
    GucRegistry::define_int_guc(
        c"pg_trickle.delta_work_mem_cap_mb",
        c"Max work_mem (MB) allowed during delta MERGE (0 = no cap).",
        c"When the planner hints would set work_mem above this cap, the refresh \
           falls back to FULL instead of executing a potentially OOM-inducing delta \
           MERGE. Set to 0 to disable. Recommended: 128–1024.",
        &PGS_DELTA_WORK_MEM_CAP_MB,
        0,    // min (0 = disabled)
        8192, // max (8 GB)
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.use_prepared_statements",
        c"Use SQL PREPARE/EXECUTE for MERGE during differential refresh.",
        c"When true, the first cache-hit cycle PREPAREs the MERGE statement and subsequent cycles EXECUTE it. Saves 1-2ms of parse/plan overhead. Disable if plan-parameter sniffing causes poor plans.",
        &PGS_USE_PREPARED_STATEMENTS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.user_triggers",
          c"User-trigger handling: auto or off.",
          c"'auto' detects row-level user triggers and switches to explicit DML so they fire correctly. \
              'off' always uses MERGE (triggers will NOT fire correctly). \
              'on' is accepted as a deprecated alias for 'auto'.",
        &PGS_USER_TRIGGERS,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.cdc_mode",
        c"CDC mechanism: auto (default), trigger, or wal.",
        c"'auto' (default) uses triggers initially and transitions to WAL-based CDC \
           if wal_level=logical, falling back to triggers on error. \
           'trigger' always uses row-level triggers for change capture. \
           'wal' requires wal_level=logical (fails otherwise).",
        &PGS_CDC_MODE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.wal_transition_timeout",
        c"Max seconds for WAL decoder catch-up during CDC transition.",
        c"When transitioning from trigger-based to WAL-based CDC, the WAL decoder must catch up \
           past the trigger's last captured LSN. If it hasn't caught up within this timeout, \
           the system falls back to trigger-based CDC.",
        &PGS_WAL_TRANSITION_TIMEOUT,
        10,    // min: 10 seconds
        3_600, // max: 1 hour
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.slot_lag_warning_threshold_mb",
        c"WAL slot lag warning threshold in MB.",
        c"When a pg_trickle WAL replication slot retains more than this much WAL, \
           the scheduler emits a slot_lag_warning NOTIFY event and pgtrickle.health_check() \
           reports WARN for slot_lag.",
        &PGS_SLOT_LAG_WARNING_THRESHOLD_MB,
        1,
        1_048_576,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.slot_lag_critical_threshold_mb",
        c"WAL slot lag critical threshold in MB.",
        c"When a pg_trickle WAL replication slot retains more than this much WAL, \
           pgtrickle.check_cdc_health() reports slot_lag_exceeds_threshold for the source.",
        &PGS_SLOT_LAG_CRITICAL_THRESHOLD_MB,
        1,
        1_048_576,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.block_source_ddl",
        c"Block column-altering DDL on source tables used by stream tables.",
        c"When true (default), ALTER TABLE that adds, drops, renames, or changes the type \
           of a column on a source table will ERROR instead of triggering reinitialization. \
           Benign DDL (indexes, comments, statistics) and constraint changes are always allowed. \
           Set to false to allow schema changes (the stream table will be reinitialized on the \
           next scheduler tick). Use ALTER STREAM TABLE to update the query before re-enabling.",
        &PGS_BLOCK_SOURCE_DDL,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.buffer_alert_threshold",
        c"Buffer growth alert threshold (pending change row count).",
        c"When a source table's change buffer exceeds this many rows, a BufferGrowthWarning \
           alert is emitted. Raise for high-throughput workloads, lower for small tables.",
        &PGS_BUFFER_ALERT_THRESHOLD,
        1_000,       // min: 1000 rows
        100_000_000, // max: 100M rows
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.compact_threshold",
        c"Change buffer compaction threshold (pending change row count).",
        c"When a source table's pending changes exceed this count, compaction removes \
           net-zero INSERT+DELETE pairs and collapses multi-change groups. Set to 0 to disable.",
        &PGS_COMPACT_THRESHOLD,
        0,           // min: 0 (disabled)
        100_000_000, // max: 100M rows
        GucContext::Suset,
        GucFlags::default(),
    );

    // BUF-LIMIT: Hard limit on change buffer rows per source table.
    GucRegistry::define_int_guc(
        c"pg_trickle.max_buffer_rows",
        c"Hard limit on change buffer rows per source table (0 = unlimited).",
        c"When a source table's change buffer exceeds this many rows at refresh time, \
           pg_trickle falls back to FULL refresh and truncates the buffer. Prevents \
           unbounded disk growth when differential refresh fails repeatedly.",
        &PGS_MAX_BUFFER_ROWS,
        0,           // min: 0 (disabled)
        100_000_000, // max: 100M rows
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

    // B-1: Aggregate fast-path.
    GucRegistry::define_bool_guc(
        c"pg_trickle.aggregate_fast_path",
        c"Use explicit DML instead of MERGE for all-algebraic aggregate stream tables.",
        c"When true (default), stream tables whose aggregates are all algebraically invertible \
           (COUNT, SUM, AVG, STDDEV, etc.) use the targeted DELETE+UPDATE+INSERT path instead \
           of MERGE, avoiding the hash-join cost. Set to false to force MERGE for all stream \
           tables.",
        &PGS_AGGREGATE_FAST_PATH,
        GucContext::Suset,
        GucFlags::default(),
    );

    // G14-SHC: Cross-backend template cache.
    GucRegistry::define_bool_guc(
        c"pg_trickle.template_cache",
        c"Enable the cross-backend delta template cache.",
        c"When true (default), delta SQL templates are persisted in an UNLOGGED catalog table \
           so that new backends skip the ~45 ms DVM parse+differentiate step. \
           Set to false to always regenerate templates from scratch.",
        &PGS_TEMPLATE_CACHE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.max_grouping_set_branches",
        c"Maximum allowed grouping set branches in CUBE/ROLLUP queries.",
        c"Prevents parsing memory exhaustion during combinatorial expansion. \
           Raise if you need more than 64 grouping set branches.",
        &PGS_MAX_GROUPING_SET_BRANCHES,
        1,
        65536,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.max_parse_depth",
        c"Maximum recursion depth for the query parser tree visitors.",
        c"Prevents stack-overflow crashes on pathological queries with deeply \
           nested subqueries, CTEs, or set operations. Returns a QueryTooComplex \
           error when the limit is exceeded. Raise only if legitimate queries \
           exceed the default.",
        &PGS_MAX_PARSE_DEPTH,
        1,
        10000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.max_diff_ctes",
        c"Maximum number of CTEs the differential query generator may produce.",
        c"Guards against unbounded memory growth from pathological queries with \
           many operators, joins, and set operations. Returns a DiffCteCountExceeded \
           error when the limit is exceeded. The default of 1000 is well above any \
           realistic query requirement.",
        &PGS_MAX_DIFF_CTES,
        10,
        100000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.ivm_topk_max_limit",
        c"Maximum LIMIT for TopK stream tables in IMMEDIATE mode.",
        c"TopK queries exceeding this LIMIT are rejected in IMMEDIATE mode. \
           Set to 0 to disable TopK in IMMEDIATE mode entirely.",
        &PGS_IVM_TOPK_MAX_LIMIT,
        0,
        1_000_000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.ivm_recursive_max_depth",
        c"Maximum recursion depth for WITH RECURSIVE CTEs in IMMEDIATE mode.",
        c"Limits the depth counter injected into semi-naive delta queries to guard \
           against infinite loops from cyclic data or very deep hierarchies inside \
           trigger bodies. Set to 0 to disable the guard (allow unlimited recursion).",
        &PGS_IVM_RECURSIVE_MAX_DEPTH,
        0,
        100_000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"pg_trickle.buffer_partitioning",
        c"Buffer table partitioning mode: off, on, or auto.",
        c"'off' uses unpartitioned heap tables (default). \
           'on' always uses PARTITION BY RANGE (lsn) for change buffers. \
           'auto' enables partitioning for sources with refresh cycles >= 30s.",
        &PGS_BUFFER_PARTITIONING,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.foreign_table_polling",
        c"Enable polling-based CDC for foreign tables.",
        c"When true, foreign tables in defining queries are supported via \
           snapshot-comparison. A local shadow table stores the previous state; \
           EXCEPT ALL computes the delta on each refresh cycle.",
        &PGS_FOREIGN_TABLE_POLLING,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pg_trickle.matview_polling",
        c"Enable polling-based CDC for materialized views.",
        c"When true, materialized views in defining queries are supported via \
           snapshot-comparison (same mechanism as foreign table polling). \
           A local shadow table stores the previous state; EXCEPT ALL computes \
           the delta on each refresh cycle.",
        &PGS_MATVIEW_POLLING,
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

    GucRegistry::define_string_guc(
        c"pg_trickle.cdc_trigger_mode",
        c"CDC trigger granularity: statement (default) or row.",
        c"'statement' uses statement-level AFTER triggers with transition tables \
           (NEW TABLE / OLD TABLE). A single invocation per DML statement processes \
           all affected rows in one bulk INSERT … SELECT, giving 50–80% less \
           write-side overhead for bulk UPDATE/DELETE. Single-row DML is unaffected. \
           'row' uses legacy per-row triggers (pg_trickle < 0.4.0 behaviour). \
           Changing this setting takes effect for newly installed CDC triggers. \
           Call pgtrickle.rebuild_cdc_triggers() to migrate existing stream tables.",
        &PGS_CDC_TRIGGER_MODE,
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
        c"pg_trickle.algebraic_drift_reset_cycles",
        c"Differential cycles between automatic full recomputes for algebraic aggregates.",
        c"After this many differential refresh cycles, stream tables with algebraic \
           aggregates (AVG, STDDEV, VAR) are automatically reinitialized to reset \
           accumulated floating-point drift in auxiliary columns. 0 disables.",
        &PGS_ALGEBRAIC_DRIFT_RESET_CYCLES,
        0,       // min (disabled)
        100_000, // max
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

    GucRegistry::define_float_guc(
        c"pg_trickle.merge_seqscan_threshold",
        c"Delta-to-ST ratio below which sequential scans are disabled for MERGE.",
        c"When the delta row count is below this fraction of the stream table size, \
           SET LOCAL enable_seqscan = off is applied before MERGE to favor index \
           lookups. Set to 0.0 to disable.",
        &PGS_MERGE_SEQSCAN_THRESHOLD,
        0.0, // min (disabled)
        1.0, // max
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
        c"pg_trickle.log_merge_sql",
        c"Log the generated MERGE SQL template on every refresh cycle.",
        c"When true, the MERGE SQL template built during differential refresh is \
           emitted to the PostgreSQL server log at LOG level. Intended for debugging \
           MERGE query generation only. Do not enable in production.",
        &PGS_LOG_MERGE_SQL,
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

    GucRegistry::define_float_guc(
        c"pg_trickle.delta_amplification_threshold",
        c"Delta amplification detection threshold (output/input ratio).",
        c"When a DIFFERENTIAL refresh produces more than this multiple of the input \
           delta rows, a WARNING is emitted so operators can identify pathological \
           join fan-out or many-to-many amplification. Set to 0.0 to disable.",
        &PGS_DELTA_AMPLIFICATION_THRESHOLD,
        0.0,       // min (disabled)
        100_000.0, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // DIAG-2: Aggregate cardinality warning threshold.
    GucRegistry::define_int_guc(
        c"pg_trickle.agg_diff_cardinality_threshold",
        c"Estimated GROUP BY cardinality threshold for algebraic aggregate warnings.",
        c"At create_stream_table time, if the defining query uses algebraic aggregates \
           (SUM, COUNT, AVG) in DIFFERENTIAL mode and the estimated group cardinality \
           is below this threshold, a WARNING is emitted suggesting FULL or AUTO mode. \
           Set to 0 to disable.",
        &PGS_AGG_DIFF_CARDINALITY_THRESHOLD,
        0,           // min (disabled)
        100_000_000, // max
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

    // VOL-1: Volatile function policy.
    GucRegistry::define_string_guc(
        c"pg_trickle.volatile_function_policy",
        c"Volatile function policy: reject (default), warn, or allow.",
        c"'reject' (default) errors on volatile functions in DIFFERENTIAL/IMMEDIATE queries. \
           'warn' emits a WARNING but allows creation. \
           'allow' permits volatile functions silently. Volatile functions produce different \
           values on each evaluation, which may break delta computation.",
        &PGS_VOLATILE_FUNCTION_POLICY,
        GucContext::Suset,
        GucFlags::default(),
    );

    // PH-D2: Merge join strategy override.
    GucRegistry::define_string_guc(
        c"pg_trickle.merge_join_strategy",
        c"Join strategy hint for MERGE: auto (default), hash_join, nested_loop, merge_join.",
        c"'auto' (default) uses delta-size heuristics to choose between nested-loop and \
           hash-join hints. 'hash_join' always disables nestloop and raises work_mem. \
           'nested_loop' always disables hashjoin and mergejoin. \
           'merge_join' always disables hashjoin and nestloop.",
        &PGS_MERGE_JOIN_STRATEGY,
        GucContext::Suset,
        GucFlags::default(),
    );

    // D-1a: UNLOGGED change buffers.
    GucRegistry::define_bool_guc(
        c"pg_trickle.unlogged_buffers",
        c"Create new change buffer tables as UNLOGGED to reduce WAL amplification.",
        c"When true, new change buffer tables are UNLOGGED (no WAL writes). \
           Reduces CDC WAL amplification by ~30% but buffers are lost on crash. \
           After crash, affected stream tables receive an automatic FULL refresh. \
           Existing buffers are not changed; use pgtrickle.convert_buffers_to_unlogged() \
           to convert them. Default: false (crash-safe, WAL-logged).",
        &PGS_UNLOGGED_BUFFERS,
        GucContext::Suset,
        GucFlags::default(),
    );

    // DUR-2: Change buffer durability mode.
    GucRegistry::define_string_guc(
        c"pg_trickle.change_buffer_durability",
        c"Change buffer durability: unlogged (default), logged, or sync.",
        c"'unlogged' (default) creates UNLOGGED change buffers for max throughput; \
           lost on crash (auto FULL refresh on recovery). \
           'logged' creates WAL-logged change buffers; survives crash, replicated. \
           'sync' adds synchronous_commit for maximum durability. \
           Supersedes pg_trickle.unlogged_buffers (compatibility alias).",
        &PGS_CHANGE_BUFFER_DURABILITY,
        GucContext::Suset,
        GucFlags::default(),
    );

    // PH-D1: MERGE strategy override.
    GucRegistry::define_string_guc(
        c"pg_trickle.merge_strategy",
        c"Delta apply strategy: auto (default) or merge.",
        c"'auto' (default) uses DELETE+INSERT for sub-1% deltas (delta_rows / target_rows \
           below merge_strategy_threshold) and MERGE otherwise. \
           'merge' always uses the MERGE statement. \
           The former 'delete_insert' value was removed in v0.19.0 (CORR-1); \
           setting it logs a WARNING and falls back to 'auto'.",
        &PGS_MERGE_STRATEGY,
        GucContext::Suset,
        GucFlags::default(),
    );

    // PH-D1: Merge strategy threshold.
    GucRegistry::define_float_guc(
        c"pg_trickle.merge_strategy_threshold",
        c"Delta ratio threshold for auto merge_strategy (default: 0.01 = 1%).",
        c"When merge_strategy = 'auto', DELETE+INSERT is used instead of MERGE when \
           delta_rows / target_rows is below this threshold. Higher values cause more \
           refreshes to use DELETE+INSERT. Range: 0.001 to 1.0.",
        &PGS_MERGE_STRATEGY_THRESHOLD,
        0.001, // min
        1.0,   // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // STAB-1: Cluster-wide connection pooler mode.
    GucRegistry::define_string_guc(
        c"pg_trickle.connection_pooler_mode",
        c"Cluster-wide connection pooler compatibility mode: off (default), transaction, session.",
        c"'off' — per-ST pooler_compatibility_mode governs. \
           'transaction' — globally disable prepared-statement reuse and suppress \
           NOTIFY emissions for PgBouncer transaction-pool compatibility. \
           'session' — explicit opt-in to session mode (same as off today).",
        &PGS_CONNECTION_POOLER_MODE,
        GucContext::Suset,
        GucFlags::default(),
    );

    // DB-5: History retention in days.
    GucRegistry::define_int_guc(
        c"pg_trickle.history_retention_days",
        c"Number of days to retain rows in pgt_refresh_history (default: 90).",
        c"The scheduler runs a daily cleanup that deletes rows from \
           pgtrickle.pgt_refresh_history older than this many days. \
           Set to 0 to disable automatic cleanup (history grows unbounded).",
        &PGS_HISTORY_RETENTION_DAYS,
        0,      // min (disabled)
        36_500, // max (~100 years)
        GucContext::Suset,
        GucFlags::default(),
    );

    // DF-G1: Dog-feeding auto-apply policy.
    GucRegistry::define_string_guc(
        c"pg_trickle.self_monitoring_auto_apply",
        c"Dog-feeding auto-apply policy: off (default), threshold_only, full.",
        c"Controls whether the self-monitoring analytics stream tables can \
           automatically adjust stream table configuration. \
           'off' — advisory only (no automatic changes). \
           'threshold_only' — auto-apply threshold recommendations from \
           df_threshold_advice when confidence is HIGH and delta > 5%%. \
           'full' — also apply scheduling hints from df_scheduling_interference.",
        &PGS_SELF_MONITORING_AUTO_APPLY,
        GucContext::Suset,
        GucFlags::default(),
    );

    // OP-2: Prometheus metrics HTTP port.
    GucRegistry::define_int_guc(
        c"pg_trickle.metrics_port",
        c"TCP port for the Prometheus/OpenMetrics endpoint served by the scheduler (0 = off).",
        c"When non-zero, the per-database scheduler exposes all pg_trickle monitoring \
           metrics at GET /metrics on this port.  Default 0 disables the endpoint.",
        &PGS_METRICS_PORT,
        0,     // min
        65535, // max
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

    // ── v0.23.0: TPC-H DVM Scaling Performance GUCs ────────────────────

    // P1-2: Delta SQL logging.
    GucRegistry::define_bool_guc(
        c"pg_trickle.log_delta_sql",
        c"Log generated delta SQL at DEBUG1 level (diagnostic only).",
        c"When true, the full delta SQL generated by the DVM engine is logged \
           before execution. Allows EXPLAIN (ANALYZE, BUFFERS) on captured SQL. \
           Do NOT enable in production.",
        &PGS_LOG_DELTA_SQL,
        GucContext::Suset,
        GucFlags::default(),
    );

    // P5-1: Delta work_mem override.
    GucRegistry::define_int_guc(
        c"pg_trickle.delta_work_mem",
        c"work_mem (MB) for delta SQL execution (0 = inherit session work_mem).",
        c"When non-zero, SET LOCAL work_mem is applied before running the \
           delta SQL. Allows tuning delta execution memory independently \
           of session work_mem without a server restart.",
        &PGS_DELTA_WORK_MEM,
        0,    // min (0 = disabled)
        8192, // max (8 GB)
        GucContext::Suset,
        GucFlags::default(),
    );

    // P5-2: Delta nestloop control.
    GucRegistry::define_bool_guc(
        c"pg_trickle.delta_enable_nestloop",
        c"Allow nested-loop joins during delta SQL execution (default on).",
        c"When false, SET LOCAL enable_nestloop = off is applied before running \
           the delta SQL. Useful for diagnosing planner regressions on large \
           right-side joins.",
        &PGS_DELTA_ENABLE_NESTLOOP,
        GucContext::Suset,
        GucFlags::default(),
    );

    // PERF-5: ANALYZE change buffer before delta execution.
    GucRegistry::define_bool_guc(
        c"pg_trickle.analyze_before_delta",
        c"Run ANALYZE on change buffer tables before delta SQL execution.",
        c"When true (default), ANALYZE is run on each source's change buffer \
           before executing the delta SQL. Ensures accurate row count estimates \
           since auto-analyze never fires on truncated-and-refilled buffers.",
        &PGS_ANALYZE_BEFORE_DELTA,
        GucContext::Suset,
        GucFlags::default(),
    );

    // SCAL-2: Change buffer overflow alert threshold.
    GucRegistry::define_int_guc(
        c"pg_trickle.max_change_buffer_alert_rows",
        c"Change buffer row count alert threshold (0 = disabled).",
        c"When non-zero, emits a pg_trickle_alert change_buffer_overflow event \
           if any source's change buffer exceeds this row count during refresh.",
        &PGS_MAX_CHANGE_BUFFER_ALERT_ROWS,
        0,           // min (0 = disabled)
        100_000_000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // UX-7: DIFF output format for aggregate UPDATE-splits.
    GucRegistry::define_string_guc(
        c"pg_trickle.diff_output_format",
        c"DIFF output format for aggregate UPDATE-splits: split or merged.",
        c"'split' (default): emit DELETE+INSERT pairs for aggregate UPDATEs. \
           'merged': re-combine into UPDATE rows for backward compatibility.",
        &PGS_DIFF_OUTPUT_FORMAT,
        GucContext::Suset,
        GucFlags::default(),
    );

    // #536: Frontier visibility holdback GUCs.
    GucRegistry::define_string_guc(
        c"pg_trickle.frontier_holdback_mode",
        c"Frontier holdback mode to prevent silent data loss from long-running transactions.",
        c"'xmin' (default): probe pg_stat_activity + pg_prepared_xacts once per tick and \
           cap the frontier to the safe upper bound, preventing change-buffer rows from \
           uncommitted transactions from being silently skipped. \
           'none': no holdback (fast but can lose rows under long-lived transactions). \
           'lsn:<N>': hold back by exactly N bytes (debugging only).",
        &PGS_FRONTIER_HOLDBACK_MODE,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.frontier_holdback_warn_seconds",
        c"Emit a WARNING when frontier holdback exceeds this many seconds (0 = disabled).",
        c"When a long-running or forgotten transaction keeps the scheduler from advancing \
           the frontier for longer than this many seconds, a WARNING is emitted at most \
           once per minute to help operators identify the blocking session. \
           Set to 0 to disable the warning.",
        &PGS_FRONTIER_HOLDBACK_WARN_SECONDS,
        0,    // min (0 = disabled)
        3600, // max (1 hour)
        GucContext::Suset,
        GucFlags::default(),
    );

    // ── v0.25.0 GUCs ─────────────────────────────────────────────────────────

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

    GucRegistry::define_int_guc(
        c"pg_trickle.template_cache_max_entries",
        c"CACHE-2: Maximum L1 template cache entries per backend (0 = unbounded).",
        c"When the cache reaches this limit, the least-recently-used entry is evicted. \
           Set to 0 for unbounded cache (default).",
        &PGS_TEMPLATE_CACHE_MAX_ENTRIES,
        0,     // min (0 = unbounded)
        65536, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.template_cache_max_bytes",
        c"PERF-006: Maximum L1 template cache memory per backend in bytes (0 = disabled).",
        c"When > 0, cache inserts evict least-recently-used entries until the total \
           estimated template bytes fit under this cap.",
        &PGS_TEMPLATE_CACHE_MAX_BYTES,
        0,
        2_147_483_647,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.frontier_holdback_probe_cache_ms",
        c"PERF-003: Holdback probe cache interval in milliseconds (0 = disabled).",
        c"When > 0, reuses the previous xmin-holdback probe result for up to this \
           many milliseconds to reduce catalog-scan overhead.",
        &PGS_FRONTIER_HOLDBACK_PROBE_CACHE_MS,
        0,
        60_000,
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.publication_lag_warn_bytes",
        c"PUB-1: Emit WARNING when subscriber WAL lag exceeds this many bytes (0 = disabled).",
        c"When a downstream publication subscriber's confirmed_flush_lsn lags behind \
           the change buffer by more than this many bytes, a WARNING is emitted and \
           the change buffer truncation is deferred. Set to 0 to disable (default).",
        &PGS_PUBLICATION_LAG_WARN_BYTES,
        0,             // min (0 = disabled)
        2_147_483_647, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // ── v0.30.0 GUCs ──────────────────────────────────────────────────────

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

    GucRegistry::define_int_guc(
        c"pg_trickle.template_cache_max_age_hours",
        c"STAB-3: Maximum age (hours) for L2 catalog template cache entries (0 = no age purge).",
        c"Entries older than this threshold are deleted during the scheduler launcher tick. \
           Prevents accumulation of stale entries after ALTER QUERY without DROP. \
           Default: 168 hours (7 days). Set to 0 to disable age-based purging.",
        &PGS_TEMPLATE_CACHE_MAX_AGE_HOURS,
        0,      // min (0 = disabled)
        87_600, // max (10 years)
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.max_parse_nodes",
        c"PERF-2: Maximum parse tree nodes per query (0 = unlimited).",
        c"Queries with more than this many nodes are rejected with QueryTooComplex to prevent \
           unbounded memory allocation. Does not apply to queries already registered. \
           Default: 0 (unlimited). Recommended: 100000 for production deployments.",
        &PGS_MAX_PARSE_NODES,
        0,          // min (0 = unlimited)
        10_000_000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // ── v0.31.0 GUCs ──────────────────────────────────────────────────────

    GucRegistry::define_bool_guc(
        c"pg_trickle.ivm_use_enr",
        c"PERF-4: Use ENR-based transition tables in IVM trigger bodies (PG18+).",
        c"When true, IMMEDIATE-mode trigger functions reference ENRs directly \
           instead of copying transition data to temp tables. \
           Requires PostgreSQL 18+ with ENR propagation to nested SPI calls. \
           Defaults to false (legacy temp-table approach) for compatibility.",
        &PGS_IVM_USE_ENR,
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

    // ── v0.27.0 GUCs ──────────────────────────────────────────────────────

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

    GucRegistry::define_int_guc(
        c"pg_trickle.metrics_request_timeout_ms",
        c"METR-2: Maximum milliseconds for a single metrics HTTP request handler.",
        c"When the metrics endpoint takes longer than this to respond, the connection \
           is dropped. Protects the scheduler tick loop from slow HTTP clients.",
        &PGS_METRICS_REQUEST_TIMEOUT_MS,
        0,       // min (0 = no timeout)
        600_000, // max (10 minutes)
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

    // ── v0.35.0 GUCs ────────────────────────────────────────────────────

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

    // A07: CDC kill switch (durable pause).
    GucRegistry::define_bool_guc(
        c"pg_trickle.cdc_paused",
        c"A07: Pause CDC trigger writes cluster-wide (durable hold).",
        c"When true, CDC trigger bodies return NULL immediately without writing to \
          the change buffer. Survives session reconnects unlike pg_trickle.enabled. \
          Default false (CDC writes enabled).",
        &PGS_CDC_PAUSED,
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

    // F17: SLA reporting window.
    GucRegistry::define_int_guc(
        c"pg_trickle.sla_window_hours",
        c"F17: History window in hours for pgtrickle.sla_summary() computations.",
        c"sla_summary() aggregates pgt_refresh_history over this many hours to compute \
          p50/p99 latency, freshness lag, error rate, and error-budget remaining.",
        &PGS_SLA_WINDOW_HOURS,
        1,     // min
        8_760, // max (1 year)
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

    // ── v0.36.0 GUCs ─────────────────────────────────────────────────────────

    // A12: WAL backpressure enforcement.
    GucRegistry::define_bool_guc(
        c"pg_trickle.enforce_backpressure",
        c"A12: Pause CDC writes when WAL slot lag exceeds the critical threshold.",
        c"When true, CDC trigger writes pause when the WAL slot lag exceeds \
          pg_trickle.slot_lag_critical_threshold_mb. Resumes when lag drops below 50% \
          of the threshold. Default false (alerts only, no throttling).",
        &PGS_ENFORCE_BACKPRESSURE,
        GucContext::Suset,
        GucFlags::default(),
    );

    // A20: Structured JSON logging.
    GucRegistry::define_string_guc(
        c"pg_trickle.log_format",
        c"A20: Log format for pg_trickle events: text (default) or json.",
        c"'text' emits standard human-readable log messages. \
          'json' emits structured JSON with fields event, pgt_id, cycle_id, \
          duration_ms, refresh_reason, error_code for OpenTelemetry/Loki integration.",
        &PGS_LOG_FORMAT,
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

    // F5: Online schema evolution.
    GucRegistry::define_bool_guc(
        c"pg_trickle.online_schema_evolution",
        c"F5: Enable online schema evolution for ALTER STREAM TABLE EVOLVE.",
        c"When true, type-compatible column additions during ALTER QUERY are handled \
          by ALTER TABLE ADD COLUMN + template re-prepare instead of full reinit. \
          Falls back to full reinit when changes are not type-compatible. \
          Default false (opt-in).",
        &PGS_ONLINE_SCHEMA_EVOLUTION,
        GucContext::Suset,
        GucFlags::default(),
    );

    // CORR-2 / UX-3: Columnar storage backend.
    GucRegistry::define_string_guc(
        c"pg_trickle.columnar_backend",
        c"CORR-2: Columnar storage backend: none (default), citus, or pg_mooncake.",
        c"'none' (default) uses standard heap tables. \
          'citus' uses Citus columnar (CREATE TABLE ... USING columnar). \
          'pg_mooncake' uses pg_mooncake columnar tables. \
          Columnar backends use the delete_insert refresh strategy (append-only).",
        &PGS_COLUMNAR_BACKEND,
        GucContext::Suset,
        GucFlags::default(),
    );

    // CORR-1 / UX-1: Temporal IVM.
    GucRegistry::define_bool_guc(
        c"pg_trickle.temporal_stream_tables",
        c"CORR-1: Enable temporal IVM (SCD Type 2) support for stream tables.",
        c"When true, stream tables created with temporal := true maintain a \
          two-dimensional frontier (frontier_lsn, valid_from_ts). Each row carries \
          __pgt_valid_from TIMESTAMPTZ and __pgt_valid_to TIMESTAMPTZ. Rows are never \
          physically deleted; close deltas set valid_to. Default false.",
        &PGS_TEMPORAL_STREAM_TABLES,
        GucContext::Suset,
        GucFlags::default(),
    );

    // F4 (v0.37.0): pgVectorMV — incremental vector aggregate operators.
    GucRegistry::define_bool_guc(
        c"pg_trickle.enable_vector_agg",
        c"F4: Enable pgVectorMV incremental vector aggregate operators (avg/sum on vector types).",
        c"When true, avg(vector_col) and sum(vector_col) in stream table defining queries \
          are handled by the DVM engine using a group-rescan strategy so they remain correct \
          under differential refresh. Requires pgvector extension to be installed. Default false.",
        &PGS_ENABLE_VECTOR_AGG,
        GucContext::Suset,
        GucFlags::default(),
    );

    // F10 (v0.37.0): OpenTelemetry W3C Trace Context propagation.
    GucRegistry::define_bool_guc(
        c"pg_trickle.enable_trace_propagation",
        c"F10: Enable W3C Trace Context propagation through the refresh pipeline.",
        c"When true, pg_trickle reads pg_trickle.trace_id from the session GUC at \
          CDC capture time and stores it in the change buffer. At refresh time, spans \
          are exported via OTLP/gRPC to pg_trickle.otel_endpoint. Default false.",
        &PGS_ENABLE_TRACE_PROPAGATION,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_trickle.otel_endpoint",
        c"F10: OTLP/gRPC endpoint for OpenTelemetry span export.",
        c"Empty string disables span export. Example: 'http://jaeger:4317'. Default empty.",
        &PGS_OTEL_ENDPOINT,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_trickle.trace_id",
        c"F10: Session-level W3C traceparent header for trace context propagation.",
        c"Set before DML: SET pg_trickle.trace_id = 'traceparent: 00-...'. \
          Captured into the change buffer when enable_trace_propagation is on. \
          Default empty.",
        &PGS_TRACE_ID,
        GucContext::Userset,
        GucFlags::default(),
    );

    // O39-8 (v0.39.0): CDC capture mode — explicit discard vs hold semantics.
    GucRegistry::define_string_guc(
        c"pg_trickle.cdc_capture_mode",
        c"O39-8: CDC capture mode when cdc_paused=on: 'discard' (default) or 'hold' (reserved).",
        c"Controls what happens to CDC writes while pg_trickle.cdc_paused=on. \
          'discard' (default): trigger bodies return NULL; changes arriving while \
          paused are dropped — stream tables MUST be reinitialized after un-pausing. \
          'hold': reserved for future use; setting this emits a WARNING and falls back \
          to 'discard' until a durable hold path is implemented. \
          Check pgtrickle.cdc_pause_status() to see the active mode.",
        &PGS_CDC_CAPTURE_MODE,
        GucContext::Suset,
        GucFlags::default(),
    );

    // ── v0.43.0: Performance tunability GUCs ──────────────────────────────

    // A44-1: Deep-join threshold GUCs.
    GucRegistry::define_int_guc(
        c"pg_trickle.part3_max_scan_count",
        c"A44-1: Max scan nodes in left join child for Part 3 correction term.",
        c"Controls the maximum number of Scan nodes in the left child of an inner \
          join for which the Part 3 correction term is emitted during differential \
          refresh. Lower values reduce SQL complexity; higher values improve \
          correctness for deep join chains. Default: 5.",
        &PGS_PART3_MAX_SCAN_COUNT,
        1,  // min
        32, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.deep_join_l0_scan_threshold",
        c"A44-1: Scan node depth threshold before switching to L1+Part3 strategy.",
        c"When a join child has more Scan nodes than this threshold, pg_trickle \
          switches from per-leaf L0/R0 reconstruction to L1/R1 with correction \
          (Part 3). This avoids excessive temp-file generation for deep join chains. \
          Default: 4.",
        &PGS_DEEP_JOIN_L0_SCAN_THRESHOLD,
        1,  // min
        32, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // A44-3: WAL poll tuning GUCs.
    GucRegistry::define_int_guc(
        c"pg_trickle.wal_max_changes_per_poll",
        c"A44-3: Maximum WAL changes fetched per poll cycle.",
        c"Controls the max_changes argument to pg_logical_slot_get_changes(). \
          Higher values increase throughput at the cost of larger per-tick memory \
          usage. Lower values reduce per-change latency for high-volume sources. \
          Default: 10000.",
        &PGS_WAL_MAX_CHANGES_PER_POLL,
        100,       // min
        1_000_000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pg_trickle.wal_max_lag_bytes",
        c"A44-3: WAL lag bytes threshold for lag warnings.",
        c"When the WAL slot lag (bytes behind the write LSN) exceeds this value, \
          a WARNING is emitted and the wal_lag_bytes metric is recorded. \
          Set to 0 to disable. Default: 65536 (64 KiB).",
        &PGS_WAL_MAX_LAG_BYTES,
        0,        // min (0 = disabled)
        i32::MAX, // max
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

    // ── v0.62.0 GUCs ───────────────────────────────────────────────────────

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

    // ── v0.63.0 GUCs ───────────────────────────────────────────────────────

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

    // ── v0.65.0 GUCs ───────────────────────────────────────────────────────

    // CDC-6: DuckLake compaction policy.
    GucRegistry::define_string_guc(
        c"pg_trickle.ducklake_compaction_policy",
        c"CDC-6: Action when a DuckLake snapshot is no longer accessible after compaction (v0.65.0).",
        c"Controls what happens when a DuckLake change-feed source\'s frontier snapshot has been           compacted away. \'fallback\' (default) triggers a full refresh automatically;           \'error\' halts the refresh until the user reinitializes.",
        &PGS_DUCKLAKE_COMPACTION_POLICY,
        GucContext::Suset,
        GucFlags::default(),
    );

    // ── v0.66.0 GUCs ───────────────────────────────────────────────────────

    // F-4: DuckLake sink Parquet compression codec.
    GucRegistry::define_string_guc(
        c"pg_trickle.ducklake_sink_compression",
        c"F-4: Parquet compression codec used by the DuckLake sink (v0.66.0).",
        c"Compression codec for Parquet files written by the DuckLake sink. \
          Allowed values: 'snappy' (default), 'gzip', 'zstd', 'none'. \
          Snappy is the best balance of compression ratio and CPU cost.",
        &PGS_DUCKLAKE_SINK_COMPRESSION,
        GucContext::Suset,
        GucFlags::default(),
    );

    // S3 endpoint override.
    GucRegistry::define_string_guc(
        c"pg_trickle.ducklake_sink_s3_endpoint",
        c"S3/object-store endpoint URL for the DuckLake sink (v0.66.0).",
        c"Override the AWS S3 endpoint URL. Leave empty to use the default \
          AWS endpoint. Set to a MinIO or other S3-compatible URL for testing \
          (e.g. 'http://localhost:9000'). Used only when the sink path starts \
          with 's3://'.",
        &PGS_DUCKLAKE_SINK_S3_ENDPOINT,
        GucContext::Suset,
        GucFlags::default(),
    );

    // S3 region.
    GucRegistry::define_string_guc(
        c"pg_trickle.ducklake_sink_s3_region",
        c"AWS S3 region for the DuckLake sink (v0.66.0).",
        c"AWS region used for S3 uploads by the DuckLake sink. \
          Default: 'us-east-1'. Ignored when ducklake_sink_s3_endpoint is set \
          to a non-AWS endpoint.",
        &PGS_DUCKLAKE_SINK_S3_REGION,
        GucContext::Suset,
        GucFlags::default(),
    );

    // S3 access key (stored as a superuser-only GUC).
    GucRegistry::define_string_guc(
        c"pg_trickle.ducklake_sink_s3_access_key",
        c"AWS S3 access key ID for the DuckLake sink (v0.66.0).",
        c"AWS access key ID for S3 uploads. Leave empty to use the \
          AWS credential chain (environment variables, IAM role, etc.). \
          This GUC requires superuser to set.",
        &PGS_DUCKLAKE_SINK_S3_ACCESS_KEY,
        GucContext::Suset,
        GucFlags::default(),
    );

    // S3 secret key (superuser-only).
    GucRegistry::define_string_guc(
        c"pg_trickle.ducklake_sink_s3_secret_key",
        c"AWS S3 secret access key for the DuckLake sink (v0.66.0).",
        c"AWS secret access key for S3 uploads. Leave empty to use the \
          AWS credential chain. This GUC requires superuser to set.",
        &PGS_DUCKLAKE_SINK_S3_SECRET_KEY,
        GucContext::Suset,
        GucFlags::default(),
    );

    // Encryption key prefix for DuckLake sink (F-9).
    GucRegistry::define_string_guc(
        c"pg_trickle.ducklake_sink_encryption_key_prefix",
        c"F-9: Key-name prefix for per-file Parquet encryption keys (v0.66.0).",
        c"When writing to an encrypted DuckLake table, the sink generates a fresh \
          per-file key and names it '<prefix>/<table_id>/<epoch_ms>'. \
          The key is stored in ducklake_data_file.encryption_key_id and applied \
          during Parquet serialisation. Leave empty to disable encryption.",
        &PGS_DUCKLAKE_SINK_ENCRYPTION_KEY_PREFIX,
        GucContext::Suset,
        GucFlags::default(),
    );

    // ── v0.69.0: DuckLake reliability & security GUCs ─────────────────────

    // ARCH-002/REL-001: Max retries before FAILED_PERMANENT.
    GucRegistry::define_int_guc(
        c"pg_trickle.ducklake_sink_max_retries",
        c"ARCH-002/REL-001: Maximum retryable attempts before FAILED_PERMANENT (v0.69.0).",
        c"When a DuckLake sink write fails with a transient error, the attempt count \
          is incremented. Once attempt_count reaches this limit the delivery row \
          transitions from FAILED_RETRYABLE to FAILED_PERMANENT. Default 3.",
        &PGS_DUCKLAKE_SINK_MAX_RETRIES,
        1,   // min
        100, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // ARCH-002/REL-001: Failure mode when delivery is FAILED_PERMANENT.
    GucRegistry::define_string_guc(
        c"pg_trickle.ducklake_sink_failure_mode",
        c"ARCH-002/REL-001: What to do when DuckLake delivery is FAILED_PERMANENT (v0.69.0).",
        c"'warn' (default): emit a WARNING and continue; the stream table stays \
          ACTIVE. 'error': propagate as a PostgreSQL error and mark the refresh \
          FAILED.",
        &PGS_DUCKLAKE_SINK_FAILURE_MODE,
        GucContext::Suset,
        GucFlags::default(),
    );

    // SEC-002: DuckLake catalog schema.
    GucRegistry::define_string_guc(
        c"pg_trickle.ducklake_catalog_schema",
        c"SEC-002: Schema that contains the DuckLake catalog tables (v0.69.0).",
        c"Default 'main'. Set to the schema name used by your DuckLake installation \
          if it differs from the default. All catalog writes (ducklake_view, \
          ducklake_snapshot, ducklake_data_file, ducklake_table_stats) use this \
          schema to avoid search_path-based misdirection.",
        &PGS_DUCKLAKE_CATALOG_SCHEMA,
        GucContext::Suset,
        GucFlags::default(),
    );
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

/// CDC-6 (v0.65.0): Returns the global default DuckLake compaction policy.
pub fn pg_trickle_ducklake_compaction_policy() -> DucklakeCompactionPolicy {
    normalize_ducklake_compaction_policy(
        PGS_DUCKLAKE_COMPACTION_POLICY
            .get()
            .map(|c| c.to_string_lossy().into_owned()),
    )
}

// ── v0.66.0 DuckLake sink accessors ────────────────────────────────────────

/// F-4 (v0.66.0): Returns the Parquet compression codec for the DuckLake sink.
pub fn pg_trickle_ducklake_sink_compression() -> String {
    PGS_DUCKLAKE_SINK_COMPRESSION
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_else(|| "snappy".to_string())
}

/// F-4 (v0.66.0): Returns the S3 endpoint URL override (empty = AWS default).
pub fn pg_trickle_ducklake_sink_s3_endpoint() -> Option<String> {
    PGS_DUCKLAKE_SINK_S3_ENDPOINT
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())
}

/// F-4 (v0.66.0): Returns the AWS S3 region for the DuckLake sink.
pub fn pg_trickle_ducklake_sink_s3_region() -> String {
    PGS_DUCKLAKE_SINK_S3_REGION
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_else(|| "us-east-1".to_string())
}

/// F-4 (v0.66.0): Returns the AWS S3 access key ID (None = use credential chain).
pub fn pg_trickle_ducklake_sink_s3_access_key() -> Option<String> {
    PGS_DUCKLAKE_SINK_S3_ACCESS_KEY
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())
}

/// F-4 (v0.66.0): Returns the AWS S3 secret access key (None = use credential chain).
pub fn pg_trickle_ducklake_sink_s3_secret_key() -> Option<String> {
    PGS_DUCKLAKE_SINK_S3_SECRET_KEY
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())
}

/// F-9 (v0.66.0): Returns the encryption key prefix for per-file Parquet keys.
/// Returns `None` when encryption is disabled (empty prefix).
pub fn pg_trickle_ducklake_sink_encryption_key_prefix() -> Option<String> {
    PGS_DUCKLAKE_SINK_ENCRYPTION_KEY_PREFIX
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())
}

// ── v0.69.0 DuckLake reliability & security accessors ──────────────────────

/// ARCH-002/REL-001 (v0.69.0): Returns the max retries before FAILED_PERMANENT.
pub fn pg_trickle_ducklake_sink_max_retries() -> i32 {
    PGS_DUCKLAKE_SINK_MAX_RETRIES.get()
}

/// ARCH-002/REL-001 (v0.69.0): Returns whether FAILED_PERMANENT propagates as
/// a PostgreSQL error (`true`) or is silently warned (`false`).
pub fn pg_trickle_ducklake_sink_failure_mode_is_error() -> bool {
    PGS_DUCKLAKE_SINK_FAILURE_MODE
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
        == Some("error")
}

/// SEC-002 (v0.69.0): Returns the DuckLake catalog schema name.
/// Defaults to `"main"` when not set.
pub fn pg_trickle_ducklake_catalog_schema() -> String {
    PGS_DUCKLAKE_CATALOG_SCHEMA
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "main".to_string())
}

// ── Convenience accessors ──────────────────────────────────────────────────

/// Returns the number of differential cycles before automatic drift reset.
pub fn pg_trickle_algebraic_drift_reset_cycles() -> i32 {
    PGS_ALGEBRAIC_DRIFT_RESET_CYCLES.get()
}

/// Returns whether automatic schedule backoff is enabled for falling-behind STs.
pub fn pg_trickle_auto_backoff() -> bool {
    PGS_AUTO_BACKOFF.get()
}

// ── v0.43.0 convenience accessors ─────────────────────────────────────────

/// A44-1: Returns the max scan count for Part 3 correction term generation.
pub fn pg_trickle_part3_max_scan_count() -> usize {
    #[cfg(test)]
    {
        5usize
    }
    #[cfg(not(test))]
    {
        PGS_PART3_MAX_SCAN_COUNT.get().max(1) as usize
    }
}

/// A44-1: Returns the deep-join L0 scan threshold for snapshot strategy selection.
pub fn pg_trickle_deep_join_l0_scan_threshold() -> usize {
    #[cfg(test)]
    {
        4usize
    }
    #[cfg(not(test))]
    {
        PGS_DEEP_JOIN_L0_SCAN_THRESHOLD.get().max(1) as usize
    }
}

/// A44-3: Returns the maximum WAL changes per poll as i64.
pub fn pg_trickle_wal_max_changes_per_poll() -> i64 {
    #[cfg(test)]
    {
        10_000i64
    }
    #[cfg(not(test))]
    {
        PGS_WAL_MAX_CHANGES_PER_POLL.get().max(100) as i64
    }
}

/// A44-3: Returns the WAL max lag bytes threshold as i64.
pub fn pg_trickle_wal_max_lag_bytes() -> i64 {
    #[cfg(test)]
    {
        65_536i64
    }
    #[cfg(not(test))]
    {
        PGS_WAL_MAX_LAG_BYTES.get() as i64
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

/// A46-7: Returns the effective invalidation ring capacity (clamped to 1–1024).
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

/// Returns the delta-to-ST ratio threshold for disabling seqscan before MERGE.
pub fn pg_trickle_merge_seqscan_threshold() -> f64 {
    PGS_MERGE_SEQSCAN_THRESHOLD.get()
}

/// Returns the current value of `pg_trickle.enabled`.
pub fn pg_trickle_enabled() -> bool {
    PGS_ENABLED.get()
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

/// A07 (v0.35.0): Returns whether CDC writes are paused cluster-wide.
pub fn pg_trickle_cdc_paused() -> bool {
    PGS_CDC_PAUSED.get()
}

/// UX-GUC (v0.35.0): Returns the NOTIFY coalescing debounce interval in ms.
/// Returns 0 when coalescing is disabled.
pub fn pg_trickle_notify_coalesce_ms() -> i32 {
    PGS_NOTIFY_COALESCE_MS.get()
}

/// F17 (v0.35.0): Returns the SLA reporting window in hours.
pub fn pg_trickle_sla_window_hours() -> i32 {
    PGS_SLA_WINDOW_HOURS.get()
}

/// A10 (v0.35.0): Returns the history pruner interval in seconds (0 = legacy).
pub fn pg_trickle_history_prune_interval_seconds() -> i32 {
    PGS_HISTORY_PRUNE_INTERVAL_SECONDS.get()
}

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

/// WM-7: Returns the watermark holdback timeout in seconds (0 = disabled).
pub fn pg_trickle_watermark_holdback_timeout() -> i32 {
    PGS_WATERMARK_HOLDBACK_TIMEOUT.get()
}

/// PH-E2: Returns the spill detection threshold in temp blocks written (0 = disabled).
pub fn pg_trickle_spill_threshold_blocks() -> i32 {
    PGS_SPILL_THRESHOLD_BLOCKS.get()
}

/// PH-E2: Returns the consecutive spill limit before FULL fallback (default 3).
pub fn pg_trickle_spill_consecutive_limit() -> i32 {
    PGS_SPILL_CONSECUTIVE_LIMIT.get()
}

/// Returns the change buffer schema name.
pub fn pg_trickle_change_buffer_schema() -> String {
    PGS_CHANGE_BUFFER_SCHEMA
        .get()
        .map(|cs| cs.to_str().unwrap_or("pgtrickle_changes").to_string())
        .unwrap_or_else(|| "pgtrickle_changes".to_string())
}

/// Returns the maximum number of concurrent refresh workers.
pub fn pg_trickle_max_concurrent_refreshes() -> i32 {
    PGS_MAX_CONCURRENT_REFRESHES.get()
}

/// Returns whether TRUNCATE cleanup is enabled.
pub fn pg_trickle_cleanup_use_truncate() -> bool {
    PGS_CLEANUP_USE_TRUNCATE.get()
}

/// Returns whether MERGE planner hints are enabled.
///
/// C4: Returns the value of `planner_aggressive`. The legacy
/// `merge_planner_hints` GUC is ignored at runtime.
pub fn pg_trickle_merge_planner_hints() -> bool {
    PGS_PLANNER_AGGRESSIVE.get()
}

/// Returns the work_mem value (in MB) for large-delta MERGE.
pub fn pg_trickle_merge_work_mem_mb() -> i32 {
    PGS_MERGE_WORK_MEM_MB.get()
}

/// SCAL-3: Returns the delta work_mem cap (MB). 0 = disabled.
pub fn pg_trickle_delta_work_mem_cap_mb() -> i32 {
    PGS_DELTA_WORK_MEM_CAP_MB.get()
}

/// Returns whether prepared statements are enabled for MERGE.
pub fn pg_trickle_use_prepared_statements() -> bool {
    PGS_USE_PREPARED_STATEMENTS.get()
}

/// Returns the canonical user-trigger handling mode.
///
/// `on` is preserved as a deprecated input alias for backward compatibility
/// but is normalized to `auto` at runtime.
pub fn pg_trickle_user_triggers_mode() -> UserTriggersMode {
    normalize_user_triggers_mode(
        PGS_USER_TRIGGERS
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// Returns the canonical user-trigger handling mode as a string.
pub fn pg_trickle_user_triggers() -> String {
    pg_trickle_user_triggers_mode().as_str().to_string()
}

/// Returns the CDC mode: `"auto"`, `"trigger"`, or `"wal"`.
pub fn pg_trickle_cdc_mode() -> String {
    PGS_CDC_MODE
        .get()
        .map(|cs| cs.to_str().unwrap_or("auto").to_string())
        .unwrap_or_else(|| "auto".to_string())
}

/// Returns the WAL transition timeout in seconds.
pub fn pg_trickle_wal_transition_timeout() -> i32 {
    PGS_WAL_TRANSITION_TIMEOUT.get()
}

/// Returns the WAL slot lag warning threshold in bytes.
pub fn pg_trickle_slot_lag_warning_threshold_bytes() -> i64 {
    threshold_mb_to_bytes(PGS_SLOT_LAG_WARNING_THRESHOLD_MB.get())
}

/// Returns the WAL slot lag critical threshold in bytes.
pub fn pg_trickle_slot_lag_critical_threshold_bytes() -> i64 {
    threshold_mb_to_bytes(PGS_SLOT_LAG_CRITICAL_THRESHOLD_MB.get())
}

/// Returns whether source DDL blocking is enabled.
pub fn pg_trickle_block_source_ddl() -> bool {
    PGS_BLOCK_SOURCE_DDL.get()
}

/// Returns the buffer alert threshold (row count).
pub fn pg_trickle_buffer_alert_threshold() -> i64 {
    PGS_BUFFER_ALERT_THRESHOLD.get() as i64
}

/// Returns the change buffer compaction threshold (row count).
/// Returns 0 when compaction is disabled.
pub fn pg_trickle_compact_threshold() -> i64 {
    PGS_COMPACT_THRESHOLD.get() as i64
}

/// Returns the max buffer rows limit (row count).
/// Returns 0 when the limit is disabled.
pub fn pg_trickle_max_buffer_rows() -> i64 {
    PGS_MAX_BUFFER_ROWS.get() as i64
}

/// Returns whether automatic index creation is enabled.
pub fn pg_trickle_auto_index() -> bool {
    PGS_AUTO_INDEX.get()
}

/// B-1: Returns whether the aggregate fast-path (explicit DML for
/// all-algebraic aggregate queries) is enabled.
pub fn pg_trickle_aggregate_fast_path() -> bool {
    PGS_AGGREGATE_FAST_PATH.get()
}

/// G14-SHC: Returns whether the cross-backend template cache is enabled.
pub fn pg_trickle_template_cache_enabled() -> bool {
    PGS_TEMPLATE_CACHE.get()
}

/// Returns the buffer partitioning mode: `"off"`, `"on"`, or `"auto"`.
pub fn pg_trickle_buffer_partitioning() -> String {
    PGS_BUFFER_PARTITIONING
        .get()
        .map(|cs| cs.to_str().unwrap_or("off").to_string())
        .unwrap_or_else(|| "off".to_string())
}

/// Returns whether foreign table polling CDC is enabled.
pub fn pg_trickle_foreign_table_polling() -> bool {
    PGS_FOREIGN_TABLE_POLLING.get()
}

/// Returns whether materialized view polling CDC is enabled.
pub fn pg_trickle_matview_polling() -> bool {
    PGS_MATVIEW_POLLING.get()
}

/// Returns whether the tick watermark (CSS1) feature is enabled.
pub fn pg_trickle_tick_watermark_enabled() -> bool {
    PGS_TICK_WATERMARK_ENABLED.get()
}

/// Returns the CDC trigger granularity mode.
pub fn pg_trickle_cdc_trigger_mode() -> CdcTriggerMode {
    normalize_cdc_trigger_mode(
        PGS_CDC_TRIGGER_MODE
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// Returns the maximum recursion depth for WITH RECURSIVE in IMMEDIATE mode.
/// Returns `None` when the guard is disabled (value = 0).
pub fn pg_trickle_ivm_recursive_max_depth() -> Option<i32> {
    normalize_recursive_max_depth(PGS_IVM_RECURSIVE_MAX_DEPTH.get())
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

fn normalize_parallel_refresh_mode(value: Option<String>) -> ParallelRefreshMode {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("dry_run") => ParallelRefreshMode::DryRun,
        Some("off") => ParallelRefreshMode::Off,
        // Default to On for None (unset) and any unrecognised value.
        // On has been the stable parallel path since v0.4.0.
        _ => ParallelRefreshMode::On,
    }
}

/// Returns the current parallel refresh mode.
pub fn pg_trickle_parallel_refresh_mode() -> ParallelRefreshMode {
    normalize_parallel_refresh_mode(
        PGS_PARALLEL_REFRESH_MODE
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// Returns the cluster-wide cap on dynamic refresh workers.
pub fn pg_trickle_max_dynamic_refresh_workers() -> i32 {
    PGS_MAX_DYNAMIC_REFRESH_WORKERS.get()
}

/// Returns the maximum fixpoint iterations for SCC convergence (CYC-4).
pub fn pg_trickle_max_fixpoint_iterations() -> i32 {
    PGS_MAX_FIXPOINT_ITERATIONS.get()
}

/// Returns whether circular (cyclic) dependencies are allowed (CYC-4).
pub fn pg_trickle_allow_circular() -> bool {
    PGS_ALLOW_CIRCULAR.get()
}

/// G-7: Returns whether tiered refresh scheduling is enabled.
pub fn pg_trickle_tiered_scheduling() -> bool {
    PGS_TIERED_SCHEDULING.get()
}

/// QF-1: Returns whether MERGE SQL template logging is enabled.
pub fn pg_trickle_log_merge_sql() -> bool {
    PGS_LOG_MERGE_SQL.get()
}

/// FUSE-5: Returns the global default fuse ceiling (0 = disabled).
pub fn pg_trickle_fuse_default_ceiling() -> i64 {
    PGS_FUSE_DEFAULT_CEILING.get() as i64
}

/// C3-1: Returns the per-database worker quota (0 = disabled).
pub fn pg_trickle_per_database_worker_quota() -> i32 {
    PGS_PER_DATABASE_WORKER_QUOTA.get()
}

/// DAG-3: Returns the delta amplification threshold (0.0 = disabled).
pub fn pg_trickle_delta_amplification_threshold() -> f64 {
    PGS_DELTA_AMPLIFICATION_THRESHOLD.get()
}

/// DIAG-2: Returns the algebraic aggregate cardinality warning threshold.
/// Returns 0 when the warning is disabled.
pub fn pg_trickle_agg_diff_cardinality_threshold() -> i32 {
    PGS_AGG_DIFF_CARDINALITY_THRESHOLD.get()
}

/// G13-SD: Returns the maximum recursion depth for query parser visitors.
pub fn pg_trickle_max_parse_depth() -> usize {
    PGS_MAX_PARSE_DEPTH.get() as usize
}

/// C-7 / R-7 (v0.54.0): Returns the maximum number of CTEs the differential
/// query generator may produce for a single refresh cycle.
pub fn pg_trickle_max_diff_ctes() -> usize {
    PGS_MAX_DIFF_CTES.get() as usize
}

/// VOL-1: Returns the volatile function handling policy.
pub fn pg_trickle_volatile_function_policy() -> VolatileFunctionPolicy {
    normalize_volatile_function_policy(
        PGS_VOLATILE_FUNCTION_POLICY
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// PH-D2: Returns the merge join strategy override.
pub fn pg_trickle_merge_join_strategy() -> MergeJoinStrategy {
    normalize_merge_join_strategy(
        PGS_MERGE_JOIN_STRATEGY
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// D-1a: Returns whether new change buffer tables should be created UNLOGGED.
///
/// **Deprecated (COR-003/ARCH-001, v0.68.0):** Use
/// `pg_trickle_change_buffer_durability()` instead.  This function emits a
/// PostgreSQL WARNING when the GUC is set to `true` so operators know to
/// migrate to `pg_trickle.change_buffer_durability`.
pub fn pg_trickle_unlogged_buffers() -> bool {
    let val = PGS_UNLOGGED_BUFFERS.get();
    if val {
        pgrx::warning!(
            "pg_trickle.unlogged_buffers is deprecated. \
             Use pg_trickle.change_buffer_durability = 'unlogged' instead."
        );
    }
    val
}

/// PH-D1: Returns the merge strategy override.
pub fn pg_trickle_merge_strategy() -> MergeStrategy {
    normalize_merge_strategy(
        PGS_MERGE_STRATEGY
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// PH-D1: Returns the merge strategy threshold for the `auto` heuristic.
pub fn pg_trickle_merge_strategy_threshold() -> f64 {
    PGS_MERGE_STRATEGY_THRESHOLD.get()
}

/// STAB-1: Returns `true` when the cluster-wide pooler mode is `"transaction"`,
/// which overrides per-ST `pooler_compatibility_mode` for all stream tables.
pub fn pg_trickle_connection_pooler_transaction_mode() -> bool {
    PGS_CONNECTION_POOLER_MODE
        .get()
        .and_then(|cs| cs.to_str().ok().map(str::to_owned))
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
        == Some("transaction")
}

/// STAB-1: Effective pooler compatibility check — `true` if either the per-ST
/// flag or the cluster-wide GUC requires pooler-safe behaviour.
pub fn effective_pooler_compat(per_st_flag: bool) -> bool {
    per_st_flag || pg_trickle_connection_pooler_transaction_mode()
}

/// DB-5: Returns the history retention period in days (0 = disabled).
pub fn pg_trickle_history_retention_days() -> i32 {
    PGS_HISTORY_RETENTION_DAYS.get()
}

/// DF-G1: Returns the current self-monitoring auto-apply policy.
pub fn pg_trickle_self_monitoring_auto_apply() -> SelfMonitoringAutoApply {
    normalize_self_monitoring_auto_apply(
        PGS_SELF_MONITORING_AUTO_APPLY
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// PAR-2: Returns the maximum parallel refresh workers (0 = serial).
pub fn pg_trickle_max_parallel_workers() -> i32 {
    PGS_MAX_PARALLEL_WORKERS.get()
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

// ── v0.23.0: TPC-H DVM Scaling Performance accessor functions ──────────────

/// P1-2: Returns whether delta SQL logging is enabled.
pub fn pg_trickle_log_delta_sql() -> bool {
    PGS_LOG_DELTA_SQL.get()
}

/// P5-1: Returns the delta work_mem override in MB (0 = disabled).
pub fn pg_trickle_delta_work_mem() -> i32 {
    PGS_DELTA_WORK_MEM.get()
}

/// P5-2: Returns whether nested-loop joins are allowed during delta execution.
pub fn pg_trickle_delta_enable_nestloop() -> bool {
    PGS_DELTA_ENABLE_NESTLOOP.get()
}

/// PERF-5: Returns whether ANALYZE is run on change buffers before delta execution.
pub fn pg_trickle_analyze_before_delta() -> bool {
    PGS_ANALYZE_BEFORE_DELTA.get()
}

/// SCAL-2: Returns the change buffer overflow alert threshold (0 = disabled).
pub fn pg_trickle_max_change_buffer_alert_rows() -> i64 {
    PGS_MAX_CHANGE_BUFFER_ALERT_ROWS.get() as i64
}

/// UX-7: Returns the DIFF output format for aggregate UPDATE-splits.
pub fn pg_trickle_diff_output_format() -> DiffOutputFormat {
    normalize_diff_output_format(
        PGS_DIFF_OUTPUT_FORMAT
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// #536: Returns the current frontier holdback mode.
pub fn pg_trickle_frontier_holdback_mode() -> FrontierHoldbackMode {
    let raw = PGS_FRONTIER_HOLDBACK_MODE
        .get()
        .and_then(|cs| cs.to_str().ok().map(str::to_owned));
    let mode = normalize_frontier_holdback_mode(raw.clone());
    if matches!(mode, FrontierHoldbackMode::InvalidLsn) {
        pgrx::warning!(
            "pg_trickle: invalid frontier_holdback_mode '{}' — \
             expected 'lsn:<bytes>' with a valid integer; defaulting to 'xmin'",
            raw.as_deref().unwrap_or("")
        );
        return FrontierHoldbackMode::Xmin;
    }
    mode
}

/// #536: Returns the frontier holdback warning threshold in seconds (0 = disabled).
pub fn pg_trickle_frontier_holdback_warn_seconds() -> i32 {
    PGS_FRONTIER_HOLDBACK_WARN_SECONDS.get()
}

// ── v0.25.0 accessor functions ─────────────────────────────────────────────

/// SCAL-5: Returns the persistent worker pool size (0 = spawn-per-task).
pub fn pg_trickle_worker_pool_size() -> i32 {
    PGS_WORKER_POOL_SIZE.get()
}

/// CACHE-2: Returns the L1 template cache max entries (0 = unbounded).
pub fn pg_trickle_template_cache_max_entries() -> i32 {
    PGS_TEMPLATE_CACHE_MAX_ENTRIES.get()
}

/// PERF-006: Returns the L1 template cache max bytes (0 = disabled).
pub fn pg_trickle_template_cache_max_bytes() -> usize {
    PGS_TEMPLATE_CACHE_MAX_BYTES.get().max(0) as usize
}

/// PERF-003: Returns the holdback-probe cache interval in milliseconds.
pub fn pg_trickle_frontier_holdback_probe_cache_ms() -> i32 {
    PGS_FRONTIER_HOLDBACK_PROBE_CACHE_MS.get()
}

/// PUB-1: Returns the publication subscriber lag warning threshold in bytes (0 = disabled).
pub fn pg_trickle_publication_lag_warn_bytes() -> i64 {
    PGS_PUBLICATION_LAG_WARN_BYTES.get() as i64
}

/// SCAL-1 (v0.30.0): Returns whether SQLSTATE-based SPI error classification is enabled.
pub fn pg_trickle_use_sqlstate_classification() -> bool {
    PGS_USE_SQLSTATE_CLASSIFICATION.get()
}

/// STAB-3 (v0.30.0): Returns the maximum age (hours) for L2 template cache entries.
pub fn pg_trickle_template_cache_max_age_hours() -> i32 {
    PGS_TEMPLATE_CACHE_MAX_AGE_HOURS.get()
}

/// PERF-2 (v0.30.0): Returns the maximum parse node count allowed per query.
pub fn pg_trickle_max_parse_nodes() -> usize {
    PGS_MAX_PARSE_NODES.get() as usize
}

/// PERF-4 (v0.31.0): Returns whether ENR-based IVM trigger mode is enabled.
pub fn pg_trickle_ivm_use_enr() -> bool {
    PGS_IVM_USE_ENR.get()
}

/// PERF-1 (v0.31.0): Returns whether adaptive batch coalescing is enabled.
pub fn pg_trickle_adaptive_batch_coalescing() -> bool {
    PGS_ADAPTIVE_BATCH_COALESCING.get()
}

/// PERF-2 (v0.31.0): Returns whether adaptive merge strategy selection is enabled.
pub fn pg_trickle_adaptive_merge_strategy() -> bool {
    PGS_ADAPTIVE_MERGE_STRATEGY.get()
}

/// SCAL-1 (v0.31.0): Returns the number of consecutive cycles before emitting
/// a back-pressure alert.
pub fn pg_trickle_backpressure_consecutive_limit() -> i32 {
    PGS_BACKPRESSURE_CONSECUTIVE_LIMIT.get()
}

// ── v0.36.0 accessor functions ─────────────────────────────────────────────

/// A12 (v0.36.0): Returns whether WAL backpressure enforcement is enabled.
pub fn pg_trickle_enforce_backpressure() -> bool {
    PGS_ENFORCE_BACKPRESSURE.get()
}

/// A20 (v0.36.0): Returns the current log format.
pub fn pg_trickle_log_format() -> LogFormat {
    normalize_log_format(
        PGS_LOG_FORMAT
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// A35 (v0.36.0): Returns the drain timeout in seconds.
pub fn pg_trickle_drain_timeout() -> i32 {
    PGS_DRAIN_TIMEOUT.get()
}

/// F5 (v0.36.0): Returns whether online schema evolution is enabled.
pub fn pg_trickle_online_schema_evolution() -> bool {
    PGS_ONLINE_SCHEMA_EVOLUTION.get()
}

/// CORR-2 / UX-3 (v0.36.0): Returns the columnar storage backend.
pub fn pg_trickle_columnar_backend() -> ColumnarBackend {
    normalize_columnar_backend(
        PGS_COLUMNAR_BACKEND
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
}

/// CORR-1 / UX-1 (v0.36.0): Returns whether temporal IVM is globally enabled.
pub fn pg_trickle_temporal_stream_tables() -> bool {
    PGS_TEMPORAL_STREAM_TABLES.get()
}

/// F4 (v0.37.0): Returns whether pgVectorMV vector aggregate support is enabled.
pub fn pg_trickle_enable_vector_agg() -> bool {
    PGS_ENABLE_VECTOR_AGG.get()
}

/// F10 (v0.37.0): Returns whether OpenTelemetry trace propagation is enabled.
pub fn pg_trickle_enable_trace_propagation() -> bool {
    PGS_ENABLE_TRACE_PROPAGATION.get()
}

/// F10 (v0.37.0): Returns the OTLP endpoint for trace export (empty = disabled).
pub fn pg_trickle_otel_endpoint() -> Option<String> {
    PGS_OTEL_ENDPOINT
        .get()
        .and_then(|s| s.to_str().ok().map(|v| v.to_string()))
}

/// F10 (v0.37.0): Returns the current session trace_id (W3C traceparent).
pub fn pg_trickle_trace_id() -> Option<String> {
    PGS_TRACE_ID
        .get()
        .and_then(|s| s.to_str().ok().map(|v| v.to_string()))
}

/// O39-8 (v0.39.0): Returns the active CDC capture mode.
///
/// When `cdc_paused = on`, this determines whether changes are discarded (default)
/// or held for later processing. `Hold` mode is reserved; if configured, a WARNING
/// is emitted and the function returns `Discard`.
pub fn pg_trickle_cdc_capture_mode() -> CdcCaptureMode {
    let raw = PGS_CDC_CAPTURE_MODE
        .get()
        .and_then(|s| s.to_str().ok().map(|v| v.to_string()));
    let mode = normalize_cdc_capture_mode(raw);
    if mode == CdcCaptureMode::Hold {
        pgrx::warning!(
            "pg_trickle: cdc_capture_mode='hold' is not yet implemented. \
             Falling back to 'discard'. Changes arriving while cdc_paused=on \
             will be dropped — reinitialize stream tables after un-pausing."
        );
        CdcCaptureMode::Discard
    } else {
        mode
    }
}

/// VP-2 (v0.47.0): Returns the global default drift threshold for
/// drift-triggered REINDEX operations.
pub fn pg_trickle_reindex_drift_threshold() -> f64 {
    PGS_REINDEX_DRIFT_THRESHOLD.get()
}

#[cfg(test)]
mod tests {
    use super::{
        CdcTriggerMode, ColumnarBackend, DiffOutputFormat, DucklakeCompactionPolicy,
        FrontierHoldbackMode, LogFormat, MergeJoinStrategy, MergeStrategy, ParallelRefreshMode,
        RefreshStrategy, SelfMonitoringAutoApply, UserTriggersMode, VolatileFunctionPolicy,
        normalize_cdc_trigger_mode, normalize_columnar_backend, normalize_diff_output_format,
        normalize_ducklake_compaction_policy, normalize_frontier_holdback_mode,
        normalize_log_format, normalize_merge_join_strategy, normalize_merge_strategy,
        normalize_parallel_refresh_mode, normalize_recursive_max_depth, normalize_refresh_strategy,
        normalize_self_monitoring_auto_apply, normalize_user_triggers_mode,
        normalize_volatile_function_policy, threshold_mb_to_bytes,
    };

    #[test]
    fn test_normalize_user_triggers_mode_defaults_to_auto() {
        assert_eq!(normalize_user_triggers_mode(None), UserTriggersMode::Auto);
        assert_eq!(
            normalize_user_triggers_mode(Some("auto".to_string())),
            UserTriggersMode::Auto
        );
        assert_eq!(
            normalize_user_triggers_mode(Some("on".to_string())),
            UserTriggersMode::Auto
        );
        assert_eq!(
            normalize_user_triggers_mode(Some("unexpected".to_string())),
            UserTriggersMode::Auto
        );
    }

    #[test]
    fn test_normalize_user_triggers_mode_accepts_off_case_insensitively() {
        assert_eq!(
            normalize_user_triggers_mode(Some("off".to_string())),
            UserTriggersMode::Off
        );
        assert_eq!(
            normalize_user_triggers_mode(Some("OFF".to_string())),
            UserTriggersMode::Off
        );
    }

    #[test]
    fn test_threshold_mb_to_bytes_converts_megabytes() {
        assert_eq!(threshold_mb_to_bytes(0), 0);
        assert_eq!(threshold_mb_to_bytes(100), 104_857_600);
        assert_eq!(threshold_mb_to_bytes(1024), 1_073_741_824);
    }

    #[test]
    fn test_normalize_cdc_trigger_mode_defaults_to_statement() {
        assert_eq!(normalize_cdc_trigger_mode(None), CdcTriggerMode::Statement);
        assert_eq!(
            normalize_cdc_trigger_mode(Some("statement".to_string())),
            CdcTriggerMode::Statement
        );
        assert_eq!(
            normalize_cdc_trigger_mode(Some("unexpected".to_string())),
            CdcTriggerMode::Statement
        );
    }

    #[test]
    fn test_normalize_cdc_trigger_mode_accepts_row_case_insensitively() {
        assert_eq!(
            normalize_cdc_trigger_mode(Some("row".to_string())),
            CdcTriggerMode::Row
        );
        assert_eq!(
            normalize_cdc_trigger_mode(Some("ROW".to_string())),
            CdcTriggerMode::Row
        );
    }

    #[test]
    fn test_normalize_recursive_max_depth_zero_disables_guard() {
        assert_eq!(normalize_recursive_max_depth(0), None);
        assert_eq!(normalize_recursive_max_depth(-5), None);
        assert_eq!(normalize_recursive_max_depth(100), Some(100));
    }

    #[test]
    fn test_parallel_refresh_mode_display_matches_as_str() {
        assert_eq!(ParallelRefreshMode::Off.as_str(), "off");
        assert_eq!(ParallelRefreshMode::DryRun.as_str(), "dry_run");
        assert_eq!(ParallelRefreshMode::On.as_str(), "on");
        assert_eq!(ParallelRefreshMode::DryRun.to_string(), "dry_run");
    }

    #[test]
    fn test_normalize_parallel_refresh_mode_defaults_to_on() {
        assert_eq!(
            normalize_parallel_refresh_mode(None),
            ParallelRefreshMode::On
        );
        assert_eq!(
            normalize_parallel_refresh_mode(Some("unexpected".to_string())),
            ParallelRefreshMode::On
        );
    }

    #[test]
    fn test_normalize_parallel_refresh_mode_accepts_supported_values() {
        assert_eq!(
            normalize_parallel_refresh_mode(Some("dry_run".to_string())),
            ParallelRefreshMode::DryRun
        );
        assert_eq!(
            normalize_parallel_refresh_mode(Some("DRY_RUN".to_string())),
            ParallelRefreshMode::DryRun
        );
        assert_eq!(
            normalize_parallel_refresh_mode(Some("on".to_string())),
            ParallelRefreshMode::On
        );
    }

    // ── P3: as_str coverage for all enum variants; threshold edge cases ─────

    #[test]
    fn test_user_triggers_mode_as_str() {
        assert_eq!(UserTriggersMode::Auto.as_str(), "auto");
        assert_eq!(UserTriggersMode::Off.as_str(), "off");
    }

    #[test]
    fn test_cdc_trigger_mode_as_str() {
        assert_eq!(CdcTriggerMode::Statement.as_str(), "statement");
        assert_eq!(CdcTriggerMode::Row.as_str(), "row");
    }

    #[test]
    fn test_parallel_refresh_mode_as_str_all_variants() {
        assert_eq!(ParallelRefreshMode::Off.as_str(), "off");
        assert_eq!(ParallelRefreshMode::DryRun.as_str(), "dry_run");
        assert_eq!(ParallelRefreshMode::On.as_str(), "on");
    }

    #[test]
    fn test_threshold_mb_to_bytes_negative_input_is_zero_or_negative() {
        // Negative megabytes should yield a non-positive byte count
        assert!(threshold_mb_to_bytes(-1) <= 0);
        assert!(threshold_mb_to_bytes(-100) < 0);
    }

    #[test]
    fn test_normalize_parallel_refresh_mode_case_insensitive_on() {
        assert_eq!(
            normalize_parallel_refresh_mode(Some("ON".to_string())),
            ParallelRefreshMode::On
        );
    }

    #[test]
    fn test_normalize_user_triggers_mode_roundtrip_via_as_str() {
        for (input, expected) in [
            ("off", UserTriggersMode::Off),
            ("OFF", UserTriggersMode::Off),
        ] {
            assert_eq!(
                normalize_user_triggers_mode(Some(input.to_string())),
                expected
            );
        }
        // as_str / normalize should be consistent
        assert_eq!(
            normalize_user_triggers_mode(Some(UserTriggersMode::Off.as_str().to_string())),
            UserTriggersMode::Off
        );
        assert_eq!(
            normalize_user_triggers_mode(Some(UserTriggersMode::Auto.as_str().to_string())),
            UserTriggersMode::Auto
        );
    }

    #[test]
    fn test_normalize_cdc_trigger_mode_roundtrip_via_as_str() {
        assert_eq!(
            normalize_cdc_trigger_mode(Some(CdcTriggerMode::Row.as_str().to_string())),
            CdcTriggerMode::Row
        );
        assert_eq!(
            normalize_cdc_trigger_mode(Some(CdcTriggerMode::Statement.as_str().to_string())),
            CdcTriggerMode::Statement
        );
    }

    #[test]
    fn test_normalize_volatile_function_policy_defaults_to_reject() {
        assert_eq!(
            normalize_volatile_function_policy(None),
            VolatileFunctionPolicy::Reject
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("reject".to_string())),
            VolatileFunctionPolicy::Reject
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("unexpected".to_string())),
            VolatileFunctionPolicy::Reject
        );
    }

    #[test]
    fn test_normalize_volatile_function_policy_accepts_warn_and_allow() {
        assert_eq!(
            normalize_volatile_function_policy(Some("warn".to_string())),
            VolatileFunctionPolicy::Warn
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("WARN".to_string())),
            VolatileFunctionPolicy::Warn
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("allow".to_string())),
            VolatileFunctionPolicy::Allow
        );
        assert_eq!(
            normalize_volatile_function_policy(Some("ALLOW".to_string())),
            VolatileFunctionPolicy::Allow
        );
    }

    #[test]
    fn test_volatile_function_policy_as_str() {
        assert_eq!(VolatileFunctionPolicy::Reject.as_str(), "reject");
        assert_eq!(VolatileFunctionPolicy::Warn.as_str(), "warn");
        assert_eq!(VolatileFunctionPolicy::Allow.as_str(), "allow");
    }

    #[test]
    fn test_normalize_volatile_function_policy_roundtrip_via_as_str() {
        for policy in [
            VolatileFunctionPolicy::Reject,
            VolatileFunctionPolicy::Warn,
            VolatileFunctionPolicy::Allow,
        ] {
            assert_eq!(
                normalize_volatile_function_policy(Some(policy.as_str().to_string())),
                policy
            );
        }
    }

    #[test]
    fn test_normalize_merge_join_strategy_defaults_to_auto() {
        assert_eq!(normalize_merge_join_strategy(None), MergeJoinStrategy::Auto);
        assert_eq!(
            normalize_merge_join_strategy(Some("auto".to_string())),
            MergeJoinStrategy::Auto
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("unexpected".to_string())),
            MergeJoinStrategy::Auto
        );
    }

    #[test]
    fn test_normalize_merge_join_strategy_all_variants() {
        assert_eq!(
            normalize_merge_join_strategy(Some("hash_join".to_string())),
            MergeJoinStrategy::HashJoin
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("HASH_JOIN".to_string())),
            MergeJoinStrategy::HashJoin
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("nested_loop".to_string())),
            MergeJoinStrategy::NestedLoop
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("NESTED_LOOP".to_string())),
            MergeJoinStrategy::NestedLoop
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("merge_join".to_string())),
            MergeJoinStrategy::MergeJoin
        );
        assert_eq!(
            normalize_merge_join_strategy(Some("MERGE_JOIN".to_string())),
            MergeJoinStrategy::MergeJoin
        );
    }

    #[test]
    fn test_merge_join_strategy_as_str() {
        assert_eq!(MergeJoinStrategy::Auto.as_str(), "auto");
        assert_eq!(MergeJoinStrategy::HashJoin.as_str(), "hash_join");
        assert_eq!(MergeJoinStrategy::NestedLoop.as_str(), "nested_loop");
        assert_eq!(MergeJoinStrategy::MergeJoin.as_str(), "merge_join");
    }

    #[test]
    fn test_normalize_merge_join_strategy_roundtrip_via_as_str() {
        for strategy in [
            MergeJoinStrategy::Auto,
            MergeJoinStrategy::HashJoin,
            MergeJoinStrategy::NestedLoop,
            MergeJoinStrategy::MergeJoin,
        ] {
            assert_eq!(
                normalize_merge_join_strategy(Some(strategy.as_str().to_string())),
                strategy
            );
        }
    }

    #[test]
    fn test_normalize_merge_strategy_defaults_to_auto() {
        assert_eq!(normalize_merge_strategy(None), MergeStrategy::Auto);
        assert_eq!(
            normalize_merge_strategy(Some("".to_string())),
            MergeStrategy::Auto
        );
        assert_eq!(
            normalize_merge_strategy(Some("garbage".to_string())),
            MergeStrategy::Auto
        );
    }

    #[test]
    fn test_normalize_merge_strategy_all_variants() {
        assert_eq!(
            normalize_merge_strategy(Some("merge".to_string())),
            MergeStrategy::Merge
        );
        // CORR-1: delete_insert now falls back to Auto with a warning
        assert_eq!(
            normalize_merge_strategy(Some("delete_insert".to_string())),
            MergeStrategy::Auto
        );
        assert_eq!(
            normalize_merge_strategy(Some("auto".to_string())),
            MergeStrategy::Auto
        );
        // Case-insensitive
        assert_eq!(
            normalize_merge_strategy(Some("DELETE_INSERT".to_string())),
            MergeStrategy::Auto
        );
        assert_eq!(
            normalize_merge_strategy(Some("MERGE".to_string())),
            MergeStrategy::Merge
        );
    }

    #[test]
    fn test_normalize_merge_strategy_roundtrip_via_as_str() {
        for strategy in [MergeStrategy::Auto, MergeStrategy::Merge] {
            assert_eq!(
                normalize_merge_strategy(Some(strategy.as_str().to_string())),
                strategy
            );
        }
    }

    // ── B-4: RefreshStrategy normalizer tests ───────────────────────

    #[test]
    fn test_normalize_refresh_strategy_defaults_to_auto() {
        assert_eq!(normalize_refresh_strategy(None), RefreshStrategy::Auto);
        assert_eq!(
            normalize_refresh_strategy(Some("auto".to_string())),
            RefreshStrategy::Auto
        );
        assert_eq!(
            normalize_refresh_strategy(Some("unexpected".to_string())),
            RefreshStrategy::Auto
        );
    }

    #[test]
    fn test_normalize_refresh_strategy_all_variants() {
        assert_eq!(
            normalize_refresh_strategy(Some("differential".to_string())),
            RefreshStrategy::Differential
        );
        assert_eq!(
            normalize_refresh_strategy(Some("DIFFERENTIAL".to_string())),
            RefreshStrategy::Differential
        );
        assert_eq!(
            normalize_refresh_strategy(Some("full".to_string())),
            RefreshStrategy::Full
        );
        assert_eq!(
            normalize_refresh_strategy(Some("FULL".to_string())),
            RefreshStrategy::Full
        );
    }

    #[test]
    fn test_refresh_strategy_as_str() {
        assert_eq!(RefreshStrategy::Auto.as_str(), "auto");
        assert_eq!(RefreshStrategy::Differential.as_str(), "differential");
        assert_eq!(RefreshStrategy::Full.as_str(), "full");
    }

    #[test]
    fn test_normalize_refresh_strategy_roundtrip_via_as_str() {
        for strategy in [
            RefreshStrategy::Auto,
            RefreshStrategy::Differential,
            RefreshStrategy::Full,
        ] {
            assert_eq!(
                normalize_refresh_strategy(Some(strategy.as_str().to_string())),
                strategy
            );
        }
    }

    // Note: GUC default value tests (PGS_WATERMARK_HOLDBACK_TIMEOUT,
    // PGS_SPILL_THRESHOLD_BLOCKS, PGS_SPILL_CONSECUTIVE_LIMIT) require a
    // PostgreSQL backend and are covered by E2E tests.  Calling
    // `GucSetting::get()` in multi-threaded unit tests triggers pgrx's
    // "postgres FFI may not be called from multiple threads" guard.

    // ── DF-G1: SelfMonitoringAutoApply normalizer tests ────────────────

    #[test]
    fn test_normalize_self_monitoring_auto_apply_defaults_to_off() {
        assert_eq!(
            normalize_self_monitoring_auto_apply(None),
            SelfMonitoringAutoApply::Off
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("off".to_string())),
            SelfMonitoringAutoApply::Off
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("unexpected".to_string())),
            SelfMonitoringAutoApply::Off
        );
    }

    #[test]
    fn test_normalize_self_monitoring_auto_apply_all_variants() {
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("threshold_only".to_string())),
            SelfMonitoringAutoApply::ThresholdOnly
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("THRESHOLD_ONLY".to_string())),
            SelfMonitoringAutoApply::ThresholdOnly
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("full".to_string())),
            SelfMonitoringAutoApply::Full
        );
        assert_eq!(
            normalize_self_monitoring_auto_apply(Some("FULL".to_string())),
            SelfMonitoringAutoApply::Full
        );
    }

    #[test]
    fn test_self_monitoring_auto_apply_as_str() {
        assert_eq!(SelfMonitoringAutoApply::Off.as_str(), "off");
        assert_eq!(
            SelfMonitoringAutoApply::ThresholdOnly.as_str(),
            "threshold_only"
        );
        assert_eq!(SelfMonitoringAutoApply::Full.as_str(), "full");
    }

    #[test]
    fn test_normalize_self_monitoring_auto_apply_roundtrip() {
        for mode in [
            SelfMonitoringAutoApply::Off,
            SelfMonitoringAutoApply::ThresholdOnly,
            SelfMonitoringAutoApply::Full,
        ] {
            assert_eq!(
                normalize_self_monitoring_auto_apply(Some(mode.as_str().to_string())),
                mode
            );
        }
    }

    // ── v0.23.0: DiffOutputFormat normalizer tests ─────────────────

    #[test]
    fn test_normalize_diff_output_format_defaults_to_split() {
        assert_eq!(normalize_diff_output_format(None), DiffOutputFormat::Split);
        assert_eq!(
            normalize_diff_output_format(Some("split".to_string())),
            DiffOutputFormat::Split
        );
        assert_eq!(
            normalize_diff_output_format(Some("unexpected".to_string())),
            DiffOutputFormat::Split
        );
    }

    #[test]
    fn test_normalize_diff_output_format_accepts_merged() {
        assert_eq!(
            normalize_diff_output_format(Some("merged".to_string())),
            DiffOutputFormat::Merged
        );
        assert_eq!(
            normalize_diff_output_format(Some("MERGED".to_string())),
            DiffOutputFormat::Merged
        );
    }

    #[test]
    fn test_diff_output_format_as_str() {
        assert_eq!(DiffOutputFormat::Split.as_str(), "split");
        assert_eq!(DiffOutputFormat::Merged.as_str(), "merged");
    }

    #[test]
    fn test_normalize_diff_output_format_roundtrip() {
        for fmt in [DiffOutputFormat::Split, DiffOutputFormat::Merged] {
            assert_eq!(
                normalize_diff_output_format(Some(fmt.as_str().to_string())),
                fmt
            );
        }
    }

    // ── #536: FrontierHoldbackMode normalizer tests ──────────────────

    #[test]
    fn test_normalize_frontier_holdback_mode_defaults_to_xmin() {
        assert_eq!(
            normalize_frontier_holdback_mode(None),
            FrontierHoldbackMode::Xmin
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("xmin".to_string())),
            FrontierHoldbackMode::Xmin
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("XMIN".to_string())),
            FrontierHoldbackMode::Xmin
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("unexpected".to_string())),
            FrontierHoldbackMode::Xmin
        );
    }

    #[test]
    fn test_normalize_frontier_holdback_mode_none() {
        assert_eq!(
            normalize_frontier_holdback_mode(Some("none".to_string())),
            FrontierHoldbackMode::None
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("NONE".to_string())),
            FrontierHoldbackMode::None
        );
    }

    #[test]
    fn test_normalize_frontier_holdback_mode_lsn_bytes() {
        assert_eq!(
            normalize_frontier_holdback_mode(Some("lsn:1048576".to_string())),
            FrontierHoldbackMode::LsnBytes(1_048_576)
        );
        assert_eq!(
            normalize_frontier_holdback_mode(Some("lsn:0".to_string())),
            FrontierHoldbackMode::LsnBytes(0)
        );
        // Invalid number → returns InvalidLsn sentinel (accessor converts to Xmin + warns)
        assert_eq!(
            normalize_frontier_holdback_mode(Some("lsn:notanumber".to_string())),
            FrontierHoldbackMode::InvalidLsn
        );
    }

    // ── v0.36.0: LogFormat normalizer tests ───────────────────────────────

    #[test]
    fn test_normalize_log_format_defaults_to_text() {
        assert_eq!(normalize_log_format(None), LogFormat::Text);
        assert_eq!(
            normalize_log_format(Some("text".to_string())),
            LogFormat::Text
        );
        assert_eq!(
            normalize_log_format(Some("unexpected".to_string())),
            LogFormat::Text
        );
    }

    #[test]
    fn test_normalize_log_format_accepts_json() {
        assert_eq!(
            normalize_log_format(Some("json".to_string())),
            LogFormat::Json
        );
        assert_eq!(
            normalize_log_format(Some("JSON".to_string())),
            LogFormat::Json
        );
    }

    #[test]
    fn test_log_format_as_str() {
        assert_eq!(LogFormat::Text.as_str(), "text");
        assert_eq!(LogFormat::Json.as_str(), "json");
    }

    // ── v0.36.0: ColumnarBackend normalizer tests ─────────────────────────

    #[test]
    fn test_normalize_columnar_backend_defaults_to_none() {
        assert_eq!(normalize_columnar_backend(None), ColumnarBackend::None);
        assert_eq!(
            normalize_columnar_backend(Some("none".to_string())),
            ColumnarBackend::None
        );
        assert_eq!(
            normalize_columnar_backend(Some("unexpected".to_string())),
            ColumnarBackend::None
        );
    }

    #[test]
    fn test_normalize_columnar_backend_all_variants() {
        assert_eq!(
            normalize_columnar_backend(Some("citus".to_string())),
            ColumnarBackend::Citus
        );
        assert_eq!(
            normalize_columnar_backend(Some("CITUS".to_string())),
            ColumnarBackend::Citus
        );
        assert_eq!(
            normalize_columnar_backend(Some("pg_mooncake".to_string())),
            ColumnarBackend::PgMooncake
        );
        assert_eq!(
            normalize_columnar_backend(Some("PG_MOONCAKE".to_string())),
            ColumnarBackend::PgMooncake
        );
    }

    #[test]
    fn test_columnar_backend_is_append_only() {
        assert!(!ColumnarBackend::None.is_append_only());
        assert!(ColumnarBackend::Citus.is_append_only());
        assert!(ColumnarBackend::PgMooncake.is_append_only());
    }

    #[test]
    fn test_columnar_backend_as_str() {
        assert_eq!(ColumnarBackend::None.as_str(), "none");
        assert_eq!(ColumnarBackend::Citus.as_str(), "citus");
        assert_eq!(ColumnarBackend::PgMooncake.as_str(), "pg_mooncake");
    }

    // ── DucklakeCompactionPolicy tests (v0.65.0) ─────────────────────────

    #[test]
    fn test_normalize_ducklake_compaction_policy_defaults_to_fallback() {
        // None (GUC not set) and unknown strings must default to Fallback.
        assert_eq!(
            normalize_ducklake_compaction_policy(None),
            DucklakeCompactionPolicy::Fallback
        );
        assert_eq!(
            normalize_ducklake_compaction_policy(Some("unknown".to_string())),
            DucklakeCompactionPolicy::Fallback
        );
    }

    #[test]
    fn test_normalize_ducklake_compaction_policy_accepts_error() {
        assert_eq!(
            normalize_ducklake_compaction_policy(Some("error".to_string())),
            DucklakeCompactionPolicy::Error
        );
        // Must be case-insensitive.
        assert_eq!(
            normalize_ducklake_compaction_policy(Some("ERROR".to_string())),
            DucklakeCompactionPolicy::Error
        );
        assert_eq!(
            normalize_ducklake_compaction_policy(Some("Error".to_string())),
            DucklakeCompactionPolicy::Error
        );
    }

    #[test]
    fn test_normalize_ducklake_compaction_policy_accepts_fallback() {
        assert_eq!(
            normalize_ducklake_compaction_policy(Some("fallback".to_string())),
            DucklakeCompactionPolicy::Fallback
        );
        assert_eq!(
            normalize_ducklake_compaction_policy(Some("FALLBACK".to_string())),
            DucklakeCompactionPolicy::Fallback
        );
    }

    #[test]
    fn test_ducklake_compaction_policy_as_str() {
        assert_eq!(DucklakeCompactionPolicy::Fallback.as_str(), "fallback");
        assert_eq!(DucklakeCompactionPolicy::Error.as_str(), "error");
    }
}
