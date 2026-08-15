//! DVM (Differential View Maintenance), query-differentiation, merge, and
//! template-cache GUCs.

use pgrx::guc::*;

// ── GUC statics ───────────────────────────────────────────────────────────

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

/// User trigger handling mode enum.
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

pub(crate) fn normalize_user_triggers_mode(value: Option<String>) -> UserTriggersMode {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("off") => UserTriggersMode::Off,
        _ => UserTriggersMode::Auto,
    }
}

/// B-1: Aggregate fast-path — use explicit DML instead of MERGE for
/// GROUP BY queries where all aggregates are algebraically invertible
/// (COUNT, SUM, AVG, etc.).  The explicit DML path (DELETE+UPDATE+INSERT)
/// avoids the MERGE hash-join cost, which is the dominant overhead for
/// aggregate stream tables with many groups.
pub static PGS_AGGREGATE_FAST_PATH: GucSetting<bool> = GucSetting::<bool>::new(true);

/// G14-SHC: Enable the cross-backend template cache backed by an UNLOGGED
/// catalog table (`pgtrickle.pgt_template_cache`).
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

/// VOL-1: Legacy volatility policy setting.
///
/// Controls how volatile functions in defining queries are handled:
/// - `"reject"` (default): report the unsafe expression.
/// - `"warn"` / `"allow"`: retained for compatibility, but cannot override
///   the v0.83 fail-closed incremental admission gate.
pub static PGS_VOLATILE_FUNCTION_POLICY: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"reject"));

/// Volatile function policy enum.
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

pub(crate) fn normalize_volatile_function_policy(value: Option<String>) -> VolatileFunctionPolicy {
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

/// Merge join strategy enum.
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

pub(crate) fn normalize_merge_join_strategy(value: Option<String>) -> MergeJoinStrategy {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("hash_join") => MergeJoinStrategy::HashJoin,
        Some("nested_loop") => MergeJoinStrategy::NestedLoop,
        Some("merge_join") => MergeJoinStrategy::MergeJoin,
        _ => MergeJoinStrategy::Auto,
    }
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

/// Merge strategy enum.
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

pub(crate) fn normalize_merge_strategy(value: Option<String>) -> MergeStrategy {
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

/// QF-1: When `true`, the MERGE SQL template is emitted to the PostgreSQL
/// server log at `LOG` level on every refresh cycle.
///
/// Intended for debugging MERGE query generation only. **Do not enable in
/// production** — every refresh will emit potentially large SQL strings to
/// the server log.
pub static PGS_LOG_MERGE_SQL: GucSetting<bool> = GucSetting::<bool>::new(false);

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

pub(crate) fn normalize_diff_output_format(value: Option<String>) -> DiffOutputFormat {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("merged") => DiffOutputFormat::Merged,
        _ => DiffOutputFormat::Split,
    }
}

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
pub static PGS_FRONTIER_HOLDBACK_WARN_SECONDS: GucSetting<i32> = GucSetting::<i32>::new(60);

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
///
/// When set, `create_stream_table()` uses the specified columnar backend and
/// routes differential refresh to the `delete_insert` strategy (columnar
/// backends are append-only).
pub static PGS_COLUMNAR_BACKEND: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"none"));

/// CORR-2 (v0.36.0): Columnar backend enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnarBackend {
    /// Standard heap storage (default).
    None,
    /// Citus columnar extension.
    Citus,
}

impl ColumnarBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnarBackend::None => "none",
            ColumnarBackend::Citus => "citus",
        }
    }

    /// Returns `true` if this backend is append-only (requires `delete_insert` strategy).
    pub fn is_append_only(self) -> bool {
        matches!(self, ColumnarBackend::Citus)
    }
}

pub fn normalize_columnar_backend(value: Option<String>) -> ColumnarBackend {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("citus") => ColumnarBackend::Citus,
        _ => ColumnarBackend::None,
    }
}

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

/// Register all DVM-related GUC variables.
pub fn register_dvm_gucs() {
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

    // VOL-1: Legacy volatility policy (the admission gate remains fail-closed).
    GucRegistry::define_string_guc(
        c"pg_trickle.volatile_function_policy",
        c"Legacy volatile function policy: reject, warn, or allow.",
        c"Volatility is FULL-only for incremental admission in v0.83. This setting is retained \
           for compatibility and cannot override that guardrail.",
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
        c"CORR-2: Columnar storage backend: none (default) or citus.",
        c"'none' (default) uses standard heap tables. \
          'citus' uses Citus columnar (CREATE TABLE ... USING columnar). \
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

    // ── v0.81.0 GUCs ─────────────────────────────────────────────────────

    // QW-5: Bounded L0/L1 thread-local template cache.
    GucRegistry::define_int_guc(
        c"pg_trickle.l1_cache_max_entries",
        c"QW-5: Maximum entries in the L0/L1 thread-local delta template and resolver caches.",
        c"When non-zero, the thread-local DELTA_TEMPLATE_CACHE and PLACEHOLDER_RESOLVER_CACHE \
          evict the least-recently-used entry once the limit is reached. \
          Default 256. Set to 0 for the legacy unbounded behaviour.",
        &PGS_L1_CACHE_MAX_ENTRIES,
        0,     // min (0 = unbounded)
        65536, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // QW-9: Chunked MERGE for large deltas.
    GucRegistry::define_int_guc(
        c"pg_trickle.merge_batch_size",
        c"QW-9: Delta rows above which the MERGE is split into batches.",
        c"When the delta result set exceeds this row count, the refresh executor \
          materialises the delta into a temporary table and runs the MERGE in \
          windows of this many rows. Reduces peak memory and lock hold time for \
          large deltas. Default 50000. Set to 0 to disable chunking.",
        &PGS_MERGE_BATCH_SIZE,
        0,          // min (0 = disabled)
        10_000_000, // max
        GucContext::Suset,
        GucFlags::default(),
    );

    // QW-1: Commit-to-visible latency tracking.
    GucRegistry::define_bool_guc(
        c"pg_trickle.commit_timestamp_tracking",
        c"QW-1: Enable commit-to-visible latency tracking via pg_xact_commit_timestamp().",
        c"When true (and track_commit_timestamp = on), the refresh executor records \
          the wall-clock latency from source transaction commit to stream-table \
          visibility. Exposed via pgtrickle.commit_latency_stats(). Default false.",
        &PGS_COMMIT_TIMESTAMP_TRACKING,
        GucContext::Suset,
        GucFlags::default(),
    );
}

// ── Accessor functions ────────────────────────────────────────────────────

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

/// B-1: Returns whether the aggregate fast-path (explicit DML for
/// all-algebraic aggregate queries) is enabled.
pub fn pg_trickle_aggregate_fast_path() -> bool {
    PGS_AGGREGATE_FAST_PATH.get()
}

/// G14-SHC: Returns whether the cross-backend template cache is enabled.
pub fn pg_trickle_template_cache_enabled() -> bool {
    PGS_TEMPLATE_CACHE.get()
}

/// Returns the number of differential cycles before automatic drift reset.
pub fn pg_trickle_algebraic_drift_reset_cycles() -> i32 {
    PGS_ALGEBRAIC_DRIFT_RESET_CYCLES.get()
}

/// Returns the delta-to-ST ratio threshold for disabling seqscan before MERGE.
pub fn pg_trickle_merge_seqscan_threshold() -> f64 {
    PGS_MERGE_SEQSCAN_THRESHOLD.get()
}

/// QF-1: Returns whether MERGE SQL template logging is enabled.
pub fn pg_trickle_log_merge_sql() -> bool {
    PGS_LOG_MERGE_SQL.get()
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

/// STAB-3 (v0.30.0): Returns the maximum age (hours) for L2 template cache entries.
pub fn pg_trickle_template_cache_max_age_hours() -> i32 {
    PGS_TEMPLATE_CACHE_MAX_AGE_HOURS.get()
}

/// PERF-2 (v0.30.0): Returns the maximum parse node count allowed per query.
pub fn pg_trickle_max_parse_nodes() -> usize {
    PGS_MAX_PARSE_NODES.get() as usize
}

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

/// Returns the maximum recursion depth for WITH RECURSIVE in IMMEDIATE mode.
/// Returns `None` when the guard is disabled (value = 0).
pub fn pg_trickle_ivm_recursive_max_depth() -> Option<i32> {
    crate::config::cdc::normalize_recursive_max_depth(PGS_IVM_RECURSIVE_MAX_DEPTH.get())
}

// ── v0.81.0 GUC statics ───────────────────────────────────────────────────

/// QW-5 (v0.81.0): Maximum entries in the thread-local L0/L1 delta template
/// and placeholder-resolver caches.
///
/// Default: 256. In 10K-ST deployments, unbounded growth in long-lived backend
/// sessions can consume significant per-backend memory. Setting this caps the
/// in-process cache and evicts the least-recently-used entry on overflow.
/// Set to 0 for the legacy unbounded behaviour.
///
/// Note: `pg_trickle.template_cache_max_entries` caps the L2 (MERGE template)
/// cache; this GUC caps the L0/L1 (delta-template / placeholder-resolver)
/// caches that live in `src/dvm/mod.rs`.
pub static PGS_L1_CACHE_MAX_ENTRIES: GucSetting<i32> = GucSetting::<i32>::new(256);

/// QW-9 (v0.81.0): Delta row count above which the MERGE is split into
/// batched chunks to reduce peak memory and lock hold time.
///
/// When the number of rows in the delta result set exceeds this threshold,
/// the refresh executor:
/// 1. Materialises the delta into a temporary table.
/// 2. Runs the MERGE in windows of `merge_batch_size` rows.
/// 3. Drops the temporary table.
///
/// Default: 50 000. Set to 0 to disable chunking (use the single large MERGE).
pub static PGS_MERGE_BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(50_000);

/// QW-1 (v0.81.0): Enable commit-to-visible latency tracking.
///
/// When `true` (and `track_commit_timestamp = on` in PostgreSQL), the refresh
/// executor reads `pg_xact_commit_timestamp()` from the earliest change-buffer
/// row in each batch and records the wall-clock latency from source transaction
/// commit to stream-table visibility in `pgt_refresh_history.commit_to_visible_ms`.
///
/// Default: `false` (disabled to avoid overhead when `track_commit_timestamp`
/// is off).
pub static PGS_COMMIT_TIMESTAMP_TRACKING: GucSetting<bool> = GucSetting::<bool>::new(false);

// ── v0.81.0 accessor functions ────────────────────────────────────────────

/// QW-5 (v0.81.0): Returns the L0/L1 thread-local cache max entries (0 = unbounded).
pub fn pg_trickle_l1_cache_max_entries() -> i32 {
    PGS_L1_CACHE_MAX_ENTRIES.get().max(0)
}

/// QW-9 (v0.81.0): Returns the chunked-MERGE batch size (0 = disabled).
pub fn pg_trickle_merge_batch_size() -> i32 {
    PGS_MERGE_BATCH_SIZE.get().max(0)
}

/// QW-1 (v0.81.0): Returns whether commit-timestamp latency tracking is enabled.
pub fn pg_trickle_commit_timestamp_tracking() -> bool {
    PGS_COMMIT_TIMESTAMP_TRACKING.get()
}
