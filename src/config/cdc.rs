//! CDC (Change-Data-Capture), change-buffer, and WAL-related GUCs.

use pgrx::guc::*;

// ── Helper ────────────────────────────────────────────────────────────────

pub(crate) fn threshold_mb_to_bytes(megabytes: i32) -> i64 {
    megabytes as i64 * 1024 * 1024
}

// ── GUC statics ───────────────────────────────────────────────────────────

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
pub static PGS_CHANGE_BUFFER_DURABILITY: GucSetting<ChangeBufferDurability> =
    GucSetting::<ChangeBufferDurability>::new(ChangeBufferDurability::Logged);

/// DUR-2: Change buffer durability mode enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PostgresGucEnum)]
pub enum ChangeBufferDurability {
    /// WAL-logged tables — survives crash, replicated.
    #[name = c"logged"]
    Logged,
    /// UNLOGGED tables — maximum performance, lost on crash.
    #[name = c"unlogged"]
    Unlogged,
    /// WAL-logged + synchronous_commit — maximum durability.
    #[name = c"sync"]
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

pub(crate) fn normalize_change_buffer_durability(value: Option<String>) -> ChangeBufferDurability {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("logged") => ChangeBufferDurability::Logged,
        Some("unlogged") => ChangeBufferDurability::Unlogged,
        Some("sync") => ChangeBufferDurability::Sync,
        _ => ChangeBufferDurability::Logged,
    }
}

/// CDC trigger granularity enum.
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

pub(crate) fn normalize_cdc_trigger_mode(value: Option<String>) -> CdcTriggerMode {
    match value.as_deref().map(str::to_ascii_lowercase).as_deref() {
        Some("row") => CdcTriggerMode::Row,
        _ => CdcTriggerMode::Statement,
    }
}

/// SCAL-2: Maximum change buffer rows per source before emitting an alert.
///
/// When non-zero, the refresh executor checks the change buffer row count
/// and emits a `pg_trickle_alert change_buffer_overflow` event if it
/// exceeds this threshold. Prevents the WAL accumulation pattern from
/// going undetected in production.
///
/// Set to 0 to disable (default).
pub static PGS_MAX_CHANGE_BUFFER_ALERT_ROWS: GucSetting<i32> = GucSetting::<i32>::new(0);

/// A07 (v0.35.0): When `true`, CDC trigger bodies return `NULL` (no-op) and
/// the change buffer is not written. Provides a durable hold that survives
/// session reconnects, unlike `pg_trickle.enabled = false` which only stops
/// the scheduler.
///
/// Default: `false` (CDC writes are enabled).
pub static PGS_CDC_PAUSED: GucSetting<bool> = GucSetting::<bool>::new(false);

/// A12 (v0.36.0): Enforce WAL backpressure when slot lag exceeds the critical threshold.
///
/// When `true`, CDC trigger writes are paused when the WAL slot lag exceeds
/// `pg_trickle.slot_lag_critical_threshold_mb`. Writes resume when lag drops
/// below 50% of the threshold. This prevents disk exhaustion at the cost of
/// temporary change-buffer growth.
///
/// Default: `false` (alerts only, no throttling).
pub static PGS_ENFORCE_BACKPRESSURE: GucSetting<bool> = GucSetting::<bool>::new(false);

/// O39-8 (v0.39.0): CDC capture mode — explicit discard vs hold semantics.
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

/// Register all CDC-related GUC variables.
pub fn register_cdc_gucs() {
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
    GucRegistry::define_enum_guc(
        c"pg_trickle.change_buffer_durability",
        c"Change buffer durability: logged (default), unlogged, or sync.",
        c"'logged' (default) creates WAL-logged change buffers; survives crash, replicated. \
           'unlogged' creates UNLOGGED change buffers for max throughput; \
           lost on crash (auto FULL refresh on recovery). \
           'sync' adds synchronous_commit for maximum durability. \
           Supersedes pg_trickle.unlogged_buffers (compatibility alias).",
        &PGS_CHANGE_BUFFER_DURABILITY,
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
}

// ── Accessor functions ────────────────────────────────────────────────────

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

/// Returns whether TRUNCATE cleanup is enabled.
pub fn pg_trickle_cleanup_use_truncate() -> bool {
    PGS_CLEANUP_USE_TRUNCATE.get()
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

/// Returns the CDC trigger granularity mode.
pub fn pg_trickle_cdc_trigger_mode() -> CdcTriggerMode {
    normalize_cdc_trigger_mode(
        PGS_CDC_TRIGGER_MODE
            .get()
            .and_then(|cs| cs.to_str().ok().map(str::to_owned)),
    )
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
    PGS_CHANGE_BUFFER_DURABILITY.get()
}

/// SCAL-2: Returns the change buffer overflow alert threshold (0 = disabled).
pub fn pg_trickle_max_change_buffer_alert_rows() -> i64 {
    PGS_MAX_CHANGE_BUFFER_ALERT_ROWS.get() as i64
}

/// A07 (v0.35.0): Returns whether CDC writes are paused cluster-wide.
pub fn pg_trickle_cdc_paused() -> bool {
    PGS_CDC_PAUSED.get()
}

/// A12 (v0.36.0): Returns whether WAL backpressure enforcement is enabled.
pub fn pg_trickle_enforce_backpressure() -> bool {
    PGS_ENFORCE_BACKPRESSURE.get()
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

/// PUB-1: Returns the publication subscriber lag warning threshold in bytes (0 = disabled).
pub fn pg_trickle_publication_lag_warn_bytes() -> i64 {
    PGS_PUBLICATION_LAG_WARN_BYTES.get() as i64
}

/// PERF-4 (v0.31.0): Returns whether ENR-based IVM trigger mode is enabled.
pub fn pg_trickle_ivm_use_enr() -> bool {
    PGS_IVM_USE_ENR.get()
}

pub(crate) fn normalize_recursive_max_depth(value: i32) -> Option<i32> {
    if value > 0 { Some(value) } else { None }
}
