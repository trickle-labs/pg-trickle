// ARCH-1B: SQL code-generation sub-module for the refresh pipeline.
//
// Contains: SQL template builders, MERGE SQL cache, prepared-statement
// helpers, change-buffer cleanup, planner hints, ST-to-ST delta capture.
//
// Q-1 (v0.79.0): Removed per-import #[allow(unused_imports)] shims and the
// unused `use super::*` glob. Imports are now concrete; rustc/clippy will
// warn if any become stale, catching stale imports early.

use crate::catalog::StreamTableMeta;

use crate::dvm;
use crate::error::PgTrickleError;
use crate::version::Frontier;
use lru::LruCache;
use pgrx::prelude::*;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;

// ── MERGE SQL template cache ────────────────────────────────────────

/// Cached MERGE SQL template for a stream table.
///
/// The template has LSN placeholders embedded in the delta SQL portion.
/// It also stores the MERGE "shell" (the parts that wrap the delta SQL),
/// the source OIDs for placeholder resolution, and the cleanup DO block
/// template.
#[derive(Clone)]
pub(crate) struct CachedMergeTemplate {
    /// Hash of the defining query — invalidation key.
    pub(crate) defining_query_hash: u64,
    /// MERGE SQL template with `__PGS_PREV_LSN_{oid}__` / `__PGS_NEW_LSN_{oid}__`
    /// placeholder tokens. Resolved to concrete LSN values before each execution.
    /// PERF-3: stored as Arc<str> so cache-hit clones are O(1) ref-count increments.
    pub(crate) merge_sql_template: Arc<str>,
    /// Parameterized MERGE SQL with `$1`, `$2`, … for LSN values (D-2).
    /// Parameter order: for each source OID (in `source_oids` order),
    /// `$2i-1` = prev_lsn, `$2i` = new_lsn.
    pub(crate) parameterized_merge_sql: Arc<str>,
    /// Source OIDs for LSN placeholder resolution.
    pub(crate) source_oids: Vec<u32>,
    /// Cleanup template with `__PGS_{PREV,NEW}_LSN_{oid}__` tokens.
    pub(crate) cleanup_sql_template: Arc<str>,

    // ── User-trigger explicit DML templates ──────────────────────────
    // These templates reference `__pgt_delta_{pgt_id}` (a temp table
    // materialized at execution time) and do NOT contain LSN placeholders.
    /// DELETE statement for the trigger-enabled DML path.
    /// Deletes rows where the delta action is 'D'.
    pub(crate) trigger_delete_template: Arc<str>,
    /// UPDATE statement for the trigger-enabled DML path.
    /// Updates existing rows where the delta action is 'I' and values changed.
    pub(crate) trigger_update_template: Arc<str>,
    /// INSERT statement for the trigger-enabled DML path.
    /// Inserts genuinely new rows where the delta action is 'I'.
    pub(crate) trigger_insert_template: Arc<str>,
    /// USING clause template with LSN placeholders (for materializing delta
    /// into a temp table in the user-trigger path).
    pub(crate) trigger_using_template: Arc<str>,
    /// A1-2: Raw delta SQL template with LSN placeholders.
    /// Used at refresh time to compute partition key range (MIN/MAX) for A1-3
    /// predicate injection. Only populated when `st_partition_key` is set,
    /// but stored for all STs to keep the struct layout consistent.
    pub(crate) delta_sql_template: Arc<str>,
    /// B-1: When true, all aggregates are algebraically invertible and the
    /// explicit DML fast-path can be used instead of MERGE.
    pub(crate) is_all_algebraic: bool,
    /// When true, the delta output has at most one row per `__pgt_row_id`.
    /// Non-deduplicated deltas (joins) may produce phantom rows that require
    /// PH-D1 with ON CONFLICT rather than MERGE.
    pub(crate) is_deduplicated: bool,
}

/// P-8: Default capacity for the MERGE template `LruCache`.
///
/// When `pg_trickle.template_cache_max_entries == 0` (the default, meaning
/// "unbounded"), the LruCache is initialised with this large capacity so that
/// no entry is ever evicted in practice. When the GUC is set to a positive
/// value, `lru_cache_resize_if_needed()` adjusts the capacity before the next
/// insertion, enabling O(1) eviction via the `lru` crate's built-in LRU logic.
// 65536 is a positive non-zero compile-time constant. This `expect()` evaluates
// at compile time and can never be reached from a SQL-callable function at runtime.
const LRU_DEFAULT_UNBOUNDED_CAP: NonZeroUsize =
    NonZeroUsize::new(65536).expect("LRU_DEFAULT_UNBOUNDED_CAP must be non-zero"); // nosemgrep: semgrep.rust.panic-in-sql-path

thread_local! {
    /// Per-session cache of MERGE SQL templates, keyed by `pgt_id`.
    ///
    /// Cross-session invalidation (G8.1): flushed when the shared
    /// `CACHE_GENERATION` counter advances.
    ///
    /// P-8 / CACHE-2: Replaced `HashMap` + O(N) manual LRU scan with
    /// `lru::LruCache` which provides O(1) put/get/eviction.
    /// `LruCache::put()` automatically evicts the least-recently-used entry
    /// when the cache is at capacity.
    pub(crate) static MERGE_TEMPLATE_CACHE: RefCell<LruCache<i64, CachedMergeTemplate>> =
        RefCell::new(LruCache::new(LRU_DEFAULT_UNBOUNDED_CAP));

    /// Local snapshot of the shared `CACHE_GENERATION` counter.
    pub(crate) static LOCAL_MERGE_CACHE_GEN: Cell<u64> = const { Cell::new(0) };
}

// ── D-2: Prepared statement tracking ────────────────────────────────

thread_local! {
    /// Tracks which `pgt_id`s have a SQL `PREPARE`d MERGE statement
    /// in the current session.  Used by the prepared-statement path
    /// to skip re-issuing `PREPARE` on cache-hit refreshes.
    pub(crate) static PREPARED_MERGE_STMTS: RefCell<HashSet<i64>> =
        RefCell::new(HashSet::new());
}

// ── C-1: Deferred change buffer cleanup ─────────────────────────────

/// Pending cleanup work from a previous differential refresh.
///
/// Instead of cleaning up consumed change buffer rows synchronously in
/// the critical path, we defer the cleanup to the start of the next
/// refresh cycle.
///
/// **IMPORTANT:** Multiple stream tables may share the same change buffer
/// (source table).  The cleanup must only delete entries that ALL consumers
/// have already processed.  We use the minimum frontier across all STs
/// that depend on each source OID as the safe cleanup threshold.
pub(crate) struct PendingCleanup {
    pub(crate) change_schema: String,
    pub(crate) source_oids: Vec<u32>,
}

thread_local! {
    /// Queue of deferred cleanup operations from previous refreshes.
    pub(crate) static PENDING_CLEANUP: RefCell<Vec<PendingCleanup>> = const { RefCell::new(Vec::new()) };
}

thread_local! {
    // NS-3: Consecutive failure counts for deferred cleanup per source OID.
    // Emits a WARNING after the 3rd consecutive failure so operators are
    // alerted to persistent cleanup problems without flooding the log.
    static CLEANUP_FAILURE_COUNTS: RefCell<std::collections::HashMap<u32, u32>> =
        RefCell::new(std::collections::HashMap::new());
}

fn upsert_cleanup_status(
    source_oid: u32,
    buffer_table: &str,
    operation: &str,
    error_message: &str,
    backlog_rows: i64,
) {
    let _ = Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_cleanup_status (
             source_relid,
             buffer_table,
             attempt_count,
             blocked,
             last_error,
             last_operation,
             last_attempt_at,
             next_retry_at,
             backlog_rows,
             updated_at
         )
         VALUES (
             $1,
             $2,
             1,
             false,
             $3,
             $4,
             now(),
             now() + interval '5 seconds',
             $5,
             now()
         )
         ON CONFLICT (source_relid) DO UPDATE SET
             buffer_table = EXCLUDED.buffer_table,
             attempt_count = pgtrickle.pgt_cleanup_status.attempt_count + 1,
             blocked = (pgtrickle.pgt_cleanup_status.attempt_count + 1) >= 3,
             last_error = EXCLUDED.last_error,
             last_operation = EXCLUDED.last_operation,
             last_attempt_at = now(),
             next_retry_at = now() + make_interval(secs => LEAST(300, 5 * (pgtrickle.pgt_cleanup_status.attempt_count + 1))),
             backlog_rows = EXCLUDED.backlog_rows,
             updated_at = now()",
        &[
            pg_sys::Oid::from(source_oid).into(),
            buffer_table.into(),
            error_message.into(),
            operation.into(),
            backlog_rows.into(),
        ],
    );
}

fn clear_cleanup_status(source_oid: u32) {
    let _ = Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_cleanup_status WHERE source_relid = $1",
        &[pg_sys::Oid::from(source_oid).into()],
    );
}

// ── DAG-4: ST bypass tables for fused-chain execution ───────────────

thread_local! {
    /// Maps upstream `pgt_id` → temp bypass table name.
    ///
    /// When a fused-chain worker refreshes an upstream member, instead of
    /// writing delta rows to the persistent `changes_pgt_{id}` buffer, it
    /// creates a TEMP TABLE with the same schema and stores the mapping
    /// here.  Downstream members in the same chain read from the temp
    /// table via this mapping.
    static ST_BYPASS_TABLES: RefCell<HashMap<i64, String>> =
        RefCell::new(HashMap::new());

    /// DI-2: Source table OIDs whose per-leaf delta fraction exceeds
    /// `max_delta_fraction`. These leaves fall back from NOT EXISTS
    /// (index-based) to EXCEPT ALL (hash-based) in snapshot construction.
    static FALLBACK_LEAF_OIDS: RefCell<std::collections::HashSet<u32>> =
        RefCell::new(std::collections::HashSet::new());
}

/// Register a bypass temp table for the given upstream pgt_id.
pub fn set_st_bypass(pgt_id: i64, temp_table: String) {
    ST_BYPASS_TABLES.with(|m| m.borrow_mut().insert(pgt_id, temp_table));
}

/// Remove the bypass mapping for a pgt_id.
pub fn clear_st_bypass(pgt_id: i64) {
    ST_BYPASS_TABLES.with(|m| m.borrow_mut().remove(&pgt_id));
}

/// Clear all bypass mappings.
pub fn clear_all_st_bypass() {
    ST_BYPASS_TABLES.with(|m| m.borrow_mut().clear());
}

/// Return the current bypass table mappings.
pub fn get_st_bypass_tables() -> HashMap<i64, String> {
    ST_BYPASS_TABLES.with(|m| m.borrow().clone())
}

/// DI-2: Set the per-leaf fallback OIDs for the current refresh cycle.
pub fn set_fallback_leaf_oids(oids: std::collections::HashSet<u32>) {
    FALLBACK_LEAF_OIDS.with(|m| *m.borrow_mut() = oids);
}

/// DI-2: Return the current per-leaf fallback OIDs.
pub fn get_fallback_leaf_oids() -> std::collections::HashSet<u32> {
    FALLBACK_LEAF_OIDS.with(|m| m.borrow().clone())
}

/// DI-2: Clear the per-leaf fallback OIDs.
pub fn clear_fallback_leaf_oids() {
    FALLBACK_LEAF_OIDS.with(|m| m.borrow_mut().clear());
}

/// Execute any pending cleanups from previous refresh cycles.
///
/// Called at the start of `execute_differential_refresh` to drain the
/// deferred cleanup queue.  Errors are logged but not propagated since
/// stale change-buffer rows are harmless due to LSN range predicates.
///
/// **Multi-ST safety:** When multiple stream tables depend on the same
/// source table, each ST may have a different frontier.  We compute the
/// minimum frontier LSN across all dependent STs for each source OID.
/// Only change buffer entries at or below this minimum are safe to delete,
/// because all consumers have already advanced past them.
///
/// **Robustness:** Each change buffer table is checked for existence via
/// `pg_class` before any DML is attempted.  When a stream table is dropped
/// and its change buffer tables are removed, stale pending-cleanup entries
/// referencing those tables are silently skipped.
pub(crate) fn drain_pending_cleanups() {
    let pending: Vec<PendingCleanup> =
        PENDING_CLEANUP.with(|q| std::mem::take(&mut *q.borrow_mut()));

    if pending.is_empty() {
        return;
    }

    // Deduplicate source OIDs and collect a consistent change schema.
    let mut all_oids = std::collections::HashSet::new();
    let mut change_schema = String::new();
    for job in &pending {
        if change_schema.is_empty() {
            change_schema.clone_from(&job.change_schema);
        }
        for &oid in &job.source_oids {
            all_oids.insert(oid);
        }
    }

    let use_truncate = crate::config::pg_trickle_cleanup_use_truncate();

    for oid in all_oids {
        // CITUS-4: Compute stable buffer name for this source OID.
        let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(oid));
        // Check that the change buffer table still exists before
        // attempting any DML.
        // PERF-2: Accept both 'r' (regular) and 'p' (partitioned) relkinds.
        let table_exists = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(\
               SELECT 1 FROM pg_class c \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               WHERE n.nspname = '{schema}' \
                 AND c.relname = '{buf_name}' \
                 AND c.relkind IN ('r', 'p')\
             )",
            schema = change_schema,
        ))
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if !table_exists {
            pgrx::debug1!(
                "[pg_trickle] Deferred cleanup: skipping {} (table dropped)",
                buf_name,
            );
            continue;
        }

        // D-1: Per-source advisory lock around min-frontier computation + DML.
        //
        // Without this lock, two concurrent refresh workers serving different
        // stream tables that share the same source OID can both compute the
        // same min-frontier, then both delete the same rows.  That is safe
        // (DELETE of already-deleted rows is a no-op) but wastes work and
        // can cause spurious "no rows found" in timing-sensitive tests.
        //
        // With the lock: exactly one worker owns the cleanup window for each
        // source OID per transaction.  The other worker skips and lets the
        // owner handle cleanup; the skipped rows will be cleaned in the next
        // cycle.  `pg_try_advisory_xact_lock` is non-blocking (returns false
        // on contention) and the lock is released automatically at transaction
        // end — no explicit unlock required.
        //
        // The lock key is the source OID cast to bigint.  OIDs are 32-bit, so
        // the bigint is always non-negative and unique per source table.
        let lock_acquired =
            // nosemgrep: rust.spi.query.dynamic-format — oid is a u32 from internal catalog lookup, not user input
            Spi::get_one::<bool>(&format!("SELECT pg_try_advisory_xact_lock({oid}::bigint)"))
                .unwrap_or(Some(true))
                .unwrap_or(true);
        if !lock_acquired {
            pgrx::debug1!(
                "[pg_trickle] D-1: cleanup advisory lock contended for OID {}, \
                 skipping this cycle",
                oid,
            );
            continue;
        }

        // Compute the minimum frontier LSN across ALL stream tables that
        // depend on this source OID.  Only entries at or below this LSN
        // have been consumed by every consumer and are safe to delete.
        //
        // We extract each ST's frontier for this source from the JSONB
        // `frontier->'sources'->'OID'->>'lsn'` path.  STs with NULL
        // frontiers (never refreshed) are excluded — they need a FULL
        // refresh first and won't read from the change buffer.
        let min_lsn: Option<String> = Spi::get_one::<String>(&format!(
            "SELECT MIN((st.frontier->'sources'->'{oid}'->>'lsn')::pg_lsn)::TEXT \
             FROM pgtrickle.pgt_stream_tables st \
             JOIN pgtrickle.pgt_dependencies dep ON dep.pgt_id = st.pgt_id \
             WHERE dep.source_relid = {oid} \
               AND dep.source_type = 'TABLE' \
               AND st.frontier IS NOT NULL \
               AND st.frontier->'sources'->'{oid}'->>'lsn' IS NOT NULL",
        ))
        .unwrap_or(None);

        let safe_lsn = match min_lsn {
            Some(lsn) if lsn != "0/0" => lsn,
            _ => {
                // No consumers with a frontier, or all at 0/0 — nothing to clean.
                pgrx::debug1!(
                    "[pg_trickle] Deferred cleanup: no safe threshold for {}, skipping",
                    buf_name,
                );
                continue;
            }
        };

        let can_truncate = if use_truncate {
            // Safe to TRUNCATE only if ALL entries are at or below the safe LSN.
            Spi::get_one::<bool>(&format!(
                "SELECT NOT EXISTS(\
                   SELECT 1 FROM \"{schema}\".{buf_name} \
                   WHERE lsn > '{safe_lsn}'::pg_lsn \
                   LIMIT 1\
                 )",
                schema = change_schema,
            ))
            .unwrap_or(Some(false))
            .unwrap_or(false)
        } else {
            false
        };

        // Task 3.3: Partitioned buffer cleanup via DETACH + DROP.
        if crate::cdc::is_buffer_partitioned(&change_schema, oid) {
            match crate::cdc::detach_consumed_partitions(&change_schema, oid, &safe_lsn) {
                Ok(n) if n > 0 => {
                    pgrx::debug1!(
                        "[pg_trickle] Deferred cleanup: detached {} partition(s) from {}",
                        n,
                        buf_name,
                    );
                }
                Err(e) => {
                    pgrx::debug1!(
                        "[pg_trickle] Deferred cleanup partition detach failed: {}",
                        e,
                    );
                }
                _ => {}
            }
            continue;
        }

        // NS-3: record a failed cleanup attempt; warn after 3 consecutive failures.
        let record_cleanup_failure = |oid: u32, operation: &str, msg: &str| {
            CLEANUP_FAILURE_COUNTS.with(|m| {
                let mut m = m.borrow_mut();
                let count = m.entry(oid).or_insert(0);
                *count += 1;
                if *count >= 3 {
                    pgrx::warning!(
                        "[pg_trickle] Deferred cleanup {} failed {} consecutive times for \
                         {}: {}",
                        operation,
                        count,
                        buf_name,
                        msg
                    );
                    // Emit NOTIFY alert on 3rd and every subsequent 10th failure
                    if *count == 3 || *count % 10 == 0 {
                        let _ = crate::monitor::emit_alert(
                            crate::monitor::AlertEvent::CleanupFailure,
                            "",
                            &buf_name,
                            serde_json::json!({
                                "source_oid": oid,
                                "consecutive_failures": *count,
                                "operation": operation,
                                "error": msg,
                            }),
                            false,
                        );
                    }
                } else {
                    pgrx::debug1!(
                        "[pg_trickle] Deferred cleanup {} failed (attempt {}): {}",
                        operation,
                        count,
                        msg
                    );
                }
            });

            let backlog_rows = Spi::get_one::<i64>(&format!(
                "SELECT count(*)::bigint FROM \"{schema}\".{buf_name}",
                schema = change_schema,
            ))
            .unwrap_or(Some(0))
            .unwrap_or(0);
            upsert_cleanup_status(oid, &buf_name, operation, msg, backlog_rows);
        };

        if can_truncate {
            match Spi::run(&format!(
                "TRUNCATE \"{schema}\".{buf_name}",
                schema = change_schema,
            )) {
                Ok(()) => {
                    match crate::cdc::restore_registered_sentinel(
                        &change_schema,
                        &buf_name,
                        "BASE",
                        oid as i64,
                    ) {
                        Ok(()) => {
                            CLEANUP_FAILURE_COUNTS.with(|m| {
                                m.borrow_mut().remove(&oid);
                            });
                            clear_cleanup_status(oid);
                        }
                        Err(e) => record_cleanup_failure(oid, "SENTINEL_RESTORE", &e.to_string()),
                    }
                }
                Err(e) => record_cleanup_failure(oid, "TRUNCATE", &e.to_string()),
            }
        } else {
            let delete_sql = format!(
                "DELETE FROM \"{schema}\".{buf_name} \
                 WHERE action IN ('I', 'D') \
                   AND lsn <= '{safe_lsn}'::pg_lsn",
                schema = change_schema,
            );
            match Spi::run(&delete_sql) {
                Ok(()) => {
                    CLEANUP_FAILURE_COUNTS.with(|m| {
                        m.borrow_mut().remove(&oid);
                    });
                    clear_cleanup_status(oid);
                }
                Err(e) => record_cleanup_failure(oid, "DELETE", &e.to_string()),
            }
        }
    }
}

/// Frontier-based cleanup: delete stale change buffer rows using the persisted
/// frontier in `pgt_stream_tables` rather than thread-local state.
///
/// This complements `drain_pending_cleanups` by handling the case where the
/// deferred cleanup was queued on a different PostgreSQL backend process
/// (e.g., when a connection pool dispatches successive refresh calls to
/// different backends).
///
/// For each source OID, computes the minimum frontier LSN across ALL stream
/// tables that depend on it, then deletes change buffer entries at or below
/// that threshold.  This is safe because all consumers have already advanced
/// past those entries.
pub(crate) fn cleanup_change_buffers_by_frontier(change_schema: &str, source_oids: &[u32]) {
    if source_oids.is_empty() {
        return;
    }

    let use_truncate = crate::config::pg_trickle_cleanup_use_truncate();

    let values_list = source_oids
        .iter()
        .map(|oid| {
            let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(*oid));
            format!("({oid}::oid, '{buf_name}')")
        })
        .collect::<Vec<_>>()
        .join(",");

    let metadata = Spi::connect(|client| -> Vec<(u32, bool, Option<String>)> {
        let sql = format!(
            "WITH src(source_oid, buf_name) AS (
                 VALUES {values_list}
             )
             SELECT
                 s.source_oid::int8,
                 EXISTS(
                     SELECT 1
                     FROM pg_class c
                     JOIN pg_namespace n ON n.oid = c.relnamespace
                     WHERE n.nspname = '{schema}'
                       AND c.relname = s.buf_name
                       AND c.relkind IN ('r', 'p')
                 ) AS table_exists,
                 (
                     SELECT MIN((st.frontier->'sources'->(s.source_oid::text)->>'lsn')::pg_lsn)::text
                     FROM pgtrickle.pgt_stream_tables st
                     JOIN pgtrickle.pgt_dependencies dep ON dep.pgt_id = st.pgt_id
                     WHERE dep.source_relid = s.source_oid
                       AND dep.source_type IN ('TABLE', 'FOREIGN_TABLE', 'MATVIEW')
                       AND st.frontier IS NOT NULL
                       AND st.frontier->'sources'->(s.source_oid::text)->>'lsn' IS NOT NULL
                 ) AS min_lsn
             FROM src s",
            values_list = values_list,
            schema = change_schema,
        );

        let mut out = Vec::new();
        if let Ok(rows) = client.select(&sql, None, &[]) {
            for row in rows {
                let oid = row.get::<i64>(1).unwrap_or(None).unwrap_or(0) as u32;
                let table_exists = row.get::<bool>(2).unwrap_or(None).unwrap_or(false);
                let min_lsn = row.get::<String>(3).unwrap_or(None);
                out.push((oid, table_exists, min_lsn));
            }
        }
        out
    });

    for (oid, table_exists, min_lsn) in metadata {
        // CITUS-4: Compute stable buffer name.
        let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(oid));

        if !table_exists {
            continue;
        }

        let safe_lsn = match min_lsn {
            Some(lsn) if lsn != "0/0" => lsn,
            _ => continue,
        };

        // Quick check: are there any entries to clean up?
        let has_stale = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(\
               SELECT 1 FROM \"{schema}\".{buf_name} \
               WHERE lsn <= '{safe_lsn}'::pg_lsn \
               LIMIT 1\
             )",
            schema = change_schema,
        ))
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if !has_stale {
            continue;
        }

        // Task 3.3: Partitioned buffer cleanup via DETACH + DROP.
        if crate::cdc::is_buffer_partitioned(change_schema, oid) {
            match crate::cdc::detach_consumed_partitions(change_schema, oid, &safe_lsn) {
                Ok(n) if n > 0 => {
                    pgrx::debug1!(
                        "[pg_trickle] Frontier cleanup: detached {} partition(s) from {}",
                        n,
                        buf_name,
                    );
                    clear_cleanup_status(oid);
                }
                Err(e) => {
                    pgrx::debug1!(
                        "[pg_trickle] Frontier cleanup partition detach failed: {}",
                        e,
                    );
                    let backlog_rows = Spi::get_one::<i64>(&format!(
                        "SELECT count(*)::bigint FROM \"{schema}\".{buf_name}",
                        schema = change_schema,
                    ))
                    .unwrap_or(Some(0))
                    .unwrap_or(0);
                    upsert_cleanup_status(
                        oid,
                        &buf_name,
                        "DETACH_PARTITIONS",
                        &e.to_string(),
                        backlog_rows,
                    );
                }
                _ => {}
            }
            continue;
        }

        let can_truncate = if use_truncate {
            Spi::get_one::<bool>(&format!(
                "SELECT NOT EXISTS(\
                   SELECT 1 FROM \"{schema}\".{buf_name} \
                   WHERE lsn > '{safe_lsn}'::pg_lsn \
                   LIMIT 1\
                 )",
                schema = change_schema,
            ))
            .unwrap_or(Some(false))
            .unwrap_or(false)
        } else {
            false
        };

        if can_truncate {
            if let Err(e) = Spi::run(&format!(
                "TRUNCATE \"{schema}\".{buf_name}",
                schema = change_schema,
            )) {
                pgrx::debug1!("[pg_trickle] Frontier-based cleanup TRUNCATE failed: {}", e);
                let backlog_rows = Spi::get_one::<i64>(&format!(
                    "SELECT count(*)::bigint FROM \"{schema}\".{buf_name}",
                    schema = change_schema,
                ))
                .unwrap_or(Some(0))
                .unwrap_or(0);
                upsert_cleanup_status(oid, &buf_name, "TRUNCATE", &e.to_string(), backlog_rows);
            } else {
                if let Err(e) = crate::cdc::restore_registered_sentinel(
                    change_schema,
                    &buf_name,
                    "BASE",
                    oid as i64,
                ) {
                    pgrx::debug1!(
                        "[pg_trickle] Frontier-based cleanup sentinel restore failed: {}",
                        e
                    );
                }
                clear_cleanup_status(oid);
            }
        } else {
            let delete_sql = format!(
                "DELETE FROM \"{schema}\".{buf_name} \
                 WHERE action IN ('I', 'D') \
                   AND lsn <= '{safe_lsn}'::pg_lsn",
                schema = change_schema,
            );
            if let Err(e) = Spi::run(&delete_sql) {
                pgrx::debug1!("[pg_trickle] Frontier-based cleanup DELETE failed: {}", e);
                let backlog_rows = Spi::get_one::<i64>(&format!(
                    "SELECT count(*)::bigint FROM \"{schema}\".{buf_name}",
                    schema = change_schema,
                ))
                .unwrap_or(Some(0))
                .unwrap_or(0);
                upsert_cleanup_status(oid, &buf_name, "DELETE", &e.to_string(), backlog_rows);
            } else {
                clear_cleanup_status(oid);
            }
        }
    }
}

/// Frontier-based cleanup for ST change buffers (`changes_pgt_{id}`).
///
/// For each upstream ST source, computes the minimum frontier LSN across all
/// downstream stream tables that depend on it, then deletes consumed rows.
pub(crate) fn cleanup_st_change_buffers_by_frontier(
    change_schema: &str,
    st_source_pgt_ids: &[i64],
) {
    if st_source_pgt_ids.is_empty() {
        return;
    }

    for &upstream_pgt_id in st_source_pgt_ids {
        let key = format!("pgt_{upstream_pgt_id}");

        // Check that the ST change buffer table exists.
        if let Err(e) = crate::cdc::validate_st_change_buffer(upstream_pgt_id, change_schema) {
            pgrx::warning!(
                "[pg_trickle] ST buffer validation failed for {}: {}",
                upstream_pgt_id,
                e
            );
            continue;
        }

        // ST-ST-6: Check if any downstream consumer has a NULL or missing
        // frontier for this ST source.  If so, skip cleanup — that consumer
        // hasn't consumed any data from this buffer yet and we must not
        // delete rows it still needs.
        let has_uninitialized_consumer = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS( \
               SELECT 1 FROM pgtrickle.pgt_stream_tables st \
               JOIN pgtrickle.pgt_dependencies dep ON dep.pgt_id = st.pgt_id \
               WHERE dep.source_type = 'STREAM_TABLE' \
                 AND dep.source_relid = ( \
                     SELECT pgt_relid FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {upstream_pgt_id} \
                 ) \
                 AND (st.frontier IS NULL \
                      OR st.frontier->'sources'->'{key}'->>'lsn' IS NULL) \
             )",
        ))
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if has_uninitialized_consumer {
            continue;
        }

        // Compute the minimum frontier LSN for this ST source across ALL
        // downstream consumers.
        let min_lsn: Option<String> = Spi::get_one::<String>(&format!(
            "SELECT MIN((st.frontier->'sources'->'{key}'->>'lsn')::pg_lsn)::TEXT \
             FROM pgtrickle.pgt_stream_tables st \
             JOIN pgtrickle.pgt_dependencies dep ON dep.pgt_id = st.pgt_id \
             WHERE dep.source_type = 'STREAM_TABLE' \
               AND dep.source_relid = ( \
                   SELECT pgt_relid FROM pgtrickle.pgt_stream_tables WHERE pgt_id = {upstream_pgt_id} \
               ) \
               AND st.frontier IS NOT NULL \
               AND st.frontier->'sources'->'{key}'->>'lsn' IS NOT NULL",
        ))
        .unwrap_or(None);

        let safe_lsn = match min_lsn {
            Some(lsn) if lsn != "0/0" => lsn,
            _ => continue,
        };

        let delete_sql = format!(
            "DELETE FROM \"{schema}\".changes_pgt_{id} \
             WHERE action IN ('I', 'D') \
               AND lsn <= '{safe_lsn}'::pg_lsn",
            schema = change_schema,
            id = upstream_pgt_id,
        );
        if let Err(e) = Spi::run(&delete_sql) {
            pgrx::debug1!(
                "[pg_trickle] ST buffer cleanup DELETE failed for changes_pgt_{}: {}",
                upstream_pgt_id,
                e,
            );
        }
    }
}

/// Flush pending cleanup entries that reference any of the given source OIDs.
///
/// Called during `drop_stream_table` to prevent stale cleanup entries from
/// referencing change buffer tables that are about to be dropped.
pub fn flush_pending_cleanups_for_oids(oids: &[u32]) {
    if oids.is_empty() {
        return;
    }
    PENDING_CLEANUP.with(|q| {
        let mut queue = q.borrow_mut();
        for entry in queue.iter_mut() {
            entry.source_oids.retain(|o| !oids.contains(o));
        }
        // Remove entries with no remaining OIDs.
        queue.retain(|e| !e.source_oids.is_empty());
    });
}

// ── D-1: Planner hint thresholds ────────────────────────────────────

/// Minimum delta rows before disabling nested-loop joins.
pub(crate) const PLANNER_HINT_NESTLOOP_THRESHOLD: i64 = 100;

/// Minimum delta rows before raising `work_mem` for hash joins.
pub(crate) const PLANNER_HINT_WORKMEM_THRESHOLD: i64 = 10_000;

/// Minimum Scan-node count before deep-join planner hints activate.
/// At 5+ tables the delta SQL generates cascading L₀ snapshot CTEs
/// whose intermediate hash joins can spill multi-GB temp files under
/// PostgreSQL's default planner choices (nested loops, low work_mem).
const DEEP_JOIN_SCAN_THRESHOLD: usize = 5;

/// Apply `SET LOCAL` planner hints based on the estimated delta size
/// and the join depth (scan count) of the defining query.
///
/// - Small delta (ratio < merge_seqscan_threshold): `SET LOCAL enable_seqscan = off`
///   to force index lookups on the stream table.
/// - delta 100–9 999: `SET LOCAL enable_nestloop = off`
/// - delta >= 10 000: also `SET LOCAL work_mem = '<N>MB'`
/// - scan_count >= 5 (deep join): disable nest loops, raise work_mem,
///   raise join_collapse_limit, and remove temp_file_limit. The delta
///   SQL for 5+ table joins produces O(n) snapshot CTEs whose hash
///   joins can exceed default temp_file_limit under poor plans.
///
/// `SET LOCAL` is automatically reset at the end of the current transaction,
/// so these hints cannot leak to other queries.
///
/// Returns `true` if the SCAL-3 work_mem cap would be exceeded, signalling
/// that the caller should fall back to a FULL refresh instead.
pub(crate) fn apply_planner_hints(
    estimated_delta: i64,
    st_relid: pg_sys::Oid,
    scan_count: usize,
    tuning: &crate::catalog::RefreshRuntimeTuning,
) -> bool {
    if !crate::config::pg_trickle_merge_planner_hints() {
        return false;
    }

    // PH-D2: Manual join strategy override — bypass heuristics entirely.
    let strategy = crate::config::pg_trickle_merge_join_strategy();
    if strategy != crate::config::MergeJoinStrategy::Auto {
        apply_fixed_join_strategy(strategy, tuning);
        return false;
    }

    // ── Deep-join hints (DI-11) ─────────────────────────────────────
    // For 5+ table joins the delta SQL generates cascading L₀ snapshot
    // CTEs. Without planner guidance, PostgreSQL may choose nested-loop
    // plans that create pathological temp file spills (>8 GB at SF=0.01).
    // Fix: disable nest loops, raise work_mem, bump join_collapse_limit
    // so the planner considers all join orderings, and remove the
    // temp_file_limit cap so the query can run to completion.
    if scan_count >= DEEP_JOIN_SCAN_THRESHOLD {
        let mb = tuning.merge_work_mem_mb.max(512);

        // SCAL-3: If a work_mem cap is set and the deep-join allocation
        // would exceed it, signal fallback to FULL refresh.
        let cap = tuning.delta_work_mem_cap_mb;
        if cap > 0 && mb > cap {
            pgrx::notice!(
                "[pg_trickle] SCAL-3: deep-join work_mem ({mb}MB) exceeds \
                 delta_work_mem_cap_mb ({cap}MB). Falling back to FULL refresh.",
            );
            return true;
        }

        if let Err(e) = Spi::run("SET LOCAL enable_nestloop = off") {
            pgrx::debug1!(
                "[pg_trickle] DI-11: failed to SET LOCAL enable_nestloop: {}",
                e
            );
        }
        // mb is a config integer, not user-supplied input; SET LOCAL cannot use parameterized queries.
        let work_mem_sql = format!("SET LOCAL work_mem = '{mb}MB'");
        if let Err(e) = Spi::run(&work_mem_sql) {
            pgrx::debug1!("[pg_trickle] DI-11: failed to SET LOCAL work_mem: {}", e);
        }
        // orderings for the inlined NOT MATERIALIZED snapshot CTEs.
        // Default is 8; deep joins can exceed this after CTE inlining.
        let jcl = (scan_count + 2).max(12);
        // jcl is a computed integer, not user-supplied input; SET LOCAL cannot use parameterized queries.
        let jcl_sql = format!("SET LOCAL join_collapse_limit = {jcl}");
        if let Err(e) = Spi::run(&jcl_sql) {
            pgrx::debug1!(
                "[pg_trickle] DI-11: failed to SET LOCAL join_collapse_limit: {}",
                e
            );
        }
        let fcl_sql = format!("SET LOCAL from_collapse_limit = {jcl}");
        if let Err(e) = Spi::run(&fcl_sql) {
            pgrx::debug1!(
                "[pg_trickle] DI-11: failed to SET LOCAL from_collapse_limit: {}",
                e
            );
        }
        // Remove temp_file_limit for this transaction so the delta query
        // can complete even if intermediate hash batches spill to disk.
        if let Err(e) = Spi::run("SET LOCAL temp_file_limit = -1") {
            pgrx::debug1!(
                "[pg_trickle] DI-11: failed to SET LOCAL temp_file_limit: {}",
                e
            );
        }
        pgrx::debug1!(
            "[pg_trickle] DI-11: deep join (scan_count={scan_count}) — \
             disabled nestloop, work_mem={mb}MB, join_collapse_limit={jcl}, \
             temp_file_limit=-1",
        );
        // Skip the normal delta-size hints below — deep-join hints
        // already cover nest-loop and work_mem.
        return false;
    }

    // P3-4: For small deltas against large stream tables, disable seqscan
    // to force index lookups on __pgt_row_id.
    let seqscan_threshold = crate::config::pg_trickle_merge_seqscan_threshold();
    if seqscan_threshold > 0.0
        && estimated_delta > 0
        && estimated_delta < PLANNER_HINT_NESTLOOP_THRESHOLD
    {
        let st_rows: i64 = Spi::get_one_with_args::<i64>(
            "SELECT GREATEST(reltuples::bigint, 1) FROM pg_class WHERE oid = $1",
            &[st_relid.into()],
        )
        .unwrap_or(Some(1000))
        .unwrap_or(1000);

        let ratio = estimated_delta as f64 / st_rows as f64;
        if ratio < seqscan_threshold {
            if let Err(e) = Spi::run("SET LOCAL enable_seqscan = off") {
                pgrx::debug1!(
                    "[pg_trickle] P3-4: failed to SET LOCAL enable_seqscan: {}",
                    e
                );
            } else {
                pgrx::debug1!(
                    "[pg_trickle] P3-4: delta/ST ratio {:.6} < threshold {:.4}, disabled seqscan",
                    ratio,
                    seqscan_threshold,
                );
            }
        }
    }

    if estimated_delta >= PLANNER_HINT_WORKMEM_THRESHOLD {
        // Large delta: disable nested loops AND raise work_mem for hash joins
        let mb = tuning.merge_work_mem_mb;

        // SCAL-3: If a work_mem cap is set and the large-delta allocation
        // would exceed it, signal fallback to FULL refresh.
        let cap = tuning.delta_work_mem_cap_mb;
        if cap > 0 && mb > cap {
            pgrx::notice!(
                "[pg_trickle] SCAL-3: large-delta work_mem ({mb}MB) exceeds \
                 delta_work_mem_cap_mb ({cap}MB). Falling back to FULL refresh.",
            );
            return true;
        }

        if let Err(e) = Spi::run("SET LOCAL enable_nestloop = off") {
            pgrx::debug1!(
                "[pg_trickle] D-1: failed to SET LOCAL enable_nestloop: {}",
                e
            );
        }
        // nosemgrep: rust.spi.run.dynamic-format — mb is a numeric value; SET LOCAL cannot use params
        if let Err(e) = Spi::run(&format!("SET LOCAL work_mem = '{mb}MB'")) {
            pgrx::debug1!("[pg_trickle] D-1: failed to SET LOCAL work_mem: {}", e);
        }
    } else if estimated_delta >= PLANNER_HINT_NESTLOOP_THRESHOLD {
        // Medium delta: just disable nested loops
        if let Err(e) = Spi::run("SET LOCAL enable_nestloop = off") {
            pgrx::debug1!(
                "[pg_trickle] D-1: failed to SET LOCAL enable_nestloop: {}",
                e
            );
        }
    }
    false
}

/// PH-D2: Apply a fixed join strategy override via `SET LOCAL` hints.
pub(crate) fn apply_fixed_join_strategy(
    strategy: crate::config::MergeJoinStrategy,
    tuning: &crate::catalog::RefreshRuntimeTuning,
) {
    let (nestloop, hashjoin, mergejoin, label) = match strategy {
        crate::config::MergeJoinStrategy::HashJoin => ("off", "on", "on", "hash_join"),
        crate::config::MergeJoinStrategy::NestedLoop => ("on", "off", "off", "nested_loop"),
        crate::config::MergeJoinStrategy::MergeJoin => ("off", "off", "on", "merge_join"),
        crate::config::MergeJoinStrategy::Auto => return,
    };

    for (param, val) in [
        ("enable_nestloop", nestloop),
        ("enable_hashjoin", hashjoin),
        ("enable_mergejoin", mergejoin),
    ] {
        // param is from a fixed extension-controlled array; val is a bool; SET LOCAL cannot use parameterized queries.
        let set_sql = format!("SET LOCAL {param} = {val}");
        if let Err(e) = Spi::run(&set_sql) {
            pgrx::debug1!("[pg_trickle] PH-D2: failed to SET LOCAL {param}: {}", e);
        }
    }

    // For hash_join strategy, also raise work_mem to avoid hash spills.
    if strategy == crate::config::MergeJoinStrategy::HashJoin {
        let mb = tuning.merge_work_mem_mb;
        // mb is a config integer, not user-supplied input; SET LOCAL cannot use parameterized queries.
        let work_mem_sql = format!("SET LOCAL work_mem = '{mb}MB'");
        if let Err(e) = Spi::run(&work_mem_sql) {
            pgrx::debug1!("[pg_trickle] PH-D2: failed to SET LOCAL work_mem: {}", e);
        }
    }

    pgrx::debug1!("[pg_trickle] PH-D2: fixed join strategy={label} applied",);
}

// ── ST-to-ST Delta Capture (Phase 8.2/8.3) ─────────────────────────────

/// Build a SQL expression that computes a content-hash of all user columns.
///
/// Downstream stream tables always see an upstream ST as keyless (no PK
/// constraint), so they compute `__pgt_row_id` from ALL user columns via
/// `row_id_key_columns()`.  The `__pgt_row_id` stored in the ST change buffer
/// **must** match that all-column content hash — otherwise the downstream
/// MERGE will never find the existing row to DELETE.
///
/// This is a pure-logic helper for unit testing.
pub fn build_content_hash_expr(prefix: &str, user_cols: &[String]) -> String {
    build_content_hash_expr_for_domain(prefix, user_cols, "KEYLESS_ROW")
}

/// Build a typed V2 identity for the supplied semantic domain.
pub fn build_content_hash_expr_for_domain(
    prefix: &str,
    user_cols: &[String],
    domain: &str,
) -> String {
    match user_cols.len() {
        0 => "pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8))".to_string(),
        1 => {
            let c = user_cols[0].replace('"', "\"\"");
            format!("pgtrickle.encode_row_id_v2('{domain}', ROW({prefix}\"{c}\"))")
        }
        _ => {
            let args: Vec<String> = user_cols
                .iter()
                .map(|c| {
                    let escaped = c.replace('"', "\"\"");
                    format!("{prefix}\"{escaped}\"")
                })
                .collect();
            crate::hash::build_row_identity_expr(domain, &args)
        }
    }
}

/// Capture delta rows from a materialized delta temp table into the ST's
/// change buffer for downstream ST consumption.
///
/// Called after the MERGE (or explicit DML) when the ST has downstream
/// consumers. The delta is already materialized in `__pgt_delta_{pgt_id}`
/// as a temp table with `__pgt_row_id`, `__pgt_action`, and user columns.
///
/// Only `'I'` (insert) and `'D'` (delete) actions are captured — updates
/// in the delta are already expressed as D+I pairs by the DVM.
pub(crate) fn capture_delta_to_st_buffer(
    st: &StreamTableMeta,
    user_cols: &[String],
) -> Result<i64, PgTrickleError> {
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let pgt_id = st.pgt_id;

    crate::cdc::set_sync_commit_for_buffer(&format!("changes_pgt_{pgt_id}"))?;

    crate::cdc::validate_st_change_buffer(pgt_id, &change_schema)?;

    // A44-10: ST change buffers use flat D+I schema (no new_/old_ prefix).
    let flat_col_list: String = user_cols
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let d_col_list: String = user_cols
        .iter()
        .map(|c| format!("d.\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    // UX-7: When diff_output_format = 'merged', recombine DELETE+INSERT
    // pairs with the same __pgt_row_id back into a single 'U' row for
    // backward compatibility with consumers expecting UPDATE operations.
    let sql = if crate::config::pg_trickle_diff_output_format()
        == crate::config::DiffOutputFormat::Merged
    {
        format!(
            "INSERT INTO \"{change_schema}\".changes_pgt_{pgt_id} \
             (lsn, action, __pgt_row_id, {flat_col_list}) \
             SELECT pg_current_wal_lsn(), \
                    CASE WHEN ins.__pgt_row_id IS NOT NULL \
                              AND del.__pgt_row_id IS NOT NULL \
                         THEN 'U' ELSE COALESCE(ins.__pgt_action, del.__pgt_action) \
                    END, \
                    COALESCE(ins.__pgt_row_id, del.__pgt_row_id), \
                    {coal_cols} \
             FROM (SELECT * FROM __pgt_delta_{pgt_id} WHERE __pgt_action = 'I') ins \
             FULL OUTER JOIN \
                  (SELECT * FROM __pgt_delta_{pgt_id} WHERE __pgt_action = 'D') del \
             ON {row_match}",
            change_schema = change_schema,
            pgt_id = pgt_id,
            flat_col_list = flat_col_list,
            row_match = build_row_id_match("ins.__pgt_row_id", "del.__pgt_row_id"),
            coal_cols = user_cols
                .iter()
                .map(|c| {
                    let escaped = c.replace('"', "\"\"");
                    format!("COALESCE(ins.\"{escaped}\", del.\"{escaped}\")")
                })
                .collect::<Vec<_>>()
                .join(", "),
        )
    } else {
        format!(
            "INSERT INTO \"{change_schema}\".changes_pgt_{pgt_id} \
             (lsn, action, __pgt_row_id, {flat_col_list}) \
             SELECT pg_current_wal_lsn(), d.__pgt_action, d.__pgt_row_id, \
                    {d_col_list} \
             FROM __pgt_delta_{pgt_id} d \
             WHERE d.__pgt_action IN ('I', 'D')"
        )
    };

    let count = Spi::connect_mut(|client| {
        let result = client
            .update(&sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Ok::<i64, PgTrickleError>(result.len() as i64)
    })?;

    if count > 0 {
        pgrx::debug1!(
            "[pg_trickle] ST-ST: captured {} delta rows to changes_pgt_{} for {}.{}",
            count,
            pgt_id,
            st.pgt_schema,
            st.pgt_name,
        );
    }

    Ok(count)
}

/// DAG-4: Capture delta to a temp bypass table instead of the persistent
/// `changes_pgt_{id}` buffer.
///
/// Creates `pg_temp.__pgt_bypass_{pgt_id}` (ON COMMIT DROP) with the same
/// schema as the persistent buffer, inserts I/D rows from the delta temp
/// table, and registers the bypass mapping so that downstream refresh
/// reads from this temp table instead.
///
/// Returns the number of captured rows.
pub fn capture_delta_to_bypass_table(
    st: &StreamTableMeta,
    user_cols_typed: &[(String, String)],
) -> Result<i64, PgTrickleError> {
    let pgt_id = st.pgt_id;

    // The delta temp table is only created during a true DIFFERENTIAL
    // refresh.  If `execute_scheduled_refresh` internally fell back to
    // FULL (e.g. no previous frontier), the table won't exist and we
    // must skip the capture to avoid a "relation does not exist" ERROR.
    // pgt_id is a plain i64, not user-supplied input.
    let delta_exists_sql = format!("SELECT to_regclass('__pgt_delta_{}') IS NOT NULL", pgt_id);
    let delta_exists: bool = Spi::get_one::<bool>(&delta_exists_sql)
        .unwrap_or(Some(false))
        .unwrap_or(false);

    if !delta_exists {
        pgrx::debug1!(
            "[pg_trickle] DAG-4: no delta table for pgt_id={}, skipping bypass capture",
            pgt_id,
        );
        return Ok(0);
    }

    let bypass_table = format!("pg_temp.__pgt_bypass_{}", pgt_id);

    // DAG-4/ST-ST-10: If a pre-snapshot table exists (created by the
    // capture_incremental_diff_to_st_buffer path in explicit_dml), use a
    // pre/post comparison to produce correct D+I pairs for value-changes.
    // The weight-aggregated __pgt_delta_ collapses D+I with the same
    // __pgt_row_id into a single I, which omits the D for old column values.
    // Downstream STs with WHERE filters on changed columns would miss the
    // deletion and retain stale rows.
    // pgt_id is a plain i64, not user-supplied input.
    let pre_snap_sql = format!("SELECT to_regclass('__pgt_pre_{}') IS NOT NULL", pgt_id);
    let pre_snapshot_exists: bool = Spi::get_one::<bool>(&pre_snap_sql)
        .unwrap_or(Some(false))
        .unwrap_or(false);

    // DAG-4/ST-ST-10: Read the MAX(lsn) from the persistent change buffer
    // so that bypass table rows use an LSN that falls within the downstream
    // ST's frontier range.  Between capture_incremental_diff_to_st_buffer
    // (which writes to the persistent buffer inside execute_differential_refresh)
    // and this bypass capture (called after execute_scheduled_refresh stores
    // frontier and other catalog metadata), WAL-generating catalog DMLs advance
    // pg_current_wal_lsn().  Using the stale higher LSN would cause the
    // downstream scan's `lsn <= new_lsn` filter to exclude bypass rows,
    // silently dropping the entire delta.
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let buffer_lsn: Option<String> = if crate::cdc::has_st_change_buffer(pgt_id, &change_schema) {
        Spi::get_one::<String>(&format!(
            "SELECT MAX(lsn)::text FROM \"{change_schema}\".changes_pgt_{pgt_id}",
        ))
        .unwrap_or(None)
    } else {
        None
    };

    let count = if pre_snapshot_exists {
        let user_cols: Vec<String> = user_cols_typed.iter().map(|(n, _)| n.clone()).collect();
        // A44-10: Bypass tables use flat D+I schema (no new_/old_ prefix).
        let col_defs: String = std::iter::once("lsn pg_lsn".to_string())
            .chain(std::iter::once("action \"char\"".to_string()))
            .chain(std::iter::once("__pgt_row_id bytea NOT NULL".to_string()))
            .chain(
                user_cols_typed
                    .iter()
                    .map(|(name, typ)| format!("\"{}\" {}", name.replace('"', "\"\""), typ)),
            )
            .collect::<Vec<_>>()
            .join(", ");
        let create_sql =
            format!("CREATE TEMP TABLE IF NOT EXISTS {bypass_table} ({col_defs}) ON COMMIT DROP",);
        Spi::run(&create_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        capture_diff_to_table(st, &user_cols, &bypass_table, pgt_id, buffer_lsn.as_deref())?
    } else {
        let sql = build_bypass_capture_sql(
            pgt_id,
            user_cols_typed,
            &bypass_table,
            buffer_lsn.as_deref(),
        );
        Spi::connect_mut(|client| {
            let result = client
                .update(&sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<i64, PgTrickleError>(result.len() as i64)
        })?
    };

    set_st_bypass(pgt_id, bypass_table.clone());

    if count > 0 {
        pgrx::debug1!(
            "[pg_trickle] DAG-4: captured {} delta rows to bypass table {} for {}.{}",
            count,
            bypass_table,
            st.pgt_schema,
            st.pgt_name,
        );
    }

    Ok(count)
}

/// Shared pre/post diff logic for capturing D+I pairs into a target table.
///
/// Used by both `capture_delta_to_bypass_table` (FusedChain path) and
/// `capture_incremental_diff_to_st_buffer` (persistent buffer path).
///
/// `target_table` must already exist with the appropriate schema
/// (`lsn, action, __pgt_row_id, col1, col2, ...` — flat D+I schema, A44-10).
/// Reads the pre-snapshot from `__pgt_pre_{pgt_id}` and compares with the
/// current state of the ST backing table.
///
/// `lsn_override`: when `Some("0/1A2B3C")`, the given literal LSN is used
/// instead of `pg_current_wal_lsn()`.  This is required for bypass tables
/// (DAG-4) where WAL-generating catalog DMLs between the persistent buffer
/// capture and the bypass capture advance `pg_current_wal_lsn()` past the
/// downstream ST's frontier upper bound.
pub(crate) fn capture_diff_to_table(
    st: &StreamTableMeta,
    user_cols: &[String],
    target_table: &str,
    pgt_id: i64,
    lsn_override: Option<&str>,
) -> Result<i64, PgTrickleError> {
    let schema = &st.pgt_schema;
    let name = &st.pgt_name;
    let quoted_table = format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        name.replace('"', "\"\""),
    );

    // A44-10: ST change buffers use flat D+I schema (no new_/old_ prefix).
    let flat_col_list: String = user_cols
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let pre_col_refs: String = user_cols
        .iter()
        .map(|c| format!("pre.\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let post_col_refs: String = user_cols
        .iter()
        .map(|c| format!("post.\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let is_distinct_pairs: String = user_cols
        .iter()
        .map(|c| {
            let qc = format!("\"{}\"", c.replace('"', "\"\""));
            format!("pre.{qc} IS DISTINCT FROM post.{qc}")
        })
        .collect::<Vec<_>>()
        .join(" OR ");

    let pre_pk_hash = "pre.__pgt_row_id";
    let post_pk_hash = "post.__pgt_row_id";
    let pre_post_match = build_row_id_match("pre.__pgt_row_id", "post.__pgt_row_id");
    let delta_post_match = build_row_id_match("delta.__pgt_row_id", "post.__pgt_row_id");

    // DAG-4: When an LSN override is provided (bypass tables), use the
    // literal value so the rows fall within the downstream frontier range.
    let lsn_expr = match lsn_override {
        Some(lsn) => format!("'{lsn}'::pg_lsn"),
        None => "pg_current_wal_lsn()".to_string(),
    };

    let mut total: i64 = 0;

    // Deleted rows: in pre but no longer in the table.
    let del_sql = format!(
        "INSERT INTO {target_table} (lsn, action, __pgt_row_id, {flat_col_list}) \
         SELECT {lsn_expr}, 'D', {pre_pk_hash}, {pre_col_refs} \
         FROM __pgt_pre_{pgt_id} pre \
         LEFT JOIN {quoted_table} post ON {pre_post_match} \
         WHERE post.__pgt_row_id IS NULL"
    );
    total += Spi::connect_mut(|c| {
        Ok::<i64, PgTrickleError>(
            c.update(&del_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .len() as i64,
        )
    })?;

    // Inserted rows: in table (scoped to delta row_ids) but not in pre.
    let ins_sql = format!(
        "INSERT INTO {target_table} (lsn, action, __pgt_row_id, {flat_col_list}) \
         SELECT {lsn_expr}, 'I', {post_pk_hash}, {post_col_refs} \
         FROM {quoted_table} post \
         JOIN (SELECT DISTINCT __pgt_row_id FROM __pgt_delta_{pgt_id}) delta \
           ON {delta_post_match} \
         LEFT JOIN __pgt_pre_{pgt_id} pre ON {pre_post_match} \
         WHERE pre.__pgt_row_id IS NULL"
    );
    total += Spi::connect_mut(|c| {
        Ok::<i64, PgTrickleError>(
            c.update(&ins_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .len() as i64,
        )
    })?;

    // Changed rows: same row_id, different column values.
    if !is_distinct_pairs.is_empty() {
        let chg_del_sql = format!(
            "INSERT INTO {target_table} (lsn, action, __pgt_row_id, {flat_col_list}) \
             SELECT {lsn_expr}, 'D', {pre_pk_hash}, {pre_col_refs} \
             FROM __pgt_pre_{pgt_id} pre \
             JOIN {quoted_table} post ON {pre_post_match} \
             WHERE {is_distinct_pairs}"
        );
        total += Spi::connect_mut(|c| {
            Ok::<i64, PgTrickleError>(
                c.update(&chg_del_sql, None, &[])
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .len() as i64,
            )
        })?;

        let chg_ins_sql = format!(
            "INSERT INTO {target_table} (lsn, action, __pgt_row_id, {flat_col_list}) \
             SELECT {lsn_expr}, 'I', {post_pk_hash}, {post_col_refs} \
             FROM {quoted_table} post \
             JOIN __pgt_pre_{pgt_id} pre ON {pre_post_match} \
             WHERE {is_distinct_pairs}"
        );
        total += Spi::connect_mut(|c| {
            Ok::<i64, PgTrickleError>(
                c.update(&chg_ins_sql, None, &[])
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                    .len() as i64,
            )
        })?;
    }

    Ok(total)
}

/// Build the SQL for creating a bypass temp table and inserting delta rows.
///
/// Pure-logic helper for unit testing.
pub fn build_bypass_capture_sql(
    pgt_id: i64,
    user_cols_typed: &[(String, String)],
    bypass_table: &str,
    lsn_override: Option<&str>,
) -> String {
    // A44-10: Bypass tables use flat D+I schema (no new_/old_ prefix).
    let col_defs: String = std::iter::once("lsn pg_lsn".to_string())
        .chain(std::iter::once("action \"char\"".to_string()))
        .chain(std::iter::once("__pgt_row_id bytea NOT NULL".to_string()))
        .chain(
            user_cols_typed
                .iter()
                .map(|(name, typ)| format!("\"{}\" {}", name.replace('"', "\"\""), typ)),
        )
        .collect::<Vec<_>>()
        .join(", ");

    let flat_col_list: String = user_cols_typed
        .iter()
        .map(|(name, _)| format!("\"{}\"", name.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let d_col_list: String = user_cols_typed
        .iter()
        .map(|(name, _)| format!("d.\"{}\"", name.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    // DAG-4: Use the persistent buffer's LSN when available so the bypass
    // rows fall within the downstream scan's frontier range.
    let lsn_expr = match lsn_override {
        Some(lsn) => format!("'{lsn}'::pg_lsn"),
        None => "pg_current_wal_lsn()".to_string(),
    };

    format!(
        "CREATE TEMP TABLE IF NOT EXISTS {bypass_table} ({col_defs}) ON COMMIT DROP;\n\
         INSERT INTO {bypass_table} (lsn, action, __pgt_row_id, {flat_col_list}) \
         SELECT {lsn_expr}, d.__pgt_action, d.__pgt_row_id, {d_col_list} \
         FROM __pgt_delta_{pgt_id} d \
         WHERE d.__pgt_action IN ('I', 'D')"
    )
}

/// Capture the effective delta from a DIFFERENTIAL refresh into the ST's
/// change buffer using a pre/post snapshot comparison.
///
/// Called after the explicit DML (DELETE + UPDATE + INSERT) when the ST has
/// downstream consumers.  Delegates to [`capture_diff_to_table`] with the
/// persistent `changes_pgt_{pgt_id}` buffer as the target.
pub(crate) fn capture_incremental_diff_to_st_buffer(
    st: &StreamTableMeta,
    user_cols: &[String],
) -> Result<i64, PgTrickleError> {
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let pgt_id = st.pgt_id;

    crate::cdc::set_sync_commit_for_buffer(&format!("changes_pgt_{pgt_id}"))?;
    crate::cdc::validate_st_change_buffer(pgt_id, &change_schema)?;

    let target_table = format!("\"{change_schema}\".changes_pgt_{pgt_id}");
    let total = capture_diff_to_table(st, user_cols, &target_table, pgt_id, None)?;

    if total > 0 {
        pgrx::debug1!(
            "[pg_trickle] ST-ST INCR: captured {} diff rows to changes_pgt_{} for {}.{}",
            total,
            pgt_id,
            st.pgt_schema,
            st.pgt_name,
        );
    }
    Ok(total)
}

/// Capture the full-refresh diff into the ST's change buffer.
///
/// Called after a FULL refresh when the ST has downstream consumers.
/// Compares a pre-refresh snapshot (`__pgt_pre_{pgt_id}`) against the
/// post-refresh state to produce I/D pairs.
pub(crate) fn capture_full_refresh_diff_to_st_buffer(
    st: &StreamTableMeta,
    user_cols: &[String],
) -> Result<i64, PgTrickleError> {
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let pgt_id = st.pgt_id;

    crate::cdc::set_sync_commit_for_buffer(&format!("changes_pgt_{pgt_id}"))?;
    crate::cdc::validate_st_change_buffer(pgt_id, &change_schema)?;

    let schema = &st.pgt_schema;
    let name = &st.pgt_name;
    let quoted_table = format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        name.replace('"', "\"\""),
    );

    // A44-10: ST change buffers use flat D+I schema (no new_/old_ prefix).
    let flat_col_list: String = user_cols
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let pre_col_refs: String = user_cols
        .iter()
        .map(|c| format!("pre.\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    let post_col_refs: String = user_cols
        .iter()
        .map(|c| format!("post.\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ");

    // IS DISTINCT FROM comparison for detecting changed rows
    let is_distinct_pairs: String = user_cols
        .iter()
        .map(|c| {
            let qc = format!("\"{}\"", c.replace('"', "\"\""));
            format!("pre.{qc} IS DISTINCT FROM post.{qc}")
        })
        .collect::<Vec<_>>()
        .join(" OR ");

    let mut total_count: i64 = 0;

    // ST-ST-9: Use content hash of all user columns for __pgt_row_id (see
    // build_content_hash_expr doc comment for rationale).
    let pre_pk_hash = "pre.__pgt_row_id";
    let post_pk_hash = "post.__pgt_row_id";
    let pre_post_match = build_row_id_match("pre.__pgt_row_id", "post.__pgt_row_id");

    // Deleted rows: in pre but not in post
    let deleted_sql = format!(
        "INSERT INTO \"{change_schema}\".changes_pgt_{pgt_id} \
         (lsn, action, __pgt_row_id, {flat_col_list}) \
         SELECT pg_current_wal_lsn(), 'D', {pre_pk_hash}, {pre_col_refs} \
         FROM __pgt_pre_{pgt_id} pre \
         LEFT JOIN {quoted_table} post ON {pre_post_match} \
         WHERE post.__pgt_row_id IS NULL"
    );
    let del_count = Spi::connect_mut(|client| {
        let result = client
            .update(&deleted_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Ok::<i64, PgTrickleError>(result.len() as i64)
    })?;
    total_count += del_count;

    // Inserted rows: in post but not in pre
    let inserted_sql = format!(
        "INSERT INTO \"{change_schema}\".changes_pgt_{pgt_id} \
         (lsn, action, __pgt_row_id, {flat_col_list}) \
         SELECT pg_current_wal_lsn(), 'I', {post_pk_hash}, {post_col_refs} \
         FROM {quoted_table} post \
         LEFT JOIN __pgt_pre_{pgt_id} pre ON {pre_post_match} \
         WHERE pre.__pgt_row_id IS NULL"
    );
    let ins_count = Spi::connect_mut(|client| {
        let result = client
            .update(&inserted_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Ok::<i64, PgTrickleError>(result.len() as i64)
    })?;
    total_count += ins_count;

    // Changed rows: same row_id but different content.
    //
    // ST-ST-9: With content-hash __pgt_row_id, the old D and new I have
    // DIFFERENT __pgt_row_id values (content changed), so the downstream
    // keyless decomposition correctly sees them as independent events
    // (no accidental cancellation).  Emit both D (old values) and
    // I (new values) so the downstream can delete the old row and
    // insert the new one.
    if !is_distinct_pairs.is_empty() {
        // D event: old content hash + old column values
        let changed_del_sql = format!(
            "INSERT INTO \"{change_schema}\".changes_pgt_{pgt_id} \
             (lsn, action, __pgt_row_id, {flat_col_list}) \
             SELECT pg_current_wal_lsn(), 'D', {pre_pk_hash}, {pre_col_refs} \
             FROM __pgt_pre_{pgt_id} pre \
             JOIN {quoted_table} post ON {pre_post_match} \
             WHERE {is_distinct_pairs}"
        );
        let chg_del_count = Spi::connect_mut(|client| {
            let result = client
                .update(&changed_del_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<i64, PgTrickleError>(result.len() as i64)
        })?;
        total_count += chg_del_count;

        // I event: new content hash + new column values
        let changed_ins_sql = format!(
            "INSERT INTO \"{change_schema}\".changes_pgt_{pgt_id} \
             (lsn, action, __pgt_row_id, {flat_col_list}) \
             SELECT pg_current_wal_lsn(), 'I', {post_pk_hash}, {post_col_refs} \
             FROM {quoted_table} post \
             JOIN __pgt_pre_{pgt_id} pre ON {pre_post_match} \
             WHERE {is_distinct_pairs}"
        );
        let chg_ins_count = Spi::connect_mut(|client| {
            let result = client
                .update(&changed_ins_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<i64, PgTrickleError>(result.len() as i64)
        })?;
        total_count += chg_ins_count;
    }

    if total_count > 0 {
        pgrx::debug1!(
            "[pg_trickle] ST-ST FULL: captured {} diff rows to changes_pgt_{} for {}.{} \
             (deleted={}, inserted={}, changed={})",
            total_count,
            pgt_id,
            st.pgt_schema,
            st.pgt_name,
            del_count,
            ins_count,
            total_count - del_count - ins_count,
        );
    }

    Ok(total_count)
}

/// Get user-facing output columns for an ST (for delta capture).
///
/// Excludes internal columns (__pgt_row_id, __pgt_count, etc.).
pub fn get_st_user_columns(st: &StreamTableMeta) -> Vec<String> {
    crate::cdc::resolve_st_output_columns(st.pgt_relid)
        .unwrap_or_default()
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// Return user column (name, type) pairs for a stream table.
///
/// Used by bypass-table creation so column types match the original ST.
pub fn get_st_user_columns_typed(st: &StreamTableMeta) -> Vec<(String, String)> {
    crate::cdc::resolve_st_output_columns(st.pgt_relid).unwrap_or_default()
}

/// Check whether this ST has downstream ST consumers that need delta capture.
pub fn has_downstream_st_consumers(pgt_id: i64) -> bool {
    crate::cdc::count_downstream_st_consumers(pgt_id) > 0
}

/// Resolve LSN placeholders in a SQL template with actual frontier values.
///
/// B3-1: When `zero_change_oids` contains a source OID, the entire LSN-range
/// predicate for that source is replaced with `FALSE`, enabling PostgreSQL's
/// planner to recognise the scan CTE as empty at plan time and skip
/// downstream join branches.
///
/// Returns `Err(PgTrickleError::UnresolvedPlaceholder)` if any
/// `__PGS_[A-Z0-9_]+__` or `__PGT_[A-Z0-9_]+__` token remains after
/// all substitution passes (A41-2).
pub(crate) fn resolve_lsn_placeholders(
    template: &str,
    source_oids: &[u32],
    prev_frontier: &Frontier,
    new_frontier: &Frontier,
    zero_change_oids: &std::collections::HashSet<u32>,
) -> Result<String, PgTrickleError> {
    // C-2: For every source OID whose LSN placeholders appear in the template,
    // assert that BOTH halves (PREV and NEW) are present before substituting.
    // If a template contains one half but not the other, the codegen produced
    // an asymmetric LSN window that would miss changes or double-count them.
    //
    // We intentionally do NOT require that every OID in source_oids has tokens
    // in the template — valid delta SQL may omit a source table entirely (e.g.
    // delete-rederivation paths that reference only some sources).
    //
    // Exception: ST-to-ST sources use a "pgt_" key prefix and are handled
    // by the separate pgt_prefix block below — skip the check for those.
    for &oid in source_oids {
        let prev_tok = format!("__PGS_PREV_LSN_{oid}__");
        let new_tok = format!("__PGS_NEW_LSN_{oid}__");
        let has_prev = template.contains(&prev_tok);
        let has_new = template.contains(&new_tok);
        if has_prev && !has_new {
            return Err(PgTrickleError::UnresolvedPlaceholder {
                token: new_tok.clone(),
                context: format!(
                    "C-2: source OID {oid} has PREV LSN placeholder but NEW LSN \
                     placeholder ({new_tok}) is absent — asymmetric LSN window in \
                     the delta template"
                ),
            });
        }
        if has_new && !has_prev {
            return Err(PgTrickleError::UnresolvedPlaceholder {
                token: prev_tok.clone(),
                context: format!(
                    "C-2: source OID {oid} has NEW LSN placeholder but PREV LSN \
                     placeholder ({prev_tok}) is absent — asymmetric LSN window in \
                     the delta template"
                ),
            });
        }
    }

    let mut sql = template.to_string();
    for &oid in source_oids {
        if zero_change_oids.contains(&oid) {
            // B3-1: Replace the full LSN range predicate with FALSE so
            // PG can prune the change-buffer scan CTE at plan time.
            let lsn_pred = format!(
                "'__PGS_PREV_LSN_{oid}__'::pg_lsn AND c.lsn <= '__PGS_NEW_LSN_{oid}__'::pg_lsn"
            );
            if sql.contains(&lsn_pred) {
                sql = sql.replace(
                    &format!("c.lsn > {lsn_pred}"),
                    "FALSE /* B3-1: zero-change source pruned */",
                );
            } else {
                // Cleanup SQL or other patterns that don't use `c.lsn` alias.
                sql = sql.replace(
                    &format!("__PGS_PREV_LSN_{oid}__"),
                    &prev_frontier.get_lsn(oid),
                );
                sql = sql.replace(
                    &format!("__PGS_NEW_LSN_{oid}__"),
                    &new_frontier.get_lsn(oid),
                );
            }
        } else {
            sql = sql.replace(
                &format!("__PGS_PREV_LSN_{oid}__"),
                &prev_frontier.get_lsn(oid),
            );
            sql = sql.replace(
                &format!("__PGS_NEW_LSN_{oid}__"),
                &new_frontier.get_lsn(oid),
            );
        }
    }

    // ST-ST-4: Resolve pgt_-prefixed placeholders for ST source frontiers.
    // These use the format `__PGS_PREV_LSN_pgt_{id}__` / `__PGS_NEW_LSN_pgt_{id}__`.
    // The frontier stores ST sources under the key "pgt_{id}".
    let pgt_prefix = "__PGS_PREV_LSN_pgt_";
    if sql.contains(pgt_prefix) {
        // Extract all pgt_ IDs from the template
        let mut search_from = 0usize;
        let mut pgt_ids: Vec<i64> = Vec::new();
        while let Some(pos) = sql[search_from..].find(pgt_prefix) {
            let start = search_from + pos + pgt_prefix.len();
            let end = sql[start..]
                .find("__")
                .map(|p| start + p)
                .unwrap_or(sql.len());
            if let Ok(id) = sql[start..end].parse::<i64>()
                && !pgt_ids.contains(&id)
            {
                pgt_ids.push(id);
            }
            search_from = end;
        }

        for pgt_id in &pgt_ids {
            let key = format!("pgt_{pgt_id}");
            let prev_lsn = prev_frontier
                .sources
                .get(&key)
                .map(|sv| sv.lsn.clone())
                .unwrap_or_else(|| "0/0".to_string());
            let new_lsn = new_frontier
                .sources
                .get(&key)
                .map(|sv| sv.lsn.clone())
                .unwrap_or_else(|| "0/0".to_string());

            sql = sql.replace(&format!("__PGS_PREV_LSN_pgt_{pgt_id}__"), &prev_lsn);
            sql = sql.replace(&format!("__PGS_NEW_LSN_pgt_{pgt_id}__"), &new_lsn);
        }
    }

    // A41-2: Assert no LSN placeholders remain.  __PGT_*__ tokens (e.g.
    // __PGT_PART_PRED__) are resolved in a later stage and must not be
    // flagged here.
    crate::dvm::check_no_remaining_placeholders_for(&sql, "resolve_lsn_placeholders", &["__PGS_"])?;

    Ok(sql)
}

// ── D-2: Prepared statement helpers ─────────────────────────────────

/// Convert a MERGE template with `'__PGS_PREV_LSN_{oid}__'::pg_lsn`
/// tokens into a parameterized SQL string with `$1`, `$2`, … placeholders.
///
/// Parameter mapping: for each source OID (in order), `$2i-1` = prev_lsn,
/// `$2i` = new_lsn.
pub(crate) fn parameterize_lsn_template(template: &str, source_oids: &[u32]) -> String {
    let mut sql = template.to_string();
    for (i, &oid) in source_oids.iter().enumerate() {
        let prev_param = format!("${}", i * 2 + 1);
        let new_param = format!("${}", i * 2 + 2);
        sql = sql.replace(&format!("'__PGS_PREV_LSN_{oid}__'::pg_lsn"), &prev_param);
        sql = sql.replace(&format!("'__PGS_NEW_LSN_{oid}__'::pg_lsn"), &new_param);
    }
    sql
}

/// Build the `(pg_lsn, pg_lsn, …)` type list for a `PREPARE` statement.
pub(crate) fn build_prepare_type_list(n_sources: usize) -> String {
    std::iter::repeat_n("pg_lsn", n_sources * 2)
        .collect::<Vec<_>>()
        .join(", ")
}

/// Build the `('0/1A2B…'::pg_lsn, '0/3C4D…'::pg_lsn, …)` value list
/// for an `EXECUTE` statement.
pub(crate) fn build_execute_params(
    source_oids: &[u32],
    prev_frontier: &Frontier,
    new_frontier: &Frontier,
) -> String {
    source_oids
        .iter()
        .flat_map(|&oid| {
            let prev = prev_frontier.get_lsn(oid);
            let new = new_frontier.get_lsn(oid);
            [format!("'{prev}'::pg_lsn"), format!("'{new}'::pg_lsn")]
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn deallocate_prepared_merge_statement(_pgt_id: i64) {
    #[cfg(not(test))]
    {
        let pgt_id = _pgt_id;
        let stmt = format!("__pgt_merge_{pgt_id}");
        // nosemgrep: rust.spi.query.dynamic-format — stmt is derived from numeric pgt_id
        let exists = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_prepared_statements WHERE name = '{stmt}')"
        ))
        .unwrap_or(Some(false))
        .unwrap_or(false);
        if exists {
            let _ = Spi::run(&format!("DEALLOCATE {stmt}")); // nosemgrep: rust.spi.run.dynamic-format — stmt is derived from numeric pgt_id, not user input
        }
    }
}

pub(crate) fn clear_prepared_merge_statements() {
    let tracked_ids =
        PREPARED_MERGE_STMTS.with(|stmts| stmts.borrow().iter().copied().collect::<Vec<_>>());

    for pgt_id in tracked_ids {
        deallocate_prepared_merge_statement(pgt_id);
    }

    PREPARED_MERGE_STMTS.with(|stmts| stmts.borrow_mut().clear());
}

/// P-8: Resize the `MERGE_TEMPLATE_CACHE` LruCache to match the current
/// `pg_trickle.template_cache_max_entries` GUC value.
///
/// Called before inserting a new cache entry.  When the GUC is 0 (default,
/// meaning "unbounded"), the cache capacity is set to
/// `LRU_DEFAULT_UNBOUNDED_CAP`.  The `lru::LruCache` handles eviction
/// automatically on `put()` — no manual O(N) scan required.
pub(crate) fn maybe_evict_lru_cache_entry() {
    let max_entries = crate::config::pg_trickle_template_cache_max_entries();
    let desired_cap = if max_entries <= 0 {
        LRU_DEFAULT_UNBOUNDED_CAP
    } else {
        // max_entries is > 0 by the branch condition; fall back to the default if
        // the conversion somehow produces zero (should never happen).
        NonZeroUsize::new(max_entries as usize).unwrap_or(LRU_DEFAULT_UNBOUNDED_CAP)
    };
    MERGE_TEMPLATE_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        if c.cap() != desired_cap {
            c.resize(desired_cap);
            crate::shmem::set_template_cache_bytes(current_merge_cache_bytes(&c) as u64);
        }
        // LruCache::put() will automatically evict the LRU entry when at
        // capacity — no additional action needed here.
    });
}

fn estimate_cached_merge_template_bytes(entry: &CachedMergeTemplate) -> usize {
    entry.merge_sql_template.len()
        + entry.parameterized_merge_sql.len()
        + entry.cleanup_sql_template.len()
        + entry.trigger_delete_template.len()
        + entry.trigger_update_template.len()
        + entry.trigger_insert_template.len()
        + entry.trigger_using_template.len()
        + entry.delta_sql_template.len()
        + entry
            .source_oids
            .len()
            .saturating_mul(std::mem::size_of::<u32>())
}

fn current_merge_cache_bytes(cache: &LruCache<i64, CachedMergeTemplate>) -> usize {
    cache
        .iter()
        .map(|(_, entry)| estimate_cached_merge_template_bytes(entry))
        .sum()
}

fn enforce_template_cache_byte_cap(
    cache: &mut LruCache<i64, CachedMergeTemplate>,
    incoming_bytes: usize,
) {
    let max_bytes = crate::config::pg_trickle_template_cache_max_bytes();
    if max_bytes == 0 {
        return;
    }

    loop {
        let current = current_merge_cache_bytes(cache);
        if current.saturating_add(incoming_bytes) <= max_bytes {
            break;
        }

        if cache.pop_lru().is_some() {
            crate::shmem::increment_template_cache_evictions();
        } else {
            break;
        }
    }
}

/// PERF-2 (v0.63.0): Retrieve the delta SQL template and source OIDs from the
/// MERGE template cache for fused refresh eligibility checks.
///
/// Returns `Some((delta_sql_template, source_oids, user_cols_needed, is_deduplicated))`
/// when a valid, non-stale cache entry exists for `pgt_id`.
/// Returns `None` when the cache is cold or the entry is stale (hash mismatch).
pub(crate) fn get_fused_refresh_template(
    pgt_id: i64,
    defining_query_hash: u64,
) -> Option<(String, Vec<u32>, bool)> {
    MERGE_TEMPLATE_CACHE.with(|cache| {
        cache
            .borrow()
            .peek(&pgt_id)
            .filter(|entry| entry.defining_query_hash == defining_query_hash)
            .map(|entry| {
                (
                    entry.delta_sql_template.as_ref().to_owned(),
                    entry.source_oids.clone(),
                    entry.is_deduplicated,
                )
            })
    })
}

pub fn invalidate_merge_cache(pgt_id: i64) {
    MERGE_TEMPLATE_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        c.pop(&pgt_id);
        crate::shmem::set_template_cache_bytes(current_merge_cache_bytes(&c) as u64);
    });
    // D-2: Also deallocate any prepared statement for this ST.
    if PREPARED_MERGE_STMTS.with(|s| s.borrow_mut().remove(&pgt_id)) {
        deallocate_prepared_merge_statement(pgt_id);
    }
    // Also invalidate the delta SQL template cache, which embeds the
    // CDC bitmask width.  Without this, a cache-miss rebuild of the MERGE
    // template would still pull a stale delta SQL template (e.g. 3-bit
    // mask) from the DELTA_TEMPLATE_CACHE, producing "cannot AND bit
    // strings of different sizes" on the first UPDATE after a CDC rebuild.
    crate::dvm::invalidate_delta_cache(pgt_id);
}

/// CACHE-3: Flush all thread-local template caches for the current backend.
///
/// Called by `pgtrickle.clear_caches()` to evict all L1 entries.
/// The caller is responsible for advancing `CACHE_GENERATION` so other
/// backends also invalidate their L1 caches on the next refresh.
pub fn flush_local_template_cache() {
    // Flush L1: MERGE template cache.
    MERGE_TEMPLATE_CACHE.with(|cache| {
        cache.borrow_mut().clear();
        crate::shmem::set_template_cache_bytes(0);
    });
    // Flush all prepared statements.
    PREPARED_MERGE_STMTS.with(|s| {
        for pgt_id in s.borrow().iter().copied() {
            deallocate_prepared_merge_statement(pgt_id);
        }
        s.borrow_mut().clear();
    });
    // Flush delta SQL template cache.
    crate::dvm::flush_all_delta_caches();
}

/// CACHE-1: Check if the L1 cache has a valid entry for `pgt_id`.
///
/// The `cache_generation` parameter is the current `CACHE_GENERATION` shmem
/// value.  An L1 entry is "valid" if it exists and its `defining_query_hash`
/// is non-zero (i.e., the cache was populated by a previous refresh, not just
/// a structural prewarm).  Full L0 (cross-backend) lookup is handled by the
/// L2 catalog path in `execute_differential_refresh`.
pub fn has_template_cache_entry(pgt_id: i64, _cache_generation: u64) -> bool {
    // P-8: Use peek() (shared borrow, does not update LRU order) since this
    // is purely a membership check with no actual cache use.
    MERGE_TEMPLATE_CACHE.with(|cache| cache.borrow().peek(&pgt_id).is_some())
}

/// Wide-table MERGE hash threshold (F41: G4.6).
///
/// When a table has more than this many user columns, the MERGE's
/// `WHEN MATCHED` IS DISTINCT FROM guard uses a hash comparison instead
/// of per-column checks, reducing SQL text length and planner overhead.
pub(crate) const WIDE_TABLE_HASH_THRESHOLD: usize = 50;

/// Build the IS DISTINCT FROM clause for the MERGE `WHEN MATCHED` guard.
///
/// For tables with ≤ [`WIDE_TABLE_HASH_THRESHOLD`] columns, generates
/// per-column text comparisons joined with `OR`.
///
/// The text cast avoids PostgreSQL's requirement that the underlying type
/// implement `=` for `IS DISTINCT FROM` to work. This matters for `json`
/// aggregate outputs, which intentionally do not have a native equality
/// operator.
///
/// For wider tables (F41), generates a single xxh64 hash comparison using
/// `pgtrickle.pg_trickle_hash()`:
/// ```sql
/// pgtrickle.pg_trickle_hash(concat_ws('\x1E', COALESCE(st."c1"::text,''), ...))
/// IS DISTINCT FROM
/// pgtrickle.pg_trickle_hash(concat_ws('\x1E', COALESCE(d."c1"::text,''), ...))
/// ```
pub(crate) fn build_is_distinct_clause(user_cols: &[String]) -> String {
    if user_cols.len() <= WIDE_TABLE_HASH_THRESHOLD {
        user_cols
            .iter()
            .map(|c| {
                let qc = format!("\"{}\"", c.replace('"', "\"\""));
                format!("st.{qc}::text IS DISTINCT FROM d.{qc}::text")
            })
            .collect::<Vec<_>>()
            .join(" OR ")
    } else {
        // Hash-based comparison for wide tables using xxh64 (faster than md5).
        // Uses pgtrickle.pg_trickle_hash() which is IMMUTABLE + PARALLEL SAFE.
        let hash_expr = |prefix: &str| -> String {
            let parts: Vec<String> = user_cols
                .iter()
                .map(|c| {
                    let qc = format!("\"{}\"", c.replace('"', "\"\""));
                    format!("COALESCE({prefix}.{qc}::text, '')")
                })
                .collect();
            format!(
                "pgtrickle.pg_trickle_hash(concat_ws({sep}, {cols}))",
                sep = "'\\x1E'",
                cols = parts.join(", ")
            )
        };
        format!("{} IS DISTINCT FROM {}", hash_expr("st"), hash_expr("d"))
    }
}

/// B3-2: Build a USING clause with weight aggregation for cross-source
/// deduplication.
///
/// Replaces the previous `DISTINCT ON (__pgt_row_id)` approach which silently
/// discarded corrections that should be algebraically combined.  Weight
/// aggregation correctly handles diamond-flow queries where multiple delta
/// branches produce overlapping corrections for the same `__pgt_row_id`:
///
/// - Net weight > 0 → INSERT
/// - Net weight < 0 → DELETE
/// - Net weight = 0 → filtered out (no-op)
///
/// B3-3: After weight aggregation an outer `DISTINCT ON (__pgt_row_id)` step
/// resolves the UPDATE case where PK-stable row IDs are used for joins.
///
/// When only a non-PK column changes (e.g. category_name), diff_project
/// produces a D row and an I row that share the same `__pgt_row_id` (because
/// the hash is over PK columns only).  The weight aggregation groups by
/// `(row_id, col_list)`, so these two rows land in *different* groups and both
/// survive the HAVING clause — giving two MERGE source rows targeting the same
/// ST row, which PostgreSQL rejects with "MERGE command cannot affect row a
/// second time".
///
/// The outer `DISTINCT ON` resolves this: for each `__pgt_row_id` that appears
/// as both D and I, `ORDER BY … CASE WHEN action='I' THEN 0 ELSE 1 END`
/// selects the I row (carrying the new column values).  MERGE then performs a
/// single WHEN MATCHED … UPDATE, which is the correct semantic for a non-PK
/// column change.
pub(crate) fn build_weight_agg_using(delta_sql: &str, user_col_list: &str) -> String {
    format!(
        "(SELECT DISTINCT ON (\"__pgt_row_id\") \
                \"__pgt_row_id\", \"__pgt_action\", {user_col_list} \
         FROM (\
             SELECT __pgt_row_id, \
                    CASE WHEN SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE -1 END) > 0 \
                         THEN 'I' ELSE 'D' END AS __pgt_action, \
                    {user_col_list} \
             FROM ({delta_sql}) __raw \
             GROUP BY __pgt_row_id, {user_col_list} \
             HAVING SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE -1 END) <> 0\
         ) \"__weighted\" \
         ORDER BY \"__pgt_row_id\", \
                  CASE WHEN \"__pgt_action\" = 'I' THEN 0 ELSE 1 END)"
    )
}

/// EC-06a: Weight-aggregate a keyless delta to cancel within-delta I/D pairs.
///
/// Without this, the 3-step DML (DELETE → UPDATE → INSERT) processes D and I
/// actions independently.  When the EC-02 correction term produces a DELETE
/// with hash H and another part produces an INSERT with the same H (or vice
/// versa), the DELETE step searches *storage* (which may not contain H) while
/// the INSERT step adds a row unconditionally — creating a phantom row.
///
/// This wrapper groups delta rows by `(__pgt_row_id, user_cols)`, computes the
/// net action (INSERT if positive, DELETE if negative, filtered if zero), and
/// expands back to the correct row count via `generate_series`.
///
/// Unlike [`build_weight_agg_using`] (keyed), this does **not** use
/// `DISTINCT ON (__pgt_row_id)` because keyless tables intentionally allow
/// multiple rows with the same `__pgt_row_id` but different column values.
pub(crate) fn build_keyless_weight_agg(delta_sql: &str, user_col_list: &str) -> String {
    // `__pgt_count` is already the logical multiplicity state for aggregate
    // and DISTINCT rows; expanding it would create duplicate physical rows.
    let expansion = if user_col_list.contains("\"__pgt_count\"") {
        ""
    } else {
        ", LATERAL generate_series(1, __w.__pgt_cnt) __gs"
    };
    format!(
        "(SELECT \"__pgt_row_id\", \"__pgt_action\", {user_col_list} \
         FROM (\
             SELECT __pgt_row_id, \
                    CASE WHEN SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE -1 END) > 0 \
                         THEN 'I' ELSE 'D' END AS __pgt_action, \
                    {user_col_list}, \
                    ABS(SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE -1 END)) AS __pgt_cnt \
             FROM ({delta_sql}) __raw \
             GROUP BY __pgt_row_id, {user_col_list} \
             HAVING SUM(CASE WHEN __pgt_action = 'I' THEN 1 ELSE -1 END) <> 0\
         ) __w{expansion})"
    )
}

/// EC-06: Build a counted DELETE template for keyless sources.
///
/// For keyless tables, multiple stream table rows can share the same
/// `__pgt_row_id` (content hash). A plain `DELETE ... USING delta` would
/// remove ALL matching rows, not just the intended count. This template
/// uses ROW_NUMBER on both the stream table and delta to pair them 1:1,
/// then deletes only the paired ctids.
pub(crate) fn build_keyless_delete_template(quoted_table: &str, pgt_id: i64) -> String {
    format!(
        "DELETE FROM {quoted_table} \
         WHERE ctid IN (\
           SELECT numbered_st.st_ctid \
           FROM (\
             SELECT st2.ctid AS st_ctid, \
                    st2.__pgt_row_id, \
                    ROW_NUMBER() OVER (\
                      PARTITION BY st2.__pgt_row_id ORDER BY st2.ctid\
                    ) AS st_rn \
             FROM {quoted_table} st2 \
             WHERE EXISTS (\
               SELECT 1 FROM (\
                 SELECT DISTINCT __pgt_row_id \
                 FROM __pgt_delta_{pgt_id} \
                 WHERE __pgt_action = 'D'\
               ) d \
               WHERE pgtrickle.row_probe_v1(st2.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) \
                 AND st2.__pgt_row_id = d.__pgt_row_id\
             )\
           ) numbered_st \
           JOIN (\
             SELECT __pgt_row_id, \
                    COUNT(*)::INT AS del_count \
             FROM __pgt_delta_{pgt_id} \
             WHERE __pgt_action = 'D' \
             GROUP BY __pgt_row_id\
           ) dc ON pgtrickle.row_probe_v1(numbered_st.__pgt_row_id) = pgtrickle.row_probe_v1(dc.__pgt_row_id) \
                         AND numbered_st.__pgt_row_id = dc.__pgt_row_id \
           WHERE numbered_st.st_rn <= dc.del_count\
         )",
        pgt_id = pgt_id,
    )
}

/// Build an INSERT SQL statement for append-only stream tables.
///
/// Extracts the USING clause from the MERGE template and rewrites it as:
///   INSERT INTO "schema"."table" (__pgt_row_id, user_cols...)
///   SELECT d.__pgt_row_id, d.user_cols...
///   FROM (...delta...) AS d
///   WHERE d.__pgt_action = 'I'
///
/// PH-E1: Estimate the output cardinality of the delta subquery by running
/// a capped COUNT. Returns `None` if the USING clause cannot be extracted or
/// the query fails (estimation is best-effort).
///
/// Extracts the delta subquery from the MERGE SQL's USING clause and runs:
///   `SELECT count(*) FROM (delta_query LIMIT <limit+1>) __pgt_est`
pub(crate) fn estimate_delta_output_rows(merge_sql: &str, limit: i32) -> Option<i64> {
    // Extract USING clause: between "USING " and " AS d ON"
    let using_start = merge_sql.find("USING ").map(|p| p + 6)?;
    let using_end = merge_sql.find(" AS d ON ")?;
    let using_clause = &merge_sql[using_start..using_end];

    let cap = (limit as i64) + 1;
    let estimate_sql = format!("SELECT count(*) FROM ({using_clause} LIMIT {cap}) __pgt_est");

    match Spi::get_one::<i64>(&estimate_sql) {
        Ok(Some(count)) => Some(count),
        Ok(None) => Some(0),
        Err(e) => {
            pgrx::debug1!(
                "[pg_trickle] PH-E1: delta estimation query failed (non-fatal): {}",
                e,
            );
            None
        }
    }
}

/// Check whether the delta SQL contains CTE markers from DVM operators
/// that are **not insert-monotonic** — i.e., where INSERT-only source
/// changes can still produce DELETE or UPDATE actions in the delta output.
///
/// When this returns `true`, the append-only INSERT fast path (A-3a) is
/// unsafe because the bare `INSERT … WHERE __pgt_action = 'I'` would miss
/// delta DELETEs and duplicate-key UPDATEs.
pub(crate) fn has_non_monotonic_cte(sql: &str) -> bool {
    sql.contains("__pgt_cte_agg_") // Aggregate: group updates → 'I' with existing row_id
        || sql.contains("__pgt_cte_join_") // INNER JOIN: source DELETEs/UPDATEs produce delta DELETEs
        || sql.contains("__pgt_cte_left_join_") // LEFT JOIN: right INSERTs remove NULL-padded rows
        || sql.contains("__pgt_cte_lj_") // LEFT JOIN flags CTE
        || sql.contains("__pgt_cte_full_join_") // FULL JOIN
        || sql.contains("__pgt_cte_fj_") // FULL JOIN flags CTE
        || sql.contains("__pgt_cte_anti_join_") // NOT EXISTS / ALL subquery
        || sql.contains("__pgt_cte_semi_join_") // EXISTS subquery
        || sql.contains("__pgt_cte_r_old_") // Semi/anti join pre-change snapshot
        || sql.contains("__pgt_cte_isect_") // INTERSECT: right INSERTs remove left rows
        || sql.contains("__pgt_cte_dist_") // DISTINCT: INSERTs change dedup outcome
        || sql.contains("__pgt_cte_exct_") // EXCEPT: right INSERTs remove left rows
        || sql.contains("__pgt_cte_win_") // Window: INSERTs change partition values
        || sql.contains("__pgt_cte_scalar_sub_") // Scalar subquery: value changes
        || sql.contains("__pgt_cte_sq_gate_") // Scalar subquery gate
        || sql.contains("__pgt_cte_lat_sq_") // Lateral subquery
        || sql.contains("__pgt_cte_rc_") // Recursive CTE
        || sql.contains("__pgt_cte_dred_") // Recursive CTE (DRed)
        || sql.contains("__pgt_cte_lat_changed_") // Lateral function
        || sql.contains("__pgt_cte_lat_old_") // Lateral function
}

/// Build an `INSERT ... SELECT` SQL statement from a MERGE SQL template
/// for append-only stream tables.
///
/// This is significantly faster than MERGE for append-only workloads
/// because it skips the DELETE, UPDATE, and IS DISTINCT FROM checks.
pub(crate) fn build_append_only_insert_sql(schema: &str, name: &str, merge_sql: &str) -> String {
    let quoted_table = format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        name.replace('"', "\"\""),
    );

    // Parse the MERGE SQL to extract:
    // 1. The USING clause (delta subquery)
    // 2. The INSERT column list
    //
    // MERGE INTO "schema"."table" AS st USING (...) AS d ON ...
    // ... WHEN NOT MATCHED AND d.__pgt_action = 'I' THEN
    //   INSERT (__pgt_row_id, col1, col2, ...)
    //   VALUES (d.__pgt_row_id, d.col1, d.col2, ...)

    // Extract USING clause: between "USING " and " AS d ON"
    let using_start = merge_sql.find("USING ").map(|p| p + 6).unwrap_or(0);
    let using_end = merge_sql.find(" AS d ON ").unwrap_or(merge_sql.len());
    let using_clause = &merge_sql[using_start..using_end];

    // Extract column list from INSERT clause
    let insert_marker = "INSERT (";
    let insert_start = merge_sql
        .rfind(insert_marker)
        .map(|p| p + insert_marker.len())
        .unwrap_or(0);
    let insert_end = merge_sql[insert_start..]
        .find(')')
        .map(|p| insert_start + p)
        .unwrap_or(merge_sql.len());
    let col_list = &merge_sql[insert_start..insert_end];

    // Extract VALUES column list (d.col prefixed)
    let values_marker = "VALUES (";
    let values_start = merge_sql
        .rfind(values_marker)
        .map(|p| p + values_marker.len())
        .unwrap_or(0);
    let values_end = merge_sql[values_start..]
        .find(')')
        .map(|p| values_start + p)
        .unwrap_or(merge_sql.len());
    let d_col_list = &merge_sql[values_start..values_end];

    format!(
        "INSERT INTO {quoted_table} ({col_list}) \
         SELECT {d_col_list} \
         FROM {using_clause} AS d \
         WHERE d.__pgt_action = 'I' \
         ON CONFLICT (__pgt_row_id) DO NOTHING"
    )
}

// ── TG2-MERGE: Extracted pure template builders ─────────────────────
//
// These functions are the core MERGE/DML template assembly logic, extracted
// from prewarm_merge_cache() and execute_differential_refresh() so they can
// be unit-tested without a database.

/// Format a quoted column list: `"col1", "col2", "col3"`.
pub(crate) fn format_col_list(user_cols: &[String]) -> String {
    user_cols
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format a prefixed quoted column list: `d."col1", d."col2"`.
pub(crate) fn format_prefixed_col_list(prefix: &str, user_cols: &[String]) -> String {
    user_cols
        .iter()
        .map(|c| format!("{prefix}.\"{}\"", c.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Format an UPDATE SET clause: `"col1" = d."col1", "col2" = d."col2"`.
pub(crate) fn format_update_set(user_cols: &[String]) -> String {
    user_cols
        .iter()
        .map(|c| {
            let qc = format!("\"{}\"", c.replace('"', "\"\""));
            format!("{qc} = d.{qc}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Match candidates with the bounded probe first, then verify complete BYTEA
/// equality. The second comparison is the correctness condition; the probe is
/// only an accelerator and is never treated as an identity.
pub(crate) fn build_row_id_match(left: &str, right: &str) -> String {
    format!("pgtrickle.row_probe_v1({left}) = pgtrickle.row_probe_v1({right}) AND {left} = {right}")
}

/// Build the core MERGE SQL template for differential refresh.
///
/// This is the primary delta-application statement: it merges incoming
/// delta rows (with `__pgt_action` = 'I' or 'D') into the stream table,
/// performing DELETE, UPDATE, or INSERT as appropriate.
///
/// Extracted as a pure function for unit testability (TG2-MERGE).
pub(crate) fn build_merge_sql(
    quoted_table: &str,
    using_clause: &str,
    user_cols: &[String],
    has_partition_key: bool,
) -> String {
    let user_col_list = format_col_list(user_cols);
    let d_user_col_list = format_prefixed_col_list("d", user_cols);
    let update_set_clause = format_update_set(user_cols);
    let is_distinct_clause = build_is_distinct_clause(user_cols);
    let row_id_match = build_row_id_match("st.__pgt_row_id", "d.__pgt_row_id");

    format!(
        "MERGE INTO {quoted_table} AS st \
         USING {using_clause} AS d \
         ON {row_id_match}{part} \
         WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE \
         WHEN MATCHED AND d.__pgt_action = 'I' AND ({is_distinct_clause}) THEN \
           UPDATE SET {update_set_clause} \
         WHEN NOT MATCHED AND d.__pgt_action = 'I' THEN \
           INSERT (__pgt_row_id, {user_col_list}) \
           VALUES (d.__pgt_row_id, {d_user_col_list})",
        part = if has_partition_key {
            " __PGT_PART_PRED__"
        } else {
            ""
        },
    )
}

/// Build the trigger-path DELETE template.
///
/// For keyless sources, uses counted DELETE via ROW_NUMBER to avoid
/// removing all rows with a matching row_id. For keyed sources, uses
/// a simple equi-join DELETE.
pub(crate) fn build_trigger_delete_sql(
    quoted_table: &str,
    pgt_id: i64,
    use_keyless: bool,
) -> String {
    if use_keyless {
        build_keyless_delete_template(quoted_table, pgt_id)
    } else {
        format!(
            "DELETE FROM {quoted_table} AS st \
             USING __pgt_delta_{pgt_id} AS d \
             WHERE {row_id_match} \
               AND d.__pgt_action = 'D'",
            row_id_match = build_row_id_match("st.__pgt_row_id", "d.__pgt_row_id"),
        )
    }
}

/// Build the trigger-path UPDATE template.
///
/// Updates existing rows where the delta action is 'I' and values changed
/// (IS DISTINCT FROM guard prevents no-op writes).
pub(crate) fn build_trigger_update_sql(
    quoted_table: &str,
    pgt_id: i64,
    user_cols: &[String],
) -> String {
    let update_set_clause = format_update_set(user_cols);
    let is_distinct_clause = build_is_distinct_clause(user_cols);
    format!(
        "UPDATE {quoted_table} AS st \
         SET {update_set_clause} \
         FROM __pgt_delta_{pgt_id} AS d \
         WHERE {row_id_match} \
           AND d.__pgt_action = 'I' \
           AND ({is_distinct_clause})",
        row_id_match = build_row_id_match("st.__pgt_row_id", "d.__pgt_row_id"),
    )
}

/// Build the trigger-path INSERT template.
///
/// For keyless sources, uses plain INSERT (no NOT EXISTS check since
/// duplicate row_ids are expected). For keyed sources, uses
/// `ON CONFLICT (__pgt_row_id) DO UPDATE SET …` (upsert) which:
///   - Inserts genuinely new rows (__pgt_row_id absent from ST)
///   - Updates existing rows when column values have changed
///   - Is a no-op when column values are identical
///
/// This replaces the previous `NOT EXISTS` approach which was vulnerable
/// to race conditions when Part 1 and Part 2 of the join delta produce
/// different __pgt_row_id hashes for the same logical row — the phantom
/// rows from prior refreshes could cause duplicate-key violations during
/// the INSERT because the NOT EXISTS check evaluated against a snapshot
/// that didn't include concurrently committed rows.
pub(crate) fn build_trigger_insert_sql(
    quoted_table: &str,
    pgt_id: i64,
    user_cols: &[String],
    use_keyless: bool,
) -> String {
    let user_col_list = format_col_list(user_cols);
    let d_user_col_list = format_prefixed_col_list("d", user_cols);
    if use_keyless && user_cols.iter().any(|column| column == "__pgt_count") {
        build_trigger_insert_sql_no_conflict(quoted_table, pgt_id, user_cols)
    } else if use_keyless {
        format!(
            "INSERT INTO {quoted_table} (__pgt_row_id, {user_col_list}) \
             SELECT d.__pgt_row_id, {d_user_col_list} \
             FROM __pgt_delta_{pgt_id} AS d \
             WHERE d.__pgt_action = 'I'",
        )
    } else {
        // Keyed: DISTINCT ON eliminates within-delta duplicates; ON CONFLICT
        // is a safety net for any remaining row_id collisions with the ST.
        // Callers that need guaranteed conflict-free inserts (PH-D1) should
        // delete all matching row_ids from the ST before executing this.
        format!(
            "INSERT INTO {quoted_table} (__pgt_row_id, {user_col_list}) \
             SELECT DISTINCT ON (d.__pgt_row_id) d.__pgt_row_id, {d_user_col_list} \
             FROM __pgt_delta_{pgt_id} AS d \
             WHERE d.__pgt_action = 'I' \
             ORDER BY d.__pgt_row_id \
             ON CONFLICT (__pgt_row_id) DO NOTHING",
        )
    }
}

/// Build an INSERT for a deduplicated delta when the target row-id index is
/// non-unique. The preceding UPDATE handles an existing row; this exact
/// `NOT EXISTS` guard prevents the INSERT from duplicating it without relying
/// on `ON CONFLICT (__pgt_row_id)`.
pub(crate) fn build_trigger_insert_sql_no_conflict(
    quoted_table: &str,
    pgt_id: i64,
    user_cols: &[String],
) -> String {
    let user_col_list = format_col_list(user_cols);
    let d_user_col_list = format_prefixed_col_list("d", user_cols);
    format!(
        "INSERT INTO {quoted_table} (__pgt_row_id, {user_col_list}) \
         SELECT DISTINCT ON (d.__pgt_row_id) d.__pgt_row_id, {d_user_col_list} \
         FROM __pgt_delta_{pgt_id} AS d \
         WHERE d.__pgt_action = 'I' \
           AND NOT EXISTS (\
             SELECT 1 FROM {quoted_table} AS st \
             WHERE {row_id_match}\
           ) \
         ORDER BY d.__pgt_row_id",
        row_id_match = build_row_id_match("st.__pgt_row_id", "d.__pgt_row_id"),
    )
}

/// Pre-warm the delta SQL + MERGE template caches for a stream table.
///
/// Called after `create_stream_table()` to avoid a cold-start penalty on
/// the first differential refresh (cycle 1). This generates the delta SQL
/// template and MERGE SQL template with placeholder tokens, caching them
/// for subsequent refreshes.
///
/// Errors are logged but not propagated — cache pre-warming is optional.
pub fn prewarm_merge_cache(st: &StreamTableMeta) {
    use crate::version::Frontier;
    use std::hash::{Hash, Hasher};

    let schema = &st.pgt_schema;
    let name = &st.pgt_name;

    if matches!(dvm::query_has_recursive_cte(&st.defining_query), Ok(true)) {
        pgrx::debug1!(
            "[pg_trickle] cache pre-warm skipped for {}.{}: recursive CTEs choose refresh strategy at runtime",
            schema,
            name,
        );
        return;
    }

    // Use dummy frontiers — placeholders will be embedded in the template
    let dummy = Frontier::new();

    let delta_result = match dvm::generate_delta_query_cached(
        st.pgt_id,
        &st.defining_query,
        &dummy,
        &dummy,
        schema,
        name,
    ) {
        Ok(r) => r,
        Err(e) => {
            pgrx::log!(
                "pg_trickle: cache pre-warm skipped for {}.{}: {}",
                schema,
                name,
                e
            );
            return;
        }
    };

    // Build the MERGE template (same logic as the cache-miss path in
    // execute_differential_refresh, but we only store the template).
    let user_cols = &delta_result.output_columns;
    let source_oids = &delta_result.source_oids;

    let quoted_table = format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        name.replace('"', "\"\""),
    );

    let user_col_list = format_col_list(user_cols);

    let delta_sql_template =
        dvm::get_delta_sql_template(st.pgt_id).unwrap_or(delta_result.delta_sql);

    // Build the USING clause — skip deduplication when the delta is already
    // deduplicated (G-M1 optimization for scan-chain queries).
    //
    // EC-06: For keyless sources the delta is never deduplicated (multiple
    // rows can share the same __pgt_row_id). Skip deduplication unconditionally.
    //
    // B3-2: For non-deduplicated deltas, use weight aggregation instead of
    // DISTINCT ON.  Weight aggregation correctly handles diamond-flow queries
    // where multiple delta branches produce overlapping corrections.
    //
    // A-2: When `has_key_changed` is available on a deduplicated delta, wrap
    // the USING clause with a filter that suppresses D-side rows for value-only
    // UPDATEs (__pgt_key_changed = FALSE).  The remaining I-side row triggers
    // the existing WHEN MATCHED THEN UPDATE clause — converting a DELETE+INSERT
    // cycle into a single UPDATE (cheaper WAL, HOT-eligible, no index churn).
    let using_clause = if delta_result.is_deduplicated && delta_result.has_key_changed {
        format!(
            "(SELECT * FROM ({delta_sql_template}) __d \
             WHERE NOT (__d.__pgt_action = 'D' AND __d.__pgt_key_changed = FALSE))"
        )
    } else if delta_result.is_deduplicated {
        format!("({delta_sql_template})")
    } else if st.has_keyless_source {
        // EC-06a: Weight-aggregate keyless deltas to cancel within-delta
        // I/D pairs.  Without this, the 3-step DML (DELETE → INSERT)
        // processes them independently: the DELETE targets storage rows
        // (which may not exist for intermediate hashes), while the INSERT
        // adds unconditionally — creating phantom rows on every refresh
        // cycle where both join sides change simultaneously (EC-02).
        build_keyless_weight_agg(&delta_sql_template, &user_col_list)
    } else {
        build_weight_agg_using(&delta_sql_template, &user_col_list)
    };

    let merge_template = build_merge_sql(
        &quoted_table,
        &using_clause,
        user_cols,
        st.st_partition_key.is_some(),
    );

    // Build cleanup template.
    let cleanup_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let cleanup_stmts: Vec<String> = source_oids
        .iter()
        .map(|oid| {
            // CITUS-4: Use stable buffer name for DELETE; keep OID-keyed LSN tokens.
            let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(*oid));
            format!(
                "DELETE FROM \"{cleanup_schema}\".{buf_name} \
                 WHERE lsn > '__PGS_PREV_LSN_{oid}__'::pg_lsn \
                 AND lsn <= '__PGS_NEW_LSN_{oid}__'::pg_lsn",
            )
        })
        .collect();
    let cleanup_template = cleanup_stmts.join(";");

    // PERF-2: Use the hash pre-computed at CREATE/ALTER time and stored in the
    // catalog.  Fall back to computing it if the stored value is 0 (e.g. rows
    // that existed before the v0.59.0 upgrade and haven't been ALTERed yet).
    let query_hash = {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        "v0.87.8-stage".hash(&mut hasher);
        if st.defining_query_hash != 0 {
            st.defining_query_hash.hash(&mut hasher);
        } else {
            st.defining_query.hash(&mut hasher);
        }
        hasher.finish()
    };

    // D-2: Parameterize MERGE template for prepared-statement execution.
    let parameterized_merge_sql = parameterize_lsn_template(&merge_template, source_oids);

    // ── User-trigger explicit DML templates ──────────────────────────
    //
    // EC-06: Keyless sources use counted DELETE (ROW_NUMBER matching)
    // and plain INSERT (no NOT EXISTS / ON CONFLICT) since duplicate
    // __pgt_row_id values are expected. The UPDATE step is a no-op
    // because the scan-level net counting decomposes updates into
    // separate D + I rows.
    let trigger_delete_template =
        build_trigger_delete_sql(&quoted_table, st.pgt_id, st.has_keyless_source);

    // EC-06: For keyless sources, the scan-level delta decomposes UPDATEs
    // into D+I pairs (different content hashes), so the UPDATE template
    // naturally matches 0 rows. For aggregate queries on keyless sources,
    // the aggregate delta produces 'I' actions for changed groups that
    // need real UPDATEs. Using the normal UPDATE template handles both
    // cases correctly.
    let trigger_update_template = build_trigger_update_sql(&quoted_table, st.pgt_id, user_cols);

    let trigger_insert_template = if st.has_keyless_source
        && (delta_result.is_deduplicated || user_cols.iter().any(|column| column == "__pgt_count"))
    {
        build_trigger_insert_sql_no_conflict(&quoted_table, st.pgt_id, user_cols)
    } else {
        build_trigger_insert_sql(&quoted_table, st.pgt_id, user_cols, st.has_keyless_source)
    };

    // Cache the MERGE template with LSN placeholder tokens.
    // Each refresh resolves the tokens to concrete LSN values
    // via string substitution, then executes the resolved SQL.
    // P-8: Resize LRU cache if needed, then put() (automatically evicts LRU at capacity).
    maybe_evict_lru_cache_entry();
    let new_entry = CachedMergeTemplate {
        defining_query_hash: query_hash,
        // PERF-3: convert to Arc<str> so cache-hit clones are O(1).
        merge_sql_template: merge_template.into(),
        parameterized_merge_sql: parameterized_merge_sql.into(),
        source_oids: source_oids.clone(),
        cleanup_sql_template: cleanup_template.into(),
        trigger_delete_template: trigger_delete_template.into(),
        trigger_update_template: trigger_update_template.into(),
        trigger_insert_template: trigger_insert_template.into(),
        trigger_using_template: using_clause.clone().into(),
        delta_sql_template: delta_sql_template.clone().into(),
        is_all_algebraic: delta_result.is_all_algebraic,
        is_deduplicated: delta_result.is_deduplicated,
    };
    let incoming_bytes = estimate_cached_merge_template_bytes(&new_entry);

    MERGE_TEMPLATE_CACHE.with(|cache| {
        let mut c = cache.borrow_mut();
        enforce_template_cache_byte_cap(&mut c, incoming_bytes);
        c.put(st.pgt_id, new_entry);
        crate::shmem::set_template_cache_bytes(current_merge_cache_bytes(&c) as u64);
    });

    pgrx::log!(
        "pg_trickle: pre-warmed delta+MERGE cache for {}.{}",
        schema,
        name
    );
}

// CODE-002: Unit tests for pure SQL-generation helpers.
// These run without a PostgreSQL backend using `just test-unit`.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::{Frontier, SourceVersion};

    // ── build_content_hash_expr ─────────────────────────────────────

    #[test]
    fn test_build_content_hash_expr_zero_cols_returns_row_id() {
        let result = build_content_hash_expr("t.", &[]);
        assert_eq!(
            result,
            "pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8))"
        );
    }

    #[test]
    fn test_build_content_hash_expr_single_col() {
        let result = build_content_hash_expr("t.", &["name".to_string()]);
        assert_eq!(
            result,
            "pgtrickle.encode_row_id_v2('KEYLESS_ROW', ROW(t.\"name\"))"
        );
    }

    #[test]
    fn test_build_content_hash_expr_single_col_with_prefix() {
        let result = build_content_hash_expr("old.", &["val".to_string()]);
        assert_eq!(
            result,
            "pgtrickle.encode_row_id_v2('KEYLESS_ROW', ROW(old.\"val\"))"
        );
    }

    #[test]
    fn test_build_content_hash_expr_multi_col() {
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let result = build_content_hash_expr("t.", &cols);
        assert_eq!(
            result,
            "pgtrickle.encode_row_id_v2('KEYLESS_ROW', ROW(t.\"a\", t.\"b\", t.\"c\"))"
        );
    }

    #[test]
    fn test_build_content_hash_expr_col_with_double_quote() {
        // Column names containing double-quotes must be escaped.
        let cols = vec!["my\"col".to_string()];
        let result = build_content_hash_expr("d.", &cols);
        assert!(
            result.contains("\"my\"\"col\""),
            "double-quote in col name should be escaped: got {result}"
        );
    }

    // ── parameterize_lsn_template ───────────────────────────────────

    #[test]
    fn test_parameterize_lsn_template_single_source() {
        let template = "SELECT * FROM t WHERE lsn > '__PGS_PREV_LSN_42__'::pg_lsn AND lsn <= '__PGS_NEW_LSN_42__'::pg_lsn";
        let result = parameterize_lsn_template(template, &[42]);
        assert_eq!(result, "SELECT * FROM t WHERE lsn > $1 AND lsn <= $2");
    }

    #[test]
    fn test_parameterize_lsn_template_multiple_sources() {
        let template = "'__PGS_PREV_LSN_1__'::pg_lsn AND '__PGS_NEW_LSN_1__'::pg_lsn AND '__PGS_PREV_LSN_2__'::pg_lsn AND '__PGS_NEW_LSN_2__'::pg_lsn";
        let result = parameterize_lsn_template(template, &[1, 2]);
        assert_eq!(result, "$1 AND $2 AND $3 AND $4");
    }

    #[test]
    fn test_parameterize_lsn_template_no_sources() {
        let template = "SELECT 1";
        let result = parameterize_lsn_template(template, &[]);
        assert_eq!(
            result, "SELECT 1",
            "empty source list should leave template unchanged"
        );
    }

    // ── build_prepare_type_list ─────────────────────────────────────

    #[test]
    fn test_build_prepare_type_list_zero_sources() {
        assert_eq!(build_prepare_type_list(0), "");
    }

    #[test]
    fn test_build_prepare_type_list_one_source() {
        assert_eq!(build_prepare_type_list(1), "pg_lsn, pg_lsn");
    }

    #[test]
    fn test_build_prepare_type_list_three_sources() {
        let result = build_prepare_type_list(3);
        assert_eq!(result, "pg_lsn, pg_lsn, pg_lsn, pg_lsn, pg_lsn, pg_lsn");
        assert_eq!(
            result.split(", ").count(),
            6,
            "3 sources * 2 params each = 6 types"
        );
    }

    // ── build_execute_params ────────────────────────────────────────

    fn make_frontier(entries: &[(u32, &str)]) -> Frontier {
        let mut f = Frontier::new();
        for &(oid, lsn) in entries {
            f.sources.insert(
                oid.to_string(),
                SourceVersion {
                    lsn: lsn.to_string(),
                    snapshot_ts: "2026-01-01T00:00:00Z".to_string(),
                },
            );
        }
        f
    }

    #[test]
    fn test_build_execute_params_single_source() {
        let prev = make_frontier(&[(10, "0/1000")]);
        let new = make_frontier(&[(10, "0/2000")]);
        let result = build_execute_params(&[10], &prev, &new);
        assert_eq!(result, "'0/1000'::pg_lsn, '0/2000'::pg_lsn");
    }

    #[test]
    fn test_build_execute_params_multiple_sources() {
        let prev = make_frontier(&[(1, "0/A"), (2, "0/B")]);
        let new = make_frontier(&[(1, "0/C"), (2, "0/D")]);
        let result = build_execute_params(&[1, 2], &prev, &new);
        assert_eq!(
            result,
            "'0/A'::pg_lsn, '0/C'::pg_lsn, '0/B'::pg_lsn, '0/D'::pg_lsn"
        );
    }

    #[test]
    fn test_build_execute_params_missing_source_defaults_to_zero() {
        let prev = Frontier::new();
        let new = Frontier::new();
        // OID not in frontier — should default to "0/0"
        let result = build_execute_params(&[99], &prev, &new);
        assert_eq!(result, "'0/0'::pg_lsn, '0/0'::pg_lsn");
    }
}
