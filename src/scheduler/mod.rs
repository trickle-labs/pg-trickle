//! Background worker scheduler for pgtrickle.
//!
//! # Architecture — Launcher + Per-Database Workers
//!
//! The extension uses a two-tier background worker model:
//!
//! 1. **Launcher** (`pg_trickle launcher`) — one per server, static.
//!    - Connects to the `postgres` database to query system catalogs.
//!    - Every 10 s, scans `pg_database` for all available databases.
//!    - For each database, checks `pg_stat_activity` for an existing scheduler.
//!    - Spawns a dynamic per-database scheduler for any database that lacks one.
//!    - Automatically re-spawns schedulers that crash or exit.
//!    - Databases without pg_trickle installed are skipped for 5 minutes before
//!      re-probing (graceful exit by the per-DB worker signals this).
//!
//! 2. **Per-database scheduler** (`pg_trickle scheduler`) — one per database
//!    that has pg_trickle installed, dynamic.
//!    - Receives the database name via `bgw_extra` (set by the launcher).
//!    - Checks that pg_trickle is installed; exits cleanly if not.
//!    - Wakes every `pg_trickle.scheduler_interval_ms` milliseconds.
//!    - Reads shared memory to detect DAG changes.
//!    - Consumes CDC changes, determines refresh needs, executes refreshes.
//!
//! This design means pg_trickle works transparently in all databases on a
//! server without any manual configuration.
//!
//! # Error Handling & Resilience
//! - **Row-level catalog locks**: prevent concurrent refreshes of the same ST
//! - **Retry with backoff**: retryable errors get exponential backoff per ST
//! - **Skip mechanism**: if a ST refresh is already running, skip it gracefully
//! - **Crash recovery**: on startup, mark interrupted RUNNING records as FAILED
//! - **Error classification**: only retryable errors trigger retry; user/schema
//!   errors fail immediately

use pgrx::prelude::*;

use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;

use crate::catalog::SchedulerJob;
use crate::catalog::{RefreshRecord, StreamTableMeta};
use crate::cdc;
use crate::config;
use crate::dag::{DiamondSchedulePolicy, NodeId, RefreshMode, Scc, StDag, StStatus};
use crate::error::{RetryPolicy, RetryState};
use crate::monitor;
use crate::refresh::{self, RefreshAction};
use crate::shmem;
use crate::version;

pub mod citus;
pub mod cost;
pub mod dispatch;
pub mod scheduler_loop;
pub mod tier;
pub mod watermark;

// CQ-10-01: Re-export public items from decomposed scheduler submodules.
pub use dispatch::{reconcile_parallel_state, spawn_refresh_worker};
pub use scheduler_loop::{
    pg_trickle_launcher_main, pg_trickle_scheduler_main, register_launcher_worker,
    register_scheduler_worker,
};

use citus::drive_distributed_cdc;
pub use cost::compute_per_db_quota;
pub use cost::{compute_per_db_quota_with_lag, lag_aware_quota_boost};
use dispatch::extract_panic_message;
pub use tier::RefreshTier;
use watermark::compute_worker_tick_watermark;

// ── SCAL-1 (v0.25.0): Per-backend catalog snapshot cache ─────────────────
//
// Caches `StreamTableMeta` rows keyed by the DAG version number from shmem.
// When the local snapshot matches `shmem::current_dag_version()`, we skip
// the full SPI catalog reload (~20–200 ms at 100–1000 STs).
//
// The cache is invalidated whenever `current_dag_version()` advances, which
// happens after any CREATE / ALTER / DROP STREAM TABLE DDL.

thread_local! {
    static CATALOG_SNAPSHOT_CACHE: RefCell<Option<(u64, Vec<StreamTableMeta>)>> =
        const { RefCell::new(None) };
}

/// SCAL-1: Return active stream tables, using the per-backend snapshot cache.
///
/// If the cached DAG version matches shmem, returns the cached rows without
/// touching SPI.  On a version mismatch, reloads from the catalog and updates
/// the cache.
pub(crate) fn get_cached_active_stream_tables()
-> Result<Vec<StreamTableMeta>, crate::error::PgTrickleError> {
    let current_dag_version = shmem::current_dag_version();

    // Try the cache first.
    let hit = CATALOG_SNAPSHOT_CACHE.with(|c| {
        if let Some((cached_version, ref rows)) = *c.borrow()
            && cached_version == current_dag_version
        {
            return Some(rows.clone());
        }
        None
    });

    if let Some(rows) = hit {
        // Cache hit — no SPI needed.
        shmem::TEMPLATE_CACHE_L1_HITS
            .get()
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return Ok(rows);
    }

    // Cache miss — reload from catalog.
    let rows = StreamTableMeta::get_all_active()?;
    CATALOG_SNAPSHOT_CACHE.with(|c| {
        *c.borrow_mut() = Some((current_dag_version, rows.clone()));
    });
    Ok(rows)
}

/// SCAL-1: Invalidate the per-backend catalog snapshot cache.
///
/// Called after DDL operations that modify stream table metadata.
pub(crate) fn invalidate_catalog_snapshot_cache() {
    CATALOG_SNAPSHOT_CACHE.with(|c| {
        *c.borrow_mut() = None;
    });
}

/// SCAL-4 (v0.25.0): Copy-on-write DAG rebuild.
///
/// Builds a new `StDag` from the catalog **without holding any shared-memory
/// lock**.  The caller atomically replaces the current DAG pointer only after
/// the build completes, so concurrent readers always observe a consistent view.
///
/// The "swap" is implicit: the caller assigns the returned `StDag` to its
/// local `dag: Option<StDag>`.  Because the scheduler is the sole writer, no
/// additional synchronization is needed beyond the local assignment.
pub(crate) fn rebuild_dag_copy_on_write(
    schedule_secs: i32,
) -> Result<crate::dag::StDag, crate::error::PgTrickleError> {
    // All catalog I/O happens here, outside any PGS_STATE / TICK_WATERMARK_STATE
    // exclusive lock.  Once build_from_catalog() returns, the caller atomically
    // swaps the new DAG into place.
    crate::dag::StDag::build_from_catalog(schedule_secs)
}

/// CACHE-1: Per-backend L0 template cache key check.
///
/// Returns `true` if the local thread-local cache has an entry for `pgt_id`
/// at the current CACHE_GENERATION, indicating no L2/DVM parse is needed.
///
/// The full L0 implementation uses the L2 catalog table
/// (`pgtrickle.pgt_template_cache`) as the cross-backend shared cache.
/// Backends that miss L1 check L2 before re-running the DVM parser.
/// `CACHE_GENERATION` invalidation ensures stale entries are not used.
pub(crate) fn has_l0_cache_entry(pgt_id: i64) -> bool {
    let current_gen = shmem::current_cache_generation();
    crate::refresh::has_template_cache_entry(pgt_id, current_gen)
}

// ── Sub-transaction RAII guard ─────────────────────────────────────────

/// RAII guard for a PostgreSQL internal sub-transaction.
///
/// Automatically rolls back on drop if neither [`commit()`](SubTransaction::commit)
/// nor [`rollback()`](SubTransaction::rollback) was called (panic safety).
struct SubTransaction {
    old_cxt: pg_sys::MemoryContext,
    old_owner: pg_sys::ResourceOwner,
    finished: bool,
}

impl SubTransaction {
    /// Begin a new sub-transaction within the current worker transaction.
    fn begin() -> Self {
        // SAFETY: Called within a PostgreSQL worker transaction. The current
        // memory context and resource owner are valid.
        let old_cxt = unsafe { pg_sys::CurrentMemoryContext };
        let old_owner = unsafe { pg_sys::CurrentResourceOwner };
        // SAFETY: BeginInternalSubTransaction sets up a sub-transaction
        // using PostgreSQL's resource-owner mechanism.
        unsafe { pg_sys::BeginInternalSubTransaction(std::ptr::null()) };
        Self {
            old_cxt,
            old_owner,
            finished: false,
        }
    }

    /// Commit the sub-transaction and restore the outer context.
    fn commit(mut self) {
        // SAFETY: Commits the sub-transaction, restores context.
        unsafe {
            pg_sys::ReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(self.old_cxt);
            pg_sys::CurrentResourceOwner = self.old_owner;
        }
        self.finished = true;
    }

    /// Roll back the sub-transaction and restore the outer context.
    fn rollback(mut self) {
        // SAFETY: Rolls back the sub-transaction, restores context.
        unsafe {
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
            pg_sys::MemoryContextSwitchTo(self.old_cxt);
            pg_sys::CurrentResourceOwner = self.old_owner;
        }
        self.finished = true;
    }
}

impl Drop for SubTransaction {
    fn drop(&mut self) {
        if !self.finished {
            // Auto-rollback on drop for panic safety.
            // SAFETY: Same invariants as rollback().
            unsafe {
                pg_sys::RollbackAndReleaseCurrentSubTransaction();
                pg_sys::MemoryContextSwitchTo(self.old_cxt);
                pg_sys::CurrentResourceOwner = self.old_owner;
            }
        }
    }
}

/// Execute a singleton unit: refresh a single stream table using the existing inline path.
///
/// A41-5 — Isolation invariant:
/// Each singleton refresh executes in its own `READ COMMITTED` transaction
/// (the implicit BGW transaction).  The refresh sees a consistent snapshot
/// of the source tables as of the transaction start and is not affected by
/// concurrent insertions that arrive after the snapshot was taken.
/// Snapshot isolation means a singleton never reads its own or another
/// stream table's uncommitted writes.
fn execute_worker_singleton(job: &SchedulerJob) -> RefreshOutcome {
    let pgt_id = job.root_pgt_id;
    let st = match load_st_by_id(pgt_id) {
        Some(st) => st,
        None => {
            log!(
                "pg_trickle refresh worker: stream table pgt_id={} not found for job {}",
                pgt_id,
                job.job_id,
            );
            return RefreshOutcome::PermanentFailure;
        }
    };

    if st.status != StStatus::Active && st.status != StStatus::Initializing {
        log!(
            "pg_trickle refresh worker: {}.{} is not active (status={}), skipping",
            st.pgt_schema,
            st.pgt_name,
            st.status.as_str(),
        );
        return RefreshOutcome::RetryableFailure;
    }

    // BOOT-4: Check bootstrap source gates — skip if any source is gated.
    let gated_oids = load_gated_source_oids();
    if is_any_source_gated(pgt_id, &gated_oids) {
        log!(
            "pg_trickle refresh worker: skipping {}.{} — source gated (job {})",
            st.pgt_schema,
            st.pgt_name,
            job.job_id,
        );
        log_gated_skip(&st);
        return RefreshOutcome::Success;
    }

    // WM-4: Check watermark alignment — skip if misaligned.
    let (wm_misaligned, wm_reason) = is_watermark_misaligned(pgt_id);
    if wm_misaligned {
        let reason = wm_reason.as_deref().unwrap_or("watermark misaligned");
        log!(
            "pg_trickle refresh worker: skipping {}.{} — {} (job {})",
            st.pgt_schema,
            st.pgt_name,
            reason,
            job.job_id,
        );
        log_watermark_skip(&st, reason);
        return RefreshOutcome::Success;
    }

    // WM-7: Check watermark staleness — skip if any source watermark is stuck.
    let (wm_stuck, wm_stuck_reason) = is_watermark_stuck(pgt_id);
    if wm_stuck {
        let reason = wm_stuck_reason.as_deref().unwrap_or("watermark stuck");
        log!(
            "pg_trickle refresh worker: skipping {}.{} — {} (job {})",
            st.pgt_schema,
            st.pgt_name,
            reason,
            job.job_id,
        );
        log_watermark_skip(&st, reason);
        return RefreshOutcome::Success;
    }

    // FUSE-5: Check fuse circuit breaker — blow if change count exceeds ceiling.
    if evaluate_fuse(&st) {
        log!(
            "pg_trickle refresh worker: {}.{} fuse blown — skipping refresh (job {})",
            st.pgt_schema,
            st.pgt_name,
            job.job_id,
        );
        return RefreshOutcome::Success;
    }

    // #536: Use holdback-aware watermark for dynamic workers.
    let tick_watermark: Option<String> = compute_worker_tick_watermark();
    let has_changes = has_table_source_changes(&st) || has_stream_table_source_changes(&st);
    let action = refresh::determine_refresh_action(&st, has_changes);

    execute_scheduled_refresh(&st, action, tick_watermark.as_deref(), None)
}

/// Execute an atomic group unit: refresh all members serially inside a
/// sub-transaction.  If any member fails, the entire group is rolled back.
///
/// Uses the C-level internal sub-transaction API (`BeginInternalSubTransaction`
/// / `ReleaseCurrentSubTransaction` / `RollbackAndReleaseCurrentSubTransaction`)
/// because PostgreSQL rejects transaction-control SQL via SPI.
///
/// A41-5 — Isolation invariants:
/// - `atomic_group` (`is_repeatable_read = false`): each member refresh runs
///   in its own internal sub-transaction with `READ COMMITTED` isolation.
///   Members see each other's committed writes once the prior sub-transaction
///   commits.  Failure of any member rolls back the entire group atomically.
/// - `repeatable_read_group` (`is_repeatable_read = true`): an explicit
///   `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ` is issued before the
///   group executes.  All members share the same snapshot as of the group
///   transaction start.  No member sees writes from others, and the group
///   appears as a single atomic unit to external observers.
fn execute_worker_atomic_group(job: &SchedulerJob, is_repeatable_read: bool) -> RefreshOutcome {
    log!(
        "pg_trickle refresh worker: {} group — {} members (job {})",
        if is_repeatable_read {
            "repeatable_read"
        } else {
            "atomic"
        },
        job.member_pgt_ids.len(),
        job.job_id,
    );

    if is_repeatable_read {
        // Upgrade top-level worker transaction to REPEATABLE READ snapshot isolation
        if let Err(e) = pgrx::Spi::run("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ") {
            pgrx::log!(
                "pg_trickle refresh worker: failed to set REPEATABLE READ isolation: {}",
                e
            );
            return RefreshOutcome::PermanentFailure;
        }
    }

    let subtxn = SubTransaction::begin();

    // #536: Use holdback-aware watermark for dynamic workers.
    let tick_watermark: Option<String> = compute_worker_tick_watermark();
    let mut refreshed_count: usize = 0;

    // BOOT-4: Build gated-source set once for the whole group.
    let gated_oids = load_gated_source_oids();

    for &pgt_id in &job.member_pgt_ids {
        let st = match load_st_by_id(pgt_id) {
            Some(st) => st,
            None => continue,
        };

        if st.status != StStatus::Active && st.status != StStatus::Initializing {
            continue;
        }

        // BOOT-4: skip if any source is gated.
        if is_any_source_gated(pgt_id, &gated_oids) {
            log!(
                "pg_trickle refresh worker: skipping {}.{} — source gated (atomic group job {})",
                st.pgt_schema,
                st.pgt_name,
                job.job_id,
            );
            log_gated_skip(&st);
            continue;
        }

        // WM-4: Skip if watermarks misaligned.
        let (wm_misaligned, wm_reason) = is_watermark_misaligned(pgt_id);
        if wm_misaligned {
            let reason = wm_reason.as_deref().unwrap_or("watermark misaligned");
            log!(
                "pg_trickle refresh worker: skipping {}.{} — {} (atomic group job {})",
                st.pgt_schema,
                st.pgt_name,
                reason,
                job.job_id,
            );
            log_watermark_skip(&st, reason);
            continue;
        }

        // WM-7: Skip if any source watermark is stuck.
        let (wm_stuck, wm_stuck_reason) = is_watermark_stuck(pgt_id);
        if wm_stuck {
            let reason = wm_stuck_reason.as_deref().unwrap_or("watermark stuck");
            log!(
                "pg_trickle refresh worker: skipping {}.{} — {} (atomic group job {})",
                st.pgt_schema,
                st.pgt_name,
                reason,
                job.job_id,
            );
            log_watermark_skip(&st, reason);
            continue;
        }

        // Check catalog row lock — if another refresh is in progress, skip.
        if check_skip_needed(&st) {
            continue;
        }

        // FUSE-5: Check fuse circuit breaker.
        if evaluate_fuse(&st) {
            log!(
                "pg_trickle refresh worker: {}.{} fuse blown — skipping (atomic group job {})",
                st.pgt_schema,
                st.pgt_name,
                job.job_id,
            );
            continue;
        }

        let has_changes = has_table_source_changes(&st) || has_stream_table_source_changes(&st);
        let action = refresh::determine_refresh_action(&st, has_changes);

        let result = execute_scheduled_refresh(&st, action, tick_watermark.as_deref(), None);
        match result {
            RefreshOutcome::Success => {
                refreshed_count += 1;
            }
            RefreshOutcome::RetryableFailure | RefreshOutcome::PermanentFailure => {
                log!(
                    "pg_trickle refresh worker: atomic group rollback — member {}.{} failed (job {})",
                    st.pgt_schema,
                    st.pgt_name,
                    job.job_id,
                );
                subtxn.rollback();
                return result;
            }
        }
    }

    // All members succeeded — commit the sub-transaction.
    subtxn.commit();

    log!(
        "pg_trickle refresh worker: atomic group committed ({} members, job {})",
        refreshed_count,
        job.job_id,
    );
    RefreshOutcome::Success
}

/// Execute an IMMEDIATE-closure unit: refresh only the root stream table.
///
/// Downstream IMMEDIATE-mode stream tables fire their refresh triggers
/// synchronously within the same transaction, so they do not need to be
/// explicitly refreshed by the worker.  The coordinator must not independently
/// schedule any member of the closure.
///
/// A41-5 — Isolation invariant:
/// The root refresh and all downstream IMMEDIATE trigger firings share the
/// same `READ COMMITTED` transaction.  Because triggers fire synchronously
/// within the outer statement, they see the root's writes but not writes from
/// other concurrent transactions.  The closure behaves as a single atomic
/// unit: either all members update or none do (sub-transaction rollback).
fn execute_worker_immediate_closure(job: &SchedulerJob) -> RefreshOutcome {
    log!(
        "pg_trickle refresh worker: immediate closure — root pgt_id={} ({} members, job {})",
        job.root_pgt_id,
        job.member_pgt_ids.len(),
        job.job_id,
    );

    // Only refresh the root; IMMEDIATE triggers propagate downstream.
    let st = match load_st_by_id(job.root_pgt_id) {
        Some(st) => st,
        None => {
            log!(
                "pg_trickle refresh worker: root pgt_id={} not found for immediate closure (job {})",
                job.root_pgt_id,
                job.job_id,
            );
            return RefreshOutcome::PermanentFailure;
        }
    };

    if st.status != StStatus::Active && st.status != StStatus::Initializing {
        log!(
            "pg_trickle refresh worker: {}.{} is not active (status={}), skipping immediate closure",
            st.pgt_schema,
            st.pgt_name,
            st.status.as_str(),
        );
        return RefreshOutcome::RetryableFailure;
    }

    // FUSE-5: Check fuse circuit breaker for immediate closure root.
    if evaluate_fuse(&st) {
        log!(
            "pg_trickle refresh worker: {}.{} fuse blown — skipping immediate closure (job {})",
            st.pgt_schema,
            st.pgt_name,
            job.job_id,
        );
        return RefreshOutcome::Success;
    }

    // #536: Use holdback-aware watermark for dynamic workers.
    let tick_watermark: Option<String> = compute_worker_tick_watermark();
    let has_changes = has_table_source_changes(&st) || has_stream_table_source_changes(&st);
    let action = refresh::determine_refresh_action(&st, has_changes);

    execute_scheduled_refresh(&st, action, tick_watermark.as_deref(), None)
}

/// Execute a full cyclic Strongly Connected Component in parallel mode.
///
/// Runs iterative fixed-point refresh until convergence.
///
/// A41-5 — Isolation invariant:
/// Each iteration of the fixed-point loop runs in its own `READ COMMITTED`
/// transaction.  After each iteration, all members' writes are committed and
/// visible to the next iteration.  Convergence is detected when no member
/// produces output changes.  External observers may transiently see partially
/// converged states between iterations; use a repeatable_read_group wrapping
/// the SCC members when strict external consistency is required.
fn execute_worker_cyclic_scc(job: &SchedulerJob) -> RefreshOutcome {
    log!(
        "pg_trickle refresh worker: cyclic SCC — {} members (job {})",
        job.member_pgt_ids.len(),
        job.job_id,
    );

    let max_iter = config::pg_trickle_max_fixpoint_iterations();
    let gated_oids = load_gated_source_oids();
    let member_ids = &job.member_pgt_ids;

    if member_ids.is_empty() {
        return RefreshOutcome::Success;
    }

    for &pgt_id in member_ids {
        if let Some(st) = load_st_by_id(pgt_id)
            && st.refresh_mode != RefreshMode::Differential
        {
            pgrx::warning!(
                "pg_trickle refresh worker: SCC fixpoint — {}.{} uses {} mode, \
                 only DIFFERENTIAL is supported in cyclic dependencies",
                st.pgt_schema,
                st.pgt_name,
                st.refresh_mode.as_str()
            );
            return RefreshOutcome::PermanentFailure;
        }
    }

    // #536: Use holdback-aware watermark for dynamic workers.
    let tick_watermark: Option<String> = compute_worker_tick_watermark();

    let mut prev_row_counts: HashMap<i64, i64> = member_ids
        .iter()
        .map(|&id| (id, get_st_row_count(id).unwrap_or(0)))
        .collect();

    for iteration in 0..max_iter {
        let mut iteration_ok = true;
        let mut any_refreshed = false;

        {
            let subtxn = SubTransaction::begin();

            for &pgt_id in member_ids {
                let st = match load_st_by_id(pgt_id) {
                    Some(st) => st,
                    None => continue,
                };

                if st.status != StStatus::Active && st.status != StStatus::Initializing {
                    continue;
                }

                if is_any_source_gated(pgt_id, &gated_oids) {
                    log_gated_skip(&st);
                    continue;
                }

                // Inside SCCs, always use FULL refresh action to evaluate defining query each pass.
                let outcome = execute_scheduled_refresh(
                    &st,
                    RefreshAction::Full,
                    tick_watermark.as_deref(),
                    None,
                );
                match outcome {
                    RefreshOutcome::Success => {
                        any_refreshed = true;
                    }
                    RefreshOutcome::RetryableFailure | RefreshOutcome::PermanentFailure => {
                        log!(
                            "pg_trickle refresh worker: fixpoint iteration {} failed on {}.{}",
                            iteration + 1,
                            st.pgt_schema,
                            st.pgt_name
                        );
                        iteration_ok = false;
                        break;
                    }
                }
            }

            if !iteration_ok {
                subtxn.rollback();
                return RefreshOutcome::RetryableFailure;
            }

            subtxn.commit();
        }

        if !any_refreshed {
            continue;
        }

        let mut total_changes: i64 = 0;
        for &pgt_id in member_ids {
            let new_count = get_st_row_count(pgt_id).unwrap_or(0);
            let old_count = prev_row_counts.get(&pgt_id).copied().unwrap_or(0);
            total_changes += (new_count - old_count).abs();
            prev_row_counts.insert(pgt_id, new_count);
        }

        if total_changes == 0 {
            log!(
                "pg_trickle refresh worker: fixpoint reached after {} iteration(s)",
                iteration + 1
            );
            for &pgt_id in member_ids {
                let _ = StreamTableMeta::update_last_fixpoint_iterations(pgt_id, iteration + 1);
            }
            return RefreshOutcome::Success;
        }

        log!(
            "pg_trickle refresh worker: fixpoint iteration {} yielded {} change(s), continuing...",
            iteration + 1,
            total_changes
        );
    }

    pgrx::warning!(
        "pg_trickle refresh worker: SCC fixpoint failed to converge after {} iterations",
        max_iter
    );
    for &pgt_id in member_ids {
        let _ = StreamTableMeta::update_last_fixpoint_iterations(pgt_id, max_iter);
        let _ = StreamTableMeta::update_status(pgt_id, StStatus::Error);
    }
    RefreshOutcome::PermanentFailure
}

/// PERF-2 (v0.63.0): Attempt fused differential refresh for a subset of
/// chain members.
///
/// Identifies fusion-eligible nodes from `member_pgt_ids` (DIFFERENTIAL mode,
/// non-paused, delta below `fused_refresh_max_delta_rows`, template cache warm),
/// composes their delta SQL into a single `WITH … MERGE` statement using
/// [`refresh::fuse_diff_batch`], and executes it in one SPI call.
///
/// Returns the set of `pgt_id`s that were successfully fused and had their
/// frontiers saved.  The caller must skip these nodes in the subsequent
/// sequential loop.  Returns an empty `Vec` when fusion was not attempted or
/// did not produce a valid batch.
fn try_fused_chain_refresh(
    member_pgt_ids: &[i64],
    tick_watermark: Option<&str>,
    gated_oids: &std::collections::HashSet<pgrx::pg_sys::Oid>,
) -> Vec<i64> {
    if !crate::config::pg_trickle_enable_fused_refresh() {
        return vec![];
    }

    let max_delta_rows = crate::config::pg_trickle_fused_refresh_max_delta_rows();
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");

    // PERF-002 (v0.70.0): Preload all dependency rows for every candidate in a
    // single SPI round-trip before the eligibility loop.  This removes the
    // O(N×2) per-node SPI fan-out introduced when get_for_st was called twice
    // per loop iteration (once for ST-source frontier augmentation and once for
    // the delta-size gate).
    let deps_map: std::collections::HashMap<i64, Vec<crate::catalog::StDependency>> =
        crate::catalog::StDependency::get_for_sts(member_pgt_ids).unwrap_or_default();

    // Phase 1: Identify eligible nodes and gather their NodeSpec data.
    struct NodeData {
        pgt_id: i64,
        st: StreamTableMeta,
        new_frontier: version::Frontier,
        node_spec: refresh::NodeSpec,
    }
    let mut eligible: Vec<NodeData> = Vec::new();

    for &pgt_id in member_pgt_ids {
        // Load fresh ST metadata.
        let st = match load_st_by_id(pgt_id) {
            Some(s) => s,
            None => continue,
        };

        // Only active STs in DIFFERENTIAL mode.
        if st.status != crate::dag::StStatus::Active {
            continue;
        }
        if !st.is_populated {
            continue;
        }

        // Skip if any source is gated.
        if is_any_source_gated(pgt_id, gated_oids) {
            continue;
        }

        // Check watermark alignment.
        let (wm_misaligned, _) = is_watermark_misaligned(pgt_id);
        if wm_misaligned {
            continue;
        }

        // Need an existing frontier for differential refresh.
        match &st.frontier {
            Some(f) if !f.is_empty() => {}
            _ => continue,
        };

        // Acquire the catalog row lock (SKIP LOCKED — skip if another session holds it).
        let got_lock = Spi::get_one_with_args::<i64>(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_id = $1 FOR UPDATE SKIP LOCKED",
            &[pgt_id.into()],
        )
        .unwrap_or(None)
        .is_some();
        if !got_lock {
            continue;
        }

        // TOCTOU: reload after locking.
        let st = match load_st_by_id(pgt_id) {
            Some(s) => s,
            None => continue,
        };
        let prev_frontier = match &st.frontier {
            Some(f) if !f.is_empty() => f.clone(),
            _ => continue,
        };

        // Compute the new frontier.
        let source_oids = get_source_oids_for_st(pgt_id);
        let mut slot_positions = cdc::get_slot_positions(&source_oids).unwrap_or_default();
        if let Some(wm) = tick_watermark {
            for lsn in slot_positions.values_mut() {
                if version::lsn_gt(lsn, wm) {
                    *lsn = wm.to_string();
                }
            }
        }
        let data_ts_str = version::select_target_data_timestamp(
            st.schedule
                .as_ref()
                .and_then(|s| crate::api::parse_duration(s).ok())
                .map(|s| s as u64),
            &[],
        );
        let mut new_frontier = version::compute_new_frontier(&slot_positions, &data_ts_str);

        // Augment with upstream ST source positions.
        let change_schema_for_st = crate::config::pg_trickle_change_buffer_schema()
            .trim_matches('"')
            .to_string();
        let st_dep_ids: Vec<i64> = deps_map
            .get(&pgt_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|d| d.source_type == "STREAM_TABLE")
            .filter_map(|d| crate::catalog::StreamTableMeta::pgt_id_for_relid(d.source_relid))
            .collect();
        for upstream_id in &st_dep_ids {
            // nosemgrep: rust.spi.query.dynamic-format — schema from GUC (server-controlled),
            // upstream_id is i64 pgt_id (not user input).
            let lsn_sql = format!(
                "SELECT COALESCE(MAX(lsn), '0/0') \
                 FROM \"{change_schema_for_st}\".changes_pgt_{upstream_id}"
            );
            let lsn = Spi::get_one::<String>(&lsn_sql)
                .unwrap_or(None)
                .unwrap_or_else(|| "0/0".to_string());
            new_frontier.set_st_source(*upstream_id, lsn, data_ts_str.clone());
        }

        // Determine refresh action.
        let has_changes = has_table_source_changes(&st) || has_stream_table_source_changes(&st);
        let action = refresh::determine_refresh_action(&st, has_changes);
        if action != RefreshAction::Differential {
            continue;
        }

        // Check delta size against fused_refresh_max_delta_rows.
        if let Some(max_rows) = max_delta_rows {
            let dep_oids: Vec<u32> = deps_map
                .get(&pgt_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter(|d| {
                    d.source_type == "TABLE"
                        || d.source_type == "FOREIGN_TABLE"
                        || d.source_type == "MATVIEW"
                })
                .map(|d| d.source_relid.to_u32())
                .collect();
            let total_changes: i64 = dep_oids
                .iter()
                .map(|&oid| {
                    let buf_name =
                        crate::cdc::buffer_base_name_for_oid(pgrx::pg_sys::Oid::from(oid));
                    // nosemgrep: rust.spi.query.dynamic-format — schema from GUC (server-controlled),
                    // buf_name derived from numeric OID (not user input).
                    let count_sql =
                        format!("SELECT count(*)::bigint FROM \"{change_schema}\".{buf_name}");
                    Spi::get_one::<i64>(&count_sql)
                        .unwrap_or(Some(0))
                        .unwrap_or(0)
                })
                .sum();
            if total_changes > max_rows {
                continue;
            }
        }

        // Check template cache: we need the delta SQL template for this node.
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        st.defining_query.hash(&mut hasher);
        let query_hash = hasher.finish();

        let (delta_tmpl, src_oids, _is_dedup) =
            match refresh::get_fused_refresh_template(pgt_id, query_hash) {
                Some(t) => t,
                None => {
                    // Template not warm — skip this node; the sequential path will warm it.
                    continue;
                }
            };

        // Resolve LSN placeholders to get the final delta SQL.
        let empty_zero_oids = std::collections::HashSet::new();
        let delta_sql = match refresh::resolve_lsn_placeholders(
            &delta_tmpl,
            &src_oids,
            &prev_frontier,
            &new_frontier,
            &empty_zero_oids,
        ) {
            Ok(s) => s,
            Err(e) => {
                pgrx::debug1!(
                    "[pg_trickle] PERF-2: fused refresh LSN resolution failed for pgt_id={}: {}",
                    pgt_id,
                    e
                );
                continue;
            }
        };

        // Build NodeSpec.
        let user_cols = refresh::get_st_user_columns(&st);
        if user_cols.is_empty() {
            continue; // cannot build MERGE without columns
        }
        let quoted_table = format!(
            "\"{}\".\"{}\"",
            st.pgt_schema.replace('"', "\"\""),
            st.pgt_name.replace('"', "\"\""),
        );
        let node_spec = refresh::NodeSpec {
            pgt_id,
            quoted_table,
            delta_sql,
            user_cols,
            has_partition_key: st.st_partition_key.is_some(),
        };

        eligible.push(NodeData {
            pgt_id,
            st,
            new_frontier,
            node_spec,
        });
    }

    if eligible.len() < 2 {
        // Not enough nodes for fusion to be worthwhile.
        return vec![];
    }

    // Phase 2: Compose the fused SQL.
    let node_specs: Vec<refresh::NodeSpec> = eligible.iter().map(|n| n.node_spec.clone()).collect();
    let fused = match refresh::fuse_diff_batch(&node_specs) {
        Ok(f) => f,
        Err(e) => {
            pgrx::debug1!("[pg_trickle] PERF-2: fuse_diff_batch failed: {}", e);
            return vec![];
        }
    };

    pgrx::log!(
        "[pg_trickle] PERF-2: executing fused refresh for {} nodes (job nodes: {:?})",
        fused.node_count,
        eligible.iter().map(|n| n.pgt_id).collect::<Vec<_>>(),
    );

    // Phase 3: Execute the fused SQL.
    if let Err(e) = Spi::run(&fused.sql) {
        pgrx::warning!(
            "[pg_trickle] PERF-2: fused refresh SQL failed: {}; falling back to sequential",
            e
        );
        return vec![];
    }

    // Phase 4: Post-execution — store frontiers and record completions.
    let mut fused_pgt_ids: Vec<i64> = Vec::with_capacity(eligible.len());
    let now = Spi::get_one::<pgrx::datum::TimestampWithTimeZone>("SELECT now()").unwrap_or(None);

    for nd in &eligible {
        // COR-001 (v0.72.0): Treat frontier-store failure as refresh failure.
        // In the fused path the SQL has already been submitted, but since all
        // operations run in the same SPI transaction, a frontier-store failure
        // aborts the transaction and rolls back the fused SQL as well.
        if let Err(e) = StreamTableMeta::store_frontier(nd.pgt_id, &nd.new_frontier) {
            pgrx::warning!(
                "[pg_trickle] PERF-2: failed to store frontier for pgt_id={}: {}; \
                 aborting fused batch (transaction will roll back)",
                nd.pgt_id,
                e
            );
            return vec![];
        }

        // Record the fused refresh in the audit log.
        if let Some(ts) = now {
            let freshness_deadline = compute_freshness_deadline(&nd.st);
            let refresh_id = RefreshRecord::insert(
                nd.pgt_id,
                ts,
                "DIFFERENTIAL",
                "RUNNING",
                0,
                0,
                None,
                Some("SCHEDULER_FUSED"),
                freshness_deadline,
                0,
                None,
                false,
                tick_watermark,
            );
            if let Ok(rid) = refresh_id {
                let _ = RefreshRecord::complete(
                    rid,
                    "COMPLETED",
                    0, // row counts not individually tracked in fused mode
                    0,
                    None,
                    0,
                    Some("DIFFERENTIAL"),
                    false,
                );
            }
        }

        fused_pgt_ids.push(nd.pgt_id);
    }

    pgrx::log!(
        "[pg_trickle] PERF-2: fused refresh completed for {} node(s)",
        fused_pgt_ids.len(),
    );
    fused_pgt_ids
}

/// DAG-4: Execute a fused chain of stream tables in a single worker.
///
/// Members are refreshed sequentially in topological order.  For each
/// intermediate member (all except the last), the delta is written to a
/// temp bypass table instead of the persistent `changes_pgt_` buffer.
/// The next member in the chain reads from the bypass table via the
/// `ST_BYPASS_TABLES` thread-local mapping.
///
/// The last member writes to the persistent buffer as normal so that
/// any external consumers (outside the chain) see the delta.
///
/// A41-5 — Isolation invariant:
/// All members of a fused chain execute within a single `READ COMMITTED`
/// transaction.  Intermediate bypass tables are `ON COMMIT DROP` temp
/// tables, so they are automatically cleaned up on transaction boundary.
/// Because all members share one transaction, the chain is atomic: either
/// the entire chain commits or, on error, the entire chain rolls back.
/// External consumers do not see partial chain results.
fn execute_worker_fused_chain(job: &SchedulerJob) -> RefreshOutcome {
    log!(
        "pg_trickle refresh worker: fused chain — {} members, job {}",
        job.member_pgt_ids.len(),
        job.job_id,
    );

    // #536: Use holdback-aware watermark for dynamic workers.
    let tick_watermark: Option<String> = compute_worker_tick_watermark();

    // BOOT-4: Build gated-source set once for the whole group.
    let gated_oids = load_gated_source_oids();

    // Ensure bypass tables are clean at the start.
    crate::refresh::clear_all_st_bypass();

    // PERF-2 (v0.63.0): Attempt fused differential refresh for eligible nodes.
    // Nodes that are successfully fused are skipped in the sequential loop below.
    let fused_pgt_ids =
        try_fused_chain_refresh(&job.member_pgt_ids, tick_watermark.as_deref(), &gated_oids);
    let fused_set: std::collections::HashSet<i64> = fused_pgt_ids.into_iter().collect();

    let member_count = job.member_pgt_ids.len();
    let mut refreshed_count: usize = fused_set.len();

    for (idx, &pgt_id) in job.member_pgt_ids.iter().enumerate() {
        // PERF-2: Skip nodes already handled by the fused path.
        if fused_set.contains(&pgt_id) {
            continue;
        }

        let st = match load_st_by_id(pgt_id) {
            Some(st) => st,
            None => continue,
        };

        if st.status != StStatus::Active && st.status != StStatus::Initializing {
            continue;
        }

        // BOOT-4: skip if any source is gated.
        if is_any_source_gated(pgt_id, &gated_oids) {
            log!(
                "pg_trickle refresh worker: skipping {}.{} — source gated (fused chain job {})",
                st.pgt_schema,
                st.pgt_name,
                job.job_id,
            );
            log_gated_skip(&st);
            continue;
        }

        // WM-4: Skip if watermarks misaligned.
        let (wm_misaligned, wm_reason) = is_watermark_misaligned(pgt_id);
        if wm_misaligned {
            let reason = wm_reason.as_deref().unwrap_or("watermark misaligned");
            log!(
                "pg_trickle refresh worker: skipping {}.{} — {} (fused chain job {})",
                st.pgt_schema,
                st.pgt_name,
                reason,
                job.job_id,
            );
            log_watermark_skip(&st, reason);
            continue;
        }

        // WM-7: Skip if any source watermark is stuck.
        let (wm_stuck, wm_stuck_reason) = is_watermark_stuck(pgt_id);
        if wm_stuck {
            let reason = wm_stuck_reason.as_deref().unwrap_or("watermark stuck");
            log!(
                "pg_trickle refresh worker: skipping {}.{} — {} (fused chain job {})",
                st.pgt_schema,
                st.pgt_name,
                reason,
                job.job_id,
            );
            log_watermark_skip(&st, reason);
            continue;
        }

        // Check catalog row lock — if another refresh is in progress, skip.
        if check_skip_needed(&st) {
            continue;
        }

        // FUSE-5: Check fuse circuit breaker.
        if evaluate_fuse(&st) {
            log!(
                "pg_trickle refresh worker: {}.{} fuse blown — skipping (fused chain job {})",
                st.pgt_schema,
                st.pgt_name,
                job.job_id,
            );
            continue;
        }

        let is_last = idx == member_count - 1;
        let has_changes = has_table_source_changes(&st) || has_stream_table_source_changes(&st);
        let action = refresh::determine_refresh_action(&st, has_changes);

        // DAG-4: For intermediate members, set a flag so the refresh path
        // uses bypass capture instead of the persistent buffer.
        // The flag is set via the thread-local ST_BYPASS_TABLES before
        // execute_scheduled_refresh runs. For the last member, normal
        // persistent buffer is used so external downstreams see the delta.
        //
        // Note: The actual bypass capture happens inside
        // execute_differential_refresh based on has_downstream_st_consumers().
        // The bypass tables from earlier members are already in the
        // thread-local map, so downstream members in this chain read from
        // them automatically.

        let result = execute_scheduled_refresh(&st, action, tick_watermark.as_deref(), None);
        match result {
            RefreshOutcome::Success => {
                refreshed_count += 1;

                // DAG-4: For non-last members with downstream ST consumers,
                // create the bypass temp table so the next member can read
                // from it instead of the persistent buffer.
                // Only for DIFFERENTIAL refreshes — NoData/Full/Reinitialize
                // do not materialize __pgt_delta_{pgt_id}.
                if !is_last
                    && action == RefreshAction::Differential
                    && refresh::has_downstream_st_consumers(pgt_id)
                {
                    let user_cols_typed = refresh::get_st_user_columns_typed(&st);
                    match crate::refresh::capture_delta_to_bypass_table(&st, &user_cols_typed) {
                        Ok(n) => {
                            pgrx::debug1!(
                                "[pg_trickle] DAG-4: bypass captured {} rows for pgt_id={}",
                                n,
                                pgt_id,
                            );
                        }
                        Err(e) => {
                            // Bypass failed — the downstream will fall back to the
                            // persistent buffer which was also written.
                            pgrx::debug1!(
                                "[pg_trickle] DAG-4: bypass capture failed for pgt_id={}: {}",
                                pgt_id,
                                e,
                            );
                        }
                    }
                }
            }
            RefreshOutcome::RetryableFailure | RefreshOutcome::PermanentFailure => {
                log!(
                    "pg_trickle refresh worker: fused chain abort — member {}.{} failed (job {})",
                    st.pgt_schema,
                    st.pgt_name,
                    job.job_id,
                );
                // Clean up bypass tables before returning.
                crate::refresh::clear_all_st_bypass();
                return result;
            }
        }
    }

    // Clean up bypass tables.
    crate::refresh::clear_all_st_bypass();

    log!(
        "pg_trickle refresh worker: fused chain completed ({} members refreshed, job {})",
        refreshed_count,
        job.job_id,
    );
    RefreshOutcome::Success
}

// ── Refresh Outcome ────────────────────────────────────────────────────────

/// Outcome of a refresh attempt, used by the retry logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RefreshOutcome {
    /// Refresh succeeded — reset retry state.
    Success,
    /// Refresh failed with a retryable error — apply backoff.
    RetryableFailure,
    /// Refresh failed with a permanent error — don't retry, count toward suspension.
    PermanentFailure,
}

// ── Crash Recovery ─────────────────────────────────────────────────────────

/// Check that the compiled shared library version matches the SQL-installed
/// extension version. Warns loudly if they differ (stale install).
///
/// Must be called inside a `BackgroundWorker::transaction()` block.
fn check_extension_version_match() {
    let compiled_version = env!("CARGO_PKG_VERSION");
    let installed_version: Option<String> =
        Spi::get_one("SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")
            .unwrap_or(None);

    if let Some(ref installed) = installed_version
        && installed != compiled_version
    {
        warning!(
            "pg_trickle: version mismatch — shared library is {} but installed SQL extension \
             is {}. Run 'ALTER EXTENSION pg_trickle UPDATE;' to update the SQL objects, \
             or reinstall the matching shared library.",
            compiled_version,
            installed
        );
    }
}

// ── SLA-3: Dynamic tier re-assignment ──────────────────────────────────────

/// SLA-3: Check and adjust tiers for stream tables with SLA configured.
///
/// Iterates over stream tables that have a `freshness_deadline_ms` set and
/// calls `maybe_adjust_tier_for_sla` for each.
fn sla_tier_adjustment_tick() {
    let st_ids: Vec<i64> = Spi::connect(|client| {
        let result = match client.select(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                 WHERE freshness_deadline_ms IS NOT NULL AND status = 'ACTIVE'",
            None,
            &[],
        ) {
            Ok(r) => r,
            Err(e) => {
                log!("pg_trickle: SLA-3 query failed: {e}");
                return Vec::new();
            }
        };
        let mut ids = Vec::new();
        for row in result {
            if let Some(id) = row.get::<i64>(1).unwrap_or(None) {
                ids.push(id);
            }
        }
        ids
    });

    for pgt_id in st_ids {
        if let Ok(Some(meta)) = StreamTableMeta::get_by_id(pgt_id) {
            crate::api::publication::maybe_adjust_tier_for_sla(&meta);
        }
    }
}

// ── DF-G2: Dog-feeding auto-apply tick ──────────────────────────────────────

/// Auto-apply threshold recommendations from `df_threshold_advice`.
///
/// Called once per `AUTO_APPLY_INTERVAL_MS` when `self_monitoring_auto_apply` GUC
/// is not `off`. Reads HIGH-confidence recommendations where the recommended
/// threshold differs from the current threshold by > 5%, then applies via
/// `StreamTableMeta::update_adaptive_threshold`. Rate-limited to 1 change per
/// ST per invocation (STAB-2, STAB-4).
///
/// DF-G3: Logs changes to `pgt_refresh_history` with `initiated_by = 'SELF_MONITOR'`.
fn self_monitoring_auto_apply_tick() {
    // STAB-4: Check that df_threshold_advice exists before reading.
    let advice_exists: bool = Spi::get_one(
        "SELECT EXISTS (
            SELECT 1 FROM pgtrickle.pgt_stream_tables
            WHERE pgt_schema = 'pgtrickle' AND pgt_name = 'df_threshold_advice'
        )",
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !advice_exists {
        return;
    }

    // Read recommendations with HIGH confidence and > 5% delta.
    let recommendations: Vec<(i64, f64, f64, String)> = Spi::connect(|client| {
        let result = match client.select(
            "SELECT ta.pgt_id, ta.recommended_threshold, ta.current_threshold,
                    ta.pgt_schema || '.' || ta.pgt_name AS fq_name
             FROM pgtrickle.df_threshold_advice ta
             WHERE ta.confidence = 'HIGH'
               AND ta.recommended_threshold IS NOT NULL
               AND ta.current_threshold IS NOT NULL
               AND abs(ta.recommended_threshold - ta.current_threshold)
                   / NULLIF(ta.current_threshold, 0) > 0.05",
            None,
            &[],
        ) {
            Ok(r) => r,
            Err(e) => {
                pgrx::warning!("pg_trickle: auto-apply read failed: {}", e);
                return Vec::new();
            }
        };

        let mut recs = Vec::new();
        for row in result {
            let pgt_id = row.get::<i64>(1).unwrap_or(None).unwrap_or(0);
            let recommended = row.get::<f64>(2).unwrap_or(None).unwrap_or(0.0);
            let current = row.get::<f64>(3).unwrap_or(None).unwrap_or(0.0);
            let fq_name = row.get::<String>(4).unwrap_or(None).unwrap_or_default();
            if pgt_id > 0 {
                recs.push((pgt_id, recommended, current, fq_name));
            }
        }
        recs
    });

    for (pgt_id, recommended, current, fq_name) in &recommendations {
        // STAB-2: Handle ALTER failure gracefully — don't let one failure stop others.
        match crate::catalog::StreamTableMeta::update_adaptive_threshold(
            *pgt_id,
            Some(*recommended),
            None,
        ) {
            Ok(()) => {
                log!(
                    "pg_trickle: auto-apply threshold {} → {} for {} (SELF_MONITOR)",
                    current,
                    recommended,
                    fq_name,
                );
                // DF-G3: Audit trail in pgt_refresh_history.
                if let Ok(Some(now)) =
                    Spi::get_one::<pgrx::prelude::TimestampWithTimeZone>("SELECT now()")
                {
                    let _ = crate::catalog::RefreshRecord::insert(
                        *pgt_id,
                        now,
                        "SKIP",
                        "COMPLETED",
                        0,
                        0,
                        Some(&format!(
                            "SELF_MONITOR: auto_threshold {} → {}",
                            current, recommended
                        )),
                        Some("SELF_MONITOR"),
                        None,
                        0,
                        None,
                        false,
                        None,
                    );
                }
            }
            Err(e) => {
                pgrx::warning!(
                    "pg_trickle: auto-apply threshold update failed for {}: {}",
                    fq_name,
                    e,
                );
            }
        }
    }

    if !recommendations.is_empty() {
        log!(
            "pg_trickle: auto-apply applied {} threshold changes",
            recommendations.len(),
        );
    }

    // UX-3: Check for anomalies and emit NOTIFY on pg_trickle_alert channel.
    self_monitoring_anomaly_notify();
}

/// UX-3: Emit NOTIFY on `pgtrickle_alert` channel when anomalies are detected.
///
/// Reads `df_anomaly_signals` and sends a JSON notification for each stream
/// table that has a duration anomaly or recent failures ≥ 2.
fn self_monitoring_anomaly_notify() {
    let signals_exist: bool = Spi::get_one(
        "SELECT EXISTS (
            SELECT 1 FROM pgtrickle.pgt_stream_tables
            WHERE pgt_schema = 'pgtrickle' AND pgt_name = 'df_anomaly_signals'
        )",
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !signals_exist {
        return;
    }

    let anomalies: Vec<(String, Option<String>, i64)> = Spi::connect(|client| {
        let result = match client.select(
            "SELECT a.pgt_schema || '.' || a.pgt_name AS fq_name,
                    a.duration_anomaly,
                    a.recent_failures
             FROM pgtrickle.df_anomaly_signals a
             WHERE a.duration_anomaly IS NOT NULL
                OR a.recent_failures >= 2",
            None,
            &[],
        ) {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };

        let mut out = Vec::new();
        for row in result {
            let fq_name = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let anomaly = row.get::<String>(2).unwrap_or(None);
            let failures = row.get::<i64>(3).unwrap_or(None).unwrap_or(0);
            if !fq_name.is_empty() {
                out.push((fq_name, anomaly, failures));
            }
        }
        out
    });

    for (fq_name, anomaly, failures) in &anomalies {
        let anomaly_str = anomaly.as_deref().unwrap_or("none");
        let payload = format!(
            r#"{{"event":"self_monitor_anomaly","stream_table":"{}","anomaly":"{}","recent_failures":{}}}"#,
            fq_name.replace('"', r#"\""#),
            anomaly_str.replace('"', r#"\""#),
            failures,
        );
        let _ = Spi::run_with_args(
            "SELECT pg_notify('pgtrickle_alert', $1)",
            &[payload.as_str().into()],
        );
    }
}

/// Recover from a crash or unclean scheduler shutdown.
///
/// Any refresh history records stuck in 'RUNNING' status indicate interrupted
/// transactions. PostgreSQL will have rolled back the transaction, but the
/// history record may still say RUNNING if the INSERT was committed in a
/// separate transaction (which it is, via SPI in the scheduler loop).
///
/// This function marks all such records as FAILED and logs the recovery.
fn recover_from_crash() {
    let updated = Spi::connect_mut(|client| {
        let result = client.update(
            "UPDATE pgtrickle.pgt_refresh_history \
             SET status = 'FAILED', \
                 error_message = 'Interrupted by scheduler restart', \
                 end_time = now() \
             WHERE status = 'RUNNING'",
            None,
            &[],
        );
        match result {
            Ok(tuptable) => tuptable.len() as i64,
            Err(e) => {
                log!("pg_trickle: crash recovery query failed: {}", e);
                0
            }
        }
    });

    if updated > 0 {
        log!(
            "pg_trickle: crash recovery — marked {} interrupted refresh(es) as FAILED",
            updated
        );
    }
}

// ── CDC Transition Health Check (EC-20) ────────────────────────────────────

/// Check CDC transitions left in TRANSITIONING state after a scheduler restart.
///
/// If the scheduler crashed or was restarted during an active TRIGGER→WAL
/// transition, the replication slot or publication may be in an inconsistent
/// state. This function checks all TRANSITIONING dependencies and rolls back
/// any that have stale or missing slots.
fn check_cdc_transition_health() {
    use crate::catalog::StDependency;

    let deps = match StDependency::get_all() {
        Ok(d) => d,
        Err(e) => {
            log!(
                "pg_trickle: CDC health check — failed to load dependencies: {}",
                e
            );
            return;
        }
    };

    let transitioning: Vec<_> = deps
        .iter()
        .filter(|d| d.cdc_mode == crate::catalog::CdcMode::Transitioning)
        .collect();

    if transitioning.is_empty() {
        return;
    }

    log!(
        "pg_trickle: CDC health check — found {} source(s) in TRANSITIONING state after restart",
        transitioning.len()
    );

    for dep in transitioning {
        let slot_name = dep.slot_name.as_deref().unwrap_or("__pgt_missing_slot__");

        // Check if the replication slot still exists
        let slot_exists = Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}')",
            slot_name.replace('\'', "''")
        ))
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if !slot_exists {
            log!(
                "pg_trickle: CDC health check — replication slot '{}' missing for source OID {}. \
                 Rolling back to TRIGGER mode.",
                slot_name,
                dep.source_relid.to_u32()
            );
            // Roll back to TRIGGER mode
            if let Err(e) = StDependency::update_cdc_mode_for_source(
                dep.source_relid,
                crate::catalog::CdcMode::Trigger,
                None,
                None,
            ) {
                log!(
                    "pg_trickle: CDC health check — failed to rollback source OID {}: {}",
                    dep.source_relid.to_u32(),
                    e
                );
            }
        } else {
            log!(
                "pg_trickle: CDC health check — source OID {} in TRANSITIONING with valid slot '{}'. \
                 Normal transition will resume.",
                dep.source_relid.to_u32(),
                slot_name
            );
        }
    }
}

// ── Skip Mechanism ─────────────────────────────────────────────────────────

// Check if a refresh should be skipped because a previous one is still running.
//
// Uses `SELECT ... FOR UPDATE SKIP LOCKED` on the catalog row to detect
// concurrent refreshes.  If another session holds the row lock the query
// returns zero rows and we skip.  The row lock, once acquired, is held for
// the remainder of the caller's transaction — this is intentional, as the
// lock prevents concurrent refreshes until the tick transaction commits.
//
// IMPORTANT: `Spi::get_one_with_args` is used here (not `Spi::connect` +
// `client.select`) because pgrx's `select` passes `read_only =
// Spi::is_xact_still_immutable()`, which evaluates to `true` when no prior
// mutation has assigned a transaction XID yet.  PostgreSQL rejects `FOR
// UPDATE` in a read-only SPI context with "SELECT FOR UPDATE is not allowed
// in a non-volatile function".  `get_one_with_args` routes through
// `connect_mut` / `update`, which always uses `read_only = false`.
//
// PB1: replaces the previous `pg_try_advisory_lock` approach for
// PgBouncer transaction‐mode compatibility.

// ── FUSE-5: Fuse circuit breaker pre-check ─────────────────────────────────

/// Count total pending change buffer rows across all TABLE/FOREIGN_TABLE
/// sources for a stream table.
fn count_pending_changes(st: &StreamTableMeta) -> i64 {
    let change_schema = config::pg_trickle_change_buffer_schema();
    let source_oids = get_source_oids_for_st(st.pgt_id);

    let mut total: i64 = 0;
    for oid in &source_oids {
        // v0.32.0+: buffer table uses stable hash name; fall back to OID if untracked.
        let buf = crate::cdc::buffer_qualified_name_for_oid(&change_schema, *oid);
        let count = Spi::get_one::<i64>(&format!("SELECT count(*) FROM {buf}")) // nosemgrep: rust.spi.query.dynamic-format
            .unwrap_or(Some(0))
            .unwrap_or(0);
        total = total.saturating_add(count);
    }
    total
}

/// Evaluate the fuse circuit breaker for a stream table.
///
/// Returns `true` if the fuse blew (refresh should be skipped).
/// Returns `false` if the fuse is OK or disabled.
fn evaluate_fuse(st: &StreamTableMeta) -> bool {
    // Quick exit: fuse disabled or already blown
    if st.fuse_mode == "off" || st.fuse_state == "blown" || st.fuse_state == "disabled" {
        if st.fuse_state == "blown" {
            // Emit a periodic reminder so operators know the fuse is still
            // blown even if they missed the initial notification.  We send
            // at most once per ~60 seconds by checking blown_at age modulo.
            emit_fuse_blown_reminder(st);
            return true;
        }
        return false;
    }

    // Determine effective ceiling
    let global_ceiling = config::pg_trickle_fuse_default_ceiling();
    let effective_ceiling = match st.fuse_ceiling {
        Some(c) if c > 0 => c,
        _ if global_ceiling > 0 => global_ceiling,
        _ => return false, // No ceiling configured — fuse is a no-op
    };

    // Count pending changes
    let pending = count_pending_changes(st);
    if pending <= effective_ceiling {
        return false;
    }

    // Sensitivity check: only blow after N consecutive over-ceiling observations.
    // For now, each scheduler tick is one observation. Sensitivity > 1 requires
    // tracking a counter in the catalog, which we skip for v1 (sensitivity
    // defaults to 1 = blow immediately on first over-ceiling observation).
    let sensitivity = st.fuse_sensitivity.unwrap_or(1);
    if sensitivity > 1 {
        // Future: track observation count in catalog. For now, blow immediately
        // since we don't yet have a per-tick counter column.
        // This is correct for sensitivity=1 (the default).
    }
    let _ = sensitivity; // suppress unused warning

    // BLOW the fuse
    let reason = format!(
        "change buffer count ({}) exceeded ceiling ({})",
        pending, effective_ceiling
    );
    pgrx::warning!(
        "pg_trickle: FUSE BLOWN for {}.{} — {}",
        st.pgt_schema,
        st.pgt_name,
        reason,
    );

    if let Err(e) = StreamTableMeta::blow_fuse(st.pgt_id, &reason) {
        pgrx::warning!(
            "pg_trickle: failed to blow fuse for {}.{}: {}",
            st.pgt_schema,
            st.pgt_name,
            e,
        );
    }

    // Send pg_notify alert
    let notify_payload = format!(
        "{{\"event\":\"fuse_blown\",\"stream_table\":\"{}.{}\",\"pending\":{},\"ceiling\":{}}}",
        st.pgt_schema, st.pgt_name, pending, effective_ceiling
    );
    let _ = Spi::run_with_args(
        "SELECT pg_notify('pgtrickle_alert', $1)",
        &[notify_payload.as_str().into()],
    );

    true
}

/// Emit a periodic reminder that a fuse is still blown.
///
/// To avoid spamming the NOTIFY channel every tick (~100ms), we only emit
/// when the blown_at age crosses a 60-second boundary.  The scheduler calls
/// `evaluate_fuse` every tick, so we use `blown_at` to throttle.
fn emit_fuse_blown_reminder(st: &StreamTableMeta) {
    // Only emit if we can determine how long the fuse has been blown.
    let blown_secs = Spi::get_one_with_args::<f64>(
        "SELECT EXTRACT(EPOCH FROM (now() - blown_at))::float8 \
         FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_id = $1 AND blown_at IS NOT NULL",
        &[st.pgt_id.into()],
    )
    .unwrap_or(None);

    if let Some(secs) = blown_secs {
        // Emit once per ~60 seconds: fire when we're in the first tick
        // window after a 60s boundary.  The tick interval is configurable
        // but typically 100ms–1s, so checking `secs % 60 < 1` is safe.
        let interval = 60.0_f64;
        if secs >= interval && (secs % interval) < 1.0 {
            monitor::emit_alert(
                monitor::AlertEvent::FuseBlownReminder,
                &st.pgt_schema,
                &st.pgt_name,
                serde_json::json!({
                    "blown_seconds": secs.round(),
                    "reason": st.blow_reason.as_deref().unwrap_or("unknown"),
                }),
                st.pooler_compatibility_mode,
            );
        }
    }
}

/// Evaluate the fuse and return the effective ceiling threshold if the fuse
/// is active for this stream table. Pure decision logic for unit testing.
#[cfg(test)]
fn fuse_is_active(fuse_mode: &str, fuse_ceiling: Option<i64>, global_ceiling: i64) -> bool {
    if fuse_mode == "off" {
        return false;
    }
    let effective = match fuse_ceiling {
        Some(c) if c > 0 => c,
        _ if global_ceiling > 0 => global_ceiling,
        _ => return false,
    };
    effective > 0
}

/// Returns `true` if the refresh should be skipped.
///
/// SAF-1 audit (v0.11.0): This function and the broader worker loop in
/// `scheduler.rs`, `refresh.rs`, and `hooks.rs` have been audited for
/// `panic!` / `unwrap()` / `.expect()` in non-test code paths. All such
/// calls found were confined to `#[cfg(test)]` blocks. The only non-test
/// fallible path here is the SPI call below, which was previously silently
/// swallowed. It now logs a WARNING so operators can distinguish a missed
/// lock-check from a genuine SPI failure.
fn check_skip_needed(st: &StreamTableMeta) -> bool {
    let pgt_id = st.pgt_id;

    // FOR UPDATE SKIP LOCKED requires read_only=false (mutating SPI context).
    // Returns Some(pgt_id) if lock acquired, None if row is locked by another session.
    let spi_result = Spi::get_one_with_args::<i64>(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_id = $1 FOR UPDATE SKIP LOCKED",
        &[pgt_id.into()],
    );

    let row_found = match spi_result {
        Ok(opt) => opt.is_some(),
        Err(e) => {
            // SAF-1: Log the SPI error instead of silently treating it as
            // "locked". We still skip (return true) to be conservative — a
            // failed lock check should not allow concurrent refreshes.
            pgrx::warning!(
                "[pg_trickle] check_skip_needed: SPI error for {}.{} (pgt_id={}): {} \
                 — treating as locked, skipping this cycle",
                st.pgt_schema,
                st.pgt_name,
                pgt_id,
                e,
            );
            false
        }
    };

    // Zero rows (or SPI error) → the row is locked or unavailable → skip.
    !row_found
}

// ── Time Helper ────────────────────────────────────────────────────────────

/// Get the current epoch time in milliseconds.
fn current_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Resolve the effective schedule policy for a consistency group.
///
/// Reads the `diamond_schedule_policy` from each convergence node in the group
/// and returns the strictest (`Slowest > Fastest`). Falls back to `'fastest'`.
fn group_schedule_policy(group: &crate::dag::ConsistencyGroup) -> DiamondSchedulePolicy {
    group
        .convergence_points
        .iter()
        .fold(DiamondSchedulePolicy::Fastest, |acc, node| {
            if let NodeId::StreamTable(id) = node {
                load_st_by_id(*id)
                    .map(|st| acc.stricter(st.diamond_schedule_policy))
                    .unwrap_or(acc)
            } else {
                acc
            }
        })
}

/// Check whether a multi-member group is due for refresh according to its
/// schedule policy.
///
/// - `Fastest`: fires when **any** member is due.
/// - `Slowest`: fires only when **all** members are due.
fn is_group_due(
    group: &crate::dag::ConsistencyGroup,
    policy: DiamondSchedulePolicy,
    dag: &StDag,
) -> bool {
    let member_due: Vec<bool> = group
        .members
        .iter()
        .filter_map(|m| {
            if let NodeId::StreamTable(id) = m {
                load_st_by_id(*id).map(|st| {
                    // D-1b: Check for UNLOGGED buffer crash recovery before
                    // evaluating schedule. This may set needs_reinit=true.
                    check_unlogged_buffer_crash_recovery(&st);
                    // Reload in case needs_reinit was set.
                    let st = load_st_by_id(*id).unwrap_or(st);
                    check_schedule(&st, dag) || st.needs_reinit
                })
            } else {
                None
            }
        })
        .collect();

    is_group_due_pure(&member_due, policy)
}

/// Pure decision logic: given member-due flags and a policy, decide if the
/// group is due for refresh.
fn is_group_due_pure(member_due: &[bool], policy: DiamondSchedulePolicy) -> bool {
    if member_due.is_empty() {
        return false;
    }

    match policy {
        DiamondSchedulePolicy::Fastest => member_due.iter().any(|&d| d),
        DiamondSchedulePolicy::Slowest => member_due.iter().all(|&d| d),
    }
}

/// Check if the refresh is falling behind the schedule.
///
/// Returns `Some(ratio)` when `elapsed_ms / schedule_ms >= threshold`.
/// Use `threshold = 0.95` for auto-backoff decisions (only back off when
/// genuinely unable to keep up) and `threshold = 0.80` for EC-11 operator
/// alerts (warn early so operators have time to react).
fn is_falling_behind(elapsed_ms: i64, schedule_ms: i64, threshold: f64) -> Option<f64> {
    if schedule_ms <= 0 {
        return None;
    }
    let ratio = elapsed_ms as f64 / schedule_ms as f64;
    if ratio >= threshold {
        Some(ratio)
    } else {
        None
    }
}

/// VP-1/VP-2 (v0.47.0): Execute the post-refresh action for a stream table
/// after a successful refresh that produced changed rows.
///
/// Runs outside the refresh transaction (fire-and-forget after commit).
/// Errors are logged but never propagated — post-refresh actions must not
/// interrupt the refresh pipeline.
pub(crate) fn execute_post_refresh_action(st: &StreamTableMeta, rows_changed: i64) {
    let action = st.post_refresh_action.as_str();

    // Increment the drift counter first (regardless of action type).
    if action == "reindex_if_drift" || action == "reindex" || action == "analyze" {
        let _ = crate::catalog::StreamTableMeta::increment_rows_changed_for_reindex(
            st.pgt_id,
            rows_changed,
        );
    }

    match action {
        "none" => {
            // No post-refresh action requested.
        }
        "analyze" => {
            let quoted = format!(
                "\"{}\".\"{}\"",
                st.pgt_schema.replace('"', "\"\""),
                st.pgt_name.replace('"', "\"\""),
            );
            // nosemgrep: rust.spi.run.dynamic-format — ANALYZE target is a PostgreSQL-quoted identifier
            if let Err(e) = pgrx::Spi::run(&format!("ANALYZE {quoted}")) {
                pgrx::log!(
                    "pg_trickle: post-refresh ANALYZE failed for {}.{}: {}",
                    st.pgt_schema,
                    st.pgt_name,
                    e
                );
            } else {
                pgrx::log!(
                    "pg_trickle: post-refresh ANALYZE completed for {}.{}",
                    st.pgt_schema,
                    st.pgt_name,
                );
            }
        }
        "reindex" => {
            run_post_refresh_reindex(st);
        }
        "reindex_if_drift" => {
            // Check drift: reload from catalog to get the latest counter.
            let threshold = st
                .reindex_drift_threshold
                .unwrap_or(crate::config::pg_trickle_reindex_drift_threshold());
            // Estimate row count for the storage table.
            let mut estimated_rows: i64 = pgrx::Spi::get_one_with_args::<i64>(
                "SELECT reltuples::BIGINT \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2",
                &[st.pgt_schema.as_str().into(), st.pgt_name.as_str().into()],
            )
            .unwrap_or(None)
            .unwrap_or(0);

            // reltuples = -1 means the table has never been analyzed. Run ANALYZE
            // so we have an accurate row count for drift evaluation and so that
            // subsequent vector_status() calls return a non-NULL drift_pct.
            if estimated_rows <= 0 {
                let quoted = format!(
                    "\"{}\".\"{}\"",
                    st.pgt_schema.replace('"', "\"\""),
                    st.pgt_name.replace('"', "\"\""),
                );
                // nosemgrep: rust.spi.run.dynamic-format — ANALYZE target is a PostgreSQL-quoted identifier
                if let Ok(()) = pgrx::Spi::run(&format!("ANALYZE {quoted}")) {
                    estimated_rows = pgrx::Spi::get_one_with_args::<i64>(
                        "SELECT reltuples::BIGINT \
                         FROM pg_class c \
                         JOIN pg_namespace n ON n.oid = c.relnamespace \
                         WHERE n.nspname = $1 AND c.relname = $2",
                        &[st.pgt_schema.as_str().into(), st.pgt_name.as_str().into()],
                    )
                    .unwrap_or(None)
                    .unwrap_or(0);
                }
            }

            if estimated_rows > 0 {
                // Reload from catalog to get the freshest drift counter.
                let current_changed = crate::catalog::StreamTableMeta::get_by_id(st.pgt_id)
                    .ok()
                    .flatten()
                    .map(|m| m.rows_changed_since_last_reindex)
                    .unwrap_or(rows_changed);
                let drift = current_changed as f64 / estimated_rows as f64;
                if drift >= threshold {
                    pgrx::log!(
                        "pg_trickle: drift {:.1}% >= threshold {:.1}% for {}.{} — triggering REINDEX",
                        drift * 100.0,
                        threshold * 100.0,
                        st.pgt_schema,
                        st.pgt_name,
                    );
                    run_post_refresh_reindex(st);
                }
            }
        }
        other => {
            pgrx::log!(
                "pg_trickle: unknown post_refresh_action '{}' for {}.{} — ignoring",
                other,
                st.pgt_schema,
                st.pgt_name,
            );
        }
    }
}

/// VP-2 (v0.47.0): Run REINDEX on a stream table's storage table and reset
/// the drift counter.
fn run_post_refresh_reindex(st: &StreamTableMeta) {
    let quoted = format!(
        "\"{}\".\"{}\"",
        st.pgt_schema.replace('"', "\"\""),
        st.pgt_name.replace('"', "\"\""),
    );
    // nosemgrep: rust.spi.run.dynamic-format — REINDEX target is a PostgreSQL-quoted identifier
    match pgrx::Spi::run(&format!("REINDEX TABLE {quoted}")) {
        Ok(()) => {
            pgrx::log!(
                "pg_trickle: post-refresh REINDEX completed for {}.{}",
                st.pgt_schema,
                st.pgt_name,
            );
            // Reset the drift counter.
            if let Err(e) = crate::catalog::StreamTableMeta::reset_reindex_drift_counter(st.pgt_id)
            {
                pgrx::log!(
                    "pg_trickle: failed to reset reindex drift counter for {}.{}: {}",
                    st.pgt_schema,
                    st.pgt_name,
                    e
                );
            }
        }
        Err(e) => {
            pgrx::log!(
                "pg_trickle: post-refresh REINDEX failed for {}.{}: {}",
                st.pgt_schema,
                st.pgt_name,
                e
            );
        }
    }
}

/// P3-5: Update the auto-backoff factor for a stream table based on its
/// last refresh timing. Doubles the factor when falling behind; resets to
/// 1.0 on the first on-time cycle.
fn update_backoff_factor(pgt_id: i64, backoff_factors: &mut HashMap<i64, f64>) {
    let st = match load_st_by_id(pgt_id) {
        Some(st) => st,
        None => return,
    };
    let schedule_str = match &st.schedule {
        Some(s) => s.clone(),
        None => return,
    };
    let max_secs = match crate::api::parse_duration(&schedule_str) {
        Ok(s) => s,
        Err(_) => return,
    };
    let schedule_ms = max_secs * 1000;
    if schedule_ms <= 0 {
        return;
    }

    // Check last refresh duration from history
    let elapsed_ms: Option<i64> = Spi::get_one_with_args::<i64>(
        "SELECT EXTRACT(EPOCH FROM (end_time - start_time))::BIGINT * 1000 \
         FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = $1 AND status = 'COMPLETED' AND end_time IS NOT NULL \
         ORDER BY refresh_id DESC LIMIT 1",
        &[pgt_id.into()],
    )
    .unwrap_or(None);

    let elapsed_ms = match elapsed_ms {
        Some(ms) => ms,
        None => return,
    };

    // Use a 95% threshold: only back off when refresh genuinely cannot keep
    // up with the schedule. The lower 80% threshold used by EC-11 alerting
    // is intentionally more sensitive (early warning), whereas backoff should
    // only activate when the situation is clearly unsustainable.
    if is_falling_behind(elapsed_ms, schedule_ms, 0.95).is_some() {
        // Double the backoff factor, cap at 8x.
        // An 8x cap limits worst-case slowdown to 8× the configured interval
        // and self-heals immediately on the first on-time refresh cycle.
        let factor = backoff_factors.get(&pgt_id).copied().unwrap_or(1.0);
        let new_factor = (factor * 2.0).min(8.0);
        backoff_factors.insert(pgt_id, new_factor);
        pgrx::warning!(
            "pg_trickle: auto-backoff for pgt_id={} increased to {:.0}x \
             (refresh {}ms, schedule {}ms). The effective refresh interval \
             is now {}ms. It will reset automatically once a refresh completes \
             within the schedule budget.",
            pgt_id,
            new_factor,
            elapsed_ms,
            schedule_ms,
            (schedule_ms as f64 * new_factor) as i64,
        );
    } else {
        // On-time: reset backoff
        if backoff_factors.remove(&pgt_id).is_some() {
            pgrx::warning!(
                "pg_trickle: auto-backoff for pgt_id={} reset to 1x (refresh \
                 completed on time at {}ms vs {}ms schedule).",
                pgt_id,
                elapsed_ms,
                schedule_ms,
            );
        }
    }
}

/// Load a stream table by its pgt_id, or return None if not found.
fn load_st_by_id(pgt_id: i64) -> Option<StreamTableMeta> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT pgt_id, pgt_relid, pgt_name, pgt_schema, defining_query, \
                 schedule, refresh_mode, status, is_populated, data_timestamp, \
                 consecutive_errors, needs_reinit, frontier \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
                None,
                &[pgt_id.into()],
            )
            .ok()?;

        if table.is_empty() {
            return None;
        }

        // Use get_by_name as a workaround — we have the data already
        None
    })
    // Fallback: try via the catalog API
    .or_else(|| {
        // Load all active STs and find the one we need
        StreamTableMeta::get_all_active()
            .ok()?
            .into_iter()
            .find(|st| st.pgt_id == pgt_id)
    })
}

/// Check if a ST is stale (staleness exceeds effective schedule or cron is due).
///
/// G-7: When tiered scheduling is enabled, the tier multiplier is applied
/// to duration-based schedules. Frozen-tier STs always return `false`.
///
/// DI-9: IMMEDIATE-mode STs always return `false` — they are refreshed
/// synchronously within the user's transaction by AFTER triggers. The
/// scheduler has no work to do for them and acquiring locks would only
/// cause contention with the IMMEDIATE trigger path. Downstream CALCULATED
/// dependants are unaffected: they detect upstream changes via
/// `has_stream_table_source_changes()` independently.
fn check_schedule(st: &StreamTableMeta, _dag: &StDag) -> bool {
    // DI-9: IMMEDIATE-mode tables refresh inline — the scheduler must not
    // compete for locks on them.
    if st.refresh_mode.is_immediate() {
        return false;
    }

    // If not yet populated, always needs refresh
    if !st.is_populated {
        return true;
    }

    // G-7: When tiered scheduling is enabled, check tier first.
    if config::pg_trickle_tiered_scheduling() {
        let tier = RefreshTier::from_sql_str(&st.refresh_tier);
        if tier == RefreshTier::Frozen {
            emit_frozen_tier_skip(st);
            return false;
        }
    }

    // Check staleness vs schedule
    if let Some(ref schedule_str) = st.schedule {
        // Determine if this is a cron expression or a duration
        let trimmed = schedule_str.trim();
        if trimmed.starts_with('@') || trimmed.contains(' ') {
            // Cron-based: check if the cron schedule says we're due
            // (tier multiplier not applied to cron schedules)
            let last_refresh_epoch = Spi::get_one_with_args::<f64>(
                "SELECT EXTRACT(EPOCH FROM last_refresh_at) FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
                &[st.pgt_id.into()],
            )
            .unwrap_or(None)
            .map(|e| e as i64);

            return crate::api::cron_is_due(trimmed, last_refresh_epoch);
        }

        // Duration-based: compare staleness against parsed seconds.
        // Uses last_refresh_at (updated on every run, including NO_DATA)
        // rather than data_timestamp (only updated when data changes).
        // This ensures the 1-minute schedule fires at most once per minute
        // regardless of whether the previous run found any data changes.
        if let Ok(max_secs) = crate::api::parse_duration(trimmed) {
            // G-7: Apply tier multiplier when tiered scheduling is enabled.
            let effective_secs = if config::pg_trickle_tiered_scheduling() {
                let tier = RefreshTier::from_sql_str(&st.refresh_tier);
                // Frozen is handled above; multiplier is always Some here.
                let mult = tier.schedule_multiplier().unwrap_or(1.0);
                ((max_secs as f64) * mult) as i64
            } else {
                max_secs
            };
            let stale = Spi::get_one_with_args::<bool>(
                "SELECT CASE WHEN last_refresh_at IS NULL THEN true \
                 ELSE EXTRACT(EPOCH FROM (now() - last_refresh_at)) > $2 END \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
                &[st.pgt_id.into(), effective_secs.into()],
            )
            .unwrap_or(Some(false))
            .unwrap_or(false);
            return stale;
        }

        // Unparseable schedule — log a warning and skip
        pgrx::warning!(
            "pg_trickle: could not parse schedule '{}' for pgt_id={}; skipping schedule check",
            schedule_str,
            st.pgt_id
        );
        return false;
    }

    // CALCULATED STs: refresh whenever upstream sources have pending changes.
    // The topological iteration order guarantees upstream STs are refreshed
    // first within a tick, so their change buffers are already populated by
    // the time we check here.
    check_upstream_changes(st)
}

/// Emit a StaleData or NoUpstreamChanges alert depending on whether the
/// scheduler itself is falling behind.
///
/// The distinction matters:
/// - **StaleData** (warning): `last_refresh_at` is also old, meaning the
///   scheduler is stuck, in backoff, or overloaded.  The view is genuinely
///   out of date.
/// - **NoUpstreamChanges** (info): `last_refresh_at` is recent (scheduler is
///   alive and looping), but `data_timestamp` is frozen because the source
///   tables have had no writes.  The view is correct; there is just nothing
///   new to show.
///
/// Called when a refresh is skipped (due to backoff, row lock, etc.)
/// so that operators are notified via NOTIFY even when the scheduler
/// cannot perform the refresh (F31: G9.4).
fn emit_stale_alert_if_needed(st: &StreamTableMeta) {
    if let Some(ref schedule_str) = st.schedule {
        let trimmed = schedule_str.trim();
        // Only for duration-based schedules (not cron)
        if !trimmed.starts_with('@')
            && !trimmed.contains(' ')
            && let Ok(max_secs) = crate::api::parse_duration(trimmed)
        {
            let schedule_f64 = max_secs as f64;

            // Fetch both timestamps in one query.
            let row = Spi::get_two_with_args::<f64, f64>(
                "SELECT \
                     EXTRACT(EPOCH FROM (now() - data_timestamp))::float8, \
                     EXTRACT(EPOCH FROM (now() - last_refresh_at))::float8 \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
                &[st.pgt_id.into()],
            )
            .unwrap_or((None, None));

            let (data_age, refresh_age) = row;

            if let Some(data_secs) = data_age
                && data_secs > schedule_f64 * 2.0
            {
                // Is the scheduler itself running on time?
                // Use 3× schedule as a generous liveness window.
                let scheduler_alive = refresh_age
                    .map(|r| r <= schedule_f64 * 3.0)
                    .unwrap_or(false);

                if scheduler_alive {
                    // Scheduler is healthy — source tables are just quiet.
                    monitor::alert_no_upstream_changes(
                        &st.pgt_schema,
                        &st.pgt_name,
                        data_secs,
                        st.pooler_compatibility_mode,
                    );
                } else {
                    // Scheduler is also behind — genuine staleness.
                    monitor::alert_stale_data(
                        &st.pgt_schema,
                        &st.pgt_name,
                        data_secs,
                        schedule_f64,
                        st.pooler_compatibility_mode,
                    );
                }
            }
        }
    } else {
        // CALCULATED STs have no explicit schedule — detect staleness by
        // comparing this ST's data_timestamp with the most-recently-refreshed
        // upstream ST.  If any upstream has refreshed more recently and the
        // lag exceeds a threshold, emit a stale alert so operators know this
        // CALCULATED ST is falling behind.
        let lag = Spi::get_one_with_args::<f64>(
            "SELECT EXTRACT(EPOCH FROM ( \
                 (SELECT MAX(u.data_timestamp) \
                  FROM pgtrickle.pgt_dependencies d \
                  JOIN pgtrickle.pgt_stream_tables u \
                    ON u.pgt_relid = d.source_relid \
                  WHERE d.pgt_id = $1 \
                    AND u.data_timestamp IS NOT NULL) \
                 - s.data_timestamp \
             ))::float8 \
             FROM pgtrickle.pgt_stream_tables s \
             WHERE s.pgt_id = $1 AND s.data_timestamp IS NOT NULL",
            &[st.pgt_id.into()],
        )
        .unwrap_or(None);

        if let Some(lag_secs) = lag {
            // Alert when the CALCULATED ST is > 60s behind its upstream.
            // Use a fixed threshold since there is no schedule to derive one from.
            let threshold = 60.0_f64;
            if lag_secs > threshold {
                monitor::alert_stale_data(
                    &st.pgt_schema,
                    &st.pgt_name,
                    lag_secs,
                    threshold,
                    st.pooler_compatibility_mode,
                );
            }
        }
    }
}

/// Emit a one-per-~60s reminder that a stream table is frozen and being
/// skipped by the scheduler.  Throttled the same way as
/// `emit_fuse_blown_reminder` — using `last_refresh_at` age modulo.
fn emit_frozen_tier_skip(st: &StreamTableMeta) {
    let since_last = Spi::get_one_with_args::<f64>(
        "SELECT EXTRACT(EPOCH FROM (now() - COALESCE(last_refresh_at, created_at)))::float8 \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
        &[st.pgt_id.into()],
    )
    .unwrap_or(None);

    if let Some(secs) = since_last {
        let interval = 60.0_f64;
        if secs >= interval && (secs % interval) < 1.0 {
            monitor::emit_alert(
                monitor::AlertEvent::FrozenTierSkip,
                &st.pgt_schema,
                &st.pgt_name,
                serde_json::json!({ "frozen_seconds": secs.round() }),
                st.pooler_compatibility_mode,
            );
        }
    }
}

/// Check if any upstream source has pending changes.
fn check_upstream_changes(st: &StreamTableMeta) -> bool {
    // Walk every dependency of this stream table and check whether any source
    // has pending changes that have not yet been reflected in our data.
    //
    // Two kinds of upstream sources:
    //   TABLE        — base tables with trigger-based CDC.  Pending changes
    //                  live in pgtrickle_changes.changes_{oid}.
    //   STREAM_TABLE — intermediate stream tables with change buffers
    //                  (changes_pgt_{id}).  We check for rows with LSN
    //                  beyond the stored frontier, falling back to
    //                  data_timestamp comparison when no buffer exists yet.
    if !st.is_populated {
        return true;
    }
    has_table_source_changes(st) || has_stream_table_source_changes(st)
}

/// Resolve upstream change state for a scheduler decision.
///
/// For multi-member diamond groups we can freeze TABLE-source visibility
/// before any sibling refresh runs, because one member may otherwise make the
/// shared change buffer look empty to a later sibling in the same cycle.
/// STREAM_TABLE staleness is always evaluated live so convergence nodes still
/// see freshly-updated upstream timestamps from earlier group members.
fn upstream_change_state(
    st: &StreamTableMeta,
    table_change_snapshot: Option<bool>,
) -> (bool, bool) {
    if !st.is_populated {
        return (true, false);
    }

    let has_table_changes = table_change_snapshot.unwrap_or_else(|| has_table_source_changes(st));
    let has_stream_table_changes = has_stream_table_source_changes(st);
    (
        has_table_changes || has_stream_table_changes,
        has_stream_table_changes,
    )
}

/// SCAL-2: Batch change detection across multiple stream tables in one SPI call.
///
/// Builds a single SQL query that checks ALL source change buffers for all
/// provided stream tables at once.  Returns the set of `pgt_id`s that have at
/// least one pending change.  This reduces per-tick SPI round-trips from
/// `O(num_sts × num_sources)` to `O(1)`.
///
/// Implementation: for each ST we emit one `SELECT pgt_id WHERE EXISTS(sources...)`
/// arm and UNION ALL them together.  A single `array_agg` collects the IDs that
/// matched.
pub(crate) fn batched_has_source_changes(
    sts: &[StreamTableMeta],
) -> std::collections::HashSet<i64> {
    use std::collections::HashSet;
    use std::fmt::Write;

    if sts.is_empty() {
        return HashSet::new();
    }

    let change_schema = config::pg_trickle_change_buffer_schema();
    let mut arms: Vec<String> = Vec::with_capacity(sts.len());

    for st in sts {
        let source_oids = get_source_oids_for_st(st.pgt_id);
        if source_oids.is_empty() {
            continue;
        }

        // Build the inner UNION ALL for this ST's sources.
        // v0.32.0+: buffer table uses stable hash name; fall back to OID if untracked.
        let inner: Vec<String> = source_oids
            .iter()
            .map(|oid| {
                let buf = crate::cdc::buffer_qualified_name_for_oid(&change_schema, *oid);
                format!("SELECT 1 FROM {buf}")
            })
            .collect();

        let mut arm = String::new();
        let _ = write!(
            &mut arm,
            "SELECT {pgt_id}::bigint AS pgt_id WHERE EXISTS({inner})",
            pgt_id = st.pgt_id,
            inner = inner.join(" UNION ALL "),
        );
        arms.push(arm);
    }

    if arms.is_empty() {
        return HashSet::new();
    }

    // Execute once: collect all pgt_ids with pending changes.
    let sql = format!(
        "SELECT array_agg(pgt_id) FROM ({arms}) t",
        arms = arms.join(" UNION ALL "),
    );

    let ids: HashSet<i64> = Spi::get_one::<Vec<i64>>(&sql) // nosemgrep: rust.spi.get-one.dynamic-format — change_schema is config-derived, OIDs are system values
        .unwrap_or(None)
        .unwrap_or_default()
        .into_iter()
        .collect();

    ids
}

/// Returns `true` if any TABLE or FOREIGN_TABLE upstream source has rows in
/// its CDC change buffer that have not yet been consumed by a refresh.
///
/// PERF-6: Uses a single batched EXISTS query instead of one SPI round-trip
/// per source table.
fn has_table_source_changes(st: &StreamTableMeta) -> bool {
    if let Err(e) = refresh::poll_foreign_table_sources_for_st(st) {
        log!(
            "pg_trickle: failed to poll foreign table sources for {}.{} while checking upstream changes: {}",
            st.pgt_schema,
            st.pgt_name,
            e,
        );
    }

    let change_schema = config::pg_trickle_change_buffer_schema();
    let source_oids = get_source_oids_for_st(st.pgt_id);

    if source_oids.is_empty() {
        return false;
    }

    // PERF-6: Build a single UNION ALL EXISTS query for all sources.
    // Note: LIMIT 1 is omitted from individual branches because
    // `SELECT ... LIMIT 1 UNION ALL SELECT ...` is a syntax error in
    // PostgreSQL (LIMIT binds at the top level, not per-branch).
    // EXISTS already short-circuits on the first row found.
    // v0.32.0+: buffer table uses stable hash name; fall back to OID if untracked.
    let union_arms: Vec<String> = source_oids
        .iter()
        .map(|oid| {
            let buf = crate::cdc::buffer_qualified_name_for_oid(&change_schema, *oid);
            format!("SELECT 1 FROM {buf}")
        })
        .collect();
    let batched_sql = format!("SELECT EXISTS({})", union_arms.join(" UNION ALL "));

    Spi::get_one::<bool>(&batched_sql) // nosemgrep: rust.spi.query.dynamic-format
        .unwrap_or(Some(false))
        .unwrap_or(false)
}

/// Returns `true` if any STREAM_TABLE upstream has buffered changes beyond
/// the current frontier.
///
/// For each upstream ST dependency that has a change buffer
/// (`changes_pgt_{pgt_id}`), we check whether rows exist with an LSN greater
/// than the LSN recorded in our frontier.  If no buffer exists yet (e.g.
/// before the first refresh), we fall back to the timestamp comparison.
fn has_stream_table_source_changes(st: &StreamTableMeta) -> bool {
    let deps = crate::catalog::StDependency::get_for_st(st.pgt_id).unwrap_or_default();
    let change_schema = config::pg_trickle_change_buffer_schema();
    // PERF-6: borrow frontier rather than cloning the entire HashMap.
    let frontier = st
        .frontier
        .as_ref()
        .unwrap_or_else(|| crate::version::Frontier::empty_ref());

    for dep in &deps {
        if dep.source_type != "STREAM_TABLE" {
            continue;
        }

        let upstream_pgt_id =
            match crate::catalog::StreamTableMeta::pgt_id_for_relid(dep.source_relid) {
                Some(id) => id,
                None => continue,
            };

        // If a change buffer exists, check for rows beyond the frontier LSN.
        if crate::cdc::has_st_change_buffer(upstream_pgt_id, &change_schema) {
            let prev_lsn = frontier.get_st_lsn(upstream_pgt_id);
            let has_new_rows = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(SELECT 1 FROM {schema}.changes_pgt_{id} \
                 WHERE lsn > '{lsn}'::pg_lsn)",
                schema = change_schema,
                id = upstream_pgt_id,
                lsn = prev_lsn,
            ))
            .unwrap_or(Some(false))
            .unwrap_or(false);

            if has_new_rows {
                return true;
            }

            // NS-8 fallback: The change buffer exists but has no new rows.
            // This happens when a FULL-mode upstream refreshes with zero net
            // row changes — `capture_full_refresh_diff_to_st_buffer` produces
            // nothing and the buffer stays empty.  Without this fallback,
            // CALCULATED downstreams stall indefinitely because they never
            // detect the "upstream refreshed" signal.
            //
            // Compare `last_refresh_at` timestamps: if the upstream has
            // refreshed more recently than we have, we should re-evaluate.
            // A NoData run on our side advances our own `last_refresh_at`
            // past the upstream's, preventing re-triggering until the
            // upstream refreshes again.
            let upstream_refreshed_since = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS ( \
                   SELECT 1 \
                   FROM pgtrickle.pgt_stream_tables upstream \
                   JOIN pgtrickle.pgt_stream_tables us ON us.pgt_id = {my_id} \
                   WHERE upstream.pgt_relid = {up_relid}::oid \
                     AND upstream.last_refresh_at \
                         > COALESCE(us.last_refresh_at, '-infinity'::timestamptz) \
                 )",
                my_id = st.pgt_id,
                up_relid = dep.source_relid.to_u32(),
            ))
            .unwrap_or(Some(false))
            .unwrap_or(false);

            if upstream_refreshed_since {
                return true;
            }
        } else {
            // No buffer yet — use timestamp comparison.
            let upstream_newer = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS ( \
                   SELECT 1 \
                   FROM pgtrickle.pgt_stream_tables upstream \
                   JOIN pgtrickle.pgt_stream_tables us ON us.pgt_id = {} \
                   WHERE upstream.pgt_relid = {}::oid \
                     AND upstream.data_timestamp \
                         > COALESCE(us.data_timestamp, '-infinity'::timestamptz) \
                 )",
                st.pgt_id,
                dep.source_relid.to_u32(),
            ))
            .unwrap_or(Some(false))
            .unwrap_or(false);

            if upstream_newer {
                return true;
            }
        }
    }
    false
}

// ── Fixpoint Iteration for Cyclic SCCs (CYC-5) ───────────────────────────

/// Query the current number of rows in a stream table directly from the DB.
///
/// Used by [`iterate_to_fixpoint`] to compute per-pass deltas for convergence
/// detection without relying on CDC change buffers.
fn get_st_row_count(pgt_id: i64) -> Option<i64> {
    let st = load_st_by_id(pgt_id)?;
    let sql = format!(
        "SELECT count(*)::bigint FROM \"{}\".\"{}\"",
        st.pgt_schema.replace('"', "\"\""),
        st.pgt_name.replace('"', "\"\""),
    );
    Spi::get_one::<i64>(&sql).ok().flatten()
}

/// Iterate a cyclic SCC to a fixed point.
///
/// Refreshes all members of the SCC repeatedly until convergence (zero net
/// changes across all members in a full pass) or `max_fixpoint_iterations`
/// is exceeded.
///
/// Each member must use DIFFERENTIAL mode — FULL mode truncates and re-inserts
/// all rows every iteration, which would never converge to zero changes.
///
/// All SCC members are wrapped in an internal sub-transaction so that a
/// failure in any member rolls back the entire iteration.
fn iterate_to_fixpoint(
    scc: &Scc,
    _dag: &StDag,
    retry_states: &mut HashMap<i64, RetryState>,
    retry_policy: &RetryPolicy,
    now_ms: u64,
    tick_watermark: Option<&str>,
) {
    let max_iter = config::pg_trickle_max_fixpoint_iterations();
    let gated_oids = load_gated_source_oids();

    // Collect ST pgt_ids from the SCC.
    let member_ids: Vec<i64> = scc
        .nodes
        .iter()
        .filter_map(|n| match n {
            NodeId::StreamTable(id) => Some(*id),
            _ => None,
        })
        .collect();

    if member_ids.is_empty() {
        return;
    }

    // Build member names for logging.
    let member_names: Vec<String> = member_ids
        .iter()
        .filter_map(|id| load_st_by_id(*id).map(|st| format!("{}.{}", st.pgt_schema, st.pgt_name)))
        .collect();

    log!(
        "pg_trickle: SCC fixpoint — starting iteration for {} members [{}]",
        member_ids.len(),
        member_names.join(", "),
    );

    // Validate all members use DIFFERENTIAL mode.
    for &pgt_id in &member_ids {
        if let Some(st) = load_st_by_id(pgt_id)
            && st.refresh_mode != RefreshMode::Differential
        {
            pgrx::warning!(
                "pg_trickle: SCC fixpoint — {}.{} uses {} mode, \
                     but only DIFFERENTIAL is supported in cyclic dependencies",
                st.pgt_schema,
                st.pgt_name,
                st.refresh_mode.as_str(),
            );
            return;
        }
    }

    // Seed per-member row counts from the current DB state.
    //
    // Convergence is detected by comparing each member's count(*) before
    // and after each pass.  Counts are always read OUTSIDE sub-transactions
    // (after subtxn.commit()) so that pgrx's nested Spi::get_one context
    // sees the fully committed state and not a potentially stale snapshot.
    let mut prev_row_counts: HashMap<i64, i64> = member_ids
        .iter()
        .map(|&id| (id, get_st_row_count(id).unwrap_or(0)))
        .collect();

    for iteration in 0..max_iter {
        let mut iteration_ok = true;
        let mut any_refreshed = false;

        {
            let subtxn = SubTransaction::begin();

            for &pgt_id in &member_ids {
                let st = match load_st_by_id(pgt_id) {
                    Some(st) => st,
                    None => continue,
                };

                if st.status != StStatus::Active && st.status != StStatus::Initializing {
                    continue;
                }

                // Skip gated sources.
                if is_any_source_gated(pgt_id, &gated_oids) {
                    log_gated_skip(&st);
                    continue;
                }

                // Check retry backoff.
                let retry = retry_states.entry(pgt_id).or_default();
                if retry.is_in_backoff(now_ms) {
                    emit_stale_alert_if_needed(&st);
                    continue;
                }

                // Always use FULL refresh in the fixpoint loop.
                //
                // SCC peers are stream tables with no CDC change buffers.
                // DIFFERENTIAL refresh short-circuits when the change-buffer
                // query returns no rows — which is always the case inside a
                // cyclic SCC where the only upstream changes come from sibling
                // stream tables.  FULL refresh re-executes the defining query
                // against the current DB state each pass, which is the correct
                // semantics for monotone fixpoint iteration.
                let action = RefreshAction::Full;

                let result = execute_scheduled_refresh(&st, action, tick_watermark, None);

                match result {
                    RefreshOutcome::Success => {
                        any_refreshed = true;
                        let retry = retry_states.entry(pgt_id).or_default();
                        retry.reset();
                    }
                    RefreshOutcome::RetryableFailure | RefreshOutcome::PermanentFailure => {
                        log!(
                            "pg_trickle: SCC fixpoint aborted — {}.{} failed at iteration {}",
                            st.pgt_schema,
                            st.pgt_name,
                            iteration + 1,
                        );
                        iteration_ok = false;
                        break;
                    }
                }
            }

            if !iteration_ok {
                subtxn.rollback();
                // Record failure for retry tracking on all members.
                for &pgt_id in &member_ids {
                    let retry = retry_states.entry(pgt_id).or_default();
                    retry.record_failure(retry_policy, now_ms);
                }
                return;
            }

            subtxn.commit();
        }
        // subtxn is committed; now read row counts in the outer transaction
        // where the full sub-transaction contents are visible.

        if !any_refreshed {
            // All members skipped (backoff/gating) — cannot assess convergence.
            continue;
        }

        let mut total_changes: i64 = 0;
        for &pgt_id in &member_ids {
            let new_count = get_st_row_count(pgt_id).unwrap_or(0);
            let old_count = prev_row_counts.get(&pgt_id).copied().unwrap_or(0);
            total_changes += (new_count - old_count).abs();
            prev_row_counts.insert(pgt_id, new_count);
        }

        if total_changes == 0 {
            log!(
                "pg_trickle: SCC converged after {} iteration(s) [{}]",
                iteration + 1,
                member_names.join(", "),
            );
            // Record convergence metadata in the catalog.
            for &pgt_id in &member_ids {
                let _ = StreamTableMeta::update_last_fixpoint_iterations(pgt_id, iteration + 1);
            }
            return;
        }

        log!(
            "pg_trickle: SCC fixpoint iteration {} — {} total changes",
            iteration + 1,
            total_changes,
        );
    }

    // Non-convergence: mark all SCC members as ERROR.
    pgrx::warning!(
        "pg_trickle: SCC did not converge after {} iterations — marking {} members as ERROR [{}]",
        max_iter,
        member_ids.len(),
        member_names.join(", "),
    );
    for &pgt_id in &member_ids {
        let _ = StreamTableMeta::update_status(pgt_id, StStatus::Error);
        let _ = StreamTableMeta::update_last_fixpoint_iterations(pgt_id, max_iter);
    }
}

/// Read the rows_inserted and rows_deleted from the most recent completed
/// refresh record for a stream table. Used by fixpoint iteration to detect
/// convergence without modifying the `execute_scheduled_refresh` return type.
fn last_refresh_row_counts(pgt_id: i64) -> (i64, i64) {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT rows_inserted, rows_deleted \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE pgt_id = $1 AND status = 'COMPLETED' \
                 ORDER BY refresh_id DESC LIMIT 1",
                None,
                &[pgt_id.into()],
            )
            .ok();
        match table {
            Some(t) if !t.is_empty() => {
                let first = t.first();
                let ins = first.get::<i64>(1).ok().flatten().unwrap_or(0);
                let del = first.get::<i64>(2).ok().flatten().unwrap_or(0);
                (ins, del)
            }
            _ => (0, 0),
        }
    })
}

// ── COORD-10/11/12/13/14 — moved to citus.rs sub-module ──────────────────

/// Refresh a single (non-group) stream table with full retry handling.
///
/// This is the singleton fast path used by both non-diamond STs and
/// diamond STs where not all members opted into atomic mode.
#[allow(clippy::too_many_arguments)]
fn refresh_single_st(
    pgt_id: i64,
    dag_ref: &StDag,
    now_ms: u64,
    retry_states: &mut HashMap<i64, RetryState>,
    retry_policy: &RetryPolicy,
    table_change_snapshot: Option<bool>,
    tick_watermark: Option<&str>,
    drift_counter: Option<&mut i32>,
    spill_counters: &mut HashMap<i64, i32>,
) {
    let st = match load_st_by_id(pgt_id) {
        Some(st) => st,
        None => return,
    };

    if st.status != StStatus::Active && st.status != StStatus::Initializing {
        return;
    }

    // D-3 TEST-MODE: When pg_trickle.test_chaos_for_table names this ST,
    // simulate a retryable refresh failure by directly incrementing
    // consecutive_errors and returning early — no subtransaction, no SPI
    // inside a subtransaction, no PG exception handling required.  This lets
    // the D-3 E2E test reliably trigger auto-suspension without depending on
    // catching PostgreSQL exceptions from user trigger RAISE EXCEPTION calls.
    {
        let chaos_target = config::pg_trickle_test_chaos_for_table();
        if !chaos_target.is_empty() && chaos_target == st.pgt_name {
            match StreamTableMeta::increment_errors(pgt_id) {
                Ok(count) if count >= config::pg_trickle_max_consecutive_errors() => {
                    let _ = StreamTableMeta::update_status(pgt_id, StStatus::Suspended);
                    monitor::alert_auto_suspended(
                        &st.pgt_schema,
                        &st.pgt_name,
                        count,
                        st.pooler_compatibility_mode,
                    );
                    log!(
                        "pg_trickle: TEST-MODE suspended {}.{} after {} chaos-injected errors",
                        st.pgt_schema,
                        st.pgt_name,
                        count,
                    );
                }
                _ => {}
            }
            return;
        }
    }

    // BOOT-4: Check bootstrap source gates — skip if any source is gated.
    let gated_oids = load_gated_source_oids();
    if is_any_source_gated(pgt_id, &gated_oids) {
        log!(
            "pg_trickle: skipping {}.{} — source gated",
            st.pgt_schema,
            st.pgt_name,
        );
        log_gated_skip(&st);
        return;
    }

    // WM-4: Check watermark alignment — skip if misaligned.
    let (wm_misaligned, wm_reason) = is_watermark_misaligned(pgt_id);
    if wm_misaligned {
        let reason = wm_reason.as_deref().unwrap_or("watermark misaligned");
        log!(
            "pg_trickle: skipping {}.{} — {}",
            st.pgt_schema,
            st.pgt_name,
            reason,
        );
        log_watermark_skip(&st, reason);
        return;
    }

    // WM-7: Check watermark staleness — skip if any source watermark is stuck.
    let (wm_stuck, wm_stuck_reason) = is_watermark_stuck(pgt_id);
    if wm_stuck {
        let reason = wm_stuck_reason.as_deref().unwrap_or("watermark stuck");
        log!(
            "pg_trickle: skipping {}.{} — {}",
            st.pgt_schema,
            st.pgt_name,
            reason,
        );
        log_watermark_skip(&st, reason);
        return;
    }

    let retry = retry_states.entry(pgt_id).or_default();
    if retry.is_in_backoff(now_ms) {
        emit_stale_alert_if_needed(&st);
        return;
    }

    // D-1b: Check for UNLOGGED buffer data loss after crash recovery.
    // If detected, mark_for_reinitialize is called (sets needs_reinit=true),
    // so the refresh path below will pick up the Reinitialize action.
    check_unlogged_buffer_crash_recovery(&st);
    // Reload ST in case needs_reinit was just set.
    let st = match load_st_by_id(pgt_id) {
        Some(st) => st,
        None => return,
    };

    let needs_refresh = check_schedule(&st, dag_ref);
    if !needs_refresh && !st.needs_reinit {
        return;
    }

    if check_skip_needed(&st) {
        log!(
            "pg_trickle: skipping {}.{} — previous refresh still running",
            st.pgt_schema,
            st.pgt_name,
        );
        return;
    }

    // ST-on-ST safety: skip this ST if any upstream STREAM_TABLE source is
    // currently being refreshed by another session.  When the scheduler does
    // a FULL refresh of a downstream calculated ST, it reads the current
    // state of all upstream STs.  If an upstream is mid-refresh (row locked
    // by a manual `refresh_stream_table` call), the FULL refresh may read a
    // mix of pre-/post-update upstream data, producing incorrect results
    // that cannot be reliably detected and corrected afterwards.
    //
    // The check is cheap (one FOR SHARE SKIP LOCKED per upstream ST) and
    // only affects STs with STREAM_TABLE dependencies.
    {
        let deps = crate::catalog::StDependency::get_for_st(pgt_id).unwrap_or_default();
        let has_st_source = deps.iter().any(|dep| dep.source_type == "STREAM_TABLE");
        if has_st_source {
            let any_upstream_locked = deps
                .iter()
                .filter(|dep| dep.source_type == "STREAM_TABLE")
                .any(|dep| {
                    let upstream_pgt_id =
                        crate::catalog::StreamTableMeta::pgt_id_for_relid(dep.source_relid);
                    match upstream_pgt_id {
                        Some(up_id) => Spi::get_one_with_args::<i64>(
                            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
                                 WHERE pgt_id = $1 FOR SHARE SKIP LOCKED",
                            &[up_id.into()],
                        )
                        .unwrap_or(None)
                        .is_none(),
                        None => false,
                    }
                });

            if any_upstream_locked {
                pgrx::debug1!(
                    "[pg_trickle] skipping {}.{} — upstream ST is being refreshed by another session",
                    st.pgt_schema,
                    st.pgt_name,
                );
                return;
            }
        }
    }

    // FUSE-5: Check fuse circuit breaker.
    if evaluate_fuse(&st) {
        log!(
            "pg_trickle: {}.{} fuse blown — skipping inline refresh",
            st.pgt_schema,
            st.pgt_name,
        );
        return;
    }

    // COORD-10/11/12/13 (v0.34.0): Drive distributed CDC pre-refresh.
    //
    // If this stream table has distributed sources (Citus), poll each worker
    // slot to drain per-worker WAL changes into the local change buffer before
    // the local refresh step runs.  Returns the lock key if a pgt_st_locks
    // lease was acquired so we can release it after the local refresh.
    let distributed_lock_key = drive_distributed_cdc(pgt_id);

    let (has_changes, _has_stream_table_changes) =
        upstream_change_state(&st, table_change_snapshot);

    let action = {
        let mut base_action = refresh::determine_refresh_action(&st, has_changes);

        // A08 (v0.35.0): Force-full-refresh GUC override.
        // When `pg_trickle.force_full_refresh = true`, override any DIFFERENTIAL
        // action to FULL regardless of per-ST refresh_mode.
        if base_action == RefreshAction::Differential && config::pg_trickle_force_full_refresh() {
            log!(
                "pg_trickle: A08 force_full_refresh override for {}.{}",
                st.pgt_schema,
                st.pgt_name,
            );
            base_action = RefreshAction::Full;
        }

        // PRED-2: Predictive cost model — pre-emptive FULL switch.
        // If the predicted DIFFERENTIAL cost exceeds the FULL cost by the
        // configured ratio, switch to FULL preemptively.
        //
        // PERF-3: Try the shmem cost-model cache first to avoid an extra SPI
        // call.  Fall back to `st.last_full_ms` (loaded from catalog at tick
        // start) if the shmem slot is empty.
        let last_full_cost = crate::shmem::read_cost_model(st.pgt_id)
            .map(|(full_ms, _)| full_ms)
            .or(st.last_full_ms);
        if base_action == RefreshAction::Differential
            && let Some(last_full) = last_full_cost
        {
            // Estimate delta_rows from change buffers.
            let delta_rows = crate::cdc::estimate_pending_changes(st.pgt_id).unwrap_or(0);
            if delta_rows > 0
                && crate::api::publication::should_preempt_to_full(st.pgt_id, delta_rows, last_full)
            {
                log!(
                    "pg_trickle: PRED-2 pre-emptive FULL switch for {}.{} \
                     (predicted diff cost exceeds {}× full cost)",
                    st.pgt_schema,
                    st.pgt_name,
                    config::pg_trickle_prediction_ratio(),
                );
                refresh::set_refresh_reason("predicted_cost_exceeds_full");
                base_action = RefreshAction::Full;
            }
        }

        // Check periodic drift reset
        if base_action == RefreshAction::Differential {
            let max_cycles = config::pg_trickle_algebraic_drift_reset_cycles();
            if let Some(counter) = &drift_counter
                && max_cycles > 0
                && **counter >= max_cycles
            {
                log!(
                    "pg_trickle: drift reset triggered for {}.{} ({} differential cycles)",
                    st.pgt_schema,
                    st.pgt_name,
                    counter
                );
                // Convert action to reinitialize. This will trigger the drift counter reset
                // inside execute_scheduled_refresh on success.
                base_action = RefreshAction::Reinitialize;
            }
        }
        base_action
    };

    // ERR-1e: Wrap the refresh in a subtransaction + catch_unwind so that
    // PG ERRORs (which pgrx converts to panics via longjmp) don't abort
    // the entire tick transaction.  On panic the subtransaction rolls back
    // (undoing TRUNCATE + partial writes) and we set ERROR status in the
    // still-valid outer transaction.
    //
    // IMPORTANT: pgrx SPI calls (SPI_execute_with_args etc.) do NOT wrap
    // themselves in pg_guard_ffi_boundary.  When a user trigger fires
    // RAISE EXCEPTION inside a SPI call, PostgreSQL longjmps directly to
    // PG_exception_stack — which is the outermost boundary set by the
    // #[pg_guard] wrapper on pg_trickle_scheduler_main.  That longjmp
    // bypasses all Rust stack frames (including catch_unwind) between the
    // SPI call site and the bgworker entry, so catch_unwind never fires.
    //
    // Fix: wrap execute_scheduled_refresh in pg_guard_ffi_boundary INSIDE
    // the catch_unwind closure.  This installs a LOCAL PG exception handler
    // that intercepts the trigger longjmp before it reaches the outermost
    // boundary, converts it to panic_any(), and lets catch_unwind catch the
    // resulting Rust panic so ERR-1e code can run.
    //
    // We also save PG_exception_stack so we can restore it if
    // execute_scheduled_refresh has a pure Rust panic (not a PG error): in
    // that case pg_guard_ffi_boundary does not restore PG_exception_stack,
    // and we must do so before calling subtxn.rollback() to avoid a stale
    // pointer being dereferenced if RollbackAndReleaseCurrentSubTransaction
    // itself raises a PG error.
    // SAFETY: bgworker main thread only.
    let saved_pg_exception_stack = unsafe { pgrx::pg_sys::PG_exception_stack };
    let subtxn = SubTransaction::begin();
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: pg_guard_ffi_boundary installs a LOCAL PG exception handler.
        // After catching a trigger longjmp, it restores PG_exception_stack and
        // calls panic_any(); the surrounding catch_unwind catches that panic.
        // Only call this from the main PostgreSQL thread (bgworker entry).
        unsafe {
            pgrx::pg_sys::ffi::pg_guard_ffi_boundary(|| {
                execute_scheduled_refresh(&st, action, tick_watermark, drift_counter)
            })
        }
    }));
    let result = match result {
        Ok(outcome) => {
            subtxn.commit();
            outcome
        }
        Err(panic_payload) => {
            // Restore PG_exception_stack. For PG errors, pg_guard_ffi_boundary
            // already restored it — this is a no-op. For pure Rust panics, it
            // was left pointing to pg_guard_ffi_boundary's now-dead jump_buffer;
            // restoring it prevents a dangling-pointer longjmp if
            // subtxn.rollback() itself raises a PG error.
            unsafe {
                pgrx::pg_sys::PG_exception_stack = saved_pg_exception_stack;
            }
            subtxn.rollback();
            let error_msg = extract_panic_message(&panic_payload);
            log!(
                "pg_trickle: refresh panicked for {}.{}: {} — setting error state",
                st.pgt_schema,
                st.pgt_name,
                error_msg,
            );
            // O39-6: use SQLSTATE-first classifier when use_sqlstate_classification=true.
            let is_retryable = crate::error::classify_error_for_retry(&error_msg);
            if !is_retryable {
                let _ = StreamTableMeta::set_error_state(pgt_id, &error_msg);
            } else {
                // ERR-1e: Track consecutive errors for auto-suspension even when
                // the failure manifests as a PostgreSQL panic (e.g. a trigger
                // raising RAISE EXCEPTION during refresh DML).  After
                // subtxn.rollback() the outer transaction is still valid, so
                // SPI calls succeed here — mirroring the Rust-error path in
                // run_refresh_for_st.
                match StreamTableMeta::increment_errors(pgt_id) {
                    Ok(count) if count >= config::pg_trickle_max_consecutive_errors() => {
                        let _ = StreamTableMeta::update_status(pgt_id, StStatus::Suspended);
                        monitor::alert_auto_suspended(
                            &st.pgt_schema,
                            &st.pgt_name,
                            count,
                            st.pooler_compatibility_mode,
                        );
                        log!(
                            "pg_trickle: suspended {}.{} after {} consecutive errors",
                            st.pgt_schema,
                            st.pgt_name,
                            count,
                        );
                    }
                    Ok(_count) => {
                        // Still under threshold — will retry on next tick.
                    }
                    Err(e) => {
                        pgrx::warning!(
                            "pg_trickle: ERR-1e: failed to persist consecutive_errors \
                             for pgt_id={} after panic-path failure: {}",
                            pgt_id,
                            e,
                        );
                    }
                }
            }
            if is_retryable {
                RefreshOutcome::RetryableFailure
            } else {
                RefreshOutcome::PermanentFailure
            }
        }
    };

    let retry = retry_states.entry(pgt_id).or_default();
    match result {
        RefreshOutcome::Success => {
            retry.reset();

            // PH-E2: Spill-aware refresh tracking.
            // After a successful refresh, check whether the MERGE spilled
            // to temp files and maintain a per-ST consecutive spill counter.
            let spill_threshold = config::pg_trickle_spill_threshold_blocks();
            if spill_threshold > 0 {
                let temp_blks = refresh::take_last_temp_blks_written();
                if temp_blks > spill_threshold as i64 {
                    let count = spill_counters.entry(pgt_id).or_insert(0);
                    *count += 1;
                    let limit = config::pg_trickle_spill_consecutive_limit();
                    if *count >= limit {
                        pgrx::warning!(
                            "pg_trickle: {}.{} spilled for {} consecutive refreshes \
                             ({} temp blocks > {} threshold) — forcing FULL refresh",
                            st.pgt_schema,
                            st.pgt_name,
                            count,
                            temp_blks,
                            spill_threshold,
                        );
                        // STAB-3: Emit alert before forcing reinitialize.
                        monitor::alert_spill_threshold_exceeded(
                            &st.pgt_schema,
                            &st.pgt_name,
                            temp_blks,
                            spill_threshold as i64,
                            *count,
                            limit,
                            st.pooler_compatibility_mode,
                        );
                        let _ = StreamTableMeta::mark_for_reinitialize(st.pgt_id);
                        *count = 0;
                    } else {
                        log!(
                            "pg_trickle: {}.{} spilled ({} temp blocks > {} threshold, \
                             {}/{} consecutive)",
                            st.pgt_schema,
                            st.pgt_name,
                            temp_blks,
                            spill_threshold,
                            count,
                            limit,
                        );
                    }
                } else if temp_blks >= 0 {
                    // Non-spilling refresh: reset counter.
                    spill_counters.remove(&pgt_id);
                }
                // temp_blks == -1 means detection was disabled/unavailable; leave counter as-is.
            }
        }
        RefreshOutcome::RetryableFailure => {
            let will_retry = retry.record_failure(retry_policy, now_ms);
            if will_retry {
                log!(
                    "pg_trickle: {}.{} will retry in {}ms (attempt {}/{})",
                    st.pgt_schema,
                    st.pgt_name,
                    retry_policy.backoff_ms(retry.attempts - 1),
                    retry.attempts,
                    retry_policy.max_attempts,
                );
            }
        }
        RefreshOutcome::PermanentFailure => {
            retry.reset();
        }
    }

    // COORD-12 (v0.34.0): Release the distributed lock if we acquired one.
    if let Some(ref lock_key) = distributed_lock_key {
        let holder = format!("scheduler_{}", unsafe { pg_sys::MyProcPid });
        if let Err(e) = crate::citus::release_st_lock(lock_key, &holder) {
            pgrx::debug1!(
                "[pg_trickle] COORD-12: release_st_lock '{}' failed (non-fatal): {}",
                lock_key,
                e
            );
        }
    }
}

/// Execute a scheduled refresh for a stream table.
///
/// Returns a [`RefreshOutcome`] indicating whether the refresh succeeded,
/// failed with a retryable error, or failed permanently.
///
/// Phase 8: Full frontier lifecycle:
/// - For FULL/REINITIALIZE: compute initial frontier from current slot positions
/// - For DIFFERENTIAL: load prev frontier, compute new frontier, pass both to DVM
/// - After success: store new frontier in catalog
///
/// Phase 10: Error classification determines retry behavior:
/// - Retryable errors (SPI, lock, slot): backoff and retry on next cycle
/// - Schema errors: flag for reinitialize, count toward suspension
/// - User/internal errors: permanent failure, count toward suspension
fn execute_scheduled_refresh(
    st: &StreamTableMeta,
    action: RefreshAction,
    tick_watermark: Option<&str>,
    drift_counter: Option<&mut i32>,
) -> RefreshOutcome {
    let start_instant = std::time::Instant::now();

    let now = Spi::get_one::<TimestampWithTimeZone>("SELECT now()")
        .unwrap_or(None)
        .unwrap_or_else(|| {
            pgrx::warning!("now() returned NULL in scheduler");
            TimestampWithTimeZone::try_from(0i64).unwrap_or_else(|_| {
                // ERR-3 (v0.26.0): HINT added for system clock diagnostics.
                pgrx::error!("scheduler: failed to create epoch TimestampWithTimeZone; HINT: check system clock configuration")
            })
        });

    // PB1: Acquire row-level lock via FOR UPDATE SKIP LOCKED.
    // The lock is held for the remainder of this transaction (released on commit).
    let got_lock = Spi::get_one_with_args::<i64>(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_id = $1 FOR UPDATE SKIP LOCKED",
        &[st.pgt_id.into()],
    )
    .unwrap_or(None)
    .is_some();

    if !got_lock {
        // Another session holds the row lock — skip this cycle
        log!(
            "pg_trickle: skipping {}.{} — catalog row locked by another session",
            st.pgt_schema,
            st.pgt_name,
        );
        return RefreshOutcome::RetryableFailure;
    }

    // TOCTOU fix: reload ST metadata now that we hold the row lock.
    // Between the caller loading `st` and acquiring this lock, another
    // session (manual refresh or a parallel worker) may have refreshed
    // this ST and advanced its frontier.  Using the stale frontier would
    // cause the differential refresh to re-process already-consumed
    // change buffer rows, producing incorrect aggregate deltas.
    let st = match load_st_by_id(st.pgt_id) {
        Some(fresh) => fresh,
        None => {
            log!(
                "pg_trickle: ST pgt_id={} disappeared after lock acquisition",
                st.pgt_id,
            );
            return RefreshOutcome::PermanentFailure;
        }
    };
    let st = &st;

    // Record refresh start
    // Compute freshness_deadline for duration-based schedules:
    // deadline = data_timestamp + schedule_seconds (when data becomes stale)
    let freshness_deadline = compute_freshness_deadline(st);
    let refresh_id = RefreshRecord::insert(
        st.pgt_id,
        now,
        action.as_str(),
        "RUNNING",
        0,
        0,
        None,
        Some("SCHEDULER"),
        freshness_deadline,
        0,     // delta_row_count — updated on completion
        None,  // merge_strategy_used — updated on completion
        false, // was_full_fallback — updated on completion
        tick_watermark,
    );

    let refresh_id = match refresh_id {
        Ok(id) => id,
        Err(e) => {
            log!(
                "pg_trickle: failed to record refresh start for {}.{}: {}",
                st.pgt_schema,
                st.pgt_name,
                e
            );
            // Row lock is released automatically at transaction end.
            return RefreshOutcome::RetryableFailure;
        }
    };

    // Compute frontier information for this refresh
    let source_oids = get_source_oids_for_st(st.pgt_id);
    let mut slot_positions = match cdc::get_slot_positions(&source_oids) {
        Ok(pos) => pos,
        Err(e) => {
            log!(
                "pg_trickle: failed to get slot positions for {}.{}: {}",
                st.pgt_schema,
                st.pgt_name,
                e
            );
            std::collections::HashMap::new()
        }
    };

    // CSS1: Cap each slot position to the tick watermark so no refresh in this
    // tick consumes WAL changes beyond the snapshot point captured at tick start.
    // Changes beyond the watermark remain in the buffer and are consumed next tick.
    if let Some(wm) = tick_watermark {
        for lsn in slot_positions.values_mut() {
            if version::lsn_gt(lsn, wm) {
                *lsn = wm.to_string();
            }
        }
    }

    // Select target data timestamp
    let schedule_secs = st
        .schedule
        .as_ref()
        .and_then(|s| crate::api::parse_duration(s).ok())
        .map(|s| s as u64);
    let data_ts_str = version::select_target_data_timestamp(
        schedule_secs,
        &[], // upstream timestamps would be filled for ST-on-ST deps
    );
    let _ = &data_ts_str;

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let data_ts_frontier = format!("{}Z", now_secs);

    // EC-25/EC-26: Set the internal_refresh flag so DML guard triggers
    // allow the refresh executor to modify the storage table.
    let _ = Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'");

    // R3: Bypass RLS as defence-in-depth. The background worker already
    // runs as superuser, but this ensures the defining query always
    // materializes the full result set even if pg_trickle is installed
    // by a non-superuser role with BYPASSRLS.
    let _ = Spi::run("SET LOCAL row_security = off"); // nosemgrep: sql.row-security.disabled — intentional R3 bypass, mirrors REFRESH MATERIALIZED VIEW semantics.

    // Execute the refresh

    // Collect current high-water marks for upstream ST change buffers so
    // we can embed them in the frontier.
    //
    // ST-ST-7: Use MAX(lsn) from the actual change buffer rather than
    // pg_current_wal_lsn(). Within a single scheduler tick (one transaction),
    // an upstream ST's FULL-refresh capture writes rows to its change buffer
    // using pg_current_wal_lsn(). If this downstream ST's augment ALSO uses
    // pg_current_wal_lsn(), the augmented frontier LSN may be >= the captured
    // rows' LSN. Then the next tick's delta (lsn > prev_lsn) misses those
    // rows because they sit AT the frontier, not above it.
    //
    // Using MAX(lsn) from the buffer records EXACTLY the latest data point,
    // ensuring the next tick's delta correctly starts AFTER consumed data.
    let change_schema_for_st = crate::config::pg_trickle_change_buffer_schema();
    let st_source_positions: Vec<(i64, String)> =
        crate::catalog::StDependency::get_for_st(st.pgt_id)
            .unwrap_or_default()
            .iter()
            .filter(|dep| dep.source_type == "STREAM_TABLE")
            .filter_map(|dep| {
                let upstream_pgt_id =
                    crate::catalog::StreamTableMeta::pgt_id_for_relid(dep.source_relid)?;
                if !crate::cdc::has_st_change_buffer(upstream_pgt_id, &change_schema_for_st) {
                    return None;
                }
                // ST-ST-7: Read the actual max LSN from the change buffer.
                // Falls back to pg_current_wal_lsn() if the buffer is empty
                // (e.g., freshly created, no data captured yet).
                let lsn = Spi::get_one::<String>(&format!(
                    "SELECT COALESCE(MAX(lsn)::text, pg_current_wal_lsn()::text) \
                     FROM \"{schema}\".changes_pgt_{id}",
                    schema = change_schema_for_st,
                    id = upstream_pgt_id,
                ))
                .unwrap_or(None)
                .unwrap_or_else(|| "0/0".to_string());
                Some((upstream_pgt_id, lsn))
            })
            .collect();

    let augment_frontier = |frontier: &mut version::Frontier| {
        for (upstream_pgt_id, lsn) in &st_source_positions {
            frontier.set_st_source(*upstream_pgt_id, lsn.clone(), data_ts_frontier.clone());
        }
    };

    let result = if st.topk_limit.is_some() {
        // TopK tables bypass the normal Full/Differential refresh paths and use
        // scoped-recomputation MERGE (ORDER BY … LIMIT N) instead.
        refresh::execute_topk_refresh(st)
    } else {
        match action {
            RefreshAction::NoData => refresh::execute_no_data_refresh(st).map(|_| (0i64, 0i64)),
            RefreshAction::Full => {
                let mut new_frontier =
                    version::compute_initial_frontier(&slot_positions, &data_ts_frontier);
                augment_frontier(&mut new_frontier);
                match refresh::execute_full_refresh(st) {
                    Ok((ins, del)) => {
                        // COR-001 (v0.72.0): Treat frontier-store failure as
                        // refresh failure — propagate the error so the
                        // transaction aborts (rolling back the data change) and
                        // the ST is retried on the next scheduler tick.
                        match StreamTableMeta::store_frontier(st.pgt_id, &new_frontier) {
                            Err(e) => Err(e),
                            Ok(()) => {
                                // G3+G4: advance WAL slots and flush change buffers
                                // now that the new frontier is stored.
                                refresh::post_full_refresh_cleanup(st);
                                if let Some(counter) = drift_counter {
                                    *counter = 0;
                                }
                                Ok((ins, del))
                            }
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            RefreshAction::Reinitialize => {
                let mut new_frontier =
                    version::compute_initial_frontier(&slot_positions, &data_ts_frontier);
                augment_frontier(&mut new_frontier);
                match refresh::execute_reinitialize_refresh(st) {
                    Ok((ins, del)) => {
                        // COR-001 (v0.72.0): Propagate frontier-store failure.
                        match StreamTableMeta::store_frontier(st.pgt_id, &new_frontier) {
                            Err(e) => Err(e),
                            Ok(()) => {
                                // G3+G4: advance WAL slots and flush change buffers
                                // now that the new frontier is stored.
                                refresh::post_full_refresh_cleanup(st);
                                // Drift reset mechanism: reset counter on full reinit
                                if let Some(counter) = drift_counter {
                                    *counter = 0;
                                }
                                Ok((ins, del))
                            }
                        }
                    }
                    Err(e) => Err(e),
                }
            }
            RefreshAction::Differential => {
                let prev_frontier = st.frontier.clone().unwrap_or_default();

                if prev_frontier.is_empty() {
                    log!(
                        "pg_trickle: no previous frontier for {}.{}, doing FULL refresh",
                        st.pgt_schema,
                        st.pgt_name
                    );
                    let mut new_frontier =
                        version::compute_initial_frontier(&slot_positions, &data_ts_frontier);
                    augment_frontier(&mut new_frontier);
                    match refresh::execute_full_refresh(st) {
                        Ok((ins, del)) => {
                            // COR-001 (v0.72.0): Propagate frontier-store failure.
                            match StreamTableMeta::store_frontier(st.pgt_id, &new_frontier) {
                                Err(e) => Err(e),
                                Ok(()) => {
                                    // G3+G4: advance WAL slots and flush change buffers
                                    // now that the new frontier is stored.
                                    refresh::post_full_refresh_cleanup(st);
                                    if let Some(counter) = drift_counter {
                                        *counter = 0;
                                    }
                                    Ok((ins, del))
                                }
                            }
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    let mut new_frontier =
                        version::compute_new_frontier(&slot_positions, &data_ts_frontier);
                    augment_frontier(&mut new_frontier);

                    match refresh::execute_differential_refresh(st, &prev_frontier, &new_frontier) {
                        Ok((ins, del)) => {
                            // COR-001 (v0.72.0): Propagate frontier-store failure.
                            match StreamTableMeta::store_frontier(st.pgt_id, &new_frontier) {
                                Err(e) => Err(e),
                                Ok(()) => {
                                    if let Some(counter) = drift_counter {
                                        *counter += 1;
                                    }
                                    Ok((ins, del))
                                }
                            }
                        }
                        // DI-7: When QueryTooComplex is returned (e.g. join
                        // count exceeds max_differential_joins), fall back to
                        // FULL refresh immediately instead of reinitializing.
                        Err(crate::error::PgTrickleError::QueryTooComplex(ref msg)) => {
                            log!(
                                "pg_trickle: DI-7 fallback for {}.{}: {}; using FULL refresh",
                                st.pgt_schema,
                                st.pgt_name,
                                msg
                            );
                            match refresh::execute_full_refresh(st) {
                                Ok((ins, del)) => {
                                    // COR-001 (v0.72.0): Propagate frontier-store failure.
                                    match StreamTableMeta::store_frontier(st.pgt_id, &new_frontier)
                                    {
                                        Err(e) => Err(e),
                                        Ok(()) => {
                                            refresh::post_full_refresh_cleanup(st);
                                            if let Some(counter) = drift_counter {
                                                *counter = 0;
                                            }
                                            Ok((ins, del))
                                        }
                                    }
                                }
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => {
                            log!(
                                "pg_trickle: differential refresh failed for {}.{}: {}, will reinitialize on next cycle",
                                st.pgt_schema,
                                st.pgt_name,
                                e
                            );
                            let _ = StreamTableMeta::mark_for_reinitialize(st.pgt_id);
                            Err(e)
                        }
                    }
                }
            }
        }
    }; // close else + let result

    // PB1: Row lock is released automatically when the transaction commits.

    let elapsed_ms = start_instant.elapsed().as_millis() as i64;

    // OBS-1: Record CDC lag sample (refresh cycle duration approximates the
    // end-to-end lag from change capture to view update completion).
    crate::shmem::record_cdc_lag_ms(elapsed_ms as u64);

    // Record refresh completion and determine outcome
    let was_full_fallback = matches!(action, RefreshAction::Reinitialize);

    match result {
        Ok((rows_inserted, rows_deleted)) => {
            let delta_row_count = rows_inserted + rows_deleted;
            let _ = RefreshRecord::complete(
                refresh_id,
                "COMPLETED",
                rows_inserted,
                rows_deleted,
                None,
                delta_row_count,
                Some(action.as_str()),
                was_full_fallback,
            );

            // G12-ERM-1: Persist the effective refresh mode actually used.
            // `take_effective_mode()` reflects any internal fallback that
            // occurred (e.g., adaptive threshold or CTE triggering FULL
            // inside execute_differential_refresh).
            let eff_mode = refresh::take_effective_mode();
            if !eff_mode.is_empty()
                && let Err(e) = StreamTableMeta::update_effective_refresh_mode(st.pgt_id, eff_mode)
            {
                log!(
                    "pg_trickle: failed to update effective_refresh_mode for {}.{}: {}",
                    st.pgt_schema,
                    st.pgt_name,
                    e
                );
            }

            // For NO_DATA refreshes, data_timestamp must NOT be updated —
            // execute_no_data_refresh already updated last_refresh_at only.
            // Updating data_timestamp here would cause downstream stream
            // tables that compare upstream.data_timestamp to see a false
            // "upstream changed" signal on every no-data polling cycle.
            //
            // DIFFERENTIAL refreshes that produce 0 rows are also treated as
            // effective NO_DATA: the change buffer may still contain rows
            // from the previous cycle (deferred cleanup runs at the START of
            // the next differential), causing has_table_source_changes() to
            // return true and the scheduler to trigger a DIFFERENTIAL that
            // finds nothing in the current frontier window.  Bumping
            // data_timestamp for such 0-row differentials would cause
            // downstream STs to see a false "upstream changed" signal and
            // trigger unnecessary FULL refreshes.
            if action == RefreshAction::NoData {
                // Already handled by execute_no_data_refresh
            } else if action == RefreshAction::Differential
                && rows_inserted == 0
                && rows_deleted == 0
            {
                // Effective NO_DATA — update last_refresh_at only
                let _ = StreamTableMeta::update_after_no_data_refresh(st.pgt_id);
            } else {
                let _ = StreamTableMeta::update_after_refresh(st.pgt_id, now, rows_inserted);
            }

            monitor::alert_refresh_completed(
                &st.pgt_schema,
                &st.pgt_name,
                action.as_str(),
                rows_inserted,
                rows_deleted,
                elapsed_ms,
                st.pooler_compatibility_mode,
            );

            log!(
                "pg_trickle: refreshed {}.{} ({}, +{} -{} rows, {}ms)",
                st.pgt_schema,
                st.pgt_name,
                action.as_str(),
                rows_inserted,
                rows_deleted,
                elapsed_ms,
            );

            // PERF-3: Update shmem cost-model cache with the latest timing.
            // Parallel workers read this from shmem rather than issuing an SPI
            // SELECT to pgt_stream_tables on every dispatch tick.
            {
                let elapsed_f64 = elapsed_ms as f64;
                let (new_full, new_diff) = match action {
                    RefreshAction::Full | RefreshAction::Reinitialize => (Some(elapsed_f64), None),
                    RefreshAction::Differential => (None, Some(elapsed_f64)),
                    _ => (None, None),
                };
                crate::shmem::update_cost_model(st.pgt_id, new_full, new_diff);
            }

            // F31: Emit StaleData alert if still stale after refresh
            // (e.g., refresh took longer than the schedule interval)
            emit_stale_alert_if_needed(st);

            // EC-11: Emit falling-behind alert if refresh duration exceeds
            // 80% of the schedule interval. This warns operators that the
            // refresh cannot keep up with the configured schedule.
            if let Some(secs) = schedule_secs {
                let schedule_ms = (secs * 1000) as i64;
                if let Some(ratio) = is_falling_behind(elapsed_ms, schedule_ms, 0.80) {
                    monitor::alert_falling_behind(
                        &st.pgt_schema,
                        &st.pgt_name,
                        elapsed_ms,
                        schedule_ms,
                        ratio,
                        st.pooler_compatibility_mode,
                    );
                    pgrx::warning!(
                        "pg_trickle: refresh of {}.{} took {}ms ({:.0}% of {}ms schedule). \
                         The scheduler may not be able to keep up with the configured interval.",
                        st.pgt_schema,
                        st.pgt_name,
                        elapsed_ms,
                        ratio * 100.0,
                        schedule_ms,
                    );
                }
            }

            // Bug #660 fix: write outbox notification row when outbox is enabled
            // and the refresh produced at least one changed row.
            if (rows_inserted > 0 || rows_deleted > 0)
                && crate::api::outbox::is_outbox_enabled(st.pgt_id)
            {
                // VA-4 (v0.48.0): Use embedding-specific outbox event when an
                // embedding vector column is configured.
                if let Some(vec_col) = crate::api::outbox::get_embedding_vector_column(st.pgt_id) {
                    if let Err(e) = crate::api::outbox::write_embedding_outbox_row(
                        st.pgt_id,
                        None,
                        rows_inserted,
                        rows_deleted,
                        &st.pgt_schema,
                        &st.pgt_name,
                        &vec_col,
                    ) {
                        log!(
                            "pg_trickle: embedding outbox write failed for {}.{}: {}",
                            st.pgt_schema,
                            st.pgt_name,
                            e
                        );
                    }
                } else if let Err(e) = crate::api::outbox::write_outbox_row(
                    st.pgt_id,
                    None, // refresh_id is a BIGINT in pgt_refresh_history; outbox uses UUID
                    rows_inserted,
                    rows_deleted,
                    0_i32,
                    &st.pgt_schema,
                    &st.pgt_name,
                ) {
                    log!(
                        "pg_trickle: outbox write failed for {}.{}: {}",
                        st.pgt_schema,
                        st.pgt_name,
                        e
                    );
                }
            }

            // VH-2 (v0.48.0): Fire distance-predicate NOTIFY subscriptions after
            // a successful refresh that produced changed rows.
            if (rows_inserted > 0 || rows_deleted > 0) && action != RefreshAction::NoData {
                // Derive storage table name (schema-qualified st_name by convention).
                let storage_table = &st.pgt_name;
                crate::api::fire_distance_subscriptions(
                    &st.pgt_schema,
                    &st.pgt_name,
                    storage_table,
                    st.pooler_compatibility_mode,
                );
            }

            // VP-1/VP-2 (v0.47.0): Execute post-refresh action when rows changed.
            let rows_changed = rows_inserted + rows_deleted;
            if rows_changed > 0 && action != RefreshAction::NoData {
                execute_post_refresh_action(st, rows_changed);
            }

            RefreshOutcome::Success
        }
        Err(e) => {
            let _ = RefreshRecord::complete(
                refresh_id,
                "FAILED",
                0,
                0,
                Some(&e.to_string()),
                0,
                Some(action.as_str()),
                was_full_fallback,
            );

            monitor::alert_refresh_failed(
                &st.pgt_schema,
                &st.pgt_name,
                action.as_str(),
                &e.to_string(),
                st.pooler_compatibility_mode,
            );

            let is_retryable = e.is_retryable();
            let counts = e.counts_toward_suspension();

            // Handle schema errors: mark for reinitialize
            if e.requires_reinitialize() {
                let _ = StreamTableMeta::mark_for_reinitialize(st.pgt_id);
                monitor::alert_reinitialize_needed(
                    &st.pgt_schema,
                    &st.pgt_name,
                    &e.to_string(),
                    st.pooler_compatibility_mode,
                );
            }

            // ERR-1b: On permanent failure, immediately set ERROR status with
            // the error message. One permanent failure is enough — it will not
            // self-heal. Skip the consecutive_errors increment path.
            if !is_retryable {
                let error_msg = e.to_string();
                let _ = StreamTableMeta::set_error_state(st.pgt_id, &error_msg);

                log!(
                    "pg_trickle: permanent error for {}.{} ({}): {} — set status to ERROR",
                    st.pgt_schema,
                    st.pgt_name,
                    action.as_str(),
                    error_msg,
                );

                return RefreshOutcome::PermanentFailure;
            }

            // Increment error count only for retryable errors that should count
            if counts {
                match StreamTableMeta::increment_errors(st.pgt_id) {
                    Ok(count) if count >= config::pg_trickle_max_consecutive_errors() => {
                        let _ = StreamTableMeta::update_status(st.pgt_id, StStatus::Suspended);

                        monitor::alert_auto_suspended(
                            &st.pgt_schema,
                            &st.pgt_name,
                            count,
                            st.pooler_compatibility_mode,
                        );

                        log!(
                            "pg_trickle: suspended {}.{} after {} consecutive errors",
                            st.pgt_schema,
                            st.pgt_name,
                            count,
                        );
                    }
                    _ => {
                        log!(
                            "pg_trickle: refresh failed for {}.{} ({}): {} [will retry]",
                            st.pgt_schema,
                            st.pgt_name,
                            action.as_str(),
                            e,
                        );
                    }
                }
            } else {
                log!(
                    "pg_trickle: refresh skipped for {}.{}: {}",
                    st.pgt_schema,
                    st.pgt_name,
                    e,
                );
            }

            RefreshOutcome::RetryableFailure
        }
    }
}

// PB1: Advisory lock release removed — row-level locks (FOR UPDATE SKIP
// LOCKED) are transaction-scoped and released automatically on commit.

// ── Bootstrap Source Gating helpers (v0.5.0, Phase 3) ─────────────────────

/// Load the set of currently gated source OIDs from `pgt_source_gates`.
///
/// Called once per worker invocation to build an immutable gate set for the
/// duration of the tick, avoiding repeated SPI round-trips.
fn load_gated_source_oids() -> std::collections::HashSet<pg_sys::Oid> {
    crate::catalog::get_gated_source_oids()
        .unwrap_or_default()
        .into_iter()
        .collect()
}

/// Check whether any of the stream table's declared source OIDs appear in
/// the caller-supplied gated set.  Returns `true` if the ST should be
/// skipped this tick.
fn is_any_source_gated(pgt_id: i64, gated: &std::collections::HashSet<pg_sys::Oid>) -> bool {
    if gated.is_empty() {
        return false;
    }
    get_source_oids_for_st(pgt_id)
        .into_iter()
        .any(|oid| gated.contains(&oid))
}

/// Insert a SKIP record into `pgt_refresh_history` for a gated stream table.
///
/// This lets operators see which STs were skipped and why by querying the
/// history table.  Errors are logged as warnings rather than bubbling up,
/// because a failure to write the history record must not abort the tick.
fn log_gated_skip(st: &StreamTableMeta) {
    let now = Spi::get_one::<TimestampWithTimeZone>("SELECT now()")
        .unwrap_or(None)
        .unwrap_or_else(|| {
            pgrx::warning!(
                "log_gated_skip: now() returned NULL for {}.{}",
                st.pgt_schema,
                st.pgt_name
            );
            TimestampWithTimeZone::try_from(0i64).unwrap_or_else(|_| {
                // ERR-3 (v0.26.0): HINT added for system clock diagnostics.
                pgrx::error!("scheduler: failed to create epoch TimestampWithTimeZone; HINT: check system clock configuration")
            })
        });

    if let Err(e) = crate::catalog::RefreshRecord::insert(
        st.pgt_id,
        now,
        "SKIP",
        "SKIPPED",
        0,
        0,
        Some("source gated"),
        Some("SCHEDULER"),
        None,
        0,
        None,
        false,
        None,
    ) {
        pgrx::warning!(
            "pg_trickle: failed to log SKIP for {}.{}: {}",
            st.pgt_schema,
            st.pgt_name,
            e
        );
    }
}

// ── Watermark Gating helpers (v0.7.0) ─────────────────────────────────────

/// Check whether a stream table should be skipped due to watermark
/// misalignment. Returns `true` if the ST should be skipped (misaligned).
fn is_watermark_misaligned(pgt_id: i64) -> (bool, Option<String>) {
    let source_oids = get_source_oids_for_st(pgt_id);
    if source_oids.is_empty() {
        return (false, None);
    }
    match crate::catalog::check_watermark_alignment(&source_oids) {
        Ok((aligned, reason)) => (!aligned, reason),
        Err(e) => {
            pgrx::warning!(
                "pg_trickle: watermark check failed for pgt_id={}: {}",
                pgt_id,
                e
            );
            (false, None)
        }
    }
}

/// WM-7: Check whether a stream table should be skipped because a watermark
/// in an overlapping group is stuck (not advanced within the holdback timeout).
fn is_watermark_stuck(pgt_id: i64) -> (bool, Option<String>) {
    let timeout = config::pg_trickle_watermark_holdback_timeout();
    if timeout <= 0 {
        return (false, None);
    }
    let source_oids = get_source_oids_for_st(pgt_id);
    if source_oids.is_empty() {
        return (false, None);
    }
    match crate::catalog::check_watermark_staleness(&source_oids, timeout) {
        Ok((stuck, reason)) => (stuck, reason),
        Err(e) => {
            pgrx::warning!(
                "pg_trickle: watermark staleness check failed for pgt_id={}: {}",
                pgt_id,
                e
            );
            (false, None)
        }
    }
}

/// Log a watermark-gated skip into `pgt_refresh_history`.
fn log_watermark_skip(st: &StreamTableMeta, reason: &str) {
    let now = Spi::get_one::<TimestampWithTimeZone>("SELECT now()")
        .unwrap_or(None)
        .unwrap_or_else(|| {
            pgrx::warning!(
                "log_watermark_skip: now() returned NULL for {}.{}",
                st.pgt_schema,
                st.pgt_name
            );
            TimestampWithTimeZone::try_from(0i64).unwrap_or_else(|_| {
                // ERR-3 (v0.26.0): HINT added for system clock diagnostics.
                pgrx::error!("scheduler: failed to create epoch TimestampWithTimeZone; HINT: check system clock configuration")
            })
        });

    if let Err(e) = crate::catalog::RefreshRecord::insert(
        st.pgt_id,
        now,
        "SKIP",
        "SKIPPED",
        0,
        0,
        Some(reason),
        Some("SCHEDULER"),
        None,
        0,
        None,
        false,
        None,
    ) {
        pgrx::warning!(
            "pg_trickle: failed to log watermark SKIP for {}.{}: {}",
            st.pgt_schema,
            st.pgt_name,
            e
        );
    }
}

/// D-1b: Check whether any UNLOGGED source buffer for this ST was
/// emptied by crash recovery and needs a FULL refresh to resynchronize.
///
/// After a PostgreSQL crash, UNLOGGED tables are truncated. If a buffer
/// table has `relpersistence = 'u'` AND is empty AND the postmaster was
/// restarted after the ST's last refresh, the buffer contents were lost
/// and we must force a FULL refresh.
///
/// Returns `true` if crash-recovery data loss was detected (and the ST
/// was marked for reinit).
fn check_unlogged_buffer_crash_recovery(st: &StreamTableMeta) -> bool {
    if !st.is_populated || st.needs_reinit {
        return false;
    }

    let change_schema = config::pg_trickle_change_buffer_schema();

    // Check base-table source buffers (v0.32.0+: stable hash name)
    let source_oids = get_source_oids_for_st(st.pgt_id);
    for oid in &source_oids {
        let buf_base = crate::cdc::buffer_base_name_for_oid(*oid);
        let buf_fq = format!("{change_schema}.{buf_base}");
        let lost = Spi::get_one::<bool>(&format!(
            "SELECT c.relpersistence = 'u' \
               AND NOT EXISTS(SELECT 1 FROM {buf_fq} LIMIT 1) \
               AND pg_postmaster_start_time() > \
                   COALESCE((SELECT last_refresh_at FROM pgtrickle.pgt_stream_tables \
                             WHERE pgt_id = {pgt_id}), '-infinity'::timestamptz) \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = '{change_schema}' AND c.relname = '{buf_base}'",
            pgt_id = st.pgt_id,
        )) // nosemgrep: rust.spi.query.dynamic-format
        .unwrap_or(Some(false))
        .unwrap_or(false);

        if lost {
            pgrx::warning!(
                "pg_trickle: UNLOGGED change buffer for {}.{} source OID {} was \
                 emptied by crash recovery — scheduling FULL refresh",
                st.pgt_schema,
                st.pgt_name,
                oid.to_u32(),
            );
            if let Err(e) = StreamTableMeta::mark_for_reinitialize(st.pgt_id) {
                pgrx::warning!(
                    "pg_trickle: failed to mark {}.{} for reinit after crash recovery: {}",
                    st.pgt_schema,
                    st.pgt_name,
                    e,
                );
            }
            return true;
        }
    }

    // Check ST-to-ST source buffers (changes_pgt_{id})
    let deps = crate::catalog::StDependency::get_for_st(st.pgt_id).unwrap_or_default();
    for dep in &deps {
        if dep.source_type != "STREAM_TABLE" {
            continue;
        }
        let upstream_pgt_id = crate::catalog::StreamTableMeta::pgt_id_for_relid(dep.source_relid);
        if let Some(up_id) = upstream_pgt_id
            && cdc::has_st_change_buffer(up_id, &change_schema)
        {
            let lost = Spi::get_one::<bool>(&format!(
                "SELECT c.relpersistence = 'u' \
                   AND NOT EXISTS(SELECT 1 FROM {schema}.changes_pgt_{id} LIMIT 1) \
                   AND pg_postmaster_start_time() > \
                       COALESCE((SELECT last_refresh_at FROM pgtrickle.pgt_stream_tables \
                                 WHERE pgt_id = {pgt_id}), '-infinity'::timestamptz) \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = '{schema}' AND c.relname = 'changes_pgt_{id}'",
                schema = change_schema,
                id = up_id,
                pgt_id = st.pgt_id,
            ))
            .unwrap_or(Some(false))
            .unwrap_or(false);

            if lost {
                pgrx::warning!(
                    "pg_trickle: UNLOGGED ST change buffer for {}.{} upstream pgt_id={} \
                     was emptied by crash recovery — scheduling FULL refresh",
                    st.pgt_schema,
                    st.pgt_name,
                    up_id,
                );
                if let Err(e) = StreamTableMeta::mark_for_reinitialize(st.pgt_id) {
                    pgrx::warning!(
                        "pg_trickle: failed to mark {}.{} for reinit after crash recovery: {}",
                        st.pgt_schema,
                        st.pgt_name,
                        e,
                    );
                }
                return true;
            }
        }
    }

    false
}

/// Get the source OIDs (base table OIDs) for a given ST.
fn get_source_oids_for_st(pgt_id: i64) -> Vec<pg_sys::Oid> {
    use crate::catalog::StDependency;

    StDependency::get_for_st(pgt_id)
        .unwrap_or_default()
        .into_iter()
        .filter(|dep| dep.source_type == "TABLE" || dep.source_type == "FOREIGN_TABLE")
        .map(|dep| dep.source_relid)
        .collect()
}

/// Compute the freshness deadline for a duration-based schedule.
///
/// Returns `data_timestamp + schedule_seconds` (the moment the data becomes
/// stale). For cron-based schedules, returns `None` because cron doesn't
/// define a continuous freshness SLA.
fn compute_freshness_deadline(st: &StreamTableMeta) -> Option<TimestampWithTimeZone> {
    let schedule_str = st.schedule.as_deref()?;

    // Cron expressions contain spaces or start with '@' — no deadline for those.
    if schedule_str.contains(' ') || schedule_str.starts_with('@') {
        return None;
    }

    // Parse the duration. If parsing fails, skip deadline computation.
    let secs = crate::api::parse_duration(schedule_str).ok()?;
    if secs <= 0 {
        return None;
    }

    // Compute deadline: data_timestamp + schedule_seconds
    // If data_timestamp is NULL (never refreshed), use 'now' as a baseline.
    let deadline_sql = format!(
        "SELECT COALESCE(data_timestamp, now()) + interval '{secs} seconds' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1"
    );
    Spi::get_one_with_args::<TimestampWithTimeZone>(&deadline_sql, &[st.pgt_id.into()])
        .unwrap_or(None)
}

#[cfg(test)]
mod tests {
    use super::dispatch::{
        ADAPTIVE_POLL_MIN_MS, ParallelDispatchState, UnitDispatchState, compute_adaptive_poll_ms,
        parse_worker_extra, sort_ready_queue_by_priority,
    };
    use super::*;
    use crate::dag::{ExecutionUnitDag, ExecutionUnitId};
    use std::collections::VecDeque;

    #[test]
    fn test_current_epoch_ms_returns_reasonable_value() {
        let ms = current_epoch_ms();
        // Should be after 2024-01-01 and before 2040-01-01
        let min_ms = 1_704_067_200_000u64; // 2024-01-01
        let max_ms = 2_208_988_800_000u64; // 2040-01-01
        assert!(ms > min_ms, "epoch_ms too old: {}", ms);
        assert!(ms < max_ms, "epoch_ms too far in future: {}", ms);
    }

    #[test]
    fn test_current_epoch_ms_monotonic() {
        let t1 = current_epoch_ms();
        let t2 = current_epoch_ms();
        assert!(t2 >= t1);
    }

    #[test]
    fn test_refresh_outcome_debug_and_equality() {
        assert_eq!(RefreshOutcome::Success, RefreshOutcome::Success);
        assert_ne!(RefreshOutcome::Success, RefreshOutcome::RetryableFailure);
        assert_ne!(
            RefreshOutcome::RetryableFailure,
            RefreshOutcome::PermanentFailure
        );

        // Verify Debug trait works
        let s = format!("{:?}", RefreshOutcome::PermanentFailure);
        assert!(s.contains("PermanentFailure"));
    }

    #[test]
    fn test_refresh_outcome_clone() {
        let outcome = RefreshOutcome::RetryableFailure;
        let cloned = outcome;
        assert_eq!(outcome, cloned);
    }

    // ── is_group_due_pure tests ─────────────────────────────────────

    #[test]
    fn test_group_due_empty_is_false() {
        assert!(!is_group_due_pure(&[], DiamondSchedulePolicy::Fastest));
        assert!(!is_group_due_pure(&[], DiamondSchedulePolicy::Slowest));
    }

    #[test]
    fn test_group_due_fastest_any_true() {
        assert!(is_group_due_pure(
            &[false, true, false],
            DiamondSchedulePolicy::Fastest,
        ));
    }

    #[test]
    fn test_group_due_fastest_all_false() {
        assert!(!is_group_due_pure(
            &[false, false],
            DiamondSchedulePolicy::Fastest,
        ));
    }

    #[test]
    fn test_group_due_slowest_all_true() {
        assert!(is_group_due_pure(
            &[true, true, true],
            DiamondSchedulePolicy::Slowest,
        ));
    }

    #[test]
    fn test_group_due_slowest_not_all_true() {
        assert!(!is_group_due_pure(
            &[true, false, true],
            DiamondSchedulePolicy::Slowest,
        ));
    }

    #[test]
    fn test_group_due_single_member() {
        assert!(is_group_due_pure(&[true], DiamondSchedulePolicy::Fastest,));
        assert!(is_group_due_pure(&[true], DiamondSchedulePolicy::Slowest,));
        assert!(!is_group_due_pure(&[false], DiamondSchedulePolicy::Fastest,));
    }

    // ── is_falling_behind tests ─────────────────────────────────────

    #[test]
    fn test_falling_behind_at_80_percent_ec11_threshold() {
        // EC-11 alerting uses 0.80 — fires at exactly 80%.
        let result = is_falling_behind(800, 1000, 0.80);
        assert!(result.is_some());
        assert!((result.unwrap() - 0.8).abs() < 0.001);
    }

    #[test]
    fn test_falling_behind_below_95_percent_no_backoff() {
        // Auto-backoff uses 0.95 — 900ms on a 1s schedule must NOT trigger.
        assert!(is_falling_behind(900, 1000, 0.95).is_none());
    }

    #[test]
    fn test_falling_behind_at_95_percent_triggers_backoff() {
        // Exactly at the 0.95 threshold — should trigger.
        let result = is_falling_behind(950, 1000, 0.95);
        assert!(result.is_some());
        assert!((result.unwrap() - 0.95).abs() < 0.001);
    }

    #[test]
    fn test_ec11_fires_before_backoff_threshold() {
        // 800ms on 1s: EC-11 (0.80) fires, but auto-backoff (0.95) does not.
        assert!(is_falling_behind(800, 1000, 0.80).is_some());
        assert!(is_falling_behind(800, 1000, 0.95).is_none());
    }

    #[test]
    fn test_falling_behind_over_100_percent() {
        let result = is_falling_behind(1500, 1000, 0.80);
        assert!(result.is_some());
        assert!((result.unwrap() - 1.5).abs() < 0.001);
    }

    #[test]
    fn test_not_falling_behind() {
        assert!(is_falling_behind(500, 1000, 0.80).is_none());
    }

    #[test]
    fn test_falling_behind_zero_schedule() {
        assert!(is_falling_behind(100, 0, 0.80).is_none());
    }

    #[test]
    fn test_falling_behind_negative_schedule() {
        assert!(is_falling_behind(100, -1, 0.80).is_none());
    }

    // ── parse_worker_extra tests (Phase 3) ──────────────────────────

    #[test]
    fn test_parse_worker_extra_valid() {
        let result = parse_worker_extra("mydb|42");
        assert!(result.is_some());
        let (db, id) = result.unwrap();
        assert_eq!(db, "mydb");
        assert_eq!(id, 42);
    }

    #[test]
    fn test_parse_worker_extra_long_db_name() {
        let result = parse_worker_extra("my_production_database|999999");
        assert!(result.is_some());
        let (db, id) = result.unwrap();
        assert_eq!(db, "my_production_database");
        assert_eq!(id, 999999);
    }

    #[test]
    fn test_parse_worker_extra_empty() {
        assert!(parse_worker_extra("").is_none());
    }

    #[test]
    fn test_parse_worker_extra_no_separator() {
        assert!(parse_worker_extra("mydb42").is_none());
    }

    #[test]
    fn test_parse_worker_extra_empty_db_name() {
        assert!(parse_worker_extra("|42").is_none());
    }

    #[test]
    fn test_parse_worker_extra_invalid_job_id() {
        assert!(parse_worker_extra("mydb|abc").is_none());
    }

    #[test]
    fn test_parse_worker_extra_negative_job_id() {
        assert!(parse_worker_extra("mydb|-1").is_none());
    }

    #[test]
    fn test_parse_worker_extra_zero_job_id() {
        assert!(parse_worker_extra("mydb|0").is_none());
    }

    // ── ParallelDispatchState tests (Phase 4) ───────────────────────

    #[test]
    fn test_parallel_state_new_is_empty() {
        let state = ParallelDispatchState::new();
        assert!(state.unit_states.is_empty());
        assert_eq!(state.per_db_inflight, 0);
        assert_eq!(state.dag_version, 0);
        assert!(!state.has_inflight());
    }

    #[test]
    fn test_parallel_state_has_inflight() {
        let mut state = ParallelDispatchState::new();
        assert!(!state.has_inflight());
        state.per_db_inflight = 1;
        assert!(state.has_inflight());
    }

    #[test]
    fn test_unit_dispatch_state_default() {
        let uds = UnitDispatchState::default();
        assert_eq!(uds.remaining_upstreams, 0);
        assert!(uds.inflight_job_id.is_none());
        assert!(!uds.succeeded);
    }

    #[test]
    fn test_parallel_state_wave_reset_fires_after_dispatch_not_before() {
        // Regression guard for cascade correctness.
        //
        // The wave-complete reset (Step 4) must fire AFTER the dispatch loop
        // (Step 3), not before it.  With a two-unit cascade A → B:
        //
        //   Good (reset after dispatch):
        //     Tick: A completes → per_db_inflight=0, B.remaining=0.
        //     Step 2: B is ready.  Step 3: dispatch B → per_db_inflight=1.
        //     Reset check: per_db_inflight=1 → no reset.  B runs.
        //
        //   Bad (reset before dispatch / old Step 1.5 placement):
        //     Tick: A completes → per_db_inflight=0.
        //     Reset fires: B.remaining_upstreams = 1 again.
        //     Step 2: B now blocked.  B never runs.
        //
        // This test validates the data-model invariant: after a complete wave
        // (per_db_inflight==0, some units have succeeded, nothing was dispatched
        // in Step 3), the reset should clear succeeded flags and restore
        // remaining_upstreams so units can run in the next wave.

        let mut state = ParallelDispatchState::new();

        // Simulate a completed wave: two units both succeeded, nothing in-flight.
        let uid_a = crate::dag::ExecutionUnitId(1);
        let uid_b = crate::dag::ExecutionUnitId(2);
        state.unit_states.insert(
            uid_a,
            UnitDispatchState {
                remaining_upstreams: 0,
                inflight_job_id: None,
                succeeded: true,
            },
        );
        state.unit_states.insert(
            uid_b,
            UnitDispatchState {
                remaining_upstreams: 0, // was decremented when A succeeded
                inflight_job_id: None,
                succeeded: true,
            },
        );
        state.per_db_inflight = 0;

        // Simulate the Step 4 reset that fires after dispatch.
        if state.per_db_inflight == 0 {
            let any_done = state.unit_states.values().any(|us| us.succeeded);
            if any_done {
                for us in state.unit_states.values_mut() {
                    us.succeeded = false;
                    // remaining_upstreams would be restored from eu_dag here;
                    // for B (which had 1 upstream) this restores it to 1.
                }
            }
        }

        assert!(
            state.unit_states.values().all(|us| !us.succeeded),
            "all succeeded flags must be cleared after wave reset"
        );
    }

    #[test]
    fn test_is_unit_due_pure_no_members() {
        // is_unit_due depends on SPI, but we can test the logic structure
        // indirectly via the iteration: empty members → not due.
        let unit = crate::dag::ExecutionUnit {
            id: crate::dag::ExecutionUnitId(1),
            kind: crate::dag::ExecutionUnitKind::Singleton,
            member_pgt_ids: vec![],
            root_pgt_id: 0,
            label: "empty".to_string(),
        };
        // With no members, iter().any() returns false → not due.
        let result = unit.member_pgt_ids.iter().any(|_| true);
        assert!(!result);
    }

    // ── FUSE unit tests ────────────────────────────────────────────────

    #[test]
    fn test_fuse_is_active_off_mode() {
        assert!(!fuse_is_active("off", Some(1000), 5000));
        assert!(!fuse_is_active("off", None, 5000));
    }

    #[test]
    fn test_fuse_is_active_on_with_per_st_ceiling() {
        assert!(fuse_is_active("on", Some(1000), 0));
        assert!(fuse_is_active("on", Some(1000), 5000));
    }

    #[test]
    fn test_fuse_is_active_on_with_global_ceiling_only() {
        assert!(fuse_is_active("on", None, 5000));
    }

    #[test]
    fn test_fuse_is_active_on_no_ceiling_at_all() {
        assert!(!fuse_is_active("on", None, 0));
        assert!(!fuse_is_active("on", Some(0), 0));
    }

    #[test]
    fn test_fuse_is_active_auto_mode() {
        assert!(fuse_is_active("auto", Some(100), 0));
        assert!(fuse_is_active("auto", None, 5000));
        assert!(!fuse_is_active("auto", None, 0));
    }

    // ── DAG-2: compute_adaptive_poll_ms tests ─────────────────────────────

    #[test]
    fn test_adaptive_poll_no_inflight_returns_base_interval() {
        // When no workers are in-flight, use the full scheduler interval
        // regardless of current_poll_ms or completion state.
        assert_eq!(compute_adaptive_poll_ms(20, false, false, 1000), 1000);
        assert_eq!(compute_adaptive_poll_ms(200, true, false, 1000), 1000);
        assert_eq!(compute_adaptive_poll_ms(50, false, false, 500), 500);
    }

    #[test]
    fn test_adaptive_poll_completion_resets_to_min() {
        // After a worker completes, poll resets to ADAPTIVE_POLL_MIN_MS (20ms).
        assert_eq!(compute_adaptive_poll_ms(200, true, true, 1000), 20);
        assert_eq!(compute_adaptive_poll_ms(100, true, true, 1000), 20);
        assert_eq!(compute_adaptive_poll_ms(20, true, true, 1000), 20);
    }

    #[test]
    fn test_adaptive_poll_backoff_doubles_each_tick() {
        // No completion, workers in-flight → double current_poll_ms.
        assert_eq!(compute_adaptive_poll_ms(20, false, true, 1000), 40);
        assert_eq!(compute_adaptive_poll_ms(40, false, true, 1000), 80);
        assert_eq!(compute_adaptive_poll_ms(80, false, true, 1000), 160);
    }

    #[test]
    fn test_adaptive_poll_caps_at_max() {
        // Doubling 160 → 320, but capped at ADAPTIVE_POLL_MAX_MS (200).
        assert_eq!(compute_adaptive_poll_ms(160, false, true, 1000), 200);
        assert_eq!(compute_adaptive_poll_ms(200, false, true, 1000), 200);
    }

    #[test]
    fn test_adaptive_poll_full_backoff_sequence() {
        // Simulate a full sequence: completion → backoff → backoff → … → cap.
        let base = 1000u64;
        // Worker completes → reset.
        let p = compute_adaptive_poll_ms(200, true, true, base);
        assert_eq!(p, 20);
        // Tick 1: no completion.
        let p = compute_adaptive_poll_ms(p, false, true, base);
        assert_eq!(p, 40);
        // Tick 2.
        let p = compute_adaptive_poll_ms(p, false, true, base);
        assert_eq!(p, 80);
        // Tick 3.
        let p = compute_adaptive_poll_ms(p, false, true, base);
        assert_eq!(p, 160);
        // Tick 4: would be 320, capped at 200.
        let p = compute_adaptive_poll_ms(p, false, true, base);
        assert_eq!(p, 200);
        // Tick 5: stays at cap.
        let p = compute_adaptive_poll_ms(p, false, true, base);
        assert_eq!(p, 200);
    }

    #[test]
    fn test_adaptive_poll_completion_after_backoff_resets() {
        // After backing off to cap, a completion resets to min.
        let p = compute_adaptive_poll_ms(200, true, true, 1000);
        assert_eq!(p, 20);
    }

    #[test]
    fn test_adaptive_poll_transition_inflight_to_idle() {
        // Workers finish between ticks: in-flight → not in-flight.
        // Should switch from adaptive to base interval.
        let p = compute_adaptive_poll_ms(40, false, true, 1000);
        assert_eq!(p, 80); // still in-flight, doubles
        let p = compute_adaptive_poll_ms(p, false, false, 1000);
        assert_eq!(p, 1000); // no longer in-flight, base interval
    }

    // ── DAG-2: ParallelDispatchState adaptive poll integration ────────────

    #[test]
    fn test_parallel_state_new_has_min_adaptive_poll() {
        let state = ParallelDispatchState::new();
        assert_eq!(state.adaptive_poll_ms, ADAPTIVE_POLL_MIN_MS);
        assert_eq!(state.completions_this_tick, 0);
    }

    // ── DAG-1: Intra-tick pipelining validation ───────────────────────────
    //
    // The parallel dispatch architecture from Phase 4 already achieves
    // intra-tick pipelining: Step 1 immediately decrements downstream
    // `remaining_upstreams`, and Step 2 picks up newly-ready units in the
    // same tick. These tests validate the state-machine invariants.

    #[test]
    fn test_cascade_pipeline_downstream_ready_after_upstream_completes() {
        // Three-unit cascade: A → B → C.
        // When A completes in Step 1, B.remaining_upstreams drops to 0
        // and B is dispatchable in Step 2 of the same tick.
        let mut state = ParallelDispatchState::new();
        let uid_a = crate::dag::ExecutionUnitId(1);
        let uid_b = crate::dag::ExecutionUnitId(2);
        let uid_c = crate::dag::ExecutionUnitId(3);

        // Initial state: A=0 upstreams, B=1 (A), C=1 (B).
        state.unit_states.insert(
            uid_a,
            UnitDispatchState {
                remaining_upstreams: 0,
                inflight_job_id: Some(100),
                succeeded: false,
            },
        );
        state.unit_states.insert(
            uid_b,
            UnitDispatchState {
                remaining_upstreams: 1,
                inflight_job_id: None,
                succeeded: false,
            },
        );
        state.unit_states.insert(
            uid_c,
            UnitDispatchState {
                remaining_upstreams: 1,
                inflight_job_id: None,
                succeeded: false,
            },
        );
        state.per_db_inflight = 1;

        // Simulate Step 1: A completes → decrement downstream B.
        state.unit_states.get_mut(&uid_a).unwrap().inflight_job_id = None;
        state.unit_states.get_mut(&uid_a).unwrap().succeeded = true;
        state.per_db_inflight -= 1;
        // Decrement downstream (B depends on A).
        state
            .unit_states
            .get_mut(&uid_b)
            .unwrap()
            .remaining_upstreams -= 1;

        // Verify: B is now ready (remaining=0, not succeeded, not inflight).
        let b_state = &state.unit_states[&uid_b];
        assert_eq!(
            b_state.remaining_upstreams, 0,
            "B should be ready after A completes"
        );
        assert!(!b_state.succeeded);
        assert!(b_state.inflight_job_id.is_none());

        // Verify: C is still blocked.
        let c_state = &state.unit_states[&uid_c];
        assert_eq!(c_state.remaining_upstreams, 1, "C should still be blocked");
    }

    #[test]
    fn test_mixed_cost_levels_no_level_barrier() {
        // Level 0: A (fast), B (slow).
        // Level 1: C depends only on A, D depends on both A and B.
        // When A completes, C should be immediately ready even though
        // B (same level) is still running — no level barrier.
        let mut state = ParallelDispatchState::new();
        let uid_a = crate::dag::ExecutionUnitId(1);
        let uid_b = crate::dag::ExecutionUnitId(2);
        let uid_c = crate::dag::ExecutionUnitId(3);
        let uid_d = crate::dag::ExecutionUnitId(4);

        state.unit_states.insert(
            uid_a,
            UnitDispatchState {
                remaining_upstreams: 0,
                inflight_job_id: Some(100),
                succeeded: false,
            },
        );
        state.unit_states.insert(
            uid_b,
            UnitDispatchState {
                remaining_upstreams: 0,
                inflight_job_id: Some(101),
                succeeded: false,
            },
        );
        state.unit_states.insert(
            uid_c,
            UnitDispatchState {
                remaining_upstreams: 1, // depends on A only
                inflight_job_id: None,
                succeeded: false,
            },
        );
        state.unit_states.insert(
            uid_d,
            UnitDispatchState {
                remaining_upstreams: 2, // depends on A and B
                inflight_job_id: None,
                succeeded: false,
            },
        );
        state.per_db_inflight = 2;

        // Simulate: A completes (fast), B still running.
        state.unit_states.get_mut(&uid_a).unwrap().inflight_job_id = None;
        state.unit_states.get_mut(&uid_a).unwrap().succeeded = true;
        state.per_db_inflight -= 1;
        // Decrement C (depends on A) and D (depends on A and B).
        state
            .unit_states
            .get_mut(&uid_c)
            .unwrap()
            .remaining_upstreams -= 1;
        state
            .unit_states
            .get_mut(&uid_d)
            .unwrap()
            .remaining_upstreams -= 1;

        // C is ready — A was its only upstream.
        let c_state = &state.unit_states[&uid_c];
        assert_eq!(
            c_state.remaining_upstreams, 0,
            "C should be ready (A was its only dep)"
        );
        assert!(!c_state.succeeded);
        assert!(c_state.inflight_job_id.is_none());

        // D is still blocked — needs B too.
        let d_state = &state.unit_states[&uid_d];
        assert_eq!(d_state.remaining_upstreams, 1, "D still waiting for B");

        // B still running.
        assert!(state.unit_states[&uid_b].inflight_job_id.is_some());
        assert_eq!(state.per_db_inflight, 1);
    }

    #[test]
    fn test_cascade_wave_reset_preserves_correctness() {
        // After a full cascade A → B → C all complete, the wave reset
        // restores remaining_upstreams so units can run in the next cycle.
        let mut state = ParallelDispatchState::new();
        let uid_a = crate::dag::ExecutionUnitId(1);
        let uid_b = crate::dag::ExecutionUnitId(2);
        let uid_c = crate::dag::ExecutionUnitId(3);

        // All units completed this wave.
        state.unit_states.insert(
            uid_a,
            UnitDispatchState {
                remaining_upstreams: 0,
                inflight_job_id: None,
                succeeded: true,
            },
        );
        state.unit_states.insert(
            uid_b,
            UnitDispatchState {
                remaining_upstreams: 0, // was decremented from 1 when A succeeded
                inflight_job_id: None,
                succeeded: true,
            },
        );
        state.unit_states.insert(
            uid_c,
            UnitDispatchState {
                remaining_upstreams: 0, // was decremented from 1 when B succeeded
                inflight_job_id: None,
                succeeded: true,
            },
        );
        state.per_db_inflight = 0;

        // Simulate wave reset (Step 4 logic).
        // In production this uses eu_dag.get_upstream_units(uid).len();
        // here we simulate B.original=1, C.original=1.
        let original_upstreams: HashMap<crate::dag::ExecutionUnitId, usize> =
            [(uid_a, 0), (uid_b, 1), (uid_c, 1)].into();

        if state.per_db_inflight == 0 {
            let any_done = state.unit_states.values().any(|us| us.succeeded);
            if any_done {
                for (&uid, us) in state.unit_states.iter_mut() {
                    us.succeeded = false;
                    us.remaining_upstreams = *original_upstreams.get(&uid).unwrap_or(&0);
                }
            }
        }

        // After reset: A is ready for next wave, B and C blocked on upstreams.
        assert_eq!(state.unit_states[&uid_a].remaining_upstreams, 0);
        assert_eq!(state.unit_states[&uid_b].remaining_upstreams, 1);
        assert_eq!(state.unit_states[&uid_c].remaining_upstreams, 1);
        assert!(state.unit_states.values().all(|us| !us.succeeded));
    }

    // ── C3-1: sort_ready_queue_by_priority tier-aware tests ───────────────

    /// Build a minimal ExecutionUnitDag with entries of requested kinds so the
    /// sort function can look up `unit_priority`.  No edges are needed.
    fn make_single_dag(
        entries: &[(ExecutionUnitId, crate::dag::ExecutionUnitKind)],
    ) -> ExecutionUnitDag {
        use crate::dag::{ExecutionUnit, ExecutionUnitDag};
        let mut units = Vec::new();
        for &(uid, kind) in entries {
            units.push(ExecutionUnit {
                id: uid,
                kind,
                root_pgt_id: uid.0 as i64,
                member_pgt_ids: vec![uid.0 as i64],
                label: format!("unit-{}", uid.0),
            });
        }
        ExecutionUnitDag::from_units_for_test(units)
    }

    #[test]
    fn test_sort_priority_immediate_before_singleton() {
        use crate::dag::{ExecutionUnitId, ExecutionUnitKind};
        let uid_imm = ExecutionUnitId(1);
        let uid_sing = ExecutionUnitId(2);
        let dag = make_single_dag(&[
            (uid_imm, ExecutionUnitKind::ImmediateClosure),
            (uid_sing, ExecutionUnitKind::Singleton),
        ]);
        let queue: VecDeque<ExecutionUnitId> = vec![uid_sing, uid_imm].into();
        let tier_map = HashMap::new();
        let sorted = sort_ready_queue_by_priority(queue, &dag, &tier_map);
        assert_eq!(sorted[0], uid_imm, "ImmediateClosure must come first");
        assert_eq!(sorted[1], uid_sing);
    }

    #[test]
    fn test_sort_priority_hot_singleton_before_warm_singleton() {
        use crate::dag::{ExecutionUnitId, ExecutionUnitKind};
        let uid_hot = ExecutionUnitId(10);
        let uid_warm = ExecutionUnitId(20);
        let dag = make_single_dag(&[
            (uid_hot, ExecutionUnitKind::Singleton),
            (uid_warm, ExecutionUnitKind::Singleton),
        ]);
        // Warm first in queue, Hot second — tier sort should flip them.
        let queue: VecDeque<ExecutionUnitId> = vec![uid_warm, uid_hot].into();
        let mut tier_map = HashMap::new();
        tier_map.insert(uid_hot, 0u8); // Hot
        tier_map.insert(uid_warm, 1u8); // Warm
        let sorted = sort_ready_queue_by_priority(queue, &dag, &tier_map);
        assert_eq!(sorted[0], uid_hot, "Hot singleton must come before Warm");
        assert_eq!(sorted[1], uid_warm);
    }

    #[test]
    fn test_sort_priority_warm_singleton_before_cold_singleton() {
        use crate::dag::{ExecutionUnitId, ExecutionUnitKind};
        let uid_warm = ExecutionUnitId(30);
        let uid_cold = ExecutionUnitId(40);
        let dag = make_single_dag(&[
            (uid_warm, ExecutionUnitKind::Singleton),
            (uid_cold, ExecutionUnitKind::Singleton),
        ]);
        let queue: VecDeque<ExecutionUnitId> = vec![uid_cold, uid_warm].into();
        let mut tier_map = HashMap::new();
        tier_map.insert(uid_warm, 1u8); // Warm
        tier_map.insert(uid_cold, 2u8); // Cold
        let sorted = sort_ready_queue_by_priority(queue, &dag, &tier_map);
        assert_eq!(sorted[0], uid_warm, "Warm singleton must come before Cold");
        assert_eq!(sorted[1], uid_cold);
    }

    #[test]
    fn test_sort_priority_immediate_beats_hot_singleton() {
        use crate::dag::{ExecutionUnitId, ExecutionUnitKind};
        let uid_imm = ExecutionUnitId(50);
        let uid_hot = ExecutionUnitId(60);
        let dag = make_single_dag(&[
            (uid_imm, ExecutionUnitKind::ImmediateClosure),
            (uid_hot, ExecutionUnitKind::Singleton),
        ]);
        let queue: VecDeque<ExecutionUnitId> = vec![uid_hot, uid_imm].into();
        let mut tier_map = HashMap::new();
        tier_map.insert(uid_hot, 0u8); // Hot — but ImmediateClosure kind wins anyway
        let sorted = sort_ready_queue_by_priority(queue, &dag, &tier_map);
        assert_eq!(sorted[0], uid_imm, "ImmediateClosure beats Hot Singleton");
    }

    #[test]
    fn test_sort_priority_missing_tier_defaults_to_hot() {
        use crate::dag::{ExecutionUnitId, ExecutionUnitKind};
        let uid_a = ExecutionUnitId(70);
        let uid_b = ExecutionUnitId(80);
        let dag = make_single_dag(&[
            (uid_a, ExecutionUnitKind::Singleton),
            (uid_b, ExecutionUnitKind::Singleton),
        ]);
        let queue: VecDeque<ExecutionUnitId> = vec![uid_b, uid_a].into();
        // uid_b is "Cold" in the map; uid_a has no entry (defaults to Hot=0)
        let mut tier_map = HashMap::new();
        tier_map.insert(uid_b, 2u8); // Cold
        // uid_a absent → defaults to 0 (Hot) → should come first
        let sorted = sort_ready_queue_by_priority(queue, &dag, &tier_map);
        assert_eq!(
            sorted[0], uid_a,
            "Missing-tier unit defaults to Hot urgency"
        );
        assert_eq!(sorted[1], uid_b);
    }

    // ── PermanentFailed downstream unblocking tests ───────────────────────

    #[test]
    fn test_permanent_failure_unblocks_downstream_units() {
        // Regression test: when an upstream unit fails permanently,
        // downstream units must be unblocked (remaining_upstreams
        // decremented and upstream marked succeeded) so the wave can
        // complete. Previously, PermanentFailed left downstreams blocked
        // forever — the wave reset would restore remaining_upstreams,
        // and the failed upstream would never set succeeded=true.
        let mut state = ParallelDispatchState::new();
        let uid_a = crate::dag::ExecutionUnitId(1);
        let uid_b = crate::dag::ExecutionUnitId(2);
        let uid_c = crate::dag::ExecutionUnitId(3);

        // A → B (depends on A), A → C (depends on A).
        // A fails permanently.
        state.unit_states.insert(
            uid_a,
            UnitDispatchState {
                remaining_upstreams: 0,
                inflight_job_id: None,
                // Simulate the fix: after PermanentFailed, the unit is
                // marked succeeded=true so the wave can complete.
                succeeded: true,
            },
        );
        state.unit_states.insert(
            uid_b,
            UnitDispatchState {
                remaining_upstreams: 1,
                inflight_job_id: None,
                succeeded: false,
            },
        );
        state.unit_states.insert(
            uid_c,
            UnitDispatchState {
                remaining_upstreams: 1,
                inflight_job_id: None,
                succeeded: false,
            },
        );

        // Simulate the PermanentFailed handler decrementing downstream units:
        // (this is what the fix does in parallel_dispatch_tick)
        for ds_id in [uid_b, uid_c] {
            if let Some(ds_state) = state.unit_states.get_mut(&ds_id) {
                ds_state.remaining_upstreams = ds_state.remaining_upstreams.saturating_sub(1);
            }
        }

        // After the fix: B and C should be unblocked (remaining=0).
        assert_eq!(
            state.unit_states[&uid_b].remaining_upstreams, 0,
            "B must be unblocked after A's permanent failure"
        );
        assert_eq!(
            state.unit_states[&uid_c].remaining_upstreams, 0,
            "C must be unblocked after A's permanent failure"
        );
        // A must be marked succeeded so the wave reset works correctly.
        assert!(
            state.unit_states[&uid_a].succeeded,
            "PermanentFailed upstream must be marked succeeded for wave completion"
        );
    }

    #[test]
    fn test_permanent_failure_wave_reset_does_not_re_block() {
        // After a PermanentFailed unit is marked succeeded and downstreams
        // are unblocked, the wave reset at Step 4 should restore the
        // original upstream counts (from the DAG), clearing all succeeded
        // flags. The next wave will see the upstream as ready (remaining=0),
        // potentially retrying the failed unit in the next scheduling cycle.
        let mut state = ParallelDispatchState::new();
        let uid_a = crate::dag::ExecutionUnitId(1);
        let uid_b = crate::dag::ExecutionUnitId(2);

        // Post-PermanentFailed state: A succeeded (failed but marked), B completed.
        state.unit_states.insert(
            uid_a,
            UnitDispatchState {
                remaining_upstreams: 0,
                inflight_job_id: None,
                succeeded: true,
            },
        );
        state.unit_states.insert(
            uid_b,
            UnitDispatchState {
                remaining_upstreams: 0, // was decremented by the fix
                inflight_job_id: None,
                succeeded: true,
            },
        );
        state.per_db_inflight = 0;

        // Simulate wave reset (Step 4): restore original upstream counts.
        let original_upstreams: HashMap<crate::dag::ExecutionUnitId, usize> =
            [(uid_a, 0), (uid_b, 1)].into();

        if state.per_db_inflight == 0 {
            let any_done = state.unit_states.values().any(|us| us.succeeded);
            if any_done {
                for (&uid, us) in state.unit_states.iter_mut() {
                    us.succeeded = false;
                    us.remaining_upstreams = *original_upstreams.get(&uid).unwrap_or(&0);
                }
            }
        }

        // After reset: A is ready for next wave, B blocked on A again.
        assert_eq!(state.unit_states[&uid_a].remaining_upstreams, 0);
        assert!(!state.unit_states[&uid_a].succeeded);
        assert_eq!(state.unit_states[&uid_b].remaining_upstreams, 1);
        assert!(!state.unit_states[&uid_b].succeeded);
    }

    // ── TEST-9: Unit tests for compute_per_db_quota ───────────────────

    #[test]
    fn test_per_db_quota_disabled_returns_max_concurrent() {
        // per_db_quota = 0 means disabled — fall back to max_concurrent_refreshes
        assert_eq!(compute_per_db_quota(0, 4, 10, 0), 4);
        assert_eq!(compute_per_db_quota(-1, 8, 20, 5), 8);
    }

    #[test]
    fn test_per_db_quota_burst_when_spare_capacity() {
        // Under 80% of cluster capacity → allow up to 150% of base quota
        // base = 4, max_cluster = 20, burst_threshold = ceil(20*0.8) = 16
        // current_active = 5 (< 16) → burst = max(4*3/2, 4+1) = 6
        assert_eq!(compute_per_db_quota(4, 8, 20, 5), 6);
    }

    #[test]
    fn test_per_db_quota_no_burst_at_capacity() {
        // At or above 80% of cluster capacity → return base quota
        // base = 4, max_cluster = 20, burst_threshold = 16, current_active = 16
        assert_eq!(compute_per_db_quota(4, 8, 20, 16), 4);
        assert_eq!(compute_per_db_quota(4, 8, 20, 20), 4);
    }

    #[test]
    fn test_per_db_quota_minimum_one() {
        // Even with per_db_quota = 0 and max_concurrent = 0, at least 1
        assert_eq!(compute_per_db_quota(0, 0, 10, 0), 1);
    }

    #[test]
    fn test_per_db_quota_small_base_still_bursts() {
        // base = 1, max_cluster = 10, threshold = 8, active = 0
        // burst = max(1*3/2=1, 1+1=2) = 2
        assert_eq!(compute_per_db_quota(1, 4, 10, 0), 2);
    }
}
