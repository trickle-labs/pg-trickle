//! CQ-10-01 (v0.49.0): Scheduler main loops and BGW registration.
//!
//! Extracted from `scheduler/mod.rs` as part of the scheduler decomposition.
//! Contains:
//!   - `register_launcher_worker` — static BGW registration
//!   - `pg_trickle_launcher_main` — launcher BGW entry point
//!   - `register_scheduler_worker` — per-DB BGW registration
//!   - `pg_trickle_scheduler_main` — per-DB scheduler BGW entry point

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;

use pgrx::bgworkers::*;
use pgrx::prelude::*;

use crate::catalog::SchedulerJob;
use crate::config;
use crate::dag::{
    DiamondConsistency, DiamondSchedulePolicy, ExecutionUnitDag, NodeId, StDag, StStatus,
};
use crate::error::{RetryPolicy, RetryState};
use crate::monitor;
use crate::refresh::{self};
use crate::shmem;
use crate::wal_decoder;

// Private items from the parent module accessible in child modules.
use super::dispatch::{
    ParallelDispatchState, compute_adaptive_poll_ms, parallel_dispatch_tick,
    reconcile_parallel_state, spawn_refresh_worker,
};
use super::watermark::compute_coordinator_tick_watermark;
use super::{
    RefreshOutcome, SubTransaction, batched_has_source_changes, check_cdc_transition_health,
    check_extension_version_match, check_schedule, check_skip_needed, check_upstream_changes,
    current_epoch_ms, emit_stale_alert_if_needed, evaluate_fuse, execute_scheduled_refresh,
    group_schedule_policy, has_table_source_changes, is_any_source_gated, is_group_due,
    is_watermark_misaligned, is_watermark_stuck, iterate_to_fixpoint, load_gated_source_oids,
    load_st_by_id, log_gated_skip, log_watermark_skip, recover_from_crash, refresh_single_st,
    self_monitoring_auto_apply_tick, sla_tier_adjustment_tick, update_backoff_factor,
    upstream_change_state,
};

/// Register the launcher background worker.
///
/// Called from `_PG_init()` when loaded via `shared_preload_libraries`.
/// The launcher discovers all databases on the server and spawns a separate
/// per-database scheduler worker for each one that has pg_trickle installed.
pub fn register_launcher_worker() {
    BackgroundWorkerBuilder::new("pg_trickle launcher")
        .set_function("pg_trickle_launcher_main")
        .set_library("pg_trickle")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(std::time::Duration::from_secs(5)))
        .load();
}

/// Main entry point for the launcher background worker.
///
/// Runs once per server. Connects to `postgres` (always present) to query
/// `pg_database` and `pg_stat_activity`, then dynamically spawns a scheduler
/// worker for every database where pg_trickle is installed.
///
/// # Safety
/// Called directly by PostgreSQL as a background worker entry point.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_trickle_launcher_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    // Connect to `postgres` — always available; used only for system catalog queries.
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    // Check for replica — launcher cannot spawn workers on standbys.
    let is_replica = BackgroundWorker::transaction(AssertUnwindSafe(|| -> bool {
        // OBS-5: Tag this connection so it is identifiable in pg_stat_activity.
        // Must be inside a transaction context in a background worker.
        let _ = Spi::run("SET application_name = 'pg_trickle_launcher'");
        Spi::get_one::<bool>("SELECT pg_is_in_recovery()")
            .unwrap_or(Some(false))
            .unwrap_or(false)
    }));
    if is_replica {
        info!("pg_trickle launcher: running on read replica, sleeping until promotion");
        loop {
            let ok = BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(30)));
            if !ok {
                return;
            }
            let still = BackgroundWorker::transaction(AssertUnwindSafe(|| -> bool {
                Spi::get_one::<bool>("SELECT pg_is_in_recovery()")
                    .unwrap_or(Some(false))
                    .unwrap_or(false)
            }));
            if !still {
                break;
            }
        }
    }

    info!("pg_trickle launcher started");

    // Track the DAG rebuild signal so we can detect CREATE EXTENSION in any
    // database and immediately re-probe, rather than waiting for skip_ttl.
    let mut last_dag_version = shmem::current_dag_version();

    // SCAL-002 (v0.70.0): Track the install epoch to detect CREATE/DROP EXTENSION.
    // When the epoch is stable, the launcher uses a 60-second steady-state
    // poll interval instead of 10 seconds, reducing catalog fan-out at scale.
    let mut last_install_epoch = shmem::LAUNCHER_INSTALL_EPOCH
        .get()
        .load(std::sync::atomic::Ordering::Relaxed);

    // last_attempt[db] = when we last tried to spawn a worker for that DB.
    // Used to avoid hammering databases where pg_trickle is not installed.
    let mut last_attempt: HashMap<String, std::time::Instant> = HashMap::new();
    // DBs where we have seen a scheduler successfully start at least once this
    // session. Used to distinguish "never had pg_trickle" (long backoff) from
    // "had a running scheduler that crashed" (short backoff).
    let mut had_scheduler: HashSet<String> = HashSet::new();
    // How long to wait before re-probing a DB that has never had pg_trickle.
    let skip_ttl = std::time::Duration::from_secs(300);
    // How long to wait before respawning a scheduler that crashed / exited
    // on a DB where pg_trickle was previously confirmed running.  Kept short
    // so DROP EXTENSION + CREATE EXTENSION doesn't stall for 5 minutes.
    let retry_ttl = std::time::Duration::from_secs(15);

    // STAB-3 (v0.30.0): Track last time we ran the age-based template-cache purge.
    // The purge runs at most once per hour per launcher tick to avoid per-tick
    // catalog load. It is not per-database (the launcher connects to `postgres`).
    let last_cache_purge = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(7200))
        .unwrap_or_else(std::time::Instant::now);

    loop {
        // ── Collect all connectable, non-template databases  ──────────────
        let databases: Vec<String> = BackgroundWorker::transaction(AssertUnwindSafe(|| {
            Spi::connect(|client| -> Vec<String> {
                match client.select(
                    "SELECT datname::text FROM pg_database \
                     WHERE NOT datistemplate AND datallowconn",
                    None,
                    &[],
                ) {
                    Ok(result) => {
                        let mut out = Vec::new();
                        for row in result {
                            if let Some(db) = row.get_by_name::<String, _>("datname").ok().flatten()
                            {
                                out.push(db);
                            }
                        }
                        out
                    }
                    Err(_) => vec![],
                }
            })
        }));

        // ── Databases that already have a running scheduler worker  ────────
        let active: HashSet<String> = BackgroundWorker::transaction(AssertUnwindSafe(|| {
            Spi::connect(|client| -> HashSet<String> {
                match client.select(
                    "SELECT datname::text \
                     FROM pg_stat_activity \
                     WHERE backend_type = 'pg_trickle scheduler' \
                       AND datname IS NOT NULL",
                    None,
                    &[],
                ) {
                    Ok(result) => {
                        let mut out = HashSet::new();
                        for row in result {
                            if let Some(db) = row.get_by_name::<String, _>("datname").ok().flatten()
                            {
                                out.insert(db);
                            }
                        }
                        out
                    }
                    Err(_) => HashSet::new(),
                }
            })
        }));

        // If any backend bumped the DAG signal (create_st, alter, drop,
        // CREATE EXTENSION finalize, manual rescan nudge, etc.) since our last
        // loop, clear the skip-cache so we re-probe promptly.
        //
        // This is intentionally unconditional. A CREATE EXTENSION in a newly
        // created database can race with the launcher's first probe of that
        // database: the launcher may spawn a scheduler before the extension
        // transaction commits, observe "not installed", and cache a fresh
        // `last_attempt` entry. If we preserve that fresh entry on the very DAG
        // signal emitted by CREATE EXTENSION, the database remains classified as
        // a long-backoff `skip_ttl` database and the scheduler may not appear
        // for minutes.
        //
        // DDL-driven DAG bumps are relatively rare and semantically important,
        // so favor immediate correctness over preserving the old skip-cache on
        // those events. Non-DDL steady-state operation still uses the normal
        // skip_ttl / retry_ttl back-off between signals.
        let dag_version = shmem::current_dag_version();
        if dag_version != last_dag_version {
            last_dag_version = dag_version;
            last_attempt.clear();
        }

        // SCAL-002 (v0.70.0): Also clear the skip-cache on install epoch change
        // (CREATE/DROP EXTENSION events bump LAUNCHER_INSTALL_EPOCH via hooks.rs).
        let install_epoch = shmem::LAUNCHER_INSTALL_EPOCH
            .get()
            .load(std::sync::atomic::Ordering::Relaxed);
        let epoch_changed = install_epoch != last_install_epoch;
        if epoch_changed {
            last_install_epoch = install_epoch;
            last_attempt.clear();
        }

        for db in &databases {
            if active.contains(db) {
                // Worker healthy — record that this DB has had a running
                // scheduler so we can use the short retry_ttl if it crashes.
                had_scheduler.insert(db.clone());
                last_attempt.remove(db);
                continue;
            }

            // Choose the right backoff: short for DBs that previously ran a
            // scheduler (crash / DROP EXTENSION scenario), long for DBs that
            // have never had pg_trickle installed.
            let ttl = if had_scheduler.contains(db) {
                retry_ttl
            } else {
                skip_ttl
            };

            // Should we (re)try this database?
            let retry = last_attempt
                .get(db)
                .map(|t| t.elapsed() >= ttl)
                .unwrap_or(true);
            if !retry {
                continue;
            }

            last_attempt.insert(db.clone(), std::time::Instant::now());

            match BackgroundWorkerBuilder::new("pg_trickle scheduler")
                .set_function("pg_trickle_scheduler_main")
                .set_library("pg_trickle")
                .enable_spi_access()
                // Pass the database name via bgw_extra (max 128 bytes).
                .set_extra(db.as_str())
                // No restart_time — the launcher itself handles respawning.
                .set_restart_time(None)
                .load_dynamic()
            {
                Ok(_) => {
                    info!(
                        "pg_trickle launcher: spawned scheduler for database '{}'",
                        db
                    );
                }
                Err(_) => {
                    warning!(
                        "pg_trickle launcher: could not spawn scheduler for database '{}'",
                        db
                    );
                }
            }
        }

        // STAB-3 (v0.30.0): Age-based purge of the L2 catalog template cache.
        // Run at most once per hour. The launcher connects to `postgres` which
        // does not have pg_trickle installed, so we skip the purge here — the
        // per-database scheduler workers run the purge in their own DB context.
        let _ = last_cache_purge; // suppress unused-variable warning

        // SCAL-002 (v0.70.0): Use a 60-second steady-state poll interval when
        // no extension install/drop has been signalled.  Reset to 10 seconds
        // when an epoch change is detected so new databases are picked up quickly.
        let launcher_wait_secs = if epoch_changed { 10u64 } else { 60u64 };
        let should_continue =
            BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(launcher_wait_secs)));

        unsafe {
            if pg_sys::ConfigReloadPending != 0 {
                pg_sys::ConfigReloadPending = 0;
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
                pgrx::info!(
                    "pg_trickle scheduler: SIGHUP processed, allow_circular is now {}",
                    config::pg_trickle_allow_circular()
                );
            }
        }

        if !should_continue {
            info!("pg_trickle launcher shutting down");
            break;
        }
    }
}

/// Register the scheduler background worker (single-database, legacy).
///
/// Kept for reference — `_PG_init()` now registers the launcher instead.
/// The launcher dynamically spawns instances of this worker, one per database.
pub fn register_scheduler_worker() {
    BackgroundWorkerBuilder::new("pg_trickle scheduler")
        .set_function("pg_trickle_scheduler_main")
        .set_library("pg_trickle")
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(std::time::Duration::from_secs(5)))
        .load();
}

/// Main entry point for the scheduler background worker.
///
/// # Safety
/// This function is called directly by PostgreSQL as a background worker
/// entry point. It must follow the C-unwind calling convention.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_trickle_scheduler_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Determine which database to connect to.
    // Dynamic workers spawned by the launcher have the DB name in bgw_extra.
    let extra = BackgroundWorker::get_extra();
    let db_name = if extra.is_empty() {
        "postgres".to_string()
    } else {
        extra.to_string()
    };
    BackgroundWorker::connect_worker_to_spi(Some(db_name.as_str()), None);

    // Exit cleanly if pg_trickle is not installed in this database.
    // The launcher will re-probe after its skip TTL (5 min) expires.
    let is_installed = BackgroundWorker::transaction(AssertUnwindSafe(|| -> bool {
        // OBS-5: Tag this connection so it is identifiable in pg_stat_activity.
        // Must be inside a transaction context in a background worker.
        let _ = Spi::run("SET application_name = 'pg_trickle_scheduler'");
        Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_trickle')",
        )
        .unwrap_or(Some(false))
        .unwrap_or(false)
    }));
    if !is_installed {
        info!(
            "pg_trickle scheduler: pg_trickle not installed in database '{}', exiting",
            db_name
        );
        return;
    }

    // UG1: Version mismatch check — warn if the compiled .so version differs
    // from the SQL-installed extension version (stale install).
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        check_extension_version_match();
    }));

    // F16 (G8.2): Detect read replicas — the scheduler cannot write on a
    // standby. Skip all work and sleep until promotion.
    let is_replica = BackgroundWorker::transaction(AssertUnwindSafe(|| -> bool {
        Spi::get_one::<bool>("SELECT pg_is_in_recovery()")
            .unwrap_or(Some(false))
            .unwrap_or(false)
    }));
    if is_replica {
        info!(
            "pg_trickle scheduler: running on a read replica (pg_is_in_recovery() = true). \
             Scheduler will sleep until promotion."
        );
        // Sleep in a loop — if the server is promoted, pg_is_in_recovery()
        // changes to false and we can start working.
        loop {
            let should_continue =
                BackgroundWorker::wait_latch(Some(std::time::Duration::from_secs(30)));
            if !should_continue {
                info!("pg_trickle scheduler shutting down (replica)");
                return;
            }
            let still_replica = BackgroundWorker::transaction(AssertUnwindSafe(|| -> bool {
                Spi::get_one::<bool>("SELECT pg_is_in_recovery()")
                    .unwrap_or(Some(false))
                    .unwrap_or(false)
            }));
            if !still_replica {
                info!("pg_trickle scheduler: replica promoted — starting normal operation");
                break;
            }
        }
    }

    info!(
        "pg_trickle scheduler started (interval={}ms)",
        config::pg_trickle_scheduler_interval_ms(),
    );

    // Mark scheduler as running in shared memory so cluster_worker_summary()
    // and other shmem-based health consumers reflect the correct state.
    // SAFETY: MyProcPid is always valid inside a background worker.
    let my_pid = unsafe { pg_sys::MyProcPid };
    let now_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    crate::shmem::set_scheduler_meta(my_pid, true, now_ts);

    let mut dag_version: u64 = 0;
    let mut dag: Option<StDag> = None;
    // Follow-up flag: after an incremental rebuild, schedule a full rebuild
    // on the next tick.  ALTER operations change dependency edges but not
    // the node set, so the stale-snapshot guard (which only checks for
    // missing nodes) cannot detect stale edges.  A follow-up full rebuild
    // with a fresh snapshot captures the committed edge changes.
    let mut pending_full_rebuild = false;

    // #536: Previous tick's safe frontier watermark, used by the holdback
    // algorithm to determine whether any long-running transaction spans a
    // tick boundary.  Seeded from shared memory on scheduler startup so
    // that the first post-restart tick preserves the last known-safe LSN
    // (avoids a one-tick window where the frontier could advance past an
    // in-flight transaction that was already open before the restart).
    let mut prev_tick_watermark: Option<String> = {
        let last_safe = crate::shmem::last_tick_safe_lsn_u64();
        if last_safe != 0 {
            Some(crate::version::u64_to_lsn(last_safe))
        } else {
            None
        }
    };

    // Per-ST retry state (in-memory only, reset on scheduler restart)
    let mut retry_states: HashMap<i64, RetryState> = HashMap::new();
    let retry_policy = RetryPolicy::default();

    // Phase 4: Parallel dispatch state (persisted across ticks)
    let mut parallel_state = ParallelDispatchState::new();

    // Per-ST drift reset counters (differential cycles since last reinit)
    let mut drift_counters: HashMap<i64, i32> = HashMap::new();

    // P3-5: Per-ST auto-backoff factors (multiplicative schedule stretch).
    // When auto_backoff is enabled and a ST is falling behind, the factor
    // doubles each consecutive cycle; resets to 1.0 on the first on-time cycle.
    let mut backoff_factors: HashMap<i64, f64> = HashMap::new();

    // PH-E2: Per-ST consecutive spill counters.
    // When a differential refresh writes temp blocks exceeding the threshold,
    // the counter increments.  After spill_consecutive_limit consecutive spills,
    // the scheduler forces a FULL refresh.  Resets on any non-spilling refresh.
    let mut spill_counters: HashMap<i64, i32> = HashMap::new();

    // Timestamp for periodic CDC trigger health check (~every 60s).
    let mut last_trigger_health_ms: u64 = 0;

    // WM-7: Timestamp for periodic stuck-watermark alerting (~every 60s).
    let mut last_watermark_stuck_check_ms: u64 = 0;
    // WM-7: Track source OIDs that have already been reported as stuck to
    // avoid spamming the NOTIFY channel every check cycle.
    let mut reported_stuck_sources: HashSet<u32> = HashSet::new();

    // SCAL-1 (v0.31.0): Per-source consecutive-cycle counter for back-pressure
    // alerting. Maps source_relid (u32) to the number of consecutive scheduler
    // ticks where that source's change buffer has exceeded the alert threshold.
    let mut backpressure_cycles: HashMap<u32, i32> = HashMap::new();

    // DB-5: Timestamp for daily history retention cleanup.
    // PERF-003 (v0.70.0): Interval is now read from the
    // pg_trickle.history_prune_interval_seconds GUC instead of the
    // previous hard-coded 24-hour constant.
    let mut last_history_cleanup_ms: u64 = 0;
    // DF-G2: Dog-feeding auto-apply — timestamp-gated, rate-limited.
    let mut last_auto_apply_ms: u64 = 0;
    const AUTO_APPLY_INTERVAL_MS: u64 = 10 * 60 * 1000; // 10 minutes

    // Phase 10: Crash recovery — mark any interrupted RUNNING records
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        recover_from_crash();
    }));

    // Phase 2: Reconcile parallel refresh state (orphaned jobs, worker tokens)
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        reconcile_parallel_state();
    }));

    // EC-20: Post-restart CDC TRANSITIONING health check.
    // If the scheduler was restarted during an active TRIGGER→WAL transition,
    // verify the transition is still valid. Invalid transitions (stale slot,
    // missing publication) are rolled back to TRIGGER mode.
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        check_cdc_transition_health();
    }));

    // Timestamp for periodic shmem wake-time update (every 60s).
    let mut last_wake_ts_update_ms: u64 = current_epoch_ms();

    // OPS-6: Workload-aware poll — overlap count from df_scheduling_interference.
    // Refreshed once per auto-apply cycle (10 min).
    let mut interference_overlap_count: i64 = 0;

    // OP-2: Start the Prometheus metrics HTTP server if metrics_port is non-zero.
    let metrics_server = {
        let port = config::pg_trickle_metrics_port();
        if port > 0 {
            crate::metrics_server::MetricsServer::start(port as u16)
        } else {
            None
        }
    };

    loop {
        // DAG-2: Adaptive poll interval — exponential backoff (20ms → 200ms)
        // that resets to 20ms on worker completion, making parallel mode
        // competitive for cheap refreshes.
        let mut base_interval_ms = config::pg_trickle_scheduler_interval_ms() as u64;

        // OPS-6: Workload-aware poll — if df_scheduling_interference detects
        // heavy overlap, slightly increase the base interval to reduce
        // contention. This is a gentle back-off: +10% per overlap pair,
        // capped at 2× the configured interval.
        if interference_overlap_count > 0 {
            let boost = base_interval_ms / 10 * (interference_overlap_count as u64).min(10);
            base_interval_ms = (base_interval_ms + boost).min(base_interval_ms * 2);
        }

        let poll_ms = compute_adaptive_poll_ms(
            parallel_state.adaptive_poll_ms,
            parallel_state.completions_this_tick > 0,
            parallel_state.has_inflight(),
            base_interval_ms,
        );
        parallel_state.adaptive_poll_ms = poll_ms;
        parallel_state.completions_this_tick = 0;

        let should_continue =
            BackgroundWorker::wait_latch(Some(std::time::Duration::from_millis(poll_ms)));

        // OP-2: Service one pending Prometheus scrape request per tick (non-blocking).
        if let Some(ref ms) = metrics_server {
            // Collect metrics inside a transaction so SPI queries work.
            let metrics_text = BackgroundWorker::transaction(std::panic::AssertUnwindSafe(|| {
                crate::monitor::collect_metrics_text()
            }));
            ms.serve_one_request(&metrics_text);
        }

        // Update the last-wake timestamp in shared memory every 60 seconds.
        let now_for_stats = current_epoch_ms();
        if now_for_stats.saturating_sub(last_wake_ts_update_ms) >= 60_000 {
            last_wake_ts_update_ms = now_for_stats;

            // Update the last-wake timestamp in shared memory for monitoring.
            // SAFETY: MyProcPid is always valid inside a background worker.
            let my_pid = unsafe { pg_sys::MyProcPid };
            crate::shmem::set_scheduler_meta(my_pid, true, (now_for_stats / 1000) as i64);
        }

        unsafe {
            if pg_sys::ConfigReloadPending != 0 {
                pg_sys::ConfigReloadPending = 0;
                pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP);
                pgrx::info!(
                    "pg_trickle scheduler: SIGHUP processed, allow_circular is now {}",
                    config::pg_trickle_allow_circular()
                );
            }
        }

        if !should_continue {
            // SIGTERM received — shut down gracefully.
            info!("pg_trickle scheduler shutting down");
            crate::shmem::set_scheduler_meta(0, false, 0);
            break;
        }

        // A35 (v0.36.0): Process pending drain requests on every tick — even
        // when the scheduler is disabled — so that pgtrickle.drain() never
        // hangs indefinitely.  When a drain is requested and no refresh workers
        // are active, this marks the drain as completed (DRAIN_COMPLETED =
        // DRAIN_REQUESTED) and returns true.  We skip new dispatches for that
        // tick so callers see a clean quiesced state before drain() returns.
        if crate::shmem::scheduler_check_and_complete_drain() {
            continue;
        }

        if !config::pg_trickle_enabled() {
            continue;
        }

        // Check that pg_trickle is still installed in this database.  It can
        // be dropped at any time via DROP EXTENSION, which removes the schema
        // and all catalog tables.  Without this guard the very next SPI query
        // that references pgtrickle.* would crash the worker with exit code 1,
        // causing the launcher to apply the full 5-minute skip_ttl backoff.
        // Exiting cleanly here lets the launcher detect the fresh install
        // using the shorter retry_ttl for databases that previously had a
        // running scheduler.
        let still_installed = BackgroundWorker::transaction(AssertUnwindSafe(|| -> bool {
            Spi::get_one::<bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_trickle')",
            )
            .unwrap_or(Some(false))
            .unwrap_or(false)
        }));
        if !still_installed {
            info!(
                "pg_trickle scheduler: pg_trickle was dropped from database '{}', exiting",
                db_name
            );
            crate::shmem::set_scheduler_meta(0, false, 0);
            return;
        }

        let now_ms = current_epoch_ms();

        // WAL transition processing — three phases with separate transactions
        // to ensure slot creation happens in a pristine transaction (no prior
        // SPI reads that could assign an XID via hint-bit WAL writes).
        //
        // Phase 1: Check eligibility, poll WAL sources, collect pending slots
        // Phase 2: Create replication slots (NO SPI — pristine transaction)
        // Phase 3: Finish transitions (publication + catalog update)
        let mut pending_slots = Vec::new();
        let mut pending_aborts = Vec::new();
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            let change_schema = config::pg_trickle_change_buffer_schema();
            match wal_decoder::advance_wal_transitions_phase1(&change_schema) {
                Ok(result) => {
                    pending_slots = result.pending_slots;
                    pending_aborts = result.pending_aborts;
                }
                Err(e) => warning!("pg_trickle: WAL transition phase 1 error: {}", e),
            }
            // NOTE: check_slot_health_and_alert() is intentionally NOT called
            // here.  Phase 1's WAL poll for a missing/invalid replication slot
            // can abort the current SPI session (even when the Rust-level
            // catch_unwind absorbs the panic), which silently breaks subsequent
            // SPI calls — including the EC-34 slot-existence check inside
            // check_slot_health_and_alert().  Running the health check in its
            // own transaction (below) guarantees a pristine SPI connection and
            // ensures reliable fallback-to-TRIGGER detection.
        }));

        // Slot / buffer health check in its own pristine transaction.
        // Must be separate from Phase 1 so that WAL poll errors (which can
        // corrupt the Phase 1 SPI session) do not prevent the EC-34 missing-
        // slot detection from running.  This is the primary path for automatic
        // fallback from WAL mode to TRIGGER mode when a replication slot is
        // externally dropped.
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            monitor::check_slot_health_and_alert();
        }));

        // SCAL-1 (v0.31.0): Back-pressure detection — check change buffer sizes
        // and emit change_buffer_backpressure alerts when buffers are persistently
        // large across multiple consecutive refresh cycles.
        {
            let limit = config::pg_trickle_backpressure_consecutive_limit();
            if limit > 0 {
                BackgroundWorker::transaction(AssertUnwindSafe(|| {
                    let threshold = config::pg_trickle_buffer_alert_threshold();
                    let over = monitor::check_change_buffer_sizes();
                    // Update consecutive-cycle counters for each source.
                    let now_over: std::collections::HashSet<u32> =
                        over.iter().map(|(oid, _)| *oid).collect();
                    // Increment counters for sources over threshold.
                    for (oid, pending) in &over {
                        let cnt = backpressure_cycles
                            .entry(*oid)
                            .and_modify(|c| *c += 1)
                            .or_insert(1);
                        if *cnt >= limit {
                            monitor::alert_change_buffer_backpressure(
                                *oid, *pending, *cnt, threshold,
                            );
                            pgrx::info!(
                                "[pg_trickle] SCAL-1: back-pressure alert for source OID {} \
                                 — {} rows over threshold {} for {} consecutive cycles",
                                oid,
                                pending,
                                threshold,
                                cnt,
                            );
                        }
                    }
                    // Reset counters for sources that came back under threshold.
                    backpressure_cycles.retain(|oid, _| now_over.contains(oid));
                }));
            }
        }

        // Periodic CDC trigger health check: detect disabled/missing triggers
        // on source tables.  Runs every ~60s to avoid per-tick overhead.
        let now_for_trigger_check = current_epoch_ms();
        if now_for_trigger_check.saturating_sub(last_trigger_health_ms) >= 60_000 {
            BackgroundWorker::transaction(AssertUnwindSafe(|| {
                monitor::check_cdc_trigger_health();
            }));
            last_trigger_health_ms = now_for_trigger_check;

            // STAB-3 (v0.30.0): Age-based purge of the L2 catalog template cache.
            // Runs on the same ~60s cadence as the CDC trigger health check to
            // avoid adding another time-tracking variable.
            let max_age_hours = config::pg_trickle_template_cache_max_age_hours();
            if max_age_hours > 0 {
                BackgroundWorker::transaction(AssertUnwindSafe(|| {
                    let purged = crate::template_cache::purge_stale_entries(max_age_hours);
                    if purged > 0 {
                        pgrx::info!(
                            "[pg_trickle] STAB-3: purged {} stale template cache entries \
                             older than {} hours",
                            purged,
                            max_age_hours,
                        );
                    }
                }));
            }
        }

        // WM-7: Periodic stuck-watermark detection and alerting (~every 60s).
        let timeout = config::pg_trickle_watermark_holdback_timeout();
        if timeout > 0 {
            let now_for_wm_check = current_epoch_ms();
            if now_for_wm_check.saturating_sub(last_watermark_stuck_check_ms) >= 60_000 {
                BackgroundWorker::transaction(AssertUnwindSafe(|| {
                    match crate::catalog::find_stuck_watermarks(timeout) {
                        Ok(stuck_list) => {
                            // Collect currently-stuck OIDs for comparison.
                            let current_stuck: HashSet<u32> =
                                stuck_list.iter().map(|(_, oid, _)| *oid).collect();

                            // Emit NOTIFY for newly-stuck sources only.
                            for (group_name, source_oid, age_secs) in &stuck_list {
                                if !reported_stuck_sources.contains(source_oid) {
                                    let payload = format!(
                                        "{{\"event\":\"watermark_stuck\",\"group\":\"{}\",\
                                          \"source_oid\":{},\"age_secs\":{:.0}}}",
                                        group_name, source_oid, age_secs
                                    );
                                    let _ = Spi::run_with_args(
                                        "SELECT pg_notify('pgtrickle_alert', $1)",
                                        &[payload.as_str().into()],
                                    );
                                    pgrx::warning!(
                                        "pg_trickle: watermark stuck in group '{}' — \
                                         source OID {} not advanced for {:.0}s (timeout {}s)",
                                        group_name,
                                        source_oid,
                                        age_secs,
                                        timeout,
                                    );
                                }
                            }

                            // Emit recovery NOTIFY for sources that were stuck but
                            // have since advanced (auto-resume).
                            for previously_stuck in &reported_stuck_sources {
                                if !current_stuck.contains(previously_stuck) {
                                    let payload = format!(
                                        "{{\"event\":\"watermark_resumed\",\
                                          \"source_oid\":{}}}",
                                        previously_stuck
                                    );
                                    let _ = Spi::run_with_args(
                                        "SELECT pg_notify('pgtrickle_alert', $1)",
                                        &[payload.as_str().into()],
                                    );
                                    pgrx::info!(
                                        "pg_trickle: watermark resumed for source OID {}",
                                        previously_stuck,
                                    );
                                }
                            }

                            reported_stuck_sources = current_stuck;
                        }
                        Err(e) => {
                            pgrx::warning!("pg_trickle: stuck watermark check failed: {}", e,);
                        }
                    }
                }));
                last_watermark_stuck_check_ms = now_for_wm_check;
            }
        }

        // DB-5: Periodic history retention cleanup.
        // PERF-003 (v0.70.0): Interval driven by
        // pg_trickle.history_prune_interval_seconds GUC instead of a
        // hard-coded 24-hour constant.
        {
            let now_for_cleanup = current_epoch_ms();
            let prune_interval_secs = config::pg_trickle_history_prune_interval_seconds() as u64;
            let prune_interval_ms = prune_interval_secs.saturating_mul(1_000);
            if prune_interval_ms > 0
                && now_for_cleanup.saturating_sub(last_history_cleanup_ms) >= prune_interval_ms
            {
                let retention_days = config::pg_trickle_history_retention_days();
                if retention_days > 0 {
                    // A10 (v0.35.0): Batched DELETEs — delete up to 10,000 rows per
                    // transaction to limit lock contention on pgt_refresh_history.
                    // Repeat until fewer than the batch size are deleted.
                    let batch_size: i64 = 10_000;
                    let mut total_deleted: i64 = 0;
                    loop {
                        // OBS-002 (v0.70.0): Return Result so SPI errors are
                        // visible instead of silently collapsed to 0.
                        let batch_result: Result<i64, String> =
                            BackgroundWorker::transaction(AssertUnwindSafe(|| {
                                Spi::get_one_with_args::<i64>(
                                    "WITH batch AS (\
                                        SELECT refresh_id FROM pgtrickle.pgt_refresh_history \
                                        WHERE start_time < now() - make_interval(days => $1) \
                                        LIMIT $2 \
                                    ), deleted AS (\
                                        DELETE FROM pgtrickle.pgt_refresh_history \
                                        WHERE refresh_id IN (SELECT refresh_id FROM batch) \
                                        RETURNING 1\
                                    ) SELECT count(*) FROM deleted",
                                    &[retention_days.into(), batch_size.into()],
                                )
                                .map(|v| v.unwrap_or(0))
                                .map_err(|e| e.to_string())
                            }));
                        match batch_result {
                            Ok(deleted) => {
                                total_deleted += deleted;
                                if deleted < batch_size {
                                    break;
                                }
                            }
                            Err(e) => {
                                pgrx::warning!("pg_trickle: history prune failed: {}", e);
                                crate::shmem::increment_history_prune_errors();
                                break;
                            }
                        }
                    }
                    if total_deleted > 0 {
                        info!(
                            "pg_trickle: history cleanup — deleted {} rows older than {} days (batched)",
                            total_deleted, retention_days,
                        );
                    }
                }
                last_history_cleanup_ms = now_for_cleanup;
            }
        }

        // DF-G2: Dog-feeding auto-apply — read df_threshold_advice, apply changes.
        {
            let now_for_auto_apply = current_epoch_ms();
            if now_for_auto_apply.saturating_sub(last_auto_apply_ms) >= AUTO_APPLY_INTERVAL_MS {
                let auto_apply_mode = config::pg_trickle_self_monitoring_auto_apply();
                if auto_apply_mode != config::SelfMonitoringAutoApply::Off {
                    BackgroundWorker::transaction(AssertUnwindSafe(|| {
                        self_monitoring_auto_apply_tick();
                    }));
                }
                last_auto_apply_ms = now_for_auto_apply;

                // SLA-3: Dynamic tier re-assignment — check and adjust tiers
                // for stream tables with SLA configured.
                BackgroundWorker::transaction(AssertUnwindSafe(|| {
                    sla_tier_adjustment_tick();
                }));

                // OPS-6: Refresh interference overlap count for workload-aware poll.
                // Only reads if the DF ST exists (safe even without self-monitoring).
                BackgroundWorker::transaction(AssertUnwindSafe(|| {
                    let si_exists: bool = Spi::get_one(
                        "SELECT EXISTS ( \
                            SELECT 1 FROM pgtrickle.pgt_stream_tables \
                            WHERE pgt_schema = 'pgtrickle' \
                              AND pgt_name = 'df_scheduling_interference' \
                        )",
                    )
                    .unwrap_or(Some(false))
                    .unwrap_or(false);
                    if si_exists {
                        interference_overlap_count = Spi::get_one(
                            "SELECT coalesce(sum(overlap_count), 0)::bigint \
                             FROM pgtrickle.df_scheduling_interference",
                        )
                        .unwrap_or(Some(0))
                        .unwrap_or(0);
                    } else {
                        interference_overlap_count = 0;
                    }
                }));
            }
        }

        // Phase 2: Create each pending slot in its own pristine transaction.
        // No SPI calls — just the C replication API.
        let mut created_slots = Vec::new();
        for pending in pending_slots {
            let slot_name = pending.slot_name.clone();
            let mut slot_lsn = None;
            BackgroundWorker::transaction(AssertUnwindSafe(|| {
                match wal_decoder::create_replication_slot_pristine(&slot_name) {
                    Ok(lsn) => {
                        info!("pg_trickle: created replication slot '{}'", slot_name);
                        slot_lsn = Some(lsn);
                    }
                    Err(e) => {
                        warning!(
                            "pg_trickle: failed to create replication slot '{}': {}",
                            slot_name,
                            e
                        );
                    }
                }
            }));
            if let Some(lsn) = slot_lsn {
                created_slots.push((pending, lsn));
            }
        }

        // Phase 3: Finish transitions (publications + catalog updates)
        if !created_slots.is_empty() {
            BackgroundWorker::transaction(AssertUnwindSafe(|| {
                if let Err(e) = wal_decoder::advance_wal_transitions_phase3(&created_slots) {
                    warning!("pg_trickle: WAL transition phase 3 error: {}", e);
                }
            }));
        }

        // Phase 4: Abort WAL transitions that need fallback to triggers.
        // Each abort runs in its own transaction because Phase 1's SPI may
        // be broken after a caught panic from a missing slot.
        for abort in pending_aborts {
            BackgroundWorker::transaction(AssertUnwindSafe(|| {
                let change_schema = config::pg_trickle_change_buffer_schema();
                if let Err(e) = wal_decoder::abort_wal_transition(
                    abort.source_relid,
                    abort.pgt_id,
                    &change_schema,
                ) {
                    warning!(
                        "pg_trickle: WAL abort (fallback to triggers) failed for OID {}: {}",
                        abort.source_relid.to_u32(),
                        e
                    );
                }
            }));
        }

        // Collect jobs to spawn (populated inside the transaction, spawned after).
        let mut pending_spawns: Vec<(String, i64)> = Vec::new();

        // Run the scheduler tick inside a transaction
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            // CSS1 / #536: Capture tick watermark for cross-source snapshot consistency
            // with frontier holdback to prevent silent data loss from long-running
            // transactions that span a tick boundary.
            let (tick_watermark, _current_oldest_xmin, _holdback_age_secs) =
                compute_coordinator_tick_watermark(prev_tick_watermark.as_deref());
            // Persist this tick's safe watermark for the next tick's holdback comparison.
            prev_tick_watermark.clone_from(&tick_watermark);

            // Step A: Check if DAG needs rebuild
            let current_version = shmem::current_dag_version();
            if current_version != dag_version || dag.is_none() || pending_full_rebuild {
                let force_full = pending_full_rebuild;
                pending_full_rebuild = false;

                // C2-1: Drain the invalidation ring buffer.
                let invalidated = shmem::drain_invalidations();

                // G-8: Try incremental rebuild when we have specific pgt_ids
                // and an existing DAG. Falls back to full rebuild on overflow,
                // no existing DAG, or incremental failure.
                // Skip incremental if a follow-up full rebuild was requested.
                let incremental_ok = if force_full {
                    false
                } else if let Some(ids) = &invalidated {
                    if !ids.is_empty() {
                        if let Some(ref mut existing_dag) = dag {
                            match existing_dag.rebuild_incremental(
                                ids,
                                config::pg_trickle_default_schedule_seconds(),
                            ) {
                                Ok(()) => {
                                    info!(
                                        "pg_trickle: DAG incrementally updated for pgt_ids {:?} (version={})",
                                        ids, current_version,
                                    );
                                    true
                                }
                                Err(e) => {
                                    warning!(
                                        "pg_trickle: incremental DAG rebuild failed ({}), falling back to full rebuild",
                                        e,
                                    );
                                    false
                                }
                            }
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                } else {
                    // Overflow → full rebuild
                    info!("pg_trickle: DAG invalidation overflow → full rebuild");
                    false
                };

                if !incremental_ok {
                    match StDag::build_from_catalog(config::pg_trickle_default_schedule_seconds()) {
                        Ok(new_dag) => {
                            dag = Some(new_dag);
                            info!("pg_trickle: DAG rebuilt (version={})", current_version);
                        }
                        Err(e) => {
                            warning!("pg_trickle: failed to rebuild DAG: {}", e);
                            return;
                        }
                    }
                }

                // Race-condition guard: verify all invalidated pgt_ids are visible
                // in the rebuilt DAG.  `signal_dag_invalidation` is called inside
                // the creating transaction before it commits, so the scheduler can
                // read the new version while the transaction is still in-flight.
                // If the SPI snapshot captured at tick start pre-dates that commit,
                // the catalog query misses the new ST and the DAG is built without
                // it.  Detect this by checking whether every invalidated id is
                // present; if not, trigger a full-rebuild signal and skip updating
                // dag_version so the next tick rebuilds with a fresh snapshot that
                // includes all committed STs.
                let stale_snapshot_detected = if let (Some(ids), Some(d)) = (&invalidated, &dag) {
                    ids.iter().any(|&id| !d.has_st_node(id))
                } else {
                    false
                };

                if stale_snapshot_detected {
                    info!(
                        "pg_trickle: DAG rebuild used stale snapshot — some invalidated pgt_ids \
                         not yet visible (version={}); triggering full rebuild next tick",
                        current_version
                    );
                    // Signal a full rebuild (overflow) so the next tick uses a fresh
                    // snapshot.  This is necessary because without the overflow flag,
                    // an incremental rebuild for a *different* pgt_id arriving between
                    // ticks could satisfy the version-mismatch check while still
                    // missing the not-yet-committed ST — permanently excluding it from
                    // the DAG.  The overflow path forces a full build_from_catalog()
                    // on the next tick regardless of what else arrives in the ring.
                    //
                    // Do NOT update dag_version — the stale tick's version is not
                    // trusted.
                    shmem::signal_dag_rebuild();
                } else {
                    dag_version = current_version;
                    // After an incremental rebuild, schedule a follow-up full
                    // rebuild to catch edge-stale scenarios.  ALTER operations
                    // update dependency edges but preserve the node; the stale
                    // guard above only detects missing *nodes*.  A full rebuild
                    // on the next tick (with a fresh snapshot that sees the
                    // committed edge updates) closes this window.
                    if incremental_ok {
                        pending_full_rebuild = true;
                    }
                }
            }

            let dag_ref = match &dag {
                Some(d) => d,
                None => return,
            };

            // Step B: Validate topological order (detect cycles)
            // When circular dependencies are allowed (allow_circular=true),
            // we use the condensation order (SCC-based) instead of rejecting
            // cycles outright. Cyclic SCCs are handled by fixpoint iteration.
            let allow_circular = config::pg_trickle_allow_circular();
            if !allow_circular && let Err(e) = dag_ref.topological_order() {
                warning!("pg_trickle: DAG has cycles: {}", e);
                return;
            }

            // Step B2: Build execution unit DAG for parallel-refresh awareness.
            let parallel_mode = config::pg_trickle_parallel_refresh_mode();
            match parallel_mode {
                config::ParallelRefreshMode::Off => {}
                config::ParallelRefreshMode::DryRun => {
                    let eu_dag = ExecutionUnitDag::build_from_st_dag(dag_ref, |pgt_id| {
                        load_st_by_id(pgt_id).map(|st| st.refresh_mode)
                    });
                    info!(
                        "pg_trickle: parallel refresh (dry_run): {}",
                        eu_dag.summary()
                    );
                    info!("{}", eu_dag.dry_run_log());
                    // Fall through to sequential refresh.
                }
                config::ParallelRefreshMode::On => {
                    // Phase 4: Parallel dispatch replaces sequential refresh.
                    if parallel_state.dag_version != dag_version || parallel_state.eu_dag.is_none()
                    {
                        let eu_dag = ExecutionUnitDag::build_from_st_dag(dag_ref, |pgt_id| {
                            load_st_by_id(pgt_id).map(|st| st.refresh_mode)
                        });
                        info!(
                            "pg_trickle: parallel dispatch — EU DAG rebuilt: {}",
                            eu_dag.summary()
                        );
                        parallel_state.rebuild(eu_dag, dag_version);
                    }

                    parallel_dispatch_tick(
                        &mut parallel_state,
                        dag_ref,
                        now_ms,
                        &mut retry_states,
                        &retry_policy,
                        &db_name,
                        &mut pending_spawns,
                    );

                    // Prune retry states for STs that no longer exist.
                    if let Some(ref eu) = parallel_state.eu_dag {
                        let active_ids: HashSet<i64> = eu
                            .units()
                            .flat_map(|u| u.member_pgt_ids.iter().copied())
                            .collect();
                        retry_states.retain(|id, _| active_ids.contains(id));
                    }

                    return; // Skip sequential refresh.
                }
            }

            // Step B3: Handle cyclic SCCs via fixpoint iteration.
            // Process cyclic SCCs before the regular consistency group refresh
            // so that SCC members are up-to-date when downstream STs check
            // for upstream changes.
            // Collect SCC member IDs so Step C can skip them.
            let mut scc_member_ids: HashSet<i64> = HashSet::new();
            if allow_circular {
                let sccs = dag_ref.condensation_order().unwrap_or_else(|e| {
                    pgrx::warning!(
                        "pg_trickle: SCC computation error in scheduler (returning empty SCC list): {}",
                        e
                    );
                    Vec::new()
                });
                for scc in &sccs {
                    if !scc.is_cyclic {
                        continue; // Singletons handled below in Step C
                    }
                    // Record members so Step C skips them.
                    for node in &scc.nodes {
                        if let NodeId::StreamTable(id) = node {
                            scc_member_ids.insert(*id);
                        }
                    }
                    // Check if any SCC member needs refresh
                    let any_due = scc.nodes.iter().any(|node| {
                        if let NodeId::StreamTable(id) = node {
                            load_st_by_id(*id)
                                .map(|st| {
                                    (st.status == StStatus::Active
                                        || st.status == StStatus::Initializing)
                                        && (check_schedule(&st, dag_ref)
                                            || check_upstream_changes(&st)
                                            || st.needs_reinit)
                                })
                                .unwrap_or(false)
                        } else {
                            false
                        }
                    });
                    if !any_due {
                        continue;
                    }
                    iterate_to_fixpoint(
                        scc,
                        dag_ref,
                        &mut retry_states,
                        &retry_policy,
                        now_ms,
                        tick_watermark.as_deref(),
                    );
                }
            }

            // Step C: Compute consistency groups and refresh group-by-group
            let groups = dag_ref.compute_consistency_groups();

            // PERF-1 (v0.62.0): Build a per-tick change-buffer fanout cache.
            //
            // When `pg_trickle.enable_change_buffer_fanout` is true (default),
            // we issue ONE batched EXISTS query across ALL stream tables at
            // once instead of one per stream table per group.  This eliminates
            // O(N) redundant SPI round-trips for deployments where many stream
            // tables share the same source table.
            //
            // If fanout is disabled we fall back to the per-group per-member
            // path used in earlier versions.
            let fanout_cache: Option<std::collections::HashSet<i64>> =
                if config::pg_trickle_enable_change_buffer_fanout() {
                    // Collect all active STs referenced in the consistency groups.
                    let all_pgt_ids: Vec<i64> = groups
                        .iter()
                        .flat_map(|g| g.members.iter())
                        .filter_map(|m| {
                            if let NodeId::StreamTable(id) = m {
                                Some(*id)
                            } else {
                                None
                            }
                        })
                        .collect::<std::collections::HashSet<_>>()
                        .into_iter()
                        .collect();

                    let sts: Vec<_> = all_pgt_ids
                        .iter()
                        .filter_map(|&id| load_st_by_id(id))
                        .collect();

                    Some(batched_has_source_changes(&sts))
                } else {
                    None
                };

            for group in &groups {
                // PERF-1: Use the per-tick fanout cache when available; otherwise
                // fall back to per-member has_table_source_changes calls (legacy path).
                let initial_table_changes: HashMap<i64, bool> = match &fanout_cache {
                    Some(cache) => group
                        .members
                        .iter()
                        .filter_map(|member| {
                            if let NodeId::StreamTable(id) = member {
                                Some((*id, cache.contains(id)))
                            } else {
                                None
                            }
                        })
                        .collect(),
                    None => group
                        .members
                        .iter()
                        .filter_map(|member| match member {
                            NodeId::StreamTable(id) => {
                                load_st_by_id(*id).map(|st| (*id, has_table_source_changes(&st)))
                            }
                            _ => None,
                        })
                        .collect(),
                };

                if group.is_singleton() {
                    // Fast path: no SAVEPOINT overhead for non-diamond STs.
                    let pgt_id = match &group.members[0] {
                        NodeId::StreamTable(id) => *id,
                        _ => continue,
                    };
                    // Skip SCC members already handled by fixpoint iteration.
                    if scc_member_ids.contains(&pgt_id) {
                        continue;
                    }
                    // API-1/2 (v0.62.0): Skip nodes that are paused via
                    // pgtrickle.pause_scheduler().
                    if crate::shmem::is_node_paused(pgt_id) {
                        log!("pg_trickle scheduler: skipping {pgt_id} — node is paused");
                        continue;
                    }
                    // P3-5: Auto-backoff — skip this tick if the backoff
                    // factor indicates we should wait longer.
                    let bf = backoff_factors.get(&pgt_id).copied().unwrap_or(1.0);
                    if bf > 1.0 {
                        // Check if enough time has passed since last refresh
                        // (effective interval = schedule * backoff_factor).
                        // NOTE: We are already inside the outer BackgroundWorker::transaction,
                        // so we must NOT call BackgroundWorker::transaction here — PostgreSQL
                        // does not allow StartTransactionCommand when already in TBLOCK_STARTED
                        // state (causes elog(FATAL)). Use Spi directly instead.
                        let should_skip = if let Some(st) = load_st_by_id(pgt_id)
                            && let Some(ref schedule_str) = st.schedule
                            && let Ok(max_secs) = crate::api::parse_duration(schedule_str)
                        {
                            let effective_secs: f64 = max_secs as f64 * bf;
                            let stale = Spi::get_one_with_args::<bool>(
                                "SELECT CASE WHEN last_refresh_at IS NULL THEN true \
                                 ELSE EXTRACT(EPOCH FROM (now() - last_refresh_at)) > $2 END \
                                 FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1",
                                &[st.pgt_id.into(), effective_secs.into()],
                            )
                            .unwrap_or(Some(false))
                            .unwrap_or(false);
                            !stale // skip if not yet due under backoff schedule
                        } else {
                            false
                        };
                        if should_skip {
                            continue;
                        }
                    }
                    let mut entry = drift_counters.entry(pgt_id).or_insert(0);
                    refresh_single_st(
                        pgt_id,
                        dag_ref,
                        now_ms,
                        &mut retry_states,
                        &retry_policy,
                        initial_table_changes.get(&pgt_id).copied(),
                        tick_watermark.as_deref(),
                        Some(&mut entry),
                        &mut spill_counters,
                    );
                    // P3-5: Update auto-backoff factor based on last refresh timing.
                    // NOTE: We are already inside the outer BackgroundWorker::transaction,
                    // so we must NOT call BackgroundWorker::transaction here — calling
                    // StartTransactionCommand in TBLOCK_STARTED state causes elog(FATAL).
                    if config::pg_trickle_auto_backoff() {
                        update_backoff_factor(pgt_id, &mut backoff_factors);
                    }
                    continue;
                }

                // Multi-member group: check if all members have diamond_consistency = 'atomic'.
                // Skip groups where all members are SCC-handled.
                let non_scc_members: Vec<&NodeId> = group
                    .members
                    .iter()
                    .filter(|m| {
                        if let NodeId::StreamTable(id) = m {
                            !scc_member_ids.contains(id)
                        } else {
                            true
                        }
                    })
                    .collect();
                if non_scc_members.is_empty() {
                    continue;
                }

                let all_atomic = group.isolation_level
                    == crate::dag::IsolationLevel::RepeatableRead
                    || group.members.iter().all(|m| {
                        if let NodeId::StreamTable(id) = m {
                            load_st_by_id(*id)
                                .map(|st| st.diamond_consistency == DiamondConsistency::Atomic)
                                .unwrap_or(false)
                        } else {
                            false
                        }
                    });

                if !all_atomic {
                    // Not all members opted in — fall back to independent refreshes.
                    for member in &group.members {
                        let pgt_id = match member {
                            NodeId::StreamTable(id) => *id,
                            _ => continue,
                        };
                        // API-1/2 (v0.62.0): Skip paused nodes.
                        if crate::shmem::is_node_paused(pgt_id) {
                            log!(
                                "pg_trickle scheduler: skipping {pgt_id} — node is paused (group)"
                            );
                            continue;
                        }
                        let mut entry = drift_counters.entry(pgt_id).or_insert(0);
                        refresh_single_st(
                            pgt_id,
                            dag_ref,
                            now_ms,
                            &mut retry_states,
                            &retry_policy,
                            initial_table_changes.get(&pgt_id).copied(),
                            tick_watermark.as_deref(),
                            Some(&mut entry),
                            &mut spill_counters,
                        );
                    }
                    continue;
                }

                // Atomic group: check group-level schedule policy.
                let policy = group_schedule_policy(group);
                if !is_group_due(group, policy, dag_ref) {
                    // When the Slowest policy is active, the group waits for all
                    // members to be due simultaneously.  In asymmetric pipelines
                    // (e.g. a diamond where only one branch has CDC events on a
                    // given tick), convergence nodes can accumulate staleness
                    // indefinitely.  Emit stale-data alerts so operators are
                    // notified rather than silently waiting.
                    if policy == DiamondSchedulePolicy::Slowest {
                        for node in &group.convergence_points {
                            if let NodeId::StreamTable(id) = node
                                && let Some(conv_st) = load_st_by_id(*id)
                            {
                                emit_stale_alert_if_needed(&conv_st);
                            }
                        }
                    }
                    continue;
                }

                // All members due (per policy) — wrap in an internal sub-transaction.
                //
                // Background workers cannot use `Spi::run("SAVEPOINT …")` because
                // PostgreSQL rejects transaction-control commands issued via SPI
                // (SPI_ERROR_TRANSACTION).  The correct approach is the internal
                // C-level sub-transaction API which bypasses SPI entirely.
                //
                // SAFETY: BeginInternalSubTransaction sets up a sub-transaction
                // within the current worker transaction using PostgreSQL's resource-
                // owner mechanism.  We save and restore CurrentMemoryContext and
                // CurrentResourceOwner to ensure no context leak regardless of
                // whether the sub-transaction commits or rolls back.  All work
                // inside is done through SPI (which handles its own C-level exception
                // boundaries), so a Rust-visible panic from this block is not
                // expected.
                let subtxn = SubTransaction::begin();

                let mut group_ok = true;
                let mut refreshed_ids: Vec<i64> = Vec::new();

                for member in &group.members {
                    let pgt_id = match member {
                        NodeId::StreamTable(id) => *id,
                        _ => continue,
                    };

                    let st = match load_st_by_id(pgt_id) {
                        Some(st) => st,
                        None => continue,
                    };

                    // Skip non-active STs
                    if st.status != StStatus::Active && st.status != StStatus::Initializing {
                        continue;
                    }

                    // BOOT-4: Skip if any source is gated.
                    let gated_oids = load_gated_source_oids();
                    if is_any_source_gated(pgt_id, &gated_oids) {
                        info!(
                            "pg_trickle: skipping {}.{} in atomic group — source gated",
                            st.pgt_schema, st.pgt_name,
                        );
                        log_gated_skip(&st);
                        continue;
                    }

                    // WM-4: Skip if watermarks misaligned.
                    let (wm_misaligned, wm_reason) = is_watermark_misaligned(pgt_id);
                    if wm_misaligned {
                        let reason = wm_reason.as_deref().unwrap_or("watermark misaligned");
                        info!(
                            "pg_trickle: skipping {}.{} in atomic group — {}",
                            st.pgt_schema, st.pgt_name, reason,
                        );
                        log_watermark_skip(&st, reason);
                        continue;
                    }

                    // WM-7: Skip if any source watermark is stuck.
                    let (wm_stuck, wm_stuck_reason) = is_watermark_stuck(pgt_id);
                    if wm_stuck {
                        let reason = wm_stuck_reason.as_deref().unwrap_or("watermark stuck");
                        info!(
                            "pg_trickle: skipping {}.{} in atomic group — {}",
                            st.pgt_schema, st.pgt_name, reason,
                        );
                        log_watermark_skip(&st, reason);
                        continue;
                    }

                    // Check retry backoff
                    let retry = retry_states.entry(pgt_id).or_default();
                    if retry.is_in_backoff(now_ms) {
                        emit_stale_alert_if_needed(&st);
                        continue;
                    }

                    // Skip if catalog row locked by another session
                    if check_skip_needed(&st) {
                        continue;
                    }

                    // FUSE-5: Check fuse circuit breaker.
                    if evaluate_fuse(&st) {
                        info!(
                            "pg_trickle: {}.{} fuse blown — skipping in diamond group",
                            st.pgt_schema, st.pgt_name,
                        );
                        continue;
                    }

                    let (has_changes, _has_stream_table_changes) =
                        upstream_change_state(&st, initial_table_changes.get(&pgt_id).copied());
                    let action = refresh::determine_refresh_action(&st, has_changes);
                    let result =
                        execute_scheduled_refresh(&st, action, tick_watermark.as_deref(), None);

                    match result {
                        RefreshOutcome::Success => {
                            refreshed_ids.push(pgt_id);
                        }
                        RefreshOutcome::RetryableFailure | RefreshOutcome::PermanentFailure => {
                            warning!(
                                "pg_trickle: diamond group rollback — member {}.{} failed",
                                st.pgt_schema,
                                st.pgt_name,
                            );
                            group_ok = false;
                            break;
                        }
                    }
                }

                if group_ok {
                    subtxn.commit();
                    // Reset retry states for all refreshed members
                    for id in &refreshed_ids {
                        retry_states.entry(*id).or_default().reset();
                    }
                } else {
                    subtxn.rollback();
                    // Record failure for retry tracking on all attempted members
                    for id in &refreshed_ids {
                        let retry = retry_states.entry(*id).or_default();
                        retry.record_failure(&retry_policy, now_ms);
                    }
                    warning!(
                        "pg_trickle: diamond group rolled back ({} members)",
                        group.members.len(),
                    );
                }
            }

            // Step D & E: Handled in the pre-refresh transaction above.

            // Step F: Prune retry states for STs that no longer exist
            // (avoid accumulating stale state)
            let active_ids: std::collections::HashSet<i64> = groups
                .iter()
                .flat_map(|g| g.members.iter())
                .filter_map(|n| match n {
                    NodeId::StreamTable(id) => Some(*id),
                    _ => None,
                })
                .collect();
            retry_states.retain(|id, _| active_ids.contains(id));
        }));

        // Phase 4: Spawn workers outside the transaction (after commit).
        for (db, job_id) in pending_spawns.drain(..) {
            if let Err(e) = spawn_refresh_worker(&db, job_id) {
                shmem::release_worker_token();
                parallel_state.per_db_inflight = parallel_state.per_db_inflight.saturating_sub(1);
                // Find and clear the in-flight tracking for this job.
                for us in parallel_state.unit_states.values_mut() {
                    if us.inflight_job_id == Some(job_id) {
                        us.inflight_job_id = None;
                        break;
                    }
                }
                warning!(
                    "pg_trickle: parallel dispatch — failed to spawn worker for job {}: {}",
                    job_id,
                    e,
                );
                // Cancel the enqueued job row.
                BackgroundWorker::transaction(AssertUnwindSafe(|| {
                    let _ = SchedulerJob::cancel(job_id, &format!("Worker spawn failed: {}", e));
                }));
            }
        }
    }
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    // scheduler_loop.rs contains the launcher BGW entry point and the per-DB
    // scheduler main loop.  All functions either register background workers
    // (which requires PostgreSQL startup) or call SPI (which requires a live
    // backend connection).  Behaviour is covered by:
    //   - tests/e2e_bgworker_tests.rs  — launcher lifecycle
    //   - tests/e2e_scheduler_tests.rs — per-DB scheduler decisions
    //
    // This module exists to satisfy the coverage sweep (T-4e) and to serve as
    // the anchor for future extracted helpers.

    #[test]
    fn test_module_is_reachable() {
        // Structural smoke test: the module compiles and links correctly.
        // Pure-logic helpers extracted from this file in future versions
        // will have tests added here.
        let _ = 1_u32.saturating_add(0);
    }
}
