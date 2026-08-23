//! CQ-10-01 (v0.49.0): Dynamic refresh worker spawn, parallel dispatch state.
//!
//! Extracted from `scheduler/mod.rs` as part of the scheduler decomposition.
//! Contains:
//!   - `spawn_refresh_worker` — spawn a dynamic per-job BGW
//!   - `pg_trickle_refresh_worker_main` — BGW entry point
//!   - `extract_panic_details`, `parse_worker_extra` — pure helpers
//!   - Parallel dispatch state structs and `parallel_dispatch_tick`
//!   - `reap_dead_worker_jobs`, `reconcile_parallel_state`

use std::collections::{HashMap, VecDeque};
use std::panic::AssertUnwindSafe;

use pgrx::bgworkers::*;
use pgrx::prelude::*;

use crate::catalog::{JobStatus, SchedulerJob, StreamTableMeta};
use crate::config;
use crate::dag::{ExecutionUnit, ExecutionUnitDag, ExecutionUnitId, StDag, StStatus};
use crate::error::{RetryPolicy, RetryState};
use crate::shmem;

// Private items from the parent module (scheduler/mod.rs) are accessible
// here because dispatch is a child module of scheduler.
use super::cost::compute_per_db_quota;
use super::tier::RefreshTier;
use super::{
    RefreshOutcome, check_schedule, emit_stale_alert_if_needed, execute_worker_atomic_group,
    execute_worker_cyclic_scc, execute_worker_fused_chain, execute_worker_immediate_closure,
    execute_worker_singleton, is_load_shed_exempt, load_st_by_id,
};

// ── Dynamic Refresh Worker (Phase 3) ──────────────────────────────────────

/// Spawn a dynamic refresh worker for a specific job.
///
/// The coordinator calls this after enqueuing a job and acquiring a worker
/// token from shared memory. The database name and job_id are packed into
/// `bgw_extra` as `"<db_name>|<job_id>"`.
///
/// **Why `|` and not `\0`?** pgrx's `BackgroundWorker::get_extra()` uses
/// `CStr::from_ptr` internally, which treats the first null byte as the
/// string terminator. A null-byte separator would therefore cause the
/// job_id portion to be silently dropped when the worker reads the extra.
/// The pipe character `|` is not a valid PostgreSQL identifier character
/// (without quoting) and is safe to use as a delimiter here.
///
/// Returns `Ok(())` on successful spawn, `Err(...)` if the dynamic worker
/// could not be registered.
pub fn spawn_refresh_worker(
    db_name: &str,
    job_id: i64,
    generation: u64,
) -> Result<(), crate::error::PgTrickleError> {
    // Pack db_name + job_id into bgw_extra (max 128 bytes).
    // Use '|' as separator — see doc comment above for why not '\0'.
    let extra = format!("{db_name}|{job_id}|{generation}");
    if extra.len() > 128 {
        return Err(crate::error::PgTrickleError::InternalError(format!(
            "bgw_extra too long ({} bytes) for db='{}' job_id={}",
            extra.len(),
            db_name,
            job_id
        )));
    }

    BackgroundWorkerBuilder::new("pg_trickle refresh worker")
        .set_function("pg_trickle_refresh_worker_main")
        .set_library("pg_trickle")
        .enable_spi_access()
        .set_extra(&extra)
        .set_restart_time(None) // no auto-restart — coordinator handles retries
        .load_dynamic()
        .map_err(|_| {
            crate::error::PgTrickleError::InternalError(
                "Failed to register dynamic refresh worker".into(),
            )
        })?;

    Ok(())
}

/// Main entry point for a dynamic refresh worker (Phase 3).
///
/// Each refresh worker:
/// 1. Parses `db_name` and `job_id` from `bgw_extra`.
/// 2. Connects to the target database.
/// 3. Claims the job (QUEUED → RUNNING).
/// 4. Executes the execution unit (singleton for now, composite later).
/// 5. Persists the outcome to the job table.
/// 6. Releases the worker token from shared memory.
///
/// # Safety
/// Called directly by PostgreSQL as a background worker entry point.
#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pg_trickle_refresh_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Parse bgw_extra: "db_name\0job_id"
    let extra = BackgroundWorker::get_extra();
    let (db_name, job_id, generation) = match parse_worker_extra(extra) {
        Some(pair) => pair,
        None => {
            warning!(
                "pg_trickle refresh worker: malformed bgw_extra '{}', exiting",
                extra
            );
            return;
        }
    };

    BackgroundWorker::connect_worker_to_spi(Some(&db_name), None);

    // Get our own PID for logging and job claiming.
    let my_pid = BackgroundWorker::transaction(AssertUnwindSafe(|| -> i32 {
        // OBS-5: Tag this connection so it is identifiable in pg_stat_activity.
        // Must be inside a transaction context in a background worker.
        let _ = Spi::run("SET application_name = 'pg_trickle_dispatcher'");
        match Spi::get_one::<i32>("SELECT pg_backend_pid()") {
            Ok(Some(pid)) => pid,
            Ok(None) => {
                pgrx::warning!(
                    "pg_trickle scheduler: pg_backend_pid() returned NULL (db='{}')",
                    db_name
                );
                0
            }
            Err(e) => {
                pgrx::warning!(
                    "pg_trickle scheduler: could not fetch backend pid (db='{}'): {}",
                    db_name,
                    e
                );
                0
            }
        }
    }));

    let database_oid = unsafe { pg_sys::MyDatabaseId.to_u32() };
    if !shmem::promote_worker_slot(database_oid, job_id, generation, my_pid) {
        warning!(
            "pg_trickle refresh worker: reservation missing (db='{}', job_id={}, generation={}), exiting",
            db_name,
            job_id,
            generation
        );
        return;
    }

    info!(
        "pg_trickle refresh worker: started (db='{}', job_id={}, pid={})",
        db_name, job_id, my_pid,
    );

    // Claim the job: QUEUED → RUNNING
    let claimed = BackgroundWorker::transaction(AssertUnwindSafe(|| -> bool {
        match SchedulerJob::claim(job_id, my_pid) {
            Ok(v) => v,
            Err(e) => {
                pgrx::warning!(
                    "pg_trickle scheduler: failed to claim job {} (db='{}', pid={}): {}",
                    job_id,
                    db_name,
                    my_pid,
                    e
                );
                false
            }
        }
    }));

    if !claimed {
        info!(
            "pg_trickle refresh worker: job {} already claimed or cancelled, exiting",
            job_id
        );
        shmem::release_worker_slot(database_oid, job_id, generation);
        return;
    }

    // Load the job details
    let job = BackgroundWorker::transaction(AssertUnwindSafe(|| -> Option<SchedulerJob> {
        SchedulerJob::get_by_id(job_id).ok().flatten()
    }));

    let job = match job {
        Some(j) => j,
        None => {
            warning!(
                "pg_trickle refresh worker: job {} not found after claim, exiting",
                job_id
            );
            BackgroundWorker::transaction(AssertUnwindSafe(|| {
                let _ = SchedulerJob::cancel(job_id, "Job row disappeared after claim");
            }));
            shmem::release_worker_slot(database_oid, job_id, generation);
            return;
        }
    };

    if job.worker_slot_generation != Some(generation as i64) {
        warning!(
            "pg_trickle refresh worker: job {} generation mismatch (worker={}, catalog={}), cancelling",
            job_id,
            generation,
            job.worker_slot_generation.unwrap_or_default()
        );
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            let _ = SchedulerJob::cancel(job_id, "Worker slot generation mismatch");
        }));
        shmem::release_worker_slot(database_oid, job_id, generation);
        return;
    }

    if job.dispatch_tick_id.is_none() || job.tick_watermark_lsn.is_none() {
        warning!(
            "pg_trickle refresh worker: cancelling legacy job {} without immutable tick bound",
            job_id
        );
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            let _ = SchedulerJob::cancel(job_id, "Missing immutable dispatch tick bound");
        }));
        shmem::release_worker_slot(database_oid, job_id, generation);
        return;
    }

    // Validate: DAG version hasn't become obsolete
    let current_dag_version = shmem::current_dag_version();
    if (current_dag_version as i64) > job.dag_version + 1 {
        info!(
            "pg_trickle refresh worker: job {} dag_version={} is stale (current={}), cancelling",
            job_id, job.dag_version, current_dag_version,
        );
        BackgroundWorker::transaction(AssertUnwindSafe(|| {
            let _ = SchedulerJob::cancel(job_id, "DAG version obsolete");
        }));
        shmem::release_worker_slot(database_oid, job_id, generation);
        return;
    }

    // Execute the unit.
    //
    // ERR-1d: Wrap in catch_unwind so that PostgreSQL ERRORs (which pgrx
    // converts to Rust panics via longjmp) don't kill the worker before we
    // can record the failure. When the refresh triggers a PG ERROR (e.g.
    // "column X does not exist"), the BackgroundWorker::transaction's
    // PgTryBuilder rethrows the caught panic, aborting the transaction.
    // Without catch_unwind, the panic propagates to #[pg_guard] and the
    // worker exits — leaving the job stuck in RUNNING and the ST never
    // transitioning to ERROR.
    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
        BackgroundWorker::transaction(AssertUnwindSafe(|| -> RefreshOutcome {
            match job.unit_kind.as_str() {
                "singleton" => execute_worker_singleton(&job),
                "atomic_group" => execute_worker_atomic_group(&job, false),
                "repeatable_read_group" => execute_worker_atomic_group(&job, true),
                "immediate_closure" => execute_worker_immediate_closure(&job),
                "cyclic_scc" => execute_worker_cyclic_scc(&job),
                "fused_chain" => execute_worker_fused_chain(&job),
                _ => {
                    warning!(
                        "pg_trickle refresh worker: unknown unit_kind '{}' for job {}",
                        job.unit_kind,
                        job_id,
                    );
                    RefreshOutcome::PermanentFailure
                }
            }
        }))
    }));

    let outcome = match outcome {
        Ok(o) => (o, None, None),
        Err(panic_payload) => {
            // ERR-1d: The refresh transaction panicked (PG ERROR). Extract
            // the error message from the panic payload and treat this as a
            // permanent failure. The original transaction was rolled back by
            // PostgreSQL, so we record the failure in a fresh transaction.
            let (error_msg, sqlstate) = extract_panic_details(&panic_payload);

            warning!(
                "pg_trickle refresh worker: job {} panicked (PG ERROR): {}",
                job_id,
                error_msg,
            );

            // ERR-1d fix: After a PG ERROR inside BackgroundWorker::transaction,
            // the PostgreSQL transaction state is left in STARTED/ABORT state
            // because CommitTransactionCommand was never called.  We must abort
            // the orphaned transaction before starting a new one, otherwise
            // StartTransactionCommand raises "unexpected state STARTED".
            // SAFETY: AbortCurrentTransaction properly cleans up the aborted
            // transaction and returns PostgreSQL to idle state.
            unsafe {
                pg_sys::AbortCurrentTransaction();
            }

            let failure = crate::error::classify_refresh_failure(sqlstate.as_deref(), &error_msg);

            // Set error state on member STs in a fresh transaction.
            BackgroundWorker::transaction(AssertUnwindSafe(|| {
                for &pgt_id in &job.member_pgt_ids {
                    if failure.retryable && failure.counts_toward_suspension {
                        let tuning = load_st_by_id(pgt_id)
                            .map(|st| st.runtime_tuning())
                            .unwrap_or_else(|| crate::catalog::RefreshRuntimeTuning {
                                merge_work_mem_mb: 0,
                                delta_work_mem_mb: 0,
                                delta_work_mem_cap_mb: 0,
                                lock_backoff_exponent: 0,
                            });
                        let _ = StreamTableMeta::record_scheduled_failure(
                            pgt_id,
                            failure.kind.code(),
                            true,
                            failure.kind == crate::error::RefreshFailureKind::OutOfMemory
                                && config::pg_trickle_self_heal_oom()
                                && tuning.merge_work_mem_mb > 0,
                            failure.kind == crate::error::RefreshFailureKind::LockTimeout
                                && config::pg_trickle_self_heal_lock_timeout(),
                        );
                    } else {
                        let _ = StreamTableMeta::set_typed_error(
                            pgt_id,
                            &failure.message,
                            failure.kind.code(),
                            failure.retryable,
                        );
                    }
                }
            }));

            let outcome = if failure.retryable {
                RefreshOutcome::RetryableFailure
            } else {
                RefreshOutcome::PermanentFailure
            };
            (outcome, Some(error_msg), sqlstate)
        }
    };

    let (outcome, panic_error_msg, panic_sqlstate) = outcome;

    // Persist outcome to the job table
    let (status, retryable) = match outcome {
        RefreshOutcome::Success => (JobStatus::Succeeded, None),
        RefreshOutcome::RetryableFailure => (JobStatus::RetryableFailed, Some(true)),
        RefreshOutcome::PermanentFailure => (JobStatus::PermanentFailed, Some(false)),
    };

    let failure = panic_error_msg
        .as_deref()
        .map(|message| crate::error::classify_refresh_failure(panic_sqlstate.as_deref(), message));
    let outcome_code = failure
        .as_ref()
        .map(|f| f.kind.code())
        .or_else(|| match outcome {
            RefreshOutcome::RetryableFailure => {
                Some(crate::error::RefreshFailureKind::UnknownRetryable.code())
            }
            RefreshOutcome::PermanentFailure => {
                Some(crate::error::RefreshFailureKind::Permanent.code())
            }
            RefreshOutcome::Success => None,
        });
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        let _ = SchedulerJob::complete_typed(
            job_id,
            status,
            panic_error_msg.as_deref(),
            retryable,
            outcome_code,
            failure.as_ref().and_then(|f| f.sqlstate.as_deref()),
        );
    }));

    info!(
        "pg_trickle refresh worker: job {} finished with {} (db='{}')",
        job_id, status, db_name,
    );

    // Release the cluster-wide worker token
    shmem::release_worker_slot(database_oid, job_id, generation);
}

/// ERR-1d: Extract a human-readable error message from a caught panic payload.
///
/// Panics from PostgreSQL ERRORs are represented as `CaughtError` in pgrx.
/// We try to downcast to known types; if that fails, we produce a generic message.
pub(super) fn extract_panic_details(
    payload: &Box<dyn std::any::Any + Send>,
) -> (String, Option<String>) {
    use pgrx::pg_sys::panic::CaughtError;

    if let Some(caught) = payload.downcast_ref::<CaughtError>() {
        return match caught {
            CaughtError::PostgresError(ereport)
            | CaughtError::ErrorReport(ereport)
            | CaughtError::RustPanic { ereport, .. } => (
                format!("{:?}", ereport),
                Some(crate::error::sqlstate_to_string(
                    ereport.sql_error_code() as isize as u32,
                )),
            ),
        };
    }
    if let Some(msg) = payload.downcast_ref::<&str>() {
        return (msg.to_string(), None);
    }
    if let Some(msg) = payload.downcast_ref::<String>() {
        return (msg.clone(), None);
    }
    (
        "unknown error (panic payload could not be decoded)".to_string(),
        None,
    )
}

/// Parse the worker's bgw_extra string: "db_name\0job_id".
pub(super) fn parse_worker_extra(extra: &str) -> Option<(String, i64, u64)> {
    // Format is "<db_name>|<job_id>" — see spawn_refresh_worker for why '|'
    // is used instead of '\0' (pgrx's get_extra() truncates at null bytes).
    let parts: Vec<&str> = extra.split('|').collect();
    if parts.len() != 3 {
        return None;
    }
    let db_name = parts[0].to_string();
    let job_id = parts[1].parse::<i64>().ok()?;
    let generation = parts[2].parse::<u64>().ok()?;
    if db_name.is_empty() || job_id <= 0 || generation == 0 {
        return None;
    }
    Some((db_name, job_id, generation))
}

// ── Parallel Dispatch State (Phase 4) ─────────────────────────────────────

/// DAG-2: Minimum adaptive poll interval (ms) when workers are in-flight.
pub(super) const ADAPTIVE_POLL_MIN_MS: u64 = 20;
/// DAG-2: Maximum adaptive poll interval (ms) — the old fixed cap.
const ADAPTIVE_POLL_MAX_MS: u64 = 200;

/// DAG-2: Compute the next adaptive poll interval.
///
/// Uses exponential backoff: starts at `ADAPTIVE_POLL_MIN_MS` after a worker
/// completion, doubles each tick with no completion, and caps at
/// `ADAPTIVE_POLL_MAX_MS`. When no workers are in-flight, returns the full
/// `base_interval_ms` (the scheduler GUC value).
///
/// This is a pure function for unit-testability.
pub(super) fn compute_adaptive_poll_ms(
    current_poll_ms: u64,
    had_completion: bool,
    has_inflight: bool,
    base_interval_ms: u64,
) -> u64 {
    if !has_inflight {
        return base_interval_ms;
    }
    if had_completion {
        return ADAPTIVE_POLL_MIN_MS;
    }
    // Exponential backoff: double the current interval, capped.
    let next = current_poll_ms.saturating_mul(2);
    std::cmp::min(next, ADAPTIVE_POLL_MAX_MS)
}

/// Per-unit coordinator state for the parallel dispatch loop.
#[derive(Debug, Default)]
pub(super) struct UnitDispatchState {
    /// Number of upstream units that must succeed before this unit is ready.
    pub(super) remaining_upstreams: usize,
    /// In-flight job ID, if a worker is currently executing this unit.
    pub(super) inflight_job_id: Option<i64>,
    /// Whether this unit completed successfully in the current cycle.
    pub(super) succeeded: bool,
}

/// Coordinator-side state for the parallel dispatch loop.
///
/// Lives outside the SPI transaction and persists across scheduler ticks.
/// Rebuilt when the DAG version changes.
pub(super) struct ParallelDispatchState {
    /// Per-unit tracking, keyed by ExecutionUnitId.
    pub(super) unit_states: HashMap<ExecutionUnitId, UnitDispatchState>,
    /// Number of refresh workers in-flight for this database coordinator.
    pub(super) per_db_inflight: u32,
    /// DAG version this dispatch state was built from.
    pub(super) dag_version: u64,
    /// The execution unit DAG (rebuilt on DAG version change).
    pub(super) eu_dag: Option<ExecutionUnitDag>,
    /// DAG-2: Current adaptive poll interval (ms). Doubles each tick without
    /// worker completions; resets to `ADAPTIVE_POLL_MIN_MS` on completion.
    pub(super) adaptive_poll_ms: u64,
    /// DAG-2: Number of worker completions observed in the last dispatch tick.
    pub(super) completions_this_tick: u32,
}

impl ParallelDispatchState {
    pub(super) fn new() -> Self {
        Self {
            unit_states: HashMap::new(),
            per_db_inflight: 0,
            dag_version: 0,
            eu_dag: None,
            adaptive_poll_ms: ADAPTIVE_POLL_MIN_MS,
            completions_this_tick: 0,
        }
    }

    /// Whether there are in-flight jobs (for shorter poll interval).
    pub(super) fn has_inflight(&self) -> bool {
        self.per_db_inflight > 0
    }

    /// Rebuild dispatch state from a fresh EU DAG.
    pub(super) fn rebuild(&mut self, eu_dag: ExecutionUnitDag, dag_version: u64) {
        // Count how many old units had in-flight jobs that will be orphaned
        // by clearing unit_states.  Subtract these from per_db_inflight
        // because the orphaned jobs will be reaped by the dead-worker scan
        // (Step 0) or will complete and be found via catalog poll. Without
        // this correction, per_db_inflight permanently drifts upward on
        // each rebuild, eventually blocking all new dispatches.
        let orphaned_inflight = self
            .unit_states
            .values()
            .filter(|us| us.inflight_job_id.is_some())
            .count() as u32;
        self.per_db_inflight = self.per_db_inflight.saturating_sub(orphaned_inflight);

        self.unit_states.clear();
        self.dag_version = dag_version;

        for unit in eu_dag.units() {
            let upstream_count = eu_dag.get_upstream_units(unit.id).len();
            self.unit_states.insert(
                unit.id,
                UnitDispatchState {
                    remaining_upstreams: upstream_count,
                    inflight_job_id: None,
                    succeeded: false,
                },
            );
        }
        self.eu_dag = Some(eu_dag);
    }
}

/// Check if an execution unit is due for refresh.
///
/// A unit is due if any member ST is due (schedule or upstream changes).
fn is_unit_due(unit: &ExecutionUnit, dag: &StDag) -> bool {
    unit.member_pgt_ids.iter().any(|&pgt_id| {
        load_st_by_id(pgt_id)
            .map(|st| {
                (st.status == StStatus::Active || st.status == StStatus::Initializing)
                    && (check_schedule(&st, dag) || st.needs_reinit)
            })
            .unwrap_or(false)
    })
}

// ── C3-1: Per-database worker quota helpers ───────────────────────────────

/// C3-1: Sort an execution unit ready queue by dispatch priority.
///
/// Priority ordering (lowest number = dispatched first):
/// 1. `ImmediateClosure` — transactional consistency, must not be delayed.
/// 2. `AtomicGroup` / `RepeatableReadGroup` — consistency group members.
/// 3. `Singleton` — normal scheduled stream tables.
/// 4. `CyclicScc` — fixpoint groups, lowest urgency.
///
/// Topological order is preserved within each priority tier because the
/// input queue was built by iterating `topo_order`.
///
/// The secondary sort key is `tier_priority`: tier values should be
/// `0 = Hot` (highest urgency), `1 = Warm`, `2 = Cold`.  Pass an empty map
/// when tier information is unavailable (all units treated as Hot).
pub(super) fn sort_ready_queue_by_priority(
    queue: VecDeque<ExecutionUnitId>,
    eu_dag: &ExecutionUnitDag,
    tier_priorities: &HashMap<ExecutionUnitId, u8>,
) -> VecDeque<ExecutionUnitId> {
    use crate::dag::ExecutionUnitKind;
    fn unit_priority(kind: ExecutionUnitKind) -> u8 {
        match kind {
            ExecutionUnitKind::ImmediateClosure => 0,
            ExecutionUnitKind::AtomicGroup | ExecutionUnitKind::RepeatableReadGroup => 1,
            ExecutionUnitKind::FusedChain => 1, // same priority as atomic groups
            ExecutionUnitKind::Singleton => 2,
            ExecutionUnitKind::CyclicScc => 3,
        }
    }
    // C3-1: Tier priority: Hot(0) > Warm(1) > Cold(2).  ImmediateClosure EUs
    // are already priority 0 by kind and do not need a tier secondary key.
    let default_tier: u8 = 0; // default to Hot when not in map
    let mut items: Vec<ExecutionUnitId> = queue.into_iter().collect();
    // stable sort preserves topological order within each priority tier.
    items.sort_by_key(|&uid| {
        let (kind_prio, tier_prio) = eu_dag
            .units()
            .find(|u| u.id == uid)
            .map(|u| {
                let kp = unit_priority(u.kind);
                // ImmediateClosure ignores tier — it always wins.
                let tp = if kp == 0 {
                    0u8
                } else {
                    *tier_priorities.get(&uid).unwrap_or(&default_tier)
                };
                (kp, tp)
            })
            .unwrap_or((u8::MAX, default_tier));
        (kind_prio, tier_prio)
    });
    items.into_iter().collect()
}

/// C3-1: Compute the "best" (most-urgent) tier priority for an execution unit.
///
/// Iterates the unit's member STs, loads each, and returns the minimum tier
/// priority value (Hot=0, Warm=1, Cold=2).  Returns `0` (Hot) for any unit
/// whose members cannot be loaded so they are never accidentally deprioritised.
///
/// Pure logic in the sense that it only reads catalog data that has already
/// been loaded; the `load_st_by_id` calls are cheap (SPI reads, already cached
/// by the catalog layer during this same scheduler tick).
fn compute_unit_tier_priority(member_pgt_ids: &[i64]) -> u8 {
    let mut best: u8 = 2; // Cold — start at lowest urgency
    for &pgt_id in member_pgt_ids {
        let tier_prio = match load_st_by_id(pgt_id) {
            Some(st) => match RefreshTier::from_sql_str(&st.refresh_tier) {
                RefreshTier::Hot => 0,
                RefreshTier::Warm => 1,
                RefreshTier::Cold | RefreshTier::Frozen => 2,
            },
            None => 0, // default to Hot urgency when ST cannot be loaded
        };
        if tier_prio < best {
            best = tier_prio;
        }
        if best == 0 {
            break; // Hot is the highest urgency; short-circuit
        }
    }
    best
}

/// Run one tick of the parallel dispatch loop.
///
/// Called from the main scheduler loop when `parallel_refresh_mode == On`.
/// Polls completed jobs, updates readiness, and enqueues new jobs.
/// Workers to spawn are appended to `pending_spawns`.
#[allow(clippy::too_many_arguments)]
pub(super) fn parallel_dispatch_tick(
    state: &mut ParallelDispatchState,
    dag: &StDag,
    now_ms: u64,
    retry_states: &mut HashMap<i64, RetryState>,
    retry_policy: &RetryPolicy,
    db_name: &str,
    pending_spawns: &mut Vec<(String, i64, u64)>,
    dispatch_tick_id: i64,
    tick_watermark_lsn: Option<&str>,
    load_deferred: bool,
) {
    let eu_dag = match &state.eu_dag {
        Some(d) => d,
        None => return,
    };

    let max_cluster = config::pg_trickle_max_dynamic_refresh_workers().max(1) as u32;
    let par_workers = config::pg_trickle_max_parallel_workers();
    let postgres_parallel_workers = match Spi::get_one::<i32>(
        "SELECT setting::int FROM pg_settings WHERE name = 'max_parallel_workers'",
    ) {
        Ok(Some(value)) => value.max(0) as u32,
        Ok(None) => {
            warning!("pg_trickle: max_parallel_workers is not available");
            return;
        }
        Err(e) => {
            warning!("pg_trickle: could not read max_parallel_workers: {}", e);
            return;
        }
    };
    let capacity = shmem::WorkerCapacity::new(
        max_cluster,
        (par_workers > 0).then_some(par_workers as u32),
        postgres_parallel_workers,
    );
    let effective_max_cluster = capacity.effective_limit;
    // C3-1: Per-database quota with burst capacity.
    let max_per_db = compute_per_db_quota(
        config::pg_trickle_per_database_worker_quota(),
        config::pg_trickle_max_concurrent_refreshes(),
        effective_max_cluster,
        shmem::active_worker_count(),
    );
    let dag_version_i64 = state.dag_version as i64;
    // SAFETY: MyProcPid is always valid inside a background worker.
    let scheduler_pid: i32 = unsafe { pg_sys::MyProcPid };
    // SAFETY: MyDatabaseId is valid in a connected scheduler backend.
    let database_oid = unsafe { pg_sys::MyDatabaseId.to_u32() };

    // ── Step 0: Reap orphaned RUNNING jobs whose worker died ─────────────
    // A background worker can die (OOM, crash, etc.) while its job is still
    // RUNNING in the catalog. The in-memory inflight tracking may have been
    // lost during a DAG rebuild, so we do a DB-level scan: cancel any
    // RUNNING job whose worker_pid is no longer in pg_stat_activity.
    let reaped = reap_dead_worker_jobs();
    if reaped > 0 {
        info!(
            "pg_trickle: parallel dispatch — reaped {} orphaned job(s) with dead workers",
            reaped,
        );

        let reclaimed = match (
            live_refresh_worker_pids(),
            live_scheduler_pids(),
            queued_job_ids(),
        ) {
            (Some(live_pids), Some(scheduler_pids), Some(queued_ids)) => {
                shmem::reap_stale_worker_slots(
                    unsafe { pg_sys::MyDatabaseId.to_u32() },
                    &live_pids,
                    &scheduler_pids,
                    &queued_ids,
                )
            }
            _ => 0,
        };
        if reclaimed > 0 {
            info!(
                "pg_trickle: parallel dispatch — reclaimed {} stale worker slot(s)",
                reclaimed
            );
        }
    }

    // ── Step 1: Poll completed jobs and process outcomes ──────────────────
    let inflight_ids: Vec<(ExecutionUnitId, i64)> = state
        .unit_states
        .iter()
        .filter_map(|(&uid, us)| us.inflight_job_id.map(|jid| (uid, jid)))
        .collect();

    for (unit_id, job_id) in inflight_ids {
        let job = match SchedulerJob::get_by_id(job_id) {
            Ok(Some(j)) => j,
            Ok(None) => {
                warning!(
                    "pg_trickle: parallel dispatch — job {} vanished, treating as failure",
                    job_id,
                );
                if let Some(us) = state.unit_states.get_mut(&unit_id) {
                    us.inflight_job_id = None;
                }
                state.per_db_inflight = state.per_db_inflight.saturating_sub(1);
                continue;
            }
            Err(e) => {
                warning!(
                    "pg_trickle: parallel dispatch — error polling job {}: {}",
                    job_id,
                    e,
                );
                continue; // Try again next tick
            }
        };

        if !job.status.is_terminal() {
            continue; // Still running
        }

        // Job completed — process outcome.
        if let Some(us) = state.unit_states.get_mut(&unit_id) {
            us.inflight_job_id = None;
        }
        state.per_db_inflight = state.per_db_inflight.saturating_sub(1);
        // DAG-2: Track completion for adaptive poll reset.
        state.completions_this_tick += 1;

        let root_pgt_id = eu_dag
            .units()
            .find(|u| u.id == unit_id)
            .map(|u| u.root_pgt_id);

        match job.status {
            JobStatus::Succeeded => {
                if let Some(us) = state.unit_states.get_mut(&unit_id) {
                    us.succeeded = true;
                }
                if let Some(rpid) = root_pgt_id {
                    retry_states.entry(rpid).or_default().reset();
                }

                // Advance downstream readiness.
                for ds_id in eu_dag.get_downstream_units(unit_id) {
                    if let Some(ds_state) = state.unit_states.get_mut(&ds_id) {
                        ds_state.remaining_upstreams =
                            ds_state.remaining_upstreams.saturating_sub(1);
                    }
                }

                info!(
                    "pg_trickle: parallel dispatch — unit completed (job {})",
                    job_id,
                );
            }
            JobStatus::RetryableFailed => {
                if let Some(rpid) = root_pgt_id {
                    let retry = retry_states.entry(rpid).or_default();
                    let will_retry = retry.record_failure(retry_policy, now_ms);
                    if will_retry {
                        warning!(
                            "pg_trickle: parallel dispatch — job {} failed (retryable), backoff {}ms",
                            job_id,
                            retry_policy.backoff_ms(retry.attempts - 1),
                        );
                    } else {
                        // Retry budget exhausted — unblock downstream units
                        // so the wave can complete. Without this, downstreams
                        // remain permanently blocked because the failed unit
                        // never sets succeeded=true and remaining_upstreams
                        // is never decremented.
                        warning!(
                            "pg_trickle: parallel dispatch — job {} retries exhausted, unblocking downstreams",
                            job_id,
                        );
                        if let Some(us) = state.unit_states.get_mut(&unit_id) {
                            us.succeeded = true;
                        }
                        for ds_id in eu_dag.get_downstream_units(unit_id) {
                            if let Some(ds_state) = state.unit_states.get_mut(&ds_id) {
                                ds_state.remaining_upstreams =
                                    ds_state.remaining_upstreams.saturating_sub(1);
                            }
                        }
                    }
                }
            }
            JobStatus::PermanentFailed | JobStatus::Cancelled => {
                if let Some(rpid) = root_pgt_id {
                    retry_states.entry(rpid).or_default().reset();
                }
                warning!(
                    "pg_trickle: parallel dispatch — job {} failed permanently",
                    job_id,
                );

                // ERR-1d: Ensure ERROR status is set on member STs for permanent
                // failures. The worker may have already set this via the
                // catch_unwind path, but this is a safety net in case the
                // worker died before completing the error-state UPDATE.
                if job.status == JobStatus::PermanentFailed {
                    // PERF-5: O(1) lookup via unit_by_id.
                    let unit = eu_dag.unit_by_id(unit_id);
                    if let Some(unit) = unit {
                        let error_detail = job
                            .outcome_detail
                            .as_deref()
                            .unwrap_or("Refresh failed permanently");
                        for &pgt_id in &unit.member_pgt_ids {
                            // Only set ERROR if not already in ERROR state.
                            if let Some(st) = load_st_by_id(pgt_id)
                                && st.status != StStatus::Error
                            {
                                let _ = StreamTableMeta::set_error_state(pgt_id, error_detail);
                            }
                        }
                    }
                }

                // Mark this unit as "succeeded" so the wave can complete.
                // The refresh itself already recorded the failure in
                // pgt_refresh_history, but we must unblock downstream
                // units — otherwise a single permanent failure silently
                // stalls every transitive downstream for the remainder of
                // the scheduler's lifetime.
                if let Some(us) = state.unit_states.get_mut(&unit_id) {
                    us.succeeded = true;
                }
                for ds_id in eu_dag.get_downstream_units(unit_id) {
                    if let Some(ds_state) = state.unit_states.get_mut(&ds_id) {
                        ds_state.remaining_upstreams =
                            ds_state.remaining_upstreams.saturating_sub(1);
                    }
                }
            }
            _ => {}
        }
    }

    // ── Step 2: Build ready queue ────────────────────────────────────────
    let mut ready_queue: VecDeque<ExecutionUnitId> = VecDeque::new();

    // Use topological order for deterministic upstream-first priority.
    let topo_order = match eu_dag.topological_order() {
        Ok(order) => order,
        Err(e) => {
            warning!("pg_trickle: parallel dispatch — topo order failed: {}", e);
            return;
        }
    };

    for &uid in &topo_order {
        let us = match state.unit_states.get(&uid) {
            Some(s) => s,
            None => continue,
        };

        // Skip completed, in-flight, or dependency-blocked units.
        if us.succeeded || us.inflight_job_id.is_some() || us.remaining_upstreams > 0 {
            continue;
        }

        // PERF-5: O(1) lookup via unit_by_id.
        let unit = match eu_dag.unit_by_id(uid) {
            Some(u) => u,
            None => continue,
        };

        // API-1/2: A paused member must not enter a parallel refresh job.
        if unit
            .member_pgt_ids
            .iter()
            .any(|&pgt_id| shmem::is_node_paused_for_database(database_oid, pgt_id))
        {
            continue;
        }

        // Check retry backoff.
        let retry = retry_states.entry(unit.root_pgt_id).or_default();
        if retry.is_in_backoff(now_ms) {
            // Emit stale alerts for members in backoff.
            for &pgt_id in &unit.member_pgt_ids {
                if let Some(st) = load_st_by_id(pgt_id) {
                    emit_stale_alert_if_needed(&st);
                }
            }
            continue;
        }

        // Check if unit is due for refresh.
        if !is_unit_due(unit, dag) {
            continue;
        }

        // Load shedding applies equally to parallel and sequential paths.
        // Committed source changes remain buffered and drain after pressure falls.
        if load_deferred
            && !unit
                .member_pgt_ids
                .iter()
                .any(|&pgt_id| load_st_by_id(pgt_id).is_none_or(|st| is_load_shed_exempt(&st)))
        {
            continue;
        }

        // Guard against duplicate in-flight jobs.
        match SchedulerJob::has_inflight_job(&unit.stable_key()) {
            Ok(true) => continue,
            Ok(false) => {}
            Err(e) => {
                warning!(
                    "pg_trickle: parallel dispatch — inflight check error for {}: {}",
                    unit.label,
                    e,
                );
                continue;
            }
        }

        ready_queue.push_back(uid);
    }

    // C3-1: Build tier priority map for the ready queue.
    // For each EU in the ready queue, compute the "best" (most urgent) tier
    // among its member STs.  This is the secondary sort key used below.
    // We only compute for units that are actually ready so the map stays small.
    // PERF-5: O(1) lookup via unit_by_id.
    let tier_map: HashMap<ExecutionUnitId, u8> = ready_queue
        .iter()
        .filter_map(|&uid| {
            eu_dag
                .unit_by_id(uid)
                .map(|u| (uid, compute_unit_tier_priority(&u.member_pgt_ids)))
        })
        .collect();

    // C3-1: Priority sort — IMMEDIATE closures first for transactional safety,
    // then atomic groups, singletons (Hot > Warm > Cold), cyclic SCCs.
    // Topological order within each priority tier is preserved.
    let ready_queue = sort_ready_queue_by_priority(ready_queue, eu_dag, &tier_map);

    // ── Step 3: Dispatch ready units within budget ───────────────────────
    let mut ready_queue = ready_queue;
    while let Some(uid) = ready_queue.pop_front() {
        if state.per_db_inflight >= max_per_db {
            break;
        }

        // PERF-5: O(1) lookup via unit_by_id.
        let unit = match eu_dag.unit_by_id(uid) {
            Some(u) => u,
            None => continue,
        };

        let tick_watermark_lsn = match tick_watermark_lsn {
            Some(lsn) => lsn,
            None => continue,
        };
        // Reserve capacity before creating a durable job row. A queued row
        // without a permit would be indistinguishable from dispatchable work.
        let Some(slot) =
            shmem::reserve_worker_slot(capacity, database_oid, 0, scheduler_pid, now_ms)
        else {
            info!(
                "pg_trickle: parallel dispatch — worker capacity exhausted ({}/{})",
                shmem::worker_slot_counts().0 + shmem::worker_slot_counts().1,
                capacity.effective_limit,
            );
            continue;
        };
        let job_id = match SchedulerJob::enqueue(
            dag_version_i64,
            &unit.stable_key(),
            unit.kind.as_str(),
            &unit.member_pgt_ids,
            unit.root_pgt_id,
            scheduler_pid,
            1,
            dispatch_tick_id,
            tick_watermark_lsn,
        ) {
            Ok(id) => id,
            Err(e) => {
                shmem::release_worker_slot(database_oid, 0, slot.generation);
                warning!(
                    "pg_trickle: parallel dispatch — failed to enqueue job for {}: {}",
                    unit.label,
                    e,
                );
                continue;
            }
        };
        // OBS-2: A new job has been added to the parallel queue.
        crate::shmem::increment_parallel_queue_depth_for_database(database_oid);
        if !shmem::bind_worker_slot(database_oid, scheduler_pid, slot.generation, job_id) {
            shmem::release_worker_slot(database_oid, 0, slot.generation);
            let _ = SchedulerJob::cancel(job_id, "worker permit binding failed");
            crate::shmem::decrement_parallel_queue_depth_for_database(database_oid);
            warning!(
                "pg_trickle: parallel dispatch — failed to bind permit to job {}",
                job_id
            );
            continue;
        }
        if let Err(e) = SchedulerJob::set_worker_slot_generation(job_id, slot.generation) {
            shmem::release_worker_slot(database_oid, job_id, slot.generation);
            let _ = SchedulerJob::cancel(job_id, "failed to persist worker slot generation");
            crate::shmem::decrement_parallel_queue_depth_for_database(unsafe {
                pg_sys::MyDatabaseId.to_u32()
            });
            warning!(
                "pg_trickle: parallel dispatch — failed to persist slot generation for job {}: {}",
                job_id,
                e
            );
            continue;
        }

        if let Some(us) = state.unit_states.get_mut(&uid) {
            us.inflight_job_id = Some(job_id);
        }
        state.per_db_inflight += 1;
        pending_spawns.push((db_name.to_string(), job_id, slot.generation));

        info!(
            "pg_trickle: parallel dispatch — enqueued {} (job_id={}, kind={})",
            unit.label, job_id, unit.kind,
        );
    }

    // ── Step 4: Reset cycle state when the wave is fully complete ────────
    // A dispatch wave completes when all in-flight workers have finished AND
    // nothing new was dispatched in Step 3 (per_db_inflight is still 0 after
    // the dispatch loop).  At that point `succeeded = true` on every unit
    // that ran this wave, and we can clear those flags so units become
    // eligible for the next scheduled wave.
    //
    // Placing the reset HERE (after dispatch) rather than before Step 2 is
    // critical for cascade correctness.  With a two-unit cascade A → B:
    //   • A completes in Step 1 → per_db_inflight drops to 0, B.remaining=0.
    //   • Step 2 sees B ready; Step 3 dispatches B → per_db_inflight becomes 1.
    //   • Reset check: per_db_inflight == 1 → no reset.  B runs correctly.
    // If the reset were placed before Step 2, per_db_inflight would be 0 at
    // that point (B not yet dispatched), causing B.remaining_upstreams to be
    // restored to 1 — blocking B forever in a cascade.
    if state.per_db_inflight == 0 {
        let any_done = state.unit_states.values().any(|us| us.succeeded);
        if any_done {
            for (&uid, us) in state.unit_states.iter_mut() {
                us.succeeded = false;
                us.remaining_upstreams = eu_dag.get_upstream_units(uid).len();
            }
            info!(
                "pg_trickle: parallel dispatch — wave complete, reset {} unit(s)",
                state.unit_states.len()
            );
        }
    }
}

/// Reap RUNNING jobs whose background worker process has died.
///
/// Called at the start of each parallel dispatch tick to detect orphaned
/// RUNNING jobs whose worker_pid is no longer in pg_stat_activity.
/// This handles the case where a worker crashes (OOM, segfault, etc.)
/// while its job status remains RUNNING in the catalog — which would
/// otherwise permanently block scheduling for that execution unit.
///
/// Returns the number of jobs reaped.
fn reap_dead_worker_jobs() -> i64 {
    Spi::get_one::<i64>(
        "WITH reaped AS ( \
             UPDATE pgtrickle.pgt_scheduler_jobs \
             SET status = 'RETRYABLE_FAILED', \
                 finished_at = now(), \
                 outcome_detail = 'Worker process died (reaped by scheduler)', \
                 retryable = true \
             WHERE status = 'RUNNING' \
               AND worker_pid IS NOT NULL \
               AND NOT EXISTS ( \
                   SELECT 1 FROM pg_stat_activity \
                   WHERE pid = pgt_scheduler_jobs.worker_pid \
               ) \
             RETURNING job_id \
         ) SELECT count(*)::bigint FROM reaped",
    )
    .unwrap_or(Some(0))
    .unwrap_or(0)
}

fn live_refresh_worker_pids() -> Option<Vec<i32>> {
    Spi::get_one::<Vec<i32>>(
        "SELECT COALESCE(array_agg(pid), ARRAY[]::int[]) \
         FROM pg_stat_activity \
         WHERE backend_type = 'pg_trickle refresh worker'",
    )
    .unwrap_or_else(|e| {
        warning!("pg_trickle: could not enumerate refresh worker PIDs: {}", e);
        None
    })
}

fn live_scheduler_pids() -> Option<Vec<i32>> {
    Spi::get_one::<Vec<i32>>(
        "SELECT COALESCE(array_agg(pid), ARRAY[]::int[]) \
         FROM pg_stat_activity \
         WHERE application_name = 'pg_trickle_scheduler'",
    )
    .unwrap_or_else(|e| {
        warning!("pg_trickle: could not enumerate scheduler PIDs: {}", e);
        None
    })
}

fn queued_job_ids() -> Option<Vec<i64>> {
    Spi::get_one::<Vec<i64>>(
        "SELECT COALESCE(array_agg(job_id), ARRAY[]::bigint[]) \
         FROM pgtrickle.pgt_scheduler_jobs \
         WHERE status = 'QUEUED'",
    )
    .unwrap_or_else(|e| {
        warning!("pg_trickle: could not enumerate queued jobs: {}", e);
        None
    })
}

/// Reconcile orphaned jobs and worker tokens at scheduler startup.
///
/// Called once during per-database scheduler initialization:
/// 1. Cancels orphaned QUEUED/RUNNING jobs whose PID is no longer alive.
/// 2. Computes the true active worker count from `pg_stat_activity`.
/// 3. Corrects the shared-memory token counter if it diverged.
pub fn reconcile_parallel_state() {
    // Step 1: Cancel orphaned jobs
    match SchedulerJob::cancel_orphaned_jobs() {
        Ok(count) if count > 0 => {
            info!(
                "pg_trickle: parallel reconciliation — cancelled {} orphaned job(s)",
                count
            );
        }
        Ok(_) => {}
        Err(e) => {
            warning!(
                "pg_trickle: parallel reconciliation — orphan cleanup error: {}",
                e
            );
        }
    }

    // Step 2: Reclaim slots for workers that disappeared during a crash.
    let reclaimed = match (
        live_refresh_worker_pids(),
        live_scheduler_pids(),
        queued_job_ids(),
    ) {
        (Some(live_pids), Some(scheduler_pids), Some(queued_ids)) => {
            shmem::reap_stale_worker_slots(
                unsafe { pg_sys::MyDatabaseId.to_u32() },
                &live_pids,
                &scheduler_pids,
                &queued_ids,
            )
        }
        _ => 0,
    };
    if reclaimed > 0 {
        info!(
            "pg_trickle: parallel reconciliation — reclaimed {} stale worker slot(s)",
            reclaimed
        );
        shmem::bump_reconcile_epoch();
    }

    // Step 3: Prune old completed jobs (keep last 1 hour)
    match SchedulerJob::prune_completed(
        config::pg_trickle_scheduler_job_retention_seconds() as i64,
        config::pg_trickle_scheduler_maintenance_batch_size(),
    ) {
        Ok(count) if count > 0 => {
            info!(
                "pg_trickle: parallel reconciliation — pruned {} old job(s)",
                count
            );
        }
        Ok(_) => {}
        Err(e) => {
            warning!("pg_trickle: parallel reconciliation — prune error: {}", e);
        }
    }
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_worker_extra ────────────────────────────────────────────────

    #[test]
    fn test_parse_worker_extra_valid() {
        let (db, job_id, generation) = parse_worker_extra("mydb|42|7").unwrap();
        assert_eq!(db, "mydb");
        assert_eq!(job_id, 42);
        assert_eq!(generation, 7);
    }

    #[test]
    fn test_parse_worker_extra_valid_large_job_id() {
        let (db, job_id, generation) = parse_worker_extra("production_db|9999999|8").unwrap();
        assert_eq!(db, "production_db");
        assert_eq!(job_id, 9999999);
        assert_eq!(generation, 8);
    }

    #[test]
    fn test_parse_worker_extra_db_with_hyphens() {
        let (db, job_id, generation) = parse_worker_extra("my-app-db|100|9").unwrap();
        assert_eq!(db, "my-app-db");
        assert_eq!(job_id, 100);
        assert_eq!(generation, 9);
    }

    #[test]
    fn test_parse_worker_extra_missing_separator() {
        assert!(parse_worker_extra("mydb42").is_none());
    }

    #[test]
    fn test_parse_worker_extra_empty_string() {
        assert!(parse_worker_extra("").is_none());
    }

    #[test]
    fn test_parse_worker_extra_empty_db_name() {
        // Empty db_name is rejected.
        assert!(parse_worker_extra("|42|1").is_none());
    }

    #[test]
    fn test_parse_worker_extra_negative_job_id() {
        // Negative job_id is rejected (job_id <= 0 check).
        assert!(parse_worker_extra("mydb|-1|1").is_none());
    }

    #[test]
    fn test_parse_worker_extra_zero_job_id() {
        // Zero job_id is rejected.
        assert!(parse_worker_extra("mydb|0|1").is_none());
    }

    #[test]
    fn test_parse_worker_extra_non_numeric_job_id() {
        assert!(parse_worker_extra("mydb|abc|1").is_none());
    }

    #[test]
    fn test_parse_worker_extra_extra_separators_in_db_name() {
        // splitn(2, '|') means anything after the first '|' is the job_id field.
        // A db name containing '|' is not valid but the second '|' becomes part
        // of the job_id string which fails to parse as i64.
        assert!(parse_worker_extra("my|db|42").is_none());
    }

    // ── compute_adaptive_poll_ms ──────────────────────────────────────────

    #[test]
    fn test_compute_adaptive_poll_no_inflight_returns_base() {
        // When no workers are in-flight, return the full base interval.
        assert_eq!(compute_adaptive_poll_ms(100, false, false, 5000), 5000);
        assert_eq!(compute_adaptive_poll_ms(20, true, false, 1000), 1000);
    }

    #[test]
    fn test_compute_adaptive_poll_completion_resets_to_min() {
        // After a worker completes, reset to ADAPTIVE_POLL_MIN_MS.
        assert_eq!(
            compute_adaptive_poll_ms(200, true, true, 5000),
            ADAPTIVE_POLL_MIN_MS
        );
    }

    #[test]
    fn test_compute_adaptive_poll_doubles_each_tick() {
        // With in-flight workers and no completion: double, capped at 200.
        assert_eq!(compute_adaptive_poll_ms(20, false, true, 5000), 40);
        assert_eq!(compute_adaptive_poll_ms(40, false, true, 5000), 80);
        assert_eq!(compute_adaptive_poll_ms(80, false, true, 5000), 160);
        assert_eq!(compute_adaptive_poll_ms(160, false, true, 5000), 200);
        // Already at max — stays capped.
        assert_eq!(compute_adaptive_poll_ms(200, false, true, 5000), 200);
    }

    #[test]
    fn test_compute_adaptive_poll_min_boundary() {
        assert_eq!(
            ADAPTIVE_POLL_MIN_MS, 20,
            "min poll interval should be 20 ms"
        );
        assert_eq!(
            compute_adaptive_poll_ms(0, false, true, 5000),
            0,
            "saturating_mul(2) of 0 stays 0"
        );
    }

    // ── ParallelDispatchState ─────────────────────────────────────────────

    #[test]
    fn test_parallel_dispatch_state_new_no_inflight() {
        let state = ParallelDispatchState::new();
        assert!(
            !state.has_inflight(),
            "new state should have no in-flight workers"
        );
        assert_eq!(state.per_db_inflight, 0);
        assert_eq!(state.dag_version, 0);
        assert_eq!(state.adaptive_poll_ms, ADAPTIVE_POLL_MIN_MS);
        assert_eq!(state.completions_this_tick, 0);
    }
}
