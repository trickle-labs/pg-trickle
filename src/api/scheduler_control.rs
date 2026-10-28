//! API-1/2 (v0.62.0): Per-node scheduler pause/resume control.
//!
//! `pause_scheduler(nodes)` marks each named stream table as paused in shared
//! memory. The scheduler will skip dispatching refreshes for a paused node on
//! every subsequent tick. Once all named nodes are paused, the function waits
//! up to `pg_trickle.scheduler_drain_timeout` seconds for any in-flight refresh
//! workers to finish before returning.
//!
//! `resume_scheduler(nodes)` removes each named node from the paused set so
//! that the scheduler resumes dispatching refreshes normally on the next tick.

use pgrx::prelude::*;

use crate::catalog::StreamTableMeta;
use crate::config;
use crate::error::PgTrickleError;
use crate::shmem;

/// Parse a possibly schema-qualified name into `(schema, name)`.
///
/// Falls back to the session's `current_schema()` when no schema prefix is
/// present.
fn resolve_node_name(qualified: &str) -> Result<(String, String), PgTrickleError> {
    let parts: Vec<&str> = qualified.splitn(2, '.').collect();
    match parts.len() {
        1 => {
            let schema = Spi::get_one::<String>("SELECT current_schema()::text")
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_else(|| "public".to_string());
            Ok((schema, parts[0].to_string()))
        }
        2 => Ok((parts[0].to_string(), parts[1].to_string())),
        _ => Err(PgTrickleError::InvalidArgument(format!(
            "invalid stream table name: {qualified}"
        ))),
    }
}

/// Resolve a possibly-qualified stream table name to its `pgt_id`.
fn pgt_id_for_name(name: &str) -> Result<i64, PgTrickleError> {
    let (schema, table) = resolve_node_name(name)?;
    let meta = StreamTableMeta::get_by_name(&schema, &table)?;
    Ok(meta.pgt_id)
}

fn current_database_oid() -> Result<u32, PgTrickleError> {
    Spi::get_one::<pg_sys::Oid>(
        "SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .map(|oid| oid.to_u32())
    .ok_or_else(|| PgTrickleError::NotFound("current database".to_string()))
}

/// Hold admission through commit, including work inside internal subtransactions.
pub(crate) fn try_lock_scheduler_targets(pgt_ids: &[i64]) -> Result<bool, PgTrickleError> {
    // Collisions every 2^32 IDs only conservatively delay unrelated work.
    let mut keys: Vec<i32> = pgt_ids.iter().map(|id| *id as i32).collect();
    keys.sort_unstable();
    keys.dedup();
    Spi::get_one_with_args::<bool>(
        "SELECT COALESCE(bool_and(pg_catalog.pg_try_advisory_xact_lock(1346851928, lock_id)), true) \
         FROM unnest($1::integer[]) AS targets(lock_id)",
        &[keys.into()],
    )
    .map_err(|error| PgTrickleError::SpiError(format!("scheduler admission for {pgt_ids:?}: {error}")))?
    .ok_or_else(|| PgTrickleError::InternalError("scheduler admission returned no decision".into()))
}

/// Recheck shared pause flags after acquiring the same locks used by drain.
pub(crate) fn admit_scheduler_targets(pgt_ids: &[i64]) -> bool {
    // Scheduled writes can synchronously maintain IMMEDIATE descendants.
    let targets = Spi::get_one_with_args::<Vec<i64>>(
        "WITH RECURSIVE write_targets(pgt_id,pgt_relid) AS ( \
             SELECT pgt_id,pgt_relid FROM pgtrickle.pgt_stream_tables WHERE pgt_id=ANY($1::bigint[]) \
             UNION \
             SELECT child.pgt_id,child.pgt_relid FROM write_targets parent \
             JOIN pgtrickle.pgt_dependencies dep ON dep.source_relid=parent.pgt_relid \
                 AND dep.source_type='STREAM_TABLE' \
             JOIN pgtrickle.pgt_stream_tables child ON child.pgt_id=dep.pgt_id \
                 AND child.refresh_mode='IMMEDIATE' \
         ) SELECT ARRAY(SELECT pgt_id FROM write_targets UNION SELECT unnest($1::bigint[]))",
        &[pgt_ids.to_vec().into()],
    )
    .unwrap_or_else(|error| pgrx::error!("scheduler write closure for {pgt_ids:?}: {error}"))
    .unwrap_or_else(|| pgrx::error!("scheduler write closure returned no membership"));
    // SAFETY: admission runs in a connected scheduler or refresh worker backend.
    let database_oid = unsafe { pg_sys::MyDatabaseId }.to_u32();
    let paused = || {
        targets
            .iter()
            .any(|id| shmem::is_node_paused_for_database(database_oid, *id))
    };
    if paused() {
        return false;
    }
    try_lock_scheduler_targets(&targets).unwrap_or_else(|error| pgrx::error!("{error}"))
        && !paused()
}

// ── SQL API ──────────────────────────────────────────────────────────────────

/// API-1 (v0.62.0): Pause the scheduler for the given stream table nodes.
///
/// Marks each node as paused in shared memory so that the scheduler skips
/// dispatching refreshes for it. After setting all pause flags, the call
/// checks matching scheduler jobs and admitted transaction locks every 100 ms up to
/// `pg_trickle.scheduler_drain_timeout` seconds. If matching work remains at
/// the timeout, the call errors and the nodes remain paused for future ticks.
///
/// Example:
/// ```sql
/// SELECT pgtrickle.pause_scheduler(ARRAY['public.my_view', 'analytics.summary']);
/// ```
#[pg_extern(schema = "pgtrickle")]
pub fn pause_scheduler(nodes: pgrx::Array<&str>) -> &'static str {
    let database_oid = current_database_oid().unwrap_or_else(|e| pgrx::error!("{}", e));
    let names: Vec<&str> = nodes
        .iter()
        .map(|name| {
            name.ok_or_else(|| {
                PgTrickleError::InvalidArgument(
                    "pause_scheduler() target array must not contain NULL".into(),
                )
            })
        })
        .collect::<Result<_, _>>()
        .unwrap_or_else(|e| pgrx::error!("{}", e));
    if names.is_empty() {
        pgrx::error!("pause_scheduler() target array must not be empty");
    }
    let max_targets = config::pg_trickle_max_control_targets();
    if names.len() > max_targets {
        pgrx::error!("pause_scheduler() accepts at most {max_targets} targets");
    }

    // Resolve and validate every target before changing shared state.
    let mut resolved = Vec::with_capacity(names.len());
    let mut seen = std::collections::HashSet::with_capacity(names.len());
    for name in names {
        if name.trim().is_empty() {
            pgrx::error!("pause_scheduler() target names cannot be empty");
        }
        let pgt_id = pgt_id_for_name(name).unwrap_or_else(|e| {
            pgrx::error!(
                "pg_trickle: pause_scheduler: could not resolve '{}': {}",
                name,
                e
            )
        });
        if !seen.insert(pgt_id) {
            pgrx::error!("pause_scheduler() contains duplicate target '{name}'");
        }
        resolved.push((pgt_id, name));
    }
    let pgt_ids: Vec<i64> = resolved.iter().map(|(pgt_id, _)| *pgt_id).collect();
    if let Err(e) = shmem::pause_nodes_for_database(database_oid, &pgt_ids) {
        pgrx::error!("pg_trickle: pause_scheduler: {}", e);
    }
    for &(pgt_id, name) in &resolved {
        pgrx::log!(
            "pg_trickle: pause_scheduler: node '{}' (pgt_id={}) marked as paused",
            name,
            pgt_id
        );
    }

    if resolved.is_empty() {
        return "OK";
    }

    // Wait for any in-flight refresh workers to drain.
    let drain_timeout_secs = config::pg_trickle_scheduler_drain_timeout();
    let poll_interval = std::time::Duration::from_millis(100);
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(drain_timeout_secs as u64);

    loop {
        let pending = Spi::get_one_with_args::<i64>(
            "SELECT count(*)::bigint \
             FROM pgtrickle.pgt_scheduler_jobs \
             WHERE status IN ('QUEUED', 'RUNNING') \
               AND member_pgt_ids && $1::bigint[]",
            &[pgt_ids.clone().into()],
        )
        .unwrap_or_else(|e| pgrx::error!("pg_trickle: pause_scheduler: {}", e))
        .unwrap_or(0);
        let admitted =
            try_lock_scheduler_targets(&pgt_ids).unwrap_or_else(|error| pgrx::error!("{error}"));
        if pending == 0 && admitted {
            break;
        }
        if std::time::Instant::now() >= deadline {
            pgrx::error!(
                "pg_trickle: pause_scheduler timed out after {}s with {} relevant job(s) remaining (admitted work drained={})",
                drain_timeout_secs,
                pending,
                admitted
            );
        }
        pgrx::check_for_interrupts!();
        unsafe {
            // SAFETY: pg_usleep is PostgreSQL's bounded backend sleep helper.
            pgrx::pg_sys::pg_usleep(poll_interval.as_micros() as std::os::raw::c_long);
        }
    }

    "OK"
}

/// API-2 (v0.62.0): Resume the scheduler for the given stream table nodes.
///
/// Removes each node from the paused set in shared memory. The scheduler will
/// resume dispatching refreshes for the node on the next tick.
///
/// Example:
/// ```sql
/// SELECT pgtrickle.resume_scheduler(ARRAY['public.my_view']);
/// ```
#[pg_extern(schema = "pgtrickle")]
pub fn resume_scheduler(nodes: pgrx::Array<&str>) -> &'static str {
    let database_oid = current_database_oid().unwrap_or_else(|e| pgrx::error!("{}", e));
    let names: Vec<&str> = nodes
        .iter()
        .map(|name| {
            name.ok_or_else(|| {
                PgTrickleError::InvalidArgument(
                    "resume_scheduler() target array must not contain NULL".into(),
                )
            })
        })
        .collect::<Result<_, _>>()
        .unwrap_or_else(|e| pgrx::error!("{}", e));
    if names.is_empty() {
        pgrx::error!("resume_scheduler() target array must not be empty");
    }
    let max_targets = config::pg_trickle_max_control_targets();
    if names.len() > max_targets {
        pgrx::error!("resume_scheduler() accepts at most {max_targets} targets");
    }
    let mut resolved = Vec::with_capacity(names.len());
    let mut seen = std::collections::HashSet::with_capacity(names.len());
    for name in names {
        if name.trim().is_empty() {
            pgrx::error!("resume_scheduler() target names cannot be empty");
        }
        let pgt_id = pgt_id_for_name(name).unwrap_or_else(|e| {
            pgrx::error!(
                "pg_trickle: resume_scheduler: could not resolve '{}': {}",
                name,
                e
            )
        });
        if !seen.insert(pgt_id) {
            pgrx::error!("resume_scheduler() contains duplicate target '{name}'");
        }
        resolved.push((pgt_id, name));
    }
    let pgt_ids: Vec<i64> = resolved.iter().map(|(pgt_id, _)| *pgt_id).collect();
    shmem::resume_nodes_for_database(database_oid, &pgt_ids);
    for (pgt_id, name) in resolved {
        pgrx::log!(
            "pg_trickle: resume_scheduler: node '{}' (pgt_id={}) removed from paused set",
            name,
            pgt_id
        );
    }

    "OK"
}
