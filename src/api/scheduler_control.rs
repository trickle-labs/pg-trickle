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

// ── SQL API ──────────────────────────────────────────────────────────────────

/// API-1 (v0.62.0): Pause the scheduler for the given stream table nodes.
///
/// Marks each node as paused in shared memory so that the scheduler skips
/// dispatching refreshes for it. After setting all pause flags, the call
/// polls `ACTIVE_REFRESH_WORKERS` every 100 ms up to
/// `pg_trickle.scheduler_drain_timeout` seconds.  If refresh workers are
/// still running at the timeout, a WARNING is logged and the function returns
/// (the nodes remain paused for future ticks).
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
        let inline = shmem::database_scheduler_slots()
            .into_iter()
            .find(|slot| slot.database_oid == database_oid)
            .map(|slot| slot.inline_pgt_id != 0 && pgt_ids.contains(&slot.inline_pgt_id))
            .unwrap_or(false);
        if pending == 0 && !inline {
            break;
        }
        if std::time::Instant::now() >= deadline {
            pgrx::error!(
                "pg_trickle: pause_scheduler timed out after {}s with {} relevant job(s) remaining",
                drain_timeout_secs,
                pending
            );
        }
        pgrx::check_for_interrupts!();
        unsafe {
            // SAFETY: pg_usleep is PostgreSQL's bounded backend sleep helper.
            pgrx::pg_sys::pg_usleep(poll_interval.as_micros() as i64);
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
