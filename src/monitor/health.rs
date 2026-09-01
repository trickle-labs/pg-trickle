//! Health check and summary (v0.55.0 decomposition).
// Extracted from src/monitor.rs in v0.55.0 module decomposition.
// All shared helpers and types are in monitor/mod.rs (use super::*).

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
struct WindowStateHealthEntry {
    pgt_id: i64,
    stream_table: String,
    node_ordinal: i32,
    spec_ordinal: i32,
    status: String,
    total_estimated_bytes: u64,
    first_for_stream: bool,
}

fn build_window_state_health_rows(
    entries: &[WindowStateHealthEntry],
    budget_bytes: u64,
) -> Vec<(String, String, String)> {
    let mut rows = Vec::new();
    for entry in entries {
        if entry.status != "READY" {
            let (severity, action) = if entry.status == "OVER_BUDGET" {
                (
                    "WARN",
                    "affected partitions use bounded recomputation until state fits the budget",
                )
            } else {
                (
                    "ERROR",
                    "run pgtrickle.repair_stream_table() before differential execution",
                )
            };
            rows.push((
                format!(
                    "window_state_{}_{}_{}",
                    entry.pgt_id, entry.node_ordinal, entry.spec_ordinal
                ),
                severity.into(),
                format!(
                    "{} node {} spec {} status={}; {}",
                    entry.stream_table,
                    entry.node_ordinal,
                    entry.spec_ordinal,
                    entry.status,
                    action
                ),
            ));
        }

        let utilization = entry
            .total_estimated_bytes
            .saturating_mul(100)
            .checked_div(budget_bytes.max(1))
            .unwrap_or(u64::MAX);
        if entry.first_for_stream && utilization >= 80 {
            rows.push((
                format!("window_state_budget_{}", entry.pgt_id),
                if utilization >= 100 { "ERROR" } else { "WARN" }.into(),
                format!(
                    "{} window state uses {} / {} bytes ({}%); new partitions recompute before the hard bound is crossed",
                    entry.stream_table,
                    entry.total_estimated_bytes,
                    budget_bytes,
                    utilization
                ),
            ));
        }
    }
    rows
}

/// A single-query health overview of the pg_trickle installation.
///
/// Returns one row per check. Each row has a `severity` of `OK`, `WARN`, or
/// `ERROR` and a human-readable `detail`. Run this to get an instant triage
/// of whether anything needs attention.
///
/// Checks performed:
/// - `scheduler_running`    — background worker is alive
/// - `error_tables`         — any stream tables in ERROR/SUSPENDED status
/// - `stale_tables`         — any stream tables where last_refresh_at age exceeds schedule (scheduler behind)
/// - `needs_reinit`         — any stream tables awaiting reinitialization
/// - `consecutive_errors`   — any stream tables accumulating errors (not yet suspended)
/// - `buffer_growth`        — any CDC change buffer with > 10 000 pending rows
/// - `slot_lag`             — any WAL replication slot retaining > 100 MB of WAL
/// - `dvm_fallbacks`        — any DVM fallback refreshes in the last hour (reason codes)
/// - `ring_overflow_trend`  — whether the invalidation ring has overflowed since startup
/// - `window_state_*`       — invalid or over-80-percent private window state
///
/// Exposed as `pgtrickle.health_check()`.
#[pg_extern(schema = "pgtrickle", name = "health_check")]
fn health_check() -> TableIterator<
    'static,
    (
        name!(check_name, String),
        name!(severity, String),
        name!(detail, String),
    ),
> {
    let mut rows: Vec<(String, String, String)> = Vec::new();

    Spi::connect(|client| {
        // ── 1. Scheduler running ────────────────────────────────────────────
        // Check launcher first, then the per-database scheduler for this DB.
        // Distinguishing the two lets us emit a precise, actionable message:
        //   - Launcher absent  → ERROR  (shared_preload_libraries / enabled)
        //   - Launcher present, no per-DB scheduler → WARN (transient start-up)
        //   - Per-DB scheduler present               → OK
        let launcher_count = client
            .select(
                "SELECT count(*)::int FROM pg_stat_activity \
                 WHERE backend_type = 'pg_trickle launcher'",
                None,
                &[],
            )
            .ok()
            .and_then(|r| r.first().get::<i32>(1).unwrap_or(None))
            .unwrap_or(0);

        let scheduler_count = client
            .select(
                "SELECT count(*)::int FROM pg_stat_activity \
                 WHERE backend_type = 'pg_trickle scheduler' \
                   AND datname = current_database()",
                None,
                &[],
            )
            .ok()
            .and_then(|r| r.first().get::<i32>(1).unwrap_or(None))
            .unwrap_or(0);

        let (sev, detail) = if scheduler_count > 0 {
            ("OK", format!("{} worker(s) running", scheduler_count))
        } else if launcher_count > 0 {
            (
                "WARN",
                "pg_trickle launcher is running but no per-database scheduler found \
                 for this database yet. The launcher will spawn one within ~10 s. \
                 If this persists beyond 1 minute check the PostgreSQL server log \
                 for 'pg_trickle launcher' messages."
                    .to_string(),
            )
        } else {
            (
                "ERROR",
                "No pg_trickle launcher or scheduler background worker found in \
                 pg_stat_activity. Check that pg_trickle is in \
                 shared_preload_libraries and pg_trickle.enabled = on."
                    .to_string(),
            )
        };
        rows.push(("scheduler_running".to_string(), sev.to_string(), detail));

        // ── 2. ERROR / SUSPENDED stream tables ─────────────────────────────
        let bad_tables: Vec<String> = client
            .select(
                "SELECT pgt_schema || '.' || pgt_name \
                 FROM pgtrickle.pgt_stream_tables \
                 WHERE status IN ('ERROR', 'SUSPENDED') \
                 ORDER BY 1",
                None,
                &[],
            )
            .map(|r| {
                r.filter_map(|row| row.get::<String>(1).unwrap_or(None))
                    .collect()
            })
            .unwrap_or_default();

        let (sev, detail) = if bad_tables.is_empty() {
            (
                "OK".to_string(),
                "All stream tables are ACTIVE or INITIALIZING".to_string(),
            )
        } else {
            (
                "ERROR".to_string(),
                format!(
                    "{} stream table(s) in ERROR/SUSPENDED: {}",
                    bad_tables.len(),
                    bad_tables.join(", ")
                ),
            )
        };
        rows.push(("error_tables".to_string(), sev, detail));

        // ── 3. Stale stream tables ──────────────────────────────────────────
        let stale_tables: Vec<String> = client
            .select(
                "SELECT pgt_schema || '.' || pgt_name \
                 FROM pgtrickle.stream_tables_info \
                 WHERE stale = true \
                 ORDER BY 1",
                None,
                &[],
            )
            .map(|r| {
                r.filter_map(|row| row.get::<String>(1).unwrap_or(None))
                    .collect()
            })
            .unwrap_or_default();

        let (sev, detail) = if stale_tables.is_empty() {
            ("OK".to_string(), "No stale stream tables".to_string())
        } else {
            (
                "WARN".to_string(),
                format!(
                    "{} stale stream table(s) (scheduler behind its schedule): {}",
                    stale_tables.len(),
                    stale_tables.join(", ")
                ),
            )
        };
        rows.push(("stale_tables".to_string(), sev, detail));

        // ── 4. needs_reinit ─────────────────────────────────────────────────
        let reinit_tables: Vec<String> = client
            .select(
                "SELECT pgt_schema || '.' || pgt_name \
                 FROM pgtrickle.pgt_stream_tables \
                 WHERE needs_reinit = true \
                 ORDER BY 1",
                None,
                &[],
            )
            .map(|r| {
                r.filter_map(|row| row.get::<String>(1).unwrap_or(None))
                    .collect()
            })
            .unwrap_or_default();

        let (sev, detail) = if reinit_tables.is_empty() {
            (
                "OK".to_string(),
                "No stream tables awaiting reinitialization".to_string(),
            )
        } else {
            (
                "WARN".to_string(),
                format!(
                    "{} stream table(s) need reinitialization (DDL change detected): {}",
                    reinit_tables.len(),
                    reinit_tables.join(", ")
                ),
            )
        };
        rows.push(("needs_reinit".to_string(), sev, detail));

        // ── 5. Consecutive errors (not yet suspended) ───────────────────────
        let erroring_tables: Vec<String> = client
            .select(
                "SELECT pgt_schema || '.' || pgt_name \
                 FROM pgtrickle.pgt_stream_tables \
                 WHERE consecutive_errors > 0 AND status NOT IN ('ERROR', 'SUSPENDED') \
                 ORDER BY consecutive_errors DESC",
                None,
                &[],
            )
            .map(|r| {
                r.filter_map(|row| row.get::<String>(1).unwrap_or(None))
                    .collect()
            })
            .unwrap_or_default();

        let (sev, detail) = if erroring_tables.is_empty() {
            (
                "OK".to_string(),
                "No stream tables accumulating refresh errors".to_string(),
            )
        } else {
            (
                "WARN".to_string(),
                format!(
                    "{} stream table(s) have consecutive errors (approaching auto-suspend): {}",
                    erroring_tables.len(),
                    erroring_tables.join(", ")
                ),
            )
        };
        rows.push(("consecutive_errors".to_string(), sev, detail));

        // ── 6. CDC buffer growth (> 10 000 pending rows for any source) ─────
        let bloated: Vec<String> = client
            .select(
                "SELECT source_table || ' (' || pending_rows || ' pending)' \
                 FROM pgtrickle.change_buffer_sizes() \
                 WHERE pending_rows > 10000 \
                 ORDER BY pending_rows DESC",
                None,
                &[],
            )
            .map(|r| {
                r.filter_map(|row| row.get::<String>(1).unwrap_or(None))
                    .collect()
            })
            .unwrap_or_default();

        let (sev, detail) = if bloated.is_empty() {
            (
                "OK".to_string(),
                "All CDC buffers within normal range".to_string(),
            )
        } else {
            (
                "WARN".to_string(),
                format!(
                    "{} CDC buffer(s) exceeding 10 000 pending rows — differential refresh may be stalled: {}",
                    bloated.len(),
                    bloated.join(", ")
                ),
            )
        };
        rows.push(("buffer_growth".to_string(), sev, detail));

        // ── 7. WAL slot lag ─────────────────────────────────────────────────
        let threshold_bytes = config::pg_trickle_slot_lag_warning_threshold_bytes();
        let lagging: Vec<String> = client
            .select(
                "SELECT slot_name || ' (' || pg_size_pretty(retained_wal_bytes) || ')' \
                 FROM pgtrickle.slot_health() \
                 WHERE retained_wal_bytes > $1 \
                 ORDER BY retained_wal_bytes DESC",
                None,
                &[threshold_bytes.into()],
            )
            .map(|r| {
                r.filter_map(|row| row.get::<String>(1).unwrap_or(None))
                    .collect()
            })
            .unwrap_or_default();

        let (sev, detail) = build_slot_lag_health_detail(&lagging, threshold_bytes);
        rows.push(("slot_lag".to_string(), sev, detail));

        // ── 8. Worker pool utilization (parallel refresh) ───────────────────
        let mode = config::pg_trickle_parallel_refresh_mode();
        if mode != config::ParallelRefreshMode::Off {
            let active = shmem::active_worker_count();
            let max_workers = config::pg_trickle_max_dynamic_refresh_workers().max(1) as u32;

            let (sev, detail) = if active >= max_workers {
                (
                    "WARN".to_string(),
                    format!(
                        "Worker pool saturated: {}/{} tokens in use — \
                         new refresh jobs will be queued until a worker finishes. \
                         Consider increasing pg_trickle.max_dynamic_refresh_workers.",
                        active, max_workers,
                    ),
                )
            } else {
                (
                    "OK".to_string(),
                    format!(
                        "{}/{} worker tokens in use (mode={})",
                        active, max_workers, mode,
                    ),
                )
            };
            rows.push(("worker_pool".to_string(), sev, detail));

            // ── 9. Queued job backlog (parallel refresh) ────────────────────
            let queued_count = client
                .select(
                    "SELECT count(*)::int FROM pgtrickle.pgt_scheduler_jobs \
                     WHERE status = 'QUEUED'",
                    None,
                    &[],
                )
                .ok()
                .and_then(|r| r.first().get::<i32>(1).unwrap_or(None))
                .unwrap_or(0);

            let (sev, detail) = if queued_count > 10 {
                (
                    "WARN".to_string(),
                    format!(
                        "{} jobs queued — refresh work is backing up. \
                         Workers may be overloaded or failing.",
                        queued_count,
                    ),
                )
            } else if queued_count > 0 {
                (
                    "OK".to_string(),
                    format!("{} job(s) waiting in queue", queued_count),
                )
            } else {
                ("OK".to_string(), "No jobs queued".to_string())
            };
            rows.push(("job_queue".to_string(), sev, detail));
        }

        // ── DX-1 (v0.61.0): Outbox owner check ───────────────────────────────
        // Publication provenance is checked separately below by immutable OID;
        // this query retains the legacy outbox ownership check.
        let owner_mismatches: Vec<String> = client
            .select(
                "SELECT \
                     st.pgt_schema || '.' || st.pgt_name AS st_fqn, \
                     'outbox'::text AS attachment_type, \
                     ob_role.rolname AS attachment_owner, \
                     st_role.rolname AS table_owner \
                 FROM pgtrickle.pgt_outbox_config oc \
                 JOIN pgtrickle.pgt_stream_tables st \
                   ON st.pgt_id = oc.stream_table_oid::bigint \
                 JOIN pg_catalog.pg_class  st_cls  ON st_cls.oid  = st.pgt_relid \
                 JOIN pg_catalog.pg_roles  st_role ON st_role.oid = st_cls.relowner \
                 JOIN pg_catalog.pg_class  ob_cls  ON ob_cls.relname = oc.tide_outbox_name \
                 JOIN pg_catalog.pg_roles  ob_role ON ob_role.oid    = ob_cls.relowner \
                 WHERE ob_role.rolname IS DISTINCT FROM st_role.rolname \
                 ORDER BY 1",
                None,
                &[],
            )
            .map(|r| {
                r.map(|row| {
                    let fqn = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
                    let typ = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
                    let attached_by = row.get::<String>(3).unwrap_or(None).unwrap_or_default();
                    let owned_by = row.get::<String>(4).unwrap_or(None).unwrap_or_default();
                    format!(
                        "ST \"{fqn}\" has a {typ} attached by role \"{attached_by}\" \
                         but owned by \"{owned_by}\""
                    )
                })
                .collect()
            })
            .unwrap_or_default();

        let (sev, detail) = if owner_mismatches.is_empty() {
            (
                "OK".to_string(),
                "No outbox ownership mismatches".to_string(),
            )
        } else {
            (
                "WARNING".to_string(),
                format!(
                    "{} attachment(s) created by non-owner: {}. \
                     Review outbox/publication created by non-owner; \
                     consider re-attaching as owner or granting explicit permissions.",
                    owner_mismatches.len(),
                    owner_mismatches.join("; ")
                ),
            )
        };
        rows.push(("attachment_owner_check".to_string(), sev, detail));

        // ── O-1 (v0.80.0): DVM fallback (forced-full) refreshes in recent history
        // Alert when any stream table was forced to a FULL refresh (was_full_fallback)
        // within the last hour. Frequent fallbacks indicate queries that should be
        // reviewed or rewritten to restore differential refresh.
        let dvm_fallback_result = client
            .select(
                "SELECT count(*)::int \
                 FROM pgtrickle.pgt_refresh_history \
                 WHERE was_full_fallback = TRUE \
                   AND start_time >= now() - interval '1 hour'",
                None,
                &[],
            )
            .ok();

        let fallback_count = dvm_fallback_result
            .map(|r| {
                let row = r.first();
                row.get::<i32>(1).unwrap_or(None).unwrap_or(0)
            })
            .unwrap_or(0);

        let (sev, detail) = if fallback_count > 0 {
            (
                "WARN".to_string(),
                format!(
                    "{} forced-full (DVM fallback) refresh(es) in the last hour. \
                     Review affected stream tables — consider rewriting queries \
                     or enabling is_append_only=true where applicable.",
                    fallback_count
                ),
            )
        } else {
            (
                "OK".to_string(),
                "No forced-full DVM fallback refreshes in the last hour".to_string(),
            )
        };
        rows.push(("dvm_fallbacks".to_string(), sev, detail));

        // ── O-2 (v0.80.0): Invalidation ring overflow trend ──────────────────
        // Alert when the invalidation ring has overflowed since startup. Each
        // overflow means a DDL event exceeded ring capacity and forced a full
        // DAG rebuild (expensive). Suggest raising invalidation_ring_capacity.
        let overflow_count = crate::shmem::invalidation_ring_overflow_count();
        let (sev, detail) = if overflow_count > 0 {
            (
                "WARN".to_string(),
                format!(
                    "{} invalidation ring overflow(s) since startup — DDL burst \
                     events exceeded ring capacity and triggered full DAG rebuilds. \
                     Consider raising pg_trickle.invalidation_ring_capacity \
                     (current: {}).",
                    overflow_count,
                    crate::config::pg_trickle_invalidation_ring_capacity(),
                ),
            )
        } else {
            (
                "OK".to_string(),
                "Invalidation ring has not overflowed since startup".to_string(),
            )
        };
        rows.push(("ring_overflow_trend".to_string(), sev.to_string(), detail));
    });

    // Registry-only window checks stay bounded by the number of semantic
    // window specifications. They never scan a dynamic state relation.
    let window_state_entries = Spi::connect(|client| {
        client
            .select(
                "SELECT ws.pgt_id, st.pgt_schema || '.' || st.pgt_name, \
                        ws.node_ordinal, ws.spec_ordinal, ws.status, \
                        SUM(ws.estimated_bytes) OVER (PARTITION BY ws.pgt_id)::bigint, \
                        ROW_NUMBER() OVER ( \
                            PARTITION BY ws.pgt_id \
                            ORDER BY ws.node_ordinal, ws.spec_ordinal \
                        ) = 1 \
                   FROM pgtrickle.pgt_window_states ws \
                   JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = ws.pgt_id \
                  ORDER BY ws.pgt_id, ws.node_ordinal, ws.spec_ordinal",
                None,
                &[],
            )?
            .map(|row| {
                Ok(WindowStateHealthEntry {
                    pgt_id: row.get::<i64>(1)?.unwrap_or_default(),
                    stream_table: row.get::<String>(2)?.unwrap_or_default(),
                    node_ordinal: row.get::<i32>(3)?.unwrap_or_default(),
                    spec_ordinal: row.get::<i32>(4)?.unwrap_or_default(),
                    status: row.get::<String>(5)?.unwrap_or_default(),
                    total_estimated_bytes: row.get::<i64>(6)?.unwrap_or_default().max(0) as u64,
                    first_for_stream: row.get::<bool>(7)?.unwrap_or(false),
                })
            })
            .collect::<Result<Vec<_>, pgrx::spi::SpiError>>()
    })
    .unwrap_or_default();
    rows.extend(build_window_state_health_rows(
        &window_state_entries,
        crate::config::pg_trickle_memory_budget().total_bytes,
    ));

    if let Some((severity, detail)) = crate::api::publication::publication_binding_health_summary()
    {
        rows.push(("publication_bindings".to_string(), severity, detail));
    }

    // v0.90.0: expose controller status without reading refresh history. The
    // state table is bounded one row per interval-targeted stream table.
    let freshness_rows = Spi::connect(|client| {
        client
            .select(
                "SELECT st.pgt_id, st.pgt_schema::text || '.' || st.pgt_name::text,
                        COALESCE(f.sla_status, 'INSUFFICIENT_DATA')::text,
                        f.p95_freshness_ms, st.freshness_deadline_ms
                   FROM pgtrickle.pgt_stream_tables st
                   LEFT JOIN pgtrickle.pgt_freshness_controller_state f
                     ON f.pgt_id = st.pgt_id
                  WHERE st.target_freshness_mode = 'INTERVAL'
                  ORDER BY st.pgt_id",
                None,
                &[],
            )
            .ok()
            .map(|result| {
                result
                    .filter_map(|row| {
                        Some((
                            row.get::<i64>(1).ok().flatten()?,
                            row.get::<String>(2).ok().flatten()?,
                            row.get::<String>(3).ok().flatten()?,
                            row.get::<f64>(4).ok().flatten(),
                            row.get::<i64>(5).ok().flatten()?,
                        ))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    });
    let mut freshness_evidence_unavailable = 0;
    let mut freshness_breaching = 0;
    let mut freshness_infeasible = 0;
    for (pgt_id, stream_table, status, p95, target) in freshness_rows {
        let severity = match status.as_str() {
            "MEETING" => "OK",
            "INSUFFICIENT_DATA" => "WARN",
            "EVIDENCE_UNAVAILABLE" | "NOT_APPLICABLE" => "WARN",
            _ => "ERROR",
        };
        rows.push((
            format!("freshness_sla_{pgt_id}"),
            severity.to_string(),
            format!(
                "{stream_table}: status={status}; p95_ms={}; target_ms={target}",
                p95.map(|value| format!("{value:.3}"))
                    .unwrap_or_else(|| "unavailable".into())
            ),
        ));
        freshness_evidence_unavailable += if status == "EVIDENCE_UNAVAILABLE" {
            1
        } else {
            0
        };
        freshness_breaching += if status == "BREACHING" { 1 } else { 0 };
        freshness_infeasible += if status == "INFEASIBLE" { 1 } else { 0 };
    }
    rows.push((
        "freshness_evidence".to_string(),
        if freshness_evidence_unavailable == 0 {
            "OK"
        } else {
            "ERROR"
        }
        .to_string(),
        format!(
            "{} interval target(s) lack exact commit provenance",
            freshness_evidence_unavailable
        ),
    ));
    rows.push((
        "freshness_breach".to_string(),
        if freshness_breaching == 0 {
            "OK"
        } else {
            "WARN"
        }
        .to_string(),
        format!("{} interval target(s) are breaching", freshness_breaching),
    ));
    rows.push((
        "freshness_infeasible".to_string(),
        if freshness_infeasible == 0 {
            "OK"
        } else {
            "WARN"
        }
        .to_string(),
        format!("{} interval target(s) are infeasible", freshness_infeasible),
    ));
    rows.push((
        "freshness_override".to_string(),
        "OK".to_string(),
        "No safety-capped freshness overrides are recorded.".to_string(),
    ));
    rows.push((
        "adaptive_workers".to_string(),
        if crate::config::pg_trickle_adaptive_workers_max()
            < crate::config::pg_trickle_adaptive_workers_min()
        {
            "ERROR"
        } else {
            "OK"
        }
        .to_string(),
        format!(
            "adaptive worker bounds={}..{}",
            crate::config::pg_trickle_adaptive_workers_min(),
            crate::config::pg_trickle_adaptive_workers_max()
        ),
    ));

    // v0.87: report the closest observed pg_trickle-owned memory component.
    // This is deliberately bounded and uses existing live counters; it is not
    // a claim about PostgreSQL-wide RSS.
    let budget = crate::config::pg_trickle_memory_budget();
    let pipeline_metrics = crate::shmem::pipeline_metrics();
    let change_buffer_bytes = Spi::get_one::<i64>(
        "SELECT COALESCE(max(buffer_bytes), 0)::bigint \
         FROM pgtrickle.change_buffer_sizes()",
    )
    .ok()
    .flatten()
    .unwrap_or(0)
    .max(0) as u64;
    let component_values = [
        (
            "delta_pipeline",
            pipeline_metrics.largest_bytes,
            budget.delta_pipeline_bytes,
        ),
        (
            "template_plan_cache",
            crate::shmem::template_cache_bytes(),
            budget.template_plan_cache_bytes,
        ),
        (
            "dag_queue",
            crate::shmem::parallel_queue_depth().saturating_mul(256),
            budget.dag_queue_bytes,
        ),
        (
            "invalidation_ring",
            crate::shmem::invalidation_ring_bytes(),
            budget.invalidation_ring_bytes,
        ),
        (
            "change_buffer",
            change_buffer_bytes,
            budget.change_buffer_bytes,
        ),
    ];
    let (component, used, limit) = component_values
        .into_iter()
        .max_by_key(|(_, used, limit)| used.saturating_mul(10_000).checked_div(*limit).unwrap_or(0))
        .unwrap_or(("delta_pipeline", 0, budget.delta_pipeline_bytes));
    let utilization = used
        .saturating_mul(100)
        .checked_div(limit.max(1))
        .unwrap_or(0);
    let severity = if utilization >= 100 {
        "ERROR"
    } else if utilization >= 80 {
        "WARN"
    } else {
        "OK"
    };
    rows.push((
        "memory_budget".to_string(),
        severity.to_string(),
        format!(
            "closest component={component}; observed/high-water={used} bytes; limit={limit} bytes; utilization={utilization}%"
        ),
    ));

    let (pressure, deferrals, factor) = crate::shmem::scheduler_load_stats();
    let threshold = crate::config::pg_trickle_load_shed_threshold();
    rows.push((
        "scheduler_load".to_string(),
        if threshold > 0.0 && pressure >= threshold {
            "WARN".to_string()
        } else {
            "OK".to_string()
        },
        format!(
            "pressure={pressure:.3}; threshold={threshold:.3}; deferral_factor={factor}; cumulative_deferrals={deferrals}"
        ),
    ));

    TableIterator::new(rows)
}

// ── UX-4: Single-endpoint health summary ────────────────────────────────────

/// Return a single-row summary of the entire pg_trickle deployment's health.
///
/// Aggregates key metrics into one place so monitoring dashboards can
/// poll a single endpoint instead of joining multiple views.
///
/// Exposed as `pgtrickle.health_summary()`.
#[pg_extern(schema = "pgtrickle", name = "health_summary")]
#[allow(clippy::type_complexity)]
fn health_summary() -> TableIterator<
    'static,
    (
        name!(total_stream_tables, i32),
        name!(active_count, i32),
        name!(error_count, i32),
        name!(suspended_count, i32),
        name!(stale_count, i32),
        name!(reinit_pending, i32),
        name!(max_staleness_seconds, Option<f64>),
        name!(scheduler_status, String),
        name!(cache_hit_rate, Option<f64>),
    ),
> {
    let row = Spi::connect(|client| {
        // ── Stream table status counts ──────────────────────────────────
        let counts = client
            .select(
                "SELECT \
                     count(*)::int AS total, \
                     count(*) FILTER (WHERE status = 'ACTIVE')::int AS active, \
                     count(*) FILTER (WHERE status = 'ERROR')::int AS errors, \
                     count(*) FILTER (WHERE status = 'SUSPENDED')::int AS suspended, \
                     count(*) FILTER (WHERE needs_reinit = true)::int AS reinit \
                 FROM pgtrickle.pgt_stream_tables",
                None,
                &[],
            )
            .ok();

        let (total, active, errors, suspended, reinit) = counts
            .map(|r| {
                let row = r.first();
                (
                    row.get::<i32>(1).unwrap_or(None).unwrap_or(0),
                    row.get::<i32>(2).unwrap_or(None).unwrap_or(0),
                    row.get::<i32>(3).unwrap_or(None).unwrap_or(0),
                    row.get::<i32>(4).unwrap_or(None).unwrap_or(0),
                    row.get::<i32>(5).unwrap_or(None).unwrap_or(0),
                )
            })
            .unwrap_or((0, 0, 0, 0, 0));

        // ── Stale count and max staleness ───────────────────────────────
        let stale_info = client
            .select(
                "SELECT \
                     count(*) FILTER (WHERE stale = true)::int, \
                     max(staleness_seconds)::float8 \
                 FROM pgtrickle.stream_tables_info",
                None,
                &[],
            )
            .ok();

        let (stale_count, max_staleness) = stale_info
            .map(|r| {
                let row = r.first();
                (
                    row.get::<i32>(1).unwrap_or(None).unwrap_or(0),
                    row.get::<f64>(2).unwrap_or(None),
                )
            })
            .unwrap_or((0, None));

        // ── Scheduler status ────────────────────────────────────────────
        // Check per-database scheduler first; fall back to launcher presence.
        let scheduler_running = client
            .select(
                "SELECT count(*)::int FROM pg_stat_activity \
                 WHERE backend_type = 'pg_trickle scheduler' \
                   AND datname = current_database()",
                None,
                &[],
            )
            .ok()
            .and_then(|r| r.first().get::<i32>(1).unwrap_or(None))
            .unwrap_or(0);

        let launcher_running = client
            .select(
                "SELECT count(*)::int FROM pg_stat_activity \
                 WHERE backend_type = 'pg_trickle launcher'",
                None,
                &[],
            )
            .ok()
            .and_then(|r| r.first().get::<i32>(1).unwrap_or(None))
            .unwrap_or(0);

        let scheduler_status = if scheduler_running > 0 {
            "ACTIVE".to_string()
        } else if launcher_running > 0 {
            "STARTING".to_string()
        } else if crate::shmem::is_shmem_available() {
            "STOPPED".to_string()
        } else {
            "NOT_LOADED".to_string()
        };

        // ── Cache hit rate from shared memory ───────────────────────────
        let cache_hit_rate = if crate::shmem::is_shmem_available() {
            let l1 = crate::shmem::TEMPLATE_CACHE_L1_HITS
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as f64;
            let l2 = crate::shmem::TEMPLATE_CACHE_L2_HITS
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as f64;
            let misses = crate::shmem::TEMPLATE_CACHE_MISSES
                .get()
                .load(std::sync::atomic::Ordering::Relaxed) as f64;
            let total_lookups = l1 + l2 + misses;
            if total_lookups > 0.0 {
                Some((l1 + l2) / total_lookups)
            } else {
                None
            }
        } else {
            None
        };

        (
            total,
            active,
            errors,
            suspended,
            stale_count,
            reinit,
            max_staleness,
            scheduler_status,
            cache_hit_rate,
        )
    });

    TableIterator::once(row)
}

/// Cross-stream-table refresh timeline, most recent first.
///
/// Returns up to `max_rows` refresh records across all stream tables in a
/// single chronological view. Useful for spotting refresh bursts, cascading
/// failures, or unexpected mode changes without having to query each stream
/// table individually.
///
/// Exposed as `pgtrickle.refresh_timeline(limit)`.
#[pg_extern(schema = "pgtrickle", name = "refresh_timeline")]
#[allow(clippy::type_complexity)]
fn refresh_timeline(
    max_rows: default!(i32, 50),
) -> TableIterator<
    'static,
    (
        name!(start_time, TimestampWithTimeZone),
        name!(stream_table, String),
        name!(action, String),
        name!(status, String),
        name!(rows_inserted, i64),
        name!(rows_updated, i64),
        name!(rows_deleted, i64),
        name!(duration_ms, Option<f64>),
        name!(error_message, Option<String>),
    ),
> {
    let epoch_zero = TimestampWithTimeZone::try_from(0i64).unwrap_or_else(|_| {
        pgrx::error!(
            "{}",
            crate::error::PgTrickleError::DiagnosticError(
                "refresh_timeline: failed to construct epoch timestamp".into()
            )
        )
    });

    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    h.start_time,
                    st.pgt_schema || '.' || st.pgt_name AS stream_table,
                    h.action,
                    h.status,
                    COALESCE(h.rows_inserted, 0)::bigint,
                    COALESCE(h.rows_updated, 0)::bigint,
                    COALESCE(h.rows_deleted, 0)::bigint,
                    CASE WHEN h.end_time IS NOT NULL
                         THEN EXTRACT(EPOCH FROM (h.end_time - h.start_time)) * 1000
                         ELSE NULL
                    END::float8,
                    h.error_message
                 FROM pgtrickle.pgt_refresh_history h
                 JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = h.pgt_id
                 ORDER BY h.start_time DESC
                 LIMIT $1",
                None,
                &[max_rows.into()],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "refresh_timeline: SPI select failed: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in result {
            let start = row
                .get::<TimestampWithTimeZone>(1)
                .unwrap_or(None)
                .unwrap_or(epoch_zero);
            let table = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
            let action = row.get::<String>(3).unwrap_or(None).unwrap_or_default();
            let status = row.get::<String>(4).unwrap_or(None).unwrap_or_default();
            let ins = row.get::<i64>(5).unwrap_or(None).unwrap_or(0);
            let updated = row.get::<i64>(6).unwrap_or(None).unwrap_or(0);
            let del = row.get::<i64>(7).unwrap_or(None).unwrap_or(0);
            let dur = row.get::<f64>(8).unwrap_or(None);
            let err = row.get::<String>(9).unwrap_or(None);
            out.push((start, table, action, status, ins, updated, del, dur, err));
        }
        out
    });

    TableIterator::new(rows)
}

/// Inventory of CDC triggers installed by pg_trickle for each tracked source.
///
/// For each source in `pgt_dependencies`, reports whether the expected pg_trickle
/// CDC triggers (`pg_trickle_cdc_<oid>` for DML and `pg_trickle_cdc_truncate_<oid>`
/// for TRUNCATE) are present and enabled in `pg_catalog`. A `present = false` row
/// indicates a missing trigger that will prevent change capture for that source.
///
/// Exposed as `pgtrickle.trigger_inventory()`.
#[pg_extern(schema = "pgtrickle", name = "trigger_inventory")]
#[allow(clippy::type_complexity)]
fn trigger_inventory() -> TableIterator<
    'static,
    (
        name!(source_table, String),
        name!(source_oid, i64),
        name!(trigger_name, String),
        name!(trigger_type, String),
        name!(present, bool),
        name!(enabled, bool),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    n.nspname::text || '.' || c.relname::text AS source_table,
                    d.source_relid::bigint                    AS source_oid,
                    'pg_trickle_cdc_' || d.source_relid::text AS dml_trigger,
                    'pg_trickle_cdc_truncate_' || d.source_relid::text AS trunc_trigger,
                    -- DML trigger present?
                    EXISTS (SELECT 1 FROM pg_trigger t
                            WHERE t.tgrelid = d.source_relid
                              AND t.tgname = 'pg_trickle_cdc_' || d.source_relid::text)
                        AS dml_present,
                    -- DML trigger enabled? (tgenabled: 'O'=origin, 'D'=disabled, etc.)
                    COALESCE((SELECT t.tgenabled != 'D'
                              FROM pg_trigger t
                              WHERE t.tgrelid = d.source_relid
                                AND t.tgname = 'pg_trickle_cdc_' || d.source_relid::text),
                             false) AS dml_enabled,
                    -- TRUNCATE trigger present?
                    EXISTS (SELECT 1 FROM pg_trigger t
                            WHERE t.tgrelid = d.source_relid
                              AND t.tgname = 'pg_trickle_cdc_truncate_' || d.source_relid::text)
                        AS trunc_present,
                    -- TRUNCATE trigger enabled?
                    COALESCE((SELECT t.tgenabled != 'D'
                              FROM pg_trigger t
                              WHERE t.tgrelid = d.source_relid
                                AND t.tgname = 'pg_trickle_cdc_truncate_' || d.source_relid::text),
                             false) AS trunc_enabled
                 FROM (SELECT DISTINCT source_relid FROM pgtrickle.pgt_dependencies
                       WHERE source_relid != 0) d
                 JOIN pg_class     c ON c.oid = d.source_relid
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 ORDER BY source_table",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "trigger_inventory: SPI select failed: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in result {
            let source_table = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let source_oid = row.get::<i64>(2).unwrap_or(None).unwrap_or(0);
            let dml_trigger = row.get::<String>(3).unwrap_or(None).unwrap_or_default();
            let trunc_trigger = row.get::<String>(4).unwrap_or(None).unwrap_or_default();
            let dml_present = row.get::<bool>(5).unwrap_or(None).unwrap_or(false);
            let dml_enabled = row.get::<bool>(6).unwrap_or(None).unwrap_or(false);
            let trunc_present = row.get::<bool>(7).unwrap_or(None).unwrap_or(false);
            let trunc_enabled = row.get::<bool>(8).unwrap_or(None).unwrap_or(false);

            // Emit one row per trigger type (DML + TRUNCATE)
            out.push((
                source_table.clone(),
                source_oid,
                dml_trigger,
                "DML".to_string(),
                dml_present,
                dml_enabled,
            ));
            out.push((
                source_table,
                source_oid,
                trunc_trigger,
                "TRUNCATE".to_string(),
                trunc_present,
                trunc_enabled,
            ));
        }
        out
    });

    TableIterator::new(rows)
}

// ── Parallel Refresh Observability (Phase 6) ──────────────────────────────

/// Worker pool utilization snapshot.
///
/// Returns a single row with:
/// - active workers (from shared memory),
/// - cluster-wide worker budget (from GUC),
/// - per-database dispatch cap (from GUC),
/// - current parallel refresh mode (from GUC),
/// - idle workers (pool_size - active),
/// - last scheduler tick time (unix seconds),
/// - invalidation ring overflow count,
/// - Citus worker failure total.
/// - adaptive worker target and resize signal.
///
/// Exposed as `pgtrickle.worker_pool_status()`.
#[pg_extern(schema = "pgtrickle", name = "worker_pool_status")]
#[allow(clippy::type_complexity)]
fn worker_pool_status() -> TableIterator<
    'static,
    (
        name!(active_workers, i32),
        name!(max_workers, i32),
        name!(per_db_cap, i32),
        name!(parallel_mode, String),
        name!(idle_workers, i32),
        name!(last_scheduler_tick_unix, i64),
        name!(ring_overflow_count, i64),
        name!(citus_failure_total, i64),
        name!(adaptive_enabled, bool),
        name!(adaptive_min, i32),
        name!(adaptive_max, i32),
        name!(adaptive_target, i32),
        name!(resize_signal, i16),
        name!(resize_consecutive, i16),
        name!(queue_depth, i32),
        name!(cpu_percent, Option<f64>),
    ),
> {
    let active = shmem::active_worker_count() as i32;
    let max_workers = config::pg_trickle_max_dynamic_refresh_workers();
    let per_db = config::pg_trickle_max_concurrent_refreshes();
    let mode = config::pg_trickle_parallel_refresh_mode().to_string();
    let idle = (max_workers - active).max(0);
    let last_tick = shmem::last_scheduler_wake();
    let ring_overflows = shmem::invalidation_ring_overflow_count() as i64;
    let citus_failures = shmem::citus_worker_failure_total() as i64;
    let adaptive_state = shmem::adaptive_worker_state();
    let adaptive_enabled = config::pg_trickle_adaptive_workers();
    let adaptive_min = config::pg_trickle_adaptive_workers_min();
    let adaptive_max = config::pg_trickle_adaptive_workers_max();
    let adaptive_target = shmem::adaptive_worker_target(
        adaptive_enabled,
        max_workers.max(1) as u32,
        adaptive_min.max(1) as u32,
        adaptive_max.max(1) as u32,
    )
    .unwrap_or(max_workers.max(1) as u32) as i32;

    TableIterator::new(vec![(
        active,
        max_workers,
        per_db,
        mode,
        idle,
        last_tick,
        ring_overflows,
        citus_failures,
        adaptive_enabled,
        adaptive_min,
        adaptive_max,
        adaptive_target,
        i16::from(adaptive_state.signal),
        i16::from(adaptive_state.consecutive),
        adaptive_state.queue_depth as i32,
        adaptive_state
            .cpu_valid
            .then_some(adaptive_state.cpu_percent as f64),
    )])
}

/// Active and recent scheduler jobs.
///
/// Returns one row per job in `pgt_scheduler_jobs` that is currently queued,
/// running, or recently completed (within the last `max_age_seconds`).
///
/// Exposed as `pgtrickle.parallel_job_status(max_age_seconds)`.
#[pg_extern(schema = "pgtrickle", name = "parallel_job_status")]
#[allow(clippy::type_complexity)]
fn parallel_job_status(
    max_age_seconds: default!(i32, 300),
) -> TableIterator<
    'static,
    (
        name!(job_id, i64),
        name!(unit_key, String),
        name!(unit_kind, String),
        name!(status, String),
        name!(member_count, i32),
        name!(attempt_no, i32),
        name!(scheduler_pid, i32),
        name!(worker_pid, Option<i32>),
        name!(enqueued_at, TimestampWithTimeZone),
        name!(started_at, Option<TimestampWithTimeZone>),
        name!(finished_at, Option<TimestampWithTimeZone>),
        name!(duration_ms, Option<f64>),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client.select(
            &format!(
                "SELECT job_id, unit_key, unit_kind, status, \
                        array_length(member_pgt_ids, 1), attempt_no, \
                        scheduler_pid, worker_pid, enqueued_at, started_at, \
                        finished_at, \
                        CASE WHEN started_at IS NOT NULL AND finished_at IS NOT NULL \
                             THEN EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000.0 \
                        END AS duration_ms \
                 FROM pgtrickle.pgt_scheduler_jobs \
                 WHERE status IN ('QUEUED', 'RUNNING') \
                    OR (finished_at > now() - interval '{} seconds') \
                 ORDER BY enqueued_at DESC",
                max_age_seconds.max(0)
            ),
            None,
            &[],
        );

        if let Ok(tup_table) = result {
            for row in tup_table {
                let job_id = row.get::<i64>(1).unwrap_or(None).unwrap_or(0);
                let unit_key = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
                let unit_kind = row.get::<String>(3).unwrap_or(None).unwrap_or_default();
                let status = row.get::<String>(4).unwrap_or(None).unwrap_or_default();
                let member_count = row.get::<i32>(5).unwrap_or(None).unwrap_or(0);
                let attempt_no = row.get::<i32>(6).unwrap_or(None).unwrap_or(1);
                let scheduler_pid = row.get::<i32>(7).unwrap_or(None).unwrap_or(0);
                let worker_pid = row.get::<i32>(8).unwrap_or(None);
                let enqueued_at = row
                    .get::<TimestampWithTimeZone>(9)
                    .unwrap_or(None)
                    .unwrap_or_else(|| {
                        TimestampWithTimeZone::try_from(0i64).unwrap_or_else(|_| {
                            pgrx::error!(
                                "{}",
                                crate::error::PgTrickleError::DiagnosticError(
                                    "parallel_job_status: failed to construct epoch timestamp"
                                        .into()
                                )
                            )
                        })
                    });
                let started_at = row.get::<TimestampWithTimeZone>(10).unwrap_or(None);
                let finished_at = row.get::<TimestampWithTimeZone>(11).unwrap_or(None);
                let duration_ms = row.get::<f64>(12).unwrap_or(None);

                rows.push((
                    job_id,
                    unit_key,
                    unit_kind,
                    status,
                    member_count,
                    attempt_no,
                    scheduler_pid,
                    worker_pid,
                    enqueued_at,
                    started_at,
                    finished_at,
                    duration_ms,
                ));
            }
        }
    });

    TableIterator::new(rows)
}

/// SCAL-1 (v0.31.0): Check all change buffers and return a list of
/// (source_relid_u32, pending_row_count) pairs for sources that exceed
/// the configured `buffer_alert_threshold`.
///
/// Called from the scheduler's periodic health-check tick. The caller
/// maintains a per-source consecutive-cycle counter and calls
/// `alert_change_buffer_backpressure` when the limit is reached.
/// PERF-1: Batch check of CDC buffer sizes in a single SPI call.
///
/// Previous implementation issued one `SELECT count(*)` per source OID.
/// This version builds a UNION ALL query that fans out across all CDC-enabled
/// source tables and returns one row per source with its pending-row count.
pub fn check_change_buffer_sizes() -> Vec<(u32, i64)> {
    let change_schema = crate::config::pg_trickle_change_buffer_schema();
    let threshold = crate::config::pg_trickle_buffer_alert_threshold();
    if threshold <= 0 {
        return Vec::new();
    }

    let sources: Vec<i64> = Spi::connect(|client| {
        match client.select(
            "SELECT DISTINCT source_relid::bigint FROM pgtrickle.pgt_change_tracking",
            None,
            &[],
        ) {
            Ok(result) => result
                .into_iter()
                .filter_map(|row| row.get::<i64>(1).ok().flatten())
                .collect(),
            Err(_) => Vec::new(),
        }
    });

    if sources.is_empty() {
        return Vec::new();
    }

    // PERF-1: Build a single UNION ALL query covering all source OIDs.
    // Each subquery returns (source_oid::bigint, count::bigint).
    let union_parts: Vec<String> = sources
        .iter()
        .map(|&relid| {
            let oid_u32 = relid as u32;
            let buf = crate::cdc::buffer_qualified_name_for_oid(
                &change_schema,
                pgrx::pg_sys::Oid::from(oid_u32),
            );
            // SAFETY: buf is constructed from a PostgreSQL OID via buffer_qualified_name_for_oid;
            // it is never user-supplied input.
            format!("SELECT {oid_u32}::bigint AS oid, count(*)::bigint AS cnt FROM {buf}") // nosemgrep: rust.spi.query.dynamic-format
        })
        .collect();
    let batched_sql = union_parts.join(" UNION ALL ");

    Spi::connect(|client| match client.select(&batched_sql, None, &[]) {
        Ok(result) => result
            .into_iter()
            .filter_map(|row| {
                let oid = row.get::<i64>(1).ok().flatten()? as u32;
                let cnt = row.get::<i64>(2).ok().flatten().unwrap_or(0);
                if cnt > threshold {
                    Some((oid, cnt))
                } else {
                    None
                }
            })
            .collect(),
        Err(_) => Vec::new(),
    })
}

// ── A44-9: wal_source_status() per-source WAL CDC diagnostics ────────────

/// A44-9 (v0.43.0): Return per-source WAL CDC status for all stream tables.
///
/// Exposes per-source information including:
/// - cdc_mode (trigger/wal/transitioning)
/// - blocked_reason if not WAL-eligible
/// - Slot name and lag bytes
/// - Publication state
/// - Last decoder error (if any)
///
/// Exposed as `pgtrickle.wal_source_status()`.
#[pg_extern(schema = "pgtrickle", name = "wal_source_status")]
#[allow(clippy::type_complexity)]
fn wal_source_status() -> TableIterator<
    'static,
    (
        name!(source_relid, i64),
        name!(source_name, String),
        name!(cdc_mode, String),
        name!(slot_name, Option<String>),
        name!(slot_lag_bytes, i64),
        name!(publication_name, Option<String>),
        name!(blocked_reason, Option<String>),
        name!(transition_started_at, Option<String>),
        name!(decoder_confirmed_lsn, Option<String>),
    ),
> {
    let rows = collect_wal_source_status_rows();
    TableIterator::new(rows)
}

/// Row type for `wal_source_status()`: (source_relid, source_name, cdc_mode,
/// slot_name, slot_lag_bytes, publication_name, blocked_reason,
/// transition_started_at, decoder_confirmed_lsn).
#[allow(clippy::type_complexity)]
fn collect_wal_source_status_rows() -> Vec<(
    i64,
    String,
    String,
    Option<String>,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
)> {
    let all_deps = StDependency::get_all().unwrap_or_default();

    // Deduplicate by source_relid.
    let mut seen_sources: std::collections::HashMap<u32, StDependency> =
        std::collections::HashMap::new();
    for dep in all_deps {
        seen_sources.entry(dep.source_relid.to_u32()).or_insert(dep);
    }

    let mut rows = Vec::new();

    for (oid_u32, dep) in seen_sources {
        let source_relid = oid_u32 as i64;

        // Get the source table's qualified name.
        let source_name = Spi::connect(|client| {
            let result = client
                .select(
                    "SELECT n.nspname::text || '.' || c.relname::text
                     FROM pg_catalog.pg_class c
                     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                     WHERE c.oid = $1::oid",
                    None,
                    &[(source_relid).into()],
                )
                .ok();
            result
                .and_then(|r| r.first().get::<String>(1).ok().flatten())
                .unwrap_or_else(|| format!("oid:{oid_u32}"))
        });

        // Get slot lag for WAL sources.
        let (slot_lag, slot_name_opt) = match dep.cdc_mode {
            CdcMode::Wal | CdcMode::Transitioning => {
                let slot = dep
                    .slot_name
                    .as_deref()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| {
                        wal_decoder::slot_name_for_source(pg_sys::Oid::from(oid_u32))
                    });
                let lag = wal_decoder::get_slot_lag_bytes(&slot).unwrap_or(0);
                (lag, Some(slot))
            }
            _ => (0i64, dep.slot_name.clone()),
        };

        // Build publication name from slot name convention.
        let publication_name = slot_name_opt.as_deref().map(|s| {
            // Publication names follow the pattern: pgtrickle_cdc_<stable_name>
            // where slot follows: pgtrickle_<stable_name>
            if let Some(suffix) = s.strip_prefix("pgtrickle_") {
                format!("pgtrickle_cdc_{suffix}")
            } else {
                s.to_string()
            }
        });

        // Determine blocked_reason for non-WAL sources.
        let blocked_reason: Option<String> = match dep.cdc_mode {
            CdcMode::Trigger => {
                Some("Using trigger-based CDC; WAL CDC not yet activated".to_string())
            }
            CdcMode::Transitioning => Some("WAL CDC transition in progress".to_string()),
            _ => None,
        };

        rows.push((
            source_relid,
            source_name,
            dep.cdc_mode.as_str().to_string(),
            slot_name_opt,
            slot_lag,
            publication_name,
            blocked_reason,
            dep.transition_started_at.clone(),
            dep.decoder_confirmed_lsn.clone(),
        ));
    }

    rows
}

#[cfg(test)]
mod window_state_tests {
    use super::*;

    fn entry(status: &str, bytes: u64) -> WindowStateHealthEntry {
        WindowStateHealthEntry {
            pgt_id: 7,
            stream_table: "public.orders_window".into(),
            node_ordinal: 1,
            spec_ordinal: 2,
            status: status.into(),
            total_estimated_bytes: bytes,
            first_for_stream: true,
        }
    }

    #[test]
    fn test_window_state_health_warns_at_eighty_percent() {
        let rows = build_window_state_health_rows(&[entry("READY", 800)], 1_000);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "window_state_budget_7");
        assert_eq!(rows[0].1, "WARN");
    }

    #[test]
    fn test_window_state_health_marks_mismatch_actionable() {
        let rows = build_window_state_health_rows(&[entry("STALE", 100)], 1_000);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, "ERROR");
        assert!(rows[0].2.contains("pgtrickle.repair_stream_table()"));
    }
}
