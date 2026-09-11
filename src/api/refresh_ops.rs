//! Refresh stream table API (v0.55.0 decomposition).
// Extracted from src/api/mod.rs in v0.55.0 module decomposition.
// All shared helpers, types, and utilities are in api/mod.rs (use super::*).

use super::*;
use crate::refresh::RefreshAction;
/// Run one refresh with the immutable source bound selected by a strict graph.
pub(crate) fn with_graph_safe_bound<T>(
    bound: &str,
    refresh: impl FnOnce() -> Result<T, PgTrickleError>,
) -> Result<T, PgTrickleError> {
    refresh::with_refresh_context(
        refresh::RefreshContext::graph(bound, refresh::current_full_policy()),
        refresh,
    )
}

fn refresh_safe_bound() -> Result<String, PgTrickleError> {
    refresh::current_safe_bound()
        .map(Ok)
        .unwrap_or_else(crate::cdc::get_current_wal_lsn)
}

fn graph_bound_is_set() -> bool {
    refresh::current_safe_bound().is_some()
}

#[derive(Debug, Clone)]
pub(crate) struct ManualRefreshResult {
    pub(crate) action: String,
    pub(crate) rows_inserted: i64,
    pub(crate) rows_updated: i64,
    pub(crate) rows_deleted: i64,
}

/// Whether the requested manual operation needs a whole-query FULL refresh.
/// TopK is scoped recomputation and is allowed by `full_policy = 'ERROR'`.
pub(crate) fn manual_requires_whole_query_full(st: &StreamTableMeta) -> bool {
    st.topk_limit.is_none()
        && (st.needs_reinit
            || !st.is_populated
            || st.frontier.as_ref().is_none_or(version::Frontier::is_empty)
            || matches!(st.refresh_mode, RefreshMode::Full | RefreshMode::Immediate))
}

/// Manually trigger a synchronous refresh of a stream table.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn refresh_stream_table(name: &str) {
    let result = refresh_stream_table_impl(name, security_context::EntryContext::SecurityDefiner);
    if let Err(e) = result {
        // RefreshSkipped is a transient, non-fatal condition: another refresh
        // is already in progress on this ST. Log it at DEBUG level and emit
        // a NOTICE (UX-8) so callers know the refresh was a no-op.
        if let PgTrickleError::RefreshSkipped(ref msg) = e {
            pgrx::notice!("refresh skipped: {}", msg);
            pgrx::debug1!("{}", e);
        } else {
            raise_error_with_context(e);
        }
    }
}

/// UX-5: Execute an arbitrary SQL statement (typically DML against a source
/// table) and then immediately refresh the named stream table, all within the
/// caller's transaction context.
///
/// This is a convenience wrapper for the common pattern:
/// ```sql
/// INSERT INTO orders VALUES (...);
/// SELECT pgtrickle.refresh_stream_table('order_totals');
/// ```
///
/// Calling `pgtrickle.write_and_refresh(sql, name)` guarantees the refresh
/// sees the writes from `sql` because both run in the same transaction.
#[pg_extern(schema = "pgtrickle")]
fn write_and_refresh(sql: &str, stream_table_name: &str) {
    // Execute the user-supplied SQL.
    if let Err(e) = Spi::run(sql) {
        pgrx::error!("write_and_refresh: user SQL failed: {}", e,);
    }
    // Refresh the stream table.
    let result = refresh_stream_table_impl(
        stream_table_name,
        security_context::EntryContext::SecurityInvoker,
    );
    if let Err(e) = result {
        if let PgTrickleError::RefreshSkipped(ref msg) = e {
            pgrx::notice!("refresh skipped: {}", msg);
        } else {
            raise_error_with_context(e);
        }
    }
}

fn refresh_stream_table_impl(
    name: &str,
    entry_context: security_context::EntryContext,
) -> Result<(), PgTrickleError> {
    // F16 (G8.2): Block manual refresh on read replicas — writes are not possible.
    let is_replica = Spi::get_one::<bool>("SELECT pg_is_in_recovery()")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);
    if is_replica {
        return Err(PgTrickleError::InvalidArgument(
            "Cannot refresh stream tables on a read replica. \
             The server is in recovery mode (pg_is_in_recovery() = true). \
             Run refresh on the primary server instead."
                .into(),
        ));
    }

    crate::api::recovery::assert_capture_ready()?;

    let (schema, table_name, st) = resolve_owned_stream_table(name, entry_context)?;

    if st.orchestration_mode.eq_ignore_ascii_case("EXTERNAL") {
        return Err(PgTrickleError::IntegrationError {
            code: "PGT_EXT_ORCHESTRATION_MODE",
            detail: format!(
                "stream table {}.{} is EXTERNAL; refreshes must be submitted by its coordinator",
                schema, table_name
            ),
        });
    }

    // Phase 10: Check if ST is suspended or in error — refuse manual refresh
    if st.status == StStatus::Suspended || st.status == StStatus::Error {
        return Err(PgTrickleError::InvalidArgument(format!(
            "stream table {}.{} is {} ; use pgtrickle.resume_stream_table('{}') first",
            schema,
            table_name,
            if st.status == StStatus::Suspended {
                "suspended"
            } else {
                "in error state"
            },
            if schema == "public" {
                table_name.clone()
            } else {
                format!("{}.{}", schema, table_name)
            },
        )));
    }

    // ── Fast no-op exit for DIFFERENTIAL mode ────────────────────────
    // Before acquiring the advisory lock, check if any source table has
    // pending changes. If not, skip the entire refresh pipeline (lock,
    // frontier computation, DVM, cleanup) — just update the timestamp.
    //
    // G-N3 optimization: source OIDs are fetched once and reused.
    let source_oids = get_source_oids_for_manual_refresh(st.pgt_id)?;

    // Phase 10: Advisory lock to prevent concurrent manual refresh.
    // Use the transaction-scoped variant so the lock is automatically
    // released when the transaction commits or rolls back — including on
    // error-triggered rollbacks where a subsequent session-level
    // pg_advisory_unlock() SPI call would silently no-op.
    let got_lock =
        Spi::get_one_with_args::<bool>("SELECT pg_try_advisory_xact_lock($1)", &[st.pgt_id.into()])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(false);

    if !got_lock {
        return Err(PgTrickleError::RefreshSkipped(format!(
            "{}.{} — another refresh is already in progress",
            schema, table_name,
        )));
    }

    // PB1 row-lock: acquire FOR UPDATE SKIP LOCKED on the catalog row so
    // the background scheduler's check_skip_needed() sees this manual
    // refresh as "in progress" and skips the ST for this tick.  Without
    // this, the advisory lock (above) and the scheduler's FOR UPDATE are
    // invisible to each other, allowing both to proceed simultaneously and
    // deadlock when the scheduler's TRUNCATE and the manual refresh's
    // catalog UPDATE race for conflicting locks.
    //
    // If the scheduler already holds the row lock (currently refreshing
    // this ST), we get zero rows back — report "already in progress".
    let row_available = Spi::get_one_with_args::<i64>(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1 FOR UPDATE SKIP LOCKED",
        &[st.pgt_id.into()],
    )
    .unwrap_or(None)
    .is_some();

    if !row_available {
        return Err(PgTrickleError::RefreshSkipped(format!(
            "{}.{} — another refresh is already in progress",
            schema, table_name,
        )));
    }

    // Reload the ST metadata now that we hold both the advisory lock and
    // the row lock.  Between the initial get_by_name() and acquiring these
    // locks, the background scheduler may have refreshed this ST and
    // advanced its frontier.  Using the stale frontier would cause the
    // differential refresh to re-process already-consumed change buffer
    // rows, producing incorrect aggregate deltas.
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

    // Transaction-level advisory lock is released automatically at
    // transaction end (commit or rollback); no explicit unlock needed.
    refresh::with_refresh_context(refresh::RefreshContext::manual(), || {
        execute_manual_refresh(&st, &schema, &table_name, &source_oids)
    })
    .map(|_| ())
}

/// Inner function for manual refresh, called while advisory lock is held.
///
/// Dispatches to FULL or DIFFERENTIAL depending on the ST's refresh mode.
/// `source_oids` are pre-fetched to avoid redundant SPI calls (G-N3).
///
/// ERG-D: Records the refresh in `pgt_refresh_history` with
/// `initiated_by = 'MANUAL'`.
pub(crate) fn execute_manual_refresh(
    st: &StreamTableMeta,
    schema: &str,
    table_name: &str,
    source_oids: &[pg_sys::Oid],
) -> Result<ManualRefreshResult, PgTrickleError> {
    // Discard a stale mode left by a prior failed Rust-level execution before
    // the common finalizer observes the next result.
    let _ = refresh::take_effective_mode();

    // EC-25/EC-26: Set the internal_refresh flag so DML guard triggers
    // allow the refresh executor to modify the storage table.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // Keep privileged prepare/finalize work independent of the caller's RLS
    // state. Definition-derived SQL runs later through with_stream_owner(),
    // which restores row_security = on for the stored owner.
    Spi::run("SET LOCAL row_security = off") // nosemgrep: sql.row-security.disabled — privileged refresh bookkeeping only; owner execution reenables RLS.
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    if st.refresh_mode != RefreshMode::Full && !st.needs_reinit {
        // Validation performs PostgreSQL parse analysis, so resolve the
        // stored user query under the same owner identity and search_path as
        // the refresh executor.
        refresh::with_stream_owner(st, || {
            crate::api::validate_incremental_mode_for_query(&st.defining_query, st.refresh_mode)
        })?;
    }

    if manual_requires_whole_query_full(st) {
        refresh::ensure_full_policy(st, "manual refresh")?;
    }

    // ERG-D: Determine the action label for history recording.
    let action = if st.topk_limit.is_some() {
        "FULL"
    } else if st.needs_reinit {
        "REINITIALIZE"
    } else {
        match st.refresh_mode {
            RefreshMode::Full | RefreshMode::Immediate => "FULL",
            RefreshMode::Differential => {
                if st.frontier.is_none() {
                    "FULL"
                } else {
                    "DIFFERENTIAL"
                }
            }
        }
    };

    // ERG-D: Record refresh start in pgt_refresh_history.
    let now = Spi::get_one::<TimestampWithTimeZone>("SELECT now()")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::InternalError("now() returned NULL".into()))?;
    let manual_tick_watermark = refresh_safe_bound()?;

    let refresh_id = RefreshRecord::insert(
        st.pgt_id,
        now,
        action,
        "RUNNING",
        0,
        0,
        None,
        Some(crate::refresh::current_initiated_by().unwrap_or("MANUAL")),
        None, // no freshness_deadline for manual refreshes
        0,
        None,
        false,
        Some(&manual_tick_watermark),
    )?;
    crate::api::output_delta::begin_capture(st.pgt_id)?;

    // TopK tables use the scoped-recomputation refresh path regardless of
    // refresh_mode (they always do ORDER BY … LIMIT N via MERGE).
    let result = if st.topk_limit.is_some() {
        refresh::execute_topk_refresh(st).map(|(ins, del)| {
            pgrx::info!(
                "Stream table {}.{} refreshed (TopK MERGE: +{} -{})",
                schema,
                table_name,
                ins,
                del,
            );
            (ins, del)
        })
    } else if st.needs_reinit {
        // When needs_reinit is set (e.g. by DDL hooks for ATTACH/DETACH
        // PARTITION, or EC-16 function body change detection), force a
        // FULL refresh regardless of the ST's refresh_mode.  This mirrors
        // the scheduler's RefreshAction::Reinitialize path.
        pgrx::info!(
            "Stream table {}.{}: needs_reinit is set, performing FULL reinitialization",
            schema,
            table_name,
        );

        // Materialize the stored user query first so PostgreSQL resolves its
        // current view/function dependencies under the captured search path.
        let refresh_st = if let Some(original_query) = &st.original_query {
            let mut refresh_st = st.clone();
            refresh_st.defining_query = original_query.clone();
            refresh_st
        } else {
            st.clone()
        };
        let full_result =
            execute_manual_full_refresh_target(&refresh_st, schema, table_name, source_oids)?;

        // Replace the inlined query and all query-derived state only after the
        // target was rebuilt from the current source definitions.
        let updated = reinit_rewrite_if_needed(st)?;
        let _window_plan = crate::window_state::prepare_for_protected_refresh(&updated)?;

        // Clear the reinit flag after successful refresh.
        let sql = format!(
            "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit = FALSE WHERE pgt_id = {}",
            updated.pgt_id,
        );
        Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        Ok(full_result)
    } else {
        match st.refresh_mode {
            RefreshMode::Full => execute_manual_full_refresh(st, schema, table_name, source_oids),
            RefreshMode::Differential => {
                execute_manual_differential_refresh(st, schema, table_name, source_oids)
            }
            RefreshMode::Immediate => {
                // For IMMEDIATE mode, manual refresh does a FULL refresh
                // (re-populate from the defining query), same as pg_ivm's
                // refresh_immv(name, true).
                execute_manual_full_refresh(st, schema, table_name, source_oids)
            }
        }
    };

    // ERG-D: Complete the refresh history record.
    let mut rows_updated_for_result = 0;
    let effective_mode = refresh::peek_effective_mode();
    let effective_action = match effective_mode {
        "FULL" => RefreshAction::Full,
        "NO_DATA" => RefreshAction::NoData,
        _ => match action {
            "FULL" => RefreshAction::Full,
            "DIFFERENTIAL" => RefreshAction::Differential,
            _ => RefreshAction::Reinitialize,
        },
    };
    match &result {
        Ok((rows_inserted, rows_deleted)) => {
            let rows_updated = refresh::take_last_rows_updated();
            rows_updated_for_result = rows_updated;
            let frontier = match StreamTableMeta::get_frontier(st.pgt_id)? {
                Some(frontier) => frontier,
                None => {
                    let positions =
                        cdc::get_slot_positions_at_bound(source_oids, &manual_tick_watermark)?;
                    version::compute_initial_frontier(&positions, &get_data_timestamp_str())
                }
            };
            let execution = refresh::RefreshExecution {
                requested_action: match action {
                    "FULL" => RefreshAction::Full,
                    "DIFFERENTIAL" => RefreshAction::Differential,
                    _ => RefreshAction::Reinitialize,
                },
                effective_action,
                frontier,
                rows_inserted: *rows_inserted,
                rows_updated,
                rows_deleted: *rows_deleted,
                data_changed: !refresh::effective_mode_is_no_data()
                    && (*rows_inserted > 0 || rows_updated > 0 || *rows_deleted > 0),
                was_full_fallback: effective_action == RefreshAction::Full && action != "FULL",
                full_reason: if effective_action == RefreshAction::Full && action == "DIFFERENTIAL"
                {
                    // Differential fallback paths persist their typed reason
                    // before entering the shared FULL executor.
                    None
                } else {
                    refresh::FullRefreshReason::for_action(effective_action, !st.is_populated)
                },
                downstream_capture_complete: true,
            };
            refresh::finalize_success(st, &execution, refresh_id, now, schema, table_name)?;
        }
        Err(e) => {
            RefreshRecord::complete_with_rows_updated(
                refresh_id,
                "FAILED",
                0,
                0,
                0,
                Some(&e.to_string()),
                0,
                None,
                false,
            )?;
        }
    }

    result.map(|(rows_inserted, rows_deleted)| ManualRefreshResult {
        action: if effective_mode.is_empty() {
            action.to_string()
        } else {
            effective_mode.to_string()
        },
        rows_inserted,
        rows_updated: rows_updated_for_result,
        rows_deleted,
    })
}

/// Execute a FULL manual refresh: truncate + repopulate from the defining query.
///
/// Re-run the query rewrite pipeline when `needs_reinit` is set and
/// the ST has an `original_query` (indicating the defining query was
/// rewritten, e.g. by view inlining). Updates the catalog's
/// `defining_query` so the full refresh uses the current view/function
/// definitions.
pub fn reinit_rewrite_if_needed(st: &StreamTableMeta) -> Result<StreamTableMeta, PgTrickleError> {
    let original = match &st.original_query {
        Some(oq) => oq.clone(),
        None => return Ok(st.clone()),
    };

    let rw = refresh::with_stream_owner(st, || run_query_rewrite_pipeline(&original))?;
    let new_defining = rw.query;
    let query_hash = crate::catalog::compute_defining_query_hash(&new_defining);
    let plan_is_current = st
        .window_strategy
        .as_ref()
        .is_none_or(|plan| plan.query_hash == query_hash);
    if new_defining == st.defining_query && st.defining_query_hash == query_hash && plan_is_current
    {
        return Ok(st.clone());
    }

    pgrx::info!(
        "Stream table {}.{}: re-inlined view/function definitions for reinit",
        st.pgt_schema,
        st.pgt_name,
    );

    // Old state belongs to the old semantic query. Replace query, hash, and
    // plan in the same transaction before the protected FULL rebuild. The
    // protected-refresh finalizer regenerates the plan after the target has
    // been materialized from this exact query.
    crate::window_state::drop_for_stream(st.pgt_id)?;
    crate::setop_state::drop_for_stream(st.pgt_id)?;
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET defining_query = $1, defining_query_hash = $2, \
             window_strategy = NULL, updated_at = now() \
         WHERE pgt_id = $3",
        &[
            new_defining.clone().into(),
            query_hash.into(),
            st.pgt_id.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let mut updated = st.clone();
    updated.defining_query = new_defining;
    updated.defining_query_hash = query_hash;
    updated.window_strategy = None;
    Ok(updated)
}

/// When user triggers are detected (and the GUC is not `"off"`), they are
/// suppressed during the TRUNCATE + INSERT via `DISABLE TRIGGER USER` /
/// `ENABLE TRIGGER USER`. A `NOTIFY pg_trickle_refresh` is emitted so
/// listeners know a FULL refresh occurred.
pub(crate) fn execute_manual_full_refresh(
    st: &StreamTableMeta,
    schema: &str,
    table_name: &str,
    source_oids: &[pg_sys::Oid],
) -> Result<(i64, i64), PgTrickleError> {
    let result = execute_manual_full_refresh_target(st, schema, table_name, source_oids)?;
    let _window_plan = crate::window_state::prepare_for_protected_refresh(st)?;
    // REL-101-1: (re)build durable, private INTERSECT/EXCEPT branch-multiplicity
    // state on the manual/create FULL refresh path, matching the scheduler path.
    if crate::dvm::query_needs_dual_count(&st.defining_query) {
        crate::setop_state::rebuild_for_full_refresh(st)?;
    }
    Ok(result)
}

fn execute_manual_full_refresh_target(
    st: &StreamTableMeta,
    schema: &str,
    table_name: &str,
    source_oids: &[pg_sys::Oid],
) -> Result<(i64, i64), PgTrickleError> {
    refresh::ensure_full_policy(st, "manual refresh")?;
    refresh::set_effective_mode("FULL");
    crate::cdc::validate_stream_table_row_identity(st)?;
    let dependencies = StDependency::get_for_st(st.pgt_id)?;
    if !st.refresh_mode.is_immediate() {
        crate::cdc::validate_required_change_buffers_for_full(st, &dependencies)?;
    }
    crate::cdc::lock_source_relations(source_oids)?;
    crate::cdc::lock_stream_table_sources(st.pgt_id, &dependencies)?;
    let safe_bound = refresh_safe_bound()?;

    let needs_diff_capture =
        !st.refresh_mode.is_immediate() && refresh::has_downstream_st_consumers(st.pgt_id);
    let prepared_user_cols = if needs_diff_capture {
        refresh::get_st_user_columns(st)
    } else {
        Vec::new()
    };
    if needs_diff_capture && !prepared_user_cols.is_empty() {
        let col_list = prepared_user_cols
            .iter()
            .map(|column| format!("\"{}\"", column.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");
        let quoted_table = format!(
            "{}.{}",
            quote_identifier(schema),
            quote_identifier(table_name),
        );
        let pre_select = format!("SELECT __pgt_row_id, {col_list} FROM {quoted_table}");
        refresh::prepare_owner_temp_table(st, &format!("__pgt_pre_{}", st.pgt_id), &pre_select)?;
        refresh::with_stream_owner(st, || {
            Spi::run(&format!(
                "INSERT INTO pg_temp.{} SELECT __pgt_row_id, {col_list} FROM {quoted_table}",
                quote_identifier(&format!("__pgt_pre_{}", st.pgt_id)),
            ))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        })?;
    }

    // EC-25/EC-26: Ensure the internal_refresh flag is set so DML guard
    // triggers allow the refresh executor to modify the storage table.
    // This is needed when called directly (e.g., from alter_stream_table)
    // without going through execute_manual_refresh.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let quoted_table = format!(
        "{}.{}",
        quote_identifier(schema),
        quote_identifier(table_name),
    );

    // Incremental INTERSECT/EXCEPT admission is intentionally guarded.  A
    // FULL refresh therefore materializes only defining-query columns and
    // cleans up dual-count state from legacy storage.
    let needs_dual_count = refresh::with_stream_owner(st, || {
        Ok(crate::dvm::query_needs_dual_count(&st.defining_query))
    })?;
    if needs_dual_count {
        refresh::with_stream_owner(st, || {
            crate::api::helpers::normalize_full_set_operation_storage(
                schema,
                table_name,
                st.pgt_relid,
                st.pgt_id,
            )
        })?;
    }

    // Check for user triggers to suppress during FULL refresh.
    let user_triggers_mode = crate::config::pg_trickle_user_triggers_mode();
    let has_triggers = match user_triggers_mode {
        crate::config::UserTriggersMode::Off => false,
        crate::config::UserTriggersMode::Auto => crate::cdc::has_user_triggers(st.pgt_relid)?,
    };

    // Suppress user triggers during TRUNCATE + INSERT to prevent
    // spurious trigger invocations with wrong semantics.
    if has_triggers {
        refresh::with_stream_owner(st, || {
            Spi::run(&format!("ALTER TABLE {quoted_table} DISABLE TRIGGER USER")) // nosemgrep: rust.spi.run.dynamic-format — ALTER TABLE DDL cannot be parameterized; quoted_table is a PostgreSQL-quoted identifier.
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        })?;
    }

    // ── Snapshot ST change buffer LSNs BEFORE TRUNCATE+INSERT ──────────
    //
    // Under READ COMMITTED, each SPI statement sees the latest committed
    // data at statement start time.  If the frontier's ST-source LSNs are
    // computed AFTER the INSERT, a concurrent upstream refresh might commit
    // new change buffer rows between the INSERT and the MAX(lsn) query.
    // The frontier would then advance past changes the INSERT never saw,
    // causing subsequent manual refreshes to believe the data is current
    // when it is actually stale.
    //
    // Capturing the LSNs before the INSERT ensures the frontier reflects
    // at most the data that was visible to the INSERT.
    let change_schema_for_snapshot =
        crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let mut st_source_lsn_snapshot = Vec::new();
    for dep in dependencies
        .iter()
        .filter(|dep| dep.source_type == "STREAM_TABLE" && !st.refresh_mode.is_immediate())
    {
        let upstream_pgt_id =
            StreamTableMeta::pgt_id_for_relid(dep.source_relid).ok_or_else(|| {
                PgTrickleError::CdcStateInvalid {
                    pgt_id: st.pgt_id,
                    source_name: format!("OID {}", dep.source_relid.to_u32()),
                    buffer: "stream-table dependency".to_string(),
                    reason: "upstream stream table metadata is missing".to_string(),
                }
            })?;
        if !crate::cdc::has_st_change_buffer(upstream_pgt_id, &change_schema_for_snapshot) {
            return Err(PgTrickleError::CdcStateInvalid {
                pgt_id: st.pgt_id,
                source_name: format!("pgt_id {upstream_pgt_id}"),
                buffer: format!("{change_schema_for_snapshot}.changes_pgt_{upstream_pgt_id}"),
                reason: "required stream-table change buffer is missing".to_string(),
            });
        }
        let lsn = Spi::get_one_with_args::<String>(
            &format!(
                "SELECT LEAST(COALESCE(MAX(lsn), '0/0'::pg_lsn), $1::pg_lsn)::text \
                 FROM \"{schema}\".changes_pgt_{id}",
                schema = change_schema_for_snapshot,
                id = upstream_pgt_id,
            ),
            &[safe_bound.as_str().into()],
        )
        .map_err(|e| PgTrickleError::CdcStateInvalid {
            pgt_id: st.pgt_id,
            source_name: format!("pgt_id {upstream_pgt_id}"),
            buffer: format!("{change_schema_for_snapshot}.changes_pgt_{upstream_pgt_id}"),
            reason: format!("could not read bounded upstream position: {e}"),
        })?
        .ok_or_else(|| PgTrickleError::CdcStateInvalid {
            pgt_id: st.pgt_id,
            source_name: format!("pgt_id {upstream_pgt_id}"),
            buffer: format!("{change_schema_for_snapshot}.changes_pgt_{upstream_pgt_id}"),
            reason: "bounded upstream position was NULL".to_string(),
        })?;
        st_source_lsn_snapshot.push((upstream_pgt_id, lsn));
    }

    let truncate_sql = format!("TRUNCATE {quoted_table}");
    refresh::with_stream_owner(st, || {
        Spi::run(&truncate_sql) // nosemgrep: rust.spi.run.dynamic-format — buffer name is catalog-derived.
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))
    })?;

    // For aggregate/distinct STs in DIFFERENTIAL mode, inject COUNT(*)
    // into the defining query so __pgt_count is populated for subsequent
    // differential refreshes.
    let effective_query = refresh::with_stream_owner(st, || {
        if st.refresh_mode == RefreshMode::Differential
            && crate::dvm::query_needs_pgt_count(&st.defining_query)
        {
            let mut eq = inject_pgt_count(&st.defining_query);
            // Also inject AVG auxiliary columns for algebraic AVG maintenance.
            let avg_aux = crate::dvm::query_avg_aux_columns(&st.defining_query);
            if !avg_aux.is_empty() {
                eq = inject_avg_aux(&eq, &avg_aux);
            }
            // Also inject sum-of-squares columns for STDDEV/VAR maintenance.
            let sum2_aux = crate::dvm::query_sum2_aux_columns(&st.defining_query);
            if !sum2_aux.is_empty() {
                let types = crate::dvm::query_statistical_aux_types(&st.defining_query);
                let typed = crate::api::typed_statistical_aux_columns(&sum2_aux, &types);
                eq = inject_sum2_aux_typed(&eq, &typed);
            }
            // Also inject cross-product columns for CORR/COVAR/REGR maintenance (P3-2).
            let covar_aux = crate::dvm::query_covar_aux_columns(&st.defining_query);
            if !covar_aux.is_empty() {
                let types = crate::dvm::query_statistical_aux_types(&st.defining_query);
                let typed = crate::api::typed_statistical_aux_columns(&covar_aux, &types);
                eq = inject_covar_aux_typed(&eq, &typed);
            }
            // Also inject nonnull-count columns for SUM NULL-transition correction (P2-2).
            let nonnull_aux = crate::dvm::query_nonnull_aux_columns(&st.defining_query);
            if !nonnull_aux.is_empty() {
                eq = inject_nonnull_aux(&eq, &nonnull_aux);
            }
            Ok(eq)
        } else {
            Ok(st.defining_query.clone())
        }
    })?;

    // Compute row_id using the same hash formula as the delta query so
    // the MERGE ON clause matches during subsequent differential refreshes.
    // Guarded INTERSECT/EXCEPT uses the direct defining-query shape. For
    // UNION (dedup), convert to UNION ALL and count.
    // For UNION ALL, decompose into per-branch subqueries with
    // child-prefixed row IDs matching diff_union_all's formula.
    let insert_body = refresh::with_stream_owner(st, || {
        if needs_dual_count {
            Ok(crate::dvm::direct_full_refresh_insert_body(
                &st.defining_query,
                &effective_query,
            ))
        } else if crate::dvm::query_needs_union_dedup_count(&st.defining_query) {
            let col_names = crate::dvm::get_defining_query_columns(&st.defining_query)?;
            if let Some(union_sql) =
                crate::dvm::try_union_dedup_refresh_sql(&st.defining_query, &col_names)
            {
                Ok(union_sql)
            } else {
                let row_id_expr = crate::dvm::row_id_expr_for_query(&st.defining_query);
                Ok(format!(
                    "SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({effective_query}) sub",
                ))
            }
        } else if let Some(ua_sql) = crate::dvm::try_union_all_refresh_sql(&st.defining_query) {
            Ok(ua_sql)
        } else {
            let row_id_expr = crate::dvm::row_id_expr_for_query(&st.defining_query);
            Ok(format!(
                "SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({effective_query}) sub",
            ))
        }
    })?;

    let insert_sql = format!("INSERT INTO {quoted_table} {insert_body}");
    let rows_inserted = refresh::with_stream_owner(st, || {
        Spi::connect_mut(|client| {
            let result = client
                .update(&insert_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<usize, PgTrickleError>(result.len())
        })
    })?;

    // Re-enable user triggers and emit NOTIFY so listeners know a FULL
    // refresh occurred.
    if has_triggers {
        refresh::with_stream_owner(st, || {
            Spi::run(&format!("ALTER TABLE {quoted_table} ENABLE TRIGGER USER")) // nosemgrep: rust.spi.run.dynamic-format — ALTER TABLE DDL cannot be parameterized; quoted_table is a PostgreSQL-quoted identifier.
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        })?;

        // PB2: Skip NOTIFY when pooler compatibility mode is enabled.
        if !st.pooler_compatibility_mode {
            let escaped_name = table_name.replace('\'', "''");
            let escaped_schema = schema.replace('\'', "''");
            // NOTIFY does not support parameterized payloads; single quotes are escaped above.
            let notify_sql = format!(
                "NOTIFY pg_trickle_refresh, '{{\"stream_table\": \"{escaped_name}\", \
                 \"schema\": \"{escaped_schema}\", \"mode\": \"FULL\"}}'"
            );
            Spi::run(&notify_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }

        pgrx::info!(
            "pg_trickle: FULL refresh of {}.{} with user triggers suppressed.",
            schema,
            table_name,
        );
    }

    if needs_diff_capture && !prepared_user_cols.is_empty() {
        refresh::capture_full_refresh_diff_to_st_buffer(st, &prepared_user_cols).map_err(|e| {
            PgTrickleError::RefreshFinalizationFailed {
                pgt_id: st.pgt_id,
                stage: "manual full-refresh downstream CDC capture".to_string(),
                reason: e.to_string(),
            }
        })?;
    }

    // Compute and store frontier so differential can start from here.
    // S3 optimization: single SPI call combines frontier storage,
    // timestamp update, and marking the ST as populated.
    let slot_positions = cdc::get_slot_positions_at_bound(source_oids, &safe_bound)?;
    let data_ts = get_data_timestamp_str();
    let mut frontier = version::compute_initial_frontier(&slot_positions, &data_ts);

    // Include ST (stream table) sources in the frontier so that the
    // scheduler's `prev_frontier.is_empty()` check doesn't trigger a
    // spurious FULL fallback on the first differential refresh.
    //
    // Uses the pre-INSERT LSN snapshot captured above to avoid the TOCTOU
    // race where concurrent upstream refreshes commit new change buffer
    // rows between the INSERT and this point, advancing the frontier past
    // data the INSERT never saw.
    for (upstream_pgt_id, lsn) in &st_source_lsn_snapshot {
        frontier.set_st_source(*upstream_pgt_id, lsn.clone(), data_ts.clone());
    }

    // Detect no-op FULL refresh: check if any upstream ST source has a
    // data_timestamp newer than ours.  If not, the TRUNCATE+INSERT above
    // reproduced identical data, so we must NOT bump data_timestamp —
    // otherwise downstream CALCULATED stream tables see a false "upstream
    // changed" signal and their own data_timestamp drifts on every no-op
    // cycle.
    //
    // This uses catalog data_timestamps (stable, not affected by change
    // buffer cleanup) rather than frontier LSN comparisons (which become
    // unreliable when buffer rows are consumed and MAX(lsn) falls back to
    // pg_current_wal_lsn()).
    let has_upstream_st_change = Spi::get_one::<bool>(&format!(
        "SELECT EXISTS( \
           SELECT 1 \
           FROM pgtrickle.pgt_dependencies dep \
           JOIN pgtrickle.pgt_stream_tables upstream \
                ON upstream.pgt_relid = dep.source_relid \
           WHERE dep.pgt_id = {pgt_id} \
             AND dep.source_type = 'STREAM_TABLE' \
             AND upstream.data_timestamp > COALESCE( \
                   (SELECT data_timestamp \
                    FROM pgtrickle.pgt_stream_tables \
                    WHERE pgt_id = {pgt_id}), \
                   '-infinity'::timestamptz) \
         )",
        pgt_id = st.pgt_id,
    ))
    .unwrap_or(Some(false))
    .unwrap_or(false);

    // Also check WAL-based sources: if any slot position advanced
    // beyond the previous frontier, data changed.
    let prev_frontier = st.frontier.clone().unwrap_or_default();
    let has_wal_change = slot_positions
        .iter()
        .any(|(oid, lsn)| prev_frontier.get_lsn(*oid) != *lsn);

    if has_upstream_st_change || has_wal_change || prev_frontier.is_empty() {
        StreamTableMeta::store_frontier_and_complete_refresh(st.pgt_id, &frontier, 0)?;
        pgrx::info!("Stream table {}.{} refreshed (FULL)", schema, table_name);
    } else {
        // No upstream changes — store frontier but preserve data_timestamp.
        StreamTableMeta::store_frontier(st.pgt_id, &frontier)?;
        StreamTableMeta::update_after_no_data_refresh(st.pgt_id)?;
        crate::refresh::set_effective_mode("NO_DATA");
        pgrx::info!(
            "Stream table {}.{} refreshed (FULL, no-op — data_timestamp preserved)",
            schema,
            table_name
        );
    }

    if st.row_identity_version != Some(crate::hash::CURRENT_ROW_IDENTITY_VERSION)
        || st.row_probe_version != Some(crate::dvm::row_id_v2::PROBE_VERSION_V1 as i16)
    {
        crate::catalog::StreamTableMeta::mark_row_identity_reinitialized(st.pgt_id)?;
    }

    Ok((rows_inserted as i64, 0i64))
}

/// Execute a DIFFERENTIAL manual refresh using the DVM engine.
///
/// If no previous frontier exists (first refresh), falls back to FULL.
fn execute_manual_differential_refresh(
    st: &StreamTableMeta,
    schema: &str,
    table_name: &str,
    source_oids: &[pg_sys::Oid],
) -> Result<(i64, i64), PgTrickleError> {
    // If the ST has never been refreshed (frontier is None), fall back to
    // a FULL refresh to establish the baseline frontier.
    if st.frontier.is_none() {
        pgrx::info!(
            "Stream table {}.{}: no previous frontier, performing FULL refresh first",
            schema,
            table_name
        );
        return execute_manual_full_refresh(st, schema, table_name, source_oids);
    }

    let prev_frontier = st.frontier.clone().unwrap_or_default();

    // If the frontier exists but tracks zero sources, the ST was populated
    // via FULL but never differentially refreshed. Fall back to FULL to
    // establish proper source tracking.
    if prev_frontier.is_empty() {
        return execute_manual_full_refresh(st, schema, table_name, source_oids);
    }

    // IMMEDIATE upstreams maintain their output through IVM triggers, not the
    // deferred stream-table change buffer consumed by this path. A FULL
    // recompute is therefore the safe boundary for this mixed-mode edge.
    if upstream_immediate_source_requires_full(st)? {
        refresh::ensure_full_policy(st, "IMMEDIATE upstream source")?;
        pgrx::info!(
            "Stream table {}.{}: IMMEDIATE upstream requires FULL refresh",
            schema,
            table_name,
        );
        return execute_manual_full_refresh(st, schema, table_name, source_oids);
    }

    if let Some(reason) = upstream_st_source_requires_full(st)? {
        refresh::ensure_full_policy(st, reason)?;
        pgrx::info!(
            "Stream table {}.{}: {} — using FULL refresh",
            schema,
            table_name,
            reason,
        );
        return execute_manual_full_refresh(st, schema, table_name, source_oids);
    }

    refresh::poll_foreign_table_sources_for_st(st)?;
    crate::cdc::lock_source_relations(source_oids)?;

    // Get current WAL positions for non-ST sources (reuses source_oids — G-N3)
    // The source lock is the visibility proof for this manual refresh: no
    // source transaction can add a change-buffer row after this bound.
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let mut safe_bound = refresh_safe_bound()?;
    if !graph_bound_is_set() {
        for source_oid in source_oids {
            let buffer_name = cdc::buffer_base_name_for_oid(*source_oid);
            // nosemgrep: rust.spi.get_one_with_args.dynamic-format — change_schema is a quoted config identifier and buffer_name is OID-derived.
            safe_bound = Spi::get_one_with_args::<String>(
                &format!(
                    "SELECT GREATEST($1::pg_lsn, COALESCE(MAX(lsn), '0/0'::pg_lsn))::text \
                     FROM \"{change_schema}\".{buffer_name}"
                ),
                &[safe_bound.as_str().into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(safe_bound);
        }
    }
    let slot_positions = cdc::get_slot_positions_at_bound(source_oids, &safe_bound)?;
    let data_ts = get_data_timestamp_str();
    let mut new_frontier = version::compute_new_frontier(&slot_positions, &data_ts);
    for (upstream_pgt_id, lsn) in upstream_st_source_positions(st, &safe_bound)? {
        new_frontier.set_st_source(upstream_pgt_id, lsn, data_ts.clone());
    }
    // A bounded buffer position can trail the stored frontier. Never replay
    // deltas by letting a differential frontier move backward.
    new_frontier.merge_from(&prev_frontier);

    // Execute the differential refresh via the DVM engine.
    // DI-7: When QueryTooComplex is returned (e.g. join count exceeds
    // max_differential_joins, or CORRELATED_SUBQUERY_DELTA_QUADRATIC / DVM-2
    // cannot safely rewrite the query), fall back to FULL refresh immediately —
    // mirrors the scheduler path in scheduler_loop.rs.
    let (rows_inserted, rows_deleted) =
        match refresh::execute_differential_refresh(st, &prev_frontier, &new_frontier) {
            Ok(counts) => counts,
            Err(crate::error::PgTrickleError::QueryTooComplex(ref msg)) => {
                pgrx::log!(
                    "[pg_trickle] DI-7 manual fallback for {}.{}: {}; using FULL refresh",
                    schema,
                    table_name,
                    msg
                );
                let (ins, del) = refresh::execute_full_refresh(st)?;
                StreamTableMeta::store_frontier(st.pgt_id, &new_frontier)?;
                refresh::post_full_refresh_cleanup(st);
                pgrx::info!(
                    "Stream table {}.{} refreshed (FULL fallback: +{} -{})",
                    schema,
                    table_name,
                    ins,
                    del,
                );
                return Ok((ins, del));
            }
            Err(e) => return Err(e),
        };

    // Store the new frontier and mark refresh complete in a single SPI call (S3).
    // Matches scheduler behavior: only update data_timestamp when rows were
    // actually written — a no-op differential must not advance data_timestamp
    // or downstream CALCULATED stream tables would see a false "upstream changed"
    // signal and trigger unnecessary refreshes.
    if rows_inserted > 0 || rows_deleted > 0 {
        StreamTableMeta::store_frontier_and_complete_refresh(
            st.pgt_id,
            &new_frontier,
            rows_inserted,
        )?;
    } else {
        // No rows changed — store frontier to advance past processed WAL range,
        // but preserve data_timestamp to avoid spurious downstream wakeups.
        StreamTableMeta::store_frontier(st.pgt_id, &new_frontier)?;
        StreamTableMeta::update_after_no_data_refresh(st.pgt_id)?;
    }

    pgrx::info!(
        "Stream table {}.{} refreshed (DIFFERENTIAL: +{} -{})",
        schema,
        table_name,
        rows_inserted,
        rows_deleted,
    );
    Ok((rows_inserted, rows_deleted))
}

fn upstream_st_source_positions(
    st: &StreamTableMeta,
    safe_bound: &str,
) -> Result<Vec<(i64, String)>, PgTrickleError> {
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    StDependency::get_for_st(st.pgt_id)?
        .into_iter()
        .filter(|dependency| dependency.source_type == "STREAM_TABLE")
        .map(|dependency| {
            let upstream_pgt_id = StreamTableMeta::pgt_id_for_relid(dependency.source_relid)
                .ok_or_else(|| PgTrickleError::CdcStateInvalid {
                    pgt_id: st.pgt_id,
                    source_name: format!("OID {}", dependency.source_relid.to_u32()),
                    buffer: "stream-table dependency".to_string(),
                    reason: "upstream stream table metadata is missing".to_string(),
                })?;
            if !crate::cdc::has_st_change_buffer(upstream_pgt_id, &change_schema) {
                return Err(PgTrickleError::CdcStateInvalid {
                    pgt_id: st.pgt_id,
                    source_name: format!("pgt_id {upstream_pgt_id}"),
                    buffer: format!("{change_schema}.changes_pgt_{upstream_pgt_id}"),
                    reason: "required stream-table change buffer is missing".to_string(),
                });
            }
            let lsn = Spi::get_one_with_args::<String>(
                &format!(
                    "SELECT LEAST(COALESCE(MAX(lsn), '0/0'::pg_lsn), $1::pg_lsn)::text \
                     FROM \"{change_schema}\".changes_pgt_{upstream_pgt_id}"
                ),
                &[safe_bound.into()],
            )
            .map_err(|e| PgTrickleError::CdcStateInvalid {
                pgt_id: st.pgt_id,
                source_name: format!("pgt_id {upstream_pgt_id}"),
                buffer: format!("{change_schema}.changes_pgt_{upstream_pgt_id}"),
                reason: format!("could not read bounded upstream position: {e}"),
            })?
            .ok_or_else(|| PgTrickleError::CdcStateInvalid {
                pgt_id: st.pgt_id,
                source_name: format!("pgt_id {upstream_pgt_id}"),
                buffer: format!("{change_schema}.changes_pgt_{upstream_pgt_id}"),
                reason: "bounded upstream position was NULL".to_string(),
            })?;
            Ok((upstream_pgt_id, lsn))
        })
        .collect()
}

fn upstream_immediate_source_requires_full(st: &StreamTableMeta) -> Result<bool, PgTrickleError> {
    for dependency in StDependency::get_for_st(st.pgt_id)? {
        if dependency.source_type == "STREAM_TABLE"
            && StreamTableMeta::get_by_relid(dependency.source_relid)?
                .refresh_mode
                .is_immediate()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn upstream_st_source_requires_full(
    st: &StreamTableMeta,
) -> Result<Option<&'static str>, PgTrickleError> {
    let mut st_source_count = 0;
    for dependency in StDependency::get_for_st(st.pgt_id)? {
        if dependency.source_type != "STREAM_TABLE" {
            continue;
        }
        st_source_count += 1;
        let upstream = StreamTableMeta::get_by_relid(dependency.source_relid)?;
        if upstream.topk_limit.is_some()
            || upstream
                .window_strategy
                .as_ref()
                .is_some_and(|plan| !plan.nodes.is_empty())
        {
            return Ok(Some("windowed or scoped upstream requires FULL refresh"));
        }
    }
    if st_source_count > 2 {
        return Ok(Some("three-way stream-table join requires FULL refresh"));
    }
    Ok(None)
}

/// Get source table OIDs for a stream table (used by manual refresh path).
pub(crate) fn get_source_oids_for_manual_refresh(
    pgt_id: i64,
) -> Result<Vec<pg_sys::Oid>, PgTrickleError> {
    let deps = StDependency::get_for_st(pgt_id)?;
    Ok(deps
        .into_iter()
        .filter(|dep| {
            matches!(
                dep.source_type.as_str(),
                "TABLE" | "FOREIGN_TABLE" | "MATVIEW"
            )
        })
        .map(|dep| dep.source_relid)
        .collect())
}
