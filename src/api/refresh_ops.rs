//! Refresh stream table API (v0.55.0 decomposition).
// Extracted from src/api/mod.rs in v0.55.0 module decomposition.
// All shared helpers, types, and utilities are in api/mod.rs (use super::*).

use super::*;

/// Manually trigger a synchronous refresh of a stream table.
#[pg_extern(schema = "pgtrickle")]
fn refresh_stream_table(name: &str) {
    let result = refresh_stream_table_impl(name);
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
    let result = refresh_stream_table_impl(stream_table_name);
    if let Err(e) = result {
        if let PgTrickleError::RefreshSkipped(ref msg) = e {
            pgrx::notice!("refresh skipped: {}", msg);
        } else {
            raise_error_with_context(e);
        }
    }
}

fn refresh_stream_table_impl(name: &str) -> Result<(), PgTrickleError> {
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

    let (schema, table_name) = parse_qualified_name(name)?;
    let st = StreamTableMeta::get_by_name(&schema, &table_name)?;

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
    execute_manual_refresh(&st, &schema, &table_name, &source_oids)
}

/// Inner function for manual refresh, called while advisory lock is held.
///
/// Dispatches to FULL or DIFFERENTIAL depending on the ST's refresh mode.
/// `source_oids` are pre-fetched to avoid redundant SPI calls (G-N3).
///
/// ERG-D: Records the refresh in `pgt_refresh_history` with
/// `initiated_by = 'MANUAL'`.
fn execute_manual_refresh(
    st: &StreamTableMeta,
    schema: &str,
    table_name: &str,
    source_oids: &[pg_sys::Oid],
) -> Result<(), PgTrickleError> {
    // EC-25/EC-26: Set the internal_refresh flag so DML guard triggers
    // allow the refresh executor to modify the storage table.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // R3: Bypass RLS for the refresh defining query so the stream table
    // always materializes the full result set regardless of who called
    // refresh_stream_table(). This mirrors REFRESH MATERIALIZED VIEW
    // semantics and prevents the "who refreshed it?" correctness hazard.
    Spi::run("SET LOCAL row_security = off") // nosemgrep: sql.row-security.disabled — intentional R3 bypass, mirrors REFRESH MATERIALIZED VIEW semantics.
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // ERG-D: Determine the action label for history recording.
    let action = if st.topk_limit.is_some() {
        "FULL"
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

    let refresh_id = RefreshRecord::insert(
        st.pgt_id,
        now,
        action,
        "RUNNING",
        0,
        0,
        None,
        Some("MANUAL"),
        None, // no freshness_deadline for manual refreshes
        0,
        None,
        false,
        None, // no tick_watermark_lsn for manual refreshes
    )?;

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

        // If the ST has an original_query (pre-rewrite, e.g. referencing
        // views by name), use it for the FULL refresh so the current view
        // definitions are resolved at execution time.  Then re-run the
        // rewrite pipeline to update the stored defining_query for future
        // differential refreshes.
        let refresh_st = if let Some(oq) = &st.original_query {
            let mut tmp = st.clone();
            tmp.defining_query = oq.clone();
            tmp
        } else {
            st.clone()
        };

        let full_result =
            execute_manual_full_refresh(&refresh_st, schema, table_name, source_oids)?;

        // After the FULL refresh has committed the correct data, re-run
        // the rewrite pipeline to store the updated inlined query.
        let updated = reinit_rewrite_if_needed(st)?;

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
    match &result {
        Ok((rows_inserted, rows_deleted)) => {
            let _ = RefreshRecord::complete(
                refresh_id,
                "COMPLETED",
                *rows_inserted,
                *rows_deleted,
                None,
                0,
                None,
                false,
            );
            // G12-ERM-1: Persist the effective refresh mode for manual
            // refreshes, mirroring the scheduler path.  Without this,
            // effective_refresh_mode stays NULL after every manual refresh,
            // which breaks diagnostics and test assertions.
            let eff_mode = crate::refresh::take_effective_mode();
            if !eff_mode.is_empty() {
                let _ = StreamTableMeta::update_effective_refresh_mode(st.pgt_id, eff_mode);
            }
            // Gap-1 fix: write outbox notification for ALL manual refresh modes.
            // Centralized here so FULL, Immediate, needs_reinit, TopK, and
            // Differential (including its fallback-to-full paths) all trigger
            // the outbox write with the actual row counts.
            if (*rows_inserted > 0 || *rows_deleted > 0)
                && crate::api::outbox::is_outbox_enabled(st.pgt_id)
                && let Err(e) = crate::api::outbox::write_outbox_row(
                    st.pgt_id,
                    None, // manual refresh has no UUID refresh_id
                    *rows_inserted,
                    *rows_deleted,
                    0_i32,
                    schema,
                    table_name,
                )
            {
                pgrx::warning!(
                    "[pg_trickle] OUTBOX: failed to write outbox row for {}.{}: {}",
                    schema,
                    table_name,
                    e
                );
            }
            // VP-1/VP-2 (v0.47.0): Execute post-refresh action for manual refreshes too,
            // mirroring the scheduler path so vector_status() drift tracking works correctly.
            let rows_changed = rows_inserted + rows_deleted;
            if rows_changed > 0 {
                crate::scheduler::execute_post_refresh_action(st, rows_changed);
            }
        }
        Err(e) => {
            let _ = RefreshRecord::complete(
                refresh_id,
                "FAILED",
                0,
                0,
                Some(&e.to_string()),
                0,
                None,
                false,
            );
        }
    }

    result.map(|_| ())
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

    let rw = run_query_rewrite_pipeline(&original)?;
    let new_defining = rw.query;
    if new_defining == st.defining_query {
        return Ok(st.clone());
    }

    pgrx::info!(
        "Stream table {}.{}: re-inlined view/function definitions for reinit",
        st.pgt_schema,
        st.pgt_name,
    );

    // Update the catalog with the new defining query.
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET defining_query = $1, updated_at = now() \
         WHERE pgt_id = $2",
        &[new_defining.clone().into(), st.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let mut updated = st.clone();
    updated.defining_query = new_defining;
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

    // Check for user triggers to suppress during FULL refresh.
    let user_triggers_mode = crate::config::pg_trickle_user_triggers_mode();
    let has_triggers = match user_triggers_mode {
        crate::config::UserTriggersMode::Off => false,
        crate::config::UserTriggersMode::Auto => crate::cdc::has_user_triggers(st.pgt_relid)?,
    };

    // Suppress user triggers during TRUNCATE + INSERT to prevent
    // spurious trigger invocations with wrong semantics.
    if has_triggers {
        Spi::run(&format!("ALTER TABLE {quoted_table} DISABLE TRIGGER USER")) // nosemgrep: rust.spi.run.dynamic-format — ALTER TABLE DDL cannot be parameterized; quoted_table is a PostgreSQL-quoted identifier.
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
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
    let st_source_lsn_snapshot: Vec<(i64, String)> =
        crate::catalog::StDependency::get_for_st(st.pgt_id)
            .unwrap_or_default()
            .into_iter()
            .filter(|dep| dep.source_type == "STREAM_TABLE")
            .filter_map(|dep| {
                let upstream_pgt_id = StreamTableMeta::pgt_id_for_relid(dep.source_relid)?;
                if !crate::cdc::has_st_change_buffer(upstream_pgt_id, &change_schema_for_snapshot) {
                    return None;
                }
                let lsn = Spi::get_one::<String>(&format!(
                    "SELECT COALESCE(MAX(lsn)::text, pg_current_wal_lsn()::text) \
             FROM \"{schema}\".changes_pgt_{id}",
                    schema = change_schema_for_snapshot,
                    id = upstream_pgt_id,
                ))
                .unwrap_or(None)
                .unwrap_or_else(|| "0/0".to_string());
                Some((upstream_pgt_id, lsn))
            })
            .collect();

    let truncate_sql = format!("TRUNCATE {quoted_table}");
    Spi::run(&truncate_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // For aggregate/distinct STs in DIFFERENTIAL mode, inject COUNT(*)
    // into the defining query so __pgt_count is populated for subsequent
    // differential refreshes.
    let effective_query = if st.refresh_mode == RefreshMode::Differential
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
            eq = inject_sum2_aux(&eq, &sum2_aux);
        }
        // Also inject cross-product columns for CORR/COVAR/REGR maintenance (P3-2).
        let covar_aux = crate::dvm::query_covar_aux_columns(&st.defining_query);
        if !covar_aux.is_empty() {
            eq = inject_covar_aux(&eq, &covar_aux);
        }
        // Also inject nonnull-count columns for SUM NULL-transition correction (P2-2).
        let nonnull_aux = crate::dvm::query_nonnull_aux_columns(&st.defining_query);
        if !nonnull_aux.is_empty() {
            eq = inject_nonnull_aux(&eq, &nonnull_aux);
        }
        eq
    } else {
        st.defining_query.clone()
    };

    // Compute row_id using the same hash formula as the delta query so
    // the MERGE ON clause matches during subsequent differential refreshes.
    // For INTERSECT/EXCEPT, compute per-branch multiplicities for dual-count
    // storage. For UNION (dedup), convert to UNION ALL and count.
    // For UNION ALL, decompose into per-branch subqueries with
    // child-prefixed row IDs matching diff_union_all's formula.
    let insert_body = if crate::dvm::query_needs_dual_count(&st.defining_query) {
        let col_names = crate::dvm::get_defining_query_columns(&st.defining_query)?;
        if let Some(set_op_sql) = crate::dvm::try_set_op_refresh_sql(&st.defining_query, &col_names)
        {
            set_op_sql
        } else {
            let row_id_expr = crate::dvm::row_id_expr_for_query(&st.defining_query);
            format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({effective_query}) sub",)
        }
    } else if crate::dvm::query_needs_union_dedup_count(&st.defining_query) {
        let col_names = crate::dvm::get_defining_query_columns(&st.defining_query)?;
        if let Some(union_sql) =
            crate::dvm::try_union_dedup_refresh_sql(&st.defining_query, &col_names)
        {
            union_sql
        } else {
            let row_id_expr = crate::dvm::row_id_expr_for_query(&st.defining_query);
            format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({effective_query}) sub",)
        }
    } else if let Some(ua_sql) = crate::dvm::try_union_all_refresh_sql(&st.defining_query) {
        ua_sql
    } else {
        let row_id_expr = crate::dvm::row_id_expr_for_query(&st.defining_query);
        format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({effective_query}) sub",)
    };

    let insert_sql = format!("INSERT INTO {quoted_table} {insert_body}");
    let rows_inserted = Spi::connect_mut(|client| {
        let result = client
            .update(&insert_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Ok::<usize, PgTrickleError>(result.len())
    })?;

    // Re-enable user triggers and emit NOTIFY so listeners know a FULL
    // refresh occurred.
    if has_triggers {
        Spi::run(&format!("ALTER TABLE {quoted_table} ENABLE TRIGGER USER")) // nosemgrep: rust.spi.run.dynamic-format — ALTER TABLE DDL cannot be parameterized; quoted_table is a PostgreSQL-quoted identifier.
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

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

    // Compute and store frontier so differential can start from here.
    // S3 optimization: single SPI call combines frontier storage,
    // timestamp update, and marking the ST as populated.
    let slot_positions = cdc::get_slot_positions(source_oids)?;
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
        pgrx::info!(
            "Stream table {}.{} refreshed (FULL, no-op — data_timestamp preserved)",
            schema,
            table_name
        );
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

    refresh::poll_foreign_table_sources_for_st(st)?;

    // ST-source guard: if ANY upstream dependency is a STREAM_TABLE, always
    // fall back to a FULL refresh.  The manual FULL refresh path
    // (`execute_manual_full_refresh`) does not populate ST change buffers
    // (`changes_pgt_`), so a downstream DIFFERENTIAL refresh would see an
    // empty change buffer and silently skip real changes.
    // The background scheduler handles this correctly (via
    // `capture_full_refresh_diff_to_st_buffer`), but the manual path
    // does not, so we must force FULL here.
    {
        let deps = StDependency::get_for_st(st.pgt_id).unwrap_or_default();
        let has_st_source = deps.iter().any(|dep| dep.source_type == "STREAM_TABLE");
        if has_st_source {
            return execute_manual_full_refresh(st, schema, table_name, source_oids);
        }
    }

    // Get current WAL positions for non-ST sources (reuses source_oids — G-N3)
    let slot_positions = cdc::get_slot_positions(source_oids)?;
    let data_ts = get_data_timestamp_str();
    let new_frontier = version::compute_new_frontier(&slot_positions, &data_ts);

    // Execute the differential refresh via the DVM engine
    let (rows_inserted, rows_deleted) =
        refresh::execute_differential_refresh(st, &prev_frontier, &new_frontier)?;

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

/// Get source table OIDs for a stream table (used by manual refresh path).
fn get_source_oids_for_manual_refresh(pgt_id: i64) -> Result<Vec<pg_sys::Oid>, PgTrickleError> {
    let deps = StDependency::get_for_st(pgt_id)?;
    Ok(deps
        .into_iter()
        .filter(|dep| dep.source_type == "TABLE" || dep.source_type == "FOREIGN_TABLE")
        .map(|dep| dep.source_relid)
        .collect())
}
