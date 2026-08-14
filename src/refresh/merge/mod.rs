// ARCH-1B: MERGE execution sub-module for the refresh pipeline.
//
// Contains: execute_topk_refresh, execute_full_refresh, execute_no_data_refresh,
// execute_differential_refresh, partition helpers, and capture_delta_explain.
//
// Q-1 (v0.79.0): Removed per-import #[allow(unused_imports)] shims. Imports
// are now concrete; `use super::*` is retained because it provides items
// defined directly in refresh/mod.rs — set_effective_mode, set_refresh_reason,
// classify_query_complexity*, classify_case_in_list_aggregate_drift, and
// classify_correlated_aggregate_subquery_in_where.

use crate::catalog::{StDependency, StreamTableMeta};

use crate::dvm;
use crate::error::PgTrickleError;
use crate::version::Frontier;
use pgrx::prelude::*;

use std::time::Instant;

use super::*;

pub mod columns;
pub mod conflict;
pub mod delete;
pub mod insert;
pub mod update;

// Re-export sub-module items into merge namespace so callers don't need
// to know which sub-module a function lives in.
pub(crate) use columns::*;
pub(crate) use conflict::*;
pub(crate) use delete::*;
pub use insert::execute_topk_refresh;
pub(crate) use update::*;

pub fn execute_full_refresh(st: &StreamTableMeta) -> Result<(i64, i64), PgTrickleError> {
    let dependencies = StDependency::get_for_st(st.pgt_id)?;
    if !st.refresh_mode.is_immediate() {
        crate::cdc::validate_required_change_buffers(st, &dependencies)?;
    }
    let source_oids: Vec<pg_sys::Oid> = dependencies
        .iter()
        .filter(|dependency| {
            matches!(
                dependency.source_type.as_str(),
                "TABLE" | "FOREIGN_TABLE" | "MATVIEW"
            )
        })
        .map(|dependency| dependency.source_relid)
        .collect();
    crate::cdc::lock_source_relations(&source_oids)?;
    crate::cdc::lock_stream_table_sources(st.pgt_id, &dependencies)?;

    // G12-ERM-1: Record the effective mode for this execution path.
    set_effective_mode("FULL");
    // OBS-4: Increment the FULL refresh mode counter.
    crate::shmem::record_refresh_mode(false);

    // EC-25/EC-26: Ensure the internal_refresh flag is set so DML guard
    // triggers allow the refresh executor to modify the storage table.
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    let schema = &st.pgt_schema;
    let name = &st.pgt_name;
    let query = &st.defining_query;

    let quoted_table = format!(
        "\"{}\".\"{}\"",
        schema.replace('"', "\"\""),
        name.replace('"', "\"\""),
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
        Spi::run(&format!("ALTER TABLE {quoted_table} DISABLE TRIGGER USER")) // nosemgrep: rust.spi.run.dynamic-format — ALTER TABLE DDL cannot be parameterized; quoted_table is a PostgreSQL-quoted identifier
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    // For aggregate/distinct STs, inject COUNT(*) AS __pgt_count into the
    // defining query so the auxiliary column is populated correctly.
    let effective_query = if st.refresh_mode == crate::dag::RefreshMode::Differential
        && crate::dvm::query_needs_pgt_count(query)
    {
        let mut eq = crate::api::inject_pgt_count(query);
        // Also inject AVG auxiliary columns (SUM/COUNT of arg) for algebraic
        // AVG maintenance.
        let avg_aux = crate::dvm::query_avg_aux_columns(query);
        if !avg_aux.is_empty() {
            eq = crate::api::inject_avg_aux(&eq, &avg_aux);
        }
        // Also inject sum-of-squares columns for STDDEV/VAR maintenance.
        let sum2_aux = crate::dvm::query_sum2_aux_columns(query);
        if !sum2_aux.is_empty() {
            eq = crate::api::inject_sum2_aux(&eq, &sum2_aux);
        }
        // Also inject nonnull-count columns for SUM NULL-transition correction (P2-2).
        let nonnull_aux = crate::dvm::query_nonnull_aux_columns(query);
        if !nonnull_aux.is_empty() {
            eq = crate::api::inject_nonnull_aux(&eq, &nonnull_aux);
        }
        eq
    } else {
        query.clone()
    };

    // ST-ST-3: Snapshot pre-state for diff capture when this ST has
    // downstream ST consumers. The snapshot is compared against the
    // post-refresh state to produce I/D pairs for the change buffer.
    let needs_diff_capture = has_downstream_st_consumers(st.pgt_id);
    let user_cols = if needs_diff_capture {
        let cols = get_st_user_columns(st);
        let col_list: String = cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect::<Vec<_>>()
            .join(", ");

        // Drop any leftover pre-snapshot from a previous iteration
        // (e.g., SCC fixpoint loops where subtransaction commits don't
        // fire ON COMMIT DROP until the outer transaction commits).
        let _ = Spi::run(&format!("DROP TABLE IF EXISTS __pgt_pre_{}", st.pgt_id)); // nosemgrep: rust.spi.run.dynamic-format — st.pgt_id is a plain i64, not user-supplied input.

        let snapshot_sql = format!(
            "CREATE TEMP TABLE __pgt_pre_{pgt_id} ON COMMIT DROP AS \
             SELECT __pgt_row_id, {col_list} FROM {quoted_table}",
            pgt_id = st.pgt_id,
        );
        Spi::run(&snapshot_sql).map_err(|e| PgTrickleError::RefreshFinalizationFailed {
            pgt_id: st.pgt_id,
            stage: "full-refresh downstream snapshot".to_string(),
            reason: format!("{schema}.{name}: {e}"),
        })?;
        cols
    } else {
        Vec::new()
    };

    // Truncate
    Spi::run(&format!("TRUNCATE {quoted_table}")) // nosemgrep: rust.spi.run.dynamic-format — TRUNCATE DDL cannot be parameterized; quoted_table is a PostgreSQL-quoted identifier
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

    // Compute row_id using the same hash formula as the delta query so
    // the MERGE ON clause matches during subsequent differential refreshes.
    // For INTERSECT/EXCEPT, compute per-branch multiplicities for dual-count
    // storage. For UNION (dedup), convert to UNION ALL and count.
    // For UNION ALL, decompose into per-branch subqueries with
    // child-prefixed row IDs matching diff_union_all's formula.
    let insert_body = if crate::dvm::query_needs_dual_count(query) {
        let col_names = crate::dvm::get_defining_query_columns(query)?;
        if let Some(set_op_sql) = crate::dvm::try_set_op_refresh_sql(query, &col_names) {
            set_op_sql
        } else {
            let row_id_expr = crate::dvm::row_id_expr_for_query(query);
            format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({effective_query}) sub",)
        }
    } else if crate::dvm::query_needs_union_dedup_count(query) {
        let col_names = crate::dvm::get_defining_query_columns(query)?;
        if let Some(union_sql) = crate::dvm::try_union_dedup_refresh_sql(query, &col_names) {
            union_sql
        } else {
            let row_id_expr = crate::dvm::row_id_expr_for_query(query);
            format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({effective_query}) sub",)
        }
    } else if let Some(ua_sql) = crate::dvm::try_union_all_refresh_sql(query) {
        ua_sql
    } else {
        let row_id_expr = crate::dvm::row_id_expr_for_query(query);
        format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({effective_query}) sub",)
    };

    let insert_sql = format!("INSERT INTO {quoted_table} {insert_body}");

    let rows_inserted = Spi::connect_mut(|client| {
        let result = client
            .update(&insert_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Ok::<usize, PgTrickleError>(result.len())
    })?;

    // ST-ST-3: Capture the full-refresh diff into the change buffer.
    // If diff capture fails, downstream DIFFERENTIAL STs would silently
    // diverge because they expect delta rows in changes_pgt_{id}. To
    // prevent that, mark all downstream STs for reinit so they do a FULL
    // refresh next cycle and resync.
    if needs_diff_capture && !user_cols.is_empty() {
        capture_full_refresh_diff_to_st_buffer(st, &user_cols).map_err(|e| {
            PgTrickleError::RefreshFinalizationFailed {
                pgt_id: st.pgt_id,
                stage: "full-refresh downstream CDC capture".to_string(),
                reason: e.to_string(),
            }
        })?;
    }

    // Re-enable user triggers and emit NOTIFY so listeners know a FULL
    // refresh occurred.
    if has_triggers {
        Spi::run(&format!("ALTER TABLE {quoted_table} ENABLE TRIGGER USER")) // nosemgrep: rust.spi.run.dynamic-format — ALTER TABLE DDL cannot be parameterized; quoted_table is a PostgreSQL-quoted identifier
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        // PB2/STAB-1: Skip NOTIFY when pooler compatibility mode is enabled.
        if !crate::config::effective_pooler_compat(st.pooler_compatibility_mode) {
            // Escape single quotes in the JSON payload.
            let escaped_name = name.replace('\'', "''");
            let escaped_schema = schema.replace('\'', "''");
            // NOTIFY does not support parameterized payloads; single quotes are escaped above.
            let notify_sql = format!(
                "NOTIFY pg_trickle_refresh, '{{\"stream_table\": \"{escaped_name}\", \
                 \"schema\": \"{escaped_schema}\", \"mode\": \"FULL\", \"rows\": {rows_inserted}}}'"
            );
            Spi::run(&notify_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }

        pgrx::info!(
            "pg_trickle: FULL refresh of {}.{} with user triggers suppressed ({} rows). \
             Row-level triggers do NOT fire for FULL refresh; use REFRESH MODE DIFFERENTIAL.",
            schema,
            name,
            rows_inserted,
        );
    }

    // PART-WARN: After a successful FULL refresh, warn if the default
    // partition of a partitioned stream table has accumulated rows.
    if st.st_partition_key.is_some() {
        warn_default_partition_growth(schema, name);
    }

    Ok((rows_inserted as i64, 0))
}

/// Post-full-refresh cleanup helper (G3 + G4).
///
/// Intended to be called immediately after any FULL or REINITIALIZE refresh
/// completes successfully, from both the scheduled refresh path and from the
/// adaptive fallback path inside `execute_differential_refresh`.
///
/// 1. **G4 — Change buffer flush**: Deletes stale change buffer rows up to the
///    minimum stored frontier across all stream tables sharing each source.
///    This prevents the next differential tick from re-examining rows that are
///    already materialized, breaking the "adaptive fallback ping-pong" pattern.
///
/// Multi-ST safety: `cleanup_change_buffers_by_frontier` queries the catalog
/// for the minimum frontier across *all* STs that share each source OID, so
/// rows that another ST still needs are never deleted.
pub fn post_full_refresh_cleanup(st: &StreamTableMeta) {
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let deps = crate::catalog::StDependency::get_for_st(st.pgt_id).unwrap_or_default();
    let source_oids: Vec<u32> = deps
        .iter()
        .filter(|d| {
            d.source_type == "TABLE"
                || d.source_type == "FOREIGN_TABLE"
                || d.source_type == "MATVIEW"
        })
        .map(|d| d.source_relid.to_u32())
        .collect();

    // Flush change buffer rows that are now irrelevant because the full
    // refresh already captured them.  Prevents the next differential cycle
    // from re-examining them and re-triggering another adaptive fallback.
    cleanup_change_buffers_by_frontier(&change_schema, &source_oids);

    // NOTE: ST change buffers (changes_pgt_{id}) are intentionally NOT
    // cleaned here. A FULL refresh reads the source table directly, not
    // the change buffer, so change buffer rows have NOT been consumed.
    // Cleaning them would remove rows that sibling consumers (other STs
    // depending on the same upstream ST) have not yet processed.
    // ST change buffer cleanup happens only in the differential refresh
    // path where the delta SQL actually reads from the change buffer.
}

/// Poll all FOREIGN_TABLE and MATVIEW dependencies for a stream table before
/// selecting a new differential frontier.
///
/// Polling writes synthetic CDC rows into the local change buffers and updates
/// the per-source snapshot tables. Callers must do this before capturing the
/// new upper frontier so the synthetic rows fall within the refresh window.
pub fn poll_foreign_table_sources_for_st(st: &StreamTableMeta) -> Result<(), PgTrickleError> {
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");

    for dep in StDependency::get_for_st(st.pgt_id)?
        .into_iter()
        .filter(|dep| dep.source_type == "FOREIGN_TABLE" || dep.source_type == "MATVIEW")
    {
        if dep.source_type == "FOREIGN_TABLE" {
            crate::cdc::poll_foreign_table_changes(dep.source_relid, &change_schema)?;
        } else {
            crate::cdc::poll_matview_changes(dep.source_relid, &change_schema)?;
        }
    }

    Ok(())
}

/// Execute a NO_DATA refresh: just advance the data timestamp.
pub fn execute_no_data_refresh(_st: &StreamTableMeta) -> Result<(), PgTrickleError> {
    // G12-ERM-1: Record the effective mode for this execution path.
    set_effective_mode("NO_DATA");

    // Catalog state is finalized by `refresh::finalize_success` after the
    // immutable frontier has been assembled by the caller.
    Ok(())
}

/// Execute an differential refresh using the DVM engine.
///
/// 1. Short-circuits if no source table has changes in the LSN window
/// 2. Uses cached delta + MERGE SQL templates (or generates on first call)
/// 3. Resolves LSN placeholders with actual frontier values
/// 4. Applies the delta to the ST storage table via a single MERGE statement
///
/// ## Caching
///
/// The first refresh for a ST generates a SQL template with placeholder
/// tokens for LSN values. Subsequent refreshes skip parsing, DVM
/// differentiation, and MERGE SQL construction — they only substitute
/// LSN values and execute. This eliminates ~45ms overhead per refresh.
/// EC-16: Check whether any function referenced in `st.functions_used` has
/// changed its source code since the last differential refresh.
///
/// For each function name in `functions_used`, queries `pg_proc` for the
/// concatenated `md5(prosrc || coalesce(probin::text, ''))` of all matching
/// overloads (joined by `,` to handle polymorphic overloads stably).  The
/// resulting `{ "func_name": "md5hex", ... }` JSON map is compared against
/// `st.function_hashes`.
///
/// **On the first call** (`st.function_hashes` is `None`): stores the current
/// hashes and returns `false` (no change — baseline is being established).
///
/// **On subsequent calls**: returns `true` iff any hash differs, in which case
/// the new hashes are persisted before returning.
///
/// Errors during SPI queries are logged and treated as "no change" to avoid
/// cascading failures from a transient catalog problem.
pub fn execute_differential_refresh(
    st: &StreamTableMeta,
    prev_frontier: &Frontier,
    new_frontier: &Frontier,
) -> Result<(i64, i64), PgTrickleError> {
    let schema = &st.pgt_schema;
    let name = &st.pgt_name;
    // F10: record start time for OTLP span (nanoseconds since Unix epoch).
    let start_ns = crate::otel::now_ns();

    if !st.is_populated {
        return Err(PgTrickleError::InvalidArgument(format!(
            "Cannot run DIFFERENTIAL refresh on unpopulated stream table {}.{}; a FULL refresh is required first.",
            schema, name
        )));
    }

    if prev_frontier.is_empty() {
        return Err(PgTrickleError::InvalidArgument(format!(
            "Cannot run DIFFERENTIAL refresh on {}.{}; no previous frontier exists.",
            schema, name
        )));
    }

    let dependencies = StDependency::get_for_st(st.pgt_id)?;
    let _validated_buffers = crate::cdc::validate_required_change_buffers(st, &dependencies)?;

    // ── EC-16: Function-body change detection ────────────────────────
    // Check whether any user-defined function referenced in this ST's
    // defining query has had its source code changed via ALTER FUNCTION
    // or CREATE OR REPLACE FUNCTION.  If so, force a full reinit on the
    // next scheduler cycle and skip the differential refresh this cycle.
    if check_proc_hashes_changed(st) {
        if let Err(e) = crate::catalog::StreamTableMeta::mark_for_reinitialize(st.pgt_id) {
            pgrx::warning!("[pg_trickle] EC-16: failed to mark ST {schema}.{name} for reinit: {e}");
        } else {
            pgrx::notice!(
                "[pg_trickle] EC-16: function body change detected for stream table \
                 {schema}.{name} — marked for full reinitialization on next cycle."
            );
        }
        return Ok((0, 0));
    }

    // ── DVM-1 (v0.78.0): CASE/IN-list aggregate drift — definitive rejection path ──
    // SUM/COUNT(CASE…) + IN-list WHERE predicates can drift when an UPDATE
    // changes the CASE-condition column: the algebraic delta evaluates the CASE
    // with the new value for both the 'D' and 'I' sides, miscounting the old
    // contribution (DI-8).
    //
    // Definitive rejection rule:
    //   • Append-only sources (no UPDATEs): GROUP_RESCAN (EXCEPT ALL) is
    //     correct — INSERT/DELETE never change CASE-condition columns.
    //     Bypass the FULL fallback and let GROUP_RESCAN handle it.
    //   • Mutable sources: the drift CAN occur via UPDATE.  FULL refresh is the
    //     definitive rejection path until DI-2 (UPDATE-split) lands.
    if crate::refresh::classify_case_in_list_aggregate_drift(&st.defining_query)
        && !st.is_append_only
    {
        crate::refresh::set_refresh_reason("CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK");
        return Err(PgTrickleError::QueryTooComplex(format!(
            "CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK: {schema}.{name} uses a CASE \
             aggregate with an IN-list WHERE predicate on a mutable source — \
             UPDATE can cause CASE-condition drift (DI-8). \
             Definitive rejection path: use is_append_only=true or wait for DI-2."
        )));
    }

    // ── DVM-2/P-1 (v0.78.0): Correlated aggregate subquery — attempt CTE rewrite ──
    // Queries with a scalar correlated aggregate subquery in the top-level WHERE
    // (e.g. col > (SELECT SUM(…) FROM t WHERE t.k = outer.k)) produce
    // O(delta × table) DVM delta SQL when evaluated naively.
    //
    // Safe patterns: single-level, bare-column equality correlations.
    //   → Attempt CTE pre-aggregation rewrite (decorrelate_scalar_subquery).
    //   → If successful: the rewritten query is differentially safe.
    // Unsafe patterns: nested IN-sublinks, dot-qualified correlation, etc.
    //   → FULL fallback with CORRELATED_SUBQUERY_DELTA_QUADRATIC reason.
    let effective_defining_query: std::borrow::Cow<str> =
        if crate::refresh::classify_correlated_aggregate_subquery_in_where(&st.defining_query) {
            match crate::dvm::rewrite_scalar_subquery_in_where(&st.defining_query) {
                Ok(rewritten)
                    if !crate::refresh::classify_correlated_aggregate_subquery_in_where(
                        &rewritten,
                    ) =>
                {
                    pgrx::debug1!(
                        "[pg_trickle] DVM-2: successfully rewrote correlated aggregate \
                         subquery for {schema}.{name} — using differential path"
                    );
                    std::borrow::Cow::Owned(rewritten)
                }
                _ => {
                    // Rewrite did not eliminate the correlated aggregate — unsafe pattern.
                    crate::refresh::set_refresh_reason("CORRELATED_SUBQUERY_DELTA_QUADRATIC");
                    return Err(PgTrickleError::QueryTooComplex(format!(
                        "CORRELATED_SUBQUERY_DELTA_QUADRATIC: {schema}.{name} has a \
                         correlated aggregate subquery in WHERE that cannot be safely \
                         pre-aggregated (nested or dot-qualified correlation). \
                         Definitive rejection path: rewrite manually or use FULL mode."
                    )));
                }
            }
        } else {
            std::borrow::Cow::Borrowed(&st.defining_query)
        };

    // ── O-1 (v0.80.0): CASE aggregate with subquery predicate — classifier uncertain ──
    // When the CASE expression inside an aggregate contains a scalar subquery or
    // EXISTS predicate, neither the DVM-1 nor DVM-2 heuristic covers the pattern
    // but string-based analysis cannot confirm the algebraic delta is correct.
    // Force FULL refresh and emit the REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN reason
    // code so operators can identify affected stream tables.
    if crate::refresh::classify_case_aggregate_subquery_uncertain(&effective_defining_query) {
        crate::refresh::set_refresh_reason("REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN");
        return Err(PgTrickleError::QueryTooComplex(format!(
            "REGEX_COMPLEXITY_CLASSIFIER_UNCERTAIN: {schema}.{name} has a CASE \
             aggregate with a subquery or EXISTS predicate — the string-based \
             classifier cannot confirm DVM algebraic delta safety. \
             Falling back to FULL refresh."
        )));
    }

    // ── P-2 (v0.78.0): Lazy complexity class back-fill ───────────────
    // If the catalog doesn't yet have a complexity class (newly created
    // stream table or upgraded from pre-0.78.0), compute and persist it now.
    // This is a best-effort fire-and-forget — failure is logged but not fatal.
    if st.query_complexity_class.is_none() {
        let class = classify_query_complexity_optree(&effective_defining_query);
        if let Err(e) =
            crate::catalog::StreamTableMeta::update_query_complexity_class(st.pgt_id, class)
        {
            pgrx::warning!(
                "[pg_trickle] P-2: failed to persist complexity class for {schema}.{name}: {e}"
            );
        }
    }

    // ── DI-7: Join-count complexity guard ────────────────────────────
    // Parse the defining query (lightweight — no differentiation) to count
    // Scan nodes in the join tree. Used for:
    //   (a) max_differential_joins guard: reject if too complex for DIFF
    //   (b) deep-join planner hints: SET LOCAL for 5+ table joins
    let scan_count = dvm::query_total_scan_count(&effective_defining_query).unwrap_or_else(|e| {
        pgrx::warning!(
            "[pg_trickle] DI-7: failed to count scans for {schema}.{name}: {e}; \
             assuming scan_count=1"
        );
        1
    });

    if let Some(max_joins) = st.max_differential_joins
        && max_joins > 0
    {
        // DI-7 uses the join-specific scan count (stops at Aggregate etc.)
        let join_sc = dvm::query_join_scan_count(&effective_defining_query).unwrap_or(0);
        if join_sc > max_joins as usize {
            return Err(PgTrickleError::QueryTooComplex(format!(
                "join scan count ({join_sc}) exceeds max_differential_joins ({max_joins}) \
                 for {schema}.{name}; falling back to FULL refresh"
            )));
        }
    }

    // ── Short-circuit: skip the entire pipeline if no changes exist ──────
    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let catalog_source_oids: Vec<u32> = dependencies
        .iter()
        .filter(|dep| {
            dep.source_type == "TABLE"
                || dep.source_type == "FOREIGN_TABLE"
                || dep.source_type == "MATVIEW"
        })
        .map(|dep| dep.source_relid.to_u32())
        .collect();

    // ── Pre-flight: verify all change buffer tables exist ─────────────
    // PERF-10-01: Single batch query instead of N separate to_regclass()
    // calls — saves one SPI round-trip per source OID.
    //
    // Query pg_class (safe — never errors for catalog tables) to confirm
    // that every source's change buffer table still exists.  If any are
    // missing (e.g. race with a concurrent DROP or stale deps), skip the
    // refresh instead of crashing with a relation-not-found ERROR.
    //
    // IMPORTANT: Change buffer tables are named `changes_{stable_name}` where
    // stable_name is the xxh64 hash of "schema.table" stored in
    // pgtrickle.pgt_change_tracking.  The LEFT JOIN resolves the stable name;
    // the COALESCE falls back to `changes_{oid}` only for untracked sources
    // (mirrors buffer_base_name_for_oid logic exactly).
    if !catalog_source_oids.is_empty() {
        let oids_sql = catalog_source_oids
            .iter()
            .map(|o| format!("{o}::oid"))
            .collect::<Vec<_>>()
            .join(", ");
        // OIDs are numeric system-catalog values; change_schema is a GUC value, not user input.
        let batch_sql = format!(
            "SELECT o.oid::bigint, \
             to_regclass('{change_schema}.' || \
                 COALESCE('changes_' || ct.source_stable_name, 'changes_' || o.oid::text) \
             ) IS NOT NULL AS reg_exists \
             FROM unnest(ARRAY[{oids_sql}]) AS o(oid) \
             LEFT JOIN pgtrickle.pgt_change_tracking ct ON ct.source_relid = o.oid"
        ); // nosemgrep: rust.spi.query.dynamic-format — oids_sql contains only numeric OIDs; change_schema is a GUC value
        let missing_oid: Option<u32> = Spi::connect(|client| {
            let result = client
                .select(&batch_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(format!("batch preflight: {e}")))?;
            for row in result {
                let oid = row.get::<i64>(1).unwrap_or(None).unwrap_or(0) as u32;
                let exists = row.get::<bool>(2).unwrap_or(None).unwrap_or(false);
                if !exists {
                    return Ok::<Option<u32>, PgTrickleError>(Some(oid));
                }
            }
            Ok(None)
        })?;
        if let Some(missing) = missing_oid {
            let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(missing));
            return Err(PgTrickleError::CdcStateInvalid {
                pgt_id: st.pgt_id,
                source_name: format!("OID {missing}"),
                buffer: format!("{change_schema}.{buf_name}"),
                reason: "required change buffer relation is missing".to_string(),
            });
        }
    }
    pgrx::debug1!(
        "[pg_trickle] PREFLIGHT OK for ST {}.{} — source OIDs: {:?}",
        schema,
        name,
        catalog_source_oids,
    );

    // ── DI-7: Delta fraction guard ──────────────────────────────────
    // When the user has configured `max_delta_fraction`, compare the
    // total change buffer row count against the ST's estimated row count
    // (pg_class.reltuples). If the ratio exceeds the threshold, reject
    // the differential refresh so the scheduler can fall back to FULL —
    // TRUNCATE + INSERT is faster than applying a large fraction of the
    // table as individual deltas.
    if let Some(max_frac) = st.max_delta_fraction
        && max_frac > 0.0
    {
        let total_changes: i64 = catalog_source_oids
            .iter()
            .map(|&oid| {
                let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(oid));
                let q = format!("SELECT count(*)::bigint FROM \"{change_schema}\".{buf_name}");
                Spi::get_one::<i64>(&q).unwrap_or(Some(0)).unwrap_or(0)
            })
            .sum();

        if total_changes > 0 {
            let estimated_rows: f64 = Spi::get_one::<f32>(&format!(
                "SELECT reltuples FROM pg_class WHERE oid = {}::oid",
                st.pgt_relid.to_u32(),
            ))
            .unwrap_or(Some(0.0))
            .unwrap_or(0.0) as f64;

            // Only check when reltuples > 0 (avoids division by zero and
            // ANALYZE-not-yet-run edge case).
            if estimated_rows > 0.0
                && delta_fraction_exceeds_threshold(total_changes, estimated_rows, max_frac)
            {
                let fraction = total_changes as f64 / estimated_rows;
                return Err(PgTrickleError::QueryTooComplex(format!(
                    "delta fraction ({:.2}%) exceeds max_delta_fraction ({:.2}%) \
                     for {schema}.{name} ({total_changes} changes / \
                     {estimated_rows:.0} estimated rows); falling back to FULL refresh",
                    fraction * 100.0,
                    max_frac * 100.0,
                )));
            }
        }
    }

    // C-1: Drain any deferred cleanups from the previous refresh cycle.
    // This runs before the decision query so stale rows are removed
    // before we check for new changes.
    drain_pending_cleanups();

    // C-1b: Frontier-based cleanup — always runs regardless of thread-local
    // state.  The deferred cleanup (above) relies on PENDING_CLEANUP in
    // thread-local storage, which is lost when a connection pool dispatches
    // successive refresh calls to different PostgreSQL backend processes.
    // This additional pass uses the catalog frontier (persisted in
    // pgt_stream_tables.frontier) to compute the safe cleanup threshold,
    // ensuring stale change buffer rows are removed even when the
    // thread-local queue is empty.
    cleanup_change_buffers_by_frontier(&change_schema, &catalog_source_oids);

    // C-1c: ST buffer cleanup — delete consumed rows from upstream ST
    // change buffers (changes_pgt_{id}) using frontier thresholds.
    let st_source_pgt_ids: Vec<i64> = StDependency::get_for_st(st.pgt_id)
        .unwrap_or_default()
        .iter()
        .filter(|dep| dep.source_type == "STREAM_TABLE")
        .filter_map(|dep| crate::catalog::StreamTableMeta::pgt_id_for_relid(dep.source_relid))
        .collect();
    cleanup_st_change_buffers_by_frontier(&change_schema, &st_source_pgt_ids);

    // C-4: Compact change buffers that exceed the configured threshold.
    // This reduces delta scan overhead by eliminating net-zero changes
    // (INSERT→DELETE pairs) and collapsing multi-change groups.
    for &oid in &catalog_source_oids {
        let prev_lsn = prev_frontier.get_lsn(oid);
        let new_lsn = new_frontier.get_lsn(oid);
        match crate::cdc::compact_change_buffer(&change_schema, oid, &prev_lsn, &new_lsn) {
            Ok(crate::cdc::CompactionResult::Contended) => {
                // COR-4: Lock was held by concurrent refresh — already counted in shmem.
                pgrx::debug1!(
                    "[pg_trickle] C-4: compaction contended for changes_{} (advisory lock busy)",
                    oid,
                );
            }
            Ok(_) => {}
            Err(e) => {
                pgrx::debug1!(
                    "[pg_trickle] C-4: compaction failed for changes_{}: {}",
                    oid,
                    e,
                );
            }
        }
    }

    // PERF-2: Auto-promote unpartitioned buffers to RANGE(lsn) partitioned
    // mode when `buffer_partitioning = 'auto'` and the buffer fill rate
    // exceeds `compact_threshold` within a single refresh cycle.
    for &oid in &catalog_source_oids {
        let prev_lsn = prev_frontier.get_lsn(oid);
        let new_lsn = new_frontier.get_lsn(oid);
        let pending = crate::cdc::count_pending_changes(&change_schema, oid, &prev_lsn, &new_lsn);
        match crate::cdc::maybe_auto_promote_buffer(&change_schema, oid, pending) {
            Ok(true) => {
                pgrx::debug1!(
                    "[pg_trickle] PERF-2: auto-promoted changes_{} to partitioned mode \
                     (pending={} exceeded threshold)",
                    oid,
                    pending,
                );
            }
            Err(e) => {
                pgrx::warning!(
                    "[pg_trickle] PERF-2: auto-promotion failed for changes_{}: {}",
                    oid,
                    e,
                );
            }
            _ => {}
        }
    }

    // DAG-5: Compact ST change buffers that exceed the threshold.
    // During rapid-fire upstream refreshes, multiple rounds of I/D pairs
    // accumulate in changes_pgt_{id} between downstream reads. Compaction
    // cancels net-zero INSERT/DELETE pairs and removes intermediate rows.
    for &upstream_pgt_id in &st_source_pgt_ids {
        if !crate::cdc::has_st_change_buffer(upstream_pgt_id, &change_schema) {
            continue;
        }
        let key = format!("pgt_{upstream_pgt_id}");
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
        if let Err(e) = crate::cdc::compact_st_change_buffer(
            &change_schema,
            upstream_pgt_id,
            &prev_lsn,
            &new_lsn,
        ) {
            pgrx::debug1!(
                "[pg_trickle] DAG-5: compaction failed for changes_pgt_{}: {}",
                upstream_pgt_id,
                e,
            );
        }
    }

    let t_decision_start = Instant::now();

    // ── E-1: Ultra-fast EXISTS for no-data short-circuit ─────────────
    // Build a single UNION ALL / EXISTS query that checks ALL source
    // change buffers in one SPI call.  The query short-circuits on the
    // first row found, making the no-data case O(index-probe) per source
    // instead of the heavier LATERAL + pg_class join used for threshold
    // computation.
    let any_changes = if catalog_source_oids.is_empty() {
        false
    } else if catalog_source_oids.len() == 1 {
        // Single source — simple EXISTS, no UNION ALL overhead
        let oid = catalog_source_oids[0];
        let prev_lsn = prev_frontier.get_lsn(oid);
        let new_lsn = new_frontier.get_lsn(oid);
        let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(oid));
        Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(\
               SELECT 1 FROM \"{change_schema}\".{buf_name} \
               WHERE lsn > '{prev_lsn}'::pg_lsn \
               AND lsn <= '{new_lsn}'::pg_lsn \
               LIMIT 1\
             )",
        ))
        .unwrap_or(Some(false))
        .unwrap_or(false)
    } else {
        // Multiple sources — UNION ALL wrapped in EXISTS.
        let union_parts: Vec<String> = catalog_source_oids
            .iter()
            .map(|oid| {
                let prev_lsn = prev_frontier.get_lsn(*oid);
                let new_lsn = new_frontier.get_lsn(*oid);
                let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(*oid));
                format!(
                    "SELECT 1 FROM \"{change_schema}\".{buf_name} \
                     WHERE lsn > '{prev_lsn}'::pg_lsn \
                     AND lsn <= '{new_lsn}'::pg_lsn",
                )
            })
            .collect();
        let union_sql = union_parts.join(" UNION ALL ");
        Spi::get_one::<bool>(&format!("SELECT EXISTS({union_sql})",))
            .unwrap_or(Some(false))
            .unwrap_or(false)
    };

    // Also check ST (stream table) change buffers for pending changes.
    // Without this, pure ST-on-ST dependencies (catalog_source_oids is
    // empty) would always short-circuit and never run DIFFERENTIAL.
    let any_st_changes = if !any_changes && !st_source_pgt_ids.is_empty() {
        st_source_pgt_ids.iter().any(|&pgt_id| {
            if !crate::cdc::has_st_change_buffer(pgt_id, &change_schema) {
                return false;
            }
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
            Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(\
                   SELECT 1 FROM \"{change_schema}\".changes_pgt_{pgt_id} \
                   WHERE lsn > '{prev_lsn}'::pg_lsn \
                   AND lsn <= '{new_lsn}'::pg_lsn \
                   LIMIT 1\
                 )",
            ))
            .unwrap_or(Some(false))
            .unwrap_or(false)
        })
    } else {
        false
    };

    if !any_changes && !any_st_changes {
        return Ok((0, 0));
    }

    // ── A-3a: Append-only heuristic fallback ─────────────────────────
    // When the stream table is marked append-only, check whether any
    // DELETE or UPDATE actions appeared in the change buffers. If so,
    // revert the flag and fall through to the normal MERGE path.
    let mut is_append_only = st.is_append_only;
    if is_append_only {
        let has_non_insert = catalog_source_oids.iter().any(|oid| {
            let prev_lsn = prev_frontier.get_lsn(*oid);
            let new_lsn = new_frontier.get_lsn(*oid);
            let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(*oid));
            match Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(\
                   SELECT 1 FROM \"{change_schema}\".{buf_name} \
                   WHERE lsn > '{prev_lsn}'::pg_lsn \
                   AND lsn <= '{new_lsn}'::pg_lsn \
                   AND action IN ('D', 'U') \
                   LIMIT 1\
                 )",
            )) {
                Ok(Some(v)) => v,
                Ok(None) => false,
                Err(e) => {
                    // SPI failure: treat as "found non-insert" (safe default).
                    pgrx::warning!(
                        "[pg_trickle] Append-only DELETE/UPDATE check failed for \
                         {} — falling back to MERGE path: {}",
                        buf_name,
                        e,
                    );
                    true
                }
            }
        })
        // Also check ST (stream table) source change buffers.
        // Without this, ST-on-ST cascades with empty catalog_source_oids
        // would vacuously miss DELETE/UPDATE actions from the upstream ST.
        || st_source_pgt_ids.iter().any(|&pgt_id| {
            if !crate::cdc::has_st_change_buffer(pgt_id, &change_schema) {
                return false;
            }
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
            match Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(\
                   SELECT 1 FROM \"{change_schema}\".changes_pgt_{pgt_id} \
                   WHERE lsn > '{prev_lsn}'::pg_lsn \
                   AND lsn <= '{new_lsn}'::pg_lsn \
                   AND action IN ('D', 'U') \
                   LIMIT 1\
                 )",
            )) {
                Ok(Some(v)) => v,
                Ok(None) => false,
                Err(_) => true, // SPI failure: safe default
            }
        });

        if has_non_insert {
            pgrx::warning!(
                "[pg_trickle] Append-only stream table {}.{} received DELETE/UPDATE — \
                 reverting to MERGE path.",
                schema,
                name,
            );
            is_append_only = false;
            if let Err(e) = StreamTableMeta::update_append_only(st.pgt_id, false) {
                pgrx::warning!(
                    "[pg_trickle] Failed to clear is_append_only for {}.{}: {}",
                    schema,
                    name,
                    e,
                );
            }
            // Flush MERGE template cache so next cycle rebuilds with MERGE path.
            crate::shmem::bump_cache_generation();
            // NS-2: Emit NOTIFY alert so operators are informed of the revert.
            crate::monitor::emit_alert(
                crate::monitor::AlertEvent::AppendOnlyReverted,
                schema,
                name,
                serde_json::json!({}),
                st.pooler_compatibility_mode,
            );
        }
    }

    // ── A-3-AO: Append-only heuristic auto-promotion ────────────────
    // When the stream table is NOT marked append-only, check whether the
    // current change buffer batch is INSERT-only. If so, opportunistically
    // use the INSERT fast path for this refresh cycle. The flag is set in
    // the catalog so subsequent refreshes also use the fast path until a
    // DELETE/UPDATE is detected (handled by the revert block above).
    //
    // Skip promotion for queries with non-monotonic operators (LEFT JOIN,
    // aggregates, anti-joins, etc.) where source INSERTs can produce delta
    // DELETEs — the append-only fast path would silently drop those DELETEs.
    // PERF-4: Extract both flags in a single `peek()` borrow to avoid
    // two separate cache-lock acquisitions and spurious LRU promotions.
    let (cached_non_monotonic, cached_is_deduplicated) = MERGE_TEMPLATE_CACHE.with(|cache| {
        cache
            .borrow()
            .peek(&st.pgt_id)
            .map(|entry| {
                (
                    has_non_monotonic_cte(&entry.merge_sql_template),
                    entry.is_deduplicated,
                )
            })
            .unwrap_or((false, true)) // no cache: allow promotion; assume dedup (safe)
    });
    if !is_append_only
        && !st.has_keyless_source
        && !cached_non_monotonic
        && cached_is_deduplicated
        && !has_downstream_st_consumers(st.pgt_id)
    {
        let has_non_insert = catalog_source_oids.iter().any(|oid| {
            let prev_lsn = prev_frontier.get_lsn(*oid);
            let new_lsn = new_frontier.get_lsn(*oid);
            let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(*oid));
            match Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(\
                   SELECT 1 FROM \"{change_schema}\".{buf_name} \
                   WHERE lsn > '{prev_lsn}'::pg_lsn \
                   AND lsn <= '{new_lsn}'::pg_lsn \
                   AND action IN ('D', 'U') \
                   LIMIT 1\
                 )",
            )) {
                Ok(Some(v)) => v,
                Ok(None) => false,
                Err(_) => true, // SPI failure: safe default (skip heuristic)
            }
        })
        // Also check ST source change buffers for DELETE/UPDATE actions.
        // Without this, ST-on-ST cascades (catalog_source_oids is empty)
        // would vacuously find no non-INSERT actions and incorrectly
        // promote to append-only, causing duplicate rows.
        || st_source_pgt_ids.iter().any(|&pgt_id| {
            if !crate::cdc::has_st_change_buffer(pgt_id, &change_schema) {
                return false;
            }
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
            match Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(\
                   SELECT 1 FROM \"{change_schema}\".changes_pgt_{pgt_id} \
                   WHERE lsn > '{prev_lsn}'::pg_lsn \
                   AND lsn <= '{new_lsn}'::pg_lsn \
                   AND action IN ('D', 'U') \
                   LIMIT 1\
                 )",
            )) {
                Ok(Some(v)) => v,
                Ok(None) => false,
                Err(_) => true, // SPI failure: safe default
            }
        });

        if !has_non_insert {
            pgrx::debug1!(
                "[pg_trickle] A-3-AO: heuristic append-only promotion for {}.{} — \
                 current batch is INSERT-only",
                schema,
                name,
            );
            is_append_only = true;
            // Persist the flag so subsequent refreshes also use the fast path.
            // If a DELETE/UPDATE appears later, the revert block above will
            // clear it and emit a WARNING + NOTIFY.
            if let Err(e) = StreamTableMeta::update_append_only(st.pgt_id, true) {
                pgrx::debug1!(
                    "[pg_trickle] A-3-AO: failed to set is_append_only for {}.{}: {}",
                    schema,
                    name,
                    e,
                );
                is_append_only = false; // revert on failure
            }
        }
    }

    // ── S2: TRUNCATE detection ───────────────────────────────────────
    // If any source table was TRUNCATEd, the change buffer contains a
    // marker row with action='T'. Differential deltas cannot represent
    // a TRUNCATE — fall back to full refresh.
    let has_truncate = catalog_source_oids.iter().any(|oid| {
        let prev_lsn = prev_frontier.get_lsn(*oid);
        let new_lsn = new_frontier.get_lsn(*oid);
        let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(*oid));
        Spi::get_one::<bool>(&format!(
            "SELECT EXISTS(\
               SELECT 1 FROM \"{change_schema}\".{buf_name} \
               WHERE lsn > '{prev_lsn}'::pg_lsn \
               AND lsn <= '{new_lsn}'::pg_lsn \
               AND action = 'T' \
               LIMIT 1\
             )",
        ))
        .unwrap_or(Some(false))
        .unwrap_or(false)
    });

    if has_truncate {
        let is_single_source = catalog_source_oids.len() == 1;
        let is_pure_truncate = is_single_source && {
            let oid = catalog_source_oids[0];
            let prev_lsn = prev_frontier.get_lsn(oid);
            let new_lsn = new_frontier.get_lsn(oid);
            let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(oid));
            !Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(\
                   SELECT 1 FROM \"{change_schema}\".{buf_name} \
                   WHERE lsn > '{prev_lsn}'::pg_lsn \
                   AND lsn <= '{new_lsn}'::pg_lsn \
                   AND action != 'T' \
                   LIMIT 1\
                 )",
            ))
            .unwrap_or(Some(false))
            .unwrap_or(false)
        };

        if is_pure_truncate {
            return execute_incremental_truncate_delete(st);
        }

        pgrx::info!(
            "[pg_trickle] Source table TRUNCATE detected — falling back to FULL refresh for {}.{}",
            schema,
            name,
        );
        let truncate_full_result = execute_full_refresh(st);
        if truncate_full_result.is_ok() {
            post_full_refresh_cleanup(st);
        }
        return truncate_full_result;
    }

    // ── P2: Capped-count threshold check (only when changes exist) ───────
    // Now that we know changes exist, check whether the change volume
    // exceeds the adaptive fallback threshold.  This heavier query is
    // skipped entirely for the no-data case (handled above).
    //
    // B-4: Check the refresh_strategy GUC first. If it's 'full', force
    // fallback unconditionally. If it's 'differential', skip the adaptive
    // threshold check entirely (never fall back). 'auto' uses the existing
    // adaptive heuristic.
    let strategy = crate::config::pg_trickle_refresh_strategy();
    let global_ratio = crate::config::pg_trickle_differential_max_change_ratio();
    let max_ratio = st.auto_threshold.unwrap_or(global_ratio);
    let mut should_fallback = strategy == crate::config::RefreshStrategy::Full;
    let skip_ratio_check = strategy == crate::config::RefreshStrategy::Differential;
    let mut total_change_count: i64 = 0;
    let mut _total_table_size: i64 = 0;
    // DI-2: Collect per-source (change_count, table_size) for the per-leaf
    // fallback decision after the P2 loop completes.
    let mut per_source_stats: Vec<(u32, i64, i64)> = Vec::new();
    // B3-1: Track source OIDs with zero changes for delta-branch pruning.
    let mut zero_change_oids: std::collections::HashSet<u32> = std::collections::HashSet::new();

    for oid in &catalog_source_oids {
        let prev_lsn = prev_frontier.get_lsn(*oid);
        let new_lsn = new_frontier.get_lsn(*oid);
        let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(*oid));

        let max_ratio_lit = if max_ratio > 0.0 {
            format!("{max_ratio}")
        } else {
            "0".to_string()
        };

        let sql = format!(
            "SELECT sz.table_size, cnt.change_count \
             FROM (SELECT GREATEST(reltuples::bigint, 1000) AS table_size \
                   FROM pg_class WHERE oid = {oid}::oid) sz, \
             LATERAL (SELECT count(*)::bigint AS change_count FROM (\
                SELECT 1 FROM \"{change_schema}\".{buf_name} \
                WHERE lsn > '{prev_lsn}'::pg_lsn \
                AND lsn <= '{new_lsn}'::pg_lsn \
                LIMIT CASE WHEN {max_ratio_lit} > 0 \
                      THEN (sz.table_size::double precision * {max_ratio_lit})::bigint + 1 \
                      ELSE 9223372036854775807 END\
             ) __pgt_capped) cnt",
        );

        // Defensive: if the change-buffer table was dropped between the
        // EXISTS check and this threshold query (e.g., during concurrent
        // DROP STREAM TABLE), treat it as zero changes rather than
        // propagating a "relation does not exist" error.
        let (table_size, change_count) = match Spi::connect(|client| {
            let row = client
                .select(&sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .first();
            let ts: i64 = row.get::<i64>(1).unwrap_or(Some(1000)).unwrap_or(1000);
            let cc: i64 = row.get::<i64>(2).unwrap_or(Some(0)).unwrap_or(0);
            Ok::<(i64, i64), PgTrickleError>((ts, cc))
        }) {
            Ok(pair) => pair,
            Err(e) => {
                pgrx::debug1!(
                    "[pg_trickle] Threshold check for {} failed (table dropped?): {}",
                    buf_name,
                    e,
                );
                (1000, 0)
            }
        };

        let threshold_rows = if max_ratio > 0.0 {
            ((table_size as f64) * max_ratio).ceil() as i64
        } else {
            i64::MAX / 2
        };

        total_change_count += change_count;
        _total_table_size += table_size;

        // DI-2: Save per-source stats for per-leaf fallback decision.
        per_source_stats.push((*oid, change_count, table_size));

        // B3-1: Record sources with zero changes for delta-branch pruning.
        if change_count == 0 {
            zero_change_oids.insert(*oid);
        }

        if change_count > threshold_rows {
            // B-4: When refresh_strategy = 'differential', skip the ratio
            // check — the user explicitly wants DIFFERENTIAL regardless of
            // change volume. The BUF-LIMIT safety check still applies below.
            if !skip_ratio_check {
                should_fallback = true;
                break; // No need to check remaining sources
            }
        }
    }

    // ── BUF-LIMIT: Hard buffer growth limit ─────────────────────────
    // If any source's change buffer exceeds max_buffer_rows, force FULL
    // refresh to prevent unbounded disk growth from repeated failures.
    let max_buffer_rows = crate::config::pg_trickle_max_buffer_rows();
    if !should_fallback && max_buffer_rows > 0 {
        for &(oid, change_count, _table_size) in &per_source_stats {
            if change_count > max_buffer_rows {
                pgrx::warning!(
                    "[pg_trickle] Change buffer for source OID {} of {}.{} has {} rows, \
                     exceeding max_buffer_rows limit ({}). Forcing FULL refresh and \
                     truncating buffer to prevent unbounded growth.",
                    oid,
                    st.pgt_schema,
                    st.pgt_name,
                    change_count,
                    max_buffer_rows,
                );
                should_fallback = true;
                break;
            }
        }
    }

    // ── B-4: Pre-refresh cost-model prediction ──────────────────────
    // When strategy = 'auto' and the ratio check didn't trigger, query
    // historical refresh timings and use the cost model to predict
    // whether DIFFERENTIAL or FULL is cheaper for the *current* delta.
    if !should_fallback && !skip_ratio_check && total_change_count > 0 {
        let complexity = classify_query_complexity(&effective_defining_query);
        if let Some(hist) = query_refresh_history_stats(st.pgt_id)
            && cost_model_prefers_full(
                hist.avg_ms_per_delta,
                hist.avg_full_ms,
                total_change_count,
                complexity,
                crate::config::pg_trickle_cost_model_safety_margin(),
            )
        {
            pgrx::debug1!(
                "[pg_trickle] B-4 cost model: FULL preferred for {}.{} \
                 (est_diff={:.1}ms > est_full×margin={:.1}ms, class={:?}, Δ={})",
                st.pgt_schema,
                st.pgt_name,
                hist.avg_ms_per_delta * complexity.diff_cost_factor() * total_change_count as f64,
                hist.avg_full_ms * crate::config::pg_trickle_cost_model_safety_margin(),
                complexity,
                total_change_count,
            );
            should_fallback = true;
        }
    }

    if should_fallback {
        // A22 (v0.35.0): Emit NOTICE on every FULL fallback so operators
        // see the reason in client log output.
        let reason = if crate::config::pg_trickle_force_full_refresh() {
            "force_full_refresh GUC override".to_string()
        } else if strategy == crate::config::RefreshStrategy::Full {
            "refresh_strategy=full GUC".to_string()
        } else {
            format!("change ratio exceeds threshold ({:.1}%)", max_ratio * 100.0,)
        };
        pgrx::notice!(
            "stream table \"{}\".\"{}\" using FULL refresh: {}",
            st.pgt_schema,
            st.pgt_name,
            reason,
        );
        pgrx::warning!(
            "[pg_trickle] Falling back to FULL refresh for {}.{}: change ratio exceeds \
             adaptive threshold ({:.0}% of source table size).\n\
             This means too many rows changed since the last refresh for differential \
             mode to be efficient. \n\
             Suggestion: increase pg_trickle.differential_max_change_ratio (currently {:.2}), \
             adjust the per-table auto_threshold via ALTER STREAM TABLE ... SET (auto_threshold = ...), \
             or refresh more frequently to reduce the change volume per cycle.",
            st.pgt_schema,
            st.pgt_name,
            max_ratio * 100.0,
            max_ratio,
        );
        let t_full_start = Instant::now();
        let result = execute_full_refresh(st);
        let full_ms = t_full_start.elapsed().as_secs_f64() * 1000.0;
        // Record FULL timing for future threshold auto-tuning.
        if let Err(e) = StreamTableMeta::update_adaptive_threshold(
            st.pgt_id,
            st.auto_threshold, // keep current threshold
            Some(full_ms),
        ) {
            pgrx::debug1!("[pg_trickle] Failed to update last_full_ms: {}", e);
        }
        // G4: Flush stale change buffer rows to prevent the next differential
        // tick from re-examining rows already materialized by this FULL refresh.
        if result.is_ok() {
            post_full_refresh_cleanup(st);
        }
        return result;
    }

    // ── D-2: Aggregate saturation check ─────────────────────────────
    // For aggregate stream tables (GROUP BY queries), if the number of
    // changes exceeds the number of groups (= rows in the materialized
    // stream table), all groups are likely affected.  In that case FULL
    // refresh is cheaper than MERGE because it avoids per-row IS DISTINCT
    // FROM checks.  This catches the common "few groups, many changes"
    // pattern that falls below the global ratio threshold.
    //
    // Skip when:
    // - `skip_ratio_check` is true (user explicitly forces DIFFERENTIAL
    //   via the `pg_trickle.refresh_strategy` GUC)
    // - group count is very small (< 10): the comparison is unreliable
    //   because a handful of INSERTs for new groups easily triggers the
    //   threshold without actually saturating existing groups
    if !should_fallback
        && !skip_ratio_check
        && total_change_count > 0
        && effective_defining_query
            .to_ascii_uppercase()
            .contains("GROUP BY")
    {
        // Try pg_class.reltuples first (cheap); fall back to COUNT(*) if
        // the stream table has never been analyzed (reltuples = -1 or 0).
        let st_group_count: i64 = Spi::get_one::<i64>(&format!(
            "SELECT CASE WHEN reltuples >= 1 THEN reltuples::bigint \
                    ELSE (SELECT COUNT(*) FROM \"{}\".\"{}\" ) END \
             FROM pg_class WHERE oid = {}::oid",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\""),
            st.pgt_relid.to_u32(),
        ))
        .unwrap_or(Some(0))
        .unwrap_or(0);

        if st_group_count >= 10 && total_change_count >= st_group_count {
            pgrx::warning!(
                "[pg_trickle] Falling back to FULL refresh for {}.{}: aggregate saturation \
                 — {} changes >= {} groups.\n\
                 When the number of source changes meets or exceeds the number of aggregate \
                 groups, FULL recomputation is faster than per-group differential MERGE. \n\
                 Suggestion: if this happens regularly, the workload may suit FULL refresh \
                 mode. Otherwise, refresh more frequently to keep the per-cycle change count \
                 below the group count.",
                schema,
                name,
                total_change_count,
                st_group_count,
            );
            let t_full_start = Instant::now();
            let result = execute_full_refresh(st);
            let full_ms = t_full_start.elapsed().as_secs_f64() * 1000.0;
            if let Err(e) = StreamTableMeta::update_adaptive_threshold(
                st.pgt_id,
                st.auto_threshold,
                Some(full_ms),
            ) {
                pgrx::debug1!("[pg_trickle] Failed to update last_full_ms: {}", e);
            }
            if result.is_ok() {
                post_full_refresh_cleanup(st);
            }
            return result;
        }
    }

    // ── DI-2: Per-leaf conditional fallback ──────────────────────────
    // When `max_delta_fraction` is configured, check whether any
    // individual source table's delta exceeds the threshold. Those
    // leaves switch from NOT EXISTS (index-based) to EXCEPT ALL
    // (hash-based) in the snapshot construction, while unaffected
    // leaves keep the faster NOT EXISTS path.
    if let Some(max_frac) = st.max_delta_fraction
        && max_frac > 0.0
    {
        let mut fallback = std::collections::HashSet::new();
        for &(oid, change_count, table_size) in &per_source_stats {
            if table_size > 0 && change_count > 0 {
                let frac = change_count as f64 / table_size as f64;
                if frac > max_frac {
                    fallback.insert(oid);
                    pgrx::debug1!(
                        "[pg_trickle] DI-2: per-leaf fallback for source OID {} \
                         ({:.1}% > {:.1}% threshold)",
                        oid,
                        frac * 100.0,
                        max_frac * 100.0,
                    );
                }
            }
        }
        if !fallback.is_empty() {
            set_fallback_leaf_oids(fallback);
        }
    }

    let t_decision = t_decision_start.elapsed();
    let t0 = Instant::now();

    // ── G8.1: Cross-session cache invalidation ──────────────────────
    let shared_gen = crate::shmem::current_cache_generation();
    LOCAL_MERGE_CACHE_GEN.with(|local| {
        if local.get() < shared_gen {
            MERGE_TEMPLATE_CACHE.with(|cache| cache.borrow_mut().clear());
            clear_prepared_merge_statements();
            local.set(shared_gen);
        }
    });

    // ── Try the MERGE template cache first ──────────────────────────
    // PERF-2: Use hash pre-computed at CREATE/ALTER time; fall back only for
    // pre-v0.59.0 rows where the stored hash is still 0.
    // DVM-2: If the effective query was rewritten, hash the rewritten version
    // to avoid serving a cached template built from the original query.
    let query_hash: u64 = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        if std::ptr::eq(
            effective_defining_query.as_ref() as *const str,
            st.defining_query.as_ref() as *const str,
        ) {
            // No rewrite: use pre-computed catalog hash when available.
            if st.defining_query_hash != 0 {
                st.defining_query_hash as u64
            } else {
                st.defining_query.hash(&mut hasher);
                hasher.finish()
            }
        } else {
            // DVM-2 rewrite happened: hash the effective (rewritten) query.
            effective_defining_query.hash(&mut hasher);
            hasher.finish()
        }
    };

    let has_recursive_cte = dvm::query_has_recursive_cte(&effective_defining_query)?;
    // Non-recursive CTEs (WITH … AS (…)) are fully supported by the DVM
    // engine: parse_defining_query_full() builds CteScan nodes and the
    // diff engine processes them via diff_cte_scan().  There is no need for
    // a FULL fallback here.  Recursive CTEs (WITH RECURSIVE) use a
    // semi-naive / DRed strategy and bypass the MERGE template cache (they
    // generate their delta SQL on every refresh instead of caching a
    // template with LSN placeholders).

    let cached = if has_recursive_cte {
        None
    } else {
        MERGE_TEMPLATE_CACHE.with(|cache| {
            let map = cache.borrow();
            map.peek(&st.pgt_id)
                .filter(|entry| entry.defining_query_hash == query_hash)
                .cloned()
        })
    };

    let was_cache_hit = cached.is_some();

    /// Resolved SQL pair: MERGE template, plus D-2 prepared-statement materials
    /// and user-trigger DML.
    struct ResolvedSql {
        merge_sql: String,
        /// Source OIDs (needed for D-2 EXECUTE parameter building).
        source_oids: Vec<u32>,
        /// Parameterized MERGE SQL with `$N` params (for PREPARE).
        parameterized_merge_sql: String,
        /// DELETE template for user-trigger path (no LSN placeholders).
        trigger_delete_sql: String,
        /// UPDATE template for user-trigger path (no LSN placeholders).
        trigger_update_sql: String,
        /// INSERT template for user-trigger path (no LSN placeholders).
        trigger_insert_sql: String,
        /// USING clause with resolved LSN values (for materializing delta
        /// into a temp table in the user-trigger path).
        trigger_using_sql: String,
        /// A1-2: Resolved delta SQL (LSN placeholders replaced with actual
        /// LSN values). Used to compute partition key range for A1-3 predicate
        /// injection on partitioned stream tables.
        resolved_delta_sql: String,
        /// B-1: Whether all aggregates are algebraically invertible.
        is_all_algebraic: bool,
        /// Whether the delta is deduplicated (at most one row per __pgt_row_id).
        is_deduplicated: bool,
    }

    let mut resolved = if let Some(entry) = cached {
        // ── Cache hit: resolve LSN placeholders ──────────────────────
        pgrx::debug1!("[pg_trickle] cache HIT for pgt_id={}", st.pgt_id);
        // Substitute __PGS_PREV/NEW_LSN_{oid}__ tokens with actual values.
        // Each execution gets a fresh plan with accurate LSN selectivity
        // estimates, avoiding the PREPARE/EXECUTE custom-plan penalty.
        let merge_sql = resolve_lsn_placeholders(
            &entry.merge_sql_template,
            &entry.source_oids,
            prev_frontier,
            new_frontier,
            &zero_change_oids,
        )?;
        let trigger_using_sql = resolve_lsn_placeholders(
            &entry.trigger_using_template,
            &entry.source_oids,
            prev_frontier,
            new_frontier,
            &zero_change_oids,
        )?;
        let resolved_delta_sql = resolve_lsn_placeholders(
            &entry.delta_sql_template,
            &entry.source_oids,
            prev_frontier,
            new_frontier,
            &zero_change_oids,
        )?;
        ResolvedSql {
            merge_sql,
            source_oids: entry.source_oids.clone(),
            parameterized_merge_sql: entry.parameterized_merge_sql.as_ref().to_owned(),
            trigger_delete_sql: entry.trigger_delete_template.as_ref().to_owned(),
            trigger_update_sql: entry.trigger_update_template.as_ref().to_owned(),
            trigger_insert_sql: entry.trigger_insert_template.as_ref().to_owned(),
            trigger_using_sql,
            resolved_delta_sql,
            is_all_algebraic: entry.is_all_algebraic,
            is_deduplicated: entry.is_deduplicated,
        }
    } else {
        // ── Cache miss: full pipeline + PREPARE + cache ──────────────
        pgrx::debug1!("[pg_trickle] cache MISS for pgt_id={}", st.pgt_id);
        let delta_result = if has_recursive_cte {
            dvm::generate_delta_query(
                &effective_defining_query,
                prev_frontier,
                new_frontier,
                schema,
                name,
            )?
        } else {
            dvm::generate_delta_query_cached(
                st.pgt_id,
                &effective_defining_query,
                prev_frontier,
                new_frontier,
                schema,
                name,
            )?
        };

        // DI-2: Clear per-leaf fallback OIDs after delta SQL generation.
        clear_fallback_leaf_oids();

        let delta_sql = delta_result.delta_sql;
        let user_cols = delta_result.output_columns;
        let source_oids = delta_result.source_oids;
        let is_dedup = delta_result.is_deduplicated;
        let has_key_changed = delta_result.has_key_changed;

        let quoted_table = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\""),
        );

        let user_col_list = format_col_list(&user_cols);

        // Build cleanup SQL templates — plain DELETE statements (no DO block).
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

        // Build the MERGE template using the raw delta SQL template
        // (with __PGS_PREV_LSN_* / __PGS_NEW_LSN_* placeholder tokens).
        let delta_sql_template = if has_recursive_cte {
            delta_sql.clone()
        } else {
            dvm::get_delta_sql_template(st.pgt_id).unwrap_or(delta_sql.clone())
        };

        // Build template USING clause — skip deduplication when deduplicated (G-M1)
        // EC-06a: For keyless sources, weight-aggregate to cancel within-delta
        // I/D pairs that would otherwise cause phantom rows.
        // B3-2: Use weight aggregation instead of DISTINCT ON for correctness
        // on diamond-flow queries.
        // A-2: Filter D-side value-only UPDATE rows when __pgt_key_changed is available.
        let template_using = if is_dedup && has_key_changed {
            format!(
                "(SELECT * FROM ({delta_sql_template}) __d \
                 WHERE NOT (__d.__pgt_action = 'D' AND __d.__pgt_key_changed = FALSE))"
            )
        } else if is_dedup {
            format!("({delta_sql_template})")
        } else if st.has_keyless_source {
            build_keyless_weight_agg(&delta_sql_template, &user_col_list)
        } else {
            build_weight_agg_using(&delta_sql_template, &user_col_list)
        };

        let merge_template = build_merge_sql(
            &quoted_table,
            &template_using,
            &user_cols,
            st.st_partition_key.is_some(),
        );
        // QF-1: Log at LOG level only when pg_trickle.log_merge_sql = on.
        if crate::config::pg_trickle_log_merge_sql() {
            pgrx::log!("[pg_trickle] MERGE SQL TEMPLATE:\n{}", merge_template);
        }

        // ── B-3: DELETE + INSERT template removed (always use MERGE) ─

        // ── D-2: Build parameterized MERGE SQL for PREPARE ─────────
        let parameterized_merge_sql = parameterize_lsn_template(&merge_template, &source_oids);

        // ── User-trigger explicit DML templates ──────────────────────
        //
        // EC-06: Keyless sources use counted DELETE + plain INSERT.
        // But if is_dedup is true, the ST itself has a unique row ID
        // so we must use standard keyed templates.
        let use_keyless = st.has_keyless_source && !is_dedup;
        let trigger_delete_template =
            build_trigger_delete_sql(&quoted_table, st.pgt_id, use_keyless);

        // EC-06: Use normal UPDATE template for keyless sources — see
        // prewarm_merge_cache comment for full rationale.
        let trigger_update_template =
            build_trigger_update_sql(&quoted_table, st.pgt_id, &user_cols);

        let trigger_insert_template =
            build_trigger_insert_sql(&quoted_table, st.pgt_id, &user_cols, use_keyless);

        // Store templates in the cache for subsequent refreshes.
        if !has_recursive_cte {
            // P-8: Resize LRU cache if needed, then put() (automatically evicts LRU at capacity).
            maybe_evict_lru_cache_entry();
            MERGE_TEMPLATE_CACHE.with(|cache| {
                cache.borrow_mut().put(
                    st.pgt_id,
                    CachedMergeTemplate {
                        defining_query_hash: query_hash,
                        // PERF-3: convert to Arc<str> for O(1) cache-hit clones.
                        merge_sql_template: merge_template.clone().into(),
                        source_oids: source_oids.clone(),
                        cleanup_sql_template: cleanup_template.into(),
                        parameterized_merge_sql: parameterized_merge_sql.clone().into(),
                        trigger_delete_template: trigger_delete_template.clone().into(),
                        trigger_update_template: trigger_update_template.clone().into(),
                        trigger_insert_template: trigger_insert_template.clone().into(),
                        trigger_using_template: template_using.clone().into(),
                        delta_sql_template: delta_sql_template.clone().into(),
                        is_all_algebraic: delta_result.is_all_algebraic,
                        is_deduplicated: delta_result.is_deduplicated,
                    },
                );
            });
        }

        // Resolve LSN placeholders for this execution.
        let merge_sql = resolve_lsn_placeholders(
            &merge_template,
            &source_oids,
            prev_frontier,
            new_frontier,
            &zero_change_oids,
        )?;
        let trigger_using_sql = resolve_lsn_placeholders(
            &template_using,
            &source_oids,
            prev_frontier,
            new_frontier,
            &zero_change_oids,
        )?;
        let resolved_delta_sql = resolve_lsn_placeholders(
            &delta_sql_template,
            &source_oids,
            prev_frontier,
            new_frontier,
            &zero_change_oids,
        )?;
        ResolvedSql {
            merge_sql,
            source_oids: source_oids.clone(),
            parameterized_merge_sql,
            trigger_delete_sql: trigger_delete_template,
            trigger_update_sql: trigger_update_template,
            trigger_insert_sql: trigger_insert_template,
            trigger_using_sql,
            resolved_delta_sql,
            is_all_algebraic: delta_result.is_all_algebraic,
            is_deduplicated: delta_result.is_deduplicated,
        }
    };

    let t1 = Instant::now();

    // PROF-DLT / PGS_PROFILE_DELTA: When the env var is set, capture
    // EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) for the delta query and write
    // the result to /tmp/delta_plans/<schema>_<name>.json.  This env var is
    // intended for E2E test diagnostics and local profiling runs.
    if std::env::var("PGS_PROFILE_DELTA").as_deref() == Ok("1") {
        capture_delta_explain(schema, name, &resolved.resolved_delta_sql);
    }

    // ── Diagnostic: detect OID mismatch between catalog and delta ────
    // If the delta template references source OIDs that are not in the
    // catalog deps, the MERGE will fail referencing nonexistent change
    // buffer tables.
    //
    // For ST-on-ST dependencies, the delta template references the
    // upstream ST's pgt_relid (the storage table OID).  These must be
    // included in the validation set alongside TABLE/FT/MV OIDs.
    let all_dep_oids: Vec<u32> = StDependency::get_for_st(st.pgt_id)
        .unwrap_or_default()
        .into_iter()
        .map(|dep| dep.source_relid.to_u32())
        .collect();
    let delta_oids = &resolved.source_oids;
    let missing_in_delta: Vec<&u32> = delta_oids
        .iter()
        .filter(|oid| !all_dep_oids.contains(oid))
        .collect();
    if !missing_in_delta.is_empty() {
        return Err(PgTrickleError::InternalError(format!(
            "OID MISMATCH (source_oids): delta template references \
             OIDs {missing_in_delta:?} not in catalog deps \
             {all_dep_oids:?}. Delta source_oids={delta_oids:?}, \
             ST={schema}.{name} pgt_id={}",
            st.pgt_id,
        )));
    }

    // ── Diagnostic: scan merge SQL for change buffer table references ─
    // Extract all `changes_NNNNN` references from the SQL to detect
    // references to OIDs not in catalog_source_oids.
    // Note: ST-on-ST queries use `changes_pgt_*` buffers (handled by
    // resolve_delta_template), so `changes_NNNNN` patterns should only
    // reference TABLE/FT/MV sources.
    // CITUS-4: v0.32.0+ stable names look like `changes_7ccce342f79debe9`
    // (16 hex chars). We must skip these — only extract purely decimal tokens
    // as OID references; any token that contains a hex letter (a-f) is a
    // stable hash name, not an OID.
    {
        let mut sql_oids: Vec<u32> = Vec::new();
        let merge_sql_ref = &resolved.merge_sql;
        let mut search_from = 0usize;
        while let Some(pos) = merge_sql_ref[search_from..].find("changes_") {
            let start = search_from + pos + 8; // skip "changes_"
            // Scan the full token (alphanumeric after "changes_")
            let token_end = merge_sql_ref[start..]
                .find(|c: char| !c.is_ascii_alphanumeric())
                .map(|p| start + p)
                .unwrap_or(merge_sql_ref.len());
            let token = &merge_sql_ref[start..token_end];
            // Only treat as an OID reference if the token is purely decimal.
            // Tokens containing hex letters (a-f) are stable hash names.
            if token.chars().all(|c| c.is_ascii_digit())
                && let Ok(oid) = token.parse::<u32>()
                && !sql_oids.contains(&oid)
            {
                sql_oids.push(oid);
            }
            search_from = token_end;
        }
        let missing_in_sql: Vec<&u32> = sql_oids
            .iter()
            .filter(|oid| !all_dep_oids.contains(oid))
            .collect();
        if !missing_in_sql.is_empty() {
            // Dump first 500 chars of merge SQL for diagnosis
            let sql_prefix: String = resolved.merge_sql.chars().take(500).collect();
            return Err(PgTrickleError::InternalError(format!(
                "OID MISMATCH (SQL text): merge SQL references changes_* \
                 for OIDs {missing_in_sql:?} not in catalog deps \
                 {all_dep_oids:?}. SQL OIDs found={sql_oids:?}, \
                 delta source_oids={delta_oids:?}, \
                 ST={schema}.{name} pgt_id={}. \
                 SQL prefix: {sql_prefix}",
                st.pgt_id,
            )));
        }
    }

    // ── P1-2: Log delta SQL when GUC enabled ────────────────────────
    if crate::config::pg_trickle_log_delta_sql() {
        pgrx::debug1!(
            "[pg_trickle] DELTA SQL for {}.{} (pgt_id={}):\n{}",
            schema,
            name,
            st.pgt_id,
            resolved.resolved_delta_sql,
        );
    }

    // ── PERF-5: ANALYZE change buffer tables before delta execution ──
    // Only ANALYZE TABLE/FT/MV source buffers (changes_{oid}).  ST-on-ST
    // sources use changes_pgt_{pgt_id} which is not in catalog_source_oids;
    // using resolved.source_oids would attempt to ANALYZE non-existent
    // tables, causing a PostgreSQL ERROR that aborts the refresh.
    if crate::config::pg_trickle_analyze_before_delta() {
        let cb_schema = crate::config::pg_trickle_change_buffer_schema();
        for &oid in &catalog_source_oids {
            let buf_name = crate::cdc::buffer_base_name_for_oid(pg_sys::Oid::from(oid));
            let analyze_sql = format!(
                "ANALYZE \"{}\".\"{}\"",
                cb_schema.replace('"', "\"\""),
                buf_name,
            );
            if let Err(e) = Spi::run(&analyze_sql) {
                pgrx::debug1!("[pg_trickle] PERF-5: failed to ANALYZE {}: {}", buf_name, e);
            }
        }
    }

    // ── SCAL-2: Change buffer overflow alert ────────────────────────
    let alert_threshold = crate::config::pg_trickle_max_change_buffer_alert_rows();
    if alert_threshold > 0 && total_change_count > alert_threshold {
        pgrx::warning!(
            "[pg_trickle] SCAL-2: change buffer for {}.{} contains {} rows \
             (threshold: {}). Consider increasing refresh frequency or \
             investigating upstream write rate.",
            schema,
            name,
            total_change_count,
            alert_threshold,
        );
    }

    // ── P5-1 / P5-2: Delta-specific planner hints from GUCs ────────
    {
        let delta_wm = crate::config::pg_trickle_delta_work_mem();
        if delta_wm > 0 {
            let work_mem_sql = format!("SET LOCAL work_mem = '{delta_wm}MB'");
            if let Err(e) = Spi::run(&work_mem_sql) {
                pgrx::warning!(
                    "[pg_trickle] P5-1: failed to SET LOCAL work_mem = '{delta_wm}MB': {}. \
                     Falling back to session work_mem.",
                    e
                );
            }
        }
        if !crate::config::pg_trickle_delta_enable_nestloop()
            && let Err(e) = Spi::run("SET LOCAL enable_nestloop = off")
        {
            pgrx::debug1!(
                "[pg_trickle] P5-2: failed to SET LOCAL enable_nestloop = off: {}",
                e
            );
        }
    }

    // ── D-1: Conditional planner hints based on delta size ───────────
    // Large deltas benefit from hash joins over nested loops. Apply
    // SET LOCAL hints that are automatically reset at transaction end.
    // Deep joins (5+ tables) get aggressive hints to avoid pathological
    // plans that spill excessive temp files.
    let work_mem_cap_exceeded = apply_planner_hints(total_change_count, st.pgt_relid, scan_count);

    // ── SCAL-3: Work-mem cap exceeded — fall back to FULL ───────────
    if work_mem_cap_exceeded {
        let t_full_start = Instant::now();
        let result = execute_full_refresh(st);
        let full_ms = t_full_start.elapsed().as_secs_f64() * 1000.0;
        if let Err(e) =
            StreamTableMeta::update_adaptive_threshold(st.pgt_id, st.auto_threshold, Some(full_ms))
        {
            pgrx::debug1!(
                "[pg_trickle] SCAL-3: failed to update adaptive threshold: {}",
                e,
            );
        }
        return result;
    }

    // ── PH-E1: Delta output cardinality estimation ──────────────────
    // Before executing expensive MERGE, run a capped COUNT on the delta
    // subquery. If the output exceeds the budget, fall back to FULL
    // refresh to prevent OOM or excessive temp-file spills.
    let max_estimate = crate::config::pg_trickle_max_delta_estimate_rows();
    if max_estimate > 0
        && let Some(estimate) = estimate_delta_output_rows(&resolved.merge_sql, max_estimate)
        && estimate > max_estimate as i64
    {
        pgrx::notice!(
            "[pg_trickle] PH-E1: Delta output estimate ({} rows) exceeds \
             max_delta_estimate_rows ({}). Falling back to FULL refresh for {}.{}.",
            estimate,
            max_estimate,
            st.pgt_schema,
            st.pgt_name,
        );
        let t_full_start = Instant::now();
        let result = execute_full_refresh(st);
        let full_ms = t_full_start.elapsed().as_secs_f64() * 1000.0;
        if let Err(e) =
            StreamTableMeta::update_adaptive_threshold(st.pgt_id, st.auto_threshold, Some(full_ms))
        {
            pgrx::debug1!(
                "[pg_trickle] PH-E1: failed to update adaptive threshold: {}",
                e,
            );
        }
        return result;
    }

    // ── A-3a: Append-only INSERT fast path ───────────────────────────
    // When the stream table is marked append-only (and hasn't been
    // reverted by the heuristic check above), skip MERGE entirely and
    // use a simple INSERT … SELECT from the delta. This avoids the
    // DELETE, UPDATE, and IS DISTINCT FROM overhead of the MERGE path.
    //
    // Non-monotonic queries are excluded: for operators like LEFT JOIN,
    // anti-join (ALL subqueries), aggregates, etc., source INSERTs can
    // produce delta DELETEs or UPDATEs that the bare INSERT path cannot
    // handle. When detected, clear the incorrectly set catalog flag so
    // subsequent refreshes skip the heuristic check overhead.
    //
    // STs with downstream ST consumers must skip this path: the fast
    // path returns early before capture_delta_to_st_buffer() runs,
    // so downstream STs would never see change buffer rows and their
    // data_timestamp would never advance — breaking ST-on-ST cascades.
    if is_append_only && !has_downstream_st_consumers(st.pgt_id) {
        let non_monotonic = has_non_monotonic_cte(&resolved.merge_sql);
        // Non-deduplicated deltas (joins, aggregates) must NOT use the
        // append-only fast path: even with ON CONFLICT DO NOTHING, the
        // delta can produce rows that collide with existing ST rows from
        // prior cycles. Revert the catalog flag and fall through to the
        // normal PH-D1/MERGE path.
        if !resolved.is_deduplicated {
            pgrx::debug1!(
                "[pg_trickle] A-3a: skipping append-only for {}.{} — \
                 non-deduplicated delta (join/aggregate)",
                schema,
                name,
            );
            let _ = StreamTableMeta::update_append_only(st.pgt_id, false);
        } else if non_monotonic {
            pgrx::debug1!(
                "[pg_trickle] A-3a: skipping append-only for {}.{} — \
                 non-monotonic query operators detected",
                schema,
                name,
            );
            let _ = StreamTableMeta::update_append_only(st.pgt_id, false);
        } else {
            let t_insert_start = Instant::now();

            // Build INSERT SQL from the resolved MERGE SQL's USING clause.
            // The MERGE SQL has the form:
            //   MERGE INTO "schema"."table" AS st USING (...delta...) AS d ON ...
            // We extract the delta subquery and wrap it in INSERT INTO.
            let insert_sql = build_append_only_insert_sql(schema, name, &resolved.merge_sql);

            // A-3a: If user_triggers = 'off' and the ST has user triggers,
            // suppress them around the INSERT (same as the normal MERGE path).
            // The trigger-suppression block runs AFTER the append-only early
            // return in the normal flow, so we must handle it here.
            let ao_triggers_mode = crate::config::pg_trickle_user_triggers_mode();
            let ao_suppress = ao_triggers_mode == crate::config::UserTriggersMode::Off
                && crate::cdc::has_user_triggers(st.pgt_relid).unwrap_or(false);
            let ao_quoted_table = format!(
                "\"{}\".\"{}\"",
                schema.replace('"', "\"\""),
                name.replace('"', "\"\""),
            );
            let ao_disable_sql = format!("ALTER TABLE {} DISABLE TRIGGER USER", ao_quoted_table);
            let ao_enable_sql = format!("ALTER TABLE {} ENABLE TRIGGER USER", ao_quoted_table);
            if ao_suppress && let Err(e) = Spi::run(&ao_disable_sql) {
                pgrx::debug1!(
                    "[pg_trickle] A-3a: failed to disable triggers for {}.{}: {}",
                    schema,
                    name,
                    e
                );
            }

            let rows_inserted = Spi::connect_mut(|client| {
                let result = client
                    .update(&insert_sql, None, &[])
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                Ok::<i64, PgTrickleError>(result.len() as i64)
            })?;

            if ao_suppress && let Err(e) = Spi::run(&ao_enable_sql) {
                pgrx::debug1!(
                    "[pg_trickle] A-3a: failed to re-enable triggers for {}.{}: {}",
                    schema,
                    name,
                    e
                );
            }

            let t_insert = t_insert_start.elapsed();
            pgrx::debug1!(
                "[pg_trickle] append-only INSERT for {}.{}: {} rows in {:.1}ms",
                schema,
                name,
                rows_inserted,
                t_insert.as_secs_f64() * 1000.0,
            );

            // C-1: Defer cleanup of consumed change buffer rows.
            let cleanup_source_oids = resolved.source_oids.clone();
            if !cleanup_source_oids.is_empty() {
                PENDING_CLEANUP.with(|q| {
                    q.borrow_mut().push(PendingCleanup {
                        change_schema: change_schema.clone(),
                        source_oids: cleanup_source_oids,
                    });
                });
            }

            pgrx::info!(
                "[PGS_PROFILE] decision={:.2}ms insert_exec={:.2}ms total={:.2}ms affected={} mode=APPEND_ONLY",
                t_decision.as_secs_f64() * 1000.0,
                t_insert.as_secs_f64() * 1000.0,
                (t_decision + t_insert).as_secs_f64() * 1000.0,
                rows_inserted,
            );

            // G12-ERM-1: Record the effective mode for this execution path.
            set_effective_mode("APPEND_ONLY");

            // G14-MDED: Count this as a differential refresh. The normal
            // record_diff_refresh() call is below the append-only early return,
            // so we must record it here before returning.
            crate::shmem::record_diff_refresh(crate::dvm::is_delta_deduplicated(st.pgt_id));

            return Ok((rows_inserted, 0));
        }
    }

    // ── User-trigger detection ───────────────────────────────────────
    // Determine whether to use the explicit DML path based on the GUC
    // and the presence of user-defined row-level triggers on the ST.
    let user_triggers_mode = crate::config::pg_trickle_user_triggers_mode();
    let use_explicit_dml = match user_triggers_mode {
        crate::config::UserTriggersMode::Off => false,
        crate::config::UserTriggersMode::Auto => crate::cdc::has_user_triggers(st.pgt_relid)?,
    };

    // EC-06: Keyless sources must use explicit DML because MERGE fails
    // when multiple target rows match a single source row (non-unique
    // __pgt_row_id). Force explicit DML path for counted deletion.
    let is_dedup_flag = crate::dvm::is_delta_deduplicated(st.pgt_id);
    let use_explicit_dml = use_explicit_dml || (st.has_keyless_source && !is_dedup_flag);

    // CIT-1: Pre-compute whether this ST lives on a Citus distributed table.
    // Used below to override the MERGE strategy — cross-shard MERGE is blocked.
    let is_distributed_st = st.st_placement == "distributed" && crate::citus::is_citus_loaded();

    // G14-MDED: Record this differential refresh execution in the shared-memory
    // profiling counters.  Called here (after the no-data short-circuit) so we
    // only count refreshes that actually process delta rows.
    crate::shmem::record_diff_refresh(is_dedup_flag);

    // ST-ST-2: Force explicit DML when this ST has downstream ST consumers.
    // The explicit DML path materializes the delta into __pgt_delta_{pgt_id},
    // which we then capture into the ST's change buffer for downstream use.
    let use_explicit_dml = use_explicit_dml || has_downstream_st_consumers(st.pgt_id);

    // When user_triggers = 'off' but there ARE user triggers on the ST,
    // suppress them during the MERGE to prevent spurious firing.
    let suppress_triggers = user_triggers_mode == crate::config::UserTriggersMode::Off
        && crate::cdc::has_user_triggers(st.pgt_relid)?;
    if suppress_triggers {
        let quoted_table = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\""),
        );
        Spi::run(&format!("ALTER TABLE {quoted_table} DISABLE TRIGGER USER")) // nosemgrep: rust.spi.run.dynamic-format — ALTER TABLE DDL cannot be parameterized; quoted_table is a PostgreSQL-quoted identifier
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    // ── B-3: Strategy selection ──────────────────────────────────────
    // PH-D1: Choose between MERGE and DELETE+INSERT based on the
    // merge_strategy GUC and the delta-to-target ratio heuristic.
    let merge_strategy = crate::config::pg_trickle_merge_strategy();
    let use_delete_insert = match merge_strategy {
        crate::config::MergeStrategy::Merge => false,
        crate::config::MergeStrategy::Auto => {
            // Heuristic: use DELETE+INSERT when delta is a small fraction
            // of the target table. Estimate target rows from pg_class.
            let threshold = crate::config::pg_trickle_merge_strategy_threshold();
            if total_change_count > 0 && threshold > 0.0 {
                let target_rows: i64 = Spi::get_one::<i64>(&format!(
                    "SELECT CASE WHEN reltuples >= 1 THEN reltuples::bigint \
                            ELSE (SELECT COUNT(*) FROM \"{}\".\"{}\" ) END \
                     FROM pg_class WHERE oid = {}::oid",
                    schema.replace('"', "\"\""),
                    name.replace('"', "\"\""),
                    st.pgt_relid.to_u32(),
                ))
                .unwrap_or(Some(0))
                .unwrap_or(0);
                if target_rows > 0 {
                    let ratio = total_change_count as f64 / target_rows as f64;
                    let chosen = ratio < threshold;
                    if chosen {
                        pgrx::debug1!(
                            "[pg_trickle] PH-D1: auto chose DELETE+INSERT for {}.{}: \
                             ratio={:.4} < threshold={:.4} ({} changes / {} target rows)",
                            schema,
                            name,
                            ratio,
                            threshold,
                            total_change_count,
                            target_rows,
                        );
                    }
                    chosen
                } else {
                    false
                }
            } else {
                false
            }
        }
    };
    // DELETE+INSERT is incompatible with the explicit DML path (user triggers,
    // keyless sources, downstream ST consumers) — those already use their own
    // decomposed DML.  Also skip for partitioned STs (hash-merge path).
    let use_delete_insert = use_delete_insert && !use_explicit_dml && st.st_partition_key.is_none();

    // PH-D1-JOIN: For non-deduplicated deltas (joins), always use PH-D1
    // with ON CONFLICT instead of MERGE.  Join deltas can produce phantom
    // rows (Part 1 and Part 2 compute different __pgt_row_id hashes for
    // the same logical row) that accumulate in the stream table.  While
    // weight aggregation + DISTINCT ON deduplicates per-refresh, phantom
    // rows from prior refreshes can still trigger UNIQUE_VIOLATION in the
    // MERGE INSERT clause (which lacks ON CONFLICT protection).  PH-D1's
    // INSERT uses ON CONFLICT (__pgt_row_id) DO UPDATE SET which safely
    // handles these collisions.
    let use_delete_insert = if !resolved.is_deduplicated
        && !st.has_keyless_source
        && !use_explicit_dml
        && st.st_partition_key.is_none()
    {
        true
    } else {
        use_delete_insert
    };

    // CIT-1: Citus distributed stream tables cannot use cross-shard MERGE.
    // A coordinator-local delta temp table is not co-located with the
    // distributed target table, so Citus rejects the MERGE statement.
    // Force the PH-D1 DELETE+INSERT path: Citus routes each INSERT row to
    // the correct shard, and DELETE … WHERE row_id IN (…) is pushed down
    // to the relevant workers.  This override runs AFTER PH-D1-JOIN so
    // that is_dedup / append-only guards are already resolved.
    let use_delete_insert =
        if is_distributed_st && !use_explicit_dml && st.st_partition_key.is_none() {
            pgrx::debug1!(
                "[pg_trickle] CIT-1: forcing PH-D1 DELETE+INSERT for distributed ST {}.{} \
             (Citus blocks cross-shard MERGE)",
                schema,
                name,
            );
            true
        } else {
            use_delete_insert
        };

    // QW-9 (v0.81.0): Chunked MERGE — when the delta exceeds
    // `pg_trickle.merge_batch_size` rows and the MERGE strategy has not
    // already been forced to PH-D1, route the refresh through the
    // DELETE+INSERT path.  The PH-D1 path materialises the full delta into
    // a single temp table and applies it via bulk DELETE+INSERT, which is
    // better suited than a MERGE join for very large deltas.
    //
    // Set `pg_trickle.merge_batch_size = 0` to disable (default behaviour).
    let use_delete_insert = {
        let batch_size = crate::config::pg_trickle_merge_batch_size();
        if !use_delete_insert
            && !use_explicit_dml
            && st.st_partition_key.is_none()
            && batch_size > 0
            && total_change_count > batch_size as i64
        {
            pgrx::debug1!(
                "[pg_trickle] QW-9: chunked MERGE — delta {} rows > batch_size {} for \
                 {}.{}; routing through PH-D1 DELETE+INSERT",
                total_change_count,
                batch_size,
                schema,
                name,
            );
            true
        } else {
            use_delete_insert
        }
    };

    // When the GUC is on and ALL aggregates are algebraically invertible
    // (COUNT, SUM, AVG, etc.), use explicit DML (DELETE+UPDATE+INSERT)
    // instead of MERGE. The explicit DML path does targeted row-level
    // operations via a materialized temp table, avoiding the hash-join
    // cost of MERGE which dominates for aggregate queries with many groups.
    let use_agg_fast_path = resolved.is_all_algebraic
        && crate::config::pg_trickle_aggregate_fast_path()
        && !st.has_keyless_source
        && !use_explicit_dml
        && !use_delete_insert
        && st.st_partition_key.is_none();
    if use_agg_fast_path {
        pgrx::debug1!(
            "[pg_trickle] B-1: aggregate fast-path enabled for {}.{} \
             (all aggregates algebraically invertible)",
            schema,
            name,
        );
    }

    // ── A1-2/A1-3: Partition-key range predicate injection ───────────
    // For partitioned stream tables, compute the MIN/MAX of the partition
    // key across the current delta and inject it as a literal range
    // predicate into the MERGE ON clause. This enables PostgreSQL partition
    // pruning: only partitions overlapping [min, max] are visited, reducing
    // MERGE I/O proportionally to the number of affected partitions.
    //
    // A1-3b: HASH partitions use a per-partition MERGE loop instead of
    // predicate injection (hash functions are not range-invertible).
    //
    // If the delta is empty (all changes cancel out), return early —
    // there is nothing to MERGE.
    let hash_merge_result: Option<(usize, &str)> = if let Some(ref pk) = st.st_partition_key {
        let method = crate::api::parse_partition_method(pk);
        if method == crate::api::PartitionMethod::Hash {
            // A1-3b: Per-partition MERGE for HASH partitioned STs.
            let count = execute_hash_partitioned_merge(
                &resolved.merge_sql,
                &resolved.resolved_delta_sql,
                schema,
                name,
                st.pgt_relid,
                pk,
                st.pgt_id,
            )?;
            Some((count, "hash_merge"))
        } else {
            // RANGE / LIST: extract bounds and inject predicate.
            match extract_partition_bounds(&resolved.resolved_delta_sql, pk)? {
                None => {
                    // Delta produced no rows for the partition key — fast path.
                    pgrx::debug1!(
                        "[pg_trickle] A1-3: empty partition-key delta for {}.{}, skipping MERGE",
                        schema,
                        name,
                    );
                    return Ok((0, 0));
                }
                Some(bounds) => {
                    pgrx::debug1!(
                        "[pg_trickle] A1-3: partition bounds for {}.{}: {:?}",
                        schema,
                        name,
                        match &bounds {
                            PartitionBounds::Range { mins, maxs } =>
                                format!("RANGE [{mins:?}, {maxs:?}]"),
                            PartitionBounds::List(vals) => format!("LIST {:?}", vals),
                        },
                    );
                    resolved.merge_sql =
                        inject_partition_predicate(&resolved.merge_sql, pk, &bounds);
                    None
                }
            }
        }
    } else {
        None
    };

    // ── D-2: Prepared-statement flag ─────────────────────────────────
    // PB2: Disable prepared statements when pooler_compatibility_mode is on.
    // A1-3: Disable for partitioned STs — the partition predicate is a
    // literal range that changes every refresh; a generic plan cannot prune
    // partitions from parameter values.
    // ST-ST-5: Disable prepared statements when the parameterized SQL
    // contains pgt_-prefixed LSN placeholders (ST sources). The
    // parameterize function only handles numeric OID placeholders; pgt_
    // placeholders remain as literals and fail to parse as pg_lsn.
    let has_pgt_placeholders = resolved
        .parameterized_merge_sql
        .contains("__PGS_PREV_LSN_pgt_")
        || resolved
            .parameterized_merge_sql
            .contains("__PGS_NEW_LSN_pgt_");
    // PB2/STAB-1: Disable prepared statements when pooler compat mode is on.
    let use_prepared = crate::config::pg_trickle_use_prepared_statements()
        && was_cache_hit
        && !crate::config::effective_pooler_compat(st.pooler_compatibility_mode)
        && st.st_partition_key.is_none()
        && !has_pgt_placeholders;

    let (merge_count, strategy_label) = if let Some(result) = hash_merge_result {
        // A1-3b: HASH per-partition MERGE already executed above.
        result
    } else if use_delete_insert {
        // ── PH-D1: DELETE+INSERT path ───────────────────────────────
        // For small deltas against large tables, separate DELETE + INSERT
        // avoids the MERGE join cost. The delta is materialized into a
        // temp table, then applied as two targeted statements.
        let t_mat_start = Instant::now();

        // Drop any stale delta table from a prior refresh in the same
        // transaction (e.g. two refresh_stream_table calls in one batch_execute).
        // ON COMMIT DROP normally handles cleanup, but that only fires at
        // transaction commit, so subsequent calls within the same transaction
        // would otherwise see "relation already exists".
        let _ = Spi::run(&format!("DROP TABLE IF EXISTS __pgt_delta_{}", st.pgt_id)); // nosemgrep: rust.spi.run.dynamic-format — st.pgt_id is a plain i64, not user-supplied input.
        let materialize_sql = format!(
            "CREATE TEMP TABLE __pgt_delta_{pgt_id} ON COMMIT DROP AS \
             SELECT * FROM {using_clause} AS d",
            pgt_id = st.pgt_id,
            using_clause = resolved.trigger_using_sql,
        );
        Spi::run(&materialize_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let t_mat = t_mat_start.elapsed();

        // Step 1: DELETE rows touched by the delta.
        //
        // For keyed sources, delete ALL rows whose __pgt_row_id appears
        // anywhere in the delta (both 'D' and 'I' actions).  This is
        // critical for join deltas: an UPDATE on a source row produces
        // both a 'D' (old values) and an 'I' (new values) with the same
        // __pgt_row_id. Deleting all matching row_ids up-front guarantees
        // the subsequent INSERT never hits a UNIQUE_VIOLATION, eliminating
        // the fragile ON CONFLICT + pre-delete approach.
        //
        // For keyless sources, only delete rows matching 'D' actions
        // (the __pgt_row_id index is non-unique, so no conflict risk).
        let t_del_start = Instant::now();
        let del_count = if !st.has_keyless_source {
            let quoted_table = format!(
                "\"{}\".\"{}\"",
                schema.replace('"', "\"\""),
                name.replace('"', "\"\""),
            );
            let unified_del_sql = format!(
                "DELETE FROM {quoted_table} WHERE __pgt_row_id IN (\
                     SELECT __pgt_row_id FROM __pgt_delta_{pgt_id}\
                 )",
                pgt_id = st.pgt_id,
            );
            Spi::connect_mut(|client| {
                let result = client
                    .update(&unified_del_sql, None, &[])
                    .map_err(|e| PgTrickleError::SpiError(format!("[PH-D1-DELETE] {}", e)))?;
                Ok::<usize, PgTrickleError>(result.len())
            })?
        } else {
            Spi::connect_mut(|client| {
                let result = client
                    .update(&resolved.trigger_delete_sql, None, &[])
                    .map_err(|e| PgTrickleError::SpiError(format!("[PH-D1-DELETE] {}", e)))?;
                Ok::<usize, PgTrickleError>(result.len())
            })?
        };
        let t_del = t_del_start.elapsed();

        // Step 2: UPDATE existing rows where values changed.
        // For keyed sources, the unified DELETE in step 1 already removed
        // all rows matching delta row_ids (both 'D' and 'I'), so the
        // UPDATE step is unnecessary — the INSERT will re-create them
        // with updated values.
        let t_upd_start = Instant::now();
        let upd_count = if st.has_keyless_source {
            // Keyless sources still need the UPDATE step
            Spi::connect_mut(|client| {
                let result = client
                    .update(&resolved.trigger_update_sql, None, &[])
                    .map_err(|e| PgTrickleError::SpiError(format!("[PH-D1-UPDATE] {}", e)))?;
                Ok::<usize, PgTrickleError>(result.len())
            })?
        } else {
            // Keyed sources: skip UPDATE, unified DELETE + INSERT handles it
            0
        };
        let t_upd = t_upd_start.elapsed();

        // Step 3: INSERT genuinely new rows.
        // For keyed sources, all conflicting row_ids were removed in step 1,
        // so no ON CONFLICT clause is needed. DISTINCT ON in the INSERT SQL
        // (from build_trigger_insert_sql) handles within-delta duplicates.
        let t_ins_start = Instant::now();
        let ins_count = Spi::connect_mut(|client| {
            let result = client
                .update(&resolved.trigger_insert_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(format!("[PH-D1-INSERT] {}", e)))?;
            Ok::<usize, PgTrickleError>(result.len())
        })?;
        let t_ins = t_ins_start.elapsed();

        pgrx::info!(
            "[PGS_PROFILE] delete_insert: materialize={:.2}ms delete={:.2}ms({}) \
             update={:.2}ms({}) insert={:.2}ms({}) for {}.{}",
            t_mat.as_secs_f64() * 1000.0,
            t_del.as_secs_f64() * 1000.0,
            del_count,
            t_upd.as_secs_f64() * 1000.0,
            upd_count,
            t_ins.as_secs_f64() * 1000.0,
            ins_count,
            schema,
            name,
        );

        (del_count + upd_count + ins_count, "delete_insert")
    } else if use_agg_fast_path {
        // ── B-1: Aggregate fast-path ────────────────────────────────
        // For all-algebraic aggregate queries, use explicit DML
        // (DELETE+UPDATE+INSERT) to avoid the MERGE hash-join cost.
        let t_mat_start = Instant::now();

        // Drop any stale delta table from a prior refresh in the same
        // transaction (same-transaction multiple refresh edge case).
        let _ = Spi::run(&format!("DROP TABLE IF EXISTS __pgt_delta_{}", st.pgt_id)); // nosemgrep: rust.spi.run.dynamic-format — st.pgt_id is a plain i64, not user-supplied input.
        let materialize_sql = format!(
            "CREATE TEMP TABLE __pgt_delta_{pgt_id} ON COMMIT DROP AS \
             SELECT * FROM {using_clause} AS d",
            pgt_id = st.pgt_id,
            using_clause = resolved.trigger_using_sql,
        );
        Spi::run(&materialize_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let t_mat = t_mat_start.elapsed();

        // Step 1: DELETE rows marked for removal
        let t_del_start = Instant::now();
        let del_count = Spi::connect_mut(|client| {
            let result = client
                .update(&resolved.trigger_delete_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<usize, PgTrickleError>(result.len())
        })?;
        let t_del = t_del_start.elapsed();

        // Step 2: UPDATE existing rows where values changed
        let t_upd_start = Instant::now();
        let upd_count = Spi::connect_mut(|client| {
            let result = client
                .update(&resolved.trigger_update_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<usize, PgTrickleError>(result.len())
        })?;
        let t_upd = t_upd_start.elapsed();

        // Step 3: INSERT genuinely new rows
        let t_ins_start = Instant::now();
        let ins_count = Spi::connect_mut(|client| {
            let result = client
                .update(&resolved.trigger_insert_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<usize, PgTrickleError>(result.len())
        })?;
        let t_ins = t_ins_start.elapsed();

        pgrx::info!(
            "[PGS_PROFILE] agg_fast_path: materialize={:.2}ms delete={:.2}ms({}) \
             update={:.2}ms({}) insert={:.2}ms({}) for {}.{}",
            t_mat.as_secs_f64() * 1000.0,
            t_del.as_secs_f64() * 1000.0,
            del_count,
            t_upd.as_secs_f64() * 1000.0,
            upd_count,
            t_ins.as_secs_f64() * 1000.0,
            ins_count,
            schema,
            name,
        );

        (del_count + upd_count + ins_count, "agg_fast_path")
    } else if use_explicit_dml {
        // ── User-trigger path: explicit DML ─────────────────────────
        // Decompose the MERGE into DELETE + UPDATE + INSERT so that
        // user-defined triggers fire with correct TG_OP / OLD / NEW.

        // Step 1: Materialize delta into a temp table (ON COMMIT DROP).
        // This avoids evaluating the delta query three times.
        let t_mat_start = Instant::now();

        // Drop any stale delta table from a prior refresh in the same
        // transaction (same-transaction multiple refresh edge case).
        let _ = Spi::run(&format!("DROP TABLE IF EXISTS __pgt_delta_{}", st.pgt_id)); // nosemgrep: rust.spi.run.dynamic-format — st.pgt_id is a plain i64, not user-supplied input.
        let materialize_sql = format!(
            "CREATE TEMP TABLE __pgt_delta_{pgt_id} ON COMMIT DROP AS \
             SELECT * FROM {using_clause} AS d",
            pgt_id = st.pgt_id,
            using_clause = resolved.trigger_using_sql,
        );
        Spi::run(&materialize_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let t_mat = t_mat_start.elapsed();

        // ST-ST-10: When this ST has downstream ST consumers, snapshot
        // the affected rows BEFORE applying DML.  The weight-aggregation
        // wrapper collapses D+I pairs for the same __pgt_row_id into a
        // single I action, which is correct for the MERGE but loses the
        // DELETE for old column values.  The pre-snapshot lets us compute
        // the true effective delta (including value-change DELETEs) after
        // the DML completes.
        let needs_diff_capture = has_downstream_st_consumers(st.pgt_id);
        let diff_capture_cols = if needs_diff_capture {
            let cols = get_st_user_columns(st);
            let col_list: String = cols
                .iter()
                .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ");

            let qt = format!(
                "\"{}\".\"{}\"",
                schema.replace('"', "\"\""),
                name.replace('"', "\"\""),
            );

            let _ = Spi::run(&format!("DROP TABLE IF EXISTS __pgt_pre_{}", st.pgt_id)); // nosemgrep: rust.spi.run.dynamic-format — st.pgt_id is a plain i64, not user-supplied input.

            let snapshot_sql = format!(
                "CREATE TEMP TABLE __pgt_pre_{pgt_id} ON COMMIT DROP AS \
                 SELECT __pgt_row_id, {col_list} FROM {qt} \
                 WHERE __pgt_row_id IN (\
                   SELECT __pgt_row_id FROM __pgt_delta_{pgt_id}\
                 )",
                pgt_id = st.pgt_id,
            );
            if let Err(e) = Spi::run(&snapshot_sql) {
                pgrx::warning!(
                    "[pg_trickle] ST-ST-10: pre-snapshot failed for {}.{}: {} — \
                     falling back to delta-based capture",
                    schema,
                    name,
                    e,
                );
                Vec::new()
            } else {
                cols
            }
        } else {
            Vec::new()
        };

        // Step 2: DELETE removed rows (AFTER DELETE triggers fire)
        let t_del_start = Instant::now();
        let del_count = Spi::connect_mut(|client| {
            let result = client
                .update(&resolved.trigger_delete_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<usize, PgTrickleError>(result.len())
        })?;
        let t_del = t_del_start.elapsed();

        // Step 3: UPDATE changed existing rows (AFTER UPDATE triggers fire)
        // The IS DISTINCT FROM guard (B-1) prevents no-op UPDATE triggers.
        let t_upd_start = Instant::now();
        let upd_count = Spi::connect_mut(|client| {
            let result = client
                .update(&resolved.trigger_update_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<usize, PgTrickleError>(result.len())
        })?;
        let t_upd = t_upd_start.elapsed();

        // Step 4: INSERT genuinely new rows (AFTER INSERT triggers fire)
        let t_ins_start = Instant::now();
        let ins_count = Spi::connect_mut(|client| {
            let result = client
                .update(&resolved.trigger_insert_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            Ok::<usize, PgTrickleError>(result.len())
        })?;
        let t_ins = t_ins_start.elapsed();

        pgrx::info!(
            "[PGS_PROFILE] explicit_dml: materialize={:.2}ms delete={:.2}ms({}) update={:.2}ms({}) insert={:.2}ms({}) for {}.{}",
            t_mat.as_secs_f64() * 1000.0,
            t_del.as_secs_f64() * 1000.0,
            del_count,
            t_upd.as_secs_f64() * 1000.0,
            upd_count,
            t_ins.as_secs_f64() * 1000.0,
            ins_count,
            schema,
            name,
        );

        // ST-ST-2/ST-ST-10: Capture effective delta to change buffer for
        // downstream ST consumers.  When a pre-snapshot was taken
        // (diff_capture_cols is non-empty), use the pre/post comparison
        // to produce accurate I/D pairs that include value-change DELETEs.
        // Otherwise, fall back to the delta-based capture.
        if needs_diff_capture {
            let mut diff_capture_ok = true;
            if !diff_capture_cols.is_empty() {
                if let Err(e) = capture_incremental_diff_to_st_buffer(st, &diff_capture_cols) {
                    pgrx::warning!(
                        "[pg_trickle] ST-ST-10: incremental diff capture failed for {}.{}: {} \
                         — marking downstream STs for reinit",
                        schema,
                        name,
                        e,
                    );
                    diff_capture_ok = false;
                }
            } else {
                let user_cols = get_st_user_columns(st);
                if let Err(e) = capture_delta_to_st_buffer(st, &user_cols) {
                    pgrx::warning!(
                        "[pg_trickle] ST-ST: delta capture failed for {}.{}: {} \
                         — marking downstream STs for reinit",
                        schema,
                        name,
                        e,
                    );
                    diff_capture_ok = false;
                }
            }
            if !diff_capture_ok
                && let Ok(downstream_ids) =
                    crate::catalog::StDependency::get_downstream_pgt_ids(st.pgt_relid)
            {
                for ds_id in &downstream_ids {
                    if let Err(e2) = StreamTableMeta::mark_for_reinitialize(*ds_id) {
                        pgrx::warning!(
                            "[pg_trickle] ST-ST: failed to mark downstream ST {} for reinit: {}",
                            ds_id,
                            e2,
                        );
                    }
                }
            }
        }

        (del_count + upd_count + ins_count, "explicit_dml")
    } else if use_prepared {
        // ── D-2: MERGE via prepared statement ────────────────────────
        // After ~5 executions PostgreSQL switches from custom to generic
        // plan, saving ~1-2ms of parse/plan overhead per refresh cycle.
        let stmt_name = format!("__pgt_merge_{}", st.pgt_id);

        let already_prepared = PREPARED_MERGE_STMTS.with(|s| s.borrow().contains(&st.pgt_id));

        if !already_prepared {
            let type_list = build_prepare_type_list(resolved.source_oids.len());
            // DEALLOCATE in case a stale statement exists from a prior
            // session within this same backend.
            // Note: DEALLOCATE does not support IF EXISTS in PostgreSQL.
            // Check pg_prepared_statements first to avoid an error.
            // nosemgrep: rust.spi.query.dynamic-format — stmt_name is derived from numeric pgt_id
            let stale_exists = Spi::get_one::<bool>(&format!(
                "SELECT EXISTS(SELECT 1 FROM pg_prepared_statements WHERE name = '{stmt_name}')"
            ))
            .unwrap_or(Some(false))
            .unwrap_or(false);
            if stale_exists {
                let _ = Spi::run(&format!("DEALLOCATE {stmt_name}")); // nosemgrep: rust.spi.run.dynamic-format — stmt_name is derived from numeric pgt_id, not user input
            }
            // nosemgrep: rust.spi.run.dynamic-format — stmt_name and type_list are derived from numeric IDs; parameterized_merge_sql is an internal template
            Spi::run(&format!(
                "PREPARE {stmt_name} ({type_list}) AS {}",
                resolved.parameterized_merge_sql
            ))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

            PREPARED_MERGE_STMTS.with(|s| {
                s.borrow_mut().insert(st.pgt_id);
            });
        }

        let params = build_execute_params(&resolved.source_oids, prev_frontier, new_frontier);
        let execute_sql = format!("EXECUTE {stmt_name}({params})");

        let n = Spi::connect_mut(|client| {
            let result = client
                .update(&execute_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(format!("[MERGE-PREPARED] {}", e)))?;
            Ok::<usize, PgTrickleError>(result.len())
        })?;
        (n, "merge_prepared")
    } else {
        // ── MERGE path (default for small deltas) ───────────────────
        let n = Spi::connect_mut(|client| {
            let result = client
                .update(&resolved.merge_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(format!("[MERGE] {}", e)))?;
            Ok::<usize, PgTrickleError>(result.len())
        })?;
        (n, "merge")
    };

    // EC-01 / v0.38.0: non-deduplicated join deltas can leave stale row_ids
    // from earlier refresh cycles that are not present in the current delta.
    // After every such differential apply, reconcile the stream table against
    // the full-query row_id set. Only for join-bearing queries where cross-cycle
    // phantom rows can materialize from partial delta application.
    //
    // EC-01b: use the parsed OpTree for join detection instead of keyword
    // matching so comma-joins (`FROM a, b`) and joins inside subqueries are
    // also covered. Falls back to `true` (run cleanup, fail-safe) if the
    // query cannot be parsed.
    let query_has_join = dvm::query_has_join(&effective_defining_query).unwrap_or(true);
    // Skip full-query reconciliation for recursive CTEs — it bypasses the
    // ivm_recursive_max_depth guard and would insert suppressed rows.
    let query_has_recursive_cte =
        dvm::query_has_recursive_cte(&effective_defining_query).unwrap_or(false);
    let phantom_cleanup_count = if query_has_join
        && !query_has_recursive_cte
        && !resolved.is_deduplicated
        && !st.has_keyless_source
        && st.st_partition_key.is_none()
    {
        let quoted_table = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\""),
        );
        super::phd1::cleanup_cross_cycle_phantoms(
            st.pgt_id,
            &quoted_table,
            &effective_defining_query,
            10_000,
        )?
    } else {
        0
    };

    if phantom_cleanup_count > 0
        && has_downstream_st_consumers(st.pgt_id)
        && let Ok(downstream_ids) =
            crate::catalog::StDependency::get_downstream_pgt_ids(st.pgt_relid)
    {
        for ds_id in &downstream_ids {
            if let Err(e) = StreamTableMeta::mark_for_reinitialize(*ds_id) {
                pgrx::warning!(
                    "[pg_trickle] EC01-2: failed to mark downstream ST {} for reinit after phantom cleanup: {}",
                    ds_id,
                    e,
                );
            }
        }
    }

    // Re-enable user triggers if they were suppressed (GUC = 'off').
    if suppress_triggers {
        let quoted_table = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\""),
        );
        Spi::run(&format!("ALTER TABLE {quoted_table} ENABLE TRIGGER USER")) // nosemgrep: rust.spi.run.dynamic-format — ALTER TABLE DDL cannot be parameterized; quoted_table is a PostgreSQL-quoted identifier
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }

    let t2 = Instant::now();

    // ── C-1: Defer cleanup to next refresh cycle ────────────────────
    // Instead of deleting consumed change-buffer rows synchronously
    // (which costs 0.5–7ms), we enqueue the cleanup work and execute it
    // at the start of the NEXT refresh.  The cleanup uses the min-frontier
    // approach to safely handle shared change buffers across multiple STs.
    let cleanup_source_oids = MERGE_TEMPLATE_CACHE.with(|cache| {
        let map = cache.borrow();
        map.peek(&st.pgt_id)
            .map(|entry| entry.source_oids.clone())
            .unwrap_or_default()
    });

    if !cleanup_source_oids.is_empty() {
        PENDING_CLEANUP.with(|q| {
            q.borrow_mut().push(PendingCleanup {
                change_schema: change_schema.clone(),
                source_oids: cleanup_source_oids,
            });
        });
    }

    let t3 = Instant::now();

    // PH-E2: Query pg_stat_statements for temp file spill metrics.
    // Store in thread-local for the scheduler to read after this function returns.
    let spill_threshold = crate::config::pg_trickle_spill_threshold_blocks();
    if spill_threshold > 0 {
        let temp_blks = crate::monitor::query_temp_file_usage(name)
            .map(|(_read, written)| written)
            .unwrap_or(0);
        set_last_temp_blks_written(temp_blks);
    } else {
        set_last_temp_blks_written(-1);
    }

    // Determine cache path and hint tier for profiling
    let cache_path = if was_cache_hit {
        "cache_hit"
    } else {
        "cache_miss"
    };

    let hint_tier = if !crate::config::pg_trickle_merge_planner_hints() {
        "off"
    } else if total_change_count >= PLANNER_HINT_WORKMEM_THRESHOLD {
        "nestloop+workmem"
    } else if total_change_count >= PLANNER_HINT_NESTLOOP_THRESHOLD {
        "nestloop"
    } else {
        "none"
    };

    // Emit timing breakdown for profiling
    pgrx::info!(
        "[PGS_PROFILE] decision={:.2}ms generate+build={:.2}ms merge_exec={:.2}ms cleanup_enqueue={:.2}ms total={:.2}ms affected={} phantom_cleanup={} delta_est={} mode=INCR path={} hints={} strategy={}",
        t_decision.as_secs_f64() * 1000.0,
        t1.duration_since(t0).as_secs_f64() * 1000.0,
        t2.duration_since(t1).as_secs_f64() * 1000.0,
        t3.duration_since(t2).as_secs_f64() * 1000.0,
        (t_decision.as_secs_f64() + t3.duration_since(t0).as_secs_f64()) * 1000.0,
        merge_count,
        phantom_cleanup_count,
        total_change_count,
        cache_path,
        hint_tier,
        strategy_label,
    );

    // PERF-2 (v0.31.0): Plan-aware delta routing log.
    // When adaptive_merge_strategy = true, log a recommendation if the
    // alternative strategy would have been a better fit based on the
    // current delta/target ratio.
    if crate::config::pg_trickle_adaptive_merge_strategy() {
        let threshold = crate::config::pg_trickle_merge_strategy_threshold();
        let target_rows: i64 = Spi::get_one::<i64>(&format!(
            "SELECT CASE WHEN reltuples >= 1 THEN reltuples::bigint \
                    ELSE (SELECT COUNT(*) FROM \"{}\".\"{}\" ) END \
             FROM pg_class WHERE oid = {}::oid",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\""),
            st.pgt_relid.to_u32(),
        ))
        .unwrap_or(Some(0))
        .unwrap_or(0);
        if target_rows > 0 && total_change_count > 0 {
            let ratio = total_change_count as f64 / target_rows as f64;
            let suggested = if ratio < threshold {
                "delete_insert"
            } else {
                "merge"
            };
            if suggested != strategy_label {
                pgrx::debug1!(
                    "[pg_trickle] PERF-2: {}.{} used strategy='{}' but ratio={:.4} suggests \
                     '{}' ({} changes / {} target rows, threshold={:.4}) — consider \
                     setting pg_trickle.merge_strategy='{}' for this table",
                    schema,
                    name,
                    strategy_label,
                    ratio,
                    suggested,
                    total_change_count,
                    target_rows,
                    threshold,
                    suggested,
                );
            }
        }
    }

    // ── Session 7: Adaptive threshold auto-tuning ───────────────────
    // Compare INCR total time against the last known FULL time. If INCR
    // is approaching or exceeding FULL, lower the threshold so future
    // refreshes at this change rate fall back to FULL sooner. If INCR
    // is significantly faster, raise the threshold to allow more
    // differential refreshes.
    let incr_total_ms = (t_decision.as_secs_f64() + t3.duration_since(t0).as_secs_f64()) * 1000.0;

    // ── UX-1: DIFF-slower-than-FULL per-query log warning ───────────
    if crate::config::pg_trickle_log_delta_sql()
        && let Some(last_full) = st.last_full_ms
        && last_full > 0.0
        && incr_total_ms > last_full
    {
        let ratio = incr_total_ms / last_full;
        pgrx::warning!(
            "[pg_trickle] DIFF refresh for {}.{} took {:.1}ms vs last FULL {:.1}ms \
             — DIFF is {:.1}x slower",
            schema,
            name,
            incr_total_ms,
            last_full,
            ratio,
        );
    }

    if let Some(last_full) = st.last_full_ms
        && last_full > 0.0
    {
        let current_threshold = st.auto_threshold.unwrap_or(global_ratio);
        let ratio_threshold =
            compute_adaptive_threshold(current_threshold, incr_total_ms, last_full);

        // ── D-3 / B-4: Cost-based threshold from historical data ────
        // Blend the ratio-based threshold with a cost-model estimate
        // derived from recent refresh history.  The cost model computes
        // the crossover delta ratio where INCR cost equals FULL cost,
        // adjusted for query complexity class.
        let complexity = classify_query_complexity(&effective_defining_query);
        let new_threshold = match estimate_cost_based_threshold(st.pgt_id, complexity) {
            Some(cost_threshold) => {
                // Weighted blend: 60% ratio-based, 40% cost-based.
                let blended = ratio_threshold * 0.6 + cost_threshold * 0.4;
                blended.clamp(0.01, 0.80)
            }
            None => ratio_threshold,
        };

        if (new_threshold - current_threshold).abs() > 0.001 {
            pgrx::debug1!(
                "[pg_trickle] Adaptive threshold: INCR={:.1}ms vs FULL={:.1}ms (ratio={:.2}), threshold {:.3} → {:.3}",
                incr_total_ms,
                last_full,
                incr_total_ms / last_full,
                current_threshold,
                new_threshold,
            );
        }
        if let Err(e) =
            StreamTableMeta::update_adaptive_threshold(st.pgt_id, Some(new_threshold), None)
        {
            pgrx::debug1!("[pg_trickle] Failed to update adaptive threshold: {}", e);
        }
    }

    // Guarantee a non-zero count when the change buffer actually had entries.
    // PostgreSQL MERGE may report 0 processed rows via SPI even when it
    // modifies data (observed with pgrx 0.17 / PostgreSQL 18).
    // Since we already verified `any_changes=true` above, the MERGE must
    // have processed at least one change buffer entry.
    let effective_count = (merge_count as i64).max(1);

    // ── DAG-3: Delta amplification detection ────────────────────────
    // After the MERGE completes, check whether the output delta is
    // disproportionately larger than the input delta (common with
    // many-to-many joins or high fan-out).  Emit a WARNING so operators
    // can identify and tune the problematic hop.
    let amplification_threshold = crate::config::pg_trickle_delta_amplification_threshold();
    if should_warn_amplification(total_change_count, effective_count, amplification_threshold) {
        let ratio = compute_amplification_ratio(total_change_count, effective_count);
        pgrx::warning!(
            "[pg_trickle] DAG-3: Delta amplification detected for {}.{}: \
             {} input rows → {} output rows ({:.1}× amplification, \
             threshold is {:.0}×). Consider restructuring the query to \
             reduce join fan-out, or raise pg_trickle.delta_amplification_threshold.",
            schema,
            name,
            total_change_count,
            effective_count,
            ratio,
            amplification_threshold,
        );
    }

    // G12-ERM-1: Record the effective mode for this execution path.
    set_effective_mode("DIFFERENTIAL");
    // OBS-4: Increment the DIFFERENTIAL refresh mode counter.
    crate::shmem::record_refresh_mode(true);

    // PART-WARN: After a successful refresh, warn if the default partition
    // of a partitioned stream table has accumulated rows.  This prompts the
    // user to create explicit named partitions.
    if st.st_partition_key.is_some() {
        warn_default_partition_growth(schema, name);
    }

    // F10 (v0.37.0): Emit OTLP trace span when trace propagation is enabled.
    // The trace context was stored in __pgt_trace_context at CDC capture time.
    // We read it from the change buffer's earliest recent row and emit a child span.
    emit_trace_span_if_enabled(st, "DIFFERENTIAL", start_ns);

    // DVM-3: Delta invariant validation (enabled by GUC; off by default).
    // When enabled, compare the stream table's row count against a full
    // recomputation of the defining query.  A mismatch indicates the
    // differential delta produced an incorrect result and is logged as a WARNING.
    if crate::config::pg_trickle_validate_delta_invariants() {
        let st_count = Spi::get_one::<i64>(&format!(
            "SELECT count(*)::bigint FROM \"{schema_esc}\".\"{name_esc}\"",
            schema_esc = schema.replace('"', "\"\""),
            name_esc = name.replace('"', "\"\""),
        ))
        .unwrap_or(Some(0))
        .unwrap_or(0);

        let dq_count = Spi::get_one::<i64>(&format!(
            "SELECT count(*)::bigint FROM ({dq}) AS __pgt_dvm3_validate",
            dq = effective_defining_query,
        ))
        .unwrap_or(Some(0))
        .unwrap_or(0);

        if st_count != dq_count {
            pgrx::warning!(
                "[pg_trickle] DVM-3: delta invariant violation for {schema}.{name}: \
                 stream table has {st_count} rows but defining query returns \
                 {dq_count} rows after applying delta (+{effective_count} effective rows)",
            );
        }
    }

    Ok((effective_count, 0))
}

/// F10 (v0.37.0): Emit an OTLP child span for the refresh cycle when
/// `pg_trickle.enable_trace_propagation = on`.
///
/// Reads the trace context from `__pgt_trace_context` in the change buffer
/// (earliest row in the most recent batch), then either:
/// - Exports via OTLP HTTP if `pg_trickle.otel_endpoint` is configured, or
/// - Logs the trace context at INFO if only propagation is enabled (no endpoint).
///
/// This function is non-fatal — any error is logged at DEBUG level.
fn emit_trace_span_if_enabled(st: &StreamTableMeta, refresh_mode: &str, start_ns: u64) {
    if !crate::config::pg_trickle_enable_trace_propagation() {
        return;
    }

    let end_ns = crate::otel::now_ns();

    // Attempt to read trace context from the change buffer.
    let trace_ctx =
        read_trace_context_from_change_buffer(st).or_else(crate::otel::read_session_trace_context);

    let Some(ctx) = trace_ctx else {
        return;
    };

    let span_name = match refresh_mode {
        "DIFFERENTIAL" => crate::otel::SPAN_DELTA_EXECUTE,
        "FULL" => crate::otel::SPAN_MERGE_APPLY,
        _ => crate::otel::SPAN_REFRESH_CYCLE,
    };

    let span = crate::otel::OtelSpan::new(ctx.clone(), span_name, start_ns, end_ns)
        .attr("refresh.mode", refresh_mode)
        .attr("st.name", format!("{}.{}", st.pgt_schema, st.pgt_name));

    let endpoint = crate::config::pg_trickle_otel_endpoint().unwrap_or_default();

    if endpoint.is_empty() {
        crate::otel::log_trace_context(
            &ctx,
            &format!("merge_apply/{refresh_mode}"),
            &format!("{}.{}", st.pgt_schema, st.pgt_name),
        );
    } else {
        crate::otel::export_span_background(endpoint, span);
    }
}

/// Read `__pgt_trace_context` from the most recent row in the change buffer for this ST.
///
/// Returns the first non-null trace context found, if any.
fn read_trace_context_from_change_buffer(
    st: &StreamTableMeta,
) -> Option<crate::otel::TraceContext> {
    use pgrx::prelude::*;

    let change_schema = crate::config::pg_trickle_change_buffer_schema().replace('"', "\"\"");
    let pgt_id = st.pgt_id;

    // Query the most recent non-null trace context from any change buffer
    // associated with the source tables of this stream table.
    let result: String = Spi::connect(|client| {
        // First try: source buffer via dependency lookup.
        if let Ok(rows) = client.select(
            &format!(
                "SELECT cb.__pgt_trace_context \
                 FROM pgtrickle.pgt_dependencies d \
                 JOIN LATERAL ( \
                     SELECT cb.__pgt_trace_context FROM {change_schema}.changes_pgt_{pgt_id} cb \
                     WHERE cb.__pgt_trace_context IS NOT NULL \
                     ORDER BY cb.change_id DESC LIMIT 1 \
                 ) cb ON TRUE \
                 WHERE d.pgt_id = {pgt_id} \
                 LIMIT 1",
                change_schema = change_schema,
                pgt_id = pgt_id,
            ),
            None,
            &[],
        ) {
            for row in rows {
                if let Ok(Some(tc)) = row.get::<String>(1) {
                    return tc;
                }
            }
        }

        // Second try: any source-table change buffer (query pgt_dependencies for relids).
        let dep_sql = format!(
            "SELECT d.source_stable_name \
             FROM pgtrickle.pgt_dependencies d \
             WHERE d.pgt_id = {pgt_id} \
               AND d.source_stable_name IS NOT NULL \
             LIMIT 3",
            pgt_id = pgt_id,
        );
        if let Ok(dep_rows) = client.select(&dep_sql, None, &[]) {
            for dep_row in dep_rows {
                if let Ok(Some(stable_name)) = dep_row.get::<String>(1) {
                    let buf_sql = format!(
                        "SELECT __pgt_trace_context FROM {change_schema}.changes_{stable_name} \
                         WHERE __pgt_trace_context IS NOT NULL \
                         ORDER BY change_id DESC LIMIT 1",
                        change_schema = change_schema,
                        stable_name = stable_name,
                    );
                    if let Ok(buf_rows) = client.select(&buf_sql, Some(1), &[]) {
                        for row in buf_rows {
                            if let Ok(Some(tc)) = row.get::<String>(1) {
                                return tc;
                            }
                        }
                    }
                }
            }
        }

        String::new()
    });

    if !result.is_empty() {
        crate::otel::TraceContext::parse(&result)
    } else {
        None
    }
}

// CODE-002: Pure helper — delta fraction threshold check.
//
// Returns `true` when `total_changes / estimated_rows > max_frac`.
// Extracted so the threshold logic can be unit-tested without a PostgreSQL backend.
//
// Precondition: caller must ensure `estimated_rows > 0.0` to avoid division
// by zero (the condition in execute_differential_refresh guards this).
pub(crate) fn delta_fraction_exceeds_threshold(
    total_changes: i64,
    estimated_rows: f64,
    max_frac: f64,
) -> bool {
    if total_changes <= 0 || estimated_rows <= 0.0 {
        return false;
    }
    let fraction = total_changes as f64 / estimated_rows;
    fraction > max_frac
}

// CODE-002: Unit tests for pure helpers in merge module.
#[cfg(test)]
mod tests {
    use super::*;

    // ── delta_fraction_exceeds_threshold ────────────────────────────

    #[test]
    fn test_delta_fraction_zero_changes_is_false() {
        assert!(!delta_fraction_exceeds_threshold(0, 1000.0, 0.1));
    }

    #[test]
    fn test_delta_fraction_zero_estimated_rows_is_false() {
        assert!(!delta_fraction_exceeds_threshold(100, 0.0, 0.1));
    }

    #[test]
    fn test_delta_fraction_exactly_at_threshold_is_false() {
        // 100 changes / 1000 rows = 0.10 — exactly at threshold, not exceeding.
        assert!(!delta_fraction_exceeds_threshold(100, 1000.0, 0.10));
    }

    #[test]
    fn test_delta_fraction_above_threshold_is_true() {
        // 101 changes / 1000 rows = 0.101 > 0.10
        assert!(delta_fraction_exceeds_threshold(101, 1000.0, 0.10));
    }

    #[test]
    fn test_delta_fraction_well_below_threshold_is_false() {
        // 10 changes / 10000 rows = 0.001 (0.1%) — well below 10% threshold
        assert!(!delta_fraction_exceeds_threshold(10, 10_000.0, 0.10));
    }

    #[test]
    fn test_delta_fraction_all_rows_changed_is_true() {
        // 1000/1000 = 1.0 > any reasonable threshold
        assert!(delta_fraction_exceeds_threshold(1000, 1000.0, 0.10));
    }

    #[test]
    fn test_delta_fraction_threshold_of_one_means_more_than_100_percent() {
        // max_frac = 1.0 means > 100% — impossible with natural data, so always false
        assert!(!delta_fraction_exceeds_threshold(1000, 1000.0, 1.0));
    }
}
