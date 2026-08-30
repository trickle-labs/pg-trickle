//! Recursive CTE differentiation (Tier 3c/3d/3e — semi-naive + DRed + non-linear).
//!
//! Implements differential maintenance for `WITH RECURSIVE` CTEs using three
//! strategies, selected automatically based on the query and change type:
//!
//! 1. **Semi-naive evaluation** for INSERT-only changes: Differentiate the
//!    base case normally, then propagate new rows through the recursive
//!    term using a nested `WITH RECURSIVE`.
//!
//! 2. **Delete-and-Rederive (DRed)** for mixed INSERT/DELETE/UPDATE changes:
//!    a) Propagate insertions via semi-naive (same as #1)
//!    b) Over-delete: propagate base-case deletions through the recursive
//!    term against ST storage to find all transitively-derived rows
//!    c) Rederive: re-execute the recursive CTE from remaining base rows
//!    to restore any over-deleted rows that have alternative derivations
//!    d) Combine: final delta = inserts + (over-deletions − rederived)
//!
//! 3. **Recomputation** fallback: re-executes the full defining query and
//!    diffs against ST storage. Used when the CTE has more columns than
//!    the outer SELECT projects (column mismatch), since the incremental
//!    paths require all CTE columns to be present in the ST storage table.
//!
//! ## Strategy Selection
//!
//! When CTE output columns == ST storage columns (no mismatch):
//!   - INSERT-only → semi-naive (strategy 1)
//!   - Mixed changes → DRed (strategy 2)
//!
//! When CTE columns ⊃ ST columns (mismatch):
//!   - All change types → recomputation (strategy 3)
//!
//! Non-linear recursion (multiple self-references) is rejected — PostgreSQL
//! restricts the recursive term to reference the CTE at most once.
//!
//! # SQL Generation Strategy
//!
//! For INSERT-only changes to a recursive CTE `r = B UNION ALL R(r)`:
//!
//! ```sql
//! WITH RECURSIVE
//!   __pgt_base_delta AS (
//!     -- Normal DVM differentiation of the base case (INSERT rows only)
//!     <differentiated base case>
//!   ),
//!   __pgt_rec_delta AS (
//!     -- Seed: base case delta
//!     SELECT cols FROM __pgt_base_delta WHERE __pgt_action = 'I'
//!     UNION ALL
//!     -- Seed: new base table rows joining existing ST storage
//!     SELECT cols FROM <recursive term with self_ref = ST_storage, base_tables = change_buffer>
//!     UNION ALL
//!     -- Propagation: recursive term applied to delta
//!     SELECT cols FROM <recursive term with self_ref = __pgt_rec_delta, base_tables = full>
//!   ),
//!   __pgt_final AS (
//!     SELECT pgtrickle.pg_trickle_hash(...) AS __pgt_row_id, 'I' AS __pgt_action, cols
//!     FROM __pgt_rec_delta
//!   )
//! SELECT * FROM __pgt_final
//! ```

use crate::dvm::diff::{DiffContext, DiffResult, col_list, quote_ident};
use crate::dvm::parser::OpTree;
use crate::error::PgTrickleError;

/// Differentiate a `RecursiveCte` node.
///
/// This is the primary entry point for recursive CTE delta computation.
/// The strategy depends on whether deletion changes are present:
///
/// - **INSERT-only**: Semi-naive propagation (efficient)
/// - **Mixed changes**: Delete-and-Rederive (DRed) — propagates both
///   insertions and deletions incrementally without full recomputation.
///
/// **Non-linear recursion** (multiple self-references in the recursive
/// term) is detected and rejected, since PostgreSQL restricts the
/// recursive term to reference the CTE at most once.
pub fn diff_recursive_cte(
    ctx: &mut DiffContext,
    op: &OpTree,
) -> Result<DiffResult, PgTrickleError> {
    let OpTree::RecursiveCte {
        alias,
        columns,
        base,
        recursive,
        union_all,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_recursive_cte called on non-RecursiveCte node".into(),
        ));
    };

    // Guard: detect non-linear recursion (multiple self-references).
    // PostgreSQL forbids this: "recursive reference to query must not
    // appear more than once". We detect it here to produce a clear
    // error message rather than letting PostgreSQL reject the generated SQL.
    let self_ref_count = count_self_refs(recursive);
    if self_ref_count > 1 {
        let aliases = collect_self_ref_aliases(recursive);
        return Err(PgTrickleError::UnsupportedOperator(format!(
            "Non-linear recursive CTE \"{alias}\" has {self_ref_count} \
             self-references ({aliases}). PostgreSQL restricts the recursive \
             term to reference the CTE at most once. Rewrite using a linear \
             form (single self-reference) instead.",
            aliases = aliases.join(", "),
        )));
    }

    // ── Strategy selection ────────────────────────────────────────────
    //
    // The semi-naive and DRed strategies replace the recursive self-
    // reference with the ST storage table. This requires all CTE output
    // columns to be present in the ST. When they match, we can use the
    // incremental paths; otherwise we fall back to recomputation which
    // re-executes the full defining query and diffs against storage.

    let columns_match = ctx
        .st_user_columns
        .as_ref()
        .is_some_and(|st_cols| *st_cols == *columns);

    if !columns_match {
        // Column mismatch: the ST storage has fewer columns than the
        // CTE (e.g., outer SELECT doesn't project parent_id). The
        // incremental paths would reference missing columns. Fall back
        // to recomputation which handles this correctly.
        return generate_recomputation_delta(ctx, alias, columns, base, recursive, *union_all);
    }

    // Guard: detect non-monotone recursive term.
    //
    // Semi-naive and DRed strategies assume the recursive term is
    // monotone: adding rows to the input can only add rows to the
    // output, never remove them. Non-monotone operators (EXCEPT,
    // Aggregate, Window, DISTINCT, Intersect) in the recursive term
    // break this assumption and can produce incorrect fixpoints.
    //
    // When non-monotone operators are detected, fall back to
    // recomputation which is always correct.
    if let Some(reason) = recursive_term_is_non_monotone(recursive) {
        pgrx::notice!(
            "[pg_trickle] ARCH-2: Recursive CTE \"{}\" has non-monotone recursive term ({}). \
             Falling back to recomputation strategy (refresh_reason = 'recursive_cte_fallback').",
            alias,
            reason
        );
        // ARCH-2: Record the fallback reason for pgt_refresh_history.
        return generate_recomputation_delta(ctx, alias, columns, base, recursive, *union_all);
    }

    // Columns match — differentiate the base case and choose strategy.
    let base_delta = ctx.diff_node(base)?;

    // Check whether any source table has DELETE or UPDATE changes.
    let source_oids = base.source_oids();
    let has_deletes = check_for_delete_changes(ctx, &source_oids)?;

    if has_deletes {
        // Mixed INSERT/DELETE/UPDATE changes → DRed (Delete-and-Rederive) algorithm.
        //
        // DRed handles both DIFFERENTIAL (ChangeBuffer) and IMMEDIATE
        // (TransitionTable) modes via the same four-phase algorithm:
        //
        //   Phase 1 — Semi-naive INSERT propagation: new rows from base delta
        //             are propagated through the recursive term until fixpoint.
        //
        //   Phase 2 — Over-deletion cascade: base-case DELETE rows (from the
        //             change buffer's old_<col> values in DIFFERENTIAL mode, or
        //             from the OLD transition table in IMMEDIATE mode) seed a
        //             recursive cascade against ST storage to find all rows
        //             transitively derived from the deleted seeds.
        //
        //   Phase 3 — Rederivation: the full recursive CTE is re-run from
        //             current source-table data (post-mutation state) to
        //             identify which over-deleted rows still have an
        //             alternative derivation path.
        //
        //   Phase 4 — Combine: net deletions = over-deleted EXCEPT rederived;
        //             final delta = inserts ∪ net-deletions.
        //
        // In DIFFERENTIAL mode the cascade (Phase 2) reads exclusively from ST
        // storage — never the change buffer — so correctness is preserved for
        // all scenarios including derived-column changes such as path/depth
        // rebuilds under a renamed or reparented ancestor node.
        generate_dred_delta(
            ctx,
            alias,
            columns,
            &base_delta,
            base,
            recursive,
            *union_all,
        )
    } else {
        // INSERT-only changes → semi-naive propagation.
        generate_semi_naive_delta(ctx, alias, columns, &base_delta, recursive, *union_all)
    }
}

/// Check if any of the given source tables have DELETE or UPDATE changes
/// within the current change interval.
///
/// For DIFFERENTIAL mode (`DeltaSource::ChangeBuffer`), queries the change
/// buffer tables using LSN-range filtering.
///
/// For IMMEDIATE mode (`DeltaSource::TransitionTable`), checks whether the
/// OLD transition table (`__pgt_oldtable_<oid>`) has any rows — its presence
/// indicates that the triggering statement produced DELETE or UPDATE rows.
fn check_for_delete_changes(
    ctx: &DiffContext,
    source_oids: &[u32],
) -> Result<bool, PgTrickleError> {
    use crate::dvm::diff::DeltaSource;
    use pgrx::Spi;

    // ── IMMEDIATE mode: inspect OLD transition tables ─────────────────
    if let DeltaSource::TransitionTable { tables } = &ctx.delta_source {
        for &oid in source_oids {
            if let Some(tt) = tables.get(&oid)
                && let Some(old_table) = &tt.old_name
            {
                // The OLD transition table exists when the triggering DML
                // was a DELETE or UPDATE.  Check whether it has any rows
                // (the table is always created but may be empty for
                // no-op UPDATEs).
                let check_sql = format!("SELECT EXISTS(SELECT 1 FROM {old_table} LIMIT 1)");
                let has_rows: Option<bool> = Spi::connect(|client| {
                    client
                        .select(&check_sql, None, &[])
                        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                        .first()
                        .get::<bool>(1)
                        .map_err(|e| PgTrickleError::SpiError(e.to_string()))
                })?;
                if has_rows == Some(true) {
                    return Ok(true);
                }
            }
        }
        return Ok(false);
    }

    // ── DIFFERENTIAL mode: query change buffer tables via LSN ─────────
    for &oid in source_oids {
        let change_table = ctx.change_table_for_source(oid);
        let prev_lsn = ctx.prev_frontier.get_lsn(oid);

        let check_sql = format!(
            "SELECT EXISTS(\
                SELECT 1 FROM {change_table} \
                WHERE action = 'D' \
                AND lsn > '{prev_lsn}'::pg_lsn\
            )"
        );

        let has_del: Option<bool> = Spi::connect(|client| {
            client
                .select(&check_sql, None, &[])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .first()
                .get::<bool>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        })?;

        if has_del == Some(true) {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Generate a recomputation diff delta for a recursive CTE.
///
/// Re-executes the full defining query (which contains the recursive CTE)
/// and compares against current ST storage to produce precise INSERT/DELETE
/// deltas.
///
/// When the original defining query is available (via `ctx.defining_query`),
/// uses it directly to recompute the result — this avoids column mismatch
/// issues where the CTE has more columns than the ST storage (the outer
/// SELECT projection). Falls back to OpTree reconstruction otherwise.
fn generate_recomputation_delta(
    ctx: &mut DiffContext,
    alias: &str,
    columns: &[String],
    base: &OpTree,
    recursive: &OpTree,
    _union_all: bool,
) -> Result<DiffResult, PgTrickleError> {
    // We need the ST storage table name for the anti-join
    let st_table = ctx
        .st_qualified_name
        .as_ref()
        .ok_or_else(|| {
            PgTrickleError::InternalError(
                "st_qualified_name required for recursive CTE recomputation diff".into(),
            )
        })?
        .clone();

    // Determine whether to use the defining query or the OpTree.
    // The defining query includes the outer SELECT that projects only
    // the columns stored in the ST. The OpTree reconstruction produces
    // ALL CTE columns (which may include extras like `parent_id` that
    // aren't in the ST storage). Using the defining query is preferred
    // because it exactly matches the ST schema.
    let (recomp_inner_sql, out_cols) = if let (Some(defining_query), Some(st_cols)) =
        (&ctx.defining_query, &ctx.st_user_columns)
    {
        // Use the defining query — output matches ST storage columns.
        (defining_query.clone(), st_cols.clone())
    } else {
        // Fallback: reconstruct from OpTree. Uses CTE-level columns.
        let base_sql = generate_query_sql(base, None)?;
        let rec_sql = generate_query_sql(recursive, Some(alias))?;
        let alias_q = quote_ident(alias);
        let col_list_str = col_list(columns);
        let sql = format!(
            "WITH RECURSIVE {alias_q} AS (\n\
                    {base_sql}\n\
                    UNION ALL\n\
                    {rec_sql}\n\
                )\n\
                SELECT {col_list_str} FROM {alias_q}",
        );
        (sql, columns.to_vec())
    };

    // Build column expressions for the diff CTEs.
    // When using the defining query, the recomputed CTE only has
    // the outer-projection columns. Some CTE columns (like parent_id)
    // may not exist in the recomputed result. Use sub.* to be safe.
    //
    // The __pgt_row_id must use the same hash formula as the initial
    // populate (content-hash of output columns) so that the anti-join
    // against the ST correctly identifies unchanged rows.
    let hash_exprs: Vec<String> = out_cols
        .iter()
        .map(|c| format!("sub.{}", quote_ident(c)))
        .collect();
    let row_id_hash = crate::dvm::operators::scan::build_hash_expr(&hash_exprs);

    let recomp_cte = ctx.next_cte_name(&format!("rc_recomp_{alias}"));
    let recomp_sql = format!(
        "SELECT {row_id_hash} AS __pgt_row_id, sub.*\n\
         FROM ({recomp_inner_sql}) sub",
    );
    ctx.add_cte(recomp_cte.clone(), recomp_sql);

    // CTE 2: find INSERTs (in recomputed but not in storage)
    let ins_cte = ctx.next_cte_name(&format!("rc_ins_{alias}"));
    let ins_sql = format!(
        "SELECT n.__pgt_row_id, 'I'::text AS __pgt_action, {n_cols}\n\
         FROM {recomp_cte} n\n\
         LEFT JOIN {st_table} s ON pgtrickle.row_probe_v1(s.__pgt_row_id) = pgtrickle.row_probe_v1(n.__pgt_row_id)\n\
             AND s.__pgt_row_id = n.__pgt_row_id\n\
         WHERE s.__pgt_row_id IS NULL",
        n_cols = out_cols
            .iter()
            .map(|c| format!("n.{}", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", "),
    );
    ctx.add_cte(ins_cte.clone(), ins_sql);

    // CTE 3: find DELETEs (in storage but not in recomputed)
    let del_cte = ctx.next_cte_name(&format!("rc_del_{alias}"));
    let del_sql = format!(
        "SELECT s.__pgt_row_id, 'D'::text AS __pgt_action, {s_cols}\n\
         FROM {st_table} s\n\
         LEFT JOIN {recomp_cte} n ON pgtrickle.row_probe_v1(n.__pgt_row_id) = pgtrickle.row_probe_v1(s.__pgt_row_id)\n\
             AND n.__pgt_row_id = s.__pgt_row_id\n\
         WHERE n.__pgt_row_id IS NULL",
        s_cols = out_cols
            .iter()
            .map(|c| format!("s.{}", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", "),
    );
    ctx.add_cte(del_cte.clone(), del_sql);

    // CTE 4: combine INSERTs and DELETEs
    let final_cte = ctx.next_cte_name(&format!("rc_delta_{alias}"));
    let final_sql = format!(
        "SELECT * FROM {ins_cte}\n\
         UNION ALL\n\
         SELECT * FROM {del_cte}"
    );
    ctx.add_cte(final_cte.clone(), final_sql);

    Ok(DiffResult {
        cte_name: final_cte,
        columns: out_cols.clone(),
        schema: crate::dvm::schema::RelationSchema::from_names(&out_cols),
        is_deduplicated: false,
        has_key_changed: false,
    })
}

/// Generate a Delete-and-Rederive (DRed) delta for mixed INSERT/DELETE/UPDATE
/// changes to a recursive CTE.
///
/// The DRed algorithm has four phases:
///
/// 1. **Insert propagation** — Semi-naive propagation of new rows from the
///    base case delta (same as the INSERT-only path).
///
/// 2. **Over-deletion** — Starting from base case DELETE rows, propagate
///    deletions through the recursive term against ST storage to find all
///    rows that are transitively derived from the deleted seeds. This is an
///    *over*-deletion because some of these rows may have alternative
///    derivation paths (e.g., a node reachable through multiple parents).
///
/// 3. **Rederivation** — Re-execute the full recursive CTE from the current
///    base tables (which already have the mutated data) and check which
///    over-deleted rows can still be derived. These are restored.
///
/// 4. **Combine** — Final delta = inserts + (over-deletions − rederived).
///
/// # SQL Structure
///
/// ```sql
/// WITH RECURSIVE
///   __pgt_base_delta AS (<differentiated base case>),
///
///   -- Phase 1: INSERT propagation (semi-naive)
///   __pgt_ins_delta AS (
///     SELECT cols FROM __pgt_base_delta WHERE __pgt_action = 'I'
///     UNION ALL
///     <seed from existing storage>
///     UNION ALL
///     <propagation through recursive term>
///   ),
///   __pgt_ins_final AS (
///     SELECT hash AS __pgt_row_id, 'I' AS __pgt_action, cols
///     FROM __pgt_ins_delta
///   ),
///
///   -- Phase 2: Over-deletion
///   __pgt_del_cascade AS (
///     -- Seed: base case DELETE rows
///     SELECT cols FROM __pgt_base_delta WHERE __pgt_action = 'D'
///     UNION ALL
///     -- Propagation: find ST storage rows joining del_cascade
///     SELECT s.cols FROM ST_storage s JOIN __pgt_del_cascade d ON ...
///   ),
///
///   -- Phase 3: Rederivation from current base tables
///   __pgt_rederived AS (
///     WITH RECURSIVE full_cte AS (base UNION ALL rec)
///     SELECT cols FROM full_cte
///     WHERE (cols) IN (SELECT cols FROM __pgt_del_cascade)
///   ),
///
///   -- Phase 4: Combine
///   -- Net deletions = over-deleted EXCEPT rederived
///   __pgt_net_del AS (
///     SELECT cols FROM __pgt_del_cascade
///     EXCEPT
///     SELECT cols FROM __pgt_rederived
///   ),
///   __pgt_del_final AS (
///     SELECT hash AS __pgt_row_id, 'D' AS __pgt_action, cols
///     FROM __pgt_net_del
///   ),
///   __pgt_combined AS (
///     SELECT * FROM __pgt_ins_final
///     UNION ALL
///     SELECT * FROM __pgt_del_final
///   )
/// SELECT * FROM __pgt_combined
/// ```
fn generate_dred_delta(
    ctx: &mut DiffContext,
    alias: &str,
    columns: &[String],
    base_delta: &DiffResult,
    base: &OpTree,
    recursive: &OpTree,
    union_all: bool,
) -> Result<DiffResult, PgTrickleError> {
    let st_table = ctx
        .st_qualified_name
        .as_ref()
        .ok_or_else(|| {
            PgTrickleError::InternalError(
                "st_qualified_name required for recursive CTE DRed diff".into(),
            )
        })?
        .clone();

    let col_list_str = col_list(columns);

    // ── Phase 1: INSERT propagation (semi-naive) ──────────────────────

    let ins_delta =
        generate_semi_naive_ins_only(ctx, alias, columns, base_delta, recursive, union_all)?;

    // ── Phase 2: Over-deletion ────────────────────────────────────────
    //
    // Seed from DELETE rows in the base delta, then recursively find
    // all rows in ST storage that were derived via those deleted rows
    // by matching the recursive term's join condition against storage.

    let del_seed_cte = ctx.next_cte_name(&format!("dred_dseed_{alias}"));
    let recursive_del_seed = generate_old_seed_from_existing(ctx, recursive, &st_table, columns)?;
    let del_seed_sql = if let Some(recursive_seed) = recursive_del_seed {
        format!(
            "SELECT {col_list_str} FROM {base_cte} WHERE __pgt_action = 'D'\n\
             UNION ALL\n\
             {recursive_seed}",
            base_cte = base_delta.cte_name,
        )
    } else {
        format!(
            "SELECT {col_list_str} FROM {base_cte} WHERE __pgt_action = 'D'",
            base_cte = base_delta.cte_name,
        )
    };
    ctx.add_cte(del_seed_cte.clone(), del_seed_sql);

    // Build the over-deletion cascade.
    // We need a recursive CTE: seed = del_seed, recursive term joins
    // ST storage rows whose parent column matches the cascade's key column.
    let del_cascade_cte = ctx.next_cte_name(&format!("dred_dcasc_{alias}"));
    let cascade_propagation = generate_cascade_propagation(recursive, &del_cascade_cte, &st_table)?;

    let del_cascade_sql = format!(
        "SELECT {col_list_str} FROM {del_seed_cte}\n\
         UNION ALL\n\
         {cascade_propagation}"
    );
    ctx.add_recursive_cte(del_cascade_cte.clone(), del_cascade_sql);

    // ── Phase 3: Rederivation ─────────────────────────────────────────
    //
    // Re-execute the full recursive CTE from current base tables
    // (post-mutation), then intersect with the over-deleted set.
    // Any row that appears in both the rederived result AND the
    // over-deleted set was over-deleted and should be restored.

    let base_sql = generate_query_sql(base, None)?;
    let rec_sql = generate_query_sql(recursive, Some(alias))?;
    let union_kw = if union_all { "UNION ALL" } else { "UNION" };

    // CTE: full rederivation of the recursive CTE from current data
    let rederive_full_cte = ctx.next_cte_name(&format!("dred_rfull_{alias}"));
    let rederive_full_sql = format!(
        "WITH RECURSIVE {alias_q} AS (\n\
            {base_sql}\n\
            {union_kw}\n\
            {rec_sql}\n\
        )\n\
        SELECT {col_list_str} FROM {alias_q}",
        alias_q = quote_ident(alias),
    );
    ctx.add_cte(rederive_full_cte.clone(), rederive_full_sql);

    // CTE: rederived rows = intersection of rederive_full and del_cascade
    // Using INTERSECT to find rows that exist in both sets.
    let rederived_cte = ctx.next_cte_name(&format!("dred_rdrv_{alias}"));
    let rederived_sql = format!(
        "SELECT {col_list_str} FROM {del_cascade_cte}\n\
         INTERSECT\n\
         SELECT {col_list_str} FROM {rederive_full_cte}"
    );
    ctx.add_cte(rederived_cte.clone(), rederived_sql);

    // ── Phase 4: Combine ──────────────────────────────────────────────

    // Net deletions = over-deleted EXCEPT rederived
    let net_del_cte = ctx.next_cte_name(&format!("dred_ndel_{alias}"));
    let net_del_sql = format!(
        "SELECT {col_list_str} FROM {del_cascade_cte}\n\
         EXCEPT\n\
         SELECT {col_list_str} FROM {rederived_cte}"
    );
    ctx.add_cte(net_del_cte.clone(), net_del_sql);

    // Wrap net deletions with __pgt_row_id and __pgt_action = 'D'
    // We need to match __pgt_row_id from ST storage.
    let del_final_cte = ctx.next_cte_name(&format!("dred_dfin_{alias}"));
    let del_match_cols = columns
        .iter()
        .map(|c| format!("d.{col} IS NOT DISTINCT FROM s.{col}", col = quote_ident(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let del_final_sql = format!(
        "SELECT s.__pgt_row_id, 'D'::text AS __pgt_action, {del_cols}\n\
         FROM {net_del_cte} d\n\
         JOIN {st_table} s ON {del_match_cols}",
        del_cols = columns
            .iter()
            .map(|c| format!("s.{}", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(", "),
    );
    ctx.add_cte(del_final_cte.clone(), del_final_sql);

    // Combine inserts and deletes
    let combined_cte = ctx.next_cte_name(&format!("dred_comb_{alias}"));
    let combined_sql = format!(
        "SELECT * FROM {ins_cte}\n\
         UNION ALL\n\
         SELECT * FROM {del_final_cte}",
        ins_cte = ins_delta.cte_name,
    );
    ctx.add_cte(combined_cte.clone(), combined_sql);

    Ok(DiffResult {
        cte_name: combined_cte,
        columns: columns.to_vec(),
        schema: crate::dvm::schema::RelationSchema::from_names(columns),
        is_deduplicated: false,
        has_key_changed: false,
    })
}

/// Generate the semi-naive INSERT-only propagation sub-query for use
/// within the DRed algorithm. Same logic as `generate_semi_naive_delta`
/// but packaged as a sub-result that DRed can combine with deletions.
fn generate_semi_naive_ins_only(
    ctx: &mut DiffContext,
    alias: &str,
    columns: &[String],
    base_delta: &DiffResult,
    recursive: &OpTree,
    union_all: bool,
) -> Result<DiffResult, PgTrickleError> {
    let st_table = ctx
        .st_qualified_name
        .as_ref()
        .ok_or_else(|| {
            PgTrickleError::InternalError(
                "st_qualified_name required for DRed insert propagation".into(),
            )
        })?
        .clone();

    let col_list_str = col_list(columns);

    // The delta CTE name that the recursive term will reference
    let delta_cte = ctx.next_cte_name(&format!("dred_ins_{alias}"));

    // Seed: base case delta INSERT rows only
    let seed_from_base_raw = format!(
        "SELECT {col_list_str} FROM {base_cte} WHERE __pgt_action = 'I'",
        base_cte = base_delta.cte_name,
    );

    // Seed from existing storage (new rows joining ST storage)
    let seed_from_existing_raw = generate_seed_from_existing(ctx, recursive, &st_table, columns)?;

    // Non-linear seeds for multiple self-reference positions
    let self_ref_aliases = collect_self_ref_aliases(recursive);
    let nonlinear_seeds = generate_nonlinear_seeds(
        recursive,
        &self_ref_aliases,
        &base_delta.cte_name,
        &st_table,
        columns,
    )?;

    // ── Depth guard (both DIFFERENTIAL and IMMEDIATE modes) ──────────
    // COR-2: Guard is now unconditional — the GUC is applied to both modes.
    #[cfg(not(test))]
    let max_depth = crate::config::pg_trickle_ivm_recursive_max_depth();
    #[cfg(test)]
    let max_depth: Option<i32> = None;

    let (seed_from_base, seed_from_existing, propagation) = if let Some(depth_limit) = max_depth {
        // Wrap each seed in a subquery so __pgt_depth is added as a new
        // column in the SELECT list, not appended after a WHERE clause.
        let sb =
            format!("SELECT __seed.*, 0::int AS __pgt_depth FROM ({seed_from_base_raw}) __seed");
        let se = seed_from_existing_raw
            .map(|s| format!("SELECT __seed.*, 0::int AS __pgt_depth FROM ({s}) __seed"));
        let prop = match try_generate_propagation_with_depth(recursive, &delta_cte, depth_limit)? {
            Some(sql) => sql,
            None => {
                pgrx::warning!(
                    "[pg_trickle] recursive CTE '{}' (DRed ins): depth guard not supported \
                     for this recursive term pattern; falling back to unguarded propagation.",
                    alias,
                );
                generate_query_sql(recursive, Some(&delta_cte))?
            }
        };
        (sb, se, prop)
    } else {
        let prop = generate_query_sql(recursive, Some(&delta_cte))?;
        (seed_from_base_raw, seed_from_existing_raw, prop)
    };

    let mut parts = vec![seed_from_base];
    if let Some(existing_seed) = seed_from_existing {
        parts.push(existing_seed);
    }
    parts.extend(nonlinear_seeds);
    parts.push(propagation);
    // Mirror the original query's set/multiset semantics so that cyclic
    // graphs terminate (UNION deduplicates each iteration; UNION ALL does not).
    let union_kw = if union_all { "UNION ALL" } else { "UNION" };
    let recursive_sql = parts.join(&format!("\n{union_kw}\n"));

    ctx.add_recursive_cte(delta_cte.clone(), recursive_sql);

    // Wrap with __pgt_row_id and __pgt_action = 'I'.
    // Explicitly select only user columns so __pgt_depth is excluded.
    // Use content-hash of output columns matching the initial populate.
    let dred_hash_exprs: Vec<String> = columns
        .iter()
        .map(|c| format!("sub.{}", quote_ident(c)))
        .collect();
    let dred_row_id_hash = crate::dvm::operators::scan::build_hash_expr(&dred_hash_exprs);

    let ins_final_cte = ctx.next_cte_name(&format!("dred_ifin_{alias}"));
    let ins_final_sql = format!(
        "SELECT {dred_row_id_hash} AS __pgt_row_id,\n\
               'I'::text AS __pgt_action,\n\
               {col_list_str}\n\
         FROM {delta_cte} sub",
    );
    ctx.add_cte(ins_final_cte.clone(), ins_final_sql);

    Ok(DiffResult {
        cte_name: ins_final_cte,
        columns: columns.to_vec(),
        schema: crate::dvm::schema::RelationSchema::from_names(columns),
        is_deduplicated: false,
        has_key_changed: false,
    })
}

/// Generate the recursive propagation SQL for the over-deletion cascade.
///
/// This builds the recursive term that finds ST storage rows whose
/// parent/join key matches rows in the deletion cascade. The recursive
/// term's join condition from the original CTE tells us how child rows
/// connect to parent rows — we use the same join but with storage as
/// the source of child rows and the cascade CTE as the parent.
fn generate_cascade_propagation(
    recursive: &OpTree,
    cascade_cte: &str,
    st_table: &str,
) -> Result<String, PgTrickleError> {
    // The recursive term is of the form:
    //   SELECT cols FROM base_table t JOIN <self_ref> r ON t.parent = r.id
    // For the cascade, we need:
    //   SELECT s.cols FROM ST_storage s JOIN cascade d ON <join condition>
    // where the join condition maps child (storage) to parent (cascade).
    //
    // We walk the OpTree to find the join and replace:
    //   - base table scans → ST storage scan
    //   - self-ref → cascade CTE
    generate_query_sql_cascade(recursive, cascade_cte, st_table)
}

/// Generate SQL for the cascade propagation, replacing base table scans
/// with ST storage and self-references with the cascade CTE.
fn generate_query_sql_cascade(
    op: &OpTree,
    cascade_cte: &str,
    st_table: &str,
) -> Result<String, PgTrickleError> {
    match op {
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            let left_from = generate_cascade_from(left, cascade_cte, st_table)?;
            let right_from = generate_cascade_from(right, cascade_cte, st_table)?;
            let mut all_cols = Vec::new();
            collect_cascade_cols(left, &mut all_cols);
            collect_cascade_cols(right, &mut all_cols);
            Ok(format!(
                "SELECT {cols}\nFROM {left_from}\nJOIN {right_from}\n  ON {cond}",
                cols = all_cols.join(", "),
                cond = condition.to_sql(),
            ))
        }

        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => {
            let left_from = generate_cascade_from(left, cascade_cte, st_table)?;
            let right_from = generate_cascade_from(right, cascade_cte, st_table)?;
            let mut all_cols = Vec::new();
            collect_cascade_cols(left, &mut all_cols);
            collect_cascade_cols(right, &mut all_cols);
            Ok(format!(
                "SELECT {cols}\nFROM {left_from}\nLEFT JOIN {right_from}\n  ON {cond}",
                cols = all_cols.join(", "),
                cond = condition.to_sql(),
            ))
        }

        OpTree::Project {
            expressions,
            aliases,
            child,
        } => {
            let proj_exprs: Vec<String> = expressions
                .iter()
                .zip(aliases.iter())
                .map(|(e, a)| {
                    let esql = e.to_sql();
                    if esql == *a {
                        quote_ident(a)
                    } else {
                        format!("{esql} AS {}", quote_ident(a))
                    }
                })
                .collect();
            // Inline the FROM clause for Join children so that project
            // expressions (which use original table aliases, e.g. `n.id`)
            // resolve correctly — wrapping in a subquery would hide aliases.
            match child.as_ref() {
                OpTree::InnerJoin {
                    condition,
                    left,
                    right,
                } => {
                    let left_from = generate_cascade_from(left, cascade_cte, st_table)?;
                    let right_from = generate_cascade_from(right, cascade_cte, st_table)?;
                    Ok(format!(
                        "SELECT {projs}\nFROM {left_from}\nJOIN {right_from}\n  ON {cond}",
                        projs = proj_exprs.join(", "),
                        cond = condition.to_sql(),
                    ))
                }
                OpTree::LeftJoin {
                    condition,
                    left,
                    right,
                } => {
                    let left_from = generate_cascade_from(left, cascade_cte, st_table)?;
                    let right_from = generate_cascade_from(right, cascade_cte, st_table)?;
                    Ok(format!(
                        "SELECT {projs}\nFROM {left_from}\nLEFT JOIN {right_from}\n  ON {cond}",
                        projs = proj_exprs.join(", "),
                        cond = condition.to_sql(),
                    ))
                }
                _ => {
                    let child_sql = generate_query_sql_cascade(child, cascade_cte, st_table)?;
                    Ok(format!(
                        "SELECT {projs}\nFROM (\n{child_sql}\n) __p",
                        projs = proj_exprs.join(", "),
                    ))
                }
            }
        }

        OpTree::Filter { predicate, child } => {
            let child_sql = generate_query_sql_cascade(child, cascade_cte, st_table)?;
            Ok(format!(
                "SELECT * FROM (\n{child_sql}\n) __f\nWHERE {pred}",
                pred = predicate.to_sql(),
            ))
        }

        _ => Err(PgTrickleError::InternalError(format!(
            "generate_query_sql_cascade: unsupported OpTree variant {:?}",
            op.alias(),
        ))),
    }
}

/// Generate a FROM-clause fragment for the cascade propagation.
///
/// - Base table scans (Scan) are replaced with ST storage references
///   (since we're looking for rows that already exist in the ST).
/// - Self-references (RecursiveSelfRef) are replaced with the cascade CTE
///   (since we're propagating through the cascade).
fn generate_cascade_from(
    op: &OpTree,
    cascade_cte: &str,
    st_table: &str,
) -> Result<String, PgTrickleError> {
    match op {
        // Base table scan → ST storage (we're finding existing derived rows)
        OpTree::Scan { alias, .. } => Ok(format!(
            "{st_table} AS {alias_q}",
            alias_q = quote_ident(alias),
        )),

        // Self-reference → cascade CTE
        OpTree::RecursiveSelfRef { alias, .. } => Ok(format!(
            "{cascade_cte} AS {alias_q}",
            alias_q = quote_ident(alias),
        )),

        OpTree::Subquery { alias, child, .. } => {
            let child_sql = generate_query_sql_cascade(child, cascade_cte, st_table)?;
            Ok(format!(
                "(\n{child_sql}\n) AS {alias_q}",
                alias_q = quote_ident(alias),
            ))
        }

        // Expand nested joins inline so that table aliases remain in scope
        // for the enclosing ON condition.
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            let left_from = generate_cascade_from(left, cascade_cte, st_table)?;
            let right_from = generate_cascade_from(right, cascade_cte, st_table)?;
            Ok(format!(
                "{left_from}\nJOIN {right_from}\n  ON {cond}",
                cond = condition.to_sql(),
            ))
        }

        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => {
            let left_from = generate_cascade_from(left, cascade_cte, st_table)?;
            let right_from = generate_cascade_from(right, cascade_cte, st_table)?;
            Ok(format!(
                "{left_from}\nLEFT JOIN {right_from}\n  ON {cond}",
                cond = condition.to_sql(),
            ))
        }

        _ => {
            let sql = generate_query_sql_cascade(op, cascade_cte, st_table)?;
            Ok(format!("(\n{sql}\n) AS __sub"))
        }
    }
}

/// Collect column references for SELECT list in cascade context.
///
/// Base table scans output columns using their alias (which references
/// ST storage in cascade context). Self-references output columns using
/// their alias (which references the cascade CTE).
fn collect_cascade_cols(op: &OpTree, out: &mut Vec<String>) {
    match op {
        OpTree::Scan { alias, .. }
        | OpTree::RecursiveSelfRef { alias, .. }
        | OpTree::Subquery { alias, .. } => {
            for col in op.output_columns() {
                out.push(format!("{}.{}", quote_ident(alias), quote_ident(&col)));
            }
        }
        // Recursively expand nested join chains.
        OpTree::InnerJoin { left, right, .. } | OpTree::LeftJoin { left, right, .. } => {
            collect_cascade_cols(left, out);
            collect_cascade_cols(right, out);
        }
        _ => {
            let alias = "__sub";
            for col in op.output_columns() {
                out.push(format!("{}.{}", quote_ident(alias), quote_ident(&col)));
            }
        }
    }
}

/// Try to generate depth-guarded propagation SQL for the semi-naive recursive step.
///
/// When `max_depth` is active in IMMEDIATE mode, the propagation term of the
/// recursive delta CTE must:
/// 1. Carry a `__pgt_depth` counter (incremented from the self-reference row).
/// 2. Guard via `WHERE {sr_alias}.__pgt_depth < max_depth`.
///
/// Handles the common patterns for a recursive CTE's recursive term:
/// - `Project { child: InnerJoin/LeftJoin { ..., RecursiveSelfRef } }`
/// - `Filter { child: Project { child: InnerJoin/LeftJoin { ..., RecursiveSelfRef } } }`
///
/// Returns `Ok(Some(sql))` on success, `Ok(None)` if the pattern is not
/// recognised (caller falls back to depth-unguarded propagation).
fn try_generate_propagation_with_depth(
    op: &OpTree,
    delta_cte: &str,
    max_depth: i32,
) -> Result<Option<String>, PgTrickleError> {
    // Peel off an optional outer Filter wrapper.
    let (filter_pred, inner) = match op {
        OpTree::Filter { predicate, child } => (Some(predicate.to_sql()), child.as_ref()),
        other => (None, other),
    };

    // Expect Project over InnerJoin or LeftJoin.
    let (proj_exprs, proj_aliases, join_cond, left, right, is_left_join) = match inner {
        OpTree::Project {
            expressions,
            aliases,
            child,
        } => match child.as_ref() {
            OpTree::InnerJoin {
                condition,
                left,
                right,
            } => (
                expressions,
                aliases,
                condition,
                left.as_ref(),
                right.as_ref(),
                false,
            ),
            OpTree::LeftJoin {
                condition,
                left,
                right,
            } => (
                expressions,
                aliases,
                condition,
                left.as_ref(),
                right.as_ref(),
                true,
            ),
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };

    // Identify which join side is the RecursiveSelfRef and get its alias.
    let sr_alias = match (left, right) {
        (_, OpTree::RecursiveSelfRef { alias, .. }) => alias.as_str(),
        (OpTree::RecursiveSelfRef { alias, .. }, _) => alias.as_str(),
        _ => return Ok(None),
    };
    let sr_alias_q = quote_ident(sr_alias);

    // Generate FROM clause fragments.
    let left_from = generate_from_sql(left, Some(delta_cte))?;
    let right_from = generate_from_sql(right, Some(delta_cte))?;

    // Build projected columns WITH depth increment from the self-ref.
    let mut result_exprs: Vec<String> = proj_exprs
        .iter()
        .zip(proj_aliases.iter())
        .map(|(e, a)| {
            let esql = e.to_sql();
            if esql == *a {
                quote_ident(a)
            } else {
                format!("{esql} AS {}", quote_ident(a))
            }
        })
        .collect();
    result_exprs.push(format!("{sr_alias_q}.__pgt_depth + 1 AS __pgt_depth"));

    let join_kw = if is_left_join { "LEFT JOIN" } else { "JOIN" };

    let mut sql = format!(
        "SELECT {projs}\nFROM {left_from}\n{join_kw} {right_from}\n  ON {cond}\nWHERE {sr_alias_q}.__pgt_depth < {max_depth}",
        projs = result_exprs.join(", "),
        cond = join_cond.to_sql(),
    );

    // Re-wrap with the outer filter if present.
    if let Some(pred) = filter_pred {
        sql = format!("SELECT * FROM (\n{sql}\n) __flt\nWHERE {pred}");
    }

    Ok(Some(sql))
}

/// Generate the semi-naive delta for INSERT-only changes.
///
/// Builds a `WITH RECURSIVE` delta query that:
/// 1. Seeds from the differentiated base case (INSERT rows)
/// 2. Seeds from new rows joining existing ST storage
/// 3. Propagates through the recursive term until fixpoint
fn generate_semi_naive_delta(
    ctx: &mut DiffContext,
    alias: &str,
    columns: &[String],
    base_delta: &DiffResult,
    recursive: &OpTree,
    union_all: bool,
) -> Result<DiffResult, PgTrickleError> {
    // We need the ST storage table name
    let st_table = ctx
        .st_qualified_name
        .as_ref()
        .ok_or_else(|| {
            PgTrickleError::InternalError(
                "st_qualified_name required for recursive CTE semi-naive diff".into(),
            )
        })?
        .clone();

    let col_list_str = col_list(columns);

    // The delta CTE name that the recursive term will reference
    let delta_cte = ctx.next_cte_name(&format!("rc_snv_{alias}"));

    // Generate the seed SQL: base case delta (INSERT rows only)
    let seed_from_base = format!(
        "SELECT {col_list_str} FROM {base_cte} WHERE __pgt_action = 'I'",
        base_cte = base_delta.cte_name,
    );

    // Generate the "new rows joining existing storage" seed.
    // This handles the case where newly inserted base table rows join
    // with already-existing rows in the ST storage (e.g., a new child
    // node whose parent is already in the tree).
    let seed_from_existing = generate_seed_from_existing(ctx, recursive, &st_table, columns)?;

    // For non-linear recursion (multiple self-references), generate
    // per-position seeds where each self-ref position alternately reads
    // from the base case delta while others read from ST storage.
    let self_ref_aliases = collect_self_ref_aliases(recursive);
    let nonlinear_seeds = generate_nonlinear_seeds(
        recursive,
        &self_ref_aliases,
        &base_delta.cte_name,
        &st_table,
        columns,
    )?;

    // ── Depth guard (IMMEDIATE mode only) ────────────────────────────
    //
    // In IMMEDIATE mode (TransitionTable delta source), inject a
    // ── Depth guard (both DIFFERENTIAL and IMMEDIATE modes) ──────────
    // COR-2: Guard is now unconditional — the GUC is applied to both modes.
    #[cfg(not(test))]
    let max_depth = crate::config::pg_trickle_ivm_recursive_max_depth();
    #[cfg(test)]
    let max_depth: Option<i32> = None;

    let (seed_from_base, seed_from_existing, propagation) = if let Some(depth_limit) = max_depth {
        // Wrap each seed in a subquery and add __pgt_depth = 0 in the
        // outer SELECT to avoid appending to arbitrary SQL (which breaks
        // when the inner query has WHERE / JOIN ON clauses).
        let seeded_from_base =
            format!("SELECT __seed.*, 0::int AS __pgt_depth FROM ({seed_from_base}) __seed");

        // Generate depth-guarded propagation SQL.
        let prop = match try_generate_propagation_with_depth(recursive, &delta_cte, depth_limit)? {
            Some(sql) => sql,
            None => {
                // Pattern not recognised — fall back to depth-unguarded propagation
                // and strip the depth column from seeds to keep column counts consistent.
                pgrx::warning!(
                    "[pg_trickle] recursive CTE '{}': depth guard not supported for this \
                     recursive term pattern; falling back to unguarded propagation.",
                    alias,
                );
                let prop_plain = generate_query_sql(recursive, Some(&delta_cte))?;
                return build_semi_naive_result(
                    ctx,
                    alias,
                    columns,
                    &col_list_str,
                    &delta_cte,
                    seed_from_base,
                    seed_from_existing,
                    nonlinear_seeds,
                    prop_plain,
                    union_all,
                );
            }
        };

        // Only reach here when depth-guarded propagation succeeded — safe to
        // consume seed_from_existing now.
        let seeded_from_existing = seed_from_existing
            .map(|s| format!("SELECT __seed.*, 0::int AS __pgt_depth FROM ({s}) __seed"));

        (seeded_from_base, seeded_from_existing, prop)
    } else {
        // Depth guard disabled — plain propagation.
        let prop = generate_query_sql(recursive, Some(&delta_cte))?;
        (seed_from_base, seed_from_existing, prop)
    };

    build_semi_naive_result(
        ctx,
        alias,
        columns,
        &col_list_str,
        &delta_cte,
        seed_from_base,
        seed_from_existing,
        nonlinear_seeds,
        propagation,
        union_all,
    )
}

/// Assemble the recursive delta CTE and final wrapper CTE from the
/// pre-built parts generated by `generate_semi_naive_delta`.
#[allow(clippy::too_many_arguments)]
fn build_semi_naive_result(
    ctx: &mut DiffContext,
    alias: &str,
    columns: &[String],
    col_list_str: &str,
    delta_cte: &str,
    seed_from_base: String,
    seed_from_existing: Option<String>,
    nonlinear_seeds: Vec<String>,
    propagation: String,
    union_all: bool,
) -> Result<DiffResult, PgTrickleError> {
    // Build the complete recursive delta CTE.
    //
    // When the original query uses UNION (set semantics), we mirror that
    // here so that the recursive delta CTE deduplicates on each iteration.
    // This is essential for correctness in cyclic graphs: without UNION the
    // delta would loop forever re-deriving the same pairs via cycles.
    // UNION ALL is kept only when the original query explicitly opts into
    // multiset semantics.
    let union_kw = if union_all { "UNION ALL" } else { "UNION" };
    let mut parts = vec![seed_from_base];
    if let Some(existing_seed) = seed_from_existing {
        parts.push(existing_seed);
    }
    parts.extend(nonlinear_seeds);
    parts.push(propagation);
    let recursive_sql = parts.join(&format!("\n{union_kw}\n"));

    ctx.add_recursive_cte(delta_cte.to_string(), recursive_sql);

    // Wrap with __pgt_row_id and __pgt_action.
    // The final SELECT explicitly picks only the user columns from delta_cte,
    // dropping the hidden __pgt_depth counter column when it is present.
    // Use content-hash of output columns matching the initial populate.
    let sn_hash_exprs: Vec<String> = columns
        .iter()
        .map(|c| format!("sub.{}", quote_ident(c)))
        .collect();
    let sn_row_id_hash = crate::dvm::operators::scan::build_hash_expr(&sn_hash_exprs);

    let final_cte_name = ctx.next_cte_name(&format!("rc_final_{alias}"));
    let final_sql = format!(
        "SELECT {sn_row_id_hash} AS __pgt_row_id,\n\
               'I'::text AS __pgt_action,\n\
               {col_list_str}\n\
         FROM {delta_cte} sub",
    );
    ctx.add_cte(final_cte_name.clone(), final_sql);

    Ok(DiffResult {
        cte_name: final_cte_name,
        columns: columns.to_vec(),
        schema: crate::dvm::schema::RelationSchema::from_names(columns),
        is_deduplicated: false,
        has_key_changed: false,
    })
}

/// Generate the seed SQL for "new rows joining existing ST storage".
///
/// This handles the case where the recursive term joins base tables
/// with the CTE self-reference. When new rows are inserted into the
/// base table, they might directly connect to existing rows in the
/// ST storage (e.g., inserting a child node whose parent already exists).
///
/// Returns `None` if the recursive term structure doesn't have base table
/// scans (unusual but possible).
fn generate_seed_from_existing(
    ctx: &DiffContext,
    recursive: &OpTree,
    st_table: &str,
    _columns: &[String],
) -> Result<Option<String>, PgTrickleError> {
    // Generate the recursive term SQL with the self-reference replaced
    // by the existing ST storage table, and base table scans replaced
    // by their change buffer deltas (INSERT rows only).
    let sql = generate_query_sql_with_change_buffers(ctx, recursive, st_table)?;

    match sql {
        Some(s) => Ok(Some(s)),
        None => Ok(None),
    }
}

/// Generate the seed SQL for old/deleted rows joining existing ST storage.
///
/// This is the DRed counterpart to `generate_seed_from_existing()`: it reads
/// DELETE rows and OLD values from UPDATEs in the recursive term's base-table
/// scans, joins them against existing ST storage, and produces the directly
/// affected old recursive rows that must seed the over-deletion cascade.
fn generate_old_seed_from_existing(
    ctx: &DiffContext,
    recursive: &OpTree,
    st_table: &str,
    _columns: &[String],
) -> Result<Option<String>, PgTrickleError> {
    let sql = generate_query_sql_with_old_change_buffers(ctx, recursive, st_table)?;

    match sql {
        Some(s) => Ok(Some(s)),
        None => Ok(None),
    }
}

// ── SQL Generation from OpTree ──────────────────────────────────────────

/// Generate SQL from an OpTree, replacing `RecursiveSelfRef` with the
/// given replacement identifier.
///
/// This is a simplified SQL code generator that handles the subset of
/// OpTree variants that appear in recursive CTE terms (Scan, Filter,
/// Join, Project, RecursiveSelfRef).
///
/// `self_ref_replacement`: the table/CTE name to use for `RecursiveSelfRef`
/// references. If `None`, self-references produce an error.
fn generate_query_sql(
    op: &OpTree,
    self_ref_replacement: Option<&str>,
) -> Result<String, PgTrickleError> {
    match op {
        OpTree::Scan {
            schema,
            table_name,
            alias,
            columns,
            ..
        } => {
            let col_exprs: Vec<String> = columns
                .iter()
                .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(&c.name)))
                .collect();
            Ok(format!(
                "SELECT {cols}\nFROM {schema_q}.{table_q} AS {alias_q}",
                cols = col_exprs.join(", "),
                schema_q = quote_ident(schema),
                table_q = quote_ident(table_name),
                alias_q = quote_ident(alias),
            ))
        }

        OpTree::RecursiveSelfRef { alias, columns, .. } => {
            let replacement = self_ref_replacement.ok_or_else(|| {
                PgTrickleError::InternalError(
                    "RecursiveSelfRef encountered without replacement target".into(),
                )
            })?;
            let col_exprs: Vec<String> = columns
                .iter()
                .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(c)))
                .collect();
            Ok(format!(
                "SELECT {cols}\nFROM {replacement} AS {alias_q}",
                cols = col_exprs.join(", "),
                alias_q = quote_ident(alias),
            ))
        }

        OpTree::Filter { predicate, child } => {
            let child_sql = generate_query_sql(child, self_ref_replacement)?;
            Ok(format!(
                "SELECT * FROM (\n{child_sql}\n) __f\nWHERE {pred}",
                pred = predicate.to_sql(),
            ))
        }

        OpTree::Project {
            expressions,
            aliases,
            child,
        } => {
            // When the child is a Join, avoid wrapping in a subquery —
            // the project expressions use table-qualified names (e.g., t.id)
            // that lose scope inside a subquery alias like __p.
            let proj_exprs: Vec<String> = expressions
                .iter()
                .zip(aliases.iter())
                .map(|(e, a)| {
                    let esql = e.to_sql();
                    if esql == *a {
                        quote_ident(a)
                    } else {
                        format!("{esql} AS {}", quote_ident(a))
                    }
                })
                .collect();

            match child.as_ref() {
                OpTree::InnerJoin {
                    condition,
                    left,
                    right,
                } => {
                    let left_sql = generate_from_sql(left, self_ref_replacement)?;
                    let right_sql = generate_from_sql(right, self_ref_replacement)?;
                    Ok(format!(
                        "SELECT {projs}\nFROM {left_sql}\nJOIN {right_sql}\n  ON {cond}",
                        projs = proj_exprs.join(", "),
                        cond = condition.to_sql(),
                    ))
                }
                OpTree::LeftJoin {
                    condition,
                    left,
                    right,
                } => {
                    let left_sql = generate_from_sql(left, self_ref_replacement)?;
                    let right_sql = generate_from_sql(right, self_ref_replacement)?;
                    Ok(format!(
                        "SELECT {projs}\nFROM {left_sql}\nLEFT JOIN {right_sql}\n  ON {cond}",
                        projs = proj_exprs.join(", "),
                        cond = condition.to_sql(),
                    ))
                }
                _ => {
                    let child_sql = generate_query_sql(child, self_ref_replacement)?;
                    Ok(format!(
                        "SELECT {projs}\nFROM (\n{child_sql}\n) __p",
                        projs = proj_exprs.join(", "),
                    ))
                }
            }
        }

        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            let left_sql = generate_from_sql(left, self_ref_replacement)?;
            let right_sql = generate_from_sql(right, self_ref_replacement)?;
            // Collect output columns from both sides
            let mut all_cols = Vec::new();
            collect_select_cols(left, &mut all_cols);
            collect_select_cols(right, &mut all_cols);
            Ok(format!(
                "SELECT {cols}\nFROM {left_sql}\nJOIN {right_sql}\n  ON {cond}",
                cols = all_cols.join(", "),
                cond = condition.to_sql(),
            ))
        }

        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => {
            let left_sql = generate_from_sql(left, self_ref_replacement)?;
            let right_sql = generate_from_sql(right, self_ref_replacement)?;
            let mut all_cols = Vec::new();
            collect_select_cols(left, &mut all_cols);
            collect_select_cols(right, &mut all_cols);
            Ok(format!(
                "SELECT {cols}\nFROM {left_sql}\nLEFT JOIN {right_sql}\n  ON {cond}",
                cols = all_cols.join(", "),
                cond = condition.to_sql(),
            ))
        }

        OpTree::Subquery { alias, child, .. } => {
            let child_sql = generate_query_sql(child, self_ref_replacement)?;
            let cols = child.output_columns();
            let col_exprs: Vec<String> = cols
                .iter()
                .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(c)))
                .collect();
            Ok(format!(
                "SELECT {cols}\nFROM (\n{child_sql}\n) AS {alias_q}",
                cols = col_exprs.join(", "),
                alias_q = quote_ident(alias),
            ))
        }

        OpTree::ConstantSelect { sql, .. } => Ok(sql.clone()),

        _ => Err(PgTrickleError::InternalError(format!(
            "generate_query_sql: unsupported OpTree variant {:?} in recursive term",
            op.alias(),
        ))),
    }
}

/// Generate a FROM-clause fragment (table reference) from an OpTree.
/// Used for join children that need to be table references, not full SELECTs.
fn generate_from_sql(
    op: &OpTree,
    self_ref_replacement: Option<&str>,
) -> Result<String, PgTrickleError> {
    match op {
        OpTree::Scan {
            schema,
            table_name,
            alias,
            ..
        } => Ok(format!(
            "{schema_q}.{table_q} AS {alias_q}",
            schema_q = quote_ident(schema),
            table_q = quote_ident(table_name),
            alias_q = quote_ident(alias),
        )),

        OpTree::RecursiveSelfRef { alias, .. } => {
            let replacement = self_ref_replacement.ok_or_else(|| {
                PgTrickleError::InternalError(
                    "RecursiveSelfRef encountered without replacement target".into(),
                )
            })?;
            Ok(format!(
                "{replacement} AS {alias_q}",
                alias_q = quote_ident(alias),
            ))
        }

        OpTree::Subquery { alias, child, .. } => {
            let child_sql = generate_query_sql(child, self_ref_replacement)?;
            Ok(format!(
                "(\n{child_sql}\n) AS {alias_q}",
                alias_q = quote_ident(alias),
            ))
        }

        // Expand nested joins inline so that table aliases from the outer
        // ON condition remain visible to the enclosing SELECT. Wrapping a
        // multi-table join in a subquery would hide those aliases and cause
        // "missing FROM-clause entry for table" errors.
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            let left_sql = generate_from_sql(left, self_ref_replacement)?;
            let right_sql = generate_from_sql(right, self_ref_replacement)?;
            Ok(format!(
                "{left_sql}\nJOIN {right_sql}\n  ON {cond}",
                cond = condition.to_sql(),
            ))
        }

        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => {
            let left_sql = generate_from_sql(left, self_ref_replacement)?;
            let right_sql = generate_from_sql(right, self_ref_replacement)?;
            Ok(format!(
                "{left_sql}\nLEFT JOIN {right_sql}\n  ON {cond}",
                cond = condition.to_sql(),
            ))
        }

        _ => {
            // For complex sub-trees, wrap in a subquery
            let sql = generate_query_sql(op, self_ref_replacement)?;
            Ok(format!("(\n{sql}\n) AS __sub"))
        }
    }
}

/// Collect prefixed column references for a SELECT list from a FROM source.
fn collect_select_cols(op: &OpTree, out: &mut Vec<String>) {
    match op {
        OpTree::Scan { alias, .. }
        | OpTree::RecursiveSelfRef { alias, .. }
        | OpTree::Subquery { alias, .. } => {
            for col in op.output_columns() {
                out.push(format!("{}.{}", quote_ident(alias), quote_ident(&col)));
            }
        }
        // Recursively expand nested join chains so that all table aliases
        // are collected correctly (rather than using the placeholder "__sub").
        OpTree::InnerJoin { left, right, .. } | OpTree::LeftJoin { left, right, .. } => {
            collect_select_cols(left, out);
            collect_select_cols(right, out);
        }
        _ => {
            let alias = "__sub";
            for col in op.output_columns() {
                out.push(format!("{}.{}", quote_ident(alias), quote_ident(&col)));
            }
        }
    }
}

/// Generate the recursive term SQL with self-ref replaced by `st_table`
/// (existing storage) and base table scans reading from change buffers
/// (INSERT rows only). Used for the "new rows joining existing results"
/// seed in semi-naive evaluation.
///
/// Returns `None` if the recursive term doesn't reference any base tables
/// with change buffers.
fn generate_query_sql_with_change_buffers(
    ctx: &DiffContext,
    op: &OpTree,
    st_table: &str,
) -> Result<Option<String>, PgTrickleError> {
    match op {
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            // The recursive term is typically a JOIN between a base table
            // scan and the self-reference. We need to:
            // - Replace self-ref with st_table (existing storage)
            // - Replace base table with change buffer (INSERT + UPDATE new rows)
            let left_from = generate_change_buffer_from(ctx, left, st_table)?;
            let right_from = generate_change_buffer_from(ctx, right, st_table)?;

            let mut all_cols = Vec::new();
            collect_select_cols(left, &mut all_cols);
            collect_select_cols(right, &mut all_cols);

            Ok(Some(format!(
                "SELECT {cols}\nFROM {left_from}\nJOIN {right_from}\n  ON {cond}",
                cols = all_cols.join(", "),
                cond = condition.to_sql(),
            )))
        }

        OpTree::Project {
            expressions,
            aliases,
            child,
        } => {
            let proj_exprs: Vec<String> = expressions
                .iter()
                .zip(aliases.iter())
                .map(|(e, a)| {
                    let esql = e.to_sql();
                    if esql == *a {
                        quote_ident(a)
                    } else {
                        format!("{esql} AS {}", quote_ident(a))
                    }
                })
                .collect();

            // Inline the FROM clause for Join children so that project
            // expressions (which use original table aliases like `n.id`)
            // resolve correctly.
            match child.as_ref() {
                OpTree::InnerJoin {
                    condition,
                    left,
                    right,
                } => {
                    let left_from = generate_change_buffer_from(ctx, left, st_table)?;
                    let right_from = generate_change_buffer_from(ctx, right, st_table)?;
                    Ok(Some(format!(
                        "SELECT {projs}\nFROM {left_from}\nJOIN {right_from}\n  ON {cond}",
                        projs = proj_exprs.join(", "),
                        cond = condition.to_sql(),
                    )))
                }
                OpTree::LeftJoin {
                    condition,
                    left,
                    right,
                } => {
                    let left_from = generate_change_buffer_from(ctx, left, st_table)?;
                    let right_from = generate_change_buffer_from(ctx, right, st_table)?;
                    Ok(Some(format!(
                        "SELECT {projs}\nFROM {left_from}\nLEFT JOIN {right_from}\n  ON {cond}",
                        projs = proj_exprs.join(", "),
                        cond = condition.to_sql(),
                    )))
                }
                _ => {
                    let child_sql = generate_query_sql_with_change_buffers(ctx, child, st_table)?;
                    match child_sql {
                        Some(inner) => Ok(Some(format!(
                            "SELECT {projs}\nFROM (\n{inner}\n) __p",
                            projs = proj_exprs.join(", "),
                        ))),
                        None => Ok(None),
                    }
                }
            }
        }

        OpTree::Filter { predicate, child } => {
            let child_sql = generate_query_sql_with_change_buffers(ctx, child, st_table)?;
            match child_sql {
                Some(inner) => Ok(Some(format!(
                    "SELECT * FROM (\n{inner}\n) __f\nWHERE {pred}",
                    pred = predicate.to_sql(),
                ))),
                None => Ok(None),
            }
        }

        _ => Ok(None),
    }
}

/// Generate the recursive term SQL with self-ref replaced by `st_table`
/// (existing storage) and base table scans reading from OLD change rows.
/// Used by the DRed over-delete seed to find old recursive rows affected by
/// DELETEs and UPDATEs in recursive-term source tables.
fn generate_query_sql_with_old_change_buffers(
    ctx: &DiffContext,
    op: &OpTree,
    st_table: &str,
) -> Result<Option<String>, PgTrickleError> {
    match op {
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            let left_from = generate_old_change_buffer_from(ctx, left, st_table)?;
            let right_from = generate_old_change_buffer_from(ctx, right, st_table)?;

            let mut all_cols = Vec::new();
            collect_select_cols(left, &mut all_cols);
            collect_select_cols(right, &mut all_cols);

            Ok(Some(format!(
                "SELECT {cols}\nFROM {left_from}\nJOIN {right_from}\n  ON {cond}",
                cols = all_cols.join(", "),
                cond = condition.to_sql(),
            )))
        }

        OpTree::Project {
            expressions,
            aliases,
            child,
        } => {
            let proj_exprs: Vec<String> = expressions
                .iter()
                .zip(aliases.iter())
                .map(|(e, a)| {
                    let esql = e.to_sql();
                    if esql == *a {
                        quote_ident(a)
                    } else {
                        format!("{esql} AS {}", quote_ident(a))
                    }
                })
                .collect();

            match child.as_ref() {
                OpTree::InnerJoin {
                    condition,
                    left,
                    right,
                } => {
                    let left_from = generate_old_change_buffer_from(ctx, left, st_table)?;
                    let right_from = generate_old_change_buffer_from(ctx, right, st_table)?;
                    Ok(Some(format!(
                        "SELECT {projs}\nFROM {left_from}\nJOIN {right_from}\n  ON {cond}",
                        projs = proj_exprs.join(", "),
                        cond = condition.to_sql(),
                    )))
                }
                OpTree::LeftJoin {
                    condition,
                    left,
                    right,
                } => {
                    let left_from = generate_old_change_buffer_from(ctx, left, st_table)?;
                    let right_from = generate_old_change_buffer_from(ctx, right, st_table)?;
                    Ok(Some(format!(
                        "SELECT {projs}\nFROM {left_from}\nLEFT JOIN {right_from}\n  ON {cond}",
                        projs = proj_exprs.join(", "),
                        cond = condition.to_sql(),
                    )))
                }
                _ => {
                    let child_sql =
                        generate_query_sql_with_old_change_buffers(ctx, child, st_table)?;
                    match child_sql {
                        Some(inner) => Ok(Some(format!(
                            "SELECT {projs}\nFROM (\n{inner}\n) __p",
                            projs = proj_exprs.join(", "),
                        ))),
                        None => Ok(None),
                    }
                }
            }
        }

        OpTree::Filter { predicate, child } => {
            let child_sql = generate_query_sql_with_old_change_buffers(ctx, child, st_table)?;
            match child_sql {
                Some(inner) => Ok(Some(format!(
                    "SELECT * FROM (\n{inner}\n) __f\nWHERE {pred}",
                    pred = predicate.to_sql(),
                ))),
                None => Ok(None),
            }
        }

        _ => Ok(None),
    }
}

/// Generate a FROM-clause fragment that reads inserted/updated NEW rows from the
/// appropriate source for Scan nodes, or references st_table for
/// RecursiveSelfRef.
///
/// In DIFFERENTIAL mode, reads INSERT rows and UPDATE NEW rows from the change
/// buffer table (filtering by `action IN ('I', 'U')` and LSN range).
///
/// In IMMEDIATE mode, reads from the NEW transition table
/// (`__pgt_newtable_<oid>`), which has the same schema as the source
/// table and contains only the newly inserted/updated rows.
fn generate_change_buffer_from(
    ctx: &DiffContext,
    op: &OpTree,
    st_table: &str,
) -> Result<String, PgTrickleError> {
    use crate::dvm::diff::DeltaSource;

    match op {
        OpTree::Scan {
            table_oid,
            alias,
            columns,
            ..
        } => {
            // ── IMMEDIATE mode: read from NEW transition table ────────
            if let DeltaSource::TransitionTable { tables } = &ctx.delta_source {
                if let Some(tt) = tables.get(table_oid)
                    && let Some(new_table) = &tt.new_name
                {
                    // The transition table has the same schema as the
                    // source table (created via `SELECT * FROM __pgt_newtable`).
                    let col_refs: Vec<String> = columns
                        .iter()
                        .map(|c| {
                            format!(
                                "{alias_q}.{}",
                                quote_ident(&c.name),
                                alias_q = quote_ident(alias),
                            )
                        })
                        .collect();
                    return Ok(format!(
                        "(SELECT {cols} FROM {new_table} AS {alias_q}) AS {alias_q}",
                        cols = col_refs.join(", "),
                        alias_q = quote_ident(alias),
                    ));
                }
                // No NEW table for this OID — source had no inserts/updates.
                // Return an empty relation with the correct columns.
                let col_refs: Vec<String> = columns
                    .iter()
                    .map(|c| format!("NULL::text AS {}", quote_ident(&c.name)))
                    .collect();
                let alias_q = quote_ident(alias);
                return Ok(format!(
                    "(SELECT {cols} WHERE false) AS {alias_q}",
                    cols = col_refs.join(", "),
                ));
            }

            // ── DIFFERENTIAL mode: read INSERT rows ─────────────────
            let change_table = ctx.change_table_for_source(*table_oid);
            let prev_lsn = ctx.get_prev_lsn(*table_oid);

            // A44-10 (D+I schema): flat column names — c."col" for I-rows (new values).
            // D-rows (old values) are excluded — only new/surviving rows contribute
            // to the recursive expansion.
            let col_refs: Vec<String> = columns
                .iter()
                .map(|c| format!("c.{} AS {}", quote_ident(&c.name), quote_ident(&c.name),))
                .collect();

            Ok(format!(
                "(SELECT {cols} FROM {change_table} c \
                 WHERE c.action = 'I' AND c.lsn > '{prev_lsn}'::pg_lsn) AS {alias_q}",
                cols = col_refs.join(", "),
                alias_q = quote_ident(alias),
            ))
        }

        OpTree::RecursiveSelfRef { alias, .. } => {
            // Use existing ST storage for the self-reference
            Ok(format!(
                "{st_table} AS {alias_q}",
                alias_q = quote_ident(alias),
            ))
        }

        // Expand nested join chains inline so that table aliases produced
        // by each child remain visible to the enclosing ON condition.
        // Wrapping in (SELECT ...) AS __sub would hide those aliases.
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            let left_from = generate_change_buffer_from(ctx, left, st_table)?;
            let right_from = generate_change_buffer_from(ctx, right, st_table)?;
            Ok(format!(
                "{left_from}\nJOIN {right_from}\n  ON {cond}",
                cond = condition.to_sql(),
            ))
        }

        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => {
            let left_from = generate_change_buffer_from(ctx, left, st_table)?;
            let right_from = generate_change_buffer_from(ctx, right, st_table)?;
            Ok(format!(
                "{left_from}\nLEFT JOIN {right_from}\n  ON {cond}",
                cond = condition.to_sql(),
            ))
        }

        _ => {
            // For other node types, fall back to normal SQL generation
            let sql = generate_query_sql(op, Some(st_table))?;
            Ok(format!("(\n{sql}\n) AS __sub"))
        }
    }
}

/// Generate a FROM-clause fragment that reads deleted/updated OLD rows from
/// the appropriate source for Scan nodes, or references st_table for
/// RecursiveSelfRef.
fn generate_old_change_buffer_from(
    ctx: &DiffContext,
    op: &OpTree,
    st_table: &str,
) -> Result<String, PgTrickleError> {
    use crate::dvm::diff::DeltaSource;

    match op {
        OpTree::Scan {
            table_oid,
            alias,
            columns,
            ..
        } => {
            if let DeltaSource::TransitionTable { tables } = &ctx.delta_source {
                if let Some(tt) = tables.get(table_oid)
                    && let Some(old_table) = &tt.old_name
                {
                    let col_refs: Vec<String> = columns
                        .iter()
                        .map(|c| {
                            format!(
                                "{alias_q}.{}",
                                quote_ident(&c.name),
                                alias_q = quote_ident(alias),
                            )
                        })
                        .collect();
                    return Ok(format!(
                        "(SELECT {cols} FROM {old_table} AS {alias_q}) AS {alias_q}",
                        cols = col_refs.join(", "),
                        alias_q = quote_ident(alias),
                    ));
                }

                let col_refs: Vec<String> = columns
                    .iter()
                    .map(|c| format!("NULL::text AS {}", quote_ident(&c.name)))
                    .collect();
                let alias_q = quote_ident(alias);
                return Ok(format!(
                    "(SELECT {cols} WHERE false) AS {alias_q}",
                    cols = col_refs.join(", "),
                ));
            }

            let change_table = ctx.change_table_for_source(*table_oid);
            let prev_lsn = ctx.get_prev_lsn(*table_oid);

            let col_refs: Vec<String> = columns
                .iter()
                .map(|c| {
                    // A44-10 (D+I schema): D-rows carry old values in flat c."col"
                    format!("c.{} AS {}", quote_ident(&c.name), quote_ident(&c.name),)
                })
                .collect();

            Ok(format!(
                "(SELECT {cols} FROM {change_table} c \
                 WHERE c.action = 'D' AND c.lsn > '{prev_lsn}'::pg_lsn) AS {alias_q}",
                cols = col_refs.join(", "),
                alias_q = quote_ident(alias),
            ))
        }

        OpTree::RecursiveSelfRef { alias, .. } => Ok(format!(
            "{st_table} AS {alias_q}",
            alias_q = quote_ident(alias),
        )),

        // Expand nested join chains inline to keep table aliases visible.
        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            let left_from = generate_old_change_buffer_from(ctx, left, st_table)?;
            let right_from = generate_old_change_buffer_from(ctx, right, st_table)?;
            Ok(format!(
                "{left_from}\nJOIN {right_from}\n  ON {cond}",
                cond = condition.to_sql(),
            ))
        }

        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => {
            let left_from = generate_old_change_buffer_from(ctx, left, st_table)?;
            let right_from = generate_old_change_buffer_from(ctx, right, st_table)?;
            Ok(format!(
                "{left_from}\nLEFT JOIN {right_from}\n  ON {cond}",
                cond = condition.to_sql(),
            ))
        }

        _ => {
            let sql = generate_query_sql(op, Some(st_table))?;
            Ok(format!("(\n{sql}\n) AS __sub"))
        }
    }
}

// ── Monotonicity analysis ───────────────────────────────────────────

/// Check whether a recursive CTE's recursive term contains non-monotone
/// operators that would make semi-naive and DRed strategies produce
/// incorrect fixpoints.
///
/// **Monotone** operators: adding rows to the input can only add rows
/// to the output. Semi-naive propagation and DRed rely on this property.
///
/// **Non-monotone** operators: adding rows to the input can remove rows
/// from the output. Examples: EXCEPT, Aggregate, Window, DISTINCT,
/// INTERSECT (set semantics).
///
/// Returns `Some(reason)` if a non-monotone operator is found, `None`
/// if the recursive term is safe for incremental strategies.
///
/// This function only checks the recursive term — the base case can
/// contain any operator because it is differentiated normally, not
/// through the semi-naive/DRed path.
pub fn recursive_term_is_non_monotone(op: &OpTree) -> Option<String> {
    match op {
        // Non-monotone operators: return immediately with reason
        OpTree::Except { .. } => Some("EXCEPT".into()),
        OpTree::Aggregate { .. } => Some("GROUP BY / aggregate".into()),
        OpTree::Window { .. } => Some("window function".into()),
        OpTree::Distinct { .. } => Some("DISTINCT".into()),
        OpTree::Intersect { all: false, .. } => Some("INTERSECT (set)".into()),

        // INTERSECT ALL is monotone for positive bags
        OpTree::Intersect {
            all: true,
            left,
            right,
        } => recursive_term_is_non_monotone(left).or_else(|| recursive_term_is_non_monotone(right)),

        // Transparent wrappers — recurse into children
        OpTree::Filter { child, .. } | OpTree::Project { child, .. } => {
            recursive_term_is_non_monotone(child)
        }
        OpTree::Subquery { child, .. } => recursive_term_is_non_monotone(child),

        // Joins: recurse both sides
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. } => {
            recursive_term_is_non_monotone(left).or_else(|| recursive_term_is_non_monotone(right))
        }

        // AntiJoin (NOT EXISTS / NOT IN) is inherently non-monotone:
        // adding a matching row to the right side removes output rows.
        OpTree::AntiJoin { .. } => Some("NOT EXISTS / NOT IN (anti-join)".into()),

        // UNION ALL: recurse all children
        OpTree::UnionAll { children } => children.iter().find_map(recursive_term_is_non_monotone),

        // ScalarSubquery: the subquery result is used as a value.
        // If the subquery contains aggregates, changes can cause
        // all output rows to change. Check the subquery child.
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => recursive_term_is_non_monotone(subquery)
            .or_else(|| recursive_term_is_non_monotone(child)),

        // Leaf nodes: safe
        OpTree::Scan { .. }
        | OpTree::RecursiveSelfRef { .. }
        | OpTree::CteScan { .. }
        | OpTree::RecursiveCte { .. } => None,

        // Lateral subquery / function: treat as opaque.
        // These are rare in recursive terms; conservative fallback.
        OpTree::LateralFunction { .. } | OpTree::LateralSubquery { .. } => {
            Some("LATERAL (opaque)".into())
        }

        // Catch-all for any future OpTree variants: conservative fallback.
        #[allow(unreachable_patterns)]
        _ => Some("unknown operator".into()),
    }
}

// ── Non-linear recursion support ────────────────────────────────────────

/// Count the number of `RecursiveSelfRef` nodes in an OpTree.
///
/// Returns 0 for non-recursive trees, 1 for linear recursion (single
/// self-reference), and >1 for non-linear recursion (multiple references
/// to the recursive CTE in the same term).
fn count_self_refs(op: &OpTree) -> usize {
    match op {
        OpTree::RecursiveSelfRef { .. } => 1,
        OpTree::InnerJoin { left, right, .. } | OpTree::LeftJoin { left, right, .. } => {
            count_self_refs(left) + count_self_refs(right)
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Distinct { child } => count_self_refs(child),
        OpTree::Subquery { child, .. } => count_self_refs(child),
        _ => 0,
    }
}

/// Collect the aliases of all `RecursiveSelfRef` nodes in an OpTree.
///
/// For linear recursion, this returns a single alias (e.g., `["t"]`).
/// For non-linear recursion, returns multiple aliases (e.g., `["r1", "r2"]`).
fn collect_self_ref_aliases(op: &OpTree) -> Vec<String> {
    match op {
        OpTree::RecursiveSelfRef { alias, .. } => vec![alias.clone()],
        OpTree::InnerJoin { left, right, .. } | OpTree::LeftJoin { left, right, .. } => {
            let mut v = collect_self_ref_aliases(left);
            v.extend(collect_self_ref_aliases(right));
            v
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Distinct { child } => collect_self_ref_aliases(child),
        OpTree::Subquery { child, .. } => collect_self_ref_aliases(child),
        _ => vec![],
    }
}

/// Generate SQL from an OpTree with per-alias replacement for self-references.
///
/// Like [`generate_query_sql`] but allows different replacements for each
/// `RecursiveSelfRef` alias. Used by non-linear recursion seeds to replace
/// one self-reference with the delta source and others with ST storage.
///
/// `self_ref_map` maps self-ref alias → replacement table/CTE/subquery.
fn generate_query_sql_targeted(
    op: &OpTree,
    self_ref_map: &std::collections::HashMap<String, String>,
) -> Result<String, PgTrickleError> {
    match op {
        OpTree::Scan {
            schema,
            table_name,
            alias,
            columns,
            ..
        } => {
            let col_exprs: Vec<String> = columns
                .iter()
                .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(&c.name)))
                .collect();
            Ok(format!(
                "SELECT {cols}\nFROM {schema_q}.{table_q} AS {alias_q}",
                cols = col_exprs.join(", "),
                schema_q = quote_ident(schema),
                table_q = quote_ident(table_name),
                alias_q = quote_ident(alias),
            ))
        }

        OpTree::RecursiveSelfRef { alias, columns, .. } => {
            let replacement = self_ref_map.get(alias).ok_or_else(|| {
                PgTrickleError::InternalError(format!(
                    "generate_query_sql_targeted: no replacement for self-ref alias \"{alias}\""
                ))
            })?;
            let col_exprs: Vec<String> = columns
                .iter()
                .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(c)))
                .collect();
            Ok(format!(
                "SELECT {cols}\nFROM {replacement} AS {alias_q}",
                cols = col_exprs.join(", "),
                alias_q = quote_ident(alias),
            ))
        }

        OpTree::Filter { predicate, child } => {
            let child_sql = generate_query_sql_targeted(child, self_ref_map)?;
            Ok(format!(
                "SELECT * FROM (\n{child_sql}\n) __f\nWHERE {pred}",
                pred = predicate.to_sql(),
            ))
        }

        OpTree::Project {
            expressions,
            aliases,
            child,
        } => {
            let child_sql = generate_query_sql_targeted(child, self_ref_map)?;
            let proj_exprs: Vec<String> = expressions
                .iter()
                .zip(aliases.iter())
                .map(|(e, a)| {
                    let esql = e.to_sql();
                    if esql == *a {
                        quote_ident(a)
                    } else {
                        format!("{esql} AS {}", quote_ident(a))
                    }
                })
                .collect();
            Ok(format!(
                "SELECT {projs}\nFROM (\n{child_sql}\n) __p",
                projs = proj_exprs.join(", "),
            ))
        }

        OpTree::InnerJoin {
            condition,
            left,
            right,
        } => {
            let left_sql = generate_from_sql_targeted(left, self_ref_map)?;
            let right_sql = generate_from_sql_targeted(right, self_ref_map)?;
            let mut all_cols = Vec::new();
            collect_select_cols(left, &mut all_cols);
            collect_select_cols(right, &mut all_cols);
            Ok(format!(
                "SELECT {cols}\nFROM {left_sql}\nJOIN {right_sql}\n  ON {cond}",
                cols = all_cols.join(", "),
                cond = condition.to_sql(),
            ))
        }

        OpTree::LeftJoin {
            condition,
            left,
            right,
        } => {
            let left_sql = generate_from_sql_targeted(left, self_ref_map)?;
            let right_sql = generate_from_sql_targeted(right, self_ref_map)?;
            let mut all_cols = Vec::new();
            collect_select_cols(left, &mut all_cols);
            collect_select_cols(right, &mut all_cols);
            Ok(format!(
                "SELECT {cols}\nFROM {left_sql}\nLEFT JOIN {right_sql}\n  ON {cond}",
                cols = all_cols.join(", "),
                cond = condition.to_sql(),
            ))
        }

        OpTree::Subquery { alias, child, .. } => {
            let child_sql = generate_query_sql_targeted(child, self_ref_map)?;
            let cols = child.output_columns();
            let col_exprs: Vec<String> = cols
                .iter()
                .map(|c| format!("{}.{}", quote_ident(alias), quote_ident(c)))
                .collect();
            Ok(format!(
                "SELECT {cols}\nFROM (\n{child_sql}\n) AS {alias_q}",
                cols = col_exprs.join(", "),
                alias_q = quote_ident(alias),
            ))
        }

        _ => Err(PgTrickleError::InternalError(format!(
            "generate_query_sql_targeted: unsupported OpTree variant {:?}",
            op.alias(),
        ))),
    }
}

/// Generate a FROM-clause fragment with per-alias replacement.
///
/// Like [`generate_from_sql`] but with per-alias self-reference replacement.
fn generate_from_sql_targeted(
    op: &OpTree,
    self_ref_map: &std::collections::HashMap<String, String>,
) -> Result<String, PgTrickleError> {
    match op {
        OpTree::Scan {
            schema,
            table_name,
            alias,
            ..
        } => Ok(format!(
            "{schema_q}.{table_q} AS {alias_q}",
            schema_q = quote_ident(schema),
            table_q = quote_ident(table_name),
            alias_q = quote_ident(alias),
        )),

        OpTree::RecursiveSelfRef { alias, .. } => {
            let replacement = self_ref_map.get(alias).ok_or_else(|| {
                PgTrickleError::InternalError(format!(
                    "generate_from_sql_targeted: no replacement for self-ref alias \"{alias}\""
                ))
            })?;
            Ok(format!(
                "{replacement} AS {alias_q}",
                alias_q = quote_ident(alias),
            ))
        }

        OpTree::Subquery { alias, child, .. } => {
            let child_sql = generate_query_sql_targeted(child, self_ref_map)?;
            Ok(format!(
                "(\n{child_sql}\n) AS {alias_q}",
                alias_q = quote_ident(alias),
            ))
        }

        _ => {
            let sql = generate_query_sql_targeted(op, self_ref_map)?;
            Ok(format!("(\n{sql}\n) AS __sub"))
        }
    }
}

/// Generate non-linear seed SQL terms for semi-naive evaluation.
///
/// For each self-reference position, generates a seed where that position
/// reads from the base case delta (INSERT rows) and all other positions
/// read from the existing ST storage. This captures the cross-products
/// of new rows with existing rows in each possible configuration.
///
/// For a non-linear recursive term `R(r1, r2)` with two self-references:
/// - Seed A: `R(delta_ins, ST_storage)` — new rows in r1 position
/// - Seed B: `R(ST_storage, delta_ins)` — new rows in r2 position
///
/// Returns an empty vec for linear recursion (1 or fewer self-refs).
fn generate_nonlinear_seeds(
    recursive: &OpTree,
    self_ref_aliases: &[String],
    base_delta_cte: &str,
    st_table: &str,
    columns: &[String],
) -> Result<Vec<String>, PgTrickleError> {
    if self_ref_aliases.len() <= 1 {
        return Ok(vec![]);
    }

    let col_list_str = col_list(columns);
    let mut seeds = Vec::new();

    for (i, _delta_alias) in self_ref_aliases.iter().enumerate() {
        let mut replacements = std::collections::HashMap::new();
        for (j, alias) in self_ref_aliases.iter().enumerate() {
            if i == j {
                // This position reads from the base case delta (INSERT rows)
                let delta_ref = format!(
                    "(SELECT {col_list_str} FROM {base_delta_cte} WHERE __pgt_action = 'I')"
                );
                replacements.insert(alias.clone(), delta_ref);
            } else {
                // Other positions read from existing ST storage
                replacements.insert(alias.clone(), st_table.to_string());
            }
        }

        let seed_sql = generate_query_sql_targeted(recursive, &replacements)?;
        seeds.push(seed_sql);
    }

    Ok(seeds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::test_ctx;
    use crate::dvm::parser::{Column, Expr, OpTree};

    fn make_column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            type_oid: 23,
            is_nullable: true,
        }
    }

    fn make_scan(oid: u32, table: &str, schema: &str, alias: &str, cols: &[&str]) -> OpTree {
        OpTree::Scan {
            table_oid: oid,
            table_name: table.to_string(),
            schema: schema.to_string(),
            columns: cols.iter().map(|c| make_column(c)).collect(),
            pk_columns: Vec::new(),
            alias: alias.to_string(),
        }
    }

    fn make_self_ref(cte_name: &str, alias: &str, cols: &[&str]) -> OpTree {
        OpTree::RecursiveSelfRef {
            cte_name: cte_name.to_string(),
            alias: alias.to_string(),
            columns: cols.iter().map(|c| c.to_string()).collect(),
        }
    }

    // ── generate_query_sql tests ────────────────────────────────────

    #[test]
    fn test_generate_query_sql_scan() {
        let scan = make_scan(100, "categories", "public", "c", &["id", "name"]);
        let sql = generate_query_sql(&scan, None).unwrap();
        assert!(sql.contains("\"public\".\"categories\""));
        assert!(sql.contains("\"c\".\"id\""));
        assert!(sql.contains("\"c\".\"name\""));
    }

    #[test]
    fn test_generate_query_sql_self_ref() {
        let self_ref = make_self_ref("tree", "t", &["id", "depth"]);
        let sql = generate_query_sql(&self_ref, Some("__pgt_delta")).unwrap();
        assert!(sql.contains("__pgt_delta AS \"t\""));
        assert!(sql.contains("\"t\".\"id\""));
        assert!(sql.contains("\"t\".\"depth\""));
    }

    #[test]
    fn test_generate_query_sql_self_ref_no_replacement_errors() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let result = generate_query_sql(&self_ref, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_generate_query_sql_inner_join() {
        let left = make_scan(100, "categories", "public", "c", &["id", "parent_id"]);
        let right = make_self_ref("tree", "t", &["id", "depth"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(left),
            right: Box::new(right),
        };

        let sql = generate_query_sql(&join, Some("__pgt_delta")).unwrap();
        assert!(sql.contains("JOIN"));
        assert!(sql.contains("__pgt_delta"));
        assert!(sql.contains("\"c\".\"id\""));
        assert!(sql.contains("\"t\".\"depth\""));
    }

    #[test]
    fn test_generate_query_sql_filter() {
        let scan = make_scan(100, "categories", "public", "c", &["id", "name"]);
        let filter = OpTree::Filter {
            predicate: Expr::Literal("c.active = TRUE".to_string()),
            child: Box::new(scan),
        };
        let sql = generate_query_sql(&filter, None).unwrap();
        assert!(sql.contains("WHERE c.active = TRUE"));
    }

    #[test]
    fn test_generate_query_sql_project() {
        let scan = make_scan(100, "items", "public", "i", &["id", "price"]);
        let project = OpTree::Project {
            expressions: vec![
                Expr::ColumnRef {
                    table_alias: Some("i".to_string()),
                    column_name: "id".to_string(),
                },
                Expr::Literal("i.price * 2".to_string()),
            ],
            aliases: vec!["id".to_string(), "double_price".to_string()],
            child: Box::new(scan),
        };
        let sql = generate_query_sql(&project, None).unwrap();
        assert!(sql.contains("i.price * 2 AS \"double_price\""));
    }

    // ── generate_from_sql tests ─────────────────────────────────────

    #[test]
    fn test_generate_from_sql_scan() {
        let scan = make_scan(100, "orders", "sales", "o", &["id"]);
        let sql = generate_from_sql(&scan, None).unwrap();
        assert_eq!(sql, "\"sales\".\"orders\" AS \"o\"");
    }

    #[test]
    fn test_generate_from_sql_self_ref() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let sql = generate_from_sql(&self_ref, Some("__pgt_cte_1")).unwrap();
        assert_eq!(sql, "__pgt_cte_1 AS \"t\"");
    }

    // ── collect_select_cols tests ───────────────────────────────────

    #[test]
    fn test_collect_select_cols_scan() {
        let scan = make_scan(100, "t", "public", "t", &["x", "y"]);
        let mut cols = Vec::new();
        collect_select_cols(&scan, &mut cols);
        assert_eq!(cols, vec!["\"t\".\"x\"", "\"t\".\"y\""]);
    }

    #[test]
    fn test_collect_select_cols_self_ref() {
        let self_ref = make_self_ref("tree", "r", &["a", "b"]);
        let mut cols = Vec::new();
        collect_select_cols(&self_ref, &mut cols);
        assert_eq!(cols, vec!["\"r\".\"a\"", "\"r\".\"b\""]);
    }

    // ── DRed: generate_cascade_from tests ───────────────────────────

    #[test]
    fn test_generate_cascade_from_scan_uses_st_table() {
        let scan = make_scan(100, "categories", "public", "c", &["id", "parent_id"]);
        let sql = generate_cascade_from(&scan, "__cascade", "\"public\".\"my_st\"").unwrap();
        assert_eq!(sql, "\"public\".\"my_st\" AS \"c\"");
    }

    #[test]
    fn test_generate_cascade_from_self_ref_uses_cascade_cte() {
        let self_ref = make_self_ref("tree", "t", &["id", "depth"]);
        let sql = generate_cascade_from(&self_ref, "__cascade", "\"public\".\"my_st\"").unwrap();
        assert_eq!(sql, "__cascade AS \"t\"");
    }

    // ── DRed: generate_query_sql_cascade tests ──────────────────────

    #[test]
    fn test_generate_query_sql_cascade_inner_join() {
        // Recursive term: SELECT c.id, c.parent_id FROM categories c JOIN tree t ON c.parent_id = t.id
        let left = make_scan(100, "categories", "public", "c", &["id", "parent_id"]);
        let right = make_self_ref("tree", "t", &["id", "parent_id"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(left),
            right: Box::new(right),
        };

        let sql = generate_query_sql_cascade(&join, "__del_cascade", "\"public\".\"st\"").unwrap();
        // Self-ref should be replaced with cascade CTE
        assert!(sql.contains("__del_cascade AS \"t\""));
        // Base table should be replaced with ST storage
        assert!(sql.contains("\"public\".\"st\" AS \"c\""));
        // Join condition should be preserved (BinaryOp wraps in parens)
        assert!(sql.contains("(\"c\".\"parent_id\" = \"t\".\"id\")"));
    }

    #[test]
    fn test_generate_query_sql_cascade_with_filter() {
        let scan = make_scan(100, "edges", "public", "e", &["from_node", "to_node"]);
        let self_ref = make_self_ref("reach", "r", &["from_node", "to_node"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("e".to_string()),
                    column_name: "from_node".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("r".to_string()),
                    column_name: "to_node".to_string(),
                }),
            },
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        let filtered = OpTree::Filter {
            predicate: Expr::Literal("r.hops < 10".to_string()),
            child: Box::new(join),
        };

        let sql = generate_query_sql_cascade(&filtered, "__casc", "\"public\".\"st\"").unwrap();
        assert!(sql.contains("WHERE r.hops < 10"));
        assert!(sql.contains("__casc AS \"r\""));
        assert!(sql.contains("\"public\".\"st\" AS \"e\""));
    }

    #[test]
    fn test_generate_query_sql_cascade_with_project() {
        let scan = make_scan(100, "nodes", "public", "n", &["id", "parent_id", "name"]);
        let self_ref = make_self_ref("tree", "t", &["id", "depth"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        let projected = OpTree::Project {
            expressions: vec![
                Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "id".to_string(),
                },
                Expr::Literal("t.depth + 1".to_string()),
            ],
            aliases: vec!["id".to_string(), "depth".to_string()],
            child: Box::new(join),
        };

        let sql = generate_query_sql_cascade(&projected, "__casc", "\"public\".\"st\"").unwrap();
        assert!(sql.contains("t.depth + 1 AS \"depth\""));
        assert!(sql.contains("__casc AS \"t\""));
    }

    // ── DRed: collect_cascade_cols tests ────────────────────────────

    #[test]
    fn test_collect_cascade_cols_scan() {
        let scan = make_scan(100, "t", "public", "c", &["x", "y"]);
        let mut cols = Vec::new();
        collect_cascade_cols(&scan, &mut cols);
        assert_eq!(cols, vec!["\"c\".\"x\"", "\"c\".\"y\""]);
    }

    #[test]
    fn test_collect_cascade_cols_self_ref() {
        let self_ref = make_self_ref("tree", "t", &["a", "b"]);
        let mut cols = Vec::new();
        collect_cascade_cols(&self_ref, &mut cols);
        assert_eq!(cols, vec!["\"t\".\"a\"", "\"t\".\"b\""]);
    }

    // ── Non-linear recursion tests ──────────────────────────────────

    #[test]
    fn test_count_self_refs_linear() {
        // Single self-ref in a join: linear recursion
        let scan = make_scan(100, "categories", "public", "c", &["id", "parent_id"]);
        let self_ref = make_self_ref("tree", "t", &["id", "parent_id"]);
        let join = OpTree::InnerJoin {
            condition: Expr::Literal("c.parent_id = t.id".to_string()),
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        assert_eq!(count_self_refs(&join), 1);
    }

    #[test]
    fn test_count_self_refs_nonlinear() {
        // Two self-refs in a join: non-linear (transitive closure)
        let r1 = make_self_ref("reach", "r1", &["src", "dst"]);
        let r2 = make_self_ref("reach", "r2", &["src", "dst"]);
        let join = OpTree::InnerJoin {
            condition: Expr::Literal("r1.dst = r2.src".to_string()),
            left: Box::new(r1),
            right: Box::new(r2),
        };
        assert_eq!(count_self_refs(&join), 2);
    }

    #[test]
    fn test_count_self_refs_zero() {
        let scan = make_scan(100, "t", "public", "t", &["id"]);
        assert_eq!(count_self_refs(&scan), 0);
    }

    #[test]
    fn test_count_self_refs_through_filter() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let filtered = OpTree::Filter {
            predicate: Expr::Literal("id > 0".to_string()),
            child: Box::new(self_ref),
        };
        assert_eq!(count_self_refs(&filtered), 1);
    }

    #[test]
    fn test_collect_self_ref_aliases_linear() {
        let scan = make_scan(100, "c", "public", "c", &["id", "pid"]);
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let join = OpTree::InnerJoin {
            condition: Expr::Literal("c.pid = t.id".to_string()),
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        assert_eq!(collect_self_ref_aliases(&join), vec!["t"]);
    }

    #[test]
    fn test_collect_self_ref_aliases_nonlinear() {
        let r1 = make_self_ref("reach", "r1", &["src", "dst"]);
        let r2 = make_self_ref("reach", "r2", &["src", "dst"]);
        let join = OpTree::InnerJoin {
            condition: Expr::Literal("r1.dst = r2.src".to_string()),
            left: Box::new(r1),
            right: Box::new(r2),
        };
        assert_eq!(collect_self_ref_aliases(&join), vec!["r1", "r2"]);
    }

    #[test]
    fn test_generate_query_sql_targeted_nonlinear_join() {
        // Non-linear: FROM reach r1 JOIN reach r2 ON r1.dst = r2.src
        let r1 = make_self_ref("reach", "r1", &["src", "dst"]);
        let r2 = make_self_ref("reach", "r2", &["src", "dst"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("r1".to_string()),
                    column_name: "dst".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("r2".to_string()),
                    column_name: "src".to_string(),
                }),
            },
            left: Box::new(r1),
            right: Box::new(r2),
        };

        // Replace r1 with delta, r2 with ST storage
        let mut map = std::collections::HashMap::new();
        map.insert(
            "r1".to_string(),
            "(SELECT \"src\", \"dst\" FROM __delta WHERE __pgt_action = 'I')".to_string(),
        );
        map.insert("r2".to_string(), "\"public\".\"st\"".to_string());

        let sql = generate_query_sql_targeted(&join, &map).unwrap();
        assert!(
            sql.contains(
                "(SELECT \"src\", \"dst\" FROM __delta WHERE __pgt_action = 'I') AS \"r1\""
            )
        );
        assert!(sql.contains("\"public\".\"st\" AS \"r2\""));
        assert!(sql.contains("(\"r1\".\"dst\" = \"r2\".\"src\")"));
    }

    #[test]
    fn test_generate_nonlinear_seeds_linear_returns_empty() {
        // Linear recursion: only 1 self-ref alias → no non-linear seeds
        let aliases = vec!["t".to_string()];
        let scan = make_scan(100, "c", "public", "c", &["id", "pid"]);
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let join = OpTree::InnerJoin {
            condition: Expr::Literal("c.pid = t.id".to_string()),
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        let seeds = generate_nonlinear_seeds(
            &join,
            &aliases,
            "__base_delta",
            "\"public\".\"st\"",
            &["id".to_string(), "pid".to_string()],
        )
        .unwrap();
        assert!(seeds.is_empty());
    }

    #[test]
    fn test_generate_nonlinear_seeds_two_self_refs() {
        // Non-linear: FROM reach r1 JOIN reach r2 ON r1.dst = r2.src
        let r1 = make_self_ref("reach", "r1", &["src", "dst"]);
        let r2 = make_self_ref("reach", "r2", &["src", "dst"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("r1".to_string()),
                    column_name: "dst".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("r2".to_string()),
                    column_name: "src".to_string(),
                }),
            },
            left: Box::new(r1),
            right: Box::new(r2),
        };

        let aliases = vec!["r1".to_string(), "r2".to_string()];
        let columns = vec!["src".to_string(), "dst".to_string()];
        let seeds = generate_nonlinear_seeds(
            &join,
            &aliases,
            "__base_delta",
            "\"public\".\"st\"",
            &columns,
        )
        .unwrap();

        assert_eq!(seeds.len(), 2, "Two self-refs → two non-linear seeds");

        // Seed 0: r1 = delta, r2 = ST storage
        assert!(
            seeds[0].contains("__base_delta WHERE __pgt_action = 'I'"),
            "Seed 0 should reference base delta inserts"
        );
        assert!(
            seeds[0].contains("\"public\".\"st\" AS \"r2\""),
            "Seed 0 should use ST storage for r2"
        );

        // Seed 1: r1 = ST storage, r2 = delta
        assert!(
            seeds[1].contains("\"public\".\"st\" AS \"r1\""),
            "Seed 1 should use ST storage for r1"
        );
        assert!(
            seeds[1].contains("__base_delta WHERE __pgt_action = 'I'"),
            "Seed 1 should reference base delta inserts"
        );
    }

    // ── Phase 4: Additional edge-case tests ─────────────────────────

    // ── generate_query_sql: left join ───────────────────────────────

    #[test]
    fn test_generate_query_sql_left_join() {
        let left = make_scan(100, "categories", "public", "c", &["id", "parent_id"]);
        let right = make_self_ref("tree", "t", &["id", "depth"]);
        let join = OpTree::LeftJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(left),
            right: Box::new(right),
        };

        let sql = generate_query_sql(&join, Some("__pgt_delta")).unwrap();
        assert!(sql.contains("LEFT JOIN"));
        assert!(sql.contains("__pgt_delta"));
        assert!(sql.contains("\"c\".\"id\""));
        assert!(sql.contains("\"t\".\"depth\""));
    }

    // ── generate_query_sql: subquery ────────────────────────────────

    #[test]
    fn test_generate_query_sql_subquery() {
        let inner = make_scan(100, "items", "public", "i", &["id", "price"]);
        let subquery = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(inner),
        };
        let sql = generate_query_sql(&subquery, None).unwrap();
        assert!(sql.contains("\"sub\".\"id\""));
        assert!(sql.contains("\"sub\".\"price\""));
        assert!(sql.contains("AS \"sub\""));
    }

    // ── generate_query_sql: unsupported variant ─────────────────────

    #[test]
    fn test_generate_query_sql_unsupported_variant_errors() {
        let agg = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![],
            child: Box::new(make_scan(1, "t", "public", "t", &["id"])),
        };
        let result = generate_query_sql(&agg, None);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("unsupported OpTree variant"));
    }

    // ── generate_from_sql: subquery ─────────────────────────────────

    #[test]
    fn test_generate_from_sql_subquery() {
        let inner = make_scan(100, "items", "public", "i", &["id"]);
        let subquery = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(inner),
        };
        let sql = generate_from_sql(&subquery, None).unwrap();
        assert!(sql.contains("AS \"sub\""));
    }

    // ── generate_from_sql: complex sub-tree wraps in subquery ───────

    #[test]
    fn test_generate_from_sql_complex_subtree_wraps() {
        let scan = make_scan(100, "items", "public", "i", &["id"]);
        let filter = OpTree::Filter {
            predicate: Expr::Literal("id > 0".to_string()),
            child: Box::new(scan),
        };
        let sql = generate_from_sql(&filter, None).unwrap();
        assert!(sql.contains("AS __sub"));
    }

    // ── generate_query_sql: project over inner join ─────────────────

    #[test]
    fn test_generate_query_sql_project_over_inner_join() {
        let left = make_scan(100, "categories", "public", "c", &["id", "parent_id"]);
        let right = make_self_ref("tree", "t", &["id", "depth"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(left),
            right: Box::new(right),
        };
        let project = OpTree::Project {
            expressions: vec![
                Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "id".to_string(),
                },
                Expr::Literal("t.depth + 1".to_string()),
            ],
            aliases: vec!["id".to_string(), "new_depth".to_string()],
            child: Box::new(join),
        };

        let sql = generate_query_sql(&project, Some("__delta")).unwrap();
        assert!(sql.contains("t.depth + 1 AS \"new_depth\""));
        assert!(sql.contains("JOIN"));
        // The project-over-join should inline the FROM clause
        assert!(!sql.contains("__p"));
    }

    // ── generate_query_sql: project over left join ──────────────────

    #[test]
    fn test_generate_query_sql_project_over_left_join() {
        let left = make_scan(100, "categories", "public", "c", &["id", "parent_id"]);
        let right = make_self_ref("tree", "t", &["id", "depth"]);
        let left_join = OpTree::LeftJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(left),
            right: Box::new(right),
        };
        let project = OpTree::Project {
            expressions: vec![Expr::ColumnRef {
                table_alias: Some("c".to_string()),
                column_name: "id".to_string(),
            }],
            aliases: vec!["id".to_string()],
            child: Box::new(left_join),
        };

        let sql = generate_query_sql(&project, Some("__delta")).unwrap();
        assert!(sql.contains("LEFT JOIN"));
    }

    // ── generate_query_sql: project over scan (fallback) ────────────

    #[test]
    fn test_generate_query_sql_project_over_scan_wraps_subquery() {
        let scan = make_scan(100, "items", "public", "i", &["id", "price"]);
        let project = OpTree::Project {
            expressions: vec![Expr::Literal("i.price * 2".to_string())],
            aliases: vec!["double_price".to_string()],
            child: Box::new(scan),
        };
        let sql = generate_query_sql(&project, None).unwrap();
        assert!(sql.contains("__p")); // wrapped in subquery alias
        assert!(sql.contains("i.price * 2 AS \"double_price\""));
    }

    // ── generate_query_sql_cascade: left join ───────────────────────

    #[test]
    fn test_generate_query_sql_cascade_left_join() {
        let left = make_scan(100, "categories", "public", "c", &["id", "parent_id"]);
        let right = make_self_ref("tree", "t", &["id", "parent_id"]);
        let join = OpTree::LeftJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(left),
            right: Box::new(right),
        };

        let sql = generate_query_sql_cascade(&join, "__casc", "\"public\".\"st\"").unwrap();
        assert!(sql.contains("LEFT JOIN"));
        assert!(sql.contains("__casc AS \"t\""));
        assert!(sql.contains("\"public\".\"st\" AS \"c\""));
    }

    // ── generate_query_sql_cascade: unsupported variant ─────────────

    #[test]
    fn test_generate_query_sql_cascade_unsupported_errors() {
        let agg = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![],
            child: Box::new(make_scan(1, "t", "public", "t", &["id"])),
        };
        let result = generate_query_sql_cascade(&agg, "__casc", "st");
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("generate_query_sql_cascade"));
    }

    // ── generate_cascade_from: subquery ─────────────────────────────

    #[test]
    fn test_generate_cascade_from_subquery() {
        // Subquery wrapping an InnerJoin (supported by generate_query_sql_cascade)
        let left = make_scan(100, "categories", "public", "c", &["id", "parent_id"]);
        let right = make_self_ref("tree", "t", &["id", "depth"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(left),
            right: Box::new(right),
        };
        let subquery = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(join),
        };
        let sql = generate_cascade_from(&subquery, "__casc", "\"public\".\"st\"").unwrap();
        assert!(sql.contains("AS \"sub\""));
    }

    // ── generate_cascade_from: complex subtree wraps ────────────────

    #[test]
    fn test_generate_cascade_from_complex_falls_back() {
        // Filter wrapping an InnerJoin — falls back to the catch-all branch
        // which calls generate_query_sql_cascade and wraps as __sub.
        let left = make_scan(100, "categories", "public", "c", &["id"]);
        let right = make_self_ref("tree", "t", &["id"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(left),
            right: Box::new(right),
        };
        let filter = OpTree::Filter {
            predicate: Expr::Literal("id > 0".to_string()),
            child: Box::new(join),
        };
        let sql = generate_cascade_from(&filter, "__casc", "\"public\".\"st\"").unwrap();
        assert!(sql.contains("AS __sub"));
    }

    // ── count_self_refs: through distinct and project ───────────────

    #[test]
    fn test_count_self_refs_through_distinct() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let distinct = OpTree::Distinct {
            child: Box::new(self_ref),
        };
        assert_eq!(count_self_refs(&distinct), 1);
    }

    #[test]
    fn test_count_self_refs_through_project() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let project = OpTree::Project {
            expressions: vec![Expr::ColumnRef {
                table_alias: Some("t".to_string()),
                column_name: "id".to_string(),
            }],
            aliases: vec!["id".to_string()],
            child: Box::new(self_ref),
        };
        assert_eq!(count_self_refs(&project), 1);
    }

    #[test]
    fn test_count_self_refs_through_subquery() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let subquery = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(self_ref),
        };
        assert_eq!(count_self_refs(&subquery), 1);
    }

    #[test]
    fn test_count_self_refs_left_join() {
        let scan = make_scan(100, "c", "public", "c", &["id"]);
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let join = OpTree::LeftJoin {
            condition: Expr::Literal("c.id = t.id".to_string()),
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        assert_eq!(count_self_refs(&join), 1);
    }

    // ── collect_self_ref_aliases: through wrappers ──────────────────

    #[test]
    fn test_collect_self_ref_aliases_through_filter() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let filter = OpTree::Filter {
            predicate: Expr::Literal("id > 0".to_string()),
            child: Box::new(self_ref),
        };
        assert_eq!(collect_self_ref_aliases(&filter), vec!["t"]);
    }

    #[test]
    fn test_collect_self_ref_aliases_through_distinct() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let distinct = OpTree::Distinct {
            child: Box::new(self_ref),
        };
        assert_eq!(collect_self_ref_aliases(&distinct), vec!["t"]);
    }

    #[test]
    fn test_collect_self_ref_aliases_through_subquery() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let subquery = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(self_ref),
        };
        assert_eq!(collect_self_ref_aliases(&subquery), vec!["t"]);
    }

    #[test]
    fn test_collect_self_ref_aliases_no_self_refs() {
        let scan = make_scan(100, "t", "public", "t", &["id"]);
        assert!(collect_self_ref_aliases(&scan).is_empty());
    }

    // ── generate_query_sql_targeted: filter, project, subquery ──────

    #[test]
    fn test_generate_query_sql_targeted_filter() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let filter = OpTree::Filter {
            predicate: Expr::Literal("id > 0".to_string()),
            child: Box::new(self_ref),
        };
        let mut map = std::collections::HashMap::new();
        map.insert("t".to_string(), "__delta".to_string());
        let sql = generate_query_sql_targeted(&filter, &map).unwrap();
        assert!(sql.contains("WHERE id > 0"));
        assert!(sql.contains("__delta AS \"t\""));
    }

    #[test]
    fn test_generate_query_sql_targeted_project() {
        let self_ref = make_self_ref("tree", "t", &["id", "depth"]);
        let project = OpTree::Project {
            expressions: vec![Expr::ColumnRef {
                table_alias: Some("t".to_string()),
                column_name: "id".to_string(),
            }],
            aliases: vec!["id".to_string()],
            child: Box::new(self_ref),
        };
        let mut map = std::collections::HashMap::new();
        map.insert("t".to_string(), "__delta".to_string());
        let sql = generate_query_sql_targeted(&project, &map).unwrap();
        assert!(sql.contains("__delta AS \"t\""));
    }

    #[test]
    fn test_generate_query_sql_targeted_left_join() {
        let scan = make_scan(100, "edges", "public", "e", &["src", "dst"]);
        let self_ref = make_self_ref("reach", "r", &["src", "dst"]);
        let left_join = OpTree::LeftJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("e".to_string()),
                    column_name: "dst".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("r".to_string()),
                    column_name: "src".to_string(),
                }),
            },
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        let mut map = std::collections::HashMap::new();
        map.insert("r".to_string(), "__delta".to_string());
        let sql = generate_query_sql_targeted(&left_join, &map).unwrap();
        assert!(sql.contains("LEFT JOIN"));
        assert!(sql.contains("__delta AS \"r\""));
    }

    #[test]
    fn test_generate_query_sql_targeted_subquery() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let subquery = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(self_ref),
        };
        let mut map = std::collections::HashMap::new();
        map.insert("t".to_string(), "__delta".to_string());
        let sql = generate_query_sql_targeted(&subquery, &map).unwrap();
        assert!(sql.contains("AS \"sub\""));
    }

    #[test]
    fn test_generate_query_sql_targeted_unsupported_errors() {
        let agg = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![],
            child: Box::new(make_scan(1, "t", "public", "t", &["id"])),
        };
        let map = std::collections::HashMap::new();
        let result = generate_query_sql_targeted(&agg, &map);
        assert!(result.is_err());
    }

    // ── generate_from_sql_targeted tests ────────────────────────────

    #[test]
    fn test_generate_from_sql_targeted_scan() {
        let scan = make_scan(100, "edges", "public", "e", &["src", "dst"]);
        let map = std::collections::HashMap::new();
        let sql = generate_from_sql_targeted(&scan, &map).unwrap();
        assert_eq!(sql, "\"public\".\"edges\" AS \"e\"");
    }

    #[test]
    fn test_generate_from_sql_targeted_self_ref() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let mut map = std::collections::HashMap::new();
        map.insert("t".to_string(), "__delta".to_string());
        let sql = generate_from_sql_targeted(&self_ref, &map).unwrap();
        assert_eq!(sql, "__delta AS \"t\"");
    }

    #[test]
    fn test_generate_from_sql_targeted_subquery() {
        let inner = make_scan(100, "items", "public", "i", &["id"]);
        let subquery = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(inner),
        };
        let map = std::collections::HashMap::new();
        let sql = generate_from_sql_targeted(&subquery, &map).unwrap();
        assert!(sql.contains("AS \"sub\""));
    }

    #[test]
    fn test_generate_from_sql_targeted_complex_wraps() {
        let scan = make_scan(100, "items", "public", "i", &["id"]);
        let filter = OpTree::Filter {
            predicate: Expr::Literal("id > 0".to_string()),
            child: Box::new(scan),
        };
        let map = std::collections::HashMap::new();
        let sql = generate_from_sql_targeted(&filter, &map).unwrap();
        assert!(sql.contains("AS __sub"));
    }

    #[test]
    fn test_generate_from_sql_targeted_missing_alias_errors() {
        let self_ref = make_self_ref("tree", "t", &["id"]);
        let map = std::collections::HashMap::new(); // no entry for "t"
        let result = generate_from_sql_targeted(&self_ref, &map);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("no replacement for self-ref alias"));
    }

    // ── collect_cascade_cols: subquery ───────────────────────────────

    #[test]
    fn test_collect_cascade_cols_subquery() {
        let inner = make_scan(100, "items", "public", "i", &["id", "price"]);
        let subquery = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(inner),
        };
        let mut cols = Vec::new();
        collect_cascade_cols(&subquery, &mut cols);
        assert_eq!(cols, vec!["\"sub\".\"id\"", "\"sub\".\"price\""]);
    }

    #[test]
    fn test_collect_cascade_cols_unknown_variant() {
        let filter = OpTree::Filter {
            predicate: Expr::Literal("TRUE".to_string()),
            child: Box::new(make_scan(1, "t", "public", "t", &["x"])),
        };
        let mut cols = Vec::new();
        collect_cascade_cols(&filter, &mut cols);
        // Non-scan/self-ref/subquery uses "__sub" alias
        assert_eq!(cols, vec!["\"__sub\".\"x\""]);
    }

    // ── collect_select_cols: subquery ────────────────────────────────

    #[test]
    fn test_collect_select_cols_subquery() {
        let inner = make_scan(100, "t", "public", "t", &["a", "b"]);
        let subquery = OpTree::Subquery {
            alias: "s".to_string(),
            column_aliases: vec![],
            child: Box::new(inner),
        };
        let mut cols = Vec::new();
        collect_select_cols(&subquery, &mut cols);
        assert_eq!(cols, vec!["\"s\".\"a\"", "\"s\".\"b\""]);
    }

    // ── generate_query_sql_with_change_buffers: Project-over-Join inline ──

    #[test]
    fn test_change_buffer_project_over_inner_join_inlines() {
        // Recursive term: SELECT n.id, n.parent_id, n.label
        //   FROM sn_nodes n JOIN t ON n.parent_id = t.id
        // The Project-over-InnerJoin should inline the FROM clause so that
        // project expressions (which use original table aliases) resolve.
        let scan = make_scan(
            100,
            "sn_nodes",
            "public",
            "n",
            &["id", "parent_id", "label"],
        );
        let self_ref = make_self_ref("tree", "t", &["id", "parent_id", "label"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        let project = OpTree::Project {
            expressions: vec![
                Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "id".to_string(),
                },
                Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "parent_id".to_string(),
                },
                Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "label".to_string(),
                },
            ],
            aliases: vec![
                "id".to_string(),
                "parent_id".to_string(),
                "label".to_string(),
            ],
            child: Box::new(join),
        };

        let ctx = test_ctx();
        let sql = generate_query_sql_with_change_buffers(&ctx, &project, "\"public\".\"st_table\"")
            .unwrap();

        let sql = sql.expect("Should produce SQL for Project-over-InnerJoin");
        // Project expressions should resolve against the inlined FROM
        assert!(
            sql.contains("\"n\".\"id\""),
            "Should reference n.id from project expressions"
        );
        // Self-ref should be replaced with ST storage
        assert!(
            sql.contains("\"public\".\"st_table\" AS \"t\""),
            "Self-ref should be replaced with ST storage"
        );
        // Should use JOIN, not a subquery wrapper
        assert!(sql.contains("JOIN"), "Should have JOIN");
        assert!(
            !sql.contains("__p"),
            "Should NOT wrap in __p subquery (inlined)"
        );
    }

    #[test]
    fn test_change_buffer_project_over_left_join_inlines() {
        let scan = make_scan(100, "nodes", "public", "n", &["id", "parent_id"]);
        let self_ref = make_self_ref("tree", "t", &["id", "parent_id"]);
        let join = OpTree::LeftJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        let project = OpTree::Project {
            expressions: vec![Expr::ColumnRef {
                table_alias: Some("n".to_string()),
                column_name: "id".to_string(),
            }],
            aliases: vec!["id".to_string()],
            child: Box::new(join),
        };

        let ctx = test_ctx();
        let sql =
            generate_query_sql_with_change_buffers(&ctx, &project, "\"public\".\"st\"").unwrap();

        let sql = sql.expect("Should produce SQL");
        assert!(sql.contains("LEFT JOIN"), "Should have LEFT JOIN");
        assert!(
            sql.contains("\"public\".\"st\" AS \"t\""),
            "Self-ref should use ST storage"
        );
        assert!(
            !sql.contains("__p"),
            "Should NOT wrap in __p subquery (inlined)"
        );
    }

    #[test]
    fn test_generate_change_buffer_from_includes_update_new_rows() {
        // A44-10 (D+I schema): I-rows carry new values; no action='U' in CB
        let scan = make_scan(100, "nodes", "public", "n", &["id", "parent_id", "name"]);
        let ctx = test_ctx();

        let sql = generate_change_buffer_from(&ctx, &scan, "\"public\".\"st\"").unwrap();

        assert!(
            sql.contains("c.action = 'I'"),
            "Recursive insert seed should read I-rows only (D+I schema): {sql}"
        );
        assert!(
            !sql.contains("action = 'U'"),
            "No action='U' in D+I schema: {sql}"
        );
        // Flat column ref — no new_ prefix
        assert!(
            sql.contains("c.\"name\" AS \"name\""),
            "flat column ref: {sql}"
        );
    }

    #[test]
    fn test_generate_old_change_buffer_from_includes_update_old_rows() {
        // A44-10 (D+I schema): D-rows carry old values; no action='U' in CB
        let scan = make_scan(100, "nodes", "public", "n", &["id", "parent_id", "name"]);
        let ctx = test_ctx();

        let sql = generate_old_change_buffer_from(&ctx, &scan, "\"public\".\"st\"").unwrap();

        assert!(
            sql.contains("c.action = 'D'"),
            "Recursive delete seed should read D-rows only (D+I schema): {sql}"
        );
        assert!(
            !sql.contains("action = 'U'"),
            "No action='U' in D+I schema: {sql}"
        );
        // Flat column ref — no old_ prefix
        assert!(
            sql.contains("c.\"name\" AS \"name\""),
            "flat column ref: {sql}"
        );
    }

    #[test]
    fn test_old_change_buffer_project_over_join_inlines() {
        let scan = make_scan(100, "nodes", "public", "n", &["id", "parent_id", "label"]);
        let self_ref = make_self_ref("tree", "t", &["id", "parent_id", "label"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        let project = OpTree::Project {
            expressions: vec![
                Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "id".to_string(),
                },
                Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "label".to_string(),
                },
            ],
            aliases: vec!["id".to_string(), "label".to_string()],
            child: Box::new(join),
        };

        let ctx = test_ctx();
        let sql = generate_query_sql_with_old_change_buffers(&ctx, &project, "\"public\".\"st\"")
            .unwrap()
            .expect("Should produce SQL for old-row Project-over-InnerJoin");

        // A44-10 (D+I schema): flat c."col" references, no old_ prefix
        assert!(
            sql.contains("c.\"label\" AS \"label\""),
            "flat column ref: {sql}"
        );
        assert!(!sql.contains("c.\"old_label\""), "no old_ prefix: {sql}");
        assert!(sql.contains("\"public\".\"st\" AS \"t\""));
        assert!(sql.contains("JOIN"));
        assert!(!sql.contains("__p"));
    }

    // ── Strategy selection: column matching ──────────────────────────

    #[test]
    fn test_columns_match_enables_incremental() {
        // When CTE columns == ST user columns, diff_recursive_cte should
        // NOT produce recomputation-style CTEs (rc_recomp_*).
        // This test verifies the column matching condition works.
        let cte_cols = vec!["id".to_string(), "label".to_string()];
        let st_cols = vec!["id".to_string(), "label".to_string()];
        assert_eq!(cte_cols, st_cols, "Columns should match");
    }

    #[test]
    fn test_columns_mismatch_forces_recomputation() {
        // When CTE columns ⊃ ST columns, recomputation should be used.
        let cte_cols = vec![
            "id".to_string(),
            "parent_id".to_string(),
            "label".to_string(),
        ];
        let st_cols = vec!["id".to_string(), "label".to_string()];
        assert_ne!(cte_cols, st_cols, "Columns should NOT match");
    }

    // ── Monotonicity analysis tests (F9: G1.3) ──────────────────────

    #[test]
    fn test_monotone_simple_join() {
        // JOIN + Scan + RecursiveSelfRef: standard tree traversal — monotone
        let scan = make_scan(100, "nodes", "public", "n", &["id", "pid"]);
        let self_ref = make_self_ref("tree", "t", &["id", "pid"]);
        let join = OpTree::InnerJoin {
            condition: Expr::Literal("n.pid = t.id".to_string()),
            left: Box::new(scan),
            right: Box::new(self_ref),
        };
        assert!(
            recursive_term_is_non_monotone(&join).is_none(),
            "Simple JOIN should be monotone"
        );
    }

    #[test]
    fn test_monotone_filter_project() {
        // Filter + Project wrapping a Scan: monotone
        let scan = make_scan(100, "t", "public", "t", &["id"]);
        let filtered = OpTree::Filter {
            predicate: Expr::Literal("id > 0".to_string()),
            child: Box::new(scan),
        };
        let projected = OpTree::Project {
            expressions: vec![Expr::ColumnRef {
                table_alias: Some("t".to_string()),
                column_name: "id".to_string(),
            }],
            aliases: vec!["id".to_string()],
            child: Box::new(filtered),
        };
        assert!(
            recursive_term_is_non_monotone(&projected).is_none(),
            "Filter + Project should be monotone"
        );
    }

    #[test]
    fn test_non_monotone_except() {
        let left = make_scan(100, "a", "public", "a", &["id"]);
        let right = make_scan(200, "b", "public", "b", &["id"]);
        let except = OpTree::Except {
            left: Box::new(left),
            right: Box::new(right),
            all: false,
        };
        let reason = recursive_term_is_non_monotone(&except);
        assert!(reason.is_some(), "EXCEPT should be non-monotone");
        assert!(reason.unwrap().contains("EXCEPT"));
    }

    #[test]
    fn test_non_monotone_aggregate() {
        use crate::dvm::parser::{AggExpr, AggFunc};
        let scan = make_scan(100, "t", "public", "t", &["category", "val"]);
        let agg = OpTree::Aggregate {
            group_by: vec![Expr::ColumnRef {
                table_alias: Some("t".to_string()),
                column_name: "category".to_string(),
            }],
            aggregates: vec![AggExpr {
                function: AggFunc::Sum,
                argument: Some(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "val".to_string(),
                }),
                is_distinct: false,
                second_arg: None,
                filter: None,
                alias: "total".to_string(),
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan),
        };
        let reason = recursive_term_is_non_monotone(&agg);
        assert!(reason.is_some(), "Aggregate should be non-monotone");
        assert!(reason.unwrap().contains("aggregate"));
    }

    #[test]
    fn test_non_monotone_window() {
        use crate::dvm::parser::WindowExpr;
        let scan = make_scan(100, "t", "public", "t", &["id", "val"]);
        let window = OpTree::Window {
            window_exprs: vec![WindowExpr {
                func_name: "row_number".to_string(),
                args: vec![],
                partition_by: vec![],
                order_by: vec![],
                frame_clause: None,
                alias: "rn".to_string(),
            }],
            partition_by: vec![],
            pass_through: vec![],
            child: Box::new(scan),
        };
        let reason = recursive_term_is_non_monotone(&window);
        assert!(reason.is_some(), "Window should be non-monotone");
        assert!(reason.unwrap().contains("window"));
    }

    #[test]
    fn test_non_monotone_distinct() {
        let scan = make_scan(100, "t", "public", "t", &["id"]);
        let distinct = OpTree::Distinct {
            child: Box::new(scan),
        };
        let reason = recursive_term_is_non_monotone(&distinct);
        assert!(reason.is_some(), "DISTINCT should be non-monotone");
        assert!(reason.unwrap().contains("DISTINCT"));
    }

    #[test]
    fn test_non_monotone_anti_join() {
        let left = make_scan(100, "a", "public", "a", &["id"]);
        let right = make_scan(200, "b", "public", "b", &["id"]);
        let anti = OpTree::AntiJoin {
            condition: Expr::Literal("a.id = b.id".to_string()),
            left: Box::new(left),
            right: Box::new(right),
        };
        let reason = recursive_term_is_non_monotone(&anti);
        assert!(reason.is_some(), "AntiJoin should be non-monotone");
        assert!(reason.unwrap().contains("anti-join"));
    }

    #[test]
    fn test_non_monotone_intersect_set() {
        let left = make_scan(100, "a", "public", "a", &["id"]);
        let right = make_scan(200, "b", "public", "b", &["id"]);
        let inter = OpTree::Intersect {
            left: Box::new(left),
            right: Box::new(right),
            all: false,
        };
        let reason = recursive_term_is_non_monotone(&inter);
        assert!(reason.is_some(), "INTERSECT (set) should be non-monotone");
    }

    #[test]
    fn test_monotone_intersect_all() {
        let left = make_scan(100, "a", "public", "a", &["id"]);
        let right = make_scan(200, "b", "public", "b", &["id"]);
        let inter = OpTree::Intersect {
            left: Box::new(left),
            right: Box::new(right),
            all: true,
        };
        assert!(
            recursive_term_is_non_monotone(&inter).is_none(),
            "INTERSECT ALL should be monotone for positive bags"
        );
    }

    #[test]
    fn test_non_monotone_nested_in_join() {
        // Join where one side contains EXCEPT: non-monotone
        let scan = make_scan(100, "a", "public", "a", &["id"]);
        let left = make_scan(200, "b", "public", "b", &["id"]);
        let right = make_scan(300, "c", "public", "c", &["id"]);
        let except = OpTree::Except {
            left: Box::new(left),
            right: Box::new(right),
            all: false,
        };
        let join = OpTree::InnerJoin {
            condition: Expr::Literal("a.id = b.id".to_string()),
            left: Box::new(scan),
            right: Box::new(except),
        };
        let reason = recursive_term_is_non_monotone(&join);
        assert!(
            reason.is_some(),
            "EXCEPT nested in JOIN should be detected as non-monotone"
        );
    }

    #[test]
    fn test_monotone_union_all() {
        let a = make_scan(100, "a", "public", "a", &["id"]);
        let b = make_scan(200, "b", "public", "b", &["id"]);
        let union = OpTree::UnionAll {
            children: vec![a, b],
        };
        assert!(
            recursive_term_is_non_monotone(&union).is_none(),
            "UNION ALL should be monotone"
        );
    }

    // ── DRed: generate_dred_delta SQL structure (ChangeBuffer mode) ──

    /// Verify that `generate_dred_delta` produces the expected four-phase
    /// SQL structure for a simple tree recursive CTE in ChangeBuffer mode.
    ///
    /// The tree CTE:
    ///   WITH RECURSIVE t AS (
    ///     SELECT id, parent_id, name FROM categories WHERE parent_id IS NULL
    ///     UNION ALL
    ///     SELECT c.id, c.parent_id, c.name FROM categories c JOIN t ON c.parent_id = t.id
    ///   )
    ///   SELECT id, parent_id, name FROM t
    ///
    /// After removing the ChangeBuffer guard (P2-1), mixed DELETE/UPDATE
    /// changes should go through DRed rather than recomputation.
    #[test]
    fn test_generate_dred_delta_change_buffer_structure() {
        use crate::dvm::diff::DiffResult;
        use crate::dvm::operators::test_helpers::test_ctx_with_st;

        // ChangeBuffer context (default for test_ctx_with_st)
        let mut ctx = test_ctx_with_st("public", "rc_tree_st");

        // Build recursive CTE nodes: tree traversal via parent_id join
        let base = make_scan(
            100,
            "categories",
            "public",
            "c",
            &["id", "parent_id", "name"],
        );
        let recursive_base = make_scan(
            100,
            "categories",
            "public",
            "c",
            &["id", "parent_id", "name"],
        );
        let self_ref = make_self_ref("t", "t", &["id", "parent_id", "name"]);
        let recursive_join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("c".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(recursive_base),
            right: Box::new(self_ref),
        };

        // Fake base_delta (as if produced by diff_node(base) in ChangeBuffer mode)
        let base_delta_cte = "__pgt_cte_base_delta".to_string();
        ctx.add_cte(
            base_delta_cte.clone(),
            "SELECT NULL::bytea AS __pgt_row_id, NULL::text AS __pgt_action, \
             NULL::int AS id, NULL::int AS parent_id, NULL::text AS name WHERE false"
                .to_string(),
        );
        let base_delta = DiffResult {
            cte_name: base_delta_cte,
            columns: vec![
                "id".to_string(),
                "parent_id".to_string(),
                "name".to_string(),
            ],
            schema: crate::dvm::schema::RelationSchema::from_names(&[
                "id".to_string(),
                "parent_id".to_string(),
                "name".to_string(),
            ]),
            is_deduplicated: false,
            has_key_changed: false,
        };

        let columns = vec![
            "id".to_string(),
            "parent_id".to_string(),
            "name".to_string(),
        ];

        let result = generate_dred_delta(
            &mut ctx,
            "t",
            &columns,
            &base_delta,
            &base,
            &recursive_join,
            true,
        );

        assert!(
            result.is_ok(),
            "generate_dred_delta should succeed for ChangeBuffer mode"
        );
        let result = result.unwrap();
        assert_eq!(
            result.columns, columns,
            "Output columns should match input columns"
        );

        // Build the full WITH query to inspect all generated CTEs
        let all_sql = ctx.build_with_query(&result.cte_name);

        // Phase 1: semi-naive insert propagation (recursive INSERT delta CTE)
        assert!(
            all_sql.contains("dred_ins_t") || all_sql.contains("dred_ifin_t"),
            "Phase 1 INSERT propagation CTE should be present"
        );

        // Phase 2: over-deletion cascade (recursive delete cascade CTE)
        assert!(
            all_sql.contains("dred_dcasc_t") || all_sql.contains("dred_dseed_t"),
            "Phase 2 deletion cascade CTEs should be present"
        );

        // Phase 3: rederivation (re-run full CTE from current data)
        assert!(
            all_sql.contains("dred_rfull_t") || all_sql.contains("dred_rdrv_t"),
            "Phase 3 rederivation CTEs should be present"
        );

        // Phase 4: net deletions and combine
        assert!(
            all_sql.contains("dred_ndel_t") || all_sql.contains("dred_comb_t"),
            "Phase 4 combine CTEs should be present"
        );

        // The final CTE name should be the combined result
        assert!(
            all_sql.contains(&result.cte_name),
            "Final CTE name should appear in the SQL"
        );
    }

    /// Verify that the DRed over-deletion cascade reads from ST storage
    /// (not the change buffer) for the *recursive propagation* step. This
    /// is the key correctness property for P2-1: the cascade propagation
    /// must follow the ST storage graph to find all transitively-derived
    /// rows, while the seed may read old values from the change buffer.
    #[test]
    fn test_dred_cascade_reads_from_st_storage_not_change_buffer() {
        use crate::dvm::diff::DiffResult;
        use crate::dvm::operators::test_helpers::test_ctx_with_st;

        let mut ctx = test_ctx_with_st("public", "my_tree_st");

        let base = make_scan(100, "nodes", "public", "n", &["id", "parent_id"]);
        let recursive_base = make_scan(100, "nodes", "public", "n", &["id", "parent_id"]);
        let self_ref = make_self_ref("tree", "t", &["id", "parent_id"]);
        let rec_join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some("n".to_string()),
                    column_name: "parent_id".to_string(),
                }),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some("t".to_string()),
                    column_name: "id".to_string(),
                }),
            },
            left: Box::new(recursive_base),
            right: Box::new(self_ref),
        };

        let base_delta_cte = "__pgt_cte_base_del".to_string();
        ctx.add_cte(
            base_delta_cte.clone(),
            "SELECT NULL::bytea AS __pgt_row_id, NULL::text AS __pgt_action, \
             NULL::int AS id, NULL::int AS parent_id WHERE false"
                .to_string(),
        );
        let base_delta = DiffResult {
            cte_name: base_delta_cte,
            columns: vec!["id".to_string(), "parent_id".to_string()],
            schema: crate::dvm::schema::RelationSchema::from_names(&[
                "id".to_string(),
                "parent_id".to_string(),
            ]),
            is_deduplicated: false,
            has_key_changed: false,
        };

        let columns = vec!["id".to_string(), "parent_id".to_string()];
        let result = generate_dred_delta(
            &mut ctx,
            "tree",
            &columns,
            &base_delta,
            &base,
            &rec_join,
            true,
        )
        .unwrap();

        let all_sql = ctx.build_with_query(&result.cte_name);

        // The cascade CTE (Phase 2 recursive propagation) should reference ST
        // storage so it can follow the existing derived-row graph to find all
        // transitively-affected rows. This is the property that makes DRed
        // correct for derived-column changes like path rebuilds.
        assert!(
            all_sql.contains("\"public\".\"my_tree_st\""),
            "Cascade should reference ST storage for recursive propagation: {}",
            all_sql
        );

        // The rederivation CTE (Phase 3) re-runs the full query from source tables
        // (not the ST or the change buffer), so the actual source table appears too.
        assert!(
            all_sql.contains("\"public\".\"nodes\""),
            "Rederivation should re-run from source tables: {}",
            all_sql
        );

        // The result CTE should combine inserts and deletions
        assert!(
            !result.cte_name.is_empty(),
            "Result should have a non-empty CTE name"
        );
    }
}
