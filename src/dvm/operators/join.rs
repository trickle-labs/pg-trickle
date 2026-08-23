//! Inner join differentiation.
//!
//! ΔI(Q ⋈C R) = (ΔQ_I ⋈C R₁) + (ΔQ_D ⋈C R₀) + (Q₀ ⋈C ΔR)
//!
//! Where:
//! - R₁ = current state of R (post-change, i.e. live table)
//! - R₀ = pre-change state of R, reconstructed as
//!   R_current EXCEPT ALL Δ_inserts UNION ALL Δ_deletes
//! - Q₀ = pre-change state of Q, reconstructed similarly
//! - ΔQ_I, ΔQ_D = insert and delete deltas for the left side
//! - ΔR = delta for the right side
//!
//! ## EC-01 fix: R₀ for DELETE deltas in Part 1
//!
//! The original formula `(ΔQ ⋈ R₁)` reads the right side AFTER all
//! changes. When the old join partner on the right is deleted before
//! the delta runs, the DELETE half finds no match and is silently
//! dropped, leaving a stale row in the stream table.
//!
//! The fix splits Part 1 into:
//! - **Part 1a**: ΔQ_inserts ⋈ R₁ (post-change right — new rows need current partners)
//! - **Part 1b**: ΔQ_deletes ⋈ R₀ (pre-change right — old rows need old partners)
//!
//! R₀ is computed identically to Q₀: `R_current EXCEPT ALL ΔR_I UNION ALL ΔR_D`.
//! The split is applied when `use_r0` is true (simple right children).
//!
//! Using Q₀ (pre-change) in Part 2 instead of Q₁ (post-change) avoids
//! double-counting when both sides change simultaneously: Part 1 already
//! handles (ΔQ ⋈ R₁), and using Q₀ excludes newly-inserted Q rows that
//! would duplicate Part 1's contribution.
//!
//! Q₀ is computed via EXCEPT ALL for:
//! - **Scan children**: one table scan + small delta (cheap)
//! - **Subquery/Aggregate children**: snapshot + delta EXCEPT ALL
//!   (critical for correlated sources like Q15 where revenue0 and
//!   MAX(total_revenue) both depend on lineitem)
//! - **Non-SemiJoin join children (≤ 2 tables)**: full join snapshot +
//!   EXCEPT ALL.  Limited to small subtrees (≤ 2 scan nodes) because
//!   larger join snapshots spill multiple GB of temp files at SF=0.01.
//!   Safe because the Q21 regression only affects SemiJoin interactions.
//!
//! For **SemiJoin-containing join children**, Q₀ via EXCEPT ALL interacts
//! badly with SemiJoin R_old (Q21 numwait regression), so Q₁ is used
//! with a correction term for shallow cases (Part 3).
//!
//! ## Nested join children — correction term (Part 3)
//!
//! For Scan children, Q₀ is computed cheaply via EXCEPT ALL / UNION ALL.
//! For nested join children (multi-table chains), computing Q₀ requires
//! the full join snapshot plus EXCEPT ALL — prohibitively expensive.
//! Instead, Part 2 uses Q₁ (post-change) and a **correction term** (Part 3)
//! that subtracts the double-counted rows:
//!
//! ```text
//! Error = (Q₁ − Q₀) ⋈ ΔR = (ΔQ_I − ΔQ_D) ⋈ ΔR
//! ```
//!
//! Part 3 joins ΔQ with ΔR directly:
//! - For ΔQ_I rows (inserts): emit with FLIPPED dr action (cancels excess)
//! - For ΔQ_D rows (deletes): emit with dr action (adds missing contribution)
//!
//! The correction is only applied when the left child is a **shallow** join
//! (both of its children are simple Scan nodes).  For deeper chains (e.g.
//! Q08 with 8 tables), cascading corrections at every level generate SQL
//! too complex for PostgreSQL's planner, so the correction is skipped.
//!
//! ## Semi-join optimization
//!
//! When the base table is scanned for the "current" side of the join,
//! a semi-join filter limits the scan to rows whose join keys appear in
//! the delta of the other side. For example, Part 2 becomes:
//!
//! ```sql
//! FROM (SELECT * FROM left_table
//!       WHERE left_key IN (SELECT DISTINCT right_key FROM delta_right)
//! ) l
//! JOIN delta_right dr ON ...
//! ```
//!
//! This converts a full sequential scan of the base table into an indexed
//! lookup when the join key has an index, providing 10x+ speedup at low
//! change rates.

use crate::config;
use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::join_common::{
    build_base_table_key_exprs, build_leaf_snapshot_sql, build_snapshot_sql, contains_semijoin,
    is_join_child, is_simple_child, join_scan_count, rewrite_join_condition,
    supports_pre_change_join_snapshot, use_pre_change_snapshot,
};
use crate::dvm::parser::{Expr, OpTree};
use crate::error::PgTrickleError;

// A44-1 (v0.43.0): Part 3 correction term scan count limit is now controlled
// by the `pg_trickle.part3_max_scan_count` GUC (default 5, previously hardcoded).
// The default value of 5 was chosen to cover 6-table semi-join chains.
// A44-1 (v0.43.0): Deep-join L0 snapshot threshold is now controlled
// by the `pg_trickle.deep_join_l0_scan_threshold` GUC (default 4, previously hardcoded).

/// Returns true if `op` is a join whose **both** children are simple (Scan)
/// nodes.  Previously this gated Part 3 correction term generation — now
/// used only for diagnostics; Part 3 is generated for all join children
/// via `is_join_child()` to fix Q07-type double-counting errors.
fn is_shallow_join(op: &OpTree) -> bool {
    match op {
        OpTree::InnerJoin { left, right, .. } | OpTree::LeftJoin { left, right, .. } => {
            is_simple_child(left) && is_simple_child(right)
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. } => is_shallow_join(child),
        _ => false,
    }
}

/// Differentiate an InnerJoin node.
pub fn diff_inner_join(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::InnerJoin {
        condition,
        left,
        right,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_inner_join called on non-InnerJoin node".into(),
        ));
    };

    // Differentiate both children
    let left_result = ctx.diff_node(left)?;
    let right_result = ctx.diff_node(right)?;

    // Get the base table references for the current snapshot.
    // For Scan children this is the table name; for nested joins
    // this is a snapshot subquery with disambiguated columns.
    let left_table = build_snapshot_sql(left);
    let right_table = build_snapshot_sql(right);

    let left_cols = &left_result.columns;
    let right_cols = &right_result.columns;

    // Disambiguate output columns using table-alias prefixed names.
    // This prevents collisions when both sides have columns with the same
    // name (e.g., both have "id", "val"). The project diff knows how to
    // resolve qualified ColumnRef(table="l", col="id") → "l__id".
    let left_prefix = left.alias();
    let right_prefix = right.alias();

    let mut output_cols = Vec::new();
    for c in left_cols {
        output_cols.push(format!("{left_prefix}__{c}"));
    }
    for c in right_cols {
        output_cols.push(format!("{right_prefix}__{c}"));
    }

    let left_col_refs: Vec<String> = left_cols
        .iter()
        .map(|c| {
            format!(
                "dl.{} AS {}",
                quote_ident(c),
                quote_ident(&format!("{left_prefix}__{c}"))
            )
        })
        .collect();
    let right_col_refs: Vec<String> = right_cols
        .iter()
        .map(|c| {
            format!(
                "r.{} AS {}",
                quote_ident(c),
                quote_ident(&format!("{right_prefix}__{c}"))
            )
        })
        .collect();
    let left_col_refs2: Vec<String> = left_cols
        .iter()
        .map(|c| {
            format!(
                "l.{} AS {}",
                quote_ident(c),
                quote_ident(&format!("{left_prefix}__{c}"))
            )
        })
        .collect();
    let right_col_refs2: Vec<String> = right_cols
        .iter()
        .map(|c| {
            format!(
                "dr.{} AS {}",
                quote_ident(c),
                quote_ident(&format!("{right_prefix}__{c}"))
            )
        })
        .collect();

    let all_cols_part1 = [left_col_refs.as_slice(), right_col_refs.as_slice()]
        .concat()
        .join(", ");
    let all_cols_part2 = [left_col_refs2.as_slice(), right_col_refs2.as_slice()]
        .concat()
        .join(", ");

    // Part 3 correction term columns: left data from delta_left (dl), right
    // data from delta_right (dr). Reuses left_col_refs (which reference dl.)
    // and right_col_refs2 (which reference dr.).
    let all_cols_correction = [left_col_refs.as_slice(), right_col_refs2.as_slice()]
        .concat()
        .join(", ");

    // Row ID: hash of both sides' PK / non-nullable key columns.
    //
    // CRITICAL: Part 1 and Part 2 MUST produce the SAME row_id for the
    // same logical output row.  We use direct PK columns from both sides
    // (always in left-first, right-second order) rather than mixing
    // pre-computed __pgt_row_id with raw PK columns.  Mixing them leads
    // to hash(hash(L), R) vs hash(L, hash(R)) which are NOT equal, causing
    // phantom row accumulation in the stream table (UNIQUE_VIOLATION in the
    // soak test).
    //
    // EC01-1 (v0.24.0): Part 1b (ΔL_D ⋈ R₀) uses the SAME hash formula
    // as Part 1a (ΔL_I ⋈ R₁): hash(left_pks, right_pks).  Because PKs
    // are immutable, the right-side PK values are identical in R₀ and R₁
    // for any given join partner, guaranteeing hash convergence across
    // insert/delete pairs spanning multiple refresh cycles.
    //
    // For Scan nodes: uses non-nullable (PK) columns directly.
    // For nested join children: falls back to row_to_json for the snapshot
    // side.
    let right_key_exprs_r = build_base_table_key_exprs(right, "r");
    let right_key_exprs_dr = build_base_table_key_exprs(right, "dr");
    let left_key_exprs_l = build_base_table_key_exprs(left, "l");
    let left_key_exprs_dl = build_base_table_key_exprs(left, "dl");

    // Part 1: delta_left ⋈ base_right → hash(left_pks_from_dl, right_pks_from_r)
    let mut hash1_args = left_key_exprs_dl.clone();
    hash1_args.extend(right_key_exprs_r.clone());
    let hash_part1 = crate::hash::build_composite_hash_expr(&hash1_args);

    // Part 2: base_left ⋈ delta_right → hash(left_pks_from_l, right_pks_from_dr)
    let mut hash2_args = left_key_exprs_l.clone();
    hash2_args.extend(right_key_exprs_dr.clone());
    let hash_part2 = crate::hash::build_composite_hash_expr(&hash2_args);

    // Rewrite join condition with aliases for each part.
    // The original condition uses the source table aliases (e.g. o.cust_id = c.id).
    // Part 1 needs: dl (delta left) + r (base right).
    // Part 2 needs: l (pre-change left) + dr (delta right).
    //
    // For nested join children, column names are disambiguated with the
    // original table alias prefix (e.g., o.cust_id → dl."o__cust_id").
    let join_cond_part1 = rewrite_join_condition(condition, left, "dl", right, "r");
    let join_cond_part2 = rewrite_join_condition(condition, left, "l", right, "dr");

    // Part 3 correction condition: dl (delta left) + dr (delta right).
    // Used when Part 2 uses L₁ (post-change) instead of L₀.  The
    // correction cancels the (ΔL ⋈ ΔR) double-counting error introduced
    // by using L₁.  Generated for join children up to PART3_MAX_SCAN_COUNT
    // scan nodes.  This fixes Q07-type revenue drift in multi-table join
    // chains that contain semi-joins.
    //
    // DI-5: Threshold raised from 3 to 5 — DI-1's named CTE L₀ snapshots
    // make delta CTEs cheaper, and the Part 3 correction joins two
    // already-computed delta CTEs (NOT MATERIALIZED), so temp file
    // pressure is lower than the pre-DI-1 era where full snapshot CTEs
    // were inlined. Covers 6-table semi-join chains (e.g. Q21 variants).
    //
    // The correction is computed eagerly here but only emitted when
    // `!use_l0` (see `correction_sql` below).
    let left_scan_count = join_scan_count(left);
    // A44-1: Read deep-join threshold from GUC (default 5, previously hardcoded).
    let part3_max_scan_count = config::pg_trickle_part3_max_scan_count();
    let join_cond_correction =
        if !is_simple_child(left) && is_join_child(left) && left_scan_count <= part3_max_scan_count
        {
            Some(rewrite_join_condition(condition, left, "dl", right, "dr"))
        } else {
            None
        };

    // Extract equi-join key pairs for semi-join optimization.
    // If we can identify (left_key, right_key) pairs from the condition,
    // we filter the base table scan to only matching keys from the delta.
    //
    // Skip the optimization when either child is a nested join — the
    // column names in the condition don't directly match the snapshot
    // or delta CTE columns for complex children.
    let equi_keys = if is_simple_child(left) && is_simple_child(right) {
        extract_equijoin_keys(condition, left_prefix, right_prefix)
    } else {
        vec![]
    };

    // Build semi-join-filtered table references.
    // Part 1: right base table filtered by delta-left join keys
    let right_table_filtered = build_semijoin_subquery(
        &right_table,
        &equi_keys,
        &left_result.cte_name,
        JoinSide::Right,
    );
    // ── Pre-change snapshot for Part 2 ────────────────────────────
    //
    // Standard DBSP: ΔJ = (ΔL ⋈ R₁) + (L₀ ⋈ ΔR)
    //
    // L₀ = the state of the left child BEFORE the current cycle's changes.
    // Reconstructed as: L_current EXCEPT ALL Δ_inserts UNION ALL Δ_deletes.
    //
    // For Scan children and Subquery/Aggregate children, L₀ is computed
    // via EXCEPT ALL on the snapshot. For Subquery children (e.g.
    // revenue0 in Q15), this is critical when the inner subquery and
    // outer child share a common source table — using L₁ instead of L₀
    // causes missed DELETE rows and duplicate INSERT rows.
    //
    // For nested join children that contain SemiJoin/AntiJoin operators,
    // computing L₀ via EXCEPT ALL interacts badly with SemiJoin R_old
    // (Q21 numwait regression). These use L₁ + correction term for
    // shallow joins, or plain L₁ for deeper chains.
    //
    // For nested join children WITHOUT SemiJoin/AntiJoin (pure InnerJoin/
    // LeftJoin chains), L₀ is computed using the per-leaf CTE-based
    // snapshot strategy (EC01B-1): each leaf Scan's pre-change state is
    // reconstructed individually, then re-joined. This avoids the
    // full-snapshot EXCEPT ALL that spilled multi-GB temp files.
    //
    // Additionally, joins inside a SemiJoin/AntiJoin ancestor must use
    // L₁ to avoid Q21-type regressions where sub-join EXCEPT ALL
    // interacts with the SemiJoin's R_old computation.
    // A44-1: Read L0 scan threshold from GUC (default 4, previously hardcoded).
    let deep_join_l0_threshold = config::pg_trickle_deep_join_l0_scan_threshold();
    let use_per_leaf_l0 =
        use_pre_change_snapshot(left, ctx.inside_semijoin, deep_join_l0_threshold);
    let use_combined_l0 = is_join_child(left)
        && !supports_pre_change_join_snapshot(left)
        && !contains_semijoin(left)
        && !ctx.inside_semijoin;
    let use_l0 = use_per_leaf_l0 || use_combined_l0;

    let left_part2_source = if use_l0 {
        if is_join_child(left) && !use_combined_l0 {
            // DI-1: Register per-leaf pre-change snapshot as a named CTE.
            // Subsequent references to the same subtree reuse the CTE,
            // eliminating redundant EXCEPT ALL evaluations.
            let pre_change = ctx.get_or_register_snapshot_cte(left);

            // Mark all leaf delta CTEs as NOT MATERIALIZED since they're
            // now referenced in both the inner join delta and the per-leaf
            // EXCEPT ALL sub-selects.
            mark_leaf_delta_ctes_not_materialized(left, ctx);

            // Apply semi-join filter to L₀ if equi-keys are available
            if equi_keys.is_empty() {
                pre_change
            } else {
                let filters: Vec<String> = equi_keys
                    .iter()
                    .map(|(left_key, right_key)| {
                        format!(
                            "{left_key} IN (SELECT DISTINCT {right_key} FROM {})",
                            right_result.cte_name
                        )
                    })
                    .collect();
                format!(
                    "(SELECT * FROM {pre_change} __l0 WHERE {filters})",
                    filters = filters.join(" AND "),
                )
            }
        } else {
            // Scan, Subquery/Aggregate child: use L₀ via single-table
            // snapshot (DI-2: NOT EXISTS for Scan, EXCEPT ALL fallback)
            let left_pre_change = build_leaf_snapshot_sql(
                left,
                &left_result.cte_name,
                left_cols,
                &ctx.fallback_leaf_oids,
            );
            // Apply semi-join filter to L₀ if equi-keys are available
            if equi_keys.is_empty() {
                left_pre_change
            } else {
                let filters: Vec<String> = equi_keys
                    .iter()
                    .map(|(left_key, right_key)| {
                        format!(
                            "{left_key} IN (SELECT DISTINCT {right_key} FROM {})",
                            right_result.cte_name
                        )
                    })
                    .collect();
                format!(
                    "(SELECT * FROM {left_pre_change} __l0 WHERE {filters})",
                    filters = filters.join(" AND "),
                )
            }
        }
    } else {
        // SemiJoin-containing nested join child: use post-change L₁ with
        // semi-join filter (L₀ via EXCEPT ALL interacts badly with
        // SemiJoin R_old, causing Q21-type regressions).
        // Shallow join children get a correction term (Part 3) below.
        build_semijoin_subquery(
            &left_table,
            &equi_keys,
            &right_result.cte_name,
            JoinSide::Left,
        )
    };

    // ── EC-01: Pre-change snapshot for Part 1 (right side) ──────────
    //
    // Standard formula: ΔJ = (ΔL ⋈ R₁) + (L₀ ⋈ ΔR)
    //
    // Part 1 joins delta_left with the right side. When a left-side row's
    // join key changes AND the old right partner is simultaneously
    // deleted, the DELETE half of Part 1 finds no match in R₁ (current
    // right) and the stale row is silently retained in the stream table.
    //
    // Fix: split Part 1 into two arms:
    //   Part 1a: ΔL_inserts ⋈ R₁  (inserts need current right partners)
    //   Part 1b: ΔL_deletes ⋈ R₀  (deletes need pre-change right partners)
    //
    // R₀ = R_current EXCEPT ALL ΔR_inserts UNION ALL ΔR_deletes
    //
    // The same child-type heuristics as use_l0 apply: Scan and simple
    // children use R₀; SemiJoin-containing deep chains fall back to R₁.
    //
    // IMPORTANT: When use_l0 is true (Part 2 uses L₀), the standard
    // DBSP formula ΔL ⋈ R₁ + L₀ ⋈ ΔR is already mathematically exact:
    //
    //   ΔL ⋈ R₁ + L₀ ⋈ ΔR
    //   = (L₁ - L₀) ⋈ R₁ + L₀ ⋈ (R₁ - R₀)
    //   = L₁⋈R₁ - L₀⋈R₁ + L₀⋈R₁ - L₀⋈R₀
    //   = J₁ - J₀  ✓
    //
    // Splitting Part 1 into 1a (ΔL_I ⋈ R₁) + 1b (ΔL_D ⋈ R₀) when L₀
    // is already available introduces an uncorrected error of ΔL_D ⋈ ΔR.
    // The EC-02 correction term cancels this within a single refresh, but
    // under sustained load with keyless join stream tables (non-unique
    // __pgt_row_id), the weight aggregation can fail to fully cancel the
    // phantom rows across many concurrent-change cycles, causing monotonic
    // row accumulation (G17-SOAK soak_join correctness violation).
    //
    // The EC-01 split is only needed when Part 2 uses L₁ (!use_l0),
    // because in that case the standard formula has its own error term
    // (ΔL ⋈ ΔR) and the EC-01 split halves the error to ΔL_I ⋈ ΔR
    // (which Part 3 then corrects).
    let use_r0 = if use_l0 {
        // L₀ available → standard formula is exact → no split needed.
        false
    } else {
        use_pre_change_snapshot(right, ctx.inside_semijoin, deep_join_l0_threshold)
    };

    let right_part1_source = if use_r0 {
        if is_join_child(right) {
            // DI-1: Named CTE snapshot for right pre-change state.
            let pre_change = ctx.get_or_register_snapshot_cte(right);

            // Mark leaf delta CTEs NOT MATERIALIZED
            mark_leaf_delta_ctes_not_materialized(right, ctx);

            // Apply semi-join filter to R₀
            if equi_keys.is_empty() {
                pre_change
            } else {
                let filters: Vec<String> = equi_keys
                    .iter()
                    .map(|(left_key, right_key)| {
                        format!(
                            "{right_key} IN (SELECT DISTINCT {left_key} FROM {})",
                            left_result.cte_name
                        )
                    })
                    .collect();
                format!(
                    "(SELECT * FROM {pre_change} __r0 WHERE {filters})",
                    filters = filters.join(" AND "),
                )
            }
        } else {
            // Scan, Subquery/Aggregate child: use R₀ via single-table
            // snapshot (DI-2: NOT EXISTS for Scan, EXCEPT ALL fallback)
            let right_pre_change = build_leaf_snapshot_sql(
                right,
                &right_result.cte_name,
                right_cols,
                &ctx.fallback_leaf_oids,
            );
            // Apply semi-join filter to R₀
            if equi_keys.is_empty() {
                right_pre_change
            } else {
                let filters: Vec<String> = equi_keys
                    .iter()
                    .map(|(left_key, right_key)| {
                        format!(
                            "{right_key} IN (SELECT DISTINCT {left_key} FROM {})",
                            left_result.cte_name
                        )
                    })
                    .collect();
                format!(
                    "(SELECT * FROM {right_pre_change} __r0 WHERE {filters})",
                    filters = filters.join(" AND "),
                )
            }
        }
    } else {
        // Nested/complex right child: fall back to R₁ (current right).
        // Part 1 is NOT split in this case — kept as-is for safety.
        right_table_filtered.clone()
    };

    let cte_name = ctx.next_cte_name("join");

    // ── Correction term for nested join children (Part 3) ───────────
    //
    // Standard DBSP: ΔJ = (ΔL ⋈ R₁) + (L₀ ⋈ ΔR)
    //
    // When using L₁ (post-change) instead of L₀ (pre-change) for nested
    // join children in Part 2, the error is:
    //   Error = (L₁ - L₀) ⋈ ΔR = (ΔL_I - ΔL_D) ⋈ ΔR
    //
    // Part 3 corrects this by joining both delta CTEs directly:
    //   - ΔL_I ⋈ ΔR rows are excess in Part 2 → emit with flipped action
    //   - ΔL_D ⋈ ΔR rows are missing from Part 2 → emit with original action
    //
    // This avoids computing L₀ for nested joins (expensive EXCEPT ALL on
    // full snapshot) and doesn't change the SemiJoin operator's inputs,
    // avoiding the Q21 regression that the previous L₀ approach caused.
    //
    // Only applied when Part 2 uses L₁ (!use_l0).  When L₀ is used
    // directly, the correction is unnecessary (no double-counting error).
    let correction_sql = if !use_l0 {
        if let Some(cond) = &join_cond_correction {
            // The correction adds a second FROM-clause reference to
            // delta_left.  PostgreSQL (12+) auto-materializes CTEs with
            // >= 2 references, which can spill huge temp files for join
            // delta CTEs.  Mark it NOT MATERIALIZED so PG inlines it.
            ctx.mark_cte_not_materialized(&left_result.cte_name);

            // Row ID for correction rows: hash of both sides' PK columns,
            // using the same canonical left-first, right-second order as
            // Part 1 and Part 2 to ensure consistent row_ids.
            let mut hash_corr_args = left_key_exprs_dl.clone();
            hash_corr_args.extend(right_key_exprs_dr.clone());
            let hash_correction = crate::hash::build_composite_hash_expr(&hash_corr_args);

            format!(
                "

UNION ALL

-- Part 3: Correction for nested join L₁ → L₀
-- Cancels excess ΔL_I ⋈ ΔR and adds missing ΔL_D ⋈ ΔR.
-- For dl.action='I': these rows are in L₁ but not L₀ → flip dr action to cancel
-- For dl.action='D': these rows are in L₀ but not L₁ → keep dr action to add
SELECT {hash_correction} AS __pgt_row_id,
       CASE WHEN dl.__pgt_action = 'I'
            THEN CASE WHEN dr.__pgt_action = 'I' THEN 'D' ELSE 'I' END
            ELSE dr.__pgt_action
       END AS __pgt_action,
       {all_cols_correction}
FROM {delta_left} dl
JOIN {delta_right} dr ON {cond}",
                delta_left = left_result.cte_name,
                delta_right = right_result.cte_name,
            )
        } else {
            // DI-5: Part 3 correction suppressed — either left child is
            // not a join, or scan count exceeds PART3_MAX_SCAN_COUNT.
            // This may cause minor drift for very deep semi-join chains.
            String::new()
        }
    } else {
        // L₀ is used directly — the standard formula is exact, no
        // correction needed.  The former EC-02 correction for the EC-01
        // split is no longer reachable here because use_r0 is now false
        // whenever use_l0 is true.
        String::new()
    };

    // When use_r0 is true, R₀ via EXCEPT ALL references the right delta CTE
    // multiple times (for the EXCEPT ALL sub-selects). Mark it NOT MATERIALIZED
    // to prevent PostgreSQL from spilling temp files for CTE materialization.
    if use_r0 {
        ctx.mark_cte_not_materialized(&right_result.cte_name);
    }

    let sql = if use_r0 {
        // ── EC-01: Split Part 1 into 1a (inserts ⋈ R₁) + 1b (deletes ⋈ R₀)
        format!(
            "\
-- Part 1a: delta_left INSERTS JOIN current_right R₁ (semi-join filtered)
SELECT {hash_part1} AS __pgt_row_id,
       dl.__pgt_action,
       {all_cols_part1}
FROM {delta_left} dl
JOIN {right_table_filtered} r ON {join_cond_part1}
WHERE dl.__pgt_action = 'I'

UNION ALL

-- Part 1b: delta_left DELETES JOIN pre-change_right R₀ (EC-01 fix)
-- R₀ = R_current NOT EXISTS ΔR + old rows (DI-2)
-- Ensures deleted left rows find their old right partner even when
-- the right partner was simultaneously deleted.
SELECT {hash_part1} AS __pgt_row_id,
       dl.__pgt_action,
       {all_cols_part1}
FROM {delta_left} dl
JOIN {right_part1_source} r ON {join_cond_part1}
WHERE dl.__pgt_action = 'D'

UNION ALL

-- Part 2: pre-change_left JOIN delta_right
-- For Scan children: L₀ via NOT EXISTS anti-join (DI-2)
-- For nested joins: L₁ = current snapshot (semi-join filtered, corrected below)
SELECT {hash_part2} AS __pgt_row_id,
       dr.__pgt_action,
       {all_cols_part2}
FROM {left_part2_source} l
JOIN {delta_right} dr ON {join_cond_part2}{correction_sql}",
            delta_left = left_result.cte_name,
            delta_right = right_result.cte_name,
        )
    } else {
        // Right child is complex — keep Part 1 unsplit (no regression).
        format!(
            "\
-- Part 1: delta_left JOIN current_right (semi-join filtered)
SELECT {hash_part1} AS __pgt_row_id,
       dl.__pgt_action,
       {all_cols_part1}
FROM {delta_left} dl
JOIN {right_table_filtered} r ON {join_cond_part1}

UNION ALL

-- Part 2: pre-change_left JOIN delta_right
-- For Scan children: L₀ via NOT EXISTS anti-join (DI-2)
-- For nested joins: L₁ = current snapshot (semi-join filtered, corrected below)
SELECT {hash_part2} AS __pgt_row_id,
       dr.__pgt_action,
       {all_cols_part2}
FROM {left_part2_source} l
JOIN {delta_right} dr ON {join_cond_part2}{correction_sql}",
            delta_left = left_result.cte_name,
            delta_right = right_result.cte_name,
        )
    };

    ctx.add_cte(cte_name.clone(), sql);

    Ok(DiffResult {
        cte_name,
        columns: output_cols,
        is_deduplicated: false,
        has_key_changed: false,
    })
}

/// Which side of the join we are filtering.
enum JoinSide {
    /// We are filtering the left base table using right-side delta keys.
    Left,
    /// We are filtering the right base table using left-side delta keys.
    Right,
}

/// An equi-join key pair: `(left_column_sql, right_column_sql)`.
type EquiKeyPair = (String, String);

/// Extract equi-join key pairs from a join condition expression.
///
/// Walks the expression tree looking for `col_a = col_b` patterns,
/// including through AND conjunctions. Returns pairs of
/// `(left_table_key, right_table_key)` where each key is normalized
/// so the first element belongs to the left child and the second to
/// the right child, based on table alias matching.
///
/// Falls back gracefully: if the condition is too complex (OR, functions,
/// non-equality operators), returns an empty vec and we skip the
/// semi-join optimization.
fn extract_equijoin_keys(
    condition: &Expr,
    left_alias: &str,
    right_alias: &str,
) -> Vec<EquiKeyPair> {
    let mut keys = Vec::new();
    collect_equijoin_keys(condition, left_alias, right_alias, &mut keys);
    keys
}

/// Recursively collect equi-join key pairs from an expression.
///
/// Table qualifiers are stripped in the output because the keys are used
/// inside semi-join subqueries where the original table aliases are not
/// in scope. However, qualifiers are inspected first to correctly orient
/// each `(left_key, right_key)` pair based on which table alias each
/// side of the `=` belongs to.
fn collect_equijoin_keys(
    expr: &Expr,
    left_alias: &str,
    right_alias: &str,
    keys: &mut Vec<EquiKeyPair>,
) {
    match expr {
        Expr::BinaryOp { op, left, right } if op == "=" => {
            // Found an equality — determine which side belongs to which table.
            let left_stripped = left.strip_qualifier().to_sql();
            let right_stripped = right.strip_qualifier().to_sql();

            let left_belongs_to = classify_column_side(left, left_alias, right_alias);
            let right_belongs_to = classify_column_side(right, left_alias, right_alias);

            match (left_belongs_to, right_belongs_to) {
                (ColumnSide::Left, ColumnSide::Right) => {
                    // Normal order: left_of_= is left-table, right_of_= is right-table
                    keys.push((left_stripped, right_stripped));
                }
                (ColumnSide::Right, ColumnSide::Left) => {
                    // Swapped: left_of_= is right-table, right_of_= is left-table
                    keys.push((right_stripped, left_stripped));
                }
                _ => {
                    // Both same side or unknown — skip this key pair
                    // (semi-join optimization won't apply for this key)
                }
            }
        }
        Expr::BinaryOp { op, left, right } if op.eq_ignore_ascii_case("AND") => {
            // AND conjunction — recurse into both sides
            collect_equijoin_keys(left, left_alias, right_alias, keys);
            collect_equijoin_keys(right, left_alias, right_alias, keys);
        }
        _ => {
            // Non-equality / non-AND: skip (don't add anything).
            // The optimization will be skipped if no keys are found.
        }
    }
}

/// Which side of the join a column belongs to.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ColumnSide {
    Left,
    Right,
    Unknown,
}

/// Determine which side of the join a column expression belongs to,
/// based on its table alias.
fn classify_column_side(expr: &Expr, left_alias: &str, right_alias: &str) -> ColumnSide {
    if let Expr::ColumnRef {
        table_alias: Some(alias),
        ..
    } = expr
    {
        if alias == left_alias {
            ColumnSide::Left
        } else if alias == right_alias {
            ColumnSide::Right
        } else {
            ColumnSide::Unknown
        }
    } else {
        ColumnSide::Unknown
    }
}

/// Build a semi-join-filtered subquery for a base table.
///
/// Given equi-join key pairs and the delta CTE name, wraps the base table
/// in a subquery that filters to only rows matching the delta's join keys.
///
/// For `JoinSide::Left`, we filter the left table using right-side keys
/// from the right delta CTE:
/// ```sql
/// (SELECT * FROM left_table WHERE left_key IN
///    (SELECT DISTINCT right_key FROM delta_right))
/// ```
///
/// If no equi-join keys were extracted, returns the plain table reference
/// (no optimization applied).
fn build_semijoin_subquery(
    base_table: &str,
    equi_keys: &[EquiKeyPair],
    delta_cte: &str,
    side: JoinSide,
) -> String {
    if equi_keys.is_empty() {
        return base_table.to_string();
    }

    // Build WHERE ... AND ... clauses for each key pair
    let filters: Vec<String> = equi_keys
        .iter()
        .map(|(left_key, right_key)| {
            match side {
                JoinSide::Left => {
                    // Filtering left table: left_key IN (SELECT DISTINCT right_key FROM delta_right)
                    format!("{left_key} IN (SELECT DISTINCT {right_key} FROM {delta_cte})")
                }
                JoinSide::Right => {
                    // Filtering right table: right_key IN (SELECT DISTINCT left_key FROM delta_left)
                    format!("{right_key} IN (SELECT DISTINCT {left_key} FROM {delta_cte})")
                }
            }
        })
        .collect();

    format!(
        "(SELECT * FROM {base_table} WHERE {filters})",
        filters = filters.join(" AND "),
    )
}

/// Get the current-state table reference for a node.
/// Delegates to `join_common::build_snapshot_sql` for the actual implementation.
/// Kept as a local alias for backward compatibility with test assertions.
#[cfg(test)]
fn get_current_table_ref(op: &OpTree) -> String {
    build_snapshot_sql(op)
}

/// EC01B-1: Walk a join subtree and mark all leaf Scan delta CTEs as
/// NOT MATERIALIZED.
///
/// When the per-leaf CTE-based pre-change snapshot is used, each leaf's
/// delta CTE is referenced in both the inner join delta computation AND
/// the EXCEPT ALL sub-selects of the pre-change snapshot. PostgreSQL
/// auto-materializes CTEs with ≥2 references, which would spill temp files.
/// Marking them NOT MATERIALIZED forces PostgreSQL to inline the CTE.
pub fn mark_leaf_delta_ctes_not_materialized(op: &OpTree, ctx: &mut DiffContext) {
    match op {
        OpTree::Scan { alias, .. } => {
            if let Some(cte_name) = ctx.scan_delta_ctes.get(alias.as_str()) {
                ctx.mark_cte_not_materialized(&cte_name.clone());
            }
        }
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => {
            mark_leaf_delta_ctes_not_materialized(left, ctx);
            mark_leaf_delta_ctes_not_materialized(right, ctx);
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. } => {
            mark_leaf_delta_ctes_not_materialized(child, ctx);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;
    use proptest::prelude::*;

    // ── diff_inner_join tests ───────────────────────────────────────

    #[test]
    fn test_diff_inner_join_basic() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id", "amount"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = inner_join(cond, left, right);
        let result = diff_inner_join(&mut ctx, &tree).unwrap();

        // Output columns should be disambiguated with table prefixes
        assert!(result.columns.contains(&"o__id".to_string()));
        assert!(result.columns.contains(&"o__cust_id".to_string()));
        assert!(result.columns.contains(&"c__id".to_string()));
        assert!(result.columns.contains(&"c__name".to_string()));
    }

    #[test]
    fn test_diff_inner_join_two_parts() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = inner_join(cond, left, right);
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // When L₀ is available (Scan ⋈ Scan), Part 1 is NOT split — standard
        // formula is exact. Part 1 uses ΔL ⋈ R₁, Part 2 uses L₀ ⋈ ΔR.
        assert_sql_contains(&sql, "Part 1");
        assert_sql_contains(&sql, "Part 2");
        assert_sql_contains(&sql, "pre-change_left");
        // R₀ is not used when L₀ is available (no EC-01 split).
        assert_sql_not_contains(&sql, "Part 1a");
        assert_sql_not_contains(&sql, "Part 1b");
    }

    #[test]
    fn test_diff_inner_join_pre_change_snapshot() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = inner_join(cond, left, right);
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Part 2 should use L₀ via NOT EXISTS anti-join (DI-2).
        // R₀ is NOT used when L₀ is available (standard formula is exact).
        assert_sql_contains(&sql, "NOT EXISTS");
        assert_sql_contains(&sql, "__pgt_action = 'D'");
        // Only L₀ needs NOT EXISTS (no R₀ split).
        let ne_count = sql.matches("NOT EXISTS").count();
        assert!(
            ne_count >= 1,
            "expected ≥1 NOT EXISTS (L₀), got {ne_count}\n{sql}"
        );
    }

    #[test]
    fn test_diff_inner_join_not_deduplicated() {
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id"]);
        let right = scan(2, "b", "public", "b", &["id"]);
        let cond = eq_cond("a", "id", "b", "id");
        let tree = inner_join(cond, left, right);
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_inner_join_error_on_non_join_node() {
        let mut ctx = test_ctx();
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_inner_join(&mut ctx, &tree);
        assert!(result.is_err());
    }

    // ── extract_equijoin_keys tests ─────────────────────────────────

    #[test]
    fn test_extract_equijoin_keys_simple_equality() {
        // Condition: o.cust_id = c.id → left alias "o", right alias "c"
        let cond = eq_cond("o", "cust_id", "c", "id");
        let keys = extract_equijoin_keys(&cond, "o", "c");
        assert_eq!(keys.len(), 1);
        // Normalized: (left_table_key, right_table_key)
        assert!(keys[0].0.contains("cust_id"));
        assert!(keys[0].1.contains("id"));
    }

    #[test]
    fn test_extract_equijoin_keys_swapped_order() {
        // Condition: c.id = o.cust_id (right-table col on LEFT of =)
        let cond = eq_cond("c", "id", "o", "cust_id");
        let keys = extract_equijoin_keys(&cond, "o", "c");
        assert_eq!(keys.len(), 1);
        // Should be normalized: (left_table_key, right_table_key)
        assert!(keys[0].0.contains("cust_id"));
        assert!(keys[0].1.contains("id"));
    }

    #[test]
    fn test_extract_equijoin_keys_and_condition() {
        let cond = binop(
            "AND",
            eq_cond("o", "a", "c", "b"),
            eq_cond("o", "x", "c", "y"),
        );
        let keys = extract_equijoin_keys(&cond, "o", "c");
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_extract_equijoin_keys_non_equality_returns_empty() {
        let cond = binop(">", qcolref("o", "a"), qcolref("c", "b"));
        let keys = extract_equijoin_keys(&cond, "o", "c");
        assert!(keys.is_empty());
    }

    // ── build_semijoin_subquery tests ───────────────────────────────

    #[test]
    fn test_build_semijoin_subquery_right_side() {
        let keys = vec![("\"cust_id\"".to_string(), "\"id\"".to_string())];
        let result = build_semijoin_subquery(
            "\"public\".\"customers\"",
            &keys,
            "__pgt_cte_scan_1",
            JoinSide::Right,
        );
        assert!(result.contains("SELECT *"));
        assert!(result.contains("WHERE"));
        assert!(result.contains("IN (SELECT DISTINCT"));
    }

    #[test]
    fn test_build_semijoin_subquery_no_keys_returns_plain() {
        let result = build_semijoin_subquery(
            "\"public\".\"customers\"",
            &[],
            "__pgt_cte_1",
            JoinSide::Right,
        );
        assert_eq!(result, "\"public\".\"customers\"");
    }

    // ── get_current_table_ref tests ─────────────────────────────────

    #[test]
    fn test_get_current_table_ref_scan() {
        let node = scan(1, "orders", "public", "o", &["id"]);
        assert_eq!(get_current_table_ref(&node), "\"public\".\"orders\"");
    }

    #[test]
    #[should_panic]
    fn test_get_current_table_ref_non_scan() {
        let node = OpTree::Distinct {
            child: Box::new(scan(1, "t", "public", "t", &["id"])),
        };
        get_current_table_ref(&node);
    }

    // ── build_base_table_key_exprs tests ────────────────────────────

    #[test]
    fn test_build_base_table_key_exprs_non_nullable() {
        let node = scan_not_null(1, "orders", "public", "o", &["id", "name"]);
        let exprs = build_base_table_key_exprs(&node, "r");
        assert!(exprs.iter().any(|e| e.contains("r.\"id\"::TEXT")));
        assert!(exprs.iter().any(|e| e.contains("r.\"name\"::TEXT")));
    }

    #[test]
    fn test_build_base_table_key_exprs_all_nullable_fallback() {
        let node = scan(1, "orders", "public", "o", &["id", "name"]);
        let exprs = build_base_table_key_exprs(&node, "r");
        // All nullable → uses all columns
        assert_eq!(exprs.len(), 2);
    }

    #[test]
    fn test_build_base_table_key_exprs_non_scan_fallback() {
        let node = OpTree::Distinct {
            child: Box::new(scan(1, "t", "public", "t", &["id"])),
        };
        let exprs = build_base_table_key_exprs(&node, "x");
        assert_eq!(exprs, vec!["row_to_json(x)::text"]);
    }

    // ── Nested join tests ───────────────────────────────────────────

    #[test]
    fn test_diff_inner_join_nested_three_tables() {
        // (orders ⋈ customers) ⋈ products — 3-table nested inner join.
        // The left child (orders ⋈ customers) is a non-SemiJoin join chain,
        // so Part 2 uses L₀ via EXCEPT ALL (no correction term needed).
        let o = scan(1, "orders", "public", "o", &["id", "cust_id", "prod_id"]);
        let c = scan(2, "customers", "public", "c", &["id", "name"]);
        let inner = inner_join(eq_cond("o", "cust_id", "c", "id"), o, c);
        let p = scan(3, "products", "public", "p", &["id", "price"]);
        let tree = inner_join(eq_cond("o", "prod_id", "p", "id"), inner, p);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree);
        assert!(
            result.is_ok(),
            "nested 3-table inner join should diff: {result:?}"
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        // Should produce Part 1 + Part 2 (with L₀ via NOT EXISTS)
        assert_sql_contains(&sql, "Part 1");
        assert_sql_contains(&sql, "Part 2");
        // L₀ uses NOT EXISTS anti-join for pre-change snapshot (DI-2)
        assert_sql_contains(&sql, "NOT EXISTS");
        // Outer join: left child is a nested join (not a Scan), so the
        // outer join's CTE does NOT emit an EC-02 correction.
        // (The inner 2-Scan join may have its own EC-02 correction.)
        let outer_cte_marker = format!("{} AS", dr.cte_name);
        let outer_sql = sql.split(&outer_cte_marker).last().unwrap_or("");
        assert_sql_not_contains(outer_sql, "EC-02 correction");
        assert_sql_not_contains(outer_sql, "Correction for nested join");
    }

    #[test]
    fn test_diff_inner_join_deep_chain_no_correction() {
        // ((a ⋈ b) ⋈ c) ⋈ d — 4-table chain.
        // EC01B-1: all levels use per-leaf CTE-based L₀ snapshot → no
        // L₁+Part3 correction term at any level.
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let inner1 = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let c = scan(3, "c", "public", "c", &["id"]);
        let inner2 = inner_join(eq_cond("a", "id", "c", "id"), inner1, c);
        let d = scan(4, "d", "public", "d", &["id"]);
        let tree = inner_join(eq_cond("a", "id", "d", "id"), inner2, d);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        // EC01B-1: per-leaf CTE snapshot replaces L₁+Part3 → zero corrections
        let correction_count = sql.matches("Correction for nested join").count();
        assert_eq!(
            correction_count, 0,
            "expected 0 correction terms (EC01B-1 per-leaf CTE), got {correction_count}\n{sql}"
        );
    }

    #[test]
    fn test_diff_inner_join_nested_uses_l0_via_except_all() {
        // Non-SemiJoin nested join children use L₀ via EXCEPT ALL,
        // eliminating the need for a correction term.
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let inner = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let c = scan(3, "c", "public", "c", &["id"]);
        let tree = inner_join(eq_cond("a", "id", "c", "id"), inner, c);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // L₀ via NOT EXISTS should be present in the outer join's Part 2
        assert_sql_contains(&sql, "NOT EXISTS");
        // Outer join: left is nested join (not Scan) → no EC-02 or L₁ correction.
        // (The inner 2-Scan join may have its own EC-02 correction.)
        let outer_cte_marker = format!("{} AS", result.cte_name);
        let outer_sql = sql.split(&outer_cte_marker).last().unwrap_or("");
        assert_sql_not_contains(outer_sql, "Correction for nested join");
        assert_sql_not_contains(outer_sql, "EC-02 correction");
        // Should still have valid structure: Part 1 + Part 2
        assert_sql_contains(&sql, "Part 1");
        assert_sql_contains(&sql, "Part 2");
    }

    #[test]
    fn test_diff_inner_join_scan_no_correction() {
        // For Scan ⋈ Scan with L₀ available, NO correction is needed — the
        // standard formula is exact. Neither EC-02 nor L₁ correction appears.
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = inner_join(cond, left, right);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // No correction terms — standard formula is exact when L₀ is used.
        assert_sql_not_contains(&sql, "EC-02 correction");
        assert_sql_not_contains(&sql, "Correction for nested join");
    }

    #[test]
    fn test_diff_inner_join_nested_skips_semijoin_optimization() {
        // When a child is a nested join, semi-join optimization is skipped
        // at the OUTER join level (the inner join's own optimization is fine).
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let inner = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let c = scan(3, "c", "public", "c", &["id"]);
        let tree = inner_join(eq_cond("a", "id", "c", "id"), inner, c);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        // The outer join CTE should NOT have equi-join semi-join filter.
        // Extract only the outer join CTE body (after its name).
        let outer_cte_marker = format!("{} AS", result.cte_name);
        let outer_sql = sql.split(&outer_cte_marker).last().unwrap_or("");
        assert_sql_not_contains(outer_sql, "IN (SELECT DISTINCT");
    }

    #[test]
    fn test_diff_inner_join_nested_uses_snapshot_subquery() {
        // The nested child should appear as a subquery snapshot, not a plain table ref
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let inner = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let c = scan(3, "c", "public", "c", &["id"]);
        let tree = inner_join(eq_cond("a", "id", "c", "id"), inner, c);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        // The nested join snapshot should generate SQL with JOIN in the snapshot subquery
        assert_sql_contains(&sql, "JOIN");
    }

    // ── NATURAL JOIN diff tests ─────────────────────────────────────

    #[test]
    fn test_diff_inner_join_with_natural_condition() {
        // Simulate what the parser produces for NATURAL JOIN:
        // two tables sharing "id" column → equi-join on id
        let left = scan(1, "orders", "public", "o", &["id", "amount"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = natural_join_cond(&left, &right);
        let tree = inner_join(cond, left, right);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        // Should produce valid SQL with two parts (delta_left JOIN right, left JOIN delta_right)
        assert_sql_contains(&sql, "Part 1");
        assert_sql_contains(&sql, "Part 2");
        assert_sql_contains(&sql, "UNION ALL");
    }

    #[test]
    fn test_diff_inner_join_natural_multiple_common_cols() {
        // Two tables sharing both "id" and "region"
        let left = scan(1, "a", "public", "a", &["id", "region", "val"]);
        let right = scan(2, "b", "public", "b", &["id", "region", "score"]);
        let cond = natural_join_cond(&left, &right);
        let tree = inner_join(cond, left, right);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        // Should have disambiguated columns from both sides
        assert!(result.columns.contains(&"a__id".to_string()));
        assert!(result.columns.contains(&"a__region".to_string()));
        assert!(result.columns.contains(&"b__id".to_string()));
        assert!(result.columns.contains(&"b__score".to_string()));
    }

    #[test]
    fn test_diff_inner_join_natural_no_common_columns() {
        // No common columns → condition is TRUE (cross join)
        let left = scan(1, "orders", "public", "o", &["order_id", "amount"]);
        let right = scan(2, "customers", "public", "c", &["cust_id", "name"]);
        let cond = natural_join_cond(&left, &right);
        assert_eq!(cond.to_sql(), "TRUE");
        let tree = inner_join(cond, left, right);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        // Should still produce valid diff SQL
        assert!(!result.columns.is_empty());
    }

    #[test]
    fn test_diff_inner_join_inside_semijoin_uses_l1() {
        // When inside_semijoin is true (i.e. this inner join is a child of
        // a SemiJoin/AntiJoin), non-SemiJoin join children should fall back
        // to L₁ (post-change snapshot) to avoid the Q21-type regression.
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let inner = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let c = scan(3, "c", "public", "c", &["id"]);
        let tree = inner_join(eq_cond("a", "id", "c", "id"), inner, c);

        let mut ctx = test_ctx();
        // Simulate being inside a SemiJoin ancestor
        ctx.inside_semijoin = true;
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // inside_semijoin → L₁ used for the left join child, so the
        // EXCEPT ALL approach is NOT used for the outer join's Part 2
        // left snapshot.  Instead, the correction term (Part 3) appears
        // for the shallow nested join child.
        assert_sql_contains(&sql, "Part 1");
        assert_sql_contains(&sql, "Part 2");
        assert_sql_contains(&sql, "Part 3");
        // L₁ with correction, not L₀ via EXCEPT ALL at the outer level
        assert_sql_contains(&sql, "Correction for nested join");
    }

    #[test]
    fn test_join_scan_count() {
        let a = scan(1, "a", "public", "a", &["id"]);
        assert_eq!(join_scan_count(&a), 1);

        let b = scan(2, "b", "public", "b", &["id"]);
        let j2 = inner_join(eq_cond("a", "id", "b", "id"), a.clone(), b.clone());
        assert_eq!(join_scan_count(&j2), 2);

        let c = scan(3, "c", "public", "c", &["id"]);
        let j3 = inner_join(eq_cond("a", "id", "c", "id"), j2.clone(), c.clone());
        assert_eq!(join_scan_count(&j3), 3);

        let d = scan(4, "d", "public", "d", &["id"]);
        let j4 = inner_join(eq_cond("a", "id", "d", "id"), j3, d);
        assert_eq!(join_scan_count(&j4), 4);
    }

    #[test]
    fn test_deep_join_no_correction_with_per_leaf_cte() {
        // EC01B-1: per-leaf CTE-based pre-change snapshot replaces L₁+Part3
        // at all depths.  Both 3-table and 4-table chains use EXCEPT ALL at
        // the per-leaf level and emit no correction term.
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let c = scan(3, "c", "public", "c", &["id"]);

        // left = a ⋈ b (2 scans) — per-leaf CTE snapshot with NOT EXISTS (DI-2)
        let j_ab = inner_join(eq_cond("a", "id", "b", "id"), a.clone(), b);
        let tree_3 = inner_join(eq_cond("a", "id", "c", "id"), j_ab, c);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree_3).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert_sql_contains(&sql, "NOT EXISTS");

        // left = a ⋈ b ⋈ c (3 scans) — EC01B-1: per-leaf CTE, no Part 3
        let a2 = scan(1, "a", "public", "a", &["id"]);
        let b2 = scan(2, "b", "public", "b", &["id"]);
        let c2 = scan(3, "c", "public", "c", &["id"]);
        let d2 = scan(4, "d", "public", "d", &["id"]);
        let j_ab2 = inner_join(eq_cond("a", "id", "b", "id"), a2, b2);
        let j_abc = inner_join(eq_cond("a", "id", "c", "id"), j_ab2, c2);
        let tree_4 = inner_join(eq_cond("a", "id", "d", "id"), j_abc, d2);

        let mut ctx2 = test_ctx();
        let result2 = diff_inner_join(&mut ctx2, &tree_4).unwrap();
        let sql2 = ctx2.build_with_query(&result2.cte_name);
        // Per-leaf CTE: NOT EXISTS present at leaf level; no L₁→L₀ nested-join
        // correction (note: EC-02 still uses "Part 3" label for Scan⋈Scan inner
        // joins, so we only check for the specific nested-join correction marker).
        assert_sql_contains(&sql2, "NOT EXISTS");
        assert_sql_not_contains(&sql2, "Correction for nested join");
    }

    #[test]
    fn test_di5_part3_threshold_raised_covers_4_scan_semijoin_chain() {
        // DI-5: Part 3 correction threshold raised from 3 → 5.
        // Build a left child with join_scan_count = 4 that contains a
        // semi-join — this triggers `use_l0 = false` (semi-join present)
        // and should emit Part 3 correction at the outer join.
        //
        // Structure: inner_join(
        //   left = inner_join(
        //     left = inner_join(
        //       left = inner_join(A, B),  // 2 scans
        //       right = C                  // 1 scan
        //     ),  // 3 scans
        //     right = D                    // 1 scan
        //   ),  // 4 scans, no semi-join here
        //   right = semi_join(E, F)       // 0 scan_count
        // )  → left has scan_count = 4, contains_semijoin due to right
        //
        // Actually, the right side being a semi_join makes the diff
        // dispatch go to diff_semi_join for the right child. Instead,
        // wrap the semi-join inside the left subtree.
        //
        // Use structure where left child = inner_join(chain4, semi_join(E, F)):
        //   chain4 = (A ⋈ B) ⋈ C ⋈ D  (4 scans)
        //   left = chain4 ⋈ semi_join(E, F)  → 4 + 0 = 4 scan_count
        //   contains_semijoin(left) = true
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let d = scan(4, "d", "public", "d", &["id"]);
        let e = scan(5, "e", "public", "e", &["id"]);
        let f = scan(6, "f", "public", "f", &["id"]);
        let g = scan(7, "g", "public", "g", &["id"]);

        // chain4: (((a ⋈ b) ⋈ c) ⋈ d)
        let j_ab = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let j_abc = inner_join(eq_cond("a", "id", "c", "id"), j_ab, c);
        let chain4 = inner_join(eq_cond("a", "id", "d", "id"), j_abc, d);

        // left: chain4 ⋈ semi_join(e, f)
        // The semi_join is wrapped inside the left's inner_join,
        // but inner_join dispatches diff_inner_join for itself,
        // calling diff_node on each child separately.
        // The outer diff_inner_join sees left = inner_join(chain4, semi_join(e,f))
        // → is_join_child(left) = true, contains_semijoin(left) = true
        // → join_scan_count(left) = 4 + 0 = 4
        let sj = semi_join(eq_cond("e", "id", "f", "id"), e, f);
        let left = inner_join(eq_cond("a", "id", "e", "id"), chain4, sj);

        // Verify preconditions
        assert_eq!(join_scan_count(&left), 4);
        assert!(is_join_child(&left));

        // Outer join: left ⋈ g
        let tree = inner_join(eq_cond("a", "id", "g", "id"), left, g);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // DI-5: With threshold 5, scan_count 4 is within range →
        // Part 3 correction should be emitted.
        assert_sql_contains(&sql, "Correction for nested join");
    }

    #[test]
    fn test_di5_part3_suppressed_above_threshold() {
        // DI-5: Part 3 correction suppressed at the outermost join when
        // left_scan_count > PART3_MAX_SCAN_COUNT (5).
        //
        // DI-11: Inner join levels with 4+ scan children now use L₁ + Part 3
        // (instead of L₀ per-leaf). So "Correction for nested join" may appear
        // at inner levels. We verify the outermost join CTE does NOT contain
        // the correction.
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let d = scan(4, "d", "public", "d", &["id"]);
        let e = scan(5, "e", "public", "e", &["id"]);
        let f = scan(6, "f", "public", "f", &["id"]);
        let g = scan(7, "g", "public", "g", &["id"]);
        let h = scan(8, "h", "public", "h", &["id"]);
        let i = scan(9, "i", "public", "i", &["id"]);

        // chain6: ((((a ⋈ b) ⋈ c) ⋈ d) ⋈ e) ⋈ f — 6 scans
        let j_ab = inner_join(eq_cond("a", "id", "b", "id"), a, b);
        let j_abc = inner_join(eq_cond("a", "id", "c", "id"), j_ab, c);
        let j_abcd = inner_join(eq_cond("a", "id", "d", "id"), j_abc, d);
        let j_abcde = inner_join(eq_cond("a", "id", "e", "id"), j_abcd, e);
        let chain6 = inner_join(eq_cond("a", "id", "f", "id"), j_abcde, f);

        // left: chain6 ⋈ semi_join(g, h) → scan_count = 6 + 0 = 6
        let sj = semi_join(eq_cond("g", "id", "h", "id"), g, h);
        let left = inner_join(eq_cond("a", "id", "g", "id"), chain6, sj);

        assert_eq!(join_scan_count(&left), 6);

        let tree = inner_join(eq_cond("a", "id", "i", "id"), left, i);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();

        // The outermost CTE is result.cte_name. Extract its SQL body.
        let outer_cte_sql = ctx
            .cte_sql(&result.cte_name)
            .expect("outer CTE should exist");

        // scan_count 6 > PART3_MAX_SCAN_COUNT (5) → outermost correction
        // suppressed (inner levels may still have corrections due to DI-11).
        assert!(
            !outer_cte_sql.contains("Correction for nested join"),
            "Outermost CTE should NOT contain Part 3 correction, got:\n{}",
            &outer_cte_sql[..outer_cte_sql.len().min(500)]
        );
    }

    // ── EC-01: R₀ via EXCEPT ALL tests ──────────────────────────────

    #[test]
    fn test_ec01_simple_join_no_split_when_l0_available() {
        // Two simple Scan children → L₀ is available, so Part 1 is NOT
        // split. The standard formula ΔL ⋈ R₁ + L₀ ⋈ ΔR is exact.
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = inner_join(cond, left, right);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Part 1 is unsplit: ΔL ⋈ R₁ (no action filter)
        assert_sql_contains(&sql, "Part 1");
        assert_sql_not_contains(&sql, "Part 1a");
        assert_sql_not_contains(&sql, "Part 1b");
        assert_sql_not_contains(&sql, "EC-01 fix");

        // Part 2 uses L₀
        assert_sql_contains(&sql, "Part 2");
        assert_sql_contains(&sql, "pre-change_left");
    }

    #[test]
    fn test_ec01_no_r0_when_l0_available() {
        // When L₀ is available, R₀ is not used — standard formula is exact.
        let left = scan(1, "a", "public", "a", &["id", "key"]);
        let right = scan(2, "b", "public", "b", &["id", "val"]);
        let cond = eq_cond("a", "key", "b", "id");
        let tree = inner_join(cond, left, right);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // L₀ uses NOT EXISTS anti-join (DI-2)
        assert_sql_contains(&sql, "NOT EXISTS");
        // Only L₀ needs NOT EXISTS (no R₀ split)
        assert_sql_not_contains(&sql, "Part 1a");
        assert_sql_not_contains(&sql, "Part 1b");
    }

    #[test]
    fn test_ec01_nested_right_child_3_scans_no_split_when_l0() {
        // Left child is a simple Scan → L₀ is available → Part 1 is NOT
        // split at the outer join level. The standard formula is exact.
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let right_inner = inner_join(eq_cond("b", "id", "c", "id"), b, c);
        let d = scan(4, "d", "public", "d", &["id"]);
        let right_deep = inner_join(eq_cond("b", "id", "d", "id"), right_inner, d);
        let tree = inner_join(eq_cond("a", "id", "b", "id"), a, right_deep);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        let outer_cte_marker = format!("{} AS", result.cte_name);
        let outer_sql = sql.split(&outer_cte_marker).last().unwrap_or("");
        // L₀ available → no EC-01 split at the outer join
        assert_sql_not_contains(outer_sql, "Part 1a");
        assert_sql_not_contains(outer_sql, "Part 1b");
        assert_sql_contains(outer_sql, "Part 1");
    }

    #[test]
    fn test_ec01_nested_right_child_2_scans_no_split_when_l0() {
        // Left child is a simple Scan → L₀ is available → Part 1 is NOT
        // split even though the right child is a nested join.
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let right_join = inner_join(eq_cond("b", "id", "c", "id"), b, c);
        let tree = inner_join(eq_cond("a", "id", "b", "id"), a, right_join);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // L₀ available → no EC-01 split
        assert_sql_not_contains(&sql, "Part 1a");
        assert_sql_not_contains(&sql, "Part 1b");
    }

    #[test]
    fn test_ec01_two_union_all_arms_when_l0_available() {
        // Simple 2-table join with L₀ available: Part 1 + Part 2 = 2 arms
        // (no EC-01 split when standard formula is exact)
        let left = scan(1, "l", "public", "l", &["id"]);
        let right = scan(2, "r", "public", "r", &["id"]);
        let tree = inner_join(eq_cond("l", "id", "r", "id"), left, right);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // The join CTE should contain Part 1 and Part 2.
        let outer_cte_marker = format!("{} AS", result.cte_name);
        let outer_sql = sql.split(&outer_cte_marker).last().unwrap_or("");
        let union_count = outer_sql.matches("UNION ALL").count();
        assert!(
            union_count >= 1,
            "expected ≥1 UNION ALL (Part1+Part2), got {union_count}"
        );
    }

    // ── Multi-table join chain tests ────────────────────────────────

    /// `(A LEFT JOIN B) INNER JOIN C` — left child is a LEFT join.
    ///
    /// This is one of the most common 3-table patterns. The inner join wraps a
    /// left join, so the left child sub-tree has nullable right columns.
    /// The inner join differentiator must propagate the nullable columns from
    /// the left join without stripping them.
    #[test]
    fn test_diff_inner_join_left_join_as_left_child() {
        // A LEFT JOIN B: orders LEFT JOIN customers on o.cust_id = c.id
        let a = scan(1, "orders", "public", "o", &["id", "cust_id", "amount"]);
        let b = scan(2, "customers", "public", "c", &["id", "name"]);
        let left_subtree = left_join(eq_cond("o", "cust_id", "c", "id"), a, b);

        // (A LEFT JOIN B) INNER JOIN C: above LEFT JOIN inner-joined to regions
        let c = scan(3, "regions", "public", "r", &["id", "country"]);
        // Join on o.amount == r.id (contrived key for testing)
        let tree = inner_join(eq_cond("o", "id", "r", "id"), left_subtree, c);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree);
        assert!(
            result.is_ok(),
            "(A LEFT JOIN B) INNER JOIN C should succeed: {result:?}"
        );

        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);

        // Output must include columns from all three tables (A, B, C)
        assert!(
            dr.columns
                .iter()
                .any(|c| c.contains("o__id") || c.contains("id")),
            "output should contain columns from A; got {:?}",
            dr.columns
        );
        // SQL itself must have parts
        assert_sql_contains(&sql, "UNION ALL");

        // Verify it uses the pre-change snapshot (NOT EXISTS) for correctness
        assert_sql_contains(&sql, "NOT EXISTS");
    }

    /// `A INNER JOIN (B INNER JOIN C)` — right child is a nested inner join.
    ///
    /// Three-table inner join chain where all joins feed into the outermost
    /// inner join. Verifies that column disambiguation works at all levels.
    #[test]
    fn test_diff_inner_join_with_nested_right_inner_join() {
        // B INNER JOIN C: lineitems INNER JOIN parts
        let b = scan(
            2,
            "lineitems",
            "public",
            "l",
            &["order_id", "part_id", "qty"],
        );
        let c = scan(3, "parts", "public", "p", &["id", "type"]);
        let inner_bc = inner_join(eq_cond("l", "part_id", "p", "id"), b, c);

        // A INNER JOIN (B INNER JOIN C): orders INNER JOIN above
        let a = scan(1, "orders", "public", "o", &["id", "region"]);
        let tree = inner_join(eq_cond("o", "id", "l", "order_id"), a, inner_bc);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree);
        assert!(
            result.is_ok(),
            "A INNER JOIN (B INNER JOIN C) should succeed: {result:?}"
        );

        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert_sql_contains(&sql, "UNION ALL");
        // Output must reference all three scans
        assert!(
            sql.to_lowercase().contains("orders"),
            "SQL must reference orders"
        );
        assert!(
            sql.to_lowercase().contains("lineitems"),
            "SQL must reference lineitems"
        );
        assert!(
            sql.to_lowercase().contains("parts"),
            "SQL must reference parts"
        );
    }

    // ── P2-4 property tests ───────────────────────────────────────────────
    //
    // Verify structural invariants of diff_inner_join() output SQL for
    // arbitrarily generated column name sets using proptest.

    proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(200))]

        /// For any non-empty left/right column sets the output always contains
        /// UNION ALL (the two-part delta decomposition).
        #[test]
        fn prop_diff_inner_join_sql_always_has_union_all(
            left_cols in proptest::collection::vec("[a-z]{2,6}", 1..4usize),
            right_cols in proptest::collection::vec("[a-z]{2,6}", 1..4usize),
        ) {
            // Prefix ensures left/right column names never collide.
            let lcols: Vec<String> = left_cols.iter().enumerate()
                .map(|(i, c)| format!("lc{i}_{c}")).collect();
            let rcols: Vec<String> = right_cols.iter().enumerate()
                .map(|(i, c)| format!("rc{i}_{c}")).collect();
            let lrefs: Vec<&str> = lcols.iter().map(|s| s.as_str()).collect();
            let rrefs: Vec<&str> = rcols.iter().map(|s| s.as_str()).collect();

            let left  = scan(1, "left_t",  "public", "l", &lrefs);
            let right = scan(2, "right_t", "public", "r", &rrefs);
            let tree  = inner_join(eq_cond("l", &lcols[0], "r", &rcols[0]), left, right);

            let mut ctx = test_ctx();
            let result  = diff_inner_join(&mut ctx, &tree);
            prop_assert!(result.is_ok(), "diff_inner_join must not fail: {result:?}");

            let sql = ctx.build_with_query(&result.unwrap().cte_name);
            prop_assert!(sql.contains("UNION ALL"),
                "inner join delta must contain UNION ALL");
        }

        /// The output column count equals left.len() + right.len() for a
        /// 2-table inner join (both sides contribute all columns).
        #[test]
        fn prop_diff_inner_join_output_col_count(
            left_cols in proptest::collection::vec("[a-z]{2,6}", 1..4usize),
            right_cols in proptest::collection::vec("[a-z]{2,6}", 1..4usize),
        ) {
            let lcols: Vec<String> = left_cols.iter().enumerate()
                .map(|(i, c)| format!("lc{i}_{c}")).collect();
            let rcols: Vec<String> = right_cols.iter().enumerate()
                .map(|(i, c)| format!("rc{i}_{c}")).collect();
            let lrefs: Vec<&str> = lcols.iter().map(|s| s.as_str()).collect();
            let rrefs: Vec<&str> = rcols.iter().map(|s| s.as_str()).collect();

            let left  = scan(1, "left_t",  "public", "l", &lrefs);
            let right = scan(2, "right_t", "public", "r", &rrefs);
            let tree  = inner_join(eq_cond("l", &lcols[0], "r", &rcols[0]), left, right);

            let mut ctx = test_ctx();
            let result  = diff_inner_join(&mut ctx, &tree).unwrap();

            prop_assert_eq!(
                result.columns.len(),
                lcols.len() + rcols.len(),
                "inner join must output all left + right columns"
            );
        }
    }
}
