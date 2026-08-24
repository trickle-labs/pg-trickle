//! Outer join differentiation.
//!
//! LEFT JOIN = INNER JOIN + anti-join for non-matching left rows.
//!
//! Differentiate the inner join part normally, then handle the anti-join:
//! - Left rows that lose their last match → INSERT with NULL right columns
//! - Left rows that gain their first match → DELETE the NULL-padded row
//!
//! ## EC-01 fix: R₀ for DELETE deltas in Part 1 and Part 3
//!
//! Part 1 (delta_left ⋈ right) and Part 3 (delta_left anti-join right)
//! together partition all delta_left rows into "matched" and "unmatched".
//! When the right partner is simultaneously deleted, the post-change
//! right table (R₁) no longer contains that partner, causing:
//! - Part 1 to miss the DELETE (no match in R₁)
//! - Part 3 to emit a DELETE with NULL right cols (anti-join succeeds)
//!
//! Fix: split both parts by action:
//! - Part 1a/3a: INSERTs check R₁ (inserts need current right state)
//! - Part 1b/3b: DELETEs check R₀ (deletes need pre-change right state)
//!
//! R₀ = R_current EXCEPT ALL ΔR_inserts UNION ALL ΔR_deletes
//!
//! ## L₀ fix: Pre-change left snapshot for Part 2
//!
//! Part 2 (left ⋈ delta_right) must use L₀ (pre-change left) instead of
//! L₁ (current left) so that right-side changes are attributed to the
//! correct (old) group key.  Without L₀, renaming a department while
//! deleting an employee causes the deletion to be attributed to the NEW
//! group key, which nets to zero and silently drops the row.
//!
//! ## EC-02 fix: ΔL_D ⋈ ΔR double-counting correction
//!
//! When both EC-01 (R₀ for Part 1b) and L₀ (Part 2) are active, the
//! cross-term ΔL_D ⋈ ΔR appears in both Part 1b and Part 2.  Part 6
//! cancels this by emitting ΔL_D ⋈ ΔR rows with the ΔR action flipped.

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::join::mark_leaf_delta_ctes_not_materialized;
use crate::dvm::operators::join_common::{
    build_leaf_snapshot_sql, build_snapshot_sql, contains_semijoin, is_join_child, is_simple_child,
    rewrite_join_condition, supports_pre_change_join_snapshot, use_pre_change_snapshot,
};
use crate::dvm::parser::OpTree;
use crate::error::PgTrickleError;

/// Differentiate a LeftJoin node.
pub fn diff_left_join(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::LeftJoin {
        condition,
        left,
        right,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_left_join called on non-LeftJoin node".into(),
        ));
    };

    // For LEFT JOIN, we reuse inner join differentiation for the matching part
    // and add the anti-join handling for non-matching left rows.

    // Differentiate both children
    let left_result = ctx.diff_node(left)?;
    let right_result = ctx.diff_node(right)?;

    // Rewrite join condition aliases for each part of the delta query.
    // For nested join children, column names are disambiguated with the
    // original table alias prefix (e.g., o.cust_id → dl."o__cust_id").
    let join_cond_part1 = rewrite_join_condition(condition, left, "dl", right, "r");
    let join_cond_part2 = rewrite_join_condition(condition, left, "l", right, "dr");
    let join_cond_antijoin = rewrite_join_condition(condition, left, "dl", right, "r");

    let left_cols = &left_result.columns;
    let right_cols = &right_result.columns;

    // Disambiguate output columns with table-alias prefix, matching
    // inner join convention so diff_project can resolve qualified refs.
    let left_prefix = left.alias();
    let right_prefix = right.alias();

    let mut output_cols = Vec::new();
    for c in left_cols {
        output_cols.push(format!("{left_prefix}__{c}"));
    }
    for c in right_cols {
        output_cols.push(format!("{right_prefix}__{c}"));
    }

    let right_table = build_snapshot_sql(right);
    let left_table = build_snapshot_sql(left);

    // Build column references with AS aliases for disambiguation
    let dl_cols: Vec<String> = left_cols
        .iter()
        .map(|c| {
            format!(
                "dl.{} AS {}",
                quote_ident(c),
                quote_ident(&format!("{left_prefix}__{c}"))
            )
        })
        .collect();
    let r_cols: Vec<String> = right_cols
        .iter()
        .map(|c| {
            format!(
                "r.{} AS {}",
                quote_ident(c),
                quote_ident(&format!("{right_prefix}__{c}"))
            )
        })
        .collect();
    let l_cols: Vec<String> = left_cols
        .iter()
        .map(|c| {
            format!(
                "l.{} AS {}",
                quote_ident(c),
                quote_ident(&format!("{left_prefix}__{c}"))
            )
        })
        .collect();
    let dr_cols: Vec<String> = right_cols
        .iter()
        .map(|c| {
            format!(
                "dr.{} AS {}",
                quote_ident(c),
                quote_ident(&format!("{right_prefix}__{c}"))
            )
        })
        .collect();
    let null_right_cols: Vec<String> = right_cols
        .iter()
        .map(|c| format!("NULL AS {}", quote_ident(&format!("{right_prefix}__{c}"))))
        .collect();

    let part1_cols = [dl_cols.as_slice(), r_cols.as_slice()].concat().join(", ");
    let part2_cols = [l_cols.as_slice(), dr_cols.as_slice()].concat().join(", ");
    let antijoin_cols = [dl_cols.as_slice(), null_right_cols.as_slice()]
        .concat()
        .join(", ");

    // For Parts 4 & 5: current_left JOIN conditions with different aliases
    // Part 4/5 JOIN: uses l and dr (same as Part 2)
    // Part 5 NOT EXISTS: uses l (current_left) and r (current_right)
    let not_exists_cond = rewrite_join_condition(condition, left, "l", right, "r");

    // R_old condition: uses l (current_left) and __pgt_r_old (pre-change right)
    let r_old_cond = rewrite_join_condition(condition, left, "l", right, "__pgt_r_old");

    // Build R_old snapshot for Parts 4/5: pre-change right state.
    // R_old = R_current EXCEPT ALL Δ_inserts UNION ALL Δ_deletes
    // Used to check whether a left row had ANY matching right row BEFORE
    // the current cycle's changes, preventing spurious NULL-padded D/I.
    let right_user_cols: Vec<&String> = right_cols.iter().filter(|c| *c != "__pgt_count").collect();

    let r_old_snapshot = if is_join_child(right) && supports_pre_change_join_snapshot(right) {
        // DI-1: Named CTE snapshot for right pre-change state.
        ctx.get_or_register_snapshot_cte(right)
    } else {
        // DI-2: NOT EXISTS for Scan, EXCEPT ALL fallback for others
        build_leaf_snapshot_sql(
            right,
            &right_result.cte_name,
            &right_user_cols
                .iter()
                .map(|c| (*c).clone())
                .collect::<Vec<_>>(),
            &ctx.fallback_leaf_oids,
        )
    };

    // Null-padded columns for Parts 4 & 5 (left from `l`, right all NULL)
    let l_null_padded_cols = [l_cols.as_slice(), null_right_cols.as_slice()]
        .concat()
        .join(", ");

    // ── EC-01: Pre-change right snapshot for Part 1b / Part 3b ──────
    //
    // Same fix as diff_inner_join: when a left DELETE's old right
    // partner is simultaneously deleted, R₁ misses the match. Split
    // Part 1 and Part 3 by action, using R₀ for DELETEs.
    //
    // R₀ = R_current EXCEPT ALL ΔR_inserts UNION ALL ΔR_deletes
    // DI-11: Use same threshold as inner join for deep R₀ reconstruction.
    //
    // When use_l0 is true (Part 2 uses L₀), the standard DBSP formula
    // is already exact — no EC-01 split needed. See diff_inner_join
    // for the full derivation.
    let use_per_leaf_l0 = use_pre_change_snapshot(left, ctx.inside_semijoin, 4);
    let use_combined_l0 = is_join_child(left)
        && !supports_pre_change_join_snapshot(left)
        && !contains_semijoin(left)
        && !ctx.inside_semijoin;
    let use_exact_l0 = use_per_leaf_l0 || use_combined_l0;
    let use_r0 = if use_exact_l0 {
        false
    } else {
        use_pre_change_snapshot(right, ctx.inside_semijoin, 4)
    };

    // Build R₀ for Parts 1b/3b (includes all right_cols for JOIN/anti-join).
    // Separate from r_old_snapshot (used for Parts 4/5 NOT EXISTS only,
    // which filters out __pgt_count).
    let r0_snapshot = if use_r0 {
        if is_join_child(right) {
            // DI-1: Named CTE snapshot for right pre-change state.
            let pre_change = ctx.get_or_register_snapshot_cte(right);
            mark_leaf_delta_ctes_not_materialized(right, ctx);
            Some(pre_change)
        } else {
            // DI-2: NOT EXISTS for Scan, EXCEPT ALL fallback for others
            let r0 = build_leaf_snapshot_sql(
                right,
                &right_result.cte_name,
                right_cols,
                &ctx.fallback_leaf_oids,
            );
            Some(r0)
        }
    } else {
        None
    };

    // When use_r0 is true, mark the right delta CTE as NOT MATERIALIZED
    // to prevent PostgreSQL from spilling temp files for the multiple
    // EXCEPT ALL / UNION ALL references.
    if use_r0 {
        ctx.mark_cte_not_materialized(&right_result.cte_name);
    }

    // ── L₀: Pre-change snapshot for Part 2 (left side) ─────────────
    //
    // Standard DBSP: Δ(L ⟕ R) inner part = (ΔL ⋈ R₁) + (L₀ ⋈ ΔR)
    //
    // L₀ = the state of the left child BEFORE the current cycle's
    // changes.  Without L₀, Part 2 uses L₁ (post-change), causing
    // right-side deletions to be attributed to new group keys rather
    // than old ones (G17-STBASE bug).
    //
    // Same logic as diff_inner_join: use L₀ for simple/Scan children,
    // fall back to L₁ for SemiJoin-containing deep chains where
    // EXCEPT ALL interacts badly.
    let left_part2_source = if use_per_leaf_l0 {
        if is_join_child(left) {
            let pre_change = ctx.get_or_register_snapshot_cte(left);
            mark_leaf_delta_ctes_not_materialized(left, ctx);
            pre_change
        } else {
            build_leaf_snapshot_sql(
                left,
                &left_result.cte_name,
                left_cols,
                &ctx.fallback_leaf_oids,
            )
        }
    } else if use_combined_l0 {
        build_leaf_snapshot_sql(
            left,
            &left_result.cte_name,
            left_cols,
            &ctx.fallback_leaf_oids,
        )
    } else {
        left_table.clone()
    };

    // When use_l0 is true and left is a Scan, the left delta CTE is
    // referenced in both Part 1 and the L₀ EXCEPT ALL sub-selects.
    // Mark NOT MATERIALIZED to prevent spilling.
    if use_exact_l0 {
        ctx.mark_cte_not_materialized(&left_result.cte_name);
    }

    // ── L₁→L₀ correction term ──────────────────────────────────────
    //
    // When Part 2 uses L₁ (!use_l0), the error is (L₁ − L₀) ⋈ ΔR.
    // A correction term (ΔL ⋈ ΔR with action flipping) fixes this for
    // non-simple join children.
    //
    // When use_l0 is true, the standard DBSP formula is exact and no
    // correction is needed.  The former EC-02 correction for the EC-01
    // split is no longer reachable because use_r0 is now false whenever
    // use_l0 is true.
    let correction_cols = [dl_cols.as_slice(), dr_cols.as_slice()].concat().join(", ");
    let join_cond_correction = rewrite_join_condition(condition, left, "dl", right, "dr");
    let hash_correction = crate::hash::build_composite_hash_expr(&[
        "dl.__pgt_row_id::TEXT".to_string(),
        "dr.__pgt_row_id::TEXT".to_string(),
    ]);
    let hash_dl_r = crate::hash::build_composite_hash_expr(&[
        "dl.__pgt_row_id::TEXT".to_string(),
        "pgtrickle.pg_trickle_hash(row_to_json(r)::text)::TEXT".to_string(),
    ]);
    let hash_l_dr = crate::hash::build_composite_hash_expr(&[
        "pgtrickle.pg_trickle_hash(row_to_json(l)::text)::TEXT".to_string(),
        "dr.__pgt_row_id::TEXT".to_string(),
    ]);

    let correction_sql = if !use_exact_l0 && is_join_child(left) && !is_simple_child(left) {
        // Part 2 uses L₁: correction for (L₁ − L₀) ⋈ ΔR error.
        // Same as inner join Part 3: ΔL_I ⋈ ΔR flipped, ΔL_D ⋈ ΔR kept.
        format!(
            "

UNION ALL

-- Part 6: L₁→L₀ correction for nested join left child.
-- Cancels excess ΔL_I ⋈ ΔR and adds missing ΔL_D ⋈ ΔR.
SELECT {hash_correction} AS __pgt_row_id,
       CASE WHEN dl.__pgt_action = 'I'
            THEN CASE WHEN dr.__pgt_action = 'I' THEN 'D' ELSE 'I' END
            ELSE dr.__pgt_action
       END AS __pgt_action,
       {correction_cols}
FROM {delta_left} dl
JOIN {delta_right} dr ON {join_cond_correction}",
            delta_left = left_result.cte_name,
            delta_right = right_result.cte_name,
        )
    } else {
        String::new()
    };

    // ── G-J2: Pre-compute right-delta action flags ──────────────────
    //
    // Parts 4 and 5 each scan all current left rows joined with the right
    // delta. When the right delta is INSERT-only, Part 5 (which handles
    // right DELETEs) scans left_table for nothing. And vice versa.
    //
    // A single bool_or CTE evaluated once tells us which parts to run,
    // allowing PostgreSQL to skip the full left_table scan for whichever
    // part returns no rows.
    let flags_cte = ctx.next_cte_name("lj_right_flags");
    ctx.add_cte(
        flags_cte.clone(),
        format!(
            "SELECT bool_or(__pgt_action = 'I') AS has_ins,\
                    bool_or(__pgt_action = 'D') AS has_del \
             FROM {delta_right}",
            delta_right = right_result.cte_name,
        ),
    );

    let cte_name = ctx.next_cte_name("left_join");

    let sql = if let Some(ref r0) = r0_snapshot {
        // ── EC-01: Split Part 1 and Part 3 by action ────────────────
        // Part 1a: INSERTs ⋈ R₁  (inserts need current right partners)
        // Part 1b: DELETEs ⋈ R₀  (deletes need pre-change right partners)
        // Part 3a: INSERTs anti-join R₁  (no match in current right → NULL pad)
        // Part 3b: DELETEs anti-join R₀  (no match in pre-change right → NULL pad)
        format!(
            "\
-- Part 1a: delta_left INSERTS JOIN current_right R₁ (matching insert rows)
SELECT {hash_dl_r} AS __pgt_row_id,
       dl.__pgt_action,
       {part1_cols}
FROM {delta_left} dl
JOIN {right_table} r ON {join_cond_part1}
WHERE dl.__pgt_action = 'I'

UNION ALL

-- Part 1b: delta_left DELETES JOIN pre-change_right R₀ (EC-01 fix)
-- R₀ via NOT EXISTS anti-join + old rows (DI-2)
-- Ensures deleted left rows find their old right partner even when
-- the right partner was simultaneously deleted.
SELECT {hash_dl_r} AS __pgt_row_id,
       dl.__pgt_action,
       {part1_cols}
FROM {delta_left} dl
JOIN {r0_snapshot} r ON {join_cond_part1}
WHERE dl.__pgt_action = 'D'

UNION ALL

-- Part 2: pre-change left (L₀) JOIN delta_right
-- Uses L₀ instead of L₁ so right-side changes are attributed to the
-- correct (old) group key — fixes G17-STBASE overcounting.
SELECT {hash_l_dr} AS __pgt_row_id,
       dr.__pgt_action,
       {part2_cols}
FROM {left_part2} l
JOIN {delta_right} dr ON {join_cond_part2}

UNION ALL

-- Part 3a: delta_left INSERTS anti-join R₁ (non-matching inserts → NULL right cols)
SELECT dl.__pgt_row_id,
       dl.__pgt_action,
       {antijoin_cols}
FROM {delta_left} dl
WHERE dl.__pgt_action = 'I'
  AND NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {join_cond_antijoin}
)

UNION ALL

-- Part 3b: delta_left DELETES anti-join R₀ (non-matching deletes → NULL right cols)
-- Uses R₀ to match the Part 1b partition: a delete that matched R₀ goes to
-- Part 1b, a delete that didn't match R₀ goes here.
SELECT dl.__pgt_row_id,
       dl.__pgt_action,
       {antijoin_cols}
FROM {delta_left} dl
WHERE dl.__pgt_action = 'D'
  AND NOT EXISTS (
    SELECT 1 FROM {r0_snapshot} r WHERE {join_cond_antijoin}
)

UNION ALL

-- Part 4: Delete stale NULL-padded rows when a left row gains its FIRST right match.
-- When a right INSERT creates a new match for a left row that previously had NO
-- matching right rows (was NULL-padded), the NULL-padded ST row must be removed.
-- Use L₀ so simultaneous left changes delete the row that actually existed.
-- We check R_old (pre-change right) to verify the left row truly had no matches
-- before. Without this check, left rows that ALREADY had matches would get
-- spurious D(NULL-padded) rows that corrupt intermediate aggregate old-state
-- reconstruction via EXCEPT ALL/UNION ALL.
SELECT 0::BIGINT AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {l_null_padded_cols}
FROM {left_part2} l
WHERE (SELECT has_ins FROM {flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'I' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  )

UNION ALL

-- Part 5: Insert NULL-padded rows when a left row loses ALL right matches.
-- When a right DELETE removes the last match for a left row, the left row
-- reverts to NULL-padded. Check current right (post-changes) to verify no
-- remaining matches exist, AND check R_old to confirm the left row previously
-- HAD matches (otherwise it was already NULL-padded — no change needed).
SELECT 0::BIGINT AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {l_null_padded_cols}
FROM {left_table} l
WHERE (SELECT has_del FROM {flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'D' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {not_exists_cond}
  )
  AND EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  ){correction_sql}",
            delta_left = left_result.cte_name,
            delta_right = right_result.cte_name,
            r0_snapshot = r0,
            left_part2 = left_part2_source,
            flags_cte = flags_cte,
        )
    } else {
        // Right child is complex — keep Part 1 and Part 3 unsplit.
        format!(
            "\
-- Part 1: delta_left JOIN current_right (matching rows)
SELECT {hash_dl_r} AS __pgt_row_id,
       dl.__pgt_action,
       {part1_cols}
FROM {delta_left} dl
JOIN {right_table} r ON {join_cond_part1}

UNION ALL

-- Part 2: pre-change left (L₀) JOIN delta_right
SELECT {hash_l_dr} AS __pgt_row_id,
       dr.__pgt_action,
       {part2_cols}
FROM {left_part2} l
JOIN {delta_right} dr ON {join_cond_part2}

UNION ALL

-- Part 3: delta_left anti-join right (non-matching left rows get NULL right cols)
SELECT dl.__pgt_row_id,
       dl.__pgt_action,
       {antijoin_cols}
FROM {delta_left} dl
WHERE NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {join_cond_antijoin}
)

UNION ALL

-- Part 4: Delete stale NULL-padded rows when a left row gains its FIRST right match.
-- Use L₀ so simultaneous left changes delete the row that actually existed.
SELECT 0::BIGINT AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {l_null_padded_cols}
FROM {left_part2} l
WHERE (SELECT has_ins FROM {flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'I' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  )

UNION ALL

-- Part 5: Insert NULL-padded rows when a left row loses ALL right matches.
SELECT 0::BIGINT AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {l_null_padded_cols}
FROM {left_table} l
WHERE (SELECT has_del FROM {flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'D' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {not_exists_cond}
  )
  AND EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  ){correction_sql}",
            delta_left = left_result.cte_name,
            delta_right = right_result.cte_name,
            left_part2 = left_part2_source,
            flags_cte = flags_cte,
        )
    };

    ctx.add_cte(cte_name.clone(), sql);

    let left_output_cols: Vec<String> = left_cols
        .iter()
        .map(|column| format!("{left_prefix}__{column}"))
        .collect();
    let right_output_cols: Vec<String> = right_cols
        .iter()
        .map(|column| format!("{right_prefix}__{column}"))
        .collect();
    let schema = left_result
        .schema
        .renamed(&left_output_cols)
        .concat(&right_result.schema.renamed(&right_output_cols).nullable());

    Ok(DiffResult {
        cte_name,
        columns: output_cols.clone(),
        schema,
        is_deduplicated: false,
        has_key_changed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    #[test]
    fn test_diff_left_join_basic() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id", "amount"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = left_join(cond, left, right);
        let result = diff_left_join(&mut ctx, &tree).unwrap();

        // Output columns should be disambiguated
        assert!(result.columns.contains(&"o__id".to_string()));
        assert!(result.columns.contains(&"c__name".to_string()));
    }

    #[test]
    fn test_diff_left_join_has_all_parts() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = left_join(cond, left, right);
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // When L₀ is available (Scan children), Part 1 and Part 3 are NOT
        // split — standard formula is exact, no EC-01 needed.
        assert_sql_contains(&sql, "Part 1");
        assert_sql_not_contains(&sql, "Part 1a");
        assert_sql_not_contains(&sql, "Part 1b");
        assert_sql_contains(&sql, "Part 2");
        assert_sql_contains(&sql, "Part 3");
        assert_sql_not_contains(&sql, "Part 3a");
        assert_sql_not_contains(&sql, "Part 3b");
        assert_sql_contains(&sql, "Part 4");
        assert_sql_contains(&sql, "Part 5");
        // EC-02 correction (Part 6) is NOT needed without EC-01 split.
        assert_sql_not_contains(&sql, "Part 6");
    }

    #[test]
    fn test_ec01_left_join_no_r0_when_l0_available() {
        // When L₀ is available (Scan children), R₀ is NOT used — standard
        // formula is exact. Parts 1 and 3 are unsplit.
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id", "bid"]);
        let right = scan(2, "b", "public", "b", &["id", "name"]);
        let cond = eq_cond("a", "bid", "b", "id");
        let tree = left_join(cond, left, right);
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // L₀ uses NOT EXISTS anti-join (DI-2)
        assert_sql_contains(&sql, "NOT EXISTS");
        // Part 1 and Part 3 are NOT split
        assert_sql_not_contains(&sql, "Part 1b");
        assert_sql_not_contains(&sql, "Part 3b");
    }

    #[test]
    fn test_ec01_left_join_no_split_when_l0_available() {
        // When L₀ is available (Scan children), Parts 1/3 are NOT split.
        // Action filters should still exist in Parts 4/5 but not from EC-01.
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id"]);
        let right = scan(2, "b", "public", "b", &["id"]);
        let cond = eq_cond("a", "id", "b", "id");
        let tree = left_join(cond, left, right);
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Parts 1/3 should NOT be split
        assert_sql_not_contains(&sql, "Part 1a");
        assert_sql_not_contains(&sql, "Part 1b");
        assert_sql_not_contains(&sql, "Part 3a");
        assert_sql_not_contains(&sql, "Part 3b");
    }

    #[test]
    fn test_diff_left_join_null_padding() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = left_join(cond, left, right);
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Anti-join part should pad right columns with NULL
        assert_sql_contains(&sql, "NULL AS");
    }

    #[test]
    fn test_diff_left_join_right_delta_flags() {
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id"]);
        let right = scan(2, "b", "public", "b", &["id"]);
        let cond = eq_cond("a", "id", "b", "id");
        let tree = left_join(cond, left, right);
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // G-J2 optimization: pre-computed right-delta action flags
        assert_sql_contains(&sql, "has_ins");
        assert_sql_contains(&sql, "has_del");
    }

    #[test]
    fn test_diff_left_join_not_deduplicated() {
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id"]);
        let right = scan(2, "b", "public", "b", &["id"]);
        let cond = eq_cond("a", "id", "b", "id");
        let tree = left_join(cond, left, right);
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_left_join_error_on_non_left_join_node() {
        let mut ctx = test_ctx();
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_left_join(&mut ctx, &tree);
        assert!(result.is_err());
    }

    // ── Nested join tests ───────────────────────────────────────────

    #[test]
    fn test_diff_left_join_nested_three_tables() {
        // (a ⋈ b) LEFT JOIN c — left child is a nested inner join
        let a = scan(1, "a", "public", "a", &["id", "bid"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let inner = inner_join(eq_cond("a", "bid", "b", "id"), a, b);
        let c = scan(3, "c", "public", "c", &["id"]);
        let tree = left_join(eq_cond("a", "id", "c", "id"), inner, c);

        let mut ctx = test_ctx();
        let result = diff_left_join(&mut ctx, &tree);
        assert!(
            result.is_ok(),
            "nested 3-table left join should diff: {result:?}"
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert_sql_contains(&sql, "UNION ALL");
    }

    #[test]
    fn test_diff_left_join_nested_right_child() {
        // a LEFT JOIN (b ⋈ c) — right child is a nested inner join
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id", "cid"]);
        let c = scan(3, "c", "public", "c", &["id"]);
        let inner = inner_join(eq_cond("b", "cid", "c", "id"), b, c);
        let tree = left_join(eq_cond("a", "id", "b", "id"), a, inner);

        let mut ctx = test_ctx();
        let result = diff_left_join(&mut ctx, &tree);
        assert!(
            result.is_ok(),
            "left join with nested right child should diff: {result:?}"
        );
    }

    // ── NATURAL LEFT JOIN diff tests ────────────────────────────────

    #[test]
    fn test_diff_left_join_with_natural_condition() {
        // Simulate NATURAL LEFT JOIN: tables share "id" column
        let left = scan(1, "orders", "public", "o", &["id", "customer_id"]);
        let right = scan(2, "items", "public", "i", &["id", "order_id"]);
        let cond = natural_join_cond(&left, &right);
        let tree = left_join(cond, left, right);

        let mut ctx = test_ctx();
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        // Left join diff should have multiple parts with UNION ALL
        assert_sql_contains(&sql, "UNION ALL");
        // Disambiguated columns from both sides
        assert!(result.columns.contains(&"o__id".to_string()));
        assert!(result.columns.contains(&"i__id".to_string()));
    }

    #[test]
    fn test_diff_left_join_natural_multiple_common_cols() {
        // Two tables sharing "id" and "region"
        let left = scan(1, "a", "public", "a", &["id", "region", "val"]);
        let right = scan(2, "b", "public", "b", &["id", "region", "score"]);
        let cond = natural_join_cond(&left, &right);
        let tree = left_join(cond, left, right);

        let mut ctx = test_ctx();
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        assert!(result.columns.contains(&"a__id".to_string()));
        assert!(result.columns.contains(&"a__region".to_string()));
        assert!(result.columns.contains(&"b__region".to_string()));
        assert!(result.columns.contains(&"b__score".to_string()));
    }

    // ── L₀ and EC-02 tests ─────────────────────────────────────────

    #[test]
    fn test_left_join_part2_uses_l0_for_scan_children() {
        // When left child is a simple Scan, Part 2 should use L₀ (pre-change
        // left snapshot via NOT EXISTS) instead of L₁ (current left table).
        let mut ctx = test_ctx();
        let left = scan(1, "dept_tree", "public", "t", &["id", "name", "path"]);
        let right = scan(2, "employees", "public", "e", &["id", "dept_id"]);
        let cond = eq_cond("t", "id", "e", "dept_id");
        let tree = left_join(cond, left, right);
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Part 2 comment should mention L₀
        assert_sql_contains(&sql, "pre-change left");
        // L₀ uses NOT EXISTS pattern for Scan child
        // Count NOT EXISTS occurrences — should have multiple: L₀, R_old,
        // Part 4, Part 5 (no R₀ since L₀ is available)
        let not_exists_count = sql.matches("NOT EXISTS").count();
        assert!(
            not_exists_count >= 3,
            "expected at least 3 NOT EXISTS for L₀+R_old+Parts, got {not_exists_count}"
        );
    }

    #[test]
    fn test_ec02_left_join_no_correction_when_l0_available() {
        // When L₀ is available (simple Scan children), the standard formula
        // is exact and no EC-02 correction is needed.
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id", "key"]);
        let right = scan(2, "b", "public", "b", &["id", "val"]);
        let cond = eq_cond("a", "key", "b", "id");
        let tree = left_join(cond, left, right);
        let result = diff_left_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // EC-02 Part 6 should NOT be present.
        assert_sql_not_contains(&sql, "Part 6: EC-02 correction");
    }
}
