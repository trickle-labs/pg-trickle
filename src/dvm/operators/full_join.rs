//! Full outer join differentiation.
//!
//! FULL OUTER JOIN = INNER JOIN + left anti-join + right anti-join.
//!
//! The delta is a multi-part UNION ALL:
//!
//! ## L₀ path (simple/Scan children — default):
//!
//! 1. **Part 1** — delta_left JOIN R₁ (unsplit — standard DBSP L₀ formula)
//! 2. **Part 2** — L₀ JOIN delta_right (pre-change left, correct attribution)
//! 3. **Part 3a** — delta_left INSERTS anti-join R₁ (non-matching left → NULL right)
//! 4. **Part 3b** — delta_left DELETES anti-join R_old (non-matching left → NULL right)
//! 5. **Part 4** — Delete stale NULL-padded left rows when new right matches appear
//!    (guard: NOT EXISTS R_old — left was previously unmatched)
//! 6. **Part 5** — Insert NULL-padded left rows when last right match removed
//!    (guards: NOT EXISTS R₁, AND EXISTS R_old, AND NOT EXISTS ΔL_I-same-key)
//! 7. **Part 6a** — delta_right INSERTS anti-join L₁ (non-matching right → NULL left)
//! 8. **Part 6b** — delta_right DELETES anti-join L_old (non-matching right → NULL left)
//! 9. **Part 7a** — Delete stale NULL-padded right rows when new left matches appear
//!    (guard: NOT EXISTS L_old — right was previously unmatched)
//! 10. **Part 7b** — Insert NULL-padded right rows when right row loses all left matches
//!     (guards: NOT EXISTS L₁, AND EXISTS L_old, AND NOT EXISTS ΔR_I-same-key)
//!
//! ## EC-01 path (complex left children):
//!
//! 1. **Part 1a** — delta_left INSERTS JOIN R₁
//! 2. **Part 1b** — delta_left DELETES JOIN R₀ (EC-01 fix)
//! 3. **Part 2** — current_left JOIN delta_right
//! 4. **Part 3a** — delta_left INSERTS anti-join R₁
//! 5. **Part 3b** — delta_left DELETES anti-join R₀
//!    6–10. Parts 4–7b same as L₀ path with R_old/L_old guards
//!
//! ## L₀ fix: Pre-change left snapshot for Part 2
//!
//! Standard DBSP formula: Δ(L ⋈ R) = (ΔL ⋈ R₁) + (L₀ ⋈ ΔR).
//! Using L₀ in Part 2 ensures right-side changes are attributed to the
//! old (pre-change) left values, preventing double-counting when both
//! sides change simultaneously (e.g. L UPDATE + R DELETE for same key).
//!
//! ## L₀ and EC-01 are mutually exclusive
//!
//! When L₀ is available (simple left children), EC-01 R₀ split for
//! Part 1/3 is NOT used. The standard formula (ΔL ⋈ R₁) + (L₀ ⋈ ΔR)
//! is already exact; splitting Part 1 would double-count Part 1b and Part 2(L₀).
//!
//! ## R_old and L_old guards for Parts 4/5 and 7a/7b
//!
//! Parts 4/5 handle left rows transitioning between matched and null-padded
//! states due to right-side changes. Guards prevent spurious transitions:
//! - Part 4 NOT EXISTS R_old: fires only when left was previously unmatched
//! - Part 5 AND EXISTS R_old: fires only when left previously had a match
//! - Part 5 AND NOT EXISTS ΔL_I-same-key: prevents duplicate with Part 3a
//!   when a left UPDATE and right DELETE happen for the same key in the same cycle
//!
//! Parts 7a/7b have symmetric guards for the right side.

use crate::dvm::diff::{DiffContext, DiffResult, quote_ident};
use crate::dvm::operators::join::mark_leaf_delta_ctes_not_materialized;
use crate::dvm::operators::join_common::{
    build_leaf_snapshot_sql, build_snapshot_sql, extract_equijoin_keys_aliased,
    rewrite_join_condition,
};
use crate::dvm::parser::OpTree;
use crate::dvm::snapshot::SnapshotPlan;
use crate::error::PgTrickleError;

/// Differentiate a FullJoin node.
pub fn diff_full_join(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::FullJoin {
        condition,
        left,
        right,
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_full_join called on non-FullJoin node".into(),
        ));
    };

    // Differentiate both children
    let left_result = ctx.diff_node(left)?;
    let right_result = ctx.diff_node(right)?;

    // Rewrite join conditions for each part
    let join_cond_part1 = rewrite_join_condition(condition, left, "dl", right, "r");
    let join_cond_part2 = rewrite_join_condition(condition, left, "l", right, "dr");
    let join_cond_antijoin_l = rewrite_join_condition(condition, left, "dl", right, "r");
    let join_cond_antijoin_r = rewrite_join_condition(condition, left, "l", right, "dr");
    let not_exists_cond_lr = rewrite_join_condition(condition, left, "l", right, "r");

    let left_cols = &left_result.columns;
    let right_cols = &right_result.columns;

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

    // Column references for each part
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
    let null_left_cols: Vec<String> = left_cols
        .iter()
        .map(|c| format!("NULL AS {}", quote_ident(&format!("{left_prefix}__{c}"))))
        .collect();

    let part1_cols = [dl_cols.as_slice(), r_cols.as_slice()].concat().join(", ");
    let part2_cols = [l_cols.as_slice(), dr_cols.as_slice()].concat().join(", ");
    let antijoin_left_cols = [dl_cols.as_slice(), null_right_cols.as_slice()]
        .concat()
        .join(", ");
    let antijoin_right_cols = [null_left_cols.as_slice(), dr_cols.as_slice()]
        .concat()
        .join(", ");

    // Null-padded columns for left-side transitions (Parts 4 & 5)
    let l_null_right_padded = [l_cols.as_slice(), null_right_cols.as_slice()]
        .concat()
        .join(", ");

    // Null-padded columns for right-side transitions (Part 7)
    let null_left_r_padded = [null_left_cols.as_slice(), r_cols.as_slice()]
        .concat()
        .join(", ");

    // ── L₀ fix: Pre-change left snapshot for Part 2 ────────────────
    //
    // Standard DBSP: Δ(L ⋈ R) inner part = (ΔL ⋈ R₁) + (L₀ ⋈ ΔR).
    // When both sides change simultaneously (e.g. L UPDATE + R DELETE for
    // the same key), using L₁ in Part 2 generates spurious deletes for
    // rows that never existed in J₀. Using L₀ avoids this.
    //
    // L₀ and EC-01 R₀ are mutually exclusive: when L₀ is available,
    // Part 1 is unsplit (no EC-01), and the standard formula is exact.
    let left_plan = SnapshotPlan::for_tree_in_context(left, ctx.inside_semijoin);
    let right_plan = SnapshotPlan::for_tree_in_context(right, ctx.inside_semijoin);
    let use_per_leaf_l0 = left_plan.uses_per_leaf();
    let use_l0 = left_plan.uses_pre_change();
    debug_assert_eq!(use_per_leaf_l0, left_plan.uses_per_leaf());
    debug_assert_eq!(use_l0, left_plan.uses_pre_change());

    // ── EC-01: Pre-change right snapshot for Parts 1b / 3b ─────────
    //
    // Only used when L₀ is NOT available (complex left children).
    // DI-11: Use same threshold as inner join for deep R₀ reconstruction.
    let use_r0 = if use_l0 {
        false
    } else {
        right_plan.uses_pre_change()
    };

    let r0_snapshot = if use_r0 {
        if right_plan.uses_per_leaf() {
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
                ctx.fallback_leaf_oids(),
                ctx.st_source_pgt_ids(),
            );
            Some(r0)
        }
    } else {
        None
    };

    if use_r0 {
        ctx.mark_cte_not_materialized(&right_result.cte_name);
    }

    // ── L₀ snapshot for Part 2 (when use_l0=true) ──────────────────
    let left_part2_source = if use_l0 {
        if use_per_leaf_l0 {
            let pre_change = ctx.get_or_register_snapshot_cte(left);
            mark_leaf_delta_ctes_not_materialized(left, ctx);
            pre_change
        } else {
            build_leaf_snapshot_sql(
                left,
                &left_result.cte_name,
                left_cols,
                ctx.fallback_leaf_oids(),
                ctx.st_source_pgt_ids(),
            )
        }
    } else {
        left_table.clone()
    };

    if use_l0 {
        ctx.mark_cte_not_materialized(&left_result.cte_name);
    }

    // ── R_old snapshot: pre-change right for Parts 3b/4/5 guards ───
    //
    // Always computed (unlike r0_snapshot which is only for EC-01).
    // Used in Part 3b anti-join, Part 4 NOT EXISTS guard, and Part 5
    // AND EXISTS guard to check whether left rows were previously matched.
    let right_user_cols: Vec<String> = right_cols
        .iter()
        .filter(|c| *c != "__pgt_count")
        .cloned()
        .collect();
    let r_old_snapshot = if right_plan.uses_per_leaf() {
        ctx.get_or_register_snapshot_cte(right)
    } else {
        build_leaf_snapshot_sql(
            right,
            &right_result.cte_name,
            &right_user_cols,
            ctx.fallback_leaf_oids(),
            ctx.st_source_pgt_ids(),
        )
    };

    // ── L_old snapshot: pre-change left for Parts 6b/7a/7b guards ──
    //
    // Symmetric to r_old_snapshot for the right anti-join side.
    let left_user_cols: Vec<String> = left_cols
        .iter()
        .filter(|c| *c != "__pgt_count")
        .cloned()
        .collect();
    let l_old_snapshot = if left_plan.uses_per_leaf() {
        ctx.get_or_register_snapshot_cte(left)
    } else {
        build_leaf_snapshot_sql(
            left,
            &left_result.cte_name,
            &left_user_cols,
            ctx.fallback_leaf_oids(),
            ctx.st_source_pgt_ids(),
        )
    };

    // ── Join conditions for R_old / L_old guards ────────────────────
    //
    // r_old_cond: `l.k = __pgt_r_old.k` — for Parts 4/5 NOT EXISTS/EXISTS.
    // l_old_cond: `__pgt_l_old.k = r.k` — for Parts 7a/7b NOT EXISTS/EXISTS.
    let r_old_cond = rewrite_join_condition(condition, left, "l", right, "__pgt_r_old");
    let l_old_cond = rewrite_join_condition(condition, left, "__pgt_l_old", right, "r");

    // ── Part 5 / Part 7b exclusion guards ───────────────────────────
    //
    // Prevents Part 5 from duplicating Part 3a when both a left row
    // INSERT (ΔL_I) and a right DELETE (ΔR_D) occur for the same join key
    // in the same cycle. Part 3a handles the null-padded insertion for new
    // left rows; Part 5 should only fire for pre-existing left rows.
    //
    // Guard: AND NOT EXISTS (SELECT 1 FROM delta_left dl2
    //                        WHERE dl2.__pgt_action = 'I'
    //                          AND join_cond(l, dl2))
    let current_keys = extract_equijoin_keys_aliased(condition, left, "l", right, "r");
    let delta_left_keys = extract_equijoin_keys_aliased(condition, left, "dl2", right, "r");
    let delta_right_keys = extract_equijoin_keys_aliased(condition, left, "l", right, "dr2");
    let same_side_condition = |current: &[(String, String)],
                               delta: &[(String, String)],
                               current_alias: &str,
                               delta_alias: &str| {
        let current_prefix = format!("\"{current_alias}\".");
        let delta_prefix = format!("\"{delta_alias}\".");
        let comparisons = current
            .iter()
            .zip(delta)
            .filter_map(|(current_pair, delta_pair)| {
                let current_expr = [&current_pair.0, &current_pair.1]
                    .into_iter()
                    .find(|expr| expr.contains(&current_prefix))?;
                let delta_expr = [&delta_pair.0, &delta_pair.1]
                    .into_iter()
                    .find(|expr| expr.contains(&delta_prefix))?;
                Some(format!("{current_expr} IS NOT DISTINCT FROM {delta_expr}"))
            })
            .collect::<Vec<_>>();
        if comparisons.is_empty() {
            "FALSE".to_string()
        } else {
            comparisons.join(" AND ")
        }
    };
    let part5_excl_cond = same_side_condition(&current_keys, &delta_left_keys, "l", "dl2");
    // Symmetric for Part 7b: AND NOT EXISTS ΔR_I with the same right-side key.
    let part7b_excl_cond = same_side_condition(&current_keys, &delta_right_keys, "r", "dr2");

    // ── Part 4 exclusion guard ───────────────────────────────────────
    //
    // Prevents Part 4 from spuriously firing D(l_new, NULL) when both a
    // new left row is INSERTED and a new right row is INSERTED for the same
    // join key in the same cycle. Part 4's purpose is to remove a
    // pre-existing null-padded left row when it gains its first right match.
    // If the left row is itself new (appearing in ΔL), it was never
    // null-padded so Part 4 must not fire.
    //
    // This guard also prevents duplication with Part 3b when L UPDATE
    // (ΔL has DEL) + R INSERT happen for same key: Part 3b handles
    // D(l_old, NULL), so Part 4 must stay silent.
    //
    // Guard: AND NOT EXISTS (SELECT 1 FROM delta_left dl2
    //                        WHERE join_cond(l, dl2))   -- any ΔL action
    // Reuses part5_excl_cond (same key-matching expression) without action filter.
    let part4_excl_cond = &part5_excl_cond;

    // ── Pre-compute delta action flags for both sides ──────────────
    let left_flags_cte = ctx.next_cte_name("fj_left_flags");
    ctx.add_cte(
        left_flags_cte.clone(),
        format!(
            "SELECT bool_or(__pgt_action = 'I') AS has_ins,\
                    bool_or(__pgt_action = 'D') AS has_del \
             FROM {delta_left}",
            delta_left = left_result.cte_name,
        ),
    );

    let right_flags_cte = ctx.next_cte_name("fj_right_flags");
    ctx.add_cte(
        right_flags_cte.clone(),
        format!(
            "SELECT bool_or(__pgt_action = 'I') AS has_ins,\
                    bool_or(__pgt_action = 'D') AS has_del \
             FROM {delta_right}",
            delta_right = right_result.cte_name,
        ),
    );

    let cte_name = ctx.next_cte_name("full_join");
    let hash_dl_r = crate::hash::build_row_identity_expr(
        "JOIN_KEY",
        &[
            "dl.__pgt_row_id".to_string(),
            "row_to_json(r)::text".to_string(),
        ],
    );
    let hash_l_dr = crate::hash::build_row_identity_expr(
        "JOIN_KEY",
        &[
            "row_to_json(l)::text".to_string(),
            "dr.__pgt_row_id".to_string(),
        ],
    );

    let sql = if use_l0 {
        // ── L₀ path: standard DBSP formula with comprehensive guards ─
        //
        // Part 1 unsplit (no EC-01): ΔL ⋈ R₁ — standard DBSP left term.
        // Part 2: L₀ ⋈ ΔR — right term uses pre-change left for correct
        //   attribution when both sides change simultaneously.
        // Part 3 split by action using R_old: prevents spurious nulls.
        // Parts 4/5/7a/7b have R_old/L_old guards and exclusion guards.
        format!(
            "\
-- Part 1: delta_left JOIN current_right R₁ (unsplit — standard DBSP L₀ formula)
SELECT {hash_dl_r} AS __pgt_row_id,
       dl.__pgt_action,
       {part1_cols}
FROM {delta_left} dl
JOIN {right_table} r ON {join_cond_part1}

UNION ALL

-- Part 2: L₀ (pre-change left) JOIN delta_right
-- Uses pre-change left state to avoid double-counting when L is updated
-- and R is changed simultaneously for the same join key.
SELECT {hash_l_dr} AS __pgt_row_id,
       dr.__pgt_action,
       {part2_cols}
FROM {left_part2} l
JOIN {delta_right} dr ON {join_cond_part2}

UNION ALL

-- Part 3a: delta_left INSERTS anti-join R₁ (non-matching inserts → NULL right cols)
SELECT dl.__pgt_row_id,
       dl.__pgt_action,
       {antijoin_left_cols}
FROM {delta_left} dl
WHERE dl.__pgt_action = 'I'
  AND NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {join_cond_antijoin_l}
)

UNION ALL

-- Part 3b: delta_left DELETES anti-join R_old (non-matching deletes → NULL right cols)
-- Uses R_old (pre-change right) so that simultaneously-deleted right rows
-- are correctly excluded from the anti-join (avoiding spurious null-padded DELETEs
-- for rows that were matched in J₀ but whose right partner was also deleted).
SELECT dl.__pgt_row_id,
       dl.__pgt_action,
       {antijoin_left_cols}
FROM {delta_left} dl
WHERE dl.__pgt_action = 'D'
  AND NOT EXISTS (
    SELECT 1 FROM {r_old_snapshot} r WHERE {join_cond_antijoin_l}
)

UNION ALL

-- Part 4: Delete stale NULL-padded left rows when a left row gains its FIRST right match.
-- Source: L₀ (pre-change left) — excludes newly-inserted left rows that were never null-padded.
-- Guard NOT EXISTS R_old: only fire when left was previously unmatched (null-padded).
-- Guard NOT EXISTS ΔL same-key: prevents duplication with Part 3b when L is also
--   being changed (UPDATE or INSERT) in the same cycle — those parts already handle the
--   null-padded removal; Part 4 must only fire for pre-existing, unchanging left rows.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {l_null_right_padded}
FROM {left_part2} l
WHERE (SELECT has_ins FROM {right_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'I' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {delta_left} dl2 WHERE {part4_excl_cond}
  )

UNION ALL

-- Part 5: Insert NULL-padded left rows when a left row loses ALL right matches.
-- Guard AND EXISTS R_old: fires only when left row previously had a match.
-- Guard AND NOT EXISTS ΔL_I same-key: prevents duplicate with Part 3a when a
-- left INSERT (from UPDATE) and a right DELETE happen for the same key in the
-- same cycle — Part 3a handles new left rows; Part 5 handles pre-existing ones.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {l_null_right_padded}
FROM {left_table} l
WHERE (SELECT has_del FROM {right_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'D' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {not_exists_cond_lr}
  )
  AND EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {delta_left} dl2 WHERE dl2.__pgt_action = 'I' AND {part5_excl_cond}
  )

UNION ALL

-- Part 6a: delta_right INSERTS anti-join L₁ (non-matching inserts → NULL left cols)
SELECT dr.__pgt_row_id,
       dr.__pgt_action,
       {antijoin_right_cols}
FROM {delta_right} dr
WHERE dr.__pgt_action = 'I'
  AND NOT EXISTS (
    SELECT 1 FROM {left_table} l WHERE {join_cond_antijoin_r}
)

UNION ALL

-- Part 6b: delta_right DELETES anti-join L_old (non-matching deletes → NULL left cols)
-- Uses L_old (pre-change left) so that simultaneously-deleted left rows are
-- correctly excluded from the anti-join (avoiding spurious null-padded DELETEs
-- for right rows that were matched in J₀ but whose left partner was also deleted).
SELECT dr.__pgt_row_id,
       dr.__pgt_action,
       {antijoin_right_cols}
FROM {delta_right} dr
WHERE dr.__pgt_action = 'D'
  AND NOT EXISTS (
    SELECT 1 FROM {l_old_snapshot} l WHERE {join_cond_antijoin_r}
)

UNION ALL

-- Part 7a: Delete stale NULL-padded right rows when a right row gains its FIRST left match.
-- Uses R_old (pre-change right) to find right rows that existed before the left INSERT.
-- Guard NOT EXISTS L_old: fires only when right was previously unmatched (null-padded).
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {null_left_r_padded}
FROM {r_old_snapshot} r
WHERE (SELECT has_ins FROM {left_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_left} dl
    WHERE dl.__pgt_action = 'I' AND {join_cond_antijoin_l}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {l_old_snapshot} __pgt_l_old WHERE {l_old_cond}
  )

UNION ALL

-- Part 7b: Insert NULL-padded right rows when a right row loses ALL left matches.
-- Guard AND EXISTS L_old: fires only when right row previously had a left match.
-- Guard AND NOT EXISTS ΔR_I same-key: prevents duplicate with Part 6a when a
-- right INSERT (from UPDATE) and a left DELETE happen for the same key in the
-- same cycle — Part 6a handles new right rows; Part 7b handles pre-existing ones.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {null_left_r_padded}
FROM {right_table} r
WHERE (SELECT has_del FROM {left_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_left} dl
    WHERE dl.__pgt_action = 'D' AND {join_cond_antijoin_l}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {left_table} l WHERE {not_exists_cond_lr}
  )
  AND EXISTS (
    SELECT 1 FROM {l_old_snapshot} __pgt_l_old WHERE {l_old_cond}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {delta_right} dr2 WHERE dr2.__pgt_action = 'I' AND {part7b_excl_cond}
  )",
            delta_left = left_result.cte_name,
            delta_right = right_result.cte_name,
            left_part2 = left_part2_source,
            r_old_snapshot = r_old_snapshot,
            l_old_snapshot = l_old_snapshot,
            left_flags_cte = left_flags_cte,
            right_flags_cte = right_flags_cte,
        )
    } else if let Some(ref r0) = r0_snapshot {
        // ── EC-01 path: Split Part 1 and Part 3 by action ────────────
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
SELECT {hash_dl_r} AS __pgt_row_id,
       dl.__pgt_action,
       {part1_cols}
FROM {delta_left} dl
JOIN {r0_snapshot} r ON {join_cond_part1}
WHERE dl.__pgt_action = 'D'

UNION ALL

-- Part 2: current_left JOIN delta_right
SELECT {hash_l_dr} AS __pgt_row_id,
       dr.__pgt_action,
       {part2_cols}
FROM {left_table} l
JOIN {delta_right} dr ON {join_cond_part2}

UNION ALL

-- Part 3a: delta_left INSERTS anti-join R₁ (non-matching inserts → NULL right cols)
SELECT dl.__pgt_row_id,
       dl.__pgt_action,
       {antijoin_left_cols}
FROM {delta_left} dl
WHERE dl.__pgt_action = 'I'
  AND NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {join_cond_antijoin_l}
)

UNION ALL

-- Part 3b: delta_left DELETES anti-join R₀ (non-matching deletes → NULL right cols)
SELECT dl.__pgt_row_id,
       dl.__pgt_action,
       {antijoin_left_cols}
FROM {delta_left} dl
WHERE dl.__pgt_action = 'D'
  AND NOT EXISTS (
    SELECT 1 FROM {r0_snapshot} r WHERE {join_cond_antijoin_l}
)

UNION ALL

-- Part 4: Delete stale NULL-padded left rows when new right matches appear
-- Guard NOT EXISTS R_old: left was previously unmatched.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {l_null_right_padded}
FROM {left_table} l
WHERE (SELECT has_ins FROM {right_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'I' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {delta_left} dl2 WHERE {part4_excl_cond}
  )

UNION ALL

-- Part 5: Insert NULL-padded left rows when left row loses all right matches
-- Guards: NOT EXISTS R₁, AND EXISTS R_old, AND NOT EXISTS ΔL_I same-key.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {l_null_right_padded}
FROM {left_table} l
WHERE (SELECT has_del FROM {right_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'D' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {not_exists_cond_lr}
  )
  AND EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {delta_left} dl2 WHERE dl2.__pgt_action = 'I' AND {part5_excl_cond}
  )

UNION ALL

-- Part 6a: delta_right INSERTS anti-join L₁ (non-matching right rows → NULL left cols)
SELECT dr.__pgt_row_id,
       dr.__pgt_action,
       {antijoin_right_cols}
FROM {delta_right} dr
WHERE dr.__pgt_action = 'I'
  AND NOT EXISTS (
    SELECT 1 FROM {left_table} l WHERE {join_cond_antijoin_r}
)

UNION ALL

-- Part 6b: delta_right DELETES anti-join L_old (non-matching deletes → NULL left cols)
SELECT dr.__pgt_row_id,
       dr.__pgt_action,
       {antijoin_right_cols}
FROM {delta_right} dr
WHERE dr.__pgt_action = 'D'
  AND NOT EXISTS (
    SELECT 1 FROM {l_old_snapshot} l WHERE {join_cond_antijoin_r}
)

UNION ALL

-- Part 7a: Delete stale NULL-padded right rows when new left matches appear
-- Uses r0_snapshot (pre-change right) to find right rows deleted in same cycle.
-- Guard NOT EXISTS L_old: right was previously unmatched.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {null_left_r_padded}
FROM {r0_snapshot} r
WHERE (SELECT has_ins FROM {left_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_left} dl
    WHERE dl.__pgt_action = 'I' AND {join_cond_antijoin_l}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {l_old_snapshot} __pgt_l_old WHERE {l_old_cond}
  )

UNION ALL

-- Part 7b: Insert NULL-padded right rows when right row loses all left matches
-- Guards: NOT EXISTS L₁, AND EXISTS L_old, AND NOT EXISTS ΔR_I same-key.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {null_left_r_padded}
FROM {right_table} r
WHERE (SELECT has_del FROM {left_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_left} dl
    WHERE dl.__pgt_action = 'D' AND {join_cond_antijoin_l}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {left_table} l WHERE {not_exists_cond_lr}
  )
  AND EXISTS (
    SELECT 1 FROM {l_old_snapshot} __pgt_l_old WHERE {l_old_cond}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {delta_right} dr2 WHERE dr2.__pgt_action = 'I' AND {part7b_excl_cond}
  )",
            delta_left = left_result.cte_name,
            delta_right = right_result.cte_name,
            r0_snapshot = r0,
            r_old_snapshot = r_old_snapshot,
            l_old_snapshot = l_old_snapshot,
            left_flags_cte = left_flags_cte,
            right_flags_cte = right_flags_cte,
        )
    } else {
        // ── Fallback: both children complex, no snapshot available ───
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

-- Part 2: current_left JOIN delta_right
SELECT {hash_l_dr} AS __pgt_row_id,
       dr.__pgt_action,
       {part2_cols}
FROM {left_table} l
JOIN {delta_right} dr ON {join_cond_part2}

UNION ALL

-- Part 3: delta_left anti-join right (non-matching left rows → NULL right cols)
SELECT dl.__pgt_row_id,
       dl.__pgt_action,
       {antijoin_left_cols}
FROM {delta_left} dl
WHERE NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {join_cond_antijoin_l}
)

UNION ALL

-- Part 4: Delete stale NULL-padded left rows when new right matches appear
-- Guard NOT EXISTS R_old: left was previously unmatched.
-- Guard NOT EXISTS ΔL same-key: prevents spurious fire when L is also changing
--   (INSERT or UPDATE) — Part 3b handles those transitions.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {l_null_right_padded}
FROM {left_table} l
WHERE (SELECT has_ins FROM {right_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'I' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {delta_left} dl2 WHERE {part4_excl_cond}
  )

UNION ALL

-- Part 5: Insert NULL-padded left rows when left row loses all right matches
-- Guards: NOT EXISTS R₁, AND EXISTS R_old, AND NOT EXISTS ΔL_I same-key.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {l_null_right_padded}
FROM {left_table} l
WHERE (SELECT has_del FROM {right_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_right} dr
    WHERE dr.__pgt_action = 'D' AND {join_cond_part2}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {right_table} r WHERE {not_exists_cond_lr}
  )
  AND EXISTS (
    SELECT 1 FROM {r_old_snapshot} __pgt_r_old WHERE {r_old_cond}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {delta_left} dl2 WHERE dl2.__pgt_action = 'I' AND {part5_excl_cond}
  )

UNION ALL

-- Part 6: delta_right anti-join left (non-matching right rows → NULL left cols)
SELECT dr.__pgt_row_id,
       dr.__pgt_action,
       {antijoin_right_cols}
FROM {delta_right} dr
WHERE NOT EXISTS (
    SELECT 1 FROM {left_table} l WHERE {join_cond_antijoin_r}
)

UNION ALL

-- Part 7a: Delete stale NULL-padded right rows when new left matches appear
-- Guard NOT EXISTS L_old: right was previously unmatched.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {null_left_r_padded}
FROM {right_table} r
WHERE (SELECT has_ins FROM {left_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_left} dl
    WHERE dl.__pgt_action = 'I' AND {join_cond_antijoin_l}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {l_old_snapshot} __pgt_l_old WHERE {l_old_cond}
  )

UNION ALL

-- Part 7b: Insert NULL-padded right rows when right row loses all left matches
-- Guards: NOT EXISTS L₁, AND EXISTS L_old, AND NOT EXISTS ΔR_I same-key.
SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(0::int8)) AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {null_left_r_padded}
FROM {right_table} r
WHERE (SELECT has_del FROM {left_flags_cte})
  AND EXISTS (
    SELECT 1 FROM {delta_left} dl
    WHERE dl.__pgt_action = 'D' AND {join_cond_antijoin_l}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {left_table} l WHERE {not_exists_cond_lr}
  )
  AND EXISTS (
    SELECT 1 FROM {l_old_snapshot} __pgt_l_old WHERE {l_old_cond}
  )
  AND NOT EXISTS (
    SELECT 1 FROM {delta_right} dr2 WHERE dr2.__pgt_action = 'I' AND {part7b_excl_cond}
  )",
            delta_left = left_result.cte_name,
            delta_right = right_result.cte_name,
            r_old_snapshot = r_old_snapshot,
            l_old_snapshot = l_old_snapshot,
            left_flags_cte = left_flags_cte,
            right_flags_cte = right_flags_cte,
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
        .nullable()
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
    fn test_diff_full_join_basic() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id", "amount"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = OpTree::FullJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_full_join(&mut ctx, &tree).unwrap();

        // Output columns should be disambiguated
        assert!(result.columns.contains(&"o__id".to_string()));
        assert!(result.columns.contains(&"o__cust_id".to_string()));
        assert!(result.columns.contains(&"c__id".to_string()));
        assert!(result.columns.contains(&"c__name".to_string()));
    }

    #[test]
    fn test_diff_full_join_has_all_parts() {
        let mut ctx = test_ctx();
        let left = scan(1, "orders", "public", "o", &["id", "cust_id"]);
        let right = scan(2, "customers", "public", "c", &["id", "name"]);
        let cond = eq_cond("o", "cust_id", "c", "id");
        let tree = OpTree::FullJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_full_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // L₀ path: Parts 1 and 2 are standard DBSP (unsplit), Parts 3a/3b split.
        // When both left and right are Scan children, use_l0=true → no EC-01 split.
        assert_sql_contains(&sql, "Part 1");
        assert_sql_not_contains(&sql, "Part 1a");
        assert_sql_not_contains(&sql, "Part 1b");
        assert_sql_contains(&sql, "Part 2");
        assert_sql_contains(&sql, "Part 3a");
        assert_sql_contains(&sql, "Part 3b");
        assert_sql_contains(&sql, "Part 4");
        assert_sql_contains(&sql, "Part 5");
        assert_sql_contains(&sql, "Part 6a");
        assert_sql_contains(&sql, "Part 6b");
        assert_sql_contains(&sql, "Part 7a");
        assert_sql_contains(&sql, "Part 7b");
        // R_old and L_old guards present
        assert_sql_contains(&sql, "__pgt_r_old");
        assert_sql_contains(&sql, "__pgt_l_old");
        assert_sql_contains(
            &sql,
            "\"l\".\"cust_id\" IS NOT DISTINCT FROM \"dl2\".\"cust_id\"",
        );
        assert_sql_contains(&sql, "\"r\".\"id\" IS NOT DISTINCT FROM \"dr2\".\"id\"");
    }

    #[test]
    fn test_l0_full_join_uses_pre_change_left_for_part2() {
        // For Scan children (use_l0=true), Part 2 should use L₀ (pre-change left).
        // This prevents spurious DELETEs when both L is updated and R is changed
        // for the same join key in the same cycle.
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id"]);
        let right = scan(2, "b", "public", "b", &["id", "name"]);
        let cond = eq_cond("a", "id", "b", "id");
        let tree = OpTree::FullJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_full_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // L₀ uses NOT EXISTS anti-join (DI-2) — EXCEPT ALL pattern
        assert_sql_contains(&sql, "NOT EXISTS");
        // Part 1 is NOT split (no EC-01 for Scan children with L₀)
        assert_sql_not_contains(&sql, "Part 1a");
        assert_sql_not_contains(&sql, "Part 1b");
        // Part 3 IS split by action using R_old
        assert_sql_contains(&sql, "Part 3a");
        assert_sql_contains(&sql, "Part 3b");
        // R_old and L_old guards for Parts 4/5 and 7a/7b
        assert_sql_contains(&sql, "__pgt_r_old");
        assert_sql_contains(&sql, "__pgt_l_old");
    }

    #[test]
    fn test_diff_full_join_null_padding_both_sides() {
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id", "val"]);
        let right = scan(2, "b", "public", "b", &["id", "name"]);
        let cond = eq_cond("a", "id", "b", "id");
        let tree = OpTree::FullJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_full_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should have NULL padding for both sides
        assert_sql_contains(&sql, "NULL AS");
    }

    #[test]
    fn test_diff_full_join_delta_flags() {
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id"]);
        let right = scan(2, "b", "public", "b", &["id"]);
        let cond = eq_cond("a", "id", "b", "id");
        let tree = OpTree::FullJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_full_join(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should have pre-computed delta flags for both sides
        assert_sql_contains(&sql, "has_ins");
        assert_sql_contains(&sql, "has_del");
    }

    #[test]
    fn test_diff_full_join_not_deduplicated() {
        let mut ctx = test_ctx();
        let left = scan(1, "a", "public", "a", &["id"]);
        let right = scan(2, "b", "public", "b", &["id"]);
        let cond = eq_cond("a", "id", "b", "id");
        let tree = OpTree::FullJoin {
            condition: cond,
            left: Box::new(left),
            right: Box::new(right),
        };
        let result = diff_full_join(&mut ctx, &tree).unwrap();
        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_full_join_error_on_non_full_join_node() {
        let mut ctx = test_ctx();
        let tree = scan(1, "t", "public", "t", &["id"]);
        let result = diff_full_join(&mut ctx, &tree);
        assert!(result.is_err());
    }

    // ── Nested join tests ───────────────────────────────────────────

    #[test]
    fn test_diff_full_join_nested_left_child() {
        // (a ⋈ b) FULL JOIN c — left child is a nested inner join
        let a = scan(1, "a", "public", "a", &["id", "bid"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let inner = inner_join(eq_cond("a", "bid", "b", "id"), a, b);
        let c = scan(3, "c", "public", "c", &["id"]);
        let tree = OpTree::FullJoin {
            condition: eq_cond("a", "id", "c", "id"),
            left: Box::new(inner),
            right: Box::new(c),
        };

        let mut ctx = test_ctx();
        let result = diff_full_join(&mut ctx, &tree);
        assert!(
            result.is_ok(),
            "full join with nested left child should diff: {result:?}"
        );
        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert_sql_contains(&sql, "UNION ALL");
    }

    #[test]
    fn test_diff_full_join_nested_both_children() {
        // (a ⋈ b) FULL JOIN (c ⋈ d) — both children are nested joins
        let a = scan(1, "a", "public", "a", &["id"]);
        let b = scan(2, "b", "public", "b", &["id"]);
        let left = inner_join(eq_cond("a", "id", "b", "id"), a, b);

        let c = scan(3, "c", "public", "c", &["id"]);
        let d = scan(4, "d", "public", "d", &["id"]);
        let right = inner_join(eq_cond("c", "id", "d", "id"), c, d);

        let tree = OpTree::FullJoin {
            condition: eq_cond("a", "id", "c", "id"),
            left: Box::new(left),
            right: Box::new(right),
        };

        let mut ctx = test_ctx();
        let result = diff_full_join(&mut ctx, &tree);
        assert!(
            result.is_ok(),
            "full join with nested children on both sides should diff: {result:?}"
        );
    }

    // ── Multi-table cross-type join chain tests ─────────────────────

    /// `A FULL JOIN (B SEMI JOIN C)` — right child is a semi-join.
    ///
    /// A full outer join where the right sub-tree filters rows via a semi-join.
    /// Verifies that the full-join differentiator handles a non-standard right
    /// child that emits only left-side columns.
    #[test]
    fn test_diff_full_join_with_semi_join_right_child() {
        // B SEMI JOIN C: orders EXISTS IN customers
        let b = scan(2, "orders", "public", "o", &["id", "cust_id"]);
        let c = scan(3, "customers", "public", "c", &["id"]);
        let right = OpTree::SemiJoin {
            condition: eq_cond("o", "cust_id", "c", "id"),
            left: Box::new(b),
            right: Box::new(c),
        };

        // A FULL JOIN (B SEMI JOIN C): regions FULL JOIN above
        let a = scan(1, "regions", "public", "r", &["id", "name"]);
        let tree = OpTree::FullJoin {
            condition: eq_cond("r", "id", "o", "id"),
            left: Box::new(a),
            right: Box::new(right),
        };

        let mut ctx = test_ctx();
        let result = diff_full_join(&mut ctx, &tree);
        assert!(
            result.is_ok(),
            "A FULL JOIN (B SEMI JOIN C) should succeed: {result:?}"
        );

        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        // Full join always emits UNION ALL across many parts
        assert_sql_contains(&sql, "UNION ALL");
        // SQL must reference the semi-join EXISTS check
        assert!(
            sql.contains("EXISTS"),
            "right sub-tree (semi-join) must appear via EXISTS"
        );
    }

    /// `(A FULL JOIN B) INNER JOIN C` — multi-type chain: outer then inner.
    ///
    /// A full join feeding into an inner join. The inner join's left child
    /// produces nullable columns from the full join; the inner join must
    /// not strip the nullable flag.
    #[test]
    fn test_diff_full_join_as_left_child_of_inner_join() {
        use crate::dvm::operators::join::diff_inner_join;

        // A FULL JOIN B
        let a = scan(1, "a", "public", "a", &["id", "val"]);
        let b = scan(2, "b", "public", "b", &["id", "score"]);
        let full = OpTree::FullJoin {
            condition: eq_cond("a", "id", "b", "id"),
            left: Box::new(a),
            right: Box::new(b),
        };

        // (A FULL JOIN B) INNER JOIN C
        let c = scan(3, "c", "public", "c", &["id", "flag"]);
        let tree = inner_join(eq_cond("a", "id", "c", "id"), full, c);

        let mut ctx = test_ctx();
        let result = diff_inner_join(&mut ctx, &tree);
        assert!(
            result.is_ok(),
            "(A FULL JOIN B) INNER JOIN C should succeed: {result:?}"
        );

        let dr = result.unwrap();
        let sql = ctx.build_with_query(&dr.cte_name);
        assert_sql_contains(&sql, "UNION ALL");
    }
}
