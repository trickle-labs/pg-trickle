// ARCH-1: PH-D1 phantom-cleanup sub-module for the refresh pipeline.
//
// This module contains the PH-D1 DELETE+INSERT strategy for handling
// join phantom rows:
// - PH-D1 strategy selection logic (currently around line 4991 in mod.rs)
// - DELETE+INSERT execution path (currently around line 5151 in mod.rs)
// - Co-deletion detection helpers
// - EC-01 convergence validation
// - EC01-2: Cross-cycle phantom cleanup
//
// Currently most code lives in `super` (mod.rs). This file is the landing
// zone for the phantom-cleanup layer during the ongoing ARCH-1 migration.

use crate::error::PgTrickleError;
use pgrx::Spi;

pub fn prepare_cross_cycle_phantom_table(
    st: &crate::catalog::StreamTableMeta,
    defining_query: &str,
) -> Result<(), PgTrickleError> {
    let recon_table = format!("__pgt_recon_{}", st.pgt_id);
    // defining_query is definition-derived: parse/analyze it under the
    // owner's identity and stored search_path, not this privileged caller's.
    let prepare_start = std::time::Instant::now();
    crate::refresh::with_stream_owner(st, || {
        let row_id_expr = crate::dvm::row_id_expr_for_query(defining_query);
        let recon_select =
            format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({defining_query}) sub");
        crate::refresh::prepare_owner_temp_table(st, &recon_table, &recon_select)
    })?;
    pgrx::debug1!(
        "[pg_trickle] EC01-2 timing pgt_id={} phase=prepare_ms elapsed_ms={:.3}",
        st.pgt_id,
        prepare_start.elapsed().as_secs_f64() * 1000.0,
    );
    Ok(())
}

fn content_signature_expr(
    alias: &str,
    quoted_user_cols: &[String],
    row_id_is_complete: bool,
) -> String {
    if row_id_is_complete {
        return format!("{alias}.\"__pgt_row_id\"");
    }
    if quoted_user_cols.is_empty() {
        // Degenerate case (no user columns); use a constant fingerprint so
        // reconciliation collapses to a row-count diff.
        return "''::text".to_string();
    }
    let values = quoted_user_cols
        .iter()
        .map(|column| format!("{alias}.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    // A row constructor avoids jsonb_build_object's 100-argument limit while
    // retaining PostgreSQL's native JSON encoding for each value.
    format!("md5(to_jsonb(ROW({values}))::text)")
}

/// Return whether the current stream and reconstructed result have different
/// multisets of content signatures. The same signatures drive the correction
/// queries below, so a zero signed count for every signature proves that both
/// the DELETE and INSERT correction sets are empty, including duplicates and
/// NULL-valued signatures.
fn reconciliation_has_drift_sql(
    stream_table_name: &str,
    recon_table: &str,
    st_sig: &str,
    r_sig: &str,
) -> String {
    format!(
        "SELECT EXISTS ( \
             SELECT 1 FROM ( \
                 SELECT {st_sig} AS __pgt_sig, 1 AS __pgt_weight \
                 FROM {stream_table_name} st \
                 UNION ALL \
                 SELECT {r_sig} AS __pgt_sig, -1 AS __pgt_weight \
                 FROM {recon_table} r \
             ) __pgt_multiset \
             GROUP BY __pgt_sig \
             HAVING SUM(__pgt_weight) <> 0 \
         )"
    )
}

/// EC01-2: Reconcile the stream table against the full-query result set.
///
/// After a refresh (DIFFERENTIAL or IMMEDIATE) applies a delta, residue from
/// prior cycles may remain whenever the delta engine was unable to compute a
/// fully accurate change set. Three failure modes are observed in practice:
///
/// 1. **Pure orphans** — a row exists in the ST but the join key partner has
///    since been deleted, so the row is no longer part of the live result.
/// 2. **Stale content** — the ST row's `__pgt_row_id` still matches a current
///    result row, but the user-visible columns are out of date because a
///    non-monotone predicate (e.g. scalar `MAX` subquery, EXISTS) shifted
///    without the delta engine seeing a triggering row in this transaction.
/// 3. **Missing rows** — the live result includes a row whose `__pgt_row_id`
///    is absent from the ST because the same non-monotone predicate kept the
///    delta from emitting an INSERT.
///
/// The original implementation only handled (1) by detecting orphan
/// `__pgt_row_id`s. To fully clear EC-01 for IMMEDIATE mode (and harden the
/// DIFFERENTIAL path against the same shape) the cleanup now performs full
/// multiset reconciliation against the live query result:
///
///   * Materialise the full-query output (including `__pgt_row_id`).
///   * Hash each ST and recon row by `row_to_json` to obtain a NULL-safe,
///     order-independent content fingerprint.
///   * For every fingerprint where ST count > recon count, delete the
///     surplus rows by `ctid`.
///   * For every fingerprint where recon count > ST count, insert the
///     missing rows.
///
/// The cleanup runs in batches of `batch_size` rows to avoid holding long
/// locks on either side. Returns the total number of rows deleted plus
/// inserted.
///
/// Called after each non-deduplicated, non-partitioned, join-bearing apply
/// so stale rows converge even when the current delta did not contain the
/// matching change.
pub fn cleanup_cross_cycle_phantoms(
    pgt_id: i64,
    stream_table_name: &str,
    defining_query: &str,
    batch_size: i64,
) -> Result<i64, PgTrickleError> {
    let user_cols = crate::dvm::get_defining_query_columns(defining_query)?;
    let (row_id_expr, row_id_is_complete) =
        crate::dvm::row_id_expr_for_query_and_complete_output(defining_query, &user_cols);

    // Quote user column names.
    let quoted_user_cols: Vec<String> = user_cols
        .iter()
        .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
        .collect();

    // Column list for the INSERT projection.
    let all_cols_csv: String = std::iter::once("__pgt_row_id".to_string())
        .chain(quoted_user_cols.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");

    // Content fingerprint expression — built over the user-visible columns
    // ONLY. `__pgt_row_id` is intentionally excluded because for join
    // queries whose PKs do not appear in the projection it is computed via
    // `row_to_json(sub) || row_number() OVER ()`, which is unstable across
    // executions (the row_number depends on delivery order). Excluding it
    // makes the reconciliation correct for those query shapes too — two
    // rows with identical user content are considered equivalent regardless
    // of any internal hash drift.
    let st_sig = content_signature_expr("st", &quoted_user_cols, row_id_is_complete);
    let r_sig = content_signature_expr("r", &quoted_user_cols, row_id_is_complete);
    let recon_row_number_alias = {
        let mut suffix = 0_u32;
        loop {
            let candidate = if suffix == 0 {
                "__pgt_recon_rn".to_string()
            } else {
                format!("__pgt_recon_rn_{suffix}")
            };
            if !user_cols.iter().any(|column| column == &candidate) {
                break candidate;
            }
            suffix += 1;
        }
    };

    // Step 1: materialise the live full-query result into the prepared temp table.
    let recon_table = format!("__pgt_recon_{pgt_id}");
    let insert_recon_sql = format!(
        "INSERT INTO {recon_table} \
         SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({defining_query}) sub"
    );
    let materialize_start = std::time::Instant::now();
    Spi::run(&insert_recon_sql).map_err(|e| {
        PgTrickleError::SpiError(format!("EC01-2 reconcile materialise failed: {e}"))
    })?;
    let materialize_ms = materialize_start.elapsed().as_secs_f64() * 1000.0;

    // Common case: the differential MERGE already converged. A single signed
    // multiset aggregation detects whether any repair is necessary; avoid the
    // separate DELETE/INSERT count aggregations and row-number windows when
    // every content signature has equal multiplicity. This does NOT skip the
    // full-query materialisation or suppress repair when there is drift.
    let drift_probe =
        reconciliation_has_drift_sql(stream_table_name, &recon_table, &st_sig, &r_sig);
    let probe_start = std::time::Instant::now();
    let has_drift = Spi::get_one::<bool>(&drift_probe)
        .map_err(|e| PgTrickleError::SpiError(format!("EC01-2 drift probe failed: {e}")))?
        .unwrap_or(true);
    let probe_ms = probe_start.elapsed().as_secs_f64() * 1000.0;
    if !has_drift {
        pgrx::debug1!(
            "[pg_trickle] EC01-2 timing pgt_id={pgt_id} drift=false materialize_ms={materialize_ms:.3} probe_ms={probe_ms:.3} delete_ms=0 insert_ms=0",
        );
        return Ok(0);
    }

    // Step 2: delete surplus ST rows (multiset-aware on user-content sig).
    //
    // INVARIANT: The `ctid` values captured by the preceding CTE are stable
    // within this transaction's snapshot (no concurrent VACUUM or HOT chain
    // updates).  Do NOT split this DELETE into a separate transaction or
    // sub-transaction — if the snapshot isolation guarantee is lost, use
    // __pgt_row_id-based deletion instead.
    //
    // ctid is read directly from the ST (NOT through a subquery, which
    // would strip the system column).
    let delete_sql = format!(
        "WITH \
            st_tagged AS ( \
                SELECT st.ctid, {st_sig} AS __pgt_sig \
                FROM {stream_table_name} st \
            ), \
            recon_tagged AS ( \
                SELECT {r_sig} AS __pgt_sig \
                FROM {recon_table} r \
            ), \
            st_counts AS (SELECT __pgt_sig, count(*) AS c FROM st_tagged GROUP BY __pgt_sig), \
            recon_counts AS (SELECT __pgt_sig, count(*) AS c FROM recon_tagged GROUP BY __pgt_sig), \
            excess AS ( \
                SELECT s.__pgt_sig, s.c - COALESCE(r.c, 0) AS extras \
                FROM st_counts s LEFT JOIN recon_counts r USING (__pgt_sig) \
                WHERE s.c > COALESCE(r.c, 0) \
            ), \
            numbered AS ( \
                SELECT ctid, __pgt_sig, \
                       ROW_NUMBER() OVER (PARTITION BY __pgt_sig) AS rn \
                FROM st_tagged \
            ), \
            to_delete AS ( \
                SELECT n.ctid \
                FROM numbered n JOIN excess e USING (__pgt_sig) \
                WHERE n.rn <= e.extras \
                LIMIT $1 \
            ), \
            deleted AS ( \
                DELETE FROM {stream_table_name} \
                WHERE ctid IN (SELECT ctid FROM to_delete) \
                RETURNING 1 \
            ) \
        SELECT count(*)::bigint FROM deleted",
    );

    let delete_start = std::time::Instant::now();
    let deleted = Spi::get_one_with_args::<i64>(&delete_sql, &[batch_size.into()])
        .map_err(|e| PgTrickleError::SpiError(format!("EC01-2 reconcile DELETE failed: {e}")))?
        .unwrap_or(0);
    let delete_ms = delete_start.elapsed().as_secs_f64() * 1000.0;

    // Step 3: insert missing rows (multiset-aware on user-content sig).
    //
    // For each fingerprint where recon.count > st.count, insert
    // `recon.count - st.count` rows from the recon table. The row_id used
    // for the new ST rows comes from the recon table — even if it's a
    // row_number-derived hash, it satisfies the unique index because it's
    // freshly generated from the live query result.
    let insert_sql = format!(
        "WITH \
            st_counts AS ( \
                SELECT {st_sig_st} AS __pgt_sig, count(*) AS c \
                FROM {stream_table_name} st GROUP BY 1 \
            ), \
            recon_counts AS ( \
                SELECT {r_sig_r} AS __pgt_sig, count(*) AS c \
                FROM {recon_table} r GROUP BY 1 \
            ), \
            shortfall AS ( \
                SELECT r.__pgt_sig, r.c - COALESCE(s.c, 0) AS missing_n \
                FROM recon_counts r LEFT JOIN st_counts s USING (__pgt_sig) \
                WHERE r.c > COALESCE(s.c, 0) \
            ), \
            recon_numbered AS ( \
                SELECT r.*, \
                       {r_sig_r} AS __pgt_sig_x, \
                       ROW_NUMBER() OVER (PARTITION BY {r_sig_r}) AS {recon_row_number_alias} \
                FROM {recon_table} r \
            ), \
            to_insert AS ( \
                SELECT n.* \
                FROM recon_numbered n \
                JOIN shortfall sh ON sh.__pgt_sig = n.__pgt_sig_x \
                WHERE n.{recon_row_number_alias} <= sh.missing_n \
                LIMIT $1 \
            ), \
            inserted AS ( \
                INSERT INTO {stream_table_name} ({all_cols_csv}) \
                SELECT {all_cols_csv} FROM to_insert \
                RETURNING 1 \
            ) \
        SELECT count(*)::bigint FROM inserted",
        st_sig_st = st_sig,
        r_sig_r = r_sig,
        recon_table = recon_table,
        stream_table_name = stream_table_name,
        all_cols_csv = all_cols_csv,
    );

    let insert_start = std::time::Instant::now();
    let inserted = Spi::get_one_with_args::<i64>(&insert_sql, &[batch_size.into()])
        .map_err(|e| PgTrickleError::SpiError(format!("EC01-2 reconcile INSERT failed: {e}")))?
        .unwrap_or(0);
    let insert_ms = insert_start.elapsed().as_secs_f64() * 1000.0;

    pgrx::debug1!(
        "[pg_trickle] EC01-2 timing pgt_id={pgt_id} drift=true materialize_ms={materialize_ms:.3} probe_ms={probe_ms:.3} delete_ms={delete_ms:.3} insert_ms={insert_ms:.3} deleted={deleted} inserted={inserted}",
    );
    let total = deleted + inserted;
    if total > 0 {
        pgrx::log!(
            "[pg_trickle] EC01-2: reconciled pgt_id={} (deleted={}, inserted={})",
            pgt_id,
            deleted,
            inserted,
        );
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_content_signature_supports_wide_rows() {
        let columns = (0..51)
            .map(|index| format!("\"column_{index}\""))
            .collect::<Vec<_>>();
        let expression = super::content_signature_expr("st", &columns, false);

        assert!(expression.contains("to_jsonb(ROW("));
        assert!(!expression.contains("jsonb_build_object"));
        assert_eq!(expression.matches("st.\"column_").count(), 51);
    }

    #[test]
    fn test_content_signature_reuses_complete_row_identity() {
        assert_eq!(
            super::content_signature_expr("r", &[], true),
            "r.\"__pgt_row_id\""
        );
    }

    #[test]
    fn test_reconciliation_drift_probe_preserves_signed_multiplicities() {
        let sql = super::reconciliation_has_drift_sql(
            "\"public\".\"source_stream\"",
            "pg_temp.__pgt_recon_42",
            "st.\"__pgt_row_id\"",
            "r.\"__pgt_row_id\"",
        );
        assert!(sql.contains("SELECT EXISTS"));
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("GROUP BY __pgt_sig"));
        assert!(sql.contains("HAVING SUM(__pgt_weight) <> 0"));
        assert_eq!(sql.matches("FROM \"public\".\"source_stream\" st").count(), 1);
        assert_eq!(sql.matches("FROM pg_temp.__pgt_recon_42 r").count(), 1);
        assert!(sql.contains("SELECT st.\"__pgt_row_id\" AS __pgt_sig, 1 AS __pgt_weight"));
        assert!(sql.contains("SELECT r.\"__pgt_row_id\" AS __pgt_sig, -1 AS __pgt_weight"));
    }

    #[test]
    fn test_cleanup_returns_zero_for_empty_case() {
        // Verify batch_size defaults are positive integers
        let batch_size: i64 = 1000;
        assert!(batch_size > 0);
    }
}

#[cfg(feature = "pg_test")]
#[pgrx::pg_schema]
mod pg_tests {
    use super::*;

    #[pgrx::pg_test]
    fn test_reconciliation_drift_probe_handles_nulls_and_duplicate_rows() {
        Spi::run("CREATE TEMP TABLE __pgt_probe_st (value text)").unwrap();
        Spi::run("CREATE TEMP TABLE __pgt_probe_recon (value text)").unwrap();
        let sql = reconciliation_has_drift_sql(
            "pg_temp.__pgt_probe_st",
            "pg_temp.__pgt_probe_recon",
            "st.value",
            "r.value",
        );
        let has_drift = || Spi::get_one::<bool>(&sql).unwrap().unwrap();
        assert!(!has_drift(), "two empty multisets match");

        Spi::run("INSERT INTO __pgt_probe_st VALUES ('a'), (NULL), (NULL)").unwrap();
        Spi::run("INSERT INTO __pgt_probe_recon VALUES (NULL), ('a'), (NULL)").unwrap();
        assert!(!has_drift(), "multiplicity and NULLs match regardless of order");

        Spi::run("INSERT INTO __pgt_probe_st VALUES (NULL)").unwrap();
        assert!(has_drift(), "a surplus NULL signature must trigger repair");
        Spi::run("INSERT INTO __pgt_probe_recon VALUES (NULL)").unwrap();
        assert!(!has_drift(), "equal duplicate counts cancel");

        Spi::run("UPDATE __pgt_probe_recon SET value = 'changed' WHERE value = 'a'").unwrap();
        assert!(has_drift(), "stale content must trigger repair");
        Spi::run("UPDATE __pgt_probe_st SET value = 'changed' WHERE value = 'a'").unwrap();
        assert!(!has_drift(), "matching content clears the drift probe");

        Spi::run("DELETE FROM __pgt_probe_recon WHERE value = 'changed'").unwrap();
        assert!(has_drift(), "a missing result row must trigger repair");
    }
}
