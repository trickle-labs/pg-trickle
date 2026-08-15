//! QUAL-2 (v0.60.0): Pure SQL-fragment helpers extracted from `codegen.rs`.
//!
//! Each function in this module is a pure function of its inputs — no SPI,
//! no global state — enabling deterministic unit tests without a PostgreSQL
//! backend.

/// Action type used in [`build_delta_target_list`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeAction {
    /// WHEN MATCHED — overwrite existing row with incoming values.
    Matched,
    /// WHEN NOT MATCHED BY TARGET — insert new row.
    NotMatchedByTarget,
    /// WHEN NOT MATCHED BY SOURCE — delete row that is no longer in the source.
    NotMatchedBySource,
}

/// Column specification used by [`build_delta_target_list`].
#[derive(Debug, Clone)]
pub(crate) struct ColumnSpec {
    /// Unquoted column name.
    pub name: String,
    /// True when the column is nullable (affects NULL handling in the target list).
    pub nullable: bool,
}

/// Build the WHERE predicate that restricts a CTE scan to the delta window.
///
/// Produces a fragment like:
/// ```sql
/// lsn > prev_lsn::pg_lsn AND lsn <= new_lsn::pg_lsn
/// ```
///
/// `lsn_col`, `prev_lsn`, and `new_lsn` are caller-controlled strings that
/// are interpolated directly into the returned SQL.  The caller is responsible
/// for quoting/escaping if needed.
pub(crate) fn build_cte_predicate(lsn_col: &str, prev_lsn: &str, new_lsn: &str) -> String {
    format!("{lsn_col} > {prev_lsn}::pg_lsn AND {lsn_col} <= {new_lsn}::pg_lsn")
}

/// Build the ON clause for a MERGE statement.
///
/// Produces a fragment like:
/// ```sql
/// target.col1 = source.col1 AND target.col2 = source.col2
/// ```
///
/// Empty `key_cols` returns `"TRUE"` (matches everything — useful for
/// keyless delta tables where we rely on a row-identity surrogate).
pub(crate) fn build_merge_join_condition(key_cols: &[&str]) -> String {
    if key_cols.is_empty() {
        return "TRUE".to_string();
    }
    key_cols
        .iter()
        .map(|c| {
            let escaped = c.replace('"', "\"\"");
            format!("target.\"{escaped}\" = source.\"{escaped}\"")
        })
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Build the MD5 content-hash column expression for deduplication.
///
/// Produces an expression that hashes all `data_cols` using
/// `pgtrickle.pg_trickle_hash_multi` (or `pg_trickle_hash` for a single
/// column), using the given `prefix` (e.g. `"source."` or `"d."`).
///
/// When `data_cols` is empty, falls back to `__pgt_row_id` for keyless tables.
pub(crate) fn build_content_hash_column(prefix: &str, data_cols: &[&str]) -> String {
    match data_cols.len() {
        0 => format!("{prefix}__pgt_row_id"),
        1 => {
            let escaped = data_cols[0].replace('"', "\"\"");
            format!("pgtrickle.pg_trickle_hash({prefix}\"{escaped}\"::TEXT)")
        }
        _ => {
            let args: Vec<String> = data_cols
                .iter()
                .map(|c| {
                    let escaped = c.replace('"', "\"\"");
                    format!("{prefix}\"{escaped}\"::TEXT")
                })
                .collect();
            crate::hash::build_composite_hash_expr(&args)
        }
    }
}

/// Build the MERGE WHEN MATCHED / NOT MATCHED target lists.
///
/// Returns the assignment or column list appropriate for `action`:
///
/// - [`MergeAction::Matched`]: assignment list for UPDATE
/// - [`MergeAction::NotMatchedByTarget`]: column+value list for INSERT
/// - [`MergeAction::NotMatchedBySource`][]: `DELETE` (no column list needed; returns empty string)
///
/// An empty `cols` slice returns an empty string for all actions.
pub(crate) fn build_delta_target_list(action: MergeAction, cols: &[ColumnSpec]) -> String {
    if cols.is_empty() {
        return String::new();
    }
    match action {
        MergeAction::Matched => cols
            .iter()
            .map(|c| {
                let escaped = c.name.replace('"', "\"\"");
                format!("\"{escaped}\" = source.\"{escaped}\"")
            })
            .collect::<Vec<_>>()
            .join(", "),
        MergeAction::NotMatchedByTarget => {
            let col_names: Vec<String> = cols
                .iter()
                .map(|c| {
                    let escaped = c.name.replace('"', "\"\"");
                    format!("\"{escaped}\"")
                })
                .collect();
            let values: Vec<String> = cols
                .iter()
                .map(|c| {
                    let escaped = c.name.replace('"', "\"\"");
                    format!("source.\"{escaped}\"")
                })
                .collect();
            format!("({}) VALUES ({})", col_names.join(", "), values.join(", "))
        }
        MergeAction::NotMatchedBySource => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── build_cte_predicate ───────────────────────────────────────────────

    #[test]
    fn test_build_cte_predicate_standard() {
        let result = build_cte_predicate("lsn", "'0/0'", "'0/1000'");
        assert_eq!(result, "lsn > '0/0'::pg_lsn AND lsn <= '0/1000'::pg_lsn");
    }

    #[test]
    fn test_build_cte_predicate_quoted_col() {
        let result = build_cte_predicate("c.\"my lsn\"", "prev", "next");
        assert_eq!(
            result,
            "c.\"my lsn\" > prev::pg_lsn AND c.\"my lsn\" <= next::pg_lsn"
        );
    }

    // ── build_merge_join_condition ────────────────────────────────────────

    #[test]
    fn test_build_merge_join_condition_empty() {
        assert_eq!(build_merge_join_condition(&[]), "TRUE");
    }

    #[test]
    fn test_build_merge_join_condition_single_key() {
        let result = build_merge_join_condition(&["id"]);
        assert_eq!(result, "target.\"id\" = source.\"id\"");
    }

    #[test]
    fn test_build_merge_join_condition_composite_key() {
        let result = build_merge_join_condition(&["tenant_id", "order_id"]);
        assert_eq!(
            result,
            "target.\"tenant_id\" = source.\"tenant_id\" AND target.\"order_id\" = source.\"order_id\""
        );
    }

    #[test]
    fn test_build_merge_join_condition_reserved_char_in_name() {
        // Column names with embedded quotes must be double-quoted inside the identifier.
        let result = build_merge_join_condition(&["col\"with\"quotes"]);
        assert_eq!(
            result,
            "target.\"col\"\"with\"\"quotes\" = source.\"col\"\"with\"\"quotes\""
        );
    }

    // ── build_content_hash_column ─────────────────────────────────────────

    #[test]
    fn test_build_content_hash_column_empty() {
        let result = build_content_hash_column("d.", &[]);
        assert_eq!(result, "d.__pgt_row_id");
    }

    #[test]
    fn test_build_content_hash_column_single_col() {
        let result = build_content_hash_column("d.", &["amount"]);
        assert_eq!(result, "pgtrickle.pg_trickle_hash(d.\"amount\"::TEXT)");
    }

    #[test]
    fn test_build_content_hash_column_multi_col() {
        let result = build_content_hash_column("source.", &["name", "value"]);
        assert_eq!(
            result,
            "pgtrickle.pg_trickle_hash_multi(ARRAY[source.\"name\"::TEXT, source.\"value\"::TEXT])"
        );
    }

    #[test]
    fn test_build_content_hash_column_different_prefix() {
        let result = build_content_hash_column("ins.", &["price"]);
        assert_eq!(result, "pgtrickle.pg_trickle_hash(ins.\"price\"::TEXT)");
    }

    // ── build_delta_target_list ───────────────────────────────────────────

    fn make_cols(names: &[&str]) -> Vec<ColumnSpec> {
        names
            .iter()
            .map(|&n| ColumnSpec {
                name: n.to_string(),
                nullable: false,
            })
            .collect()
    }

    #[test]
    fn test_build_delta_target_list_empty_when_matched() {
        let result = build_delta_target_list(MergeAction::Matched, &[]);
        assert_eq!(result, "");
    }

    #[test]
    fn test_build_delta_target_list_when_matched_single_col() {
        let cols = make_cols(&["amount"]);
        let result = build_delta_target_list(MergeAction::Matched, &cols);
        assert_eq!(result, "\"amount\" = source.\"amount\"");
    }

    #[test]
    fn test_build_delta_target_list_when_matched_multi_col() {
        let cols = make_cols(&["name", "value"]);
        let result = build_delta_target_list(MergeAction::Matched, &cols);
        assert_eq!(
            result,
            "\"name\" = source.\"name\", \"value\" = source.\"value\""
        );
    }

    #[test]
    fn test_build_delta_target_list_when_not_matched_by_target() {
        let cols = make_cols(&["id", "name"]);
        let result = build_delta_target_list(MergeAction::NotMatchedByTarget, &cols);
        assert_eq!(
            result,
            "(\"id\", \"name\") VALUES (source.\"id\", source.\"name\")"
        );
    }

    #[test]
    fn test_build_delta_target_list_when_not_matched_by_source() {
        let cols = make_cols(&["id"]);
        let result = build_delta_target_list(MergeAction::NotMatchedBySource, &cols);
        // DELETE action needs no column list.
        assert_eq!(result, "");
    }

    #[test]
    fn test_build_delta_target_list_reserved_quote_in_col_name() {
        let cols = make_cols(&["col\"name"]);
        let result = build_delta_target_list(MergeAction::Matched, &cols);
        assert_eq!(result, "\"col\"\"name\" = source.\"col\"\"name\"");
    }
}
