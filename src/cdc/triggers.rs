//! QUAL-3 (v0.60.0): Trigger SQL builders and column-name escaping logic.
//!
//! This module contains pure-logic helpers extracted from `cdc.rs` as part of
//! the QUAL-3 module decomposition.  All functions here are either pure (no SPI)
//! or are the trigger DDL builders that depend on SPI only for relation lookup.

// ── Reserved change-buffer column names ────────────────────────────────────

/// Built-in CDC metadata column names that live at the top of every change-buffer
/// table.  A source table column with any of these names would collide with the
/// metadata column in a flat (A44-10 D+I) change-buffer schema.
pub(super) const RESERVED_CB_COLS: &[&str] = &[
    "change_id",
    "lsn",
    "action",
    "__pgt_row_id",
    "changed_cols",
    "source_xid",
    "source_commit_at",
    "__pgt_trace_context",
];

/// Map a *source* column name to its change-buffer storage name.
///
/// When a source column name matches one of [`RESERVED_CB_COLS`], the column is
/// stored in the change buffer as `__usr_{name}` to prevent a
/// `column "…" specified more than once` error in PostgreSQL.
/// All other names pass through unchanged.
pub fn cb_col_name(name: &str) -> String {
    if RESERVED_CB_COLS.contains(&name) {
        format!("__usr_{name}")
    } else {
        name.to_string()
    }
}

pub(super) fn is_pgvector_type(type_name: &str) -> bool {
    matches!(
        type_name
            .split('(')
            .next()
            .unwrap_or("")
            .trim()
            .rsplit('.')
            .next()
            .unwrap_or("")
            .trim_matches('"'),
        "vector" | "halfvec" | "sparsevec"
    )
}

pub(super) fn is_numeric_type(type_name: &str) -> bool {
    matches!(
        type_name
            .split('(')
            .next()
            .unwrap_or("")
            .trim()
            .rsplit('.')
            .next()
            .unwrap_or("")
            .trim_matches('"'),
        "numeric" | "decimal"
    )
}

pub(super) fn changed_col_bit_expr(new_value: &str, old_value: &str, type_name: &str) -> String {
    if is_pgvector_type(type_name) {
        return format!(
            "(CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_type t \
             JOIN pg_catalog.pg_depend d ON d.classid = 'pg_catalog.pg_type'::regclass::oid AND d.objid = t.oid \
             JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid \
             WHERE t.oid = pg_catalog.pg_typeof({new_value})::oid \
               AND d.refclassid = 'pg_catalog.pg_extension'::regclass::oid AND d.deptype = 'e' \
               AND e.extname = 'vector' AND t.typname IN ('vector', 'halfvec', 'sparsevec')) \
             THEN CASE WHEN {new_value}::text IS DISTINCT FROM {old_value}::text \
                       THEN B'1' ELSE B'0' END ELSE B'1' END)::varbit"
        );
    }
    let comparison = if is_numeric_type(type_name) {
        format!(
            "{new_value} IS DISTINCT FROM {old_value} OR \
             scale({new_value}) IS DISTINCT FROM scale({old_value})"
        )
    } else {
        return crate::cdc_elision::changed_column_sql(old_value, new_value);
    };
    format!("(CASE WHEN {comparison} THEN B'1' ELSE B'0' END)::varbit")
}

/// Build the VARBIT bitmask expression for `changed_cols` in UPDATE triggers.
/// Numeric scale changes and admitted binary images are observable by SQL.
///
/// Returns `None` for keyless tables (`pk_columns` empty): all columns
/// contribute to the content hash and must always be present.
///
/// For INSERT and DELETE rows `changed_cols` is stored as `NULL`, indicating
/// that all new_*/old_* column values are populated (backward-compatible).
pub fn build_changed_cols_bitmask_expr(
    pk_columns: &[String],
    columns: &[(String, String)],
) -> Option<String> {
    if pk_columns.is_empty() {
        return None; // keyless: must always write all columns
    }
    let parts: Vec<String> = columns
        .iter()
        .map(|(col_name, type_name)| {
            let qcol = col_name.replace('"', "\"\"");
            changed_col_bit_expr(
                &format!("NEW.\"{qcol}\""),
                &format!("OLD.\"{qcol}\""),
                type_name,
            )
        })
        .collect();
    Some(parts.join(" ||\n        "))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── cb_col_name tests ─────────────────────────────────────────────────

    #[test]
    fn test_cb_col_name_escapes_reserved_pgt_prefix() {
        // All RESERVED_CB_COLS must be prefixed with __usr_
        assert_eq!(cb_col_name("action"), "__usr_action");
        assert_eq!(cb_col_name("lsn"), "__usr_lsn");
        assert_eq!(cb_col_name("__pgt_row_id"), "__usr___pgt_row_id");
        assert_eq!(cb_col_name("changed_cols"), "__usr_changed_cols");
        assert_eq!(cb_col_name("change_id"), "__usr_change_id");
    }

    #[test]
    fn test_cb_col_name_passes_through_normal_name() {
        assert_eq!(cb_col_name("id"), "id");
        assert_eq!(cb_col_name("order_id"), "order_id");
        assert_eq!(cb_col_name("status"), "status");
        assert_eq!(cb_col_name("amount"), "amount");
    }

    // ── build_changed_cols_bitmask_expr tests ────────────────────────────

    #[test]
    fn test_build_changed_cols_bitmask_expr_single_col() {
        let pk = vec!["id".to_string()];
        let cols = vec![
            ("id".to_string(), "integer".to_string()),
            ("val".to_string(), "text".to_string()),
        ];
        let expr = build_changed_cols_bitmask_expr(&pk, &cols).unwrap();
        assert!(
            expr.contains("IS DISTINCT FROM"),
            "should use IS DISTINCT FROM: {expr}"
        );
        assert!(expr.contains("::varbit"), "should use VARBIT type: {expr}");
    }

    #[test]
    fn test_build_changed_cols_bitmask_expr_empty() {
        // Keyless table (empty pk) → None
        let cols = vec![("a".to_string(), "int".to_string())];
        assert!(build_changed_cols_bitmask_expr(&[], &cols).is_none());
    }

    #[test]
    fn test_build_changed_cols_bitmask_expr_unknown_types_retain_changes() {
        let pk = vec!["id".to_string()];
        let cols = vec![
            ("id".to_string(), "integer".to_string()),
            ("embedding".to_string(), "vector".to_string()),
        ];
        let expr = build_changed_cols_bitmask_expr(&pk, &cols).unwrap();
        assert!(
            expr.contains("ELSE B'1' END)::varbit"),
            "unknown pgvector types must retain changes: {expr}"
        );

        let qualified_expr = build_changed_cols_bitmask_expr(
            &pk,
            &[
                ("id".to_string(), "integer".to_string()),
                ("embedding".to_string(), "public.vector(3)".to_string()),
            ],
        )
        .unwrap();
        assert!(
            qualified_expr.contains("pg_catalog.pg_typeof(NEW.\"embedding\")::oid"),
            "actual type OIDs must govern the policy: {qualified_expr}"
        );
    }

    #[test]
    fn test_build_changed_cols_bitmask_expr_numeric_compares_scale_representation() {
        let pk = vec!["id".to_string()];
        let cols = vec![
            ("id".to_string(), "integer".to_string()),
            (
                "amount".to_string(),
                "pg_catalog.numeric(12, 2)".to_string(),
            ),
        ];
        let expr = build_changed_cols_bitmask_expr(&pk, &cols).unwrap();
        assert!(
            expr.contains("NEW.\"amount\" IS DISTINCT FROM OLD.\"amount\" OR")
                && expr.contains("scale(NEW.\"amount\") IS DISTINCT FROM scale(OLD.\"amount\")"),
            "numeric changed_cols must observe scale changes: {expr}"
        );
    }
}
