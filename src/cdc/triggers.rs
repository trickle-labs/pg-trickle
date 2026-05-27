//! QUAL-3 (v0.60.0): Trigger SQL builders and column-name escaping logic.
//!
//! This module contains pure-logic helpers extracted from `cdc.rs` as part of
//! the QUAL-3 module decomposition.  All functions here are either pure (no SPI)
//! or are the trigger DDL builders that depend on SPI only for relation lookup.

// ── Reserved change-buffer column names ────────────────────────────────────

/// Built-in CDC metadata column names that live at the top of every change-buffer
/// table.  A source table column with any of these names would collide with the
/// metadata column in a flat (A44-10 D+I) change-buffer schema.
pub(super) const RESERVED_CB_COLS: &[&str] =
    &["change_id", "lsn", "action", "pk_hash", "changed_cols"];

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

/// Build the VARBIT bitmask expression for `changed_cols` in UPDATE triggers.
///
/// Each non-PK column gets a `CASE WHEN NEW.col IS DISTINCT FROM OLD.col THEN
/// B'1' ELSE B'0' END)::varbit` bit that is concatenated with `||`.
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
            // pgvector types (vector, halfvec, sparsevec) do not define an '='
            // operator, so IS DISTINCT FROM (which uses '=') would fail.
            // Cast to text for comparison — text always supports equality.
            let base_type = type_name.split('(').next().unwrap_or("").trim();
            let is_pgvector = matches!(base_type, "vector" | "halfvec" | "sparsevec");
            if is_pgvector {
                format!(
                    "(CASE WHEN NEW.\"{qcol}\"::text IS DISTINCT FROM OLD.\"{qcol}\"::text \
                     THEN B'1' ELSE B'0' END)::varbit"
                )
            } else {
                format!(
                    "(CASE WHEN NEW.\"{qcol}\" IS DISTINCT FROM OLD.\"{qcol}\" \
                     THEN B'1' ELSE B'0' END)::varbit"
                )
            }
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
        assert_eq!(cb_col_name("pk_hash"), "__usr_pk_hash");
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
    fn test_build_changed_cols_bitmask_expr_pgvector_casts_to_text() {
        let pk = vec!["id".to_string()];
        let cols = vec![
            ("id".to_string(), "integer".to_string()),
            ("embedding".to_string(), "vector".to_string()),
        ];
        let expr = build_changed_cols_bitmask_expr(&pk, &cols).unwrap();
        assert!(
            expr.contains("::text"),
            "pgvector col should cast to text: {expr}"
        );
    }
}
