// TEST-10-03 (v0.49.0): Public fuzz-harness wrappers for merge SQL pipeline.
//
// These thin wrappers expose `pub(crate)` pure-Rust functions so the
// cargo-fuzz targets (a separate crate) can reach them without a PostgreSQL
// backend.  All functions here are pure Rust — no SPI, no pgrx, no I/O.

/// Quote a value as a PostgreSQL string literal using dollar-quote style escaping.
///
/// Wraps [`crate::refresh::merge::columns::pg_quote_literal`].
pub fn merge_pg_quote_literal_pub(s: &str) -> String {
    crate::refresh::merge::columns::pg_quote_literal(s)
}

/// Parse a PostgreSQL HASH partition bound spec such as
/// `"FOR VALUES WITH (modulus 4, remainder 2)"`.
///
/// Returns `Ok((modulus, remainder))` or an error on malformed input.
///
/// Wraps [`crate::refresh::merge::columns::parse_hash_bound_spec`].
pub fn merge_parse_hash_bound_spec_pub(
    s: &str,
) -> Result<(i32, i32), crate::error::PgTrickleError> {
    crate::refresh::merge::columns::parse_hash_bound_spec(s)
}

/// Extract an integer value following a keyword in a partition bound spec.
///
/// Wraps [`crate::refresh::merge::columns::extract_keyword_int`].
pub fn merge_extract_keyword_int_pub(
    s: &str,
    keyword: &str,
) -> Result<i32, crate::error::PgTrickleError> {
    crate::refresh::merge::columns::extract_keyword_int(s, keyword)
}

/// Compute the amplification ratio (output rows / input rows) for delta sizing.
///
/// Returns 0.0 when `input_delta <= 0`.
///
/// Wraps [`crate::refresh::merge::delete::compute_amplification_ratio`].
pub fn merge_compute_amplification_ratio_pub(input_delta: i64, output_delta: i64) -> f64 {
    crate::refresh::merge::delete::compute_amplification_ratio(input_delta, output_delta)
}

/// Determine whether the amplification ratio exceeds `threshold` and a
/// WARNING should be emitted.
///
/// Wraps [`crate::refresh::merge::delete::should_warn_amplification`].
pub fn merge_should_warn_amplification_pub(
    input_delta: i64,
    output_delta: i64,
    threshold: f64,
) -> bool {
    crate::refresh::merge::delete::should_warn_amplification(input_delta, output_delta, threshold)
}

/// Build a `__pgt_row_id` hash expression for a list of user columns.
///
/// Returns a SQL expression string.  Does not touch the database.
///
/// Wraps [`crate::refresh::codegen::build_content_hash_expr`].
pub fn merge_build_content_hash_expr_pub(prefix: &str, user_cols: &[String]) -> String {
    crate::refresh::codegen::build_content_hash_expr(prefix, user_cols)
}

// ── P-5 (v0.80.0): DVM classifier fuzz wrappers ──────────────────────────────
//
// These thin wrappers expose the DVM query-pattern classifiers so that
// `snapshot_fingerprint_fuzz` can exercise them against arbitrary SQL strings
// without requiring a PostgreSQL backend.

/// Detect CASE aggregate with IN-list WHERE predicate (q12-like).
///
/// Wraps [`crate::refresh::classify_case_in_list_aggregate_drift`].
pub fn refresh_classify_case_in_list_pub(query: &str) -> bool {
    crate::refresh::classify_case_in_list_aggregate_drift(query)
}

/// Detect correlated aggregate scalar subquery in WHERE (q20-like).
///
/// Wraps [`crate::refresh::classify_correlated_aggregate_subquery_in_where`].
pub fn refresh_classify_correlated_subquery_pub(query: &str) -> bool {
    crate::refresh::classify_correlated_aggregate_subquery_in_where(query)
}

/// Detect CASE aggregate with EXISTS/subquery inside predicate (regex classifier uncertain).
///
/// Wraps [`crate::refresh::classify_case_aggregate_subquery_uncertain`].
pub fn refresh_classify_case_aggregate_subquery_uncertain_pub(query: &str) -> bool {
    crate::refresh::classify_case_aggregate_subquery_uncertain(query)
}
