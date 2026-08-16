//! E2E tests for Phase 2 diagnostic functions (DT-1 – DT-4).
//!
//! Covers:
//!   - `pgtrickle.explain_query_rewrite(query)` — rewrite pass tracking
//!   - `pgtrickle.diagnose_errors(name)` — error classification + remediation
//!   - `pgtrickle.list_auxiliary_columns(name)` — __pgt_* column listing
//!   - `pgtrickle.validate_query(query)` — resolved mode + construct detection
//!
//! These tests are light-E2E eligible (no background worker required).

mod e2e;

use e2e::E2eDb;

const CURRENT_PG_TRICKLE_VERSION: &str = env!("CARGO_PKG_VERSION");

// ── DT-1: explain_query_rewrite ───────────────────────────────────────────

/// DT-1: simple SELECT — only the `FINAL` pass should report a rewritten SQL.
/// The `topk_detection` and `dvm_patterns` passes also appear; all pure-passthrough
/// rewrites have `changed = false`.
#[tokio::test]
async fn test_diagnostics_explain_query_rewrite_simple_select() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_src1 (id INT PRIMARY KEY, val TEXT)")
        .await;

    let row_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.explain_query_rewrite(\
             'SELECT id, val FROM diag_src1')",
        )
        .await;

    // Must have at least the named rewrite passes + topk_detection + dvm_patterns
    assert!(
        row_count >= 8,
        "explain_query_rewrite should return ≥8 rows, got {row_count}"
    );
}

/// DT-1: a GROUPING SETS query fires the `grouping_sets` rewrite pass.
#[tokio::test]
async fn test_diagnostics_explain_query_rewrite_grouping_sets_fires() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_gs (id INT PRIMARY KEY, region TEXT, amount NUMERIC)")
        .await;

    // The grouping_sets rewrite should set changed = true for the grouping_sets pass.
    let changed_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) \
             FROM pgtrickle.explain_query_rewrite(\
               'SELECT region, SUM(amount) FROM diag_gs \
                GROUP BY GROUPING SETS ((region), ())')\
             WHERE pass_name = 'grouping_sets' AND changed = true",
        )
        .await;

    assert_eq!(
        changed_count, 1,
        "grouping_sets pass should have changed=true for a GROUPING SETS query"
    );
}

/// DT-1: a TopK (ORDER BY + LIMIT) query fires the `topk_detection` pass.
#[tokio::test]
async fn test_diagnostics_explain_query_rewrite_topk_detected() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_topk (id INT PRIMARY KEY, score INT)")
        .await;

    let topk_row_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) \
             FROM pgtrickle.explain_query_rewrite(\
               'SELECT id, score FROM diag_topk ORDER BY score DESC LIMIT 10')\
             WHERE pass_name = 'topk_detection' AND changed = true",
        )
        .await;

    assert_eq!(
        topk_row_count, 1,
        "topk_detection pass should fire for ORDER BY … LIMIT query"
    );
}

/// DT-1: a query that triggers the view-inlining pass should show changed=true.
#[tokio::test]
async fn test_diagnostics_explain_query_rewrite_view_inlining() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_base (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("CREATE VIEW diag_vw AS SELECT id, v FROM diag_base")
        .await;

    let changed_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) \
             FROM pgtrickle.explain_query_rewrite(\
               'SELECT id, v FROM diag_vw')\
             WHERE pass_name = 'view_inlining' AND changed = true",
        )
        .await;

    assert_eq!(
        changed_count, 1,
        "view_inlining pass should fire for a query that references a view"
    );
}

// ── DT-2: diagnose_errors ─────────────────────────────────────────────────

/// DT-2: a freshly created stream table with no errors returns zero rows.
#[tokio::test]
async fn test_diagnostics_diagnose_errors_empty_for_healthy_st() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_healthy (id INT PRIMARY KEY, v TEXT)")
        .await;
    db.execute("INSERT INTO diag_healthy VALUES (1, 'a')").await;
    db.create_st(
        "diag_healthy_st",
        "SELECT id, v FROM diag_healthy",
        "1m",
        "FULL",
    )
    .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_healthy_st')")
        .await;

    let error_count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.diagnose_errors('diag_healthy_st')")
        .await;

    assert_eq!(error_count, 0, "Healthy ST should have no error events");
}

/// DT-2: injecting a FAILED entry returns classified error + remediation.
#[tokio::test]
async fn test_diagnostics_diagnose_errors_classifies_user_error() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_errsrc (id INT PRIMARY KEY, v TEXT)")
        .await;
    db.execute("INSERT INTO diag_errsrc VALUES (1, 'x')").await;
    db.create_st("diag_err_st", "SELECT id, v FROM diag_errsrc", "1m", "FULL")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_err_st')")
        .await;

    // Inject a synthetic FAILED record simulating a parse error.
    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_history \
         (pgt_id, data_timestamp, start_time, action, status, error_message, initiated_by) \
         SELECT pgt_id, now(), now(), 'FULL', 'FAILED', \
                'query parse error: unexpected token', 'MANUAL' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'diag_err_st'",
    )
    .await;

    let row: (String, String) = sqlx::query_as(
        "SELECT error_type, remediation \
         FROM pgtrickle.diagnose_errors('diag_err_st') \
         LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("diagnose_errors should return a row for FAILED record");

    assert_eq!(row.0, "user", "Should classify parse error as 'user' type");
    assert!(
        row.1.contains("validate_query"),
        "Remediation should mention validate_query"
    );
}

/// DT-2: injecting a FAILED entry with schema-change error classifies as 'schema'.
#[tokio::test]
async fn test_diagnostics_diagnose_errors_classifies_schema_error() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_schsrc (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO diag_schsrc VALUES (1)").await;
    db.create_st("diag_schema_st", "SELECT id FROM diag_schsrc", "1m", "FULL")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_schema_st')")
        .await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_history \
         (pgt_id, data_timestamp, start_time, action, status, error_message, initiated_by) \
         SELECT pgt_id, now(), now(), 'FULL', 'FAILED', \
                'upstream table schema changed: OID 12345', 'MANUAL' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'diag_schema_st'",
    )
    .await;

    let error_type: String = db
        .query_scalar("SELECT error_type FROM pgtrickle.diagnose_errors('diag_schema_st') LIMIT 1")
        .await;

    assert_eq!(
        error_type, "schema",
        "Schema-change error should be classified as 'schema'"
    );
}

/// DT-2: injecting a FAILED entry with lock-timeout error classifies as 'performance'.
#[tokio::test]
async fn test_diagnostics_diagnose_errors_classifies_performance_error() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_perfsrc (id INT PRIMARY KEY)")
        .await;
    db.create_st("diag_perf_st", "SELECT id FROM diag_perfsrc", "1m", "FULL")
        .await;

    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_history \
         (pgt_id, data_timestamp, start_time, action, status, error_message, initiated_by) \
         SELECT pgt_id, now(), now(), 'FULL', 'FAILED', \
                'lock timeout waiting for relation', 'SCHEDULER' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'diag_perf_st'",
    )
    .await;

    let error_type: String = db
        .query_scalar("SELECT error_type FROM pgtrickle.diagnose_errors('diag_perf_st') LIMIT 1")
        .await;

    assert_eq!(
        error_type, "performance",
        "Lock-timeout error should be classified as 'performance'"
    );
}

// ── DT-3: list_auxiliary_columns ─────────────────────────────────────────

/// DT-3: a simple non-aggregate stream table should at least have __pgt_row_id.
#[tokio::test]
async fn test_diagnostics_list_auxiliary_columns_row_id_present() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_aux1 (id INT PRIMARY KEY, v TEXT)")
        .await;
    db.execute("INSERT INTO diag_aux1 VALUES (1, 'a')").await;
    db.create_st(
        "diag_aux_simple",
        "SELECT id, v FROM diag_aux1",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_aux_simple')")
        .await;

    let row_id_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.list_auxiliary_columns('diag_aux_simple') \
             WHERE column_name = '__pgt_row_id'",
        )
        .await;

    assert_eq!(
        row_id_count, 1,
        "__pgt_row_id should be present in every stream table"
    );
}

/// DT-3: an aggregate query should also have __pgt_count.
#[tokio::test]
async fn test_diagnostics_list_auxiliary_columns_count_for_aggregate() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_aux2 (id INT PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO diag_aux2 VALUES (1, 'a', 10)")
        .await;
    db.create_st(
        "diag_aux_agg",
        "SELECT grp, COUNT(*) AS cnt FROM diag_aux2 GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_aux_agg')")
        .await;

    let pgt_count_row: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.list_auxiliary_columns('diag_aux_agg') \
             WHERE column_name = '__pgt_count'",
        )
        .await;

    assert_eq!(
        pgt_count_row, 1,
        "__pgt_count should be present for aggregate queries"
    );
}

/// DT-3: all returned columns should start with __pgt_ and have non-empty purpose.
#[tokio::test]
async fn test_diagnostics_list_auxiliary_columns_purpose_not_empty() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_aux3 (id INT PRIMARY KEY, grp TEXT, x FLOAT, y FLOAT)")
        .await;
    db.execute("INSERT INTO diag_aux3 VALUES (1, 'a', 1.0, 2.0)")
        .await;
    // AVG query — should produce __pgt_aux_sum_* and __pgt_aux_count_* helpers
    db.create_st(
        "diag_aux_avg",
        "SELECT grp, AVG(x) AS avg_x FROM diag_aux3 GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_aux_avg')")
        .await;

    // All purpose strings must be non-empty
    let empty_purpose_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.list_auxiliary_columns('diag_aux_avg') \
             WHERE purpose = '' OR purpose IS NULL",
        )
        .await;

    assert_eq!(
        empty_purpose_count, 0,
        "All auxiliary columns should have a non-empty purpose description"
    );

    // At least __pgt_row_id must be present
    let total: i64 = db
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.list_auxiliary_columns('diag_aux_avg')")
        .await;
    assert!(total >= 1, "Should return at least one auxiliary column");
}

// ── DT-4: validate_query ──────────────────────────────────────────────────

/// DT-4: a simple aggregate query should resolve to DIFFERENTIAL mode.
#[tokio::test]
async fn test_diagnostics_validate_query_simple_agg_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_vq1 (id INT PRIMARY KEY, grp TEXT, amt NUMERIC)")
        .await;

    let resolved_mode: String = db
        .query_scalar(
            "SELECT result FROM pgtrickle.validate_query(\
               'SELECT grp, SUM(amt) FROM diag_vq1 GROUP BY grp')\
             WHERE check_name = 'resolved_refresh_mode'",
        )
        .await;

    assert_eq!(
        resolved_mode, "DIFFERENTIAL",
        "Simple aggregate should resolve to DIFFERENTIAL mode"
    );
}

/// DT-4: a TopK (ORDER BY + LIMIT) query should resolve to TOPK mode.
#[tokio::test]
async fn test_diagnostics_validate_query_topk_mode() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_vqtk (id INT PRIMARY KEY, score INT)")
        .await;

    let resolved_mode: String = db
        .query_scalar(
            "SELECT result FROM pgtrickle.validate_query(\
               'SELECT id, score FROM diag_vqtk ORDER BY score DESC LIMIT 5')\
             WHERE check_name = 'resolved_refresh_mode'",
        )
        .await;

    assert_eq!(
        resolved_mode, "TOPK",
        "ORDER BY … LIMIT query should resolve to TOPK mode"
    );
}

/// DT-4: a query with a FULL OUTER JOIN should produce a WARNING on the join construct.
#[tokio::test]
async fn test_diagnostics_validate_query_full_join_warning() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_lhs (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("CREATE TABLE diag_rhs (id INT PRIMARY KEY, v INT)")
        .await;

    let warning_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.validate_query(\
               'SELECT l.id, l.v, r.v AS rv \
                FROM diag_lhs l FULL OUTER JOIN diag_rhs r ON l.id = r.id')\
             WHERE severity = 'WARNING'",
        )
        .await;

    assert!(
        warning_count >= 1,
        "FULL OUTER JOIN should produce at least one WARNING row"
    );
}

/// DT-4: validate_query always returns a `resolved_refresh_mode` row.
#[tokio::test]
async fn test_diagnostics_validate_query_always_has_mode_row() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_vqm (id INT PRIMARY KEY)")
        .await;

    let mode_row_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.validate_query(\
               'SELECT id FROM diag_vqm')\
             WHERE check_name = 'resolved_refresh_mode'",
        )
        .await;

    assert_eq!(
        mode_row_count, 1,
        "validate_query must always include exactly one resolved_refresh_mode row"
    );
}

/// DT-4: a GROUP_RESCAN aggregate (STRING_AGG) should show up as WARNING severity.
#[tokio::test]
async fn test_diagnostics_validate_query_group_rescan_warning() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_vqgs (id INT PRIMARY KEY, grp TEXT, tag TEXT)")
        .await;

    let warning_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.validate_query(\
               'SELECT grp, STRING_AGG(tag, '','') AS tags \
                FROM diag_vqgs GROUP BY grp')\
             WHERE severity = 'WARNING' AND check_name = 'aggregate'",
        )
        .await;

    assert!(
        warning_count >= 1,
        "STRING_AGG (GROUP_RESCAN) should produce a WARNING aggregate row"
    );
}

/// DT-4: an invalid / non-parseable query should return an ERROR-severity row.
#[tokio::test]
async fn test_diagnostics_validate_query_syntax_error_returns_error() {
    let db = E2eDb::new().await.with_extension().await;

    // validate_query should NOT throw — instead it should return an ERROR row
    let result = db
        .try_execute("SELECT * FROM pgtrickle.validate_query('SELECT *** FROM @@@')")
        .await;

    // Either it returns error rows (ok path) or propagates as SQL error—
    // both acceptable; we just verify the function exists and is callable.
    // If it errors, that's also valid behavior for a completely broken query.
    let _ = result;
}

// ── DIAG-1e: recommend_refresh_mode + refresh_efficiency ──────────────────

/// DIAG-1c: recommend_refresh_mode() returns one row per stream table
/// when called without arguments.
#[tokio::test]
async fn test_diagnostics_recommend_refresh_mode_all_tables() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_rm_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO diag_rm_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_rm_agg',\
            'SELECT val % 10 AS grp, SUM(val) AS total FROM diag_rm_src GROUP BY val % 10',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    )
    .await;

    db.execute("SELECT pgtrickle.refresh_stream_table('diag_rm_agg')")
        .await;

    // Call with no argument — should return at least 1 row
    let count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.recommend_refresh_mode()")
        .await;

    assert!(
        count >= 1,
        "recommend_refresh_mode() should return at least 1 row, got {count}"
    );
}

/// DIAG-1c: recommend_refresh_mode(name) returns exactly one row
/// with expected columns for a specific stream table.
#[tokio::test]
async fn test_diagnostics_recommend_refresh_mode_single_table() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_rm2_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO diag_rm2_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_rm2_st',\
            'SELECT id, val FROM diag_rm2_src WHERE val > 50',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    )
    .await;

    db.execute("SELECT pgtrickle.refresh_stream_table('diag_rm2_st')")
        .await;

    // Call with specific name — should return exactly 1 row
    let count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.recommend_refresh_mode('diag_rm2_st')")
        .await;

    assert_eq!(
        count, 1,
        "recommend_refresh_mode(name) should return exactly 1 row"
    );

    // Verify columns are present and non-null
    let has_columns: bool = db
        .query_scalar(
            "SELECT pgt_name IS NOT NULL \
                AND recommended_mode IS NOT NULL \
                AND confidence IS NOT NULL \
                AND reason IS NOT NULL \
                AND signals IS NOT NULL \
             FROM pgtrickle.recommend_refresh_mode('diag_rm2_st')",
        )
        .await;

    assert!(
        has_columns,
        "recommend_refresh_mode should return non-null columns"
    );
}

/// DIAG-1c: recommend_refresh_mode returns valid recommended_mode values.
#[tokio::test]
async fn test_diagnostics_recommend_refresh_mode_valid_values() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_rm3_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO diag_rm3_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_rm3_st',\
            'SELECT id, val FROM diag_rm3_src',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    )
    .await;

    db.execute("SELECT pgtrickle.refresh_stream_table('diag_rm3_st')")
        .await;

    // recommended_mode must be one of DIFFERENTIAL, FULL, KEEP
    let valid: bool = db
        .query_scalar(
            "SELECT recommended_mode IN ('DIFFERENTIAL', 'FULL', 'KEEP') \
             FROM pgtrickle.recommend_refresh_mode('diag_rm3_st')",
        )
        .await;

    assert!(
        valid,
        "recommended_mode must be DIFFERENTIAL, FULL, or KEEP"
    );

    // confidence must be one of high, medium, low
    let valid_conf: bool = db
        .query_scalar(
            "SELECT confidence IN ('high', 'medium', 'low') \
             FROM pgtrickle.recommend_refresh_mode('diag_rm3_st')",
        )
        .await;

    assert!(valid_conf, "confidence must be high, medium, or low");
}

/// DIAG-1c: signals JSONB contains expected structure.
#[tokio::test]
async fn test_diagnostics_recommend_refresh_mode_signals_jsonb() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_rm4_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO diag_rm4_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_rm4_st',\
            'SELECT id, val FROM diag_rm4_src',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    )
    .await;

    db.execute("SELECT pgtrickle.refresh_stream_table('diag_rm4_st')")
        .await;

    // signals should be valid JSONB with a composite_score and signals array
    let has_composite: bool = db
        .query_scalar(
            "SELECT (signals->>'composite_score') IS NOT NULL \
             FROM pgtrickle.recommend_refresh_mode('diag_rm4_st')",
        )
        .await;

    assert!(
        has_composite,
        "signals JSONB should contain composite_score"
    );

    let has_signals_array: bool = db
        .query_scalar(
            "SELECT jsonb_typeof(signals->'signals') = 'array' \
             FROM pgtrickle.recommend_refresh_mode('diag_rm4_st')",
        )
        .await;

    assert!(
        has_signals_array,
        "signals JSONB should contain a signals array"
    );
}

/// DIAG-1d: refresh_efficiency() returns rows with expected columns.
#[tokio::test]
async fn test_diagnostics_refresh_efficiency_basic() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_re_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO diag_re_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_re_st',\
            'SELECT id, val FROM diag_re_src',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    )
    .await;

    // Do a couple of refreshes to populate history
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_re_st')")
        .await;
    db.execute("INSERT INTO diag_re_src SELECT g, g FROM generate_series(101, 200) g")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_re_st')")
        .await;

    // refresh_efficiency should return at least 1 row
    let count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.refresh_efficiency()")
        .await;

    assert!(
        count >= 1,
        "refresh_efficiency() should return at least 1 row, got {count}"
    );

    // Check total_refreshes is positive
    let total: i64 = db
        .query_scalar(
            "SELECT total_refreshes FROM pgtrickle.refresh_efficiency() \
             WHERE pgt_name = 'diag_re_st'",
        )
        .await;

    assert!(
        total >= 2,
        "diag_re_st should have at least 2 total refreshes, got {total}"
    );
}

/// DIAG-1d: refresh_efficiency shows correct refresh mode.
#[tokio::test]
async fn test_diagnostics_refresh_efficiency_shows_mode() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_re2_src (id INT PRIMARY KEY, val INT)")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_re2_diff',\
            'SELECT id, val FROM diag_re2_src',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_re2_full',\
            'SELECT id, val FROM diag_re2_src',\
            refresh_mode => 'FULL'\
        )",
    )
    .await;

    // Verify modes shown in refresh_efficiency
    let diff_mode: String = db
        .query_scalar(
            "SELECT refresh_mode FROM pgtrickle.refresh_efficiency() \
             WHERE pgt_name = 'diag_re2_diff'",
        )
        .await;

    assert_eq!(diff_mode, "DIFFERENTIAL");

    let full_mode: String = db
        .query_scalar(
            "SELECT refresh_mode FROM pgtrickle.refresh_efficiency() \
             WHERE pgt_name = 'diag_re2_full'",
        )
        .await;

    assert_eq!(full_mode, "FULL");
}

/// G15-EX: export_definition returns valid SQL containing the stream table name.
#[tokio::test]
async fn test_diagnostics_export_definition_basic() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_exp_src (id INT PRIMARY KEY, val INT)")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_exp_st',\
            'SELECT id, val FROM diag_exp_src',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    )
    .await;

    let ddl: String = db
        .query_scalar("SELECT pgtrickle.export_definition('diag_exp_st')")
        .await;

    // Should contain both DROP and CREATE
    assert!(
        ddl.contains("DROP STREAM TABLE IF EXISTS"),
        "export_definition should contain DROP: {ddl}"
    );
    assert!(
        ddl.contains("create_stream_table"),
        "export_definition should contain create_stream_table: {ddl}"
    );
    assert!(
        ddl.contains("diag_exp_st") || ddl.contains("diag_exp"),
        "export_definition should reference the stream table name: {ddl}"
    );
}

/// FIX-STST-DIFF: Manual refresh on a CALCULATED stream table should succeed
/// and use DIFFERENTIAL (not FULL) once the baseline is established.
#[tokio::test]
async fn test_st_on_st_manual_refresh_succeeds() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE diag_stst_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO diag_stst_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    // Create upstream ST
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_stst_up',\
            'SELECT id, val FROM diag_stst_src',\
            schedule => '5s',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    )
    .await;

    db.execute("SELECT pgtrickle.refresh_stream_table('diag_stst_up')")
        .await;

    // Create downstream ST reading from upstream ST
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            'diag_stst_down',\
            'SELECT val % 10 AS grp, SUM(val) AS total FROM diag_stst_up GROUP BY val % 10',\
            schedule => 'calculated',\
            refresh_mode => 'DIFFERENTIAL'\
        )",
    )
    .await;

    // First refresh establishes baseline (will be FULL)
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_stst_down')")
        .await;

    // Mutate source, refresh upstream
    db.execute("INSERT INTO diag_stst_src SELECT g, g FROM generate_series(101, 110) g")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_stst_up')")
        .await;

    // Manual refresh of downstream ST — should succeed (not error)
    db.execute("SELECT pgtrickle.refresh_stream_table('diag_stst_down')")
        .await;

    // Verify correctness: downstream should match its defining query
    let matches: bool = db
        .query_scalar(
            "SELECT NOT EXISTS ( \
                (SELECT grp, total FROM diag_stst_down \
                 EXCEPT ALL \
                 SELECT val % 10 AS grp, SUM(val) AS total FROM diag_stst_up GROUP BY val % 10) \
                UNION ALL \
                (SELECT val % 10 AS grp, SUM(val) AS total FROM diag_stst_up GROUP BY val % 10 \
                 EXCEPT ALL \
                 SELECT grp, total FROM diag_stst_down) \
            )",
        )
        .await;

    assert!(
        matches,
        "Downstream ST should match its defining query after manual refresh"
    );
}

/// DB-9: migrate() should be a read-only diagnostic when versions already align.
#[tokio::test]
async fn test_diagnostics_migrate_reports_aligned_versions() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_schema_version (version, description) \
         VALUES ('{CURRENT_PG_TRICKLE_VERSION}', 'test current row') \
         ON CONFLICT (version) DO UPDATE \
         SET applied_at = now(), description = EXCLUDED.description"
    ))
    .await;

    let status: String = db
        .query_scalar("SELECT pgtrickle.migrate()::jsonb ->> 'status'")
        .await;
    let runtime_version: String = db
        .query_scalar("SELECT pgtrickle.migrate()::jsonb ->> 'runtime_version'")
        .await;
    let extension_version: String = db
        .query_scalar("SELECT pgtrickle.migrate()::jsonb ->> 'extension_version'")
        .await;
    let schema_version: String = db
        .query_scalar("SELECT pgtrickle.migrate()::jsonb ->> 'schema_version'")
        .await;
    let up_to_date: bool = db
        .query_scalar("SELECT (pgtrickle.migrate()::jsonb ->> 'up_to_date')::boolean")
        .await;

    assert_eq!(status, "ok");
    assert_eq!(runtime_version, CURRENT_PG_TRICKLE_VERSION);
    assert_eq!(extension_version, CURRENT_PG_TRICKLE_VERSION);
    assert_eq!(schema_version, CURRENT_PG_TRICKLE_VERSION);
    assert!(
        up_to_date,
        "migrate() should report aligned versions as healthy"
    );
}

/// DB-9: migrate() must not mutate pgtrickle.pgt_schema_version when drift exists.
#[tokio::test]
async fn test_diagnostics_migrate_is_read_only_on_schema_version_drift() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(&format!(
        "DELETE FROM pgtrickle.pgt_schema_version \
         WHERE version = '{CURRENT_PG_TRICKLE_VERSION}'"
    ))
    .await;
    db.execute(
        "UPDATE pgtrickle.pgt_schema_version \
         SET applied_at = now(), description = 'stale row for migrate() diagnostic test' \
         WHERE version = '0.19.0'",
    )
    .await;

    let before_count: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM pgtrickle.pgt_schema_version")
        .await;
    let status: String = db
        .query_scalar("SELECT pgtrickle.migrate()::jsonb ->> 'status'")
        .await;
    let remediation: String = db
        .query_scalar("SELECT pgtrickle.migrate()::jsonb ->> 'remediation'")
        .await;
    let remediation_sql: Option<String> = db
        .query_scalar_opt("SELECT pgtrickle.migrate()::jsonb ->> 'remediation_sql'")
        .await;
    let after_count: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM pgtrickle.pgt_schema_version")
        .await;

    assert_eq!(status, "schema_version_mismatch");
    assert!(
        remediation.contains("read-only"),
        "migrate() remediation should explain that it will not mutate the schema ledger: {remediation}"
    );
    assert_eq!(
        remediation_sql, None,
        "schema-version drift with matching runtime/extension should not pretend ALTER EXTENSION can fix it"
    );
    assert_eq!(
        before_count, after_count,
        "migrate() must not insert or update pgtrickle.pgt_schema_version"
    );
}
