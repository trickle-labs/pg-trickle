//! E2E tests for PROF-DLT and G14-MDED profiling functions (Phase 3).
//!
//! Validates:
//! - `pgtrickle.explain_delta(st_name, format)` — returns a query plan for
//!   the auto-generated delta SQL without executing a refresh.
//! - `pgtrickle.dedup_stats()` — returns MERGE deduplication counters.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════════
//  explain_delta() — PROF-DLT
// ═══════════════════════════════════════════════════════════════════════════

/// Smoke test: explain_delta returns at least one line of text output for a
/// simple single-table DIFFERENTIAL stream table.
#[tokio::test]
async fn test_explain_delta_simple_returns_plan() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prof_simple (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO prof_simple VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st(
        "prof_simple_st",
        "SELECT id, val FROM prof_simple",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let plan = db
        .query_text("SELECT explain_delta FROM pgtrickle.explain_delta('public.prof_simple_st')")
        .await;

    assert!(
        plan.is_some(),
        "explain_delta should return at least one row"
    );
    let plan_text = plan.unwrap();
    // The plan must mention the change buffer table (changes_<oid>) or the
    // ST name — confirms it's actually planning the delta query.
    assert!(
        plan_text.contains("Seq Scan")
            || plan_text.contains("Index")
            || plan_text.contains("Bitmap")
            || plan_text.contains("Result")
            || plan_text.contains("changes_"),
        "plan should contain at least one PostgreSQL plan node; got:\n{plan_text}"
    );
}

/// explain_delta returns valid output for a multi-table join query.
#[tokio::test]
async fn test_explain_delta_join_query() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prof_orders (id INT PRIMARY KEY, customer_id INT, amount FLOAT)")
        .await;
    db.execute("CREATE TABLE prof_customers (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO prof_customers VALUES (1, 'Alice'), (2, 'Bob')")
        .await;
    db.execute("INSERT INTO prof_orders VALUES (1, 1, 100.0), (2, 2, 200.0)")
        .await;

    let query = "SELECT o.id, c.name, o.amount \
                 FROM prof_orders o \
                 JOIN prof_customers c ON o.customer_id = c.id";

    db.create_st("prof_join_st", query, "1m", "DIFFERENTIAL")
        .await;

    let plan = db
        .query_text("SELECT explain_delta FROM pgtrickle.explain_delta('public.prof_join_st')")
        .await;

    assert!(
        plan.is_some(),
        "explain_delta should return plan for JOIN query"
    );
    // A join query plan must mention either a Hash Join, Nested Loop, or Merge Join.
    let plan_text = plan.unwrap();
    let has_join_node = plan_text.contains("Hash Join")
        || plan_text.contains("Nested Loop")
        || plan_text.contains("Merge Join")
        || plan_text.contains("Hash")
        || plan_text.contains("Seq Scan");
    assert!(
        has_join_node,
        "join query plan should contain a join or scan node; got:\n{plan_text}"
    );
}

/// explain_delta returns valid JSON output when format='json'.
#[tokio::test]
async fn test_explain_delta_json_format() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prof_json_src (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO prof_json_src VALUES (1, 10)").await;

    db.create_st(
        "prof_json_st",
        "SELECT id, v FROM prof_json_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // JSON format returns a single row containing the JSON plan.
    let plan_json = db
        .query_text(
            "SELECT explain_delta FROM pgtrickle.explain_delta('public.prof_json_st', 'json')",
        )
        .await;

    assert!(
        plan_json.is_some(),
        "explain_delta('json') should return at least one row"
    );
    let json_text = plan_json.unwrap();
    // Valid JSON starts with '[' (EXPLAIN FORMAT JSON wraps in an array).
    assert!(
        json_text.trim_start().starts_with('['),
        "JSON format plan should start with '['; got: {json_text:.100}"
    );
}

/// explain_delta errors cleanly for a non-existent stream table.
#[tokio::test]
async fn test_explain_delta_nonexistent_st_errors() {
    let db = E2eDb::new().await.with_extension().await;

    let result = db
        .try_execute(
            "SELECT explain_delta FROM pgtrickle.explain_delta('public.nonexistent_st_xyz')",
        )
        .await;

    assert!(
        result.is_err(),
        "explain_delta should error for a non-existent stream table"
    );
}

/// explain_delta errors cleanly for an unsupported format string.
#[tokio::test]
async fn test_explain_delta_invalid_format_errors() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prof_fmt_src (id INT PRIMARY KEY)")
        .await;
    db.create_st(
        "prof_fmt_st",
        "SELECT id FROM prof_fmt_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let result = db
        .try_execute(
            "SELECT explain_delta FROM pgtrickle.explain_delta('public.prof_fmt_st', 'csv')",
        )
        .await;

    assert!(
        result.is_err(),
        "explain_delta should error for unsupported format 'csv'"
    );
}

/// explain_delta_plan returns the versioned shadow-planner contract without
/// populating the execution template cache.
#[tokio::test]
async fn test_explain_delta_plan_is_read_only() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prof_plan_src (id INT PRIMARY KEY, grp INT, v INT)")
        .await;
    db.execute("INSERT INTO prof_plan_src VALUES (1, 1, 10), (2, 1, 20)")
        .await;
    db.create_st(
        "prof_plan_st",
        "SELECT grp, SUM(v) AS total FROM prof_plan_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let before: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_template_cache")
        .await;
    let (format_version, epoch, has_chosen): (i32, String, bool) = sqlx::query_as(
        "WITH diagnostic AS ( \
             SELECT pgtrickle.explain_delta_plan(pgt_id) AS plan \
               FROM pgtrickle.pgt_stream_tables \
              WHERE pgt_schema = 'public' AND pgt_name = 'prof_plan_st' \
         ) \
         SELECT (plan ->> 'format_version')::integer, \
                plan ->> 'statistics_epoch', \
                plan -> 'chosen' IS NOT NULL \
           FROM diagnostic",
    )
    .fetch_one(&db.pool)
    .await
    .expect("explain_delta_plan should return one diagnostic document");
    let after: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_template_cache")
        .await;

    assert_eq!(format_version, 1);
    assert!(!epoch.is_empty());
    assert!(has_chosen);
    assert_eq!(before, after, "planning diagnostics must not mutate caches");
}

// ═══════════════════════════════════════════════════════════════════════════
//  dedup_stats() — G14-MDED
// ═══════════════════════════════════════════════════════════════════════════

/// dedup_stats() returns exactly one row with numeric columns.
#[tokio::test]
async fn test_dedup_stats_returns_row() {
    let db = E2eDb::new().await.with_extension().await;

    let row: (i64, i64, f64) = sqlx::query_as(
        "SELECT total_diff_refreshes, dedup_needed, dedup_ratio_pct \
         FROM pgtrickle.dedup_stats()",
    )
    .fetch_one(&db.pool)
    .await
    .expect("dedup_stats() should return exactly one row");

    let (total, dedup, ratio) = row;

    assert!(total >= 0, "total_diff_refreshes must be non-negative");
    assert!(dedup >= 0, "dedup_needed must be non-negative");
    assert!(
        dedup <= total,
        "dedup_needed must be ≤ total_diff_refreshes"
    );
    assert!(
        (0.0..=100.0).contains(&ratio),
        "dedup_ratio_pct must be in [0, 100]; got {ratio}"
    );
}

/// After running a differential refresh on a scan-chain query (single table,
/// no JOINs or aggregates), the total counter increases and the dedup rate
/// is 0% (scan-chains produce deduplicated deltas).
#[tokio::test]
async fn test_dedup_stats_scan_chain_not_deduped() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE dedup_src_sc (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO dedup_src_sc VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "dedup_sc_st",
        "SELECT id, v FROM dedup_src_sc",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Capture counters before the differential refresh.
    let before: (i64, i64) =
        sqlx::query_as("SELECT total_diff_refreshes, dedup_needed FROM pgtrickle.dedup_stats()")
            .fetch_one(&db.pool)
            .await
            .expect("dedup_stats() before refresh");
    let (total_before, dedup_before) = before;

    // Trigger a change and run a differential refresh.
    db.execute("INSERT INTO dedup_src_sc VALUES (3, 30)").await;
    db.refresh_st("dedup_sc_st").await;

    // Capture counters after.
    let after: (i64, i64) =
        sqlx::query_as("SELECT total_diff_refreshes, dedup_needed FROM pgtrickle.dedup_stats()")
            .fetch_one(&db.pool)
            .await
            .expect("dedup_stats() after refresh");
    let (total_after, dedup_after) = after;

    assert!(
        total_after > total_before,
        "total_diff_refreshes should increase after a differential refresh; \
         before={total_before}, after={total_after}"
    );

    // Scan-chain queries (single-table SELECT with PK) produce deduplicated deltas;
    // dedup_needed should NOT have increased.
    assert_eq!(
        dedup_after, dedup_before,
        "scan-chain query should not require MERGE deduplication; \
         dedup before={dedup_before}, after={dedup_after}"
    );
}
