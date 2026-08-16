//! E2E tests for error handling and edge cases.
//!
//! Validates the extension rejects invalid inputs gracefully and
//! handles edge cases like subqueries, CTEs, and DDL on source tables.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Invalid SQL ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_invalid_sql_in_defining_query() {
    let db = E2eDb::new().await.with_extension().await;

    // Typo: FORM instead of FROM
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('bad_sql_st', \
             $$ SELECT * FORM orders $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "Typo in SQL should produce a clear error");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("syntax")
            || err_msg.contains("FORM")
            || err_msg.contains("orders"),
        "Error should indicate a SQL syntax issue, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_self_referencing_query_fails() {
    let db = E2eDb::new().await.with_extension().await;

    // Self-referencing: the ST in its own defining query
    // Note: the table doesn't exist yet when create is called, so this should fail
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('self_ref_st', \
             $$ SELECT * FROM self_ref_st $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_err(),
        "Self-referencing defining query should fail"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("self_ref_st")
            || err_msg.to_lowercase().contains("does not exist")
            || err_msg.to_lowercase().contains("self-referenc"),
        "Error should reference the ST name or indicate it doesn't exist, got: {err_msg}"
    );
}

// ── Valid Complex Queries ──────────────────────────────────────────────

#[tokio::test]
async fn test_subquery_in_defining_query() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sub_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO sub_src VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    // Subqueries in FROM are supported since CTE Tier 1 implementation.
    // FULL mode should work — the raw SQL is valid PostgreSQL.
    db.create_st(
        "subq_st",
        "SELECT * FROM (SELECT id, val FROM sub_src WHERE val > 10) sub",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.subq_st").await;
    assert_eq!(count, 2, "Subquery should return rows where val > 10");
}

#[tokio::test]
async fn test_cte_with_window_function_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE cte_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO cte_src VALUES (1, 100), (2, 200), (3, 300)")
        .await;

    // CTEs are now supported. Window functions (ROW_NUMBER) are valid SQL
    // so FULL mode should work — the raw query is executed as-is.
    db.create_st(
        "cte_st",
        "WITH ranked AS (SELECT id, val, ROW_NUMBER() OVER (ORDER BY val DESC) AS rn FROM cte_src) SELECT id, val FROM ranked WHERE rn <= 2",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.cte_st").await;
    assert_eq!(count, 2, "Should return top 2 rows by val DESC");
}

// ── Schedule Edge Cases ──────────────────────────────────────────────

#[tokio::test]
async fn test_create_with_zero_schedule() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE zero_sched_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO zero_sched_src VALUES (1)").await;

    // Zero schedule — might succeed or be rejected, depending on implementation
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('zero_sched_st', \
             $$ SELECT id FROM zero_sched_src $$, '0s', 'FULL')",
        )
        .await;

    // Either it succeeds with zero stored, or it's rejected
    // If it succeeds, verify it's stored correctly
    if result.is_ok() {
        let sched: String = db
            .query_scalar(
                "SELECT schedule FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'zero_sched_st'",
            )
            .await;
        assert_eq!(sched, "0s");
    }
    // If it fails, that's also acceptable behavior
}

#[tokio::test]
async fn test_create_with_negative_schedule() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE neg_sched_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO neg_sched_src VALUES (1)").await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('neg_sched_st', \
             $$ SELECT id FROM neg_sched_src $$, '-1m', 'FULL')",
        )
        .await;

    // Negative durations should be rejected by the parser
    // '-1m' has a '-' which is not a valid digit or unit
    assert!(result.is_err(), "Negative schedule should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("schedule")
            || err_msg.to_lowercase().contains("duration")
            || err_msg.to_lowercase().contains("invalid")
            || err_msg.contains("-1m"),
        "Error should indicate the schedule is invalid, got: {err_msg}"
    );
}

// ── Source Table DDL ───────────────────────────────────────────────────

#[tokio::test]
async fn test_drop_source_table_with_active_st() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE drop_me_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO drop_me_src VALUES (1, 'data')")
        .await;

    db.create_st("orphan_st", "SELECT id, val FROM drop_me_src", "1m", "FULL")
        .await;

    // Drop the source table while ST exists
    // The event trigger should handle this (or CASCADE might clean up)
    let result = db.try_execute("DROP TABLE drop_me_src CASCADE").await;

    // Whether this succeeds or not depends on event trigger behavior.
    // If it succeeds, the ST might be invalidated or auto-dropped.
    // If it fails, the extension prevents orphaning.
    if result.is_ok() {
        // Check if ST was cleaned up or marked for reinit
        let st_exists: bool = db
            .query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'orphan_st')",
            )
            .await;
        // Either the ST was cleaned up, or it may still exist in error state
        if st_exists {
            let (status, _, _, _) = db.pgt_status("orphan_st").await;
            // The ST should be in some error/invalid state
            assert!(
                status == "ERROR" || status == "SUSPENDED" || status == "ACTIVE",
                "ST should be in a recognizable state after source drop: {}",
                status
            );
        }
    }
}

#[tokio::test]
async fn test_alter_source_table_add_column() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE alter_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO alter_src VALUES (1, 'hello')")
        .await;

    db.create_st(
        "alter_src_st",
        "SELECT id, val FROM alter_src",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.alter_src_st").await, 1);

    // Add a column to the source table (disable the DDL guard, alter, re-enable —
    // all on the same connection so the session-local SET is visible to the ALTER).
    db.execute_seq(&[
        "SET pg_trickle.block_source_ddl = false",
        "ALTER TABLE alter_src ADD COLUMN new_col INT DEFAULT 42",
        "SET pg_trickle.block_source_ddl = true",
    ])
    .await;

    // Insert a row with the new column
    db.execute("INSERT INTO alter_src VALUES (2, 'world', 99)")
        .await;

    // Refresh should still work (defining query only selects id, val)
    db.refresh_st("alter_src_st").await;

    assert_eq!(db.count("public.alter_src_st").await, 2);
}

// ── ORDER BY / LIMIT / OFFSET ──────────────────────────────────────────

#[tokio::test]
async fn test_order_by_without_limit_is_accepted() {
    // ORDER BY is silently ignored for stream tables (like CREATE MATERIALIZED VIEW)
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE orderby_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO orderby_src VALUES (1, 'a'), (2, 'b')")
        .await;

    // Should succeed — ORDER BY is accepted and discarded
    db.create_st(
        "orderby_st",
        "SELECT id, val FROM orderby_src ORDER BY id",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.orderby_st").await;
    assert_eq!(count, 2, "ORDER BY query should create ST with all rows");
}

#[tokio::test]
async fn test_limit_returns_unsupported_error() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE limit_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO limit_src SELECT g, g FROM generate_series(1, 10) g")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('limit_st', \
             $$ SELECT id, val FROM limit_src LIMIT 5 $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "LIMIT should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("LIMIT"),
        "Error should mention LIMIT, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_offset_returns_unsupported_error() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE offset_src (id INT PRIMARY KEY, val INT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('offset_st', \
             $$ SELECT id, val FROM offset_src OFFSET 5 $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "OFFSET should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("OFFSET"),
        "Error should mention OFFSET, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_order_by_with_limit_accepted_as_topk() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE orderlimit_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO orderlimit_src VALUES (1, 10), (2, 20)")
        .await;

    // ORDER BY + LIMIT is now accepted as a TopK stream table
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('orderlimit_st', \
             $$ SELECT id, val FROM orderlimit_src ORDER BY id LIMIT 10 $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "ORDER BY + LIMIT should be accepted as TopK, got: {:?}",
        result.err()
    );

    // Verify it was created with TopK metadata
    let topk_limit: i32 = db
        .query_scalar(
            "SELECT topk_limit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'orderlimit_st'",
        )
        .await;
    assert_eq!(topk_limit, 10, "topk_limit should be stored");
}

#[tokio::test]
async fn test_limit_offset_combined_returns_error() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE limoff_src (id INT PRIMARY KEY, val INT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('limoff_st', \
             $$ SELECT id, val FROM limoff_src LIMIT 10 OFFSET 5 $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_err(),
        "LIMIT + OFFSET together should be rejected"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("OFFSET") || err_msg.contains("LIMIT"),
        "Error should mention LIMIT or OFFSET, got: {err_msg}"
    );
}

// ── FETCH FIRST / FETCH NEXT rejection (G1) ────────────────────────────

#[tokio::test]
async fn test_fetch_first_returns_unsupported_error() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fetch_src (id INT PRIMARY KEY, val INT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('fetch_st', \
             $$ SELECT id, val FROM fetch_src FETCH FIRST 5 ROWS ONLY $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "FETCH FIRST should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("LIMIT"),
        "Error should mention LIMIT, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_fetch_next_returns_unsupported_error() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fetchnext_src (id INT PRIMARY KEY, val INT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('fetchnext_st', \
             $$ SELECT id, val FROM fetchnext_src FETCH NEXT 3 ROWS ONLY $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "FETCH NEXT should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("LIMIT") || err_msg.contains("FETCH"),
        "Error should mention LIMIT or FETCH, got: {err_msg}"
    );
}

// ── FOR UPDATE / FOR SHARE rejection ───────────────────────────────────

#[tokio::test]
async fn test_for_update_rejected_with_clear_error() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE forupd_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO forupd_src VALUES (1, 'a')").await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('forupd_st', \
             $$ SELECT id, val FROM forupd_src FOR UPDATE $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "FOR UPDATE should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("FOR UPDATE") || err_msg.contains("FOR SHARE"),
        "Error should mention FOR UPDATE/FOR SHARE, got: {err_msg}"
    );
    assert!(
        err_msg.contains("row-level locking"),
        "Error should explain the reason, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_for_share_rejected_with_clear_error() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE forshr_src (id INT PRIMARY KEY, val TEXT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('forshr_st', \
             $$ SELECT id, val FROM forshr_src FOR SHARE $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "FOR SHARE should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("FOR UPDATE") || err_msg.contains("FOR SHARE"),
        "Error should mention FOR UPDATE/FOR SHARE, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_for_no_key_update_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE fornku_src (id INT PRIMARY KEY, val TEXT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('fornku_st', \
             $$ SELECT id, val FROM fornku_src FOR NO KEY UPDATE $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "FOR NO KEY UPDATE should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("FOR UPDATE")
            || err_msg.contains("FOR SHARE")
            || err_msg.contains("row-level locking"),
        "Error should mention row-level locking clause, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_for_key_share_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE forks_src (id INT PRIMARY KEY, val TEXT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('forks_st', \
             $$ SELECT id, val FROM forks_src FOR KEY SHARE $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "FOR KEY SHARE should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("FOR UPDATE")
            || err_msg.contains("FOR SHARE")
            || err_msg.contains("row-level locking"),
        "Error should mention row-level locking clause, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_order_by_in_differential_mode_is_accepted() {
    // ORDER BY should also be accepted (and ignored) in DIFFERENTIAL mode
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE orderby_diff_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO orderby_diff_src VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "orderby_diff_st",
        "SELECT id, val FROM orderby_diff_src ORDER BY val DESC",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    let count = db.count("public.orderby_diff_st").await;
    assert_eq!(count, 2, "ORDER BY in DIFFERENTIAL mode should be accepted");
}

// ── Concurrent Refresh Safety ──────────────────────────────────────────

#[tokio::test]
async fn test_concurrent_refresh_safety() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE conc_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO conc_src SELECT g, g FROM generate_series(1, 100) g")
        .await;

    db.create_st("conc_st", "SELECT id, val FROM conc_src", "1m", "FULL")
        .await;

    // Insert more data
    db.execute("INSERT INTO conc_src SELECT g, g FROM generate_series(101, 200) g")
        .await;

    // Two concurrent refreshes — use advisory lock so one waits
    let pool = db.pool.clone();
    let pool2 = db.pool.clone();

    let h1 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('conc_st')")
            .execute(&pool)
            .await
    });
    let h2 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('conc_st')")
            .execute(&pool2)
            .await
    });

    let (r1, r2) = tokio::join!(h1, h2);

    // At least one should succeed. Both might succeed (serialized by advisory lock).
    // We just need no data corruption.
    let success_count = [r1, r2]
        .iter()
        .filter(|r| r.as_ref().map(|inner| inner.is_ok()).unwrap_or(false))
        .count();
    assert!(
        success_count >= 1,
        "At least one concurrent refresh should succeed"
    );

    // Verify data integrity
    let count = db.count("public.conc_st").await;
    assert_eq!(count, 200, "All 200 rows should be present after refresh");
}

// ── GROUPING SETS / CUBE / ROLLUP rejection ─────────────────────────────

#[tokio::test]
async fn test_grouping_sets_accepted_via_rewrite() {
    // GROUPING SETS are auto-rewritten to UNION ALL of plain GROUP BY queries.
    // Note: The rewrite currently has a limitation when GROUPING SETS reference
    // columns that aren't in the select list, which may produce errors.
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE gs_src (id SERIAL PRIMARY KEY, dept TEXT, region TEXT, amount NUMERIC)",
    )
    .await;

    // Use a simpler GROUPING SETS that only references columns in select list.
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('gs_st', \
             $$ SELECT dept, SUM(amount) FROM gs_src \
             GROUP BY GROUPING SETS ((dept), ()) $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "GROUPING SETS should be accepted via auto-rewrite, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_rollup_accepted_via_rewrite() {
    // ROLLUP is auto-rewritten to UNION ALL of plain GROUP BY queries.
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE rollup_src (id SERIAL PRIMARY KEY, dept TEXT, region TEXT, amount NUMERIC)",
    )
    .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('rollup_st', \
             $$ SELECT dept, region, SUM(amount) FROM rollup_src \
             GROUP BY ROLLUP (dept, region) $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "ROLLUP should be accepted via auto-rewrite, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_cube_accepted_via_rewrite() {
    // CUBE is auto-rewritten to UNION ALL of plain GROUP BY queries.
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE cube_src (id SERIAL PRIMARY KEY, dept TEXT, region TEXT, amount NUMERIC)",
    )
    .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('cube_st', \
             $$ SELECT dept, region, SUM(amount) FROM cube_src \
             GROUP BY CUBE (dept, region) $$, '1m', 'FULL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "CUBE should be accepted via auto-rewrite, got: {:?}",
        result.err()
    );
}

#[tokio::test]
async fn test_grouping_sets_differential_mode_also_rewritten() {
    // GROUPING SETS are auto-rewritten to UNION ALL even in DIFFERENTIAL mode.
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE gsd_src (id SERIAL PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('gsd_st', \
             $$ SELECT dept, SUM(amount) FROM gsd_src \
             GROUP BY GROUPING SETS ((dept), ()) $$, '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "GROUPING SETS should be accepted via auto-rewrite in DIFFERENTIAL mode, got: {:?}",
        result.err()
    );
}

// ── TABLESAMPLE rejection ───────────────────────────────────────────────

#[tokio::test]
async fn test_tablesample_bernoulli_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ts_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO ts_src SELECT generate_series(1,100), generate_series(1,100)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('ts_st', \
             $$ SELECT id, val FROM ts_src TABLESAMPLE BERNOULLI(10) $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "TABLESAMPLE should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("TABLESAMPLE"),
        "Error should mention TABLESAMPLE, got: {err_msg}"
    );
    assert!(
        err_msg.contains("random()") || err_msg.contains("WHERE"),
        "Error should suggest random()/WHERE alternative, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_tablesample_system_rejected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE tss_src (id INT PRIMARY KEY, val INT)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('tss_st', \
             $$ SELECT id, val FROM tss_src TABLESAMPLE SYSTEM(50) $$, '1m', 'FULL')",
        )
        .await;
    assert!(result.is_err(), "TABLESAMPLE SYSTEM should be rejected");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("TABLESAMPLE"),
        "Error should mention TABLESAMPLE, got: {err_msg}"
    );
}

// ── Ordered-set aggregate now supported ──────────────────────────────────

#[tokio::test]
async fn test_percentile_cont_now_supported_in_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE pct_src (id SERIAL PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO pct_src (dept, amount) VALUES ('A', 100), ('A', 200), ('B', 300)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('pct_st', \
             $$ SELECT dept, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS median \
             FROM pct_src GROUP BY dept $$, '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        result.is_ok(),
        "PERCENTILE_CONT in DIFFERENTIAL mode should now be supported, got: {:?}",
        result.err()
    );
}

// ── Non-deterministic function handling ───────────────────────────────

#[tokio::test]
async fn test_volatile_function_rejected_in_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE nd_vol_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO nd_vol_src VALUES (1, 10), (2, 20)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('nd_vol_st', \
             $$ SELECT id, random() AS sample, val FROM nd_vol_src $$, '1m', 'DIFFERENTIAL')",
        )
        .await;

    assert!(
        result.is_err(),
        "VOLATILE functions in DIFFERENTIAL mode should be rejected"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("volatile"),
        "Error should mention volatility, got: {err_msg}"
    );
    assert!(
        err_msg.contains("FULL") || err_msg.contains("AUTO"),
        "Error should include a safe refresh-mode remediation, got: {err_msg}"
    );
}

#[tokio::test]
async fn test_immutable_function_allowed_in_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE nd_imm_src (id INT PRIMARY KEY, name TEXT, qty INT)")
        .await;
    db.execute("INSERT INTO nd_imm_src VALUES (1, 'Alpha', 2), (2, 'Beta', 3)")
        .await;

    let defining_query =
        "SELECT id, lower(name) AS normalized_name, qty * 2 AS doubled_qty FROM nd_imm_src";

    db.create_st("nd_imm_st", defining_query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("public.nd_imm_st", defining_query)
        .await;

    db.execute("UPDATE nd_imm_src SET name = 'GAMMA', qty = 4 WHERE id = 2")
        .await;
    db.refresh_st("nd_imm_st").await;

    db.assert_st_matches_query("public.nd_imm_st", defining_query)
        .await;
}

#[tokio::test]
async fn test_stable_function_falls_back_to_full_in_auto_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE nd_stable_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO nd_stable_src VALUES (1, 10), (2, 20)")
        .await;

    db.try_execute_with_notices(
        "SELECT pgtrickle.create_stream_table('nd_stable_st', \
             $$ SELECT id, CURRENT_TIMESTAMP AS created_at, val FROM nd_stable_src $$, \
             '1m', 'AUTO')",
    )
    .await
    .expect("stable-function create_stream_table call should succeed");

    let (_, mode, _, _) = db.pgt_status("nd_stable_st").await;
    assert_eq!(mode, "FULL");
    let count = db.count("public.nd_stable_st").await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn test_nested_volatile_where_expression_rejected_in_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE nd_where_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO nd_where_src VALUES (1, 5), (2, 15), (3, 25)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('nd_where_st', \
             $$ SELECT id, val FROM nd_where_src \
                WHERE CASE WHEN random() > 0.5 THEN true ELSE val > 10 END $$, \
             '1m', 'DIFFERENTIAL')",
        )
        .await;

    assert!(
        result.is_err(),
        "Nested VOLATILE expressions in WHERE should be rejected in DIFFERENTIAL mode"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("volatile"),
        "Error should mention volatility, got: {err_msg}"
    );
}

// ── Volatile operator detection (G7.2) ──────────────────────────────────

/// Verify that a custom volatile operator is detected and rejected in
/// DIFFERENTIAL mode (Gap G7.2).
#[tokio::test]
async fn test_volatile_operator_rejected_in_differential() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a volatile function and a custom operator that uses it.
    db.execute(
        "CREATE FUNCTION volatile_add(int, int) RETURNS int AS $$ \
         SELECT ($1 + $2 + (random() * 0)::int) $$ LANGUAGE SQL VOLATILE",
    )
    .await;
    db.execute("CREATE OPERATOR |+| (LEFTARG = int, RIGHTARG = int, FUNCTION = volatile_add)")
        .await;

    db.execute("CREATE TABLE volop_src (id INT PRIMARY KEY, a INT, b INT)")
        .await;
    db.execute("INSERT INTO volop_src VALUES (1, 10, 20), (2, 30, 40)")
        .await;

    // DIFFERENTIAL mode should reject the volatile operator.
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table('volop_st', \
             $$ SELECT id, a |+| b AS result FROM volop_src $$, '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        result.is_err(),
        "Volatile operator in DIFFERENTIAL mode should be rejected"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("volatile"),
        "Error should mention volatile, got: {err_msg}"
    );
}

/// Verify that a custom volatile operator works in FULL mode.
#[tokio::test]
async fn test_volatile_operator_allowed_in_full_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE FUNCTION volatile_mul(int, int) RETURNS int AS $$ \
         SELECT ($1 * $2 + (random() * 0)::int) $$ LANGUAGE SQL VOLATILE",
    )
    .await;
    db.execute("CREATE OPERATOR |*| (LEFTARG = int, RIGHTARG = int, FUNCTION = volatile_mul)")
        .await;

    db.execute("CREATE TABLE volop2_src (id INT PRIMARY KEY, a INT, b INT)")
        .await;
    db.execute("INSERT INTO volop2_src VALUES (1, 5, 3), (2, 7, 4)")
        .await;

    // FULL mode should allow volatile operators.
    db.create_st(
        "volop2_st",
        "SELECT id, a |*| b AS result FROM volop2_src",
        "1m",
        "FULL",
    )
    .await;

    let count = db.count("public.volop2_st").await;
    assert_eq!(count, 2);
}

// ── Error Recovery: resume, suspended status ───────────────────────────

/// D2 — resume_stream_table() on a SUSPENDED ST → back to ACTIVE.
#[tokio::test]
async fn test_resume_stream_table_clears_suspended_status() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE err_resume_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO err_resume_src VALUES (1, 1)").await;

    db.create_st(
        "err_resume_st",
        "SELECT id, val FROM err_resume_src",
        "1m",
        "FULL",
    )
    .await;

    // Suspend via alter_stream_table
    db.alter_st("err_resume_st", "status => 'SUSPENDED'").await;

    let (status, _, _, _) = db.pgt_status("err_resume_st").await;
    assert_eq!(status, "SUSPENDED");

    // Resume
    db.execute("SELECT pgtrickle.resume_stream_table('err_resume_st')")
        .await;

    let (status_after, _, _, errors_after) = db.pgt_status("err_resume_st").await;
    assert_eq!(
        status_after, "ACTIVE",
        "resume_stream_table() should transition from SUSPENDED to ACTIVE"
    );
    assert_eq!(errors_after, 0, "consecutive_errors should be reset to 0");
}

/// D3 — Refresh is rejected for a SUSPENDED ST.
#[tokio::test]
async fn test_refresh_rejected_for_suspended_st() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE err_susp_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO err_susp_src VALUES (1, 1)").await;

    db.create_st(
        "err_susp_st",
        "SELECT id, val FROM err_susp_src",
        "1m",
        "FULL",
    )
    .await;

    // Suspend the ST
    db.alter_st("err_susp_st", "status => 'SUSPENDED'").await;

    // Refresh should fail while suspended
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('err_susp_st')")
        .await;
    assert!(
        result.is_err(),
        "refresh_stream_table() should be rejected for a SUSPENDED ST"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.to_lowercase().contains("suspended") || msg.to_lowercase().contains("resume"),
        "Error should mention suspension or resume, got: {msg}"
    );
}

/// D — resume_stream_table() on an unknown ST returns error.
#[tokio::test]
async fn test_resume_unknown_stream_table_errors() {
    let db = E2eDb::new().await.with_extension().await;

    let result = db
        .try_execute("SELECT pgtrickle.resume_stream_table('nonexistent_st')")
        .await;
    assert!(
        result.is_err(),
        "Resuming unknown ST should return an error"
    );
}

// ── D1 — Transaction abort leaves no orphans ──────────────────────────

/// D1 — A rolled-back transaction that called create_stream_table()
/// should leave no catalog entry, no storage table, and no CDC triggers.
#[tokio::test]
async fn test_create_st_transaction_abort_leaves_no_orphans() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE err_txn_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO err_txn_src VALUES (1, 1), (2, 2)")
        .await;

    // Execute create inside a transaction that we roll back.
    // We use separate statements because sqlx auto-commits each query.
    let pool = db.pool.clone();
    let mut tx = pool.begin().await.expect("begin txn");
    let create_result = sqlx::query(
        "SELECT pgtrickle.create_stream_table('err_txn_st', \
         $$ SELECT id, val FROM err_txn_src $$, '1m', 'FULL')",
    )
    .execute(&mut *tx)
    .await;

    // Whether create succeeded or not, we roll back the transaction.
    let _ = create_result;
    tx.rollback().await.expect("rollback txn");

    // After rollback: no storage table should exist.
    let exists = db.table_exists("public", "err_txn_st").await;
    assert!(
        !exists,
        "Storage table should not exist after transaction rollback"
    );

    // No catalog entry should remain.
    let cat_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'err_txn_st'",
        )
        .await;
    assert_eq!(
        cat_count, 0,
        "No catalog entry should remain after rollback"
    );

    // No CDC triggers referencing this ST should exist.
    let trigger_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_trigger t \
             JOIN pg_class c ON c.oid = t.tgrelid \
             WHERE c.relname = 'err_txn_src' \
             AND t.tgname LIKE '%pgtrickle%err_txn_st%'",
        )
        .await;
    assert_eq!(
        trigger_count, 0,
        "No CDC triggers should remain after rollback"
    );
}

// ── TEST-001 (v0.70.0): COR-002 LATERAL body volatility ───────────────────

/// COR-002: A LATERAL subquery containing `random()` must be rejected when
/// the volatile_function_policy is 'reject' (the default) and the refresh
/// mode is DIFFERENTIAL.
///
/// Before v0.70.0, only the left-hand child of the LATERAL node was scanned
/// for volatile expressions; the body SQL was silently skipped, allowing
/// non-deterministic queries to poison differential maintenance.
#[tokio::test]
async fn test_lateral_volatile_subquery_rejected_in_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_vol_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO lat_vol_src VALUES (1, 10), (2, 20)")
        .await;

    // Volatile expression inside a LATERAL subquery: random().
    // With volatile_function_policy = 'reject' (default) and DIFFERENTIAL
    // mode, create_stream_table must fail.
    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
             'lat_vol_sub_st', \
             $$ SELECT s.id, r.noise \
                FROM lat_vol_src s \
                LEFT JOIN LATERAL (SELECT random()::numeric AS noise) AS r ON true $$, \
             '1m', 'DIFFERENTIAL')",
        )
        .await;

    assert!(
        result.is_err(),
        "LATERAL subquery containing random() should be rejected in DIFFERENTIAL mode"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("volatile")
            || err_msg.to_lowercase().contains("unsupported"),
        "Error should mention volatile expressions, got: {err_msg}"
    );
}

/// COR-002: A LATERAL SRF containing a volatile function in its argument
/// must also be caught. `generate_series(1, (random()*10)::int)` uses random()
/// as the upper bound, making the set non-deterministic.
#[tokio::test]
async fn test_lateral_volatile_srf_argument_rejected_in_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_vol_srf_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO lat_vol_srf_src VALUES (1), (2)")
        .await;

    let result = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
             'lat_vol_srf_st', \
             $$ SELECT s.id, g.n \
                FROM lat_vol_srf_src s, \
                LATERAL generate_series(1, (random() * 5)::int) AS g(n) $$, \
             '1m', 'DIFFERENTIAL')",
        )
        .await;

    assert!(
        result.is_err(),
        "LATERAL SRF with volatile argument should be rejected in DIFFERENTIAL mode"
    );
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("volatile")
            || err_msg.to_lowercase().contains("unsupported"),
        "Error should mention volatile expressions, got: {err_msg}"
    );
}

/// COR-002: An immutable but uninspectable LATERAL SRF must use the safe
/// AUTO-to-FULL path rather than entering incremental maintenance.
#[tokio::test]
async fn test_lateral_immutable_srf_falls_back_to_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE lat_immut_src (id INT PRIMARY KEY, tags JSONB)")
        .await;
    db.execute(
        "INSERT INTO lat_immut_src VALUES \
         (1, '[\"rust\",\"pg\"]'), \
         (2, '[\"sql\"]')",
    )
    .await;

    // jsonb_array_elements_text is immutable, but its correlated body cannot
    // be independently parsed for incremental admission.
    db.create_st(
        "lat_immut_st",
        "SELECT s.id, e.tag \
         FROM lat_immut_src s, \
         jsonb_array_elements_text(s.tags) AS e(tag)",
        "1m",
        "AUTO",
    )
    .await;

    let (status, mode, populated, errors) = db.pgt_status("lat_immut_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated, "Stream table should be populated after creation");
    assert_eq!(errors, 0);

    // 2 tags for id=1 + 1 tag for id=2 = 3 rows.
    assert_eq!(db.count("public.lat_immut_st").await, 3);
}
