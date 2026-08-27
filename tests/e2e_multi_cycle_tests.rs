//! E2E tests for multi-cycle refresh correctness (F24: G8.2).
//!
//! Validates that multiple DML → refresh cycles produce correct cumulative
//! results for aggregate, join, and window queries, and that prepared
//! statement caching survives across cycles.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Multi-cycle aggregate
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multi_cycle_aggregate_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE mc_agg (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO mc_agg (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    let q = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt FROM mc_agg GROUP BY grp";
    db.create_st("mc_agg_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("mc_agg_st", q).await;

    // Cycle 1: inserts
    db.execute("INSERT INTO mc_agg (grp, val) VALUES ('a', 5), ('c', 30)")
        .await;
    db.refresh_st("mc_agg_st").await;
    db.assert_st_matches_query("mc_agg_st", q).await;

    // Cycle 2: updates
    db.execute("UPDATE mc_agg SET val = val * 2 WHERE grp = 'b'")
        .await;
    db.refresh_st("mc_agg_st").await;
    db.assert_st_matches_query("mc_agg_st", q).await;

    // Cycle 3: deletes
    db.execute("DELETE FROM mc_agg WHERE grp = 'c'").await;
    db.refresh_st("mc_agg_st").await;
    db.assert_st_matches_query("mc_agg_st", q).await;

    // Cycle 4: mixed
    db.execute("INSERT INTO mc_agg (grp, val) VALUES ('a', 100)")
        .await;
    db.execute("UPDATE mc_agg SET grp = 'b' WHERE grp = 'a' AND val = 5")
        .await;
    db.execute("DELETE FROM mc_agg WHERE grp = 'b' AND val = 40")
        .await;
    db.refresh_st("mc_agg_st").await;
    db.assert_st_matches_query("mc_agg_st", q).await;

    // Cycle 5: no changes (idempotent refresh)
    db.refresh_st("mc_agg_st").await;
    db.assert_st_matches_query("mc_agg_st", q).await;
}

#[tokio::test]
async fn test_multi_cycle_avg_algebraic() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE mc_avg (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC)")
        .await;
    db.execute("INSERT INTO mc_avg (grp, val) VALUES ('a', 10), ('a', 20), ('b', 100)")
        .await;

    let q = "SELECT grp, AVG(val) AS avg_val FROM mc_avg GROUP BY grp";
    db.create_st("mc_avg_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("mc_avg_st", q).await;

    // Cycle 1: inserts shift the average
    db.execute("INSERT INTO mc_avg (grp, val) VALUES ('a', 30), ('b', 200)")
        .await;
    db.refresh_st("mc_avg_st").await;
    db.assert_st_matches_query("mc_avg_st", q).await;

    // Cycle 2: update changes values
    db.execute("UPDATE mc_avg SET val = 50 WHERE grp = 'a' AND val = 10")
        .await;
    db.refresh_st("mc_avg_st").await;
    db.assert_st_matches_query("mc_avg_st", q).await;

    // Cycle 3: delete reduces group size
    db.execute("DELETE FROM mc_avg WHERE grp = 'a' AND val = 20")
        .await;
    db.refresh_st("mc_avg_st").await;
    db.assert_st_matches_query("mc_avg_st", q).await;

    // Cycle 4: mixed operations — insert + delete in one cycle
    db.execute("INSERT INTO mc_avg (grp, val) VALUES ('a', 5), ('c', 42)")
        .await;
    db.execute("DELETE FROM mc_avg WHERE grp = 'b' AND val = 100")
        .await;
    db.refresh_st("mc_avg_st").await;
    db.assert_st_matches_query("mc_avg_st", q).await;

    // Cycle 5: no-op refresh
    db.refresh_st("mc_avg_st").await;
    db.assert_st_matches_query("mc_avg_st", q).await;
}

#[tokio::test]
async fn test_multi_cycle_stddev_algebraic() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE mc_sd (id SERIAL PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO mc_sd (dept, amount) VALUES \
         ('eng', 100), ('eng', 200), ('eng', 300), \
         ('sales', 50), ('sales', 150)",
    )
    .await;

    let q = "SELECT dept, ROUND(ROUND(STDDEV_POP(amount), 4), 4) AS sd, ROUND(ROUND(VAR_POP(amount), 4), 4) AS vp FROM mc_sd GROUP BY dept";
    db.create_st("mc_sd_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("mc_sd_st", q).await;

    // Cycle 1: insert widens distribution
    db.execute("INSERT INTO mc_sd (dept, amount) VALUES ('eng', 1000)")
        .await;
    db.refresh_st("mc_sd_st").await;
    db.assert_st_matches_query("mc_sd_st", q).await;

    // Cycle 2: delete narrows it
    db.execute("DELETE FROM mc_sd WHERE dept = 'eng' AND amount = 1000")
        .await;
    db.refresh_st("mc_sd_st").await;
    db.assert_st_matches_query("mc_sd_st", q).await;

    // Cycle 3: update shifts values
    db.execute("UPDATE mc_sd SET amount = 250 WHERE dept = 'sales' AND amount = 50")
        .await;
    db.refresh_st("mc_sd_st").await;
    db.assert_st_matches_query("mc_sd_st", q).await;

    // Cycle 4: mixed — add new group + modify existing
    db.execute("INSERT INTO mc_sd (dept, amount) VALUES ('hr', 80), ('hr', 120)")
        .await;
    db.execute("DELETE FROM mc_sd WHERE dept = 'eng' AND amount = 100")
        .await;
    db.refresh_st("mc_sd_st").await;
    db.assert_st_matches_query("mc_sd_st", q).await;

    // Cycle 5: no-op refresh
    db.refresh_st("mc_sd_st").await;
    db.assert_st_matches_query("mc_sd_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-cycle JOIN
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multi_cycle_join_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE mc_left (id SERIAL PRIMARY KEY, key INT, lval TEXT)")
        .await;
    db.execute("CREATE TABLE mc_right (id SERIAL PRIMARY KEY, key INT, rval TEXT)")
        .await;
    db.execute("INSERT INTO mc_left (key, lval) VALUES (1, 'a'), (2, 'b')")
        .await;
    db.execute("INSERT INTO mc_right (key, rval) VALUES (1, 'x'), (3, 'z')")
        .await;

    let q = "SELECT l.key, l.lval, r.rval \
             FROM mc_left l JOIN mc_right r ON l.key = r.key";
    db.create_st("mc_join_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("mc_join_st", q).await;

    // Cycle 1
    db.execute("INSERT INTO mc_right (key, rval) VALUES (2, 'y')")
        .await;
    db.refresh_st("mc_join_st").await;
    db.assert_st_matches_query("mc_join_st", q).await;

    // Cycle 2
    db.execute("UPDATE mc_left SET key = 3 WHERE lval = 'a'")
        .await;
    db.refresh_st("mc_join_st").await;
    db.assert_st_matches_query("mc_join_st", q).await;

    // Cycle 3
    db.execute("DELETE FROM mc_right WHERE key = 2").await;
    db.refresh_st("mc_join_st").await;
    db.assert_st_matches_query("mc_join_st", q).await;

    // Cycle 4
    db.execute("INSERT INTO mc_left (key, lval) VALUES (3, 'c')")
        .await;
    db.execute("INSERT INTO mc_right (key, rval) VALUES (3, 'w')")
        .await;
    db.refresh_st("mc_join_st").await;
    db.assert_st_matches_query("mc_join_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-cycle WINDOW
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multi_cycle_window_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE mc_win (id SERIAL PRIMARY KEY, dept TEXT, salary INT)")
        .await;
    db.execute(
        "INSERT INTO mc_win (dept, salary) VALUES \
         ('eng', 100), ('eng', 200), ('sales', 150)",
    )
    .await;

    let q = "SELECT dept, salary, \
             ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn \
             FROM mc_win";
    db.create_st("mc_win_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("mc_win_st", q).await;

    for i in 0..5 {
        db.execute(&format!(
            "INSERT INTO mc_win (dept, salary) VALUES ('eng', {})",
            300 + i * 10
        ))
        .await;
        db.refresh_st("mc_win_st").await;
        db.assert_st_matches_query("mc_win_st", q).await;
    }

    // Delete three rows across cycles
    db.execute("DELETE FROM mc_win WHERE salary = 100").await;
    db.refresh_st("mc_win_st").await;
    db.assert_st_matches_query("mc_win_st", q).await;

    db.execute("DELETE FROM mc_win WHERE salary = 200").await;
    db.refresh_st("mc_win_st").await;
    db.assert_st_matches_query("mc_win_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-cycle with prepared statements (cache survival)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multi_cycle_prepared_statement_cache() {
    let db = E2eDb::new().await.with_extension().await;
    // Ensure prepared statements are on
    db.execute("SET pg_trickle.use_prepared_statements = on")
        .await;
    db.execute("CREATE TABLE mc_prep (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    // Insert multiple groups to prevent the differential refresh from falling back
    // to a FULL refresh due to the "aggregate saturation threshold" (where total_changes >= group_count).
    // A FULL refresh bypasses the MERGE path entirely, so prepared statements would never be used.
    db.execute(
        "INSERT INTO mc_prep (grp, val) VALUES ('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5)",
    )
    .await;

    let q = "SELECT grp, SUM(val) AS total FROM mc_prep GROUP BY grp";
    db.create_st("mc_prep_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("mc_prep_st", q).await;

    // Run enough cycles to trigger generic plan (typically ~5+ executions)
    for i in 2..=8 {
        db.execute(&format!(
            "INSERT INTO mc_prep (grp, val) VALUES ('a', {})",
            i
        ))
        .await;
        db.refresh_st("mc_prep_st").await;
        db.assert_st_matches_query("mc_prep_st", q).await;
    }
}

// Requires shared_preload_libraries for CACHE_GENERATION shared memory.
#[cfg(not(feature = "light-e2e"))]
#[tokio::test]
async fn test_cache_invalidation_refreshes_without_prepared_statements() {
    let db = E2eDb::new().await.with_extension().await;

    let (client, connection) =
        tokio_postgres::connect(db.connection_string(), tokio_postgres::NoTls)
            .await
            .expect("Failed to open dedicated test session");
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            panic!("Dedicated test session failed: {error}");
        }
    });

    client
        .batch_execute(
            "SET pg_trickle.use_prepared_statements = on;
             -- Disable the aggregate fast-path so this GROUP BY+SUM query uses the
             -- regular differential refresh path. SQL PREPARE/EXECUTE is disabled
             -- while refresh SQL runs under the stream owner.
             SET pg_trickle.aggregate_fast_path = off;
             CREATE TABLE mc_prep_invalidate (id SERIAL PRIMARY KEY, grp TEXT, val INT);
             -- Insert multiple groups to avoid the aggregate saturation threshold
             -- forcing a fall back to FULL refresh, which skirts the MERGE path.
             INSERT INTO mc_prep_invalidate (grp, val) VALUES ('a', 1), ('b', 2), ('c', 3), ('d', 4), ('e', 5);",
        )
        .await
        .expect("Failed to set up prepared-statement invalidation test");

    let q = "SELECT grp, SUM(val) AS total FROM mc_prep_invalidate GROUP BY grp";
    client
        .execute(
            "SELECT pgtrickle.create_stream_table($1, $2, schedule => '1m', refresh_mode => 'DIFFERENTIAL')",
            &[&"mc_prep_invalidate_st", &q],
        )
        .await
        .expect("Failed to create stream table");

    client
        .batch_execute(
            "INSERT INTO mc_prep_invalidate (grp, val) VALUES ('a', 2);
             SELECT pgtrickle.refresh_stream_table('mc_prep_invalidate_st');
             INSERT INTO mc_prep_invalidate (grp, val) VALUES ('a', 4);
             SELECT pgtrickle.refresh_stream_table('mc_prep_invalidate_st');",
        )
        .await
        .expect("Failed to warm differential refresh path");

    client
        .batch_execute(
            "SELECT pgtrickle.alter_stream_table('mc_prep_invalidate_st', schedule => '2m');
             INSERT INTO mc_prep_invalidate (grp, val) VALUES ('a', 3);
             SELECT pgtrickle.refresh_stream_table('mc_prep_invalidate_st');",
        )
        .await
        .expect("Failed to invalidate cache and refresh stream table");

    let st_total: i64 = client
        .query_one(
            "SELECT total FROM mc_prep_invalidate_st WHERE grp = 'a'",
            &[],
        )
        .await
        .expect("Failed to query refreshed stream table")
        .get(0);
    assert_eq!(
        st_total, 10,
        "Stream table should reflect the post-invalidation refresh"
    );

    let prepared_count: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_prepared_statements WHERE name LIKE '__pgt_merge_%'",
            &[],
        )
        .await
        .expect("Failed to inspect prepared statements")
        .get(0);
    assert_eq!(
        prepared_count, 0,
        "Refresh should not leave prepared MERGE statements, found {}",
        prepared_count
    );

    drop(client);
    connection_task
        .await
        .expect("Dedicated session task failed");
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-cycle: group elimination and revival
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_multi_cycle_group_elimination_revival() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE mc_grp (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO mc_grp (grp, val) VALUES ('a', 10), ('b', 20)")
        .await;

    let q = "SELECT grp, SUM(val) AS total FROM mc_grp GROUP BY grp";
    db.create_st("mc_grp_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("mc_grp_st", q).await;

    // Eliminate group 'a'
    db.execute("DELETE FROM mc_grp WHERE grp = 'a'").await;
    db.refresh_st("mc_grp_st").await;
    db.assert_st_matches_query("mc_grp_st", q).await;

    // Revive group 'a'
    db.execute("INSERT INTO mc_grp (grp, val) VALUES ('a', 50)")
        .await;
    db.refresh_st("mc_grp_st").await;
    db.assert_st_matches_query("mc_grp_st", q).await;

    // Eliminate again and add new group
    db.execute("DELETE FROM mc_grp WHERE grp = 'a'").await;
    db.execute("INSERT INTO mc_grp (grp, val) VALUES ('c', 99)")
        .await;
    db.refresh_st("mc_grp_st").await;
    db.assert_st_matches_query("mc_grp_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// EC-16: Function body change detection via pg_proc hash polling
// ═══════════════════════════════════════════════════════════════════════

/// EC-16: Replacing a function used in a stream table's defining query
/// is detected by the DDL event trigger which marks `needs_reinit = true`.
/// The next manual refresh automatically performs a FULL reinitialization
/// and produces correct results using the new function logic.
#[tokio::test]
async fn test_ec16_function_body_change_marks_reinit() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a helper function: doubles the value
    db.execute(
        "CREATE FUNCTION ec16_calc(v INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$ SELECT v * 2 $$",
    )
    .await;

    db.execute("CREATE TABLE ec16_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO ec16_src VALUES (1, 10), (2, 20)")
        .await;

    db.create_st(
        "ec16_fn_st",
        "SELECT id, ec16_calc(val) AS computed FROM ec16_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial populate should use v*2
    let sum: i64 = db
        .query_scalar("SELECT SUM(computed) FROM public.ec16_fn_st")
        .await;
    assert_eq!(sum, 60, "Initial: 10*2 + 20*2 = 60");

    // First differential refresh — establishes function hash baseline
    db.execute("INSERT INTO ec16_src VALUES (3, 5)").await;
    db.refresh_st("ec16_fn_st").await;

    let sum2: i64 = db
        .query_scalar("SELECT SUM(computed) FROM public.ec16_fn_st")
        .await;
    assert_eq!(sum2, 70, "After insert: 60 + 5*2 = 70");

    // Verify needs_reinit is false before function change
    let reinit_before: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ec16_fn_st'",
        )
        .await;
    assert!(
        !reinit_before,
        "needs_reinit should be false before function change"
    );

    // Replace the function: now triples instead of doubling.
    // The DDL event trigger fires and sets needs_reinit = true.
    db.execute(
        "CREATE OR REPLACE FUNCTION ec16_calc(v INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$ SELECT v * 3 $$",
    )
    .await;

    // DDL hook should have set needs_reinit = true
    let reinit_after_ddl: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ec16_fn_st'",
        )
        .await;
    assert!(
        reinit_after_ddl,
        "needs_reinit should be true after CREATE OR REPLACE FUNCTION (DDL hook)"
    );

    // Insert data + refresh — should automatically perform a FULL reinit
    // because needs_reinit is set, then clear the flag.
    db.execute("INSERT INTO ec16_src VALUES (4, 1)").await;
    db.refresh_st("ec16_fn_st").await;

    // After full reinit with new function (v*3):
    // id=1: 10*3=30, id=2: 20*3=60, id=3: 5*3=15, id=4: 1*3=3
    let sum3: i64 = db
        .query_scalar("SELECT SUM(computed) FROM public.ec16_fn_st")
        .await;
    assert_eq!(
        sum3, 108,
        "After reinit with new function: 30+60+15+3 = 108"
    );

    // needs_reinit should be cleared after the successful FULL reinit
    let reinit_after_refresh: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ec16_fn_st'",
        )
        .await;
    assert!(
        !reinit_after_refresh,
        "needs_reinit should be false after successful FULL reinitialization"
    );
}

/// EC-16: After function change, a refresh automatically performs a FULL
/// reinitialization and produces correct results using the new function logic.
/// Verifies the complete recovery flow including data correctness.
#[tokio::test]
async fn test_ec16_function_change_full_refresh_recovery() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a helper function: adds 100
    db.execute(
        "CREATE FUNCTION ec16r_calc(v INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$ SELECT v + 100 $$",
    )
    .await;

    db.execute("CREATE TABLE ec16r_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO ec16r_src VALUES (1, 5), (2, 10)")
        .await;

    db.create_st(
        "ec16r_fn_st",
        "SELECT id, ec16r_calc(val) AS computed FROM ec16r_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial: 5+100=105, 10+100=110
    let sum_initial: i64 = db
        .query_scalar("SELECT SUM(computed) FROM public.ec16r_fn_st")
        .await;
    assert_eq!(sum_initial, 215, "Initial: (5+100) + (10+100) = 215");

    // Establish function hash baseline via a differential refresh
    db.execute("INSERT INTO ec16r_src VALUES (3, 20)").await;
    db.refresh_st("ec16r_fn_st").await;

    // Replace function: now adds 200 instead of 100.
    // The DDL event trigger sets needs_reinit = true.
    db.execute(
        "CREATE OR REPLACE FUNCTION ec16r_calc(v INT) RETURNS INT LANGUAGE SQL IMMUTABLE AS $$ SELECT v + 200 $$",
    )
    .await;

    // Insert more data
    db.execute("INSERT INTO ec16r_src VALUES (4, 1)").await;

    // The next refresh detects needs_reinit and performs a FULL reinit
    // automatically, using the new function logic.
    db.refresh_st("ec16r_fn_st").await;

    // After full reinit with new function (v+200):
    // id=1: 5+200=205, id=2: 10+200=210, id=3: 20+200=220, id=4: 1+200=201
    let sum_after: i64 = db
        .query_scalar("SELECT SUM(computed) FROM public.ec16r_fn_st")
        .await;
    assert_eq!(
        sum_after, 836,
        "After full reinit with new function: 205+210+220+201 = 836"
    );

    // needs_reinit should be cleared after the successful reinit
    let reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ec16r_fn_st'",
        )
        .await;
    assert!(
        !reinit,
        "needs_reinit should be cleared after successful FULL reinitialization"
    );

    // The stream table should still be in DIFFERENTIAL mode (the reinit
    // was transparent — it doesn't change the configured refresh mode)
    let mode: String = db
        .query_scalar(
            "SELECT refresh_mode FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ec16r_fn_st'",
        )
        .await;
    assert_eq!(
        mode, "DIFFERENTIAL",
        "Refresh mode should remain DIFFERENTIAL after automatic reinit"
    );
}

/// EC-16: A stream table whose defining query uses no user-defined functions
/// should not be affected by the hash polling mechanism.
#[tokio::test]
async fn test_ec16_no_functions_unaffected() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ec16n_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO ec16n_src VALUES (1, 10)").await;

    db.create_st(
        "ec16n_st",
        "SELECT id, val FROM ec16n_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Multiple refresh cycles — should never trigger needs_reinit
    db.execute("INSERT INTO ec16n_src VALUES (2, 20)").await;
    db.refresh_st("ec16n_st").await;

    db.execute("INSERT INTO ec16n_src VALUES (3, 30)").await;
    db.refresh_st("ec16n_st").await;

    let reinit: bool = db
        .query_scalar(
            "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ec16n_st'",
        )
        .await;
    assert!(
        !reinit,
        "Stream table without functions should never have needs_reinit set by hash polling"
    );
    assert_eq!(db.count("public.ec16n_st").await, 3);
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 2 (TESTING_GAPS_2): Join multi-cycle correctness
// ═══════════════════════════════════════════════════════════════════════

/// LEFT JOIN multi-cycle: INSERT, unmatched-right INSERT, join-key UPDATE,
/// DELETE, restore — all modes exercised over 5 cycles.
#[tokio::test]
async fn test_multi_cycle_left_join_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE lj_left (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE lj_right (id INT PRIMARY KEY, lft_id INT, rval TEXT)")
        .await;
    db.execute("INSERT INTO lj_left VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .await;
    db.execute("INSERT INTO lj_right VALUES (10, 1, 'rx'), (11, 2, 'ry')")
        .await;

    let q = "SELECT l.id, l.val, r.rval \
             FROM lj_left l LEFT JOIN lj_right r ON r.lft_id = l.id \
             ORDER BY l.id";
    db.create_st("lj_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("lj_st", q).await;

    // Cycle 1: INSERT matched rows on both sides
    db.execute("INSERT INTO lj_left VALUES (4, 'delta')").await;
    db.execute("INSERT INTO lj_right VALUES (12, 4, 'rz')")
        .await;
    db.refresh_st("lj_st").await;
    db.assert_st_matches_query("lj_st", q).await;

    // Cycle 2: INSERT unmatched right (should not affect result)
    db.execute("INSERT INTO lj_right VALUES (99, 999, 'orphan')")
        .await;
    db.refresh_st("lj_st").await;
    db.assert_st_matches_query("lj_st", q).await;

    // Cycle 3: UPDATE join key on left — loses old match, gains new
    db.execute("UPDATE lj_left SET id = 5 WHERE id = 3").await;
    db.refresh_st("lj_st").await;
    db.assert_st_matches_query("lj_st", q).await;

    // Cycle 4: DELETE from left — row disappears entirely
    db.execute("DELETE FROM lj_right WHERE lft_id = 1").await;
    db.execute("DELETE FROM lj_left WHERE id = 1").await;
    db.refresh_st("lj_st").await;
    db.assert_st_matches_query("lj_st", q).await;

    // Cycle 5: restore a match for the previously unmatched left row
    db.execute("INSERT INTO lj_right VALUES (20, 5, 'restored')")
        .await;
    db.refresh_st("lj_st").await;
    db.assert_st_matches_query("lj_st", q).await;
}

/// RIGHT JOIN multi-cycle: symmetric of the LEFT JOIN test.
#[tokio::test]
async fn test_multi_cycle_right_join_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE rj_left (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("CREATE TABLE rj_right (id INT PRIMARY KEY, lft_id INT, rval TEXT)")
        .await;
    db.execute("INSERT INTO rj_left VALUES (1, 'a'), (2, 'b')")
        .await;
    db.execute("INSERT INTO rj_right VALUES (10, 1, 'x'), (11, 3, 'z')")
        .await;

    let q = "SELECT l.id AS lid, l.val, r.id AS rid, r.rval \
             FROM rj_left l RIGHT JOIN rj_right r ON l.id = r.lft_id";
    db.create_st("rj_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("rj_st", q).await;

    // Cycle 1: new right row with a matching left
    db.execute("INSERT INTO rj_right VALUES (12, 2, 'y')").await;
    db.refresh_st("rj_st").await;
    db.assert_st_matches_query("rj_st", q).await;

    // Cycle 2: UPDATE right join-key to break match
    db.execute("UPDATE rj_right SET lft_id = 99 WHERE id = 10")
        .await;
    db.refresh_st("rj_st").await;
    db.assert_st_matches_query("rj_st", q).await;

    // Cycle 3: DELETE unmatched right row
    db.execute("DELETE FROM rj_right WHERE id = 11").await;
    db.refresh_st("rj_st").await;
    db.assert_st_matches_query("rj_st", q).await;

    // Cycle 4: INSERT left to match orphaned right
    db.execute("INSERT INTO rj_left VALUES (99, 'new')").await;
    db.refresh_st("rj_st").await;
    db.assert_st_matches_query("rj_st", q).await;

    // Cycle 5: bulk DELETE left — all rows become NULL-left
    db.execute("DELETE FROM rj_left").await;
    db.refresh_st("rj_st").await;
    db.assert_st_matches_query("rj_st", q).await;
}

/// FULL JOIN multi-cycle: rows appear on either or both sides.
#[tokio::test]
async fn test_multi_cycle_full_join_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE fj_a (k INT PRIMARY KEY, av TEXT)")
        .await;
    db.execute("CREATE TABLE fj_b (k INT PRIMARY KEY, bv TEXT)")
        .await;
    db.execute("INSERT INTO fj_a VALUES (1, 'a1'), (2, 'a2')")
        .await;
    db.execute("INSERT INTO fj_b VALUES (2, 'b2'), (3, 'b3')")
        .await;

    let q = "SELECT a.k AS ak, a.av, b.k AS bk, b.bv \
             FROM fj_a a FULL JOIN fj_b b ON a.k = b.k";
    db.create_st("fj_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("fj_st", q).await;

    // Cycle 1: add matched pair
    db.execute("INSERT INTO fj_a VALUES (3, 'a3')").await;
    db.refresh_st("fj_st").await;
    db.assert_st_matches_query("fj_st", q).await;

    // Cycle 2: insert unmatched on each side
    db.execute("INSERT INTO fj_a VALUES (4, 'a4')").await;
    db.execute("INSERT INTO fj_b VALUES (5, 'b5')").await;
    db.refresh_st("fj_st").await;
    db.assert_st_matches_query("fj_st", q).await;

    // Cycle 3: UPDATE to create new match (was previously unmatched)
    db.execute("INSERT INTO fj_b VALUES (4, 'b4')").await;
    db.refresh_st("fj_st").await;
    db.assert_st_matches_query("fj_st", q).await;

    // Cycle 4: DELETE from one side — partner becomes NULL-padded
    db.execute("DELETE FROM fj_a WHERE k = 3").await;
    db.refresh_st("fj_st").await;
    db.assert_st_matches_query("fj_st", q).await;

    // Cycle 5: DELETE from both sides of a matched pair
    db.execute("DELETE FROM fj_a WHERE k = 2").await;
    db.execute("DELETE FROM fj_b WHERE k = 2").await;
    db.refresh_st("fj_st").await;
    db.assert_st_matches_query("fj_st", q).await;
}

/// Focused test: UPDATE on the join key severs old match and forges new one.
#[tokio::test]
async fn test_multi_cycle_join_key_update() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE jku_parent (id INT PRIMARY KEY, pval TEXT)")
        .await;
    db.execute("CREATE TABLE jku_child (id INT PRIMARY KEY, parent_id INT, cval TEXT)")
        .await;
    db.execute("INSERT INTO jku_parent VALUES (1, 'p1'), (2, 'p2'), (3, 'p3')")
        .await;
    db.execute("INSERT INTO jku_child VALUES (10, 1, 'c1'), (11, 2, 'c2')")
        .await;

    let q = "SELECT p.id, p.pval, c.cval \
             FROM jku_parent p JOIN jku_child c ON c.parent_id = p.id";
    db.create_st("jku_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("jku_st", q).await;

    // Cycle 1: change child join key → moves from p1 to p2
    db.execute("UPDATE jku_child SET parent_id = 2 WHERE id = 10")
        .await;
    db.refresh_st("jku_st").await;
    db.assert_st_matches_query("jku_st", q).await;

    // Cycle 2: change child join key to previously unmatched parent
    db.execute("UPDATE jku_child SET parent_id = 3 WHERE id = 11")
        .await;
    db.refresh_st("jku_st").await;
    db.assert_st_matches_query("jku_st", q).await;

    // Cycle 3: change parent PK — both sides updated
    db.execute("UPDATE jku_parent SET id = 4 WHERE id = 3")
        .await;
    db.execute("UPDATE jku_child SET parent_id = 4 WHERE parent_id = 3")
        .await;
    db.refresh_st("jku_st").await;
    db.assert_st_matches_query("jku_st", q).await;

    // Cycle 4: no changes (idempotent)
    db.refresh_st("jku_st").await;
    db.assert_st_matches_query("jku_st", q).await;
}

/// Concurrent DML on both sides of a join within the same cycle.
#[tokio::test]
async fn test_multi_cycle_join_both_sides_changed() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE jbs_orders (oid INT PRIMARY KEY, status TEXT)")
        .await;
    db.execute("CREATE TABLE jbs_items (iid INT PRIMARY KEY, oid INT, qty INT)")
        .await;
    db.execute("INSERT INTO jbs_orders VALUES (1, 'open'), (2, 'closed'), (3, 'open')")
        .await;
    db.execute("INSERT INTO jbs_items VALUES (101, 1, 5), (102, 1, 3), (103, 2, 7)")
        .await;

    let q = "SELECT o.oid, o.status, SUM(i.qty) AS total_qty \
             FROM jbs_orders o JOIN jbs_items i ON i.oid = o.oid \
             GROUP BY o.oid, o.status";
    db.create_st("jbs_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("jbs_st", q).await;

    // Cycle 1: changes on both sides simultaneously
    db.execute("INSERT INTO jbs_orders VALUES (4, 'open')")
        .await;
    db.execute("INSERT INTO jbs_items VALUES (104, 3, 2), (105, 4, 9)")
        .await;
    db.execute("UPDATE jbs_orders SET status = 'shipped' WHERE oid = 1")
        .await;
    db.execute("UPDATE jbs_items SET qty = 10 WHERE iid = 102")
        .await;
    db.refresh_st("jbs_st").await;
    db.assert_st_matches_query("jbs_st", q).await;

    // Cycle 2: delete order → all its items become orphaned (removed from join)
    db.execute("DELETE FROM jbs_items WHERE oid = 2").await;
    db.execute("DELETE FROM jbs_orders WHERE oid = 2").await;
    db.refresh_st("jbs_st").await;
    db.assert_st_matches_query("jbs_st", q).await;

    // Cycle 3: move items to a different order
    db.execute("UPDATE jbs_items SET oid = 4 WHERE oid = 3")
        .await;
    db.refresh_st("jbs_st").await;
    db.assert_st_matches_query("jbs_st", q).await;
}

/// 4-table join chain: t1 → t2 → t3 → t4. DML at each position.
#[tokio::test]
async fn test_deep_join_chain_4_tables_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE jc_t1 (id INT PRIMARY KEY, v1 TEXT)")
        .await;
    db.execute("CREATE TABLE jc_t2 (id INT PRIMARY KEY, t1_id INT, v2 TEXT)")
        .await;
    db.execute("CREATE TABLE jc_t3 (id INT PRIMARY KEY, t2_id INT, v3 TEXT)")
        .await;
    db.execute("CREATE TABLE jc_t4 (id INT PRIMARY KEY, t3_id INT, v4 TEXT)")
        .await;
    db.execute("INSERT INTO jc_t1 VALUES (1,'a'),(2,'b')").await;
    db.execute("INSERT INTO jc_t2 VALUES (10,1,'c'),(11,2,'d')")
        .await;
    db.execute("INSERT INTO jc_t3 VALUES (100,10,'e'),(101,11,'f')")
        .await;
    db.execute("INSERT INTO jc_t4 VALUES (1000,100,'g')").await;

    let q = "SELECT t1.v1, t2.v2, t3.v3, t4.v4 \
             FROM jc_t1 t1 \
             JOIN jc_t2 t2 ON t2.t1_id = t1.id \
             JOIN jc_t3 t3 ON t3.t2_id = t2.id \
             JOIN jc_t4 t4 ON t4.t3_id = t3.id";
    db.create_st("jc_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("jc_st", q).await;

    // Cycle 1: extend chain from the middle
    db.execute("INSERT INTO jc_t3 VALUES (102, 10, 'h')").await;
    db.execute("INSERT INTO jc_t4 VALUES (1001, 101, 'i')")
        .await;
    db.refresh_st("jc_st").await;
    db.assert_st_matches_query("jc_st", q).await;

    // Cycle 2: update a value in the middle of the chain
    db.execute("UPDATE jc_t2 SET v2 = 'D' WHERE id = 11").await;
    db.refresh_st("jc_st").await;
    db.assert_st_matches_query("jc_st", q).await;

    // Cycle 3: delete a root row — cascades through the chain
    db.execute("DELETE FROM jc_t4 WHERE t3_id IN (SELECT id FROM jc_t3 WHERE t2_id = 11)")
        .await;
    db.execute("DELETE FROM jc_t3 WHERE t2_id = 11").await;
    db.execute("DELETE FROM jc_t2 WHERE t1_id = 2").await;
    db.execute("DELETE FROM jc_t1 WHERE id = 2").await;
    db.refresh_st("jc_st").await;
    db.assert_st_matches_query("jc_st", q).await;

    // Cycle 4: add new leaf only (result still same — no new chain)
    db.execute("INSERT INTO jc_t4 VALUES (1002, 999, 'orphan')")
        .await;
    db.refresh_st("jc_st").await;
    db.assert_st_matches_query("jc_st", q).await;
}

/// NULL in join key: rows with NULL never match (SQL three-valued logic).
#[tokio::test]
async fn test_multi_cycle_join_null_key_transitions() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE nk_fact (id INT PRIMARY KEY, dim_id INT, fval TEXT)")
        .await;
    db.execute("CREATE TABLE nk_dim (id INT PRIMARY KEY, dval TEXT)")
        .await;
    db.execute("INSERT INTO nk_dim VALUES (1, 'd1'), (2, 'd2')")
        .await;
    db.execute("INSERT INTO nk_fact VALUES (10, 1, 'f1'), (11, NULL, 'fnull'), (12, 2, 'f2')")
        .await;

    // INNER JOIN — NULL dim_id rows must be absent from result
    let q = "SELECT f.id, f.fval, d.dval \
             FROM nk_fact f JOIN nk_dim d ON f.dim_id = d.id";
    db.create_st("nk_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("nk_st", q).await;

    // Cycle 1: set NULL key to a real value → row joins in
    db.execute("UPDATE nk_fact SET dim_id = 1 WHERE id = 11")
        .await;
    db.refresh_st("nk_st").await;
    db.assert_st_matches_query("nk_st", q).await;

    // Cycle 2: set real key back to NULL → row drops out
    db.execute("UPDATE nk_fact SET dim_id = NULL WHERE id = 11")
        .await;
    db.refresh_st("nk_st").await;
    db.assert_st_matches_query("nk_st", q).await;

    // Cycle 3: insert a row with NULL from the start
    db.execute("INSERT INTO nk_fact VALUES (13, NULL, 'fnull2')")
        .await;
    db.refresh_st("nk_st").await;
    db.assert_st_matches_query("nk_st", q).await;

    // Cycle 4: delete a matched row
    db.execute("DELETE FROM nk_fact WHERE id = 10").await;
    db.refresh_st("nk_st").await;
    db.assert_st_matches_query("nk_st", q).await;

    // Cycle 5: insert dim and update NULL facts to point at it
    db.execute("INSERT INTO nk_dim VALUES (3, 'd3')").await;
    db.execute("UPDATE nk_fact SET dim_id = 3 WHERE dim_id IS NULL")
        .await;
    db.refresh_st("nk_st").await;
    db.assert_st_matches_query("nk_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Phase 4.4 (TESTING_GAPS_2): Multi-cycle lateral join + recursive CTE
// ═══════════════════════════════════════════════════════════════════════

/// LATERAL JOIN multi-cycle: parent + correlated subquery, 5 DML cycles.
#[tokio::test]
async fn test_multi_cycle_lateral_join_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE lat_parent (pid INT PRIMARY KEY, pname TEXT)")
        .await;
    db.execute("CREATE TABLE lat_score (sid INT PRIMARY KEY, pid INT, score INT)")
        .await;
    db.execute("INSERT INTO lat_parent VALUES (1,'alice'),(2,'bob'),(3,'carol')")
        .await;
    db.execute("INSERT INTO lat_score VALUES (10,1,80),(11,1,90),(12,2,70),(13,3,95),(14,3,85)")
        .await;

    // LATERAL correlated subquery: best score per parent
    let q = "SELECT p.pid, p.pname, top.best \
             FROM lat_parent p \
             JOIN LATERAL (\
               SELECT MAX(s.score) AS best FROM lat_score s WHERE s.pid = p.pid\
             ) top ON true";
    db.create_st("lat_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("lat_st", q).await;

    // Cycle 1: add new score that becomes the new best
    db.execute("INSERT INTO lat_score VALUES (15, 1, 99)").await;
    db.refresh_st("lat_st").await;
    db.assert_st_matches_query("lat_st", q).await;

    // Cycle 2: delete the current best — score reverts
    db.execute("DELETE FROM lat_score WHERE sid = 15").await;
    db.refresh_st("lat_st").await;
    db.assert_st_matches_query("lat_st", q).await;

    // Cycle 3: add new parent (no scores yet → best = NULL)
    db.execute("INSERT INTO lat_parent VALUES (4,'dave')").await;
    db.refresh_st("lat_st").await;
    db.assert_st_matches_query("lat_st", q).await;

    // Cycle 4: add scores for the new parent
    db.execute("INSERT INTO lat_score VALUES (16,4,60),(17,4,75)")
        .await;
    db.refresh_st("lat_st").await;
    db.assert_st_matches_query("lat_st", q).await;

    // Cycle 5: delete all scores for bob → best becomes NULL
    db.execute("DELETE FROM lat_score WHERE pid = 2").await;
    db.refresh_st("lat_st").await;
    db.assert_st_matches_query("lat_st", q).await;
}

/// Recursive CTE multi-cycle: org hierarchy — insert/delete nodes, depth changes.
#[tokio::test]
async fn test_multi_cycle_recursive_cte_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE rc_org (eid INT PRIMARY KEY, ename TEXT, manager_id INT)")
        .await;
    // Seed the hierarchy: CEO → VP → Mgr → Employee
    db.execute(
        "INSERT INTO rc_org VALUES \
         (1,'CEO',NULL),(2,'VP_Eng',1),(3,'VP_Sales',1),\
         (4,'Mgr_BE',2),(5,'Mgr_FE',2),(6,'Dev1',4),(7,'Dev2',4)",
    )
    .await;

    let q = "WITH RECURSIVE hierarchy AS (\
               SELECT eid, ename, manager_id, 0 AS depth \
               FROM rc_org WHERE manager_id IS NULL \
               UNION ALL \
               SELECT e.eid, e.ename, e.manager_id, h.depth + 1 \
               FROM rc_org e JOIN hierarchy h ON e.manager_id = h.eid\
             ) \
             SELECT eid, ename, depth FROM hierarchy";
    db.create_st("rc_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("rc_st", q).await;

    // Cycle 1: add a new leaf
    db.execute("INSERT INTO rc_org VALUES (8,'Dev3',5)").await;
    db.refresh_st("rc_st").await;
    db.assert_st_matches_query("rc_st", q).await;

    // Cycle 2: add a new manager (mid-tree node with children attached later)
    db.execute("INSERT INTO rc_org VALUES (9,'Mgr_Sales',3)")
        .await;
    db.refresh_st("rc_st").await;
    db.assert_st_matches_query("rc_st", q).await;

    // Cycle 3: attach leaf to new manager
    db.execute("INSERT INTO rc_org VALUES (10,'Sales1',9)")
        .await;
    db.refresh_st("rc_st").await;
    db.assert_st_matches_query("rc_st", q).await;

    // Cycle 4: update a name (no structural change)
    db.execute("UPDATE rc_org SET ename = 'CTO' WHERE eid = 2")
        .await;
    db.refresh_st("rc_st").await;
    db.assert_st_matches_query("rc_st", q).await;

    // Cycle 5: delete a leaf node
    db.execute("DELETE FROM rc_org WHERE eid = 7").await;
    db.refresh_st("rc_st").await;
    db.assert_st_matches_query("rc_st", q).await;
}
