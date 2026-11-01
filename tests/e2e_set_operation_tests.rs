//! E2E tests for INTERSECT / EXCEPT FULL-refresh correctness (F19: G2.4).
//!
//! Validates set operations (INTERSECT, INTERSECT ALL, EXCEPT, EXCEPT ALL).
//! INTERSECT differential maintenance is covered for its bounded direct-scan
//! shape. Other set-operation forms remain FULL-only. The REL-101 tests verify
//! durable private multiplicity state, privacy, and cleanup.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

async fn refresh_st_with_expected_action(db: &E2eDb, name: &str, expected_action: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let mut tx = db.pool.begin().await.expect("begin differential refresh");
        sqlx::query("SET LOCAL pg_trickle.refresh_strategy = 'differential'")
            .execute(&mut *tx)
            .await
            .expect("set differential refresh strategy");
        sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
            .bind(name)
            .execute(&mut *tx)
            .await
            .unwrap_or_else(|error| panic!("differential refresh of {name} failed: {error:?}"));
        let completed: Option<(String, bool)> = sqlx::query_as(
            "SELECT action, was_full_fallback FROM pgtrickle.pgt_refresh_history \
             WHERE xmin = pg_current_xact_id()::xid \
               AND initiated_by = 'MANUAL' AND status = 'COMPLETED' \
             ORDER BY refresh_id DESC LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await
        .expect("check differential refresh history");
        tx.commit().await.expect("commit differential refresh");
        if let Some((action, was_full_fallback)) = completed {
            assert_eq!(action, expected_action, "{name}: wrong refresh route");
            assert!(
                !was_full_fallback,
                "{name}: differential refresh fell back to FULL"
            );
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for manual refresh of {name}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn refresh_st_differential(db: &E2eDb, name: &str) {
    refresh_st_with_expected_action(db, name, "DIFFERENTIAL").await;
}

// ═══════════════════════════════════════════════════════════════════════
// INTERSECT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_intersect_basic_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO isect_a (val) VALUES (1), (2), (3)")
        .await;
    db.execute("INSERT INTO isect_b (val) VALUES (2), (3), (4)")
        .await;

    let q = "SELECT val FROM isect_a INTERSECT SELECT val FROM isect_b";
    db.create_st("isect_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("isect_st", q).await;

    // Add value to both → appears in intersection
    db.execute("INSERT INTO isect_a (val) VALUES (4)").await;
    db.refresh_st("isect_st").await;
    db.assert_st_matches_query("isect_st", q).await;

    // Remove shared value from one side
    db.execute("DELETE FROM isect_b WHERE val = 2").await;
    db.refresh_st("isect_st").await;
    db.assert_st_matches_query("isect_st", q).await;
}

#[tokio::test]
async fn test_intersect_differential_presence_transitions_and_private_counts() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("CREATE TABLE isect_diff_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_diff_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO isect_diff_a (val) VALUES (7), (7), (8)")
        .await;
    db.execute("INSERT INTO isect_diff_b (val) VALUES (7), (9)")
        .await;

    let query = "SELECT val FROM isect_diff_a INTERSECT SELECT val FROM isect_diff_b";
    db.create_st("isect_diff_st", query, "1m", "DIFFERENTIAL")
        .await;
    let requested_mode: String = db
        .query_scalar(
            "SELECT refresh_mode::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'isect_diff_st'",
        )
        .await;
    assert_eq!(requested_mode, "DIFFERENTIAL", "INTERSECT admission mode");
    let requested_mode: String = db
        .query_scalar(
            "SELECT requested_refresh_mode FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'isect_diff_st'",
        )
        .await;
    assert_eq!(requested_mode, "DIFFERENTIAL", "INTERSECT requested mode");
    db.assert_st_matches_query("isect_diff_st", query).await;
    db.refresh_st_with_retry("isect_diff_st").await;
    let (frontier, source_count): (Option<String>, i64) = sqlx::query_as(
        "SELECT s.frontier::text, count(d.source_relid) \
         FROM pgtrickle.pgt_stream_tables s \
         LEFT JOIN pgtrickle.pgt_dependencies d USING (pgt_id) \
         WHERE s.pgt_name = 'isect_diff_st' GROUP BY s.frontier",
    )
    .fetch_one(&db.pool)
    .await
    .expect("INTERSECT frontier and dependencies");
    assert!(
        source_count >= 2,
        "INTERSECT source dependencies: {source_count}"
    );
    assert!(
        frontier.as_deref().is_some_and(|value| value != "{}"),
        "INTERSECT full baseline did not establish a source frontier: {frontier:?}"
    );
    let pgt_id = setop_pgt_id(&db, "isect_diff_st").await;
    let state_rel = format!("pgtrickle.\"__pgt_setop_state_{pgt_id}\"");

    // A left-side multiplicity change updates private state, but does not
    // change the visible set while both counts remain positive.
    db.execute("INSERT INTO isect_diff_a (val) VALUES (7)")
        .await;
    refresh_st_differential(&db, "isect_diff_st").await;
    e2e::oracle::assert_effective_refresh_mode(&db, "isect_diff_st", "DIFFERENTIAL")
        .await
        .unwrap();
    db.assert_st_matches_query("isect_diff_st", query).await;
    let count_l: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_l FROM {state_rel} WHERE val = 7"
        ))
        .await;
    let count_r: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_r FROM {state_rel} WHERE val = 7"
        ))
        .await;
    assert_eq!((count_l, count_r), (3, 1));
    let public_count: i64 = db
        .query_scalar("SELECT count(*) FROM isect_diff_st WHERE val = 7")
        .await;
    assert_eq!(public_count, 1, "INTERSECT remains a set");

    // Crossing right count to zero removes the visible row but preserves the
    // left multiplicity for a later zero-to-positive transition.
    db.execute("DELETE FROM isect_diff_b WHERE val = 7").await;
    refresh_st_differential(&db, "isect_diff_st").await;
    e2e::oracle::assert_effective_refresh_mode(&db, "isect_diff_st", "DIFFERENTIAL")
        .await
        .unwrap();
    db.assert_st_matches_query("isect_diff_st", query).await;
    let hidden_count_l: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_l FROM {state_rel} WHERE val = 7"
        ))
        .await;
    let hidden_count_r: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_r FROM {state_rel} WHERE val = 7"
        ))
        .await;
    assert_eq!((hidden_count_l, hidden_count_r), (3, 0));
    let public_count: i64 = db
        .query_scalar("SELECT count(*) FROM isect_diff_st WHERE val = 7")
        .await;
    assert_eq!(public_count, 0);

    db.execute("INSERT INTO isect_diff_b (val) VALUES (7)")
        .await;
    refresh_st_differential(&db, "isect_diff_st").await;
    e2e::oracle::assert_effective_refresh_mode(&db, "isect_diff_st", "DIFFERENTIAL")
        .await
        .unwrap();
    db.assert_st_matches_query("isect_diff_st", query).await;
    let restored_count: i64 = db
        .query_scalar("SELECT count(*) FROM isect_diff_st WHERE val = 7")
        .await;
    assert_eq!(
        restored_count, 1,
        "zero-to-positive crossing reinserts once"
    );

    db.execute("SELECT pgtrickle.alter_stream_table('isect_diff_st', refresh_mode => 'FULL')")
        .await;
    db.refresh_st_with_retry("isect_diff_st").await;
    let full_history: (String, bool) = sqlx::query_as(
        "SELECT action, was_full_fallback FROM pgtrickle.pgt_refresh_history \
         WHERE initiated_by = 'MANUAL' ORDER BY refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("FULL refresh history");
    assert_eq!(full_history, ("FULL".to_string(), false));
    e2e::oracle::assert_effective_refresh_mode(&db, "isect_diff_st", "FULL")
        .await
        .unwrap();
    db.assert_st_matches_query("isect_diff_st", query).await;

    db.execute(
        "SELECT pgtrickle.alter_stream_table('isect_diff_st', refresh_mode => 'DIFFERENTIAL')",
    )
    .await;

    #[cfg(not(feature = "light-e2e"))]
    {
        db.pool.close().await;
        let restarted = tokio::process::Command::new("docker")
            .args(["restart", db.container_id()])
            .output()
            .await
            .expect("restart INTERSECT E2E container");
        assert!(
            restarted.status.success(),
            "INTERSECT container restart failed"
        );
        db.pool = db.reconnect_after_restart().await;
        db.assert_st_matches_query("isect_diff_st", query).await;
        let after_restart_count: i64 = db
            .query_scalar(&format!(
                "SELECT __pgt_count_l FROM {state_rel} WHERE val = 7"
            ))
            .await;
        assert_eq!(after_restart_count, 3, "restart preserves private state");
    }
    db.execute("INSERT INTO isect_diff_a (val) VALUES (7)")
        .await;
    refresh_st_differential(&db, "isect_diff_st").await;
    e2e::oracle::assert_effective_refresh_mode(&db, "isect_diff_st", "DIFFERENTIAL")
        .await
        .unwrap();
    db.assert_st_matches_query("isect_diff_st", query).await;
}

#[tokio::test]
async fn test_intersect_differential_empty_invisible_rows_simultaneous_changes_and_noop() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_empty_a (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_empty_b (id INT PRIMARY KEY, val INT)")
        .await;
    let query = "SELECT val FROM isect_empty_a INTERSECT SELECT val FROM isect_empty_b";
    db.create_st("isect_empty_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st_with_retry("isect_empty_st").await;
    db.assert_st_matches_query("isect_empty_st", query).await;
    let pgt_id = setop_pgt_id(&db, "isect_empty_st").await;
    let state_rel = format!("pgtrickle.\"__pgt_setop_state_{pgt_id}\"");

    let state_count_before_noop: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM {state_rel}"))
        .await;
    refresh_st_differential(&db, "isect_empty_st").await;
    db.assert_st_matches_query("isect_empty_st", query).await;
    let (noop_action, noop_delta): (String, i64) = sqlx::query_as(
        "SELECT action, delta_row_count FROM pgtrickle.pgt_refresh_history \
         WHERE initiated_by = 'MANUAL' ORDER BY refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("no-op differential history");
    assert_eq!(noop_action, "DIFFERENTIAL");
    assert_eq!(noop_delta, 0, "no-op refresh emits no rows");
    let state_count_after_noop: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM {state_rel}"))
        .await;
    assert_eq!(state_count_after_noop, state_count_before_noop);

    db.execute("INSERT INTO isect_empty_a VALUES (1, 1), (2, 1), (3, 2)")
        .await;
    refresh_st_differential(&db, "isect_empty_st").await;
    db.assert_st_matches_query("isect_empty_st", query).await;
    let invisible_count: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_l FROM {state_rel} WHERE val = 1 AND __pgt_count_r = 0"
        ))
        .await;
    assert_eq!(
        invisible_count, 2,
        "one empty branch keeps both rows hidden"
    );

    db.execute_seq(&[
        "BEGIN",
        "DELETE FROM isect_empty_a WHERE id = 1",
        "UPDATE isect_empty_a SET val = 3 WHERE id = 3",
        "INSERT INTO isect_empty_b VALUES (10, 1), (11, 3), (12, 9)",
        "COMMIT",
    ])
    .await;
    refresh_st_differential(&db, "isect_empty_st").await;
    e2e::oracle::assert_effective_refresh_mode(&db, "isect_empty_st", "DIFFERENTIAL")
        .await
        .unwrap();
    db.assert_st_matches_query("isect_empty_st", query).await;

    db.execute_seq(&[
        "BEGIN",
        "UPDATE isect_empty_a SET val = 4 WHERE id = 2",
        "INSERT INTO isect_empty_a VALUES (4, 5)",
        "DELETE FROM isect_empty_b WHERE id = 11",
        "UPDATE isect_empty_b SET val = 10 WHERE id = 12",
        "INSERT INTO isect_empty_b VALUES (13, 5)",
        "COMMIT",
    ])
    .await;
    refresh_st_differential(&db, "isect_empty_st").await;
    db.assert_st_matches_query("isect_empty_st", query).await;
    let visible: i64 = db
        .query_scalar("SELECT count(*) FROM isect_empty_st WHERE val = 5")
        .await;
    assert_eq!(
        visible, 1,
        "simultaneous changes expose the new shared row once"
    );
}

#[tokio::test]
async fn test_intersect_admission_rejects_other_forms_and_auto_selects_only_bounded_shape() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_admit_a (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_admit_b (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_admit_c (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO isect_admit_a VALUES (1, 1)").await;
    db.execute("INSERT INTO isect_admit_b VALUES (1, 1)").await;

    let explicit_all = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                 'isect_admit_all_explicit', \
                 $$SELECT val FROM isect_admit_a INTERSECT ALL SELECT val FROM isect_admit_b$$, \
                 '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(explicit_all.is_err(), "INTERSECT ALL remains rejected");
    let explicit_except = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                 'isect_admit_except_explicit', \
                 $$SELECT val FROM isect_admit_a EXCEPT SELECT val FROM isect_admit_b$$, \
                 '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(explicit_except.is_err(), "EXCEPT remains rejected");
    let explicit_nested = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                 'isect_admit_nested_explicit', \
                 $$SELECT val FROM isect_admit_a INTERSECT \
                   (SELECT val FROM isect_admit_b UNION SELECT val FROM isect_admit_c)$$, \
                 '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        explicit_nested.is_err(),
        "nested set operations remain rejected"
    );

    let ordinary = "SELECT val FROM isect_admit_a INTERSECT SELECT val FROM isect_admit_b";
    db.create_st("isect_admit_auto", ordinary, "1m", "AUTO")
        .await;
    let (auto_requested, auto_effective): (String, String) = sqlx::query_as(
        "SELECT requested_refresh_mode, refresh_mode FROM pgtrickle.pgt_stream_tables \
         WHERE pgt_name = 'isect_admit_auto'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("AUTO INTERSECT admission metadata");
    assert_eq!(auto_requested, "AUTO");
    assert_eq!(auto_effective, "DIFFERENTIAL");
    db.assert_st_matches_query("isect_admit_auto", ordinary)
        .await;
    db.refresh_st_with_retry("isect_admit_auto").await;
    db.execute_seq(&[
        "BEGIN",
        "INSERT INTO isect_admit_a VALUES (2, 2)",
        "INSERT INTO isect_admit_b VALUES (2, 2)",
        "COMMIT",
    ])
    .await;
    refresh_st_differential(&db, "isect_admit_auto").await;
    db.assert_st_matches_query("isect_admit_auto", ordinary)
        .await;

    let all_auto = "SELECT val FROM isect_admit_a INTERSECT ALL SELECT val FROM isect_admit_b";
    db.create_st("isect_admit_all_auto", all_auto, "1m", "AUTO")
        .await;
    let all_mode: String = db
        .query_scalar(
            "SELECT refresh_mode FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'isect_admit_all_auto'",
        )
        .await;
    assert_eq!(all_mode, "FULL", "AUTO keeps INTERSECT ALL on FULL");
    db.assert_st_matches_query("isect_admit_all_auto", all_auto)
        .await;

    let except_auto = "SELECT val FROM isect_admit_a EXCEPT SELECT val FROM isect_admit_b";
    db.create_st("isect_admit_except_auto", except_auto, "1m", "AUTO")
        .await;
    let except_mode: String = db
        .query_scalar(
            "SELECT refresh_mode FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'isect_admit_except_auto'",
        )
        .await;
    assert_eq!(except_mode, "FULL", "AUTO keeps EXCEPT on FULL");
    db.assert_st_matches_query("isect_admit_except_auto", except_auto)
        .await;
}

#[tokio::test]
async fn test_intersect_private_state_rebound_on_alter_query() {
    let db = E2eDb::new().await.with_extension().await;
    for table in [
        "isect_alter_a",
        "isect_alter_b",
        "isect_alter_c",
        "isect_alter_d",
    ] {
        db.execute(&format!(
            "CREATE TABLE {table} (id INT PRIMARY KEY, val INT)"
        ))
        .await;
    }
    db.execute("INSERT INTO isect_alter_a VALUES (1, 1)").await;
    db.execute("INSERT INTO isect_alter_b VALUES (1, 1)").await;
    db.execute("INSERT INTO isect_alter_c VALUES (1, 2)").await;
    db.execute("INSERT INTO isect_alter_d VALUES (1, 2)").await;

    let old_query = "SELECT val FROM isect_alter_a INTERSECT SELECT val FROM isect_alter_b";
    let new_query = "SELECT val FROM isect_alter_c INTERSECT SELECT val FROM isect_alter_d";
    db.create_st("isect_alter_st", old_query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st_with_retry("isect_alter_st").await;
    let pgt_id = setop_pgt_id(&db, "isect_alter_st").await;
    let state_rel = format!("pgtrickle.\"__pgt_setop_state_{pgt_id}\"");
    let old_state_oid: i64 = db
        .query_scalar(&format!("SELECT '{state_rel}'::regclass::oid::bigint"))
        .await;

    db.execute(&format!(
        "SELECT pgtrickle.alter_stream_table('isect_alter_st', query => $$ {new_query} $$)"
    ))
    .await;
    db.assert_st_matches_query("isect_alter_st", new_query)
        .await;
    let new_state_oid: i64 = db
        .query_scalar(&format!("SELECT '{state_rel}'::regclass::oid::bigint"))
        .await;
    assert_ne!(
        new_state_oid, old_state_oid,
        "ALTER rebuilds bound private state"
    );
    let catalogued: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_set_operation_states \
             WHERE pgt_id = {pgt_id} AND operation = 'INTERSECT' AND is_all = false"
        ))
        .await;
    assert_eq!(
        catalogued, 1,
        "ALTER leaves exactly one current state binding"
    );

    db.execute("INSERT INTO isect_alter_c VALUES (2, 3)").await;
    db.execute("INSERT INTO isect_alter_d VALUES (2, 3)").await;
    refresh_st_differential(&db, "isect_alter_st").await;
    db.assert_st_matches_query("isect_alter_st", new_query)
        .await;
}

#[tokio::test]
async fn test_intersect_differential_missing_state_falls_back_to_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_fallback_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_fallback_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO isect_fallback_a (val) VALUES (1)")
        .await;
    db.execute("INSERT INTO isect_fallback_b (val) VALUES (1)")
        .await;

    let query = "SELECT val FROM isect_fallback_a INTERSECT SELECT val FROM isect_fallback_b";
    db.create_st("isect_fallback_st", query, "1m", "DIFFERENTIAL")
        .await;
    let pgt_id = setop_pgt_id(&db, "isect_fallback_st").await;
    db.execute(&format!(
        "DELETE FROM pgtrickle.pgt_set_operation_states WHERE pgt_id = {pgt_id}"
    ))
    .await;

    db.execute("INSERT INTO isect_fallback_a (val) VALUES (2)")
        .await;
    db.execute("INSERT INTO isect_fallback_b (val) VALUES (2)")
        .await;
    db.refresh_st("isect_fallback_st").await;
    db.assert_st_matches_query("isect_fallback_st", query).await;
    let effective_mode: Option<String> = db
        .query_scalar(
            "SELECT effective_refresh_mode FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'isect_fallback_st'",
        )
        .await;
    assert_eq!(
        effective_mode.as_deref(),
        Some("FULL"),
        "missing state forces a safe full refresh"
    );
    let rebuilt_count: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_l FROM pgtrickle.\"__pgt_setop_state_{pgt_id}\" WHERE val = 2"
        ))
        .await;
    assert_eq!(rebuilt_count, 1);
}

#[tokio::test]
async fn test_intersect_differential_stale_state_binding_falls_back_and_rebuilds() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_stale_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_stale_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO isect_stale_a (val) VALUES (1)")
        .await;
    db.execute("INSERT INTO isect_stale_b (val) VALUES (1)")
        .await;

    let query = "SELECT val FROM isect_stale_a INTERSECT SELECT val FROM isect_stale_b";
    db.create_st("isect_stale_st", query, "1m", "DIFFERENTIAL")
        .await;
    let pgt_id = setop_pgt_id(&db, "isect_stale_st").await;
    let state_rel = format!("pgtrickle.\"__pgt_setop_state_{pgt_id}\"");
    db.execute("CREATE TABLE isect_stale_decoy (value INT)")
        .await;
    let decoy_oid: i64 = db
        .query_scalar("SELECT 'isect_stale_decoy'::regclass::oid::bigint")
        .await;
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_set_operation_states SET state_relid = {decoy_oid} \
         WHERE pgt_id = {pgt_id}"
    ))
    .await;

    db.execute("INSERT INTO isect_stale_a (val) VALUES (2)")
        .await;
    db.execute("INSERT INTO isect_stale_b (val) VALUES (2)")
        .await;
    db.refresh_st("isect_stale_st").await;
    db.assert_st_matches_query("isect_stale_st", query).await;

    let (action, was_full_fallback): (String, bool) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT action, was_full_fallback FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = {pgt_id} AND initiated_by = 'MANUAL' \
             ORDER BY refresh_id DESC LIMIT 1"
    )))
    .fetch_one(&db.pool)
    .await
    .expect("read refresh route after stale state fallback");
    assert_eq!((action.as_str(), was_full_fallback), ("FULL", true));

    let (registered_oid, actual_oid): (i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT state.state_relid::bigint, '{state_rel}'::regclass::oid::bigint \
             FROM pgtrickle.pgt_set_operation_states state WHERE state.pgt_id = {pgt_id}"
    )))
    .fetch_one(&db.pool)
    .await
    .expect("read rebuilt state binding");
    assert_eq!(
        registered_oid, actual_oid,
        "rebuilt state binding must be current"
    );
    assert_ne!(
        registered_oid, decoy_oid,
        "a restored stale OID must not bind the decoy"
    );
    let decoy_survived: bool = db
        .query_scalar("SELECT to_regclass('isect_stale_decoy') IS NOT NULL")
        .await;
    assert!(
        decoy_survived,
        "rebuilding set-op state must preserve the unrelated table"
    );
    let (left_count, right_count): (i64, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_count_l, __pgt_count_r FROM {state_rel} WHERE val = 2"
    )))
    .fetch_one(&db.pool)
    .await
    .expect("read rebuilt branch counts");
    assert_eq!((left_count, right_count), (1, 1));
}

#[tokio::test]
async fn test_intersect_differential_public_failure_rolls_back_private_state() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_rollback_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_rollback_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    let query = "SELECT val FROM isect_rollback_a INTERSECT SELECT val FROM isect_rollback_b";
    db.create_st("isect_rollback_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st_with_retry("isect_rollback_st").await;
    let pgt_id = setop_pgt_id(&db, "isect_rollback_st").await;
    let state_rel = format!("pgtrickle.\"__pgt_setop_state_{pgt_id}\"");

    db.execute("ALTER TABLE isect_rollback_st ADD CONSTRAINT reject_42 CHECK (val <> 42)")
        .await;
    let frontier_before: String = db
        .query_scalar(
            "SELECT frontier::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'isect_rollback_st'",
        )
        .await;
    db.execute("INSERT INTO isect_rollback_a (val) VALUES (42)")
        .await;
    db.execute("INSERT INTO isect_rollback_b (val) VALUES (42)")
        .await;
    let failed = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('isect_rollback_st')")
        .await;
    assert!(
        failed.is_err(),
        "the public MERGE must hit the test constraint"
    );

    let frontier_after: String = db
        .query_scalar(
            "SELECT frontier::text FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'isect_rollback_st'",
        )
        .await;
    assert_eq!(
        frontier_after, frontier_before,
        "failed public MERGE must not publish the refresh frontier"
    );

    let private_count: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM {state_rel} WHERE val = 42"))
        .await;
    assert_eq!(private_count, 0, "private state update must roll back");
    let public_count: i64 = db
        .query_scalar("SELECT count(*) FROM isect_rollback_st WHERE val = 42")
        .await;
    assert_eq!(public_count, 0, "public result remains unchanged");

    db.execute("ALTER TABLE isect_rollback_st DROP CONSTRAINT reject_42")
        .await;
    refresh_st_differential(&db, "isect_rollback_st").await;
    e2e::oracle::assert_effective_refresh_mode(&db, "isect_rollback_st", "DIFFERENTIAL")
        .await
        .unwrap();
    db.assert_st_matches_query("isect_rollback_st", query).await;
    let private_count: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_l FROM {state_rel} WHERE val = 42"
        ))
        .await;
    assert_eq!(private_count, 1, "retry applies the pending private delta");
}

#[tokio::test]
async fn test_intersect_differential_multicolumn_null_keys() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_null_a (id SERIAL PRIMARY KEY, x INT, y TEXT)")
        .await;
    db.execute("CREATE TABLE isect_null_b (id SERIAL PRIMARY KEY, x INT, y TEXT)")
        .await;
    db.execute("INSERT INTO isect_null_a (x, y) VALUES (1, NULL), (1, NULL), (2, 'b')")
        .await;
    db.execute("INSERT INTO isect_null_b (x, y) VALUES (1, NULL), (2, 'z')")
        .await;

    let query = "SELECT x, y FROM isect_null_a INTERSECT SELECT x, y FROM isect_null_b";
    db.create_st("isect_null_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.refresh_st_with_retry("isect_null_st").await;
    db.assert_st_matches_query("isect_null_st", query).await;
    let pgt_id = setop_pgt_id(&db, "isect_null_st").await;

    db.execute("DELETE FROM isect_null_b WHERE x = 1").await;
    refresh_st_differential(&db, "isect_null_st").await;
    e2e::oracle::assert_effective_refresh_mode(&db, "isect_null_st", "DIFFERENTIAL")
        .await
        .unwrap();
    db.assert_st_matches_query("isect_null_st", query).await;
    let hidden_count: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.\"__pgt_setop_state_{pgt_id}\" \
             WHERE x = 1 AND y IS NULL AND __pgt_count_l = 2 AND __pgt_count_r = 0",
        ))
        .await;
    assert_eq!(hidden_count, 1, "NULL tuple remains in private state");

    db.execute("INSERT INTO isect_null_b (x, y) VALUES (1, NULL)")
        .await;
    refresh_st_differential(&db, "isect_null_st").await;
    e2e::oracle::assert_effective_refresh_mode(&db, "isect_null_st", "DIFFERENTIAL")
        .await
        .unwrap();
    db.assert_st_matches_query("isect_null_st", query).await;
    let visible_count: i64 = db
        .query_scalar("SELECT count(*) FROM isect_null_st WHERE x = 1 AND y IS NULL")
        .await;
    assert_eq!(visible_count, 1, "NULL tuple is reinserted once");
}

#[tokio::test]
async fn test_intersect_all_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_all_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE isect_all_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO isect_all_a (val) VALUES (1), (1), (2), (3)")
        .await;
    db.execute("INSERT INTO isect_all_b (val) VALUES (1), (2), (2), (3)")
        .await;

    let q = "SELECT val FROM isect_all_a INTERSECT ALL SELECT val FROM isect_all_b";
    db.create_st("isect_all_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("isect_all_st", q).await;

    // Add duplicate in A
    db.execute("INSERT INTO isect_all_a (val) VALUES (2)").await;
    db.refresh_st("isect_all_st").await;
    db.assert_st_matches_query("isect_all_st", q).await;

    // Remove from B
    db.execute("DELETE FROM isect_all_b WHERE val = 1").await;
    db.refresh_st("isect_all_st").await;
    db.assert_st_matches_query("isect_all_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// EXCEPT
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_except_basic_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE exc_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE exc_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO exc_a (val) VALUES (1), (2), (3)")
        .await;
    db.execute("INSERT INTO exc_b (val) VALUES (2), (4)").await;

    let q = "SELECT val FROM exc_a EXCEPT SELECT val FROM exc_b";
    db.create_st("exc_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("exc_st", q).await;

    // Add to B a value that exists in A → shrinks result
    db.execute("INSERT INTO exc_b (val) VALUES (1)").await;
    db.refresh_st("exc_st").await;
    db.assert_st_matches_query("exc_st", q).await;

    // Add to A a new value
    db.execute("INSERT INTO exc_a (val) VALUES (5)").await;
    db.refresh_st("exc_st").await;
    db.assert_st_matches_query("exc_st", q).await;

    // Remove from B → re-exposes value in A
    db.execute("DELETE FROM exc_b WHERE val = 2").await;
    db.refresh_st("exc_st").await;
    db.assert_st_matches_query("exc_st", q).await;
}

#[tokio::test]
async fn test_except_all_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE exc_all_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE exc_all_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO exc_all_a (val) VALUES (1), (1), (1), (2)")
        .await;
    db.execute("INSERT INTO exc_all_b (val) VALUES (1), (2)")
        .await;

    let q = "SELECT val FROM exc_all_a EXCEPT ALL SELECT val FROM exc_all_b";
    db.create_st("exc_all_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("exc_all_st", q).await;

    // Remove duplicate from A
    db.execute("DELETE FROM exc_all_a WHERE id = (SELECT MIN(id) FROM exc_all_a WHERE val = 1)")
        .await;
    db.refresh_st("exc_all_st").await;
    db.assert_st_matches_query("exc_all_st", q).await;

    // Add to B → more subtractions
    db.execute("INSERT INTO exc_all_b (val) VALUES (1)").await;
    db.refresh_st("exc_all_st").await;
    db.assert_st_matches_query("exc_all_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-way chain (A UNION ALL B EXCEPT C)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_set_ops_chain_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE so_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE so_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE so_c (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO so_a (val) VALUES (1), (2)").await;
    db.execute("INSERT INTO so_b (val) VALUES (3), (4)").await;
    db.execute("INSERT INTO so_c (val) VALUES (2), (3)").await;

    let q = "(SELECT val FROM so_a UNION ALL SELECT val FROM so_b) \
             EXCEPT SELECT val FROM so_c";
    db.create_st("so_chain_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("so_chain_st", q).await;

    db.execute("INSERT INTO so_c (val) VALUES (1)").await;
    db.refresh_st("so_chain_st").await;
    db.assert_st_matches_query("so_chain_st", q).await;

    db.execute("DELETE FROM so_b WHERE val = 4").await;
    db.refresh_st("so_chain_st").await;
    db.assert_st_matches_query("so_chain_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Multi-column set operations
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_intersect_multi_column_full() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE isect_mc_a (id SERIAL PRIMARY KEY, x INT, y TEXT)")
        .await;
    db.execute("CREATE TABLE isect_mc_b (id SERIAL PRIMARY KEY, x INT, y TEXT)")
        .await;
    db.execute("INSERT INTO isect_mc_a (x, y) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    db.execute("INSERT INTO isect_mc_b (x, y) VALUES (1, 'a'), (2, 'z'), (4, 'd')")
        .await;

    let q = "SELECT x, y FROM isect_mc_a INTERSECT SELECT x, y FROM isect_mc_b";
    db.create_st("isect_mc_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("isect_mc_st", q).await;

    // Make (2,'b') match by updating B
    db.execute("UPDATE isect_mc_b SET y = 'b' WHERE x = 2")
        .await;
    db.refresh_st("isect_mc_st").await;
    db.assert_st_matches_query("isect_mc_st", q).await;

    // Remove matching row from A
    db.execute("DELETE FROM isect_mc_a WHERE x = 1").await;
    db.refresh_st("isect_mc_st").await;
    db.assert_st_matches_query("isect_mc_st", q).await;
}

#[tokio::test]
async fn test_set_operation_with_nulls() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE set_null_a (id INT, val TEXT)")
        .await;
    db.execute("CREATE TABLE set_null_b (id INT, val TEXT)")
        .await;

    db.execute("INSERT INTO set_null_a VALUES (1, NULL), (NULL, 'A')")
        .await;
    db.execute("INSERT INTO set_null_b VALUES (1, NULL), (NULL, 'B')")
        .await;

    let q = "SELECT id, val FROM set_null_a UNION ALL SELECT id, val FROM set_null_b";

    db.create_st("set_null_st", q, "1m", "DIFFERENTIAL").await;

    db.assert_st_matches_query("set_null_st", q).await;

    db.execute("INSERT INTO set_null_a VALUES (NULL, NULL)")
        .await;
    db.refresh_st("set_null_st").await;

    db.assert_st_matches_query("set_null_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// REL-101-1 / REL-101-3: durable, private per-branch multiplicity state
//
// A FULL refresh of a set-operation stream table always builds a private,
// extension-owned state relation holding the exact left/right branch
// multiplicities keyed by semantic row identity. The state must never leak
// counters or invisible rows into the user-visible stream table, and must be
// cleaned up on drop.
// ═══════════════════════════════════════════════════════════════════════

async fn setop_pgt_id(db: &E2eDb, name: &str) -> i64 {
    db.query_scalar::<i64>(&format!(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{name}'"
    ))
    .await
}

#[tokio::test]
async fn test_setop_private_state_intersect_counts_and_privacy() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ps_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE ps_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    // val=2 appears twice on the left, once on the right.
    db.execute("INSERT INTO ps_a (val) VALUES (1), (2), (2), (3)")
        .await;
    db.execute("INSERT INTO ps_b (val) VALUES (2), (3), (3), (4)")
        .await;

    let q = "SELECT val FROM ps_a INTERSECT SELECT val FROM ps_b";
    db.create_st("ps_isect", q, "1m", "FULL").await;

    // User-visible results are exactly the query result (multiset equality).
    db.assert_st_matches_query("ps_isect", q).await;
    let pgt_id = setop_pgt_id(&db, "ps_isect").await;
    let state_rel = format!("pgtrickle.\"__pgt_setop_state_{pgt_id}\"");

    // The private state relation exists and is catalogued.
    let exists: bool = db
        .query_scalar(&format!("SELECT to_regclass('{state_rel}') IS NOT NULL"))
        .await;
    assert!(exists, "private set-operation state relation must exist");
    let catalogued: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_set_operation_states \
             WHERE pgt_id = {pgt_id} AND operation = 'INTERSECT' AND is_all = false"
        ))
        .await;
    assert_eq!(
        catalogued, 1,
        "catalog row must describe the state relation"
    );

    // Counters never leak into the user-visible stream table.
    let leaked: i64 = db
        .query_scalar(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_name = 'ps_isect' AND column_name LIKE '\\_\\_pgt\\_count%'",
        )
        .await;
    assert_eq!(leaked, 0, "multiplicity counters must not leak into the ST");

    // The private state holds the exact per-branch multiplicities.
    let count_l: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_l FROM {state_rel} WHERE val = 2"
        ))
        .await;
    let count_r: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_r FROM {state_rel} WHERE val = 2"
        ))
        .await;
    assert_eq!((count_l, count_r), (2, 1), "val=2 has 2 left, 1 right");

    // Rebuild after a source change keeps the state exact.
    db.execute("INSERT INTO ps_b (val) VALUES (2)").await;
    db.refresh_st("ps_isect").await;
    db.assert_st_matches_query("ps_isect", q).await;
    let count_r2: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_r FROM {state_rel} WHERE val = 2"
        ))
        .await;
    assert_eq!(count_r2, 2, "right count for val=2 must rebuild to 2");

    // DROP removes the private relation and its catalog rows.
    db.drop_st("ps_isect").await;
    let exists_after: bool = db
        .query_scalar(&format!("SELECT to_regclass('{state_rel}') IS NOT NULL"))
        .await;
    assert!(
        !exists_after,
        "private state relation must be dropped with ST"
    );
    let catalogued_after: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM pgtrickle.pgt_set_operation_states WHERE pgt_id = {pgt_id}"
        ))
        .await;
    assert_eq!(catalogued_after, 0, "catalog rows must be cleaned up");
}

#[tokio::test]
async fn test_setop_private_state_except_tracks_invisible_rows() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE pe_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE pe_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    // val=1 only on the left (visible), val=2 on both (invisible under EXCEPT).
    db.execute("INSERT INTO pe_a (val) VALUES (1), (2), (2)")
        .await;
    db.execute("INSERT INTO pe_b (val) VALUES (2)").await;

    let q = "SELECT val FROM pe_a EXCEPT SELECT val FROM pe_b";
    db.create_st("pe_exc", q, "1m", "FULL").await;

    // The visible ST contains only visible rows (val=1) — never the invisible
    // row val=2.
    db.assert_st_matches_query("pe_exc", q).await;
    let visible_has_2: i64 = db
        .query_scalar("SELECT count(*) FROM pe_exc WHERE val = 2")
        .await;
    assert_eq!(
        visible_has_2, 0,
        "invisible EXCEPT rows must not appear in ST"
    );

    let pgt_id = setop_pgt_id(&db, "pe_exc").await;
    let state_rel = format!("pgtrickle.\"__pgt_setop_state_{pgt_id}\"");

    // The private state DOES track the invisible row's per-branch counts so a
    // future differential path can restore it if the right branch shrinks.
    let l: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_l FROM {state_rel} WHERE val = 2"
        ))
        .await;
    let r: i64 = db
        .query_scalar(&format!(
            "SELECT __pgt_count_r FROM {state_rel} WHERE val = 2"
        ))
        .await;
    assert_eq!((l, r), (2, 1), "val=2 tracked with 2 left, 1 right");

    db.drop_st("pe_exc").await;
    let exists_after: bool = db
        .query_scalar(&format!("SELECT to_regclass('{state_rel}') IS NOT NULL"))
        .await;
    assert!(
        !exists_after,
        "private state relation must be dropped with ST"
    );
}

#[tokio::test]
async fn test_setop_private_state_built_for_all_forms() {
    // Private multiplicity state is built for every set-operation form on FULL
    // refresh, without any configuration. The visible ST stays exact; the
    // counters live only in the private state relation.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE pd_a (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE pd_b (id SERIAL PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO pd_a (val) VALUES (1), (2), (2)")
        .await;
    db.execute("INSERT INTO pd_b (val) VALUES (2), (3)").await;

    let forms = [
        ("pd_isect", "INTERSECT", "INTERSECT", false),
        ("pd_isect_all", "INTERSECT ALL", "INTERSECT", true),
        ("pd_exc", "EXCEPT", "EXCEPT", false),
        ("pd_exc_all", "EXCEPT ALL", "EXCEPT", true),
    ];

    for (name, op_sql, op_catalog, is_all) in forms {
        let q = format!("SELECT val FROM pd_a {op_sql} SELECT val FROM pd_b");
        db.create_st(name, &q, "1m", "FULL").await;
        // Visible result stays exactly correct.
        db.assert_st_matches_query(name, &q).await;

        let pgt_id = setop_pgt_id(&db, name).await;
        let state_rel = format!("pgtrickle.\"__pgt_setop_state_{pgt_id}\"");
        let exists: bool = db
            .query_scalar(&format!("SELECT to_regclass('{state_rel}') IS NOT NULL"))
            .await;
        assert!(exists, "{name}: private state relation must be built");

        // Catalog row records the operation/is_all classification.
        let catalogued: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_set_operation_states \
                 WHERE pgt_id = {pgt_id} AND operation = '{op_catalog}' AND is_all = {is_all}"
            ))
            .await;
        assert_eq!(catalogued, 1, "{name}: catalog row must classify the form");

        // Counters never leak into the visible stream table.
        let leaked: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM information_schema.columns \
                 WHERE table_name = '{name}' AND column_name LIKE '\\_\\_pgt\\_count%'"
            ))
            .await;
        assert_eq!(leaked, 0, "{name}: counters must not leak into the ST");

        // val=2 has multiplicity 2 on the left and 1 on the right in every form.
        let count_l: i64 = db
            .query_scalar(&format!(
                "SELECT __pgt_count_l FROM {state_rel} WHERE val = 2"
            ))
            .await;
        let count_r: i64 = db
            .query_scalar(&format!(
                "SELECT __pgt_count_r FROM {state_rel} WHERE val = 2"
            ))
            .await;
        assert_eq!(
            (count_l, count_r),
            (2, 1),
            "{name}: val=2 tracked with 2 left, 1 right"
        );

        db.drop_st(name).await;
        let exists_after: bool = db
            .query_scalar(&format!("SELECT to_regclass('{state_rel}') IS NOT NULL"))
            .await;
        assert!(
            !exists_after,
            "{name}: private state must be dropped with ST"
        );
    }
}
