//! E2E tests for HAVING clause transition correctness (F25: G6.2).
//!
//! Validates that groups crossing the HAVING threshold in and out,
//! and rows migrating between groups, produce correct differential results.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use e2e::oracle;

async fn refresh_witnessed_differential(db: &E2eDb, stream: &str) {
    let source_oid: i64 = db
        .query_scalar(&format!(
            "SELECT d.source_relid::bigint FROM pgtrickle.pgt_dependencies d \
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = '{stream}'"
        ))
        .await;
    let buffer = db.change_buffer_table(source_oid).await;
    let captured: i64 = db
        .query_scalar(&format!(
            "SELECT count(*) FROM {buffer} WHERE action IN ('I', 'D', 'U')"
        ))
        .await;
    assert!(
        captured > 0,
        "{stream}: the source mutation must be captured"
    );
    let before: i64 = db
        .query_scalar("SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history")
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ],
        &format!("SELECT pgtrickle.refresh_stream_table('{stream}')"),
    )
    .await
    .expect("changed-data differential refresh");
    let (action, fallback, changed): (String, bool, i64) = sqlx::query_as(sqlx::AssertSqlSafe(
        format!(
            "SELECT action, was_full_fallback, delta_row_count \
             FROM pgtrickle.pgt_refresh_history WHERE refresh_id > {before} \
             AND pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{stream}') \
             AND status = 'COMPLETED' ORDER BY refresh_id DESC LIMIT 1"
        ),
    ))
    .fetch_one(&db.pool)
    .await
    .expect("new completed refresh history");
    assert_eq!(action, "DIFFERENTIAL", "{stream}: wrong executed route");
    assert!(
        !fallback,
        "{stream}: fallback cannot qualify the transition"
    );
    assert!(changed >= 0, "{stream}: invalid result delta size");
}

#[tokio::test]
async fn test_having_all_nine_truth_transitions_match_postgresql() {
    use std::collections::BTreeSet;

    let db = E2eDb::new().await.with_extension().await;
    let values = ["1", "-1", "NULL"];
    for (family, predicate) in [
        "SUM(v) > 0",
        "AVG(v) > 0",
        "SUM(v) > 0 AND COUNT(*) = 1",
        "AVG(v) > 0 OR COUNT(*) = 0",
    ]
    .into_iter()
    .enumerate()
    {
        let source = format!("having_truth_source_{family}");
        let stream = format!("having_truth_stream_{family}");
        db.execute(&format!(
            "CREATE TABLE {source} (id INT PRIMARY KEY, g INT, v INT)"
        ))
        .await;
        for id in 0..9 {
            db.execute(&format!(
                "INSERT INTO {source} VALUES ({id}, {id}, {})",
                values[id / 3]
            ))
            .await;
        }
        db.execute(&format!("INSERT INTO {source} VALUES (100, 100, 5)"))
            .await;
        let query = format!(
            "SELECT g, SUM(v) AS total, AVG(v) AS mean, COUNT(*) AS rows, COUNT(v) AS nonnull \
             FROM {source} GROUP BY g HAVING {predicate}"
        );
        db.execute(&format!(
            "SELECT pgtrickle.create_stream_table(name => '{stream}', query => $q${query}$q$, \
             schedule => '24h', refresh_mode => 'DIFFERENTIAL', cdc_mode => 'trigger')"
        ))
        .await;
        oracle::assert_st_query_exact(&db, &stream, &query, "initial HAVING state").await;
        let truth_query =
            format!("SELECT g, ({predicate}) FROM {source} WHERE g < 100 GROUP BY g ORDER BY g");
        let old: Vec<(i32, Option<bool>)> =
            sqlx::query_as(sqlx::AssertSqlSafe(truth_query.clone()))
                .fetch_all(&db.pool)
                .await
                .unwrap();
        db.execute(&format!(
            "UPDATE {source} SET v = CASE id % 3 WHEN 0 THEN 1 WHEN 1 THEN -1 ELSE NULL END WHERE id < 100"
        ))
        .await;
        let new: Vec<(i32, Option<bool>)> = sqlx::query_as(sqlx::AssertSqlSafe(truth_query))
            .fetch_all(&db.pool)
            .await
            .unwrap();
        assert_eq!((old.len(), new.len()), (9, 9));
        let observed: BTreeSet<_> = old
            .iter()
            .zip(&new)
            .map(|(old, new)| {
                assert_eq!(old.0, new.0);
                (old.1, new.1)
            })
            .collect();
        assert_eq!(
            observed.len(),
            9,
            "{predicate}: all actual PostgreSQL transitions must be reached"
        );
        refresh_witnessed_differential(&db, &stream).await;
        oracle::assert_st_query_exact(&db, &stream, &query, "nine HAVING transitions").await;
        for mutation in [
            format!("DELETE FROM {source} WHERE id < 100"),
            format!("INSERT INTO {source} VALUES (1, 1, 2)"),
            format!("UPDATE {source} SET g = 50 WHERE id = 1"),
            format!("UPDATE {source} SET v = 3 WHERE id = 1"),
        ] {
            db.execute(&mutation).await;
            refresh_witnessed_differential(&db, &stream).await;
            oracle::assert_st_query_exact(&db, &stream, &query, &mutation).await;
        }
    }
}

#[tokio::test]
async fn test_having_grouped_and_global_empty_lifecycle_match_postgresql() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE lifecycle_source (id INT PRIMARY KEY, g TEXT, v INT)")
        .await;
    let queries = [
        (
            "lifecycle_grouped",
            "SELECT g, COUNT(*) AS rows, COUNT(v) AS nonnull, SUM(v) AS total, AVG(v) AS mean FROM lifecycle_source GROUP BY g",
        ),
        (
            "lifecycle_global",
            "SELECT COUNT(*) AS rows, COUNT(v) AS nonnull, SUM(v) AS total, AVG(v) AS mean FROM lifecycle_source",
        ),
        (
            "lifecycle_global_having",
            "SELECT COUNT(*) AS rows, COUNT(v) AS nonnull, SUM(v) AS total FROM lifecycle_source HAVING SUM(v) > 0",
        ),
        (
            "lifecycle_global_avg_having",
            "SELECT COUNT(*) AS rows, COUNT(v) AS nonnull, AVG(v) AS mean FROM lifecycle_source HAVING AVG(v) > 0",
        ),
    ];
    for (stream, query) in queries {
        db.execute(&format!(
            "SELECT pgtrickle.create_stream_table(name => '{stream}', query => $q${query}$q$, \
             schedule => '24h', refresh_mode => 'DIFFERENTIAL', cdc_mode => 'trigger')"
        ))
        .await;
        oracle::assert_st_query_exact(&db, stream, query, "initial empty aggregate").await;
    }
    for mutation in [
        "INSERT INTO lifecycle_source VALUES (1, 'a', NULL)",
        "UPDATE lifecycle_source SET v = 5 WHERE id = 1",
        "INSERT INTO lifecycle_source VALUES (2, 'a', NULL)",
        "UPDATE lifecycle_source SET v = NULL WHERE id = 1",
        "DELETE FROM lifecycle_source",
        "INSERT INTO lifecycle_source VALUES (3, 'b', 2)",
    ] {
        db.execute(mutation).await;
        for (stream, query) in queries {
            refresh_witnessed_differential(&db, stream).await;
            oracle::assert_st_query_exact(&db, stream, query, mutation).await;
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Group crosses HAVING threshold: appears / disappears
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_having_group_crosses_threshold_up() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE hv_up (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO hv_up (grp, val) VALUES ('a', 10), ('b', 5)")
        .await;

    let q = "SELECT grp, SUM(val) AS total FROM hv_up GROUP BY grp HAVING SUM(val) > 20";
    db.create_st("hv_up_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("hv_up_st", q).await;

    // Group 'a' crosses threshold (10 + 15 = 25 > 20)
    db.execute("INSERT INTO hv_up (grp, val) VALUES ('a', 15)")
        .await;
    db.refresh_st("hv_up_st").await;
    db.assert_st_matches_query("hv_up_st", q).await;

    // Group 'b' also crosses
    db.execute("INSERT INTO hv_up (grp, val) VALUES ('b', 20)")
        .await;
    db.refresh_st("hv_up_st").await;
    db.assert_st_matches_query("hv_up_st", q).await;
}

#[tokio::test]
async fn test_having_group_crosses_threshold_down() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE hv_down (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO hv_down (grp, val) VALUES ('a', 30), ('a', 10), ('b', 50)")
        .await;

    let q = "SELECT grp, SUM(val) AS total FROM hv_down GROUP BY grp HAVING SUM(val) > 20";
    db.create_st("hv_down_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("hv_down_st", q).await;

    // Remove from 'a' so sum drops below threshold (30→10)
    db.execute("DELETE FROM hv_down WHERE grp = 'a' AND val = 30")
        .await;
    db.refresh_st("hv_down_st").await;
    db.assert_st_matches_query("hv_down_st", q).await;

    // Reduce further — 'a' total = 10, still below
    db.execute("UPDATE hv_down SET val = 5 WHERE grp = 'a' AND val = 10")
        .await;
    db.refresh_st("hv_down_st").await;
    db.assert_st_matches_query("hv_down_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Group oscillates across threshold (up → down → up)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_having_threshold_oscillation() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE hv_osc (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO hv_osc (grp, val) VALUES ('a', 15), ('a', 10)")
        .await;

    let q = "SELECT grp, SUM(val) AS total FROM hv_osc GROUP BY grp HAVING SUM(val) >= 30";
    db.create_st("hv_osc_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("hv_osc_st", q).await;

    // Cross up: 15+10+10 = 35 >= 30
    db.execute("INSERT INTO hv_osc (grp, val) VALUES ('a', 10)")
        .await;
    db.refresh_st("hv_osc_st").await;
    db.assert_st_matches_query("hv_osc_st", q).await;

    // Cross down: delete 15 → 10+10 = 20 < 30
    db.execute("DELETE FROM hv_osc WHERE val = 15").await;
    db.refresh_st("hv_osc_st").await;
    db.assert_st_matches_query("hv_osc_st", q).await;

    // Cross up again: +20 → 10+10+20 = 40 >= 30
    db.execute("INSERT INTO hv_osc (grp, val) VALUES ('a', 20)")
        .await;
    db.refresh_st("hv_osc_st").await;
    db.assert_st_matches_query("hv_osc_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Row migrates between groups (UPDATE grp column)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_having_row_group_migration() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE hv_mig (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO hv_mig (grp, val) VALUES ('a', 25), ('a', 10), ('b', 5)")
        .await;

    let q = "SELECT grp, SUM(val) AS total FROM hv_mig GROUP BY grp HAVING SUM(val) > 20";
    db.create_st("hv_mig_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("hv_mig_st", q).await;

    // Move val=25 from 'a' to 'b': a drops (10 ≤ 20), b rises (5+25=30 > 20)
    db.execute("UPDATE hv_mig SET grp = 'b' WHERE val = 25")
        .await;
    db.refresh_st("hv_mig_st").await;
    db.assert_st_matches_query("hv_mig_st", q).await;

    // Move it back
    db.execute("UPDATE hv_mig SET grp = 'a' WHERE val = 25")
        .await;
    db.refresh_st("hv_mig_st").await;
    db.assert_st_matches_query("hv_mig_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// HAVING with COUNT condition
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_having_count_threshold() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE hv_cnt (id SERIAL PRIMARY KEY, grp TEXT, val TEXT)")
        .await;
    db.execute("INSERT INTO hv_cnt (grp, val) VALUES ('a', 'x'), ('a', 'y'), ('b', 'z')")
        .await;

    let q = "SELECT grp, COUNT(*) AS cnt FROM hv_cnt GROUP BY grp HAVING COUNT(*) >= 2";
    db.create_st("hv_cnt_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("hv_cnt_st", q).await;

    // 'b' reaches threshold
    db.execute("INSERT INTO hv_cnt (grp, val) VALUES ('b', 'w')")
        .await;
    db.refresh_st("hv_cnt_st").await;
    db.assert_st_matches_query("hv_cnt_st", q).await;

    // 'a' drops below
    db.execute("DELETE FROM hv_cnt WHERE grp = 'a' AND val = 'x'")
        .await;
    db.refresh_st("hv_cnt_st").await;
    db.assert_st_matches_query("hv_cnt_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Multiple HAVING conditions (SUM AND COUNT)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_having_multiple_conditions() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE hv_multi (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO hv_multi (grp, val) VALUES \
         ('a', 10), ('a', 20), ('a', 30), ('b', 100), ('c', 5), ('c', 5)",
    )
    .await;

    let q = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt \
             FROM hv_multi GROUP BY grp \
             HAVING SUM(val) > 15 AND COUNT(*) >= 2";
    db.create_st("hv_multi_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("hv_multi_st", q).await;

    // 'b' gets a second row → now passes COUNT cond
    db.execute("INSERT INTO hv_multi (grp, val) VALUES ('b', 50)")
        .await;
    db.refresh_st("hv_multi_st").await;
    db.assert_st_matches_query("hv_multi_st", q).await;

    // Remove from 'a' until SUM drops below 15
    db.execute("DELETE FROM hv_multi WHERE grp = 'a' AND val = 30")
        .await;
    db.execute("DELETE FROM hv_multi WHERE grp = 'a' AND val = 20")
        .await;
    db.refresh_st("hv_multi_st").await;
    db.assert_st_matches_query("hv_multi_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Group complete elimination (delete all rows in group)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_having_group_complete_elimination() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE hv_elim (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO hv_elim (grp, val) VALUES ('a', 50), ('a', 60), ('b', 100)")
        .await;

    let q = "SELECT grp, SUM(val) AS total FROM hv_elim GROUP BY grp HAVING SUM(val) > 30";
    db.create_st("hv_elim_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("hv_elim_st", q).await;

    // Delete all 'a' rows
    db.execute("DELETE FROM hv_elim WHERE grp = 'a'").await;
    db.refresh_st("hv_elim_st").await;
    db.assert_st_matches_query("hv_elim_st", q).await;

    // Re-add 'a' above threshold
    db.execute("INSERT INTO hv_elim (grp, val) VALUES ('a', 999)")
        .await;
    db.refresh_st("hv_elim_st").await;
    db.assert_st_matches_query("hv_elim_st", q).await;
}
