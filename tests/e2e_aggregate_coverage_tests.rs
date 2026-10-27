//! E2E tests for aggregate function differential correctness (F17: G6.1).
//!
//! Validates that each supported aggregate function produces correct
//! differential results after INSERT, UPDATE, and DELETE operations.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

async fn latest_merge_strategy(db: &E2eDb, stream_table: &str) -> String {
    db.query_scalar(&format!(
        "SELECT COALESCE(h.merge_strategy_used, '') \
         FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE st.pgt_name = '{stream_table}' \
         ORDER BY h.refresh_id DESC LIMIT 1"
    ))
    .await
}

async fn refresh_differential(db: &E2eDb, stream_table: &str) {
    let refresh = format!("SELECT pgtrickle.refresh_stream_table('{stream_table}')");
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ],
        &refresh,
    )
    .await
    .unwrap_or_else(|error| panic!("{stream_table}: differential refresh failed: {error}"));

    let differential_completed: bool = db
        .query_scalar(&format!(
            "SELECT h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' \
             AND NOT h.was_full_fallback \
             FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = '{stream_table}' \
             ORDER BY h.refresh_id DESC LIMIT 1"
        ))
        .await;
    assert!(
        differential_completed,
        "{stream_table}: no differential refresh"
    );
}

async fn refresh_vector_st(db: &E2eDb, stream_table: &str) {
    let refresh = format!("SELECT pgtrickle.refresh_stream_table('{stream_table}')");
    db.execute_seq(&[
        "SET pg_trickle.aggregate_fast_path = on",
        "SET pg_trickle.validate_delta_invariants = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        &refresh,
    ])
    .await;
}

// ═══════════════════════════════════════════════════════════════════════
// Basic aggregates: SUM, AVG, COUNT, MIN, MAX
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_vector_aggregate_selected_for_bounded_mixed_pages_and_text_key_falls_back() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE vector_page_source (id INT PRIMARY KEY, grp INT NOT NULL, val INT)")
        .await;
    db.execute(
        "INSERT INTO vector_page_source SELECT i, i % 37, i % 101 FROM generate_series(1, 3000) i",
    )
    .await;
    let query = "SELECT grp, SUM(val) AS total, COUNT(*) AS rows, AVG(val) AS mean \
                 FROM vector_page_source GROUP BY grp";
    db.create_st("vector_page_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.execute(
        "UPDATE vector_page_source SET grp = CASE WHEN id <= 100 THEN (grp + 1) % 37 ELSE grp END, \
         val = val + 7 WHERE id <= 900",
    )
    .await;
    db.execute("DELETE FROM vector_page_source WHERE id BETWEEN 901 AND 1200")
        .await;
    db.execute(
        "INSERT INTO vector_page_source SELECT i, i % 37, i % 101 FROM generate_series(4001, 4400) i",
    )
    .await;
    refresh_vector_st(&db, "vector_page_st").await;
    db.assert_st_matches_query("vector_page_st", query).await;
    let has_fractional_avg: bool = db
        .query_scalar("SELECT EXISTS (SELECT 1 FROM vector_page_st WHERE mean <> trunc(mean))")
        .await;
    assert!(has_fractional_avg, "AVG must retain its numeric fraction");
    let vector_evidence: String = db
        .query_scalar(
            "SELECT (pgtrickle.explain_delta_plan(pgt_id) -> 'vector_path')::text \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'vector_page_st'",
        )
        .await;
    assert_eq!(
        latest_merge_strategy(&db, "vector_page_st").await,
        "vector_agg",
        "vector evidence: {vector_evidence}"
    );

    db.execute("CREATE TABLE vector_fallback_source (id INT PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO vector_fallback_source VALUES (1, 'a', 1), (2, 'a', 2)")
        .await;
    let fallback_query = "SELECT grp, SUM(val) AS total FROM vector_fallback_source GROUP BY grp";
    db.create_st("vector_fallback_st", fallback_query, "1m", "DIFFERENTIAL")
        .await;
    db.execute("INSERT INTO vector_fallback_source VALUES (3, 'a', 3)")
        .await;
    refresh_vector_st(&db, "vector_fallback_st").await;
    db.assert_st_matches_query("vector_fallback_st", fallback_query)
        .await;
    assert_ne!(
        latest_merge_strategy(&db, "vector_fallback_st").await,
        "vector_agg"
    );

    db.execute("CREATE TABLE vector_extreme_source (id INT PRIMARY KEY, grp INT, val INT)")
        .await;
    db.execute("INSERT INTO vector_extreme_source VALUES (1, 1, 10), (2, 1, 20)")
        .await;
    let extreme_query = "SELECT grp, MIN(val) AS low, MAX(val) AS high \
                         FROM vector_extreme_source GROUP BY grp";
    db.create_st("vector_extreme_st", extreme_query, "1m", "DIFFERENTIAL")
        .await;
    db.execute("INSERT INTO vector_extreme_source VALUES (3, 1, 5), (4, 2, 40)")
        .await;
    refresh_vector_st(&db, "vector_extreme_st").await;
    db.assert_st_matches_query("vector_extreme_st", extreme_query)
        .await;
    assert_eq!(
        latest_merge_strategy(&db, "vector_extreme_st").await,
        "vector_agg"
    );
    db.execute("DELETE FROM vector_extreme_source WHERE id IN (3, 4)")
        .await;
    refresh_vector_st(&db, "vector_extreme_st").await;
    db.assert_st_matches_query("vector_extreme_st", extreme_query)
        .await;
    assert_eq!(
        latest_merge_strategy(&db, "vector_extreme_st").await,
        "vector_agg"
    );

    db.execute("CREATE TABLE vector_global_source (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO vector_global_source VALUES (1, NULL), (2, 10), (3, 30)")
        .await;
    let global_query = "SELECT COUNT(val) AS present, SUM(val) AS total, AVG(val) AS mean, \
                        MIN(val) AS low, MAX(val) AS high FROM vector_global_source";
    db.create_st("vector_global_st", global_query, "1m", "DIFFERENTIAL")
        .await;
    db.execute_seq(&[
        "UPDATE vector_global_source SET val = CASE id WHEN 1 THEN 20 WHEN 2 THEN NULL ELSE val END",
        "DELETE FROM vector_global_source WHERE id = 3",
        "INSERT INTO vector_global_source VALUES (4, 40)",
    ])
    .await;
    refresh_vector_st(&db, "vector_global_st").await;
    db.assert_st_matches_query("vector_global_st", global_query)
        .await;
    let global_evidence: String = db
        .query_scalar(
            "SELECT (pgtrickle.explain_delta_plan(pgt_id) -> 'vector_path')::text \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'vector_global_st'",
        )
        .await;
    assert_eq!(
        latest_merge_strategy(&db, "vector_global_st").await,
        "vector_agg",
        "vector evidence: {global_evidence}"
    );
}

#[tokio::test]
async fn test_agg_sum_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_sum (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO agg_sum (grp, val) VALUES ('a', 10), ('a', 20), ('b', 30)")
        .await;

    let q = "SELECT grp, SUM(val) AS total FROM agg_sum GROUP BY grp";
    db.create_st("agg_sum_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_sum_st", q).await;

    // INSERT
    db.execute("INSERT INTO agg_sum (grp, val) VALUES ('a', 5)")
        .await;
    db.refresh_st("agg_sum_st").await;
    db.assert_st_matches_query("agg_sum_st", q).await;

    // UPDATE
    db.execute("UPDATE agg_sum SET val = 100 WHERE grp = 'b'")
        .await;
    db.refresh_st("agg_sum_st").await;
    db.assert_st_matches_query("agg_sum_st", q).await;

    // DELETE
    db.execute("DELETE FROM agg_sum WHERE val = 5").await;
    db.refresh_st("agg_sum_st").await;
    db.assert_st_matches_query("agg_sum_st", q).await;
}

#[tokio::test]
async fn test_agg_sum_special_numeric_values_rescan_affected_groups() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_special_sum (id INT PRIMARY KEY, grp TEXT, val NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO agg_special_sum VALUES \
         (1,'pos','Infinity'),(2,'pos',1), \
         (3,'pos_delete','Infinity'),(4,'pos_delete',1), \
         (5,'neg','-Infinity'),(6,'neg',2), \
         (7,'nan','NaN'),(8,'nan',3), \
         (9,'null',NULL),(10,'null',NULL), \
         (11,'finite',0),(12,'finite',4), \
         (17,'repeat','Infinity'),(18,'repeat','Infinity'),(19,'repeat',1), \
         (20,'to_null','Infinity'),(21,'to_null',NULL), \
         (22,'nan_delete','NaN'),(23,'nan_delete',3), \
         (24,'to_empty','Infinity')",
    )
    .await;

    let queries = [
        (
            "agg_special_sum_numeric",
            "SELECT grp, SUM(val) AS total FROM agg_special_sum GROUP BY grp",
        ),
        (
            "agg_special_sum_expression",
            "SELECT grp, SUM(val + 0::numeric) AS total \
             FROM agg_special_sum GROUP BY grp",
        ),
        (
            "agg_special_sum_float",
            "SELECT grp, SUM(val::double precision) AS total \
             FROM agg_special_sum GROUP BY grp",
        ),
        (
            "agg_special_avg_numeric",
            "SELECT grp, AVG(val) AS average FROM agg_special_sum GROUP BY grp",
        ),
        (
            "agg_special_avg_float",
            "SELECT grp, AVG(val::double precision) AS average \
             FROM agg_special_sum GROUP BY grp",
        ),
    ];
    for (name, query) in queries {
        db.create_st(name, query, "1m", "DIFFERENTIAL").await;
        db.assert_st_matches_query(name, query).await;
    }

    for mutation in [
        "DELETE FROM agg_special_sum WHERE id=2", // finite deletion while +Infinity remains
        "DELETE FROM agg_special_sum WHERE id=3", // +Infinity deletion leaves finite 1
        "UPDATE agg_special_sum SET val=5 WHERE id=1", // +Infinity → finite
        "UPDATE agg_special_sum SET val=3 WHERE id=5", // -Infinity → finite
        "UPDATE agg_special_sum SET val=0 WHERE id=7", // NaN → zero
        "DELETE FROM agg_special_sum WHERE id=22", // NaN deletion leaves finite 3
        "INSERT INTO agg_special_sum VALUES \
         (13,'competing','Infinity'),(14,'competing','-Infinity'),(15,'competing',7)",
        "DELETE FROM agg_special_sum WHERE id=13", // competing infinities → -Infinity
        "DELETE FROM agg_special_sum WHERE id=14", // -Infinity removal recovers 7
        "DELETE FROM agg_special_sum WHERE id=17", // repeated +Infinity still sums to +Infinity
        "DELETE FROM agg_special_sum WHERE id=18", // remaining +Infinity removal recovers 1
        "UPDATE agg_special_sum SET val=NULL WHERE id=20", // +Infinity → all-NULL group
        "DELETE FROM agg_special_sum WHERE id=21", // all-NULL group becomes empty
        "DELETE FROM agg_special_sum WHERE id=24", // special-only group becomes empty
        "UPDATE agg_special_sum SET val=8 WHERE id=12", // finite update
        "UPDATE agg_special_sum SET val=0 WHERE id=12", // zero
        "UPDATE agg_special_sum SET val=NULL WHERE id=12", // zero → NULL
        "DELETE FROM agg_special_sum WHERE grp='null'", // all-NULL group removed
        "DELETE FROM agg_special_sum WHERE grp='finite'", // empty group removed
        "DELETE FROM agg_special_sum",             // empty input
        "INSERT INTO agg_special_sum VALUES (16,'again',0)", // repopulate
    ] {
        db.execute(mutation).await;
        for (name, query) in queries {
            db.try_execute_with_config(
                &[
                    "SET pg_trickle.refresh_strategy = 'differential'",
                    "SET pg_trickle.differential_max_change_ratio = 0",
                ],
                &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("{name}: differential refresh failed after {mutation}: {error}")
            });
            db.assert_st_matches_query(name, query).await;
            let differential_completed: bool = db
                .query_scalar(&format!(
                    "SELECT h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' \
                     AND NOT h.was_full_fallback \
                     FROM pgtrickle.pgt_refresh_history h \
                     JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
                     WHERE st.pgt_name = '{name}' \
                     ORDER BY h.refresh_id DESC LIMIT 1"
                ))
                .await;
            assert!(
                differential_completed,
                "{name}: no differential refresh after {mutation}"
            );
        }
    }
}

#[tokio::test]
async fn test_agg_sum_special_numeric_global_and_batched_changes() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_global_special (id INT PRIMARY KEY, val NUMERIC)")
        .await;
    db.execute("INSERT INTO agg_global_special VALUES (1,'Infinity'),(2,1)")
        .await;
    let query = "SELECT SUM(val) AS total FROM agg_global_special";
    db.create_st("agg_global_special_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("agg_global_special_st", query)
        .await;

    db.execute("DELETE FROM agg_global_special WHERE id=1")
        .await;
    refresh_differential(&db, "agg_global_special_st").await;
    db.assert_st_matches_query("agg_global_special_st", query)
        .await;

    db.execute("UPDATE agg_global_special SET val='Infinity' WHERE id=2")
        .await;
    db.execute("UPDATE agg_global_special SET val=2 WHERE id=2")
        .await;
    db.execute("INSERT INTO agg_global_special VALUES (3,5)")
        .await;
    refresh_differential(&db, "agg_global_special_st").await;
    db.assert_st_matches_query("agg_global_special_st", query)
        .await;

    db.execute("DELETE FROM agg_global_special").await;
    refresh_differential(&db, "agg_global_special_st").await;
    db.assert_st_matches_query("agg_global_special_st", query)
        .await;

    db.execute("INSERT INTO agg_global_special VALUES (4,NULL)")
        .await;
    refresh_differential(&db, "agg_global_special_st").await;
    db.assert_st_matches_query("agg_global_special_st", query)
        .await;
}

#[tokio::test]
async fn test_agg_statistical_special_numeric_rescans_auxiliaries() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_stat_special (id INT PRIMARY KEY, grp TEXT, val NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO agg_stat_special VALUES \
         (1,'x','Infinity'),(2,'x',1),(3,'x',3), \
         (4,'y','-Infinity'),(5,'y',2), \
         (6,'all_null','Infinity'),(7,'all_null',NULL)",
    )
    .await;

    let query = "SELECT grp, STDDEV_POP(val) AS sd, VAR_POP(val) AS variance \
                 FROM public.agg_stat_special GROUP BY grp";
    db.create_st("agg_stat_special_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("agg_stat_special_st", query)
        .await;

    db.execute("UPDATE agg_stat_special SET val=NULL WHERE id=6")
        .await;
    refresh_differential(&db, "agg_stat_special_st").await;
    db.assert_st_matches_query("agg_stat_special_st", query)
        .await;

    db.execute("DELETE FROM agg_stat_special WHERE id IN (1,4)")
        .await;
    refresh_differential(&db, "agg_stat_special_st").await;
    db.assert_st_matches_query("agg_stat_special_st", query)
        .await;
}

#[tokio::test]
async fn test_agg_avg_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_avg (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC)")
        .await;
    db.execute("INSERT INTO agg_avg (grp, val) VALUES ('x', 10), ('x', 20), ('y', 30)")
        .await;

    let q = "SELECT grp, AVG(val) AS avg_val FROM agg_avg GROUP BY grp";
    db.create_st("agg_avg_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_avg_st", q).await;

    db.execute("INSERT INTO agg_avg (grp, val) VALUES ('x', 30)")
        .await;
    db.refresh_st("agg_avg_st").await;
    db.assert_st_matches_query("agg_avg_st", q).await;

    db.execute("DELETE FROM agg_avg WHERE val = 10 AND grp = 'x'")
        .await;
    db.refresh_st("agg_avg_st").await;
    db.assert_st_matches_query("agg_avg_st", q).await;
}

#[tokio::test]
async fn test_agg_count_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_cnt (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO agg_cnt (grp, val) VALUES ('a', 1), ('a', 2), ('b', 3)")
        .await;

    let q = "SELECT grp, COUNT(*) AS cnt FROM agg_cnt GROUP BY grp";
    db.create_st("agg_cnt_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_cnt_st", q).await;

    db.execute("INSERT INTO agg_cnt (grp, val) VALUES ('a', 4), ('c', 5)")
        .await;
    db.refresh_st("agg_cnt_st").await;
    db.assert_st_matches_query("agg_cnt_st", q).await;

    db.execute("DELETE FROM agg_cnt WHERE grp = 'a' AND val = 1")
        .await;
    db.refresh_st("agg_cnt_st").await;
    db.assert_st_matches_query("agg_cnt_st", q).await;
}

#[tokio::test]
async fn test_agg_min_max_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_mm (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO agg_mm (grp, val) VALUES ('a', 10), ('a', 20), ('a', 30)")
        .await;

    let q = "SELECT grp, MIN(val) AS lo, MAX(val) AS hi FROM agg_mm GROUP BY grp";
    db.create_st("agg_mm_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_mm_st", q).await;

    db.execute("INSERT INTO agg_mm (grp, val) VALUES ('a', 5)")
        .await;
    db.refresh_st("agg_mm_st").await;
    db.assert_st_matches_query("agg_mm_st", q).await;

    db.execute("DELETE FROM agg_mm WHERE val = 30").await;
    db.refresh_st("agg_mm_st").await;
    db.assert_st_matches_query("agg_mm_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// DISTINCT aggregates
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_agg_expression_group_by_without_distinct_stays_differential() {
    for filtered in [false, true] {
        let db = E2eDb::new().await.with_extension().await;
        db.execute(
            "CREATE TABLE agg_events (id INT PRIMARY KEY, ts TIMESTAMPTZ NOT NULL, props JSONB NOT NULL)",
        )
        .await;
        db.execute(
            r#"INSERT INTO agg_events VALUES
               (1, '2026-01-01 23:30:00+00', '{"path":"/a"}'),
               (2, '2026-01-02 01:00:00+00', '{"path":"/a"}'),
               (3, '2026-01-02 02:00:00+00', '{}')"#,
        )
        .await;

        let predicate = if filtered { "WHERE id > 1" } else { "" };
        let queries: Vec<_> = [
            ("agg_daily", "(ts AT TIME ZONE 'UTC')::date"),
            ("agg_paths", "coalesce(props ->> 'path', '')"),
        ]
        .into_iter()
        .map(|(name, key)| {
            (
                name,
                format!(
                    "SELECT {key} AS key, COUNT(*) AS n FROM agg_events {predicate} GROUP BY {key}"
                ),
            )
        })
        .collect();
        for (name, query) in &queries {
            db.create_st(name, query, "1m", "DIFFERENTIAL").await;
            db.assert_st_matches_query(name, query).await;
        }

        for mutation in [
            r#"INSERT INTO agg_events VALUES
               (4, '2026-01-02 03:00:00+00', '{"path":"/b"}'),
               (5, '2026-01-03 00:00:00+00', '{"path":null}')"#,
            r#"UPDATE agg_events SET ts = '2026-01-03 01:00:00+00', props = '{"path":"/b"}'
               WHERE id IN (2, 3)"#,
            "DELETE FROM agg_events WHERE id IN (2, 3, 4, 5)",
        ] {
            db.execute(mutation).await;
            for (name, query) in &queries {
                refresh_differential(&db, name).await;
                db.assert_st_matches_query(name, query).await;
            }
        }
    }
}

#[tokio::test]
async fn test_agg_count_distinct_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_cdist (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO agg_cdist (grp, val) VALUES \
             ('a', 1), ('a', 1), ('a', 2), ('a', NULL), ('b', 3)",
    )
    .await;

    let q = "SELECT grp, COUNT(DISTINCT val) AS uniq FROM agg_cdist GROUP BY grp";
    db.create_st("agg_cdist_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_cdist_st", q).await;

    // Duplicate → distinct count unchanged
    db.execute("INSERT INTO agg_cdist (grp, val) VALUES ('a', 1)")
        .await;
    db.refresh_st("agg_cdist_st").await;
    db.assert_st_matches_query("agg_cdist_st", q).await;

    // New unique
    db.execute("INSERT INTO agg_cdist (grp, val) VALUES ('a', 99)")
        .await;
    db.refresh_st("agg_cdist_st").await;
    db.assert_st_matches_query("agg_cdist_st", q).await;

    // Retract duplicates, then the last occurrence.
    db.execute("DELETE FROM agg_cdist WHERE id IN (SELECT id FROM agg_cdist WHERE grp = 'a' AND val = 1 LIMIT 2)")
            .await;
    db.refresh_st("agg_cdist_st").await;
    db.assert_st_matches_query("agg_cdist_st", q).await;

    db.execute("DELETE FROM agg_cdist WHERE grp = 'a' AND val = 99")
        .await;
    db.refresh_st("agg_cdist_st").await;
    db.assert_st_matches_query("agg_cdist_st", q).await;

    // Empty group transition.
    db.execute("DELETE FROM agg_cdist WHERE grp = 'b'").await;
    db.refresh_st("agg_cdist_st").await;
    db.assert_st_matches_query("agg_cdist_st", q).await;
}

#[tokio::test]
async fn test_agg_sum_distinct_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_sdist (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO agg_sdist (grp, val) VALUES \
             ('a', 10), ('a', 10), ('a', 20), ('a', NULL), ('b', 5)",
    )
    .await;

    let q = "SELECT grp, SUM(DISTINCT val) AS total FROM agg_sdist GROUP BY grp";
    db.create_st("agg_sdist_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_sdist_st", q).await;

    db.execute("INSERT INTO agg_sdist (grp, val) VALUES ('a', 30)")
        .await;
    db.refresh_st("agg_sdist_st").await;
    db.assert_st_matches_query("agg_sdist_st", q).await;

    db.execute("DELETE FROM agg_sdist WHERE id IN (SELECT id FROM agg_sdist WHERE grp = 'a' AND val = 10 LIMIT 1)")
            .await;
    db.refresh_st("agg_sdist_st").await;
    db.assert_st_matches_query("agg_sdist_st", q).await;

    db.execute("DELETE FROM agg_sdist WHERE grp = 'a' AND val IN (10, 20, 30)")
        .await;
    db.refresh_st("agg_sdist_st").await;
    db.assert_st_matches_query("agg_sdist_st", q).await;
}

#[tokio::test]
async fn test_agg_avg_distinct_numeric_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_adist (id SERIAL PRIMARY KEY, grp INT, val NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO agg_adist (grp, val) VALUES \
             (1, 1000000000000.01), (1, 1000000000000.01), \
             (1, 1000000000000.02), (1, NULL), (2, 5)",
    )
    .await;

    let q = "SELECT grp, AVG(DISTINCT val) AS avg_val FROM agg_adist GROUP BY grp";
    db.create_st("agg_adist_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_adist_st", q).await;

    db.execute("INSERT INTO agg_adist (grp, val) VALUES (1, 1000000000000.03)")
        .await;
    db.refresh_st("agg_adist_st").await;
    db.assert_st_matches_query("agg_adist_st", q).await;

    db.execute("DELETE FROM agg_adist WHERE id IN (SELECT id FROM agg_adist WHERE grp = 1 AND val = 1000000000000.01 LIMIT 2)")
            .await;
    db.refresh_st("agg_adist_st").await;
    db.assert_st_matches_query("agg_adist_st", q).await;
}

#[tokio::test]
async fn test_agg_distinct_bound_exceeded_falls_back_to_full() {
    for in_cte in [false, true] {
        let db = E2eDb::new().await.with_extension().await;
        db.execute("CREATE TABLE agg_dbound (id INT PRIMARY KEY, grp INT, val INT)")
            .await;
        db.execute("INSERT INTO agg_dbound VALUES (1, 1, 1), (2, 1, 2)")
            .await;

        let q = if in_cte {
            "WITH counts AS (SELECT grp, COUNT(DISTINCT val) AS uniq FROM agg_dbound GROUP BY grp) SELECT * FROM counts"
        } else {
            "SELECT grp, COUNT(DISTINCT val) AS uniq FROM agg_dbound GROUP BY grp"
        };
        db.create_st("agg_dbound_st", q, "1m", "DIFFERENTIAL").await;
        db.assert_st_matches_query("agg_dbound_st", q).await;

        db.execute("INSERT INTO agg_dbound VALUES (3, 1, 3)").await;
        db.try_execute_with_config(
            &["SET pg_trickle.distinct_agg_max_values_per_group = 2"],
            "SELECT pgtrickle.refresh_stream_table('agg_dbound_st')",
        )
        .await
        .expect("DISTINCT bound overflow should fall back to FULL");
        db.assert_st_matches_query("agg_dbound_st", q).await;

        let (action, full_fallback): (String, bool) = sqlx::query_as(
            "SELECT action, was_full_fallback FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE st.pgt_name = 'agg_dbound_st' \
         ORDER BY refresh_id DESC LIMIT 1",
        )
        .fetch_one(&db.pool)
        .await
        .expect("refresh history");
        assert_eq!(action, "FULL");
        assert!(full_fallback);
    }
}

#[tokio::test]
async fn test_agg_distinct_unsupported_forms_use_full_or_reject_diff() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_dunsupported (id INT PRIMARY KEY, grp INT, txt TEXT, val INT)")
        .await;
    db.execute("INSERT INTO agg_dunsupported VALUES (1, 1, 'a', 10), (2, 1, 'A', 20)")
        .await;

    let explicit = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(
                    'agg_dunsupported_diff_st',
                    $$SELECT grp, COUNT(DISTINCT txt) AS uniq FROM agg_dunsupported GROUP BY grp$$,
                    '1m',
                    refresh_mode => 'DIFFERENTIAL'
                )",
        )
        .await;
    assert!(
        explicit.is_err(),
        "collation-sensitive DISTINCT must reject explicit DIFF"
    );

    db.execute(
        "SELECT pgtrickle.create_stream_table(
                'agg_dunsupported_auto_st',
                $$SELECT grp, COUNT(DISTINCT txt) AS uniq FROM agg_dunsupported GROUP BY grp$$,
                '1m',
                refresh_mode => 'AUTO'
            )",
    )
    .await;
    let stored_mode: String = db
        .query_scalar(
            "SELECT refresh_mode FROM pgtrickle.pgt_stream_tables \
                 WHERE pgt_name = 'agg_dunsupported_auto_st'",
        )
        .await;
    assert_eq!(stored_mode, "FULL");
}

// ═══════════════════════════════════════════════════════════════════════
// String/Array aggregates
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_agg_array_agg_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_arr (id SERIAL PRIMARY KEY, grp TEXT, val TEXT)")
        .await;
    db.execute("INSERT INTO agg_arr (grp, val) VALUES ('a', 'x'), ('a', 'y'), ('b', 'z')")
        .await;

    let q = "SELECT grp, ARRAY_AGG(val ORDER BY val) AS vals FROM agg_arr GROUP BY grp";
    db.create_st("agg_arr_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_arr_st", q).await;

    db.execute("INSERT INTO agg_arr (grp, val) VALUES ('a', 'w')")
        .await;
    db.refresh_st("agg_arr_st").await;
    db.assert_st_matches_query("agg_arr_st", q).await;

    db.execute("DELETE FROM agg_arr WHERE val = 'x'").await;
    db.refresh_st("agg_arr_st").await;
    db.assert_st_matches_query("agg_arr_st", q).await;
}

#[tokio::test]
async fn test_agg_string_agg_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_str (id SERIAL PRIMARY KEY, grp TEXT, val TEXT)")
        .await;
    db.execute("INSERT INTO agg_str (grp, val) VALUES ('a', 'x'), ('a', 'y'), ('b', 'z')")
        .await;

    let q = "SELECT grp, STRING_AGG(val, ',' ORDER BY val) AS csv FROM agg_str GROUP BY grp";
    db.create_st("agg_str_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_str_st", q).await;

    db.execute("INSERT INTO agg_str (grp, val) VALUES ('a', 'w')")
        .await;
    db.refresh_st("agg_str_st").await;
    db.assert_st_matches_query("agg_str_st", q).await;

    db.execute("DELETE FROM agg_str WHERE val = 'y'").await;
    db.refresh_st("agg_str_st").await;
    db.assert_st_matches_query("agg_str_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Boolean aggregates
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_agg_bool_and_or_every_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_bool (id SERIAL PRIMARY KEY, grp TEXT, flag BOOLEAN)")
        .await;
    db.execute("INSERT INTO agg_bool (grp, flag) VALUES ('a', true), ('a', true), ('b', false)")
        .await;

    let q = "SELECT grp, BOOL_AND(flag) AS all_true, BOOL_OR(flag) AS any_true, \
             EVERY(flag) AS every_true FROM agg_bool GROUP BY grp";
    db.create_st("agg_bool_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_bool_st", q).await;

    db.execute("INSERT INTO agg_bool (grp, flag) VALUES ('a', false)")
        .await;
    db.refresh_st("agg_bool_st").await;
    db.assert_st_matches_query("agg_bool_st", q).await;

    db.execute("DELETE FROM agg_bool WHERE grp = 'a' AND flag = false")
        .await;
    db.refresh_st("agg_bool_st").await;
    db.assert_st_matches_query("agg_bool_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Bit aggregates
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_agg_bit_and_or_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_bit (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO agg_bit (grp, val) VALUES ('a', 7), ('a', 5), ('b', 3)")
        .await;

    let q = "SELECT grp, BIT_AND(val) AS band, BIT_OR(val) AS bor \
             FROM agg_bit GROUP BY grp";
    db.create_st("agg_bit_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_bit_st", q).await;

    db.execute("INSERT INTO agg_bit (grp, val) VALUES ('a', 4)")
        .await;
    db.refresh_st("agg_bit_st").await;
    db.assert_st_matches_query("agg_bit_st", q).await;

    db.execute("DELETE FROM agg_bit WHERE grp = 'a' AND val = 7")
        .await;
    db.refresh_st("agg_bit_st").await;
    db.assert_st_matches_query("agg_bit_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// JSON aggregates
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_agg_json_agg_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_json (id SERIAL PRIMARY KEY, grp TEXT, val TEXT)")
        .await;
    db.execute("INSERT INTO agg_json (grp, val) VALUES ('a', 'x'), ('a', 'y'), ('b', 'z')")
        .await;

    let q = "SELECT grp, JSON_AGG(val ORDER BY val) AS arr FROM agg_json GROUP BY grp";
    db.create_st("agg_json_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_json_st", q).await;

    db.execute("INSERT INTO agg_json (grp, val) VALUES ('a', 'w')")
        .await;
    db.refresh_st_with_retry("agg_json_st").await;
    db.assert_st_matches_query("agg_json_st", q).await;
}

#[tokio::test]
async fn test_agg_jsonb_agg_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_jsonb (id SERIAL PRIMARY KEY, grp TEXT, val TEXT)")
        .await;
    db.execute("INSERT INTO agg_jsonb (grp, val) VALUES ('a', 'x'), ('b', 'y')")
        .await;

    let q = "SELECT grp, JSONB_AGG(val ORDER BY val) AS arr FROM agg_jsonb GROUP BY grp";
    db.create_st("agg_jsonb_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_jsonb_st", q).await;

    db.execute("INSERT INTO agg_jsonb (grp, val) VALUES ('a', 'z')")
        .await;
    db.refresh_st("agg_jsonb_st").await;
    db.assert_st_matches_query("agg_jsonb_st", q).await;
}

#[tokio::test]
async fn test_agg_json_object_agg_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_jobj (id SERIAL PRIMARY KEY, grp TEXT, k TEXT, v TEXT)")
        .await;
    db.execute(
        "INSERT INTO agg_jobj (grp, k, v) VALUES \
         ('a', 'k1', 'v1'), ('a', 'k2', 'v2'), ('b', 'k3', 'v3')",
    )
    .await;

    let q = "SELECT grp, JSON_OBJECT_AGG(k, v) AS obj FROM agg_jobj GROUP BY grp";
    db.create_st("agg_jobj_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_jobj_st", q).await;

    db.execute("INSERT INTO agg_jobj (grp, k, v) VALUES ('a', 'k4', 'v4')")
        .await;
    db.refresh_st("agg_jobj_st").await;
    db.assert_st_matches_query("agg_jobj_st", q).await;
}

#[tokio::test]
async fn test_agg_jsonb_object_agg_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_jbobj (id SERIAL PRIMARY KEY, grp TEXT, k TEXT, v TEXT)")
        .await;
    db.execute("INSERT INTO agg_jbobj (grp, k, v) VALUES ('a', 'k1', 'v1'), ('b', 'k2', 'v2')")
        .await;

    let q = "SELECT grp, JSONB_OBJECT_AGG(k, v) AS obj FROM agg_jbobj GROUP BY grp";
    db.create_st("agg_jbobj_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_jbobj_st", q).await;

    db.execute("INSERT INTO agg_jbobj (grp, k, v) VALUES ('a', 'k3', 'v3')")
        .await;
    db.refresh_st("agg_jbobj_st").await;
    db.assert_st_matches_query("agg_jbobj_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Ordered-set aggregates: PERCENTILE_CONT, PERCENTILE_DISC, MODE
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_agg_percentile_cont_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_pct (id SERIAL PRIMARY KEY, grp TEXT, val NUMERIC)")
        .await;
    db.execute("INSERT INTO agg_pct (grp, val) VALUES ('a', 10), ('a', 20), ('a', 30), ('b', 50)")
        .await;

    let q = "SELECT grp, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) AS median \
             FROM agg_pct GROUP BY grp";
    db.create_st("agg_pct_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_pct_st", q).await;

    db.execute("INSERT INTO agg_pct (grp, val) VALUES ('a', 40)")
        .await;
    db.refresh_st("agg_pct_st").await;
    db.assert_st_matches_query("agg_pct_st", q).await;

    db.execute("DELETE FROM agg_pct WHERE val = 10").await;
    db.refresh_st("agg_pct_st").await;
    db.assert_st_matches_query("agg_pct_st", q).await;
}

#[tokio::test]
async fn test_agg_percentile_disc_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_pcd (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO agg_pcd (grp, val) VALUES ('a', 10), ('a', 20), ('a', 30)")
        .await;

    let q = "SELECT grp, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY val) AS med \
             FROM agg_pcd GROUP BY grp";
    db.create_st("agg_pcd_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_pcd_st", q).await;

    db.execute("INSERT INTO agg_pcd (grp, val) VALUES ('a', 15)")
        .await;
    db.refresh_st("agg_pcd_st").await;
    db.assert_st_matches_query("agg_pcd_st", q).await;
}

#[tokio::test]
async fn test_agg_mode_differential() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_mode (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute("INSERT INTO agg_mode (grp, val) VALUES ('a', 1), ('a', 1), ('a', 2), ('b', 3)")
        .await;

    let q = "SELECT grp, MODE() WITHIN GROUP (ORDER BY val) AS mode_val \
             FROM agg_mode GROUP BY grp";
    db.create_st("agg_mode_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_mode_st", q).await;

    // Change mode by adding more val=2 rows
    db.execute("INSERT INTO agg_mode (grp, val) VALUES ('a', 2), ('a', 2)")
        .await;
    db.refresh_st("agg_mode_st").await;
    db.assert_st_matches_query("agg_mode_st", q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// Mixed DML aggregate stress test
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_agg_mixed_dml_stress() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE agg_stress (id SERIAL PRIMARY KEY, grp TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO agg_stress (grp, val) VALUES \
         ('a', 10), ('a', 20), ('a', 30), ('b', 40), ('b', 50)",
    )
    .await;

    let q = "SELECT grp, SUM(val) AS total, COUNT(*) AS cnt, AVG(val) AS avg_val \
             FROM agg_stress GROUP BY grp";
    db.create_st("agg_stress_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("agg_stress_st", q).await;

    // Mixed: insert, update, delete
    db.execute("INSERT INTO agg_stress (grp, val) VALUES ('a', 5), ('c', 100)")
        .await;
    db.execute("UPDATE agg_stress SET val = 99 WHERE grp = 'b' AND val = 40")
        .await;
    db.execute("DELETE FROM agg_stress WHERE grp = 'a' AND val = 10")
        .await;
    db.refresh_st("agg_stress_st").await;
    db.assert_st_matches_query("agg_stress_st", q).await;

    // Another round
    db.execute("DELETE FROM agg_stress WHERE grp = 'c'").await;
    db.execute("INSERT INTO agg_stress (grp, val) VALUES ('b', 1)")
        .await;
    db.refresh_st("agg_stress_st").await;
    db.assert_st_matches_query("agg_stress_st", q).await;
}
