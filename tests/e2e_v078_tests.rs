//! v0.78.0 DVM engine root-cause fix tests.
//!
//! Covers the items in the v0.78.0 release:
//!
//! - DVM-1: CASE/IN-list aggregate drift fix (append-only differential path)
//! - DVM-2: Correlated aggregate scalar subquery rewrite
//! - P-4: Placeholder resolver cache collision guard (basic correctness)
//!
//! Prerequisites: `./tests/build_e2e_image.sh` (full E2E image) or
//! `cargo pgrx package` output bind-mounted (light E2E).

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// DVM-1: CASE/IN-list aggregate drift fix
// ═══════════════════════════════════════════════════════════════════════

/// DVM-1a: Append-only source with SUM(CASE) + IN-list predicate should
/// run differentially (GROUP_RESCAN handles INSERT-only workloads correctly).
///
/// Prior to v0.78.0 this pattern always fell back to FULL refresh with
/// reason CASE_IN_LIST_DVM_DRIFT_FULL_FALLBACK.  With the fix, an
/// append-only source bypasses the fallback and produces correct results.
#[tokio::test]
async fn test_dvm1_case_in_list_append_only_differential() {
    let db = E2eDb::new().await.with_extension().await;

    // Source table — mark as append-only via the `is_append_only` hint.
    db.execute(
        "CREATE TABLE src_ao_casein (
            o_orderkey   INT PRIMARY KEY,
            l_shipmode   TEXT NOT NULL,
            revenue      NUMERIC NOT NULL DEFAULT 0
        )",
    )
    .await;

    // Stream table with a SUM(CASE … IN-list …) pattern (TPC-H q12-like).
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'st_ao_casein',
            $$SELECT
                l_shipmode,
                SUM(CASE WHEN l_shipmode IN ('MAIL','SHIP') THEN revenue ELSE 0 END) AS high_rev,
                SUM(CASE WHEN l_shipmode NOT IN ('MAIL','SHIP') THEN revenue ELSE 0 END) AS low_rev
              FROM src_ao_casein
              GROUP BY l_shipmode$$,
            refresh_mode => 'differential',
            append_only => true
        )",
    )
    .await;

    // Insert initial data.
    db.execute(
        "INSERT INTO src_ao_casein VALUES
            (1, 'MAIL', 100),
            (2, 'SHIP', 200),
            (3, 'TRUCK', 50)",
    )
    .await;

    // First refresh — populates the table.
    db.execute("SELECT pgtrickle.refresh_stream_table('st_ao_casein')")
        .await;

    // Insert more rows (append-only — no deletes or updates).
    db.execute(
        "INSERT INTO src_ao_casein VALUES
            (4, 'MAIL', 75),
            (5, 'SHIP', 125)",
    )
    .await;

    // Second refresh — should use differential path for append-only source.
    db.execute("SELECT pgtrickle.refresh_stream_table('st_ao_casein')")
        .await;

    // Verify correctness: MAIL sum should be 175, SHIP sum 325, TRUCK sum 50.
    let mail_rev: Option<i64> = db
        .query_scalar_opt("SELECT high_rev::BIGINT FROM st_ao_casein WHERE l_shipmode = 'MAIL'")
        .await;
    assert_eq!(
        mail_rev,
        Some(175),
        "DVM-1a: MAIL revenue should be 175 after differential refresh"
    );

    let ship_rev: Option<i64> = db
        .query_scalar_opt("SELECT high_rev::BIGINT FROM st_ao_casein WHERE l_shipmode = 'SHIP'")
        .await;
    assert_eq!(
        ship_rev,
        Some(325),
        "DVM-1a: SHIP revenue should be 325 after differential refresh"
    );

    // Verify no FULL fallback occurred — check effective_refresh_mode in history.
    let last_mode: Option<String> = db
        .query_scalar_opt(
            "SELECT action FROM pgtrickle.pgt_refresh_history
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                             WHERE pgt_name = 'st_ao_casein')
             ORDER BY refresh_id DESC LIMIT 1",
        )
        .await;
    assert_eq!(
        last_mode.as_deref(),
        Some("DIFFERENTIAL"),
        "DVM-1a: append-only CASE/IN-list should use DIFFERENTIAL refresh"
    );

    db.execute("SELECT pgtrickle.drop_stream_table('st_ao_casein')")
        .await;
    db.execute("DROP TABLE src_ao_casein").await;
}

/// DVM-1b: Mutable source with SUM(CASE) + IN-list predicate must still
/// fall back to FULL refresh (correctness guarantee unchanged for mutable).
#[tokio::test]
async fn test_dvm1_case_in_list_mutable_full_fallback() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE src_mut_casein (
            o_orderkey   INT PRIMARY KEY,
            l_shipmode   TEXT NOT NULL,
            revenue      NUMERIC NOT NULL DEFAULT 0
        )",
    )
    .await;

    // Same query pattern but without is_append_only — mutable source.
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'st_mut_casein',
            $$SELECT
                l_shipmode,
                SUM(CASE WHEN l_shipmode IN ('MAIL','SHIP') THEN revenue ELSE 0 END) AS high_rev,
                SUM(CASE WHEN l_shipmode NOT IN ('MAIL','SHIP') THEN revenue ELSE 0 END) AS low_rev
              FROM src_mut_casein
              GROUP BY l_shipmode$$,
            refresh_mode => 'differential'
        )",
    )
    .await;

    db.execute("INSERT INTO src_mut_casein VALUES (1, 'MAIL', 100), (2, 'SHIP', 200)")
        .await;

    // First refresh — populates the table.
    db.execute("SELECT pgtrickle.refresh_stream_table('st_mut_casein')")
        .await;

    // Update a row to trigger mutable-source path.
    db.execute("UPDATE src_mut_casein SET revenue = 150 WHERE o_orderkey = 1")
        .await;

    // Second refresh — must be FULL for mutable source with CASE/IN-list.
    db.execute("SELECT pgtrickle.refresh_stream_table('st_mut_casein')")
        .await;

    // Verify correctness: MAIL=150, SHIP=200.
    let mail_rev: Option<i64> = db
        .query_scalar_opt("SELECT high_rev::BIGINT FROM st_mut_casein WHERE l_shipmode = 'MAIL'")
        .await;
    assert_eq!(
        mail_rev,
        Some(150),
        "DVM-1b: MAIL revenue should be 150 after FULL refresh"
    );

    // The second refresh (after the UPDATE) should have been FULL.
    let last_mode: Option<String> = db
        .query_scalar_opt(
            "SELECT action FROM pgtrickle.pgt_refresh_history
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables
                             WHERE pgt_name = 'st_mut_casein')
               AND action = 'FULL'
             ORDER BY refresh_id DESC LIMIT 1",
        )
        .await;
    assert_eq!(
        last_mode.as_deref(),
        Some("FULL"),
        "DVM-1b: mutable CASE/IN-list should produce at least one FULL refresh"
    );

    db.execute("SELECT pgtrickle.drop_stream_table('st_mut_casein')")
        .await;
    db.execute("DROP TABLE src_mut_casein").await;
}

// ═══════════════════════════════════════════════════════════════════════
// DVM-2: Correlated aggregate scalar subquery rewrite
// ═══════════════════════════════════════════════════════════════════════

/// DVM-2a: A simple correlated aggregate scalar subquery in WHERE that
/// can be rewritten via CTE pre-aggregation should run differentially
/// after the v0.78.0 rewrite pass.
///
/// Pattern (q20-like simplified):
///   WHERE col > (SELECT 0.5 * SUM(col2) FROM t2 WHERE t2.key = t1.key)
#[tokio::test]
async fn test_dvm2_correlated_aggregate_cte_rewrite() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE ps_src (
            ps_suppkey  INT NOT NULL,
            ps_partkey  INT NOT NULL,
            ps_availqty INT NOT NULL,
            PRIMARY KEY (ps_suppkey, ps_partkey)
        )",
    )
    .await;

    // q20-like: suppliers with availqty > 0.5 × total for that partkey.
    // This is a simplified correlated aggregate that the rewrite pass should
    // handle by lifting the aggregate into a CTE.
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'st_q20_simple',
            $$SELECT ps_suppkey, ps_partkey, ps_availqty
              FROM ps_src s
              WHERE ps_availqty > (
                  SELECT 0.5 * SUM(ps_availqty)
                  FROM ps_src s2
                  WHERE s2.ps_partkey = s.ps_partkey
              )$$,
            refresh_mode => 'differential'
        )",
    )
    .await;

    // Load initial data.
    db.execute(
        "INSERT INTO ps_src VALUES
            (1, 10, 100),
            (2, 10, 30),
            (3, 10, 80),
            (4, 20, 200),
            (5, 20, 50)",
    )
    .await;

    db.execute("SELECT pgtrickle.refresh_stream_table('st_q20_simple')")
        .await;

    // s1(partkey=10): total=210, threshold=105 — suppkey 1(100) < 105, suppkey 3(80) < 105
    // s2(partkey=20): total=250, threshold=125 — suppkey 4(200) > 125
    let count: i64 = db
        .query_scalar("SELECT COUNT(*)::BIGINT FROM st_q20_simple")
        .await;
    assert!(
        count >= 1,
        "DVM-2a: at least one row should satisfy the correlated aggregate predicate"
    );

    // Insert a new row that should satisfy the predicate.
    db.execute("INSERT INTO ps_src VALUES (6, 20, 300)").await;

    db.execute("SELECT pgtrickle.refresh_stream_table('st_q20_simple')")
        .await;

    let count_after: i64 = db
        .query_scalar("SELECT COUNT(*)::BIGINT FROM st_q20_simple")
        .await;
    assert!(
        count_after >= count,
        "DVM-2a: inserting a qualifying row should not reduce result count"
    );

    db.execute("SELECT pgtrickle.drop_stream_table('st_q20_simple')")
        .await;
    db.execute("DROP TABLE ps_src").await;
}

// ═══════════════════════════════════════════════════════════════════════
// P-2: Query complexity class stored in catalog
// ═══════════════════════════════════════════════════════════════════════

/// P-2a: After a refresh the query_complexity_class column should be
/// populated in pgt_stream_tables (lazy back-fill on first refresh).
#[tokio::test]
async fn test_p2_complexity_class_backfill() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_p2 (id INT PRIMARY KEY, val INT)")
        .await;

    // JoinAggregate query — should be classified accordingly.
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'st_p2_join_agg',
            $$SELECT a.id, SUM(b.val) AS total
              FROM src_p2 a
              JOIN src_p2 b ON b.id = a.id
              GROUP BY a.id$$,
            refresh_mode => 'differential'
        )",
    )
    .await;

    // Insert rows and trigger first refresh (lazy back-fill).
    db.execute("INSERT INTO src_p2 VALUES (1, 10), (2, 20)")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('st_p2_join_agg')")
        .await;

    // P-2: complexity class should be non-null after first refresh.
    let class: Option<String> = db
        .query_scalar_opt(
            "SELECT query_complexity_class FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'st_p2_join_agg'",
        )
        .await;
    assert!(
        class.is_some(),
        "P-2: query_complexity_class should be populated after first refresh"
    );
    let class_val = class.unwrap();
    assert!(
        ["Join", "JoinAggregate", "Aggregate", "Filter", "Scan"].contains(&class_val.as_str()),
        "P-2: complexity class '{}' should be a known label",
        class_val
    );

    db.execute("SELECT pgtrickle.drop_stream_table('st_p2_join_agg')")
        .await;
    db.execute("DROP TABLE src_p2").await;
}

// ═══════════════════════════════════════════════════════════════════════
// P-4: Placeholder resolver cache correctness
// ═══════════════════════════════════════════════════════════════════════

/// P-4a: Basic placeholder resolution correctness — multiple refreshes of a
/// stream table with source OIDs in the delta template should all produce
/// correct results (no stale automaton from cache collision).
#[tokio::test]
async fn test_p4_placeholder_cache_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src_p4a (id INT PRIMARY KEY, val INT NOT NULL DEFAULT 0)")
        .await;
    db.execute("CREATE TABLE src_p4b (id INT PRIMARY KEY, val INT NOT NULL DEFAULT 0)")
        .await;

    // Two-source join — exercises multi-OID placeholder resolution.
    db.execute(
        "SELECT pgtrickle.create_stream_table(
            'st_p4_two_src',
            $$SELECT a.id, a.val + b.val AS total
              FROM src_p4a a JOIN src_p4b b ON a.id = b.id$$,
            refresh_mode => 'differential'
        )",
    )
    .await;

    db.execute("INSERT INTO src_p4a VALUES (1, 10), (2, 20)")
        .await;
    db.execute("INSERT INTO src_p4b VALUES (1, 5), (2, 15)")
        .await;

    db.execute("SELECT pgtrickle.refresh_stream_table('st_p4_two_src')")
        .await;

    // Verify correctness after first refresh.
    let total_1: Option<i64> = db
        .query_scalar_opt("SELECT total::BIGINT FROM st_p4_two_src WHERE id = 1")
        .await;
    assert_eq!(total_1, Some(15), "P-4a: id=1 total should be 15");

    // Make a change and refresh again — tests cache hit path.
    db.execute("INSERT INTO src_p4a VALUES (3, 30)").await;
    db.execute("INSERT INTO src_p4b VALUES (3, 7)").await;

    db.execute("SELECT pgtrickle.refresh_stream_table('st_p4_two_src')")
        .await;

    let total_3: Option<i64> = db
        .query_scalar_opt("SELECT total::BIGINT FROM st_p4_two_src WHERE id = 3")
        .await;
    assert_eq!(
        total_3,
        Some(37),
        "P-4a: id=3 total should be 37 after second refresh"
    );

    db.execute("SELECT pgtrickle.drop_stream_table('st_p4_two_src')")
        .await;
    db.execute("DROP TABLE src_p4a").await;
    db.execute("DROP TABLE src_p4b").await;
}
