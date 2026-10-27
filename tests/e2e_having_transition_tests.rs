//! E2E tests for HAVING clause transition correctness (F25: G6.2).
//!
//! Validates that groups crossing the HAVING threshold in and out,
//! and rows migrating between groups, produce correct differential results.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

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
    db.refresh_st_until_converged("hv_mig_st").await;
    db.assert_st_matches_query("hv_mig_st", q).await;

    // Move it back
    db.execute("UPDATE hv_mig SET grp = 'a' WHERE val = 25")
        .await;
    db.refresh_st_until_converged("hv_mig_st").await;
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
