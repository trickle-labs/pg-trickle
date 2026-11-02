//! A42-15: Keyless multiset property test.
//!
//! Verifies that a keyless (no PRIMARY KEY) stream table maintains multiset
//! equivalence with the source query after random sequences of
//! INSERT/UPDATE/DELETE operations across multiple refresh cycles.
//!
//! A "multiset" here means that if the source has N rows with value V, the
//! stream table must also have exactly N rows with value V.
//!
//! Prerequisites: `just test-e2e`

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Keyless multiset invariant
// ═══════════════════════════════════════════════════════════════════════

/// A42-15: Keyless source — basic multiset equivalence across 5 cycles.
#[tokio::test]
async fn test_keyless_multiset_basic_invariant() {
    let db = E2eDb::new().await.with_extension().await;
    // No PRIMARY KEY — keyless source
    db.execute("CREATE TABLE km_src (v INT, label TEXT)").await;
    db.execute("INSERT INTO km_src VALUES (1,'a'),(2,'b'),(3,'c')")
        .await;

    let q = "SELECT v, label FROM km_src";
    // Note: creation emits a WARNING about keyless source — that is expected.
    db.create_st("km_st", q, "1m", "DIFFERENTIAL").await;
    db.assert_st_matches_query("km_st", q).await;

    // Cycle 1: Insert a duplicate row (same values as existing)
    db.execute("INSERT INTO km_src VALUES (1, 'a')").await;
    db.refresh_st("km_st").await;
    db.assert_st_matches_query("km_st", q).await;

    // Cycle 2: Delete one of the two duplicate rows
    db.execute("DELETE FROM km_src WHERE ctid = (SELECT ctid FROM km_src WHERE v = 1 LIMIT 1)")
        .await;
    db.refresh_st("km_st").await;
    db.assert_st_matches_query("km_st", q).await;

    // Cycle 3: Insert a new distinct row
    db.execute("INSERT INTO km_src VALUES (4, 'd')").await;
    db.refresh_st("km_st").await;
    db.assert_st_matches_query("km_st", q).await;

    // Cycle 4: Bulk update (changes values for all rows)
    db.execute("UPDATE km_src SET v = v + 10").await;
    db.refresh_st("km_st").await;
    db.assert_st_matches_query("km_st", q).await;

    // Cycle 5: Delete all rows
    db.execute("DELETE FROM km_src").await;
    db.refresh_st("km_st").await;
    db.assert_st_matches_query("km_st", q).await;
}

/// A42-15: Keyless multiset — many duplicate rows survive insert/delete rounds.
#[tokio::test]
async fn test_keyless_multiset_duplicate_rows() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE km_dup (x INT)").await;
    db.execute("INSERT INTO km_dup VALUES (1),(1),(1),(2),(2),(3)")
        .await;

    let q = "SELECT x FROM km_dup ORDER BY x";
    db.create_st("km_dup_st", q, "1m", "FULL").await; // Use FULL for keyless
    db.assert_st_matches_query("km_dup_st", "SELECT x FROM km_dup")
        .await;

    // Insert more duplicates
    db.execute("INSERT INTO km_dup VALUES (1),(1),(2)").await;
    db.refresh_st("km_dup_st").await;
    db.assert_st_matches_query("km_dup_st", "SELECT x FROM km_dup")
        .await;

    // Delete some (but not all) duplicates
    db.execute("DELETE FROM km_dup WHERE ctid IN (SELECT ctid FROM km_dup WHERE x = 1 LIMIT 2)")
        .await;
    db.refresh_st("km_dup_st").await;
    db.assert_st_matches_query("km_dup_st", "SELECT x FROM km_dup")
        .await;
}

/// Keyless differential scans must expand the signed multiplicity within one refresh window.
#[tokio::test]
async fn test_keyless_multiset_same_window_insert_delete_preserves_multiplicity() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE km_window (v INT, label TEXT)")
        .await;
    db.execute("INSERT INTO km_window VALUES (1,'same')").await;

    let query = "SELECT v,label FROM km_window";
    db.create_st("km_window_st", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("km_window_st", query).await;

    // Three identical inserts and one identical delete have net multiplicity +2.
    db.execute("INSERT INTO km_window VALUES (1,'same'),(1,'same'),(1,'same')")
        .await;
    db.execute(
        "DELETE FROM km_window WHERE ctid = (SELECT ctid FROM km_window \
         WHERE v = 1 AND label = 'same' LIMIT 1)",
    )
    .await;
    db.refresh_st("km_window_st").await;
    db.assert_st_matches_query("km_window_st", query).await;
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM km_window_st WHERE v=1 AND label='same'")
        .await;
    assert_eq!(count, 3, "the positive net count expands to two new rows");

    // Removing two of those identical rows must retract exactly two occurrences.
    db.execute(
        "DELETE FROM km_window WHERE ctid IN (SELECT ctid FROM km_window \
         WHERE v = 1 AND label = 'same' LIMIT 2)",
    )
    .await;
    db.refresh_st("km_window_st").await;
    db.assert_st_matches_query("km_window_st", query).await;
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM km_window_st WHERE v=1 AND label='same'")
        .await;
    assert_eq!(count, 1, "the negative net count retracts two rows");
}

/// A42-15: Keyless multiset — 10 random cycles of mixed operations.
///
/// Property: ∀ cycle: multiset(km_prop_st) == multiset(SELECT * FROM km_prop)
#[tokio::test]
async fn test_keyless_multiset_property_10_cycles() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE km_prop (val INT, cat TEXT)").await;
    db.execute("INSERT INTO km_prop VALUES (10,'A'),(20,'B'),(10,'A'),(30,'C')")
        .await;

    let q = "SELECT val, cat FROM km_prop";
    db.create_st("km_prop_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("km_prop_st", q).await;

    // 10 cycles of operations
    for cycle in 1_i32..=10 {
        match cycle % 3 {
            0 => {
                // Insert new rows (some duplicates of existing)
                db.execute(&format!(
                    "INSERT INTO km_prop VALUES ({v}, 'A'), ({v}, 'B')",
                    v = cycle * 5
                ))
                .await;
            }
            1 => {
                // Update all rows of one category
                let cat = if cycle % 2 == 0 { "A" } else { "B" };
                db.execute(&format!(
                    "UPDATE km_prop SET val = val + 1 WHERE cat = '{cat}'"
                ))
                .await;
            }
            _ => {
                // Delete a subset
                db.execute("DELETE FROM km_prop WHERE ctid IN (SELECT ctid FROM km_prop LIMIT 1)")
                    .await;
            }
        }
        db.refresh_st("km_prop_st").await;
        db.assert_st_matches_query("km_prop_st", q).await;
    }
}

/// A42-15: Keyless source with aggregate (COUNT) — full refresh mode.
#[tokio::test]
async fn test_keyless_aggregate_full_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE km_agg (cat TEXT, val INT)").await;
    db.execute("INSERT INTO km_agg VALUES ('X',1),('X',2),('X',1),('Y',5)")
        .await;

    let q = "SELECT cat, COUNT(*) AS cnt, SUM(val) AS total FROM km_agg GROUP BY cat";
    db.create_st("km_agg_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("km_agg_st", q).await;

    // Insert duplicate rows
    db.execute("INSERT INTO km_agg VALUES ('X',1),('Y',5)")
        .await;
    db.refresh_st("km_agg_st").await;
    db.assert_st_matches_query("km_agg_st", q).await;

    // Delete all Y rows
    db.execute("DELETE FROM km_agg WHERE cat = 'Y'").await;
    db.refresh_st("km_agg_st").await;
    db.assert_st_matches_query("km_agg_st", q).await;
}
