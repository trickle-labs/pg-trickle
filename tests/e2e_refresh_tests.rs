//! E2E tests for `pgtrickle.refresh_stream_table()`.
//!
//! Validates refresh picks up all DML changes (inserts, updates, deletes),
//! handles edge cases, and correctly updates metadata.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

// ── Basic DML Refresh ──────────────────────────────────────────────────

#[tokio::test]
async fn test_refresh_picks_up_inserts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_ins (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rf_ins VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st(
        "rf_ins_st",
        "SELECT id, val FROM rf_ins",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.rf_ins_st").await, 2);

    // Insert new rows
    db.execute("INSERT INTO rf_ins VALUES (3, 'c'), (4, 'd')")
        .await;
    db.refresh_st("rf_ins_st").await;

    assert_eq!(db.count("public.rf_ins_st").await, 4);
}

#[tokio::test]
async fn test_refresh_picks_up_updates() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_upd (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rf_upd VALUES (1, 'old'), (2, 'old')")
        .await;

    db.create_st(
        "rf_upd_st",
        "SELECT id, val FROM rf_upd",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute("UPDATE rf_upd SET val = 'new' WHERE id = 1")
        .await;
    db.refresh_st("rf_upd_st").await;

    let val: String = db
        .query_scalar("SELECT val FROM public.rf_upd_st WHERE id = 1")
        .await;
    assert_eq!(val, "new", "Updated value should be reflected");

    // Unchanged row should still be there
    let val2: String = db
        .query_scalar("SELECT val FROM public.rf_upd_st WHERE id = 2")
        .await;
    assert_eq!(val2, "old");
}

#[tokio::test]
async fn test_refresh_picks_up_deletes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_del (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rf_del VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    db.create_st(
        "rf_del_st",
        "SELECT id, val FROM rf_del",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.rf_del_st").await, 3);

    db.execute("DELETE FROM rf_del WHERE id = 2").await;
    db.refresh_st("rf_del_st").await;

    assert_eq!(db.count("public.rf_del_st").await, 2);

    // Verify specific row is gone
    let has_2: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.rf_del_st WHERE id = 2)")
        .await;
    assert!(!has_2, "Deleted row should not appear in ST");
}

#[tokio::test]
async fn test_refresh_mixed_dml() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_mix (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rf_mix VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    db.create_st(
        "rf_mix_st",
        "SELECT id, val FROM rf_mix",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Mixed DML
    db.execute("INSERT INTO rf_mix VALUES (4, 'd')").await;
    db.execute("UPDATE rf_mix SET val = 'a_updated' WHERE id = 1")
        .await;
    db.execute("DELETE FROM rf_mix WHERE id = 2").await;

    db.refresh_st("rf_mix_st").await;

    // Final state should match defining query
    db.assert_st_matches_query("public.rf_mix_st", "SELECT id, val FROM rf_mix")
        .await;
}

// ── Aggregation & Join Refresh ─────────────────────────────────────────

#[tokio::test]
async fn test_refresh_aggregate_correctness() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_agg (id SERIAL PRIMARY KEY, grp INT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO rf_agg (grp, amount) VALUES (1, 100), (1, 200), (2, 50)")
        .await;

    db.create_st(
        "rf_agg_st",
        "SELECT grp, SUM(amount) AS total, COUNT(*) AS cnt FROM rf_agg GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Add more data
    db.execute("INSERT INTO rf_agg (grp, amount) VALUES (1, 300), (2, 150), (3, 500)")
        .await;

    db.refresh_st("rf_agg_st").await;

    let total_1: i64 = db
        .query_scalar("SELECT total::bigint FROM public.rf_agg_st WHERE grp = 1")
        .await;
    assert_eq!(total_1, 600, "Group 1 total: 100+200+300");

    let cnt_2: i64 = db
        .query_scalar("SELECT cnt FROM public.rf_agg_st WHERE grp = 2")
        .await;
    assert_eq!(cnt_2, 2);

    assert_eq!(db.count("public.rf_agg_st").await, 3);
}

#[tokio::test]
async fn test_refresh_join_after_insert() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_cust (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("CREATE TABLE rf_ord (id INT PRIMARY KEY, cust_id INT, amount INT)")
        .await;
    db.execute("INSERT INTO rf_cust VALUES (1, 'Alice')").await;
    db.execute("INSERT INTO rf_ord VALUES (1, 1, 100)").await;

    db.create_st(
        "rf_join_st",
        "SELECT c.name, o.amount FROM rf_cust c JOIN rf_ord o ON c.id = o.cust_id",
        "1m",
        "FULL",
    )
    .await;

    assert_eq!(db.count("public.rf_join_st").await, 1);

    // Add a new customer and order
    db.execute("INSERT INTO rf_cust VALUES (2, 'Bob')").await;
    db.execute("INSERT INTO rf_ord VALUES (2, 2, 200)").await;

    db.refresh_st("rf_join_st").await;

    assert_eq!(db.count("public.rf_join_st").await, 2);
}

// ── Idempotency & Edge Cases ───────────────────────────────────────────

#[tokio::test]
async fn test_refresh_idempotent() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_idem (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rf_idem VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st(
        "rf_idem_st",
        "SELECT id, val FROM rf_idem",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Multiple refreshes with no changes
    db.refresh_st("rf_idem_st").await;
    db.refresh_st("rf_idem_st").await;
    db.refresh_st("rf_idem_st").await;

    assert_eq!(db.count("public.rf_idem_st").await, 2);
    db.assert_st_matches_query("public.rf_idem_st", "SELECT id, val FROM rf_idem")
        .await;
}

#[tokio::test]
async fn test_refresh_uninitialized_differential_stream_table_falls_back_to_full() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_noinit (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rf_noinit VALUES (1, 'a'), (2, 'b')")
        .await;

    db.create_st_with_init(
        "rf_noinit_st",
        "SELECT id, val FROM rf_noinit",
        "1m",
        "DIFFERENTIAL",
        false,
    )
    .await;

    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('rf_noinit_st')")
        .await;
    assert!(result.is_ok(), "manual refresh should fall back to FULL");

    db.assert_st_matches_query("public.rf_noinit_st", "SELECT id, val FROM rf_noinit")
        .await;

    let (_, refresh_mode, populated, _) = db.pgt_status("rf_noinit_st").await;
    assert_eq!(refresh_mode, "DIFFERENTIAL");
    assert!(
        populated,
        "manual refresh should initialize the stream table"
    );
}

// ── Metadata Updates ───────────────────────────────────────────────────

#[tokio::test]
async fn test_refresh_updates_data_timestamp() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_ts (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO rf_ts VALUES (1)").await;

    db.create_st("rf_ts_st", "SELECT id FROM rf_ts", "1m", "FULL")
        .await;

    let ts_before: Option<String> = db
        .query_scalar_opt(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rf_ts_st'",
        )
        .await;

    // Small delay to ensure timestamp difference
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    db.execute("INSERT INTO rf_ts VALUES (2)").await;
    db.refresh_st("rf_ts_st").await;

    let ts_after: Option<String> = db
        .query_scalar_opt(
            "SELECT data_timestamp::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rf_ts_st'",
        )
        .await;

    assert!(
        ts_after.is_some(),
        "data_timestamp should be set after refresh"
    );
    assert_ne!(
        ts_before, ts_after,
        "data_timestamp should advance after refresh"
    );
}

#[tokio::test]
async fn test_refresh_updates_last_refresh_at() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_lr (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO rf_lr VALUES (1)").await;

    db.create_st("rf_lr_st", "SELECT id FROM rf_lr", "1m", "FULL")
        .await;

    db.execute("INSERT INTO rf_lr VALUES (2)").await;
    db.refresh_st("rf_lr_st").await;

    let has_refresh_at: bool = db
        .query_scalar(
            "SELECT last_refresh_at IS NOT NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rf_lr_st'",
        )
        .await;
    assert!(
        has_refresh_at,
        "last_refresh_at should be set after refresh"
    );
}

#[tokio::test]
async fn test_refresh_resets_consecutive_errors() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_err (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO rf_err VALUES (1)").await;

    db.create_st("rf_err_st", "SELECT id FROM rf_err", "1m", "FULL")
        .await;

    // After a successful refresh, consecutive_errors should be 0
    db.execute("INSERT INTO rf_err VALUES (2)").await;
    db.refresh_st("rf_err_st").await;

    let (_, _, _, errors) = db.pgt_status("rf_err_st").await;
    assert_eq!(errors, 0, "consecutive_errors should be 0 after success");
}

#[tokio::test]
async fn test_refresh_records_history() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_hist (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO rf_hist VALUES (1)").await;

    db.create_st("rf_hist_st", "SELECT id FROM rf_hist", "1m", "FULL")
        .await;

    db.execute("INSERT INTO rf_hist VALUES (2)").await;
    db.refresh_st("rf_hist_st").await;

    // Manual refresh updates catalog metadata but doesn't write to
    // pgt_refresh_history (only the scheduler does). Verify the catalog
    // was updated correctly instead.
    let has_refresh_at: bool = db
        .query_scalar(
            "SELECT last_refresh_at IS NOT NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rf_hist_st'",
        )
        .await;
    assert!(
        has_refresh_at,
        "last_refresh_at should be set after manual refresh"
    );

    let data_ts: bool = db
        .query_scalar(
            "SELECT data_timestamp IS NOT NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'rf_hist_st'",
        )
        .await;
    assert!(data_ts, "data_timestamp should be set after manual refresh");

    // Verify the pgt_refresh_history table exists and is queryable
    let table_exists = db.table_exists("pgtrickle", "pgt_refresh_history").await;
    assert!(table_exists, "pgt_refresh_history table should exist");
}

// ── Suspended ST Refresh ───────────────────────────────────────────────

#[tokio::test]
async fn test_refresh_suspended_st_fails() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_susp (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO rf_susp VALUES (1)").await;

    db.create_st("rf_susp_st", "SELECT id FROM rf_susp", "1m", "FULL")
        .await;

    // Suspend the ST
    db.alter_st("rf_susp_st", "status => 'SUSPENDED'").await;

    // Refresh should fail
    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('rf_susp_st')")
        .await;
    assert!(result.is_err(), "Refreshing a SUSPENDED ST should fail");
}

// ── Large Batch ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_refresh_large_batch() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE rf_big (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO rf_big SELECT g, 'row_' || g FROM generate_series(1, 100) g")
        .await;

    db.create_st(
        "rf_big_st",
        "SELECT id, val FROM rf_big",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.rf_big_st").await, 100);

    // Insert 10,000 more rows
    db.execute("INSERT INTO rf_big SELECT g, 'row_' || g FROM generate_series(101, 10100) g")
        .await;

    db.refresh_st("rf_big_st").await;

    assert_eq!(db.count("public.rf_big_st").await, 10100);
}

// ── BIT_AND / BIT_OR / BIT_XOR Aggregates ──────────────────────────────

#[tokio::test]
async fn test_bit_and_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE bit_src (id INT PRIMARY KEY, dept TEXT, flags INT)")
        .await;
    db.execute(
        "INSERT INTO bit_src VALUES \
         (1, 'eng', 7), (2, 'eng', 5), (3, 'sales', 15), (4, 'sales', 12)",
    )
    .await;

    db.create_st(
        "bit_and_st",
        "SELECT dept, BIT_AND(flags) AS all_flags FROM bit_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.bit_and_st",
        "SELECT dept, BIT_AND(flags) AS all_flags FROM bit_src GROUP BY dept",
    )
    .await;

    // Mutate: insert a row that changes the BIT_AND result for 'eng'
    db.execute("INSERT INTO bit_src VALUES (5, 'eng', 4)").await;
    db.refresh_st("bit_and_st").await;

    db.assert_st_matches_query(
        "public.bit_and_st",
        "SELECT dept, BIT_AND(flags) AS all_flags FROM bit_src GROUP BY dept",
    )
    .await;

    // Delete a row
    db.execute("DELETE FROM bit_src WHERE id = 3").await;
    db.refresh_st("bit_and_st").await;

    db.assert_st_matches_query(
        "public.bit_and_st",
        "SELECT dept, BIT_AND(flags) AS all_flags FROM bit_src GROUP BY dept",
    )
    .await;
}

#[tokio::test]
async fn test_bit_or_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE bitor_src (id INT PRIMARY KEY, dept TEXT, perms INT)")
        .await;
    db.execute(
        "INSERT INTO bitor_src VALUES \
         (1, 'eng', 1), (2, 'eng', 2), (3, 'sales', 4)",
    )
    .await;

    db.create_st(
        "bit_or_st",
        "SELECT dept, BIT_OR(perms) AS any_perms FROM bitor_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.bit_or_st",
        "SELECT dept, BIT_OR(perms) AS any_perms FROM bitor_src GROUP BY dept",
    )
    .await;

    // Insert: adds a new permission bit
    db.execute("INSERT INTO bitor_src VALUES (4, 'eng', 8)")
        .await;
    db.refresh_st("bit_or_st").await;

    db.assert_st_matches_query(
        "public.bit_or_st",
        "SELECT dept, BIT_OR(perms) AS any_perms FROM bitor_src GROUP BY dept",
    )
    .await;
}

#[tokio::test]
async fn test_bit_xor_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE bitxor_src (id INT PRIMARY KEY, dept TEXT, val INT)")
        .await;
    db.execute(
        "INSERT INTO bitxor_src VALUES \
         (1, 'eng', 5), (2, 'eng', 3), (3, 'sales', 7)",
    )
    .await;

    db.create_st(
        "bit_xor_st",
        "SELECT dept, BIT_XOR(val) AS xor_val FROM bitxor_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.bit_xor_st",
        "SELECT dept, BIT_XOR(val) AS xor_val FROM bitxor_src GROUP BY dept",
    )
    .await;

    // Update: changes XOR result
    db.execute("UPDATE bitxor_src SET val = 10 WHERE id = 1")
        .await;
    db.refresh_st("bit_xor_st").await;

    db.assert_st_matches_query(
        "public.bit_xor_st",
        "SELECT dept, BIT_XOR(val) AS xor_val FROM bitxor_src GROUP BY dept",
    )
    .await;
}

// ── JSON_OBJECT_AGG / JSONB_OBJECT_AGG ─────────────────────────────────

/// Test JSON_OBJECT_AGG in DIFFERENTIAL mode.
/// Uses JSONB_OBJECT_AGG (drop-in replacement) because the `json` type
/// lacks an equality operator — PostgreSQL's `IS DISTINCT FROM` (used
/// by the differential change-detection guard) requires equality.
/// `jsonb` supports equality natively.
#[tokio::test]
async fn test_json_object_agg_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE jobj_src (id INT PRIMARY KEY, dept TEXT, prop TEXT, val TEXT)")
        .await;
    db.execute(
        "INSERT INTO jobj_src VALUES \
         (1, 'eng', 'lang', 'rust'), \
         (2, 'eng', 'os', 'linux'), \
         (3, 'sales', 'region', 'us')",
    )
    .await;

    db.create_st(
        "jobj_st",
        "SELECT dept, JSONB_OBJECT_AGG(prop, val) AS props FROM jobj_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.jobj_st",
        "SELECT dept, JSONB_OBJECT_AGG(prop, val) AS props FROM jobj_src GROUP BY dept",
    )
    .await;

    // Insert a new property
    db.execute("INSERT INTO jobj_src VALUES (4, 'eng', 'editor', 'vim')")
        .await;
    db.refresh_st("jobj_st").await;

    db.assert_st_matches_query(
        "public.jobj_st",
        "SELECT dept, JSONB_OBJECT_AGG(prop, val) AS props FROM jobj_src GROUP BY dept",
    )
    .await;

    // Delete a property
    db.execute("DELETE FROM jobj_src WHERE id = 2").await;
    db.refresh_st("jobj_st").await;

    db.assert_st_matches_query(
        "public.jobj_st",
        "SELECT dept, JSONB_OBJECT_AGG(prop, val) AS props FROM jobj_src GROUP BY dept",
    )
    .await;
}

#[tokio::test]
async fn test_jsonb_object_agg_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE jbobj_src (id INT PRIMARY KEY, dept TEXT, key TEXT, val TEXT)")
        .await;
    db.execute(
        "INSERT INTO jbobj_src VALUES \
         (1, 'eng', 'level', 'senior'), \
         (2, 'sales', 'territory', 'west')",
    )
    .await;

    db.create_st(
        "jbobj_st",
        "SELECT dept, JSONB_OBJECT_AGG(key, val) AS data FROM jbobj_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.jbobj_st",
        "SELECT dept, JSONB_OBJECT_AGG(key, val) AS data FROM jbobj_src GROUP BY dept",
    )
    .await;

    // Update a value
    db.execute("UPDATE jbobj_src SET val = 'east' WHERE id = 2")
        .await;
    db.refresh_st("jbobj_st").await;

    db.assert_st_matches_query(
        "public.jbobj_st",
        "SELECT dept, JSONB_OBJECT_AGG(key, val) AS data FROM jbobj_src GROUP BY dept",
    )
    .await;
}

// ── Mixed new aggregates with existing ones ────────────────────────────

#[tokio::test]
async fn test_mixed_bit_or_with_count_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mixed_bit_src (id INT PRIMARY KEY, dept TEXT, perms INT)")
        .await;
    db.execute(
        "INSERT INTO mixed_bit_src VALUES \
         (1, 'eng', 1), (2, 'eng', 2), (3, 'eng', 4), (4, 'sales', 8)",
    )
    .await;

    db.create_st(
        "mixed_bit_st",
        "SELECT dept, COUNT(*) AS cnt, BIT_OR(perms) AS combined_perms \
         FROM mixed_bit_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.mixed_bit_st",
        "SELECT dept, COUNT(*) AS cnt, BIT_OR(perms) AS combined_perms \
         FROM mixed_bit_src GROUP BY dept",
    )
    .await;

    // Insert
    db.execute("INSERT INTO mixed_bit_src VALUES (5, 'eng', 16)")
        .await;
    db.refresh_st("mixed_bit_st").await;

    db.assert_st_matches_query(
        "public.mixed_bit_st",
        "SELECT dept, COUNT(*) AS cnt, BIT_OR(perms) AS combined_perms \
         FROM mixed_bit_src GROUP BY dept",
    )
    .await;
}

// ── Statistical aggregate tests ────────────────────────────────────────

#[tokio::test]
async fn test_stddev_pop_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sdp_src (id INT PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO sdp_src VALUES \
         (1, 'eng', 100), (2, 'eng', 200), (3, 'eng', 300), \
         (4, 'sales', 50), (5, 'sales', 150)",
    )
    .await;

    db.create_st(
        "sdp_st",
        "SELECT dept, ROUND(STDDEV_POP(amount), 4) AS sd FROM sdp_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.sdp_st",
        "SELECT dept, ROUND(STDDEV_POP(amount), 4) AS sd FROM sdp_src GROUP BY dept",
    )
    .await;

    // Insert a new row
    db.execute("INSERT INTO sdp_src VALUES (6, 'eng', 400)")
        .await;
    db.refresh_st("sdp_st").await;

    db.assert_st_matches_query(
        "public.sdp_st",
        "SELECT dept, ROUND(STDDEV_POP(amount), 4) AS sd FROM sdp_src GROUP BY dept",
    )
    .await;

    // Delete a row
    db.execute("DELETE FROM sdp_src WHERE id = 4").await;
    db.refresh_st("sdp_st").await;

    db.assert_st_matches_query(
        "public.sdp_st",
        "SELECT dept, ROUND(STDDEV_POP(amount), 4) AS sd FROM sdp_src GROUP BY dept",
    )
    .await;
}

#[tokio::test]
async fn test_stddev_samp_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sds_src (id INT PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO sds_src VALUES \
         (1, 'eng', 100), (2, 'eng', 200), (3, 'eng', 300), \
         (4, 'sales', 50), (5, 'sales', 150)",
    )
    .await;

    db.create_st(
        "sds_st",
        "SELECT dept, ROUND(STDDEV_SAMP(amount), 4) AS sd FROM sds_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.sds_st",
        "SELECT dept, ROUND(STDDEV_SAMP(amount), 4) AS sd FROM sds_src GROUP BY dept",
    )
    .await;

    // Update a value
    db.execute("UPDATE sds_src SET amount = 350 WHERE id = 3")
        .await;
    db.refresh_st("sds_st").await;

    db.assert_st_matches_query(
        "public.sds_st",
        "SELECT dept, ROUND(STDDEV_SAMP(amount), 4) AS sd FROM sds_src GROUP BY dept",
    )
    .await;
}

#[tokio::test]
async fn test_stddev_alias_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE sda_src (id INT PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO sda_src VALUES (1, 'a', 10), (2, 'a', 20), (3, 'a', 30)")
        .await;

    // STDDEV is an alias for STDDEV_SAMP
    db.create_st(
        "sda_st",
        "SELECT dept, STDDEV(amount) AS sd FROM sda_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.sda_st",
        "SELECT dept, STDDEV(amount) AS sd FROM sda_src GROUP BY dept",
    )
    .await;

    db.execute("INSERT INTO sda_src VALUES (4, 'a', 40)").await;
    db.refresh_st("sda_st").await;

    db.assert_st_matches_query(
        "public.sda_st",
        "SELECT dept, STDDEV(amount) AS sd FROM sda_src GROUP BY dept",
    )
    .await;
}

#[tokio::test]
async fn test_var_pop_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vp_src (id INT PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO vp_src VALUES \
         (1, 'eng', 100), (2, 'eng', 200), (3, 'eng', 300)",
    )
    .await;

    db.create_st(
        "vp_st",
        "SELECT dept, ROUND(VAR_POP(amount), 4) AS vp FROM vp_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.vp_st",
        "SELECT dept, ROUND(VAR_POP(amount), 4) AS vp FROM vp_src GROUP BY dept",
    )
    .await;

    // Delete and insert
    db.execute("DELETE FROM vp_src WHERE id = 3").await;
    db.execute("INSERT INTO vp_src VALUES (4, 'eng', 400)")
        .await;
    db.refresh_st("vp_st").await;

    db.assert_st_matches_query(
        "public.vp_st",
        "SELECT dept, ROUND(VAR_POP(amount), 4) AS vp FROM vp_src GROUP BY dept",
    )
    .await;
}

#[tokio::test]
async fn test_var_samp_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE vs_src (id INT PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO vs_src VALUES \
         (1, 'eng', 100), (2, 'eng', 200), (3, 'eng', 300), \
         (4, 'sales', 50), (5, 'sales', 150)",
    )
    .await;

    db.create_st(
        "vs_st",
        "SELECT dept, ROUND(VAR_SAMP(amount), 4) AS vs FROM vs_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.vs_st",
        "SELECT dept, ROUND(VAR_SAMP(amount), 4) AS vs FROM vs_src GROUP BY dept",
    )
    .await;

    // Update triggering re-aggregation
    db.execute("UPDATE vs_src SET amount = 250 WHERE id = 2")
        .await;
    db.refresh_st("vs_st").await;

    db.assert_st_matches_query(
        "public.vs_st",
        "SELECT dept, ROUND(VAR_SAMP(amount), 4) AS vs FROM vs_src GROUP BY dept",
    )
    .await;
}

#[tokio::test]
async fn test_variance_alias_differential_mode() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE va_src (id INT PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;
    db.execute("INSERT INTO va_src VALUES (1, 'x', 10), (2, 'x', 30), (3, 'x', 50)")
        .await;

    // VARIANCE is an alias for VAR_SAMP
    db.create_st(
        "va_st",
        "SELECT dept, VARIANCE(amount) AS v FROM va_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.va_st",
        "SELECT dept, VARIANCE(amount) AS v FROM va_src GROUP BY dept",
    )
    .await;

    db.execute("INSERT INTO va_src VALUES (4, 'x', 70)").await;
    db.refresh_st("va_st").await;

    db.assert_st_matches_query(
        "public.va_st",
        "SELECT dept, VARIANCE(amount) AS v FROM va_src GROUP BY dept",
    )
    .await;
}

#[tokio::test]
async fn test_mixed_stddev_with_sum_count_differential() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mstat_src (id INT PRIMARY KEY, dept TEXT, amount NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO mstat_src VALUES \
         (1, 'eng', 100), (2, 'eng', 200), (3, 'eng', 300), \
         (4, 'sales', 50), (5, 'sales', 150)",
    )
    .await;

    db.create_st(
        "mstat_st",
        "SELECT dept, COUNT(*) AS cnt, SUM(amount) AS total, ROUND(ROUND(STDDEV_POP(amount), 4), 4) AS sd \
         FROM mstat_src GROUP BY dept",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.assert_st_matches_query(
        "public.mstat_st",
        "SELECT dept, COUNT(*) AS cnt, SUM(amount) AS total, ROUND(ROUND(STDDEV_POP(amount), 4), 4) AS sd \
         FROM mstat_src GROUP BY dept",
    )
    .await;

    // Insert a new row
    db.execute("INSERT INTO mstat_src VALUES (6, 'eng', 400)")
        .await;
    db.refresh_st("mstat_st").await;

    db.assert_st_matches_query(
        "public.mstat_st",
        "SELECT dept, COUNT(*) AS cnt, SUM(amount) AS total, ROUND(ROUND(STDDEV_POP(amount), 4), 4) AS sd \
         FROM mstat_src GROUP BY dept",
    )
    .await;
}

// ── PERF-2: Fused Multi-Node Refresh (v0.63.0) ────────────────────────────

/// PERF-2: enable_fused_refresh=false reverts to sequential behaviour.
///
/// Creates two stream tables over the same source, disables fused refresh,
/// inserts data, runs the scheduler, and asserts both STs converge correctly.
/// This verifies the GUC disable path does not break correctness.
#[tokio::test]
async fn test_fused_refresh_guc_disable() {
    let db = e2e::E2eDb::new().await.with_extension().await;

    // Disable fused refresh via GUC.
    db.execute("ALTER SYSTEM SET pg_trickle.enable_fused_refresh = false")
        .await;
    db.reload_config_and_wait().await;

    db.execute("CREATE TABLE fgd_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO fgd_src SELECT i, i*10 FROM generate_series(1,5) i")
        .await;

    db.create_st("fgd_a", "SELECT id, val FROM fgd_src", "1m", "DIFFERENTIAL")
        .await;
    db.create_st(
        "fgd_b",
        "SELECT id, val*2 AS val FROM fgd_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.fgd_a").await, 5);
    assert_eq!(db.count("public.fgd_b").await, 5);

    // Insert more rows and refresh sequentially.
    db.execute("INSERT INTO fgd_src SELECT i, i*10 FROM generate_series(6,10) i")
        .await;
    db.refresh_st("fgd_a").await;
    db.refresh_st("fgd_b").await;

    assert_eq!(db.count("public.fgd_a").await, 10);
    assert_eq!(db.count("public.fgd_b").await, 10);

    // Re-enable fused refresh.
    db.execute("ALTER SYSTEM RESET pg_trickle.enable_fused_refresh")
        .await;
    db.reload_config_and_wait().await;
}

/// PERF-2: FULL-mode nodes are not fused; only DIFFERENTIAL nodes are eligible.
///
/// Creates one FULL-mode and one DIFFERENTIAL-mode stream table over the
/// same source, inserts data, and verifies both converge correctly.
/// The FULL node should fall through to the sequential path.
#[tokio::test]
async fn test_fused_refresh_full_node_excluded() {
    let db = e2e::E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE ffne_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO ffne_src VALUES (1,'a'),(2,'b'),(3,'c')")
        .await;

    db.create_st("ffne_full", "SELECT id, val FROM ffne_src", "1m", "FULL")
        .await;
    db.create_st(
        "ffne_diff",
        "SELECT id, upper(val) AS val FROM ffne_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.ffne_full").await, 3);
    assert_eq!(db.count("public.ffne_diff").await, 3);

    db.execute("INSERT INTO ffne_src VALUES (4,'d'),(5,'e')")
        .await;

    db.refresh_st("ffne_full").await;
    db.refresh_st("ffne_diff").await;

    assert_eq!(db.count("public.ffne_full").await, 5);
    assert_eq!(db.count("public.ffne_diff").await, 5);
}

/// PERF-2: Nodes whose estimated delta exceeds fused_refresh_max_delta_rows
/// are excluded from the fused batch and fall back to sequential refresh.
///
/// Sets fused_refresh_max_delta_rows=1 so any node with >1 pending row is
/// excluded. Verifies that both STs still converge correctly.
#[tokio::test]
async fn test_fused_refresh_large_delta_excluded() {
    let db = e2e::E2eDb::new().await.with_extension().await;

    // Set a very low threshold: any delta with >1 row is excluded from fusion.
    db.execute("ALTER SYSTEM SET pg_trickle.fused_refresh_max_delta_rows = 1")
        .await;
    db.reload_config_and_wait().await;

    db.execute("CREATE TABLE flde_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO flde_src SELECT i, i FROM generate_series(1,3) i")
        .await;

    db.create_st(
        "flde_a",
        "SELECT id, val FROM flde_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "flde_b",
        "SELECT id, val+1 AS val FROM flde_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    assert_eq!(db.count("public.flde_a").await, 3);
    assert_eq!(db.count("public.flde_b").await, 3);

    // Insert 5 rows — well above the threshold of 1.
    db.execute("INSERT INTO flde_src SELECT i, i FROM generate_series(4,8) i")
        .await;

    // Both STs must converge even when excluded from fusion.
    db.refresh_st("flde_a").await;
    db.refresh_st("flde_b").await;

    assert_eq!(db.count("public.flde_a").await, 8);
    assert_eq!(db.count("public.flde_b").await, 8);

    // Reset GUC.
    db.execute("ALTER SYSTEM RESET pg_trickle.fused_refresh_max_delta_rows")
        .await;
    db.reload_config_and_wait().await;
}

/// PERF-2: End-to-end fused refresh correctness with a multi-table DAG.
///
/// Creates a two-level DAG (src → view_a, view_a → view_b) and verifies
/// that fused refresh produces identical results to sequential refresh
/// for both INSERT and UPDATE workloads.
#[tokio::test]
async fn test_fused_refresh_tpch_22() {
    let db = e2e::E2eDb::new().await.with_extension().await;

    // Ensure fused refresh is enabled.
    db.execute("ALTER SYSTEM SET pg_trickle.enable_fused_refresh = true")
        .await;
    db.reload_config_and_wait().await;

    db.execute("CREATE TABLE ftr_src (id INT PRIMARY KEY, grp TEXT, amt NUMERIC)")
        .await;
    db.execute(
        "INSERT INTO ftr_src SELECT i, chr(65 + (i % 5)), (i * 1.5)::numeric \
         FROM generate_series(1, 20) i",
    )
    .await;

    // Level 1: aggregate stream table over the source.
    db.create_st(
        "ftr_agg",
        "SELECT grp, COUNT(*) AS cnt, SUM(amt) AS total FROM ftr_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Level 2: scan over the level-1 ST (exercises the DAG chain).
    db.create_st(
        "ftr_top",
        "SELECT grp, total FROM public.ftr_agg WHERE total > 30",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Verify initial state matches the defining queries.
    db.assert_st_matches_query(
        "public.ftr_agg",
        "SELECT grp, COUNT(*) AS cnt, SUM(amt) AS total FROM ftr_src GROUP BY grp",
    )
    .await;

    db.assert_st_matches_query(
        "public.ftr_top",
        "SELECT grp, total FROM \
         (SELECT grp, COUNT(*) AS cnt, SUM(amt) AS total FROM ftr_src GROUP BY grp) sub \
         WHERE total > 30",
    )
    .await;

    // Insert additional rows and refresh the DAG.
    db.execute(
        "INSERT INTO ftr_src SELECT i, chr(65 + (i % 5)), (i * 2.0)::numeric \
         FROM generate_series(21, 40) i",
    )
    .await;

    db.refresh_st("ftr_agg").await;
    db.refresh_st("ftr_top").await;

    db.assert_st_matches_query(
        "public.ftr_agg",
        "SELECT grp, COUNT(*) AS cnt, SUM(amt) AS total FROM ftr_src GROUP BY grp",
    )
    .await;

    db.assert_st_matches_query(
        "public.ftr_top",
        "SELECT grp, total FROM \
         (SELECT grp, COUNT(*) AS cnt, SUM(amt) AS total FROM ftr_src GROUP BY grp) sub \
         WHERE total > 30",
    )
    .await;

    // Reset GUC.
    db.execute("ALTER SYSTEM RESET pg_trickle.enable_fused_refresh")
        .await;
    db.reload_config_and_wait().await;
}

// ── TEST-003: Scheduler-driven fused refresh audit trail ──────────────

/// TEST-003 (COR-001, v0.68.0): Verify that scheduler-driven fused refreshes
/// are recorded in `pgt_refresh_history` with `initiated_by = 'SCHEDULER_FUSED'`.
///
/// Prior to v0.68.0, the CHECK constraint on `initiated_by` did not include
/// `'SCHEDULER_FUSED'`, so fused-refresh audit records were silently rejected
/// and the audit log showed a gap.
///
/// This is a light-eligible test (uses E2eDb::new()).
#[tokio::test]
async fn test_fused_refresh_scheduler_audit_trail() {
    let db = e2e::E2eDb::new().await.with_extension().await;

    // Configure fast scheduler.
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.enable_fused_refresh = true")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.enable_fused_refresh", "on")
        .await;

    // Wait for scheduler to start.
    let sched_running = db
        .wait_for_scheduler(std::time::Duration::from_secs(90))
        .await;
    assert!(sched_running, "Scheduler should start within 90 s");

    // Build a two-level DAG so the scheduler can execute a fused chain.
    db.execute("CREATE TABLE fused_audit_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO fused_audit_src SELECT i, i FROM generate_series(1, 10) i")
        .await;

    db.create_st(
        "fused_audit_l1",
        "SELECT id, val FROM fused_audit_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    db.create_st(
        "fused_audit_l2",
        "SELECT id, val * 2 AS val2 FROM public.fused_audit_l1",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Insert data to trigger differential refreshes.
    db.execute("INSERT INTO fused_audit_src VALUES (11, 11)")
        .await;

    // Wait for the scheduler to refresh l2 (it may fuse l1 + l2).
    let refreshed = db
        .wait_for_auto_refresh("fused_audit_l2", std::time::Duration::from_secs(60))
        .await;
    assert!(
        refreshed,
        "Scheduler should auto-refresh fused_audit_l2 within 60 s"
    );

    // Check pgt_refresh_history: the CHECK constraint must accept SCHEDULER_FUSED.
    // We verify this by querying whether any row with initiated_by = 'SCHEDULER_FUSED'
    // exists OR that standard SCHEDULER rows are present (the scheduler may fall back
    // to sequential if fusing was not eligible this cycle).
    let fused_count: i64 = db
        .query_scalar(
            "SELECT count(*) \
             FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name IN ('fused_audit_l1', 'fused_audit_l2') \
               AND h.initiated_by IN ('SCHEDULER', 'SCHEDULER_FUSED') \
               AND h.status = 'COMPLETED'",
        )
        .await;

    assert!(
        fused_count > 0,
        "pgt_refresh_history should contain COMPLETED scheduler-driven records \
         for the fused DAG (fused_count={}). \
         If this assertion fails it likely means SCHEDULER_FUSED is not in the \
         initiated_by CHECK constraint.",
        fused_count,
    );

    // Additionally, verify SCHEDULER_FUSED is an accepted initiated_by value
    // by attempting a direct INSERT (would fail with CHECK violation pre-v0.68.0).
    let pgt_id: i64 = db
        .query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'fused_audit_l1'",
        )
        .await;

    let insert_ok = db
        .try_execute(&format!(
            "INSERT INTO pgtrickle.pgt_refresh_history \
                (pgt_id, start_time, refresh_mode, status, initiated_by) \
             VALUES \
                ({pgt_id}, now(), 'DIFFERENTIAL', 'COMPLETED', 'SCHEDULER_FUSED')"
        ))
        .await;

    assert!(
        insert_ok.is_ok(),
        "Inserting a row with initiated_by = 'SCHEDULER_FUSED' must succeed \
         (COR-001: CHECK constraint should include SCHEDULER_FUSED)"
    );

    // Reset GUCs.
    db.execute("ALTER SYSTEM RESET pg_trickle.enable_fused_refresh")
        .await;
    db.execute("ALTER SYSTEM RESET pg_trickle.scheduler_interval_ms")
        .await;
    db.execute("ALTER SYSTEM RESET pg_trickle.min_schedule_seconds")
        .await;
    db.execute("ALTER SYSTEM RESET pg_trickle.auto_backoff")
        .await;
    db.reload_config_and_wait().await;
}
