//! v0.77.0 correctness stop-the-line tests.
//!
//! Covers the items in the Assessment-15 hardening arc that are specific
//! to v0.77.0:
//!
//! - D-1: Multi-consumer cleanup advisory lock (overlap test)
//! - D-2: IMMEDIATE mode SAVEPOINT / subtransaction rollback
//!
//! Prerequisites: `./tests/build_e2e_image.sh` (full E2E image) or
//! `cargo pgrx package` output bind-mounted (light E2E).

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// D-1: Multi-consumer cleanup advisory lock
// ═══════════════════════════════════════════════════════════════════════

/// D-1a: Two stream tables sharing the same source table both refresh
/// correctly with no change buffer row loss after concurrent cleanup cycles.
///
/// This test exercises the scenario where two consumers (stream tables)
/// depend on a single source.  The cleanup code (drain_pending_cleanups)
/// must not delete rows from the change buffer that have not yet been
/// consumed by both stream tables.
///
/// The D-1 advisory lock (`pg_try_advisory_xact_lock`) ensures that only
/// one cleanup worker owns the min-frontier computation + DELETE window
/// per source OID per transaction; the other skips and lets the next
/// cycle clean up.
#[tokio::test]
async fn test_d1_shared_source_two_consumers_no_row_loss() {
    let db = E2eDb::new().await.with_extension().await;

    // Single source table
    db.execute("CREATE TABLE d1_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO d1_src SELECT g, g * 10 FROM generate_series(1, 20) g")
        .await;

    // Two stream tables depending on the same source
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'd1_st_a',\
            query => $$SELECT id, val FROM d1_src WHERE val < 100$$,\
            schedule => '24h',\
            refresh_mode => 'DIFFERENTIAL'\
         )",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'd1_st_b',\
            query => $$SELECT id, val FROM d1_src WHERE val >= 100$$,\
            schedule => '24h',\
            refresh_mode => 'DIFFERENTIAL'\
         )",
    )
    .await;

    // Full initialisation
    db.refresh_st("d1_st_a").await;
    db.refresh_st("d1_st_b").await;

    let a_initial = db.count("public.d1_st_a").await;
    let b_initial = db.count("public.d1_st_b").await;
    assert_eq!(
        a_initial, 9,
        "D-1a: st_a pre-condition (val 10..90): {a_initial}"
    );
    assert_eq!(
        b_initial, 11,
        "D-1a: st_b pre-condition (val 100..200): {b_initial}"
    );

    // Apply changes that affect both consumers
    db.execute("UPDATE d1_src SET val = val + 5 WHERE id <= 10")
        .await;
    db.execute("INSERT INTO d1_src VALUES (21, 55), (22, 115)")
        .await;

    // Refresh both stream tables — the second refresh must still see the
    // correct min-frontier for the shared source (D-1 advisory lock must
    // not cause the second refresh to miss rows or double-delete).
    db.refresh_st("d1_st_a").await;
    db.refresh_st("d1_st_b").await;

    // Verify correctness: compare against the defining queries directly.
    let a_expected: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM d1_src WHERE val < 100")
        .await;
    let b_expected: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM d1_src WHERE val >= 100")
        .await;

    let a_actual = db.count("public.d1_st_a").await;
    let b_actual = db.count("public.d1_st_b").await;

    assert_eq!(
        a_actual, a_expected,
        "D-1a: st_a row count mismatch: got {a_actual}, expected {a_expected}"
    );
    assert_eq!(
        b_actual, b_expected,
        "D-1a: st_b row count mismatch: got {b_actual}, expected {b_expected}"
    );
}

/// D-1b: After multiple refresh cycles with interleaved DML, the change
/// buffer must be progressively cleaned without dropping live rows needed
/// by the slower consumer.
#[tokio::test]
async fn test_d1_multi_cycle_cleanup_preserves_slow_consumer_rows() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE d1b_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO d1b_src SELECT g, g FROM generate_series(1, 10) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'd1b_fast',\
            query => $$SELECT id, val FROM d1b_src$$,\
            schedule => '24h',\
            refresh_mode => 'DIFFERENTIAL'\
         )",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'd1b_slow',\
            query => $$SELECT id, val * 2 AS val2 FROM d1b_src$$,\
            schedule => '24h',\
            refresh_mode => 'DIFFERENTIAL'\
         )",
    )
    .await;

    db.refresh_st("d1b_fast").await;
    db.refresh_st("d1b_slow").await;

    // Multiple DML + refresh cycles: fast consumer refreshes every cycle;
    // slow consumer refreshes every other cycle.
    for cycle in 0..4u64 {
        let base = (cycle as i32 + 1) * 100;
        db.execute(&format!(
            "INSERT INTO d1b_src VALUES ({}, {})",
            100 + cycle,
            base
        ))
        .await;
        db.refresh_st("d1b_fast").await;
        if cycle % 2 == 1 {
            // Slow consumer only refreshes every other cycle
            db.refresh_st("d1b_slow").await;
        }
    }
    // Final refresh for slow consumer
    db.refresh_st("d1b_slow").await;

    // Both stream tables must be consistent with their defining queries.
    let fast_expected: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM d1b_src")
        .await;
    let slow_expected: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM d1b_src")
        .await;

    assert_eq!(
        db.count("public.d1b_fast").await,
        fast_expected,
        "D-1b: fast consumer must be consistent after {fast_expected} rows"
    );
    assert_eq!(
        db.count("public.d1b_slow").await,
        slow_expected,
        "D-1b: slow consumer must be consistent after {slow_expected} rows"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// D-2: IMMEDIATE mode SAVEPOINT / subtransaction rollback
// ═══════════════════════════════════════════════════════════════════════

/// D-2a: Rolling back a SAVEPOINT inside a transaction that touches an
/// IMMEDIATE stream table source must not leave ghost rows in the stream
/// table.
///
/// The IMMEDIATE refresh trigger fires on the source table INSERT/UPDATE/
/// DELETE AFTER EACH ROW.  When a SAVEPOINT is rolled back, the trigger
/// fires for the DML but the outer transaction does not commit — the ST
/// update must also be rolled back atomically.
#[tokio::test]
async fn test_d2_savepoint_rollback_no_ghost_rows() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE d2_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO d2_src VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'd2_st',\
            query => $$SELECT id, val FROM d2_src$$,\
            refresh_mode => 'IMMEDIATE'\
         )",
    )
    .await;

    assert_eq!(db.count("public.d2_st").await, 3, "D-2a: pre-condition");

    // Begin a transaction, INSERT inside a SAVEPOINT, then roll back the SAVEPOINT.
    // The 3 original rows must still be present; no ghost row for id=99.
    db.execute(
        "BEGIN;\
         INSERT INTO d2_src VALUES (99, 'ghost');\
         SAVEPOINT sp1;\
         INSERT INTO d2_src VALUES (100, 'also_ghost');\
         ROLLBACK TO SAVEPOINT sp1;\
         ROLLBACK",
    )
    .await;

    let count = db.count("public.d2_st").await;
    assert_eq!(
        count, 3,
        "D-2a: after rolled-back transaction, stream table must still have 3 rows, got {count}"
    );

    let ghost: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.d2_st WHERE id IN (99, 100))")
        .await;
    assert!(
        !ghost,
        "D-2a: rolled-back rows (id 99, 100) must not appear in stream table"
    );
}

/// D-2b: Partial SAVEPOINT rollback — committed DML before the SAVEPOINT
/// must be visible; rolled-back DML after the SAVEPOINT must not be.
#[tokio::test]
async fn test_d2_partial_savepoint_rollback_committed_rows_visible() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE d2b_src (id INT PRIMARY KEY, val TEXT NOT NULL)")
        .await;
    db.execute("INSERT INTO d2b_src VALUES (1, 'original')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'd2b_st',\
            query => $$SELECT id, val FROM d2b_src$$,\
            refresh_mode => 'IMMEDIATE'\
         )",
    )
    .await;

    assert_eq!(db.count("public.d2b_st").await, 1, "D-2b: pre-condition");

    // Commit a row, then roll back a subsequent SAVEPOINT.
    db.execute(
        "BEGIN;\
         INSERT INTO d2b_src VALUES (2, 'committed');\
         SAVEPOINT sp1;\
         INSERT INTO d2b_src VALUES (3, 'rolled_back');\
         ROLLBACK TO SAVEPOINT sp1;\
         COMMIT",
    )
    .await;

    let count = db.count("public.d2b_st").await;
    assert_eq!(
        count, 2,
        "D-2b: after partial rollback+commit, stream table must have 2 rows, got {count}"
    );

    let committed: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.d2b_st WHERE val = 'committed')")
        .await;
    assert!(
        committed,
        "D-2b: committed row must be visible in stream table"
    );

    let rolled_back: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.d2b_st WHERE val = 'rolled_back')")
        .await;
    assert!(
        !rolled_back,
        "D-2b: rolled-back row must not appear in stream table"
    );
}

/// D-2c: Nested SAVEPOINTs — rolling back to an outer SAVEPOINT must not
/// leave rows committed by inner SAVEPOINTs in the stream table.
#[tokio::test]
async fn test_d2_nested_savepoints_full_rollback_no_ghost_rows() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE d2c_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO d2c_src SELECT g, g FROM generate_series(1, 5) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'd2c_st',\
            query => $$SELECT id, val FROM d2c_src$$,\
            refresh_mode => 'IMMEDIATE'\
         )",
    )
    .await;

    assert_eq!(db.count("public.d2c_st").await, 5, "D-2c: pre-condition");

    // Nested savepoints — rollback to outer savepoint discards inner work.
    db.execute(
        "BEGIN;\
         SAVEPOINT outer_sp;\
         INSERT INTO d2c_src VALUES (10, 10);\
         SAVEPOINT inner_sp;\
         INSERT INTO d2c_src VALUES (11, 11);\
         RELEASE SAVEPOINT inner_sp;\
         INSERT INTO d2c_src VALUES (12, 12);\
         ROLLBACK TO SAVEPOINT outer_sp;\
         ROLLBACK",
    )
    .await;

    let count = db.count("public.d2c_st").await;
    assert_eq!(
        count, 5,
        "D-2c: after rolling back to outer savepoint and aborting, stream table must still have 5 rows, got {count}"
    );

    let any_ghost: bool = db
        .query_scalar("SELECT EXISTS(SELECT 1 FROM public.d2c_st WHERE id IN (10, 11, 12))")
        .await;
    assert!(
        !any_ghost,
        "D-2c: all savepoint-nested rows must be absent from the stream table"
    );
}
