//! Regression coverage for #1439: partitioned stream-table producers with
//! downstream DIFFERENTIAL consumers. These tests run against an installed
//! PostgreSQL extension (full and light Testcontainers harnesses).

mod e2e;

use e2e::E2eDb;

// With downstream stream tables, refresh uses explicit DELETE/UPDATE/INSERT
// for exact CDC capture. PostgreSQL cannot give partitioned storage a global
// UNIQUE (__pgt_row_id) constraint, so keyed INSERTs must not infer one.

/// Ensure a matching result was produced by the explicit differential DML
/// path, not by a silent FULL fallback that could mask the regression.
async fn assert_explicit_partitioned_refresh(db: &E2eDb, name: &str) {
    let (action, strategy, full_fallback): (String, Option<String>, bool) = sqlx::query_as(
        "SELECT h.action, h.merge_strategy_used, h.was_full_fallback
         FROM pgtrickle.pgt_refresh_history h
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id)
         WHERE s.pgt_name = $1 AND h.initiated_by = 'MANUAL'
         ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .bind(name)
    .fetch_one(&db.pool)
    .await
    .expect("read producer refresh history");
    assert_eq!(action, "DIFFERENTIAL", "{name} must refresh differentially");
    assert!(!full_fallback, "{name} must not use a FULL fallback");
    assert_eq!(
        strategy.as_deref(),
        Some("explicit_dml"),
        "{name} must use the partitioned explicit-DML path"
    );
}

async fn assert_partitioned_projection_cascade(db: &E2eDb) {
    db.assert_st_matches_query(
        "p1439_up",
        "SELECT id, event_day, region, amount FROM p1439_src",
    )
    .await;
    // Dropping id makes duplicate output rows intentional. EXCEPT ALL in the
    // oracle must catch both lost occurrences and extra downstream CDC rows.
    db.assert_st_matches_query("p1439_rows", "SELECT region, amount FROM p1439_src")
        .await;
    db.assert_st_matches_query(
        "p1439_totals",
        "SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM p1439_src GROUP BY region",
    )
    .await;

    let duplicate_ids: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM (
                SELECT __pgt_row_id FROM p1439_up
                GROUP BY __pgt_row_id HAVING COUNT(*) > 1
             ) duplicates",
        )
        .await;
    assert_eq!(
        duplicate_ids, 0,
        "keyed producer must not duplicate row identities"
    );
}

async fn refresh_partitioned_projection_cascade(db: &E2eDb) {
    db.refresh_st_with_retry("p1439_up").await;
    db.refresh_st_with_retry("p1439_rows").await;
    db.refresh_st_with_retry("p1439_totals").await;
    assert_partitioned_projection_cascade(db).await;
}

async fn exercise_partitioned_projection_cascade(partition_by: &str, expected_strategy: &str) {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE p1439_src (
            id INT PRIMARY KEY,
            event_day DATE NOT NULL,
            region TEXT NOT NULL,
            amount INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO p1439_src VALUES
            (1, '2026-01-10', 'north', 10),
            (2, '2026-02-10', 'north', 10),
            (3, '2026-01-20', 'south', 30),
            (4, '2026-02-20', 'south', 40)",
    )
    .await;

    db.create_st_partitioned(
        "p1439_up",
        "SELECT id, event_day, region, amount FROM p1439_src",
        "1m",
        "DIFFERENTIAL",
        partition_by,
    )
    .await;
    let actual_strategy: String = db
        .query_scalar(
            "SELECT partstrat::text FROM pg_partitioned_table
             WHERE partrelid = 'p1439_up'::regclass",
        )
        .await;
    assert_eq!(actual_strategy, expected_strategy);
    // Materialize the producer before registering either downstream consumer.
    db.refresh_st_with_retry("p1439_up").await;
    db.create_st(
        "p1439_rows",
        "SELECT region, amount FROM p1439_up",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "p1439_totals",
        "SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM p1439_up GROUP BY region",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    refresh_partitioned_projection_cascade(&db).await;

    // INSERTs add new identities, including two indistinguishable visible
    // rows (north, 10) in the downstream projection.
    db.execute(
        "INSERT INTO p1439_src VALUES
            (5, '2026-01-15', 'north', 10),
            (6, '2026-04-15', 'north', 10)",
    )
    .await;
    refresh_partitioned_projection_cascade(&db).await;
    assert_explicit_partitioned_refresh(&db, "p1439_up").await;

    // Move a stable keyed row across RANGE date / LIST region partitions;
    // also exercise HASH routing when the partition key is region.
    db.execute(
        "UPDATE p1439_src
         SET event_day = '2026-03-15', region = 'south', amount = 15
         WHERE id = 2",
    )
    .await;
    db.execute("UPDATE p1439_src SET amount = 11 WHERE id = 1")
        .await;
    refresh_partitioned_projection_cascade(&db).await;
    assert_explicit_partitioned_refresh(&db, "p1439_up").await;

    db.execute("DELETE FROM p1439_src WHERE id IN (3, 5)").await;
    refresh_partitioned_projection_cascade(&db).await;
    assert_explicit_partitioned_refresh(&db, "p1439_up").await;
    // Replaying with no new source changes must not create duplicate rows.
    refresh_partitioned_projection_cascade(&db).await;
}

#[tokio::test]
async fn test_partitioned_range_keyed_producer_with_downstream_dml() {
    exercise_partitioned_projection_cascade("event_day", "r").await;
}

#[tokio::test]
async fn test_partitioned_list_keyed_producer_with_downstream_dml() {
    exercise_partitioned_projection_cascade("LIST:region", "l").await;
}

#[tokio::test]
async fn test_partitioned_hash_keyed_producer_with_downstream_dml() {
    exercise_partitioned_projection_cascade("HASH:region", "h").await;
}

async fn assert_partitioned_aggregate_cascade(db: &E2eDb) {
    let source_aggregate = "SELECT event_day, region, COUNT(*) AS n, SUM(amount) AS total
                            FROM p1439_agg_src GROUP BY event_day, region";
    db.assert_st_matches_query("p1439_agg_up", source_aggregate)
        .await;
    db.assert_st_matches_query("p1439_agg_rows", source_aggregate)
        .await;
    db.assert_st_matches_query(
        "p1439_agg_totals",
        &format!(
            "SELECT region, SUM(n) AS n, SUM(total) AS total
             FROM ({source_aggregate}) grouped GROUP BY region"
        ),
    )
    .await;

    let duplicate_ids: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM (
                SELECT __pgt_row_id FROM p1439_agg_up
                GROUP BY __pgt_row_id HAVING COUNT(*) > 1
             ) duplicates",
        )
        .await;
    assert_eq!(
        duplicate_ids, 0,
        "aggregate producer must not duplicate group identities"
    );
}

async fn refresh_partitioned_aggregate_cascade(db: &E2eDb) {
    db.refresh_st_with_retry("p1439_agg_up").await;
    db.refresh_st_with_retry("p1439_agg_rows").await;
    db.refresh_st_with_retry("p1439_agg_totals").await;
    assert_partitioned_aggregate_cascade(db).await;
}

async fn exercise_partitioned_aggregate_cascade(partition_by: &str, expected_strategy: &str) {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE p1439_agg_src (
            id INT PRIMARY KEY,
            event_day DATE NOT NULL,
            region TEXT NOT NULL,
            amount INT NOT NULL
        )",
    )
    .await;
    db.execute(
        "INSERT INTO p1439_agg_src VALUES
            (1, '2026-01-15', 'north', 10),
            (2, '2026-01-15', 'north', 20),
            (3, '2026-02-15', 'north', 30),
            (4, '2026-02-15', 'south', 40)",
    )
    .await;

    db.create_st_partitioned(
        "p1439_agg_up",
        "SELECT event_day, region, COUNT(*) AS n, SUM(amount) AS total
         FROM p1439_agg_src GROUP BY event_day, region",
        "1m",
        "DIFFERENTIAL",
        partition_by,
    )
    .await;
    let actual_strategy: String = db
        .query_scalar(
            "SELECT partstrat::text FROM pg_partitioned_table
             WHERE partrelid = 'p1439_agg_up'::regclass",
        )
        .await;
    assert_eq!(actual_strategy, expected_strategy);
    db.refresh_st_with_retry("p1439_agg_up").await;
    db.create_st(
        "p1439_agg_rows",
        "SELECT event_day, region, n, total FROM p1439_agg_up",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "p1439_agg_totals",
        "SELECT region, SUM(n) AS n, SUM(total) AS total
         FROM p1439_agg_up GROUP BY region",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    refresh_partitioned_aggregate_cascade(&db).await;

    // Update an existing group and insert a previously absent group.
    db.execute(
        "INSERT INTO p1439_agg_src VALUES
            (5, '2026-01-15', 'north', 7),
            (6, '2026-03-15', 'east', 50)",
    )
    .await;
    refresh_partitioned_aggregate_cascade(&db).await;
    assert_explicit_partitioned_refresh(&db, "p1439_agg_up").await;

    // Move a group member to another date and region, changing the key
    // of two groups (and possibly the physical output partition).
    db.execute(
        "UPDATE p1439_agg_src
         SET event_day = '2026-03-15', region = 'south', amount = 22
         WHERE id = 2",
    )
    .await;
    db.execute("UPDATE p1439_agg_src SET amount = 33 WHERE id = 3")
        .await;
    refresh_partitioned_aggregate_cascade(&db).await;
    assert_explicit_partitioned_refresh(&db, "p1439_agg_up").await;

    // Remove the last member of two groups, checking downstream DELETE CDC.
    db.execute("DELETE FROM p1439_agg_src WHERE id IN (4, 6)")
        .await;
    refresh_partitioned_aggregate_cascade(&db).await;
    assert_explicit_partitioned_refresh(&db, "p1439_agg_up").await;
    refresh_partitioned_aggregate_cascade(&db).await;
}

#[tokio::test]
async fn test_partitioned_range_aggregate_producer_with_downstream_dml() {
    exercise_partitioned_aggregate_cascade("event_day", "r").await;
}

#[tokio::test]
async fn test_partitioned_list_aggregate_producer_with_downstream_dml() {
    exercise_partitioned_aggregate_cascade("LIST:region", "l").await;
}

#[tokio::test]
async fn test_partitioned_hash_aggregate_producer_with_downstream_dml() {
    exercise_partitioned_aggregate_cascade("HASH:region", "h").await;
}
