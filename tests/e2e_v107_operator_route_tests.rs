//! Executable v0.107 quickstart and operator-recovery route.

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_v107_operator_route_preserves_captured_changes() {
    let db = E2eDb::new().await.with_extension().await;
    let query = "SELECT region, SUM(amount) AS total, COUNT(*) AS order_count \
                 FROM v107_orders GROUP BY region";

    db.execute(
        "CREATE TABLE v107_orders (id INT PRIMARY KEY, region TEXT NOT NULL, amount INT NOT NULL)",
    )
    .await;
    db.execute("INSERT INTO v107_orders VALUES (1, 'US', 100), (2, 'EU', 200)")
        .await;

    let admitted: String = db
        .query_scalar(&format!(
            "SELECT result FROM pgtrickle.validate_query($${query}$$) \
             WHERE check_name = 'resolved_refresh_mode'"
        ))
        .await;
    assert_eq!(admitted, "DIFFERENTIAL");

    db.create_st("v107_revenue", query, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("v107_revenue", query).await;

    db.execute("UPDATE v107_orders SET region = 'EU', amount = 150 WHERE id = 1")
        .await;
    db.refresh_st("v107_revenue").await;
    db.assert_st_matches_query("v107_revenue", query).await;

    db.alter_st("v107_revenue", "status => 'SUSPENDED'").await;
    assert!(
        db.try_execute("SELECT pgtrickle.refresh_stream_table('v107_revenue')")
            .await
            .is_err(),
        "a suspended stream table must reject refresh"
    );

    let first = sqlx::query("INSERT INTO v107_orders VALUES (3, 'US', 300)").execute(&db.pool);
    let second = sqlx::query("INSERT INTO v107_orders VALUES (4, 'APAC', 400)").execute(&db.pool);
    let (first, second) = tokio::join!(first, second);
    first.expect("first concurrent source write");
    second.expect("second concurrent source write");

    db.execute("SELECT pgtrickle.resume_stream_table('v107_revenue')")
        .await;
    db.refresh_st("v107_revenue").await;
    db.assert_st_matches_query("v107_revenue", query).await;

    db.alter_st("v107_revenue", "schedule => '2m'").await;
    let repair: String = db
        .query_scalar("SELECT pgtrickle.repair_stream_table('v107_revenue')")
        .await;
    assert!(repair.contains("frontier reset"));
    db.refresh_st("v107_revenue").await;
    db.assert_st_matches_query("v107_revenue", query).await;

    let (action, status): (String, String) = sqlx::query_as(
        "SELECT h.action, h.status FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE st.pgt_name = 'v107_revenue' ORDER BY refresh_id DESC LIMIT 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("latest refresh evidence");
    assert_eq!(
        (action.as_str(), status.as_str()),
        ("REINITIALIZE", "COMPLETED")
    );
}
