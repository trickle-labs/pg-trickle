//! Default-disabled qualification-hook coverage for an on-demand extension load.

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_output_delta_qualification_is_disabled_for_on_demand_load() {
    let db = E2eDb::new_with_output_delta_qualification_disabled()
        .await
        .with_extension()
        .await;
    let error = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle.qualify_output_delta_recovery(
             '00000000-0000-0000-0000-000000000000'::uuid, 'INVALIDATED')",
    )
    .fetch_one(&db.pool)
    .await
    .expect_err("qualification must remain disabled without its postmaster setting");
    assert!(error.to_string().contains("PGT_EXT_QUALIFICATION_DISABLED"));
}
