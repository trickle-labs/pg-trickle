//! v0.104 Graph V1 stable admission compatibility coverage.

mod common;
mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_v104_graph_v1_is_stable_by_default() {
    let db = E2eDb::new().await.with_extension().await;

    let enabled: bool = db
        .query_scalar(
            "SELECT enabled FROM pgtrickle.integration_capabilities() \
             WHERE capability = 'external_graph_refresh'",
        )
        .await;
    let status: String = db
        .query_scalar(
            "SELECT details->>'status' FROM pgtrickle.integration_capabilities() \
             WHERE capability = 'external_graph_refresh'",
        )
        .await;
    assert!(enabled, "Graph V1 must be enabled by default");
    assert_eq!(status, "stable");
}
