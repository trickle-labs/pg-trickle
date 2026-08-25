//! v0.87.6 COR-19: strategy, recovery, and resource pressure.
//!
//! Replays the permanent corpus across supported refresh-mode / CDC-mode
//! variants and adds an explicit transaction-barrier and resource-boundary
//! case (a generous statement_timeout budget must not be silently exceeded).

mod e2e;

#[path = "e2e/dvm_fuzz/mod.rs"]
mod dvm_fuzz;

#[path = "e2e/dvm_fuzz/strategy.rs"]
mod strategy;

use e2e::E2eDb;
use std::path::Path;

fn scenario_with_variant(
    scenario: &dvm_fuzz::Scenario,
    case: strategy::StrategyCase,
) -> dvm_fuzz::Scenario {
    let mut variant = scenario.clone();
    variant.execution.requested_refresh_mode =
        strategy::requested_refresh_mode(case.refresh_mode).to_string();
    variant.expected_capability.expected_mode =
        strategy::expected_mode(case.refresh_mode).to_string();
    variant.expected_capability.differential = matches!(
        case.refresh_mode,
        strategy::RefreshModeVariant::Differential
    );
    variant
}

#[tokio::test]
async fn test_v0876_strategy_variants_converge_for_corpus_cases() {
    let corpus_paths = [
        "tests/corpus/dvm_regressions/cor938_physical_width.json",
        "tests/corpus/dvm_regressions/cor939_two_leaf_snapshot.json",
    ];

    let scenarios: Vec<dvm_fuzz::Scenario> = corpus_paths
        .iter()
        .map(|p| dvm_fuzz::load_scenario(Path::new(p)).expect("corpus scenario must load"))
        .collect();

    let db = E2eDb::new().await.with_extension().await;

    for scenario in &scenarios {
        for case in strategy::all_variants() {
            db.alter_system_set_and_wait(
                "pg_trickle.cdc_mode",
                strategy::cdc_mode_literal(case.cdc_mode),
                strategy::cdc_mode_name(case.cdc_mode),
            )
            .await;

            let variant_scenario = scenario_with_variant(scenario, case);
            let result = dvm_fuzz::replay(&db, &variant_scenario).await;
            assert!(
                result.is_ok(),
                "scenario '{}' under variant {:?} failed: {}",
                scenario.scenario_id,
                case,
                result.err().map(|e| e.to_string()).unwrap_or_default()
            );
        }
    }

    db.alter_system_set_and_wait("pg_trickle.cdc_mode", "'auto'", "auto")
        .await;
}

/// A single committed multi-row transaction and the same mutations applied
/// as separate auto-committed statements (each followed by a refresh) must
/// converge to the same exact result -- a transaction barrier does not
/// change the differential outcome. A generous statement_timeout budget
/// must also not be silently exceeded on refresh.
#[tokio::test]
async fn test_v0876_transaction_barrier_and_statement_timeout_resource_boundary() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TABLE v0876_orders (id INT PRIMARY KEY, cust_id INT, amount INT)",
        "CREATE TABLE v0876_customers (id INT PRIMARY KEY, region TEXT)",
        "INSERT INTO v0876_customers VALUES (1, 'east'), (2, 'west')",
        "INSERT INTO v0876_orders VALUES (1, 1, 100), (2, 2, 200)",
    ])
    .await;

    let query = "SELECT c.region, SUM(o.amount) AS total \
                 FROM v0876_orders o JOIN v0876_customers c ON c.id = o.cust_id \
                 GROUP BY c.region";

    db.create_st("v0876_barrier", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st("v0876_incremental", query, "1h", "DIFFERENTIAL")
        .await;
    db.refresh_st("v0876_barrier").await;
    db.refresh_st("v0876_incremental").await;

    let mutations = [
        "INSERT INTO v0876_orders VALUES (3, 1, 50)",
        "UPDATE v0876_orders SET amount = amount + 10 WHERE id = 2",
        "INSERT INTO v0876_orders VALUES (4, 2, 30)",
    ];

    // Barrier: all mutations committed together in one explicit transaction,
    // refreshed once afterward.
    let mut barrier_stmts = vec!["BEGIN"];
    barrier_stmts.extend_from_slice(&mutations);
    barrier_stmts.push("COMMIT");
    db.execute_seq(&barrier_stmts).await;
    db.refresh_st("v0876_barrier").await;

    // Incremental: each mutation auto-committed separately, refreshed after each.
    for mutation in &mutations {
        db.execute(mutation).await;
        db.refresh_st("v0876_incremental").await;
    }

    e2e::oracle::assert_st_query_exact(&db, "v0876_barrier", query, "transaction barrier").await;
    e2e::oracle::assert_st_query_exact(&db, "v0876_incremental", query, "incremental commits")
        .await;
    e2e::oracle::compare_sts(&db, "v0876_barrier", "v0876_incremental")
        .await
        .expect("committed-transaction and incremental refresh histories must converge");

    // Resource boundary: a generous statement_timeout budget must not be
    // silently exceeded by a refresh.
    db.execute("SET statement_timeout = '30s'").await;
    db.execute("INSERT INTO v0876_orders VALUES (5, 1, 5)")
        .await;
    db.refresh_st("v0876_barrier").await;
    e2e::oracle::assert_st_query_exact(&db, "v0876_barrier", query, "resource boundary refresh")
        .await;
    db.execute("SET statement_timeout = 0").await;
}
