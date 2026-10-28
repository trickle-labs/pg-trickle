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
        // The bounded receipt-witnessed case below owns WAL qualification.
        for case in strategy::all_variants()
            .into_iter()
            .filter(|case| case.cdc_mode == strategy::CdcModeVariant::Trigger)
        {
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

fn route_matches(
    expected_mode: &str,
    action: &str,
    status: &str,
    strategy: &str,
    fallback: bool,
    rows_changed: i64,
) -> bool {
    action == expected_mode
        && status == "COMPLETED"
        && !fallback
        && rows_changed > 0
        && (expected_mode != "DIFFERENTIAL" || strategy != "FULL")
}

#[test]
fn test_strategy_route_witness_rejects_silent_fallback_and_empty_work() {
    assert!(route_matches(
        "DIFFERENTIAL",
        "DIFFERENTIAL",
        "COMPLETED",
        "MERGE",
        false,
        1
    ));
    assert!(route_matches("FULL", "FULL", "COMPLETED", "FULL", false, 1));
    for (action, status, strategy, fallback, changed) in [
        ("FULL", "COMPLETED", "FULL", true, 1),
        ("DIFFERENTIAL", "COMPLETED", "FULL", false, 1),
        ("DIFFERENTIAL", "FAILED", "MERGE", false, 1),
        ("DIFFERENTIAL", "COMPLETED", "MERGE", false, 0),
    ] {
        assert!(!route_matches(
            "DIFFERENTIAL",
            action,
            status,
            strategy,
            fallback,
            changed
        ));
    }
}

/// A bounded PR matrix: every axis executes committed INSERT, UPDATE and DELETE.
/// Longer composed-corpus combinations remain in the scheduled test above.
#[tokio::test]
async fn test_strategy_changed_data_trigger_wal_full_differential_witnessed() {
    use std::time::Duration;

    let wait = Duration::from_secs(60);
    let mut observed = 0;
    for case in strategy::all_variants() {
        let db = E2eDb::new_dedicated().await.with_extension().await;
        db.alter_system_set_and_wait("pg_trickle.scheduler_interval_ms", "100", "100")
            .await;
        assert!(
            db.wait_for_scheduler(wait).await,
            "{case:?}: scheduler missing"
        );
        db.execute_seq(&[
            "CREATE TABLE route_source (id INT PRIMARY KEY, value TEXT)",
            "ALTER TABLE route_source REPLICA IDENTITY FULL",
            "INSERT INTO route_source VALUES (1, 'baseline')",
        ])
        .await;
        let mode = strategy::requested_refresh_mode(case.refresh_mode);
        let cdc = strategy::cdc_mode_name(case.cdc_mode);
        db.execute(&format!(
            "SELECT pgtrickle.create_stream_table(name => 'route_stream', \
             query => 'SELECT id, value FROM route_source', schedule => '24h', \
             refresh_mode => '{mode}', cdc_mode => '{cdc}')"
        ))
        .await;
        let expected_cdc = cdc.to_uppercase();
        let mode_query = format!(
            "SELECT count(*) = 1 AND bool_and(cdc_mode = '{expected_cdc}') \
             FROM pgtrickle.pgt_dependencies \
             WHERE source_relid = 'route_source'::regclass"
        );
        assert!(
            db.wait_for_condition(
                "selected CDC route",
                &mode_query,
                wait,
                Duration::from_millis(50)
            )
            .await,
            "{case:?}: source did not reach the selected CDC route"
        );
        db.refresh_st("route_stream").await;
        for (cycle, mutation) in [
            "INSERT INTO route_source VALUES (2, 'route-insert')",
            "UPDATE route_source SET value = 'route-update' WHERE id = 2",
            "DELETE FROM route_source WHERE id = 2",
        ]
        .iter()
        .enumerate()
        {
            let before: i64 = db
                .query_scalar(
                    "SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history",
                )
                .await;
            let receipt_before: i64 = db
                .query_scalar("SELECT COALESCE(max(receipt_id), 0) FROM pgtrickle.pgt_wal_receipts")
                .await;
            db.execute(mutation).await;
            if case.cdc_mode == strategy::CdcModeVariant::Wal {
                assert!(
                    db.wait_for_condition(
                        "changed row durably received and applied through WAL",
                        &format!(
                            "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_wal_receipts \
                         WHERE source_relid = 'route_source'::regclass \
                         AND receipt_id > {receipt_before} AND source_change \
                         AND status = 'ACKNOWLEDGED' AND data LIKE '%route-%')"
                        ),
                        wait,
                        Duration::from_millis(50)
                    )
                    .await,
                    "{case:?} cycle {cycle}: missing applied WAL receipt"
                );
            }
            if !db.query_scalar::<bool>(&mode_query).await {
                let logs = tokio::process::Command::new("docker")
                    .args(["logs", "--tail", "150", db.container_id()])
                    .output()
                    .await
                    .expect("collect failing route's PostgreSQL log");
                panic!(
                    "{case:?} cycle {cycle}: CDC fallback\n{}",
                    String::from_utf8_lossy(&logs.stderr)
                );
            }
            if case.refresh_mode == strategy::RefreshModeVariant::Differential {
                let buffer = db
                    .change_buffer_table(db.table_oid("route_source").await as i64)
                    .await;
                let captured: i64 = db
                    .query_scalar(&format!(
                        "SELECT count(*) FROM {buffer} WHERE action IN ('I', 'U', 'D')"
                    ))
                    .await;
                assert!(
                    captured > 0,
                    "{case:?} cycle {cycle}: differential change buffer was empty"
                );
            }
            db.try_execute_with_config(
                &[&format!(
                    "SET pg_trickle.refresh_strategy = '{}'",
                    mode.to_lowercase()
                )],
                "SELECT pgtrickle.refresh_stream_table('route_stream')",
            )
            .await
            .expect("changed-data refresh must succeed");
            let (action, status, merge, fallback, changed): (String, String, String, bool, i64) =
                sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT action, status, COALESCE(merge_strategy_used, ''), \
                     was_full_fallback, rows_inserted + rows_updated + rows_deleted \
                     FROM pgtrickle.pgt_refresh_history \
                     WHERE refresh_id > {before} AND pgt_id = \
                     (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'route_stream') \
                     ORDER BY refresh_id DESC LIMIT 1"
                ))).fetch_one(&db.pool).await.expect("must record a new refresh for this cycle");
            assert!(
                route_matches(mode, &action, &status, &merge, fallback, changed),
                "{case:?} cycle {cycle}: {action}, {status}, {merge}, fallback={fallback}, rows={changed}"
            );
            e2e::oracle::assert_effective_refresh_mode(&db, "route_stream", mode)
                .await
                .unwrap();
            e2e::oracle::assert_st_query_exact(
                &db,
                "route_stream",
                "SELECT id, value FROM route_source",
                "witnessed changed-data route",
            )
            .await;
            assert!(
                db.query_scalar::<bool>(&mode_query).await,
                "{case:?} cycle {cycle}: CDC fallback"
            );
            observed += 1;
        }
    }
    assert_eq!(
        observed, 12,
        "all four routes and three mutations must be observed"
    );
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
        "CREATE TABLE v0876_orders_barrier (id INT PRIMARY KEY, cust_id INT, amount INT)",
        "CREATE TABLE v0876_orders_incremental (id INT PRIMARY KEY, cust_id INT, amount INT)",
        "CREATE TABLE v0876_customers (id INT PRIMARY KEY, region TEXT)",
        "INSERT INTO v0876_customers VALUES (1, 'east'), (2, 'west')",
        "INSERT INTO v0876_orders_barrier VALUES (1, 1, 100), (2, 2, 200)",
        "INSERT INTO v0876_orders_incremental VALUES (1, 1, 100), (2, 2, 200)",
    ])
    .await;

    let barrier_query = "SELECT c.region, SUM(o.amount) AS total \
                 FROM v0876_orders_barrier o JOIN v0876_customers c ON c.id = o.cust_id \
                 GROUP BY c.region";
    let incremental_query = "SELECT c.region, SUM(o.amount) AS total \
                 FROM v0876_orders_incremental o JOIN v0876_customers c ON c.id = o.cust_id \
                 GROUP BY c.region";

    db.create_st("v0876_barrier", barrier_query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st("v0876_incremental", incremental_query, "1h", "DIFFERENTIAL")
        .await;
    db.refresh_st("v0876_barrier").await;
    db.refresh_st("v0876_incremental").await;

    let barrier_mutations = [
        "INSERT INTO v0876_orders_barrier VALUES (3, 1, 50)",
        "UPDATE v0876_orders_barrier SET amount = amount + 10 WHERE id = 2",
        "INSERT INTO v0876_orders_barrier VALUES (4, 2, 30)",
    ];
    let incremental_mutations = [
        "INSERT INTO v0876_orders_incremental VALUES (3, 1, 50)",
        "UPDATE v0876_orders_incremental SET amount = amount + 10 WHERE id = 2",
        "INSERT INTO v0876_orders_incremental VALUES (4, 2, 30)",
    ];

    // Barrier: all mutations committed together in one explicit transaction,
    // refreshed once afterward.
    let mut barrier_stmts = vec!["BEGIN"];
    barrier_stmts.extend_from_slice(&barrier_mutations);
    barrier_stmts.push("COMMIT");
    db.execute_seq(&barrier_stmts).await;
    db.refresh_st("v0876_barrier").await;

    // Incremental: each mutation auto-committed separately, refreshed after each.
    for mutation in &incremental_mutations {
        db.execute(mutation).await;
        db.refresh_st("v0876_incremental").await;
    }

    e2e::oracle::assert_st_query_exact(&db, "v0876_barrier", barrier_query, "transaction barrier")
        .await;
    e2e::oracle::assert_st_query_exact(
        &db,
        "v0876_incremental",
        incremental_query,
        "incremental commits",
    )
    .await;
    e2e::oracle::compare_sts(&db, "v0876_barrier", "v0876_incremental")
        .await
        .expect("committed-transaction and incremental refresh histories must converge");

    // Resource boundary: a generous statement_timeout budget must not be
    // silently exceeded by a refresh.
    db.execute("SET statement_timeout = '30s'").await;
    db.execute("INSERT INTO v0876_orders_barrier VALUES (5, 1, 5)")
        .await;
    db.refresh_st("v0876_barrier").await;
    e2e::oracle::assert_st_query_exact(
        &db,
        "v0876_barrier",
        barrier_query,
        "resource boundary refresh",
    )
    .await;
    db.execute("SET statement_timeout = 0").await;
}
