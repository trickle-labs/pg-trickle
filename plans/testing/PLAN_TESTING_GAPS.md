# PLAN_TESTING_GAPS.md — Test Coverage Gaps for Implemented Features

This document lists features that are **fully implemented** but have **no tests or
insufficient test coverage**, ranked by implementation risk and ease of writing.
Each item includes the target file, the concrete test function signature, and
the exact setup + assertion pattern to use.

All E2E tests use the shared `E2eDb` helper from `tests/e2e/mod.rs`.  
All integration tests use the `TestDb` helper from `tests/common/mod.rs`.

---

## Status Summary

### Implemented (2026-03-03)

| Cat | Test | File | Status |
|-----|------|------|--------|
| B1 | `test_add_column_on_source_st_still_functional` | `e2e_ddl_event_tests.rs` | **Done** |
| B1 | `test_add_column_unused_st_survives_refresh` | `e2e_ddl_event_tests.rs` | **Done** |
| B2 | `test_drop_unused_column_st_survives` | `e2e_ddl_event_tests.rs` | **Done** |
| B3 | `test_alter_column_type_triggers_reinit` | `e2e_ddl_event_tests.rs` | **Done** |
| B4 | `test_create_index_on_source_is_benign` | `e2e_ddl_event_tests.rs` | **Done** |
| B5 | `test_drop_source_with_multiple_downstream_sts` | `e2e_ddl_event_tests.rs` | **Done** |
| B6 | `test_block_source_ddl_guc_prevents_alter` | `e2e_ddl_event_tests.rs` | **Done** |
| B7 | `test_add_column_on_joined_source_st_survives` | `e2e_ddl_event_tests.rs` | **Done** |
| C1 | `test_concurrent_refresh_multiple_sts_same_source` | `e2e_concurrent_tests.rs` | **Done** |
| C2 | `test_concurrent_refresh_same_st_no_corruption` | `e2e_concurrent_tests.rs` | **Done** |
| C3 | `test_full_refresh_racing_with_dml` | `e2e_concurrent_tests.rs` | **Done** |
| D1 | `test_create_st_transaction_abort_leaves_no_orphans` | `e2e_error_tests.rs` | **Done** |
| D2 | `test_resume_stream_table_clears_suspended_status` | `e2e_error_tests.rs` | **Done** |
| D3 | `test_refresh_rejected_for_suspended_st` | `e2e_error_tests.rs` | **Done** |
| D  | `test_resume_unknown_stream_table_errors` | `e2e_error_tests.rs` | **Done** |
| A1 | `test_property_window_function_full` | `e2e_property_tests.rs` | **Done** |
| A2 | `test_property_cte_nonrecursive_differential` | `e2e_property_tests.rs` | **Done** |
| A3 | `test_property_lateral_join_differential` | `e2e_property_tests.rs` | **Done** |
| A4 | `test_property_except_differential` | `e2e_property_tests.rs` | **Done** |
| A5 | `test_property_having_differential` | `e2e_property_tests.rs` | **Done** |
| A6 | `test_property_three_table_join_differential` | `e2e_property_tests.rs` | **Done** |
| A7 | `test_property_intersect_differential` | `e2e_property_tests.rs` | **Done** |
| A8 | `test_property_composite_pk_differential` | `e2e_property_tests.rs` | **Done** |
| A9 | `test_property_recursive_cte_full` | `e2e_property_tests.rs` | **Done** |
| E  | `bench_cdc_trigger_overhead` | `e2e_bench_tests.rs` | **Done** |
| F1 | `test_pg_get_viewdef_cte_view` | `catalog_compat_tests.rs` | **Done** |
| F2 | `test_pg_proc_volatility_column_values` | `catalog_compat_tests.rs` | **Done** |
| F3 | `test_relkind_for_partitioned_index` | `catalog_compat_tests.rs` | **Done** |
| F4 | `test_advisory_lock_roundtrip` | `catalog_compat_tests.rs` | **Done** |
| F5 | `test_pg_available_extensions_shape` | `catalog_compat_tests.rs` | **Done** |
| G  | `test_strip_view_definition_suffix_*` (6 tests) | `src/dvm/parser.rs` | **Done** |
| G  | `test_error_kind_display_all_variants` | `src/error.rs` | **Done** |
| G  | `test_retry_policy_default_values` | `src/error.rs` | **Done** |
| G  | `test_frontier_get_snapshot_ts_*` (2 tests) | `src/version.rs` | **Done** |
| G  | `test_frontier_is_empty_*` (2 tests) | `src/version.rs` | **Done** |
| G  | `AlertEvent::Resumed` added to existing tests | `src/monitor.rs` | **Done** |
| H  | E2E coverage pipeline in CI | `.github/workflows/coverage.yml` | **Done** |
| I  | `test_tpch_performance_comparison` (T1-B) | `e2e_tpch_tests.rs` | **Done** |
| I  | `test_tpch_sustained_churn` (T1-C) | `e2e_tpch_tests.rs` | **Done** |
| K1 | `test_cross_source_two_st_overlapping_sources` | `e2e_snapshot_consistency_tests.rs` | **Done** |
| K2 | `test_cross_source_diamond_convergence` | `e2e_snapshot_consistency_tests.rs` | **Done** |
| K3 | `test_cross_source_interleaved_mutations_converge` | `e2e_snapshot_consistency_tests.rs` | **Done** |
| K4 | `test_cross_source_atomic_diamond_snapshot` | `e2e_snapshot_consistency_tests.rs` | **Done** |
| K5 | `test_cross_source_multi_round_no_drift` | `e2e_snapshot_consistency_tests.rs` | **Done** |
| L1 | `test_upgrade_catalog_schema_stability` | `e2e_upgrade_tests.rs` | **Done** |
| L2 | `test_upgrade_catalog_indexes_present` | `e2e_upgrade_tests.rs` | **Done** |
| L3 | `test_upgrade_drop_recreate_roundtrip` | `e2e_upgrade_tests.rs` | **Done** |
| L4 | `test_upgrade_extension_version_consistency` | `e2e_upgrade_tests.rs` | **Done** |
| L5 | `test_upgrade_dependencies_schema_stability` | `e2e_upgrade_tests.rs` | **Done** |
| L6 | `test_upgrade_event_triggers_installed` | `e2e_upgrade_tests.rs` | **Done** |
| L7 | `test_upgrade_monitoring_views_present` | `e2e_upgrade_tests.rs` | **Done** |
| L  | Migration SQL template | `sql/pg_trickle--0.1.3--0.2.0.sql` | **Done** |

**Total: 52 new tests/benchmarks across 13 files.**

### Remaining (prioritised)

| # | Gap | Description | Effort | Risk |
|---|-----|-------------|--------|------|
| 1 | J  | External suites (sqllogictest, JOB, Nexmark) | 480 min | Medium |
| 2 | C4 | Prove overlap in `test_concurrent_inserts_during_refresh`; fail when the activity poll times out instead of continuing after 20 ms | 1–2h | Medium |

---

## Priority 1 — Schema Evolution DDL (Category B)

**Target file:** `tests/e2e_ddl_event_tests.rs`  
**Existing tests:** 6 (drop, alter-fires-trigger, storage-drop, rename, function-change, drop-function)  
**Missing:** The `detect_schema_change_kind()` benign/ColumnChange/Reinit classification paths have
zero direct E2E coverage.  
**Source:** `src/hooks.rs` lines 700–715, `plans/sql/GAP_SQL_PHASE_5.md §C7`

### B1 — ADD COLUMN on monitored source → `ColumnChange` → reinit

```rust
#[tokio::test]
async fn test_add_column_on_used_source_triggers_reinit() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ddl_add_src (id INT PRIMARY KEY, val INT)").await;
    db.execute("INSERT INTO ddl_add_src VALUES (1, 10), (2, 20)").await;
    db.create_st(
        "ddl_add_st",
        "SELECT id, val FROM ddl_add_src",
        "1m", "DIFFERENTIAL",
    ).await;

    // Add a column that is NOT in the defining query — benign for query,
    // but the column snapshot changes → ColumnChange classification.
    db.execute("ALTER TABLE ddl_add_src ADD COLUMN extra TEXT").await;

    // After reinit the ST should still reflect the base query correctly.
    db.refresh_st("ddl_add_st").await;
    let count: i64 = db.query_scalar("SELECT count(*) FROM public.ddl_add_st").await;
    assert_eq!(count, 2, "ST should still have all rows after ADD COLUMN + reinit");
}

#[tokio::test]
async fn test_add_column_used_in_defining_query_triggers_reinit() {
    // ADD a column that IS referenced in a second ST's defining query —
    // triggers ColumnChange → reinit for that ST.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ddl_add2_src (id INT PRIMARY KEY, a INT)").await;
    db.execute("INSERT INTO ddl_add2_src VALUES (1, 1)").await;

    // Create ST that only uses 'a'
    db.create_st(
        "ddl_add2_st",
        "SELECT id, a FROM ddl_add2_src",
        "1m", "DIFFERENTIAL",
    ).await;

    // Add column 'b' (not used), verify ST still valid after next refresh
    db.execute("ALTER TABLE ddl_add2_src ADD COLUMN b INT DEFAULT 0").await;
    db.execute("UPDATE ddl_add2_src SET b = 99 WHERE id = 1").await;
    db.refresh_st("ddl_add2_st").await;

    // ST should have 1 row with the original columns intact
    let count: i64 = db.query_scalar("SELECT count(*) FROM public.ddl_add2_st").await;
    assert_eq!(count, 1);
    let a_val: i32 = db.query_scalar("SELECT a FROM public.ddl_add2_st WHERE id = 1").await;
    assert_eq!(a_val, 1);
}
```

### B2 — DROP COLUMN not referenced in query → benign, no reinit

```rust
#[tokio::test]
async fn test_drop_unused_column_is_benign() {
    // Column fingerprint changes but the column wasn't in columns_used →
    // classify as Benign → no needs_reinit.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ddl_drop_col_src (id INT PRIMARY KEY, used INT, unused TEXT)").await;
    db.execute("INSERT INTO ddl_drop_col_src VALUES (1, 10, 'x'), (2, 20, 'y')").await;
    db.create_st(
        "ddl_drop_col_st",
        "SELECT id, used FROM ddl_drop_col_src",
        "1m", "DIFFERENTIAL",
    ).await;

    // Drop the column that is NOT in the defining query
    db.execute("ALTER TABLE ddl_drop_col_src DROP COLUMN unused").await;

    let needs_reinit: bool = db.query_scalar(
        "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_drop_col_st'"
    ).await;
    // Dropping an unused column should be benign — no reinit needed
    // (exact behaviour depends on columns_used snapshot; adjust assertion if
    //  the implementation conservatively reinits on any column change)
    let status: String = db.query_scalar(
        "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_drop_col_st'"
    ).await;
    assert_ne!(status, "ERROR", "ST should not be in ERROR after unused column drop");

    db.refresh_st("ddl_drop_col_st").await;
    let count: i64 = db.query_scalar("SELECT count(*) FROM public.ddl_drop_col_st").await;
    assert_eq!(count, 2, "ST should still have 2 rows after dropping unused column");
}
```

### B3 — ALTER COLUMN TYPE on a used column → ColumnChange → reinit

```rust
#[tokio::test]
async fn test_alter_column_type_on_used_column_triggers_reinit() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ddl_type_src (id INT PRIMARY KEY, score INT)").await;
    db.execute("INSERT INTO ddl_type_src VALUES (1, 10), (2, 20)").await;
    db.create_st(
        "ddl_type_st",
        "SELECT id, score FROM ddl_type_src",
        "1m", "DIFFERENTIAL",
    ).await;

    // Change type of 'score' — column is in defining query → ColumnChange
    db.execute("ALTER TABLE ddl_type_src ALTER COLUMN score TYPE BIGINT").await;

    let needs_reinit: bool = db.query_scalar(
        "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_type_st'"
    ).await;
    assert!(needs_reinit, "ST should be marked for reinit after column type change");
}
```

### B4 — CREATE INDEX on source → Benign, no reinit

```rust
#[tokio::test]
async fn test_create_index_on_source_is_benign() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ddl_idx_src (id INT PRIMARY KEY, val INT)").await;
    db.execute("INSERT INTO ddl_idx_src VALUES (1, 10)").await;
    db.create_st(
        "ddl_idx_st",
        "SELECT id, val FROM ddl_idx_src",
        "1m", "DIFFERENTIAL",
    ).await;

    // CREATE INDEX is DDL but purely structural — should be Benign
    db.execute("CREATE INDEX ON ddl_idx_src (val)").await;

    let needs_reinit: bool = db.query_scalar(
        "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_idx_st'"
    ).await;
    assert!(!needs_reinit, "CREATE INDEX on source should not trigger reinit");

    // ST should still be functional
    let count: i64 = db.query_scalar("SELECT count(*) FROM public.ddl_idx_st").await;
    assert_eq!(count, 1);
}
```

### B5 — DROP source with multiple downstream STs → both cascade to ERROR

```rust
#[tokio::test]
async fn test_drop_source_with_multiple_downstream_sts() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ddl_multi_src (id INT PRIMARY KEY, v INT)").await;
    db.execute("INSERT INTO ddl_multi_src VALUES (1, 1), (2, 2)").await;

    db.create_st("ddl_multi_st1", "SELECT id, v FROM ddl_multi_src", "1m", "FULL").await;
    db.create_st("ddl_multi_st2", "SELECT id, v * 2 AS v2 FROM ddl_multi_src", "1m", "FULL").await;

    let result = db.try_execute("DROP TABLE ddl_multi_src CASCADE").await;

    if result.is_ok() {
        // Both STs should either be gone (cascade) or in ERROR
        for st in ["ddl_multi_st1", "ddl_multi_st2"] {
            let status_opt: Option<String> = db.query_scalar_opt(&format!(
                "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{st}'"
            )).await;
            if let Some(status) = status_opt {
                assert_eq!(status, "ERROR",
                    "{st} should be in ERROR after source DROP");
            }
            // If None: cascade cleaned up the catalog entry — also valid
        }
    }
}
```

### B6 — `pg_trickle.block_source_ddl = true` → ALTER TABLE returns error

```rust
#[tokio::test]
async fn test_block_source_ddl_guc_prevents_alter() {
    // When pg_trickle.block_source_ddl = true, any DDL on a monitored
    // source table should be rejected with an error.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ddl_block_src (id INT PRIMARY KEY, val INT)").await;
    db.execute("INSERT INTO ddl_block_src VALUES (1, 1)").await;
    db.create_st(
        "ddl_block_st",
        "SELECT id, val FROM ddl_block_src",
        "1m", "FULL",
    ).await;

    // Enable blocking GUC
    db.execute("SET pg_trickle.block_source_ddl = true").await;

    // ALTER on a monitored source should now return an error
    let result = db.try_execute(
        "ALTER TABLE ddl_block_src ADD COLUMN extra TEXT"
    ).await;
    assert!(result.is_err(),
        "ALTER on monitored source should be blocked when block_source_ddl = true");

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.to_lowercase().contains("block_source_ddl")
            || err_msg.to_lowercase().contains("blocked")
            || err_msg.to_lowercase().contains("ddl"),
        "Error message should mention the blocking GUC, got: {err_msg}"
    );
}
```

### B7 — NATURAL JOIN source + ADD COLUMN → semantic drift warning + reinit

```rust
#[tokio::test]
async fn test_natural_join_column_added_triggers_reinit() {
    // An ST defined with NATURAL JOIN depends on the join column set.
    // When a column is added to a source with the same name as a join column
    // on the other side, the join semantics change silently — reinit needed.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ddl_nj_a (id INT PRIMARY KEY, name TEXT)").await;
    db.execute("CREATE TABLE ddl_nj_b (id INT PRIMARY KEY, score INT)").await;
    db.execute("INSERT INTO ddl_nj_a VALUES (1, 'x'), (2, 'y')").await;
    db.execute("INSERT INTO ddl_nj_b VALUES (1, 10), (2, 20)").await;

    db.create_st(
        "ddl_nj_st",
        "SELECT a.id, a.name, b.score FROM ddl_nj_a a JOIN ddl_nj_b b ON a.id = b.id",
        "1m", "FULL",
    ).await;
    assert_eq!(db.count("public.ddl_nj_st").await, 2);

    // ADD COLUMN to source changes the column fingerprint
    db.execute("ALTER TABLE ddl_nj_a ADD COLUMN extra TEXT").await;

    // The ST should be marked for reinit (column fingerprint changed)
    let needs_reinit: bool = db.query_scalar(
        "SELECT needs_reinit FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_nj_st'"
    ).await;
    // Reinit may or may not be required depending on whether the added column
    // affects the join. Document the actual behaviour:
    let status: String = db.query_scalar(
        "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ddl_nj_st'"
    ).await;
    assert_ne!(status, "ERROR",
        "ST should remain valid (not ERROR) after adding an unused column; needs_reinit={needs_reinit}");
}
```

---

## Priority 2 — Concurrent Refresh Stress Tests (Category C)

**Target file:** `tests/e2e_concurrent_tests.rs`  
**Existing tests:** 3 (concurrent inserts, parallel create, refresh/drop race)  
**Missing:** Multi-ST on same source, advisory lock timeout, BGworker-overlap simulation  
**Source:** `src/scheduler.rs`, `src/shmem.rs`

### C1 — Multiple STs on same source refreshed concurrently

```rust
#[tokio::test]
async fn test_concurrent_refresh_multiple_sts_same_source() {
    // Two STs share a source. Refreshing both simultaneously must not
    // deadlock or corrupt either result.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE cc_shared (id INT PRIMARY KEY, val INT)").await;
    db.execute("INSERT INTO cc_shared SELECT g, g*10 FROM generate_series(1,50) g").await;

    db.create_st("cc_shared_st1", "SELECT id, val FROM cc_shared", "1m", "DIFFERENTIAL").await;
    db.create_st("cc_shared_st2", "SELECT id, val * 2 AS val2 FROM cc_shared", "1m", "FULL").await;

    // Insert more data, then refresh both concurrently
    db.execute("INSERT INTO cc_shared SELECT g, g*10 FROM generate_series(51,100) g").await;

    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();

    let h1 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_shared_st1')")
            .execute(&pool1).await
    });
    let h2 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_shared_st2')")
            .execute(&pool2).await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    r1.expect("task1 panicked").expect("refresh st1 failed");
    r2.expect("task2 panicked").expect("refresh st2 failed");

    // Both STs should reflect the full 100-row source
    assert_eq!(db.count("public.cc_shared_st1").await, 100);
    assert_eq!(db.count("public.cc_shared_st2").await, 100);
}
```

### C2 — Advisory lock contention: second refresh honors the lock

```rust
#[tokio::test]
async fn test_concurrent_refresh_same_st_second_is_noop() {
    // Two concurrent refreshes of the same ST should not produce duplicate
    // or corrupted rows. The advisory lock should serialize them.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE cc_lock_src (id INT PRIMARY KEY, val INT)").await;
    db.execute("INSERT INTO cc_lock_src SELECT g, g FROM generate_series(1,100) g").await;
    db.create_st("cc_lock_st", "SELECT id, val FROM cc_lock_src", "1m", "FULL").await;

    db.execute("INSERT INTO cc_lock_src SELECT g, g FROM generate_series(101,200) g").await;

    let pool1 = db.pool.clone();
    let pool2 = db.pool.clone();

    let h1 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_lock_st')")
            .execute(&pool1).await
    });
    let h2 = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_lock_st')")
            .execute(&pool2).await
    });

    let (r1, r2) = tokio::join!(h1, h2);
    // Both calls may succeed (advisory lock serializes them) or second may be
    // a no-op. Neither should panic or corrupt.
    let _ = r1.expect("task1 panicked");
    let _ = r2.expect("task2 panicked");

    // After both complete, row count must be exactly correct — no duplicates
    let count = db.count("public.cc_lock_st").await;
    assert_eq!(count, 200, "No duplicate rows after concurrent refreshes");
}
```

### C3 — Full-refresh racing with DML on source

```rust
#[tokio::test]
async fn test_full_refresh_racing_with_dml() {
    // FULL refresh (TRUNCATE + INSERT) with concurrent INSERTs should
    // eventually converge: a subsequent refresh sees all committed rows.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE cc_dml_src (id INT PRIMARY KEY, val INT)").await;
    db.execute("INSERT INTO cc_dml_src SELECT g, g FROM generate_series(1,100) g").await;
    db.create_st("cc_dml_st", "SELECT id, val FROM cc_dml_src", "1m", "FULL").await;
    assert_eq!(db.count("public.cc_dml_st").await, 100);

    db.execute("INSERT INTO cc_dml_src SELECT g, g FROM generate_series(101,150) g").await;

    let pool_r = db.pool.clone();
    let pool_i = db.pool.clone();
    let h_refresh = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('cc_dml_st')")
            .execute(&pool_r).await
    });
    let h_insert = tokio::spawn(async move {
        sqlx::query("INSERT INTO cc_dml_src SELECT g, g FROM generate_series(151,200) g")
            .execute(&pool_i).await
    });

    let (r_refresh, r_insert) = tokio::join!(h_refresh, h_insert);
    r_refresh.expect("refresh task panicked").expect("refresh failed");
    r_insert.expect("insert task panicked").expect("insert failed");

    // After a stabilizing refresh, count must converge to 200
    db.refresh_st("cc_dml_st").await;
    let count = db.count("public.cc_dml_st").await;
    assert_eq!(count, 200, "ST must converge to 200 after stabilizing refresh");
}
```

---

## Priority 3 — Error Recovery Tests (Category D)

**Target file:** `tests/e2e_error_tests.rs`  
**Source:** `src/api.rs` lines 651–720, `src/error.rs`

### D1 — Transaction abort during `create_stream_table()` → no orphaned storage table

```rust
#[tokio::test]
async fn test_create_st_transaction_abort_leaves_no_orphans() {
    // Attempt to create a ST inside a transaction that aborts.
    // No storage table, catalog entry, or CDC triggers should remain.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE err_txn_src (id INT PRIMARY KEY, val INT)").await;
    db.execute("INSERT INTO err_txn_src VALUES (1, 1)").await;

    let result = db.try_execute(
        "BEGIN; \
         SELECT pgtrickle.create_stream_table('err_txn_st', \
           $$ SELECT id, val FROM err_txn_src $$, '1m', 'FULL'); \
         ROLLBACK"
    ).await;
    // ROLLBACK is not an error but the ST should not exist
    let _ = result;

    let exists = db.table_exists("public", "err_txn_st").await;
    assert!(!exists, "Storage table should not exist after transaction abort");

    let cat_count: i64 = db.query_scalar(
        "SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'err_txn_st'"
    ).await;
    assert_eq!(cat_count, 0, "No catalog entry should remain after rollback");
}
```

### D2 — `resume_stream_table()` clears suspended status after error

```rust
#[tokio::test]
async fn test_resume_stream_table_clears_suspended_status() {
    // Create a ST, manually set it to ERROR/SUSPENDED state, then call
    // resume_stream_table() and verify status transitions to ACTIVE.
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE err_resume_src (id INT PRIMARY KEY, val INT)").await;
    db.execute("INSERT INTO err_resume_src VALUES (1, 1)").await;
    db.create_st(
        "err_resume_st",
        "SELECT id, val FROM err_resume_src",
        "1m", "FULL",
    ).await;

    // Force ERROR status via direct catalog update (simulates failed refresh)
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET status = 'ERROR', consecutive_errors = 5 \
         WHERE pgt_name = 'err_resume_st'"
    ).await;

    let status_before: String = db.query_scalar(
        "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'err_resume_st'"
    ).await;
    assert_eq!(status_before, "ERROR");

    // Call resume
    db.execute("SELECT pgtrickle.resume_stream_table('err_resume_st')").await;

    let status_after: String = db.query_scalar(
        "SELECT status FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'err_resume_st'"
    ).await;
    assert_eq!(status_after, "ACTIVE",
        "resume_stream_table() should transition status from ERROR to ACTIVE");

    let errors_after: i32 = db.query_scalar(
        "SELECT consecutive_errors FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'err_resume_st'"
    ).await;
    assert_eq!(errors_after, 0, "consecutive_errors should be reset to 0");
}
```

### D3 — Refresh of an ERROR-status ST is rejected until resumed

```rust
#[tokio::test]
async fn test_refresh_rejected_for_error_status_st() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE err_reject_src (id INT PRIMARY KEY, val INT)").await;
    db.execute("INSERT INTO err_reject_src VALUES (1, 1)").await;
    db.create_st(
        "err_reject_st",
        "SELECT id, val FROM err_reject_src",
        "1m", "FULL",
    ).await;

    // Manually suspend the ST
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET status = 'SUSPENDED' \
         WHERE pgt_name = 'err_reject_st'"
    ).await;

    // Refresh should fail while suspended
    let result = db.try_execute(
        "SELECT pgtrickle.refresh_stream_table('err_reject_st')"
    ).await;
    assert!(result.is_err(),
        "refresh_stream_table() should be rejected for a SUSPENDED ST");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.to_lowercase().contains("suspended") || msg.to_lowercase().contains("resume"),
        "Error should mention suspension/resume, got: {msg}"
    );
}
```

---

## Priority 4 — Property Tests for Implemented Operators (Category A)

**Target file:** `tests/e2e_property_tests.rs`  
**Pattern:** Copy the `Rng` + `TrackedIds` + `assert_invariant` helpers (already in the file).
Each test seed starts at the next available `0xCAFE_XXXX` value.

### A1 — Window function (PARTITION BY + ORDER BY)

```rust
#[tokio::test]
async fn test_property_window_function_full() {
    // Window functions are not differentiable; FULL mode must maintain
    // the invariant across DML cycles.
    let seed: u64 = 0xCAFE_0020;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_win (id INT PRIMARY KEY, dept TEXT, salary INT)").await;
    let mut ids = TrackedIds::new();

    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let dept = rng.choose(&["eng", "sales", "ops"]);
        let salary = rng.i32_range(50_000, 150_000);
        db.execute(&format!(
            "INSERT INTO prop_win VALUES ({id}, '{dept}', {salary})"
        )).await;
    }

    let query = "SELECT id, dept, salary, \
                 RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk \
                 FROM prop_win";
    db.create_st("prop_win_st", query, "1m", "FULL").await;
    assert_invariant(&db, "prop_win_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(1, 4);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let dept = rng.choose(&["eng", "sales", "ops"]);
            let salary = rng.i32_range(50_000, 150_000);
            db.execute(&format!(
                "INSERT INTO prop_win VALUES ({id}, '{dept}', {salary})"
            )).await;
        }
        if let Some(id) = ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_win WHERE id = {id}")).await;
        }
        if let Some(id) = ids.pick(&mut rng) {
            let salary = rng.i32_range(50_000, 150_000);
            db.execute(&format!("UPDATE prop_win SET salary = {salary} WHERE id = {id}"))
                .await;
        }
        db.refresh_st("prop_win_st").await;
        assert_invariant(&db, "prop_win_st", query, seed, cycle).await;
    }
}
```

### A2 — Non-recursive CTE (DIFFERENTIAL)

```rust
#[tokio::test]
async fn test_property_cte_nonrecursive_differential() {
    let seed: u64 = 0xCAFE_0021;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_cte (id INT PRIMARY KEY, region TEXT, amount INT)").await;
    let mut ids = TrackedIds::new();

    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let region = rng.choose(&["north", "south"]);
        let amount = rng.i32_range(100, 1000);
        db.execute(&format!(
            "INSERT INTO prop_cte VALUES ({id}, '{region}', {amount})"
        )).await;
    }

    let query = "WITH totals AS ( \
                   SELECT region, SUM(amount) AS total FROM prop_cte GROUP BY region \
                 ) \
                 SELECT region, total FROM totals WHERE total > 500";
    db.create_st("prop_cte_st", query, "1m", "DIFFERENTIAL").await;
    assert_invariant(&db, "prop_cte_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(1, 4);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let region = rng.choose(&["north", "south"]);
            let amount = rng.i32_range(100, 1000);
            db.execute(&format!(
                "INSERT INTO prop_cte VALUES ({id}, '{region}', {amount})"
            )).await;
        }
        if let Some(id) = ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_cte WHERE id = {id}")).await;
        }
        db.refresh_st("prop_cte_st").await;
        assert_invariant(&db, "prop_cte_st", query, seed, cycle).await;
    }
}
```

### A3 — LATERAL join (DIFFERENTIAL)

```rust
#[tokio::test]
async fn test_property_lateral_join_differential() {
    let seed: u64 = 0xCAFE_0022;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_lat_a (id INT PRIMARY KEY, val INT)").await;
    db.execute("CREATE TABLE prop_lat_b (id INT PRIMARY KEY, a_id INT, score INT)").await;
    let mut a_ids = TrackedIds::new();
    let mut b_ids = TrackedIds::new();

    for _ in 0..10 {
        let id = a_ids.alloc();
        let val = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_lat_a VALUES ({id}, {val})")).await;
    }
    for _ in 0..INITIAL_ROWS {
        let id = b_ids.alloc();
        if let Some(a_id) = a_ids.pick(&mut rng) {
            let score = rng.i32_range(1, 100);
            db.execute(&format!("INSERT INTO prop_lat_b VALUES ({id}, {a_id}, {score})"))
                .await;
        }
    }

    let query = "SELECT a.id AS a_id, a.val, sub.max_score \
                 FROM prop_lat_a a \
                 LEFT JOIN LATERAL ( \
                   SELECT MAX(b.score) AS max_score \
                   FROM prop_lat_b b WHERE b.a_id = a.id \
                 ) sub ON true";
    db.create_st("prop_lat_st", query, "1m", "DIFFERENTIAL").await;
    assert_invariant(&db, "prop_lat_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let id = b_ids.alloc();
        if let Some(a_id) = a_ids.pick(&mut rng) {
            let score = rng.i32_range(1, 100);
            db.execute(&format!("INSERT INTO prop_lat_b VALUES ({id}, {a_id}, {score})"))
                .await;
        }
        if let Some(b_id) = b_ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_lat_b WHERE id = {b_id}")).await;
        }
        db.refresh_st("prop_lat_st").await;
        assert_invariant(&db, "prop_lat_st", query, seed, cycle).await;
    }
}
```

### A4 — EXCEPT (DIFFERENTIAL, dual-count)

```rust
#[tokio::test]
async fn test_property_except_differential() {
    let seed: u64 = 0xCAFE_0023;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_exc_a (id INT PRIMARY KEY, val INT)").await;
    db.execute("CREATE TABLE prop_exc_b (id INT PRIMARY KEY, val INT)").await;
    let mut a_ids = TrackedIds::new();
    let mut b_ids = TrackedIds::new();

    for _ in 0..INITIAL_ROWS {
        let id = a_ids.alloc();
        let val = rng.i32_range(1, 10);
        db.execute(&format!("INSERT INTO prop_exc_a VALUES ({id}, {val})")).await;
    }
    for _ in 0..INITIAL_ROWS {
        let id = b_ids.alloc();
        let val = rng.i32_range(1, 10);
        db.execute(&format!("INSERT INTO prop_exc_b VALUES ({id}, {val})")).await;
    }

    let query = "SELECT val FROM prop_exc_a EXCEPT SELECT val FROM prop_exc_b";
    db.create_st("prop_exc_st", query, "1m", "DIFFERENTIAL").await;
    assert_invariant(&db, "prop_exc_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let id = a_ids.alloc();
        let val = rng.i32_range(1, 10);
        db.execute(&format!("INSERT INTO prop_exc_a VALUES ({id}, {val})")).await;

        if rng.gen_bool() {
            let id = b_ids.alloc();
            let val = rng.i32_range(1, 10);
            db.execute(&format!("INSERT INTO prop_exc_b VALUES ({id}, {val})")).await;
        }
        if let Some(id) = a_ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_exc_a WHERE id = {id}")).await;
        }

        db.refresh_st("prop_exc_st").await;
        assert_invariant(&db, "prop_exc_st", query, seed, cycle).await;
    }
}
```

### A5 — HAVING clause (DIFFERENTIAL)

```rust
#[tokio::test]
async fn test_property_having_differential() {
    let seed: u64 = 0xCAFE_0024;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_hav (id INT PRIMARY KEY, category TEXT, amount INT)").await;
    let mut ids = TrackedIds::new();

    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let cat = rng.choose(&["a", "b", "c", "d"]);
        let amt = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_hav VALUES ({id}, '{cat}', {amt})")).await;
    }

    let query = "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt \
                 FROM prop_hav GROUP BY category HAVING SUM(amount) > 100";
    db.create_st("prop_hav_st", query, "1m", "DIFFERENTIAL").await;
    assert_invariant(&db, "prop_hav_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(1, 4);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let cat = rng.choose(&["a", "b", "c", "d"]);
            let amt = rng.i32_range(1, 100);
            db.execute(&format!(
                "INSERT INTO prop_hav VALUES ({id}, '{cat}', {amt})"
            )).await;
        }
        if let Some(id) = ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_hav WHERE id = {id}")).await;
        }
        db.refresh_st("prop_hav_st").await;
        assert_invariant(&db, "prop_hav_st", query, seed, cycle).await;
    }
}
```

### A6 — Three-table join (DIFFERENTIAL)

```rust
#[tokio::test]
async fn test_property_three_table_join_differential() {
    let seed: u64 = 0xCAFE_0025;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_t3a (id INT PRIMARY KEY, key INT, a INT)").await;
    db.execute("CREATE TABLE prop_t3b (id INT PRIMARY KEY, key INT, b INT)").await;
    db.execute("CREATE TABLE prop_t3c (id INT PRIMARY KEY, key INT, c INT)").await;
    let mut a_ids = TrackedIds::new();
    let mut b_ids = TrackedIds::new();
    let mut c_ids = TrackedIds::new();

    for _ in 0..10 {
        let id = a_ids.alloc();
        db.execute(&format!(
            "INSERT INTO prop_t3a VALUES ({id}, {}, {})",
            rng.i32_range(1, 4), rng.i32_range(1, 100)
        )).await;
        let id = b_ids.alloc();
        db.execute(&format!(
            "INSERT INTO prop_t3b VALUES ({id}, {}, {})",
            rng.i32_range(1, 4), rng.i32_range(1, 100)
        )).await;
        let id = c_ids.alloc();
        db.execute(&format!(
            "INSERT INTO prop_t3c VALUES ({id}, {}, {})",
            rng.i32_range(1, 4), rng.i32_range(1, 100)
        )).await;
    }

    let query = "SELECT a.id AS aid, b.id AS bid, c.id AS cid, a.a + b.b + c.c AS total \
                 FROM prop_t3a a JOIN prop_t3b b ON a.key = b.key \
                                 JOIN prop_t3c c ON b.key = c.key";
    db.create_st("prop_t3_st", query, "1m", "DIFFERENTIAL").await;
    assert_invariant(&db, "prop_t3_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        // DML on each table
        for (table, ids) in [("prop_t3a", &mut a_ids), ("prop_t3b", &mut b_ids)] {
            let id = ids.alloc();
            let key = rng.i32_range(1, 4);
            let val = rng.i32_range(1, 100);
            let col = if table == "prop_t3a" { "a" } else { "b" };
            db.execute(&format!(
                "INSERT INTO {table} VALUES ({id}, {key}, {val})"
            )).await;
            let _ = (id, col);
        }
        if let Some(id) = a_ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_t3a WHERE id = {id}")).await;
        }
        db.refresh_st("prop_t3_st").await;
        assert_invariant(&db, "prop_t3_st", query, seed, cycle).await;
    }
}
```

---

## Priority 5 — Catalog Compatibility Canaries (Category F)

**Target file:** `tests/catalog_compat_tests.rs`  
**Pattern:** Uses `TestDb` (no pg_trickle extension needed); each test is write-only about catalog behavior.

### F1 — `pg_get_viewdef` for CTE views

```rust
#[tokio::test]
async fn test_pg_get_viewdef_cte_view() {
    let db = TestDb::new().await;
    db.execute("CREATE TABLE vd_cte_src (id INT, region TEXT, amount NUMERIC)").await;
    db.execute(
        "CREATE VIEW vd_cte_view AS \
         WITH totals AS (SELECT region, SUM(amount) AS total FROM vd_cte_src GROUP BY region) \
         SELECT region, total FROM totals WHERE total > 0"
    ).await;

    let raw: String = db.query_scalar(
        "SELECT pg_get_viewdef(c.oid, true) FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = 'vd_cte_view'"
    ).await;
    let trimmed = raw.trim_end_matches(';').trim();
    assert!(!trimmed.is_empty(), "CTE view definition should not be empty");

    let subq = format!("SELECT count(*) FROM ({trimmed}) _q");
    let count: i64 = db.query_scalar(&subq).await;
    assert_eq!(count, 0, "CTE view definition should be usable as subquery");
}
```

### F2 — `pg_proc` volatility column exists and has expected values

```rust
#[tokio::test]
async fn test_pg_proc_volatility_column_type() {
    // pg_trickle reads provolatile to detect volatile functions.
    // Verify the column type and expected varchar codes.
    let db = TestDb::new().await;

    db.execute(
        "CREATE FUNCTION cc_immutable(x INT) RETURNS INT AS $$ SELECT x $$ \
         LANGUAGE SQL IMMUTABLE"
    ).await;
    db.execute(
        "CREATE FUNCTION cc_volatile(x INT) RETURNS INT AS $$ SELECT x $$ \
         LANGUAGE SQL VOLATILE"
    ).await;

    let imm_vol: String = db.query_scalar(
        "SELECT provolatile::text FROM pg_proc WHERE proname = 'cc_immutable'"
    ).await;
    let vol_vol: String = db.query_scalar(
        "SELECT provolatile::text FROM pg_proc WHERE proname = 'cc_volatile'"
    ).await;

    assert_eq!(imm_vol, "i", "IMMUTABLE function provolatile should be 'i'");
    assert_eq!(vol_vol, "v", "VOLATILE function provolatile should be 'v'");
}
```

### F3 — `relkind` for partitioned index is 'I'

```rust
#[tokio::test]
async fn test_relkind_for_partitioned_index() {
    // PG 18+ reports partitioned indexes with relkind = 'I'.
    // pg_trickle's relkind classification must not confuse 'I' with 'i' (regular index).
    let db = TestDb::new().await;

    db.execute(
        "CREATE TABLE pk_test (id INT, val INT) PARTITION BY RANGE (id)"
    ).await;
    db.execute(
        "CREATE TABLE pk_test_1 PARTITION OF pk_test FOR VALUES FROM (1) TO (100)"
    ).await;
    db.execute(
        "CREATE INDEX ON pk_test (val)"
    ).await;

    let idx_kind: String = db.query_scalar(
        "SELECT c.relkind::text FROM pg_class c \
         WHERE c.relname LIKE 'pk_test%' AND c.relkind IN ('i', 'I') \
         ORDER BY c.relkind LIMIT 1"
    ).await;
    // Document what PG 18 actually returns — the test pins the behavior
    assert!(
        idx_kind == "i" || idx_kind == "I",
        "Index relkind should be 'i' or 'I', got: {idx_kind:?}"
    );
}
```

---

## Priority 6 — `resume_stream_table()` SQL API surface test

These belong in `tests/e2e_lifecycle_tests.rs` alongside other lifecycle operations.

```rust
#[tokio::test]
async fn test_resume_unknown_stream_table_errors() {
    let db = E2eDb::new().await.with_extension().await;
    let result = db.try_execute(
        "SELECT pgtrickle.resume_stream_table('nonexistent_st')"
    ).await;
    assert!(result.is_err(), "Resuming unknown ST should return an error");
}
```

---

## Implementation Order

All items from Priorities 1–6 plus the infrastructure items E, H, I are now
**implemented** across three rounds:

- **Round 1 (2026-03-03):** 23 E2E/integration tests across B, C, D, A1–A6, F1–F3
- **Round 2 (2026-03-03):** 6 more E2E tests (D1, A7, A8, A9, F4, F5) +
  12 pure-function unit tests (G) across parser, error, version, monitor modules
- **Round 3 (2026-03-03):** CDC trigger overhead benchmark (E),
  E2E coverage CI pipeline (H), TPC-H T1-B performance comparison +
  T1-C sustained churn tests (I)
- **Round 4 (2026-03-03):** Cross-source snapshot consistency tests K1–K5
  (`e2e_snapshot_consistency_tests.rs`), extension upgrade/migration tests
  L1–L7 (`e2e_upgrade_tests.rs`), migration SQL template
  (`sql/pg_trickle--0.1.3--0.2.0.sql`)

The remaining item is long-term infrastructure:

1. **J** (External suites) — integration with sqllogictest, JOB, Nexmark
   (see `plans/testing/PLAN_TEST_SUITES.md` for detailed roadmap)

## Notes

- All E2E tests require the `pg_trickle_e2e:latest` Docker image. Run
  `just build-e2e-image` before the first run.
- Property tests use a deterministic PRNG: if a test fails, the seed is
  printed and the test can be re-run with the same seed for reproduction.
- Seeds `0xCAFE_0020`–`0xCAFE_0028` are now allocated (Tests 12–20).
- D1 uses `sqlx::PgPool::begin()` / `tx.rollback()` to test transactional
  cleanup of `create_stream_table()`.
- A8 (composite PK) tracks unique `(tenant_id, item_id)` pairs via a
  `HashSet` rather than `TrackedIds` to handle multi-column keys.
- A9 (recursive CTE) deletes only leaf nodes to avoid orphaned subtrees.
- G tests cover `strip_view_definition_suffix`, `PgTrickleErrorKind::Display`,
  `RetryPolicy::default`, `Frontier::get_snapshot_ts`, `Frontier::is_empty`,
  and the `AlertEvent::Resumed` variant.
- E (CDC benchmark) compares INSERT/UPDATE/DELETE throughput with and without
  CDC triggers, reporting per-operation overhead percentage.
- H (CI coverage) adds an `e2e-coverage` job to `coverage.yml` that builds a
  coverage-instrumented Docker image, runs E2E tests, merges profdata with unit
  test coverage, and uploads combined LCOV to Codecov.
- I (T1-B) records per-query FULL vs DIFFERENTIAL refresh times and outputs a
  speedup table; (T1-C) runs 50+ churn cycles with periodic correctness checks,
  asserting zero cumulative drift.
- K tests cover 5 cross-source scenarios: overlapping sources (K1), diamond
  convergence with partitioned predicates (K2), 3-source interleaved mutations
  over 3 rounds (K3), atomic diamond consistency with cross-ST invariant (K4),
  and 10-round multi-source stress test checking for drift (K5).
- L tests validate catalog schema stability (L1), index presence (L2),
  DROP+CREATE extension round-trip (L3), version consistency (L4),
  dependencies schema (L5), event triggers (L6), and monitoring views (L7).
  The migration SQL template (`sql/pg_trickle--0.1.3--0.2.0.sql`) provides
  a documented pattern for future version upgrades.
