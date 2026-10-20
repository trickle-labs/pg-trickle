//! E2E tests that verify the examples from docs/GETTING_STARTED.md work correctly.
//!
//! Covers every substantive code example in the guide:
//!
//! - Step 2: `department_tree` (recursive CTE, '1m', DIFFERENTIAL) — creation
//!   and initial data
//! - Step 3: `department_stats` (LEFT JOIN + agg, NULL schedule, DIFFERENTIAL)
//!   and `department_report` (GROUP BY 1, '1m', DIFFERENTIAL) — chained creation
//! - Step 4a: Single INSERT into `employees` cascades through `department_stats`
//!   and `department_report` (depth = 2, one group updated)
//! - Step 4b: INSERT into `departments` (root table) cascades through all three
//!   layers (semi-naive evaluation in `department_tree`)
//! - Step 4c: UPDATE department name cascades via DRed through all three layers
//!   (all paths containing old name are updated)
//! - Step 4d: DELETE an employee adjusts aggregate correctly
//! - Step 7: Drop in reverse dependency order — storage tables, triggers, and
//!   catalog entries are all removed
//! - Regression: function-call GROUP BY (split_part) against a base-table source
//!   exercises the differential DVM path; this was the path that triggered the
//!   "column d.split_part(...) does not exist" bug (the department_report query
//!   uses a stream-table source so manual refresh always falls back to FULL,
//!   masking the bug in the standard step-4a test)
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

const Q_TREE: &str = "WITH RECURSIVE tree AS (
            SELECT id, name, parent_id, name AS path, 0 AS depth
            FROM departments
            WHERE parent_id IS NULL
            UNION ALL
            SELECT d.id, d.name, d.parent_id,
                   tree.path || ' > ' || d.name AS path,
                   tree.depth + 1
            FROM departments d
            JOIN tree ON d.parent_id = tree.id
        )
        SELECT id, name, parent_id, path, depth FROM tree";

const Q_STATS: &str = "SELECT
                t.id          AS department_id,
                t.name        AS department_name,
                t.path        AS full_path,
                t.depth,
                COUNT(e.id)                AS headcount,
                COALESCE(SUM(e.salary), 0) AS total_salary,
                COALESCE(AVG(e.salary), 0) AS avg_salary
            FROM department_tree t
            LEFT JOIN employees e ON e.department_id = t.id
            GROUP BY t.id, t.name, t.path, t.depth";

const Q_REPORT: &str = "SELECT
            split_part(full_path, ' > ', 2) AS division,
            SUM(headcount)                  AS total_headcount,
            SUM(total_salary)               AS total_payroll
        FROM department_stats
        WHERE depth >= 1
        GROUP BY 1";

// ── Base table setup helpers ─────────────────────────────────────────────────

/// Create `departments` and `employees` tables exactly as shown in Step 1.
async fn setup_base_tables(db: &E2eDb) {
    db.execute(
        "CREATE TABLE departments (
            id        SERIAL PRIMARY KEY,
            name      TEXT NOT NULL,
            parent_id INT REFERENCES departments(id)
        )",
    )
    .await;

    db.execute(
        "CREATE TABLE employees (
            id            SERIAL PRIMARY KEY,
            name          TEXT NOT NULL,
            department_id INT NOT NULL REFERENCES departments(id),
            salary        NUMERIC(10,2) NOT NULL
        )",
    )
    .await;

    // Top-level
    db.execute("INSERT INTO departments (id, name, parent_id) VALUES (1, 'Company', NULL)")
        .await;

    // Second level
    db.execute(
        "INSERT INTO departments (id, name, parent_id) VALUES
            (2, 'Engineering', 1),
            (3, 'Sales',       1),
            (4, 'Operations',  1)",
    )
    .await;

    // Third level (under Engineering)
    db.execute(
        "INSERT INTO departments (id, name, parent_id) VALUES
            (5, 'Backend',  2),
            (6, 'Frontend', 2),
            (7, 'Platform', 2)",
    )
    .await;

    // Employees
    db.execute(
        "INSERT INTO employees (name, department_id, salary) VALUES
            ('Alice',   5, 120000),
            ('Bob',     5, 115000),
            ('Charlie', 6, 110000),
            ('Diana',   7, 130000),
            ('Eve',     3, 95000),
            ('Frank',   3, 90000),
            ('Grace',   4, 100000)",
    )
    .await;
}

/// Create `department_tree` — recursive CTE, '1m', DIFFERENTIAL (Step 2).
async fn create_department_tree(db: &E2eDb) {
    db.create_st("department_tree", Q_TREE, "1m", "DIFFERENTIAL")
        .await;
}

/// Create `department_stats` — LEFT JOIN + agg, NULL schedule, DIFFERENTIAL (Step 3).
async fn create_department_stats(db: &E2eDb) {
    // NULL schedule = CALCULATED mode (inherit from downstream).
    // The helper only accepts &str, so use execute() directly.
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table(
            'department_stats',
            $${}$$,
            'calculated',
            'DIFFERENTIAL'
        )",
        Q_STATS
    ))
    .await;
}

/// Create `department_report` — ordinal GROUP BY 1, '1m', DIFFERENTIAL (Step 3).
async fn create_department_report(db: &E2eDb) {
    db.create_st("department_report", Q_REPORT, "1m", "DIFFERENTIAL")
        .await;
    // Keep combined and sequential mutation windows under the guide's manual control.
    #[cfg(not(feature = "light-e2e"))]
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['department_tree', 'department_stats', 'department_report'])")
        .await;
}

// ── Step 2 ───────────────────────────────────────────────────────────────────

/// Step 2: `department_tree` is populated on creation with all 7 departments,
/// correct paths, and correct depths.
#[tokio::test]
async fn test_getting_started_step2_department_tree_initial_data() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;

    // 7 rows: one per department
    assert_eq!(
        db.count("public.department_tree").await,
        7,
        "Should have one row per department"
    );

    // Root department at depth 0
    let company_depth: i32 = db
        .query_scalar("SELECT depth FROM department_tree WHERE name = 'Company'")
        .await;
    assert_eq!(company_depth, 0);

    // Third-level path
    let backend_path: String = db
        .query_scalar("SELECT path FROM department_tree WHERE name = 'Backend'")
        .await;
    assert_eq!(backend_path, "Company > Engineering > Backend");

    // Third-level depth
    let backend_depth: i32 = db
        .query_scalar("SELECT depth FROM department_tree WHERE name = 'Backend'")
        .await;
    assert_eq!(backend_depth, 2);

    // Status checks
    let (status, mode, populated, errors) = db.pgt_status("department_tree").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "DIFFERENTIAL");
    assert!(populated);
    assert_eq!(errors, 0);
}

// ── Step 3 ───────────────────────────────────────────────────────────────────

/// Step 3: `department_stats` is populated with correct headcounts and salaries.
#[tokio::test]
async fn test_getting_started_step3_department_stats_initial_data() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;
    create_department_stats(&db).await;

    assert_eq!(
        db.count("public.department_stats").await,
        7,
        "One row per department"
    );

    // Backend: Alice (120000) + Bob (115000) = 2 employees, 235000 total
    let (headcount, total_salary): (i64, String) = sqlx::query_as(
        "SELECT headcount::bigint, total_salary::text
         FROM department_stats WHERE department_name = 'Backend'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("Backend stats query failed");
    assert_eq!(headcount, 2);
    assert_eq!(total_salary, "235000.00");

    // Company has no direct employees (no one reports to id=1)
    let company_headcount: i64 = db
        .query_scalar(
            "SELECT headcount::bigint FROM department_stats WHERE department_name = 'Company'",
        )
        .await;
    assert_eq!(company_headcount, 0);

    // Sales: Eve (95000) + Frank (90000) = 2 employees, 185000 total
    let (sales_hc, sales_sal): (i64, String) = sqlx::query_as(
        "SELECT headcount::bigint, total_salary::text
         FROM department_stats WHERE department_name = 'Sales'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("Sales stats query failed");
    assert_eq!(sales_hc, 2);
    assert_eq!(sales_sal, "185000.00");

    let (status, _, populated, errors) = db.pgt_status("department_stats").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
    assert_eq!(errors, 0);
}

/// Step 3: `department_report` — ordinal GROUP BY 1 — is created and correctly populated.
#[tokio::test]
async fn test_getting_started_step3_department_report_initial_data() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;
    create_department_stats(&db).await;
    create_department_report(&db).await;

    // Depth >= 1 means Company (depth=0) is excluded; 3 divisions remain
    assert_eq!(
        db.count("public.department_report").await,
        3,
        "Engineering, Sales, Operations"
    );

    let divs: Vec<String> =
        sqlx::query_scalar("SELECT division FROM department_report ORDER BY division")
            .fetch_all(&db.pool)
            .await
            .expect("department_report query failed");
    assert_eq!(divs, vec!["Engineering", "Operations", "Sales"]);

    // Engineering: Alice(5) + Bob(5) + Charlie(6) + Diana(7) = 4 employees
    let eng_hc: i64 = db
        .query_scalar(
            "SELECT total_headcount::bigint FROM department_report WHERE division = 'Engineering'",
        )
        .await;
    assert_eq!(eng_hc, 4);

    // Engineering total payroll: 120000+115000+110000+130000 = 475000
    let eng_payroll: String = db
        .query_scalar(
            "SELECT total_payroll::text FROM department_report WHERE division = 'Engineering'",
        )
        .await;
    assert_eq!(eng_payroll, "475000.00");

    let (status, _, populated, errors) = db.pgt_status("department_report").await;
    assert_eq!(status, "ACTIVE");
    assert!(populated);
    assert_eq!(errors, 0);
}

// ── Step 4a ──────────────────────────────────────────────────────────────────

/// Step 4a: INSERT Heidi into Frontend. Only Frontend group in `department_stats`
/// is updated; only Engineering row in `department_report` is updated.
#[tokio::test]
async fn test_getting_started_step4a_employee_insert_cascades() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;
    create_department_stats(&db).await;
    create_department_report(&db).await;

    // Pre-conditions
    let pre_hc: i64 = db
        .query_scalar(
            "SELECT headcount::bigint FROM department_stats WHERE department_name = 'Frontend'",
        )
        .await;
    assert_eq!(pre_hc, 1, "Frontend starts with Charlie only");

    // Insert Heidi
    db.execute("INSERT INTO employees (name, department_id, salary) VALUES ('Heidi', 6, 105000)")
        .await;

    // Refresh in source-to-sink order (department_tree not involved)
    db.refresh_st_until_converged("department_stats").await;
    db.refresh_st_until_converged("department_report").await;

    // Frontend: 1 → 2 employees; 110000 → 215000
    let (post_hc, post_sal): (i64, String) = sqlx::query_as(
        "SELECT headcount::bigint, total_salary::text
         FROM department_stats WHERE department_name = 'Frontend'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("Frontend stats failed");
    assert_eq!(post_hc, 2);
    assert_eq!(post_sal, "215000.00");

    // Engineering division: 4 → 5; 475000 → 580000
    let (eng_hc, eng_payroll): (i64, String) = sqlx::query_as(
        "SELECT total_headcount::bigint, total_payroll::text
         FROM department_report WHERE division = 'Engineering'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("Engineering report failed");
    assert_eq!(eng_hc, 5);
    assert_eq!(eng_payroll, "580000.00");

    // Other divisions untouched
    let sales_hc: i64 = db
        .query_scalar(
            "SELECT total_headcount::bigint FROM department_report WHERE division = 'Sales'",
        )
        .await;
    assert_eq!(sales_hc, 2, "Sales not affected by Heidi's insertion");
}

// ── Step 4b ──────────────────────────────────────────────────────────────────

/// Step 4b: INSERT DevOps under Engineering cascades through all three layers.
/// `department_tree` uses semi-naive evaluation; DevOps appears with correct
/// path and depth in all three stream tables.
#[tokio::test]
async fn test_getting_started_step4b_department_insert_cascades_all_layers() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;
    create_department_stats(&db).await;
    create_department_report(&db).await;

    db.execute("INSERT INTO departments (id, name, parent_id) VALUES (8, 'DevOps', 2)")
        .await;

    // Refresh all three layers in dependency order
    db.refresh_st_until_converged("department_tree").await;
    db.refresh_st_until_converged("department_stats").await;
    db.refresh_st_until_converged("department_report").await;

    // department_tree: DevOps at depth 2 with correct path
    let (devops_path, devops_depth): (String, i32) =
        sqlx::query_as("SELECT path, depth FROM department_tree WHERE name = 'DevOps'")
            .fetch_one(&db.pool)
            .await
            .expect("DevOps tree row missing");
    assert_eq!(devops_path, "Company > Engineering > DevOps");
    assert_eq!(devops_depth, 2);
    assert_eq!(
        db.count("public.department_tree").await,
        8,
        "7 + DevOps = 8 rows"
    );

    // department_stats: DevOps present with 0 employees
    let devops_hc: i64 = db
        .query_scalar(
            "SELECT headcount::bigint FROM department_stats WHERE department_name = 'DevOps'",
        )
        .await;
    assert_eq!(devops_hc, 0);
    assert_eq!(db.count("public.department_stats").await, 8);

    // department_report: Engineering headcount unchanged (DevOps has 0 staff)
    let eng_hc: i64 = db
        .query_scalar(
            "SELECT total_headcount::bigint FROM department_report WHERE division = 'Engineering'",
        )
        .await;
    assert_eq!(
        eng_hc, 4,
        "DevOps has no employees; Engineering headcount unchanged"
    );
}

// ── Step 4c ──────────────────────────────────────────────────────────────────

/// Step 4c: UPDATE department name "Engineering" → "R&D" cascades via DRed
/// through all three stream tables. All paths and the division name are updated.
#[tokio::test]
async fn test_getting_started_step4c_department_rename_cascades() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;
    create_department_stats(&db).await;
    create_department_report(&db).await;

    db.execute("UPDATE departments SET name = 'R&D' WHERE id = 2")
        .await;

    db.refresh_st_until_converged("department_tree").await;
    db.refresh_st_until_converged("department_stats").await;
    db.refresh_st_until_converged("department_report").await;

    // department_tree: no path contains "Engineering"
    let old_path_count: i64 = db
        .query_scalar("SELECT count(*) FROM department_tree WHERE path LIKE '%Engineering%'")
        .await;
    assert_eq!(old_path_count, 0, "No paths should reference 'Engineering'");

    // R&D itself + 3 sub-teams = 4 rows with 'R&D' in path
    let rnd_count: i64 = db
        .query_scalar("SELECT count(*) FROM department_tree WHERE path LIKE '%R&D%'")
        .await;
    assert_eq!(rnd_count, 4);

    let backend_path: String = db
        .query_scalar("SELECT path FROM department_tree WHERE name = 'Backend'")
        .await;
    assert_eq!(backend_path, "Company > R&D > Backend");

    // department_stats: full_path updated
    let rnd_full_path: String = db
        .query_scalar("SELECT full_path FROM department_stats WHERE department_name = 'R&D'")
        .await;
    assert_eq!(rnd_full_path, "Company > R&D");

    // department_report: "Engineering" gone, "R&D" present with same headcount
    let old_div_count: i64 = db
        .query_scalar("SELECT count(*) FROM department_report WHERE division = 'Engineering'")
        .await;
    assert_eq!(old_div_count, 0, "'Engineering' division should be removed");

    let rnd_hc: i64 = db
        .query_scalar(
            "SELECT total_headcount::bigint FROM department_report WHERE division = 'R&D'",
        )
        .await;
    assert_eq!(rnd_hc, 4, "'R&D' should have the same 4 employees");

    // department_stats: sub-department rows Backend/Frontend/Platform must still
    // exist after the DRed cascade — their full_path should reference R&D, not
    // Engineering.  Regression guard for the ΔL⋈ΔR overcounting bug where
    // sub-department rows were silently deleted from department_stats.
    let backend_path: String = db
        .query_scalar("SELECT full_path FROM department_stats WHERE department_name = 'Backend'")
        .await;
    assert_eq!(
        backend_path, "Company > R&D > Backend",
        "Backend full_path must be updated to R&D after rename"
    );
    let backend_hc: i64 = db
        .query_scalar(
            "SELECT headcount::bigint FROM department_stats WHERE department_name = 'Backend'",
        )
        .await;
    assert_eq!(
        backend_hc, 2,
        "Backend headcount must be preserved after rename"
    );

    let frontend_path: String = db
        .query_scalar("SELECT full_path FROM department_stats WHERE department_name = 'Frontend'")
        .await;
    assert_eq!(frontend_path, "Company > R&D > Frontend");

    let platform_path: String = db
        .query_scalar("SELECT full_path FROM department_stats WHERE department_name = 'Platform'")
        .await;
    assert_eq!(platform_path, "Company > R&D > Platform");
}

#[tokio::test]
async fn test_getting_started_refresh_retry_waits_for_concurrent_lock() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE retry_source (id INT PRIMARY KEY)")
        .await;
    db.create_st(
        "retry_report",
        "SELECT id FROM retry_source",
        "24h",
        "DIFFERENTIAL",
    )
    .await;
    #[cfg(not(feature = "light-e2e"))]
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['retry_report'])")
        .await;

    for (id, lock_sql) in [
        "SELECT pg_advisory_xact_lock(pgt_id) FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retry_report'",
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'retry_report' FOR UPDATE",
    ]
    .iter()
    .enumerate()
    {
        db.execute(&format!("INSERT INTO retry_source VALUES ({id})"))
            .await;
        let mut lock = db.pool.begin().await.expect("begin competing refresh");
        sqlx::query(sqlx::AssertSqlSafe(*lock_sql))
            .execute(&mut *lock)
            .await
            .expect("hold competing refresh lock");

        let refresh = db.refresh_st_with_retry("retry_report");
        tokio::pin!(refresh);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(250), &mut refresh)
                .await
                .is_err(),
            "refresh helper must wait when the SQL API skips a busy refresh"
        );
        lock.commit().await.expect("release competing refresh lock");
        refresh.await;
        db.assert_st_matches_query("retry_report", "SELECT id FROM retry_source")
            .await;
    }
    db.refresh_st_with_retry("retry_report").await;
}

// ── Step 4c → 4d combined (regression: G17-STBASE) ───────────────────────────

/// Regression — G17-STBASE: rename a department (step 4c, which produces D+I
/// pairs in the department_tree ST change buffer) followed immediately by
/// deleting an employee in that department (step 4d).
///
/// When both changes are pending simultaneously at department_stats' refresh,
/// Part 2 of the LEFT JOIN delta must use L₀ (pre-change left snapshot) so
/// that right-side changes are attributed to the old group key.  The EC-02
/// correction cancels the ΔL_D ⋈ ΔR cross-term double-counting between
/// Part 1b and Part 2.  The final state should be Backend with headcount=1
/// (just Alice), using DIFFERENTIAL mode — no FULL fallback needed.
#[tokio::test]
async fn test_getting_started_step4c_then_4d_combined_regression() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;
    create_department_stats(&db).await;
    create_department_report(&db).await;

    // Step 4c: rename Engineering → R&D; refresh department_tree so the DRed
    // D+I pairs land in the ST change buffer for department_stats.
    db.execute("UPDATE departments SET name = 'R&D' WHERE id = 2")
        .await;
    db.refresh_st_until_converged("department_tree").await;

    // Step 4d: delete Bob immediately — department_stats now has BOTH pending
    // ST-source changes (from the DRed cascade above) and a base-table change
    // (the employee deletion).  The L₀ fix in outer_join.rs ensures the
    // differential formula handles this correctly without FULL fallback.
    db.execute("DELETE FROM employees WHERE name = 'Bob'").await;

    // Refresh department_stats (DIFFERENTIAL — L₀ + EC-02 correction handles
    // the combined ST + base-table changes correctly).
    db.refresh_st_until_converged("department_stats").await;
    db.refresh_st_until_converged("department_report").await;

    // Backend must exist with headcount=1 (only Alice remains).
    let (hc, total_sal, avg_sal): (i64, String, String) = sqlx::query_as(
        "SELECT headcount::bigint,
                round(total_salary, 2)::text,
                round(avg_salary, 2)::text
         FROM department_stats WHERE department_name = 'Backend'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("Backend stats row must exist after rename+delete");
    assert_eq!(hc, 1, "headcount must be 1 after Bob's deletion");
    assert_eq!(
        total_sal, "120000.00",
        "total_salary must be Alice's salary"
    );
    assert_eq!(avg_sal, "120000.00");

    // full_path must reflect the rename.
    let backend_path: String = db
        .query_scalar("SELECT full_path FROM department_stats WHERE department_name = 'Backend'")
        .await;
    assert_eq!(backend_path, "Company > R&D > Backend");

    // R&D division in department_report: 3 employees (Alice, Charlie, Diana).
    let rnd_hc: i64 = db
        .query_scalar(
            "SELECT total_headcount::bigint FROM department_report WHERE division = 'R&D'",
        )
        .await;
    assert_eq!(rnd_hc, 3, "R&D headcount must be 3 after Bob's deletion");
}

// ── Step 4c → 4d sequential (regression: SF-7 __pgt_count) ──────────────────

/// Regression — SF-7 __pgt_count: rename a department (step 4c) and delete an
/// employee (step 4d) in SEPARATE refresh cycles, each processed via
/// DIFFERENTIAL.
///
/// Before the SF-7 fix, the Project operator stripped __pgt_count from the
/// aggregate delta output.  The MERGE INSERT clause didn't include __pgt_count,
/// so newly inserted group rows (from the rename's D+I path change) got the
/// column default (0) instead of the correct count.  On the subsequent delete
/// step, the aggregate merge computed `new_count = 0 - 1 = -1 ≤ 0`, emitting
/// a DELETE action that removed the Backend row entirely.
///
/// The fix forwards __pgt_count through the Project CTE so the MERGE writes
/// the correct value on INSERT/UPDATE.
#[tokio::test]
async fn test_getting_started_step4c_then_4d_sequential_regression() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;
    create_department_stats(&db).await;
    create_department_report(&db).await;

    // ── Cycle 1: rename Engineering → R&D ────────────────────────────
    db.execute("UPDATE departments SET name = 'R&D' WHERE id = 2")
        .await;
    db.refresh_st_until_converged("department_tree").await;
    db.refresh_st_until_converged("department_stats").await;
    db.refresh_st_until_converged("department_report").await;

    // All 7 department rows must exist with correct __pgt_count.
    // (setup_base_tables creates 7 departments — DevOps is added in step 2.4b
    // which this test intentionally skips.)
    let row_count: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM department_stats")
        .await;
    assert_eq!(row_count, 7, "all 7 departments present after rename");

    // Backend must have __pgt_count = 2 (Alice + Bob still there).
    let backend_count: i64 = db
        .query_scalar(
            "SELECT __pgt_count::bigint FROM department_stats WHERE department_name = 'Backend'",
        )
        .await;
    assert_eq!(
        backend_count, 2,
        "Backend __pgt_count must be 2 after rename (SF-7)"
    );

    // ── Cycle 2: delete Bob ──────────────────────────────────────────
    db.execute("DELETE FROM employees WHERE name = 'Bob'").await;
    db.refresh_st_until_converged("department_stats").await;
    db.refresh_st_until_converged("department_report").await;

    // Backend must still exist with headcount=1 (only Alice remains).
    let (hc, total_sal, avg_sal): (i64, String, String) = sqlx::query_as(
        "SELECT headcount::bigint,
                round(total_salary, 2)::text,
                round(avg_salary, 2)::text
         FROM department_stats WHERE department_name = 'Backend'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("Backend stats row must exist after sequential rename → delete");
    assert_eq!(hc, 1, "headcount must be 1 after Bob's deletion");
    assert_eq!(
        total_sal, "120000.00",
        "total_salary must be Alice's salary"
    );
    assert_eq!(avg_sal, "120000.00");

    // Backend __pgt_count must be 1 (one LEFT JOIN output row).
    let backend_count: i64 = db
        .query_scalar(
            "SELECT __pgt_count::bigint FROM department_stats WHERE department_name = 'Backend'",
        )
        .await;
    assert_eq!(
        backend_count, 1,
        "Backend __pgt_count must be 1 after delete (SF-7)"
    );

    // All 7 rows still present (no departments vanished).
    let row_count: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM department_stats")
        .await;
    assert_eq!(row_count, 7, "all 7 departments still present after delete");

    // R&D division headcount must be 3 (Alice, Charlie, Diana).
    let rnd_hc: i64 = db
        .query_scalar(
            "SELECT total_headcount::bigint FROM department_report WHERE division = 'R&D'",
        )
        .await;
    assert_eq!(
        rnd_hc, 3,
        "R&D headcount must be 3 after sequential rename + delete"
    );
}

// ── Step 4d ──────────────────────────────────────────────────────────────────

/// Step 4d: DELETE Bob from Backend. Headcount drops 2→1, salary 235000→120000.
/// Only Backend group is affected; other groups and Sales/Operations division untouched.
#[tokio::test]
async fn test_getting_started_step4d_employee_delete_updates_aggregates() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;
    create_department_stats(&db).await;
    create_department_report(&db).await;

    db.execute("DELETE FROM employees WHERE name = 'Bob'").await;

    db.refresh_st_until_converged("department_stats").await;
    db.refresh_st_until_converged("department_report").await;

    // Backend: 2 → 1 employee; 235000 → 120000
    let (hc, total_sal, avg_sal): (i64, String, String) = sqlx::query_as(
        "SELECT headcount::bigint,
                round(total_salary, 2)::text,
                round(avg_salary, 2)::text
         FROM department_stats WHERE department_name = 'Backend'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("Backend stats query failed");
    assert_eq!(hc, 1);
    assert_eq!(total_sal, "120000.00");
    assert_eq!(avg_sal, "120000.00");

    // Engineering division: 4 → 3
    let eng_hc: i64 = db
        .query_scalar(
            "SELECT total_headcount::bigint FROM department_report WHERE division = 'Engineering'",
        )
        .await;
    assert_eq!(eng_hc, 3);

    // Sales untouched
    let sales_hc: i64 = db
        .query_scalar(
            "SELECT total_headcount::bigint FROM department_report WHERE division = 'Sales'",
        )
        .await;
    assert_eq!(sales_hc, 2, "Sales unaffected by Bob's deletion");
}

// ── Step 7 ───────────────────────────────────────────────────────────────────

/// Step 7: Drop stream tables in reverse dependency order. Storage tables,
/// CDC triggers, and catalog entries are all cleaned up.
#[tokio::test]
async fn test_getting_started_step7_drop_in_order() {
    let db = E2eDb::new().await.with_extension().await;
    setup_base_tables(&db).await;
    create_department_tree(&db).await;
    create_department_stats(&db).await;
    create_department_report(&db).await;

    // Capture OIDs for trigger name checks before dropping
    let dept_oid = db.table_oid("departments").await;
    let empl_oid = db.table_oid("employees").await;

    // Drop in reverse dependency order (dependents first)
    db.drop_st("department_report").await;
    db.drop_st("department_stats").await;
    db.drop_st("department_tree").await;

    // Catalog entries removed
    let cat_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name IN ('department_tree', 'department_stats', 'department_report')",
        )
        .await;
    assert_eq!(cat_count, 0, "All stream tables removed from catalog");

    // Storage tables dropped
    assert!(
        !db.table_exists("public", "department_tree").await,
        "department_tree storage table should be gone"
    );
    assert!(
        !db.table_exists("public", "department_stats").await,
        "department_stats storage table should be gone"
    );
    assert!(
        !db.table_exists("public", "department_report").await,
        "department_report storage table should be gone"
    );

    // CDC triggers removed from base tables
    // v0.32.0+: trigger names use stable hash suffix. Use pgtrickle.source_stable_name()
    // which resolves via pg_class (survives pgt_change_tracking cleanup after all STs dropped).
    let dept_stable: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            dept_oid
        ))
        .await;
    let empl_stable: String = db
        .query_scalar(&format!(
            "SELECT pgtrickle.source_stable_name({}::oid)",
            empl_oid
        ))
        .await;
    let dept_trigger = format!("pg_trickle_cdc_{}", dept_stable);
    let empl_trigger = format!("pg_trickle_cdc_{}", empl_stable);
    assert!(
        !db.trigger_exists(&dept_trigger, "departments").await,
        "CDC trigger on departments should be removed"
    );
    assert!(
        !db.trigger_exists(&empl_trigger, "employees").await,
        "CDC trigger on employees should be removed"
    );

    // Base tables themselves still exist
    assert!(db.table_exists("public", "departments").await);
    assert!(db.table_exists("public", "employees").await);
}

// ── Regression: function-call GROUP BY against a base-table source ───────────

/// Regression test for the bug where GROUP BY with a function-call expression
/// (e.g., `GROUP BY 1` when position 1 is `split_part(...)`) caused the
/// delta CTE to omit an explicit alias, producing:
///
///   column d.split_part(full_path, ' > ', 2) does not exist
///
/// This was only triggered by the **background scheduler** for the
/// `department_report` query (which sources `department_stats`, a stream
/// table), because the manual refresh path for ST-on-ST dependencies always
/// falls back to FULL, bypassing the differential DVM engine.
///
/// This test exercises the differential path directly against a **base table**
/// source so `refresh_st()` uses the DVM engine and the broken delta SQL
/// (if the bug re-regresses) causes an immediate test failure.
#[tokio::test]
async fn test_getting_started_func_call_group_by_differential_against_base_table() {
    let db = E2eDb::new().await.with_extension().await;

    // Minimal table: name contains a "category-subcategory" prefix we'll
    // extract with split_part().
    db.execute(
        "CREATE TABLE products (
            id       SERIAL PRIMARY KEY,
            sku      TEXT NOT NULL,
            price    NUMERIC(10,2) NOT NULL
        )",
    )
    .await;

    db.execute(
        "INSERT INTO products (sku, price) VALUES
            ('Electronics-TV',  499.99),
            ('Electronics-PC',  899.99),
            ('Books-Novel',      14.99),
            ('Books-Textbook',   49.99)",
    )
    .await;

    // GROUP BY 1 where position-1 is split_part(sku, '-', 1) — the exact
    // expression shape that triggered the bug.
    db.create_st(
        "product_summary",
        "SELECT split_part(sku, '-', 1) AS category,
                COUNT(*) AS product_count,
                SUM(price) AS total_price
         FROM products
         GROUP BY 1",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial population via full refresh at creation time.
    let initial_rows: i64 = db.count("public.product_summary").await;
    assert_eq!(initial_rows, 2, "Electronics and Books categories");

    let electronics_count: i64 = db
        .query_scalar(
            "SELECT product_count::bigint FROM product_summary WHERE category = 'Electronics'",
        )
        .await;
    assert_eq!(electronics_count, 2);

    // Insert a new product — triggers a CDC change against the base table,
    // so the next differential refresh will build the delta CTE with
    // split_part(...) in the GROUP BY.
    db.execute("INSERT INTO products (sku, price) VALUES ('Electronics-Tablet', 349.99)")
        .await;

    // refresh_st() takes the DIFFERENTIAL path here (source is a plain TABLE,
    // not a stream table): this is where the bug would have manifested as
    // "column d.split_part(sku, '-', 1) does not exist".
    db.refresh_st_until_converged("product_summary").await;

    let post_count: i64 = db
        .query_scalar(
            "SELECT product_count::bigint FROM product_summary WHERE category = 'Electronics'",
        )
        .await;
    assert_eq!(post_count, 3, "Electronics count must be 3 after INSERT");

    let post_price: String = db
        .query_scalar(
            "SELECT total_price::text FROM product_summary WHERE category = 'Electronics'",
        )
        .await;
    assert_eq!(post_price, "1749.97");

    // Books unchanged
    let books_count: i64 = db
        .query_scalar("SELECT product_count::bigint FROM product_summary WHERE category = 'Books'")
        .await;
    assert_eq!(books_count, 2, "Books group must be unaffected");

    // Status must be ACTIVE and DIFFERENTIAL after a successful differential refresh
    let (status, mode, _, _) = db.pgt_status("product_summary").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "DIFFERENTIAL");
}
