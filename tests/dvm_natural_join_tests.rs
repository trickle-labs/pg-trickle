//! Execution-backed tests for "natural-join style" DVM SQL.
//!
//! PostgreSQL `NATURAL JOIN` is parsed as a regular join whose condition
//! equates all shared column names.  The DVM engine receives an
//! `InnerJoin` / `LeftJoin` / `FullJoin` node whose condition references
//! the **same column name** on both sides, e.g. `e.dept_id = d.dept_id`.
//!
//! The critical path this exercises is `rewrite_join_condition`: when
//! both scan children expose a column with the same identifier (here
//! `dept_id`), the rewriter must still correctly map `e.dept_id` →
//! `dl."dept_id"` and `d.dept_id` → `r."dept_id"` rather than
//! confusing the two.
//!
//! Schema: employees (id, dept_id, name) join departments (dept_id, label)
//! on employees.dept_id = departments.dept_id.
//!
//! Delta output columns: e__id, e__dept_id, e__name, d__dept_id, d__label.
//! For LEFT JOIN, d__dept_id and d__label may be NULL.
//! For FULL JOIN, e__id, e__dept_id, e__name may also be NULL.

mod common;

use common::TestDb;
use pg_trickle::dvm::DiffContext;
use pg_trickle::dvm::parser::{Column, Expr, OpTree};
use pg_trickle::version::Frontier;

// ── column helpers ────────────────────────────────────────────────────────────

fn int_col(name: &str) -> Column {
    Column {
        name: name.to_string(),
        type_oid: 23,
        is_nullable: true,
    }
}

fn text_col(name: &str) -> Column {
    Column {
        name: name.to_string(),
        type_oid: 25,
        is_nullable: true,
    }
}

fn scan_with_pk(
    oid: u32,
    table_name: &str,
    alias: &str,
    columns: Vec<Column>,
    pk_columns: &[&str],
) -> OpTree {
    OpTree::Scan {
        table_oid: oid,
        table_name: table_name.to_string(),
        schema: "public".to_string(),
        columns,
        pk_columns: pk_columns.iter().map(|c| (*c).to_string()).collect(),
        alias: alias.to_string(),
    }
}

/// Natural-join-style condition: same column name on both sides.
/// Models what the parser produces for `NATURAL JOIN` when the two
/// tables share exactly one column: `e.dept_id = d.dept_id`.
fn natural_cond(left_alias: &str, right_alias: &str, shared_col: &str) -> Expr {
    Expr::BinaryOp {
        op: "=".to_string(),
        left: Box::new(Expr::ColumnRef {
            table_alias: Some(left_alias.to_string()),
            column_name: shared_col.to_string(),
        }),
        right: Box::new(Expr::ColumnRef {
            table_alias: Some(right_alias.to_string()),
            column_name: shared_col.to_string(),
        }),
    }
}

// ── context / tree builders ───────────────────────────────────────────────────

fn make_ctx() -> DiffContext {
    let mut prev_frontier = Frontier::new();
    prev_frontier.set_source(1, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());
    prev_frontier.set_source(2, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());

    let mut frontier = Frontier::new();
    frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());
    frontier.set_source(2, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    DiffContext::new_standalone(prev_frontier, frontier)
}

fn employees_scan() -> OpTree {
    scan_with_pk(
        1,
        "employees",
        "e",
        vec![int_col("id"), int_col("dept_id"), text_col("name")],
        &["id"],
    )
}

fn departments_scan() -> OpTree {
    scan_with_pk(
        2,
        "departments",
        "d",
        vec![int_col("dept_id"), text_col("label")],
        &["dept_id"],
    )
}

fn natural_inner_join_tree() -> OpTree {
    OpTree::InnerJoin {
        condition: natural_cond("e", "d", "dept_id"),
        left: Box::new(employees_scan()),
        right: Box::new(departments_scan()),
    }
}

fn natural_left_join_tree() -> OpTree {
    OpTree::LeftJoin {
        condition: natural_cond("e", "d", "dept_id"),
        left: Box::new(employees_scan()),
        right: Box::new(departments_scan()),
    }
}

fn natural_full_join_tree() -> OpTree {
    OpTree::FullJoin {
        condition: natural_cond("e", "d", "dept_id"),
        left: Box::new(employees_scan()),
        right: Box::new(departments_scan()),
    }
}

// ── DB setup ─────────────────────────────────────────────────────────────────

async fn setup_nj_db() -> TestDb {
    let db = TestDb::new().await;

    sqlx::raw_sql(
        r#"
CREATE SCHEMA IF NOT EXISTS pgtrickle;
CREATE SCHEMA IF NOT EXISTS pgtrickle_changes;

CREATE OR REPLACE FUNCTION pgtrickle.pg_trickle_hash(val TEXT)
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT hashtextextended(COALESCE(val, ''), 0)::BIGINT
$$;

CREATE OR REPLACE FUNCTION pgtrickle.pg_trickle_hash_multi(vals TEXT[])
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT hashtextextended(COALESCE(array_to_string(vals, '|', '<NULL>'), ''), 0)::BIGINT
$$;

CREATE TABLE public.employees (
    id      INT PRIMARY KEY,
    dept_id INT NOT NULL,
    name    TEXT NOT NULL
);

CREATE TABLE public.departments (
    dept_id INT PRIMARY KEY,
    label   TEXT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id    BIGSERIAL PRIMARY KEY,
    lsn          PG_LSN NOT NULL,
    action       CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id       INT,
    dept_id  INT,
    name     TEXT
);

CREATE TABLE pgtrickle_changes.changes_2 (
    change_id    BIGSERIAL PRIMARY KEY,
    lsn          PG_LSN NOT NULL,
    action       CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    dept_id  INT,
    label    TEXT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up natural-join execution database");

    db
}

async fn reset_nj_fixture(db: &TestDb) {
    db.execute(
        "TRUNCATE TABLE \
         pgtrickle_changes.changes_1, \
         pgtrickle_changes.changes_2, \
         public.employees, \
         public.departments \
         RESTART IDENTITY",
    )
    .await;
}

/// Query inner-join delta rows: (action, e.id, e.dept_id, e.name, d.dept_id, d.label)
/// Both right columns are non-nullable (inner join).
async fn query_nj_inner_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(String, i32, i32, String, i32, String)> {
    sqlx::query_as::<_, (String, i32, i32, String, i32, String)>(sqlx::AssertSqlSafe(format!(
        r#"SELECT __pgt_action,
                  "e__id",
                  "e__dept_id",
                  "e__name",
                  "d__dept_id",
                  "d__label"
           FROM ({sql}) delta
           ORDER BY __pgt_action, "e__id""#
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated natural inner-join delta SQL")
}

/// Query left-join delta rows: d columns are nullable.
async fn query_nj_left_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(String, i32, i32, String, Option<i32>, Option<String>)> {
    sqlx::query_as::<_, (String, i32, i32, String, Option<i32>, Option<String>)>(
        sqlx::AssertSqlSafe(format!(
            r#"SELECT __pgt_action,
                  "e__id",
                  "e__dept_id",
                  "e__name",
                  "d__dept_id",
                  "d__label"
           FROM ({sql}) delta
           ORDER BY __pgt_action, "e__id""#
        )),
    )
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated natural left-join delta SQL")
}

/// Query full-join delta rows: both sides can be NULL.
async fn query_nj_full_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(
    String,
    Option<i32>,
    Option<i32>,
    Option<String>,
    Option<i32>,
    Option<String>,
)> {
    sqlx::query_as::<
        _,
        (
            String,
            Option<i32>,
            Option<i32>,
            Option<String>,
            Option<i32>,
            Option<String>,
        ),
    >(sqlx::AssertSqlSafe(format!(
        r#"SELECT __pgt_action,
                  "e__id",
                  "e__dept_id",
                  "e__name",
                  "d__dept_id",
                  "d__label"
           FROM ({sql}) delta
           ORDER BY __pgt_action, "e__id" NULLS LAST, "d__dept_id" NULLS LAST"#
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated natural full-join delta SQL")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// INNER JOIN with natural-join-style condition: insert an employee whose
/// department already exists.  The shared column `dept_id` appears on both
/// sides of the condition (`e.dept_id = d.dept_id`).
///
/// Validates that `rewrite_join_condition` correctly rewrites the same-named
/// column on each side rather than confusing `dl.dept_id` with `r.dept_id`.
#[tokio::test]
async fn test_diff_natural_inner_join_employee_insert_matches_department() {
    let db = setup_nj_db().await;
    let sql = make_ctx()
        .differentiate(&natural_inner_join_tree())
        .expect("natural inner-join differentiation should succeed");

    reset_nj_fixture(&db).await;
    // Current state: department 10 exists, employees 1 and 3 are present.
    db.execute("INSERT INTO public.employees   VALUES (1, 10, 'Bob'), (3, 10, 'Alice')")
        .await;
    db.execute("INSERT INTO public.departments VALUES (10, 'Engineering')")
        .await;

    // INSERT employee (3, dept_id=10, "Alice").
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, dept_id, name) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(3), 3, 10, 'Alice')",
    )
    .await;

    // Part 1a: I(3, 10, "Alice") ⋈ departments{(10, "Engineering")}
    //   → I(e__id=3, e__dept_id=10, e__name="Alice", d__dept_id=10, d__label="Engineering")
    assert_eq!(
        query_nj_inner_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            3,
            10,
            "Alice".to_string(),
            10,
            "Engineering".to_string()
        )]
    );
}

/// INNER JOIN: delete an employee.  Part 1b (ΔL_deletes ⋈ R₀) reconstructs
/// the pre-change right state and emits a D row.
///
/// This also validates the shared-column condition rewriting for the R₀ path.
#[tokio::test]
async fn test_diff_natural_inner_join_employee_delete_emits_d_row() {
    let db = setup_nj_db().await;
    let sql = make_ctx()
        .differentiate(&natural_inner_join_tree())
        .expect("natural inner-join differentiation should succeed");

    reset_nj_fixture(&db).await;
    // Current state: employee 3 already deleted; department 10 still exists.
    db.execute("INSERT INTO public.employees   VALUES (1, 10, 'Bob')")
        .await;
    db.execute("INSERT INTO public.departments VALUES (10, 'Engineering')")
        .await;

    // DELETE employee (3, dept_id=10, "Alice").
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, dept_id, name) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(3), 3, 10, 'Alice')",
    )
    .await;

    // Part 1b: D(3, 10, "Alice") ⋈ R₀{(10,"Engineering")}
    //   → D(e__id=3, e__dept_id=10, e__name="Alice", d__dept_id=10, d__label="Engineering")
    assert_eq!(
        query_nj_inner_rows(&db, &sql).await,
        vec![(
            "D".to_string(),
            3,
            10,
            "Alice".to_string(),
            10,
            "Engineering".to_string()
        )]
    );
}

/// INNER JOIN: right-side delete fans out to multiple left rows.
/// Department 10 is deleted; two employees (1 and 2) referenced it.
/// Part 2 (L₀ ⋈ ΔR) emits D for each matching employee.
#[tokio::test]
async fn test_diff_natural_inner_join_department_delete_fans_out() {
    let db = setup_nj_db().await;
    let sql = make_ctx()
        .differentiate(&natural_inner_join_tree())
        .expect("natural inner-join differentiation should succeed");

    reset_nj_fixture(&db).await;
    // Current state: department 10 deleted; employees still exist.
    db.execute("INSERT INTO public.employees VALUES (1, 10, 'Bob'), (2, 10, 'Carol')")
        .await;
    // departments is empty

    // DELETE department (dept_id=10, "Engineering").
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, __pgt_row_id, dept_id, label) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(10), 10, 'Engineering')",
    )
    .await;

    // Part 2: L₀(employees) ⋈ D(dept_id=10)
    //   → D(1, 10, "Bob", 10, "Engineering") and D(2, 10, "Carol", 10, "Engineering")
    assert_eq!(
        query_nj_inner_rows(&db, &sql).await,
        vec![
            (
                "D".to_string(),
                1,
                10,
                "Bob".to_string(),
                10,
                "Engineering".to_string()
            ),
            (
                "D".to_string(),
                2,
                10,
                "Carol".to_string(),
                10,
                "Engineering".to_string()
            ),
        ]
    );
}

/// LEFT JOIN with natural-join-style condition: insert an employee whose
/// department does NOT exist.  Part 3a (anti-join INSERTs) fires and emits
/// a row with NULL right columns.
///
/// This validates the natural-join condition rewriting for the anti-join
/// NOT EXISTS predicate path.
#[tokio::test]
async fn test_diff_natural_left_join_employee_insert_with_no_department() {
    let db = setup_nj_db().await;
    let sql = make_ctx()
        .differentiate(&natural_left_join_tree())
        .expect("natural left-join differentiation should succeed");

    reset_nj_fixture(&db).await;
    // Employee 5 references dept_id=99 which does not exist.
    db.execute("INSERT INTO public.employees VALUES (5, 99, 'Dave')")
        .await;
    // departments is empty

    // INSERT employee (5, dept_id=99, "Dave") — no matching department.
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, dept_id, name) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(5), 5, 99, 'Dave')",
    )
    .await;

    // Part 3a: I(5, 99, "Dave") anti-join departments → I with NULL right columns.
    assert_eq!(
        query_nj_left_rows(&db, &sql).await,
        vec![("I".to_string(), 5, 99, "Dave".to_string(), None, None)]
    );
}

/// FULL JOIN with natural-join-style condition: insert a department that
/// has no employees.  Part 6 (right anti-join left) fires and emits a row
/// with NULL left columns.
///
/// This validates the natural-join condition rewriting for the FULL JOIN
/// right-side anti-join path (unique to FULL OUTER JOIN).
#[tokio::test]
async fn test_diff_natural_full_join_department_insert_with_no_employees() {
    let db = setup_nj_db().await;
    let sql = make_ctx()
        .differentiate(&natural_full_join_tree())
        .expect("natural full-join differentiation should succeed");

    reset_nj_fixture(&db).await;
    // Department 50 is inserted but no employees reference it.
    db.execute("INSERT INTO public.employees   VALUES (1, 10, 'Bob')")
        .await;
    db.execute("INSERT INTO public.departments VALUES (50, 'Marketing')")
        .await;

    // INSERT department (dept_id=50, "Marketing") — no employees reference it.
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, __pgt_row_id, dept_id, label) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(50), 50, 'Marketing')",
    )
    .await;

    // Part 6 (full-join only): I with NULL left columns.
    assert_eq!(
        query_nj_full_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            None,
            None,
            None,
            Some(50),
            Some("Marketing".to_string())
        )]
    );
}
