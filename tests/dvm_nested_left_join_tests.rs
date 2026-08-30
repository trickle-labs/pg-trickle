//! Execution-backed tests for nested left-join DVM SQL.
//!
//! Tests the three-table LEFT JOIN chain:
//!   (employees e LEFT JOIN departments d ON e.dept_id = d.dept_id)
//!       LEFT JOIN managers m ON d.mgr_id = m.id
//!
//! Delta output columns:
//!   join__e__id, join__e__dept_id, join__e__name  (always non-NULL — e is the anchor),
//!   join__d__dept_id, join__d__mgr_id, join__d__label  (nullable — inner LEFT JOIN),
//!   m__id, m__mgr_name  (nullable — outer LEFT JOIN).

mod common;

use common::TestDb;
use pg_trickle::dvm::DiffContext;
use pg_trickle::dvm::parser::{Column, Expr, OpTree};
use pg_trickle::version::Frontier;

// ── column helpers ─────────────────────────────────────────────────────────────

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

fn eq_cond(left_alias: &str, left_col: &str, right_alias: &str, right_col: &str) -> Expr {
    Expr::BinaryOp {
        op: "=".to_string(),
        left: Box::new(Expr::ColumnRef {
            table_alias: Some(left_alias.to_string()),
            column_name: left_col.to_string(),
        }),
        right: Box::new(Expr::ColumnRef {
            table_alias: Some(right_alias.to_string()),
            column_name: right_col.to_string(),
        }),
    }
}

// ── context / tree builders ────────────────────────────────────────────────────

fn make_ctx_3() -> DiffContext {
    let mut prev_frontier = Frontier::new();
    prev_frontier.set_source(1, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());
    prev_frontier.set_source(2, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());
    prev_frontier.set_source(3, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());

    let mut frontier = Frontier::new();
    frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());
    frontier.set_source(2, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());
    frontier.set_source(3, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    DiffContext::new_standalone(prev_frontier, frontier)
}

/// (employees e LEFT JOIN departments d ON e.dept_id = d.dept_id)
///     LEFT JOIN managers m ON d.mgr_id = m.id
fn build_nested_left_join_tree() -> OpTree {
    let employees = scan_with_pk(
        1,
        "employees",
        "e",
        vec![int_col("id"), int_col("dept_id"), text_col("name")],
        &["id"],
    );
    let departments = scan_with_pk(
        2,
        "departments",
        "d",
        vec![int_col("dept_id"), int_col("mgr_id"), text_col("label")],
        &["dept_id"],
    );
    let inner_join = OpTree::LeftJoin {
        condition: eq_cond("e", "dept_id", "d", "dept_id"),
        left: Box::new(employees),
        right: Box::new(departments),
    };
    let managers = scan_with_pk(
        3,
        "managers",
        "m",
        vec![int_col("id"), text_col("mgr_name")],
        &["id"],
    );
    OpTree::LeftJoin {
        condition: eq_cond("d", "mgr_id", "m", "id"),
        left: Box::new(inner_join),
        right: Box::new(managers),
    }
}

// ── DB setup ──────────────────────────────────────────────────────────────────

async fn setup_nested_lj_db() -> TestDb {
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
    mgr_id  INT NOT NULL,
    label   TEXT NOT NULL
);

CREATE TABLE public.managers (
    id       INT PRIMARY KEY,
    mgr_name TEXT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id   BIGSERIAL PRIMARY KEY,
    lsn         PG_LSN NOT NULL,
    action      CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id      INT,
    dept_id INT,
    name    TEXT
);

CREATE TABLE pgtrickle_changes.changes_2 (
    change_id   BIGSERIAL PRIMARY KEY,
    lsn         PG_LSN NOT NULL,
    action      CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    dept_id INT,
    mgr_id  INT,
    label   TEXT
);

CREATE TABLE pgtrickle_changes.changes_3 (
    change_id    BIGSERIAL PRIMARY KEY,
    lsn          PG_LSN NOT NULL,
    action       CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id       INT,
    mgr_name TEXT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up nested left-join execution database");

    db
}

async fn reset_nested_lj_fixture(db: &TestDb) {
    db.execute(
        "TRUNCATE TABLE \
         pgtrickle_changes.changes_1, \
         pgtrickle_changes.changes_2, \
         pgtrickle_changes.changes_3, \
         public.employees, \
         public.departments, \
         public.managers \
         RESTART IDENTITY",
    )
    .await;
}

/// Query delta rows for the nested left join output.
///
/// e columns (join__e__*) are non-nullable (employees always present as anchor).
/// d columns (join__d__*) are nullable (inner LEFT JOIN may not match).
/// m columns (m__*) are nullable (outer LEFT JOIN may not match).
type NestedLjRow = (
    String,         // __pgt_action
    i32,            // join__e__id
    i32,            // join__e__dept_id
    String,         // join__e__name
    Option<i32>,    // join__d__dept_id
    Option<i32>,    // join__d__mgr_id
    Option<String>, // join__d__label
    Option<i32>,    // m__id
    Option<String>, // m__mgr_name
);

async fn query_nested_lj_rows(db: &TestDb, sql: &str) -> Vec<NestedLjRow> {
    sqlx::query_as::<_, NestedLjRow>(sqlx::AssertSqlSafe(format!(
        r#"SELECT __pgt_action,
                  "join__e__id",
                  "join__e__dept_id",
                  "join__e__name",
                  "join__d__dept_id",
                  "join__d__mgr_id",
                  "join__d__label",
                  "m__id",
                  "m__mgr_name"
           FROM ({sql}) delta
           ORDER BY __pgt_action, "join__e__id""#
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated nested left-join delta SQL")
}

// ── tests ──────────────────────────────────────────────────────────────────────

/// Employee inserted, dept and manager both exist: the delta cascades through
/// both LEFT JOINs and emits a fully-matched I row.
///
/// Inner Part 1a → outer Part 1a.
#[tokio::test]
async fn test_diff_nested_left_join_innermost_insert_fully_matched() {
    let db = setup_nested_lj_db().await;
    let sql = make_ctx_3()
        .differentiate(&build_nested_left_join_tree())
        .expect("nested left-join differentiation should succeed");

    reset_nested_lj_fixture(&db).await;
    db.execute("INSERT INTO public.departments VALUES (10, 99, 'Engineering')")
        .await;
    db.execute("INSERT INTO public.managers VALUES (99, 'Alice')")
        .await;

    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, dept_id, name) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(1), 1, 10, 'Bob')",
    )
    .await;

    assert_eq!(
        query_nested_lj_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            1,
            10,
            "Bob".to_string(),
            Some(10),
            Some(99),
            Some("Engineering".to_string()),
            Some(99),
            Some("Alice".to_string()),
        )]
    );
}

/// Employee inserted with no matching department: the inner LEFT JOIN
/// NULL-pads d columns, the outer LEFT JOIN gets d.mgr_id = NULL so m is
/// also NULL.  Both right sides are fully NULL-padded.
///
/// Inner Part 3a → outer Part 3a (cascaded NULL padding).
#[tokio::test]
async fn test_diff_nested_left_join_insert_with_no_dept() {
    let db = setup_nested_lj_db().await;
    let sql = make_ctx_3()
        .differentiate(&build_nested_left_join_tree())
        .expect("nested left-join differentiation should succeed");

    reset_nested_lj_fixture(&db).await;
    // dept_id=20 has no matching department row; insert only the employee.
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, dept_id, name) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(1), 1, 20, 'Bob')",
    )
    .await;

    assert_eq!(
        query_nested_lj_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            1,
            20,
            "Bob".to_string(),
            None,
            None,
            None,
            None,
            None,
        )]
    );
}

/// Manager deleted while employee and department still exist: the outer LEFT
/// JOIN loses its last right match, emitting D(matched) + I(NULL-padded m).
///
/// Outer Part 2 (L₀ ⋈ ΔR_deletes) → D(matched)
/// Outer Part 5 (count = 0 in R₁)  → I(NULL-padded m)
#[tokio::test]
async fn test_diff_nested_left_join_outermost_delete() {
    let db = setup_nested_lj_db().await;
    let sql = make_ctx_3()
        .differentiate(&build_nested_left_join_tree())
        .expect("nested left-join differentiation should succeed");

    reset_nested_lj_fixture(&db).await;
    // Employee and department still present; manager 99 has been deleted.
    db.execute("INSERT INTO public.employees   VALUES (1, 10, 'Bob')")
        .await;
    db.execute("INSERT INTO public.departments VALUES (10, 99, 'Engineering')")
        .await;

    db.execute(
        "INSERT INTO pgtrickle_changes.changes_3 \
         (lsn, action, __pgt_row_id, id, mgr_name) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(99), 99, 'Alice')",
    )
    .await;

    let rows = query_nested_lj_rows(&db, &sql).await;

    // Ordered by action then join__e__id: 'D' < 'I'
    assert_eq!(
        rows.len(),
        2,
        "expected D(matched) + I(null-padded); rows={rows:?}"
    );

    assert_eq!(
        rows[0],
        (
            "D".to_string(),
            1,
            10,
            "Bob".to_string(),
            Some(10),
            Some(99),
            Some("Engineering".to_string()),
            Some(99),
            Some("Alice".to_string()),
        ),
        "D row should carry the previously-matched manager data; rows={rows:?}"
    );
    assert_eq!(
        rows[1],
        (
            "I".to_string(),
            1,
            10,
            "Bob".to_string(),
            Some(10),
            Some(99),
            Some("Engineering".to_string()),
            None,
            None,
        ),
        "I row should be NULL-padded for manager columns; rows={rows:?}"
    );
}
