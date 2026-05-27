//! Execution-backed tests for semi-join and anti-join DVM SQL.
//!
//! These tests exercise the generated delta SQL directly against a live
//! PostgreSQL container. They target the highest-risk transition cases from
//! PLAN_TEST_EVALS_UNIT.md without requiring the full extension runtime.

mod common;

use common::TestDb;
use pg_trickle::dvm::DiffContext;
use pg_trickle::dvm::parser::{Column, Expr, OpTree};
use pg_trickle::version::Frontier;

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

fn make_ctx() -> DiffContext {
    let mut prev_frontier = Frontier::new();
    prev_frontier.set_source(1, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());
    prev_frontier.set_source(2, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());

    let mut new_frontier = Frontier::new();
    new_frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());
    new_frontier.set_source(2, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    DiffContext::new_standalone(prev_frontier, new_frontier)
}

fn build_semi_join_tree() -> OpTree {
    let left = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), int_col("cust_id"), int_col("amount")],
        &["id"],
    );
    let right = scan_with_pk(
        2,
        "customers",
        "c",
        vec![int_col("id"), text_col("name")],
        &["id"],
    );

    OpTree::SemiJoin {
        condition: eq_cond("o", "cust_id", "c", "id"),
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn build_anti_join_tree() -> OpTree {
    let left = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), int_col("cust_id"), int_col("amount")],
        &["id"],
    );
    let right = scan_with_pk(
        2,
        "customers",
        "c",
        vec![int_col("id"), text_col("name")],
        &["id"],
    );

    OpTree::AntiJoin {
        condition: eq_cond("o", "cust_id", "c", "id"),
        left: Box::new(left),
        right: Box::new(right),
    }
}

async fn setup_operator_db() -> TestDb {
    let db = TestDb::new().await;

    sqlx::raw_sql(
        r#"
CREATE SCHEMA IF NOT EXISTS pgtrickle;
CREATE SCHEMA IF NOT EXISTS pgtrickle_changes;

CREATE OR REPLACE FUNCTION pgtrickle.pg_trickle_hash(val ANYELEMENT)
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT hashtextextended(COALESCE(val::text, '<NULL>'), 0)::BIGINT
$$;

CREATE OR REPLACE FUNCTION pgtrickle.pg_trickle_hash_multi(vals TEXT[])
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT hashtextextended(COALESCE(array_to_string(vals, '|', '<NULL>'), ''), 0)::BIGINT
$$;

CREATE TABLE public.orders (
    id INT PRIMARY KEY,
    cust_id INT NOT NULL,
    amount INT NOT NULL
);

CREATE TABLE public.customers (
    id INT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn PG_LSN NOT NULL,
    action CHAR(1) NOT NULL,
    pk_hash BIGINT,
    new_id INT,
    new_cust_id INT,
    new_amount INT,
    old_id INT,
    old_cust_id INT,
    old_amount INT
);

CREATE TABLE pgtrickle_changes.changes_2 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn PG_LSN NOT NULL,
    action CHAR(1) NOT NULL,
    pk_hash BIGINT,
    new_id INT,
    new_name TEXT,
    old_id INT,
    old_name TEXT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up operator execution database");

    db
}

async fn reset_semijoin_fixture(db: &TestDb) {
    db.execute(
        "TRUNCATE TABLE \
         pgtrickle_changes.changes_1, \
         pgtrickle_changes.changes_2, \
         public.orders, \
         public.customers \
         RESTART IDENTITY",
    )
    .await;

    db.execute("INSERT INTO public.orders VALUES (1, 10, 100), (2, 20, 200)")
        .await;
}

async fn query_rows(db: &TestDb, sql: &str) -> Vec<(String, i32, i32, i32)> {
    sqlx::query_as::<_, (String, i32, i32, i32)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, id, cust_id, amount FROM ({sql}) delta ORDER BY id"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated delta SQL")
}

#[tokio::test]
async fn test_diff_semi_join_executes_match_gain_and_loss() {
    let db = setup_operator_db().await;
    let sql = make_ctx()
        .differentiate(&build_semi_join_tree())
        .expect("semi-join differentiation should succeed");

    reset_semijoin_fixture(&db).await;
    db.execute("INSERT INTO public.customers VALUES (10, 'Alice'), (20, 'Bob')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 (lsn, action, pk_hash, new_id, new_name) \
         VALUES ('0/1', 'I', 10, 10, 'Alice')",
    )
    .await;

    assert_eq!(
        query_rows(&db, &sql).await,
        vec![("I".to_string(), 1, 10, 100)]
    );

    reset_semijoin_fixture(&db).await;
    db.execute("INSERT INTO public.customers VALUES (20, 'Bob')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 (lsn, action, pk_hash, old_id, old_name) \
         VALUES ('0/1', 'D', 10, 10, 'Alice')",
    )
    .await;

    assert_eq!(
        query_rows(&db, &sql).await,
        vec![("D".to_string(), 1, 10, 100)]
    );
}

#[tokio::test]
async fn test_diff_anti_join_executes_match_gain_and_loss() {
    let db = setup_operator_db().await;
    let sql = make_ctx()
        .differentiate(&build_anti_join_tree())
        .expect("anti-join differentiation should succeed");

    reset_semijoin_fixture(&db).await;
    db.execute("INSERT INTO public.customers VALUES (10, 'Alice'), (20, 'Bob')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 (lsn, action, pk_hash, new_id, new_name) \
         VALUES ('0/1', 'I', 10, 10, 'Alice')",
    )
    .await;

    assert_eq!(
        query_rows(&db, &sql).await,
        vec![("D".to_string(), 1, 10, 100)]
    );

    reset_semijoin_fixture(&db).await;
    db.execute("INSERT INTO public.customers VALUES (20, 'Bob')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 (lsn, action, pk_hash, old_id, old_name) \
         VALUES ('0/1', 'D', 10, 10, 'Alice')",
    )
    .await;

    assert_eq!(
        query_rows(&db, &sql).await,
        vec![("I".to_string(), 1, 10, 100)]
    );
}

#[tokio::test]
async fn test_diff_semi_join_executes_simultaneous_left_and_right_deltas() {
    let db = setup_operator_db().await;
    let sql = make_ctx()
        .differentiate(&build_semi_join_tree())
        .expect("semi-join differentiation should succeed");

    reset_semijoin_fixture(&db).await;
    db.execute("INSERT INTO public.orders VALUES (3, 10, 300)")
        .await;
    db.execute("INSERT INTO public.customers VALUES (10, 'Alice')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 (lsn, action, pk_hash, new_id, new_cust_id, new_amount) \
         VALUES ('0/1', 'I', 3, 3, 10, 300)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 (lsn, action, pk_hash, new_id, new_name, old_id, old_name) \
         VALUES \
         ('0/2', 'I', 10, 10, 'Alice', NULL, NULL), \
         ('0/3', 'D', 20, NULL, NULL, 20, 'Bob')",
    )
    .await;

    assert_eq!(
        query_rows(&db, &sql).await,
        vec![
            ("I".to_string(), 1, 10, 100),
            ("D".to_string(), 2, 20, 200),
            ("I".to_string(), 3, 10, 300),
        ]
    );
}

#[tokio::test]
async fn test_diff_anti_join_executes_simultaneous_left_and_right_deltas() {
    let db = setup_operator_db().await;
    let sql = make_ctx()
        .differentiate(&build_anti_join_tree())
        .expect("anti-join differentiation should succeed");

    reset_semijoin_fixture(&db).await;
    db.execute("INSERT INTO public.orders VALUES (3, 10, 300)")
        .await;
    db.execute("INSERT INTO public.customers VALUES (10, 'Alice')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 (lsn, action, pk_hash, new_id, new_cust_id, new_amount) \
         VALUES ('0/1', 'I', 3, 3, 10, 300)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 (lsn, action, pk_hash, new_id, new_name, old_id, old_name) \
         VALUES \
         ('0/2', 'I', 10, 10, 'Alice', NULL, NULL), \
         ('0/3', 'D', 20, NULL, NULL, 20, 'Bob')",
    )
    .await;

    assert_eq!(
        query_rows(&db, &sql).await,
        vec![("D".to_string(), 1, 10, 100), ("I".to_string(), 2, 20, 200),]
    );
}

#[tokio::test]
async fn test_diff_semi_join_ignores_unmatched_left_insert() {
    let db = setup_operator_db().await;
    let sql = make_ctx()
        .differentiate(&build_semi_join_tree())
        .expect("semi-join differentiation should succeed");

    reset_semijoin_fixture(&db).await;
    db.execute("INSERT INTO public.orders VALUES (3, 30, 300)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 (lsn, action, pk_hash, new_id, new_cust_id, new_amount) \
         VALUES ('0/1', 'I', 3, 3, 30, 300)",
    )
    .await;

    assert!(query_rows(&db, &sql).await.is_empty());
}

#[tokio::test]
async fn test_diff_anti_join_emits_unmatched_left_insert() {
    let db = setup_operator_db().await;
    let sql = make_ctx()
        .differentiate(&build_anti_join_tree())
        .expect("anti-join differentiation should succeed");

    reset_semijoin_fixture(&db).await;
    db.execute("INSERT INTO public.orders VALUES (3, 30, 300)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 (lsn, action, pk_hash, new_id, new_cust_id, new_amount) \
         VALUES ('0/1', 'I', 3, 3, 30, 300)",
    )
    .await;

    assert_eq!(
        query_rows(&db, &sql).await,
        vec![("I".to_string(), 3, 30, 300)]
    );
}

fn make_nested_ctx() -> DiffContext {
    let mut prev_frontier = Frontier::new();
    prev_frontier.set_source(1, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());
    prev_frontier.set_source(2, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());
    prev_frontier.set_source(3, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());

    let mut new_frontier = Frontier::new();
    new_frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());
    new_frontier.set_source(2, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());
    new_frontier.set_source(3, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    DiffContext::new_standalone(prev_frontier, new_frontier)
}

fn build_nested_semi_join_tree() -> OpTree {
    let left = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), int_col("cust_id"), int_col("amount")],
        &["id"],
    );
    let right_outer = scan_with_pk(
        2,
        "customers",
        "c",
        vec![int_col("id"), text_col("name")],
        &["id"],
    );
    let right_inner = scan_with_pk(
        3,
        "vips",
        "v",
        vec![int_col("customer_id")],
        &["customer_id"],
    );

    let right = OpTree::SemiJoin {
        condition: Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("c".to_string()),
                column_name: "id".to_string(),
            }),
            right: Box::new(Expr::ColumnRef {
                table_alias: Some("v".to_string()),
                column_name: "customer_id".to_string(),
            }),
        },
        left: Box::new(right_outer),
        right: Box::new(right_inner),
    };

    OpTree::SemiJoin {
        condition: Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("o".to_string()),
                column_name: "cust_id".to_string(),
            }),
            right: Box::new(Expr::ColumnRef {
                table_alias: Some("c".to_string()),
                column_name: "id".to_string(),
            }),
        },
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn build_nested_anti_join_tree() -> OpTree {
    let left = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), int_col("cust_id"), int_col("amount")],
        &["id"],
    );
    let right_outer = scan_with_pk(
        2,
        "customers",
        "c",
        vec![int_col("id"), text_col("name")],
        &["id"],
    );
    let right_inner = scan_with_pk(
        3,
        "vips",
        "v",
        vec![int_col("customer_id")],
        &["customer_id"],
    );

    let right = OpTree::SemiJoin {
        condition: Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("c".to_string()),
                column_name: "id".to_string(),
            }),
            right: Box::new(Expr::ColumnRef {
                table_alias: Some("v".to_string()),
                column_name: "customer_id".to_string(),
            }),
        },
        left: Box::new(right_outer),
        right: Box::new(right_inner),
    };

    OpTree::AntiJoin {
        condition: Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("o".to_string()),
                column_name: "cust_id".to_string(),
            }),
            right: Box::new(Expr::ColumnRef {
                table_alias: Some("c".to_string()),
                column_name: "id".to_string(),
            }),
        },
        left: Box::new(left),
        right: Box::new(right),
    }
}

#[tokio::test]
async fn test_diff_semi_join_executes_nested() {
    let db = setup_operator_db().await;
    let sql = make_nested_ctx()
        .differentiate(&build_nested_semi_join_tree())
        .expect("nested semi-join differentiation should succeed");

    reset_semijoin_fixture(&db).await;

    // Create new structures inside test schema
    db.execute(
        "CREATE TABLE IF NOT EXISTS public.vips (
            customer_id INT PRIMARY KEY
        )",
    )
    .await;
    db.execute(
        "CREATE TABLE IF NOT EXISTS pgtrickle_changes.changes_3 (
            change_id BIGSERIAL PRIMARY KEY,
            lsn PG_LSN NOT NULL,
            action CHAR(1) NOT NULL,
            pk_hash BIGINT,
            new_customer_id INT,
            old_customer_id INT
        )",
    )
    .await;

    db.execute("TRUNCATE TABLE pgtrickle_changes.changes_3, public.vips RESTART IDENTITY")
        .await;
    db.execute("TRUNCATE TABLE public.customers RESTART IDENTITY")
        .await;

    // insert required rows into customers. Note: `reset_semijoin_fixture` already inserted base orders [1, 2].
    db.execute("INSERT INTO public.customers VALUES (10, 'Alice'), (20, 'Bob')")
        .await;

    // set up `vips` with [10] only, making order 1 match and order 2 fail.
    db.execute("INSERT INTO public.vips VALUES (10)").await;

    // insert VIP 20 AND log change into changes_3 simultaneously
    db.execute("INSERT INTO public.vips VALUES (20)").await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_3 (lsn, action, pk_hash, new_customer_id)          VALUES ('0/1', 'I', pgtrickle.pg_trickle_hash(20::text), 20);"
    ).await;

    assert_eq!(
        query_rows(&db, &sql).await,
        vec![("I".to_string(), 2, 20, 200)]
    );
}

#[tokio::test]
async fn test_diff_anti_join_executes_nested() {
    let db = setup_operator_db().await;
    let sql = make_nested_ctx()
        .differentiate(&build_nested_anti_join_tree())
        .expect("nested anti-join differentiation should succeed");

    reset_semijoin_fixture(&db).await;

    db.execute(
        "CREATE TABLE IF NOT EXISTS public.vips (
            customer_id INT PRIMARY KEY
        )",
    )
    .await;
    db.execute(
        "CREATE TABLE IF NOT EXISTS pgtrickle_changes.changes_3 (
            change_id BIGSERIAL PRIMARY KEY,
            lsn PG_LSN NOT NULL,
            action CHAR(1) NOT NULL,
            pk_hash BIGINT,
            new_customer_id INT,
            old_customer_id INT
        )",
    )
    .await;

    db.execute("TRUNCATE TABLE pgtrickle_changes.changes_3, public.vips RESTART IDENTITY")
        .await;
    db.execute("TRUNCATE TABLE public.customers RESTART IDENTITY")
        .await;

    db.execute("INSERT INTO public.customers VALUES (10, 'Alice'), (20, 'Bob')")
        .await;

    // Base state: both match. We delete VIP 20!
    db.execute("INSERT INTO public.vips VALUES (10), (20)")
        .await;
    db.execute("DELETE FROM public.vips WHERE customer_id = 20")
        .await;

    db.execute(
        "INSERT INTO pgtrickle_changes.changes_3 (lsn, action, pk_hash, old_customer_id)          VALUES ('0/1', 'D', pgtrickle.pg_trickle_hash(20::text), 20);"
    ).await;

    assert_eq!(
        query_rows(&db, &sql).await,
        vec![("I".to_string(), 2, 20, 200)]
    );
}

#[tokio::test]
async fn test_diff_anti_join_null_absence() {
    // "Null/absence" transition: deleting a left-side row that is currently
    // unmatched (its join-key has no corresponding right-side row) should emit
    // 'D' from the anti-join delta (departure from the unmatched set).
    let db = setup_operator_db().await;
    let sql = make_ctx()
        .differentiate(&build_anti_join_tree())
        .expect("anti-join differentiation should succeed");

    reset_semijoin_fixture(&db).await;

    // Only customer 20 (Bob) exists.
    // Order 1 (cust_id=10) has NO matching customer  → IS in the anti-join.
    // Order 2 (cust_id=20) has a matching customer   → NOT in the anti-join.
    db.execute("INSERT INTO public.customers VALUES (20, 'Bob')")
        .await;

    // Delete order 1, which was unmatched (right side absent, no customer with id=10).
    db.execute("DELETE FROM public.orders WHERE id = 1").await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_cust_id, old_amount) \
         VALUES ('0/1', 'D', 1, 1, 10, 100)",
    )
    .await;

    // Order 1 was in the anti-join (unmatched) and is now deleted → emit 'D'.
    assert_eq!(
        query_rows(&db, &sql).await,
        vec![("D".to_string(), 1, 10, 100)]
    );
}
