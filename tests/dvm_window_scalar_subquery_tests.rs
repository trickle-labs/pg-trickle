//! Execution-backed tests for window and scalar-subquery DVM SQL.
//!
//! These tests execute generated delta SQL against a standalone PostgreSQL
//! container so we can validate result rows for the remaining thin operators
//! called out in PLAN_TEST_EVALS_UNIT.md.

mod common;

use common::TestDb;
use pg_trickle::dvm::DiffContext;
use pg_trickle::dvm::parser::{Column, Expr, OpTree, SortExpr, WindowExpr};
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

fn colref(name: &str) -> Expr {
    Expr::ColumnRef {
        table_alias: None,
        column_name: name.to_string(),
    }
}

fn sort_asc(name: &str) -> SortExpr {
    SortExpr {
        expr: colref(name),
        ascending: true,
        nulls_first: false,
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

fn make_window_ctx(st_name: &str) -> DiffContext {
    let mut prev_frontier = Frontier::new();
    prev_frontier.set_source(1, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());

    let mut frontier = Frontier::new();
    frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    DiffContext::new_standalone(prev_frontier, frontier).with_pgt_name("public", st_name)
}

fn make_scalar_ctx() -> DiffContext {
    let mut prev_frontier = Frontier::new();
    prev_frontier.set_source(1, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());
    prev_frontier.set_source(2, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());

    let mut frontier = Frontier::new();
    frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());
    frontier.set_source(2, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    DiffContext::new_standalone(prev_frontier, frontier)
}

fn make_shared_source_ctx() -> DiffContext {
    // Only OID 1 — both the outer scan and the inner Filter reference the same
    // source table (orders / OID 1).  This exercises the DBSP C₀ formula where
    // the outer child and inner subquery share the same change buffer.
    let mut prev_frontier = Frontier::new();
    prev_frontier.set_source(1, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());

    let mut frontier = Frontier::new();
    frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    DiffContext::new_standalone(prev_frontier, frontier)
}

fn build_row_number_window_tree() -> OpTree {
    let child = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), text_col("region"), int_col("amount")],
        &["id"],
    );

    OpTree::Window {
        window_exprs: vec![WindowExpr {
            func_name: "ROW_NUMBER".to_string(),
            args: vec![],
            partition_by: vec![colref("region")],
            order_by: vec![sort_asc("amount")],
            frame_clause: None,
            alias: "rn".to_string(),
        }],
        partition_by: vec![colref("region")],
        pass_through: vec![
            (colref("id"), "id".to_string()),
            (colref("region"), "region".to_string()),
            (colref("amount"), "amount".to_string()),
        ],
        child: Box::new(child),
    }
}

fn build_running_sum_window_tree() -> OpTree {
    let child = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), text_col("region"), int_col("amount")],
        &["id"],
    );

    OpTree::Window {
        window_exprs: vec![WindowExpr {
            func_name: "SUM".to_string(),
            args: vec![colref("amount")],
            partition_by: vec![colref("region")],
            order_by: vec![sort_asc("amount")],
            frame_clause: Some("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string()),
            alias: "running_total".to_string(),
        }],
        partition_by: vec![colref("region")],
        pass_through: vec![
            (colref("id"), "id".to_string()),
            (colref("region"), "region".to_string()),
            (colref("amount"), "amount".to_string()),
        ],
        child: Box::new(child),
    }
}

fn build_multi_window_tree() -> OpTree {
    let child = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), text_col("region"), int_col("amount")],
        &["id"],
    );

    OpTree::Window {
        window_exprs: vec![
            WindowExpr {
                func_name: "ROW_NUMBER".to_string(),
                args: vec![],
                partition_by: vec![colref("region")],
                order_by: vec![sort_asc("amount")],
                frame_clause: None,
                alias: "rn".to_string(),
            },
            WindowExpr {
                func_name: "SUM".to_string(),
                args: vec![colref("amount")],
                partition_by: vec![colref("region")],
                order_by: vec![sort_asc("amount")],
                frame_clause: Some("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string()),
                alias: "running_total".to_string(),
            },
        ],
        partition_by: vec![colref("region")],
        pass_through: vec![
            (colref("id"), "id".to_string()),
            (colref("region"), "region".to_string()),
            (colref("amount"), "amount".to_string()),
        ],
        child: Box::new(child),
    }
}

fn build_scalar_subquery_tree() -> OpTree {
    let outer = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), int_col("amount")],
        &["id"],
    );
    let inner = scan_with_pk(2, "config", "c", vec![int_col("tax_rate")], &["id"]);

    OpTree::ScalarSubquery {
        subquery: Box::new(inner),
        alias: "current_tax".to_string(),
        subquery_source_oids: vec![2],
        child: Box::new(outer),
    }
}

fn build_shared_source_scalar_tree() -> OpTree {
    // Outer: all rows from orders (OID 1).
    // Inner: the `amount` of the single row with id=2 from orders (same OID 1).
    //
    // Using a Filter over the same table forces the inner to be deterministic
    // (exactly one row) while sharing the change buffer with the outer child.
    // This models the Q15-style scalar-subquery pattern.
    //
    // Column layout: put `amount` first so it becomes scalar_col (index 0)
    // picked up by diff_scalar_subquery.
    let outer = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), int_col("amount")],
        &["id"],
    );
    let inner_scan = scan_with_pk(
        1,
        "orders",
        "l",
        vec![int_col("amount"), int_col("id")], // amount first → becomes scalar_col
        &["id"],
    );
    let inner_filter = OpTree::Filter {
        predicate: Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("l".to_string()),
                column_name: "id".to_string(),
            }),
            right: Box::new(Expr::Literal("2".to_string())),
        },
        child: Box::new(inner_scan),
    };
    OpTree::ScalarSubquery {
        subquery: Box::new(inner_filter),
        alias: "ref_amount".to_string(),
        subquery_source_oids: vec![1],
        child: Box::new(outer),
    }
}

async fn setup_window_db() -> TestDb {
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

CREATE TABLE public.orders (
    id INT PRIMARY KEY,
    region TEXT NOT NULL,
    amount INT NOT NULL
);

CREATE TABLE public.window_unpartitioned_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    id INT NOT NULL,
    region TEXT NOT NULL,
    amount INT NOT NULL,
    global_rn BIGINT NOT NULL
);

CREATE TABLE public.window_over_agg_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    sum_amount BIGINT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    rank_amount BIGINT NOT NULL
);

CREATE TABLE public.window_row_number_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    id INT NOT NULL,
    region TEXT NOT NULL,
    amount INT NOT NULL,
    rn BIGINT NOT NULL
);

CREATE TABLE public.window_running_sum_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    id INT NOT NULL,
    region TEXT NOT NULL,
    amount INT NOT NULL,
    running_total BIGINT NOT NULL
);

CREATE TABLE public.window_multi_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    id INT NOT NULL,
    region TEXT NOT NULL,
    amount INT NOT NULL,
    rn BIGINT NOT NULL,
    running_total BIGINT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn PG_LSN NOT NULL,
    action CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id INT,
    region TEXT,
    amount INT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up window execution database");

    db
}

async fn setup_scalar_db() -> TestDb {
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

CREATE TABLE public.orders (
    id INT PRIMARY KEY,
    amount INT NOT NULL
);

CREATE TABLE public.config (
    id INT PRIMARY KEY,
    tax_rate INT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn PG_LSN NOT NULL,
    action CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id INT,
    amount INT
);

CREATE TABLE pgtrickle_changes.changes_2 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn PG_LSN NOT NULL,
    action CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id INT,
    tax_rate INT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up scalar-subquery execution database");

    db
}

async fn query_window_rows(
    db: &TestDb,
    sql: &str,
    value_column: &str,
) -> Vec<(String, i32, String, i32, i64)> {
    sqlx::query_as::<_, (String, i32, String, i32, i64)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, id, region, amount, {value_column} FROM ({sql}) delta ORDER BY __pgt_action, id, {value_column}"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated window delta SQL")
}

async fn query_multi_window_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(String, i32, String, i32, i64, i64)> {
    sqlx::query_as::<_, (String, i32, String, i32, i64, i64)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, id, region, amount, rn, running_total \
         FROM ({sql}) delta ORDER BY __pgt_action, id"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated multi-window delta SQL")
}

async fn query_scalar_rows(db: &TestDb, sql: &str) -> Vec<(String, i32, i32, i32)> {
    sqlx::query_as::<_, (String, i32, i32, i32)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, id, amount, current_tax FROM ({sql}) delta ORDER BY __pgt_action, id"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated scalar-subquery delta SQL")
}

async fn query_shared_scalar_rows(db: &TestDb, sql: &str) -> Vec<(String, i32, i32, i32)> {
    sqlx::query_as::<_, (String, i32, i32, i32)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, id, amount, ref_amount FROM ({sql}) delta ORDER BY __pgt_action, id"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated shared-source scalar-subquery delta SQL")
}

#[tokio::test]
async fn test_diff_window_executes_partition_local_row_number_recompute() {
    let db = setup_window_db().await;
    let sql = make_window_ctx("window_row_number_st")
        .differentiate(&build_row_number_window_tree())
        .expect("window differentiation should succeed");

    db.execute(
        "TRUNCATE TABLE pgtrickle_changes.changes_1, public.window_row_number_st, public.orders RESTART IDENTITY",
    )
    .await;

    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (2, 'east', 20), \
         (3, 'west', 15), \
         (4, 'east', 15)",
    )
    .await;
    db.execute(
        "INSERT INTO public.window_row_number_st VALUES \
         (pgtrickle.test_int_to_row_id(1), 1, 'east', 10, 1), \
         (pgtrickle.test_int_to_row_id(2), 2, 'east', 20, 2), \
         (pgtrickle.test_int_to_row_id(3), 3, 'west', 15, 1)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(4), 4, 'east', 15)",
    )
    .await;

    assert_eq!(
        query_window_rows(&db, &sql, "rn").await,
        vec![
            ("D".to_string(), 1, "east".to_string(), 10, 1),
            ("D".to_string(), 2, "east".to_string(), 20, 2),
            ("I".to_string(), 1, "east".to_string(), 10, 1),
            ("I".to_string(), 2, "east".to_string(), 20, 3),
            ("I".to_string(), 4, "east".to_string(), 15, 2),
        ]
    );
}

#[tokio::test]
async fn test_diff_window_executes_frame_sensitive_running_sum_recompute() {
    let db = setup_window_db().await;
    let sql = make_window_ctx("window_running_sum_st")
        .differentiate(&build_running_sum_window_tree())
        .expect("window differentiation should succeed");

    db.execute(
        "TRUNCATE TABLE pgtrickle_changes.changes_1, public.window_running_sum_st, public.orders RESTART IDENTITY",
    )
    .await;

    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (3, 'west', 15), \
         (5, 'west', 30), \
         (6, 'west', 25)",
    )
    .await;
    db.execute(
        "INSERT INTO public.window_running_sum_st VALUES \
         (pgtrickle.test_int_to_row_id(1), 1, 'east', 10, 10), \
         (pgtrickle.test_int_to_row_id(3), 3, 'west', 15, 15), \
         (pgtrickle.test_int_to_row_id(5), 5, 'west', 30, 45)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(6), 6, 'west', 25)",
    )
    .await;

    assert_eq!(
        query_window_rows(&db, &sql, "running_total").await,
        vec![
            ("D".to_string(), 3, "west".to_string(), 15, 15),
            ("D".to_string(), 5, "west".to_string(), 30, 45),
            ("I".to_string(), 3, "west".to_string(), 15, 15),
            ("I".to_string(), 5, "west".to_string(), 30, 70),
            ("I".to_string(), 6, "west".to_string(), 25, 40),
        ]
    );
}

#[tokio::test]
async fn test_diff_scalar_subquery_executes_inner_change_recompute() {
    let db = setup_scalar_db().await;
    let sql = make_scalar_ctx()
        .differentiate(&build_scalar_subquery_tree())
        .expect("scalar-subquery differentiation should succeed");

    db.execute(
        "TRUNCATE TABLE pgtrickle_changes.changes_1, pgtrickle_changes.changes_2, public.orders, public.config RESTART IDENTITY",
    )
    .await;
    db.execute("INSERT INTO public.orders VALUES (1, 100), (2, 200)")
        .await;
    db.execute("INSERT INTO public.config VALUES (1, 20)").await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, __pgt_row_id, id, tax_rate) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(1), 1, 10), \
                ('0/2', 'I', pgtrickle.test_int_to_row_id(1), 1, 20)",
    )
    .await;

    assert_eq!(
        query_scalar_rows(&db, &sql).await,
        vec![
            ("D".to_string(), 1, 100, 10),
            ("D".to_string(), 2, 200, 10),
            ("I".to_string(), 1, 100, 20),
            ("I".to_string(), 2, 200, 20),
        ]
    );
}

#[tokio::test]
async fn test_diff_scalar_subquery_executes_outer_insert_with_current_scalar() {
    let db = setup_scalar_db().await;
    let sql = make_scalar_ctx()
        .differentiate(&build_scalar_subquery_tree())
        .expect("scalar-subquery differentiation should succeed");

    db.execute(
        "TRUNCATE TABLE pgtrickle_changes.changes_1, pgtrickle_changes.changes_2, public.orders, public.config RESTART IDENTITY",
    )
    .await;
    db.execute("INSERT INTO public.orders VALUES (1, 100), (2, 200), (3, 300)")
        .await;
    db.execute("INSERT INTO public.config VALUES (1, 10)").await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(3), 3, 300)",
    )
    .await;

    assert_eq!(
        query_scalar_rows(&db, &sql).await,
        vec![("I".to_string(), 3, 300, 10)]
    );
}

#[tokio::test]
async fn test_diff_window_executes_partition_move_recomputes_both_partitions() {
    let db = setup_window_db().await;
    let sql = make_window_ctx("window_row_number_st")
        .differentiate(&build_row_number_window_tree())
        .expect("window differentiation should succeed");

    db.execute(
        "TRUNCATE TABLE pgtrickle_changes.changes_1, public.window_row_number_st, public.orders RESTART IDENTITY",
    )
    .await;

    // Seed the current (post-update) state of orders: order 2 has moved east→west.
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (2, 'west', 20), \
         (3, 'west', 15)",
    )
    .await;

    // Storage table reflects the pre-update window state.
    db.execute(
        "INSERT INTO public.window_row_number_st VALUES \
         (pgtrickle.test_int_to_row_id(1), 1, 'east', 10, 1), \
         (pgtrickle.test_int_to_row_id(2), 2, 'east', 20, 2), \
         (pgtrickle.test_int_to_row_id(3), 3, 'west', 15, 1)",
    )
    .await;

    // UPDATE: order 2 moves from 'east' to 'west' (amount unchanged).
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(2), 2, 'east', 20), \
                ('0/2', 'I', pgtrickle.test_int_to_row_id(2), 2, 'west', 20)",
    )
    .await;

    // Both partitions are affected: east loses row 2, west gains row 2.
    // East new state: {(1,10)} → rn=1.  West new state: {(3,15),(2,20)} → rn=1,2.
    assert_eq!(
        query_window_rows(&db, &sql, "rn").await,
        vec![
            ("D".to_string(), 1, "east".to_string(), 10, 1),
            ("D".to_string(), 2, "east".to_string(), 20, 2),
            ("D".to_string(), 3, "west".to_string(), 15, 1),
            ("I".to_string(), 1, "east".to_string(), 10, 1),
            ("I".to_string(), 2, "west".to_string(), 20, 2),
            ("I".to_string(), 3, "west".to_string(), 15, 1),
        ]
    );
}

#[tokio::test]
async fn test_diff_window_executes_multi_window_expression_recompute() {
    let db = setup_window_db().await;
    let sql = make_window_ctx("window_multi_st")
        .differentiate(&build_multi_window_tree())
        .expect("multi-window differentiation should succeed");

    db.execute(
        "TRUNCATE TABLE pgtrickle_changes.changes_1, public.window_multi_st, public.orders RESTART IDENTITY",
    )
    .await;

    // Initial east partition: orders 1 (amount=10) and 2 (amount=20).
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (2, 'east', 20), \
         (3, 'east', 15)",
    )
    .await;

    // Storage reflects pre-insert window state for east: two rows.
    db.execute(
        "INSERT INTO public.window_multi_st VALUES \
         (pgtrickle.test_int_to_row_id(1), 1, 'east', 10, 1, 10), \
         (pgtrickle.test_int_to_row_id(2), 2, 'east', 20, 2, 30)",
    )
    .await;

    // INSERT order 3 into east partition (new current state already seeded above).
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(3), 3, 'east', 15)",
    )
    .await;

    // After insert, east sorted by amount: (1,10)→rn=1,rt=10, (3,15)→rn=2,rt=25, (2,20)→rn=3,rt=45.
    // Both ROW_NUMBER and the running SUM update for the whole partition.
    assert_eq!(
        query_multi_window_rows(&db, &sql).await,
        vec![
            ("D".to_string(), 1, "east".to_string(), 10, 1_i64, 10_i64),
            ("D".to_string(), 2, "east".to_string(), 20, 2_i64, 30_i64),
            ("I".to_string(), 1, "east".to_string(), 10, 1_i64, 10_i64),
            ("I".to_string(), 2, "east".to_string(), 20, 3_i64, 45_i64),
            ("I".to_string(), 3, "east".to_string(), 15, 2_i64, 25_i64),
        ]
    );
}

#[tokio::test]
async fn test_diff_scalar_subquery_executes_simultaneous_outer_and_inner_change() {
    // Exercises the DBSP cross-product formula Δ(C × S) = (ΔC × S₁) + (C₀ × ΔS)
    // when both the outer child (orders) and inner scalar source (config) change
    // in the same batch.  The C₀ pre-image (computed via EXCEPT ALL) must exclude
    // the newly inserted order row from the Part-2 DELETE output.
    let db = setup_scalar_db().await;
    let sql = make_scalar_ctx()
        .differentiate(&build_scalar_subquery_tree())
        .expect("scalar-subquery differentiation should succeed");

    db.execute(
        "TRUNCATE TABLE pgtrickle_changes.changes_1, pgtrickle_changes.changes_2, public.orders, public.config RESTART IDENTITY",
    )
    .await;

    // Post-change state: order 3 inserted, config tax_rate updated to 20.
    db.execute("INSERT INTO public.orders VALUES (1, 100), (2, 200), (3, 300)")
        .await;
    db.execute("INSERT INTO public.config VALUES (1, 20)").await;

    // Outer change: INSERT order (3, 300).
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(3), 3, 300)",
    )
    .await;

    // Inner change: UPDATE config tax_rate from 10 → 20.
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, __pgt_row_id, id, tax_rate) \
         VALUES ('0/2', 'D', pgtrickle.test_int_to_row_id(1), 1, 10), \
                ('0/3', 'I', pgtrickle.test_int_to_row_id(1), 1, 20)",
    )
    .await;

    // Part 1 (ΔC × S₁): new outer row (3,300) with new scalar 20 → I(3,300,20).
    // Part 2 (C₀ × ΔS): C₀ = {(1,100),(2,200)} (excludes the inserted row 3).
    //   Emits D(1,100,10), D(2,200,10) and I(1,100,20), I(2,200,20).
    // Row (3,300) must NOT appear in any Part-2 DELETE — that would indicate
    // the implementation is incorrectly using C₁ instead of C₀.
    assert_eq!(
        query_scalar_rows(&db, &sql).await,
        vec![
            ("D".to_string(), 1, 100, 10),
            ("D".to_string(), 2, 200, 10),
            ("I".to_string(), 1, 100, 20),
            ("I".to_string(), 2, 200, 20),
            ("I".to_string(), 3, 300, 20),
        ]
    );
}

#[tokio::test]
async fn test_diff_scalar_subquery_executes_shared_source_outer_insert_stable_scalar() {
    // Both the outer scan (orders) and the inner Filter(id=2, orders) reference
    // source OID 1 — the same change buffer (changes_1).  This exercises the
    // shared-source path: the diff engine must correctly split one change buffer
    // into an outer delta CTE and an inner delta CTE, and the C₀ EXCEPT ALL must
    // exclude newly inserted outer rows so Part-2 does not fire when the inner
    // scalar is stable.
    let db = setup_scalar_db().await;
    let sql = make_shared_source_ctx()
        .differentiate(&build_shared_source_scalar_tree())
        .expect("shared-source scalar-subquery differentiation should succeed");

    db.execute(
        "TRUNCATE TABLE pgtrickle_changes.changes_1, pgtrickle_changes.changes_2, \
         public.orders, public.config RESTART IDENTITY",
    )
    .await;

    // Initial (post-change) state: orders {(1,100),(2,200),(3,999)}.
    // Inner scalar = amount WHERE id=2 = 200 (stable — id=3 does not match filter id=2).
    db.execute("INSERT INTO public.orders VALUES (1, 100), (2, 200), (3, 999)")
        .await;

    // Outer change only: INSERT order (3, 999).
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(3), 3, 999)",
    )
    .await;

    // Only Part 1 fires (inner delta is empty — filter id=2 passes nothing from
    // the insert of id=3).  C₀ EXCEPT ALL correctly excludes row 3 from the
    // outer pre-change snapshot, confirming the shared-source path does not
    // accidentally include the new row in a spurious Part-2 DELETE.
    assert_eq!(
        query_shared_scalar_rows(&db, &sql).await,
        vec![("I".to_string(), 3, 999, 200)]
    );
}

fn build_unpartitioned_window_tree() -> OpTree {
    let child = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), text_col("region"), int_col("amount")],
        &["id"],
    );

    OpTree::Window {
        window_exprs: vec![WindowExpr {
            func_name: "ROW_NUMBER".to_string(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![sort_asc("amount")],
            frame_clause: None,
            alias: "global_rn".to_string(),
        }],
        partition_by: vec![],
        pass_through: vec![
            (colref("id"), "id".to_string()),
            (colref("region"), "region".to_string()),
            (colref("amount"), "amount".to_string()),
        ],
        child: Box::new(child),
    }
}

#[tokio::test]
async fn test_diff_window_executes_unpartitioned_global_recompute() {
    let db = setup_window_db().await;
    let sql = make_window_ctx("window_unpartitioned_st")
        .differentiate(&build_unpartitioned_window_tree())
        .expect("window differentiation should succeed");

    db.execute(
        "TRUNCATE TABLE pgtrickle_changes.changes_1, public.window_unpartitioned_st, public.orders RESTART IDENTITY",
    )
    .await;

    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (2, 'east', 20), \
         (3, 'west', 15), \
         (4, 'east', 40)",
    )
    .await;
    db.execute(
        "INSERT INTO public.window_unpartitioned_st VALUES \
         (pgtrickle.test_int_to_row_id(1), 1, 'east', 10, 1), \
         (pgtrickle.test_int_to_row_id(3), 3, 'west', 15, 2), \
         (pgtrickle.test_int_to_row_id(2), 2, 'east', 20, 3), \
         (pgtrickle.test_int_to_row_id(4), 4, 'east', 40, 4)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(5), 5, 'north', 5)",
    )
    .await;

    // A change to amount=5 should bump everything down by 1 in row_number!
    // The query returns (action, id, region, amount, rn).
    assert_eq!(
        query_window_rows(&db, &sql, "global_rn").await,
        vec![
            ("D".to_string(), 1, "east".to_string(), 10, 1),
            ("D".to_string(), 2, "east".to_string(), 20, 3),
            ("D".to_string(), 3, "west".to_string(), 15, 2),
            ("D".to_string(), 4, "east".to_string(), 40, 4),
            ("I".to_string(), 1, "east".to_string(), 10, 2),
            ("I".to_string(), 2, "east".to_string(), 20, 4),
            ("I".to_string(), 3, "west".to_string(), 15, 3),
            ("I".to_string(), 4, "east".to_string(), 40, 5),
            ("I".to_string(), 5, "north".to_string(), 5, 1),
        ]
    );
}
