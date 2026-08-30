//! Execution-backed tests for nested natural-join-style DVM SQL.
//!
//! Schema: (sales INNER JOIN branches ON s.branch_id = b.branch_id)
//!               INNER JOIN regions ON b.region_id = r.region_id
//!
//! Delta output columns:
//! join__s__id, join__s__branch_id, join__s__amount,
//! join__b__branch_id, join__b__region_id, join__b__name,
//! r__region_id, r__name.

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

fn build_nested_natural_join_tree() -> OpTree {
    let sales = scan_with_pk(
        1,
        "sales",
        "s",
        vec![int_col("id"), int_col("branch_id"), int_col("amount")],
        &["id"],
    );
    let branches = scan_with_pk(
        2,
        "branches",
        "b",
        vec![int_col("branch_id"), int_col("region_id"), text_col("name")],
        &["branch_id"],
    );
    let inner_join = OpTree::InnerJoin {
        condition: natural_cond("s", "b", "branch_id"),
        left: Box::new(sales),
        right: Box::new(branches),
    };
    let regions = scan_with_pk(
        3,
        "regions",
        "r",
        vec![int_col("region_id"), text_col("name")],
        &["region_id"],
    );
    OpTree::InnerJoin {
        condition: natural_cond("b", "r", "region_id"),
        left: Box::new(inner_join),
        right: Box::new(regions),
    }
}

// ── DB setup ─────────────────────────────────────────────────────────────────

async fn setup_nested_nj_db() -> TestDb {
    let db = TestDb::new().await;

    sqlx::raw_sql(
        r#"
CREATE SCHEMA IF NOT EXISTS pgtrickle;
CREATE SCHEMA IF NOT EXISTS pgtrickle_changes;

CREATE OR REPLACE FUNCTION pgtrickle.pg_trickle_hash_multi(vals TEXT[])
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT hashtextextended(COALESCE(array_to_string(vals, '|', '<NULL>'), ''), 0)::BIGINT
$$;

CREATE TABLE public.sales (
    id        INT PRIMARY KEY,
    branch_id INT NOT NULL,
    amount    INT NOT NULL
);

CREATE TABLE public.branches (
    branch_id INT PRIMARY KEY,
    region_id INT NOT NULL,
    name      TEXT NOT NULL
);

CREATE TABLE public.regions (
    region_id INT PRIMARY KEY,
    name      TEXT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id      BIGSERIAL PRIMARY KEY,
    lsn            PG_LSN NOT NULL,
    action         CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id         INT,
    branch_id  INT,
    amount     INT
);

CREATE TABLE pgtrickle_changes.changes_2 (
    change_id      BIGSERIAL PRIMARY KEY,
    lsn            PG_LSN NOT NULL,
    action         CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    branch_id  INT,
    region_id  INT,
    name       TEXT
);

CREATE TABLE pgtrickle_changes.changes_3 (
    change_id      BIGSERIAL PRIMARY KEY,
    lsn            PG_LSN NOT NULL,
    action         CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    region_id  INT,
    name       TEXT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up nested natural-join database");

    db
}

async fn reset_nested_nj_fixture(db: &TestDb) {
    db.execute(
        "TRUNCATE TABLE \
         pgtrickle_changes.changes_1, \
         pgtrickle_changes.changes_2, \
         pgtrickle_changes.changes_3, \
         public.sales, \
         public.branches, \
         public.regions \
         RESTART IDENTITY",
    )
    .await;
}

/// Query delta rows: (action, s.id, s.branch_id, s.amount, b.branch_id, b.region_id, b.name, r.region_id, r.name)
async fn query_nested_nj_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(String, i32, i32, i32, i32, i32, String, i32, String)> {
    sqlx::query_as::<_, _>(sqlx::AssertSqlSafe(format!(
        r#"SELECT __pgt_action,
                  "join__s__id",
                  "join__s__branch_id",
                  "join__s__amount",
                  "join__b__branch_id",
                  "join__b__region_id",
                  "join__b__name",
                  "r__region_id",
                  "r__name"
           FROM ({sql}) delta
           ORDER BY __pgt_action, "join__s__id""#
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated nested natural-join delta SQL")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Innermost insert cascades through natural joins:
/// Part 1a -> Part 1a.
#[tokio::test]
async fn test_diff_nested_natural_join_innermost_insert_fully_matched() {
    let db = setup_nested_nj_db().await;
    let sql = make_ctx_3()
        .differentiate(&build_nested_natural_join_tree())
        .expect("differentiation should succeed");

    reset_nested_nj_fixture(&db).await;

    // Existing branches and regions
    db.execute("INSERT INTO public.branches VALUES (10, 20, 'Downtown')")
        .await;
    db.execute("INSERT INTO public.regions VALUES (20, 'North')")
        .await;

    // Insert sale matching branch 10
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, branch_id, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(1), 10, 100)",
    )
    .await;

    let rows = query_nested_nj_rows(&db, &sql).await;

    assert_eq!(
        rows,
        vec![(
            "I".to_string(),
            1,
            10,
            100,
            10,
            20,
            "Downtown".to_string(),
            20,
            "North".to_string()
        )]
    );
}

/// Outermost delete: region deleted while inner join (sale+branch) remains intact.
/// Outer join's Part 2 (L₀ ⋈ ΔR_deletes) should emit D for matched.
#[tokio::test]
async fn test_diff_nested_natural_join_outermost_delete() {
    let db = setup_nested_nj_db().await;
    let sql = make_ctx_3()
        .differentiate(&build_nested_natural_join_tree())
        .expect("differentiation should succeed");

    reset_nested_nj_fixture(&db).await;

    db.execute("INSERT INTO public.sales VALUES (1, 10, 100)")
        .await;
    db.execute("INSERT INTO public.branches VALUES (10, 20, 'Downtown')")
        .await;
    // Region 20 has been deleted

    db.execute(
        "INSERT INTO pgtrickle_changes.changes_3 \
         (lsn, action, __pgt_row_id, region_id, name) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(20), 'North')",
    )
    .await;

    let rows = query_nested_nj_rows(&db, &sql).await;

    assert_eq!(
        rows,
        vec![(
            "D".to_string(),
            1,
            10,
            100,
            10,
            20,
            "Downtown".to_string(),
            20,
            "North".to_string()
        )]
    );
}
