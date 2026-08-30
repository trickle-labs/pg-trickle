//! Execution-backed tests for nested full-join (FULL OUTER JOIN) DVM SQL.
//!
//! Tests the three-table FULL JOIN chain:
//!   (orders o FULL JOIN products p ON o.prod_id = p.id)
//!       FULL JOIN categories c ON p.cat_id = c.id
//!
//! Delta output columns (all nullable — either side can be NULL-padded):
//!   join__o__id, join__o__prod_id, join__o__amount,
//!   join__p__id, join__p__cat_id, join__p__name,
//!   c__id, c__label.

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

/// (orders o FULL JOIN products p ON o.prod_id = p.id)
///     FULL JOIN categories c ON p.cat_id = c.id
fn build_nested_full_join_tree() -> OpTree {
    let orders = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), int_col("prod_id"), int_col("amount")],
        &["id"],
    );
    let products = scan_with_pk(
        2,
        "products",
        "p",
        vec![int_col("id"), int_col("cat_id"), text_col("name")],
        &["id"],
    );
    let inner_join = OpTree::FullJoin {
        condition: eq_cond("o", "prod_id", "p", "id"),
        left: Box::new(orders),
        right: Box::new(products),
    };
    let categories = scan_with_pk(
        3,
        "categories",
        "c",
        vec![int_col("id"), text_col("label")],
        &["id"],
    );
    OpTree::FullJoin {
        condition: eq_cond("p", "cat_id", "c", "id"),
        left: Box::new(inner_join),
        right: Box::new(categories),
    }
}

// ── DB setup ──────────────────────────────────────────────────────────────────

async fn setup_nested_fj_db() -> TestDb {
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
    id      INT PRIMARY KEY,
    prod_id INT NOT NULL,
    amount  INT NOT NULL
);

CREATE TABLE public.products (
    id     INT PRIMARY KEY,
    cat_id INT NOT NULL,
    name   TEXT NOT NULL
);

CREATE TABLE public.categories (
    id    INT PRIMARY KEY,
    label TEXT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id   BIGSERIAL PRIMARY KEY,
    lsn         PG_LSN NOT NULL,
    action      CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id      INT,
    prod_id INT,
    amount  INT
);

CREATE TABLE pgtrickle_changes.changes_2 (
    change_id  BIGSERIAL PRIMARY KEY,
    lsn        PG_LSN NOT NULL,
    action     CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id     INT,
    cat_id INT,
    name   TEXT
);

CREATE TABLE pgtrickle_changes.changes_3 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn       PG_LSN NOT NULL,
    action    CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id    INT,
    label TEXT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up nested full-join execution database");

    db
}

async fn reset_nested_fj_fixture(db: &TestDb) {
    db.execute(
        "TRUNCATE TABLE \
         pgtrickle_changes.changes_1, \
         pgtrickle_changes.changes_2, \
         pgtrickle_changes.changes_3, \
         public.orders, \
         public.products, \
         public.categories \
         RESTART IDENTITY",
    )
    .await;
}

/// Query delta rows for the nested full join output.
///
/// All columns are nullable because either side of a FULL JOIN can be NULL-padded.
type NestedFjRow = (
    String,         // __pgt_action
    Option<i32>,    // join__o__id
    Option<i32>,    // join__o__prod_id
    Option<i32>,    // join__o__amount
    Option<i32>,    // join__p__id
    Option<i32>,    // join__p__cat_id
    Option<String>, // join__p__name
    Option<i32>,    // c__id
    Option<String>, // c__label
);

async fn query_nested_fj_rows(db: &TestDb, sql: &str) -> Vec<NestedFjRow> {
    sqlx::query_as::<_, NestedFjRow>(sqlx::AssertSqlSafe(format!(
        r#"SELECT __pgt_action,
                  "join__o__id",
                  "join__o__prod_id",
                  "join__o__amount",
                  "join__p__id",
                  "join__p__cat_id",
                  "join__p__name",
                  "c__id",
                  "c__label"
           FROM ({sql}) delta
           ORDER BY __pgt_action,
                    "join__o__id" NULLS LAST,
                    "join__p__id" NULLS LAST,
                    "c__id" NULLS LAST"#
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated nested full-join delta SQL")
}

// ── tests ──────────────────────────────────────────────────────────────────────

/// Order inserted, product and category both exist: the delta cascades through
/// both FULL JOINs and emits a fully-matched I row with all columns populated.
///
/// Inner Part 1a → outer Part 1a: full cascade of a matching innermost insert.
#[tokio::test]
async fn test_diff_nested_full_join_innermost_insert_fully_matched() {
    let db = setup_nested_fj_db().await;
    let sql = make_ctx_3()
        .differentiate(&build_nested_full_join_tree())
        .expect("nested full-join differentiation should succeed");

    reset_nested_fj_fixture(&db).await;
    db.execute("INSERT INTO public.products   VALUES (10, 5, 'Widget')")
        .await;
    db.execute("INSERT INTO public.categories VALUES (5, 'Electronics')")
        .await;

    // INSERT order (1, 10, 100)
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, prod_id, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(1), 1, 10, 100)",
    )
    .await;

    assert_eq!(
        query_nested_fj_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            Some(1),
            Some(10),
            Some(100),
            Some(10),
            Some(5),
            Some("Widget".to_string()),
            Some(5),
            Some("Electronics".to_string()),
        )]
    );
}

/// Category inserted with no matching products: the outer FULL JOIN has no
/// left-side rows that reference this category, so Part 6 fires and emits an
/// I row with all left/inner columns NULL and only c.* populated.
///
/// This path is UNIQUE TO FULL OUTER JOIN (Part 6 — symmetric right-anti path).
#[tokio::test]
async fn test_diff_nested_full_join_outermost_right_insert_unmatched() {
    let db = setup_nested_fj_db().await;
    let sql = make_ctx_3()
        .differentiate(&build_nested_full_join_tree())
        .expect("nested full-join differentiation should succeed");

    reset_nested_fj_fixture(&db).await;
    // Insert order and product that reference cat_id=5, NOT 99.
    db.execute("INSERT INTO public.orders   VALUES (1, 10, 100)")
        .await;
    db.execute("INSERT INTO public.products VALUES (10, 5, 'Widget')")
        .await;
    db.execute("INSERT INTO public.categories VALUES (99, 'Rare')")
        .await;

    // INSERT category (99, "Rare") — no products reference cat_id=99
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_3 \
         (lsn, action, __pgt_row_id, id, label) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(99), 99, 'Rare')",
    )
    .await;

    // Part 6: I with all inner/left columns NULL, only c.* set.
    assert_eq!(
        query_nested_fj_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(99),
            Some("Rare".to_string()),
        )]
    );
}
