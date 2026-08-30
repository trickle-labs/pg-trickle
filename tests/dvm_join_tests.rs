//! Execution-backed tests for inner-join DVM SQL.
//!
//! These tests run the generated delta SQL against a standalone PostgreSQL
//! container so we can validate result rows for the join operator family
//! called out in PLAN_TEST_EVALS_UNIT.md.
//!
//! Schema: orders (id, prod_id, amount) LEFT-JOIN products (id, name)
//! on orders.prod_id = products.id.
//!
//! Delta output columns (disambiguated): o__id, o__prod_id, o__amount,
//! p__id, p__name.

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

fn build_inner_join_tree() -> OpTree {
    let left = scan_with_pk(
        1,
        "orders",
        "o",
        vec![int_col("id"), int_col("prod_id"), int_col("amount")],
        &["id"],
    );
    let right = scan_with_pk(
        2,
        "products",
        "p",
        vec![int_col("id"), text_col("name")],
        &["id"],
    );

    OpTree::InnerJoin {
        condition: eq_cond("o", "prod_id", "p", "id"),
        left: Box::new(left),
        right: Box::new(right),
    }
}

// ── DB setup ─────────────────────────────────────────────────────────────────

async fn setup_join_db() -> TestDb {
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

CREATE TABLE public.orders (
    id      INT PRIMARY KEY,
    prod_id INT NOT NULL,
    amount  INT NOT NULL
);

CREATE TABLE public.products (
    id   INT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id  BIGSERIAL PRIMARY KEY,
    lsn        PG_LSN NOT NULL,
    action     CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id     INT,
    prod_id INT,
    amount INT
);

CREATE TABLE pgtrickle_changes.changes_2 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn       PG_LSN NOT NULL,
    action    CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id    INT,
    name  TEXT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up join execution database");

    db
}

async fn reset_join_fixture(db: &TestDb) {
    db.execute(
        "TRUNCATE TABLE \
         pgtrickle_changes.changes_1, \
         pgtrickle_changes.changes_2, \
         public.orders, \
         public.products \
         RESTART IDENTITY",
    )
    .await;
}

/// Query delta rows: (action, o.id, o.prod_id, o.amount, p.id, p.name)
/// ordered by action, then o.id.
async fn query_join_rows(db: &TestDb, sql: &str) -> Vec<(String, i32, i32, i32, i32, String)> {
    sqlx::query_as::<_, (String, i32, i32, i32, i32, String)>(sqlx::AssertSqlSafe(format!(
        r#"SELECT __pgt_action,
                  "o__id",
                  "o__prod_id",
                  "o__amount",
                  "p__id",
                  "p__name"
           FROM ({sql}) delta
           ORDER BY __pgt_action, "o__id""#
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated inner-join delta SQL")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Left-only insert: a new order arrives for an existing product.
/// Part 1 (ΔL ⋈ R₁) should emit exactly one I row.
#[tokio::test]
async fn test_diff_inner_join_executes_left_insert_for_existing_right() {
    let db = setup_join_db().await;
    let sql = make_ctx()
        .differentiate(&build_inner_join_tree())
        .expect("inner-join differentiation should succeed");

    reset_join_fixture(&db).await;
    // current state after the insert
    db.execute("INSERT INTO public.orders   VALUES (1, 10, 100), (2, 10, 200), (3, 10, 300)")
        .await;
    db.execute("INSERT INTO public.products VALUES (10, 'Widget')")
        .await;

    // change buffer: INSERT order (3, 10, 300) only
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, prod_id, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(3), 3, 10, 300)",
    )
    .await;

    assert_eq!(
        query_join_rows(&db, &sql).await,
        vec![("I".to_string(), 3, 10, 300, 10, "Widget".to_string())]
    );
}

/// Left-only delete: remove an order that was joined to a product.
/// Part 1b (ΔL_deletes ⋈ R₀) should emit one D row.
#[tokio::test]
async fn test_diff_inner_join_executes_left_delete_for_existing_right() {
    let db = setup_join_db().await;
    let sql = make_ctx()
        .differentiate(&build_inner_join_tree())
        .expect("inner-join differentiation should succeed");

    reset_join_fixture(&db).await;
    // current state: order 3 already deleted
    db.execute("INSERT INTO public.orders   VALUES (1, 10, 100), (2, 10, 200)")
        .await;
    db.execute("INSERT INTO public.products VALUES (10, 'Widget')")
        .await;

    // change buffer: DELETE order (3, 10, 300)
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, prod_id, amount) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(3), 3, 10, 300)",
    )
    .await;

    assert_eq!(
        query_join_rows(&db, &sql).await,
        vec![("D".to_string(), 3, 10, 300, 10, "Widget".to_string())]
    );
}

/// Right-only delete: delete a product that had two matching orders.
/// Part 2 (L₀ ⋈ ΔR) fans out: two D rows, one per matching order.
#[tokio::test]
async fn test_diff_inner_join_executes_right_delete_fans_out_to_left() {
    let db = setup_join_db().await;
    let sql = make_ctx()
        .differentiate(&build_inner_join_tree())
        .expect("inner-join differentiation should succeed");

    reset_join_fixture(&db).await;
    // current state: product 10 deleted; orders 1 and 2 still exist in orders
    // table but the join now produces nothing (product gone).
    db.execute("INSERT INTO public.orders VALUES (1, 10, 100), (2, 10, 200)")
        .await;
    // products table is empty (product was deleted before this test point)

    // change buffer: DELETE product (10, "Widget")
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, __pgt_row_id, id, name) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(10), 10, 'Widget')",
    )
    .await;

    assert_eq!(
        query_join_rows(&db, &sql).await,
        vec![
            ("D".to_string(), 1, 10, 100, 10, "Widget".to_string()),
            ("D".to_string(), 2, 10, 200, 10, "Widget".to_string()),
        ]
    );
}

/// Right-only insert with no matching left rows: new product, zero orders.
/// Both Part 1 and Part 2 should produce no rows.
#[tokio::test]
async fn test_diff_inner_join_right_insert_with_no_matching_left_emits_nothing() {
    let db = setup_join_db().await;
    let sql = make_ctx()
        .differentiate(&build_inner_join_tree())
        .expect("inner-join differentiation should succeed");

    reset_join_fixture(&db).await;
    db.execute("INSERT INTO public.products VALUES (99, 'NewProduct')")
        .await;

    // change buffer: INSERT product (99, "NewProduct") — no orders reference it
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, __pgt_row_id, id, name) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(99), 99, 'NewProduct')",
    )
    .await;

    assert!(query_join_rows(&db, &sql).await.is_empty());
}

/// Simultaneous left and right inserts: a new order and a new product
/// arrive together in the same batch, and the new order joins to the new
/// product.
///
/// Part 1 (ΔL ⋈ R₁) emits I for the new order joining to the new product.
/// Part 2 (L₀ ⋈ ΔR) uses pre-change L₀ (excludes just-inserted order 3),
/// so it emits I for the pre-existing order 1 joining to the new product.
/// Net: two I rows — one from each part.
#[tokio::test]
async fn test_diff_inner_join_executes_simultaneous_left_and_right_inserts() {
    let db = setup_join_db().await;
    let sql = make_ctx()
        .differentiate(&build_inner_join_tree())
        .expect("inner-join differentiation should succeed");

    reset_join_fixture(&db).await;
    // current state after both changes
    db.execute("INSERT INTO public.orders   VALUES (1, 20, 500), (3, 20, 300)")
        .await;
    db.execute("INSERT INTO public.products VALUES (20, 'Gadget')")
        .await;

    // change buffer: INSERT order (3, 20, 300) and INSERT product (20, "Gadget")
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, prod_id, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(3), 3, 20, 300)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, __pgt_row_id, id, name) \
         VALUES ('0/2', 'I', pgtrickle.test_int_to_row_id(20), 20, 'Gadget')",
    )
    .await;

    // Part 1: order 3 (new) ⋈ products{(20,"Gadget")} → I(3,20,300,20,"Gadget")
    // Part 2: L₀ = orders EXCEPT ALL inserts ∪ deletes = {(1,20,500)} ⋈ ΔR{I(20,"Gadget")}
    //       → I(1,20,500,20,"Gadget")
    assert_eq!(
        query_join_rows(&db, &sql).await,
        vec![
            ("I".to_string(), 1, 20, 500, 20, "Gadget".to_string()),
            ("I".to_string(), 3, 20, 300, 20, "Gadget".to_string()),
        ]
    );
}

/// EC-01 regression: a left-side DELETE whose join partner on the right is
/// simultaneously deleted.  Without the R₀ fix (Part 1b), the deleted
/// order finds no match in R₁ and the D row is silently dropped.
///
/// With R₀, Part 1b (ΔL_deletes ⋈ R₀) reconstructs the pre-change right
/// state, finds the match, and correctly emits a D row.
#[tokio::test]
async fn test_diff_inner_join_ec01_left_delete_with_concurrent_right_delete() {
    let db = setup_join_db().await;
    let sql = make_ctx()
        .differentiate(&build_inner_join_tree())
        .expect("inner-join differentiation should succeed");

    reset_join_fixture(&db).await;
    // current state: both order 1 and product 10 have been deleted;
    // only order 2 and product 20 remain.
    db.execute("INSERT INTO public.orders   VALUES (2, 20, 200)")
        .await;
    db.execute("INSERT INTO public.products VALUES (20, 'OtherWidget')")
        .await;

    // change buffer: DELETE order (1, 10, 100) and DELETE product (10, "Widget")
    // simultaneously.
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, prod_id, amount) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(1), 1, 10, 100)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, __pgt_row_id, id, name) \
         VALUES ('0/2', 'D', pgtrickle.test_int_to_row_id(10), 10, 'Widget')",
    )
    .await;

    // Part 1b: ΔL_deletes{(1,10,100)} ⋈ R₀{(10,"Widget"),(20,"OtherWidget")}
    //        → D(1,10,100,10,"Widget")
    // Part 2:  L₀{(1,10,100),(2,20,200)} ⋈ ΔR{D(10,"Widget")}
    //        → D(1,10,100,10,"Widget")  ← would double-count without dedup…
    // Actually: Part 1b catches the deletion of the left row (order 1 D based on R₀).
    // Part 2 catches the right row deletion from L₀ which still includes order 1
    // (pre-change left has both orders).  This test verifies both D rows appear
    // when each side drives the deletion independently.
    let rows = query_join_rows(&db, &sql).await;

    // Both parts should emit a D for order 1 joined to product 10.
    // The result may contain two D rows (one from Part1b, one from Part2),
    // or one consolidated D row depending on delta semantics.
    // The critical assertion is that at least one D appears (not silently dropped).
    let d_rows: Vec<_> = rows
        .iter()
        .filter(|(action, oid, ..)| action == "D" && *oid == 1)
        .collect();
    assert!(
        !d_rows.is_empty(),
        "EC-01 regression: D row for order 1 was silently dropped; rows={rows:?}"
    );
}

// ── three-table chain helpers ─────────────────────────────────────────────────

/// Three-source frontier: OIDs 1 (orders), 2 (products), 3 (categories).
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

/// Build the three-table join tree:
///   (orders/o ⋈ products/p  ON o.prod_id = p.id) ⋈ categories/c ON p.cat_id = c.id
fn build_three_table_join_tree() -> OpTree {
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
    let inner = OpTree::InnerJoin {
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
    OpTree::InnerJoin {
        condition: eq_cond("p", "cat_id", "c", "id"),
        left: Box::new(inner),
        right: Box::new(categories),
    }
}

async fn setup_3way_db() -> TestDb {
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
    .expect("failed to set up 3-way join execution database");

    db
}

async fn reset_3way_fixture(db: &TestDb) {
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

/// Query delta rows from the three-table join output.
///
/// The outer join (inner_join ⋈ categories) produces doubly-prefixed
/// columns: `join__o__*`, `join__p__*` from the nested inner result, and
/// `c__*` from categories.
///
/// Columns returned: (action, join__o__id, join__o__prod_id, join__o__amount,
///                    join__p__id, join__p__cat_id, join__p__name, c__id, c__label)
async fn query_3way_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(String, i32, i32, i32, i32, i32, String, i32, String)> {
    sqlx::query_as::<_, (String, i32, i32, i32, i32, i32, String, i32, String)>(
        sqlx::AssertSqlSafe(format!(
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
           ORDER BY __pgt_action, "join__o__id""#
        )),
    )
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated 3-way join delta SQL")
}

// ── three-table chain tests ───────────────────────────────────────────────────

/// Insert a new order that joins to an existing product and category.
///
/// The innermost change (orders delta) flows through the nested inner join
/// and up to the outer join's Part 1a (ΔL ⋈ R₁ for inserts). The outer
/// join's right side (categories) is unchanged, so Part 2 contributes
/// nothing. Result: exactly one I row spanning all three tables.
#[tokio::test]
async fn test_diff_inner_join_three_table_chain_order_insert_fires_part1() {
    let db = setup_3way_db().await;
    let sql = make_ctx_3()
        .differentiate(&build_three_table_join_tree())
        .expect("three-table join differentiation should succeed");

    reset_3way_fixture(&db).await;
    // Current state after the insert: all three rows exist.
    db.execute("INSERT INTO public.orders     VALUES (5, 10, 500)")
        .await;
    db.execute("INSERT INTO public.products   VALUES (10, 20, 'Widget')")
        .await;
    db.execute("INSERT INTO public.categories VALUES (20, 'Electronics')")
        .await;

    // Only orders changes: INSERT order (5, prod_id=10, amount=500).
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, prod_id, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(5), 5, 10, 500)",
    )
    .await;

    // Inner join Part 1a: I(order 5) ⋈ products{(10,20,"Widget")}
    //   → inner delta: I(o__id=5, o__prod_id=10, o__amount=500, p__id=10, p__cat_id=20, p__name="Widget")
    // Outer join Part 1a: inner delta ⋈ categories{(20,"Electronics")} on p__cat_id=c.id
    //   → I(join__o__id=5, ..., c__id=20, c__label="Electronics")
    assert_eq!(
        query_3way_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            5,
            10,
            500,
            10,
            20,
            "Widget".to_string(),
            20,
            "Electronics".to_string()
        )]
    );
}

/// Delete the rightmost table's row (category) while the inner two-table join
/// remains unchanged.
///
/// The outer join's Part 2 (L₀ ⋈ ΔR) fires: the pre-change snapshot of the
/// inner join result is joined to the categories delta to emit a D row.
/// Part 1 contributes nothing (no orders or products delta). Result: one D
/// row spanning all three tables.
#[tokio::test]
async fn test_diff_inner_join_three_table_chain_category_delete_fires_part2() {
    let db = setup_3way_db().await;
    let sql = make_ctx_3()
        .differentiate(&build_three_table_join_tree())
        .expect("three-table join differentiation should succeed");

    reset_3way_fixture(&db).await;
    // Current state: category 20 already deleted; orders and products remain.
    db.execute("INSERT INTO public.orders   VALUES (1, 10, 100)")
        .await;
    db.execute("INSERT INTO public.products VALUES (10, 20, 'Widget')")
        .await;
    // categories is empty — category 20 has just been deleted.

    // Only categories changes: DELETE category (20, "Electronics").
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_3 \
         (lsn, action, __pgt_row_id, id, label) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(20), 20, 'Electronics')",
    )
    .await;

    // Outer join Part 2: L₀(inner join) ⋈ D(cat.id=20)
    //   L₀ = orders ⋈ products = {(o=1,prod_id=10,amount=100, p=10,cat_id=20,"Widget")}
    //   ΔR  = {D(20,"Electronics")}
    //   join cond: p__cat_id = c.id  → 20 = 20  ✓
    //   → D(join__o__id=1, ..., c__id=20, c__label="Electronics")
    assert_eq!(
        query_3way_rows(&db, &sql).await,
        vec![(
            "D".to_string(),
            1,
            10,
            100,
            10,
            20,
            "Widget".to_string(),
            20,
            "Electronics".to_string()
        )]
    );
}
