//! Execution-backed tests for left-join (outer-join) DVM SQL.
//!
//! These tests run the generated delta SQL against a standalone PostgreSQL
//! container to validate result rows for the outer-join operator, including
//! the NULL-padding anti-join paths and EC-01 concurrent-delete fix.
//!
//! Schema: orders (id, prod_id, amount) LEFT JOIN products (id, name)
//! on orders.prod_id = products.id.
//!
//! Delta output columns: o__id, o__prod_id, o__amount, p__id (nullable),
//! p__name (nullable).

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

    let mut new_frontier = Frontier::new();
    new_frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());
    new_frontier.set_source(2, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    DiffContext::new_standalone(prev_frontier, new_frontier)
}

fn build_left_join_tree() -> OpTree {
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

    OpTree::LeftJoin {
        condition: eq_cond("o", "prod_id", "p", "id"),
        left: Box::new(left),
        right: Box::new(right),
    }
}

// ── DB setup ─────────────────────────────────────────────────────────────────

async fn setup_lj_db() -> TestDb {
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
    id   INT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id   BIGSERIAL PRIMARY KEY,
    lsn         PG_LSN NOT NULL,
    action      CHAR(1) NOT NULL,
    pk_hash     BIGINT,
    new_id      INT,
    new_prod_id INT,
    new_amount  INT,
    old_id      INT,
    old_prod_id INT,
    old_amount  INT
);

CREATE TABLE pgtrickle_changes.changes_2 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn       PG_LSN NOT NULL,
    action    CHAR(1) NOT NULL,
    pk_hash   BIGINT,
    new_id    INT,
    new_name  TEXT,
    old_id    INT,
    old_name  TEXT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up outer-join execution database");

    db
}

async fn reset_lj_fixture(db: &TestDb) {
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

/// Query delta rows: (action, o.id, o.prod_id, o.amount, p.id?, p.name?)
/// ordered by action then o.id.
async fn query_lj_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(String, i32, i32, i32, Option<i32>, Option<String>)> {
    sqlx::query_as::<_, (String, i32, i32, i32, Option<i32>, Option<String>)>(sqlx::AssertSqlSafe(
        format!(
            r#"SELECT __pgt_action,
                  "o__id",
                  "o__prod_id",
                  "o__amount",
                  "p__id",
                  "p__name"
           FROM ({sql}) delta
           ORDER BY __pgt_action, "o__id""#
        ),
    ))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated left-join delta SQL")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Left insert with a matching right row.
/// Part 1a fires: I(order, product) — same as inner join.
#[tokio::test]
async fn test_diff_left_join_executes_left_insert_with_matching_right() {
    let db = setup_lj_db().await;
    let sql = make_ctx()
        .differentiate(&build_left_join_tree())
        .expect("left-join differentiation should succeed");

    reset_lj_fixture(&db).await;
    db.execute("INSERT INTO public.orders   VALUES (1, 10, 100), (2, 10, 200), (3, 10, 300)")
        .await;
    db.execute("INSERT INTO public.products VALUES (10, 'Widget')")
        .await;

    // INSERT order (3, 10, 300)
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_prod_id, new_amount) \
         VALUES ('0/1', 'I', 3, 3, 10, 300)",
    )
    .await;

    assert_eq!(
        query_lj_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            3,
            10,
            300,
            Some(10),
            Some("Widget".to_string())
        )]
    );
}

/// Left insert with NO matching right row.
/// Part 3a (anti-join INSERTs) fires: I row with NULL right columns.
#[tokio::test]
async fn test_diff_left_join_executes_left_insert_with_no_matching_right() {
    let db = setup_lj_db().await;
    let sql = make_ctx()
        .differentiate(&build_left_join_tree())
        .expect("left-join differentiation should succeed");

    reset_lj_fixture(&db).await;
    // order references prod_id=99, which has no product row
    db.execute("INSERT INTO public.orders VALUES (3, 99, 300)")
        .await;
    // no products at all

    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_prod_id, new_amount) \
         VALUES ('0/1', 'I', 3, 3, 99, 300)",
    )
    .await;

    // NULL-padded row: right columns are NULL
    assert_eq!(
        query_lj_rows(&db, &sql).await,
        vec![("I".to_string(), 3, 99, 300, None, None)]
    );
}

/// Left delete that was matched: the order had a product partner.
/// Part 1b (ΔL_deletes ⋈ R₀) emits D with the product data.
#[tokio::test]
async fn test_diff_left_join_executes_left_delete_matched_row() {
    let db = setup_lj_db().await;
    let sql = make_ctx()
        .differentiate(&build_left_join_tree())
        .expect("left-join differentiation should succeed");

    reset_lj_fixture(&db).await;
    // current state: order 3 already deleted, product 10 still exists
    db.execute("INSERT INTO public.orders   VALUES (1, 10, 100), (2, 10, 200)")
        .await;
    db.execute("INSERT INTO public.products VALUES (10, 'Widget')")
        .await;

    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_prod_id, old_amount) \
         VALUES ('0/1', 'D', 3, 3, 10, 300)",
    )
    .await;

    assert_eq!(
        query_lj_rows(&db, &sql).await,
        vec![(
            "D".to_string(),
            3,
            10,
            300,
            Some(10),
            Some("Widget".to_string())
        )]
    );
}

/// Left delete that was unmatched (had NULL right columns).
/// Part 3b (anti-join DELETEs) fires: D row with NULL right.
#[tokio::test]
async fn test_diff_left_join_executes_left_delete_unmatched_row() {
    let db = setup_lj_db().await;
    let sql = make_ctx()
        .differentiate(&build_left_join_tree())
        .expect("left-join differentiation should succeed");

    reset_lj_fixture(&db).await;
    // current state: order 3 already deleted (was prod_id=99, no product)
    db.execute("INSERT INTO public.orders VALUES (1, 10, 100)")
        .await;
    db.execute("INSERT INTO public.products VALUES (10, 'Widget')")
        .await;

    // DELETE order (3, 99, 300): was NULL-padded
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_prod_id, old_amount) \
         VALUES ('0/1', 'D', 3, 3, 99, 300)",
    )
    .await;

    // Part 3b: D with NULL right columns
    assert_eq!(
        query_lj_rows(&db, &sql).await,
        vec![("D".to_string(), 3, 99, 300, None, None)]
    );
}

/// Right insert creates the first match for an existing unmatched left row.
/// Part 4 emits D (removes the stale NULL-padded row) and Part 2 emits I
/// (adds the newly matched row).
#[tokio::test]
async fn test_diff_left_join_right_insert_gains_first_match() {
    let db = setup_lj_db().await;
    let sql = make_ctx()
        .differentiate(&build_left_join_tree())
        .expect("left-join differentiation should succeed");

    reset_lj_fixture(&db).await;
    // Initial state: order 1 (prod_id=20) was NULL-padded (no product 20).
    // New state: product 20 exists after the insert.
    db.execute("INSERT INTO public.orders   VALUES (1, 20, 100)")
        .await;
    db.execute("INSERT INTO public.products VALUES (20, 'NewProd')")
        .await;

    // INSERT product (20, "NewProd")
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, pk_hash, new_id, new_name) \
         VALUES ('0/1', 'I', 20, 20, 'NewProd')",
    )
    .await;

    let rows = query_lj_rows(&db, &sql).await;

    // Part 4: D(1, 20, 100, NULL, NULL) — removes stale NULL-padded ST row
    // Part 2: I(1, 20, 100, 20, "NewProd") — newly matched row
    let d_rows: Vec<_> = rows.iter().filter(|(a, ..)| a == "D").collect();
    let i_rows: Vec<_> = rows.iter().filter(|(a, ..)| a == "I").collect();

    assert_eq!(
        d_rows,
        vec![&("D".to_string(), 1, 20, 100, None, None)],
        "Part 4 should emit D(null-padded)"
    );
    assert_eq!(
        i_rows,
        vec![&(
            "I".to_string(),
            1,
            20,
            100,
            Some(20),
            Some("NewProd".to_string())
        )],
        "Part 2 should emit I(matched)"
    );
}

/// Right delete removes the last match for a left row.
/// Part 2 emits D (matched row gone) and Part 5 emits I (NULL-padded row
/// replaces it because the left row now has no right partner).
#[tokio::test]
async fn test_diff_left_join_right_delete_loses_last_match() {
    let db = setup_lj_db().await;
    let sql = make_ctx()
        .differentiate(&build_left_join_tree())
        .expect("left-join differentiation should succeed");

    reset_lj_fixture(&db).await;
    // Current state: product 10 deleted; order 1 now has no right partner.
    db.execute("INSERT INTO public.orders VALUES (1, 10, 100)")
        .await;
    // products table is empty after delete

    // DELETE product (10, "Widget")
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, pk_hash, old_id, old_name) \
         VALUES ('0/1', 'D', 10, 10, 'Widget')",
    )
    .await;

    let rows = query_lj_rows(&db, &sql).await;

    let d_rows: Vec<_> = rows.iter().filter(|(a, ..)| a == "D").collect();
    let i_rows: Vec<_> = rows.iter().filter(|(a, ..)| a == "I").collect();

    // Part 2: D(1, 10, 100, 10, "Widget") — matched row removed from output
    assert_eq!(
        d_rows,
        vec![&(
            "D".to_string(),
            1,
            10,
            100,
            Some(10),
            Some("Widget".to_string())
        )],
        "Part 2 should emit D(matched row)"
    );
    // Part 5: I(1, 10, 100, NULL, NULL) — NULL-padded row replaces it
    assert_eq!(
        i_rows,
        vec![&("I".to_string(), 1, 10, 100, None, None)],
        "Part 5 should emit I(null-padded)"
    );
}

/// EC-01 regression for outer join: left DELETE with a concurrent right DELETE.
/// Without the R₀ fix, Part 1 (delta_left ⋈ R₁) misses the match (right row
/// gone), Part 3 incorrectly emits D(NULL-padded), and the original
/// D(matched) row is silently lost.
/// With R₀, Part 1b correctly joins the deleted order against the pre-change
/// right snapshot and emits the matched D row.
#[tokio::test]
async fn test_diff_left_join_ec01_left_delete_with_concurrent_right_delete() {
    let db = setup_lj_db().await;
    let sql = make_ctx()
        .differentiate(&build_left_join_tree())
        .expect("left-join differentiation should succeed");

    reset_lj_fixture(&db).await;
    // current state: both order 1 and product 10 have been deleted
    db.execute("INSERT INTO public.orders   VALUES (2, 20, 200)")
        .await;
    db.execute("INSERT INTO public.products VALUES (20, 'Other')")
        .await;

    // Simultaneous: DELETE order (1, 10, 100) and DELETE product (10, "Widget")
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_prod_id, old_amount) \
         VALUES ('0/1', 'D', 1, 1, 10, 100)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, pk_hash, old_id, old_name) \
         VALUES ('0/2', 'D', 10, 10, 'Widget')",
    )
    .await;

    let rows = query_lj_rows(&db, &sql).await;

    // The critical assertion: we must get at least one D row for order 1.
    // Part 1b (ΔL_deletes ⋈ R₀) should find the match even though product 10
    // is gone from R₁.  A D(NULL-padded) row from Part 3b is also acceptable
    // as long as the matched D row is present.
    let d_order1: Vec<_> = rows
        .iter()
        .filter(|(action, oid, ..)| action == "D" && *oid == 1)
        .collect();

    assert!(
        !d_order1.is_empty(),
        "EC-01 regression: D row for order 1 is missing; rows={rows:?}"
    );

    // Verify the matched D row appears (not just the NULL-padded one).
    let matched_d: Vec<_> = d_order1
        .iter()
        .filter(|(_, _, _, _, pid, _)| pid.is_some())
        .collect();

    assert!(
        !matched_d.is_empty(),
        "EC-01 regression: matched D(order 1) missing — only NULL-padded D appears; \
         rows={rows:?}"
    );
}
