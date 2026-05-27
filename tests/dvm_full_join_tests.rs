//! Execution-backed tests for full-join (FULL OUTER JOIN) DVM SQL.
//!
//! These tests run the generated delta SQL against a standalone PostgreSQL
//! container to validate result rows for the full-join operator, covering both
//! the left-side paths (Parts 1-5, mirroring outer_join) and the symmetric
//! right-side paths (Parts 6-7) that are unique to FULL OUTER JOIN.
//!
//! Schema: orders (id, prod_id, amount) FULL JOIN products (id, name)
//! on orders.prod_id = products.id.
//!
//! Delta output columns: o__id (nullable), o__prod_id (nullable),
//! o__amount (nullable), p__id (nullable), p__name (nullable).
//! Both sides are nullable because either side can be NULL-padded.

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

fn build_full_join_tree() -> OpTree {
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

    OpTree::FullJoin {
        condition: eq_cond("o", "prod_id", "p", "id"),
        left: Box::new(left),
        right: Box::new(right),
    }
}

// ── DB setup ─────────────────────────────────────────────────────────────────

async fn setup_fj_db() -> TestDb {
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
    .expect("failed to set up full-join execution database");

    db
}

async fn reset_fj_fixture(db: &TestDb) {
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

/// Query delta rows: (action, o.id?, o.prod_id?, o.amount?, p.id?, p.name?)
/// Both left and right columns are nullable because either side can be NULL-padded
/// in a FULL OUTER JOIN.
/// Ordered by action then o.id NULLS LAST then p.id NULLS LAST.
async fn query_fj_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(
    String,
    Option<i32>,
    Option<i32>,
    Option<i32>,
    Option<i32>,
    Option<String>,
)> {
    sqlx::query_as::<
        _,
        (
            String,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<String>,
        ),
    >(sqlx::AssertSqlSafe(format!(
        r#"SELECT __pgt_action,
                  "o__id",
                  "o__prod_id",
                  "o__amount",
                  "p__id",
                  "p__name"
           FROM ({sql}) delta
           ORDER BY __pgt_action, "o__id" NULLS LAST, "p__id" NULLS LAST"#
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated full-join delta SQL")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Left insert with a matching right row.
/// Part 1a fires: I with both left and right columns populated.
/// (Same behaviour as LEFT JOIN / INNER JOIN for this case.)
#[tokio::test]
async fn test_diff_full_join_executes_left_insert_with_matching_right() {
    let db = setup_fj_db().await;
    let sql = make_ctx()
        .differentiate(&build_full_join_tree())
        .expect("full-join differentiation should succeed");

    reset_fj_fixture(&db).await;
    db.execute("INSERT INTO public.orders   VALUES (1, 10, 100), (2, 10, 200)")
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
        query_fj_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            Some(3),
            Some(10),
            Some(300),
            Some(10),
            Some("Widget".to_string())
        )]
    );
}

/// Left insert with NO matching right row.
/// Part 3a (anti-join INSERTs) fires: I row with NULL right columns.
/// (Identical to LEFT JOIN behaviour.)
#[tokio::test]
async fn test_diff_full_join_executes_left_insert_with_no_matching_right() {
    let db = setup_fj_db().await;
    let sql = make_ctx()
        .differentiate(&build_full_join_tree())
        .expect("full-join differentiation should succeed");

    reset_fj_fixture(&db).await;
    db.execute("INSERT INTO public.orders VALUES (3, 99, 300)")
        .await;
    // no products at all

    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_prod_id, new_amount) \
         VALUES ('0/1', 'I', 3, 3, 99, 300)",
    )
    .await;

    assert_eq!(
        query_fj_rows(&db, &sql).await,
        vec![("I".to_string(), Some(3), Some(99), Some(300), None, None)]
    );
}

/// Right insert with NO matching left row.
/// Part 6 (right anti-join) fires: I row with NULL left columns.
/// This path is UNIQUE TO FULL OUTER JOIN — LEFT JOIN has no Part 6.
#[tokio::test]
async fn test_diff_full_join_executes_right_insert_with_no_matching_left() {
    let db = setup_fj_db().await;
    let sql = make_ctx()
        .differentiate(&build_full_join_tree())
        .expect("full-join differentiation should succeed");

    reset_fj_fixture(&db).await;
    // orders exist but none reference prod_id=50
    db.execute("INSERT INTO public.orders VALUES (1, 10, 100)")
        .await;
    db.execute("INSERT INTO public.products VALUES (50, 'Orphan')")
        .await;

    // INSERT product (50, "Orphan") — no orders reference prod_id=50
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, pk_hash, new_id, new_name) \
         VALUES ('0/1', 'I', 50, 50, 'Orphan')",
    )
    .await;

    // Part 6: I with NULL left columns
    assert_eq!(
        query_fj_rows(&db, &sql).await,
        vec![(
            "I".to_string(),
            None,
            None,
            None,
            Some(50),
            Some("Orphan".to_string())
        )]
    );
}

/// Left delete that was matched: the order had a product partner.
/// Part 1b (ΔL_deletes ⋈ R₀) emits D with both sides populated.
#[tokio::test]
async fn test_diff_full_join_executes_left_delete_matched_row() {
    let db = setup_fj_db().await;
    let sql = make_ctx()
        .differentiate(&build_full_join_tree())
        .expect("full-join differentiation should succeed");

    reset_fj_fixture(&db).await;
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
        query_fj_rows(&db, &sql).await,
        vec![(
            "D".to_string(),
            Some(3),
            Some(10),
            Some(300),
            Some(10),
            Some("Widget".to_string())
        )]
    );
}

/// Right delete with NO matching left row.
/// Part 6 fires: D row with NULL left columns.
/// This path is UNIQUE TO FULL OUTER JOIN.
#[tokio::test]
async fn test_diff_full_join_executes_right_delete_unmatched_row() {
    let db = setup_fj_db().await;
    let sql = make_ctx()
        .differentiate(&build_full_join_tree())
        .expect("full-join differentiation should succeed");

    reset_fj_fixture(&db).await;
    // current state: product 50 already deleted; no orders reference prod_id=50
    db.execute("INSERT INTO public.orders VALUES (1, 10, 100)")
        .await;
    // products table is empty (product 50 was deleted)

    // DELETE product (50, "Orphan"): no left partner
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, pk_hash, old_id, old_name) \
         VALUES ('0/1', 'D', 50, 50, 'Orphan')",
    )
    .await;

    // Part 6: D with NULL left columns
    assert_eq!(
        query_fj_rows(&db, &sql).await,
        vec![(
            "D".to_string(),
            None,
            None,
            None,
            Some(50),
            Some("Orphan".to_string())
        )]
    );
}

/// Left insert that creates the first left match for an existing unmatched right row.
/// Part 1a emits I(left, right) — the newly matched row.
/// Part 7a emits D(NULL_left, right) — removes the stale NULL-padded right row.
/// This "right row gains its first left partner" path is UNIQUE TO FULL OUTER JOIN.
#[tokio::test]
async fn test_diff_full_join_left_insert_removes_null_padded_right() {
    let db = setup_fj_db().await;
    let sql = make_ctx()
        .differentiate(&build_full_join_tree())
        .expect("full-join differentiation should succeed");

    reset_fj_fixture(&db).await;
    // Product 30 existed but had no matching orders (NULL-padded right row in view).
    // Now order 5 (prod_id=30) is inserted — gives product 30 its first left partner.
    db.execute("INSERT INTO public.products VALUES (30, 'P30')")
        .await;
    db.execute("INSERT INTO public.orders VALUES (5, 30, 500)")
        .await;

    // INSERT order (5, 30, 500)
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_prod_id, new_amount) \
         VALUES ('0/1', 'I', 5, 5, 30, 500)",
    )
    .await;

    let rows = query_fj_rows(&db, &sql).await;

    // Part 7a: D(NULL_left, 30, "P30") — removes stale NULL-padded right row
    let d_rows: Vec<_> = rows.iter().filter(|(a, ..)| a == "D").collect();
    // Part 1a: I(5, 30, 500, 30, "P30") — newly matched row
    let i_rows: Vec<_> = rows.iter().filter(|(a, ..)| a == "I").collect();

    assert_eq!(
        d_rows,
        vec![&(
            "D".to_string(),
            None,
            None,
            None,
            Some(30),
            Some("P30".to_string())
        )],
        "Part 7a should emit D(null-padded right row)"
    );
    assert_eq!(
        i_rows,
        vec![&(
            "I".to_string(),
            Some(5),
            Some(30),
            Some(500),
            Some(30),
            Some("P30".to_string())
        )],
        "Part 1a should emit I(matched row)"
    );
}

/// Left delete that removes the last left match for a right row.
/// Part 1b emits D(left, right) — removes the matched row.
/// Part 7b emits I(NULL_left, right) — re-inserts the NULL-padded right row.
/// This "right row loses its last left partner" path is UNIQUE TO FULL OUTER JOIN.
#[tokio::test]
async fn test_diff_full_join_left_delete_restores_null_padded_right() {
    let db = setup_fj_db().await;
    let sql = make_ctx()
        .differentiate(&build_full_join_tree())
        .expect("full-join differentiation should succeed");

    reset_fj_fixture(&db).await;
    // Order 5 was the ONLY left row matching product 30. It has just been deleted.
    // After the delete, product 30 has no left partners → NULL-padded right row re-appears.
    db.execute("INSERT INTO public.products VALUES (30, 'P30')")
        .await;
    // orders is empty: order 5 already deleted

    // DELETE order (5, 30, 500) — was the sole left match for product 30
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_prod_id, old_amount) \
         VALUES ('0/1', 'D', 5, 5, 30, 500)",
    )
    .await;

    let rows = query_fj_rows(&db, &sql).await;

    // Part 1b: D(5, 30, 500, 30, "P30") — removes the matched row
    let d_rows: Vec<_> = rows.iter().filter(|(a, ..)| a == "D").collect();
    // Part 7b: I(NULL_left, 30, "P30") — re-inserts NULL-padded right row (now unmatched)
    let i_rows: Vec<_> = rows.iter().filter(|(a, ..)| a == "I").collect();

    assert_eq!(
        d_rows,
        vec![&(
            "D".to_string(),
            Some(5),
            Some(30),
            Some(500),
            Some(30),
            Some("P30".to_string())
        )],
        "Part 1b should emit D(matched row)"
    );
    assert_eq!(
        i_rows,
        vec![&(
            "I".to_string(),
            None,
            None,
            None,
            Some(30),
            Some("P30".to_string())
        )],
        "Part 7b should emit I(null-padded right row)"
    );
}

/// EC-01 regression: left DELETE with a concurrent right DELETE.
///
/// When order 5 (prod_id=30) and product 30 are simultaneously deleted,
/// Part 1b must use R₀ (pre-change right snapshot) rather than R₁
/// (post-change) to find the match. Without EC-01 the left DELETE would
/// silently lose its right partner and produce a wrong NULL-padded D row.
///
/// Asserts that at least one D row with non-NULL right columns appears
/// in the output (verifying R₀ correctly located the pre-change right row).
#[tokio::test]
async fn test_diff_full_join_ec01_concurrent_left_delete_right_delete() {
    let db = setup_fj_db().await;
    let sql = make_ctx()
        .differentiate(&build_full_join_tree())
        .expect("full-join differentiation should succeed");

    reset_fj_fixture(&db).await;
    // Both order 5 and product 30 have already been deleted from the tables.
    // The change buffers record both deletes in the same batch.
    // orders and products are both empty.

    // DELETE order (5, 30, 500)
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_prod_id, old_amount) \
         VALUES ('0/1', 'D', 5, 5, 30, 500)",
    )
    .await;
    // DELETE product (30, "P30")
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_2 \
         (lsn, action, pk_hash, old_id, old_name) \
         VALUES ('0/1', 'D', 30, 30, 'P30')",
    )
    .await;

    let rows = query_fj_rows(&db, &sql).await;

    // EC-01 fix: Part 1b (D using R₀) must emit a matched D row with p__id = Some(30).
    // Without the fix, this would produce a NULL-padded D row from Part 3b instead.
    let matched_d = rows
        .iter()
        .any(|(action, _, _, _, p_id, _)| action == "D" && p_id.is_some());

    assert!(
        matched_d,
        "EC-01: expected at least one D row with non-NULL p__id (matched via R₀), got: {rows:?}"
    );
}
