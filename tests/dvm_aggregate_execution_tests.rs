//! Execution-backed tests for aggregate DVM SQL.
//!
//! These tests run generated aggregate delta SQL against a standalone
//! PostgreSQL container so we can validate result rows for representative
//! algebraic and rescan aggregate families.

mod common;

use common::TestDb;
use pg_trickle::dvm::DiffContext;
use pg_trickle::dvm::parser::{AggExpr, AggFunc, Column, Expr, OpTree, SortExpr};
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

fn lit(value: &str) -> Expr {
    Expr::Literal(value.to_string())
}

fn binop(op: &str, left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: op.to_string(),
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn scan_orders() -> OpTree {
    OpTree::Scan {
        table_oid: 1,
        table_name: "orders".to_string(),
        schema: "public".to_string(),
        columns: vec![
            int_col("id"),
            text_col("region"),
            int_col("amount"),
            text_col("label"),
        ],
        pk_columns: vec!["id".to_string()],
        alias: "o".to_string(),
    }
}

fn count_star(alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::CountStar,
        argument: None,
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
    }
}

fn sum_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Sum,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
    }
}

fn avg_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Avg,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
    }
}

fn min_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Min,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
    }
}

fn max_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Max,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
    }
}

fn string_agg_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::StringAgg,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: Some(lit("', '")),
        filter: None,
        order_within_group: Some(vec![sort_asc("amount")]),
    }
}

fn mode_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Mode,
        argument: None,
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: Some(vec![sort_asc(column)]),
    }
}

fn json_object_agg_col(key_col: &str, val_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::JsonObjectAgg,
        argument: Some(colref(key_col)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: Some(colref(val_col)),
        filter: None,
        order_within_group: None,
    }
}

fn jsonb_object_agg_col(key_col: &str, val_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::JsonbObjectAgg,
        argument: Some(colref(key_col)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: Some(colref(val_col)),
        filter: None,
        order_within_group: None,
    }
}

fn percentile_cont_col(fraction: &str, order_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::PercentileCont,
        argument: Some(lit(fraction)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: Some(vec![sort_asc(order_col)]),
    }
}

fn percentile_disc_col(fraction: &str, order_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::PercentileDisc,
        argument: Some(lit(fraction)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: Some(vec![sort_asc(order_col)]),
    }
}

fn filtered_count_col(column: &str, alias: &str, filter: Expr) -> AggExpr {
    AggExpr {
        function: AggFunc::Count,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: Some(filter),
        order_within_group: None,
    }
}

fn grouped_aggregate(aggregate: AggExpr) -> OpTree {
    OpTree::Aggregate {
        group_by: vec![colref("region")],
        aggregates: vec![aggregate],
        child: Box::new(scan_orders()),
    }
}

fn build_multi_group_mixed_family_tree() -> OpTree {
    OpTree::Aggregate {
        group_by: vec![colref("region"), colref("label")],
        aggregates: vec![
            count_star("cnt"),
            sum_col("amount", "total_amt"),
            max_col("amount", "max_amt"),
        ],
        child: Box::new(scan_orders()),
    }
}

async fn query_multi_agg_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(String, String, String, i64, i64, i32)> {
    sqlx::query_as::<_, (String, String, String, i64, i64, i32)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, label, cnt, total_amt, max_amt \
         FROM ({sql}) delta ORDER BY __pgt_action DESC, region, label"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute multi-group aggregate delta SQL")
}

fn make_aggregate_ctx(st_name: &str, st_user_columns: &[&str]) -> DiffContext {
    let mut prev_frontier = Frontier::new();
    prev_frontier.set_source(1, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());

    let mut new_frontier = Frontier::new();
    new_frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    let mut ctx = DiffContext::new_standalone(prev_frontier, new_frontier)
        .with_pgt_name("public", st_name)
        .with_defining_query("SELECT region, amount, label FROM public.orders");
    ctx.st_user_columns = Some(st_user_columns.iter().map(|c| (*c).to_string()).collect());
    ctx.st_has_pgt_count = true;
    ctx
}

async fn setup_aggregate_db() -> TestDb {
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
    amount INT NOT NULL,
    label TEXT NOT NULL DEFAULT ''
);

CREATE TABLE public.agg_count_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    order_count BIGINT NOT NULL
);

CREATE TABLE public.agg_sum_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    total_amount BIGINT NOT NULL
);

CREATE TABLE public.agg_avg_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    avg_amount NUMERIC NOT NULL
);

CREATE TABLE public.agg_min_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    min_amount INT NOT NULL
);

CREATE TABLE public.agg_max_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    max_amount INT NOT NULL
);

CREATE TABLE public.agg_string_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    member_labels TEXT NOT NULL
);

CREATE TABLE public.agg_mode_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    mode_amount INT NOT NULL
);

CREATE TABLE public.agg_json_object_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    amount_map JSON NOT NULL
);

CREATE TABLE public.agg_jsonb_object_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    amount_map JSONB NOT NULL
);

CREATE TABLE public.agg_percentile_cont_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    median_amount NUMERIC NOT NULL
);

CREATE TABLE public.agg_percentile_disc_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    median_disc_amount INT NOT NULL
);

CREATE TABLE public.agg_filtered_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    high_value_count BIGINT NOT NULL
);

CREATE TABLE public.agg_jsonb_arr_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    amount_arr JSONB NOT NULL
);

CREATE TABLE public.agg_multi_st (
    __pgt_row_id BIGINT PRIMARY KEY,
    region TEXT NOT NULL,
    label TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    cnt BIGINT NOT NULL,
    total_amt BIGINT NOT NULL,
    max_amt INT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn PG_LSN NOT NULL,
    action CHAR(1) NOT NULL,
    pk_hash BIGINT,
    new_id INT,
    new_region TEXT,
    new_amount INT,
    new_label TEXT,
    old_id INT,
    old_region TEXT,
    old_amount INT,
    old_label TEXT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up aggregate execution database");

    db
}

async fn reset_aggregate_fixture(db: &TestDb) {
    db.execute(
        "TRUNCATE TABLE \
         pgtrickle_changes.changes_1, \
         public.orders, \
         public.agg_count_st, \
         public.agg_sum_st, \
         public.agg_avg_st, \
         public.agg_min_st, \
         public.agg_max_st, \
         public.agg_string_st, \
         public.agg_mode_st, \
         public.agg_json_object_st, \
         public.agg_jsonb_object_st, \
         public.agg_percentile_cont_st, \
         public.agg_percentile_disc_st, \
         public.agg_filtered_st, \
         public.agg_jsonb_arr_st, \
         public.agg_multi_st \
         RESTART IDENTITY",
    )
    .await;
}

async fn query_bigint_aggregate_rows(
    db: &TestDb,
    sql: &str,
    aggregate_column: &str,
) -> Vec<(String, String, i64, i64)> {
    sqlx::query_as::<_, (String, String, i64, i64)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, __pgt_count, {aggregate_column} \
         FROM ({sql}) delta ORDER BY __pgt_action, region"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated aggregate delta SQL")
}

async fn query_numeric_aggregate_rows(
    db: &TestDb,
    sql: &str,
    aggregate_column: &str,
) -> Vec<(String, String, i64, String)> {
    sqlx::query_as::<_, (String, String, i64, String)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, __pgt_count, ({aggregate_column})::numeric(10,2)::text \
         FROM ({sql}) delta ORDER BY __pgt_action, region"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated aggregate delta SQL")
}

async fn query_text_aggregate_rows(
    db: &TestDb,
    sql: &str,
    aggregate_column: &str,
) -> Vec<(String, String, i64, String)> {
    sqlx::query_as::<_, (String, String, i64, String)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, __pgt_count, ({aggregate_column})::text \
         FROM ({sql}) delta ORDER BY __pgt_action, region"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated aggregate delta SQL")
}

async fn query_json_aggregate_rows(
    db: &TestDb,
    sql: &str,
    aggregate_column: &str,
) -> Vec<(String, String, i64, String)> {
    sqlx::query_as::<_, (String, String, i64, String)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, __pgt_count, (({aggregate_column})::jsonb)::text \
         FROM ({sql}) delta ORDER BY __pgt_action, region"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated aggregate delta SQL")
}

#[tokio::test]
async fn test_diff_aggregate_executes_count_star_gain_and_loss() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_count_st", &["region", "order_count"])
        .differentiate(&grouped_aggregate(count_star("order_count")))
        .expect("count aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (2, 'east', 20), \
         (4, 'east', 40)",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_count_st VALUES \
         (100, 'east', 2, 2), \
         (200, 'west', 1, 1)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount) \
         VALUES ('0/1', 'I', 4, 4, 'east', 40)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_region, old_amount) \
         VALUES ('0/2', 'D', 3, 3, 'west', 30)",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "order_count").await,
        vec![
            ("D".to_string(), "west".to_string(), 1, 1),
            ("I".to_string(), "east".to_string(), 3, 3),
        ]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_sum_update_with_balanced_delta() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_sum_st", &["region", "total_amount"])
        .differentiate(&grouped_aggregate(sum_col("amount", "total_amount")))
        .expect("sum aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (3, 'east', 15)",
    )
    .await;
    db.execute("INSERT INTO public.agg_sum_st VALUES (100, 'east', 2, 30)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount) \
         VALUES ('0/1', 'I', 3, 3, 'east', 15)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_region, old_amount) \
         VALUES ('0/2', 'D', 2, 2, 'east', 20)",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "total_amount").await,
        vec![("I".to_string(), "east".to_string(), 2, 25)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_avg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_avg_st", &["region", "avg_amount"])
        .differentiate(&grouped_aggregate(avg_col("amount", "avg_amount")))
        .expect("avg aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (2, 'east', 20), \
         (3, 'east', 30)",
    )
    .await;
    db.execute("INSERT INTO public.agg_avg_st VALUES (100, 'east', 2, 15.00)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount) \
         VALUES ('0/1', 'I', 3, 3, 'east', 30)",
    )
    .await;

    assert_eq!(
        query_numeric_aggregate_rows(&db, &sql, "avg_amount").await,
        vec![("I".to_string(), "east".to_string(), 3, "20.00".to_string())]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_filtered_count_update() {
    let db = setup_aggregate_db().await;
    let filter = binop(">=", colref("amount"), lit("20"));
    let sql = make_aggregate_ctx("agg_filtered_st", &["region", "high_value_count"])
        .differentiate(&grouped_aggregate(filtered_count_col(
            "amount",
            "high_value_count",
            filter,
        )))
        .expect("filtered aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 25), \
         (2, 'east', 20)",
    )
    .await;
    db.execute("INSERT INTO public.agg_filtered_st VALUES (100, 'east', 2, 1)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount, old_id, old_region, old_amount) \
         VALUES ('0/1', 'U', 1, 1, 'east', 25, 1, 'east', 10)",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "high_value_count").await,
        vec![("I".to_string(), "east".to_string(), 2, 2)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_min_rescan_on_deleted_extremum() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_min_st", &["region", "min_amount"])
        .differentiate(&grouped_aggregate(min_col("amount", "min_amount")))
        .expect("min aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (2, 'east', 20, 'b'), \
         (3, 'east', 30, 'c')",
    )
    .await;
    db.execute("INSERT INTO public.agg_min_st VALUES (100, 'east', 3, 10)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_region, old_amount, old_label) \
         VALUES ('0/1', 'D', 1, 1, 'east', 10, 'a')",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "(min_amount)::bigint").await,
        vec![("I".to_string(), "east".to_string(), 2, 20)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_max_rescan_on_deleted_extremum() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_max_st", &["region", "max_amount"])
        .differentiate(&grouped_aggregate(max_col("amount", "max_amount")))
        .expect("max aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b')",
    )
    .await;
    db.execute("INSERT INTO public.agg_max_st VALUES (100, 'east', 3, 30)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, old_id, old_region, old_amount, old_label) \
         VALUES ('0/1', 'D', 3, 3, 'east', 30, 'c')",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "(max_amount)::bigint").await,
        vec![("I".to_string(), "east".to_string(), 2, 20)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_string_agg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_string_st", &["region", "member_labels"])
        .differentiate(&grouped_aggregate(string_agg_col("label", "member_labels")))
        .expect("string_agg differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 15, 'b'), \
         (3, 'east', 20, 'c')",
    )
    .await;
    db.execute("INSERT INTO public.agg_string_st VALUES (100, 'east', 2, 'a, c')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount, new_label) \
         VALUES ('0/1', 'I', 2, 2, 'east', 15, 'b')",
    )
    .await;

    assert_eq!(
        query_text_aggregate_rows(&db, &sql, "member_labels").await,
        vec![(
            "I".to_string(),
            "east".to_string(),
            3,
            "a, b, c".to_string()
        )]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_mode_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_mode_st", &["region", "mode_amount"])
        .differentiate(&grouped_aggregate(mode_col("amount", "mode_amount")))
        .expect("mode differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 10, 'b'), \
         (3, 'east', 20, 'c'), \
         (4, 'east', 20, 'd'), \
         (5, 'east', 20, 'e')",
    )
    .await;
    db.execute("INSERT INTO public.agg_mode_st VALUES (100, 'east', 3, 10)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount, new_label) \
         VALUES \
         ('0/1', 'I', 4, 4, 'east', 20, 'd'), \
         ('0/2', 'I', 5, 5, 'east', 20, 'e')",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "(mode_amount)::bigint").await,
        vec![("I".to_string(), "east".to_string(), 5, 20)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_json_object_agg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_json_object_st", &["region", "amount_map"])
        .differentiate(&grouped_aggregate(json_object_agg_col(
            "label",
            "amount",
            "amount_map",
        )))
        .expect("json_object_agg differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 30, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_json_object_st VALUES \
         (100, 'east', 2, '{\"a\":10,\"c\":30}')",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount, new_label) \
         VALUES ('0/1', 'I', 2, 2, 'east', 20, 'b')",
    )
    .await;

    assert_eq!(
        query_json_aggregate_rows(&db, &sql, "amount_map").await,
        vec![(
            "I".to_string(),
            "east".to_string(),
            3,
            "{\"a\": 10, \"b\": 20, \"c\": 30}".to_string(),
        )]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_jsonb_object_agg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_jsonb_object_st", &["region", "amount_map"])
        .differentiate(&grouped_aggregate(jsonb_object_agg_col(
            "label",
            "amount",
            "amount_map",
        )))
        .expect("jsonb_object_agg differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 30, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_jsonb_object_st VALUES \
         (100, 'east', 2, '{\"a\":10,\"c\":30}')",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount, new_label) \
         VALUES ('0/1', 'I', 2, 2, 'east', 20, 'b')",
    )
    .await;

    assert_eq!(
        query_json_aggregate_rows(&db, &sql, "amount_map").await,
        vec![(
            "I".to_string(),
            "east".to_string(),
            3,
            "{\"a\": 10, \"b\": 20, \"c\": 30}".to_string(),
        )]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_percentile_cont_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_percentile_cont_st", &["region", "median_amount"])
        .differentiate(&grouped_aggregate(percentile_cont_col(
            "0.5",
            "amount",
            "median_amount",
        )))
        .expect("percentile_cont differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 40, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_percentile_cont_st VALUES \
         (100, 'east', 2, 25.00)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount, new_label) \
         VALUES ('0/1', 'I', 2, 2, 'east', 20, 'b')",
    )
    .await;

    assert_eq!(
        query_numeric_aggregate_rows(&db, &sql, "median_amount").await,
        vec![("I".to_string(), "east".to_string(), 3, "20.00".to_string())]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_percentile_disc_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_percentile_disc_st", &["region", "median_disc_amount"])
        .differentiate(&grouped_aggregate(percentile_disc_col(
            "0.5",
            "amount",
            "median_disc_amount",
        )))
        .expect("percentile_disc differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 40, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_percentile_disc_st VALUES \
         (100, 'east', 2, 10)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount, new_label) \
         VALUES ('0/1', 'I', 2, 2, 'east', 20, 'b')",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "(median_disc_amount)::bigint").await,
        vec![("I".to_string(), "east".to_string(), 3, 20)]
    );
}

fn jsonb_agg_col(argument: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::JsonbAgg,
        argument: Some(colref(argument)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
    }
}

#[tokio::test]
async fn test_diff_aggregate_executes_jsonb_agg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_jsonb_arr_st", &["region", "amount_arr"])
        .differentiate(&grouped_aggregate(jsonb_agg_col("amount", "amount_arr")))
        .expect("jsonb_agg differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 30, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_jsonb_arr_st VALUES \
         (100, 'east', 2, '[10, 30]')",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount, new_label) \
         VALUES ('0/1', 'I', 2, 2, 'east', 20, 'b')",
    )
    .await;

    let rows = query_json_aggregate_rows(&db, &sql, "amount_arr").await;
    assert_eq!(rows.len(), 1);
    let (action, region, count, json_str) = &rows[0];
    assert_eq!(action, "I");
    assert_eq!(region, "east");
    assert_eq!(*count, 3);
    assert!(json_str.contains("10"));
    assert!(json_str.contains("20"));
    assert!(json_str.contains("30"));
}

#[tokio::test]
async fn test_diff_aggregate_executes_multi_group_mixed_family() {
    // Tests that simultaneous changes to two different groups produce the
    // correct delta rows for each group, and that a mixed aggregate family
    // (COUNT + SUM + MAX) is computed correctly in a single rescan pass.
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx(
        "agg_multi_st",
        &["region", "label", "cnt", "total_amt", "max_amt"],
    )
    .differentiate(&build_multi_group_mixed_family_tree())
    .expect("multi-group mixed-family aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;

    // Seed base state: three groups — east-A, east-B, west-A.
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'A'), \
         (2, 'east', 20, 'B'), \
         (3, 'west', 15, 'A')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_multi_st \
         (__pgt_row_id, region, label, __pgt_count, cnt, total_amt, max_amt) VALUES \
         (1, 'east', 'A', 1, 1, 10, 10), \
         (2, 'east', 'B', 1, 1, 20, 20), \
         (3, 'west', 'A', 1, 1, 15, 15)",
    )
    .await;

    // Delta: two groups change simultaneously — east-A and west-A each gain a row.
    db.execute("INSERT INTO public.orders VALUES (4, 'east', 40, 'A'), (5, 'west', 40, 'A')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, pk_hash, new_id, new_region, new_amount, new_label) VALUES \
         ('0/1', 'I', 4, 4, 'east', 40, 'A'), \
         ('0/2', 'I', 5, 5, 'west', 40, 'A')",
    )
    .await;

    let rows = query_multi_agg_rows(&db, &sql).await;

    // Two groups changed → two upsert rows (I for each changed group).
    assert_eq!(
        rows.len(),
        2,
        "expected 2 delta rows, one per changed group"
    );

    // east-A: count 1→2, sum 10+40=50, max(10,40)=40
    assert_eq!(
        rows[0],
        (
            "I".to_string(),
            "east".to_string(),
            "A".to_string(),
            2,
            50,
            40
        )
    );
    // west-A: count 1→2, sum 15+40=55, max(15,40)=40
    assert_eq!(
        rows[1],
        (
            "I".to_string(),
            "west".to_string(),
            "A".to_string(),
            2,
            55,
            40
        )
    );
}
