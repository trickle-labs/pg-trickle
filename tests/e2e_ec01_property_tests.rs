//! EC-01 release-gate property tests.
//!
//! These tests compare DIFFERENTIAL stream tables against FULL stream tables
//! after every mutation cycle. They are intentionally small but high-churn:
//! random deletes and updates hit both sides of joins in the same cycle, which
//! is the failure mode that used to leave cross-cycle phantom rows behind.

mod e2e;

use e2e::{
    E2eDb,
    property_support::{SeededRng, assert_st_query_invariant},
};

const BASE_SEED: u64 = 0xEC01_0038_0001;
const DEFAULT_CYCLES: usize = 100;

fn ec01_cycles() -> usize {
    std::env::var("PGS_EC01_CYCLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_CYCLES)
}

fn pick_live(rng: &mut SeededRng, values: &[i32]) -> Option<i32> {
    if values.is_empty() {
        None
    } else {
        Some(values[rng.usize_range(0, values.len() - 1)])
    }
}

fn remove_live(rng: &mut SeededRng, values: &mut Vec<i32>) -> Option<i32> {
    if values.is_empty() {
        None
    } else {
        let index = rng.usize_range(0, values.len() - 1);
        Some(values.swap_remove(index))
    }
}

async fn assert_full_diff_equal(db: &E2eDb, full_st: &str, diff_st: &str, seed: u64, cycle: usize) {
    let cols: String = db
        .query_scalar(&format!(
            "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) \
             FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = '{diff_st}' \
               AND column_name NOT LIKE '__pgt_%'"
        ))
        .await;

    let matches: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS ( \
                (SELECT {cols} FROM public.{diff_st} EXCEPT ALL \
                 SELECT {cols} FROM public.{full_st}) \
                UNION ALL \
                (SELECT {cols} FROM public.{full_st} EXCEPT ALL \
                 SELECT {cols} FROM public.{diff_st}) \
            )"
        ))
        .await;

    if matches {
        return;
    }

    let full_count: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM public.{full_st}"))
        .await;
    let diff_count: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM public.{diff_st}"))
        .await;
    let action_counts: Vec<(String, i64)> = sqlx::query_as(
        "SELECT action::text, count(*)::bigint \
         FROM pgtrickle_changes.changes_ec01_orders \
         GROUP BY action::text \
         ORDER BY action::text",
    )
    .fetch_all(&db.pool)
    .await
    .unwrap_or_default();
    let extra_rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT row_to_json(x)::text FROM ( \
             SELECT __pgt_row_id, {cols} FROM public.{diff_st} \
             EXCEPT ALL \
             SELECT __pgt_row_id, {cols} FROM public.{full_st} \
         ) x LIMIT 10"
    )))
    .fetch_all(&db.pool)
    .await
    .unwrap_or_default();
    let missing_rows: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT row_to_json(x)::text FROM ( \
             SELECT __pgt_row_id, {cols} FROM public.{full_st} \
             EXCEPT ALL \
             SELECT __pgt_row_id, {cols} FROM public.{diff_st} \
         ) x LIMIT 10"
    )))
    .fetch_all(&db.pool)
    .await
    .unwrap_or_default();

    panic!(
        "EC-01 DIFF-vs-FULL divergence at cycle {cycle} seed={seed:#x}\n\
         FULL rows={full_count}, DIFF rows={diff_count}\n\
         order change action counts={action_counts:?}\n\
         extra DIFF rows={extra_rows:?}\n\
         missing DIFF rows={missing_rows:?}"
    );
}

async fn seed_tables(db: &E2eDb) -> (Vec<i32>, Vec<i32>, Vec<i32>) {
    db.execute("CREATE TABLE ec01_accounts (id INT PRIMARY KEY, region INT, tier INT)")
        .await;
    db.execute("CREATE TABLE ec01_products (id INT PRIMARY KEY, category INT, active BOOLEAN)")
        .await;
    db.execute(
        "CREATE TABLE ec01_orders (id INT PRIMARY KEY, account_id INT, product_id INT, amount INT)",
    )
    .await;

    let mut accounts = Vec::new();
    for id in 1..=8 {
        accounts.push(id);
        db.execute(&format!(
            "INSERT INTO ec01_accounts VALUES ({id}, {}, {})",
            (id % 3) + 1,
            (id % 2) + 1,
        ))
        .await;
    }

    let mut products = Vec::new();
    for id in 1..=8 {
        products.push(id);
        db.execute(&format!(
            "INSERT INTO ec01_products VALUES ({id}, {}, {})",
            (id % 4) + 1,
            if id % 5 == 0 { "false" } else { "true" },
        ))
        .await;
    }

    let mut orders = Vec::new();
    for id in 1..=24 {
        orders.push(id);
        let account_id = accounts[(id as usize - 1) % accounts.len()];
        let product_id = products[(id as usize * 3 - 1) % products.len()];
        db.execute(&format!(
            "INSERT INTO ec01_orders VALUES ({id}, {account_id}, {product_id}, {})",
            10 + id * 3,
        ))
        .await;
    }

    (accounts, products, orders)
}

#[tokio::test]
async fn test_ec01_join_diff_vs_full_converges_under_random_co_deletes() {
    let seed = BASE_SEED;
    let mut rng = SeededRng::new(seed);
    let cycles = ec01_cycles();
    let db = E2eDb::new().await.with_extension().await;
    db.execute("ALTER SYSTEM SET pg_trickle.enabled = false")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    let (mut accounts, mut products, mut orders) = seed_tables(&db).await;
    let mut next_account_id = 9;
    let mut next_product_id = 9;
    let mut next_order_id = 25;

    let query = "SELECT a.region, p.category, \
                        SUM(o.amount)::bigint AS total_amount, \
                        COUNT(*)::bigint AS order_count \
                 FROM ec01_orders o \
                 JOIN ec01_accounts a ON a.id = o.account_id \
                 JOIN ec01_products p ON p.id = o.product_id \
                 WHERE p.active \
                 GROUP BY a.region, p.category";

    db.create_st("ec01_full_st", query, "24h", "FULL").await;
    db.create_st("ec01_diff_st", query, "24h", "DIFFERENTIAL")
        .await;

    assert_st_query_invariant(&db, "ec01_full_st", query, seed, 0, "baseline-full").await;
    assert_st_query_invariant(&db, "ec01_diff_st", query, seed, 0, "baseline-diff").await;
    assert_full_diff_equal(&db, "ec01_full_st", "ec01_diff_st", seed, 0).await;

    for cycle in 1..=cycles {
        if accounts.len() < 3 {
            let id = next_account_id;
            next_account_id += 1;
            accounts.push(id);
            db.execute(&format!(
                "INSERT INTO ec01_accounts VALUES ({id}, {}, {})",
                rng.i32_range(1, 3),
                rng.i32_range(1, 2),
            ))
            .await;
        }
        if products.len() < 3 {
            let id = next_product_id;
            next_product_id += 1;
            products.push(id);
            db.execute(&format!(
                "INSERT INTO ec01_products VALUES ({id}, {}, true)",
                rng.i32_range(1, 4),
            ))
            .await;
        }

        for _ in 0..rng.usize_range(1, 3) {
            let id = next_order_id;
            next_order_id += 1;
            let account_id = pick_live(&mut rng, &accounts).expect("accounts replenished");
            let product_id = pick_live(&mut rng, &products).expect("products replenished");
            let amount = rng.i32_range(1, 500);
            orders.push(id);
            db.execute(&format!(
                "INSERT INTO ec01_orders VALUES ({id}, {account_id}, {product_id}, {amount})"
            ))
            .await;
        }

        for _ in 0..rng.usize_range(1, 4) {
            if let Some(order_id) = pick_live(&mut rng, &orders) {
                let account_id = pick_live(&mut rng, &accounts).expect("accounts replenished");
                let product_id = pick_live(&mut rng, &products).expect("products replenished");
                let amount = rng.i32_range(1, 500);
                db.execute(&format!(
                    "UPDATE ec01_orders \
                     SET account_id = {account_id}, product_id = {product_id}, amount = {amount} \
                     WHERE id = {order_id}"
                ))
                .await;
            }
        }

        if let Some(account_id) = pick_live(&mut rng, &accounts) {
            db.execute(&format!(
                "UPDATE ec01_accounts SET region = {}, tier = {} WHERE id = {account_id}",
                rng.i32_range(1, 3),
                rng.i32_range(1, 2),
            ))
            .await;
        }
        if let Some(product_id) = pick_live(&mut rng, &products) {
            db.execute(&format!(
                "UPDATE ec01_products SET category = {}, active = {} WHERE id = {product_id}",
                rng.i32_range(1, 4),
                if rng.gen_bool() { "true" } else { "false" },
            ))
            .await;
        }

        for _ in 0..rng.usize_range(1, 3) {
            if let Some(order_id) = remove_live(&mut rng, &mut orders) {
                db.execute(&format!("DELETE FROM ec01_orders WHERE id = {order_id}"))
                    .await;
            }
        }
        if cycle % 3 == 0
            && let Some(product_id) = remove_live(&mut rng, &mut products)
        {
            db.execute(&format!(
                "DELETE FROM ec01_products WHERE id = {product_id}"
            ))
            .await;
        }
        if cycle % 5 == 0
            && let Some(account_id) = remove_live(&mut rng, &mut accounts)
        {
            db.execute(&format!(
                "DELETE FROM ec01_accounts WHERE id = {account_id}"
            ))
            .await;
        }

        db.refresh_st_with_retry("ec01_diff_st").await;
        db.refresh_st_with_retry("ec01_full_st").await;

        assert_st_query_invariant(&db, "ec01_full_st", query, seed, cycle, "full").await;
        assert_st_query_invariant(&db, "ec01_diff_st", query, seed, cycle, "diff").await;
        assert_full_diff_equal(&db, "ec01_full_st", "ec01_diff_st", seed, cycle).await;
    }
}
