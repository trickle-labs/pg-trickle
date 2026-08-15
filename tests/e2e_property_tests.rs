//! E2E property-based correctness tests for pgtrickle.
//!
//! **THE KEY INVARIANT** (DBSP §4, Gupta & Mumick 1995 §3):
//!
//! > For every ST, at every data timestamp:
//! >   Contents(ST) = Result(defining_query)   (multiset equality)
//!
//! Each test:
//! 1. Creates source tables with a fixed schema
//! 2. Inserts randomised initial data (deterministic PRNG)
//! 3. Creates a stream table (DIFFERENTIAL or FULL)
//! 4. Verifies the invariant
//! 5. Repeats N cycles of: random DML → refresh → verify invariant
//!
//! Randomisation uses a deterministic SplitMix64 PRNG seeded per test.
//! On failure the seed is printed for reproduction.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::{
    E2eDb,
    property_support::{SeededRng as Rng, TrackedIds},
};

// ── Configuration ──────────────────────────────────────────────────────

const INITIAL_ROWS: usize = 15;
const CYCLES: usize = 5;

// ── Invariant assertion ────────────────────────────────────────────────

/// Assert the KEY INVARIANT: ST contents == defining query result.
///
/// Compares only user-visible columns (excludes all `__pgt_*` internal
/// columns) using `EXCEPT ALL` for correct multiset (bag) comparison.
async fn assert_invariant(db: &E2eDb, pgt_name: &str, query: &str, seed: u64, cycle: usize) {
    let st_table = format!("public.{pgt_name}");

    // User-visible columns (exclude all __pgt_* internal columns)
    let cols: String = db
        .query_scalar(&format!(
            "SELECT string_agg(column_name, ', ' ORDER BY ordinal_position) \
             FROM information_schema.columns \
             WHERE table_schema = 'public' AND table_name = '{pgt_name}' \
               AND column_name NOT LIKE '__pgt_%'"
        ))
        .await;

    // INTERSECT/EXCEPT STs keep invisible rows for multiplicity tracking.
    // Compare only the user-visible subset, matching the main E2E harness.
    let has_dual_counts: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.columns \
                WHERE table_schema = 'public' AND table_name = '{pgt_name}' \
                  AND column_name = '__pgt_count_l')"
        ))
        .await;

    let dq_upper = query.to_uppercase();
    let set_op_filter = if has_dual_counts {
        if dq_upper.contains("INTERSECT ALL") {
            " WHERE LEAST(__pgt_count_l, __pgt_count_r) > 0"
        } else if dq_upper.contains("INTERSECT") {
            " WHERE __pgt_count_l > 0 AND __pgt_count_r > 0"
        } else if dq_upper.contains("EXCEPT ALL") {
            " WHERE __pgt_count_l > __pgt_count_r"
        } else if dq_upper.contains("EXCEPT") {
            " WHERE __pgt_count_l > 0 AND __pgt_count_r = 0"
        } else {
            ""
        }
    } else {
        ""
    };

    // Multiset equality: symmetric EXCEPT ALL must be empty
    let matches: bool = db
        .query_scalar(&format!(
            "SELECT NOT EXISTS ( \
                (SELECT {cols} FROM {st_table}{set_op_filter} EXCEPT ALL ({query})) \
                UNION ALL \
                (({query}) EXCEPT ALL SELECT {cols} FROM {st_table}{set_op_filter}) \
            )"
        ))
        .await;

    if !matches {
        let st_count: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM {st_table}{set_op_filter}"))
            .await;
        let q_count: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM ({query}) _q"))
            .await;
        let extra: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM \
                 (SELECT {cols} FROM {st_table}{set_op_filter} EXCEPT ALL ({query})) _x"
            ))
            .await;
        let missing: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM \
                 (({query}) EXCEPT ALL SELECT {cols} FROM {st_table}{set_op_filter}) _x"
            ))
            .await;

        panic!(
            "INVARIANT VIOLATED at cycle {} (seed={:#x})\n\
             ST: {}, Query: {}\n\
             ST rows: {}, Query rows: {}\n\
             Extra in ST: {}, Missing from ST: {}",
            cycle, seed, pgt_name, query, st_count, q_count, extra, missing,
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 1: Simple scan — SELECT all columns
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_scan_differential() {
    let seed: u64 = 0xCAFE_0001;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_s (id INT PRIMARY KEY, val INT, label TEXT)")
        .await;

    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let val = rng.i32_range(-100, 100);
        let label = rng.gen_alpha(4);
        db.execute(&format!(
            "INSERT INTO prop_s VALUES ({id}, {val}, '{label}')"
        ))
        .await;
    }

    let query = "SELECT id, val, label FROM prop_s";
    db.create_st("prop_s_st", query, "1m", "DIFFERENTIAL").await;
    assert_invariant(&db, "prop_s_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        // Inserts
        let n_ins = rng.usize_range(2, 5);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let val = rng.i32_range(-100, 100);
            let label = rng.gen_alpha(4);
            db.execute(&format!(
                "INSERT INTO prop_s VALUES ({id}, {val}, '{label}')"
            ))
            .await;
        }

        // Updates
        let n_upd = rng.usize_range(0, 3);
        for _ in 0..n_upd {
            if let Some(id) = ids.pick(&mut rng) {
                let val = rng.i32_range(-100, 100);
                let label = rng.gen_alpha(4);
                db.execute(&format!(
                    "UPDATE prop_s SET val = {val}, label = '{label}' WHERE id = {id}"
                ))
                .await;
            }
        }

        // Deletes
        let n_del = rng.usize_range(0, 2);
        for _ in 0..n_del {
            if let Some(id) = ids.remove_random(&mut rng) {
                db.execute(&format!("DELETE FROM prop_s WHERE id = {id}"))
                    .await;
            }
        }

        db.refresh_st("prop_s_st").await;
        assert_invariant(&db, "prop_s_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 2: Filter — rows crossing the predicate boundary
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_filter_differential() {
    let seed: u64 = 0xCAFE_0002;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_f (id INT PRIMARY KEY, score INT, tag TEXT)")
        .await;

    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let score = rng.i32_range(20, 80); // straddles filter boundary (> 50)
        let tag = rng.gen_alpha(3);
        db.execute(&format!(
            "INSERT INTO prop_f VALUES ({id}, {score}, '{tag}')"
        ))
        .await;
    }

    let query = "SELECT id, score, tag FROM prop_f WHERE score > 50";
    db.create_st("prop_f_st", query, "1m", "DIFFERENTIAL").await;
    assert_invariant(&db, "prop_f_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(2, 5);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let score = rng.i32_range(20, 80);
            let tag = rng.gen_alpha(3);
            db.execute(&format!(
                "INSERT INTO prop_f VALUES ({id}, {score}, '{tag}')"
            ))
            .await;
        }

        // Updates — some will cross the filter boundary
        let n_upd = rng.usize_range(1, 3);
        for _ in 0..n_upd {
            if let Some(id) = ids.pick(&mut rng) {
                let score = rng.i32_range(20, 80);
                db.execute(&format!(
                    "UPDATE prop_f SET score = {score} WHERE id = {id}"
                ))
                .await;
            }
        }

        let n_del = rng.usize_range(0, 2);
        for _ in 0..n_del {
            if let Some(id) = ids.remove_random(&mut rng) {
                db.execute(&format!("DELETE FROM prop_f WHERE id = {id}"))
                    .await;
            }
        }

        db.refresh_st("prop_f_st").await;
        assert_invariant(&db, "prop_f_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 3: Inner join — DML on both sides
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_join_differential() {
    let seed: u64 = 0xCAFE_0003;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_jl (id INT PRIMARY KEY, val INT, key INT)")
        .await;
    db.execute("CREATE TABLE prop_jr (id INT PRIMARY KEY, val INT, key INT)")
        .await;

    let mut l_ids = TrackedIds::new();
    let mut r_ids = TrackedIds::new();

    for _ in 0..INITIAL_ROWS {
        let id = l_ids.alloc();
        let val = rng.i32_range(1, 100);
        let key = rng.i32_range(1, 5); // limited range → many matches
        db.execute(&format!("INSERT INTO prop_jl VALUES ({id}, {val}, {key})"))
            .await;
    }
    for _ in 0..INITIAL_ROWS {
        let id = r_ids.alloc();
        let val = rng.i32_range(1, 100);
        let key = rng.i32_range(1, 5);
        db.execute(&format!("INSERT INTO prop_jr VALUES ({id}, {val}, {key})"))
            .await;
    }

    let query = "SELECT l.id AS lid, l.val AS lval, r.id AS rid, r.val AS rval \
                 FROM prop_jl l JOIN prop_jr r ON l.key = r.key";
    db.create_st("prop_j_st", query, "1m", "DIFFERENTIAL").await;
    assert_invariant(&db, "prop_j_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        // DML on left table
        let n_ins = rng.usize_range(1, 3);
        for _ in 0..n_ins {
            let id = l_ids.alloc();
            let val = rng.i32_range(1, 100);
            let key = rng.i32_range(1, 5);
            db.execute(&format!("INSERT INTO prop_jl VALUES ({id}, {val}, {key})"))
                .await;
        }
        if rng.gen_bool()
            && let Some(id) = l_ids.remove_random(&mut rng)
        {
            db.execute(&format!("DELETE FROM prop_jl WHERE id = {id}"))
                .await;
        }

        // DML on right table
        let n_ins = rng.usize_range(1, 3);
        for _ in 0..n_ins {
            let id = r_ids.alloc();
            let val = rng.i32_range(1, 100);
            let key = rng.i32_range(1, 5);
            db.execute(&format!("INSERT INTO prop_jr VALUES ({id}, {val}, {key})"))
                .await;
        }
        let n_upd = rng.usize_range(0, 2);
        for _ in 0..n_upd {
            if let Some(id) = r_ids.pick(&mut rng) {
                let val = rng.i32_range(1, 100);
                db.execute(&format!("UPDATE prop_jr SET val = {val} WHERE id = {id}"))
                    .await;
            }
        }

        db.refresh_st("prop_j_st").await;
        assert_invariant(&db, "prop_j_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 4: Aggregate — GROUP BY with COUNT + SUM
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_aggregate_differential() {
    let seed: u64 = 0xCAFE_0004;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_agg (id INT PRIMARY KEY, region TEXT, amount INT)")
        .await;

    let regions = ["north", "south", "east", "west"];
    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let region = rng.choose(&regions);
        let amount = rng.i32_range(10, 500);
        db.execute(&format!(
            "INSERT INTO prop_agg VALUES ({id}, '{region}', {amount})"
        ))
        .await;
    }

    let query = "SELECT region, COUNT(*) AS cnt, SUM(amount) AS total \
                 FROM prop_agg GROUP BY region";
    db.create_st("prop_agg_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_agg_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(2, 5);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let region = rng.choose(&regions);
            let amount = rng.i32_range(10, 500);
            db.execute(&format!(
                "INSERT INTO prop_agg VALUES ({id}, '{region}', {amount})"
            ))
            .await;
        }

        let n_upd = rng.usize_range(0, 3);
        for _ in 0..n_upd {
            if let Some(id) = ids.pick(&mut rng) {
                let amount = rng.i32_range(10, 500);
                db.execute(&format!(
                    "UPDATE prop_agg SET amount = {amount} WHERE id = {id}"
                ))
                .await;
            }
        }

        let n_del = rng.usize_range(0, 2);
        for _ in 0..n_del {
            if let Some(id) = ids.remove_random(&mut rng) {
                db.execute(&format!("DELETE FROM prop_agg WHERE id = {id}"))
                    .await;
            }
        }

        db.refresh_st("prop_agg_st").await;
        assert_invariant(&db, "prop_agg_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 5: DISTINCT — duplicate-aware multiset tracking
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_distinct_differential() {
    let seed: u64 = 0xCAFE_0005;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_dist (id INT PRIMARY KEY, color TEXT, size TEXT)")
        .await;

    let colors = ["red", "blue", "green"];
    let sizes = ["s", "m", "l"];
    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let color = rng.choose(&colors);
        let size = rng.choose(&sizes);
        db.execute(&format!(
            "INSERT INTO prop_dist VALUES ({id}, '{color}', '{size}')"
        ))
        .await;
    }

    let query = "SELECT DISTINCT color, size FROM prop_dist";
    db.create_st("prop_dist_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_dist_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(2, 4);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let color = rng.choose(&colors);
            let size = rng.choose(&sizes);
            db.execute(&format!(
                "INSERT INTO prop_dist VALUES ({id}, '{color}', '{size}')"
            ))
            .await;
        }

        let n_upd = rng.usize_range(0, 2);
        for _ in 0..n_upd {
            if let Some(id) = ids.pick(&mut rng) {
                let color = rng.choose(&colors);
                let size = rng.choose(&sizes);
                db.execute(&format!(
                    "UPDATE prop_dist SET color = '{color}', size = '{size}' WHERE id = {id}"
                ))
                .await;
            }
        }

        let n_del = rng.usize_range(0, 2);
        for _ in 0..n_del {
            if let Some(id) = ids.remove_random(&mut rng) {
                db.execute(&format!("DELETE FROM prop_dist WHERE id = {id}"))
                    .await;
            }
        }

        db.refresh_st("prop_dist_st").await;
        assert_invariant(&db, "prop_dist_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 6: LEFT JOIN — NULL padding when right side has no match
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_left_join_differential() {
    let seed: u64 = 0xCAFE_0006;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_ljl (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE prop_ljr (id INT PRIMARY KEY, left_id INT, detail TEXT)")
        .await;

    let mut l_ids = TrackedIds::new();
    let mut r_ids = TrackedIds::new();

    for _ in 0..INITIAL_ROWS {
        let id = l_ids.alloc();
        let val = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_ljl VALUES ({id}, {val})"))
            .await;
    }
    // Right side: some match existing left IDs, some don't
    for _ in 0..10 {
        let id = r_ids.alloc();
        let left_id = rng.i32_range(1, INITIAL_ROWS as i32 + 5);
        let detail = rng.gen_alpha(3);
        db.execute(&format!(
            "INSERT INTO prop_ljr VALUES ({id}, {left_id}, '{detail}')"
        ))
        .await;
    }

    let query = "SELECT l.id AS lid, l.val, r.detail \
                 FROM prop_ljl l LEFT JOIN prop_ljr r ON l.id = r.left_id";
    db.create_st("prop_lj_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_lj_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        // DML on left
        let n_ins = rng.usize_range(1, 3);
        for _ in 0..n_ins {
            let id = l_ids.alloc();
            let val = rng.i32_range(1, 100);
            db.execute(&format!("INSERT INTO prop_ljl VALUES ({id}, {val})"))
                .await;
        }
        if rng.gen_bool()
            && let Some(id) = l_ids.remove_random(&mut rng)
        {
            db.execute(&format!("DELETE FROM prop_ljl WHERE id = {id}"))
                .await;
        }

        // DML on right — some left_ids valid, some not
        let n_ins = rng.usize_range(1, 3);
        for _ in 0..n_ins {
            let id = r_ids.alloc();
            let left_id = rng.i32_range(1, l_ids.next_unused_id() + 3);
            let detail = rng.gen_alpha(3);
            db.execute(&format!(
                "INSERT INTO prop_ljr VALUES ({id}, {left_id}, '{detail}')"
            ))
            .await;
        }
        if rng.gen_bool()
            && let Some(id) = r_ids.remove_random(&mut rng)
        {
            db.execute(&format!("DELETE FROM prop_ljr WHERE id = {id}"))
                .await;
        }

        db.refresh_st("prop_lj_st").await;
        assert_invariant(&db, "prop_lj_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 7: UNION ALL — DML on both branches
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_union_all_differential() {
    let seed: u64 = 0xCAFE_0007;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_ua1 (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE prop_ua2 (id INT PRIMARY KEY, val INT)")
        .await;

    let mut ids1 = TrackedIds::new();
    let mut ids2 = TrackedIds::new();

    for _ in 0..10 {
        let id = ids1.alloc();
        let val = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_ua1 VALUES ({id}, {val})"))
            .await;
    }
    for _ in 0..10 {
        let id = ids2.alloc();
        let val = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_ua2 VALUES ({id}, {val})"))
            .await;
    }

    let query = "SELECT id, val FROM prop_ua1 UNION ALL SELECT id, val FROM prop_ua2";
    db.create_st("prop_ua_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_ua_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        // DML on source 1
        let n_ins = rng.usize_range(1, 3);
        for _ in 0..n_ins {
            let id = ids1.alloc();
            let val = rng.i32_range(1, 100);
            db.execute(&format!("INSERT INTO prop_ua1 VALUES ({id}, {val})"))
                .await;
        }
        if rng.gen_bool()
            && let Some(id) = ids1.remove_random(&mut rng)
        {
            db.execute(&format!("DELETE FROM prop_ua1 WHERE id = {id}"))
                .await;
        }

        // DML on source 2
        let n_ins = rng.usize_range(1, 3);
        for _ in 0..n_ins {
            let id = ids2.alloc();
            let val = rng.i32_range(1, 100);
            db.execute(&format!("INSERT INTO prop_ua2 VALUES ({id}, {val})"))
                .await;
        }
        let n_upd = rng.usize_range(0, 2);
        for _ in 0..n_upd {
            if let Some(id) = ids2.pick(&mut rng) {
                let val = rng.i32_range(1, 100);
                db.execute(&format!("UPDATE prop_ua2 SET val = {val} WHERE id = {id}"))
                    .await;
            }
        }

        db.refresh_st("prop_ua_st").await;
        assert_invariant(&db, "prop_ua_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 8: Filter + Aggregate combined — rows crossing filter boundary
//         change group membership and aggregated values
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_filter_aggregate_differential() {
    let seed: u64 = 0xCAFE_0008;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_fa (id INT PRIMARY KEY, grp INT, val INT)")
        .await;

    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let grp = rng.i32_range(1, 4);
        let val = rng.i32_range(-50, 100); // some negative → filtered out
        db.execute(&format!("INSERT INTO prop_fa VALUES ({id}, {grp}, {val})"))
            .await;
    }

    let query = "SELECT grp, COUNT(*) AS cnt, SUM(val) AS total \
                 FROM prop_fa WHERE val > 0 GROUP BY grp";
    db.create_st("prop_fa_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_fa_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(2, 5);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let grp = rng.i32_range(1, 4);
            let val = rng.i32_range(-50, 100);
            db.execute(&format!("INSERT INTO prop_fa VALUES ({id}, {grp}, {val})"))
                .await;
        }

        // Updates — val may cross the filter boundary
        let n_upd = rng.usize_range(1, 3);
        for _ in 0..n_upd {
            if let Some(id) = ids.pick(&mut rng) {
                let val = rng.i32_range(-50, 100);
                db.execute(&format!("UPDATE prop_fa SET val = {val} WHERE id = {id}"))
                    .await;
            }
        }

        let n_del = rng.usize_range(0, 2);
        for _ in 0..n_del {
            if let Some(id) = ids.remove_random(&mut rng) {
                db.execute(&format!("DELETE FROM prop_fa WHERE id = {id}"))
                    .await;
            }
        }

        db.refresh_st("prop_fa_st").await;
        assert_invariant(&db, "prop_fa_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 9: Join + Aggregate — orders joined to customers, grouped by region
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_join_aggregate_differential() {
    let seed: u64 = 0xCAFE_0009;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_ja_ord (id INT PRIMARY KEY, cust_id INT, amount INT)")
        .await;
    db.execute("CREATE TABLE prop_ja_cust (id INT PRIMARY KEY, region TEXT)")
        .await;

    let regions = ["north", "south", "east", "west"];

    // Create customers first
    let mut c_ids = TrackedIds::new();
    for _ in 0..5 {
        let id = c_ids.alloc();
        let region = rng.choose(&regions);
        db.execute(&format!(
            "INSERT INTO prop_ja_cust VALUES ({id}, '{region}')"
        ))
        .await;
    }

    // Create orders referencing customers
    let mut o_ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = o_ids.alloc();
        let cust_id = rng.i32_range(1, c_ids.next_unused_id() - 1);
        let amount = rng.i32_range(10, 500);
        db.execute(&format!(
            "INSERT INTO prop_ja_ord VALUES ({id}, {cust_id}, {amount})"
        ))
        .await;
    }

    let query = "SELECT c.region, COUNT(*) AS cnt, SUM(o.amount) AS total \
                 FROM prop_ja_ord o JOIN prop_ja_cust c ON o.cust_id = c.id \
                 GROUP BY c.region";
    db.create_st("prop_ja_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_ja_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        // New orders
        let n_ins = rng.usize_range(2, 4);
        for _ in 0..n_ins {
            let id = o_ids.alloc();
            let cust_id = rng.i32_range(1, c_ids.next_unused_id() - 1);
            let amount = rng.i32_range(10, 500);
            db.execute(&format!(
                "INSERT INTO prop_ja_ord VALUES ({id}, {cust_id}, {amount})"
            ))
            .await;
        }

        // Update order amounts
        let n_upd = rng.usize_range(0, 2);
        for _ in 0..n_upd {
            if let Some(id) = o_ids.pick(&mut rng) {
                let amount = rng.i32_range(10, 500);
                db.execute(&format!(
                    "UPDATE prop_ja_ord SET amount = {amount} WHERE id = {id}"
                ))
                .await;
            }
        }

        // Delete some orders
        if rng.gen_bool()
            && let Some(id) = o_ids.remove_random(&mut rng)
        {
            db.execute(&format!("DELETE FROM prop_ja_ord WHERE id = {id}"))
                .await;
        }

        // Add a new customer midway through
        if cycle == 3 {
            let id = c_ids.alloc();
            let region = rng.choose(&regions);
            db.execute(&format!(
                "INSERT INTO prop_ja_cust VALUES ({id}, '{region}')"
            ))
            .await;
        }

        db.refresh_st("prop_ja_st").await;
        assert_invariant(&db, "prop_ja_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 10: CTE + filter + aggregate — WITH clause inlined by DVM
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_cte_differential() {
    let seed: u64 = 0xCAFE_000A;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_cte (id INT PRIMARY KEY, grp TEXT, val INT)")
        .await;

    let groups = ["x", "y", "z"];
    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let grp = rng.choose(&groups);
        let val = rng.i32_range(-30, 100);
        db.execute(&format!(
            "INSERT INTO prop_cte VALUES ({id}, '{grp}', {val})"
        ))
        .await;
    }

    let query = "WITH positive AS (SELECT id, grp, val FROM prop_cte WHERE val > 0) \
                 SELECT grp, COUNT(*) AS cnt, SUM(val) AS total \
                 FROM positive GROUP BY grp";
    db.create_st("prop_cte_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_cte_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(2, 5);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let grp = rng.choose(&groups);
            let val = rng.i32_range(-30, 100);
            db.execute(&format!(
                "INSERT INTO prop_cte VALUES ({id}, '{grp}', {val})"
            ))
            .await;
        }

        let n_upd = rng.usize_range(1, 3);
        for _ in 0..n_upd {
            if let Some(id) = ids.pick(&mut rng) {
                let val = rng.i32_range(-30, 100);
                db.execute(&format!("UPDATE prop_cte SET val = {val} WHERE id = {id}"))
                    .await;
            }
        }

        if rng.gen_bool()
            && let Some(id) = ids.remove_random(&mut rng)
        {
            db.execute(&format!("DELETE FROM prop_cte WHERE id = {id}"))
                .await;
        }

        db.refresh_st("prop_cte_st").await;
        assert_invariant(&db, "prop_cte_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 11: FULL refresh mode — validates the invariant with full
//          recomputation across scan, filter, and aggregate patterns
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_full_mode() {
    let seed: u64 = 0xCAFE_000B;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_full (id INT PRIMARY KEY, grp TEXT, val INT)")
        .await;

    let groups = ["alpha", "beta", "gamma"];
    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let grp = rng.choose(&groups);
        let val = rng.i32_range(1, 100);
        db.execute(&format!(
            "INSERT INTO prop_full VALUES ({id}, '{grp}', {val})"
        ))
        .await;
    }

    // Three STs with different operator patterns, all FULL mode
    let q_scan = "SELECT id, grp, val FROM prop_full";
    let q_agg = "SELECT grp, COUNT(*) AS cnt, SUM(val) AS total \
                 FROM prop_full GROUP BY grp";
    let q_filt = "SELECT id, val FROM prop_full WHERE val > 50";

    db.create_st("propf_scan", q_scan, "1m", "FULL").await;
    db.create_st("propf_agg", q_agg, "1m", "FULL").await;
    db.create_st("propf_filt", q_filt, "1m", "FULL").await;

    assert_invariant(&db, "propf_scan", q_scan, seed, 0).await;
    assert_invariant(&db, "propf_agg", q_agg, seed, 0).await;
    assert_invariant(&db, "propf_filt", q_filt, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(2, 5);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let grp = rng.choose(&groups);
            let val = rng.i32_range(1, 100);
            db.execute(&format!(
                "INSERT INTO prop_full VALUES ({id}, '{grp}', {val})"
            ))
            .await;
        }

        let n_upd = rng.usize_range(1, 3);
        for _ in 0..n_upd {
            if let Some(id) = ids.pick(&mut rng) {
                let val = rng.i32_range(1, 100);
                let grp = rng.choose(&groups);
                db.execute(&format!(
                    "UPDATE prop_full SET val = {val}, grp = '{grp}' WHERE id = {id}"
                ))
                .await;
            }
        }

        let n_del = rng.usize_range(0, 2);
        for _ in 0..n_del {
            if let Some(id) = ids.remove_random(&mut rng) {
                db.execute(&format!("DELETE FROM prop_full WHERE id = {id}"))
                    .await;
            }
        }

        db.refresh_st("propf_scan").await;
        db.refresh_st("propf_agg").await;
        db.refresh_st("propf_filt").await;

        assert_invariant(&db, "propf_scan", q_scan, seed, cycle).await;
        assert_invariant(&db, "propf_agg", q_agg, seed, cycle).await;
        assert_invariant(&db, "propf_filt", q_filt, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 12: Window function — FULL mode (not differentiable)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_window_function_full() {
    let seed: u64 = 0xCAFE_0020;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_win (id INT PRIMARY KEY, dept TEXT, salary INT)")
        .await;

    let depts = ["eng", "sales", "ops"];
    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let dept = rng.choose(&depts);
        let salary = rng.i32_range(50_000, 150_000);
        db.execute(&format!(
            "INSERT INTO prop_win VALUES ({id}, '{dept}', {salary})"
        ))
        .await;
    }

    let query = "SELECT id, dept, salary, \
                 RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk \
                 FROM prop_win";
    db.create_st("prop_win_st", query, "1m", "FULL").await;
    assert_invariant(&db, "prop_win_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(1, 4);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let dept = rng.choose(&depts);
            let salary = rng.i32_range(50_000, 150_000);
            db.execute(&format!(
                "INSERT INTO prop_win VALUES ({id}, '{dept}', {salary})"
            ))
            .await;
        }
        if let Some(id) = ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_win WHERE id = {id}"))
                .await;
        }
        if let Some(id) = ids.pick(&mut rng) {
            let salary = rng.i32_range(50_000, 150_000);
            db.execute(&format!(
                "UPDATE prop_win SET salary = {salary} WHERE id = {id}"
            ))
            .await;
        }
        db.refresh_st("prop_win_st").await;
        assert_invariant(&db, "prop_win_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 13: Non-recursive CTE — DIFFERENTIAL
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_cte_nonrecursive_differential() {
    let seed: u64 = 0xCAFE_0021;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_cte2 (id INT PRIMARY KEY, region TEXT, amount INT)")
        .await;

    let regions = ["north", "south"];
    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let region = rng.choose(&regions);
        let amount = rng.i32_range(100, 1000);
        db.execute(&format!(
            "INSERT INTO prop_cte2 VALUES ({id}, '{region}', {amount})"
        ))
        .await;
    }

    let query = "WITH totals AS ( \
                   SELECT region, SUM(amount) AS total FROM prop_cte2 GROUP BY region \
                 ) \
                 SELECT region, total FROM totals WHERE total > 500";
    db.create_st("prop_cte2_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_cte2_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(1, 4);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let region = rng.choose(&regions);
            let amount = rng.i32_range(100, 1000);
            db.execute(&format!(
                "INSERT INTO prop_cte2 VALUES ({id}, '{region}', {amount})"
            ))
            .await;
        }
        if let Some(id) = ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_cte2 WHERE id = {id}"))
                .await;
        }
        db.refresh_st("prop_cte2_st").await;
        assert_invariant(&db, "prop_cte2_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 14: LATERAL join — DIFFERENTIAL
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_lateral_join_differential() {
    let seed: u64 = 0xCAFE_0022;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_lat_a (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE prop_lat_b (id INT PRIMARY KEY, a_id INT, score INT)")
        .await;

    let mut a_ids = TrackedIds::new();
    let mut b_ids = TrackedIds::new();

    for _ in 0..10 {
        let id = a_ids.alloc();
        let val = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_lat_a VALUES ({id}, {val})"))
            .await;
    }
    for _ in 0..INITIAL_ROWS {
        let id = b_ids.alloc();
        if let Some(a_id) = a_ids.pick(&mut rng) {
            let score = rng.i32_range(1, 100);
            db.execute(&format!(
                "INSERT INTO prop_lat_b VALUES ({id}, {a_id}, {score})"
            ))
            .await;
        }
    }

    let query = "SELECT a.id AS a_id, a.val, sub.max_score \
                 FROM prop_lat_a a \
                 LEFT JOIN LATERAL ( \
                   SELECT MAX(b.score) AS max_score \
                   FROM prop_lat_b b WHERE b.a_id = a.id \
                 ) sub ON true";
    db.create_st("prop_lat_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_lat_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let id = b_ids.alloc();
        if let Some(a_id) = a_ids.pick(&mut rng) {
            let score = rng.i32_range(1, 100);
            db.execute(&format!(
                "INSERT INTO prop_lat_b VALUES ({id}, {a_id}, {score})"
            ))
            .await;
        }
        if let Some(b_id) = b_ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_lat_b WHERE id = {b_id}"))
                .await;
        }
        db.refresh_st("prop_lat_st").await;
        assert_invariant(&db, "prop_lat_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 15: EXCEPT — AUTO falls back to FULL
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_except_differential() {
    let seed: u64 = 0xCAFE_0023;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_exc_a (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE prop_exc_b (id INT PRIMARY KEY, val INT)")
        .await;

    let mut a_ids = TrackedIds::new();
    let mut b_ids = TrackedIds::new();

    for _ in 0..INITIAL_ROWS {
        let id = a_ids.alloc();
        let val = rng.i32_range(1, 10);
        db.execute(&format!("INSERT INTO prop_exc_a VALUES ({id}, {val})"))
            .await;
    }
    for _ in 0..INITIAL_ROWS {
        let id = b_ids.alloc();
        let val = rng.i32_range(1, 10);
        db.execute(&format!("INSERT INTO prop_exc_b VALUES ({id}, {val})"))
            .await;
    }

    let query = "SELECT val FROM prop_exc_a EXCEPT SELECT val FROM prop_exc_b";
    db.create_st("prop_exc_st", query, "1m", "AUTO").await;
    db.assert_st_matches_query("prop_exc_st", query).await;

    for cycle in 1..=CYCLES {
        let id = a_ids.alloc();
        let val = rng.i32_range(1, 10);
        db.execute(&format!("INSERT INTO prop_exc_a VALUES ({id}, {val})"))
            .await;

        if rng.gen_bool() {
            let id = b_ids.alloc();
            let val = rng.i32_range(1, 10);
            db.execute(&format!("INSERT INTO prop_exc_b VALUES ({id}, {val})"))
                .await;
        }
        if let Some(id) = a_ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_exc_a WHERE id = {id}"))
                .await;
        }

        db.refresh_st("prop_exc_st").await;
        db.assert_st_matches_query("prop_exc_st", query).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 16: HAVING clause — DIFFERENTIAL
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_having_differential() {
    let seed: u64 = 0xCAFE_0024;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_hav (id INT PRIMARY KEY, category TEXT, amount INT)")
        .await;

    let cats = ["a", "b", "c", "d"];
    let mut ids = TrackedIds::new();
    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        let cat = rng.choose(&cats);
        let amt = rng.i32_range(1, 100);
        db.execute(&format!(
            "INSERT INTO prop_hav VALUES ({id}, '{cat}', {amt})"
        ))
        .await;
    }

    let query = "SELECT category, SUM(amount) AS total, COUNT(*) AS cnt \
                 FROM prop_hav GROUP BY category HAVING SUM(amount) > 100";
    db.create_st("prop_hav_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_hav_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let n_ins = rng.usize_range(1, 4);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let cat = rng.choose(&cats);
            let amt = rng.i32_range(1, 100);
            db.execute(&format!(
                "INSERT INTO prop_hav VALUES ({id}, '{cat}', {amt})"
            ))
            .await;
        }
        if let Some(id) = ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_hav WHERE id = {id}"))
                .await;
        }
        db.refresh_st("prop_hav_st").await;
        assert_invariant(&db, "prop_hav_st", query, seed, cycle).await;
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Test 17: Three-table join — DIFFERENTIAL
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_property_three_table_join_differential() {
    let seed: u64 = 0xCAFE_0025;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_t3a (id INT PRIMARY KEY, key INT, a INT)")
        .await;
    db.execute("CREATE TABLE prop_t3b (id INT PRIMARY KEY, key INT, b INT)")
        .await;
    db.execute("CREATE TABLE prop_t3c (id INT PRIMARY KEY, key INT, c INT)")
        .await;

    let mut a_ids = TrackedIds::new();
    let mut b_ids = TrackedIds::new();
    let mut c_ids = TrackedIds::new();

    for _ in 0..10 {
        let id = a_ids.alloc();
        let key = rng.i32_range(1, 4);
        let v = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_t3a VALUES ({id}, {key}, {v})"))
            .await;

        let id = b_ids.alloc();
        let key = rng.i32_range(1, 4);
        let v = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_t3b VALUES ({id}, {key}, {v})"))
            .await;

        let id = c_ids.alloc();
        let key = rng.i32_range(1, 4);
        let v = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_t3c VALUES ({id}, {key}, {v})"))
            .await;
    }

    let query = "SELECT a.id AS aid, b.id AS bid, c.id AS cid, a.a + b.b + c.c AS total \
                 FROM prop_t3a a JOIN prop_t3b b ON a.key = b.key \
                                 JOIN prop_t3c c ON b.key = c.key";
    db.create_st("prop_t3_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_t3_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        // DML on table a
        let id = a_ids.alloc();
        let key = rng.i32_range(1, 4);
        let v = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_t3a VALUES ({id}, {key}, {v})"))
            .await;

        // DML on table b
        let id = b_ids.alloc();
        let key = rng.i32_range(1, 4);
        let v = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_t3b VALUES ({id}, {key}, {v})"))
            .await;

        if let Some(id) = a_ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_t3a WHERE id = {id}"))
                .await;
        }

        db.refresh_st("prop_t3_st").await;
        assert_invariant(&db, "prop_t3_st", query, seed, cycle).await;
    }
}

// ── Test 18: INTERSECT (AUTO falls back to FULL) ────────────────────────

/// A7 — INTERSECT set operation with differential maintenance.
#[tokio::test]
async fn test_property_intersect_differential() {
    let seed: u64 = 0xCAFE_0026;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE prop_int_a (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("CREATE TABLE prop_int_b (id INT PRIMARY KEY, val INT)")
        .await;

    let mut a_ids = TrackedIds::new();
    let mut b_ids = TrackedIds::new();

    // Insert initial data with overlapping val ranges to ensure non-empty INTERSECT
    for _ in 0..INITIAL_ROWS {
        let id = a_ids.alloc();
        let val = rng.i32_range(1, 8);
        db.execute(&format!("INSERT INTO prop_int_a VALUES ({id}, {val})"))
            .await;
    }
    for _ in 0..INITIAL_ROWS {
        let id = b_ids.alloc();
        let val = rng.i32_range(1, 8);
        db.execute(&format!("INSERT INTO prop_int_b VALUES ({id}, {val})"))
            .await;
    }

    let query = "SELECT val FROM prop_int_a INTERSECT SELECT val FROM prop_int_b";
    db.create_st("prop_int_st", query, "1m", "AUTO").await;
    assert_invariant(&db, "prop_int_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        let id = a_ids.alloc();
        let val = rng.i32_range(1, 8);
        db.execute(&format!("INSERT INTO prop_int_a VALUES ({id}, {val})"))
            .await;

        if rng.gen_bool() {
            let id = b_ids.alloc();
            let val = rng.i32_range(1, 8);
            db.execute(&format!("INSERT INTO prop_int_b VALUES ({id}, {val})"))
                .await;
        }
        if let Some(id) = a_ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_int_a WHERE id = {id}"))
                .await;
        }
        if let Some(id) = b_ids.remove_random(&mut rng) {
            db.execute(&format!("DELETE FROM prop_int_b WHERE id = {id}"))
                .await;
        }

        db.refresh_st("prop_int_st").await;
        assert_invariant(&db, "prop_int_st", query, seed, cycle).await;
    }
}

// ── Test 19: Composite PK (DIFFERENTIAL) ───────────────────────────────

/// A8 — Multi-column primary key table with differential maintenance.
#[tokio::test]
async fn test_property_composite_pk_differential() {
    let seed: u64 = 0xCAFE_0027;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE prop_cpk_src (\
         tenant_id INT, item_id INT, quantity INT, \
         PRIMARY KEY (tenant_id, item_id))",
    )
    .await;

    // Track composite keys as (tenant_id * 10000 + item_id) packed into i64
    let mut ids = TrackedIds::new();
    let mut used_keys: std::collections::HashSet<(i32, i32)> = std::collections::HashSet::new();

    for _ in 0..INITIAL_ROWS {
        let _id_unused = ids.alloc();
        let tenant = rng.i32_range(1, 4);
        let item = rng.i32_range(1, 20);
        if used_keys.insert((tenant, item)) {
            let qty = rng.i32_range(1, 100);
            db.execute(&format!(
                "INSERT INTO prop_cpk_src VALUES ({tenant}, {item}, {qty})"
            ))
            .await;
        }
    }

    let query = "SELECT tenant_id, item_id, quantity FROM prop_cpk_src";
    db.create_st("prop_cpk_st", query, "1m", "DIFFERENTIAL")
        .await;
    assert_invariant(&db, "prop_cpk_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        // Insert new unique composite key rows
        let n_ins = rng.usize_range(1, 3);
        for _ in 0..n_ins {
            let _id_unused = ids.alloc();
            let tenant = rng.i32_range(1, 4);
            let item = rng.i32_range(1, 30);
            if used_keys.insert((tenant, item)) {
                let qty = rng.i32_range(1, 100);
                db.execute(&format!(
                    "INSERT INTO prop_cpk_src VALUES ({tenant}, {item}, {qty})"
                ))
                .await;
            }
        }

        // Delete a random existing row
        if !used_keys.is_empty() && rng.gen_bool() {
            let keys: Vec<(i32, i32)> = used_keys.iter().copied().collect();
            let idx = rng.usize_range(0, keys.len().saturating_sub(1));
            let (t, i) = keys[idx];
            used_keys.remove(&(t, i));
            db.execute(&format!(
                "DELETE FROM prop_cpk_src WHERE tenant_id = {t} AND item_id = {i}"
            ))
            .await;
        }

        // Update a random existing row
        if !used_keys.is_empty() {
            let keys: Vec<(i32, i32)> = used_keys.iter().copied().collect();
            let idx = rng.usize_range(0, keys.len().saturating_sub(1));
            let (t, i) = keys[idx];
            let new_qty = rng.i32_range(1, 100);
            db.execute(&format!(
                "UPDATE prop_cpk_src SET quantity = {new_qty} \
                 WHERE tenant_id = {t} AND item_id = {i}"
            ))
            .await;
        }

        db.refresh_st("prop_cpk_st").await;
        assert_invariant(&db, "prop_cpk_st", query, seed, cycle).await;
    }
}

// ── Test 20: Recursive CTE (FULL mode) ────────────────────────────────

/// A9 — Recursive CTE is not differentiable; FULL mode must maintain the
/// invariant across DML cycles.
#[tokio::test]
async fn test_property_recursive_cte_full() {
    let seed: u64 = 0xCAFE_0028;
    let mut rng = Rng::new(seed);
    let db = E2eDb::new().await.with_extension().await;

    // Adjacency list for a tree/graph structure
    db.execute(
        "CREATE TABLE prop_rcte_nodes (\
         id INT PRIMARY KEY, parent_id INT, label TEXT)",
    )
    .await;

    let mut ids = TrackedIds::new();

    // Build initial tree: root node + children
    let root_id = ids.alloc();
    db.execute(&format!(
        "INSERT INTO prop_rcte_nodes VALUES ({root_id}, NULL, 'root')"
    ))
    .await;

    for _ in 0..INITIAL_ROWS {
        let id = ids.alloc();
        // Pick a random existing node as parent
        let parent = if let Some(p) = ids.pick(&mut rng) {
            p
        } else {
            root_id
        };
        let label = rng.choose(&["alpha", "beta", "gamma", "delta"]);
        db.execute(&format!(
            "INSERT INTO prop_rcte_nodes VALUES ({id}, {parent}, '{label}')"
        ))
        .await;
    }

    let query = "WITH RECURSIVE tree AS ( \
                   SELECT id, parent_id, label, 1 AS depth \
                   FROM prop_rcte_nodes WHERE parent_id IS NULL \
                   UNION ALL \
                   SELECT n.id, n.parent_id, n.label, t.depth + 1 \
                   FROM prop_rcte_nodes n JOIN tree t ON n.parent_id = t.id \
                 ) \
                 SELECT id, parent_id, label, depth FROM tree";
    db.create_st("prop_rcte_st", query, "1m", "FULL").await;
    assert_invariant(&db, "prop_rcte_st", query, seed, 0).await;

    for cycle in 1..=CYCLES {
        // Add new leaf nodes
        let n_ins = rng.usize_range(1, 3);
        for _ in 0..n_ins {
            let id = ids.alloc();
            let parent = if let Some(p) = ids.pick(&mut rng) {
                p
            } else {
                root_id
            };
            let label = rng.choose(&["alpha", "beta", "gamma", "delta"]);
            db.execute(&format!(
                "INSERT INTO prop_rcte_nodes VALUES ({id}, {parent}, '{label}')"
            ))
            .await;
        }

        // Delete a random non-root leaf (avoid cascading orphans by only
        // deleting nodes that have no children)
        if rng.gen_bool() {
            let leaf_opt: Option<i32> = db
                .query_scalar_opt(
                    "SELECT n.id FROM prop_rcte_nodes n \
                     WHERE n.parent_id IS NOT NULL \
                     AND NOT EXISTS ( \
                       SELECT 1 FROM prop_rcte_nodes c WHERE c.parent_id = n.id \
                     ) \
                     ORDER BY n.id LIMIT 1",
                )
                .await;
            if let Some(leaf_id) = leaf_opt {
                db.execute(&format!("DELETE FROM prop_rcte_nodes WHERE id = {leaf_id}"))
                    .await;
                // Remove from tracked set (best-effort — TrackedIds doesn't support
                // arbitrary removal by value, but the invariant check doesn't depend on it)
            }
        }

        db.refresh_st("prop_rcte_st").await;
        assert_invariant(&db, "prop_rcte_st", query, seed, cycle).await;
    }
}
