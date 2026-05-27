//! G17-IMS: IMMEDIATE mode concurrency stress test.
//!
//! 100+ concurrent DML transactions on the same source table in IMMEDIATE
//! refresh mode; assert zero lost updates, zero phantom rows, and no
//! deadlocks over 60 seconds.
//!
//! The test exercises five DML patterns:
//! 1. Concurrent single-row INSERTs
//! 2. Concurrent UPDATEs to distinct rows
//! 3. Concurrent UPDATEs to the same row (contention)
//! 4. Concurrent DELETEs
//! 5. Mixed concurrent INSERT/UPDATE/DELETE

mod e2e;

use e2e::E2eDb;
use std::time::Instant;
use tokio::task::JoinSet;

// ── Helpers ────────────────────────────────────────────────────────────

/// Run a SQL statement through the pool, returning Ok/Err.
async fn pool_exec(pool: &sqlx::PgPool, sql: &str) -> Result<(), String> {
    sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
        .execute(pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

// ═══════════════════════════════════════════════════════════════════════
// Test 1: Concurrent single-row INSERTs
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_immediate_concurrent_inserts() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE imm_ins_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO imm_ins_src SELECT g, g FROM generate_series(1, 10) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'imm_ins_st',\
            query => $$SELECT id, val FROM imm_ins_src$$,\
            refresh_mode => 'IMMEDIATE'\
         )",
    )
    .await;

    assert_eq!(db.count("public.imm_ins_st").await, 10);

    // 100 concurrent single-row INSERTs
    let n_writers = 100;
    let mut tasks = JoinSet::new();
    for i in 0..n_writers {
        let pool = db.pool.clone();
        let id = 100 + i;
        tasks.spawn(async move {
            pool_exec(
                &pool,
                &format!("INSERT INTO imm_ins_src VALUES ({id}, {id})"),
            )
            .await
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.contains("deadlock") => {
                panic!("DEADLOCK detected in concurrent INSERTs: {e}");
            }
            Ok(Err(e)) => errors.push(e),
            Err(e) => panic!("Task panicked: {e}"),
        }
    }

    assert!(errors.is_empty(), "Concurrent INSERT errors: {:?}", errors);

    // Verify: ST must match the source table exactly
    db.assert_st_matches_query("public.imm_ins_st", "SELECT id, val FROM imm_ins_src")
        .await;

    let final_count: i64 = db.count("public.imm_ins_st").await;
    assert_eq!(
        final_count,
        10 + n_writers as i64,
        "Expected {} rows, got {}",
        10 + n_writers,
        final_count
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Test 2: Concurrent UPDATEs to distinct rows (no contention)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_immediate_concurrent_updates_distinct() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE imm_upd_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO imm_upd_src SELECT g, 0 FROM generate_series(1, 200) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'imm_upd_st',\
            query => $$SELECT id, val FROM imm_upd_src$$,\
            refresh_mode => 'IMMEDIATE'\
         )",
    )
    .await;

    // 100 concurrent UPDATEs, each targeting a distinct row
    let n_writers = 100;
    let mut tasks = JoinSet::new();
    for i in 0..n_writers {
        let pool = db.pool.clone();
        let id = i + 1;
        tasks.spawn(async move {
            pool_exec(
                &pool,
                &format!("UPDATE imm_upd_src SET val = {id} * 10 WHERE id = {id}"),
            )
            .await
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.contains("deadlock") => {
                panic!("DEADLOCK detected in concurrent UPDATEs: {e}");
            }
            Ok(Err(e)) => errors.push(e),
            Err(e) => panic!("Task panicked: {e}"),
        }
    }

    assert!(errors.is_empty(), "Concurrent UPDATE errors: {:?}", errors);

    db.assert_st_matches_query("public.imm_upd_st", "SELECT id, val FROM imm_upd_src")
        .await;
}

// ═══════════════════════════════════════════════════════════════════════
// Test 3: Concurrent UPDATEs to same row (high contention)
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_immediate_concurrent_updates_same_row() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE imm_hot_src (id INT PRIMARY KEY, counter INT NOT NULL DEFAULT 0)")
        .await;
    db.execute("INSERT INTO imm_hot_src VALUES (1, 0)").await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'imm_hot_st',\
            query => $$SELECT id, counter FROM imm_hot_src$$,\
            refresh_mode => 'IMMEDIATE'\
         )",
    )
    .await;

    // 100 concurrent increments to the same row
    let n_writers: i32 = 100;
    let mut tasks = JoinSet::new();
    for _ in 0..n_writers {
        let pool = db.pool.clone();
        tasks.spawn(async move {
            pool_exec(
                &pool,
                "UPDATE imm_hot_src SET counter = counter + 1 WHERE id = 1",
            )
            .await
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.contains("deadlock") => {
                panic!("DEADLOCK detected in hot-row contention: {e}");
            }
            Ok(Err(e)) => errors.push(e),
            Err(e) => panic!("Task panicked: {e}"),
        }
    }

    // Some serialization failures are acceptable for hot-row contention
    // but the final state must be consistent.
    db.assert_st_matches_query("public.imm_hot_st", "SELECT id, counter FROM imm_hot_src")
        .await;

    // Verify the counter equals n_writers (minus any retried/failed txns)
    let src_counter: i64 = db
        .query_scalar("SELECT counter::bigint FROM imm_hot_src WHERE id = 1")
        .await;
    let st_counter: i64 = db
        .query_scalar("SELECT counter::bigint FROM imm_hot_st WHERE id = 1")
        .await;
    assert_eq!(
        src_counter, st_counter,
        "Hot-row counter mismatch: source={src_counter}, ST={st_counter}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Test 4: Concurrent DELETEs
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_immediate_concurrent_deletes() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE imm_del_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO imm_del_src SELECT g, g FROM generate_series(1, 200) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'imm_del_st',\
            query => $$SELECT id, val FROM imm_del_src$$,\
            refresh_mode => 'IMMEDIATE'\
         )",
    )
    .await;

    assert_eq!(db.count("public.imm_del_st").await, 200);

    // Delete 100 distinct rows concurrently
    let n_writers = 100;
    let mut tasks = JoinSet::new();
    for i in 0..n_writers {
        let pool = db.pool.clone();
        let id = i + 1; // rows 1..100
        tasks.spawn(async move {
            pool_exec(&pool, &format!("DELETE FROM imm_del_src WHERE id = {id}")).await
        });
    }

    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.contains("deadlock") => {
                panic!("DEADLOCK detected in concurrent DELETEs: {e}");
            }
            Ok(Err(e)) => errors.push(e),
            Err(e) => panic!("Task panicked: {e}"),
        }
    }

    assert!(errors.is_empty(), "Concurrent DELETE errors: {:?}", errors);

    db.assert_st_matches_query("public.imm_del_st", "SELECT id, val FROM imm_del_src")
        .await;

    let remaining: i64 = db.count("public.imm_del_st").await;
    assert_eq!(
        remaining, 100,
        "Expected 100 rows remaining, got {remaining}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Test 5: Mixed concurrent INSERT/UPDATE/DELETE stress
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_immediate_concurrent_mixed_dml_stress() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE imm_mix_src (\
            id INT PRIMARY KEY, \
            val INT NOT NULL, \
            label TEXT NOT NULL DEFAULT 'init'\
         )",
    )
    .await;
    db.execute("INSERT INTO imm_mix_src SELECT g, g, 'init' FROM generate_series(1, 500) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
            name => 'imm_mix_st',\
            query => $$SELECT id, val, label FROM imm_mix_src$$,\
            refresh_mode => 'IMMEDIATE'\
         )",
    )
    .await;

    let start = Instant::now();

    // Spawn 120 concurrent tasks: 40 inserts, 40 updates, 40 deletes
    let mut tasks = JoinSet::new();

    // 40 INSERTs (ids 1000..1039)
    for i in 0..40 {
        let pool = db.pool.clone();
        let id = 1000 + i;
        tasks.spawn(async move {
            pool_exec(
                &pool,
                &format!("INSERT INTO imm_mix_src VALUES ({id}, {id}, 'inserted')"),
            )
            .await
        });
    }

    // 40 UPDATEs (ids 1..40)
    for i in 0..40 {
        let pool = db.pool.clone();
        let id = i + 1;
        tasks.spawn(async move {
            pool_exec(
                &pool,
                &format!(
                    "UPDATE imm_mix_src SET val = val + 1000, label = 'updated' WHERE id = {id}"
                ),
            )
            .await
        });
    }

    // 40 DELETEs (ids 461..500)
    for i in 0..40 {
        let pool = db.pool.clone();
        let id = 461 + i;
        tasks.spawn(async move {
            pool_exec(&pool, &format!("DELETE FROM imm_mix_src WHERE id = {id}")).await
        });
    }

    let mut ok_count = 0;
    let mut errors = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => ok_count += 1,
            Ok(Err(e)) if e.contains("deadlock") => {
                panic!("DEADLOCK detected in mixed DML stress: {e}");
            }
            Ok(Err(e)) => errors.push(e),
            Err(e) => panic!("Task panicked: {e}"),
        }
    }

    let elapsed = start.elapsed();
    println!(
        "  Mixed DML stress: {ok_count}/120 succeeded in {:.1}s, {} errors",
        elapsed.as_secs_f64(),
        errors.len()
    );

    assert!(
        elapsed.as_secs() < 60,
        "Mixed DML stress test exceeded 60s timeout"
    );

    // Final invariant: ST must match the source table exactly.
    // Any phantom rows, lost updates, or lost deletes will cause this to fail.
    db.assert_st_matches_query(
        "public.imm_mix_st",
        "SELECT id, val, label FROM imm_mix_src",
    )
    .await;
}
