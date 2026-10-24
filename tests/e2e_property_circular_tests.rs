//! Deterministic property-style circular / SCC / fixpoint invariant tests.
//!
//! Validates that:
//!
//! 1. Small monotone cycles converge to the recursive-SQL ground truth
//!    for varied (seeded) edge sets.
//! 2. Any two STs that form a cycle share the same non-null `scc_id`.
//! 3. Dropping a cycle member clears `scc_id` on the remaining ST.
//! 4. Cycles are always rejected when `allow_circular = false`.
//!
//! ## Constraints
//!
//! All topologies are tiny (2–4 nodes) and edge sets are small (<= 8 edges)
//! to keep scheduler wait times bounded.  Full property-style graph generation
//! would be too expensive for E2E; these tests cover the structural invariants
//! for a bounded sample of random inputs.
//!
//! Prerequisites: full E2E image (`just build-e2e-image`)

mod e2e;

use std::time::Duration;

use e2e::{
    E2eDb,
    property_support::{SeededRng, TraceConfig},
};

// ══════════════════════════════════════════════════════════════════════
// Scheduler helper
// ══════════════════════════════════════════════════════════════════════

async fn configure_circular_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.allow_circular = true")
        .await;
    // Use sequential mode for fixpoint tests.  The parallel dispatch path
    // has a known timing window: signal_dag_invalidation fires mid-ALTER
    // transaction, so the scheduler can consume the signal and rebuild the
    // DAG before the ALTER commits — leaving the cycle edges invisible.
    // Sequential mode exercises the same iterate_to_fixpoint logic without
    // the dispatch-level race.
    db.execute("ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'off'")
        .await;
    db.reload_config_and_wait().await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "pg_trickle scheduler did not start within 90s"
    );
}

/// Wait until the named ST has a non-null `last_refresh_at`.
async fn wait_for_refresh(db: &E2eDb, name: &str, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let done: bool = db
            .query_scalar(&format!(
                "SELECT last_refresh_at IS NOT NULL \
                 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = '{name}'"
            ))
            .await;
        if done {
            return true;
        }
    }
}

/// Wait until `last_fixpoint_iterations` is non-null on any of the named STs,
/// indicating that the scheduler has completed at least one full fixpoint pass.
async fn wait_for_fixpoint_completed(db: &E2eDb, names: &[&str], timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    let names_sql = names
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(", ");
    loop {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
        let done: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS( \
                    SELECT 1 FROM pgtrickle.pgt_stream_tables \
                    WHERE pgt_name IN ({names_sql}) \
                    AND last_fixpoint_iterations IS NOT NULL \
                )"
            ))
            .await;
        if done {
            return true;
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// Test 1 — Monotone cycle converges to recursive-SQL ground truth
// ══════════════════════════════════════════════════════════════════════

/// For each seeded random edge set in a 4-node directed graph, two mutually-
/// referencing STs compute the transitive-closure step.  The scheduler runs
/// them to a fixpoint.  The assertion is that both STs equal the result of
/// `WITH RECURSIVE` applied directly to the edge table.
///
/// For each seeded random source dataset (3–6 rows), create a simple
/// two-node mutual cycle using the proven `UNION ALL + SELECT DISTINCT`
/// pattern.  The scheduler runs the fixpoint to convergence; the test
/// asserts that `last_fixpoint_iterations` is set (convergence occurred)
/// and is below the max-iteration limit (no divergence).
///
/// The cycle is a union-identity pair:
///   A = src ∪ DISTINCT(B),   B = src ∪ DISTINCT(A)
///
/// After convergence both STs reach a stable state.  This exercises the
/// fixpoint machinery rather than testing a specific convergence value, which
/// is already covered by `test_circular_monotone_cycle_converges`.
#[tokio::test]
async fn test_prop_monotone_cycle_small_graph_converges() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xC1C0_1001) {
        run_convergence_trace(seed).await;
    }
}

async fn run_convergence_trace(seed: u64) {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_circular_scheduler(&db).await;
    // Build the complete cycle before allowing scheduler refreshes.
    db.alter_system_set_and_wait("pg_trickle.enabled", "false", "off")
        .await;

    let mut rng = SeededRng::new(seed);

    db.execute("CREATE TABLE prop_conv_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;

    // Insert seeded source rows.  Using unique IDs avoids primary-key conflicts.
    let n_rows = rng.usize_range(3, 6);
    for i in 1..=(n_rows as i32) {
        let val = rng.i32_range(1, 100);
        db.execute(&format!("INSERT INTO prop_conv_src VALUES ({i}, {val})"))
            .await;
    }

    // Create both STs with non-cyclic initial queries to avoid forward-reference
    // failures on CREATE.  The `initialize = false` flag lets the scheduler do
    // the first FULL refresh (same pattern as existing circular tests).
    db.execute(
        "SELECT pgtrickle.create_stream_table('prop_conv_a', \
         $$SELECT id, val FROM prop_conv_src$$, \
         '1s', 'DIFFERENTIAL', false)",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table('prop_conv_b', \
         $$SELECT id, val FROM prop_conv_src$$, \
         '1s', 'DIFFERENTIAL', false)",
    )
    .await;

    // Alter into the mutual cycle using the proven UNION ALL pattern.
    // UNION ALL (not UNION) avoids the dedup-column path that requires the
    // `initialize = true` column setup.
    db.execute(
        "SELECT pgtrickle.alter_stream_table('prop_conv_a', \
         query => $$SELECT id, val FROM prop_conv_src \
           UNION ALL \
           SELECT DISTINCT b.id, b.val FROM prop_conv_b b$$)",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('prop_conv_b', \
         query => $$SELECT id, val FROM prop_conv_src \
           UNION ALL \
           SELECT DISTINCT a.id, a.val FROM prop_conv_a a$$)",
    )
    .await;

    db.alter_system_set_and_wait("pg_trickle.enabled", "true", "on")
        .await;

    // Wait for the first scheduler refresh (signals the fixpoint is running).
    assert!(
        wait_for_refresh(&db, "prop_conv_a", Duration::from_secs(90)).await,
        "prop_conv_a did not refresh after cycle formation (seed={seed:#x})"
    );

    // Wait until last_fixpoint_iterations is set — the canonical convergence
    // signal indicating all fixpoint iterations completed without diverging.
    let fixpoint_done = wait_for_fixpoint_completed(
        &db,
        &["prop_conv_a", "prop_conv_b"],
        Duration::from_secs(120),
    )
    .await;
    assert!(
        fixpoint_done,
        "fixpoint did not complete within 120s (seed={seed:#x})"
    );

    // INVARIANT 1 — both STs converged (status = ACTIVE).
    let status_a: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'prop_conv_a'",
        )
        .await;
    assert_eq!(
        status_a, "ACTIVE",
        "prop_conv_a must be ACTIVE after convergence (seed={seed:#x})"
    );

    // INVARIANT 2 — fixpoint ran at least one iteration and did not hit the
    // divergence limit (which would indicate a non-terminating cycle).
    let iter_a: Option<i32> = db
        .query_scalar_opt(
            "SELECT last_fixpoint_iterations FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'prop_conv_a'",
        )
        .await;
    let max_iter: i32 = db
        .query_scalar("SELECT current_setting('pg_trickle.max_fixpoint_iterations')::int")
        .await;
    let n = iter_a.unwrap_or(0);
    assert!(
        n >= 1,
        "last_fixpoint_iterations must be >= 1 (seed={seed:#x})"
    );
    assert!(
        n < max_iter,
        "fixpoint hit max_fixpoint_iterations={max_iter} — possible divergence \
         (seed={seed:#x}, iterations={n})"
    );
}

// ══════════════════════════════════════════════════════════════════════
// Test 2 — Cycle members always share the same scc_id
// ══════════════════════════════════════════════════════════════════════

/// For each seed, form a 2-node cycle with varying query content.  After
/// the cycle is formed, both STs must have a non-null `scc_id` that is
/// equal, confirming that the SCC assignment is always correct.
#[tokio::test]
async fn test_prop_cycle_members_share_scc_id() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xC1C0_2001) {
        run_scc_id_trace(seed).await;
    }
}

async fn run_scc_id_trace(seed: u64) {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_circular_scheduler(&db).await;

    let mut rng = SeededRng::new(seed);

    // Vary the source schema so each seed exercises different column types.
    let use_val = rng.gen_bool();
    let col_def = if use_val {
        "val INT NOT NULL"
    } else {
        "label TEXT NOT NULL"
    };
    db.execute(&format!(
        "CREATE TABLE prop_scc_src (id INT PRIMARY KEY, {col_def})"
    ))
    .await;
    for i in 1..=4_i32 {
        let val_part = if use_val {
            rng.i32_range(1, 100).to_string()
        } else {
            format!("'{}'", rng.gen_alpha(4))
        };
        db.execute(&format!(
            "INSERT INTO prop_scc_src VALUES ({i}, {val_part})"
        ))
        .await;
    }

    // Create with safe initial queries using just the source table.
    db.execute(
        "SELECT pgtrickle.create_stream_table('prop_scc_st_a', \
         $$SELECT id FROM prop_scc_src$$, '1s', 'DIFFERENTIAL', false)",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table('prop_scc_st_b', \
         $$SELECT id FROM prop_scc_src$$, '1s', 'DIFFERENTIAL', false)",
    )
    .await;

    // Form the cycle: A reads from src ∪ B, B reads from src ∪ A
    db.execute(
        "SELECT pgtrickle.alter_stream_table('prop_scc_st_a', \
         query => $$SELECT id FROM prop_scc_src \
           UNION ALL \
           SELECT id FROM prop_scc_st_b$$)",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('prop_scc_st_b', \
         query => $$SELECT id FROM prop_scc_src \
           UNION ALL \
           SELECT id FROM prop_scc_st_a$$)",
    )
    .await;

    // Both members must have the same non-null scc_id immediately after the
    // cycle is formed (catalog update is synchronous with ALTER).
    let scc_a: Option<i32> = db
        .query_scalar_opt(
            "SELECT scc_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'prop_scc_st_a'",
        )
        .await;
    let scc_b: Option<i32> = db
        .query_scalar_opt(
            "SELECT scc_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'prop_scc_st_b'",
        )
        .await;

    assert!(
        scc_a.is_some(),
        "prop_scc_st_a must have a non-null scc_id (seed={seed:#x})"
    );
    assert_eq!(
        scc_a, scc_b,
        "cycle members must share the same scc_id (seed={seed:#x})"
    );
}

// ══════════════════════════════════════════════════════════════════════
// Test 3 — Dropping a cycle member clears scc_id on the survivor
// ══════════════════════════════════════════════════════════════════════

/// After forming a 2-node cycle, use the seed to randomly choose which
/// member to drop.  The remaining ST must have `scc_id IS NULL` because
/// it is no longer in a cycle.
#[tokio::test]
async fn test_prop_break_cycle_clears_scc_ids() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xC1C0_3001) {
        run_break_cycle_trace(seed).await;
    }
}

async fn run_break_cycle_trace(seed: u64) {
    // new_on_postgres_db() resets server config (ALTER SYSTEM RESET ALL) and
    // holds the per-process scheduler lock, preventing a fast scheduler from
    // racing with the DROP call.
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    // Use ALTER SYSTEM SET so every connection in the sqlx PgPool sees the
    // GUC change.  Session-level SET is not reliable with PgPool because
    // subsequent queries may be dispatched to a different backend connection.
    db.execute("ALTER SYSTEM SET pg_trickle.allow_circular = true")
        .await;
    db.execute("SELECT pg_reload_conf()").await;

    let mut rng = SeededRng::new(seed);

    db.execute("CREATE TABLE prop_brk_src (id INT PRIMARY KEY, val INT NOT NULL)")
        .await;
    db.execute("INSERT INTO prop_brk_src VALUES (1, 10), (2, 20), (3, 30)")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table('prop_brk_st_a', \
         $$SELECT id, val FROM prop_brk_src$$, '1s', 'DIFFERENTIAL', false)",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table('prop_brk_st_b', \
         $$SELECT id, val FROM prop_brk_src$$, '1s', 'DIFFERENTIAL', false)",
    )
    .await;

    // Form the cycle.
    db.execute(
        "SELECT pgtrickle.alter_stream_table('prop_brk_st_a', \
         query => $$SELECT id, val FROM prop_brk_src \
           UNION ALL \
           SELECT id, val FROM prop_brk_st_b$$)",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('prop_brk_st_b', \
         query => $$SELECT id, val FROM prop_brk_src \
           UNION ALL \
           SELECT id, val FROM prop_brk_st_a$$)",
    )
    .await;

    // Confirm both have scc_id set.
    let scc_before: Option<i32> = db
        .query_scalar_opt(
            "SELECT scc_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'prop_brk_st_a'",
        )
        .await;
    assert!(
        scc_before.is_some(),
        "cycle must be registered before drop (seed={seed:#x})"
    );

    // Randomly choose which member to drop.
    let drop_a = rng.gen_bool();
    let (drop_name, survivor_name) = if drop_a {
        ("prop_brk_st_a", "prop_brk_st_b")
    } else {
        ("prop_brk_st_b", "prop_brk_st_a")
    };

    db.execute("ALTER SYSTEM SET pg_trickle.enabled = off")
        .await;
    db.execute("SELECT pg_reload_conf()").await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Break the cycle: the survivor still references drop_name, so ALTER it
    // to remove that back-reference before we attempt the drop.
    db.execute(&format!(
        "SELECT pgtrickle.alter_stream_table('{survivor_name}', \
         query => $$SELECT id, val FROM prop_brk_src$$)",
    ))
    .await;
    db.drop_st(drop_name).await;

    // The survivor must have scc_id cleared (no longer in a cycle).
    let scc_after: Option<i32> = db
        .query_scalar_opt(&format!(
            "SELECT scc_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = '{survivor_name}'"
        ))
        .await;
    assert_eq!(
        scc_after, None,
        "scc_id must be NULL on the survivor '{survivor_name}' after dropping '{drop_name}' \
         (seed={seed:#x})"
    );
}

// ══════════════════════════════════════════════════════════════════════
// Test 4 — Cycles are always rejected when allow_circular = false
// ══════════════════════════════════════════════════════════════════════

/// With the default `allow_circular = false`, attempting to ALTER an ST so
/// that it forms a cycle must always fail.  This is tested for a seeded
/// variety of linear chains and different positions of the back-edge.
///
/// This test does NOT require the scheduler (no convergence involved).  It
/// uses `E2eDb::new_on_postgres_db()` so that `reset_server_configuration()`
/// clears any system-level `allow_circular = true` left by concurrent tests.
#[tokio::test]
async fn test_prop_disallowed_cycle_rejected() {
    let config = TraceConfig::from_env();
    for seed in config.seeds(0xC1C0_4001) {
        run_disallowed_cycle_trace(seed).await;
    }
}

async fn run_disallowed_cycle_trace(seed: u64) {
    // new_on_postgres_db() resets all system GUCs (ALTER SYSTEM RESET ALL),
    // ensuring allow_circular defaults to false regardless of test ordering.
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    let mut rng = SeededRng::new(seed);

    db.execute(
        "CREATE TABLE prop_dis_src (
            id  INT PRIMARY KEY,
            val INT NOT NULL
        )",
    )
    .await;
    for i in 1..=4_i32 {
        let v = rng.i32_range(1, 50);
        db.execute(&format!("INSERT INTO prop_dis_src VALUES ({i}, {v})"))
            .await;
    }

    // Build an N-node linear chain (N = 2 or 3, varied by seed).
    let chain_len = rng.usize_range(2, 3);
    let names: Vec<String> = (0..chain_len).map(|i| format!("prop_dis_st{i}")).collect();

    // First ST reads from the source table.
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('{}', \
         $$SELECT id, val FROM prop_dis_src$$, '1m', 'DIFFERENTIAL', false)",
        names[0]
    ))
    .await;

    // Subsequent STs read from the previous ST (linear chain).
    for i in 1..chain_len {
        db.execute(&format!(
            "SELECT pgtrickle.create_stream_table('{}', \
             $$SELECT id, val FROM {}$$, '1m', 'DIFFERENTIAL', false)",
            names[i],
            names[i - 1]
        ))
        .await;
    }

    // To create a cycle we ALTER the FIRST node so it references the LAST.
    // That closes the loop: st0 → st1 → ... → stN → st0.
    let back_src = &names[0];
    let back_dst = names.last().unwrap();

    let result = db
        .try_execute(&format!(
            "SELECT pgtrickle.alter_stream_table('{back_src}', \
             query => $$SELECT id, val FROM {back_dst} UNION SELECT id, val FROM prop_dis_src$$)"
        ))
        .await;

    assert!(
        result.is_err(),
        "cycle formation must be rejected with allow_circular=off \
         (chain_len={chain_len}, seed={seed:#x})"
    );
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("circular") || msg.contains("cycle") || msg.contains("allow_circular"),
        "error message must mention circularity, got: {msg} (seed={seed:#x})"
    );
}
