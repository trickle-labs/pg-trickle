//! v0.89 ROW_NUMBER candidate correctness and measured rejection.

mod e2e;

use e2e::E2eDb;
use serde::Serialize;
use std::time::Instant;

async fn assert_partition_recompute_refresh(db: &E2eDb, pgt_id: i64) {
    db.refresh_st("wi_st").await;
    db.assert_st_matches_query(
        "public.wi_st",
        "SELECT id, value, row_number() OVER (ORDER BY value, id) AS rn FROM public.wi_source",
    )
    .await;

    let (action, reason, detail, full_fallback): (String, Option<String>, Option<String>, bool) =
        sqlx::query_as(
            "SELECT action, refresh_reason, refresh_reason_detail, was_full_fallback \
         FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = $1 AND status = 'COMPLETED' \
         ORDER BY refresh_id DESC LIMIT 1",
        )
        .bind(pgt_id)
        .fetch_one(&db.pool)
        .await
        .expect("load ROW_NUMBER refresh evidence");
    assert_eq!(action, "DIFFERENTIAL");
    assert_eq!(reason.as_deref(), Some("WINDOW_RECOMPUTE_CHEAPER"));
    let detail: serde_json::Value =
        serde_json::from_str(detail.as_deref().expect("ROW_NUMBER fallback detail"))
            .expect("valid ROW_NUMBER fallback detail");
    assert_eq!(detail["strategy"], "partition_recompute");
    assert!(!full_fallback);
}

#[tokio::test]
async fn test_row_number_rejected_candidate_converges_via_partition_recompute() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE wi_source (id bigint PRIMARY KEY, value bigint NOT NULL)")
        .await;
    db.execute(
        "INSERT INTO wi_source \
         SELECT value, value FROM generate_series(1, 20000) AS value",
    )
    .await;
    db.create_st(
        "wi_st",
        "SELECT id, value, row_number() OVER (ORDER BY value, id) AS rn FROM public.wi_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'wi_st'")
        .await;
    db.execute("INSERT INTO wi_source VALUES (20001, 20001)")
        .await;
    assert_partition_recompute_refresh(&db, pgt_id).await;

    db.execute("INSERT INTO wi_source VALUES (0, 0)").await;
    assert_partition_recompute_refresh(&db, pgt_id).await;

    db.execute("UPDATE wi_source SET value = 10000 WHERE id = 20001")
        .await;
    assert_partition_recompute_refresh(&db, pgt_id).await;

    db.execute("DELETE FROM wi_source WHERE id = 10000").await;
    assert_partition_recompute_refresh(&db, pgt_id).await;

    let explanation: serde_json::Value = db
        .query_scalar("SELECT pgtrickle.explain_json('public.wi_st')")
        .await;
    assert_eq!(
        explanation["window"]["last_actual_strategy"],
        "partition_recompute"
    );
}

#[derive(Serialize)]
struct WindowBenchSample {
    partition_rows: i64,
    shape: &'static str,
    strategy: String,
    repetition: usize,
    elapsed_ms: f64,
    wal_bytes: i64,
    output_rows: i64,
}

async fn measured_refresh(
    db: &E2eDb,
    st_name: &str,
    pgt_id: i64,
    partition_rows: i64,
    shape: &'static str,
    expected_strategy: &'static str,
    repetition: usize,
) -> WindowBenchSample {
    let before: String = db.query_scalar("SELECT pg_current_wal_lsn()::text").await;
    let started = Instant::now();
    db.refresh_st(st_name).await;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let (wal_bytes, output_rows, reason): (i64, i64, Option<String>) = sqlx::query_as(
        "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint, \
                COALESCE(rows_inserted, 0) + COALESCE(rows_updated, 0) \
                    + COALESCE(rows_deleted, 0), refresh_reason \
         FROM pgtrickle.pgt_refresh_history \
         WHERE pgt_id = $2 AND status = 'COMPLETED' \
         ORDER BY refresh_id DESC LIMIT 1",
    )
    .bind(before)
    .bind(pgt_id)
    .fetch_one(&db.pool)
    .await
    .expect("load benchmark refresh evidence");
    let strategy = if reason.is_none() {
        "state_backed"
    } else {
        assert_eq!(reason.as_deref(), Some("WINDOW_RECOMPUTE_CHEAPER"));
        "partition_recompute"
    };
    assert_eq!(strategy, expected_strategy);
    WindowBenchSample {
        partition_rows,
        shape,
        strategy: strategy.to_string(),
        repetition,
        elapsed_ms,
        wal_bytes,
        output_rows,
    }
}

/// Reproducible v0.89 admission matrix. Run with `--ignored --nocapture`.
#[tokio::test]
#[ignore]
async fn bench_row_number_state_vs_partition_recompute() {
    let db = E2eDb::new().await.with_extension().await;
    let mut samples = Vec::new();

    for partition_rows in [1_000_i64, 10_000, 100_000] {
        let runtime_source = format!("wb_runtime_{partition_rows}");
        let baseline_source = format!("wb_baseline_{partition_rows}");
        let runtime_st = format!("wb_runtime_st_{partition_rows}");
        let baseline_st = format!("wb_baseline_st_{partition_rows}");
        db.execute(&format!(
            "CREATE TABLE {runtime_source} (id bigint PRIMARY KEY, value bigint NOT NULL)"
        ))
        .await;
        db.execute(&format!(
            "CREATE TABLE {baseline_source} (LIKE {runtime_source} INCLUDING ALL)"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {runtime_source} SELECT n, n FROM generate_series(1, {partition_rows}) n"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {baseline_source} SELECT * FROM {runtime_source}"
        ))
        .await;
        db.create_st(
            &runtime_st,
            &format!(
                "SELECT id, value, row_number() OVER (ORDER BY value, id) AS rn FROM public.{runtime_source}"
            ),
            "1m",
            "DIFFERENTIAL",
        )
        .await;
        db.create_st(
            &baseline_st,
            &format!(
                "SELECT id, value, row_number() OVER (ORDER BY value, id) AS rn FROM public.{baseline_source}"
            ),
            "1m",
            "DIFFERENTIAL",
        )
        .await;
        let runtime_id: i64 = sqlx::query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1",
        )
        .bind(&runtime_st)
        .fetch_one(&db.pool)
        .await
        .expect("load runtime benchmark pgt_id");
        let baseline_id: i64 = sqlx::query_scalar(
            "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1",
        )
        .bind(&baseline_st)
        .fetch_one(&db.pool)
        .await
        .expect("load baseline benchmark pgt_id");
        sqlx::query(
            "UPDATE pgtrickle.pgt_stream_tables SET window_strategy = \
                 jsonb_set(jsonb_set(jsonb_set(window_strategy, \
                     '{nodes,0,functions,0,strategy}', '\"ordered_suffix\"'), \
                     '{nodes,0,functions,0,runtime_enabled}', 'true'), \
                     '{nodes,0,functions,0,fallback_reason}', 'null'), \
                 needs_reinit = true WHERE pgt_id = $1",
        )
        .bind(runtime_id)
        .execute(&db.pool)
        .await
        .expect("enable benchmark-only ROW_NUMBER candidate");
        db.refresh_st(&runtime_st).await;
        sqlx::query(
            "UPDATE pgtrickle.pgt_stream_tables SET window_strategy = \
                 jsonb_set(jsonb_set(window_strategy, \
                     '{nodes,0,functions,0,runtime_enabled}', 'false'), \
                     '{nodes,0,functions,0,fallback_reason}', \
                     to_jsonb('WINDOW_RECOMPUTE_CHEAPER'::text)) WHERE pgt_id = $1",
        )
        .bind(baseline_id)
        .execute(&db.pool)
        .await
        .expect("force benchmark baseline to partition recompute");

        for (shape, start) in [("tail_insert", partition_rows + 1), ("front_insert", -1)] {
            for repetition in 0..6 {
                let value = if shape == "tail_insert" {
                    start + repetition as i64
                } else {
                    start - repetition as i64
                };
                db.execute(&format!(
                    "INSERT INTO {runtime_source} VALUES ({value}, {value})"
                ))
                .await;
                db.execute(&format!(
                    "INSERT INTO {baseline_source} VALUES ({value}, {value})"
                ))
                .await;
                let runtime = measured_refresh(
                    &db,
                    &runtime_st,
                    runtime_id,
                    partition_rows,
                    shape,
                    "state_backed",
                    repetition,
                )
                .await;
                let baseline = measured_refresh(
                    &db,
                    &baseline_st,
                    baseline_id,
                    partition_rows,
                    shape,
                    "partition_recompute",
                    repetition,
                )
                .await;
                db.assert_st_matches_query(
                    &format!("public.{runtime_st}"),
                    &format!(
                        "SELECT id, value, row_number() OVER (ORDER BY value, id) AS rn FROM public.{runtime_source}"
                    ),
                )
                .await;
                db.assert_st_matches_query(
                    &format!("public.{baseline_st}"),
                    &format!(
                        "SELECT id, value, row_number() OVER (ORDER BY value, id) AS rn FROM public.{baseline_source}"
                    ),
                )
                .await;
                if repetition > 0 {
                    samples.extend([runtime, baseline]);
                }
            }
        }
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&samples).expect("serialize benchmark samples")
    );
}
