//! F4 (v0.37.0): E2E tests for pgVectorMV — incremental vector aggregate operators.
//!
//! Validates that stream tables computing `avg(embedding)` and `sum(embedding)`
//! over pgvector `vector` typed columns produce correct results after INSERT,
//! UPDATE, and DELETE operations (correctness vs FULL refresh).
//!
//! Uses the group-rescan strategy: affected groups are re-aggregated from source
//! data using pgvector's native `avg(vector)` and `sum(vector)` aggregates.
//!
//! Prerequisites: `./tests/build_e2e_image.sh` (includes pgvector)

mod e2e;

use e2e::E2eDb;

// ═══════════════════════════════════════════════════════════════════════
// Helper: enable pgvector and the vector agg GUC
// ═══════════════════════════════════════════════════════════════════════

async fn setup_pgvector(db: &E2eDb) {
    db.execute("CREATE EXTENSION IF NOT EXISTS vector").await;
}

async fn wait_for_ann_worker_exit(db: &E2eDb, id: i64) -> serde_json::Value {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let status: serde_json::Value =
                sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
                    .bind(id)
                    .fetch_one(&db.pool)
                    .await
                    .expect("inspect disconnected worker");
            if status["state"] != "RUNNING" {
                return status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("worker backend did not exit")
}

#[tokio::test]
async fn test_vector_aggregate_mode_change_uses_saved_path_and_admission_setting() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db.pool.acquire().await.expect("vector mode session");
    for statement in [
        "CREATE SCHEMA vector_mode_context",
        "CREATE TABLE vector_mode_context.source (id int PRIMARY KEY, grp int, v public.vector(2))",
        "INSERT INTO vector_mode_context.source VALUES (1, 1, '[1,2]'), (2, 1, '[3,4]')",
        "SET search_path = vector_mode_context, public",
        "SELECT pgtrickle.create_stream_table('public.vector_mode_st', 'SELECT grp, avg(v) AS centroid FROM source GROUP BY grp', '1h', 'FULL')",
        "RESET search_path",
        "SET pg_trickle.enable_vector_agg = off",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("prepare vector mode change");
    }
    let alter = "SELECT pgtrickle.alter_stream_table('public.vector_mode_st', refresh_mode => 'DIFFERENTIAL')";
    let error = sqlx::query(alter)
        .execute(&mut *conn)
        .await
        .expect_err("disabled vector aggregates must be rejected");
    assert!(error.to_string().contains("enable_vector_agg"), "{error}");
    sqlx::query("SET pg_trickle.enable_vector_agg = on")
        .execute(&mut *conn)
        .await
        .expect("enable vector aggregates");
    sqlx::query(alter)
        .execute(&mut *conn)
        .await
        .expect("alter with saved search path");
    sqlx::query("INSERT INTO vector_mode_context.source VALUES (3, 1, '[5,6]')")
        .execute(&mut *conn)
        .await
        .expect("mutate vector source");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('public.vector_mode_st')")
        .execute(&mut *conn)
        .await
        .expect("refresh vector stream");
    let correct: bool = sqlx::query_scalar(
        "SELECT centroid::text = '[3,4]' FROM public.vector_mode_st WHERE grp = 1",
    )
    .fetch_one(&mut *conn)
    .await
    .expect("inspect centroid");
    assert!(correct);
}

#[tokio::test]
async fn test_centroid_tutorial_sql_runs_on_installed_extension() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let tutorial = include_str!("../docs/tutorials/PGVECTOR_EMBEDDING_PIPELINES.md");
    let blocks: Vec<&str> = tutorial
        .split("```sql\n")
        .skip(1)
        .map(|block| block.split("```").next().expect("SQL fence"))
        .collect();
    let mut conn = db.pool.acquire().await.expect("tutorial connection");
    sqlx::query("SET pg_trickle.enable_vector_agg = on")
        .execute(&mut *conn)
        .await
        .expect("enable vector aggregates");
    for block in &blocks[1..=5] {
        sqlx::raw_sql(*block)
            .execute(&mut *conn)
            .await
            .unwrap_or_else(|error| panic!("tutorial SQL failed: {error}\n{block}"));
    }
    let centroid: String =
        sqlx::query_scalar("SELECT centroid::text FROM user_centroid WHERE user_id = 42")
            .fetch_one(&mut *conn)
            .await
            .expect("tutorial centroid");
    assert_eq!(centroid, "[0.1,0.2,0.3]");
    let (doc_id, distance): (i64, f64) = sqlx::query_as(sqlx::AssertSqlSafe(blocks[6]))
        .bind(42_i32)
        .fetch_one(&mut *conn)
        .await
        .expect("tutorial parameterized similarity search");
    assert_eq!(doc_id, 999);
    assert!(distance.abs() < 1e-6);
}

#[tokio::test]
async fn test_centroid_numerical_guard_reports_rebase_reasons() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let delta: Vec<f64> = db.query_scalar("SELECT pgtrickle._centroid_delta(v::real[], sign) FROM (VALUES ('[1,2]'::vector, 1::smallint), ('[1,2]'::vector, 1::smallint), (NULL::vector, -1::smallint), ('[1,2]'::vector, -1::smallint)) AS input(v, sign)").await;
    assert_eq!(delta, vec![1.0, 3.0, 1.0, 2.0, 3.0, 6.0]);
    let merged: Vec<f64> = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_merge(ARRAY[]::float8[], $1::float8[], 2, true)",
    )
    .bind(&delta)
    .fetch_one(&db.pool)
    .await
    .expect("merge centroid delta");
    assert_eq!(merged, vec![1.0, 5.0, 3.0, 1.0, 2.0, 3.0, 6.0]);
    let initialized: Vec<f64> = db.query_scalar("SELECT pgtrickle._centroid_init(pgtrickle._centroid_delta(ARRAY[1]::real[], 1::smallint), 1) FROM generate_series(1, 5000)").await;
    assert_eq!(initialized, vec![5000.0, 5000.0, 0.0, 5000.0, 5000.0]);
    let finalized: bool = db.query_scalar("SELECT pgtrickle._centroid_finalize(ARRAY[2,2,0,3,4,3,4]::float8[], 2)::vector(2) = '[1.5,2]'::vector AND pgtrickle._centroid_finalize(ARRAY[0,0,0,0,0]::float8[], 1) IS NULL").await;
    assert!(
        finalized,
        "finalization must produce a vector or NULL for no vectors"
    );
    let unsafe_finalize = db
        .try_execute("SELECT pgtrickle._centroid_finalize(ARRAY[1,3,1,0,2e20]::float8[], 1)")
        .await;
    assert!(
        unsafe_finalize.is_err(),
        "unsafe state must not produce a centroid"
    );
    let init_reason: Option<String> = sqlx::query_scalar("SELECT pgtrickle._centroid_rebase_reason($1::bigint, $2::bigint, $3::bigint, ARRAY[$4]::float8[], ARRAY[$5]::float8[])")
        .bind(5000_i64).bind(5000_i64).bind(0_i64).bind(5000.0_f64).bind(5000.0_f64)
        .fetch_one(&db.pool).await.expect("check initialized state");
    assert!(
        init_reason.is_none(),
        "initial group size is not a change budget"
    );
    let invalid_init = db
        .try_execute("SELECT pgtrickle._centroid_init(ARRAY[1,3,1,3]::float8[], 1)")
        .await;
    assert!(
        invalid_init.is_err(),
        "initialization must reject retractions"
    );
    let invalid_merge = db
        .try_execute(
            "SELECT pgtrickle._centroid_merge(ARRAY[[1.0, 2.0]]::float8[], ARRAY[]::float8[], 1, false)",
        )
        .await;
    assert!(
        invalid_merge.is_err(),
        "multidimensional state must fail safely"
    );
    let missing_state = db
        .try_execute(
            "SELECT pgtrickle._centroid_merge(ARRAY[]::float8[], ARRAY[]::float8[], 1, false)",
        )
        .await;
    assert!(
        missing_state.is_err(),
        "existing groups cannot use missing state"
    );
    assert!(
        db.try_execute("SELECT pgtrickle._centroid_check_count(ARRAY[2,2,0,2,2]::float8[], 1, 1)")
            .await
            .is_err(),
        "non-null count cannot exceed group size"
    );
    let invalid_delta = db.try_execute("SELECT pgtrickle._centroid_delta(v, 1::smallint) FROM (VALUES (ARRAY[1]::real[]), (ARRAY[1,2]::real[])) AS input(v)").await;
    assert!(invalid_delta.is_err(), "mixed dimensions must fail safely");
    let matches: bool = db.query_scalar(
        "SELECT pgtrickle._centroid_rebase_reason(2, 2, 0, ARRAY[5.0]::float8[], ARRAY[5.0]::float8[]) IS NULL \
         AND pgtrickle._centroid_rebase_reason(1, 3, 1, ARRAY[0.0]::float8[], ARRAY[2e20]::float8[]) = 'NUMERICAL_ERROR' \
         AND pgtrickle._centroid_rebase_reason(1, 4096, 4096, ARRAY[1.0]::float8[], ARRAY[1.0]::float8[]) = 'CHANGE_LIMIT' \
         AND pgtrickle._centroid_rebase_reason(-1, 1, 0, ARRAY[1.0]::float8[], ARRAY[1.0]::float8[]) = 'INVALID_STATE' \
         AND pgtrickle._centroid_rebase_reason(1, 1, 0, ARRAY[NULL]::float8[], ARRAY[1.0]::float8[]) = 'INVALID_STATE' \
         AND pgtrickle._centroid_rebase_reason(1, 1, 0, ARRAY[[1.0, 2.0]]::float8[], ARRAY[1.0, 2.0]::float8[]) = 'INVALID_STATE' \
         AND pgtrickle._centroid_rebase_reason(1, 1, 0, '[0:0]={1}'::float8[], ARRAY[1.0]::float8[]) = 'INVALID_STATE' \
         AND pgtrickle._centroid_rebase_reason(NULL, 1, 0, ARRAY[1.0]::float8[], ARRAY[1.0]::float8[]) = 'INVALID_STATE'"
    ).await;
    assert!(
        matches,
        "guard must report safe, unsafe, and malformed state"
    );
    let cancellation: bool = db.query_scalar(
        "WITH input AS (SELECT '[1e20]'::vector AS large, '[1]'::vector AS small), \
         state AS (SELECT large, small, vector_accum(vector_accum(ARRAY[0]::float8[], large), small) AS a FROM input) \
         SELECT pgtrickle._centroid_rebase_reason(1, 3, 1, \
                    ARRAY[a[2] - (large::real[])[1]::float8], \
                    ARRAY[2 * abs((large::real[])[1]::float8) + 1]) = 'NUMERICAL_ERROR' \
                AND avg(small) = '[1]'::vector FROM state GROUP BY a, large"
    ).await;
    assert!(
        cancellation,
        "real pgvector state must rebase after cancellation"
    );
    db.execute("CREATE TABLE centroid_guard_toast (sums float8[], magnitudes float8[])")
        .await;
    db.execute("ALTER TABLE centroid_guard_toast ALTER COLUMN sums SET STORAGE EXTERNAL")
        .await;
    db.execute("ALTER TABLE centroid_guard_toast ALTER COLUMN magnitudes SET STORAGE EXTERNAL")
        .await;
    db.execute("INSERT INTO centroid_guard_toast SELECT array_agg(i::float8), array_agg(i::float8) FROM generate_series(1, 16000) i").await;
    let external: bool = db.query_scalar("SELECT pg_relation_size(reltoastrelid) > 0 FROM pg_class WHERE oid = 'centroid_guard_toast'::regclass").await;
    assert!(external, "test arrays must be stored in the TOAST relation");
    let toasted_safe: bool = db.query_scalar("SELECT pgtrickle._centroid_rebase_reason(1, 1, 0, sums, magnitudes) IS NULL FROM centroid_guard_toast").await;
    assert!(toasted_safe, "guard must detoast large state arrays safely");
    db.execute("CREATE TABLE centroid_guard_values (id int PRIMARY KEY, v vector(1) NOT NULL)")
        .await;
    db.execute(
        r#"DO $test$
        DECLARE
            cycle int; slot int; old_value real; value real; native real;
            n bigint := 2; operations bigint := 2; changes bigint := 0;
            total float8 := ('1e20'::real)::float8 + 1;
            magnitude float8 := abs(('1e20'::real)::float8) + 1;
            state float8[]; delta float8[];
            reason text; accepted int := 0; rebased int := 0;
        BEGIN
            INSERT INTO centroid_guard_values VALUES (0, '[1e20]'), (1, '[1]');
            DELETE FROM centroid_guard_values WHERE id = 0;
            total := total - ('1e20'::real)::float8;
            magnitude := magnitude + abs(('1e20'::real)::float8);
            n := 1; operations := 3; changes := 1;
            reason := pgtrickle._centroid_rebase_reason(n, operations, changes, ARRAY[total], ARRAY[magnitude]);
            IF reason IS DISTINCT FROM 'NUMERICAL_ERROR' THEN
                RAISE EXCEPTION 'lost contribution was not detected: %', reason;
            END IF;
            total := 1; magnitude := 1; operations := 1; changes := 0; rebased := 1;
            state := ARRAY[1, 1, 0, 1, 1]::float8[];
            FOR cycle IN 1..1000 LOOP
                slot := 2 + (cycle * 17 + cycle / 7) % 61;
                SELECT (v::real[])[1] INTO old_value FROM centroid_guard_values WHERE id = slot;
                IF FOUND THEN
                    DELETE FROM centroid_guard_values WHERE id = slot;
                    SELECT pgtrickle._centroid_delta(ARRAY[old_value], -1::smallint) INTO delta;
                ELSE
                    value := CASE WHEN cycle % 97 = 0 THEN '1e20'::real
                                  ELSE ((cycle * 17) % 2001 - 1000)::real END;
                    INSERT INTO centroid_guard_values VALUES (slot, ARRAY[value]::vector(1));
                    SELECT pgtrickle._centroid_delta(ARRAY[value], 1::smallint) INTO delta;
                END IF;
                state := pgtrickle._centroid_merge(state, delta, 1, false);
                n := state[1]::bigint; operations := state[2]::bigint; changes := state[3]::bigint;
                total := state[4]; magnitude := state[5];
                reason := pgtrickle._centroid_rebase_reason(n, operations, changes, ARRAY[total], ARRAY[magnitude]);
                IF reason IS NOT NULL THEN
                    SELECT count(*), COALESCE(sum((v::real[])[1]::float8), 0),
                           COALESCE(sum(abs((v::real[])[1]::float8)), 0)
                      INTO n, total, magnitude FROM centroid_guard_values;
                    operations := n; changes := 0; rebased := rebased + 1;
                    state := ARRAY[n::float8, operations::float8, 0, total, magnitude];
                ELSE
                    SELECT (avg(v)::real[])[1] INTO native FROM centroid_guard_values;
                    IF native IS NOT NULL AND abs((total / n)::real::float8 - native::float8)
                        > 1e-5 + 1e-6 * abs(native::float8) THEN
                        RAISE EXCEPTION 'guard accepted wrong centroid on cycle %', cycle;
                    END IF;
                    accepted := accepted + 1;
                END IF;
            END LOOP;
            IF accepted < 100 OR rebased = 0 THEN
                RAISE EXCEPTION 'insufficient guard coverage: accepted %, rebased %', accepted, rebased;
            END IF;
        END $test$"#,
    )
    .await;
}

#[tokio::test]
async fn test_centroid_numerical_guard_multidimensional_churn() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE centroid_churn_values (id int PRIMARY KEY, v vector(3))")
        .await;
    db.execute(
        r#"DO $test$
        DECLARE
            cycle int; slot int; component int; seed bigint := 17;
            old_value real[]; new_value real[]; delta float8[]; state float8[];
            actual real[]; native real[]; reason text;
            accepted int := 0; rebased int := 0; change_limit int := 0; numerical int := 0;
            native_count bigint;
        BEGIN
            INSERT INTO centroid_churn_values
            SELECT i, CASE WHEN i % 5 = 0 THEN NULL
                           ELSE ARRAY[i::real, (-i)::real, (i % 3)::real]::vector(3) END
            FROM generate_series(0, 31) i;
            SELECT pgtrickle._centroid_init(pgtrickle._centroid_delta(v::real[], 1::smallint), 3)
              INTO state FROM centroid_churn_values;
            FOR cycle IN 1..2600 LOOP
                seed := (seed * 48271) % 2147483647;
                slot := (seed % 32)::int;
                SELECT v::real[] INTO old_value FROM centroid_churn_values WHERE id = slot;
                seed := (seed * 48271) % 2147483647;
                new_value := CASE WHEN cycle % 31 = 0 THEN NULL
                    WHEN cycle > 2200 AND cycle % 71 = 0
                        THEN ARRAY['1e20'::real, '-1e20'::real, 1::real]
                    ELSE ARRAY[((seed % 2001) - 1000)::real / 7,
                               (((seed / 17) % 2001) - 1000)::real / 11,
                               (((seed / 113) % 2001) - 1000)::real / 13] END;
                UPDATE centroid_churn_values SET v = new_value::vector(3) WHERE id = slot;
                SELECT pgtrickle._centroid_delta(v, sign) INTO delta
                FROM (VALUES (old_value, -1::smallint), (new_value, 1::smallint)) d(v, sign);
                state := pgtrickle._centroid_merge(state, delta, 3, false);
                SELECT count(v) INTO native_count FROM centroid_churn_values;
                IF state[1] <> native_count THEN
                    RAISE EXCEPTION 'centroid non-null count mismatch on cycle %', cycle;
                END IF;
                reason := pgtrickle._centroid_rebase_reason(
                    state[1]::bigint, state[2]::bigint, state[3]::bigint,
                    state[4:6], state[7:9]);
                IF reason IS NOT NULL THEN
                    IF reason = 'INVALID_STATE' THEN
                        RAISE EXCEPTION 'valid update produced invalid state on cycle %', cycle;
                    END IF;
                    IF reason = 'CHANGE_LIMIT' THEN change_limit := change_limit + 1; END IF;
                    IF reason = 'NUMERICAL_ERROR' THEN numerical := numerical + 1; END IF;
                    SELECT pgtrickle._centroid_init(pgtrickle._centroid_delta(v::real[], 1::smallint), 3)
                      INTO state FROM centroid_churn_values;
                    rebased := rebased + 1;
                    CONTINUE;
                END IF;
                actual := pgtrickle._centroid_finalize(state, 3);
                SELECT avg(v)::real[] INTO native FROM centroid_churn_values;
                IF actual IS NULL OR native IS NULL THEN
                    IF actual IS DISTINCT FROM native THEN
                        RAISE EXCEPTION 'centroid NULL mismatch on cycle %', cycle;
                    END IF;
                ELSE
                    FOR component IN 1..3 LOOP
                        IF abs(actual[component]::float8 - native[component]::float8)
                            > 1e-5 + 1e-6 * abs(native[component]::float8) THEN
                            RAISE EXCEPTION 'centroid mismatch on cycle %, component %: % vs %',
                                cycle, component, actual[component], native[component];
                        END IF;
                    END LOOP;
                END IF;
                accepted := accepted + 1;
            END LOOP;
            IF accepted < 2000 OR rebased = 0 OR change_limit = 0 OR numerical = 0 THEN
                RAISE EXCEPTION 'insufficient churn coverage: accepted %, rebased %, change limit %, numerical %',
                    accepted, rebased, change_limit, numerical;
            END IF;
        END $test$"#,
    ).await;
}

#[tokio::test]
async fn test_centroid_initialization_query_preserves_nulls_and_join_multiplicity() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE centroid_init_values (id int PRIMARY KEY, grp int, v vector(2))")
        .await;
    db.execute(
        "INSERT INTO centroid_init_values VALUES (1, 1, '[1,2]'), (2, 1, NULL), (3, 2, '[-1,4]'), (4, 3, NULL)",
    )
    .await;
    db.execute("CREATE TABLE centroid_init_copies (source_id int NOT NULL)")
        .await;
    db.execute("INSERT INTO centroid_init_copies VALUES (1), (1), (2), (3), (4)")
        .await;
    let query = "SELECT v.grp, avg(v.v) AS centroid FROM centroid_init_values v JOIN centroid_init_copies c ON c.source_id = v.id GROUP BY v.grp";
    let generated: Option<String> =
        sqlx::query_scalar("SELECT pgtrickle._centroid_initialization_sql($1)")
            .bind(query)
            .fetch_one(&db.pool)
            .await
            .expect("generate initialization query");
    let sql = generated.expect("fixed-dimension vector average over an inner join is admitted");
    // The generated SQL comes from the fixed query and tables created above.
    let sql = format!(
        "SELECT grp, centroid::real[], __pgt_group_count, __pgt_centroid_state FROM ({sql}) AS initialized"
    );
    type InitializedCentroid = (i32, Option<Vec<f32>>, i64, Vec<f64>);
    let mut rows: Vec<InitializedCentroid> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_all(&db.pool)
        .await
        .expect("execute initialization query");
    rows.sort_by_key(|row| row.0);
    assert_eq!(
        rows,
        vec![
            (
                1,
                Some(vec![1.0, 2.0]),
                3,
                vec![2.0, 2.0, 0.0, 2.0, 4.0, 2.0, 4.0]
            ),
            (
                2,
                Some(vec![-1.0, 4.0]),
                1,
                vec![1.0, 1.0, 0.0, -1.0, 4.0, 1.0, 4.0]
            ),
            (3, None, 1, vec![0.0; 7]),
        ]
    );
    let create = format!(
        "SELECT pgtrickle.create_stream_table('centroid_init_st', $${query}$$, '1h', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    let staged: Option<String> = sqlx::query_scalar("SELECT pgtrickle._centroid_staging_sql($1)")
        .bind(query)
        .fetch_one(&db.pool)
        .await
        .expect("generate staging query");
    let staged = staged.expect("group identities are admitted");
    let verify = format!(
        "WITH staged AS ({staged}) SELECT NOT EXISTS ((SELECT grp, __pgt_row_id, centroid::text, __pgt_group_count FROM staged EXCEPT ALL SELECT grp, __pgt_row_id, centroid::text, __pgt_count FROM centroid_init_st) UNION ALL (SELECT grp, __pgt_row_id, centroid::text, __pgt_count FROM centroid_init_st EXCEPT ALL SELECT grp, __pgt_row_id, centroid::text, __pgt_group_count FROM staged))"
    );
    let matches: bool = db.query_scalar(&verify).await;
    assert!(
        matches,
        "staged rows must match the stream table and its identities"
    );
    let restricted: bool = db.query_scalar("SELECT NOT EXISTS (SELECT 1 FROM pg_proc p CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) a WHERE p.oid = 'pgtrickle._centroid_stage_state(oid)'::regprocedure AND a.grantee = 0 AND a.privilege_type = 'EXECUTE')").await;
    assert!(restricted, "staging must not be executable by PUBLIC");
    let mut transaction = db.pool.begin().await.expect("begin staging transaction");
    let stage_one: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_stage_state('centroid_init_st'::regclass::oid)",
    )
    .fetch_one(&mut *transaction)
    .await
    .expect("stage owner snapshot");
    db.execute("UPDATE centroid_init_values SET v='[9,9]' WHERE id=1")
        .await;
    let stage_two: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_stage_state('centroid_init_st'::regclass::oid)",
    )
    .fetch_one(&mut *transaction)
    .await
    .expect("stage newer owner snapshot");
    assert_ne!(
        stage_one, stage_two,
        "staging must not overwrite an existing temp table"
    );
    let compare = format!(
        "SELECT (SELECT centroid::text FROM {stage_one} WHERE grp=1), (SELECT centroid::text FROM {stage_two} WHERE grp=1)"
    );
    let (first, second): (String, String) = sqlx::query_as(sqlx::AssertSqlSafe(compare.as_str()))
        .fetch_one(&mut *transaction)
        .await
        .expect("compare staged snapshots");
    assert_eq!(first, "[1,2]");
    assert_eq!(second, "[9,9]");
    transaction
        .rollback()
        .await
        .expect("rollback staged tables");
    let mut preparation = db.pool.begin().await.expect("begin state preparation");
    let staged: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_prepare_state('centroid_init_st'::regclass::oid)",
    )
    .fetch_one(&mut *preparation)
    .await
    .expect("prepare centroid state");
    let compare = format!(
        "SELECT (SELECT count(*) FROM {staged}) = 3 AND NOT EXISTS ((SELECT __pgt_row_id, __pgt_group_count, __pgt_centroid_state FROM {staged} EXCEPT ALL SELECT row_id, group_count, state FROM pgtrickle.pgt_centroid_groups WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_init_st'::regclass)) UNION ALL (SELECT row_id, group_count, state FROM pgtrickle.pgt_centroid_groups WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_init_st'::regclass) EXCEPT ALL SELECT __pgt_row_id, __pgt_group_count, __pgt_centroid_state FROM {staged}))"
    );
    let equal: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(compare.as_str()))
        .fetch_one(&mut *preparation)
        .await
        .expect("compare staged and stored state");
    assert!(equal, "stored groups must come from the returned snapshot");
    preparation.commit().await.expect("commit prepared state");
    let matches_contract: bool = db.query_scalar("SELECT c.status = 'BUILDING' AND c.schema_version = 1 AND c.stream_relid = s.pgt_relid AND c.defining_query = s.defining_query AND c.contract_generation = s.contract_generation FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_relid = 'centroid_init_st'::regclass").await;
    assert!(
        matches_contract,
        "prepared state must remain incomplete and bound to the current contract"
    );
    let state_sql = "SELECT g.state FROM pgtrickle.pgt_centroid_groups g JOIN centroid_init_st s ON s.__pgt_row_id = g.row_id WHERE s.grp = 1";
    let state: Vec<f64> = db.query_scalar(state_sql).await;
    assert_eq!(state, vec![2.0, 2.0, 0.0, 18.0, 18.0, 18.0, 18.0]);
    assert!(
        db.try_execute("UPDATE pgtrickle.pgt_centroid_groups SET state = ARRAY[1.0]::float8[]")
            .await
            .is_err()
    );
    assert!(db.try_execute("UPDATE pgtrickle.pgt_centroid_groups SET state = ARRAY[4.0, 4.0, 0.0, 1.0, 1.0]::float8[] WHERE group_count = 3").await.is_err());
    let mut replacement = db.pool.begin().await.expect("begin state replacement");
    sqlx::query("UPDATE centroid_init_values SET v = '[4,4]' WHERE id = 1")
        .execute(&mut *replacement)
        .await
        .expect("update source in replacement transaction");
    let replaced: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_prepare_state('centroid_init_st'::regclass::oid)",
    )
    .fetch_one(&mut *replacement)
    .await
    .expect("replace prepared state");
    let new_state: Vec<f64> = sqlx::query_scalar(state_sql)
        .fetch_one(&mut *replacement)
        .await
        .expect("read replacement state");
    assert_eq!(new_state, vec![2.0, 2.0, 0.0, 8.0, 8.0, 8.0, 8.0]);
    let staged_state: Vec<f64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_centroid_state FROM {replaced} WHERE grp = 1"
    )))
    .fetch_one(&mut *replacement)
    .await
    .expect("read replacement staged state");
    assert_eq!(staged_state, new_state);
    replacement
        .rollback()
        .await
        .expect("rollback state replacement");
    let restored: Vec<f64> = db.query_scalar(state_sql).await;
    assert_eq!(
        restored, state,
        "rollback must restore the original private state"
    );
    let restarted = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart PostgreSQL with prepared centroid state");
    assert!(
        restarted.status.success(),
        "{}",
        String::from_utf8_lossy(&restarted.stderr)
    );
    db.pool.close().await;
    db.pool = db.reconnect_after_restart().await;
    let after_restart: Vec<f64> = db.query_scalar(state_sql).await;
    assert_eq!(after_restart, state, "private state must survive restart");
    db.execute("SET pg_trickle.enable_vector_agg = on").await;
    db.execute_seq(&[
        "CREATE ROLE centroid_stage_owner NOLOGIN",
        "CREATE ROLE centroid_stage_other NOLOGIN",
        "GRANT USAGE ON SCHEMA pgtrickle TO centroid_stage_owner, centroid_stage_other",
        "GRANT EXECUTE ON FUNCTION pgtrickle._centroid_stage_state(oid) TO centroid_stage_owner, centroid_stage_other",
        "GRANT EXECUTE ON FUNCTION pgtrickle.refresh_stream_table(text) TO centroid_stage_owner",
        "ALTER TABLE centroid_init_st OWNER TO centroid_stage_owner",
    ])
    .await;
    let private: bool = db.query_scalar("SELECT NOT has_table_privilege('centroid_stage_other', 'pgtrickle.pgt_centroid_states', 'SELECT') AND NOT has_table_privilege('centroid_stage_other', 'pgtrickle.pgt_centroid_groups', 'SELECT') AND NOT has_function_privilege('centroid_stage_other', 'pgtrickle._centroid_prepare_state(oid)', 'EXECUTE')").await;
    assert!(
        private,
        "private centroid state must not be exposed to other roles"
    );
    let mut unauthorized = db.pool.begin().await.expect("begin unauthorized staging");
    sqlx::query("SET LOCAL ROLE centroid_stage_other")
        .execute(&mut *unauthorized)
        .await
        .expect("set unrelated role");
    let error = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle._centroid_stage_state('centroid_init_st'::regclass::oid)",
    )
    .fetch_one(&mut *unauthorized)
    .await
    .expect_err("a non-owner must not stage another stream table");
    assert!(error.to_string().contains("must be owner of stream table"));
    unauthorized
        .rollback()
        .await
        .expect("rollback unauthorized staging");

    let mut unprivileged_owner = db.pool.begin().await.expect("begin owner staging");
    sqlx::query("SET LOCAL ROLE centroid_stage_owner")
        .execute(&mut *unprivileged_owner)
        .await
        .expect("set stream owner role");
    let error = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle._centroid_stage_state('centroid_init_st'::regclass::oid)",
    )
    .fetch_one(&mut *unprivileged_owner)
    .await
    .expect_err("staging must respect the owner's source privileges");
    let message = error.to_string();
    assert!(
        message.contains("permission denied for table centroid_init_values")
            || message.contains("permission denied for table centroid_init_copies"),
        "unexpected staging error: {message}"
    );
    unprivileged_owner
        .rollback()
        .await
        .expect("rollback owner staging");

    db.execute(
        "GRANT SELECT ON centroid_init_values, centroid_init_copies TO centroid_stage_owner",
    )
    .await;
    let mut owner = db.pool.begin().await.expect("begin authorized staging");
    sqlx::query("SET LOCAL ROLE centroid_stage_owner")
        .execute(&mut *owner)
        .await
        .expect("set authorized owner role");
    let staged: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_stage_state('centroid_init_st'::regclass::oid)",
    )
    .fetch_one(&mut *owner)
    .await
    .expect("stage as stream owner");
    let sql = format!("SELECT centroid::text FROM {staged} WHERE grp = 1");
    let centroid: String = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_one(&mut *owner)
        .await
        .expect("read owner staging table");
    assert_eq!(centroid, "[9,9]");
    owner.rollback().await.expect("rollback authorized staging");
    db.execute_seq(&[
        "UPDATE centroid_init_values SET v = '[8,8]' WHERE id = 1",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_init_st'::regclass",
    ]).await;
    let mut owner_refresh = db.pool.begin().await.expect("begin owner refresh");
    sqlx::query("SET LOCAL ROLE centroid_stage_owner")
        .execute(&mut *owner_refresh)
        .await
        .expect("set owner for refresh");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_init_st')")
        .execute(&mut *owner_refresh)
        .await
        .expect("refresh prepared centroid as owner");
    owner_refresh.commit().await.expect("commit owner refresh");
    let refreshed: bool = db.query_scalar("SELECT s.centroid::text = '[8,8]' AND g.state = ARRAY[2,2,0,16,16,16,16]::float8[] FROM centroid_init_st s JOIN pgtrickle.pgt_centroid_groups g ON g.row_id = s.__pgt_row_id WHERE s.grp = 1").await;
    assert!(
        refreshed,
        "owner refresh must update visible rows and private state"
    );
    db.execute("CREATE TABLE centroid_init_empty (id int PRIMARY KEY, v vector(2))")
        .await;
    let empty_sql: Option<String> = db.query_scalar("SELECT pgtrickle._centroid_initialization_sql('SELECT avg(v) AS centroid FROM centroid_init_empty')").await;
    let empty_sql = empty_sql.expect("empty scalar aggregate is admitted");
    let empty_sql = format!(
        "SELECT centroid::real[], __pgt_group_count, __pgt_centroid_state FROM ({empty_sql}) AS initialized"
    );
    let empty: (Option<Vec<f32>>, i64, Vec<f64>) =
        sqlx::query_as(sqlx::AssertSqlSafe(empty_sql.as_str()))
            .fetch_one(&db.pool)
            .await
            .expect("execute empty initialization query");
    assert_eq!(empty, (None, 0, vec![0.0; 7]));
    let unsupported: bool = db.query_scalar("SELECT pgtrickle._centroid_initialization_sql('SELECT grp, avg(DISTINCT v) FROM centroid_init_values GROUP BY grp') IS NULL").await;
    assert!(unsupported, "DISTINCT must retain the safe rescan strategy");
    let volatile: bool = db.query_scalar("SELECT pgtrickle._centroid_initialization_sql('SELECT grp, avg(v) FROM centroid_init_values WHERE random() < 1 GROUP BY grp') IS NULL").await;
    assert!(
        volatile,
        "volatile queries must not be admitted for initialization"
    );
    let hidden_group: bool = db.query_scalar("SELECT pgtrickle._centroid_initialization_sql('SELECT avg(v) FROM centroid_init_values GROUP BY grp') IS NULL").await;
    assert!(
        hidden_group,
        "groups missing from output cannot be matched to state"
    );
    let before_failed_prepare: Vec<f64> = db.query_scalar(state_sql).await;
    db.execute("ALTER TABLE centroid_init_values ENABLE ROW LEVEL SECURITY")
        .await;
    let rls: bool = sqlx::query_scalar("SELECT pgtrickle._centroid_initialization_sql($1) IS NULL")
        .bind(query)
        .fetch_one(&db.pool)
        .await
        .expect("check RLS admission");
    assert!(rls, "RLS sources must retain the safe strategy");
    assert!(
        db.try_execute(
            "SELECT pgtrickle._centroid_prepare_state('centroid_init_st'::regclass::oid)"
        )
        .await
        .is_err()
    );
    let after_failed_prepare: Vec<f64> = db.query_scalar(state_sql).await;
    assert_eq!(
        after_failed_prepare, before_failed_prepare,
        "failed preparation must preserve prior state"
    );
    db.execute("SELECT pgtrickle.drop_stream_table('centroid_init_st')")
        .await;
    let remaining: i64 = db.query_scalar("SELECT (SELECT count(*) FROM pgtrickle.pgt_centroid_states) + (SELECT count(*) FROM pgtrickle.pgt_centroid_groups)").await;
    assert_eq!(
        remaining, 0,
        "dropping the stream must remove private state"
    );
}

#[tokio::test]
async fn test_centroid_signed_join_delta_preserves_moves_and_multiplicity() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire centroid delta session");
    for sql in [
        "SET pg_trickle.enable_vector_agg = on",
        "CREATE TABLE centroid_delta_values (id int PRIMARY KEY, grp int, v vector(2))",
        "CREATE TABLE centroid_delta_copies (source_id int NOT NULL)",
        "INSERT INTO centroid_delta_values VALUES (1, 1, '[1,2]'), (2, 2, NULL)",
        "INSERT INTO centroid_delta_copies VALUES (1), (1), (2)",
        "SELECT pgtrickle.create_stream_table('centroid_delta_st', $$SELECT v.grp, avg(v.v) AS centroid FROM centroid_delta_values v JOIN centroid_delta_copies c ON c.source_id = v.id GROUP BY v.grp$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle._centroid_prepare_state('centroid_delta_st'::regclass::oid)",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_delta_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_delta_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("set up centroid delta");
    }
    let previous: serde_json::Value = sqlx::query_scalar("SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_delta_st'::regclass")
        .fetch_one(&mut *conn).await.expect("read initial frontier");
    sqlx::query("UPDATE centroid_delta_values SET grp = 2, v = '[4,5]' WHERE id = 1")
        .execute(&mut *conn)
        .await
        .expect("move vector group");
    sqlx::query("UPDATE centroid_delta_values SET v = '[10,20]' WHERE id = 2")
        .execute(&mut *conn)
        .await
        .expect("change NULL vector to a vector");
    sqlx::query("INSERT INTO centroid_delta_copies VALUES (1)")
        .execute(&mut *conn)
        .await
        .expect("add duplicate join contribution");
    sqlx::query("INSERT INTO centroid_delta_values VALUES (3, 3, '[7,-3]')")
        .execute(&mut *conn)
        .await
        .expect("create new group");
    sqlx::query("INSERT INTO centroid_delta_copies VALUES (3), (3)")
        .execute(&mut *conn)
        .await
        .expect("duplicate new group contribution");
    let mut next = previous.clone();
    for source in next["sources"]
        .as_object_mut()
        .expect("frontier sources")
        .values_mut()
    {
        source["lsn"] = serde_json::json!("FF/FFFFFFFF");
    }
    let mut invalid = next.clone();
    invalid["sources"]
        .as_object_mut()
        .expect("frontier sources")
        .values_mut()
        .next()
        .expect("a source")["lsn"] = serde_json::json!("0/1' OR true --");
    let error = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle._centroid_delta_sql('centroid_delta_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(invalid.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect_err("invalid frontier must be rejected");
    assert!(error.to_string().contains("invalid centroid frontier LSN"));
    let source_key = previous["sources"]
        .as_object()
        .expect("frontier sources")
        .keys()
        .next()
        .expect("a source")
        .clone();
    assert_ne!(previous["sources"][&source_key]["lsn"], "0/0");
    let mut missing_previous = previous.clone();
    missing_previous["sources"]
        .as_object_mut()
        .expect("previous sources")
        .remove(&source_key);
    let mut missing_next = next.clone();
    missing_next["sources"]
        .as_object_mut()
        .expect("next sources")
        .remove(&source_key);
    let mut regressed = next.clone();
    regressed["sources"][&source_key]["lsn"] = serde_json::json!("0/0");
    for function in [
        "_centroid_delta_sql",
        "_centroid_candidate_sql",
        "_centroid_stage_delta",
        "_centroid_stage_candidates",
    ] {
        let sql =
            format!("SELECT pgtrickle.{function}('centroid_delta_st'::regclass::oid, $1, $2)");
        for (from, to, expected) in [
            (&missing_previous, &next, "previous frontier is missing"),
            (&previous, &missing_next, "next frontier is missing"),
            (&previous, &regressed, "frontier regresses"),
        ] {
            let error = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(sql.as_str()))
                .bind(from.to_string())
                .bind(to.to_string())
                .fetch_one(&mut *conn)
                .await
                .expect_err("incomplete or backwards frontier must be rejected");
            assert!(
                error.to_string().contains(expected),
                "unexpected frontier error: {error}"
            );
        }
    }
    let generated: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_delta_sql('centroid_delta_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(next.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect("generate signed centroid delta");
    let sql = format!(
        "SELECT grp, __pgt_group_delta, __pgt_centroid_delta, __pgt_row_id = pgtrickle.encode_row_id_v3('GROUP_KEY', ROW(grp)) FROM ({generated}) AS delta ORDER BY grp"
    );
    let rows: Vec<(i32, i64, Vec<f64>, bool)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_all(&mut *conn)
        .await
        .expect("execute signed centroid delta");
    assert_eq!(rows.len(), 3);
    assert_eq!((rows[0].0, rows[0].1, rows[0].2[0]), (1, -2, -2.0));
    assert_eq!(&rows[0].2[2..4], &[-2.0, -4.0]);
    assert_eq!((rows[1].0, rows[1].1, rows[1].2[0]), (2, 3, 4.0));
    assert_eq!(&rows[1].2[2..4], &[22.0, 35.0]);
    assert_eq!((rows[2].0, rows[2].1, rows[2].2[0]), (3, 2, 2.0));
    assert_eq!(&rows[2].2[2..4], &[14.0, -6.0]);
    assert!(
        rows.iter().all(|row| row.3),
        "group identities must match visible rows"
    );
    let candidates: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_candidate_sql('centroid_delta_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(next.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect("generate checked centroid candidates");
    let compare = format!(
        "WITH native AS MATERIALIZED (\
         SELECT v.grp, avg(v.v)::text AS centroid FROM centroid_delta_values v \
         JOIN centroid_delta_copies c ON c.source_id = v.id GROUP BY v.grp), raw_candidate AS MATERIALIZED ({candidates}), \
         candidate AS MATERIALIZED (SELECT grp, centroid::text FROM raw_candidate WHERE __pgt_action = 'I') \
         SELECT NOT EXISTS ((SELECT * FROM candidate EXCEPT ALL SELECT * FROM native) \
                            UNION ALL (SELECT * FROM native EXCEPT ALL SELECT * FROM candidate))"
    );
    let matches: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(compare.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect("compare merged centroid state to native AVG");
    assert!(
        matches,
        "merged deltas must match native centroids, including new and removed groups"
    );
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin missing-state check");
    sqlx::query("DELETE FROM pgtrickle.pgt_centroid_groups WHERE row_id = (SELECT __pgt_row_id FROM centroid_delta_st WHERE grp = 2)")
        .execute(&mut *conn).await.expect("remove an existing group's state");
    let error = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(compare.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect_err("missing state must not be treated as a new group");
    assert!(error.to_string().contains("missing state"));
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("restore deleted state");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin inconsistent-count check");
    sqlx::query("UPDATE pgtrickle.pgt_centroid_groups SET group_count = group_count + 1 WHERE row_id = (SELECT __pgt_row_id FROM centroid_delta_st WHERE grp = 2)")
        .execute(&mut *conn).await.expect("make the private count inconsistent");
    sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(compare.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect_err("mismatched public and private counts must fail");
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("restore private count");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin corrupt-accumulator check");
    sqlx::query("UPDATE pgtrickle.pgt_centroid_groups SET state[4] = state[4] + 1 WHERE row_id = (SELECT __pgt_row_id FROM centroid_delta_st WHERE grp = 2)")
        .execute(&mut *conn).await.expect("corrupt an otherwise plausible accumulator");
    let error = sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(compare.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect_err("changed accumulator must not produce a candidate");
    assert!(error.to_string().contains("invalid prior state"));
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("restore private accumulator");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin stale-contract check");
    sqlx::query("UPDATE pgtrickle.pgt_centroid_states SET contract_generation = contract_generation - 1 WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_delta_st'::regclass)")
        .execute(&mut *conn).await.expect("make state contract stale");
    let error = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle._centroid_candidate_sql('centroid_delta_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(next.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect_err("stale state must not produce candidates");
    assert!(
        error
            .to_string()
            .contains("centroid state is missing or stale")
    );
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("restore state contract");
    for statement in [
        "CREATE ROLE centroid_delta_owner NOLOGIN",
        "CREATE ROLE centroid_delta_outsider NOLOGIN",
        "GRANT USAGE ON SCHEMA pgtrickle TO centroid_delta_owner",
        "GRANT EXECUTE ON FUNCTION pgtrickle._centroid_stage_delta(oid, text, text) TO centroid_delta_owner",
        "GRANT EXECUTE ON FUNCTION pgtrickle._centroid_stage_candidates(oid, text, text) TO centroid_delta_owner",
        "GRANT SELECT ON centroid_delta_values, centroid_delta_copies TO centroid_delta_owner",
        "ALTER TABLE centroid_delta_st OWNER TO centroid_delta_owner",
        "BEGIN",
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT SELECT ON TABLES TO centroid_delta_owner",
        "ALTER DEFAULT PRIVILEGES FOR ROLE postgres GRANT SELECT ON TABLES TO centroid_delta_outsider",
        "CREATE TEMP TABLE centroid_default_acl_probe (v int) ON COMMIT DROP",
        "SET LOCAL ROLE centroid_delta_owner",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("set up owner delta staging");
    }
    let private: bool = sqlx::query_scalar("SELECT NOT has_table_privilege(current_user, 'pgtrickle.pgt_centroid_groups', 'SELECT') AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'pgtrickle_changes' AND c.relname IN ('changes_' || 'centroid_delta_values'::regclass::oid::text, 'changes_' || 'centroid_delta_copies'::regclass::oid::text) AND has_table_privilege(current_user, c.oid, 'SELECT'))")
        .fetch_one(&mut *conn).await.expect("check private state and buffers");
    assert!(
        private,
        "the stream owner must not read private state or CDC buffers"
    );
    let default_grant: bool = sqlx::query_scalar(
        "SELECT has_table_privilege(current_user, 'pg_temp.centroid_default_acl_probe', 'SELECT')",
    )
    .fetch_one(&mut *conn)
    .await
    .expect("check permissive default ACL fixture");
    assert!(
        default_grant,
        "fixture must grant the owner SELECT by default"
    );
    let staged: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_stage_delta('centroid_delta_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(next.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect("stage signed delta as owner");
    let no_raw_buffers: bool = sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE relnamespace = pg_my_temp_schema() AND relname LIKE '__pgt_cd_%')")
        .fetch_one(&mut *conn).await.expect("check raw CDC cleanup");
    assert!(
        no_raw_buffers,
        "raw staged CDC must not remain readable by the owner"
    );
    let sql =
        format!("SELECT grp, __pgt_group_delta, __pgt_centroid_delta FROM {staged} ORDER BY grp");
    let staged_rows: Vec<(i32, i64, Vec<f64>)> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
        .fetch_all(&mut *conn)
        .await
        .expect("read owner-staged delta");
    assert_eq!(staged_rows.len(), rows.len());
    for (staged, original) in staged_rows.iter().zip(&rows) {
        assert_eq!(
            (staged.0, staged.1, &staged.2),
            (original.0, original.1, &original.2)
        );
    }
    let (prefix, nonce) = staged
        .trim_end_matches('"')
        .rsplit_once('_')
        .expect("generated stage name");
    let next_nonce: u64 = nonce.parse::<u64>().expect("stage nonce") + 1;
    let collision = format!("{prefix}_{next_nonce}\"");
    let create = format!("CREATE TEMP TABLE {collision} (marker int) ON COMMIT DROP");
    sqlx::query(sqlx::AssertSqlSafe(create.as_str()))
        .execute(&mut *conn)
        .await
        .expect("create colliding temp table");
    let insert = format!("INSERT INTO {collision} VALUES (73)");
    sqlx::query(sqlx::AssertSqlSafe(insert.as_str()))
        .execute(&mut *conn)
        .await
        .expect("seed colliding temp table");
    sqlx::query("SAVEPOINT collision")
        .execute(&mut *conn)
        .await
        .expect("savepoint before collision");
    let error = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle._centroid_stage_delta('centroid_delta_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(next.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect_err("colliding temp table must not be replaced");
    assert!(error.to_string().contains("already exists"));
    sqlx::query("ROLLBACK TO SAVEPOINT collision")
        .execute(&mut *conn)
        .await
        .expect("recover after collision");
    let select = format!("SELECT marker FROM {collision}");
    let marker: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(select.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect("read preserved temp table");
    assert_eq!(marker, 73);
    let pgt_id: i64 = prefix
        .rsplit('_')
        .next()
        .expect("stream ID")
        .parse()
        .expect("numeric stream ID");
    let source_oid: i64 =
        sqlx::query_scalar("SELECT 'centroid_delta_values'::regclass::oid::bigint")
            .fetch_one(&mut *conn)
            .await
            .expect("source OID");
    let buffer_collision = format!(
        "pg_temp.\"__pgt_cd_{pgt_id}_{source_oid}_{}\"",
        next_nonce + 1
    );
    let create = format!("CREATE TEMP TABLE {buffer_collision} (marker int) ON COMMIT DROP");
    sqlx::query(sqlx::AssertSqlSafe(create.as_str()))
        .execute(&mut *conn)
        .await
        .expect("create colliding CDC temp table");
    let insert = format!("INSERT INTO {buffer_collision} VALUES (91)");
    sqlx::query(sqlx::AssertSqlSafe(insert.as_str()))
        .execute(&mut *conn)
        .await
        .expect("seed colliding CDC temp table");
    sqlx::query("SAVEPOINT buffer_collision")
        .execute(&mut *conn)
        .await
        .expect("savepoint before CDC collision");
    let error = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle._centroid_stage_delta('centroid_delta_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(next.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect_err("colliding CDC temp table must not be replaced");
    assert!(error.to_string().contains("already exists"));
    sqlx::query("ROLLBACK TO SAVEPOINT buffer_collision")
        .execute(&mut *conn)
        .await
        .expect("recover after CDC collision");
    let select = format!("SELECT marker FROM {buffer_collision}");
    let marker: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(select.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect("read preserved CDC temp table");
    assert_eq!(marker, 91);
    let public_collision = format!("pg_temp.\"__pgt_cp_{pgt_id}_{}\"", next_nonce + 3);
    let create = format!("CREATE TEMP TABLE {public_collision} (marker int) ON COMMIT DROP");
    sqlx::query(sqlx::AssertSqlSafe(create.as_str()))
        .execute(&mut *conn)
        .await
        .expect("create colliding public candidate table");
    let insert = format!("INSERT INTO {public_collision} VALUES (92)");
    sqlx::query(sqlx::AssertSqlSafe(insert.as_str()))
        .execute(&mut *conn)
        .await
        .expect("seed colliding public candidate table");
    sqlx::query("SAVEPOINT public_collision")
        .execute(&mut *conn)
        .await
        .expect("savepoint before public collision");
    let error = sqlx::query_scalar::<_, String>(
        "SELECT pgtrickle._centroid_stage_candidates('centroid_delta_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(next.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect_err("colliding public table must not be replaced");
    assert!(error.to_string().contains("already exists"));
    sqlx::query("ROLLBACK TO SAVEPOINT public_collision")
        .execute(&mut *conn)
        .await
        .expect("recover after public collision");
    let select = format!("SELECT marker FROM {public_collision}");
    let marker: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(select.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect("read preserved public candidate table");
    assert_eq!(marker, 92);
    let candidates: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_stage_candidates('centroid_delta_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(next.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect("stage candidates across owner boundary");
    let can_read: bool =
        sqlx::query_scalar("SELECT has_table_privilege(current_user, $1, 'SELECT')")
            .bind(&candidates)
            .fetch_one(&mut *conn)
            .await
            .expect("check candidate table ACL");
    assert!(
        !can_read,
        "candidate state must remain private even with permissive default privileges"
    );
    let public_candidates = candidates.replace("__pgt_cc_", "__pgt_cp_");
    assert_ne!(public_candidates, candidates);
    let public_acl: bool = sqlx::query_scalar("SELECT has_table_privilege(current_user, $1, 'SELECT') AND NOT has_table_privilege(current_user, $1, 'UPDATE') AND NOT has_table_privilege('centroid_delta_outsider', $1, 'SELECT') AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = pg_catalog.to_regclass($1) AND attname = '__pgt_centroid_state' AND NOT attisdropped)")
        .bind(&public_candidates).fetch_one(&mut *conn).await.expect("check public candidate ACL and columns");
    assert!(
        public_acl,
        "only visible candidate columns may be read by the stream owner"
    );
    let compare_public = format!(
        "WITH native AS (SELECT v.grp, avg(v.v)::text AS centroid FROM centroid_delta_values v JOIN centroid_delta_copies c ON c.source_id = v.id GROUP BY v.grp), candidate AS (SELECT grp, centroid::text FROM {public_candidates} WHERE __pgt_action = 'I') SELECT NOT EXISTS ((SELECT * FROM candidate EXCEPT ALL SELECT * FROM native) UNION ALL (SELECT * FROM native EXCEPT ALL SELECT * FROM candidate))"
    );
    let public_matches: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(compare_public.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect("compare owner-visible candidates");
    assert!(
        public_matches,
        "owner-visible candidates must match native centroids"
    );
    sqlx::query("SAVEPOINT atomic_centroid_apply")
        .execute(&mut *conn)
        .await
        .expect("begin candidate apply experiment");
    sqlx::query("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .execute(&mut *conn)
        .await
        .expect("enable refresh guard for candidate apply");
    let merge = format!(
        "MERGE INTO centroid_delta_st AS s USING {public_candidates} AS d \
         ON pgtrickle.row_probe_v1(s.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) \
            AND s.__pgt_row_id = d.__pgt_row_id \
         WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE \
         WHEN MATCHED AND d.__pgt_action = 'I' AND \
              (s.grp, s.centroid, s.__pgt_count) IS DISTINCT FROM (d.grp, d.centroid, d.__pgt_count) \
              THEN UPDATE SET grp = d.grp, centroid = d.centroid, __pgt_count = d.__pgt_count \
         WHEN NOT MATCHED AND d.__pgt_action = 'I' THEN \
              INSERT (__pgt_row_id, grp, centroid, __pgt_count) \
              VALUES (d.__pgt_row_id, d.grp, d.centroid, d.__pgt_count)"
    );
    sqlx::query(sqlx::AssertSqlSafe(merge.as_str()))
        .execute(&mut *conn)
        .await
        .expect("apply visible candidates as stream owner");
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("restore state owner");
    let delete = format!(
        "DELETE FROM pgtrickle.pgt_centroid_groups g USING {candidates} c WHERE g.pgt_id = $1 AND g.row_id = c.__pgt_row_id AND c.__pgt_action = 'D'"
    );
    sqlx::query(sqlx::AssertSqlSafe(delete.as_str()))
        .bind(pgt_id)
        .execute(&mut *conn)
        .await
        .expect("delete private group state");
    let upsert = format!(
        "INSERT INTO pgtrickle.pgt_centroid_groups (pgt_id, row_id, group_count, state, state_checksum) SELECT $1, c.__pgt_row_id, c.__pgt_count, c.__pgt_centroid_state, pg_catalog.md5(pg_catalog.int8send($1::bigint) || c.__pgt_row_id || pg_catalog.int8send(c.__pgt_count) || pg_catalog.array_send(c.__pgt_centroid_state)) FROM {candidates} c WHERE c.__pgt_action = 'I' ON CONFLICT (pgt_id, row_id) DO UPDATE SET group_count = EXCLUDED.group_count, state = EXCLUDED.state, state_checksum = EXCLUDED.state_checksum"
    );
    sqlx::query(sqlx::AssertSqlSafe(upsert.as_str()))
        .bind(pgt_id)
        .execute(&mut *conn)
        .await
        .expect("upsert private group state");
    sqlx::query(
        "UPDATE pgtrickle.pgt_centroid_states SET baseline_frontier = $2::jsonb WHERE pgt_id = $1",
    )
    .bind(pgt_id)
    .bind(next.to_string())
    .execute(&mut *conn)
    .await
    .expect("advance candidate baseline");
    sqlx::query("UPDATE pgtrickle.pgt_stream_tables SET frontier = $2::jsonb WHERE pgt_id = $1")
        .bind(pgt_id)
        .bind(next.to_string())
        .execute(&mut *conn)
        .await
        .expect("advance candidate frontier");
    let applied: bool = sqlx::query_scalar("WITH native AS (SELECT v.grp, avg(v.v)::text AS centroid, count(*)::bigint AS group_count, count(v.v)::bigint AS vector_count FROM centroid_delta_values v JOIN centroid_delta_copies c ON c.source_id = v.id GROUP BY v.grp), actual AS (SELECT s.grp, s.centroid::text AS centroid, s.__pgt_count AS group_count, g.state[1]::bigint AS vector_count FROM centroid_delta_st s JOIN pgtrickle.pgt_centroid_groups g ON g.pgt_id = $1 AND g.row_id = s.__pgt_row_id WHERE g.group_count = s.__pgt_count AND g.state_checksum = pg_catalog.md5(pg_catalog.int8send(g.pgt_id) || g.row_id || pg_catalog.int8send(g.group_count) || pg_catalog.array_send(g.state))) SELECT NOT EXISTS ((SELECT * FROM native EXCEPT ALL SELECT * FROM actual) UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM native)) AND (SELECT count(*) FROM pgtrickle.pgt_centroid_groups WHERE pgt_id = $1) = (SELECT count(*) FROM native) AND EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE c.pgt_id = $1 AND c.baseline_frontier = t.frontier AND t.frontier = $2::jsonb)")
        .bind(pgt_id).bind(next.to_string()).fetch_one(&mut *conn).await.expect("verify visible rows, state, and frontier");
    assert!(
        applied,
        "candidate apply must match native results and private state"
    );
    sqlx::query("ROLLBACK TO SAVEPOINT atomic_centroid_apply")
        .execute(&mut *conn)
        .await
        .expect("roll back candidate apply");
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("restore extension administrator");
    let restored: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE c.pgt_id = $1 AND c.baseline_frontier = t.frontier AND t.frontier = $2::jsonb) AND EXISTS (SELECT 1 FROM centroid_delta_st WHERE grp = 1 AND __pgt_count = 2) AND NOT EXISTS (SELECT 1 FROM centroid_delta_st WHERE grp = 3) AND (SELECT count(*) FROM pgtrickle.pgt_centroid_groups WHERE pgt_id = $1) = 2")
        .bind(pgt_id).bind(previous.to_string()).fetch_one(&mut *conn).await.expect("verify rollback of candidate apply");
    assert!(
        restored,
        "candidate apply rollback must restore rows, state, and frontier"
    );
    let compare = format!(
        "WITH native AS (SELECT v.grp, avg(v.v)::text AS centroid FROM centroid_delta_values v JOIN centroid_delta_copies c ON c.source_id = v.id GROUP BY v.grp), candidate AS (SELECT grp, centroid::text FROM {candidates} WHERE __pgt_action = 'I') SELECT NOT EXISTS ((SELECT * FROM candidate EXCEPT ALL SELECT * FROM native) UNION ALL (SELECT * FROM native EXCEPT ALL SELECT * FROM candidate))"
    );
    let matches: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(compare.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect("compare privately staged candidates");
    assert!(
        matches,
        "owner-separated candidates must match native centroids"
    );
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("rollback staged delta");
    for sql in [
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('centroid_delta_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("apply joined centroid delta");
    }
    let applied: bool = sqlx::query_scalar("WITH native AS (SELECT v.grp, avg(v.v)::text AS centroid, count(*)::bigint AS n FROM centroid_delta_values v JOIN centroid_delta_copies c ON c.source_id = v.id GROUP BY v.grp), actual AS (SELECT grp, centroid::text AS centroid, __pgt_count AS n FROM centroid_delta_st) SELECT NOT EXISTS ((SELECT * FROM native EXCEPT ALL SELECT * FROM actual) UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM native)) AND (SELECT merge_strategy_used = 'centroid_algebraic' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_delta_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1)")
        .fetch_one(&mut *conn).await.expect("check joined centroid apply");
    assert!(
        applied,
        "joined centroid refresh must preserve group moves and multiplicities"
    );
}

#[tokio::test]
async fn test_centroid_scalar_candidate_retains_null_row_after_last_delete() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire scalar centroid session");
    for sql in [
        "SET pg_trickle.enable_vector_agg = on",
        "CREATE TABLE centroid_scalar_src (id int PRIMARY KEY, v vector(2))",
        "INSERT INTO centroid_scalar_src VALUES (1, '[1,2]')",
        "SELECT pgtrickle.create_stream_table('centroid_scalar_st', $$SELECT avg(v) AS centroid FROM centroid_scalar_src$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle._centroid_prepare_state('centroid_scalar_st'::regclass::oid)",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_scalar_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_scalar_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("set up scalar centroid");
    }
    let previous: serde_json::Value = sqlx::query_scalar("SELECT frontier FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_scalar_st'::regclass")
        .fetch_one(&mut *conn).await.expect("read scalar frontier");
    sqlx::query("DELETE FROM centroid_scalar_src")
        .execute(&mut *conn)
        .await
        .expect("delete last vector");
    let mut next = previous.clone();
    for source in next["sources"]
        .as_object_mut()
        .expect("frontier sources")
        .values_mut()
    {
        source["lsn"] = serde_json::json!("FF/FFFFFFFF");
    }
    let candidate: String = sqlx::query_scalar(
        "SELECT pgtrickle._centroid_candidate_sql('centroid_scalar_st'::regclass::oid, $1, $2)",
    )
    .bind(previous.to_string())
    .bind(next.to_string())
    .fetch_one(&mut *conn)
    .await
    .expect("generate scalar candidate");
    let sql = format!(
        "SELECT __pgt_action, centroid::text, __pgt_count, __pgt_row_id = (SELECT __pgt_row_id FROM centroid_scalar_st) FROM ({candidate}) AS result"
    );
    let row: (String, Option<String>, i64, bool) =
        sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_one(&mut *conn)
            .await
            .expect("read scalar candidate");
    assert_eq!(row, ("I".into(), None, 0, true));
    for sql in [
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('centroid_scalar_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("apply scalar centroid deletion");
    }
    let retained: bool = sqlx::query_scalar("SELECT (SELECT count(*) = 1 AND bool_and(centroid IS NULL AND __pgt_count = 0) FROM centroid_scalar_st) AND (SELECT merge_strategy_used = 'centroid_algebraic' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_scalar_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1)")
        .fetch_one(&mut *conn).await.expect("check applied empty scalar centroid");
    assert!(
        retained,
        "an empty scalar centroid must retain its NULL row"
    );
}

#[tokio::test]
async fn test_centroid_full_refresh_uses_prepared_snapshot_and_rolls_back() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "CREATE TABLE centroid_full_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_full_src VALUES (1, 1, '[1,2]'), (2, 1, NULL), (3, 2, '[3,4]')",
        "SELECT pgtrickle.create_stream_table('centroid_full_st', $$SELECT grp, avg(v) AS centroid FROM centroid_full_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle._centroid_prepare_state('centroid_full_st'::regclass::oid)",
    ]).await;
    let mut contract_change = db.pool.begin().await.expect("begin contract change");
    sqlx::query("UPDATE pgtrickle.pgt_stream_tables SET contract_generation = contract_generation + 1 WHERE pgt_relid = 'centroid_full_st'::regclass")
        .execute(&mut *contract_change).await.expect("lock and change contract");
    let mut waiter = db
        .pool
        .acquire()
        .await
        .expect("acquire preparation session");
    let waiter_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *waiter)
        .await
        .expect("get preparation backend");
    let waiting = tokio::spawn(async move {
        sqlx::query_scalar::<_, String>(
            "SELECT pgtrickle._centroid_prepare_state('centroid_full_st'::regclass::oid)",
        )
        .fetch_one(&mut *waiter)
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let blocked: bool = sqlx::query_scalar("SELECT cardinality(pg_blocking_pids($1)) > 0")
                .bind(waiter_pid)
                .fetch_one(&db.pool)
                .await
                .expect("inspect preparation lock");
            if blocked {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("preparation must wait for the catalog lock");
    contract_change
        .commit()
        .await
        .expect("commit contract change");
    let error = waiting
        .await
        .expect("join preparation task")
        .expect_err("changed contract must be rejected");
    assert!(error.to_string().contains("stream table contract changed"));
    db.execute_seq(&[
        "SELECT pgtrickle._centroid_prepare_state('centroid_full_st'::regclass::oid)",
        "UPDATE centroid_full_src SET v = '[5,6]' WHERE id = 1",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_full_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_full_st')",
    ]).await;
    let check = "SELECT s.centroid::text = '[5,6]' AND s.__pgt_count = 2 AND g.group_count = 2 AND g.state = ARRAY[1,1,0,5,6,5,6]::float8[] AND c.status = 'BUILDING' AND c.materialized_xid IS NULL AND c.baseline_frontier = t.frontier AND t.frontier IS NOT NULL FROM centroid_full_st s JOIN pgtrickle.pgt_centroid_groups g ON g.row_id = s.__pgt_row_id JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE s.grp = 1 AND t.pgt_relid = 'centroid_full_st'::regclass";
    let matches: bool = db.query_scalar(check).await;
    assert!(
        matches,
        "manual FULL refresh must use one snapshot for rows and state"
    );
    let mut rollback = db.pool.begin().await.expect("begin refresh rollback");
    sqlx::query("UPDATE centroid_full_src SET v = '[7,8]' WHERE id = 1")
        .execute(&mut *rollback)
        .await
        .expect("update source");
    sqlx::query("UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_full_st'::regclass")
        .execute(&mut *rollback).await.expect("force manual FULL refresh");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_full_st')")
        .execute(&mut *rollback)
        .await
        .expect("refresh within transaction");
    rollback.rollback().await.expect("roll back refresh");
    let restored: bool = db.query_scalar(check).await;
    assert!(
        restored,
        "rollback must restore visible rows, state, and frontier"
    );

    db.execute_seq(&[
        "UPDATE centroid_full_src SET v = '[9,10]' WHERE id = 1",
        "SET pg_trickle.force_full_refresh = on",
        "SELECT pgtrickle.refresh_stream_table('centroid_full_st')",
        "SET pg_trickle.force_full_refresh = off",
    ])
    .await;
    let updated: bool = db.query_scalar("SELECT s.centroid::text = '[9,10]' AND s.__pgt_count = 2 AND g.group_count = 2 AND g.state = ARRAY[1,1,0,9,10,9,10]::float8[] AND c.status = 'BUILDING' AND c.baseline_frontier IS NOT NULL FROM centroid_full_st s JOIN pgtrickle.pgt_centroid_groups g ON g.row_id = s.__pgt_row_id JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) WHERE s.grp = 1").await;
    assert!(
        updated,
        "differential FULL fallback must also rebuild from one snapshot"
    );
    let mut differential_rollback = db.pool.begin().await.expect("begin differential rollback");
    for statement in [
        "SET LOCAL pg_trickle.refresh_strategy = 'differential'",
        "UPDATE centroid_full_src SET v = '[11,12]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_full_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *differential_rollback)
            .await
            .expect("perform differential refresh inside rollback");
    }
    let cleared: bool = sqlx::query_scalar("SELECT baseline_frontier IS NULL FROM pgtrickle.pgt_centroid_states WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_full_st'::regclass)")
        .fetch_one(&mut *differential_rollback).await.expect("check transactional invalidation");
    assert!(cleared, "differential refresh must invalidate its baseline");
    differential_rollback
        .rollback()
        .await
        .expect("roll back differential refresh");
    let restored: bool = db.query_scalar("SELECT baseline_frontier IS NOT NULL FROM pgtrickle.pgt_centroid_states WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_full_st'::regclass)").await;
    assert!(restored, "rollback must restore the prepared baseline");
    db.execute_seq(&[
        "UPDATE centroid_full_src SET v = '[11,12]' WHERE id = 1",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('centroid_full_st')",
        "RESET pg_trickle.refresh_strategy",
    ])
    .await;
    let invalidated: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND c.materialized_xid IS NULL FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_full_st'::regclass").await;
    assert!(
        invalidated,
        "differential refresh must clear the prepared baseline"
    );
    let unbound = db.try_execute("SELECT pgtrickle._centroid_stage_candidates('centroid_full_st'::regclass::oid, frontier::text, frontier::text) FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_full_st'::regclass").await;
    assert!(
        unbound.is_err(),
        "differential refresh must invalidate the old frontier binding"
    );
    db.execute_seq(&[
        "UPDATE pgtrickle.pgt_centroid_states SET contract_generation = contract_generation - 1 WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_full_st'::regclass)",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_full_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_full_st')",
    ]).await;
    let safe_fallback: bool = db.query_scalar("SELECT NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_full_st'::regclass) AND EXISTS (SELECT 1 FROM centroid_full_st WHERE grp = 1 AND centroid::text = '[11,12]')").await;
    assert!(
        safe_fallback,
        "stale contracts must discard prepared state and retain FULL refresh"
    );
}

#[tokio::test]
async fn test_centroid_opt_in_applies_and_falls_back_after_corruption() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire centroid refresh session");
    for sql in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_live_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_live_src VALUES (1, 1, '[1,2]'), (2, 1, NULL), (3, 2, '[3,4]')",
        "SELECT pgtrickle.create_stream_table('centroid_live_st', $$SELECT grp, avg(v) AS centroid FROM centroid_live_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_live_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
        "UPDATE centroid_live_src SET grp = 2, v = '[5,6]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("execute centroid refresh setup");
    }
    let strategy: String = sqlx::query_scalar("SELECT h.merge_strategy_used FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&mut *conn).await.expect("read centroid refresh strategy");
    assert_eq!(strategy, "centroid_algebraic");
    let cost: String = sqlx::query_scalar("SELECT h.refresh_reason_detail FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&mut *conn).await.expect("read algebraic cost evidence");
    let cost: serde_json::Value =
        serde_json::from_str(&cost).expect("parse algebraic cost evidence");
    assert_eq!(cost["merge_strategy"], "centroid_algebraic");
    assert_eq!(cost["input_delta_rows"], 2);
    assert_eq!(cost["output_rows_changed"], 2);
    assert_eq!(cost["input_delta_rows_may_be_incomplete"], false);
    assert!(cost["source_rows_estimate"].as_i64().is_some());
    sqlx::query("ANALYZE centroid_live_st")
        .execute(&mut *conn)
        .await
        .expect("collect centroid group estimate");
    let fixed_payload: bool = sqlx::query_scalar("SELECT count(*) = 2 AND bool_and(octet_length(pg_catalog.array_send(g.state)) + octet_length(g.state_checksum) + 16 = 152) FROM pgtrickle.pgt_centroid_groups g JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass")
        .fetch_one(&mut *conn).await.expect("measure centroid fixed payload");
    assert!(
        fixed_payload,
        "fixture must match the fixed-payload estimate"
    );
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin explain scan check");
    let counters_sql = "SELECT COALESCE(pg_stat_get_xact_tuples_returned('centroid_live_src'::regclass), 0), COALESCE(pg_stat_get_xact_tuples_fetched('centroid_live_src'::regclass), 0), COALESCE(pg_stat_get_xact_tuples_returned('pgtrickle.pgt_centroid_groups'::regclass), 0), COALESCE(pg_stat_get_xact_tuples_fetched('pgtrickle.pgt_centroid_groups'::regclass), 0)";
    let before_reads: (i64, i64, i64, i64) = sqlx::query_as(counters_sql)
        .fetch_one(&mut *conn)
        .await
        .expect("inspect initial explain tuple reads");
    let explanation: String =
        sqlx::query_scalar("SELECT pgtrickle.explain_stream_table('centroid_live_st')")
            .fetch_one(&mut *conn)
            .await
            .expect("explain algebraic centroid refresh");
    let after_reads: (i64, i64, i64, i64) = sqlx::query_as(counters_sql)
        .fetch_one(&mut *conn)
        .await
        .expect("inspect explain tuple reads");
    assert_eq!(
        before_reads, after_reads,
        "explain must not scan source or private groups"
    );
    sqlx::query("COMMIT")
        .execute(&mut *conn)
        .await
        .expect("end explain scan check");
    assert!(
        explanation.contains("centroid: GROUP_RESCAN")
            && explanation.contains("Last refresh strategy: centroid_algebraic"),
        "explain must distinguish fallback classification from the executed strategy: {explanation}"
    );
    assert!(
        explanation
            .contains("152 serialized fixed-payload bytes/group; 2 estimated groups, 304 bytes"),
        "explain must estimate potential state without scanning groups: {explanation}"
    );
    let check: bool = sqlx::query_scalar("WITH native AS (SELECT grp, avg(v)::text AS centroid, count(*)::bigint AS n FROM centroid_live_src GROUP BY grp), actual AS (SELECT grp, centroid::text AS centroid, __pgt_count AS n FROM centroid_live_st) SELECT NOT EXISTS ((SELECT * FROM native EXCEPT ALL SELECT * FROM actual) UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM native)) AND EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL)")
        .fetch_one(&mut *conn).await.expect("check applied centroid");
    assert!(check, "visible rows and state must advance together");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin live refresh rollback");
    for sql in [
        "SET LOCAL pg_trickle.differential_max_change_ratio = 0",
        "UPDATE centroid_live_src SET v = '[9,10]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("refresh inside rollback");
    }
    let changed: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM centroid_live_st WHERE grp = 2 AND centroid::text = '[6,7]') AND (SELECT merge_strategy_used = 'centroid_algebraic' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1)")
        .fetch_one(&mut *conn).await.expect("check refresh before rollback");
    assert!(changed, "rollback test must apply an algebraic refresh");
    let uncapped: String = sqlx::query_scalar("SELECT h.refresh_reason_detail FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&mut *conn).await.expect("read uncapped cost evidence");
    let uncapped: serde_json::Value =
        serde_json::from_str(&uncapped).expect("parse uncapped cost evidence");
    assert_eq!(uncapped["input_delta_rows_may_be_incomplete"], false);
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("roll back live refresh");
    let restored: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM centroid_live_st WHERE grp = 2 AND centroid::text = '[4,5]') AND EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL)")
        .fetch_one(&mut *conn).await.expect("check live rollback");
    assert!(
        restored,
        "rollback must restore visible rows and the state frontier"
    );
    for sql in [
        "BEGIN",
        "SET LOCAL pg_trickle.differential_max_change_ratio = 0.001",
        "UPDATE pgtrickle.pgt_stream_tables SET auto_threshold = NULL WHERE pgt_relid = 'centroid_live_st'::regclass",
        "UPDATE centroid_live_src SET v = '[9,10]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("exercise the bounded change probe");
    }
    let bounded: String = sqlx::query_scalar("SELECT h.refresh_reason_detail FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&mut *conn).await.expect("read bounded cost evidence");
    let bounded: serde_json::Value =
        serde_json::from_str(&bounded).expect("parse bounded cost evidence");
    assert_eq!(bounded["input_delta_rows_may_be_incomplete"], true);
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("roll back bounded refresh");
    for sql in [
        "CREATE ROLE centroid_live_owner NOLOGIN",
        "GRANT USAGE ON SCHEMA pgtrickle TO centroid_live_owner",
        "GRANT EXECUTE ON FUNCTION pgtrickle.refresh_stream_table(text) TO centroid_live_owner",
        "GRANT SELECT ON centroid_live_src TO centroid_live_owner",
        "ALTER TABLE centroid_live_st OWNER TO centroid_live_owner",
        "UPDATE centroid_live_src SET v = '[11,12]' WHERE id = 1",
        "SET ROLE centroid_live_owner",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
        "RESET ROLE",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("refresh centroid as stream owner");
    }
    let owner_refresh: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM centroid_live_st WHERE grp = 2 AND centroid::text = '[7,8]') AND (SELECT merge_strategy_used = 'centroid_algebraic' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1)")
        .fetch_one(&mut *conn).await.expect("check owner refresh");
    assert!(
        owner_refresh,
        "a non-superuser owner must use the algebraic path"
    );
    for sql in [
        "UPDATE pgtrickle.pgt_centroid_groups SET state[4] = state[4] + 1 WHERE row_id = (SELECT __pgt_row_id FROM centroid_live_st WHERE grp = 2)",
        "UPDATE centroid_live_src SET v = '[7,8]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("execute corruption fallback");
    }
    let fallback: bool = sqlx::query_scalar("SELECT (SELECT merge_strategy_used = 'centroid_rebase_group_rescan' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) AND EXISTS (SELECT 1 FROM centroid_live_st WHERE grp = 2 AND centroid::text = '[5,6]') AND EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL)")
        .fetch_one(&mut *conn).await.expect("check corruption fallback");
    assert!(
        fallback,
        "corrupt state must be rebuilt after the native group rescan"
    );
    let fallback_reason: String = sqlx::query_scalar("SELECT h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' FROM pgtrickle.pgt_stream_tables t CROSS JOIN LATERAL (SELECT refresh_reason_detail FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h WHERE t.pgt_relid = 'centroid_live_st'::regclass")
        .fetch_one(&mut *conn).await.expect("read rejected-state fallback reason");
    assert_eq!(fallback_reason, "STATE_REJECTED");
    let explanation: String =
        sqlx::query_scalar("SELECT pgtrickle.explain_stream_table('centroid_live_st')")
            .fetch_one(&mut *conn)
            .await
            .expect("explain centroid rebase");
    assert!(
        explanation.contains("Last refresh strategy: centroid_rebase_group_rescan"),
        "explain must report the executed rebase: {explanation}"
    );
    for sql in [
        "INSERT INTO centroid_live_src VALUES (4, 3, '[8,9]')",
        "UPDATE centroid_live_src SET v = '[2,4]' WHERE id = 2",
        "DELETE FROM centroid_live_src WHERE id = 3",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("rebuild and apply centroid changes");
    }
    let recovered: bool = sqlx::query_scalar("WITH native AS (SELECT grp, avg(v)::text AS centroid, count(*)::bigint AS n FROM centroid_live_src GROUP BY grp), actual AS (SELECT grp, centroid::text AS centroid, __pgt_count AS n FROM centroid_live_st) SELECT NOT EXISTS ((SELECT * FROM native EXCEPT ALL SELECT * FROM actual) UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM native)) AND (SELECT merge_strategy_used = 'centroid_algebraic' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1)")
        .fetch_one(&mut *conn).await.expect("check recovered centroid");
    assert!(
        recovered,
        "rebuilt state must handle inserts, deletes, and NULL transitions"
    );
    for sql in [
        "UPDATE pgtrickle.pgt_centroid_groups SET state[2] = 4096, state[3] = 4096 WHERE row_id = (SELECT __pgt_row_id FROM centroid_live_st WHERE grp = 2)",
        "UPDATE pgtrickle.pgt_centroid_groups SET state_checksum = pg_catalog.md5(pg_catalog.int8send(pgt_id) || row_id || pg_catalog.int8send(group_count) || pg_catalog.array_send(state)) WHERE row_id = (SELECT __pgt_row_id FROM centroid_live_st WHERE grp = 2)",
        "UPDATE centroid_live_src SET v = '[9,10]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("force bounded centroid rebase");
    }
    let change_limit: bool = sqlx::query_scalar("SELECT merge_strategy_used = 'centroid_rebase_group_rescan' AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'reason' LIKE '%ChangeLimit%' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&mut *conn).await.expect("check centroid rebase reason");
    assert!(
        change_limit,
        "change-limit rebases must be visible in refresh history"
    );
    for sql in [
        "DELETE FROM centroid_live_src WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("remove last group member");
    }
    let removed: bool = sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM centroid_live_st WHERE grp = 2) AND (SELECT merge_strategy_used = 'centroid_algebraic' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1)")
        .fetch_one(&mut *conn).await.expect("check empty group removal");
    assert!(removed, "the last deletion must remove its group");
    for sql in [
        "SELECT pgtrickle.create_stream_table('centroid_live_child', $$SELECT grp, centroid::text AS centroid FROM centroid_live_st$$, '1h', 'DIFFERENTIAL')",
        "UPDATE centroid_live_src SET v = '[4,6]' WHERE id = 2",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .unwrap_or_else(|error| panic!("downstream statement {sql}: {error}"));
    }
    let pgt_id: i64 = sqlx::query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_live_st'::regclass")
        .fetch_one(&mut *conn).await.expect("read downstream source ID");
    let capture = format!("pgtrickle_changes.changes_pgt_{pgt_id}");
    let check = format!(
        "SELECT (SELECT merge_strategy_used <> 'centroid_algebraic' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_live_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) AND EXISTS (SELECT 1 FROM {capture} WHERE action = 'D' AND grp = 1 AND centroid::text = '[2,4]') AND EXISTS (SELECT 1 FROM {capture} WHERE action = 'I' AND grp = 1 AND centroid::text = '[4,6]')"
    );
    let downstream: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(check.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect("check downstream fallback");
    assert!(
        downstream,
        "downstream consumers must retain exact capture through fallback"
    );
    sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_live_child')")
        .execute(&mut *conn)
        .await
        .expect("refresh downstream centroid child");
    let child_matches: bool = sqlx::query_scalar("SELECT NOT EXISTS ((SELECT grp, centroid::text FROM centroid_live_st EXCEPT ALL SELECT grp, centroid FROM centroid_live_child) UNION ALL (SELECT grp, centroid FROM centroid_live_child EXCEPT ALL SELECT grp, centroid::text FROM centroid_live_st))")
        .fetch_one(&mut *conn).await.expect("compare downstream centroid child");
    assert!(
        child_matches,
        "downstream centroid child must match the parent"
    );
    for sql in [
        "UPDATE centroid_live_src SET v = NULL WHERE id = 2",
        "SELECT pgtrickle.refresh_stream_table('centroid_live_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("refresh downstream NULL transition");
    }
    let check = format!(
        "SELECT EXISTS (SELECT 1 FROM {capture} WHERE action = 'D' AND grp = 1 AND centroid::text = '[4,6]') AND EXISTS (SELECT 1 FROM {capture} WHERE action = 'I' AND grp = 1 AND centroid IS NULL)"
    );
    let null_transition: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(check.as_str()))
        .fetch_one(&mut *conn)
        .await
        .expect("check downstream NULL transition");
    assert!(
        null_transition,
        "downstream capture must preserve NULL transitions"
    );
    sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_live_child')")
        .execute(&mut *conn)
        .await
        .expect("refresh downstream NULL transition");
    let child_null: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM centroid_live_child WHERE grp = 1 AND centroid IS NULL)",
    )
    .fetch_one(&mut *conn)
    .await
    .expect("check downstream NULL result");
    assert!(child_null, "the child must receive the NULL centroid");
    for sql in [
        "CREATE TABLE centroid_unrelated_src (id int PRIMARY KEY, v int)",
        "INSERT INTO centroid_unrelated_src VALUES (1, 2)",
        "SELECT pgtrickle.create_stream_table('centroid_unrelated_st', $$SELECT sum(v) AS total FROM centroid_unrelated_src$$, '1h', 'DIFFERENTIAL')",
        "INSERT INTO centroid_unrelated_src VALUES (2, 3)",
        "SELECT pgtrickle.refresh_stream_table('centroid_unrelated_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("refresh unrelated aggregate with centroid opt-in");
    }
    let unrelated: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM centroid_unrelated_st WHERE total = 5) AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_unrelated_st'::regclass)")
        .fetch_one(&mut *conn).await.expect("check unrelated aggregate");
    assert!(
        unrelated,
        "centroid opt-in must not change unrelated aggregates"
    );
    for sql in [
        "SELECT pgtrickle.create_stream_table('centroid_sum_st', $$SELECT grp, sum(v) AS total FROM centroid_live_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE centroid_live_src SET v = '[6,7]' WHERE id = 4",
        "SELECT pgtrickle.refresh_stream_table('centroid_sum_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("refresh vector SUM with centroid opt-in");
    }
    let sum_fallback: bool = sqlx::query_scalar("WITH native AS (SELECT grp, sum(v)::text AS total FROM centroid_live_src GROUP BY grp), actual AS (SELECT grp, total::text AS total FROM centroid_sum_st) SELECT NOT EXISTS ((SELECT * FROM native EXCEPT ALL SELECT * FROM actual) UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM native)) AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_sum_st'::regclass)")
        .fetch_one(&mut *conn).await.expect("check vector SUM fallback");
    assert!(
        sum_fallback,
        "vector SUM must retain its own refresh strategy"
    );
}

#[tokio::test]
async fn test_centroid_randomized_refresh_churn_matches_native_avg() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db.pool.acquire().await.expect("acquire churn session");
    for sql in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_churn_src (id int PRIMARY KEY, grp int NOT NULL, v vector(3))",
        "INSERT INTO centroid_churn_src SELECT id, id % 5, format('[%s,%s,%s]', CASE WHEN id % 2 = 0 THEN 10000 ELSE -10000 END, id, -id)::vector FROM generate_series(1, 30) AS id",
        "SELECT pgtrickle.create_stream_table('centroid_churn_st', $$SELECT grp, avg(v) AS centroid FROM centroid_churn_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_churn_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_churn_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("set up centroid churn");
    }

    let bound: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_churn_st'::regclass AND c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL)")
        .fetch_one(&mut *conn).await.expect("inspect initial churn state");
    assert!(bound, "churn fixture must initialize centroid state");

    let mut seed = 0x7a13_9465_u64;
    let mut algebraic = 0;
    for step in 0..160 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = if step == 0 {
            999
        } else {
            ((seed >> 32) % 48 + 1) as i32
        };
        let group = ((seed >> 24) % 5) as i32;
        if step % 7 == 0 {
            sqlx::query("DELETE FROM centroid_churn_src WHERE id = $1")
                .bind(id)
                .execute(&mut *conn)
                .await
                .expect("delete churn row");
        } else {
            let vector = (step % 9 != 0).then(|| {
                let x = if seed & 1 == 0 { 10000.0 } else { -10000.0 };
                format!(
                    "[{x},{},{}]",
                    (seed % 1000) as f64 / 17.0,
                    step as f64 / -13.0
                )
            });
            sqlx::query("INSERT INTO centroid_churn_src VALUES ($1, $2, $3::text::vector) ON CONFLICT (id) DO UPDATE SET grp = EXCLUDED.grp, v = EXCLUDED.v")
                .bind(id).bind(group).bind(vector).execute(&mut *conn).await.expect("upsert churn row");
        }
        sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_churn_st')")
            .execute(&mut *conn)
            .await
            .expect("refresh centroid churn");
        let matches: bool = sqlx::query_scalar(
            "WITH native AS (SELECT grp, avg(v)::real[] AS v, count(*) AS n FROM centroid_churn_src GROUP BY grp), \
             actual AS (SELECT grp, centroid::real[] AS v, __pgt_count AS n FROM centroid_churn_st) \
             SELECT NOT EXISTS (SELECT 1 FROM native n FULL JOIN actual a USING (grp) \
             WHERE n.n IS DISTINCT FROM a.n OR (n.v IS NULL) <> (a.v IS NULL) OR \
               EXISTS (SELECT 1 FROM generate_series(1, 3) AS i \
                 WHERE abs(n.v[i]::float8 - a.v[i]::float8) > 1e-5 + 1e-6 * abs(n.v[i]::float8)))"
        ).fetch_one(&mut *conn).await.expect("compare churn with native AVG");
        assert!(matches, "centroid diverged from native AVG at step {step}");
        let used_algebraic: bool = sqlx::query_scalar("SELECT merge_strategy_used = 'centroid_algebraic' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_churn_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
            .fetch_one(&mut *conn).await.expect("inspect churn strategy");
        let bound: bool = sqlx::query_scalar("SELECT COALESCE(c.baseline_frontier = t.frontier, false) FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) WHERE t.pgt_relid = 'centroid_churn_st'::regclass")
            .fetch_one(&mut *conn).await.expect("inspect churn state");
        assert!(
            bound,
            "churn state became unbound at step {step}, algebraic={used_algebraic}"
        );
        algebraic += usize::from(used_algebraic);
    }
    assert!(
        algebraic >= 100,
        "only {algebraic} refreshes used algebraic state"
    );
}

#[tokio::test]
async fn test_centroid_crash_recovers_committed_source_and_rolls_back_refresh() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire crash fixture session");
    let durable: bool = sqlx::query_scalar("SELECT current_setting('fsync') = 'on' AND current_setting('synchronous_commit') IN ('on', 'local', 'remote_write', 'remote_apply')")
        .fetch_one(&mut *conn).await.expect("check crash fixture durability");
    assert!(durable, "crash test requires durable commits");
    for sql in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_crash_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_crash_src VALUES (1, 1, '[1,2]'), (2, 1, '[3,4]')",
        "SELECT pgtrickle.create_stream_table('centroid_crash_st', $$SELECT grp, avg(v) AS centroid FROM centroid_crash_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_crash_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_crash_st')",
        "UPDATE centroid_crash_src SET v = '[5,6]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_crash_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("prepare crash fixture");
    }
    let state_sql = "SELECT g.state, g.state_checksum, t.frontier::text FROM pgtrickle.pgt_centroid_groups g JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_crash_st'::regclass AND c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL";
    let before: (Vec<f64>, String, String) = sqlx::query_as(state_sql)
        .fetch_one(&mut *conn)
        .await
        .expect("capture committed centroid state");
    let algebraic: bool = sqlx::query_scalar("SELECT h.merge_strategy_used = 'centroid_algebraic' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_crash_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1")
        .fetch_one(&mut *conn).await.expect("check pre-crash strategy");
    assert!(algebraic, "fixture must establish live algebraic state");
    for sql in [
        "UPDATE centroid_crash_src SET v = '[7,8]' WHERE id = 2",
        "BEGIN",
        "UPDATE centroid_crash_src SET v = '[99,99]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_crash_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("prepare uncommitted refresh");
    }
    let uncommitted: bool = sqlx::query_scalar(
        "SELECT s.centroid::text = '[53,53.5]' AND g.state_checksum <> $1 AND h.merge_strategy_used = 'centroid_algebraic' FROM centroid_crash_st s JOIN pgtrickle.pgt_centroid_groups g ON g.row_id = s.__pgt_row_id JOIN pgtrickle.pgt_stream_tables t ON t.pgt_id = g.pgt_id JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_crash_st'::regclass AND s.grp = 1",
    )
    .bind(&before.1)
    .fetch_one(&mut *conn)
    .await
    .expect("check uncommitted refresh");
    assert!(
        uncommitted,
        "fixture must apply both visible and private algebraic changes before the crash"
    );
    let killed = tokio::process::Command::new("docker")
        .args(["kill", "--signal", "KILL", db.container_id()])
        .output()
        .await
        .expect("kill PostgreSQL container");
    assert!(
        killed.status.success(),
        "{}",
        String::from_utf8_lossy(&killed.stderr)
    );
    drop(conn);
    db.pool.close().await;
    let started = tokio::process::Command::new("docker")
        .args(["start", db.container_id()])
        .output()
        .await
        .expect("start PostgreSQL container");
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    db.pool = db.reconnect_after_restart().await;
    let mut conn = db.pool.acquire().await.expect("reconnect after crash");
    let restored: (Vec<f64>, String, String) = sqlx::query_as(state_sql)
        .fetch_one(&mut *conn)
        .await
        .expect("read recovered centroid state");
    assert_eq!(
        restored, before,
        "crash must roll back state and frontier together"
    );
    let source_and_visible: bool = sqlx::query_scalar("SELECT (SELECT v::text FROM centroid_crash_src WHERE id = 1) = '[5,6]' AND (SELECT v::text FROM centroid_crash_src WHERE id = 2) = '[7,8]' AND (SELECT centroid::text FROM centroid_crash_st WHERE grp = 1) = '[4,5]'")
        .fetch_one(&mut *conn).await.expect("inspect crash recovery");
    assert!(
        source_and_visible,
        "committed source update must survive while the uncommitted refresh rolls back"
    );
    for sql in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('centroid_crash_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .expect("refresh recovered centroid");
    }
    let recovered: bool = sqlx::query_scalar("SELECT public.vector_send(s.centroid) = public.vector_send((SELECT avg(v) FROM centroid_crash_src)) AND s.centroid::text = '[6,7]' AND c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL AND h.merge_strategy_used = 'centroid_algebraic' FROM centroid_crash_st s CROSS JOIN pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_crash_st'::regclass")
        .fetch_one(&mut *conn).await.expect("check post-crash catch-up");
    assert!(
        recovered,
        "algebraic refresh must apply the committed source change after crash recovery"
    );
}

#[tokio::test]
async fn test_centroid_refresh_serializes_concurrent_source_writer() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut refresh = db.pool.acquire().await.expect("acquire refresh session");
    for sql in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_concurrent_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_concurrent_src VALUES (1, 1, '[1,2]'), (2, 1, '[3,4]')",
        "SELECT pgtrickle.create_stream_table('centroid_concurrent_st', $$SELECT grp, avg(v) AS centroid FROM centroid_concurrent_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_concurrent_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_concurrent_st')",
        "UPDATE centroid_concurrent_src SET v = '[5,6]' WHERE id = 1",
        "BEGIN",
        "SELECT pgtrickle.refresh_stream_table('centroid_concurrent_st')",
    ] {
        sqlx::query(sql)
            .execute(&mut *refresh)
            .await
            .expect("prepare concurrent centroid refresh");
    }
    let applied: bool = sqlx::query_scalar("SELECT h.merge_strategy_used = 'centroid_algebraic' AND c.baseline_frontier = t.frontier FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_concurrent_st'::regclass")
        .fetch_one(&mut *refresh).await.expect("inspect protected centroid refresh");
    assert!(
        applied,
        "fixture must hold a completed algebraic refresh open"
    );
    let refresh_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *refresh)
        .await
        .expect("read refresh backend");
    let mut writer = db.pool.acquire().await.expect("acquire writer session");
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .expect("read writer backend");
    let write = tokio::spawn(async move {
        sqlx::query("UPDATE centroid_concurrent_src SET v = '[7,8]' WHERE id = 2")
            .execute(&mut *writer)
            .await
            .expect("execute concurrent source update");
    });
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let blocked: bool =
                sqlx::query_scalar("SELECT $1 = ANY(pg_catalog.pg_blocking_pids($2))")
                    .bind(refresh_pid)
                    .bind(writer_pid)
                    .fetch_one(&db.pool)
                    .await
                    .expect("inspect writer lock");
            if blocked {
                break;
            }
            assert!(
                !write.is_finished(),
                "writer completed before the protected refresh committed"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("writer did not wait for the centroid source lock");
    sqlx::query("COMMIT")
        .execute(&mut *refresh)
        .await
        .expect("commit protected refresh");
    write.await.expect("join concurrent source update");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_concurrent_st')")
        .execute(&mut *refresh)
        .await
        .expect("refresh concurrent source update");
    let correct: bool = sqlx::query_scalar("SELECT public.vector_send(s.centroid) = public.vector_send((SELECT avg(v) FROM centroid_concurrent_src)) AND s.centroid::text = '[6,7]' AND c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL AND h.merge_strategy_used = 'centroid_algebraic' FROM centroid_concurrent_st s CROSS JOIN pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_concurrent_st'::regclass")
        .fetch_one(&mut *refresh).await.expect("compare concurrent refresh with native AVG");
    assert!(
        correct,
        "the committed writer must appear in the next algebraic refresh"
    );
    sqlx::query("UPDATE centroid_concurrent_src SET v = '[9,10]' WHERE id = 1")
        .execute(&mut *refresh)
        .await
        .expect("create a pending source change");
    let mut writer = db.pool.begin().await.expect("begin concurrent writer");
    let writer_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *writer)
        .await
        .expect("read concurrent writer backend");
    sqlx::query("UPDATE centroid_concurrent_src SET v = '[11,12]' WHERE id = 2")
        .execute(&mut *writer)
        .await
        .expect("hold uncommitted source update");
    let waiting_refresh = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_concurrent_st')")
            .execute(&mut *refresh)
            .await
            .expect("refresh while writer holds source lock");
        refresh
    });
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let blocked: bool =
                sqlx::query_scalar("SELECT $1 = ANY(pg_catalog.pg_blocking_pids($2))")
                    .bind(writer_pid)
                    .bind(refresh_pid)
                    .fetch_one(&db.pool)
                    .await
                    .expect("inspect refresh lock");
            if blocked {
                break;
            }
            assert!(
                !waiting_refresh.is_finished(),
                "refresh completed before the writer committed"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("centroid refresh did not wait for the concurrent writer");
    writer.commit().await.expect("commit concurrent writer");
    let mut refresh = waiting_refresh.await.expect("join waiting refresh");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_concurrent_st')")
        .execute(&mut *refresh)
        .await
        .expect("refresh after concurrent writer");
    let correct: bool = sqlx::query_scalar("SELECT public.vector_send(centroid) = public.vector_send((SELECT avg(v) FROM centroid_concurrent_src)) AND centroid::text = '[10,11]' FROM centroid_concurrent_st WHERE grp = 1")
        .fetch_one(&mut *refresh).await.expect("compare writer-first refresh with native AVG");
    assert!(
        correct,
        "both committed updates must survive the concurrent refresh"
    );
    sqlx::query("UPDATE centroid_concurrent_src SET v = '[13,14]' WHERE id = 2")
        .execute(&mut *refresh)
        .await
        .expect("update source after concurrency");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_concurrent_st')")
        .execute(&mut *refresh)
        .await
        .expect("refresh after concurrency");
    let correct_again: bool = sqlx::query_scalar("SELECT public.vector_send(centroid) = public.vector_send((SELECT avg(v) FROM centroid_concurrent_src)) AND centroid::text = '[11,12]' FROM centroid_concurrent_st WHERE grp = 1")
        .fetch_one(&mut *refresh).await.expect("check post-concurrency refresh");
    assert!(
        correct_again,
        "a later refresh must not reuse stale centroid state"
    );
}

#[tokio::test]
async fn test_centroid_missing_state_rebuilds_or_stays_unbound() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_missing_src (id int PRIMARY KEY, grp int, v vector(1))",
        "INSERT INTO centroid_missing_src VALUES (1, 1, '[1]'), (2, 1, '[3]')",
        "SELECT pgtrickle.create_stream_table('centroid_missing_st', $$SELECT grp, avg(v) AS centroid FROM centroid_missing_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_missing_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_missing_st')",
        "DELETE FROM pgtrickle.pgt_centroid_states WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_missing_st'::regclass)",
        "UPDATE centroid_missing_src SET v = '[5]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_missing_st')",
    ]).await;
    let rebuilt: bool = db.query_scalar("SELECT c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL AND h.merge_strategy_used = 'centroid_rebase_group_rescan' AND (h.refresh_reason_detail::jsonb->'centroid_rebase'->>'reason') = 'centroid private state is missing' AND EXISTS (SELECT 1 FROM centroid_missing_st WHERE centroid::text = '[4]') FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_missing_st'::regclass").await;
    assert!(
        rebuilt,
        "missing state should be rebuilt after a verified group rescan"
    );
    let fallback_reason: String = db.query_scalar("SELECT h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' FROM pgtrickle.pgt_stream_tables t CROSS JOIN LATERAL (SELECT refresh_reason_detail FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h WHERE t.pgt_relid = 'centroid_missing_st'::regclass").await;
    assert_eq!(fallback_reason, "STATE_MISSING");
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "UPDATE centroid_missing_src SET v = '[7]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_missing_st')",
    ])
    .await;
    let resumed: bool = db.query_scalar("SELECT h.merge_strategy_used = 'centroid_algebraic' AND EXISTS (SELECT 1 FROM centroid_missing_st WHERE centroid::text = '[5]') FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_missing_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(
        resumed,
        "verified recovery must resume algebraic maintenance"
    );
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_missing_unsafe_src (id int PRIMARY KEY, grp int, v vector(1))",
        "INSERT INTO centroid_missing_unsafe_src VALUES (1, 1, '[1e20]'), (2, 1, '[-1e20]'), (3, 1, '[1]')",
        "SELECT pgtrickle.create_stream_table('centroid_missing_unsafe_st', $$SELECT grp, avg(v) AS centroid FROM centroid_missing_unsafe_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_missing_unsafe_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_missing_unsafe_st')",
        "DELETE FROM pgtrickle.pgt_centroid_states WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_missing_unsafe_st'::regclass)",
        "UPDATE centroid_missing_unsafe_src SET v = '[2]' WHERE id = 3",
        "SELECT pgtrickle.refresh_stream_table('centroid_missing_unsafe_st')",
    ]).await;
    let unbound: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'outcome' = 'unbound' FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_missing_unsafe_st'::regclass").await;
    assert!(
        unbound,
        "unsafe missing-state rebuild must leave an unbound marker"
    );
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "UPDATE centroid_missing_unsafe_src SET v = '[3]' WHERE id = 3",
        "SELECT pgtrickle.refresh_stream_table('centroid_missing_unsafe_st')",
    ])
    .await;
    let no_retry: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND NOT (h.refresh_reason_detail::jsonb ? 'centroid_rebase') AND public.vector_send(s.centroid) = public.vector_send((SELECT avg(v) FROM centroid_missing_unsafe_src)) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true CROSS JOIN centroid_missing_unsafe_st s WHERE t.pgt_relid = 'centroid_missing_unsafe_st'::regclass").await;
    assert!(
        no_retry,
        "failed missing-state rebuild must not retry on every refresh"
    );
}

#[tokio::test]
async fn test_centroid_repeated_large_rewrites_do_not_rebase_every_refresh() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "CREATE TABLE centroid_rewrite_src (id int PRIMARY KEY, grp int, v vector(1))",
        "INSERT INTO centroid_rewrite_src SELECT id, 1, '[1]'::vector FROM generate_series(1, 2100) id",
        "SELECT pgtrickle.create_stream_table('centroid_rewrite_st', $$SELECT grp, avg(v) AS centroid FROM centroid_rewrite_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_rewrite_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_rewrite_st')",
        "UPDATE centroid_rewrite_src SET v = '[2]'",
        "SELECT pgtrickle.refresh_stream_table('centroid_rewrite_st')",
    ]).await;
    let first: bool = db.query_scalar("SELECT c.baseline_frontier = t.frontier AND c.last_change_limit_rebase AND h.merge_strategy_used = 'centroid_rebase_group_rescan' AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'reason' LIKE '%ChangeLimit%' FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_rewrite_st'::regclass").await;
    assert!(
        first,
        "first large rewrite must demonstrate a change-limit rebase"
    );
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "UPDATE centroid_rewrite_src SET v = '[2.5]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_rewrite_st')",
    ])
    .await;
    let cleared: bool = db.query_scalar("SELECT NOT c.last_change_limit_rebase AND c.baseline_frontier = t.frontier AND h.merge_strategy_used = 'centroid_algebraic' FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_rewrite_st'::regclass").await;
    assert!(
        cleared,
        "productive algebraic maintenance must clear the prior rebase marker"
    );
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "UPDATE centroid_rewrite_src SET v = '[2]'",
        "SELECT pgtrickle.refresh_stream_table('centroid_rewrite_st')",
    ])
    .await;
    let rebased_again: bool = db.query_scalar("SELECT c.last_change_limit_rebase AND c.baseline_frontier = t.frontier AND h.merge_strategy_used = 'centroid_rebase_group_rescan' FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_rewrite_st'::regclass").await;
    assert!(
        rebased_again,
        "an intervening algebraic refresh must allow a later rebase"
    );
    db.execute("DELETE FROM pgtrickle.pgt_refresh_history WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_rewrite_st'::regclass)").await;
    let restarted = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart centroid rewrite fixture");
    assert!(
        restarted.status.success(),
        "{}",
        String::from_utf8_lossy(&restarted.stderr)
    );
    db.pool.close().await;
    db.pool = db.reconnect_after_restart().await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.refresh_stream_table('centroid_rewrite_st')",
        "UPDATE centroid_rewrite_src SET v = '[3]'",
        "SELECT pgtrickle.refresh_stream_table('centroid_rewrite_st')",
    ])
    .await;
    let suppressed: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND h.merge_strategy_used <> 'centroid_rebase_group_rescan' AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'outcome' = 'unbound' AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'rebuild' = 'suppressed_consecutive_change_limit' AND public.vector_send(s.centroid) = public.vector_send((SELECT avg(v) FROM centroid_rewrite_src)) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true CROSS JOIN centroid_rewrite_st s WHERE t.pgt_relid = 'centroid_rewrite_st'::regclass").await;
    assert!(
        suppressed,
        "back-to-back change-limit failures must not rebuild all private groups repeatedly"
    );
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "UPDATE centroid_rewrite_src SET v = '[4]'",
        "SELECT pgtrickle.refresh_stream_table('centroid_rewrite_st')",
    ])
    .await;
    let no_retry: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND NOT (h.refresh_reason_detail::jsonb ? 'centroid_rebase') AND public.vector_send(s.centroid) = public.vector_send((SELECT avg(v) FROM centroid_rewrite_src)) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true CROSS JOIN centroid_rewrite_st s WHERE t.pgt_relid = 'centroid_rewrite_st'::regclass").await;
    assert!(
        no_retry,
        "later high-rewrite refreshes must remain correct without another rebase"
    );
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_rewrite_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_rewrite_st')",
        "UPDATE centroid_rewrite_src SET v = '[5]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_rewrite_st')",
    ]).await;
    let resumed: bool = db.query_scalar("SELECT c.baseline_frontier = t.frontier AND h.merge_strategy_used = 'centroid_algebraic' AND public.vector_send(s.centroid) = public.vector_send((SELECT avg(v) FROM centroid_rewrite_src)) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true CROSS JOIN centroid_rewrite_st s WHERE t.pgt_relid = 'centroid_rewrite_st'::regclass").await;
    assert!(
        resumed,
        "a protected FULL refresh must allow algebraic maintenance to resume"
    );
}

#[tokio::test]
async fn test_centroid_unsafe_rebase_remains_unbound_without_retry_storm() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_unstable_src (id int PRIMARY KEY, grp int, v vector(1))",
        "INSERT INTO centroid_unstable_src VALUES (1, 1, '[1e20]'), (2, 1, '[-1e20]'), (3, 1, '[1]')",
        "SELECT pgtrickle.create_stream_table('centroid_unstable_st', $$SELECT grp, avg(v) AS centroid FROM centroid_unstable_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_unstable_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_unstable_st')",
    ]).await;
    let unsafe_baseline: bool = db.query_scalar("SELECT c.baseline_frontier = t.frontier AND pgtrickle._centroid_rebase_reason(g.state[1]::bigint, g.state[2]::bigint, g.state[3]::bigint, g.state[4:4], g.state[5:5]) = 'NUMERICAL_ERROR' FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN pgtrickle.pgt_centroid_groups g USING (pgt_id) WHERE t.pgt_relid = 'centroid_unstable_st'::regclass").await;
    assert!(
        unsafe_baseline,
        "fixture must exceed the numerical error guard"
    );
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "UPDATE centroid_unstable_src SET v = '[2]' WHERE id = 3",
        "SELECT pgtrickle.refresh_stream_table('centroid_unstable_st')",
    ])
    .await;
    let rejected: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'outcome' = 'unbound' AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'reason' LIKE '%NumericalError%' AND public.vector_send(s.centroid) = public.vector_send((SELECT avg(v) FROM centroid_unstable_src)) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true CROSS JOIN centroid_unstable_st s WHERE t.pgt_relid = 'centroid_unstable_st'::regclass").await;
    assert!(
        rejected,
        "unsafe rebuild must stay unbound and report its trigger"
    );
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "UPDATE centroid_unstable_src SET v = '[3]' WHERE id = 3",
        "SELECT pgtrickle.refresh_stream_table('centroid_unstable_st')",
    ])
    .await;
    let no_retry: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND NOT (h.refresh_reason_detail::jsonb ? 'centroid_rebase') AND public.vector_send(s.centroid) = public.vector_send((SELECT avg(v) FROM centroid_unstable_src)) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true CROSS JOIN centroid_unstable_st s WHERE t.pgt_relid = 'centroid_unstable_st'::regclass").await;
    assert!(
        no_retry,
        "unbound state must use the safe strategy without another rebuild"
    );
    let fallback_reason: String = db.query_scalar("SELECT h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' FROM pgtrickle.pgt_stream_tables t CROSS JOIN LATERAL (SELECT refresh_reason_detail FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h WHERE t.pgt_relid = 'centroid_unstable_st'::regclass").await;
    assert_eq!(fallback_reason, "STATE_UNBOUND");
}

#[tokio::test]
async fn test_centroid_rebase_rejects_mismatched_visible_groups() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_rebase_mismatch_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_rebase_mismatch_src VALUES (1, 1, '[1,2]'), (2, 2, '[3,4]')",
        "SELECT pgtrickle.create_stream_table('centroid_rebase_mismatch_st', $$SELECT grp, avg(v) AS centroid FROM centroid_rebase_mismatch_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_rebase_mismatch_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_rebase_mismatch_st')",
        "BEGIN",
        "SET LOCAL pg_trickle.internal_refresh = 'true'",
        "UPDATE centroid_rebase_mismatch_st SET centroid = '[99,99]' WHERE grp = 1",
        "COMMIT",
        "UPDATE pgtrickle.pgt_centroid_groups SET state[4] = state[4] + 1 WHERE row_id = (SELECT __pgt_row_id FROM centroid_rebase_mismatch_st WHERE grp = 2)",
        "UPDATE centroid_rebase_mismatch_src SET v = '[5,6]' WHERE id = 2",
        "SELECT pgtrickle.refresh_stream_table('centroid_rebase_mismatch_st')",
    ]).await;
    let safe: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM centroid_rebase_mismatch_st WHERE grp = 1 AND centroid::text = '[99,99]') AND EXISTS (SELECT 1 FROM centroid_rebase_mismatch_st WHERE grp = 2 AND centroid::text = '[5,6]') AND EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_rebase_mismatch_st'::regclass AND c.baseline_frontier IS NULL) AND (SELECT merge_strategy_used <> 'centroid_rebase_group_rescan' FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_rebase_mismatch_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1)").await;
    assert!(
        safe,
        "rebase must refuse to bind state when visible groups disagree"
    );
    let detail: String = db.query_scalar("SELECT h.refresh_reason_detail FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_rebase_mismatch_st'::regclass AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1").await;
    let evidence: serde_json::Value = serde_json::from_str(&detail)
        .unwrap_or_else(|error| panic!("invalid rebase evidence {detail:?}: {error}"));
    assert_eq!(
        evidence["centroid_rebase"]["outcome"], "unbound",
        "{detail}"
    );
}

#[tokio::test]
async fn test_centroid_cost_model_accepts_real_refresh_history() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db.pool.acquire().await.expect("centroid history session");
    for statement in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = off",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "CREATE TABLE centroid_history_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_history_src SELECT id, id % 2, '[1,2]'::vector FROM generate_series(1, 4) id",
        "SELECT pgtrickle.create_stream_table('centroid_history_st', $$SELECT grp, avg(v) AS centroid FROM centroid_history_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle.pause_scheduler(ARRAY['public.centroid_history_st'])",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("prepare real centroid history");
    }
    for _ in 0..3 {
        sqlx::query("UPDATE centroid_history_src SET v = v + '[1,1]'::vector WHERE id IN (1, 2)")
            .execute(&mut *conn)
            .await
            .expect("mutate rescan input");
        sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_history_st')")
            .execute(&mut *conn)
            .await
            .expect("record rescan sample");
    }
    for statement in [
        "SET pg_trickle.enable_centroid_state = on",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_history_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_history_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("bind algebraic state");
    }
    for _ in 0..3 {
        sqlx::query("UPDATE centroid_history_src SET v = v + '[1,1]'::vector WHERE id IN (1, 2)")
            .execute(&mut *conn)
            .await
            .expect("mutate algebraic input");
        sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_history_st')")
            .execute(&mut *conn)
            .await
            .expect("record algebraic sample");
    }
    let (rescans, algebraic): (i64, i64) = sqlx::query_as(
        "SELECT count(*) FILTER (WHERE h.merge_strategy_used IN ('merge', 'merge_prepared', 'explicit_dml')), \
                count(*) FILTER (WHERE h.merge_strategy_used = 'centroid_algebraic') \
         FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) \
         WHERE t.pgt_relid = 'centroid_history_st'::regclass AND h.action = 'DIFFERENTIAL' \
           AND h.status = 'COMPLETED' AND h.end_time > h.start_time \
           AND (h.refresh_reason_detail::jsonb->>'schema_version')::int = 1 \
           AND (h.refresh_reason_detail::jsonb->>'plan_identity')::bigint = t.defining_query_hash \
           AND (h.refresh_reason_detail::jsonb->>'contract_generation')::bigint = t.contract_generation \
           AND (h.refresh_reason_detail::jsonb->>'input_delta_rows')::bigint = 4 \
           AND (h.refresh_reason_detail::jsonb->>'input_delta_rows_may_be_incomplete')::boolean = false \
           AND (h.refresh_reason_detail::jsonb->>'output_rows_changed')::bigint > 0 \
           AND (h.refresh_reason_detail::jsonb->>'source_rows_estimate')::bigint = 1000 \
           AND NOT (h.refresh_reason_detail::jsonb ? 'centroid_fallback') \
           AND NOT (h.refresh_reason_detail::jsonb ? 'centroid_rebase')",
    ).fetch_one(&mut *conn).await.expect("inspect comparable real samples");
    assert_eq!((rescans, algebraic), (3, 3));
    let (rescan_seconds, algebraic_seconds): (f64, f64) = sqlx::query_as(
        "SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM h.end_time - h.start_time)::float8) \
                    FILTER (WHERE h.merge_strategy_used IN ('merge', 'merge_prepared', 'explicit_dml')), \
                percentile_disc(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM h.end_time - h.start_time)::float8) \
                    FILTER (WHERE h.merge_strategy_used = 'centroid_algebraic') \
         FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) \
         WHERE t.pgt_relid = 'centroid_history_st'::regclass AND h.action = 'DIFFERENTIAL' \
           AND h.status = 'COMPLETED'",
    ).fetch_one(&mut *conn).await.expect("measure recorded strategy medians");
    sqlx::query("UPDATE centroid_history_src SET v = v + '[1,1]'::vector WHERE id IN (1, 2)")
        .execute(&mut *conn)
        .await
        .expect("mutate selection input");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_history_st')")
        .execute(&mut *conn)
        .await
        .expect("select from real timing history");
    let (strategy, reason): (String, Option<String>) = sqlx::query_as(
        "WITH latest AS MATERIALIZED ( \
           SELECT h.merge_strategy_used, h.refresh_reason_detail \
           FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) \
           WHERE t.pgt_relid = 'centroid_history_st'::regclass AND h.status = 'COMPLETED' \
           ORDER BY h.refresh_id DESC LIMIT 1) \
         SELECT merge_strategy_used, refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' FROM latest",
    ).fetch_one(&mut *conn).await.expect("inspect real-history decision");
    if rescan_seconds < algebraic_seconds * 0.8 {
        assert_ne!(strategy, "centroid_algebraic");
        assert_eq!(reason.as_deref(), Some("COST_MODEL_RESCAN"));
    } else {
        assert_eq!(strategy, "centroid_algebraic");
        assert_eq!(reason, None);
    }
    drop(conn);
    db.assert_st_matches_query(
        "centroid_history_st",
        "SELECT grp, avg(v)::vector(2) AS centroid FROM centroid_history_src GROUP BY grp",
    )
    .await;
}

#[tokio::test]
async fn test_centroid_auto_samples_cold_rescan_history() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db.pool.acquire().await.expect("centroid sampling session");
    for statement in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "CREATE TABLE centroid_sample_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_sample_src SELECT id, id, '[1,2]'::vector FROM generate_series(1, 40) id",
        "ANALYZE centroid_sample_src",
        "SELECT pgtrickle.create_stream_table('centroid_sample_st', $$SELECT grp, avg(v) AS centroid FROM centroid_sample_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle.pause_scheduler(ARRAY['public.centroid_sample_st'])",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_sample_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_sample_st')",
        "ANALYZE centroid_sample_st",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("prepare centroid sampling");
    }
    for _ in 0..3 {
        sqlx::query("UPDATE centroid_sample_src SET v = v + '[1,1]'::vector WHERE id IN (1, 2)")
            .execute(&mut *conn)
            .await
            .expect("mutate algebraic sample");
        sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_sample_st')")
            .execute(&mut *conn)
            .await
            .expect("refresh algebraic sample");
    }
    for statement in [
        "SET pg_trickle.refresh_strategy = 'auto'",
        "UPDATE centroid_sample_src SET v = v + '[1,1]'::vector WHERE id IN (1, 2)",
        "BEGIN",
        "SELECT pgtrickle.refresh_stream_table('centroid_sample_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("prepare rolled-back centroid sample");
    }
    let sampled_in_transaction: bool = sqlx::query_scalar(
        "SELECT c.baseline_frontier IS NULL AND (c.cost_rescan_active OR c.cost_rescan_xid IS NOT NULL) \
         FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) \
         WHERE t.pgt_relid = 'centroid_sample_st'::regclass",
    ).fetch_one(&mut *conn).await.expect("inspect transactional sample");
    assert!(sampled_in_transaction);
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("roll back sampled rescan");
    let restored: bool = sqlx::query_scalar(
        "SELECT c.baseline_frontier = t.frontier AND NOT c.cost_rescan_active AND c.cost_rescan_xid IS NULL \
         AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id \
           AND CASE WHEN h.refresh_reason_detail IS JSON THEN \
             h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' = 'COST_MODEL_SAMPLE_RESCAN' \
             ELSE false END) \
         FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) \
         WHERE t.pgt_relid = 'centroid_sample_st'::regclass",
    ).fetch_one(&mut *conn).await.expect("inspect rolled-back sample");
    assert!(restored);
    for statement in ["SELECT pgtrickle.refresh_stream_table('centroid_sample_st')"] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("sample centroid rescan");
    }
    let (strategy, reason, unbound): (String, Option<String>, bool) = sqlx::query_as(
        "SELECT h.merge_strategy_used, h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason', \
                c.baseline_frontier IS NULL AND c.cost_rescan_active \
         FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) \
         JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id \
           AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true \
         WHERE t.pgt_relid = 'centroid_sample_st'::regclass",
    ).fetch_one(&mut *conn).await.expect("inspect sampled rescan");
    assert_ne!(strategy, "centroid_algebraic");
    assert_eq!(reason.as_deref(), Some("COST_MODEL_SAMPLE_RESCAN"));
    assert!(unbound);
    let sampled_correct: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS ((SELECT grp, centroid FROM centroid_sample_st EXCEPT ALL \
           SELECT grp, avg(v)::vector(2) FROM centroid_sample_src GROUP BY grp) UNION ALL \
          (SELECT grp, avg(v)::vector(2) FROM centroid_sample_src GROUP BY grp EXCEPT ALL \
           SELECT grp, centroid FROM centroid_sample_st))",
    )
    .fetch_one(&mut *conn)
    .await
    .expect("compare sampled rescan with native query");
    assert!(sampled_correct);
    for statement in [
        "SET pg_trickle.refresh_strategy = 'differential'",
        "UPDATE centroid_sample_src SET v = '[9,10]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_sample_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("rebind after sampled rescan");
    }
    let rebound: bool = sqlx::query_scalar(
        "SELECT c.baseline_frontier = t.frontier AND NOT c.cost_rescan_active \
                AND h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' = 'COST_MODEL_REBIND' \
         FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) \
         JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id \
           AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true \
         WHERE t.pgt_relid = 'centroid_sample_st'::regclass",
    ).fetch_one(&mut *conn).await.expect("inspect rebind after sample");
    assert!(rebound);
    drop(conn);
    db.assert_st_matches_query(
        "centroid_sample_st",
        "SELECT grp, avg(v)::vector(2) AS centroid FROM centroid_sample_src GROUP BY grp",
    )
    .await;
}

#[tokio::test]
async fn test_centroid_cost_model_rescans_and_rebinds_safely() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db.pool.acquire().await.expect("centroid cost session");
    for statement in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "CREATE TABLE centroid_policy_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_policy_src SELECT id, id % 2, '[1,2]'::vector FROM generate_series(1, 4) id",
        "SELECT pgtrickle.create_stream_table('centroid_policy_st', $$SELECT grp, avg(v) AS centroid FROM centroid_policy_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle.pause_scheduler(ARRAY['public.centroid_policy_st'])",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_policy_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_policy_st')",
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, status, merge_strategy_used, refresh_reason_detail) SELECT t.pgt_id, now() - interval '5 minutes', now() - interval '5 minutes', now() - interval '5 minutes' + CASE WHEN s.strategy = 'merge' THEN interval '2 milliseconds' ELSE interval '100 milliseconds' END, 'DIFFERENTIAL', 'COMPLETED', s.strategy, jsonb_build_object('schema_version', 1, 'input_delta_rows', 4, 'input_delta_rows_may_be_incomplete', false, 'output_rows_changed', 2, 'source_rows_estimate', 1000, 'plan_identity', t.defining_query_hash, 'contract_generation', t.contract_generation)::text FROM pgtrickle.pgt_stream_tables t CROSS JOIN (SELECT strategy FROM (VALUES ('merge'), ('centroid_algebraic')) s(strategy) CROSS JOIN generate_series(1, 3)) s WHERE t.pgt_relid = 'centroid_policy_st'::regclass",
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, status, merge_strategy_used, refresh_reason_detail) SELECT pgt_id, now(), '-infinity'::timestamptz, now(), 'DIFFERENTIAL', 'COMPLETED', 'merge', 'not-json' FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_policy_st'::regclass",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("prepare cost-based centroid rescan");
    }
    for statement in [
        "BEGIN",
        "UPDATE centroid_policy_src SET v = '[5,6]' WHERE id IN (1, 2)",
        "SELECT pgtrickle.refresh_stream_table('centroid_policy_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("exercise cost-rescan rollback");
    }
    let provisional: bool = sqlx::query_scalar("SELECT c.baseline_frontier IS NULL AND c.cost_rescan_active FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect provisional cost marker");
    assert!(
        provisional,
        "cost-rescan mode must be visible inside the transaction"
    );
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("roll back cost rescan");
    let restored: bool = sqlx::query_scalar("SELECT c.baseline_frontier = t.frontier AND NOT c.cost_rescan_active AND c.cost_rescan_xid IS NULL FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect rolled-back cost marker");
    assert!(
        restored,
        "rollback must restore the bound baseline and cost marker"
    );
    for statement in [
        "UPDATE centroid_policy_src SET v = '[5,6]' WHERE id IN (1, 2)",
        "SELECT pgtrickle.refresh_stream_table('centroid_policy_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("select cost-based rescan");
    }
    let selected: bool = sqlx::query_scalar("SELECT c.baseline_frontier IS NULL AND c.cost_rescan_active AND c.cost_rescan_xid IS NULL AND h.merge_strategy_used <> 'centroid_algebraic' AND h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' = 'COST_MODEL_RESCAN' FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect selected rescan");
    assert!(
        selected,
        "cost-based rescan must leave private state unbound"
    );
    let idle_xmin: String = sqlx::query_scalar("SELECT c.xmin::text FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("read cost marker transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_policy_st')")
        .execute(&mut *conn)
        .await
        .expect("refresh without source changes");
    let idle_marker: bool = sqlx::query_scalar("SELECT c.baseline_frontier IS NULL AND c.cost_rescan_active AND c.cost_rescan_xid IS NULL FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect idle cost marker");
    assert!(
        idle_marker,
        "no-data refresh must preserve cost-rescan mode"
    );
    let after_idle_xmin: String = sqlx::query_scalar("SELECT c.xmin::text FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("read idle cost marker transaction");
    assert_eq!(
        idle_xmin, after_idle_xmin,
        "idle refresh must not rewrite the cost marker"
    );
    drop(conn);
    let restarted = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart cost-rescan fixture");
    assert!(
        restarted.status.success(),
        "{}",
        String::from_utf8_lossy(&restarted.stderr)
    );
    db.pool.close().await;
    db.pool = db.reconnect_after_restart().await;
    let mut conn = db.pool.acquire().await.expect("reconnect cost session");
    for statement in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SELECT pgtrickle.pause_scheduler(ARRAY['public.centroid_policy_st'])",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("restore cost settings");
    }
    let restarted_marker: bool = sqlx::query_scalar("SELECT c.baseline_frontier IS NULL AND c.cost_rescan_active AND c.cost_rescan_xid IS NULL FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect restarted cost marker");
    assert!(
        restarted_marker,
        "committed cost-rescan mode must survive restart"
    );
    for statement in [
        "UPDATE centroid_policy_src SET v = '[7,8]' WHERE id IN (1, 2)",
        "SELECT pgtrickle.refresh_stream_table('centroid_policy_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("continue cost rescan");
    }
    let active: String = sqlx::query_scalar("SELECT h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' FROM pgtrickle.pgt_stream_tables t CROSS JOIN LATERAL (SELECT refresh_reason_detail FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect active cost rescan");
    assert_eq!(active, "COST_MODEL_RESCAN_ACTIVE");
    for statement in [
        "BEGIN",
        "UPDATE pgtrickle.pgt_refresh_history SET end_time = start_time + interval '1 microsecond' WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_policy_st'::regclass) AND merge_strategy_used = 'centroid_algebraic'",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("prepare cost reversal");
    }
    let mut rebind_round = None;
    for round in 0..10 {
        let value = if round % 2 == 0 { "[5,6]" } else { "[7,8]" };
        sqlx::query("UPDATE centroid_policy_src SET v = $1::vector WHERE id IN (1, 2)")
            .bind(value)
            .execute(&mut *conn)
            .await
            .expect("change cost fixture");
        sqlx::query("SELECT pgtrickle.refresh_stream_table('centroid_policy_st')")
            .execute(&mut *conn)
            .await
            .expect("refresh cost fixture");
        let reason: String = sqlx::query_scalar("SELECT h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' FROM pgtrickle.pgt_stream_tables t CROSS JOIN LATERAL (SELECT refresh_reason_detail FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
            .fetch_one(&mut *conn).await.expect("read cost transition");
        if reason == "COST_MODEL_REBIND" {
            rebind_round = Some(round);
            break;
        }
        assert_eq!(reason, "COST_MODEL_RESCAN_ACTIVE");
    }
    let history: String = sqlx::query_scalar("SELECT COALESCE(string_agg(format('id=%s start=%s end=%s strategy=%s detail=%s', h.refresh_id, h.start_time, h.end_time, h.merge_strategy_used, h.refresh_reason_detail), E'\\n' ORDER BY h.start_time DESC, h.refresh_id DESC), '') FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect cost reversal history");
    assert_eq!(
        rebind_round,
        Some(7),
        "cost reversal must wait for the cooldown; refresh history:\n{history}"
    );
    let bound: bool = sqlx::query_scalar("SELECT c.baseline_frontier = t.frontier AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'outcome' = 'bound' FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT refresh_reason_detail FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect cost reversal");
    assert!(bound, "cost reversal must bind a verified baseline");
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("roll back cost reversal fixture");
    for statement in [
        "BEGIN",
        "SET LOCAL pg_trickle.internal_refresh = 'true'",
        "UPDATE centroid_policy_st SET centroid = '[99,99]' WHERE grp = 0",
        "SET LOCAL pg_trickle.internal_refresh = 'false'",
        "UPDATE centroid_policy_src SET v = '[9,10]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_policy_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("exercise unsafe cost rebind");
    }
    let rejected: bool = sqlx::query_scalar("SELECT c.baseline_frontier IS NULL AND NOT c.cost_rescan_active AND c.cost_rescan_xid IS NULL AND h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' = 'COST_MODEL_REBIND' AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'outcome' = 'unbound' FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect rejected cost rebind");
    assert!(
        rejected,
        "an untouched visible mismatch must prevent rebinding"
    );
    for statement in [
        "UPDATE centroid_policy_src SET v = '[11,12]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_policy_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("refresh after rejected rebind");
    }
    let no_retry: bool = sqlx::query_scalar("SELECT c.baseline_frontier IS NULL AND h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' = 'STATE_UNBOUND' AND NOT (h.refresh_reason_detail::jsonb ? 'centroid_rebase') FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect failed rebind retry policy");
    assert!(
        no_retry,
        "a rejected cost rebind must not cause a retry storm"
    );
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("roll back unsafe rebind fixture");
    for statement in [
        "BEGIN",
        "INSERT INTO pgtrickle.pgt_refresh_history (pgt_id, data_timestamp, start_time, end_time, action, status, merge_strategy_used) SELECT pgt_id, clock_timestamp(), clock_timestamp(), clock_timestamp(), 'NO_DATA', 'COMPLETED', 'DIFFERENTIAL' FROM pgtrickle.pgt_stream_tables CROSS JOIN generate_series(1, 70) WHERE pgt_relid = 'centroid_policy_st'::regclass",
        "UPDATE centroid_policy_src SET v = '[9,10]' WHERE id IN (1, 2)",
        "SELECT pgtrickle.refresh_stream_table('centroid_policy_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("rebind after bounded history expires");
    }
    let durable: bool = sqlx::query_scalar("SELECT c.baseline_frontier = t.frontier AND NOT c.cost_rescan_active AND h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' = 'COST_MODEL_REBIND' AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'reason' = 'centroid cost history is inconclusive' FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT refresh_reason_detail FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect durable cost marker");
    assert!(
        durable,
        "cost-rescan mode must survive history-window eviction"
    );
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("roll back expired-history fixture");
    for statement in [
        "UPDATE centroid_policy_src SET v = '[9,10]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_policy_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("rebind after a small change");
    }
    let rebound: bool = sqlx::query_scalar("SELECT c.baseline_frontier = t.frontier AND NOT c.cost_rescan_active AND c.cost_rescan_xid IS NULL AND h.merge_strategy_used = 'centroid_rebase_group_rescan' AND h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' = 'COST_MODEL_REBIND' AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'outcome' = 'bound' FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_policy_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect rebound centroid state");
    assert!(
        rebound,
        "a small change must report and bind a protected rebase"
    );
    for statement in [
        "UPDATE centroid_policy_src SET v = '[11,12]' WHERE id IN (1, 2)",
        "SELECT pgtrickle.refresh_stream_table('centroid_policy_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("refresh during cost cooldown");
    }
    let correct: bool = sqlx::query_scalar("WITH native AS (SELECT grp, avg(v)::text AS centroid FROM centroid_policy_src GROUP BY grp), actual AS (SELECT grp, centroid::text AS centroid FROM centroid_policy_st) SELECT NOT EXISTS ((SELECT * FROM native EXCEPT ALL SELECT * FROM actual) UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM native)) AND (SELECT h.merge_strategy_used = 'centroid_algebraic' AND c.baseline_frontier = t.frontier FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_policy_st'::regclass)")
        .fetch_one(&mut *conn).await.expect("check cooldown result");
    assert!(
        correct,
        "cooldown must preserve correct algebraic maintenance"
    );
}

#[tokio::test]
async fn test_centroid_cost_based_full_fallback_rebinds_state() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let mut conn = db.pool.acquire().await.expect("centroid cost session");
    for statement in [
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_cost_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_cost_src SELECT id, id % 2, '[1,2]'::vector FROM generate_series(1, 100) id",
        "ANALYZE centroid_cost_src",
        "SELECT pgtrickle.create_stream_table('centroid_cost_st', $$SELECT grp, avg(v) AS centroid FROM centroid_cost_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_cost_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_cost_st')",
        "INSERT INTO pgtrickle.pgt_cost_model_summary (pgt_id, avg_full_ms, avg_diff_ms, sample_count) SELECT pgt_id, 0.1, 100.0, 3 FROM pgtrickle.pgt_stream_tables WHERE pgt_relid = 'centroid_cost_st'::regclass ON CONFLICT (pgt_id) DO UPDATE SET avg_full_ms = EXCLUDED.avg_full_ms, avg_diff_ms = EXCLUDED.avg_diff_ms, sample_count = EXCLUDED.sample_count",
        "SET pg_trickle.refresh_strategy = 'auto'",
        "SET pg_trickle.differential_max_change_ratio = 0.8",
        "UPDATE centroid_cost_src SET v = '[5,6]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_cost_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("prepare cost-based centroid fallback");
    }
    let fell_back: bool = sqlx::query_scalar("SELECT h.action = 'FULL' AND h.was_full_fallback AND h.refresh_reason = 'COST_MODEL_PREFERRED_FULL' AND c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_cost_st'::regclass")
        .fetch_one(&mut *conn).await.expect("inspect cost-based fallback");
    assert!(fell_back, "cost-based FULL must bind refreshed state");
    for statement in [
        "SET pg_trickle.refresh_strategy = 'differential'",
        "UPDATE centroid_cost_src SET v = '[7,8]' WHERE id = 1",
        "SELECT pgtrickle.refresh_stream_table('centroid_cost_st')",
    ] {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("resume algebraic centroid maintenance");
    }
    let resumed: bool = sqlx::query_scalar("WITH native AS (SELECT grp, avg(v)::text AS centroid FROM centroid_cost_src GROUP BY grp), actual AS (SELECT grp, centroid::text AS centroid FROM centroid_cost_st) SELECT NOT EXISTS ((SELECT * FROM native EXCEPT ALL SELECT * FROM actual) UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM native)) AND (SELECT h.merge_strategy_used = 'centroid_algebraic' AND c.baseline_frontier = t.frontier FROM pgtrickle.pgt_stream_tables t JOIN pgtrickle.pgt_centroid_states c USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_cost_st'::regclass)")
        .fetch_one(&mut *conn).await.expect("inspect resumed centroid state");
    assert!(resumed, "algebraic refresh must resume with correct output");
}

#[tokio::test]
async fn test_centroid_change_limit_rebase_rejects_untouched_visible_mismatch() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "CREATE TABLE centroid_limit_mismatch_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_limit_mismatch_src VALUES (1, 1, '[1,2]'), (2, 2, '[3,4]')",
        "SELECT pgtrickle.create_stream_table('centroid_limit_mismatch_st', $$SELECT grp, avg(v) AS centroid FROM centroid_limit_mismatch_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_limit_mismatch_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_limit_mismatch_st')",
        "BEGIN",
        "SET LOCAL pg_trickle.internal_refresh = 'true'",
        "UPDATE centroid_limit_mismatch_st SET centroid = '[99,99]' WHERE grp = 1",
        "COMMIT",
        "UPDATE pgtrickle.pgt_centroid_groups SET state[2] = 4096, state[3] = 4096 WHERE row_id = (SELECT __pgt_row_id FROM centroid_limit_mismatch_st WHERE grp = 2)",
        "UPDATE pgtrickle.pgt_centroid_groups SET state_checksum = pg_catalog.md5(pg_catalog.int8send(pgt_id) || row_id || pg_catalog.int8send(group_count) || pg_catalog.array_send(state)) WHERE row_id = (SELECT __pgt_row_id FROM centroid_limit_mismatch_st WHERE grp = 2)",
        "UPDATE centroid_limit_mismatch_src SET v = '[5,6]' WHERE id = 2",
        "SELECT pgtrickle.refresh_stream_table('centroid_limit_mismatch_st')",
    ]).await;
    let safe: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'outcome' = 'unbound' AND h.refresh_reason_detail::jsonb->'centroid_rebase'->>'reason' LIKE '%ChangeLimit%' AND EXISTS (SELECT 1 FROM centroid_limit_mismatch_st WHERE grp = 1 AND centroid::text = '[99,99]') AND EXISTS (SELECT 1 FROM centroid_limit_mismatch_st WHERE grp = 2 AND centroid::text = '[5,6]') FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true WHERE t.pgt_relid = 'centroid_limit_mismatch_st'::regclass").await;
    assert!(
        safe,
        "a change-limit rebase must not bind an unchecked mismatch"
    );
}

#[tokio::test]
async fn test_vector_downstream_preserves_signed_zero_changes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "CREATE TABLE vector_sign_source (id int PRIMARY KEY, v vector(2))",
        "INSERT INTO vector_sign_source VALUES (1, '[0,-0]')",
        "SELECT pgtrickle.create_stream_table('vector_sign_parent', $$SELECT id, v FROM vector_sign_source$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle.create_stream_table('vector_sign_child', $$SELECT id, get_byte(public.vector_send(v), 8) AS sign_byte FROM vector_sign_parent$$, '1h', 'DIFFERENTIAL')",
    ]).await;
    let before: i32 = db
        .query_scalar("SELECT sign_byte FROM vector_sign_child WHERE id = 1")
        .await;
    assert_eq!(
        before, 128,
        "fixture must preserve the negative-zero sign bit"
    );
    db.execute_seq(&[
        "UPDATE vector_sign_source SET v = '[0,0]' WHERE id = 1",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('vector_sign_parent')",
        "SELECT pgtrickle.refresh_stream_table('vector_sign_child')",
    ])
    .await;
    let correct: bool = db
        .query_scalar(
            "SELECT count(*) = 1 AND bool_and(sign_byte = 0) FROM vector_sign_child WHERE id = 1",
        )
        .await;
    assert!(
        correct,
        "downstream expressions must observe signed-zero changes"
    );
}

#[tokio::test]
async fn test_vector_downstream_preserves_half_and_sparse_values() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "CREATE TABLE vector_kinds_source (id int PRIMARY KEY, h halfvec(2), s sparsevec(2))",
        "INSERT INTO vector_kinds_source VALUES (1, '[1,2]', '{1:1,2:2}/2')",
        "SELECT pgtrickle.create_stream_table('vector_kinds_parent', $$SELECT id, h, s FROM vector_kinds_source$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle.create_stream_table('vector_kinds_child', $$SELECT id, h::text AS h, s::text AS s FROM vector_kinds_parent$$, '1h', 'DIFFERENTIAL')",
        "UPDATE vector_kinds_source SET h = '[3,4]', s = '{1:3,2:4}/2' WHERE id = 1",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('vector_kinds_parent')",
        "SELECT pgtrickle.refresh_stream_table('vector_kinds_child')",
    ]).await;
    let matches: bool = db.query_scalar("SELECT NOT EXISTS ((SELECT id, h::text, s::text FROM vector_kinds_parent EXCEPT ALL SELECT id, h, s FROM vector_kinds_child) UNION ALL (SELECT id, h, s FROM vector_kinds_child EXCEPT ALL SELECT id, h::text, s::text FROM vector_kinds_parent))").await;
    assert!(
        matches,
        "downstream child must receive halfvec and sparsevec changes"
    );
    db.execute_seq(&[
        "UPDATE vector_kinds_source SET h = NULL, s = NULL WHERE id = 1",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('vector_kinds_parent')",
        "SELECT pgtrickle.refresh_stream_table('vector_kinds_child')",
    ])
    .await;
    let nulls: bool = db.query_scalar("SELECT count(*) = 1 AND bool_and(h IS NULL AND s IS NULL) FROM vector_kinds_child WHERE id = 1").await;
    assert!(nulls, "downstream child must receive NULL vector values");
}

#[tokio::test]
async fn test_vector_downstream_uses_relocated_extension_schema() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute_seq(&[
        "CREATE SCHEMA relocated_vector",
        "CREATE EXTENSION vector WITH SCHEMA relocated_vector",
        "SET search_path = public, relocated_vector",
        "CREATE TABLE vector_relocated_source (id int PRIMARY KEY, v relocated_vector.vector(2))",
        "INSERT INTO vector_relocated_source VALUES (1, '[1,2]')",
        "SELECT pgtrickle.create_stream_table('vector_relocated_parent', $$SELECT id, v FROM vector_relocated_source$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle.create_stream_table('vector_relocated_child', $$SELECT id, v::text AS v FROM vector_relocated_parent$$, '1h', 'DIFFERENTIAL')",
        "UPDATE vector_relocated_source SET v = '[3,4]' WHERE id = 1",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('vector_relocated_parent')",
        "SELECT pgtrickle.refresh_stream_table('vector_relocated_child')",
    ]).await;
    let correct: bool = db.query_scalar("SELECT count(*) = 1 AND bool_and(v = '[3,4]') FROM vector_relocated_child WHERE id = 1").await;
    assert!(
        correct,
        "downstream identity must use the relocated pgvector sender"
    );
}

#[tokio::test]
async fn test_centroid_source_ahead_fallback_is_reported() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "CREATE TABLE centroid_ahead_live_src (id int PRIMARY KEY, grp int, v vector(2))",
        "INSERT INTO centroid_ahead_live_src VALUES (1, 1, '[1,2]')",
        "SELECT pgtrickle.create_stream_table('centroid_ahead_live_st', $$SELECT grp, avg(v) AS centroid FROM centroid_ahead_live_src GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle.pause_scheduler(ARRAY['public.centroid_ahead_live_st'])",
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_ahead_live_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_ahead_live_st')",
    ]).await;
    let bound: bool = db.query_scalar("SELECT COALESCE(c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL, false) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_ahead_live_st'::regclass").await;
    assert!(bound, "source-ahead fixture requires a bound initial state");
    db.execute("UPDATE centroid_ahead_live_src SET v = '[3,4]' WHERE id = 1")
        .await;
    let buffer: String = db.query_scalar("SELECT format('%I.%I', 'pgtrickle_changes', 'changes_' || COALESCE(source_stable_name, source_relid::text)) FROM pgtrickle.pgt_change_tracking WHERE source_relid = 'centroid_ahead_live_src'::regclass::oid").await;
    let moved: i64 = db.query_scalar(&format!("WITH moved AS (UPDATE {buffer} SET lsn = 'FF/FFFFFFFF'::pg_lsn WHERE action = 'D' RETURNING 1) SELECT count(*)::bigint FROM moved")).await;
    assert_eq!(
        moved, 1,
        "fixture must put one captured change ahead of the frontier"
    );
    let still_bound: bool = db.query_scalar("SELECT COALESCE(c.baseline_frontier = t.frontier AND c.materialized_xid IS NULL AND NOT c.cost_rescan_active AND c.cost_rescan_xid IS NULL AND c.stream_relid = t.pgt_relid AND c.defining_query = t.defining_query AND c.contract_generation = t.contract_generation AND c.schema_version = 1 AND c.status = 'BUILDING', false) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_ahead_live_st'::regclass").await;
    assert!(
        still_bound,
        "the injected ahead row must not itself unbind state"
    );
    let before: String = db.query_scalar("SELECT format('state=%s stream_generation=%s frontier=%s', row_to_json(c), t.contract_generation, t.frontier) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_ahead_live_st'::regclass").await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SET pg_trickle.enable_centroid_state = on",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SELECT pgtrickle.refresh_stream_table('centroid_ahead_live_st')",
    ])
    .await;
    let safe_and_reported: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND h.merge_strategy_used <> 'centroid_algebraic' AND h.refresh_reason_detail::jsonb->'centroid_fallback'->>'reason' = 'SOURCE_AHEAD_OF_FRONTIER' AND s.centroid::text = '[3,4]' FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true CROSS JOIN centroid_ahead_live_st s WHERE t.pgt_relid = 'centroid_ahead_live_st'::regclass").await;
    let detail: String = db.query_scalar("SELECT format('baseline=%s strategy=%s detail=%s centroid=%s', coalesce(c.baseline_frontier::text, 'null'), h.merge_strategy_used, coalesce(h.refresh_reason_detail, 'null'), s.centroid::text) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) JOIN LATERAL (SELECT * FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h ON true CROSS JOIN centroid_ahead_live_st s WHERE t.pgt_relid = 'centroid_ahead_live_st'::regclass").await;
    let history: String = db.query_scalar("SELECT COALESCE(string_agg(format('id=%s by=%s status=%s detail=%s', h.refresh_id, h.initiated_by, h.status, h.refresh_reason_detail), E'\\n' ORDER BY h.refresh_id), '') FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_ahead_live_st'::regclass").await;
    let after: String = db.query_scalar("SELECT format('state=%s stream_generation=%s frontier=%s', row_to_json(c), t.contract_generation, t.frontier) FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) WHERE t.pgt_relid = 'centroid_ahead_live_st'::regclass").await;
    assert!(
        safe_and_reported,
        "source-ahead fallback must remain correct, unbound, and observable: {detail}\nbefore: {before}\nafter: {after}\n{history}"
    );
    let evidence: String = db.query_scalar("SELECT h.refresh_reason_detail FROM pgtrickle.pgt_stream_tables t CROSS JOIN LATERAL (SELECT refresh_reason_detail FROM pgtrickle.pgt_refresh_history h WHERE h.pgt_id = t.pgt_id AND h.status = 'COMPLETED' ORDER BY h.refresh_id DESC LIMIT 1) h WHERE t.pgt_relid = 'centroid_ahead_live_st'::regclass").await;
    let evidence: serde_json::Value =
        serde_json::from_str(&evidence).expect("parse fallback evidence");
    assert!(evidence["input_delta_rows_may_be_incomplete"].is_boolean());
}

#[tokio::test]
async fn test_centroid_full_refresh_does_not_bind_ahead_buffer() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "CREATE TABLE centroid_ahead_src (id int PRIMARY KEY, v vector(2))",
        "INSERT INTO centroid_ahead_src VALUES (1, '[1,2]')",
        "SELECT pgtrickle.create_stream_table('centroid_ahead_st', $$SELECT avg(v) AS centroid FROM centroid_ahead_src$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle._centroid_prepare_state('centroid_ahead_st'::regclass::oid)",
        "UPDATE centroid_ahead_src SET v = '[3,4]' WHERE id = 1",
    ]).await;
    let buffer: String = db.query_scalar("SELECT format('%I.%I', 'pgtrickle_changes', 'changes_' || COALESCE(source_stable_name, source_relid::text)) FROM pgtrickle.pgt_change_tracking WHERE source_relid = 'centroid_ahead_src'::regclass::oid").await;
    let sql = format!(
        "WITH changed AS (UPDATE {buffer} SET lsn = 'FF/FFFFFFFF'::pg_lsn WHERE action <> 'S' RETURNING 1) SELECT count(*)::bigint FROM changed"
    );
    let changed: i64 = db.query_scalar(&sql).await;
    assert!(changed > 0, "the test must move a captured change ahead");
    db.execute_seq(&[
        "UPDATE pgtrickle.pgt_stream_tables SET frontier = NULL WHERE pgt_relid = 'centroid_ahead_st'::regclass",
        "SELECT pgtrickle.refresh_stream_table('centroid_ahead_st')",
    ]).await;
    let unbound: bool = db.query_scalar("SELECT c.baseline_frontier IS NULL AND c.materialized_xid IS NULL AND t.frontier IS NOT NULL AND s.centroid::text = '[3,4]' FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) CROSS JOIN centroid_ahead_st s WHERE t.pgt_relid = 'centroid_ahead_st'::regclass").await;
    assert!(
        unbound,
        "a FULL snapshot ahead of its frontier must stay unbound"
    );
}

fn ann_worker_command(
    db: &E2eDb,
    database: &str,
    user: &str,
    request_id: i64,
    lock_timeout: &str,
) -> tokio::process::Command {
    use std::process::Stdio;

    let script = std::fs::File::open(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/scripts/ann_maintenance_worker.sql"
    ))
    .expect("open ANN worker script");
    let mut command = tokio::process::Command::new("docker");
    command
        .args([
            "exec",
            "-i",
            "-e",
            "PGAPPNAME=pgtrickle_ann_worker",
            db.container_id(),
            "psql",
            "-X",
            "-w",
            "-U",
            user,
            "-d",
            database,
            "-v",
        ])
        .arg(format!("request_id={request_id}"))
        .arg("-v")
        .arg(format!("ann_lock_timeout={lock_timeout}"))
        .args(["-f", "-"])
        .stdin(Stdio::from(script))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

fn ann_threshold_command(
    db: &E2eDb,
    database: &str,
    user: &str,
    stream: &str,
    index: &str,
    lock_timeout: &str,
) -> tokio::process::Command {
    use std::process::Stdio;
    let script = std::fs::File::open(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/scripts/ann_threshold_request.sql"
    ))
    .expect("open threshold request script");
    let mut command = tokio::process::Command::new("docker");
    command
        .args([
            "exec",
            "-i",
            db.container_id(),
            "psql",
            "-X",
            "-w",
            "-qAt",
            "-U",
            user,
            "-d",
            database,
            "-v",
        ])
        .arg(format!("ann_stream={stream}"))
        .arg("-v")
        .arg(format!("ann_index={index}"))
        .arg("-v")
        .arg(format!("ann_lock_timeout={lock_timeout}"))
        .args(["-f", "-"])
        .stdin(Stdio::from(script))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

#[tokio::test]
async fn test_ann_threshold_request_script_records_only_eligible_work() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_policy_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute(
        "INSERT INTO ann_policy_source SELECT i, '[1,0,0]'::vector FROM generate_series(1,10) i",
    )
    .await;
    db.create_st(
        "ann_policy_stream",
        "SELECT id, embedding FROM ann_policy_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_policy_idx ON ann_policy_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('ann_policy_stream', reindex_drift_threshold => 0.2)",
    )
    .await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let run = || {
        ann_threshold_command(
            &db,
            &database,
            "postgres",
            "public.ann_policy_stream",
            "public.ann_policy_idx",
            "5s",
        )
    };
    let initial = run().output().await.expect("inspect missing baseline");
    assert!(
        initial.status.success(),
        "{}",
        String::from_utf8_lossy(&initial.stderr)
    );
    assert!(String::from_utf8_lossy(&initial.stdout).contains("missing_baseline"));
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_policy_stream'::regclass::oid, 'ann_policy_idx'::regclass::oid)").await;
    let baseline = ann_worker_command(&db, &database, "postgres", request, "5s")
        .output()
        .await
        .expect("establish baseline");
    assert!(
        baseline.status.success(),
        "{}",
        String::from_utf8_lossy(&baseline.stderr)
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let below = run().output().await.expect("check unchanged corpus");
    assert!(
        below.status.success(),
        "{}",
        String::from_utf8_lossy(&below.stderr)
    );
    assert!(String::from_utf8_lossy(&below.stdout).contains("below_threshold"));
    db.execute("UPDATE ann_policy_source SET embedding = '[0,1,0]' WHERE id <= 3")
        .await;
    db.refresh_st("ann_policy_stream").await;
    let old_index: i64 = db
        .query_scalar("SELECT 'ann_policy_idx'::regclass::oid::bigint")
        .await;
    let scheduled = run().output().await.expect("request eligible work");
    assert!(
        scheduled.status.success(),
        "{}",
        String::from_utf8_lossy(&scheduled.stderr)
    );
    let decision: serde_json::Value =
        serde_json::from_slice(&scheduled.stdout).expect("parse threshold decision");
    assert_eq!(decision["decision"], "requested");
    let scheduled_request = decision["request_id"]
        .as_i64()
        .expect("scheduled request id");
    let new_index: i64 = db
        .query_scalar("SELECT 'ann_policy_idx'::regclass::oid::bigint")
        .await;
    assert_eq!(old_index, new_index, "request script must not run DDL");
    let state: String =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)->>'state'")
            .bind(scheduled_request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect requested work");
    assert_eq!(state, "REQUESTED");
    let duplicate = run().output().await.expect("deduplicate work");
    assert!(
        duplicate.status.success(),
        "{}",
        String::from_utf8_lossy(&duplicate.stderr)
    );
    assert!(String::from_utf8_lossy(&duplicate.stdout).contains("already_requested"));
    let completed = ann_worker_command(&db, &database, "postgres", scheduled_request, "5s")
        .output()
        .await
        .expect("execute requested work");
    assert!(
        completed.status.success(),
        "{}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let rebuilt: i64 = db
        .query_scalar("SELECT 'ann_policy_idx'::regclass::oid::bigint")
        .await;
    assert_ne!(old_index, rebuilt);
    db.execute("CREATE INDEX ann_policy_btree ON ann_policy_stream (id)")
        .await;
    let unrelated = ann_threshold_command(
        &db,
        &database,
        "postgres",
        "public.ann_policy_stream",
        "public.ann_policy_btree",
        "5s",
    )
    .output()
    .await
    .expect("check non-ANN index");
    assert!(
        !unrelated.status.success(),
        "non-ANN indexes must not be queued"
    );
    db.assert_st_matches_query(
        "ann_policy_stream",
        "SELECT id, embedding FROM ann_policy_source",
    )
    .await;
    let mut blocker = db.pool.begin().await.expect("begin refresh barrier");
    sqlx::query("LOCK TABLE ann_policy_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold refresh barrier");
    let locked = ann_threshold_command(
        &db,
        &database,
        "postgres",
        "public.ann_policy_stream",
        "public.ann_policy_idx",
        "200ms",
    )
    .output()
    .await
    .expect("check lock timeout");
    assert!(!locked.status.success());
    assert!(String::from_utf8_lossy(&locked.stderr).contains("lock timeout"));
    blocker.rollback().await.expect("release refresh barrier");
    db.execute("CREATE ROLE ann_policy_operator LOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO ann_policy_operator")
        .await;
    db.execute(
        "GRANT SELECT ON pgtrickle.pgt_stream_tables, ann_policy_stream TO ann_policy_operator",
    )
    .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.request_ann_maintenance_if_needed(oid,oid,bigint,bigint,bigint) TO ann_policy_operator").await;
    let denied = ann_threshold_command(
        &db,
        &database,
        "ann_policy_operator",
        "public.ann_policy_stream",
        "public.ann_policy_idx",
        "5s",
    )
    .output()
    .await
    .expect("run without MAINTAIN");
    assert!(
        !denied.status.success(),
        "SELECT alone must not request maintenance"
    );
    db.execute("GRANT MAINTAIN ON ann_policy_stream TO ann_policy_operator")
        .await;
    let permitted = ann_threshold_command(
        &db,
        &database,
        "ann_policy_operator",
        "public.ann_policy_stream",
        "public.ann_policy_idx",
        "5s",
    )
    .output()
    .await
    .expect("run with MAINTAIN");
    assert!(
        permitted.status.success(),
        "{}",
        String::from_utf8_lossy(&permitted.stderr)
    );
    assert!(String::from_utf8_lossy(&permitted.stdout).contains("cooldown"));
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_churn_operations = NULL WHERE pgt_name = 'ann_policy_stream'").await;
    let unknown = run().output().await.expect("check unknown churn");
    assert!(
        unknown.status.success(),
        "{}",
        String::from_utf8_lossy(&unknown.stderr)
    );
    assert!(String::from_utf8_lossy(&unknown.stdout).contains("unknown_work"));
    db.execute("CREATE TABLE ann_policy_empty_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_policy_empty",
        "SELECT id, embedding FROM ann_policy_empty_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_policy_empty_idx ON ann_policy_empty USING hnsw (embedding vector_l2_ops)").await;
    let empty = ann_threshold_command(
        &db,
        &database,
        "postgres",
        "public.ann_policy_empty",
        "public.ann_policy_empty_idx",
        "5s",
    )
    .output()
    .await
    .expect("check empty estimate");
    assert!(
        empty.status.success(),
        "{}",
        String::from_utf8_lossy(&empty.stderr)
    );
    assert!(String::from_utf8_lossy(&empty.stdout).contains("unknown_estimate"));
    db.execute(
        "ALTER INDEX ann_policy_empty_idx RENAME TO \"empty; DROP TABLE ann_policy_source; --\"",
    )
    .await;
    let quoted = ann_threshold_command(
        &db,
        &database,
        "postgres",
        "public.ann_policy_empty",
        "public.\"empty; DROP TABLE ann_policy_source; --\"",
        "5s",
    )
    .output()
    .await
    .expect("check quoted index name");
    assert!(
        quoted.status.success(),
        "{}",
        String::from_utf8_lossy(&quoted.stderr)
    );
    assert!(String::from_utf8_lossy(&quoted.stdout).contains("unknown_estimate"));
    assert_eq!(db.count("ann_policy_source").await, 10);
}

#[tokio::test]
async fn test_ann_worker_limits_concurrent_requests_per_database() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut requests = Vec::new();
    for i in 0..5 {
        db.execute(&format!(
            "CREATE TABLE ann_slot_source_{i} (id int PRIMARY KEY, embedding vector(3))"
        ))
        .await;
        db.create_st(
            &format!("ann_slot_stream_{i}"),
            &format!("SELECT id, embedding FROM ann_slot_source_{i}"),
            "1h",
            "DIFFERENTIAL",
        )
        .await;
        db.execute(&format!("CREATE INDEX ann_slot_idx_{i} ON ann_slot_stream_{i} USING hnsw (embedding vector_l2_ops)")).await;
        let request: i64 = db.query_scalar(&format!("SELECT pgtrickle.request_ann_maintenance('ann_slot_stream_{i}'::regclass::oid, 'ann_slot_idx_{i}'::regclass::oid)")).await;
        requests.push(request);
    }
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    sqlx::query("LOCK TABLE ann_slot_stream_0, ann_slot_stream_1, ann_slot_stream_2, ann_slot_stream_3, ann_slot_stream_4 IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold barrier");
    let mut workers = Vec::new();
    for request in &requests[..4] {
        let mut command = ann_worker_command(&db, &database, "postgres", *request, "30s");
        command.kill_on_drop(true);
        workers.push(command.spawn().expect("start worker"));
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut running = 0;
            for request in &requests[..4] {
                let state: String =
                    sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)->>'state'")
                        .bind(request)
                        .fetch_one(&db.pool)
                        .await
                        .expect("inspect request");
                running += usize::from(state == "RUNNING");
            }
            if running == 4 {
                break;
            }
            for worker in &mut workers {
                assert!(
                    worker.try_wait().expect("inspect worker").is_none(),
                    "worker exited before the barrier"
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("workers did not reach barrier");
    let rejected = ann_worker_command(&db, &database, "postgres", requests[4], "30s")
        .output()
        .await
        .expect("run fifth worker");
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("worker capacity exhausted"));
    let state: String =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)->>'state'")
            .bind(requests[4])
            .fetch_one(&db.pool)
            .await
            .expect("inspect rejected request");
    assert_eq!(state, "REQUESTED");
    blocker.commit().await.expect("release barrier");
    for worker in workers {
        let output = tokio::time::timeout(Duration::from_secs(30), worker.wait_with_output())
            .await
            .expect("worker did not finish")
            .expect("wait for worker");
        assert!(
            output.status.success(),
            "worker failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let retried = ann_worker_command(&db, &database, "postgres", requests[4], "30s")
        .output()
        .await
        .expect("retry fifth worker");
    assert!(
        retried.status.success(),
        "retry failed: {}",
        String::from_utf8_lossy(&retried.stderr)
    );
}

#[tokio::test]
async fn test_ann_threshold_requests_deduplicate_and_reject_stale_estimates() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_threshold_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_threshold_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_threshold_stream",
        "SELECT id, embedding FROM ann_threshold_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_threshold_idx ON ann_threshold_stream USING hnsw (embedding vector_l2_ops)").await;
    db.execute("SELECT pgtrickle.alter_stream_table('ann_threshold_stream', reindex_drift_threshold => 0.20)").await;
    let watermark_sql = "SELECT ann_churn_generation, ann_churn_operations FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_threshold_stream'";
    let policy_sql = "SELECT pgtrickle.request_ann_maintenance_if_needed('ann_threshold_stream'::regclass::oid, 'ann_threshold_idx'::regclass::oid, 10, $1, $2)";
    let (generation, operations): (i64, i64) = sqlx::query_as(watermark_sql)
        .fetch_one(&db.pool)
        .await
        .expect("read estimate watermark");
    let initial: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("inspect missing baseline");
    assert_eq!(initial["decision"], "missing_baseline");
    let invalid = db.try_execute("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_threshold_stream'::regclass::oid, 'ann_threshold_idx'::regclass::oid, 0, 0, 0)").await;
    assert!(
        invalid.is_err(),
        "an empty or missing estimate is not healthy"
    );
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_threshold_stream'::regclass::oid, 'ann_threshold_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim baseline");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report baseline");
    let mut estimate_tx = db.pool.begin().await.expect("begin estimate");
    sqlx::query("LOCK TABLE ann_threshold_stream IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *estimate_tx)
        .await
        .expect("lock estimate target");
    sqlx::query("ANALYZE ann_threshold_stream")
        .execute(&mut *estimate_tx)
        .await
        .expect("analyze estimate target");
    let (estimated, actual_generation, actual_operations): (i64, i64, i64) = sqlx::query_as(
        "SELECT c.reltuples::bigint, s.ann_churn_generation, s.ann_churn_operations FROM pgtrickle.pgt_stream_tables s JOIN pg_catalog.pg_class c ON c.oid=s.pgt_relid WHERE s.pgt_name='ann_threshold_stream'",
    ).fetch_one(&mut *estimate_tx).await.expect("capture estimate and watermark");
    assert!(estimated > 0);
    assert_eq!(
        (actual_generation, actual_operations),
        (generation, operations)
    );
    estimate_tx.commit().await.expect("commit estimate");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let below: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("check below threshold");
    assert_eq!(below["decision"], "below_threshold");
    db.execute("CREATE ROLE ann_threshold_operator NOLOGIN")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO ann_threshold_operator")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.request_ann_maintenance_if_needed(oid,oid,bigint,bigint,bigint) TO ann_threshold_operator").await;
    db.execute("GRANT SELECT ON ann_threshold_stream TO ann_threshold_operator")
        .await;
    let mut restricted = db.pool.begin().await.expect("begin restricted request");
    sqlx::query("SET LOCAL ROLE ann_threshold_operator")
        .execute(&mut *restricted)
        .await
        .expect("set restricted role");
    let denied = sqlx::query_scalar::<_, serde_json::Value>(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&mut *restricted)
        .await;
    assert!(
        denied.is_err(),
        "SELECT alone must not authorize threshold maintenance"
    );
    restricted.rollback().await.expect("end restricted request");
    db.execute("GRANT MAINTAIN ON ann_threshold_stream TO ann_threshold_operator")
        .await;
    let mut permitted = db.pool.begin().await.expect("begin authorized request");
    sqlx::query("SET LOCAL ROLE ann_threshold_operator")
        .execute(&mut *permitted)
        .await
        .expect("set authorized role");
    let allowed: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&mut *permitted)
        .await
        .expect("check authorized threshold");
    assert_eq!(allowed["decision"], "below_threshold");
    permitted.rollback().await.expect("end authorized request");
    let mut blocker = db.pool.begin().await.expect("begin watermark race");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("read blocker pid");
    sqlx::query("UPDATE pgtrickle.pgt_stream_tables SET ann_churn_operations = ann_churn_operations + 1 WHERE pgt_name='ann_threshold_stream'")
        .execute(&mut *blocker).await.expect("hold uncommitted watermark");
    let pool = db.pool.clone();
    let check = tokio::spawn(async move {
        let mut conn = pool.acquire().await.expect("acquire threshold session");
        sqlx::query("SET application_name = 'ann_threshold_race'")
            .execute(&mut *conn)
            .await
            .expect("name threshold session");
        sqlx::query_scalar::<_, serde_json::Value>(policy_sql)
            .bind(generation)
            .bind(operations)
            .fetch_one(&mut *conn)
            .await
            .expect("check concurrent watermark")
    });
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name='ann_threshold_race' AND $1 = ANY(pg_catalog.pg_blocking_pids(pid)))")
                .bind(blocker_pid).fetch_one(&db.pool).await.expect("inspect threshold wait");
            if waiting { break; }
            assert!(!check.is_finished(), "threshold check exited before waiting");
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }).await.expect("threshold check did not wait for watermark");
    blocker.commit().await.expect("commit watermark");
    let raced = check.await.expect("join threshold check");
    assert_eq!(raced["decision"], "stale_estimate");
    db.execute("INSERT INTO ann_threshold_source VALUES (2, '[0,1,0]'), (3, '[0,0,1]')")
        .await;
    db.refresh_st("ann_threshold_stream").await;
    let stale: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("check stale estimate");
    assert_eq!(stale["decision"], "stale_estimate");
    let (generation, operations): (i64, i64) = sqlx::query_as(watermark_sql)
        .fetch_one(&db.pool)
        .await
        .expect("read new estimate watermark");
    let mut tx = db.pool.begin().await.expect("begin request rollback");
    let rolled_back: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&mut *tx)
        .await
        .expect("request inside transaction");
    assert_eq!(rolled_back["decision"], "requested");
    tx.rollback().await.expect("roll back request");
    let previous: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect rolled-back request");
    assert_eq!(previous["state"], "SUCCEEDED");
    let mut callers = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let pool = db.pool.clone();
        callers.spawn(async move {
            sqlx::query_scalar::<_, serde_json::Value>(policy_sql)
                .bind(generation)
                .bind(operations)
                .fetch_one(&pool)
                .await
                .expect("request threshold maintenance")
        });
    }
    let mut newly_requested = 0;
    while let Some(result) = callers.join_next().await {
        let decision = result.expect("join threshold caller");
        assert_eq!(decision["request_id"].as_i64(), Some(request));
        match decision["decision"].as_str() {
            Some("requested") => newly_requested += 1,
            Some("already_requested" | "raced") => {}
            other => panic!("unexpected threshold decision: {other:?}"),
        }
    }
    assert_eq!(
        newly_requested, 1,
        "concurrent checks must create one request"
    );
    let duplicate: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("deduplicate request");
    assert_eq!(duplicate["decision"], "already_requested");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim threshold request");
    let running: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("deduplicate running request");
    assert_eq!(running["decision"], "already_requested");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect running request");
    assert_eq!(status["pending_during_run"], false);
    db.execute("INSERT INTO ann_threshold_source VALUES (4, '[1,1,0]')")
        .await;
    db.refresh_st("ann_threshold_stream").await;
    let (during_generation, during_operations): (i64, i64) = sqlx::query_as(watermark_sql)
        .fetch_one(&db.pool)
        .await
        .expect("read watermark during run");
    let during: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(during_generation)
        .bind(during_operations)
        .fetch_one(&db.pool)
        .await
        .expect("check while running");
    assert_eq!(during["decision"], "already_requested");
    let running_status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect running deduplication");
    assert_eq!(running_status["pending_during_run"], false);
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("finish threshold request");
    let covered: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect covered work");
    assert_eq!(
        covered["known_operations_since_reported_coverage"].as_i64(),
        Some(1)
    );
    let (generation, operations): (i64, i64) = sqlx::query_as(watermark_sql)
        .fetch_one(&db.pool)
        .await
        .expect("read later watermark");
    let cooldown: serde_json::Value = sqlx::query_scalar(policy_sql)
        .bind(generation)
        .bind(operations)
        .fetch_one(&db.pool)
        .await
        .expect("check cooldown");
    assert_eq!(cooldown["decision"], "cooldown");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let before_index: i64 = db
        .query_scalar("SELECT 'ann_threshold_idx'::regclass::oid::bigint")
        .await;
    let next: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_threshold_stream'::regclass::oid, 'ann_threshold_idx'::regclass::oid, 5, $1, $2)")
        .bind(generation).bind(operations).fetch_one(&db.pool).await.expect("request later threshold");
    assert_eq!(next["decision"], "requested");
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let output = ann_worker_command(
        &db,
        &database,
        "postgres",
        next["request_id"].as_i64().expect("request id"),
        "30s",
    )
    .output()
    .await
    .expect("run threshold worker");
    assert!(
        output.status.success(),
        "threshold worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let after_index: i64 = db
        .query_scalar("SELECT 'ann_threshold_idx'::regclass::oid::bigint")
        .await;
    assert_ne!(
        before_index, after_index,
        "worker must complete concurrent reindex"
    );
    db.assert_st_matches_query(
        "ann_threshold_stream",
        "SELECT id, embedding FROM ann_threshold_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_churn_reports_unknown_for_partitioned_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_churn_partition_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_churn_partition_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st_partitioned(
        "ann_churn_partition_stream",
        "SELECT id, embedding FROM ann_churn_partition_source",
        "1h",
        "DIFFERENTIAL",
        "id",
    )
    .await;
    db.execute("CREATE INDEX ann_churn_partition_idx ON ann_churn_partition_stream USING hnsw (embedding vector_l2_ops)").await;
    let leaf: i64 = db.query_scalar("SELECT i.indexrelid::bigint FROM pg_index i JOIN pg_inherits h ON h.inhrelid=i.indrelid WHERE h.inhparent='ann_churn_partition_stream'::regclass AND i.indisvalid AND EXISTS (SELECT 1 FROM pg_class c JOIN pg_am a ON a.oid=c.relam WHERE c.oid=i.indexrelid AND a.amname='hnsw') LIMIT 1").await;
    let request: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_churn_partition_stream'::regclass::oid, $1::bigint::oid)")
        .bind(leaf).fetch_one(&db.pool).await.expect("request leaf");
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim request");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report baseline");
    let partition_decision: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_churn_partition_stream'::regclass::oid, $1::bigint::oid, 10, 0, 0)")
        .bind(leaf).fetch_one(&db.pool).await.expect("check partition policy");
    assert_eq!(partition_decision["decision"], "partitioned_unknown");
    assert!(partition_decision["known_operations"].is_null());
    db.execute("INSERT INTO ann_churn_partition_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_churn_partition_stream").await;
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect partition change");
    assert!(status["known_operations_since_reported_coverage"].is_null());
    db.assert_st_matches_query(
        "ann_churn_partition_stream",
        "SELECT id, embedding FROM ann_churn_partition_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_churn_reports_unknown_with_user_triggers() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_churn_trigger_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_churn_trigger_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_churn_trigger_stream",
        "SELECT id, embedding FROM ann_churn_trigger_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_churn_trigger_idx ON ann_churn_trigger_stream USING hnsw (embedding vector_l2_ops)").await;
    db.execute("CREATE FUNCTION ann_churn_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").await;
    db.execute("CREATE TRIGGER ann_churn_trigger AFTER INSERT ON ann_churn_trigger_stream FOR EACH ROW EXECUTE FUNCTION ann_churn_trigger_fn()").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_churn_trigger_stream'::regclass::oid, 'ann_churn_trigger_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim request");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report baseline");
    db.execute("INSERT INTO ann_churn_trigger_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_churn_trigger_stream").await;
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect trigger change");
    assert!(status["known_operations_since_reported_coverage"].is_null());
    db.assert_st_matches_query(
        "ann_churn_trigger_stream",
        "SELECT id, embedding FROM ann_churn_trigger_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_churn_tracks_immediate_changes_and_preserves_unknown_triggers() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_churn_immediate_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_churn_immediate_source VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('ann_churn_immediate_stream', 'SELECT id, embedding FROM ann_churn_immediate_source', NULL, 'IMMEDIATE')").await;
    db.execute("CREATE INDEX ann_churn_immediate_idx ON ann_churn_immediate_stream USING hnsw (embedding vector_l2_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_churn_immediate_stream'::regclass::oid, 'ann_churn_immediate_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim request");
    db.execute("INSERT INTO ann_churn_immediate_source VALUES (2, '[0,1,0]')")
        .await;
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report baseline");
    let baseline: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect baseline");
    assert_eq!(
        baseline["known_operations_since_reported_coverage"].as_i64(),
        Some(1),
        "immediate work committed after the claim must remain outstanding"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect immediate change");
    assert_eq!(
        status["known_operations_since_reported_coverage"].as_i64(),
        Some(1)
    );
    let mut tx = db.pool.begin().await.expect("begin immediate rollback");
    sqlx::query("INSERT INTO ann_churn_immediate_source VALUES (3, '[0,0,1]')")
        .execute(&mut *tx)
        .await
        .expect("insert in transaction");
    let uncommitted: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *tx)
            .await
            .expect("inspect uncommitted churn");
    assert_eq!(
        uncommitted["known_operations_since_reported_coverage"].as_i64(),
        Some(2)
    );
    tx.rollback().await.expect("roll back immediate change");
    let rolled_back: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect rolled-back churn");
    assert_eq!(
        rolled_back["known_operations_since_reported_coverage"].as_i64(),
        Some(1)
    );
    db.execute("UPDATE ann_churn_immediate_source SET embedding='[1,1,0]' WHERE id=2")
        .await;
    db.execute("DELETE FROM ann_churn_immediate_source WHERE id=1")
        .await;
    let changed: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect update and delete");
    assert!(
        changed["known_operations_since_reported_coverage"]
            .as_i64()
            .is_some_and(|count| count >= 3)
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let requested: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_churn_immediate_stream'::regclass::oid, 'ann_churn_immediate_idx'::regclass::oid, 10, $1, $2)")
        .bind(changed["current_churn_generation"].as_i64().expect("current generation"))
        .bind(changed["current_churn_operations"].as_i64().expect("current operations"))
        .fetch_one(&db.pool).await.expect("check measured immediate work");
    assert_eq!(requested["decision"], "requested");
    db.execute("CREATE FUNCTION ann_churn_immediate_trigger_fn() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$").await;
    db.execute("CREATE TRIGGER ann_churn_immediate_trigger AFTER INSERT ON ann_churn_immediate_stream FOR EACH ROW EXECUTE FUNCTION ann_churn_immediate_trigger_fn()").await;
    db.execute("INSERT INTO ann_churn_immediate_source VALUES (3, '[0,0,1]')")
        .await;
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect triggered change");
    assert!(status["known_operations_since_reported_coverage"].is_null());
    let unknown: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance_if_needed('ann_churn_immediate_stream'::regclass::oid, 'ann_churn_immediate_idx'::regclass::oid, 10, $1, $2)")
        .bind(status["current_churn_generation"].as_i64().expect("current generation"))
        .bind(status["current_churn_operations"].as_i64().expect("current operations"))
        .fetch_one(&db.pool).await.expect("check unknown immediate work");
    assert_eq!(unknown["decision"], "unknown_work");
    db.execute("DROP TRIGGER ann_churn_immediate_trigger ON ann_churn_immediate_stream")
        .await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_churn_operations=9223372036854775807 WHERE pgt_name='ann_churn_immediate_stream'").await;
    db.execute("INSERT INTO ann_churn_immediate_source VALUES (4, '[1,0,1]')")
        .await;
    let overflow: Option<i64> = sqlx::query_scalar("SELECT ann_churn_operations FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_churn_immediate_stream'")
        .fetch_one(&db.pool).await.expect("read overflowed counter");
    assert!(overflow.is_none(), "counter overflow must remain unknown");
    db.assert_st_matches_query(
        "ann_churn_immediate_stream",
        "SELECT id, embedding FROM ann_churn_immediate_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_churn_preserves_concurrent_work_and_unknown_rewrites() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_churn_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_churn_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_churn_stream",
        "SELECT id, embedding FROM ann_churn_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_churn_idx ON ann_churn_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    db.execute("CREATE INDEX ann_churn_other_idx ON ann_churn_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute(
        "SELECT pgtrickle.snapshot_stream_table('ann_churn_stream', 'public.ann_churn_archive')",
    )
    .await;
    let request_sql = "SELECT pgtrickle.request_ann_maintenance('ann_churn_stream'::regclass::oid, 'ann_churn_idx'::regclass::oid)";
    let request: i64 = db.query_scalar(request_sql).await;
    let other: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_churn_stream'::regclass::oid, 'ann_churn_other_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    let claim: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .fetch_one(&mut *worker)
        .await
        .expect("claim request");
    assert!(claim["known_operations_since_reported_coverage"].is_null());
    db.execute("INSERT INTO ann_churn_source VALUES (2, '[0,1,0]'), (3, '[0,0,1]')")
        .await;
    db.refresh_st("ann_churn_stream").await;
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("report coverage");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect coverage");
    assert_eq!(
        status["reported_churn_operations"],
        claim["claim_churn_operations"]
    );
    assert_eq!(
        status["known_operations_since_reported_coverage"].as_i64(),
        Some(2)
    );
    let independent: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(other)
            .fetch_one(&db.pool)
            .await
            .expect("inspect other index");
    assert!(independent["known_operations_since_reported_coverage"].is_null());
    let mut tx = db.pool.begin().await.expect("begin rollback test");
    sqlx::query("INSERT INTO ann_churn_source VALUES (4, '[1,1,0]')")
        .execute(&mut *tx)
        .await
        .expect("insert rolled-back row");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('ann_churn_stream')")
        .execute(&mut *tx)
        .await
        .expect("refresh in transaction");
    tx.rollback().await.expect("roll back refresh");
    let after_rollback: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect rollback");
    assert_eq!(
        after_rollback["known_operations_since_reported_coverage"].as_i64(),
        Some(2)
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let retry: i64 = db.query_scalar(request_sql).await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(retry)
        .execute(&mut *worker)
        .await
        .expect("claim retry");
    db.execute("INSERT INTO ann_churn_source VALUES (4, '[1,1,0]')")
        .await;
    db.refresh_st("ann_churn_stream").await;
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
        .bind(retry)
        .execute(&mut *worker)
        .await
        .expect("report failed retry");
    let failed_retry: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(retry)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect failed retry");
    assert_eq!(
        failed_retry["known_operations_since_reported_coverage"].as_i64(),
        Some(3)
    );
    db.execute("UPDATE ann_churn_source SET embedding = '[1,0,1]' WHERE id = 2")
        .await;
    db.execute("DELETE FROM ann_churn_source WHERE id = 3")
        .await;
    db.refresh_st("ann_churn_stream").await;
    let after_update_delete: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(retry)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect update and delete");
    assert!(
        after_update_delete["known_operations_since_reported_coverage"]
            .as_i64()
            .is_some_and(|count| count >= 5)
    );
    db.execute(
        "SELECT pgtrickle.restore_from_snapshot('ann_churn_stream', 'public.ann_churn_archive')",
    )
    .await;
    let after_restore: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect restore");
    assert!(after_restore["known_operations_since_reported_coverage"].is_null());
    db.refresh_st("ann_churn_stream").await;
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let renewed: i64 = db.query_scalar(request_sql).await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(renewed)
        .execute(&mut *worker)
        .await
        .expect("claim after rewrite");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
        .bind(renewed)
        .execute(&mut *worker)
        .await
        .expect("report failure");
    let after_failure: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(renewed)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect failure");
    assert!(after_failure["known_operations_since_reported_coverage"].is_null());
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(renewed)
        .execute(&mut *worker)
        .await
        .expect("retry claim");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(renewed)
        .execute(&mut *worker)
        .await
        .expect("establish new baseline");
    let recovered: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(renewed)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect new baseline");
    assert_eq!(
        recovered["known_operations_since_reported_coverage"].as_i64(),
        Some(0)
    );
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_churn_operations = 9223372036854775807 WHERE pgt_name='ann_churn_stream'").await;
    db.execute("INSERT INTO ann_churn_source VALUES (5, '[1,1,1]')")
        .await;
    db.refresh_st("ann_churn_stream").await;
    let overflow: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(renewed)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect overflow");
    assert!(overflow["known_operations_since_reported_coverage"].is_null());
    db.assert_st_matches_query(
        "ann_churn_stream",
        "SELECT id, embedding FROM ann_churn_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_maintenance_coverage_preserves_refreshes_after_claim() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_coverage_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_coverage_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_coverage_stream",
        "SELECT id, embedding FROM ann_coverage_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_coverage_idx ON ann_coverage_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE INDEX ann_coverage_other_idx ON ann_coverage_stream USING hnsw (embedding vector_l2_ops)").await;
    let request_sql = "SELECT pgtrickle.request_ann_maintenance('ann_coverage_stream'::regclass::oid, 'ann_coverage_idx'::regclass::oid)";
    let request: i64 = db.query_scalar(request_sql).await;
    let other: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_coverage_stream'::regclass::oid, 'ann_coverage_other_idx'::regclass::oid)").await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET rows_changed_since_last_reindex=42 WHERE pgt_name='ann_coverage_stream'").await;
    db.execute("INSERT INTO ann_coverage_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_coverage_stream").await;
    let epoch_at_claim: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_coverage_stream'").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("LOCK TABLE ann_coverage_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold barrier");
    let mut child = ann_worker_command(&db, &database, "postgres", request, "30s")
        .spawn()
        .expect("start worker");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE datname=current_database() AND application_name='pgtrickle_ann_worker' AND $1 = ANY(pg_blocking_pids(pid)))")
                .bind(blocker_pid).fetch_one(&db.pool).await.expect("inspect worker");
            if waiting { break; }
            assert!(child.try_wait().expect("inspect worker process").is_none(), "worker ended before reaching barrier");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("worker did not reach barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect claim");
    assert!(status["last_observed_epoch"].as_i64() < Some(epoch_at_claim));
    assert_eq!(
        status["claim_epoch"].as_i64(),
        Some(epoch_at_claim),
        "claim must capture the latest committed epoch"
    );
    assert!(status["reported_covered_epoch"].is_null());
    db.execute("INSERT INTO ann_coverage_source VALUES (3, '[0,0,1]')")
        .await;
    db.refresh_st("ann_coverage_stream").await;
    let later_epoch: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_coverage_stream'").await;
    assert!(later_epoch > epoch_at_claim);
    blocker.commit().await.expect("release barrier");
    let output = tokio::time::timeout(Duration::from_secs(30), child.wait_with_output())
        .await
        .expect("worker did not finish")
        .expect("wait for worker");
    assert!(
        output.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let legacy_counter: i64 = db.query_scalar("SELECT rows_changed_since_last_reindex FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_coverage_stream'").await;
    assert_eq!(
        legacy_counter, 42,
        "completion must not reset the table-wide churn counter"
    );
    let replacement: i64 = db.query_scalar(request_sql).await;
    assert_ne!(request, replacement);
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(replacement)
            .fetch_one(&db.pool)
            .await
            .expect("inspect replacement coverage");
    assert_eq!(
        status["reported_covered_epoch"].as_i64(),
        Some(epoch_at_claim)
    );
    assert_eq!(
        status["epoch_advances_since_reported_coverage"].as_i64(),
        Some(later_epoch - epoch_at_claim)
    );
    assert!(status["reported_covered_at"].is_string());
    let independent: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(other)
            .fetch_one(&db.pool)
            .await
            .expect("inspect independent index");
    assert!(
        independent["reported_covered_epoch"].is_null(),
        "other indexes must not inherit coverage"
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let mut blocker = db.pool.begin().await.expect("begin retry barrier");
    sqlx::query("LOCK TABLE ann_coverage_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold retry barrier");
    let output = ann_worker_command(&db, &database, "postgres", replacement, "250ms")
        .output()
        .await
        .expect("run failed retry");
    assert!(!output.status.success());
    blocker.rollback().await.expect("release retry barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(replacement)
            .fetch_one(&db.pool)
            .await
            .expect("inspect failed retry");
    assert_eq!(status["state"], "FAILED");
    assert_eq!(
        status["reported_covered_epoch"].as_i64(),
        Some(epoch_at_claim),
        "a failed rebuild must not advance coverage"
    );
}

#[tokio::test]
async fn test_ann_maintenance_coverage_rolls_back_and_keeps_unknown_unknown() {
    use sqlx::Acquire;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_coverage_tx_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_coverage_tx_stream",
        "SELECT id, embedding FROM ann_coverage_tx_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_coverage_tx_idx ON ann_coverage_tx_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request_sql = "SELECT pgtrickle.request_ann_maintenance('ann_coverage_tx_stream'::regclass::oid, 'ann_coverage_tx_idx'::regclass::oid)";
    let first: i64 = db.query_scalar(request_sql).await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("claim request");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_coverage_tx_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild index");
    let mut tx = (&mut *worker).begin().await.expect("begin completion");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(first)
        .execute(&mut *tx)
        .await
        .expect("report in transaction");
    let outside: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect uncommitted report");
    assert_eq!(outside["state"], "RUNNING");
    assert!(
        outside["reported_covered_epoch"].is_null(),
        "uncommitted coverage must not be visible"
    );
    tx.rollback().await.expect("roll back completion");
    let rolled_back: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect rolled-back report");
    assert_eq!(rolled_back["state"], "RUNNING");
    assert!(rolled_back["reported_covered_epoch"].is_null());
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("commit completion");
    let committed: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect committed report");
    assert!(committed["reported_covered_epoch"].is_number());
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_refresh_epoch=NULL WHERE pgt_name='ann_coverage_tx_stream'").await;
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at=clock_timestamp() - interval '1 second'").await;
    let next: i64 = db.query_scalar(request_sql).await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("claim unknown epoch");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_coverage_tx_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild with unknown epoch");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("finish unknown epoch");
    let unknown: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(next)
            .fetch_one(&db.pool)
            .await
            .expect("inspect unknown coverage");
    assert!(unknown["claim_epoch"].is_null());
    assert!(unknown["reported_covered_epoch"].is_null());
    assert!(unknown["epoch_advances_since_reported_coverage"].is_null());
    assert!(unknown["reported_covered_at"].is_string());
}

#[tokio::test]
async fn test_ann_worker_allows_reads_and_refresh_during_concurrent_reindex() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_live_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_live_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_live_stream",
        "SELECT id, embedding FROM ann_live_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_live_idx ON ann_live_stream USING hnsw (embedding vector_l2_ops)")
        .await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_live_stream'::regclass::oid, 'ann_live_idx'::regclass::oid)").await;

    let mut old_snapshot = db.pool.begin().await.expect("begin old snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *old_snapshot)
        .await
        .expect("set isolation");
    sqlx::query("SELECT count(*) FROM ann_live_stream")
        .fetch_one(&mut *old_snapshot)
        .await
        .expect("establish old snapshot");
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut command = ann_worker_command(&db, &database, "postgres", request, "30s");
    command.kill_on_drop(true);
    let mut worker = command.spawn().expect("start worker");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_progress_create_index p JOIN pg_stat_activity a USING (pid) WHERE a.application_name = 'pgtrickle_ann_worker' AND p.command = 'REINDEX CONCURRENTLY' AND p.phase LIKE 'waiting for old snapshots%')")
                .fetch_one(&db.pool).await.expect("inspect reindex progress");
            if waiting {
                break;
            }
            assert!(worker.try_wait().expect("inspect worker").is_none(), "worker exited before reaching old snapshot wait");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("worker did not reach concurrent reindex snapshot wait");

    tokio::time::timeout(Duration::from_secs(5), async {
        db.execute("INSERT INTO ann_live_source VALUES (2, '[0,1,0]')")
            .await;
        db.refresh_st("ann_live_stream").await;
        assert_eq!(db.count("ann_live_stream").await, 2);
        let mut reader = db.pool.begin().await.expect("begin reader");
        sqlx::query("SET LOCAL enable_seqscan = off")
            .execute(&mut *reader)
            .await
            .expect("enable index plan");
        let plan: Vec<String> = sqlx::query_scalar("EXPLAIN (COSTS OFF) SELECT id FROM ann_live_stream ORDER BY embedding <-> '[0,1,0]'::vector LIMIT 1")
            .fetch_all(&mut *reader).await.expect("explain nearest-neighbor read");
        assert!(plan.iter().any(|line| line.contains("ann_live_idx")), "expected ANN index in {plan:?}");
        let nearest: i32 = sqlx::query_scalar("SELECT id FROM ann_live_stream ORDER BY embedding <-> '[0,1,0]'::vector LIMIT 1")
            .fetch_one(&mut *reader).await.expect("read nearest neighbor");
        assert_eq!(nearest, 2);
        reader.commit().await.expect("finish reader");
    })
    .await
    .expect("reads or refresh writes blocked during concurrent reindex");
    old_snapshot.commit().await.expect("release old snapshot");
    let output = tokio::time::timeout(Duration::from_secs(30), worker.wait_with_output())
        .await
        .expect("worker did not finish")
        .expect("wait for worker");
    assert!(
        output.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect result");
    assert_eq!(status["state"], "SUCCEEDED");
    assert_eq!(db.count("ann_live_stream").await, 2);
}

#[tokio::test]
async fn test_ann_worker_rebuilds_only_the_requested_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_worker_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_worker_stream",
        "SELECT id, embedding FROM ann_worker_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX \"ann worker\"\"; DROP TABLE ann_worker_source; --\" ON ann_worker_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE INDEX ann_worker_btree ON ann_worker_stream (id)")
        .await;
    let (index_oid, old_node, btree_node): (i64, i64, i64) = sqlx::query_as("SELECT i.indexrelid::bigint, pg_relation_filenode(i.indexrelid)::bigint, pg_relation_filenode('ann_worker_btree'::regclass)::bigint FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid='ann_worker_stream'::regclass AND c.relname='ann worker\"; DROP TABLE ann_worker_source; --'")
        .fetch_one(&db.pool).await.expect("index identities");
    let request: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_stream'::regclass::oid, $1::bigint::oid)")
        .bind(index_oid).fetch_one(&db.pool).await.expect("request work");
    db.execute("CREATE ROLE ann_worker_operator LOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, public TO ann_worker_operator")
        .await;
    db.execute("GRANT SELECT, MAINTAIN ON ann_worker_stream TO ann_worker_operator")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.claim_ann_maintenance(bigint), pgtrickle.ann_maintenance_request(bigint), pgtrickle.finish_ann_maintenance(bigint, boolean, text) TO ann_worker_operator").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let output = ann_worker_command(&db, &database, "ann_worker_operator", request, "5s")
        .output()
        .await
        .expect("run ANN worker");
    assert!(
        output.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect result");
    assert_eq!(status["state"], "SUCCEEDED");
    let (new_oid, new_node, other_node): (i64, i64, i64) = sqlx::query_as("SELECT i.indexrelid::bigint, pg_relation_filenode(i.indexrelid)::bigint, pg_relation_filenode('ann_worker_btree'::regclass)::bigint FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid WHERE i.indrelid='ann_worker_stream'::regclass AND c.relname='ann worker\"; DROP TABLE ann_worker_source; --'")
        .fetch_one(&db.pool).await.expect("rebuilt index identities");
    assert_ne!(index_oid, new_oid);
    assert_ne!(old_node, new_node);
    assert_eq!(
        btree_node, other_node,
        "unrelated indexes must not be rebuilt"
    );
    assert_eq!(
        db.count("ann_worker_source").await,
        1,
        "index names must not become SQL"
    );
    let replacement: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_stream'::regclass::oid, $1::bigint::oid)")
        .bind(new_oid).fetch_one(&db.pool).await.expect("request replacement index");
    db.execute("REVOKE MAINTAIN ON ann_worker_stream FROM ann_worker_operator")
        .await;
    let output = ann_worker_command(&db, &database, "ann_worker_operator", replacement, "5s")
        .output()
        .await
        .expect("run without MAINTAIN");
    assert!(
        !output.status.success(),
        "worker must respect revoked MAINTAIN"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(replacement)
            .fetch_one(&db.pool)
            .await
            .expect("inspect denied request");
    assert_eq!(status["state"], "REQUESTED");
    db.execute("GRANT MAINTAIN ON ann_worker_stream TO ann_worker_operator")
        .await;
    let output = ann_worker_command(&db, &database, "ann_worker_operator", replacement, "5s")
        .output()
        .await
        .expect("run during cooldown");
    assert!(!output.status.success(), "worker must respect cooldown");
}

#[tokio::test]
async fn test_ann_worker_rebuilds_only_the_requested_partition_leaf() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_partition_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_partition_source SELECT i, '[1,0,0]'::vector FROM generate_series(1, 20) i")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('ann_partition_stream', $$SELECT id, embedding FROM ann_partition_source$$, '1h', 'DIFFERENTIAL', partition_by => 'HASH:id:4')").await;
    db.execute("CREATE INDEX ann_partition_hnsw ON ann_partition_stream USING hnsw (embedding vector_cosine_ops)").await;
    let leaf_indexes: Vec<(i64, String, i64)> = sqlx::query_as(
        "SELECT idx.oid::bigint, idx.relname::text, pg_relation_filenode(idx.oid)::bigint \
         FROM pg_inherits h JOIN pg_index i ON i.indrelid = h.inhrelid \
         JOIN pg_class idx ON idx.oid = i.indexrelid JOIN pg_am a ON a.oid = idx.relam \
         WHERE h.inhparent = 'ann_partition_stream'::regclass AND a.amname = 'hnsw' \
         ORDER BY idx.relname",
    )
    .fetch_all(&db.pool)
    .await
    .expect("find partition leaf indexes");
    assert_eq!(leaf_indexes.len(), 4);
    let request: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_partition_stream'::regclass::oid, $1::bigint::oid)")
        .bind(leaf_indexes[0].0).fetch_one(&db.pool).await.expect("request one leaf");
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let output = ann_worker_command(&db, &database, "postgres", request, "5s")
        .output()
        .await
        .expect("run partition leaf worker");
    assert!(
        output.status.success(),
        "worker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect partition request");
    assert_eq!(status["state"], "SUCCEEDED");
    for (position, (old_oid, name, old_node)) in leaf_indexes.iter().enumerate() {
        let (oid, node): (i64, i64) = sqlx::query_as("SELECT oid::bigint, pg_relation_filenode(oid)::bigint FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relname = $1")
            .bind(name).fetch_one(&db.pool).await.expect("inspect leaf after rebuild");
        if position == 0 {
            assert_ne!(
                (oid, node),
                (*old_oid, *old_node),
                "requested leaf must be rebuilt"
            );
        } else {
            assert_eq!(
                (oid, node),
                (*old_oid, *old_node),
                "other leaves must remain untouched"
            );
        }
    }
    db.assert_st_matches_query(
        "ann_partition_stream",
        "SELECT id, embedding FROM ann_partition_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_worker_rejects_a_replaced_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_stale_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_worker_stale_stream",
        "SELECT id, embedding FROM ann_worker_stale_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_worker_stale_idx ON ann_worker_stale_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_stale_stream'::regclass::oid, 'ann_worker_stale_idx'::regclass::oid)").await;
    db.execute("DROP INDEX ann_worker_stale_idx").await;
    db.execute("CREATE INDEX ann_worker_stale_idx ON ann_worker_stale_stream USING hnsw (embedding vector_cosine_ops)").await;
    let before: i64 = db
        .query_scalar("SELECT 'ann_worker_stale_idx'::regclass::oid::bigint")
        .await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let output = ann_worker_command(&db, &database, "postgres", request, "5s")
        .output()
        .await
        .expect("run stale request");
    assert!(
        !output.status.success(),
        "worker must reject the stale identity"
    );
    let after: i64 = db
        .query_scalar("SELECT 'ann_worker_stale_idx'::regclass::oid::bigint")
        .await;
    assert_eq!(before, after, "replacement must not be rebuilt");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect stale request");
    assert_eq!(status["state"], "REQUESTED");
    assert_eq!(status["identity_current"], false);
}

#[tokio::test]
async fn test_ann_worker_revalidates_index_after_claim() {
    use std::process::Stdio;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_race_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_worker_race_stream",
        "SELECT id, embedding FROM ann_worker_race_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_worker_race_idx ON ann_worker_race_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_race_stream'::regclass::oid, 'ann_worker_race_idx'::regclass::oid)").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut command = ann_worker_command(&db, &database, "postgres", request, "5s");
    command.stdin(Stdio::piped());
    let mut child = command.spawn().expect("start ANN worker");
    let mut input = child.stdin.take().expect("worker stdin");
    let script = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/scripts/ann_maintenance_worker.sql"
    ));
    let claim_line =
        "SELECT pgtrickle.claim_ann_maintenance(:'request_id'::bigint) AS ann_claim \\gset\n";
    let (before, after) = script
        .split_once(claim_line)
        .expect("worker claim boundary");
    input
        .write_all(before.as_bytes())
        .await
        .expect("send worker setup");
    input
        .write_all(claim_line.as_bytes())
        .await
        .expect("send worker claim");
    input.flush().await.expect("flush worker input");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let status: serde_json::Value =
                sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
                    .bind(request)
                    .fetch_one(&db.pool)
                    .await
                    .expect("inspect claim");
            if status["state"] == "RUNNING" {
                break;
            }
            assert!(
                child.try_wait().expect("inspect worker process").is_none(),
                "worker exited before claiming"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("worker did not claim request");
    db.execute("DROP INDEX ann_worker_race_idx").await;
    db.execute("CREATE INDEX ann_worker_race_idx ON ann_worker_race_stream USING hnsw (embedding vector_cosine_ops)").await;
    let replacement: i64 = db
        .query_scalar("SELECT 'ann_worker_race_idx'::regclass::oid::bigint")
        .await;
    input
        .write_all(after.as_bytes())
        .await
        .expect("send remainder of worker");
    input.shutdown().await.expect("close worker input");
    let output = tokio::time::timeout(Duration::from_secs(15), child.wait_with_output())
        .await
        .expect("worker did not finish")
        .expect("wait for worker");
    assert!(
        !output.status.success(),
        "worker must not execute a stale claim"
    );
    let after: i64 = db
        .query_scalar("SELECT 'ann_worker_race_idx'::regclass::oid::bigint")
        .await;
    assert_eq!(replacement, after, "replacement must not be rebuilt");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect stale claim");
    assert_eq!(
        status["state"],
        "FAILED",
        "worker stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn test_ann_worker_records_lock_timeout_and_can_retry() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_timeout_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_worker_timeout_stream",
        "SELECT id, embedding FROM ann_worker_timeout_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_worker_timeout_idx ON ann_worker_timeout_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_timeout_stream'::regclass::oid, 'ann_worker_timeout_idx'::regclass::oid)").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    sqlx::query("LOCK TABLE ann_worker_timeout_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold barrier");
    let output = ann_worker_command(&db, &database, "postgres", request, "250ms")
        .output()
        .await
        .expect("run timed worker");
    assert!(
        !output.status.success(),
        "timed-out rebuild must return an error"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect failure");
    assert_eq!(
        status["state"],
        "FAILED",
        "worker output: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        status["last_error"]
            .as_str()
            .unwrap_or_default()
            .contains("Concurrent reindex failed"),
        "status: {status}; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("lock timeout"));
    assert!(status["next_eligible_at"].is_string());
    blocker.rollback().await.expect("release barrier");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let output = ann_worker_command(&db, &database, "postgres", request, "5s")
        .output()
        .await
        .expect("retry worker");
    assert!(
        output.status.success(),
        "retry failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect retry");
    assert_eq!(status["state"], "SUCCEEDED");
}

#[tokio::test]
async fn test_ann_worker_records_backend_cancellation() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_worker_cancel_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_worker_cancel_stream",
        "SELECT id, embedding FROM ann_worker_cancel_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_worker_cancel_idx ON ann_worker_cancel_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_worker_cancel_stream'::regclass::oid, 'ann_worker_cancel_idx'::regclass::oid)").await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("LOCK TABLE ann_worker_cancel_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold barrier");
    let mut child = ann_worker_command(&db, &database, "postgres", request, "30s")
        .spawn()
        .expect("start ANN worker");
    let worker_pid = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_catalog.pg_stat_activity WHERE datname = current_database() AND application_name = 'pgtrickle_ann_worker' AND $1 = ANY(pg_blocking_pids(pid)) LIMIT 1")
                .bind(blocker_pid).fetch_optional(&db.pool).await.expect("inspect worker");
            if let Some(pid) = pid { break pid; }
            assert!(child.try_wait().expect("inspect worker process").is_none(), "worker ended before reaching the barrier");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("worker did not reach barrier");
    tokio::time::timeout(Duration::from_secs(10), async {
        assert_eq!(db.count("ann_worker_cancel_stream").await, 0);
        db.execute("INSERT INTO ann_worker_cancel_source VALUES (1, '[1,0,0]')")
            .await;
        db.refresh_st("ann_worker_cancel_stream").await;
        assert_eq!(db.count("ann_worker_cancel_stream").await, 1);
    })
    .await
    .expect("reads and refresh must progress while the worker waits");
    assert!(
        child.try_wait().expect("inspect worker").is_none(),
        "worker must still be blocked"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("request cancellation");
    assert_eq!(status["claim_pid"], worker_pid);
    let signaled: bool = sqlx::query_scalar("SELECT pg_catalog.pg_cancel_backend(pid) FROM pg_catalog.pg_stat_activity WHERE pid = $1 AND backend_start = $2::text::timestamptz")
        .bind(worker_pid).bind(status["claim_backend_start"].as_str().expect("backend start"))
        .fetch_one(&db.pool).await.expect("signal worker");
    assert!(signaled);
    let output = tokio::time::timeout(Duration::from_secs(15), child.wait_with_output())
        .await
        .expect("worker did not exit")
        .expect("wait for worker");
    assert!(
        !output.status.success(),
        "canceled worker must return failure"
    );
    blocker.rollback().await.expect("release barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect cancellation");
    assert_eq!(
        status["state"],
        "CANCELLED",
        "worker stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        status["current_epoch"].as_i64() > status["claim_epoch"].as_i64(),
        "refresh after the claim must remain visible"
    );
    assert!(
        status["last_error"]
            .as_str()
            .unwrap_or_default()
            .contains("Concurrent reindex failed")
    );
}

#[tokio::test]
async fn test_ann_maintenance_intent_is_transactional_deduplicated_and_identity_bound() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_intent_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_intent_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_intent_stream",
        "SELECT id, embedding FROM ann_intent_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_intent_hnsw ON ann_intent_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE INDEX ann_intent_btree ON ann_intent_stream (id)")
        .await;
    let request_sql = "SELECT pgtrickle.request_ann_maintenance('ann_intent_stream'::regclass::oid, 'ann_intent_hnsw'::regclass::oid)";
    let mut tx = db.pool.begin().await.expect("begin intent transaction");
    let _: i64 = sqlx::query_scalar(request_sql)
        .fetch_one(&mut *tx)
        .await
        .expect("request intent");
    let outside: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_ann_maintenance_requests")
        .await;
    assert_eq!(outside, 0, "uncommitted intent must not be visible");
    tx.rollback().await.expect("roll back intent");
    assert_eq!(
        db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.pgt_ann_maintenance_requests")
            .await,
        0
    );

    let (a, b) = tokio::join!(
        sqlx::query_scalar::<_, i64>(request_sql).fetch_one(&db.pool),
        sqlx::query_scalar::<_, i64>(request_sql).fetch_one(&db.pool)
    );
    let first = a.expect("first request");
    assert_eq!(first, b.expect("concurrent duplicate request"));
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect request");
    assert_eq!(status["state"], "REQUESTED");
    assert_eq!(status["identity_current"], true);
    let (first_epoch, last_epoch): (Option<i64>, Option<i64>) = sqlx::query_as("SELECT first_observed_epoch, last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1").bind(first).fetch_one(&db.pool).await.expect("request epochs");
    assert!(first_epoch.is_some());
    assert_eq!(first_epoch, last_epoch);
    db.execute("INSERT INTO ann_intent_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_intent_stream").await;
    let changed: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect intervening refresh");
    assert_eq!(changed["identity_current"], true);
    assert!(changed["current_epoch"].as_i64() > changed["last_observed_epoch"].as_i64());
    let repeated: i64 = db.query_scalar(request_sql).await;
    assert_eq!(first, repeated);
    let dump_condition: String = db.query_scalar("SELECT extcondition[array_position(extconfig, 'pgtrickle.pgt_ann_maintenance_requests'::regclass::oid)] FROM pg_extension WHERE extname='pg_trickle'").await;
    assert_eq!(
        dump_condition, "WHERE false",
        "database-local requests must not be restored as valid work"
    );
    let claim_dump_condition: String = db.query_scalar("SELECT extcondition[array_position(extconfig, 'pgtrickle.pgt_ann_maintenance_claims'::regclass::oid)] FROM pg_extension WHERE extname='pg_trickle'").await;
    assert_eq!(claim_dump_condition, "WHERE false");
    let (original, latest): (Option<i64>, Option<i64>) = sqlx::query_as("SELECT first_observed_epoch, last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1").bind(first).fetch_one(&db.pool).await.expect("updated request epochs");
    assert_eq!(original, first_epoch);
    assert!(latest > original);
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_refresh_epoch = NULL WHERE pgt_name = 'ann_intent_stream'").await;
    assert_eq!(db.query_scalar::<i64>(request_sql).await, first);
    let unknown: (Option<i64>, Option<i64>) = sqlx::query_as("SELECT first_observed_epoch, last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1").bind(first).fetch_one(&db.pool).await.expect("unknown epochs");
    assert_eq!(unknown, (None, None));
    let invalid = db.try_execute("SELECT pgtrickle.request_ann_maintenance('ann_intent_stream'::regclass::oid, 'ann_intent_btree'::regclass::oid)").await;
    assert!(invalid.is_err(), "non-ANN indexes must be rejected");
    db.execute("DROP INDEX ann_intent_hnsw").await;
    let stale: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(first)
            .fetch_one(&db.pool)
            .await
            .expect("inspect stale request");
    assert_eq!(stale["identity_current"], false);
    db.execute("CREATE INDEX ann_intent_hnsw ON ann_intent_stream USING hnsw (embedding vector_cosine_ops)").await;
    let replacement: i64 = db.query_scalar(request_sql).await;
    assert_ne!(
        first, replacement,
        "a replacement index must not reuse the old request"
    );
    assert_eq!(
        db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.pgt_ann_maintenance_requests")
            .await,
        2
    );
    db.execute("SELECT pgtrickle.drop_stream_table('ann_intent_stream')")
        .await;
    assert_eq!(
        db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.pgt_ann_maintenance_requests")
            .await,
        0
    );
}

#[tokio::test]
async fn test_ann_maintenance_intent_requires_execute_select_and_maintain() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_intent_auth_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_intent_auth_stream",
        "SELECT id, embedding FROM ann_intent_auth_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_intent_auth_idx ON ann_intent_auth_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE ROLE ann_intent_operator NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO ann_intent_operator")
        .await;
    db.execute("GRANT SELECT, MAINTAIN ON ann_intent_auth_stream TO ann_intent_operator")
        .await;
    let private: bool = db.query_scalar("SELECT NOT has_table_privilege('ann_intent_operator', 'pgtrickle.pgt_ann_maintenance_requests', 'SELECT') AND NOT has_table_privilege('ann_intent_operator', 'pgtrickle.pgt_ann_maintenance_claims', 'SELECT')").await;
    assert!(private, "request catalog must not be publicly readable");
    let cancel_private: bool = db.query_scalar("SELECT NOT has_function_privilege('ann_intent_operator', 'pgtrickle.cancel_ann_maintenance(bigint)', 'EXECUTE')").await;
    assert!(cancel_private, "PUBLIC must not execute cancellation");
    let mut tx = db.pool.begin().await.expect("begin unauthorized call");
    sqlx::query("SET LOCAL ROLE ann_intent_operator")
        .execute(&mut *tx)
        .await
        .expect("set role");
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_intent_auth_stream'::regclass::oid, 'ann_intent_auth_idx'::regclass::oid)";
    assert!(
        sqlx::query_scalar::<_, i64>(request)
            .fetch_one(&mut *tx)
            .await
            .is_err(),
        "PUBLIC must not execute the function"
    );
    tx.rollback().await.expect("end unauthorized call");
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.request_ann_maintenance(oid, oid) TO ann_intent_operator").await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.ann_maintenance_request(bigint) TO ann_intent_operator").await;
    db.execute("REVOKE MAINTAIN ON ann_intent_auth_stream FROM ann_intent_operator")
        .await;
    let mut tx = db.pool.begin().await.expect("begin denied privilege call");
    sqlx::query("SET LOCAL ROLE ann_intent_operator")
        .execute(&mut *tx)
        .await
        .expect("set role");
    assert!(
        sqlx::query_scalar::<_, i64>(request)
            .fetch_one(&mut *tx)
            .await
            .is_err(),
        "MAINTAIN must be rechecked"
    );
    tx.rollback().await.expect("end denied call");
    db.execute("GRANT MAINTAIN ON ann_intent_auth_stream TO ann_intent_operator")
        .await;
    let mut tx = db.pool.begin().await.expect("begin authorized call");
    sqlx::query("SET LOCAL ROLE ann_intent_operator")
        .execute(&mut *tx)
        .await
        .expect("set role");
    let id: i64 = sqlx::query_scalar(request)
        .fetch_one(&mut *tx)
        .await
        .expect("authorized request");
    assert!(id > 0);
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .expect("authorized inspection");
    assert_eq!(status["identity_current"], true);
    tx.commit().await.expect("commit authorized request");
    db.execute("REVOKE MAINTAIN ON ann_intent_auth_stream FROM ann_intent_operator")
        .await;
    let mut tx = db.pool.begin().await.expect("begin revoked inspection");
    sqlx::query("SET LOCAL ROLE ann_intent_operator")
        .execute(&mut *tx)
        .await
        .expect("set role");
    assert!(
        sqlx::query_scalar::<_, serde_json::Value>("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await.expect("end revoked inspection");
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.claim_ann_maintenance(bigint) TO ann_intent_operator",
    )
    .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.finish_ann_maintenance(bigint, boolean, text) TO ann_intent_operator").await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.cancel_ann_maintenance(bigint) TO ann_intent_operator",
    )
    .await;
    let mut operator = db.pool.acquire().await.expect("operator session");
    sqlx::query("SET ROLE ann_intent_operator")
        .execute(&mut *operator)
        .await
        .expect("set operator role");
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&mut *operator)
            .await
            .is_err(),
        "claim must recheck MAINTAIN"
    );
    assert!(
        sqlx::query("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .execute(&mut *operator)
            .await
            .is_err(),
        "cancellation must recheck MAINTAIN"
    );
    db.execute("GRANT MAINTAIN ON ann_intent_auth_stream TO ann_intent_operator")
        .await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *operator)
        .await
        .expect("authorized claim");
    db.execute("REVOKE MAINTAIN ON ann_intent_auth_stream FROM ann_intent_operator")
        .await;
    assert!(
        sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
            .bind(id)
            .execute(&mut *operator)
            .await
            .is_err(),
        "success must recheck MAINTAIN"
    );
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'privilege revoked')")
        .bind(id)
        .execute(&mut *operator)
        .await
        .expect("record failure after revocation");
    sqlx::query("RESET ROLE")
        .execute(&mut *operator)
        .await
        .expect("reset operator role");
}

#[tokio::test]
async fn test_ann_maintenance_intent_delayed_request_cannot_regress_epoch() {
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_intent_race_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_intent_race_stream",
        "SELECT id, embedding FROM ann_intent_race_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_intent_race_idx ON ann_intent_race_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE FUNCTION public.hold_old_ann_request() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN \
        IF current_setting('application_name') = 'ann-old-request' THEN \
          PERFORM pg_advisory_xact_lock(7341, 82); END IF; RETURN NEW; END $$").await;
    db.execute("CREATE TRIGGER hold_old_ann_request BEFORE INSERT ON pgtrickle.pgt_ann_maintenance_requests \
        FOR EACH ROW EXECUTE FUNCTION public.hold_old_ann_request()").await;
    let mut blocker = db.pool.begin().await.expect("begin barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("SELECT pg_advisory_xact_lock(7341, 82)")
        .execute(&mut *blocker)
        .await
        .expect("hold request barrier");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let old = tokio::spawn(async move {
        let mut tx = pool.begin().await.expect("begin old request");
        sqlx::query("SET LOCAL application_name = 'ann-old-request'")
            .execute(&mut *tx)
            .await
            .expect("mark old request");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx)
            .await
            .expect("old request pid");
        sender.send(pid).expect("send pid");
        let id: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_intent_race_stream'::regclass::oid, 'ann_intent_race_idx'::regclass::oid)")
            .fetch_one(&mut *tx).await.expect("submit old request");
        tx.commit().await.expect("commit old request");
        id
    });
    let old_pid = receiver.await.expect("receive old request pid");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                .bind(blocker_pid)
                .bind(old_pid)
                .fetch_one(&db.pool)
                .await
                .expect("inspect barrier");
            if waiting {
                break;
            }
            assert!(
                !old.is_finished(),
                "old request ended before reaching the barrier"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("old request did not reach the barrier");
    db.execute("INSERT INTO ann_intent_race_source VALUES (1, '[1,0,0]')")
        .await;
    db.refresh_st("ann_intent_race_stream").await;
    let newer: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_intent_race_stream'::regclass::oid, 'ann_intent_race_idx'::regclass::oid)").await;
    let observed: i64 = sqlx::query_scalar("SELECT last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1")
        .bind(newer).fetch_one(&db.pool).await.expect("read newer epoch");
    blocker.commit().await.expect("release barrier");
    assert_eq!(old.await.expect("join old request"), newer);
    let (first, last): (i64, i64) = sqlx::query_as("SELECT first_observed_epoch, last_observed_epoch FROM pgtrickle.pgt_ann_maintenance_requests WHERE request_id = $1")
        .bind(newer).fetch_one(&db.pool).await.expect("read merged epochs");
    assert!(first < last, "the older observation should remain visible");
    assert_eq!(
        last, observed,
        "a delayed request must not lower the latest epoch"
    );
}

#[tokio::test]
async fn test_ann_maintenance_cancel_is_transactional_and_reopens_only_on_request() {
    use sqlx::Connection;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_cancel_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_cancel_stream",
        "SELECT id, embedding FROM ann_cancel_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_cancel_idx ON ann_cancel_stream USING hnsw (embedding vector_cosine_ops)",
    )
    .await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_cancel_stream'::regclass::oid, 'ann_cancel_idx'::regclass::oid)";
    let id: i64 = db.query_scalar(request).await;
    let mut tx = db.pool.begin().await.expect("begin cancellation");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&mut *tx)
            .await
            .expect("cancel in transaction");
    assert_eq!(status["state"], "CANCELLED");
    tx.rollback().await.expect("roll back cancellation");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("read rolled-back status");
    assert_eq!(status["state"], "REQUESTED");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("cancel queued request");
    assert_eq!(status["state"], "CANCELLED");
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&db.pool)
            .await
            .is_err()
    );
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("claim reopened request");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("request running cancellation");
    assert_eq!(status["state"], "RUNNING");
    assert_eq!(status["cancel_requested"], true);
    assert!(status["claim_pid"].is_number());
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'operator canceled')")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("report canceled work");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("canceled outcome");
    assert_eq!(status["state"], "CANCELLED");
    assert_eq!(status["last_error"], "operator canceled");
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("claim after canceled outcome");
    sqlx::query("SELECT pgtrickle.cancel_ann_maintenance($1)")
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("cancel before disconnect");
    worker
        .detach()
        .close()
        .await
        .expect("disconnect canceled worker");
    let status = wait_for_ann_worker_exit(&db, id).await;
    assert_eq!(status["state"], "CANCELLED");
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&db.pool)
            .await
            .is_err(),
        "a canceled interrupted claim must not restart itself"
    );
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let mut worker = db.pool.acquire().await.expect("replacement worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("new request can reclaim interrupted work");
    sqlx::query("SELECT pgtrickle.cancel_ann_maintenance($1)")
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("request late cancellation");
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    worker
        .detach()
        .close()
        .await
        .expect("disconnect after a later request");
    let status = wait_for_ann_worker_exit(&db, id).await;
    assert_eq!(status["state"], "INTERRUPTED");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let mut worker = db.pool.acquire().await.expect("final worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("claim the later request");
    sqlx::query("SELECT pgtrickle.cancel_ann_maintenance($1)")
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("request cancellation before completion");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_cancel_idx")
        .execute(&mut *worker)
        .await
        .expect("complete before cancellation is delivered");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("report completed rebuild");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("completed status");
    assert_eq!(
        status["state"], "SUCCEEDED",
        "a late cancellation must not hide a completed rebuild"
    );
}

#[tokio::test]
async fn test_ann_maintenance_cancellation_preserves_later_request() {
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_cancel_live_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_cancel_live_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_cancel_live_stream",
        "SELECT id, embedding FROM ann_cancel_live_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_cancel_live_idx ON ann_cancel_live_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_cancel_live_stream'::regclass::oid, 'ann_cancel_live_idx'::regclass::oid)";
    let id: i64 = db.query_scalar(request).await;
    let mut blocker = db.pool.begin().await.expect("begin writer barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("LOCK TABLE ann_cancel_live_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold writer barrier");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let worker = tokio::spawn(async move {
        let mut conn = pool.acquire().await.expect("worker session");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await
            .expect("worker pid");
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&mut *conn)
            .await
            .expect("claim work");
        sender.send(pid).expect("send worker pid");
        let error = sqlx::query("REINDEX INDEX CONCURRENTLY ann_cancel_live_idx")
            .execute(&mut *conn)
            .await
            .expect_err("rebuild must be canceled");
        let code = error
            .as_database_error()
            .and_then(|e| e.code())
            .map(|code| code.into_owned());
        sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'query canceled')")
            .bind(id)
            .execute(&mut *conn)
            .await
            .expect("record canceled outcome");
        code
    });
    let pid = receiver.await.expect("receive worker pid");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                .bind(blocker_pid)
                .bind(pid)
                .fetch_one(&db.pool)
                .await
                .expect("inspect barrier");
            if waiting {
                break;
            }
            assert!(
                !worker.is_finished(),
                "worker ended before reaching barrier"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("rebuild did not reach barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.cancel_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("request cancellation");
    assert_eq!(status["state"], "RUNNING");
    assert_eq!(status["cancel_requested"], true);
    assert_eq!(status["claim_pid"], pid);
    db.execute("INSERT INTO ann_cancel_live_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_cancel_live_stream").await;
    assert_eq!(
        db.query_scalar::<i64>(request).await,
        id,
        "later request must be retained"
    );
    let signaled: bool = sqlx::query_scalar("SELECT pg_catalog.pg_cancel_backend(pid) FROM pg_catalog.pg_stat_activity WHERE pid = $1 AND backend_start = $2::text::timestamptz")
        .bind(pid).bind(status["claim_backend_start"].as_str().expect("worker start"))
        .fetch_one(&db.pool).await.expect("signal worker with native privileges");
    assert!(signaled);
    let code = tokio::time::timeout(Duration::from_secs(15), worker)
        .await
        .expect("worker did not stop")
        .expect("join worker");
    assert_eq!(code.as_deref(), Some("57014"));
    blocker.rollback().await.expect("release barrier");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("inspect canceled work");
    assert_eq!(
        status["state"], "REQUESTED",
        "the later request must remain pending"
    );
    assert_eq!(status["cancel_requested"], false);
    assert_eq!(status["last_error"], "query canceled");
    assert!(status["next_eligible_at"].is_string());
    assert_eq!(db.count("ann_cancel_live_stream").await, 2);
}

#[tokio::test]
async fn test_ann_maintenance_cooldown_survives_reindex_and_failure_backoff() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_cooldown_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_cooldown_stream",
        "SELECT id, embedding FROM ann_cooldown_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_cooldown_idx ON ann_cooldown_stream USING hnsw (embedding vector_cosine_ops)").await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_cooldown_stream'::regclass::oid, 'ann_cooldown_idx'::regclass::oid)";
    let first: i64 = db.query_scalar(request).await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("claim first request");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_cooldown_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild index");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("finish first request");
    let next: i64 = db.query_scalar(request).await;
    assert_ne!(first, next);
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(next)
            .fetch_one(&db.pool)
            .await
            .expect("replacement status");
    assert!(
        status["next_eligible_at"].is_string(),
        "successful rebuild must expose a cooldown for its replacement"
    );
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(next)
            .execute(&mut *worker)
            .await
            .is_err(),
        "cooldown must prevent another rebuild"
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    for (attempt, minimum_seconds, maximum_seconds) in [
        (1, 15.0, 31.0),
        (2, 40.0, 61.0),
        (3, 100.0, 121.0),
        (4, 200.0, 241.0),
        (5, 400.0, 481.0),
        (6, 800.0, 961.0),
        (6, 800.0, 961.0),
    ] {
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(next)
            .execute(&mut *worker)
            .await
            .expect("claim after delay");
        sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
            .bind(next)
            .execute(&mut *worker)
            .await
            .expect("record failed attempt");
        let status: serde_json::Value =
            sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
                .bind(next)
                .fetch_one(&db.pool)
                .await
                .expect("failure status");
        assert_eq!(status["consecutive_failures"], attempt);
        let delay: f64 = db.query_scalar("SELECT extract(epoch FROM (next_eligible_at - clock_timestamp()))::float8 FROM pgtrickle.pgt_ann_maintenance_claims").await;
        assert!(
            delay > minimum_seconds && delay < maximum_seconds,
            "failure delay must increase on retry and stop at the cap"
        );
        assert!(
            sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
                .bind(next)
                .execute(&mut *worker)
                .await
                .is_err()
        );
        db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    }
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("claim after backoff");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_cooldown_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild after failures");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("record successful retry");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(next)
            .fetch_one(&db.pool)
            .await
            .expect("successful retry status");
    assert_eq!(status["consecutive_failures"], 0);
}

#[tokio::test]
async fn test_ann_maintenance_claim_serializes_concurrent_reindex_replacement() {
    use sqlx::Connection;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_swap_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_swap_stream",
        "SELECT id, embedding FROM ann_swap_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_swap_idx ON ann_swap_stream USING hnsw (embedding vector_cosine_ops)",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_swap_other_idx ON ann_swap_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_swap_stream'::regclass::oid, 'ann_swap_idx'::regclass::oid)";
    let first: i64 = db.query_scalar(request).await;
    let mut worker = db.pool.acquire().await.expect("first worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("first claim");
    let distinct: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_swap_stream'::regclass::oid, 'ann_swap_other_idx'::regclass::oid)").await;
    let mut other = db.pool.acquire().await.expect("second worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(distinct)
        .execute(&mut *other)
        .await
        .expect("a different ANN index can be claimed independently");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'not run')")
        .bind(distinct)
        .execute(&mut *other)
        .await
        .expect("release unrelated claim");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_swap_idx")
        .execute(&mut *worker)
        .await
        .expect("replace index");
    let replacement: i64 = db.query_scalar(request).await;
    assert_ne!(first, replacement);
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(replacement)
            .execute(&mut *other)
            .await
            .is_err(),
        "replacement index must remain claimed until the first worker finishes"
    );
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(first)
        .execute(&mut *worker)
        .await
        .expect("finish first claim");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second' WHERE leaf_relid = 'ann_swap_stream'::regclass::oid AND request_id IS NULL").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(replacement)
        .execute(&mut *other)
        .await
        .expect("claim after first worker finishes");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_swap_idx")
        .execute(&mut *other)
        .await
        .expect("replace index again");
    let next: i64 = db.query_scalar(request).await;
    other
        .detach()
        .close()
        .await
        .expect("disconnect second worker");
    wait_for_ann_worker_exit(&db, replacement).await;
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("reclaim replacement after worker disconnects");
}

#[tokio::test]
async fn test_ann_claim_interval_starts_after_guard_lock_wait() {
    use sqlx::Connection;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_interval_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_interval_stream",
        "SELECT id, embedding FROM ann_interval_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_interval_idx ON ann_interval_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_interval_stream'::regclass::oid, 'ann_interval_idx'::regclass::oid)").await;
    let mut first = db.pool.acquire().await.expect("first worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *first)
        .await
        .expect("initial claim");
    first.detach().close().await.expect("disconnect worker");
    wait_for_ann_worker_exit(&db, request).await;
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let mut blocker = db.pool.begin().await.expect("begin guard lock");
    sqlx::query("SELECT 1 FROM pgtrickle.pgt_ann_maintenance_claims FOR UPDATE")
        .fetch_one(&mut *blocker)
        .await
        .expect("lock guard row");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("guard backend");
    let pool = db.pool.clone();
    let claimant = tokio::spawn(async move {
        let mut conn = pool.acquire().await.expect("claimant session");
        sqlx::query("SET application_name = 'ann_interval_claimant'")
            .execute(&mut *conn)
            .await
            .expect("name claimant");
        sqlx::query_scalar::<_, f64>("WITH claim AS MATERIALIZED (SELECT pgtrickle.claim_ann_maintenance($1) AS status) SELECT extract(epoch FROM (status->>'next_eligible_at')::timestamptz - clock_timestamp())::float8 FROM claim")
            .bind(request).fetch_one(&mut *conn).await.expect("read remaining interval")
    });
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name='ann_interval_claimant' AND $1 = ANY(pg_blocking_pids(pid)))")
                .bind(blocker_pid).fetch_one(&db.pool).await.expect("inspect blocked claim");
            if waiting { break; }
            assert!(!claimant.is_finished(), "claim exited before the guard wait");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("claim did not wait on guard");
    tokio::time::sleep(Duration::from_secs(3)).await;
    blocker.commit().await.expect("release guard");
    let remaining = claimant.await.expect("join claimant");
    assert!(
        remaining > 58.0,
        "interval began before the guard was acquired: {remaining}"
    );
}

#[tokio::test]
async fn test_ann_interrupted_claims_back_off_and_success_resets() {
    use sqlx::Connection;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_crash_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_crash_stream",
        "SELECT id, embedding FROM ann_crash_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_crash_idx ON ann_crash_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_crash_stream'::regclass::oid, 'ann_crash_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("initial worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("initial claim");
    worker
        .detach()
        .close()
        .await
        .expect("interrupt initial claim");
    wait_for_ann_worker_exit(&db, request).await;
    for failures in 1..=6 {
        assert!(
            sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
                .bind(request)
                .execute(&db.pool)
                .await
                .is_err(),
            "interrupted retry must respect its deadline"
        );
        db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
        let mut worker = db.pool.acquire().await.expect("retry worker");
        let status: serde_json::Value =
            sqlx::query_scalar("SELECT pgtrickle.claim_ann_maintenance($1)")
                .bind(request)
                .fetch_one(&mut *worker)
                .await
                .expect("claim after deadline");
        assert_eq!(status["consecutive_failures"], failures);
        let expected_seconds = 30 * (1 << failures.min(5));
        let remaining: f64 = sqlx::query_scalar("SELECT extract(epoch FROM next_eligible_at - clock_timestamp())::float8 FROM pgtrickle.pgt_ann_maintenance_claims WHERE request_id = $1")
            .bind(request).fetch_one(&mut *worker).await.expect("read backoff");
        assert!(
            remaining > f64::from(expected_seconds) * 0.9
                && remaining <= f64::from(expected_seconds),
            "unexpected backoff: {remaining}"
        );
        if failures == 6 {
            sqlx::query("REINDEX INDEX CONCURRENTLY ann_crash_idx")
                .execute(&mut *worker)
                .await
                .expect("rebuild after interruptions");
            sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
                .bind(request)
                .execute(&mut *worker)
                .await
                .expect("report success");
        } else {
            worker.detach().close().await.expect("interrupt retry");
            wait_for_ann_worker_exit(&db, request).await;
        }
    }
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect successful retry");
    assert_eq!(status["state"], "SUCCEEDED");
    assert_eq!(status["consecutive_failures"], 0);
}

#[tokio::test]
async fn test_ann_maintenance_claim_is_session_bound_and_retryable() {
    use sqlx::Connection;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_claim_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_claim_stream",
        "SELECT id, embedding FROM ann_claim_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute(
        "CREATE INDEX ann_claim_idx ON ann_claim_stream USING hnsw (embedding vector_cosine_ops)",
    )
    .await;
    let request = "SELECT pgtrickle.request_ann_maintenance('ann_claim_stream'::regclass::oid, 'ann_claim_idx'::regclass::oid)";
    let id: i64 = db.query_scalar(request).await;
    let mut tx = db.pool.begin().await.expect("begin claim transaction");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *tx)
        .await
        .expect("claim inside transaction");
    tx.rollback().await.expect("roll back claim");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("rolled-back status");
    assert_eq!(status["state"], "REQUESTED");
    let mut worker = db.pool.acquire().await.expect("worker connection");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .fetch_one(&mut *worker)
            .await
            .expect("claim request");
    assert_eq!(status["state"], "RUNNING");
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&db.pool)
            .await
            .is_err(),
        "another session cannot claim live work"
    );
    assert!(
        sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
            .bind(id)
            .execute(&db.pool)
            .await
            .is_err(),
        "another session cannot finish a claim"
    );
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("record failure");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("failed status");
    assert_eq!(status["state"], "FAILED");
    assert_eq!(status["last_error"], "test failure");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("retry failed request");
    worker.detach().close().await.expect("disconnect worker");
    let status = wait_for_ann_worker_exit(&db, id).await;
    assert_eq!(status["state"], "INTERRUPTED");
    assert!(status["next_eligible_at"].is_string());
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(id)
            .execute(&db.pool)
            .await
            .is_err(),
        "interrupted claims must respect the minimum interval"
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let mut worker = db.pool.acquire().await.expect("replacement worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("reclaim interrupted request");
    db.execute("INSERT INTO ann_claim_source VALUES (1, '[1,0,0]')")
        .await;
    db.refresh_st("ann_claim_stream").await;
    assert_eq!(db.query_scalar::<i64>(request).await, id);
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_claim_idx")
        .execute(&mut *worker)
        .await
        .expect("concurrent rebuild");
    let stale: bool = db.query_scalar("SELECT (v->>'maintenance_stale')::boolean FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'ann_claim_stream'").await;
    assert!(
        !stale,
        "a live rebuild must not be marked stale before completion"
    );
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(id)
        .execute(&mut *worker)
        .await
        .expect("record success");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(id)
            .fetch_one(&db.pool)
            .await
            .expect("completed status");
    assert_eq!(
        status["state"], "REQUESTED",
        "work requested during the run stays pending"
    );
    assert_eq!(
        status["identity_current"], false,
        "concurrent rebuild replaces the index identity"
    );
    let stale: bool = db.query_scalar("SELECT (v->>'maintenance_stale')::boolean FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'ann_claim_stream'").await;
    assert!(
        stale,
        "the stale pending request must be visible in stream diagnostics"
    );
    let next: i64 = db.query_scalar(request).await;
    assert_ne!(id, next);
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("claim replacement index");
    sqlx::query("REINDEX INDEX CONCURRENTLY ann_claim_idx")
        .execute(&mut *worker)
        .await
        .expect("rebuild replacement index");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(next)
        .execute(&mut *worker)
        .await
        .expect("record success");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(next)
            .fetch_one(&db.pool)
            .await
            .expect("completed status");
    assert_eq!(status["state"], "SUCCEEDED");
    sqlx::query("SELECT pgtrickle.cancel_ann_maintenance($1)")
        .bind(id)
        .execute(&db.pool)
        .await
        .expect("cancel stale request after requesting the replacement");
    let stale: bool = db.query_scalar("SELECT (v->>'maintenance_stale')::boolean FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'ann_claim_stream'").await;
    assert!(!stale, "canceled stale requests must not appear actionable");
}

#[tokio::test]
async fn test_ann_worker_retries_after_restart_during_concurrent_reindex() {
    use std::time::Duration;

    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_restart_live_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_restart_live_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_restart_live_stream",
        "SELECT id, embedding FROM ann_restart_live_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_restart_live_idx ON ann_restart_live_stream USING hnsw (embedding vector_l2_ops)").await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_restart_live_stream'::regclass::oid, 'ann_restart_live_idx'::regclass::oid)").await;
    let original_index: i64 = db
        .query_scalar("SELECT 'ann_restart_live_idx'::regclass::oid::bigint")
        .await;
    let database: String = db.query_scalar("SELECT current_database()::text").await;
    let mut snapshot = db.pool.begin().await.expect("begin old snapshot");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *snapshot)
        .await
        .expect("set snapshot isolation");
    sqlx::query("SELECT count(*) FROM ann_restart_live_stream")
        .fetch_one(&mut *snapshot)
        .await
        .expect("hold old snapshot");
    let mut command = ann_worker_command(&db, &database, "postgres", request, "30s");
    command.kill_on_drop(true);
    let mut worker = command.spawn().expect("start concurrent worker");
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_progress_create_index p JOIN pg_stat_activity a USING (pid) WHERE a.application_name = 'pgtrickle_ann_worker' AND p.command = 'REINDEX CONCURRENTLY' AND p.phase LIKE 'waiting for old snapshots%')")
                .fetch_one(&db.pool).await.expect("inspect reindex progress");
            if waiting { break; }
            assert!(worker.try_wait().expect("inspect worker").is_none(), "worker exited before restart barrier");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }).await.expect("reindex did not reach snapshot wait");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() + interval '5 minutes'").await;
    let restarted = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart PostgreSQL during reindex");
    assert!(
        restarted.status.success(),
        "{}",
        String::from_utf8_lossy(&restarted.stderr)
    );
    drop(snapshot);
    let interrupted = tokio::time::timeout(Duration::from_secs(30), worker.wait_with_output())
        .await
        .expect("worker remained alive")
        .expect("wait for interrupted worker");
    assert!(
        !interrupted.status.success(),
        "interrupted worker must not report success"
    );
    db.pool.close().await;
    db.pool = db.reconnect_after_restart().await;
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect interrupted request");
    assert_eq!(status["state"], "INTERRUPTED");
    assert_eq!(status["identity_current"], true);
    assert!(status["next_eligible_at"].is_string());
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(request)
            .execute(&db.pool)
            .await
            .is_err(),
        "restart must not erase the attempt interval"
    );
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() - interval '1 second'").await;
    let invalid_before: i64 = db.query_scalar("SELECT count(*) FROM pg_index WHERE indrelid='ann_restart_live_stream'::regclass AND NOT indisvalid").await;
    assert!(
        invalid_before > 0,
        "interrupted concurrent reindex should leave an invalid index for operator inspection"
    );
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.ann_restart_live_stream'])")
        .await;
    db.execute("INSERT INTO ann_restart_live_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st_with_retry("ann_restart_live_stream").await;
    let retried = ann_worker_command(&db, &database, "postgres", request, "30s")
        .output()
        .await
        .expect("retry interrupted worker");
    assert!(
        retried.status.success(),
        "retry failed: {}",
        String::from_utf8_lossy(&retried.stderr)
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&db.pool)
            .await
            .expect("inspect retry");
    assert_eq!(status["state"], "SUCCEEDED");
    let (new_index, valid): (i64, bool) = sqlx::query_as("SELECT i.indexrelid::bigint, i.indisvalid AND i.indisready AND i.indislive FROM pg_index i WHERE i.indexrelid = 'ann_restart_live_idx'::regclass")
        .fetch_one(&db.pool).await.expect("inspect rebuilt index");
    assert_ne!(original_index, new_index);
    assert!(valid);
    let invalid_after: i64 = db.query_scalar("SELECT count(*) FROM pg_index WHERE indrelid='ann_restart_live_stream'::regclass AND NOT indisvalid").await;
    assert_eq!(
        invalid_after, invalid_before,
        "retry must not silently drop an interrupted index"
    );
    assert_eq!(invalid_after, 1, "the test created one interrupted index");
    let cleanup: String = db.query_scalar("SELECT format('DROP INDEX CONCURRENTLY %I.%I', n.nspname, c.relname) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE i.indrelid='ann_restart_live_stream'::regclass AND NOT i.indisvalid").await;
    sqlx::query(sqlx::AssertSqlSafe(cleanup))
        .execute(&db.pool)
        .await
        .expect("remove the verified abandoned test index");
    let remaining: i64 = db.query_scalar("SELECT count(*) FROM pg_index WHERE indrelid='ann_restart_live_stream'::regclass AND NOT indisvalid").await;
    assert_eq!(remaining, 0);
    db.assert_st_matches_query(
        "ann_restart_live_stream",
        "SELECT id, embedding FROM ann_restart_live_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_maintenance_intent_survives_restart() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_intent_restart_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_intent_restart_stream",
        "SELECT id, embedding FROM ann_intent_restart_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_intent_restart_idx ON ann_intent_restart_stream USING hnsw (embedding vector_cosine_ops)").await;
    let sql = "SELECT pgtrickle.request_ann_maintenance('ann_intent_restart_stream'::regclass::oid, 'ann_intent_restart_idx'::regclass::oid)";
    let before: i64 = db.query_scalar(sql).await;
    let mut worker = db.pool.acquire().await.expect("restart worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(before)
        .execute(&mut *worker)
        .await
        .expect("claim before restart");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
        .bind(before)
        .execute(&mut *worker)
        .await
        .expect("fail before restart");
    db.execute("UPDATE pgtrickle.pgt_ann_maintenance_claims SET next_eligible_at = clock_timestamp() + interval '5 minutes'").await;
    drop(worker);
    db.pool.close().await;
    let restarted = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart PostgreSQL");
    assert!(restarted.status.success());
    db.pool = db.reconnect_after_restart().await;
    let after: i64 = db.query_scalar(sql).await;
    assert_eq!(
        before, after,
        "restart must preserve and deduplicate the request"
    );
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(after)
            .fetch_one(&db.pool)
            .await
            .expect("status after restart");
    assert_eq!(status["consecutive_failures"], 1);
    assert!(
        status["next_eligible_at"].is_string(),
        "retry schedule must survive restart"
    );
    assert!(
        sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
            .bind(after)
            .execute(&db.pool)
            .await
            .is_err(),
        "restart must not erase backoff"
    );
}

#[tokio::test]
async fn test_ann_refresh_epoch_is_transactional_and_independent_of_legacy_counter() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_epoch_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "ann_epoch_stream",
        "SELECT id, embedding FROM ann_epoch_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let epoch_sql = "SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_stream'";
    let before: i64 = db.query_scalar(epoch_sql).await;
    db.execute("INSERT INTO ann_epoch_source VALUES (1, '[1,0,0]')")
        .await;
    let mut tx = db.pool.begin().await.expect("begin refresh transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('ann_epoch_stream')")
        .execute(&mut *tx)
        .await
        .expect("refresh in transaction");
    let in_transaction: i64 = sqlx::query_scalar(epoch_sql)
        .fetch_one(&mut *tx)
        .await
        .expect("read uncommitted epoch");
    assert!(in_transaction > before);
    let outside: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(
        outside, before,
        "uncommitted refresh must not publish its epoch"
    );
    tx.rollback().await.expect("roll back refresh");
    let rolled_back: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(rolled_back, before);
    db.refresh_st("ann_epoch_stream").await;
    let committed: i64 = db.query_scalar(epoch_sql).await;
    assert!(committed > before);
    db.refresh_st("ann_epoch_stream").await;
    let no_data: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(
        no_data, committed,
        "a no-data differential check must not advance the epoch"
    );
    db.execute(
        "SELECT pgtrickle.alter_stream_table('ann_epoch_stream', post_refresh_action => 'reindex')",
    )
    .await;
    db.execute("INSERT INTO ann_epoch_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_epoch_stream").await;
    let (after_reindex, counter): (i64, i64) = sqlx::query_as("SELECT ann_refresh_epoch, rows_changed_since_last_reindex FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_stream'")
        .fetch_one(&db.pool).await.expect("read maintenance counters");
    assert!(after_reindex > committed);
    assert_eq!(counter, 0, "legacy reindex resets only the churn counter");
    db.assert_st_matches_query(
        "ann_epoch_stream",
        "SELECT id, embedding FROM ann_epoch_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_refresh_epoch_tracks_snapshot_restore_transactionally() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_snapshot_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_snapshot_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_snapshot_stream",
        "SELECT id, embedding FROM ann_snapshot_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.snapshot_stream_table('ann_snapshot_stream', 'public.ann_snapshot_archive')").await;
    db.execute("INSERT INTO ann_snapshot_source VALUES (2, '[0,1,0]')")
        .await;
    db.refresh_st("ann_snapshot_stream").await;
    let epoch_sql = "SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_snapshot_stream'";
    let before: i64 = db.query_scalar(epoch_sql).await;
    let mut tx = db.pool.begin().await.expect("begin restore transaction");
    sqlx::query("SELECT pgtrickle.restore_from_snapshot('ann_snapshot_stream', 'public.ann_snapshot_archive')")
        .execute(&mut *tx).await.expect("restore snapshot");
    let uncommitted: i64 = sqlx::query_scalar(epoch_sql)
        .fetch_one(&mut *tx)
        .await
        .expect("read epoch");
    assert!(uncommitted > before);
    tx.rollback().await.expect("roll back restore");
    let rolled_back: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(rolled_back, before);
    db.execute("SELECT pgtrickle.restore_from_snapshot('ann_snapshot_stream', 'public.ann_snapshot_archive')").await;
    let committed: i64 = db.query_scalar(epoch_sql).await;
    assert!(committed > before);
    let rows: i64 = db
        .query_scalar("SELECT count(*) FROM ann_snapshot_stream")
        .await;
    assert_eq!(rows, 1);
}

#[tokio::test]
async fn test_ann_refresh_epoch_tracks_immediate_topk_changes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_topk_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_topk_source VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('ann_topk_stream', 'SELECT id, embedding FROM ann_topk_source ORDER BY id DESC LIMIT 1', NULL, 'IMMEDIATE')").await;
    let epoch_sql = "SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_topk_stream'";
    let before: i64 = db.query_scalar(epoch_sql).await;
    let mut tx = db.pool.begin().await.expect("begin Top-K transaction");
    sqlx::query("INSERT INTO ann_topk_source VALUES (2, '[0,1,0]')")
        .execute(&mut *tx)
        .await
        .expect("insert in Top-K transaction");
    let uncommitted: i64 = sqlx::query_scalar(epoch_sql)
        .fetch_one(&mut *tx)
        .await
        .expect("read uncommitted Top-K epoch");
    assert!(uncommitted > before);
    tx.rollback().await.expect("roll back Top-K change");
    let rolled_back: i64 = db.query_scalar(epoch_sql).await;
    assert_eq!(rolled_back, before);
    db.execute("INSERT INTO ann_topk_source VALUES (2, '[0,1,0]')")
        .await;
    let id: i32 = db.query_scalar("SELECT id FROM ann_topk_stream").await;
    assert_eq!(id, 2);
    let after: i64 = db.query_scalar(epoch_sql).await;
    assert!(after > before);
}

#[tokio::test]
async fn test_ann_refresh_epoch_tracks_immediate_truncate_transactionally() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_truncate_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_truncate_source VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('ann_truncate_stream', 'SELECT id, embedding FROM ann_truncate_source', NULL, 'IMMEDIATE')").await;
    db.execute(
        "CREATE INDEX ann_truncate_idx ON ann_truncate_stream USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('ann_truncate_stream'::regclass::oid, 'ann_truncate_idx'::regclass::oid)").await;
    let mut worker = db.pool.acquire().await.expect("worker session");
    let claim: serde_json::Value = sqlx::query_scalar("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .fetch_one(&mut *worker)
        .await
        .expect("claim request");
    let claim_epoch = claim["claim_epoch"].as_i64().expect("claim epoch");
    let mut tx = db.pool.begin().await.expect("begin truncate transaction");
    sqlx::query("TRUNCATE ann_truncate_source")
        .execute(&mut *tx)
        .await
        .expect("truncate source");
    let uncommitted: i64 = sqlx::query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_truncate_stream'")
        .fetch_one(&mut *tx).await.expect("read uncommitted epoch");
    assert!(uncommitted > claim_epoch);
    tx.rollback().await.expect("roll back truncate");
    let after_rollback: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_truncate_stream'").await;
    assert_eq!(after_rollback, claim_epoch);
    db.execute("TRUNCATE ann_truncate_source").await;
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, true, '')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("finish request");
    let status: serde_json::Value =
        sqlx::query_scalar("SELECT pgtrickle.ann_maintenance_request($1)")
            .bind(request)
            .fetch_one(&mut *worker)
            .await
            .expect("inspect request");
    assert_eq!(status["reported_covered_epoch"].as_i64(), Some(claim_epoch));
    assert!(
        status["epoch_advances_since_reported_coverage"]
            .as_i64()
            .is_some_and(|n| n > 0)
    );
    let remaining: i64 = db
        .query_scalar("SELECT count(*) FROM ann_truncate_stream")
        .await;
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn test_ann_refresh_epoch_covers_immediate_and_full_rebuilds() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_epoch_modes_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_epoch_modes_source VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('ann_epoch_immediate', 'SELECT id, embedding FROM ann_epoch_modes_source', NULL, 'IMMEDIATE')").await;
    db.create_st(
        "ann_epoch_full",
        "SELECT id, embedding FROM ann_epoch_modes_source",
        "1h",
        "FULL",
    )
    .await;
    let immediate_before: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_immediate'").await;
    let full_before: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_full'").await;
    db.refresh_st("ann_epoch_full").await;
    let full_noop: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_full'").await;
    assert!(
        full_noop > full_before,
        "a full storage rewrite must advance the epoch even when its result is unchanged"
    );
    db.execute("INSERT INTO ann_epoch_modes_source VALUES (2, '[0,1,0]')")
        .await;
    let immediate_after: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_immediate'").await;
    assert!(immediate_after > immediate_before);
    db.refresh_st("ann_epoch_full").await;
    let full_after: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_full'").await;
    assert!(full_after > full_noop);
    db.execute("SELECT pgtrickle.alter_stream_table('ann_epoch_full', query => 'SELECT id, embedding FROM ann_epoch_modes_source WHERE id > 1')").await;
    let after_alter: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name='ann_epoch_full'").await;
    assert!(
        after_alter > full_after,
        "storage rebuild must advance the epoch"
    );
    db.assert_st_matches_query(
        "ann_epoch_full",
        "SELECT id, embedding FROM ann_epoch_modes_source WHERE id > 1",
    )
    .await;
}

#[tokio::test]
async fn test_ann_refresh_epoch_overflow_becomes_unknown_without_losing_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE ann_epoch_overflow_source (id int PRIMARY KEY)")
        .await;
    db.create_st(
        "ann_epoch_overflow",
        "SELECT id FROM ann_epoch_overflow_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET ann_refresh_epoch = 9223372036854775807 WHERE pgt_name = 'ann_epoch_overflow'").await;
    db.execute("INSERT INTO ann_epoch_overflow_source VALUES (1)")
        .await;
    db.refresh_st("ann_epoch_overflow").await;
    let unknown: Option<i64> = db.query_scalar_opt("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_overflow'").await;
    assert_eq!(unknown, None);
    db.assert_st_matches_query(
        "ann_epoch_overflow",
        "SELECT id FROM ann_epoch_overflow_source",
    )
    .await;
    db.execute("INSERT INTO ann_epoch_overflow_source VALUES (2)")
        .await;
    db.refresh_st("ann_epoch_overflow").await;
    let still_unknown: Option<i64> = db.query_scalar_opt("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_overflow'").await;
    assert_eq!(still_unknown, None);
    db.assert_st_matches_query(
        "ann_epoch_overflow",
        "SELECT id FROM ann_epoch_overflow_source",
    )
    .await;
}

#[tokio::test]
async fn test_ann_refresh_epoch_tracks_immediate_phantom_cleanup_without_delta() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE epoch_join_left (id int PRIMARY KEY, value int)")
        .await;
    db.execute("CREATE TABLE epoch_join_right (id int PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO epoch_join_left VALUES (1, 10)")
        .await;
    db.execute("INSERT INTO epoch_join_right VALUES (1)").await;
    let query =
        "SELECT l.id, l.value FROM epoch_join_left l JOIN epoch_join_right r ON l.id = r.id";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('epoch_join_stream', $${query}$$, NULL, 'IMMEDIATE')"
    ))
    .await;
    // Simulate a pre-existing phantom to exercise cleanup when the new delta is empty.
    db.execute("ALTER TABLE epoch_join_stream DISABLE TRIGGER USER")
        .await;
    db.execute("INSERT INTO epoch_join_stream (__pgt_row_id, id, value) VALUES (decode('00', 'hex'), 999, 999)").await;
    db.execute("ALTER TABLE epoch_join_stream ENABLE TRIGGER USER")
        .await;
    let before: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'epoch_join_stream'").await;
    let before_churn: (i64, i64) = sqlx::query_as("SELECT ann_churn_generation, ann_churn_operations FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'epoch_join_stream'")
        .fetch_one(&db.pool).await.expect("read churn before phantom cleanup");
    db.execute("INSERT INTO epoch_join_left VALUES (2, 20)")
        .await;
    let after: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'epoch_join_stream'").await;
    assert!(after > before, "cleanup-only writes must advance the epoch");
    let after_churn: (i64, i64) = sqlx::query_as("SELECT ann_churn_generation, ann_churn_operations FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'epoch_join_stream'")
        .fetch_one(&db.pool).await.expect("read churn after phantom cleanup");
    assert_eq!(after_churn, (before_churn.0, before_churn.1 + 1));
    db.assert_st_matches_query("epoch_join_stream", query).await;
}

#[tokio::test]
async fn test_ann_refresh_epoch_survives_database_restart() {
    let mut db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute("CREATE TABLE ann_epoch_restart_source (id int PRIMARY KEY)")
        .await;
    db.create_st(
        "ann_epoch_restart",
        "SELECT id FROM ann_epoch_restart_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("INSERT INTO ann_epoch_restart_source VALUES (1)")
        .await;
    db.refresh_st("ann_epoch_restart").await;
    let before: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_restart'").await;
    db.pool.close().await;
    let restarted = tokio::process::Command::new("docker")
        .args(["restart", db.container_id()])
        .output()
        .await
        .expect("restart PostgreSQL");
    assert!(restarted.status.success(), "container restart failed");
    db.pool = db.reconnect_after_restart().await;
    let after: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_restart'").await;
    assert_eq!(after, before);
    db.execute("INSERT INTO ann_epoch_restart_source VALUES (2)")
        .await;
    db.refresh_st("ann_epoch_restart").await;
    let next: i64 = db.query_scalar("SELECT ann_refresh_epoch FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'ann_epoch_restart'").await;
    assert!(next > after);
    db.assert_st_matches_query(
        "ann_epoch_restart",
        "SELECT id FROM ann_epoch_restart_source",
    )
    .await;
}

#[tokio::test]
async fn test_pgvector_compat_installed_version() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    let (server, installed, pg_trickle): (i32, String, String) = sqlx::query_as(
        "SELECT current_setting('server_version_num')::int, \
         (SELECT extversion FROM pg_extension WHERE extname = 'vector'), \
         (SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle')",
    )
    .fetch_one(&db.pool)
    .await
    .expect("inspect installed versions");
    assert_eq!(server / 10_000, 18, "wrong PostgreSQL major version");
    assert_eq!(pg_trickle, env!("CARGO_PKG_VERSION"));
    if let Ok(expected) = std::env::var("PGVECTOR_EXPECTED_VERSION") {
        assert_eq!(server, 180_006, "wrong PostgreSQL compatibility image");
        assert_eq!(installed, expected, "wrong pgvector test image");
    }
}

#[tokio::test]
async fn test_pgvector_helper_missing_extension_is_actionable_and_atomic() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE no_vector_source (id int PRIMARY KEY, embedding text)")
        .await;
    let error = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table_v2('no_vector_stream', 'no_vector_source', 'embedding', 'cosine')")
        .await.expect_err("helper requires pgvector");
    assert!(
        error
            .to_string()
            .contains("pgvector extension is not installed"),
        "unexpected error: {error}"
    );
    let clean: bool = db
        .query_scalar(
            "SELECT to_regclass('no_vector_stream') IS NULL AND NOT EXISTS \
        (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'no_vector_stream')",
        )
        .await;
    assert!(clean, "failed helper must not leave a stream table");
}

#[tokio::test]
async fn test_pgvector_helper_dry_run_rejects_invalid_legacy_columns() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE preview_source (id int PRIMARY KEY, \"Tag,Name\" text, __pgt_row_id text, embedding vector(3))")
        .await;
    for columns in ["missing", "id, id", "embedding", "__pgt_row_id"] {
        let error = sqlx::query(
            "SELECT * FROM pgtrickle.embedding_stream_table('preview_stream', 'preview_source', 'embedding', $1, '1h', 'hnsw', true)",
        )
        .bind(columns)
        .execute(&db.pool)
        .await;
        assert!(error.is_err(), "invalid columns accepted: {columns}");
    }
    let clean: bool = db.query_scalar("SELECT to_regclass('preview_stream') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'preview_stream')").await;
    assert!(clean, "dry-run validation must not create a stream table");
    let actions: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgtrickle.embedding_stream_table('preview_stream', 'preview_source', 'embedding', $1, '1h', 'hnsw', true)",
    )
    .bind("id, \"Tag,Name\"")
    .fetch_one(&db.pool)
    .await
    .expect("validate quoted column preview");
    assert!(actions > 0);
    db.execute("CREATE SEQUENCE preview_seq").await;
    db.execute("CREATE FUNCTION preview_side_effect() RETURNS bigint LANGUAGE sql IMMUTABLE AS $$SELECT nextval('preview_seq')$$").await;
    let _: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgtrickle.embedding_stream_table('preview_stream', 'preview_source', 'embedding', $1, '1h', 'hnsw', true)",
    )
    .bind("id, preview_side_effect() AS marker")
    .fetch_one(&db.pool)
    .await
    .expect("preview must not evaluate expressions");
    let called: bool = db.query_scalar("SELECT is_called FROM preview_seq").await;
    assert!(!called, "dry run must not execute the defining query");
    let created: bool = db
        .query_scalar("SELECT to_regclass('preview_stream') IS NOT NULL")
        .await;
    assert!(
        !created,
        "successful previews must not create a stream table"
    );
}

#[tokio::test]
async fn test_pgvector_helper_invalid_column_is_actionable_and_atomic() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE invalid_vector_source (id int PRIMARY KEY, embedding text)")
        .await;
    for column in ["missing", "embedding"] {
        let error = db.try_execute(&format!("SELECT * FROM pgtrickle.embedding_stream_table_v2('invalid_vector_stream', 'invalid_vector_source', '{column}', 'cosine', '{{\"columns\":[\"id\"]}}')"))
            .await.expect_err("helper requires a pgvector column");
        assert!(
            error.to_string().contains("must be a pgvector"),
            "unexpected error: {error}"
        );
        let clean: bool = db
            .query_scalar(
                "SELECT to_regclass('invalid_vector_stream') IS NULL AND NOT EXISTS \
            (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'invalid_vector_stream')",
            )
            .await;
        assert!(clean, "failed helper must not leave a stream table");
    }
}

#[tokio::test]
async fn test_manual_ann_candidates_select_only_authorized_leaf_indexes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute(
        "CREATE TABLE ann_manual_source (id int PRIMARY KEY, embedding vector(3), label text)",
    )
    .await;
    db.execute("INSERT INTO ann_manual_source VALUES (1, '[1,0,0]', 'alpha')")
        .await;
    db.create_st_partitioned(
        "ann_manual_stream",
        "SELECT id, embedding, label FROM ann_manual_source",
        "1h",
        "DIFFERENTIAL",
        "id",
    )
    .await;
    db.execute("CREATE INDEX ann_manual_hnsw ON ann_manual_stream USING hnsw (embedding vector_cosine_ops)").await;
    db.execute("CREATE INDEX ann_manual_ivf ON ann_manual_stream USING ivfflat (embedding vector_cosine_ops) WITH (lists = 1)").await;
    db.execute("CREATE INDEX ann_manual_label ON ann_manual_stream (label)")
        .await;
    let indexes: Vec<(i64, String, String)> = sqlx::query_as(include_str!(
        "fixtures/pgvector_ann_maintenance_candidates.sql"
    ))
    .bind("ann_manual_stream")
    .fetch_all(&db.pool)
    .await
    .expect("discover ANN leaf indexes");
    assert_eq!(indexes.len(), 2);
    let methods: std::collections::HashSet<&str> =
        indexes.iter().map(|row| row.2.as_str()).collect();
    assert_eq!(methods, ["hnsw", "ivfflat"].into_iter().collect());
    let root: i64 = db
        .query_scalar("SELECT 'ann_manual_stream'::regclass::oid::bigint")
        .await;
    let request: i64 = sqlx::query_scalar(
        "SELECT pgtrickle.request_ann_maintenance($1::bigint::oid, $2::bigint::oid)",
    )
    .bind(root)
    .bind(indexes[0].0)
    .fetch_one(&db.pool)
    .await
    .expect("request a partition leaf");
    assert!(request > 0);
    let parent = db.try_execute("SELECT pgtrickle.request_ann_maintenance('ann_manual_stream'::regclass::oid, 'ann_manual_hnsw'::regclass::oid)").await;
    assert!(
        parent.is_err(),
        "partitioned parent indexes must not be enqueued as leaves"
    );
    for (oid, _, _) in indexes {
        let leaf: bool = sqlx::query_scalar("SELECT c.relkind = 'i' AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhparent = c.oid) FROM pg_class c WHERE c.oid = $1::bigint::oid")
            .bind(oid).fetch_one(&db.pool).await.expect("inspect leaf index");
        assert!(leaf);
    }
    let ordinary: Vec<(i64, String, String)> = sqlx::query_as(include_str!(
        "fixtures/pgvector_ann_maintenance_candidates.sql"
    ))
    .bind("ann_manual_source")
    .fetch_all(&db.pool)
    .await
    .expect("check non-stream table");
    assert!(ordinary.is_empty());
    db.execute("CREATE ROLE ann_manual_operator NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO ann_manual_operator")
        .await;
    db.execute(
        "GRANT SELECT ON pgtrickle.pgt_stream_tables, ann_manual_stream TO ann_manual_operator",
    )
    .await;
    let leaf: String = db.query_scalar("SELECT inhrelid::regclass::text FROM pg_inherits WHERE inhparent = 'ann_manual_stream'::regclass").await;
    let mut tx = db.pool.begin().await.expect("begin restricted check");
    sqlx::query("SET LOCAL ROLE ann_manual_operator")
        .execute(&mut *tx)
        .await
        .expect("set maintenance role");
    let denied: Vec<(i64, String, String)> = sqlx::query_as(include_str!(
        "fixtures/pgvector_ann_maintenance_candidates.sql"
    ))
    .bind("ann_manual_stream")
    .fetch_all(&mut *tx)
    .await
    .expect("check missing MAINTAIN");
    assert!(denied.is_empty());
    tx.rollback().await.expect("end restricted check");
    db.execute(&format!(
        "GRANT MAINTAIN ON TABLE {leaf} TO ann_manual_operator"
    ))
    .await;
    let mut tx = db.pool.begin().await.expect("begin authorized check");
    sqlx::query("SET LOCAL ROLE ann_manual_operator")
        .execute(&mut *tx)
        .await
        .expect("set maintenance role");
    let allowed: Vec<(i64, String, String)> = sqlx::query_as(include_str!(
        "fixtures/pgvector_ann_maintenance_candidates.sql"
    ))
    .bind("ann_manual_stream")
    .fetch_all(&mut *tx)
    .await
    .expect("check MAINTAIN grant");
    assert_eq!(allowed.len(), 2);
    tx.rollback().await.expect("end authorized check");
}

#[tokio::test]
async fn test_manual_concurrent_ann_rebuild_allows_reads_and_refresh_writes() {
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE ann_online_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO ann_online_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "ann_online_stream",
        "SELECT id, embedding FROM ann_online_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX ann_online_hnsw ON ann_online_stream USING hnsw (embedding vector_cosine_ops)").await;
    let before: i64 = db
        .query_scalar("SELECT 'ann_online_hnsw'::regclass::oid::bigint")
        .await;
    let mut blocker = db.pool.begin().await.expect("begin writer barrier");
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker)
        .await
        .expect("barrier pid");
    sqlx::query("LOCK TABLE ann_online_stream IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold writer barrier");
    let (pid_tx, pid_rx) = tokio::sync::oneshot::channel();
    let pool = db.pool.clone();
    let rebuild = tokio::spawn(async move {
        let mut conn = pool
            .acquire()
            .await
            .expect("acquire maintenance connection");
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await
            .expect("maintenance pid");
        pid_tx.send(pid).expect("send maintenance pid");
        sqlx::query("SET statement_timeout = '30s'")
            .execute(&mut *conn)
            .await
            .expect("set timeout");
        sqlx::query("REINDEX INDEX CONCURRENTLY ann_online_hnsw")
            .execute(&mut *conn)
            .await
    });
    let pid = pid_rx.await.expect("receive maintenance pid");
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let waiting: bool = sqlx::query_scalar("SELECT $1 = ANY(pg_blocking_pids($2))")
                .bind(blocker_pid)
                .bind(pid)
                .fetch_one(&db.pool)
                .await
                .expect("inspect rebuild blocker");
            if waiting {
                break;
            }
            assert!(
                !rebuild.is_finished(),
                "rebuild ended before reaching the writer barrier"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("rebuild did not wait for the writer barrier");
    tokio::time::timeout(Duration::from_secs(10), async {
        assert_eq!(db.count("ann_online_stream").await, 1);
        db.execute("INSERT INTO ann_online_source VALUES (2, '[0,1,0]')")
            .await;
        db.refresh_st("ann_online_stream").await;
        assert_eq!(db.count("ann_online_stream").await, 2);
    })
    .await
    .expect("reads and refresh must progress while rebuild waits");
    assert!(
        !rebuild.is_finished(),
        "barrier must still hold the rebuild"
    );
    blocker.commit().await.expect("release writer barrier");
    rebuild
        .await
        .expect("join maintenance task")
        .expect("concurrent rebuild");
    let after: i64 = db
        .query_scalar("SELECT 'ann_online_hnsw'::regclass::oid::bigint")
        .await;
    assert_ne!(before, after, "concurrent rebuild must replace the index");
    db.assert_st_matches_query(
        "ann_online_stream",
        "SELECT id, embedding FROM ann_online_source",
    )
    .await;
}

#[tokio::test]
async fn test_wide_vectors_refresh_without_oversized_covering_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE bounded_index_source (id int PRIMARY KEY, value int)")
        .await;
    db.create_st(
        "bounded_index_stream",
        "SELECT id, value FROM bounded_index_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    let covered: bool = db.query_scalar("SELECT indnatts > indnkeyatts FROM pg_index WHERE indexrelid = 'bounded_index_stream_row_id_idx'::regclass").await;
    assert!(covered, "small fixed-width outputs should remain covered");
    for dimensions in [768, 1536] {
        let source = format!("wide_vectors_{dimensions}");
        let stream = format!("wide_vector_st_{dimensions}");
        db.execute(&format!(
            "CREATE TABLE {source} (id int PRIMARY KEY, embedding vector({dimensions}))"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} SELECT 1, ARRAY(SELECT sin(n::float8)::real \
            FROM generate_series(1, {dimensions}) n)::vector({dimensions})"
        ))
        .await;
        db.create_st(
            &stream,
            &format!("SELECT id, embedding FROM {source}"),
            "1h",
            "DIFFERENTIAL",
        )
        .await;
        let no_includes: bool = db
            .query_scalar(&format!(
                "SELECT indnatts = indnkeyatts FROM pg_index \
            WHERE indexrelid = '{stream}_row_id_idx'::regclass"
            ))
            .await;
        assert!(
            no_includes,
            "unbounded vectors must not enter the B-tree index"
        );
        db.execute(&format!(
            "CREATE INDEX {stream}_ann ON {stream} USING hnsw (embedding vector_cosine_ops)"
        ))
        .await;
        db.execute(&format!(
            "UPDATE {source} SET embedding = ARRAY(SELECT cos(n::float8)::real \
            FROM generate_series(1, {dimensions}) n)::vector({dimensions})"
        ))
        .await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.refresh_strategy = 'differential'",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            &format!("SELECT pgtrickle.refresh_stream_table('{stream}')"),
        )
        .await
        .expect("refresh wide vector");
        db.assert_st_matches_query(&stream, &format!("SELECT id, embedding FROM {source}"))
            .await;
    }
}

#[tokio::test]
async fn test_vector_centroid_rescan_preserves_join_multiplicity_and_rollback() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE centroid_join_vectors (id int PRIMARY KEY, grp int NOT NULL, embedding vector(3))").await;
    db.execute("CREATE TABLE centroid_join_edges (id int PRIMARY KEY, vector_id int NOT NULL REFERENCES centroid_join_vectors(id))").await;
    db.execute("INSERT INTO centroid_join_vectors VALUES (1, 1, '[1,0,0]'), (2, 1, '[0,1,0]'), (3, 2, NULL)").await;
    db.execute("INSERT INTO centroid_join_edges VALUES (1, 1), (2, 1), (3, 2), (4, 3)")
        .await;
    let definition = "SELECT v.grp, avg(v.embedding) AS centroid FROM centroid_join_vectors v JOIN centroid_join_edges e ON e.vector_id = v.id GROUP BY v.grp";
    let compare = "SELECT NOT EXISTS ((SELECT grp, centroid FROM centroid_join_st EXCEPT ALL SELECT v.grp, avg(v.embedding) FROM centroid_join_vectors v JOIN centroid_join_edges e ON e.vector_id=v.id GROUP BY v.grp) UNION ALL (SELECT v.grp, avg(v.embedding) FROM centroid_join_vectors v JOIN centroid_join_edges e ON e.vector_id=v.id GROUP BY v.grp EXCEPT ALL SELECT grp, centroid FROM centroid_join_st))";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('centroid_join_st', $${definition}$$, '1h', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    for statement in [
        "INSERT INTO centroid_join_edges VALUES (5, 2)",
        "UPDATE centroid_join_vectors SET grp=2, embedding='[0,0,1]' WHERE id=1",
        "DELETE FROM centroid_join_edges WHERE id=2",
        "UPDATE centroid_join_vectors SET embedding='[1,1,0]' WHERE id=3",
        "DELETE FROM centroid_join_edges WHERE vector_id=2",
    ] {
        db.execute(statement).await;
        db.try_execute_with_config(
            &[
                "SET pg_trickle.enable_vector_agg = on",
                "SET pg_trickle.refresh_strategy = 'differential'",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            "SELECT pgtrickle.refresh_stream_table('centroid_join_st')",
        )
        .await
        .expect("refresh joined centroids");
        let differential: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='centroid_join_st' ORDER BY h.refresh_id DESC LIMIT 1").await;
        assert!(differential, "joined centroid fell back to full refresh");
        let matches: bool = db.query_scalar(compare).await;
        assert!(
            matches,
            "joined centroid must preserve duplicate contributions after {statement}"
        );
    }
    let mut tx = db.pool.begin().await.expect("begin rolled-back mutation");
    sqlx::query("UPDATE centroid_join_vectors SET grp=7, embedding='[9,9,9]' WHERE id=1")
        .execute(&mut *tx)
        .await
        .expect("mutate in transaction");
    tx.rollback().await.expect("roll back mutation");
    let matches: bool = db.query_scalar(compare).await;
    assert!(
        matches,
        "rolled-back source mutations must not alter the centroid"
    );
    db.try_execute_with_config(
        &["SET pg_trickle.enable_vector_agg = on"],
        "SELECT pgtrickle.reinitialize_stream_table('centroid_join_st')",
    )
    .await
    .expect("reinitialize joined centroid");
    assert!(
        db.query_scalar::<bool>(compare).await,
        "reinitialized centroid must match the source"
    );
    db.execute("UPDATE centroid_join_vectors SET embedding='[0,1,1]' WHERE id=1")
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.enable_vector_agg = on",
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ],
        "SELECT pgtrickle.refresh_stream_table('centroid_join_st')",
    )
    .await
    .expect("refresh after reinitialization");
    let differential: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='centroid_join_st' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(
        differential,
        "post-reinitialization refresh fell back to full"
    );
    assert!(
        db.query_scalar::<bool>(compare).await,
        "centroid must remain correct after reinitialization"
    );
}

#[tokio::test]
async fn test_vector_centroid_rescan_matches_native_avg_under_sustained_churn() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE centroid_churn_source (id int PRIMARY KEY, grp int NOT NULL, embedding vector(3))").await;
    db.execute("INSERT INTO centroid_churn_source SELECT i, i % 4, CASE WHEN i % 7 = 0 THEN NULL ELSE ARRAY[((i % 11) - 5)::real, ((i % 13) - 6)::real, ((i % 17) - 8)::real]::vector(3) END FROM generate_series(1, 48) i").await;
    db.execute("INSERT INTO centroid_churn_source VALUES (101, 9, '[1e8,0,0]'), (102, 9, '[1,0,0]'), (103, 9, '[-1e8,0,0]')").await;
    db.execute(
        "INSERT INTO centroid_churn_source VALUES (104, 10, '[1e20,0,0]'), (105, 10, '[1,0,0]')",
    )
    .await;
    let (float32_sum, native_avg): (f32, f32) = sqlx::query_as("SELECT (sum(v ORDER BY ord)::real[])[1], (avg(v ORDER BY ord)::real[])[1] FROM (VALUES (1, '[1e8,0,0]'::vector), (2, '[1,0,0]'::vector), (3, '[-1e8,0,0]'::vector)) AS input(ord, v)")
        .fetch_one(&db.pool).await.expect("measure cancellation control");
    assert_eq!(float32_sum, 0.0);
    assert!(
        (native_avg - 1.0 / 3.0).abs() < 1e-6,
        "native AVG must preserve the small contribution"
    );
    let (native_first, native_reordered): (f32, f32) = sqlx::query_as("SELECT (avg(v ORDER BY ord)::real[])[1], (avg(v ORDER BY CASE ord WHEN 1 THEN 1 WHEN 3 THEN 2 ELSE 3 END)::real[])[1] FROM (VALUES (1, '[1e20,0,0]'::vector), (2, '[1,0,0]'::vector), (3, '[-1e20,0,0]'::vector)) AS input(ord, v)")
        .fetch_one(&db.pool).await.expect("measure native order sensitivity");
    assert_eq!(native_first, 0.0);
    assert!((native_reordered - 1.0 / 3.0).abs() < 1e-6);
    let retracted_sum: f64 = db
        .query_scalar("SELECT (1e20::float8 + 1::float8) - 1e20::float8")
        .await;
    assert_eq!(retracted_sum, 0.0, "float8 state loses the small value");
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SELECT pgtrickle.create_stream_table('centroid_churn_st', $$SELECT grp, avg(embedding) AS centroid FROM centroid_churn_source GROUP BY grp$$, '1h', 'DIFFERENTIAL')",
    ]).await;
    let explanation: String = db
        .query_scalar("SELECT pgtrickle.explain_stream_table('centroid_churn_st')")
        .await;
    assert!(
        explanation.contains("centroid: GROUP_RESCAN"),
        "unexpected current strategy: {explanation}"
    );

    let mut rng = 0x4d59_5df4_d0f3_3173_u64;
    for cycle in 0..40 {
        rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let id = ((rng >> 16) % 48 + 1) as i32;
        let group = ((rng >> 32) % 5) as i32;
        match cycle % 4 {
            0 => {
                sqlx::query("DELETE FROM centroid_churn_source WHERE id=$1")
                    .bind(id)
                    .execute(&db.pool)
                    .await
                    .expect("delete input");
            }
            1 => {
                sqlx::query("INSERT INTO centroid_churn_source VALUES ($1, $2, NULL) ON CONFLICT (id) DO UPDATE SET grp=EXCLUDED.grp, embedding=NULL")
                .bind(id).bind(group).execute(&db.pool).await.expect("upsert null input");
            }
            _ => {
                let x = ((rng >> 8) % 2001) as i32 - 1000;
                let y = ((rng >> 24) % 2001) as i32 - 1000;
                let z = ((rng >> 40) % 2001) as i32 - 1000;
                let embedding = format!("[{x},{y},{z}]");
                sqlx::query("INSERT INTO centroid_churn_source VALUES ($1, $2, $3::text::vector) ON CONFLICT (id) DO UPDATE SET grp=EXCLUDED.grp, embedding=EXCLUDED.embedding")
                    .bind(id).bind(group).bind(embedding).execute(&db.pool).await.expect("upsert vector input");
            }
        }
        if cycle % 2 == 0 {
            db.execute("DELETE FROM centroid_churn_source WHERE id = 103")
                .await;
            db.execute("DELETE FROM centroid_churn_source WHERE id = 104")
                .await;
        } else {
            db.execute("INSERT INTO centroid_churn_source VALUES (103, 9, '[-1e8,0,0]')")
                .await;
            db.execute("INSERT INTO centroid_churn_source VALUES (104, 10, '[1e20,0,0]')")
                .await;
        }
        db.try_execute_with_config(
            &[
                "SET pg_trickle.enable_vector_agg = on",
                "SET pg_trickle.refresh_strategy = 'differential'",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            "SELECT pgtrickle.refresh_stream_table('centroid_churn_st')",
        )
        .await
        .expect("refresh centroid after mutation");
        let differential: bool = db.query_scalar("SELECT h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='centroid_churn_st' ORDER BY h.refresh_id DESC LIMIT 1").await;
        assert!(differential, "cycle {cycle} fell back to full refresh");
        let actual: Vec<(i32, Option<Vec<f32>>)> =
            sqlx::query_as("SELECT grp, centroid::real[] FROM centroid_churn_st ORDER BY grp")
                .fetch_all(&db.pool)
                .await
                .expect("read maintained centroids");
        let expected: Vec<(i32, Option<Vec<f32>>)> = sqlx::query_as("SELECT grp, avg(embedding)::real[] FROM centroid_churn_source GROUP BY grp ORDER BY grp")
            .fetch_all(&db.pool).await.expect("read native centroids");
        if cycle % 2 == 0 {
            assert_eq!(
                actual.iter().find(|(group, _)| *group == 10).unwrap().1,
                Some(vec![1.0, 0.0, 0.0]),
                "retracting the large vector must recover the small contribution on cycle {cycle}"
            );
        }
        assert_eq!(actual.len(), expected.len(), "group count on cycle {cycle}");
        for ((actual_group, actual), (expected_group, expected)) in actual.iter().zip(&expected) {
            assert_eq!(actual_group, expected_group, "group key on cycle {cycle}");
            match (actual, expected) {
                (None, None) => {}
                (Some(actual), Some(expected)) => {
                    assert_eq!(actual.len(), expected.len());
                    for (actual, expected) in actual.iter().zip(expected) {
                        let error = (f64::from(*actual) - f64::from(*expected)).abs();
                        let tolerance = 1e-5 + 1e-6 * f64::from(*expected).abs();
                        assert!(
                            error <= tolerance,
                            "cycle {cycle}, group {actual_group}: {actual} vs {expected}, error {error}"
                        );
                    }
                }
                _ => panic!("NULL centroid mismatch on cycle {cycle}, group {actual_group}"),
            }
        }
    }
}

#[tokio::test]
async fn test_vector_aggregate_unknown_dimensions_rejects_mixed_input_without_data_loss() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE unknown_vector_dims (id int PRIMARY KEY, embedding vector)")
        .await;
    db.execute("INSERT INTO unknown_vector_dims VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SELECT pgtrickle.create_stream_table('unknown_vector_st', $$SELECT avg(embedding) AS centroid FROM unknown_vector_dims$$, '1m', 'DIFFERENTIAL')",
    ]).await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='unknown_vector_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "vector");
    let error = db.try_execute("CREATE INDEX unknown_vector_ann ON unknown_vector_st USING hnsw (centroid vector_l2_ops)").await.expect_err("an undimensioned vector cannot be indexed directly");
    assert!(
        error.to_string().contains("dimensions"),
        "unexpected error: {error}"
    );

    db.execute("INSERT INTO unknown_vector_dims VALUES (2, '[1,0]')")
        .await;
    let error = db
        .try_execute_with_config(
            &[
                "SET pg_trickle.enable_vector_agg = on",
                "SET pg_trickle.differential_max_change_ratio = 0",
            ],
            "SELECT pgtrickle.refresh_stream_table('unknown_vector_st')",
        )
        .await
        .expect_err("mixed dimensions must not be silently narrowed");
    assert!(
        error.to_string().contains("dimensions"),
        "unexpected error: {error}"
    );
    let centroid: String = db
        .query_scalar("SELECT centroid::text FROM unknown_vector_st")
        .await;
    assert_eq!(centroid, "[1,0,0]");
}

#[tokio::test]
async fn test_vector_aggregate_qualified_join_uses_argument_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE typed_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE typed_scalars (id int PRIMARY KEY, embedding double precision)")
        .await;
    db.execute("INSERT INTO typed_vectors VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.execute("INSERT INTO typed_scalars VALUES (1, 42), (2, 10)")
        .await;
    let query = "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('typed_join_st', $${query}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    let explanation: String = db
        .query_scalar("SELECT pgtrickle.explain_stream_table('typed_join_st')")
        .await;
    assert!(
        explanation.contains("centroid: GROUP_RESCAN"),
        "vector AVG must report its actual strategy: {explanation}"
    );
    assert!(
        explanation.contains("scalar_mean: ALGEBRAIC_VIA_AUX"),
        "scalar AVG must retain its own strategy: {explanation}"
    );
    let scalar_type: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='typed_join_st'::regclass AND attname='scalar_mean'").await;
    assert_eq!(scalar_type, "double precision");
    let vector_type: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='typed_join_st'::regclass AND attname='centroid'").await;
    assert_eq!(vector_type, "vector(3)");
    let oracle = "SELECT avg(s.embedding) AS scalar_mean, avg(v.embedding)::vector(3) AS centroid FROM typed_scalars s JOIN typed_vectors v USING (id)";
    for mutations in [
        vec![
            "UPDATE typed_scalars SET embedding=20 WHERE id=2",
            "UPDATE typed_vectors SET embedding='[0,0,1]' WHERE id=2",
        ],
        vec![
            "UPDATE typed_vectors SET embedding=NULL WHERE id=1",
            "UPDATE typed_scalars SET embedding=NULL WHERE id=1",
        ],
        vec![
            "UPDATE typed_vectors SET embedding='[1,0,1]' WHERE id=1",
            "UPDATE typed_scalars SET embedding=5 WHERE id=1",
        ],
        vec!["DELETE FROM typed_vectors WHERE id=2"],
        vec![
            "INSERT INTO typed_vectors VALUES (3, '[1,1,0]')",
            "INSERT INTO typed_scalars VALUES (3, 7)",
        ],
    ] {
        db.execute_seq(&mutations).await;
        let previous: i64 = db.query_scalar("SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='typed_join_st'").await;
        db.execute_seq(&[
            "SET pg_trickle.enable_vector_agg = on",
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
            "SELECT pgtrickle.refresh_stream_table('typed_join_st')",
        ])
        .await;
        let differential: bool = sqlx::query_scalar("SELECT h.refresh_id > $1 AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='typed_join_st' ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(previous).fetch_one(&db.pool).await.expect("inspect mixed-type aggregate refresh");
        assert!(
            differential,
            "mixed-type join must refresh differentially after {mutations:?}"
        );
        db.assert_st_matches_query("typed_join_st", oracle).await;
    }
}

#[tokio::test]
async fn test_vector_aggregate_cast_uses_cast_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE typed_casts (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO typed_casts VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let query = "SELECT avg(embedding::halfvec(3)) AS centroid FROM typed_casts";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('typed_cast_st', $${query}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='typed_cast_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "halfvec(3)");
    for mutation in [
        "UPDATE typed_casts SET embedding = '[0,0,1]' WHERE id = 2",
        "INSERT INTO typed_casts VALUES (3, '[1,1,0]')",
        "UPDATE typed_casts SET embedding = NULL WHERE id = 1",
        "UPDATE typed_casts SET embedding = '[1,0,1]' WHERE id = 1",
        "DELETE FROM typed_casts WHERE id = 2",
    ] {
        db.execute(mutation).await;
        let previous: i64 = db.query_scalar("SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='typed_cast_st'").await;
        db.execute_seq(&[
            "SET pg_trickle.enable_vector_agg = on",
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
            "SELECT pgtrickle.refresh_stream_table('typed_cast_st')",
        ])
        .await;
        let differential: bool = sqlx::query_scalar("SELECT h.refresh_id > $1 AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='typed_cast_st' ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(previous).fetch_one(&db.pool).await.expect("inspect cast aggregate refresh");
        assert!(
            differential,
            "cast aggregate must refresh differentially after {mutation}"
        );
        db.assert_st_matches_query(
            "typed_cast_st",
            "SELECT avg(embedding::halfvec(3))::halfvec(3) AS centroid FROM typed_casts",
        )
        .await;
    }
}

#[tokio::test]
async fn test_vector_aggregate_join_uses_each_bound_dimension() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE dims_three (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE dims_two (id int PRIMARY KEY, embedding vector(2))")
        .await;
    db.execute("INSERT INTO dims_three VALUES (1, '[1,0,0]')")
        .await;
    db.execute("INSERT INTO dims_two VALUES (1, '[1,0]')").await;
    let create = "SELECT pgtrickle.create_stream_table('dims_st', $$SELECT avg(a.embedding) AS three, avg(b.embedding) AS two FROM dims_three a JOIN dims_two b USING(id)$$, '1m', 'DIFFERENTIAL')";
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", create])
        .await;
    let types: Vec<(String, String)> = sqlx::query_as("SELECT attname::text, format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='dims_st'::regclass AND attname IN ('three','two') ORDER BY attname").fetch_all(&db.pool).await.expect("read dimensioned output types");
    assert_eq!(
        types,
        vec![
            ("three".into(), "vector(3)".into()),
            ("two".into(), "vector(2)".into())
        ]
    );
    let oracle = "SELECT avg(a.embedding)::vector(3) AS three, avg(b.embedding)::vector(2) AS two FROM dims_three a JOIN dims_two b USING(id)";
    for mutations in [
        vec!["UPDATE dims_two SET embedding='[0,1]' WHERE id=1"],
        vec![
            "INSERT INTO dims_three VALUES (2, '[0,1,0]')",
            "INSERT INTO dims_two VALUES (2, '[1,1]')",
        ],
        vec!["UPDATE dims_three SET embedding=NULL WHERE id=1"],
        vec!["UPDATE dims_three SET embedding='[0,0,1]' WHERE id=1"],
        vec!["DELETE FROM dims_two WHERE id=2"],
    ] {
        db.execute_seq(&mutations).await;
        let previous: i64 = db.query_scalar("SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='dims_st'").await;
        db.execute_seq(&[
            "SET pg_trickle.enable_vector_agg = on",
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
            "SELECT pgtrickle.refresh_stream_table('dims_st')",
        ])
        .await;
        let differential: bool = sqlx::query_scalar("SELECT h.refresh_id > $1 AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='dims_st' ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(previous).fetch_one(&db.pool).await.expect("inspect dimensioned join refresh");
        assert!(
            differential,
            "dimensioned join must refresh differentially after {mutations:?}"
        );
        db.assert_st_matches_query("dims_st", oracle).await;
    }
}

#[tokio::test]
async fn test_vector_aggregate_normalized_expression_keeps_unknown_dimension() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE normalized_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO normalized_source VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let create = "SELECT pgtrickle.create_stream_table('normalized_st', $$SELECT avg(l2_normalize(embedding)) AS centroid FROM normalized_source$$, '1m', 'DIFFERENTIAL')";
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", create])
        .await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='normalized_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "vector");
    for mutation in [
        "UPDATE normalized_source SET embedding='[0,0,1]' WHERE id=2",
        "INSERT INTO normalized_source VALUES (3, '[1,1,0]')",
        "UPDATE normalized_source SET embedding=NULL WHERE id=1",
        "UPDATE normalized_source SET embedding='[1,0,1]' WHERE id=1",
        "DELETE FROM normalized_source WHERE id=2",
    ] {
        db.execute(mutation).await;
        let previous: i64 = db.query_scalar("SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='normalized_st'").await;
        db.execute_seq(&[
            "SET pg_trickle.enable_vector_agg = on",
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
            "SELECT pgtrickle.refresh_stream_table('normalized_st')",
        ])
        .await;
        let differential: bool = sqlx::query_scalar("SELECT h.refresh_id > $1 AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='normalized_st' ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(previous).fetch_one(&db.pool).await.expect("inspect normalized aggregate refresh");
        assert!(
            differential,
            "normalized aggregate must refresh differentially after {mutation}"
        );
        db.assert_st_matches_query(
            "normalized_st",
            "SELECT avg(l2_normalize(embedding)) AS centroid FROM normalized_source",
        )
        .await;
    }
}

#[tokio::test]
async fn test_vector_aggregate_immediate_uses_analyzed_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE immediate_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO immediate_vectors VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", "SELECT pgtrickle.create_stream_table('immediate_vectors_st', $$SELECT avg(embedding::halfvec(3)) AS centroid FROM immediate_vectors$$, NULL, 'IMMEDIATE')", "UPDATE immediate_vectors SET embedding='[0,1,0]' WHERE id=1"]).await;
    db.assert_st_matches_query(
        "immediate_vectors_st",
        "SELECT avg(embedding::halfvec(3))::halfvec(3) AS centroid FROM immediate_vectors",
    )
    .await;
}

#[tokio::test]
async fn test_vector_aggregate_nonpublic_extension_schema() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"Vec.X\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"Vec.X\"")
        .await;
    db.execute("CREATE TABLE private_vectors (id int PRIMARY KEY, embedding \"Vec.X\".vector(3))")
        .await;
    db.execute("INSERT INTO private_vectors VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&["SET search_path = public, \"Vec.X\"", "SET pg_trickle.enable_vector_agg = on", "SELECT pgtrickle.create_stream_table('private_vector_st', $$SELECT avg(embedding) AS centroid FROM private_vectors$$, '1m', 'DIFFERENTIAL')"]).await;
    let type_matches: bool = db.query_scalar("SELECT atttypid='\"Vec.X\".vector'::regtype AND atttypmod=3 FROM pg_attribute WHERE attrelid='private_vector_st'::regclass AND attname='centroid'").await;
    assert!(type_matches);
    let mut owner_session = db.pool.begin().await.expect("begin vector-schema check");
    sqlx::query("SET LOCAL search_path = public, \"Vec.X\"")
        .execute(&mut *owner_session)
        .await
        .expect("set vector search path");
    let initialization: Option<String> = sqlx::query_scalar("SELECT pgtrickle._centroid_initialization_sql('SELECT avg(embedding) AS centroid FROM private_vectors')")
        .fetch_one(&mut *owner_session).await.expect("generate nonpublic initialization");
    let initialization = initialization.expect("nonpublic vector aggregate is admitted");
    let sql = format!(
        "SELECT centroid::real[], __pgt_group_count, __pgt_centroid_state FROM ({initialization}) AS initialized"
    );
    let (centroid, count, state): (Vec<f32>, i64, Vec<f64>) =
        sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
            .fetch_one(&mut *owner_session)
            .await
            .expect("execute nonpublic initialization");
    assert_eq!(
        (centroid, count, state),
        (
            vec![1.0, 0.0, 0.0],
            1,
            vec![1.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0]
        )
    );
    owner_session
        .rollback()
        .await
        .expect("end vector-schema check");
    for mutation in [
        "UPDATE private_vectors SET embedding='[0,1,0]' WHERE id=1",
        "INSERT INTO private_vectors VALUES (2, '[1,1,0]')",
        "UPDATE private_vectors SET embedding=NULL WHERE id=1",
        "UPDATE private_vectors SET embedding='[1,0,1]' WHERE id=1",
        "DELETE FROM private_vectors WHERE id=2",
    ] {
        db.execute(mutation).await;
        let previous: i64 = db.query_scalar("SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='private_vector_st'").await;
        db.execute_seq(&[
            "SET search_path = public, \"Vec.X\"",
            "SET pg_trickle.enable_vector_agg = on",
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
            "SELECT pgtrickle.refresh_stream_table('private_vector_st')",
        ])
        .await;
        let differential: bool = sqlx::query_scalar("SELECT h.refresh_id > $1 AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='private_vector_st' ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(previous).fetch_one(&db.pool).await.expect("inspect nonpublic aggregate refresh");
        assert!(
            differential,
            "nonpublic aggregate must refresh differentially after {mutation}"
        );
        let same: bool = db.query_scalar("SELECT centroid::text IS NOT DISTINCT FROM (SELECT \"Vec.X\".avg(embedding)::text FROM private_vectors) FROM private_vector_st").await;
        assert!(same, "nonpublic aggregate differs after {mutation}");
    }
}

#[tokio::test]
async fn test_vector_projection_retains_typmod_for_direct_ann_index() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE projected_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO projected_vectors VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('projected_vector_st', $$SELECT id, embedding FROM projected_vectors$$, '1m', 'DIFFERENTIAL')").await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='projected_vector_st'::regclass AND attname='embedding'").await;
    assert_eq!(typ, "vector(3)");
    db.execute("CREATE INDEX projected_vector_ann ON projected_vector_st USING hnsw (embedding vector_l2_ops)").await;
    let mut tx = db.pool.begin().await.expect("begin plan check");
    sqlx::query("SET LOCAL enable_seqscan=off")
        .execute(&mut *tx)
        .await
        .expect("disable sequential scan");
    let plan: serde_json::Value = sqlx::query_scalar("EXPLAIN (FORMAT JSON) SELECT id FROM projected_vector_st ORDER BY embedding <-> '[1,0,0]'::vector LIMIT 1").fetch_one(&mut *tx).await.expect("explain vector search");
    assert!(
        plan.to_string().contains("projected_vector_ann"),
        "direct index is not eligible: {plan}"
    );
    tx.rollback().await.expect("end plan check");
}

#[tokio::test]
async fn test_vector_aggregate_cte_alias_and_group_key_changes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE cte_vectors (id int PRIMARY KEY, grp int, embedding vector(3))")
        .await;
    db.execute("INSERT INTO cte_vectors VALUES (1, 1, '[1,0,0]'), (2, 2, '[0,1,0]')")
        .await;
    let query = "WITH c AS (SELECT id, grp, embedding AS vec FROM cte_vectors) SELECT grp, avg(vec) AS centroid FROM c GROUP BY grp";
    let create = format!(
        "SELECT pgtrickle.create_stream_table('cte_vectors_st', $${query}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create])
        .await;
    let typ: String = db.query_scalar("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid='cte_vectors_st'::regclass AND attname='centroid'").await;
    assert_eq!(typ, "vector(3)");
    for mutation in [
        "UPDATE cte_vectors SET grp=2, embedding='[0,0,1]' WHERE id=1",
        "INSERT INTO cte_vectors VALUES (3, 1, '[1,1,0]')",
        "UPDATE cte_vectors SET embedding=NULL WHERE id=2",
        "UPDATE cte_vectors SET embedding='[1,0,1]' WHERE id=2",
        "DELETE FROM cte_vectors WHERE id=1",
    ] {
        db.execute(mutation).await;
        let previous: i64 = db.query_scalar("SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='cte_vectors_st'").await;
        db.execute_seq(&[
            "SET pg_trickle.enable_vector_agg = on",
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
            "SELECT pgtrickle.refresh_stream_table('cte_vectors_st')",
        ])
        .await;
        let differential: bool = sqlx::query_scalar("SELECT h.refresh_id > $1 AND h.action='DIFFERENTIAL' AND h.status='COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='cte_vectors_st' ORDER BY h.refresh_id DESC LIMIT 1")
            .bind(previous).fetch_one(&db.pool).await.expect("inspect CTE aggregate refresh");
        assert!(
            differential,
            "CTE aggregate must refresh differentially after {mutation}"
        );
        db.assert_st_matches_query("cte_vectors_st", "WITH c AS (SELECT id, grp, embedding AS vec FROM cte_vectors) SELECT grp, avg(vec)::vector(3) AS centroid FROM c GROUP BY grp").await;
    }
}

#[tokio::test]
async fn test_vector_aggregate_disabled_reports_actionable_error() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE disabled_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    let error = db.try_execute_with_config(&["SET pg_trickle.enable_vector_agg = off"], "SELECT pgtrickle.create_stream_table('disabled_vector_st', $$SELECT avg(embedding) AS centroid FROM disabled_vectors$$, '1m', 'DIFFERENTIAL')").await.expect_err("vector aggregate should require opt-in");
    assert!(
        error.to_string().contains("enable_vector_agg"),
        "unexpected error: {error}"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = off", "SELECT pgtrickle.create_stream_table('disabled_auto_st', $$SELECT avg(embedding) AS centroid FROM disabled_vectors$$, '1m', 'AUTO')"]).await;
    let mode: String = db.query_scalar("SELECT refresh_mode::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name='disabled_auto_st'").await;
    assert_eq!(mode, "FULL");
}

#[tokio::test]
async fn test_vector_aggregate_alter_query_keeps_expression_index_usable() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alter_vectors (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alter_vectors VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", "SELECT pgtrickle.create_stream_table('alter_vectors_st', $$SELECT avg(embedding) AS centroid FROM alter_vectors$$, '1m', 'DIFFERENTIAL')"]).await;
    db.execute("CREATE INDEX alter_vectors_ann ON alter_vectors_st USING hnsw ((l2_normalize(centroid)::vector(3)) vector_l2_ops)").await;
    let alter = "SELECT pgtrickle.alter_stream_table('alter_vectors_st', query => $$SELECT avg(embedding) AS centroid FROM alter_vectors WHERE id > 0$$)";
    let error = db
        .try_execute_with_config(&["SET pg_trickle.enable_vector_agg = on"], alter)
        .await
        .expect_err("ALTER must not silently drop the expression index");
    assert!(
        error
            .to_string()
            .contains("expression or ANN index alter_vectors_ann"),
        "unexpected error: {error}"
    );
    let usable: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index WHERE indexrelid=to_regclass('alter_vectors_ann') AND indisvalid AND indisready)").await;
    assert!(usable, "ALTER QUERY must retain a usable expression index");
    db.assert_st_matches_query(
        "alter_vectors_st",
        "SELECT avg(embedding)::vector(3) AS centroid FROM alter_vectors",
    )
    .await;
    db.execute("DROP INDEX alter_vectors_ann").await;
    db.execute("CREATE INDEX alter_vectors_ann_plain ON alter_vectors_st USING hnsw (centroid vector_l2_ops)").await;
    let error = db
        .try_execute_with_config(&["SET pg_trickle.enable_vector_agg = on"], alter)
        .await
        .expect_err("ALTER must not silently drop a plain ANN index");
    assert!(
        error.to_string().contains("alter_vectors_ann_plain"),
        "unexpected error: {error}"
    );
    db.execute("DROP INDEX alter_vectors_ann_plain").await;
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", alter])
        .await;
    db.execute("CREATE INDEX alter_vectors_ann ON alter_vectors_st USING hnsw ((l2_normalize(centroid)::vector(3)) vector_l2_ops)").await;
    db.assert_st_matches_query(
        "alter_vectors_st",
        "SELECT avg(embedding)::vector(3) AS centroid FROM alter_vectors WHERE id > 0",
    )
    .await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-1: User-taste centroid — avg(embedding) per user
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_avg_centroid_insert() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE embeddings (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO embeddings (user_id, embedding) VALUES
            (1, '[1,0,0]'),
            (1, '[0,1,0]'),
            (2, '[0,0,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM embeddings GROUP BY user_id";
    // pgvector's avg(vector) is undimensioned; F4 materializes the source dimension.
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM embeddings GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_st", expected_q).await;

    // INSERT new embedding for user 1
    db.execute("INSERT INTO embeddings (user_id, embedding) VALUES (1, '[1,1,0]')")
        .await;
    db.refresh_st("centroid_st").await;
    db.assert_st_matches_query("centroid_st", expected_q).await;

    // INSERT new user
    db.execute("INSERT INTO embeddings (user_id, embedding) VALUES (3, '[0,1,1]')")
        .await;
    db.refresh_st("centroid_st").await;
    db.assert_st_matches_query("centroid_st", expected_q).await;
}

#[tokio::test]
async fn test_pgvector_avg_centroid_update() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_upd (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_upd (user_id, embedding) VALUES
            (1, '[1,0,0]'),
            (1, '[0,1,0]'),
            (2, '[1,1,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM emb_upd GROUP BY user_id";
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM emb_upd GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_upd_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_upd_st", expected_q)
        .await;

    // UPDATE embedding
    db.execute("UPDATE emb_upd SET embedding = '[0,0,1]' WHERE user_id = 1 AND id = 1")
        .await;
    db.refresh_st("centroid_upd_st").await;
    db.assert_st_matches_query("centroid_upd_st", expected_q)
        .await;
}

#[tokio::test]
async fn test_pgvector_avg_centroid_delete() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_del (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_del (user_id, embedding) VALUES
            (1, '[1,0,0]'),
            (1, '[0,1,0]'),
            (2, '[1,1,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM emb_del GROUP BY user_id";
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM emb_del GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_del_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_del_st", expected_q)
        .await;

    // DELETE one row from user 1
    db.execute("DELETE FROM emb_del WHERE user_id = 1 AND id = 1")
        .await;
    db.refresh_st("centroid_del_st").await;
    db.assert_st_matches_query("centroid_del_st", expected_q)
        .await;

    // DELETE entire user group
    db.execute("DELETE FROM emb_del WHERE user_id = 2").await;
    db.refresh_st("centroid_del_st").await;
    db.assert_st_matches_query("centroid_del_st", expected_q)
        .await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-2: vector_sum — sum(embedding) per group
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_sum_differential() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_sum_src (
            id SERIAL PRIMARY KEY,
            grp TEXT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_sum_src (grp, embedding) VALUES
            ('a', '[1,0,0]'),
            ('a', '[0,1,0]'),
            ('b', '[1,1,1]')",
    )
    .await;

    let q = "SELECT grp, sum(embedding) AS total_vec FROM emb_sum_src GROUP BY grp";
    let expected_q =
        "SELECT grp, sum(embedding)::vector(3) AS total_vec FROM emb_sum_src GROUP BY grp";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('vec_sum_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("vec_sum_st", expected_q).await;

    // INSERT
    db.execute("INSERT INTO emb_sum_src (grp, embedding) VALUES ('a', '[0,0,1]')")
        .await;
    db.refresh_st("vec_sum_st").await;
    db.assert_st_matches_query("vec_sum_st", expected_q).await;

    // DELETE
    db.execute("DELETE FROM emb_sum_src WHERE grp = 'b'").await;
    db.refresh_st("vec_sum_st").await;
    db.assert_st_matches_query("vec_sum_st", expected_q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-3: Explicit FULL refresh for a distance-ordered Top-K query
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_distance_topk_explicit_full() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_dist (
            id SERIAL PRIMARY KEY,
            category TEXT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_dist (category, embedding) VALUES
            ('cat1', '[1,0,0]'),
            ('cat1', '[0.9,0.1,0]'),
            ('cat2', '[0,1,0]')",
    )
    .await;

    // This exercises explicit FULL mode, not automatic fallback.
    let q = "SELECT id, category, embedding FROM emb_dist ORDER BY embedding <-> '[1,0,0]' LIMIT 5";
    db.create_st("dist_st", q, "1m", "FULL").await;
    db.assert_st_matches_query("dist_st", q).await;

    // Refresh after insert
    db.execute("INSERT INTO emb_dist (category, embedding) VALUES ('cat1', '[0.8,0.2,0]')")
        .await;
    db.refresh_st("dist_st").await;
    db.assert_st_matches_query("dist_st", q).await;
}

#[tokio::test]
async fn test_pgvector_row_local_distance_differential() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE distance_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO distance_src VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let query = "SELECT id, embedding <=> '[1,0,0]'::vector AS distance \
                 FROM distance_src WHERE embedding <-> '[1,0,0]'::vector < 1";
    db.create_st("distance_local_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.create_st("distance_auto_st", query, "1h", "AUTO").await;
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('distance_immediate_st', $${query}$$, refresh_mode => 'IMMEDIATE')"
    )).await;

    for mutation in [
        "INSERT INTO distance_src VALUES (3, '[0.9,0.1,0]')",
        "UPDATE distance_src SET embedding = '[0,1,0]' WHERE id = 1",
        "UPDATE distance_src SET embedding = '[1,0,0]' WHERE id = 2",
        "DELETE FROM distance_src WHERE id = 3",
    ] {
        db.execute(mutation).await;
        db.assert_st_matches_query("distance_immediate_st", query)
            .await;
        for name in ["distance_local_st", "distance_auto_st"] {
            let previous_id: i64 = db
                .query_scalar(&format!(
                    "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = '{name}'"
                ))
                .await;
            db.try_execute_with_config(
                &[
                    "SET pg_trickle.refresh_strategy = 'differential'",
                    "SET pg_trickle.differential_max_change_ratio = 0",
                ],
                &format!("SELECT pgtrickle.refresh_stream_table('{name}')"),
            )
            .await
            .expect("refresh row-local vector query");
            let differential: bool = db.query_scalar(&format!(
                "SELECT h.refresh_id > {previous_id} AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
                 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                 WHERE s.pgt_name = '{name}' ORDER BY h.refresh_id DESC LIMIT 1"
            )).await;
            assert!(differential, "{name}: no completed differential refresh");
            db.assert_st_matches_query(name, query).await;
        }
    }
    db.execute_seq(&[
        "BEGIN",
        "INSERT INTO distance_src VALUES (10, '[1,0,0]')",
        "ROLLBACK",
    ])
    .await;
    for name in [
        "distance_local_st",
        "distance_auto_st",
        "distance_immediate_st",
    ] {
        db.assert_st_matches_query(name, query).await;
    }
}

#[tokio::test]
async fn test_pgvector_row_local_distance_refresh_avoids_source_scan() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute_seq(&[
        "CREATE TABLE distance_scan_src (id int PRIMARY KEY, embedding vector(3))",
        "INSERT INTO distance_scan_src SELECT i, '[1,0,0]'::vector FROM generate_series(1, 1000) i",
        "SELECT pgtrickle.create_stream_table('distance_scan_st', $$SELECT id, embedding <=> '[1,0,0]'::vector AS distance FROM distance_scan_src WHERE embedding <-> '[1,0,0]'::vector < 1$$, '1h', 'DIFFERENTIAL')",
        "SELECT pgtrickle.pause_scheduler(ARRAY['public.distance_scan_st'])",
        "UPDATE distance_scan_src SET embedding = '[0,1,0]' WHERE id = 1",
    ]).await;
    let mut tx = db.pool.begin().await.expect("begin measured refresh");
    for setting in [
        "SET LOCAL pg_trickle.refresh_strategy = 'differential'",
        "SET LOCAL pg_trickle.differential_max_change_ratio = 0",
    ] {
        sqlx::query(setting)
            .execute(&mut *tx)
            .await
            .expect("configure refresh");
    }
    let counters = "SELECT pg_stat_get_xact_tuples_returned('distance_scan_src'::regclass)::bigint, pg_stat_get_xact_tuples_fetched('distance_scan_src'::regclass)::bigint";
    let before: (i64, i64) = sqlx::query_as(counters)
        .fetch_one(&mut *tx)
        .await
        .expect("read initial source counters");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('distance_scan_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh row-local distance");
    let after: (i64, i64) = sqlx::query_as(counters)
        .fetch_one(&mut *tx)
        .await
        .expect("read final source counters");
    assert!(
        after.0 - before.0 + after.1 - before.1 <= 10,
        "one changed row must not read unrelated source history: before={before:?}, after={after:?}"
    );
    tx.commit().await.expect("commit measured refresh");
    let differential: bool = db.query_scalar("SELECT h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'distance_scan_st' ORDER BY h.refresh_id DESC LIMIT 1").await;
    assert!(differential, "refresh must use the differential path");
    db.assert_st_matches_query("distance_scan_st", "SELECT id, embedding <=> '[1,0,0]'::vector AS distance FROM distance_scan_src WHERE embedding <-> '[1,0,0]'::vector < 1").await;
}

#[tokio::test]
async fn test_pgvector_distance_topk_recomputes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE distance_rank_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute(
        "INSERT INTO distance_rank_src VALUES (1, '[1,0,0]'), (2, '[0,1,0]'), (3, '[0,0,1]')",
    )
    .await;
    let query = "SELECT id, embedding FROM distance_rank_src \
                 ORDER BY embedding <-> '[1,0,0]'::vector, id LIMIT 2";
    db.create_st("distance_rank_st", query, "1h", "DIFFERENTIAL")
        .await;

    for mutation in [
        "INSERT INTO distance_rank_src VALUES (4, '[0.9,0,0]')",
        "UPDATE distance_rank_src SET embedding = '[0.8,0,0]' WHERE id = 3",
        "DELETE FROM distance_rank_src WHERE id = 1",
        "DELETE FROM distance_rank_src WHERE id IN (2, 3, 4)",
    ] {
        db.execute(mutation).await;
        let previous_id: i64 = db.query_scalar(
            "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'distance_rank_st'"
        ).await;
        db.refresh_st("distance_rank_st").await;
        let recomputed: bool = db.query_scalar(&format!(
            "SELECT h.refresh_id > {previous_id} AND h.status = 'COMPLETED' AND h.merge_strategy_used = 'TOP_K' \
             FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
             WHERE s.pgt_name = 'distance_rank_st' ORDER BY h.refresh_id DESC LIMIT 1"
        )).await;
        assert!(
            recomputed,
            "Top-K refresh was not recorded as recomputation"
        );
        db.assert_st_matches_query("distance_rank_st", query).await;
    }
}

#[tokio::test]
async fn test_pgvector_distance_types_and_functions_differential() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    for (kind, first, second, zero) in [
        ("vector", "[1,0,0]", "[0,1,0]", "[0,0,0]"),
        ("halfvec", "[1,0,0]", "[0,1,0]", "[0,0,0]"),
        ("sparsevec", "{1:1}/3", "{2:1}/3", "{}/3"),
    ] {
        let source = format!("distance_{kind}_src");
        let stream = format!("distance_{kind}_st");
        db.execute(&format!(
            "CREATE TABLE {source} (id int PRIMARY KEY, embedding {kind}(3))"
        ))
        .await;
        db.execute(&format!(
            "INSERT INTO {source} VALUES (1, '{first}'), (2, '{zero}'), (3, NULL)"
        ))
        .await;
        let pivot = format!("'{first}'::{kind}");
        let query = format!(
            "SELECT id, embedding <-> {pivot} AS l2, embedding <=> {pivot} AS cosine, \
             embedding <#> {pivot} AS neg_ip, embedding <+> {pivot} AS l1, \
             l2_distance(embedding, {pivot}) AS l2_fn, \
             cosine_distance(embedding, {pivot}) AS cosine_fn, \
             inner_product(embedding, {pivot}) AS ip_fn, \
             l1_distance(embedding, {pivot}) AS l1_fn FROM {source}"
        );
        db.create_st(&stream, &query, "1h", "DIFFERENTIAL").await;
        db.assert_st_matches_query(&stream, &query).await;

        for mutation in [
            format!("INSERT INTO {source} VALUES (4, '{second}')"),
            format!("UPDATE {source} SET embedding = '{second}' WHERE id IN (1, 3)"),
            format!("DELETE FROM {source} WHERE id = 2"),
        ] {
            db.execute(&mutation).await;
            let previous_id: i64 = db
                .query_scalar(&format!(
                    "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = '{stream}'"
                ))
                .await;
            db.try_execute_with_config(
                &[
                    "SET pg_trickle.refresh_strategy = 'differential'",
                    "SET pg_trickle.differential_max_change_ratio = 0",
                ],
                &format!("SELECT pgtrickle.refresh_stream_table('{stream}')"),
            )
            .await
            .expect("refresh vector distance expressions");
            let differential: bool = db.query_scalar(&format!(
                "SELECT h.refresh_id > {previous_id} AND h.action = 'DIFFERENTIAL' \
                 AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
                 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                 WHERE s.pgt_name = '{stream}' ORDER BY h.refresh_id DESC LIMIT 1"
            )).await;
            assert!(differential, "{kind}: no completed differential refresh");
            db.assert_st_matches_query(&stream, &query).await;
        }
    }
}

#[tokio::test]
async fn test_pgvector_distance_changing_pivot_differential() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE distance_docs (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE distance_pivot (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO distance_docs VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.execute("INSERT INTO distance_pivot VALUES (1, '[1,0,0]')")
        .await;
    let query = "SELECT d.id, d.embedding <-> p.embedding AS distance \
                 FROM distance_docs d CROSS JOIN distance_pivot p WHERE p.id = 1";
    db.create_st("distance_pivot_st", query, "1h", "DIFFERENTIAL")
        .await;
    db.execute("UPDATE distance_pivot SET embedding = '[0,1,0]' WHERE id = 1")
        .await;
    let previous_id: i64 = db
        .query_scalar(
            "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'distance_pivot_st'",
        )
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ],
        "SELECT pgtrickle.refresh_stream_table('distance_pivot_st')",
    )
    .await
    .expect("refresh after changing the pivot");
    let differential: bool = db
        .query_scalar(&format!(
            "SELECT h.refresh_id > {previous_id} AND h.action = 'DIFFERENTIAL' \
         AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
         FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
         WHERE s.pgt_name = 'distance_pivot_st' ORDER BY h.refresh_id DESC LIMIT 1"
        ))
        .await;
    assert!(differential, "pivot change did not refresh differentially");
    db.assert_st_matches_query("distance_pivot_st", query).await;
}

#[tokio::test]
async fn test_pgvector_distance_non_public_extension_schema() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"Vector.Ext\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"Vector.Ext\"")
        .await;
    db.execute(
        "CREATE TABLE schema_distance_src (id int PRIMARY KEY, embedding \"Vector.Ext\".vector(3), pivot \"Vector.Ext\".vector(3))",
    )
    .await;
    db.execute("INSERT INTO schema_distance_src VALUES (1, '[1,0,0]', '[1,0,0]')")
        .await;
    let query = "SELECT id, embedding OPERATOR(\"Vector.Ext\".<=>) pivot AS distance \
                 FROM schema_distance_src";
    db.create_st("schema_distance_st", query, "1h", "DIFFERENTIAL")
        .await;
    let previous_id: i64 = db
        .query_scalar(
            "SELECT COALESCE(MAX(h.refresh_id), 0) FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'schema_distance_st'",
        )
        .await;
    db.execute("UPDATE schema_distance_src SET embedding = '[0,1,0]' WHERE id = 1")
        .await;
    db.try_execute_with_config(
        &[
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ],
        "SELECT pgtrickle.refresh_stream_table('schema_distance_st')",
    )
    .await
    .expect("refresh schema-qualified vector distance");
    let differential: bool = db.query_scalar(&format!(
        "SELECT h.refresh_id > {previous_id} AND h.action = 'DIFFERENTIAL' AND h.status = 'COMPLETED' AND NOT h.was_full_fallback \
         FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
         WHERE s.pgt_name = 'schema_distance_st' ORDER BY h.refresh_id DESC LIMIT 1"
    )).await;
    assert!(differential, "schema-qualified distance fell back to FULL");
    db.assert_st_matches_query("schema_distance_st", query)
        .await;
}

// ═══════════════════════════════════════════════════════════════════════
// F4-4: HNSW index on centroid stream table
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_pgvector_hnsw_index_on_stream_table() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE emb_hnsw_src (
            id SERIAL PRIMARY KEY,
            user_id INT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO emb_hnsw_src (user_id, embedding) VALUES
            (1, '[1,0,0]'), (1, '[0.9,0.1,0]'),
            (2, '[0,1,0]'), (2, '[0,0.9,0.1]'),
            (3, '[0,0,1]')",
    )
    .await;

    let q = "SELECT user_id, avg(embedding) AS centroid FROM emb_hnsw_src GROUP BY user_id";
    let expected_q =
        "SELECT user_id, avg(embedding)::vector(3) AS centroid FROM emb_hnsw_src GROUP BY user_id";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('centroid_hnsw_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;
    db.assert_st_matches_query("centroid_hnsw_st", expected_q)
        .await;

    // Create HNSW index on the centroid stream table
    db.execute(
        "CREATE INDEX centroid_hnsw_idx ON public.centroid_hnsw_st \
         USING hnsw (centroid vector_cosine_ops)",
    )
    .await;

    // Verify refresh still works with index in place
    db.execute("INSERT INTO emb_hnsw_src (user_id, embedding) VALUES (1, '[1,0,0]')")
        .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SELECT pgtrickle.refresh_stream_table('centroid_hnsw_st')",
    ])
    .await;
    db.assert_st_matches_query("centroid_hnsw_st", expected_q)
        .await;

    let index_ready: bool = db
        .query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_index i \
         JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_am am ON am.oid = c.relam \
         JOIN pg_opclass op ON op.oid = i.indclass[0] \
         WHERE i.indexrelid = 'public.centroid_hnsw_idx'::regclass \
         AND i.indrelid = 'public.centroid_hnsw_st'::regclass \
         AND i.indisvalid AND i.indisready AND am.amname = 'hnsw' \
         AND op.opcname = 'vector_cosine_ops')",
        )
        .await;
    assert!(index_ready, "expected a valid, ready cosine HNSW index");

    // Test index eligibility separately from the planner's cost-based choice.
    let mut tx = db.pool.begin().await.expect("begin plan transaction");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .expect("discourage sequential scan");
    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT user_id FROM public.centroid_hnsw_st ORDER BY centroid <=> '[1,0,0]'::vector LIMIT 1",
    )
    .fetch_all(&mut *tx)
    .await
    .expect("explain cosine ANN query");
    assert!(
        plan.iter().any(|line| line.contains("centroid_hnsw_idx")),
        "cosine query must be eligible for the HNSW index: {plan:?}"
    );
    let wrong_metric_plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT user_id FROM public.centroid_hnsw_st ORDER BY centroid <-> '[1,0,0]'::vector LIMIT 1",
    )
    .fetch_all(&mut *tx)
    .await
    .expect("explain L2 query");
    assert!(
        wrong_metric_plan
            .iter()
            .all(|line| !line.contains("centroid_hnsw_idx")),
        "L2 ordering must not use the cosine HNSW index: {wrong_metric_plan:?}"
    );
    tx.rollback().await.expect("end plan transaction");

    // Verify the matching search returns the nearest centroid.
    let nn_result = db
        .query_scalar_opt::<i32>("SELECT user_id FROM public.centroid_hnsw_st ORDER BY centroid <=> '[1,0,0]'::vector LIMIT 1")
        .await;
    assert_eq!(nn_result, Some(1));
}

// ═══════════════════════════════════════════════════════════════════════
// T-VP1 (v0.47.0): Drift-based reindex on HNSW stream table
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_post_refresh_reindex_failure_does_not_rollback_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE maintenance_fail_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "maintenance_fail_st",
        "SELECT id, embedding FROM maintenance_fail_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX maintenance_fail_hnsw ON maintenance_fail_st USING hnsw (embedding vector_l2_ops)").await;
    db.execute("SELECT pgtrickle.alter_stream_table('maintenance_fail_st', post_refresh_action => 'reindex')").await;
    db.execute("CREATE FUNCTION fail_reindex() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected reindex failure'; END $$").await;
    db.execute("CREATE EVENT TRIGGER fail_reindex ON ddl_command_start WHEN TAG IN ('REINDEX') EXECUTE FUNCTION fail_reindex()").await;
    db.execute("INSERT INTO maintenance_fail_src VALUES (1,'[1,0,0]')")
        .await;
    let notices = db
        .try_execute_with_notices("SELECT pgtrickle.refresh_stream_table('maintenance_fail_st')")
        .await
        .expect("maintenance failure must not roll back refresh");
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains("injected reindex failure")),
        "maintenance failure must be observable: {notices:?}"
    );
    db.assert_st_matches_query(
        "maintenance_fail_st",
        "SELECT id, embedding FROM maintenance_fail_src",
    )
    .await;
    let changed: i64 = db
        .query_scalar("SELECT rows_changed_since_last_reindex FROM pgtrickle.pgt_stream_tables WHERE pgt_name='maintenance_fail_st'")
        .await;
    assert_eq!(
        changed, 1,
        "failed maintenance must retain the changed-row count"
    );
}

#[tokio::test]
async fn test_post_refresh_reindex_success_and_transaction_rollback() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE maintenance_success_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "maintenance_success_st",
        "SELECT id, embedding FROM maintenance_success_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX maintenance_success_hnsw ON maintenance_success_st USING hnsw (embedding vector_l2_ops)").await;
    db.execute("SELECT pgtrickle.alter_stream_table('maintenance_success_st', post_refresh_action => 'reindex')").await;
    let original: i64 = db
        .query_scalar("SELECT pg_relation_filenode('maintenance_success_hnsw'::regclass)::bigint")
        .await;
    db.execute("INSERT INTO maintenance_success_src VALUES (1,'[1,0,0]')")
        .await;
    let mut tx = db.pool.begin().await.expect("begin refresh transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('maintenance_success_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh and reindex");
    let rebuilt: i64 = sqlx::query_scalar(
        "SELECT pg_relation_filenode('maintenance_success_hnsw'::regclass)::bigint",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("read rebuilt index identity");
    assert_ne!(
        rebuilt, original,
        "successful maintenance must rebuild the index"
    );
    tx.rollback().await.expect("rollback refresh and reindex");
    let after_rollback: i64 = db
        .query_scalar("SELECT pg_relation_filenode('maintenance_success_hnsw'::regclass)::bigint")
        .await;
    assert_eq!(after_rollback, original);
    assert_eq!(db.count("public.maintenance_success_st").await, 0);
    db.refresh_st("maintenance_success_st").await;
    db.assert_st_matches_query(
        "maintenance_success_st",
        "SELECT id, embedding FROM maintenance_success_src",
    )
    .await;
    let state: (i64, bool) = sqlx::query_as("SELECT rows_changed_since_last_reindex,last_reindex_at IS NOT NULL FROM pgtrickle.pgt_stream_tables WHERE pgt_name='maintenance_success_st'").fetch_one(&db.pool).await.expect("inspect successful maintenance");
    assert_eq!(state, (0, true));
}

/// T-VP1: Create a vector stream table with post_refresh_action='reindex_if_drift',
/// change rows beyond the threshold, verify that the drift counter increments
/// and that last_reindex_at is updated after a refresh.
#[tokio::test]
async fn test_vector_post_refresh_action_reindex_if_drift() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE drift_src (
            id SERIAL PRIMARY KEY,
            embedding vector(3)
        )",
    )
    .await;

    // Insert initial rows
    for i in 1..=20i32 {
        db.execute(&format!(
            "INSERT INTO drift_src (embedding) VALUES ('[{},{},0]')",
            i, i
        ))
        .await;
    }

    // Create stream table
    let q = "SELECT id, embedding FROM drift_src";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('drift_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    ))
    .await;

    // Configure drift-triggered REINDEX (50% threshold so we can trigger it)
    db.execute(
        "SELECT pgtrickle.alter_stream_table('drift_st', \
         post_refresh_action => 'reindex_if_drift', \
         reindex_drift_threshold => 0.50)",
    )
    .await;

    // Add more rows to exceed 50% threshold (20 rows * 50% = 10, add 11)
    for i in 21..=31i32 {
        db.execute(&format!(
            "INSERT INTO drift_src (embedding) VALUES ('[{},{},1]')",
            i, i
        ))
        .await;
    }

    // Refresh — should increment rows_changed_since_last_reindex
    db.refresh_st("drift_st").await;

    // Check that vector_status() shows this stream table
    let drift_pct = db
        .query_scalar_opt::<f64>(
            "SELECT drift_pct FROM pgtrickle.vector_status() WHERE name = 'public.drift_st'",
        )
        .await;
    assert!(
        drift_pct.is_some(),
        "vector_status() should return a row for drift_st"
    );

    // Verify post_refresh_action is stored correctly
    let action = db
        .query_scalar_opt::<String>(
            "SELECT post_refresh_action FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'drift_st'",
        )
        .await
        .expect("post_refresh_action should be set");
    assert_eq!(
        action, "reindex_if_drift",
        "post_refresh_action should be 'reindex_if_drift'"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// T-VP2 (v0.47.0): vector_status() accuracy and reset behavior
// ═══════════════════════════════════════════════════════════════════════

/// T-VP2: Verify that pgtrickle.vector_status() reports correct lag, drift,
/// and metadata for vector stream tables. Also verify that after an alter to
/// 'none', the table disappears from vector_status().
#[tokio::test]
async fn test_vector_status_view_accuracy_and_reset() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE vs_src (
            id SERIAL PRIMARY KEY,
            body TEXT,
            embedding vector(3)
        )",
    )
    .await;

    db.execute(
        "INSERT INTO vs_src (body, embedding) VALUES
             ('hello', '[1,0,0]'),
             ('world', '[0,1,0]'),
             ('foo',   '[0,0,1]')",
    )
    .await;

    let q = "SELECT id, body, embedding FROM vs_src";
    db.execute(&format!(
        "SELECT pgtrickle.create_stream_table('vs_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    ))
    .await;

    // Set post_refresh_action to 'analyze'
    db.execute("SELECT pgtrickle.alter_stream_table('vs_st', post_refresh_action => 'analyze')")
        .await;

    // Do a refresh to trigger the action
    db.refresh_st("vs_st").await;

    // vector_status() should show vs_st
    let row = db
        .query_scalar_opt::<String>(
            "SELECT post_refresh_action FROM pgtrickle.vector_status() \
             WHERE name = 'public.vs_st'",
        )
        .await;
    assert!(
        row.is_some(),
        "vector_status() should include vs_st after setting post_refresh_action"
    );
    assert_eq!(
        row.unwrap(),
        "analyze",
        "post_refresh_action should be 'analyze'"
    );

    // data_timestamp should be set (not NULL)
    let ts = db
        .query_scalar_opt::<String>(
            "SELECT data_timestamp::TEXT FROM pgtrickle.vector_status() \
             WHERE name = 'public.vs_st'",
        )
        .await;
    assert!(
        ts.is_some(),
        "data_timestamp should not be NULL in vector_status()"
    );

    // Reset to 'none' — should disappear from vector_status()
    db.execute("SELECT pgtrickle.alter_stream_table('vs_st', post_refresh_action => 'none')")
        .await;

    let gone = db
        .query_scalar_opt::<String>(
            "SELECT post_refresh_action FROM pgtrickle.vector_status() \
             WHERE name = 'public.vs_st'",
        )
        .await;
    assert!(
        gone.is_none(),
        "vs_st should not appear in vector_status() after resetting to 'none'"
    );
}

#[tokio::test]
async fn test_vector_diagnostics_catalog_visibility_and_index_metadata() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute(
        "CREATE TABLE diagnostic_src (id int PRIMARY KEY, a vector(3), b vector(3), c vector)",
    )
    .await;
    db.execute("INSERT INTO diagnostic_src VALUES (1, '[1,0,0]', '[0,1,0]', '[1,1,0]')")
        .await;
    db.execute("SELECT pgtrickle.create_stream_table('diagnostic_st', $$SELECT id, a, b, c FROM diagnostic_src$$, '1m', 'DIFFERENTIAL')").await;
    db.execute("CREATE INDEX diagnostic_cosine ON diagnostic_st USING hnsw (a vector_cosine_ops) WHERE id > 0").await;
    db.execute("CREATE INDEX diagnostic_expression ON diagnostic_st USING hnsw ((c::vector(3)) vector_l2_ops)").await;
    db.execute("CREATE INDEX diagnostic_ivf ON diagnostic_st USING ivfflat ((c::vector(3)) vector_l2_ops) WITH (lists = 1)").await;

    let columns = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'post_refresh_action' = 'none' AND v->>'dimensions' = '3' AND v->'embedding_freshness' = 'null'::jsonb AND v->>'maintenance_pending' = 'false' AND v->>'maintenance_failed' = 'false' AND v->>'maintenance_stale' = 'false'").await;
    assert_eq!(columns, 2);
    let unknown = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'column' = 'c' AND v->'dimensions' = 'null'::jsonb AND v->'requested_index_state' = 'null'::jsonb AND v->>'requested_index_reason' IS NOT NULL").await;
    assert_eq!(unknown, 1);
    let indexes = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'valid' = 'true' AND v->>'ready' = 'true' AND v->>'dimensions' = '3' AND ((v->>'index' = 'diagnostic_cosine' AND v->>'access_method' = 'hnsw' AND v->>'opclass' = 'vector_cosine_ops' AND v->>'predicate' IS NOT NULL) OR (v->>'index' = 'diagnostic_expression' AND v->>'access_method' = 'hnsw' AND v->>'opclass' = 'vector_l2_ops') OR (v->>'index' = 'diagnostic_ivf' AND v->>'access_method' = 'ivfflat' AND v->>'opclass' = 'vector_l2_ops'))").await;
    assert_eq!(indexes, 3);
    let request: i64 = db.query_scalar("SELECT pgtrickle.request_ann_maintenance('diagnostic_st'::regclass::oid, 'diagnostic_cosine'::regclass::oid)").await;
    let pending: bool = db.query_scalar("SELECT (v->>'maintenance_pending')::boolean FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' LIMIT 1").await;
    assert!(pending);
    let mut worker = db.pool.acquire().await.expect("acquire worker");
    sqlx::query("SELECT pgtrickle.claim_ann_maintenance($1)")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("claim request");
    sqlx::query("SELECT pgtrickle.finish_ann_maintenance($1, false, 'test failure')")
        .bind(request)
        .execute(&mut *worker)
        .await
        .expect("fail request");
    let failed: bool = db.query_scalar("SELECT (v->>'maintenance_failed')::boolean AND v->>'maintenance_error' = 'test failure' AND v->>'maintenance_pending' = 'false' FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' LIMIT 1").await;
    assert!(failed);

    db.execute("CREATE ROLE diagnostic_observer").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO diagnostic_observer")
        .await;
    let mut conn = db.pool.acquire().await.expect("acquire connection");
    sqlx::query("SET ROLE diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("set role");
    let hidden: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtrickle.vector_stream_status_v2()")
            .fetch_one(&mut *conn)
            .await
            .expect("inspect as observer");
    assert_eq!(hidden, 0);
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset role");
    sqlx::query("GRANT SELECT ON diagnostic_st TO diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("grant select");
    sqlx::query("SET ROLE diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("set role");
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.ann_index_status_v2()")
        .fetch_one(&mut *conn)
        .await
        .expect("inspect as observer");
    assert_eq!(visible, 3);
    let redacted: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->'maintenance_pending' = 'null'::jsonb AND v->'maintenance_failed' = 'null'::jsonb AND v->'maintenance_stale' = 'null'::jsonb AND v->'maintenance_error' = 'null'::jsonb AND v->>'maintenance_reason' IS NOT NULL")
        .fetch_one(&mut *conn).await.expect("inspect redacted maintenance");
    assert_eq!(redacted, 3);
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset role");
    sqlx::query("GRANT MAINTAIN ON diagnostic_st TO diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("grant maintenance visibility");
    sqlx::query("SET ROLE diagnostic_observer")
        .execute(&mut *conn)
        .await
        .expect("set maintenance observer");
    let authorized: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_st' AND v->>'maintenance_failed' = 'true' AND v->>'maintenance_error' = 'test failure'")
        .fetch_one(&mut *conn).await.expect("inspect authorized maintenance");
    assert_eq!(authorized, 3);
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset role");
}

#[tokio::test]
async fn test_vector_diagnostics_idle_is_catalog_only_and_hides_query_vector() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE idle_vector_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO idle_vector_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "idle_vector_st",
        "SELECT id, embedding FROM idle_vector_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.pause_scheduler(ARRAY['public.idle_vector_st'])")
        .await;
    db.execute(
        "CREATE INDEX idle_vector_ann ON idle_vector_st USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('idle_vector_st', 'idle_alert', 'embedding', '[0.123,0.456,0.789]', '<->', 0.5)").await;
    db.execute("UPDATE pgtrickle.pgt_stream_tables SET last_refresh_at = now() - interval '30 days' WHERE pgt_relid = 'idle_vector_st'::regclass").await;
    db.execute("CREATE ROLE idle_vector_reader").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO idle_vector_reader")
        .await;
    db.execute("GRANT SELECT ON idle_vector_st TO idle_vector_reader")
        .await;
    let mut tx = db
        .pool
        .begin()
        .await
        .expect("begin idle diagnostic inspection");
    sqlx::query("SET LOCAL ROLE idle_vector_reader")
        .execute(&mut *tx)
        .await
        .expect("set reader role");
    let before: (i64, i64, i64, i64) = sqlx::query_as("SELECT pg_stat_get_xact_tuples_returned('idle_vector_st'::regclass)::bigint, pg_stat_get_xact_tuples_fetched('idle_vector_st'::regclass)::bigint, pg_stat_get_xact_tuples_returned('idle_vector_source'::regclass)::bigint, pg_stat_get_xact_tuples_fetched('idle_vector_source'::regclass)::bigint")
        .fetch_one(&mut *tx).await.expect("read initial vector table counters");
    let status: serde_json::Value = sqlx::query_scalar(
        "SELECT v FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'idle_vector_st'",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("inspect idle vector stream");
    let index: serde_json::Value = sqlx::query_scalar(
        "SELECT v FROM pgtrickle.ann_index_status_v2() v WHERE v->>'index' = 'idle_vector_ann'",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("inspect idle vector index");
    let after: (i64, i64, i64, i64) = sqlx::query_as("SELECT pg_stat_get_xact_tuples_returned('idle_vector_st'::regclass)::bigint, pg_stat_get_xact_tuples_fetched('idle_vector_st'::regclass)::bigint, pg_stat_get_xact_tuples_returned('idle_vector_source'::regclass)::bigint, pg_stat_get_xact_tuples_fetched('idle_vector_source'::regclass)::bigint")
        .fetch_one(&mut *tx).await.expect("read final vector table counters");
    assert_eq!(
        before, after,
        "catalog inspection must not read vector rows"
    );
    assert_eq!(status["status"], "ACTIVE");
    assert!(status["embedding_freshness"].is_null());
    assert!(status["backlog"].is_null());
    assert!(status["embedding_freshness_reason"].is_string());
    assert!(status["backlog_reason"].is_string());
    assert_eq!(index["valid"], true);
    assert_eq!(index["opclass"], "vector_l2_ops");
    assert!(
        !status.to_string().contains("0.123") && !index.to_string().contains("0.123"),
        "diagnostics must not expose the subscription vector"
    );
    tx.rollback().await.expect("end idle diagnostic inspection");
    db.assert_st_matches_query(
        "idle_vector_st",
        "SELECT id, embedding FROM idle_vector_source",
    )
    .await;
}

#[tokio::test]
async fn test_vector_index_expectation_reports_configuration_and_visibility() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE expect_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO expect_source VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "expect_stream",
        "SELECT id, embedding FROM expect_source",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("CREATE INDEX expect_ann ON expect_stream USING hnsw (embedding vector_l2_ops)")
        .await;
    db.execute("CREATE INDEX expect_partial ON expect_stream USING hnsw (embedding vector_l2_ops) WHERE id > 0").await;
    db.execute("CREATE TABLE expect_other (embedding vector(3))")
        .await;
    db.execute(
        "CREATE INDEX expect_other_ann ON expect_other USING hnsw (embedding vector_l2_ops)",
    )
    .await;
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire expectation session");
    sqlx::raw_sql(include_str!("fixtures/pgvector_index_expectation.sql"))
        .execute(&mut *conn)
        .await
        .expect("install session-local expectation query");
    for (index, opclass, expression, expected) in [
        (None, None, None, "exact_search"),
        (
            Some("public.expect_ann"),
            None,
            Some("embedding"),
            "incomplete_expectation",
        ),
        (
            Some("public.missing_ann"),
            Some("vector_l2_ops"),
            Some("embedding"),
            "missing_index",
        ),
        (
            Some("public.expect_other_ann"),
            Some("vector_l2_ops"),
            Some("embedding"),
            "unavailable_or_wrong_stream",
        ),
        (
            Some("public.expect_ann"),
            Some("vector_cosine_ops"),
            Some("embedding"),
            "metric_mismatch",
        ),
        (
            Some("public.expect_ann"),
            Some("vector_l2_ops"),
            Some("other_expression"),
            "expression_mismatch",
        ),
        (
            Some("public.expect_ann"),
            Some("vector_l2_ops"),
            Some("embedding"),
            "ready",
        ),
    ] {
        let actual: String = sqlx::query_scalar(
            "SELECT pg_temp.ann_expectation('expect_stream'::regclass, $1, $2, $3, NULL)",
        )
        .bind(index)
        .bind(opclass)
        .bind(expression)
        .fetch_one(&mut *conn)
        .await
        .expect("check expected index");
        assert_eq!(actual, expected);
    }
    let partial_mismatch: String = sqlx::query_scalar("SELECT pg_temp.ann_expectation('expect_stream'::regclass, 'public.expect_partial', 'vector_l2_ops', 'embedding', NULL)")
        .fetch_one(&mut *conn).await.expect("check unexpected partial index");
    assert_eq!(partial_mismatch, "predicate_mismatch");
    let partial_ready: String = sqlx::query_scalar("SELECT pg_temp.ann_expectation('expect_stream'::regclass, 'public.expect_partial', 'vector_l2_ops', 'embedding', '(id > 0)')")
        .fetch_one(&mut *conn).await.expect("check intended partial index");
    assert_eq!(partial_ready, "ready");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin invalid-index test");
    sqlx::query("UPDATE pg_catalog.pg_index SET indisvalid = false WHERE indexrelid = 'expect_ann'::regclass")
        .execute(&mut *conn).await.expect("mark test index invalid");
    let invalid: String = sqlx::query_scalar("SELECT pg_temp.ann_expectation('expect_stream'::regclass, 'public.expect_ann', 'vector_l2_ops', 'embedding', NULL)")
        .fetch_one(&mut *conn).await.expect("check invalid index");
    assert_eq!(invalid, "invalid_index");
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .expect("restore index state");
    sqlx::query("CREATE ROLE expect_observer")
        .execute(&mut *conn)
        .await
        .expect("create observer");
    sqlx::query("GRANT USAGE ON SCHEMA pgtrickle TO expect_observer")
        .execute(&mut *conn)
        .await
        .expect("grant schema");
    sqlx::query("SET ROLE expect_observer")
        .execute(&mut *conn)
        .await
        .expect("set observer");
    let hidden: Option<String> = sqlx::query_scalar("SELECT pg_temp.ann_expectation('expect_stream'::regclass, 'public.expect_ann', 'vector_l2_ops', 'embedding', NULL)")
        .fetch_one(&mut *conn).await.expect("check hidden stream");
    assert!(
        hidden.is_none(),
        "expectation must not reveal an inaccessible stream"
    );
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset observer");
    sqlx::query("GRANT SELECT ON expect_stream TO expect_observer")
        .execute(&mut *conn)
        .await
        .expect("grant stream visibility");
    sqlx::query("SET ROLE expect_observer")
        .execute(&mut *conn)
        .await
        .expect("set authorized observer");
    let visible: String = sqlx::query_scalar("SELECT pg_temp.ann_expectation('expect_stream'::regclass, 'public.expect_ann', 'vector_l2_ops', 'embedding', NULL)")
        .fetch_one(&mut *conn).await.expect("check visible stream");
    assert_eq!(visible, "ready");
    sqlx::query("RESET ROLE")
        .execute(&mut *conn)
        .await
        .expect("reset authorized observer");
}

#[tokio::test]
async fn test_vector_diagnostics_reports_partition_leaf_indexes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE diagnostic_partition_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st_partitioned(
        "diagnostic_partition_st",
        "SELECT id, embedding FROM diagnostic_partition_src",
        "1m",
        "DIFFERENTIAL",
        "id",
    )
    .await;
    db.execute("CREATE INDEX diagnostic_partition_ann ON diagnostic_partition_st USING hnsw (embedding vector_l2_ops)").await;
    let leaves = db.query_scalar::<i64>("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE v->>'stream_table_oid' = 'diagnostic_partition_st'::regclass::oid::text AND v->>'table_oid' <> v->>'stream_table_oid' AND v->>'opclass' = 'vector_l2_ops'").await;
    assert!(leaves > 0);
    let (leaf_index, leaf_table): (i64, i64) = sqlx::query_as("SELECT (v->>'index_oid')::bigint, (v->>'table_oid')::bigint FROM pgtrickle.ann_index_status_v2() v WHERE v->>'stream_table_oid' = 'diagnostic_partition_st'::regclass::oid::text LIMIT 1")
        .fetch_one(&db.pool).await.expect("find partition leaf");
    let _: i64 = sqlx::query_scalar("SELECT pgtrickle.request_ann_maintenance('diagnostic_partition_st'::regclass::oid, $1::bigint::oid)")
        .bind(leaf_index).fetch_one(&db.pool).await.expect("request leaf maintenance");
    let pending: bool = db.query_scalar("SELECT (v->>'maintenance_pending')::boolean FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_partition_st' LIMIT 1").await;
    assert!(pending);
    db.execute("CREATE SCHEMA diagnostic_private").await;
    let move_leaf: String = sqlx::query_scalar(
        "SELECT format('ALTER TABLE %s SET SCHEMA diagnostic_private', $1::bigint::oid::regclass)",
    )
    .bind(leaf_table)
    .fetch_one(&db.pool)
    .await
    .expect("quote leaf relation");
    db.execute(&move_leaf).await;
    db.execute("CREATE ROLE diagnostic_partition_observer")
        .await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle TO diagnostic_partition_observer")
        .await;
    db.execute(
        "GRANT SELECT, MAINTAIN ON diagnostic_partition_st TO diagnostic_partition_observer",
    )
    .await;
    let mut observer = db.pool.begin().await.expect("begin observer");
    sqlx::query("SET LOCAL ROLE diagnostic_partition_observer")
        .execute(&mut *observer)
        .await
        .expect("set observer");
    let redacted: bool = sqlx::query_scalar("SELECT v->'maintenance_pending' = 'null'::jsonb FROM pgtrickle.vector_stream_status_v2() v WHERE v->>'table' = 'diagnostic_partition_st' LIMIT 1")
        .fetch_one(&mut *observer).await.expect("inspect partition visibility");
    assert!(
        redacted,
        "root privileges alone must not expose leaf maintenance"
    );
    let hidden: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE (v->>'table_oid')::bigint = $1")
        .bind(leaf_table).fetch_one(&mut *observer).await.expect("inspect leaf index visibility");
    assert_eq!(
        hidden, 0,
        "root access must not expose an inaccessible leaf index"
    );
    observer.rollback().await.expect("finish observer");
    let grant_leaf: String = sqlx::query_scalar("SELECT format('GRANT SELECT ON TABLE %s TO diagnostic_partition_observer', $1::bigint::oid::regclass)")
        .bind(leaf_table).fetch_one(&db.pool).await.expect("quote leaf grant");
    db.execute(&grant_leaf).await;
    let mut observer = db.pool.begin().await.expect("begin leaf observer");
    sqlx::query("SET LOCAL ROLE diagnostic_partition_observer")
        .execute(&mut *observer)
        .await
        .expect("set leaf observer");
    let hidden: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE (v->>'table_oid')::bigint = $1")
        .bind(leaf_table).fetch_one(&mut *observer).await.expect("inspect leaf without schema usage");
    assert_eq!(
        hidden, 0,
        "leaf SELECT without schema USAGE must not expose the index"
    );
    observer.rollback().await.expect("finish leaf observer");
    db.execute("GRANT USAGE ON SCHEMA diagnostic_private TO diagnostic_partition_observer")
        .await;
    let mut observer = db.pool.begin().await.expect("begin authorized observer");
    sqlx::query("SET LOCAL ROLE diagnostic_partition_observer")
        .execute(&mut *observer)
        .await
        .expect("set authorized observer");
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE (v->>'table_oid')::bigint = $1")
        .bind(leaf_table).fetch_one(&mut *observer).await.expect("inspect authorized leaf");
    assert_eq!(visible, 1);
    observer
        .rollback()
        .await
        .expect("finish authorized observer");
    db.execute("REVOKE SELECT ON diagnostic_partition_st FROM diagnostic_partition_observer")
        .await;
    let mut observer = db.pool.begin().await.expect("begin leaf-only observer");
    sqlx::query("SET LOCAL ROLE diagnostic_partition_observer")
        .execute(&mut *observer)
        .await
        .expect("set leaf-only observer");
    let hidden: i64 = sqlx::query_scalar("SELECT count(*) FROM pgtrickle.ann_index_status_v2() v WHERE (v->>'table_oid')::bigint = $1")
        .bind(leaf_table).fetch_one(&mut *observer).await.expect("inspect leaf without root access");
    assert_eq!(
        hidden, 0,
        "leaf access must not bypass the root visibility check"
    );
    observer
        .rollback()
        .await
        .expect("finish leaf-only observer");
}

// ═══════════════════════════════════════════════════════════════════════
// T-VP3 / VP-5 (v0.47.0): Vector-aggregate cases — shard-additive verification
// ═══════════════════════════════════════════════════════════════════════

/// T-VP3 / VP-5: Verify that vector_avg() produces correct results consistent
/// with pgvector's native avg(vector) aggregate. This exercises the same
/// shard-additive algebra that would run in a Citus distributed deployment.
///
/// This test runs on the standard single-node e2e image (no Citus required)
/// but validates the arithmetic that the Citus path relies on.
#[tokio::test]
async fn test_vector_avg_shard_additive_correctness() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE shard_src (
            id      SERIAL PRIMARY KEY,
            shard   INTEGER NOT NULL,
            vec     vector(4)
        )",
    )
    .await;

    // Simulate two logical shards with known vectors
    // Shard 1: avg([1,2,3,4], [3,4,5,6]) = [2,3,4,5]
    // Shard 2: avg([0,0,0,0], [4,4,4,4]) = [2,2,2,2]
    db.execute(
        "INSERT INTO shard_src (shard, vec) VALUES
             (1, '[1,2,3,4]'), (1, '[3,4,5,6]'),
             (2, '[0,0,0,0]'), (2, '[4,4,4,4]')",
    )
    .await;

    let q = "SELECT shard, avg(vec) AS centroid, count(*) AS cnt FROM shard_src GROUP BY shard";
    let expected_q = "SELECT shard, avg(vec)::vector(4) AS centroid, count(*) AS cnt FROM shard_src GROUP BY shard";
    let create_sql = format!(
        "SELECT pgtrickle.create_stream_table('shard_avg_st', $${q}$$, '1m', 'DIFFERENTIAL')"
    );
    db.execute_seq(&["SET pg_trickle.enable_vector_agg = on", &create_sql])
        .await;

    db.assert_st_matches_query("shard_avg_st", expected_q).await;

    // Verify shard 1 centroid: avg([1,2,3,4],[3,4,5,6]) = [2,3,4,5]
    let s1_centroid: Option<String> = db
        .query_scalar_opt("SELECT centroid::TEXT FROM public.shard_avg_st WHERE shard = 1")
        .await;
    assert!(s1_centroid.is_some(), "shard 1 centroid should be present");
    // pgvector formats vectors as '[x,y,z,w]'; check it's close to [2,3,4,5]
    let ct = s1_centroid.unwrap();
    assert!(
        ct.contains("2") && ct.contains("3") && ct.contains("4") && ct.contains("5"),
        "shard 1 centroid ({ct}) should approximate [2,3,4,5]"
    );

    // Now insert into shard 1 — differential refresh must update centroid correctly
    db.execute("INSERT INTO shard_src (shard, vec) VALUES (1, '[5,6,7,8]')")
        .await;
    db.refresh_st("shard_avg_st").await;
    db.assert_st_matches_query("shard_avg_st", expected_q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// VH-1 (v0.48.0): halfvec/sparsevec aggregate type output correctness
// ═══════════════════════════════════════════════════════════════════════

/// VH-1a: avg(halfvec_col) output column should be typed halfvec(N), not vector(N).
#[tokio::test]
async fn test_pgvector_halfvec_avg_output_type() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE hv_src (
            id     SERIAL PRIMARY KEY,
            grp    INT,
            emb    halfvec(4)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO hv_src (grp, emb) VALUES
            (1, '[1,0,0,0]'),
            (1, '[0,1,0,0]'),
            (2, '[0,0,1,0]')",
    )
    .await;

    let q = "SELECT grp, avg(emb) AS centroid FROM hv_src GROUP BY grp";
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        &format!(
            "SELECT pgtrickle.create_stream_table('hv_avg_st', $${q}$$, '1m', 'DIFFERENTIAL')"
        ),
    ])
    .await;

    // The output column 'centroid' must be typed halfvec(4), not vector(4).
    let typname: Option<String> = db
        .query_scalar_opt(
            "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             WHERE c.relname = 'hv_avg_st' \
               AND a.attname = 'centroid' \
               AND a.attnum > 0",
        )
        .await;
    assert!(
        typname.is_some(),
        "column 'centroid' must exist in hv_avg_st"
    );
    let tn = typname.unwrap();
    assert!(
        tn.starts_with("halfvec"),
        "centroid column should be halfvec(4), got: {tn}"
    );
    let explanation: String = db
        .query_scalar("SELECT pgtrickle.explain_stream_table('hv_avg_st')")
        .await;
    assert!(
        explanation.contains("Potential centroid state: not estimated"),
        "halfvec must not claim an algebraic vector-state estimate: {explanation}"
    );
}

/// VH-1b: avg(halfvec) values are correct after differential refresh.
#[tokio::test]
async fn test_pgvector_halfvec_avg_values_correct() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE hv_vals_src (
            id  SERIAL PRIMARY KEY,
            grp INT,
            emb halfvec(2)
        )",
    )
    .await;
    db.execute("INSERT INTO hv_vals_src (grp, emb) VALUES (1, '[2,4]'), (1, '[4,2]')")
        .await;

    let q = "SELECT grp, avg(emb) AS centroid FROM hv_vals_src GROUP BY grp";
    let expected_q = "SELECT grp, avg(emb)::halfvec(2) AS centroid FROM hv_vals_src GROUP BY grp";
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        &format!(
            "SELECT pgtrickle.create_stream_table('hv_vals_st', $${q}$$, '1m', 'DIFFERENTIAL')"
        ),
    ])
    .await;
    db.assert_st_matches_query("hv_vals_st", expected_q).await;

    // INSERT — grp 1 centroid should become avg([2,4],[4,2],[6,0]) = [4,2]
    db.execute("INSERT INTO hv_vals_src (grp, emb) VALUES (1, '[6,0]')")
        .await;
    db.refresh_st("hv_vals_st").await;
    db.assert_st_matches_query("hv_vals_st", expected_q).await;
}

// ═══════════════════════════════════════════════════════════════════════
// VH-2 (v0.48.0): Distance subscription catalog operations
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_distance_subscription_notifies_only_membership_crossings() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_source (id int PRIMARY KEY, note text, embedding vector(3))")
        .await;
    db.create_st(
        "alert_st",
        "SELECT id, note, embedding FROM alert_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen("distance_crossing")
        .await
        .expect("listen for crossings");
    db.execute("SELECT pgtrickle.subscribe_distance('public.alert_st', 'distance_crossing', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    db.execute("INSERT INTO alert_source VALUES (1, 'first', '[1,0,0]')")
        .await;
    db.refresh_st("alert_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("enter notification")
        .expect("receive enter");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert_eq!(payload["exited"], 0);
    db.execute("UPDATE alert_source SET note='changed' WHERE id=1")
        .await;
    db.refresh_st("alert_st").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "unchanged membership must not notify"
    );
    drop(listener);
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("reconnect listener");
    listener
        .listen("distance_crossing")
        .await
        .expect("listen for exit");
    db.execute("UPDATE alert_source SET embedding='[0,1,0]' WHERE id=1")
        .await;
    db.refresh_st("alert_st").await;
    let value: String = db
        .query_scalar("SELECT embedding::text FROM alert_st WHERE id=1")
        .await;
    assert_eq!(value, "[0,1,0]");
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("exit notification")
        .expect("receive exit");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 0);
    assert_eq!(payload["exited"], 1);
    db.refresh_st("alert_st").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "unchanged refresh must not notify"
    );
    let mut tx = db
        .pool
        .begin()
        .await
        .expect("begin rolled-back source change");
    sqlx::query("INSERT INTO alert_source VALUES (2, 'rolled back', '[1,0,0]')")
        .execute(&mut *tx)
        .await
        .expect("insert in transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('alert_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh in transaction");
    tx.rollback().await.expect("rollback");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "rolled-back change must not notify"
    );
}

#[tokio::test]
async fn test_distance_subscription_alter_query_reports_committed_crossings() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_alter_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_alter_src VALUES (1, '[1,0,0]'), (2, '[1,0,0]')")
        .await;
    db.create_st(
        "alert_alter_st",
        "SELECT id, embedding FROM alert_alter_src WHERE id = 1",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_alter_st', 'alter_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("alter_crossings").await.expect("listen");

    db.execute("SELECT pgtrickle.alter_stream_table('alert_alter_st', query => $$SELECT id, embedding FROM alert_alter_src WHERE id = 2$$)").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("receive ALTER event")
        .expect("listener event");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert_eq!(payload["exited"], 1);
    let refresh_id = payload["refresh_id"].as_i64().expect("refresh id");
    let recorded: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id = $1 AND s.pgt_name = 'alert_alter_st' AND h.action = 'FULL' AND h.status = 'COMPLETED')")
        .bind(refresh_id).fetch_one(&db.pool).await.expect("refresh history");
    assert!(recorded);

    db.execute("SELECT pgtrickle.alter_stream_table('alert_alter_st', query => $$SELECT id, embedding FROM alert_alter_src WHERE id = 2$$)").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err()
    );

    let mut tx = db.pool.begin().await.expect("begin rollback test");
    sqlx::query("SELECT pgtrickle.alter_stream_table('alert_alter_st', query => $$SELECT id, embedding FROM alert_alter_src WHERE id = 1$$)")
        .execute(&mut *tx).await.expect("alter before rollback");
    tx.rollback().await.expect("rollback ALTER");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err()
    );
    let id: i32 = db.query_scalar("SELECT id FROM alert_alter_st").await;
    assert_eq!(id, 2);
    let latest: i64 = db.query_scalar("SELECT max(refresh_id) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'alert_alter_st')").await;
    assert_eq!(
        latest, refresh_id,
        "rolled-back ALTER must not leave history"
    );

    db.execute("UPDATE pgtrickle.pgt_distance_subscriptions SET query_vector = 'invalid' WHERE channel = 'alter_crossings'").await;
    db.execute("SELECT pgtrickle.alter_stream_table('alert_alter_st', query => $$SELECT id, embedding FROM alert_alter_src WHERE id = 1$$)").await;
    let id: i32 = db.query_scalar("SELECT id FROM alert_alter_st").await;
    assert_eq!(id, 1, "notification failure must not roll back ALTER");
}

#[tokio::test]
async fn test_distance_subscription_partition_rebuild_reports_crossings() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_partition_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_partition_src VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    db.create_st(
        "alert_partition_st",
        "SELECT id, embedding FROM alert_partition_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_partition_st', 'partition_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen("partition_crossings")
        .await
        .expect("listen");
    db.execute("UPDATE alert_partition_src SET embedding = CASE id WHEN 1 THEN '[0,1,0]'::vector ELSE '[1,0,0]'::vector END").await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('alert_partition_st', partition_by => 'HASH:id:2')",
    )
    .await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("receive partition rebuild event")
        .expect("listener event");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert_eq!(payload["exited"], 1);
    let refresh_id = payload["refresh_id"].as_i64().expect("refresh id");
    let recorded: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id = $1 AND s.pgt_name = 'alert_partition_st' AND h.action = 'FULL' AND h.status = 'COMPLETED')").bind(refresh_id).fetch_one(&db.pool).await.expect("refresh history");
    assert!(recorded);

    let mut tx = db.pool.begin().await.expect("begin rollback test");
    sqlx::query("UPDATE alert_partition_src SET embedding = '[1,0,0]' WHERE id = 1")
        .execute(&mut *tx)
        .await
        .expect("update before rollback");
    sqlx::query("SELECT pgtrickle.alter_stream_table('alert_partition_st', partition_by => '')")
        .execute(&mut *tx)
        .await
        .expect("rebuild before rollback");
    tx.rollback().await.expect("rollback partition rebuild");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err()
    );
    let latest: i64 = db.query_scalar("SELECT max(refresh_id) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'alert_partition_st')").await;
    assert_eq!(
        latest, refresh_id,
        "rolled-back rebuild must not leave history"
    );
}

#[tokio::test]
async fn test_distance_subscription_mode_change_reports_crossings() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_mode_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_mode_src VALUES (1, '[1,0,0]')")
        .await;
    db.create_st(
        "alert_mode_st",
        "SELECT id, embedding FROM alert_mode_src",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_mode_st', 'mode_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("mode_crossings").await.expect("listen");
    db.execute("UPDATE alert_mode_src SET embedding = '[0,1,0]' WHERE id = 1")
        .await;
    db.execute("SELECT pgtrickle.alter_stream_table('alert_mode_st', refresh_mode => 'FULL')")
        .await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("receive mode-change event")
        .expect("listener event");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 0);
    assert_eq!(payload["exited"], 1);
    let refresh_id = payload["refresh_id"].as_i64().expect("refresh id");
    let recorded: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id = $1 AND s.pgt_name = 'alert_mode_st' AND h.action = 'FULL' AND h.status = 'COMPLETED')").bind(refresh_id).fetch_one(&db.pool).await.expect("refresh history");
    assert!(recorded);

    let mut tx = db.pool.begin().await.expect("begin rollback test");
    sqlx::query("UPDATE alert_mode_src SET embedding = '[1,0,0]' WHERE id = 1")
        .execute(&mut *tx)
        .await
        .expect("update before rollback");
    sqlx::query(
        "SELECT pgtrickle.alter_stream_table('alert_mode_st', refresh_mode => 'DIFFERENTIAL')",
    )
    .execute(&mut *tx)
    .await
    .expect("change mode before rollback");
    tx.rollback().await.expect("rollback mode change");
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err()
    );
    let latest: i64 = db.query_scalar("SELECT max(refresh_id) FROM pgtrickle.pgt_refresh_history WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'alert_mode_st')").await;
    assert_eq!(
        latest, refresh_id,
        "rolled-back mode change must not leave history"
    );
}

#[tokio::test]
async fn test_distance_subscription_alter_query_notifies_parent_and_downstream() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_chain_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_chain_src VALUES (1, '[1,0,0]'), (2, '[1,0,0]')")
        .await;
    db.create_st(
        "alert_chain_parent",
        "SELECT id, embedding FROM alert_chain_src WHERE id = 1",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute_seq(&[
        "SET pg_trickle.enable_vector_agg = on",
        "SELECT pgtrickle.create_stream_table('alert_chain_child', $$SELECT id, avg(embedding)::vector(3) AS embedding FROM alert_chain_parent GROUP BY id$$, '1h', 'DIFFERENTIAL')",
        "RESET pg_trickle.enable_vector_agg",
    ]).await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_chain_parent', 'parent_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen("parent_crossings")
        .await
        .expect("listen to parent");
    listener
        .listen("child_crossings")
        .await
        .expect("listen to child");
    let mut registration_tx = db.pool.begin().await.expect("begin registration");
    sqlx::query("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'alert_chain_child' FOR UPDATE")
        .execute(&mut *registration_tx).await.expect("lock child catalog row");
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *registration_tx)
        .await
        .expect("registration PID");
    let pool = db.pool.clone();
    let (pid_sender, pid_receiver) = tokio::sync::oneshot::channel();
    let alter = tokio::spawn(async move {
        let mut conn = pool.acquire().await?;
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *conn)
            .await?;
        let _ = pid_sender.send(pid);
        sqlx::query("SELECT pgtrickle.alter_stream_table('alert_chain_parent', query => $$SELECT id, embedding FROM alert_chain_src WHERE id = 2$$)").execute(&mut *conn).await
    });
    let alter_pid = tokio::time::timeout(Duration::from_secs(10), pid_receiver)
        .await
        .expect("ALTER PID timeout")
        .expect("ALTER PID");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let blocked: bool =
                sqlx::query_scalar("SELECT $1 = ANY(pg_catalog.pg_blocking_pids($2))")
                    .bind(blocker)
                    .bind(alter_pid)
                    .fetch_one(&db.pool)
                    .await
                    .expect("inspect ALTER lock");
            if blocked {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("ALTER must wait for downstream registration");
    sqlx::query("SELECT pgtrickle.subscribe_distance('alert_chain_child', 'child_crossings', 'embedding', '[1,0,0]', '<->', 0.5)")
        .execute(&mut *registration_tx).await.expect("register child subscription");
    registration_tx
        .commit()
        .await
        .expect("commit child subscription");
    tokio::time::timeout(Duration::from_secs(30), alter)
        .await
        .expect("ALTER completes")
        .expect("ALTER task")
        .expect("ALTER succeeds");
    let child_records: i64 = db.query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name = 'alert_chain_child' AND h.action = 'FULL' AND h.initiated_by = 'MANUAL' AND h.status = 'COMPLETED'").await;
    assert_eq!(
        child_records, 1,
        "downstream rebuild must have notification history"
    );
    let child_ids: Vec<i32> = sqlx::query_scalar("SELECT id FROM alert_chain_child ORDER BY id")
        .fetch_all(&db.pool)
        .await
        .expect("read downstream");
    assert_eq!(
        child_ids,
        vec![2],
        "downstream rebuild must contain replacement rows"
    );
    let mut channels = std::collections::BTreeSet::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .unwrap_or_else(|_| panic!("missing chain event; received {channels:?}"))
            .expect("listener event");
        let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
        assert_eq!(payload["entered"], 1);
        assert_eq!(payload["exited"], 1);
        let name = match event.channel() {
            "parent_crossings" => "alert_chain_parent",
            "child_crossings" => "alert_chain_child",
            other => panic!("unexpected channel {other}"),
        };
        let recorded: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE h.refresh_id = $1 AND s.pgt_name = $2 AND h.status = 'COMPLETED')")
            .bind(payload["refresh_id"].as_i64().expect("refresh id")).bind(name).fetch_one(&db.pool).await.expect("refresh history");
        assert!(recorded);
        channels.insert(event.channel().to_string());
    }
    assert_eq!(
        channels,
        [
            "child_crossings".to_string(),
            "parent_crossings".to_string()
        ]
        .into_iter()
        .collect()
    );
}

#[tokio::test]
async fn test_distance_subscription_failed_alter_retry_restores_snapshot_context() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_retry_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_retry_src VALUES (1, '[1,0,0]'), (2, '[1,0,0]')")
        .await;
    db.create_st(
        "alert_retry_st",
        "SELECT id, embedding FROM alert_retry_src WHERE id = 1",
        "1h",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_retry_st', 'retry_crossings', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    db.execute("CREATE FUNCTION public.fail_alert_alter() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.pgt_name = 'alert_retry_st' AND NEW.status = 'SUSPENDED' AND current_setting('test.fail_alert_alter', true) = 'on' THEN RAISE EXCEPTION 'injected ALTER failure'; END IF; RETURN NEW; END $$").await;
    db.execute("CREATE TRIGGER fail_alert_alter BEFORE UPDATE ON pgtrickle.pgt_stream_tables FOR EACH ROW EXECUTE FUNCTION public.fail_alert_alter()").await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("retry_crossings").await.expect("listen");
    let mut conn = db.pool.acquire().await.expect("acquire connection");
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .expect("begin");
    sqlx::query("SAVEPOINT before_failed_alter")
        .execute(&mut *conn)
        .await
        .expect("savepoint");
    sqlx::query("SET LOCAL test.fail_alert_alter = 'on'")
        .execute(&mut *conn)
        .await
        .expect("enable failure");
    let alter = "SELECT pgtrickle.alter_stream_table('alert_retry_st', query => $$SELECT id, embedding FROM alert_retry_src WHERE id = 2$$)";
    let error = sqlx::query(alter)
        .execute(&mut *conn)
        .await
        .expect_err("injected ALTER must fail");
    assert!(error.to_string().contains("injected ALTER failure"));
    sqlx::query("ROLLBACK TO SAVEPOINT before_failed_alter")
        .execute(&mut *conn)
        .await
        .expect("rollback failed ALTER");
    sqlx::query("SET LOCAL test.fail_alert_alter = 'off'")
        .execute(&mut *conn)
        .await
        .expect("disable failure");
    sqlx::query(alter)
        .execute(&mut *conn)
        .await
        .expect("retry ALTER");
    sqlx::query("COMMIT")
        .execute(&mut *conn)
        .await
        .expect("commit retry");
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("receive retry event")
        .expect("listener event");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert_eq!(payload["exited"], 1);
}

#[tokio::test]
async fn test_distance_subscription_small_delta_snapshots_only_affected_rows() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_batch_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute(
        "INSERT INTO alert_batch_src SELECT i, '[1,0,0]'::vector FROM generate_series(1,1000) i",
    )
    .await;
    db.create_st(
        "alert_batch_st",
        "SELECT id, embedding FROM alert_batch_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('public.alert_batch_st', 'batch_alerts', 'embedding', '[1,0,0]', '<->', 0.5)")
        .await;
    db.execute("UPDATE alert_batch_src SET embedding='[0,1,0]' WHERE id=1")
        .await;
    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_schema='public' AND pgt_name='alert_batch_st'")
        .await;
    let mut tx = db.pool.begin().await.expect("begin refresh transaction");
    sqlx::query("SELECT pgtrickle.refresh_stream_table('alert_batch_st')")
        .execute(&mut *tx)
        .await
        .expect("refresh one changed row");
    let snapshot_sql = format!("SELECT count(*) FROM pg_temp.__pgt_distance_pre_{pgt_id}");
    let before_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(snapshot_sql))
        .fetch_one(&mut *tx)
        .await
        .expect("inspect affected pre-refresh rows");
    let ids_sql = format!("SELECT count(*) FROM pg_temp.__pgt_distance_ids_{pgt_id}");
    let id_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(ids_sql))
        .fetch_one(&mut *tx)
        .await
        .expect("inspect affected identities");
    assert_eq!(before_count, 1);
    assert_eq!(id_count, 1);
    let plan_sql = format!(
        "EXPLAIN (ANALYZE, BUFFERS) SELECT st.* FROM pg_temp.__pgt_distance_ids_{pgt_id} d \
         CROSS JOIN LATERAL (SELECT st.* FROM public.alert_batch_st st \
         WHERE pgtrickle.row_probe_v1(st.__pgt_row_id) = pgtrickle.row_probe_v1(d.__pgt_row_id) \
           AND st.__pgt_row_id = d.__pgt_row_id OFFSET 0) st"
    );
    let plan: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(plan_sql))
        .fetch_all(&mut *tx)
        .await
        .expect("inspect affected-row lookup plan");
    assert!(
        plan.iter().any(|line| line.contains("Index")
            && line.contains("Scan")
            && line.contains("row_id_idx")),
        "affected-row lookup should use the row-identity index: {plan:?}"
    );
    tx.commit().await.expect("commit refresh");
}

#[tokio::test]
async fn test_distance_subscription_typed_predicates_and_authorization() {
    use sqlx::postgres::PgListener;
    use std::collections::HashSet;
    use std::time::Duration;

    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_typed_src (id int PRIMARY KEY, dense vector(3), half halfvec(3), bits bit(3), sparse sparsevec(3))").await;
    db.create_st(
        "alert_typed_st",
        "SELECT id, dense, half, bits, sparse FROM alert_typed_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    for channel in [
        "typed_dense",
        "typed_cosine",
        "typed_half",
        "typed_bits",
        "typed_jaccard",
        "typed_sparse",
    ] {
        listener.listen(channel).await.expect("listen");
    }
    for sql in [
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','typed_dense','dense','[1,0,0]','<#>',-0.5)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','typed_cosine','dense','[1,0,0]','<=>',0.1)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_half','half','[1,0,0]','<->',0.5)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_bits','bits','101','<~>',1)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_jaccard','bits','101','<%>',0.5)",
        "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','typed_sparse','sparse','{1:1}/3','<->',0.5)",
    ] {
        db.execute(sql).await;
    }
    for sql in [
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_dim','dense','[1,0]','<->',0.5)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_op','bits','101','<->',0.5)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_nan','dense','[1,0,0]','<->','NaN'::float8)",
        "SELECT pgtrickle.subscribe_distance('alert_typed_st','bad_inf','dense','[1,0,0]','<->','Infinity'::float8)",
    ] {
        assert!(
            db.try_execute(sql).await.is_err(),
            "invalid subscription accepted: {sql}"
        );
    }
    for (query, actual) in [("10", 2), ("1011", 4)] {
        let sql = format!(
            "SELECT pgtrickle.subscribe_distance('alert_typed_st','typed_bits','bits','{query}','<~>',1)"
        );
        let error = db
            .try_execute(&sql)
            .await
            .expect_err("reject wrong bit length");
        assert!(
            error.to_string().contains(&format!(
                "bit query has {actual} dimensions; column bits requires 3"
            )),
            "unexpected error: {error}"
        );
    }
    let retained: String = db.query_scalar("SELECT query_vector FROM pgtrickle.pgt_distance_subscriptions WHERE channel = 'typed_bits'").await;
    assert_eq!(
        retained, "101",
        "invalid edits must preserve the subscription"
    );
    db.execute("INSERT INTO alert_typed_src VALUES (1,'[1,0,0]','[1,0,0]','101','{1:1}/3')")
        .await;
    db.refresh_st("alert_typed_st").await;
    let mut channels = HashSet::new();
    for _ in 0..6 {
        let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .expect("typed alert")
            .expect("receive typed alert");
        let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
        assert_eq!(payload["entered"], 1);
        channels.insert(event.channel().to_owned());
    }
    assert_eq!(channels.len(), 6);

    db.execute("CREATE ROLE alert_intruder NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, public TO alert_intruder")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.subscribe_distance(text,text,text,text,text,double precision), pgtrickle.unsubscribe_distance(text,text) TO alert_intruder").await;
    let error = db.try_execute_with_role("SET ROLE alert_intruder", "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','stolen','dense','[1,0,0]','<->',0.5)", "RESET ROLE").await.expect_err("non-owner subscription must fail");
    assert!(
        error.to_string().contains("must be owner"),
        "unexpected denial: {error}"
    );
    let mut tx = db.pool.begin().await.expect("begin observer transaction");
    sqlx::query("SET LOCAL ROLE alert_intruder")
        .execute(&mut *tx)
        .await
        .expect("set role");
    let visible: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pgtrickle.list_distance_subscriptions(NULL)")
            .fetch_one(&mut *tx)
            .await
            .expect("list visible subscriptions");
    assert_eq!(visible, 0);
    tx.rollback().await.expect("end observer transaction");

    db.execute("CREATE ROLE alert_owner NOLOGIN").await;
    db.execute("GRANT USAGE ON SCHEMA pgtrickle, public TO alert_owner")
        .await;
    db.execute("GRANT EXECUTE ON FUNCTION pgtrickle.subscribe_distance(text,text,text,text,text,double precision), pgtrickle.unsubscribe_distance(text,text) TO alert_owner").await;
    db.execute("ALTER TABLE public.alert_typed_st OWNER TO alert_owner")
        .await;
    let result = db.try_execute_with_role("SET ROLE alert_owner", "SELECT pgtrickle.subscribe_distance('public.alert_typed_st','owner_alert','dense','[1,0,0]','<->',0.5)", "RESET ROLE").await;
    assert!(
        result.is_ok(),
        "stream owner should be able to subscribe: {result:?}"
    );
    let mut tx = db.pool.begin().await.expect("begin owner transaction");
    sqlx::query("SET LOCAL ROLE alert_owner")
        .execute(&mut *tx)
        .await
        .expect("set owner role");
    let visible: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pgtrickle.list_distance_subscriptions('public.alert_typed_st')",
    )
    .fetch_one(&mut *tx)
    .await
    .expect("list owner subscriptions");
    assert_eq!(visible, 7);
    tx.rollback().await.expect("end owner transaction");
}

#[tokio::test]
async fn test_distance_subscription_key_delete_and_null_transitions() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_key_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_key_src VALUES (1,'[1,0,0]')")
        .await;
    db.create_st(
        "alert_key_st",
        "SELECT id, embedding FROM alert_key_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("key_alert").await.expect("listen");
    db.execute("SELECT pgtrickle.subscribe_distance('alert_key_st','key_alert','embedding','[1,0,0]','<->',0.5)").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), listener.recv())
            .await
            .is_err(),
        "registration establishes a baseline"
    );
    for (mutation, entered, exited) in [
        ("UPDATE alert_key_src SET id=2 WHERE id=1", 1, 1),
        ("DELETE FROM alert_key_src WHERE id=2", 0, 1),
        ("INSERT INTO alert_key_src VALUES (3,'[1,0,0]')", 1, 0),
        ("UPDATE alert_key_src SET embedding=NULL WHERE id=3", 0, 1),
    ] {
        db.execute(mutation).await;
        db.refresh_st("alert_key_st").await;
        let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .expect("crossing notification")
            .expect("receive crossing");
        let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
        assert_eq!(payload["entered"], entered, "{mutation}");
        assert_eq!(payload["exited"], exited, "{mutation}");
    }
}

#[tokio::test]
async fn test_distance_subscription_full_refresh_and_edit_baseline() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_full_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO alert_full_src VALUES (1,'[1,0,0]')")
        .await;
    db.create_st(
        "alert_full_st",
        "SELECT id, embedding FROM alert_full_src",
        "1m",
        "FULL",
    )
    .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("full_alert").await.expect("listen");
    db.execute("SELECT pgtrickle.subscribe_distance('alert_full_st','full_alert','embedding','[1,0,0]','<->',0.5)").await;
    db.execute("UPDATE alert_full_src SET embedding='[0,1,0]' WHERE id=1")
        .await;
    db.refresh_st("alert_full_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("full exit")
        .expect("receive full exit");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["exited"], 1);
    db.execute("SELECT pgtrickle.subscribe_distance('alert_full_st','full_alert','embedding','[0,1,0]','<->',0.5)").await;
    db.refresh_st("alert_full_st").await;
    assert!(
        tokio::time::timeout(Duration::from_millis(500), listener.recv())
            .await
            .is_err(),
        "subscription edits establish a baseline"
    );
    db.execute("DELETE FROM alert_full_src WHERE id=1").await;
    db.refresh_st("alert_full_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("full delete exit")
        .expect("receive full delete exit");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["exited"], 1);
    db.execute("INSERT INTO alert_full_src VALUES (2,'[0,1,0]')")
        .await;
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET needs_reinit=true WHERE pgt_name='alert_full_st'",
    )
    .await;
    db.refresh_st("alert_full_st").await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("reinitialization entry")
        .expect("receive reinitialization entry");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
}

#[tokio::test]
async fn test_distance_subscription_quoted_names_and_extension_schema() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"Vector Ext\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"Vector Ext\"")
        .await;
    db.execute("CREATE SCHEMA \"Odd.Schema\"").await;
    db.execute("CREATE TABLE \"Odd.Schema\".source (id int PRIMARY KEY, \"vec weird\" \"Vector Ext\".vector(3))").await;
    db.execute("SELECT pgtrickle.create_stream_table('\"Odd.Schema\".\"Alert Table\"', $$SELECT id, \"vec weird\" FROM \"Odd.Schema\".source$$, '1m', 'DIFFERENTIAL')").await;
    let channel = "alerts \"odd'";
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener
        .listen(channel)
        .await
        .expect("listen to quoted channel");
    db.execute_seq(&[
        "SET search_path TO \"Odd.Schema\", public",
        "SELECT pgtrickle.subscribe_distance('\"Alert Table\"', 'alerts \"odd''', '\"vec weird\"', '[1,0,0]', '<->', 0.5)",
        "RESET search_path",
    ]).await;
    db.execute("INSERT INTO \"Odd.Schema\".source VALUES (1,'[1,0,0]')")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('\"Odd.Schema\".\"Alert Table\"')")
        .await;
    let event = tokio::time::timeout(Duration::from_secs(5), listener.recv())
        .await
        .expect("quoted alert")
        .expect("receive alert");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(event.channel(), channel);
    assert_eq!(payload["schema"], "Odd.Schema");
    assert_eq!(payload["stream_table"], "Alert Table");
    assert_eq!(payload["entered"], 1);
    db.execute("SELECT pgtrickle.drop_stream_table('\"Odd.Schema\".\"Alert Table\"')")
        .await;
    let count: i64 = db.query_scalar("SELECT count(*) FROM pgtrickle.pgt_distance_subscriptions WHERE channel='alerts \"odd''' ").await;
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_distance_subscription_rejects_unsupported_notification_modes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_modes_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "alert_modes_st",
        "SELECT id, embedding FROM alert_modes_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('alert_modes_st','mode_alert','embedding','[1,0,0]','<->',0.5)").await;
    db.execute(
        "SELECT pgtrickle.alter_stream_table('alert_modes_st', pooler_compatibility_mode => true)",
    )
    .await;
    let error = db.try_execute("SELECT pgtrickle.subscribe_distance('alert_modes_st','new_alert','embedding','[1,0,0]','<->',0.5)").await.expect_err("pooler mode must reject subscriptions");
    assert!(
        error.to_string().contains("pooler compatibility"),
        "unexpected error: {error}"
    );
    db.execute("INSERT INTO alert_modes_src VALUES (1,'[1,0,0]')")
        .await;
    let notices = db
        .try_execute_with_notices("SELECT pgtrickle.refresh_stream_table('alert_modes_st')")
        .await
        .expect("refresh remains successful");
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains("distance notifications are disabled")),
        "missing pooler warning: {notices:?}"
    );
    db.create_st(
        "alert_immediate_st",
        "SELECT id, embedding FROM alert_modes_src",
        "1m",
        "IMMEDIATE",
    )
    .await;
    let error = db.try_execute("SELECT pgtrickle.subscribe_distance('alert_immediate_st','immediate_alert','embedding','[1,0,0]','<->',0.5)").await.expect_err("IMMEDIATE mode must reject subscriptions");
    assert!(
        error.to_string().contains("IMMEDIATE"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn test_distance_subscription_waits_for_refresh_catalog_lock() {
    use std::time::Duration;
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_lock_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "alert_lock_st",
        "SELECT id, embedding FROM alert_lock_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let mut tx = db.pool.begin().await.expect("begin lock transaction");
    sqlx::query(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name='alert_lock_st' FOR UPDATE",
    )
    .execute(&mut *tx)
    .await
    .expect("hold refresh catalog lock");
    let pool = db.pool.clone();
    let mut registration = tokio::spawn(async move {
        sqlx::query("SELECT pgtrickle.subscribe_distance('alert_lock_st','lock_alert','embedding','[1,0,0]','<->',0.5)")
            .execute(&pool).await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut registration)
            .await
            .is_err(),
        "registration must wait for the refresh lock"
    );
    tx.commit().await.expect("release refresh catalog lock");
    tokio::time::timeout(Duration::from_secs(5), registration)
        .await
        .expect("registration completes")
        .expect("registration task")
        .expect("registration succeeds");
}

#[tokio::test]
async fn test_distance_subscription_fused_chain_scheduler_emits_crossing() {
    use sqlx::postgres::PgListener;
    use std::time::Duration;
    let db = E2eDb::new_dedicated().await.with_extension().await;
    setup_pgvector(&db).await;
    for setting in [
        "ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100",
        "ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1",
        "ALTER SYSTEM SET pg_trickle.auto_backoff = off",
        "ALTER SYSTEM SET pg_trickle.parallel_refresh_mode = 'on'",
        "ALTER SYSTEM SET pg_trickle.enable_fused_refresh = on",
    ] {
        db.execute(setting).await;
    }
    db.reload_config_and_wait().await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "scheduler did not start"
    );
    db.execute("CREATE TABLE fused_alert_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "fused_alert_first",
        "SELECT id, embedding FROM fused_alert_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;
    db.create_st(
        "fused_alert_second",
        "SELECT id FROM fused_alert_first",
        "1s",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("INSERT INTO fused_alert_src VALUES (1,'[0,1,0]')")
        .await;
    assert!(
        db.wait_for_condition(
            "fused chain control converged",
            "SELECT count(*)=1 FROM fused_alert_second",
            Duration::from_secs(30),
            Duration::from_millis(100)
        )
        .await
    );
    // shortcut: workers start with cold template caches, so this checks chain scheduling, not fused SQL; add that control when fusion can be witnessed.
    assert!(db.wait_for_condition("fused chain control job", "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_scheduler_jobs WHERE unit_kind='fused_chain' AND status='SUCCEEDED')", Duration::from_secs(30), Duration::from_millis(100)).await);
    let previous_job: i64 = db
        .query_scalar(
            "SELECT max(job_id) FROM pgtrickle.pgt_scheduler_jobs WHERE unit_kind='fused_chain'",
        )
        .await;
    let mut listener = PgListener::connect_with(&db.pool)
        .await
        .expect("connect listener");
    listener.listen("fused_alert").await.expect("listen");
    db.execute("SELECT pgtrickle.subscribe_distance('fused_alert_first','fused_alert','embedding','[1,0,0]','<->',0.5)").await;
    db.execute("INSERT INTO fused_alert_src VALUES (100,'[1,0,0]')")
        .await;
    let event = tokio::time::timeout(Duration::from_secs(30), listener.recv())
        .await
        .expect("scheduler crossing")
        .expect("receive scheduler crossing");
    let payload: serde_json::Value = serde_json::from_str(event.payload()).expect("valid JSON");
    assert_eq!(payload["entered"], 1);
    assert!(db.wait_for_condition("subscribed fused chain job", &format!("SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_scheduler_jobs WHERE unit_kind='fused_chain' AND status='SUCCEEDED' AND job_id > {previous_job})"), Duration::from_secs(30), Duration::from_millis(100)).await);
    let initiated_by: String = db.query_scalar("SELECT h.initiated_by FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) WHERE s.pgt_name='fused_alert_first' AND h.refresh_id=(SELECT max(h2.refresh_id) FROM pgtrickle.pgt_refresh_history h2 WHERE h2.pgt_id=s.pgt_id AND h2.status='COMPLETED')").await;
    assert_ne!(initiated_by, "SCHEDULER_FUSED");
}

#[tokio::test]
async fn test_distance_subscription_invalid_stored_value_does_not_abort_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE alert_error_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.create_st(
        "alert_error_st",
        "SELECT id, embedding FROM alert_error_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    db.execute("SELECT pgtrickle.subscribe_distance('public.alert_error_st', 'distance_error', 'embedding', '[1,0,0]', '<->', 0.5)").await;
    db.execute("UPDATE pgtrickle.pgt_distance_subscriptions SET query_vector='invalid' WHERE stream_table='public.alert_error_st'").await;
    db.execute("INSERT INTO alert_error_source VALUES (1, '[1,0,0]')")
        .await;
    let notices = db
        .try_execute_with_notices("SELECT pgtrickle.refresh_stream_table('alert_error_st')")
        .await
        .expect("alert failure must not abort refresh");
    assert!(
        notices
            .iter()
            .any(|notice| notice.contains("distance subscription failed")),
        "missing failure warning: {notices:?}"
    );
    let count: i64 = db.query_scalar("SELECT count(*) FROM alert_error_st").await;
    assert_eq!(count, 1);
}

/// VH-2a: subscribe_distance() inserts into pgt_distance_subscriptions.
#[tokio::test]
async fn test_distance_subscription_catalog_insert() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute("CREATE TABLE ds_src (id SERIAL PRIMARY KEY, grp INT, emb vector(3))")
        .await;
    db.execute("INSERT INTO ds_src (grp, emb) VALUES (1, '[1,0,0]'), (1, '[0,1,0]')")
        .await;
    db.create_st(
        "ds_st",
        "SELECT grp, avg(emb)::vector(3) AS centroid FROM ds_src GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.subscribe_distance(\
             'public.ds_st', 'near_centroid', 'centroid', '[1,0,0]', '<->', 0.5\
         )",
    )
    .await;

    let count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_distance_subscriptions \
             WHERE stream_table = 'public.ds_st' AND channel = 'near_centroid'",
        )
        .await;
    assert_eq!(count, 1, "subscription should be registered in catalog");
    let threshold: f64 = db
        .query_scalar("SELECT threshold FROM pgtrickle.pgt_distance_subscriptions WHERE stream_table = 'public.ds_st' AND channel = 'near_centroid'")
        .await;
    assert_eq!(threshold, 0.5);
    db.execute("SELECT pgtrickle.subscribe_distance('public.ds_st', 'near_centroid', 'centroid', '[1,0,0]', '<->', 0.3)").await;
    let threshold: f64 = db
        .query_scalar("SELECT threshold FROM pgtrickle.list_distance_subscriptions('public.ds_st') WHERE channel = 'near_centroid'")
        .await;
    assert_eq!(threshold, 0.3);
}

/// VH-2b: unsubscribe_distance() removes the catalog entry.
#[tokio::test]
async fn test_distance_subscription_unsubscribe() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute("CREATE TABLE ds2_src (id SERIAL PRIMARY KEY, emb vector(2))")
        .await;
    db.create_st(
        "ds2_st",
        "SELECT id, emb FROM ds2_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.subscribe_distance(\
             'public.ds2_st', 'ch_unsub', 'emb', '[1,0]', '<->', 0.3\
         )",
    )
    .await;
    db.execute("SELECT pgtrickle.unsubscribe_distance('public.ds2_st', 'ch_unsub')")
        .await;

    let count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_distance_subscriptions \
             WHERE stream_table = 'public.ds2_st' AND channel = 'ch_unsub'",
        )
        .await;
    assert_eq!(count, 0, "subscription should be removed after unsubscribe");
}

/// VH-2c: list_distance_subscriptions() returns subscriptions for a stream table.
#[tokio::test]
async fn test_distance_subscription_list() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute("CREATE TABLE ds3_src (id SERIAL PRIMARY KEY, emb vector(2))")
        .await;
    db.create_st(
        "ds3_st",
        "SELECT id, emb FROM ds3_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute_seq(&[
        "SELECT pgtrickle.subscribe_distance('public.ds3_st', 'ch_a', 'emb', '[1,0]', '<->', 0.5)",
        "SELECT pgtrickle.subscribe_distance('public.ds3_st', 'ch_b', 'emb', '[0,1]', '<->', 0.8)",
    ])
    .await;

    let count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM pgtrickle.list_distance_subscriptions('public.ds3_st')")
        .await;
    assert_eq!(count, 2, "list_distance_subscriptions should return 2 rows");
}

// ═══════════════════════════════════════════════════════════════════════
// VA-1 (v0.48.0): embedding_stream_table() ergonomic API
// ═══════════════════════════════════════════════════════════════════════

/// VA-1a: embedding_stream_table() creates a stream table with correct schema.
#[tokio::test]
async fn test_embedding_stream_table_creates_st() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE va1_src (
            id        SERIAL PRIMARY KEY,
            tenant_id INT,
            content   TEXT,
            embedding vector(3)
        )",
    )
    .await;
    db.execute(
        "INSERT INTO va1_src (tenant_id, content, embedding) VALUES
            (1, 'hello', '[1,0,0]'),
            (1, 'world', '[0,1,0]'),
            (2, 'foo',   '[0,0,1]')",
    )
    .await;

    // dry_run=true: returns SQL actions without executing
    let action_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.embedding_stream_table(\
                 'va1_emb_st', 'public.va1_src', 'embedding', \
                 NULL, '1m', 'hnsw', TRUE\
             ) AS t(action)",
        )
        .await;
    assert!(
        action_count >= 1,
        "dry_run should return at least one action, got: {action_count}"
    );
}

/// VA-1b: embedding_stream_table() with dry_run=false creates a real stream table.
#[tokio::test]
async fn test_embedding_stream_table_creates_real_st() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE va1b_src (
            id        SERIAL PRIMARY KEY,
            embedding vector(2)
        )",
    )
    .await;
    db.execute("INSERT INTO va1b_src (embedding) VALUES ('[1,0]'), ('[0,1]')")
        .await;

    db.execute(
        "SELECT pgtrickle.embedding_stream_table(\
             'va1b_emb_st', 'public.va1b_src', 'embedding', \
             NULL, '1m', 'hnsw', FALSE\
         )",
    )
    .await;

    // Stream table should exist
    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'va1b_emb_st')",
        )
        .await;
    assert!(
        exists,
        "embedding_stream_table() should create pgt_stream_tables entry"
    );
}

#[tokio::test]
async fn test_embedding_stream_table_legacy_l2_is_atomic_and_has_no_reindex_policy() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO helper_src VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let preview: Vec<(String,)> = sqlx::query_as(
        "SELECT action FROM pgtrickle.embedding_stream_table('helper_st', 'helper_src', 'embedding', NULL, '1m', 'hnsw', true)"
    ).fetch_all(&db.pool).await.expect("preview helper");
    assert_eq!(preview.len(), 2);
    assert!(preview[1].0.contains("vector_l2_ops"));
    assert!(!preview.iter().any(|row| row.0.contains("reindex_if_drift")));
    let absent: bool = db.query_scalar("SELECT to_regclass('public.helper_st') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'helper_st')").await;
    assert!(absent, "dry run must not create a stream table");

    db.execute("SELECT * FROM pgtrickle.embedding_stream_table('helper_st', 'helper_src', 'embedding', NULL, '1m', 'hnsw', false)").await;
    let index_name: String = db
        .query_scalar(
            "SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_am am ON am.oid = c.relam JOIN pg_opclass op ON op.oid = i.indclass[0] \
         WHERE i.indrelid = 'public.helper_st'::regclass AND i.indisvalid AND i.indisready \
         AND am.amname = 'hnsw' AND op.opcname = 'vector_l2_ops'",
        )
        .await;
    let policy: String = db.query_scalar("SELECT post_refresh_action::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'helper_st'").await;
    assert_eq!(policy, "none");
    let mut tx = db.pool.begin().await.expect("begin plan check");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable sequential scan");
    sqlx::query("SET LOCAL enable_bitmapscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable bitmap scan");
    sqlx::query("SET LOCAL enable_indexonlyscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable covering scan");
    let plan: serde_json::Value = sqlx::query_scalar(
        "EXPLAIN (FORMAT JSON) SELECT id FROM helper_st ORDER BY embedding::vector(3) <-> '[1,0,0]'::vector LIMIT 1"
    ).fetch_one(&mut *tx).await.expect("explain L2 query");
    assert!(
        plan.to_string()
            .contains(&format!("\"Index Name\":\"{index_name}\"")),
        "matching L2 index is not eligible: {plan}"
    );
    tx.rollback().await.expect("end plan check");
}

#[tokio::test]
async fn test_embedding_stream_table_v2_metrics_and_options() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE metric_source (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("INSERT INTO metric_source VALUES (1, '[1,0,0]'), (2, '[0,1,0]')")
        .await;
    let preview: Vec<(String,)> = sqlx::query_as(
        r#"SELECT action FROM pgtrickle.embedding_stream_table_v2('metric_cosine', 'metric_source', 'embedding', 'cosine', '{"columns":["id"],"hnsw_m":8,"hnsw_ef_construction":32}', true)"#,
    ).fetch_all(&db.pool).await.expect("preview cosine helper");
    assert_eq!(preview.len(), 2);
    assert!(preview[1].0.contains("vector_cosine_ops"));
    let absent: bool = db
        .query_scalar("SELECT to_regclass('public.metric_cosine') IS NULL")
        .await;
    assert!(absent);
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_cosine', 'metric_source', 'embedding', 'cosine', '{"columns":["id"],"hnsw_m":8,"hnsw_ef_construction":32}')"#).await;
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_ip', 'metric_source', 'embedding', 'inner_product', '{"index_type":"ivfflat","ivfflat_lists":1,"post_refresh_action":"analyze"}')"#).await;
    let cosine: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_opclass op ON op.oid=i.indclass[0] WHERE i.indrelid='metric_cosine'::regclass AND i.indisvalid AND i.indisready AND op.opcname='vector_cosine_ops' AND c.reloptions @> ARRAY['m=8','ef_construction=32'])").await;
    assert!(cosine);
    let inner_product: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_opclass op ON op.oid=i.indclass[0] WHERE i.indrelid='metric_ip'::regclass AND i.indisvalid AND i.indisready AND op.opcname='vector_ip_ops' AND c.reloptions @> ARRAY['lists=1'])").await;
    assert!(inner_product);
    let mut tx = db.pool.begin().await.expect("begin metric plan check");
    sqlx::query("SET LOCAL enable_seqscan = off")
        .execute(&mut *tx)
        .await
        .expect("disable sequential scan");
    for (table, opclass, query) in [
        (
            "metric_cosine",
            "vector_cosine_ops",
            "EXPLAIN (FORMAT JSON) SELECT id FROM metric_cosine ORDER BY embedding::vector(3) <=> '[1,0,0]'::vector LIMIT 1",
        ),
        (
            "metric_ip",
            "vector_ip_ops",
            "EXPLAIN (FORMAT JSON) SELECT id FROM metric_ip ORDER BY embedding::vector(3) <#> '[1,0,0]'::vector LIMIT 1",
        ),
    ] {
        let index: String = sqlx::query_scalar("SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_opclass op ON op.oid=i.indclass[0] WHERE i.indrelid=$1::regclass AND op.opcname=$2")
            .bind(table).bind(opclass).fetch_one(&mut *tx).await.expect("find metric index");
        let plan: serde_json::Value = sqlx::query_scalar(query)
            .fetch_one(&mut *tx)
            .await
            .expect("explain metric query");
        assert!(
            plan.to_string()
                .contains(&format!("\"Index Name\":\"{index}\"")),
            "matching index is not eligible: {plan}"
        );
    }
    tx.rollback().await.expect("end metric plan check");
    let policy: String = db.query_scalar("SELECT post_refresh_action::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name='metric_ip'").await;
    assert_eq!(policy, "analyze");
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_full', 'metric_source', 'embedding', 'l2', '{"refresh_mode":"FULL","refresh_interval":"1h"}')"#).await;
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('metric_immediate', 'metric_source', 'embedding', 'l2', '{"refresh_mode":"IMMEDIATE"}')"#).await;
    let modes: i64 = db.query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables WHERE (pgt_name='metric_full' AND refresh_mode='FULL') OR (pgt_name='metric_immediate' AND refresh_mode='IMMEDIATE')").await;
    assert_eq!(modes, 2);
    for options in [
        r#"{"unknown":1}"#,
        r#"{"columns":["missing"]}"#,
        r#"{"columns":["id","id"]}"#,
        r#"{"columns":["id); DROP TABLE metric_source; --"]}"#,
        r#"{"hnsw_m":1}"#,
        r#"{"hnsw_m":80}"#,
        r#"{"ivfflat_lists":2}"#,
        r#"{"index_type":"ivfflat","ivfflat_lists":0}"#,
        r#"{"refresh_mode":"IMMEDIATE","refresh_interval":"1m"}"#,
    ] {
        let sql = format!(
            "SELECT * FROM pgtrickle.embedding_stream_table_v2('invalid_metric_st', 'metric_source', 'embedding', 'cosine', '{options}', true)"
        );
        assert!(db.try_execute(&sql).await.is_err(), "accepted {options}");
    }
    assert!(db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table_v2('invalid_metric_st', 'metric_source', 'embedding', 'unknown', NULL, true)").await.is_err());
}

#[tokio::test]
async fn test_embedding_stream_table_index_conflict_rolls_back_creation() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_conflict_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    db.execute("CREATE TABLE helper_unrelated (id int)").await;
    let preview: String = db.query_scalar(
        "SELECT action FROM pgtrickle.embedding_stream_table('helper_conflict_st', 'helper_conflict_src', 'embedding', NULL, '1m', 'hnsw', true) WHERE action LIKE '-- 2.%'"
    ).await;
    let index_name = preview.split('"').nth(1).expect("preview index identifier");
    assert!(
        index_name.starts_with("pgt_ann_")
            && index_name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
    );
    db.execute(&format!(
        "CREATE INDEX {index_name} ON helper_unrelated (id)"
    ))
    .await;
    assert!(db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('helper_conflict_st', 'helper_conflict_src', 'embedding', NULL, '1m', 'hnsw', true)").await.is_err());
    let result = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('helper_conflict_st', 'helper_conflict_src', 'embedding', NULL, '1m', 'hnsw', false)").await;
    assert!(
        result.is_err(),
        "conflicting index must fail instead of claiming success"
    );
    let clean: bool = db.query_scalar("SELECT to_regclass('public.helper_conflict_st') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'helper_conflict_st') AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_change_tracking WHERE source_relid = 'helper_conflict_src'::regclass)").await;
    assert!(clean, "index failure must roll back stream and CDC state");
}

#[tokio::test]
async fn test_embedding_stream_table_starter_fixture_executes() {
    let db = E2eDb::new().await.with_extension().await;
    sqlx::raw_sql(include_str!("fixtures/pgvector_l2_starter.sql"))
        .execute(&db.pool)
        .await
        .expect("execute L2 starter SQL");
    let nearest: i64 = db
        .query_scalar(
            "SELECT id FROM doc_corpus ORDER BY embedding::vector(3) <-> '[1,0,0]'::vector LIMIT 1",
        )
        .await;
    assert_eq!(nearest, 1);
    let cosine_nearest: i64 = db.query_scalar("SELECT id FROM corpus_cosine ORDER BY embedding::vector(3) <=> '[1,0,0]'::vector LIMIT 1").await;
    assert_eq!(cosine_nearest, 1);
}

#[tokio::test]
async fn test_embedding_stream_table_quoted_names_and_vector_schema() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE SCHEMA \"V.X\"").await;
    db.execute("CREATE EXTENSION vector WITH SCHEMA \"V.X\"")
        .await;
    db.execute("CREATE SCHEMA \"A.B\"").await;
    db.execute("CREATE SCHEMA \"D.S\"").await;
    db.execute("CREATE TABLE \"A.B\".\"Doc.Source\" (id int PRIMARY KEY, \"Embedding Value\" \"V.X\".vector(3))").await;
    db.execute("INSERT INTO \"A.B\".\"Doc.Source\" VALUES (1, '[1,0,0]')")
        .await;
    db.execute("SELECT * FROM pgtrickle.embedding_stream_table('\"D.S\".\"Corpus.A\"', '\"A.B\".\"Doc.Source\"', '\"Embedding Value\"', NULL, '1h', 'hnsw', false)").await;
    db.execute(r#"SELECT * FROM pgtrickle.embedding_stream_table_v2('"D.S"."Cosine.Corpus"', '"A.B"."Doc.Source"', '"Embedding Value"', 'cosine', '{"columns":["id"]}')"#).await;
    let v2_valid: bool = db.query_scalar("SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_opclass op ON op.oid=i.indclass[0] JOIN pg_namespace n ON n.oid=op.opcnamespace WHERE i.indrelid='\"D.S\".\"Cosine.Corpus\"'::regclass AND i.indisvalid AND i.indisready AND op.opcname='vector_cosine_ops' AND n.nspname='V.X')").await;
    assert!(v2_valid);
    let valid: bool = db.query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_opclass op ON op.oid = i.indclass[0] JOIN pg_namespace n ON n.oid = op.opcnamespace \
         JOIN pg_am am ON am.oid = c.relam WHERE i.indrelid = '\"D.S\".\"Corpus.A\"'::regclass \
         AND i.indisvalid AND i.indisready AND am.amname = 'hnsw' \
         AND op.opcname = 'vector_l2_ops' AND n.nspname = 'V.X')"
    ).await;
    assert!(
        valid,
        "quoted relation and extension names must produce a valid L2 index"
    );
    let direct = db
        .try_execute(
            "INSERT INTO \"D.S\".\"Corpus.A\" (id, \"Embedding Value\") VALUES (3, '[0,0,1]')",
        )
        .await;
    assert!(
        direct.is_err(),
        "quoted stream table must retain its DML guard"
    );
    db.execute("INSERT INTO \"A.B\".\"Doc.Source\" VALUES (2, '[0,1,0]')")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('\"D.S\".\"Corpus.A\"')")
        .await;
    let rows: i64 = db
        .query_scalar("SELECT count(*) FROM \"D.S\".\"Corpus.A\"")
        .await;
    assert_eq!(rows, 2, "quoted stream must refresh after source changes");
}

#[tokio::test]
async fn test_embedding_stream_table_long_names_have_distinct_valid_indexes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_long_src (id int PRIMARY KEY, embedding vector(3))")
        .await;
    let prefix = "long_corpus_".to_string() + &"x".repeat(45);
    let first = format!("{prefix}a");
    let second = format!("{prefix}b");
    for name in [&first, &second] {
        db.execute(&format!(
            "SELECT * FROM pgtrickle.embedding_stream_table('{name}', 'helper_long_src', 'embedding')"
        ))
        .await;
    }
    let indexes: Vec<(String,)> = sqlx::query_as(
        "SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         JOIN pg_am am ON am.oid = c.relam WHERE i.indrelid IN ($1::regclass, $2::regclass) \
         AND am.amname = 'hnsw' AND i.indisvalid AND i.indisready ORDER BY c.relname",
    )
    .bind(&first)
    .bind(&second)
    .fetch_all(&db.pool)
    .await
    .expect("inspect long-name indexes");
    assert_eq!(indexes.len(), 2);
    assert_ne!(indexes[0].0, indexes[1].0);
    assert!(indexes.iter().all(|(name,)| name.len() <= 63));
}

#[tokio::test]
async fn test_embedding_stream_table_dry_run_rejects_invalid_source_types() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE scalar_source (id int PRIMARY KEY, embedding int)")
        .await;
    let no_extension = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('bad_st', 'scalar_source', 'embedding', dry_run => true)").await;
    assert!(
        no_extension.is_err(),
        "pgvector must be installed for a validated preview"
    );
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE undimensioned_source (id int PRIMARY KEY, embedding vector)")
        .await;
    db.execute("CREATE SCHEMA fake_vector").await;
    db.execute("CREATE DOMAIN fake_vector.vector AS text").await;
    db.execute("CREATE TABLE impostor_source (id int PRIMARY KEY, embedding fake_vector.vector)")
        .await;
    for source in [
        "missing_source",
        "scalar_source",
        "undimensioned_source",
        "impostor_source",
    ] {
        let sql = format!(
            "SELECT * FROM pgtrickle.embedding_stream_table('bad_st', '{source}', 'embedding', dry_run => true)"
        );
        assert!(
            db.try_execute(&sql).await.is_err(),
            "invalid source {source} must be rejected"
        );
    }
    let absent: bool = db.query_scalar("SELECT to_regclass('public.bad_st') IS NULL AND NOT EXISTS (SELECT 1 FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'bad_st')").await;
    assert!(absent, "invalid previews must not create objects");
}

#[tokio::test]
async fn test_embedding_stream_table_halfvec_and_sparsevec_indexes() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;
    db.execute("CREATE TABLE helper_half_src (id int PRIMARY KEY, embedding halfvec(3))")
        .await;
    db.execute("CREATE TABLE helper_sparse_src (id int PRIMARY KEY, embedding sparsevec(3))")
        .await;
    db.execute("INSERT INTO helper_half_src VALUES (1, '[1,0,0]')")
        .await;
    db.execute("INSERT INTO helper_sparse_src VALUES (1, '{1:1}/3')")
        .await;
    let unsupported = db.try_execute("SELECT * FROM pgtrickle.embedding_stream_table('sparse_bad', 'helper_sparse_src', 'embedding', index_type => 'ivfflat', dry_run => true)").await;
    assert!(unsupported.is_err(), "IVFFlat does not support sparsevec");
    for (source, stream, opclass) in [
        ("helper_half_src", "helper_half_st", "halfvec_l2_ops"),
        ("helper_sparse_src", "helper_sparse_st", "sparsevec_l2_ops"),
    ] {
        db.execute(&format!(
            "SELECT * FROM pgtrickle.embedding_stream_table('{stream}', '{source}', 'embedding')"
        ))
        .await;
        let valid: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
             JOIN pg_am am ON am.oid = c.relam JOIN pg_opclass op ON op.oid = i.indclass[0] \
             WHERE i.indrelid = '{stream}'::regclass AND i.indisvalid AND i.indisready \
             AND am.amname = 'hnsw' AND op.opcname = '{opclass}')"
            ))
            .await;
        assert!(valid, "{stream}: expected a valid {opclass} index");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// VA-4 (v0.48.0): attach_embedding_outbox() catalog registration
// ═══════════════════════════════════════════════════════════════════════

/// VA-4: attach_embedding_outbox() registers the embedding_vector_column.
#[tokio::test]
async fn test_attach_embedding_outbox_catalog_entry() {
    let db = E2eDb::new().await.with_extension().await;
    setup_pgvector(&db).await;

    db.execute(
        "CREATE TABLE va4_src (
            id        SERIAL PRIMARY KEY,
            embedding vector(3)
        )",
    )
    .await;
    db.execute("INSERT INTO va4_src (embedding) VALUES ('[1,0,0]'), ('[0,1,0]')")
        .await;
    db.create_st(
        "va4_emb_st",
        "SELECT id, embedding FROM va4_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.execute("CREATE EXTENSION pg_tide").await;

    db.execute("SELECT pgtrickle.attach_embedding_outbox('va4_emb_st', 'embedding')")
        .await;

    let vec_col: Option<String> = db
        .query_scalar_opt(
            "SELECT embedding_vector_column FROM pgtrickle.pgt_outbox_config \
             WHERE stream_table_name = 'public.va4_emb_st'",
        )
        .await;
    assert_eq!(
        vec_col,
        Some("embedding".to_string()),
        "attach_embedding_outbox should store embedding_vector_column"
    );
}
