use super::e2e::E2eDb;
use std::{env, fs, path::PathBuf, process::Command};

const LIBRARY_PATH: &str = "/usr/lib/postgresql/18/lib/pg_trickle.so";

pub async fn preflight(db: &E2eDb, expected_case: &str) -> bool {
    let Ok(case) = env::var("PGT_DVM_MUTATION_CASE") else {
        return false;
    };
    assert_eq!(
        case, expected_case,
        "wrong E2E test selected for mutation case"
    );

    let expected_sha = env::var("PGT_DVM_MUTATION_EXPECTED_LIBRARY_SHA256")
        .expect("mutation campaign must provide the expected library SHA-256");
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire backend for loaded-library attestation");
    sqlx::query("LOAD 'pg_trickle'")
        .execute(&mut *conn)
        .await
        .expect("load pg_trickle in attested backend");
    let (backend_pid, library_symbol, library_search_path, bound_functions): (
        i32,
        String,
        String,
        i64,
    ) = sqlx::query_as(
        "SELECT pg_backend_pid(), min(probin), current_setting('dynamic_library_path'), count(*) \
         FROM pg_proc \
         WHERE pronamespace = 'pgtrickle'::regnamespace AND probin IS NOT NULL",
    )
    .fetch_one(&mut *conn)
    .await
    .expect("read backend identity and extension library binding");
    assert_eq!(library_symbol, "pg_trickle");
    assert_eq!(library_search_path, "$libdir");
    assert!(bound_functions > 0, "extension library has no SQL bindings");
    drop(conn);
    let inspect = format!("sha256sum '{LIBRARY_PATH}'");
    let output = Command::new("docker")
        .args([
            "exec",
            "--user",
            "0:0",
            db.container_id(),
            "sh",
            "-ec",
            &inspect,
        ])
        .output()
        .expect("run Docker command for loaded-library attestation");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "DVM_MUTATION_ROUTE_FAILURE: could not hash PostgreSQL library {LIBRARY_PATH}; stdout={stdout}; stderr={stderr}"
    );
    let loaded_sha = stdout
        .lines()
        .last()
        .and_then(|line| line.split_whitespace().next())
        .expect("sha256sum output for installed extension library");
    assert_eq!(
        loaded_sha, expected_sha,
        "DVM_MUTATION_STALE_BINARY: loaded extension SHA-256 differs from the candidate image"
    );

    let artifact_dir = artifact_dir();
    fs::create_dir_all(&artifact_dir).expect("create mutation artifact directory");
    let runtime_image = env::var("PGS_E2E_IMAGE").unwrap_or_default();
    let package_dir = env::var("PGT_E2E_MUTATION_PACKAGE_DIR").unwrap_or_default();
    let loaded_evidence = format!(
        "container_id={}\nruntime_image={runtime_image}\npackage_dir={package_dir}\nbackend_pid={backend_pid}\nlibrary_symbol={library_symbol}\nlibrary_search_path={library_search_path}\nresolved_path={LIBRARY_PATH}\nload=success\nlibrary_sha256={loaded_sha}\nsha256sum={}",
        db.container_id(),
        stdout.trim()
    );
    fs::write(artifact_dir.join("loaded-library.txt"), loaded_evidence)
        .expect("save loaded-library attestation");
    let server_version: String = sqlx::query_scalar("SELECT current_setting('server_version')")
        .fetch_one(&db.pool)
        .await
        .expect("read PostgreSQL version");
    println!(
        "DVM_MUTATION_PREFLIGHT case={case} server_version={server_version} backend_pid={backend_pid} library_sha256={loaded_sha} library_symbol={library_symbol} library_search_path={library_search_path} resolved_path={LIBRARY_PATH} load=success artifact={} ",
        artifact_dir.display()
    );
    assert!(
        env::var_os("PGT_DVM_MUTATION_FORCE_FAILURE").is_none(),
        "DVM_MUTATION_FORCED_CLEANUP_FAILURE: injected after library attestation"
    );
    true
}

pub async fn record_target_bags(db: &E2eDb, stream_table: &str, query: &str, context: &str) {
    assert!(
        stream_table
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_'),
        "mutation oracle table must be a simple identifier"
    );
    let table = format!("public.\"{stream_table}\"");
    let bag_sql = |source: &str| {
        format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(bag_row) ORDER BY to_jsonb(bag_row)::text), '[]'::jsonb)::text \
             FROM ({source}) AS bag_row"
        )
    };
    // The identifier is validated above and the defining query comes from the test fixture.
    let actual_sql = bag_sql(&format!("SELECT * FROM {table}"));
    let actual: String = sqlx::query_scalar(sqlx::AssertSqlSafe(actual_sql))
        .fetch_one(&db.pool)
        .await
        .expect("capture exact stream-table result bag");
    let expected: String = sqlx::query_scalar(sqlx::AssertSqlSafe(bag_sql(query)))
        .fetch_one(&db.pool)
        .await
        .expect("capture exact defining-query result bag");
    let evidence_dir = artifact_dir();
    fs::create_dir_all(&evidence_dir).expect("create exact-bag artifact directory");
    fs::write(
        evidence_dir.join("expected-actual-bags.txt"),
        format!("context={context}\nexpected={expected}\nactual={actual}\n"),
    )
    .expect("save exact expected and actual row bags");
}

pub async fn create_differential_stream_table(db: &E2eDb, name: &str, query: &str) {
    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire connection for explicit differential setup");
    for setting in [
        "LOAD 'pg_trickle'",
        "SET pg_trickle.cdc_mode = 'trigger'",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SET pg_trickle.aggregate_fast_path = off",
    ] {
        sqlx::query(setting)
            .execute(&mut *conn)
            .await
            .unwrap_or_else(|error| panic!("set differential preflight config {setting}: {error}"));
    }
    sqlx::query("SELECT pgtrickle.create_stream_table($1, $2, '1m', 'DIFFERENTIAL')")
        .bind(name)
        .bind(query)
        .execute(&mut *conn)
        .await
        .unwrap_or_else(|error| {
            panic!("create explicit DIFFERENTIAL stream table {name}: {error}")
        });
    let mode: String = sqlx::query_scalar(
        "SELECT refresh_mode::text FROM pgtrickle.pgt_stream_tables WHERE pgt_name = $1",
    )
    .bind(name)
    .fetch_one(&mut *conn)
    .await
    .expect("read admitted stream-table refresh mode");
    assert_eq!(
        mode, "DIFFERENTIAL",
        "public API did not admit DIFFERENTIAL"
    );
}

pub async fn refresh_target(db: &E2eDb, name: &str, expected_case: &str) {
    let case = env::var("PGT_DVM_MUTATION_CASE").expect("campaign case is set by the pilot runner");
    assert_eq!(
        case, expected_case,
        "wrong target refresh for mutation case"
    );
    let expected_sql = env::var("PGT_DVM_MUTATION_EXPECTED_SQL_FRAGMENT")
        .expect("mutation campaign must specify the expected generated SQL fragment");

    let mut conn = db
        .pool
        .acquire()
        .await
        .expect("acquire connection for targeted differential refresh");
    for setting in [
        "LOAD 'pg_trickle'",
        "SET pg_trickle.cdc_mode = 'trigger'",
        "SET pg_trickle.refresh_strategy = 'differential'",
        "SET pg_trickle.differential_max_change_ratio = 0",
        "SET pg_trickle.aggregate_fast_path = off",
        "SET pg_trickle.log_delta_sql = on",
        "SET log_min_messages = 'debug1'",
    ] {
        sqlx::query(setting)
            .execute(&mut *conn)
            .await
            .unwrap_or_else(|error| panic!("set targeted refresh config {setting}: {error}"));
    }
    let settings: (String, String, String, String, String, String, String) = sqlx::query_as(
        "SELECT current_setting('server_version'), current_setting('pg_trickle.cdc_mode'), \
                current_setting('pg_trickle.refresh_strategy'), \
                current_setting('pg_trickle.differential_max_change_ratio'), \
                current_setting('pg_trickle.aggregate_fast_path'), \
                current_setting('pg_trickle.log_delta_sql'), \
                current_setting('log_min_messages')",
    )
    .fetch_one(&mut *conn)
    .await
    .expect("capture targeted refresh configuration");

    sqlx::query("SELECT pgtrickle.refresh_stream_table($1)")
        .bind(name)
        .execute(&mut *conn)
        .await
        .unwrap_or_else(|error| panic!("DVM_MUTATION_REFRESH_FAILURE: refresh {name}: {error}"));

    let history: (String, String, bool, i64, String) = sqlx::query_as(
        "SELECT h.action::text, h.status::text, h.was_full_fallback, h.delta_row_count, \
                st.effective_refresh_mode::text \
         FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
         WHERE st.pgt_name = $1 ORDER BY h.refresh_id DESC LIMIT 1",
    )
    .bind(name)
    .fetch_one(&mut *conn)
    .await
    .expect("read target refresh history");
    assert_eq!(history.0, "DIFFERENTIAL", "target refresh action");
    assert_eq!(history.1, "COMPLETED", "target refresh status");
    assert!(!history.2, "target refresh fell back to FULL");
    assert!(history.3 > 0, "target refresh did no changed work");
    assert_eq!(history.4, "DIFFERENTIAL", "effective refresh mode");

    let output = Command::new("docker")
        .args(["logs", db.container_id()])
        .output()
        .expect("read PostgreSQL logs for generated delta SQL");
    let mut logs = String::from_utf8_lossy(&output.stdout).into_owned();
    logs.push_str(&String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success(), "read PostgreSQL logs: {logs}");
    let artifact_dir = artifact_dir();
    fs::create_dir_all(&artifact_dir).expect("create mutation artifact directory");
    fs::write(artifact_dir.join("target-delta.log"), logs.as_bytes())
        .expect("save generated delta SQL log");
    assert!(
        logs.contains(&expected_sql),
        "DVM_MUTATION_ROUTE_FAILURE: generated delta SQL did not contain target fragment {expected_sql:?}; saved logs to {}",
        artifact_dir.join("target-delta.log").display()
    );
    if case == "having-unknown-retraction" {
        assert!(
            logs.contains("aggregate updates that no longer satisfy HAVING"),
            "DVM_MUTATION_ROUTE_FAILURE: generated delta SQL omitted the HAVING retraction arm"
        );
    }

    let (version, cdc, strategy, change_ratio, fast_path, log_delta_sql, log_min_messages) =
        settings;
    let evidence = format!(
        "case={case}\nstream_table={name}\nserver_version={version}\ncdc_mode={cdc}\nrefresh_strategy={strategy}\ndifferential_max_change_ratio={change_ratio}\naggregate_fast_path={fast_path}\nlog_delta_sql={log_delta_sql}\nlog_min_messages={log_min_messages}\naction={}\nstatus={}\nwas_full_fallback={}\ndelta_row_count={}\neffective_refresh_mode={}\nexpected_sql_fragment={expected_sql}\npostgres_logs=target-delta.log\n",
        history.0, history.1, history.2, history.3, history.4,
    );
    fs::write(artifact_dir.join("target-refresh.txt"), evidence)
        .expect("save target refresh evidence");
    println!(
        "DVM_MUTATION_ROUTE case={case} action={} status={} fallback={} delta_rows={} effective_mode={} sql_log={} evidence={}",
        history.0,
        history.1,
        history.2,
        history.3,
        history.4,
        artifact_dir.join("target-delta.log").display(),
        artifact_dir.join("target-refresh.txt").display(),
    );
}

fn artifact_dir() -> PathBuf {
    PathBuf::from(
        env::var_os("PGT_DVM_MUTATION_ARTIFACT_DIR")
            .expect("mutation campaign must provide an artifact directory"),
    )
}
