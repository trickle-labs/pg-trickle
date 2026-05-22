//! E2E tests for the DuckLake sink write path (v0.66.0 / v0.68.0).
//!
//! Tests cover:
//!   - v0.66.0 (F-2/F-4): Parquet file written and catalog registered
//!     after scheduler refresh.
//!   - v0.66.0 (Release Gate): Catalog is NOT modified when upload fails
//!     (rollback invariant E2E proof).
//!   - v0.68.0 (COR-004): Timestamp/timestamptz columns survive the sink
//!     write path without becoming NULL.
//!
//! These tests require the background scheduler (`shared_preload_libraries`)
//! and can only run in the full E2E harness (`just test-e2e`).

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helpers ───────────────────────────────────────────────────────────────

/// DuckLake catalog tables DDL — individual statements.
///
/// These are the minimal stubs of the DuckLake catalog tables that
/// `register_ducklake_data_file()` writes into.  A real DuckLake deployment
/// would have additional columns and constraints; we only create what the
/// pg_trickle sink needs.
///
/// Each DDL statement is a separate entry because `sqlx::query()` (used by
/// `E2eDb::execute`) is a prepared statement and cannot execute multiple
/// commands at once.  Use `db.execute_seq(DUCKLAKE_CATALOG_DDL)` to run them.
const DUCKLAKE_CATALOG_DDL: &[&str] = &[
    // SEC-002 (v0.69.0): the sink writes to pg_trickle.ducklake_catalog_schema
    // which defaults to "main".  Create the schema first so all catalog tables
    // land there instead of in "public".
    "CREATE SCHEMA IF NOT EXISTS main",
    "CREATE TABLE IF NOT EXISTS main.ducklake_data_file (\
        data_file_id    BIGSERIAL PRIMARY KEY,\
        table_id        BIGINT NOT NULL,\
        begin_snapshot  BIGINT NOT NULL,\
        path            TEXT,\
        row_count       BIGINT,\
        file_size_bytes BIGINT,\
        encryption_key_id TEXT\
    )",
    "CREATE TABLE IF NOT EXISTS main.ducklake_table_stats (\
        table_id   BIGINT PRIMARY KEY,\
        row_count  BIGINT NOT NULL DEFAULT 0,\
        file_count BIGINT NOT NULL DEFAULT 0\
    )",
    "CREATE TABLE IF NOT EXISTS main.ducklake_snapshot (\
        table_id      BIGINT NOT NULL,\
        snapshot_id   BIGINT NOT NULL,\
        snapshot_time TIMESTAMPTZ NOT NULL DEFAULT now(),\
        created_by    TEXT,\
        PRIMARY KEY (table_id, snapshot_id)\
    )",
];

/// Configure the scheduler for fast-cycling tests (100 ms interval).
async fn configure_fast_scheduler(db: &E2eDb) {
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "100")
        .await;
    db.wait_for_setting("pg_trickle.min_schedule_seconds", "1")
        .await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(
        sched_running,
        "pg_trickle scheduler did not start within 90 s"
    );
}

/// Wait until the stream table `pgt_name` has at least `min_count` COMPLETED
/// refresh history rows.
async fn wait_for_n_refreshes(
    db: &E2eDb,
    pgt_name: &str,
    min_count: i64,
    timeout: Duration,
) -> i64 {
    let start = std::time::Instant::now();
    loop {
        let count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
                 WHERE d.pgt_name = '{pgt_name}' AND h.status = 'COMPLETED'"
            ))
            .await;
        if count >= min_count {
            return count;
        }
        if start.elapsed() > timeout {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Wait until `main.ducklake_data_file` has at least one row for `table_id`.
///
/// `create_stream_table` runs an initial MANUAL refresh (and records a
/// COMPLETED row in `pgt_refresh_history`) *before* persisting the DuckLake
/// sink configuration.  `wait_for_n_refreshes` can therefore return after
/// seeing that early MANUAL record — before the scheduler has had a chance to
/// run the first SCHEDULED refresh (the one that actually drives the sink).
///
/// Polling `ducklake_data_file` directly avoids the race: the scheduler
/// writes the catalog row in the same transaction as the COMPLETED history
/// record, so visibility is guaranteed once the row appears.
async fn wait_for_ducklake_data_file(db: &E2eDb, table_id: i64, timeout: Duration) -> i64 {
    let start = std::time::Instant::now();
    loop {
        let count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM main.ducklake_data_file \
                 WHERE table_id = {table_id}"
            ))
            .await;
        if count >= 1 {
            return count;
        }
        if start.elapsed() > timeout {
            return count;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

/// v0.66.0 (F-2/F-4): After a successful scheduler refresh, the DuckLake sink
/// writes a Parquet file to the configured `ducklake_sink_path` and registers
/// the file in the `ducklake_data_file` catalog table.
///
/// This test uses a `file://` path inside the container's `/tmp/` directory
/// as a proxy for S3/MinIO — the upload logic is identical; only the transport
/// layer differs.  The catalog registration proves the file was written first
/// (upload-before-catalog ordering).
#[tokio::test]
async fn test_ducklake_sink_parquet_file_written_after_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Create the minimal DuckLake catalog tables the sink writes into.
    db.execute_seq(DUCKLAKE_CATALOG_DDL).await;

    // Source table with initial rows.
    db.execute("CREATE TABLE dl_sink_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO dl_sink_src VALUES (1, 'alpha'), (2, 'beta')")
        .await;

    // Stream table with the DuckLake sink configured.
    // ducklake_sink_table_id = 1 points to the DuckLake table we want to
    // register files against.
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             'dl_sink_write_st', \
             'SELECT id, val FROM dl_sink_src', \
             '1s', 'FULL', \
             sink => 'ducklake', \
             ducklake_sink_path => 'file:///tmp/ducklake_sink_write_test/', \
             ducklake_sink_table_id => 1\
         )",
    )
    .await;

    // Wait for at least 2 COMPLETED refreshes: the first is the initial population
    // (created by initialize_st before update_sink_config is called, so no ducklake
    // sink runs then). The second is the first scheduler-triggered refresh that
    // actually calls run_ducklake_sink with the configured sink path/table_id.
    let refreshes = wait_for_n_refreshes(&db, "dl_sink_write_st", 2, Duration::from_secs(60)).await;
    assert!(
        refreshes >= 2,
        "dl_sink_write_st must have at least 2 COMPLETED refreshes, got {refreshes}"
    );

    // Wait for the DuckLake sink to write at least one catalog entry.
    //
    // `create_stream_table` records a COMPLETED refresh row in
    // `pgt_refresh_history` during its initial MANUAL refresh — *before* the
    // DuckLake sink configuration is persisted.  `wait_for_n_refreshes` can
    // therefore return immediately after seeing that early record, before the
    // scheduler has executed the first SCHEDULED refresh (which drives the
    // sink).  Polling `ducklake_data_file` directly avoids the race.
    let file_count = wait_for_ducklake_data_file(&db, 1, Duration::from_secs(60)).await;
    assert!(
        file_count >= 1,
        "Expected at least 1 ducklake_data_file row for table_id=1, \
         but got {file_count}. \
         Sink may not have run or the catalog write was skipped."
    );

    // Verify the registered path starts with the expected prefix.
    let path: Option<String> = db
        .query_scalar(
            "SELECT path FROM main.ducklake_data_file \
             WHERE table_id = 1 ORDER BY data_file_id LIMIT 1",
        )
        .await;
    let path = path.expect("ducklake_data_file.path must be non-NULL");
    assert!(
        path.starts_with("file:///tmp/ducklake_sink_write_test/"),
        "Expected path to start with 'file:///tmp/ducklake_sink_write_test/', \
         got: '{path}'"
    );

    // The snapshot table must also have a row.
    let snap_count: i64 = db
        .query_scalar("SELECT count(*) FROM main.ducklake_snapshot WHERE table_id = 1")
        .await;
    assert!(
        snap_count >= 1,
        "Expected at least 1 ducklake_snapshot row, got {snap_count}"
    );

    // The provenance table must record the write (v0.67.0 INT-11).
    let prov_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_ducklake_provenance \
             WHERE stream_table_name = 'dl_sink_write_st'",
        )
        .await;
    assert!(
        prov_count >= 1,
        "Expected at least 1 provenance row for dl_sink_write_st, got {prov_count}"
    );
}

/// v0.66.0 (Release Gate): When the DuckLake sink upload fails, the catalog
/// (`ducklake_data_file` / `ducklake_snapshot`) is NOT modified.
///
/// This is the E2E proof of the rollback invariant described in the module
/// docstring of `src/ducklake_sink.rs`: upload is attempted _before_ any
/// catalog write, and a failed upload short-circuits via `?`, so
/// `register_ducklake_data_file` is never called.
///
/// We use `gs://` (Google Cloud Storage) as the sink path. pg_trickle does
/// not bundle a GCS object-store driver in its default build, so this scheme
/// always returns `DucklakeUploadError("not yet supported")` — a deterministic,
/// fast failure that exercises the same code path as any S3/network error.
#[tokio::test]
async fn test_ducklake_sink_catalog_not_modified_when_upload_fails() {
    let db = E2eDb::new().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Create the minimal DuckLake catalog tables.
    db.execute_seq(DUCKLAKE_CATALOG_DDL).await;

    // Source table with data so the sink actually tries to write something.
    db.execute("CREATE TABLE dl_sink_fail_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO dl_sink_fail_src VALUES (1, 'row1')")
        .await;

    // Stream table pointing at a GCS path — this WILL fail on upload.
    // table_id = 2 (distinct from the write test so no cross-test interference).
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             'dl_sink_fail_st', \
             'SELECT id, val FROM dl_sink_fail_src', \
             '1s', 'FULL', \
             sink => 'ducklake', \
             ducklake_sink_path => 'gs://fake-bucket/prefix/', \
             ducklake_sink_table_id => 2\
         )",
    )
    .await;

    // Wait for at least one refresh to complete.
    let refreshes = wait_for_n_refreshes(&db, "dl_sink_fail_st", 1, Duration::from_secs(60)).await;
    assert!(
        refreshes >= 1,
        "dl_sink_fail_st must have at least 1 COMPLETED refresh, got {refreshes}. \
         Sink failure must NOT block the refresh cycle."
    );

    // The stream table must remain ACTIVE — sink failures are warnings, not errors.
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'dl_sink_fail_st'",
        )
        .await;
    assert_eq!(
        status, "ACTIVE",
        "Stream table must stay ACTIVE even when sink upload fails"
    );

    // CRITICAL: ducklake_data_file and ducklake_snapshot must have ZERO rows
    // for table_id=2, proving the catalog was never touched when upload failed.
    let file_count: i64 = db
        .query_scalar("SELECT count(*) FROM main.ducklake_data_file WHERE table_id = 2")
        .await;
    assert_eq!(
        file_count, 0,
        "ducklake_data_file must have 0 rows for table_id=2 when upload fails \
         (rollback invariant violated: got {file_count} rows)"
    );

    let snap_count: i64 = db
        .query_scalar("SELECT count(*) FROM main.ducklake_snapshot WHERE table_id = 2")
        .await;
    assert_eq!(
        snap_count, 0,
        "ducklake_snapshot must have 0 rows for table_id=2 when upload fails \
         (rollback invariant violated: got {snap_count} rows)"
    );
}

/// v0.68.0 (COR-004): Timestamp and timestamptz columns must not become NULL
/// in the Parquet output after a sink write.
///
/// The fix in `fetch_stream_table_rows()` emits `EXTRACT(EPOCH FROM col::timestamptz) * 1000000`
/// for timestamp columns instead of `col::text`, so `write_parquet_bytes()`
/// receives a parseable i64 value.  Without the fix, the text cast yields a
/// locale-sensitive string that cannot be parsed as i64, causing silent NULL
/// coercion.
///
/// We verify indirectly: a successful catalog registration with `row_count > 0`
/// means the Parquet file was written without errors.  A NULL coercion bug
/// would either raise a parse error or produce a file with all-NULL timestamps
/// (which would still register in the catalog, but with incorrect data — that
/// deeper check is left for a dedicated Parquet reader test).
#[tokio::test]
async fn test_ducklake_sink_timestamp_roundtrip() {
    let db = E2eDb::new().await.with_extension().await;
    configure_fast_scheduler(&db).await;

    // Create the minimal DuckLake catalog tables.
    db.execute_seq(DUCKLAKE_CATALOG_DDL).await;

    // Source table with a timestamptz column.
    db.execute(
        "CREATE TABLE dl_ts_src (\
             id          INT PRIMARY KEY, \
             event_time  TIMESTAMPTZ NOT NULL\
         )",
    )
    .await;
    db.execute(
        "INSERT INTO dl_ts_src VALUES \
         (1, '2024-01-15 12:30:00+00'), \
         (2, '2024-06-01 00:00:00+00')",
    )
    .await;

    // Stream table with sink.  table_id = 3 for isolation.
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
             'dl_ts_sink_st', \
             'SELECT id, event_time FROM dl_ts_src', \
             '1s', 'FULL', \
             sink => 'ducklake', \
             ducklake_sink_path => 'file:///tmp/ducklake_ts_test/', \
             ducklake_sink_table_id => 3\
         )",
    )
    .await;

    // Wait for at least 2 COMPLETED refreshes: the first is the initial population
    // (before the sink config is active), the second is the first scheduler-triggered
    // refresh that calls run_ducklake_sink and registers the Parquet file.
    let refreshes = wait_for_n_refreshes(&db, "dl_ts_sink_st", 2, Duration::from_secs(60)).await;
    assert!(
        refreshes >= 2,
        "dl_ts_sink_st must have at least 2 COMPLETED refreshes, got {refreshes}"
    );

    // Wait for the DuckLake sink to write at least one catalog entry before
    // reading row_count.  The initial MANUAL refresh (recorded before the sink
    // config is persisted) causes wait_for_n_refreshes to return early; the
    // scheduler-driven refresh (which runs the sink) may still be in flight.
    wait_for_ducklake_data_file(&db, 3, Duration::from_secs(60)).await;

    // Verify catalog registration succeeded with row_count = 2.
    // A row_count of 0 or NULL would indicate all rows were dropped/nullified.
    let row_count: Option<i64> = db
        .query_scalar(
            "SELECT row_count FROM main.ducklake_data_file \
             WHERE table_id = 3 ORDER BY data_file_id LIMIT 1",
        )
        .await;
    let row_count = row_count.expect("ducklake_data_file must have a row for table_id=3");
    assert_eq!(
        row_count, 2,
        "Expected 2 rows written to Parquet (COR-004: timestamps must not be \
         silently NULLed), got {row_count}"
    );
}
