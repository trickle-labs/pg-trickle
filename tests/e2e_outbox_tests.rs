//! E2E tests for the pg_tide outbox integration (v0.46.0).
//!
//! Covers:
//! - TIDE-7: attach_outbox() — registers a pg_tide outbox for a stream table
//! - TIDE-7: detach_outbox() — removes the catalog entry
//! - TIDE-3: PgTideMissing error when pg_tide is not installed
//! - TIDE-4: OutboxAlreadyEnabled error on duplicate attach
//! - TIDE-5: OutboxNotEnabled error on detach without attach
//!
//! The full E2E image includes a SQL-only pg_tide contract stub. Tests create
//! it explicitly so absent-extension cases remain meaningful.

mod e2e;

use e2e::E2eDb;

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Create a minimal DIFFERENTIAL stream table for outbox integration tests.
async fn make_outbox_st(db: &E2eDb, src: &str, st: &str) {
    db.execute(&format!(
        "CREATE TABLE {src} (id INT PRIMARY KEY, val TEXT)"
    ))
    .await;
    db.execute(&format!(
        "INSERT INTO {src} VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .await;
    db.create_st(
        st,
        &format!("SELECT id, val FROM {src}"),
        "1m",
        "DIFFERENTIAL",
    )
    .await;
}

/// Install a minimal pg_tide stub so attach_outbox() can call
/// tide.outbox_create() without the real extension being installed.
async fn install_pg_tide_stub(db: &E2eDb) {
    db.execute("CREATE EXTENSION pg_tide").await;
}

async fn install_pg_tide_version(db: &E2eDb, version: &str) {
    db.execute(&format!("CREATE EXTENSION pg_tide VERSION '{version}'"))
        .await;
}

// ══════════════════════════════════════════════════════════════════════════════
// TIDE-3: PgTideMissing — attach_outbox fails when pg_tide is absent
// ══════════════════════════════════════════════════════════════════════════════

/// TIDE-3a: attach_outbox() raises an error when pg_tide is not installed.
#[tokio::test]
async fn test_attach_outbox_fails_without_pg_tide() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob3a_src", "ob3a_st").await;

    // Do NOT install the pg_tide stub — pg_tide should be absent.
    let result = db
        .try_execute("SELECT pgtrickle.attach_outbox('ob3a_st')")
        .await;

    assert!(
        result.is_err(),
        "attach_outbox() should fail when pg_tide is not installed"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("pg_tide") || err.contains("tide"),
        "Error should mention pg_tide; got: {err}"
    );
}

/// LSEC-21: supported compatibility is explicit, not inferred from a callable
/// function name alone.
#[tokio::test]
async fn test_attach_outbox_rejects_unsupported_older_pg_tide() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob21_old_src", "ob21_old_st").await;
    install_pg_tide_version(&db, "0.46.0").await;

    let err = db
        .try_execute("SELECT pgtrickle.attach_outbox('ob21_old_st')")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("unsupported") && err.contains("0.46.0"));
}

/// LSEC-21: newer pg_tide versions are rejected until the compatibility
/// contract is intentionally widened.
#[tokio::test]
async fn test_attach_outbox_rejects_unsupported_newer_pg_tide() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob21_new_src", "ob21_new_st").await;
    install_pg_tide_version(&db, "0.54.0").await;

    let err = db
        .try_execute("SELECT pgtrickle.attach_outbox('ob21_new_st')")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("unsupported") && err.contains("0.54.0"));
}

/// LSEC-21: a supported extension with an incomplete outbox catalog is
/// reported as an upgrade-in-progress state.
#[tokio::test]
async fn test_attach_outbox_rejects_incomplete_pg_tide_upgrade() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob21_upgrade_src", "ob21_upgrade_st").await;
    install_pg_tide_stub(&db).await;
    db.execute("ALTER EXTENSION pg_tide DROP TABLE tide.tide_outbox_config")
        .await;
    db.execute("DROP TABLE tide.tide_outbox_config").await;

    let err = db
        .try_execute("SELECT pgtrickle.attach_outbox('ob21_upgrade_st')")
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("not ready") && err.contains("tide.tide_outbox_config"));
}

/// LSEC-19: pg_tide observes the original stream owner, not pg_trickle's
/// extension owner, during the external create call.
#[tokio::test]
async fn test_attach_outbox_calls_pg_tide_as_original_caller() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob19_identity_src", "ob19_identity_st").await;
    install_pg_tide_stub(&db).await;
    db.execute_seq(&[
        "CREATE ROLE ob19_identity_owner LOGIN",
        "GRANT USAGE ON SCHEMA pgtrickle, tide TO ob19_identity_owner",
        "GRANT SELECT ON ob19_identity_src TO ob19_identity_owner",
        "GRANT INSERT ON tide.tide_outbox_config, tide.outbox_caller_log TO ob19_identity_owner",
        "GRANT SELECT ON tide.tide_outbox_config TO ob19_identity_owner",
        "GRANT EXECUTE ON FUNCTION pgtrickle.attach_outbox(text, integer, integer) TO ob19_identity_owner",
        "ALTER TABLE ob19_identity_st OWNER TO ob19_identity_owner",
    ])
    .await;

    db.try_execute_with_role(
        "SET ROLE ob19_identity_owner",
        "SELECT pgtrickle.attach_outbox('ob19_identity_st')",
        "RESET ROLE",
    )
    .await
    .expect("caller-owned attach should succeed");

    let observed: String = db
        .query_scalar("SELECT caller_name FROM tide.outbox_caller_log LIMIT 1")
        .await;
    assert_eq!(observed, "ob19_identity_owner");
}

/// LSEC-21: pg_tide permission failures are surfaced as denied operations and
/// do not leave a private mapping behind.
#[tokio::test]
async fn test_attach_outbox_surfaces_pg_tide_denial() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob21_denied_src", "ob21_denied_st").await;
    install_pg_tide_stub(&db).await;
    db.execute_seq(&[
        "CREATE ROLE ob21_denied_owner LOGIN",
        "GRANT USAGE ON SCHEMA pgtrickle, tide TO ob21_denied_owner",
        "GRANT SELECT ON ob21_denied_src TO ob21_denied_owner",
        "GRANT SELECT ON tide.tide_outbox_config TO ob21_denied_owner",
        "GRANT EXECUTE ON FUNCTION pgtrickle.attach_outbox(text, integer, integer) TO ob21_denied_owner",
        "REVOKE EXECUTE ON FUNCTION tide.outbox_create(text, integer, integer) FROM PUBLIC",
        "ALTER TABLE ob21_denied_st OWNER TO ob21_denied_owner",
    ])
    .await;

    let result = db
        .try_execute_with_role(
            "SET ROLE ob21_denied_owner",
            "SELECT pgtrickle.attach_outbox('ob21_denied_st')",
            "RESET ROLE",
        )
        .await;
    let err = result.unwrap_err().to_string();
    assert!(err.contains("denied") && err.contains("outbox_create"));
    let mapped: bool = db
        .query_scalar(
            "SELECT EXISTS ( \
               SELECT 1 FROM pgtrickle.pgt_outbox_config \
                WHERE stream_table_name = 'public.ob21_denied_st' \
             )",
        )
        .await;
    assert!(!mapped, "denied attach must not create a private mapping");
}

// ══════════════════════════════════════════════════════════════════════════════
// TIDE-7: attach_outbox — catalog registration
// ══════════════════════════════════════════════════════════════════════════════

/// TIDE-7a: attach_outbox() registers the stream table in pgt_outbox_config.
#[tokio::test]
async fn test_attach_outbox_creates_catalog_entry() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob7a_src", "ob7a_st").await;
    install_pg_tide_stub(&db).await;

    db.execute("SELECT pgtrickle.attach_outbox('ob7a_st')")
        .await;

    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_outbox_config \
             WHERE stream_table_name = 'public.ob7a_st')",
        )
        .await;
    assert!(
        exists,
        "pgt_outbox_config entry should be created after attach_outbox()"
    );
}

/// TIDE-7b: attach_outbox() stores the correct tide_outbox_name.
#[tokio::test]
async fn test_attach_outbox_stores_tide_outbox_name() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob7b_src", "ob7b_st").await;
    install_pg_tide_stub(&db).await;

    db.execute("SELECT pgtrickle.attach_outbox('ob7b_st')")
        .await;

    let outbox_name: String = db
        .query_scalar(
            "SELECT tide_outbox_name FROM pgtrickle.pgt_outbox_config \
             WHERE stream_table_name = 'public.ob7b_st'",
        )
        .await;

    assert_eq!(
        outbox_name, "outbox_ob7b_st",
        "tide_outbox_name should follow the 'outbox_<st_name>' convention"
    );
}

/// TIDE-19: attach_outbox() stores immutable pg_tide identity provenance.
#[tokio::test]
async fn test_attach_outbox_stores_pg_tide_provenance() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob19_src", "ob19_st").await;
    install_pg_tide_stub(&db).await;

    db.execute("SELECT pgtrickle.attach_outbox('ob19_st')")
        .await;

    let matches: bool = db
        .query_scalar(
            "SELECT EXISTS ( \
               SELECT 1 \
                 FROM pgtrickle.pgt_outbox_config oc \
                 JOIN pg_catalog.pg_extension e \
                   ON e.oid = oc.pg_tide_extension_oid \
                WHERE oc.stream_table_name = 'public.ob19_st' \
                  AND e.extname::text = 'pg_tide' \
                  AND oc.pg_tide_version = e.extversion::text \
                  AND oc.tide_outbox_created_at = ( \
                      SELECT created_at FROM tide.tide_outbox_config \
                       WHERE outbox_name = oc.tide_outbox_name \
                  ) \
             )",
        )
        .await;
    assert!(
        matches,
        "outbox mapping must record live pg_tide provenance"
    );
}

/// TIDE-7c: attach_outbox() with custom retention_hours and threshold succeeds.
#[tokio::test]
async fn test_attach_outbox_with_custom_params() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob7c_src", "ob7c_st").await;
    install_pg_tide_stub(&db).await;

    // Custom retention + threshold (the stub accepts any values).
    db.execute("SELECT pgtrickle.attach_outbox('ob7c_st', 48, 5000)")
        .await;

    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_outbox_config \
             WHERE stream_table_name = 'public.ob7c_st')",
        )
        .await;
    assert!(
        exists,
        "Catalog entry should exist after attach_outbox() with custom params"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// TIDE-4: OutboxAlreadyEnabled — duplicate attach
// ══════════════════════════════════════════════════════════════════════════════

/// TIDE-4: attach_outbox() raises an error when called twice on the same ST.
#[tokio::test]
async fn test_attach_outbox_fails_on_duplicate() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob4_src", "ob4_st").await;
    install_pg_tide_stub(&db).await;

    db.execute("SELECT pgtrickle.attach_outbox('ob4_st')").await;

    let result = db
        .try_execute("SELECT pgtrickle.attach_outbox('ob4_st')")
        .await;

    assert!(
        result.is_err(),
        "attach_outbox() should fail when called twice on the same stream table"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("already") || err.contains("outbox"),
        "Error should mention duplicate outbox; got: {err}"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// TIDE-7: detach_outbox — catalog cleanup
// ══════════════════════════════════════════════════════════════════════════════

/// TIDE-7d: detach_outbox() removes the pgt_outbox_config entry.
#[tokio::test]
async fn test_detach_outbox_removes_catalog_entry() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob7d_src", "ob7d_st").await;
    install_pg_tide_stub(&db).await;

    db.execute("SELECT pgtrickle.attach_outbox('ob7d_st')")
        .await;
    db.execute("SELECT pgtrickle.detach_outbox('ob7d_st')")
        .await;

    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_outbox_config \
             WHERE stream_table_name = 'public.ob7d_st')",
        )
        .await;
    assert!(
        !exists,
        "pgt_outbox_config entry should be removed after detach_outbox()"
    );
}

/// TIDE-7e: detach_outbox(if_exists => true) succeeds silently when not attached.
#[tokio::test]
async fn test_detach_outbox_if_exists_silently_succeeds() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob7e_src", "ob7e_st").await;

    // No attach — detach with if_exists=true should not raise an error.
    db.execute("SELECT pgtrickle.detach_outbox('ob7e_st', true)")
        .await;
}

// ══════════════════════════════════════════════════════════════════════════════
// TIDE-5: OutboxNotEnabled — detach without prior attach
// ══════════════════════════════════════════════════════════════════════════════

/// TIDE-5: detach_outbox() raises an error when the outbox is not attached.
#[tokio::test]
async fn test_detach_outbox_fails_when_not_attached() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob5_src", "ob5_st").await;

    let result = db
        .try_execute("SELECT pgtrickle.detach_outbox('ob5_st')")
        .await;

    assert!(
        result.is_err(),
        "detach_outbox() should fail when the outbox is not attached"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("not") || err.contains("outbox"),
        "Error should mention outbox not attached; got: {err}"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// TIDE-7: Integration — outbox write on refresh
// ══════════════════════════════════════════════════════════════════════════════

/// TIDE-7f: After attach_outbox(), refreshing the stream table calls
/// tide.outbox_publish() (verified by a counter-incrementing stub).
#[tokio::test]
async fn test_attach_outbox_publish_called_on_refresh() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob7f_src", "ob7f_st").await;

    // Install a stub that counts calls.
    db.execute("CREATE EXTENSION pg_tide").await;
    db.execute("CREATE TABLE tide_publish_log (ts timestamptz default now())")
        .await;

    db.execute("SELECT pgtrickle.attach_outbox('ob7f_st')")
        .await;

    // Insert rows to ensure the refresh produces a non-empty delta.
    db.execute("INSERT INTO ob7f_src VALUES (4, 'd'), (5, 'e')")
        .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('ob7f_st')")
        .await;

    let publish_count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM tide_publish_log")
        .await;

    assert!(
        publish_count >= 1,
        "tide.outbox_publish() should have been called at least once during refresh; \
         got {} calls",
        publish_count
    );
}

/// TIDE-20: a same-named replacement is rejected instead of receiving the
/// original stream table's events.
#[tokio::test]
async fn test_outbox_refresh_rejects_recreated_tide_outbox() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob20_src", "ob20_st").await;
    install_pg_tide_stub(&db).await;
    db.execute("SELECT pgtrickle.attach_outbox('ob20_st')")
        .await;

    db.execute("DELETE FROM tide.tide_outbox_config WHERE outbox_name = 'outbox_ob20_st'")
        .await;
    db.execute("SELECT tide.outbox_create('outbox_ob20_st', 24, 10000)")
        .await;
    db.execute("INSERT INTO ob20_src VALUES (4, 'd')").await;

    let result = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('ob20_st')")
        .await;
    assert!(
        result.is_err(),
        "refresh must fail closed on stale outbox identity"
    );
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("stale") || err.contains("binding"),
        "error should identify the stale binding; got: {err}"
    );
}

// ══════════════════════════════════════════════════════════════════════════════
// TEST-001 (v0.72.0): Outbox catalog OID invariant
// ══════════════════════════════════════════════════════════════════════════════

/// TEST-001a: `pgt_outbox_config.stream_table_oid` must equal the stream
/// table's `pgt_relid` (the real PostgreSQL relation OID in `pg_class`), not
/// the internal `pgt_id` cast to OID.
///
/// This is the regression test for COR-002/API-001 (v0.72.0): before the fix
/// the column stored `pgt_id::oid`, which made user joins to `pg_class` or
/// `pgt_stream_tables.pgt_relid` return no rows.
#[tokio::test]
async fn test_outbox_stream_table_oid_equals_pgt_relid() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob_oid_src", "ob_oid_st").await;
    install_pg_tide_stub(&db).await;

    db.execute("SELECT pgtrickle.attach_outbox('ob_oid_st')")
        .await;

    // The catalog invariant: stream_table_oid must equal pgt_relid.
    let matches: bool = db
        .query_scalar(
            "SELECT EXISTS( \
               SELECT 1 \
               FROM pgtrickle.pgt_outbox_config oc \
               JOIN pgtrickle.pgt_stream_tables st \
                    ON oc.stream_table_oid = st.pgt_relid \
               WHERE oc.stream_table_name = 'public.ob_oid_st' \
             )",
        )
        .await;

    assert!(
        matches,
        "pgt_outbox_config.stream_table_oid must equal pgt_stream_tables.pgt_relid \
         (not pgt_id::oid). Join returned no rows — catalog OID mismatch detected."
    );
}

/// TEST-001b: `stream_table_oid` must also be present in `pg_class.oid`, so
/// users can join to `pg_class` for table metadata.
#[tokio::test]
async fn test_outbox_stream_table_oid_exists_in_pg_class() {
    let db = E2eDb::new().await.with_extension().await;
    make_outbox_st(&db, "ob_pgclass_src", "ob_pgclass_st").await;
    install_pg_tide_stub(&db).await;

    db.execute("SELECT pgtrickle.attach_outbox('ob_pgclass_st')")
        .await;

    let found_in_pg_class: bool = db
        .query_scalar(
            "SELECT EXISTS( \
               SELECT 1 \
               FROM pgtrickle.pgt_outbox_config oc \
               JOIN pg_catalog.pg_class c ON c.oid = oc.stream_table_oid \
               WHERE oc.stream_table_name = 'public.ob_pgclass_st' \
             )",
        )
        .await;

    assert!(
        found_in_pg_class,
        "pgt_outbox_config.stream_table_oid must be present in pg_class.oid"
    );
}
