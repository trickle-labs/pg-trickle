//! E2E tests for extension upgrade/migration safety (Category L).
//!
//! Two groups of tests:
//!
//! **L1–L7 (schema stability):** Run against the standard E2E image. Verify
//! catalog schema, indexes, event triggers, and views are correct. These
//! run on every E2E test invocation.
//!
//! **L8–L13 (true upgrade path):** Run against the upgrade E2E image
//! (`pg_trickle_upgrade_e2e:latest`). Install old version, populate data,
//! run `ALTER EXTENSION UPDATE`, and verify everything still works.
//! Requires: `./tests/build_e2e_upgrade_image.sh` (or `just build-upgrade-image`).
//! Set `PGS_E2E_IMAGE=pg_trickle_upgrade_e2e:latest` to run these.
//!
//! See `plans/sql/PLAN_UPGRADE_MIGRATIONS.md` for the full upgrade strategy.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

const CURRENT_PG_TRICKLE_VERSION: &str = env!("CARGO_PKG_VERSION");

// ══════════════════════════════════════════════════════════════════════
// L1 — Catalog schema matches expected columns
// ══════════════════════════════════════════════════════════════════════

/// Verify that the `pgt_stream_tables` catalog table has all expected
/// columns with correct types. This catches accidental schema drift
/// and ensures migration scripts know the baseline schema.
#[tokio::test]
async fn test_upgrade_catalog_schema_stability() {
    let db = E2eDb::new().await.with_extension().await;

    // Expected columns in pgt_stream_tables (alphabetical)
    let expected_columns = vec![
        ("auto_threshold", "double precision"),
        ("blow_reason", "text"),
        ("blown_at", "timestamp with time zone"),
        ("column_lineage", "jsonb"),
        ("consecutive_errors", "integer"),
        ("created_at", "timestamp with time zone"),
        ("data_timestamp", "timestamp with time zone"),
        ("defining_query", "text"),
        ("defining_query_hash", "bigint"),
        ("defining_search_path", "text"),
        ("diamond_consistency", "text"),
        ("diamond_schedule_policy", "text"),
        ("downstream_publication_name", "text"),
        ("effective_refresh_mode", "text"),
        ("freshness_deadline_ms", "bigint"),
        ("frontier", "jsonb"),
        ("function_hashes", "text"),
        ("functions_used", "ARRAY"),
        ("fuse_ceiling", "bigint"),
        ("fuse_mode", "text"),
        ("fuse_sensitivity", "integer"),
        ("fuse_state", "text"),
        ("has_keyless_source", "boolean"),
        ("is_append_only", "boolean"),
        ("is_populated", "boolean"),
        ("last_error_at", "timestamp with time zone"),
        ("last_error_code", "text"),
        ("last_error_message", "text"),
        ("last_error_retryable", "boolean"),
        ("last_fixpoint_iterations", "integer"),
        ("last_full_ms", "double precision"),
        ("last_refresh_at", "timestamp with time zone"),
        ("max_delta_fraction", "double precision"),
        ("max_differential_joins", "integer"),
        ("needs_reinit", "boolean"),
        ("original_query", "text"),
        ("pgt_id", "bigint"),
        ("pgt_name", "text"),
        ("pgt_relid", "oid"),
        ("pgt_schema", "text"),
        ("pooler_compatibility_mode", "boolean"),
        ("refresh_tier", "text"),
        ("requested_cdc_mode", "text"),
        ("refresh_mode", "text"),
        ("requested_refresh_mode", "text"),
        ("refresh_reason", "text"),
        ("refresh_reason_detail", "text"),
        ("row_identity_version", "smallint"),
        ("scc_id", "integer"),
        ("schedule", "text"),
        ("self_heal_lock_backoff_exponent", "smallint"),
        ("self_heal_success_streak", "smallint"),
        ("self_heal_work_mem_percent", "smallint"),
        ("shadow_table_name", "text"),
        ("st_partition_key", "text"),
        ("st_placement", "text"), // CITUS-3: v0.32.0
        ("status", "text"),
        ("storage_backend", "text"),       // v0.36.0: CORR-2/UX-3
        ("storage_fillfactor", "integer"), // v0.73.0: HOT-1
        ("target_freshness_mode", "text"),
        ("tentative_frontier", "jsonb"),
        ("temporal_mode", "boolean"), // v0.36.0: CORR-1/UX-1
        ("topk_limit", "integer"),
        ("topk_offset", "integer"),
        ("topk_order_by", "text"),
        ("updated_at", "timestamp with time zone"),
        // v0.47.0: VP-1/VP-2 — post-refresh action hooks
        ("post_refresh_action", "text"),
        ("reindex_drift_threshold", "double precision"),
        ("rows_changed_since_last_reindex", "bigint"),
        ("last_reindex_at", "timestamp with time zone"),
        ("in_shadow_build", "boolean"),
        ("query_complexity_class", "text"), // v0.78.0: P-2
    ];

    for (col_name, expected_type) in &expected_columns {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS( \
                    SELECT 1 FROM information_schema.columns \
                    WHERE table_schema = 'pgtrickle' \
                      AND table_name = 'pgt_stream_tables' \
                      AND column_name = '{col_name}' \
                      AND data_type LIKE '%{expected_type}%' \
                )"
            ))
            .await;
        assert!(
            exists,
            "Column '{col_name}' with type containing '{expected_type}' not found in catalog"
        );
    }

    // Verify total column count to detect unexpected additions
    let col_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_schema = 'pgtrickle' AND table_name = 'pgt_stream_tables'",
        )
        .await;
    assert_eq!(
        col_count,
        expected_columns.len() as i64,
        "Unexpected number of columns in pgt_stream_tables (schema drift?)"
    );
}

// ══════════════════════════════════════════════════════════════════════
// L2 — Catalog indexes survive extension lifecycle
// ══════════════════════════════════════════════════════════════════════

/// Verify that expected indexes exist on catalog tables.
#[tokio::test]
async fn test_upgrade_catalog_indexes_present() {
    let db = E2eDb::new().await.with_extension().await;

    let expected_indexes = vec![
        ("pgt_stream_tables", "idx_pgt_status"),
        ("pgt_stream_tables", "idx_pgt_name"),
        ("pgt_snapshots", "idx_pgt_snapshots_pgt_id"),
        ("pgt_dependencies", "idx_deps_source"),
    ];

    for (table, index) in &expected_indexes {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS( \
                    SELECT 1 FROM pg_indexes \
                    WHERE schemaname = 'pgtrickle' \
                      AND tablename = '{table}' \
                      AND indexname = '{index}' \
                )"
            ))
            .await;
        assert!(exists, "Index '{index}' on '{table}' not found");
    }
}

#[tokio::test]
async fn test_upgrade_migration_era_catalog_tables_and_config_dump_policy_present() {
    let db = E2eDb::new().await.with_extension().await;

    for table_name in ["pgt_snapshots", "pgt_subscriptions"] {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS( \
                    SELECT 1 FROM information_schema.tables \
                    WHERE table_schema = 'pgtrickle' \
                      AND table_name = '{table_name}' \
                )"
            ))
            .await;
        assert!(exists, "Table 'pgtrickle.{table_name}' not found");
    }

    let config_dump_relations: Vec<String> = sqlx::query_scalar(
        "SELECT format('%I.%I', n.nspname, c.relname) \
         FROM pg_extension ext \
         JOIN LATERAL unnest(ext.extconfig) AS cfg(relid) ON true \
         JOIN pg_class c ON c.oid = cfg.relid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE ext.extname = 'pg_trickle' \
         ORDER BY 1",
    )
    .fetch_all(&db.pool)
    .await
    .expect("load pg_extension_config_dump registrations");

    for relation in ["pgtrickle.pgt_snapshots", "pgtrickle.pgt_subscriptions"] {
        assert!(
            config_dump_relations.iter().any(|item| item == relation),
            "Config-dump policy missing for {relation}"
        );
    }
}

// ══════════════════════════════════════════════════════════════════════
// L3 — DROP EXTENSION CASCADE + CREATE EXTENSION round-trip
// ══════════════════════════════════════════════════════════════════════

/// Simulate the destructive upgrade path: DROP EXTENSION CASCADE + fresh
/// CREATE EXTENSION. After round-trip, the extension must be fully
/// functional (create ST, refresh, verify).
#[tokio::test]
async fn test_upgrade_drop_recreate_roundtrip() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a source + ST to have data in the system
    db.execute("CREATE TABLE lr_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO lr_src VALUES (1, 'hello'), (2, 'world')")
        .await;
    db.create_st("lr_st", "SELECT id, val FROM lr_src", "1m", "FULL")
        .await;
    assert_eq!(db.count("public.lr_st").await, 2);

    // DROP EXTENSION CASCADE — this destroys all pg_trickle objects
    db.execute("DROP EXTENSION pg_trickle CASCADE").await;

    // Verify cleanup: catalog tables gone
    let schemas_exist: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.schemata \
                WHERE schema_name = 'pgtrickle' \
            )",
        )
        .await;
    assert!(!schemas_exist, "pgtrickle schema should be dropped");

    // Source table must survive the DROP EXTENSION
    assert_eq!(db.count("lr_src").await, 2);

    // Re-create extension
    db.execute("CREATE EXTENSION pg_trickle CASCADE").await;

    // Verify extension is fully functional after round-trip
    db.create_st(
        "lr_st_new",
        "SELECT id, val FROM lr_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.count("public.lr_st_new").await, 2);

    // Mutate and refresh
    db.execute("INSERT INTO lr_src VALUES (3, 'again')").await;
    db.refresh_st("lr_st_new").await;
    db.assert_st_matches_query("public.lr_st_new", "SELECT id, val FROM lr_src")
        .await;
}

// ══════════════════════════════════════════════════════════════════════
// L4 — Extension version matches Cargo.toml
// ══════════════════════════════════════════════════════════════════════

/// Verify the installed extension version matches the expected version
/// from Cargo.toml. This catches version mismatch between the binary
/// and the control file.
#[tokio::test]
async fn test_upgrade_extension_version_consistency() {
    let db = E2eDb::new().await.with_extension().await;

    let version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")
        .await;

    // The version should be a valid semver string
    assert!(
        version.contains('.'),
        "Extension version '{}' doesn't look like semver",
        version
    );

    // Verify it's accessible via our version function too
    let fn_version: String = db.query_scalar("SELECT pgtrickle.version()").await;
    assert_eq!(
        version, fn_version,
        "pg_extension version ({}) must match pgtrickle.version() ({})",
        version, fn_version
    );
}

// ══════════════════════════════════════════════════════════════════════
// L5 — Dependencies table schema stability
// ══════════════════════════════════════════════════════════════════════

/// Verify that `pgt_dependencies` has all expected columns. Upgrade
/// migrations must know this schema to add/alter columns safely.
#[tokio::test]
async fn test_upgrade_dependencies_schema_stability() {
    let db = E2eDb::new().await.with_extension().await;

    let expected_dep_columns = vec![
        "pgt_id",
        "source_relid",
        "source_type",
        "columns_used",
        "column_snapshot",
        "schema_fingerprint",
        "cdc_mode",
        "slot_name",
        "decoder_confirmed_lsn",
        "transition_started_at",
        "source_stable_name", // CITUS-4: v0.32.0
        "source_placement",   // CITUS-3: v0.32.0
    ];

    for col_name in &expected_dep_columns {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS( \
                    SELECT 1 FROM information_schema.columns \
                    WHERE table_schema = 'pgtrickle' \
                      AND table_name = 'pgt_dependencies' \
                      AND column_name = '{col_name}' \
                )"
            ))
            .await;
        assert!(exists, "Column '{col_name}' not found in pgt_dependencies");
    }
}

// ══════════════════════════════════════════════════════════════════════
// L6 — Event triggers survive extension lifecycle
// ══════════════════════════════════════════════════════════════════════

/// Verify event triggers are properly installed and functional after
/// CREATE EXTENSION. These triggers are critical for DDL monitoring.
#[tokio::test]
async fn test_upgrade_event_triggers_installed() {
    let db = E2eDb::new().await.with_extension().await;

    let triggers = vec!["pg_trickle_ddl_tracker", "pg_trickle_drop_tracker"];

    for trigger_name in &triggers {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS( \
                    SELECT 1 FROM pg_event_trigger \
                    WHERE evtname = '{trigger_name}' \
                )"
            ))
            .await;
        assert!(
            exists,
            "Event trigger '{trigger_name}' not found after CREATE EXTENSION"
        );
    }
}

// ══════════════════════════════════════════════════════════════════════
// L7 — Views survive extension lifecycle
// ══════════════════════════════════════════════════════════════════════

/// Verify that monitoring views are created and queryable.
#[tokio::test]
async fn test_upgrade_monitoring_views_present() {
    let db = E2eDb::new().await.with_extension().await;

    let views = vec![
        "pgtrickle.stream_tables_info",
        "pgtrickle.pg_stat_stream_tables",
        "pgtrickle.pgt_cdc_status",
    ];

    for view in &views {
        // Just verify the view is queryable (no rows expected)
        let count: i64 = db
            .query_scalar(&format!("SELECT count(*) FROM {view}"))
            .await;
        assert_eq!(count, 0, "View {view} should be empty but queryable");
    }
}

// ══════════════════════════════════════════════════════════════════════
// L15 — v0.9.0 schema additions exist after fresh install
// ══════════════════════════════════════════════════════════════════════

/// Verify that tables and indexes introduced in v0.9.0 are present after
/// a fresh CREATE EXTENSION. Specifically asserts the Cross-Source Snapshot
/// Consistency table (pgt_refresh_groups) and its structure.
#[tokio::test]
async fn test_upgrade_v090_catalog_additions() {
    if upgrade_image_available() {
        eprintln!(
            "SKIP: test_upgrade_v090_catalog_additions is a fresh-install schema \
             assertion for the standard E2E image"
        );
        return;
    }

    let db = E2eDb::new().await.with_extension().await;

    // This test asserts schema additions introduced in the current development
    // line (v0.9.0-era features). In upgrade-matrix jobs that intentionally
    // install an older SQL version (for example 0.2.0), these objects are not
    // expected to exist. Skip unless installed SQL matches the current binary.
    let installed_version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")
        .await;
    let lib_version = env!("CARGO_PKG_VERSION");
    if installed_version != lib_version {
        eprintln!(
            "SKIP: test_upgrade_v090_catalog_additions requires installed SQL \
             version to match binary version ({lib_version}), got {installed_version}"
        );
        return;
    }

    // pgt_refresh_groups must exist (added in 0.9.0 for Cross-Source Snapshot Consistency)
    let table_exists: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.tables \
                WHERE table_schema = 'pgtrickle' \
                  AND table_name = 'pgt_refresh_groups' \
            )",
        )
        .await;
    assert!(
        table_exists,
        "pgtrickle.pgt_refresh_groups must exist after fresh install"
    );

    // Verify all expected columns are present
    let expected_cols = vec![
        ("group_id", "integer"),
        ("group_name", "text"),
        ("member_oids", "ARRAY"),
        ("isolation", "text"),
        ("created_at", "timestamp with time zone"),
    ];
    for (col, typ) in &expected_cols {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS( \
                    SELECT 1 FROM information_schema.columns \
                    WHERE table_schema = 'pgtrickle' \
                      AND table_name = 'pgt_refresh_groups' \
                      AND column_name = '{col}' \
                      AND data_type LIKE '%{typ}%' \
                )"
            ))
            .await;
        assert!(
            exists,
            "pgt_refresh_groups.{col} (type ~'{typ}') missing after fresh install"
        );
    }

    // The table should be empty on a fresh install
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_refresh_groups")
        .await;
    assert_eq!(
        count, 0,
        "pgt_refresh_groups should be empty after fresh install"
    );

    // Verify the unique constraint on group_name is functional
    db.execute(
        "INSERT INTO pgtrickle.pgt_refresh_groups (group_name, member_oids) \
         VALUES ('test_group', ARRAY[]::OID[])",
    )
    .await;
    let dup_result = sqlx::query(
        "INSERT INTO pgtrickle.pgt_refresh_groups (group_name, member_oids) \
         VALUES ('test_group', ARRAY[]::OID[])",
    )
    .execute(&db.pool)
    .await;
    assert!(
        dup_result.is_err(),
        "Duplicate group_name insert should fail (unique constraint)"
    );

    // Cleanup
    db.execute("DELETE FROM pgtrickle.pgt_refresh_groups WHERE group_name = 'test_group'")
        .await;
}

// ══════════════════════════════════════════════════════════════════════
// L8–L13 — True version-to-version upgrade tests
// ══════════════════════════════════════════════════════════════════════
//
// These tests require the upgrade E2E image:
//   PGS_E2E_IMAGE=pg_trickle_upgrade_e2e:latest
//
// They are #[ignore]d by default because the upgrade image must be built
// separately. Run with:
//   just test-upgrade 0.1.3 0.2.2
// which builds the image and sets the env var automatically.

/// Helper: returns true if the upgrade E2E image is available.
/// Used to skip tests gracefully when image isn't built.
fn upgrade_image_available() -> bool {
    matches!(std::env::var("PGS_UPGRADE_FROM"), Ok(v) if !v.is_empty())
}

#[tokio::test]
#[ignore]
async fn test_upgrade_v0877_backfills_defining_search_path() {
    if !upgrade_image_available()
        || std::env::var("PGS_UPGRADE_FROM").as_deref() != Ok("0.87.6")
        || std::env::var("PGS_UPGRADE_TO").as_deref() != Ok(CURRENT_PG_TRICKLE_VERSION)
    {
        eprintln!("SKIP: requires the 0.87.6 -> 0.87.7 upgrade image");
        return;
    }

    let db = E2eDb::new_without_extension().await;
    db.execute("CREATE EXTENSION pg_trickle VERSION '0.87.6' CASCADE")
        .await;
    db.execute("CREATE ROLE lsec_upgrade_owner").await;
    db.execute("CREATE TABLE public.lsec_upgrade_storage (id integer)")
        .await;
    db.execute("ALTER TABLE public.lsec_upgrade_storage OWNER TO lsec_upgrade_owner")
        .await;
    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables \
         (pgt_relid, pgt_name, pgt_schema, defining_query) \
         VALUES ('public.lsec_upgrade_storage'::regclass, \
                 'lsec_upgrade_storage', 'public', 'SELECT id FROM public.lsec_upgrade_storage')",
    )
    .await;

    db.execute("ALTER EXTENSION pg_trickle UPDATE TO '0.87.7'")
        .await;

    let path: String = db
        .query_scalar(
            "SELECT defining_search_path \
             FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'lsec_upgrade_storage'",
        )
        .await;
    assert_eq!(path, "\"lsec_upgrade_owner\", public");

    let not_null: bool = db
        .query_scalar(
            "SELECT is_nullable = 'NO' \
         FROM information_schema.columns \
         WHERE table_schema = 'pgtrickle' \
           AND table_name = 'pgt_stream_tables' \
           AND column_name = 'defining_search_path'",
        )
        .await;
    assert!(not_null);
}

#[tokio::test]
#[ignore]
async fn test_upgrade_v0877_missing_storage_aborts_before_catalog_mutation() {
    if !upgrade_image_available()
        || std::env::var("PGS_UPGRADE_FROM").as_deref() != Ok("0.87.6")
        || std::env::var("PGS_UPGRADE_TO").as_deref() != Ok(CURRENT_PG_TRICKLE_VERSION)
    {
        eprintln!("SKIP: requires the 0.87.6 -> 0.87.7 upgrade image");
        return;
    }

    let db = E2eDb::new_without_extension().await;
    db.execute("CREATE EXTENSION pg_trickle VERSION '0.87.6' CASCADE")
        .await;
    db.execute(
        "INSERT INTO pgtrickle.pgt_stream_tables \
         (pgt_relid, pgt_name, pgt_schema, defining_query) \
         VALUES (4000000000::oid, 'lsec_missing_storage', 'public', 'SELECT 1')",
    )
    .await;

    let result = db
        .try_execute("ALTER EXTENSION pg_trickle UPDATE TO '0.87.7'")
        .await;
    assert!(
        result.is_err(),
        "upgrade must reject a missing storage relation"
    );

    let ext_version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")
        .await;
    assert_eq!(ext_version, "0.87.6");
    let column_exists: bool = db
        .query_scalar(
            "SELECT EXISTS ( \
         SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'pgtrickle' \
           AND table_name = 'pgt_stream_tables' \
           AND column_name = 'defining_search_path')",
        )
        .await;
    assert!(
        !column_exists,
        "failed upgrade must leave the old catalog unchanged"
    );
}

// ══════════════════════════════════════════════════════════════════════
// L8 — All new functions exist after ALTER EXTENSION UPDATE
// ══════════════════════════════════════════════════════════════════════

/// Install from old version, run ALTER EXTENSION UPDATE, verify all
/// new functions are callable.
#[tokio::test]
#[ignore]
async fn test_upgrade_chain_new_functions_exist() {
    if !upgrade_image_available() {
        eprintln!("SKIP: PGS_UPGRADE_FROM not set (need upgrade E2E image)");
        return;
    }
    let from_version = std::env::var("PGS_UPGRADE_FROM").unwrap();
    let to_version =
        std::env::var("PGS_UPGRADE_TO").unwrap_or_else(|_| CURRENT_PG_TRICKLE_VERSION.into());

    // The .so binary is always the current version. Calling pg_trickle functions
    // requires the SQL catalog to match — skip when upgrading to an older version.
    let lib_version = env!("CARGO_PKG_VERSION");
    if to_version != lib_version {
        eprintln!(
            "SKIP: test_upgrade_chain_new_functions_exist requires SQL version to match \
             binary version ({lib_version}), got PGS_UPGRADE_TO={to_version}"
        );
        return;
    }

    // Start container WITHOUT auto-extension, install old version manually
    let db = E2eDb::new_without_extension().await;

    // Install old version
    db.execute(&format!(
        "CREATE EXTENSION pg_trickle VERSION '{from_version}' CASCADE"
    ))
    .await;

    // Verify old version is installed
    let installed_version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")
        .await;
    assert_eq!(installed_version, from_version);

    // Run the upgrade
    db.execute(&format!(
        "ALTER EXTENSION pg_trickle UPDATE TO '{to_version}'"
    ))
    .await;

    // Verify new version
    let new_version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")
        .await;
    assert_eq!(new_version, to_version);

    // All new functions in 0.2.0 must be callable
    let new_functions = vec![
        "SELECT * FROM pgtrickle.change_buffer_sizes()",
        "SELECT * FROM pgtrickle.health_check()",
        "SELECT * FROM pgtrickle.dependency_tree()",
        "SELECT * FROM pgtrickle.trigger_inventory()",
        "SELECT * FROM pgtrickle.refresh_timeline(50)",
        "SELECT pgtrickle.version()",
        "SELECT * FROM pgtrickle.diamond_groups()",
        "SELECT * FROM pgtrickle.pgt_scc_status()",
    ];

    for func_call in &new_functions {
        let result = sqlx::query(*func_call).fetch_all(&db.pool).await;
        assert!(
            result.is_ok(),
            "Function call failed after upgrade: {func_call}: {:?}",
            result.err()
        );
    }
}

// ══════════════════════════════════════════════════════════════════════
// L9 — Existing stream tables survive upgrade and refresh
// ══════════════════════════════════════════════════════════════════════

/// Install old version, populate source data, upgrade, then verify that
/// stream tables can be created and refreshed on the upgraded schema.
///
/// Note: we cannot call `create_stream_table()` *before* the upgrade because
/// the Docker image contains only the current binary (0.2.2). That binary's
/// `create_stream_table` inserts columns (`topk_offset`, `has_keyless_source`,
/// `function_hashes`) that do not exist in the 0.1.3 schema. Testing that
/// stream tables created under a true 0.1.3 binary survive the upgrade is
/// covered by the archive-snapshot tests (L1–L7) and manual upgrade validation.
/// This test validates: source data survives the upgrade, the full upgrade
/// chain completes, and the upgraded schema is fully usable.
#[tokio::test]
#[ignore]
async fn test_upgrade_chain_stream_tables_survive() {
    if !upgrade_image_available() {
        return;
    }
    let from_version = std::env::var("PGS_UPGRADE_FROM").unwrap();
    let to_version =
        std::env::var("PGS_UPGRADE_TO").unwrap_or_else(|_| CURRENT_PG_TRICKLE_VERSION.into());

    // The .so binary is always the current version. Calling pg_trickle functions
    // requires the SQL catalog to match — skip when upgrading to an older version.
    let lib_version = env!("CARGO_PKG_VERSION");
    if to_version != lib_version {
        eprintln!(
            "SKIP: test_upgrade_chain_stream_tables_survive requires SQL version to match \
             binary version ({lib_version}), got PGS_UPGRADE_TO={to_version}"
        );
        return;
    }

    let db = E2eDb::new_without_extension().await;
    db.execute(&format!(
        "CREATE EXTENSION pg_trickle VERSION '{from_version}' CASCADE"
    ))
    .await;

    // Create source table and populate under old version.
    // We intentionally do NOT call create_stream_table() here — the 0.2.2
    // binary cannot write to the 0.1.3 catalog schema (missing columns).
    db.execute("CREATE TABLE upgrade_src (id INT PRIMARY KEY, name TEXT)")
        .await;
    db.execute("INSERT INTO upgrade_src VALUES (1, 'alice'), (2, 'bob')")
        .await;

    // Upgrade (applies 0.1.3→0.2.0, 0.2.0→0.2.1, and 0.2.1→0.2.2 migration scripts)
    db.execute(&format!(
        "ALTER EXTENSION pg_trickle UPDATE TO '{to_version}'"
    ))
    .await;

    // Verify the upgrade reached the expected version
    let new_version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")
        .await;
    assert_eq!(new_version, to_version);

    // Source data must survive the upgrade
    let src_count: i64 = db.query_scalar("SELECT count(*) FROM upgrade_src").await;
    assert_eq!(src_count, 2);

    // Create a stream table with the upgraded schema — must work
    db.create_st(
        "upgrade_st",
        "SELECT id, name FROM upgrade_src",
        "1m",
        "FULL",
    )
    .await;
    assert_eq!(db.count("public.upgrade_st").await, 2);

    // Insert more data and refresh — must work on upgraded schema
    db.execute("INSERT INTO upgrade_src VALUES (3, 'carol')")
        .await;
    db.refresh_st("upgrade_st").await;
    assert_eq!(db.count("public.upgrade_st").await, 3);

    // Verify data correctness: ST must exactly match source after upgrade + refresh
    db.assert_st_matches_query("public.upgrade_st", "SELECT id, name FROM upgrade_src")
        .await;
}

// ══════════════════════════════════════════════════════════════════════
// L10 — Views queryable after upgrade
// ══════════════════════════════════════════════════════════════════════

/// Verify all monitoring views are queryable after ALTER EXTENSION UPDATE.
#[tokio::test]
#[ignore]
async fn test_upgrade_chain_views_queryable() {
    if !upgrade_image_available() {
        return;
    }
    let from_version = std::env::var("PGS_UPGRADE_FROM").unwrap();
    let to_version =
        std::env::var("PGS_UPGRADE_TO").unwrap_or_else(|_| CURRENT_PG_TRICKLE_VERSION.into());

    let db = E2eDb::new_without_extension().await;
    db.execute(&format!(
        "CREATE EXTENSION pg_trickle VERSION '{from_version}' CASCADE"
    ))
    .await;

    // Upgrade
    db.execute(&format!(
        "ALTER EXTENSION pg_trickle UPDATE TO '{to_version}'"
    ))
    .await;

    // All views must be queryable
    let views = vec![
        "pgtrickle.stream_tables_info",
        "pgtrickle.pg_stat_stream_tables",
    ];
    for view in &views {
        let result = sqlx::query(sqlx::AssertSqlSafe(format!("SELECT count(*) FROM {view}")))
            .fetch_one(&db.pool)
            .await;
        assert!(
            result.is_ok(),
            "View {view} not queryable after upgrade: {:?}",
            result.err()
        );
    }
}

// ══════════════════════════════════════════════════════════════════════
// L11 — Event triggers functional after upgrade
// ══════════════════════════════════════════════════════════════════════

/// Verify event triggers are present after ALTER EXTENSION UPDATE.
#[tokio::test]
#[ignore]
async fn test_upgrade_chain_event_triggers_present() {
    if !upgrade_image_available() {
        return;
    }
    let from_version = std::env::var("PGS_UPGRADE_FROM").unwrap();
    let to_version =
        std::env::var("PGS_UPGRADE_TO").unwrap_or_else(|_| CURRENT_PG_TRICKLE_VERSION.into());

    let db = E2eDb::new_without_extension().await;
    db.execute(&format!(
        "CREATE EXTENSION pg_trickle VERSION '{from_version}' CASCADE"
    ))
    .await;

    // Upgrade
    db.execute(&format!(
        "ALTER EXTENSION pg_trickle UPDATE TO '{to_version}'"
    ))
    .await;

    let triggers = vec!["pg_trickle_ddl_tracker", "pg_trickle_drop_tracker"];
    for trigger_name in &triggers {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS( \
                    SELECT 1 FROM pg_event_trigger \
                    WHERE evtname = '{trigger_name}' \
                )"
            ))
            .await;
        assert!(
            exists,
            "Event trigger '{trigger_name}' missing after upgrade"
        );
    }
}

// ══════════════════════════════════════════════════════════════════════
// L12 — Version function reports new version after upgrade
// ══════════════════════════════════════════════════════════════════════

/// Verify pgtrickle.version() matches the new version after upgrade.
#[tokio::test]
#[ignore]
async fn test_upgrade_chain_version_consistency() {
    if !upgrade_image_available() {
        return;
    }
    let from_version = std::env::var("PGS_UPGRADE_FROM").unwrap();
    let to_version =
        std::env::var("PGS_UPGRADE_TO").unwrap_or_else(|_| CURRENT_PG_TRICKLE_VERSION.into());

    // This assertion only holds when the SQL extension version being tested
    // matches the compiled binary version loaded in the container.
    let lib_version = env!("CARGO_PKG_VERSION");
    if to_version != lib_version {
        eprintln!(
            "SKIP: test_upgrade_chain_version_consistency requires SQL version to match \
             binary version ({lib_version}), got PGS_UPGRADE_TO={to_version}"
        );
        return;
    }

    let db = E2eDb::new_without_extension().await;
    db.execute(&format!(
        "CREATE EXTENSION pg_trickle VERSION '{from_version}' CASCADE"
    ))
    .await;

    // Upgrade
    db.execute(&format!(
        "ALTER EXTENSION pg_trickle UPDATE TO '{to_version}'"
    ))
    .await;

    // pg_extension version must match
    let ext_version: String = db
        .query_scalar("SELECT extversion FROM pg_extension WHERE extname = 'pg_trickle'")
        .await;
    assert_eq!(ext_version, to_version);

    // pgtrickle.version() returns the compiled .so version, which always
    // equals CARGO_PKG_VERSION regardless of SQL extension version.
    let fn_version: String = db.query_scalar("SELECT pgtrickle.version()").await;
    assert_eq!(
        fn_version, lib_version,
        "pgtrickle.version() should return the compiled library version"
    );
}

// ══════════════════════════════════════════════════════════════════════
// L13 — Function count matches fresh install after upgrade
// ══════════════════════════════════════════════════════════════════════

/// After upgrading, the number of pgtrickle.* functions should match
/// a fresh CREATE EXTENSION install. This catches functions that are
/// in the full install SQL but missing from the upgrade script.
#[tokio::test]
#[ignore]
async fn test_upgrade_chain_function_parity_with_fresh_install() {
    if !upgrade_image_available() {
        return;
    }
    let from_version = std::env::var("PGS_UPGRADE_FROM").unwrap();
    let to_version =
        std::env::var("PGS_UPGRADE_TO").unwrap_or_else(|_| CURRENT_PG_TRICKLE_VERSION.into());

    let db = E2eDb::new_without_extension().await;

    // Install old version then upgrade
    db.execute(&format!(
        "CREATE EXTENSION pg_trickle VERSION '{from_version}' CASCADE"
    ))
    .await;
    db.execute(&format!(
        "ALTER EXTENSION pg_trickle UPDATE TO '{to_version}'"
    ))
    .await;

    // Count functions after upgrade
    let upgraded_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_proc p \
             JOIN pg_namespace n ON p.pronamespace = n.oid \
             WHERE n.nspname = 'pgtrickle'",
        )
        .await;

    // Now do a fresh install *at the same SQL version* for comparison:
    // Drop and recreate to get fresh install counts
    db.execute("DROP EXTENSION pg_trickle CASCADE").await;
    db.execute(&format!(
        "CREATE EXTENSION pg_trickle VERSION '{to_version}' CASCADE"
    ))
    .await;

    let fresh_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_proc p \
             JOIN pg_namespace n ON p.pronamespace = n.oid \
             WHERE n.nspname = 'pgtrickle'",
        )
        .await;

    assert_eq!(
        upgraded_count,
        fresh_count,
        "Upgraded install has {} functions but fresh install has {} — \
         the upgrade script is missing {} function(s)",
        upgraded_count,
        fresh_count,
        fresh_count - upgraded_count
    );
}

// ══════════════════════════════════════════════════════════════════════
// L14 — v0.87.13 outbox provenance backfill
// ══════════════════════════════════════════════════════════════════════

/// Verify that the v0.87.12 outbox catalog is upgraded only after its live
/// pg_tide object can be identified, and that the identity is persisted.
#[tokio::test]
#[ignore]
async fn test_upgrade_v08713_backfills_outbox_provenance() {
    if !upgrade_image_available()
        || std::env::var("PGS_UPGRADE_FROM").as_deref() != Ok("0.87.12")
        || std::env::var("PGS_UPGRADE_TO").as_deref() != Ok(CURRENT_PG_TRICKLE_VERSION)
    {
        eprintln!("SKIP: requires the 0.87.12 -> 0.87.13 upgrade image");
        return;
    }

    let db = E2eDb::new_without_extension().await;
    db.execute("CREATE EXTENSION pg_trickle VERSION '0.87.12' CASCADE")
        .await;
    db.execute("CREATE EXTENSION pg_tide").await;
    db.execute("CREATE TABLE upgrade_outbox_source (id integer)")
        .await;
    db.execute("SELECT tide.outbox_create('outbox_upgrade_outbox_st', 24, 10000)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle.pgt_outbox_config \
         (stream_table_oid, stream_table_name, tide_outbox_name) \
         VALUES ('public.upgrade_outbox_source'::regclass, \
                 'public.upgrade_outbox_source', 'outbox_upgrade_outbox_st')",
    )
    .await;

    db.execute("ALTER EXTENSION pg_trickle UPDATE TO '0.87.13'")
        .await;

    let matches: bool = db
        .query_scalar(
            "SELECT oc.pg_tide_extension_oid = e.oid \
                    AND oc.pg_tide_version = e.extversion::text \
                    AND oc.tide_outbox_created_at = tc.created_at \
               FROM pgtrickle.pgt_outbox_config oc \
               JOIN pg_catalog.pg_extension e \
                 ON e.extname::text = 'pg_tide' \
               JOIN tide.tide_outbox_config tc \
                 ON tc.outbox_name = oc.tide_outbox_name \
              WHERE oc.stream_table_name = 'public.upgrade_outbox_source'",
        )
        .await;
    assert!(matches, "upgrade must backfill exact pg_tide provenance");
}

// ══════════════════════════════════════════════════════════════════════
// L15 — Upgrade script schema additions verified automatically
// ══════════════════════════════════════════════════════════════════════

/// After upgrading FROM → TO, parse each intermediate upgrade SQL script
/// in the chain and verify that every `CREATE TABLE`, `ADD COLUMN`,
/// `CREATE FUNCTION`, and `CREATE VIEW` it declares actually exists in the
/// database.
///
/// This is data-driven: adding a new `pg_trickle--X--Y.sql` file
/// automatically gets coverage — no new test function required.
#[tokio::test]
#[ignore]
async fn test_upgrade_schema_additions_from_sql() {
    if !upgrade_image_available() {
        return;
    }
    let from_version = std::env::var("PGS_UPGRADE_FROM").unwrap();
    let to_version =
        std::env::var("PGS_UPGRADE_TO").unwrap_or_else(|_| CURRENT_PG_TRICKLE_VERSION.into());

    let db = E2eDb::new_without_extension().await;

    db.execute(&format!(
        "CREATE EXTENSION pg_trickle VERSION '{from_version}' CASCADE"
    ))
    .await;
    db.execute(&format!(
        "ALTER EXTENSION pg_trickle UPDATE TO '{to_version}'"
    ))
    .await;

    // Walk the upgrade chain and collect all intermediate SQL scripts
    let mut current = from_version.clone();
    let mut sql_files: Vec<String> = Vec::new();
    while current != to_version {
        // Find the next step from `current`.
        // Collect all candidates sorted by name so that when multiple scripts
        // start from the same version (e.g. 0.41.0→0.47.0 and 0.41.0→0.48.0)
        // the traversal is deterministic: prefer the smallest next-version hop.
        let prefix = format!("pg_trickle--{}--", current);
        let pattern = format!("sql/{prefix}");
        let mut candidates: Vec<std::fs::DirEntry> = std::fs::read_dir("sql")
            .expect("sql/ directory")
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                name.starts_with(&prefix) && name.ends_with(".sql")
            })
            .collect();
        candidates.sort_by_key(|e| e.file_name());
        if let Some(entry) = candidates.into_iter().next() {
            let name = entry.file_name().to_string_lossy().to_string();
            let next = name
                .strip_prefix(&prefix)
                .unwrap()
                .strip_suffix(".sql")
                .unwrap()
                .to_string();
            sql_files.push(entry.path().to_string_lossy().to_string());
            current = next;
        } else {
            panic!(
                "No upgrade script found for {pattern}*.sql — cannot reach {to_version} from {from_version}"
            );
        }
    }

    eprintln!(
        "Checking schema additions from {} upgrade script(s) ({} → {})",
        sql_files.len(),
        from_version,
        to_version
    );

    // Pre-collect all functions explicitly DROPped anywhere in the upgrade
    // chain.  A function created in step N and then dropped in step M > N
    // will not exist in the final state, so we must skip those assertions.
    // Pattern handles both unquoted (schema.func) and quoted (schema."func") identifiers.
    let dropped_functions: std::collections::HashSet<(String, String)> = sql_files
        .iter()
        .flat_map(|p| {
            let raw = std::fs::read_to_string(p).unwrap_or_default();
            let content: String = raw
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            regex_lite::Regex::new(
                r#"(?i)DROP\s+FUNCTION\s+(?:IF\s+EXISTS\s+)?"?(\w+)"?\."?(\w+)"?"#,
            )
            .unwrap()
            .captures_iter(&content)
            .map(|c| {
                (
                    c.get(1).unwrap().as_str().to_lowercase(),
                    c.get(2).unwrap().as_str().to_lowercase(),
                )
            })
            .collect::<Vec<_>>()
        })
        .collect();

    // Pre-collect all tables explicitly DROPped anywhere in the upgrade chain.
    // A table created in step N and then dropped in step M > N will not exist
    // in the final state, so we must skip those assertions.
    // Pattern handles both unquoted (schema.table) and quoted (schema."table") identifiers.
    let dropped_tables: std::collections::HashSet<(String, String)> = sql_files
        .iter()
        .flat_map(|p| {
            let raw = std::fs::read_to_string(p).unwrap_or_default();
            let content: String = raw
                .lines()
                .filter(|l| !l.trim_start().starts_with("--"))
                .collect::<Vec<_>>()
                .join("\n");
            regex_lite::Regex::new(r#"(?i)DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?"?(\w+)"?\."?(\w+)"?"#)
                .unwrap()
                .captures_iter(&content)
                .map(|c| {
                    (
                        c.get(1).unwrap().as_str().to_lowercase(),
                        c.get(2).unwrap().as_str().to_lowercase(),
                    )
                })
                .collect::<Vec<_>>()
        })
        .collect();

    let mut checks = 0;

    for sql_path in &sql_files {
        let raw =
            std::fs::read_to_string(sql_path).unwrap_or_else(|e| panic!("read {sql_path}: {e}"));
        // Strip single-line SQL comments so commented-out DDL examples don't
        // produce false positives in the regexes below.
        let sql_content: String = raw
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        let sql_lower = sql_content.to_lowercase();

        // Extract CREATE TABLE declarations (schema.table)
        // Pattern handles both unquoted (schema.table) and quoted (schema."table") identifiers.
        for cap in regex_lite::Regex::new(
            r#"(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"?(\w+)"?\."?(\w+)"?"#,
        )
        .unwrap()
        .captures_iter(&sql_content)
        {
            let schema = cap.get(1).unwrap().as_str().to_lowercase();
            let table = cap.get(2).unwrap().as_str().to_lowercase();
            // Skip tables explicitly dropped later in the same upgrade chain
            if dropped_tables.contains(&(schema.clone(), table.clone())) {
                continue;
            }
            let exists: bool = db
                .query_scalar(&format!(
                    "SELECT EXISTS( \
                        SELECT 1 FROM information_schema.tables \
                        WHERE table_schema = '{schema}' AND table_name = '{table}' \
                    )"
                ))
                .await;
            assert!(
                exists,
                "Table {schema}.{table} declared in {sql_path} not found after upgrade"
            );
            checks += 1;
        }

        // Extract ADD COLUMN declarations
        for cap in regex_lite::Regex::new(r"(?i)ADD\s+COLUMN\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)")
            .unwrap()
            .captures_iter(&sql_content)
        {
            let col = cap.get(1).unwrap().as_str().to_lowercase();
            // Find the nearest preceding ALTER TABLE to get the table name
            let col_pos = cap.get(0).unwrap().start();
            let preceding = &sql_lower[..col_pos];
            if let Some(table_cap) = regex_lite::Regex::new(
                r#"(?i)alter\s+table\s+(?:if\s+exists\s+)?"?(\w+)"?\."?(\w+)"?"#,
            )
            .unwrap()
            .captures_iter(preceding)
            .last()
            {
                let schema = table_cap.get(1).unwrap().as_str();
                let table = table_cap.get(2).unwrap().as_str();
                let exists: bool = db
                    .query_scalar(&format!(
                        "SELECT EXISTS( \
                            SELECT 1 FROM information_schema.columns \
                            WHERE table_schema = '{schema}' \
                              AND table_name = '{table}' \
                              AND column_name = '{col}' \
                        )"
                    ))
                    .await;
                assert!(
                    exists,
                    "Column {schema}.{table}.{col} declared in {sql_path} not found after upgrade"
                );
                checks += 1;
            }
        }

        // Extract CREATE [OR REPLACE] FUNCTION declarations
        // Pattern handles both unquoted (schema.func) and quoted (schema."func") identifiers.
        for cap in regex_lite::Regex::new(
            r#"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+"?(\w+)"?\."?(\w+)"?"#,
        )
        .unwrap()
        .captures_iter(&sql_content)
        {
            let schema = cap.get(1).unwrap().as_str().to_lowercase();
            let func = cap.get(2).unwrap().as_str().to_lowercase();
            // Skip functions explicitly dropped later in the same upgrade chain
            if dropped_functions.contains(&(schema.clone(), func.clone())) {
                continue;
            }
            let exists: bool = db
                .query_scalar(&format!(
                    "SELECT EXISTS( \
                        SELECT 1 FROM pg_proc p \
                        JOIN pg_namespace n ON p.pronamespace = n.oid \
                        WHERE n.nspname = '{schema}' AND p.proname = '{func}' \
                    )"
                ))
                .await;
            assert!(
                exists,
                "Function {schema}.{func}() declared in {sql_path} not found after upgrade"
            );
            checks += 1;
        }

        // Extract CREATE [OR REPLACE] VIEW declarations
        // Pattern handles both unquoted (schema.view) and quoted (schema."view") identifiers.
        for cap in regex_lite::Regex::new(
            r#"(?i)CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+"?(\w+)"?\."?(\w+)"?"#,
        )
        .unwrap()
        .captures_iter(&sql_content)
        {
            let schema = cap.get(1).unwrap().as_str().to_lowercase();
            let view = cap.get(2).unwrap().as_str().to_lowercase();
            let exists: bool = db
                .query_scalar(&format!(
                    "SELECT EXISTS( \
                        SELECT 1 FROM information_schema.views \
                        WHERE table_schema = '{schema}' AND table_name = '{view}' \
                    )"
                ))
                .await;
            assert!(
                exists,
                "View {schema}.{view} declared in {sql_path} not found after upgrade"
            );
            checks += 1;
        }
    }

    if checks == 0 {
        eprintln!(
            "No schema additions found in upgrade scripts (empty/no-op upgrade) — skipping object checks"
        );
    } else {
        eprintln!("Verified {checks} schema object(s) from upgrade SQL scripts");
    }
}

// ══════════════════════════════════════════════════════════════════════
// TEST-8: v0.19.0-specific catalog integrity checks
// ══════════════════════════════════════════════════════════════════════

/// TEST-8a: Verify DB-3 pgt_schema_version table exists and is seeded.
#[tokio::test]
async fn test_upgrade_schema_version_table_present() {
    let db = E2eDb::new().await.with_extension().await;

    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.tables \
                WHERE table_schema = 'pgtrickle' AND table_name = 'pgt_schema_version' \
            )",
        )
        .await;
    assert!(
        exists,
        "pgt_schema_version table should exist after install"
    );

    // Should have at least one row (the initial version)
    let count: i64 = db
        .query_scalar("SELECT count(*)::bigint FROM pgtrickle.pgt_schema_version")
        .await;
    assert!(
        count >= 1,
        "pgt_schema_version should be seeded with initial version"
    );
}

/// TEST-8b: Verify DB-2 FK ON DELETE CASCADE on pgt_refresh_history.pgt_id.
/// Dropping a stream table should cascade-delete its refresh history.
#[tokio::test]
async fn test_upgrade_refresh_history_fk_cascade() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE fk_src (id INT PRIMARY KEY, v INT)")
        .await;
    db.execute("INSERT INTO fk_src VALUES (1, 10)").await;
    db.create_st("fk_st", "SELECT id, v FROM fk_src", "1m", "FULL")
        .await;

    // Verify history exists
    let has_history: bool = db
        .query_scalar(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_refresh_history \
             WHERE pgt_id = (SELECT pgt_id FROM pgtrickle.pgt_stream_tables \
             WHERE pgt_name = 'fk_st'))",
        )
        .await;
    assert!(has_history, "Refresh history should exist after create");

    // Get pgt_id before drop
    let pgt_id: i64 = db
        .query_scalar("SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'fk_st'")
        .await;

    // Drop the stream table
    db.execute("SELECT pgtrickle.drop_stream_table('fk_st')")
        .await;

    // History should be cascade-deleted
    let orphan_history: bool = db
        .query_scalar(&format!(
            "SELECT EXISTS(SELECT 1 FROM pgtrickle.pgt_refresh_history WHERE pgt_id = {pgt_id})"
        ))
        .await;
    assert!(
        !orphan_history,
        "Refresh history should be cascade-deleted after drop_stream_table"
    );
}

/// TEST-8c: Verify PERF-4 catalog indexes exist.
#[tokio::test]
async fn test_upgrade_v019_catalog_indexes_present() {
    let db = E2eDb::new().await.with_extension().await;

    // idx_pgt_relid on pgt_stream_tables(pgt_relid)
    let relid_idx: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM pg_indexes \
                WHERE schemaname = 'pgtrickle' AND indexname = 'idx_pgt_relid' \
            )",
        )
        .await;
    assert!(relid_idx, "idx_pgt_relid index should exist");

    // idx_deps_pgt_id on pgt_dependencies(pgt_id)
    let deps_idx: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM pg_indexes \
                WHERE schemaname = 'pgtrickle' AND indexname = 'idx_deps_pgt_id' \
            )",
        )
        .await;
    assert!(deps_idx, "idx_deps_pgt_id index should exist");
}

/// TEST-v0.78.0: Verify v0.78.0 schema additions exist after upgrade.
///
/// Checks that P-2 (query_complexity_class column) and P-3
/// (pgt_cost_model_summary table) are present after upgrading to 0.78.0.
#[tokio::test]
async fn test_upgrade_v078_catalog_additions() {
    let db = E2eDb::new().await.with_extension().await;

    // P-2: query_complexity_class column on pgt_stream_tables
    let has_complexity_col: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.columns \
                WHERE table_schema = 'pgtrickle' \
                  AND table_name = 'pgt_stream_tables' \
                  AND column_name = 'query_complexity_class' \
            )",
        )
        .await;
    assert!(
        has_complexity_col,
        "query_complexity_class column should exist on pgt_stream_tables"
    );

    // P-3: pgt_cost_model_summary table
    let has_summary_table: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.tables \
                WHERE table_schema = 'pgtrickle' \
                  AND table_name = 'pgt_cost_model_summary' \
            )",
        )
        .await;
    assert!(
        has_summary_table,
        "pgt_cost_model_summary table should exist after v0.78.0 upgrade"
    );

    // P-3: Verify table structure — pgt_id primary key
    let has_pk: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM information_schema.table_constraints \
                WHERE table_schema = 'pgtrickle' \
                  AND table_name = 'pgt_cost_model_summary' \
                  AND constraint_type = 'PRIMARY KEY' \
            )",
        )
        .await;
    assert!(has_pk, "pgt_cost_model_summary should have a primary key");
}

/// TEST-v0.79.0: Verify v0.79.0 new SQL functions exist after upgrade.
///
/// Checks that A-1 and A-2 convenience functions are present:
///   create_stream_table_fast_append_only()
///   set_stream_table_refresh_policy()
///   set_stream_table_storage_policy()
///   pause_stream_table()
#[tokio::test]
async fn test_upgrade_v079_new_functions() {
    let db = E2eDb::new().await.with_extension().await;

    // All four new v0.79.0 functions should exist in the pgtrickle schema.
    let expected_functions = [
        "create_stream_table_fast_append_only",
        "set_stream_table_refresh_policy",
        "set_stream_table_storage_policy",
        "pause_stream_table",
    ];

    for fn_name in &expected_functions {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS( \
                    SELECT 1 FROM pg_proc p \
                    JOIN pg_namespace n ON n.oid = p.pronamespace \
                    WHERE n.nspname = 'pgtrickle' \
                      AND p.proname = '{fn_name}' \
                )"
            ))
            .await;
        assert!(
            exists,
            "A-1/A-2 (v0.79.0): function pgtrickle.{fn_name}() should exist after upgrade"
        );
    }
}

/// TEST-v0.80.0: Verify v0.80.0 metrics_summary() has new cleanup columns.
///
/// Checks that O-3 cleanup backlog columns are present:
///   cleanup_backlog_count
///   cleanup_blocked_count
#[tokio::test]
async fn test_upgrade_v080_metrics_summary_columns() {
    let db = E2eDb::new().await.with_extension().await;

    // Verify cleanup_backlog_count column is queryable (O-3).
    // The value may be 0 or NULL when pgt_cleanup_status is empty, but
    // the column must exist (query must not fail).
    let _backlog: Option<i64> = db
        .query_scalar_opt("SELECT cleanup_backlog_count FROM pgtrickle.metrics_summary()")
        .await;

    // Verify cleanup_blocked_count column is queryable (O-3).
    let _blocked: Option<i64> = db
        .query_scalar_opt("SELECT cleanup_blocked_count FROM pgtrickle.metrics_summary()")
        .await;
}
