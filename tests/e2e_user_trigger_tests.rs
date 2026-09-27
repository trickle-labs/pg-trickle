//! E2E tests for user-defined triggers on stream tables.
//!
//! Validates that DIFFERENTIAL refresh fires triggers with correct TG_OP,
//! OLD, and NEW values via the explicit DML path, and that FULL refresh
//! correctly suppresses user row-level triggers.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

/// Disable the background scheduler for the current test database.
///
/// Trigger tests verify exact audit trail entries from manual refreshes.
/// A concurrent scheduler refresh can consume CDC changes before the manual
/// refresh, causing missing trigger invocations or extra trigger fires.
async fn disable_scheduler(db: &E2eDb) {
    db.execute(
        "DO $$ BEGIN \
           EXECUTE format('ALTER DATABASE %I SET pg_trickle.enabled = off', current_database()); \
         END $$",
    )
    .await;
    db.execute(
        "SELECT pg_terminate_backend(pid) \
         FROM pg_stat_activity \
         WHERE datname = current_database() \
           AND backend_type = 'pg_trickle scheduler' \
           AND pid <> pg_backend_pid()",
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

// ── Helper: create an audit trigger setup ──────────────────────────────
//
// Creates an audit log table and an AFTER trigger on the given stream table
// that records TG_OP, OLD, and NEW values for each fired row-level event.

/// SQL to create the audit infrastructure on a stream table.
/// The stream table must have columns (id INT, val TEXT).
fn audit_trigger_sql(st_name: &str) -> Vec<String> {
    vec![
        // Audit log table
        "CREATE TABLE audit_log (
            audit_id SERIAL PRIMARY KEY,
            op TEXT NOT NULL,
            old_id INT,
            old_val TEXT,
            new_id INT,
            new_val TEXT,
            fired_at TIMESTAMPTZ DEFAULT now()
        )"
        .to_string(),
        // Trigger function
        "CREATE OR REPLACE FUNCTION audit_trigger_fn()
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                INSERT INTO audit_log (op, new_id, new_val)
                VALUES ('INSERT', NEW.id, NEW.val);
                RETURN NEW;
            ELSIF TG_OP = 'UPDATE' THEN
                INSERT INTO audit_log (op, old_id, old_val, new_id, new_val)
                VALUES ('UPDATE', OLD.id, OLD.val, NEW.id, NEW.val);
                RETURN NEW;
            ELSIF TG_OP = 'DELETE' THEN
                INSERT INTO audit_log (op, old_id, old_val)
                VALUES ('DELETE', OLD.id, OLD.val);
                RETURN OLD;
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql"
            .to_string(),
        // Attach trigger to stream table
        format!(
            "CREATE TRIGGER audit_trig
            AFTER INSERT OR UPDATE OR DELETE ON {st_name}
            FOR EACH ROW EXECUTE FUNCTION audit_trigger_fn()"
        ),
    ]
}

// ── Explicit DML: INSERT trigger ───────────────────────────────────────

#[tokio::test]
async fn test_explicit_dml_insert() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    // Source table
    db.execute("CREATE TABLE src_ins (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_ins VALUES (1, 'a')").await;

    // Create stream table (DIFFERENTIAL)
    db.create_st(
        "st_ins",
        "SELECT id, val FROM src_ins",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Establish frontier so subsequent refreshes use DIFFERENTIAL (not FULL
    // fallback). Without this, the first refresh_st falls back to FULL which
    // suppresses user triggers.
    db.refresh_st("st_ins").await;

    // Attach audit trigger on the stream table
    for sql in audit_trigger_sql("st_ins") {
        db.execute(&sql).await;
    }

    // Clear audit log (the CREATE + initial refresh may have fired triggers
    // before we attached them, but we attached after create, so log is clean)
    db.execute("TRUNCATE audit_log").await;

    // Insert a new source row and refresh
    db.execute("INSERT INTO src_ins VALUES (2, 'b')").await;
    db.refresh_st("st_ins").await;

    // Verify the ST has both rows
    let st_count: i64 = db.count("st_ins").await;
    assert_eq!(st_count, 2, "ST should have 2 rows after refresh");

    // Verify audit log captured an INSERT for the new row
    let insert_count: i64 = db
        .query_scalar("SELECT count(*) FROM audit_log WHERE op = 'INSERT'")
        .await;
    assert!(
        insert_count >= 1,
        "Audit log should have at least 1 INSERT entry, got {}",
        insert_count
    );

    // Verify the inserted row details
    let new_id: i32 = db
        .query_scalar("SELECT new_id FROM audit_log WHERE op = 'INSERT' AND new_id = 2 LIMIT 1")
        .await;
    assert_eq!(new_id, 2, "Audit should record new_id = 2");

    let new_val: String = db
        .query_scalar("SELECT new_val FROM audit_log WHERE op = 'INSERT' AND new_id = 2 LIMIT 1")
        .await;
    assert_eq!(new_val, "b", "Audit should record new_val = 'b'");
}

// ── Explicit DML: UPDATE trigger ───────────────────────────────────────

#[tokio::test]
async fn test_explicit_dml_update() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE src_upd (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_upd VALUES (1, 'old')").await;

    db.create_st(
        "st_upd",
        "SELECT id, val FROM src_upd",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh to populate
    db.refresh_st("st_upd").await;

    // Attach audit trigger and clear log
    for sql in audit_trigger_sql("st_upd") {
        db.execute(&sql).await;
    }
    db.execute("TRUNCATE audit_log").await;

    // Update source and refresh
    db.execute("UPDATE src_upd SET val = 'new' WHERE id = 1")
        .await;
    db.refresh_st("st_upd").await;

    // ST should still have 1 row with updated value
    let val: String = db.query_scalar("SELECT val FROM st_upd WHERE id = 1").await;
    assert_eq!(val, "new", "ST should have updated value");

    // Audit should show an UPDATE with correct OLD and NEW
    let update_count: i64 = db
        .query_scalar("SELECT count(*) FROM audit_log WHERE op = 'UPDATE'")
        .await;
    assert!(
        update_count >= 1,
        "Audit log should have at least 1 UPDATE entry, got {}",
        update_count
    );

    let old_val: String = db
        .query_scalar("SELECT old_val FROM audit_log WHERE op = 'UPDATE' AND old_id = 1 LIMIT 1")
        .await;
    assert_eq!(old_val, "old", "OLD.val should be 'old'");

    let new_val: String = db
        .query_scalar("SELECT new_val FROM audit_log WHERE op = 'UPDATE' AND new_id = 1 LIMIT 1")
        .await;
    assert_eq!(new_val, "new", "NEW.val should be 'new'");
}

// ── Explicit DML: DELETE trigger ───────────────────────────────────────

#[tokio::test]
async fn test_explicit_dml_delete() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE src_del (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_del VALUES (1, 'bye')").await;

    db.create_st(
        "st_del",
        "SELECT id, val FROM src_del",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh
    db.refresh_st("st_del").await;

    // Attach audit trigger and clear
    for sql in audit_trigger_sql("st_del") {
        db.execute(&sql).await;
    }
    db.execute("TRUNCATE audit_log").await;

    // Delete source row and refresh
    db.execute("DELETE FROM src_del WHERE id = 1").await;
    db.refresh_st("st_del").await;

    // ST should be empty
    let st_count: i64 = db.count("st_del").await;
    assert_eq!(st_count, 0, "ST should be empty after source row deleted");

    // Audit should show DELETE with correct OLD
    let delete_count: i64 = db
        .query_scalar("SELECT count(*) FROM audit_log WHERE op = 'DELETE'")
        .await;
    assert!(
        delete_count >= 1,
        "Audit log should have at least 1 DELETE entry, got {}",
        delete_count
    );

    let old_id: i32 = db
        .query_scalar("SELECT old_id FROM audit_log WHERE op = 'DELETE' LIMIT 1")
        .await;
    assert_eq!(old_id, 1, "OLD.id should be 1");

    let old_val: String = db
        .query_scalar("SELECT old_val FROM audit_log WHERE op = 'DELETE' LIMIT 1")
        .await;
    assert_eq!(old_val, "bye", "OLD.val should be 'bye'");
}

// ── Explicit DML: no-op skip via IS DISTINCT FROM ──────────────────────

#[tokio::test]
async fn test_explicit_dml_no_op_skip() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    // Use an aggregate ST so that source row changes may not change the aggregate.
    db.execute("CREATE TABLE src_agg (id INT PRIMARY KEY, grp TEXT, amount INT)")
        .await;
    db.execute("INSERT INTO src_agg VALUES (1, 'A', 10), (2, 'A', 20)")
        .await;

    db.create_st(
        "st_agg",
        "SELECT grp, sum(amount) AS total FROM src_agg GROUP BY grp",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh
    db.refresh_st("st_agg").await;

    // Attach audit trigger on the aggregate ST
    // The ST has columns (grp TEXT, total BIGINT) — adapt the trigger
    db.execute(
        "CREATE TABLE audit_agg (
            audit_id SERIAL PRIMARY KEY,
            op TEXT NOT NULL,
            old_grp TEXT,
            old_total BIGINT,
            new_grp TEXT,
            new_total BIGINT
        )",
    )
    .await;
    db.execute(
        "CREATE OR REPLACE FUNCTION audit_agg_fn()
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                INSERT INTO audit_agg (op, new_grp, new_total)
                VALUES ('INSERT', NEW.grp, NEW.total);
                RETURN NEW;
            ELSIF TG_OP = 'UPDATE' THEN
                INSERT INTO audit_agg (op, old_grp, old_total, new_grp, new_total)
                VALUES ('UPDATE', OLD.grp, OLD.total, NEW.grp, NEW.total);
                RETURN NEW;
            ELSIF TG_OP = 'DELETE' THEN
                INSERT INTO audit_agg (op, old_grp, old_total)
                VALUES ('DELETE', OLD.grp, OLD.total);
                RETURN OLD;
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql",
    )
    .await;
    db.execute(
        "CREATE TRIGGER audit_agg_trig
        AFTER INSERT OR UPDATE OR DELETE ON st_agg
        FOR EACH ROW EXECUTE FUNCTION audit_agg_fn()",
    )
    .await;
    db.execute("TRUNCATE audit_agg").await;

    // Update source such that the aggregate sum does NOT change for group A:
    // Change (1, 'A', 10) → (1, 'A', 15) and (2, 'A', 20) → (2, 'A', 15)
    // Total stays 30.
    db.execute("UPDATE src_agg SET amount = 15 WHERE id = 1")
        .await;
    db.execute("UPDATE src_agg SET amount = 15 WHERE id = 2")
        .await;
    db.refresh_st("st_agg").await;

    // The IS DISTINCT FROM guard should have prevented an UPDATE trigger
    // because the aggregate result (grp='A', total=30) is unchanged.
    let audit_count: i64 = db.count("audit_agg").await;
    assert_eq!(
        audit_count, 0,
        "No triggers should fire when aggregate result is unchanged, got {} entries",
        audit_count
    );
}

// ── No trigger → MERGE strategy ────────────────────────────────────────

#[tokio::test]
async fn test_no_trigger_uses_merge() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE src_merge (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_merge VALUES (1, 'x')").await;

    // Create ST without any user triggers
    db.create_st(
        "st_merge",
        "SELECT id, val FROM src_merge",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Insert new row
    db.execute("INSERT INTO src_merge VALUES (2, 'y')").await;
    db.refresh_st("st_merge").await;

    // Verify data is correct (MERGE path should work normally)
    let count: i64 = db.count("st_merge").await;
    assert_eq!(count, 2, "ST should have 2 rows after MERGE refresh");

    db.assert_st_matches_query("st_merge", "SELECT id, val FROM src_merge")
        .await;
}

// ── Audit trail: full scenario ─────────────────────────────────────────

#[tokio::test]
async fn test_trigger_audit_trail() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE src_audit (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_audit VALUES (1, 'first'), (2, 'second')")
        .await;

    db.create_st(
        "st_audit",
        "SELECT id, val FROM src_audit",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh
    db.refresh_st("st_audit").await;

    // Attach audit trigger
    for sql in audit_trigger_sql("st_audit") {
        db.execute(&sql).await;
    }
    db.execute("TRUNCATE audit_log").await;

    // Perform mixed DML on source: insert + update + delete
    db.execute("INSERT INTO src_audit VALUES (3, 'third')")
        .await;
    db.execute("UPDATE src_audit SET val = 'updated' WHERE id = 1")
        .await;
    db.execute("DELETE FROM src_audit WHERE id = 2").await;
    db.refresh_st("st_audit").await;

    // Verify ST matches expected state
    db.assert_st_matches_query("st_audit", "SELECT id, val FROM src_audit")
        .await;

    // Verify audit trail: should have INSERT(3), UPDATE(1), DELETE(2)
    let insert_count: i64 = db
        .query_scalar("SELECT count(*) FROM audit_log WHERE op = 'INSERT' AND new_id = 3")
        .await;
    assert!(insert_count >= 1, "Should have INSERT for id=3");

    let update_count: i64 = db
        .query_scalar("SELECT count(*) FROM audit_log WHERE op = 'UPDATE' AND old_id = 1")
        .await;
    assert!(update_count >= 1, "Should have UPDATE for id=1");

    let delete_count: i64 = db
        .query_scalar("SELECT count(*) FROM audit_log WHERE op = 'DELETE' AND old_id = 2")
        .await;
    assert!(delete_count >= 1, "Should have DELETE for id=2");
}

// ── GUC: off suppresses triggers ───────────────────────────────────────

#[tokio::test]
async fn test_guc_off_suppresses_triggers() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE src_guc_off (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_guc_off VALUES (1, 'a')").await;

    db.create_st(
        "st_guc_off",
        "SELECT id, val FROM src_guc_off",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh
    db.refresh_st("st_guc_off").await;
    db.create_st(
        "st_guc_off_child",
        "SELECT id, val FROM st_guc_off",
        "1m",
        "IMMEDIATE",
    )
    .await;

    // Attach trigger
    for sql in audit_trigger_sql("st_guc_off") {
        db.execute(&sql).await;
    }
    db.execute("TRUNCATE audit_log").await;

    // Modify source
    db.execute("INSERT INTO src_guc_off VALUES (2, 'b')").await;

    // Set GUC to 'off' and refresh on the SAME connection so the
    // session-level SET is visible to the refresh function.
    // This avoids ALTER SYSTEM which is global and races with parallel tests.
    db.execute_seq(&[
        "SET pg_trickle.user_triggers = 'off'",
        "SELECT pgtrickle.refresh_stream_table('st_guc_off')",
    ])
    .await;

    // ST should have the data
    let count: i64 = db.count("st_guc_off").await;
    assert_eq!(count, 2, "ST should have 2 rows");
    db.assert_st_matches_query("st_guc_off_child", "SELECT id, val FROM src_guc_off")
        .await;

    // Audit log should be EMPTY — triggers were suppressed
    let audit_count: i64 = db.count("audit_log").await;
    assert_eq!(
        audit_count, 0,
        "Audit log should be empty when GUC is 'off', got {} entries",
        audit_count
    );
}

// ── GUC: auto detects triggers ─────────────────────────────────────────

#[tokio::test]
async fn test_guc_auto_detects_triggers() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE src_guc_auto (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_guc_auto VALUES (1, 'a')").await;

    db.create_st(
        "st_guc_auto",
        "SELECT id, val FROM src_guc_auto",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh
    db.refresh_st("st_guc_auto").await;

    // GUC should default to 'auto'
    let guc_val = db.show_setting("pg_trickle.user_triggers").await;
    assert_eq!(guc_val, "auto", "Default GUC should be 'auto'");

    // Attach trigger — auto mode should detect it
    for sql in audit_trigger_sql("st_guc_auto") {
        db.execute(&sql).await;
    }
    db.execute("TRUNCATE audit_log").await;

    // Modify source and refresh
    db.execute("INSERT INTO src_guc_auto VALUES (2, 'b')").await;
    db.refresh_st("st_guc_auto").await;

    // Auto mode should have detected the trigger and used explicit DML
    let insert_count: i64 = db
        .query_scalar("SELECT count(*) FROM audit_log WHERE op = 'INSERT'")
        .await;
    assert!(
        insert_count >= 1,
        "Auto mode should detect trigger and fire it, got {} INSERT entries",
        insert_count
    );
}

// ── GUC: deprecated on alias still detects triggers ───────────────────

#[tokio::test]
async fn test_guc_on_alias_detects_triggers() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE src_guc_on (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_guc_on VALUES (1, 'a')").await;

    db.create_st(
        "st_guc_on",
        "SELECT id, val FROM src_guc_on",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    db.refresh_st("st_guc_on").await;

    for sql in audit_trigger_sql("st_guc_on") {
        db.execute(&sql).await;
    }
    db.execute("TRUNCATE audit_log").await;

    // Set GUC to deprecated 'on' alias and refresh on the SAME connection.
    // 'on' is treated as 'auto' — should still detect triggers.
    db.execute("INSERT INTO src_guc_on VALUES (2, 'b')").await;
    db.execute_seq(&[
        "SET pg_trickle.user_triggers = 'on'",
        "SELECT pgtrickle.refresh_stream_table('st_guc_on')",
    ])
    .await;

    let insert_count: i64 = db
        .query_scalar("SELECT count(*) FROM audit_log WHERE op = 'INSERT'")
        .await;
    assert!(
        insert_count >= 1,
        "Deprecated 'on' alias should still detect trigger and fire it, got {} INSERT entries",
        insert_count
    );
}

// ── FULL refresh suppresses row-level triggers ─────────────────────────

#[tokio::test]
async fn test_full_refresh_suppresses_triggers() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE src_full (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_full VALUES (1, 'a'), (2, 'b')")
        .await;

    // Create a FULL mode stream table
    db.create_st("st_full", "SELECT id, val FROM src_full", "1m", "FULL")
        .await;
    db.create_st(
        "st_full_child",
        "SELECT id, val FROM st_full",
        "1m",
        "IMMEDIATE",
    )
    .await;

    // Attach audit trigger
    for sql in audit_trigger_sql("st_full") {
        db.execute(&sql).await;
    }
    db.execute(
        "CREATE FUNCTION audit_truncate_fn() RETURNS trigger AS $$
         BEGIN INSERT INTO audit_log (op) VALUES ('TRUNCATE'); RETURN NULL; END;
         $$ LANGUAGE plpgsql",
    )
    .await;
    db.execute(
        "CREATE TRIGGER audit_truncate AFTER TRUNCATE ON st_full \
         FOR EACH STATEMENT EXECUTE FUNCTION audit_truncate_fn()",
    )
    .await;
    db.execute("TRUNCATE audit_log").await;

    // Modify source and do a FULL refresh
    db.execute("INSERT INTO src_full VALUES (3, 'c')").await;
    db.refresh_st("st_full").await;

    // ST should have all 3 rows
    let count: i64 = db.count("st_full").await;
    assert_eq!(count, 3, "ST should have 3 rows after FULL refresh");
    db.assert_st_matches_query("st_full_child", "SELECT id, val FROM src_full")
        .await;

    // Audit log should be EMPTY — FULL refresh suppresses row-level triggers
    let audit_count: i64 = db.count("audit_log").await;
    assert_eq!(
        audit_count, 0,
        "FULL refresh should suppress row-level triggers, got {} entries",
        audit_count
    );

    db.execute("DELETE FROM src_full").await;
    db.refresh_st("st_full").await;
    assert_eq!(db.count("st_full").await, 0);
    db.assert_st_matches_query("st_full_child", "SELECT id, val FROM src_full")
        .await;
    assert_eq!(
        db.count("audit_log").await,
        0,
        "empty FULL refresh must suppress application INSERT and TRUNCATE triggers"
    );
}

#[tokio::test]
async fn test_full_refresh_suppresses_reserved_prefix_only_application_triggers() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.execute("CREATE TABLE prefix_only_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO prefix_only_src VALUES (1, 10)")
        .await;
    db.create_st(
        "prefix_only_upstream",
        "SELECT id, val FROM prefix_only_src",
        "1s",
        "FULL",
    )
    .await;
    db.create_st(
        "prefix_only_child",
        "SELECT id, val * 2 AS doubled FROM prefix_only_upstream",
        "1m",
        "IMMEDIATE",
    )
    .await;
    db.execute("CREATE TABLE prefix_only_audit (trigger_name TEXT NOT NULL)")
        .await;
    db.execute(
        "CREATE FUNCTION prefix_only_audit_fn() RETURNS trigger AS $$
         BEGIN INSERT INTO prefix_only_audit VALUES (TG_NAME); RETURN NEW; END;
         $$ LANGUAGE plpgsql",
    )
    .await;
    for name in ["pgt_only_app", "pg_trickle_only_app"] {
        db.execute(&format!(
            "CREATE TRIGGER {name} AFTER INSERT ON prefix_only_upstream \
             FOR EACH ROW EXECUTE FUNCTION prefix_only_audit_fn()"
        ))
        .await;
    }

    db.execute("INSERT INTO prefix_only_src VALUES (2, 20)")
        .await;
    db.refresh_st("prefix_only_upstream").await;
    assert_eq!(
        db.count("prefix_only_audit").await,
        0,
        "manual FULL must suppress app triggers even when their names use internal prefixes"
    );
    db.assert_st_matches_query(
        "prefix_only_child",
        "SELECT id, val * 2 AS doubled FROM prefix_only_src",
    )
    .await;

    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "scheduler should be running"
    );
    let previous_refresh_id: i64 = db
        .query_scalar(
            "SELECT COALESCE(max(h.refresh_id), 0) \
             FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = 'prefix_only_upstream'",
        )
        .await;
    db.execute("INSERT INTO prefix_only_src VALUES (3, 30)")
        .await;
    assert!(
        db.wait_for_condition(
            "scheduled FULL with prefix-only application triggers",
            &format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
                 WHERE st.pgt_name = 'prefix_only_upstream' \
                   AND h.refresh_id > {previous_refresh_id} \
                   AND h.action = 'FULL' AND h.status = 'COMPLETED' \
                   AND h.initiated_by = 'SCHEDULER')"
            ),
            Duration::from_secs(45),
            Duration::from_millis(100),
        )
        .await,
        "scheduled FULL history should show the actual refresh"
    );
    assert_eq!(
        db.count("prefix_only_audit").await,
        0,
        "scheduled FULL must suppress app triggers even when their names use internal prefixes"
    );
    db.assert_st_matches_query(
        "prefix_only_child",
        "SELECT id, val * 2 AS doubled FROM prefix_only_src",
    )
    .await;
}

#[tokio::test]
async fn test_user_triggers_full_refresh_preserves_enabled_states() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    db.execute("CREATE TABLE full_state_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO full_state_src VALUES (1, 10)")
        .await;
    db.create_st(
        "full_state_upstream",
        "SELECT id, val FROM full_state_src",
        "1s",
        "FULL",
    )
    .await;
    db.refresh_st("full_state_upstream").await;
    db.create_st(
        "full_state_child",
        "SELECT id, val * 2 AS doubled FROM full_state_upstream",
        "1m",
        "IMMEDIATE",
    )
    .await;
    db.execute(
        "CREATE FUNCTION full_state_noop() RETURNS trigger AS $$
         BEGIN IF TG_LEVEL = 'ROW' THEN RETURN NEW; END IF; RETURN NULL; END;
         $$ LANGUAGE plpgsql",
    )
    .await;
    db.execute("CREATE TABLE full_state_audit (trigger_name TEXT NOT NULL)")
        .await;
    db.execute(
        "CREATE FUNCTION full_state_audit_trigger() RETURNS trigger AS $$
         BEGIN
           INSERT INTO full_state_audit VALUES (TG_NAME);
           IF TG_LEVEL = 'ROW' THEN RETURN NEW; END IF;
           RETURN NULL;
         END;
         $$ LANGUAGE plpgsql",
    )
    .await;
    for (name, event) in [
        (
            "app_ordinary",
            "BEFORE INSERT ON full_state_upstream FOR EACH ROW",
        ),
        (
            "app_disabled",
            "AFTER UPDATE ON full_state_upstream FOR EACH ROW",
        ),
        (
            "app_replica",
            "AFTER DELETE ON full_state_upstream FOR EACH ROW",
        ),
        (
            "app_always",
            "AFTER TRUNCATE ON full_state_upstream FOR EACH STATEMENT",
        ),
    ] {
        db.execute(&format!(
            "CREATE TRIGGER {name} {event} EXECUTE FUNCTION full_state_noop()"
        ))
        .await;
    }
    db.execute("ALTER TABLE full_state_upstream DISABLE TRIGGER app_disabled")
        .await;
    db.execute("ALTER TABLE full_state_upstream ENABLE REPLICA TRIGGER app_replica")
        .await;
    db.execute("ALTER TABLE full_state_upstream ENABLE ALWAYS TRIGGER app_always")
        .await;
    for name in ["pg_trickle_audit", "pgt_audit"] {
        db.execute(&format!(
            "CREATE TRIGGER {name} AFTER INSERT ON full_state_upstream \
             FOR EACH ROW EXECUTE FUNCTION full_state_audit_trigger()"
        ))
        .await;
    }

    let expected_states =
        "app_always:A,app_disabled:D,app_ordinary:O,app_replica:R,pg_trickle_audit:O,pgt_audit:O";
    let actual_states: String = db
        .query_scalar(
            "SELECT string_agg(tgname::text || ':' || tgenabled::text, ',' ORDER BY tgname) \
             FROM pg_catalog.pg_trigger WHERE tgrelid = 'full_state_upstream'::regclass \
               AND (tgname LIKE 'app_%' OR tgname IN ('pg_trickle_audit', 'pgt_audit'))",
        )
        .await;
    assert_eq!(actual_states, expected_states, "seed trigger modes");

    db.execute("INSERT INTO full_state_src VALUES (2, 20)")
        .await;
    db.refresh_st("full_state_upstream").await;
    let actual_states: String = db
        .query_scalar(
            "SELECT string_agg(tgname::text || ':' || tgenabled::text, ',' ORDER BY tgname) \
             FROM pg_catalog.pg_trigger WHERE tgrelid = 'full_state_upstream'::regclass \
               AND (tgname LIKE 'app_%' OR tgname IN ('pg_trickle_audit', 'pgt_audit'))",
        )
        .await;
    assert_eq!(actual_states, expected_states, "manual FULL refresh");
    let audit_rows: i64 = db.count("full_state_audit").await;
    assert_eq!(
        audit_rows, 0,
        "manual FULL suppresses reserved-prefix application triggers"
    );
    let maintenance_triggers: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_catalog.pg_trigger \
             WHERE tgrelid = 'full_state_upstream'::regclass \
               AND tgname LIKE 'pgt_ivm_%' AND tgenabled = 'O'",
        )
        .await;
    assert_eq!(
        maintenance_triggers, 8,
        "manual FULL must leave IVM triggers active"
    );
    db.assert_st_matches_query(
        "full_state_child",
        "SELECT id, val * 2 AS doubled FROM full_state_src",
    )
    .await;

    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 100")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.reload_config_and_wait().await;
    assert!(
        db.wait_for_scheduler(Duration::from_secs(90)).await,
        "scheduler should be running"
    );
    let previous_refresh_id: i64 = db
        .query_scalar(
            "SELECT COALESCE(max(h.refresh_id), 0) \
             FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
             WHERE st.pgt_name = 'full_state_upstream'",
        )
        .await;
    db.execute("INSERT INTO full_state_src VALUES (3, 30)")
        .await;
    assert!(
        db.wait_for_condition(
            "scheduled FULL with application triggers",
            &format!(
                "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables st USING (pgt_id) \
                 WHERE st.pgt_name = 'full_state_upstream' \
                   AND h.refresh_id > {previous_refresh_id} \
                   AND h.action = 'FULL' AND h.status = 'COMPLETED' \
                   AND h.initiated_by = 'SCHEDULER')"
            ),
            Duration::from_secs(45),
            Duration::from_millis(100),
        )
        .await,
        "scheduled FULL history should show the actual refresh"
    );
    let actual_states: String = db
        .query_scalar(
            "SELECT string_agg(tgname::text || ':' || tgenabled::text, ',' ORDER BY tgname) \
             FROM pg_catalog.pg_trigger WHERE tgrelid = 'full_state_upstream'::regclass \
               AND (tgname LIKE 'app_%' OR tgname IN ('pg_trickle_audit', 'pgt_audit'))",
        )
        .await;
    assert_eq!(actual_states, expected_states, "scheduled FULL refresh");
    let audit_rows: i64 = db.count("full_state_audit").await;
    assert_eq!(
        audit_rows, 0,
        "scheduled FULL suppresses reserved-prefix application triggers"
    );
    let maintenance_triggers: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pg_catalog.pg_trigger \
             WHERE tgrelid = 'full_state_upstream'::regclass \
               AND tgname LIKE 'pgt_ivm_%' AND tgenabled = 'O'",
        )
        .await;
    assert_eq!(
        maintenance_triggers, 8,
        "scheduled FULL must leave IVM triggers active"
    );
    db.assert_st_matches_query(
        "full_state_child",
        "SELECT id, val * 2 AS doubled FROM full_state_src",
    )
    .await;
}

// ── BEFORE trigger modifies NEW ────────────────────────────────────────

#[tokio::test]
async fn test_before_trigger_modifies_new() {
    let db = E2eDb::new().await.with_extension().await;
    disable_scheduler(&db).await;

    db.execute("CREATE TABLE src_before (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO src_before VALUES (1, 'hello')")
        .await;

    db.create_st(
        "st_before",
        "SELECT id, val FROM src_before",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Initial refresh
    db.refresh_st("st_before").await;

    // Attach a BEFORE UPDATE trigger that uppercases val
    db.execute(
        "CREATE OR REPLACE FUNCTION uppercase_fn()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.val := upper(NEW.val);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql",
    )
    .await;
    db.execute(
        "CREATE TRIGGER uppercase_trig
        BEFORE UPDATE ON st_before
        FOR EACH ROW EXECUTE FUNCTION uppercase_fn()",
    )
    .await;

    // Update source and refresh
    db.execute("UPDATE src_before SET val = 'world' WHERE id = 1")
        .await;
    db.refresh_st("st_before").await;

    // The BEFORE trigger should have uppercased the value
    let val: String = db
        .query_scalar("SELECT val FROM st_before WHERE id = 1")
        .await;
    assert_eq!(
        val, "WORLD",
        "BEFORE UPDATE trigger should uppercase the value"
    );
}
