//! PostgreSQL-backed checks for the v0.87.16 row-identity V2 integration.

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_row_id_v2_is_the_only_stable_function_admitted_by_resolved_identity() {
    let db = E2eDb::new().await.with_extension().await;
    let encoder_is_owned_and_parallel_safe: bool = db
        .query_scalar(
            "SELECT p.proparallel = 's' AND p.provolatile = 's' AND EXISTS (\
                 SELECT 1 FROM pg_catalog.pg_depend d \
                 JOIN pg_catalog.pg_extension e ON e.oid = d.refobjid \
                 WHERE d.classid = 'pg_catalog.pg_proc'::regclass \
                   AND d.objid = p.oid AND d.deptype = 'e' \
                   AND e.extname = 'pg_trickle'\
             ) FROM pg_catalog.pg_proc p \
             WHERE p.oid = 'pgtrickle.encode_row_id_v2(text,anyelement)'::regprocedure",
        )
        .await;
    assert!(encoder_is_owned_and_parallel_safe);

    db.execute("CREATE TABLE row_id_admission_source (id int PRIMARY KEY, value text)")
        .await;
    db.execute("INSERT INTO row_id_admission_source VALUES (1, 'a')")
        .await;
    db.create_st(
        "row_id_admission",
        "SELECT id, value, pgtrickle.encode_row_id_v2(\
             'MDM_SOURCE_KEY_V1', ROW(id, value)) AS encoded_id \
         FROM row_id_admission_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.pgt_status("row_id_admission").await.1, "DIFFERENTIAL");

    for mutation in [
        "INSERT INTO row_id_admission_source VALUES (2, 'b')",
        "UPDATE row_id_admission_source SET value = 'updated' WHERE id = 1",
        "DELETE FROM row_id_admission_source WHERE id = 2",
    ] {
        db.execute(mutation).await;
        db.refresh_st("row_id_admission").await;
        db.assert_st_matches_query(
            "public.row_id_admission",
            "SELECT id, value, pgtrickle.encode_row_id_v2(\
                 'MDM_SOURCE_KEY_V1', ROW(id, value)) AS encoded_id \
             FROM row_id_admission_source",
        )
        .await;
    }
    let mut transaction = db.pool.begin().await.expect("begin rollback probe");
    sqlx::query("INSERT INTO row_id_admission_source VALUES (3, 'rolled back')")
        .execute(&mut *transaction)
        .await
        .expect("insert rollback probe");
    transaction.rollback().await.expect("rollback probe");
    db.refresh_st_with_retry("row_id_admission").await;
    db.assert_st_matches_query(
        "public.row_id_admission",
        "SELECT id, value, pgtrickle.encode_row_id_v2(\
             'MDM_SOURCE_KEY_V1', ROW(id, value)) AS encoded_id \
         FROM row_id_admission_source",
    )
    .await;

    db.execute("CREATE SCHEMA impostor").await;
    db.execute(
        "CREATE FUNCTION impostor.encode_row_id_v2(text, anyelement) RETURNS bytea \
         LANGUAGE sql STABLE PARALLEL SAFE AS 'SELECT decode(md5($1), ''hex'')'",
    )
    .await;
    let impostor = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                 'row_id_impostor', \
                 $$SELECT id, impostor.encode_row_id_v2('x', ROW(id)) \
                   FROM row_id_admission_source$$, \
                 '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        impostor.is_err(),
        "same-named stable function must be rejected"
    );

    db.execute("CREATE TABLE unsupported_identity_source (id int PRIMARY KEY, tags int[])")
        .await;
    db.execute("INSERT INTO unsupported_identity_source VALUES (1, ARRAY[1, 2])")
        .await;
    let unsupported_type = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                 'unsupported_identity', \
                 $$SELECT pgtrickle.encode_row_id_v2('MDM_SOURCE_KEY_V1', ROW(tags)) \
                   FROM unsupported_identity_source$$, \
                 '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        unsupported_type.is_err(),
        "unsupported identity type must fail during registration"
    );

    db.execute(
        "CREATE COLLATION row_id_nondeterministic (\
             provider = icu, locale = 'und', deterministic = false); \
         CREATE TABLE nondeterministic_identity_source (\
             id int PRIMARY KEY, value text COLLATE row_id_nondeterministic)",
    )
    .await;
    db.execute("INSERT INTO nondeterministic_identity_source VALUES (1, 'a')")
        .await;
    let nondeterministic_collation = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                 'nondeterministic_identity', \
                 $$SELECT pgtrickle.encode_row_id_v2('MDM_SOURCE_KEY_V1', ROW(value)) \
                   FROM nondeterministic_identity_source$$, \
                 '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        nondeterministic_collation.is_err(),
        "non-deterministic identity collation must fail during registration"
    );
}

#[tokio::test]
async fn test_row_id_v2_sql_entry_points_are_exact_and_bounded() {
    let db = E2eDb::new().await.with_extension().await;

    let identity: Vec<u8> = db
        .query_scalar(
            "SELECT pgtrickle.encode_row_id_v2(\
             'SCAN_KEY', ROW(1::int4, 'a'::text))",
        )
        .await;
    assert_eq!(
        identity,
        vec![
            0x02, 0x01, 0, 0, 0, 2, 0x03, 0x01, 0x80, 0, 0, 1, 0x09, 0x01, b'a', 0, 0, 0xff,
        ]
    );

    let mdm_identity: Vec<u8> = db
        .query_scalar(
            "SELECT pgtrickle.encode_row_id_v2(\
             'MDM_SOURCE_KEY_V1', ROW(1::int4))",
        )
        .await;
    assert_eq!(
        mdm_identity,
        vec![
            0x02, 0x08, 0x00, 0x00, 0x00, 0x01, 0x03, 0x01, 0x80, 0x00, 0x00, 0x01, 0xff,
        ]
    );

    let numeric_equal: bool = db
        .query_scalar(
            "SELECT pgtrickle.encode_row_id_v2('SCAN_KEY', ROW(1.00::numeric)) = \
             pgtrickle.encode_row_id_v2('SCAN_KEY', ROW(1.000::numeric))",
        )
        .await;
    assert!(numeric_equal);

    let numeric_negative: String = db
        .query_scalar(
            "SELECT encode(pgtrickle.encode_row_id_v2(\
             'SCAN_KEY', ROW(-12.30::numeric)), 'hex')",
        )
        .await;
    assert_eq!(
        numeric_negative,
        "0201000000010801017ffffffefffffffccecdccff"
    );

    let domains_disjoint: bool = db
        .query_scalar(
            "SELECT pgtrickle.encode_row_id_v2('SCAN_KEY', ROW(1::int4)) <> \
             pgtrickle.encode_row_id_v2('GROUP_KEY', ROW(1::int4))",
        )
        .await;
    assert!(domains_disjoint);

    let probe: Vec<u8> = db
        .query_scalar("SELECT pgtrickle.row_probe_v1(decode(repeat('ab', 129), 'hex'))")
        .await;
    assert_eq!(probe.len(), 144);
    assert_eq!(&probe[..128], &[0xab; 128]);

    let rejected = db
        .try_execute("SELECT pgtrickle.encode_row_id_v2('SCAN_KEY', ROW(ARRAY[1, 2]::int4[]))")
        .await;
    assert!(
        rejected.is_err(),
        "unsupported array identity must be rejected"
    );
}

#[tokio::test]
async fn test_computed_mdm_source_key_keeps_project_rows_distinct() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute(
        "CREATE TABLE computed_key_identity (entity_id uuid PRIMARY KEY); \
         INSERT INTO computed_key_identity VALUES \
             ('10000000-0000-0000-0000-000000000001'); \
         CREATE TABLE computed_key_source ( \
             id integer PRIMARY KEY, name text NOT NULL, deleted boolean NOT NULL); \
         INSERT INTO computed_key_source VALUES \
             (1, 'Alice', false), (2, 'Bob', false)",
    )
    .await;
    let defining_query = "SELECT 'crm'::text AS source_name, \
                pgtrickle.encode_row_id_v2('MDM_SOURCE_KEY_V1', ROW( \
                    (SELECT entity_id FROM computed_key_identity), id)) AS source_record_key, \
                name \
           FROM computed_key_source \
          WHERE deleted IS NOT TRUE";
    db.create_st("computed_key_stream", defining_query, "1m", "DIFFERENTIAL")
        .await;

    for mutation in [
        "INSERT INTO computed_key_source VALUES (3, 'Carol', false)",
        "UPDATE computed_key_source SET name = 'Alice Updated' WHERE id = 1",
        "DELETE FROM computed_key_source WHERE id = 2",
    ] {
        db.execute(mutation).await;
        db.refresh_st("computed_key_stream").await;
        db.assert_st_matches_query("public.computed_key_stream", defining_query)
            .await;
    }
}

#[tokio::test]
async fn test_row_id_v2_sql_entry_point_accepts_supported_scalar_families() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TYPE row_id_v2_test_enum AS ENUM ('alpha', 'beta')")
        .await;

    let identity: Vec<u8> = db
        .query_scalar(
            "SELECT pgtrickle.encode_row_id_v2('SYNTHETIC', ROW(\
                true::bool, 1::int2, 1::int4, 1::int8, 1::oid,\
                1.0::float4, 1.0::float8, 1.0::numeric, 'a'::text,\
                'a'::varchar, 'a'::bpchar, decode('0001', 'hex')::bytea,\
                '00112233-4455-6677-8899-aabbccddeeff'::uuid, DATE '2000-01-01',\
                TIME '12:34:56', TIMESTAMP '2000-01-01 12:34:56',\
                TIMESTAMPTZ '2000-01-01 12:34:56+00', TIMETZ '12:34:56+00',\
                INTERVAL '1 day', inet '192.0.2.1/24', cidr '192.0.2.0/24',\
                '08:00:2b:01:02:03'::macaddr, '08:00:2b:01:02:03:04:05'::macaddr8,\
                B'101'::bit(3), B'101'::varbit))",
        )
        .await;
    assert!(!identity.is_empty());

    let enum_identity: Vec<u8> = db
        .query_scalar(
            "SELECT pgtrickle.encode_row_id_v2(\
             'SYNTHETIC', ROW('alpha'::row_id_v2_test_enum))",
        )
        .await;
    assert!(!enum_identity.is_empty());
}

#[tokio::test]
async fn test_row_id_v2_recreation_preflight_is_read_only_and_requires_ack() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute("CREATE TABLE preflight_source (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO preflight_source VALUES (1, 'a')")
        .await;
    db.create_st(
        "preflight_stream",
        "SELECT id, val FROM preflight_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    let stream_tables_before: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables")
        .await;
    let buffers_before: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_change_buffers")
        .await;

    db.execute("SELECT pgtrickle.row_identity_v2_record_inventory('e2e preflight inventory')")
        .await;
    let consumer_id: i64 = db
        .query_scalar(
            "SELECT pgtrickle.row_identity_v2_register_consumer(\
             'e2e-consumer', 'postgres', ARRAY['public.preflight_stream'],\
             true, true, 'Change the downstream identity to BYTEA and resnapshot')",
        )
        .await;
    db.execute(&format!(
        "SELECT pgtrickle.row_identity_v2_acknowledge_consumer({consumer_id}, 'PENDING')"
    ))
    .await;
    let report: String = db
        .query_scalar("SELECT pgtrickle.row_identity_v2_recreation_preflight()::text")
        .await;
    assert!(report.contains("EXTERNAL_CONSUMER_INVENTORY"));
    assert!(report.contains("SCHEDULER_PAUSED"));
    let inventory_ok: bool = db
        .query_scalar(
            "SELECT (SELECT (check_item ->> 'ok')::boolean \
             FROM jsonb_array_elements( \
                 pgtrickle.row_identity_v2_recreation_preflight() -> 'checks' \
             ) AS checks(check_item) \
             WHERE check_item ->> 'check' = 'EXTERNAL_CONSUMER_INVENTORY')",
        )
        .await;
    assert!(!inventory_ok);

    db.execute("SELECT pgtrickle.row_identity_v2_acknowledge_inventory()")
        .await;
    let acknowledged_report: String = db
        .query_scalar("SELECT pgtrickle.row_identity_v2_recreation_preflight()::text")
        .await;
    assert!(acknowledged_report.contains("EXTERNAL_CONSUMER_INVENTORY"));
    let inventory_ok_after_ack: bool = db
        .query_scalar(
            "SELECT (SELECT (check_item ->> 'ok')::boolean \
             FROM jsonb_array_elements( \
                 pgtrickle.row_identity_v2_recreation_preflight() -> 'checks' \
             ) AS checks(check_item) \
             WHERE check_item ->> 'check' = 'EXTERNAL_CONSUMER_INVENTORY')",
        )
        .await;
    assert!(inventory_ok_after_ack);

    let stream_tables_after: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_stream_tables")
        .await;
    let buffers_after: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_change_buffers")
        .await;
    assert_eq!(stream_tables_before, stream_tables_after);
    assert_eq!(buffers_before, buffers_after);
}
