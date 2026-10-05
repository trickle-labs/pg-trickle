//! PostgreSQL-backed checks for the v0.87.16 row-identity V2 integration.

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_row_id_v2_non_composite_inputs_error_and_backend_survives() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TYPE row_id_guard_pair AS (id int4, value text)",
        "CREATE TYPE row_id_guard_enum AS ENUM ('x')",
        "CREATE DOMAIN row_id_guard_int AS int4",
        "CREATE DOMAIN row_id_guard_array AS int4[]",
    ])
    .await;
    let mut connection = db.pool.acquire().await.expect("pin probe connection");
    for argument in [
        "1::int4",
        "true::bool",
        "'x'::text",
        "ARRAY[1,2]::int4[]",
        "ARRAY[]::int4[]",
        "ARRAY[ROW(1, 'a')::row_id_guard_pair]",
        "'x'::row_id_guard_enum",
        "'{}'::jsonb",
        "int4range(1,2)",
        "1::row_id_guard_int",
        "ARRAY[1,2]::row_id_guard_array",
    ] {
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *connection)
            .await
            .expect("probe backend PID");
        let error = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT pgtrickle.encode_row_id_v2('SCAN_KEY', {argument})"
        )))
        .execute(&mut *connection)
        .await
        .expect_err(argument);
        let diagnostic = error
            .as_database_error()
            .expect("must receive a PostgreSQL error, not a transport failure")
            .downcast_ref::<sqlx::postgres::PgDatabaseError>();
        assert_eq!(diagnostic.severity(), sqlx::postgres::PgSeverity::Error);
        assert!(
            diagnostic.code() == "42804" || diagnostic.code().starts_with("22"),
            "{argument}: {diagnostic}"
        );
        assert!(
            diagnostic.message().contains("record") || diagnostic.message().contains("composite"),
            "{argument}: {diagnostic}"
        );
        println!("{argument}: {} {diagnostic}", diagnostic.code());
        let survived: (i32, i32) = sqlx::query_as("SELECT pg_backend_pid(), 1")
            .fetch_one(&mut *connection)
            .await
            .expect("same pinned backend must survive the rejected argument");
        assert_eq!(survived, (pid, 1), "{argument}: backend changed");
    }
}

#[tokio::test]
async fn test_row_id_v2_anonymous_records_keep_exact_bytes() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    for (argument, expected) in [
        ("ROW(1::int4)", "020100000001030180000001ff"),
        (
            "ROW(1::int4, 'a'::text)",
            "0201000000020301800000010901610000ff",
        ),
        ("ROW(NULL::int4, NULL::text)", "02010000000203000900ff"),
    ] {
        let hex: String = db
            .query_scalar(&format!(
                "SELECT encode(pgtrickle.encode_row_id_v2('SCAN_KEY', {argument}), 'hex')"
            ))
            .await;
        assert_eq!(hex, expected, "{argument}");
    }
}

#[tokio::test]
async fn test_row_id_v2_named_composites_keep_exact_bytes() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TYPE row_id_guard_pair AS (id int4, value text)",
        "CREATE TABLE row_id_guard_table (id int4, value text)",
        "INSERT INTO row_id_guard_table VALUES (1, 'a')",
    ])
    .await;
    for query in [
        "SELECT encode(pgtrickle.encode_row_id_v2(\
             'SCAN_KEY', ROW(1::int4, 'a'::text)::row_id_guard_pair), 'hex')",
        "SELECT encode(pgtrickle.encode_row_id_v2(\
             'SCAN_KEY', t), 'hex') FROM row_id_guard_table t",
    ] {
        let hex: String = db.query_scalar(query).await;
        assert_eq!(hex, "0201000000020301800000010901610000ff");
    }
}

#[tokio::test]
async fn test_row_id_v2_composite_domain_uses_approved_policy() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TYPE row_id_guard_pair AS (id int4, value text)",
        "CREATE DOMAIN row_id_guard_pair_domain AS row_id_guard_pair",
    ])
    .await;
    let domain: String = db
        .query_scalar(
            "SELECT encode(pgtrickle.encode_row_id_v2('SCAN_KEY', \
             (ROW(1::int4, 'a'::text)::row_id_guard_pair)::row_id_guard_pair_domain), 'hex')",
        )
        .await;
    let base: String = db
        .query_scalar(
            "SELECT encode(pgtrickle.encode_row_id_v2(\
             'SCAN_KEY', ROW(1::int4, 'a'::text)::row_id_guard_pair), 'hex')",
        )
        .await;
    assert_eq!(domain, "0201000000020301800000010901610000ff");
    assert_eq!(base, "0201000000020301800000010901610000ff");
    assert_eq!(domain, base);
}

#[tokio::test]
async fn test_row_identity_v2_and_v3_encoders_are_admitted_by_resolved_identity() {
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
         FROM public.row_id_admission_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.pgt_status("row_id_admission").await.1, "DIFFERENTIAL");

    db.create_st(
        "row_id_admission_v3",
        "SELECT id, value, pgtrickle.encode_row_id_v3(\
             'MDM_SOURCE_KEY_V1', ROW(id, value)) AS encoded_id \
         FROM public.row_id_admission_source",
        "1m",
        "DIFFERENTIAL",
    )
    .await;
    assert_eq!(db.pgt_status("row_id_admission_v3").await.1, "DIFFERENTIAL");

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
             FROM public.row_id_admission_source",
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
             FROM public.row_id_admission_source",
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
                   FROM public.row_id_admission_source$$, \
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
                   FROM public.unsupported_identity_source$$, \
                 '1m', 'DIFFERENTIAL')",
        )
        .await;
    assert!(
        unsupported_type.is_err(),
        "unsupported identity type must fail during registration"
    );

    db.execute_seq(&[
        "CREATE COLLATION row_id_nondeterministic (\
             provider = icu, locale = 'und', deterministic = false)",
        "CREATE TABLE nondeterministic_identity_source (\
             id int PRIMARY KEY, value text COLLATE row_id_nondeterministic)",
    ])
    .await;
    db.execute("INSERT INTO nondeterministic_identity_source VALUES (1, 'a')")
        .await;
    let nondeterministic_collation = db
        .try_execute(
            "SELECT pgtrickle.create_stream_table(\
                 'nondeterministic_identity', \
                 $$SELECT pgtrickle.encode_row_id_v2('MDM_SOURCE_KEY_V1', ROW(value)) \
                   FROM public.nondeterministic_identity_source$$, \
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
async fn test_projected_source_key_differential_update_removes_full_row() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TABLE projected_key_source (id bigint PRIMARY KEY, name text NOT NULL)",
        "INSERT INTO projected_key_source VALUES (1, 'old')",
    ])
    .await;
    let query = "SELECT 'crm'::text AS source_name, \
        pgtrickle.encode_row_id_v2('SCAN_KEY', ROW(id)) AS source_record_key, name \
        FROM public.projected_key_source";
    db.create_st("projected_key_stream", query, "1m", "DIFFERENTIAL")
        .await;
    for (mutation, expected) in [
        (
            "UPDATE projected_key_source SET name = 'new' WHERE id = 1",
            "[\"new\"]",
        ),
        ("DELETE FROM projected_key_source WHERE id = 1", "[]"),
    ] {
        db.execute(mutation).await;
        db.refresh_st("projected_key_stream").await;
        let actual: String = db
            .query_scalar("SELECT COALESCE(jsonb_agg(name ORDER BY name), '[]'::jsonb)::text FROM public.projected_key_stream")
            .await;
        assert_eq!(actual, expected);
        db.assert_st_matches_query("public.projected_key_stream", query)
            .await;
    }
}

#[tokio::test]
async fn test_projected_source_without_pk_preserves_duplicate_values() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TABLE projected_duplicates (id bigint PRIMARY KEY, name text NOT NULL)",
        "INSERT INTO projected_duplicates VALUES (1, 'same'), (2, 'same')",
    ])
    .await;
    let query = "SELECT name FROM public.projected_duplicates";
    db.create_st("projected_duplicate_stream", query, "1m", "DIFFERENTIAL")
        .await;
    for mutation in [
        "UPDATE projected_duplicates SET name = 'new' WHERE id = 1",
        "DELETE FROM projected_duplicates WHERE id = 2",
    ] {
        db.execute(mutation).await;
        db.refresh_st("projected_duplicate_stream").await;
        db.assert_st_matches_query("public.projected_duplicate_stream", query)
            .await;
    }
}

#[tokio::test]
async fn test_computed_mdm_source_key_keeps_project_rows_distinct() {
    let db = E2eDb::new().await.with_extension().await;
    db.execute_seq(&[
        "CREATE TABLE computed_key_identity (entity_id uuid PRIMARY KEY)",
        "INSERT INTO computed_key_identity VALUES \
             ('10000000-0000-0000-0000-000000000001')",
        "CREATE TABLE computed_key_source ( \
             id integer PRIMARY KEY, name text NOT NULL, deleted boolean NOT NULL)",
        "INSERT INTO computed_key_source VALUES \
             (1, 'Alice', false), (2, 'Bob', false)",
    ])
    .await;
    let defining_query = "SELECT 'crm'::text AS source_name, \
                pgtrickle.encode_row_id_v2('MDM_SOURCE_KEY_V1', ROW( \
                    (SELECT entity_id FROM public.computed_key_identity), id)) AS source_record_key, \
                name \
           FROM public.computed_key_source \
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
        "SELECT id, val FROM public.preflight_source",
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

#[tokio::test]
async fn test_row_id_v2_bpchar_identity_matches_postgres_equality() {
    let db = E2eDb::new_dedicated().await.with_extension().await;
    let comparisons: Vec<(bool, bool, i32)> = sqlx::query_as(
        "WITH cases(name, left_value, right_value) AS (VALUES \
            ('trailing_spaces', 'x'::text, 'x  '::text), \
            ('all_spaces', ' '::text, '   '::text), \
            ('tab_equal_after_space', ('x' || chr(9))::text, ('x' || chr(9) || ' ')::text), \
            ('tab_significant', 'x'::text, ('x' || chr(9))::text), \
            ('lf_significant', 'x'::text, ('x' || chr(10))::text), \
            ('vt_significant', 'x'::text, ('x' || chr(11))::text), \
            ('ff_significant', 'x'::text, ('x' || chr(12))::text), \
            ('cr_significant', 'x'::text, ('x' || chr(13))::text), \
            ('interior_space', 'a b'::text, 'ab'::text), \
            ('interior_and_trailing_space', 'a b '::text, 'a b'::text), \
            ('different_prefix', 'abc'::text, 'abcd'::text) \
        ) \
        SELECT left_value::bpchar = right_value::bpchar, \
               pgtrickle.encode_row_id_v3('SCAN_KEY', ROW(left_value::bpchar)) = \
                   pgtrickle.encode_row_id_v3('SCAN_KEY', ROW(right_value::bpchar)), \
               get_byte(pgtrickle.encode_row_id_v3('SCAN_KEY', ROW(left_value::bpchar)), 0) \
          FROM cases ORDER BY name",
    )
    .fetch_all(&db.pool)
    .await
    .expect("compare PostgreSQL bpchar equality with full V3 identity bytes");
    assert_eq!(comparisons.len(), 11);
    for (postgres_equal, identity_equal, version) in comparisons {
        assert_eq!(version, 3);
        assert_eq!(
            identity_equal, postgres_equal,
            "complete encoded identity equality must match PostgreSQL bpchar equality"
        );
    }

    let v2_keeps_its_historical_bytes: bool = db
        .query_scalar(
            "SELECT pgtrickle.encode_row_id_v2('SCAN_KEY', ROW('x'::bpchar)) = \
                    pgtrickle.encode_row_id_v2('SCAN_KEY', ROW(('x' || chr(9))::bpchar))",
        )
        .await;
    assert!(v2_keeps_its_historical_bytes);
    let corrected_distinguishes_tab: bool = db
        .query_scalar(
            "SELECT pgtrickle.encode_row_id_v3('SCAN_KEY', ROW('x'::bpchar)) <> \
                    pgtrickle.encode_row_id_v3('SCAN_KEY', ROW(('x' || chr(9))::bpchar))",
        )
        .await;
    assert!(corrected_distinguishes_tab);
}
