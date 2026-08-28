//! PostgreSQL-backed checks for the v0.87.15 row-identity V2 API.

mod e2e;

use e2e::E2eDb;

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
