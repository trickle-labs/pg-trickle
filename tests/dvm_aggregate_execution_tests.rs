//! Execution-backed tests for aggregate DVM SQL.
//!
//! These tests run generated aggregate delta SQL against a standalone
//! PostgreSQL container so we can validate result rows for representative
//! algebraic and rescan aggregate families.

#[path = "../verification/cdc_elision.rs"]
mod cdc_elision;
mod common;
#[path = "../verification/join_plan.rs"]
mod join_plan;
#[path = "../verification/aggregate_lifecycle.rs"]
mod lifecycle;
#[path = "../src/dvm/operators/nonfinite.rs"]
mod nonfinite;

use common::TestDb;
use pg_trickle::dvm::DiffContext;
use pg_trickle::dvm::parser::{AggExpr, AggFunc, Column, Expr, OpTree, SortExpr};
use pg_trickle::version::Frontier;

#[tokio::test]
async fn test_semantic_sql_controls_reject_predicate_count_payload_and_join_mutants() {
    use sha2::{Digest, Sha256};
    let digest = |bytes: &[u8]| {
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };

    struct Control {
        name: &'static str,
        phase: &'static str,
        source: (&'static str, &'static str),
        oracle: String,
        baseline: String,
        mutant: String,
    }
    async fn bag(db: &TestDb, sql: &str) -> Vec<serde_json::Value> {
        let mut rows: Vec<serde_json::Value> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT to_jsonb(r) FROM ({sql}) r"
        )))
        .fetch_all(&db.pool)
        .await
        .expect("SQL setup/execution errors cannot qualify a semantic control");
        rows.sort_by_cached_key(serde_json::Value::to_string);
        rows
    }
    let db = TestDb::new().await;
    db.execute("CREATE TYPE control_pair AS (a int,b int)")
        .await;
    let mut controls = Vec::new();

    let retraction = lifecycle::having_retraction_sql("p");
    assert_eq!(retraction.matches("IS NOT TRUE").count(), 1);
    let input = "WITH t(id,p) AS (VALUES(1,NULL::boolean))";
    controls.push(Control {
        name: "wrong-predicate-visibility",
        phase: "having_retraction",
        source: ("verification/aggregate_lifecycle.rs", include_str!("../verification/aggregate_lifecycle.rs")),
        oracle: format!("{input} SELECT id,'D'::text AS action FROM (SELECT id FROM t EXCEPT ALL SELECT id FROM t WHERE p) old_only"),
        baseline: format!("{input} SELECT id,'D'::text AS action FROM t WHERE {retraction}"),
        mutant: format!("{input} SELECT id,'D'::text AS action FROM t WHERE {}",retraction.replace("IS NOT TRUE","IS FALSE")),
    });

    let mut count = count_star("counted");
    count.function = AggFunc::Count;
    count.argument = Some(colref("payload"));
    let (inserts, deletes) =
        pg_trickle::dvm::operators::aggregate::agg_delta_exprs(&count, &["payload".into()]);
    let input = "WITH t(__pgt_action,payload) AS (VALUES \
        ('I',NULL::control_pair),('I',ROW(NULL,NULL)::control_pair), \
        ('I',ROW(1,NULL)::control_pair),('I',ROW(1,2)::control_pair), \
        ('D',ROW(NULL,NULL)::control_pair))";
    let generated = format!("{input} SELECT {inserts} AS inserted,{deletes} AS deleted FROM t");
    assert!(generated.contains("COUNT(CASE WHEN __pgt_action = 'I' THEN payload ELSE NULL END)"));
    assert!(generated.contains("COUNT(CASE WHEN __pgt_action = 'D' THEN payload ELSE NULL END)"));
    controls.push(Control {
        name:"wrong-nonnull-count", phase:"count_argument_nullability",
        source:("src/dvm/operators/aggregate.rs",include_str!("../src/dvm/operators/aggregate.rs")),
        oracle:format!("{input} SELECT pg_catalog.count(payload) FILTER(WHERE __pgt_action='I') AS inserted,pg_catalog.count(payload) FILTER(WHERE __pgt_action='D') AS deleted FROM t"),
        mutant:generated.replace(
            "THEN payload ELSE NULL END",
            "THEN CASE WHEN payload IS NOT NULL THEN 1 ELSE NULL END ELSE NULL END",
        ),
        baseline:generated,
    });

    let raw = "SELECT decode('01','hex') AS __pgt_row_id,'D'::text AS __pgt_action,7 AS id,'{\"value\":1}'::json AS payload \
        UNION ALL SELECT decode('01','hex'),'I',7,'{\"value\":2}'::json";
    let generated = cdc_elision::keyed_weight_sql(raw, "id,payload");
    assert_eq!(
        generated
            .matches("PARTITION BY __pgt_row_id,__pgt_image")
            .count(),
        1
    );
    let apply = |delta: &str| {
        format!(
            "SELECT old.id,CASE WHEN d.__pgt_action='I' THEN d.payload ELSE old.payload END AS payload \
        FROM (SELECT 7 AS id,'{{\"value\":1}}'::json AS payload) old LEFT JOIN {delta} d ON d.id=old.id"
        )
    };
    controls.push(Control {
        name: "lost-old-payload",
        phase: "apply_same_key_replacement",
        source: (
            "verification/cdc_elision.rs",
            include_str!("../verification/cdc_elision.rs"),
        ),
        oracle: "SELECT 7 AS id,'{\"value\":2}'::json AS payload".into(),
        baseline: apply(&generated),
        mutant: apply(&generated.replace(
            "PARTITION BY __pgt_row_id,__pgt_image",
            "PARTITION BY __pgt_row_id",
        )),
    });

    let insert = join_plan::Term {
        left_scan: 0,
        right_scan: 1,
        left_boundary: join_plan::Boundary::After,
        right_boundary: join_plan::Boundary::After,
        retract: false,
    };
    let delete = join_plan::Term {
        left_boundary: join_plan::Boundary::Before,
        right_boundary: join_plan::Boundary::Before,
        retract: true,
        ..insert
    };
    let generated = join_plan::endpoint_changes_sql(
        "old_endpoint",
        "new_endpoint",
        &["v".into()],
        insert,
        delete,
    )
    .unwrap();
    assert!(generated.contains("new_endpoint"));
    let input = "WITH old_endpoint AS(SELECT decode('01','hex') AS __pgt_row_id,10 AS v), \
        new_endpoint AS(SELECT decode('01','hex') AS __pgt_row_id,20 AS v)";
    controls.push(Control {
        name:"wrong-join-snapshot",phase:"endpoint_actions",
        source:("verification/join_plan.rs",include_str!("../verification/join_plan.rs")),
        oracle:format!("{input} SELECT __pgt_row_id,'D'::text AS __pgt_action,v FROM (SELECT * FROM old_endpoint EXCEPT ALL SELECT * FROM new_endpoint) d \
            UNION ALL SELECT __pgt_row_id,'I'::text,v FROM (SELECT * FROM new_endpoint EXCEPT ALL SELECT * FROM old_endpoint) i"),
        baseline:format!("{input} {generated}"),
        mutant:format!("{input} {}",generated.replace("new_endpoint","old_endpoint")),
    });

    assert_eq!(
        controls.len(),
        4,
        "the semantic control floor cannot shrink"
    );
    let mut observed = Vec::new();
    for control in controls {
        assert_ne!(
            control.baseline, control.mutant,
            "{}: mutation did not change executable SQL",
            control.name
        );
        let expected = bag(&db, &control.oracle).await;
        assert!(
            !expected.is_empty(),
            "{}: empty native expectation",
            control.name
        );
        let baseline = bag(&db, &control.baseline).await;
        assert_eq!(
            baseline, expected,
            "{}: correct generated baseline must pass",
            control.name
        );
        let mutant = bag(&db, &control.mutant).await;
        assert_ne!(
            mutant, expected,
            "{}: executable SQL mutant escaped its intended bag detector",
            control.name
        );
        observed.push(serde_json::json!({
            "case":control.name,"expected_failure_class":"MultisetMismatch","failure_class":"MultisetMismatch",
            "expected_phase":control.phase,"phase":control.phase,"baseline_passed":true,"mutant_executed":true,
            "source_path":control.source.0,"source_sha256":digest(control.source.1.as_bytes()),
            "oracle_sql_sha256":digest(control.oracle.as_bytes()),
            "baseline_sql_sha256":digest(control.baseline.as_bytes()),
            "mutant_sql_sha256":digest(control.mutant.as_bytes()),
            "expected":expected,"baseline":baseline,"mutant":mutant
        }));
    }
    let directory = std::path::Path::new(".p2p/work/issue-1218-semantic-controls/evidence");
    std::fs::create_dir_all(directory).unwrap();
    std::fs::write(directory.join("semantic-sql-controls.json"),serde_json::to_vec_pretty(&serde_json::json!({
        "schema":"pg-trickle-semantic-sql-controls-v1","selected":4,"observed":observed,
        "candidate_commit":std::env::var("PGT_RELEASE_CANDIDATE_COMMIT").ok(),
        "test_source_sha256":digest(include_bytes!("dvm_aggregate_execution_tests.rs")),
        "boundary":"Generated production-adapter SQL executes in stock PostgreSQL. Mutants alter that SQL; setup/execution errors fail the test rather than qualify a kill."
    })).unwrap()).unwrap();
}

#[tokio::test]
async fn test_nonfinite_aggregate_rescan_tracks_each_frontier() {
    let db = TestDb::new().await;
    for (sql_type, oid, average_type) in [
        ("numeric", 1700, "numeric"),
        ("real", 700, "double precision"),
        ("double precision", 701, "double precision"),
    ] {
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP TABLE IF EXISTS public.special_st,public.orders,pgtrickle_changes.changes_1; \
             CREATE SCHEMA IF NOT EXISTS pgtrickle_changes; \
             CREATE TABLE public.orders(id int,region text,amount {sql_type},label text); \
             CREATE TABLE pgtrickle_changes.changes_1(change_id bigserial,lsn pg_lsn,action text,__pgt_row_id bytea,id int,region text,amount {sql_type},label text); \
             CREATE TABLE public.special_st(__pgt_row_id bytea,region text,__pgt_count bigint,total {sql_type},mean {average_type}, \
                 __pgt_aux_nonnull_total bigint,__pgt_aux_sum_mean numeric,__pgt_aux_count_mean bigint); \
             INSERT INTO public.orders VALUES(2,'east',1,''); \
             INSERT INTO pgtrickle_changes.changes_1(lsn,action,__pgt_row_id,id,region,amount,label) VALUES('0/1','D',decode('01','hex'),1,'east','Infinity',''); \
             INSERT INTO public.special_st VALUES(decode('10','hex'),'east',2,'Infinity','Infinity',2,'Infinity',2);"
        ))).execute(&db.pool).await.unwrap();
        let mut scan = scan_orders();
        if let OpTree::Scan { columns, .. } = &mut scan {
            columns
                .iter_mut()
                .find(|column| column.name == "amount")
                .unwrap()
                .type_oid = oid;
        }
        let tree = OpTree::Aggregate {
            group_by: vec![colref("region")],
            aggregates: vec![sum_col("amount", "total"), avg_col("amount", "mean")],
            child: Box::new(scan),
        };
        let delta = make_aggregate_ctx("special_st", &["region", "total", "mean"])
            .differentiate(&tree)
            .unwrap();
        let (count,total,mean,aux): (i64,String,String,String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT __pgt_count,total::text,mean::text,__pgt_aux_sum_mean::text FROM ({delta}) d"
        ))).fetch_one(&db.pool).await.unwrap();
        assert_eq!(
            (count, total, mean.parse::<f64>().unwrap(), aux),
            (1, "1".into(), 1.0, "1".into()),
            "{sql_type}: deleted infinity must leave the finite source value"
        );
        db.execute(&format!("WITH delta AS ({delta}) UPDATE public.special_st s SET \
            __pgt_count=d.__pgt_count,total=d.total,mean=d.mean,__pgt_aux_sum_mean=d.__pgt_aux_sum_mean, \
            __pgt_aux_count_mean=d.__pgt_aux_count_mean,__pgt_aux_nonnull_total=d.__pgt_aux_nonnull_total \
            FROM delta d WHERE s.region=d.region")).await;
        db.execute("INSERT INTO public.orders VALUES(3,'east',3,'')")
            .await;
        db.execute(
            "INSERT INTO pgtrickle_changes.changes_1(lsn,action,__pgt_row_id,id,region,amount,label) \
             VALUES('0/20','I',decode('03','hex'),3,'east',3,'')",
        )
        .await;
        let mut previous = Frontier::new();
        previous.set_source(1, "0/10".into(), "2025-01-01T00:00:10Z".into());
        let mut next = Frontier::new();
        next.set_source(1, "0/30".into(), "2025-01-01T00:00:30Z".into());
        let mut ctx =
            DiffContext::new_standalone(previous, next).with_pgt_name("public", "special_st");
        ctx.st_user_columns = Some(vec!["region".into(), "total".into(), "mean".into()]);
        ctx.st_has_pgt_count = true;
        let delta = ctx.differentiate(&tree).unwrap();
        let (count, total, mean): (i64, String, String) = sqlx::query_as(sqlx::AssertSqlSafe(
            format!("SELECT __pgt_count,total::text,mean::numeric::text FROM ({delta}) d"),
        ))
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(count, 2);
        assert_eq!(total, "4");
        assert_eq!(
            mean.parse::<f64>().unwrap(),
            2.0,
            "{sql_type}: next cut must consume the later insert once"
        );
        if oid == 1700 {
            db.execute("CREATE DOMAIN special_region AS text NOT NULL")
                .await;
            db.execute("ALTER TABLE special_st ALTER COLUMN region TYPE special_region")
                .await;
            db.execute("TRUNCATE special_st,pgtrickle_changes.changes_1")
                .await;
            db.execute("INSERT INTO orders VALUES(4,'new','Infinity','')")
                .await;
            db.execute("INSERT INTO pgtrickle_changes.changes_1(lsn,action,__pgt_row_id,id,region,amount,label) VALUES('0/1','I',decode('04','hex'),4,'new','Infinity','')").await;
            let delta = make_aggregate_ctx("special_st", &["region", "total", "mean"])
                .differentiate(&tree)
                .unwrap();
            let total: String = db
                .query_scalar(&format!("SELECT total::text FROM ({delta}) d"))
                .await;
            assert_eq!(
                total, "Infinity",
                "new group must preserve a NOT NULL domain grouping key"
            );
        }
    }
    // Exercise the same initialization and signed-state helpers against native aggregates.
    let initial = nonfinite::state_sql("v");
    let merged = nonfinite::merge_sql("a.state", "i.state", "d.state");
    let value = nonfinite::value_sql("m.state");
    let guard = nonfinite::has_special_sql("a.state");
    let result: String = db
        .query_scalar(&format!(
            "WITH a AS(SELECT {initial} state FROM (VALUES('Infinity'::numeric),(1)) x(v)), \
        i AS(SELECT {initial} state FROM (SELECT NULL::numeric v WHERE false) x), \
        d AS(SELECT {initial} state FROM (VALUES('Infinity'::numeric)) x(v)), \
        m AS(SELECT {merged} state FROM a,i,d WHERE {guard}) SELECT {value} FROM m"
        ))
        .await;
    assert_eq!(result, "1");
    db.execute("CREATE DOMAIN special_number AS numeric").await;
    let wrapped = "CASE WHEN keep THEN v END";
    let state = nonfinite::state_sql(wrapped);
    let initial = nonfinite::state_sql("v");
    let observed: bool = db
        .query_scalar(&format!(
            "SELECT \
        bool_and(pg_typeof(v)::oid <> 1700 AND pg_typeof({wrapped})::oid=1700) \
        AND ({state}) = '{{\"finite\":1,\"nan\":0,\"pos\":1,\"neg\":0}}'::jsonb \
        AND ({initial})=({state}) \
        FROM (VALUES('Infinity'::special_number,true),(1::special_number,true)) x(v,keep)"
        ))
        .await;
    assert!(
        observed,
        "domain initialization and CASE-guarded deltas must admit the same effective base type"
    );
}

#[tokio::test]
async fn test_join_endpoint_sql_preserves_images_retractions_and_bag_duplicates() {
    let db = TestDb::new().await;
    assert!(join_plan::admitted_equality_type(23));
    assert!(!join_plan::admitted_equality_type(1700));
    let insert = join_plan::Term {
        left_scan: 0,
        right_scan: 1,
        left_boundary: join_plan::Boundary::After,
        right_boundary: join_plan::Boundary::After,
        retract: false,
    };
    let delete = join_plan::Term {
        left_boundary: join_plan::Boundary::Before,
        right_boundary: join_plan::Boundary::Before,
        retract: true,
        ..insert
    };
    let columns = vec!["n".to_string(), "j".to_string()];
    let sql =
        join_plan::endpoint_changes_sql("old_endpoint", "new_endpoint", &columns, insert, delete)
            .unwrap();
    let query = format!(
        "WITH old_endpoint AS (SELECT decode(id,'hex') AS __pgt_row_id,n,j FROM (VALUES \
           ('01',1.0::numeric,'{{\"x\":1}}'::json),('02',NULL,NULL),('04',4.0,'{{\"x\":4}}'), \
           ('05',2.0,'{{\"x\":5}}'),('06',NULL,NULL)) v(id,n,j)), \
         new_endpoint AS (SELECT decode(id,'hex') AS __pgt_row_id,n,j FROM (VALUES \
           ('01',1.00::numeric,'{{\"x\":1}}'::json),('03',NULL,'{{\"x\":3}}'), \
           ('04',4.0,'{{\"x\":4}}'),('05',2.0,'{{\"x\":55}}')) v(id,n,j)) \
         SELECT encode(__pgt_row_id,'hex'),__pgt_action,scale(n),j::text FROM ({sql}) changes ORDER BY 1,2"
    );
    let rows: Vec<(String, String, Option<i32>, Option<String>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(query.clone()))
            .fetch_all(&db.pool)
            .await
            .unwrap();
    let expected = [
        ("01", "D", Some(1), Some("{\"x\":1}")),
        ("01", "I", Some(2), Some("{\"x\":1}")),
        ("02", "D", None, None),
        ("03", "I", None, Some("{\"x\":3}")),
        ("05", "D", Some(1), Some("{\"x\":5}")),
        ("05", "I", Some(1), Some("{\"x\":55}")),
        ("06", "D", None, None),
    ]
    .into_iter()
    .map(|(id, action, scale, json)| {
        (
            id.to_string(),
            action.to_string(),
            scale,
            json.map(str::to_string),
        )
    })
    .collect::<Vec<_>>();
    assert_eq!(rows, expected, "endpoint bag/image actions differ");
    db.execute("CREATE SCHEMA endpoint_shadow").await;
    db.execute("CREATE FUNCTION endpoint_shadow.never_equal(bytea,bytea) RETURNS boolean LANGUAGE SQL IMMUTABLE STRICT AS 'SELECT false'").await;
    db.execute("CREATE OPERATOR endpoint_shadow.= (FUNCTION=endpoint_shadow.never_equal,LEFTARG=bytea,RIGHTARG=bytea)").await;
    let mut connection = db.pool.acquire().await.unwrap();
    sqlx::query("SET search_path=endpoint_shadow,pg_catalog,public")
        .execute(&mut *connection)
        .await
        .unwrap();
    let (shadowed, builtin_key): (bool, bool) =
        sqlx::query_as("SELECT decode('01','hex')=decode('01','hex'),1=1")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
    assert!(
        !shadowed && builtin_key,
        "control must shadow only internal bytea equality"
    );
    let shadow_rows: Vec<(String, String, Option<i32>, Option<String>)> =
        sqlx::query_as(sqlx::AssertSqlSafe(query))
            .fetch_all(&mut *connection)
            .await
            .unwrap();
    assert_eq!(
        shadow_rows, expected,
        "owner search_path changed complete identity matching"
    );
    let mixed = join_plan::Term {
        right_boundary: join_plan::Boundary::Before,
        ..insert
    };
    assert!(
        join_plan::endpoint_changes_sql("old_endpoint", "new_endpoint", &columns, mixed, delete)
            .is_none()
    );
}

#[tokio::test]
async fn test_cdc_keyed_weight_uses_complete_images_without_scalar_equality() {
    let db = TestDb::new().await;
    let delta = "SELECT decode('01','hex') AS __pgt_row_id, 'D'::text AS __pgt_action, 1 AS id, 1.0::numeric AS n, '{\"x\":1}'::json AS j \
                 UNION ALL SELECT decode('01','hex'),'I',1,1.00::numeric,'{\"x\":2}'::json";
    let rendered = cdc_elision::keyed_weight_sql(delta, "id,n,j");
    let (action, scale, json): (String, i32, String) = sqlx::query_as(sqlx::AssertSqlSafe(
        format!("SELECT __pgt_action,scale(n),j::text FROM {rendered} d"),
    ))
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        (action, scale, json),
        ("I".to_string(), 2, "{\"x\":2}".to_string())
    );
    let scale_delta = "SELECT decode('01','hex') AS __pgt_row_id, 'D'::text AS __pgt_action, 1 AS id, 1.0::numeric AS n \
                       UNION ALL SELECT decode('01','hex'),'I',1,1.00::numeric";
    let rendered = cdc_elision::keyed_weight_sql(scale_delta, "id,n");
    let scale: i32 = db
        .query_scalar(&format!("SELECT scale(n) FROM {rendered} d"))
        .await;
    assert_eq!(scale, 2, "SQL-equal images must not cancel a scale update");
    let transient = "SELECT decode('01','hex') AS __pgt_row_id, 'I'::text AS __pgt_action, 1 AS id, NULL::json AS j \
                     UNION ALL SELECT decode('01','hex'),'D',1,NULL::json";
    let rendered = cdc_elision::keyed_weight_sql(transient, "id,j");
    let count: i64 = db
        .query_scalar(&format!("SELECT count(*) FROM {rendered} d"))
        .await;
    assert_eq!(count, 0, "genuine same-image cancellation");
    let collision = "SELECT decode('01','hex') AS __pgt_row_id,'I'::text AS __pgt_action,7 AS __pgt_image,9 AS __pgt_weight";
    let rendered = cdc_elision::keyed_weight_sql(collision, "__pgt_image,__pgt_weight");
    let payload: (i32, i32) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_image,__pgt_weight FROM {rendered} d"
    )))
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(
        payload,
        (7, 9),
        "internal aliases must not shadow payload columns"
    );
    let image_name = format!("__pgt_image{}", "_".repeat(52));
    let weight_name = format!("__pgt_weight{}", "_".repeat(51));
    let columns = format!("__pgt_image,__pgt_weight,{image_name},{weight_name}");
    let collision = format!(
        "SELECT decode('01','hex') AS __pgt_row_id,'I'::text AS __pgt_action,\
         7 AS __pgt_image,9 AS __pgt_weight,11 AS {image_name},13 AS {weight_name}"
    );
    let rendered = cdc_elision::keyed_weight_sql(&collision, &columns);
    let payload: (i32, i32, i32, i32) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {columns} FROM {rendered} d"
    )))
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(payload, (7, 9, 11, 13), "63-byte payload names survive");
}

#[tokio::test]
async fn test_cdc_image_policy_preserves_native_scalar_observations() {
    let db = TestDb::new().await;
    db.execute("CREATE DOMAIN image_numeric AS numeric").await;
    let rendered = cdc_elision::changed_column_sql("a.v", "b.v");
    let mut observed = 0;
    for (data_type, values, admitted) in [
        ("boolean", "(NULL::boolean),(false),(true)", true),
        (
            "smallint",
            "(NULL::smallint),('-32768'::smallint),('32767'::smallint)",
            true,
        ),
        (
            "integer",
            "(NULL::int),(-2147483648),(-1),(0),(2147483647)",
            true,
        ),
        (
            "bigint",
            "(NULL::bigint),('-9223372036854775808'::bigint),('9223372036854775807'::bigint)",
            true,
        ),
        (
            "numeric",
            "(NULL::numeric),(1.0),(1.00),(-1),(0),('Infinity'::numeric),('-Infinity'::numeric),('NaN'::numeric)",
            false,
        ),
        ("text", "(NULL::text),('a'),('A'),('é')", false),
        (
            "varchar",
            "(NULL::varchar(3)),('a'::varchar(3)),('A'::varchar(3))",
            false,
        ),
        (
            "bpchar",
            "(NULL::character(2)),('a'::character(2)),('b'::character(2))",
            false,
        ),
        (
            "bytea",
            "(NULL::bytea),('\\x'::bytea),('\\x0000'::bytea)",
            false,
        ),
        (
            "uuid",
            "(NULL::uuid),('00000000-0000-0000-0000-000000000000'::uuid),('ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)",
            true,
        ),
        (
            "json",
            "(NULL::json),('{}'::json),('{\"v\":1}'::json)",
            false,
        ),
        (
            "image_numeric",
            "(NULL::image_numeric),(1.0::image_numeric),(1.00::image_numeric)",
            false,
        ),
    ] {
        let oid: i64 = db
            .query_scalar(&format!("SELECT '{data_type}'::regtype::oid::bigint"))
            .await;
        assert_eq!(
            cdc_elision::admitted_image_type(oid as u32),
            admitted,
            "{data_type}: actual catalog OID"
        );
        let observations: Vec<(String, bool)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT ({rendered})::text, a.v::text IS DISTINCT FROM b.v::text FROM (VALUES {values}) a(v) CROSS JOIN (VALUES {values}) b(v)"
        ))).fetch_all(&db.pool).await.unwrap();
        for (bit, different_observation) in observations {
            assert_eq!(
                bit,
                if !admitted || different_observation {
                    "1"
                } else {
                    "0"
                },
                "{data_type}: native text/NULL observation"
            );
            observed += 1;
        }
    }
    assert_eq!(observed, 186);
    let scale_control: (bool, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT a.v IS DISTINCT FROM b.v, ({rendered})::text FROM (VALUES(1.0::numeric)) a(v) CROSS JOIN (VALUES(1.00::numeric)) b(v)"
    ))).fetch_one(&db.pool).await.unwrap();
    assert_eq!(
        scale_control,
        (false, "1".to_string()),
        "value-equality control must lose the scale change"
    );
    db.execute("CREATE TABLE image_storage (id INT, v TEXT)")
        .await;
    db.execute("INSERT INTO image_storage VALUES (1,repeat('abc',10000))")
        .await;
    db.execute("ALTER TABLE image_storage ALTER COLUMN v SET STORAGE EXTERNAL")
        .await;
    db.execute("INSERT INTO image_storage VALUES (2,repeat('abc',10000))")
        .await;
    let (same_wire, different_storage, kept): (bool,bool,String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT record_send(ROW(a.v)) = record_send(ROW(b.v)), pg_column_size(a.v) <> pg_column_size(b.v), ({rendered})::text FROM image_storage a CROSS JOIN image_storage b WHERE a.id=1 AND b.id=2"
    ))).fetch_one(&db.pool).await.unwrap();
    assert!(
        same_wire && different_storage,
        "wire equality must not hide storage observations"
    );
    assert_eq!(
        kept, "1",
        "variable-length values require a stronger observation contract before elision"
    );
}

#[tokio::test]
async fn test_semantic_ternary_partition_and_plan_variation_preserve_bags() {
    let db = TestDb::new().await;
    db.execute("CREATE TABLE semantic_input (k INT, v NUMERIC)")
        .await;
    db.execute("INSERT INTO semantic_input VALUES (1,NULL),(1,-1),(1,0),(1,1),(1,1),(2,NULL)")
        .await;
    // These transformations admit only deterministic, total expressions and
    // compare bags, without relying on output order or floating point algebra.
    let partitioned = "SELECT k,v FROM semantic_input WHERE (v>0) IS TRUE UNION ALL SELECT k,v FROM semantic_input WHERE (v>0) IS FALSE UNION ALL SELECT k,v FROM semantic_input WHERE (v>0) IS UNKNOWN";
    let same: bool = db.query_scalar(&format!("SELECT NOT EXISTS (((SELECT k,v FROM semantic_input) EXCEPT ALL ({partitioned})) UNION ALL (({partitioned}) EXCEPT ALL (SELECT k,v FROM semantic_input)))")).await;
    assert!(
        same,
        "ternary partition must preserve duplicate and NULL rows"
    );
    let wrong: bool = db.query_scalar("SELECT EXISTS ((SELECT k,v FROM semantic_input) EXCEPT ALL (SELECT k,v FROM semantic_input WHERE v>0 UNION ALL SELECT k,v FROM semantic_input WHERE NOT(v>0)))").await;
    assert!(
        wrong,
        "omitting UNKNOWN must be detected by the independent bag oracle"
    );
    let query = "SELECT k,COUNT(*) AS rows,COUNT(v) AS arguments,SUM(v) AS total FROM semantic_input GROUP BY k";
    let mut connection = db.pool.acquire().await.unwrap();
    sqlx::raw_sql("SET enable_hashagg=on; SET enable_sort=off")
        .execute(&mut *connection)
        .await
        .unwrap();
    let hash_plan: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (FORMAT JSON) {query}"
    )))
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    let hash_rows: Vec<serde_json::Value> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT to_jsonb(r) FROM ({query}) r ORDER BY k"
    )))
    .fetch_all(&mut *connection)
    .await
    .unwrap();
    sqlx::raw_sql("SET enable_hashagg=off; SET enable_sort=on")
        .execute(&mut *connection)
        .await
        .unwrap();
    let sorted_plan: serde_json::Value = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "EXPLAIN (FORMAT JSON) {query}"
    )))
    .fetch_one(&mut *connection)
    .await
    .unwrap();
    let sorted_rows: Vec<serde_json::Value> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT to_jsonb(r) FROM ({query}) r ORDER BY k"
    )))
    .fetch_all(&mut *connection)
    .await
    .unwrap();
    assert_eq!(hash_plan[0]["Plan"]["Strategy"], "Hashed");
    assert_eq!(sorted_plan[0]["Plan"]["Strategy"], "Sorted");
    assert_eq!(hash_rows, sorted_rows);
}

#[test]
fn test_lifecycle_having_sql_uses_total_truth_tests_and_identity_guards() {
    let tree = OpTree::Filter {
        predicate: binop(">", colref("total_amount"), lit("0")),
        child: Box::new(grouped_aggregate(sum_col("amount", "total_amount"))),
    };
    let sql = make_aggregate_ctx("agg_sum_st", &["region", "total_amount"])
        .differentiate(&tree)
        .unwrap();
    assert!(sql.contains("IS NOT TRUE"), "{sql}");
    assert!(sql.contains("IS TRUE"), "{sql}");
    assert!(!sql.contains("WHERE NOT ("), "{sql}");
    assert!(sql.contains("AND __pgt_action = 'I'"), "{sql}");
    assert!(sql.contains("st.__pgt_row_id ="), "{sql}");
}

#[tokio::test]
async fn test_lifecycle_visibility_decisions_match_postgresql_bag_differences() {
    let db = TestDb::new().await;
    type Observation = (bool, Option<bool>, bool, Option<bool>, bool, bool, bool);
    let rows: Vec<Observation> = sqlx::query_as(
        "WITH truth(p) AS (VALUES (TRUE), (FALSE), (NULL::bool)), \
         present(e) AS (VALUES (TRUE), (FALSE)), changed(c) AS (VALUES (TRUE), (FALSE)) \
         SELECT a.e, p.p, b.e, q.p, c.c, \
           EXISTS((SELECT 7 WHERE a.e AND p.p) EXCEPT ALL \
                  (SELECT CASE WHEN c.c THEN 8 ELSE 7 END WHERE b.e AND q.p)), \
           EXISTS((SELECT CASE WHEN c.c THEN 8 ELSE 7 END WHERE b.e AND q.p) EXCEPT ALL \
                  (SELECT 7 WHERE a.e AND p.p)) \
         FROM present a CROSS JOIN truth p CROSS JOIN present b CROSS JOIN truth q CROSS JOIN changed c"
    ).fetch_all(&db.pool).await.unwrap();
    assert_eq!(rows.len(), 72);
    for (old_exists, old_truth, new_exists, new_truth, changed, retract, insert) in rows {
        let actions =
            lifecycle::visibility_actions(old_exists, old_truth, new_exists, new_truth, changed);
        assert_eq!(
            (actions.retract, actions.insert),
            (retract, insert),
            "old=({old_exists}, {old_truth:?}), new=({new_exists}, {new_truth:?}), changed={changed}"
        );
    }
    let rendered = lifecycle::having_retraction_sql("p");
    let admitted = lifecycle::having_admission_sql("p");
    let observations: Vec<(Option<bool>, bool, bool, bool)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT p, ({rendered}), (NOT p) IS TRUE, ({admitted}) FROM (VALUES (TRUE), (FALSE), (NULL::bool)) v(p)"
    ))).fetch_all(&db.pool).await.unwrap();
    assert_eq!(
        observations,
        vec![
            (Some(true), false, false, true),
            (Some(false), true, true, false),
            (None, true, false, false)
        ]
    );
}

#[tokio::test]
async fn test_lifecycle_group_and_null_plans_match_postgresql_small_bags() {
    let db = TestDb::new().await;
    let values = ["NULL::int", "-1", "0", "1"];
    let mut bags = vec!["SELECT NULL::int AS v WHERE FALSE".to_string()];
    for (index, value) in values.iter().enumerate() {
        bags.push(format!("VALUES ({value})"));
        for other in &values[index..] {
            bags.push(format!("VALUES ({value}), ({other})"));
        }
    }
    assert_eq!(bags.len(), 15);
    let mut observed = 0;
    for global in [false, true] {
        for bag in &bags {
            let direct = if global {
                "SELECT COUNT(*), COUNT(v), SUM(v) FROM data"
            } else {
                "SELECT COUNT(*), COUNT(v), SUM(v) FROM (SELECT 1 AS g, v FROM data) d GROUP BY g"
            };
            let expected: Vec<(i64, i64, Option<i64>)> = sqlx::query_as(sqlx::AssertSqlSafe(
                format!("WITH data(v) AS ({bag}) {direct}"),
            ))
            .fetch_all(&db.pool)
            .await
            .unwrap();
            let presence = lifecycle::group_presence_sql(global, "rows");
            let total = lifecycle::aggregate_value_sql(true, "nonnull", "COALESCE(total, 99)");
            let count = lifecycle::aggregate_value_sql(false, "nonnull", "rows");
            let actual: Vec<(i64, i64, Option<i64>)> =
                sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "WITH data(v) AS ({bag}), state AS \
                 (SELECT COUNT(*) AS rows, COUNT(v) AS nonnull, SUM(v) AS total FROM data) \
                 SELECT {count}, nonnull, {total} FROM state WHERE {presence}"
                )))
                .fetch_all(&db.pool)
                .await
                .unwrap();
            assert_eq!(actual, expected, "global={global}, bag={bag}");
            observed += 1;
        }
    }
    assert_eq!(observed, 30);
    let expected = [[["N", "I"], ["D", "U"]], [["I", "I"], ["U", "U"]]];
    for global in [false, true] {
        for old in [false, true] {
            for nonempty in [false, true] {
                let expression = lifecycle::group_action_sql(
                    global,
                    if old { "TRUE" } else { "FALSE" },
                    if nonempty { "1" } else { "0" },
                );
                let actual: String =
                    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT {expression}")))
                        .fetch_one(&db.pool)
                        .await
                        .unwrap();
                assert_eq!(
                    actual,
                    expected[global as usize][old as usize][nonempty as usize]
                );
            }
        }
    }
}

fn int_col(name: &str) -> Column {
    Column {
        name: name.to_string(),
        type_oid: 23,
        is_nullable: true,
    }
}

fn text_col(name: &str) -> Column {
    Column {
        name: name.to_string(),
        type_oid: 25,
        is_nullable: true,
    }
}

fn colref(name: &str) -> Expr {
    Expr::ColumnRef {
        table_alias: None,
        column_name: name.to_string(),
    }
}

fn sort_asc(name: &str) -> SortExpr {
    SortExpr {
        expr: colref(name),
        ascending: true,
        nulls_first: false,
    }
}

fn lit(value: &str) -> Expr {
    Expr::Literal(value.to_string())
}

fn binop(op: &str, left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: op.to_string(),
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn scan_orders() -> OpTree {
    OpTree::Scan {
        table_oid: 1,
        table_name: "orders".to_string(),
        schema: "public".to_string(),
        columns: vec![
            int_col("id"),
            text_col("region"),
            int_col("amount"),
            text_col("label"),
        ],
        pk_columns: vec!["id".to_string()],
        alias: "o".to_string(),
    }
}

fn count_star(alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::CountStar,
        argument: None,
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
        statistical_support: None,
    }
}

fn sum_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Sum,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
        statistical_support: None,
    }
}

fn avg_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Avg,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
        statistical_support: None,
    }
}

fn min_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Min,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
        statistical_support: None,
    }
}

fn max_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Max,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
        statistical_support: None,
    }
}

fn string_agg_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::StringAgg,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: Some(lit("', '")),
        filter: None,
        order_within_group: Some(vec![sort_asc("amount")]),
        statistical_support: None,
    }
}

fn mode_col(column: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Mode,
        argument: None,
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: Some(vec![sort_asc(column)]),
        statistical_support: None,
    }
}

fn json_object_agg_col(key_col: &str, val_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::JsonObjectAgg,
        argument: Some(colref(key_col)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: Some(colref(val_col)),
        filter: None,
        order_within_group: None,
        statistical_support: None,
    }
}

fn jsonb_object_agg_col(key_col: &str, val_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::JsonbObjectAgg,
        argument: Some(colref(key_col)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: Some(colref(val_col)),
        filter: None,
        order_within_group: None,
        statistical_support: None,
    }
}

fn percentile_cont_col(fraction: &str, order_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::PercentileCont,
        argument: Some(lit(fraction)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: Some(vec![sort_asc(order_col)]),
        statistical_support: None,
    }
}

fn percentile_disc_col(fraction: &str, order_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::PercentileDisc,
        argument: Some(lit(fraction)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: Some(vec![sort_asc(order_col)]),
        statistical_support: None,
    }
}

fn filtered_count_col(column: &str, alias: &str, filter: Expr) -> AggExpr {
    AggExpr {
        function: AggFunc::Count,
        argument: Some(colref(column)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: Some(filter),
        order_within_group: None,
        statistical_support: None,
    }
}

fn grouped_aggregate(aggregate: AggExpr) -> OpTree {
    OpTree::Aggregate {
        group_by: vec![colref("region")],
        aggregates: vec![aggregate],
        child: Box::new(scan_orders()),
    }
}

fn build_multi_group_mixed_family_tree() -> OpTree {
    OpTree::Aggregate {
        group_by: vec![colref("region"), colref("label")],
        aggregates: vec![
            count_star("cnt"),
            sum_col("amount", "total_amt"),
            max_col("amount", "max_amt"),
        ],
        child: Box::new(scan_orders()),
    }
}

async fn query_multi_agg_rows(
    db: &TestDb,
    sql: &str,
) -> Vec<(String, String, String, i64, i64, i32)> {
    sqlx::query_as::<_, (String, String, String, i64, i64, i32)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, label, cnt, total_amt, max_amt \
         FROM ({sql}) delta ORDER BY __pgt_action DESC, region, label"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute multi-group aggregate delta SQL")
}

fn make_aggregate_ctx(st_name: &str, st_user_columns: &[&str]) -> DiffContext {
    let mut prev_frontier = Frontier::new();
    prev_frontier.set_source(1, "0/0".to_string(), "2025-01-01T00:00:00Z".to_string());

    let mut frontier = Frontier::new();
    frontier.set_source(1, "0/10".to_string(), "2025-01-01T00:00:10Z".to_string());

    let mut ctx = DiffContext::new_standalone(prev_frontier, frontier)
        .with_pgt_name("public", st_name)
        .with_defining_query("SELECT region, amount, label FROM public.orders");
    ctx.st_user_columns = Some(st_user_columns.iter().map(|c| (*c).to_string()).collect());
    ctx.st_has_pgt_count = true;
    ctx
}

async fn setup_aggregate_db() -> TestDb {
    let db = TestDb::new().await;

    sqlx::raw_sql(
        r#"
CREATE SCHEMA IF NOT EXISTS pgtrickle;
CREATE SCHEMA IF NOT EXISTS pgtrickle_changes;

CREATE OR REPLACE FUNCTION pgtrickle.pg_trickle_hash(val TEXT)
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT hashtextextended(COALESCE(val, ''), 0)::BIGINT
$$;

CREATE OR REPLACE FUNCTION pgtrickle.pg_trickle_hash_multi(vals TEXT[])
RETURNS BIGINT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT hashtextextended(COALESCE(array_to_string(vals, '|', '<NULL>'), ''), 0)::BIGINT
$$;

CREATE TABLE public.orders (
    id INT PRIMARY KEY,
    region TEXT NOT NULL,
    amount INT NOT NULL,
    label TEXT NOT NULL DEFAULT ''
);

CREATE TABLE public.agg_count_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    order_count BIGINT NOT NULL
);

CREATE TABLE public.agg_sum_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    total_amount BIGINT NOT NULL,
    __pgt_aux_nonnull_total_amount BIGINT NOT NULL
);

CREATE TABLE public.agg_avg_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    avg_amount NUMERIC NOT NULL,
    __pgt_aux_sum_avg_amount NUMERIC NOT NULL,
    __pgt_aux_count_avg_amount BIGINT NOT NULL
);

CREATE TABLE public.agg_min_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    min_amount INT NOT NULL
);

CREATE TABLE public.agg_max_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    max_amount INT NOT NULL
);

CREATE TABLE public.agg_string_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    member_labels TEXT NOT NULL
);

CREATE TABLE public.agg_mode_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    mode_amount INT NOT NULL
);

CREATE TABLE public.agg_json_object_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    amount_map JSON NOT NULL
);

CREATE TABLE public.agg_jsonb_object_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    amount_map JSONB NOT NULL
);

CREATE TABLE public.agg_percentile_cont_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    median_amount NUMERIC NOT NULL
);

CREATE TABLE public.agg_percentile_disc_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    median_disc_amount INT NOT NULL
);

CREATE TABLE public.agg_filtered_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    high_value_count BIGINT NOT NULL
);

CREATE TABLE public.agg_jsonb_arr_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    amount_arr JSONB NOT NULL
);

CREATE TABLE public.agg_multi_st (
    __pgt_row_id BYTEA PRIMARY KEY,
    region TEXT NOT NULL,
    label TEXT NOT NULL,
    __pgt_count BIGINT NOT NULL,
    cnt BIGINT NOT NULL,
    total_amt BIGINT NOT NULL,
    max_amt INT NOT NULL,
    __pgt_aux_nonnull_total_amt BIGINT NOT NULL
);

CREATE TABLE pgtrickle_changes.changes_1 (
    change_id BIGSERIAL PRIMARY KEY,
    lsn PG_LSN NOT NULL,
    action CHAR(1) NOT NULL,
    __pgt_row_id BYTEA NOT NULL,
    id INT,
    region TEXT,
    amount INT,
    label TEXT
);
"#,
    )
    .execute(&db.pool)
    .await
    .expect("failed to set up aggregate execution database");

    db
}

async fn reset_aggregate_fixture(db: &TestDb) {
    db.execute(
        "TRUNCATE TABLE \
         pgtrickle_changes.changes_1, \
         public.orders, \
         public.agg_count_st, \
         public.agg_sum_st, \
         public.agg_avg_st, \
         public.agg_min_st, \
         public.agg_max_st, \
         public.agg_string_st, \
         public.agg_mode_st, \
         public.agg_json_object_st, \
         public.agg_jsonb_object_st, \
         public.agg_percentile_cont_st, \
         public.agg_percentile_disc_st, \
         public.agg_filtered_st, \
         public.agg_jsonb_arr_st, \
         public.agg_multi_st \
         RESTART IDENTITY",
    )
    .await;
}

async fn query_bigint_aggregate_rows(
    db: &TestDb,
    sql: &str,
    aggregate_column: &str,
) -> Vec<(String, String, i64, i64)> {
    sqlx::query_as::<_, (String, String, i64, i64)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, __pgt_count, {aggregate_column} \
         FROM ({sql}) delta ORDER BY __pgt_action, region"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated aggregate delta SQL")
}

async fn query_numeric_aggregate_rows(
    db: &TestDb,
    sql: &str,
    aggregate_column: &str,
) -> Vec<(String, String, i64, String)> {
    sqlx::query_as::<_, (String, String, i64, String)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, __pgt_count, ({aggregate_column})::numeric(10,2)::text \
         FROM ({sql}) delta ORDER BY __pgt_action, region"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated aggregate delta SQL")
}

async fn query_text_aggregate_rows(
    db: &TestDb,
    sql: &str,
    aggregate_column: &str,
) -> Vec<(String, String, i64, String)> {
    sqlx::query_as::<_, (String, String, i64, String)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, __pgt_count, ({aggregate_column})::text \
         FROM ({sql}) delta ORDER BY __pgt_action, region"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated aggregate delta SQL")
}

async fn query_json_aggregate_rows(
    db: &TestDb,
    sql: &str,
    aggregate_column: &str,
) -> Vec<(String, String, i64, String)> {
    sqlx::query_as::<_, (String, String, i64, String)>(sqlx::AssertSqlSafe(format!(
        "SELECT __pgt_action, region, __pgt_count, (({aggregate_column})::jsonb)::text \
         FROM ({sql}) delta ORDER BY __pgt_action, region"
    )))
    .fetch_all(&db.pool)
    .await
    .expect("failed to execute generated aggregate delta SQL")
}

#[tokio::test]
async fn test_diff_aggregate_executes_count_star_gain_and_loss() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_count_st", &["region", "order_count"])
        .differentiate(&grouped_aggregate(count_star("order_count")))
        .expect("count aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (2, 'east', 20), \
         (4, 'east', 40)",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_count_st VALUES \
         (pgtrickle.test_int_to_row_id(100), 'east', 2, 2), \
         (pgtrickle.test_int_to_row_id(200), 'west', 1, 1)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(4), 4, 'east', 40)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/2', 'D', pgtrickle.test_int_to_row_id(3), 3, 'west', 30)",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "order_count").await,
        vec![
            ("D".to_string(), "west".to_string(), 1, 1),
            ("I".to_string(), "east".to_string(), 3, 3),
        ]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_sum_update_with_balanced_delta() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_sum_st", &["region", "total_amount"])
        .differentiate(&grouped_aggregate(sum_col("amount", "total_amount")))
        .expect("sum aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (3, 'east', 15)",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_sum_st VALUES (pgtrickle.test_int_to_row_id(100), 'east', 2, 30, 2)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(3), 3, 'east', 15)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/2', 'D', pgtrickle.test_int_to_row_id(2), 2, 'east', 20)",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "total_amount").await,
        vec![("I".to_string(), "east".to_string(), 2, 25)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_avg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_avg_st", &["region", "avg_amount"])
        .differentiate(&grouped_aggregate(avg_col("amount", "avg_amount")))
        .expect("avg aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10), \
         (2, 'east', 20), \
         (3, 'east', 30)",
    )
    .await;
    db.execute("INSERT INTO public.agg_avg_st VALUES (pgtrickle.test_int_to_row_id(100), 'east', 2, 15.00, 30.00, 2)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(3), 3, 'east', 30)",
    )
    .await;

    assert_eq!(
        query_numeric_aggregate_rows(&db, &sql, "avg_amount").await,
        vec![("I".to_string(), "east".to_string(), 3, "20.00".to_string())]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_filtered_count_update() {
    let db = setup_aggregate_db().await;
    let filter = binop(">=", colref("amount"), lit("20"));
    let sql = make_aggregate_ctx("agg_filtered_st", &["region", "high_value_count"])
        .differentiate(&grouped_aggregate(filtered_count_col(
            "amount",
            "high_value_count",
            filter,
        )))
        .expect("filtered aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 25), \
         (2, 'east', 20)",
    )
    .await;
    db.execute("INSERT INTO public.agg_filtered_st VALUES (pgtrickle.test_int_to_row_id(100), 'east', 2, 1)")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(1), 1, 'east', 10), \
                ('0/2', 'I', pgtrickle.test_int_to_row_id(1), 1, 'east', 25)",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "high_value_count").await,
        vec![("I".to_string(), "east".to_string(), 2, 2)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_min_rescan_on_deleted_extremum() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_min_st", &["region", "min_amount"])
        .differentiate(&grouped_aggregate(min_col("amount", "min_amount")))
        .expect("min aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (2, 'east', 20, 'b'), \
         (3, 'east', 30, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_min_st VALUES (pgtrickle.test_int_to_row_id(100), 'east', 3, 10)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(1), 1, 'east', 10, 'a')",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "(min_amount)::bigint").await,
        vec![("I".to_string(), "east".to_string(), 2, 20)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_max_rescan_on_deleted_extremum() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_max_st", &["region", "max_amount"])
        .differentiate(&grouped_aggregate(max_col("amount", "max_amount")))
        .expect("max aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_max_st VALUES (pgtrickle.test_int_to_row_id(100), 'east', 3, 30)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) \
         VALUES ('0/1', 'D', pgtrickle.test_int_to_row_id(3), 3, 'east', 30, 'c')",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "(max_amount)::bigint").await,
        vec![("I".to_string(), "east".to_string(), 2, 20)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_string_agg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_string_st", &["region", "member_labels"])
        .differentiate(&grouped_aggregate(string_agg_col("label", "member_labels")))
        .expect("string_agg differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 15, 'b'), \
         (3, 'east', 20, 'c')",
    )
    .await;
    db.execute("INSERT INTO public.agg_string_st VALUES (pgtrickle.test_int_to_row_id(100), 'east', 2, 'a, c')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(2), 2, 'east', 15, 'b')",
    )
    .await;

    assert_eq!(
        query_text_aggregate_rows(&db, &sql, "member_labels").await,
        vec![(
            "I".to_string(),
            "east".to_string(),
            3,
            "a, b, c".to_string()
        )]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_mode_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_mode_st", &["region", "mode_amount"])
        .differentiate(&grouped_aggregate(mode_col("amount", "mode_amount")))
        .expect("mode differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 10, 'b'), \
         (3, 'east', 20, 'c'), \
         (4, 'east', 20, 'd'), \
         (5, 'east', 20, 'e')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_mode_st VALUES (pgtrickle.test_int_to_row_id(100), 'east', 3, 10)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) \
         VALUES \
         ('0/1', 'I', pgtrickle.test_int_to_row_id(4), 4, 'east', 20, 'd'), \
         ('0/2', 'I', pgtrickle.test_int_to_row_id(5), 5, 'east', 20, 'e')",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "(mode_amount)::bigint").await,
        vec![("I".to_string(), "east".to_string(), 5, 20)]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_json_object_agg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_json_object_st", &["region", "amount_map"])
        .differentiate(&grouped_aggregate(json_object_agg_col(
            "label",
            "amount",
            "amount_map",
        )))
        .expect("json_object_agg differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 30, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_json_object_st VALUES \
         (pgtrickle.test_int_to_row_id(100), 'east', 2, '{\"a\":10,\"c\":30}')",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(2), 2, 'east', 20, 'b')",
    )
    .await;

    assert_eq!(
        query_json_aggregate_rows(&db, &sql, "amount_map").await,
        vec![(
            "I".to_string(),
            "east".to_string(),
            3,
            "{\"a\": 10, \"b\": 20, \"c\": 30}".to_string(),
        )]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_jsonb_object_agg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_jsonb_object_st", &["region", "amount_map"])
        .differentiate(&grouped_aggregate(jsonb_object_agg_col(
            "label",
            "amount",
            "amount_map",
        )))
        .expect("jsonb_object_agg differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 30, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_jsonb_object_st VALUES \
         (pgtrickle.test_int_to_row_id(100), 'east', 2, '{\"a\":10,\"c\":30}')",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(2), 2, 'east', 20, 'b')",
    )
    .await;

    assert_eq!(
        query_json_aggregate_rows(&db, &sql, "amount_map").await,
        vec![(
            "I".to_string(),
            "east".to_string(),
            3,
            "{\"a\": 10, \"b\": 20, \"c\": 30}".to_string(),
        )]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_percentile_cont_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_percentile_cont_st", &["region", "median_amount"])
        .differentiate(&grouped_aggregate(percentile_cont_col(
            "0.5",
            "amount",
            "median_amount",
        )))
        .expect("percentile_cont differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 40, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_percentile_cont_st VALUES \
         (pgtrickle.test_int_to_row_id(100), 'east', 2, 25.00)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(2), 2, 'east', 20, 'b')",
    )
    .await;

    assert_eq!(
        query_numeric_aggregate_rows(&db, &sql, "median_amount").await,
        vec![("I".to_string(), "east".to_string(), 3, "20.00".to_string())]
    );
}

#[tokio::test]
async fn test_diff_aggregate_executes_percentile_disc_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_percentile_disc_st", &["region", "median_disc_amount"])
        .differentiate(&grouped_aggregate(percentile_disc_col(
            "0.5",
            "amount",
            "median_disc_amount",
        )))
        .expect("percentile_disc differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 40, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_percentile_disc_st VALUES \
         (pgtrickle.test_int_to_row_id(100), 'east', 2, 10)",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(2), 2, 'east', 20, 'b')",
    )
    .await;

    assert_eq!(
        query_bigint_aggregate_rows(&db, &sql, "(median_disc_amount)::bigint").await,
        vec![("I".to_string(), "east".to_string(), 3, 20)]
    );
}

fn jsonb_agg_col(argument: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::JsonbAgg,
        argument: Some(colref(argument)),
        alias: alias.to_string(),
        is_distinct: false,
        second_arg: None,
        filter: None,
        order_within_group: None,
        statistical_support: None,
    }
}

#[tokio::test]
async fn test_diff_aggregate_executes_jsonb_agg_rescan_update() {
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx("agg_jsonb_arr_st", &["region", "amount_arr"])
        .differentiate(&grouped_aggregate(jsonb_agg_col("amount", "amount_arr")))
        .expect("jsonb_agg differentiation should succeed");

    reset_aggregate_fixture(&db).await;
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'a'), \
         (2, 'east', 20, 'b'), \
         (3, 'east', 30, 'c')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_jsonb_arr_st VALUES \
         (pgtrickle.test_int_to_row_id(100), 'east', 2, '[10, 30]')",
    )
    .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) \
         VALUES ('0/1', 'I', pgtrickle.test_int_to_row_id(2), 2, 'east', 20, 'b')",
    )
    .await;

    let rows = query_json_aggregate_rows(&db, &sql, "amount_arr").await;
    assert_eq!(rows.len(), 1);
    let (action, region, count, json_str) = &rows[0];
    assert_eq!(action, "I");
    assert_eq!(region, "east");
    assert_eq!(*count, 3);
    assert!(json_str.contains("10"));
    assert!(json_str.contains("20"));
    assert!(json_str.contains("30"));
}

#[tokio::test]
async fn test_diff_aggregate_executes_multi_group_mixed_family() {
    // Tests that simultaneous changes to two different groups produce the
    // correct delta rows for each group, and that a mixed aggregate family
    // (COUNT + SUM + MAX) is computed correctly in a single rescan pass.
    let db = setup_aggregate_db().await;
    let sql = make_aggregate_ctx(
        "agg_multi_st",
        &["region", "label", "cnt", "total_amt", "max_amt"],
    )
    .differentiate(&build_multi_group_mixed_family_tree())
    .expect("multi-group mixed-family aggregate differentiation should succeed");

    reset_aggregate_fixture(&db).await;

    // Seed base state: three groups — east-A, east-B, west-A.
    db.execute(
        "INSERT INTO public.orders VALUES \
         (1, 'east', 10, 'A'), \
         (2, 'east', 20, 'B'), \
         (3, 'west', 15, 'A')",
    )
    .await;
    db.execute(
        "INSERT INTO public.agg_multi_st \
         (__pgt_row_id, region, label, __pgt_count, cnt, total_amt, max_amt, __pgt_aux_nonnull_total_amt) VALUES \
         (pgtrickle.test_int_to_row_id(1), 'east', 'A', 1, 1, 10, 10, 1), \
         (pgtrickle.test_int_to_row_id(2), 'east', 'B', 1, 1, 20, 20, 1), \
         (pgtrickle.test_int_to_row_id(3), 'west', 'A', 1, 1, 15, 15, 1)",
    )
    .await;

    // Delta: two groups change simultaneously — east-A and west-A each gain a row.
    db.execute("INSERT INTO public.orders VALUES (4, 'east', 40, 'A'), (5, 'west', 40, 'A')")
        .await;
    db.execute(
        "INSERT INTO pgtrickle_changes.changes_1 \
         (lsn, action, __pgt_row_id, id, region, amount, label) VALUES \
         ('0/1', 'I', pgtrickle.test_int_to_row_id(4), 4, 'east', 40, 'A'), \
         ('0/2', 'I', pgtrickle.test_int_to_row_id(5), 5, 'west', 40, 'A')",
    )
    .await;

    let rows = query_multi_agg_rows(&db, &sql).await;

    // Two groups changed → two upsert rows (I for each changed group).
    assert_eq!(
        rows.len(),
        2,
        "expected 2 delta rows, one per changed group"
    );

    // east-A: count 1→2, sum 10+40=50, max(10,40)=40
    assert_eq!(
        rows[0],
        (
            "I".to_string(),
            "east".to_string(),
            "A".to_string(),
            2,
            50,
            40
        )
    );
    // west-A: count 1→2, sum 15+40=55, max(15,40)=40
    assert_eq!(
        rows[1],
        (
            "I".to_string(),
            "west".to_string(),
            "A".to_string(),
            2,
            55,
            40
        )
    );
}
