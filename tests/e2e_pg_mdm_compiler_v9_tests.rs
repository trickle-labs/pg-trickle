//! Executable conformance for pg-mdm compiler-v9 graph SQL.

mod e2e;
#[allow(dead_code)]
#[path = "conformance/reference_clients.rs"]
mod reference_clients;

use e2e::E2eDb;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const ARTIFACT: &str = include_str!("fixtures/pg_mdm_compiler_v9_person.json");

const SYNC_SOURCE_RECORDS: &str = "INSERT INTO mdm_graph.source_records
        (source_record_key, source_record_id, source_identity_id, active)
     SELECT source_record_key, md5(encode(source_record_key, 'hex'))::uuid,
            source_identity_id, true
       FROM mdm_graph.fixture_source_keys
     ON CONFLICT (source_record_key) DO UPDATE SET active = true;
     UPDATE mdm_graph.source_records sr
        SET active = EXISTS (
            SELECT 1 FROM mdm_graph.fixture_source_keys k
             WHERE k.source_record_key = sr.source_record_key
        )";

#[derive(Deserialize)]
struct Artifact {
    compiler_version: i32,
    nodes: Vec<Node>,
    roots: Vec<String>,
}

#[derive(Deserialize)]
struct Node {
    logical_id: String,
    dependencies: Vec<String>,
    defining_sql: String,
}

const EXPECTED_LOGICAL_IDS: &[&str] = &[
    "records/crm",
    "records/erp",
    "normalized/name",
    "normalized/email",
    "normalized/birth_date",
    "blocks/composite_person",
    "block-stats/composite_person",
    "block-overflow/composite_person",
    "blocks/exact_email",
    "block-stats/exact_email",
    "block-overflow/exact_email",
    "blocks/prefix_name",
    "block-stats/prefix_name",
    "block-overflow/prefix_name",
    "blocks/token_name",
    "block-stats/token_name",
    "block-overflow/token_name",
    "pairs/person",
    "pair-stats/person",
    "pair-overflow/person",
    "evidence/person",
    "golden/person",
];

struct Fixture {
    db: E2eDb,
    nodes: Vec<Node>,
    stream_names: BTreeMap<String, String>,
    full_names: BTreeMap<String, String>,
    root: String,
    digest: Vec<u8>,
}

fn relation_name(prefix: &str, logical_id: &str) -> String {
    format!(
        "{prefix}_{}",
        logical_id.replace(['/', '-'], "_").replace("person", "p")
    )
}

async fn sync_source_records<'e, E>(executor: E)
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::raw_sql(SYNC_SOURCE_RECORDS)
        .execute(executor)
        .await
        .expect("source identities should be synchronized");
}

fn render(sql: &str, names: &BTreeMap<String, String>) -> String {
    let rendered = names.iter().fold(sql.to_string(), |rendered, (id, name)| {
        rendered.replace(&format!("@{{{id}}}"), &format!("public.{name}"))
    });
    assert!(
        !rendered.contains("@{"),
        "compiler SQL retained an unresolved logical relation: {rendered}"
    );
    rendered
}

/// The exact compiler-v9 artifact uses PostgreSQL's STABLE `concat_ws`.
/// This fixture-only equivalent preserves its one- and two-value NULL-skipping
/// behavior while allowing the rest of the evidence graph to be exercised.
fn immutable_evidence_sql(sql: &str) -> String {
    let mut rewritten = sql.to_string();
    for (call, equivalent) in [
        (
            "pg_catalog.concat_ws(pg_catalog.chr(31), l0.normalized, l1.normalized)",
            "(CASE WHEN l0.normalized IS NULL THEN COALESCE(l1.normalized, ''::text) \
              WHEN l1.normalized IS NULL THEN l0.normalized \
              ELSE l0.normalized || pg_catalog.chr(31) || l1.normalized END)",
        ),
        (
            "pg_catalog.concat_ws(pg_catalog.chr(31), r0.normalized, r1.normalized)",
            "(CASE WHEN r0.normalized IS NULL THEN COALESCE(r1.normalized, ''::text) \
              WHEN r1.normalized IS NULL THEN r0.normalized \
              ELSE r0.normalized || pg_catalog.chr(31) || r1.normalized END)",
        ),
        (
            "pg_catalog.concat_ws(pg_catalog.chr(31), l0.normalized)",
            "COALESCE(l0.normalized, ''::text)",
        ),
        (
            "pg_catalog.concat_ws(pg_catalog.chr(31), r0.normalized)",
            "COALESCE(r0.normalized, ''::text)",
        ),
    ] {
        rewritten = rewritten.replace(call, equivalent);
    }
    assert!(
        !rewritten.contains("pg_catalog.concat_ws"),
        "fixture rewrite missed a compiler concat_ws call"
    );
    rewritten
}

async fn create_stream(db: &E2eDb, name: &str, query: String) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT pgtrickle.create_stream_table(
            name => $1, query => $2, schedule => '1h',
            refresh_mode => 'DIFFERENTIAL', initialize => false,
            orchestration_mode => 'EXTERNAL')",
    )
    .bind(name)
    .bind(query)
    .execute(&db.pool)
    .await
    .map(|_| ())
}

fn rank(id: &str) -> u8 {
    if id.starts_with("records/") {
        0
    } else if id.starts_with("normalized/") {
        1
    } else if id.starts_with("blocks/") {
        2
    } else if id.starts_with("block-stats/") {
        3
    } else if id.starts_with("block-overflow/") {
        4
    } else if id.starts_with("pair-stats/") {
        5
    } else if id.starts_with("pair-overflow/") {
        6
    } else if id.starts_with("pairs/") {
        7
    } else if id.starts_with("evidence/") {
        8
    } else {
        9
    }
}

async fn install_mdm_contract(db: &E2eDb) {
    sqlx::raw_sql(
        r#"
        CREATE SCHEMA mdm_graph;
        CREATE TABLE mdm_graph.source_identity_map (
            entity_name text NOT NULL,
            source_name text NOT NULL,
            entity_id uuid NOT NULL,
            source_identity_id uuid PRIMARY KEY
        );
        CREATE TABLE mdm_graph.source_records (
            source_record_key bytea PRIMARY KEY,
            source_record_id uuid NOT NULL,
            source_identity_id uuid NOT NULL,
            active boolean NOT NULL
        );
        CREATE TABLE mdm_graph.definition_limits (
            entity_name text PRIMARY KEY,
            expanded_definition jsonb NOT NULL
        );
        INSERT INTO mdm_graph.source_identity_map VALUES
            ('person', 'crm', '10000000-0000-0000-0000-000000000001',
             '20000000-0000-0000-0000-000000000001'),
            ('person', 'erp', '10000000-0000-0000-0000-000000000001',
             '20000000-0000-0000-0000-000000000002');
        INSERT INTO mdm_graph.definition_limits VALUES
            ('person', '{"limits":{"max_comparator_work":10000}}');

        CREATE FUNCTION mdm_graph.normalize_text(
            raw_value text, cleaner text, cleaner_version integer,
            input_state text, options jsonb
        ) RETURNS TABLE (state text, normalized text, canonical_bytes bytea)
        LANGUAGE SQL IMMUTABLE PARALLEL SAFE
        AS $fn$
            SELECT CASE WHEN raw_value IS NULL OR input_state IN ('null', 'deleted')
                        THEN COALESCE(input_state, 'null') ELSE 'value' END,
                   CASE WHEN raw_value IS NULL OR input_state IN ('null', 'deleted')
                        THEN NULL ELSE lower(trim(raw_value)) END,
                   CASE WHEN raw_value IS NULL OR input_state IN ('null', 'deleted')
                        THEN NULL ELSE convert_to(lower(trim(raw_value)), 'UTF8') END
        $fn$;
        CREATE FUNCTION mdm_graph.normalize_date(
            raw_value date, cleaner text, cleaner_version integer,
            input_state text, options jsonb
        ) RETURNS TABLE (state text, normalized text, canonical_bytes bytea)
        LANGUAGE SQL IMMUTABLE PARALLEL SAFE
        AS $fn$
            SELECT CASE WHEN raw_value IS NULL OR input_state IN ('null', 'deleted')
                        THEN COALESCE(input_state, 'null') ELSE 'value' END,
                   CASE WHEN raw_value IS NULL OR input_state IN ('null', 'deleted')
                        THEN NULL ELSE raw_value::text END,
                   CASE WHEN raw_value IS NULL OR input_state IN ('null', 'deleted')
                        THEN NULL ELSE convert_to(raw_value::text, 'UTF8') END
        $fn$;
        CREATE FUNCTION mdm_graph.normalized_levenshtein_score(
            left_value text, right_value text, max_work bigint
        ) RETURNS integer
        LANGUAGE SQL IMMUTABLE PARALLEL SAFE
        AS $fn$
            SELECT CASE WHEN left_value = right_value THEN 10000
                        WHEN max_work > 0
                         AND abs(length(left_value) - length(right_value)) <= 2 THEN 8000
                        ELSE 0 END
        $fn$;
        CREATE FUNCTION mdm_graph.evidence_digest(value text) RETURNS bytea
        LANGUAGE SQL IMMUTABLE PARALLEL SAFE
        AS $fn$ SELECT convert_to(md5(value), 'UTF8') $fn$;
        CREATE FUNCTION mdm_graph.fixture_date(value integer) RETURNS date
        LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE
        AS $fn$ SELECT DATE '1970-01-01' + value $fn$;
        CREATE CAST (integer AS date)
            WITH FUNCTION mdm_graph.fixture_date(integer) AS ASSIGNMENT;

        CREATE TABLE public.mdm_crm (
            id integer PRIMARY KEY, display_name text, name_state text NOT NULL,
            email text, email_state text NOT NULL, birth_date integer,
            birth_date_state text NOT NULL, changed_at timestamptz NOT NULL,
            deleted boolean NOT NULL DEFAULT false
        );
        CREATE TABLE public.mdm_erp (
            tenant_id integer NOT NULL, external_id text NOT NULL,
            legal_name text, name_state text NOT NULL, email text,
            email_state text NOT NULL, birth_date integer,
            birth_date_state text NOT NULL, changed_at timestamptz NOT NULL,
            PRIMARY KEY (tenant_id, external_id)
        );
        INSERT INTO public.mdm_crm VALUES
            (1, 'Alice Example', 'present', 'shared@example.test', 'present',
             DATE '1990-01-01' - DATE '1970-01-01', 'present', now(), false),
            (2, 'Null Email', 'present', NULL, 'null',
             NULL, 'null', now(), false);
        INSERT INTO public.mdm_erp VALUES
            (7, 'A', 'Alicia Example', 'present', 'shared@example.test', 'present',
             DATE '1990-01-01' - DATE '1970-01-01', 'present', now());

        CREATE VIEW mdm_graph.fixture_source_keys AS
        SELECT pgtrickle.encode_row_id_v2('MDM_SOURCE_KEY_V1', ROW(
                   '10000000-0000-0000-0000-000000000001'::uuid,
                   '20000000-0000-0000-0000-000000000001'::uuid, id)) AS source_record_key,
               '20000000-0000-0000-0000-000000000001'::uuid AS source_identity_id
          FROM public.mdm_crm WHERE deleted IS NOT TRUE
        UNION ALL
        SELECT pgtrickle.encode_row_id_v2('MDM_SOURCE_KEY_V1', ROW(
                   '10000000-0000-0000-0000-000000000001'::uuid,
                   '20000000-0000-0000-0000-000000000002'::uuid,
                   tenant_id, external_id)) AS source_record_key,
               '20000000-0000-0000-0000-000000000002'::uuid AS source_identity_id
          FROM public.mdm_erp;
        "#,
    )
    .execute(&db.pool)
    .await
    .expect("install pg-mdm fixture contract");
    sync_source_records(&db.pool).await;
}

async fn rebuild_and_compare(fixture: &Fixture, phase: &str) {
    for node in &fixture.nodes {
        let full = &fixture.full_names[&node.logical_id];
        let sql = render(&node.defining_sql, &fixture.full_names);
        fixture.db.execute(&format!("TRUNCATE {full}")).await;
        fixture
            .db
            .execute(&format!("INSERT INTO {full} {sql}"))
            .await;
    }
    for node in &fixture.nodes {
        let stream = &fixture.stream_names[&node.logical_id];
        let full = &fixture.full_names[&node.logical_id];
        e2e::oracle::assert_st_query_exact(
            &fixture.db,
            stream,
            &format!("TABLE {full}"),
            &format!("{} FULL equivalence after {phase}", node.logical_id),
        )
        .await;
    }
}

async fn strict_refresh_result<'e, E>(executor: E, root: &str, digest: &[u8], policy: &str) -> Value
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_scalar(
        "SELECT node_results FROM pgtrickle.refresh_graph_strict(
             ARRAY[$1::regclass], $2::bytea, $3)",
    )
    .bind(root)
    .bind(digest)
    .bind(policy)
    .fetch_one(executor)
    .await
    .expect("strict compiler-v9 graph refresh should succeed")
}

fn assert_bootstrap_is_full(results: &Value) {
    let nodes = results.as_object().expect("node results are an object");
    assert!(!nodes.is_empty(), "bootstrap returned no node results");
    assert!(nodes.values().all(|node| node["action"] == "FULL"));
}

fn assert_refresh_is_differential(results: &Value) {
    let nodes = results.as_object().expect("node results are an object");
    assert!(!nodes.is_empty(), "refresh returned no node results");
    assert!(
        nodes
            .values()
            .all(|node| { matches!(node["action"].as_str(), Some("DIFFERENTIAL" | "NO_DATA")) }),
        "steady-state refresh used a non-differential action: {results}"
    );
    assert!(
        nodes.values().any(|node| node["action"] == "DIFFERENTIAL"),
        "mutation did not execute a differential node: {results}"
    );
}

fn assert_no_writes(results: &Value) {
    let nodes = results.as_object().expect("node results are an object");
    assert!(nodes.values().all(|node| {
        ["rows_inserted", "rows_updated", "rows_deleted"]
            .iter()
            .all(|field| node[*field].as_i64() == Some(0))
    }));
}

async fn graph_fingerprint(fixture: &Fixture) -> BTreeMap<String, String> {
    let mut fingerprints = BTreeMap::new();
    for (logical_id, relation) in &fixture.stream_names {
        let fingerprint: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT md5(COALESCE(string_agg(row_to_json(t)::text, '|' \
             ORDER BY row_to_json(t)::text), '')) FROM public.{relation} t"
        )))
        .fetch_one(&fixture.db.pool)
        .await
        .unwrap_or_else(|error| panic!("fingerprint {logical_id}: {error}"));
        fingerprints.insert(logical_id.clone(), fingerprint);
    }
    fingerprints
}

async fn relation_count(fixture: &Fixture, logical_id: &str) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*)::bigint FROM public.{}",
        fixture.stream_names[logical_id]
    )))
    .fetch_one(&fixture.db.pool)
    .await
    .unwrap_or_else(|error| panic!("count {logical_id}: {error}"))
}

async fn setup_fixture() -> Fixture {
    let db = E2eDb::new().await.with_extension().await;
    install_mdm_contract(&db).await;
    let mut artifact: Artifact = serde_json::from_str(ARTIFACT).expect("compiler fixture is JSON");
    assert_eq!(artifact.compiler_version, 9);
    assert_eq!(artifact.roots, ["golden/person"]);
    let logical_ids = artifact
        .nodes
        .iter()
        .map(|node| node.logical_id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        logical_ids,
        EXPECTED_LOGICAL_IDS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
    );
    let golden = artifact
        .nodes
        .iter()
        .find(|node| node.logical_id == "golden/person")
        .expect("compiler artifact has a golden root");
    assert_eq!(golden.dependencies, ["normalized/name", "evidence/person"]);
    artifact.nodes.sort_by_key(|node| rank(&node.logical_id));
    let stream_names = artifact
        .nodes
        .iter()
        .map(|node| {
            (
                node.logical_id.clone(),
                relation_name("mdm_v9_st", &node.logical_id),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let full_names = artifact
        .nodes
        .iter()
        .map(|node| {
            (
                node.logical_id.clone(),
                relation_name("mdm_v9_full", &node.logical_id),
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut admitted_nodes = Vec::new();
    for node in artifact.nodes {
        let exact_query = render(&node.defining_sql, &stream_names);
        if node.logical_id == "evidence/person" {
            let error = create_stream(&db, &stream_names[&node.logical_id], exact_query)
                .await
                .expect_err("the exact compiler-v9 evidence SQL must expose its concat_ws blocker");
            assert!(
                error.to_string().contains("STABLE"),
                "unexpected evidence admission failure: {error}"
            );
            let fixture_query = render(&immutable_evidence_sql(&node.defining_sql), &stream_names);
            create_stream(&db, &stream_names[&node.logical_id], fixture_query)
                .await
                .unwrap_or_else(|error| panic!("immutable evidence was not admitted: {error}"));
        } else {
            create_stream(&db, &stream_names[&node.logical_id], exact_query)
                .await
                .unwrap_or_else(|error| panic!("{} was not admitted: {error}", node.logical_id));
        }
        let full_query = render(&node.defining_sql, &full_names);
        db.execute(&format!(
            "CREATE TABLE {} AS {full_query} WITH NO DATA",
            full_names[&node.logical_id]
        ))
        .await;
        admitted_nodes.push(node);
    }
    let root = format!("public.{}", stream_names["golden/person"]);
    let digest = reference_clients::graph_digest(&db.pool, &root).await;
    let bootstrap = strict_refresh_result(&db.pool, &root, &digest, "ALLOW").await;
    assert_bootstrap_is_full(&bootstrap);
    let fixture = Fixture {
        db,
        nodes: admitted_nodes,
        stream_names,
        full_names,
        root,
        digest,
    };
    rebuild_and_compare(&fixture, "bootstrap").await;
    fixture
}

async fn mutate_refresh_and_compare(fixture: &Fixture, mutation: &str) {
    let mut tx = fixture
        .db
        .pool
        .begin()
        .await
        .expect("begin compiler-v9 mutation");
    sqlx::raw_sql(sqlx::AssertSqlSafe(mutation.to_owned()))
        .execute(&mut *tx)
        .await
        .expect("apply compiler-v9 source mutation");
    sync_source_records(&mut *tx).await;
    sqlx::query("SET LOCAL pg_trickle.refresh_strategy = 'differential'")
        .execute(&mut *tx)
        .await
        .expect("force differential conformance strategy");
    let refresh = strict_refresh_result(&mut *tx, &fixture.root, &fixture.digest, "ERROR").await;
    assert_refresh_is_differential(&refresh);
    tx.commit().await.expect("commit compiler-v9 mutation");
    rebuild_and_compare(fixture, mutation).await;
}

#[tokio::test]
async fn test_pg_mdm_compiler_v9_bootstrap_and_mutations_match_full_without_fallback() {
    let fixture = setup_fixture().await;
    let initial_pairs = relation_count(&fixture, "pairs/person").await;

    // An overflow is a caller-visible terminal condition. Prove the caller
    // can reject it in the same transaction and leave every graph member at
    // its pre-refresh state.
    let before_overflow = graph_fingerprint(&fixture).await;
    let mut overflow_tx = fixture.db.pool.begin().await.expect("begin overflow probe");
    sqlx::raw_sql(
        "INSERT INTO mdm_crm VALUES
         (90, 'Alice Overflow One', 'present', 'shared@example.test', 'present',
          DATE '1990-01-01' - DATE '1970-01-01', 'present', now(), false),
         (91, 'Alice Overflow Two', 'present', 'shared@example.test', 'present',
          DATE '1990-01-01' - DATE '1970-01-01', 'present', now(), false)",
    )
    .execute(&mut *overflow_tx)
    .await
    .expect("insert overflow candidates");
    sync_source_records(&mut *overflow_tx).await;
    sqlx::query("SET LOCAL pg_trickle.refresh_strategy = 'differential'")
        .execute(&mut *overflow_tx)
        .await
        .expect("configure compiler-v9 conformance refresh");
    let refresh =
        strict_refresh_result(&mut *overflow_tx, &fixture.root, &fixture.digest, "ERROR").await;
    assert_refresh_is_differential(&refresh);
    let overflow_relation = &fixture.stream_names["block-overflow/exact_email"];
    let overflow_rows: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*)::bigint FROM public.{overflow_relation}"
    )))
    .fetch_one(&mut *overflow_tx)
    .await
    .expect("read overflow terminal");
    assert!(
        overflow_rows > 0,
        "overflow probe did not reach its terminal"
    );
    overflow_tx
        .rollback()
        .await
        .expect("roll back overflow graph refresh");
    assert_eq!(graph_fingerprint(&fixture).await, before_overflow);

    // Scalar-key insert: crosses the exact-email block limit and exercises
    // all candidate block/stat/overflow families plus evidence and golden.
    mutate_refresh_and_compare(
        &fixture,
        "INSERT INTO mdm_crm VALUES
         (3, 'Alice Third', 'present', 'shared@example.test', 'present',
          DATE '1990-01-01' - DATE '1970-01-01', 'present', now(), false)",
    )
    .await;
    let pairs_after_add = relation_count(&fixture, "pairs/person").await;
    assert!(
        pairs_after_add > initial_pairs,
        "scalar insert did not add a candidate pair"
    );

    // Composite-key insert: proves source-key encoding and propagation from
    // the second record family through the complete graph.
    mutate_refresh_and_compare(
        &fixture,
        "INSERT INTO mdm_erp VALUES
         (7, 'B', 'Alice Fourth', 'present', 'shared@example.test', 'present',
          DATE '1990-01-01' - DATE '1970-01-01', 'present', now())",
    )
    .await;
    let pairs_after_overflow = relation_count(&fixture, "pairs/person").await;
    assert!(
        pairs_after_overflow < pairs_after_add,
        "candidate overflow did not remove suppressed pairs"
    );

    // Change every normalized value type and move the record between exact,
    // prefix, token, and composite candidate blocks.
    mutate_refresh_and_compare(
        &fixture,
        "UPDATE mdm_crm
            SET display_name = 'Zelda Changed', email = 'unique@example.test',
                birth_date = DATE '2001-02-03' - DATE '1970-01-01',
                changed_at = now()
          WHERE id = 3",
    )
    .await;

    // NULL-to-value and value-to-NULL transitions cover normalizer state
    // rows as well as removal and reintroduction of candidate evidence.
    mutate_refresh_and_compare(
        &fixture,
        "UPDATE mdm_crm
            SET email = 'null-now-valued@example.test', email_state = 'present',
                birth_date = DATE '1985-05-05' - DATE '1970-01-01',
                birth_date_state = 'present', changed_at = now()
          WHERE id = 2",
    )
    .await;
    mutate_refresh_and_compare(
        &fixture,
        "UPDATE mdm_crm
            SET display_name = NULL, name_state = 'null',
                email = NULL, email_state = 'null',
                birth_date = NULL, birth_date_state = 'null', changed_at = now()
          WHERE id = 3",
    )
    .await;

    // Exercise both the scalar source's soft-delete predicate and a physical
    // delete from the composite-key source.
    mutate_refresh_and_compare(
        &fixture,
        "UPDATE mdm_crm SET deleted = true, changed_at = now() WHERE id = 1",
    )
    .await;
    mutate_refresh_and_compare(
        &fixture,
        "UPDATE mdm_crm SET deleted = false, changed_at = now() WHERE id = 1",
    )
    .await;
    mutate_refresh_and_compare(
        &fixture,
        "DELETE FROM mdm_erp WHERE tenant_id = 7 AND external_id = 'B'",
    )
    .await;

    // A no-change strict refresh stays incremental and produces no writes.
    let mut no_change_tx = fixture
        .db
        .pool
        .begin()
        .await
        .expect("begin no-change probe");
    sqlx::query("SET LOCAL pg_trickle.refresh_strategy = 'differential'")
        .execute(&mut *no_change_tx)
        .await
        .expect("force differential no-change strategy");
    let no_change =
        strict_refresh_result(&mut *no_change_tx, &fixture.root, &fixture.digest, "ERROR").await;
    assert_no_writes(&no_change);
    assert!(
        no_change
            .as_object()
            .expect("node results are an object")
            .values()
            .all(|node| node["action"] != "FULL")
    );
    no_change_tx
        .commit()
        .await
        .expect("commit no-change refresh");

    // Roll back a successful graph refresh, prove exact restoration, then
    // retry the same source mutation and compare the result with a clean FULL
    // rebuild of every compiler node.
    let before_retry = graph_fingerprint(&fixture).await;
    let retry_sql = "UPDATE mdm_erp
                        SET legal_name = 'Retry Person',
                            email = 'retry@example.test', changed_at = now()
                      WHERE tenant_id = 7 AND external_id = 'A'";
    let mut retry_tx = fixture.db.pool.begin().await.expect("begin retry probe");
    sqlx::raw_sql(retry_sql)
        .execute(&mut *retry_tx)
        .await
        .expect("apply rolled-back mutation");
    sync_source_records(&mut *retry_tx).await;
    sqlx::query("SET LOCAL pg_trickle.refresh_strategy = 'differential'")
        .execute(&mut *retry_tx)
        .await
        .expect("force differential retry strategy");
    let rolled_back_refresh =
        strict_refresh_result(&mut *retry_tx, &fixture.root, &fixture.digest, "ERROR").await;
    assert_refresh_is_differential(&rolled_back_refresh);
    retry_tx.rollback().await.expect("roll back graph refresh");
    assert_eq!(graph_fingerprint(&fixture).await, before_retry);
    mutate_refresh_and_compare(&fixture, retry_sql).await;
}
