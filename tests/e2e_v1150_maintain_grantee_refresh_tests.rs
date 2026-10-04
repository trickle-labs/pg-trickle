//! v1.0.1150 exact-target MAINTAIN refresh authorization.

mod e2e;

use e2e::E2eDb;

const CREATE_STREAM_ARGS: &str = "text, text, text, text, boolean, text, text, text, boolean, boolean, \
     text, integer, double precision, text, boolean, text, integer, text, text";
const ALTER_STREAM_ARGS: &str = "text, text, text, text, text, text, text, text, boolean, boolean, \
     text, text, bigint, integer, text, integer, double precision, text, double precision, text";

async fn create_role(db: &E2eDb, role: &str) {
    db.execute(&format!(
        "DO $$ BEGIN CREATE ROLE {role}; EXCEPTION WHEN duplicate_object THEN NULL; END $$"
    ))
    .await;
    db.execute(&format!(
        "GRANT USAGE ON SCHEMA public, pgtrickle TO {role}"
    ))
    .await;
    db.execute(&format!("GRANT CREATE ON SCHEMA public TO {role}"))
        .await;
}

async fn grant_create_api(db: &E2eDb, role: &str) {
    db.execute(&format!(
        "GRANT EXECUTE ON FUNCTION pgtrickle.create_stream_table({CREATE_STREAM_ARGS}) TO {role}"
    ))
    .await;
}

async fn grant_refresh_apis(db: &E2eDb, role: &str) {
    for signature in [
        "refresh_stream_table(text)",
        "stream_table_contract(regclass)",
        "graph_contract(regclass[])",
        "refresh_graph_strict(regclass[], bytea, text)",
        "write_and_refresh(text, text)",
    ] {
        db.execute(&format!(
            "GRANT EXECUTE ON FUNCTION pgtrickle.{signature} TO {role}"
        ))
        .await;
    }
}

async fn create_stream_as(
    db: &E2eDb,
    owner: &str,
    name: &str,
    query: &str,
    mode: &str,
    orchestration: &str,
) {
    grant_create_api(db, owner).await;
    let statement = format!(
        "SELECT pgtrickle.create_stream_table(\
            name => '{name}', query => $query${query}$query$, schedule => '1h', \
            refresh_mode => '{mode}', initialize => false, \
            orchestration_mode => '{orchestration}')"
    );
    try_as_role(db, owner, &statement)
        .await
        .unwrap_or_else(|error| panic!("creating {name} as {owner} failed: {error}"));
}

async fn try_as_role(db: &E2eDb, role: &str, sql: &str) -> Result<(), sqlx::Error> {
    db.try_execute_with_role(&format!("SET ROLE {role}"), sql, "RESET ROLE")
        .await
}

async fn try_as_role_with_path(
    db: &E2eDb,
    role: &str,
    search_path: &str,
    sql: &str,
) -> Result<(), sqlx::Error> {
    let mut connection = db.pool.acquire().await.expect("acquire role connection");
    sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE {role}")))
        .execute(&mut *connection)
        .await
        .expect("set test role");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SET search_path TO {search_path}"
    )))
    .execute(&mut *connection)
    .await
    .expect("set test search_path");
    let result = sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
        .execute(&mut *connection)
        .await
        .map(|_| ());
    let _ = sqlx::query("RESET ROLE").execute(&mut *connection).await;
    let _ = sqlx::query("RESET search_path")
        .execute(&mut *connection)
        .await;
    result
}

async fn query_as_role_text(db: &E2eDb, role: &str, sql: &str) -> String {
    let mut connection = db.pool.acquire().await.expect("acquire role connection");
    sqlx::query(sqlx::AssertSqlSafe(format!("SET ROLE {role}")))
        .execute(&mut *connection)
        .await
        .expect("set test role");
    let result = sqlx::query_scalar::<_, String>(sqlx::AssertSqlSafe(sql.to_owned()))
        .fetch_one(&mut *connection)
        .await
        .unwrap_or_else(|error| panic!("role query failed: {error}; SQL: {sql}"));
    sqlx::query("RESET ROLE")
        .execute(&mut *connection)
        .await
        .expect("reset test role");
    result
}

async fn setup_basic_stream(db: &E2eDb, stream: &str, external: bool) {
    create_role(db, "v1150_owner").await;
    create_role(db, "v1150_worker").await;
    db.execute(&format!(
        "CREATE TABLE {stream}_source (id integer PRIMARY KEY)"
    ))
    .await;
    db.execute(&format!("INSERT INTO {stream}_source VALUES (1)"))
        .await;
    db.execute(&format!("GRANT SELECT ON {stream}_source TO v1150_owner"))
        .await;
    create_stream_as(
        db,
        "v1150_owner",
        stream,
        &format!("SELECT id FROM {stream}_source"),
        "FULL",
        if external { "EXTERNAL" } else { "MANAGED" },
    )
    .await;
    grant_refresh_apis(db, "v1150_worker").await;
}

async fn manual_history_count(db: &E2eDb, stream: &str) -> i64 {
    db.query_scalar(&format!(
        "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
         JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
         WHERE s.pgt_name = '{stream}' AND h.initiated_by = 'MANUAL'"
    ))
    .await
}

async fn state_oracle(db: &E2eDb, streams: &[&str]) -> String {
    let contents = streams
        .iter()
        .map(|stream| {
            format!(
                "'{stream}', COALESCE((SELECT jsonb_agg(to_jsonb(t) ORDER BY t.id) \
                 FROM public.{stream} t), '[]'::jsonb)"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let names = streams
        .iter()
        .map(|stream| format!("'{stream}'"))
        .collect::<Vec<_>>()
        .join(", ");
    db.query_scalar(&format!(
        "SELECT jsonb_build_object(\
            'contents', jsonb_build_object({contents}), \
            'catalog', COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY s.pgt_id) \
                FROM pgtrickle.pgt_stream_tables s WHERE s.pgt_name IN ({names})), '[]'::jsonb), \
            'member_history', COALESCE((SELECT jsonb_agg(to_jsonb(h) ORDER BY h.refresh_id) \
                FROM pgtrickle.pgt_refresh_history h JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                WHERE s.pgt_name IN ({names})), '[]'::jsonb), \
            'graph_batches', COALESCE((SELECT jsonb_agg(to_jsonb(b) ORDER BY b.pgt_id, b.batch_token) \
                FROM pgtrickle.pgt_output_delta_batches b JOIN pgtrickle.pgt_stream_tables s USING (pgt_id) \
                WHERE s.pgt_name IN ({names}) AND b.graph_refresh_id IS NOT NULL), '[]'::jsonb), \
            'graph_sequence', (SELECT jsonb_build_array(last_value, is_called) \
                FROM pgtrickle.pgt_graph_refresh_id_seq))::text"
    ))
    .await
}

async fn setup_graph_closure(db: &E2eDb) {
    create_role(db, "v1150_owner").await;
    create_role(db, "v1150_worker").await;
    db.execute("CREATE TABLE v1150_graph_source (id integer PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO v1150_graph_source VALUES (1)")
        .await;
    db.execute("GRANT SELECT ON v1150_graph_source TO v1150_owner")
        .await;
    create_stream_as(
        db,
        "v1150_owner",
        "v1150_upstream",
        "SELECT id FROM v1150_graph_source",
        "FULL",
        "EXTERNAL",
    )
    .await;
    create_stream_as(
        db,
        "v1150_owner",
        "v1150_dependent",
        "SELECT id FROM v1150_upstream",
        "FULL",
        "EXTERNAL",
    )
    .await;
    grant_refresh_apis(db, "v1150_worker").await;
    db.execute("GRANT SELECT, MAINTAIN ON v1150_graph_source TO v1150_worker")
        .await;
    db.execute("GRANT MAINTAIN ON v1150_upstream, v1150_dependent TO v1150_worker")
        .await;
}

#[tokio::test]
async fn test_v1150_owner_can_refresh_stream_table() {
    let db = E2eDb::new().await.with_extension().await;
    setup_basic_stream(&db, "v1150_owner_ok", false).await;
    grant_refresh_apis(&db, "v1150_owner").await;

    try_as_role(
        &db,
        "v1150_owner",
        "SELECT pgtrickle.refresh_stream_table('v1150_owner_ok')",
    )
    .await
    .expect("the stream owner should still refresh");

    assert_eq!(db.count("public.v1150_owner_ok").await, 1);
    assert_eq!(manual_history_count(&db, "v1150_owner_ok").await, 1);
}

#[tokio::test]
async fn test_v1150_exact_maintain_grantee_can_refresh_without_ownership() {
    let db = E2eDb::new().await.with_extension().await;
    setup_basic_stream(&db, "v1150_direct", false).await;
    db.execute("GRANT MAINTAIN ON v1150_direct TO v1150_worker")
        .await;

    let owns: bool = db
        .query_scalar("SELECT pg_has_role('v1150_worker', 'v1150_owner', 'USAGE')")
        .await;
    assert!(!owns, "the delegated role must not be owner-equivalent");
    try_as_role(
        &db,
        "v1150_worker",
        "SELECT pgtrickle.refresh_stream_table('v1150_direct')",
    )
    .await
    .expect("exact-target MAINTAIN should authorize manual refresh");

    assert_eq!(db.count("public.v1150_direct").await, 1);
    assert_eq!(manual_history_count(&db, "v1150_direct").await, 1);
}

#[tokio::test]
async fn test_v1150_unrelated_maintain_does_not_authorize_target() {
    let db = E2eDb::new().await.with_extension().await;
    setup_basic_stream(&db, "v1150_unrelated_target", true).await;
    create_stream_as(
        &db,
        "v1150_owner",
        "v1150_unrelated_other",
        "SELECT id FROM v1150_unrelated_target_source",
        "FULL",
        "EXTERNAL",
    )
    .await;
    db.execute("GRANT SELECT, MAINTAIN ON v1150_unrelated_target_source TO v1150_worker")
        .await;
    db.execute("GRANT MAINTAIN ON v1150_unrelated_other TO v1150_worker")
        .await;
    let before = state_oracle(&db, &["v1150_unrelated_target"]).await;

    for sql in [
        "SELECT pgtrickle.refresh_stream_table('v1150_unrelated_target')".to_string(),
        "SELECT * FROM pgtrickle.stream_table_contract('public.v1150_unrelated_target'::regclass)"
            .to_string(),
        "SELECT * FROM pgtrickle.graph_contract(ARRAY['public.v1150_unrelated_target'::regclass])"
            .to_string(),
        "SELECT * FROM pgtrickle.refresh_graph_strict(\
            ARRAY['public.v1150_unrelated_target'::regclass], decode(repeat('00', 32), 'hex'))"
            .to_string(),
    ] {
        let error = try_as_role(&db, "v1150_worker", &sql)
            .await
            .expect_err("an unrelated MAINTAIN grant must not admit the target");
        assert!(
            error.to_string().contains("MAINTAIN")
                || error.to_string().contains("owner-equivalent"),
            "denial should identify the missing exact-target authorization: {error}"
        );
    }

    assert_eq!(
        state_oracle(&db, &["v1150_unrelated_target"]).await,
        before,
        "denied direct, contract, and strict calls must preserve all target state"
    );
}

#[tokio::test]
async fn test_v1150_graph_member_authorization_fails_before_mutation() {
    let db = E2eDb::new().await.with_extension().await;
    setup_basic_stream(&db, "v1150_graph_denied", true).await;
    db.execute("GRANT SELECT, MAINTAIN ON v1150_graph_denied_source TO v1150_worker")
        .await;
    let before = state_oracle(&db, &["v1150_graph_denied"]).await;

    let error = try_as_role(
        &db,
        "v1150_worker",
        "SELECT * FROM pgtrickle.refresh_graph_strict(\
            ARRAY['public.v1150_graph_denied'::regclass], decode(repeat('00', 32), 'hex'))",
    )
    .await
    .expect_err("strict graph refresh must reject the unauthorized member");
    assert!(error.to_string().contains("MAINTAIN"));
    assert_eq!(
        state_oracle(&db, &["v1150_graph_denied"]).await,
        before,
        "strict admission must reject before member/frontier/catalog/history/graph mutation"
    );
}

#[tokio::test]
async fn test_v1150_revoked_maintain_blocks_graph_admission() {
    let db = E2eDb::new().await.with_extension().await;
    setup_basic_stream(&db, "v1150_graph_revoke", true).await;
    db.execute("GRANT SELECT, MAINTAIN ON v1150_graph_revoke_source TO v1150_worker")
        .await;
    db.execute("GRANT MAINTAIN ON v1150_graph_revoke TO v1150_worker")
        .await;
    let digest = query_as_role_text(
        &db,
        "v1150_worker",
        "SELECT encode(graph_digest, 'hex') FROM pgtrickle.graph_contract(\
            ARRAY['public.v1150_graph_revoke'::regclass])",
    )
    .await;
    db.execute("REVOKE MAINTAIN ON v1150_graph_revoke FROM v1150_worker")
        .await;
    let before = state_oracle(&db, &["v1150_graph_revoke"]).await;

    for sql in [
        "SELECT * FROM pgtrickle.stream_table_contract('public.v1150_graph_revoke'::regclass)"
            .to_string(),
        "SELECT * FROM pgtrickle.graph_contract(ARRAY['public.v1150_graph_revoke'::regclass])"
            .to_string(),
        format!(
            "SELECT * FROM pgtrickle.refresh_graph_strict(\
                ARRAY['public.v1150_graph_revoke'::regclass], decode('{digest}', 'hex'))"
        ),
    ] {
        try_as_role(&db, "v1150_worker", &sql)
            .await
            .expect_err("revoked MAINTAIN must block fresh and stale-digest admission");
    }

    assert_eq!(
        state_oracle(&db, &["v1150_graph_revoke"]).await,
        before,
        "revoked graph admission must preserve member, frontier, catalog, history, and graph state"
    );
}

#[tokio::test]
async fn test_v1150_maintain_grantee_refreshes_without_source_select() {
    let db = E2eDb::new().await.with_extension().await;
    setup_basic_stream(&db, "v1150_no_source_select", false).await;
    db.execute("GRANT MAINTAIN ON v1150_no_source_select TO v1150_worker")
        .await;
    let has_select: bool = db
        .query_scalar(
            "SELECT has_table_privilege('v1150_worker', 'v1150_no_source_select_source', 'SELECT')",
        )
        .await;
    assert!(
        !has_select,
        "the refresh worker must not have source SELECT"
    );

    try_as_role(
        &db,
        "v1150_worker",
        "SELECT pgtrickle.refresh_stream_table('v1150_no_source_select')",
    )
    .await
    .expect("stored-query owner rights should supply source reads");

    assert_eq!(db.count("public.v1150_no_source_select").await, 1);
    assert_eq!(manual_history_count(&db, "v1150_no_source_select").await, 1);
}

#[tokio::test]
async fn test_v1150_maintain_grantee_can_inspect_graph_contract() {
    let db = E2eDb::new().await.with_extension().await;
    setup_graph_closure(&db).await;

    for member in ["v1150_upstream", "v1150_dependent"] {
        let relation = query_as_role_text(
            &db,
            "v1150_worker",
            &format!(
                "SELECT contract #>> '{{relation,name}}' FROM pgtrickle.stream_table_contract(\
                    'public.{member}'::regclass)"
            ),
        )
        .await;
        assert_eq!(relation, member, "contract must describe {member}");
    }
    let members = query_as_role_text(
        &db,
        "v1150_worker",
        "SELECT string_agg(m.member->>'identity', ',' ORDER BY m.member->>'identity') \
         FROM pgtrickle.graph_contract(ARRAY['public.v1150_dependent'::regclass]) graph, \
         LATERAL jsonb_array_elements(graph.contract->'members') AS m(member)",
    )
    .await;
    assert_eq!(members, "public.v1150_dependent,public.v1150_upstream");
}

#[tokio::test]
async fn test_v1150_maintain_grantee_can_strictly_refresh_graph() {
    let db = E2eDb::new().await.with_extension().await;
    setup_graph_closure(&db).await;
    db.execute("INSERT INTO v1150_graph_source VALUES (2)")
        .await;

    let results = query_as_role_text(
        &db,
        "v1150_worker",
        "SELECT node_results::text FROM pgtrickle.refresh_graph_strict(\
            ARRAY['public.v1150_dependent'::regclass], \
            (SELECT graph_digest FROM pgtrickle.graph_contract(\
                ARRAY['public.v1150_dependent'::regclass])), 'ALLOW')",
    )
    .await;
    assert!(results.contains("public.v1150_upstream"), "{results}");
    assert!(results.contains("public.v1150_dependent"), "{results}");
    assert!(
        results.contains("COMPLETED"),
        "both members should refresh: {results}"
    );
    for member in ["v1150_upstream", "v1150_dependent"] {
        let ids: String = db
            .query_scalar(&format!(
                "SELECT string_agg(id::text, ',' ORDER BY id) FROM public.{member}"
            ))
            .await;
        assert_eq!(ids, "1,2", "strict refresh must update {member}");
    }
}

#[tokio::test]
async fn test_v1150_delegated_refresh_executes_as_stream_owner() {
    let db = E2eDb::new().await.with_extension().await;
    create_role(&db, "v1150_owner").await;
    create_role(&db, "v1150_worker").await;
    db.execute("CREATE TABLE v1150_identity_source (id integer PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO v1150_identity_source VALUES (1)")
        .await;
    db.execute("GRANT SELECT ON v1150_identity_source TO v1150_owner")
        .await;
    db.execute(
        "CREATE FUNCTION v1150_current_user() RETURNS text LANGUAGE sql STABLE \
         SECURITY INVOKER AS $$ SELECT current_user::text $$",
    )
    .await;
    create_stream_as(
        &db,
        "v1150_owner",
        "v1150_identity_st",
        "SELECT id, v1150_current_user() AS evaluated_as FROM v1150_identity_source",
        "FULL",
        "MANAGED",
    )
    .await;
    grant_refresh_apis(&db, "v1150_worker").await;
    db.execute("GRANT MAINTAIN ON v1150_identity_st TO v1150_worker")
        .await;
    db.execute("INSERT INTO v1150_identity_source VALUES (2)")
        .await;

    try_as_role(
        &db,
        "v1150_worker",
        "SELECT pgtrickle.refresh_stream_table('v1150_identity_st')",
    )
    .await
    .expect("delegated caller should refresh");
    let owner: String = db
        .query_scalar("SELECT evaluated_as FROM public.v1150_identity_st WHERE id = 2")
        .await;
    assert_eq!(owner, "v1150_owner");
}

#[tokio::test]
async fn test_v1150_delegated_refresh_preserves_owner_rls() {
    let db = E2eDb::new().await.with_extension().await;
    create_role(&db, "v1150_owner").await;
    create_role(&db, "v1150_worker").await;
    db.execute("CREATE TABLE v1150_rls_source (id integer PRIMARY KEY, tenant text NOT NULL)")
        .await;
    db.execute("INSERT INTO v1150_rls_source VALUES (1, 'v1150_owner'), (2, 'hidden')")
        .await;
    db.execute("ALTER TABLE v1150_rls_source ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute(
        "CREATE POLICY v1150_owner_rows ON v1150_rls_source \
         TO v1150_owner USING (tenant = current_user)",
    )
    .await;
    db.execute("GRANT SELECT ON v1150_rls_source TO v1150_owner")
        .await;
    create_stream_as(
        &db,
        "v1150_owner",
        "v1150_rls_st",
        "SELECT id, tenant FROM v1150_rls_source",
        "FULL",
        "MANAGED",
    )
    .await;
    grant_refresh_apis(&db, "v1150_worker").await;
    db.execute("GRANT MAINTAIN ON v1150_rls_st TO v1150_worker")
        .await;
    db.execute("INSERT INTO v1150_rls_source VALUES (3, 'v1150_owner'), (4, 'hidden')")
        .await;

    try_as_role(
        &db,
        "v1150_worker",
        "SELECT pgtrickle.refresh_stream_table('v1150_rls_st')",
    )
    .await
    .expect("delegated refresh should preserve owner RLS evaluation");

    let rows: String = db
        .query_scalar("SELECT string_agg(id::text, ',' ORDER BY id) FROM public.v1150_rls_st")
        .await;
    assert_eq!(rows, "1,3");
    let hidden: i64 = db
        .query_scalar("SELECT count(*) FROM public.v1150_rls_st WHERE tenant = 'hidden'")
        .await;
    assert_eq!(hidden, 0);
}

#[tokio::test]
async fn test_v1150_delegated_refresh_uses_captured_search_path() {
    let db = E2eDb::new().await.with_extension().await;
    create_role(&db, "v1150_owner").await;
    create_role(&db, "v1150_worker").await;
    db.execute("CREATE SCHEMA v1150_definition").await;
    db.execute("CREATE SCHEMA v1150_caller").await;
    db.execute("GRANT USAGE ON SCHEMA v1150_definition TO v1150_owner")
        .await;
    db.execute("CREATE TABLE v1150_definition.path_probe (id integer PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO v1150_definition.path_probe VALUES (10)")
        .await;
    db.execute("CREATE TABLE v1150_caller.path_probe (id integer PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO v1150_caller.path_probe VALUES (20)")
        .await;
    db.execute("GRANT SELECT ON v1150_definition.path_probe TO v1150_owner")
        .await;
    grant_create_api(&db, "v1150_owner").await;
    let create = "SELECT pgtrickle.create_stream_table(\
        name => 'public.v1150_path_st', query => 'SELECT id FROM path_probe', \
        schedule => '1h', refresh_mode => 'FULL', initialize => false)";
    try_as_role_with_path(&db, "v1150_owner", "v1150_definition, public", create)
        .await
        .expect("defining the stream under its source search_path should succeed");
    grant_refresh_apis(&db, "v1150_worker").await;
    db.execute("GRANT MAINTAIN ON v1150_path_st TO v1150_worker")
        .await;
    db.execute("INSERT INTO v1150_definition.path_probe VALUES (11)")
        .await;

    try_as_role_with_path(
        &db,
        "v1150_worker",
        "v1150_caller, public",
        "SELECT pgtrickle.refresh_stream_table('public.v1150_path_st')",
    )
    .await
    .expect("delegated refresh should use the stored definition path");

    let ids: String = db
        .query_scalar("SELECT string_agg(id::text, ',' ORDER BY id) FROM public.v1150_path_st")
        .await;
    assert_eq!(
        ids, "10,11",
        "refresh must ignore the caller's decoy path object"
    );
}

#[tokio::test]
async fn test_v1150_maintain_does_not_authorize_lifecycle_operations() {
    let db = E2eDb::new().await.with_extension().await;
    setup_basic_stream(&db, "v1150_lifecycle", false).await;
    create_role(&db, "v1150_recipient").await;
    db.execute(&format!(
        "GRANT EXECUTE ON FUNCTION pgtrickle.alter_stream_table({ALTER_STREAM_ARGS}) TO v1150_worker"
    ))
    .await;
    db.execute(
        "GRANT EXECUTE ON FUNCTION pgtrickle.drop_stream_table(text, boolean) TO v1150_worker",
    )
    .await;
    db.execute("GRANT MAINTAIN ON v1150_lifecycle TO v1150_worker")
        .await;
    let before = state_oracle(&db, &["v1150_lifecycle"]).await;
    let definition: String = db
        .query_scalar(
            "SELECT defining_query FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'v1150_lifecycle'",
        )
        .await;
    let owner: String = db
        .query_scalar(
            "SELECT pg_get_userbyid(c.relowner) FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relname = 'v1150_lifecycle'",
        )
        .await;

    for sql in [
        "SELECT pgtrickle.alter_stream_table('v1150_lifecycle', schedule => '2h')",
        "SELECT pgtrickle.drop_stream_table('v1150_lifecycle')",
        "ALTER TABLE public.v1150_lifecycle OWNER TO v1150_recipient",
    ] {
        try_as_role(&db, "v1150_worker", sql)
            .await
            .expect_err("MAINTAIN must not authorize owner-only lifecycle or DDL");
    }

    assert_eq!(state_oracle(&db, &["v1150_lifecycle"]).await, before);
    assert_eq!(
        db.query_scalar::<String>(
            "SELECT defining_query FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'v1150_lifecycle'",
        )
        .await,
        definition
    );
    assert_eq!(
        db.query_scalar::<String>(
            "SELECT pg_get_userbyid(c.relowner) FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = 'public' AND c.relname = 'v1150_lifecycle'",
        )
        .await,
        owner
    );
    assert_eq!(db.count("public.v1150_lifecycle").await, 0);
}

#[tokio::test]
async fn test_v1150_write_and_refresh_maintain_grantee_without_source_select() {
    let db = E2eDb::new().await.with_extension().await;
    create_role(&db, "v1150_owner").await;
    create_role(&db, "v1150_worker").await;
    db.execute("CREATE SCHEMA v1150_write_definition").await;
    db.execute("CREATE SCHEMA v1150_write_caller").await;
    db.execute("GRANT USAGE ON SCHEMA v1150_write_definition TO v1150_owner")
        .await;
    db.execute(
        "CREATE TABLE v1150_write_definition.source (id integer PRIMARY KEY, tenant text NOT NULL)",
    )
    .await;
    db.execute(
        "INSERT INTO v1150_write_definition.source VALUES (1, 'v1150_owner'), (2, 'hidden')",
    )
    .await;
    db.execute("ALTER TABLE v1150_write_definition.source ENABLE ROW LEVEL SECURITY")
        .await;
    db.execute(
        "CREATE POLICY v1150_write_owner_rows ON v1150_write_definition.source \
         TO v1150_owner USING (tenant = current_user)",
    )
    .await;
    db.execute("GRANT SELECT ON v1150_write_definition.source TO v1150_owner")
        .await;
    db.execute("CREATE TABLE v1150_write_caller.source (id integer PRIMARY KEY, tenant text)")
        .await;
    db.execute("INSERT INTO v1150_write_caller.source VALUES (99, 'v1150_owner')")
        .await;
    db.execute("CREATE TABLE v1150_write_probe (who text NOT NULL)")
        .await;
    db.execute("CREATE TABLE v1150_write_denied (who text NOT NULL)")
        .await;
    db.execute("GRANT INSERT ON v1150_write_probe TO v1150_worker")
        .await;
    let create = "SELECT pgtrickle.create_stream_table(\
        name => 'public.v1150_write_st', \
        query => 'SELECT id, tenant, current_user::text AS evaluated_as FROM source', \
        schedule => '1h', refresh_mode => 'FULL', initialize => false)";
    grant_create_api(&db, "v1150_owner").await;
    try_as_role_with_path(&db, "v1150_owner", "v1150_write_definition, public", create)
        .await
        .expect("owner should define the stored query under the definition path");
    grant_refresh_apis(&db, "v1150_worker").await;
    db.execute("GRANT MAINTAIN ON v1150_write_st TO v1150_worker")
        .await;
    db.execute(
        "INSERT INTO v1150_write_definition.source VALUES (3, 'v1150_owner'), (4, 'hidden')",
    )
    .await;
    let has_select: bool = db
        .query_scalar(
            "SELECT has_table_privilege('v1150_worker', \
             'v1150_write_definition.source', 'SELECT')",
        )
        .await;
    let has_probe_insert: bool = db
        .query_scalar("SELECT has_table_privilege('v1150_worker', 'v1150_write_probe', 'INSERT')")
        .await;
    let has_denied_insert: bool = db
        .query_scalar("SELECT has_table_privilege('v1150_worker', 'v1150_write_denied', 'INSERT')")
        .await;
    assert!(!has_select, "worker must not have source SELECT");
    assert!(
        has_probe_insert,
        "worker has only the needed caller-SQL INSERT"
    );
    assert!(
        !has_denied_insert,
        "worker lacks INSERT on the denied probe"
    );

    let before = state_oracle(&db, &["v1150_write_st"]).await;
    let denied = try_as_role_with_path(
        &db,
        "v1150_worker",
        "v1150_write_caller, public",
        "SELECT pgtrickle.write_and_refresh(\
            $sql$INSERT INTO v1150_write_denied VALUES (current_user::text)$sql$, \
            'public.v1150_write_st')",
    )
    .await;
    assert!(denied.is_err(), "caller SQL without INSERT must fail");
    assert_eq!(
        state_oracle(&db, &["v1150_write_st"]).await,
        before,
        "failed invoker SQL must not reach or mutate the refresh"
    );

    try_as_role_with_path(
        &db,
        "v1150_worker",
        "v1150_write_caller, public",
        "SELECT pgtrickle.write_and_refresh(\
            $sql$INSERT INTO v1150_write_probe VALUES (current_user::text)$sql$, \
            'public.v1150_write_st')",
    )
    .await
    .expect("caller SQL with its own INSERT grant and target MAINTAIN should succeed");

    let caller: String = db.query_scalar("SELECT who FROM v1150_write_probe").await;
    assert_eq!(
        caller, "v1150_worker",
        "supplied SQL must run as its caller"
    );
    let output: String = db
        .query_scalar(
            "SELECT string_agg(id::text || ':' || tenant || ':' || evaluated_as, ',' ORDER BY id) \
             FROM public.v1150_write_st",
        )
        .await;
    assert_eq!(
        output, "1:v1150_owner:v1150_owner,3:v1150_owner:v1150_owner",
        "stored query must use owner identity, captured path, and owner RLS"
    );
    assert_eq!(manual_history_count(&db, "v1150_write_st").await, 1);
}
