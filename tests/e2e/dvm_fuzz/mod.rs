//! Versioned DVM scenarios and their deterministic E2E runner.

#![allow(dead_code, clippy::result_large_err)]

use crate::e2e::{self, E2eDb};
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::path::Path;

pub mod coverage;
pub mod metamorphic;
pub mod mutation;
pub mod query;
pub mod semantic;

pub const SCENARIO_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scenario {
    pub scenario_id: String,
    pub format_version: u32,
    pub generator_version: String,
    pub seed: u64,
    pub schema: SchemaSpec,
    pub initial_data: Vec<String>,
    pub query: QuerySpec,
    pub cycles: Vec<MutationCycle>,
    pub execution: ExecutionSettings,
    pub expected_capability: ExpectedCapability,
    pub features: FeatureVector,
    #[serde(default)]
    pub preserve_sql_boundaries: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSpec {
    pub name: String,
    pub setup_sql: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    pub stream_table: String,
    pub defining_query: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationCycle {
    pub name: String,
    pub mutations: Vec<Mutation>,
    #[serde(default)]
    pub changed_leaves: Vec<String>,
    #[serde(default)]
    pub mutation_intents: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mutation {
    pub sql: String,
    pub expected_affected_rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSettings {
    pub schedule: String,
    pub requested_refresh_mode: String,
    #[serde(default)]
    pub require_differential_history: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectedCapability {
    pub differential: bool,
    pub expected_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureVector {
    pub aggregates: Vec<String>,
    pub joins: Vec<String>,
    pub simultaneous_source_changes: bool,
    pub nullable_groups: bool,
    pub duplicate_rows: bool,
    #[serde(default)]
    pub changed_leaf_buckets: Vec<u8>,
    #[serde(default)]
    pub mutation_intents: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayFailure {
    pub scenario_id: String,
    pub invariant: String,
    pub failure_class: String,
    pub phase: String,
    pub cycle: Option<usize>,
    pub mutation: Option<usize>,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation_diff: Option<e2e::oracle::RelationDiff>,
}

impl std::fmt::Display for ReplayFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}) during {}: {}",
            self.failure_class, self.invariant, self.phase, self.detail
        )
    }
}

pub fn load_scenario(path: &Path) -> Result<Scenario, String> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    let scenario: Scenario = serde_json::from_str(&contents)
        .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;
    validate_scenario(&scenario)?;
    Ok(scenario)
}

pub fn validate_scenario(scenario: &Scenario) -> Result<(), String> {
    if scenario.format_version != SCENARIO_FORMAT_VERSION {
        return Err(format!(
            "unsupported scenario format {} (expected {})",
            scenario.format_version, SCENARIO_FORMAT_VERSION
        ));
    }
    for (label, value) in [
        ("scenario_id", scenario.scenario_id.as_str()),
        ("generator_version", scenario.generator_version.as_str()),
        ("schema.name", scenario.schema.name.as_str()),
        ("query.stream_table", scenario.query.stream_table.as_str()),
        (
            "query.defining_query",
            scenario.query.defining_query.as_str(),
        ),
        ("execution.schedule", scenario.execution.schedule.as_str()),
        (
            "execution.requested_refresh_mode",
            scenario.execution.requested_refresh_mode.as_str(),
        ),
        (
            "expected_capability.expected_mode",
            scenario.expected_capability.expected_mode.as_str(),
        ),
    ] {
        if value.trim().is_empty() {
            return Err(format!("{label} must not be empty"));
        }
    }
    validate_identifier(&scenario.schema.name, "schema.name")?;
    if scenario.query.columns.is_empty() {
        return Err("query.columns must not be empty".to_string());
    }
    if scenario.schema.setup_sql.is_empty() || scenario.initial_data.is_empty() {
        return Err("scenario must contain setup SQL and initial data".to_string());
    }
    if scenario.cycles.is_empty() {
        return Err("scenario must contain at least one mutation cycle".to_string());
    }
    for (cycle_index, cycle) in scenario.cycles.iter().enumerate() {
        if cycle.name.trim().is_empty() || cycle.mutations.is_empty() {
            return Err(format!(
                "cycle {cycle_index} must have a name and mutations"
            ));
        }
        for (mutation_index, mutation) in cycle.mutations.iter().enumerate() {
            if mutation.sql.trim().is_empty() {
                return Err(format!(
                    "cycle {cycle_index} mutation {mutation_index} has empty SQL"
                ));
            }
        }
        if cycle
            .changed_leaves
            .iter()
            .any(|leaf| leaf.trim().is_empty())
        {
            return Err(format!(
                "cycle {cycle_index} contains an empty changed source leaf"
            ));
        }
    }
    Ok(())
}

fn validate_identifier(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        || !value.as_bytes()[0].is_ascii_alphabetic()
    {
        return Err(format!("{label} is not a safe SQL identifier: {value}"));
    }
    Ok(())
}

fn dollar_quote(value: &str) -> String {
    ["$dvm$", value, "$dvm$"].concat()
}

fn failure(
    scenario: &Scenario,
    invariant: &str,
    failure_class: &str,
    phase: impl Into<String>,
    cycle: Option<usize>,
    mutation: Option<usize>,
    detail: impl Into<String>,
) -> ReplayFailure {
    ReplayFailure {
        scenario_id: scenario.scenario_id.clone(),
        invariant: invariant.to_string(),
        failure_class: failure_class.to_string(),
        phase: phase.into(),
        cycle,
        mutation,
        detail: detail.into(),
        relation_diff: None,
    }
}

async fn execute(
    db: &E2eDb,
    scenario: &Scenario,
    sql: &str,
    phase: impl Into<String>,
    cycle: Option<usize>,
    mutation: Option<usize>,
) -> Result<(), ReplayFailure> {
    let phase = phase.into();
    db.try_execute(sql).await.map_err(|e| {
        failure(
            scenario,
            "I-05",
            "ProductFailure",
            phase,
            cycle,
            mutation,
            format!("{e}\nSQL: {sql}"),
        )
    })
}

async fn execute_traced(
    db: &E2eDb,
    scenario: &Scenario,
    sql: &str,
    phase: impl Into<String>,
    cycle: Option<usize>,
    mutation: Option<usize>,
) -> Result<(), ReplayFailure> {
    let phase = phase.into();
    let mut config = vec!["SET pg_trickle.dvm_decision_trace = on"];
    if scenario.execution.require_differential_history {
        config.extend([
            "SET pg_trickle.refresh_strategy = 'differential'",
            "SET pg_trickle.differential_max_change_ratio = 0",
        ]);
    }
    db.try_execute_with_config(&config, sql).await.map_err(|e| {
        failure(
            scenario,
            "I-05",
            "ProductFailure",
            phase,
            cycle,
            mutation,
            format!("{e}\nSQL: {sql}"),
        )
    })
}

async fn compare(
    db: &E2eDb,
    scenario: &Scenario,
    phase: impl Into<String>,
    cycle: Option<usize>,
) -> Result<(), ReplayFailure> {
    let phase = phase.into();
    if let Err(diff) = e2e::oracle::compare_st_to_query(
        db,
        &scenario.query.stream_table,
        &scenario.query.defining_query,
    )
    .await
    {
        let mut replay_failure = failure(
            scenario,
            "I-02",
            if diff.comparison_error.is_some() {
                "OracleFailure"
            } else if diff.schema_mismatch.is_some() {
                "SchemaMismatch"
            } else {
                "MultisetMismatch"
            },
            phase,
            cycle,
            None,
            diff.to_string(),
        );
        replay_failure.relation_diff = Some(diff);
        return Err(replay_failure);
    }
    Ok(())
}

async fn replay_inner(db: &E2eDb, scenario: &Scenario) -> Result<(), ReplayFailure> {
    execute(
        db,
        scenario,
        &format!("CREATE SCHEMA {}", scenario.schema.name),
        "schema setup",
        None,
        None,
    )
    .await?;
    for (index, sql) in scenario.schema.setup_sql.iter().enumerate() {
        execute(db, scenario, sql, format!("setup SQL {index}"), None, None).await?;
    }
    for (index, sql) in scenario.initial_data.iter().enumerate() {
        execute(
            db,
            scenario,
            sql,
            format!("initial data {index}"),
            None,
            None,
        )
        .await?;
    }

    execute(
        db,
        scenario,
        &format!(
            "SELECT pgtrickle.create_stream_table('{}', {}, '{}', '{}')",
            scenario.query.stream_table,
            dollar_quote(&scenario.query.defining_query),
            scenario.execution.schedule,
            scenario.execution.requested_refresh_mode
        ),
        "stream-table admission",
        None,
        None,
    )
    .await?;
    execute_traced(
        db,
        scenario,
        &format!(
            "SELECT pgtrickle.refresh_stream_table('{}')",
            scenario.query.stream_table
        ),
        "initial refresh",
        None,
        None,
    )
    .await?;
    if scenario.expected_capability.differential
        && let Err(product_failure) = e2e::oracle::assert_effective_refresh_mode(
            db,
            &scenario.query.stream_table,
            &scenario.expected_capability.expected_mode,
        )
        .await
    {
        return Err(failure(
            scenario,
            "I-04",
            "SilentFallback",
            "initial refresh mode",
            None,
            None,
            product_failure.reason,
        ));
    }
    compare(db, scenario, "initial exact comparison", None).await?;

    for (cycle_index, cycle) in scenario.cycles.iter().enumerate() {
        let previous_refresh: i64 = if scenario.execution.require_differential_history {
            db.query_scalar(
                "SELECT COALESCE(max(refresh_id), 0) FROM pgtrickle.pgt_refresh_history",
            )
            .await
        } else {
            0
        };
        for (mutation_index, mutation) in cycle.mutations.iter().enumerate() {
            let result = sqlx::raw_sql(sqlx::AssertSqlSafe(mutation.sql.clone()))
                .execute(&db.pool)
                .await
                .map_err(|e| {
                    failure(
                        scenario,
                        "I-05",
                        "GeneratorInvalid",
                        format!("{} mutation", cycle.name),
                        Some(cycle_index),
                        Some(mutation_index),
                        format!("{e}\nSQL: {}", mutation.sql),
                    )
                })?;
            if result.rows_affected() != mutation.expected_affected_rows {
                return Err(failure(
                    scenario,
                    "I-05",
                    "GeneratorInvalid",
                    format!("{} mutation", cycle.name),
                    Some(cycle_index),
                    Some(mutation_index),
                    format!(
                        "expected {} affected rows, got {}\nSQL: {}",
                        mutation.expected_affected_rows,
                        result.rows_affected(),
                        mutation.sql
                    ),
                ));
            }
        }
        let mut captured_cuts: Vec<(i64, String)> = Vec::new();
        if scenario.execution.require_differential_history {
            let source_oids: Vec<i64> = sqlx::query_scalar("SELECT DISTINCT d.source_relid::bigint FROM pgtrickle.pgt_dependencies d JOIN pgtrickle.pgt_stream_tables s USING(pgt_id) WHERE s.pgt_schema || '.' || s.pgt_name=$1")
                .bind(&scenario.query.stream_table).fetch_all(&db.pool).await.map_err(|e| failure(scenario,"I-04","InfrastructureFailure","capture witness",Some(cycle_index),None,e.to_string()))?;
            let mut pending = Vec::new();
            for oid in source_oids {
                let buffer = db.change_buffer_table(oid).await;
                pending.push(format!("SELECT {oid}::bigint AS oid,max(lsn)::text AS lsn FROM {buffer} WHERE action IN ('I','D') AND lsn > (SELECT coalesce(frontier->'sources'->'{oid}'->>'lsn','0/0')::pg_lsn FROM pgtrickle.pgt_stream_tables WHERE pgt_schema || '.' || pgt_name='{}')",scenario.query.stream_table));
            }
            if pending.is_empty() {
                return Err(failure(
                    scenario,
                    "I-04",
                    "CoverageNotReached",
                    "capture witness",
                    Some(cycle_index),
                    None,
                    "no captured sources",
                ));
            }
            let pending = format!(
                "SELECT * FROM ({}) pending WHERE lsn IS NOT NULL",
                pending.join(" UNION ALL ")
            );
            if !db
                .wait_for_condition(
                    "committed captured changes",
                    &format!("SELECT EXISTS ({pending})"),
                    std::time::Duration::from_secs(5),
                    std::time::Duration::from_millis(10),
                )
                .await
            {
                return Err(failure(
                    scenario,
                    "I-04",
                    "CoverageNotReached",
                    "capture witness",
                    Some(cycle_index),
                    None,
                    "no committed captured changes above the consumer cut",
                ));
            }
            captured_cuts = sqlx::query_as(sqlx::AssertSqlSafe(pending))
                .fetch_all(&db.pool)
                .await
                .map_err(|e| {
                    failure(
                        scenario,
                        "I-04",
                        "InfrastructureFailure",
                        "capture witness",
                        Some(cycle_index),
                        None,
                        e.to_string(),
                    )
                })?;
        }
        execute_traced(
            db,
            scenario,
            &format!(
                "SELECT pgtrickle.refresh_stream_table('{}')",
                scenario.query.stream_table
            ),
            format!("{} refresh", cycle.name),
            Some(cycle_index),
            None,
        )
        .await?;
        if let Err(product_failure) = e2e::oracle::assert_effective_refresh_mode(
            db,
            &scenario.query.stream_table,
            &scenario.expected_capability.expected_mode,
        )
        .await
        {
            return Err(failure(
                scenario,
                "I-04",
                "SilentFallback",
                format!("{} refresh mode", cycle.name),
                Some(cycle_index),
                None,
                product_failure.reason,
            ));
        }
        if scenario.execution.require_differential_history {
            for (oid, lsn) in captured_cuts {
                let covered: bool = db.query_scalar(&format!("SELECT coalesce(frontier->'sources'->'{oid}'->>'lsn','0/0')::pg_lsn >= '{lsn}'::pg_lsn FROM pgtrickle.pgt_stream_tables WHERE pgt_schema || '.' || pgt_name='{}'", scenario.query.stream_table)).await;
                if !covered {
                    return Err(failure(
                        scenario,
                        "I-04",
                        "CoverageNotReached",
                        format!("{} captured source cut", cycle.name),
                        Some(cycle_index),
                        None,
                        format!(
                            "source {oid} captured through {lsn}, but refresh held an earlier cut"
                        ),
                    ));
                }
            }
            let witnessed: bool = db
                .query_scalar(&format!(
                    "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables s ON s.pgt_id = h.pgt_id \
                 WHERE s.pgt_schema || '.' || s.pgt_name = '{}' \
                 AND h.refresh_id > {previous_refresh} AND h.status = 'COMPLETED' \
                 AND h.action = 'DIFFERENTIAL' AND NOT h.was_full_fallback)",
                    scenario.query.stream_table
                ))
                .await;
            if !witnessed {
                return Err(failure(
                    scenario,
                    "I-04",
                    "SilentFallback",
                    format!("{} changed-data route", cycle.name),
                    Some(cycle_index),
                    None,
                    "no fresh completed differential history without fallback",
                ));
            }
        }
        compare(
            db,
            scenario,
            format!("{} exact comparison", cycle.name),
            Some(cycle_index),
        )
        .await?;
    }
    Ok(())
}

pub async fn replay(db: &E2eDb, scenario: &Scenario) -> Result<(), ReplayFailure> {
    validate_scenario(scenario).map_err(|detail| {
        failure(
            scenario,
            "I-12",
            "GeneratorInvalid",
            "scenario validation",
            None,
            None,
            detail,
        )
    })?;

    let result = replay_inner(db, scenario).await;
    let cleanup_sql = format!("DROP SCHEMA IF EXISTS {} CASCADE", scenario.schema.name);
    if let Err(failure) = result {
        let postgres_log = collect_postgres_log(db).await;
        write_artifact(scenario, &failure, &postgres_log);
        write_state_snapshot(db, scenario).await;
        let _ = db.try_execute(&cleanup_sql).await;
        return Err(failure);
    }
    let cleanup = db.try_execute(&cleanup_sql).await;
    if let Err(cleanup_error) = cleanup {
        let failure = failure(
            scenario,
            "I-11",
            "InfrastructureFailure",
            "cleanup",
            None,
            None,
            cleanup_error.to_string(),
        );
        let postgres_log = collect_postgres_log(db).await;
        write_artifact(scenario, &failure, &postgres_log);
        return Err(failure);
    }
    Ok(())
}

async fn write_state_snapshot(db: &E2eDb, scenario: &Scenario) {
    let relations: Vec<(String, String)> = sqlx::query_as(
        "SELECT schemaname::text,tablename::text FROM pg_tables WHERE schemaname=$1 \
         UNION ALL SELECT current_setting('pg_trickle.change_buffer_schema'), \
         'changes_' || coalesce(ct.source_stable_name,ct.source_relid::text) \
         FROM pgtrickle.pgt_change_tracking ct JOIN pg_class c ON c.oid=ct.source_relid \
         JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname::text=$1",
    )
    .bind(&scenario.schema.name)
    .fetch_all(&db.pool)
    .await
    .unwrap_or_default();
    let mut rows = serde_json::Map::new();
    let progress: Result<serde_json::Value, _> = sqlx::query_scalar("SELECT coalesce(jsonb_agg(to_jsonb(s)),'[]'::jsonb) FROM pgtrickle.pgt_stream_tables s WHERE pgt_schema=$1").bind(&scenario.schema.name).fetch_one(&db.pool).await;
    if let Ok(value) = progress {
        rows.insert("stream_metadata".to_string(), value);
    }
    for (schema, name) in relations {
        let qualified = format!(
            "\"{}\".\"{}\"",
            schema.replace('"', "\"\""),
            name.replace('"', "\"\"")
        );
        let observation: Result<String, _> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT coalesce(jsonb_agg(to_jsonb(r)),'[]'::jsonb)::text FROM (SELECT * FROM {qualified} LIMIT 100) r"
        ))).fetch_one(&db.pool).await;
        if let Ok(value) = observation {
            rows.insert(qualified, serde_json::Value::String(value));
        }
    }
    let root =
        std::env::var("DVM_ARTIFACT_DIR").unwrap_or_else(|_| "artifacts/dvm-fuzz".to_string());
    if let Ok(contents) = serde_json::to_string_pretty(
        &serde_json::json!({"row_limit_per_relation":100,"relations":rows}),
    ) {
        let _ = std::fs::write(
            std::path::Path::new(&root)
                .join(&scenario.scenario_id)
                .join("boundary-state.json"),
            contents,
        );
    }
}

pub fn render_replay_sql(scenario: &Scenario) -> String {
    let mut sql = String::new();
    let _ = writeln!(sql, "CREATE SCHEMA {};\n", scenario.schema.name);
    for statement in &scenario.schema.setup_sql {
        let _ = writeln!(sql, "{statement};");
    }
    for statement in &scenario.initial_data {
        let _ = writeln!(sql, "{statement};");
    }
    if scenario.execution.require_differential_history {
        let _ = writeln!(
            sql,
            "SET pg_trickle.refresh_strategy = 'differential';\nSET pg_trickle.differential_max_change_ratio = 0;"
        );
    }
    let _ = writeln!(
        sql,
        "SELECT pgtrickle.create_stream_table('{}', {}, '{}', '{}');",
        scenario.query.stream_table,
        dollar_quote(&scenario.query.defining_query),
        scenario.execution.schedule,
        scenario.execution.requested_refresh_mode
    );
    let _ = writeln!(
        sql,
        "SELECT pgtrickle.refresh_stream_table('{}');",
        scenario.query.stream_table
    );
    for cycle in &scenario.cycles {
        let _ = writeln!(sql, "-- cycle: {}", cycle.name);
        for mutation in &cycle.mutations {
            let _ = writeln!(sql, "{};", mutation.sql);
        }
        let _ = writeln!(
            sql,
            "SELECT pgtrickle.refresh_stream_table('{}');",
            scenario.query.stream_table
        );
    }
    sql
}

async fn collect_postgres_log(db: &E2eDb) -> String {
    let Ok(output) = tokio::process::Command::new("docker")
        .args(["logs", "--tail", "1000", db.container_id()])
        .output()
        .await
    else {
        return String::new();
    };
    let mut log = String::from_utf8_lossy(&output.stdout).into_owned();
    log.push_str(&String::from_utf8_lossy(&output.stderr));
    log
}

fn trace_artifact(log: &str) -> String {
    let mut events = Vec::new();
    for trace in parsed_traces(log) {
        if let Some(trace_events) = trace.get("events").and_then(serde_json::Value::as_array) {
            events.extend(trace_events.iter().cloned());
        }
    }
    serde_json::to_string_pretty(&serde_json::json!({
        "events": events,
        "available": !events.is_empty(),
        "source": "postgres_log"
    }))
    .unwrap_or_else(|_| "{\"events\":[],\"available\":false}\n".to_string())
}

fn parsed_traces(log: &str) -> Vec<serde_json::Value> {
    log.lines()
        .filter_map(|line| {
            let raw = line.split_once("dvm_decision_trace=")?.1.trim();
            serde_json::from_str(raw).ok()
        })
        .collect()
}

fn coverage_artifact(log: &str) -> String {
    let mut observed = coverage::SemanticCoverageObservation::default();
    for trace in parsed_traces(log) {
        observed.observe_decision_trace(&trace);
    }
    serde_json::to_string_pretty(&observed).unwrap_or_else(|_| "{}\n".to_string())
}

fn generated_delta_artifact(log: &str) -> String {
    log.lines()
        .rev()
        .find_map(|line| {
            let raw = line
                .split_once("dvm_generated_delta_sql=")
                .map(|(_, value)| value.trim())?;
            serde_json::from_str::<String>(raw).ok()
        })
        .unwrap_or_default()
}

fn write_artifact(scenario: &Scenario, failure: &ReplayFailure, postgres_log: &str) {
    let root = std::env::var_os("DVM_ARTIFACT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("artifacts/dvm-fuzz"));
    let dir = root.join(&scenario.scenario_id);
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    let write = |name: &str, contents: String| {
        let _ = std::fs::write(dir.join(name), contents);
    };
    write(
        "scenario.json",
        serde_json::to_string_pretty(scenario).unwrap_or_default(),
    );
    write(
        "feature_vector.json",
        serde_json::to_string_pretty(&scenario.features).unwrap_or_default(),
    );
    write("setup.sql", scenario.schema.setup_sql.join(";\n"));
    write("defining_query.sql", scenario.query.defining_query.clone());
    write(
        "mutations.sql",
        scenario
            .cycles
            .iter()
            .flat_map(|cycle| cycle.mutations.iter().map(|mutation| mutation.sql.clone()))
            .collect::<Vec<_>>()
            .join(";\n"),
    );
    write("replay.sql", render_replay_sql(scenario));
    write("replay.sh", "just dvm-replay scenario.json\n".to_string());
    let diff = failure.relation_diff.as_ref();
    write("actual_rows.jsonl", String::new());
    write("expected_rows.jsonl", String::new());
    write(
        "extra_rows.jsonl",
        diff.map(|value| value.extra_rows.join("\n"))
            .unwrap_or_default(),
    );
    write(
        "missing_rows.jsonl",
        diff.map(|value| value.missing_rows.join("\n"))
            .unwrap_or_default(),
    );
    write(
        "actual_schema.json",
        diff.and_then(|value| serde_json::to_string_pretty(&value.actual_signature).ok())
            .unwrap_or_default(),
    );
    write(
        "expected_schema.json",
        diff.and_then(|value| serde_json::to_string_pretty(&value.expected_signature).ok())
            .unwrap_or_default(),
    );
    write("dvm_trace.json", trace_artifact(postgres_log));
    write("coverage.json", coverage_artifact(postgres_log));
    write(
        "generated_delta.sql",
        generated_delta_artifact(postgres_log),
    );
    write(
        "postgres.log",
        if postgres_log.is_empty() {
            failure.detail.clone()
        } else {
            postgres_log.to_string()
        },
    );
    write(
        "failure.json",
        serde_json::to_string_pretty(failure).unwrap_or_default(),
    );
    write(
        "environment.txt",
        format!(
            "package_version={}\nscenario_format={}\nos={}\narch={}\n",
            env!("CARGO_PKG_VERSION"),
            SCENARIO_FORMAT_VERSION,
            std::env::consts::OS,
            std::env::consts::ARCH
        ),
    );
}
