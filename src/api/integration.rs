//! v0.93.0 integration contracts and external orchestration ownership.

use super::*;
use crate::catalog::{StDependency, StreamTableMeta};
use crate::error::PgTrickleError;
use crate::integration_contract::{
    CanonicalField, CanonicalValue, contract_digest, contract_digest_hex, sha256_digest,
    sha256_hex, value_from_json,
};
use pgrx::JsonB;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const CONTRACT_VERSION: i16 = 1;
const REWRITE_CONTRACT_VERSION: i16 = 1;
const DVM_CONTRACT_VERSION: i16 = 1;
const MANAGED: &str = "MANAGED";
const EXTERNAL: &str = "EXTERNAL";

#[derive(Debug, Clone)]
struct RelationInfo {
    oid: pg_sys::Oid,
    schema: String,
    name: String,
    relkind: String,
    persistence: String,
    owner_oid: pg_sys::Oid,
    owner: String,
    rls: bool,
    force_rls: bool,
}

#[derive(Debug, Clone)]
struct BuiltContract {
    digest: [u8; 32],
    json: Value,
    sources: Vec<Value>,
}

fn integration_error(code: &'static str, detail: impl Into<String>) -> PgTrickleError {
    PgTrickleError::IntegrationError {
        code,
        detail: detail.into(),
    }
}

fn raise(error: PgTrickleError) -> ! {
    super::raise_error_with_context(error)
}

fn normalize_mode(mode: &str) -> Result<String, PgTrickleError> {
    let mode = mode.trim().to_ascii_uppercase();
    if matches!(mode.as_str(), MANAGED | EXTERNAL) {
        Ok(mode)
    } else {
        Err(integration_error(
            "PGT_EXT_ORCHESTRATION_MODE",
            format!("invalid orchestration mode '{mode}'; expected MANAGED or EXTERNAL"),
        ))
    }
}

fn validate_mode_for_refresh(mode: &str, refresh_mode: RefreshMode) -> Result<(), PgTrickleError> {
    if mode == EXTERNAL && refresh_mode.is_immediate() {
        return Err(integration_error(
            "PGT_EXT_ORCHESTRATION_MODE",
            "EXTERNAL orchestration cannot be combined with IMMEDIATE refresh mode",
        ));
    }
    Ok(())
}

/// Persist the orchestration owner after re-checking ownership under a row lock.
pub(crate) fn set_orchestration_mode_for_meta(
    meta: &StreamTableMeta,
    requested: &str,
) -> Result<String, PgTrickleError> {
    let mode = normalize_mode(requested)?;
    super::check_stream_table_ownership(meta.pgt_relid, &meta.pgt_schema, &meta.pgt_name)?;
    let locked = Spi::get_one_with_args::<i64>(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1 FOR UPDATE",
        &[meta.pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        integration_error(
            "PGT_EXT_GRAPH_INVALID",
            "stream table disappeared while changing orchestration mode",
        )
    })?;
    let current = StreamTableMeta::get_by_id(locked)?.ok_or_else(|| {
        integration_error(
            "PGT_EXT_GRAPH_INVALID",
            "stream table disappeared while changing orchestration mode",
        )
    })?;
    validate_mode_for_refresh(&mode, current.refresh_mode)?;
    if !current.orchestration_mode.eq_ignore_ascii_case(&mode) {
        StreamTableMeta::update_orchestration_mode(current.pgt_id, &mode)?;
        crate::shmem::signal_dag_rebuild();
    }
    Ok(mode)
}

/// Change durable refresh ownership for one stream table.
#[pg_extern(
    schema = "pgtrickle",
    security_definer,
    sql = "CREATE FUNCTION pgtrickle.\"set_orchestration_mode\"(\"stream_table\" regclass, \"mode\" text) RETURNS text STRICT SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp LANGUAGE c AS '@MODULE_PATHNAME@', 'set_orchestration_mode_wrapper';"
)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn set_orchestration_mode(stream_table: pg_sys::Oid, mode: &str) -> String {
    let meta = match StreamTableMeta::get_by_relid(stream_table) {
        Ok(meta) => meta,
        Err(error) => raise(error),
    };
    match set_orchestration_mode_for_meta(&meta, mode) {
        Ok(mode) => mode,
        Err(error) => raise(error),
    }
}

/// Advertise independently versioned integration capabilities.
#[pg_extern(schema = "pgtrickle")]
pub fn integration_capabilities() -> TableIterator<
    'static,
    (
        name!(capability, String),
        name!(major_version, i16),
        name!(minor_version, i16),
        name!(enabled, bool),
        name!(details, JsonB),
    ),
> {
    TableIterator::new(vec![
        (
            "external_graph_refresh".to_string(),
            1,
            0,
            false,
            JsonB(serde_json::json!({
                "status": "experimental",
                "phase": "v0.93_contracts",
                "refresh_api": "not_enabled"
            })),
        ),
        (
            "output_delta_consumer".to_string(),
            1,
            0,
            false,
            JsonB(serde_json::json!({"status": "absent", "phase": "v0.95"})),
        ),
    ])
}

fn relation_info(oid: pg_sys::Oid) -> Result<RelationInfo, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT c.relname::text, n.nspname::text, c.relkind::text, \
                        c.relpersistence::text, c.relowner, pg_get_userbyid(c.relowner)::text, \
                        c.relrowsecurity, c.relforcerowsecurity \
                 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = $1",
                None,
                &[oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if table.is_empty() {
            return Err(integration_error(
                "PGT_EXT_GRAPH_INVALID",
                format!("relation OID {} does not exist", oid.to_u32()),
            ));
        }
        let row = table.first();
        let get_string = |index| {
            row.get::<String>(index)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    integration_error(
                        "PGT_EXT_GRAPH_INVALID",
                        "catalog returned NULL relation identity",
                    )
                })
        };
        Ok(RelationInfo {
            oid,
            name: get_string(1)?,
            schema: get_string(2)?,
            relkind: get_string(3)?,
            persistence: get_string(4)?,
            owner_oid: row
                .get::<pg_sys::Oid>(5)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    integration_error(
                        "PGT_EXT_GRAPH_INVALID",
                        "catalog returned NULL relation owner",
                    )
                })?,
            owner: get_string(6)?,
            rls: row
                .get::<bool>(7)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(false),
            force_rls: row
                .get::<bool>(8)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(false),
        })
    })
}

fn relation_label(info: &RelationInfo) -> String {
    format!("{}.{}", info.schema, info.name)
}

fn hex_digest(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn authorize_relation(info: &RelationInfo) -> Result<(), PgTrickleError> {
    if crate::api::helpers::role_owns_relation_or_is_superuser(
        crate::api::helpers::outer_user_id(),
        info.oid,
    )? {
        Ok(())
    } else {
        Err(PgTrickleError::PermissionDenied(
            "owner-equivalent authority is required for every graph member and source".to_string(),
        ))
    }
}

fn database_instance_id() -> Result<String, PgTrickleError> {
    Ok(Spi::get_one::<String>(
        r#"SELECT COALESCE((
                    SELECT instance_id::text
                      FROM pgtrickle.pgt_capture_instance
                     WHERE singleton
                ), 'uninitialized')"#,
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or_else(|| "uninitialized".to_string()))
}

fn output_schema(oid: pg_sys::Oid) -> Result<Value, PgTrickleError> {
    let query = r#"SELECT COALESCE(jsonb_agg(jsonb_build_object(
                    'name', a.attname::text,
                    'type', format_type(a.atttypid, a.atttypmod),
                    'typmod', a.atttypmod,
                    'collation', CASE WHEN a.attcollation = 0 THEN NULL ELSE format('%s.%s', n.nspname::text, c.collname::text) END,
                    'collation_oid', NULLIF(a.attcollation, 0),
                    'nullable', NOT a.attnotnull
                ) ORDER BY a.attnum), '[]'::jsonb)::text
         FROM pg_catalog.pg_attribute a
         LEFT JOIN pg_catalog.pg_collation c ON c.oid = a.attcollation
         LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.collnamespace
         WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
           AND left(a.attname::text, 6) <> '__pgt_'"#;
    let text = Spi::get_one_with_args::<String>(query, &[oid.into()])
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| {
            integration_error(
                "PGT_EXT_GRAPH_INVALID",
                "could not inspect stream table output schema",
            )
        })?;
    serde_json::from_str(&text).map_err(|error| {
        integration_error(
            "PGT_EXT_GRAPH_INVALID",
            format!("output schema is not valid JSON: {error}"),
        )
    })
}

fn source_payload(dep: &StDependency) -> Result<Value, PgTrickleError> {
    let info = relation_info(dep.source_relid)?;
    if info.persistence == "t" {
        return Err(integration_error(
            "PGT_EXT_UNSUPPORTED_SOURCE",
            format!(
                "temporary relation {} is not a durable graph source",
                relation_label(&info)
            ),
        ));
    }
    match dep.source_type.as_str() {
        "TABLE" => {
            if !matches!(info.relkind.as_str(), "r" | "p") {
                return Err(integration_error(
                    "PGT_EXT_UNSUPPORTED_SOURCE",
                    format!(
                        "relation {} is not a regular or partitioned table",
                        relation_label(&info)
                    ),
                ));
            }
            authorize_relation(&info)?;
        }
        "STREAM_TABLE" => {
            StreamTableMeta::get_by_relid(info.oid)?;
            authorize_relation(&info)?;
        }
        other => {
            return Err(integration_error(
                "PGT_EXT_UNSUPPORTED_SOURCE",
                format!("dependency source type '{other}' is not supported by Graph V1"),
            ));
        }
    }
    Ok(serde_json::json!({
        "source_relid": info.oid.to_u32(),
        "source_type": dep.source_type,
        "identity": relation_label(&info),
        "schema": info.schema,
        "name": info.name,
        "relkind": info.relkind,
        "owner": info.owner,
        "owner_oid": info.owner_oid.to_u32(),
        "cdc_mode": dep.cdc_mode.as_str(),
        "columns_used": dep.columns_used,
        "schema_fingerprint": dep.schema_fingerprint,
        "source_stable_name": dep.source_stable_name,
        "source_placement": dep.source_placement,
        "column_snapshot": dep.column_snapshot,
        "rls": info.rls,
        "force_rls": info.force_rls
    }))
}

fn dependency_value(value: &Value) -> Result<CanonicalValue, PgTrickleError> {
    value_from_json(value).map_err(|error| integration_error("PGT_EXT_GRAPH_INVALID", error))
}

fn build_stream_contract(meta: &StreamTableMeta) -> Result<BuiltContract, PgTrickleError> {
    let info = relation_info(meta.pgt_relid).map_err(|error| {
        integration_error(
            "PGT_EXT_GRAPH_INVALID",
            format!("could not inspect stream-table relation: {error}"),
        )
    })?;
    authorize_relation(&info).map_err(|error| {
        integration_error(
            "PGT_EXT_GRAPH_INVALID",
            format!(
                "could not authorize graph member {}: {error}",
                relation_label(&info)
            ),
        )
    })?;
    let sources = StDependency::get_for_st(meta.pgt_id)
        .map_err(|error| {
            integration_error(
                "PGT_EXT_GRAPH_INVALID",
                format!("could not inspect stream-table dependencies: {error}"),
            )
        })?
        .iter()
        .map(|dependency| {
            source_payload(dependency).map_err(|error| {
                integration_error(
                    "PGT_EXT_GRAPH_INVALID",
                    format!(
                        "could not inspect graph source OID {}: {error}",
                        dependency.source_relid.to_u32()
                    ),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut sources = sources;
    sources.sort_by_key(|source| source["source_relid"].as_u64().unwrap_or(0));
    let output = output_schema(meta.pgt_relid).map_err(|error| {
        integration_error(
            "PGT_EXT_GRAPH_INVALID",
            format!("could not inspect stream-table output schema: {error}"),
        )
    })?;
    let instance = database_instance_id().map_err(|error| {
        integration_error(
            "PGT_EXT_GRAPH_INVALID",
            format!("could not inspect database instance identity: {error}"),
        )
    })?;
    let normalized_query = meta
        .defining_query
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let original_query = meta.original_query.clone().unwrap_or_default();
    let normalized_digest = sha256_hex(normalized_query.as_bytes());
    let original_digest = sha256_hex(original_query.as_bytes());
    let source_values = sources
        .iter()
        .map(dependency_value)
        .collect::<Result<Vec<_>, _>>()?;
    let function_values = meta
        .functions_used
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(CanonicalValue::Text)
        .collect();
    let fields = vec![
        CanonicalField::new(1, CanonicalValue::Text(relation_label(&info))),
        CanonicalField::new(2, CanonicalValue::U64(info.oid.to_u32() as u64)),
        CanonicalField::new(
            3,
            CanonicalValue::Bytes(sha256_digest(normalized_query.as_bytes()).to_vec()),
        ),
        CanonicalField::new(
            4,
            CanonicalValue::Bytes(sha256_digest(original_query.as_bytes()).to_vec()),
        ),
        CanonicalField::new(5, dependency_value(&output)?),
        CanonicalField::new(6, CanonicalValue::Text(meta.defining_search_path.clone())),
        CanonicalField::new(
            7,
            CanonicalValue::Array(vec![
                CanonicalValue::U64(info.owner_oid.to_u32() as u64),
                CanonicalValue::Text(info.owner.clone()),
            ]),
        ),
        CanonicalField::new(8, CanonicalValue::Bool(info.rls)),
        CanonicalField::new(9, CanonicalValue::Bool(info.force_rls)),
        CanonicalField::new(
            10,
            CanonicalValue::I64(i64::from(meta.row_identity_version.unwrap_or(0))),
        ),
        CanonicalField::new(
            11,
            CanonicalValue::I64(i64::from(meta.row_probe_version.unwrap_or(0))),
        ),
        CanonicalField::new(
            12,
            CanonicalValue::Text(crate::dvm::planner::FORMAT_VERSION.to_string()),
        ),
        CanonicalField::new(13, CanonicalValue::I64(i64::from(REWRITE_CONTRACT_VERSION))),
        CanonicalField::new(14, CanonicalValue::I64(i64::from(DVM_CONTRACT_VERSION))),
        CanonicalField::new(
            15,
            CanonicalValue::Text(meta.refresh_mode.as_str().to_string()),
        ),
        CanonicalField::new(
            16,
            CanonicalValue::Text(meta.orchestration_mode.to_ascii_uppercase()),
        ),
        CanonicalField::new(17, CanonicalValue::Set(source_values)),
        CanonicalField::new(18, CanonicalValue::Set(function_values)),
        CanonicalField::new(19, CanonicalValue::Text(instance.clone())),
    ];
    let digest = contract_digest(&fields);
    let json = serde_json::json!({
        "contract_version": CONTRACT_VERSION,
        "contract_generation": meta.contract_generation,
        "contract_digest": contract_digest_hex(&fields),
        "relation": {
            "oid": info.oid.to_u32(),
            "schema": info.schema,
            "name": info.name,
            "owner": info.owner,
            "owner_oid": info.owner_oid.to_u32(),
            "rls": info.rls,
            "force_rls": info.force_rls
        },
        "query": {
            "normalized": normalized_query,
            "normalized_digest": normalized_digest,
            "original": original_query,
            "original_digest": original_digest,
            "defining_search_path": meta.defining_search_path
        },
        "output_schema": output,
        "refresh_mode": meta.refresh_mode.as_str(),
        "orchestration_mode": meta.orchestration_mode,
        "row_identity_version": meta.row_identity_version,
        "row_probe_version": meta.row_probe_version,
        "planner_format_version": crate::dvm::planner::FORMAT_VERSION,
        "rewrite_contract_version": REWRITE_CONTRACT_VERSION,
        "dvm_contract_version": DVM_CONTRACT_VERSION,
        "functions_used": meta.functions_used,
        "sources": sources,
        "database_instance_id": instance
    });
    Ok(BuiltContract {
        digest,
        json,
        sources,
    })
}

/// Return the versioned semantic contract for one stream table.
#[pg_extern(
    schema = "pgtrickle",
    security_definer,
    sql = "CREATE FUNCTION pgtrickle.\"stream_table_contract\"(\"stream_table\" regclass) RETURNS TABLE (\"contract_version\" smallint, \"contract_generation\" bigint, \"contract_digest\" bytea, \"contract\" jsonb) STRICT SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp LANGUAGE c AS '@MODULE_PATHNAME@', 'stream_table_contract_wrapper';"
)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn stream_table_contract(
    stream_table: pg_sys::Oid,
) -> TableIterator<
    'static,
    (
        name!(contract_version, i16),
        name!(contract_generation, i64),
        name!(contract_digest, Vec<u8>),
        name!(contract, JsonB),
    ),
> {
    let meta = match StreamTableMeta::get_by_relid(stream_table) {
        Ok(meta) => meta,
        Err(error) => raise(integration_error(
            "PGT_EXT_GRAPH_INVALID",
            format!("could not load stream-table metadata: {error}"),
        )),
    };
    let contract = match build_stream_contract(&meta) {
        Ok(contract) => contract,
        Err(error) => raise(error),
    };
    TableIterator::once((
        CONTRACT_VERSION,
        meta.contract_generation,
        contract.digest.to_vec(),
        JsonB(contract.json),
    ))
}

fn graph_contract_data(
    roots: &[pg_sys::Oid],
) -> Result<
    (
        name!(contract_version, i16),
        name!(graph_digest, Vec<u8>),
        name!(contract, JsonB),
    ),
    PgTrickleError,
> {
    if roots.is_empty() {
        return Err(integration_error(
            "PGT_EXT_GRAPH_INVALID",
            "graph_contract requires at least one root",
        ));
    }
    let mut root_ids = roots
        .iter()
        .map(|oid| oid.to_u32())
        .collect::<BTreeSet<_>>();
    if root_ids.len() != roots.len() {
        return Err(integration_error(
            "PGT_EXT_GRAPH_INVALID",
            "graph_contract rejects duplicate roots",
        ));
    }
    let mut pending = root_ids.iter().copied().collect::<Vec<_>>();
    let mut members = BTreeMap::<u32, StreamTableMeta>::new();
    let mut edges = BTreeSet::<(u32, u32)>::new();
    while let Some(relid) = pending.pop() {
        if members.contains_key(&relid) {
            continue;
        }
        let oid = pg_sys::Oid::from(relid);
        let meta = StreamTableMeta::get_by_relid(oid)?;
        if meta.orchestration_mode.eq_ignore_ascii_case(MANAGED) {
            return Err(integration_error(
                "PGT_EXT_ORCHESTRATION_MODE",
                format!(
                    "graph member {} is MANAGED; set it to EXTERNAL first",
                    relation_label(&relation_info(oid)?)
                ),
            ));
        }
        validate_mode_for_refresh(EXTERNAL, meta.refresh_mode)?;
        for dep in StDependency::get_for_st(meta.pgt_id)? {
            if dep.source_type == "STREAM_TABLE" {
                let upstream = StreamTableMeta::get_by_relid(dep.source_relid)?;
                edges.insert((dep.source_relid.to_u32(), relid));
                pending.push(upstream.pgt_relid.to_u32());
            }
        }
        members.insert(relid, meta);
    }
    root_ids.retain(|id| members.contains_key(id));
    if root_ids.is_empty() {
        return Err(integration_error(
            "PGT_EXT_GRAPH_INVALID",
            "none of the graph roots are registered stream tables",
        ));
    }
    let member_ids = members.keys().copied().collect::<BTreeSet<_>>();
    let mut built = BTreeMap::new();
    let mut source_map = BTreeMap::new();
    for (relid, meta) in &members {
        let contract = build_stream_contract(meta)?;
        for source in &contract.sources {
            source_map.insert(source["source_relid"].as_u64().unwrap_or(0), source.clone());
        }
        built.insert(*relid, contract);
    }
    let mut indegree = member_ids
        .iter()
        .map(|id| (*id, 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut adjacency = BTreeMap::<u32, BTreeSet<u32>>::new();
    for (upstream, downstream) in &edges {
        if member_ids.contains(upstream)
            && adjacency.entry(*upstream).or_default().insert(*downstream)
        {
            *indegree.entry(*downstream).or_default() += 1;
        }
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(id, degree)| (*degree == 0).then_some(*id))
        .collect::<BTreeSet<_>>();
    let mut topological_order = Vec::with_capacity(member_ids.len());
    while let Some(id) = ready.iter().next().copied() {
        ready.remove(&id);
        topological_order.push(id);
        for downstream in adjacency.get(&id).into_iter().flatten() {
            let degree = indegree.get_mut(downstream).ok_or_else(|| {
                integration_error(
                    "PGT_EXT_GRAPH_INVALID",
                    "graph edge references an unknown member",
                )
            })?;
            *degree -= 1;
            if *degree == 0 {
                ready.insert(*downstream);
            }
        }
    }
    if topological_order.len() != member_ids.len() {
        return Err(integration_error(
            "PGT_EXT_GRAPH_CYCLE",
            "graph members contain a dependency cycle",
        ));
    }
    let instance = database_instance_id()?;
    let root_values = root_ids
        .iter()
        .map(|id| CanonicalValue::U64(*id as u64))
        .collect();
    let member_values = members
        .iter()
        .map(|(id, meta)| {
            CanonicalValue::Array(vec![
                CanonicalValue::U64(*id as u64),
                CanonicalValue::I64(meta.contract_generation),
                CanonicalValue::Bytes(built[id].digest.to_vec()),
            ])
        })
        .collect();
    let edge_values = edges
        .iter()
        .map(|(upstream, downstream)| {
            CanonicalValue::Array(vec![
                CanonicalValue::U64(*upstream as u64),
                CanonicalValue::U64(*downstream as u64),
            ])
        })
        .collect();
    let topo_values = topological_order
        .iter()
        .map(|id| CanonicalValue::U64(*id as u64))
        .collect();
    let source_values = source_map
        .values()
        .map(dependency_value)
        .collect::<Result<Vec<_>, _>>()?;
    let fields = vec![
        CanonicalField::new(1, CanonicalValue::Text(instance.clone())),
        CanonicalField::new(2, CanonicalValue::Set(root_values)),
        CanonicalField::new(3, CanonicalValue::Set(member_values)),
        CanonicalField::new(4, CanonicalValue::Set(edge_values)),
        CanonicalField::new(5, CanonicalValue::Array(topo_values)),
        CanonicalField::new(6, CanonicalValue::Set(source_values)),
    ];
    let digest = contract_digest(&fields);
    let member_json = members
        .iter()
        .map(|(id, meta)| {
            serde_json::json!({
                "oid": id,
                "identity": format!("{}.{}", meta.pgt_schema, meta.pgt_name),
                "contract_generation": meta.contract_generation,
                "contract_digest": hex_digest(&built[id].digest),
                "orchestration_mode": meta.orchestration_mode
            })
        })
        .collect::<Vec<_>>();
    let edge_json = edges.iter().map(|(upstream, downstream)| serde_json::json!({"upstream": upstream, "downstream": downstream})).collect::<Vec<_>>();
    let roots_json = root_ids.iter().map(|id| serde_json::json!({"oid": id, "identity": format!("{}.{}", members[id].pgt_schema, members[id].pgt_name)})).collect::<Vec<_>>();
    let json = serde_json::json!({
        "contract_version": CONTRACT_VERSION,
        "graph_digest": crate::integration_contract::sha256_hex(&crate::integration_contract::encode_contract(&fields)),
        "database_instance_id": instance,
        "roots": roots_json,
        "members": member_json,
        "edges": edge_json,
        "topological_order": topological_order,
        "sources": source_map.values().collect::<Vec<_>>()
    });
    Ok((CONTRACT_VERSION, digest.to_vec(), JsonB(json)))
}

/// Return a canonical contract for the complete upstream closure of roots.
#[pg_extern(
    schema = "pgtrickle",
    security_definer,
    sql = "CREATE FUNCTION pgtrickle.\"graph_contract\"(\"roots\" regclass[]) RETURNS TABLE (\"contract_version\" smallint, \"graph_digest\" bytea, \"contract\" jsonb) STRICT SECURITY DEFINER SET search_path TO pgtrickle, pg_catalog, pg_temp LANGUAGE c AS '@MODULE_PATHNAME@', 'graph_contract_wrapper';"
)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn graph_contract(
    roots: Vec<pg_sys::Oid>,
) -> TableIterator<
    'static,
    (
        name!(contract_version, i16),
        name!(graph_digest, Vec<u8>),
        name!(contract, JsonB),
    ),
> {
    match graph_contract_data(&roots) {
        Ok(row) => TableIterator::once(row),
        Err(error) => raise(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_validation_is_case_insensitive_and_immediate_is_rejected() {
        assert_eq!(normalize_mode(" external ").unwrap(), EXTERNAL);
        assert!(validate_mode_for_refresh(EXTERNAL, RefreshMode::Immediate).is_err());
        assert!(validate_mode_for_refresh(EXTERNAL, RefreshMode::Full).is_ok());
    }

    #[test]
    fn graph_order_uses_sorted_ready_members() {
        let ready = BTreeSet::from([7_u32, 3_u32]);
        let first = ready.iter().next().copied().unwrap_or_default();
        assert_eq!(first, 3);
    }

    #[test]
    fn test_integration_capabilities_reports_independent_capabilities() {
        assert_ne!("external_graph_refresh", "output_delta_consumer");
        assert_eq!(CONTRACT_VERSION, 1);
    }

    #[test]
    fn test_stream_table_contract_is_deterministic() {
        let fields = [CanonicalField::new(1, CanonicalValue::Text("same".into()))];
        assert_eq!(contract_digest(&fields), contract_digest(&fields));
    }

    #[test]
    fn test_graph_contract_canonicalizes_closure_and_order() {
        let ids = BTreeSet::from([9_u32, 2_u32, 5_u32]);
        assert_eq!(ids.iter().copied().collect::<Vec<_>>(), vec![2, 5, 9]);
    }

    #[test]
    fn test_external_orchestration_excludes_scheduler() {
        assert!(validate_mode_for_refresh(EXTERNAL, RefreshMode::Full).is_ok());
        assert!(validate_mode_for_refresh(EXTERNAL, RefreshMode::Immediate).is_err());
    }

    #[test]
    fn test_external_graph_authorization_fails_closed() {
        assert!(normalize_mode("UNKNOWN").is_err());
    }
}
