//! Private catalog lifecycle for benchmarked window-state candidates.

use std::collections::{HashMap, HashSet};

use pgrx::prelude::*;

use crate::catalog::{StreamTableMeta, compute_defining_query_hash};
use crate::dvm::parser::{
    WindowFunctionKind, WindowIncrementalStrategy, WindowStrategyNode, WindowStrategyPlan,
};
use crate::error::PgTrickleError;

pub(crate) const WINDOW_STATE_SCHEMA_VERSION: i16 = 2;
pub(crate) const WINDOW_STATE_STRATEGY_VERSION: i16 =
    crate::dvm::parser::WINDOW_STRATEGY_VERSION as i16;
const WINDOW_STATE_BUDGET_EXCEEDED: &str = "WINDOW_STATE_BUDGET_EXCEEDED";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WindowStateStatus {
    Building,
    Ready,
    Stale,
    OverBudget,
}

impl WindowStateStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Building => "BUILDING",
            Self::Ready => "READY",
            Self::Stale => "STALE",
            Self::OverBudget => "OVER_BUDGET",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "BUILDING" => Some(Self::Building),
            "READY" => Some(Self::Ready),
            "STALE" => Some(Self::Stale),
            "OVER_BUDGET" => Some(Self::OverBudget),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowStateRegistryEntry {
    pub(crate) pgt_id: i64,
    pub(crate) node_ordinal: i32,
    pub(crate) spec_ordinal: i32,
    pub(crate) partition_relid: pg_sys::Oid,
    pub(crate) row_relid: pg_sys::Oid,
    pub(crate) peer_relid: Option<pg_sys::Oid>,
    pub(crate) schema_version: i16,
    pub(crate) strategy_version: i16,
    pub(crate) query_hash: i64,
    pub(crate) state_generation: i64,
    pub(crate) status: WindowStateStatus,
    pub(crate) estimated_bytes: i64,
    pub(crate) last_validated_at: Option<TimestampWithTimeZone>,
    pub(crate) updated_at: TimestampWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowStateExpectation {
    pub(crate) node_ordinal: i32,
    pub(crate) spec_ordinal: i32,
    pub(crate) partition_name: String,
    pub(crate) row_name: String,
    pub(crate) peer_name: Option<String>,
    pub(crate) schema_version: i16,
    pub(crate) strategy_version: i16,
    pub(crate) query_hash: i64,
}

impl WindowStateExpectation {
    pub(crate) fn current(
        node_ordinal: i32,
        spec_ordinal: i32,
        state_name_prefix: &str,
        peer_required: bool,
        query_hash: i64,
    ) -> Self {
        let state_name_prefix = state_name_prefix
            .strip_prefix("pgtrickle.")
            .unwrap_or(state_name_prefix);
        Self {
            node_ordinal,
            spec_ordinal,
            partition_name: format!("{state_name_prefix}_partitions"),
            row_name: format!("{state_name_prefix}_rows"),
            peer_name: peer_required.then(|| format!("{state_name_prefix}_peers")),
            schema_version: WINDOW_STATE_SCHEMA_VERSION,
            strategy_version: WINDOW_STATE_STRATEGY_VERSION,
            query_hash,
        }
    }
}

struct RelationFacts {
    schema: String,
    name: String,
    relkind: String,
    persistence: String,
    owner_matches: bool,
    extension_member: bool,
    has_generation_column: bool,
    public_access_revoked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetColumn {
    name: String,
    type_oid: u32,
    typmod: i32,
    collation_oid: u32,
    not_null: bool,
}

struct TargetLayout {
    qualified_name: String,
    owner_oid: pg_sys::Oid,
    owner_name: String,
    columns: Vec<TargetColumn>,
}

struct RuntimeNodeState {
    pgt_id: i64,
    expected: WindowStateExpectation,
    target: String,
    target_owner_oid: pg_sys::Oid,
    target_owner_name: String,
    target_columns: Vec<TargetColumn>,
    partition_columns: Vec<TargetColumn>,
    order_columns: Vec<(TargetColumn, bool, bool)>,
    identity_columns: Vec<TargetColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadyRowNumberState {
    pub(crate) row_relation: String,
}

fn spi_error(error: pgrx::spi::SpiError) -> PgTrickleError {
    PgTrickleError::SpiError(error.to_string())
}

fn invalid(
    pgt_id: i64,
    node_ordinal: i32,
    spec_ordinal: i32,
    reason: impl Into<String>,
) -> PgTrickleError {
    PgTrickleError::WindowStateInvalid {
        pgt_id,
        node_ordinal,
        spec_ordinal,
        reason: reason.into(),
    }
}

fn budget_invalid(
    pgt_id: i64,
    node_ordinal: i32,
    spec_ordinal: i32,
    reason: impl Into<String>,
) -> PgTrickleError {
    invalid(
        pgt_id,
        node_ordinal,
        spec_ordinal,
        format!("{WINDOW_STATE_BUDGET_EXCEEDED}: {}", reason.into()),
    )
}

fn is_budget_error(error: &PgTrickleError) -> bool {
    matches!(
        error,
        PgTrickleError::WindowStateInvalid { reason, .. }
            if reason.starts_with(WINDOW_STATE_BUDGET_EXCEEDED)
    )
}

fn disable_runtime_for_budget(plan: &WindowStrategyPlan) -> WindowStrategyPlan {
    let mut plan = plan.clone();
    for node in &mut plan.nodes {
        for function in &mut node.functions {
            if function.runtime_enabled {
                function.runtime_enabled = false;
                function.fallback_reason = Some(WINDOW_STATE_BUDGET_EXCEEDED.into());
            }
        }
    }
    plan
}

/// Return the versioned plan for a stream table, lazily populating rows that
/// predate v0.89. Runtime-disabled plans need no private side relations.
pub(crate) fn ensure_plan(
    st: &StreamTableMeta,
) -> Result<Option<WindowStrategyPlan>, PgTrickleError> {
    let query_hash = current_query_hash(st);
    let stored_plan = StreamTableMeta::get_by_id(st.pgt_id)?
        .ok_or_else(|| PgTrickleError::NotFound(format!("pgt_id={}", st.pgt_id)))?
        .window_strategy;
    if let Some(plan) = &stored_plan {
        if plan.query_hash != query_hash {
            return Err(invalid(
                st.pgt_id,
                -1,
                -1,
                format!(
                    "lazy-plan strategy query hash is {}, expected {query_hash}",
                    plan.query_hash
                ),
            ));
        }
        let states = runtime_states_for_plan(st, plan)?;
        let expected: Vec<_> = states.iter().map(|state| state.expected.clone()).collect();
        if !expected.is_empty() {
            // A restored, upgraded, or initialize=false stream can have a
            // valid persisted plan before its first protected materialization.
            // Build only when the registry is wholly absent; a partial
            // registry remains corruption and must fail closed below.
            if entries_for_stream(st.pgt_id)?.is_empty() {
                let rebuilt = rebuild_with_budget_fallback(st.pgt_id, plan, &states)?;
                return Ok(Some(rebuilt));
            }
            validate_registry(st.pgt_id, &expected)?;
            validate_runtime_access(st.pgt_id, &states)?;
        }
        return Ok(stored_plan);
    }

    let mut plan = analyze_and_persist_plan(st, query_hash)?;
    let states = runtime_states_for_plan(st, &plan)?;
    if !states.is_empty() {
        plan = rebuild_with_budget_fallback(st.pgt_id, &plan, &states)?;
    }
    Ok(Some(plan))
}

fn current_query_hash(st: &StreamTableMeta) -> i64 {
    if st.defining_query_hash == 0 {
        compute_defining_query_hash(&st.defining_query)
    } else {
        st.defining_query_hash
    }
}

fn analyze_and_persist_plan(
    st: &StreamTableMeta,
    query_hash: i64,
) -> Result<WindowStrategyPlan, PgTrickleError> {
    let plan = crate::refresh::with_stream_owner(st, || {
        if !crate::dvm::parser::query_has_window_functions(&st.defining_query)? {
            return Ok(WindowStrategyPlan::empty(query_hash));
        }
        Ok(crate::dvm::parse_defining_query_full(&st.defining_query)?
            .window_strategy
            .unwrap_or_else(|| WindowStrategyPlan::empty(query_hash)))
    })?
    .with_query_hash(query_hash)
    .with_state_names(st.pgt_id);
    persist_plan(st.pgt_id, Some(&plan))?;
    Ok(plan)
}

fn simple_column_name(expression: &str) -> Option<String> {
    let expression = expression.trim();
    if expression.starts_with('"') && expression.ends_with('"') && expression.len() >= 2 {
        let mut name = String::new();
        let mut chars = expression[1..expression.len() - 1].chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '"' {
                chars.next_if_eq(&'"')?;
                name.push('"');
            } else {
                name.push(ch);
            }
        }
        return (!name.is_empty()).then_some(name);
    }
    let mut chars = expression.chars();
    let first = chars.next()?;
    if !(first == '_' || first.is_ascii_lowercase())
        || !chars.all(|ch| ch == '_' || ch == '$' || ch.is_ascii_lowercase() || ch.is_ascii_digit())
    {
        return None;
    }
    Some(expression.to_string())
}

fn load_target_layout(st: &StreamTableMeta) -> Result<TargetLayout, PgTrickleError> {
    Spi::connect(|client| {
        let relation = client
            .select(
                "SELECT n.nspname::text, c.relname::text, c.relkind::text, \
                        c.relowner, r.rolname::text \
                 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 JOIN pg_catalog.pg_roles r ON r.oid = c.relowner \
                 WHERE c.oid = $1",
                None,
                &[st.pgt_relid.into()],
            )
            .map_err(spi_error)?;
        if relation.is_empty() {
            return Err(invalid(st.pgt_id, -1, -1, "target stream table is missing"));
        }
        let row = relation.first();
        let schema = row
            .get::<String>(1)
            .map_err(spi_error)?
            .ok_or_else(|| PgTrickleError::InternalError("target schema is NULL".into()))?;
        let name = row
            .get::<String>(2)
            .map_err(spi_error)?
            .ok_or_else(|| PgTrickleError::InternalError("target name is NULL".into()))?;
        let relkind = row
            .get::<String>(3)
            .map_err(spi_error)?
            .ok_or_else(|| PgTrickleError::InternalError("target relkind is NULL".into()))?;
        let owner_oid = row
            .get::<pg_sys::Oid>(4)
            .map_err(spi_error)?
            .ok_or_else(|| PgTrickleError::InternalError("target owner OID is NULL".into()))?;
        let owner_name = row
            .get::<String>(5)
            .map_err(spi_error)?
            .ok_or_else(|| PgTrickleError::InternalError("target owner name is NULL".into()))?;
        if !matches!(relkind.as_str(), "r" | "p") {
            return Err(invalid(
                st.pgt_id,
                -1,
                -1,
                format!("target stream table has unsupported relkind {relkind}"),
            ));
        }

        let rows = client
            .select(
                "SELECT attname::text, atttypid, atttypmod, attcollation, attnotnull \
                 FROM pg_catalog.pg_attribute \
                 WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped \
                 ORDER BY attnum",
                None,
                &[st.pgt_relid.into()],
            )
            .map_err(spi_error)?;
        let mut columns = Vec::with_capacity(rows.len());
        for row in rows {
            columns.push(TargetColumn {
                name: row.get::<String>(1).map_err(spi_error)?.ok_or_else(|| {
                    PgTrickleError::InternalError("target column name is NULL".into())
                })?,
                type_oid: row
                    .get::<pg_sys::Oid>(2)
                    .map_err(spi_error)?
                    .ok_or_else(|| {
                        PgTrickleError::InternalError("target column type is NULL".into())
                    })?
                    .to_u32(),
                typmod: row.get::<i32>(3).map_err(spi_error)?.ok_or_else(|| {
                    PgTrickleError::InternalError("target column typmod is NULL".into())
                })?,
                collation_oid: row
                    .get::<pg_sys::Oid>(4)
                    .map_err(spi_error)?
                    .ok_or_else(|| {
                        PgTrickleError::InternalError("target column collation is NULL".into())
                    })?
                    .to_u32(),
                not_null: row.get::<bool>(5).map_err(spi_error)?.unwrap_or(false),
            });
        }
        if columns.is_empty() {
            return Err(invalid(
                st.pgt_id,
                -1,
                -1,
                "target stream table has no columns",
            ));
        }
        Ok(TargetLayout {
            qualified_name: format!(
                "{}.{}",
                crate::api::quote_identifier(&schema),
                crate::api::quote_identifier(&name)
            ),
            owner_oid,
            owner_name,
            columns,
        })
    })
}

fn target_column(
    columns: &[TargetColumn],
    name: &str,
    pgt_id: i64,
    node_ordinal: i32,
    spec_ordinal: i32,
    role: &str,
) -> Result<TargetColumn, PgTrickleError> {
    columns
        .iter()
        .find(|column| column.name == name)
        .cloned()
        .ok_or_else(|| {
            invalid(
                pgt_id,
                node_ordinal,
                spec_ordinal,
                format!("{role} column {name:?} is not present in the target stream table"),
            )
        })
}

#[allow(clippy::too_many_arguments)]
fn validate_column_metadata(
    column: &TargetColumn,
    type_oid: u32,
    typmod: i32,
    collation_oid: u32,
    pgt_id: i64,
    node_ordinal: i32,
    spec_ordinal: i32,
    role: &str,
) -> Result<(), PgTrickleError> {
    if (column.type_oid, column.typmod, column.collation_oid) == (type_oid, typmod, collation_oid) {
        Ok(())
    } else {
        Err(invalid(
            pgt_id,
            node_ordinal,
            spec_ordinal,
            format!(
                "{role} column {:?} metadata is ({}, {}, {}), expected ({type_oid}, {typmod}, {collation_oid})",
                column.name, column.type_oid, column.typmod, column.collation_oid
            ),
        ))
    }
}

fn has_default_btree_opclass(type_oid: u32) -> Result<bool, PgTrickleError> {
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS ( \
             SELECT 1 FROM pg_catalog.pg_opclass opc \
             JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod \
             WHERE am.amname = 'btree' AND opc.opcdefault AND opc.opcintype = $1 \
         )",
        &[pg_sys::Oid::from(type_oid).into()],
    )
    .map_err(spi_error)
    .map(|value| value.unwrap_or(false))
}

fn order_uses_default_btree(
    type_oid: u32,
    sort_operator_oid: u32,
    equality_operator_oid: u32,
    ascending: bool,
) -> Result<bool, PgTrickleError> {
    let strategy = if ascending { 1_i16 } else { 5_i16 };
    Spi::get_one_with_args::<bool>(
        "SELECT EXISTS ( \
             SELECT 1 \
             FROM pg_catalog.pg_opclass opc \
             JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod \
             JOIN pg_catalog.pg_amop sort_op \
               ON sort_op.amopfamily = opc.opcfamily \
              AND sort_op.amoplefttype = $1 AND sort_op.amoprighttype = $1 \
              AND sort_op.amopopr = $2 AND sort_op.amopstrategy = $4 \
             JOIN pg_catalog.pg_amop equality_op \
               ON equality_op.amopfamily = opc.opcfamily \
              AND equality_op.amoplefttype = $1 AND equality_op.amoprighttype = $1 \
              AND equality_op.amopopr = $3 AND equality_op.amopstrategy = 3 \
             WHERE am.amname = 'btree' AND opc.opcdefault AND opc.opcintype = $1 \
         )",
        &[
            pg_sys::Oid::from(type_oid).into(),
            pg_sys::Oid::from(sort_operator_oid).into(),
            pg_sys::Oid::from(equality_operator_oid).into(),
            strategy.into(),
        ],
    )
    .map_err(spi_error)
    .map(|value| value.unwrap_or(false))
}

fn runtime_node_state(
    st: &StreamTableMeta,
    plan: &WindowStrategyPlan,
    node: &WindowStrategyNode,
    target: &TargetLayout,
) -> Result<RuntimeNodeState, PgTrickleError> {
    let node_ordinal = i32::try_from(node.node_ordinal)
        .map_err(|_| invalid(st.pgt_id, -1, -1, "window node ordinal exceeds int4"))?;
    let spec_ordinal = i32::try_from(node.spec_ordinal).map_err(|_| {
        invalid(
            st.pgt_id,
            node_ordinal,
            -1,
            "window spec ordinal exceeds int4",
        )
    })?;
    let canonical_prefix = format!(
        "pgtrickle.__pgt_window_{}_{}_{}",
        st.pgt_id, node.node_ordinal, node.spec_ordinal
    );
    if node.state_name_prefix != canonical_prefix {
        return Err(invalid(
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            format!(
                "state name prefix is {:?}, expected {canonical_prefix:?}",
                node.state_name_prefix
            ),
        ));
    }

    let enabled: Vec<_> = node
        .functions
        .iter()
        .filter(|function| function.runtime_enabled)
        .collect();
    if enabled.is_empty() {
        return Err(invalid(
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "runtime state was requested for a disabled node",
        ));
    }
    if enabled.iter().any(|function| {
        !function.eligible
            || function.kind != WindowFunctionKind::RowNumber
            || function.strategy != WindowIncrementalStrategy::OrderedSuffix
    }) {
        return Err(invalid(
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "only eligible ROW_NUMBER ordered-suffix state can be materialized",
        ));
    }

    if target
        .columns
        .iter()
        .any(|column| column.name == "state_generation")
    {
        return Err(invalid(
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "target column state_generation conflicts with private state metadata",
        ));
    }
    let row_id = target_column(
        &target.columns,
        "__pgt_row_id",
        st.pgt_id,
        node_ordinal,
        spec_ordinal,
        "internal row identity",
    )?;
    if row_id.type_oid != pg_sys::BYTEAOID.to_u32() {
        return Err(invalid(
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "target __pgt_row_id is not bytea",
        ));
    }

    for function in enabled {
        let column = target_column(
            &target.columns,
            &function.alias,
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "ROW_NUMBER result",
        )?;
        validate_column_metadata(
            &column,
            function.result_type_oid,
            function.result_typmod,
            function.result_collation_oid,
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "ROW_NUMBER result",
        )?;
    }

    let mut partition_names = HashSet::new();
    let mut partition_columns = Vec::with_capacity(node.spec.partition.len());
    for expression in &node.spec.partition {
        let name = simple_column_name(&expression.expression).ok_or_else(|| {
            invalid(
                st.pgt_id,
                node_ordinal,
                spec_ordinal,
                format!(
                    "partition expression {:?} is not a simple target column",
                    expression.expression
                ),
            )
        })?;
        if !partition_names.insert(name.clone()) {
            return Err(invalid(
                st.pgt_id,
                node_ordinal,
                spec_ordinal,
                format!("partition column {name:?} appears more than once"),
            ));
        }
        let column = target_column(
            &target.columns,
            &name,
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "partition",
        )?;
        validate_column_metadata(
            &column,
            expression.type_oid,
            expression.typmod,
            expression.collation_oid,
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "partition",
        )?;
        if !has_default_btree_opclass(column.type_oid)? {
            return Err(invalid(
                st.pgt_id,
                node_ordinal,
                spec_ordinal,
                format!("partition column {name:?} has no default btree operator class"),
            ));
        }
        partition_columns.push(column);
    }

    let mut order_columns = Vec::with_capacity(node.spec.order.len());
    for order in &node.spec.order {
        let name = simple_column_name(&order.expression).ok_or_else(|| {
            invalid(
                st.pgt_id,
                node_ordinal,
                spec_ordinal,
                format!(
                    "order expression {:?} is not a simple target column",
                    order.expression
                ),
            )
        })?;
        let column = target_column(
            &target.columns,
            &name,
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "order",
        )?;
        validate_column_metadata(
            &column,
            order.type_oid,
            order.typmod,
            order.collation_oid,
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "order",
        )?;
        if !order_uses_default_btree(
            column.type_oid,
            order.sort_operator_oid,
            order.equality_operator_oid,
            order.ascending,
        )? {
            return Err(invalid(
                st.pgt_id,
                node_ordinal,
                spec_ordinal,
                format!("order column {name:?} does not use its default btree operators"),
            ));
        }
        order_columns.push((column, order.ascending, order.nulls_first));
    }

    if node.child_identity.is_empty() {
        return Err(invalid(
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "ROW_NUMBER state requires a nonempty exact child identity",
        ));
    }
    let mut identity_names = HashSet::new();
    let mut identity_columns = Vec::with_capacity(node.child_identity.len());
    for name in &node.child_identity {
        if !identity_names.insert(name.clone()) {
            return Err(invalid(
                st.pgt_id,
                node_ordinal,
                spec_ordinal,
                format!("identity column {name:?} appears more than once"),
            ));
        }
        let column = target_column(
            &target.columns,
            name,
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "identity",
        )?;
        if !has_default_btree_opclass(column.type_oid)? {
            return Err(invalid(
                st.pgt_id,
                node_ordinal,
                spec_ordinal,
                format!("identity column {name:?} has no default btree operator class"),
            ));
        }
        identity_columns.push(column);
    }

    let order_key_count = partition_columns.len()
        + order_columns.len()
        + identity_columns
            .iter()
            .filter(|identity| {
                !partition_columns
                    .iter()
                    .any(|column| column.name == identity.name)
                    && !order_columns
                        .iter()
                        .any(|(column, _, _)| column.name == identity.name)
            })
            .count();
    if order_key_count > 32 || identity_columns.len() > 32 || partition_columns.len() > 32 {
        return Err(invalid(
            st.pgt_id,
            node_ordinal,
            spec_ordinal,
            "window state requires more than PostgreSQL's 32 index columns",
        ));
    }

    Ok(RuntimeNodeState {
        pgt_id: st.pgt_id,
        expected: WindowStateExpectation::current(
            node_ordinal,
            spec_ordinal,
            &canonical_prefix,
            false,
            plan.query_hash,
        ),
        target: target.qualified_name.clone(),
        target_owner_oid: target.owner_oid,
        target_owner_name: target.owner_name.clone(),
        target_columns: target.columns.clone(),
        partition_columns,
        order_columns,
        identity_columns,
    })
}

fn runtime_states_for_plan(
    st: &StreamTableMeta,
    plan: &WindowStrategyPlan,
) -> Result<Vec<RuntimeNodeState>, PgTrickleError> {
    let runtime_nodes: Vec<_> = plan
        .nodes
        .iter()
        .filter(|node| {
            node.functions
                .iter()
                .any(|function| function.runtime_enabled)
        })
        .collect();
    if runtime_nodes.is_empty() {
        return Ok(Vec::new());
    }
    let target = load_target_layout(st)?;
    runtime_nodes
        .into_iter()
        .map(|node| runtime_node_state(st, plan, node, &target))
        .collect()
}

fn validate_entry_metadata(
    entry: &WindowStateRegistryEntry,
    expected: &WindowStateExpectation,
) -> Result<(), String> {
    if entry.status != WindowStateStatus::Ready {
        return Err(format!("registry status is {}", entry.status.as_str()));
    }
    if entry.schema_version != expected.schema_version {
        return Err(format!(
            "schema version is {}, expected {}",
            entry.schema_version, expected.schema_version
        ));
    }
    if entry.strategy_version != expected.strategy_version {
        return Err(format!(
            "strategy version is {}, expected {}",
            entry.strategy_version, expected.strategy_version
        ));
    }
    if entry.query_hash != expected.query_hash {
        return Err(format!(
            "query hash is {}, expected {}",
            entry.query_hash, expected.query_hash
        ));
    }
    if entry.peer_relid.is_some() != expected.peer_name.is_some() {
        return Err("peer relation presence does not match the strategy".into());
    }
    Ok(())
}

fn validate_registry_invariants(
    entries: &[WindowStateRegistryEntry],
    allowed_bytes: u64,
) -> Result<(), String> {
    let Some(first) = entries.first() else {
        return Ok(());
    };
    let mut total_bytes = 0_u64;
    let mut relation_owners = HashMap::new();
    for entry in entries {
        if entry.state_generation != first.state_generation {
            return Err(format!(
                "registry generation is {}, expected the shared generation {}",
                entry.state_generation, first.state_generation
            ));
        }
        let bytes = u64::try_from(entry.estimated_bytes)
            .map_err(|_| format!("registry size {} is negative", entry.estimated_bytes))?;
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| "registry size accounting overflowed".to_string())?;

        for (role, oid) in [
            ("partition", Some(entry.partition_relid)),
            ("row", Some(entry.row_relid)),
            ("peer", entry.peer_relid),
        ] {
            let Some(oid) = oid else {
                continue;
            };
            if let Some((owner_node, owner_spec, owner_role)) =
                relation_owners.insert(oid, (entry.node_ordinal, entry.spec_ordinal, role))
            {
                return Err(format!(
                    "relation OID {} is reused by {owner_role} state ({owner_node}, {owner_spec}) and {role} state ({}, {})",
                    oid.to_u32(),
                    entry.node_ordinal,
                    entry.spec_ordinal
                ));
            }
        }
    }
    if total_bytes > allowed_bytes {
        return Err(format!(
            "registry accounts for {total_bytes} bytes, exceeding the {allowed_bytes}-byte per-stream-table window-state budget"
        ));
    }
    Ok(())
}

fn projected_registry_bytes(
    entries: &[WindowStateRegistryEntry],
    node_ordinal: i32,
    spec_ordinal: i32,
    replacement_bytes: i64,
) -> Result<u64, String> {
    let mut found = false;
    let mut total = 0_u64;
    for entry in entries {
        let bytes = if entry.node_ordinal == node_ordinal && entry.spec_ordinal == spec_ordinal {
            found = true;
            replacement_bytes
        } else {
            entry.estimated_bytes
        };
        let bytes = u64::try_from(bytes).map_err(|_| format!("state size {bytes} is negative"))?;
        total = total
            .checked_add(bytes)
            .ok_or_else(|| "state size accounting overflowed".to_string())?;
    }
    if found {
        Ok(total)
    } else {
        Err("BUILDING registry row is missing".into())
    }
}

fn measured_entry_bytes(entry: &WindowStateRegistryEntry) -> Result<i64, PgTrickleError> {
    Spi::get_one_with_args::<i64>(
        "SELECT COALESCE(sum(pg_catalog.pg_total_relation_size(relid)), 0)::bigint \
         FROM ( \
             SELECT DISTINCT unnest(ARRAY[$1::oid, $2::oid, $3::oid]) AS relid \
         ) relations \
         WHERE relid IS NOT NULL",
        &[
            entry.partition_relid.into(),
            entry.row_relid.into(),
            entry.peer_relid.into(),
        ],
    )
    .map_err(spi_error)?
    .ok_or_else(|| PgTrickleError::InternalError("state relation size is NULL".into()))
}

fn relation_facts(oid: pg_sys::Oid) -> Result<Option<RelationFacts>, PgTrickleError> {
    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT n.nspname::text, c.relname::text, c.relkind::text, \
                        c.relpersistence::text, c.relowner = e.extowner, \
                        EXISTS ( \
                            SELECT 1 FROM pg_catalog.pg_depend d \
                            WHERE d.classid = 'pg_catalog.pg_class'::regclass \
                              AND d.objid = c.oid \
                              AND d.refclassid = 'pg_catalog.pg_extension'::regclass \
                              AND d.refobjid = e.oid AND d.deptype = 'e' \
                        ), \
                        EXISTS ( \
                            SELECT 1 FROM pg_catalog.pg_attribute a \
                            WHERE a.attrelid = c.oid \
                              AND a.attname = 'state_generation' \
                              AND a.atttypid = 'pg_catalog.int8'::regtype \
                              AND a.attnotnull AND NOT a.attisdropped \
                        ), \
                        NOT EXISTS ( \
                            SELECT 1 \
                            FROM pg_catalog.aclexplode(COALESCE( \
                                c.relacl, pg_catalog.acldefault('r', c.relowner) \
                            )) acl \
                            WHERE acl.grantee = 0 \
                        ) \
                 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 CROSS JOIN pg_catalog.pg_extension e \
                 WHERE c.oid = $1 AND e.extname = 'pg_trickle'",
                None,
                &[oid.into()],
            )
            .map_err(spi_error)?;
        if table.is_empty() {
            return Ok(None);
        }
        let row = table.first();
        Ok(Some(RelationFacts {
            schema: row
                .get::<String>(1)
                .map_err(spi_error)?
                .ok_or_else(|| PgTrickleError::InternalError("state schema is NULL".into()))?,
            name: row
                .get::<String>(2)
                .map_err(spi_error)?
                .ok_or_else(|| PgTrickleError::InternalError("state name is NULL".into()))?,
            relkind: row
                .get::<String>(3)
                .map_err(spi_error)?
                .ok_or_else(|| PgTrickleError::InternalError("state relkind is NULL".into()))?,
            persistence: row
                .get::<String>(4)
                .map_err(spi_error)?
                .ok_or_else(|| PgTrickleError::InternalError("state persistence is NULL".into()))?,
            owner_matches: row.get::<bool>(5).map_err(spi_error)?.unwrap_or(false),
            extension_member: row.get::<bool>(6).map_err(spi_error)?.unwrap_or(false),
            has_generation_column: row.get::<bool>(7).map_err(spi_error)?.unwrap_or(false),
            public_access_revoked: row.get::<bool>(8).map_err(spi_error)?.unwrap_or(false),
        }))
    })
}

fn validate_relation(
    entry: &WindowStateRegistryEntry,
    oid: pg_sys::Oid,
    expected_name: &str,
) -> Result<(), PgTrickleError> {
    let facts = relation_facts(oid)?.ok_or_else(|| {
        invalid(
            entry.pgt_id,
            entry.node_ordinal,
            entry.spec_ordinal,
            format!("state relation OID {} does not exist", oid.to_u32()),
        )
    })?;
    let mismatch = if facts.schema != "pgtrickle" {
        Some(format!(
            "relation {expected_name} is in schema {}",
            facts.schema
        ))
    } else if facts.name != expected_name {
        Some(format!(
            "relation OID {} is named {}, expected {expected_name}",
            oid.to_u32(),
            facts.name
        ))
    } else if facts.relkind != "r" {
        Some(format!("relation {expected_name} is not an ordinary table"))
    } else if facts.persistence != "p" {
        Some(format!(
            "relation {expected_name} is not permanent LOGGED state"
        ))
    } else if !facts.owner_matches {
        Some(format!(
            "relation {expected_name} is not owned by the extension owner"
        ))
    } else if !facts.extension_member {
        Some(format!(
            "relation {expected_name} is not an extension member"
        ))
    } else if !facts.has_generation_column {
        Some(format!(
            "relation {expected_name} lacks a NOT NULL bigint state_generation column"
        ))
    } else if !facts.public_access_revoked {
        Some(format!("relation {expected_name} grants access to PUBLIC"))
    } else {
        None
    };
    match mismatch {
        Some(reason) => Err(invalid(
            entry.pgt_id,
            entry.node_ordinal,
            entry.spec_ordinal,
            reason,
        )),
        None => {
            let qualified = format!(
                "{}.{}",
                crate::api::quote_identifier(&facts.schema),
                crate::api::quote_identifier(&facts.name)
            );
            // nosemgrep: rust.spi.get_one_with_args.dynamic-format -- qualified contains only quote_identifier-escaped catalog identifiers.
            let generation_matches = Spi::get_one_with_args::<bool>(
                &format!(
                    "SELECT NOT EXISTS ( \
                         SELECT FROM {qualified} \
                         WHERE state_generation IS DISTINCT FROM $1 LIMIT 1 \
                     )"
                ),
                &[entry.state_generation.into()],
            )
            .map_err(spi_error)?
            .unwrap_or(false);
            if generation_matches {
                Ok(())
            } else {
                Err(invalid(
                    entry.pgt_id,
                    entry.node_ordinal,
                    entry.spec_ordinal,
                    format!(
                        "relation {expected_name} contains rows outside registry generation {}",
                        entry.state_generation
                    ),
                ))
            }
        }
    }
}

fn validate_select_privilege(
    state: &RuntimeNodeState,
    oid: pg_sys::Oid,
    role: &str,
) -> Result<(), PgTrickleError> {
    let can_select = Spi::get_one_with_args::<bool>(
        "SELECT pg_catalog.has_table_privilege($1::oid, $2::oid, 'SELECT')",
        &[state.target_owner_oid.into(), oid.into()],
    )
    .map_err(spi_error)?
    .unwrap_or(false);
    if can_select {
        Ok(())
    } else {
        Err(invalid(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            format!("stream owner {role:?} cannot SELECT private window state"),
        ))
    }
}

fn relation_columns(oid: pg_sys::Oid) -> Result<Vec<TargetColumn>, PgTrickleError> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT attname::text, atttypid, atttypmod, attcollation, attnotnull \
                 FROM pg_catalog.pg_attribute \
                 WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped \
                 ORDER BY attnum",
                None,
                &[oid.into()],
            )
            .map_err(spi_error)?;
        rows.into_iter()
            .map(|row| {
                Ok(TargetColumn {
                    name: row.get::<String>(1).map_err(spi_error)?.ok_or_else(|| {
                        PgTrickleError::InternalError("state column name is NULL".into())
                    })?,
                    type_oid: row
                        .get::<pg_sys::Oid>(2)
                        .map_err(spi_error)?
                        .ok_or_else(|| {
                            PgTrickleError::InternalError("state column type is NULL".into())
                        })?
                        .to_u32(),
                    typmod: row.get::<i32>(3).map_err(spi_error)?.ok_or_else(|| {
                        PgTrickleError::InternalError("state column typmod is NULL".into())
                    })?,
                    collation_oid: row
                        .get::<pg_sys::Oid>(4)
                        .map_err(spi_error)?
                        .ok_or_else(|| {
                            PgTrickleError::InternalError("state column collation is NULL".into())
                        })?
                        .to_u32(),
                    not_null: row.get::<bool>(5).map_err(spi_error)?.unwrap_or(false),
                })
            })
            .collect()
    })
}

fn metadata_column(name: &str) -> TargetColumn {
    TargetColumn {
        name: name.into(),
        type_oid: pg_sys::INT8OID.to_u32(),
        typmod: -1,
        collation_oid: pg_sys::InvalidOid.to_u32(),
        not_null: true,
    }
}

fn validate_state_columns(
    state: &RuntimeNodeState,
    entry: &WindowStateRegistryEntry,
) -> Result<(), PgTrickleError> {
    let identity_names: HashSet<_> = state
        .identity_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    let mut expected_rows = state.target_columns.clone();
    for column in &mut expected_rows {
        column.not_null |= identity_names.contains(column.name.as_str());
    }
    expected_rows.push(metadata_column("state_generation"));
    let actual_rows = relation_columns(entry.row_relid)?;
    if actual_rows != expected_rows {
        return Err(invalid(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            format!("row-state columns are {actual_rows:?}, expected {expected_rows:?}"),
        ));
    }

    let mut expected_partitions = state.partition_columns.clone();
    expected_partitions.push(metadata_column("row_count"));
    expected_partitions.push(metadata_column("state_generation"));
    let actual_partitions = relation_columns(entry.partition_relid)?;
    if actual_partitions != expected_partitions {
        return Err(invalid(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            format!(
                "partition-state columns are {actual_partitions:?}, expected {expected_partitions:?}"
            ),
        ));
    }
    Ok(())
}

fn default_btree_opclass(type_oid: u32) -> Result<pg_sys::Oid, PgTrickleError> {
    Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT opc.oid \
         FROM pg_catalog.pg_opclass opc \
         JOIN pg_catalog.pg_am am ON am.oid = opc.opcmethod \
         WHERE am.amname = 'btree' AND opc.opcdefault AND opc.opcintype = $1",
        &[pg_sys::Oid::from(type_oid).into()],
    )
    .map_err(spi_error)?
    .ok_or_else(|| {
        PgTrickleError::InternalError(format!(
            "default btree operator class for type OID {type_oid} disappeared"
        ))
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_state_index(
    state: &RuntimeNodeState,
    relation_oid: pg_sys::Oid,
    relation_columns: &[TargetColumn],
    name: &str,
    keys: &[(String, u32, i16)],
    unique: bool,
    nulls_not_distinct: bool,
) -> Result<(), PgTrickleError> {
    let facts = Spi::connect(|client| {
        let table = client
            .select(
                "SELECT i.indisvalid, i.indisready, i.indisunique, \
                        i.indnullsnotdistinct, am.amname::text, i.indnkeyatts, \
                        i.indkey::text, i.indoption::text, i.indclass::text, \
                        i.indexprs IS NULL AND i.indpred IS NULL \
                 FROM pg_catalog.pg_class idx \
                 JOIN pg_catalog.pg_namespace n ON n.oid = idx.relnamespace \
                 JOIN pg_catalog.pg_index i ON i.indexrelid = idx.oid \
                 JOIN pg_catalog.pg_class table_rel ON table_rel.oid = i.indrelid \
                 JOIN pg_catalog.pg_am am ON am.oid = idx.relam \
                 WHERE n.nspname = 'pgtrickle' AND idx.relname = $1 \
                   AND table_rel.oid = $2",
                None,
                &[name.into(), relation_oid.into()],
            )
            .map_err(spi_error)?;
        if table.is_empty() {
            return Ok(None);
        }
        let row = table.first();
        Ok(Some((
            row.get::<bool>(1).map_err(spi_error)?.unwrap_or(false),
            row.get::<bool>(2).map_err(spi_error)?.unwrap_or(false),
            row.get::<bool>(3).map_err(spi_error)?.unwrap_or(false),
            row.get::<bool>(4).map_err(spi_error)?.unwrap_or(false),
            row.get::<String>(5).map_err(spi_error)?.ok_or_else(|| {
                PgTrickleError::InternalError("index access method is NULL".into())
            })?,
            row.get::<i16>(6)
                .map_err(spi_error)?
                .ok_or_else(|| PgTrickleError::InternalError("index key count is NULL".into()))?,
            row.get::<String>(7)
                .map_err(spi_error)?
                .ok_or_else(|| PgTrickleError::InternalError("index keys are NULL".into()))?,
            row.get::<String>(8)
                .map_err(spi_error)?
                .ok_or_else(|| PgTrickleError::InternalError("index options are NULL".into()))?,
            row.get::<String>(9).map_err(spi_error)?.ok_or_else(|| {
                PgTrickleError::InternalError("index operator classes are NULL".into())
            })?,
            row.get::<bool>(10).map_err(spi_error)?.unwrap_or(false),
        )))
    })?;
    let Some((
        valid,
        ready,
        actual_unique,
        actual_nulls,
        access_method,
        key_count,
        key_text,
        option_text,
        opclass_text,
        simple,
    )) = facts
    else {
        return Err(invalid(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            format!("required state index {name} is missing"),
        ));
    };
    let expected_attnums = keys
        .iter()
        .map(|(name, _, _)| {
            relation_columns
                .iter()
                .position(|column| column.name == *name)
                .and_then(|position| i16::try_from(position + 1).ok())
                .ok_or_else(|| {
                    invalid(
                        state.pgt_id,
                        state.expected.node_ordinal,
                        state.expected.spec_ordinal,
                        format!("index {name} references a missing state column"),
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let expected_options: Vec<_> = keys.iter().map(|(_, _, option)| *option).collect();
    let expected_opclasses = keys
        .iter()
        .map(|(_, type_oid, _)| default_btree_opclass(*type_oid).map(|oid| oid.to_u32()))
        .collect::<Result<Vec<_>, _>>()?;
    let actual_attnums = key_text
        .split_whitespace()
        .map(str::parse::<i16>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| PgTrickleError::InternalError(format!("invalid indkey: {error}")))?;
    let actual_options = option_text
        .split_whitespace()
        .map(str::parse::<i16>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| PgTrickleError::InternalError(format!("invalid indoption: {error}")))?;
    let actual_opclasses = opclass_text
        .split_whitespace()
        .map(str::parse::<u32>)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| PgTrickleError::InternalError(format!("invalid indclass: {error}")))?;
    if !valid
        || !ready
        || actual_unique != unique
        || actual_nulls != nulls_not_distinct
        || access_method != "btree"
        || usize::try_from(key_count).ok() != Some(keys.len())
        || actual_attnums != expected_attnums
        || actual_options != expected_options
        || actual_opclasses != expected_opclasses
        || !simple
    {
        return Err(invalid(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            format!("state index {name} does not match its exact btree contract"),
        ));
    }
    Ok(())
}

fn validate_state_indexes(
    state: &RuntimeNodeState,
    entry: &WindowStateRegistryEntry,
) -> Result<(), PgTrickleError> {
    let mut row_columns = state.target_columns.clone();
    row_columns.push(metadata_column("state_generation"));
    let mut partition_columns = state.partition_columns.clone();
    partition_columns.push(metadata_column("row_count"));
    partition_columns.push(metadata_column("state_generation"));

    let identity_keys: Vec<_> = state
        .identity_columns
        .iter()
        .map(|column| (column.name.clone(), column.type_oid, 0_i16))
        .collect();
    validate_state_index(
        state,
        entry.row_relid,
        &row_columns,
        &state_index_name(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            "identity",
        ),
        &identity_keys,
        true,
        true,
    )?;
    validate_state_index(
        state,
        entry.row_relid,
        &row_columns,
        &state_index_name(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            "delta",
        ),
        &[("__pgt_row_id".into(), pg_sys::BYTEAOID.to_u32(), 0_i16)],
        false,
        false,
    )?;

    let mut seen = HashSet::new();
    let mut order_keys = Vec::new();
    for column in &state.partition_columns {
        seen.insert(column.name.clone());
        order_keys.push((column.name.clone(), column.type_oid, 0_i16));
    }
    for (column, ascending, nulls_first) in &state.order_columns {
        seen.insert(column.name.clone());
        let option =
            (if *ascending { 0_i16 } else { 1_i16 }) | (if *nulls_first { 2_i16 } else { 0_i16 });
        order_keys.push((column.name.clone(), column.type_oid, option));
    }
    for column in &state.identity_columns {
        if seen.insert(column.name.clone()) {
            order_keys.push((column.name.clone(), column.type_oid, 0_i16));
        }
    }
    validate_state_index(
        state,
        entry.row_relid,
        &row_columns,
        &state_index_name(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            "order",
        ),
        &order_keys,
        false,
        false,
    )?;

    let partition_keys = if state.partition_columns.is_empty() {
        vec![("state_generation".into(), pg_sys::INT8OID.to_u32(), 0_i16)]
    } else {
        state
            .partition_columns
            .iter()
            .map(|column| (column.name.clone(), column.type_oid, 0_i16))
            .collect()
    };
    validate_state_index(
        state,
        entry.partition_relid,
        &partition_columns,
        &state_index_name(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            "part",
        ),
        &partition_keys,
        true,
        true,
    )
}

fn validate_runtime_access(pgt_id: i64, states: &[RuntimeNodeState]) -> Result<(), PgTrickleError> {
    let entries = entries_for_stream(pgt_id)?;
    for state in states {
        let entry = entries
            .iter()
            .find(|entry| {
                entry.node_ordinal == state.expected.node_ordinal
                    && entry.spec_ordinal == state.expected.spec_ordinal
            })
            .ok_or_else(|| {
                invalid(
                    pgt_id,
                    state.expected.node_ordinal,
                    state.expected.spec_ordinal,
                    "READY registry row is missing during ACL validation",
                )
            })?;
        validate_select_privilege(state, entry.row_relid, &state.target_owner_name)?;
        validate_select_privilege(state, entry.partition_relid, &state.target_owner_name)?;
        validate_state_columns(state, entry)?;
        validate_state_indexes(state, entry)?;
    }
    Ok(())
}

fn state_relation_name(name: &str) -> String {
    format!(
        "{}.{}",
        crate::api::quote_identifier("pgtrickle"),
        crate::api::quote_identifier(name)
    )
}

fn state_index_name(pgt_id: i64, node_ordinal: i32, spec_ordinal: i32, role: &str) -> String {
    format!("__pgt_w_{pgt_id}_{node_ordinal}_{spec_ordinal}_{role}_idx")
}

fn run_state_sql(state: &RuntimeNodeState, action: &str, sql: &str) -> Result<(), PgTrickleError> {
    // nosemgrep: rust.spi.run.dynamic -- callers assemble SQL only from catalog
    // names escaped with quote_identifier, validated column names, and integers.
    Spi::run(sql).map_err(|error| {
        invalid(
            state.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            format!("could not {action}: {error}"),
        )
    })
}

fn relation_oid(name: &str) -> Result<pg_sys::Oid, PgTrickleError> {
    Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass($1)::oid",
        &[format!("pgtrickle.{name}").into()],
    )
    .map_err(spi_error)?
    .ok_or_else(|| PgTrickleError::InternalError(format!("state relation {name} is missing")))
}

fn next_state_generation(pgt_id: i64) -> Result<i64, PgTrickleError> {
    let current = Spi::get_one_with_args::<i64>(
        "SELECT max(state_generation) FROM pgtrickle.pgt_window_states WHERE pgt_id = $1",
        &[pgt_id.into()],
    )
    .map_err(spi_error)?
    .unwrap_or(0);
    current
        .checked_add(1)
        .filter(|generation| *generation > 0)
        .ok_or_else(|| invalid(pgt_id, -1, -1, "window state generation overflowed"))
}

fn drop_named_state_relation(
    pgt_id: i64,
    node_ordinal: i32,
    spec_ordinal: i32,
    name: &str,
) -> Result<(), PgTrickleError> {
    let oid = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass($1)::oid",
        &[format!("pgtrickle.{name}").into()],
    )
    .map_err(spi_error)?;
    let Some(oid) = oid else {
        return Ok(());
    };
    let facts = relation_facts(oid)?.ok_or_else(|| {
        invalid(
            pgt_id,
            node_ordinal,
            spec_ordinal,
            format!("state relation {name} disappeared during cleanup"),
        )
    })?;
    if facts.schema != "pgtrickle"
        || facts.name != name
        || facts.relkind != "r"
        || facts.persistence != "p"
        || !facts.owner_matches
        || !facts.extension_member
    {
        return Err(invalid(
            pgt_id,
            node_ordinal,
            spec_ordinal,
            format!("refusing to replace unowned state relation {name}"),
        ));
    }
    let qualified = state_relation_name(name);
    // nosemgrep: rust.spi.run.dynamic-format -- the catalog name is checked and quote_identifier-escaped.
    Spi::run(&format!(
        "ALTER EXTENSION pg_trickle DROP TABLE {qualified}"
    ))
    .map_err(spi_error)?;
    // nosemgrep: rust.spi.run.dynamic-format -- the catalog name is checked and quote_identifier-escaped.
    Spi::run(&format!("DROP TABLE {qualified}")).map_err(spi_error)
}

fn build_runtime_state(
    pgt_id: i64,
    state: &RuntimeNodeState,
    generation: i64,
) -> Result<(), PgTrickleError> {
    let partition = state_relation_name(&state.expected.partition_name);
    let rows = state_relation_name(&state.expected.row_name);
    let partition_select = if state.partition_columns.is_empty() {
        format!(
            "SELECT count(*)::bigint AS row_count, NULL::bigint AS state_generation \
             FROM {}",
            state.target
        )
    } else {
        let columns = state
            .partition_columns
            .iter()
            .map(|column| crate::api::quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT {columns}, count(*)::bigint AS row_count, \
                    NULL::bigint AS state_generation \
             FROM {} GROUP BY {columns}",
            state.target
        )
    };

    run_state_sql(
        state,
        "create the row-state relation",
        &format!(
            "CREATE TABLE {rows} AS \
             SELECT source.*, NULL::bigint AS state_generation \
             FROM {} AS source WITH NO DATA",
            state.target
        ),
    )?;
    run_state_sql(
        state,
        "create the partition-state relation",
        &format!("CREATE TABLE {partition} AS {partition_select} WITH NO DATA"),
    )?;

    let identity_names: HashSet<_> = state
        .identity_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    for column in &state.target_columns {
        if column.not_null || identity_names.contains(column.name.as_str()) {
            run_state_sql(
                state,
                "set row-state nullability",
                &format!(
                    "ALTER TABLE {rows} ALTER COLUMN {} SET NOT NULL",
                    crate::api::quote_identifier(&column.name)
                ),
            )?;
        }
    }
    for column in &state.partition_columns {
        if column.not_null {
            run_state_sql(
                state,
                "set partition-state nullability",
                &format!(
                    "ALTER TABLE {partition} ALTER COLUMN {} SET NOT NULL",
                    crate::api::quote_identifier(&column.name)
                ),
            )?;
        }
    }
    run_state_sql(
        state,
        "constrain row-state generation",
        &format!(
            "ALTER TABLE {rows} \
             ALTER COLUMN state_generation SET NOT NULL, \
             ADD CONSTRAINT state_generation_positive CHECK (state_generation > 0)"
        ),
    )?;
    run_state_sql(
        state,
        "constrain partition-state metadata",
        &format!(
            "ALTER TABLE {partition} \
             ALTER COLUMN row_count SET NOT NULL, \
             ALTER COLUMN state_generation SET NOT NULL, \
             ADD CONSTRAINT row_count_nonnegative CHECK (row_count >= 0), \
             ADD CONSTRAINT state_generation_positive CHECK (state_generation > 0)"
        ),
    )?;

    for relation in [&partition, &rows] {
        run_state_sql(
            state,
            "revoke PUBLIC state access",
            &format!("REVOKE ALL ON TABLE {relation} FROM PUBLIC"),
        )?;
        run_state_sql(
            state,
            "grant stream-owner state access",
            &format!(
                "GRANT SELECT ON TABLE {relation} TO {}",
                crate::api::quote_identifier(&state.target_owner_name)
            ),
        )?;
        run_state_sql(
            state,
            "attach a state relation to the extension",
            &format!("ALTER EXTENSION pg_trickle ADD TABLE {relation}"),
        )?;
    }

    let partition_relid = relation_oid(&state.expected.partition_name)?;
    let row_relid = relation_oid(&state.expected.row_name)?;
    validate_select_privilege(state, partition_relid, &state.target_owner_name)?;
    validate_select_privilege(state, row_relid, &state.target_owner_name)?;
    register_building(
        pgt_id,
        state.expected.node_ordinal,
        state.expected.spec_ordinal,
        partition_relid,
        row_relid,
        None,
        state.expected.query_hash,
        generation,
    )?;

    run_state_sql(
        state,
        "populate row state",
        &format!(
            "INSERT INTO {rows} SELECT source.*, {generation}::bigint FROM {} AS source",
            state.target
        ),
    )?;
    let partition_insert = if state.partition_columns.is_empty() {
        format!(
            "INSERT INTO {partition} \
             SELECT count(*)::bigint, {generation}::bigint FROM {}",
            state.target
        )
    } else {
        let columns = state
            .partition_columns
            .iter()
            .map(|column| crate::api::quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "INSERT INTO {partition} \
             SELECT {columns}, count(*)::bigint, {generation}::bigint \
             FROM {} GROUP BY {columns}",
            state.target
        )
    };
    run_state_sql(state, "populate partition state", &partition_insert)?;

    let identity_keys = state
        .identity_columns
        .iter()
        .map(|column| crate::api::quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let identity_index = crate::api::quote_identifier(&state_index_name(
        pgt_id,
        state.expected.node_ordinal,
        state.expected.spec_ordinal,
        "identity",
    ));
    run_state_sql(
        state,
        "create the exact row-identity index",
        &format!(
            "CREATE UNIQUE INDEX {identity_index} ON {rows} USING btree \
             ({identity_keys}) NULLS NOT DISTINCT"
        ),
    )?;
    let delta_row_id_index = crate::api::quote_identifier(&state_index_name(
        pgt_id,
        state.expected.node_ordinal,
        state.expected.spec_ordinal,
        "delta",
    ));
    run_state_sql(
        state,
        "create the delta row-id index",
        &format!("CREATE INDEX {delta_row_id_index} ON {rows} USING btree (__pgt_row_id)"),
    )?;

    let mut seen = HashSet::new();
    let mut order_keys = Vec::new();
    for column in &state.partition_columns {
        seen.insert(column.name.clone());
        order_keys.push(format!(
            "{} ASC NULLS LAST",
            crate::api::quote_identifier(&column.name)
        ));
    }
    for (column, ascending, nulls_first) in &state.order_columns {
        seen.insert(column.name.clone());
        order_keys.push(format!(
            "{} {} NULLS {}",
            crate::api::quote_identifier(&column.name),
            if *ascending { "ASC" } else { "DESC" },
            if *nulls_first { "FIRST" } else { "LAST" }
        ));
    }
    for column in &state.identity_columns {
        if seen.insert(column.name.clone()) {
            order_keys.push(format!(
                "{} ASC NULLS LAST",
                crate::api::quote_identifier(&column.name)
            ));
        }
    }
    let order_index = crate::api::quote_identifier(&state_index_name(
        pgt_id,
        state.expected.node_ordinal,
        state.expected.spec_ordinal,
        "order",
    ));
    run_state_sql(
        state,
        "create the window-order index",
        &format!(
            "CREATE INDEX {order_index} ON {rows} USING btree ({})",
            order_keys.join(", ")
        ),
    )?;

    let partition_index = crate::api::quote_identifier(&state_index_name(
        pgt_id,
        state.expected.node_ordinal,
        state.expected.spec_ordinal,
        "part",
    ));
    let partition_keys = if state.partition_columns.is_empty() {
        "state_generation".to_string()
    } else {
        state
            .partition_columns
            .iter()
            .map(|column| crate::api::quote_identifier(&column.name))
            .collect::<Vec<_>>()
            .join(", ")
    };
    run_state_sql(
        state,
        "create the partition-key index",
        &format!(
            "CREATE UNIQUE INDEX {partition_index} ON {partition} USING btree \
             ({partition_keys}) NULLS NOT DISTINCT"
        ),
    )?;

    mark_ready(pgt_id, &state.expected, generation, 0)
}

fn rebuild_runtime_states(pgt_id: i64, states: &[RuntimeNodeState]) -> Result<(), PgTrickleError> {
    let generation = (!states.is_empty())
        .then(|| next_state_generation(pgt_id))
        .transpose()?;
    drop_for_stream(pgt_id)?;
    for state in states {
        for name in [
            state.expected.partition_name.as_str(),
            state.expected.row_name.as_str(),
        ] {
            drop_named_state_relation(
                pgt_id,
                state.expected.node_ordinal,
                state.expected.spec_ordinal,
                name,
            )?;
        }
        let peer_name = format!(
            "__pgt_window_{}_{}_{}_peers",
            pgt_id, state.expected.node_ordinal, state.expected.spec_ordinal
        );
        drop_named_state_relation(
            pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            &peer_name,
        )?;
    }
    if let Some(generation) = generation {
        for state in states {
            build_runtime_state(pgt_id, state, generation)?;
        }
        let expected: Vec<_> = states.iter().map(|state| state.expected.clone()).collect();
        validate_registry(pgt_id, &expected)?;
        validate_runtime_access(pgt_id, states)?;
    }
    Ok(())
}

fn rebuild_with_budget_fallback(
    pgt_id: i64,
    plan: &WindowStrategyPlan,
    states: &[RuntimeNodeState],
) -> Result<WindowStrategyPlan, PgTrickleError> {
    match rebuild_runtime_states(pgt_id, states) {
        Ok(()) => Ok(plan.clone()),
        Err(error) if is_budget_error(&error) => {
            // The target FULL/delta already succeeded in this transaction.
            // Discard only derived state and retain the deterministic
            // partition-recompute fallback.
            drop_for_stream(pgt_id)?;
            let disabled = disable_runtime_for_budget(plan);
            persist_plan(pgt_id, Some(&disabled))?;
            Ok(disabled)
        }
        Err(error) => Err(error),
    }
}

fn refresh_accounted_bytes(
    pgt_id: i64,
    expected: &[WindowStateExpectation],
) -> Result<(), PgTrickleError> {
    let entries = entries_for_stream(pgt_id)?;
    let mut measured = Vec::with_capacity(entries.len());
    let mut total = 0_u64;
    for entry in &entries {
        let bytes = measured_entry_bytes(entry)?;
        let bytes_u64 = u64::try_from(bytes).map_err(|_| {
            invalid(
                pgt_id,
                entry.node_ordinal,
                entry.spec_ordinal,
                format!("measured state size {bytes} is negative"),
            )
        })?;
        total = total.checked_add(bytes_u64).ok_or_else(|| {
            invalid(
                pgt_id,
                entry.node_ordinal,
                entry.spec_ordinal,
                "window state size accounting overflowed",
            )
        })?;
        measured.push((entry.node_ordinal, entry.spec_ordinal, bytes));
    }
    let allowed = crate::config::pg_trickle_memory_budget().window_state_bytes;
    if total > allowed {
        return Err(budget_invalid(
            pgt_id,
            -1,
            -1,
            format!(
                "measured state size {total} exceeds the {allowed}-byte per-stream-table window-state budget"
            ),
        ));
    }
    for (node_ordinal, spec_ordinal, bytes) in measured {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_window_states \
             SET estimated_bytes = $1, last_validated_at = now(), updated_at = now() \
             WHERE pgt_id = $2 AND node_ordinal = $3 AND spec_ordinal = $4 \
               AND status = 'READY'",
            &[
                bytes.into(),
                pgt_id.into(),
                node_ordinal.into(),
                spec_ordinal.into(),
            ],
        )
        .map_err(spi_error)?;
    }
    validate_registry(pgt_id, expected)
}

/// Synchronize READY ROW_NUMBER side state from an already-applied target
/// delta. The caller owns the surrounding refresh transaction.
pub(crate) fn sync_after_differential(
    st: &StreamTableMeta,
    plan: &WindowStrategyPlan,
    delta_table: &str,
) -> Result<(), PgTrickleError> {
    let expected_delta = format!(
        "pg_temp.{}",
        crate::sql_builder::ident(&format!("__pgt_delta_{}", st.pgt_id))
    );
    if delta_table != expected_delta {
        return Err(invalid(
            st.pgt_id,
            -1,
            -1,
            format!("delta relation is {delta_table:?}, expected {expected_delta:?}"),
        ));
    }
    let states = runtime_states_for_plan(st, plan)?;
    let expected: Vec<_> = states.iter().map(|state| state.expected.clone()).collect();
    validate_registry(st.pgt_id, &expected)?;
    validate_runtime_access(st.pgt_id, &states)?;
    // nosemgrep: rust.spi.query.dynamic-format -- delta_table is validated against the numeric-ID-derived expected_delta above.
    let has_delta = Spi::get_one::<bool>(&format!("SELECT EXISTS (SELECT 1 FROM {delta_table})"))
        .map_err(spi_error)?
        .unwrap_or(false);
    if !has_delta {
        return Ok(());
    }

    let entries = entries_for_stream(st.pgt_id)?;
    let current_generation = entries
        .first()
        .map(|entry| entry.state_generation)
        .ok_or_else(|| invalid(st.pgt_id, -1, -1, "READY registry row disappeared"))?;
    let next_generation = next_state_generation(st.pgt_id)?;
    for state in &states {
        if !entries.iter().any(|entry| {
            entry.node_ordinal == state.expected.node_ordinal
                && entry.spec_ordinal == state.expected.spec_ordinal
        }) {
            return Err(invalid(
                st.pgt_id,
                state.expected.node_ordinal,
                state.expected.spec_ordinal,
                "READY registry row disappeared during state synchronization",
            ));
        }
        let rows = state_relation_name(&state.expected.row_name);
        let partition = state_relation_name(&state.expected.partition_name);
        let affected = format!(
            "pg_temp.{}",
            crate::api::quote_identifier(&format!(
                "__pgt_w_aff_{}_{}_{}",
                st.pgt_id, state.expected.node_ordinal, state.expected.spec_ordinal
            ))
        );

        if !state.partition_columns.is_empty() {
            run_state_sql(
                state,
                "reset the affected-partition staging table",
                &format!("DROP TABLE IF EXISTS {affected}"),
            )?;
            let old_keys = state
                .partition_columns
                .iter()
                .map(|column| format!("old.{}", crate::api::quote_identifier(&column.name)))
                .collect::<Vec<_>>()
                .join(", ");
            let new_keys = state
                .partition_columns
                .iter()
                .map(|column| format!("new.{}", crate::api::quote_identifier(&column.name)))
                .collect::<Vec<_>>()
                .join(", ");
            run_state_sql(
                state,
                "capture affected window partitions",
                &format!(
                    "CREATE TEMP TABLE {affected} ON COMMIT DROP AS \
                     SELECT {old_keys} FROM {rows} old \
                     WHERE EXISTS (SELECT 1 FROM {delta_table} d \
                                   WHERE d.__pgt_row_id = old.__pgt_row_id) \
                     UNION \
                     SELECT {new_keys} FROM {} new \
                     WHERE EXISTS (SELECT 1 FROM {delta_table} d \
                                   WHERE d.__pgt_row_id = new.__pgt_row_id)",
                    state.target
                ),
            )?;
        }

        run_state_sql(
            state,
            "remove changed rows from window state",
            &format!(
                "DELETE FROM {rows} old USING \
                 (SELECT DISTINCT __pgt_row_id FROM {delta_table}) d \
                 WHERE old.__pgt_row_id = d.__pgt_row_id"
            ),
        )?;
        run_state_sql(
            state,
            "copy changed target rows into window state",
            &format!(
                "INSERT INTO {rows} \
                 SELECT target.*, {}::bigint FROM {} target \
                 WHERE EXISTS (SELECT 1 FROM {delta_table} d \
                               WHERE d.__pgt_row_id = target.__pgt_row_id)",
                next_generation, state.target
            ),
        )?;

        if state.partition_columns.is_empty() {
            run_state_sql(
                state,
                "refresh singleton window partition",
                &format!(
                    "TRUNCATE TABLE {partition}; \
                     INSERT INTO {partition} \
                     SELECT count(*)::bigint, {}::bigint FROM {}",
                    next_generation, state.target
                ),
            )?;
        } else {
            let columns = state
                .partition_columns
                .iter()
                .map(|column| crate::api::quote_identifier(&column.name))
                .collect::<Vec<_>>()
                .join(", ");
            let target_columns = state
                .partition_columns
                .iter()
                .map(|column| format!("target.{}", crate::api::quote_identifier(&column.name)))
                .collect::<Vec<_>>()
                .join(", ");
            let partition_match = state
                .partition_columns
                .iter()
                .map(|column| {
                    let column = crate::api::quote_identifier(&column.name);
                    format!("part.{column} IS NOT DISTINCT FROM affected.{column}")
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            let target_match = state
                .partition_columns
                .iter()
                .map(|column| {
                    let column = crate::api::quote_identifier(&column.name);
                    format!("target.{column} IS NOT DISTINCT FROM affected.{column}")
                })
                .collect::<Vec<_>>()
                .join(" AND ");
            run_state_sql(
                state,
                "remove stale window partition counts",
                &format!(
                    "DELETE FROM {partition} part USING {affected} affected \
                     WHERE {partition_match}"
                ),
            )?;
            run_state_sql(
                state,
                "refresh affected window partition counts",
                &format!(
                    "INSERT INTO {partition} ({columns}, row_count, state_generation) \
                     SELECT {target_columns}, count(*)::bigint, {}::bigint \
                     FROM {} target JOIN {affected} affected ON {target_match} \
                     GROUP BY {target_columns}",
                    next_generation, state.target
                ),
            )?;
        }
        run_state_sql(
            state,
            "advance row-state generation",
            &format!("UPDATE {rows} SET state_generation = {next_generation}"),
        )?;
        run_state_sql(
            state,
            "advance partition-state generation",
            &format!("UPDATE {partition} SET state_generation = {next_generation}"),
        )?;
    }
    let advanced = Spi::get_one_with_args::<i64>(
        "WITH updated AS ( \
             UPDATE pgtrickle.pgt_window_states \
             SET state_generation = $1, updated_at = now() \
             WHERE pgt_id = $2 AND state_generation = $3 AND status = 'READY' \
             RETURNING 1 \
         ) SELECT count(*)::bigint FROM updated",
        &[
            next_generation.into(),
            st.pgt_id.into(),
            current_generation.into(),
        ],
    )
    .map_err(spi_error)?
    .unwrap_or(0);
    if advanced != states.len() as i64 {
        return Err(invalid(
            st.pgt_id,
            -1,
            -1,
            format!(
                "advanced {advanced} READY registry rows, expected {}",
                states.len()
            ),
        ));
    }
    match refresh_accounted_bytes(st.pgt_id, &expected) {
        Ok(()) => Ok(()),
        Err(error) if is_budget_error(&error) => {
            drop_for_stream(st.pgt_id)?;
            let disabled = disable_runtime_for_budget(plan);
            persist_plan(st.pgt_id, Some(&disabled))
        }
        Err(error) => Err(error),
    }
}

/// Return the validated READY state used by the benchmark-only ROW_NUMBER
/// candidate. Production plans keep runtime state disabled.
pub(crate) fn ready_row_number_state(
    st: &StreamTableMeta,
    plan: &WindowStrategyPlan,
) -> Result<Option<ReadyRowNumberState>, PgTrickleError> {
    let states = runtime_states_for_plan(st, plan)?;
    if states.is_empty() {
        return Ok(None);
    }
    if states.len() != 1 {
        return Err(invalid(
            st.pgt_id,
            -1,
            -1,
            format!(
                "ROW_NUMBER runtime expects one state node, found {}",
                states.len()
            ),
        ));
    }
    let state = &states[0];
    let expected = [state.expected.clone()];
    validate_registry(st.pgt_id, &expected)?;
    validate_runtime_access(st.pgt_id, &states)?;
    let entries = entries_for_stream(st.pgt_id)?;
    entries.first().ok_or_else(|| {
        invalid(
            st.pgt_id,
            state.expected.node_ordinal,
            state.expected.spec_ordinal,
            "READY registry row disappeared during cost lookup",
        )
    })?;
    Ok(Some(ReadyRowNumberState {
        row_relation: state_relation_name(&state.expected.row_name),
    }))
}

/// Rebuild runtime-enabled state after a FULL or protected reinitialization.
/// Recompute-only plans remove stale derived objects and create no side state.
pub(crate) fn prepare_for_protected_refresh(
    st: &StreamTableMeta,
) -> Result<Option<WindowStrategyPlan>, PgTrickleError> {
    let query_hash = current_query_hash(st);
    // The caller's metadata can predate lazy planning or an in-transaction
    // budget fallback. Read the authoritative catalog value, but never make a
    // configured FULL depend on differential query analysis.
    let mut plan = StreamTableMeta::get_by_id(st.pgt_id)?
        .ok_or_else(|| PgTrickleError::NotFound(format!("pgt_id={}", st.pgt_id)))?
        .window_strategy;
    if let Some(plan) = &plan
        && plan.query_hash != query_hash
    {
        return Err(invalid(
            st.pgt_id,
            -1,
            -1,
            format!(
                "protected-refresh strategy query hash is {}, expected {query_hash}",
                plan.query_hash
            ),
        ));
    }
    if plan.is_none() && st.refresh_mode != crate::dag::RefreshMode::Full {
        plan = Some(analyze_and_persist_plan(st, query_hash)?);
    }
    // Validate every referenced target column before destructive cleanup. A
    // runtime-enabled plan outside the deliberately narrow ROW_NUMBER contract
    // leaves the previous generation intact and aborts the surrounding FULL.
    let states = plan
        .as_ref()
        .map(|plan| runtime_states_for_plan(st, plan))
        .transpose()?
        .unwrap_or_default();
    if let Some(current) = &plan {
        plan = Some(rebuild_with_budget_fallback(st.pgt_id, current, &states)?);
    } else {
        rebuild_runtime_states(st.pgt_id, &states)?;
    }
    Ok(plan)
}

pub(crate) fn entries_for_stream(
    pgt_id: i64,
) -> Result<Vec<WindowStateRegistryEntry>, PgTrickleError> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT pgt_id, node_ordinal, spec_ordinal, partition_relid, row_relid, \
                        peer_relid, schema_version, strategy_version, query_hash, \
                        state_generation, status, estimated_bytes, last_validated_at, updated_at \
                 FROM pgtrickle.pgt_window_states \
                 WHERE pgt_id = $1 ORDER BY node_ordinal, spec_ordinal",
                None,
                &[pgt_id.into()],
            )
            .map_err(spi_error)?;
        let mut entries = Vec::with_capacity(rows.len());
        for row in rows {
            let node_ordinal = row
                .get::<i32>(2)
                .map_err(spi_error)?
                .ok_or_else(|| PgTrickleError::InternalError("node_ordinal is NULL".into()))?;
            let spec_ordinal = row
                .get::<i32>(3)
                .map_err(spi_error)?
                .ok_or_else(|| PgTrickleError::InternalError("spec_ordinal is NULL".into()))?;
            let status_text = row.get::<String>(11).map_err(spi_error)?.ok_or_else(|| {
                PgTrickleError::InternalError("window state status is NULL".into())
            })?;
            let status = WindowStateStatus::parse(&status_text).ok_or_else(|| {
                invalid(
                    pgt_id,
                    node_ordinal,
                    spec_ordinal,
                    format!("unknown registry status {status_text}"),
                )
            })?;
            entries.push(WindowStateRegistryEntry {
                pgt_id: row
                    .get::<i64>(1)
                    .map_err(spi_error)?
                    .ok_or_else(|| PgTrickleError::InternalError("pgt_id is NULL".into()))?,
                node_ordinal,
                spec_ordinal,
                partition_relid: row.get::<pg_sys::Oid>(4).map_err(spi_error)?.ok_or_else(
                    || PgTrickleError::InternalError("partition_relid is NULL".into()),
                )?,
                row_relid: row
                    .get::<pg_sys::Oid>(5)
                    .map_err(spi_error)?
                    .ok_or_else(|| PgTrickleError::InternalError("row_relid is NULL".into()))?,
                peer_relid: row.get::<pg_sys::Oid>(6).map_err(spi_error)?,
                schema_version: row.get::<i16>(7).map_err(spi_error)?.ok_or_else(|| {
                    PgTrickleError::InternalError("schema_version is NULL".into())
                })?,
                strategy_version: row.get::<i16>(8).map_err(spi_error)?.ok_or_else(|| {
                    PgTrickleError::InternalError("strategy_version is NULL".into())
                })?,
                query_hash: row
                    .get::<i64>(9)
                    .map_err(spi_error)?
                    .ok_or_else(|| PgTrickleError::InternalError("query_hash is NULL".into()))?,
                state_generation: row.get::<i64>(10).map_err(spi_error)?.ok_or_else(|| {
                    PgTrickleError::InternalError("state_generation is NULL".into())
                })?,
                status,
                estimated_bytes: row.get::<i64>(12).map_err(spi_error)?.ok_or_else(|| {
                    PgTrickleError::InternalError("estimated_bytes is NULL".into())
                })?,
                last_validated_at: row.get::<TimestampWithTimeZone>(13).map_err(spi_error)?,
                updated_at: row
                    .get::<TimestampWithTimeZone>(14)
                    .map_err(spi_error)?
                    .ok_or_else(|| PgTrickleError::InternalError("updated_at is NULL".into()))?,
            });
        }
        Ok(entries)
    })
}

pub(crate) fn persist_plan(
    pgt_id: i64,
    plan: Option<&WindowStrategyPlan>,
) -> Result<(), PgTrickleError> {
    let Some(plan) = plan else {
        let updated = Spi::get_one_with_args::<bool>(
            "UPDATE pgtrickle.pgt_stream_tables \
             SET window_strategy = NULL, updated_at = now() WHERE pgt_id = $1 \
             RETURNING true",
            &[pgt_id.into()],
        )
        .map_err(spi_error)?;
        return if updated == Some(true) {
            Ok(())
        } else {
            Err(PgTrickleError::NotFound(format!("pgt_id={pgt_id}")))
        };
    };
    let json = plan
        .to_json()
        .map_err(|reason| invalid(pgt_id, -1, -1, reason))?;
    let updated = Spi::get_one_with_args::<bool>(
        "WITH updated AS ( \
             UPDATE pgtrickle.pgt_stream_tables \
             SET window_strategy = $1, defining_query_hash = $3, updated_at = now() \
             WHERE pgt_id = $2 AND defining_query_hash IN (0, $3) \
             RETURNING 1 \
         ) SELECT EXISTS (SELECT 1 FROM updated)",
        &[
            pgrx::JsonB(json).into(),
            pgt_id.into(),
            plan.query_hash.into(),
        ],
    )
    .map_err(spi_error)?;
    if updated == Some(true) {
        Ok(())
    } else {
        Err(invalid(
            pgt_id,
            -1,
            -1,
            "stream table is missing or its defining-query hash changed",
        ))
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn register_building(
    pgt_id: i64,
    node_ordinal: i32,
    spec_ordinal: i32,
    partition_relid: pg_sys::Oid,
    row_relid: pg_sys::Oid,
    peer_relid: Option<pg_sys::Oid>,
    query_hash: i64,
    state_generation: i64,
) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_window_states \
         (pgt_id, node_ordinal, spec_ordinal, partition_relid, row_relid, peer_relid, \
          schema_version, strategy_version, query_hash, state_generation, status) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'BUILDING') \
         ON CONFLICT (pgt_id, node_ordinal, spec_ordinal) DO UPDATE SET \
           partition_relid = EXCLUDED.partition_relid, row_relid = EXCLUDED.row_relid, \
           peer_relid = EXCLUDED.peer_relid, schema_version = EXCLUDED.schema_version, \
           strategy_version = EXCLUDED.strategy_version, query_hash = EXCLUDED.query_hash, \
           state_generation = EXCLUDED.state_generation, status = 'BUILDING', \
           estimated_bytes = 0, last_validated_at = NULL, updated_at = now()",
        &[
            pgt_id.into(),
            node_ordinal.into(),
            spec_ordinal.into(),
            partition_relid.into(),
            row_relid.into(),
            peer_relid.into(),
            WINDOW_STATE_SCHEMA_VERSION.into(),
            WINDOW_STATE_STRATEGY_VERSION.into(),
            query_hash.into(),
            state_generation.into(),
        ],
    )
    .map_err(spi_error)
}

pub(crate) fn mark_ready(
    pgt_id: i64,
    expected: &WindowStateExpectation,
    state_generation: i64,
    estimated_bytes: i64,
) -> Result<(), PgTrickleError> {
    let allowed_bytes = crate::config::pg_trickle_memory_budget().window_state_bytes;
    if estimated_bytes < 0 {
        return Err(invalid(
            pgt_id,
            expected.node_ordinal,
            expected.spec_ordinal,
            format!("estimated state size {estimated_bytes} is negative"),
        ));
    }
    let entries = entries_for_stream(pgt_id)?;
    let entry = entries
        .iter()
        .find(|entry| {
            entry.node_ordinal == expected.node_ordinal
                && entry.spec_ordinal == expected.spec_ordinal
        })
        .ok_or_else(|| {
            invalid(
                pgt_id,
                expected.node_ordinal,
                expected.spec_ordinal,
                "BUILDING registry row is missing",
            )
        })?;
    let mut ready_entry = entry.clone();
    ready_entry.status = WindowStateStatus::Ready;
    validate_entry_metadata(&ready_entry, expected)
        .map_err(|reason| invalid(pgt_id, expected.node_ordinal, expected.spec_ordinal, reason))?;
    if entry.status != WindowStateStatus::Building || entry.state_generation != state_generation {
        return Err(invalid(
            pgt_id,
            expected.node_ordinal,
            expected.spec_ordinal,
            "BUILDING registry row with the expected generation was not found",
        ));
    }
    validate_relation(entry, entry.partition_relid, &expected.partition_name)?;
    validate_relation(entry, entry.row_relid, &expected.row_name)?;
    if let (Some(peer_relid), Some(peer_name)) = (entry.peer_relid, &expected.peer_name) {
        validate_relation(entry, peer_relid, peer_name)?;
    }

    // The analyzed estimate is an admission guard; the physical size is the
    // durable accounting value once all relations and indexes exist. Keep the
    // larger value so a low estimate can never admit state over the ceiling.
    let measured_bytes = measured_entry_bytes(entry)?;
    let accounted_bytes = estimated_bytes.max(measured_bytes);
    let projected_bytes = projected_registry_bytes(
        &entries,
        expected.node_ordinal,
        expected.spec_ordinal,
        accounted_bytes,
    )
    .map_err(|reason| invalid(pgt_id, expected.node_ordinal, expected.spec_ordinal, reason))?;
    if projected_bytes > allowed_bytes {
        return Err(budget_invalid(
            pgt_id,
            expected.node_ordinal,
            expected.spec_ordinal,
            format!(
                "projected state size {projected_bytes} exceeds the {allowed_bytes}-byte per-stream-table window-state budget"
            ),
        ));
    }

    let updated = Spi::get_one_with_args::<bool>(
        "UPDATE pgtrickle.pgt_window_states \
         SET status = 'READY', estimated_bytes = $1, last_validated_at = now(), updated_at = now() \
         WHERE pgt_id = $2 AND node_ordinal = $3 AND spec_ordinal = $4 \
           AND state_generation = $5 AND status = 'BUILDING' \
         RETURNING true",
        &[
            accounted_bytes.into(),
            pgt_id.into(),
            expected.node_ordinal.into(),
            expected.spec_ordinal.into(),
            state_generation.into(),
        ],
    )
    .map_err(spi_error)?;
    if updated == Some(true) {
        Ok(())
    } else {
        Err(invalid(
            pgt_id,
            expected.node_ordinal,
            expected.spec_ordinal,
            "BUILDING registry row with the expected generation was not found",
        ))
    }
}

pub(crate) fn mark_status(
    pgt_id: i64,
    node_ordinal: i32,
    spec_ordinal: i32,
    status: WindowStateStatus,
) -> Result<(), PgTrickleError> {
    if matches!(
        status,
        WindowStateStatus::Building | WindowStateStatus::Ready
    ) {
        return Err(invalid(
            pgt_id,
            node_ordinal,
            spec_ordinal,
            "BUILDING and READY transitions require their validated lifecycle helpers",
        ));
    }
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_window_states \
         SET status = $1, updated_at = now() \
         WHERE pgt_id = $2 AND node_ordinal = $3 AND spec_ordinal = $4",
        &[
            status.as_str().into(),
            pgt_id.into(),
            node_ordinal.into(),
            spec_ordinal.into(),
        ],
    )
    .map_err(spi_error)
}

pub(crate) fn validate_registry(
    pgt_id: i64,
    expected: &[WindowStateExpectation],
) -> Result<(), PgTrickleError> {
    let entries = entries_for_stream(pgt_id)?;
    if entries.len() != expected.len() {
        return Err(invalid(
            pgt_id,
            -1,
            -1,
            format!(
                "registry has {} entries, strategy expects {}",
                entries.len(),
                expected.len()
            ),
        ));
    }
    validate_registry_invariants(
        &entries,
        crate::config::pg_trickle_memory_budget().window_state_bytes,
    )
    .map_err(|reason| invalid(pgt_id, -1, -1, reason))?;
    for expectation in expected {
        let entry = entries
            .iter()
            .find(|entry| {
                entry.node_ordinal == expectation.node_ordinal
                    && entry.spec_ordinal == expectation.spec_ordinal
            })
            .ok_or_else(|| {
                invalid(
                    pgt_id,
                    expectation.node_ordinal,
                    expectation.spec_ordinal,
                    "expected registry row is missing",
                )
            })?;
        validate_entry_metadata(entry, expectation).map_err(|reason| {
            invalid(
                pgt_id,
                expectation.node_ordinal,
                expectation.spec_ordinal,
                reason,
            )
        })?;
        validate_relation(entry, entry.partition_relid, &expectation.partition_name)?;
        validate_relation(entry, entry.row_relid, &expectation.row_name)?;
        if let (Some(peer_relid), Some(peer_name)) = (entry.peer_relid, &expectation.peer_name) {
            validate_relation(entry, peer_relid, peer_name)?;
        }
    }
    Ok(())
}

pub(crate) fn mark_for_reinitialization(pgt_id: i64, detail: &str) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_window_states \
         SET status = 'STALE', updated_at = now() \
         WHERE pgt_id = $1 AND status <> 'STALE'",
        &[pgt_id.into()],
    )
    .map_err(spi_error)?;
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET needs_reinit = true, refresh_reason = 'WINDOW_STATE_MISMATCH', \
             refresh_reason_detail = $1, updated_at = now() \
         WHERE pgt_id = $2",
        &[detail.into(), pgt_id.into()],
    )
    .map_err(spi_error)
}

pub(crate) fn drop_for_stream(pgt_id: i64) -> Result<(), PgTrickleError> {
    let entries = entries_for_stream(pgt_id)?;
    let mut relations = HashMap::new();
    for entry in &entries {
        relations
            .entry(entry.partition_relid)
            .or_insert((entry.node_ordinal, entry.spec_ordinal));
        relations
            .entry(entry.row_relid)
            .or_insert((entry.node_ordinal, entry.spec_ordinal));
        if let Some(peer_relid) = entry.peer_relid {
            relations
                .entry(peer_relid)
                .or_insert((entry.node_ordinal, entry.spec_ordinal));
        }
    }

    for (oid, (node_ordinal, spec_ordinal)) in relations {
        // A missing relation is already gone. Continue cleanup so a corrupt
        // registry cannot make DROP or protected reinitialization impossible.
        let Some(facts) = relation_facts(oid)? else {
            continue;
        };
        if facts.schema != "pgtrickle"
            || facts.relkind != "r"
            || facts.persistence != "p"
            || !facts.owner_matches
            || !facts.extension_member
        {
            return Err(invalid(
                pgt_id,
                node_ordinal,
                spec_ordinal,
                format!(
                    "refusing to drop unowned state relation OID {}",
                    oid.to_u32()
                ),
            ));
        }
        let qualified = format!(
            "{}.{}",
            crate::api::quote_identifier(&facts.schema),
            crate::api::quote_identifier(&facts.name)
        );
        // nosemgrep: rust.spi.run.dynamic-format -- qualified is built only from quote_identifier-escaped catalog identifiers.
        Spi::run(&format!(
            "ALTER EXTENSION pg_trickle DROP TABLE {qualified}"
        ))
        .map_err(spi_error)?;
        Spi::run(&format!("DROP TABLE {qualified}")) // nosemgrep: rust.spi.run.dynamic-format -- qualified is built only from quote_identifier-escaped catalog identifiers.
            .map_err(spi_error)?;
    }

    Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_window_states WHERE pgt_id = $1",
        &[pgt_id.into()],
    )
    .map_err(spi_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(status: WindowStateStatus) -> WindowStateRegistryEntry {
        WindowStateRegistryEntry {
            pgt_id: 42,
            node_ordinal: 1,
            spec_ordinal: 2,
            partition_relid: pg_sys::Oid::from(100_u32),
            row_relid: pg_sys::Oid::from(101_u32),
            peer_relid: None,
            schema_version: WINDOW_STATE_SCHEMA_VERSION,
            strategy_version: WINDOW_STATE_STRATEGY_VERSION,
            query_hash: 9,
            state_generation: 1,
            status,
            estimated_bytes: 0,
            last_validated_at: None,
            updated_at: TimestampWithTimeZone::try_from(0_i64).expect("valid timestamp"),
        }
    }

    #[test]
    fn test_registry_metadata_fails_closed() {
        let expected = WindowStateExpectation::current(1, 2, "__pgt_window_42_1_2", false, 9);
        assert!(validate_entry_metadata(&entry(WindowStateStatus::Ready), &expected).is_ok());
        assert_eq!(
            validate_entry_metadata(&entry(WindowStateStatus::Building), &expected),
            Err("registry status is BUILDING".into())
        );

        let mut wrong_hash = entry(WindowStateStatus::Ready);
        wrong_hash.query_hash = 10;
        assert_eq!(
            validate_entry_metadata(&wrong_hash, &expected),
            Err("query hash is 10, expected 9".into())
        );
    }

    #[test]
    fn test_state_names_are_deterministic() {
        let expected = WindowStateExpectation::current(1, 2, "__pgt_window_42_1_2", true, 9);
        assert_eq!(expected.partition_name, "__pgt_window_42_1_2_partitions");
        assert_eq!(expected.row_name, "__pgt_window_42_1_2_rows");
        assert_eq!(
            expected.peer_name.as_deref(),
            Some("__pgt_window_42_1_2_peers")
        );

        let qualified =
            WindowStateExpectation::current(1, 2, "pgtrickle.__pgt_window_42_1_2", true, 9);
        assert_eq!(qualified.partition_name, expected.partition_name);
    }

    #[test]
    fn test_simple_column_name_rejects_expressions_and_qualification() {
        assert_eq!(simple_column_name("dept"), Some("dept".into()));
        assert_eq!(simple_column_name(r#""Odd Name""#), Some("Odd Name".into()));
        assert_eq!(simple_column_name(r#""a""b""#), Some("a\"b".into()));
        assert_eq!(simple_column_name("source.dept"), None);
        assert_eq!(simple_column_name("lower(dept)"), None);
    }

    #[test]
    fn test_registry_invariants_enforce_shared_generation_and_unique_relations() {
        let first = entry(WindowStateStatus::Ready);
        let mut second = first.clone();
        second.node_ordinal = 3;
        second.spec_ordinal = 4;
        second.partition_relid = pg_sys::Oid::from(102_u32);
        second.row_relid = pg_sys::Oid::from(103_u32);
        assert!(validate_registry_invariants(&[first.clone(), second.clone()], 1).is_ok());

        second.state_generation = 2;
        assert!(
            validate_registry_invariants(&[first.clone(), second.clone()], 1)
                .expect_err("mixed generations must fail")
                .contains("shared generation")
        );

        second.state_generation = first.state_generation;
        second.partition_relid = first.row_relid;
        assert!(
            validate_registry_invariants(&[first, second], 1)
                .expect_err("reused relation OIDs must fail")
                .contains("is reused")
        );
    }

    #[test]
    fn test_projected_registry_bytes_enforces_per_stream_total() {
        let mut first = entry(WindowStateStatus::Building);
        first.estimated_bytes = 40;
        let mut second = first.clone();
        second.node_ordinal = 3;
        second.spec_ordinal = 4;
        second.partition_relid = pg_sys::Oid::from(102_u32);
        second.row_relid = pg_sys::Oid::from(103_u32);
        second.estimated_bytes = 50;

        assert_eq!(
            projected_registry_bytes(&[first, second], 1, 2, 60),
            Ok(110)
        );
    }
}
