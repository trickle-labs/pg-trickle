//! Pure semantic planning for incremental window maintenance.

use super::*;
use crate::refresh::RefreshReasonCode;

/// Fully analyzed input for one raw window function. The PostgreSQL-facing
/// collector builds these records; the strategy decision below is pure.
#[derive(Debug, Clone)]
pub(crate) struct WindowPlanningInput {
    pub signature: String,
    pub function_oid: u32,
    pub kind: WindowFunctionKind,
    pub argument_types: Vec<WindowExpressionType>,
    pub result_type_oid: u32,
    pub result_type_sql: String,
    pub result_typmod: i32,
    pub result_collation_oid: u32,
    pub partition: Vec<WindowExpressionType>,
    pub order: Vec<WindowOrderKey>,
    pub frame: WindowFrameSpec,
    pub filter: Option<String>,
    pub parse_location: Option<i32>,
}

impl WindowPlanningInput {
    pub(crate) fn unresolved(window: &WindowExpr, frame: WindowFrameSpec) -> Self {
        Self {
            signature: window_signature(window),
            function_oid: 0,
            kind: WindowFunctionKind::Unsupported,
            argument_types: window
                .args
                .iter()
                .map(|expr| unresolved_expression(expr.to_sql()))
                .collect(),
            result_type_oid: 0,
            result_type_sql: String::new(),
            result_typmod: -1,
            result_collation_oid: 0,
            partition: window
                .partition_by
                .iter()
                .map(|expr| unresolved_expression(expr.to_sql()))
                .collect(),
            order: window
                .order_by
                .iter()
                .map(|sort| WindowOrderKey {
                    expression: sort.expr.to_sql(),
                    type_oid: 0,
                    typmod: -1,
                    collation_oid: 0,
                    ascending: sort.ascending,
                    nulls_first: sort.nulls_first,
                    sort_operator_oid: 0,
                    equality_operator_oid: 0,
                })
                .collect(),
            frame,
            filter: None,
            parse_location: None,
        }
    }
}

fn unresolved_expression(expression: String) -> WindowExpressionType {
    WindowExpressionType {
        expression,
        type_oid: 0,
        typmod: -1,
        collation_oid: 0,
    }
}

pub(crate) fn window_signature(window: &WindowExpr) -> String {
    let partition = window
        .partition_by
        .iter()
        .map(Expr::to_sql)
        .collect::<Vec<_>>()
        .join(",");
    let order = window
        .order_by
        .iter()
        .map(|sort| {
            format!(
                "{}:{}:{}",
                sort.expr.to_sql(),
                sort.ascending,
                sort.nulls_first
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{}({})|{partition}|{order}|{}|{}",
        window.func_name,
        window
            .args
            .iter()
            .map(Expr::to_sql)
            .collect::<Vec<_>>()
            .join(","),
        window.frame_clause.as_deref().unwrap_or_default(),
        window.alias
    )
}

/// Classify only functions resolved by PostgreSQL into `pg_catalog`.
pub(crate) fn classify_window_function(namespace: &str, name: &str) -> WindowFunctionKind {
    if namespace != "pg_catalog" {
        return WindowFunctionKind::Unsupported;
    }
    match name {
        "row_number" => WindowFunctionKind::RowNumber,
        "rank" => WindowFunctionKind::Rank,
        "dense_rank" => WindowFunctionKind::DenseRank,
        "lag" => WindowFunctionKind::Lag,
        "lead" => WindowFunctionKind::Lead,
        "first_value" => WindowFunctionKind::FirstValue,
        "last_value" => WindowFunctionKind::LastValue,
        "nth_value" => WindowFunctionKind::NthValue,
        "sum" => WindowFunctionKind::Sum,
        "count" => WindowFunctionKind::Count,
        _ => WindowFunctionKind::Unsupported,
    }
}

/// Build the deterministic v0.89 semantic strategy. Missing analyzed records
/// remain present as explicit partition-recompute decisions.
pub(crate) fn build_window_strategy_plan(
    tree: &OpTree,
    cte_registry: &CteRegistry,
    inputs: &[WindowPlanningInput],
) -> Option<WindowStrategyPlan> {
    let mut inputs = inputs.to_vec();
    let mut nodes = Vec::new();
    let mut next_node_ordinal = 0;
    collect_window_plans(tree, &mut inputs, &mut next_node_ordinal, &mut nodes);
    for (_, cte) in &cte_registry.entries {
        collect_window_plans(cte, &mut inputs, &mut next_node_ordinal, &mut nodes);
    }
    if nodes.is_empty() {
        return None;
    }

    let semantic_fingerprint = semantic_plan_fingerprint(&nodes);
    Some(WindowStrategyPlan {
        schema_version: WINDOW_STRATEGY_SCHEMA_VERSION,
        strategy_version: WINDOW_STRATEGY_VERSION,
        query_hash: 0,
        identity_version: WINDOW_IDENTITY_VERSION,
        semantic_fingerprint,
        nodes,
    })
}

/// Hash only semantic plan fields. Output aliases, parser locations, state
/// relation names, and runtime admission are deliberately excluded: none of
/// them changes the window specification whose state this fingerprint names.
fn semantic_plan_fingerprint(nodes: &[WindowStrategyNode]) -> String {
    let mut canonical = format!("window-strategy-v{WINDOW_STRATEGY_VERSION}");
    for node in nodes {
        canonical.push_str(&format!(
            "|node={}:{}|spec={:?}|identity={:?}",
            node.node_ordinal, node.spec_ordinal, node.spec, node.child_identity
        ));
        for function in &node.functions {
            canonical.push_str(&format!(
                "|function={}:{:?}",
                function.function_ordinal,
                (
                    function.function_oid,
                    function.kind,
                    &function.argument_types,
                    function.result_type_oid,
                    &function.result_type_sql,
                    function.result_typmod,
                    function.result_collation_oid,
                    &function.frame,
                    function.constant_offset,
                    &function.filter,
                    function.strategy,
                    function.eligible,
                )
            ));
        }
    }
    format!("{:016x}", xxhash_rust::xxh3::xxh3_64(canonical.as_bytes()))
}

fn collect_window_plans(
    tree: &OpTree,
    inputs: &mut Vec<WindowPlanningInput>,
    next_node_ordinal: &mut u32,
    nodes: &mut Vec<WindowStrategyNode>,
) {
    match tree {
        OpTree::Window {
            window_exprs,
            child,
            ..
        } => {
            let node_ordinal = *next_node_ordinal;
            *next_node_ordinal += 1;
            let child_identity = exact_window_identity_columns(child);
            let child_semantic_identity = semantic_identity(child);
            let mut specs: Vec<WindowStrategyNode> = Vec::new();

            for (function_ordinal, window) in window_exprs.iter().enumerate() {
                let signature = window_signature(window);
                let metadata = inputs
                    .iter()
                    .position(|input| input.signature == signature)
                    .map(|position| inputs.remove(position))
                    .unwrap_or_else(|| {
                        WindowPlanningInput::unresolved(window, WindowFrameSpec::postgres_default())
                    });
                let frame_key = metadata
                    .kind
                    .observes_frame()
                    .then(|| metadata.frame.clone());
                let key = WindowSpecKey {
                    partition: metadata.partition.clone(),
                    order: metadata.order.clone(),
                    frame: frame_key,
                    child_semantic_identity: child_semantic_identity.clone(),
                };
                let function = plan_function(
                    window,
                    metadata,
                    function_ordinal as u32,
                    child_identity.as_deref(),
                );

                if let Some(spec) = specs.iter_mut().find(|spec| spec.spec == key) {
                    spec.functions.push(function);
                } else {
                    specs.push(WindowStrategyNode {
                        node_ordinal,
                        spec_ordinal: specs.len() as u32,
                        state_name_prefix: String::new(),
                        spec: key,
                        child_identity: child_identity.clone().unwrap_or_default(),
                        functions: vec![function],
                    });
                }
            }
            nodes.extend(specs);
            collect_window_plans(child, inputs, next_node_ordinal, nodes);
        }
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. }
        | OpTree::Distinct { child }
        | OpTree::LateralFunction { child, .. }
        | OpTree::LateralSubquery { child, .. } => {
            collect_window_plans(child, inputs, next_node_ordinal, nodes);
        }
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. }
        | OpTree::Intersect { left, right, .. }
        | OpTree::Except { left, right, .. } => {
            collect_window_plans(left, inputs, next_node_ordinal, nodes);
            collect_window_plans(right, inputs, next_node_ordinal, nodes);
        }
        OpTree::UnionAll { children } => {
            for child in children {
                collect_window_plans(child, inputs, next_node_ordinal, nodes);
            }
        }
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => {
            collect_window_plans(subquery, inputs, next_node_ordinal, nodes);
            collect_window_plans(child, inputs, next_node_ordinal, nodes);
        }
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            collect_window_plans(base, inputs, next_node_ordinal, nodes);
            collect_window_plans(recursive, inputs, next_node_ordinal, nodes);
        }
        OpTree::Aggregate { child, .. } => {
            collect_window_plans(child, inputs, next_node_ordinal, nodes);
        }
        OpTree::Scan { .. }
        | OpTree::ConstantSelect { .. }
        | OpTree::CteScan { .. }
        | OpTree::RecursiveSelfRef { .. } => {}
    }
}

fn plan_function(
    window: &WindowExpr,
    metadata: WindowPlanningInput,
    function_ordinal: u32,
    child_identity: Option<&[String]>,
) -> WindowFunctionStrategy {
    let offset = constant_argument(window, metadata.kind);
    let fallback = static_fallback(window, &metadata, offset, child_identity);
    let eligible = fallback.is_none();
    // The v0.89 benchmark rejected every measured state-backed ROW_NUMBER
    // cell. Keep the semantic candidate visible, but do not route production
    // refreshes into a slower full-partition implementation.
    let runtime_enabled = false;
    let fallback_reason = Some(
        fallback
            .unwrap_or(if metadata.kind == WindowFunctionKind::RowNumber {
                RefreshReasonCode::WindowRecomputeCheaper
            } else {
                RefreshReasonCode::WindowIncrementalUnimplemented
            })
            .as_str()
            .to_string(),
    );

    WindowFunctionStrategy {
        function_ordinal,
        alias: window.alias.clone(),
        function_oid: metadata.function_oid,
        kind: metadata.kind,
        argument_types: metadata.argument_types,
        result_type_oid: metadata.result_type_oid,
        result_type_sql: metadata.result_type_sql,
        result_typmod: metadata.result_typmod,
        result_collation_oid: metadata.result_collation_oid,
        frame: metadata.frame,
        constant_offset: offset,
        filter: metadata.filter,
        parse_location: metadata.parse_location,
        strategy: WindowIncrementalStrategy::PartitionRecompute,
        eligible,
        runtime_enabled,
        fallback_reason,
    }
}

fn static_fallback(
    window: &WindowExpr,
    metadata: &WindowPlanningInput,
    offset: Option<i64>,
    child_identity: Option<&[String]>,
) -> Option<RefreshReasonCode> {
    if metadata.function_oid == 0
        || metadata.result_type_oid == 0
        || metadata.result_type_sql.is_empty()
        || metadata.argument_types.iter().any(|arg| arg.type_oid == 0)
        || metadata.partition.iter().any(|part| part.type_oid == 0)
        || metadata.order.iter().any(|order| {
            order.type_oid == 0 || order.sort_operator_oid == 0 || order.equality_operator_oid == 0
        })
        || (metadata.kind.observes_frame() && !frame_metadata_is_resolved(&metadata.frame))
    {
        return Some(RefreshReasonCode::WindowMetadataUnresolved);
    }
    if metadata.kind == WindowFunctionKind::Unsupported {
        return Some(RefreshReasonCode::WindowUnsupportedFunction);
    }
    if child_identity.is_none() {
        return Some(RefreshReasonCode::WindowNoStableIdentity);
    }
    if metadata.filter.is_some() {
        return Some(RefreshReasonCode::WindowUnsupportedArgument);
    }
    if metadata.kind.observes_frame()
        && !frame_supported(metadata.kind, &metadata.frame, metadata.order.is_empty())
    {
        return Some(RefreshReasonCode::WindowUnsupportedFrame);
    }
    if !arguments_supported(window, metadata.kind, offset) {
        return Some(RefreshReasonCode::WindowUnsupportedArgument);
    }
    if metadata.kind == WindowFunctionKind::Sum
        && !matches!(
            metadata.argument_types.first().map(|arg| arg.type_oid),
            Some(20 | 21 | 23 | PG_NUMERIC_TYPE_OID)
        )
    {
        return Some(RefreshReasonCode::WindowUnsupportedType);
    }
    None
}

fn frame_metadata_is_resolved(frame: &WindowFrameSpec) -> bool {
    [&frame.start, &frame.end].into_iter().all(|bound| {
        !matches!(
            bound,
            WindowFrameBound::OffsetPreceding(offset)
                | WindowFrameBound::OffsetFollowing(offset)
                if offset.type_oid == 0
        )
    })
}

/// PostgreSQL analysis must describe exactly the same raw function shape.
/// A partial `zip()` merge would otherwise make truncated metadata look
/// complete, so any count or frame-kind mismatch leaves the whole function
/// unresolved.
pub(crate) fn analyzed_window_shape_matches(
    raw: &WindowPlanningInput,
    argument_count: usize,
    partition_count: usize,
    order_count: usize,
    analyzed_frame: &WindowFrameSpec,
) -> bool {
    raw.argument_types.len() == argument_count
        && raw.partition.len() == partition_count
        && raw.order.len() == order_count
        && frame_shapes_match(&raw.frame, analyzed_frame)
}

fn frame_shapes_match(raw: &WindowFrameSpec, analyzed: &WindowFrameSpec) -> bool {
    raw.mode == analyzed.mode
        && raw.exclusion == analyzed.exclusion
        && bound_shapes_match(&raw.start, &analyzed.start)
        && bound_shapes_match(&raw.end, &analyzed.end)
}

fn bound_shapes_match(raw: &WindowFrameBound, analyzed: &WindowFrameBound) -> bool {
    matches!(
        (raw, analyzed),
        (
            WindowFrameBound::UnboundedPreceding,
            WindowFrameBound::UnboundedPreceding
        ) | (
            WindowFrameBound::OffsetPreceding(_),
            WindowFrameBound::OffsetPreceding(_)
        ) | (WindowFrameBound::CurrentRow, WindowFrameBound::CurrentRow)
            | (
                WindowFrameBound::OffsetFollowing(_),
                WindowFrameBound::OffsetFollowing(_)
            )
            | (
                WindowFrameBound::UnboundedFollowing,
                WindowFrameBound::UnboundedFollowing
            )
    )
}

fn arguments_supported(window: &WindowExpr, kind: WindowFunctionKind, offset: Option<i64>) -> bool {
    match kind {
        WindowFunctionKind::RowNumber
        | WindowFunctionKind::Rank
        | WindowFunctionKind::DenseRank => window.args.is_empty(),
        WindowFunctionKind::Lag | WindowFunctionKind::Lead => {
            (1..=3).contains(&window.args.len()) && offset.is_some_and(|value| value >= 0)
        }
        WindowFunctionKind::FirstValue | WindowFunctionKind::LastValue => window.args.len() == 1,
        WindowFunctionKind::NthValue => {
            window.args.len() == 2 && offset.is_some_and(|value| value > 0)
        }
        WindowFunctionKind::Sum => window.args.len() == 1,
        WindowFunctionKind::Count => window.args.len() <= 1,
        WindowFunctionKind::Unsupported => false,
    }
}

fn constant_argument(window: &WindowExpr, kind: WindowFunctionKind) -> Option<i64> {
    let argument = match kind {
        WindowFunctionKind::Lag | WindowFunctionKind::Lead => {
            return window
                .args
                .get(1)
                .map(parse_integer_constant)
                .unwrap_or(Some(1));
        }
        WindowFunctionKind::NthValue => window.args.get(1),
        _ => return None,
    }?;
    parse_integer_constant(argument)
}

fn parse_integer_constant(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(value) => value.trim().parse().ok(),
        _ => None,
    }
}

fn frame_supported(kind: WindowFunctionKind, frame: &WindowFrameSpec, no_order: bool) -> bool {
    if frame.exclusion != WindowFrameExclusion::None || frame.mode == WindowFrameMode::Groups {
        return false;
    }
    let cumulative = frame.start == WindowFrameBound::UnboundedPreceding
        && frame.end == WindowFrameBound::CurrentRow
        && matches!(frame.mode, WindowFrameMode::Rows | WindowFrameMode::Range);
    let whole = frame.start == WindowFrameBound::UnboundedPreceding
        && (frame.end == WindowFrameBound::UnboundedFollowing
            || (no_order && frame.end == WindowFrameBound::CurrentRow));

    match kind {
        WindowFunctionKind::FirstValue
        | WindowFunctionKind::LastValue
        | WindowFunctionKind::NthValue
        | WindowFunctionKind::Sum
        | WindowFunctionKind::Count => cumulative || whole,
        _ => true,
    }
}

/// Prove an exact child identity for window state without changing the legacy
/// `row_id_key_columns()` compatibility heuristics.
pub fn exact_window_identity_columns(tree: &OpTree) -> Option<Vec<String>> {
    match tree {
        OpTree::Scan {
            pk_columns,
            columns,
            ..
        } if !pk_columns.is_empty()
            && pk_columns
                .iter()
                .all(|key| columns.iter().any(|column| column.name == *key)) =>
        {
            Some(pk_columns.clone())
        }
        OpTree::Filter { child, .. } | OpTree::Subquery { child, .. } => {
            exact_window_identity_columns(child)
        }
        OpTree::Project {
            expressions,
            aliases,
            child,
        } => {
            let child_keys = exact_window_identity_columns(child)?;
            let mapped = child_keys
                .iter()
                .map(|key| {
                    expressions
                        .iter()
                        .position(|expr| {
                            matches!(expr, Expr::ColumnRef { column_name, .. } if column_name == key)
                        })
                        .and_then(|position| aliases.get(position).cloned())
                })
                .collect::<Option<Vec<_>>>()?;
            exact_identity_names(mapped)
        }
        OpTree::Aggregate { group_by, .. } => {
            exact_identity_names(group_by.iter().map(Expr::output_name).collect())
        }
        OpTree::Distinct { child } => exact_identity_names(child.output_columns()),
        OpTree::Intersect {
            left, all: false, ..
        }
        | OpTree::Except {
            left, all: false, ..
        } => exact_identity_names(left.output_columns()),
        OpTree::SemiJoin { left, .. } | OpTree::AntiJoin { left, .. } => {
            exact_window_identity_columns(left)
        }
        OpTree::ConstantSelect { .. } => Some(Vec::new()),
        _ => None,
    }
}

fn exact_identity_names(names: Vec<String>) -> Option<Vec<String>> {
    let mut unique = std::collections::HashSet::with_capacity(names.len());
    names
        .iter()
        .all(|name| !name.is_empty() && unique.insert(name.as_str()))
        .then_some(names)
}

fn semantic_identity(tree: &OpTree) -> String {
    let canonical = format!("{tree:?}");
    format!("{:016x}", xxhash_rust::xxh3::xxh3_64(canonical.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> Expr {
        Expr::ColumnRef {
            table_alias: None,
            column_name: name.into(),
        }
    }

    fn scan(pk: &[&str]) -> OpTree {
        OpTree::Scan {
            table_oid: 42,
            table_name: "events".into(),
            schema: "public".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    type_oid: 23,
                    is_nullable: false,
                },
                Column {
                    name: "score".into(),
                    type_oid: 23,
                    is_nullable: false,
                },
            ],
            pk_columns: pk.iter().map(|key| (*key).into()).collect(),
            alias: "e".into(),
        }
    }

    fn window(kind: WindowFunctionKind, args: Vec<Expr>) -> (OpTree, WindowPlanningInput) {
        let expr = WindowExpr {
            func_name: "row_number".into(),
            args,
            partition_by: vec![],
            order_by: vec![
                SortExpr {
                    expr: col("score"),
                    ascending: true,
                    nulls_first: false,
                },
                SortExpr {
                    expr: col("id"),
                    ascending: true,
                    nulls_first: false,
                },
            ],
            frame_clause: None,
            alias: "value".into(),
        };
        let input = WindowPlanningInput {
            signature: window_signature(&expr),
            function_oid: 3100,
            kind,
            argument_types: vec![],
            result_type_oid: 20,
            result_type_sql: "bigint".into(),
            result_typmod: -1,
            result_collation_oid: 0,
            partition: vec![],
            order: vec![
                WindowOrderKey {
                    expression: "score".into(),
                    type_oid: 23,
                    typmod: -1,
                    collation_oid: 0,
                    ascending: true,
                    nulls_first: false,
                    sort_operator_oid: 97,
                    equality_operator_oid: 96,
                },
                WindowOrderKey {
                    expression: "id".into(),
                    type_oid: 23,
                    typmod: -1,
                    collation_oid: 0,
                    ascending: true,
                    nulls_first: false,
                    sort_operator_oid: 97,
                    equality_operator_oid: 96,
                },
            ],
            frame: WindowFrameSpec::postgres_default(),
            filter: None,
            parse_location: Some(7),
        };
        (
            OpTree::Window {
                window_exprs: vec![expr],
                partition_by: vec![],
                pass_through: vec![(col("id"), "id".into()), (col("score"), "score".into())],
                child: Box::new(scan(&["id"])),
            },
            input,
        )
    }

    #[test]
    fn frame_defaults_and_exclusions_render_exactly() {
        assert_eq!(
            WindowFrameSpec::postgres_default().to_sql(),
            "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
        );
        let excluded = WindowFrameSpec {
            mode: WindowFrameMode::Rows,
            start: WindowFrameBound::OffsetPreceding(WindowFrameOffset {
                sql: "3".into(),
                type_oid: 20,
                typmod: -1,
                collation_oid: 0,
            }),
            end: WindowFrameBound::OffsetFollowing(WindowFrameOffset {
                sql: "2".into(),
                type_oid: 20,
                typmod: -1,
                collation_oid: 0,
            }),
            exclusion: WindowFrameExclusion::Ties,
            was_implicit: false,
        };
        assert_eq!(
            excluded.to_sql(),
            "ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING EXCLUDE TIES"
        );

        for (exclusion, suffix) in [
            (WindowFrameExclusion::None, ""),
            (WindowFrameExclusion::CurrentRow, " EXCLUDE CURRENT ROW"),
            (WindowFrameExclusion::Group, " EXCLUDE GROUP"),
            (WindowFrameExclusion::Ties, " EXCLUDE TIES"),
        ] {
            let frame = WindowFrameSpec {
                mode: WindowFrameMode::Groups,
                start: WindowFrameBound::UnboundedPreceding,
                end: WindowFrameBound::UnboundedFollowing,
                exclusion,
                was_implicit: false,
            };
            assert_eq!(
                frame.to_sql(),
                format!("GROUPS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING{suffix}")
            );
        }
    }

    #[test]
    fn planner_keeps_row_number_on_measured_partition_recompute() {
        let (tree, input) = window(WindowFunctionKind::RowNumber, vec![]);
        let plan = build_window_strategy_plan(
            &tree,
            &CteRegistry::default(),
            std::slice::from_ref(&input),
        )
        .expect("window plan");
        assert!(plan.nodes[0].functions[0].eligible);
        assert!(!plan.nodes[0].functions[0].runtime_enabled);
        assert_eq!(
            plan.nodes[0].functions[0].strategy,
            WindowIncrementalStrategy::PartitionRecompute
        );
        assert_eq!(
            plan.nodes[0].functions[0].fallback_reason.as_deref(),
            Some("WINDOW_RECOMPUTE_CHEAPER")
        );
        assert_eq!(plan.nodes[0].node_ordinal, 0);
        assert_eq!(plan.nodes[0].functions[0].function_ordinal, 0);

        let OpTree::Window {
            window_exprs,
            partition_by,
            pass_through,
            ..
        } = tree
        else {
            unreachable!()
        };
        let no_key = OpTree::Window {
            window_exprs,
            partition_by,
            pass_through,
            child: Box::new(scan(&[])),
        };
        let plan = build_window_strategy_plan(&no_key, &CteRegistry::default(), &[input])
            .expect("window plan");
        assert!(!plan.nodes[0].functions[0].runtime_enabled);
        assert_eq!(
            plan.nodes[0].functions[0].fallback_reason.as_deref(),
            Some("WINDOW_NO_STABLE_IDENTITY")
        );
    }

    #[test]
    fn planner_runtime_rejects_mixed_window_node() {
        let (mut tree, input) = window(WindowFunctionKind::RowNumber, vec![]);
        let OpTree::Window { window_exprs, .. } = &mut tree else {
            unreachable!()
        };
        let mut rank = window_exprs[0].clone();
        rank.func_name = "rank".into();
        rank.alias = "rank_value".into();
        window_exprs.push(rank.clone());
        let mut rank_input = input.clone();
        rank_input.signature = window_signature(&rank);
        rank_input.kind = WindowFunctionKind::Rank;

        let plan = build_window_strategy_plan(&tree, &CteRegistry::default(), &[input, rank_input])
            .expect("window plan");
        assert!(
            plan.nodes[0]
                .functions
                .iter()
                .all(|function| !function.runtime_enabled)
        );
        assert_eq!(
            plan.nodes[0].functions[0].fallback_reason.as_deref(),
            Some("WINDOW_RECOMPUTE_CHEAPER")
        );
        assert_eq!(
            plan.nodes[0].functions[1].fallback_reason.as_deref(),
            Some("WINDOW_INCREMENTAL_UNIMPLEMENTED")
        );
    }

    #[test]
    fn planner_rejects_unresolved_and_unknown_versions() {
        let (tree, _) = window(WindowFunctionKind::RowNumber, vec![]);
        let plan =
            build_window_strategy_plan(&tree, &CteRegistry::default(), &[]).expect("window plan");
        assert_eq!(
            plan.nodes[0].functions[0].fallback_reason.as_deref(),
            Some("WINDOW_METADATA_UNRESOLVED")
        );

        let mut json = plan.to_json().expect("serialize plan");
        assert_eq!(
            WindowStrategyPlan::from_json(json.clone()).expect("deserialize plan"),
            plan
        );
        json["strategy_version"] = serde_json::json!(99);
        assert!(WindowStrategyPlan::from_json(json).is_err());
    }

    #[test]
    fn exact_identity_does_not_treat_keyless_content_as_unique() {
        assert_eq!(exact_window_identity_columns(&scan(&[])), None);
        assert_eq!(
            exact_window_identity_columns(&scan(&["id"])),
            Some(vec!["id".into()])
        );
    }

    #[test]
    fn function_classification_is_closed_and_namespace_aware() {
        assert_eq!(
            classify_window_function("pg_catalog", "rank"),
            WindowFunctionKind::Rank
        );
        assert_eq!(
            classify_window_function("public", "rank"),
            WindowFunctionKind::Unsupported
        );
        assert_eq!(
            classify_window_function("pg_catalog", "custom_rank"),
            WindowFunctionKind::Unsupported
        );
    }

    #[test]
    fn semantic_fingerprint_ignores_nonsemantic_runtime_fields() {
        let (tree, input) = window(WindowFunctionKind::RowNumber, vec![]);
        let plan = build_window_strategy_plan(&tree, &CteRegistry::default(), &[input])
            .expect("window plan");
        let mut changed = plan.nodes.clone();
        changed[0].state_name_prefix = "different_runtime_relation".into();
        changed[0].functions[0].alias = "different_output_alias".into();
        changed[0].functions[0].parse_location = Some(999);
        changed[0].functions[0].runtime_enabled = false;
        changed[0].functions[0].fallback_reason = Some("WINDOW_RECOMPUTE_CHEAPER".into());

        assert_eq!(
            semantic_plan_fingerprint(&plan.nodes),
            semantic_plan_fingerprint(&changed)
        );
    }

    #[test]
    fn analyzed_shape_mismatch_fails_closed() {
        let (_, input) = window(WindowFunctionKind::RowNumber, vec![]);
        assert!(analyzed_window_shape_matches(
            &input,
            0,
            0,
            2,
            &WindowFrameSpec::postgres_default()
        ));
        assert!(!analyzed_window_shape_matches(
            &input,
            2,
            0,
            1,
            &WindowFrameSpec::postgres_default()
        ));

        let mismatched_frame = WindowFrameSpec {
            mode: WindowFrameMode::Rows,
            ..WindowFrameSpec::postgres_default()
        };
        assert!(!analyzed_window_shape_matches(
            &input,
            0,
            0,
            1,
            &mismatched_frame
        ));
    }

    #[test]
    fn planner_rejects_unresolved_frame_offset_metadata() {
        let (tree, mut input) = window(WindowFunctionKind::Sum, vec![col("score")]);
        input.argument_types = vec![WindowExpressionType {
            expression: "score".into(),
            type_oid: 23,
            typmod: -1,
            collation_oid: 0,
        }];
        input.frame = WindowFrameSpec {
            mode: WindowFrameMode::Rows,
            start: WindowFrameBound::OffsetPreceding(WindowFrameOffset {
                sql: "1".into(),
                type_oid: 0,
                typmod: -1,
                collation_oid: 0,
            }),
            end: WindowFrameBound::CurrentRow,
            exclusion: WindowFrameExclusion::None,
            was_implicit: false,
        };
        let plan = build_window_strategy_plan(&tree, &CteRegistry::default(), &[input])
            .expect("window plan");
        assert_eq!(
            plan.nodes[0].functions[0].fallback_reason.as_deref(),
            Some("WINDOW_METADATA_UNRESOLVED")
        );
    }

    #[test]
    fn exact_identity_rejects_duplicate_or_ambiguous_names() {
        let projected = OpTree::Project {
            expressions: vec![col("id"), col("score")],
            aliases: vec!["same".into(), "same".into()],
            child: Box::new(scan(&["id", "score"])),
        };
        assert_eq!(exact_window_identity_columns(&projected), None);

        let grouped = OpTree::Aggregate {
            group_by: vec![col("id"), col("id")],
            aggregates: vec![],
            child: Box::new(scan(&["id"])),
        };
        assert_eq!(exact_window_identity_columns(&grouped), None);
    }
}
