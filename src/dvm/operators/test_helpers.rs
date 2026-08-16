//! Shared test helpers for DVM operator unit tests.
//!
//! Provides builders for `OpTree` variants, a standalone `DiffContext`
//! constructor, and assertion helpers. All helpers are `#[cfg(test)]`
//! and never touch PostgreSQL.

use crate::dvm::diff::DiffContext;
use crate::dvm::parser::{
    AggExpr, AggFunc, Column, Expr, OpTree, PG_FLOAT8_TYPE_OID, PG_NUMERIC_TYPE_OID, SortExpr,
    StatisticalAggSupport, WindowExpr,
};
use crate::version::Frontier;

// ── DiffContext builder ─────────────────────────────────────────────────

/// Create a `DiffContext` suitable for unit tests (no PG dependency).
pub fn test_ctx() -> DiffContext {
    DiffContext::new_standalone(Frontier::new(), Frontier::new())
}

/// Create a `DiffContext` with a ST name set (needed by aggregate/distinct/window).
pub fn test_ctx_with_st(schema: &str, name: &str) -> DiffContext {
    DiffContext::new_standalone(Frontier::new(), Frontier::new()).with_pgt_name(schema, name)
}

// ── Column builder ──────────────────────────────────────────────────────

/// Build a `Column` with default type_oid=23 (int4) and nullable=true.
pub fn col(name: &str) -> Column {
    Column {
        name: name.to_string(),
        type_oid: 23,
        is_nullable: true,
    }
}

/// Build a non-nullable `Column`.
pub fn col_not_null(name: &str) -> Column {
    Column {
        name: name.to_string(),
        type_oid: 23,
        is_nullable: false,
    }
}

// ── OpTree builders ─────────────────────────────────────────────────────

/// Build a basic Scan node.
pub fn scan(oid: u32, table: &str, schema: &str, alias: &str, cols: &[&str]) -> OpTree {
    OpTree::Scan {
        table_oid: oid,
        table_name: table.to_string(),
        schema: schema.to_string(),
        columns: cols.iter().map(|c| col(c)).collect(),
        pk_columns: Vec::new(),
        alias: alias.to_string(),
    }
}

/// Build a Scan node with explicit PK columns.
pub fn scan_with_pk(
    oid: u32,
    table: &str,
    schema: &str,
    alias: &str,
    cols: &[&str],
    pk: &[&str],
) -> OpTree {
    OpTree::Scan {
        table_oid: oid,
        table_name: table.to_string(),
        schema: schema.to_string(),
        columns: cols.iter().map(|c| col(c)).collect(),
        pk_columns: pk.iter().map(|c| c.to_string()).collect(),
        alias: alias.to_string(),
    }
}

/// Build a Scan node with non-nullable columns.
pub fn scan_not_null(oid: u32, table: &str, schema: &str, alias: &str, cols: &[&str]) -> OpTree {
    OpTree::Scan {
        table_oid: oid,
        table_name: table.to_string(),
        schema: schema.to_string(),
        columns: cols.iter().map(|c| col_not_null(c)).collect(),
        pk_columns: Vec::new(),
        alias: alias.to_string(),
    }
}

/// Build a Filter node.
pub fn filter(predicate: Expr, child: OpTree) -> OpTree {
    OpTree::Filter {
        predicate,
        child: Box::new(child),
    }
}

/// Build a Project node.
pub fn project(exprs: Vec<Expr>, aliases: Vec<&str>, child: OpTree) -> OpTree {
    OpTree::Project {
        expressions: exprs,
        aliases: aliases.into_iter().map(|a| a.to_string()).collect(),
        child: Box::new(child),
    }
}

/// Build an Aggregate node.
pub fn aggregate(group_by: Vec<Expr>, aggregates: Vec<AggExpr>, child: OpTree) -> OpTree {
    OpTree::Aggregate {
        group_by,
        aggregates,
        child: Box::new(child),
    }
}

/// Build an InnerJoin node.
pub fn inner_join(condition: Expr, left: OpTree, right: OpTree) -> OpTree {
    OpTree::InnerJoin {
        condition,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build a LeftJoin node.
pub fn left_join(condition: Expr, left: OpTree, right: OpTree) -> OpTree {
    OpTree::LeftJoin {
        condition,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build a FullJoin node.
pub fn full_join(condition: Expr, left: OpTree, right: OpTree) -> OpTree {
    OpTree::FullJoin {
        condition,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build a Distinct node.
pub fn distinct(child: OpTree) -> OpTree {
    OpTree::Distinct {
        child: Box::new(child),
    }
}

/// Build a UnionAll node.
pub fn union_all(children: Vec<OpTree>) -> OpTree {
    OpTree::UnionAll { children }
}

/// Build an Intersect node.
pub fn intersect(left: OpTree, right: OpTree, all: bool) -> OpTree {
    OpTree::Intersect {
        left: Box::new(left),
        right: Box::new(right),
        all,
    }
}

/// Build an Except node.
pub fn except(left: OpTree, right: OpTree, all: bool) -> OpTree {
    OpTree::Except {
        left: Box::new(left),
        right: Box::new(right),
        all,
    }
}

/// Build a Window node.
pub fn window(
    window_exprs: Vec<WindowExpr>,
    partition_by: Vec<Expr>,
    pass_through: Vec<(Expr, String)>,
    child: OpTree,
) -> OpTree {
    OpTree::Window {
        window_exprs,
        partition_by,
        pass_through,
        child: Box::new(child),
    }
}

/// Build a Subquery node.
pub fn subquery(alias: &str, col_aliases: Vec<&str>, child: OpTree) -> OpTree {
    OpTree::Subquery {
        alias: alias.to_string(),
        column_aliases: col_aliases.into_iter().map(|c| c.to_string()).collect(),
        child: Box::new(child),
    }
}

/// Build a LateralSubquery node.
pub fn lateral_subquery(
    subquery_sql: &str,
    alias: &str,
    col_aliases: Vec<&str>,
    output_cols: Vec<&str>,
    is_left_join: bool,
    subquery_source_oids: Vec<u32>,
    child: OpTree,
) -> OpTree {
    OpTree::LateralSubquery {
        subquery_sql: subquery_sql.to_string(),
        alias: alias.to_string(),
        column_aliases: col_aliases.into_iter().map(|c| c.to_string()).collect(),
        output_cols: output_cols.into_iter().map(|c| c.to_string()).collect(),
        is_left_join,
        subquery_source_oids,
        correlation_predicates: Vec::new(),
        child: Box::new(child),
    }
}

/// Build a CteScan node.
pub fn cte_scan(
    cte_id: usize,
    cte_name: &str,
    alias: &str,
    cols: Vec<&str>,
    cte_def_aliases: Vec<&str>,
    column_aliases: Vec<&str>,
) -> OpTree {
    OpTree::CteScan {
        cte_id,
        cte_name: cte_name.to_string(),
        alias: alias.to_string(),
        columns: cols.into_iter().map(|c| c.to_string()).collect(),
        cte_def_aliases: cte_def_aliases.into_iter().map(|c| c.to_string()).collect(),
        column_aliases: column_aliases.into_iter().map(|c| c.to_string()).collect(),
        body: None,
    }
}

/// Build a SemiJoin node (EXISTS / IN subquery).
pub fn semi_join(condition: Expr, left: OpTree, right: OpTree) -> OpTree {
    OpTree::SemiJoin {
        condition,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build an AntiJoin node (NOT EXISTS / NOT IN subquery).
pub fn anti_join(condition: Expr, left: OpTree, right: OpTree) -> OpTree {
    OpTree::AntiJoin {
        condition,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build a ScalarSubquery node.
pub fn scalar_subquery(
    subquery: OpTree,
    alias: &str,
    source_oids: Vec<u32>,
    child: OpTree,
) -> OpTree {
    OpTree::ScalarSubquery {
        subquery: Box::new(subquery),
        alias: alias.to_string(),
        subquery_source_oids: source_oids,
        child: Box::new(child),
    }
}

// ── Expr helpers ────────────────────────────────────────────────────────

/// Build a `ColumnRef` without table qualifier.
pub fn colref(name: &str) -> Expr {
    Expr::ColumnRef {
        table_alias: None,
        column_name: name.to_string(),
    }
}

/// Build a qualified `ColumnRef`.
pub fn qcolref(table: &str, name: &str) -> Expr {
    Expr::ColumnRef {
        table_alias: Some(table.to_string()),
        column_name: name.to_string(),
    }
}

/// Build a simple binary op expression.
pub fn binop(op: &str, left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: op.to_string(),
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// Build an equality condition: `left_table.left_col = right_table.right_col`.
pub fn eq_cond(lt: &str, lc: &str, rt: &str, rc: &str) -> Expr {
    binop("=", qcolref(lt, lc), qcolref(rt, rc))
}

/// Build a literal expression.
pub fn lit(val: &str) -> Expr {
    Expr::Literal(val.to_string())
}

fn numeric_statistical_support() -> Option<StatisticalAggSupport> {
    Some(StatisticalAggSupport::with_accumulator(
        PG_NUMERIC_TYPE_OID,
        "numeric",
    ))
}

fn float8_statistical_support() -> Option<StatisticalAggSupport> {
    Some(StatisticalAggSupport::with_accumulator(
        PG_FLOAT8_TYPE_OID,
        "double precision",
    ))
}

// ── AggExpr helpers ─────────────────────────────────────────────────────

/// Build a COUNT(*) aggregate.
pub fn count_star(alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::CountStar,
        argument: None,
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a SUM(col) aggregate.
pub fn sum_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Sum,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a COUNT(col) aggregate.
pub fn count_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Count,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build an AVG(col) aggregate.
pub fn avg_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Avg,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a MIN(col) aggregate.
pub fn min_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Min,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a MAX(col) aggregate.
pub fn max_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Max,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a BOOL_AND(col) aggregate.
pub fn bool_and_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::BoolAnd,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a BOOL_OR(col) aggregate.
pub fn bool_or_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::BoolOr,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a STRING_AGG(col, sep) aggregate.
pub fn string_agg_col(col: &str, sep: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::StringAgg,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: Some(Expr::Literal(sep.to_string())),
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build an ARRAY_AGG(col) aggregate.
pub fn array_agg_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::ArrayAgg,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build an aggregate with a FILTER clause.
pub fn with_filter(mut agg: AggExpr, filter: Expr) -> AggExpr {
    agg.filter = Some(filter);
    agg
}

/// Build a BIT_AND(col) aggregate.
pub fn bit_and_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::BitAnd,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a BIT_OR(col) aggregate.
pub fn bit_or_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::BitOr,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a BIT_XOR(col) aggregate.
pub fn bit_xor_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::BitXor,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a JSON_OBJECT_AGG(key, value) aggregate.
pub fn json_object_agg_col(key_col: &str, val_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::JsonObjectAgg,
        argument: Some(colref(key_col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: Some(colref(val_col)),
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a JSONB_OBJECT_AGG(key, value) aggregate.
pub fn jsonb_object_agg_col(key_col: &str, val_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::JsonbObjectAgg,
        argument: Some(colref(key_col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: Some(colref(val_col)),
        order_within_group: None,
        statistical_support: None,
    }
}

/// Build a STDDEV_POP(col) aggregate.
pub fn stddev_pop_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::StddevPop,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: numeric_statistical_support(),
    }
}

/// Build a STDDEV_SAMP(col) aggregate (also used for STDDEV alias).
pub fn stddev_samp_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::StddevSamp,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: numeric_statistical_support(),
    }
}

/// Build a VAR_POP(col) aggregate.
pub fn var_pop_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::VarPop,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: numeric_statistical_support(),
    }
}

/// Build a VAR_SAMP(col) aggregate (also used for VARIANCE alias).
pub fn var_samp_col(col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::VarSamp,
        argument: Some(colref(col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: None,
        statistical_support: numeric_statistical_support(),
    }
}

/// Build a MODE() WITHIN GROUP (ORDER BY col) aggregate.
pub fn mode_col(order_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::Mode,
        argument: None,
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: Some(vec![SortExpr {
            expr: colref(order_col),
            ascending: true,
            nulls_first: false,
        }]),
        statistical_support: None,
    }
}

/// Build a PERCENTILE_CONT(fraction) WITHIN GROUP (ORDER BY col) aggregate.
pub fn percentile_cont_col(fraction: &str, order_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::PercentileCont,
        argument: Some(lit(fraction)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: Some(vec![SortExpr {
            expr: colref(order_col),
            ascending: true,
            nulls_first: false,
        }]),
        statistical_support: None,
    }
}

/// Build a PERCENTILE_DISC(fraction) WITHIN GROUP (ORDER BY col) aggregate.
pub fn percentile_disc_col(fraction: &str, order_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: AggFunc::PercentileDisc,
        argument: Some(lit(fraction)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: None,
        order_within_group: Some(vec![SortExpr {
            expr: colref(order_col),
            ascending: true,
            nulls_first: false,
        }]),
        statistical_support: None,
    }
}

/// Build a generic two-argument regression aggregate: `func(y_col, x_col)`.
///
/// Covers: CORR, COVAR_POP, COVAR_SAMP, REGR_AVGX, REGR_AVGY,
/// REGR_COUNT, REGR_INTERCEPT, REGR_R2, REGR_SLOPE, REGR_SXX,
/// REGR_SXY, REGR_SYY.
pub fn regression_agg(func: AggFunc, y_col: &str, x_col: &str, alias: &str) -> AggExpr {
    AggExpr {
        function: func,
        argument: Some(colref(y_col)),
        alias: alias.to_string(),
        is_distinct: false,
        filter: None,
        second_arg: Some(colref(x_col)),
        order_within_group: None,
        statistical_support: float8_statistical_support(),
    }
}

// ── WindowExpr helpers ──────────────────────────────────────────────────

/// Build a simple window expression (e.g., `ROW_NUMBER() OVER (PARTITION BY ...)`).
pub fn window_expr(
    func_name: &str,
    args: Vec<Expr>,
    partition_by: Vec<Expr>,
    order_by: Vec<SortExpr>,
    alias: &str,
) -> WindowExpr {
    WindowExpr {
        func_name: func_name.to_string(),
        args,
        partition_by,
        order_by,
        frame_clause: None,
        alias: alias.to_string(),
    }
}

/// Build an ascending SortExpr.
pub fn sort_asc(expr: Expr) -> SortExpr {
    SortExpr {
        expr,
        ascending: true,
        nulls_first: false,
    }
}

// ── Assertion helpers ───────────────────────────────────────────────────

/// Assert that the generated SQL contains a substring (case-sensitive).
pub fn assert_sql_contains(sql: &str, expected: &str) {
    assert!(
        sql.contains(expected),
        "Expected SQL to contain:\n  {expected}\nGot:\n  {sql}",
    );
}

/// Assert that the generated SQL does NOT contain a substring (case-sensitive).
pub fn assert_sql_not_contains(sql: &str, unexpected: &str) {
    assert!(
        !sql.contains(unexpected),
        "Expected SQL NOT to contain:\n  {unexpected}\nGot:\n  {sql}",
    );
}

/// Synthesize a NATURAL JOIN condition by finding common column names
/// between left and right OpTree children.
///
/// Replicates the logic the parser uses for `isNatural = true` in JoinExpr.
pub fn natural_join_cond(left: &OpTree, right: &OpTree) -> Expr {
    let left_cols = left.output_columns();
    let right_cols = right.output_columns();
    let left_alias = left.alias().to_string();
    let right_alias = right.alias().to_string();

    let common: Vec<String> = left_cols
        .iter()
        .filter(|lc| right_cols.iter().any(|rc| rc.eq_ignore_ascii_case(lc)))
        .cloned()
        .collect();

    if common.is_empty() {
        Expr::Literal("TRUE".into())
    } else {
        let mut parts: Vec<Expr> = Vec::with_capacity(common.len());
        for col_name in &common {
            parts.push(Expr::BinaryOp {
                left: Box::new(Expr::ColumnRef {
                    table_alias: Some(left_alias.clone()),
                    column_name: col_name.clone(),
                }),
                op: "=".into(),
                right: Box::new(Expr::ColumnRef {
                    table_alias: Some(right_alias.clone()),
                    column_name: col_name.clone(),
                }),
            });
        }
        parts
            .into_iter()
            .reduce(|acc, part| Expr::BinaryOp {
                op: "AND".into(),
                left: Box::new(acc),
                right: Box::new(part),
            })
            .unwrap_or(Expr::Literal("TRUE".into()))
    }
}
