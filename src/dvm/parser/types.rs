//! Type definitions for the parser's intermediate representation.
//!
//! Contains [`Column`], [`Expr`], [`AggFunc`], [`AggExpr`], [`SortExpr`],
//! [`CorrelationPredicate`], [`WindowExpr`], [`OpTree`], [`CteRegistry`],
//! and [`ParseResult`] plus their associated `impl` blocks.

/// Column metadata.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub type_oid: u32,
    pub is_nullable: bool,
}

pub const PG_FLOAT8_TYPE_OID: u32 = 701;
pub const PG_NUMERIC_TYPE_OID: u32 = 1700;

/// A SQL type resolved from PostgreSQL catalog metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSqlType {
    pub oid: u32,
    pub sql: String,
}

/// Parse/analyze support metadata for statistical aggregates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticalAggSupport {
    /// Byte offset of the aggregate call in the original query, if known.
    pub parse_location: Option<i32>,
    /// PostgreSQL's resolved accumulator arithmetic type for square/cross
    /// products, if supported.
    pub accumulator_type: Option<ResolvedSqlType>,
}

impl StatisticalAggSupport {
    pub fn with_location(parse_location: i32) -> Self {
        Self {
            parse_location: Some(parse_location),
            accumulator_type: None,
        }
    }

    pub fn with_accumulator(oid: u32, sql: impl Into<String>) -> Self {
        Self {
            parse_location: None,
            accumulator_type: Some(ResolvedSqlType {
                oid,
                sql: sql.into(),
            }),
        }
    }
}

/// A SQL expression (simplified representation).
#[derive(Debug, Clone)]
pub enum Expr {
    /// A column reference: `table.column` or just `column`.
    ColumnRef {
        table_alias: Option<String>,
        column_name: String,
    },
    /// A literal value.
    Literal(String),
    /// A binary operation: `left op right`.
    BinaryOp {
        op: String,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// A function call: `func(args...)`.
    FuncCall { func_name: String, args: Vec<Expr> },
    /// A star expression: `*` or `table.*`.
    Star { table_alias: Option<String> },
    /// Raw SQL text (fallback for complex expressions).
    Raw(String),
}

impl Expr {
    /// Convert expression back to SQL text.
    ///
    /// Implemented as a thin wrapper over [`to_sql_into`] to avoid
    /// allocations when the caller already has a write buffer.
    pub fn to_sql(&self) -> String {
        let mut buf = String::with_capacity(32);
        self.to_sql_into(&mut buf);
        buf
    }

    /// Write SQL text for this expression into `buf`.
    ///
    /// P-5 (v0.54.0): Writes directly into the caller's buffer, avoiding
    /// intermediate `String` allocations for complex nested expressions.
    /// For a `BinaryOp` with depth d, this reduces allocations from O(2^d)
    /// (one per sub-expression) to O(1) (the final buffer write).
    pub fn to_sql_into(&self, buf: &mut String) {
        match self {
            Expr::ColumnRef {
                table_alias,
                column_name,
            } => match table_alias {
                Some(alias) => {
                    buf.push('"');
                    buf.push_str(&alias.replace('"', "\"\""));
                    buf.push_str("\".\"");
                    buf.push_str(&column_name.replace('"', "\"\""));
                    buf.push('"');
                }
                None => buf.push_str(column_name),
            },
            Expr::Literal(val) => buf.push_str(val),
            Expr::BinaryOp { op, left, right } => {
                buf.push('(');
                left.to_sql_into(buf);
                buf.push(' ');
                buf.push_str(op);
                buf.push(' ');
                right.to_sql_into(buf);
                buf.push(')');
            }
            Expr::FuncCall { func_name, args } => {
                buf.push_str(func_name);
                buf.push('(');
                for (i, a) in args.iter().enumerate() {
                    if i > 0 {
                        buf.push_str(", ");
                    }
                    a.to_sql_into(buf);
                }
                buf.push(')');
            }
            Expr::Star { table_alias } => match table_alias {
                Some(alias) => {
                    buf.push_str(alias);
                    buf.push_str(".*");
                }
                None => buf.push('*'),
            },
            Expr::Raw(sql) => buf.push_str(sql),
        }
    }

    /// Return the output column name for this expression.
    ///
    /// For `ColumnRef`, returns just the `column_name` (stripping the table
    /// qualifier) since PostgreSQL output columns from a subquery don't
    /// carry the original table alias.
    pub fn output_name(&self) -> String {
        match self {
            Expr::ColumnRef { column_name, .. } => column_name.clone(),
            _ => self.to_sql(),
        }
    }

    /// Return a copy of this expression with all table qualifiers stripped
    /// from `ColumnRef` nodes.
    ///
    /// This is needed when the expression references columns from a CTE
    /// whose output columns are unqualified.
    pub fn strip_qualifier(&self) -> Expr {
        match self {
            Expr::ColumnRef { column_name, .. } => Expr::ColumnRef {
                table_alias: None,
                column_name: column_name.clone(),
            },
            Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
                op: op.clone(),
                left: Box::new(left.strip_qualifier()),
                right: Box::new(right.strip_qualifier()),
            },
            Expr::FuncCall { func_name, args } => Expr::FuncCall {
                func_name: func_name.clone(),
                args: args.iter().map(|a| a.strip_qualifier()).collect(),
            },
            _ => self.clone(),
        }
    }

    /// Return a copy of this expression with table aliases rewritten.
    ///
    /// Replaces `old_left` → `new_left` and `old_right` → `new_right` in
    /// all `ColumnRef` nodes.  Used to adapt join conditions to the
    /// aliases present in each part of the join delta query.
    pub fn rewrite_aliases(
        &self,
        old_left: &str,
        new_left: &str,
        old_right: &str,
        new_right: &str,
    ) -> Expr {
        match self {
            Expr::ColumnRef {
                table_alias: Some(alias),
                column_name,
            } => {
                let new_alias = if alias == old_left {
                    new_left
                } else if alias == old_right {
                    new_right
                } else {
                    alias.as_str()
                };
                Expr::ColumnRef {
                    table_alias: Some(new_alias.to_string()),
                    column_name: column_name.clone(),
                }
            }
            Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
                op: op.clone(),
                left: Box::new(left.rewrite_aliases(old_left, new_left, old_right, new_right)),
                right: Box::new(right.rewrite_aliases(old_left, new_left, old_right, new_right)),
            },
            Expr::FuncCall { func_name, args } => Expr::FuncCall {
                func_name: func_name.clone(),
                args: args
                    .iter()
                    .map(|a| a.rewrite_aliases(old_left, new_left, old_right, new_right))
                    .collect(),
            },
            _ => self.clone(),
        }
    }
}

/// Aggregate function types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    CountStar,
    Sum,
    Avg,
    Min,
    Max,
    /// Group-rescan aggregates: any change in the group triggers full re-aggregation.
    BoolAnd,
    BoolOr,
    StringAgg,
    ArrayAgg,
    JsonAgg,
    JsonbAgg,
    BitAnd,
    BitOr,
    BitXor,
    JsonObjectAgg,
    JsonbObjectAgg,
    /// Statistical aggregates — group-rescan strategy.
    StddevPop,
    StddevSamp,
    VarPop,
    VarSamp,
    /// Ordered-set aggregates — group-rescan strategy.
    /// These use WITHIN GROUP (ORDER BY ...) syntax.
    Mode,
    PercentileCont,
    PercentileDisc,
    /// ANY_VALUE aggregate (PG 16+) — group-rescan strategy.
    /// Returns an arbitrary non-null value from the group.
    AnyValue,
    /// SQL/JSON standard aggregate: JSON_OBJECTAGG(key: value ...)
    /// Carries the fully deparsed SQL since the special `key: value` syntax
    /// cannot be reconstructed from function name + arguments alone.
    /// Group-rescan strategy.
    JsonObjectAggStd(String),
    /// SQL/JSON standard aggregate: JSON_ARRAYAGG(expr ...)
    /// Carries the fully deparsed SQL since the inline `ABSENT ON NULL`,
    /// `ORDER BY`, and `RETURNING` clauses differ from regular function syntax.
    /// Group-rescan strategy.
    JsonArrayAggStd(String),
    /// Regression/correlation aggregates — group-rescan strategy.
    /// These are two-argument aggregates: `func(Y, X)`.
    Corr,
    CovarPop,
    CovarSamp,
    RegrAvgx,
    RegrAvgy,
    RegrCount,
    RegrIntercept,
    RegrR2,
    RegrSlope,
    RegrSxx,
    RegrSxy,
    RegrSyy,
    /// XMLAGG aggregate — group-rescan strategy.
    /// Optional ORDER BY is stored in `order_within_group`.
    XmlAgg,
    /// Hypothetical-set aggregates — group-rescan strategy.
    /// These use `WITHIN GROUP (ORDER BY ...)` syntax with one hypothetical value
    /// as the function argument (e.g., `RANK(42) WITHIN GROUP (ORDER BY score)`).
    HypRank,
    HypDenseRank,
    HypPercentRank,
    HypCumeDist,
    /// Complex expression wrapping multiple aggregate calls.
    ///
    /// Created when a SELECT target is an arithmetic or conditional expression
    /// containing nested aggregate function calls, e.g.:
    ///   `100 * SUM(a) / CASE WHEN SUM(b) = 0 THEN NULL ELSE SUM(b) END`
    ///
    /// Stores the fully deparsed SQL of the entire expression (including the
    /// nested aggregate calls). Uses the group-rescan strategy: any change in
    /// the group triggers full re-evaluation of the expression from source data.
    ComplexExpression(String),
    /// User-defined aggregate (CREATE AGGREGATE) — group-rescan strategy.
    ///
    /// Stores the fully deparsed SQL of the aggregate call (including any
    /// FILTER, ORDER BY, WITHIN GROUP, and DISTINCT clauses) since UDAs
    /// may use exotic syntax that cannot be reconstructed from parts.
    /// Any change in the group triggers full re-aggregation from source data.
    UserDefined(String),
    /// F4 (v0.37.0): pgvector element-wise average — algebraic via aux.
    ///
    /// Maintains `__pgt_aux_sum_*` (vector) and `__pgt_aux_count_*` (bigint).
    /// Result = `__pgt_aux_sum / __pgt_aux_count` (element-wise scalar division).
    /// Gate: `pg_trickle.enable_vector_agg = on`.
    VectorAvg,
    /// F4 (v0.37.0): pgvector element-wise sum — algebraically invertible.
    ///
    /// Maintains `__pgt_aux_sum_*` (vector) via running sum.
    /// On INSERT: add element-wise; on DELETE: subtract element-wise.
    /// Gate: `pg_trickle.enable_vector_agg = on`.
    VectorSum,
}

/// Determine the effective output column names for a LATERAL SRF.
///
/// When the query provides `AS alias(c1, c2, ...)`, those names win.
/// Otherwise we infer PostgreSQL's default names for common built-ins that
/// expose structured results through field references like `e.value` or
/// `kv.key` / `kv.value`. For everything else we preserve the existing
/// fallback of using the table alias as the single column name.
pub fn lateral_function_output_columns(
    func_sql: &str,
    alias: &str,
    column_aliases: &[String],
) -> Vec<String> {
    if !column_aliases.is_empty() {
        return column_aliases.to_vec();
    }

    infer_default_lateral_function_columns(func_sql).unwrap_or_else(|| vec![alias.to_string()])
}

fn infer_default_lateral_function_columns(func_sql: &str) -> Option<Vec<String>> {
    let open_paren = func_sql.find('(')?;
    let func_head = func_sql[..open_paren].trim();
    let func_name = func_head
        .rsplit('.')
        .next()
        .unwrap_or(func_head)
        .replace('"', "")
        .to_ascii_lowercase();

    match func_name.as_str() {
        "json_each" | "json_each_text" | "jsonb_each" | "jsonb_each_text" => {
            Some(vec!["key".to_string(), "value".to_string()])
        }
        "json_array_elements"
        | "json_array_elements_text"
        | "jsonb_array_elements"
        | "jsonb_array_elements_text" => Some(vec!["value".to_string()]),
        "json_object_keys" | "jsonb_object_keys" => Some(vec!["key".to_string()]),
        _ => None,
    }
}

impl AggFunc {
    /// Name of the aggregate function for SQL generation.
    pub fn sql_name(&self) -> &'static str {
        match self {
            AggFunc::Count | AggFunc::CountStar => "COUNT",
            AggFunc::Sum => "SUM",
            AggFunc::Avg => "AVG",
            AggFunc::Min => "MIN",
            AggFunc::Max => "MAX",
            AggFunc::BoolAnd => "BOOL_AND",
            AggFunc::BoolOr => "BOOL_OR",
            AggFunc::StringAgg => "STRING_AGG",
            AggFunc::ArrayAgg => "ARRAY_AGG",
            AggFunc::JsonAgg => "JSON_AGG",
            AggFunc::JsonbAgg => "JSONB_AGG",
            AggFunc::BitAnd => "BIT_AND",
            AggFunc::BitOr => "BIT_OR",
            AggFunc::BitXor => "BIT_XOR",
            AggFunc::JsonObjectAgg => "JSON_OBJECT_AGG",
            AggFunc::JsonbObjectAgg => "JSONB_OBJECT_AGG",
            AggFunc::JsonObjectAggStd(_) => "JSON_OBJECTAGG",
            AggFunc::JsonArrayAggStd(_) => "JSON_ARRAYAGG",
            AggFunc::StddevPop => "STDDEV_POP",
            AggFunc::StddevSamp => "STDDEV_SAMP",
            AggFunc::VarPop => "VAR_POP",
            AggFunc::VarSamp => "VAR_SAMP",
            AggFunc::AnyValue => "ANY_VALUE",
            AggFunc::Mode => "MODE",
            AggFunc::PercentileCont => "PERCENTILE_CONT",
            AggFunc::PercentileDisc => "PERCENTILE_DISC",
            AggFunc::Corr => "CORR",
            AggFunc::CovarPop => "COVAR_POP",
            AggFunc::CovarSamp => "COVAR_SAMP",
            AggFunc::RegrAvgx => "REGR_AVGX",
            AggFunc::RegrAvgy => "REGR_AVGY",
            AggFunc::RegrCount => "REGR_COUNT",
            AggFunc::RegrIntercept => "REGR_INTERCEPT",
            AggFunc::RegrR2 => "REGR_R2",
            AggFunc::RegrSlope => "REGR_SLOPE",
            AggFunc::RegrSxx => "REGR_SXX",
            AggFunc::RegrSxy => "REGR_SXY",
            AggFunc::RegrSyy => "REGR_SYY",
            AggFunc::XmlAgg => "XMLAGG",
            AggFunc::HypRank => "RANK",
            AggFunc::HypDenseRank => "DENSE_RANK",
            AggFunc::HypPercentRank => "PERCENT_RANK",
            AggFunc::HypCumeDist => "CUME_DIST",
            AggFunc::ComplexExpression(_) => "COMPLEX_EXPRESSION",
            AggFunc::UserDefined(_) => "USER_DEFINED",
            AggFunc::VectorAvg => "avg",
            AggFunc::VectorSum => "sum",
        }
    }

    /// Returns true for aggregates that are maintained algebraically using
    /// auxiliary columns on the stream table.
    ///
    /// - **AVG**: stores `__pgt_aux_sum_*` and `__pgt_aux_count_*`;
    ///   `new_avg = (old_sum + Δsum) / (old_count + Δcount)`.
    /// - **STDDEV/VAR**: additionally stores `__pgt_aux_sum2_*` (sum of squares);
    ///   `var_pop = (n·sum2 − sum²) / n²`, etc.
    /// - **CORR/COVAR/REGR_***: stores cross-product auxiliary columns
    ///   `__pgt_aux_sumx_*`, `__pgt_aux_sumy_*`, `__pgt_aux_sumxy_*`,
    ///   `__pgt_aux_sumx2_*`, `__pgt_aux_sumy2_*` for O(1) algebraic maintenance.
    pub fn is_algebraic_via_aux(&self) -> bool {
        matches!(
            self,
            AggFunc::Avg
                | AggFunc::StddevPop
                | AggFunc::StddevSamp
                | AggFunc::VarPop
                | AggFunc::VarSamp
                | AggFunc::Corr
                | AggFunc::CovarPop
                | AggFunc::CovarSamp
                | AggFunc::RegrAvgx
                | AggFunc::RegrAvgy
                | AggFunc::RegrCount
                | AggFunc::RegrIntercept
                | AggFunc::RegrR2
                | AggFunc::RegrSlope
                | AggFunc::RegrSxx
                | AggFunc::RegrSxy
                | AggFunc::RegrSyy
        )
    }

    /// Returns true for aggregates that need a sum-of-squares auxiliary
    /// column (`__pgt_aux_sum2_*`) in addition to sum and count.
    pub fn needs_sum_of_squares(&self) -> bool {
        matches!(
            self,
            AggFunc::StddevPop | AggFunc::StddevSamp | AggFunc::VarPop | AggFunc::VarSamp
        )
    }

    /// Returns true for two-argument regression/correlation aggregates
    /// that need cross-product auxiliary columns (P3-2).
    ///
    /// These aggregates take `(Y, X)` arguments and require:
    /// - `__pgt_aux_sumx_*`: running SUM(X)
    /// - `__pgt_aux_sumy_*`: running SUM(Y)
    /// - `__pgt_aux_sumxy_*`: running SUM(X*Y)
    /// - `__pgt_aux_sumx2_*`: running SUM(X²)
    /// - `__pgt_aux_sumy2_*`: running SUM(Y²)
    ///
    /// Count is shared with `__pgt_aux_count_*` from `is_algebraic_via_aux`.
    pub fn needs_cross_products(&self) -> bool {
        matches!(
            self,
            AggFunc::Corr
                | AggFunc::CovarPop
                | AggFunc::CovarSamp
                | AggFunc::RegrAvgx
                | AggFunc::RegrAvgy
                | AggFunc::RegrCount
                | AggFunc::RegrIntercept
                | AggFunc::RegrR2
                | AggFunc::RegrSlope
                | AggFunc::RegrSxx
                | AggFunc::RegrSxy
                | AggFunc::RegrSyy
        )
    }

    /// Returns true for aggregates that use the group-rescan strategy:
    /// any change in a group triggers full re-aggregation from source data.
    pub fn is_group_rescan(&self) -> bool {
        matches!(
            self,
            AggFunc::BoolAnd
                | AggFunc::BoolOr
                | AggFunc::StringAgg
                | AggFunc::ArrayAgg
                | AggFunc::JsonAgg
                | AggFunc::JsonbAgg
                | AggFunc::BitAnd
                | AggFunc::BitOr
                | AggFunc::BitXor
                | AggFunc::JsonObjectAgg
                | AggFunc::JsonbObjectAgg
                | AggFunc::JsonObjectAggStd(_)
                | AggFunc::JsonArrayAggStd(_)
                | AggFunc::AnyValue
                | AggFunc::Mode
                | AggFunc::PercentileCont
                | AggFunc::PercentileDisc
                | AggFunc::XmlAgg
                | AggFunc::HypRank
                | AggFunc::HypDenseRank
                | AggFunc::HypPercentRank
                | AggFunc::HypCumeDist
                | AggFunc::ComplexExpression(_)
                | AggFunc::UserDefined(_)
                // F4 (v0.37.0): vector aggregates use group-rescan strategy
                // (re-aggregate affected groups using pgvector's native avg/sum)
                | AggFunc::VectorAvg
                | AggFunc::VectorSum
        )
    }
}

/// An aggregate expression in a GROUP BY query.
#[derive(Debug, Clone)]
pub struct AggExpr {
    pub function: AggFunc,
    pub argument: Option<Expr>,
    pub alias: String,
    /// Whether DISTINCT is applied to the aggregate argument.
    pub is_distinct: bool,
    /// Optional second argument (e.g., separator for STRING_AGG).
    pub second_arg: Option<Expr>,
    /// Optional FILTER (WHERE ...) clause on the aggregate.
    pub filter: Option<Expr>,
    /// Optional WITHIN GROUP (ORDER BY ...) clause for ordered-set aggregates
    /// (MODE, PERCENTILE_CONT, PERCENTILE_DISC).
    pub order_within_group: Option<Vec<SortExpr>>,
    /// Statistical aggregate parse/analyzer metadata, populated for supported
    /// STDDEV/VAR/CORR/COVAR/REGR signatures.
    pub statistical_support: Option<StatisticalAggSupport>,
}

impl AggExpr {
    pub fn statistical_accumulator_type(&self) -> Option<&ResolvedSqlType> {
        self.statistical_support
            .as_ref()
            .and_then(|support| support.accumulator_type.as_ref())
    }
}

/// Resolve the accumulator arithmetic type OID for a supported statistical
/// aggregate from the resolved transition-function argument types.
///
/// `transition_arg_type_oids` are the PostgreSQL `pg_proc.proargtypes` OIDs for
/// the aggregate transition function named by the resolved aggregate overload.
/// For the supported families we only accept arithmetic in `numeric` or
/// `double precision`; anything else remains unresolved and must fall back to
/// FULL maintenance.
pub fn resolve_statistical_accumulator_type_oid(
    function: &AggFunc,
    transition_arg_type_oids: &[u32],
) -> Option<u32> {
    let supported = |oid| oid == PG_NUMERIC_TYPE_OID || oid == PG_FLOAT8_TYPE_OID;

    if function.needs_sum_of_squares() {
        let oid = *transition_arg_type_oids.get(1)?;
        return supported(oid).then_some(oid);
    }

    if function.needs_cross_products() {
        let x_oid = *transition_arg_type_oids.get(1)?;
        let y_oid = *transition_arg_type_oids.get(2)?;
        if x_oid == y_oid && supported(x_oid) {
            return Some(x_oid);
        }
    }

    None
}

/// G12-AGG: Classify the maintenance strategy for a single aggregate expression.
///
/// Returns one of:
/// - `"ALGEBRAIC_INVERTIBLE"` — COUNT, COUNT(*), SUM
/// - `"ALGEBRAIC_VIA_AUX"` — AVG, STDDEV, VAR, CORR, REGR_*
/// - `"SEMI_ALGEBRAIC"` — MIN, MAX
/// - `"GROUP_RESCAN"` — STRING_AGG, ARRAY_AGG, BIT_AND, etc.
///
/// DISTINCT aggregates are always classified as `"GROUP_RESCAN"` because
/// the algebraic and semi-algebraic paths do not support DISTINCT.
pub fn classify_agg_strategy(agg: &AggExpr) -> &'static str {
    if agg.is_distinct {
        return "GROUP_RESCAN";
    }
    match &agg.function {
        AggFunc::Count | AggFunc::CountStar | AggFunc::Sum => "ALGEBRAIC_INVERTIBLE",
        f if f.is_algebraic_via_aux() => "ALGEBRAIC_VIA_AUX",
        AggFunc::Min | AggFunc::Max => "SEMI_ALGEBRAIC",
        _ => "GROUP_RESCAN",
    }
}

#[cfg(test)]
mod statistical_accumulator_type_tests {
    use super::*;

    #[test]
    fn test_statistical_accumulator_type_stddev_numeric() {
        let resolved = resolve_statistical_accumulator_type_oid(
            &AggFunc::StddevPop,
            &[0, PG_NUMERIC_TYPE_OID],
        );
        assert_eq!(resolved, Some(PG_NUMERIC_TYPE_OID));
    }

    #[test]
    fn test_statistical_accumulator_type_corr_float8() {
        let resolved = resolve_statistical_accumulator_type_oid(
            &AggFunc::Corr,
            &[0, PG_FLOAT8_TYPE_OID, PG_FLOAT8_TYPE_OID],
        );
        assert_eq!(resolved, Some(PG_FLOAT8_TYPE_OID));
    }

    #[test]
    fn test_statistical_accumulator_type_rejects_unsupported_signature() {
        let resolved = resolve_statistical_accumulator_type_oid(
            &AggFunc::Corr,
            &[0, PG_NUMERIC_TYPE_OID, PG_FLOAT8_TYPE_OID],
        );
        assert_eq!(resolved, None);
    }
}

#[cfg(test)]
mod classify_agg_strategy_tests {
    use super::*;

    fn make_agg(func: AggFunc, distinct: bool) -> AggExpr {
        AggExpr {
            function: func,
            argument: None,
            alias: "test_agg".to_string(),
            is_distinct: distinct,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        }
    }

    #[test]
    fn test_count_is_algebraic_invertible() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::Count, false)),
            "ALGEBRAIC_INVERTIBLE"
        );
    }

    #[test]
    fn test_count_star_is_algebraic_invertible() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::CountStar, false)),
            "ALGEBRAIC_INVERTIBLE"
        );
    }

    #[test]
    fn test_sum_is_algebraic_invertible() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::Sum, false)),
            "ALGEBRAIC_INVERTIBLE"
        );
    }

    #[test]
    fn test_avg_is_algebraic_via_aux() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::Avg, false)),
            "ALGEBRAIC_VIA_AUX"
        );
    }

    #[test]
    fn test_stddev_pop_is_algebraic_via_aux() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::StddevPop, false)),
            "ALGEBRAIC_VIA_AUX"
        );
    }

    #[test]
    fn test_corr_is_algebraic_via_aux() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::Corr, false)),
            "ALGEBRAIC_VIA_AUX"
        );
    }

    #[test]
    fn test_min_is_semi_algebraic() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::Min, false)),
            "SEMI_ALGEBRAIC"
        );
    }

    #[test]
    fn test_max_is_semi_algebraic() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::Max, false)),
            "SEMI_ALGEBRAIC"
        );
    }

    #[test]
    fn test_string_agg_is_group_rescan() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::StringAgg, false)),
            "GROUP_RESCAN"
        );
    }

    #[test]
    fn test_array_agg_is_group_rescan() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::ArrayAgg, false)),
            "GROUP_RESCAN"
        );
    }

    #[test]
    fn test_json_agg_is_group_rescan() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::JsonAgg, false)),
            "GROUP_RESCAN"
        );
    }

    #[test]
    fn test_bool_and_is_group_rescan() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::BoolAnd, false)),
            "GROUP_RESCAN"
        );
    }

    #[test]
    fn test_user_defined_is_group_rescan() {
        assert_eq!(
            classify_agg_strategy(&make_agg(
                AggFunc::UserDefined("custom_agg(x)".into()),
                false
            )),
            "GROUP_RESCAN",
        );
    }

    #[test]
    fn test_distinct_count_is_group_rescan() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::Count, true)),
            "GROUP_RESCAN"
        );
    }

    #[test]
    fn test_distinct_sum_is_group_rescan() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::Sum, true)),
            "GROUP_RESCAN"
        );
    }

    #[test]
    fn test_mode_is_group_rescan() {
        assert_eq!(
            classify_agg_strategy(&make_agg(AggFunc::Mode, false)),
            "GROUP_RESCAN"
        );
    }
}

#[cfg(test)]
mod is_all_algebraic_agg_tests {
    use super::*;

    fn make_agg(func: AggFunc) -> AggExpr {
        AggExpr {
            function: func,
            argument: None,
            alias: "a".to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        }
    }

    fn scan_child() -> Box<OpTree> {
        Box::new(OpTree::Scan {
            table_name: "t".to_string(),
            schema: "public".to_string(),
            alias: "t".to_string(),
            columns: vec![],
            table_oid: 0,
            pk_columns: vec![],
        })
    }

    #[test]
    fn test_no_aggregate_returns_false() {
        let tree = OpTree::Scan {
            table_name: "t".to_string(),
            schema: "public".to_string(),
            alias: "t".to_string(),
            columns: vec![],
            table_oid: 0,
            pk_columns: vec![],
        };
        assert!(!tree.is_all_algebraic_agg());
    }

    #[test]
    fn test_all_invertible_returns_true() {
        let tree = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![
                make_agg(AggFunc::Count),
                make_agg(AggFunc::Sum),
                make_agg(AggFunc::CountStar),
            ],
            child: scan_child(),
        };
        assert!(tree.is_all_algebraic_agg());
    }

    #[test]
    fn test_avg_via_aux_returns_true() {
        let tree = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![make_agg(AggFunc::Sum), make_agg(AggFunc::Avg)],
            child: scan_child(),
        };
        assert!(tree.is_all_algebraic_agg());
    }

    #[test]
    fn test_min_semi_algebraic_returns_false() {
        let tree = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![make_agg(AggFunc::Sum), make_agg(AggFunc::Min)],
            child: scan_child(),
        };
        assert!(!tree.is_all_algebraic_agg());
    }

    #[test]
    fn test_string_agg_group_rescan_returns_false() {
        let tree = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![make_agg(AggFunc::Sum), make_agg(AggFunc::StringAgg)],
            child: scan_child(),
        };
        assert!(!tree.is_all_algebraic_agg());
    }

    #[test]
    fn test_aggregate_under_project_returns_true() {
        let agg = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![make_agg(AggFunc::Count)],
            child: scan_child(),
        };
        let tree = OpTree::Project {
            expressions: vec![],
            aliases: vec![],
            child: Box::new(agg),
        };
        assert!(tree.is_all_algebraic_agg());
    }
}

/// Sort expression for ORDER BY or window functions.
#[derive(Debug, Clone)]
pub struct SortExpr {
    pub expr: Expr,
    pub ascending: bool,
    pub nulls_first: bool,
}

/// A correlation predicate extracted from a LATERAL subquery's WHERE clause.
///
/// Represents an equality of the form `inner_alias.inner_col = outer_alias.outer_col`
/// where the inner alias belongs to a FROM item inside the subquery and the outer
/// alias references the preceding FROM item (the LATERAL dependency).
///
/// Used by P2-6 to scope inner-change re-execution: instead of re-materializing
/// the entire outer table when inner sources change, we join the change buffer
/// against the outer table using the correlation column.
#[derive(Debug, Clone)]
pub struct CorrelationPredicate {
    /// Column name on the outer (child) table, unqualified.
    pub outer_col: String,
    /// Alias of the inner table inside the subquery.
    pub inner_alias: String,
    /// Column name on the inner table.
    pub inner_col: String,
    /// OID of the inner table (resolved from `inner_alias`).
    pub inner_oid: u32,
}

/// A window function expression: `func(args) OVER (PARTITION BY ... ORDER BY ... frame)`.
#[derive(Debug, Clone)]
pub struct WindowExpr {
    /// Function name (e.g., `row_number`, `rank`, `sum`).
    pub func_name: String,
    /// Function arguments (empty for `ROW_NUMBER()`, `RANK()`; one expr for `SUM(x)` etc.).
    pub args: Vec<Expr>,
    /// PARTITION BY expressions.
    pub partition_by: Vec<Expr>,
    /// ORDER BY expressions within the window frame.
    pub order_by: Vec<SortExpr>,
    /// Window frame clause (e.g., `ROWS BETWEEN 3 PRECEDING AND CURRENT ROW`).
    /// `None` means default frame.
    pub frame_clause: Option<String>,
    /// Output alias for this window expression.
    pub alias: String,
}

impl WindowExpr {
    /// Render the window function back to SQL text.
    pub fn to_sql(&self) -> String {
        let args_sql = if self.args.is_empty() {
            String::new()
        } else {
            self.args
                .iter()
                .map(|a| a.to_sql())
                .collect::<Vec<_>>()
                .join(", ")
        };

        let mut over_parts = Vec::new();
        if !self.partition_by.is_empty() {
            let pb = self
                .partition_by
                .iter()
                .map(|e| e.to_sql())
                .collect::<Vec<_>>()
                .join(", ");
            over_parts.push(format!("PARTITION BY {pb}"));
        }
        if !self.order_by.is_empty() {
            let ob = self
                .order_by
                .iter()
                .map(|s| {
                    let dir = if s.ascending { "ASC" } else { "DESC" };
                    let nulls = if s.ascending {
                        if s.nulls_first { " NULLS FIRST" } else { "" }
                    } else if s.nulls_first {
                        ""
                    } else {
                        " NULLS LAST"
                    };
                    format!("{} {dir}{nulls}", s.expr.to_sql())
                })
                .collect::<Vec<_>>()
                .join(", ");
            over_parts.push(format!("ORDER BY {ob}"));
        }

        if let Some(ref frame) = self.frame_clause {
            over_parts.push(frame.clone());
        }

        format!(
            "{}({}) OVER ({})",
            self.func_name,
            args_sql,
            over_parts.join(" "),
        )
    }
}

/// The operator tree — intermediate representation of a defining query.
#[derive(Debug, Clone)]
pub enum OpTree {
    /// Base table scan.
    Scan {
        table_oid: u32,
        table_name: String,
        schema: String,
        columns: Vec<Column>,
        /// Primary key column names from `pg_constraint` (empty if no PK).
        pk_columns: Vec<String>,
        alias: String,
    },
    /// Projection (SELECT expressions).
    Project {
        expressions: Vec<Expr>,
        aliases: Vec<String>,
        child: Box<OpTree>,
    },
    /// Filter (WHERE clause).
    Filter { predicate: Expr, child: Box<OpTree> },
    /// Inner join.
    InnerJoin {
        condition: Expr,
        left: Box<OpTree>,
        right: Box<OpTree>,
    },
    /// Left outer join.
    LeftJoin {
        condition: Expr,
        left: Box<OpTree>,
        right: Box<OpTree>,
    },
    /// Full outer join.
    FullJoin {
        condition: Expr,
        left: Box<OpTree>,
        right: Box<OpTree>,
    },
    /// GROUP BY with aggregates.
    Aggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<AggExpr>,
        child: Box<OpTree>,
    },
    /// DISTINCT.
    Distinct { child: Box<OpTree> },
    /// UNION ALL.
    UnionAll { children: Vec<OpTree> },
    /// INTERSECT [ALL] of two branches.
    ///
    /// Set semantics (all=false): a row appears if it exists in *both* branches.
    /// Bag semantics (all=true): preserves duplicates up to the minimum count.
    Intersect {
        left: Box<OpTree>,
        right: Box<OpTree>,
        all: bool,
    },
    /// EXCEPT [ALL] (left minus right).
    ///
    /// Set semantics (all=false): a row appears if it exists in the left but not right.
    /// Bag semantics (all=true): preserves duplicates up to max(0, left_count - right_count).
    Except {
        left: Box<OpTree>,
        right: Box<OpTree>,
        all: bool,
    },
    /// A subquery in FROM (either inlined CTE or explicit subselect).
    ///
    /// The child OpTree is the parsed body of the subquery.
    /// Column aliases come from `FROM (...) AS alias(c1, c2, ...)`.
    Subquery {
        alias: String,
        column_aliases: Vec<String>,
        child: Box<OpTree>,
    },
    /// Reference to a CTE whose body is stored in the [`CteRegistry`].
    ///
    /// Multiple `CteScan` nodes can share the same `cte_id`, enabling
    /// the diff engine to differentiate the CTE body only once and
    /// reuse the result for every reference (Tier 2 optimization).
    CteScan {
        /// Index into [`CteRegistry::entries`].
        cte_id: usize,
        /// User-visible CTE name (e.g. `"totals"`).
        cte_name: String,
        /// FROM alias (may differ from `cte_name` if `FROM totals AS t`).
        alias: String,
        /// Output columns of the CTE body (copied at parse time).
        columns: Vec<String>,
        /// Column aliases from the CTE definition: `WITH x(a, b) AS (...)`.
        /// When non-empty, these rename the CTE body's output columns.
        cte_def_aliases: Vec<String>,
        /// Column aliases from `FROM cte AS alias(c1, c2, ...)`.
        /// When non-empty, these override `cte_def_aliases` / `columns` in output.
        column_aliases: Vec<String>,
        /// Parsed body of the CTE, stored for snapshot SQL generation.
        ///
        /// When this `CteScan` appears as a join child, [`build_snapshot_sql`]
        /// recurses into `body` to produce a valid SQL expression for the
        /// CTE's current state instead of using the alias (which is not a
        /// real relation).  `None` only in unit-test constructions that do
        /// not exercise the snapshot path.
        body: Option<Box<OpTree>>,
    },
    /// Recursive CTE: `WITH RECURSIVE name AS (base UNION [ALL] recursive)`.
    ///
    /// The base case (`base`) is a standard OpTree parsed from the
    /// non-recursive term. The `recursive` term contains a
    /// [`RecursiveSelfRef`] node wherever the CTE references itself.
    ///
    /// Stored in the [`CteRegistry`] and referenced via [`CteScan`].
    RecursiveCte {
        /// CTE name (e.g. `"tree"`).
        alias: String,
        /// Output column names.
        columns: Vec<String>,
        /// The non-recursive term (base case).
        base: Box<OpTree>,
        /// The recursive term (references self via `RecursiveSelfRef`).
        recursive: Box<OpTree>,
        /// `true` for `UNION ALL`, `false` for `UNION` (which deduplicates).
        union_all: bool,
    },
    /// Self-reference inside a recursive CTE's recursive term.
    ///
    /// Represents the `FROM <cte_name>` reference within the recursive
    /// term of a `WITH RECURSIVE` CTE. During differentiation, this is
    /// replaced with either the current ST storage (for seed computation)
    /// or the delta CTE (for recursive propagation).
    RecursiveSelfRef {
        /// Name of the recursive CTE being referenced.
        cte_name: String,
        /// FROM alias (may differ from `cte_name`).
        alias: String,
        /// Output columns (same as the `RecursiveCte`'s columns).
        columns: Vec<String>,
    },
    /// Window function evaluation.
    ///
    /// Represents one or more window function expressions applied to
    /// the child's output. All window expressions must share the same
    /// PARTITION BY clause (different ORDER BY is allowed).
    ///
    /// DVM strategy: partition-based recomputation — recompute entire
    /// partitions that contain any changed rows.
    Window {
        /// Window function expressions (at least one).
        window_exprs: Vec<WindowExpr>,
        /// Shared PARTITION BY columns (may be empty for un-partitioned windows).
        partition_by: Vec<Expr>,
        /// Pass-through (non-window) columns: `(expr, alias)`.
        pass_through: Vec<(Expr, String)>,
        /// Child operator producing the window function's input.
        child: Box<OpTree>,
    },
    /// A set-returning function in the FROM clause with implicit LATERAL semantics.
    ///
    /// Examples: `jsonb_array_elements(p.data)`, `unnest(a.tags)`,
    /// `generate_series(1, 10)`.
    ///
    /// The function call is stored as raw SQL because SRFs may reference
    /// columns from the left-hand side of the implicit cross join (LATERAL),
    /// making them impossible to fully decompose at parse time.
    ///
    /// DVM strategy: row-scoped recomputation — for each changed source row,
    /// delete old expansions and re-expand the SRF for the new row values.
    LateralFunction {
        /// The complete function call as SQL text,
        /// e.g. `jsonb_array_elements(p.data->'children')`.
        func_sql: String,
        /// The FROM alias, e.g. `child` from `... AS child`.
        alias: String,
        /// Column aliases from `AS alias(c1, c2)`, if any.
        column_aliases: Vec<String>,
        /// Whether `WITH ORDINALITY` was specified (adds a `bigint` ordinal column).
        with_ordinality: bool,
        /// The left-hand FROM item that this function may reference (LATERAL dependency).
        child: Box<OpTree>,
    },
    /// A LATERAL subquery in the FROM clause.
    ///
    /// The subquery is correlated — it references columns from preceding
    /// FROM items. The subquery body is stored as raw SQL because it
    /// cannot be independently differentiated (it depends on outer row
    /// context).
    ///
    /// DVM strategy: row-scoped recomputation — for each changed outer row,
    /// re-execute the subquery against the new outer row values.
    LateralSubquery {
        /// The complete subquery body as SQL text.
        subquery_sql: String,
        /// The FROM alias (e.g. `latest` from `... AS latest`).
        alias: String,
        /// Column aliases from `AS alias(c1, c2, ...)`, if any.
        column_aliases: Vec<String>,
        /// Output column names determined from the subquery's SELECT list.
        /// Used when `column_aliases` is empty.
        output_cols: Vec<String>,
        /// Whether this is a LEFT JOIN LATERAL (true) or CROSS JOIN LATERAL (false).
        /// LEFT JOIN preserves outer rows even when the subquery returns no rows.
        is_left_join: bool,
        /// Source table OIDs referenced by the subquery body.
        /// Needed for CDC trigger setup.
        subquery_source_oids: Vec<u32>,
        /// Correlation predicates extracted from the subquery WHERE clause (P2-6).
        /// When available, inner-change re-execution is scoped to only outer rows
        /// that correlate with changed inner rows, reducing O(|outer|) to O(delta).
        correlation_predicates: Vec<CorrelationPredicate>,
        /// The left-hand FROM item that this subquery may reference
        /// (LATERAL dependency).
        child: Box<OpTree>,
    },
    /// Semi-join (EXISTS / IN subquery).
    ///
    /// Emits all rows from `left` that have at least one matching row
    /// in `right` according to `condition`. Implements:
    /// - `WHERE EXISTS (SELECT ... FROM right_src WHERE correlation_cond)`
    /// - `WHERE x IN (SELECT col FROM right_src)`
    ///
    /// DVM strategy: two-part delta — left-side changes filtered by existence
    /// check against current right; right-side changes trigger re-evaluation
    /// of affected left rows.
    SemiJoin {
        condition: Expr,
        left: Box<OpTree>,
        right: Box<OpTree>,
    },
    /// Anti-join (NOT EXISTS / NOT IN subquery).
    ///
    /// Emits all rows from `left` that have NO matching row in `right`.
    /// Implements `WHERE NOT EXISTS (...)` and `WHERE x NOT IN (SELECT ...)`.
    ///
    /// DVM strategy: inverse of semi-join — left-side changes filtered by
    /// non-existence check; right-side changes trigger re-evaluation of
    /// affected left rows.
    AntiJoin {
        condition: Expr,
        left: Box<OpTree>,
        right: Box<OpTree>,
    },
    /// Scalar subquery in SELECT list (uncorrelated).
    ///
    /// Represents `SELECT (SELECT agg(...) FROM src) AS alias, ... FROM main`.
    /// The subquery produces a single scalar value that is cross-joined to
    /// every row from `child`. When the subquery's source changes, all
    /// output rows must be recomputed with the new scalar value.
    ScalarSubquery {
        /// Parsed OpTree for the inner scalar query.
        subquery: Box<OpTree>,
        /// Alias for the scalar result column.
        alias: String,
        /// Source table OIDs from the subquery (for CDC).
        subquery_source_oids: Vec<u32>,
        /// The outer query that produces the non-scalar columns.
        child: Box<OpTree>,
    },
    /// Constant-expression SELECT with no FROM clause.
    ///
    /// Used exclusively for the base case of `WITH RECURSIVE` CTEs whose
    /// anchor is a pure constant query such as `SELECT 1 AS id`.  These
    /// anchors have no source tables and therefore produce no delta rows;
    /// the full recursion is driven by the recursive term's source tables.
    ///
    /// The `sql` field holds the verbatim deparsed SQL so that helper
    /// functions that need to embed the anchor into a `WITH RECURSIVE`
    /// block (e.g., the DRed rederivation CTE) have a valid SQL fragment.
    ConstantSelect {
        /// Column names emitted by this node.
        columns: Vec<String>,
        /// Verbatim SQL of the original constant SELECT (no FROM clause).
        sql: String,
    },
}

/// Registry of parsed CTE bodies, shared across the OpTree.
///
/// The parser stores each CTE body here exactly once. Every reference
/// to that CTE in the main query produces a [`OpTree::CteScan`] node
/// pointing back via `cte_id`. The diff engine can then differentiate
/// each body once and cache the result.
#[derive(Debug, Clone, Default)]
pub struct CteRegistry {
    /// `(cte_name, parsed_body)` — index is the `cte_id`.
    pub entries: Vec<(String, OpTree)>,
}

impl CteRegistry {
    /// Collect all base table OIDs referenced in CTE bodies.
    pub fn source_oids(&self) -> Vec<u32> {
        self.entries
            .iter()
            .flat_map(|(_, tree)| tree.source_oids())
            .collect()
    }

    /// Look up a CTE entry by id.
    pub fn get(&self, cte_id: usize) -> Option<&(String, OpTree)> {
        self.entries.get(cte_id)
    }
}

/// The result of parsing a defining query: the main OpTree plus a
/// registry of CTE bodies (empty when the query has no CTEs).
#[derive(Debug, Clone)]
pub struct ParseResult {
    /// The operator tree for the main SELECT.
    pub tree: OpTree,
    /// Registry of CTE bodies referenced by [`OpTree::CteScan`] nodes.
    pub cte_registry: CteRegistry,
    /// Whether the original query contained `WITH RECURSIVE`.
    ///
    /// When `true`, the OpTree may be incomplete — recursive CTE bodies
    /// are not parsed into the tree. Only FULL refresh mode is supported.
    pub has_recursion: bool,
    /// Advisory warnings collected during parsing (e.g. NATURAL JOIN column
    /// drift risk). Emit these once at the call site that presents diagnostics
    /// to the user; downstream parser calls (cache pre-warm, row-id derivation)
    /// should silently discard them.
    pub warnings: Vec<String>,
}

impl ParseResult {
    /// Collect `(table_oid, Vec<column_name>)` for all source tables
    /// referenced across the main tree and CTE bodies.
    ///
    /// Used to populate `pgt_dependencies.columns_used` at creation time
    /// so `detect_schema_change_kind()` can accurately classify DDL events.
    pub fn source_columns_used(&self) -> std::collections::HashMap<u32, Vec<String>> {
        let mut map: std::collections::HashMap<u32, std::collections::HashSet<String>> =
            std::collections::HashMap::new();
        self.tree.collect_source_columns(&mut map);
        for (_, cte_tree) in &self.cte_registry.entries {
            cte_tree.collect_source_columns(&mut map);
        }
        map.into_iter()
            .map(|(oid, set)| {
                let mut cols: Vec<String> = set.into_iter().collect();
                cols.sort();
                (oid, cols)
            })
            .collect()
    }

    /// A-2: Collect source columns from key positions (GROUP BY, JOIN ON,
    /// WHERE) across the main tree and CTE bodies.
    ///
    /// Returns `(table_oid, Vec<column_name>)` pairs for columns whose changes
    /// can affect row identity, grouping, or filtering. Used by the
    /// `changed_cols` bitmask to distinguish key changes from value-only changes.
    pub fn source_key_columns_used(&self) -> std::collections::HashMap<u32, Vec<String>> {
        let mut main_keys = self.tree.source_key_columns_used();
        for (_, cte_tree) in &self.cte_registry.entries {
            let cte_keys = cte_tree.source_key_columns_used();
            for (oid, cols) in cte_keys {
                let entry = main_keys.entry(oid).or_default();
                for col in cols {
                    if !entry.contains(&col) {
                        entry.push(col);
                    }
                }
                entry.sort();
            }
        }
        main_keys
    }

    /// Collect all function names referenced in the defining query (G8.2).
    ///
    /// Used to populate `pgt_stream_tables.functions_used` at creation time
    /// so that DDL hooks can detect `CREATE OR REPLACE FUNCTION` /
    /// `DROP FUNCTION` events that affect this stream table.
    ///
    /// Returns a sorted, deduplicated list of function names (lowercase).
    pub fn functions_used(&self) -> Vec<String> {
        let mut names = std::collections::HashSet::new();
        Self::collect_expr_funcs(&self.tree, &mut names);
        for (_, cte_tree) in &self.cte_registry.entries {
            Self::collect_expr_funcs(cte_tree, &mut names);
        }
        let mut result: Vec<String> = names.into_iter().collect();
        result.sort();
        result
    }

    /// Walk an OpTree recursively collecting function names from Expr nodes.
    fn collect_expr_funcs(tree: &OpTree, names: &mut std::collections::HashSet<String>) {
        match tree {
            OpTree::Scan { .. }
            | OpTree::CteScan { .. }
            | OpTree::RecursiveSelfRef { .. }
            | OpTree::ConstantSelect { .. } => {}
            OpTree::Project {
                expressions, child, ..
            } => {
                for expr in expressions {
                    Self::collect_funcs_from_expr(expr, names);
                }
                Self::collect_expr_funcs(child, names);
            }
            OpTree::Filter { predicate, child } => {
                Self::collect_funcs_from_expr(predicate, names);
                Self::collect_expr_funcs(child, names);
            }
            OpTree::InnerJoin {
                condition,
                left,
                right,
            }
            | OpTree::LeftJoin {
                condition,
                left,
                right,
            }
            | OpTree::FullJoin {
                condition,
                left,
                right,
            }
            | OpTree::SemiJoin {
                condition,
                left,
                right,
            }
            | OpTree::AntiJoin {
                condition,
                left,
                right,
            } => {
                Self::collect_funcs_from_expr(condition, names);
                Self::collect_expr_funcs(left, names);
                Self::collect_expr_funcs(right, names);
            }
            OpTree::Aggregate {
                group_by,
                aggregates,
                child,
            } => {
                for expr in group_by {
                    Self::collect_funcs_from_expr(expr, names);
                }
                for agg in aggregates {
                    // The aggregate function itself
                    names.insert(agg.function.sql_name().to_lowercase());
                    if let Some(arg) = &agg.argument {
                        Self::collect_funcs_from_expr(arg, names);
                    }
                    if let Some(filter) = &agg.filter {
                        Self::collect_funcs_from_expr(filter, names);
                    }
                    if let Some(second) = &agg.second_arg {
                        Self::collect_funcs_from_expr(second, names);
                    }
                }
                Self::collect_expr_funcs(child, names);
            }
            OpTree::Distinct { child }
            | OpTree::Subquery { child, .. }
            | OpTree::LateralFunction { child, .. }
            | OpTree::LateralSubquery { child, .. }
            | OpTree::ScalarSubquery { child, .. } => {
                Self::collect_expr_funcs(child, names);
            }
            OpTree::UnionAll { children } => {
                for c in children {
                    Self::collect_expr_funcs(c, names);
                }
            }
            OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
                Self::collect_expr_funcs(left, names);
                Self::collect_expr_funcs(right, names);
            }
            OpTree::RecursiveCte {
                base, recursive, ..
            } => {
                Self::collect_expr_funcs(base, names);
                Self::collect_expr_funcs(recursive, names);
            }
            OpTree::Window {
                window_exprs,
                partition_by,
                child,
                ..
            } => {
                for we in window_exprs {
                    // Window function name
                    names.insert(we.func_name.to_lowercase());
                    for arg in &we.args {
                        Self::collect_funcs_from_expr(arg, names);
                    }
                    for pb in &we.partition_by {
                        Self::collect_funcs_from_expr(pb, names);
                    }
                    for ob in &we.order_by {
                        Self::collect_funcs_from_expr(&ob.expr, names);
                    }
                }
                for pb in partition_by {
                    Self::collect_funcs_from_expr(pb, names);
                }
                Self::collect_expr_funcs(child, names);
            }
        }
    }

    /// Extract function names from an Expr recursively.
    fn collect_funcs_from_expr(expr: &Expr, names: &mut std::collections::HashSet<String>) {
        match expr {
            Expr::FuncCall { func_name, args } => {
                names.insert(func_name.to_lowercase());
                for arg in args {
                    Self::collect_funcs_from_expr(arg, names);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_funcs_from_expr(left, names);
                Self::collect_funcs_from_expr(right, names);
            }
            // ColumnRef, Literal, Star, Raw: no function calls
            // (Raw may contain functions, but we don't parse them here —
            // the volatility checker handles those separately.)
            _ => {}
        }
    }
}

/// Look through transparent wrapper nodes (Filter, Subquery) to find the
/// underlying operator. Used by `row_id_key_columns()` — a Filter sitting
/// between Project and InnerJoin should not prevent PK-based row_ids.
pub fn unwrap_transparent(op: &OpTree) -> &OpTree {
    match op {
        OpTree::Filter { child, .. } | OpTree::Subquery { child, .. } => unwrap_transparent(child),
        other => other,
    }
}

impl OpTree {
    /// Get the alias for this node (used in CTE naming).
    pub fn alias(&self) -> &str {
        match self {
            OpTree::Scan { alias, .. } => alias,
            OpTree::Project { .. } => "project",
            OpTree::Filter { .. } => "filter",
            OpTree::InnerJoin { .. } => "join",
            OpTree::LeftJoin { .. } => "left_join",
            OpTree::FullJoin { .. } => "full_join",
            OpTree::Aggregate { .. } => "agg",
            OpTree::Distinct { .. } => "distinct",
            OpTree::UnionAll { .. } => "union",
            OpTree::Intersect { .. } => "intersect",
            OpTree::Except { .. } => "except",
            OpTree::Subquery { alias, .. } => alias,
            OpTree::CteScan { alias, .. } => alias,
            OpTree::RecursiveCte { alias, .. } => alias,
            OpTree::RecursiveSelfRef { alias, .. } => alias,
            OpTree::Window { .. } => "window",
            OpTree::LateralFunction { alias, .. } => alias,
            OpTree::LateralSubquery { alias, .. } => alias,
            OpTree::SemiJoin { .. } => "semi_join",
            OpTree::AntiJoin { .. } => "anti_join",
            OpTree::ScalarSubquery { alias, .. } => alias,
            OpTree::ConstantSelect { .. } => "__const",
        }
    }

    /// Return a human-readable name for this node kind (used in error messages).
    pub fn node_kind(&self) -> &str {
        match self {
            OpTree::Scan { .. } => "scan",
            OpTree::Project { .. } => "project",
            OpTree::Filter { .. } => "filter",
            OpTree::InnerJoin { .. } => "inner join",
            OpTree::LeftJoin { .. } => "left join",
            OpTree::FullJoin { .. } => "full join",
            OpTree::Aggregate { .. } => "aggregate",
            OpTree::Distinct { .. } => "distinct",
            OpTree::UnionAll { .. } => "union all",
            OpTree::Intersect { all, .. } => {
                if *all {
                    "intersect all"
                } else {
                    "intersect"
                }
            }
            OpTree::Except { all, .. } => {
                if *all {
                    "except all"
                } else {
                    "except"
                }
            }
            OpTree::Subquery { .. } => "subquery",
            OpTree::CteScan { .. } => "cte scan",
            OpTree::RecursiveCte { .. } => "recursive cte",
            OpTree::RecursiveSelfRef { .. } => "recursive self-reference",
            OpTree::Window { .. } => "window",
            OpTree::LateralFunction { .. } => "lateral function",
            OpTree::LateralSubquery { .. } => "lateral subquery",
            OpTree::SemiJoin { .. } => "semi join",
            OpTree::AntiJoin { .. } => "anti join",
            OpTree::ScalarSubquery { .. } => "scalar subquery",
            OpTree::ConstantSelect { .. } => "constant select",
        }
    }

    /// Returns `true` if the operator tree contains Aggregate or Distinct,
    /// meaning the storage table needs a `__pgt_count BIGINT` auxiliary
    /// column for differential maintenance.
    ///
    /// Recurses through transparent wrappers (`Filter`, `Project`,
    /// `Subquery`) that may sit on top (e.g. HAVING adds a `Filter`
    /// around the `Aggregate`).
    pub fn needs_pgt_count(&self) -> bool {
        match self {
            OpTree::Aggregate { .. } => true,
            OpTree::Distinct { child, .. } => {
                // UNION (dedup) = Distinct { UnionAll }; uses union-dedup count path instead
                !matches!(child.as_ref(), OpTree::UnionAll { .. })
            }
            // Transparent wrappers — delegate to child
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. } => child.needs_pgt_count(),
            // INTERSECT/EXCEPT use guarded legacy/private dual state, not
            // __pgt_count in the FULL storage relation.
            OpTree::Intersect { .. } | OpTree::Except { .. } => false,
            _ => false,
        }
    }

    /// Returns the list of AVG auxiliary columns that need to be added to
    /// the stream table storage for algebraic AVG maintenance.
    ///
    /// For each non-DISTINCT AVG aggregate at the top level, returns a tuple
    /// of `(sum_col_name, count_col_name, arg_sql)`:
    /// - `sum_col_name`: `__pgt_aux_sum_{alias}` — stores running SUM
    /// - `count_col_name`: `__pgt_aux_count_{alias}` — stores running COUNT
    /// - `arg_sql`: the SQL expression for the aggregate argument (e.g. `"amount"`)
    ///
    /// Returns an empty vec if the query has no AVG aggregates or if the
    /// operator tree is not an aggregate query.
    pub fn avg_aux_columns(&self) -> Vec<(String, String, String)> {
        match self {
            OpTree::Aggregate { aggregates, .. } => {
                let mut aux = Vec::new();
                for agg in aggregates {
                    if agg.function.is_algebraic_via_aux() && !agg.is_distinct {
                        let arg_sql = agg
                            .argument
                            .as_ref()
                            .map(|e| e.to_sql())
                            .unwrap_or_else(|| "1".to_string());
                        aux.push((
                            format!("__pgt_aux_sum_{}", agg.alias),
                            format!("__pgt_aux_count_{}", agg.alias),
                            arg_sql,
                        ));
                    }
                }
                aux
            }
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. } => child.avg_aux_columns(),
            _ => Vec::new(),
        }
    }

    /// For each non-DISTINCT STDDEV/VAR aggregate, returns a tuple of
    /// `(sum2_col_name, arg_sql)`:
    /// - `sum2_col_name`: `__pgt_aux_sum2_{alias}` — stores running SUM(x²)
    /// - `arg_sql`: the SQL expression for the aggregate argument
    ///
    /// These are needed in addition to the sum/count from `avg_aux_columns`.
    pub fn sum2_aux_columns(&self) -> Vec<(String, String)> {
        match self {
            OpTree::Aggregate { aggregates, .. } => {
                let mut aux = Vec::new();
                for agg in aggregates {
                    if agg.function.needs_sum_of_squares() && !agg.is_distinct {
                        let arg_sql = agg
                            .argument
                            .as_ref()
                            .map(|e| e.to_sql())
                            .unwrap_or_else(|| "1".to_string());
                        aux.push((format!("__pgt_aux_sum2_{}", agg.alias), arg_sql));
                    }
                }
                aux
            }
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. } => child.sum2_aux_columns(),
            _ => Vec::new(),
        }
    }

    /// For each non-DISTINCT CORR/COVAR/REGR_* aggregate, returns tuples of
    /// `(col_name, arg_sql)` for cross-product auxiliary columns (P3-2):
    ///
    /// - `__pgt_aux_sumx_{alias}`:  running SUM(X)
    /// - `__pgt_aux_sumy_{alias}`:  running SUM(Y)
    /// - `__pgt_aux_sumxy_{alias}`: running SUM(X*Y)
    /// - `__pgt_aux_sumx2_{alias}`: running SUM(X²)
    /// - `__pgt_aux_sumy2_{alias}`: running SUM(Y²)
    ///
    /// (Count is shared with `__pgt_aux_count_*` from `avg_aux_columns`.)
    ///
    /// For regression aggs, argument = Y (first), second_arg = X (second).
    pub fn covar_aux_columns(&self) -> Vec<(String, String)> {
        match self {
            OpTree::Aggregate { aggregates, .. } => {
                let mut aux = Vec::new();
                for agg in aggregates {
                    if agg.function.needs_cross_products() && !agg.is_distinct {
                        let y_sql = agg
                            .argument
                            .as_ref()
                            .map(|e| e.to_sql())
                            .unwrap_or_else(|| "0".to_string());
                        let x_sql = agg
                            .second_arg
                            .as_ref()
                            .map(|e| e.to_sql())
                            .unwrap_or_else(|| "0".to_string());
                        let a = &agg.alias;
                        // Use a combined key for arg_sql: "x_expr|y_expr"
                        aux.push((format!("__pgt_aux_sumx_{a}"), x_sql.clone()));
                        aux.push((format!("__pgt_aux_sumy_{a}"), y_sql.clone()));
                        aux.push((format!("__pgt_aux_sumxy_{a}"), format!("{x_sql}|{y_sql}")));
                        aux.push((format!("__pgt_aux_sumx2_{a}"), x_sql.clone()));
                        aux.push((format!("__pgt_aux_sumy2_{a}"), y_sql));
                    }
                }
                aux
            }
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. } => child.covar_aux_columns(),
            _ => Vec::new(),
        }
    }

    /// Return the analyzed accumulator type for each statistical auxiliary
    /// column. The type comes from PostgreSQL's transition signature rather
    /// than from the source expression's textual type.
    pub fn statistical_aux_types(&self) -> Vec<(String, String)> {
        match self {
            OpTree::Aggregate { aggregates, .. } => {
                let mut types = Vec::new();
                for agg in aggregates {
                    let Some(accumulator_type) = agg.statistical_accumulator_type() else {
                        continue;
                    };
                    let ty = accumulator_type.sql.clone();
                    if agg.function.needs_sum_of_squares() && !agg.is_distinct {
                        types.push((format!("__pgt_aux_sum2_{}", agg.alias), ty.clone()));
                    }
                    if agg.function.needs_cross_products() && !agg.is_distinct {
                        let a = &agg.alias;
                        for suffix in ["sumx", "sumy", "sumxy", "sumx2", "sumy2"] {
                            types.push((format!("__pgt_aux_{suffix}_{a}"), ty.clone()));
                        }
                    }
                }
                types
            }
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. } => child.statistical_aux_types(),
            _ => Vec::new(),
        }
    }

    /// For each non-DISTINCT SUM aggregate,
    /// returns a tuple of `(nonnull_col_name, arg_sql)`:
    /// - `nonnull_col_name`: `__pgt_aux_nonnull_{alias}` — stores running
    ///   count of non-NULL argument values in the group
    /// - `arg_sql`: the SQL expression for the aggregate argument
    ///
    /// The auxiliary is maintained for every SUM because algebraic inversion
    /// cannot distinguish an empty/non-qualifying group from a real zero
    /// without retaining the number of qualifying non-NULL inputs.  FILTER
    /// predicates are folded into the tracked expression so initialization and
    /// delta maintenance count the same rows.
    ///
    /// Returns an empty vec if the operator tree is not an aggregate query.
    pub fn nonnull_aux_columns(&self) -> Vec<(String, String)> {
        match self {
            OpTree::Aggregate { aggregates, .. } => {
                let mut aux = Vec::new();
                for agg in aggregates {
                    if matches!(agg.function, AggFunc::Sum) && !agg.is_distinct {
                        let arg_sql = agg
                            .argument
                            .as_ref()
                            .map(|e| e.to_sql())
                            .unwrap_or_else(|| "0".to_string());
                        let arg_sql = if let Some(filter) = &agg.filter {
                            format!("CASE WHEN {} THEN ({arg_sql}) END", filter.to_sql())
                        } else {
                            arg_sql
                        };
                        aux.push((format!("__pgt_aux_nonnull_{}", agg.alias), arg_sql));
                    }
                }
                aux
            }
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. } => child.nonnull_aux_columns(),
            _ => Vec::new(),
        }
    }

    /// Whether this operator represents a UNION (without ALL) that needs
    /// deduplication counting via `__pgt_count` in a wrapped UNION ALL form.
    pub fn needs_union_dedup_count(&self) -> bool {
        match self {
            OpTree::Distinct { child, .. } => matches!(child.as_ref(), OpTree::UnionAll { .. }),
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. } => child.needs_union_dedup_count(),
            _ => false,
        }
    }

    /// Whether this operator needs dual multiplicity counts
    /// (`__pgt_count_l`, `__pgt_count_r`) for set operation tracking.
    pub fn needs_dual_count(&self) -> bool {
        match self {
            OpTree::Intersect { .. } | OpTree::Except { .. } => true,
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. } => child.needs_dual_count(),
            _ => false,
        }
    }

    /// Extract GROUP BY column names from an aggregate operator.
    ///
    /// Returns `Some(vec!["col1", "col2"])` for `Aggregate` nodes with
    /// GROUP BY columns, `None` for non-aggregate queries or aggregates
    /// without GROUP BY (scalar aggregates).
    ///
    /// For transparent wrappers (`Project`, `Subquery`), delegates to child.
    /// When a `Project` renames GROUP BY columns (e.g., `t.id AS department_id`),
    /// the Project's aliases are used instead of the raw Aggregate names,
    /// matching the storage table's column names.
    pub fn group_by_columns(&self) -> Option<Vec<String>> {
        match self {
            OpTree::Aggregate { group_by, .. } => {
                if group_by.is_empty() {
                    None
                } else {
                    Some(group_by.iter().map(|e| e.output_name()).collect())
                }
            }
            OpTree::Project {
                child,
                aliases,
                expressions,
            } => {
                // If the child is an Aggregate with GROUP BY, resolve the
                // group-by column names through this Project's alias mapping.
                // The storage table uses the Project's aliases (the SELECT
                // output names), not the Aggregate's raw group-by expression
                // names. Without this, `GROUP BY t.id` with alias
                // `department_id` would produce an index on "id" while the
                // storage table column is "department_id".
                if let Some(raw_names) = child.group_by_columns() {
                    let resolved: Vec<String> = raw_names
                        .iter()
                        .map(|raw| {
                            // Handle ordinal GROUP BY (e.g., GROUP BY 1):
                            // resolve the integer position to the Nth alias.
                            if let Ok(pos) = raw.parse::<usize>()
                                && pos >= 1
                                && pos <= aliases.len()
                            {
                                return aliases[pos - 1].clone();
                            }
                            // Find the expression in this Project that
                            // references the raw group-by column, and return
                            // the corresponding alias.
                            for (expr, alias) in expressions.iter().zip(aliases.iter()) {
                                if expr.output_name() == *raw {
                                    return alias.clone();
                                }
                            }
                            // No matching expression — keep the raw name
                            // (shouldn't happen for well-formed trees).
                            raw.clone()
                        })
                        .collect();
                    Some(resolved)
                } else {
                    None
                }
            }
            OpTree::Subquery { child, .. } => child.group_by_columns(),
            _ => None,
        }
    }

    /// Returns `true` if the top-level operator (after transparent wrappers)
    /// is a `Distinct` node. Used by AUTO-IDX-1 to decide whether to create
    /// an index on the DISTINCT output columns.
    pub fn has_distinct(&self) -> bool {
        match self {
            OpTree::Distinct { .. } => true,
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. } => child.has_distinct(),
            _ => false,
        }
    }

    /// Return the column names that should be hashed to produce `__pgt_row_id`.
    ///
    /// This mirrors the hash generation in each operator's diff function:
    /// - **Scan**: non-nullable (heuristic PK) columns, or all if none
    /// - **Aggregate**: GROUP BY columns  
    /// - **Distinct**: all output columns
    /// - **Filter/Project/Subquery/CteScan**: delegate to child
    /// - **Window**: pass-through (non-window) columns
    /// - **Join/UnionAll/RecursiveCte**: returns `None` (no simple column list)
    ///
    /// Returns `None` for operators whose row-id computation is too complex
    /// to express as a simple column-hash expression (e.g., joins combine
    /// two sub-hashes).  Callers should fall back to a generic hash.
    pub fn row_id_key_columns(&self) -> Option<Vec<String>> {
        match self {
            OpTree::Scan {
                columns,
                pk_columns,
                ..
            } => {
                // Use PK columns if available (matches the CDC trigger identity).
                if !pk_columns.is_empty() {
                    return Some(pk_columns.clone());
                }
                // Keyless table (S10): use all columns as content hash key,
                // matching the all-column identity computed by the CDC trigger.
                Some(columns.iter().map(|c| c.name.clone()).collect())
            }
            OpTree::Filter { child, .. } => child.row_id_key_columns(),
            OpTree::Project {
                child,
                aliases,
                expressions,
            } => {
                // For join children, use PK-based row_ids for stability.
                // Content-based hashes (all columns) break when both join
                // sides change simultaneously — the DELETE hash is computed
                // with the OTHER side's CURRENT values, not the OLD values
                // stored in the ST. PK columns are stable across value
                // changes, so DELETE hashes always match the stored row_id.
                //
                // Look through transparent wrappers (Filter, Subquery) to
                // find the underlying join node. Q15 has Project > Filter >
                // InnerJoin — the Filter must not prevent PK-based row_ids.
                let unwrapped = unwrap_transparent(child);
                if matches!(
                    unwrapped,
                    OpTree::InnerJoin { .. } | OpTree::LeftJoin { .. } | OpTree::FullJoin { .. }
                ) {
                    let pk_aliases = join_pk_aliases(expressions, aliases, unwrapped);
                    return Some(pk_aliases.unwrap_or_else(|| aliases.clone()));
                }
                // Lateral subqueries have a complete content identity. SRF
                // bodies are FULL-only when their result cannot be inspected;
                // returning no key here makes FULL refresh use its generic
                // row_to_json identity instead of trying to encode JSONB.
                if matches!(unwrapped, OpTree::LateralSubquery { .. }) {
                    return Some(aliases.clone());
                }
                // For semi-join/anti-join children (EXISTS / NOT EXISTS / IN),
                // use all projected output columns as a content hash.  A semi-join
                // filters the left side without changing its cardinality or
                // introducing new columns, so the projected output is a stable
                // identifier as long as it is unique — which it typically is
                // (the left table's PK columns are usually projected).  Both the
                // FULL refresh (hash from projected columns) and  the DIFFERENTIAL
                // refresh (row_id recomputed in diff_project) then use the same
                // formula, preventing the row_id mismatch that would leave stale
                // rows in the stream table after a differential refresh.
                if matches!(unwrapped, OpTree::SemiJoin { .. } | OpTree::AntiJoin { .. }) {
                    return Some(aliases.clone());
                }
                // Project may drop or rename columns — map child's key
                // column names through the aliasing, then verify they're in
                // the output. E.g., Aggregate GROUP BY "name" → Project
                // alias "region" at the same position.
                let child_out = child.output_columns();
                // When a Project narrows a CTE scan (e.g., SELECT id, name
                // FROM cte where the CTE produces [id, parent_id, name]),
                // positional mapping is unreliable — child_out[1] is
                // "parent_id" but aliases[1] is "name". In that specific
                // CTE case, fall back to content-hashing the projected
                // columns, which matches the CteScan wrapper's formula.
                //
                // For other narrowing projections (notably the EC-03 window
                // subquery-lift rewrite), hashing only the projected aliases
                // SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
                // is unsafe because the derived output may not be unique
                // (e.g., CASE/CAST/COALESCE over row_number()) and initial
                // population would hit duplicate __pgt_row_id values. Those
                // shapes must fall back to the generic row_number()-based
                // hash via `None` instead.
                //
                // Exception: Project(Filter(Scan)) where the Filter exposes
                // WHERE-clause columns in its output (needed by the predicate
                // but not in the SELECT list). Standard positional mapping is
                // unreliable here, but we can still recover the correct
                // PK-based row_id by mapping child key columns through the
                // explicit Project expressions. This case is critical for
                // queries like `SELECT id, val AS vb FROM t WHERE side >= 2`
                // where the differential refresh uses PK-based hash(id) and
                // the full refresh must produce the same value.
                if child_out.len() != aliases.len() {
                    if matches!(unwrapped, OpTree::CteScan { .. }) {
                        return Some(aliases.clone());
                    }
                    // Try expression-based key column mapping: for each child
                    // key column, find the Project expression that outputs it
                    // (as a plain ColumnRef) and map to the corresponding alias.
                    if let Some(child_keys) = child.row_id_key_columns() {
                        let mapped: Vec<String> = child_keys
                            .iter()
                            .filter_map(|k| {
                                let pos = expressions.iter().position(|expr| {
                                    matches!(
                                        expr,
                                        Expr::ColumnRef { column_name, .. }
                                            if column_name == k
                                    )
                                })?;
                                aliases.get(pos).cloned()
                            })
                            .collect();
                        let out = self.output_columns();
                        // A nested narrowing projection over a prior
                        // projection may retain only part of a composite
                        // child key. Use the retained columns so full and
                        // differential refreshes share the same row-id
                        // expression; keyless storage handles any duplicate
                        // row IDs that result.
                        let is_nested_projection = matches!(
                            child.as_ref(),
                            OpTree::Subquery { child: nested, .. }
                                if matches!(nested.as_ref(), OpTree::Project { .. })
                        );
                        if !mapped.is_empty()
                            && (mapped.len() == child_keys.len() || is_nested_projection)
                            && mapped.iter().all(|m| out.contains(m))
                        {
                            return Some(mapped);
                        }
                    }
                    return None;
                }
                match child.row_id_key_columns() {
                    Some(keys) => {
                        let mapped: Vec<String> = keys
                            .iter()
                            .map(|k| {
                                if let Some(pos) = child_out.iter().position(|c| c == k) {
                                    aliases.get(pos).cloned().unwrap_or_else(|| k.clone())
                                } else {
                                    k.clone()
                                }
                            })
                            .collect();
                        let out = self.output_columns();
                        if mapped.iter().all(|k| out.contains(k)) {
                            Some(mapped)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            OpTree::Distinct { child } => {
                // Distinct hashes all child output columns.
                Some(child.output_columns())
            }
            OpTree::Intersect { left, .. } | OpTree::Except { left, .. } => {
                // INTERSECT/EXCEPT hash all output columns (like Distinct).
                Some(left.output_columns())
            }
            OpTree::Aggregate { group_by, .. } => {
                let cols: Vec<String> = group_by.iter().map(|e| e.output_name()).collect();
                if cols.is_empty() {
                    // Scalar aggregate: an empty key is the complete singleton identity.
                    Some(Vec::new())
                } else {
                    Some(cols)
                }
            }
            OpTree::Subquery { child, .. } => child.row_id_key_columns(),
            OpTree::CteScan { columns, .. } => {
                // CTE scan: use all CTE output columns as content hash.
                // This matches the intermediate aggregate's row_id formula
                // when the CTE body contains an aggregate.
                Some(columns.clone())
            }
            OpTree::Window { .. } => {
                // Window functions like RANK/DENSE_RANK can produce identical
                // output rows (tied values). Fall back to row_number-based
                // hash in the initial population to guarantee uniqueness.
                // The window diff operator computes its own unique row_ids
                // during partition recomputation.
                None
            }
            OpTree::LateralFunction { .. } => {
                // SRF expansions have no natural primary key. Let callers
                // use the generic complete-row identity path.
                None
            }
            OpTree::LateralSubquery { .. } => {
                // Use content-hash for row IDs so that the initial population
                // and differential refresh produce consistent __pgt_row_id values.
                let cols = self.output_columns();
                if cols.is_empty() { None } else { Some(cols) }
            }
            OpTree::ScalarSubquery { child, .. } => {
                // Scalar subquery row identity comes from the visible outer
                // child columns only. The scalar value itself changes for all
                // rows simultaneously and is not part of row identity.
                //
                // Use the outer child's visible output columns rather than
                // its internal row-id strategy, because the full-refresh path
                // can only hash columns present in the outer SELECT. The
                // differential operator recomputes the same visible-column
                // hash for both outer-row deltas and scalar-change rewrites.
                let cols = child.output_columns();
                if cols.is_empty() { None } else { Some(cols) }
            }
            // Join, UnionAll, RecursiveCte: complex hash, no simple column list
            _ => None,
        }
    }

    /// Get the output columns of this operator node.
    pub fn output_columns(&self) -> Vec<String> {
        match self {
            OpTree::Scan { columns, .. } => columns.iter().map(|c| c.name.clone()).collect(),
            OpTree::Project { aliases, .. } => aliases.clone(),
            OpTree::Filter { child, .. } => child.output_columns(),
            OpTree::InnerJoin { left, right, .. }
            | OpTree::LeftJoin { left, right, .. }
            | OpTree::FullJoin { left, right, .. } => {
                let mut cols = left.output_columns();
                cols.extend(right.output_columns());
                cols
            }
            OpTree::Aggregate {
                group_by,
                aggregates,
                ..
            } => {
                let mut cols: Vec<String> = group_by.iter().map(|e| e.output_name()).collect();
                cols.extend(aggregates.iter().map(|a| a.alias.clone()));
                cols
            }
            OpTree::Distinct { child } => child.output_columns(),
            OpTree::UnionAll { children } => children
                .first()
                .map(|c| c.output_columns())
                .unwrap_or_default(),
            OpTree::Intersect { left, .. } | OpTree::Except { left, .. } => left.output_columns(),
            OpTree::Subquery {
                column_aliases,
                child,
                ..
            } => {
                if column_aliases.is_empty() {
                    child.output_columns()
                } else {
                    column_aliases.clone()
                }
            }
            OpTree::CteScan {
                columns,
                cte_def_aliases,
                column_aliases,
                ..
            } => {
                if !column_aliases.is_empty() {
                    column_aliases.clone()
                } else if !cte_def_aliases.is_empty() {
                    cte_def_aliases.clone()
                } else {
                    columns.clone()
                }
            }
            OpTree::RecursiveCte { columns, .. } => columns.clone(),
            OpTree::RecursiveSelfRef { columns, .. } => columns.clone(),
            OpTree::Window {
                window_exprs,
                pass_through,
                ..
            } => {
                let mut cols: Vec<String> = pass_through
                    .iter()
                    .map(|(_, alias)| alias.clone())
                    .collect();
                cols.extend(window_exprs.iter().map(|w| w.alias.clone()));
                cols
            }
            OpTree::LateralFunction {
                func_sql,
                alias,
                column_aliases,
                with_ordinality,
                child,
                ..
            } => {
                // Output = child columns + SRF result columns + optional ordinality
                let mut cols = child.output_columns();
                cols.extend(lateral_function_output_columns(
                    func_sql,
                    alias,
                    column_aliases,
                ));
                if *with_ordinality {
                    cols.push("ordinality".to_string());
                }
                cols
            }
            OpTree::LateralSubquery {
                column_aliases,
                output_cols,
                child,
                ..
            } => {
                // Output = child columns + subquery result columns
                let mut cols = child.output_columns();
                if column_aliases.is_empty() {
                    cols.extend(output_cols.clone());
                } else {
                    cols.extend(column_aliases.clone());
                }
                cols
            }
            OpTree::SemiJoin { left, .. } | OpTree::AntiJoin { left, .. } => {
                // Semi/anti-join only emits left-side columns
                left.output_columns()
            }
            OpTree::ScalarSubquery { alias, child, .. } => {
                // Outer columns + scalar result column
                let mut cols = child.output_columns();
                cols.push(alias.clone());
                cols
            }
            OpTree::ConstantSelect { columns, .. } => columns.clone(),
        }
    }

    /// Check whether this tree contains a join whose output does not include
    /// primary key columns from both sides.
    ///
    /// When this returns `true`, the `__pgt_row_id` hash computed from
    /// output columns cannot uniquely identify every join result row. The
    /// storage table must use a non-unique index on `__pgt_row_id` and the
    /// keyless refresh strategy (CTID-based deletion) to handle duplicates.
    ///
    /// Returns `false` when:
    /// - PKs from both sides are in the output (unique by construction), or
    /// - One side's PK is missing but the join condition equates the missing
    ///   side's PK (many-to-one join: each row from the covered side matches
    ///   at most one row from the uncovered side, so the covered PK suffices).
    pub fn has_incomplete_join_pk(&self) -> bool {
        match self {
            OpTree::Project {
                expressions,
                aliases,
                child,
            } => {
                let unwrapped = unwrap_transparent(child);
                if matches!(
                    unwrapped,
                    OpTree::InnerJoin { .. } | OpTree::LeftJoin { .. } | OpTree::FullJoin { .. }
                ) && (join_pk_aliases(expressions, aliases, unwrapped).is_none()
                    || full_join_has_nullable_key(unwrapped))
                {
                    // At least one side's PK is missing from the output.
                    // Check if the join is many-to-one (the uncovered side
                    // is joined on its PK), making the covered PK sufficient.
                    if full_join_has_nullable_key(unwrapped)
                        || !is_many_to_one_join(expressions, aliases, unwrapped)
                    {
                        return true;
                    }
                }
                child.has_incomplete_join_pk()
            }
            OpTree::Filter { child, .. }
            | OpTree::Distinct { child }
            | OpTree::Subquery { child, .. } => child.has_incomplete_join_pk(),
            OpTree::Aggregate { child, .. } => child.has_incomplete_join_pk(),
            OpTree::InnerJoin { left, right, .. }
            | OpTree::LeftJoin { left, right, .. }
            | OpTree::FullJoin { left, right, .. } => {
                left.has_incomplete_join_pk() || right.has_incomplete_join_pk()
            }
            OpTree::UnionAll { children } => children.iter().any(|c| c.has_incomplete_join_pk()),
            _ => false,
        }
    }

    /// Collect all base table OIDs referenced in this tree.
    pub fn source_oids(&self) -> Vec<u32> {
        match self {
            OpTree::Scan { table_oid, .. } => vec![*table_oid],
            OpTree::Project { child, .. }
            | OpTree::Filter { child, .. }
            | OpTree::Distinct { child } => child.source_oids(),
            OpTree::Aggregate { child, .. } => child.source_oids(),
            OpTree::InnerJoin { left, right, .. }
            | OpTree::LeftJoin { left, right, .. }
            | OpTree::FullJoin { left, right, .. }
            | OpTree::Intersect { left, right, .. }
            | OpTree::Except { left, right, .. } => {
                let mut oids = left.source_oids();
                oids.extend(right.source_oids());
                oids
            }
            OpTree::UnionAll { children } => {
                children.iter().flat_map(|c| c.source_oids()).collect()
            }
            OpTree::Subquery { child, .. } => child.source_oids(),
            // CteScan OIDs are resolved via the CteRegistry at diff time
            OpTree::CteScan { .. } => vec![],
            OpTree::RecursiveCte {
                base, recursive, ..
            } => {
                let mut oids = base.source_oids();
                oids.extend(recursive.source_oids());
                oids.sort_unstable();
                oids.dedup();
                oids
            }
            // Self-references don't contribute base table OIDs
            OpTree::RecursiveSelfRef { .. } => vec![],
            OpTree::Window { child, .. } => child.source_oids(),
            OpTree::LateralFunction { child, .. } => child.source_oids(),
            OpTree::LateralSubquery {
                subquery_source_oids,
                child,
                ..
            } => {
                let mut oids = child.source_oids();
                oids.extend(subquery_source_oids.iter());
                oids.sort_unstable();
                oids.dedup();
                oids
            }
            OpTree::SemiJoin { left, right, .. } | OpTree::AntiJoin { left, right, .. } => {
                let mut oids = left.source_oids();
                oids.extend(right.source_oids());
                oids.sort_unstable();
                oids.dedup();
                oids
            }
            OpTree::ScalarSubquery {
                subquery_source_oids,
                child,
                ..
            } => {
                let mut oids = child.source_oids();
                oids.extend(subquery_source_oids.iter());
                oids.sort_unstable();
                oids.dedup();
                oids
            }
            // Constant anchors have no source tables — they never produce delta rows.
            OpTree::ConstantSelect { .. } => vec![],
        }
    }

    /// G12-AGG: Classify the maintenance strategy for each aggregate in the
    /// tree.  Returns a list of `(alias, strategy)` pairs.
    ///
    /// Strategies:
    /// - `"ALGEBRAIC_INVERTIBLE"` — COUNT, COUNT(*), SUM: O(1) algebraic delta.
    /// - `"ALGEBRAIC_VIA_AUX"` — AVG, STDDEV, VAR, CORR, REGR_*: maintained via
    ///   auxiliary columns on the storage table.
    /// - `"SEMI_ALGEBRAIC"` — MIN, MAX: algebraic for inserts, group-rescan on
    ///   extremum deletion.
    /// - `"GROUP_RESCAN"` — STRING_AGG, ARRAY_AGG, JSON_AGG, etc.: any change
    ///   in the group triggers full re-aggregation from source data.
    ///
    /// Returns an empty vec when there is no `Aggregate` node.
    pub fn aggregate_strategies(&self) -> Vec<(String, &'static str)> {
        match self {
            OpTree::Aggregate { aggregates, .. } => aggregates
                .iter()
                .map(|agg| {
                    let strategy = classify_agg_strategy(agg);
                    (agg.alias.clone(), strategy)
                })
                .collect(),
            OpTree::Project { child, .. }
            | OpTree::Filter { child, .. }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. }
            | OpTree::Distinct { child } => child.aggregate_strategies(),
            _ => Vec::new(),
        }
    }

    /// G12-AGG: Return the names of group-rescan aggregates in the tree.
    /// Used to emit a warning at `create_stream_table` time.
    pub fn group_rescan_aggregate_names(&self) -> Vec<String> {
        self.aggregate_strategies()
            .into_iter()
            .filter(|(_, strategy)| *strategy == "GROUP_RESCAN")
            .map(|(alias, _)| alias)
            .collect()
    }

    /// DIAG-2: Return the names of algebraic aggregates in the tree.
    /// Used to emit a low-cardinality warning at `create_stream_table` time.
    pub fn algebraic_aggregate_names(&self) -> Vec<String> {
        self.aggregate_strategies()
            .into_iter()
            .filter(|(_, strategy)| {
                *strategy == "ALGEBRAIC_INVERTIBLE"
                    || *strategy == "ALGEBRAIC_VIA_AUX"
                    || *strategy == "SEMI_ALGEBRAIC"
            })
            .map(|(alias, _)| alias)
            .collect()
    }

    /// B-1: Check whether the tree has an Aggregate node AND all its
    /// aggregates use algebraically invertible strategies (ALGEBRAIC_INVERTIBLE
    /// or ALGEBRAIC_VIA_AUX).  SEMI_ALGEBRAIC (MIN/MAX) is excluded because
    /// it can require group-rescan on extremum deletion.
    ///
    /// Returns `true` only when every aggregate in the tree is fully
    /// algebraic, enabling the explicit DML fast-path at refresh time.
    pub fn is_all_algebraic_agg(&self) -> bool {
        let strategies = self.aggregate_strategies();
        if strategies.is_empty() {
            return false; // No aggregates — not eligible
        }
        strategies.iter().all(|(_, strategy)| {
            *strategy == "ALGEBRAIC_INVERTIBLE" || *strategy == "ALGEBRAIC_VIA_AUX"
        })
    }

    /// Collect `(table_oid, Vec<column_name>)` pairs from all Scan nodes.
    ///
    /// When the same table appears in multiple Scan nodes (self-join), the
    /// column lists are merged (union of column names). This is used to
    /// populate `pgt_dependencies.columns_used` at creation time.
    pub fn source_columns_used(&self) -> std::collections::HashMap<u32, Vec<String>> {
        let mut map: std::collections::HashMap<u32, std::collections::HashSet<String>> =
            std::collections::HashMap::new();
        self.collect_source_columns(&mut map);
        map.into_iter()
            .map(|(oid, set)| {
                let mut cols: Vec<String> = set.into_iter().collect();
                cols.sort();
                (oid, cols)
            })
            .collect()
    }

    /// A-2: Collect source columns from "key" positions — GROUP BY expressions,
    /// JOIN ON conditions, and Filter WHERE predicates.
    ///
    /// Returns `(table_oid, Vec<column_name>)` pairs where columns appear in key
    /// contexts. A change to a key column can affect row identity/grouping,
    /// whereas a change to a value-only column (only in SELECT / aggregate input)
    /// cannot move a row to a different group.
    ///
    /// Used by the changed_cols bitmask infrastructure to distinguish key changes
    /// from value-only changes.
    pub fn source_key_columns_used(&self) -> std::collections::HashMap<u32, Vec<String>> {
        // Step 1: Collect column names from key-position expressions.
        let mut key_names: std::collections::HashSet<String> = std::collections::HashSet::new();
        self.collect_key_column_names(&mut key_names);

        // Step 2: Build a mapping from table_alias → (table_oid, column_names).
        let mut alias_map: std::collections::HashMap<
            String,
            (u32, std::collections::HashSet<String>),
        > = std::collections::HashMap::new();
        self.collect_scan_alias_map(&mut alias_map);

        // Step 3: Intersect key names with Scan columns per table.
        // A column name in key_names matches a Scan column if:
        // - It appears in a ColumnRef with a matching table_alias, OR
        // - It appears unqualified and matches a Scan column name.
        let mut result_map: std::collections::HashMap<u32, std::collections::HashSet<String>> =
            std::collections::HashMap::new();

        for (oid, scan_cols) in alias_map.values() {
            for kn in &key_names {
                if scan_cols.contains(kn) {
                    result_map.entry(*oid).or_default().insert(kn.clone());
                }
            }
        }

        result_map
            .into_iter()
            .map(|(oid, set)| {
                let mut cols: Vec<String> = set.into_iter().collect();
                cols.sort();
                (oid, cols)
            })
            .collect()
    }

    /// Recursive helper — collects column names from GROUP BY, JOIN ON, and
    /// Filter WHERE expressions (key positions that affect row identity).
    fn collect_key_column_names(&self, names: &mut std::collections::HashSet<String>) {
        match self {
            OpTree::Aggregate {
                group_by, child, ..
            } => {
                for expr in group_by {
                    Self::collect_column_names_from_expr(expr, names);
                }
                child.collect_key_column_names(names);
            }
            OpTree::InnerJoin {
                condition,
                left,
                right,
            }
            | OpTree::LeftJoin {
                condition,
                left,
                right,
            }
            | OpTree::FullJoin {
                condition,
                left,
                right,
            } => {
                Self::collect_column_names_from_expr(condition, names);
                left.collect_key_column_names(names);
                right.collect_key_column_names(names);
            }
            OpTree::Filter { predicate, child } => {
                Self::collect_column_names_from_expr(predicate, names);
                child.collect_key_column_names(names);
            }
            OpTree::Project { child, .. }
            | OpTree::Distinct { child }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. }
            | OpTree::LateralFunction { child, .. }
            | OpTree::LateralSubquery { child, .. }
            | OpTree::ScalarSubquery { child, .. } => {
                child.collect_key_column_names(names);
            }
            OpTree::SemiJoin {
                condition,
                left,
                right,
            }
            | OpTree::AntiJoin {
                condition,
                left,
                right,
            } => {
                Self::collect_column_names_from_expr(condition, names);
                left.collect_key_column_names(names);
                right.collect_key_column_names(names);
            }
            OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
                left.collect_key_column_names(names);
                right.collect_key_column_names(names);
            }
            OpTree::UnionAll { children } => {
                for c in children {
                    c.collect_key_column_names(names);
                }
            }
            OpTree::RecursiveCte {
                base, recursive, ..
            } => {
                base.collect_key_column_names(names);
                recursive.collect_key_column_names(names);
            }
            OpTree::Scan { .. }
            | OpTree::CteScan { .. }
            | OpTree::RecursiveSelfRef { .. }
            | OpTree::ConstantSelect { .. } => {}
        }
    }

    /// Extract column names from an expression (GROUP BY, JOIN ON, WHERE).
    fn collect_column_names_from_expr(expr: &Expr, names: &mut std::collections::HashSet<String>) {
        match expr {
            Expr::ColumnRef { column_name, .. } => {
                names.insert(column_name.clone());
            }
            Expr::BinaryOp { left, right, .. } => {
                Self::collect_column_names_from_expr(left, names);
                Self::collect_column_names_from_expr(right, names);
            }
            Expr::FuncCall { args, .. } => {
                for arg in args {
                    Self::collect_column_names_from_expr(arg, names);
                }
            }
            Expr::Literal(_) | Expr::Star { .. } | Expr::Raw(_) => {}
        }
    }

    /// Build a mapping from table alias/name → (table_oid, column_names) from
    /// Scan nodes in the tree.
    fn collect_scan_alias_map(
        &self,
        map: &mut std::collections::HashMap<String, (u32, std::collections::HashSet<String>)>,
    ) {
        match self {
            OpTree::Scan {
                table_oid,
                alias,
                columns,
                ..
            } => {
                let entry = map
                    .entry(alias.clone())
                    .or_insert_with(|| (*table_oid, std::collections::HashSet::new()));
                for col in columns {
                    entry.1.insert(col.name.clone());
                }
            }
            OpTree::Project { child, .. }
            | OpTree::Filter { child, .. }
            | OpTree::Distinct { child }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. }
            | OpTree::LateralFunction { child, .. }
            | OpTree::LateralSubquery { child, .. }
            | OpTree::ScalarSubquery { child, .. } => child.collect_scan_alias_map(map),
            OpTree::Aggregate { child, .. } => child.collect_scan_alias_map(map),
            OpTree::InnerJoin { left, right, .. }
            | OpTree::LeftJoin { left, right, .. }
            | OpTree::FullJoin { left, right, .. }
            | OpTree::SemiJoin { left, right, .. }
            | OpTree::AntiJoin { left, right, .. }
            | OpTree::Intersect { left, right, .. }
            | OpTree::Except { left, right, .. } => {
                left.collect_scan_alias_map(map);
                right.collect_scan_alias_map(map);
            }
            OpTree::UnionAll { children } => {
                for c in children {
                    c.collect_scan_alias_map(map);
                }
            }
            OpTree::RecursiveCte {
                base, recursive, ..
            } => {
                base.collect_scan_alias_map(map);
                recursive.collect_scan_alias_map(map);
            }
            OpTree::CteScan { .. }
            | OpTree::RecursiveSelfRef { .. }
            | OpTree::ConstantSelect { .. } => {}
        }
    }

    /// Recursive helper — collects column names per table OID from Scan nodes.
    fn collect_source_columns(
        &self,
        map: &mut std::collections::HashMap<u32, std::collections::HashSet<String>>,
    ) {
        match self {
            OpTree::Scan {
                table_oid, columns, ..
            } => {
                let entry = map.entry(*table_oid).or_default();
                for col in columns {
                    entry.insert(col.name.clone());
                }
            }
            OpTree::Project { child, .. }
            | OpTree::Filter { child, .. }
            | OpTree::Distinct { child } => child.collect_source_columns(map),
            OpTree::Aggregate { child, .. } => child.collect_source_columns(map),
            OpTree::InnerJoin { left, right, .. }
            | OpTree::LeftJoin { left, right, .. }
            | OpTree::FullJoin { left, right, .. }
            | OpTree::Intersect { left, right, .. }
            | OpTree::Except { left, right, .. } => {
                left.collect_source_columns(map);
                right.collect_source_columns(map);
            }
            OpTree::UnionAll { children } => {
                for c in children {
                    c.collect_source_columns(map);
                }
            }
            OpTree::Subquery { child, .. } => child.collect_source_columns(map),
            OpTree::CteScan { .. } => { /* resolved via CteRegistry */ }
            OpTree::RecursiveCte {
                base, recursive, ..
            } => {
                base.collect_source_columns(map);
                recursive.collect_source_columns(map);
            }
            OpTree::RecursiveSelfRef { .. } => {}
            OpTree::Window { child, .. } => child.collect_source_columns(map),
            OpTree::LateralFunction { child, .. } => child.collect_source_columns(map),
            OpTree::LateralSubquery { child, .. } => child.collect_source_columns(map),
            OpTree::SemiJoin { left, right, .. } | OpTree::AntiJoin { left, right, .. } => {
                left.collect_source_columns(map);
                right.collect_source_columns(map);
            }
            OpTree::ScalarSubquery { child, .. } => child.collect_source_columns(map),
            OpTree::ConstantSelect { .. } => {}
        }
    }

    /// Prune `Scan.columns` to only those columns referenced by the
    /// defining query (plus PK columns needed for row_id computation).
    ///
    /// This reduces I/O in `diff_scan_change_buffer()` — fewer `new_*` /
    /// `old_*` columns are selected from the change buffer, which matters
    /// for wide source tables where the view references only a few columns.
    ///
    /// The approach is conservative: we collect all column names referenced
    /// anywhere in the tree's expressions, then intersect with each Scan's
    /// column set. Unqualified column refs and `Raw` / `Star` expressions
    /// cause the pass to bail out for safety (keeping all columns).
    pub fn prune_scan_columns(&mut self) {
        let mut refs = ColumnRefSet::default();
        if !self.collect_all_column_refs(&mut refs) {
            // Bail out — found Star/Raw that prevent safe pruning.
            return;
        }
        // If no column refs were collected (e.g. SELECT * with no Project wrapper),
        // bail out to avoid incorrectly pruning all non-PK columns.
        if refs.qualified.is_empty() && refs.unqualified.is_empty() {
            return;
        }
        self.apply_column_pruning(&refs);
    }

    /// Collect all `Expr::ColumnRef` occurrences in the tree.
    ///
    /// Returns `false` if an unparseable expression (`Star`, `Raw`) is
    /// encountered, meaning pruning should be skipped.
    fn collect_all_column_refs(&self, refs: &mut ColumnRefSet) -> bool {
        match self {
            OpTree::Scan { .. } | OpTree::CteScan { .. } | OpTree::RecursiveSelfRef { .. } => true,
            OpTree::Project {
                expressions, child, ..
            } => {
                for expr in expressions {
                    if !collect_refs_from_expr(expr, refs) {
                        return false;
                    }
                }
                child.collect_all_column_refs(refs)
            }
            OpTree::Filter {
                predicate, child, ..
            } => {
                if !collect_refs_from_expr(predicate, refs) {
                    return false;
                }
                child.collect_all_column_refs(refs)
            }
            OpTree::InnerJoin {
                condition,
                left,
                right,
            }
            | OpTree::LeftJoin {
                condition,
                left,
                right,
            }
            | OpTree::FullJoin {
                condition,
                left,
                right,
            } => {
                if !collect_refs_from_expr(condition, refs) {
                    return false;
                }
                left.collect_all_column_refs(refs) && right.collect_all_column_refs(refs)
            }
            OpTree::Aggregate {
                group_by,
                aggregates,
                child,
            } => {
                for expr in group_by {
                    if !collect_refs_from_expr(expr, refs) {
                        return false;
                    }
                }
                for agg in aggregates {
                    if !collect_refs_from_agg(agg, refs) {
                        return false;
                    }
                }
                child.collect_all_column_refs(refs)
            }
            OpTree::Distinct { child } => child.collect_all_column_refs(refs),
            OpTree::UnionAll { children } => {
                children.iter().all(|c| c.collect_all_column_refs(refs))
            }
            OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
                left.collect_all_column_refs(refs) && right.collect_all_column_refs(refs)
            }
            OpTree::Subquery { child, .. } => child.collect_all_column_refs(refs),
            OpTree::RecursiveCte {
                base, recursive, ..
            } => base.collect_all_column_refs(refs) && recursive.collect_all_column_refs(refs),
            OpTree::Window {
                window_exprs,
                partition_by,
                pass_through,
                child,
            } => {
                for we in window_exprs {
                    for arg in &we.args {
                        if !collect_refs_from_expr(arg, refs) {
                            return false;
                        }
                    }
                    for pb in &we.partition_by {
                        if !collect_refs_from_expr(pb, refs) {
                            return false;
                        }
                    }
                    for ob in &we.order_by {
                        if !collect_refs_from_expr(&ob.expr, refs) {
                            return false;
                        }
                    }
                }
                for pb in partition_by {
                    if !collect_refs_from_expr(pb, refs) {
                        return false;
                    }
                }
                for (expr, _) in pass_through {
                    if !collect_refs_from_expr(expr, refs) {
                        return false;
                    }
                }
                child.collect_all_column_refs(refs)
            }
            OpTree::LateralFunction { .. } => {
                // func_sql is raw SQL text — cannot extract refs, so bail
                // out to avoid pruning columns the function might reference.
                false
            }
            OpTree::LateralSubquery { .. } => {
                // subquery_sql is raw SQL — same safety concern.
                false
            }
            OpTree::SemiJoin {
                condition,
                left,
                right,
            }
            | OpTree::AntiJoin {
                condition,
                left,
                right,
            } => {
                if !collect_refs_from_expr(condition, refs) {
                    return false;
                }
                left.collect_all_column_refs(refs) && right.collect_all_column_refs(refs)
            }
            OpTree::ScalarSubquery {
                subquery, child, ..
            } => subquery.collect_all_column_refs(refs) && child.collect_all_column_refs(refs),
            // Constant anchors have no column references — scanning is a no-op.
            OpTree::ConstantSelect { .. } => true,
        }
    }

    /// Apply pruning to Scan nodes based on collected column references.
    fn apply_column_pruning(&mut self, refs: &ColumnRefSet) {
        match self {
            OpTree::Scan {
                columns,
                pk_columns,
                alias,
                ..
            } => {
                // Qualified refs for this scan alias + all unqualified refs
                let demanded: std::collections::HashSet<&str> = refs
                    .qualified
                    .get(alias.as_str())
                    .into_iter()
                    .flatten()
                    .map(|s| s.as_str())
                    .chain(refs.unqualified.iter().map(|s| s.as_str()))
                    .collect();

                columns.retain(|c| {
                    demanded.contains(c.name.as_str()) || pk_columns.iter().any(|pk| pk == &c.name)
                });
            }
            OpTree::Project { child, .. }
            | OpTree::Filter { child, .. }
            | OpTree::Distinct { child }
            | OpTree::Subquery { child, .. }
            | OpTree::LateralSubquery { child, .. }
            | OpTree::LateralFunction { child, .. }
            | OpTree::Window { child, .. } => {
                child.apply_column_pruning(refs);
            }
            OpTree::ScalarSubquery {
                subquery, child, ..
            } => {
                subquery.apply_column_pruning(refs);
                child.apply_column_pruning(refs);
            }
            OpTree::Aggregate { child, .. } => child.apply_column_pruning(refs),
            OpTree::InnerJoin { left, right, .. }
            | OpTree::LeftJoin { left, right, .. }
            | OpTree::FullJoin { left, right, .. }
            | OpTree::Intersect { left, right, .. }
            | OpTree::Except { left, right, .. }
            | OpTree::SemiJoin { left, right, .. }
            | OpTree::AntiJoin { left, right, .. } => {
                left.apply_column_pruning(refs);
                right.apply_column_pruning(refs);
            }
            OpTree::UnionAll { children } => {
                for c in children {
                    c.apply_column_pruning(refs);
                }
            }
            OpTree::RecursiveCte {
                base, recursive, ..
            } => {
                base.apply_column_pruning(refs);
                recursive.apply_column_pruning(refs);
            }
            OpTree::CteScan { .. }
            | OpTree::RecursiveSelfRef { .. }
            | OpTree::ConstantSelect { .. } => {}
        }
    }
}

/// Accumulated column references from all expressions in an OpTree.
#[derive(Default)]
struct ColumnRefSet {
    /// `alias → {column_names}` for qualified references (`t.col`).
    qualified: std::collections::HashMap<String, std::collections::HashSet<String>>,
    /// Column names without table qualifier.
    unqualified: std::collections::HashSet<String>,
}

/// Extract column references from an expression.
/// Returns `false` on Star/Raw (cannot determine exact columns).
fn collect_refs_from_expr(expr: &Expr, refs: &mut ColumnRefSet) -> bool {
    match expr {
        Expr::ColumnRef {
            table_alias: Some(alias),
            column_name,
        } => {
            refs.qualified
                .entry(alias.clone())
                .or_default()
                .insert(column_name.clone());
            true
        }
        Expr::ColumnRef {
            table_alias: None,
            column_name,
        } => {
            refs.unqualified.insert(column_name.clone());
            true
        }
        Expr::Literal(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            collect_refs_from_expr(left, refs) && collect_refs_from_expr(right, refs)
        }
        Expr::FuncCall { args, .. } => args.iter().all(|a| collect_refs_from_expr(a, refs)),
        Expr::Star { .. } | Expr::Raw(_) => false,
    }
}

/// Extract column references from an aggregate expression.
fn collect_refs_from_agg(agg: &AggExpr, refs: &mut ColumnRefSet) -> bool {
    if let Some(ref arg) = agg.argument
        && !collect_refs_from_expr(arg, refs)
    {
        return false;
    }
    if let Some(ref second) = agg.second_arg
        && !collect_refs_from_expr(second, refs)
    {
        return false;
    }
    if let Some(ref filter) = agg.filter
        && !collect_refs_from_expr(filter, refs)
    {
        return false;
    }
    if let Some(ref owg) = agg.order_within_group {
        for sort in owg {
            if !collect_refs_from_expr(&sort.expr, refs) {
                return false;
            }
        }
    }
    true
}

// ═══════════════════════════════════════════════════════════════════════
// Join PK helpers — used by row_id_key_columns and diff_project
// ═══════════════════════════════════════════════════════════════════════

/// For a Project over InnerJoin/LeftJoin, find the output aliases that
/// correspond to stable key columns of the join's source tables.
///
/// PK-based row-IDs are stable across value changes on the other side of
/// the join, fixing the simultaneous-change bug where DELETE hashes are
/// computed with current (not old) values.
///
/// Aggregate CTEs contribute their GROUP BY keys. If only one side's key is
/// visible, it is sufficient for a many-to-one join when the other side's
/// stable key is fully constrained by the join condition.
///
/// Returns `None` if no stable key aliases can be identified (caller should
/// fall back to hashing all aliases).
pub fn join_pk_aliases(
    expressions: &[Expr],
    aliases: &[String],
    join_child: &OpTree,
) -> Option<Vec<String>> {
    join_pk_expr_indices_for(expressions, join_child).map(|indices| {
        indices
            .into_iter()
            .filter_map(|index| aliases.get(index).cloned())
            .collect()
    })
}

/// For a Project over InnerJoin/LeftJoin, return the indices of
/// expressions that correspond to PK columns.
///
/// Used by `diff_project` to hash only PK-corresponding expressions
/// for the delta's row-ID recomputation.
pub fn join_pk_expr_indices(expressions: &[Expr], join_child: &OpTree) -> Vec<usize> {
    join_pk_expr_indices_for(expressions, join_child).unwrap_or_default()
}

fn join_pk_expr_indices_for(expressions: &[Expr], join_child: &OpTree) -> Option<Vec<usize>> {
    let (left, right) = match join_child {
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => (left.as_ref(), right.as_ref()),
        _ => return None,
    };

    let left_keys = stable_join_keys(left);
    let right_keys = stable_join_keys(right);
    let left_indices = matching_key_indices(expressions, &left_keys);
    let right_indices = matching_key_indices(expressions, &right_keys);

    if !left_indices.is_empty() && !right_indices.is_empty() {
        let mut indices = left_indices;
        indices.extend(right_indices);
        return Some(indices);
    }

    let condition = match join_child {
        OpTree::InnerJoin { condition, .. }
        | OpTree::LeftJoin { condition, .. }
        | OpTree::FullJoin { condition, .. } => condition,
        _ => unreachable!(),
    };

    if !left_indices.is_empty()
        && !right_keys.is_empty()
        && right_keys.iter().all(|(alias, key)| {
            condition_contains_cross_side_key(condition, alias, key, &left_keys)
        })
    {
        return Some(left_indices);
    }
    if !right_indices.is_empty()
        && !left_keys.is_empty()
        && left_keys.iter().all(|(alias, key)| {
            condition_contains_cross_side_key(condition, alias, key, &right_keys)
        })
    {
        return Some(right_indices);
    }

    None
}

/// Return stable `(source alias, key column)` pairs visible from a join side.
/// GROUP BY keys are stable identities for aggregate CTEs.
fn stable_join_keys(op: &OpTree) -> Vec<(String, String)> {
    match op {
        OpTree::Scan { alias, .. } | OpTree::CteScan { alias, .. } => scan_pk_columns(op)
            .into_iter()
            .map(|key| (alias.clone(), key))
            .collect(),
        OpTree::Aggregate { group_by, .. } => group_by
            .iter()
            .map(|expr| (op.alias().to_string(), expr.output_name()))
            .collect(),
        OpTree::Filter { child, .. } | OpTree::Project { child, .. } => stable_join_keys(child),
        OpTree::Subquery {
            alias,
            column_aliases,
            child,
        } => {
            let child_columns = child.output_columns();
            let visible_columns = if column_aliases.is_empty() {
                child_columns.clone()
            } else {
                column_aliases.clone()
            };
            stable_join_keys(child)
                .into_iter()
                .filter_map(|(_, key)| {
                    child_columns
                        .iter()
                        .position(|column| column == &key)
                        .and_then(|index| visible_columns.get(index))
                        .map(|visible| (alias.clone(), visible.clone()))
                })
                .collect()
        }
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => {
            let mut keys = stable_join_keys(left);
            keys.extend(stable_join_keys(right));
            keys
        }
        OpTree::SemiJoin { left, .. } | OpTree::AntiJoin { left, .. } => stable_join_keys(left),
        _ => Vec::new(),
    }
}

fn matching_key_indices(expressions: &[Expr], keys: &[(String, String)]) -> Vec<usize> {
    expressions
        .iter()
        .enumerate()
        .filter_map(|(index, expr)| match expr {
            Expr::ColumnRef {
                table_alias: Some(alias),
                column_name,
            } if keys
                .iter()
                .any(|(key_alias, key)| key_alias == alias && key == column_name) =>
            {
                Some(index)
            }
            _ => None,
        })
        .collect()
}

fn condition_contains_cross_side_key(
    condition: &Expr,
    key_alias: &str,
    key_column: &str,
    other_keys: &[(String, String)],
) -> bool {
    match condition {
        Expr::BinaryOp { op, left, right } if op == "=" => {
            let matches_key = |expr: &Expr, other: &Expr| {
                matches!(
                    expr,
                    Expr::ColumnRef {
                        table_alias: Some(alias),
                        column_name,
                    } if alias == key_alias
                        && column_name == key_column
                        && matches!(
                            other,
                            Expr::ColumnRef {
                                table_alias: Some(other_alias),
                                column_name: other_column,
                            } if other_keys
                                .iter()
                                .any(|(alias, key)| alias == other_alias && key == other_column)
                        )
                )
            };
            matches_key(left, right) || matches_key(right, left)
        }
        Expr::BinaryOp { left, right, .. } => {
            condition_contains_cross_side_key(left, key_alias, key_column, other_keys)
                || condition_contains_cross_side_key(right, key_alias, key_column, other_keys)
        }
        _ => false,
    }
}

/// Extract stable key column names from a Scan, transparent wrapper, or
/// aggregate CTE scan node.
///
/// Uses the real PK columns from `pg_constraint` if available, otherwise
/// falls back to non-nullable columns as a heuristic.
pub(crate) fn scan_pk_columns(op: &OpTree) -> Vec<String> {
    match op {
        OpTree::Scan {
            pk_columns,
            columns,
            ..
        } => {
            if !pk_columns.is_empty() {
                return pk_columns.clone();
            }
            // Fallback: non-nullable columns heuristic
            let non_nullable: Vec<String> = columns
                .iter()
                .filter(|c| !c.is_nullable)
                .map(|c| c.name.clone())
                .collect();
            if non_nullable.is_empty() {
                columns.iter().map(|c| c.name.clone()).collect()
            } else {
                non_nullable
            }
        }
        OpTree::Filter { child, .. } => scan_pk_columns(child),
        OpTree::Project {
            child,
            expressions,
            aliases,
        } => {
            let child_columns = child.output_columns();
            scan_pk_columns(child)
                .iter()
                .filter_map(|key| {
                    expressions
                        .iter()
                        .position(|expr| {
                            matches!(expr, Expr::ColumnRef { column_name, .. } if column_name == key)
                        })
                        .or_else(|| child_columns.iter().position(|column| column == key))
                        .and_then(|index| aliases.get(index))
                        .cloned()
                })
                .collect()
        }
        OpTree::Subquery {
            child,
            column_aliases,
            ..
        } if !column_aliases.is_empty() => {
            let child_columns = child.output_columns();
            scan_pk_columns(child)
                .iter()
                .filter_map(|key| {
                    child_columns
                        .iter()
                        .position(|column| column == key)
                        .and_then(|index| column_aliases.get(index))
                        .cloned()
                })
                .collect()
        }
        OpTree::Subquery { child, .. } => scan_pk_columns(child),
        OpTree::CteScan {
            body: Some(body),
            columns,
            cte_def_aliases,
            column_aliases,
            ..
        } => {
            let body_keys = body.row_id_key_columns().unwrap_or_default();
            let body_columns = body.output_columns();
            let visible_columns = if !column_aliases.is_empty() {
                column_aliases
            } else if !cte_def_aliases.is_empty() {
                cte_def_aliases
            } else {
                columns
            };
            body_keys
                .iter()
                .filter_map(|key| {
                    body_columns
                        .iter()
                        .position(|column| column == key)
                        .and_then(|index| visible_columns.get(index))
                        .cloned()
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Return whether any candidate key can contain NULL in the operator output.
/// Unknown expressions are treated conservatively as nullable.
fn key_columns_are_nullable(op: &OpTree, keys: &[String]) -> bool {
    keys.iter().any(|key| !key_column_is_non_nullable(op, key))
}

fn full_join_has_nullable_key(op: &OpTree) -> bool {
    let OpTree::FullJoin { left, right, .. } = op else {
        return false;
    };
    let left_keys = scan_pk_columns(left);
    let right_keys = scan_pk_columns(right);
    key_columns_are_nullable(left, &left_keys) || key_columns_are_nullable(right, &right_keys)
}

fn key_column_is_non_nullable(op: &OpTree, key: &str) -> bool {
    match op {
        OpTree::Scan {
            columns,
            pk_columns,
            ..
        } => {
            pk_columns.iter().any(|pk| pk == key)
                || columns
                    .iter()
                    .find(|column| column.name == key)
                    .is_some_and(|column| !column.is_nullable)
        }
        OpTree::Filter { child, .. } => key_column_is_non_nullable(child, key),
        OpTree::Project {
            child,
            expressions,
            aliases,
        } => expressions
            .iter()
            .zip(aliases)
            .find_map(|(expr, alias)| {
                (alias == key).then(|| match expr {
                    Expr::ColumnRef { column_name, .. } => {
                        key_column_is_non_nullable(child, column_name)
                    }
                    _ => false,
                })
            })
            .unwrap_or(false),
        OpTree::Subquery {
            child,
            column_aliases,
            ..
        } => {
            if column_aliases.is_empty() {
                return key_column_is_non_nullable(child, key);
            }
            let child_columns = child.output_columns();
            column_aliases
                .iter()
                .position(|alias| alias == key)
                .is_some_and(|index| {
                    child_columns
                        .get(index)
                        .is_some_and(|child_key| key_column_is_non_nullable(child, child_key))
                })
        }
        OpTree::Aggregate {
            child, group_by, ..
        } => group_by
            .iter()
            .find(|expr| expr.output_name() == key)
            .and_then(|expr| match expr {
                Expr::ColumnRef { column_name, .. } => {
                    Some(key_column_is_non_nullable(child, column_name))
                }
                _ => None,
            })
            .unwrap_or(false),
        OpTree::CteScan {
            body: Some(body),
            columns,
            cte_def_aliases,
            column_aliases,
            ..
        } => {
            let visible_columns = if !column_aliases.is_empty() {
                column_aliases
            } else if !cte_def_aliases.is_empty() {
                cte_def_aliases
            } else {
                columns
            };
            let body_columns = body.output_columns();
            visible_columns
                .iter()
                .position(|column| column == key)
                .is_some_and(|index| {
                    body_columns
                        .get(index)
                        .is_some_and(|body_key| key_column_is_non_nullable(body, body_key))
                })
        }
        _ => false,
    }
}

/// Check whether a Project-over-Join is a many-to-one join where the
/// covered side's PK in the output suffices for unique row identification.
///
/// A join is many-to-one when:
/// - One side's PK is in the output (the "covered" side)
/// - The other side's PK is NOT in the output (the "uncovered" side)
/// - The join condition equates the uncovered side's PK column(s)
///   (guaranteeing at most one match per covered-side row)
///
/// Examples:
/// - `bids b JOIN auctions a ON a.id = b.auction_id` with `b.id` in output:
///   Right PK (`a.id`) is part of the join condition → many-to-one ✓
/// - `s1 JOIN s2 ON s1.category = s2.category` with `s1.id` in output:
///   Right PK (`s2.id`) is NOT part of the join condition → many-to-many ✗
fn is_many_to_one_join(expressions: &[Expr], aliases: &[String], join_child: &OpTree) -> bool {
    let (left, right, condition) = match join_child {
        OpTree::InnerJoin {
            left,
            right,
            condition,
        }
        | OpTree::LeftJoin {
            left,
            right,
            condition,
        }
        | OpTree::FullJoin {
            left,
            right,
            condition,
        } => (left.as_ref(), right.as_ref(), condition),
        _ => return false,
    };

    let left_alias = left.alias();
    let right_alias = right.alias();
    let left_pks = scan_pk_columns(left);
    let right_pks = scan_pk_columns(right);

    // Determine which side's PK is covered in the output.
    let left_covered = !left_pks.is_empty()
        && left_pks.iter().all(|pk| {
            expressions.iter().zip(aliases.iter()).any(|(expr, _)| {
                matches!(expr, Expr::ColumnRef { table_alias: Some(tbl), column_name }
                    if tbl == left_alias && column_name == pk)
            })
        });
    let right_covered = !right_pks.is_empty()
        && right_pks.iter().all(|pk| {
            expressions.iter().zip(aliases.iter()).any(|(expr, _)| {
                matches!(expr, Expr::ColumnRef { table_alias: Some(tbl), column_name }
                    if tbl == right_alias && column_name == pk)
            })
        });

    // Extract equi-join column names from the condition.
    let equi_cols = extract_equijoin_col_names(condition);

    // If the left PK is covered but right isn't, check if the right
    // side's PK columns are ALL in the join condition.
    if left_covered && !right_covered {
        return right_pks
            .iter()
            .all(|pk| equi_cols.iter().any(|(_, right_col)| right_col == pk));
    }

    // If the right PK is covered but left isn't, check if the left
    // side's PK columns are ALL in the join condition.
    if right_covered && !left_covered {
        return left_pks
            .iter()
            .all(|pk| equi_cols.iter().any(|(left_col, _)| left_col == pk));
    }

    // Neither side or both sides covered — not a simple many-to-one.
    false
}

/// Extract equi-join column names from a condition expression.
///
/// Returns `(left_col_name, right_col_name)` pairs for simple `a.col = b.col`
/// patterns, including through AND conjunctions.
fn extract_equijoin_col_names(condition: &Expr) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    collect_equijoin_col_names(condition, &mut pairs);
    pairs
}

fn collect_equijoin_col_names(expr: &Expr, pairs: &mut Vec<(String, String)>) {
    match expr {
        Expr::BinaryOp { op, left, right } if op == "=" => {
            if let (
                Expr::ColumnRef {
                    column_name: left_col,
                    ..
                },
                Expr::ColumnRef {
                    column_name: right_col,
                    ..
                },
            ) = (left.as_ref(), right.as_ref())
            {
                pairs.push((left_col.clone(), right_col.clone()));
                // Also push reversed for symmetric matching
                pairs.push((right_col.clone(), left_col.clone()));
            }
        }
        Expr::BinaryOp { op, left, right } if op.eq_ignore_ascii_case("AND") => {
            collect_equijoin_col_names(left, pairs);
            collect_equijoin_col_names(right, pairs);
        }
        _ => {}
    }
}
