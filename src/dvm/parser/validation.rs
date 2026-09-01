//! Query validation: volatility checking, IVM support, IMMEDIATE mode,
//! and monotonicity analysis.

use super::*;
use crate::dag::RefreshMode;
use crate::error::PgTrickleError;

/// A structured reason why a query cannot use incremental maintenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationIssue {
    pub code: &'static str,
    pub operator: &'static str,
    pub reason: String,
    pub hint: String,
}

/// Conservative admission result for incremental execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncrementalAdmission {
    Proven,
    FullOnly(Vec<ValidationIssue>),
}

impl IncrementalAdmission {
    pub fn is_proven(&self) -> bool {
        matches!(self, Self::Proven)
    }
}

/// Apply the single AUTO/explicit refresh-mode admission matrix.
pub fn resolve_incremental_mode(
    requested: RefreshMode,
    is_auto: bool,
    admission: &IncrementalAdmission,
) -> Result<RefreshMode, PgTrickleError> {
    if requested == RefreshMode::Full || admission.is_proven() {
        return Ok(requested);
    }
    if is_auto {
        return Ok(RefreshMode::Full);
    }

    let issues = match admission {
        IncrementalAdmission::FullOnly(issues) => issues,
        IncrementalAdmission::Proven => unreachable!(),
    };
    let detail = issues
        .iter()
        .map(|issue| format!("{}: {}", issue.operator, issue.reason))
        .collect::<Vec<_>>()
        .join("; ");
    Err(PgTrickleError::UnsupportedOperator(format!(
        "{} mode is not safe for incremental maintenance: {}. Use FULL or AUTO. {}",
        requested.as_str(),
        detail,
        issues
            .first()
            .map(|issue| issue.hint.as_str())
            .unwrap_or("Rewrite the query or use FULL refresh."),
    )))
}

/// Classify known unproven incremental forms without mutating any state.
pub fn incremental_admission(
    result: &ParseResult,
    volatility: char,
    immediate: bool,
) -> Result<IncrementalAdmission, PgTrickleError> {
    let mut issues = Vec::new();
    collect_admission_issues(&result.tree, immediate, &mut issues);
    for (_, body) in &result.cte_registry.entries {
        collect_admission_issues(body, immediate, &mut issues);
    }

    if volatility >= 's' {
        let (code, reason) = if volatility == 'v' {
            (
                "DVM-81-6-VOLATILE",
                "volatile expressions have unsafe volatility and can change between evaluations",
            )
        } else {
            (
                "DVM-81-6-STABLE",
                "stable expressions have refresh-dependent volatility and can change between refreshes",
            )
        };
        issues.push(ValidationIssue {
            code,
            operator: if volatility == 'v' {
                "VOLATILE"
            } else {
                "STABLE"
            },
            reason: reason.to_string(),
            hint: "Use FULL or AUTO, or replace the expression with an IMMUTABLE one.".to_string(),
        });
    }

    if let Err(error) = check_ivm_support_with_registry(result) {
        issues.push(ValidationIssue {
            code: "DVM-81-6-IVM-SUPPORT",
            operator: "DVM",
            reason: format!("the query is not incrementally supported: {error}"),
            hint: "Use FULL or AUTO, or rewrite the query using supported operators.".to_string(),
        });
    }

    if issues.is_empty() {
        Ok(IncrementalAdmission::Proven)
    } else {
        Ok(IncrementalAdmission::FullOnly(issues))
    }
}

fn collect_admission_issues(tree: &OpTree, immediate: bool, issues: &mut Vec<ValidationIssue>) {
    match tree {
        OpTree::Intersect { left, right, .. } => {
            issues.push(ValidationIssue {
                code: "DVM-81-1-SET",
                operator: "INTERSECT",
                reason: "INTERSECT state is not yet proven safe for incremental execution"
                    .to_string(),
                hint: "Use FULL or AUTO until private set-operation state is available."
                    .to_string(),
            });
            collect_admission_issues(left, immediate, issues);
            collect_admission_issues(right, immediate, issues);
        }
        OpTree::Except { left, right, .. } => {
            issues.push(ValidationIssue {
                code: "DVM-81-1-SET",
                operator: "EXCEPT",
                reason: "EXCEPT state is not yet proven safe for incremental execution".to_string(),
                hint: "Use FULL or AUTO until private set-operation state is available."
                    .to_string(),
            });
            collect_admission_issues(left, immediate, issues);
            collect_admission_issues(right, immediate, issues);
        }
        OpTree::LateralSubquery {
            subquery_source_oids,
            correlation_predicates,
            subquery_sql: _subquery_sql,
            child,
            ..
        } => {
            #[cfg(not(test))]
            if let Err(error) = parse_defining_query_full(_subquery_sql)
                .and_then(|inner| check_ivm_support_with_registry(&inner))
            {
                issues.push(ValidationIssue {
                    code: "DVM-81-5-LATERAL-BODY",
                    operator: "LATERAL",
                    reason: format!("the LATERAL subquery body is not incrementally supported: {error}"),
                    hint: "Use FULL or AUTO, or rewrite the LATERAL subquery without unsupported clauses."
                        .to_string(),
                });
            }
            let child_columns = child.output_columns();
            match child.row_id_key_columns() {
                Some(keys)
                    if !keys.is_empty() && keys.iter().all(|k| child_columns.contains(k)) => {}
                _ => issues.push(ValidationIssue {
                    code: "DVM-81-5-LATERAL-IDENTITY",
                    operator: "LATERAL",
                    reason: "the outer dependency has no exact row identity available for deletion"
                        .to_string(),
                    hint: "Retain a stable outer key in the query output or use FULL/AUTO."
                        .to_string(),
                }),
            }
            for predicate in correlation_predicates {
                if predicate.inner_oid == 0
                    || !subquery_source_oids.contains(&predicate.inner_oid)
                    || predicate.inner_alias.is_empty()
                    || predicate.inner_col.is_empty()
                    || !child_columns.contains(&predicate.outer_col)
                {
                    issues.push(ValidationIssue {
                        code: "DVM-81-5-LATERAL-DEPENDENCY",
                        operator: "LATERAL",
                        reason: "an outer/inner dependency is unresolved or not represented by the parsed source metadata"
                            .to_string(),
                        hint: "Use FULL/AUTO or rewrite the LATERAL correlation as an explicit supported join."
                            .to_string(),
                    });
                    break;
                }
            }
            if subquery_source_oids.contains(&0) {
                issues.push(ValidationIssue {
                    code: "DVM-81-5-LATERAL-DEPENDENCY",
                    operator: "LATERAL",
                    reason: "the LATERAL subquery contains an unresolved inner source dependency"
                        .to_string(),
                    hint: "Use FULL/AUTO or resolve every inner source relation.".to_string(),
                });
            }
            if immediate && !subquery_source_oids.is_empty() {
                issues.push(ValidationIssue {
                    code: "DVM-81-5-IMMEDIATE-LATERAL",
                    operator: "LATERAL",
                    reason: "IMMEDIATE transition-table maintenance for mutable inner sources is not implemented"
                        .to_string(),
                    hint: "Use DIFFERENTIAL, FULL, or AUTO, or remove the mutable inner source."
                        .to_string(),
                });
            }
            collect_admission_issues(child, immediate, issues);
        }
        OpTree::LateralFunction {
            func_sql: _func_sql,
            child,
            ..
        } => {
            #[cfg(not(test))]
            if let Err(error) = parse_defining_query_full(&format!("SELECT {_func_sql}"))
                .and_then(|inner| check_ivm_support_with_registry(&inner))
            {
                issues.push(ValidationIssue {
                    code: "DVM-81-5-LATERAL-BODY",
                    operator: "LATERAL",
                    reason: format!(
                        "the LATERAL function body is not incrementally supported: {error}"
                    ),
                    hint: "Use FULL or AUTO, or rewrite the LATERAL function body without unsupported constructs."
                        .to_string(),
                });
            }
            collect_admission_issues(child, immediate, issues);
        }
        OpTree::Project { child, .. }
        | OpTree::Filter { child, .. }
        | OpTree::Distinct { child }
        | OpTree::Subquery { child, .. }
        | OpTree::Window { child, .. } => collect_admission_issues(child, immediate, issues),
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. }
        | OpTree::SemiJoin { left, right, .. }
        | OpTree::AntiJoin { left, right, .. } => {
            collect_admission_issues(left, immediate, issues);
            collect_admission_issues(right, immediate, issues);
        }
        OpTree::Aggregate { child, .. } => collect_admission_issues(child, immediate, issues),
        OpTree::UnionAll { children } => {
            for child in children {
                collect_admission_issues(child, immediate, issues);
            }
        }
        OpTree::ScalarSubquery {
            child, subquery, ..
        } => {
            collect_admission_issues(child, immediate, issues);
            collect_admission_issues(subquery, immediate, issues);
        }
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            collect_admission_issues(base, immediate, issues);
            collect_admission_issues(recursive, immediate, issues);
        }
        OpTree::Scan { .. }
        | OpTree::CteScan { .. }
        | OpTree::RecursiveSelfRef { .. }
        | OpTree::ConstantSelect { .. } => {}
    }
}

// ── Volatility checking ─────────────────────────────────────────────────

/// Volatility ordering: volatile > stable > immutable.
///
/// Returns the "worse" (more volatile) of two volatility classes.
///   - `'v'` (volatile) > `'s'` (stable) > `'i'` (immutable)
pub fn max_volatility(a: char, b: char) -> char {
    match (a, b) {
        ('v', _) | (_, 'v') => 'v',
        ('s', _) | (_, 's') => 's',
        _ => 'i',
    }
}

// P-2: Thread-local caches for function and operator volatility lookups.
//
// Each backend calls `lookup_function_volatility()` once per unique
// unresolved function name encountered during simplified DVM parsing.  Without caching, a
// query with 50 function calls would issue 50 SPI round-trips to
// `pg_proc` (~1ms each = 50ms overhead per parse).  With the
// thread-local cache, each name is resolved at most once per session.
#[cfg(not(test))]
thread_local! {
    static FUNCTION_VOLATILITY_CACHE: std::cell::RefCell<std::collections::HashMap<String, char>> =
        std::cell::RefCell::new(std::collections::HashMap::new());

    static OPERATOR_VOLATILITY_CACHE: std::cell::RefCell<std::collections::HashMap<String, char>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

/// Clear the thread-local volatility caches.
///
/// Called by `pgtrickle.clear_caches()` so any custom-function or
/// custom-operator volatility changes take effect without restarting.
#[cfg(not(test))]
pub fn flush_volatility_cache() {
    FUNCTION_VOLATILITY_CACHE.with(|c| c.borrow_mut().clear());
    OPERATOR_VOLATILITY_CACHE.with(|c| c.borrow_mut().clear());
}

#[cfg(test)]
pub fn flush_volatility_cache() {}

/// Look up the volatility category of a PostgreSQL function by name.
///
/// Returns `'i'` (immutable), `'s'` (stable), or `'v'` (volatile).
/// Simplified parser expressions do not retain PostgreSQL's resolved function
/// OID, so an unresolved name is deliberately treated as volatile. The SQL
/// admission path uses `analyzed_query_volatility`, which reads `provolatile`
/// by the resolved OID and therefore handles overloads exactly.
///
/// P-2: Conservative results remain cached so repeated unresolved names do
/// not add work to the parser path.
#[cfg(not(test))]
pub fn lookup_function_volatility(func_name: &str) -> Result<char, PgTrickleError> {
    let bare_name = func_name.rsplit('.').next().unwrap_or(func_name);

    // P-2: Check thread-local cache before issuing an SPI round-trip.
    if let Some(cached) = FUNCTION_VOLATILITY_CACHE.with(|c| c.borrow().get(bare_name).copied()) {
        return Ok(cached);
    }

    // Do not query pg_proc by proname: overloads can have different
    // volatility, and this representation has no resolved argument types.
    let result = 'v';

    // P-2: Cache the result for future lookups in this session.
    FUNCTION_VOLATILITY_CACHE.with(|c| c.borrow_mut().insert(bare_name.to_string(), result));

    Ok(result)
}

/// Test-only stub: SPI is unavailable in unit tests, so assume volatile.
#[cfg(test)]
pub fn lookup_function_volatility(_func_name: &str) -> Result<char, PgTrickleError> {
    Ok('v')
}

/// Look up the volatility category of a PostgreSQL operator by name.
///
/// Joins `pg_operator` → `pg_proc` via `oprcode` to get the implementing
/// function's `provolatile`.
///
/// **Overload disambiguation:** Many operators (`+`, `-`, `||`, etc.) have
/// overloads spanning different type categories. For example, `+` is
/// immutable for `int4 + int4` but stable for `timestamp + interval`
/// (timezone-dependent), and `||` is immutable for `text || text` but
/// stable for `textanycat(text, anynonarray)`. Without argument-type
/// resolution we cannot determine which overload applies.
///
/// To avoid false "stable" warnings on expressions like `depth + 1` or
/// `path || '>'`, we first compute the worst volatility across
/// **concrete non-temporal** overloads (those whose left AND right operand
/// types are neither date/time category `'D'` nor pseudo-type category
/// `'P'`). If at least one such overload exists, we return that result.
/// Only when ALL overloads involve temporal or pseudo-types do we fall
/// back to the worst across all overloads.
///
/// Returns `'i'` if the operator is not found — all built-in operators
/// (arithmetic, comparison, logical) are immutable. Unknown operators
/// default to `'i'` because operator-not-found typically means the query
/// would fail anyway; the volatile/stable cases arise only with custom
/// operators that DO exist in `pg_operator`.
///
/// Closes Gap G7.2: custom operators with volatile `oprcode` functions
/// (e.g., PostGIS `&&` or user-defined operators) are still detected.
///
/// P-2: Results are cached in a thread-local `HashMap` so each operator
/// name is looked up via SPI at most once per backend session.
#[cfg(not(test))]
pub fn lookup_operator_volatility(op_name: &str) -> Result<char, PgTrickleError> {
    // P-2: Check thread-local cache before issuing an SPI round-trip.
    if let Some(cached) = OPERATOR_VOLATILITY_CACHE.with(|c| c.borrow().get(op_name).copied()) {
        return Ok(cached);
    }

    let result = Spi::connect(|client| {
        let result = client.select(
            "SELECT p.provolatile::text AS vol, \
                    COALESCE(tl.typcategory::text, 'X') AS lcat, \
                    COALESCE(tr.typcategory::text, 'X') AS rcat \
             FROM pg_catalog.pg_operator o \
             JOIN pg_catalog.pg_proc p ON o.oprcode = p.oid \
             LEFT JOIN pg_catalog.pg_type tl ON o.oprleft = tl.oid \
             LEFT JOIN pg_catalog.pg_type tr ON o.oprright = tr.oid \
             WHERE o.oprname = $1",
            None,
            &[op_name.into()],
        )?;

        let mut worst_all = 'i';
        let mut worst_concrete = 'i';
        let mut has_concrete = false;

        for row in result {
            let vol: String = row
                .get_by_name::<String, _>("vol")
                .ok()
                .flatten()
                .unwrap_or_else(|| "i".to_string());
            let lcat: String = row
                .get_by_name::<String, _>("lcat")
                .ok()
                .flatten()
                .unwrap_or_else(|| "X".to_string());
            let rcat: String = row
                .get_by_name::<String, _>("rcat")
                .ok()
                .flatten()
                .unwrap_or_else(|| "X".to_string());

            let ch = vol.chars().next().unwrap_or('i');
            worst_all = max_volatility(worst_all, ch);

            // Exclude overloads with temporal (D) or pseudo-type (P)
            // arguments — these cause false positives when the actual
            // resolved overload uses concrete non-temporal types.
            let lc = lcat.chars().next().unwrap_or('X');
            let rc = rcat.chars().next().unwrap_or('X');
            if lc != 'D' && lc != 'P' && rc != 'D' && rc != 'P' {
                has_concrete = true;
                worst_concrete = max_volatility(worst_concrete, ch);
            }
        }

        // Prefer concrete non-temporal overloads when available.
        Ok(if has_concrete {
            worst_concrete
        } else {
            worst_all
        })
    })
    .map_err(|e: pgrx::spi::SpiError| {
        PgTrickleError::SpiError(format!("operator volatility lookup failed: {e}"))
    })?;

    // P-2: Cache the result for future lookups in this session.
    OPERATOR_VOLATILITY_CACHE.with(|c| c.borrow_mut().insert(op_name.to_string(), result));

    Ok(result)
}

/// Test-only stub: SPI is unavailable in unit tests, so assume immutable
/// for operators (most built-in operators are immutable). In the real
/// implementation, temporal/pseudo-type overloads are filtered out to avoid
/// false stable warnings — see the `#[cfg(not(test))]` variant.
#[cfg(test)]
pub fn lookup_operator_volatility(_op_name: &str) -> Result<char, PgTrickleError> {
    Ok('i')
}

/// Recursively scan an `Expr` tree and update `worst` with the volatility
/// of any `FuncCall` or `BinaryOp` operator nodes found.
///
/// For `Expr::BinaryOp`, looks up the operator's implementing function in
/// `pg_operator` → `pg_proc.provolatile` to detect custom volatile operators
/// (Gap G7.2).
///
/// For `Expr::Raw` nodes, re-parses the SQL fragment via `raw_parser()` and
/// walks the parse tree to detect function calls or operators that may be
/// volatile. This closes the G7.1 gap where expressions like
/// `CASE WHEN now() > x THEN ...` deparsed to `Expr::Raw` would bypass
/// volatility checking.
pub fn collect_volatilities(expr: &Expr, worst: &mut char) -> Result<(), PgTrickleError> {
    match expr {
        Expr::FuncCall { func_name, args } => {
            // COALESCE, NULLIF, GREATEST, LEAST are parser constructs that
            // don't appear in pg_proc. They are inherently immutable — their
            // result depends only on their arguments. Skip the pg_proc lookup
            // and just check argument volatility.
            //
            // NOT is also a builtin boolean operator (BoolExpr::NOT_EXPR in the
            // parse tree, mapped to Expr::FuncCall { func_name: "NOT" }). The
            // pg_proc entry is named "boolnot", not "not", so a proname lookup
            // for "NOT" returns 0 rows and the conservative 'v' default would
            // be applied incorrectly. Boolean NOT is immutable; its volatility
            // is purely that of its argument.
            let upper = func_name.to_uppercase();
            let is_builtin_construct = matches!(
                upper.as_str(),
                "COALESCE" | "NULLIF" | "GREATEST" | "LEAST" | "NOT"
            );
            if !is_builtin_construct {
                let vol = lookup_function_volatility(func_name)?;
                *worst = max_volatility(*worst, vol);
            }
            for arg in args {
                collect_volatilities(arg, worst)?;
            }
        }
        Expr::BinaryOp { op, left, right } => {
            // G7.2: Check the operator's implementing function volatility.
            let vol = lookup_operator_volatility(op)?;
            *worst = max_volatility(*worst, vol);
            collect_volatilities(left, worst)?;
            collect_volatilities(right, worst)?;
        }
        Expr::Raw(sql) => {
            // Re-parse the raw SQL fragment to find any embedded function calls.
            collect_raw_expr_volatility(sql, worst)?;
        }
        // ColumnRef, Literal, Star: no function calls.
        _ => {}
    }
    Ok(())
}

/// Re-parse a raw SQL expression string and walk the parse tree for function
/// calls, updating `worst` with the volatility of each function found.
///
/// Wraps the expression in `SELECT (expr)` to make it a valid top-level
/// statement, then uses `raw_parser()` to parse and recursively walks the
/// resulting parse tree nodes.
#[cfg(not(test))]
fn collect_raw_expr_volatility(raw_sql: &str, worst: &mut char) -> Result<(), PgTrickleError> {
    // Skip simple literals and column references — no function calls possible.
    if raw_sql.is_empty()
        || raw_sql == "NULL"
        || raw_sql.starts_with('\'')
        || raw_sql.parse::<f64>().is_ok()
    {
        return Ok(());
    }

    let wrapper = format!("SELECT ({})", raw_sql);
    let list = match parse_query(wrapper.as_str()) {
        Ok(l) => l,
        Err(_) => {
            // Parsing failed — conservatively assume volatile.
            *worst = max_volatility(*worst, 'v');
            return Ok(());
        }
    };
    for raw_stmt in list.iter_ptr() {
        let stmt = pg_deref!(raw_stmt).stmt;
        if !stmt.is_null() {
            walk_node_for_volatility(stmt, worst)?;
        }
    }

    Ok(())
}

/// Test-only stub: assume volatile for any Expr::Raw (safe default).
#[cfg(test)]
fn collect_raw_expr_volatility(_raw_sql: &str, worst: &mut char) -> Result<(), PgTrickleError> {
    // In tests, conservatively assume volatile for raw expressions.
    *worst = max_volatility(*worst, 'v');
    Ok(())
}

/// Scan a complete SQL string for volatile function calls using the raw SQL
/// parser (no semantic resolution). Used as a fallback when
/// `parse_defining_query_full` fails — e.g. for LATERAL bodies with no FROM
/// clause (`SELECT random()::numeric AS noise`) or correlated subqueries that
/// reference outer-scope columns.
///
/// Correctly handles both cases:
/// - `SELECT random()::numeric AS noise` → finds `random()` → marks volatile
/// - `SELECT col FROM t WHERE t.id = outer.id LIMIT 1` → no volatile fns → no-op
#[cfg(not(test))]
fn scan_sql_for_volatility(sql: &str, worst: &mut char) -> Result<(), PgTrickleError> {
    let list = parse_query(sql).map_err(|e| {
        PgTrickleError::QueryParseError(format!("cannot inspect expression volatility: {e}"))
    })?;
    for raw_stmt in list.iter_ptr() {
        let stmt = pg_deref!(raw_stmt).stmt;
        if !stmt.is_null() {
            walk_node_for_volatility(stmt, worst)?;
        }
    }
    Ok(())
}

pub(crate) fn sql_value_function_name(op: pg_sys::SQLValueFunctionOp::Type) -> &'static str {
    match op {
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_DATE => "CURRENT_DATE",
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_TIME => "CURRENT_TIME",
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_TIME_N => "CURRENT_TIME",
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP => "CURRENT_TIMESTAMP",
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP_N => "CURRENT_TIMESTAMP",
        pg_sys::SQLValueFunctionOp::SVFOP_LOCALTIME => "LOCALTIME",
        pg_sys::SQLValueFunctionOp::SVFOP_LOCALTIME_N => "LOCALTIME",
        pg_sys::SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP => "LOCALTIMESTAMP",
        pg_sys::SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP_N => "LOCALTIMESTAMP",
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_ROLE => "CURRENT_ROLE",
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_USER => "CURRENT_USER",
        pg_sys::SQLValueFunctionOp::SVFOP_USER => "USER",
        pg_sys::SQLValueFunctionOp::SVFOP_SESSION_USER => "SESSION_USER",
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_CATALOG => "CURRENT_CATALOG",
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_SCHEMA => "CURRENT_SCHEMA",
        _ => "CURRENT_TIMESTAMP",
    }
}

fn sql_value_function_volatility(op: pg_sys::SQLValueFunctionOp::Type) -> char {
    match op {
        pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_DATE
        | pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_TIME
        | pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_TIME_N
        | pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP
        | pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_TIMESTAMP_N
        | pg_sys::SQLValueFunctionOp::SVFOP_LOCALTIME
        | pg_sys::SQLValueFunctionOp::SVFOP_LOCALTIME_N
        | pg_sys::SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP
        | pg_sys::SQLValueFunctionOp::SVFOP_LOCALTIMESTAMP_N
        | pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_ROLE
        | pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_USER
        | pg_sys::SQLValueFunctionOp::SVFOP_USER
        | pg_sys::SQLValueFunctionOp::SVFOP_SESSION_USER
        | pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_CATALOG
        | pg_sys::SQLValueFunctionOp::SVFOP_CURRENT_SCHEMA => 's',
        _ => 's',
    }
}

/// Recursively walk a pg_sys::Node tree looking for FuncCall nodes and
/// update `worst` with the volatility of each function found.
#[cfg(not(test))]
fn walk_node_for_volatility(
    node: *mut pg_sys::Node,
    worst: &mut char,
) -> Result<(), PgTrickleError> {
    if node.is_null() {
        return Ok(());
    }

    if let Some(fcall) = cast_node!(node, T_FuncCall, pg_sys::FuncCall) {
        // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
        if let Ok(func_name) = unsafe { extract_func_name(fcall.funcname) } {
            let vol = lookup_function_volatility(&func_name)?;
            *worst = max_volatility(*worst, vol);
        }
        // Also walk function arguments
        let args_list = pg_list::<pg_sys::Node>(fcall.args);
        for n in args_list.iter_ptr() {
            walk_node_for_volatility(n, worst)?;
        }
        // Walk FILTER clause
        if !fcall.agg_filter.is_null() {
            walk_node_for_volatility(fcall.agg_filter, worst)?;
        }
    } else if let Some(svf) = cast_node!(node, T_SQLValueFunction, pg_sys::SQLValueFunction) {
        *worst = max_volatility(*worst, sql_value_function_volatility(svf.op));
    } else if let Some(stmt) = cast_node!(node, T_SelectStmt, pg_sys::SelectStmt) {
        // Walk target list
        let tlist = pg_list::<pg_sys::Node>(stmt.targetList);
        for n in tlist.iter_ptr() {
            walk_node_for_volatility(n, worst)?;
        }
        // Walk WHERE clause
        if !stmt.whereClause.is_null() {
            walk_node_for_volatility(stmt.whereClause, worst)?;
        }
        // Walk HAVING clause
        if !stmt.havingClause.is_null() {
            walk_node_for_volatility(stmt.havingClause, worst)?;
        }
    } else if let Some(rt) = cast_node!(node, T_ResTarget, pg_sys::ResTarget) {
        if !rt.val.is_null() {
            walk_node_for_volatility(rt.val, worst)?;
        }
    } else if let Some(aexpr) = cast_node!(node, T_A_Expr, pg_sys::A_Expr) {
        // G7.2: Check the operator's implementing function volatility.
        if !aexpr.name.is_null()
            // SAFETY: Parse-tree node pointer from raw_parser; valid within current memory context.
            && let Ok(op_name) = unsafe { extract_operator_name(aexpr.name) }
        {
            let vol = lookup_operator_volatility(&op_name)?;
            *worst = max_volatility(*worst, vol);
        }
        if !aexpr.lexpr.is_null() {
            walk_node_for_volatility(aexpr.lexpr, worst)?;
        }
        if !aexpr.rexpr.is_null() {
            walk_node_for_volatility(aexpr.rexpr, worst)?;
        }
    } else if let Some(bexpr) = cast_node!(node, T_BoolExpr, pg_sys::BoolExpr) {
        let args_list = pg_list::<pg_sys::Node>(bexpr.args);
        for n in args_list.iter_ptr() {
            walk_node_for_volatility(n, worst)?;
        }
    } else if let Some(tc) = cast_node!(node, T_TypeCast, pg_sys::TypeCast) {
        if !tc.arg.is_null() {
            walk_node_for_volatility(tc.arg, worst)?;
        }
    } else if let Some(ce) = cast_node!(node, T_CaseExpr, pg_sys::CaseExpr) {
        if !ce.arg.is_null() {
            walk_node_for_volatility(ce.arg as *mut pg_sys::Node, worst)?;
        }
        let whens = pg_list::<pg_sys::Node>(ce.args);
        for w in whens.iter_ptr() {
            walk_node_for_volatility(w, worst)?;
        }
        if !ce.defresult.is_null() {
            walk_node_for_volatility(ce.defresult as *mut pg_sys::Node, worst)?;
        }
    } else if let Some(cw) = cast_node!(node, T_CaseWhen, pg_sys::CaseWhen) {
        if !cw.expr.is_null() {
            walk_node_for_volatility(cw.expr as *mut pg_sys::Node, worst)?;
        }
        if !cw.result.is_null() {
            walk_node_for_volatility(cw.result as *mut pg_sys::Node, worst)?;
        }
    } else if let Some(ce) = cast_node!(node, T_CoalesceExpr, pg_sys::CoalesceExpr) {
        let args = pg_list::<pg_sys::Node>(ce.args);
        for n in args.iter_ptr() {
            walk_node_for_volatility(n, worst)?;
        }
    } else if let Some(nt) = cast_node!(node, T_NullTest, pg_sys::NullTest) {
        if !nt.arg.is_null() {
            walk_node_for_volatility(nt.arg as *mut pg_sys::Node, worst)?;
        }
    } else if let Some(cc) = cast_node!(node, T_CollateClause, pg_sys::CollateClause) {
        if !cc.arg.is_null() {
            walk_node_for_volatility(cc.arg, worst)?;
        }
    } else if let Some(sl) = cast_node!(node, T_SubLink, pg_sys::SubLink) {
        if !sl.testexpr.is_null() {
            walk_node_for_volatility(sl.testexpr, worst)?;
        }
        if !sl.subselect.is_null() {
            walk_node_for_volatility(sl.subselect, worst)?;
        }
    }
    // Other node types (ColumnRef, A_Const, etc.) contain no functions.

    Ok(())
}

/// Return the worst volatility found in an expression tree.
pub fn worst_volatility(expr: &Expr) -> Result<char, PgTrickleError> {
    let mut worst = 'i';
    collect_volatilities(expr, &mut worst)?;
    Ok(worst)
}

/// Walk an entire OpTree and return the worst volatility found in any
/// expression (target list, WHERE, JOIN conditions, HAVING, aggregates,
/// window functions).
pub fn tree_worst_volatility(tree: &OpTree) -> Result<char, PgTrickleError> {
    let mut worst = 'i';
    tree_collect_volatility(tree, &mut worst)?;
    Ok(worst)
}

/// Walk an entire [`ParseResult`] (tree + CTE registry) for volatility.
pub fn tree_worst_volatility_with_registry(result: &ParseResult) -> Result<char, PgTrickleError> {
    let mut worst = 'i';
    // Check all CTE bodies
    for (_name, body) in &result.cte_registry.entries {
        tree_collect_volatility(body, &mut worst)?;
    }
    tree_collect_volatility(&result.tree, &mut worst)?;
    Ok(worst)
}

pub(crate) fn tree_collect_volatility(
    tree: &OpTree,
    worst: &mut char,
) -> Result<(), PgTrickleError> {
    match tree {
        OpTree::Scan { .. } | OpTree::CteScan { .. } | OpTree::RecursiveSelfRef { .. } => {}
        OpTree::Project {
            expressions, child, ..
        } => {
            for expr in expressions {
                collect_volatilities(expr, worst)?;
            }
            tree_collect_volatility(child, worst)?;
        }
        OpTree::Filter { predicate, child } => {
            collect_volatilities(predicate, worst)?;
            tree_collect_volatility(child, worst)?;
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
            collect_volatilities(condition, worst)?;
            tree_collect_volatility(left, worst)?;
            tree_collect_volatility(right, worst)?;
        }
        OpTree::Aggregate {
            group_by,
            aggregates,
            child,
        } => {
            for expr in group_by {
                collect_volatilities(expr, worst)?;
            }
            for agg in aggregates {
                if let Some(arg) = &agg.argument {
                    collect_volatilities(arg, worst)?;
                }
                if let Some(filter) = &agg.filter {
                    collect_volatilities(filter, worst)?;
                }
                if let Some(second) = &agg.second_arg {
                    collect_volatilities(second, worst)?;
                }
            }
            tree_collect_volatility(child, worst)?;
        }
        OpTree::Distinct { child } | OpTree::Subquery { child, .. } => {
            tree_collect_volatility(child, worst)?;
        }
        OpTree::LateralFunction {
            func_sql: _func_sql,
            child,
            ..
        } => {
            // COR-002 (v0.70.0): Scan the LATERAL function body for volatile
            // calls. Previously only `child` (the left-hand scan) was checked;
            // volatile expressions inside `func_sql` (e.g. `random()`) would
            // bypass the volatile_function_policy check silently.
            //
            // Two-phase scan:
            // 1. Try full semantic parse — uses resolved OpTree for accuracy.
            // 2. On failure (e.g. outer-scope refs, no FROM clause), fall back
            //    to raw AST walk via `scan_sql_for_volatility` which detects
            //    volatile FuncCall nodes without needing catalog resolution.
            //
            // Special case: JSON_TABLE and similar SQL/JSON table-valued
            // functions contain the COLUMNS keyword in their deparsed form.
            // The wrapped SQL `SELECT JSON_TABLE(... COLUMNS (...))` is not a
            // valid standalone statement — raw_parser ereports on COLUMNS in
            // expression context. These constructs are stable (non-volatile),
            // so skip scanning entirely when the COLUMNS keyword is present.
            #[cfg(not(test))]
            {
                if !_func_sql.to_ascii_uppercase().contains(" COLUMNS (") {
                    let wrapped = format!("SELECT {_func_sql}");
                    match parse_defining_query_full(&wrapped) {
                        Ok(inner) => tree_collect_volatility(&inner.tree, worst)?,
                        Err(_) => scan_sql_for_volatility(&wrapped, worst)?,
                    }
                }
            }
            tree_collect_volatility(child, worst)?;
        }
        OpTree::LateralSubquery {
            subquery_sql: _subquery_sql,
            child,
            ..
        } => {
            // COR-002 (v0.70.0): Scan the LATERAL subquery body for volatile
            // expressions. The subquery body is stored as raw SQL and was
            // previously not scanned by the volatility walker.
            //
            // Two-phase scan:
            // 1. Try full semantic parse — uses resolved OpTree for accuracy.
            // 2. On failure (e.g. outer-scope refs, no FROM clause), fall back
            //    to raw AST walk via `scan_sql_for_volatility` which detects
            //    volatile FuncCall nodes without needing catalog resolution.
            //    This correctly handles:
            //    - `SELECT random()::numeric AS noise` (no FROM → full parse
            //      fails, raw walk finds random())
            //    - Correlated `SELECT col FROM t WHERE t.id = outer.id LIMIT 1`
            //      (full parse fails, raw walk finds no volatile fns → safe)
            #[cfg(not(test))]
            {
                match parse_defining_query_full(_subquery_sql) {
                    Ok(inner) => tree_collect_volatility(&inner.tree, worst)?,
                    Err(_) => scan_sql_for_volatility(_subquery_sql, worst)?,
                }
            }
            tree_collect_volatility(child, worst)?;
        }
        OpTree::UnionAll { children } => {
            for child in children {
                tree_collect_volatility(child, worst)?;
            }
        }
        OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
            tree_collect_volatility(left, worst)?;
            tree_collect_volatility(right, worst)?;
        }
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            tree_collect_volatility(base, worst)?;
            tree_collect_volatility(recursive, worst)?;
        }
        OpTree::Window {
            window_exprs,
            partition_by,
            pass_through,
            child,
        } => {
            for we in window_exprs {
                for arg in &we.args {
                    collect_volatilities(arg, worst)?;
                }
                for pb in &we.partition_by {
                    collect_volatilities(pb, worst)?;
                }
                for ob in &we.order_by {
                    collect_volatilities(&ob.expr, worst)?;
                }
            }
            for pb in partition_by {
                collect_volatilities(pb, worst)?;
            }
            for (expr, _) in pass_through {
                collect_volatilities(expr, worst)?;
            }
            tree_collect_volatility(child, worst)?;
        }
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => {
            tree_collect_volatility(subquery, worst)?;
            tree_collect_volatility(child, worst)?;
        }
        // Constant anchors have no expressions to scan — volatility is unaffected.
        OpTree::ConstantSelect { .. } => {}
    }
    Ok(())
}

/// Check if an operator tree is supported for differential maintenance.
pub fn check_ivm_support(tree: &OpTree) -> Result<(), PgTrickleError> {
    check_ivm_support_inner(tree)
}

/// Check if a [`ParseResult`] (tree + CTE registry) is fully supported.
pub fn check_ivm_support_with_registry(result: &ParseResult) -> Result<(), PgTrickleError> {
    // Validate all CTE bodies first
    for (name, body) in &result.cte_registry.entries {
        check_ivm_support_inner(body)
            .map_err(|e| PgTrickleError::UnsupportedOperator(format!("in CTE '{name}': {e}")))?;
    }
    check_ivm_support_inner(&result.tree)
}

/// Check that both children of a join resolve to direct table references.
///
/// Previously used to reject nested joins. Kept for test coverage — the callers
/// in `check_ivm_support_inner()` have been removed now that nested joins are
/// supported in DIFFERENTIAL mode.
#[cfg(test)]
fn check_join_children_are_base_tables(
    left: &OpTree,
    right: &OpTree,
    join_kind: &str,
) -> Result<(), PgTrickleError> {
    if !resolves_to_base_table(left) {
        return Err(PgTrickleError::UnsupportedOperator(format!(
            "nested joins are not supported for DIFFERENTIAL mode: \
             left child of {join_kind} must resolve to a base table, \
             not a {}",
            left.node_kind(),
        )));
    }
    if !resolves_to_base_table(right) {
        return Err(PgTrickleError::UnsupportedOperator(format!(
            "nested joins are not supported for DIFFERENTIAL mode: \
             right child of {join_kind} must resolve to a base table, \
             not a {}",
            right.node_kind(),
        )));
    }
    Ok(())
}

/// Check if an operator tree node resolves to a direct base table reference.
///
/// Returns `true` for `Scan` (a direct table) and for transparent wrappers
/// (`Filter`, `Project`, `Subquery`) that eventually wrap a `Scan`.
/// Returns `false` for joins, aggregates, unions, and other complex nodes
/// that cannot be expressed as a simple `FROM table_name`.
#[cfg(test)]
pub(crate) fn resolves_to_base_table(op: &OpTree) -> bool {
    match op {
        OpTree::Scan { .. } => true,
        OpTree::Filter { child, .. }
        | OpTree::Project { child, .. }
        | OpTree::Subquery { child, .. } => resolves_to_base_table(child),
        OpTree::CteScan { .. } => {
            // CTE scans reference a named CTE — treated as a base relation
            // for the purpose of join child validation.
            true
        }
        OpTree::RecursiveSelfRef { .. } => {
            // Recursive self-references act as table sources within
            // the recursive term of a recursive CTE body.
            true
        }
        OpTree::LateralSubquery { child, .. } | OpTree::LateralFunction { child, .. } => {
            resolves_to_base_table(child)
        }
        _ => false,
    }
}

/// F4 (v0.37.0): Returns true if an expression contains any pgvector distance
/// operator: `<->` (L2), `<=>` (cosine), `<#>` (inner product), `<+>` (L1).
///
/// These operators are non-monotone and non-linear — no differentiation rule
/// exists for them. Stream tables using these in WHERE/ORDER BY predicates
/// are "FULL-fallback safe": they always produce correct results via FULL
/// refresh. This function is used to emit a clear INFO/WARNING instead of a
/// silent fallback.
fn contains_pgvector_distance_op_in_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Raw(s) => {
            s.contains("<->") || s.contains("<=>") || s.contains("<#>") || s.contains("<+>")
        }
        // Recurse into structured expressions if needed in the future.
        // For now, Raw is the primary representation for complex predicates.
        _ => false,
    }
}

pub(crate) fn check_ivm_support_inner(tree: &OpTree) -> Result<(), PgTrickleError> {
    match tree {
        OpTree::Scan { .. } => Ok(()),
        OpTree::Project { child, .. } => check_ivm_support(child),
        OpTree::Filter { predicate, child } => {
            // F4 (v0.37.0): pgvector distance operators in WHERE clauses are
            // FULL-fallback safe — no differentiation rule exists for non-monotone,
            // non-linear operators like <->, <=>, <#>, <+>. Document the fallback
            // so users get a clear INFO message instead of a silent FULL refresh.
            if contains_pgvector_distance_op_in_expr(predicate) {
                return Err(PgTrickleError::UnsupportedOperator(
                    "pgvector distance operator (<->, <=>, <#>, <+>) in WHERE clause — \
                     falling back to FULL refresh (expected and safe: distance operators \
                     are non-monotone and cannot be differentiated)"
                        .to_string(),
                ));
            }
            check_ivm_support(child)
        }
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => {
            // Nested joins (join-of-join) are supported: the diff engine
            // handles recursive delta computation over the join tree.
            check_ivm_support(left)?;
            check_ivm_support(right)
        }
        OpTree::Aggregate {
            child, aggregates, ..
        } => {
            for agg in aggregates {
                let needs_second_arg = matches!(
                    agg.function,
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
                );
                let needs_argument = needs_second_arg
                    || matches!(
                        agg.function,
                        AggFunc::StddevPop
                            | AggFunc::StddevSamp
                            | AggFunc::VarPop
                            | AggFunc::VarSamp
                    );
                if needs_argument
                    && (agg.argument.is_none() || (needs_second_arg && agg.second_arg.is_none()))
                {
                    return Err(PgTrickleError::UnsupportedOperator(format!(
                        "{} aggregate '{}' has an unresolved operand signature; \
                         use FULL or provide the required operand(s)",
                        agg.function.sql_name(),
                        agg.alias
                    )));
                }
                if (agg.function.needs_sum_of_squares() || agg.function.needs_cross_products())
                    && agg.statistical_accumulator_type().is_none()
                {
                    return Err(PgTrickleError::UnsupportedOperator(format!(
                        "{} aggregate '{}' has an unresolved operand signature; \
                         use FULL or provide a supported signature",
                        agg.function.sql_name(),
                        agg.alias
                    )));
                }
                match agg.function {
                    AggFunc::Count
                    | AggFunc::CountStar
                    | AggFunc::Sum
                    | AggFunc::Avg
                    | AggFunc::Min
                    | AggFunc::Max
                    | AggFunc::BoolAnd
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
                    | AggFunc::StddevPop
                    | AggFunc::StddevSamp
                    | AggFunc::VarPop
                    | AggFunc::VarSamp
                    | AggFunc::AnyValue
                    | AggFunc::Mode
                    | AggFunc::PercentileCont
                    | AggFunc::PercentileDisc
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
                    | AggFunc::XmlAgg
                    | AggFunc::HypRank
                    | AggFunc::HypDenseRank
                    | AggFunc::HypPercentRank
                    | AggFunc::HypCumeDist
                    | AggFunc::ComplexExpression(_)
                    | AggFunc::UserDefined(_)
                    // F4: vector aggregates — supported via group-rescan strategy
                    | AggFunc::VectorAvg
                    | AggFunc::VectorSum => {}
                }
            }
            check_ivm_support(child)
        }
        OpTree::Distinct { child } => check_ivm_support(child),
        OpTree::UnionAll { children } => {
            for child in children {
                check_ivm_support(child)?;
            }
            Ok(())
        }
        OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
            check_ivm_support(left)?;
            check_ivm_support(right)
        }
        OpTree::Subquery { child, .. } => check_ivm_support(child),
        // CteScan delegates to its body via the registry at a higher level.
        // The CTE body itself is validated when first parsed.
        OpTree::CteScan { .. } => Ok(()),
        // Recursive CTEs are supported for differential mode via semi-naive evaluation.
        // Both base and recursive terms must be independently DVM-compatible.
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            check_ivm_support_inner(base)?;
            check_ivm_support_inner(recursive)
        }
        // Self-references are always valid within a recursive CTE context.
        OpTree::RecursiveSelfRef { .. } => Ok(()),
        // Window functions use partition-based recomputation.
        OpTree::Window { child, .. } => check_ivm_support(child),
        // Lateral SRFs use row-scoped recomputation.
        // COR-002 (v0.70.0): Also check the function body for unsupported constructs.
        OpTree::LateralFunction {
            func_sql: _func_sql,
            child,
            ..
        } => {
            #[cfg(not(test))]
            {
                // COR-002: JSON_TABLE and similar SQL/JSON table-valued functions
                // contain the COLUMNS keyword in their deparsed form.
                // "SELECT JSON_TABLE(... COLUMNS (...))" is not valid standalone SQL —
                // raw_parser ereports on COLUMNS in expression context, causing a
                // PostgreSQL panic that propagates past `if let Ok`.
                // JSON_TABLE has no unsupported IVM constructs, so skip safely.
                if !_func_sql.to_ascii_uppercase().contains(" COLUMNS (") {
                    let wrapped = format!("SELECT {_func_sql}");
                    match parse_defining_query_full(&wrapped) {
                        Ok(inner) => check_ivm_support_inner(&inner.tree)?,
                        Err(e) => {
                            return Err(PgTrickleError::UnsupportedOperator(format!(
                                "cannot inspect LATERAL function body: {e}"
                            )));
                        }
                    }
                }
            }
            check_ivm_support(child)
        }
        // Lateral subqueries use row-scoped recomputation.
        // COR-002 (v0.70.0): Also check the subquery body for unsupported constructs.
        OpTree::LateralSubquery {
            subquery_sql: _subquery_sql,
            child,
            ..
        } => {
            #[cfg(not(test))]
            {
                match parse_defining_query_full(_subquery_sql) {
                    Ok(inner) => check_ivm_support_inner(&inner.tree)?,
                    Err(e) => {
                        return Err(PgTrickleError::UnsupportedOperator(format!(
                            "cannot inspect LATERAL subquery body: {e}"
                        )));
                    }
                }
            }
            check_ivm_support(child)
        }
        // Semi-join (EXISTS / IN subquery): both sides must be DVM-compatible.
        OpTree::SemiJoin { left, right, .. } | OpTree::AntiJoin { left, right, .. } => {
            check_ivm_support(left)?;
            check_ivm_support(right)
        }
        // Scalar subquery: both the outer child and inner subquery must be DVM-compatible.
        OpTree::ScalarSubquery {
            subquery, child, ..
        } => {
            check_ivm_support(subquery)?;
            check_ivm_support(child)
        }
        // Constant anchors (no FROM) are always DVM-compatible — they produce no delta.
        OpTree::ConstantSelect { .. } => Ok(()),
    }
}

// ── IMMEDIATE-mode validation ──────────────────────────────────────────────

/// Validate that a defining query is compatible with IMMEDIATE mode.
///
/// IMMEDIATE mode uses statement-level AFTER triggers with transition tables
/// and does not support the full range of SQL constructs that DIFFERENTIAL
/// mode handles. This function parses the defining query and rejects
/// unsupported constructs with an actionable error message.
///
/// **Supported in IMMEDIATE mode:**
/// - Simple SELECT (Scan, Filter, Project)
/// - JOINs (INNER, LEFT, FULL)
/// - GROUP BY with standard aggregates
/// - DISTINCT
/// - Simple subqueries in FROM
/// - Non-recursive CTEs
/// - EXISTS/IN subqueries (SemiJoin/AntiJoin)
///
/// **Rejected:** (reserved for future restrictions)
/// - None currently — recursive CTEs (Task 5.1) and WhTopK (Task 5.2) are
///   both now supported with appropriate GUC gates and runtime warnings.
pub fn validate_immediate_mode_support(defining_query: &str) -> Result<(), PgTrickleError> {
    let result = parse_defining_query_full(defining_query)?;

    // First validate general DVM support (aggregates, etc.).
    check_ivm_support_with_registry(&result)?;

    // Then validate IMMEDIATE-specific restrictions.
    check_immediate_support(&result.tree)?;

    // Also check CTE bodies.
    for entry in &result.cte_registry.entries {
        check_immediate_support(&entry.1)?;
    }

    Ok(())
}

/// Walk an OpTree and reject constructs not yet supported in IMMEDIATE mode.
fn check_immediate_support(tree: &OpTree) -> Result<(), PgTrickleError> {
    match tree {
        // Leaf nodes — always OK.
        OpTree::Scan { .. } | OpTree::CteScan { .. } | OpTree::RecursiveSelfRef { .. } => Ok(()),

        // Transparent wrappers — recurse into child.
        OpTree::Project { child, .. }
        | OpTree::Filter { child, .. }
        | OpTree::Distinct { child }
        | OpTree::Subquery { child, .. } => check_immediate_support(child),

        // Joins — recurse into both sides.
        OpTree::InnerJoin { left, right, .. }
        | OpTree::LeftJoin { left, right, .. }
        | OpTree::FullJoin { left, right, .. } => {
            check_immediate_support(left)?;
            check_immediate_support(right)
        }

        // Aggregates — allowed, recurse into child.
        OpTree::Aggregate { child, .. } => check_immediate_support(child),

        // UNION ALL — allowed (each branch is a scan/filter/project chain).
        OpTree::UnionAll { children } => {
            for child in children {
                check_immediate_support(child)?;
            }
            Ok(())
        }

        // INTERSECT/EXCEPT — allowed.
        OpTree::Intersect { left, right, .. } | OpTree::Except { left, right, .. } => {
            check_immediate_support(left)?;
            check_immediate_support(right)
        }

        // SemiJoin/AntiJoin (EXISTS/IN subqueries) — allowed.
        OpTree::SemiJoin { left, right, .. } | OpTree::AntiJoin { left, right, .. } => {
            check_immediate_support(left)?;
            check_immediate_support(right)
        }

        // Window functions — partition-based recomputation via DVM engine.
        // The delta is derived from child Scan nodes which support both
        // ChangeBuffer and TransitionTable modes.
        OpTree::Window { child, .. } => check_immediate_support(child),

        // LATERAL inner-source changes do not have a transition-table
        // implementation yet. Outer-only lateral expansion remains eligible.
        OpTree::LateralSubquery {
            subquery_source_oids,
            child,
            ..
        } => {
            if !subquery_source_oids.is_empty() {
                return Err(PgTrickleError::UnsupportedOperator(
                    "IMMEDIATE LATERAL with mutable inner sources is not supported; \
                     use DIFFERENTIAL, FULL, or AUTO."
                        .into(),
                ));
            }
            check_immediate_support(child)
        }

        // LATERAL set-returning functions (unnest(), jsonb_array_elements())
        // — row-scoped recomputation via child delta.
        OpTree::LateralFunction { child, .. } => check_immediate_support(child),

        // Scalar subqueries in SELECT — delta derived from child and
        // subquery Scan nodes.
        OpTree::ScalarSubquery {
            child, subquery, ..
        } => {
            check_immediate_support(child)?;
            check_immediate_support(subquery)
        }

        // ── Recursive CTEs ────────────────────────────────────────
        // Task 5.1: Allow recursive CTEs in IMMEDIATE mode.
        // Semi-naive evaluation works with DeltaSource::TransitionTable;
        // emit a warning about potential stack-depth issues for deep recursion.
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            pgrx::warning!(
                "pg_trickle: WITH RECURSIVE in IMMEDIATE mode uses semi-naive evaluation \
                 inside the trigger.  A depth counter guards against infinite loops \
                 (pg_trickle.ivm_recursive_max_depth, default 100).  For very deep \
                 hierarchies, raise this GUC or watch for \
                 'stack depth limit exceeded' errors."
            );
            check_immediate_support(base)?;
            check_immediate_support(recursive)
        }
        // Constant anchors (no FROM) are always IMMEDIATE-compatible.
        OpTree::ConstantSelect { .. } => Ok(()),
    }
}

// ── Monotonicity Checker (CYC-2) ──────────────────────────────────────────

/// Check if an OpTree is monotone (safe for cyclic fixed-point iteration).
///
/// A monotone query is one where adding input rows can only add output rows,
/// never remove them. This property guarantees convergence when stream tables
/// form circular dependencies, because:
/// - Each iteration can only add rows (monotonicity)
/// - The result set is bounded (finite input → finite output)
/// - The sequence must reach a fixed point in finite steps
///
/// Returns `Ok(())` if all operators are monotone. Returns `Err` with a
/// descriptive error if a non-monotone operator is found.
///
/// # Non-monotone operators
///
/// - **Aggregate**: COUNT/SUM can decrease when rows are deleted
/// - **Except**: adding rows to the right branch removes output
/// - **Window functions**: rank can change, causing rows to appear/disappear
/// - **AntiJoin** (NOT EXISTS/NOT IN): adding right rows removes output
///
/// # References
///
/// - Datalog stratification theory
/// - DBSP (Dynamic Batch Stream Processing) monotone fragment
pub fn check_monotonicity(tree: &OpTree) -> Result<(), PgTrickleError> {
    match tree {
        // Leaf nodes — always monotone.
        OpTree::Scan { .. } | OpTree::CteScan { .. } | OpTree::RecursiveSelfRef { .. } => Ok(()),

        // Transparent wrappers — monotonicity depends on child.
        OpTree::Project { child, .. }
        | OpTree::Filter { child, .. }
        | OpTree::Distinct { child }
        | OpTree::Subquery { child, .. } => check_monotonicity(child),

        // INNER JOIN preserves monotonicity when both inputs do.
        OpTree::InnerJoin { left, right, .. } => {
            check_monotonicity(left)?;
            check_monotonicity(right)
        }

        // Outer joins can replace NULL-extended rows when the other side grows.
        OpTree::LeftJoin { .. } | OpTree::FullJoin { .. } => {
            Err(PgTrickleError::UnsupportedOperator(
                "LEFT/FULL JOIN is not monotone for circular dependencies — \
                 adding matching rows can replace NULL-extended output."
                    .into(),
            ))
        }

        // UNION ALL — monotone if all children are.
        OpTree::UnionAll { children } => {
            for child in children {
                check_monotonicity(child)?;
            }
            Ok(())
        }

        // INTERSECT is conservatively excluded with the other unproven set
        // operators until its private state has a circular-query proof.
        OpTree::Intersect { .. } => Err(PgTrickleError::UnsupportedOperator(
            "INTERSECT is not admitted in circular dependencies until its \
             incremental state has been proven monotone."
                .into(),
        )),

        // SemiJoin (EXISTS/IN) — monotone (adding right rows adds left matches).
        OpTree::SemiJoin { left, right, .. } => {
            check_monotonicity(left)?;
            check_monotonicity(right)
        }

        // Recursive CTEs — monotone if both terms are.
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            check_monotonicity(base)?;
            check_monotonicity(recursive)
        }

        OpTree::LateralFunction { .. } | OpTree::LateralSubquery { .. } => {
            Err(PgTrickleError::UnsupportedOperator(
                "LATERAL is not monotone for circular dependencies — \
                 row-scoped expansion can replace or retract output."
                    .into(),
            ))
        }

        OpTree::ScalarSubquery { .. } => Err(PgTrickleError::UnsupportedOperator(
            "Scalar subqueries are not monotone for circular dependencies — \
             cardinality or values can replace existing output."
                .into(),
        )),

        // ── Non-monotone operators ────────────────────────────────────
        OpTree::Aggregate { .. } => Err(PgTrickleError::UnsupportedOperator(
            "Aggregate is not monotone — cannot participate in a circular dependency. \
             Aggregates (COUNT, SUM, AVG, etc.) can decrease when input rows are removed, \
             which prevents fixed-point convergence."
                .into(),
        )),

        OpTree::Except { .. } => Err(PgTrickleError::UnsupportedOperator(
            "EXCEPT is not monotone — cannot participate in a circular dependency. \
             Adding rows to the right branch of EXCEPT removes output rows, \
             which prevents fixed-point convergence."
                .into(),
        )),

        OpTree::Window { .. } => Err(PgTrickleError::UnsupportedOperator(
            "Window functions are not monotone — cannot participate in a circular dependency. \
             Window function results (ROW_NUMBER, RANK, etc.) change when input rows are \
             added or removed, which prevents fixed-point convergence."
                .into(),
        )),

        OpTree::AntiJoin { .. } => Err(PgTrickleError::UnsupportedOperator(
            "NOT EXISTS / NOT IN is not monotone — cannot participate in a circular dependency. \
             Adding rows to the subquery side removes output rows, which prevents \
             fixed-point convergence."
                .into(),
        )),
        // Constant anchors are trivially monotone — they produce no rows and have
        // no source tables, so adding more data can only grow the output, never shrink it.
        OpTree::ConstantSelect { .. } => Ok(()),
    }
}

/// Check the main query and every parsed CTE body before circular admission.
pub fn check_monotonicity_with_registry(result: &ParseResult) -> Result<(), PgTrickleError> {
    check_monotonicity(&result.tree)?;
    for (_, body) in &result.cte_registry.entries {
        check_monotonicity(body)?;
    }
    Ok(())
}

#[cfg(test)]
mod monotonicity_tests {
    use super::*;

    /// Helper: create a minimal Scan node.
    fn scan() -> OpTree {
        OpTree::Scan {
            table_oid: 1,
            table_name: "t".to_string(),
            schema: "public".to_string(),
            columns: vec![],
            pk_columns: vec![],
            alias: "t".to_string(),
        }
    }

    #[test]
    fn test_scan_is_monotone() {
        assert!(check_monotonicity(&scan()).is_ok());
    }

    #[test]
    fn test_filter_project_is_monotone() {
        let tree = OpTree::Filter {
            predicate: Expr::Literal("true".into()),
            child: Box::new(OpTree::Project {
                expressions: vec![],
                aliases: vec![],
                child: Box::new(scan()),
            }),
        };
        assert!(check_monotonicity(&tree).is_ok());
    }

    #[test]
    fn test_inner_join_is_monotone() {
        let tree = OpTree::InnerJoin {
            condition: Expr::Literal("true".into()),
            left: Box::new(scan()),
            right: Box::new(scan()),
        };
        assert!(check_monotonicity(&tree).is_ok());
    }

    #[test]
    fn test_left_join_is_not_monotone() {
        let tree = OpTree::LeftJoin {
            condition: Expr::Literal("true".into()),
            left: Box::new(scan()),
            right: Box::new(scan()),
        };
        assert!(check_monotonicity(&tree).is_err());
    }

    #[test]
    fn test_union_all_is_monotone() {
        let tree = OpTree::UnionAll {
            children: vec![scan(), scan()],
        };
        assert!(check_monotonicity(&tree).is_ok());
    }

    #[test]
    fn test_distinct_is_monotone() {
        let tree = OpTree::Distinct {
            child: Box::new(scan()),
        };
        assert!(check_monotonicity(&tree).is_ok());
    }

    #[test]
    fn test_statistical_aggregate_missing_operand_is_full_only() {
        let agg = AggExpr {
            function: AggFunc::Corr,
            argument: Some(Expr::ColumnRef {
                table_alias: None,
                column_name: "y".into(),
            }),
            alias: "corr".into(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: Some(StatisticalAggSupport::with_accumulator(
                PG_FLOAT8_TYPE_OID,
                "double precision",
            )),
        };
        let tree = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![agg],
            child: Box::new(scan()),
        };
        assert!(check_ivm_support(&tree).is_err());
    }

    #[test]
    fn test_statistical_aggregate_unresolved_signature_is_full_only() {
        let agg = AggExpr {
            function: AggFunc::StddevPop,
            argument: Some(Expr::ColumnRef {
                table_alias: None,
                column_name: "amount".into(),
            }),
            alias: "sd".into(),
            is_distinct: false,
            filter: None,
            second_arg: None,
            order_within_group: None,
            statistical_support: Some(StatisticalAggSupport::with_location(12)),
        };
        let tree = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![agg],
            child: Box::new(scan()),
        };
        assert!(check_ivm_support(&tree).is_err());
    }

    #[test]
    fn test_semi_join_is_monotone() {
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("true".into()),
            left: Box::new(scan()),
            right: Box::new(scan()),
        };
        assert!(check_monotonicity(&tree).is_ok());
    }

    #[test]
    fn test_intersect_is_not_monotone() {
        let tree = OpTree::Intersect {
            left: Box::new(scan()),
            right: Box::new(scan()),
            all: false,
        };
        assert!(check_monotonicity(&tree).is_err());
    }

    #[test]
    fn test_aggregate_is_not_monotone() {
        let tree = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![],
            child: Box::new(scan()),
        };
        let err = check_monotonicity(&tree).unwrap_err();
        assert!(format!("{}", err).contains("Aggregate"));
    }

    #[test]
    fn test_except_is_not_monotone() {
        let tree = OpTree::Except {
            left: Box::new(scan()),
            right: Box::new(scan()),
            all: false,
        };
        let err = check_monotonicity(&tree).unwrap_err();
        assert!(format!("{}", err).contains("EXCEPT"));
    }

    #[test]
    fn test_window_is_not_monotone() {
        let tree = OpTree::Window {
            window_exprs: vec![],
            partition_by: vec![],
            pass_through: vec![],
            child: Box::new(scan()),
        };
        let err = check_monotonicity(&tree).unwrap_err();
        assert!(format!("{}", err).contains("Window"));
    }

    #[test]
    fn test_anti_join_is_not_monotone() {
        let tree = OpTree::AntiJoin {
            condition: Expr::Literal("true".into()),
            left: Box::new(scan()),
            right: Box::new(scan()),
        };
        let err = check_monotonicity(&tree).unwrap_err();
        assert!(format!("{}", err).contains("NOT EXISTS"));
    }

    #[test]
    fn test_nested_non_monotone_detected() {
        // Filter { child: Aggregate { ... } } → non-monotone.
        let tree = OpTree::Filter {
            predicate: Expr::Literal("true".into()),
            child: Box::new(OpTree::Aggregate {
                group_by: vec![],
                aggregates: vec![],
                child: Box::new(scan()),
            }),
        };
        assert!(check_monotonicity(&tree).is_err());
    }

    #[test]
    fn test_join_with_non_monotone_child() {
        // InnerJoin where right side has an Aggregate → non-monotone.
        let tree = OpTree::InnerJoin {
            condition: Expr::Literal("true".into()),
            left: Box::new(scan()),
            right: Box::new(OpTree::Aggregate {
                group_by: vec![],
                aggregates: vec![],
                child: Box::new(scan()),
            }),
        };
        assert!(check_monotonicity(&tree).is_err());
    }

    #[test]
    fn test_admission_matrix_full_auto_and_explicit_modes() {
        let issue = ValidationIssue {
            code: "TEST",
            operator: "INTERSECT",
            reason: "unproven".into(),
            hint: "use FULL".into(),
        };
        let full_only = IncrementalAdmission::FullOnly(vec![issue]);
        assert!(matches!(
            resolve_incremental_mode(RefreshMode::Full, false, &full_only),
            Ok(RefreshMode::Full)
        ));
        assert!(matches!(
            resolve_incremental_mode(RefreshMode::Differential, true, &full_only),
            Ok(RefreshMode::Full)
        ));
        assert!(resolve_incremental_mode(RefreshMode::Differential, false, &full_only).is_err());
        assert!(resolve_incremental_mode(RefreshMode::Immediate, false, &full_only).is_err());
        assert!(matches!(
            resolve_incremental_mode(
                RefreshMode::Differential,
                false,
                &IncrementalAdmission::Proven
            ),
            Ok(RefreshMode::Differential)
        ));
    }

    #[test]
    fn test_scalar_and_lateral_are_not_monotone() {
        let scalar = OpTree::ScalarSubquery {
            alias: "s".into(),
            subquery_source_oids: vec![],
            subquery: Box::new(scan()),
            child: Box::new(scan()),
        };
        assert!(check_monotonicity(&scalar).is_err());

        let lateral = OpTree::LateralFunction {
            func_sql: "unnest(t.tags)".into(),
            alias: "x".into(),
            column_aliases: vec![],
            with_ordinality: false,
            child: Box::new(scan()),
        };
        assert!(check_monotonicity(&lateral).is_err());
    }

    #[test]
    fn test_inspection_failure_is_full_only() {
        let result = ParseResult {
            tree: OpTree::Filter {
                predicate: Expr::Raw("uninspectable_expression".into()),
                child: Box::new(scan()),
            },
            cte_registry: CteRegistry::default(),
            has_recursion: false,
            warnings: vec![],
            window_strategy: None,
        };
        let admission = incremental_admission(&result, 'v', false).unwrap();
        assert!(matches!(admission, IncrementalAdmission::FullOnly(_)));
        assert!(resolve_incremental_mode(RefreshMode::Differential, false, &admission).is_err());
        assert!(matches!(
            resolve_incremental_mode(RefreshMode::Differential, true, &admission),
            Ok(RefreshMode::Full)
        ));
    }

    #[test]
    fn test_set_and_mutable_lateral_admission_is_full_only() {
        let set_result = ParseResult {
            tree: OpTree::Except {
                left: Box::new(scan()),
                right: Box::new(scan()),
                all: false,
            },
            cte_registry: CteRegistry::default(),
            has_recursion: false,
            warnings: vec![],
            window_strategy: None,
        };
        assert!(matches!(
            incremental_admission(&set_result, 'i', false).unwrap(),
            IncrementalAdmission::FullOnly(_)
        ));

        let lateral_result = ParseResult {
            tree: OpTree::LateralSubquery {
                subquery_sql: "SELECT id FROM inner_t".into(),
                alias: "inner".into(),
                column_aliases: vec![],
                output_cols: vec!["id".into()],
                is_left_join: false,
                subquery_source_oids: vec![42],
                correlation_predicates: vec![],
                child: Box::new(scan()),
            },
            cte_registry: CteRegistry::default(),
            has_recursion: false,
            warnings: vec![],
            window_strategy: None,
        };
        assert!(matches!(
            incremental_admission(&lateral_result, 'i', true).unwrap(),
            IncrementalAdmission::FullOnly(_)
        ));
    }

    #[test]
    fn test_lateral_unresolved_dependency_is_full_only() {
        let lateral_result = ParseResult {
            tree: OpTree::LateralSubquery {
                subquery_sql: "SELECT x FROM inner_t i WHERE i.fk = missing.id".into(),
                alias: "inner".into(),
                column_aliases: vec![],
                output_cols: vec!["x".into()],
                is_left_join: false,
                subquery_source_oids: vec![42],
                correlation_predicates: vec![CorrelationPredicate {
                    outer_col: "missing_id".into(),
                    inner_alias: "i".into(),
                    inner_col: "fk".into(),
                    inner_oid: 99,
                }],
                child: Box::new(OpTree::Scan {
                    table_oid: 1,
                    table_name: "outer_t".into(),
                    schema: "public".into(),
                    columns: vec![Column {
                        name: "id".into(),
                        type_oid: 23,
                        is_nullable: false,
                    }],
                    pk_columns: vec!["id".into()],
                    alias: "o".into(),
                }),
            },
            cte_registry: CteRegistry::default(),
            has_recursion: false,
            warnings: vec![],
            window_strategy: None,
        };
        let admission = incremental_admission(&lateral_result, 'i', false).unwrap();
        let IncrementalAdmission::FullOnly(issues) = admission else {
            panic!("unresolved LATERAL dependency must not be admitted");
        };
        assert!(
            issues
                .iter()
                .any(|i| i.code == "DVM-81-5-LATERAL-DEPENDENCY")
        );
    }

    #[test]
    fn test_lateral_missing_outer_identity_is_full_only() {
        let lateral_result = ParseResult {
            tree: OpTree::LateralSubquery {
                subquery_sql: "SELECT x FROM inner_t i".into(),
                alias: "inner".into(),
                column_aliases: vec![],
                output_cols: vec!["x".into()],
                is_left_join: false,
                subquery_source_oids: vec![42],
                correlation_predicates: vec![],
                child: Box::new(OpTree::InnerJoin {
                    condition: Expr::Literal("TRUE".into()),
                    left: Box::new(scan()),
                    right: Box::new(scan()),
                }),
            },
            cte_registry: CteRegistry::default(),
            has_recursion: false,
            warnings: vec![],
            window_strategy: None,
        };
        let admission = incremental_admission(&lateral_result, 'i', false).unwrap();
        let IncrementalAdmission::FullOnly(issues) = admission else {
            panic!("complex outer identity must not be admitted");
        };
        assert!(issues.iter().any(|i| i.code == "DVM-81-5-LATERAL-IDENTITY"));
    }
}
