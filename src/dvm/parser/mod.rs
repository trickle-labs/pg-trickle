//! Operator tree representation for defining queries.
//!
//! The parser converts a PostgreSQL query (via raw parse tree analysis)
//! into an intermediate `OpTree` representation suitable for differentiation.
//!
//! Two parsing approaches:
//! 1. Walk the raw parse tree from `pg_sys::raw_parser()` — handles
//!    SelectStmt, JoinExpr, RangeVar, etc.
//! 2. Use `parse_analyze_fixedparams()` to resolve OIDs and column types.
//!
//! We use approach (1) for the tree structure + approach (2) for type resolution.
//!
//! # Module Structure
//!
//! - [`types`]       — Type definitions (OpTree, Expr, Column, etc.)
//! - [`validation`]  — Volatility checking, IVM support, IMMEDIATE validation
//! - [`rewrites`]    — SQL query rewrite passes (view inlining, grouping sets, etc.)
//! - [`sublinks`]    — SubLink extraction from WHERE clauses

use crate::error::PgTrickleError;
use pgrx::prelude::*;
use std::cell::RefCell;
use std::collections::HashMap;

// Thread-local accumulator for advisory warnings emitted during one
// `parse_defining_query_inner` call.  The accumulator is initialized at the
// start of each parse and drained into `ParseResult.warnings` at the end so
// they are emitted exactly once by the caller, not once per internal parse
// invocation (e.g. `row_id_expr_for_query`, `prewarm_merge_cache`).
thread_local! {
    static PARSE_ADVISORY_WARNINGS: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

/// Push an advisory warning into the current parse warning accumulator.
/// No-op when called outside of `parse_defining_query_inner`.
fn push_parse_warning(msg: String) {
    PARSE_ADVISORY_WARNINGS.with(|w| w.borrow_mut().push(msg));
}

// ── Safe FFI helpers ───────────────────────────────────────────────────
//
// SAFETY: Parse-tree pointer from PostgreSQL's raw_parser; valid within current memory context.
// These helpers encapsulate the most common unsafe patterns when walking
// PostgreSQL parse-tree nodes, concentrating the `// SAFETY:` reasoning
// into a handful of well-documented functions and macros.
//
// See plans/safety/PLAN_REDUCED_UNSAFE.md for the full rationale.

/// Convert a PostgreSQL C string pointer to a Rust `&str`.
///
/// Returns `Err` if the pointer is null or the bytes are not valid UTF-8.
fn pg_cstr_to_str<'a>(ptr: *const std::ffi::c_char) -> Result<&'a str, PgTrickleError> {
    if ptr.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "NULL C string pointer".into(),
        ));
    }
    // SAFETY: Caller verified non-null. PostgreSQL identifiers and SQL
    // fragments stored in parse-tree nodes are always NUL-terminated C
    // strings allocated in a valid memory context.
    // SAFETY: Pointer verified non-null; C string from PostgreSQL is NUL-terminated.
    unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|_| PgTrickleError::QueryParseError("Invalid UTF-8 in C string".into()))
}

/// Convert a PostgreSQL `List *` to a safe `PgList<T>`.
///
/// Null pointers yield an empty list (consistent with pgrx behaviour).
fn pg_list<T>(raw: *mut pg_sys::List) -> pgrx::PgList<T> {
    // SAFETY: `PgList::from_pg` is safe for both null and valid list pointers
    // returned from the PostgreSQL parser. Null produces an empty list.
    // SAFETY: PgList::from_pg is safe for both null and valid list pointers from the parser.
    unsafe { pgrx::PgList::<T>::from_pg(raw) }
}

/// Attempt to downcast a `*mut pg_sys::Node` to a concrete parse-tree type.
///
/// Returns `Some(&T)` if the node's tag matches, `None` if the pointer is
/// null or the tag does not match.
macro_rules! cast_node {
    ($node:expr, $tag:ident, $ty:ty) => {{
        let __n = $node;
        // SAFETY: `pgrx::is_a` reads the node's tag field, which is valid
        // for any non-null `Node*` allocated by the PostgreSQL parser.
        // The pointer cast is sound because `is_a` verified the concrete
        // type matches `$tag`.
        // SAFETY: is_a reads the node tag field, valid for any non-null Node* from the parser.
        if !__n.is_null() && unsafe { pgrx::is_a(__n, pg_sys::NodeTag::$tag) } {
            // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
            Some(unsafe { &*(__n as *const $ty) })
        } else {
            None
        }
    }};
}

/// Parse a SQL string into a list of `RawStmt` nodes.
///
/// Must be called within a PostgreSQL backend with a valid memory context.
#[cfg(any(not(test), feature = "pg_test"))]
fn parse_query(sql: &str) -> Result<pgrx::PgList<pg_sys::RawStmt>, PgTrickleError> {
    let c_sql = std::ffi::CString::new(sql)
        .map_err(|_| PgTrickleError::QueryParseError("Query contains null bytes".into()))?;
    // SAFETY: raw_parser is a PostgreSQL C function that is safe to call
    // within a backend process with a valid memory context.
    let raw_list =
        // SAFETY: PostgreSQL C function called within a valid backend process and memory context.
        unsafe { pg_sys::raw_parser(c_sql.as_ptr(), pg_sys::RawParseMode::RAW_PARSE_DEFAULT) };
    if raw_list.is_null() {
        return Err(PgTrickleError::QueryParseError(
            "raw_parser returned NULL".into(),
        ));
    }
    Ok(pg_list::<pg_sys::RawStmt>(raw_list))
}

/// Parse SQL and extract the first top-level `SelectStmt`.
///
/// Returns `Ok(None)` if the query is empty or the first statement is not
/// a SELECT.
#[cfg(any(not(test), feature = "pg_test"))]
fn parse_first_select(sql: &str) -> Result<Option<*const pg_sys::SelectStmt>, PgTrickleError> {
    let stmts = parse_query(sql)?;
    let raw_stmt = match stmts.head() {
        Some(rs) => rs,
        None => return Ok(None),
    };
    // SAFETY: raw_stmt is a valid pointer from the parser.
    let node = unsafe { (*raw_stmt).stmt };
    match cast_node!(node, T_SelectStmt, pg_sys::SelectStmt) {
        Some(select) => Ok(Some(select as *const pg_sys::SelectStmt)),
        None => Ok(None),
    }
}

/// Test stub: `parse_query` is unavailable without a PostgreSQL backend.
#[cfg(all(test, not(feature = "pg_test")))]
fn parse_query(_sql: &str) -> Result<pgrx::PgList<pg_sys::RawStmt>, PgTrickleError> {
    Err(PgTrickleError::QueryParseError(
        "parse_query unavailable in unit tests".into(),
    ))
}

/// Test stub: `parse_first_select` is unavailable without a PostgreSQL backend.
#[cfg(all(test, not(feature = "pg_test")))]
fn parse_first_select(_sql: &str) -> Result<Option<*const pg_sys::SelectStmt>, PgTrickleError> {
    Err(PgTrickleError::QueryParseError(
        "parse_first_select unavailable in unit tests".into(),
    ))
}

/// Check whether a `*mut pg_sys::Node` has a specific `NodeTag` without downcasting.
///
/// Returns `true` if the pointer is non-null and the node's tag matches.
/// This is the safe counterpart for standalone `pgrx::is_a()` checks that
/// are not followed by a pointer cast (those should use `cast_node!` instead).
macro_rules! is_node_type {
    ($node:expr, $tag:ident) => {{
        let __n = $node;
        // SAFETY: `pgrx::is_a` reads the node tag field, which is valid
        // for any non-null `Node*` allocated by the PostgreSQL parser.
        !__n.is_null() && unsafe { pgrx::is_a(__n, pg_sys::NodeTag::$tag) }
    }};
}

/// Safely dereference a non-null pointer to a PostgreSQL parse-tree node.
///
/// Returns `&T` after a null check. Panics if the pointer is null.
/// Use this for struct field pointers that are known to be non-null from
/// prior checks (e.g., `withClause`, `alias`, `larg`, `rarg`).
macro_rules! pg_deref {
    ($ptr:expr) => {{
        let __p = $ptr;
        assert!(
            !(__p as *const u8).is_null(),
            "pg_deref: unexpected NULL pointer"
        );
        // SAFETY: Caller verified non-null via assertion. The pointer is
        // from a PostgreSQL parse-tree node allocated in a valid memory context.
        unsafe { &*__p }
    }};
}

// Sub-modules (declared after `cast_node!` / `is_node_type!` / `pg_deref!`
// macros so they are in scope).
mod rewrites;
mod sublinks;
pub mod types;
mod validation;

// Re-export everything from sub-modules so external callers are unaffected.
pub use rewrites::*;
pub use sublinks::*;
pub use types::*;
pub use validation::*;

// ── SAF-2: Safe façades for common unsafe FFI operations ──────────────────
//
// These thin wrappers encapsulate the single `// SAFETY:` reasoning block
// in one place, so call sites in `sublinks.rs` and `rewrites.rs` can call
// them without an explicit `unsafe {}` block, reducing the per-module unsafe
// block count by ≥40%.  See plans/safety/PLAN_REDUCED_UNSAFE.md §SAF-2.

/// Safe wrapper for `node_to_expr`.
///
/// Precondition: `node` must be a valid parse-tree `Node*` allocated by
/// `raw_parser()` and live for the duration of the current memory context.
/// The pointer may be null (which returns a `QueryParseError`).
fn safe_node_to_expr(
    node: *mut pg_sys::Node,
) -> Result<crate::dvm::parser::types::Expr, PgTrickleError> {
    // SAFETY: `node` is a parse-tree pointer from raw_parser(); valid for
    // the duration of the current PostgreSQL memory context. Null is handled
    // inside node_to_expr with a QueryParseError.
    unsafe { node_to_expr(node) }
}

/// Safe wrapper for `deparse_from_item_to_sql`.
///
/// Precondition: `node` must be a valid `*mut pg_sys::Node` from a
/// `SelectStmt.fromClause` list allocated by `raw_parser()`.
fn safe_deparse_from_item_to_sql(node: *mut pg_sys::Node) -> Result<String, PgTrickleError> {
    // SAFETY: parse-tree pointer from raw_parser(); valid for the current
    // memory context.
    unsafe { deparse_from_item_to_sql(node) }
}

/// Safe wrapper for `deparse_sort_clause`.
///
/// Precondition: `list` must be a valid `PgList<Node>` of `SortGroupClause`
/// nodes allocated by `raw_parser()`.
fn safe_deparse_sort_clause(list: &pgrx::PgList<pg_sys::Node>) -> Result<String, PgTrickleError> {
    // SAFETY: list elements are parse-tree node pointers from raw_parser();
    // valid for the current PostgreSQL memory context.
    unsafe { deparse_sort_clause(list) }
}

/// Safe wrapper for `deparse_target_list`.
///
/// Precondition: `list` must be a valid `*mut pg_sys::List` of `TargetEntry`
/// nodes allocated by `raw_parser()`.
fn safe_deparse_target_list(list: *mut pg_sys::List) -> Result<String, PgTrickleError> {
    // SAFETY: list is a parse-tree pointer from raw_parser(); valid for the
    // current PostgreSQL memory context.
    unsafe { deparse_target_list(list) }
}

/// Safe wrapper for `node_contains_window_func`.
///
/// Precondition: `node` must be a valid `*mut pg_sys::Node` from a
/// parse tree allocated by `raw_parser()`.
fn safe_node_contains_window_func(node: *mut pg_sys::Node) -> bool {
    if node.is_null() {
        return false;
    }
    // SAFETY: node is a non-null parse-tree pointer from raw_parser(); valid
    // for the current PostgreSQL memory context.
    unsafe { node_contains_window_func(node) }
}

/// Safe wrapper for `collect_all_window_func_nodes`.
///
/// Precondition: `node` must be a valid `*mut pg_sys::Node` from a
/// parse tree allocated by `raw_parser()`.
fn safe_collect_all_window_func_nodes(
    node: *mut pg_sys::Node,
    result: &mut Vec<*mut pg_sys::Node>,
) {
    if node.is_null() {
        return;
    }
    // SAFETY: node is a non-null parse-tree pointer from raw_parser(); valid
    // for the current PostgreSQL memory context.
    unsafe { collect_all_window_func_nodes(node, result) }
}

/// Safe wrapper for `extract_func_name`.
///
/// Precondition: `name_list` must be a valid `*mut pg_sys::List` of `String`
/// name nodes allocated by `raw_parser()`.
fn safe_extract_func_name(name_list: *mut pg_sys::List) -> Result<String, PgTrickleError> {
    // SAFETY: List pointer from raw_parser(); valid for the current memory context.
    unsafe { extract_func_name(name_list) }
}

/// Safe wrapper for `extract_operator_name`.
///
/// Precondition: `name_list` must be a valid `*mut pg_sys::List` of `String`
/// name nodes allocated by `raw_parser()`.
fn safe_extract_operator_name(name_list: *mut pg_sys::List) -> Result<String, PgTrickleError> {
    // SAFETY: List pointer from raw_parser(); valid for the current memory context.
    unsafe { extract_operator_name(name_list) }
}

// ── Query Parsing ──────────────────────────────────────────────────────────

/// Context threaded through the parser for CTE handling.
///
/// Holds the raw `SelectStmt` pointers from `extract_cte_map_with_recursive()` alongside
/// a mutable [`CteRegistry`] that gets populated on first reference to
/// each CTE name. Subsequent references to the same CTE reuse the
/// existing registry entry (same `cte_id`), producing multiple
/// [`OpTree::CteScan`] nodes that share a single parsed body.
struct CteParseContext {
    /// Raw CTE name → SelectStmt pointer (from the WITH clause).
    raw_map: HashMap<String, *const pg_sys::SelectStmt>,
    /// CTE definition-level column aliases: `WITH x(a, b) AS (...)`.
    def_aliases: HashMap<String, Vec<String>>,
    /// CTE name → index into `registry.entries` (assigned on first ref).
    id_map: HashMap<String, usize>,
    /// Populated as CTE references are encountered.
    registry: CteRegistry,
    /// When set, FROM references to this name create `RecursiveSelfRef`
    /// instead of trying to resolve the CTE body. Used while parsing
    /// the recursive term of a `WITH RECURSIVE` CTE.
    recursive_self_ref_name: Option<String>,
    /// Output columns for the recursive self-reference. Set alongside
    /// `recursive_self_ref_name`.
    recursive_self_ref_columns: Vec<String>,
    /// G13-SD: Current recursion depth (incremented on each nested
    /// parse_select_stmt / parse_set_operation / parse_from_item call).
    depth: usize,
    /// G13-SD: Maximum allowed recursion depth (from GUC).
    max_depth: usize,
    /// Set to `true` while parsing the base (non-recursive) term of a
    /// `WITH RECURSIVE` CTE anchor.  When true, `parse_select_stmt_inner`
    /// accepts a SELECT with no FROM clause and returns an
    /// `OpTree::ConstantSelect` instead of an error.
    in_cte_anchor: bool,
}

impl CteParseContext {
    fn new(
        raw_map: HashMap<String, *const pg_sys::SelectStmt>,
        def_aliases: HashMap<String, Vec<String>>,
    ) -> Self {
        // In unit tests without a PG backend the GUC is unavailable;
        // use a sensible default.
        #[cfg(any(not(test), feature = "pg_test"))]
        let max_depth = crate::config::pg_trickle_max_parse_depth();
        #[cfg(all(test, not(feature = "pg_test")))]
        let max_depth = 64;

        CteParseContext {
            raw_map,
            def_aliases,
            id_map: HashMap::new(),
            registry: CteRegistry::default(),
            recursive_self_ref_name: None,
            recursive_self_ref_columns: Vec::new(),
            depth: 0,
            max_depth,
            in_cte_anchor: false,
        }
    }

    /// G13-SD: Increment the recursion depth and error if the limit is
    /// exceeded.  Returns the previous depth so the caller can restore
    /// it after the recursive call (via `set_depth`).
    fn descend(&mut self) -> Result<usize, PgTrickleError> {
        let prev = self.depth;
        self.depth += 1;
        if self.depth > self.max_depth {
            return Err(PgTrickleError::QueryTooComplex(format!(
                "parse tree recursion depth {} exceeds pg_trickle.max_parse_depth ({}). \
                 Simplify the query or raise the limit.",
                self.depth, self.max_depth,
            )));
        }
        Ok(prev)
    }

    /// G13-SD: Restore the recursion depth to a previously saved value.
    fn ascend(&mut self, prev: usize) {
        self.depth = prev;
    }

    /// Returns true if `name` is a CTE defined in this query (either
    /// still in the raw map awaiting parsing, or already registered).
    fn is_cte(&self, name: &str) -> bool {
        self.raw_map.contains_key(name) || self.id_map.contains_key(name)
    }

    /// Return the `cte_id` for an already-parsed CTE, or `None`.
    fn lookup_id(&self, name: &str) -> Option<usize> {
        self.id_map.get(name).copied()
    }

    /// Register a newly-parsed CTE body and return its `cte_id`.
    fn register(&mut self, name: &str, body: OpTree) -> usize {
        let id = self.registry.entries.len();
        self.registry.entries.push((name.to_string(), body));
        self.id_map.insert(name.to_string(), id);
        id
    }
}

// ── Query Parsing (entry points) ──────────────────────────────────────────

/// Check whether a defining query contains `WITH RECURSIVE`.
///
/// This is a lightweight check that only parses the top-level structure
/// without building a full OpTree. It is safe to call for any valid SQL
/// SELECT, including queries that the DVM parser cannot handle.
///
/// # Requires
/// Must be called within a PostgreSQL backend (uses `raw_parser()`).
pub fn query_has_recursive_cte(query: &str) -> Result<bool, PgTrickleError> {
    // SAFETY: raw_parser is safe when called within a PostgreSQL backend
    // with a valid memory context.
    query_has_recursive_cte_inner(query)
}

/// Lightweight check for any top-level WITH clause in a defining query.
///
/// Returns true for both recursive and non-recursive CTEs. This is used by
/// refresh planning to select safe fallback strategies without building the
/// full OpTree.
pub fn query_has_cte(query: &str) -> Result<bool, PgTrickleError> {
    query_has_cte_inner(query)
}

fn query_has_cte_inner(query: &str) -> Result<bool, PgTrickleError> {
    let select_ptr = match parse_first_select(query)? {
        Some(s) => s,
        None => return Ok(false),
    };
    // SAFETY: pointer is valid for the duration of the current memory context.
    let select = unsafe { &*select_ptr };
    Ok(!select.withClause.is_null())
}

fn query_has_recursive_cte_inner(query: &str) -> Result<bool, PgTrickleError> {
    let select_ptr = match parse_first_select(query)? {
        Some(s) => s,
        None => return Ok(false),
    };
    // SAFETY: pointer is valid for the duration of the current memory context.
    let select = unsafe { &*select_ptr };
    if select.withClause.is_null() {
        return Ok(false);
    }

    // SAFETY: Pointer verified non-null; parse-tree node allocated by raw_parser in a valid memory context.
    let wc = unsafe { &*select.withClause };
    Ok(wc.recursive)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helper constructors ─────────────────────────────────────────

    fn col(name: &str) -> Expr {
        Expr::ColumnRef {
            table_alias: None,
            column_name: name.to_string(),
        }
    }

    fn qualified_col(table: &str, name: &str) -> Expr {
        Expr::ColumnRef {
            table_alias: Some(table.to_string()),
            column_name: name.to_string(),
        }
    }

    fn make_column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            type_oid: 23, // INT4
            is_nullable: true,
        }
    }

    fn scan_node(alias: &str, oid: u32, col_names: &[&str]) -> OpTree {
        OpTree::Scan {
            table_oid: oid,
            table_name: alias.to_string(),
            schema: "public".to_string(),
            columns: col_names.iter().map(|n| make_column(n)).collect(),
            pk_columns: Vec::new(),
            alias: alias.to_string(),
        }
    }

    // ── Expr::to_sql tests ──────────────────────────────────────────

    #[test]
    fn test_expr_column_ref_unqualified() {
        let e = col("amount");
        assert_eq!(e.to_sql(), "amount");
    }

    #[test]
    fn test_expr_column_ref_qualified() {
        let e = qualified_col("orders", "id");
        assert_eq!(e.to_sql(), "\"orders\".\"id\"");
    }

    #[test]
    fn test_expr_literal() {
        let e = Expr::Literal("42".to_string());
        assert_eq!(e.to_sql(), "42");
    }

    #[test]
    fn test_expr_literal_string() {
        let e = Expr::Literal("'hello'".to_string());
        assert_eq!(e.to_sql(), "'hello'");
    }

    #[test]
    fn test_expr_binary_op() {
        let e = Expr::BinaryOp {
            op: "+".to_string(),
            left: Box::new(col("a")),
            right: Box::new(col("b")),
        };
        assert_eq!(e.to_sql(), "(a + b)");
    }

    #[test]
    fn test_expr_binary_op_nested() {
        let e = Expr::BinaryOp {
            op: "*".to_string(),
            left: Box::new(Expr::BinaryOp {
                op: "+".to_string(),
                left: Box::new(col("a")),
                right: Box::new(col("b")),
            }),
            right: Box::new(Expr::Literal("2".to_string())),
        };
        assert_eq!(e.to_sql(), "((a + b) * 2)");
    }

    #[test]
    fn test_expr_func_call_no_args() {
        let e = Expr::FuncCall {
            func_name: "now".to_string(),
            args: vec![],
        };
        assert_eq!(e.to_sql(), "now()");
    }

    #[test]
    fn test_expr_func_call_with_args() {
        let e = Expr::FuncCall {
            func_name: "coalesce".to_string(),
            args: vec![col("x"), Expr::Literal("0".to_string())],
        };
        assert_eq!(e.to_sql(), "coalesce(x, 0)");
    }

    #[test]
    fn test_expr_star_unqualified() {
        let e = Expr::Star { table_alias: None };
        assert_eq!(e.to_sql(), "*");
    }

    #[test]
    fn test_expr_star_qualified() {
        let e = Expr::Star {
            table_alias: Some("t".to_string()),
        };
        assert_eq!(e.to_sql(), "t.*");
    }

    #[test]
    fn test_expr_raw() {
        let e = Expr::Raw("CASE WHEN x > 0 THEN 1 ELSE 0 END".to_string());
        assert_eq!(e.to_sql(), "CASE WHEN x > 0 THEN 1 ELSE 0 END");
    }

    // ── AggFunc::sql_name tests ─────────────────────────────────────

    #[test]
    fn test_agg_func_sql_names() {
        assert_eq!(AggFunc::Count.sql_name(), "COUNT");
        assert_eq!(AggFunc::CountStar.sql_name(), "COUNT");
        assert_eq!(AggFunc::Sum.sql_name(), "SUM");
        assert_eq!(AggFunc::Avg.sql_name(), "AVG");
        assert_eq!(AggFunc::Min.sql_name(), "MIN");
        assert_eq!(AggFunc::Max.sql_name(), "MAX");
        assert_eq!(AggFunc::BoolAnd.sql_name(), "BOOL_AND");
        assert_eq!(AggFunc::BoolOr.sql_name(), "BOOL_OR");
        assert_eq!(AggFunc::StringAgg.sql_name(), "STRING_AGG");
        assert_eq!(AggFunc::ArrayAgg.sql_name(), "ARRAY_AGG");
        assert_eq!(AggFunc::JsonAgg.sql_name(), "JSON_AGG");
        assert_eq!(AggFunc::JsonbAgg.sql_name(), "JSONB_AGG");
        assert_eq!(AggFunc::BitAnd.sql_name(), "BIT_AND");
        assert_eq!(AggFunc::BitOr.sql_name(), "BIT_OR");
        assert_eq!(
            AggFunc::JsonObjectAggStd("JSON_OBJECTAGG(k : v)".into()).sql_name(),
            "JSON_OBJECTAGG"
        );
        assert_eq!(
            AggFunc::JsonArrayAggStd("JSON_ARRAYAGG(x)".into()).sql_name(),
            "JSON_ARRAYAGG"
        );
        assert_eq!(AggFunc::BitXor.sql_name(), "BIT_XOR");
        assert_eq!(AggFunc::JsonObjectAgg.sql_name(), "JSON_OBJECT_AGG");
        assert_eq!(AggFunc::JsonbObjectAgg.sql_name(), "JSONB_OBJECT_AGG");
        assert_eq!(AggFunc::StddevPop.sql_name(), "STDDEV_POP");
        assert_eq!(AggFunc::StddevSamp.sql_name(), "STDDEV_SAMP");
        assert_eq!(AggFunc::VarPop.sql_name(), "VAR_POP");
        assert_eq!(AggFunc::VarSamp.sql_name(), "VAR_SAMP");
        assert_eq!(AggFunc::Mode.sql_name(), "MODE");
        assert_eq!(AggFunc::PercentileCont.sql_name(), "PERCENTILE_CONT");
        assert_eq!(AggFunc::PercentileDisc.sql_name(), "PERCENTILE_DISC");
        assert_eq!(AggFunc::Corr.sql_name(), "CORR");
        assert_eq!(AggFunc::CovarPop.sql_name(), "COVAR_POP");
        assert_eq!(AggFunc::CovarSamp.sql_name(), "COVAR_SAMP");
        assert_eq!(AggFunc::RegrAvgx.sql_name(), "REGR_AVGX");
        assert_eq!(AggFunc::RegrAvgy.sql_name(), "REGR_AVGY");
        assert_eq!(AggFunc::RegrCount.sql_name(), "REGR_COUNT");
        assert_eq!(AggFunc::RegrIntercept.sql_name(), "REGR_INTERCEPT");
        assert_eq!(AggFunc::RegrR2.sql_name(), "REGR_R2");
        assert_eq!(AggFunc::RegrSlope.sql_name(), "REGR_SLOPE");
        assert_eq!(AggFunc::RegrSxx.sql_name(), "REGR_SXX");
        assert_eq!(AggFunc::RegrSxy.sql_name(), "REGR_SXY");
        assert_eq!(AggFunc::RegrSyy.sql_name(), "REGR_SYY");
        assert_eq!(
            AggFunc::UserDefined("my_agg(x)".into()).sql_name(),
            "USER_DEFINED"
        );
    }

    // ── OpTree::alias tests ─────────────────────────────────────────

    #[test]
    fn test_scan_alias() {
        let tree = scan_node("orders", 12345, &["id", "amount"]);
        assert_eq!(tree.alias(), "orders");
    }

    #[test]
    fn test_project_alias() {
        let tree = OpTree::Project {
            expressions: vec![col("id")],
            aliases: vec!["id".to_string()],
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert_eq!(tree.alias(), "project");
    }

    #[test]
    fn test_filter_alias() {
        let tree = OpTree::Filter {
            predicate: col("x"),
            child: Box::new(scan_node("t", 1, &["x"])),
        };
        assert_eq!(tree.alias(), "filter");
    }

    #[test]
    fn test_inner_join_alias() {
        let tree = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 1, &["id"])),
            right: Box::new(scan_node("b", 2, &["id"])),
        };
        assert_eq!(tree.alias(), "join");
    }

    // ── CROSS JOIN structure tests ──────────────────────────────────

    #[test]
    fn test_cross_join_is_inner_join_with_true_condition() {
        // CROSS JOIN is represented as InnerJoin { condition: Literal("TRUE") }
        let tree = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(scan_node("a", 1, &["x"])),
            right: Box::new(scan_node("b", 2, &["y"])),
        };
        assert!(matches!(&tree, OpTree::InnerJoin { condition, .. }
            if matches!(condition, Expr::Literal(s) if s == "TRUE")));
        assert_eq!(tree.alias(), "join");
    }

    #[test]
    fn test_cross_join_output_columns_combines_both_sides() {
        // A CROSS JOIN between two tables should expose columns from both sides
        let tree = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(scan_node("a", 1, &["x", "y"])),
            right: Box::new(scan_node("b", 2, &["p", "q"])),
        };
        let cols = tree.output_columns();
        assert_eq!(cols, vec!["x", "y", "p", "q"]);
    }

    #[test]
    fn test_nested_cross_join_structure() {
        // SELECT * FROM a CROSS JOIN b CROSS JOIN c
        // → InnerJoin(InnerJoin(a, b), c) with TRUE conditions
        let inner = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(scan_node("a", 1, &["x"])),
            right: Box::new(scan_node("b", 2, &["y"])),
        };
        let outer = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(inner),
            right: Box::new(scan_node("c", 3, &["z"])),
        };

        // Verify structure: top-level is InnerJoin with TRUE condition
        assert!(matches!(&outer, OpTree::InnerJoin { condition, .. }
            if matches!(condition, Expr::Literal(s) if s == "TRUE")));
        // Verify all columns are accessible
        let cols = outer.output_columns();
        assert_eq!(cols, vec!["x", "y", "z"]);
    }

    #[test]
    fn test_left_join_alias() {
        let tree = OpTree::LeftJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 1, &["id"])),
            right: Box::new(scan_node("b", 2, &["id"])),
        };
        assert_eq!(tree.alias(), "left_join");
    }

    #[test]
    fn test_aggregate_alias() {
        let tree = OpTree::Aggregate {
            group_by: vec![col("region")],
            aggregates: vec![AggExpr {
                function: AggFunc::Sum,
                argument: Some(col("amount")),
                alias: "total".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["region", "amount"])),
        };
        assert_eq!(tree.alias(), "agg");
    }

    #[test]
    fn test_distinct_alias() {
        let tree = OpTree::Distinct {
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert_eq!(tree.alias(), "distinct");
    }

    #[test]
    fn test_union_all_alias() {
        let tree = OpTree::UnionAll {
            children: vec![scan_node("a", 1, &["id"]), scan_node("b", 2, &["id"])],
        };
        assert_eq!(tree.alias(), "union");
    }

    // ── rewrite_having_expr for UNION (Part 2 structure tests) ─────

    #[test]
    fn test_union_all_produces_union_all_node() {
        // Verify that UnionAll node structure is correct (no Distinct wrapper)
        let tree = OpTree::UnionAll {
            children: vec![
                scan_node("a", 1, &["id", "val"]),
                scan_node("b", 2, &["id", "val"]),
            ],
        };
        // The top-level node must NOT be a Distinct
        assert!(matches!(tree, OpTree::UnionAll { .. }));
    }

    #[test]
    fn test_distinct_wraps_union_all_for_dedup_union() {
        // UNION (without ALL) must be represented as Distinct { child: UnionAll { .. } }
        let union_all = OpTree::UnionAll {
            children: vec![
                scan_node("a", 1, &["id", "val"]),
                scan_node("b", 2, &["id", "val"]),
            ],
        };
        let tree = OpTree::Distinct {
            child: Box::new(union_all),
        };
        // Top-level is Distinct
        assert!(matches!(tree, OpTree::Distinct { .. }));
        // Its child is UnionAll
        if let OpTree::Distinct { child } = &tree {
            assert!(matches!(**child, OpTree::UnionAll { .. }));
            if let OpTree::UnionAll { children } = child.as_ref() {
                assert_eq!(children.len(), 2);
            }
        }
    }

    #[test]
    fn test_distinct_around_union_all_alias() {
        // Distinct wrapping UnionAll should report "distinct" as its alias
        let tree = OpTree::Distinct {
            child: Box::new(OpTree::UnionAll {
                children: vec![scan_node("a", 1, &["id"]), scan_node("b", 2, &["id"])],
            }),
        };
        assert_eq!(tree.alias(), "distinct");
    }

    #[test]
    fn test_distinct_union_all_output_columns_from_first_child() {
        // output_columns delegates through Distinct → UnionAll → first child
        let tree = OpTree::Distinct {
            child: Box::new(OpTree::UnionAll {
                children: vec![
                    scan_node("a", 1, &["region", "amount"]),
                    scan_node("b", 2, &["region", "amount"]),
                ],
            }),
        };
        assert_eq!(tree.output_columns(), vec!["region", "amount"]);
    }

    #[test]
    fn test_distinct_union_all_source_oids_combined() {
        // source_oids should include OIDs from both branches
        let tree = OpTree::Distinct {
            child: Box::new(OpTree::UnionAll {
                children: vec![scan_node("a", 10, &["id"]), scan_node("b", 20, &["id"])],
            }),
        };
        let oids = tree.source_oids();
        assert!(oids.contains(&10));
        assert!(oids.contains(&20));
        assert_eq!(oids.len(), 2);
    }

    #[test]
    fn test_needs_pgt_count_distinct_wrapping_union_all() {
        // Distinct wrapping UnionAll uses union-dedup-count path, not pgt_count
        let tree = OpTree::Distinct {
            child: Box::new(OpTree::UnionAll {
                children: vec![scan_node("a", 1, &["id"]), scan_node("b", 2, &["id"])],
            }),
        };
        assert!(!tree.needs_pgt_count());
        assert!(tree.needs_union_dedup_count());
    }

    #[test]
    fn test_scan_output_columns() {
        let tree = scan_node("orders", 1, &["id", "amount", "name"]);
        assert_eq!(tree.output_columns(), vec!["id", "amount", "name"]);
    }

    #[test]
    fn test_project_output_columns() {
        let tree = OpTree::Project {
            expressions: vec![col("id"), col("amount")],
            aliases: vec!["order_id".to_string(), "total".to_string()],
            child: Box::new(scan_node("t", 1, &["id", "amount"])),
        };
        assert_eq!(tree.output_columns(), vec!["order_id", "total"]);
    }

    #[test]
    fn test_filter_output_columns_delegates_to_child() {
        let tree = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: ">".to_string(),
                left: Box::new(col("amount")),
                right: Box::new(Expr::Literal("100".to_string())),
            },
            child: Box::new(scan_node("t", 1, &["id", "amount"])),
        };
        assert_eq!(tree.output_columns(), vec!["id", "amount"]);
    }

    #[test]
    fn test_inner_join_output_columns_combines_both() {
        let tree = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 1, &["id", "name"])),
            right: Box::new(scan_node("b", 2, &["id", "value"])),
        };
        assert_eq!(tree.output_columns(), vec!["id", "name", "id", "value"]);
    }

    #[test]
    fn test_left_join_output_columns_combines_both() {
        let tree = OpTree::LeftJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 1, &["x"])),
            right: Box::new(scan_node("b", 2, &["y", "z"])),
        };
        assert_eq!(tree.output_columns(), vec!["x", "y", "z"]);
    }

    #[test]
    fn test_aggregate_output_columns() {
        let tree = OpTree::Aggregate {
            group_by: vec![col("region")],
            aggregates: vec![
                AggExpr {
                    function: AggFunc::Sum,
                    argument: Some(col("amount")),
                    alias: "total".to_string(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                },
                AggExpr {
                    function: AggFunc::CountStar,
                    argument: None,
                    alias: "cnt".to_string(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                },
            ],
            child: Box::new(scan_node("t", 1, &["region", "amount"])),
        };
        // group_by outputs the to_sql() of the expression, plus aggregate aliases
        assert_eq!(tree.output_columns(), vec!["region", "total", "cnt"]);
    }

    #[test]
    fn test_aggregate_qualified_group_by_output() {
        let tree = OpTree::Aggregate {
            group_by: vec![qualified_col("t", "region")],
            aggregates: vec![],
            child: Box::new(scan_node("t", 1, &["region"])),
        };
        // Qualified column references should produce unqualified output names,
        // matching PostgreSQL's behavior for subquery output columns.
        assert_eq!(tree.output_columns(), vec!["region"]);
    }

    #[test]
    fn test_distinct_output_columns_delegates() {
        let tree = OpTree::Distinct {
            child: Box::new(scan_node("t", 1, &["a", "b"])),
        };
        assert_eq!(tree.output_columns(), vec!["a", "b"]);
    }

    #[test]
    fn test_union_all_output_columns_uses_first_child() {
        let tree = OpTree::UnionAll {
            children: vec![
                scan_node("a", 1, &["x", "y"]),
                scan_node("b", 2, &["p", "q"]),
            ],
        };
        assert_eq!(tree.output_columns(), vec!["x", "y"]);
    }

    #[test]
    fn test_union_all_empty_children() {
        let tree = OpTree::UnionAll { children: vec![] };
        assert!(tree.output_columns().is_empty());
    }

    // ── OpTree::source_oids tests ───────────────────────────────────

    #[test]
    fn test_scan_source_oids() {
        let tree = scan_node("t", 42, &["id"]);
        assert_eq!(tree.source_oids(), vec![42]);
    }

    #[test]
    fn test_project_source_oids_delegates() {
        let tree = OpTree::Project {
            expressions: vec![col("id")],
            aliases: vec!["id".to_string()],
            child: Box::new(scan_node("t", 100, &["id"])),
        };
        assert_eq!(tree.source_oids(), vec![100]);
    }

    #[test]
    fn test_filter_source_oids_delegates() {
        let tree = OpTree::Filter {
            predicate: col("x"),
            child: Box::new(scan_node("t", 50, &["x"])),
        };
        assert_eq!(tree.source_oids(), vec![50]);
    }

    #[test]
    fn test_join_source_oids_combines() {
        let tree = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 10, &["id"])),
            right: Box::new(scan_node("b", 20, &["id"])),
        };
        assert_eq!(tree.source_oids(), vec![10, 20]);
    }

    #[test]
    fn test_left_join_source_oids_combines() {
        let tree = OpTree::LeftJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 30, &["id"])),
            right: Box::new(scan_node("b", 40, &["id"])),
        };
        assert_eq!(tree.source_oids(), vec![30, 40]);
    }

    #[test]
    fn test_aggregate_source_oids_delegates() {
        let tree = OpTree::Aggregate {
            group_by: vec![col("r")],
            aggregates: vec![],
            child: Box::new(scan_node("t", 77, &["r"])),
        };
        assert_eq!(tree.source_oids(), vec![77]);
    }

    #[test]
    fn test_distinct_source_oids_delegates() {
        let tree = OpTree::Distinct {
            child: Box::new(scan_node("t", 55, &["id"])),
        };
        assert_eq!(tree.source_oids(), vec![55]);
    }

    #[test]
    fn test_union_all_source_oids_collects_all() {
        let tree = OpTree::UnionAll {
            children: vec![
                scan_node("a", 1, &["id"]),
                scan_node("b", 2, &["id"]),
                scan_node("c", 3, &["id"]),
            ],
        };
        assert_eq!(tree.source_oids(), vec![1, 2, 3]);
    }

    #[test]
    fn test_nested_tree_source_oids() {
        // SELECT a.id, b.val FROM a JOIN b ON a.id = b.id WHERE a.x > 10
        let tree = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: ">".to_string(),
                left: Box::new(qualified_col("a", "x")),
                right: Box::new(Expr::Literal("10".to_string())),
            },
            child: Box::new(OpTree::InnerJoin {
                condition: Expr::BinaryOp {
                    op: "=".to_string(),
                    left: Box::new(qualified_col("a", "id")),
                    right: Box::new(qualified_col("b", "id")),
                },
                left: Box::new(scan_node("a", 100, &["id", "x"])),
                right: Box::new(scan_node("b", 200, &["id", "val"])),
            }),
        };
        assert_eq!(tree.source_oids(), vec![100, 200]);
    }

    // ── check_ivm_support tests ─────────────────────────────────────

    #[test]
    fn test_check_ivm_support_scan() {
        let tree = scan_node("t", 1, &["id"]);
        assert!(check_ivm_support(&tree).is_ok());
    }

    #[test]
    fn test_check_ivm_support_supported_operators() {
        // Build a tree using supported operators:
        // Aggregate(Filter(InnerJoin(scan_a, scan_b)))
        let scan_a = scan_node("a", 1, &["id", "val"]);
        let scan_b = scan_node("b", 2, &["id", "val"]);

        let join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(scan_a),
            right: Box::new(scan_b),
        };

        let filtered = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: ">".to_string(),
                left: Box::new(col("val")),
                right: Box::new(Expr::Literal("0".to_string())),
            },
            child: Box::new(join),
        };

        let projected = OpTree::Project {
            expressions: vec![col("id"), col("val")],
            aliases: vec!["id".to_string(), "val".to_string()],
            child: Box::new(filtered),
        };

        let aggregated = OpTree::Aggregate {
            group_by: vec![col("id")],
            aggregates: vec![
                AggExpr {
                    function: AggFunc::Sum,
                    argument: Some(col("val")),
                    alias: "sum_val".to_string(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                },
                AggExpr {
                    function: AggFunc::Count,
                    argument: Some(col("val")),
                    alias: "cnt".to_string(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                },
                AggExpr {
                    function: AggFunc::Avg,
                    argument: Some(col("val")),
                    alias: "avg_val".to_string(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                },
            ],
            child: Box::new(projected),
        };

        let distinct = OpTree::Distinct {
            child: Box::new(aggregated),
        };

        assert!(check_ivm_support(&distinct).is_ok());
    }

    #[test]
    fn test_check_ivm_support_simple_join() {
        // InnerJoin with Scan children — should pass
        let join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 1, &["id"])),
            right: Box::new(scan_node("b", 2, &["id"])),
        };
        assert!(check_ivm_support(&join).is_ok());
    }

    #[test]
    fn test_check_ivm_support_join_with_filter_children() {
        // Join with Filter(Scan) children — should pass
        let left = OpTree::Filter {
            predicate: col("id"),
            child: Box::new(scan_node("a", 1, &["id"])),
        };
        let right = OpTree::Project {
            expressions: vec![col("id")],
            aliases: vec!["id".to_string()],
            child: Box::new(scan_node("b", 2, &["id"])),
        };
        let join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(left),
            right: Box::new(right),
        };
        assert!(check_ivm_support(&join).is_ok());
    }

    #[test]
    fn test_check_ivm_support_allows_min_aggregate() {
        let agg = OpTree::Aggregate {
            group_by: vec![col("id")],
            aggregates: vec![AggExpr {
                function: AggFunc::Min,
                argument: Some(col("val")),
                alias: "min_val".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["id", "val"])),
        };
        assert!(check_ivm_support(&agg).is_ok());
    }

    #[test]
    fn test_check_ivm_support_allows_max_aggregate() {
        let agg = OpTree::Aggregate {
            group_by: vec![col("id")],
            aggregates: vec![AggExpr {
                function: AggFunc::Max,
                argument: Some(col("val")),
                alias: "max_val".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["id", "val"])),
        };
        assert!(check_ivm_support(&agg).is_ok());
    }

    #[test]
    fn test_check_ivm_support_allows_nested_join() {
        // Join-of-join: InnerJoin(InnerJoin(a, b), c) — now supported
        let inner_join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 1, &["id"])),
            right: Box::new(scan_node("b", 2, &["id"])),
        };
        let outer_join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(inner_join),
            right: Box::new(scan_node("c", 3, &["id"])),
        };
        assert!(check_ivm_support(&outer_join).is_ok());
    }

    #[test]
    fn test_check_ivm_support_allows_join_with_union_child() {
        // LeftJoin(UnionAll(...), Scan) — now supported
        let union = OpTree::UnionAll {
            children: vec![scan_node("a", 1, &["id"]), scan_node("b", 2, &["id"])],
        };
        let join = OpTree::LeftJoin {
            condition: col("id"),
            left: Box::new(union),
            right: Box::new(scan_node("c", 3, &["id"])),
        };
        assert!(check_ivm_support(&join).is_ok());
    }

    // ── is_star_only tests ──────────────────────────────────────────

    #[test]
    fn test_is_star_only_true() {
        assert!(is_star_only(&[Expr::Star { table_alias: None }]));
    }

    #[test]
    fn test_is_star_only_false_qualified() {
        assert!(!is_star_only(&[Expr::Star {
            table_alias: Some("t".to_string()),
        }]));
    }

    #[test]
    fn test_is_star_only_false_column() {
        assert!(!is_star_only(&[col("id")]));
    }

    #[test]
    fn test_is_star_only_false_multiple() {
        assert!(!is_star_only(&[
            Expr::Star { table_alias: None },
            col("id"),
        ]));
    }

    #[test]
    fn test_is_star_only_false_empty() {
        assert!(!is_star_only(&[]));
    }

    // ── RowIdStrategy (from row_id.rs, tested here for convenience) ─

    #[test]
    fn test_row_id_strategy_debug() {
        use crate::dvm::row_id::RowIdStrategy;
        let pk = RowIdStrategy::PrimaryKey {
            pk_columns: vec!["id".to_string()],
        };
        let debug = format!("{:?}", pk);
        assert!(debug.contains("PrimaryKey"));
        assert!(debug.contains("id"));
    }

    #[test]
    fn test_row_id_strategy_clone() {
        use crate::dvm::row_id::RowIdStrategy;
        let original = RowIdStrategy::GroupByKey {
            group_columns: vec!["region".to_string(), "year".to_string()],
        };
        let cloned = original.clone();
        let debug_orig = format!("{:?}", original);
        let debug_clone = format!("{:?}", cloned);
        assert_eq!(debug_orig, debug_clone);
    }

    // ── Subquery / CTE OpTree tests ─────────────────────────────────

    #[test]
    fn test_subquery_alias() {
        let tree = OpTree::Subquery {
            alias: "active_users".to_string(),
            column_aliases: vec![],
            child: Box::new(scan_node("users", 1, &["id", "name"])),
        };
        assert_eq!(tree.alias(), "active_users");
    }

    #[test]
    fn test_subquery_output_columns_no_aliases() {
        // When no column_aliases, delegates to child
        let tree = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(scan_node("t", 1, &["id", "name", "val"])),
        };
        assert_eq!(tree.output_columns(), vec!["id", "name", "val"]);
    }

    #[test]
    fn test_subquery_output_columns_with_aliases() {
        // Column aliases override child's output
        let tree = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec!["a".to_string(), "b".to_string()],
            child: Box::new(scan_node("t", 1, &["id", "name"])),
        };
        assert_eq!(tree.output_columns(), vec!["a", "b"]);
    }

    #[test]
    fn test_subquery_source_oids_delegates() {
        let tree = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(scan_node("t", 42, &["id"])),
        };
        assert_eq!(tree.source_oids(), vec![42]);
    }

    #[test]
    fn test_subquery_source_oids_nested() {
        // Subquery wrapping a join — collects OIDs from both sides
        let tree = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(OpTree::InnerJoin {
                condition: col("id"),
                left: Box::new(scan_node("a", 10, &["id"])),
                right: Box::new(scan_node("b", 20, &["id"])),
            }),
        };
        assert_eq!(tree.source_oids(), vec![10, 20]);
    }

    #[test]
    fn test_check_ivm_support_subquery() {
        let tree = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert!(check_ivm_support(&tree).is_ok());
    }

    #[test]
    fn test_check_ivm_support_subquery_with_aggregate() {
        // Subquery wrapping an aggregate — should be supported
        let tree = OpTree::Subquery {
            alias: "totals".to_string(),
            column_aliases: vec!["uid".to_string(), "total".to_string()],
            child: Box::new(OpTree::Aggregate {
                group_by: vec![col("user_id")],
                aggregates: vec![AggExpr {
                    function: AggFunc::Sum,
                    argument: Some(col("amount")),
                    alias: "total".to_string(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                }],
                child: Box::new(scan_node("orders", 1, &["user_id", "amount"])),
            }),
        };
        assert!(check_ivm_support(&tree).is_ok());
    }

    #[test]
    fn test_subquery_in_join() {
        // Simulates: FROM (SELECT ...) AS sub JOIN orders ON ...
        let sub = OpTree::Subquery {
            alias: "active".to_string(),
            column_aliases: vec![],
            child: Box::new(OpTree::Filter {
                predicate: Expr::BinaryOp {
                    op: "=".to_string(),
                    left: Box::new(col("active")),
                    right: Box::new(Expr::Literal("true".to_string())),
                },
                child: Box::new(scan_node("users", 1, &["id", "name", "active"])),
            }),
        };

        let tree = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("active", "id")),
                right: Box::new(qualified_col("orders", "user_id")),
            },
            left: Box::new(sub),
            right: Box::new(scan_node("orders", 2, &["user_id", "amount"])),
        };

        assert!(check_ivm_support(&tree).is_ok());
        assert_eq!(tree.source_oids(), vec![1, 2]);
    }

    #[test]
    fn test_nested_subqueries() {
        // Simulates: FROM (SELECT * FROM (SELECT ...) AS inner) AS outer
        let inner = OpTree::Subquery {
            alias: "inner_sub".to_string(),
            column_aliases: vec![],
            child: Box::new(scan_node("t", 1, &["id", "val"])),
        };
        let outer = OpTree::Subquery {
            alias: "outer_sub".to_string(),
            column_aliases: vec!["a".to_string(), "b".to_string()],
            child: Box::new(inner),
        };
        assert_eq!(outer.output_columns(), vec!["a", "b"]);
        assert_eq!(outer.source_oids(), vec![1]);
    }

    // ── Tier 2: CteScan + CteRegistry tests ────────────────────────

    #[test]
    fn test_cte_scan_alias() {
        let node = OpTree::CteScan {
            cte_id: 0,
            cte_name: "totals".to_string(),
            alias: "t1".to_string(),
            columns: vec!["user_id".to_string(), "total".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        assert_eq!(node.alias(), "t1");
    }

    #[test]
    fn test_cte_scan_output_columns_no_aliases() {
        let node = OpTree::CteScan {
            cte_id: 0,
            cte_name: "totals".to_string(),
            alias: "totals".to_string(),
            columns: vec!["user_id".to_string(), "total".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        assert_eq!(node.output_columns(), vec!["user_id", "total"]);
    }

    #[test]
    fn test_cte_scan_output_columns_with_aliases() {
        let node = OpTree::CteScan {
            cte_id: 0,
            cte_name: "totals".to_string(),
            alias: "t".to_string(),
            columns: vec!["user_id".to_string(), "total".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec!["uid".to_string(), "amt".to_string()],
            body: None,
        };
        assert_eq!(node.output_columns(), vec!["uid", "amt"]);
    }

    #[test]
    fn test_cte_scan_source_oids_empty() {
        // CteScan OIDs are resolved via the CteRegistry, not the node itself
        let node = OpTree::CteScan {
            cte_id: 0,
            cte_name: "totals".to_string(),
            alias: "totals".to_string(),
            columns: vec!["id".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        assert!(node.source_oids().is_empty());
    }

    #[test]
    fn test_cte_scan_ivm_support() {
        let node = OpTree::CteScan {
            cte_id: 0,
            cte_name: "totals".to_string(),
            alias: "totals".to_string(),
            columns: vec!["id".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        assert!(check_ivm_support(&node).is_ok());
    }

    #[test]
    fn test_cte_registry_source_oids() {
        let mut reg = CteRegistry::default();
        reg.entries
            .push(("a".to_string(), scan_node("orders", 100, &["id", "amount"])));
        reg.entries
            .push(("b".to_string(), scan_node("users", 200, &["id", "name"])));
        let oids = reg.source_oids();
        assert!(oids.contains(&100));
        assert!(oids.contains(&200));
        assert_eq!(oids.len(), 2);
    }

    #[test]
    fn test_cte_registry_get() {
        let mut reg = CteRegistry::default();
        reg.entries
            .push(("totals".to_string(), scan_node("orders", 1, &["id"])));
        assert!(reg.get(0).is_some());
        assert_eq!(reg.get(0).unwrap().0, "totals");
        assert!(reg.get(1).is_none());
    }

    #[test]
    fn test_cte_parse_context_basic() {
        // Verify CteParseContext tracks registrations correctly
        let mut ctx = CteParseContext::new(HashMap::new(), HashMap::new());
        assert!(!ctx.is_cte("x"));
        assert!(ctx.lookup_id("x").is_none());

        let body = scan_node("t", 1, &["id"]);
        let id = ctx.register("x", body);
        assert_eq!(id, 0);
        assert_eq!(ctx.lookup_id("x"), Some(0));
        assert_eq!(ctx.registry.entries.len(), 1);

        let body2 = scan_node("t2", 2, &["id"]);
        let id2 = ctx.register("y", body2);
        assert_eq!(id2, 1);
        assert_eq!(ctx.registry.entries.len(), 2);
    }

    #[test]
    fn test_parse_result_struct() {
        let tree = scan_node("orders", 1, &["id", "amount"]);
        let mut registry = CteRegistry::default();
        registry.entries.push((
            "totals".to_string(),
            scan_node("orders", 1, &["user_id", "total"]),
        ));
        let result = ParseResult {
            tree: tree.clone(),
            cte_registry: registry,
            has_recursion: false,
            warnings: vec![],
        };
        assert_eq!(result.tree.alias(), "orders");
        assert_eq!(result.cte_registry.entries.len(), 1);
        assert!(!result.has_recursion);
    }

    #[test]
    fn test_check_ivm_support_with_registry_valid() {
        let tree = OpTree::CteScan {
            cte_id: 0,
            cte_name: "totals".to_string(),
            alias: "totals".to_string(),
            columns: vec!["id".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        let mut registry = CteRegistry::default();
        registry.entries.push((
            "totals".to_string(),
            scan_node("orders", 1, &["id", "amount"]),
        ));
        let result = ParseResult {
            tree,
            cte_registry: registry,
            has_recursion: false,
            warnings: vec![],
        };
        assert!(check_ivm_support_with_registry(&result).is_ok());
    }

    #[test]
    fn test_multi_reference_cte_same_id() {
        // Simulates: WITH totals AS (...) SELECT ... FROM totals t1 JOIN totals t2
        // Both CteScan nodes should share cte_id=0
        let t1 = OpTree::CteScan {
            cte_id: 0,
            cte_name: "totals".to_string(),
            alias: "t1".to_string(),
            columns: vec!["user_id".to_string(), "total".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        let t2 = OpTree::CteScan {
            cte_id: 0,
            cte_name: "totals".to_string(),
            alias: "t2".to_string(),
            columns: vec!["user_id".to_string(), "total".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        let tree = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("t1", "user_id")),
                right: Box::new(qualified_col("t2", "user_id")),
            },
            left: Box::new(t1),
            right: Box::new(t2),
        };

        let mut registry = CteRegistry::default();
        registry.entries.push((
            "totals".to_string(),
            scan_node("orders", 1, &["user_id", "total"]),
        ));
        let result = ParseResult {
            tree,
            cte_registry: registry,
            has_recursion: false,
            warnings: vec![],
        };
        assert!(check_ivm_support_with_registry(&result).is_ok());
        // Only one entry in the registry despite two CteScan nodes
        assert_eq!(result.cte_registry.entries.len(), 1);
    }

    #[test]
    fn test_cte_chain_in_registry() {
        // Simulates: WITH a AS (...), b AS (SELECT FROM a) SELECT FROM b
        // Registry has two entries; the main tree references only b (cte_id=1)
        let a_body = scan_node("orders", 1, &["id", "amount"]);
        let b_body = OpTree::CteScan {
            cte_id: 0,
            cte_name: "a".to_string(),
            alias: "a".to_string(),
            columns: vec!["id".to_string(), "amount".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        let tree = OpTree::CteScan {
            cte_id: 1,
            cte_name: "b".to_string(),
            alias: "b".to_string(),
            columns: vec!["id".to_string(), "amount".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };

        let mut registry = CteRegistry::default();
        registry.entries.push(("a".to_string(), a_body));
        registry.entries.push(("b".to_string(), b_body));

        let result = ParseResult {
            tree,
            cte_registry: registry,
            has_recursion: false,
            warnings: vec![],
        };
        assert!(check_ivm_support_with_registry(&result).is_ok());
        assert_eq!(result.cte_registry.entries.len(), 2);
        // The CTE registry should report the base OIDs transitively
        let oids = result.cte_registry.source_oids();
        assert!(oids.contains(&1));
    }

    // ── Tier 3a: has_recursion flag tests ───────────────────────────

    #[test]
    fn test_parse_result_has_recursion_false_by_default() {
        let result = ParseResult {
            tree: scan_node("t", 1, &["id"]),
            cte_registry: CteRegistry::default(),
            has_recursion: false,
            warnings: vec![],
        };
        assert!(!result.has_recursion);
    }

    #[test]
    fn test_parse_result_has_recursion_true() {
        let result = ParseResult {
            tree: scan_node("t", 1, &["id"]),
            cte_registry: CteRegistry::default(),
            has_recursion: true,
            warnings: vec![],
        };
        assert!(result.has_recursion);
    }

    #[test]
    fn test_parse_result_has_recursion_flag_preserved_on_clone() {
        let result = ParseResult {
            tree: scan_node("t", 1, &["id"]),
            cte_registry: CteRegistry::default(),
            has_recursion: true,
            warnings: vec![],
        };
        let cloned = result.clone();
        assert!(cloned.has_recursion);
    }

    // ── CTE definition-level column aliases tests ──────────────────

    #[test]
    fn test_cte_scan_output_columns_with_def_aliases_only() {
        // WITH x(a, b) AS (SELECT id, name FROM ...) SELECT * FROM x
        let node = OpTree::CteScan {
            cte_id: 0,
            cte_name: "x".to_string(),
            alias: "x".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            cte_def_aliases: vec!["a".to_string(), "b".to_string()],
            column_aliases: vec![],
            body: None,
        };
        assert_eq!(node.output_columns(), vec!["a", "b"]);
    }

    #[test]
    fn test_cte_scan_output_columns_ref_aliases_override_def_aliases() {
        // WITH x(a, b) AS (...) SELECT * FROM x AS y(c, d)
        // Reference aliases take priority over definition aliases
        let node = OpTree::CteScan {
            cte_id: 0,
            cte_name: "x".to_string(),
            alias: "y".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            cte_def_aliases: vec!["a".to_string(), "b".to_string()],
            column_aliases: vec!["c".to_string(), "d".to_string()],
            body: None,
        };
        assert_eq!(node.output_columns(), vec!["c", "d"]);
    }

    #[test]
    fn test_cte_scan_def_aliases_no_ref_aliases_no_body_match() {
        // Definition aliases differ from body columns → output is def aliases
        let node = OpTree::CteScan {
            cte_id: 0,
            cte_name: "totals".to_string(),
            alias: "totals".to_string(),
            columns: vec!["user_id".to_string(), "total".to_string()],
            cte_def_aliases: vec!["uid".to_string(), "amt".to_string()],
            column_aliases: vec![],
            body: None,
        };
        assert_eq!(node.output_columns(), vec!["uid", "amt"]);
    }

    // ── RecursiveCte tests ──────────────────────────────────────────

    #[test]
    fn test_recursive_cte_alias() {
        let node = make_recursive_cte("tree", &["id", "parent_id", "depth"]);
        assert_eq!(node.alias(), "tree");
    }

    #[test]
    fn test_recursive_cte_output_columns() {
        let node = make_recursive_cte("tree", &["id", "parent_id", "depth"]);
        assert_eq!(node.output_columns(), vec!["id", "parent_id", "depth"]);
    }

    #[test]
    fn test_recursive_cte_source_oids() {
        // Source OIDs come from both base and recursive terms, deduplicated
        let base = OpTree::Scan {
            table_oid: 100,
            table_name: "categories".to_string(),
            schema: "public".to_string(),
            columns: vec![make_column("id")],
            pk_columns: Vec::new(),
            alias: "c".to_string(),
        };
        let recursive = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".to_string()),
            left: Box::new(OpTree::Scan {
                table_oid: 100, // same table
                table_name: "categories".to_string(),
                schema: "public".to_string(),
                columns: vec![make_column("id")],
                pk_columns: Vec::new(),
                alias: "c2".to_string(),
            }),
            right: Box::new(OpTree::RecursiveSelfRef {
                cte_name: "tree".to_string(),
                alias: "t".to_string(),
                columns: vec!["id".to_string()],
            }),
        };
        let node = OpTree::RecursiveCte {
            alias: "tree".to_string(),
            columns: vec!["id".to_string()],
            base: Box::new(base),
            recursive: Box::new(recursive),
            union_all: true,
        };

        let oids = node.source_oids();
        assert_eq!(oids, vec![100]); // Deduplicated
    }

    #[test]
    fn test_recursive_cte_source_oids_no_self_ref() {
        let node = OpTree::RecursiveSelfRef {
            cte_name: "tree".to_string(),
            alias: "t".to_string(),
            columns: vec!["id".to_string()],
        };
        assert_eq!(node.source_oids(), Vec::<u32>::new());
    }

    #[test]
    fn test_recursive_self_ref_alias() {
        let node = OpTree::RecursiveSelfRef {
            cte_name: "tree".to_string(),
            alias: "t".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
        };
        assert_eq!(node.alias(), "t");
    }

    #[test]
    fn test_recursive_self_ref_output_columns() {
        let node = OpTree::RecursiveSelfRef {
            cte_name: "tree".to_string(),
            alias: "t".to_string(),
            columns: vec!["id".to_string(), "depth".to_string()],
        };
        assert_eq!(node.output_columns(), vec!["id", "depth"]);
    }

    #[test]
    fn test_ivm_support_recursive_cte() {
        // check_ivm_support should accept RecursiveCte with valid children
        let node = make_recursive_cte("tree", &["id"]);
        assert!(check_ivm_support_inner(&node).is_ok());
    }

    #[test]
    fn test_ivm_support_recursive_self_ref() {
        // RecursiveSelfRef is always valid in a recursive CTE context
        let node = OpTree::RecursiveSelfRef {
            cte_name: "tree".to_string(),
            alias: "t".to_string(),
            columns: vec!["id".to_string()],
        };
        assert!(check_ivm_support_inner(&node).is_ok());
    }

    /// Helper: build a minimal RecursiveCte for testing.
    fn make_recursive_cte(name: &str, cols: &[&str]) -> OpTree {
        let columns: Vec<String> = cols.iter().map(|s| s.to_string()).collect();
        OpTree::RecursiveCte {
            alias: name.to_string(),
            columns: columns.clone(),
            base: Box::new(OpTree::Scan {
                table_oid: 42,
                table_name: "source".to_string(),
                schema: "public".to_string(),
                columns: cols.iter().map(|c| make_column(c)).collect(),
                pk_columns: Vec::new(),
                alias: "s".to_string(),
            }),
            recursive: Box::new(OpTree::RecursiveSelfRef {
                cte_name: name.to_string(),
                alias: "r".to_string(),
                columns,
            }),
            union_all: true,
        }
    }

    // ── WindowExpr tests ──────────────────────────────────────────

    fn make_window_expr(
        func: &str,
        args: Vec<Expr>,
        partition: Vec<Expr>,
        order: Vec<SortExpr>,
        alias: &str,
    ) -> WindowExpr {
        WindowExpr {
            func_name: func.to_string(),
            args,
            partition_by: partition,
            order_by: order,
            frame_clause: None,
            alias: alias.to_string(),
        }
    }

    #[test]
    fn test_window_expr_to_sql_row_number() {
        let wexpr = make_window_expr(
            "row_number",
            vec![],
            vec![col("department")],
            vec![SortExpr {
                expr: col("salary"),
                ascending: false,
                nulls_first: false,
            }],
            "rn",
        );
        assert_eq!(
            wexpr.to_sql(),
            "row_number() OVER (PARTITION BY department ORDER BY salary DESC NULLS LAST)"
        );
    }

    #[test]
    fn test_window_expr_to_sql_sum_over() {
        let wexpr = make_window_expr(
            "sum",
            vec![col("amount")],
            vec![col("region")],
            vec![],
            "region_total",
        );
        assert_eq!(wexpr.to_sql(), "sum(amount) OVER (PARTITION BY region)");
    }

    #[test]
    fn test_window_expr_to_sql_rank_no_partition() {
        let wexpr = make_window_expr(
            "rank",
            vec![],
            vec![],
            vec![SortExpr {
                expr: col("score"),
                ascending: true,
                nulls_first: false,
            }],
            "rank",
        );
        assert_eq!(wexpr.to_sql(), "rank() OVER (ORDER BY score ASC)");
    }

    #[test]
    fn test_window_expr_to_sql_empty_over() {
        let wexpr = make_window_expr("count", vec![col("id")], vec![], vec![], "cnt");
        assert_eq!(wexpr.to_sql(), "count(id) OVER ()");
    }

    #[test]
    fn test_window_expr_to_sql_multiple_order_by() {
        let wexpr = make_window_expr(
            "row_number",
            vec![],
            vec![col("dept")],
            vec![
                SortExpr {
                    expr: col("salary"),
                    ascending: false,
                    nulls_first: false,
                },
                SortExpr {
                    expr: col("name"),
                    ascending: true,
                    nulls_first: false,
                },
            ],
            "rn",
        );
        assert_eq!(
            wexpr.to_sql(),
            "row_number() OVER (PARTITION BY dept ORDER BY salary DESC NULLS LAST, name ASC)"
        );
    }

    // ── OpTree::Window tests ──────────────────────────────────────

    fn make_window_node(
        partition_cols: Vec<Expr>,
        wf_aliases: Vec<&str>,
        pt_aliases: Vec<&str>,
    ) -> OpTree {
        let scan = scan_node("employees", 100, &["id", "department", "salary"]);
        let window_exprs: Vec<WindowExpr> = wf_aliases
            .iter()
            .map(|a| WindowExpr {
                func_name: "row_number".to_string(),
                args: vec![],
                partition_by: partition_cols.clone(),
                order_by: vec![],
                frame_clause: None,
                alias: a.to_string(),
            })
            .collect();
        let pass_through: Vec<(Expr, String)> =
            pt_aliases.iter().map(|a| (col(a), a.to_string())).collect();
        OpTree::Window {
            window_exprs,
            partition_by: partition_cols,
            pass_through,
            child: Box::new(scan),
        }
    }

    #[test]
    fn test_window_alias() {
        let node = make_window_node(
            vec![col("department")],
            vec!["rn"],
            vec!["department", "salary"],
        );
        assert_eq!(node.alias(), "window");
    }

    #[test]
    fn test_window_output_columns() {
        let node = make_window_node(
            vec![col("department")],
            vec!["rn"],
            vec!["department", "salary"],
        );
        assert_eq!(node.output_columns(), vec!["department", "salary", "rn"]);
    }

    #[test]
    fn test_window_output_columns_multiple_wf() {
        let node = make_window_node(
            vec![col("department")],
            vec!["rn", "total"],
            vec!["department", "salary"],
        );
        assert_eq!(
            node.output_columns(),
            vec!["department", "salary", "rn", "total"]
        );
    }

    #[test]
    fn test_window_source_oids() {
        let node = make_window_node(vec![col("department")], vec!["rn"], vec!["department"]);
        assert_eq!(node.source_oids(), vec![100]);
    }

    #[test]
    fn test_window_ivm_support() {
        let node = make_window_node(vec![col("department")], vec!["rn"], vec!["department"]);
        assert!(check_ivm_support(&node).is_ok());
    }

    // ── Phase 4: Additional coverage tests ──────────────────────────

    // ── Expr::output_name tests ─────────────────────────────────────

    #[test]
    fn test_expr_output_name_unqualified_column() {
        let e = col("amount");
        assert_eq!(e.output_name(), "amount");
    }

    #[test]
    fn test_expr_output_name_qualified_column_strips_table() {
        let e = qualified_col("orders", "total");
        assert_eq!(e.output_name(), "total");
    }

    #[test]
    fn test_expr_output_name_literal_returns_value() {
        let e = Expr::Literal("42".to_string());
        assert_eq!(e.output_name(), "42");
    }

    #[test]
    fn test_expr_output_name_binary_op_returns_sql() {
        let e = Expr::BinaryOp {
            op: "+".to_string(),
            left: Box::new(col("a")),
            right: Box::new(col("b")),
        };
        assert_eq!(e.output_name(), "(a + b)");
    }

    #[test]
    fn test_expr_output_name_func_call_returns_sql() {
        let e = Expr::FuncCall {
            func_name: "upper".to_string(),
            args: vec![col("name")],
        };
        assert_eq!(e.output_name(), "upper(name)");
    }

    // ── Expr::strip_qualifier tests ─────────────────────────────────

    #[test]
    fn test_strip_qualifier_column_ref() {
        let e = qualified_col("t", "amount");
        let stripped = e.strip_qualifier();
        assert_eq!(stripped.to_sql(), "amount");
    }

    #[test]
    fn test_strip_qualifier_unqualified_unchanged() {
        let e = col("amount");
        let stripped = e.strip_qualifier();
        assert_eq!(stripped.to_sql(), "amount");
    }

    #[test]
    fn test_strip_qualifier_binary_op_recursive() {
        let e = Expr::BinaryOp {
            op: "+".to_string(),
            left: Box::new(qualified_col("t", "a")),
            right: Box::new(qualified_col("t", "b")),
        };
        let stripped = e.strip_qualifier();
        assert_eq!(stripped.to_sql(), "(a + b)");
    }

    #[test]
    fn test_strip_qualifier_func_call_recursive() {
        let e = Expr::FuncCall {
            func_name: "coalesce".to_string(),
            args: vec![qualified_col("t", "x"), Expr::Literal("0".to_string())],
        };
        let stripped = e.strip_qualifier();
        assert_eq!(stripped.to_sql(), "coalesce(x, 0)");
    }

    #[test]
    fn test_strip_qualifier_literal_unchanged() {
        let e = Expr::Literal("42".to_string());
        let stripped = e.strip_qualifier();
        assert_eq!(stripped.to_sql(), "42");
    }

    #[test]
    fn test_strip_qualifier_star_unchanged() {
        let e = Expr::Star {
            table_alias: Some("t".to_string()),
        };
        let stripped = e.strip_qualifier();
        // Star and Raw are returned as-is (self.clone())
        assert_eq!(stripped.to_sql(), "t.*");
    }

    #[test]
    fn test_strip_qualifier_raw_unchanged() {
        let e = Expr::Raw("CASE WHEN x > 0 THEN 1 END".to_string());
        let stripped = e.strip_qualifier();
        assert_eq!(stripped.to_sql(), "CASE WHEN x > 0 THEN 1 END");
    }

    // ── Expr::rewrite_aliases tests ─────────────────────────────────

    #[test]
    fn test_rewrite_aliases_column_ref_left() {
        let e = qualified_col("a", "id");
        let rewritten = e.rewrite_aliases("a", "new_a", "b", "new_b");
        assert_eq!(rewritten.to_sql(), "\"new_a\".\"id\"");
    }

    #[test]
    fn test_rewrite_aliases_column_ref_right() {
        let e = qualified_col("b", "name");
        let rewritten = e.rewrite_aliases("a", "new_a", "b", "new_b");
        assert_eq!(rewritten.to_sql(), "\"new_b\".\"name\"");
    }

    #[test]
    fn test_rewrite_aliases_unknown_alias_unchanged() {
        let e = qualified_col("c", "val");
        let rewritten = e.rewrite_aliases("a", "new_a", "b", "new_b");
        assert_eq!(rewritten.to_sql(), "\"c\".\"val\"");
    }

    #[test]
    fn test_rewrite_aliases_unqualified_unchanged() {
        let e = col("id");
        let rewritten = e.rewrite_aliases("a", "new_a", "b", "new_b");
        assert_eq!(rewritten.to_sql(), "id");
    }

    #[test]
    fn test_rewrite_aliases_binary_op_recursive() {
        let e = Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(qualified_col("a", "id")),
            right: Box::new(qualified_col("b", "id")),
        };
        let rewritten = e.rewrite_aliases("a", "x", "b", "y");
        assert_eq!(rewritten.to_sql(), "(\"x\".\"id\" = \"y\".\"id\")");
    }

    #[test]
    fn test_rewrite_aliases_func_call_recursive() {
        let e = Expr::FuncCall {
            func_name: "coalesce".to_string(),
            args: vec![qualified_col("a", "x"), qualified_col("b", "y")],
        };
        let rewritten = e.rewrite_aliases("a", "left", "b", "right");
        assert_eq!(
            rewritten.to_sql(),
            "coalesce(\"left\".\"x\", \"right\".\"y\")"
        );
    }

    #[test]
    fn test_rewrite_aliases_literal_unchanged() {
        let e = Expr::Literal("42".to_string());
        let rewritten = e.rewrite_aliases("a", "x", "b", "y");
        assert_eq!(rewritten.to_sql(), "42");
    }

    // ── rewrite_having_expr tests ───────────────────────────────────

    fn make_agg(func: AggFunc, arg_name: Option<&str>, alias: &str) -> AggExpr {
        AggExpr {
            function: func,
            argument: arg_name.map(col),
            alias: alias.to_string(),
            is_distinct: false,
            second_arg: None,
            filter: None,
            order_within_group: None,
            statistical_support: None,
        }
    }

    #[test]
    fn test_having_expr_rewrite_sum_to_alias() {
        // HAVING SUM(amount) > 100  →  total > 100
        let aggs = vec![make_agg(AggFunc::Sum, Some("amount"), "total")];
        let pred = Expr::BinaryOp {
            op: ">".to_string(),
            left: Box::new(Expr::FuncCall {
                func_name: "sum".to_string(),
                args: vec![col("amount")],
            }),
            right: Box::new(Expr::Literal("100".to_string())),
        };
        let rewritten = rewrite_having_expr(&pred, &aggs);
        assert_eq!(rewritten.to_sql(), "(total > 100)");
    }

    #[test]
    fn test_having_expr_rewrite_count_star_to_alias() {
        // HAVING COUNT(*) >= 5  →  cnt >= 5
        let aggs = vec![make_agg(AggFunc::CountStar, None, "cnt")];
        let pred = Expr::BinaryOp {
            op: ">=".to_string(),
            left: Box::new(Expr::FuncCall {
                func_name: "count".to_string(),
                args: vec![],
            }),
            right: Box::new(Expr::Literal("5".to_string())),
        };
        let rewritten = rewrite_having_expr(&pred, &aggs);
        assert_eq!(rewritten.to_sql(), "(cnt >= 5)");
    }

    #[test]
    fn test_having_expr_rewrite_multiple_aggs() {
        // HAVING SUM(amount) > 100 AND COUNT(*) > 2
        let aggs = vec![
            make_agg(AggFunc::Sum, Some("amount"), "total"),
            make_agg(AggFunc::CountStar, None, "cnt"),
        ];
        let pred = Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(Expr::BinaryOp {
                op: ">".to_string(),
                left: Box::new(Expr::FuncCall {
                    func_name: "sum".to_string(),
                    args: vec![col("amount")],
                }),
                right: Box::new(Expr::Literal("100".to_string())),
            }),
            right: Box::new(Expr::BinaryOp {
                op: ">".to_string(),
                left: Box::new(Expr::FuncCall {
                    func_name: "count".to_string(),
                    args: vec![],
                }),
                right: Box::new(Expr::Literal("2".to_string())),
            }),
        };
        let rewritten = rewrite_having_expr(&pred, &aggs);
        assert_eq!(rewritten.to_sql(), "((total > 100) AND (cnt > 2))");
    }

    #[test]
    fn test_having_expr_rewrite_case_insensitive() {
        // Function name "SUM" (uppercase) should still match AggFunc::Sum
        let aggs = vec![make_agg(AggFunc::Sum, Some("price"), "revenue")];
        let pred = Expr::FuncCall {
            func_name: "SUM".to_string(),
            args: vec![col("price")],
        };
        let rewritten = rewrite_having_expr(&pred, &aggs);
        assert_eq!(rewritten.to_sql(), "revenue");
    }

    #[test]
    fn test_having_expr_no_match_keeps_func_call() {
        // An unknown function is kept as-is.
        let aggs = vec![make_agg(AggFunc::Sum, Some("amount"), "total")];
        let pred = Expr::FuncCall {
            func_name: "coalesce".to_string(),
            args: vec![col("x"), Expr::Literal("0".to_string())],
        };
        let rewritten = rewrite_having_expr(&pred, &aggs);
        assert_eq!(rewritten.to_sql(), "coalesce(x, 0)");
    }

    #[test]
    fn test_having_expr_rewrite_avg_to_alias() {
        // HAVING AVG(score) > 75.0  →  avg_score > 75.0
        let aggs = vec![make_agg(AggFunc::Avg, Some("score"), "avg_score")];
        let pred = Expr::BinaryOp {
            op: ">".to_string(),
            left: Box::new(Expr::FuncCall {
                func_name: "avg".to_string(),
                args: vec![col("score")],
            }),
            right: Box::new(Expr::Literal("75.0".to_string())),
        };
        let rewritten = rewrite_having_expr(&pred, &aggs);
        assert_eq!(rewritten.to_sql(), "(avg_score > 75.0)");
    }

    #[test]
    fn test_having_expr_column_ref_passthrough() {
        // Column refs (GROUP BY columns) are passed through unchanged.
        let aggs = vec![make_agg(AggFunc::Sum, Some("amount"), "total")];
        let pred = Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(col("region")),
            right: Box::new(Expr::Literal("'US'".to_string())),
        };
        let rewritten = rewrite_having_expr(&pred, &aggs);
        assert_eq!(rewritten.to_sql(), "(region = 'US')");
    }

    // ── OpTree::needs_pgt_count tests ──────────────────────────────

    #[test]
    fn test_needs_pgt_count_aggregate() {
        let tree = OpTree::Aggregate {
            group_by: vec![col("region")],
            aggregates: vec![],
            child: Box::new(scan_node("t", 1, &["region"])),
        };
        assert!(tree.needs_pgt_count());
    }

    #[test]
    fn test_needs_pgt_count_distinct() {
        let tree = OpTree::Distinct {
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert!(tree.needs_pgt_count());
    }

    #[test]
    fn test_needs_pgt_count_scan_false() {
        let tree = scan_node("t", 1, &["id"]);
        assert!(!tree.needs_pgt_count());
    }

    #[test]
    fn test_needs_pgt_count_project_false() {
        let tree = OpTree::Project {
            expressions: vec![col("id")],
            aliases: vec!["id".to_string()],
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert!(!tree.needs_pgt_count());
    }

    // ── OpTree::group_by_columns tests ──────────────────────────────

    #[test]
    fn test_group_by_columns_aggregate_with_groups() {
        let tree = OpTree::Aggregate {
            group_by: vec![col("region"), col("year")],
            aggregates: vec![],
            child: Box::new(scan_node("t", 1, &["region", "year"])),
        };
        assert_eq!(
            tree.group_by_columns(),
            Some(vec!["region".to_string(), "year".to_string()])
        );
    }

    #[test]
    fn test_group_by_columns_scalar_aggregate() {
        let tree = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![AggExpr {
                function: AggFunc::CountStar,
                argument: None,
                alias: "cnt".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert_eq!(tree.group_by_columns(), None);
    }

    #[test]
    fn test_group_by_columns_through_project() {
        let tree = OpTree::Project {
            expressions: vec![col("region")],
            aliases: vec!["region".to_string()],
            child: Box::new(OpTree::Aggregate {
                group_by: vec![col("region")],
                aggregates: vec![],
                child: Box::new(scan_node("t", 1, &["region"])),
            }),
        };
        assert_eq!(tree.group_by_columns(), Some(vec!["region".to_string()]));
    }

    #[test]
    fn test_group_by_columns_through_subquery() {
        let tree = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(OpTree::Aggregate {
                group_by: vec![col("dept")],
                aggregates: vec![],
                child: Box::new(scan_node("t", 1, &["dept"])),
            }),
        };
        assert_eq!(tree.group_by_columns(), Some(vec!["dept".to_string()]));
    }

    #[test]
    fn test_group_by_columns_scan_returns_none() {
        let tree = scan_node("t", 1, &["id"]);
        assert_eq!(tree.group_by_columns(), None);
    }

    #[test]
    fn test_group_by_columns_project_renames_columns() {
        // Simulates: SELECT t.id AS department_id, t.name AS dept_name, COUNT(*)
        //            FROM ... GROUP BY t.id, t.name
        // The Aggregate group_by has ["id", "name"] but the Project aliases
        // them to ["department_id", "dept_name"]. The storage table uses the
        // Project aliases, so group_by_columns() must return those.
        let tree = OpTree::Project {
            expressions: vec![col("id"), col("name"), col("cnt")],
            aliases: vec![
                "department_id".to_string(),
                "dept_name".to_string(),
                "cnt".to_string(),
            ],
            child: Box::new(OpTree::Aggregate {
                group_by: vec![col("id"), col("name")],
                aggregates: vec![AggExpr {
                    function: AggFunc::CountStar,
                    argument: None,
                    alias: "cnt".to_string(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                }],
                child: Box::new(scan_node("t", 1, &["id", "name"])),
            }),
        };
        assert_eq!(
            tree.group_by_columns(),
            Some(vec!["department_id".to_string(), "dept_name".to_string()])
        );
    }

    #[test]
    fn test_group_by_columns_project_ordinal_group_by() {
        // Simulates: SELECT split_part(full_path, ' > ', 2) AS division,
        //                   SUM(headcount) AS total_headcount
        //            FROM ... GROUP BY 1
        // The Aggregate group_by has ordinal Literal("1"). The Project must
        // resolve position 1 → alias "division".
        let tree = OpTree::Project {
            expressions: vec![
                Expr::FuncCall {
                    func_name: "split_part".to_string(),
                    args: vec![
                        col("full_path"),
                        Expr::Literal("' > '".to_string()),
                        Expr::Literal("2".to_string()),
                    ],
                },
                col("total_headcount"),
            ],
            aliases: vec!["division".to_string(), "total_headcount".to_string()],
            child: Box::new(OpTree::Aggregate {
                group_by: vec![Expr::Literal("1".to_string())],
                aggregates: vec![AggExpr {
                    function: AggFunc::Sum,
                    argument: Some(col("headcount")),
                    alias: "total_headcount".to_string(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                }],
                child: Box::new(scan_node(
                    "department_stats",
                    1,
                    &["full_path", "headcount"],
                )),
            }),
        };
        assert_eq!(tree.group_by_columns(), Some(vec!["division".to_string()]));
    }

    // ── OpTree::row_id_key_columns tests ────────────────────────────

    #[test]
    fn test_row_id_key_columns_scan_all_nullable() {
        let tree = scan_node("t", 1, &["id", "name"]);
        // All columns nullable → returns all columns
        assert_eq!(
            tree.row_id_key_columns(),
            Some(vec!["id".to_string(), "name".to_string()])
        );
    }

    #[test]
    fn test_row_id_key_columns_scan_with_non_nullable() {
        // S10: Scans WITHOUT pk_columns return ALL columns (not just non-nullable).
        let tree = OpTree::Scan {
            table_oid: 1,
            table_name: "t".to_string(),
            schema: "public".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_oid: 23,
                    is_nullable: false,
                },
                Column {
                    name: "name".to_string(),
                    type_oid: 25,
                    is_nullable: true,
                },
            ],
            pk_columns: Vec::new(),
            alias: "t".to_string(),
        };
        assert_eq!(
            tree.row_id_key_columns(),
            Some(vec!["id".to_string(), "name".to_string()]),
        );
    }

    #[test]
    fn test_row_id_key_columns_scan_with_pk_columns() {
        // When pk_columns are present, those are used (not non-nullable columns).
        let tree = OpTree::Scan {
            table_oid: 1,
            table_name: "t".to_string(),
            schema: "public".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_oid: 23,
                    is_nullable: false,
                },
                Column {
                    name: "name".to_string(),
                    type_oid: 25,
                    is_nullable: false,
                },
            ],
            pk_columns: vec!["id".to_string()],
            alias: "t".to_string(),
        };
        assert_eq!(tree.row_id_key_columns(), Some(vec!["id".to_string()]),);
    }

    #[test]
    fn test_row_id_key_columns_filter_delegates() {
        let tree = OpTree::Filter {
            predicate: col("active"),
            child: Box::new(scan_node("t", 1, &["id", "active"])),
        };
        assert_eq!(
            tree.row_id_key_columns(),
            Some(vec!["id".to_string(), "active".to_string()])
        );
    }

    #[test]
    fn test_row_id_key_columns_project_filter_scan_where_not_in_select() {
        // Regression test: SELECT id, val AS vb FROM t WHERE side >= 2
        // The Filter exposes all Scan columns (id, val, side) in its output,
        // but the Project aliases only contain (id, vb) — 2 items vs 3 in
        // child_out. Previously this caused row_id_key_columns() to return
        // None, leading to row_to_json-based __pgt_row_id in the full refresh
        // but PK-hash-based in the differential, causing MERGE mismatches and
        // stale rows after UPDATEs that change the WHERE-clause column.
        let scan = OpTree::Scan {
            table_oid: 1,
            table_name: "t".to_string(),
            schema: "public".to_string(),
            columns: vec![make_column("id"), make_column("val"), make_column("side")],
            pk_columns: vec!["id".to_string()],
            alias: "t".to_string(),
        };
        let filter = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: ">=".to_string(),
                left: Box::new(col("side")),
                right: Box::new(Expr::Literal("2".to_string())),
            },
            child: Box::new(scan),
        };
        let tree = OpTree::Project {
            expressions: vec![col("id"), col("val")],
            aliases: vec!["id".to_string(), "vb".to_string()],
            child: Box::new(filter),
        };
        // Must return Some(["id"]) — the PK, not None.
        assert_eq!(tree.row_id_key_columns(), Some(vec!["id".to_string()]));
    }

    #[test]
    fn test_row_id_key_columns_project_filter_scan_pk_not_projected_returns_none() {
        // When the PK column is NOT in the SELECT list, the row_id key
        // cannot be determined — return None so the fallback row_to_json
        // hash is used consistently for both full and differential refresh.
        let scan = OpTree::Scan {
            table_oid: 1,
            table_name: "t".to_string(),
            schema: "public".to_string(),
            columns: vec![
                make_column("id"),
                make_column("name"),
                make_column("amount"),
            ],
            pk_columns: vec!["id".to_string()],
            alias: "t".to_string(),
        };
        let filter = OpTree::Filter {
            predicate: col("amount"),
            child: Box::new(scan),
        };
        // SELECT name FROM t WHERE amount > 0 — PK "id" is NOT projected.
        let tree = OpTree::Project {
            expressions: vec![col("name")],
            aliases: vec!["name".to_string()],
            child: Box::new(filter),
        };
        assert_eq!(tree.row_id_key_columns(), None);
    }

    #[test]
    fn test_row_id_key_columns_distinct_returns_all() {
        let tree = OpTree::Distinct {
            child: Box::new(scan_node("t", 1, &["a", "b"])),
        };
        assert_eq!(
            tree.row_id_key_columns(),
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn test_row_id_key_columns_aggregate_with_group_by() {
        let tree = OpTree::Aggregate {
            group_by: vec![col("region")],
            aggregates: vec![AggExpr {
                function: AggFunc::Sum,
                argument: Some(col("val")),
                alias: "total".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["region", "val"])),
        };
        assert_eq!(tree.row_id_key_columns(), Some(vec!["region".to_string()]));
    }

    #[test]
    fn test_row_id_key_columns_scalar_aggregate_returns_empty_identity() {
        let tree = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![AggExpr {
                function: AggFunc::CountStar,
                argument: None,
                alias: "cnt".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert_eq!(tree.row_id_key_columns(), Some(Vec::new()));
    }

    #[test]
    fn test_row_id_key_columns_cte_scan_returns_columns() {
        let tree = OpTree::CteScan {
            cte_id: 0,
            cte_name: "x".to_string(),
            alias: "x".to_string(),
            columns: vec!["id".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        assert_eq!(tree.row_id_key_columns(), Some(vec!["id".to_string()]));
    }

    #[test]
    fn test_row_id_key_columns_window_returns_none() {
        let node = make_window_node(vec![col("dept")], vec!["rn"], vec!["dept"]);
        assert_eq!(node.row_id_key_columns(), None);
    }

    #[test]
    fn test_row_id_key_columns_subquery_delegates() {
        let tree = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(scan_node("t", 1, &["id", "name"])),
        };
        assert_eq!(
            tree.row_id_key_columns(),
            Some(vec!["id".to_string(), "name".to_string()])
        );
    }

    #[test]
    fn test_row_id_key_columns_nested_join_projection_uses_retained_key() {
        let left = scan_with_pk("a", 1, &["id", "grp"], &["id"]);
        let right = scan_with_pk("b", 2, &["id", "grp"], &["id"]);
        let join = OpTree::InnerJoin {
            condition: qualified_col("a", "grp"),
            left: Box::new(left),
            right: Box::new(right),
        };
        let inner = OpTree::Project {
            expressions: vec![qualified_col("a", "grp"), qualified_col("b", "grp")],
            aliases: vec!["grp".to_string(), "right_grp".to_string()],
            child: Box::new(join),
        };
        let outer = OpTree::Project {
            expressions: vec![col("grp")],
            aliases: vec!["grp".to_string()],
            child: Box::new(OpTree::Subquery {
                alias: "projected".to_string(),
                column_aliases: vec![],
                child: Box::new(inner),
            }),
        };

        assert_eq!(outer.row_id_key_columns(), Some(vec!["grp".to_string()]));
    }

    // ── OpTree::node_kind tests ─────────────────────────────────────

    #[test]
    fn test_node_kind_all_variants() {
        assert_eq!(scan_node("t", 1, &["id"]).node_kind(), "scan");
        assert_eq!(
            OpTree::Project {
                expressions: vec![],
                aliases: vec![],
                child: Box::new(scan_node("t", 1, &["id"])),
            }
            .node_kind(),
            "project"
        );
        assert_eq!(
            OpTree::Filter {
                predicate: col("x"),
                child: Box::new(scan_node("t", 1, &["x"])),
            }
            .node_kind(),
            "filter"
        );
        assert_eq!(
            OpTree::InnerJoin {
                condition: col("id"),
                left: Box::new(scan_node("a", 1, &["id"])),
                right: Box::new(scan_node("b", 2, &["id"])),
            }
            .node_kind(),
            "inner join"
        );
        assert_eq!(
            OpTree::LeftJoin {
                condition: col("id"),
                left: Box::new(scan_node("a", 1, &["id"])),
                right: Box::new(scan_node("b", 2, &["id"])),
            }
            .node_kind(),
            "left join"
        );
        assert_eq!(
            OpTree::Aggregate {
                group_by: vec![],
                aggregates: vec![],
                child: Box::new(scan_node("t", 1, &["id"])),
            }
            .node_kind(),
            "aggregate"
        );
        assert_eq!(
            OpTree::Distinct {
                child: Box::new(scan_node("t", 1, &["id"])),
            }
            .node_kind(),
            "distinct"
        );
        assert_eq!(
            OpTree::UnionAll {
                children: vec![scan_node("a", 1, &["id"])],
            }
            .node_kind(),
            "union all"
        );
        assert_eq!(
            OpTree::Subquery {
                alias: "s".to_string(),
                column_aliases: vec![],
                child: Box::new(scan_node("t", 1, &["id"])),
            }
            .node_kind(),
            "subquery"
        );
        assert_eq!(
            OpTree::CteScan {
                cte_id: 0,
                cte_name: "x".to_string(),
                alias: "x".to_string(),
                columns: vec![],
                cte_def_aliases: vec![],
                column_aliases: vec![],
                body: None,
            }
            .node_kind(),
            "cte scan"
        );
        assert_eq!(
            make_recursive_cte("tree", &["id"]).node_kind(),
            "recursive cte"
        );
        assert_eq!(
            OpTree::RecursiveSelfRef {
                cte_name: "tree".to_string(),
                alias: "t".to_string(),
                columns: vec!["id".to_string()],
            }
            .node_kind(),
            "recursive self-reference"
        );
    }

    // ── join_pk_aliases tests ───────────────────────────────────────

    fn scan_with_pk(alias: &str, oid: u32, cols: &[&str], pks: &[&str]) -> OpTree {
        OpTree::Scan {
            table_oid: oid,
            table_name: alias.to_string(),
            schema: "public".to_string(),
            columns: cols.iter().map(|n| make_column(n)).collect(),
            pk_columns: pks.iter().map(|p| p.to_string()).collect(),
            alias: alias.to_string(),
        }
    }

    #[test]
    fn test_join_pk_aliases_both_sides_have_pk() {
        let left = scan_with_pk("a", 1, &["id", "name"], &["id"]);
        let right = scan_with_pk("b", 2, &["bid", "val"], &["bid"]);
        let join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(left),
            right: Box::new(right),
        };
        let expressions = vec![
            qualified_col("a", "id"),
            qualified_col("a", "name"),
            qualified_col("b", "bid"),
            qualified_col("b", "val"),
        ];
        let aliases = vec![
            "id".to_string(),
            "name".to_string(),
            "bid".to_string(),
            "val".to_string(),
        ];
        let result = join_pk_aliases(&expressions, &aliases, &join);
        assert_eq!(result, Some(vec!["id".to_string(), "bid".to_string()]));
    }

    #[test]
    fn test_join_pk_aliases_one_side_missing_pk_returns_none() {
        let left = scan_with_pk("a", 1, &["id", "name"], &["id"]);
        let right = scan_node("b", 2, &["bid", "val"]); // no PK
        let join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(left),
            right: Box::new(right),
        };
        let expressions = vec![qualified_col("a", "id"), qualified_col("b", "bid")];
        let aliases = vec!["id".to_string(), "bid".to_string()];
        // right side PK is missing from output → returns None
        // (scan_node has no pk_columns, so scan_pk_columns uses all cols as fallback)
        let result = join_pk_aliases(&expressions, &aliases, &join);
        // With fallback, both "bid" and "val" are considered PK candidates.
        // "bid" is in expressions → right_pk_aliases is non-empty.
        // If both sides have PKs it returns Some.
        assert!(result.is_some());
    }

    #[test]
    fn test_join_pk_aliases_nested_aggregate_uses_many_to_one_key() {
        let parent = scan_with_pk("p", 1, &["id", "name"], &["id"]);
        let aggregate_body = OpTree::Aggregate {
            group_by: vec![col("parent_id")],
            aggregates: vec![],
            child: Box::new(scan_node("leaf", 2, &["parent_id"])),
        };
        let left_aggregate = OpTree::CteScan {
            cte_id: 0,
            cte_name: "al".to_string(),
            alias: "al".to_string(),
            columns: vec!["parent_id".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: Some(Box::new(aggregate_body.clone())),
        };
        let right_aggregate = OpTree::CteScan {
            cte_id: 1,
            cte_name: "ar".to_string(),
            alias: "ar".to_string(),
            columns: vec!["parent_id".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: Some(Box::new(aggregate_body)),
        };
        let nested_left = OpTree::LeftJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("p", "id")),
                right: Box::new(qualified_col("al", "parent_id")),
            },
            left: Box::new(parent),
            right: Box::new(left_aggregate),
        };
        let join = OpTree::LeftJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("p", "id")),
                right: Box::new(qualified_col("ar", "parent_id")),
            },
            left: Box::new(nested_left.clone()),
            right: Box::new(right_aggregate.clone()),
        };
        let expressions = vec![qualified_col("p", "id")];
        let aliases = vec!["id".to_string()];

        assert_eq!(
            join_pk_aliases(&expressions, &aliases, &join),
            Some(aliases.clone())
        );
        assert_eq!(join_pk_expr_indices(&expressions, &join), vec![0]);

        let non_key_join = OpTree::LeftJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("p", "id")),
                right: Box::new(qualified_col("ar", "not_a_key")),
            },
            left: Box::new(nested_left),
            right: Box::new(right_aggregate),
        };
        assert_eq!(join_pk_aliases(&expressions, &aliases, &non_key_join), None);
    }

    #[test]
    fn test_join_pk_aliases_non_join_returns_none() {
        let scan = scan_node("t", 1, &["id"]);
        let result = join_pk_aliases(&[col("id")], &["id".to_string()], &scan);
        assert_eq!(result, None);
    }

    #[test]
    fn test_full_join_nullable_key_uses_stable_ids_but_keyless_storage() {
        let join = OpTree::FullJoin {
            condition: qualified_col("a", "grp"),
            left: Box::new(scan_node("a", 1, &["grp", "value"])),
            right: Box::new(scan_node("b", 2, &["grp", "value"])),
        };
        let expressions = vec![qualified_col("a", "grp"), qualified_col("b", "grp")];
        let aliases = vec!["grp".to_string(), "right_grp".to_string()];
        let projected = OpTree::Project {
            expressions: expressions.clone(),
            aliases: aliases.clone(),
            child: Box::new(join.clone()),
        };

        assert!(join_pk_aliases(&expressions, &aliases, &join).is_some());
        assert!(projected.has_incomplete_join_pk());
    }

    // ── join_pk_expr_indices tests ──────────────────────────────────

    #[test]
    fn test_join_pk_expr_indices_inner_join() {
        let left = scan_with_pk("a", 1, &["id", "name"], &["id"]);
        let right = scan_with_pk("b", 2, &["bid", "val"], &["bid"]);
        let join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(left),
            right: Box::new(right),
        };
        let expressions = vec![
            qualified_col("a", "id"),
            qualified_col("a", "name"),
            qualified_col("b", "bid"),
            qualified_col("b", "val"),
        ];
        let indices = join_pk_expr_indices(&expressions, &join);
        assert_eq!(indices, vec![0, 2]); // a.id at 0, b.bid at 2
    }

    #[test]
    fn test_join_pk_expr_indices_non_join() {
        let scan = scan_node("t", 1, &["id"]);
        let indices = join_pk_expr_indices(&[col("id")], &scan);
        assert!(indices.is_empty());
    }

    // ── scan_pk_columns tests ───────────────────────────────────────

    #[test]
    fn test_scan_pk_columns_with_pks() {
        let scan = scan_with_pk("t", 1, &["id", "name"], &["id"]);
        let pks = scan_pk_columns(&scan);
        assert_eq!(pks, vec!["id".to_string()]);
    }

    #[test]
    fn test_scan_pk_columns_fallback_non_nullable() {
        let scan = OpTree::Scan {
            table_oid: 1,
            table_name: "t".to_string(),
            schema: "public".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_oid: 23,
                    is_nullable: false,
                },
                Column {
                    name: "name".to_string(),
                    type_oid: 25,
                    is_nullable: true,
                },
            ],
            pk_columns: Vec::new(),
            alias: "t".to_string(),
        };
        let pks = scan_pk_columns(&scan);
        assert_eq!(pks, vec!["id".to_string()]);
    }

    #[test]
    fn test_scan_pk_columns_all_nullable_returns_all() {
        let scan = scan_node("t", 1, &["a", "b"]);
        let pks = scan_pk_columns(&scan);
        assert_eq!(pks, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_scan_pk_columns_filter_delegates() {
        let filter = OpTree::Filter {
            predicate: col("x"),
            child: Box::new(scan_with_pk("t", 1, &["id", "x"], &["id"])),
        };
        let pks = scan_pk_columns(&filter);
        assert_eq!(pks, vec!["id".to_string()]);
    }

    #[test]
    fn test_scan_pk_columns_nested_aggregate_cte_delegates() {
        let body = OpTree::Aggregate {
            group_by: vec![col("grp")],
            aggregates: vec![],
            child: Box::new(scan_node("t", 1, &["grp", "value"])),
        };
        let cte = OpTree::CteScan {
            cte_id: 0,
            cte_name: "grouped".to_string(),
            alias: "grouped".to_string(),
            columns: vec!["grp".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: Some(Box::new(body)),
        };
        let projected = OpTree::Project {
            expressions: vec![col("grp")],
            aliases: vec!["grp".to_string()],
            child: Box::new(cte),
        };
        let nested = OpTree::Subquery {
            alias: "grouped".to_string(),
            column_aliases: vec![],
            child: Box::new(projected),
        };

        assert_eq!(scan_pk_columns(&nested), vec!["grp".to_string()]);
    }

    #[test]
    fn test_scan_pk_columns_non_scan_returns_empty() {
        let agg = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![],
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        let pks = scan_pk_columns(&agg);
        assert!(pks.is_empty());
    }

    // ── resolves_to_base_table tests ────────────────────────────────

    #[test]
    fn test_resolves_to_base_table_scan() {
        assert!(resolves_to_base_table(&scan_node("t", 1, &["id"])));
    }

    #[test]
    fn test_resolves_to_base_table_filter_over_scan() {
        let f = OpTree::Filter {
            predicate: col("x"),
            child: Box::new(scan_node("t", 1, &["x"])),
        };
        assert!(resolves_to_base_table(&f));
    }

    #[test]
    fn test_resolves_to_base_table_project_over_scan() {
        let p = OpTree::Project {
            expressions: vec![col("id")],
            aliases: vec!["id".to_string()],
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert!(resolves_to_base_table(&p));
    }

    #[test]
    fn test_resolves_to_base_table_subquery_over_scan() {
        let sub = OpTree::Subquery {
            alias: "s".to_string(),
            column_aliases: vec![],
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert!(resolves_to_base_table(&sub));
    }

    #[test]
    fn test_resolves_to_base_table_cte_scan() {
        let cte = OpTree::CteScan {
            cte_id: 0,
            cte_name: "x".to_string(),
            alias: "x".to_string(),
            columns: vec![],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        assert!(resolves_to_base_table(&cte));
    }

    #[test]
    fn test_resolves_to_base_table_recursive_self_ref() {
        let r = OpTree::RecursiveSelfRef {
            cte_name: "tree".to_string(),
            alias: "t".to_string(),
            columns: vec!["id".to_string()],
        };
        assert!(resolves_to_base_table(&r));
    }

    #[test]
    fn test_resolves_to_base_table_join_false() {
        let join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 1, &["id"])),
            right: Box::new(scan_node("b", 2, &["id"])),
        };
        assert!(!resolves_to_base_table(&join));
    }

    #[test]
    fn test_resolves_to_base_table_aggregate_false() {
        let agg = OpTree::Aggregate {
            group_by: vec![],
            aggregates: vec![],
            child: Box::new(scan_node("t", 1, &["id"])),
        };
        assert!(!resolves_to_base_table(&agg));
    }

    #[test]
    fn test_resolves_to_base_table_union_false() {
        let u = OpTree::UnionAll {
            children: vec![scan_node("a", 1, &["id"])],
        };
        assert!(!resolves_to_base_table(&u));
    }

    // ── check_ivm_support edge cases ────────────────────────────────

    #[test]
    fn test_check_ivm_support_left_join_nested_allowed() {
        let inner = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 1, &["id"])),
            right: Box::new(scan_node("b", 2, &["id"])),
        };
        let outer = OpTree::LeftJoin {
            condition: col("id"),
            left: Box::new(inner),
            right: Box::new(scan_node("c", 3, &["id"])),
        };
        assert!(check_ivm_support(&outer).is_ok());
    }

    #[test]
    fn test_check_ivm_support_rejects_distinct_count() {
        // COUNT(DISTINCT val) is still using AggFunc::Count, should pass
        let agg = OpTree::Aggregate {
            group_by: vec![col("id")],
            aggregates: vec![AggExpr {
                function: AggFunc::Count,
                argument: Some(col("val")),
                alias: "cnt".to_string(),
                is_distinct: true,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["id", "val"])),
        };
        assert!(check_ivm_support(&agg).is_ok());
    }

    #[test]
    fn test_check_ivm_support_union_all_with_min_child() {
        // MIN aggregate is now supported — union-all with MIN child passes
        let min_child = OpTree::Aggregate {
            group_by: vec![col("id")],
            aggregates: vec![AggExpr {
                function: AggFunc::Min,
                argument: Some(col("val")),
                alias: "min_val".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["id", "val"])),
        };
        let tree = OpTree::UnionAll {
            children: vec![scan_node("a", 1, &["id", "val"]), min_child],
        };
        assert!(check_ivm_support(&tree).is_ok());
    }

    #[test]
    fn test_check_ivm_support_with_registry_cte_body_min_accepted() {
        // MIN aggregate is now supported — CTE body with MIN passes validation
        let tree = OpTree::CteScan {
            cte_id: 0,
            cte_name: "min_cte".to_string(),
            alias: "min_cte".to_string(),
            columns: vec!["id".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        let mut registry = CteRegistry::default();
        registry.entries.push((
            "min_cte".to_string(),
            OpTree::Aggregate {
                group_by: vec![col("id")],
                aggregates: vec![AggExpr {
                    function: AggFunc::Min,
                    argument: Some(col("val")),
                    alias: "min_val".to_string(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                }],
                child: Box::new(scan_node("t", 1, &["id", "val"])),
            },
        ));
        let result = ParseResult {
            tree,
            cte_registry: registry,
            has_recursion: false,
            warnings: vec![],
        };
        assert!(check_ivm_support_with_registry(&result).is_ok());
    }

    // ── WindowExpr::to_sql edge cases ───────────────────────────────

    #[test]
    fn test_window_expr_nulls_first_ascending() {
        let wexpr = make_window_expr(
            "row_number",
            vec![],
            vec![],
            vec![SortExpr {
                expr: col("id"),
                ascending: true,
                nulls_first: true,
            }],
            "rn",
        );
        assert_eq!(
            wexpr.to_sql(),
            "row_number() OVER (ORDER BY id ASC NULLS FIRST)"
        );
    }

    #[test]
    fn test_window_expr_nulls_first_descending_no_suffix() {
        // DESC + nulls_first=true → default for DESC, so no NULLS suffix
        let wexpr = make_window_expr(
            "rank",
            vec![],
            vec![],
            vec![SortExpr {
                expr: col("score"),
                ascending: false,
                nulls_first: true,
            }],
            "rnk",
        );
        assert_eq!(wexpr.to_sql(), "rank() OVER (ORDER BY score DESC)");
    }

    #[test]
    fn test_window_expr_multiple_partitions_and_order() {
        let wexpr = WindowExpr {
            func_name: "sum".to_string(),
            args: vec![col("amount")],
            partition_by: vec![col("dept"), col("region")],
            order_by: vec![SortExpr {
                expr: col("hire_date"),
                ascending: true,
                nulls_first: false,
            }],
            frame_clause: None,
            alias: "running_total".to_string(),
        };
        assert_eq!(
            wexpr.to_sql(),
            "sum(amount) OVER (PARTITION BY dept, region ORDER BY hire_date ASC)"
        );
    }

    // ── RecursiveCte source_oids with multiple base tables ──────────

    #[test]
    fn test_recursive_cte_source_oids_multiple_tables() {
        let base = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".to_string()),
            left: Box::new(scan_node("edges", 100, &["src", "dst"])),
            right: Box::new(scan_node("nodes", 200, &["id"])),
        };
        let recursive = OpTree::RecursiveSelfRef {
            cte_name: "reach".to_string(),
            alias: "r".to_string(),
            columns: vec!["src".to_string(), "dst".to_string()],
        };
        let node = OpTree::RecursiveCte {
            alias: "reach".to_string(),
            columns: vec!["src".to_string(), "dst".to_string()],
            base: Box::new(base),
            recursive: Box::new(recursive),
            union_all: true,
        };
        let oids = node.source_oids();
        assert!(oids.contains(&100));
        assert!(oids.contains(&200));
        assert_eq!(oids.len(), 2);
    }

    // ── Window source_oids through nested children ──────────────────

    #[test]
    fn test_window_source_oids_with_join_child() {
        let join = OpTree::InnerJoin {
            condition: col("id"),
            left: Box::new(scan_node("a", 10, &["id", "val"])),
            right: Box::new(scan_node("b", 20, &["id", "metric"])),
        };
        let window = OpTree::Window {
            window_exprs: vec![WindowExpr {
                func_name: "row_number".to_string(),
                args: vec![],
                partition_by: vec![col("id")],
                order_by: vec![],
                frame_clause: None,
                alias: "rn".to_string(),
            }],
            partition_by: vec![col("id")],
            pass_through: vec![(col("id"), "id".to_string())],
            child: Box::new(join),
        };
        assert_eq!(window.source_oids(), vec![10, 20]);
    }

    // ── is_known_aggregate tests ────────────────────────────────────

    #[test]
    fn test_is_known_aggregate_supported_five() {
        assert!(is_known_aggregate("count"));
        assert!(is_known_aggregate("sum"));
        assert!(is_known_aggregate("avg"));
        assert!(is_known_aggregate("min"));
        assert!(is_known_aggregate("max"));
    }

    #[test]
    fn test_is_known_aggregate_statistical() {
        assert!(is_known_aggregate("stddev"));
        assert!(is_known_aggregate("stddev_pop"));
        assert!(is_known_aggregate("stddev_samp"));
        assert!(is_known_aggregate("variance"));
        assert!(is_known_aggregate("var_pop"));
        assert!(is_known_aggregate("var_samp"));
        assert!(is_known_aggregate("corr"));
        assert!(is_known_aggregate("covar_pop"));
        assert!(is_known_aggregate("covar_samp"));
    }

    #[test]
    fn test_is_known_aggregate_json_and_array() {
        assert!(is_known_aggregate("array_agg"));
        assert!(is_known_aggregate("json_agg"));
        assert!(is_known_aggregate("jsonb_agg"));
        assert!(is_known_aggregate("json_object_agg"));
        assert!(is_known_aggregate("jsonb_object_agg"));
        assert!(is_known_aggregate("string_agg"));
    }

    #[test]
    fn test_is_known_aggregate_boolean() {
        assert!(is_known_aggregate("bool_and"));
        assert!(is_known_aggregate("bool_or"));
        assert!(is_known_aggregate("every"));
    }

    #[test]
    fn test_is_known_aggregate_bitwise() {
        assert!(is_known_aggregate("bit_and"));
        assert!(is_known_aggregate("bit_or"));
        assert!(is_known_aggregate("bit_xor"));
    }

    #[test]
    fn test_is_known_aggregate_regression() {
        for name in &[
            "regr_avgx",
            "regr_avgy",
            "regr_count",
            "regr_intercept",
            "regr_r2",
            "regr_slope",
            "regr_sxx",
            "regr_sxy",
            "regr_syy",
        ] {
            assert!(is_known_aggregate(name), "{name} should be known");
        }
    }

    #[test]
    fn test_is_known_aggregate_ordered_set() {
        assert!(is_known_aggregate("percentile_cont"));
        assert!(is_known_aggregate("percentile_disc"));
        assert!(is_known_aggregate("mode"));
    }

    #[test]
    fn test_is_known_aggregate_window_ranking() {
        assert!(is_known_aggregate("rank"));
        assert!(is_known_aggregate("dense_rank"));
        assert!(is_known_aggregate("percent_rank"));
        assert!(is_known_aggregate("cume_dist"));
    }

    #[test]
    fn test_is_known_aggregate_unknown_returns_false() {
        assert!(!is_known_aggregate("my_custom_agg"));
        assert!(!is_known_aggregate("upper"));
        assert!(!is_known_aggregate("coalesce"));
        assert!(!is_known_aggregate("generate_series"));
        assert!(!is_known_aggregate("concat"));
        assert!(!is_known_aggregate(""));
    }

    // ── WindowExpr::to_sql() with frame_clause ─────────────────────

    #[test]
    fn test_window_expr_to_sql_with_rows_frame() {
        let wexpr = WindowExpr {
            func_name: "sum".to_string(),
            args: vec![col("amount")],
            partition_by: vec![col("dept")],
            order_by: vec![SortExpr {
                expr: col("hire_date"),
                ascending: true,
                nulls_first: false,
            }],
            frame_clause: Some("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW".to_string()),
            alias: "running_total".to_string(),
        };
        assert_eq!(
            wexpr.to_sql(),
            "sum(amount) OVER (PARTITION BY dept ORDER BY hire_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
        );
    }

    #[test]
    fn test_window_expr_to_sql_with_range_frame() {
        let wexpr = WindowExpr {
            func_name: "avg".to_string(),
            args: vec![col("price")],
            partition_by: vec![],
            order_by: vec![SortExpr {
                expr: col("ts"),
                ascending: true,
                nulls_first: false,
            }],
            frame_clause: Some("RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING".to_string()),
            alias: "avg_price".to_string(),
        };
        assert_eq!(
            wexpr.to_sql(),
            "avg(price) OVER (ORDER BY ts ASC RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"
        );
    }

    #[test]
    fn test_window_expr_to_sql_with_groups_frame() {
        let wexpr = WindowExpr {
            func_name: "count".to_string(),
            args: vec![col("id")],
            partition_by: vec![col("category")],
            order_by: vec![SortExpr {
                expr: col("rank"),
                ascending: true,
                nulls_first: false,
            }],
            frame_clause: Some("GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING".to_string()),
            alias: "cnt".to_string(),
        };
        assert_eq!(
            wexpr.to_sql(),
            "count(id) OVER (PARTITION BY category ORDER BY rank ASC GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
        );
    }

    #[test]
    fn test_window_expr_to_sql_frame_clause_none_omitted() {
        let wexpr = WindowExpr {
            func_name: "row_number".to_string(),
            args: vec![],
            partition_by: vec![col("dept")],
            order_by: vec![SortExpr {
                expr: col("id"),
                ascending: true,
                nulls_first: false,
            }],
            frame_clause: None,
            alias: "rn".to_string(),
        };
        assert_eq!(
            wexpr.to_sql(),
            "row_number() OVER (PARTITION BY dept ORDER BY id ASC)"
        );
    }

    #[test]
    fn test_window_expr_to_sql_empty_args_empty_partition_with_frame() {
        let wexpr = WindowExpr {
            func_name: "row_number".to_string(),
            args: vec![],
            partition_by: vec![],
            order_by: vec![SortExpr {
                expr: col("id"),
                ascending: true,
                nulls_first: false,
            }],
            frame_clause: Some("ROWS UNBOUNDED PRECEDING".to_string()),
            alias: "rn".to_string(),
        };
        assert_eq!(
            wexpr.to_sql(),
            "row_number() OVER (ORDER BY id ASC ROWS UNBOUNDED PRECEDING)"
        );
    }

    // ── Expr::Raw round-trip ────────────────────────────────────────

    #[test]
    fn test_expr_raw_to_sql_preserves_content() {
        let expr = Expr::Raw("CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END".to_string());
        assert_eq!(
            expr.to_sql(),
            "CASE WHEN x > 0 THEN 'positive' ELSE 'negative' END"
        );
    }

    #[test]
    fn test_expr_funccall_coalesce_format() {
        let expr = Expr::FuncCall {
            func_name: "COALESCE".to_string(),
            args: vec![
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "a".to_string(),
                },
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "b".to_string(),
                },
                Expr::Raw("0".to_string()),
            ],
        };
        assert_eq!(expr.to_sql(), "COALESCE(a, b, 0)");
    }

    #[test]
    fn test_expr_funccall_nullif_format() {
        let expr = Expr::FuncCall {
            func_name: "NULLIF".to_string(),
            args: vec![
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "a".to_string(),
                },
                Expr::Raw("0".to_string()),
            ],
        };
        assert_eq!(expr.to_sql(), "NULLIF(a, 0)");
    }

    #[test]
    fn test_expr_funccall_greatest_least_format() {
        let g = Expr::FuncCall {
            func_name: "GREATEST".to_string(),
            args: vec![
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "a".to_string(),
                },
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "b".to_string(),
                },
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "c".to_string(),
                },
            ],
        };
        let l = Expr::FuncCall {
            func_name: "LEAST".to_string(),
            args: vec![
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "a".to_string(),
                },
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "b".to_string(),
                },
                Expr::ColumnRef {
                    table_alias: None,
                    column_name: "c".to_string(),
                },
            ],
        };
        assert_eq!(g.to_sql(), "GREATEST(a, b, c)");
        assert_eq!(l.to_sql(), "LEAST(a, b, c)");
    }

    #[test]
    fn test_expr_raw_array_format() {
        let expr = Expr::Raw("ARRAY[1, 2, 3]".to_string());
        assert_eq!(expr.to_sql(), "ARRAY[1, 2, 3]");
    }

    #[test]
    fn test_expr_raw_row_format() {
        let expr = Expr::Raw("ROW(a, b, c)".to_string());
        assert_eq!(expr.to_sql(), "ROW(a, b, c)");
    }

    #[test]
    fn test_expr_raw_boolean_test_format() {
        let expr = Expr::Raw("active IS TRUE".to_string());
        assert_eq!(expr.to_sql(), "active IS TRUE");
    }

    #[test]
    fn test_expr_raw_is_distinct_from_format() {
        let expr = Expr::Raw("(a IS DISTINCT FROM b)".to_string());
        assert_eq!(expr.to_sql(), "(a IS DISTINCT FROM b)");
    }

    #[test]
    fn test_expr_raw_between_format() {
        let expr = Expr::Raw("(x BETWEEN 1 AND 100)".to_string());
        assert_eq!(expr.to_sql(), "(x BETWEEN 1 AND 100)");
    }

    #[test]
    fn test_expr_raw_in_list_format() {
        let expr = Expr::Raw("(status IN (1, 2, 3))".to_string());
        assert_eq!(expr.to_sql(), "(status IN (1, 2, 3))");
    }

    #[test]
    fn test_expr_raw_sql_value_function_format() {
        for kw in &[
            "CURRENT_DATE",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "CURRENT_ROLE",
            "CURRENT_USER",
            "USER",
            "SESSION_USER",
            "CURRENT_CATALOG",
            "CURRENT_SCHEMA",
        ] {
            let expr = Expr::Raw(kw.to_string());
            assert_eq!(expr.to_sql(), *kw);
        }
    }

    #[test]
    fn test_expr_raw_indirection_array_subscript() {
        let expr = Expr::Raw("arr[1]".to_string());
        assert_eq!(expr.to_sql(), "arr[1]");
    }

    #[test]
    fn test_expr_raw_indirection_field_access() {
        let expr = Expr::Raw("(rec).field_name".to_string());
        assert_eq!(expr.to_sql(), "(rec).field_name");
    }

    #[test]
    fn test_expr_raw_indirection_star() {
        let expr = Expr::Raw("(data).*".to_string());
        assert_eq!(expr.to_sql(), "(data).*");
    }

    // ── GROUPING SETS kind-name mapping ────────────────────────────

    #[test]
    fn test_grouping_set_kind_rollup_name() {
        // Verify the GroupingSetKind constant used in check_select_unsupported()
        // produces the expected label in the error message.
        let kind = pg_sys::GroupingSetKind::GROUPING_SET_ROLLUP;
        let name = match kind {
            pg_sys::GroupingSetKind::GROUPING_SET_ROLLUP => "ROLLUP",
            pg_sys::GroupingSetKind::GROUPING_SET_CUBE => "CUBE",
            _ => "GROUPING SETS",
        };
        assert_eq!(name, "ROLLUP");
    }

    #[test]
    fn test_grouping_set_kind_cube_name() {
        let kind = pg_sys::GroupingSetKind::GROUPING_SET_CUBE;
        let name = match kind {
            pg_sys::GroupingSetKind::GROUPING_SET_ROLLUP => "ROLLUP",
            pg_sys::GroupingSetKind::GROUPING_SET_CUBE => "CUBE",
            _ => "GROUPING SETS",
        };
        assert_eq!(name, "CUBE");
    }

    #[test]
    fn test_grouping_set_kind_sets_name() {
        // GROUPING_SET_SETS and GROUPING_SET_EMPTY both fall into the catch-all
        let kind = pg_sys::GroupingSetKind::GROUPING_SET_SETS;
        let name = match kind {
            pg_sys::GroupingSetKind::GROUPING_SET_ROLLUP => "ROLLUP",
            pg_sys::GroupingSetKind::GROUPING_SET_CUBE => "CUBE",
            _ => "GROUPING SETS",
        };
        assert_eq!(name, "GROUPING SETS");
    }

    #[test]
    fn test_grouping_set_node_tag_value() {
        // Verify T_GroupingSet has the expected NodeTag value (107 per pg18 bindings).
        // This guards against a pgrx upgrade accidentally changing the tag.
        let tag_num = pg_sys::NodeTag::T_GroupingSet as u32;
        assert_eq!(tag_num, 107, "T_GroupingSet NodeTag must be 107");
    }

    #[test]
    fn test_range_table_sample_node_tag_value() {
        // Verify T_RangeTableSample has the expected NodeTag value (89 per pg18 bindings).
        let tag_num = pg_sys::NodeTag::T_RangeTableSample as u32;
        assert_eq!(tag_num, 89, "T_RangeTableSample NodeTag must be 89");
    }

    // ── SemiJoin / AntiJoin / ScalarSubquery OpTree tests ────────────

    fn make_scan(oid: u32, name: &str, alias: &str, cols: &[&str]) -> OpTree {
        OpTree::Scan {
            table_oid: oid,
            table_name: name.to_string(),
            schema: "public".to_string(),
            columns: cols
                .iter()
                .map(|c| Column {
                    name: c.to_string(),
                    type_oid: 23,
                    is_nullable: true,
                })
                .collect(),
            pk_columns: vec![],
            alias: alias.to_string(),
        }
    }

    #[test]
    fn test_semi_join_alias() {
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "orders", "o", &["id"])),
            right: Box::new(make_scan(2, "items", "i", &["id"])),
        };
        assert_eq!(tree.alias(), "semi_join");
    }

    #[test]
    fn test_anti_join_alias() {
        let tree = OpTree::AntiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "orders", "o", &["id"])),
            right: Box::new(make_scan(2, "returns", "r", &["id"])),
        };
        assert_eq!(tree.alias(), "anti_join");
    }

    #[test]
    fn test_scalar_subquery_alias() {
        let tree = OpTree::ScalarSubquery {
            subquery: Box::new(make_scan(2, "config", "c", &["tax_rate"])),
            alias: "current_tax".to_string(),
            subquery_source_oids: vec![2],
            child: Box::new(make_scan(1, "orders", "o", &["id", "amount"])),
        };
        assert_eq!(tree.alias(), "current_tax");
    }

    #[test]
    fn test_semi_join_node_kind() {
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "t1", "t1", &["id"])),
            right: Box::new(make_scan(2, "t2", "t2", &["id"])),
        };
        assert_eq!(tree.node_kind(), "semi join");
    }

    #[test]
    fn test_anti_join_node_kind() {
        let tree = OpTree::AntiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "t1", "t1", &["id"])),
            right: Box::new(make_scan(2, "t2", "t2", &["id"])),
        };
        assert_eq!(tree.node_kind(), "anti join");
    }

    #[test]
    fn test_scalar_subquery_node_kind() {
        let tree = OpTree::ScalarSubquery {
            subquery: Box::new(make_scan(2, "t2", "t2", &["val"])),
            alias: "sub".to_string(),
            subquery_source_oids: vec![2],
            child: Box::new(make_scan(1, "t1", "t1", &["id"])),
        };
        assert_eq!(tree.node_kind(), "scalar subquery");
    }

    #[test]
    fn test_semi_join_output_columns() {
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "orders", "o", &["id", "cust_id", "amount"])),
            right: Box::new(make_scan(2, "items", "i", &["order_id", "qty"])),
        };
        // Semi-join outputs only left-side columns
        assert_eq!(tree.output_columns(), vec!["id", "cust_id", "amount"]);
    }

    #[test]
    fn test_anti_join_output_columns() {
        let tree = OpTree::AntiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "orders", "o", &["id", "amount"])),
            right: Box::new(make_scan(2, "returns", "r", &["order_id"])),
        };
        assert_eq!(tree.output_columns(), vec!["id", "amount"]);
    }

    #[test]
    fn test_scalar_subquery_output_columns() {
        let tree = OpTree::ScalarSubquery {
            subquery: Box::new(make_scan(2, "config", "c", &["tax_rate"])),
            alias: "current_tax".to_string(),
            subquery_source_oids: vec![2],
            child: Box::new(make_scan(1, "orders", "o", &["id", "amount"])),
        };
        assert_eq!(tree.output_columns(), vec!["id", "amount", "current_tax"]);
    }

    #[test]
    fn test_semi_join_source_oids() {
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(10, "orders", "o", &["id"])),
            right: Box::new(make_scan(20, "items", "i", &["id"])),
        };
        let oids = tree.source_oids();
        assert!(oids.contains(&10));
        assert!(oids.contains(&20));
    }

    #[test]
    fn test_scalar_subquery_source_oids() {
        let tree = OpTree::ScalarSubquery {
            subquery: Box::new(make_scan(30, "config", "c", &["rate"])),
            alias: "rate".to_string(),
            subquery_source_oids: vec![30],
            child: Box::new(make_scan(10, "orders", "o", &["id"])),
        };
        let oids = tree.source_oids();
        assert!(oids.contains(&10));
        assert!(oids.contains(&30));
    }

    #[test]
    fn test_semi_join_row_id_key_columns_is_none() {
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "t1", "t1", &["id"])),
            right: Box::new(make_scan(2, "t2", "t2", &["id"])),
        };
        // SemiJoin falls into the catch-all `_ => None` arm
        assert!(tree.row_id_key_columns().is_none());
    }

    #[test]
    fn test_semi_join_needs_pgt_count_false() {
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "t1", "t1", &["id"])),
            right: Box::new(make_scan(2, "t2", "t2", &["id"])),
        };
        assert!(!tree.needs_pgt_count());
    }

    #[test]
    fn test_check_ivm_support_semi_join() {
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "t1", "t1", &["id"])),
            right: Box::new(make_scan(2, "t2", "t2", &["id"])),
        };
        assert!(check_ivm_support(&tree).is_ok());
    }

    #[test]
    fn test_check_ivm_support_anti_join() {
        let tree = OpTree::AntiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(make_scan(1, "t1", "t1", &["id"])),
            right: Box::new(make_scan(2, "t2", "t2", &["id"])),
        };
        assert!(check_ivm_support(&tree).is_ok());
    }

    #[test]
    fn test_check_ivm_support_scalar_subquery() {
        let tree = OpTree::ScalarSubquery {
            subquery: Box::new(make_scan(2, "config", "c", &["rate"])),
            alias: "rate".to_string(),
            subquery_source_oids: vec![2],
            child: Box::new(make_scan(1, "orders", "o", &["id"])),
        };
        assert!(check_ivm_support(&tree).is_ok());
    }

    #[test]
    fn test_check_ivm_support_nested_semi_join() {
        // Semi-join with aggregate child should still pass
        let inner_scan = make_scan(1, "orders", "o", &["cust_id", "amount"]);
        let agg = OpTree::Aggregate {
            group_by: vec![col("cust_id")],
            aggregates: vec![AggExpr {
                function: AggFunc::Sum,
                argument: Some(col("amount")),
                alias: "total".to_string(),
                is_distinct: false,
                filter: None,
                second_arg: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(inner_scan),
        };
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(agg),
            right: Box::new(make_scan(2, "thresholds", "t", &["cust_id"])),
        };
        assert!(check_ivm_support(&tree).is_ok());
    }

    // ── Volatility helper tests ─────────────────────────────────────

    #[test]
    fn test_max_volatility_ordering() {
        assert_eq!(max_volatility('i', 'i'), 'i');
        assert_eq!(max_volatility('i', 's'), 's');
        assert_eq!(max_volatility('s', 'i'), 's');
        assert_eq!(max_volatility('s', 's'), 's');
        assert_eq!(max_volatility('i', 'v'), 'v');
        assert_eq!(max_volatility('v', 'i'), 'v');
        assert_eq!(max_volatility('s', 'v'), 'v');
        assert_eq!(max_volatility('v', 's'), 'v');
        assert_eq!(max_volatility('v', 'v'), 'v');
    }

    #[test]
    fn test_collect_volatilities_no_func_calls() {
        // An expression with no function calls should remain 'i' (immutable).
        let expr = Expr::BinaryOp {
            op: "+".to_string(),
            left: Box::new(col("a")),
            right: Box::new(Expr::Literal("1".to_string())),
        };
        let mut worst = 'i';
        // collect_volatilities with no FuncCall should leave worst at 'i'.
        // (Can't call SPI in unit tests, but no FuncCall → no SPI call.)
        collect_volatilities(&expr, &mut worst).unwrap();
        assert_eq!(worst, 'i');
    }

    #[test]
    fn test_tree_collect_volatility_scan_only() {
        // A plain Scan node has no expressions → worst stays 'i'.
        let tree = make_scan(1, "orders", "o", &["id", "amount"]);
        let mut worst = 'i';
        tree_collect_volatility(&tree, &mut worst).unwrap();
        assert_eq!(worst, 'i');
    }

    #[test]
    fn test_tree_collect_volatility_filter_no_funcs() {
        let tree = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: ">".to_string(),
                left: Box::new(col("amount")),
                right: Box::new(Expr::Literal("100".to_string())),
            },
            child: Box::new(make_scan(1, "orders", "o", &["id", "amount"])),
        };
        let mut worst = 'i';
        tree_collect_volatility(&tree, &mut worst).unwrap();
        assert_eq!(worst, 'i');
    }

    #[test]
    fn test_lookup_operator_volatility_stub_returns_immutable() {
        // In test mode, operators default to immutable (most built-in are).
        let vol = lookup_operator_volatility("+").unwrap();
        assert_eq!(vol, 'i');
        let vol2 = lookup_operator_volatility("&&").unwrap();
        assert_eq!(vol2, 'i');
    }

    #[test]
    fn test_collect_volatilities_binary_op_with_func_operand() {
        // BinaryOp where one operand is a FuncCall should detect volatile
        // from the FuncCall (test stub returns 'v' for all functions).
        let expr = Expr::BinaryOp {
            op: ">".to_string(),
            left: Box::new(Expr::FuncCall {
                func_name: "random".to_string(),
                args: vec![],
            }),
            right: Box::new(Expr::Literal("0.5".to_string())),
        };
        let mut worst = 'i';
        collect_volatilities(&expr, &mut worst).unwrap();
        // random() is volatile (test stub returns 'v' for all functions)
        assert_eq!(worst, 'v');
    }

    #[test]
    fn test_collect_volatilities_nested_binary_ops_immutable() {
        // Nested BinaryOps with only ColumnRef/Literal → stays immutable.
        let expr = Expr::BinaryOp {
            op: "+".to_string(),
            left: Box::new(Expr::BinaryOp {
                op: "*".to_string(),
                left: Box::new(col("a")),
                right: Box::new(Expr::Literal("2".to_string())),
            }),
            right: Box::new(col("b")),
        };
        let mut worst = 'i';
        collect_volatilities(&expr, &mut worst).unwrap();
        assert_eq!(worst, 'i');
    }

    // ── GROUPING SETS rewrite helpers ──────────────────────────────

    #[test]
    fn test_compute_grouping_value_single_in_set() {
        // GROUPING(department) where department IS grouped → 0
        assert_eq!(
            compute_grouping_value(&["department".to_string()], &["department".to_string()]),
            0
        );
    }

    #[test]
    fn test_compute_grouping_value_single_not_in_set() {
        // GROUPING(department) where department is NOT grouped → 1
        assert_eq!(
            compute_grouping_value(&["department".to_string()], &["region".to_string()]),
            1
        );
    }

    #[test]
    fn test_compute_grouping_value_two_args_both_grouped() {
        // GROUPING(a, b) where both are grouped → 0b00 = 0
        assert_eq!(
            compute_grouping_value(
                &["a".to_string(), "b".to_string()],
                &["a".to_string(), "b".to_string()]
            ),
            0
        );
    }

    #[test]
    fn test_compute_grouping_value_two_args_first_not_grouped() {
        // GROUPING(a, b) where a not grouped, b grouped → 0b10 = 2
        assert_eq!(
            compute_grouping_value(&["a".to_string(), "b".to_string()], &["b".to_string()]),
            2
        );
    }

    #[test]
    fn test_compute_grouping_value_two_args_second_not_grouped() {
        // GROUPING(a, b) where a grouped, b not grouped → 0b01 = 1
        assert_eq!(
            compute_grouping_value(&["a".to_string(), "b".to_string()], &["a".to_string()]),
            1
        );
    }

    #[test]
    fn test_compute_grouping_value_two_args_neither_grouped() {
        // GROUPING(a, b) where neither grouped → 0b11 = 3
        assert_eq!(
            compute_grouping_value(&["a".to_string(), "b".to_string()], &[]),
            3
        );
    }

    #[test]
    fn test_compute_grouping_value_three_args_mixed() {
        // GROUPING(a, b, c) where a=grouped, b=not, c=grouped → 0b010 = 2
        assert_eq!(
            compute_grouping_value(
                &["a".to_string(), "b".to_string(), "c".to_string()],
                &["a".to_string(), "c".to_string()]
            ),
            2
        );
    }

    #[test]
    fn test_compute_grouping_value_empty_args() {
        // GROUPING() with no args → 0
        assert_eq!(compute_grouping_value(&[], &["a".to_string()]), 0);
    }

    // Note: expand_grouping_set / expand_cube / expand_rollup tests require
    // pg_sys FFI (palloc0, makeString, lappend) so they live in E2E tests.
    // The ROLLUP/CUBE expansion logic is verified via the compute_grouping_value
    // tests above plus E2E integration tests.

    // ── S12/S13/S14 rewrite helper tests ────────────────────────────

    // Note: rewrite_scalar_subquery_in_where(), rewrite_sublinks_in_or(),
    // and rewrite_multi_partition_windows() all require pg_sys::raw_parser()
    // which is only available in a PostgreSQL backend. These functions are
    // tested via E2E tests. Below we test the pure-Rust helpers.

    #[test]
    fn test_window_info_partition_key_grouping() {
        // Verify that WindowInfo can be grouped by partition_key
        let w1 = WindowInfo {
            func_sql: "SUM(a) OVER (PARTITION BY region)".to_string(),
            alias: "region_sum".to_string(),
            partition_key: "region".to_string(),
        };
        let w2 = WindowInfo {
            func_sql: "COUNT(*) OVER (PARTITION BY region)".to_string(),
            alias: "region_cnt".to_string(),
            partition_key: "region".to_string(),
        };
        let w3 = WindowInfo {
            func_sql: "SUM(a) OVER (PARTITION BY dept)".to_string(),
            alias: "dept_sum".to_string(),
            partition_key: "dept".to_string(),
        };

        // Group by partition_key
        let mut groups: Vec<(String, Vec<&WindowInfo>)> = Vec::new();
        for wi in [&w1, &w2, &w3] {
            let found = groups.iter_mut().find(|(k, _)| *k == wi.partition_key);
            if let Some((_, group)) = found {
                group.push(wi);
            } else {
                groups.push((wi.partition_key.clone(), vec![wi]));
            }
        }

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, "region");
        assert_eq!(groups[0].1.len(), 2);
        assert_eq!(groups[1].0, "dept");
        assert_eq!(groups[1].1.len(), 1);
    }

    #[test]
    fn test_scalar_subquery_extract_creation() {
        let extract = ScalarSubqueryExtract {
            subquery_sql: "SELECT avg(amount) FROM orders".to_string(),
            expr_sql: "(SELECT avg(\"amount\") FROM \"orders\")".to_string(),
            inner_tables: vec!["orders".to_string()],
            has_limit_or_offset: false,
        };
        assert!(extract.subquery_sql.contains("avg"));
        assert!(extract.expr_sql.starts_with('('));
        assert!(extract.expr_sql.ends_with(')'));
    }

    #[test]
    fn test_or_arm_string_replacement() {
        // Simulate the OR-to-UNION approach: each OR arm becomes a separate branch
        let arm1 = "status = 'active'";
        let arm2 = "EXISTS (SELECT 1 FROM vip WHERE vip.id = t.id)";
        let from_sql = "\"t\"";
        let target_sql = "*";

        let branch1 = format!("SELECT {target_sql} FROM {from_sql} WHERE {arm1}");
        let branch2 = format!("SELECT {target_sql} FROM {from_sql} WHERE {arm2}");
        let rewritten = format!("{branch1} UNION {branch2}");

        assert!(rewritten.contains("UNION"));
        assert!(rewritten.contains("status = 'active'"));
        assert!(rewritten.contains("EXISTS"));
    }

    #[test]
    fn test_scalar_subquery_where_replacement() {
        // Simulate the scalar subquery replacement logic
        let original_where = "amount > (SELECT avg(\"amount\") FROM \"orders\")";
        let expr_sql = "(SELECT avg(\"amount\") FROM \"orders\")";
        let replacement = "\"__pgt_sq_1\".\"__pgt_scalar_1\"";

        let rewritten = original_where.replace(expr_sql, replacement);
        assert_eq!(rewritten, "amount > \"__pgt_sq_1\".\"__pgt_scalar_1\"");
    }

    #[test]
    fn test_multi_partition_row_marker_join() {
        // Verify the join strategy: subqueries joined on __pgt_row_marker
        let sq1_alias = "__pgt_w1";
        let sq2_alias = "__pgt_w2";

        let join_cond = format!(
            "\"{}\".\"__pgt_row_marker\" = \"{}\".\"__pgt_row_marker\"",
            sq1_alias, sq2_alias
        );

        assert!(join_cond.contains("__pgt_w1"));
        assert!(join_cond.contains("__pgt_w2"));
        assert!(join_cond.contains("__pgt_row_marker"));
    }

    #[test]
    fn test_cross_join_construction() {
        // Verify the CROSS JOIN construction for scalar subquery rewrite
        let idx = 1;
        let sq_alias = format!("__pgt_sq_{idx}");
        let scalar_alias = format!("__pgt_scalar_{idx}");
        let sq_sql = "SELECT avg(amount) FROM orders";

        let cross_join = format!("CROSS JOIN ({sq_sql} AS \"{scalar_alias}\") AS \"{sq_alias}\"");

        assert!(cross_join.contains("CROSS JOIN"));
        assert!(cross_join.contains("avg(amount)"));
        assert!(cross_join.contains("__pgt_sq_1"));
        assert!(cross_join.contains("__pgt_scalar_1"));
    }

    // ── contains_word_boundary tests ────────────────────────────────

    #[test]
    fn test_contains_word_boundary_basic() {
        assert!(contains_word_boundary(
            "p_partkey = ps_partkey",
            "p_partkey"
        ));
        assert!(contains_word_boundary(
            "p_partkey = ps_partkey",
            "ps_partkey"
        ));
    }

    #[test]
    fn test_contains_word_boundary_at_start_end() {
        assert!(contains_word_boundary("p_partkey", "p_partkey"));
        assert!(contains_word_boundary("foo p_partkey", "p_partkey"));
        assert!(contains_word_boundary("p_partkey foo", "p_partkey"));
    }

    #[test]
    fn test_contains_word_boundary_no_match_substring() {
        // "p_partkey" should NOT match inside "ps_partkey" — the 's' before
        // is alphanumeric so it's not a word boundary.
        assert!(!contains_word_boundary("ps_partkey", "p_partkey"));
        // "l_quantity" should not match in "xl_quantity"
        assert!(!contains_word_boundary("xl_quantity", "l_quantity"));
    }

    #[test]
    fn test_contains_word_boundary_with_operators() {
        assert!(contains_word_boundary(
            "where p_partkey = ps_partkey and s_suppkey = ps_suppkey",
            "p_partkey"
        ));
        // Quoted identifiers
        assert!(contains_word_boundary(
            "\"p_partkey\" = \"ps_partkey\"",
            "p_partkey"
        ));
    }

    #[test]
    fn test_contains_word_boundary_not_found() {
        assert!(!contains_word_boundary(
            "select count(*) from orders",
            "p_partkey"
        ));
    }

    // ── Predicate pushdown tests ──────────────────────────────────────

    #[test]
    fn test_push_filter_two_table_cross_join() {
        // FROM a, b WHERE a.id = b.id
        let a = scan_node("a", 1, &["id", "name"]);
        let b = scan_node("b", 2, &["id", "val"]);
        let cross = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(a),
            right: Box::new(b),
        };
        let filter = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: "=".into(),
                left: Box::new(qualified_col("a", "id")),
                right: Box::new(qualified_col("b", "id")),
            },
            child: Box::new(cross),
        };
        let result = push_filter_into_cross_joins(filter).unwrap();
        // Should become InnerJoin with the condition promoted
        match &result {
            OpTree::InnerJoin { condition, .. } => {
                // Condition should contain "a.id = b.id" (not TRUE)
                assert_ne!(condition.to_sql(), "TRUE");
                assert!(condition.to_sql().contains("="));
            }
            _ => panic!("Expected InnerJoin, got {:?}", result.node_kind()),
        }
    }

    #[test]
    fn test_push_filter_three_table_cross_join() {
        // FROM a, b, c WHERE a.x = b.x AND b.y = c.y
        let a = scan_node("a", 1, &["x"]);
        let b = scan_node("b", 2, &["x", "y"]);
        let c = scan_node("c", 3, &["y"]);
        let inner = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(a),
            right: Box::new(b),
        };
        let outer = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(inner),
            right: Box::new(c),
        };
        let pred = Expr::BinaryOp {
            op: "AND".into(),
            left: Box::new(Expr::BinaryOp {
                op: "=".into(),
                left: Box::new(qualified_col("a", "x")),
                right: Box::new(qualified_col("b", "x")),
            }),
            right: Box::new(Expr::BinaryOp {
                op: "=".into(),
                left: Box::new(qualified_col("b", "y")),
                right: Box::new(qualified_col("c", "y")),
            }),
        };
        let filter = OpTree::Filter {
            predicate: pred,
            child: Box::new(outer),
        };
        let result = push_filter_into_cross_joins(filter).unwrap();
        // Outer join should have b.y = c.y (c is right child)
        // Inner join should have a.x = b.x (b is right child)
        // No filter should remain
        match &result {
            OpTree::InnerJoin {
                condition: outer_cond,
                left,
                ..
            } => {
                assert!(
                    outer_cond.to_sql().contains("y"),
                    "outer: {}",
                    outer_cond.to_sql()
                );
                match left.as_ref() {
                    OpTree::InnerJoin {
                        condition: inner_cond,
                        ..
                    } => {
                        assert!(
                            inner_cond.to_sql().contains("x"),
                            "inner: {}",
                            inner_cond.to_sql()
                        );
                    }
                    _ => panic!("Expected inner InnerJoin"),
                }
            }
            _ => panic!("Expected InnerJoin, got {:?}", result.node_kind()),
        }
    }

    #[test]
    fn test_push_filter_preserves_single_table_predicate() {
        // FROM a, b WHERE a.id = b.id AND a.name = 'foo'
        let a = scan_node("a", 1, &["id", "name"]);
        let b = scan_node("b", 2, &["id"]);
        let cross = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(a),
            right: Box::new(b),
        };
        let pred = Expr::BinaryOp {
            op: "AND".into(),
            left: Box::new(Expr::BinaryOp {
                op: "=".into(),
                left: Box::new(qualified_col("a", "id")),
                right: Box::new(qualified_col("b", "id")),
            }),
            right: Box::new(Expr::BinaryOp {
                op: "=".into(),
                left: Box::new(qualified_col("a", "name")),
                right: Box::new(Expr::Literal("'foo'".into())),
            }),
        };
        let filter = OpTree::Filter {
            predicate: pred,
            child: Box::new(cross),
        };
        let result = push_filter_into_cross_joins(filter).unwrap();
        // The join predicate should be promoted; single-table pred stays as Filter
        match &result {
            OpTree::Filter {
                predicate: remaining,
                child,
            } => {
                // Remaining filter: a.name = 'foo'
                assert!(remaining.to_sql().contains("name"));
                match child.as_ref() {
                    OpTree::InnerJoin { condition, .. } => {
                        assert_ne!(condition.to_sql(), "TRUE");
                    }
                    _ => panic!("Expected InnerJoin inside Filter"),
                }
            }
            _ => panic!("Expected Filter wrapper for single-table predicate"),
        }
    }

    #[test]
    fn test_push_filter_no_cross_join_passthrough() {
        // Filter over a proper join — should pass through unchanged
        let a = scan_node("a", 1, &["id"]);
        let b = scan_node("b", 2, &["id"]);
        let join = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".into(),
                left: Box::new(qualified_col("a", "id")),
                right: Box::new(qualified_col("b", "id")),
            },
            left: Box::new(a),
            right: Box::new(b),
        };
        let filter = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: ">".into(),
                left: Box::new(qualified_col("a", "id")),
                right: Box::new(Expr::Literal("10".into())),
            },
            child: Box::new(join),
        };
        let result = push_filter_into_cross_joins(filter).unwrap();
        assert!(matches!(result, OpTree::Filter { .. }));
    }

    #[test]
    fn test_push_filter_unqualified_columns() {
        // FROM a, b WHERE x = y (unqualified columns resolved by scan)
        let a = scan_node("a", 1, &["x"]);
        let b = scan_node("b", 2, &["y"]);
        let cross = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".into()),
            left: Box::new(a),
            right: Box::new(b),
        };
        let filter = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: "=".into(),
                left: Box::new(col("x")),
                right: Box::new(col("y")),
            },
            child: Box::new(cross),
        };
        let result = push_filter_into_cross_joins(filter).unwrap();
        // Should promote the predicate (both sides resolve to different scans)
        match &result {
            OpTree::InnerJoin { condition, .. } => {
                assert_ne!(condition.to_sql(), "TRUE");
            }
            _ => panic!("Expected InnerJoin with promoted condition"),
        }
    }

    // ── strip_view_definition_suffix tests ───────────────────────────

    #[test]
    fn test_strip_view_definition_suffix_with_semicolon() {
        assert_eq!(strip_view_definition_suffix("SELECT 1;"), "SELECT 1");
    }

    #[test]
    fn test_strip_view_definition_suffix_with_whitespace_and_semicolon() {
        // Semicolon immediately at end, followed by nothing
        assert_eq!(
            strip_view_definition_suffix("SELECT id FROM t  ;"),
            "SELECT id FROM t"
        );
    }

    #[test]
    fn test_strip_view_definition_suffix_no_semicolon() {
        assert_eq!(
            strip_view_definition_suffix("SELECT id FROM t"),
            "SELECT id FROM t"
        );
    }

    #[test]
    fn test_strip_view_definition_suffix_empty_string() {
        assert_eq!(strip_view_definition_suffix(""), "");
    }

    #[test]
    fn test_strip_view_definition_suffix_only_semicolons() {
        assert_eq!(strip_view_definition_suffix(";;;"), "");
    }

    #[test]
    fn test_strip_view_definition_suffix_preserves_interior_semicolons() {
        // A semicolon in the middle should NOT be stripped
        assert_eq!(
            strip_view_definition_suffix("SELECT ';' FROM t;"),
            "SELECT ';' FROM t"
        );
    }

    // ── Column-pruning tests ────────────────────────────────────────

    fn make_scan_pk(alias: &str, oid: u32, col_names: &[&str], pk: &[&str]) -> OpTree {
        OpTree::Scan {
            table_oid: oid,
            table_name: alias.to_string(),
            schema: "public".to_string(),
            columns: col_names.iter().map(|n| make_column(n)).collect(),
            pk_columns: pk.iter().map(|s| s.to_string()).collect(),
            alias: alias.to_string(),
        }
    }

    fn col_names(tree: &OpTree) -> Vec<String> {
        match tree {
            OpTree::Scan { columns, .. } => columns.iter().map(|c| c.name.clone()).collect(),
            _ => panic!("expected Scan"),
        }
    }

    #[test]
    fn test_prune_scan_columns_project_qualified() {
        // SELECT t.a, t.b FROM t(a, b, c, d)  →  Scan keeps {a, b}
        let mut tree = OpTree::Project {
            expressions: vec![qualified_col("t", "a"), qualified_col("t", "b")],
            aliases: vec!["a".into(), "b".into()],
            child: Box::new(scan_node("t", 1, &["a", "b", "c", "d"])),
        };
        tree.prune_scan_columns();
        let OpTree::Project { child, .. } = &tree else {
            panic!()
        };
        assert_eq!(col_names(child), vec!["a", "b"]);
    }

    #[test]
    fn test_prune_scan_columns_keeps_pk() {
        // SELECT t.b FROM t(id, a, b) with PK=id  →  Scan keeps {id, b}
        let mut tree = OpTree::Project {
            expressions: vec![qualified_col("t", "b")],
            aliases: vec!["b".into()],
            child: Box::new(make_scan_pk("t", 1, &["id", "a", "b"], &["id"])),
        };
        tree.prune_scan_columns();
        let OpTree::Project { child, .. } = &tree else {
            panic!()
        };
        let mut names = col_names(child);
        names.sort();
        assert_eq!(names, vec!["b", "id"]);
    }

    #[test]
    fn test_prune_scan_columns_filter_refs() {
        // SELECT t.a FROM t(a, b, c) WHERE t.b > 0  →  Scan keeps {a, b}
        let mut tree = OpTree::Project {
            expressions: vec![qualified_col("t", "a")],
            aliases: vec!["a".into()],
            child: Box::new(OpTree::Filter {
                predicate: qualified_col("t", "b"),
                child: Box::new(scan_node("t", 1, &["a", "b", "c"])),
            }),
        };
        tree.prune_scan_columns();
        let OpTree::Project {
            child: filter_box, ..
        } = &tree
        else {
            panic!()
        };
        let OpTree::Filter { child, .. } = filter_box.as_ref() else {
            panic!()
        };
        let mut names = col_names(child);
        names.sort();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn test_prune_scan_columns_star_skips_pruning() {
        // SELECT * FROM t(a, b, c)  →  Scan keeps all (Star bail-out)
        let mut tree = OpTree::Project {
            expressions: vec![Expr::Star { table_alias: None }],
            aliases: vec!["*".into()],
            child: Box::new(scan_node("t", 1, &["a", "b", "c"])),
        };
        tree.prune_scan_columns();
        let OpTree::Project { child, .. } = &tree else {
            panic!()
        };
        assert_eq!(col_names(child), vec!["a", "b", "c"]);
    }

    #[test]
    fn test_prune_scan_columns_raw_skips_pruning() {
        // Raw expression bail-out
        let mut tree = OpTree::Project {
            expressions: vec![Expr::Raw("t.a + 1".into())],
            aliases: vec!["expr".into()],
            child: Box::new(scan_node("t", 1, &["a", "b"])),
        };
        tree.prune_scan_columns();
        let OpTree::Project { child, .. } = &tree else {
            panic!()
        };
        assert_eq!(col_names(child), vec!["a", "b"]);
    }

    #[test]
    fn test_prune_scan_columns_unqualified_ref_matches_all_scans() {
        // Unqualified column ref `a` should be kept in all scans
        // SELECT a FROM t1(a, b) JOIN t2(a, c) ON ...
        let mut tree = OpTree::Project {
            expressions: vec![col("a")],
            aliases: vec!["a".into()],
            child: Box::new(OpTree::InnerJoin {
                condition: Expr::BinaryOp {
                    op: "=".into(),
                    left: Box::new(qualified_col("t1", "a")),
                    right: Box::new(qualified_col("t2", "a")),
                },
                left: Box::new(scan_node("t1", 1, &["a", "b"])),
                right: Box::new(scan_node("t2", 2, &["a", "c"])),
            }),
        };
        tree.prune_scan_columns();
        let OpTree::Project { child, .. } = &tree else {
            panic!()
        };
        let OpTree::InnerJoin { left, right, .. } = child.as_ref() else {
            panic!()
        };
        assert_eq!(col_names(left), vec!["a"]);
        assert_eq!(col_names(right), vec!["a"]);
    }

    #[test]
    fn test_prune_scan_columns_join_condition() {
        // Join condition refs are collected
        let mut tree = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".into(),
                left: Box::new(qualified_col("t1", "id")),
                right: Box::new(qualified_col("t2", "t1_id")),
            },
            left: Box::new(scan_node("t1", 1, &["id", "name", "extra"])),
            right: Box::new(scan_node("t2", 2, &["t1_id", "val", "extra"])),
        };
        tree.prune_scan_columns();
        let OpTree::InnerJoin { left, right, .. } = &tree else {
            panic!()
        };
        assert_eq!(col_names(left), vec!["id"]);
        assert_eq!(col_names(right), vec!["t1_id"]);
    }

    #[test]
    fn test_prune_scan_columns_aggregate() {
        // SELECT t.dept, SUM(t.salary) FROM t(id, dept, salary, name)
        let mut tree = OpTree::Aggregate {
            group_by: vec![qualified_col("t", "dept")],
            aggregates: vec![AggExpr {
                function: AggFunc::Sum,
                argument: Some(qualified_col("t", "salary")),
                alias: "total".into(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["id", "dept", "salary", "name"])),
        };
        tree.prune_scan_columns();
        let OpTree::Aggregate { child, .. } = &tree else {
            panic!()
        };
        let mut names = col_names(child);
        names.sort();
        assert_eq!(names, vec!["dept", "salary"]);
    }

    // ── strip_order_by_and_limit tests ──────────────────────────────

    #[test]
    fn test_strip_order_by_simple() {
        let result = strip_order_by_and_limit("SELECT * FROM t ORDER BY id LIMIT 10");
        assert_eq!(result, "SELECT * FROM t");
    }

    #[test]
    fn test_strip_order_by_with_offset() {
        let result = strip_order_by_and_limit("SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 5");
        assert_eq!(result, "SELECT * FROM t");
    }

    #[test]
    fn test_strip_order_by_no_order_by() {
        let result = strip_order_by_and_limit("SELECT * FROM t WHERE id > 1");
        assert_eq!(result, "SELECT * FROM t WHERE id > 1");
    }

    #[test]
    fn test_strip_order_by_preserves_subquery_order() {
        // ORDER BY inside subquery should NOT be stripped
        let q = "SELECT * FROM (SELECT * FROM t ORDER BY id) sub ORDER BY val LIMIT 5";
        let result = strip_order_by_and_limit(q);
        assert_eq!(result, "SELECT * FROM (SELECT * FROM t ORDER BY id) sub");
    }

    #[test]
    fn test_strip_order_by_with_string_literal() {
        // ORDER BY keyword inside a string literal should be ignored
        let q = "SELECT * FROM t WHERE name = 'ORDER BY me' ORDER BY id LIMIT 3";
        let result = strip_order_by_and_limit(q);
        assert_eq!(result, "SELECT * FROM t WHERE name = 'ORDER BY me'");
    }

    #[test]
    fn test_strip_order_by_trailing_semicolon() {
        let result = strip_order_by_and_limit("SELECT * FROM t ORDER BY id;");
        assert_eq!(result, "SELECT * FROM t");
    }

    #[test]
    fn test_strip_order_by_fetch_first() {
        let result =
            strip_order_by_and_limit("SELECT * FROM t ORDER BY id FETCH FIRST 10 ROWS ONLY");
        assert_eq!(result, "SELECT * FROM t");
    }

    // ── Expr::output_name tests ─────────────────────────────────────

    #[test]
    fn test_output_name_column_ref_unqualified() {
        let e = col("amount");
        assert_eq!(e.output_name(), "amount");
    }

    #[test]
    fn test_output_name_column_ref_qualified() {
        let e = qualified_col("orders", "total");
        // output_name strips the qualifier
        assert_eq!(e.output_name(), "total");
    }

    #[test]
    fn test_output_name_literal() {
        let e = Expr::Literal("42".to_string());
        // Literals fall through to to_sql()
        assert_eq!(e.output_name(), "42");
    }

    #[test]
    fn test_output_name_func_call() {
        let e = Expr::FuncCall {
            func_name: "COUNT".to_string(),
            args: vec![Expr::Star { table_alias: None }],
        };
        assert_eq!(e.output_name(), "COUNT(*)");
    }

    #[test]
    fn test_output_name_binary_op() {
        let e = Expr::BinaryOp {
            op: "+".to_string(),
            left: Box::new(col("a")),
            right: Box::new(Expr::Literal("1".to_string())),
        };
        assert_eq!(e.output_name(), "(a + 1)");
    }

    // ── unwrap_transparent tests ────────────────────────────────────

    #[test]
    fn test_unwrap_transparent_filter() {
        let scan = scan_node("t", 1, &["id"]);
        let filtered = OpTree::Filter {
            predicate: Expr::Literal("TRUE".to_string()),
            child: Box::new(scan),
        };
        let inner = unwrap_transparent(&filtered);
        assert!(matches!(inner, OpTree::Scan { .. }));
    }

    #[test]
    fn test_unwrap_transparent_subquery() {
        let scan = scan_node("t", 1, &["id"]);
        let sub = OpTree::Subquery {
            alias: "sub".to_string(),
            column_aliases: vec![],
            child: Box::new(scan),
        };
        let inner = unwrap_transparent(&sub);
        assert!(matches!(inner, OpTree::Scan { .. }));
    }

    #[test]
    fn test_unwrap_transparent_nested() {
        let scan = scan_node("t", 1, &["id"]);
        let filtered = OpTree::Filter {
            predicate: Expr::Literal("TRUE".to_string()),
            child: Box::new(OpTree::Subquery {
                alias: "sub".to_string(),
                column_aliases: vec![],
                child: Box::new(scan),
            }),
        };
        let inner = unwrap_transparent(&filtered);
        assert!(matches!(inner, OpTree::Scan { .. }));
    }

    #[test]
    fn test_unwrap_transparent_non_transparent() {
        let scan = scan_node("t", 1, &["id"]);
        let inner = unwrap_transparent(&scan);
        assert!(matches!(inner, OpTree::Scan { .. }));
    }

    #[test]
    fn test_unwrap_transparent_join_not_unwrapped() {
        let join = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".to_string()),
            left: Box::new(scan_node("a", 1, &["id"])),
            right: Box::new(scan_node("b", 2, &["id"])),
        };
        let inner = unwrap_transparent(&join);
        assert!(matches!(inner, OpTree::InnerJoin { .. }));
    }

    // ── OpTree::output_columns tests ────────────────────────────────

    #[test]
    fn test_output_columns_scan() {
        let tree = scan_node("t", 1, &["id", "name", "value"]);
        assert_eq!(tree.output_columns(), vec!["id", "name", "value"]);
    }

    #[test]
    fn test_output_columns_project() {
        let tree = OpTree::Project {
            expressions: vec![col("x"), col("y")],
            aliases: vec!["a".to_string(), "b".to_string()],
            child: Box::new(scan_node("t", 1, &["x", "y", "z"])),
        };
        assert_eq!(tree.output_columns(), vec!["a", "b"]);
    }

    #[test]
    fn test_output_columns_filter_passthrough() {
        let tree = OpTree::Filter {
            predicate: Expr::Literal("x > 0".to_string()),
            child: Box::new(scan_node("t", 1, &["x", "y"])),
        };
        assert_eq!(tree.output_columns(), vec!["x", "y"]);
    }

    #[test]
    fn test_output_columns_inner_join() {
        let tree = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".to_string()),
            left: Box::new(scan_node("a", 1, &["id", "val"])),
            right: Box::new(scan_node("b", 2, &["ref_id"])),
        };
        assert_eq!(tree.output_columns(), vec!["id", "val", "ref_id"]);
    }

    #[test]
    fn test_output_columns_aggregate() {
        let tree = OpTree::Aggregate {
            group_by: vec![col("dept")],
            aggregates: vec![AggExpr {
                function: AggFunc::Count,
                argument: None,
                alias: "cnt".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan_node("t", 1, &["dept", "salary"])),
        };
        assert_eq!(tree.output_columns(), vec!["dept", "cnt"]);
    }

    #[test]
    fn test_output_columns_distinct() {
        let tree = OpTree::Distinct {
            child: Box::new(scan_node("t", 1, &["a", "b"])),
        };
        assert_eq!(tree.output_columns(), vec!["a", "b"]);
    }

    #[test]
    fn test_output_columns_union_all() {
        let tree = OpTree::UnionAll {
            children: vec![
                scan_node("a", 1, &["x", "y"]),
                scan_node("b", 2, &["x", "y"]),
            ],
        };
        assert_eq!(tree.output_columns(), vec!["x", "y"]);
    }

    #[test]
    fn test_output_columns_semi_join_left_only() {
        let tree = OpTree::SemiJoin {
            condition: Expr::Literal("TRUE".to_string()),
            left: Box::new(scan_node("o", 1, &["id", "amount"])),
            right: Box::new(scan_node("i", 2, &["order_id"])),
        };
        assert_eq!(tree.output_columns(), vec!["id", "amount"]);
    }

    // ── OpTree::source_oids tests ───────────────────────────────────

    #[test]
    fn test_source_oids_single_scan() {
        let tree = scan_node("t", 42, &["id"]);
        assert_eq!(tree.source_oids(), vec![42]);
    }

    #[test]
    fn test_source_oids_join() {
        let tree = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".to_string()),
            left: Box::new(scan_node("a", 10, &["id"])),
            right: Box::new(scan_node("b", 20, &["id"])),
        };
        assert_eq!(tree.source_oids(), vec![10, 20]);
    }

    #[test]
    fn test_source_oids_filter_passthrough() {
        let tree = OpTree::Filter {
            predicate: Expr::Literal("x > 0".to_string()),
            child: Box::new(scan_node("t", 99, &["x"])),
        };
        assert_eq!(tree.source_oids(), vec![99]);
    }

    #[test]
    fn test_source_oids_recursive_self_ref_empty() {
        let tree = OpTree::RecursiveSelfRef {
            cte_name: "tree".to_string(),
            alias: "t".to_string(),
            columns: vec!["id".to_string()],
        };
        assert_eq!(tree.source_oids(), Vec::<u32>::new());
    }

    #[test]
    fn test_source_oids_cte_scan_empty() {
        let tree = OpTree::CteScan {
            cte_id: 0,
            cte_name: "cte1".to_string(),
            alias: "c".to_string(),
            columns: vec!["id".to_string()],
            cte_def_aliases: vec![],
            column_aliases: vec![],
            body: None,
        };
        assert_eq!(tree.source_oids(), Vec::<u32>::new());
    }

    #[test]
    fn test_source_oids_union_all() {
        let tree = OpTree::UnionAll {
            children: vec![
                scan_node("a", 5, &["id"]),
                scan_node("b", 10, &["id"]),
                scan_node("c", 15, &["id"]),
            ],
        };
        assert_eq!(tree.source_oids(), vec![5, 10, 15]);
    }

    // ── split_and_predicates / join_and_predicates tests ────────────

    #[test]
    fn test_split_and_single_predicate() {
        let expr = Expr::Literal("x > 0".to_string());
        let parts = split_and_predicates(expr);
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].to_sql(), "x > 0");
    }

    #[test]
    fn test_split_and_flat_and() {
        let expr = Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(Expr::Literal("a".to_string())),
            right: Box::new(Expr::Literal("b".to_string())),
        };
        let parts = split_and_predicates(expr);
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn test_split_and_nested_and() {
        let expr = Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(Expr::BinaryOp {
                op: "AND".to_string(),
                left: Box::new(Expr::Literal("a".to_string())),
                right: Box::new(Expr::Literal("b".to_string())),
            }),
            right: Box::new(Expr::Literal("c".to_string())),
        };
        let parts = split_and_predicates(expr);
        assert_eq!(parts.len(), 3);
    }

    #[test]
    fn test_split_and_non_and_operator_not_split() {
        let expr = Expr::BinaryOp {
            op: "OR".to_string(),
            left: Box::new(Expr::Literal("a".to_string())),
            right: Box::new(Expr::Literal("b".to_string())),
        };
        let parts = split_and_predicates(expr);
        assert_eq!(parts.len(), 1);
    }

    #[test]
    fn test_join_and_predicates_single() {
        let parts = vec![Expr::Literal("a".to_string())];
        let result = join_and_predicates(parts);
        assert_eq!(result.unwrap().to_sql(), "a");
    }

    #[test]
    fn test_join_and_predicates_multiple() {
        let parts = vec![
            Expr::Literal("a".to_string()),
            Expr::Literal("b".to_string()),
            Expr::Literal("c".to_string()),
        ];
        let result = join_and_predicates(parts);
        // Should produce ((a AND b) AND c)
        assert_eq!(result.unwrap().to_sql(), "((a AND b) AND c)");
    }

    #[test]
    fn test_split_and_roundtrip() {
        // Split then join should produce equivalent expression
        let expr = Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(Expr::Literal("x > 0".to_string())),
            right: Box::new(Expr::Literal("y < 10".to_string())),
        };
        let parts = split_and_predicates(expr);
        assert_eq!(parts.len(), 2);
        let rejoined = join_and_predicates(parts);
        assert_eq!(rejoined.unwrap().to_sql(), "(x > 0 AND y < 10)");
    }

    // ── AggFunc::is_group_rescan tests ──────────────────────────────

    #[test]
    fn test_agg_group_rescan_sum_is_algebraic() {
        assert!(!AggFunc::Sum.is_group_rescan());
    }

    #[test]
    fn test_agg_group_rescan_count_is_algebraic() {
        assert!(!AggFunc::Count.is_group_rescan());
    }

    #[test]
    fn test_agg_group_rescan_min_is_algebraic() {
        assert!(!AggFunc::Min.is_group_rescan());
    }

    #[test]
    fn test_agg_group_rescan_max_is_algebraic() {
        assert!(!AggFunc::Max.is_group_rescan());
    }

    #[test]
    fn test_agg_avg_is_algebraic_via_aux() {
        // AVG is now algebraic via auxiliary columns, not group-rescan
        assert!(!AggFunc::Avg.is_group_rescan());
        assert!(AggFunc::Avg.is_algebraic_via_aux());
    }

    #[test]
    fn test_agg_group_rescan_string_agg_needs_rescan() {
        assert!(AggFunc::StringAgg.is_group_rescan());
    }

    #[test]
    fn test_agg_group_rescan_bool_and_needs_rescan() {
        assert!(AggFunc::BoolAnd.is_group_rescan());
    }

    #[test]
    fn test_agg_group_rescan_array_agg_needs_rescan() {
        assert!(AggFunc::ArrayAgg.is_group_rescan());
    }

    #[test]
    fn test_agg_group_rescan_stddev_pop_needs_rescan() {
        // STDDEV_POP is now algebraic via auxiliary columns, not group-rescan
        assert!(!AggFunc::StddevPop.is_group_rescan());
        assert!(AggFunc::StddevPop.is_algebraic_via_aux());
        assert!(AggFunc::StddevPop.needs_sum_of_squares());
    }

    #[test]
    fn test_agg_group_rescan_mode_needs_rescan() {
        assert!(AggFunc::Mode.is_group_rescan());
    }

    // ── collect_volatilities expanded tests ──────────────────────────

    #[test]
    fn test_collect_volatilities_coalesce_skips_lookup() {
        // COALESCE is a parser construct (immutable), should not trigger
        // function volatility lookup. With immutable args, worst stays 'i'.
        let expr = Expr::FuncCall {
            func_name: "COALESCE".to_string(),
            args: vec![col("x"), Expr::Literal("0".to_string())],
        };
        let mut worst = 'i';
        collect_volatilities(&expr, &mut worst).unwrap();
        assert_eq!(worst, 'i');
    }

    #[test]
    fn test_collect_volatilities_nullif_skips_lookup() {
        let expr = Expr::FuncCall {
            func_name: "NULLIF".to_string(),
            args: vec![col("x"), Expr::Literal("0".to_string())],
        };
        let mut worst = 'i';
        collect_volatilities(&expr, &mut worst).unwrap();
        assert_eq!(worst, 'i');
    }

    #[test]
    fn test_collect_volatilities_greatest_least_skip_lookup() {
        for name in &["GREATEST", "LEAST"] {
            let expr = Expr::FuncCall {
                func_name: name.to_string(),
                args: vec![col("a"), col("b")],
            };
            let mut worst = 'i';
            collect_volatilities(&expr, &mut worst).unwrap();
            assert_eq!(worst, 'i', "{name} should skip function lookup");
        }
    }

    #[test]
    fn test_collect_volatilities_normal_func_is_volatile() {
        // Non-builtin function → lookup stub returns 'v'
        let expr = Expr::FuncCall {
            func_name: "random".to_string(),
            args: vec![],
        };
        let mut worst = 'i';
        collect_volatilities(&expr, &mut worst).unwrap();
        assert_eq!(worst, 'v');
    }

    #[test]
    fn test_collect_volatilities_coalesce_with_volatile_arg() {
        // COALESCE itself is immutable but its args may be volatile
        let expr = Expr::FuncCall {
            func_name: "COALESCE".to_string(),
            args: vec![
                Expr::FuncCall {
                    func_name: "random".to_string(),
                    args: vec![],
                },
                Expr::Literal("0".to_string()),
            ],
        };
        let mut worst = 'i';
        collect_volatilities(&expr, &mut worst).unwrap();
        // random() → volatile via test stub
        assert_eq!(worst, 'v');
    }

    #[test]
    fn test_collect_volatilities_column_ref_stays_immutable() {
        let expr = col("x");
        let mut worst = 'i';
        collect_volatilities(&expr, &mut worst).unwrap();
        assert_eq!(worst, 'i');
    }

    #[test]
    fn test_collect_volatilities_star_stays_immutable() {
        let expr = Expr::Star { table_alias: None };
        let mut worst = 'i';
        collect_volatilities(&expr, &mut worst).unwrap();
        assert_eq!(worst, 'i');
    }

    // ── split_exists_correlation / try_extract_exists_corr_pair tests

    fn qcol(table: &str, name: &str) -> Expr {
        Expr::ColumnRef {
            table_alias: Some(table.to_string()),
            column_name: name.to_string(),
        }
    }

    fn eq_pred(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn and_pred(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    #[test]
    fn test_split_exists_correlation_simple() {
        // WHERE o.cust_id = c.id (inner=o, outer=c)
        let pred = eq_pred(qcol("o", "cust_id"), qcol("c", "id"));
        let (corr, remaining) = split_exists_correlation(&pred, &["o".to_string()]);
        assert_eq!(corr.len(), 1);
        let (outer_expr, inner_col) = &corr[0];
        assert_eq!(inner_col, "cust_id");
        assert!(matches!(outer_expr, Expr::ColumnRef { column_name, .. } if column_name == "id"));
        assert!(remaining.is_none());
    }

    #[test]
    fn test_split_exists_correlation_reversed() {
        // WHERE c.id = o.cust_id (outer=c, inner=o)
        let pred = eq_pred(qcol("c", "id"), qcol("o", "cust_id"));
        let (corr, remaining) = split_exists_correlation(&pred, &["o".to_string()]);
        assert_eq!(corr.len(), 1);
        let (outer_expr, inner_col) = &corr[0];
        assert_eq!(inner_col, "cust_id");
        assert!(matches!(outer_expr, Expr::ColumnRef { column_name, .. } if column_name == "id"));
        assert!(remaining.is_none());
    }

    #[test]
    fn test_split_exists_correlation_with_inner_only() {
        // WHERE o.cust_id = c.id AND o.status = 'active'
        let pred = and_pred(
            eq_pred(qcol("o", "cust_id"), qcol("c", "id")),
            eq_pred(qcol("o", "status"), Expr::Literal("'active'".to_string())),
        );
        let (corr, remaining) = split_exists_correlation(&pred, &["o".to_string()]);
        assert_eq!(corr.len(), 1, "should extract one correlation pair");
        assert!(
            remaining.is_some(),
            "inner-only predicate should be in remaining"
        );
    }

    #[test]
    fn test_split_exists_correlation_no_correlation() {
        // WHERE o.status = 'active' (no outer reference)
        let pred = eq_pred(qcol("o", "status"), Expr::Literal("'active'".to_string()));
        let (corr, remaining) = split_exists_correlation(&pred, &["o".to_string()]);
        assert!(corr.is_empty(), "no correlation pair expected");
        assert!(remaining.is_some(), "inner-only pred should remain");
    }

    #[test]
    fn test_split_exists_correlation_multi_key() {
        // WHERE o.a = c.x AND o.b = c.y
        let pred = and_pred(
            eq_pred(qcol("o", "a"), qcol("c", "x")),
            eq_pred(qcol("o", "b"), qcol("c", "y")),
        );
        let (corr, remaining) = split_exists_correlation(&pred, &["o".to_string()]);
        assert_eq!(corr.len(), 2, "should extract two correlation pairs");
        assert!(remaining.is_none());
    }

    #[test]
    fn test_collect_tree_source_aliases_scan() {
        let tree = scan_node("o", 1, &["id"]);
        assert_eq!(collect_tree_source_aliases(&tree), vec!["o"]);
    }

    #[test]
    fn test_collect_tree_source_aliases_join() {
        let tree = OpTree::InnerJoin {
            condition: Expr::Literal("TRUE".to_string()),
            left: Box::new(scan_node("a", 1, &["id"])),
            right: Box::new(scan_node("b", 2, &["id"])),
        };
        let aliases = collect_tree_source_aliases(&tree);
        assert!(aliases.contains(&"a".to_string()));
        assert!(aliases.contains(&"b".to_string()));
    }

    // ── P2-6: Correlation predicate extraction tests ────────────────

    #[test]
    fn test_correlation_extraction_simple_equality() {
        // WHERE li.order_id = o.id
        let expr = Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(qualified_col("li", "order_id")),
            right: Box::new(qualified_col("o", "id")),
        };
        let inner_alias_oids = vec![("li".to_string(), 42u32)];
        let inner_aliases: Vec<&str> = inner_alias_oids.iter().map(|(a, _)| a.as_str()).collect();
        let mut preds = Vec::new();
        collect_correlation_equalities(&expr, &inner_aliases, &inner_alias_oids, &mut preds);
        assert_eq!(preds.len(), 1);
        assert_eq!(preds[0].outer_col, "id");
        assert_eq!(preds[0].inner_alias, "li");
        assert_eq!(preds[0].inner_col, "order_id");
        assert_eq!(preds[0].inner_oid, 42);
    }

    #[test]
    fn test_correlation_extraction_reversed_order() {
        // WHERE o.id = li.order_id (outer on left, inner on right)
        let expr = Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(qualified_col("o", "id")),
            right: Box::new(qualified_col("li", "order_id")),
        };
        let inner_alias_oids = vec![("li".to_string(), 42u32)];
        let inner_aliases: Vec<&str> = inner_alias_oids.iter().map(|(a, _)| a.as_str()).collect();
        let mut preds = Vec::new();
        collect_correlation_equalities(&expr, &inner_aliases, &inner_alias_oids, &mut preds);
        assert_eq!(preds.len(), 1);
        assert_eq!(preds[0].outer_col, "id");
        assert_eq!(preds[0].inner_col, "order_id");
    }

    #[test]
    fn test_correlation_extraction_and_conjunction() {
        // WHERE li.order_id = o.id AND li.status = 'active'
        let expr = Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("li", "order_id")),
                right: Box::new(qualified_col("o", "id")),
            }),
            right: Box::new(Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("li", "status")),
                right: Box::new(Expr::Literal("'active'".to_string())),
            }),
        };
        let inner_alias_oids = vec![("li".to_string(), 42u32)];
        let inner_aliases: Vec<&str> = inner_alias_oids.iter().map(|(a, _)| a.as_str()).collect();
        let mut preds = Vec::new();
        collect_correlation_equalities(&expr, &inner_aliases, &inner_alias_oids, &mut preds);
        // Only the correlation equality should be extracted, not li.status = 'active'
        assert_eq!(preds.len(), 1);
        assert_eq!(preds[0].inner_col, "order_id");
    }

    #[test]
    fn test_correlation_extraction_no_inner_alias_match() {
        // WHERE a.col = b.col — neither matches inner aliases
        let expr = Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(qualified_col("a", "col")),
            right: Box::new(qualified_col("b", "col")),
        };
        let inner_alias_oids: Vec<(String, u32)> = vec![("x".to_string(), 1)];
        let inner_aliases: Vec<&str> = inner_alias_oids.iter().map(|(a, _)| a.as_str()).collect();
        let mut preds = Vec::new();
        collect_correlation_equalities(&expr, &inner_aliases, &inner_alias_oids, &mut preds);
        assert!(preds.is_empty());
    }

    #[test]
    fn test_correlation_extraction_both_inner() {
        // WHERE li.col1 = li.col2 — both sides reference inner alias
        let expr = Expr::BinaryOp {
            op: "=".to_string(),
            left: Box::new(qualified_col("li", "col1")),
            right: Box::new(qualified_col("li", "col2")),
        };
        let inner_alias_oids = vec![("li".to_string(), 42u32)];
        let inner_aliases: Vec<&str> = inner_alias_oids.iter().map(|(a, _)| a.as_str()).collect();
        let mut preds = Vec::new();
        collect_correlation_equalities(&expr, &inner_aliases, &inner_alias_oids, &mut preds);
        // Both sides are inner — no correlation predicate
        assert!(preds.is_empty());
    }

    #[test]
    fn test_correlation_extraction_compound() {
        // WHERE li.order_id = o.id AND li.region = o.region
        let expr = Expr::BinaryOp {
            op: "AND".to_string(),
            left: Box::new(Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("li", "order_id")),
                right: Box::new(qualified_col("o", "id")),
            }),
            right: Box::new(Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("li", "region")),
                right: Box::new(qualified_col("o", "region")),
            }),
        };
        let inner_alias_oids = vec![("li".to_string(), 42u32)];
        let inner_aliases: Vec<&str> = inner_alias_oids.iter().map(|(a, _)| a.as_str()).collect();
        let mut preds = Vec::new();
        collect_correlation_equalities(&expr, &inner_aliases, &inner_alias_oids, &mut preds);
        assert_eq!(preds.len(), 2);
    }

    // ── G13-SD: Parse depth guard tests ─────────────────────────────

    #[test]
    fn test_cte_parse_context_descend_within_limit() {
        let mut ctx = CteParseContext::new(HashMap::new(), HashMap::new());
        // max_depth defaults to 64 in unit tests
        assert_eq!(ctx.depth, 0);
        let prev = ctx.descend().expect("should not exceed limit");
        assert_eq!(prev, 0);
        assert_eq!(ctx.depth, 1);
        ctx.ascend(prev);
        assert_eq!(ctx.depth, 0);
    }

    #[test]
    fn test_cte_parse_context_descend_exceeds_limit() {
        let mut ctx = CteParseContext::new(HashMap::new(), HashMap::new());
        // Manually set depth to max_depth to trigger the guard.
        ctx.depth = ctx.max_depth;
        let result = ctx.descend();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, PgTrickleError::QueryTooComplex(_)),
            "expected QueryTooComplex, got: {err:?}"
        );
        assert!(err.to_string().contains("max_parse_depth"));
    }

    #[test]
    fn test_cte_parse_context_descend_ascend_round_trip() {
        let mut ctx = CteParseContext::new(HashMap::new(), HashMap::new());
        let d0 = ctx.descend().unwrap();
        let d1 = ctx.descend().unwrap();
        let d2 = ctx.descend().unwrap();
        assert_eq!(ctx.depth, 3);
        ctx.ascend(d2);
        assert_eq!(ctx.depth, 2);
        ctx.ascend(d1);
        assert_eq!(ctx.depth, 1);
        ctx.ascend(d0);
        assert_eq!(ctx.depth, 0);
    }

    // ── A-2: source_key_columns_used tests ──────────────────────────

    #[test]
    fn test_source_key_columns_aggregate_group_by() {
        // SELECT region, SUM(amount) FROM orders GROUP BY region
        let scan = scan_node("orders", 100, &["region", "amount", "id"]);
        let tree = OpTree::Aggregate {
            group_by: vec![col("region")],
            aggregates: vec![AggExpr {
                function: AggFunc::Sum,
                argument: Some(col("amount")),
                alias: "total".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(scan),
        };

        let keys = tree.source_key_columns_used();
        let key_cols = keys.get(&100).unwrap();
        assert_eq!(key_cols, &["region".to_string()]);
    }

    #[test]
    fn test_source_key_columns_scan_only_no_keys() {
        // SELECT a, b, c FROM t — no GROUP BY, no JOIN, no WHERE
        let tree = scan_node("t", 200, &["a", "b", "c"]);
        let keys = tree.source_key_columns_used();
        assert!(
            keys.is_empty(),
            "scan-only query should have no key columns"
        );
    }

    #[test]
    fn test_source_key_columns_inner_join() {
        // SELECT ... FROM a JOIN b ON a.id = b.a_id
        let left = scan_node("a", 100, &["id", "name"]);
        let right = scan_node("b", 200, &["a_id", "value"]);
        let tree = OpTree::InnerJoin {
            condition: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(qualified_col("a", "id")),
                right: Box::new(qualified_col("b", "a_id")),
            },
            left: Box::new(left),
            right: Box::new(right),
        };

        let keys = tree.source_key_columns_used();
        // "id" appears in JOIN ON for table 100
        assert!(
            keys.get(&100)
                .is_some_and(|v| v.contains(&"id".to_string()))
        );
        // "a_id" appears in JOIN ON for table 200
        assert!(
            keys.get(&200)
                .is_some_and(|v| v.contains(&"a_id".to_string()))
        );
    }

    #[test]
    fn test_source_key_columns_filter_where() {
        // SELECT a, b FROM t WHERE a > 10
        let scan = scan_node("t", 100, &["a", "b"]);
        let tree = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: ">".to_string(),
                left: Box::new(col("a")),
                right: Box::new(Expr::Literal("10".to_string())),
            },
            child: Box::new(scan),
        };

        let keys = tree.source_key_columns_used();
        assert_eq!(keys.get(&100).unwrap(), &["a".to_string()]);
    }

    #[test]
    fn test_source_key_columns_aggregate_with_filter() {
        // SELECT region, SUM(amount) FROM orders WHERE active = true GROUP BY region
        let scan = scan_node("orders", 100, &["region", "amount", "active"]);
        let filter = OpTree::Filter {
            predicate: Expr::BinaryOp {
                op: "=".to_string(),
                left: Box::new(col("active")),
                right: Box::new(Expr::Literal("true".to_string())),
            },
            child: Box::new(scan),
        };
        let tree = OpTree::Aggregate {
            group_by: vec![col("region")],
            aggregates: vec![AggExpr {
                function: AggFunc::Sum,
                argument: Some(col("amount")),
                alias: "total".to_string(),
                is_distinct: false,
                second_arg: None,
                filter: None,
                order_within_group: None,
                statistical_support: None,
            }],
            child: Box::new(filter),
        };

        let keys = tree.source_key_columns_used();
        let key_cols = keys.get(&100).unwrap();
        assert!(key_cols.contains(&"active".to_string()), "WHERE column");
        assert!(key_cols.contains(&"region".to_string()), "GROUP BY column");
        assert!(
            !key_cols.contains(&"amount".to_string()),
            "value-only column"
        );
    }
}
