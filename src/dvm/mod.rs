//! Differential View Maintenance (DVM) engine.
//!
//! This module implements query differentiation — transforming a defining
//! query Q into a delta query ΔQ that computes only the changes over a
//! given interval.
//!
//! # Theoretical Basis
//!
//! The differential computation framework in this module is derived from:
//!
//! - **DBSP**: Budiu, M. et al. (2023). "DBSP: Automatic Incremental View
//!   Maintenance for Rich Query Languages." PVLDB, 16(7), 1601–1614.
//!   <https://arxiv.org/abs/2203.16684>
//!   The `Z-set` abstraction (rows with +1/−1 multiplicity) directly maps to
//!   the `__pgt_action` column produced by the delta operators.
//!
//! - **Gupta & Mumick (1995)**: "Maintenance of Materialized Views: Problems,
//!   Techniques, and Applications." IEEE Data Engineering Bulletin, 18(2).
//!   The per-operator differentiation rules in `operators/` follow the
//!   derivation given in section 3 of this survey.
//!
//! - **Koch, C. et al. (2014)**: "DBToaster: Higher-order Delta Processing
//!   for Dynamic, Frequently Fresh Views." VLDB Journal, 23(2), 253–278.
//!   Recursive delta compilation strategy inspiration.
//!
//! - **PostgreSQL `REFRESH MATERIALIZED VIEW CONCURRENTLY`** (since PostgreSQL
//!   9.4, December 2014, commit `96ef3b8`): the snapshot-diff strategy used
//!   for recomputation-diff refreshes mirrors the algorithm in
//!   `src/backend/commands/matview.c`.
//!
//! # Submodules
//! - `parser` — Parse defining query into an operator tree
//! - `diff` — Query differentiation framework
//! - `row_id` — Row ID generation strategies
//! - `operators` — Per-operator differentiation rules
//!
//! # Usage
//! ```ignore
//! use crate::dvm::generate_delta_query;
//!
//! let result = generate_delta_query(
//!     &defining_query,
//!     &prev_frontier,
//!     &new_frontier,
//!     "myschema",
//!     "my_st",
//! )?;
//! let delta_sql = result.delta_sql;
//! let columns = result.output_columns;
//! let oids = result.source_oids;
//! ```

pub mod diff;
pub mod operators;
pub mod parser;
pub mod row_id;
pub mod schema;
pub mod snapshot;

pub use diff::DiffContext;
pub use parser::{
    CteRegistry, IncrementalAdmission, ParseResult, TopKInfo, ValidationIssue, check_ivm_support,
    check_ivm_support_with_registry, check_monotonicity, check_monotonicity_with_registry,
    classify_agg_strategy, detect_topk_pattern, has_order_by_without_limit, incremental_admission,
    parse_defining_query, parse_defining_query_full, query_has_cte, query_has_recursive_cte,
    reject_limit_offset, reject_materialized_views, reject_unsupported_constructs,
    resolve_incremental_mode, rewrite_correlated_scalar_in_select, rewrite_demorgan_sublinks,
    rewrite_distinct_on, rewrite_grouping_sets, rewrite_nested_window_exprs, rewrite_rows_from,
    rewrite_scalar_subquery_in_where, rewrite_sublinks_in_or, rewrite_views_inline,
    topk_query_volatility, tree_worst_volatility_with_registry, validate_immediate_mode_support,
    warn_limit_without_order_in_subqueries,
};

use crate::error::PgTrickleError;
use crate::version::Frontier;

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

// ── Delta template cache ─────────────────────────────────────────────

/// Cached delta query template: stores the SQL with LSN placeholder tokens
/// and the metadata (output columns, source OIDs) that remain stable across
/// refreshes for the same defining query.
#[derive(Clone)]
struct CachedDeltaTemplate {
    /// Hash of the defining query string — used to detect changes.
    defining_query_hash: u64,
    /// Delta SQL with `__PGS_PREV_LSN_{oid}__` / `__PGS_NEW_LSN_{oid}__`
    /// placeholder tokens instead of literal LSN values.
    delta_sql_template: String,
    /// User-facing output column names (excludes __pgt_row_id / __pgt_action).
    output_columns: Vec<String>,
    /// Deduplicated source table OIDs.
    source_oids: Vec<u32>,
    /// Whether the delta output is already deduplicated per __pgt_row_id.
    is_deduplicated: bool,
    /// A-2: Whether the delta includes `__pgt_key_changed` boolean column.
    has_key_changed: bool,
    /// B-1: Whether all aggregates are algebraically invertible.
    is_all_algebraic: bool,
    /// QW-5 (v0.81.0): Insertion-order counter for LRU eviction.
    last_used: u64,
}

#[derive(Clone)]
struct CachedPlaceholderResolver {
    ac: aho_corasick::AhoCorasick,
    st_source_pgt_ids: Vec<i64>,
    /// P-4 (v0.78.0): The exact key string used to build this entry
    /// (`template|oid1|oid2|...`).  Stored to detect hash collisions: if the
    /// current key string hashes to the same `u64` but is textually different,
    /// we rebuild the entry rather than returning a stale automaton.
    canonical_key_src: String,
    /// QW-5 (v0.81.0): Insertion-order counter for LRU eviction.
    last_used: u64,
}

thread_local! {
    /// Per-session cache of delta SQL templates, keyed by `pgt_id`.
    ///
    /// The template is invalidated when the defining query hash changes
    /// (e.g. after `ALTER STREAM TABLE`). Stale entries for dropped STs
    /// are harmless — they'll be evicted on the next cache miss.
    ///
    /// Cross-session invalidation (G8.1): flushed when the shared
    /// `CACHE_GENERATION` counter advances.
    static DELTA_TEMPLATE_CACHE: RefCell<HashMap<i64, CachedDeltaTemplate>> =
        RefCell::new(HashMap::new());

    /// Local snapshot of the shared `CACHE_GENERATION` counter.
    /// When the shared value advances past this, the entire cache is flushed.
    static LOCAL_DELTA_CACHE_GEN: Cell<u64> = const { Cell::new(0) };

    /// PERF-005 (v0.73.0): Cache compiled placeholder resolver automata for
    /// `resolve_delta_template()`.
    static PLACEHOLDER_RESOLVER_CACHE: RefCell<HashMap<u64, CachedPlaceholderResolver>> =
        RefCell::new(HashMap::new());

    /// QW-5 (v0.81.0): Monotone insertion counter for LRU eviction of
    /// DELTA_TEMPLATE_CACHE and PLACEHOLDER_RESOLVER_CACHE.
    static L1_CACHE_CLOCK: Cell<u64> = const { Cell::new(0) };
}

/// Hash a string using the default hasher (for cache invalidation).
fn hash_string(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

// ── QW-5: L0/L1 cache LRU helpers ────────────────────────────────────────

/// Advance the L1 cache clock and return the new tick value.
fn l1_next_clock() -> u64 {
    L1_CACHE_CLOCK.with(|c| {
        let v = c.get().wrapping_add(1);
        c.set(v);
        v
    })
}

/// Insert a delta template into DELTA_TEMPLATE_CACHE, evicting the
/// least-recently-used entry when the cache is at capacity (QW-5).
fn l1_insert_delta_template(pgt_id: i64, mut entry: CachedDeltaTemplate) {
    let clock = l1_next_clock();
    entry.last_used = clock;
    DELTA_TEMPLATE_CACHE.with(|cache| {
        #[cfg(not(test))]
        let max = crate::config::pg_trickle_l1_cache_max_entries();
        #[cfg(test)]
        let max = 256_i32;
        let mut map = cache.borrow_mut();
        if max > 0
            && map.len() >= max as usize
            && let Some(oldest_key) = map.iter().min_by_key(|(_, v)| v.last_used).map(|(k, _)| *k)
        {
            map.remove(&oldest_key);
        }
        map.insert(pgt_id, entry);
    });
}

/// Insert a placeholder resolver into PLACEHOLDER_RESOLVER_CACHE, evicting
/// the least-recently-used entry when the cache is at capacity (QW-5).
fn l1_insert_placeholder_resolver(key: u64, mut entry: CachedPlaceholderResolver) {
    let clock = l1_next_clock();
    entry.last_used = clock;
    PLACEHOLDER_RESOLVER_CACHE.with(|cache| {
        #[cfg(not(test))]
        let max = crate::config::pg_trickle_l1_cache_max_entries();
        #[cfg(test)]
        let max = 256_i32;
        let mut map = cache.borrow_mut();
        if max > 0
            && map.len() >= max as usize
            && let Some(oldest_key) = map.iter().min_by_key(|(_, v)| v.last_used).map(|(k, _)| *k)
        {
            map.remove(&oldest_key);
        }
        map.insert(key, entry);
    });
}

/// A41-2: Check that no `__PGS_[A-Z0-9_]+__` or `__PGT_[A-Z0-9_]+__`
/// placeholder tokens remain in a resolved SQL string.
///
/// Returns `Ok(())` when the SQL is clean.  Returns
/// `Err(PgTrickleError::UnresolvedPlaceholder { token, context })` with
/// the first unresolved token found and the caller-supplied `context`
/// string (usually the stream table name or function name).
///
/// Both `__PGS_` (LSN placeholder) and `__PGT_` (pgt-internal) families
/// are checked in a single pass.
pub(crate) fn check_no_remaining_placeholders(
    sql: &str,
    context: &str,
) -> Result<(), PgTrickleError> {
    check_no_remaining_placeholders_for(sql, context, &["__PGS_", "__PGT_"])
}

/// Like [`check_no_remaining_placeholders`] but only checks the specified
/// placeholder prefix families.  Use this when a resolution step is only
/// responsible for a subset of placeholder families (e.g. `resolve_lsn_placeholders`
/// only resolves `__PGS_*__` tokens; `__PGT_*__` tokens are resolved later).
pub(crate) fn check_no_remaining_placeholders_for(
    sql: &str,
    context: &str,
    prefixes: &[&str],
) -> Result<(), PgTrickleError> {
    // Fast path: none of the prefixes are present.
    if prefixes.iter().all(|p| !sql.contains(p)) {
        return Ok(());
    }

    // Extract the first unresolved token.
    for prefix in prefixes {
        let mut pos = 0;
        while let Some(start) = sql[pos..].find(prefix) {
            let token_start = pos + start;
            // Tokens end with `__`.  Skip the opening `__` from the prefix.
            let after_prefix = token_start + prefix.len();
            if let Some(end_offset) = sql[after_prefix..].find("__") {
                let token = &sql[token_start..after_prefix + end_offset + 2];
                // Validate that the token contains only uppercase letters, digits,
                // and underscores (i.e. it is an actual placeholder, not an SQL
                // identifier that happens to start with `__PGS`/`__PGT`).
                let inner = &sql[after_prefix..after_prefix + end_offset];
                if inner
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                {
                    return Err(PgTrickleError::UnresolvedPlaceholder {
                        token: token.to_string(),
                        context: context.to_string(),
                    });
                }
                pos = after_prefix + end_offset + 2;
            } else {
                break;
            }
        }
    }
    Ok(())
}

/// Resolve a delta SQL template by substituting LSN placeholder tokens
/// with actual frontier values.
///
/// Returns `Err(PgTrickleError::UnresolvedPlaceholder)` if any
/// `__PGS_[A-Z0-9_]+__` or `__PGT_[A-Z0-9_]+__` token remains after
/// all substitution passes (A41-2).
///
/// P-1: Uses a single-pass `aho_corasick::AhoCorasick` multi-pattern
/// replacer to resolve all `__PGS_PREV_LSN_{oid}__` /
/// `__PGS_NEW_LSN_{oid}__` tokens in O(template_length) regardless of
/// how many source OIDs are present.  The previous implementation called
/// `.replace()` twice per OID — O(k × template_length) for k source tables.
fn resolve_delta_template(
    template: &str,
    source_oids: &[u32],
    prev_frontier: &Frontier,
    new_frontier: &Frontier,
) -> Result<String, PgTrickleError> {
    // Fast path: no OIDs to resolve (e.g. constant-select or IMMEDIATE mode
    // queries that have already had their tokens stripped).
    if source_oids.is_empty() && !template.contains("__PGS_PREV_LSN_pgt_") {
        let sql = template.to_string();
        check_no_remaining_placeholders(&sql, "resolve_delta_template")?;
        return Ok(sql);
    }

    let mut resolver_key_src = template.to_string();
    for oid in source_oids {
        resolver_key_src.push('|');
        resolver_key_src.push_str(&oid.to_string());
    }
    let resolver_key = hash_string(&resolver_key_src);

    let resolver = PLACEHOLDER_RESOLVER_CACHE.with(|cache| {
        // P-4 (v0.78.0): Verify canonical key to guard against hash collisions.
        if let Some(existing) = cache.borrow().get(&resolver_key) {
            if existing.canonical_key_src == resolver_key_src {
                return Ok::<CachedPlaceholderResolver, PgTrickleError>(existing.clone());
            }
            // Hash collision detected — evict stale entry and rebuild.
            pgrx::warning!(
                "[pg_trickle] P-4: placeholder resolver cache hash collision for key {resolver_key}; \
                 rebuilding entry"
            );
            cache.borrow_mut().remove(&resolver_key);
        }

        let mut patterns: Vec<String> = Vec::with_capacity(source_oids.len() * 2);
        for &oid in source_oids {
            patterns.push(format!("__PGS_PREV_LSN_{oid}__"));
            patterns.push(format!("__PGS_NEW_LSN_{oid}__"));
        }

        // ST-ST-4: Resolve pgt_-prefixed placeholders for ST source frontiers.
        let pgt_prefix = "__PGS_PREV_LSN_pgt_";
        let mut pgt_ids: Vec<i64> = Vec::new();
        if template.contains(pgt_prefix) {
            let mut search_from = 0usize;
            while let Some(pos) = template[search_from..].find(pgt_prefix) {
                let start = search_from + pos + pgt_prefix.len();
                let end = template[start..]
                    .find("__")
                    .map(|p| start + p)
                    .unwrap_or(template.len());
                if let Ok(id) = template[start..end].parse::<i64>()
                    && !pgt_ids.contains(&id)
                {
                    pgt_ids.push(id);
                }
                search_from = end;
            }
        }

        for pgt_id in &pgt_ids {
            patterns.push(format!("__PGS_PREV_LSN_pgt_{pgt_id}__"));
            patterns.push(format!("__PGS_NEW_LSN_pgt_{pgt_id}__"));
        }

        let ac = aho_corasick::AhoCorasick::new(&patterns)
            .map_err(|e| PgTrickleError::InternalError(format!("placeholder resolver: {e}")))?;

        let built = CachedPlaceholderResolver {
            ac,
            st_source_pgt_ids: pgt_ids,
            canonical_key_src: resolver_key_src.clone(),
            last_used: 0, // set by l1_insert_placeholder_resolver
        };
        l1_insert_placeholder_resolver(resolver_key, built.clone());
        Ok(built)
    })?;

    let mut replacements: Vec<String> =
        Vec::with_capacity(source_oids.len() * 2 + resolver.st_source_pgt_ids.len() * 2);
    for &oid in source_oids {
        replacements.push(prev_frontier.get_lsn(oid));
        replacements.push(new_frontier.get_lsn(oid));
    }

    for pgt_id in &resolver.st_source_pgt_ids {
        let key = format!("pgt_{pgt_id}");
        let prev_lsn = prev_frontier
            .sources
            .get(&key)
            .map(|sv| sv.lsn.clone())
            .unwrap_or_else(|| "0/0".to_string());
        let new_lsn = new_frontier
            .sources
            .get(&key)
            .map(|sv| sv.lsn.clone())
            .unwrap_or_else(|| "0/0".to_string());
        replacements.push(prev_lsn);
        replacements.push(new_lsn);
    }

    // Single-pass replacement via cached Aho-Corasick automaton.
    let sql = if replacements.is_empty() {
        template.to_string()
    } else {
        resolver.ac.replace_all(template, replacements.as_slice())
    };

    // A41-2: Assert no placeholders remain.
    check_no_remaining_placeholders(&sql, "resolve_delta_template")?;

    Ok(sql)
}

/// Invalidate cached delta templates for a given ST (e.g. after DDL).
pub fn invalidate_delta_cache(pgt_id: i64) {
    DELTA_TEMPLATE_CACHE.with(|cache| {
        cache.borrow_mut().remove(&pgt_id);
    });
    PLACEHOLDER_RESOLVER_CACHE.with(|cache| {
        cache.borrow_mut().clear();
    });
}

/// CACHE-3: Flush all entries from the thread-local delta template cache.
///
/// Called by `refresh::flush_local_template_cache()` as part of the
/// full `pgtrickle.clear_caches()` operation.
pub fn flush_all_delta_caches() {
    DELTA_TEMPLATE_CACHE.with(|cache| cache.borrow_mut().clear());
    PLACEHOLDER_RESOLVER_CACHE.with(|cache| cache.borrow_mut().clear());
}

/// UX-1 / CACHE-OBS: Return the current number of entries in the L1
/// (thread-local) delta template cache for this backend connection.
pub fn delta_cache_size() -> usize {
    DELTA_TEMPLATE_CACHE.with(|cache| cache.borrow().len())
}

/// Retrieve the raw delta SQL template (with placeholder tokens) for a ST.
///
/// Returns `None` if the template has not been generated yet.
/// The returned template contains `__PGS_PREV_LSN_{oid}__` and
/// `__PGS_NEW_LSN_{oid}__` tokens that must be resolved before execution.
pub fn get_delta_sql_template(pgt_id: i64) -> Option<String> {
    DELTA_TEMPLATE_CACHE.with(|cache| {
        cache
            .borrow()
            .get(&pgt_id)
            .map(|entry| entry.delta_sql_template.clone())
    })
}

/// Check whether the cached delta for a ST is deduplicated (at most one
/// row per `__pgt_row_id`), allowing the MERGE to skip DISTINCT ON.
pub fn is_delta_deduplicated(pgt_id: i64) -> bool {
    DELTA_TEMPLATE_CACHE.with(|cache| {
        cache
            .borrow()
            .get(&pgt_id)
            .map(|entry| entry.is_deduplicated)
            .unwrap_or(false)
    })
}

/// A-2: Check whether the cached delta for a ST includes a `__pgt_key_changed`
/// boolean column, enabling the MERGE to filter out D-side value-only UPDATEs.
pub fn delta_has_key_changed(pgt_id: i64) -> bool {
    DELTA_TEMPLATE_CACHE.with(|cache| {
        cache
            .borrow()
            .get(&pgt_id)
            .map(|entry| entry.has_key_changed)
            .unwrap_or(false)
    })
}

/// PROF-DLT: Parse a defining query and return the deduplicated list of source
/// table OIDs (TABLE, MATVIEW, FOREIGN_TABLE, and upstream ST storage OIDs).
///
/// Used by `explain_delta()` to build a max-frontier before generating the
/// delta SQL for EXPLAIN without performing a real refresh.
pub fn get_source_oids_for_query(query: &str) -> Result<Vec<u32>, crate::error::PgTrickleError> {
    let result = parse_defining_query_full(query)?;
    let mut oids: Vec<u32> = result.tree.source_oids();
    oids.extend(result.cte_registry.source_oids());
    oids.sort_unstable();
    oids.dedup();
    Ok(oids)
}

/// DI-7: Parse a defining query and return the number of Scan nodes in the
/// join tree. Used to decide whether to fall back to FULL refresh when the
/// join tree complexity exceeds `max_differential_joins`.
pub fn query_join_scan_count(query: &str) -> Result<usize, PgTrickleError> {
    let result = parse_defining_query_full(query)?;
    Ok(operators::join_common::join_scan_count(&result.tree))
}

/// Count the total number of Scan nodes in the defining query, traversing
/// through ALL node types (Aggregate, Window, Distinct, etc.).
/// Used by DI-11 planner hints to detect deep-join queries that need
/// aggressive plan guidance.
pub fn query_total_scan_count(query: &str) -> Result<usize, PgTrickleError> {
    let result = parse_defining_query_full(query)?;
    Ok(operators::join_common::total_scan_count(&result.tree))
}

/// EC-01 / EC-01b: Returns `true` when the parsed defining query contains
/// any join-shaped node (InnerJoin / LeftJoin / FullJoin / SemiJoin /
/// AntiJoin) at any depth, including comma-joins (which the parser
/// normalises to `InnerJoin`).
///
/// Used as the gating predicate for `cleanup_cross_cycle_phantoms` in both
/// the DIFFERENTIAL and IMMEDIATE refresh paths.
pub fn query_has_join(query: &str) -> Result<bool, PgTrickleError> {
    let result = parse_defining_query_full(query)?;
    Ok(operators::join_common::tree_contains_join(&result.tree))
}

/// Check whether an OpTree is a "scan-chain" — only Scan, Filter, Project,
/// and Subquery nodes (no Aggregate, Join, UnionAll, Distinct, Window,
/// RecursiveCte, or CteScan).
///
/// When the top-level tree is a scan-chain, the scan delta can produce
/// deduplicated output (at most one row per PK) which eliminates the
/// need for DISTINCT ON in the MERGE statement.
///
/// **Filter is NOT part of a scan chain.** In merge-safe dedup mode the
/// scan emits only a single INSERT event for UPDATEs (no paired DELETE).
/// If a filter sits above the scan, an UPDATE that moves a row from
/// *passing* the predicate to *failing* it would only produce an INSERT
/// with new values, which the filter discards — leaving the stale old
/// row in the ST.  Standard D+I mode (merge_safe_dedup=false) emits
/// both DELETE(old) and INSERT(new), so the filter correctly passes the
/// DELETE for old values that matched and discards the INSERT with new
/// values that don't match.
fn is_scan_chain_tree(tree: &parser::OpTree) -> bool {
    match tree {
        parser::OpTree::Scan { .. } => true,
        parser::OpTree::Project { child, .. } => is_scan_chain_tree(child),
        parser::OpTree::Subquery { child, .. } => is_scan_chain_tree(child),
        _ => false,
    }
}

/// Result returned by [`generate_delta_query`], bundling the delta SQL
/// together with metadata extracted from the single parse so callers
/// do not need to re-parse the defining query.
pub struct DeltaQueryResult {
    /// The complete delta SQL (WITH … SELECT …).
    pub delta_sql: String,
    /// User-facing output column names (excludes __pgt_row_id / __pgt_action).
    pub output_columns: Vec<String>,
    /// Deduplicated source table OIDs (from both main tree and CTE registry).
    pub source_oids: Vec<u32>,
    /// When true, the delta has at most one row per `__pgt_row_id`,
    /// so the MERGE can skip the outer DISTINCT ON + ORDER BY.
    pub is_deduplicated: bool,
    /// A-2: When true, the top-level delta CTE includes a `__pgt_key_changed`
    /// boolean column. The MERGE template can use this to filter out D-side
    /// rows for value-only UPDATEs, halving MERGE source rows and converting
    /// DELETE+INSERT to a single UPDATE.
    pub has_key_changed: bool,
    /// B-1: When true, all aggregates in the query are algebraically invertible
    /// (COUNT, SUM, AVG, STDDEV, etc.), enabling the explicit DML fast-path
    /// at refresh time instead of MERGE.
    pub is_all_algebraic: bool,
}

/// Generate the full delta SQL query for a defining query.
///
/// This is the main entry point for the DVM engine. It:
/// 1. Parses the defining query into an OpTree + CTE registry
/// 2. Checks DVM support (including CTE bodies)
/// 3. Generates the delta query via differentiation
///
/// For recursive CTEs, a recomputation diff strategy is used: the
/// defining query is re-executed in full and diffed against the current
/// ST storage to produce precise INSERT/DELETE deltas.
///
/// Returns a [`DeltaQueryResult`] containing the delta SQL, output
/// column names, and source OIDs — all derived from a single parse.
pub fn generate_delta_query(
    defining_query: &str,
    prev_frontier: &Frontier,
    new_frontier: &Frontier,
    pgt_schema: &str,
    pgt_name: &str,
) -> Result<DeltaQueryResult, PgTrickleError> {
    // Step 1: Parse the defining query into an operator tree + CTE registry.
    // This now handles recursive CTEs via OpTree::RecursiveCte, so no
    // early bypass is needed.
    let mut result = parse_defining_query_full(defining_query)?;

    // Extract source OIDs before moving cte_registry.
    let mut source_oids: Vec<u32> = result.tree.source_oids();
    source_oids.extend(result.cte_registry.source_oids());
    source_oids.sort_unstable();
    source_oids.dedup();

    // F4 (v0.37.0): Reclassify avg/sum on vector-typed columns to VectorAvg/VectorSum
    // so the DVM uses the group-rescan strategy. Only active when enable_vector_agg = on.
    if crate::config::pg_trickle_enable_vector_agg() {
        let vector_cols = resolve_vector_columns_for_sources(&source_oids);
        if !vector_cols.is_empty() {
            reclassify_vector_aggregates(&mut result.tree, &vector_cols);
        }
    }

    // Step 2: Check DVM support (validates CTE bodies + main tree)
    check_ivm_support_with_registry(&result)?;
    row_id::verify_plan_row_id_schema(&result.tree).map_err(|e| {
        PgTrickleError::InvalidArgument(format!("RowIdSchema verification failed: {e}"))
    })?;

    // Step 3: Generate the delta query.
    // Use differentiate_with_columns() to get the diff result's column list,
    // which includes auxiliary columns (e.g. __pgt_count) for aggregate/distinct.
    let st_user_cols = result.tree.output_columns();
    let is_scan_chain = is_scan_chain_tree(&result.tree);
    let has_pgt_count = result.tree.needs_pgt_count();
    let mut ctx = DiffContext::new(prev_frontier.clone(), new_frontier.clone())
        .with_pgt_name(pgt_schema, pgt_name)
        .with_cte_registry(result.cte_registry)
        .with_defining_query(defining_query);
    ctx.st_user_columns = Some(st_user_cols);
    ctx.merge_safe_dedup = is_scan_chain;
    ctx.st_has_pgt_count = has_pgt_count;

    // P2-5: Resolve CDC column ordinals for each source table so the
    // scan operator can build a changed_cols bitmask filter.
    ctx.source_cdc_columns = resolve_cdc_columns_for_sources(&source_oids);

    // A-2: Resolve key columns (GROUP BY, JOIN ON, WHERE) per source table
    // so the scan operator can compute a key-only bitmask for value-only
    // UPDATE detection.
    ctx.source_key_columns = result.tree.source_key_columns_used();

    // ST-ST-4: Resolve which sources are STs for proper buffer table routing.
    ctx.st_source_pgt_ids = resolve_st_source_pgt_ids(&source_oids);

    // C-4 (v0.54.0): Validate that all ST sources have entries in both frontiers.
    //
    // If a source ST was dropped after the consuming ST was created, its pgt_id
    // will no longer be in the catalog (resolve_st_source_pgt_ids skips it), but
    // the delta query references its change buffer which may be gone.
    // For each CURRENT ST source (known to exist via resolve_st_source_pgt_ids),
    // validate its frontier entry is present. Newly-created ST sources that have
    // never been refreshed legitimately have no frontier entry yet — we skip them
    // only if the new_frontier also lacks the key (both missing → not yet seeded).
    if !ctx.st_source_pgt_ids.is_empty() {
        for &src_pgt_id in ctx.st_source_pgt_ids.values() {
            let key = format!("pgt_{src_pgt_id}");
            if !prev_frontier.sources.contains_key(&key) && !new_frontier.sources.contains_key(&key)
            {
                return Err(PgTrickleError::StSourceFrontierMissing(src_pgt_id));
            }
        }
    }

    // CITUS-4: Pre-resolve stable buffer names so the scan generator
    // does not need to call SPI during SQL generation.
    ctx.source_buffer_names = resolve_buffer_names_for_sources(&source_oids);

    // DAG-4: Apply any active bypass table mappings from fused-chain execution.
    ctx.st_bypass_tables = crate::refresh::get_st_bypass_tables();

    // DI-2: Per-leaf conditional fallback — leaves whose delta fraction
    // exceeds max_delta_fraction use EXCEPT ALL instead of NOT EXISTS.
    ctx.fallback_leaf_oids = crate::refresh::get_fallback_leaf_oids();

    let (delta_sql, output_columns, diff_dedup, diff_has_key_changed) =
        ctx.differentiate_with_columns(&result.tree)?;

    Ok(DeltaQueryResult {
        delta_sql,
        output_columns,
        source_oids,
        is_deduplicated: diff_dedup,
        has_key_changed: diff_has_key_changed,
        is_all_algebraic: result.tree.is_all_algebraic_agg(),
    })
}

/// Generate the full delta SQL query, using a per-session cache to avoid
/// re-parsing and re-differentiating the defining query on every refresh.
///
/// On the first call for a given `pgt_id`, the defining query is parsed,
/// validated, and differentiated with LSN placeholders. The resulting SQL
/// template and metadata are cached. On subsequent calls, the cached
/// template is resolved with actual frontier LSN values — skipping the
/// parse, DVM-support check, and differentiation entirely.
///
/// Cache entries are keyed by `pgt_id` and invalidated when the
/// `defining_query` hash changes (e.g. after `ALTER STREAM TABLE`).
pub fn generate_delta_query_cached(
    pgt_id: i64,
    defining_query: &str,
    prev_frontier: &Frontier,
    new_frontier: &Frontier,
    pgt_schema: &str,
    pgt_name: &str,
) -> Result<DeltaQueryResult, PgTrickleError> {
    // DAG-4: When bypass tables are active, the cached SQL template
    // has the wrong table names.  Fall back to the uncached path.
    let bypass_tables = crate::refresh::get_st_bypass_tables();
    if !bypass_tables.is_empty() {
        return generate_delta_query(
            defining_query,
            prev_frontier,
            new_frontier,
            pgt_schema,
            pgt_name,
        );
    }

    // DI-2: When per-leaf fallback OIDs are active, the cached SQL
    // template uses NOT EXISTS for all leaves. Fall back to the uncached
    // path so the affected leaves emit EXCEPT ALL instead.
    let fallback_oids = crate::refresh::get_fallback_leaf_oids();
    if !fallback_oids.is_empty() {
        return generate_delta_query(
            defining_query,
            prev_frontier,
            new_frontier,
            pgt_schema,
            pgt_name,
        );
    }

    let query_hash = hash_string(defining_query);

    // G8.1: Cross-session cache invalidation — flush if the shared
    // generation counter has advanced past our local snapshot.
    let shared_gen = crate::shmem::current_cache_generation();
    LOCAL_DELTA_CACHE_GEN.with(|local| {
        if local.get() < shared_gen {
            DELTA_TEMPLATE_CACHE.with(|cache| {
                let mut map = cache.borrow_mut();
                // UX-1 / CACHE-OBS: Track evictions before clearing.
                let evicted = map.len() as u64;
                map.clear();
                if evicted > 0 && crate::shmem::is_shmem_available() {
                    crate::shmem::TEMPLATE_CACHE_EVICTIONS
                        .get()
                        .fetch_add(evicted, std::sync::atomic::Ordering::Relaxed);
                }
            });
            local.set(shared_gen);
        }
    });

    // Check the thread-local cache.
    // OPS-10-02: Detect stale entries (hash mismatch) and count them.
    let (cached, was_stale) = DELTA_TEMPLATE_CACHE.with(|cache| {
        let map = cache.borrow();
        match map.get(&pgt_id) {
            Some(entry) if entry.defining_query_hash == query_hash => (Some(entry.clone()), false),
            Some(_) => (None, true), // stale: entry exists but hash changed
            None => (None, false),   // cold miss: no entry at all
        }
    });
    if was_stale {
        // Evict the stale entry so it doesn't consume cache space.
        DELTA_TEMPLATE_CACHE.with(|cache| {
            cache.borrow_mut().remove(&pgt_id);
        });
        crate::shmem::increment_template_cache_stale_evictions();
    }

    if let Some(entry) = cached {
        // Cache hit — resolve placeholders and return.
        // UX-1 / CACHE-OBS: Track L1 hit.
        if crate::shmem::is_shmem_available() {
            crate::shmem::TEMPLATE_CACHE_L1_HITS
                .get()
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let delta_sql = resolve_delta_template(
            &entry.delta_sql_template,
            &entry.source_oids,
            prev_frontier,
            new_frontier,
        )?;
        return Ok(DeltaQueryResult {
            delta_sql,
            output_columns: entry.output_columns,
            source_oids: entry.source_oids,
            is_deduplicated: entry.is_deduplicated,
            has_key_changed: entry.has_key_changed,
            is_all_algebraic: entry.is_all_algebraic,
        });
    }

    // G14-SHC: L2 cache — check the catalog-backed template cache.
    // ~1 ms SPI lookup, vs ~45 ms full DVM parse+differentiate.
    if let Some(ct) = crate::template_cache::lookup(pgt_id, query_hash) {
        // Track L2 hit (only when shmem is initialized; skipped in
        // Light E2E mode where shared_preload_libraries is not set).
        if crate::shmem::is_shmem_available() {
            crate::shmem::TEMPLATE_CACHE_L2_HITS
                .get()
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        let entry = CachedDeltaTemplate {
            defining_query_hash: query_hash,
            delta_sql_template: ct.delta_sql_template.clone(),
            output_columns: ct.output_columns.clone(),
            source_oids: ct.source_oids.clone(),
            is_deduplicated: ct.is_deduplicated,
            has_key_changed: ct.has_key_changed,
            is_all_algebraic: ct.is_all_algebraic,
            last_used: 0, // set by l1_insert_delta_template
        };
        // Promote to L1 (thread-local) for subsequent calls (QW-5: with LRU eviction).
        l1_insert_delta_template(pgt_id, entry);
        let delta_sql = resolve_delta_template(
            &ct.delta_sql_template,
            &ct.source_oids,
            prev_frontier,
            new_frontier,
        )?;
        return Ok(DeltaQueryResult {
            delta_sql,
            output_columns: ct.output_columns,
            source_oids: ct.source_oids,
            is_deduplicated: ct.is_deduplicated,
            has_key_changed: ct.has_key_changed,
            is_all_algebraic: ct.is_all_algebraic,
        });
    }

    // Cache miss — parse, differentiate with placeholder mode, and cache.
    // Track full miss (only when shmem is initialized).
    if crate::shmem::is_shmem_available() {
        crate::shmem::TEMPLATE_CACHE_MISSES
            .get()
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    // M-6 (v0.55.0): Time the DVM parse pass.
    let parse_start = std::time::Instant::now();
    let mut result = parse_defining_query_full(defining_query)?;
    let parse_elapsed_ms = parse_start.elapsed().as_millis() as u64;
    crate::shmem::increment_dvm_parse_ms(parse_elapsed_ms);

    let mut source_oids: Vec<u32> = result.tree.source_oids();
    source_oids.extend(result.cte_registry.source_oids());
    source_oids.sort_unstable();
    source_oids.dedup();

    // F4 (v0.37.0): Reclassify avg/sum on vector-typed columns.
    if crate::config::pg_trickle_enable_vector_agg() {
        let vector_cols = resolve_vector_columns_for_sources(&source_oids);
        if !vector_cols.is_empty() {
            reclassify_vector_aggregates(&mut result.tree, &vector_cols);
        }
    }

    check_ivm_support_with_registry(&result)?;
    row_id::verify_plan_row_id_schema(&result.tree).map_err(|e| {
        PgTrickleError::InvalidArgument(format!("RowIdSchema verification failed: {e}"))
    })?;

    // Generate template with placeholder tokens instead of literal LSNs.
    // Use dummy frontiers — the actual LSN values come from placeholders.
    let is_scan_chain = is_scan_chain_tree(&result.tree);
    let st_user_cols = result.tree.output_columns();
    let has_pgt_count = result.tree.needs_pgt_count();
    let mut ctx = DiffContext::new(Frontier::new(), Frontier::new())
        .with_placeholders()
        .with_pgt_name(pgt_schema, pgt_name)
        .with_cte_registry(result.cte_registry)
        .with_defining_query(defining_query);
    ctx.st_user_columns = Some(st_user_cols);
    ctx.merge_safe_dedup = is_scan_chain;
    ctx.st_has_pgt_count = has_pgt_count;

    // P2-5: Resolve CDC column ordinals for bitmask filter.
    ctx.source_cdc_columns = resolve_cdc_columns_for_sources(&source_oids);

    // A-2: Resolve key columns for value-only UPDATE detection.
    ctx.source_key_columns = result.tree.source_key_columns_used();

    // ST-ST-4: Resolve which sources are STs for proper buffer table routing.
    ctx.st_source_pgt_ids = resolve_st_source_pgt_ids(&source_oids);

    // CITUS-4: Pre-resolve stable buffer names so the scan generator
    // does not need to call SPI during SQL generation.
    ctx.source_buffer_names = resolve_buffer_names_for_sources(&source_oids);

    let (template_sql, output_columns, diff_dedup, diff_has_key_changed) =
        ctx.differentiate_with_columns(&result.tree)?;

    let is_all_algebraic = result.tree.is_all_algebraic_agg();

    // Store in cache (QW-5: with LRU eviction).
    let entry = CachedDeltaTemplate {
        defining_query_hash: query_hash,
        delta_sql_template: template_sql.clone(),
        output_columns: output_columns.clone(),
        source_oids: source_oids.clone(),
        is_deduplicated: diff_dedup,
        has_key_changed: diff_has_key_changed,
        is_all_algebraic,
        last_used: 0, // set by l1_insert_delta_template
    };
    l1_insert_delta_template(pgt_id, entry);

    // G14-SHC: Persist to L2 (catalog table) for cross-backend sharing.
    let _ = crate::template_cache::store(
        pgt_id,
        query_hash,
        &crate::template_cache::CachedTemplate {
            delta_sql_template: template_sql.clone(),
            output_columns: output_columns.clone(),
            source_oids: source_oids.clone(),
            is_deduplicated: diff_dedup,
            has_key_changed: diff_has_key_changed,
            is_all_algebraic,
        },
    );

    // CACHE-1: Signal that the L0/L2 shared cache has been populated at the
    // current CACHE_GENERATION.  Other backends can check this before deciding
    // whether to run the expensive DVM parse.
    if crate::shmem::is_shmem_available() {
        crate::shmem::signal_l0_cache_populated();
        // M-6 (v0.55.0): Track delta SQL template size.
        crate::shmem::increment_delta_query_bytes(template_sql.len() as u64);
    }

    // Resolve placeholders for this invocation.
    let delta_sql =
        resolve_delta_template(&template_sql, &source_oids, prev_frontier, new_frontier)?;

    Ok(DeltaQueryResult {
        delta_sql,
        output_columns,
        source_oids,
        is_deduplicated: diff_dedup,
        has_key_changed: diff_has_key_changed,
        is_all_algebraic,
    })
}

/// P2-5: Resolve CDC column names for each source table OID.
///
/// Returns a map from `table_oid` → ordered CDC column names. The index
/// in the Vec corresponds to the bit position in the `changed_cols`
/// bitmask stored by the CDC trigger. If resolution fails for a source
/// (e.g. the table was dropped), that OID is simply omitted — the scan
/// operator will skip the bitmask filter for that source.
fn resolve_cdc_columns_for_sources(source_oids: &[u32]) -> HashMap<u32, Vec<String>> {
    let mut map = HashMap::new();
    for &oid in source_oids {
        if let Ok(cols) = crate::cdc::resolve_referenced_column_defs(pgrx::pg_sys::Oid::from(oid)) {
            map.insert(oid, cols.into_iter().map(|(name, _)| name).collect());
        }
    }
    map
}

/// ST-ST-4: Resolve which source OIDs are stream tables and map them
/// to their upstream `pgt_id`.
///
/// Returns a map of `table_oid → pgt_id` for sources that are stream tables.
/// Base tables (no entry in `pgt_stream_tables`) are not included.
fn resolve_st_source_pgt_ids(source_oids: &[u32]) -> HashMap<u32, i64> {
    let mut map = HashMap::new();
    for &oid in source_oids {
        if let Some(pgt_id) =
            crate::catalog::StreamTableMeta::pgt_id_for_relid(pgrx::pg_sys::Oid::from(oid))
        {
            map.insert(oid, pgt_id);
        }
    }
    map
}

/// CITUS-4: Resolve the change buffer base name for each source OID.
///
/// For base tables (not ST sources), the buffer is named
/// `changes_{stable_name}` in v0.32.0+.  This avoids calling SPI from
/// inside the SQL generator, which would panic in unit-test contexts.
fn resolve_buffer_names_for_sources(source_oids: &[u32]) -> HashMap<u32, String> {
    let mut map = HashMap::new();
    for &oid in source_oids {
        let name = crate::cdc::buffer_base_name_for_oid(pgrx::pg_sys::Oid::from(oid));
        map.insert(oid, name);
    }
    map
}

// ── F4 (v0.37.0): pgVectorMV — reclassify avg/sum on vector columns ─────────

/// F4: Query SPI for columns of vector type (`vector`, `halfvec`, `sparsevec`)
/// across the given source OIDs.
///
/// Returns a map `oid → set_of_column_names` for columns that are vector-typed.
/// Called from `generate_delta_query` when `enable_vector_agg` is on.
/// Non-fatal: if SPI is unavailable (e.g. unit-test context), returns empty map.
#[cfg(feature = "pg18")]
pub(crate) fn resolve_vector_columns_for_sources(
    source_oids: &[u32],
) -> HashMap<u32, std::collections::HashSet<String>> {
    use pgrx::prelude::*;
    let mut map: HashMap<u32, std::collections::HashSet<String>> = HashMap::new();
    if source_oids.is_empty() {
        return map;
    }
    let oid_list = source_oids
        .iter()
        .map(|o| o.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT a.attrelid::bigint, a.attname::text \
         FROM pg_catalog.pg_attribute a \
         JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
         WHERE a.attrelid IN ({oid_list}) \
           AND a.attnum > 0 \
           AND NOT a.attisdropped \
           AND t.typname IN ('vector', 'halfvec', 'sparsevec')"
    );
    let rows = Spi::connect(|client| -> Vec<(u32, String)> {
        let mut rows = Vec::new();
        let Ok(tup) = client.select(&sql, None, &[]) else {
            return rows;
        };
        for row in tup {
            let relid: Option<i64> = row.get::<i64>(1).ok().flatten();
            let attname: Option<String> = row.get::<String>(2).ok().flatten();
            if let (Some(r), Some(n)) = (relid, attname) {
                rows.push((r as u32, n));
            }
        }
        rows
    });
    for (oid, col) in rows {
        map.entry(oid).or_default().insert(col);
    }
    map
}

#[cfg(not(feature = "pg18"))]
pub(crate) fn resolve_vector_columns_for_sources(
    _source_oids: &[u32],
) -> HashMap<u32, std::collections::HashSet<String>> {
    HashMap::new()
}

/// F4: Walk the OpTree recursively and reclassify `AggFunc::Avg` / `AggFunc::Sum`
/// on vector-typed columns to `AggFunc::VectorAvg` / `AggFunc::VectorSum`.
///
/// pgvector overloads the standard `avg(vector)` and `sum(vector)` aggregates.
/// For vector-typed argument columns, the DVM must use the group-rescan strategy
/// (re-aggregating affected groups) instead of the algebraic auxiliary-column
/// strategy (which generates `COALESCE(st.col, 0) + delta` — invalid for vectors).
pub(crate) fn reclassify_vector_aggregates(
    tree: &mut parser::OpTree,
    vector_cols: &HashMap<u32, std::collections::HashSet<String>>,
) {
    use parser::{AggFunc, Expr, OpTree};

    // Helper: extract a simple column name from an aggregate argument.
    fn agg_col_name(arg: &Option<Expr>) -> Option<&str> {
        match arg {
            Some(Expr::ColumnRef { column_name, .. }) => Some(column_name.as_str()),
            _ => None,
        }
    }

    // Helper: check if any source in the child tree has this column as vector type.
    fn is_vector_col(
        child_oids: &[u32],
        col_name: &str,
        vector_cols: &HashMap<u32, std::collections::HashSet<String>>,
    ) -> bool {
        child_oids
            .iter()
            .any(|oid| vector_cols.get(oid).is_some_and(|s| s.contains(col_name)))
    }

    match tree {
        OpTree::Aggregate {
            aggregates, child, ..
        } => {
            reclassify_vector_aggregates(child, vector_cols);
            let child_oids = child.source_oids();
            for agg in aggregates.iter_mut() {
                let col_name = agg_col_name(&agg.argument).map(str::to_string);
                let new_func = match agg.function {
                    AggFunc::Avg => col_name.as_deref().and_then(|c| {
                        if is_vector_col(&child_oids, c, vector_cols) {
                            Some(AggFunc::VectorAvg)
                        } else {
                            None
                        }
                    }),
                    AggFunc::Sum => col_name.as_deref().and_then(|c| {
                        if is_vector_col(&child_oids, c, vector_cols) {
                            Some(AggFunc::VectorSum)
                        } else {
                            None
                        }
                    }),
                    _ => None,
                };
                if let Some(f) = new_func {
                    agg.function = f;
                }
            }
        }
        OpTree::Filter { child, .. } => reclassify_vector_aggregates(child, vector_cols),
        OpTree::Project { child, .. } => reclassify_vector_aggregates(child, vector_cols),
        OpTree::InnerJoin { left, right, .. } => {
            reclassify_vector_aggregates(left, vector_cols);
            reclassify_vector_aggregates(right, vector_cols);
        }
        OpTree::LeftJoin { left, right, .. } => {
            reclassify_vector_aggregates(left, vector_cols);
            reclassify_vector_aggregates(right, vector_cols);
        }
        OpTree::FullJoin { left, right, .. } => {
            reclassify_vector_aggregates(left, vector_cols);
            reclassify_vector_aggregates(right, vector_cols);
        }
        OpTree::SemiJoin { left, right, .. } => {
            reclassify_vector_aggregates(left, vector_cols);
            reclassify_vector_aggregates(right, vector_cols);
        }
        OpTree::AntiJoin { left, right, .. } => {
            reclassify_vector_aggregates(left, vector_cols);
            reclassify_vector_aggregates(right, vector_cols);
        }
        OpTree::Window { child, .. } => reclassify_vector_aggregates(child, vector_cols),
        OpTree::Distinct { child, .. } => reclassify_vector_aggregates(child, vector_cols),
        OpTree::Subquery { child, .. } => reclassify_vector_aggregates(child, vector_cols),
        OpTree::LateralFunction { child, .. } => reclassify_vector_aggregates(child, vector_cols),
        OpTree::LateralSubquery { child, .. } => reclassify_vector_aggregates(child, vector_cols),
        OpTree::ScalarSubquery { child, .. } => reclassify_vector_aggregates(child, vector_cols),
        OpTree::UnionAll { children, .. } => {
            for c in children.iter_mut() {
                reclassify_vector_aggregates(c, vector_cols);
            }
        }
        OpTree::Intersect { left, right, .. } => {
            reclassify_vector_aggregates(left, vector_cols);
            reclassify_vector_aggregates(right, vector_cols);
        }
        OpTree::Except { left, right, .. } => {
            reclassify_vector_aggregates(left, vector_cols);
            reclassify_vector_aggregates(right, vector_cols);
        }
        OpTree::CteScan { body, .. } => {
            if let Some(b) = body.as_mut() {
                reclassify_vector_aggregates(b, vector_cols);
            }
        }
        OpTree::RecursiveCte {
            base, recursive, ..
        } => {
            reclassify_vector_aggregates(base, vector_cols);
            reclassify_vector_aggregates(recursive, vector_cols);
        }
        // Leaf nodes (Scan, RecursiveSelfRef, ConstantSelect, Values, etc.)
        _ => {}
    }
}

/// F4: Walk an OpTree and return `(output_alias, dimension, typename)` for every
/// `VectorAvg` / `VectorSum` aggregate whose source column has an explicit
/// `vector(N)` / `halfvec(N)` / `sparsevec(N)` dimension in `pg_attribute`.
///
/// This is used after stream-table creation to `ALTER COLUMN … TYPE <type>(N)`
/// so that HNSW / IVFFlat indexes (which require explicit dimensions) can be
/// built on the centroid column.  The returned typename is one of `vector`,
/// `halfvec`, or `sparsevec` — callers must use the correct type expression.
///
/// VH-1 (v0.48.0): returns typename so halfvec/sparsevec output columns are
/// correctly typed (previously always used `vector(N)`).
#[cfg(feature = "pg18")]
pub(crate) fn extract_vector_agg_output_dims(tree: &parser::OpTree) -> Vec<(String, i32, String)> {
    use parser::{AggFunc, Expr, OpTree};
    use pgrx::prelude::*;

    /// Returns `(atttypmod, typname)` for the first matching vector-typed column.
    fn lookup_typmod_and_name(source_oids: &[u32], col_name: &str) -> (i32, String) {
        if source_oids.is_empty() {
            return (-1, "vector".to_string());
        }
        let oid_list = source_oids
            .iter()
            .map(|o| o.to_string())
            .collect::<Vec<_>>()
            .join(",");
        // Only consider vector/halfvec/sparsevec columns — others return defaults.
        let sql = format!(
            "SELECT a.atttypmod, t.typname::text \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid IN ({oid_list}) \
               AND a.attname = $1 \
               AND t.typname IN ('vector', 'halfvec', 'sparsevec') \
               AND a.attnum > 0 \
               AND NOT a.attisdropped \
             LIMIT 1",
        );
        // Execute via SPI and parse two columns.
        Spi::connect(|client| {
            let mut tup = client.select(&sql, None, &[col_name.into()])?;
            if let Some(row) = tup.next() {
                let typmod = row.get::<i32>(1).unwrap_or(None).unwrap_or(-1);
                let typname = row
                    .get::<String>(2)
                    .unwrap_or(None)
                    .unwrap_or_else(|| "vector".to_string());
                return Ok::<_, pgrx::spi::Error>((typmod, typname));
            }
            Ok((-1, "vector".to_string()))
        })
        .unwrap_or((-1, "vector".to_string()))
    }

    fn walk(tree: &OpTree, result: &mut Vec<(String, i32, String)>) {
        match tree {
            OpTree::Aggregate {
                aggregates, child, ..
            } => {
                walk(child, result);
                let child_oids = child.source_oids();
                for agg in aggregates {
                    if !matches!(agg.function, AggFunc::VectorAvg | AggFunc::VectorSum) {
                        continue;
                    }
                    let col_name = match &agg.argument {
                        Some(Expr::ColumnRef { column_name, .. }) => column_name.as_str(),
                        _ => continue,
                    };
                    let (typmod, typname) = lookup_typmod_and_name(&child_oids, col_name);
                    if typmod > 0 {
                        result.push((agg.alias.clone(), typmod, typname));
                    }
                }
            }
            OpTree::Filter { child, .. }
            | OpTree::Project { child, .. }
            | OpTree::Subquery { child, .. }
            | OpTree::Window { child, .. }
            | OpTree::Distinct { child }
            | OpTree::LateralFunction { child, .. }
            | OpTree::LateralSubquery { child, .. }
            | OpTree::ScalarSubquery { child, .. } => walk(child, result),
            OpTree::InnerJoin { left, right, .. }
            | OpTree::LeftJoin { left, right, .. }
            | OpTree::FullJoin { left, right, .. }
            | OpTree::SemiJoin { left, right, .. }
            | OpTree::AntiJoin { left, right, .. }
            | OpTree::Intersect { left, right, .. }
            | OpTree::Except { left, right, .. } => {
                walk(left, result);
                walk(right, result);
            }
            OpTree::UnionAll { children, .. } => {
                for c in children {
                    walk(c, result);
                }
            }
            OpTree::RecursiveCte {
                base, recursive, ..
            } => {
                walk(base, result);
                walk(recursive, result);
            }
            OpTree::CteScan { body, .. } => {
                if let Some(b) = body.as_ref() {
                    walk(b, result);
                }
            }
            _ => {}
        }
    }

    let mut result = Vec::new();
    walk(tree, &mut result);
    result
}

#[cfg(not(feature = "pg18"))]
pub(crate) fn extract_vector_agg_output_dims(_tree: &parser::OpTree) -> Vec<(String, i32, String)> {
    Vec::new()
}

/// Check whether a defining query needs the `__pgt_count` auxiliary column
/// (the top-level operator is Aggregate or Distinct).
///
/// Uses a lightweight parse — no SPI or database access required.
pub fn query_needs_pgt_count(defining_query: &str) -> bool {
    parse_defining_query(defining_query)
        .map(|tree| tree.needs_pgt_count())
        .unwrap_or(false)
}

/// Extract AVG auxiliary column definitions from a defining query.
///
/// Returns `(sum_col_name, count_col_name, arg_sql)` tuples for each
/// non-DISTINCT AVG aggregate, or an empty vec if none.
pub fn query_avg_aux_columns(defining_query: &str) -> Vec<(String, String, String)> {
    parse_defining_query(defining_query)
        .map(|tree| tree.avg_aux_columns())
        .unwrap_or_default()
}

/// Returns `(sum2_col_name, arg_sql)` tuples for each non-DISTINCT STDDEV/VAR
/// aggregate that needs a sum-of-squares auxiliary column. Empty if none.
pub fn query_sum2_aux_columns(defining_query: &str) -> Vec<(String, String)> {
    parse_defining_query(defining_query)
        .map(|tree| tree.sum2_aux_columns())
        .unwrap_or_default()
}

/// Returns `(col_name, arg_sql)` tuples for each cross-product auxiliary
/// column needed by CORR/COVAR/REGR_* aggregates (P3-2). Empty if none.
pub fn query_covar_aux_columns(defining_query: &str) -> Vec<(String, String)> {
    parse_defining_query(defining_query)
        .map(|tree| tree.covar_aux_columns())
        .unwrap_or_default()
}

/// Return PostgreSQL's analyzed accumulator type for each statistical
/// auxiliary column.
pub fn query_statistical_aux_types(defining_query: &str) -> Vec<(String, String)> {
    parse_defining_query(defining_query)
        .map(|tree| tree.statistical_aux_types())
        .unwrap_or_default()
}

/// Returns `(nonnull_col_name, arg_sql)` tuples for each non-DISTINCT SUM
/// aggregate above a FULL JOIN child that needs an auxiliary nonnull-count
/// column (`__pgt_aux_nonnull_*`) for P2-2 NULL-transition correction.
/// Empty if no such aggregates exist.
pub fn query_nonnull_aux_columns(defining_query: &str) -> Vec<(String, String)> {
    parse_defining_query(defining_query)
        .map(|tree| tree.nonnull_aux_columns())
        .unwrap_or_default()
}

/// Check whether a defining query is an INTERSECT or EXCEPT that has the
/// legacy/private dual-count state shape (`__pgt_count_l`, `__pgt_count_r`).
pub fn query_needs_dual_count(defining_query: &str) -> bool {
    parse_defining_query(defining_query)
        .map(|tree| tree.needs_dual_count())
        .unwrap_or(false)
}

/// Check whether a defining query is a UNION (without ALL) that needs
/// deduplicated counting via a wrapped UNION ALL.
pub fn query_needs_union_dedup_count(defining_query: &str) -> bool {
    parse_defining_query(defining_query)
        .map(|tree| tree.needs_union_dedup_count())
        .unwrap_or(false)
}

/// Check whether the defining query contains a join whose output does not
/// include primary key columns from both sides.
///
/// When `true`, the `__pgt_row_id` hash computed from output columns cannot
/// uniquely identify every join result row. The storage index must be
/// non-unique and the keyless refresh strategy should be used.
pub fn query_has_incomplete_join_pk(defining_query: &str) -> bool {
    parse_defining_query(defining_query)
        .map(|tree| tree.has_incomplete_join_pk())
        .unwrap_or(false)
}

/// Extract GROUP BY column names from a defining query.
///
/// Returns `Some(["region", "category"])` for aggregate queries with
/// GROUP BY, `None` for non-aggregate or scalar-aggregate queries.
///
/// Uses a lightweight parse — no SPI or database access required.
pub fn extract_group_by_columns(defining_query: &str) -> Option<Vec<String>> {
    parse_defining_query(defining_query)
        .ok()
        .and_then(|tree| tree.group_by_columns())
}

/// Generate a SQL expression for computing `__pgt_row_id` from a subquery
/// aliased as `sub`, matching the hash formula used by the delta query.
///
/// Returns an expression like `pgtrickle.pg_trickle_hash(sub."id"::text)` for scan PK,
/// `pgtrickle.pg_trickle_hash(sub."region"::text)` for aggregate GROUP BY, etc.
///
/// Falls back to `pgtrickle.pg_trickle_hash(row_to_json(sub)::text)` for queries whose
/// row-id computation is too complex (joins, union all).
pub fn row_id_expr_for_query(defining_query: &str) -> String {
    let tree = parse_defining_query(defining_query).ok();
    let key_cols = tree.as_ref().and_then(|t| t.row_id_key_columns());

    match key_cols {
        Some(cols) if cols.len() == 1 => {
            format!(
                "pgtrickle.pg_trickle_hash(sub.{}::text)",
                diff::quote_ident(&cols[0]),
            )
        }
        Some(cols) if cols.len() > 1 => {
            let array_items: Vec<String> = cols
                .iter()
                .map(|c| format!("sub.{}::TEXT", diff::quote_ident(c)))
                .collect();
            crate::hash::build_composite_hash_expr(&array_items)
        }
        _ => {
            // Scalar aggregate (no GROUP BY): use singleton sentinel hash
            // matching the differential delta's __singleton_group row_id.
            // Without this, FULL refresh would use row_to_json hashing
            // while DIFF uses '__singleton_group', causing __pgt_row_id
            // mismatch and phantom row insertion.
            if tree.as_ref().is_some_and(is_scalar_aggregate_root) {
                "pgtrickle.pg_trickle_hash('__singleton_group')".to_string()
            } else {
                // Fallback for complex queries (joins, union all, etc.)
                // Include row_number() to disambiguate duplicate-content rows
                // (e.g., recursive CTEs with UNION ALL that reach the same
                // values via different derivation paths).
                "pgtrickle.pg_trickle_hash(row_to_json(sub)::text || '/' || row_number() OVER ()::text)"
                    .to_string()
            }
        }
    }
}

/// Build the INSERT body used to materialize a defining query during a FULL
/// refresh.  Set-operation tables use this direct shape so only the defining
/// query's columns are written; branch multiplicity state is not part of the
/// user-facing storage relation.
pub fn direct_full_refresh_insert_body(
    defining_query: &str,
    materialization_query: &str,
) -> String {
    let row_id_expr = row_id_expr_for_query(defining_query);
    direct_full_refresh_insert_body_with_row_id(&row_id_expr, materialization_query)
}

fn direct_full_refresh_insert_body_with_row_id(
    row_id_expr: &str,
    materialization_query: &str,
) -> String {
    format!("SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({materialization_query}) sub")
}

/// Check whether the root of an OpTree is a scalar aggregate (GROUP BY
/// with no columns). Looks through transparent wrappers (Filter, Project,
/// Subquery) to find the Aggregate node.
fn is_scalar_aggregate_root(tree: &parser::OpTree) -> bool {
    match tree {
        parser::OpTree::Aggregate { group_by, .. } => group_by.is_empty(),
        parser::OpTree::Filter { child, .. }
        | parser::OpTree::Project { child, .. }
        | parser::OpTree::Subquery { child, .. } => is_scalar_aggregate_root(child),
        _ => false,
    }
}

/// For UNION ALL queries, generate a full-refresh SELECT SQL that computes
/// per-branch child-prefixed row IDs matching the delta query's formula.
///
/// Returns `None` if the query is not a top-level UNION ALL or the branches
/// cannot be decomposed (e.g., a branch has no deterministic PK columns).
///
/// The returned SQL is a SELECT producing `__pgt_row_id` plus user columns,
/// ready to be prefixed with `INSERT INTO schema.table`.
pub fn try_union_all_refresh_sql(defining_query: &str) -> Option<String> {
    let branches = split_top_level_union_all(defining_query)?;

    let mut parts = Vec::new();
    for (i, branch_sql) in branches.iter().enumerate() {
        let idx = i + 1;
        // Parse the branch to determine its row-id key columns.
        let tree = parse_defining_query(branch_sql).ok()?;
        let key_cols = tree.row_id_key_columns()?;

        // Bail out if any key column is a parser-internal synthetic name
        // (e.g. "col_0" from target_alias_for_res_target for expression
        // columns like `val + 1`).  These names do not correspond to actual
        // SQL output column names and would cause "column sub.col_N does
        // not exist" errors at runtime.
        if key_cols.iter().any(|c| is_synthetic_column_name(c)) {
            return None;
        }

        // Build the child hash expression (same formula as the scan diff).
        let child_hash = if key_cols.len() == 1 {
            format!(
                "pgtrickle.pg_trickle_hash(sub.{}::text)",
                diff::quote_ident(&key_cols[0]),
            )
        } else {
            let items: Vec<String> = key_cols
                .iter()
                .map(|c| format!("sub.{}::TEXT", diff::quote_ident(c)))
                .collect();
            crate::hash::build_composite_hash_expr(&items)
        };

        // Wrap with branch prefix (matching diff_union_all's idx = i + 1).
        let row_id_expr = crate::hash::build_composite_hash_expr(&[
            format!("'{idx}'::TEXT"),
            format!("({child_hash})::TEXT"),
        ]);

        parts.push(format!(
            "SELECT {row_id_expr} AS __pgt_row_id, sub.* FROM ({branch_sql}) sub",
        ));
    }

    Some(parts.join("\nUNION ALL\n"))
}

/// Returns `true` if the column name is a parser-internal synthetic alias
/// generated by `target_alias_for_res_target` (e.g. `col_0`, `col_12`).
fn is_synthetic_column_name(name: &str) -> bool {
    name.strip_prefix("col_")
        .is_some_and(|suffix| !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()))
}

/// Split a SQL query on top-level `UNION ALL` boundaries.
///
/// Returns `None` if the query has no top-level UNION ALL.
/// Respects parentheses, single-quoted strings, and double-quoted identifiers.
fn split_top_level_union_all(query: &str) -> Option<Vec<String>> {
    let bytes = query.as_bytes();
    let len = bytes.len();
    let mut parts = Vec::new();
    let mut depth: i32 = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut last_split = 0;

    let mut i = 0;
    while i < len {
        let ch = bytes[i];

        if in_single_quote {
            if ch == b'\'' {
                if i + 1 < len && bytes[i + 1] == b'\'' {
                    i += 2; // escaped quote
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            if ch == b'"' {
                if i + 1 < len && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }

        match ch {
            b'\'' => in_single_quote = true,
            b'"' => in_double_quote = true,
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ if depth == 0
                && i + 5 <= len
                && bytes[i..i + 5].eq_ignore_ascii_case(b"UNION")
                && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_')) =>
            {
                // Skip whitespace after "UNION"
                let mut j = i + 5;
                while j < len && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                // Check for "ALL" keyword
                if j + 3 <= len
                    && bytes[j..j + 3].eq_ignore_ascii_case(b"ALL")
                    && (j + 3 >= len
                        || !(bytes[j + 3].is_ascii_alphanumeric() || bytes[j + 3] == b'_'))
                {
                    parts.push(query[last_split..i].trim().to_string());
                    last_split = j + 3;
                    i = j + 3;
                    continue;
                }
            }
            _ => {}
        }

        i += 1;
    }

    parts.push(query[last_split..].trim().to_string());

    if parts.len() >= 2 { Some(parts) } else { None }
}

/// Replace top-level `UNION` (without `ALL`) keywords with `UNION ALL`.
///
/// Returns `None` when no replaceable `UNION` is found.
/// Respects parentheses, single-quoted strings, and double-quoted identifiers.
fn replace_top_level_union_with_union_all(query: &str) -> Option<String> {
    let bytes = query.as_bytes();
    let len = bytes.len();
    let mut result = String::new();
    let mut depth: i32 = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut last_copy = 0;
    let mut found = false;

    let mut i = 0;
    while i < len {
        let ch = bytes[i];

        if in_single_quote {
            if ch == b'\'' {
                if i + 1 < len && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            if ch == b'"' {
                if i + 1 < len && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }

        match ch {
            b'\'' => in_single_quote = true,
            b'"' => in_double_quote = true,
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ if depth == 0
                // Look for UNION keyword at word boundary
                && i + 5 <= len
                && bytes[i..i + 5].eq_ignore_ascii_case(b"UNION")
                && (i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_'))
                && (i + 5 >= len
                    || !(bytes[i + 5].is_ascii_alphanumeric() || bytes[i + 5] == b'_')) =>
            {
                // Skip whitespace after UNION
                let mut j = i + 5;
                while j < len && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                // Check if NOT followed by ALL
                let has_all = j + 3 <= len
                    && bytes[j..j + 3].eq_ignore_ascii_case(b"ALL")
                    && (j + 3 >= len
                        || !(bytes[j + 3].is_ascii_alphanumeric() || bytes[j + 3] == b'_'));
                if !has_all {
                    // Insert " ALL" after UNION
                    result.push_str(&query[last_copy..i + 5]);
                    result.push_str(" ALL");
                    last_copy = i + 5;
                    found = true;
                }
            }
            _ => {}
        }

        i += 1;
    }

    if found {
        result.push_str(&query[last_copy..]);
        Some(result)
    } else {
        None
    }
}

/// For UNION (without ALL) queries, generate a full-refresh SELECT that
/// computes per-unique-row multiplicity counts across all branches by
/// converting `UNION` to `UNION ALL` and wrapping with `COUNT(*)`.
///
/// Returns `None` when the query does not contain a replaceable top-level
/// `UNION` keyword.
pub fn try_union_dedup_refresh_sql(
    defining_query: &str,
    column_names: &[String],
) -> Option<String> {
    let union_all_version = replace_top_level_union_with_union_all(defining_query)?;

    let quoted_cols: Vec<String> = column_names.iter().map(|c| diff::quote_ident(c)).collect();
    let sub_cols: Vec<String> = column_names
        .iter()
        .map(|c| format!("sub.{}", diff::quote_ident(c)))
        .collect();
    let col_list = sub_cols.join(", ");
    let group_list = sub_cols.join(", ");

    // Hash expression for __pgt_row_id (references outer sub2)
    let hash_items: Vec<String> = quoted_cols
        .iter()
        .map(|c| format!("sub2.{c}::TEXT"))
        .collect();
    let hash_expr = if hash_items.len() == 1 {
        format!("pgtrickle.pg_trickle_hash({})", hash_items[0])
    } else {
        crate::hash::build_composite_hash_expr(&hash_items)
    };

    let outer_cols: Vec<String> = quoted_cols.iter().map(|c| format!("sub2.{c}")).collect();
    let outer_col_list = outer_cols.join(", ");

    let sql = format!(
        "SELECT {hash_expr} AS __pgt_row_id, {outer_col_list}, sub2.__pgt_count\n\
         FROM (\n\
         \x20 SELECT {col_list}, COUNT(*) AS __pgt_count\n\
         \x20 FROM ({union_all_version}) sub\n\
         \x20 GROUP BY {group_list}\n\
         ) sub2"
    );

    Some(sql)
}

/// The kind and components of a top-level INTERSECT or EXCEPT operation.
#[derive(Debug, Clone, PartialEq)]
pub enum SetOpKind {
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

/// Result of splitting a query on a top-level set operation keyword.
#[derive(Debug, Clone)]
pub struct SetOpParts {
    pub kind: SetOpKind,
    pub left: String,
    pub right: String,
}

/// Split a SQL query on the **outermost** `INTERSECT [ALL]` or `EXCEPT [ALL]`
/// keyword. Returns `None` when no top-level set operation is found.
///
/// Respects parentheses, single-quoted strings, and double-quoted identifiers,
/// following the same approach as `split_top_level_union_all`.
fn split_top_level_set_op(query: &str) -> Option<SetOpParts> {
    let bytes = query.as_bytes();
    let len = bytes.len();
    let mut depth: i32 = 0;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    let mut i = 0;
    while i < len {
        let ch = bytes[i];

        if in_single_quote {
            if ch == b'\'' {
                if i + 1 < len && bytes[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                in_single_quote = false;
            }
            i += 1;
            continue;
        }
        if in_double_quote {
            if ch == b'"' {
                if i + 1 < len && bytes[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                in_double_quote = false;
            }
            i += 1;
            continue;
        }

        match ch {
            b'\'' => in_single_quote = true,
            b'"' => in_double_quote = true,
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ if depth == 0 => {
                let is_word_start =
                    i == 0 || !(bytes[i - 1].is_ascii_alphanumeric() || bytes[i - 1] == b'_');

                // Try INTERSECT (9 chars)
                if is_word_start
                    && i + 9 <= len
                    && bytes[i..i + 9].eq_ignore_ascii_case(b"INTERSECT")
                    && (i + 9 >= len
                        || !(bytes[i + 9].is_ascii_alphanumeric() || bytes[i + 9] == b'_'))
                {
                    let left = query[..i].trim().to_string();
                    let mut j = i + 9;
                    while j < len && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    let (kind, right_start) = if j + 3 <= len
                        && bytes[j..j + 3].eq_ignore_ascii_case(b"ALL")
                        && (j + 3 >= len
                            || !(bytes[j + 3].is_ascii_alphanumeric() || bytes[j + 3] == b'_'))
                    {
                        (SetOpKind::IntersectAll, j + 3)
                    } else {
                        (SetOpKind::Intersect, i + 9)
                    };
                    let right = query[right_start..].trim().to_string();
                    return Some(SetOpParts { kind, left, right });
                }

                // Try EXCEPT (6 chars)
                if is_word_start
                    && i + 6 <= len
                    && bytes[i..i + 6].eq_ignore_ascii_case(b"EXCEPT")
                    && (i + 6 >= len
                        || !(bytes[i + 6].is_ascii_alphanumeric() || bytes[i + 6] == b'_'))
                {
                    let left = query[..i].trim().to_string();
                    let mut j = i + 6;
                    while j < len && bytes[j].is_ascii_whitespace() {
                        j += 1;
                    }
                    let (kind, right_start) = if j + 3 <= len
                        && bytes[j..j + 3].eq_ignore_ascii_case(b"ALL")
                        && (j + 3 >= len
                            || !(bytes[j + 3].is_ascii_alphanumeric() || bytes[j + 3] == b'_'))
                    {
                        (SetOpKind::ExceptAll, j + 3)
                    } else {
                        (SetOpKind::Except, i + 6)
                    };
                    let right = query[right_start..].trim().to_string();
                    return Some(SetOpParts { kind, left, right });
                }
            }
            _ => {}
        }

        i += 1;
    }

    None
}

/// For INTERSECT / EXCEPT queries, generate a full-refresh SELECT that
/// computes per-branch multiplicity counts (`__pgt_count_l`, `__pgt_count_r`)
/// for the private state shape used by the differential operators.
///
/// Returns `None` when the query is not a top-level set operation.
pub fn try_set_op_refresh_sql(defining_query: &str, column_names: &[String]) -> Option<String> {
    let parts = split_top_level_set_op(defining_query)?;

    if column_names.is_empty() {
        return None;
    }

    let canonical_cols: Vec<String> = (1..=column_names.len())
        .map(|n| format!("__pgt_set_c{n}"))
        .collect();
    let canonical_col_list = canonical_cols.join(", ");
    let left_alias_list = canonical_col_list.clone();
    let right_alias_list = canonical_col_list.clone();
    let group_list = canonical_col_list.clone();
    let join_condition = canonical_cols
        .iter()
        .map(|c| format!("l.{c} IS NOT DISTINCT FROM r.{c}"))
        .collect::<Vec<_>>()
        .join(" AND ");

    // For FULL OUTER JOIN, columns from one side may be NULL.
    // Use COALESCE to pick from whichever side matched.
    let (select_cols, hash_items_final) = {
        let coalesced: Vec<String> = canonical_cols
            .iter()
            .zip(column_names)
            .map(|(canonical, output)| {
                format!(
                    "COALESCE(l.{canonical}, r.{canonical}) AS {output}",
                    canonical = canonical,
                    output = diff::quote_ident(output),
                )
            })
            .collect();
        let hash_items_c: Vec<String> = canonical_cols
            .iter()
            .map(|c| format!("COALESCE(l.{c}, r.{c})::TEXT"))
            .collect();
        (coalesced.join(",\n       "), hash_items_c)
    };

    let hash_expr_final = if hash_items_final.len() == 1 {
        format!("pgtrickle.pg_trickle_hash({})", hash_items_final[0])
    } else {
        crate::hash::build_composite_hash_expr(&hash_items_final)
    };

    let sql = format!(
        "WITH __pgt_set_branches AS (\n\
         \x20 SELECT {canonical_col_list}, 0::smallint AS __pgt_branch\n\
         \x20 FROM ({left}) AS __pgt_left_branch({left_alias_list})\n\
         \x20 UNION ALL\n\
         \x20 SELECT {canonical_col_list}, 1::smallint AS __pgt_branch\n\
         \x20 FROM ({right}) AS __pgt_right_branch({right_alias_list})\n\
         ),\n\
         __pgt_left AS (\n\
         \x20 SELECT {canonical_col_list}, COUNT(*) AS __cnt\n\
         \x20 FROM __pgt_set_branches\n\
         \x20 WHERE __pgt_branch = 0\n\
         \x20 GROUP BY {group_list}\n\
         ),\n\
         __pgt_right AS (\n\
         \x20 SELECT {canonical_col_list}, COUNT(*) AS __cnt\n\
         \x20 FROM __pgt_set_branches\n\
         \x20 WHERE __pgt_branch = 1\n\
         \x20 GROUP BY {group_list}\n\
         )\n\
         SELECT {hash_expr_final} AS __pgt_row_id,\n\
         \x20      {select_cols},\n\
         \x20      COALESCE(l.__cnt, 0) AS __pgt_count_l,\n\
         \x20      COALESCE(r.__cnt, 0) AS __pgt_count_r\n\
         FROM __pgt_left l\n\
         FULL OUTER JOIN __pgt_right r ON {join_condition}",
        left = parts.left,
        right = parts.right,
    );

    Some(sql)
}

/// Get output column names from a defining query by running it with LIMIT 0.
///
/// This works for all query types including recursive CTEs, since PostgreSQL
/// handles the full query execution (we just inspect the result metadata).
pub fn get_defining_query_columns(defining_query: &str) -> Result<Vec<String>, PgTrickleError> {
    use pgrx::Spi;

    let probe_sql = format!("SELECT * FROM ({defining_query}) __pgt_probe LIMIT 0");

    Spi::connect(|client| {
        let result = client
            .select(&probe_sql, None, &[])
            .map_err(|e| PgTrickleError::SpiError(format!("Column probe failed: {e}")))?;

        let ncols = result
            .columns()
            .map_err(|e| PgTrickleError::SpiError(format!("Failed to get column count: {e}")))?;

        if ncols == 0 {
            return Err(PgTrickleError::QueryParseError(
                "Defining query produces no columns".into(),
            ));
        }

        let mut columns = Vec::with_capacity(ncols);
        for i in 1..=ncols {
            let name = result
                .column_name(i)
                .unwrap_or_else(|_| format!("column_{i}"));
            columns.push(name);
        }

        Ok(columns)
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;
    use proptest::prelude::*;

    // ── split_top_level_union_all (existing) ────────────────────────

    #[test]
    fn test_split_union_all_simple() {
        let parts =
            split_top_level_union_all("SELECT id FROM t1 UNION ALL SELECT id FROM t2").unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "SELECT id FROM t1");
        assert_eq!(parts[1], "SELECT id FROM t2");
    }

    #[test]
    fn test_split_union_all_three_branches() {
        let parts = split_top_level_union_all(
            "SELECT a FROM t1 UNION ALL SELECT a FROM t2 UNION ALL SELECT a FROM t3",
        )
        .unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[2], "SELECT a FROM t3");
    }

    #[test]
    fn test_split_union_all_in_subquery_not_split() {
        // UNION ALL inside parens should NOT be split at the top level.
        let result = split_top_level_union_all("SELECT * FROM (SELECT 1 UNION ALL SELECT 2) sub");
        assert!(result.is_none());
    }

    #[test]
    fn test_split_union_all_case_insensitive() {
        let parts =
            split_top_level_union_all("SELECT id FROM t1 union all SELECT id FROM t2").unwrap();
        assert_eq!(parts.len(), 2);
    }

    #[test]
    fn test_split_union_all_with_extra_whitespace() {
        let parts =
            split_top_level_union_all("SELECT id FROM t1  UNION  ALL  SELECT id FROM t2").unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "SELECT id FROM t1");
        assert_eq!(parts[1], "SELECT id FROM t2");
    }

    #[test]
    fn test_split_no_union_all() {
        assert!(split_top_level_union_all("SELECT id FROM t1").is_none());
    }

    #[test]
    fn test_split_union_without_all_not_split() {
        // Plain UNION (without ALL) should not be split.
        assert!(split_top_level_union_all("SELECT id FROM t1 UNION SELECT id FROM t2").is_none());
    }

    #[test]
    fn test_split_union_all_preserves_quoted_strings() {
        let parts =
            split_top_level_union_all("SELECT 'UNION ALL' FROM t1 UNION ALL SELECT id FROM t2")
                .unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], "SELECT 'UNION ALL' FROM t1");
    }

    // ── hash_string() ───────────────────────────────────────────────

    #[test]
    fn test_hash_string_deterministic() {
        let h1 = hash_string("SELECT id FROM orders");
        let h2 = hash_string("SELECT id FROM orders");
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_hash_string_different_inputs_differ() {
        let h1 = hash_string("SELECT id FROM orders");
        let h2 = hash_string("SELECT id FROM items");
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_hash_string_empty() {
        // Should not panic; produces a valid u64
        let _ = hash_string("");
    }

    // ── resolve_delta_template() ────────────────────────────────────

    #[test]
    fn test_resolve_delta_template_single_oid() {
        let mut prev = Frontier::new();
        prev.set_source(42, "0/1000".to_string(), "ts".to_string());
        let mut new_f = Frontier::new();
        new_f.set_source(42, "0/2000".to_string(), "ts".to_string());

        let template = "SELECT * FROM changes WHERE lsn > '__PGS_PREV_LSN_42__' AND lsn <= '__PGS_NEW_LSN_42__'";
        let resolved = resolve_delta_template(template, &[42], &prev, &new_f).unwrap();
        assert!(resolved.contains("0/1000"));
        assert!(resolved.contains("0/2000"));
        assert!(!resolved.contains("__PGS_PREV_LSN_42__"));
        assert!(!resolved.contains("__PGS_NEW_LSN_42__"));
    }

    #[test]
    fn test_resolve_delta_template_multiple_oids() {
        let mut prev = Frontier::new();
        prev.set_source(10, "0/AA".to_string(), "ts".to_string());
        prev.set_source(20, "0/BB".to_string(), "ts".to_string());
        let mut new_f = Frontier::new();
        new_f.set_source(10, "0/CC".to_string(), "ts".to_string());
        new_f.set_source(20, "0/DD".to_string(), "ts".to_string());

        let template =
            "__PGS_PREV_LSN_10__ __PGS_NEW_LSN_10__ __PGS_PREV_LSN_20__ __PGS_NEW_LSN_20__";
        let resolved = resolve_delta_template(template, &[10, 20], &prev, &new_f).unwrap();
        assert_eq!(resolved, "0/AA 0/CC 0/BB 0/DD");
    }

    #[test]
    fn test_resolve_delta_template_no_placeholders() {
        let prev = Frontier::new();
        let new_f = Frontier::new();
        let resolved = resolve_delta_template("SELECT 1", &[], &prev, &new_f).unwrap();
        assert_eq!(resolved, "SELECT 1");
    }

    #[test]
    fn test_resolve_delta_template_missing_oid_defaults() {
        // OID 999 not in frontier — get_lsn returns "0/0"
        let prev = Frontier::new();
        let new_f = Frontier::new();
        let resolved =
            resolve_delta_template("__PGS_PREV_LSN_999__", &[999], &prev, &new_f).unwrap();
        assert_eq!(resolved, "0/0");
    }

    #[test]
    fn test_resolve_delta_template_pgt_placeholders() {
        let mut prev = Frontier::new();
        prev.set_st_source(7, "0/A000".to_string(), "ts".to_string());
        let mut new_f = Frontier::new();
        new_f.set_st_source(7, "0/B000".to_string(), "ts".to_string());

        let template = "SELECT * FROM changes_pgt_7 WHERE lsn > '__PGS_PREV_LSN_pgt_7__' AND lsn <= '__PGS_NEW_LSN_pgt_7__'";
        let resolved = resolve_delta_template(template, &[], &prev, &new_f).unwrap();
        assert!(resolved.contains("0/A000"));
        assert!(resolved.contains("0/B000"));
        assert!(!resolved.contains("__PGS_PREV_LSN_pgt_7__"));
        assert!(!resolved.contains("__PGS_NEW_LSN_pgt_7__"));
    }

    #[test]
    fn test_resolve_delta_template_mixed_oid_and_pgt() {
        let mut prev = Frontier::new();
        prev.set_source(42, "0/1000".to_string(), "ts".to_string());
        prev.set_st_source(5, "0/2000".to_string(), "ts".to_string());
        let mut new_f = Frontier::new();
        new_f.set_source(42, "0/3000".to_string(), "ts".to_string());
        new_f.set_st_source(5, "0/4000".to_string(), "ts".to_string());

        let template =
            "__PGS_PREV_LSN_42__ __PGS_NEW_LSN_42__ __PGS_PREV_LSN_pgt_5__ __PGS_NEW_LSN_pgt_5__";
        let resolved = resolve_delta_template(template, &[42], &prev, &new_f).unwrap();
        assert_eq!(resolved, "0/1000 0/3000 0/2000 0/4000");
    }

    // ── T-A41-2: check_no_remaining_placeholders() / placeholder validation ──

    /// T-A41-2a: Clean SQL returns Ok(()).
    #[test]
    fn test_check_no_remaining_placeholders_clean_sql() {
        let sql = "SELECT id FROM t WHERE lsn > '0/1000'::pg_lsn";
        assert!(check_no_remaining_placeholders(sql, "test").is_ok());
    }

    /// T-A41-2b: A remaining __PGS_*__ token returns Err(UnresolvedPlaceholder).
    #[test]
    fn test_check_no_remaining_placeholders_pgs_token() {
        let sql = "SELECT * FROM t WHERE lsn > '__PGS_PREV_LSN_42__'::pg_lsn";
        let err = check_no_remaining_placeholders(sql, "my_st").unwrap_err();
        match err {
            crate::error::PgTrickleError::UnresolvedPlaceholder { token, context } => {
                assert!(token.contains("__PGS_PREV_LSN_42__"), "token={token}");
                assert_eq!(context, "my_st");
            }
            other => panic!("expected UnresolvedPlaceholder, got {other:?}"),
        }
    }

    /// T-A41-2c: A remaining __PGT_*__ token returns Err(UnresolvedPlaceholder).
    #[test]
    fn test_check_no_remaining_placeholders_pgt_token() {
        let sql = "SELECT __PGT_ROWID_COL__ FROM t";
        let err = check_no_remaining_placeholders(sql, "ctx").unwrap_err();
        match err {
            crate::error::PgTrickleError::UnresolvedPlaceholder { token, .. } => {
                assert!(token.contains("__PGT_ROWID_COL__"), "token={token}");
            }
            other => panic!("expected UnresolvedPlaceholder, got {other:?}"),
        }
    }

    /// T-A41-2d: Mixed set — one resolved, one not — still returns Err.
    #[test]
    fn test_check_no_remaining_placeholders_mixed_resolved() {
        // OID 42 resolved, but OID 99 is still a placeholder.
        let sql = "lsn > '0/1000'::pg_lsn AND lsn <= '__PGS_NEW_LSN_99__'::pg_lsn";
        let err = check_no_remaining_placeholders(sql, "st").unwrap_err();
        match err {
            crate::error::PgTrickleError::UnresolvedPlaceholder { token, .. } => {
                assert!(token.contains("99"), "token={token}");
            }
            other => panic!("expected UnresolvedPlaceholder, got {other:?}"),
        }
    }

    /// T-A41-2e: Repeated placeholders — first one triggers the error.
    #[test]
    fn test_check_no_remaining_placeholders_repeated_token() {
        let sql = "__PGS_PREV_LSN_5__ ... __PGS_PREV_LSN_5__";
        assert!(check_no_remaining_placeholders(sql, "ctx").is_err());
    }

    /// T-A41-2f: resolve_delta_template returns Err when a placeholder is
    /// not covered by source_oids (unknown OID → token remains).
    #[test]
    fn test_resolve_delta_template_unknown_oid_returns_err() {
        let prev = Frontier::new();
        let new_f = Frontier::new();
        // OID 99999 is NOT in source_oids → placeholder is not substituted.
        let template = "__PGS_PREV_LSN_99999__";
        // OID 99999 IS in source_oids → gets substituted with "0/0" (default for missing frontier)
        // so this should succeed (empty frontier gives 0/0):
        let ok = resolve_delta_template(template, &[99999], &prev, &new_f);
        assert!(
            ok.is_ok(),
            "known oid with missing frontier defaults to 0/0"
        );

        // But if OID is NOT in source_oids at all, the placeholder stays raw:
        let err = resolve_delta_template(template, &[], &prev, &new_f);
        assert!(
            err.is_err(),
            "unknown oid not in source_oids leaves placeholder unresolved"
        );
    }

    /// T-A41-2g: pgt-prefixed placeholder that is not in any frontier returns Err.
    #[test]
    fn test_resolve_delta_template_unknown_pgt_oid_returns_err() {
        let prev = Frontier::new();
        let new_f = Frontier::new();
        // The pgt-extraction regex will find pgt_999, but prev/new frontiers
        // don't have that key — however the function substitutes a "0/0" default,
        // so this should SUCCEED (no remaining placeholder).
        let template = "__PGS_PREV_LSN_pgt_999__ ... __PGS_NEW_LSN_pgt_999__";
        let ok = resolve_delta_template(template, &[], &prev, &new_f);
        assert!(
            ok.is_ok(),
            "pgt placeholder with missing frontier defaults to 0/0 — no error"
        );
    }

    /// T-A41-2h: check_no_remaining_placeholders_for() with only __PGS_ prefix
    /// does NOT flag __PGT_PART_PRED__, which is a legitimate later-stage token.
    #[test]
    fn test_check_no_remaining_placeholders_for_pgs_only_ignores_pgt() {
        // Simulate the SQL that resolve_lsn_placeholders produces: all __PGS_*__
        // tokens are gone, but __PGT_PART_PRED__ remains (resolved later).
        let sql = "MERGE INTO st USING d ON st.id = d.id __PGT_PART_PRED__";
        let result = check_no_remaining_placeholders_for(sql, "test", &["__PGS_"]);
        assert!(
            result.is_ok(),
            "checking only __PGS_ should ignore __PGT_PART_PRED__"
        );
    }

    /// T-A41-2i: check_no_remaining_placeholders (both families) DOES catch
    /// __PGT_PART_PRED__ when called in a context that resolves all __PGT__s.
    #[test]
    fn test_check_no_remaining_placeholders_both_catches_pgt_part_pred() {
        let sql = "MERGE INTO st USING d ON st.id = d.id __PGT_PART_PRED__";
        let result = check_no_remaining_placeholders(sql, "full_check");
        assert!(result.is_err(), "full check should catch __PGT_PART_PRED__");
    }

    // ── is_scan_chain_tree() ────────────────────────────────────────

    #[test]
    fn test_is_scan_chain_bare_scan() {
        let s = scan(1, "t", "public", "t", &["id"]);
        assert!(is_scan_chain_tree(&s));
    }

    #[test]
    fn test_is_scan_chain_project_over_scan() {
        let s = scan(1, "t", "public", "t", &["id", "name"]);
        let p = project(vec![colref("id")], vec!["id"], s);
        assert!(is_scan_chain_tree(&p));
    }

    #[test]
    fn test_is_scan_chain_subquery_over_scan() {
        let s = scan(1, "t", "public", "t", &["id"]);
        let sq = subquery("sub", vec!["id"], s);
        assert!(is_scan_chain_tree(&sq));
    }

    #[test]
    fn test_is_scan_chain_filter_is_false() {
        // Filter is NOT part of a scan chain (see doc comment)
        let s = scan(1, "t", "public", "t", &["id", "val"]);
        let f = filter(binop(">", colref("val"), lit("10")), s);
        assert!(!is_scan_chain_tree(&f));
    }

    #[test]
    fn test_is_scan_chain_aggregate_is_false() {
        let s = scan(1, "t", "public", "t", &["id", "amount"]);
        let agg = aggregate(vec![colref("id")], vec![sum_col("amount", "total")], s);
        assert!(!is_scan_chain_tree(&agg));
    }

    #[test]
    fn test_is_scan_chain_join_is_false() {
        let l = scan(1, "t1", "public", "t1", &["id"]);
        let r = scan(2, "t2", "public", "t2", &["id"]);
        let j = inner_join(eq_cond("t1", "id", "t2", "id"), l, r);
        assert!(!is_scan_chain_tree(&j));
    }

    #[test]
    fn test_is_scan_chain_distinct_is_false() {
        let s = scan(1, "t", "public", "t", &["id"]);
        let d = distinct(s);
        assert!(!is_scan_chain_tree(&d));
    }

    // ── Cache ops: invalidate / get / is_deduplicated ───────────────

    #[test]
    fn test_cache_empty_returns_none() {
        let pgt_id = -9999;
        invalidate_delta_cache(pgt_id); // ensure clean
        assert!(get_delta_sql_template(pgt_id).is_none());
        assert!(!is_delta_deduplicated(pgt_id));
    }

    #[test]
    fn test_cache_insert_and_retrieve() {
        let pgt_id = -9998;
        let entry = CachedDeltaTemplate {
            defining_query_hash: 12345,
            delta_sql_template: "WITH cte AS (SELECT 1) SELECT * FROM cte".to_string(),
            output_columns: vec!["id".to_string()],
            source_oids: vec![42],
            is_deduplicated: true,
            has_key_changed: false,
            is_all_algebraic: false,
            last_used: 0,
        };
        DELTA_TEMPLATE_CACHE.with(|cache| {
            cache.borrow_mut().insert(pgt_id, entry);
        });

        let tmpl = get_delta_sql_template(pgt_id).unwrap();
        assert!(tmpl.contains("SELECT 1"));
        assert!(is_delta_deduplicated(pgt_id));

        // Cleanup
        invalidate_delta_cache(pgt_id);
        assert!(get_delta_sql_template(pgt_id).is_none());
    }

    #[test]
    fn test_cache_invalidate_removes_entry() {
        let pgt_id = -9997;
        let entry = CachedDeltaTemplate {
            defining_query_hash: 0,
            delta_sql_template: "SELECT 1".to_string(),
            output_columns: vec![],
            source_oids: vec![],
            is_deduplicated: false,
            has_key_changed: false,
            is_all_algebraic: false,
            last_used: 0,
        };
        DELTA_TEMPLATE_CACHE.with(|cache| {
            cache.borrow_mut().insert(pgt_id, entry);
        });
        assert!(get_delta_sql_template(pgt_id).is_some());

        invalidate_delta_cache(pgt_id);
        assert!(get_delta_sql_template(pgt_id).is_none());
        assert!(!is_delta_deduplicated(pgt_id));
    }

    #[test]
    fn test_cache_has_key_changed_returns_correct_value() {
        let pgt_id = -9996;
        invalidate_delta_cache(pgt_id);
        // Default when not cached:
        assert!(!delta_has_key_changed(pgt_id));

        // Insert with has_key_changed = true
        let entry = CachedDeltaTemplate {
            defining_query_hash: 0,
            delta_sql_template: "SELECT 1".to_string(),
            output_columns: vec![],
            source_oids: vec![],
            is_deduplicated: true,
            has_key_changed: true,
            is_all_algebraic: false,
            last_used: 0,
        };
        DELTA_TEMPLATE_CACHE.with(|cache| {
            cache.borrow_mut().insert(pgt_id, entry);
        });
        assert!(delta_has_key_changed(pgt_id));

        // Cleanup
        invalidate_delta_cache(pgt_id);
        assert!(!delta_has_key_changed(pgt_id));
    }

    // ── OpTree::needs_pgt_count() (unit, no PG parse) ──────────────

    #[test]
    fn test_needs_pgt_count_aggregate() {
        let s = scan(1, "t", "public", "t", &["id", "amount"]);
        let agg = aggregate(vec![colref("id")], vec![sum_col("amount", "total")], s);
        assert!(agg.needs_pgt_count());
    }

    #[test]
    fn test_needs_pgt_count_distinct() {
        let s = scan(1, "t", "public", "t", &["id"]);
        let d = distinct(s);
        assert!(d.needs_pgt_count());
    }

    #[test]
    fn test_needs_pgt_count_scan_false() {
        let s = scan(1, "t", "public", "t", &["id"]);
        assert!(!s.needs_pgt_count());
    }

    // ── split_top_level_set_op ──────────────────────────────────────

    #[test]
    fn test_split_set_op_intersect() {
        let parts =
            split_top_level_set_op("SELECT val FROM t1 INTERSECT SELECT val FROM t2").unwrap();
        assert_eq!(parts.kind, SetOpKind::Intersect);
        assert_eq!(parts.left, "SELECT val FROM t1");
        assert_eq!(parts.right, "SELECT val FROM t2");
    }

    #[test]
    fn test_split_set_op_intersect_all() {
        let parts =
            split_top_level_set_op("SELECT val FROM t1 INTERSECT ALL SELECT val FROM t2").unwrap();
        assert_eq!(parts.kind, SetOpKind::IntersectAll);
        assert_eq!(parts.left, "SELECT val FROM t1");
        assert_eq!(parts.right, "SELECT val FROM t2");
    }

    #[test]
    fn test_split_set_op_except() {
        let parts = split_top_level_set_op("SELECT val FROM t1 EXCEPT SELECT val FROM t2").unwrap();
        assert_eq!(parts.kind, SetOpKind::Except);
        assert_eq!(parts.left, "SELECT val FROM t1");
        assert_eq!(parts.right, "SELECT val FROM t2");
    }

    #[test]
    fn test_split_set_op_except_all() {
        let parts =
            split_top_level_set_op("SELECT val FROM t1 EXCEPT ALL SELECT val FROM t2").unwrap();
        assert_eq!(parts.kind, SetOpKind::ExceptAll);
        assert_eq!(parts.left, "SELECT val FROM t1");
        assert_eq!(parts.right, "SELECT val FROM t2");
    }

    #[test]
    fn test_split_set_op_case_insensitive() {
        let parts =
            split_top_level_set_op("SELECT val FROM t1 intersect SELECT val FROM t2").unwrap();
        assert_eq!(parts.kind, SetOpKind::Intersect);
    }

    #[test]
    fn test_split_set_op_inside_parens_not_split() {
        let result = split_top_level_set_op("SELECT * FROM (SELECT 1 INTERSECT SELECT 2) sub");
        assert!(result.is_none());
    }

    #[test]
    fn test_split_set_op_no_set_op() {
        assert!(split_top_level_set_op("SELECT id FROM t1").is_none());
    }

    #[test]
    fn test_split_set_op_parenthesized_left() {
        let parts = split_top_level_set_op(
            "(SELECT val FROM t1 UNION ALL SELECT val FROM t2) EXCEPT SELECT val FROM t3",
        )
        .unwrap();
        assert_eq!(parts.kind, SetOpKind::Except);
        assert_eq!(
            parts.left,
            "(SELECT val FROM t1 UNION ALL SELECT val FROM t2)"
        );
        assert_eq!(parts.right, "SELECT val FROM t3");
    }

    #[test]
    fn test_split_set_op_preserves_quoted_strings() {
        let parts =
            split_top_level_set_op("SELECT 'INTERSECT' FROM t1 INTERSECT SELECT val FROM t2")
                .unwrap();
        assert_eq!(parts.kind, SetOpKind::Intersect);
        assert_eq!(parts.left, "SELECT 'INTERSECT' FROM t1");
    }

    #[test]
    fn test_set_op_refresh_sql_binds_branches_positionally_and_null_safely() {
        let columns = vec!["left_name".to_string(), "value".to_string()];
        let sql = try_set_op_refresh_sql(
            "SELECT a AS left_name, b FROM left_t \
             INTERSECT ALL \
             SELECT x AS right_name, y FROM right_t",
            &columns,
        )
        .unwrap();

        assert!(sql.contains(
            "FROM (SELECT a AS left_name, b FROM left_t) \
             AS __pgt_left_branch(__pgt_set_c1, __pgt_set_c2)"
        ));
        assert!(sql.contains(
            "FROM (SELECT x AS right_name, y FROM right_t) \
             AS __pgt_right_branch(__pgt_set_c1, __pgt_set_c2)"
        ));
        assert!(sql.contains("l.__pgt_set_c1 IS NOT DISTINCT FROM r.__pgt_set_c1"));
        assert!(sql.contains("l.__pgt_set_c2 IS NOT DISTINCT FROM r.__pgt_set_c2"));
        assert!(sql.contains("UNION ALL"));
        assert!(!sql.contains(" USING ("));
    }

    #[test]
    fn test_direct_full_refresh_insert_body_has_no_set_state_columns() {
        let sql = direct_full_refresh_insert_body_with_row_id(
            "pgtrickle.pg_trickle_hash(sub.value::text)",
            "SELECT value FROM left_t INTERSECT SELECT value FROM right_t",
        );

        assert!(sql.contains("AS __pgt_row_id"));
        assert!(sql.contains("sub.*"));
        assert!(!sql.contains("__pgt_count_l"));
        assert!(!sql.contains("__pgt_count_r"));
    }

    // ── is_scalar_aggregate_root() ─────────────────────────────────

    #[test]
    fn test_scalar_aggregate_root_bare() {
        let s = scan(1, "t", "public", "t", &["id", "amount"]);
        let agg = aggregate(vec![], vec![sum_col("amount", "total")], s);
        assert!(is_scalar_aggregate_root(&agg));
    }

    #[test]
    fn test_scalar_aggregate_root_with_filter() {
        let s = scan(1, "t", "public", "t", &["id", "amount"]);
        let f = filter(binop(">", colref("amount"), lit("0")), s);
        let agg = aggregate(vec![], vec![sum_col("amount", "total")], f);
        // Aggregate is root, Filter is child — scalar agg root should be true
        assert!(is_scalar_aggregate_root(&agg));
    }

    #[test]
    fn test_scalar_aggregate_root_through_project() {
        let s = scan(1, "t", "public", "t", &["id", "amount"]);
        let agg = aggregate(vec![], vec![sum_col("amount", "total")], s);
        let p = project(vec![colref("total")], vec!["revenue"], agg);
        // Project wraps the Aggregate — should see through
        assert!(is_scalar_aggregate_root(&p));
    }

    #[test]
    fn test_not_scalar_aggregate_with_group_by() {
        let s = scan(1, "t", "public", "t", &["id", "amount"]);
        let agg = aggregate(vec![colref("id")], vec![sum_col("amount", "total")], s);
        assert!(!is_scalar_aggregate_root(&agg));
    }

    #[test]
    fn test_not_scalar_aggregate_scan() {
        let s = scan(1, "t", "public", "t", &["id"]);
        assert!(!is_scalar_aggregate_root(&s));
    }

    // ── P2 property / fuzz tests ──────────────────────────────────────────

    proptest! {
        #[test]
        fn prop_split_top_level_union_all_no_panic(input in ".*") {
            let _ = split_top_level_union_all(&input);
        }

        #[test]
        fn prop_split_top_level_set_op_no_panic(input in ".*") {
            let _ = split_top_level_set_op(&input);
        }
    }
}
