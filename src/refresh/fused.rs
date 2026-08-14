//! PERF-2 (v0.63.0): Fused multi-node refresh engine.
//!
//! Composes per-node delta SQL + MERGE statements into a single
//! `WITH … MERGE; MERGE; …` CTE chain, reducing round-trips and enabling
//! the PostgreSQL planner to share sub-expressions across multiple nodes
//! in the same topological batch.
//!
//! # SQL Pattern
//!
//! Given two nodes A and B (in topological order), the output is:
//!
//! ```sql
//! WITH
//!   __pgt_cte_scan_1 AS MATERIALIZED (SELECT ... FROM pgtrickle_changes.changes_X ...),
//!   __pgt_cte_scan_2 AS (...),            -- A's delta CTEs
//!   _apply_<pgt_id_A> AS (
//!     MERGE INTO "schema"."A" AS st
//!     USING __pgt_cte_scan_2 AS d
//!     ON st.__pgt_row_id = d.__pgt_row_id
//!     WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE
//!     WHEN MATCHED AND d.__pgt_action = 'I' AND (...) THEN UPDATE SET ...
//!     WHEN NOT MATCHED AND d.__pgt_action = 'I' THEN INSERT (...) VALUES (...)
//!     RETURNING 1
//!   ),
//!   __pgt_cte_scan_100 AS MATERIALIZED (SELECT ... FROM pgtrickle_changes.changes_Y ...),
//!   __pgt_cte_scan_101 AS (...),           -- B's delta CTEs (renumbered)
//! MERGE INTO "schema"."B" AS st
//! USING __pgt_cte_scan_101 AS d
//! ON st.__pgt_row_id = d.__pgt_row_id
//! ...;
//! ```
//!
//! # CTE Deduplication
//!
//! Source-delta CTEs whose SQL bodies are byte-identical after normalisation
//! (same source OID, same LSN bounds) are emitted only once.  Subsequent
//! references to the deduplicated CTE use the first occurrence's name.
//!
//! # Name Collision Prevention
//!
//! CTEs from different nodes are renumbered using a per-node counter offset
//! (node index × 100).  Node 0 keeps its original `__pgt_cte_*_N` names;
//! node 1 gets `__pgt_cte_*_100`, `__pgt_cte_*_101`, …; node 2 gets
//! `__pgt_cte_*_200`, etc.

use std::collections::HashMap;

use crate::error::PgTrickleError;

/// Per-node counter block size for name-collision prevention.
const CTE_OFFSET_BLOCK: usize = 100;

/// Per-node input to the fused refresh engine.
///
/// Each `NodeSpec` corresponds to one stream-table node in a topological
/// refresh batch.  The `delta_sql` field must contain the fully-resolved
/// delta SQL for the node (all `__PGS_*__` LSN placeholder tokens already
/// substituted with concrete values).
#[derive(Debug, Clone)]
pub struct NodeSpec {
    /// Stream table `pgt_id` — used to name the wrapping apply CTE.
    pub pgt_id: i64,
    /// Quoted table reference, e.g. `"public"."my_view"`.
    pub quoted_table: String,
    /// Fully-resolved delta SQL.
    ///
    /// Format (as emitted by the DVM engine):
    /// ```sql
    /// WITH __pgt_cte_scan_1 AS MATERIALIZED (...), ..., __pgt_cte_scan_N AS (...)
    /// SELECT * FROM __pgt_cte_scan_N
    /// ```
    /// LSN placeholders must already be replaced with concrete values before
    /// constructing a `NodeSpec`.
    pub delta_sql: String,
    /// User-facing column names (excluding `__pgt_row_id` and `__pgt_action`).
    pub user_cols: Vec<String>,
    /// `true` when the stream table has a partition key column.
    ///
    /// When set, the MERGE statement includes a `__PGT_PART_PRED__`-style
    /// predicate on the partition key; for the fused path this placeholder
    /// is stripped (no partition routing needed in the single-statement path).
    pub has_partition_key: bool,
}

/// Output of [`fuse_diff_batch`].
#[derive(Debug, Clone)]
pub struct FusedOutput {
    /// The composed SQL statement — a single `WITH … MERGE` chain.
    pub sql: String,
    /// Number of stream-table nodes included in this batch.
    pub node_count: usize,
}

// ── Internal CTE representation ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct CteEntry {
    name: String,
    body: String,
    is_recursive: bool,
    is_materialized: bool,
}

// ── CTE parser ──────────────────────────────────────────────────────────

/// Find the end index of the balanced `(…)` span starting at `offset` within `bytes`.
///
/// Handles single-quoted string literals (including `''` escape sequences) and
/// double-quoted identifiers so that parentheses inside them are not counted.
///
/// Returns `None` if `bytes[offset] != b'('` or the string is unterminated.
fn balanced_paren_end(bytes: &[u8], offset: usize) -> Option<usize> {
    if bytes.get(offset)? != &b'(' {
        return None;
    }
    let mut depth = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut i = offset;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_double => {
                if in_single && bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                in_single = !in_single;
            }
            b'"' if !in_single => {
                in_double = !in_double;
            }
            b'(' if !in_single && !in_double => depth += 1,
            b')' if !in_single && !in_double => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Check whether `s` (case-insensitively) starts with a keyword `kw` followed by
/// a space, tab, newline, or `(`.
fn starts_kw(s: &str, kw: &str) -> bool {
    let upper = s.to_ascii_uppercase();
    if !upper.starts_with(kw) {
        return false;
    }
    matches!(
        s.as_bytes().get(kw.len()),
        Some(b' ' | b'\t' | b'\n' | b'\r' | b'(')
    )
}

/// Parse a delta SQL string produced by the DVM engine into a list of CTE
/// definitions and the final bare SELECT.
///
/// # Format
///
/// The DVM engine always produces SQL of the form:
/// ```text
/// WITH [RECURSIVE] name1 AS [MATERIALIZED] (body1),
///                  name2 AS (body2),
///                  …
/// SELECT * FROM nameN
/// ```
///
/// A bare `SELECT * FROM …` (no CTEs) is also accepted.
///
/// # Returns
///
/// `(ctes, final_select)` where `ctes` is a list of
/// `(name, body, is_recursive, is_materialized)` tuples and `final_select`
/// is the trailing `SELECT * FROM …` string.
pub(crate) fn parse_delta_ctes(sql: &str) -> (Vec<CteEntry>, String) {
    let s = sql.trim();
    let upper = s.to_ascii_uppercase();

    // Detect `WITH [RECURSIVE]` prefix.
    let (is_recursive, after_with) = if upper.starts_with("WITH RECURSIVE ") {
        (true, &s["WITH RECURSIVE ".len()..])
    } else if upper.starts_with("WITH ") {
        (false, &s["WITH ".len()..])
    } else {
        // No CTE prefix — the whole thing is the final SELECT.
        return (vec![], s.to_string());
    };

    let mut ctes: Vec<CteEntry> = Vec::new();
    let mut pos = after_with.trim_start().as_ptr() as usize - s.as_ptr() as usize;

    loop {
        let remaining = &s[pos..];
        if remaining.trim_start().is_empty() {
            break;
        }

        // Extract CTE name (identifier: runs until whitespace or '(').
        let r = remaining.trim_start();
        let pos_adj = remaining.len() - r.len();
        pos += pos_adj;
        let remaining = r;

        let name_len = remaining
            .find(|c: char| c.is_whitespace())
            .unwrap_or(remaining.len());
        let cte_name = remaining[..name_len].to_string();
        pos += name_len;

        // Skip to "AS".
        let after_name = s[pos..].trim_start();
        let skip = s[pos..].len() - after_name.len();
        pos += skip;
        if !starts_kw(s[pos..].trim_start(), "AS") {
            // Not a CTE definition — fall through to final select.
            break;
        }
        pos += 2; // skip "AS"

        // Skip optional whitespace.
        let rest = s[pos..].trim_start();
        let skip = s[pos..].len() - rest.len();
        pos += skip;

        // Detect MATERIALIZED / NOT MATERIALIZED.
        let upper_rest = s[pos..].to_ascii_uppercase();
        let (is_materialized, extra_skip) = if upper_rest.starts_with("NOT MATERIALIZED") {
            (false, "NOT MATERIALIZED".len())
        } else if upper_rest.starts_with("MATERIALIZED") {
            (true, "MATERIALIZED".len())
        } else {
            (false, 0)
        };
        pos += extra_skip;

        // Skip whitespace before `(`.
        let rest = s[pos..].trim_start();
        let skip = s[pos..].len() - rest.len();
        pos += skip;

        // Expect `(`.
        let end = match balanced_paren_end(s.as_bytes(), pos) {
            Some(e) => e,
            None => break,
        };
        let body = s[pos + 1..end].trim().to_string();
        pos = end + 1;

        ctes.push(CteEntry {
            name: cte_name,
            body,
            is_recursive,
            is_materialized,
        });

        // Skip optional comma + whitespace.
        let rest = s[pos..].trim_start();
        let skip = s[pos..].len() - rest.len();
        pos += skip;
        if s[pos..].starts_with(',') {
            pos += 1;
        } else {
            // No comma — next is the final SELECT.
            break;
        }
    }

    let final_select = s[pos..].trim().to_string();
    (ctes, final_select)
}

/// Rename all occurrences of each key in `remap` to its value within `sql`.
///
/// Only whole-identifier occurrences are replaced: a match is valid when
/// it is preceded and followed by a non-identifier character (or the string
/// boundary).  SQL identifiers consist of `[A-Za-z0-9_]` characters.
pub(crate) fn rename_ctes(sql: &str, remap: &HashMap<String, String>) -> String {
    if remap.is_empty() {
        return sql.to_string();
    }
    let mut result = sql.to_string();
    for (old, new) in remap {
        // Use a simple scan-and-replace approach that respects identifier boundaries.
        let mut out = String::with_capacity(result.len());
        let mut search_from = 0usize;
        while let Some(pos) = result[search_from..].find(old.as_str()) {
            let abs_pos = search_from + pos;
            // Check left boundary.
            let left_ok = abs_pos == 0 || {
                let c = result.as_bytes()[abs_pos - 1];
                !c.is_ascii_alphanumeric() && c != b'_'
            };
            // Check right boundary.
            let right_ok = abs_pos + old.len() >= result.len() || {
                let c = result.as_bytes()[abs_pos + old.len()];
                !c.is_ascii_alphanumeric() && c != b'_'
            };
            if left_ok && right_ok {
                out.push_str(&result[search_from..abs_pos]);
                out.push_str(new.as_str());
                search_from = abs_pos + old.len();
            } else {
                out.push_str(&result[search_from..abs_pos + 1]);
                search_from = abs_pos + 1;
            }
        }
        out.push_str(&result[search_from..]);
        result = out;
    }
    result
}

// ── Merge SQL builder ───────────────────────────────────────────────────

/// Build a minimal MERGE SQL statement for the fused path.
///
/// The `using_cte` argument is the **bare CTE name** (no parentheses) of
/// the delta CTE that should be used as the MERGE source.
fn build_fused_merge(node: &NodeSpec, using_cte: &str) -> String {
    use super::codegen::{
        build_is_distinct_clause, format_col_list, format_prefixed_col_list, format_update_set,
    };
    let user_col_list = format_col_list(&node.user_cols);
    let d_user_col_list = format_prefixed_col_list("d", &node.user_cols);
    let update_set = format_update_set(&node.user_cols);
    let is_distinct = build_is_distinct_clause(&node.user_cols);
    // Partition-key predicate: strip in fused path (no partition routing).
    let _ = node.has_partition_key;
    format!(
        "MERGE INTO {qt} AS st \
         USING {using_cte} AS d \
         ON st.__pgt_row_id = d.__pgt_row_id \
         WHEN MATCHED AND d.__pgt_action = 'D' THEN DELETE \
         WHEN MATCHED AND d.__pgt_action = 'I' AND ({is_distinct}) THEN \
           UPDATE SET {update_set} \
         WHEN NOT MATCHED AND d.__pgt_action = 'I' THEN \
           INSERT (__pgt_row_id, {user_col_list}) \
           VALUES (d.__pgt_row_id, {d_user_col_list})",
        qt = node.quoted_table,
    )
}

// ── Public API ──────────────────────────────────────────────────────────

/// Compose delta SQL for multiple nodes into a single CTE-fused MERGE chain.
///
/// Nodes must be supplied in **topological order** (upstream nodes before
/// downstream nodes).  The function:
///
/// 1. Parses each node's `delta_sql` into a list of CTE definitions and
///    a final `SELECT * FROM …` expression.
/// 2. Renames each CTE to a per-node namespace to prevent collisions.
/// 3. Deduplicates CTEs whose bodies are byte-identical (shared source-delta
///    CTEs with the same LSN bounds).
/// 4. Lifts all CTEs into a shared top-level `WITH` block.
/// 5. Wraps every MERGE in an apply CTE with `RETURNING merge_action()`.
/// 6. Returns one aggregated `(pgt_id, merge_action, count)` row per action.
///
/// Returns `Err` only if `nodes` is empty.
pub fn fuse_diff_batch(nodes: &[NodeSpec]) -> Result<FusedOutput, PgTrickleError> {
    if nodes.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "fuse_diff_batch: empty node list".into(),
        ));
    }

    let node_count = nodes.len();

    // ── Phase 1: Parse + rename CTEs for each node ───────────────────────
    struct ParsedNode {
        /// CTEs after renaming (no collisions with other nodes).
        ctes: Vec<CteEntry>,
        /// The final `SELECT * FROM <cte>` expression (may reference a renamed CTE).
        final_select: String,
        /// Name of the last CTE produced (i.e. the delta result CTE).
        final_cte_name: String,
        /// Bare pgt_id — used to name the wrapping apply CTE.
        pgt_id: i64,
    }

    // body_hash → canonical CTE name (deduplication registry).
    let mut seen_bodies: HashMap<String, String> = HashMap::new();
    // Shared CTE pool (in emit order).
    let mut cte_pool: Vec<CteEntry> = Vec::new();

    let mut parsed: Vec<ParsedNode> = Vec::with_capacity(node_count);

    for (node_idx, node) in nodes.iter().enumerate() {
        let (raw_ctes, final_select) = parse_delta_ctes(&node.delta_sql);

        // Build a remap table: original name → namespaced name.
        let offset = node_idx * CTE_OFFSET_BLOCK;
        let mut remap: HashMap<String, String> = HashMap::with_capacity(raw_ctes.len());

        for cte in &raw_ctes {
            // Determine the new name for this CTE, offset by node index.
            let new_name = if node_idx == 0 {
                cte.name.clone()
            } else {
                rename_with_offset(&cte.name, offset)
            };
            remap.insert(cte.name.clone(), new_name);
        }

        // Apply remaps to CTE bodies + final select.
        let mut node_ctes: Vec<CteEntry> = Vec::with_capacity(raw_ctes.len());
        for cte in raw_ctes {
            let new_name = remap[&cte.name].clone();
            let new_body = rename_ctes(&cte.body, &remap);
            node_ctes.push(CteEntry {
                name: new_name,
                body: new_body,
                is_recursive: cte.is_recursive,
                is_materialized: cte.is_materialized,
            });
        }
        let renamed_final_select = rename_ctes(&final_select, &remap);

        // Phase 2: Deduplicate and add to shared pool.
        // For each node CTE: if its body already exists in the pool, reuse the
        // existing name.  Otherwise, add it.
        let mut dedup_remap: HashMap<String, String> = HashMap::new();

        for cte in node_ctes {
            if let Some(existing_name) = seen_bodies.get(&cte.body) {
                // Deduplicated: reuse existing CTE name.
                dedup_remap.insert(cte.name.clone(), existing_name.clone());
            } else {
                // New CTE body — add to pool.
                seen_bodies.insert(cte.body.clone(), cte.name.clone());
                cte_pool.push(cte.clone());
                // No remap needed (keep the new name).
            }
        }

        // Apply dedup remap to the final_select.
        let final_select_dedup = rename_ctes(&renamed_final_select, &dedup_remap);

        // Extract the final CTE name from the final_select.
        // The DVM engine always produces `SELECT * FROM <cte_name>`.
        let final_cte_name = extract_final_cte_name(&final_select_dedup)
            .unwrap_or_else(|| format!("__pgt_f{node_idx}_delta"));

        parsed.push(ParsedNode {
            ctes: vec![], // already added to pool
            final_select: final_select_dedup,
            final_cte_name,
            pgt_id: node.pgt_id,
        });
        // Suppress unused warning — we only need the push above; `ctes` is moved into the pool.
        let _last = parsed.last().map(|p| p.ctes.len());
    }

    // ── Phase 3: Compose the SQL ─────────────────────────────────────────

    let has_recursive = cte_pool.iter().any(|c| c.is_recursive);
    let with_kw = if has_recursive {
        "WITH RECURSIVE"
    } else {
        "WITH"
    };

    let mut cte_defs: Vec<String> = Vec::with_capacity(cte_pool.len() + node_count - 1);

    // Emit the shared CTE pool first.
    for cte in &cte_pool {
        let def = if cte.is_materialized {
            format!("{} AS MATERIALIZED (\n{}\n)", cte.name, cte.body)
        } else {
            format!("{} AS (\n{}\n)", cte.name, cte.body)
        };
        cte_defs.push(def);
    }

    // Emit one data-modifying apply CTE and one action-count CTE per node.
    for (pnode, node) in parsed.iter().zip(nodes.iter()) {
        let merge_sql = build_fused_merge(node, &pnode.final_cte_name);
        let apply_cte_name = format!("_apply_{}", pnode.pgt_id);
        cte_defs.push(format!(
            "{apply_cte_name} AS (\n{merge_sql}\nRETURNING merge_action() AS __pgt_merge_action\n)"
        ));
        cte_defs.push(format!(
            "__pgt_fused_counts_{} AS (\n  SELECT {}::bigint AS pgt_id, \
                    __pgt_merge_action AS merge_action, \
                    count(*)::bigint AS action_count \
             FROM {} GROUP BY __pgt_merge_action\n)",
            pnode.pgt_id, pnode.pgt_id, apply_cte_name,
        ));
    }

    let count_selects = parsed
        .iter()
        .map(|node| {
            format!(
                "SELECT pgt_id, merge_action, action_count FROM __pgt_fused_counts_{}",
                node.pgt_id
            )
        })
        .collect::<Vec<_>>()
        .join("\nUNION ALL\n");

    let sql = if cte_defs.is_empty() {
        count_selects
    } else {
        format!("{with_kw} {}\n{count_selects}", cte_defs.join(",\n"))
    };

    Ok(FusedOutput { sql, node_count })
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Extract the CTE name from a `SELECT * FROM <name>` string.
fn extract_final_cte_name(select: &str) -> Option<String> {
    let upper = select.to_ascii_uppercase();
    let from_pos = upper.find("FROM ")?;
    let name_part = select[from_pos + 5..].trim();
    // Take up to the first whitespace or end.
    let end = name_part
        .find(|c: char| c.is_whitespace() || c == ';')
        .unwrap_or(name_part.len());
    let name = name_part[..end].trim_end_matches(';').to_string();
    if name.is_empty() { None } else { Some(name) }
}

/// Rename a CTE counter within a name by adding `offset`.
///
/// CTE names produced by the DVM engine have the form
/// `__pgt_cte_{prefix}_{counter}`.  The offset is added to `{counter}`
/// so names remain within the same namespace but at a different position.
///
/// If the name does not match the expected pattern, a simple suffix is
/// appended instead: `<name>_n{offset}`.
fn rename_with_offset(name: &str, offset: usize) -> String {
    // The name pattern is __pgt_cte_{anything}_{counter}
    // Find the last underscore-separated numeric suffix.
    if let Some(last_us) = name.rfind('_')
        && let Ok(n) = name[last_us + 1..].parse::<usize>()
    {
        return format!("{}_{}", &name[..last_us], n + offset);
    }
    // Fallback: append offset as a suffix.
    format!("{name}_n{offset}")
}

// ── Unit tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_delta_sql(
        source_oid: u32,
        prev_lsn: &str,
        new_lsn: &str,
        counter_base: usize,
    ) -> String {
        format!(
            "WITH __pgt_cte_scan_{c1} AS MATERIALIZED (\
SELECT __pgt_row_id, __pgt_action, id, val \
FROM pgtrickle_changes.changes_{oid} \
WHERE lsn > '{prev}'::pg_lsn AND lsn <= '{new}'::pg_lsn\
),\n\
__pgt_cte_scan_{c2} AS (\
SELECT a.__pgt_row_id, a.__pgt_action, a.id, a.val \
FROM __pgt_cte_scan_{c1} a\
)\n\
SELECT * FROM __pgt_cte_scan_{c2}",
            oid = source_oid,
            prev = prev_lsn,
            new = new_lsn,
            c1 = counter_base,
            c2 = counter_base + 1,
        )
    }

    fn make_node(pgt_id: i64, table: &str, delta_sql: String) -> NodeSpec {
        NodeSpec {
            pgt_id,
            quoted_table: table.to_string(),
            delta_sql,
            user_cols: vec!["id".to_string(), "val".to_string()],
            has_partition_key: false,
        }
    }

    /// PERF-2: Shared source-delta CTEs (identical body) are deduplicated.
    #[test]
    fn fuse_diff_batch_cte_dedup() {
        // Both nodes read from the same source (OID 12345, same LSN window).
        let delta_a = make_delta_sql(12345, "0/1000", "0/2000", 1);
        let delta_b = make_delta_sql(12345, "0/1000", "0/2000", 1); // identical delta

        let nodes = vec![
            make_node(101, r#""public"."view_a""#, delta_a),
            make_node(102, r#""public"."view_b""#, delta_b),
        ];

        let out = fuse_diff_batch(&nodes).expect("fuse_diff_batch failed");

        // The scan CTE for source 12345 should appear exactly once.
        let scan_cte_body_fragment = "pgtrickle_changes.changes_12345";
        let count = out.sql.matches(scan_cte_body_fragment).count();
        assert_eq!(
            count, 1,
            "source-delta CTE for OID 12345 should be deduplicated (expected 1 occurrence, got {count})\nSQL:\n{}",
            out.sql
        );

        assert_eq!(out.node_count, 2);
        // Both MERGE targets must appear.
        assert!(
            out.sql.contains(r#""public"."view_a""#),
            "missing view_a in output"
        );
        assert!(
            out.sql.contains(r#""public"."view_b""#),
            "missing view_b in output"
        );
        // Every MERGE is wrapped so its exact action counts can be returned.
        assert!(
            out.sql.contains("_apply_102"),
            "last node must be wrapped in an apply CTE"
        );
        // The first node must be wrapped in a _apply_ CTE.
        assert!(
            out.sql.contains("_apply_101"),
            "first node must be wrapped in an apply CTE"
        );
    }

    /// PERF-2: All CTE names in the fused output are unique.
    #[test]
    fn fuse_diff_batch_name_collision_free() {
        // Both nodes use DIFFERENT sources and start from counter=1 in their
        // individual delta SQL — this creates a potential collision.
        let delta_a = make_delta_sql(11111, "0/100", "0/200", 1);
        let delta_b = make_delta_sql(22222, "0/300", "0/400", 1); // also starts at counter 1

        let nodes = vec![
            make_node(201, r#""public"."node_a""#, delta_a),
            make_node(202, r#""public"."node_b""#, delta_b),
        ];

        let out = fuse_diff_batch(&nodes).expect("fuse_diff_batch failed");
        let sql = &out.sql;

        // Collect all CTE definition names: patterns like "name AS (" or "name AS MATERIALIZED (".
        // We look for `__pgt_*` or `_apply_*` followed by " AS (" or " AS MATERIALIZED (".
        let mut definitions: Vec<String> = Vec::new();
        let upper_sql = sql.to_ascii_uppercase();
        // Find " AS (" and " AS MATERIALIZED (" patterns — these are CTE definitions.
        for kw in &[" AS (", " AS MATERIALIZED (", " AS NOT MATERIALIZED ("] {
            let mut pos = 0usize;
            while let Some(idx) = upper_sql[pos..].find(kw) {
                let abs = pos + idx;
                // Walk backwards to find the identifier.
                let before = sql[..abs].trim_end();
                let id_start = before
                    .rfind(|c: char| c.is_whitespace() || c == ',')
                    .map(|p| p + 1)
                    .unwrap_or(0);
                let candidate = before[id_start..].trim();
                if candidate.starts_with("__pgt_") || candidate.starts_with("_apply_") {
                    definitions.push(candidate.to_string());
                }
                pos = abs + kw.len();
            }
        }

        // Deduplicate and check uniqueness.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for def in &definitions {
            assert!(
                seen.insert(def.clone()),
                "duplicate CTE definition '{def}' in fused SQL:\n{sql}"
            );
        }

        assert_eq!(out.node_count, 2);
        // Both MERGE targets present.
        assert!(sql.contains(r#""public"."node_a""#));
        assert!(sql.contains(r#""public"."node_b""#));
    }

    /// Single-node fuse: the MERGE is wrapped so exact action counts are returned.
    #[test]
    fn fuse_diff_batch_single_node() {
        let delta = make_delta_sql(99999, "0/0", "0/100", 1);
        let nodes = vec![make_node(301, r#""public"."solo""#, delta)];
        let out = fuse_diff_batch(&nodes).expect("fuse_diff_batch single-node failed");
        assert_eq!(out.node_count, 1);
        assert!(out.sql.contains(r#""public"."solo""#));
        assert!(out.sql.contains("_apply_301"));
        assert!(out.sql.contains("__pgt_fused_counts_301"));
    }

    /// parse_delta_ctes handles a bare SELECT (no WITH).
    #[test]
    fn parse_delta_ctes_no_with() {
        let sql = "SELECT * FROM some_table";
        let (ctes, final_sel) = parse_delta_ctes(sql);
        assert!(ctes.is_empty());
        assert_eq!(final_sel, "SELECT * FROM some_table");
    }

    /// parse_delta_ctes handles MATERIALIZED keyword.
    #[test]
    fn parse_delta_ctes_materialized() {
        let sql = "WITH foo AS MATERIALIZED (SELECT 1 AS x), bar AS (SELECT x FROM foo) SELECT * FROM bar";
        let (ctes, final_sel) = parse_delta_ctes(sql);
        assert_eq!(ctes.len(), 2, "expected 2 CTEs, got {}", ctes.len());
        assert_eq!(ctes[0].name, "foo");
        assert!(ctes[0].is_materialized);
        assert_eq!(ctes[1].name, "bar");
        assert!(!ctes[1].is_materialized);
        assert_eq!(final_sel.trim(), "SELECT * FROM bar");
    }

    /// rename_with_offset increments the numeric suffix.
    #[test]
    fn rename_with_offset_increments_counter() {
        assert_eq!(
            rename_with_offset("__pgt_cte_scan_1", 100),
            "__pgt_cte_scan_101"
        );
        assert_eq!(
            rename_with_offset("__pgt_cte_scan_99", 200),
            "__pgt_cte_scan_299"
        );
        // Non-numeric suffix: append _nOFFSET.
        assert_eq!(rename_with_offset("some_cte", 50), "some_cte_n50");
    }

    // ── PERF-2 property tests ────────────────────────────────────────────────

    use proptest::prelude::*;

    /// Strategy producing a small source OID (to keep SQL compact).
    fn arb_oid() -> impl Strategy<Value = u32> {
        1_u32..=99_999_u32
    }

    /// Strategy producing a random LSN string in `HI/LO` hex format.
    fn arb_lsn() -> impl Strategy<Value = String> {
        (0_u32..=0xFF, 0_u32..=0xFFFF_FFFF).prop_map(|(hi, lo)| format!("{hi:X}/{lo:X}"))
    }

    /// Build a NodeSpec from explicit parameters (no global state needed).
    fn node_spec_from(
        pgt_id: i64,
        table: &str,
        oid: u32,
        lsn_prev: &str,
        lsn_new: &str,
        counter_start: usize,
    ) -> NodeSpec {
        NodeSpec {
            pgt_id,
            quoted_table: table.to_string(),
            delta_sql: make_delta_sql(oid, lsn_prev, lsn_new, counter_start),
            user_cols: vec!["val".into()],
            has_partition_key: false,
        }
    }

    proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(50))]

        /// PERF-2 PROP: fuse_diff_batch is deterministic — same inputs produce
        /// byte-identical output.
        #[test]
        fn prop_fused_batch_deterministic(
            oid_a in arb_oid(),
            oid_b in arb_oid(),
            lsn_prev in arb_lsn(),
            lsn_new  in arb_lsn(),
        ) {
            let nodes = vec![
                node_spec_from(101, r#""public"."det_a""#, oid_a, &lsn_prev, &lsn_new, 1),
                node_spec_from(102, r#""public"."det_b""#, oid_b, &lsn_prev, &lsn_new, 1),
            ];
            let out1 = fuse_diff_batch(&nodes).unwrap();
            let out2 = fuse_diff_batch(&nodes).unwrap();
            prop_assert_eq!(out1.sql, out2.sql, "fuse_diff_batch must be deterministic");
            prop_assert_eq!(out1.node_count, out2.node_count);
        }

        /// PERF-2 PROP: fuse_diff_batch output contains all MERGE target tables.
        #[test]
        fn prop_fused_batch_contains_all_tables(
            oid_a in arb_oid(),
            oid_b in arb_oid(),
            oid_c in arb_oid(),
            lsn_prev in arb_lsn(),
            lsn_new  in arb_lsn(),
        ) {
            let nodes = vec![
                node_spec_from(201, r#""pub"."ta""#, oid_a, &lsn_prev, &lsn_new, 1),
                node_spec_from(202, r#""pub"."tb""#, oid_b, &lsn_prev, &lsn_new, 1),
                node_spec_from(203, r#""pub"."tc""#, oid_c, &lsn_prev, &lsn_new, 1),
            ];
            let out = fuse_diff_batch(&nodes).unwrap();
            prop_assert!(
                out.sql.contains(r#""pub"."ta""#),
                "fused SQL missing ta:\n{}", out.sql
            );
            prop_assert!(
                out.sql.contains(r#""pub"."tb""#),
                "fused SQL missing tb:\n{}", out.sql
            );
            prop_assert!(
                out.sql.contains(r#""pub"."tc""#),
                "fused SQL missing tc:\n{}", out.sql
            );
            prop_assert_eq!(out.node_count, 3);
        }

        /// PERF-2 PROP: fuse_diff_batch on a single node is equivalent to the
        /// direct per-node MERGE path (no CTE name collisions, single MERGE).
        #[test]
        fn prop_fused_single_node_no_rename_needed(
            oid in arb_oid(),
            lsn_prev in arb_lsn(),
            lsn_new  in arb_lsn(),
        ) {
            let node = node_spec_from(301, r#""s"."t""#, oid, &lsn_prev, &lsn_new, 1);
            let out = fuse_diff_batch(&[node]).unwrap();
            prop_assert_eq!(out.node_count, 1);
            prop_assert!(
                out.sql.contains(r#"MERGE INTO "s"."t""#),
                "single-node fused SQL must contain MERGE INTO:\n{}", out.sql
            );
            // The single node still has an apply wrapper so action counts are
            // returned alongside the data-modifying CTE.
            let trimmed = out.sql.trim().to_ascii_uppercase();
            let is_with = trimmed.starts_with("WITH");
            prop_assert!(is_with, "fused SQL must start with WITH:\n{}", out.sql);
        }

        /// PERF-2 PROP: all CTE names in fused output are unique (no collisions).
        ///
        /// Exercises the rename_with_offset collision-prevention logic with
        /// two random source OIDs and random counter starts.
        #[test]
        fn prop_fused_cte_names_unique(
            oid_a in arb_oid(),
            oid_b in arb_oid(),
            lsn_prev in arb_lsn(),
            lsn_new  in arb_lsn(),
        ) {
            let nodes = vec![
                node_spec_from(401, r#""s"."ua""#, oid_a, &lsn_prev, &lsn_new, 1),
                node_spec_from(402, r#""s"."ub""#, oid_b, &lsn_prev, &lsn_new, 1),
            ];
            let out = fuse_diff_batch(&nodes).unwrap();
            let sql = &out.sql;
            let upper = sql.to_ascii_uppercase();

            // Collect CTE definition names by matching " AS (" and " AS MATERIALIZED (".
            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            for kw in &[" AS (", " AS MATERIALIZED (", " AS NOT MATERIALIZED ("] {
                let mut pos = 0usize;
                while let Some(idx) = upper[pos..].find(kw) {
                    let abs = pos + idx;
                    let before = sql[..abs].trim_end();
                    let id_start = before
                        .rfind(|c: char| c.is_whitespace() || c == ',')
                        .map(|p| p + 1)
                        .unwrap_or(0);
                    let candidate = before[id_start..].trim();
                    if candidate.starts_with("__pgt_") || candidate.starts_with("_apply_") {
                        prop_assert!(
                            seen.insert(candidate.to_string()),
                            "duplicate CTE name '{candidate}' in:\n{sql}"
                        );
                    }
                    pos = abs + kw.len();
                }
            }
        }
    }
}
