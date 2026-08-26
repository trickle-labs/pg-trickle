//! Base table scan differentiation.
//!
//! ΔI(Scan(T)) reads from the change buffer table for T.
//!
//! The change buffer table `pgtrickle_changes.changes_<stable_name>` contains:
//! - change_id BIGSERIAL — insertion ordering (CACHE 1 invariant, see cdc.rs)
//! - lsn PG_LSN
//! - action CHAR(1) — 'I', 'U', 'D'
//! - pk_hash BIGINT — pre-computed PK hash (optional)
//! - new_{col} TYPE — NEW row values (INSERT/UPDATE)
//! - old_{col} TYPE — OLD row values (UPDATE/DELETE)
//!
//! For UPDATEs, we split into DELETE (old values) + INSERT (new values).
//! Row IDs are computed as hash of the primary key columns.
//!
//! ## Single-pass design
//!
//! The change buffer is scanned **once** using typed columns rather than
//! JSONB deserialization. Columns are referenced directly as
//! `c."new_{col}"` / `c."old_{col}"` with proper PostgreSQL types,
//! eliminating `jsonb_populate_record` overhead.

use crate::dvm::diff::{DeltaSource, DiffContext, DiffResult, quote_ident};
use crate::dvm::parser::{Expr, OpTree};
use crate::error::PgTrickleError;

/// Differentiate a Scan node.
///
/// Reads from the change buffer in a **single pass** and produces a delta
/// with columns: `__pgt_row_id`, `__pgt_action`, plus all table columns.
///
/// UPDATEs are expanded into (DELETE old, INSERT new) via UNION ALL
/// branches, so the change buffer index on `lsn` is used exactly once.
///
/// Column extraction uses typed columns `c."new_{col}"` / `c."old_{col}"`
/// directly from the change buffer table — no JSONB deserialization.
///
/// When the context's `delta_source` is `TransitionTable`, reads from
/// statement-level trigger transition tables (ENRs) instead — no LSN
/// filtering, no net-effect computation needed.
pub fn diff_scan(ctx: &mut DiffContext, op: &OpTree) -> Result<DiffResult, PgTrickleError> {
    let OpTree::Scan {
        table_oid,
        table_name,
        schema,
        columns,
        pk_columns,
        alias,
        ..
    } = op
    else {
        return Err(PgTrickleError::InternalError(
            "diff_scan called on non-Scan node".into(),
        ));
    };

    // Dispatch based on delta source: transition tables (IMMEDIATE) vs
    // change buffer (DIFFERENTIAL).
    // Clone the delta_source to avoid holding an immutable borrow on ctx
    // while passing it as mutable to the sub-functions.
    let delta_source = ctx.delta_source.clone();
    match &delta_source {
        DeltaSource::TransitionTable { tables } => diff_scan_transition(
            ctx, *table_oid, schema, table_name, columns, pk_columns, alias, tables,
        ),
        DeltaSource::ChangeBuffer => {
            diff_scan_change_buffer(ctx, *table_oid, columns, pk_columns, alias)
        }
    }
}

/// Differentiate a Scan node using trigger transition tables (IMMEDIATE mode).
///
/// Transition tables are registered as Ephemeral Named Relations (ENRs):
/// - `__pgt_newtable_<oid>`: rows from INSERT/UPDATE (NEW values)
/// - `__pgt_oldtable_<oid>`: rows from UPDATE/DELETE (OLD values)
///
/// The delta is straightforward:
/// - INSERT: `SELECT 'I', cols FROM newtable` (with row_id from PK hash)
/// - DELETE: `SELECT 'D', cols FROM oldtable` (with row_id from PK hash)
/// - UPDATE: DELETE from oldtable UNION ALL INSERT from newtable
///
/// No net-effect computation is needed because each statement trigger
/// fires exactly once — there are no multiple changes per PK within
/// a single trigger invocation.
#[allow(clippy::too_many_arguments)]
fn diff_scan_transition(
    ctx: &mut DiffContext,
    table_oid: u32,
    schema: &str,
    table_name: &str,
    columns: &[crate::dvm::parser::Column],
    pk_columns: &[String],
    alias: &str,
    tables: &std::collections::HashMap<u32, crate::dvm::diff::TransitionTableNames>,
) -> Result<DiffResult, PgTrickleError> {
    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

    // Look up the transition table names for this source OID.
    let tt_names = tables.get(&table_oid);

    // Build the row_id hash expression from PK columns (or all columns
    // for keyless tables). Unlike the change buffer path, there is no
    // pre-computed pk_hash — we compute it from the actual column values.
    let hash_cols: Vec<&str> = if pk_columns.is_empty() {
        columns.iter().map(|c| c.name.as_str()).collect()
    } else {
        pk_columns.iter().map(|s| s.as_str()).collect()
    };

    let hash_args: Vec<String> = hash_cols
        .iter()
        .map(|c| format!("{}::TEXT", quote_ident(c)))
        .collect();
    let row_id_expr = build_hash_expr(&hash_args);

    // Column list for SELECT
    let col_refs: Vec<String> = columns.iter().map(|c| quote_ident(&c.name)).collect();
    let col_refs_str = col_refs.join(", ");

    // Build the delta CTE from available transition tables.
    // Both branches may exist for UPDATE, or only one for INSERT/DELETE.
    let mut branches = Vec::new();

    if let Some(tt) = tt_names {
        if let Some(ref old_name) = tt.old_name {
            // DELETE branch: rows from oldtable
            branches.push(format!(
                "\
SELECT {row_id_expr} AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {col_refs_str}
FROM {old_table}",
                old_table = quote_ident(old_name),
            ));
        }
        if let Some(ref new_name) = tt.new_name {
            // INSERT branch: rows from newtable
            branches.push(format!(
                "\
SELECT {row_id_expr} AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {col_refs_str}
FROM {new_table}",
                new_table = quote_ident(new_name),
            ));
        }
    }

    let cte_name = ctx.next_cte_name(&format!("scan_{alias}"));
    if branches.is_empty() {
        // No transition tables for this source — emit an empty delta.
        // This can happen for sources that weren't modified in this statement.
        //
        // Select from the actual source table with WHERE false to inherit
        // correct column types. Untyped NULL defaults to text in PostgreSQL,
        // which causes type-mismatch errors when downstream CTEs compare
        // these columns with typed columns from the other join side.
        let col_refs_str = columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let qualified_table = format!("{}.{}", quote_ident(schema), quote_ident(table_name),);
        let sql = format!(
            "SELECT NULL::BIGINT AS __pgt_row_id, NULL::TEXT AS __pgt_action, \
             {col_refs_str} FROM {qualified_table} WHERE false",
        );
        ctx.add_cte(cte_name.clone(), sql);
    } else {
        let sql = branches.join("\nUNION ALL\n");
        ctx.add_cte(cte_name.clone(), sql);
    }

    // Transition tables produce at most one row per PK per statement,
    // so the delta is inherently deduplicated for scan-chain queries.
    let is_deduplicated = ctx.merge_safe_dedup;

    // EC01B-1: Record this scan's delta CTE name for per-leaf
    // pre-change snapshots in deep join trees.
    ctx.scan_delta_ctes
        .insert(alias.to_string(), cte_name.clone());

    Ok(DiffResult {
        cte_name,
        columns: col_names,
        schema: crate::dvm::schema::RelationSchema::from_parser_columns(columns),
        is_deduplicated,
        has_key_changed: false,
    })
}

/// Differentiate a Scan node using change buffer tables (DIFFERENTIAL mode).
///
/// This is the original code path that reads from
/// `pgtrickle_changes.changes_<oid>` with LSN-range filtering and
/// net-effect computation.
fn diff_scan_change_buffer(
    ctx: &mut DiffContext,
    table_oid: u32,
    columns: &[crate::dvm::parser::Column],
    pk_columns: &[String],
    alias: &str,
) -> Result<DiffResult, PgTrickleError> {
    let change_table = ctx.change_table_for_source(table_oid);

    let prev_lsn = ctx.get_prev_lsn(table_oid);
    let new_lsn = ctx.get_new_lsn(table_oid);

    let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();

    // Keyless table handling (A42-14 / formerly tracked as EC-06).
    //
    // When pk_columns is empty, the source table has no primary key.
    // The pk_hash is an all-column content hash. Identical rows produce
    // identical pk_hash values. The current implementation uses a
    // *net-counting* strategy:
    //
    // **Net-counting approach (current):**
    // - INSERT delta: adds one row per unique pk_hash, merged with existing
    //   counts to determine whether a row appears / disappears / changes value.
    // - DELETE delta: removes matching hash rows.
    // - The stream table uses a non-unique row-id index for keyless sources
    //   so duplicate rows CAN coexist (see CREATE TABLE codegen).
    // - pk_stats aggregates net insert/delete counts per pk_hash so that
    //   two inserts of the same row result in a count of +2, not +1.
    //
    // **Known limitations:**
    // - For tables where identical rows are simultaneously inserted and
    //   deleted in the same cycle, the net delta may incorrectly cancel them.
    //   This is acceptable for append-only and PK-bearing tables.
    //
    // **Test coverage:**
    // - E2E: `test_keyless_multiset_property` in e2e_keyless_tests.rs
    //   verifies multiset equivalence after insert/update/delete sequences.
    // - Integration: `test_scan_*_keyless_*` in the unit test section below.
    //
    // A creation-time WARNING is emitted when a keyless source is detected
    // so operators can choose to add a primary key.
    let is_keyless = pk_columns.is_empty();

    // pk_hash is always pre-computed in the change buffer (from PK columns
    // or all-column content hash for keyless tables — S10).
    // Always use the pre-computed pk_hash from the trigger (G-J1 optimization).
    let pk_hash_expr = "c.pk_hash".to_string();

    // A44-10 (D+I schema): In the D+I schema, both D-rows and I-rows have the
    // same pre-computed pk_hash (OLD pk_hash for D-rows, NEW pk_hash for I-rows).
    // The __pgt_row_id for a D-row must match the existing ST row (computed from
    // OLD column values). The trigger/WAL-decoder sets pk_hash appropriately:
    //   - D-row from UPDATE: pk_hash from OLD values
    //   - I-row from UPDATE: pk_hash from NEW values
    //   - Genuine INSERT: pk_hash from NEW values
    //   - Genuine DELETE: pk_hash from OLD values
    // For D-rows, c.pk_hash IS the old pk_hash — no separate old_pk_hash_expr needed.
    let old_pk_hash_expr = "c.pk_hash".to_string();

    // A44-10 (D+I schema): Flat column references — no new_/old_ prefix.
    // D-rows carry old values in c."col"; I-rows carry new values.
    // The ST-source (is_st_source) distinction is gone — both base and ST
    // change buffers use the same flat schema.
    // Use cb_col_name() to map reserved names (e.g. user column "action" → "__usr_action").
    let col_refs: Vec<String> = columns
        .iter()
        .map(|c| format!("c.{}", quote_ident(&crate::cdc::cb_col_name(&c.name))))
        .collect();
    let typed_col_refs_str = col_refs.join(",\n       ");

    // Output column references: both DELETE and INSERT use flat c."col".
    // The alias is the original column name for downstream CTE compatibility.
    let col_ref_aliased: Vec<String> = columns
        .iter()
        .map(|c| {
            format!(
                "c.{} AS {}",
                quote_ident(&crate::cdc::cb_col_name(&c.name)),
                quote_ident(&c.name),
            )
        })
        .collect();
    let old_col_refs: Vec<String> = col_ref_aliased.clone();
    let new_col_refs: Vec<String> = col_ref_aliased;

    // ## Net-effect scan delta (split fast-path approach)
    //
    // When the same PK has multiple changes within one refresh window
    // (e.g., INSERT then UPDATE, or INSERT then DELETE), we compute the
    // NET effect rather than emitting all raw events.
    //
    // CTE 1 (pk_stats): Groups changes by PK hash, counts per PK.
    //
    // CTE 2 (single): Fast path for PKs with exactly one change (~95%
    // of PKs). No window functions needed — action IS first/last action.
    //
    // CTE 3 (multi_raw): Slow path for PKs with multiple changes.
    // Applies FIRST_VALUE/LAST_VALUE window functions to determine the
    // net effect. The window sort operates on a much smaller data set.
    //
    // CTE 4 (scan_raw): UNION ALL of single + multi_raw paths.
    //
    // CTE 5 (scan): Emits D/I events filtered by first_action/last_action:
    // - DELETE only when first_action != 'I' (row existed before the cycle)
    // - INSERT only when last_action != 'D' (row still exists after the cycle)
    //
    // This correctly handles:
    // - Plain INSERT:         → I(new)
    // - Plain DELETE:         → D(old)
    // - Plain UPDATE:         → D(old) + I(new)
    // - INSERT + UPDATE:      → I(final)   (no spurious DELETE)
    // - INSERT + DELETE:      → nothing     (cancels out)
    // - UPDATE + DELETE:      → D(original)
    // - Multiple UPDATEs:     → D(first old) + I(last new)
    // - INSERT + UPDATE + DELETE: → nothing (cancels out)
    //
    // ## Merge-safe dedup mode (G-M1 optimization)
    //
    // When `ctx.merge_safe_dedup` is true (scan-chain queries without
    // aggregate/join/union above), the DELETE branch is further restricted
    // to only emit when the row is TRULY deleted (last_action = 'D').
    // For updates, only the INSERT branch fires. This produces exactly
    // ONE row per PK, allowing the MERGE to skip DISTINCT ON + ORDER BY.

    // ── R1: pk_stats CTE — count changes per PK ──────────────────────
    //
    // Used to split single-change PKs (fast path, no window functions)
    // from multi-change PKs (require FIRST_VALUE/LAST_VALUE).
    let mut lsn_filter = format!("c.lsn > '{prev_lsn}'::pg_lsn AND c.lsn <= '{new_lsn}'::pg_lsn");

    // ── P2-5 / WB-1: changed_cols VARBIT filter ─────────────────────
    //
    // The CDC trigger stores a VARBIT in `changed_cols` for UPDATE rows:
    // bit at position i (leftmost=0) is B'1' when column i changed.
    // NULL for INSERT/DELETE (all columns populated). When column pruning
    // has reduced the referenced set, we can skip UPDATE rows where none
    // of the referenced columns actually changed. Works for any column count.
    //
    // Cross-backend safety: the cached delta SQL template embeds a fixed-
    // width bitmask literal (e.g. B'0110' for 3 CDC columns).  If the CDC
    // trigger is later rebuilt with additional columns (e.g. 4-bit), a
    // stale prepared template on another backend would cause
    // "cannot AND bit strings of different sizes" when the bit-widths
    // differ.  Using a CASE expression ensures the bit operation is only
    // evaluated when the widths match; PostgreSQL guarantees CASE WHEN
    // branches are evaluated in order (unlike AND/OR in WHERE clauses
    // which the optimizer may reorder).  Stale masks fall through to
    // ELSE TRUE = conservatively include the row (slight over-scan, but
    // always correct and never an error).
    if !is_keyless
        && let Some(cdc_cols) = ctx.source_cdc_columns.get(&table_oid)
        && let Some((mask_str, zero_str)) = compute_varbit_changed_cols_mask(columns, cdc_cols)
    {
        let mask_len = mask_str.len();
        lsn_filter.push_str(&format!(
            " AND (c.changed_cols IS NULL \
             OR CASE WHEN bit_length(c.changed_cols) = {mask_len} \
                     THEN (c.changed_cols & B'{mask_str}') != B'{zero_str}' \
                     ELSE TRUE END)"
        ));
    }

    // ── A-2: Key-column change detection expression ──────────────────
    //
    // When key columns (GROUP BY, JOIN ON, WHERE) are a strict subset of
    // CDC columns, compute an expression that evaluates to TRUE when a key
    // column changed (or the row is INSERT/DELETE). FALSE means only
    // value columns changed — the row stays in its group/join bucket.
    let key_change_expr: Option<String> = if !is_keyless {
        if let Some(cdc_cols) = ctx.source_cdc_columns.get(&table_oid)
            && let Some(key_cols) = ctx.source_key_columns.get(&table_oid)
            && let Some((key_mask, key_zero)) = compute_varbit_key_cols_mask(key_cols, cdc_cols)
        {
            let mask_len = key_mask.len();
            Some(format!(
                "CASE WHEN c.changed_cols IS NULL THEN TRUE \
                 WHEN bit_length(c.changed_cols) != {mask_len} THEN TRUE \
                 WHEN (c.changed_cols & B'{key_mask}') = B'{key_zero}' THEN FALSE \
                 ELSE TRUE END"
            ))
        } else {
            None
        }
    } else {
        None
    };

    // ── EC-06: Keyless net-counting path ─────────────────────────────
    //
    // For keyless tables, identical rows share the same pk_hash (content
    // hash), causing the DISTINCT ON logic to collapse independent events.
    // Instead, we decompose all changes into atomic +1 (INSERT) / -1
    // (DELETE) operations per content hash, sum to get a net count, and
    // expand using generate_series.
    //
    // A44-10 (D+I schema): UPDATEs are already decomposed at write time
    // (D-row + I-row), so no UNION ALL for 'U' rows is needed here.
    if is_keyless {
        // A44-10: Flat column references (no new_/old_ prefix).
        // Use cb_col_name() to map reserved source column names to their CB storage names.
        let col_refs_raw: Vec<String> = columns
            .iter()
            .map(|c| {
                format!(
                    "c.{} AS {}",
                    quote_ident(&crate::cdc::cb_col_name(&c.name)),
                    quote_ident(&c.name),
                )
            })
            .collect();
        // Use array_agg(col)[1] instead of MAX(col) so that types without a
        // comparison operator (e.g. jsonb, json, arrays, composites) are handled
        // correctly. All rows sharing a content_hash are identical by construction,
        // so picking the first element is semantically equivalent to MAX/MIN for
        // comparable types while being universally type-safe.
        let max_col_refs: Vec<String> = columns
            .iter()
            .map(|c| {
                format!(
                    "(array_agg({}))[1] AS {}",
                    quote_ident(&c.name),
                    quote_ident(&c.name)
                )
            })
            .collect();
        let sub_col_refs: Vec<String> = columns
            .iter()
            .map(|c| format!("sub.{}", quote_ident(&c.name)))
            .collect();

        // CTE: Decompose all events into atomic +1/-1 per content hash.
        // A44-10 (D+I schema): No UPDATE UNION ALL branches needed —
        // UPDATEs arrive as D-row + I-row pairs from the trigger/WAL decoder.
        let decomp_cte = ctx.next_cte_name(&format!("kl_decomp_{alias}"));
        let decomp_sql = format!(
            "\
-- INSERT events: +1 per content hash
SELECT {pk_hash_expr} AS content_hash, 1 AS delta_sign,
       {col_refs}
FROM {change_table} c
WHERE {lsn_filter} AND c.action = 'I'

UNION ALL

-- DELETE events: -1 per content hash
SELECT {pk_hash_expr} AS content_hash, -1 AS delta_sign,
       {col_refs}
FROM {change_table} c
WHERE {lsn_filter} AND c.action = 'D'",
            col_refs = col_refs_raw.join(",\n       "),
        );
        ctx.add_cte(decomp_cte.clone(), decomp_sql);

        // Final CTE: aggregate net counts and expand with generate_series.
        let cte_name = ctx.next_cte_name(&format!("scan_{alias}"));
        let sql = format!(
            "\
-- Net inserts: content hashes with net_count > 0
SELECT sub.content_hash AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {sub_cols}
FROM (
    SELECT content_hash,
           {max_cols},
           SUM(delta_sign)::INT AS net_count
    FROM {decomp_cte}
    GROUP BY content_hash
    HAVING SUM(delta_sign) > 0
) sub
CROSS JOIN generate_series(1, sub.net_count) gs

UNION ALL

-- Net deletes: content hashes with net_count < 0
SELECT sub.content_hash AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {sub_cols}
FROM (
    SELECT content_hash,
           {max_cols},
           SUM(delta_sign)::INT AS net_count
    FROM {decomp_cte}
    GROUP BY content_hash
    HAVING SUM(delta_sign) < 0
) sub
CROSS JOIN generate_series(1, -sub.net_count) gs",
            sub_cols = sub_col_refs.join(",\n       "),
            max_cols = max_col_refs.join(",\n           "),
        );
        ctx.add_cte(cte_name.clone(), sql);

        return Ok(DiffResult {
            cte_name,
            columns: col_names,
            schema: crate::dvm::schema::RelationSchema::from_parser_columns(columns),
            is_deduplicated: false,
            has_key_changed: false,
        });
    }

    // ── PK-based tables: standard pk_stats / single / multi pipeline ─
    let pk_stats_cte = ctx.next_cte_name(&format!("pk_stats_{alias}"));
    let pk_stats_sql = format!(
        "\
SELECT {pk_hash_expr} AS __pk_hash, count(*) AS cnt
FROM {change_table} c
WHERE {lsn_filter}
GROUP BY {pk_hash_expr}",
    );
    ctx.add_cte(pk_stats_cte.clone(), pk_stats_sql);

    // ── R2: Single-change fast path (no window functions) ────────────
    //
    // ~95% of PKs typically have exactly one change per refresh cycle.
    // For these, first_action = last_action = action — skip the sort.
    let single_cte = ctx.next_cte_name(&format!("single_{alias}"));
    let key_col_single = key_change_expr.as_deref().map_or(String::new(), |expr| {
        format!(",\n       {expr} AS __pgt_key_changed")
    });
    let single_sql = format!(
        "\
SELECT {pk_hash_expr} AS __pk_hash,
       {old_pk_hash_expr} AS __pk_hash_old,
       c.action,
       c.change_id,
       {typed_col_refs_str},
       c.action AS __first_action,
       c.action AS __last_action{key_col_single}
FROM {change_table} c
JOIN {pk_stats_cte} p ON p.__pk_hash = {pk_hash_expr} AND p.cnt = 1
WHERE {lsn_filter}",
    );
    ctx.add_cte(single_cte.clone(), single_sql);

    // ── R3: Multi-change path with window functions ──────────────────
    //
    // Only apply FIRST_VALUE/LAST_VALUE to PKs with multiple changes.
    // The window sort now operates on a much smaller data set.
    let multi_cte = ctx.next_cte_name(&format!("multi_raw_{alias}"));
    // A-2: For multi-change PKs, key_changed = TRUE if ANY change in the
    // PK's change set touched a key column. Use bool_or() over the window.
    let key_col_multi = if let Some(ref expr) = key_change_expr {
        format!(
            ",\n       bool_or({expr}) OVER (\
             PARTITION BY {pk_hash_expr}\
             ) AS __pgt_key_changed"
        )
    } else {
        String::new()
    };
    let multi_sql = format!(
        "\
SELECT {pk_hash_expr} AS __pk_hash,
       {old_pk_hash_expr} AS __pk_hash_old,
       c.action,
       c.change_id,
       {typed_col_refs_str},
       FIRST_VALUE(c.action) OVER (
           PARTITION BY {pk_hash_expr} ORDER BY c.change_id
       ) AS __first_action,
       LAST_VALUE(c.action) OVER (
           PARTITION BY {pk_hash_expr} ORDER BY c.change_id
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) AS __last_action{key_col_multi}
FROM {change_table} c
JOIN {pk_stats_cte} p ON p.__pk_hash = {pk_hash_expr} AND p.cnt > 1
WHERE {lsn_filter}",
    );
    ctx.add_cte(multi_cte.clone(), multi_sql);

    // ── R4: Union single + multi paths ───────────────────────────────
    let raw_cte_name = ctx.next_cte_name(&format!("scan_raw_{alias}"));
    let raw_sql = format!(
        "\
SELECT * FROM {single_cte}
UNION ALL
SELECT * FROM {multi_cte}",
    );
    ctx.add_cte(raw_cte_name.clone(), raw_sql);

    // CTE 2: Emit D/I events with net-effect filtering
    let cte_name = ctx.next_cte_name(&format!("scan_{alias}"));
    let is_deduplicated;

    // DELETE __pgt_row_id uses __pk_hash_old (computed from OLD column
    // values) so it matches the existing ST row — critical for keyless
    // tables where UPDATE changes ALL columns and thus the hash.
    // INSERT __pgt_row_id uses __pk_hash (from NEW column values).

    // ── P2-7: Build pushed-filter WHERE clauses ──────────────────────
    //
    // When a Filter was pushed into this Scan (via DiffContext), inject
    // the rewritten predicate into the final scan CTE's output branches.
    // A44-10 (D+I schema): both branches use the same flat column name
    // (no old_/new_ prefix). D-rows have old values, I-rows have new values.
    let (del_pushed_filter, ins_pushed_filter) =
        if let Some(ref predicate) = ctx.scan_pushed_predicate {
            let del = rewrite_predicate_for_scan(predicate, "");
            let ins = rewrite_predicate_for_scan(predicate, "");
            (format!("\nWHERE {del}"), format!("\nWHERE {ins}"))
        } else {
            (String::new(), String::new())
        };

    // ── A-2: Key-changed column for downstream operators ───────────
    // When key_change_expr is available, include __pgt_key_changed in
    // the final delta CTE output. Downstream operators (aggregate, join)
    // can use this to optimize value-only UPDATEs.
    let key_changed_col_del = if key_change_expr.is_some() {
        ",\n       c.__pgt_key_changed"
    } else {
        ""
    };
    let key_changed_col_ins = key_changed_col_del; // Same for both branches.

    let sql = if ctx.merge_safe_dedup {
        // ── Merge-safe dedup mode ──────────────────────────────────────
        // Emit at most ONE row per PK: DELETE only for true deletes OR
        // when the row ID changed (keyless table UPDATE). INSERT for any
        // row that exists after the cycle (incl. updates).
        is_deduplicated = true;
        format!(
            "\
-- DELETE events: row existed before AND was truly deleted, OR
-- the row hash changed (keyless table update — old row must be removed).
-- For PK-based tables __pk_hash_old == __pk_hash always, so the OR
-- clause never fires → no regression.
SELECT c.__pk_hash_old AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {old_col_refs}{key_del}
FROM (
  SELECT DISTINCT ON (s.__pk_hash_old)
         s.*
  FROM {raw_cte_name} s
  WHERE s.__first_action != 'I'
    AND (s.__last_action = 'D' OR s.__pk_hash_old != s.__pk_hash)
  ORDER BY s.__pk_hash_old, s.change_id
) c{del_pushed_filter}

UNION ALL

-- INSERT events: row exists after (handles inserts + updates)
SELECT c.__pk_hash AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {new_col_refs}{key_ins}
FROM (
  SELECT DISTINCT ON (s.__pk_hash)
         s.*
  FROM {raw_cte_name} s
  WHERE s.__last_action != 'D'
  ORDER BY s.__pk_hash, s.change_id DESC
) c{ins_pushed_filter}",
            old_col_refs = old_col_refs.join(",\n       "),
            new_col_refs = new_col_refs.join(",\n       "),
            key_del = key_changed_col_del,
            key_ins = key_changed_col_ins,
        )
    } else {
        // ── Standard mode (D+I pairs for updates) ──────────────────────
        // Required when aggregate/join/union consumes the scan delta.
        is_deduplicated = false;
        format!(
            "\
-- DELETE events: row existed before (first_action != 'I')
-- Uses old_* columns from the earliest non-INSERT change per PK.
-- __pk_hash_old ensures the row_id matches the existing ST row,
-- which is critical for keyless tables where all-column hash changes.
SELECT c.__pk_hash_old AS __pgt_row_id,
       'D'::TEXT AS __pgt_action,
       {old_col_refs}{key_del}
FROM (
  SELECT DISTINCT ON (s.__pk_hash)
         s.*
  FROM {raw_cte_name} s
  WHERE s.action != 'I' AND s.__first_action != 'I'
  ORDER BY s.__pk_hash, s.change_id
) c{del_pushed_filter}

UNION ALL

-- INSERT events: row exists after (last_action != 'D')
-- Uses new_* columns from the latest non-DELETE change per PK.
SELECT c.__pk_hash AS __pgt_row_id,
       'I'::TEXT AS __pgt_action,
       {new_col_refs}{key_ins}
FROM (
  SELECT DISTINCT ON (s.__pk_hash)
         s.*
  FROM {raw_cte_name} s
  WHERE s.action != 'D' AND s.__last_action != 'D'
  ORDER BY s.__pk_hash, s.change_id DESC
) c{ins_pushed_filter}",
            old_col_refs = old_col_refs.join(",\n       "),
            new_col_refs = new_col_refs.join(",\n       "),
            key_del = key_changed_col_del,
            key_ins = key_changed_col_ins,
        )
    };

    ctx.add_cte(cte_name.clone(), sql);

    // EC01B-1: Record this scan's delta CTE name so that
    // build_pre_change_snapshot_sql can construct per-leaf pre-change
    // snapshots for deep join trees without full-snapshot EXCEPT ALL.
    ctx.scan_delta_ctes
        .insert(alias.to_string(), cte_name.clone());

    Ok(DiffResult {
        cte_name,
        columns: col_names,
        schema: crate::dvm::schema::RelationSchema::from_parser_columns(columns),
        is_deduplicated,
        has_key_changed: key_change_expr.is_some(),
    })
}

/// Find effective hash columns for a table (used in tests and for reference).
///
/// Uses real PK columns from `pg_constraint` if available (populated during
/// parsing). Falls back to all columns for keyless tables (S10), which
/// matches the all-column content hash stored as pk_hash in the CDC trigger.
#[cfg(test)]
fn find_pk_columns(pk_columns: &[String], columns: &[crate::dvm::parser::Column]) -> Vec<String> {
    if !pk_columns.is_empty() {
        return pk_columns.to_vec();
    }
    // Keyless table: use all columns (matches CDC trigger all-column hash).
    columns.iter().map(|c| c.name.clone()).collect()
}

/// P2-5: Compute a `changed_cols` bitmask from pruned Scan columns.
///
/// For each column in `scan_columns` (the pruned set), finds its index in
/// `cdc_columns` (the full CDC column list ordered by `attnum`) and sets
/// WB-1: Compute a VARBIT mask string for the `changed_cols` scan filter.
///
/// Returns `Some((mask_bits, zero_bits))` where each string has one character
/// per CDC column in ordinal order: '1' if the column is referenced by the scan,
/// '0' otherwise. Both strings have length `cdc_columns.len()`.
///
/// Returns `None` when the mask is trivially unfiltered:
/// - No scan columns mapped to CDC columns (mask = all zeros).
/// - Every CDC column is referenced (full mask — any UPDATE passes).
fn compute_varbit_changed_cols_mask(
    scan_columns: &[crate::dvm::parser::Column],
    cdc_columns: &[String],
) -> Option<(String, String)> {
    let n = cdc_columns.len();
    if n == 0 {
        return None;
    }
    let mut mask_bits: Vec<char> = vec!['0'; n];
    for col in scan_columns {
        if let Some(idx) = cdc_columns.iter().position(|c| c == &col.name) {
            mask_bits[idx] = '1';
        }
    }
    let any_set = mask_bits.contains(&'1');
    let any_unset = mask_bits.contains(&'0');
    // Only useful when it is a strict subset (not empty, not fully set).
    if !any_set || !any_unset {
        return None;
    }
    let mask_str: String = mask_bits.iter().collect();
    let zero_str: String = "0".repeat(n);
    Some((mask_str, zero_str))
}

/// A-2: Compute a VARBIT mask for key columns only (GROUP BY, JOIN ON, WHERE).
///
/// Similar to `compute_varbit_changed_cols_mask` but restricted to columns
/// that appear in key positions. When `changed_cols & key_mask = all_zeros`,
/// the UPDATE changed only value columns — the row stays in its group/join
/// bucket.
///
/// Returns `Some((key_mask, zero_str))` when the key set is non-empty and a
/// strict subset of all CDC columns. Returns `None` when:
/// - No key columns mapped (all columns are value-only — mask is trivially zero).
/// - All CDC columns are key columns (mask equals the all-columns mask — no benefit).
/// - No key columns data available.
pub(crate) fn compute_varbit_key_cols_mask(
    key_columns: &[String],
    cdc_columns: &[String],
) -> Option<(String, String)> {
    let n = cdc_columns.len();
    if n == 0 || key_columns.is_empty() {
        return None;
    }
    let mut mask_bits: Vec<char> = vec!['0'; n];
    let mut any_set = false;
    for key_col in key_columns {
        if let Some(idx) = cdc_columns.iter().position(|c| c == key_col) {
            mask_bits[idx] = '1';
            any_set = true;
        }
    }
    if !any_set {
        return None;
    }
    // Only useful when it's a strict subset (not all key columns).
    let any_unset = mask_bits.contains(&'0');
    if !any_unset {
        // All columns are key columns — no value-only optimization possible.
        return None;
    }
    let mask_str: String = mask_bits.iter().collect();
    let zero_str: String = "0".repeat(n);
    Some((mask_str, zero_str))
}

// ── P2-7: Predicate pushdown helpers ────────────────────────────────────

/// Check whether a predicate can be pushed into a Scan's final CTE.
///
/// A predicate is pushable when every column reference resolves to a column
/// in `scan_columns` (identified by the scan's alias or unqualified), and
/// the expression contains no `Raw` or `Star` nodes (which can't be
/// reliably rewritten to use `old_`/`new_` column prefixes).
pub fn is_predicate_pushable_to_scan(
    expr: &Expr,
    scan_alias: &str,
    scan_col_names: &[String],
) -> bool {
    match expr {
        Expr::ColumnRef {
            table_alias,
            column_name,
        } => match table_alias {
            Some(alias) if alias == scan_alias => scan_col_names.contains(column_name),
            None => scan_col_names.contains(column_name),
            _ => false,
        },
        Expr::Literal(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            is_predicate_pushable_to_scan(left, scan_alias, scan_col_names)
                && is_predicate_pushable_to_scan(right, scan_alias, scan_col_names)
        }
        Expr::FuncCall { args, .. } => args
            .iter()
            .all(|a| is_predicate_pushable_to_scan(a, scan_alias, scan_col_names)),
        Expr::Raw(_) | Expr::Star { .. } => false,
    }
}

/// Rewrite a predicate expression for use in a change buffer scan CTE.
///
/// Each `ColumnRef` is mapped to `c."<prefix><column_name>"` where
/// `prefix` is `"old_"` for the DELETE branch or `"new_"` for the
/// INSERT branch.
pub fn rewrite_predicate_for_scan(expr: &Expr, prefix: &str) -> String {
    match expr {
        Expr::ColumnRef { column_name, .. } => {
            // A44-10 (D+I schema): prefix is "" — just use the flat column name.
            // Apply cb_col_name() so reserved source column names (e.g. "action")
            // resolve to their change-buffer storage name ("__usr_action").
            let cb_name = crate::cdc::cb_col_name(column_name);
            format!("c.\"{}{}\"", prefix, cb_name.replace('"', "\"\""))
        }
        Expr::Literal(val) => val.clone(),
        Expr::BinaryOp { op, left, right } => {
            format!(
                "({} {op} {})",
                rewrite_predicate_for_scan(left, prefix),
                rewrite_predicate_for_scan(right, prefix),
            )
        }
        Expr::FuncCall { func_name, args } => {
            let rewritten: Vec<String> = args
                .iter()
                .map(|a| rewrite_predicate_for_scan(a, prefix))
                .collect();
            format!("{func_name}({})", rewritten.join(", "))
        }
        _ => expr.to_sql(),
    }
}

/// Build a hash expression from a list of SQL expressions.
pub fn build_hash_expr(exprs: &[String]) -> String {
    if exprs.len() == 1 {
        format!("pgtrickle.pg_trickle_hash({})", exprs[0])
    } else {
        // Wrap each expression in parentheses to ensure ::TEXT cast binds
        // to the whole expression, not just the last operand. Without
        // parens, `a * (1 - b)::TEXT` would cast only `b` to TEXT due to
        // SQL precedence of :: over arithmetic operators.
        let array_items: Vec<String> = exprs.iter().map(|e| format!("({e})::TEXT")).collect();
        crate::hash::build_composite_hash_expr(&array_items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::operators::test_helpers::*;

    // ── diff_scan basic ─────────────────────────────────────────────

    #[test]
    fn test_diff_scan_basic_columns() {
        let mut ctx = test_ctx();
        let tree = scan(100, "orders", "public", "o", &["id", "amount", "region"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();

        assert_eq!(result.columns, vec!["id", "amount", "region"]);
        assert!(!result.cte_name.is_empty());
    }

    #[test]
    fn test_diff_scan_generates_change_table_ref() {
        let mut ctx = test_ctx();
        let tree = scan(42, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_sql_contains(&sql, "\"pgtrickle_changes\".changes_42");
    }

    #[test]
    fn test_diff_scan_lsn_filter() {
        let mut ctx = test_ctx();
        let tree = scan(100, "orders", "public", "o", &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Default frontiers produce "0/0" LSNs
        assert_sql_contains(&sql, "c.lsn > '0/0'::pg_lsn");
        assert_sql_contains(&sql, "c.lsn <= '0/0'::pg_lsn");
    }

    #[test]
    fn test_diff_scan_placeholder_mode() {
        let mut ctx = test_ctx().with_placeholders();
        let tree = scan(55, "items", "public", "i", &["id", "name"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert_sql_contains(&sql, "__PGS_PREV_LSN_55__");
        assert_sql_contains(&sql, "__PGS_NEW_LSN_55__");
    }

    #[test]
    fn test_diff_scan_with_pk_columns() {
        let mut ctx = test_ctx();
        let tree = scan_with_pk(100, "orders", "public", "o", &["id", "amount"], &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // With PK columns, should use pre-computed pk_hash
        assert_sql_contains(&sql, "c.pk_hash");
    }

    #[test]
    fn test_diff_scan_without_pk_fallback() {
        let mut ctx = test_ctx();
        // S10: Even tables without PK now use c.pk_hash (the CDC trigger computes
        // an all-column content hash, stored in the change buffer's pk_hash column).
        let tree = scan_not_null(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should always use pre-computed c.pk_hash (keyless or not).
        assert_sql_contains(&sql, "c.pk_hash");
    }

    #[test]
    fn test_diff_scan_typed_column_refs() {
        // A44-10 (D+I schema): flat column refs, no new_/old_ prefix
        let mut ctx = test_ctx();
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should reference flat typed columns
        assert_sql_contains(&sql, "c.\"id\"");
        assert_sql_contains(&sql, "c.\"amount\"");
        // No new_/old_ prefixed columns
        assert_sql_not_contains(&sql, "c.\"new_id\"");
        assert_sql_not_contains(&sql, "c.\"old_id\"");
    }

    #[test]
    fn test_diff_scan_delete_and_insert_branches() {
        let mut ctx = test_ctx();
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should have DELETE and INSERT event branches
        assert_sql_contains(&sql, "'D'::TEXT AS __pgt_action");
        assert_sql_contains(&sql, "'I'::TEXT AS __pgt_action");
    }

    #[test]
    fn test_diff_scan_merge_safe_dedup() {
        let mut ctx = test_ctx();
        ctx.merge_safe_dedup = true;
        let tree = scan_with_pk(100, "orders", "public", "o", &["id", "amount"], &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();

        // Merge-safe dedup → is_deduplicated = true
        assert!(result.is_deduplicated);

        let sql = ctx.build_with_query(&result.cte_name);
        // Should have the "truly deleted" filter
        assert_sql_contains(&sql, "__last_action = 'D'");
    }

    #[test]
    fn test_diff_scan_standard_mode_not_deduplicated() {
        let mut ctx = test_ctx();
        let tree = scan(100, "orders", "public", "o", &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();

        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_scan_error_on_non_scan_node() {
        let mut ctx = test_ctx();
        let tree = OpTree::Distinct {
            child: Box::new(scan(1, "t", "public", "t", &["id"])),
        };
        let result = diff_scan(&mut ctx, &tree);
        assert!(result.is_err());
    }

    #[test]
    fn test_diff_scan_single_column() {
        let mut ctx = test_ctx();
        let tree = scan(1, "t", "public", "t", &["val"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        assert_eq!(result.columns, vec!["val"]);
    }

    #[test]
    fn test_diff_scan_many_columns() {
        let mut ctx = test_ctx();
        let cols: Vec<&str> = (0..20)
            .map(|i| Box::leak(format!("c{i}").into_boxed_str()) as &str)
            .collect();
        let tree = scan(1, "wide", "public", "w", &cols);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        assert_eq!(result.columns.len(), 20);
    }

    // ── find_pk_columns tests ───────────────────────────────────────

    #[test]
    fn test_find_pk_columns_explicit() {
        let pk = vec!["id".to_string()];
        let cols = vec![col("id"), col("name")];
        assert_eq!(find_pk_columns(&pk, &cols), vec!["id"]);
    }

    #[test]
    fn test_find_pk_columns_fallback_all_columns() {
        // S10: Keyless table — falls back to all columns (no non-nullable heuristic).
        let pk: Vec<String> = vec![];
        let cols = vec![col_not_null("id"), col("name")];
        assert_eq!(find_pk_columns(&pk, &cols), vec!["id", "name"]);
    }

    #[test]
    fn test_find_pk_columns_fallback_all_nullable() {
        let pk: Vec<String> = vec![];
        let cols = vec![col("a"), col("b")];
        assert_eq!(find_pk_columns(&pk, &cols), vec!["a", "b"]);
    }

    // ── build_hash_expr tests ───────────────────────────────────────

    #[test]
    fn test_build_hash_expr_single() {
        let result = build_hash_expr(&["x".to_string()]);
        assert_eq!(result, "pgtrickle.pg_trickle_hash(x)");
    }

    #[test]
    fn test_build_hash_expr_multiple() {
        let result = build_hash_expr(&["a".to_string(), "b".to_string()]);
        assert!(result.contains("pgtrickle.pg_trickle_hash_multi"));
        assert!(result.contains("(a)::TEXT"));
        assert!(result.contains("(b)::TEXT"));
    }

    #[test]
    fn test_build_hash_expr_complex_expressions_parenthesized() {
        // Verify that complex expressions are wrapped in parens before ::TEXT
        // to prevent operator precedence issues (e.g. `a * b::TEXT` becoming
        // `a * (b::TEXT)` instead of the intended `(a * b)::TEXT`).
        let result = build_hash_expr(&[
            "l_extendedprice * (1 - l_discount)".to_string(),
            "volume".to_string(),
        ]);
        assert!(result.contains("(l_extendedprice * (1 - l_discount))::TEXT"));
        assert!(result.contains("(volume)::TEXT"));
    }

    // ── Transition table scan tests (IMMEDIATE mode) ────────────────

    fn transition_ctx(
        table_oid: u32,
        old_name: Option<&str>,
        new_name: Option<&str>,
    ) -> DiffContext {
        use crate::dvm::diff::{DeltaSource, TransitionTableNames};

        let mut tables = std::collections::HashMap::new();
        tables.insert(
            table_oid,
            TransitionTableNames {
                old_name: old_name.map(|s| s.to_string()),
                new_name: new_name.map(|s| s.to_string()),
            },
        );
        test_ctx().with_delta_source(DeltaSource::TransitionTable { tables })
    }

    #[test]
    fn test_diff_scan_transition_insert_only() {
        let mut ctx = transition_ctx(100, None, Some("__pgt_newtable_100"));
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should reference the new transition table
        assert_sql_contains(&sql, "__pgt_newtable_100");
        // Should have INSERT action
        assert_sql_contains(&sql, "'I'::TEXT AS __pgt_action");
        // Should NOT have DELETE action (no old table)
        assert!(!sql.contains("'D'::TEXT AS __pgt_action"));
        // Should NOT reference change buffer
        assert!(!sql.contains("pgtrickle_changes"));
    }

    #[test]
    fn test_diff_scan_transition_delete_only() {
        let mut ctx = transition_ctx(100, Some("__pgt_oldtable_100"), None);
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should reference the old transition table
        assert_sql_contains(&sql, "__pgt_oldtable_100");
        // Should have DELETE action
        assert_sql_contains(&sql, "'D'::TEXT AS __pgt_action");
        // Should NOT have INSERT action
        assert!(!sql.contains("'I'::TEXT AS __pgt_action"));
    }

    #[test]
    fn test_diff_scan_transition_update() {
        let mut ctx = transition_ctx(100, Some("__pgt_oldtable_100"), Some("__pgt_newtable_100"));
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should reference both transition tables
        assert_sql_contains(&sql, "__pgt_newtable_100");
        assert_sql_contains(&sql, "__pgt_oldtable_100");
        // Should have both DELETE and INSERT actions
        assert_sql_contains(&sql, "'D'::TEXT AS __pgt_action");
        assert_sql_contains(&sql, "'I'::TEXT AS __pgt_action");
        // Combined via UNION ALL
        assert_sql_contains(&sql, "UNION ALL");
    }

    #[test]
    fn test_diff_scan_transition_no_tables_emits_empty() {
        // Source OID not in the transition table map → empty delta
        use crate::dvm::diff::{DeltaSource, TransitionTableNames};

        let mut tables = std::collections::HashMap::new();
        tables.insert(
            999,
            TransitionTableNames {
                old_name: Some("__pgt_oldtable_999".to_string()),
                new_name: Some("__pgt_newtable_999".to_string()),
            },
        );
        let mut ctx = test_ctx().with_delta_source(DeltaSource::TransitionTable { tables });
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should have WHERE false for empty delta
        assert_sql_contains(&sql, "WHERE false");
    }

    #[test]
    fn test_diff_scan_transition_uses_pk_hash() {
        let mut ctx = transition_ctx(100, None, Some("__pgt_newtable_100"));
        let tree = scan_with_pk(100, "orders", "public", "o", &["id", "amount"], &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should compute row_id from PK column
        assert_sql_contains(&sql, "pgtrickle.pg_trickle_hash");
        assert_sql_contains(&sql, "\"id\"::TEXT");
    }

    #[test]
    fn test_diff_scan_transition_columns_preserved() {
        let mut ctx = transition_ctx(42, None, Some("__pgt_newtable_42"));
        let tree = scan(42, "items", "public", "i", &["id", "name", "price"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();

        assert_eq!(result.columns, vec!["id", "name", "price"]);
    }

    #[test]
    fn test_diff_scan_transition_no_lsn_filter() {
        let mut ctx = transition_ctx(100, None, Some("__pgt_newtable_100"));
        let tree = scan(100, "orders", "public", "o", &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Transition table mode should NOT have LSN filtering
        assert!(!sql.contains("pg_lsn"));
        assert!(!sql.contains("c.lsn"));
    }

    // ── EC-06: Keyless net-counting delta tests ────────────────────────

    #[test]
    fn test_diff_scan_keyless_uses_net_counting() {
        let mut ctx = test_ctx();
        // scan() creates keyless tables (empty pk_columns)
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should use generate_series for net counting
        assert_sql_contains(&sql, "generate_series");
        // Should have decomposition CTE with delta_sign
        assert_sql_contains(&sql, "delta_sign");
        // Should compute net counts
        assert_sql_contains(&sql, "SUM(delta_sign)");
    }

    #[test]
    fn test_diff_scan_keyless_never_deduplicated() {
        let mut ctx = test_ctx();
        ctx.merge_safe_dedup = true;
        // Even with merge_safe_dedup, keyless tables are never deduplicated
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();

        assert!(!result.is_deduplicated);
    }

    #[test]
    fn test_diff_scan_keyless_has_delete_and_insert_branches() {
        let mut ctx = test_ctx();
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should have both D and I action branches
        assert_sql_contains(&sql, "'D'::TEXT AS __pgt_action");
        assert_sql_contains(&sql, "'I'::TEXT AS __pgt_action");
    }

    #[test]
    fn test_diff_scan_keyless_decomposes_updates() {
        // A44-10 (D+I schema): UPDATEs are pre-decomposed at write time.
        // The keyless scan path should NOT contain c.action = 'U' branches.
        let mut ctx = test_ctx();
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // D+I schema: no action='U' branches
        assert_sql_not_contains(&sql, "c.action = 'U'");
        // Should have INSERT and DELETE branches only
        assert_sql_contains(&sql, "c.action = 'I'");
        assert_sql_contains(&sql, "c.action = 'D'");
        // Flat column references (no old_/new_ prefix)
        assert_sql_contains(&sql, "c.\"id\"");
        assert_sql_contains(&sql, "c.\"amount\"");
        assert_sql_not_contains(&sql, "old_id");
        assert_sql_not_contains(&sql, "old_amount");
    }

    #[test]
    fn test_diff_scan_keyless_no_distinct_on() {
        let mut ctx = test_ctx();
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Keyless path should NOT use DISTINCT ON
        assert!(!sql.contains("DISTINCT ON"));
    }

    #[test]
    fn test_diff_scan_pk_based_uses_distinct_on() {
        let mut ctx = test_ctx();
        let tree = scan_with_pk(100, "orders", "public", "o", &["id", "amount"], &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // PK-based tables should still use DISTINCT ON
        assert_sql_contains(&sql, "DISTINCT ON");
    }

    #[test]
    fn test_diff_scan_keyless_max_aggregation() {
        let mut ctx = test_ctx();
        let tree = scan(100, "orders", "public", "o", &["id", "amount"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should use array_agg(col)[1] to pick a representative value
        // for each content-hash group (works for all types, including
        // jsonb which lacks a comparison operator needed by MAX).
        assert_sql_contains(&sql, "array_agg(");
    }

    // ── P2-5 / WB-1: changed_cols VARBIT mask ──────────────────────

    #[test]
    fn test_compute_varbit_changed_cols_mask_basic() {
        use crate::dvm::parser::Column;
        let cdc_cols = vec!["id".into(), "name".into(), "amount".into(), "region".into()];
        // Pruned: only "id" and "amount" are referenced.
        let scan_cols = vec![
            Column {
                name: "id".into(),
                type_oid: 23,
                is_nullable: false,
            },
            Column {
                name: "amount".into(),
                type_oid: 23,
                is_nullable: true,
            },
        ];
        let result = compute_varbit_changed_cols_mask(&scan_cols, &cdc_cols);
        // id=pos0, name=pos1, amount=pos2, region=pos3
        // referenced: id(0), amount(2) → mask = '1010', zero = '0000'
        assert_eq!(result, Some(("1010".into(), "0000".into())));
    }

    #[test]
    fn test_compute_varbit_changed_cols_mask_all_columns() {
        use crate::dvm::parser::Column;
        let cdc_cols = vec!["a".into(), "b".into(), "c".into()];
        let scan_cols = vec![
            Column {
                name: "a".into(),
                type_oid: 23,
                is_nullable: false,
            },
            Column {
                name: "b".into(),
                type_oid: 23,
                is_nullable: false,
            },
            Column {
                name: "c".into(),
                type_oid: 23,
                is_nullable: false,
            },
        ];
        // All columns referenced — no filter needed.
        let result = compute_varbit_changed_cols_mask(&scan_cols, &cdc_cols);
        assert_eq!(result, None);
    }

    #[test]
    fn test_compute_varbit_changed_cols_mask_single_column() {
        use crate::dvm::parser::Column;
        let cdc_cols = vec!["id".into(), "name".into(), "status".into()];
        let scan_cols = vec![Column {
            name: "status".into(),
            type_oid: 25,
            is_nullable: true,
        }];
        // status at position 2 → mask = '001', zero = '000'
        let result = compute_varbit_changed_cols_mask(&scan_cols, &cdc_cols);
        assert_eq!(result, Some(("001".into(), "000".into())));
    }

    #[test]
    fn test_compute_varbit_changed_cols_mask_wide_table() {
        use crate::dvm::parser::Column;
        // >63 columns — WB-1 must handle these without cap.
        let cdc_cols: Vec<String> = (0..70).map(|i| format!("col{i}")).collect();
        let scan_cols = vec![
            Column {
                name: "col0".into(),
                type_oid: 23,
                is_nullable: false,
            },
            Column {
                name: "col64".into(),
                type_oid: 23,
                is_nullable: false,
            },
        ];
        let result = compute_varbit_changed_cols_mask(&scan_cols, &cdc_cols);
        let (mask, zero) = result.expect("should produce a filter for wide tables");
        assert_eq!(mask.len(), 70);
        assert_eq!(zero.len(), 70);
        assert_eq!(&mask[..1], "1"); // col0 referenced
        assert_eq!(&mask[1..64], &"0".repeat(63)); // cols 1-63 not referenced
        assert_eq!(&mask[64..65], "1"); // col64 referenced
        assert_eq!(&mask[65..], &"0".repeat(5)); // cols 65-69 not referenced
        assert_eq!(zero, "0".repeat(70));
    }

    #[test]
    fn test_p2_5_bitmask_filter_in_sql() {
        // When source_cdc_columns is populated and columns are pruned,
        // the generated SQL should contain a changed_cols VARBIT filter.
        let mut ctx = test_ctx();
        ctx.source_cdc_columns.insert(
            100,
            vec!["id".into(), "name".into(), "amount".into(), "region".into()],
        );
        // Scan only references "id" and "amount" (2 of 4 columns).
        let tree = scan_with_pk(100, "orders", "public", "o", &["id", "amount"], &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // id=pos0, amount=pos2 → mask='1010', zero='0000', mask_len=4
        assert_sql_contains(
            &sql,
            "c.changed_cols IS NULL OR CASE WHEN bit_length(c.changed_cols) = 4 THEN (c.changed_cols & B'1010') != B'0000' ELSE TRUE END",
        );
    }

    #[test]
    fn test_p2_5_bitmask_filter_skipped_when_all_columns() {
        // When all CDC columns are referenced, no bitmask filter is added.
        let mut ctx = test_ctx();
        ctx.source_cdc_columns
            .insert(100, vec!["id".into(), "amount".into()]);
        // Scan references ALL CDC columns.
        let tree = scan_with_pk(100, "orders", "public", "o", &["id", "amount"], &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should NOT have a changed_cols filter
        assert!(!sql.contains("changed_cols"));
    }

    #[test]
    fn test_p2_5_bitmask_filter_skipped_for_keyless() {
        // Keyless tables should not get a changed_cols filter.
        let mut ctx = test_ctx();
        ctx.source_cdc_columns
            .insert(100, vec!["id".into(), "name".into(), "amount".into()]);
        // Keyless scan (no pk_columns).
        let tree = scan(100, "orders", "public", "o", &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Should NOT have a changed_cols filter
        assert!(!sql.contains("changed_cols"));
    }

    // ── P2-7: predicate pushdown ────────────────────────────────────

    #[test]
    fn test_is_predicate_pushable_column_ref_qualified() {
        let cols = vec!["id".into(), "status".into()];
        let expr = Expr::BinaryOp {
            op: "=".into(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("o".into()),
                column_name: "status".into(),
            }),
            right: Box::new(Expr::Literal("'shipped'".into())),
        };
        assert!(is_predicate_pushable_to_scan(&expr, "o", &cols));
    }

    #[test]
    fn test_is_predicate_pushable_unqualified() {
        let cols = vec!["id".into(), "amount".into()];
        let expr = Expr::BinaryOp {
            op: ">".into(),
            left: Box::new(Expr::ColumnRef {
                table_alias: None,
                column_name: "amount".into(),
            }),
            right: Box::new(Expr::Literal("100".into())),
        };
        assert!(is_predicate_pushable_to_scan(&expr, "o", &cols));
    }

    #[test]
    fn test_is_predicate_not_pushable_wrong_alias() {
        let cols = vec!["id".into()];
        let expr = Expr::ColumnRef {
            table_alias: Some("other".into()),
            column_name: "id".into(),
        };
        assert!(!is_predicate_pushable_to_scan(&expr, "o", &cols));
    }

    #[test]
    fn test_is_predicate_not_pushable_raw() {
        let cols = vec!["id".into()];
        let expr = Expr::Raw("id > 5".into());
        assert!(!is_predicate_pushable_to_scan(&expr, "o", &cols));
    }

    #[test]
    fn test_rewrite_predicate_for_scan_old() {
        let expr = Expr::BinaryOp {
            op: "=".into(),
            left: Box::new(Expr::ColumnRef {
                table_alias: Some("o".into()),
                column_name: "status".into(),
            }),
            right: Box::new(Expr::Literal("'shipped'".into())),
        };
        let sql = rewrite_predicate_for_scan(&expr, "old_");
        assert_eq!(sql, "(c.\"old_status\" = 'shipped')");
    }

    #[test]
    fn test_rewrite_predicate_for_scan_new() {
        let expr = Expr::BinaryOp {
            op: "AND".into(),
            left: Box::new(Expr::BinaryOp {
                op: "=".into(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: None,
                    column_name: "status".into(),
                }),
                right: Box::new(Expr::Literal("'shipped'".into())),
            }),
            right: Box::new(Expr::BinaryOp {
                op: ">".into(),
                left: Box::new(Expr::ColumnRef {
                    table_alias: None,
                    column_name: "amount".into(),
                }),
                right: Box::new(Expr::Literal("100".into())),
            }),
        };
        let sql = rewrite_predicate_for_scan(&expr, "new_");
        assert_eq!(
            sql,
            "((c.\"new_status\" = 'shipped') AND (c.\"new_amount\" > 100))"
        );
    }

    #[test]
    fn test_p2_7_pushed_filter_in_sql() {
        // A44-10 (D+I schema): pushed filter uses flat c."col" (no old_/new_ prefix)
        let mut ctx = test_ctx();
        ctx.scan_pushed_predicate = Some(Expr::BinaryOp {
            op: "=".into(),
            left: Box::new(Expr::ColumnRef {
                table_alias: None,
                column_name: "status".into(),
            }),
            right: Box::new(Expr::Literal("'shipped'".into())),
        });
        let tree = scan_with_pk(100, "orders", "public", "o", &["id", "status"], &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        // Both DELETE and INSERT branches use flat c."status"
        assert_sql_contains(&sql, "c.\"status\" = 'shipped'");
        // No old_/new_ prefixed column refs
        assert_sql_not_contains(&sql, "c.\"old_status\"");
        assert_sql_not_contains(&sql, "c.\"new_status\"");
    }

    #[test]
    fn test_p2_7_no_pushed_filter_without_predicate() {
        // Without a pushed predicate, no old_/new_ filter clauses.
        let mut ctx = test_ctx();
        let tree = scan_with_pk(100, "orders", "public", "o", &["id", "status"], &["id"]);
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);

        assert!(!sql.contains("old_status\" ="));
        assert!(!sql.contains("new_status\" ="));
    }

    // ── A-2: Key-column mask tests ──────────────────────────────────

    #[test]
    fn test_compute_varbit_key_cols_mask_basic() {
        // 4 CDC cols, 1 is a key column
        let cdc_cols: Vec<String> = vec![
            "id".into(),
            "region".into(),
            "amount".into(),
            "notes".into(),
        ];
        let key_cols: Vec<String> = vec!["region".into()];
        let result = compute_varbit_key_cols_mask(&key_cols, &cdc_cols);
        assert_eq!(result, Some(("0100".to_string(), "0000".to_string())));
    }

    #[test]
    fn test_compute_varbit_key_cols_mask_multiple_keys() {
        let cdc_cols: Vec<String> = vec!["a".into(), "b".into(), "c".into(), "d".into()];
        let key_cols: Vec<String> = vec!["a".into(), "c".into()];
        let result = compute_varbit_key_cols_mask(&key_cols, &cdc_cols);
        assert_eq!(result, Some(("1010".to_string(), "0000".to_string())));
    }

    #[test]
    fn test_compute_varbit_key_cols_mask_all_keys() {
        // When all columns are key columns, no value-only optimization
        let cdc_cols: Vec<String> = vec!["a".into(), "b".into()];
        let key_cols: Vec<String> = vec!["a".into(), "b".into()];
        let result = compute_varbit_key_cols_mask(&key_cols, &cdc_cols);
        assert_eq!(result, None, "all-key mask should return None");
    }

    #[test]
    fn test_compute_varbit_key_cols_mask_no_keys() {
        // No key columns → no mask
        let cdc_cols: Vec<String> = vec!["a".into(), "b".into()];
        let key_cols: Vec<String> = vec![];
        let result = compute_varbit_key_cols_mask(&key_cols, &cdc_cols);
        assert_eq!(result, None, "empty key cols should return None");
    }

    #[test]
    fn test_compute_varbit_key_cols_mask_no_match() {
        // Key column name doesn't match any CDC column
        let cdc_cols: Vec<String> = vec!["a".into(), "b".into()];
        let key_cols: Vec<String> = vec!["x".into()];
        let result = compute_varbit_key_cols_mask(&key_cols, &cdc_cols);
        assert_eq!(result, None, "non-matching key should return None");
    }

    #[test]
    fn test_a2_key_changed_annotation_in_scan_sql() {
        // When source_key_columns has entries for a scan' source table,
        // the generated delta SQL should contain __pgt_key_changed.
        let mut ctx = test_ctx();
        ctx.source_cdc_columns
            .insert(100, vec!["id".into(), "region".into(), "amount".into()]);
        ctx.source_key_columns.insert(100, vec!["region".into()]);

        let tree = scan_with_pk(
            100,
            "orders",
            "public",
            "o",
            &["id", "region", "amount"],
            &["id"],
        );
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert!(
            sql.contains("__pgt_key_changed"),
            "delta SQL should contain __pgt_key_changed column when key cols are set"
        );
    }

    #[test]
    fn test_a2_no_key_changed_without_key_cols() {
        // Without source_key_columns, no __pgt_key_changed annotation.
        let mut ctx = test_ctx();
        let tree = scan_with_pk(
            100,
            "orders",
            "public",
            "o",
            &["id", "region", "amount"],
            &["id"],
        );
        let result = diff_scan(&mut ctx, &tree).unwrap();
        let sql = ctx.build_with_query(&result.cte_name);
        assert!(
            !sql.contains("__pgt_key_changed"),
            "delta SQL should NOT contain __pgt_key_changed without key cols"
        );
    }
}
