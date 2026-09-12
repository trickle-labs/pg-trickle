// Sub-module of src/refresh/merge — see mod.rs for overview.
#[allow(unused_imports)]
use super::*;

pub(crate) fn pg_quote_literal(val: &str) -> String {
    format!("'{}'", val.replace('\'', "''"))
}

/// A1-2/A1-1b/A1-1d: Per-column bounds for partition pruning predicates.
///
/// **Range** — min/max vectors (one entry per partition key column).
/// **List**  — distinct values for the single LIST column.
pub(crate) enum PartitionBounds {
    Range {
        mins: Vec<String>,
        maxs: Vec<String>,
    },
    List(Vec<String>),
}

/// A1-2/A1-1b/A1-1d: Extract the partition bounds from the resolved delta SQL.
/// Returns `None` when the delta is empty.
///
/// * **RANGE** keys → `MIN/MAX` per column.
/// * **LIST** keys  → `SELECT DISTINCT col::text` (single column).
pub(crate) fn extract_partition_bounds(
    resolved_delta_sql: &str,
    partition_key: &str,
) -> Result<Option<PartitionBounds>, PgTrickleError> {
    let method = crate::api::parse_partition_method(partition_key);
    let cols = crate::api::parse_partition_key_columns(partition_key);

    match method {
        crate::api::PartitionMethod::Hash => {
            // HASH partitions use a parent-level MERGE, so this function
            // should never be called for HASH.
            Err(PgTrickleError::SpiError(
                "extract_partition_bounds called for HASH partition (should use parent-level MERGE)".to_string(),
            ))
        }
        crate::api::PartitionMethod::List => {
            // LIST: single column — collect distinct values.
            let qcol = crate::api::quote_identifier(&cols[0]);
            let sql = format!(
                "SELECT DISTINCT {qcol}::text FROM ({resolved_delta_sql}) AS __pgt_part_probe ORDER BY 1"
            );
            let result = Spi::connect(|client| {
                let rows = client
                    .select(&sql, None, &[])
                    .map_err(|e| PgTrickleError::SpiError(format!("partition list: {e}")))?;
                let mut values = Vec::new();
                for row in rows {
                    if let Some(v) = row
                        .get::<String>(1)
                        .map_err(|e| PgTrickleError::SpiError(format!("partition list col: {e}")))?
                    {
                        values.push(v);
                    }
                }
                if values.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(PartitionBounds::List(values)))
                }
            })?;
            Ok(result)
        }
        crate::api::PartitionMethod::Range => {
            // RANGE: min/max per column.
            let min_exprs: Vec<String> = cols
                .iter()
                .map(|c| format!("MIN({})::text", crate::api::quote_identifier(c)))
                .collect();
            let max_exprs: Vec<String> = cols
                .iter()
                .map(|c| format!("MAX({})::text", crate::api::quote_identifier(c)))
                .collect();
            let select_clause = min_exprs
                .iter()
                .chain(max_exprs.iter())
                .cloned()
                .collect::<Vec<_>>()
                .join(", ");
            let sql =
                format!("SELECT {select_clause} FROM ({resolved_delta_sql}) AS __pgt_part_probe");
            let result = Spi::connect(|client| {
                let row = client
                    .select(&sql, None, &[])
                    .map_err(|e| PgTrickleError::SpiError(format!("partition range: {e}")))?
                    .first();
                let n = cols.len();
                let mut mins = Vec::with_capacity(n);
                let mut maxs = Vec::with_capacity(n);
                for i in 0..n {
                    let map_spi = |e: pgrx::spi::SpiError| {
                        PgTrickleError::SpiError(format!("partition range col {i}: {e}"))
                    };
                    match row.get::<String>(i + 1).map_err(map_spi)? {
                        Some(v) => mins.push(v),
                        None => return Ok(None), // delta is empty
                    }
                }
                for i in 0..n {
                    let map_spi = |e: pgrx::spi::SpiError| {
                        PgTrickleError::SpiError(format!("partition range col {i}: {e}"))
                    };
                    match row.get::<String>(n + i + 1).map_err(map_spi)? {
                        Some(v) => maxs.push(v),
                        None => return Ok(None),
                    }
                }
                Ok(Some(PartitionBounds::Range { mins, maxs }))
            })?;
            Ok(result)
        }
    }
}

/// A1-3/A1-1b/A1-1d: Replace the `__PGT_PART_PRED__` placeholder in the MERGE
/// SQL with a partition-pruning predicate for the current delta.
///
/// * **Single-column RANGE**: `AND st."col" BETWEEN '<min>' AND '<max>'`
/// * **Multi-column RANGE**: `AND ROW(st."a", st."b") >= ROW(...) AND ROW(...) <= ROW(...)`
/// * **LIST**: `AND st."col" IN ('v1', 'v2', ...)`
pub(crate) fn inject_partition_predicate(
    merge_sql: &str,
    partition_key: &str,
    bounds: &PartitionBounds,
) -> String {
    let cols = crate::api::parse_partition_key_columns(partition_key);
    let pred = match bounds {
        PartitionBounds::List(values) => {
            let qk = crate::api::quote_identifier(&cols[0]);
            let literals: Vec<String> = values.iter().map(|v| pg_quote_literal(v)).collect();
            format!(" AND st.{qk} IN ({})", literals.join(", "))
        }
        PartitionBounds::Range { mins, maxs } => {
            if cols.len() == 1 {
                // Single-column: simple BETWEEN (backward compatible)
                let qk = crate::api::quote_identifier(&cols[0]);
                format!(
                    " AND st.{qk} BETWEEN {} AND {}",
                    pg_quote_literal(&mins[0]),
                    pg_quote_literal(&maxs[0]),
                )
            } else {
                // Multi-column: ROW comparison
                let st_cols: Vec<String> = cols
                    .iter()
                    .map(|c| format!("st.{}", crate::api::quote_identifier(c)))
                    .collect();
                let min_literals: Vec<String> = mins.iter().map(|v| pg_quote_literal(v)).collect();
                let max_literals: Vec<String> = maxs.iter().map(|v| pg_quote_literal(v)).collect();
                format!(
                    " AND ROW({}) >= ROW({}) AND ROW({}) <= ROW({})",
                    st_cols.join(", "),
                    min_literals.join(", "),
                    st_cols.join(", "),
                    max_literals.join(", "),
                )
            }
        }
    };
    merge_sql.replace("__PGT_PART_PRED__", &pred)
}

// ── HASH partition bound parsing ───────────────────────────────────

/// Parse a PostgreSQL HASH partition bound spec.
///
/// Input: `"FOR VALUES WITH (modulus 4, remainder 2)"`
/// Returns: `(4, 2)`
pub(crate) fn parse_hash_bound_spec(spec: &str) -> Result<(i32, i32), PgTrickleError> {
    // Parsing pattern: "FOR VALUES WITH (modulus N, remainder M)"
    let upper = spec.to_uppercase();
    let modulus = extract_keyword_int(&upper, "MODULUS")?;
    let remainder = extract_keyword_int(&upper, "REMAINDER")?;
    Ok((modulus, remainder))
}

/// Extract an integer value following a keyword in a partition bound spec.
pub(crate) fn extract_keyword_int(spec: &str, keyword: &str) -> Result<i32, PgTrickleError> {
    let pos = spec
        .find(keyword)
        .ok_or_else(|| PgTrickleError::SpiError(format!("missing {keyword} in bound spec")))?;
    let after = &spec[pos + keyword.len()..];
    let digits: String = after
        .chars()
        .skip_while(|c| !c.is_ascii_digit())
        .take_while(|c| c.is_ascii_digit())
        .collect();
    digits
        .parse::<i32>()
        .map_err(|_| PgTrickleError::SpiError(format!("invalid {keyword} value in bound spec")))
}

// ── PART-WARN: Default partition growth warning ─────────────────────

/// After a successful refresh of a partitioned stream table, check whether
/// the default (catch-all) partition has rows. If so, emit a WARNING
/// prompting the user to create explicit named partitions.
///
/// The check is deliberately lightweight: a single `count(*)` on the default
/// partition. If the default partition does not exist (unlikely but possible
/// if the user detached it), the check is silently skipped.
pub(crate) fn warn_default_partition_growth(schema: &str, name: &str) {
    let default_name = format!("{name}_default");
    let qschema = crate::api::quote_identifier(schema);
    let qdefault = crate::api::quote_identifier(&default_name);

    // Check existence first via pg_catalog to avoid "relation does not exist"
    // errors from SPI (pgrx SPI does not catch catalog errors via Result).
    // Use parameterized query to safely pass schema/table names.
    let exists = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT 1 FROM pg_catalog.pg_class c \
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname = $2 \
                 LIMIT 1",
                None,
                &[schema.into(), default_name.as_str().into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Ok::<bool, PgTrickleError>(!rows.is_empty())
    })
    .unwrap_or(false);

    if !exists {
        return; // No default partition — nothing to warn about.
    }

    let sql = format!("SELECT count(*)::bigint FROM {qschema}.{qdefault}");
    match Spi::get_one::<i64>(&sql) {
        Ok(Some(count)) if count > 0 => {
            pgrx::warning!(
                "pg_trickle: PART-WARN: default partition {schema}.{default_name} of \
                 stream table {schema}.{name} contains {count} row(s). \
                 Create explicit named partitions to improve query performance and \
                 enable partition pruning. Example:\n  \
                 CREATE TABLE {schema}.{name}_2026q1 PARTITION OF {schema}.{name} \
                 FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');"
            );
        }
        Ok(_) => {}  // Default partition is empty — no warning.
        Err(_) => {} // Silently skip on any other error.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── pg_quote_literal tests (TEST-1) ────────────────────────────────────

    #[test]
    fn test_pg_quote_literal_simple() {
        assert_eq!(pg_quote_literal("hello"), "'hello'");
    }

    #[test]
    fn test_pg_quote_literal_escapes_single_quote() {
        assert_eq!(pg_quote_literal("it's"), "'it''s'");
    }

    #[test]
    fn test_pg_quote_literal_empty_string() {
        assert_eq!(pg_quote_literal(""), "''");
    }

    // ── parse_hash_bound_spec tests (TEST-1) ────────────────────────────────

    #[test]
    fn test_parse_hash_bound_spec_basic() {
        let (m, r) = parse_hash_bound_spec("FOR VALUES WITH (modulus 4, remainder 2)").unwrap();
        assert_eq!(m, 4);
        assert_eq!(r, 2);
    }

    #[test]
    fn test_parse_hash_bound_spec_single_shard() {
        let (m, r) = parse_hash_bound_spec("FOR VALUES WITH (modulus 1, remainder 0)").unwrap();
        assert_eq!(m, 1);
        assert_eq!(r, 0);
    }

    #[test]
    fn test_parse_hash_bound_spec_missing_modulus_errors() {
        assert!(parse_hash_bound_spec("FOR VALUES WITH (remainder 2)").is_err());
    }

    #[test]
    fn test_parse_hash_bound_spec_missing_remainder_errors() {
        assert!(parse_hash_bound_spec("FOR VALUES WITH (modulus 4)").is_err());
    }

    // ── extract_keyword_int tests (TEST-1) ─────────────────────────────────

    #[test]
    fn test_extract_keyword_int_finds_value() {
        assert_eq!(
            extract_keyword_int("MODULUS 8, REMAINDER 3", "MODULUS").unwrap(),
            8
        );
    }

    #[test]
    fn test_extract_keyword_int_missing_keyword_errors() {
        assert!(extract_keyword_int("REMAINDER 3", "MODULUS").is_err());
    }

    #[test]
    fn test_extract_keyword_int_handles_large_value() {
        assert_eq!(
            extract_keyword_int("MODULUS 1024, REMAINDER 0", "MODULUS").unwrap(),
            1024
        );
    }
}
