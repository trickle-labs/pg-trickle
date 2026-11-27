// Sub-module of src/refresh/merge — see mod.rs for overview.
#[allow(unused_imports)]
use super::*;

pub(crate) fn pg_quote_literal(val: &str) -> String {
    format!("'{}'", val.replace('\'', "''"))
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
