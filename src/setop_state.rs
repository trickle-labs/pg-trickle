//! REL-101-1 / REL-101-3: durable, private per-branch multiplicity state for
//! `INTERSECT` / `EXCEPT` stream tables.
//!
//! Set operations preserve PostgreSQL multiset semantics by tracking, for each
//! semantic output row, how many times it appears in the left branch and how
//! many times it appears in the right branch. The visible stream-table relation
//! must only ever contain the user-facing result rows — never these branch
//! counters and never the rows that are currently invisible under the operation
//! (for example a value present in both branches of `EXCEPT`). This module owns
//! a **separate, private, extension-owned** state relation
//! (`pgtrickle.__pgt_setop_state_<pgt_id>`) that stores those multiplicities so
//! that they never leak through the stream table.
//!
//! The state is populated during FULL refresh (the authoritative path) and is
//! the durable source of old branch counts for a future differential path.
//! Differential maintenance of set operations remains FULL-only (fail-closed)
//! until it is proven; this module never changes the visible stream-table
//! contents.
//!
//! ## Privacy and ownership
//!
//! The state relation lives in the `pgtrickle` schema, is `REVOKE`d from
//! `PUBLIC`, is a permanent extension member, and is registered in
//! `pgtrickle.pgt_set_operation_states`. It is created and populated inside the
//! same refresh transaction as the visible FULL refresh, so a rollback discards
//! both. Population reads the user source tables under the stream owner's role
//! and stored `search_path`; the extension-privileged role only ever copies the
//! already-materialized rows out of a session-local temp table.

use pgrx::prelude::*;

use crate::catalog::StreamTableMeta;
use crate::error::PgTrickleError;

/// Physical layout version of the private state relation. Bump when the column
/// shape of the state relation changes so that stale relations are rebuilt.
pub(crate) const SETOP_STATE_SCHEMA_VERSION: i16 = 1;

/// A catalogued private-state relation for a stream table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SetOpStateEntry {
    pub(crate) pgt_id: i64,
    pub(crate) node_ordinal: i32,
    pub(crate) operation: String,
    pub(crate) is_all: bool,
    pub(crate) state_relid: pg_sys::Oid,
    pub(crate) schema_version: i16,
}

fn spi_error(error: impl std::fmt::Display) -> PgTrickleError {
    PgTrickleError::SpiError(error.to_string())
}

/// Bare (unqualified) name of the private state relation for a stream table.
pub(crate) fn state_relation_basename(pgt_id: i64) -> String {
    format!("__pgt_setop_state_{pgt_id}")
}

/// Schema-qualified, quoted name of the private state relation.
pub(crate) fn qualified_state_relation(pgt_id: i64) -> String {
    format!(
        "pgtrickle.{}",
        crate::api::quote_identifier(&state_relation_basename(pgt_id))
    )
}

/// Session-local temp table used to stage branch multiplicities under the
/// stream owner before they are copied into the private state relation.
fn staging_relation_basename(pgt_id: i64) -> String {
    format!("__pgt_setop_src_{pgt_id}")
}

/// Read the catalogued private-state rows for a stream table.
pub(crate) fn entries_for_stream(pgt_id: i64) -> Result<Vec<SetOpStateEntry>, PgTrickleError> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT node_ordinal, operation, is_all, state_relid, schema_version \
                 FROM pgtrickle.pgt_set_operation_states \
                 WHERE pgt_id = $1 \
                 ORDER BY node_ordinal",
                None,
                &[pgt_id.into()],
            )
            .map_err(spi_error)?;

        let mut entries = Vec::new();
        for row in rows {
            let node_ordinal = row.get::<i32>(1).map_err(spi_error)?.ok_or_else(|| {
                PgTrickleError::InternalError("setop state node_ordinal was NULL".into())
            })?;
            let operation = row.get::<String>(2).map_err(spi_error)?.ok_or_else(|| {
                PgTrickleError::InternalError("setop state operation was NULL".into())
            })?;
            let is_all = row.get::<bool>(3).map_err(spi_error)?.unwrap_or(false);
            let state_relid = row
                .get::<pg_sys::Oid>(4)
                .map_err(spi_error)?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("setop state state_relid was NULL".into())
                })?;
            let schema_version = row.get::<i16>(5).map_err(spi_error)?.unwrap_or(0);
            entries.push(SetOpStateEntry {
                pgt_id,
                node_ordinal,
                operation,
                is_all,
                state_relid,
                schema_version,
            });
        }
        Ok(entries)
    })
}

/// Facts about a candidate state relation, used to refuse touching relations
/// that are not the extension's own private state.
struct RelationFacts {
    schema: String,
    name: String,
    relkind: String,
    persistence: String,
    owner_matches: bool,
    extension_member: bool,
}

fn relation_facts(oid: pg_sys::Oid) -> Result<Option<RelationFacts>, PgTrickleError> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT n.nspname::text, c.relname::text, c.relkind::text, \
                        c.relpersistence::text, \
                        (c.relowner = (SELECT extowner FROM pg_extension \
                                       WHERE extname = 'pg_trickle')) AS owner_matches, \
                        EXISTS (SELECT 1 FROM pg_depend d \
                                JOIN pg_extension e ON e.oid = d.refobjid \
                                WHERE d.classid = 'pg_class'::regclass \
                                  AND d.objid = c.oid \
                                  AND d.deptype = 'e' \
                                  AND e.extname = 'pg_trickle') AS extension_member \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE c.oid = $1",
                None,
                &[oid.into()],
            )
            .map_err(spi_error)?;

        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some(RelationFacts {
            schema: row.get::<String>(1).map_err(spi_error)?.unwrap_or_default(),
            name: row.get::<String>(2).map_err(spi_error)?.unwrap_or_default(),
            relkind: row.get::<String>(3).map_err(spi_error)?.unwrap_or_default(),
            persistence: row.get::<String>(4).map_err(spi_error)?.unwrap_or_default(),
            owner_matches: row.get::<bool>(5).map_err(spi_error)?.unwrap_or(false),
            extension_member: row.get::<bool>(6).map_err(spi_error)?.unwrap_or(false),
        }))
    })
}

/// Whether a relation OID is safe for this module to drop: it must be the
/// expected private state relation in `pgtrickle`, a permanent ordinary table,
/// owned by the extension owner, and an extension member.
fn is_owned_state_relation(pgt_id: i64, oid: pg_sys::Oid) -> Result<bool, PgTrickleError> {
    let Some(facts) = relation_facts(oid)? else {
        return Ok(false);
    };
    Ok(facts.schema == "pgtrickle"
        && facts.name == state_relation_basename(pgt_id)
        && facts.relkind == "r"
        && facts.persistence == "p"
        && facts.owner_matches
        && facts.extension_member)
}

/// Drop the private state relation (if present and owned) and delete its
/// catalog rows. Idempotent — safe to call when no state exists.
///
/// REL-101-3: invoked on stream-table drop, on query/identity change, and
/// before a rebuild so a stale relation cannot outlive its owner.
pub(crate) fn drop_for_stream(pgt_id: i64) -> Result<(), PgTrickleError> {
    let entries = entries_for_stream(pgt_id)?;
    let mut relids: Vec<pg_sys::Oid> = entries.iter().map(|entry| entry.state_relid).collect();

    // Also consider a deterministically-named relation that may exist without a
    // catalog row (for example if a prior transaction failed mid-rebuild).
    if let Some(oid) = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass($1)::oid",
        &[qualified_state_relation(pgt_id).into()],
    )
    .map_err(spi_error)?
    .filter(|oid| !relids.contains(oid))
    {
        relids.push(oid);
    }

    for oid in relids {
        if !is_owned_state_relation(pgt_id, oid)? {
            // A missing relation is already gone; a foreign relation must never
            // be dropped. Either way, skip it and continue cleanup so a corrupt
            // registry cannot make DROP impossible.
            continue;
        }
        let qualified = qualified_state_relation(pgt_id);
        // nosemgrep: rust.spi.run.dynamic-format — qualified is quote_identifier-escaped and validated above.
        Spi::run(&format!(
            "ALTER EXTENSION pg_trickle DROP TABLE {qualified}"
        ))
        .map_err(spi_error)?;
        // nosemgrep: rust.spi.run.dynamic-format — qualified is quote_identifier-escaped and validated above.
        Spi::run(&format!("DROP TABLE {qualified}")).map_err(spi_error)?;
    }

    Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_set_operation_states WHERE pgt_id = $1",
        &[pgt_id.into()],
    )
    .map_err(spi_error)?;
    Ok(())
}

/// Register (or replace) the catalog row describing the private state relation.
fn register_state(
    pgt_id: i64,
    operation: &str,
    is_all: bool,
    state_relid: pg_sys::Oid,
) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_set_operation_states \
             (pgt_id, node_ordinal, operation, is_all, state_relid, schema_version) \
         VALUES ($1, 0, $2, $3, $4, $5) \
         ON CONFLICT (pgt_id, node_ordinal) DO UPDATE SET \
             operation = EXCLUDED.operation, \
             is_all = EXCLUDED.is_all, \
             state_relid = EXCLUDED.state_relid, \
             schema_version = EXCLUDED.schema_version",
        &[
            pgt_id.into(),
            operation.into(),
            is_all.into(),
            state_relid.into(),
            (SETOP_STATE_SCHEMA_VERSION).into(),
        ],
    )
    .map_err(spi_error)
}

/// REL-101-1: (Re)build the durable private multiplicity state for a
/// set-operation stream table during FULL refresh.
///
/// Returns `Ok(true)` when a state relation was (re)built, `Ok(false)` when the
/// stream table is not a set operation (in which case any stale state is
/// cleaned up). Population is unconditional for set-operation stream tables:
/// the private multiplicity state is a standard, always-maintained artifact of
/// a set-operation FULL refresh, never exposed through the visible stream
/// table.
///
/// Must be called after the visible stream table has been materialized, inside
/// the same refresh transaction.
pub(crate) fn rebuild_for_full_refresh(st: &StreamTableMeta) -> Result<bool, PgTrickleError> {
    // Classify the query first — non-set-operation stream tables never carry
    // set-operation state, and any leftover state for such a table is stale.
    let Some((operation, is_all)) = crate::dvm::classify_top_level_set_op(&st.defining_query)
    else {
        drop_for_stream(st.pgt_id)?;
        return Ok(false);
    };

    let columns = crate::refresh::get_st_user_columns(st);
    if columns.is_empty() {
        return Err(PgTrickleError::InternalError(format!(
            "set-operation stream table {}.{} has no user columns",
            st.pgt_schema, st.pgt_name
        )));
    }

    let Some(population_sql) = crate::dvm::try_set_op_refresh_sql(&st.defining_query, &columns)
    else {
        // Classification succeeded but the SQL builder declined (e.g. the query
        // is not a simple top-level set operation). Stay fail-closed.
        drop_for_stream(st.pgt_id)?;
        return Ok(false);
    };

    // Remove any prior relation/catalog row before rebuilding so the relation
    // identity and generation are always fresh.
    drop_for_stream(st.pgt_id)?;

    let staging = format!(
        "pg_temp.{}",
        crate::api::quote_identifier(&staging_relation_basename(st.pgt_id))
    );

    // Step 1: stage branch multiplicities under the stream owner so the source
    // tables are read with the correct role, RLS, and search_path.
    crate::refresh::with_stream_owner(st, || {
        Spi::run(&format!("DROP TABLE IF EXISTS {staging}")) // nosemgrep: rust.spi.run.dynamic-format -- staging is pg_temp-qualified and quote_identifier-escaped.
            .map_err(spi_error)?;
        // nosemgrep: rust.spi.run.dynamic-format -- staging is quoted; population_sql is generated from validated column names by try_set_op_refresh_sql.
        Spi::run(&format!(
            "CREATE TEMP TABLE {staging} ON COMMIT DROP AS {population_sql}"
        ))
        .map_err(|e| {
            PgTrickleError::SpiError(format!(
                "set-operation state population failed: {e}; SQL: {population_sql}"
            ))
        })?;
        Ok(())
    })?;

    // Step 2: as the extension-privileged role, copy the staged rows into the
    // private permanent relation. This only reads the session-local temp table
    // (never the user source tables directly) and creates DDL in `pgtrickle`.
    let qualified = qualified_state_relation(st.pgt_id);

    // nosemgrep: rust.spi.run.dynamic-format -- qualified and staging are quoted identifiers.
    Spi::run(&format!(
        "CREATE TABLE {qualified} AS TABLE {staging} WITH NO DATA"
    ))
    .map_err(spi_error)?;
    // nosemgrep: rust.spi.run.dynamic-format -- qualified and staging are quoted identifiers.
    Spi::run(&format!("INSERT INTO {qualified} SELECT * FROM {staging}")).map_err(spi_error)?;

    // The semantic row identity is unique; enforce it so probe joins are exact.
    let index_name = crate::api::quote_identifier(&format!("__pgt_setop_state_{}_pk", st.pgt_id));
    // nosemgrep: rust.spi.run.dynamic-format -- identifiers are quote_identifier-escaped.
    Spi::run(&format!(
        "CREATE UNIQUE INDEX {index_name} ON {qualified} (__pgt_row_id)"
    ))
    .map_err(spi_error)?;

    // Privacy: never expose the private state to other roles.
    // nosemgrep: rust.spi.run.dynamic-format -- qualified is a quoted identifier.
    Spi::run(&format!("REVOKE ALL ON TABLE {qualified} FROM PUBLIC")).map_err(spi_error)?;
    let owner = crate::api::quote_identifier(&crate::refresh::stream_owner_name(st)?);
    // nosemgrep: rust.spi.run.dynamic-format -- qualified and owner are quoted identifiers.
    Spi::run(&format!("GRANT SELECT ON TABLE {qualified} TO {owner}")).map_err(spi_error)?;

    // Make the relation a permanent extension member so it is preserved and
    // dropped with the extension lifecycle.
    // nosemgrep: rust.spi.run.dynamic-format -- qualified is a quoted identifier.
    Spi::run(&format!("ALTER EXTENSION pg_trickle ADD TABLE {qualified}")).map_err(spi_error)?;

    let state_relid = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass($1)::oid",
        &[qualified.clone().into()],
    )
    .map_err(spi_error)?
    .ok_or_else(|| {
        PgTrickleError::InternalError(format!(
            "private set-operation state relation {qualified} vanished after creation"
        ))
    })?;

    register_state(st.pgt_id, operation, is_all, state_relid)?;

    // Best-effort staging cleanup under the owner (ON COMMIT DROP is the safety
    // net if this fails).
    crate::refresh::drop_owner_temp_table(st, &staging_relation_basename(st.pgt_id));

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_relation_names_are_deterministic() {
        assert_eq!(state_relation_basename(7), "__pgt_setop_state_7");
        assert_eq!(
            qualified_state_relation(7),
            "pgtrickle.\"__pgt_setop_state_7\""
        );
        assert_eq!(staging_relation_basename(7), "__pgt_setop_src_7");
    }

    #[test]
    fn test_state_names_are_unique_per_stream() {
        assert_ne!(state_relation_basename(1), state_relation_basename(2));
        assert_ne!(qualified_state_relation(1), qualified_state_relation(2));
    }

    #[test]
    fn test_schema_version_is_positive() {
        let version: i16 = SETOP_STATE_SCHEMA_VERSION;
        assert!(version > 0);
    }
}
