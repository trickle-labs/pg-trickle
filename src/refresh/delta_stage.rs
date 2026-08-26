//! Privileged staging of bounded CDC windows for owner-executed delta SQL.

use crate::catalog::{StDependency, StreamTableMeta};
use crate::error::PgTrickleError;
use crate::version::Frontier;
use pgrx::prelude::*;
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct DeltaStage {
    names: HashMap<u32, String>,
}

impl DeltaStage {
    pub fn source_tables(&self) -> &HashMap<u32, String> {
        &self.names
    }

    pub fn table_name(pgt_id: i64, source_oid: u32) -> String {
        format!("__pgt_cdc_{pgt_id}_{source_oid}")
    }

    /// Copy every source's immutable refresh window before changing identity.
    pub fn prepare(
        st: &StreamTableMeta,
        dependencies: &[StDependency],
        prev_frontier: &Frontier,
        new_frontier: &Frontier,
    ) -> Result<Self, PgTrickleError> {
        let owner = crate::refresh::stream_owner_name(st)?;
        let owner = quote_ident(&owner);
        let change_schema = crate::config::pg_trickle_change_buffer_schema();
        let bypass_tables = crate::refresh::get_st_bypass_tables();
        let mut stage = Self::default();

        for dependency in dependencies {
            let source_oid = dependency.source_relid.to_u32();
            let (buffer, prev_lsn, new_lsn) = if dependency.source_type == "STREAM_TABLE" {
                let upstream_pgt_id = StreamTableMeta::pgt_id_for_relid(dependency.source_relid)
                    .ok_or_else(|| PgTrickleError::CdcStateInvalid {
                        pgt_id: st.pgt_id,
                        source_name: format!("OID {source_oid}"),
                        buffer: "stream-table dependency".to_string(),
                        reason: "upstream stream table metadata is missing".to_string(),
                    })?;
                let key = format!("pgt_{upstream_pgt_id}");
                (
                    bypass_tables
                        .get(&upstream_pgt_id)
                        .cloned()
                        .unwrap_or_else(|| {
                            format!("{change_schema}.changes_pgt_{upstream_pgt_id}")
                        }),
                    frontier_lsn(prev_frontier, &key),
                    frontier_lsn(new_frontier, &key),
                )
            } else if matches!(
                dependency.source_type.as_str(),
                "TABLE" | "FOREIGN_TABLE" | "MATVIEW"
            ) {
                (
                    format!(
                        "{change_schema}.{}",
                        crate::cdc::buffer_base_name_for_oid(dependency.source_relid)
                    ),
                    prev_frontier.get_lsn(source_oid),
                    new_frontier.get_lsn(source_oid),
                )
            } else {
                continue;
            };

            stage.stage_source(st, dependency, &buffer, &prev_lsn, &new_lsn, &owner)?;
        }

        Ok(stage)
    }

    fn stage_source(
        &mut self,
        st: &StreamTableMeta,
        dependency: &StDependency,
        source_buffer: &str,
        prev_lsn: &str,
        new_lsn: &str,
        owner: &str,
    ) -> Result<(), PgTrickleError> {
        let pgt_id = st.pgt_id;
        let source_oid = dependency.source_relid.to_u32();
        let name = Self::table_name(pgt_id, source_oid);
        let qualified_name = format!("pg_temp.{}", quote_ident(&name));
        let source = quote_qualified(source_buffer)?;
        Spi::run(&format!("DROP TABLE IF EXISTS {qualified_name}")) // nosemgrep: rust.spi.run.dynamic-format — qualified_name is built from quote_ident.
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run_with_args(
            &format!(
                "CREATE TEMP TABLE {qualified_name} ON COMMIT DROP AS \
                 SELECT * FROM {source} WHERE lsn > $1::pg_lsn AND lsn <= $2::pg_lsn"
            ),
            &[prev_lsn.into(), new_lsn.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let rls_enabled = Spi::get_one_with_args::<bool>(
            "SELECT relrowsecurity FROM pg_catalog.pg_class WHERE oid = $1",
            &[dependency.source_relid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);
        let privileges = if rls_enabled {
            "SELECT, DELETE"
        } else {
            "SELECT"
        };
        Spi::run(&format!(
            "GRANT {privileges} ON TABLE {qualified_name} TO {owner}"
        ))
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let (source_table, hash_columns) =
            crate::refresh::source_visibility_key(dependency.source_relid)?;
        let visible_hash = crate::refresh::codegen::build_content_hash_expr("s.", &hash_columns);
        let filter_sql = format!(
            "DELETE FROM {qualified_name} c \
             WHERE c.action <> 'D' AND NOT EXISTS (\
               SELECT 1 FROM {source_table} s WHERE {visible_hash} = c.pk_hash\
             )"
        );
        if rls_enabled {
            // Reduce each key to its net change before checking the current
            // owner-visible source row. Without this, an insert followed by a
            // delete in one refresh window loses only its insert image and
            // leaves an unmatched delete. This is the same first/last
            // compaction used by persistent CDC buffers.
            Spi::run(&format!(
                "DELETE FROM {qualified_name} WHERE change_id IN (\
                   SELECT change_id FROM (\
                     SELECT change_id, \
                            ROW_NUMBER() OVER (PARTITION BY pk_hash ORDER BY change_id) AS rn_asc, \
                            ROW_NUMBER() OVER (PARTITION BY pk_hash ORDER BY change_id DESC) AS rn_desc, \
                            FIRST_VALUE(action) OVER (\
                              PARTITION BY pk_hash ORDER BY change_id\
                            ) AS first_act, \
                            LAST_VALUE(action) OVER (\
                              PARTITION BY pk_hash ORDER BY change_id \
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING\
                            ) AS last_act \
                     FROM {qualified_name} WHERE action IN ('I', 'D')\
                   ) __pgt_ranked \
                   WHERE (first_act = 'I' AND last_act = 'D') \
                      OR (rn_asc > 1 AND rn_desc > 1)\
                 )"
            ))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }

        let access_sql = format!("SELECT 1 FROM {source_table} LIMIT 0");
        crate::refresh::with_stream_owner(st, || {
            Spi::run(&access_sql) // nosemgrep: rust.spi.run.dynamic-format — source_table is built by pg_catalog.format('%I.%I').
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            if rls_enabled {
                Spi::run(&filter_sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            }
            Ok(())
        })?;
        if rls_enabled {
            Spi::run(&format!(
                "REVOKE DELETE ON TABLE {qualified_name} FROM {owner}"
            ))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
        self.names.insert(source_oid, qualified_name);
        Ok(())
    }
}

fn frontier_lsn(frontier: &Frontier, key: &str) -> String {
    frontier
        .sources
        .get(key)
        .map(|source| source.lsn.clone())
        .unwrap_or_else(|| "0/0".to_string())
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn quote_qualified(value: &str) -> Result<String, PgTrickleError> {
    let mut parts = value.split('.');
    let schema = parts
        .next()
        .ok_or_else(|| PgTrickleError::InvalidArgument("empty CDC buffer name".into()))?;
    let table = parts.next().ok_or_else(|| {
        PgTrickleError::InvalidArgument("CDC buffer must be schema-qualified".into())
    })?;
    if parts.next().is_some() || schema.is_empty() || table.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "invalid CDC buffer name".into(),
        ));
    }
    Ok(format!("{}.{}", quote_ident(schema), quote_ident(table)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_stream_and_oid_scoped() {
        assert_eq!(DeltaStage::table_name(7, 42), "__pgt_cdc_7_42");
    }
}
