//! Privileged staging of bounded CDC windows for owner-executed delta SQL.

use crate::catalog::{StDependency, StreamTableMeta};
use crate::error::PgTrickleError;
use crate::version::Frontier;
use pgrx::prelude::*;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default)]
pub struct DeltaStage {
    names: HashMap<u32, String>,
    /// CREATE TEMP VIEW has no ON COMMIT DROP in PostgreSQL, so explicitly
    /// remove these source-dependent views after the refresh has used them.
    /// The actual materialized bounded sources are ON COMMIT DROP tables.
    bounded_views: Vec<String>,
    centroid_nonce: Option<u64>,
}

impl DeltaStage {
    pub fn source_tables(&self) -> &HashMap<u32, String> {
        &self.names
    }

    pub fn table_name(pgt_id: i64, source_oid: u32) -> String {
        format!("__pgt_cdc_{pgt_id}_{source_oid}")
    }

    /// A source relation as of its NEW frontier, for every live read in DVM.
    /// The name is stable so cached delta templates can refer to it.
    pub fn bounded_table_name(pgt_id: i64, source_oid: u32) -> String {
        format!("__pgt_bound_{pgt_id}_{source_oid}")
    }

    fn bounded_name(&self, pgt_id: i64, source_oid: u32) -> String {
        match self.centroid_nonce {
            Some(nonce) => format!("__pgt_bound_{pgt_id}_{source_oid}_{nonce}"),
            None => Self::bounded_table_name(pgt_id, source_oid),
        }
    }

    /// Copy every source's immutable refresh window before changing identity.
    pub fn prepare(
        st: &StreamTableMeta,
        dependencies: &[StDependency],
        prev_frontier: &Frontier,
        new_frontier: &Frontier,
    ) -> Result<Self, PgTrickleError> {
        Self::prepare_inner(st, dependencies, prev_frontier, new_frontier, None)
    }

    pub(crate) fn prepare_centroid(
        st: &StreamTableMeta,
        dependencies: &[StDependency],
        prev_frontier: &Frontier,
        new_frontier: &Frontier,
        nonce: u64,
    ) -> Result<Self, PgTrickleError> {
        Self::prepare_inner(st, dependencies, prev_frontier, new_frontier, Some(nonce))
    }

    fn prepare_inner(
        st: &StreamTableMeta,
        dependencies: &[StDependency],
        prev_frontier: &Frontier,
        new_frontier: &Frontier,
        centroid_nonce: Option<u64>,
    ) -> Result<Self, PgTrickleError> {
        let owner = crate::refresh::stream_owner_name(st)?;
        let owner = quote_ident(&owner);
        let change_schema = crate::config::pg_trickle_change_buffer_schema();
        let bypass_tables = crate::refresh::get_st_bypass_tables();
        let mut stage = Self {
            names: HashMap::new(),
            bounded_views: Vec::new(),
            centroid_nonce,
        };

        // Never queue a blocking SHARE lock behind continuously active
        // writers. For an uncontended source, a conditional SHARE lock permits
        // the cheap no-future VIEW fast path. Under contention, reconstruct
        // the bounded source by copying live rows and future CDC in ONE SQL
        // statement/MVCC snapshot without waiting for source writers.
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

            let source_locked = if matches!(
                dependency.source_type.as_str(),
                "TABLE" | "MATVIEW" | "STREAM_TABLE"
            ) {
                try_share_lock_source(dependency.source_relid)?
            } else {
                false
            };
            stage.stage_source(
                st,
                dependency,
                &buffer,
                &prev_lsn,
                &new_lsn,
                &owner,
                source_locked,
            )?;
        }

        Ok(stage)
    }

    #[allow(clippy::too_many_arguments)] // Source stage needs both LSN cuts, owner, and conditional-lock state.
    fn stage_source(
        &mut self,
        st: &StreamTableMeta,
        dependency: &StDependency,
        source_buffer: &str,
        prev_lsn: &str,
        new_lsn: &str,
        owner: &str,
        source_locked: bool,
    ) -> Result<(), PgTrickleError> {
        let pgt_id = st.pgt_id;
        let source_oid = dependency.source_relid.to_u32();
        let name = match self.centroid_nonce {
            Some(nonce) => format!("__pgt_cd_{pgt_id}_{source_oid}_{nonce}"),
            None => Self::table_name(pgt_id, source_oid),
        };
        let qualified_name = format!("pg_temp.{}", quote_ident(&name));
        let source = quote_qualified(source_buffer)?;
        if self.centroid_nonce.is_some() {
            let existing = Spi::get_one_with_args::<pg_sys::Oid>(
                "SELECT pg_catalog.to_regclass($1)::oid",
                &[qualified_name.as_str().into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            if existing.is_some() {
                return Err(PgTrickleError::InternalError(format!(
                    "centroid CDC staging relation {qualified_name} already exists"
                )));
            }
        } else {
            Spi::run(&format!("DROP TABLE IF EXISTS {qualified_name}")) // nosemgrep: rust.spi.run.dynamic-format — qualified_name is built from quote_ident.
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
        // nosemgrep: rust.spi.run_with_args.dynamic-format — qualified_name and source are quoted identifiers; LSNs are parameters.
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
        // nosemgrep: rust.spi.run.dynamic-format — privileges is fixed; qualified_name and owner are quoted identifiers.
        Spi::run(&format!(
            "GRANT {privileges} ON TABLE {qualified_name} TO {owner}"
        ))
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        let (source_table, hash_columns) =
            crate::refresh::source_visibility_key(dependency.source_relid)?;
        let identity_domain = if crate::cdc::resolve_pk_columns(dependency.source_relid)?.is_empty()
        {
            "KEYLESS_ROW"
        } else {
            "SCAN_KEY"
        };
        let visible_hash = crate::refresh::codegen::build_content_hash_expr_for_domain(
            "s.",
            &hash_columns,
            identity_domain,
        );
        let filter_sql = format!(
            "DELETE FROM {qualified_name} c \
             WHERE c.action <> 'D' AND NOT EXISTS (\
               SELECT 1 FROM {source_table} s \
               WHERE pgtrickle.row_probe_v1({visible_hash}) = pgtrickle.row_probe_v1(c.__pgt_row_id) \
                 AND {visible_hash} = c.__pgt_row_id\
             )"
        );
        if rls_enabled {
            // Reduce each key to its net change before checking the current
            // owner-visible source row. Without this, an insert followed by a
            // delete in one refresh window loses only its insert image and
            // leaves an unmatched delete. This is the same first/last
            // compaction used by persistent CDC buffers.
            // nosemgrep: rust.spi.run.dynamic-format — qualified_name is a quoted internal identifier.
            Spi::run(&format!(
                "DELETE FROM {qualified_name} WHERE change_id IN (\
                   SELECT change_id FROM (\
                     SELECT change_id, \
                            ROW_NUMBER() OVER (PARTITION BY __pgt_row_id ORDER BY change_id) AS rn_asc, \
                            ROW_NUMBER() OVER (PARTITION BY __pgt_row_id ORDER BY change_id DESC) AS rn_desc, \
                            FIRST_VALUE(action) OVER (\
                              PARTITION BY __pgt_row_id ORDER BY change_id\
                            ) AS first_act, \
                            LAST_VALUE(action) OVER (\
                              PARTITION BY __pgt_row_id ORDER BY change_id \
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
            // nosemgrep: rust.spi.run.dynamic-format — qualified_name and owner are quoted identifiers.
            Spi::run(&format!(
                "REVOKE DELETE ON TABLE {qualified_name} FROM {owner}"
            ))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
        // Stage the logical source at new_lsn as well as the bounded delta.
        // The contended path reads the live relation and future CDC in ONE
        // statement, so both sides share exactly one MVCC snapshot.
        self.stage_bounded_source(
            st,
            dependency,
            &source,
            new_lsn,
            owner,
            &source_table,
            &hash_columns,
            source_locked,
        )?;
        self.names.insert(source_oid, qualified_name);
        Ok(())
    }

    /// The vector-centroid source-ahead fallback deliberately recomputes the
    /// output from the *current* source, without binding its private state.
    /// An LSN window that splits one UPDATE's D/I images cannot describe an
    /// as-of relation: reversing only its post-bound D image may resurrect an
    /// obsolete row. This special fallback must retain its existing current-
    /// state rescan contract; other DIFFERENTIAL operations stay LSN-bounded.
    ///
    /// Caller holds SHARE source locks from prepare_inner, so the live
    /// relations remain stable through the subsequent MERGE.
    pub(crate) fn use_live_sources_for_centroid_rescan(
        &mut self,
        st: &StreamTableMeta,
        dependencies: &[StDependency],
    ) -> Result<(), PgTrickleError> {
        let owner = quote_ident(&crate::refresh::stream_owner_name(st)?);
        for dependency in dependencies {
            let oid = dependency.source_relid.to_u32();
            if !self.names.contains_key(&oid) {
                continue;
            }
            let bound = format!(
                "pg_temp.{}",
                quote_ident(&self.bounded_name(st.pgt_id, oid))
            );
            let kind = Spi::get_one_with_args::<String>(
                "SELECT (SELECT relkind::text FROM pg_catalog.pg_class \
                 WHERE oid = pg_catalog.to_regclass($1))",
                &[bound.as_str().into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            match kind.as_deref() {
                Some("v") => {
                    // nosemgrep: rust.spi.run.dynamic-format -- numeric-ID-scoped quoted temp view.
                    Spi::run(&format!("DROP VIEW {bound}"))
                }
                Some("r") => {
                    // nosemgrep: rust.spi.run.dynamic-format -- numeric-ID-scoped quoted temp table.
                    Spi::run(&format!("DROP TABLE {bound}"))
                }
                _ => {
                    return Err(PgTrickleError::InternalError(format!(
                        "missing bounded source stage for OID {oid}"
                    )));
                }
            }
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let (source, _) = crate::refresh::source_visibility_key(dependency.source_relid)?;
            // nosemgrep: rust.spi.run.dynamic-format -- source is catalog-qualified, bound is quoted.
            Spi::run(&format!(
                "CREATE TEMP VIEW {bound} AS SELECT * FROM {source}"
            ))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            // nosemgrep: rust.spi.run.dynamic-format -- bound and owner are quoted identifiers.
            Spi::run(&format!("GRANT SELECT ON TABLE {bound} TO {owner}"))
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            if !self.bounded_views.contains(&bound) {
                self.bounded_views.push(bound);
            }
        }
        Ok(())
    }

    /// Drop source-dependent session-local views after the last DVM read.
    /// Unlike CREATE TEMP TABLE ... ON COMMIT DROP, PostgreSQL temporary
    /// views survive commit and can prevent DROP TABLE on the base source.
    /// Explicit cleanup preserves the zero-copy fast path without leaking
    /// dependencies into later transactions or tests.
    pub(crate) fn cleanup_bounded_views(&self) -> Result<(), PgTrickleError> {
        for view in &self.bounded_views {
            // nosemgrep: rust.spi.run.dynamic-format -- internally-generated, quoted temp view.
            Spi::run(&format!("DROP VIEW {view}")) // nosemgrep: rust.spi.run.dynamic-format
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        }
        Ok(())
    }

    /// Build a source snapshot at the NEW logical frontier. If no post-bound
    /// changes are visible, a temporary view avoids copying a large relation.
    /// When a conditional SHARE lock succeeds, the view is stable until
    /// MERGE ends. Under contention we materialize the complete relation
    /// against a single MVCC snapshot, without blocking source writers.
    ///
    /// With post-bound changes, materialize a snapshot that reverses them.
    /// For keyed tables the earliest event per identity gives the old row;
    /// for keyless tables signed multiplicities preserve duplicate rows.
    #[allow(clippy::too_many_arguments)]
    fn stage_bounded_source(
        &mut self,
        st: &StreamTableMeta,
        dependency: &StDependency,
        buffer: &str,
        new_lsn: &str,
        owner: &str,
        source_table: &str,
        hash_columns: &[String],
        source_locked: bool,
    ) -> Result<(), PgTrickleError> {
        let source_oid = dependency.source_relid.to_u32();
        let name = self.bounded_name(st.pgt_id, source_oid);
        let target = format!("pg_temp.{}", quote_ident(&name));
        // The bound originates from frontier metadata. Validate it before
        // interpolating into a nested CREATE TABLE AS SELECT statement.
        crate::lsn::Lsn::parse(new_lsn).map_err(|error| {
            PgTrickleError::InvalidArgument(format!("invalid bounded source frontier LSN: {error}"))
        })?;

        // A previous no-gap refresh in a pooled backend may have left a
        // session-local VIEW. A table is ON COMMIT DROP instead.
        let old_kind = Spi::get_one_with_args::<String>(
            "SELECT (SELECT c.relkind::text FROM pg_catalog.pg_class c \
                     WHERE c.oid = pg_catalog.to_regclass($1))",
            &[target.as_str().into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        match old_kind.as_deref() {
            Some("v") => {
                // nosemgrep: rust.spi.run.dynamic-format -- target is a
                // quoted numeric-ID-scoped pg_temp relation controlled here.
                Spi::run(&format!("DROP VIEW {target}")) // nosemgrep: rust.spi.run.dynamic-format
            }
            Some(_) => {
                // nosemgrep: rust.spi.run.dynamic-format -- target is a
                // quoted numeric-ID-scoped pg_temp relation controlled here.
                Spi::run(&format!("DROP TABLE {target}")) // nosemgrep: rust.spi.run.dynamic-format
            }
            None => Ok(()),
        }
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;

        // When the nonblocking SHARE lock succeeds, the source cannot change
        // during this refresh. The window CTAS above has already switched SPI
        // to writable execution, so the probe below uses a fresh READ
        // COMMITTED snapshot. We do not need to copy the entire post-frontier
        // CDC buffer into another temporary table just to decide whether a
        // stable source view is safe. One query probes both event categories.
        //
        // When the source is contended, it is NOT safe to probe now and
        // choose a view: another source transaction might commit before the
        // next SPI statement. Always materialize the inverse in a single
        // snapshot of source and post-frontier CDC in that case.
        let has_future = if source_locked {
            // buffer is a catalog-derived, quoted qualified identifier;
            // the frontier is a bound $1::pg_lsn parameter, not SQL text.
            let probe_sql = format!(
                "SELECT \\
                   EXISTS (SELECT 1 FROM {buffer} WHERE lsn > $1::pg_lsn AND action = 'T'), \\
                   EXISTS (SELECT 1 FROM {buffer} WHERE lsn > $1::pg_lsn AND action IN ('I', 'D'))"
            );
            let (future_truncate, future_changes) =
                Spi::get_two_with_args::<bool, bool>(&probe_sql, &[new_lsn.into()])
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            if future_truncate != Some(false) {
                return Err(PgTrickleError::UnsupportedOperator(format!(
                    "post-bound TRUNCATE or unknown CDC status for source OID {source_oid} requires FULL refresh"
                )));
            }
            future_changes.ok_or_else(|| {
                PgTrickleError::InternalError(format!(
                    "post-bound CDC probe was NULL for source OID {source_oid}"
                ))
            })?
        } else {
            true
        };

        if !has_future {
            // The source lock makes the live source identical to its bounded
            // state until the transaction ends. No full-table copy needed.
            // nosemgrep: rust.spi.run.dynamic-format -- both names are
            // catalog-derived, explicitly quoted identifiers.
            // nosemgrep: rust.spi.run.dynamic-format -- source and target identifiers quoted.
            Spi::run(&format!(
                "CREATE TEMP VIEW {target} AS SELECT * FROM {source_table}"
            ))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            self.bounded_views.push(target.clone());
        } else {
            let pk_columns = crate::cdc::resolve_pk_columns(dependency.source_relid)?;
            let is_st = dependency.source_type == "STREAM_TABLE";
            let keyless = !is_st && pk_columns.is_empty();

            // Use the real column order, but only columns captured in CDC.
            // The change buffer can track a superset of this ST's columns;
            // absent source columns are never referenced by the DVM plan.
            let columns = Spi::connect(|client| {
                let rows = client
                    .select(
                        "SELECT attname::text FROM pg_catalog.pg_attribute \
                         WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped \
                         ORDER BY attnum",
                        None,
                        &[dependency.source_relid.into()],
                    )
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                rows.into_iter()
                    .map(|row| {
                        row.get::<String>(1)
                            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                            .ok_or_else(|| {
                                PgTrickleError::InternalError(
                                    "source column name unexpectedly NULL".into(),
                                )
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()
            })?;
            let captured: HashSet<String> = Spi::connect(|client| {
                let rows = client
                    .select(
                        "SELECT a.attname::text FROM pg_catalog.pg_attribute a \
                         WHERE a.attrelid = pg_catalog.to_regclass($1) \
                           AND a.attnum > 0 AND NOT a.attisdropped",
                        None,
                        &[buffer.into()],
                    )
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
                rows.into_iter()
                    .map(|row| {
                        row.get::<String>(1)
                            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                            .ok_or_else(|| {
                                PgTrickleError::InternalError(
                                    "buffer column name unexpectedly NULL".into(),
                                )
                            })
                    })
                    .collect::<Result<HashSet<_>, _>>()
            })?;
            let mut cols = columns
                .into_iter()
                .filter(|col| captured.contains(&crate::cdc::cb_col_name(col)))
                .collect::<Vec<_>>();
            if is_st && !cols.iter().any(|col| col == "__pgt_row_id") {
                cols.push("__pgt_row_id".to_string());
            }
            if cols.is_empty() {
                return Err(PgTrickleError::UnsupportedOperator(format!(
                    "cannot reconstruct source OID {source_oid} without captured columns"
                )));
            }
            let live_cols = cols
                .iter()
                .map(|c| format!("s.{}", quote_ident(c)))
                .collect::<Vec<_>>()
                .join(", ");
            let first_cols = cols
                .iter()
                .map(|c| format!("f.{}", quote_ident(c)))
                .collect::<Vec<_>>()
                .join(", ");
            let future_cols = cols
                .iter()
                .filter(|c| *c != "__pgt_row_id")
                .map(|c| {
                    format!(
                        "c.{} AS {}",
                        quote_ident(&crate::cdc::cb_col_name(c)),
                        quote_ident(c)
                    )
                })
                .collect::<Vec<_>>();
            let future_cols = if future_cols.is_empty() {
                String::new()
            } else {
                format!(", {}", future_cols.join(", "))
            };
            let projected = cols
                .iter()
                .filter(|c| *c != "__pgt_row_id")
                .map(|c| format!("f.{}", quote_ident(c)))
                .collect::<Vec<_>>();
            let selected = if projected.is_empty() {
                String::new()
            } else {
                format!(", {}", projected.join(", "))
            };
            let first = format!(
                "SELECT DISTINCT ON (f.__pgt_event_identity) f.__pgt_event_identity AS __pgt_row_id, f.__pgt_event_action AS __pgt_future_action{selected} FROM __pgt_future f \
                 ORDER BY f.__pgt_event_identity, f.__pgt_event_sequence"
            );
            // The source lock protects direct CDC reads; without the lock,
            // this CDC scan and live-source scan occur in the same CTAS
            // statement and therefore share one PostgreSQL MVCC snapshot.
            let common = format!(
                "WITH __pgt_future AS MATERIALIZED (\
                   SELECT c.__pgt_row_id AS __pgt_event_identity, c.action AS __pgt_event_action, c.change_id AS __pgt_event_sequence{future_cols} \
                   FROM {buffer} c \
                   WHERE c.lsn > '{new_lsn}'::pg_lsn AND c.action IN ('I', 'D')\
                 ), __pgt_first AS ({first})"
            );
            let select = if keyless {
                let identity = crate::refresh::codegen::build_content_hash_expr_for_domain(
                    "s.",
                    hash_columns,
                    "KEYLESS_ROW",
                );
                format!(
                    "{common}, __pgt_net AS (\
                       SELECT __pgt_event_identity AS __pgt_row_id, \
                         SUM(CASE WHEN __pgt_event_action = 'I' THEN 1 ELSE -1 END)::bigint AS net \
                       FROM __pgt_future GROUP BY __pgt_event_identity\
                     ), __pgt_live AS (\
                       SELECT {live_cols}, {identity} AS __pgt_bound_identity, \
                         ROW_NUMBER() OVER (PARTITION BY {identity}) AS __pgt_bound_rn \
                       FROM {source_table} s\
                     ) \
                     SELECT {cols} FROM __pgt_live l \
                     LEFT JOIN __pgt_net n ON n.__pgt_row_id = l.__pgt_bound_identity \
                     WHERE l.__pgt_bound_rn > GREATEST(n.net, 0) \
                     UNION ALL SELECT {first_cols} FROM __pgt_first f \
                     JOIN __pgt_net n ON n.__pgt_row_id = f.__pgt_row_id \
                     CROSS JOIN LATERAL generate_series(1::bigint, GREATEST(-n.net, 0)) g",
                    cols = cols
                        .iter()
                        .map(|c| format!("l.{}", quote_ident(c)))
                        .collect::<Vec<_>>()
                        .join(", "),
                )
            } else {
                let identity = if is_st {
                    "s.__pgt_row_id".to_string()
                } else {
                    crate::refresh::codegen::build_content_hash_expr_for_domain(
                        "s.",
                        hash_columns,
                        "SCAN_KEY",
                    )
                };
                format!(
                    "{common} \
                     SELECT {live_cols} FROM {source_table} s \
                     WHERE NOT EXISTS (SELECT 1 FROM __pgt_first f \
                       WHERE f.__pgt_row_id = {identity}) \
                     UNION ALL SELECT {first_cols} FROM __pgt_first f \
                     WHERE f.__pgt_future_action = 'D'"
                )
            };

            // PostgreSQL's statement snapshot is shared by the live source
            // and CDC scans in the same CTAS, even while writers commit.
            // A future TRUNCATE has no D-images. For a contended source
            // materialize a sentinel row *in the same snapshot* so it can
            // be detected reliably; a separate preflight query could miss it.
            let guard = quote_ident(&format!(
                "__pgt_future_truncate_guard_{}_{}",
                st.pgt_id, source_oid
            ));
            let select = if source_locked {
                select
            } else {
                let null_cols = cols
                    .iter()
                    .map(|col| format!("NULL AS {}", quote_ident(col)))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(
                    "SELECT b.*, FALSE AS {guard} FROM ({select}) b \
                     UNION ALL SELECT {null_cols}, TRUE AS {guard} \
                     WHERE EXISTS (SELECT 1 FROM {buffer} t \
                       WHERE t.lsn > '{new_lsn}'::pg_lsn AND t.action = 'T')"
                )
            };
            // The SQL contains only catalog-derived qualified relations,
            // quote_ident-projected columns, a validated pg_lsn literal, and
            // the extension-generated inverse query. DDL identifiers cannot
            // be bound as SPI parameters. Keep the reviewed SQL in a named
            // value for diagnostics and a single execution.
            let create_bound_sql =
                format!("CREATE TEMP TABLE {target} ON COMMIT DROP AS {select}");
            Spi::run(&create_bound_sql)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            if !source_locked {
                // Only rows from the CTAS snapshot affect this guard.
                // nosemgrep: rust.spi.query.dynamic-format
                let future_truncate = Spi::get_one::<bool>(&format!(
                    "SELECT EXISTS (SELECT 1 FROM {target} WHERE {guard})"
                )) // nosemgrep: rust.spi.query.dynamic-format
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(false);
                if future_truncate {
                    return Err(PgTrickleError::UnsupportedOperator(format!(
                        "post-bound TRUNCATE for source OID {source_oid} requires FULL refresh"
                    )));
                }
                // Remove the internal marker before the DVM uses this table.
                // nosemgrep: rust.spi.run.dynamic-format
                Spi::run(&format!("ALTER TABLE {target} DROP COLUMN {guard}")) // nosemgrep: rust.spi.run.dynamic-format
                    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            }
        }
        // nosemgrep: rust.spi.run.dynamic-format -- temp relation is
        // numeric-ID-scoped and the owner is identifier-quoted.
        Spi::run(&format!("GRANT SELECT ON TABLE {target} TO {owner}")) // nosemgrep: rust.spi.run.dynamic-format
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Ok(())
    }
}

// Standalone Rust unit-test executables do not load PostgreSQL's dynamic
// backend symbols. Keep the real conditional lock in extension builds; E2E
// tests against a PostgreSQL server exercise the actual locking branch.
#[cfg(test)]
fn conditional_source_share_lock(_oid: pg_sys::Oid) -> bool {
    false
}

#[cfg(not(test))]
fn conditional_source_share_lock(oid: pg_sys::Oid) -> bool {
    // SAFETY: The relation OID was verified in pg_class by this backend and
    // this API is called on the PostgreSQL backend thread. It fails without
    // waiting when another transaction holds a conflicting lock.
    unsafe { pg_sys::ConditionalLockRelationOid(oid, pg_sys::ShareLock as i32) }
}

/// Try to fence a source and all partition descendants without ever waiting
/// behind a continuously active writer. A failed attempt is not an error:
/// callers must instead materialize an as-of source from a single MVCC
/// snapshot of the live relation and its post-frontier CDC.
fn try_share_lock_source(source_oid: pg_sys::Oid) -> Result<bool, PgTrickleError> {
    let relations = Spi::connect(|client| {
        let rows = client
            .select(
                "WITH RECURSIVE rels(oid) AS (
                    SELECT $1::oid
                    UNION ALL
                    SELECT i.inhrelid FROM pg_catalog.pg_inherits i
                    JOIN rels r ON r.oid = i.inhparent
                 )
                 SELECT r.oid::text, c.relkind::text FROM rels r
                 JOIN pg_catalog.pg_class c ON c.oid = r.oid
                 ORDER BY r.oid",
                None,
                &[source_oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut oids = Vec::new();
        for row in rows {
            let oid = row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("source OID was NULL".into()))?
                .parse::<u32>()
                .map_err(|e| PgTrickleError::SpiError(format!("invalid source OID: {e}")))?;
            let relkind = row
                .get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("source relkind was NULL".into()))?;
            if relkind != "f" {
                oids.push(pg_sys::Oid::from(oid));
            }
        }
        Ok::<_, PgTrickleError>(oids)
    })?;
    if relations.is_empty() {
        return Ok(false);
    }
    for oid in relations {
        if !conditional_source_share_lock(oid) {
            return Ok(false);
        }
    }
    Ok(true)
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
