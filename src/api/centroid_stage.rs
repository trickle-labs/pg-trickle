//! Transaction-local staging for centroid initialization.

use std::sync::atomic::{AtomicU64, Ordering};

use pgrx::prelude::*;

use crate::catalog::StreamTableMeta;
use crate::error::PgTrickleError;

static NEXT_STAGE: AtomicU64 = AtomicU64::new(1);

/// Materialize the visible centroid and private state from one owner snapshot.
pub(crate) fn stage_centroid_initialization(
    st: &StreamTableMeta,
) -> Result<String, PgTrickleError> {
    crate::refresh::with_stream_owner(st, || {
        let sql = crate::dvm::centroid_numerics::centroid_staging_sql(&st.defining_query)?
            .ok_or_else(|| {
                PgTrickleError::UnsupportedOperator(format!(
                    "centroid initialization is not admitted for {}.{}",
                    st.pgt_schema, st.pgt_name
                ))
            })?;
        for _ in 0..16 {
            let suffix = NEXT_STAGE.fetch_add(1, Ordering::Relaxed);
            let basename = format!("__pgt_ci_{}_{}", st.pgt_id, suffix);
            let table = format!("pg_temp.{}", crate::sql_builder::ident(&basename));
            let existing = Spi::get_one_with_args::<pg_sys::Oid>(
                "SELECT pg_catalog.to_regclass($1)::oid",
                &[table.as_str().into()],
            )
            .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
            if existing.is_some() {
                continue;
            }
            // nosemgrep: rust.spi.run.dynamic-format -- the temp name is internally generated and the query is derived from the validated definition.
            Spi::run(&format!(
                "CREATE TEMP TABLE {table} ON COMMIT DROP AS {sql}"
            ))
            .map_err(|error| {
                PgTrickleError::SpiError(format!("centroid initialization staging: {error}"))
            })?;
            return Ok(table);
        }
        Err(PgTrickleError::InternalError(format!(
            "could not allocate a centroid staging relation for {}.{}",
            st.pgt_schema, st.pgt_name
        )))
    })
}

/// Hold the catalog and storage identity stable for centroid staging.
fn lock_centroid_stream(
    st: &StreamTableMeta,
    require_owner: bool,
) -> Result<StreamTableMeta, PgTrickleError> {
    let locked = Spi::connect_mut(|client| {
        let rows = client.update(
            "SELECT pgt_relid = $2 AND pgt_schema = $3 AND pgt_name = $4 \
             AND defining_query = $5 AND contract_generation = $6 \
             FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1 FOR UPDATE",
            None,
            &[
                st.pgt_id.into(),
                st.pgt_relid.into(),
                st.pgt_schema.as_str().into(),
                st.pgt_name.as_str().into(),
                st.defining_query.as_str().into(),
                st.contract_generation.into(),
            ],
        )?;
        match rows.into_iter().next() {
            Some(row) => row.get::<bool>(1),
            None => Ok(None),
        }
    })
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    let Some(locked) = locked else {
        return Err(PgTrickleError::NotFound(format!(
            "stream table metadata for pgt_id={}",
            st.pgt_id
        )));
    };
    if !locked {
        return Err(PgTrickleError::InternalError(format!(
            "stream table contract changed while preparing centroid state for {}.{}",
            st.pgt_schema, st.pgt_name
        )));
    }
    let current = StreamTableMeta::get_by_id(st.pgt_id)?.ok_or_else(|| {
        PgTrickleError::NotFound(format!("stream table metadata for pgt_id={}", st.pgt_id))
    })?;
    if current.pgt_relid != st.pgt_relid
        || current.pgt_schema != st.pgt_schema
        || current.pgt_name != st.pgt_name
        || current.defining_query != st.defining_query
        || current.contract_generation != st.contract_generation
    {
        return Err(PgTrickleError::InternalError(format!(
            "stream table contract changed while preparing centroid state for {}.{}",
            st.pgt_schema, st.pgt_name
        )));
    }
    let st = current;
    let target = format!(
        "{}.{}",
        crate::sql_builder::ident(&st.pgt_schema),
        crate::sql_builder::ident(&st.pgt_name)
    );
    // nosemgrep: rust.spi.run.dynamic-format -- both relation identifiers come from the stream catalog and are quoted.
    Spi::run(&format!("LOCK TABLE {target} IN ACCESS SHARE MODE"))
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    let locked_relid = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass($1)::oid",
        &[target.as_str().into()],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    if locked_relid != Some(st.pgt_relid) {
        return Err(PgTrickleError::InternalError(format!(
            "stream table identity changed while preparing centroid state for {target}"
        )));
    }
    if require_owner {
        super::helpers::check_stream_table_ownership(st.pgt_relid, &st.pgt_schema, &st.pgt_name)?;
    }
    Ok(st)
}

/// Store incomplete state and return the snapshot for the visible refresh.
fn prepare_centroid_state(
    st: &StreamTableMeta,
    require_owner: bool,
) -> Result<String, PgTrickleError> {
    let st = lock_centroid_stream(st, require_owner)?;
    let staged = stage_centroid_initialization(&st)?;
    store_centroid_state(&st, &staged)?;
    Ok(staged)
}

fn store_centroid_state(st: &StreamTableMeta, staged: &str) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_centroid_states \
         (pgt_id, stream_relid, defining_query, contract_generation, schema_version, status) \
         VALUES ($1, $2, $3, $4, 1, 'BUILDING') \
         ON CONFLICT (pgt_id) DO UPDATE SET stream_relid = EXCLUDED.stream_relid, \
         defining_query = EXCLUDED.defining_query, contract_generation = EXCLUDED.contract_generation, \
         schema_version = 1, materialized_xid = NULL, baseline_frontier = NULL, \
         last_change_limit_rebase = false, status = 'BUILDING'",
        &[
            st.pgt_id.into(),
            st.pgt_relid.into(),
            st.defining_query.as_str().into(),
            st.contract_generation.into(),
        ],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    Spi::run_with_args(
        "DELETE FROM pgtrickle.pgt_centroid_groups WHERE pgt_id = $1",
        &[st.pgt_id.into()],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    // nosemgrep: rust.spi.run.dynamic-format -- staged is an internally generated, quoted pg_temp name.
    Spi::run_with_args(
        &format!(
            "INSERT INTO pgtrickle.pgt_centroid_groups (pgt_id, row_id, group_count, state, state_checksum) \
             SELECT $1, __pgt_row_id, __pgt_group_count, __pgt_centroid_state, \
               pg_catalog.md5(pg_catalog.int8send($1::bigint) || __pgt_row_id || \
                 pg_catalog.int8send(__pgt_group_count) || pg_catalog.array_send(__pgt_centroid_state)) \
             FROM {staged}"
        ),
        &[st.pgt_id.into()],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    Ok(())
}

/// Remember a failed missing-state repair so later refreshes do not retry it.
pub(crate) fn mark_missing_state_unbound(st: &StreamTableMeta) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "INSERT INTO pgtrickle.pgt_centroid_states \
         (pgt_id, stream_relid, defining_query, contract_generation, schema_version, status) \
         SELECT pgt_id, pgt_relid, defining_query, contract_generation, 1, 'BUILDING' \
         FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1 AND pgt_relid = $2 \
           AND defining_query = $3 AND contract_generation = $4 ON CONFLICT (pgt_id) DO NOTHING",
        &[
            st.pgt_id.into(),
            st.pgt_relid.into(),
            st.defining_query.as_str().into(),
            st.contract_generation.into(),
        ],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))
}

/// Rebuild explicitly prepared state during FULL refresh, or retain the existing strategy.
pub(crate) fn prepare_for_full_refresh(
    st: &StreamTableMeta,
) -> Result<Option<String>, PgTrickleError> {
    if st.refresh_mode != crate::dag::RefreshMode::Differential {
        return Ok(None);
    }
    let installed = Spi::get_one::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass('pgtrickle.pgt_centroid_states')::oid",
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    if installed.is_none() {
        return Ok(None);
    }
    let prepared = Spi::get_one_with_args::<bool>(
        "SELECT (SELECT stream_relid = $2 AND defining_query = $3 AND contract_generation = $4 \
         AND schema_version = 1 AND status IN ('BUILDING', 'READY') \
         FROM pgtrickle.pgt_centroid_states WHERE pgt_id = $1)",
        &[
            st.pgt_id.into(),
            st.pgt_relid.into(),
            st.defining_query.as_str().into(),
            st.contract_generation.into(),
        ],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    let prepared = match prepared {
        Some(prepared) => prepared,
        None if crate::config::pg_trickle_enable_centroid_state() => true,
        None => return Ok(None),
    };
    let admitted = if prepared {
        crate::refresh::with_stream_owner(st, || {
            Ok(crate::dvm::centroid_numerics::centroid_staging_sql(&st.defining_query)?.is_some())
        })?
    } else {
        false
    };
    if !admitted {
        Spi::run_with_args(
            "DELETE FROM pgtrickle.pgt_centroid_states WHERE pgt_id = $1",
            &[st.pgt_id.into()],
        )
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
        return Ok(None);
    }
    prepare_centroid_state(st, false).map(Some)
}

/// Construct the visible row projection from the same owner snapshot as the state.
pub(crate) fn staged_insert_sql(
    st: &StreamTableMeta,
    staged: &str,
) -> Result<String, PgTrickleError> {
    let columns = crate::refresh::get_st_user_columns(st);
    if columns.is_empty() {
        return Err(PgTrickleError::InternalError(format!(
            "centroid stream table {}.{} has no output columns",
            st.pgt_schema, st.pgt_name
        )));
    }
    let columns = columns
        .iter()
        .map(|name| crate::sql_builder::ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "(__pgt_row_id, {columns}, __pgt_count) SELECT __pgt_row_id, {columns}, __pgt_group_count FROM {staged}"
    ))
}

/// Record that this transaction populated visible rows from the staged state.
pub(crate) fn mark_materialized(pgt_id: i64) -> Result<(), PgTrickleError> {
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_centroid_states SET materialized_xid = pg_current_xact_id(), \
         baseline_frontier = NULL WHERE pgt_id = $1 AND status = 'BUILDING'",
        &[pgt_id.into()],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))
}

/// Discard the baseline after a refresh that did not maintain private state.
pub(crate) fn invalidate_baseline(st: &StreamTableMeta) -> Result<(), PgTrickleError> {
    if st.refresh_mode != crate::dag::RefreshMode::Differential {
        return Ok(());
    }
    let installed = Spi::get_one::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass('pgtrickle.pgt_centroid_states')::oid",
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    if installed.is_some() {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_centroid_states c SET baseline_frontier = \
               CASE WHEN c.materialized_xid = pg_current_xact_id_if_assigned() \
                 AND c.baseline_frontier = t.frontier AND c.stream_relid = t.pgt_relid \
                 AND c.defining_query = t.defining_query AND c.contract_generation = t.contract_generation \
                 THEN c.baseline_frontier ELSE NULL END, materialized_xid = NULL \
             FROM pgtrickle.pgt_stream_tables t WHERE c.pgt_id = $1 AND t.pgt_id = c.pgt_id \
             AND (c.baseline_frontier IS NOT NULL OR c.materialized_xid IS NOT NULL)",
            &[st.pgt_id.into()],
        )
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    }
    Ok(())
}

/// Bind only a same-transaction FULL snapshot whose trigger buffers do not exceed its frontier.
pub(crate) fn bind_materialized_frontier(
    st: &StreamTableMeta,
    frontier: &crate::version::Frontier,
) -> Result<(), PgTrickleError> {
    if st.refresh_mode != crate::dag::RefreshMode::Differential {
        return Ok(());
    }
    let installed = Spi::get_one::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass('pgtrickle.pgt_centroid_states')::oid",
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    if installed.is_none() {
        return Ok(());
    }
    let frontier_json = serde_json::to_value(frontier).map_err(|error| {
        PgTrickleError::InternalError(format!("serialize centroid frontier: {error}"))
    })?;
    let pending = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c \
         JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) \
         WHERE c.pgt_id = $1 AND c.materialized_xid = pg_current_xact_id_if_assigned() \
           AND c.stream_relid = t.pgt_relid AND c.defining_query = t.defining_query \
           AND c.contract_generation = t.contract_generation AND c.schema_version = 1 \
           AND c.status = 'BUILDING' AND t.frontier = $2)",
        &[st.pgt_id.into(), pgrx::JsonB(frontier_json.clone()).into()],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?
    .unwrap_or(false);
    if !pending {
        return Ok(());
    }
    let dependencies = crate::catalog::StDependency::get_for_st(st.pgt_id)?;
    let safe = sources_at_frontier(&dependencies, frontier)?;
    if safe {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_centroid_states c SET baseline_frontier = t.frontier, materialized_xid = NULL \
             FROM pgtrickle.pgt_stream_tables t WHERE c.pgt_id = $1 AND t.pgt_id = c.pgt_id \
             AND c.materialized_xid = pg_current_xact_id_if_assigned() AND c.stream_relid = t.pgt_relid \
             AND c.defining_query = t.defining_query AND c.contract_generation = t.contract_generation \
             AND t.frontier = $2",
            &[st.pgt_id.into(), pgrx::JsonB(frontier_json).into()],
        )
    } else {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_centroid_states SET baseline_frontier = NULL, materialized_xid = NULL \
             WHERE pgt_id = $1 AND materialized_xid = pg_current_xact_id_if_assigned()",
            &[st.pgt_id.into()],
        )
    }.map_err(|error| PgTrickleError::SpiError(error.to_string()))
}

/// Check a frontier against source relations already locked against writes.
pub(crate) fn sources_at_frontier(
    dependencies: &[crate::catalog::StDependency],
    frontier: &crate::version::Frontier,
) -> Result<bool, PgTrickleError> {
    let mut safe = !dependencies.is_empty();
    for dependency in dependencies {
        if dependency.source_type != "TABLE"
            || dependency.cdc_mode != crate::catalog::CdcMode::Trigger
            || dependency.source_placement != "local"
        {
            safe = false;
            break;
        }
        let oid = dependency.source_relid;
        let ordinary = Spi::get_one_with_args::<bool>(
            "SELECT COALESCE((SELECT c.relkind = 'r' AND NOT c.relrowsecurity AND NOT EXISTS \
             (SELECT 1 FROM pg_catalog.pg_inherits i WHERE i.inhparent = c.oid) \
             FROM pg_catalog.pg_class c WHERE c.oid = $1), false)",
            &[oid.into()],
        )
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?
        .unwrap_or(false);
        let Some(position) = frontier.sources.get(&oid.to_u32().to_string()) else {
            safe = false;
            break;
        };
        if !ordinary || crate::lsn::Lsn::parse(&position.lsn).is_err() {
            safe = false;
            break;
        }
        let buffer = crate::cdc::buffer_qualified_name_for_oid(
            &crate::config::pg_trickle_change_buffer_schema(),
            oid,
        );
        let exists = Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT pg_catalog.to_regclass($1)::oid",
            &[buffer.as_str().into()],
        )
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
        if exists.is_none() {
            safe = false;
            break;
        }
        // nosemgrep: rust.spi.get_one_with_args.dynamic-format -- buffer is catalog-derived and quoted; the frontier is a parameter.
        let ahead = Spi::get_one_with_args::<bool>(
            &format!("SELECT EXISTS (SELECT 1 FROM {buffer} WHERE lsn > $1::pg_lsn)"),
            &[position.lsn.as_str().into()],
        )
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?
        .unwrap_or(true);
        if ahead {
            safe = false;
            break;
        }
    }
    Ok(safe)
}

/// Internal entry point for preparing incomplete private state.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn _centroid_prepare_state(stream_table: pg_sys::Oid) -> String {
    let result = (|| {
        let st = StreamTableMeta::get_by_relid(stream_table)?;
        super::helpers::check_stream_table_ownership(st.pgt_relid, &st.pgt_schema, &st.pgt_name)?;
        prepare_centroid_state(&st, true)
    })();
    result.unwrap_or_else(|error: PgTrickleError| pgrx::error!("{error}"))
}

/// Internal entry point for exercising owner-context staging.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn _centroid_stage_state(stream_table: pg_sys::Oid) -> String {
    let result = (|| {
        let st = StreamTableMeta::get_by_relid(stream_table)?;
        super::helpers::check_stream_table_ownership(st.pgt_relid, &st.pgt_schema, &st.pgt_name)?;
        stage_centroid_initialization(&st)
    })();
    result.unwrap_or_else(|error: PgTrickleError| pgrx::error!("{error}"))
}

/// Require a complete, non-regressing CDC window for every admitted source.
fn validate_centroid_window(
    dependencies: &[crate::catalog::StDependency],
    previous: &crate::version::Frontier,
    next: &crate::version::Frontier,
) -> Result<(), PgTrickleError> {
    if dependencies.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "centroid frontier has no source dependencies".into(),
        ));
    }
    for dependency in dependencies {
        if dependency.source_type != "TABLE" {
            return Err(PgTrickleError::UnsupportedOperator(format!(
                "centroid frontier does not support source type {}",
                dependency.source_type
            )));
        }
        let key = dependency.source_relid.to_u32().to_string();
        let parse = |frontier: &crate::version::Frontier, bound: &str| {
            let position = frontier.sources.get(&key).ok_or_else(|| {
                PgTrickleError::InvalidArgument(format!(
                    "centroid {bound} frontier is missing source {key}"
                ))
            })?;
            crate::lsn::Lsn::parse(&position.lsn).map_err(|error| {
                PgTrickleError::InvalidArgument(format!(
                    "invalid centroid frontier LSN for source {key}: {error}"
                ))
            })
        };
        if parse(next, "next")? < parse(previous, "previous")? {
            return Err(PgTrickleError::InvalidArgument(format!(
                "centroid frontier regresses for source {key}"
            )));
        }
    }
    Ok(())
}

/// Internal query generator for signed centroid contributions.
fn centroid_delta_request(
    stream_table: pg_sys::Oid,
    previous: &str,
    next: &str,
) -> Result<
    (
        StreamTableMeta,
        crate::version::Frontier,
        crate::version::Frontier,
    ),
    PgTrickleError,
> {
    let st = StreamTableMeta::get_by_relid(stream_table)?;
    super::helpers::check_stream_table_ownership(st.pgt_relid, &st.pgt_schema, &st.pgt_name)?;
    let previous = crate::version::Frontier::from_json(previous).map_err(|error| {
        PgTrickleError::InvalidArgument(format!("invalid prior frontier: {error}"))
    })?;
    let next = crate::version::Frontier::from_json(next).map_err(|error| {
        PgTrickleError::InvalidArgument(format!("invalid next frontier: {error}"))
    })?;
    for frontier in [&previous, &next] {
        for source in frontier.sources.values() {
            crate::lsn::Lsn::parse(&source.lsn).map_err(|error| {
                PgTrickleError::InvalidArgument(format!("invalid centroid frontier LSN: {error}"))
            })?;
        }
    }
    let dependencies = crate::catalog::StDependency::get_for_st(st.pgt_id)?;
    validate_centroid_window(&dependencies, &previous, &next)?;
    Ok((st, previous, next))
}

#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn _centroid_delta_sql(stream_table: pg_sys::Oid, previous: &str, next: &str) -> String {
    centroid_delta_request(stream_table, previous, next)
        .and_then(|(st, previous, next)| {
            Ok(crate::dvm::generate_centroid_delta_query(&st, &previous, &next)?.delta_sql)
        })
        .unwrap_or_else(|error| pgrx::error!("{error}"))
}

/// Materialize bounded signed contributions under the stream owner's role.
fn stage_centroid_delta(
    st: &StreamTableMeta,
    previous: &crate::version::Frontier,
    next: &crate::version::Frontier,
) -> Result<String, PgTrickleError> {
    let admitted = crate::refresh::with_stream_owner(st, || {
        Ok(crate::dvm::centroid_numerics::centroid_staging_sql(&st.defining_query)?.is_some())
    })?;
    if !admitted {
        return Err(PgTrickleError::UnsupportedOperator(
            "centroid differential query is not admitted".into(),
        ));
    }
    let nonce = NEXT_STAGE.fetch_add(1, Ordering::Relaxed);
    let dependencies = crate::catalog::StDependency::get_for_st(st.pgt_id)?;
    validate_centroid_window(&dependencies, previous, next)?;
    let buffers = crate::refresh::delta_stage::DeltaStage::prepare_centroid(
        st,
        &dependencies,
        previous,
        next,
        nonce,
    )?;
    let delta = crate::dvm::generate_centroid_delta_query_staged(
        st,
        previous,
        next,
        buffers.source_tables(),
    )?
    .delta_sql;
    let table = crate::refresh::with_stream_owner(st, || {
        let name = format!("__pgt_delta_{}_{}", st.pgt_id, nonce);
        let table = format!("pg_temp.{}", crate::sql_builder::ident(&name));
        let existing = Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT pg_catalog.to_regclass($1)::oid",
            &[table.as_str().into()],
        )
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
        if existing.is_some() {
            return Err(PgTrickleError::InternalError(format!(
                "centroid delta staging relation {table} already exists"
            )));
        }
        // nosemgrep: rust.spi.run.dynamic-format -- table is internally named and delta is generated from the admitted definition.
        Spi::run(&format!(
            "CREATE TEMP TABLE {table} ON COMMIT DROP AS {delta}"
        ))
        .map_err(|error| PgTrickleError::SpiError(format!("centroid delta staging: {error}")))?;
        Ok(table)
    })?;
    for source in buffers.source_tables().values() {
        // nosemgrep: rust.spi.run.dynamic-format -- source is an internally generated, quoted temp relation.
        Spi::run(&format!("DROP TABLE {source}"))
            .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    }
    Ok(table)
}

/// Internal owner-context staging of a bounded centroid delta.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn _centroid_stage_delta(stream_table: pg_sys::Oid, previous: &str, next: &str) -> String {
    centroid_delta_request(stream_table, previous, next)
        .and_then(|(st, previous, next)| stage_centroid_delta(&st, &previous, &next))
        .unwrap_or_else(|error| pgrx::error!("{error}"))
}

/// Build checked candidate rows without changing either state or storage.
fn centroid_candidate_sql(
    st: &StreamTableMeta,
    delta_sql: &str,
    staged_old: Option<&str>,
) -> Result<String, PgTrickleError> {
    let valid = Spi::get_one_with_args::<bool>(
        "SELECT (SELECT stream_relid = $2 AND defining_query = $3 AND contract_generation = $4 \
         AND schema_version = 1 AND status IN ('BUILDING', 'READY') \
         FROM pgtrickle.pgt_centroid_states WHERE pgt_id = $1)",
        &[
            st.pgt_id.into(),
            st.pgt_relid.into(),
            st.defining_query.as_str().into(),
            st.contract_generation.into(),
        ],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?
    .unwrap_or(false);
    if !valid {
        return Err(PgTrickleError::InvalidArgument(
            "centroid state is missing or stale".into(),
        ));
    }
    let parsed = crate::refresh::with_stream_owner(st, || {
        let mut parsed = crate::dvm::parse_defining_query_full(&st.defining_query)?;
        crate::dvm::parser::enrich_vector_aggregates(&st.defining_query, &mut parsed, false)?;
        Ok(parsed)
    })?;
    let crate::dvm::parser::OpTree::Aggregate {
        group_by,
        aggregates,
        ..
    } = &parsed.tree
    else {
        return Err(PgTrickleError::UnsupportedOperator(
            "centroid aggregate is missing".into(),
        ));
    };
    let [aggregate] = aggregates.as_slice() else {
        return Err(PgTrickleError::UnsupportedOperator(
            "centroid aggregate is ambiguous".into(),
        ));
    };
    let metadata = aggregate
        .analyzed_argument
        .as_ref()
        .ok_or_else(|| PgTrickleError::InvalidArgument("centroid vector type is missing".into()))?;
    let groups = group_by
        .iter()
        .map(|expr| crate::sql_builder::ident(&expr.output_name()))
        .collect::<Vec<_>>();
    let delta_groups = groups
        .iter()
        .map(|name| format!("d.{name}"))
        .collect::<Vec<_>>();
    let result_groups = groups
        .iter()
        .map(|name| format!("c.{name}"))
        .collect::<Vec<_>>();
    let delta_prefix = if delta_groups.is_empty() {
        String::new()
    } else {
        format!("{}, ", delta_groups.join(", "))
    };
    let result_prefix = if result_groups.is_empty() {
        String::new()
    } else {
        format!("{}, ", result_groups.join(", "))
    };
    let action = if groups.is_empty() {
        "'I'::text"
    } else {
        "CASE WHEN c.__pgt_count = 0 THEN 'D' ELSE 'I' END"
    };
    let table = staged_old.map(str::to_owned).unwrap_or_else(|| {
        format!(
            "{}.{}",
            crate::sql_builder::ident(&st.pgt_schema),
            crate::sql_builder::ident(&st.pgt_name)
        )
    });
    let vector_type = format!(
        "{}.vector({})",
        crate::sql_builder::ident(&metadata.type_schema),
        metadata.typmod
    );
    let alias = crate::sql_builder::ident(&aggregate.alias);
    Ok(format!(
        "WITH __pgt_candidate_delta AS MATERIALIZED ({delta_sql}), \
         __pgt_candidate_merge AS MATERIALIZED (\
           SELECT d.__pgt_row_id, {delta_prefix}COALESCE(g.group_count, 0) + d.__pgt_group_delta AS __pgt_count, \
             pgtrickle._centroid_merge(CASE WHEN g.row_id IS NULL THEN ARRAY[]::float8[] \
               WHEN g.state_checksum = pg_catalog.md5(pg_catalog.int8send(g.pgt_id) || g.row_id || \
                 pg_catalog.int8send(g.group_count) || pg_catalog.array_send(g.state)) THEN g.state \
               ELSE NULL::float8[] END, d.__pgt_centroid_delta, {dim}, \
               CASE WHEN old.__pgt_row_id IS NOT NULL AND g.row_id IS NOT NULL AND old.__pgt_count IS DISTINCT FROM g.group_count \
                    THEN NULL::boolean ELSE old.__pgt_row_id IS NULL END) AS state \
           FROM __pgt_candidate_delta d LEFT JOIN {table} old ON old.__pgt_row_id = d.__pgt_row_id \
           LEFT JOIN pgtrickle.pgt_centroid_groups g ON g.pgt_id = {id} AND g.row_id = d.__pgt_row_id), \
         __pgt_candidate_checked AS MATERIALIZED (\
           SELECT m.*, pgtrickle._centroid_check_count(m.state, m.__pgt_count, {dim}) AS checked_state \
           FROM __pgt_candidate_merge m) \
         SELECT c.__pgt_row_id, {action} AS __pgt_action, {result_prefix}\
                pgtrickle._centroid_finalize(c.checked_state, {dim})::{vector_type} AS {alias}, \
                c.__pgt_count, c.checked_state AS __pgt_centroid_state \
         FROM __pgt_candidate_checked c",
        dim = metadata.typmod,
        id = st.pgt_id,
    ))
}

/// Internal query generator for checked centroid candidates.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn _centroid_candidate_sql(stream_table: pg_sys::Oid, previous: &str, next: &str) -> String {
    let result =
        centroid_delta_request(stream_table, previous, next).and_then(|(st, previous, next)| {
            let delta = crate::dvm::generate_centroid_delta_query(&st, &previous, &next)?.delta_sql;
            centroid_candidate_sql(&st, &delta, None)
        });
    result.unwrap_or_else(|error| pgrx::error!("{error}"))
}

pub(crate) struct StagedCentroidCandidates {
    pub(crate) private: String,
    pub(crate) public: String,
}

/// Remove default grants before exposing a staged relation across an owner boundary.
fn restrict_staged_table(table: &str) -> Result<(), PgTrickleError> {
    // nosemgrep: rust.spi.run.dynamic-format -- table is an internally generated temp relation.
    Spi::run(&format!("REVOKE ALL ON TABLE {table} FROM PUBLIC"))
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    let grantees = Spi::connect(|client| {
        let rows = client.select(
            "SELECT DISTINCT r.rolname::text FROM pg_catalog.pg_class c \
             CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a \
             JOIN pg_catalog.pg_roles r ON r.oid = a.grantee \
             WHERE c.oid = pg_catalog.to_regclass($1) AND a.grantee <> c.relowner",
            None,
            &[table.into()],
        )?;
        rows.into_iter()
            .map(|row| row.get::<String>(1))
            .collect::<Result<Vec<_>, _>>()
    })
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    for grantee in grantees.into_iter().flatten() {
        let role = crate::sql_builder::ident(&grantee);
        // nosemgrep: rust.spi.run.dynamic-format -- table is internal and role is catalog-derived and quoted.
        Spi::run(&format!("REVOKE ALL ON TABLE {table} FROM {role}"))
            .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    }
    Ok(())
}

/// Stage checked candidates for transactional application.
pub(crate) fn stage_centroid_candidates(
    st: &StreamTableMeta,
    previous: &crate::version::Frontier,
    next: &crate::version::Frontier,
    require_owner: bool,
) -> Result<StagedCentroidCandidates, PgTrickleError> {
    let locked = lock_centroid_stream(st, require_owner)?;
    let st = &locked;
    let previous_json = serde_json::to_value(previous).map_err(|error| {
        PgTrickleError::InternalError(format!("serialize centroid frontier: {error}"))
    })?;
    let matches = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c \
         JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) \
         WHERE c.pgt_id = $1 AND c.baseline_frontier = $2 AND t.frontier = $2 \
           AND c.materialized_xid IS NULL AND c.stream_relid = t.pgt_relid \
           AND c.defining_query = t.defining_query AND c.contract_generation = t.contract_generation \
           AND c.schema_version = 1 AND c.status = 'BUILDING')",
        &[st.pgt_id.into(), pgrx::JsonB(previous_json).into()],
    ).map_err(|error| PgTrickleError::SpiError(error.to_string()))?.unwrap_or(false);
    if !matches {
        return Err(PgTrickleError::InvalidArgument(
            "centroid state is not bound to the current frontier".into(),
        ));
    }
    let delta = stage_centroid_delta(st, previous, next)?;
    let nonce = NEXT_STAGE.fetch_add(1, Ordering::Relaxed);
    let old_table = format!(
        "pg_temp.{}",
        crate::sql_builder::ident(&format!("__pgt_co_{}_{}", st.pgt_id, nonce))
    );
    let candidate_table = format!(
        "pg_temp.{}",
        crate::sql_builder::ident(&format!("__pgt_cc_{}_{}", st.pgt_id, nonce))
    );
    let public_table = format!(
        "pg_temp.{}",
        crate::sql_builder::ident(&format!("__pgt_cp_{}_{}", st.pgt_id, nonce))
    );
    for table in [&old_table, &candidate_table, &public_table] {
        let exists = Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT pg_catalog.to_regclass($1)::oid",
            &[table.as_str().into()],
        )
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
        if exists.is_some() {
            return Err(PgTrickleError::InternalError(format!(
                "centroid candidate staging relation {table} already exists"
            )));
        }
    }
    let target = format!(
        "{}.{}",
        crate::sql_builder::ident(&st.pgt_schema),
        crate::sql_builder::ident(&st.pgt_name)
    );
    crate::refresh::with_stream_owner(st, || {
        // nosemgrep: rust.spi.run.dynamic-format -- all identifiers are quoted catalog or internally generated names.
        Spi::run(&format!(
            "CREATE TEMP TABLE {old_table} ON COMMIT DROP AS \
             SELECT s.__pgt_row_id, s.__pgt_count FROM {target} s JOIN {delta} d \
             ON pgtrickle.row_probe_v1(s.__pgt_row_id) OPERATOR(pg_catalog.=) pgtrickle.row_probe_v1(d.__pgt_row_id) \
             AND s.__pgt_row_id OPERATOR(pg_catalog.=) d.__pgt_row_id"
        ))
        .map_err(|error| PgTrickleError::SpiError(format!("centroid public staging: {error}")))
    })?;
    let sql = centroid_candidate_sql(st, &format!("SELECT * FROM {delta}"), Some(&old_table))?;
    // nosemgrep: rust.spi.run.dynamic-format -- candidate_table is internally named and sql only reads validated staged relations and private state.
    Spi::run(&format!(
        "CREATE TEMP TABLE {candidate_table} ON COMMIT DROP AS {sql}"
    ))
    .map_err(|error| PgTrickleError::SpiError(format!("centroid candidate staging: {error}")))?;
    restrict_staged_table(&candidate_table)?;
    let columns = crate::refresh::get_st_user_columns(st)
        .iter()
        .map(|name| crate::sql_builder::ident(name))
        .collect::<Vec<_>>()
        .join(", ");
    // nosemgrep: rust.spi.run.dynamic-format -- table names are internal and columns are quoted catalog identifiers.
    Spi::run(&format!(
        "CREATE TEMP TABLE {public_table} ON COMMIT DROP AS \
         SELECT __pgt_row_id, __pgt_action, {columns}, __pgt_count FROM {candidate_table}"
    ))
    .map_err(|error| PgTrickleError::SpiError(format!("centroid public candidates: {error}")))?;
    restrict_staged_table(&public_table)?;
    let owner = crate::sql_builder::ident(&crate::refresh::stream_owner_name(st)?);
    // nosemgrep: rust.spi.run.dynamic-format -- table is internal and owner is catalog-derived and quoted.
    Spi::run(&format!("GRANT SELECT ON TABLE {public_table} TO {owner}"))
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    Ok(StagedCentroidCandidates {
        private: candidate_table,
        public: public_table,
    })
}

/// Apply checked visible rows and private state in the caller's transaction.
pub(crate) fn apply_centroid_candidates(
    st: &StreamTableMeta,
    staged: &StagedCentroidCandidates,
    next: &crate::version::Frontier,
) -> Result<i64, PgTrickleError> {
    let table = crate::sql_builder::qualified(&st.pgt_schema, &st.pgt_name);
    let mut columns = crate::refresh::get_st_user_columns(st);
    columns.push("__pgt_count".to_string());
    let merge = crate::refresh::codegen::build_merge_sql(&table, &staged.public, &columns, false);
    let applied = crate::refresh::with_stream_owner(st, || {
        Spi::connect_mut(|client| {
            let rows = client
                .update(&merge, None, &[])
                .map_err(|error| PgTrickleError::SpiError(format!("centroid merge: {error}")))?;
            Ok(rows.len() as i64)
        })
    })?;
    let private = &staged.private;
    // nosemgrep: rust.spi.run_with_args.dynamic-format -- private is an internally generated temp relation.
    Spi::run_with_args(
        &format!(
            "DELETE FROM pgtrickle.pgt_centroid_groups g USING {private} c \
         WHERE g.pgt_id = $1 AND g.row_id = c.__pgt_row_id AND c.__pgt_action = 'D'"
        ),
        &[st.pgt_id.into()],
    )
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    // nosemgrep: rust.spi.run_with_args.dynamic-format -- private is an internally generated temp relation.
    Spi::run_with_args(&format!(
        "INSERT INTO pgtrickle.pgt_centroid_groups (pgt_id, row_id, group_count, state, state_checksum) \
         SELECT $1, c.__pgt_row_id, c.__pgt_count, c.__pgt_centroid_state, \
           pg_catalog.md5(pg_catalog.int8send($1::bigint) || c.__pgt_row_id || \
             pg_catalog.int8send(c.__pgt_count) || pg_catalog.array_send(c.__pgt_centroid_state)) \
         FROM {private} c WHERE c.__pgt_action = 'I' \
         ON CONFLICT (pgt_id, row_id) DO UPDATE SET group_count = EXCLUDED.group_count, \
           state = EXCLUDED.state, state_checksum = EXCLUDED.state_checksum"
    ), &[st.pgt_id.into()]).map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    set_pending_frontier(st, next, (applied > 0).then_some(false))?;
    Ok(applied)
}

fn set_pending_frontier(
    st: &StreamTableMeta,
    next: &crate::version::Frontier,
    last_change_limit_rebase: Option<bool>,
) -> Result<(), PgTrickleError> {
    let next = serde_json::to_value(next).map_err(|error| {
        PgTrickleError::InternalError(format!("serialize centroid frontier: {error}"))
    })?;
    let updated = Spi::connect_mut(|client| {
        let rows = client.update(
            "UPDATE pgtrickle.pgt_centroid_states SET baseline_frontier = $2, materialized_xid = pg_current_xact_id(), \
             last_change_limit_rebase = COALESCE($3, last_change_limit_rebase) WHERE pgt_id = $1",
            None, &[st.pgt_id.into(), pgrx::JsonB(next).into(), last_change_limit_rebase.into()],
        ).map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
        Ok::<_, PgTrickleError>(rows.len())
    })?;
    if updated != 1 {
        return Err(PgTrickleError::InternalError(
            "centroid state disappeared during apply".into(),
        ));
    }
    Ok(())
}

/// Advance a bound state when differential refresh found no source changes.
pub(crate) fn advance_empty_frontier(
    st: &StreamTableMeta,
    dependencies: &[crate::catalog::StDependency],
    previous: &crate::version::Frontier,
    next: &crate::version::Frontier,
) -> Result<(), PgTrickleError> {
    let previous = serde_json::to_value(previous).map_err(|error| {
        PgTrickleError::InternalError(format!("serialize centroid frontier: {error}"))
    })?;
    let bound = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_centroid_states c JOIN pgtrickle.pgt_stream_tables t USING (pgt_id) \
         WHERE c.pgt_id = $1 AND c.baseline_frontier = $2 AND t.frontier = $2 \
           AND c.materialized_xid IS NULL AND c.stream_relid = t.pgt_relid \
           AND c.defining_query = t.defining_query AND c.contract_generation = t.contract_generation \
           AND c.schema_version = 1 AND c.status = 'BUILDING')",
        &[st.pgt_id.into(), pgrx::JsonB(previous).into()],
    ).map_err(|error| PgTrickleError::SpiError(error.to_string()))?.unwrap_or(false);
    if bound {
        let oids = dependencies
            .iter()
            .map(|dependency| dependency.source_relid)
            .collect::<Vec<_>>();
        crate::cdc::lock_source_relations(&oids)?;
        if sources_at_frontier(dependencies, next)? {
            set_pending_frontier(st, next, None)?;
        }
    }
    Ok(())
}

/// Rebuild state only when a locked source snapshot exactly matches stored rows.
pub(crate) fn rebase_after_rescan(
    st: &StreamTableMeta,
    dependencies: &[crate::catalog::StDependency],
    frontier: &crate::version::Frontier,
    change_limit: bool,
) -> Result<bool, PgTrickleError> {
    let st = lock_centroid_stream(st, false)?;
    let source_oids = dependencies
        .iter()
        .map(|dependency| dependency.source_relid)
        .collect::<Vec<_>>();
    crate::cdc::lock_source_relations(&source_oids)?;
    if !sources_at_frontier(dependencies, frontier)? {
        return Ok(false);
    }
    let target_without_rls = Spi::get_one_with_args::<bool>(
        "SELECT COALESCE((SELECT NOT relrowsecurity FROM pg_catalog.pg_class WHERE oid = $1), false)",
        &[st.pgt_relid.into()],
    ).map_err(|error| PgTrickleError::SpiError(error.to_string()))?.unwrap_or(false);
    if !target_without_rls {
        return Ok(false);
    }
    let senders = crate::dvm::pgvector_binary_senders()?;
    let vector_columns = Spi::connect(|client| {
        let rows = client.select(
            "SELECT attname::text, atttypid::bigint FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped",
            None, &[st.pgt_relid.into()],
        )?;
        let mut columns = Vec::new();
        for row in rows {
            if let (Some(name), Some(oid)) = (row.get::<String>(1)?, row.get::<i64>(2)?)
                && let Ok(oid) = u32::try_from(oid)
                && let Some(sender) = senders.get(&oid)
            { columns.push((name, sender.clone())); }
        }
        Ok::<_, pgrx::spi::Error>(columns)
    }).map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    let [(column, sender)] = vector_columns.as_slice() else {
        return Ok(false);
    };
    let staged = stage_centroid_initialization(&st)?;
    // nosemgrep: rust.spi.get_one.dynamic-format -- staged is an internally generated, quoted temp relation.
    let qualified = Spi::get_one::<bool>(&format!(
        "SELECT NOT EXISTS (SELECT 1 FROM {staged} s CROSS JOIN LATERAL \
         (SELECT (pg_catalog.cardinality(s.__pgt_centroid_state) - 3) / 2 AS dim) d \
         WHERE pgtrickle._centroid_rebase_reason(s.__pgt_centroid_state[1]::bigint, \
           s.__pgt_centroid_state[2]::bigint, s.__pgt_centroid_state[3]::bigint, \
           s.__pgt_centroid_state[4:3 + d.dim], s.__pgt_centroid_state[4 + d.dim:3 + 2 * d.dim]) IS NOT NULL)"
    )).map_err(|error| PgTrickleError::SpiError(error.to_string()))?.unwrap_or(false);
    if !qualified {
        return Ok(false);
    }
    let target = crate::sql_builder::qualified(&st.pgt_schema, &st.pgt_name);
    let column = crate::sql_builder::ident(column);
    // nosemgrep: rust.spi.get_one.dynamic-format -- relations and sender are internally generated or catalog-derived and quoted.
    let matches = Spi::get_one::<bool>(&format!(
        "WITH expected AS MATERIALIZED (SELECT __pgt_row_id, __pgt_group_count AS n, {sender}({column}) AS value FROM {staged}), \
         actual AS MATERIALIZED (SELECT __pgt_row_id, __pgt_count AS n, {sender}({column}) AS value FROM {target}) \
         SELECT NOT EXISTS ((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual) UNION ALL \
                            (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))"
    )).map_err(|error| PgTrickleError::SpiError(error.to_string()))?.unwrap_or(false);
    if !matches {
        return Ok(false);
    }
    store_centroid_state(&st, &staged)?;
    set_pending_frontier(&st, frontier, Some(change_limit))?;
    Ok(true)
}

/// Internal owner-separated staging of checked centroid candidates.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
fn _centroid_stage_candidates(stream_table: pg_sys::Oid, previous: &str, next: &str) -> String {
    centroid_delta_request(stream_table, previous, next)
        .and_then(|(st, previous, next)| stage_centroid_candidates(&st, &previous, &next, true))
        .map(|staged| {
            let _ = staged.public;
            staged.private
        })
        .unwrap_or_else(|error| pgrx::error!("{error}"))
}

extension_sql!(
    r#"REVOKE ALL ON FUNCTION pgtrickle._centroid_stage_state(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle._centroid_prepare_state(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle._centroid_delta_sql(oid, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle._centroid_stage_delta(oid, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle._centroid_stage_candidates(oid, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION pgtrickle._centroid_candidate_sql(oid, text, text) FROM PUBLIC;"#,
    name = "pg_trickle_centroid_stage_privileges",
    requires = [
        _centroid_stage_state,
        _centroid_prepare_state,
        _centroid_delta_sql,
        _centroid_stage_delta,
        _centroid_stage_candidates,
        _centroid_candidate_sql
    ],
);
