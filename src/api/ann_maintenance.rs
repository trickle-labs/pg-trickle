//! Durable, explicit ANN maintenance intents.

use pgrx::prelude::*;

use crate::error::PgTrickleError;

/// Record one explicit ANN maintenance intent without executing a rebuild.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn request_ann_maintenance(stream_table: pg_sys::Oid, index_oid: pg_sys::Oid) -> i64 {
    request_ann_maintenance_impl(stream_table, index_oid)
        .unwrap_or_else(|error| pgrx::error!("request_ann_maintenance: {error}"))
}

fn request_ann_maintenance_impl(
    stream_table: pg_sys::Oid,
    index: pg_sys::Oid,
) -> Result<i64, PgTrickleError> {
    let caller = super::helpers::outer_user_id();
    let request = Spi::connect_mut(|client| {
        let rows = client.update(
            "WITH RECURSIVE root AS (\
               SELECT s.pgt_id, s.pgt_relid, s.ann_refresh_epoch, c.relnamespace \
               FROM pgtrickle.pgt_stream_tables s \
               JOIN pg_catalog.pg_class c ON c.oid = s.pgt_relid \
               WHERE s.pgt_relid = $1\
             ), storage(relid) AS (\
               SELECT pgt_relid FROM root \
               UNION ALL SELECT i.inhrelid FROM pg_catalog.pg_inherits i \
                 JOIN storage parent ON parent.relid = i.inhparent\
             ) \
             INSERT INTO pgtrickle.pgt_ann_maintenance_requests \
               (pgt_id, database_oid, system_identifier, root_relid, leaf_relid, \
                index_relid, index_filenode, index_definition, first_observed_epoch, last_observed_epoch) \
             SELECT root.pgt_id, (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()), \
               (SELECT system_identifier::text FROM pg_catalog.pg_control_system()), \
               root.pgt_relid, leaf.oid, idx.oid, pg_catalog.pg_relation_filenode(idx.oid), \
               pg_catalog.pg_get_indexdef(idx.oid), root.ann_refresh_epoch, root.ann_refresh_epoch \
             FROM root CROSS JOIN storage s \
             JOIN pg_catalog.pg_class leaf ON leaf.oid = s.relid \
             JOIN pg_catalog.pg_index i ON i.indrelid = leaf.oid \
             JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid \
             JOIN pg_catalog.pg_am am ON am.oid = idx.relam \
             WHERE idx.oid = $2 AND idx.relkind = 'i' AND am.amname IN ('hnsw', 'ivfflat') \
               AND i.indisvalid AND i.indisready AND i.indislive \
               AND pg_catalog.pg_relation_filenode(idx.oid) IS NOT NULL \
               AND pg_catalog.has_schema_privilege($3, root.relnamespace, 'USAGE') \
               AND pg_catalog.has_table_privilege($3, root.pgt_relid, 'SELECT') \
               AND pg_catalog.has_schema_privilege($3, leaf.relnamespace, 'USAGE') \
               AND pg_catalog.has_table_privilege($3, leaf.oid, 'MAINTAIN') \
             ON CONFLICT (pgt_id, index_relid, index_filenode) DO UPDATE \
               SET last_requested_at = pg_catalog.clock_timestamp(), \
                   pending_during_run = pgtrickle.pgt_ann_maintenance_requests.pending_during_run \
                     OR pgtrickle.pgt_ann_maintenance_requests.state = 'RUNNING', \
                   state = CASE WHEN pgtrickle.pgt_ann_maintenance_requests.state = 'RUNNING' \
                     THEN 'RUNNING' ELSE 'REQUESTED' END, \
                   first_observed_epoch = CASE \
                     WHEN pgtrickle.pgt_ann_maintenance_requests.first_observed_epoch IS NULL \
                       OR EXCLUDED.first_observed_epoch IS NULL THEN NULL \
                     ELSE LEAST(pgtrickle.pgt_ann_maintenance_requests.first_observed_epoch, EXCLUDED.first_observed_epoch) END, \
                   last_observed_epoch = CASE \
                     WHEN pgtrickle.pgt_ann_maintenance_requests.last_observed_epoch IS NULL \
                       OR EXCLUDED.last_observed_epoch IS NULL THEN NULL \
                     ELSE GREATEST(pgtrickle.pgt_ann_maintenance_requests.last_observed_epoch, EXCLUDED.last_observed_epoch) END \
             WHERE pgtrickle.pgt_ann_maintenance_requests.database_oid = EXCLUDED.database_oid \
               AND pgtrickle.pgt_ann_maintenance_requests.system_identifier = EXCLUDED.system_identifier \
               AND pgtrickle.pgt_ann_maintenance_requests.root_relid = EXCLUDED.root_relid \
               AND pgtrickle.pgt_ann_maintenance_requests.leaf_relid = EXCLUDED.leaf_relid \
               AND pgtrickle.pgt_ann_maintenance_requests.index_definition = EXCLUDED.index_definition \
             RETURNING request_id",
            Some(1),
            &[stream_table.into(), index.into(), caller.into()],
        )?;
        if rows.is_empty() {
            Ok(None)
        } else {
            rows.first().get::<i64>(1)
        }
    })
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    request.ok_or_else(|| {
        PgTrickleError::InvalidArgument(format!(
            "index {index} is not a valid, authorized ANN leaf of stream table {stream_table}, or its recorded identity changed"
        ))
    })
}

/// Inspect a request while rechecking the caller's current privileges.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn ann_maintenance_request(request_id: i64) -> pgrx::JsonB {
    ann_maintenance_request_impl(request_id)
        .unwrap_or_else(|error| pgrx::error!("ann_maintenance_request: {error}"))
}

fn ann_maintenance_request_impl(request_id: i64) -> Result<pgrx::JsonB, PgTrickleError> {
    let caller = super::helpers::outer_user_id();
    Spi::connect_mut(|client| {
        let rows = client.update(
        "SELECT (SELECT pg_catalog.jsonb_build_object(\
           'request_id', r.request_id, 'state', CASE WHEN r.state = 'RUNNING' AND NOT EXISTS (\
             SELECT 1 FROM pg_catalog.pg_stat_activity a WHERE a.pid = r.claim_pid \
               AND a.backend_start = r.claim_backend_start) THEN 'INTERRUPTED' ELSE r.state END, \
           'claim_epoch', r.claim_epoch, 'pending_during_run', r.pending_during_run, \
           'last_error', r.last_error, 'completed_at', r.completed_at, \
           'stream_table_oid', r.root_relid, 'leaf_table_oid', r.leaf_relid, \
           'index_oid', r.index_relid, 'first_observed_epoch', r.first_observed_epoch, \
           'last_observed_epoch', r.last_observed_epoch, 'current_epoch', s.ann_refresh_epoch, \
           'requested_at', r.requested_at, 'last_requested_at', r.last_requested_at, \
           'identity_current', r.database_oid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database()) \
             AND r.system_identifier = (SELECT system_identifier::text FROM pg_catalog.pg_control_system()) \
             AND r.root_relid = s.pgt_relid \
             AND EXISTS (WITH RECURSIVE storage(relid) AS (\
               SELECT s.pgt_relid UNION ALL SELECT h.inhrelid FROM pg_catalog.pg_inherits h \
               JOIN storage parent ON parent.relid = h.inhparent) \
               SELECT 1 FROM storage WHERE relid = r.leaf_relid) \
             AND EXISTS (\
               SELECT 1 FROM pg_catalog.pg_index i \
               JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid \
               JOIN pg_catalog.pg_am am ON am.oid = idx.relam \
               WHERE idx.oid = r.index_relid AND i.indrelid = r.leaf_relid \
                 AND idx.relkind = 'i' AND am.amname IN ('hnsw', 'ivfflat') \
                 AND i.indisvalid AND i.indisready AND i.indislive \
                 AND pg_catalog.pg_relation_filenode(idx.oid) = r.index_filenode \
                 AND pg_catalog.pg_get_indexdef(idx.oid) = r.index_definition)) \
         FROM pgtrickle.pgt_ann_maintenance_requests r \
         JOIN pgtrickle.pgt_stream_tables s ON s.pgt_id = r.pgt_id \
         JOIN pg_catalog.pg_class root ON root.oid = s.pgt_relid \
         LEFT JOIN pg_catalog.pg_class leaf ON leaf.oid = r.leaf_relid \
         WHERE r.request_id = $1 \
           AND pg_catalog.has_schema_privilege($2, root.relnamespace, 'USAGE') \
           AND pg_catalog.has_table_privilege($2, root.oid, 'SELECT') \
           AND CASE WHEN leaf.oid IS NULL THEN pg_catalog.pg_has_role($2, root.relowner, 'USAGE') \
                    ELSE pg_catalog.has_schema_privilege($2, leaf.relnamespace, 'USAGE') \
                     AND pg_catalog.has_table_privilege($2, leaf.oid, 'MAINTAIN') END)",
        None,
        &[request_id.into(), caller.into()],
        )?;
        rows.first().get::<pgrx::JsonB>(1)
    })
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?
    .ok_or_else(|| PgTrickleError::NotFound(format!("request {request_id} is unavailable or not authorized")))
}

/// Claim an intent for execution by this PostgreSQL session.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn claim_ann_maintenance(request_id: i64) -> pgrx::JsonB {
    claim_ann_maintenance_impl(request_id)
        .unwrap_or_else(|error| pgrx::error!("claim_ann_maintenance: {error}"))
}

fn claim_ann_maintenance_impl(request_id: i64) -> Result<pgrx::JsonB, PgTrickleError> {
    let status = ann_maintenance_request_impl(request_id)?;
    if status.0["identity_current"] != true {
        return Err(PgTrickleError::InvalidArgument(format!(
            "request {request_id} no longer identifies the requested index"
        )));
    }
    let caller = super::helpers::outer_user_id();
    let claimed = Spi::connect_mut(|client| {
        let guard = client.update(
            "INSERT INTO pgtrickle.pgt_ann_maintenance_claims \
               (pgt_id, leaf_relid, index_key, request_id, claim_pid, claim_backend_start) \
             SELECT r.pgt_id, r.leaf_relid, pg_catalog.sha256(pg_catalog.convert_to(r.index_definition, 'UTF8')), \
               r.request_id, pg_catalog.pg_backend_pid(), \
               (SELECT backend_start FROM pg_catalog.pg_stat_activity WHERE pid = pg_catalog.pg_backend_pid()) \
             FROM pgtrickle.pgt_ann_maintenance_requests r WHERE r.request_id = $1 \
             ON CONFLICT (pgt_id, leaf_relid, index_key) DO UPDATE \
               SET request_id = EXCLUDED.request_id, claim_pid = EXCLUDED.claim_pid, \
                   claim_backend_start = EXCLUDED.claim_backend_start \
             WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a \
               WHERE a.pid = pgtrickle.pgt_ann_maintenance_claims.claim_pid \
                 AND a.backend_start = pgtrickle.pgt_ann_maintenance_claims.claim_backend_start) \
             RETURNING request_id",
            Some(1),
            &[request_id.into()],
        )?;
        if guard.is_empty() {
            return Ok::<_, pgrx::spi::Error>(false);
        }
        let rows = client.update(
            "UPDATE pgtrickle.pgt_ann_maintenance_requests r \
             SET state = 'RUNNING', claim_pid = pg_catalog.pg_backend_pid(), \
                 claim_backend_start = (SELECT backend_start FROM pg_catalog.pg_stat_activity \
                   WHERE pid = pg_catalog.pg_backend_pid()), \
                 claim_role = $2, claim_epoch = r.last_observed_epoch, \
                 pending_during_run = false, last_error = NULL \
             WHERE r.request_id = $1 AND (r.state IN ('REQUESTED', 'FAILED') \
               OR (r.state = 'RUNNING' AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity a \
                 WHERE a.pid = r.claim_pid AND a.backend_start = r.claim_backend_start))) \
             RETURNING r.request_id",
            Some(1),
            &[request_id.into(), caller.into()],
        )?;
        Ok::<_, pgrx::spi::Error>(!rows.is_empty())
    })
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    if !claimed {
        return Err(PgTrickleError::InvalidArgument(format!(
            "request {request_id} or its index is already claimed, or the request is completed"
        )));
    }
    ann_maintenance_request_impl(request_id)
}

/// Record the outcome reported by the session that claimed an intent.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn finish_ann_maintenance(request_id: i64, succeeded: bool, error_message: &str) {
    finish_ann_maintenance_impl(request_id, succeeded, error_message)
        .unwrap_or_else(|error| pgrx::error!("finish_ann_maintenance: {error}"));
}

fn finish_ann_maintenance_impl(
    request_id: i64,
    succeeded: bool,
    error_message: &str,
) -> Result<(), PgTrickleError> {
    if error_message.len() > 2048
        || (succeeded && !error_message.is_empty())
        || (!succeeded && error_message.is_empty())
    {
        return Err(PgTrickleError::InvalidArgument("provide an empty error for success or a nonempty error of at most 2048 bytes for failure".into()));
    }
    if succeeded {
        // A revoked caller may report failure, but cannot claim successful coverage.
        ann_maintenance_request_impl(request_id)?;
    }
    let caller = super::helpers::outer_user_id();
    let finished = Spi::connect_mut(|client| {
        let rows = client.update(
            "WITH finished AS (UPDATE pgtrickle.pgt_ann_maintenance_requests r \
             SET state = CASE WHEN NOT $2 THEN 'FAILED' \
               WHEN r.pending_during_run THEN 'REQUESTED' ELSE 'SUCCEEDED' END, \
               last_error = CASE WHEN $2 THEN NULL ELSE $3 END, \
               completed_at = CASE WHEN $2 THEN pg_catalog.clock_timestamp() ELSE r.completed_at END, \
               claim_pid = NULL, claim_backend_start = NULL, claim_role = NULL \
             WHERE r.request_id = $1 AND r.state = 'RUNNING' AND r.claim_role = $4 \
               AND r.claim_pid = pg_catalog.pg_backend_pid() \
               AND r.claim_backend_start = (SELECT backend_start FROM pg_catalog.pg_stat_activity \
                 WHERE pid = pg_catalog.pg_backend_pid()) \
             RETURNING r.request_id) \
             DELETE FROM pgtrickle.pgt_ann_maintenance_claims guard USING finished \
             WHERE guard.request_id = finished.request_id \
               AND guard.claim_pid = pg_catalog.pg_backend_pid() \
               AND guard.claim_backend_start = (SELECT backend_start FROM pg_catalog.pg_stat_activity \
                 WHERE pid = pg_catalog.pg_backend_pid()) \
             RETURNING guard.request_id",
            Some(1),
            &[request_id.into(), succeeded.into(), error_message.into(), caller.into()],
        )?;
        Ok::<_, pgrx::spi::Error>(!rows.is_empty())
    })
    .map_err(|error| PgTrickleError::SpiError(error.to_string()))?;
    if !finished {
        return Err(PgTrickleError::InvalidArgument(format!(
            "request {request_id} is not claimed by this session and role"
        )));
    }
    Ok(())
}
