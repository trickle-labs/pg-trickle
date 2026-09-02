//! v0.92 recovery, capture ownership, and upgrade-boundary APIs.
//!
//! Recovery is deliberately fail-closed: a copied catalog may not resume
//! capture until the current database identity and every required source
//! frontier have been checked.

use super::*;
const STATE_ACTIVE: &str = "ACTIVE";
const STATE_QUARANTINED: &str = "QUARANTINED";
const STATE_QUIESCED: &str = "QUIESCED";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryClass {
    SelfRepairable,
    ReinitializationRequired,
    OperatorInterventionRequired,
}

impl RecoveryClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SelfRepairable => "SELF_REPAIRABLE",
            Self::ReinitializationRequired => "REINITIALIZATION_REQUIRED",
            Self::OperatorInterventionRequired => "OPERATOR_INTERVENTION_REQUIRED",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureFailure {
    MissingTrigger,
    MissingSlot,
    UnavailableWal,
    MissingBuffer,
    CorruptBuffer,
    CloneDetected,
    InterruptedRepair,
}

impl CaptureFailure {
    pub const fn reason_code(self) -> &'static str {
        match self {
            Self::MissingTrigger => "CDC_TRIGGER_MISSING",
            Self::MissingSlot => "CDC_SLOT_MISSING",
            Self::UnavailableWal => "CDC_WAL_UNAVAILABLE",
            Self::MissingBuffer => "CDC_BUFFER_MISSING",
            Self::CorruptBuffer => "CDC_BUFFER_CORRUPT",
            Self::CloneDetected => "CDC_CLONE_DETECTED",
            Self::InterruptedRepair => "CDC_REPAIR_INTERRUPTED",
        }
    }

    pub const fn class(self) -> RecoveryClass {
        match self {
            Self::CloneDetected | Self::InterruptedRepair => {
                RecoveryClass::OperatorInterventionRequired
            }
            Self::MissingTrigger
            | Self::MissingSlot
            | Self::UnavailableWal
            | Self::MissingBuffer
            | Self::CorruptBuffer => RecoveryClass::ReinitializationRequired,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaptureGate {
    Ready,
    Quiesced,
    Quarantined,
}

#[derive(Debug, Clone)]
struct InstanceState {
    instance_id: String,
    database_oid: i64,
    system_identifier: String,
    state: String,
    observed_database_oid: Option<i64>,
    observed_system_identifier: Option<String>,
}

fn current_database_oid() -> Result<i64, PgTrickleError> {
    Spi::get_one::<i64>(
        "SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = current_database()",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| PgTrickleError::NotFound("current database".to_string()))
}

fn current_system_identifier() -> Result<String, PgTrickleError> {
    Spi::get_one::<String>("SELECT system_identifier::text FROM pg_catalog.pg_control_system()")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| {
            PgTrickleError::InternalError("pg_control_system returned no system identifier".into())
        })
}

fn new_instance_id() -> Result<String, PgTrickleError> {
    Spi::get_one::<String>(
        "SELECT md5(format('%s:%s:%s:%s', current_database(), clock_timestamp(), \
         pg_backend_pid(), random()))",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InternalError("could not generate capture instance identity".into())
    })
}

fn load_instance_state() -> Result<Option<InstanceState>, PgTrickleError> {
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT instance_id::text, database_oid::bigint, system_identifier::text, \
                        state::text, observed_database_oid::bigint, \
                        observed_system_identifier::text \
                   FROM pgtrickle.pgt_capture_instance \
                  WHERE singleton",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if rows.is_empty() {
            return Ok(None);
        }
        let row = rows.first();
        Ok(Some(InstanceState {
            instance_id: row
                .get::<String>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("capture instance id is NULL".into())
                })?,
            database_oid: row
                .get::<i64>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("capture database oid is NULL".into())
                })?,
            system_identifier: row
                .get::<String>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("capture system identifier is NULL".into())
                })?,
            state: row
                .get::<String>(4)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_else(|| STATE_ACTIVE.to_string()),
            observed_database_oid: row
                .get::<i64>(5)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?,
            observed_system_identifier: row
                .get::<String>(6)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?,
        }))
    })
}

fn quarantine_instance(
    expected: &InstanceState,
    database_oid: i64,
    system_identifier: &str,
) -> Result<(), PgTrickleError> {
    let reason = format!(
        "capture ownership belongs to database_oid={} system_identifier={}, \
         observed database_oid={} system_identifier={}",
        expected.database_oid, expected.system_identifier, database_oid, system_identifier
    );
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_capture_instance
            SET state = 'QUARANTINED',
                observed_database_oid = $1,
                observed_system_identifier = $2,
                quarantine_reason = $3,
                last_seen_at = now()
          WHERE singleton",
        &[
            database_oid.into(),
            system_identifier.into(),
            reason.as_str().into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Spi::run(
        "UPDATE pgtrickle.pgt_stream_tables
            SET status = 'SUSPENDED',
                needs_reinit = true,
                refresh_reason = 'CDC_CLONE_DETECTED',
                refresh_reason_detail = 'Capture ownership changed after restore or clone; \
                                          run pgtrickle.recover_capture_instance() before repair.',
                updated_at = now()
          WHERE status <> 'SUSPENDED'",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))
}

fn ensure_capture_instance() -> Result<CaptureGate, PgTrickleError> {
    let database_oid = current_database_oid()?;
    let system_identifier = current_system_identifier()?;
    let existing = load_instance_state()?;
    let Some(instance) = existing else {
        let instance_id = new_instance_id()?;
        Spi::run_with_args(
            "INSERT INTO pgtrickle.pgt_capture_instance
                    (singleton, instance_id, database_oid, system_identifier, state)
             VALUES (true, $1, $2, $3, 'ACTIVE')",
            &[
                instance_id.as_str().into(),
                database_oid.into(),
                system_identifier.as_str().into(),
            ],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        return Ok(CaptureGate::Ready);
    };

    if instance.database_oid != database_oid
        || instance.system_identifier != system_identifier
        || instance.state == STATE_QUARANTINED
    {
        if instance.state != STATE_QUARANTINED {
            quarantine_instance(&instance, database_oid, &system_identifier)?;
        }
        return Ok(CaptureGate::Quarantined);
    }

    if instance.state == STATE_QUIESCED {
        return Ok(CaptureGate::Quiesced);
    }

    Spi::run("UPDATE pgtrickle.pgt_capture_instance SET last_seen_at = now() WHERE singleton")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Ok(CaptureGate::Ready)
}

pub(crate) fn capture_gate_allows_work() -> Result<bool, PgTrickleError> {
    Ok(matches!(ensure_capture_instance()?, CaptureGate::Ready))
}

pub(crate) fn assert_capture_ready() -> Result<(), PgTrickleError> {
    match ensure_capture_instance()? {
        CaptureGate::Ready => Ok(()),
        CaptureGate::Quiesced => Err(PgTrickleError::InvalidArgument(
            "pg_trickle capture is quiesced; run pgtrickle.resume_all() after the upgrade".into(),
        )),
        CaptureGate::Quarantined => Err(PgTrickleError::InvalidArgument(
            "pg_trickle capture is quarantined because database identity changed; \
             run pgtrickle.recover_capture_instance() and then repair affected stream tables"
                .into(),
        )),
    }
}

fn count_query(sql: &str) -> Result<i64, PgTrickleError> {
    Spi::get_one::<i64>(sql)
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::InternalError("recovery count query returned NULL".into()))
}

fn check(name: &str, status: &str, detail: String) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "status": status,
        "detail": detail,
    })
}

fn status_rank(status: &str) -> u8 {
    match status {
        "BLOCKER" => 4,
        "OPERATOR_INTERVENTION_REQUIRED" => 4,
        "REINITIALIZATION_REQUIRED" => 3,
        "DRAIN_RECOMMENDED" => 2,
        "WARNING" => 1,
        _ => 0,
    }
}

fn overall_status(checks: &[serde_json::Value]) -> &'static str {
    let max = checks
        .iter()
        .filter_map(|check| check.get("status").and_then(serde_json::Value::as_str))
        .map(status_rank)
        .max()
        .unwrap_or(0);
    match max {
        4 => "BLOCKER",
        3 => "BLOCKER",
        2 => "DRAIN_RECOMMENDED",
        1 => "WARNING",
        _ => "SAFE",
    }
}

fn validate_recovery_impl() -> Result<serde_json::Value, PgTrickleError> {
    let gate = ensure_capture_instance()?;
    let mut checks = Vec::new();

    if gate == CaptureGate::Quarantined {
        checks.push(check(
            "capture_instance",
            RecoveryClass::OperatorInterventionRequired.as_str(),
            "CDC_CLONE_DETECTED: database identity differs from the recorded capture owner; \
             capture remains disabled"
                .to_string(),
        ));
    } else {
        checks.push(check(
            "capture_instance",
            "SAFE",
            "database and PostgreSQL system identities match the recorded capture owner"
                .to_string(),
        ));
    }

    let missing_sources = count_query(
        "SELECT count(*)::bigint
           FROM pgtrickle.pgt_dependencies dep
          WHERE dep.source_type IN ('TABLE', 'FOREIGN_TABLE', 'MATVIEW')
            AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class c WHERE c.oid = dep.source_relid
            )",
    )?;
    if missing_sources > 0 {
        Spi::run(
            "UPDATE pgtrickle.pgt_stream_tables st
                SET status = 'SUSPENDED',
                    needs_reinit = true,
                    refresh_reason = 'CDC_SOURCE_MISSING',
                    refresh_reason_detail = 'A captured source relation is missing after restore.',
                    updated_at = now()
              WHERE st.pgt_id IN (
                  SELECT dep.pgt_id FROM pgtrickle.pgt_dependencies dep
                   WHERE dep.source_type IN ('TABLE', 'FOREIGN_TABLE', 'MATVIEW')
                     AND NOT EXISTS (
                         SELECT 1 FROM pg_catalog.pg_class c WHERE c.oid = dep.source_relid
                     )
              )",
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    checks.push(check(
        "catalog_dependencies",
        if missing_sources == 0 {
            "SAFE"
        } else {
            "BLOCKER"
        },
        format!("CDC_SOURCE_MISSING: {missing_sources} captured source relation(s) are missing"),
    ));

    let missing_triggers = count_query(
        "SELECT count(*)::bigint
           FROM pgtrickle.pgt_dependencies dep
          WHERE dep.source_type = 'TABLE'
            AND dep.cdc_mode IN ('TRIGGER', 'TRANSITIONING')
            AND NOT EXISTS (
                SELECT 1
                  FROM pg_catalog.pg_trigger trig
                 WHERE trig.tgrelid = dep.source_relid
                   AND NOT trig.tgisinternal
                   AND trig.tgenabled <> 'D'
                   AND trig.tgname LIKE 'pg_trickle_cdc_%'
            )",
    )?;
    if missing_triggers > 0 {
        Spi::run(
            "UPDATE pgtrickle.pgt_stream_tables st
                SET status = 'SUSPENDED',
                    needs_reinit = true,
                    refresh_reason = 'CDC_TRIGGER_MISSING',
                    refresh_reason_detail = 'A required CDC trigger is missing or disabled.',
                    updated_at = now()
              WHERE EXISTS (
                  SELECT 1
                    FROM pgtrickle.pgt_dependencies dep
                   WHERE dep.pgt_id = st.pgt_id
                     AND dep.source_type = 'TABLE'
                     AND dep.cdc_mode IN ('TRIGGER', 'TRANSITIONING')
                     AND NOT EXISTS (
                         SELECT 1
                           FROM pg_catalog.pg_trigger trig
                          WHERE trig.tgrelid = dep.source_relid
                            AND NOT trig.tgisinternal
                            AND trig.tgenabled <> 'D'
                            AND trig.tgname LIKE 'pg_trickle_cdc_%'
                     )
              )",
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    checks.push(check(
        "capture_triggers",
        if missing_triggers == 0 {
            "SAFE"
        } else {
            RecoveryClass::ReinitializationRequired.as_str()
        },
        format!(
            "CDC_TRIGGER_MISSING: {missing_triggers} source relation(s) are missing a CDC trigger"
        ),
    ));

    let missing_buffers = count_query(
        "SELECT count(*)::bigint
           FROM pgtrickle.pgt_dependencies dep
           JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = dep.pgt_id
          WHERE dep.source_type IN ('TABLE', 'FOREIGN_TABLE', 'MATVIEW')
            AND NOT EXISTS (
                SELECT 1 FROM pgtrickle.pgt_change_buffers cb
                 WHERE cb.source_kind = 'BASE' AND cb.source_id = dep.source_relid::bigint
            )",
    )?;
    if missing_buffers > 0 {
        Spi::run(
            "UPDATE pgtrickle.pgt_stream_tables st
                SET status = 'SUSPENDED',
                    needs_reinit = true,
                    refresh_reason = 'CDC_BUFFER_MISSING',
                    refresh_reason_detail = 'A required CDC buffer registration is missing.',
                    updated_at = now()
              WHERE EXISTS (
                  SELECT 1
                    FROM pgtrickle.pgt_dependencies dep
                   WHERE dep.pgt_id = st.pgt_id
                     AND dep.source_type IN ('TABLE', 'FOREIGN_TABLE', 'MATVIEW')
                     AND NOT EXISTS (
                         SELECT 1
                           FROM pgtrickle.pgt_change_buffers cb
                          WHERE cb.source_kind = 'BASE'
                            AND cb.source_id = dep.source_relid::bigint
                     )
              )",
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    checks.push(check(
        "capture_buffers",
        if missing_buffers == 0 {
            "SAFE"
        } else {
            RecoveryClass::ReinitializationRequired.as_str()
        },
        format!("CDC_BUFFER_MISSING: {missing_buffers} source buffer registration(s) are missing"),
    ));

    let missing_buffer_relations = count_query(
        "SELECT count(*)::bigint
           FROM pgtrickle.pgt_change_buffers cb
          WHERE cb.source_kind = 'BASE'
            AND to_regclass(format(
                    '%I.%I',
                    current_setting('pg_trickle.change_buffer_schema'),
                    cb.buffer_key
                )) IS NULL",
    )?;
    if missing_buffer_relations > 0 {
        Spi::run(
            "UPDATE pgtrickle.pgt_stream_tables st
                SET status = 'SUSPENDED',
                    needs_reinit = true,
                    refresh_reason = 'CDC_BUFFER_MISSING',
                    refresh_reason_detail = 'A registered CDC buffer relation is missing.',
                    updated_at = now()
              WHERE EXISTS (
                  SELECT 1
                    FROM pgtrickle.pgt_dependencies dep
                    JOIN pgtrickle.pgt_change_buffers cb
                      ON cb.source_kind = 'BASE'
                     AND cb.source_id = dep.source_relid::bigint
                   WHERE dep.pgt_id = st.pgt_id
                     AND to_regclass(format(
                             '%I.%I',
                             current_setting('pg_trickle.change_buffer_schema'),
                             cb.buffer_key
                         )) IS NULL
              )",
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    checks.push(check(
        "capture_buffer_relations",
        if missing_buffer_relations == 0 {
            "SAFE"
        } else {
            RecoveryClass::ReinitializationRequired.as_str()
        },
        format!(
            "CDC_BUFFER_MISSING: {missing_buffer_relations} registered physical \
             change-buffer relation(s) are missing"
        ),
    ));

    let corrupt_buffers = count_query(
        "SELECT count(*)::bigint
           FROM pgtrickle.pgt_change_buffers cb
          WHERE cb.source_kind = 'BASE'
            AND to_regclass(format(
                    '%I.%I',
                    current_setting('pg_trickle.change_buffer_schema'),
                    cb.buffer_key
                )) IS NOT NULL
            AND NOT (
                EXISTS (
                    SELECT 1
                      FROM information_schema.columns col
                     WHERE col.table_schema = current_setting('pg_trickle.change_buffer_schema')
                       AND col.table_name = cb.buffer_key
                       AND col.column_name = 'lsn'
                )
                AND EXISTS (
                    SELECT 1
                      FROM information_schema.columns col
                     WHERE col.table_schema = current_setting('pg_trickle.change_buffer_schema')
                       AND col.table_name = cb.buffer_key
                       AND col.column_name = 'action'
                )
                AND EXISTS (
                    SELECT 1
                      FROM information_schema.columns col
                     WHERE col.table_schema = current_setting('pg_trickle.change_buffer_schema')
                       AND col.table_name = cb.buffer_key
                       AND col.column_name = '__pgt_row_id'
                )
            )",
    )?;
    if corrupt_buffers > 0 {
        Spi::run(
            "UPDATE pgtrickle.pgt_stream_tables st
                SET status = 'SUSPENDED',
                    needs_reinit = true,
                    refresh_reason = 'CDC_BUFFER_CORRUPT',
                    refresh_reason_detail = 'A CDC buffer relation has an invalid schema.',
                    updated_at = now()
              WHERE EXISTS (
                  SELECT 1
                    FROM pgtrickle.pgt_dependencies dep
                    JOIN pgtrickle.pgt_change_buffers cb
                      ON cb.source_kind = 'BASE'
                     AND cb.source_id = dep.source_relid::bigint
                   WHERE dep.pgt_id = st.pgt_id
                     AND to_regclass(format(
                             '%I.%I',
                             current_setting('pg_trickle.change_buffer_schema'),
                             cb.buffer_key
                         )) IS NOT NULL
                     AND NOT (
                         EXISTS (
                             SELECT 1
                               FROM information_schema.columns col
                              WHERE col.table_schema = current_setting('pg_trickle.change_buffer_schema')
                                AND col.table_name = cb.buffer_key
                                AND col.column_name = 'lsn'
                         )
                         AND EXISTS (
                             SELECT 1
                               FROM information_schema.columns col
                              WHERE col.table_schema = current_setting('pg_trickle.change_buffer_schema')
                                AND col.table_name = cb.buffer_key
                                AND col.column_name = 'action'
                         )
                         AND EXISTS (
                             SELECT 1
                               FROM information_schema.columns col
                              WHERE col.table_schema = current_setting('pg_trickle.change_buffer_schema')
                                AND col.table_name = cb.buffer_key
                                AND col.column_name = '__pgt_row_id'
                         )
                     )
              )",
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    checks.push(check(
        "capture_buffer_schema",
        if corrupt_buffers == 0 {
            "SAFE"
        } else {
            CaptureFailure::CorruptBuffer.class().as_str()
        },
        format!(
            "CDC_BUFFER_CORRUPT: {corrupt_buffers} change-buffer relation(s) have an invalid schema"
        ),
    ));

    let missing_slots = count_query(
        "SELECT count(*)::bigint
           FROM pgtrickle.pgt_dependencies dep
          WHERE dep.cdc_mode IN ('WAL', 'TRANSITIONING')
            AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_replication_slots slot
                 WHERE slot.slot_name = dep.slot_name
                   AND slot.database = current_database()
            )",
    )?;
    if missing_slots > 0 {
        Spi::run(
            "UPDATE pgtrickle.pgt_stream_tables st
                SET status = 'SUSPENDED',
                    needs_reinit = true,
                    refresh_reason = 'CDC_SLOT_MISSING',
                    refresh_reason_detail = 'A required WAL capture slot is missing.',
                    updated_at = now()
              WHERE EXISTS (
                  SELECT 1
                    FROM pgtrickle.pgt_dependencies dep
                   WHERE dep.pgt_id = st.pgt_id
                     AND dep.cdc_mode IN ('WAL', 'TRANSITIONING')
                     AND NOT EXISTS (
                         SELECT 1
                           FROM pg_catalog.pg_replication_slots slot
                          WHERE slot.slot_name = dep.slot_name
                            AND slot.database = current_database()
                     )
              )",
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    checks.push(check(
        "capture_slots",
        if missing_slots == 0 {
            "SAFE"
        } else {
            RecoveryClass::ReinitializationRequired.as_str()
        },
        format!("CDC_SLOT_MISSING: {missing_slots} WAL capture slot(s) are missing"),
    ));

    let unavailable_wal = count_query(
        "SELECT count(*)::bigint
           FROM pgtrickle.pgt_dependencies dep
           JOIN pg_catalog.pg_replication_slots slot
             ON slot.slot_name = dep.slot_name
            AND slot.database = current_database()
          WHERE dep.cdc_mode IN ('WAL', 'TRANSITIONING')
            AND slot.wal_status = 'lost'",
    )?;
    if unavailable_wal > 0 {
        Spi::run(
            "UPDATE pgtrickle.pgt_stream_tables st
                SET status = 'SUSPENDED',
                    needs_reinit = true,
                    refresh_reason = 'CDC_WAL_UNAVAILABLE',
                    refresh_reason_detail = 'Required WAL history is no longer available.',
                    updated_at = now()
              WHERE EXISTS (
                  SELECT 1
                    FROM pgtrickle.pgt_dependencies dep
                    JOIN pg_catalog.pg_replication_slots slot
                      ON slot.slot_name = dep.slot_name
                     AND slot.database = current_database()
                   WHERE dep.pgt_id = st.pgt_id
                     AND dep.cdc_mode IN ('WAL', 'TRANSITIONING')
                     AND slot.wal_status = 'lost'
              )",
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    checks.push(check(
        "capture_wal",
        if unavailable_wal == 0 {
            "SAFE"
        } else {
            CaptureFailure::UnavailableWal.class().as_str()
        },
        format!(
            "CDC_WAL_UNAVAILABLE: {unavailable_wal} WAL capture slot(s) no longer \
             retain required history"
        ),
    ));

    let current_lsn = Spi::get_one::<String>("SELECT pg_current_wal_lsn()::text")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::InternalError("pg_current_wal_lsn returned NULL".into()))?;
    let frontier_rows = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT pgt_id, frontier::text
                   FROM pgtrickle.pgt_stream_tables
                  WHERE frontier IS NOT NULL",
                None,
                &[],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut ids = Vec::new();
        for row in rows {
            let pgt_id = row
                .get::<i64>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("frontier pgt_id is NULL".into()))?;
            let json = row
                .get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("frontier JSON is NULL".into()))?;
            let frontier = crate::version::Frontier::from_json(&json).map_err(|e| {
                PgTrickleError::InvalidArgument(format!(
                    "frontier for pgt_id={pgt_id} is invalid: {e}"
                ))
            })?;
            if frontier
                .sources
                .values()
                .any(|source| crate::version::lsn_gt(&source.lsn, &current_lsn))
            {
                ids.push(pgt_id);
            }
        }
        Ok::<_, PgTrickleError>(ids)
    })?;
    for pgt_id in &frontier_rows {
        Spi::run_with_args(
            "UPDATE pgtrickle.pgt_stream_tables
                SET status = 'SUSPENDED',
                    needs_reinit = true,
                    refresh_reason = 'RECOVERY_FRONTIER_UNPROVEN',
                    refresh_reason_detail = 'The restored WAL position is behind the persisted frontier; \
                                              a FULL rebuild is required before capture can resume.',
                    updated_at = now()
              WHERE pgt_id = $1",
            &[(*pgt_id).into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    checks.push(check(
        "frontiers",
        if frontier_rows.is_empty() {
            "SAFE"
        } else {
            RecoveryClass::ReinitializationRequired.as_str()
        },
        format!(
            "RECOVERY_FRONTIER_UNPROVEN: {} persisted frontier(s) are ahead of \
             recoverable WAL position {}",
            frontier_rows.len(),
            current_lsn
        ),
    ));

    let status = if gate == CaptureGate::Quarantined {
        "OPERATOR_INTERVENTION_REQUIRED"
    } else if checks.iter().any(|item| {
        matches!(
            item.get("status").and_then(serde_json::Value::as_str),
            Some("REINITIALIZATION_REQUIRED")
        )
    }) {
        "REINITIALIZATION_REQUIRED"
    } else {
        "SAFE"
    };
    Ok(serde_json::json!({
        "ok": status == "SAFE",
        "status": status,
        "checks": checks,
    }))
}

/// Validate capture ownership, source infrastructure, and persisted frontiers.
#[pg_extern(schema = "pgtrickle")]
pub fn validate_recovery() -> String {
    match validate_recovery_impl() {
        Ok(report) => serde_json::to_string(&report).unwrap_or_else(|_| {
            "{\"ok\":false,\"status\":\"BLOCKER\",\"detail\":\"recovery report serialization failed\"}"
                .to_string()
        }),
        Err(error) => super::raise_error_with_context(error),
    }
}

/// Return the current capture-instance identity and quarantine state.
#[pg_extern(schema = "pgtrickle")]
pub fn capture_instance_status() -> String {
    let state = match load_instance_state() {
        Ok(Some(state)) => state,
        Ok(None) => {
            return "{\"state\":\"UNINITIALIZED\"}".to_string();
        }
        Err(error) => super::raise_error_with_context(error),
    };
    serde_json::to_string(&serde_json::json!({
        "instance_id": state.instance_id,
        "database_oid": state.database_oid,
        "system_identifier": state.system_identifier,
        "state": state.state,
        "observed_database_oid": state.observed_database_oid,
        "observed_system_identifier": state.observed_system_identifier,
    }))
    .unwrap_or_else(|_| "{\"state\":\"BLOCKED\"}".to_string())
}

/// Quiesce capture and refresh dispatch before a PostgreSQL or extension upgrade.
#[pg_extern(schema = "pgtrickle")]
pub fn quiesce(timeout_s: default!(i32, 60)) -> bool {
    let timeout = match validation::timeout_seconds("quiesce timeout", timeout_s) {
        Ok(value) => value,
        Err(error) => super::raise_error_with_context(error),
    };
    if let Err(error) = assert_capture_ready() {
        super::raise_error_with_context(error);
    }
    let epoch = match crate::shmem::signal_drain() {
        Some(epoch) => epoch,
        None => return false,
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout);
    loop {
        if crate::shmem::check_drain_completed(epoch) {
            if let Err(error) = Spi::run(
                "UPDATE pgtrickle.pgt_capture_instance
                    SET state = 'QUIESCED', last_seen_at = now()
                  WHERE singleton",
            ) {
                super::raise_error_with_context(PgTrickleError::SpiError(error.to_string()));
            }
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        pgrx::check_for_interrupts!();
        // SAFETY: pg_usleep is PostgreSQL's bounded backend sleep helper.
        unsafe { pg_sys::pg_usleep(100_000) };
    }
}

/// Resume all capture and refresh dispatch after a completed upgrade.
#[pg_extern(schema = "pgtrickle")]
pub fn resume_all() -> bool {
    let gate = match ensure_capture_instance() {
        Ok(gate) => gate,
        Err(error) => super::raise_error_with_context(error),
    };
    if gate == CaptureGate::Quarantined {
        super::raise_error_with_context(PgTrickleError::InvalidArgument(
            "capture instance is quarantined; recover_capture_instance() is required".into(),
        ));
    }
    let state = match load_instance_state() {
        Ok(Some(state)) => state,
        Ok(None) => return false,
        Err(error) => super::raise_error_with_context(error),
    };
    if state.state == STATE_QUARANTINED {
        super::raise_error_with_context(PgTrickleError::InvalidArgument(
            "capture instance is quarantined; recover_capture_instance() is required".into(),
        ));
    }
    let changed = crate::shmem::resume_after_drain();
    if let Err(error) = Spi::run(
        "UPDATE pgtrickle.pgt_capture_instance
            SET state = 'ACTIVE', last_seen_at = now()
          WHERE singleton",
    ) {
        super::raise_error_with_context(PgTrickleError::SpiError(error.to_string()));
    }
    changed
}

/// Backward-compatible alias for the upgrade boundary.
#[pg_extern(schema = "pgtrickle")]
pub fn pause_all() -> bool {
    quiesce(60)
}

/// Adopt the current database as a new capture owner after an explicit clone recovery.
#[pg_extern(schema = "pgtrickle", security_definer)]
#[search_path(pgtrickle, pg_catalog, pg_temp)]
pub fn recover_capture_instance() -> String {
    let is_superuser = match Spi::get_one::<bool>(
        "SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user",
    ) {
        Ok(Some(value)) => value,
        Ok(None) => super::raise_error_with_context(PgTrickleError::NotFound(
            "current database role".to_string(),
        )),
        Err(error) => super::raise_error_with_context(PgTrickleError::SpiError(error.to_string())),
    };
    if !is_superuser {
        super::raise_error_with_context(PgTrickleError::PermissionDenied(
            "recover_capture_instance() requires a superuser".into(),
        ));
    }
    let database_oid = match current_database_oid() {
        Ok(value) => value,
        Err(error) => super::raise_error_with_context(error),
    };
    let system_identifier = match current_system_identifier() {
        Ok(value) => value,
        Err(error) => super::raise_error_with_context(error),
    };
    let instance_id = match new_instance_id() {
        Ok(value) => value,
        Err(error) => super::raise_error_with_context(error),
    };
    if let Err(error) = Spi::run_with_args(
        "UPDATE pgtrickle.pgt_capture_instance
            SET instance_id = $1,
                database_oid = $2,
                system_identifier = $3,
                state = 'ACTIVE',
                observed_database_oid = NULL,
                observed_system_identifier = NULL,
                quarantine_reason = NULL,
                last_seen_at = now()
          WHERE singleton",
        &[
            instance_id.as_str().into(),
            database_oid.into(),
            system_identifier.as_str().into(),
        ],
    ) {
        super::raise_error_with_context(PgTrickleError::SpiError(error.to_string()));
    }
    if let Err(error) = Spi::run(
        "UPDATE pgtrickle.pgt_stream_tables
            SET status = 'SUSPENDED',
                needs_reinit = true,
                frontier = NULL,
                is_populated = false,
                refresh_reason = 'CDC_CLONE_RECOVERY_REQUIRED',
                refresh_reason_detail = 'Capture ownership was explicitly adopted; repair each stream table before resuming.',
                updated_at = now()",
    ) {
        super::raise_error_with_context(PgTrickleError::SpiError(error.to_string()));
    }
    crate::shmem::resume_after_drain();
    format!(
        "capture ownership adopted for database_oid={} system_identifier={}; \
         all stream tables require protected reinitialization",
        database_oid, system_identifier
    )
}

fn preflight_upgrade_impl() -> Result<serde_json::Value, PgTrickleError> {
    let mut checks = Vec::new();
    let instance = ensure_capture_instance()?;
    checks.push(check(
        "capture_instance",
        if instance == CaptureGate::Quarantined {
            "BLOCKER"
        } else {
            "SAFE"
        },
        if instance == CaptureGate::Quarantined {
            "capture ownership is quarantined after a restore or clone".to_string()
        } else {
            "capture ownership identity is valid".to_string()
        },
    ));

    let suspended = count_query(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_stream_tables
          WHERE status IN ('SUSPENDED', 'ERROR') OR needs_reinit",
    )?;
    checks.push(check(
        "suspended_tables",
        if suspended == 0 { "SAFE" } else { "BLOCKER" },
        format!("{suspended} stream table(s) are suspended, errored, or awaiting rebuild"),
    ));

    let in_flight = count_query(
        "SELECT count(*)::bigint FROM pgtrickle.pgt_scheduler_jobs
          WHERE status IN ('QUEUED', 'RUNNING')",
    )?;
    checks.push(check(
        "in_flight_refreshes",
        if in_flight == 0 {
            "SAFE"
        } else {
            "DRAIN_RECOMMENDED"
        },
        format!("{in_flight} scheduler job(s) are queued or running"),
    ));

    let backlog = count_query(
        "SELECT COALESCE(sum(n_live_tup), 0)::bigint
           FROM pg_stat_all_tables
          WHERE schemaname = current_setting('pg_trickle.change_buffer_schema', true)",
    )?;
    checks.push(check(
        "capture_backlog",
        if backlog == 0 {
            "SAFE"
        } else {
            "DRAIN_RECOMMENDED"
        },
        format!("{backlog} estimated change-buffer row(s) remain"),
    ));

    let invalid_catalog = count_query(
        "SELECT count(*)::bigint
           FROM pgtrickle.pgt_stream_tables
          WHERE pgt_relid = 0 OR pgt_name = '' OR pgt_schema = ''",
    )?;
    checks.push(check(
        "catalog_integrity",
        if invalid_catalog == 0 {
            "SAFE"
        } else {
            "BLOCKER"
        },
        format!("{invalid_catalog} invalid stream-table catalog row(s) found"),
    ));

    let version_mismatch = Spi::get_one_with_args::<bool>(
        "SELECT extversion::text <> $1
              OR NOT EXISTS (
                    SELECT 1
                      FROM pgtrickle.pgt_schema_version
                     WHERE version = $1
              )
           FROM pg_catalog.pg_extension
          WHERE extname = 'pg_trickle'",
        &[env!("CARGO_PKG_VERSION").into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| PgTrickleError::NotFound("pg_trickle extension".to_string()))?;
    checks.push(check(
        "persistent_state_versions",
        if version_mismatch { "WARNING" } else { "SAFE" },
        if version_mismatch {
            "the extension version and server version require review before upgrade".to_string()
        } else {
            "persistent catalog state is readable".to_string()
        },
    ));

    checks.push(check(
        "rebuild_disk_headroom",
        "WARNING",
        "verify free filesystem space externally; protected rebuilds may temporarily require a second result table".to_string(),
    ));

    let status = overall_status(&checks);
    Ok(serde_json::json!({
        "status": status,
        "safe": status == "SAFE",
        "checks": checks,
    }))
}

/// Check whether an upgrade can proceed and return stable machine-readable statuses.
#[pg_extern(schema = "pgtrickle")]
pub fn preflight_upgrade() -> String {
    match preflight_upgrade_impl() {
        Ok(report) => serde_json::to_string(&report).unwrap_or_else(|_| {
            "{\"status\":\"BLOCKER\",\"safe\":false,\"detail\":\"upgrade preflight serialization failed\"}"
                .to_string()
        }),
        Err(error) => super::raise_error_with_context(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_classes_fail_closed() {
        assert_eq!(
            CaptureFailure::MissingTrigger.class(),
            RecoveryClass::ReinitializationRequired
        );
        assert_eq!(
            CaptureFailure::CloneDetected.class(),
            RecoveryClass::OperatorInterventionRequired
        );
        assert_eq!(
            CaptureFailure::MissingTrigger.reason_code(),
            "CDC_TRIGGER_MISSING"
        );
    }

    #[test]
    fn status_order_is_stable() {
        assert_eq!(status_rank("BLOCKER"), 4);
        assert_eq!(status_rank("DRAIN_RECOMMENDED"), 2);
        assert_eq!(status_rank("SAFE"), 0);
    }
}
