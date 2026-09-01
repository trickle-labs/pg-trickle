//! NOTIFY alert types and helpers (v0.55.0 decomposition).
// Extracted from src/monitor.rs in v0.55.0 module decomposition.
// All shared helpers and types are in monitor/mod.rs (use super::*).

use super::*;

const MAX_ALERT_BYTES: usize = 7_900;

// ── NOTIFY Alerting ────────────────────────────────────────────────────────

/// Alert event types emitted on the `pg_trickle_alert` NOTIFY channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AlertEvent {
    /// data staleness exceeds 2× schedule.
    StaleData,
    /// ST suspended after consecutive errors.
    AutoSuspended,
    /// ST resumed after suspension.
    Resumed,
    /// Upstream DDL change requires reinitialize.
    ReinitializeNeeded,
    /// Trigger-mode change buffers are growing.
    BufferGrowthWarning,
    /// WAL replication slot retention is growing.
    SlotLagWarning,
    /// Refresh completed successfully.
    RefreshCompleted,
    /// Refresh failed.
    RefreshFailed,
    /// EC-11: Refresh duration is approaching the schedule interval.
    SchedulerFallingBehind,
    /// NS-2: append_only flag automatically reverted after DELETE/UPDATE detected.
    AppendOnlyReverted,
    /// FUSE: Periodic reminder that a fuse is still blown.
    FuseBlownReminder,
    /// Fuse circuit breaker was blown.
    FuseBlown,
    /// G-7: Stream table skipped because its tier is Frozen.
    FrozenTierSkip,
    /// CDC trigger missing or disabled on a source table.
    CdcTriggerDisabled,
    /// Change buffer cleanup persistently failing for a source OID.
    CleanupFailure,
    /// Scheduler is running normally but no upstream source rows have changed
    /// — data_timestamp is frozen because there is genuinely nothing new.
    NoUpstreamChanges,
    /// STAB-3 / PH-E2: Delta MERGE spilled to temp files, exceeding the
    /// configured `pg_trickle.spill_threshold_blocks` for consecutive refreshes.
    SpillThresholdExceeded,
    /// PLAN-3 (v0.27.0): Cost model predicts next refresh will exceed the
    /// ST's `freshness_deadline_ms` SLA by > 20%.
    PredictedSlaBreach,
    /// SCAL-1 (v0.31.0): Change buffer has exceeded `buffer_alert_threshold`
    /// for N consecutive refresh cycles — back-pressure is building.
    ChangeBufferBackpressure,
    /// A source watermark has stopped advancing.
    WatermarkStuck,
    /// A previously stuck source watermark resumed.
    WatermarkResumed,
    /// Self-monitoring detected an anomaly.
    SelfMonitorAnomaly,
    /// An exact freshness target entered sustained breach.
    FreshnessBreach,
    /// An exact freshness target recovered from breach or infeasibility.
    FreshnessRecovered,
    /// The fastest compatible refresh cost cannot meet the target.
    FreshnessInfeasible,
}

impl AlertEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertEvent::StaleData => "stale_data",
            AlertEvent::AutoSuspended => "auto_suspended",
            AlertEvent::Resumed => "resumed",
            AlertEvent::ReinitializeNeeded => "reinitialize_needed",
            AlertEvent::BufferGrowthWarning => "buffer_growth_warning",
            AlertEvent::SlotLagWarning => "slot_lag_warning",
            AlertEvent::RefreshCompleted => "refresh_completed",
            AlertEvent::RefreshFailed => "refresh_failed",
            AlertEvent::SchedulerFallingBehind => "scheduler_falling_behind",
            AlertEvent::AppendOnlyReverted => "append_only_reverted",
            AlertEvent::FuseBlownReminder => "fuse_blown_reminder",
            AlertEvent::FuseBlown => "fuse_blown",
            AlertEvent::FrozenTierSkip => "frozen_tier_skip",
            AlertEvent::CdcTriggerDisabled => "cdc_trigger_disabled",
            AlertEvent::CleanupFailure => "cleanup_failure",
            AlertEvent::NoUpstreamChanges => "no_upstream_changes",
            AlertEvent::SpillThresholdExceeded => "spill_threshold_exceeded",
            AlertEvent::PredictedSlaBreach => "predicted_sla_breach",
            AlertEvent::ChangeBufferBackpressure => "change_buffer_backpressure",
            AlertEvent::WatermarkStuck => "watermark_stuck",
            AlertEvent::WatermarkResumed => "watermark_resumed",
            AlertEvent::SelfMonitorAnomaly => "self_monitor_anomaly",
            AlertEvent::FreshnessBreach => "freshness_breach",
            AlertEvent::FreshnessRecovered => "freshness_recovered",
            AlertEvent::FreshnessInfeasible => "freshness_infeasible",
        }
    }
}

/// Emit a bounded freshness status transition notification.
pub fn alert_freshness_transition(
    event: AlertEvent,
    pgt_schema: &str,
    pgt_name: &str,
    target_ms: i64,
    p95_ms: Option<f64>,
    reason: &str,
) {
    let _ = emit_alert(
        event,
        pgt_schema,
        pgt_name,
        json!({
            "target_ms": target_ms,
            "p95_ms": p95_ms,
            "reason": reason,
        }),
        false,
    );
}

/// Emit a NOTIFY on the `pg_trickle_alert` channel with a JSON payload.
///
/// The payload is a JSON object with at minimum an `event` field.
/// Callers can add arbitrary key-value pairs for context.
///
/// PB2: When `skip_notify` is true, the NOTIFY is silently suppressed
/// (for stream tables in pooler compatibility mode).
pub fn emit_alert(
    event: AlertEvent,
    pgt_schema: &str,
    pgt_name: &str,
    extra: serde_json::Value,
    skip_notify: bool,
) -> Result<(), String> {
    if skip_notify {
        return Ok(());
    }
    let payload = build_alert_payload(event, pgt_schema, pgt_name, extra);
    let result = emit_alert_payload(&payload);
    if let Err(ref error) = result {
        pgrx::warning!(
            "pg_trickle: failed to emit alert {}: {}",
            event.as_str(),
            error
        );
    }
    result
}

pub fn emit_alert_without_stream(
    event: AlertEvent,
    extra: serde_json::Value,
) -> Result<(), String> {
    let result = emit_alert_payload(&build_generic_alert_payload(event, extra));
    if let Err(ref error) = result {
        pgrx::warning!(
            "pg_trickle: failed to emit alert {}: {}",
            event.as_str(),
            error
        );
    }
    result
}

fn emit_alert_payload(payload: &str) -> Result<(), String> {
    Spi::run_with_args(
        "SELECT pg_notify('pg_trickle_alert', $1)",
        &[payload.into()],
    )
    .map_err(|e| e.to_string())
}

fn database_identity() -> u32 {
    // SAFETY: MyDatabaseId is initialized for every connected PostgreSQL backend.
    unsafe { pg_sys::MyDatabaseId }.to_u32()
}

/// Build a JSON alert payload for a NOTIFY event.
///
/// Uses `serde_json` to produce a well-formed JSON object regardless of
/// what characters appear in field values.  The payload is truncated to
/// 7,900 bytes if it exceeds PostgreSQL's NOTIFY payload limit (~8,000 bytes).
pub(crate) fn build_alert_payload(
    event: AlertEvent,
    pgt_schema: &str,
    pgt_name: &str,
    extra: serde_json::Value,
) -> String {
    let mut obj = json!({
        "event": event.as_str(),
        "database_oid": database_identity(),
        "pgt_schema": pgt_schema,
        "pgt_name": pgt_name,
        "st": format!("{}.{}", pgt_schema, pgt_name),
    });
    if let (serde_json::Value::Object(base), serde_json::Value::Object(ext)) = (&mut obj, extra) {
        for (key, value) in ext {
            if !matches!(
                key.as_str(),
                "event" | "database_oid" | "pgt_schema" | "pgt_name" | "st"
            ) {
                base.insert(key, value);
            }
        }
    }
    reduce_alert_payload(obj)
}

fn build_generic_alert_payload(event: AlertEvent, extra: serde_json::Value) -> String {
    let mut obj = serde_json::Map::new();
    obj.insert("event".to_string(), json!(event.as_str()));
    obj.insert("database_oid".to_string(), json!(database_identity()));
    if let serde_json::Value::Object(ext) = extra {
        for (key, value) in ext {
            if !matches!(key.as_str(), "event" | "database_oid") {
                obj.insert(key, value);
            }
        }
    }
    reduce_alert_payload(serde_json::Value::Object(obj))
}

fn reduce_alert_payload(mut value: serde_json::Value) -> String {
    let oversized = value.to_string().len() > MAX_ALERT_BYTES;
    if !oversized {
        return value.to_string();
    }
    if let serde_json::Value::Object(ref mut object) = value {
        object.insert("truncated".to_string(), json!(true));
        shrink_strings(object, 512);
        let mut optional: Vec<String> = object
            .keys()
            .filter(|key| {
                !matches!(
                    key.as_str(),
                    "event" | "database_oid" | "pgt_schema" | "pgt_name" | "st" | "truncated"
                )
            })
            .cloned()
            .collect();
        optional.sort();
        let mut omitted = Vec::new();
        for key in optional {
            if object_serialized_len(object) <= MAX_ALERT_BYTES {
                break;
            }
            object.remove(&key);
            omitted.push(key.chars().take(64).collect::<String>());
        }
        if !omitted.is_empty() {
            object.insert("omitted_fields".to_string(), json!(omitted));
        }
        for limit in [128, 32, 8, 1] {
            if object_serialized_len(object) <= MAX_ALERT_BYTES {
                break;
            }
            shrink_strings(object, limit);
        }
        if object_serialized_len(object) > MAX_ALERT_BYTES {
            object.insert("omitted_fields".to_string(), json!(["additional_fields"]));
            shrink_strings(object, 1);
        }
    }
    let payload = value.to_string();
    if payload.len() <= MAX_ALERT_BYTES {
        return payload;
    }
    let mut minimal = serde_json::Map::new();
    if let serde_json::Value::Object(object) = value {
        for key in ["event", "database_oid", "pgt_schema", "pgt_name", "st"] {
            if let Some(value) = object.get(key) {
                minimal.insert(key.to_string(), value.clone());
            }
        }
    }
    minimal.insert("truncated".to_string(), json!(true));
    let payload = serde_json::Value::Object(minimal).to_string();
    debug_assert!(payload.len() <= MAX_ALERT_BYTES);
    payload
}

fn object_serialized_len(object: &serde_json::Map<String, serde_json::Value>) -> usize {
    serde_json::Value::Object(object.clone()).to_string().len()
}

fn shrink_strings(value: &mut serde_json::Map<String, serde_json::Value>, max_chars: usize) {
    fn visit(value: &mut serde_json::Value, max_chars: usize) {
        match value {
            serde_json::Value::String(text) => {
                if text.chars().count() > max_chars {
                    let end = text
                        .char_indices()
                        .nth(max_chars)
                        .map(|(index, _)| index)
                        .unwrap_or(text.len());
                    text.truncate(end);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    visit(value, max_chars);
                }
            }
            serde_json::Value::Object(values) => {
                for value in values.values_mut() {
                    visit(value, max_chars);
                }
            }
            _ => {}
        }
    }
    for value in value.values_mut() {
        visit(value, max_chars);
    }
}

/// Emit a stale-data alert.
pub fn alert_stale_data(
    pgt_schema: &str,
    pgt_name: &str,
    staleness_secs: f64,
    schedule_secs: f64,
    skip_notify: bool,
) {
    let ratio = if schedule_secs > 0.0 {
        staleness_secs / schedule_secs
    } else {
        0.0
    };
    let _ = emit_alert(
        AlertEvent::StaleData,
        pgt_schema,
        pgt_name,
        json!({
            "staleness_seconds": (staleness_secs * 10.0).round() / 10.0,
            "schedule_seconds": (schedule_secs * 10.0).round() / 10.0,
            "ratio": (ratio * 100.0).round() / 100.0,
        }),
        skip_notify,
    );
}

/// Emit a no-upstream-changes informational event.
///
/// Fired when the scheduler is healthy (last_refresh_at is recent) but
/// data_timestamp has not advanced because no source rows changed.  This
/// is distinct from a genuine staleness problem.
pub fn alert_no_upstream_changes(
    pgt_schema: &str,
    pgt_name: &str,
    idle_secs: f64,
    skip_notify: bool,
) {
    let _ = emit_alert(
        AlertEvent::NoUpstreamChanges,
        pgt_schema,
        pgt_name,
        json!({ "idle_seconds": (idle_secs * 10.0).round() / 10.0 }),
        skip_notify,
    );
}

/// Emit an auto-suspended alert.
pub fn alert_auto_suspended(pgt_schema: &str, pgt_name: &str, error_count: i32, skip_notify: bool) {
    let _ = emit_alert(
        AlertEvent::AutoSuspended,
        pgt_schema,
        pgt_name,
        json!({ "consecutive_errors": error_count }),
        skip_notify,
    );
}

/// Emit a resumed alert (ST cleared from SUSPENDED back to ACTIVE).
pub fn alert_resumed(pgt_schema: &str, pgt_name: &str, skip_notify: bool) {
    let _ = emit_alert(
        AlertEvent::Resumed,
        pgt_schema,
        pgt_name,
        json!({ "previous_status": "SUSPENDED" }),
        skip_notify,
    );
}

/// Emit a reinitialize-needed alert.
pub fn alert_reinitialize_needed(
    pgt_schema: &str,
    pgt_name: &str,
    reason: &str,
    skip_notify: bool,
) {
    let _ = emit_alert(
        AlertEvent::ReinitializeNeeded,
        pgt_schema,
        pgt_name,
        json!({ "reason": reason }),
        skip_notify,
    );
}

/// Emit a buffer growth warning.
pub fn alert_buffer_growth(slot_name: &str, pending_bytes: i64) {
    let _ = emit_alert_without_stream(
        AlertEvent::BufferGrowthWarning,
        json!({
        "slot_name": slot_name,
        "pending_bytes": pending_bytes,
        }),
    );
}

/// Emit a WAL slot lag warning.
pub fn alert_slot_lag(slot_name: &str, retained_wal_bytes: i64, threshold_bytes: i64) {
    let _ = emit_alert_without_stream(
        AlertEvent::SlotLagWarning,
        json!({
        "slot_name": slot_name,
        "retained_wal_bytes": retained_wal_bytes,
        "threshold_bytes": threshold_bytes,
        }),
    );
}

/// SCAL-1 (v0.31.0): Emit a change-buffer back-pressure alert.
///
/// Fired when a source table's change buffer has exceeded
/// `pg_trickle.buffer_alert_threshold` for N consecutive refresh cycles,
/// indicating that consumers are not keeping up with producers.
pub fn alert_change_buffer_backpressure(
    source_oid: u32,
    pending_rows: i64,
    consecutive_cycles: i32,
    threshold: i64,
) {
    let _ = emit_alert_without_stream(
        AlertEvent::ChangeBufferBackpressure,
        json!({
        "source_oid": source_oid,
        "pending_rows": pending_rows,
        "consecutive_cycles": consecutive_cycles,
        "threshold": threshold,
        }),
    );
}

pub(crate) fn collect_slot_health_rows() -> Vec<(String, i64, bool, i64, String)> {
    let mut rows = Vec::new();

    let trigger_rows: Vec<_> = Spi::connect(|client| {
        let result = client
            .select(
                "SELECT
                    ct.slot_name,
                    ct.source_relid::bigint
                FROM pgtrickle.pgt_change_tracking ct",
                None,
                &[],
            )
            .unwrap_or_else(|e| {
                pgrx::error!(
                    "{}",
                    crate::error::PgTrickleError::DiagnosticError(format!(
                        "slot_health: SPI select failed: {e}"
                    ))
                )
            });

        let mut out = Vec::new();
        for row in result {
            let slot = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
            let relid = row.get::<i64>(2).unwrap_or(None).unwrap_or(0);
            out.push((slot, relid));
        }
        out
    });

    let all_deps = StDependency::get_all().unwrap_or_default();
    let mut wal_sources = std::collections::HashMap::new();
    for dep in &all_deps {
        if matches!(dep.cdc_mode, CdcMode::Wal | CdcMode::Transitioning) {
            wal_sources
                .entry(dep.source_relid.to_u32())
                .or_insert((dep.cdc_mode, dep.slot_name.clone()));
        }
    }

    for (slot, relid) in trigger_rows {
        let source_oid_u32 = relid as u32;
        if let Some((mode, _)) = wal_sources.remove(&source_oid_u32) {
            let slot_name = wal_decoder::slot_name_for_source(pg_sys::Oid::from(source_oid_u32));
            let lag = wal_decoder::get_slot_lag_bytes(&slot_name).unwrap_or(0);
            rows.push((slot_name, relid, true, lag, mode.as_str().to_lowercase()));
        } else {
            rows.push((slot, relid, true, 0, "trigger".to_string()));
        }
    }

    for (oid_u32, (mode, slot_opt)) in wal_sources {
        let slot_name = slot_opt
            .unwrap_or_else(|| wal_decoder::slot_name_for_source(pg_sys::Oid::from(oid_u32)));
        let lag = wal_decoder::get_slot_lag_bytes(&slot_name).unwrap_or(0);
        rows.push((
            slot_name,
            oid_u32 as i64,
            true,
            lag,
            mode.as_str().to_lowercase(),
        ));
    }

    rows
}

pub(crate) fn build_cdc_health_alert(
    lag_bytes: i64,
    threshold_bytes: i64,
    slot_exists: bool,
    cdc_mode: CdcMode,
) -> Option<String> {
    if lag_bytes > threshold_bytes {
        Some(format!(
            "slot_lag_exceeds_threshold: {} bytes > {} bytes",
            lag_bytes, threshold_bytes
        ))
    } else if !slot_exists && cdc_mode == CdcMode::Wal {
        Some("replication_slot_missing".to_string())
    } else {
        None
    }
}

pub(crate) fn build_slot_lag_health_detail(
    lagging: &[String],
    threshold_bytes: i64,
) -> (String, String) {
    if lagging.is_empty() {
        (
            "OK".to_string(),
            "All WAL replication slots within normal range".to_string(),
        )
    } else {
        (
            "WARN".to_string(),
            format!(
                "{} WAL slot(s) retaining more than {} bytes: {}",
                lagging.len(),
                threshold_bytes,
                lagging.join(", ")
            ),
        )
    }
}

/// Emit a refresh-completed alert.
pub fn alert_refresh_completed(
    pgt_schema: &str,
    pgt_name: &str,
    action: &str,
    rows_inserted: i64,
    rows_deleted: i64,
    duration_ms: i64,
    skip_notify: bool,
) {
    let _ = emit_alert(
        AlertEvent::RefreshCompleted,
        pgt_schema,
        pgt_name,
        json!({
            "action": action,
            "rows_inserted": rows_inserted,
            "rows_deleted": rows_deleted,
            "duration_ms": duration_ms,
        }),
        skip_notify,
    );
}

/// Emit a refresh-failed alert.
pub fn alert_refresh_failed(
    pgt_schema: &str,
    pgt_name: &str,
    action: &str,
    error: &str,
    skip_notify: bool,
) {
    let _ = emit_alert(
        AlertEvent::RefreshFailed,
        pgt_schema,
        pgt_name,
        json!({
            "action": action,
            "error": error,
        }),
        skip_notify,
    );
}

/// EC-11: Emit a scheduler-falling-behind alert when refresh duration
/// exceeds 80% of the schedule interval.
pub fn alert_falling_behind(
    pgt_schema: &str,
    pgt_name: &str,
    elapsed_ms: i64,
    schedule_ms: i64,
    ratio: f64,
    skip_notify: bool,
) {
    let _ = emit_alert(
        AlertEvent::SchedulerFallingBehind,
        pgt_schema,
        pgt_name,
        json!({
            "elapsed_ms": elapsed_ms,
            "schedule_ms": schedule_ms,
            "ratio": (ratio * 100.0).round() / 100.0,
        }),
        skip_notify,
    );
}

/// STAB-3 / PH-E2: Emit a spill-threshold-exceeded alert when a refresh
/// spills more than `pg_trickle.spill_threshold_blocks` temp blocks to disk
/// for more than `consecutive` consecutive cycles.
pub fn alert_spill_threshold_exceeded(
    pgt_schema: &str,
    pgt_name: &str,
    temp_blks_written: i64,
    threshold_blocks: i64,
    consecutive: i32,
    limit: i32,
    skip_notify: bool,
) {
    let _ = emit_alert(
        AlertEvent::SpillThresholdExceeded,
        pgt_schema,
        pgt_name,
        json!({
            "temp_blks_written": temp_blks_written,
            "threshold_blocks": threshold_blocks,
            "consecutive": consecutive,
            "limit": limit,
        }),
        skip_notify,
    );
}
