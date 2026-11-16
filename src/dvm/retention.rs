//! Conservative query-side candidates for partition-local aggregate groups.
//!
//! A candidate is not sufficient to authorize retention: callers must also
//! validate the partition key, every bound, and the retention lifecycle.

use super::parser::{AggFunc, Expr, OpTree};
use serde::{Deserialize, Serialize};

const DATE_OID: u32 = 1082;
const TIMESTAMP_OID: u32 = 1114;
const TIMESTAMPTZ_OID: u32 = 1184;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RetentionBucket {
    Exact,
    HourTimestamp,
    HourUtcTimestamptz,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionGroupCandidate {
    pub output_column: String,
    pub bucket: RetentionBucket,
    pub source_type_oid: u32,
    pub source_oid: u32,
    pub source_column: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionQueryError {
    UnsupportedShape,
    UnsupportedAggregate,
    InvalidSourceKey,
    MissingProjectedGroupKey,
}

/// PostgreSQL's internal date days or timestamp microseconds since its epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RetentionBound {
    Date(i32),
    Timestamp(i64),
    Timestamptz(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RetentionMode {
    Trim,
    Retain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedRange {
    pub lower: RetentionBound,
    pub upper: RetentionBound,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionState {
    pub version: u8,
    pub mode: RetentionMode,
    pub candidate: RetentionGroupCandidate,
    pub cutoff: RetentionBound,
    pub query_hash: i64,
    pub sealed: Vec<SealedRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionStateError {
    InvalidState,
    InvalidRange(RetentionRangeError),
    NotExpired,
    Overlap,
    CutoffRegression,
    TrimCannotSeal,
}

fn bound_value(bound: RetentionBound) -> i64 {
    match bound {
        RetentionBound::Date(value) => i64::from(value),
        RetentionBound::Timestamp(value) | RetentionBound::Timestamptz(value) => value,
    }
}

impl RetentionState {
    /// Validate persisted state before relying on it for a retention transition.
    pub fn validate(&self) -> Result<(), RetentionStateError> {
        if self.version != 1
            || self.candidate.source_oid == 0
            || self.candidate.source_column.is_empty()
            || self.candidate.output_column.is_empty()
            || (self.mode == RetentionMode::Trim && !self.sealed.is_empty())
        {
            return Err(RetentionStateError::InvalidState);
        }
        let finite_cutoff = match self.cutoff {
            RetentionBound::Date(value) => {
                self.candidate.source_type_oid == DATE_OID && value != i32::MIN && value != i32::MAX
            }
            RetentionBound::Timestamp(value) => {
                self.candidate.source_type_oid == TIMESTAMP_OID
                    && value != i64::MIN
                    && value != i64::MAX
            }
            RetentionBound::Timestamptz(value) => {
                self.candidate.source_type_oid == TIMESTAMPTZ_OID
                    && value != i64::MIN
                    && value != i64::MAX
            }
        };
        let valid_bucket = matches!(
            (self.candidate.bucket, self.candidate.source_type_oid),
            (
                RetentionBucket::Exact,
                DATE_OID | TIMESTAMP_OID | TIMESTAMPTZ_OID
            ) | (RetentionBucket::HourTimestamp, TIMESTAMP_OID)
                | (RetentionBucket::HourUtcTimestamptz, TIMESTAMPTZ_OID)
        );
        if !finite_cutoff || !valid_bucket {
            return Err(RetentionStateError::InvalidState);
        }
        let mut previous_upper = None;
        for range in &self.sealed {
            let expired = range_is_expired(&self.candidate, range.lower, range.upper, self.cutoff)
                .map_err(RetentionStateError::InvalidRange)?;
            if !expired {
                return Err(RetentionStateError::NotExpired);
            }
            if previous_upper.is_some_and(|upper| upper > bound_value(range.lower)) {
                return Err(RetentionStateError::Overlap);
            }
            previous_upper = Some(bound_value(range.upper));
        }
        Ok(())
    }

    /// Add a completely expired range, preserving exact repeats and rejecting overlap.
    pub fn seal(&mut self, range: SealedRange) -> Result<bool, RetentionStateError> {
        self.validate()?;
        if self.mode != RetentionMode::Retain {
            return Err(RetentionStateError::TrimCannotSeal);
        }
        if !range_is_expired(&self.candidate, range.lower, range.upper, self.cutoff)
            .map_err(RetentionStateError::InvalidRange)?
        {
            return Err(RetentionStateError::NotExpired);
        }
        if self.sealed.contains(&range) {
            return Ok(false);
        }
        if self.sealed.iter().any(|old| {
            bound_value(range.lower) < bound_value(old.upper)
                && bound_value(old.lower) < bound_value(range.upper)
        }) {
            return Err(RetentionStateError::Overlap);
        }
        self.sealed.push(range);
        self.sealed.sort_by_key(|item| bound_value(item.lower));
        Ok(true)
    }

    /// Move the cutoff forward without unsealing historical periods.
    pub fn advance_cutoff(&mut self, cutoff: RetentionBound) -> Result<(), RetentionStateError> {
        self.validate()?;
        if std::mem::discriminant(&cutoff) != std::mem::discriminant(&self.cutoff) {
            return Err(RetentionStateError::InvalidState);
        }
        if bound_value(cutoff) < bound_value(self.cutoff) {
            return Err(RetentionStateError::CutoffRegression);
        }
        let mut updated = self.clone();
        updated.cutoff = cutoff;
        updated.validate()?;
        self.cutoff = cutoff;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionRangeError {
    TypeMismatch,
    InfiniteBound,
    InvalidRange,
    UnalignedHour,
}

fn state_has_sealed_history(state: &serde_json::Value) -> Result<bool, &'static str> {
    let state: RetentionState =
        serde_json::from_value(state.clone()).map_err(|_| "malformed partition retention state")?;
    state
        .validate()
        .map_err(|_| "invalid partition retention invariants")?;
    Ok(state.mode == RetentionMode::Retain && !state.sealed.is_empty())
}

/// Fail closed when a whole-query operation would discard retained history.
#[cfg(not(test))]
pub(crate) fn ensure_history_reconstructible(
    pgt_id: i64,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies \
             WHERE pgt_id = $1 AND column_snapshot ? 'partition_retention'",
                None,
                &[pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        for row in rows {
            let state = row
                .get::<pgrx::JsonB>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InvalidArgument("partition retention state is missing".into())
                })?;
            if state_has_sealed_history(&state.0).map_err(|error| {
                PgTrickleError::InvalidArgument(format!(
                    "invalid partition retention state: {error}"
                ))
            })? {
                return Err(PgTrickleError::InvalidArgument(
                    "retained partition history cannot be reconstructed from the current source; \
                     a whole-query rebuild or restore would discard it"
                        .into(),
                ));
            }
        }
        Ok(())
    })
}

#[cfg(test)]
pub(crate) fn ensure_history_reconstructible(
    _pgt_id: i64,
) -> Result<(), crate::error::PgTrickleError> {
    Ok(())
}

/// RETAIN must capture a cut before any source partition can disappear.
#[cfg(not(test))]
pub(crate) fn ensure_removal_prepared(pgt_id: i64) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies \
             WHERE pgt_id = $1 AND column_snapshot ? 'partition_retention'",
                None,
                &[pgt_id.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        for row in rows {
            let value = row
                .get::<pgrx::JsonB>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InvalidArgument("partition retention state is missing".into())
                })?;
            let state: RetentionState = serde_json::from_value(value.0).map_err(|e| {
                PgTrickleError::InvalidArgument(format!("invalid partition retention state: {e}"))
            })?;
            state.validate().map_err(|e| {
                PgTrickleError::InvalidArgument(format!("invalid partition retention state: {e:?}"))
            })?;
            if state.mode == RetentionMode::Retain {
                return Err(PgTrickleError::InvalidArgument(
                    "RETAIN requires a prepared partition cut to preserve retained partition history".into(),
                ));
            }
        }
        Ok(())
    })
}

#[cfg(test)]
pub(crate) fn ensure_removal_prepared(_pgt_id: i64) -> Result<(), crate::error::PgTrickleError> {
    Ok(())
}

/// A query change invalidates the proof associated with any configured policy.
#[cfg(not(test))]
pub(crate) fn ensure_query_change_allowed(pgt_id: i64) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let configured = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_dependencies \
         WHERE pgt_id = $1 AND column_snapshot ? 'partition_retention')",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(true);
    if configured {
        return Err(PgTrickleError::InvalidArgument(
            "disable partition retention before changing the stream table query; \
             retained history cannot be reconstructed from deleted source rows"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn ensure_query_change_allowed(
    _pgt_id: i64,
) -> Result<(), crate::error::PgTrickleError> {
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPartition {
    pub oid: u32,
    pub lower: RetentionBound,
    pub upper: RetentionBound,
    pub expired: bool,
}

/// A candidate cut is valid only while the current transaction holds its locks.
#[cfg(not(test))]
#[derive(Debug)]
pub struct PreparedRetentionCut {
    pgt_id: i64,
    source_oid: u32,
    target_oid: u32,
    range: SealedRange,
    state_before_removal: RetentionState,
    state_after_removal: RetentionState,
    safe_bound: String,
    transaction_id: u64,
    subtransaction_id: u32,
}

#[cfg(not(test))]
thread_local! {
    static PREPARED_DETACH: std::cell::RefCell<Option<PreparedRetentionCut>> = const { std::cell::RefCell::new(None) };
}

#[cfg(not(test))]
struct PreparedDetachGuard;

#[cfg(not(test))]
impl Drop for PreparedDetachGuard {
    fn drop(&mut self) {
        PREPARED_DETACH.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

/// Run the matching ordinary DETACH while its validated cut is active.
#[cfg(not(test))]
pub fn with_prepared_retained_detach<T>(
    cut: PreparedRetentionCut,
    detach: impl FnOnce() -> Result<T, crate::error::PgTrickleError>,
) -> Result<T, crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    PREPARED_DETACH.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_some() {
            return Err(PgTrickleError::InvalidArgument(
                "nested retention detach is not supported".into(),
            ));
        }
        *slot = Some(cut);
        Ok(())
    })?;
    let _guard = PreparedDetachGuard;
    let result = detach()?;
    let pending = PREPARED_DETACH.with(|slot| slot.borrow().is_some());
    if pending {
        return Err(PgTrickleError::InvalidArgument(
            "prepared partition detach was not finalized by the DDL hook".into(),
        ));
    }
    Ok(result)
}

/// Consume only the cut for this stream and source; all other DDL stays unchanged.
#[cfg(not(test))]
pub(crate) fn finish_prepared_retained_detach(
    pgt_id: i64,
    source_oid: pgrx::pg_sys::Oid,
) -> Result<bool, crate::error::PgTrickleError> {
    let cut = PREPARED_DETACH.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot
            .as_ref()
            .is_some_and(|cut| cut.pgt_id == pgt_id && cut.source_oid == source_oid.to_u32())
        {
            slot.take()
        } else {
            None
        }
    });
    match cut {
        Some(cut) => {
            match cut.state_after_removal.mode {
                RetentionMode::Retain => finalize_retained_cut(cut)?,
                RetentionMode::Trim => finalize_trimmed_cut(cut)?,
            }
            Ok(true)
        }
        None => Ok(false),
    }
}

#[cfg(test)]
pub(crate) fn finish_prepared_retained_detach(
    _pgt_id: i64,
    _source_oid: pgrx::pg_sys::Oid,
) -> Result<bool, crate::error::PgTrickleError> {
    Ok(false)
}

/// Drain this stream's trigger CDC under source SHARE locks before removal.
/// The caller must perform the matching DDL in the same transaction.
#[cfg(not(test))]
pub fn prepare_retention_cut(
    pgt_id: i64,
    target_oid: pgrx::pg_sys::Oid,
) -> Result<PreparedRetentionCut, crate::error::PgTrickleError> {
    use crate::catalog::{CdcMode, StDependency, StreamTableMeta, compute_defining_query_hash};
    use crate::dag::{RefreshMode, StStatus};
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;

    Spi::run_with_args(
        "SELECT pg_catalog.pg_advisory_xact_lock($1)",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Spi::get_one_with_args::<i64>(
        "SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_id = $1 FOR UPDATE",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| PgTrickleError::NotFound(format!("stream table {pgt_id}")))?;
    let st = StreamTableMeta::get_by_id(pgt_id)?
        .ok_or_else(|| PgTrickleError::NotFound(format!("stream table {pgt_id}")))?;
    if st.status != StStatus::Active
        || st.refresh_mode != RefreshMode::Differential
        || st.needs_reinit
        || !st.is_populated
        || st.topk_limit.is_some()
        || st
            .frontier
            .as_ref()
            .is_none_or(crate::version::Frontier::is_empty)
        || st.orchestration_mode.eq_ignore_ascii_case("EXTERNAL")
    {
        return Err(PgTrickleError::InvalidArgument(
            "retention requires an active, initialized, managed DIFFERENTIAL stream without pending reinitialization".into(),
        ));
    }
    let dependencies = StDependency::get_for_st(pgt_id)?;
    let [dependency] = dependencies.as_slice() else {
        return Err(PgTrickleError::InvalidArgument(
            "retention requires exactly one source".into(),
        ));
    };
    if dependency.source_type != "TABLE" || dependency.cdc_mode != CdcMode::Trigger {
        return Err(PgTrickleError::InvalidArgument(
            "retention requires a trigger-CDC table source".into(),
        ));
    }
    let source_oid = dependency.source_relid;
    let value = pgrx::Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies \
         WHERE pgt_id = $1 AND source_relid = $2",
        &[pgt_id.into(), source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InvalidArgument("partition retention is not configured".into())
    })?;
    let mut state: RetentionState = serde_json::from_value(value.0).map_err(|e| {
        PgTrickleError::InvalidArgument(format!("invalid partition retention state: {e}"))
    })?;
    state.validate().map_err(|e| {
        PgTrickleError::InvalidArgument(format!("invalid partition retention state: {e:?}"))
    })?;
    if state.mode == RetentionMode::Retain {
        if st.st_partition_key.is_some()
            || !st.storage_backend.eq_ignore_ascii_case("heap")
            || !st.st_placement.eq_ignore_ascii_case("local")
        {
            return Err(PgTrickleError::InvalidArgument(
                "RETAIN requires local, unpartitioned heap storage".into(),
            ));
        }
        let durable = Spi::get_one_with_args::<bool>(
            "SELECT s.relkind = 'r' AND s.relpersistence = 'p' AND src.relpersistence = 'p' \
                    AND b.durability IN ('logged', 'sync') \
             FROM pg_catalog.pg_class s JOIN pg_catalog.pg_class src ON src.oid = $2 \
             JOIN pgtrickle.pgt_change_buffers b ON b.source_kind = 'BASE' AND b.source_id = $2::oid::bigint \
             WHERE s.oid = $1",
            &[st.pgt_relid.into(), source_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);
        if !durable {
            return Err(PgTrickleError::InvalidArgument(
                "RETAIN requires logged source, stream storage, and CDC buffer".into(),
            ));
        }
    }
    if state.mode == RetentionMode::Trim {
        let indexed = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_index i \
             JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid \
             JOIN pg_catalog.pg_am am ON am.oid = idx.relam \
             JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attname = $2 \
             JOIN pg_catalog.pg_opclass op ON op.oid = i.indclass[0] \
             WHERE i.indrelid = $1 AND i.indisvalid AND i.indisready \
               AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indkey[0] = a.attnum \
               AND am.amname = 'btree' AND op.opcdefault AND op.opcintype = a.atttypid)",
            &[
                st.pgt_relid.into(),
                state.candidate.output_column.clone().into(),
            ],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);
        let has_export = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_output_delta_consumers \
             WHERE pgt_id = $1 AND state <> 'DROPPED')",
            &[pgt_id.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(true);
        if !indexed || has_export || crate::cdc::has_user_triggers(st.pgt_relid)? {
            return Err(PgTrickleError::InvalidArgument(
                "TRIM requires a btree index on the output group key and no output-delta consumers or user triggers".into(),
            ));
        }
        let table = crate::sql_builder::qualified(&st.pgt_schema, &st.pgt_name);
        crate::refresh::with_stream_owner(&st, || {
            Spi::run(&format!("LOCK TABLE ONLY {table} IN ROW EXCLUSIVE MODE"))
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        })?;
    }
    if state.candidate.source_oid != source_oid.to_u32()
        || state.query_hash != compute_defining_query_hash(&st.defining_query)
    {
        return Err(PgTrickleError::InvalidArgument(
            "retention source or defining query changed; reconfigure before removal".into(),
        ));
    }
    let candidate = crate::refresh::with_stream_owner(&st, || {
        let tree = super::parser::parse_defining_query(&st.defining_query)?;
        query_group_candidate(&tree, source_oid.to_u32(), &state.candidate.source_column).map_err(
            |e| {
                PgTrickleError::InvalidArgument(format!(
                    "retention query is no longer eligible: {e:?}"
                ))
            },
        )
    })?;
    if candidate != state.candidate {
        return Err(PgTrickleError::InvalidArgument(
            "retention group mapping changed; reconfigure before removal".into(),
        ));
    }
    let layout = validate_partition_layout(
        source_oid,
        &candidate.source_column,
        &candidate,
        state.cutoff,
    )?;
    for partition in &layout {
        if state.sealed.iter().any(|range| {
            bound_value(partition.lower) < bound_value(range.upper)
                && bound_value(range.lower) < bound_value(partition.upper)
        }) {
            return Err(PgTrickleError::InvalidArgument(
                "a live partition overlaps retained history".into(),
            ));
        }
    }
    let partition = layout
        .iter()
        .find(|partition| partition.oid == target_oid.to_u32())
        .ok_or_else(|| {
            PgTrickleError::InvalidArgument(
                "retention target is not a direct source partition".into(),
            )
        })?;
    if !partition.expired {
        return Err(PgTrickleError::InvalidArgument(
            "partition is not wholly below the retention cutoff".into(),
        ));
    }
    let range = SealedRange {
        lower: partition.lower,
        upper: partition.upper,
    };
    let state_before_removal = state.clone();
    if state.mode == RetentionMode::Retain {
        state.seal(range).map_err(|e| {
            PgTrickleError::InvalidArgument(format!("cannot seal retained range: {e:?}"))
        })?;
    }

    crate::cdc::lock_source_relations(&[source_oid])?;
    let safe_bound = crate::cdc::get_current_wal_lsn()?;
    let refresh = crate::refresh::with_refresh_context(
        crate::refresh::RefreshContext::graph(&safe_bound, crate::refresh::FullPolicy::Error),
        || {
            crate::api::refresh_ops::execute_manual_refresh(
                &st,
                &st.pgt_schema,
                &st.pgt_name,
                &[source_oid],
            )
        },
    )?;
    if !matches!(refresh.action.as_str(), "DIFFERENTIAL" | "NO_DATA") {
        return Err(PgTrickleError::InvalidArgument(format!(
            "retention preparation performed unexpected refresh mode {}",
            refresh.action
        )));
    }
    // SAFETY: PostgreSQL exposes these identifiers for the current backend transaction.
    let (transaction_id, subtransaction_id) = unsafe {
        (
            pg_sys::GetTopFullTransactionId().value,
            pg_sys::GetCurrentSubTransactionId(),
        )
    };
    Ok(PreparedRetentionCut {
        pgt_id,
        source_oid: source_oid.to_u32(),
        target_oid: target_oid.to_u32(),
        range,
        state_before_removal,
        state_after_removal: state,
        safe_bound,
        transaction_id,
        subtransaction_id,
    })
}

#[cfg(not(test))]
fn retention_bound_sql(bound: RetentionBound) -> String {
    match bound {
        RetentionBound::Date(days) => format!("(DATE '2000-01-01' OPERATOR(pg_catalog.+) {days})"),
        RetentionBound::Timestamp(micros) | RetentionBound::Timestamptz(micros) => {
            let days = micros.div_euclid(86_400_000_000);
            let remainder = micros.rem_euclid(86_400_000_000);
            let timestamp = format!(
                "(TIMESTAMP '2000-01-01' OPERATOR(pg_catalog.+) ({days} OPERATOR(pg_catalog.*) INTERVAL '1 day') OPERATOR(pg_catalog.+) ({remainder} OPERATOR(pg_catalog.*) INTERVAL '1 microsecond'))"
            );
            if matches!(bound, RetentionBound::Timestamptz(_)) {
                format!("({timestamp} AT TIME ZONE 'UTC')")
            } else {
                timestamp
            }
        }
    }
}

/// Delete only the groups proved to belong to the removed range.
#[cfg(not(test))]
fn finalize_trimmed_cut(cut: PreparedRetentionCut) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    // SAFETY: PostgreSQL exposes these identifiers for the current backend transaction.
    let same_transaction = unsafe {
        pg_sys::GetTopFullTransactionIdIfAny().value == cut.transaction_id
            && pg_sys::GetCurrentSubTransactionId() == cut.subtransaction_id
    };
    let detached = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE pid = pg_catalog.pg_backend_pid() \
                       AND relation = $1 AND mode = 'ShareLock' AND granted) \
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent = $1 AND inhrelid = $2)",
        &[pg_sys::Oid::from(cut.source_oid).into(), pg_sys::Oid::from(cut.target_oid).into()],
    ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?.unwrap_or(false);
    if !same_transaction || !detached || cut.state_after_removal.mode != RetentionMode::Trim {
        return Err(PgTrickleError::InvalidArgument(
            "trim cut belongs to another transaction or target is still attached".into(),
        ));
    }
    let st = crate::catalog::StreamTableMeta::get_by_id(cut.pgt_id)?
        .ok_or_else(|| PgTrickleError::NotFound(format!("stream table {}", cut.pgt_id)))?;
    let cols = crate::refresh::get_st_user_columns(&st);
    if cols.is_empty() {
        return Err(PgTrickleError::InvalidArgument(
            "cannot resolve TRIM output columns".into(),
        ));
    }
    let table = crate::sql_builder::qualified(&st.pgt_schema, &st.pgt_name);
    let key = crate::sql_builder::ident(&cut.state_after_removal.candidate.output_column);
    let columns = cols
        .iter()
        .map(|c| crate::sql_builder::ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let lower = retention_bound_sql(cut.range.lower);
    let upper = retention_bound_sql(cut.range.upper);
    let has_downstream = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_dependencies dep \
         JOIN pgtrickle.pgt_stream_tables downstream ON downstream.pgt_id = dep.pgt_id \
         WHERE dep.source_relid = $1 AND dep.source_type = 'STREAM_TABLE' \
           AND downstream.refresh_mode <> 'IMMEDIATE')",
        &[st.pgt_relid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| PgTrickleError::InternalError("cannot inspect downstream streams".into()))?;
    let pre = if has_downstream {
        Some(crate::refresh::prepare_owner_temp_table(
            &st,
            &format!("__pgt_trim_{}", st.pgt_id),
            &format!("SELECT __pgt_row_id, {columns} FROM {table}"),
        )?)
    } else {
        None
    };
    Spi::run("SET LOCAL pg_trickle.internal_refresh = 'true'")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    crate::refresh::with_stream_owner(&st, || {
        let delete = format!(
            "DELETE FROM {table} WHERE {key} OPERATOR(pg_catalog.>=) {lower} AND {key} OPERATOR(pg_catalog.<) {upper}"
        );
        let sql = if let Some(pre) = &pre {
            format!(
                "WITH removed AS ({delete} RETURNING __pgt_row_id, {columns}) INSERT INTO {pre} SELECT * FROM removed"
            )
        } else {
            delete
        };
        Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))
    })?;
    if let Some(pre) = pre {
        let schema = crate::config::pg_trickle_change_buffer_schema();
        crate::cdc::set_sync_commit_for_buffer(&format!("changes_pgt_{}", st.pgt_id))?;
        crate::cdc::validate_st_change_buffer(st.pgt_id, &schema)?;
        let buffer = crate::sql_builder::qualified(&schema, &format!("changes_pgt_{}", st.pgt_id));
        Spi::run(&format!(
            "INSERT INTO {buffer} (lsn, action, __pgt_row_id, source_xid, source_commit_at, {columns}) \
             SELECT pg_catalog.pg_current_wal_insert_lsn(), 'D', __pgt_row_id, NULL::xid, NULL::timestamptz, {columns} FROM {pre}"
        )).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    }
    Ok(())
}

/// Persist a retained cut after its partition has been removed in this transaction.
#[cfg(not(test))]
pub fn finalize_retained_cut(
    cut: PreparedRetentionCut,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    // SAFETY: PostgreSQL exposes these identifiers for the current backend transaction.
    let same_transaction = unsafe {
        pg_sys::GetTopFullTransactionIdIfAny().value == cut.transaction_id
            && pg_sys::GetCurrentSubTransactionId() == cut.subtransaction_id
    };
    if !same_transaction || cut.state_after_removal.mode != RetentionMode::Retain {
        return Err(PgTrickleError::InvalidArgument(
            "retained cut belongs to another transaction or mode".into(),
        ));
    }
    let valid = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE pid = pg_catalog.pg_backend_pid() \
                       AND relation = $1 AND mode = 'ShareLock' AND granted) \
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhparent = $1 AND inhrelid = $2)",
        &[pg_sys::Oid::from(cut.source_oid).into(), pg_sys::Oid::from(cut.target_oid).into()],
    ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?.unwrap_or(false);
    if !valid {
        return Err(PgTrickleError::InvalidArgument(
            "retention source lock is missing or target is still attached".into(),
        ));
    }
    cut.state_after_removal
        .validate()
        .map_err(|e| PgTrickleError::InvalidArgument(format!("invalid retained state: {e:?}")))?;
    let before = pgrx::JsonB(
        serde_json::to_value(&cut.state_before_removal)
            .map_err(|e| PgTrickleError::InternalError(e.to_string()))?,
    );
    let after = pgrx::JsonB(
        serde_json::to_value(&cut.state_after_removal)
            .map_err(|e| PgTrickleError::InternalError(e.to_string()))?,
    );
    let updated = Spi::get_one_with_args::<i64>(
        "UPDATE pgtrickle.pgt_dependencies SET column_snapshot = \
           pg_catalog.jsonb_set(column_snapshot, '{partition_retention}', $3, true) \
         WHERE pgt_id = $1 AND source_relid = $2 AND column_snapshot->'partition_retention' = $4 \
         RETURNING pgt_id",
        &[
            cut.pgt_id.into(),
            pg_sys::Oid::from(cut.source_oid).into(),
            after.into(),
            before.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    if updated != Some(cut.pgt_id) {
        return Err(PgTrickleError::InvalidArgument(
            "retention state changed during partition removal".into(),
        ));
    }
    Ok(())
}

/// Reject attachments that could mix live rows with sealed historical groups.
#[cfg(not(test))]
pub(crate) fn ensure_retained_attach_safe(
    pgt_id: i64,
    source_oid: pgrx::pg_sys::Oid,
    empty: bool,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;

    let value = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies \
         WHERE pgt_id = $1 AND source_relid = $2",
        &[pgt_id.into(), source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    let Some(value) = value else { return Ok(()) };
    let state: RetentionState = serde_json::from_value(value.0).map_err(|e| {
        PgTrickleError::InvalidArgument(format!("invalid partition retention state: {e}"))
    })?;
    state.validate().map_err(|e| {
        PgTrickleError::InvalidArgument(format!("invalid partition retention state: {e:?}"))
    })?;
    if state.mode != RetentionMode::Retain || state.sealed.is_empty() {
        return Ok(());
    }
    if !empty {
        return Err(PgTrickleError::InvalidArgument(
            "cannot attach a nonempty partition to a stream with retained history; attach an empty future partition and insert live rows normally".into(),
        ));
    }
    // SAFETY: The GUC nest is restored even if PostgreSQL reports an error.
    let layout = unsafe {
        let level = pg_sys::NewGUCNestLevel();
        pgrx::PgTryBuilder::new(std::panic::AssertUnwindSafe(|| {
            Spi::run("SET LOCAL search_path = pg_catalog, pg_temp")
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            validate_partition_layout(
                source_oid,
                &state.candidate.source_column,
                &state.candidate,
                state.cutoff,
            )
        }))
        .finally(move || pg_sys::AtEOXact_GUC(false, level))
        .execute()?
    };
    if layout.iter().any(|partition| {
        state.sealed.iter().any(|range| {
            bound_value(partition.lower) < bound_value(range.upper)
                && bound_value(range.lower) < bound_value(partition.upper)
        })
    }) {
        return Err(PgTrickleError::InvalidArgument(
            "cannot attach a partition overlapping retained history".into(),
        ));
    }
    Ok(())
}

/// Lock and validate a source's direct RANGE partitions against a cutoff.
///
/// The lock is held until the caller's transaction ends. This validates only
/// the current layout; later partition DDL must be checked again.
#[cfg(not(test))]
pub fn validate_partition_layout(
    source_oid: pgrx::pg_sys::Oid,
    key: &str,
    candidate: &RetentionGroupCandidate,
    cutoff: RetentionBound,
) -> Result<Vec<RetentionPartition>, crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;

    if candidate.source_oid != source_oid.to_u32() || candidate.source_column != key {
        return Err(PgTrickleError::InvalidArgument(
            "retention group candidate does not belong to this source key".into(),
        ));
    }
    let catalog_position = Spi::get_one::<i32>(
        "SELECT pg_catalog.array_position(pg_catalog.current_schemas(false), 'pg_catalog'::pg_catalog.name)",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    if catalog_position.is_some_and(|position| position != 1) {
        return Err(PgTrickleError::InvalidArgument(
            "retention layout validation requires pg_catalog before other schemas in search_path"
                .into(),
        ));
    }
    let cutoff_valid = match cutoff {
        RetentionBound::Date(value) => {
            candidate.source_type_oid == DATE_OID && value != i32::MIN && value != i32::MAX
        }
        RetentionBound::Timestamp(value) => {
            candidate.source_type_oid == TIMESTAMP_OID && value != i64::MIN && value != i64::MAX
        }
        RetentionBound::Timestamptz(value) => {
            candidate.source_type_oid == TIMESTAMPTZ_OID && value != i64::MIN && value != i64::MAX
        }
    };
    let bucket_valid = matches!(
        (candidate.bucket, candidate.source_type_oid),
        (
            RetentionBucket::Exact,
            DATE_OID | TIMESTAMP_OID | TIMESTAMPTZ_OID
        ) | (RetentionBucket::HourTimestamp, TIMESTAMP_OID)
            | (RetentionBucket::HourUtcTimestamptz, TIMESTAMPTZ_OID)
    );
    if !cutoff_valid || !bucket_valid {
        return Err(PgTrickleError::InvalidArgument(
            "retention cutoff must be finite and match the source key type".into(),
        ));
    }
    if !Spi::get_one::<bool>(
        "SELECT pg_catalog.current_setting('transaction_isolation') = 'read committed'",
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false)
    {
        return Err(PgTrickleError::InvalidArgument(
            "retention layout validation requires READ COMMITTED isolation".into(),
        ));
    }
    let name = Spi::get_one_with_args::<String>(
        "SELECT pg_catalog.format('%I.%I', n.nspname, c.relname) \
         FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.oid = $1 AND c.relkind = 'p'",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InvalidArgument("retention source must be a partitioned table".into())
    })?;
    Spi::run(&format!(
        "LOCK TABLE ONLY {name} IN SHARE UPDATE EXCLUSIVE MODE"
    ))
    .map_err(|e| PgTrickleError::SpiError(format!("cannot lock retention source {name}: {e}")))?;
    let locked = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks \
         WHERE pid = pg_catalog.pg_backend_pid() AND relation = $1 \
           AND mode = 'ShareUpdateExclusiveLock' AND granted)",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);
    if !locked {
        return Err(PgTrickleError::InvalidArgument(
            "retention source identity changed while acquiring its lock".into(),
        ));
    }
    let valid = Spi::get_one_with_args::<bool>(
        "SELECT p.partstrat = 'r' AND p.partnatts = 1 AND p.partexprs IS NULL \
                AND p.partdefid = 0 AND a.attname::text = $2 AND a.attnotnull \
                AND a.atttypid = $3::oid AND p.partcollation[0] = 0 \
                AND op.opcdefault AND op.opcintype = a.atttypid AND am.amname = 'btree' \
         FROM pg_catalog.pg_partitioned_table p \
         JOIN pg_catalog.pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = p.partattrs[0] \
         JOIN pg_catalog.pg_opclass op ON op.oid = p.partclass[0] \
         JOIN pg_catalog.pg_am am ON am.oid = op.opcmethod \
         WHERE p.partrelid = $1",
        &[
            source_oid.into(),
            key.into(),
            pgrx::pg_sys::Oid::from(candidate.source_type_oid).into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);
    if !valid {
        return Err(PgTrickleError::InvalidArgument(
            "retention requires one non-null RANGE column with its default btree operator class and no default partition".into(),
        ));
    }
    Spi::connect_mut(|client| {
        let rows = client.update(
            "SELECT c.oid::bigint, c.relpartbound::text, i.inhdetachpending FROM pg_catalog.pg_inherits i \
             JOIN pg_catalog.pg_class c ON c.oid = i.inhrelid \
             WHERE i.inhparent = $1 ORDER BY c.oid",
            None,
            &[source_oid.into()],
        ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut partitions = Vec::new();
        for row in rows {
            if row
                .get::<bool>(3)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or(true)
            {
                return Err(PgTrickleError::InvalidArgument(
                    "retention source has a pending partition detach".into(),
                ));
            }
            let oid = row
                .get::<i64>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| PgTrickleError::InternalError("invalid partition OID".into()))?;
            let node = row
                .get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InvalidArgument(format!("partition {oid} has no bound"))
                })?;
            let range = decode_catalog_range(&node).ok_or_else(|| {
                PgTrickleError::InvalidArgument(format!(
                    "partition {oid} has an unsupported or unbounded RANGE bound"
                ))
            })?;
            let type_oid = range["type_oid"]
                .as_u64()
                .and_then(|value| u32::try_from(value).ok())
                .ok_or_else(|| {
                    PgTrickleError::InternalError("invalid partition bound type".into())
                })?;
            let read = |name: &str| -> Result<RetentionBound, PgTrickleError> {
                let value = range[name].as_i64().ok_or_else(|| {
                    PgTrickleError::InternalError("invalid partition bound value".into())
                })?;
                match type_oid {
                    DATE_OID => i32::try_from(value)
                        .map(RetentionBound::Date)
                        .map_err(|_| PgTrickleError::InternalError("invalid date bound".into())),
                    TIMESTAMP_OID => Ok(RetentionBound::Timestamp(value)),
                    TIMESTAMPTZ_OID => Ok(RetentionBound::Timestamptz(value)),
                    _ => Err(PgTrickleError::InvalidArgument(format!(
                        "partition {oid} has an unsupported bound type"
                    ))),
                }
            };
            let lower = read("lower")?;
            let upper = read("upper")?;
            let expired = range_is_expired(candidate, lower, upper, cutoff).map_err(|error| {
                PgTrickleError::InvalidArgument(format!(
                    "partition {oid} is not eligible for retention: {error:?}"
                ))
            })?;
            partitions.push(RetentionPartition {
                oid,
                lower,
                upper,
                expired,
            });
        }
        Ok(partitions)
    })
}

/// Decode a catalog-owned partition bound before its relation disappears.
#[cfg(not(test))]
pub(crate) fn decode_catalog_range(node: &str) -> Option<serde_json::Value> {
    use pgrx::pg_sys;
    let input = std::ffi::CString::new(node).ok()?;
    // SAFETY: `node` comes directly from pg_class.relpartbound in this backend.
    // PostgreSQL allocates the deserialized tree in the current memory context;
    // each pointer is checked for its expected node tag before dereferencing.
    unsafe {
        let raw = pg_sys::stringToNode(input.as_ptr()) as *const pg_sys::Node;
        let root = raw.as_ref()?;
        if root.type_ != pg_sys::NodeTag::T_PartitionBoundSpec {
            return None;
        }
        let spec = &*(raw as *const pg_sys::PartitionBoundSpec);
        if spec.is_default
            || spec.strategy != b'r' as std::ffi::c_char
            || pg_sys::list_length(spec.lowerdatums) != 1
            || pg_sys::list_length(spec.upperdatums) != 1
        {
            return None;
        }
        let read = |list: *mut pg_sys::List| -> Option<(u32, i64)> {
            let raw = pg_sys::list_nth(list, 0) as *const pg_sys::Node;
            if raw.as_ref()?.type_ != pg_sys::NodeTag::T_PartitionRangeDatum {
                return None;
            }
            let datum = &*(raw as *const pg_sys::PartitionRangeDatum);
            if datum.kind != pg_sys::PartitionRangeDatumKind::PARTITION_RANGE_DATUM_VALUE {
                return None;
            }
            let value = datum.value.as_ref()?;
            if value.type_ != pg_sys::NodeTag::T_Const {
                return None;
            }
            let value = &*(datum.value as *const pg_sys::Const);
            if value.constisnull || !value.constbyval {
                return None;
            }
            let oid = value.consttype.to_u32();
            let raw = value.constvalue.value();
            let value = match (oid, value.constlen) {
                (DATE_OID, 4) => i64::from(raw as i32),
                (TIMESTAMP_OID | TIMESTAMPTZ_OID, 8) => raw as i64,
                _ => return None,
            };
            Some((oid, value))
        };
        let (lower_type, lower) = read(spec.lowerdatums)?;
        let (upper_type, upper) = read(spec.upperdatums)?;
        if lower_type != upper_type {
            return None;
        }
        Some(serde_json::json!({
            "type_oid": lower_type, "lower": lower, "upper": upper
        }))
    }
}

/// Check one finite PostgreSQL RANGE interval `[lower, upper)` against a cutoff.
pub fn range_is_expired(
    candidate: &RetentionGroupCandidate,
    lower: RetentionBound,
    upper: RetentionBound,
    cutoff: RetentionBound,
) -> Result<bool, RetentionRangeError> {
    let bucket = candidate.bucket;
    let bound_type = match lower {
        RetentionBound::Date(_) => DATE_OID,
        RetentionBound::Timestamp(_) => TIMESTAMP_OID,
        RetentionBound::Timestamptz(_) => TIMESTAMPTZ_OID,
    };
    if candidate.source_type_oid != bound_type {
        return Err(RetentionRangeError::TypeMismatch);
    }
    let (lower, upper, cutoff, hourly) = match (bucket, lower, upper, cutoff) {
        (
            RetentionBucket::Exact,
            RetentionBound::Date(l),
            RetentionBound::Date(u),
            RetentionBound::Date(c),
        ) => {
            if [l, u, c]
                .iter()
                .any(|value| *value == i32::MIN || *value == i32::MAX)
            {
                return Err(RetentionRangeError::InfiniteBound);
            }
            (i64::from(l), i64::from(u), i64::from(c), false)
        }
        (
            RetentionBucket::Exact | RetentionBucket::HourTimestamp,
            RetentionBound::Timestamp(l),
            RetentionBound::Timestamp(u),
            RetentionBound::Timestamp(c),
        ) => (l, u, c, bucket == RetentionBucket::HourTimestamp),
        (
            RetentionBucket::Exact | RetentionBucket::HourUtcTimestamptz,
            RetentionBound::Timestamptz(l),
            RetentionBound::Timestamptz(u),
            RetentionBound::Timestamptz(c),
        ) => (l, u, c, bucket == RetentionBucket::HourUtcTimestamptz),
        _ => return Err(RetentionRangeError::TypeMismatch),
    };
    if [lower, upper, cutoff]
        .iter()
        .any(|value| *value == i64::MIN || *value == i64::MAX)
    {
        return Err(RetentionRangeError::InfiniteBound);
    }
    if lower >= upper {
        return Err(RetentionRangeError::InvalidRange);
    }
    const HOUR_MICROS: i64 = 3_600_000_000;
    if hourly && (lower.rem_euclid(HOUR_MICROS) != 0 || upper.rem_euclid(HOUR_MICROS) != 0) {
        return Err(RetentionRangeError::UnalignedHour);
    }
    Ok(upper <= cutoff)
}

fn source_column(expr: &Expr, alias: &str, key: &str) -> bool {
    matches!(expr, Expr::ColumnRef { table_alias, column_name }
        if column_name == key && table_alias.as_deref().is_none_or(|table| table == alias))
}

fn bucket(expr: &Expr, alias: &str, key: &str, type_oid: u32) -> Option<RetentionBucket> {
    if source_column(expr, alias, key) {
        return Some(RetentionBucket::Exact);
    }
    let Expr::FuncCall { func_name, args } = expr else {
        return None;
    };
    if func_name != "pg_catalog.date_trunc" {
        return None;
    }
    match args.as_slice() {
        [Expr::Literal(unit), input]
            if type_oid == TIMESTAMP_OID
                && unit == "'hour'"
                && source_column(input, alias, key) =>
        {
            Some(RetentionBucket::HourTimestamp)
        }
        [Expr::Literal(unit), input, Expr::Literal(zone)]
            if type_oid == TIMESTAMPTZ_OID
                && unit == "'hour'"
                && zone == "'UTC'"
                && source_column(input, alias, key) =>
        {
            Some(RetentionBucket::HourUtcTimestamptz)
        }
        _ => None,
    }
}

/// Find a projected group key tied to the specified non-null source column.
///
/// Hour candidates require separately verified hour-aligned RANGE bounds.
pub fn query_group_candidate(
    tree: &OpTree,
    source_oid: u32,
    key: &str,
) -> Result<RetentionGroupCandidate, RetentionQueryError> {
    let (projection, aggregate) = match tree {
        OpTree::Project {
            expressions,
            aliases,
            child,
        } if expressions.len() == aliases.len() => (Some((expressions, aliases)), child.as_ref()),
        OpTree::Aggregate { .. } => (None, tree),
        _ => return Err(RetentionQueryError::UnsupportedShape),
    };
    let OpTree::Aggregate {
        group_by,
        aggregates,
        child,
    } = aggregate
    else {
        return Err(RetentionQueryError::UnsupportedShape);
    };
    let OpTree::Scan {
        table_oid,
        columns,
        alias,
        ..
    } = child.as_ref()
    else {
        return Err(RetentionQueryError::UnsupportedShape);
    };
    let column = columns
        .iter()
        .find(|column| column.name == key && !column.is_nullable)
        .filter(|column| matches!(column.type_oid, DATE_OID | TIMESTAMP_OID | TIMESTAMPTZ_OID))
        .filter(|_| *table_oid == source_oid)
        .ok_or(RetentionQueryError::InvalidSourceKey)?;
    if aggregates.is_empty()
        || aggregates.iter().any(|aggregate| {
            !matches!(
                aggregate.function,
                AggFunc::CountStar | AggFunc::Count | AggFunc::Sum
            ) || aggregate.is_distinct
                || aggregate.filter.is_some()
                || aggregate.second_arg.is_some()
                || aggregate.order_within_group.is_some()
        })
    {
        return Err(RetentionQueryError::UnsupportedAggregate);
    }

    for group in group_by {
        let Some(candidate) = bucket(group, alias, key, column.type_oid) else {
            continue;
        };
        let group_output = group.output_name();
        if candidate != RetentionBucket::Exact
            && columns.iter().any(|column| column.name == group_output)
        {
            return Err(RetentionQueryError::UnsupportedShape);
        }
        if group_by
            .iter()
            .filter(|expr| expr.output_name() == group_output)
            .count()
            != 1
            || aggregates
                .iter()
                .any(|aggregate| aggregate.alias == group_output)
        {
            return Err(RetentionQueryError::UnsupportedShape);
        }
        let output = if let Some((expressions, aliases)) = projection {
            let mut matching = expressions.iter().zip(aliases).filter_map(|(expr, output)| {
                matches!(expr, Expr::ColumnRef { table_alias: None, column_name } if *column_name == group_output)
                    .then_some(output)
            });
            let Some(output) = matching.next() else {
                continue;
            };
            if matching.next().is_some()
                || aliases.iter().filter(|alias| *alias == output).count() != 1
            {
                return Err(RetentionQueryError::UnsupportedShape);
            }
            output.clone()
        } else {
            group_output
        };
        if output.is_empty() {
            return Err(RetentionQueryError::UnsupportedShape);
        }
        return Ok(RetentionGroupCandidate {
            output_column: output,
            bucket: candidate,
            source_type_oid: column.type_oid,
            source_oid,
            source_column: key.to_string(),
        });
    }
    Err(RetentionQueryError::MissingProjectedGroupKey)
}

#[cfg(all(feature = "e2e-unsafe-test-hooks", not(test)))]
#[pgrx::pg_extern(schema = "pgtrickle")]
fn retention_candidate_test(query: &str, source_oid: pgrx::pg_sys::Oid, key: &str) -> String {
    match super::parser::parse_defining_query(query) {
        Ok(tree) => match query_group_candidate(&tree, source_oid.to_u32(), key) {
            Ok(candidate) => format!(
                "Ok({:?}, {:?}, {})",
                candidate.output_column, candidate.bucket, candidate.source_type_oid
            ),
            Err(error) => format!("Err({error:?})"),
        },
        Err(error) => format!("parse error: {error}"),
    }
}

#[cfg(all(feature = "e2e-unsafe-test-hooks", not(test)))]
#[pgrx::pg_extern(schema = "pgtrickle")]
fn retention_layout_test(
    query: &str,
    source_oid: pgrx::pg_sys::Oid,
    key: &str,
    cutoff: i64,
) -> pgrx::JsonB {
    let tree = super::parser::parse_defining_query(query)
        .unwrap_or_else(|error| pgrx::error!("retention test parse: {}", error));
    let candidate = query_group_candidate(&tree, source_oid.to_u32(), key)
        .unwrap_or_else(|error| pgrx::error!("retention test candidate: {:?}", error));
    let cutoff = match candidate.source_type_oid {
        DATE_OID => RetentionBound::Date(
            i32::try_from(cutoff).unwrap_or_else(|_| pgrx::error!("invalid date cutoff")),
        ),
        TIMESTAMP_OID => RetentionBound::Timestamp(cutoff),
        TIMESTAMPTZ_OID => RetentionBound::Timestamptz(cutoff),
        _ => pgrx::error!("invalid retention source type"),
    };
    let partitions = validate_partition_layout(source_oid, key, &candidate, cutoff)
        .unwrap_or_else(|error| pgrx::error!("retention test layout: {}", error));
    let value = |bound| match bound {
        RetentionBound::Date(value) => i64::from(value),
        RetentionBound::Timestamp(value) | RetentionBound::Timestamptz(value) => value,
    };
    pgrx::JsonB(
        serde_json::json!(partitions.into_iter().map(|partition| serde_json::json!({
        "oid": partition.oid, "lower": value(partition.lower), "upper": value(partition.upper),
        "expired": partition.expired
    })).collect::<Vec<_>>()),
    )
}

#[cfg(all(feature = "e2e-unsafe-test-hooks", not(test)))]
#[pgrx::pg_extern(schema = "pgtrickle")]
fn retention_prepare_test(pgt_id: i64, target_oid: pgrx::pg_sys::Oid) -> pgrx::JsonB {
    let prepared = prepare_retention_cut(pgt_id, target_oid)
        .unwrap_or_else(|error| pgrx::error!("retention test prepare: {}", error));
    pgrx::JsonB(serde_json::json!({
        "pgt_id": prepared.pgt_id,
        "target_oid": prepared.target_oid,
        "range": prepared.range,
        "state_after_removal": prepared.state_after_removal,
        "safe_bound": prepared.safe_bound,
    }))
}

#[cfg(all(feature = "e2e-unsafe-test-hooks", not(test)))]
#[pgrx::pg_extern(schema = "pgtrickle")]
fn retention_detach_test(pgt_id: i64, target_oid: pgrx::pg_sys::Oid, fail: bool) -> pgrx::JsonB {
    use pgrx::prelude::*;
    let cut = prepare_retention_cut(pgt_id, target_oid)
        .unwrap_or_else(|error| pgrx::error!("retention test prepare: {}", error));
    let command = Spi::get_one_with_args::<String>(
        "SELECT pg_catalog.format('ALTER TABLE %I.%I DETACH PARTITION %I.%I', \
           pn.nspname, p.relname, cn.nspname, c.relname) \
         FROM pg_catalog.pg_class p JOIN pg_catalog.pg_namespace pn ON pn.oid = p.relnamespace \
         JOIN pg_catalog.pg_class c ON c.oid = $2 \
         JOIN pg_catalog.pg_namespace cn ON cn.oid = c.relnamespace WHERE p.oid = $1",
        &[pg_sys::Oid::from(cut.source_oid).into(), target_oid.into()],
    )
    .unwrap_or_else(|error| pgrx::error!("retention test DDL: {}", error))
    .unwrap_or_else(|| pgrx::error!("retention test source or target disappeared"));
    let expected = serde_json::to_value(&cut.state_after_removal)
        .unwrap_or_else(|error| pgrx::error!("retention test state: {}", error));
    with_prepared_retained_detach(cut, || {
        Spi::run(&command)
            .map_err(|error| crate::error::PgTrickleError::SpiError(error.to_string()))
    })
    .unwrap_or_else(|error| pgrx::error!("retention test detach: {}", error));
    if fail {
        pgrx::error!("intentional retention test rollback");
    }
    pgrx::JsonB(expected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dvm::parser::{AggExpr, Column};

    fn col(name: &str) -> Expr {
        Expr::ColumnRef {
            table_alias: Some("e".into()),
            column_name: name.into(),
        }
    }

    fn hour(name: &str, zone: Option<&str>) -> Expr {
        let mut args = vec![Expr::Literal("'hour'".into()), col(name)];
        if let Some(zone) = zone {
            args.push(Expr::Literal(format!("'{zone}'")));
        }
        Expr::FuncCall {
            func_name: "pg_catalog.date_trunc".into(),
            args,
        }
    }

    fn query(expression: Expr, group: Expr, type_oid: u32, nullable: bool) -> OpTree {
        let expression = if expression.to_sql() == group.to_sql() {
            Expr::ColumnRef {
                table_alias: None,
                column_name: group.output_name(),
            }
        } else {
            expression
        };
        OpTree::Project {
            expressions: vec![expression],
            aliases: vec!["bucket".into()],
            child: Box::new(OpTree::Aggregate {
                group_by: vec![group],
                aggregates: vec![AggExpr {
                    analyzed_argument: None,
                    function: AggFunc::CountStar,
                    argument: None,
                    alias: "n".into(),
                    is_distinct: false,
                    second_arg: None,
                    filter: None,
                    order_within_group: None,
                    statistical_support: None,
                }],
                child: Box::new(OpTree::Scan {
                    table_oid: 42,
                    table_name: "events".into(),
                    schema: "public".into(),
                    alias: "e".into(),
                    pk_columns: vec![],
                    columns: vec![Column {
                        name: "occurred".into(),
                        type_oid,
                        is_nullable: nullable,
                    }],
                }),
            }),
        }
    }

    #[test]
    fn admits_only_the_proven_group_key_candidates() {
        let cases = [
            (
                col("occurred"),
                col("occurred"),
                DATE_OID,
                RetentionBucket::Exact,
            ),
            (
                hour("occurred", None),
                hour("occurred", None),
                TIMESTAMP_OID,
                RetentionBucket::HourTimestamp,
            ),
            (
                hour("occurred", Some("UTC")),
                hour("occurred", Some("UTC")),
                TIMESTAMPTZ_OID,
                RetentionBucket::HourUtcTimestamptz,
            ),
        ];
        for (expression, group, type_oid, expected) in cases {
            assert_eq!(
                query_group_candidate(&query(expression, group, type_oid, false), 42, "occurred"),
                Ok(RetentionGroupCandidate {
                    output_column: "bucket".into(),
                    bucket: expected,
                    source_type_oid: type_oid,
                    source_oid: 42,
                    source_column: "occurred".into(),
                })
            );
        }
        let OpTree::Project { child, .. } =
            query(col("occurred"), col("occurred"), DATE_OID, false)
        else {
            unreachable!()
        };
        assert_eq!(
            query_group_candidate(&child, 42, "occurred"),
            Ok(RetentionGroupCandidate {
                output_column: "occurred".into(),
                bucket: RetentionBucket::Exact,
                source_type_oid: DATE_OID,
                source_oid: 42,
                source_column: "occurred".into(),
            })
        );
    }

    #[test]
    fn rejects_unproven_column_timezone_and_nullability() {
        assert_eq!(
            query_group_candidate(
                &query(
                    hour("other", None),
                    hour("other", None),
                    TIMESTAMP_OID,
                    false
                ),
                42,
                "occurred"
            ),
            Err(RetentionQueryError::MissingProjectedGroupKey)
        );
        let unqualified = Expr::FuncCall {
            func_name: "date_trunc".into(),
            args: vec![Expr::Literal("'hour'".into()), col("occurred")],
        };
        assert_eq!(
            query_group_candidate(
                &query(unqualified.clone(), unqualified, TIMESTAMP_OID, false),
                42,
                "occurred"
            ),
            Err(RetentionQueryError::MissingProjectedGroupKey)
        );
        assert_eq!(
            query_group_candidate(
                &query(
                    hour("occurred", None),
                    hour("occurred", None),
                    TIMESTAMPTZ_OID,
                    false
                ),
                42,
                "occurred"
            ),
            Err(RetentionQueryError::MissingProjectedGroupKey)
        );
        assert_eq!(
            query_group_candidate(
                &query(col("occurred"), col("occurred"), DATE_OID, true),
                42,
                "occurred"
            ),
            Err(RetentionQueryError::InvalidSourceKey)
        );
        assert_eq!(
            query_group_candidate(
                &query(col("occurred"), col("occurred"), DATE_OID, false),
                43,
                "occurred"
            ),
            Err(RetentionQueryError::InvalidSourceKey)
        );
        assert_eq!(
            query_group_candidate(
                &query(
                    hour("occurred", Some("Europe/Oslo")),
                    hour("occurred", Some("Europe/Oslo")),
                    TIMESTAMPTZ_OID,
                    false
                ),
                42,
                "occurred"
            ),
            Err(RetentionQueryError::MissingProjectedGroupKey)
        );
        assert_eq!(
            query_group_candidate(
                &query(col("occurred"), col("other"), DATE_OID, false),
                42,
                "occurred"
            ),
            Err(RetentionQueryError::MissingProjectedGroupKey)
        );
    }

    #[test]
    fn rejects_unsupported_aggregates_and_query_shapes() {
        let mut tree = query(col("occurred"), col("occurred"), DATE_OID, false);
        let OpTree::Project { child, .. } = &mut tree else {
            unreachable!()
        };
        let OpTree::Aggregate { aggregates, .. } = child.as_mut() else {
            unreachable!()
        };
        aggregates[0].is_distinct = true;
        assert_eq!(
            query_group_candidate(&tree, 42, "occurred"),
            Err(RetentionQueryError::UnsupportedAggregate)
        );

        let tree = OpTree::Filter {
            predicate: Expr::Literal("true".into()),
            child: Box::new(query(col("occurred"), col("occurred"), DATE_OID, false)),
        };
        assert_eq!(
            query_group_candidate(&tree, 42, "occurred"),
            Err(RetentionQueryError::UnsupportedShape)
        );

        let group = hour("occurred", None);
        let conflicting_name = group.output_name();
        let mut tree = query(group.clone(), group, TIMESTAMP_OID, false);
        let OpTree::Project { child, .. } = &mut tree else {
            unreachable!()
        };
        let OpTree::Aggregate { child, .. } = child.as_mut() else {
            unreachable!()
        };
        let OpTree::Scan { columns, .. } = child.as_mut() else {
            unreachable!()
        };
        columns.push(Column {
            name: conflicting_name,
            type_oid: TIMESTAMP_OID,
            is_nullable: false,
        });
        assert_eq!(
            query_group_candidate(&tree, 42, "occurred"),
            Err(RetentionQueryError::UnsupportedShape)
        );
    }

    #[test]
    fn range_checks_half_open_bounds_alignment_and_types() {
        use RetentionBound::{Date, Timestamp, Timestamptz};
        let hour = 3_600_000_000;
        let candidate = |bucket, source_type_oid| RetentionGroupCandidate {
            output_column: "bucket".into(),
            bucket,
            source_type_oid,
            source_oid: 42,
            source_column: "occurred".into(),
        };
        let date = candidate(RetentionBucket::Exact, DATE_OID);
        let timestamp = candidate(RetentionBucket::Exact, TIMESTAMP_OID);
        let hourly = candidate(RetentionBucket::HourTimestamp, TIMESTAMP_OID);
        let utc = candidate(RetentionBucket::HourUtcTimestamptz, TIMESTAMPTZ_OID);
        assert_eq!(range_is_expired(&date, Date(1), Date(2), Date(2)), Ok(true));
        assert_eq!(
            range_is_expired(&date, Date(1), Date(3), Date(2)),
            Ok(false)
        );
        assert_eq!(
            range_is_expired(&hourly, Timestamp(-hour), Timestamp(0), Timestamp(0)),
            Ok(true)
        );
        assert_eq!(
            range_is_expired(&hourly, Timestamp(-hour + 1), Timestamp(0), Timestamp(0)),
            Err(RetentionRangeError::UnalignedHour)
        );
        assert_eq!(
            range_is_expired(
                &utc,
                Timestamptz(0),
                Timestamptz(hour),
                Timestamptz(hour - 1)
            ),
            Ok(false)
        );
        assert_eq!(
            range_is_expired(&date, Date(i32::MIN), Date(2), Date(2)),
            Err(RetentionRangeError::InfiniteBound)
        );
        assert_eq!(
            range_is_expired(&timestamp, Timestamp(0), Timestamp(i64::MAX), Timestamp(0)),
            Err(RetentionRangeError::InfiniteBound)
        );
        assert_eq!(
            range_is_expired(&date, Date(2), Date(2), Date(2)),
            Err(RetentionRangeError::InvalidRange)
        );
        assert_eq!(
            range_is_expired(&hourly, Date(1), Date(2), Date(2)),
            Err(RetentionRangeError::TypeMismatch)
        );
        assert_eq!(
            range_is_expired(&timestamp, Timestamp(0), Timestamptz(hour), Timestamp(hour)),
            Err(RetentionRangeError::TypeMismatch)
        );
    }

    #[test]
    fn retained_history_guard_fails_closed_for_unknown_state() {
        let mut state = sample_state(RetentionMode::Retain);
        assert_eq!(
            state_has_sealed_history(&serde_json::to_value(&state).unwrap()),
            Ok(false)
        );
        state
            .seal(SealedRange {
                lower: RetentionBound::Date(0),
                upper: RetentionBound::Date(1),
            })
            .unwrap();
        assert_eq!(
            state_has_sealed_history(&serde_json::to_value(&state).unwrap()),
            Ok(true)
        );
        state.version = 2;
        assert_eq!(
            state_has_sealed_history(&serde_json::to_value(&state).unwrap()),
            Err("invalid partition retention invariants")
        );
        assert_eq!(
            state_has_sealed_history(&serde_json::json!({"version":1,"mode":"RETAIN"})),
            Err("malformed partition retention state")
        );
    }

    fn sample_state(mode: RetentionMode) -> RetentionState {
        RetentionState {
            version: 1,
            mode,
            candidate: RetentionGroupCandidate {
                output_column: "d".into(),
                bucket: RetentionBucket::Exact,
                source_type_oid: DATE_OID,
                source_oid: 42,
                source_column: "d".into(),
            },
            cutoff: RetentionBound::Date(30),
            query_hash: 7,
            sealed: vec![],
        }
    }

    #[test]
    fn sealing_is_sorted_idempotent_and_rejects_overlap_and_cutoff_regression() {
        use RetentionBound::Date;
        let mut state = sample_state(RetentionMode::Retain);
        let first = SealedRange {
            lower: Date(10),
            upper: Date(20),
        };
        let earlier = SealedRange {
            lower: Date(0),
            upper: Date(10),
        };
        assert_eq!(state.seal(first), Ok(true));
        assert_eq!(state.seal(first), Ok(false));
        assert_eq!(state.seal(earlier), Ok(true));
        assert_eq!(state.sealed, vec![earlier, first]);
        assert_eq!(
            state.seal(SealedRange {
                lower: Date(19),
                upper: Date(21)
            }),
            Err(RetentionStateError::Overlap)
        );
        assert_eq!(
            state.seal(SealedRange {
                lower: Date(20),
                upper: Date(31)
            }),
            Err(RetentionStateError::NotExpired)
        );
        assert_eq!(
            state.advance_cutoff(Date(29)),
            Err(RetentionStateError::CutoffRegression)
        );
        assert_eq!(state.advance_cutoff(Date(40)), Ok(()));
        assert_eq!(state.cutoff, Date(40));
        assert_eq!(
            state.advance_cutoff(Date(i32::MAX)),
            Err(RetentionStateError::InvalidState)
        );
        assert_eq!(state.cutoff, Date(40));
        let encoded = serde_json::to_value(&state).unwrap();
        assert_eq!(
            encoded,
            serde_json::json!({
                "version": 1, "mode": "RETAIN",
                "candidate": {"output_column": "d", "bucket": "EXACT", "source_type_oid": 1082,
                              "source_oid": 42, "source_column": "d"},
                "cutoff": {"kind": "DATE", "value": 40}, "query_hash": 7,
                "sealed": [
                    {"lower": {"kind": "DATE", "value": 0}, "upper": {"kind": "DATE", "value": 10}},
                    {"lower": {"kind": "DATE", "value": 10}, "upper": {"kind": "DATE", "value": 20}}
                ]
            })
        );
        assert_eq!(
            serde_json::from_value::<RetentionState>(encoded).unwrap(),
            state
        );
        assert_eq!(
            sample_state(RetentionMode::Trim).seal(first),
            Err(RetentionStateError::TrimCannotSeal)
        );
    }
}
