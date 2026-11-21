//! Query-side proofs of partition-local aggregate groups.
//!
//! A candidate is not sufficient to authorize retention: callers must also
//! validate the partition key, every bound, and the retention lifecycle.

use super::parser::{AggFunc, Expr, OpTree};
#[cfg(not(test))]
use pgrx::pg_sys;
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionQueryError {
    UnsupportedShape(String),
    UnsupportedAggregate(String),
    FloatingPointSum,
    InvalidSourceKey(String),
    MissingProjectedGroupKey(String),
}

impl std::fmt::Display for RetentionQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedShape(reason)
            | Self::UnsupportedAggregate(reason)
            | Self::InvalidSourceKey(reason)
            | Self::MissingProjectedGroupKey(reason) => f.write_str(reason),
            Self::FloatingPointSum => write!(
                f,
                "floating-point SUM is unsupported; restore complete raw history and use a numeric aggregate"
            ),
        }
    }
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingRetentionDetach {
    pub parent_oid: u32,
    pub target_oid: u32,
    pub range: SealedRange,
}

#[cfg(not(test))]
pub(crate) fn concurrent_detach_reason(parent_oid: u32, target_oid: u32) -> String {
    format!("RETENTION_DETACH:{parent_oid}:{target_oid}")
}

#[cfg(not(test))]
fn retention_catchup(
    st: &crate::catalog::StreamTableMeta,
    source_oid: pg_sys::Oid,
    safe_bound: &str,
    candidate: &RetentionGroupCandidate,
) -> Result<crate::api::refresh_ops::ManualRefreshResult, crate::error::PgTrickleError> {
    let partition_key =
        retention_pruning_key(st.st_partition_key.as_deref(), &candidate.output_column)
            .map(|key| (st.pgt_id, key));
    with_retention_index_scans(|| {
        crate::refresh::with_refresh_context(
            crate::refresh::RefreshContext::retention(safe_bound, partition_key),
            || {
                crate::api::refresh_ops::execute_manual_refresh(
                    st,
                    &st.pgt_schema,
                    &st.pgt_name,
                    &[source_oid],
                )
            },
        )
    })
}

fn retention_pruning_key(partition_spec: Option<&str>, group_column: &str) -> Option<String> {
    // The validated retention group key is non-null and immutable for a group identity.
    let keys = crate::api::parse_partition_key_columns(partition_spec?);
    (keys.len() == 1 && keys[0] == group_column).then(|| group_column.to_string())
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<PendingRetentionDetach>,
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
        if let Some(pending) = self.pending {
            if pending.parent_oid == 0
                || pending.target_oid == 0
                || pending.parent_oid == pending.target_oid
            {
                return Err(RetentionStateError::InvalidState);
            }
            if !range_is_expired(
                &self.candidate,
                pending.range.lower,
                pending.range.upper,
                self.cutoff,
            )
            .map_err(RetentionStateError::InvalidRange)?
            {
                return Err(RetentionStateError::NotExpired);
            }
            if self.sealed.iter().any(|old| {
                bound_value(pending.range.lower) < bound_value(old.upper)
                    && bound_value(old.lower) < bound_value(pending.range.upper)
            }) {
                return Err(RetentionStateError::Overlap);
            }
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
    Ok(
        state.pending.is_some()
            || (state.mode == RetentionMode::Retain && !state.sealed.is_empty()),
    )
}

#[cfg(not(test))]
fn unreconstructible_history_reason(
    pgt_id: i64,
) -> Result<Option<&'static str>, crate::error::PgTrickleError> {
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
                if state.0.get("pending").is_some_and(|value| !value.is_null()) {
                    return Ok(Some(
                        "a pending retention detach must finish before rebuilding or changing the policy; if interrupted, run DETACH PARTITION FINALIZE",
                    ));
                }
                return Ok(Some(
                    "retained partition history cannot be reconstructed from the current source; \
                     a whole-query rebuild or restore would discard it",
                ));
            }
        }
        Ok(None)
    })
}

/// Fail closed when a whole-query operation would discard retained history.
#[cfg(not(test))]
pub(crate) fn ensure_history_reconstructible(
    pgt_id: i64,
) -> Result<(), crate::error::PgTrickleError> {
    if let Some(reason) = unreconstructible_history_reason(pgt_id)? {
        return Err(crate::error::PgTrickleError::InvalidArgument(reason.into()));
    }
    Ok(())
}

#[cfg(not(test))]
pub(crate) fn has_unreconstructible_history(
    pgt_id: i64,
) -> Result<bool, crate::error::PgTrickleError> {
    Ok(unreconstructible_history_reason(pgt_id)?.is_some())
}

#[cfg(test)]
pub(crate) fn has_unreconstructible_history(
    _pgt_id: i64,
) -> Result<bool, crate::error::PgTrickleError> {
    Ok(false)
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
    static PREPARED_DETACH: std::cell::RefCell<Vec<PreparedRetentionCut>> = const { std::cell::RefCell::new(Vec::new()) };
    static PREPARED_CALLBACK_XID: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(not(test))]
fn clear_prepared_detaches() {
    PREPARED_DETACH.with(|slot| {
        if let Ok(mut cuts) = slot.try_borrow_mut() {
            cuts.clear();
        }
    });
    PREPARED_CALLBACK_XID.with(|xid| xid.set(0));
}

#[cfg(not(test))]
fn register_prepared_detach(cut: PreparedRetentionCut) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    let register = PREPARED_CALLBACK_XID.with(|xid| {
        if xid.get() == cut.transaction_id {
            false
        } else {
            xid.set(cut.transaction_id);
            true
        }
    });
    if register {
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Abort, clear_prepared_detaches);
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::Commit, clear_prepared_detaches);
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::PreCommit, || {
            PREPARED_DETACH.with(|slot| {
                if !slot.try_borrow().is_ok_and(|cuts| cuts.is_empty()) {
                    pgrx::error!("pg_trickle: a prepared partition detach was not finalized");
                }
            });
        });
        pgrx::register_xact_callback(pgrx::PgXactCallbackEvent::PrePrepare, || {
            PREPARED_DETACH.with(|slot| {
                if !slot.try_borrow().is_ok_and(|cuts| cuts.is_empty()) {
                    pgrx::error!("pg_trickle: a prepared partition detach was not finalized");
                }
            });
        });
        pgrx::register_subxact_callback(pgrx::PgSubXactCallbackEvent::AbortSub, |subid, _| {
            PREPARED_DETACH.with(|slot| {
                if let Ok(mut cuts) = slot.try_borrow_mut() {
                    cuts.retain(|cut| cut.subtransaction_id != subid);
                }
            });
        });
    }
    PREPARED_DETACH.with(|slot| {
        let mut cuts = slot.borrow_mut();
        if cuts.iter().any(|existing| {
            existing.pgt_id == cut.pgt_id
                && existing.source_oid == cut.source_oid
                && existing.target_oid == cut.target_oid
        }) {
            return Err(PgTrickleError::InvalidArgument(
                "partition cut is already prepared".into(),
            ));
        }
        cuts.push(cut);
        Ok(())
    })
}

#[cfg(not(test))]
fn prepared_detach_exists(pgt_id: i64, source_oid: u32, target_oid: u32) -> bool {
    // SAFETY: PostgreSQL exposes these identifiers for the current backend transaction.
    let (xid, subid) = unsafe {
        (
            pg_sys::GetTopFullTransactionIdIfAny().value,
            pg_sys::GetCurrentSubTransactionId(),
        )
    };
    PREPARED_DETACH.with(|slot| {
        slot.borrow().iter().any(|cut| {
            cut.pgt_id == pgt_id
                && cut.source_oid == source_oid
                && cut.target_oid == target_oid
                && cut.transaction_id == xid
                && cut.subtransaction_id == subid
        })
    })
}

#[cfg(not(test))]
fn retention_policies_for_source(
    source_oid: pg_sys::Oid,
) -> Result<Vec<(i64, RetentionState)>, crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    Spi::connect(|client| {
        let rows = client.select(
            "SELECT pgt_id, column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies \
             WHERE source_relid = $1 AND source_type = 'TABLE' AND column_snapshot ? 'partition_retention' \
             ORDER BY pgt_id",
            None, &[source_oid.into()],
        ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        let mut policies = Vec::new();
        for row in rows {
            let id = row
                .get::<i64>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InternalError("missing retention stream ID".into())
                })?;
            let value = row
                .get::<pgrx::JsonB>(2)
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
            policies.push((id, state));
        }
        Ok::<_, PgTrickleError>(policies)
    })
}

/// Persist a protected cut before PostgreSQL commits the first detach phase.
#[cfg(not(test))]
pub(crate) fn prepare_concurrent_retention_detaches(
    source_oid: pg_sys::Oid,
    parent_oid: pg_sys::Oid,
    target_oid: pg_sys::Oid,
) -> Result<Vec<i64>, crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let mut prepared = Vec::new();
    for (pgt_id, mut state) in retention_policies_for_source(source_oid)? {
        if let Some(pending) = state.pending {
            if pending.parent_oid != parent_oid.to_u32()
                || pending.target_oid != target_oid.to_u32()
            {
                return Err(PgTrickleError::InvalidArgument(
                    "another retention detach is pending for this stream".into(),
                ));
            }
            prepared.push(pgt_id);
            continue;
        }
        let partition = match resolve_retention_partition(
            source_oid,
            target_oid,
            &state.candidate,
            state.cutoff,
            None,
        ) {
            Ok(partition) => partition,
            Err(PgTrickleError::InvalidArgument(_)) if state.mode == RetentionMode::Trim => {
                continue;
            }
            Err(error) => return Err(error),
        };
        if partition.is_none_or(|partition| !partition.expired) {
            if state.mode == RetentionMode::Trim {
                continue;
            }
            return Err(PgTrickleError::InvalidArgument(
                "RETAIN requires an aligned partition wholly below its retention cutoff".into(),
            ));
        }
        let cut = prepare_retention_cut(pgt_id, target_oid)?;
        let before = pgrx::JsonB(
            serde_json::to_value(&state)
                .map_err(|e| PgTrickleError::InternalError(e.to_string()))?,
        );
        state.pending = Some(PendingRetentionDetach {
            parent_oid: parent_oid.to_u32(),
            target_oid: target_oid.to_u32(),
            range: cut.range,
        });
        state.validate().map_err(|e| {
            PgTrickleError::InvalidArgument(format!("invalid pending retention detach: {e:?}"))
        })?;
        let after = pgrx::JsonB(
            serde_json::to_value(&state)
                .map_err(|e| PgTrickleError::InternalError(e.to_string()))?,
        );
        let updated = Spi::get_one_with_args::<i64>(
            "UPDATE pgtrickle.pgt_dependencies SET column_snapshot = \
             pg_catalog.jsonb_set(column_snapshot, '{partition_retention}', $3, true) \
             WHERE pgt_id = $1 AND source_relid = $2 AND column_snapshot->'partition_retention' = $4 \
             RETURNING pgt_id",
            &[pgt_id.into(), source_oid.into(), after.into(), before.into()],
        ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        if updated != Some(pgt_id) {
            return Err(PgTrickleError::InvalidArgument(
                "retention state changed during concurrent detach preparation".into(),
            ));
        }
        prepared.push(pgt_id);
    }
    Ok(prepared)
}

/// Replay the pending CDC and complete the cut in PostgreSQL's second transaction.
#[cfg(not(test))]
pub(crate) fn finish_concurrent_retention_detach(
    pgt_id: i64,
    source_oid: pg_sys::Oid,
) -> Result<bool, crate::error::PgTrickleError> {
    use crate::catalog::{StreamTableMeta, compute_defining_query_hash};
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;

    let Some((_, mut state)) = retention_policies_for_source(source_oid)?
        .into_iter()
        .find(|(id, _)| *id == pgt_id)
    else {
        return Ok(false);
    };
    let Some(pending) = state.pending else {
        return Ok(false);
    };
    let detached = Spi::get_one_with_args::<bool>(
        "SELECT NOT EXISTS (WITH RECURSIVE ancestors(oid) AS ( \
         SELECT inhparent FROM pg_catalog.pg_inherits WHERE inhrelid = $2 \
         UNION ALL SELECT i.inhparent FROM pg_catalog.pg_inherits i JOIN ancestors a ON i.inhrelid = a.oid \
         ) SELECT 1 FROM ancestors WHERE oid = $1)",
        &[source_oid.into(), pg_sys::Oid::from(pending.target_oid).into()],
    ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?.unwrap_or(false);
    if !detached {
        return Err(PgTrickleError::InvalidArgument(
            "pending retention partition is still attached".into(),
        ));
    }
    crate::api::alter::lock_alter_target(pgt_id)?;
    let mut st = StreamTableMeta::get_by_id(pgt_id)?
        .ok_or_else(|| PgTrickleError::NotFound(format!("stream table {pgt_id}")))?;
    if st.status != crate::dag::StStatus::Suspended
        || st.needs_reinit
        || state.query_hash != compute_defining_query_hash(&st.defining_query)
    {
        return Err(PgTrickleError::InvalidArgument(
            "pending retention stream changed or requires reinitialization".into(),
        ));
    }
    validate_retention_storage(&st, source_oid, &state.candidate, state.mode, false)?;
    let candidate = crate::refresh::with_stream_owner(&st, || {
        let tree = super::parser::parse_defining_query(&st.defining_query)?;
        let candidate =
            query_group_candidate(&tree, source_oid.to_u32(), &state.candidate.source_column)
                .map_err(|e| {
                    PgTrickleError::InvalidArgument(format!(
                        "retention query is no longer eligible: {e}"
                    ))
                })?;
        validate_retention_group_collations(&tree, source_oid)?;
        validate_builtin_aggregates(&st.defining_query, &tree)?;
        Ok(candidate)
    })?;
    if candidate != state.candidate {
        return Err(PgTrickleError::InvalidArgument(
            "retention group mapping changed during concurrent detach".into(),
        ));
    }
    crate::cdc::lock_source_relations(&[source_oid])?;
    ensure_durable_partition_tree(source_oid)?;
    ensure_retention_capture(pgt_id)?;
    ensure_no_live_sealed_overlap(source_oid, &state, None)?;
    let reason = concurrent_detach_reason(pending.parent_oid, pending.target_oid);
    let activated = Spi::get_one_with_args::<i64>(
        "WITH activated AS (UPDATE pgtrickle.pgt_stream_tables SET status = 'ACTIVE' \
         WHERE pgt_id = $1 AND status = 'SUSPENDED' AND refresh_reason = $2 RETURNING pgt_id) \
         SELECT (SELECT pgt_id FROM activated)",
        &[pgt_id.into(), reason.as_str().into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    if activated != Some(pgt_id) {
        return Err(PgTrickleError::InvalidArgument(
            "pending retention stream suspension changed during concurrent detach".into(),
        ));
    }
    st.status = crate::dag::StStatus::Active;
    let safe_bound = crate::cdc::get_current_wal_lsn()?;
    let refresh = retention_catchup(&st, source_oid, &safe_bound, &candidate)?;
    if !matches!(refresh.action.as_str(), "DIFFERENTIAL" | "NO_DATA") {
        return Err(PgTrickleError::InvalidArgument(format!(
            "pending retention replay performed unexpected refresh mode {}",
            refresh.action
        )));
    }
    let before = pgrx::JsonB(
        serde_json::to_value(&state).map_err(|e| PgTrickleError::InternalError(e.to_string()))?,
    );
    state.pending = None;
    let after = pgrx::JsonB(
        serde_json::to_value(&state).map_err(|e| PgTrickleError::InternalError(e.to_string()))?,
    );
    let updated = Spi::get_one_with_args::<i64>(
        "UPDATE pgtrickle.pgt_dependencies SET column_snapshot = \
         pg_catalog.jsonb_set(column_snapshot, '{partition_retention}', $3, true) \
         WHERE pgt_id = $1 AND source_relid = $2 AND column_snapshot->'partition_retention' = $4 \
         RETURNING pgt_id",
        &[
            pgt_id.into(),
            source_oid.into(),
            after.into(),
            before.into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    if updated != Some(pgt_id) {
        return Err(PgTrickleError::InvalidArgument(
            "pending retention state changed during replay".into(),
        ));
    }
    let mut after_removal = state.clone();
    if state.mode == RetentionMode::Retain {
        after_removal.seal(pending.range).map_err(|e| {
            PgTrickleError::InvalidArgument(format!("cannot seal retained range: {e:?}"))
        })?;
    }
    // SAFETY: PostgreSQL exposes these identifiers for the current backend transaction.
    let (transaction_id, subtransaction_id) = unsafe {
        (
            pg_sys::GetTopFullTransactionId().value,
            pg_sys::GetCurrentSubTransactionId(),
        )
    };
    let cut = PreparedRetentionCut {
        pgt_id,
        source_oid: source_oid.to_u32(),
        target_oid: pending.target_oid,
        range: pending.range,
        state_before_removal: state,
        state_after_removal: after_removal,
        safe_bound,
        transaction_id,
        subtransaction_id,
    };
    match cut.state_after_removal.mode {
        RetentionMode::Retain => finalize_retained_cut(cut)?,
        RetentionMode::Trim => finalize_trimmed_cut(cut)?,
    }
    Spi::run_with_args(
        "UPDATE pgtrickle.pgt_stream_tables SET refresh_reason = NULL, refresh_reason_detail = NULL \
         WHERE pgt_id = $1 AND refresh_reason = $2",
        &[pgt_id.into(), reason.as_str().into()],
    ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    Ok(true)
}

/// Prepare eligible configured consumers before PostgreSQL detaches the child.
#[cfg(not(test))]
pub(crate) fn prepare_automatic_detaches(
    source_oid: pg_sys::Oid,
    target_oid: pg_sys::Oid,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    let policies = retention_policies_for_source(source_oid)?;
    // Refreshes and configuration take stream locks before source locks.
    for (pgt_id, _) in &policies {
        crate::api::alter::lock_alter_target(*pgt_id)?;
    }
    for (pgt_id, state) in policies {
        if state.pending.is_some() {
            return Err(PgTrickleError::InvalidArgument(
                "cannot remove another partition while a retention detach is pending".into(),
            ));
        }
        if prepared_detach_exists(pgt_id, source_oid.to_u32(), target_oid.to_u32()) {
            continue;
        }
        let partition = match resolve_retention_partition(
            source_oid,
            target_oid,
            &state.candidate,
            state.cutoff,
            None,
        ) {
            Ok(partition) => partition,
            Err(PgTrickleError::InvalidArgument(_)) if state.mode == RetentionMode::Trim => {
                continue;
            }
            Err(error) => return Err(error),
        };
        if partition.is_none_or(|partition| !partition.expired) {
            if state.mode == RetentionMode::Trim {
                continue;
            }
            return Err(PgTrickleError::InvalidArgument(
                "RETAIN requires an aligned partition wholly below its retention cutoff".into(),
            ));
        }
        register_prepared_detach(prepare_retention_cut(pgt_id, target_oid)?)?;
    }
    Ok(())
}

#[cfg(not(test))]
struct PreparedDetachGuard;

#[cfg(not(test))]
impl Drop for PreparedDetachGuard {
    fn drop(&mut self) {
        PREPARED_DETACH.with(|slot| {
            slot.borrow_mut().clear();
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
        if !slot.borrow().is_empty() {
            return Err(PgTrickleError::InvalidArgument(
                "nested retention detach is not supported".into(),
            ));
        }
        Ok(())
    })?;
    register_prepared_detach(cut)?;
    let _guard = PreparedDetachGuard;
    let result = detach()?;
    let pending = PREPARED_DETACH.with(|slot| !slot.borrow().is_empty());
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
    target_oid: Option<pgrx::pg_sys::Oid>,
) -> Result<bool, crate::error::PgTrickleError> {
    // SAFETY: PostgreSQL exposes these identifiers for the current backend transaction.
    let (xid, subid) = unsafe {
        (
            pg_sys::GetTopFullTransactionIdIfAny().value,
            pg_sys::GetCurrentSubTransactionId(),
        )
    };
    let cut = PREPARED_DETACH.with(|slot| {
        let mut cuts = slot.borrow_mut();
        cuts.iter()
            .position(|cut| {
                cut.pgt_id == pgt_id
                    && cut.source_oid == source_oid.to_u32()
                    && target_oid.is_none_or(|target| cut.target_oid == target.to_u32())
                    && cut.transaction_id == xid
                    && cut.subtransaction_id == subid
            })
            .map(|position| cuts.remove(position))
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
    _target_oid: Option<pgrx::pg_sys::Oid>,
) -> Result<bool, crate::error::PgTrickleError> {
    Ok(false)
}

/// Finalize all prepared cuts for this DROP before ancestry snapshots change.
#[cfg(not(test))]
pub(crate) fn finish_prepared_drops(
    dropped: &[pg_sys::Oid],
) -> Result<std::collections::HashSet<(i64, u32, u32)>, crate::error::PgTrickleError> {
    // SAFETY: PostgreSQL exposes these identifiers for the current backend transaction.
    let (xid, subid) = unsafe {
        (
            pg_sys::GetTopFullTransactionIdIfAny().value,
            pg_sys::GetCurrentSubTransactionId(),
        )
    };
    let candidates = PREPARED_DETACH.with(|slot| {
        slot.borrow()
            .iter()
            .filter(|cut| {
                cut.transaction_id == xid
                    && cut.subtransaction_id == subid
                    && dropped.iter().any(|oid| oid.to_u32() == cut.target_oid)
            })
            .map(|cut| (cut.pgt_id, cut.source_oid, cut.target_oid))
            .collect::<Vec<_>>()
    });
    let mut finalized = std::collections::HashSet::new();
    for (id, source, target) in candidates {
        if finish_prepared_retained_detach(
            id,
            pg_sys::Oid::from(source),
            Some(pg_sys::Oid::from(target)),
        )? {
            finalized.insert((id, source, target));
        }
    }
    Ok(finalized)
}

#[cfg(test)]
pub(crate) fn finish_prepared_drops(
    _dropped: &[pgrx::pg_sys::Oid],
) -> Result<std::collections::HashSet<(i64, u32, u32)>, crate::error::PgTrickleError> {
    Ok(std::collections::HashSet::new())
}

/// Validate the stream and resolve the single trigger-CDC source.
#[cfg(not(test))]
pub(crate) fn retention_source(
    st: &crate::catalog::StreamTableMeta,
) -> Result<pg_sys::Oid, crate::error::PgTrickleError> {
    use crate::catalog::{CdcMode, StDependency};
    use crate::dag::{RefreshMode, StStatus};
    use crate::error::PgTrickleError;
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
    let dependencies = StDependency::get_for_st(st.pgt_id)?;
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
    Ok(dependency.source_relid)
}

/// Prevent untracked TRUNCATE and writes during lossy CDC pause.
#[cfg(not(test))]
pub(crate) fn install_partition_truncate_guards(
    source_oid: pg_sys::Oid,
) -> Result<(), crate::error::PgTrickleError> {
    crate::cdc::with_internal_trigger_ddl(|| install_partition_truncate_guards_inner(source_oid))
}

#[cfg(not(test))]
fn install_partition_truncate_guards_inner(
    source_oid: pg_sys::Oid,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let configured = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_dependencies WHERE source_relid = $1 \
         AND source_type = 'TABLE' AND column_snapshot ? 'partition_retention')",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);
    if !configured {
        return Ok(());
    }
    let name = crate::cdc::get_cdc_name_for_source(source_oid);
    let schema = crate::config::pg_trickle_change_buffer_schema();
    let function_name = format!("pg_trickle_retention_truncate_fn_{name}");
    let function = crate::sql_builder::qualified(&schema, &function_name);
    let trigger_name = format!("pg_trickle_retention_truncate_{name}");
    let trigger = crate::sql_builder::ident(&trigger_name);
    let capture_name = format!("pg_trickle_cdc_{name}");
    let capture_trigger = crate::sql_builder::ident(&capture_name);
    let capture_function = format!(
        "{}()",
        crate::sql_builder::qualified(&schema, &format!("pg_trickle_cdc_fn_{name}"))
    );
    let source = source_oid.to_u32();
    let sql = format!(
        "CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER \
         SET search_path = pg_catalog, pgtrickle, pg_temp AS $pgt$ \
         BEGIN \
           IF TG_OP = 'TRUNCATE' OR (pg_catalog.current_setting('pg_trickle.cdc_paused', true) = 'on' \
                  AND pg_catalog.current_setting('pg_trickle.cdc_capture_mode', true) IS DISTINCT FROM 'hold') THEN \
             IF EXISTS (SELECT 1 FROM pgtrickle.pgt_dependencies WHERE source_relid = {source}::oid \
                        AND source_type = 'TABLE' AND column_snapshot ? 'partition_retention') \
                AND (TG_RELID = {source}::oid OR EXISTS (WITH RECURSIVE ancestors(oid) AS ( \
                      SELECT inhparent FROM pg_catalog.pg_inherits WHERE inhrelid = TG_RELID \
                      UNION ALL SELECT i.inhparent FROM pg_catalog.pg_inherits i JOIN ancestors a ON i.inhrelid = a.oid \
                    ) SELECT 1 FROM ancestors WHERE oid = {source}::oid)) THEN \
               IF TG_OP = 'TRUNCATE' THEN \
                 RAISE EXCEPTION 'pg_trickle: TRUNCATE of a partition with retention is not supported' USING ERRCODE = '0A000'; \
               END IF; \
               RAISE EXCEPTION 'pg_trickle: writes to a retention source are blocked while CDC is paused in discard mode' USING ERRCODE = '0A000'; \
             END IF; \
           END IF; \
           RETURN NULL; \
         END; $pgt$"
    );
    Spi::run(&sql).map_err(|e| PgTrickleError::SpiError(e.to_string()))?; // nosemgrep: rust.spi.run.dynamic-format — identifiers are quoted and source is a catalog OID.
    let children: Vec<(pg_sys::Oid, String)> = Spi::connect(|client| {
        let rows = client
            .select(
                "SELECT c.oid, pg_catalog.format('%I.%I', n.nspname, c.relname) \
             FROM pg_catalog.pg_partition_tree($1::regclass) t \
             JOIN pg_catalog.pg_class c ON c.oid = t.relid \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             ORDER BY c.oid",
                None,
                &[source_oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        rows.map(|row| {
            let oid = row
                .get::<pg_sys::Oid>(1)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("partition OID is missing".into()))?;
            let name = row
                .get::<String>(2)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InternalError("partition name is missing".into()))?;
            Ok::<_, PgTrickleError>((oid, name))
        })
        .collect::<Result<Vec<_>, PgTrickleError>>()
    })?;
    let signature = format!("{function}()");
    for (oid, table) in children {
        let capture = Spi::get_one_with_args::<String>(
            "SELECT (SELECT tgenabled::text FROM pg_catalog.pg_trigger WHERE tgrelid = $1 AND tgname = $2 \
             AND tgfoid = pg_catalog.to_regprocedure($3) AND tgtype = 29 AND tgqual IS NULL AND tgnargs = 0 \
             AND tgconstraint = 0 AND NOT tgdeferrable AND tgenabled IN ('O','A'))",
            &[oid.into(), capture_name.as_str().into(), capture_function.as_str().into()],
        ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        match capture.as_deref() {
            Some("A") => {}
            // nosemgrep: rust.spi.run.dynamic-format — table and trigger identifiers are quoted.
            Some("O") => Spi::run(&format!(
                "ALTER TABLE ONLY {table} ENABLE ALWAYS TRIGGER {capture_trigger}"
            ))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?, // nosemgrep: rust.spi.run.dynamic-format — identifiers are catalog-quoted.
            _ => {
                return Err(PgTrickleError::InvalidArgument(
                    "partition retention row CDC trigger is incomplete".into(),
                ));
            }
        }
        let existing = Spi::get_one_with_args::<bool>(
            "SELECT pg_catalog.bool_and(tgfoid = pg_catalog.to_regprocedure($3) \
                     AND tgtype = 62 AND tgenabled = 'A' AND tgqual IS NULL AND tgnargs = 0 \
                     AND tgconstraint = 0 AND NOT tgdeferrable) \
             FROM pg_catalog.pg_trigger WHERE tgrelid = $1 AND tgname = $2",
            &[
                oid.into(),
                trigger_name.as_str().into(),
                signature.as_str().into(),
            ],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        match existing {
            Some(true) => continue,
            Some(false) => {
                return Err(PgTrickleError::InvalidArgument(
                    "partition retention source guard was changed or disabled".into(),
                ));
            }
            None => {}
        }
        // nosemgrep: rust.spi.run.dynamic-format — all identifiers are quoted.
        Spi::run(&format!("CREATE TRIGGER {trigger} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION {function}()"))
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?; // nosemgrep: rust.spi.run.dynamic-format — identifiers are catalog-quoted.
        // nosemgrep: rust.spi.run.dynamic-format — table and trigger identifiers are quoted.
        Spi::run(&format!(
            "ALTER TABLE ONLY {table} ENABLE ALWAYS TRIGGER {trigger}"
        ))
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?; // nosemgrep: rust.spi.run.dynamic-format — identifiers are catalog-quoted.
    }
    Ok(())
}

#[cfg(not(test))]
pub(crate) fn remove_partition_truncate_guards_if_unused(
    source_oid: pg_sys::Oid,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let configured = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_dependencies WHERE source_relid = $1 \
         AND source_type = 'TABLE' AND column_snapshot ? 'partition_retention')",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(true);
    if !configured {
        drop_partition_truncate_guard(source_oid)?;
    }
    Ok(())
}

#[cfg(not(test))]
pub(crate) fn drop_partition_truncate_guard(
    source_oid: pg_sys::Oid,
) -> Result<(), crate::error::PgTrickleError> {
    crate::cdc::with_internal_trigger_ddl(|| drop_partition_truncate_guard_inner(source_oid))
}

#[cfg(not(test))]
fn drop_partition_truncate_guard_inner(
    source_oid: pg_sys::Oid,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let name = format!(
        "pg_trickle_retention_truncate_fn_{}",
        crate::cdc::get_cdc_name_for_source(source_oid)
    );
    let function =
        crate::sql_builder::qualified(&crate::config::pg_trickle_change_buffer_schema(), &name);
    // nosemgrep: rust.spi.run.dynamic-format — function is a quoted internal identifier.
    Spi::run(&format!("DROP FUNCTION IF EXISTS {function}() CASCADE"))
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?; // nosemgrep: rust.spi.run.dynamic-format — identifier is quoted from internal CDC metadata.
    Ok(())
}

/// Group identities must use equality compatible with the typed row encoding.
#[cfg(not(test))]
pub(crate) fn validate_retention_group_collations(
    tree: &OpTree,
    source_oid: pg_sys::Oid,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let aggregate = match tree {
        OpTree::Project { child, .. } => child.as_ref(),
        tree => tree,
    };
    let OpTree::Aggregate { group_by, .. } = aggregate else {
        return Err(PgTrickleError::InvalidArgument(
            "partition retention requires a supported grouped aggregate".into(),
        ));
    };
    for group in group_by {
        let Expr::ColumnRef { column_name, .. } = group else {
            continue;
        };
        let deterministic = Spi::get_one_with_args::<bool>(
            "SELECT a.attcollation OPERATOR(pg_catalog.=) 0 \
                    OR COALESCE(c.collisdeterministic, false) \
             FROM pg_catalog.pg_attribute a \
             LEFT JOIN pg_catalog.pg_collation c ON c.oid OPERATOR(pg_catalog.=) a.attcollation \
             WHERE a.attrelid OPERATOR(pg_catalog.=) $1 \
               AND a.attname OPERATOR(pg_catalog.=) $2 \
               AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped",
            &[source_oid.into(), column_name.as_str().into()],
        )
        .map_err(|error| PgTrickleError::SpiError(error.to_string()))?
        .ok_or_else(|| {
            PgTrickleError::InvalidArgument(format!(
                "cannot inspect retention grouping column {column_name}"
            ))
        })?;
        if !deterministic {
            return Err(PgTrickleError::InvalidArgument(format!(
                "partition retention grouping column {column_name} uses a nondeterministic collation; use a deterministic collation before configuring or refreshing retention"
            )));
        }
    }
    Ok(())
}

/// Reject refresh when partition CDC or its TRUNCATE protection is incomplete.
#[cfg(not(test))]
pub(crate) fn ensure_retention_capture(pgt_id: i64) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let source = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT (SELECT source_relid FROM pgtrickle.pgt_dependencies WHERE pgt_id = $1 \
         AND source_type = 'TABLE' AND column_snapshot ? 'partition_retention' LIMIT 1)",
        &[pgt_id.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    let Some(source) = source else {
        return Ok(());
    };
    let st = crate::catalog::StreamTableMeta::get_by_id(pgt_id)?
        .ok_or_else(|| PgTrickleError::NotFound(format!("stream table {pgt_id}")))?;
    crate::refresh::with_stream_owner(&st, || {
        let tree = super::parser::parse_defining_query(&st.defining_query)?;
        validate_retention_group_collations(&tree, source)?;
        if has_floating_sum(&tree) {
            return Err(PgTrickleError::InvalidArgument(
                "partition retention does not support floating-point SUM; restore complete raw history and use a numeric aggregate before configuring or refreshing retention".into(),
            ));
        }
        Ok(())
    })?;
    let name = crate::cdc::get_cdc_name_for_source(source);
    let capture_name = format!("pg_trickle_cdc_{name}");
    let guard_name = format!("pg_trickle_retention_truncate_{name}");
    let guard_function = format!(
        "{}()",
        crate::sql_builder::qualified(
            &crate::config::pg_trickle_change_buffer_schema(),
            &format!("pg_trickle_retention_truncate_fn_{name}"),
        )
    );
    let capture_function = format!(
        "{}()",
        crate::sql_builder::qualified(
            &crate::config::pg_trickle_change_buffer_schema(),
            &format!("pg_trickle_cdc_fn_{name}"),
        )
    );
    let valid = Spi::get_one_with_args::<bool>(
        "WITH RECURSIVE tree(relid) AS (SELECT $1::oid UNION \
           SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN tree p ON i.inhparent = p.relid), \
         root AS (SELECT tgfoid FROM pg_catalog.pg_trigger \
           WHERE tgrelid = $1 AND tgname = $2 AND tgtype = 29 AND tgenabled = 'A' \
             AND tgfoid = pg_catalog.to_regprocedure($5) AND tgqual IS NULL AND tgnargs = 0 \
             AND tgconstraint = 0 AND NOT tgdeferrable) \
         SELECT EXISTS (SELECT 1 FROM root) AND NOT EXISTS ( \
           SELECT 1 FROM tree p \
           WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t, root r \
                     WHERE t.tgrelid = p.relid AND t.tgname = $2 AND t.tgfoid = r.tgfoid \
                       AND t.tgtype = 29 AND t.tgenabled = 'A' AND t.tgqual IS NULL AND t.tgnargs = 0 \
                       AND t.tgconstraint = 0 AND NOT t.tgdeferrable \
                       AND (p.relid = $1 OR t.tgparentid <> 0)) \
              OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t \
                     WHERE t.tgrelid = p.relid AND t.tgname = $3 \
                       AND t.tgfoid = pg_catalog.to_regprocedure($4) AND t.tgtype = 62 \
                       AND t.tgenabled = 'A' AND t.tgqual IS NULL AND t.tgnargs = 0 \
                       AND t.tgconstraint = 0 AND NOT t.tgdeferrable) \
         )",
        &[
            source.into(),
            capture_name.as_str().into(),
            guard_name.as_str().into(),
            guard_function.as_str().into(),
            capture_function.as_str().into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);
    if !valid {
        return Err(PgTrickleError::InvalidArgument(
            "partition retention CDC or source guards are incomplete; restore capture before refresh or removal".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn ensure_retention_capture(_pgt_id: i64) -> Result<(), crate::error::PgTrickleError> {
    Ok(())
}

/// Validate storage and CDC assumptions shared by configuration and cuts.
#[cfg(not(test))]
pub(crate) fn validate_retention_storage(
    st: &crate::catalog::StreamTableMeta,
    source_oid: pg_sys::Oid,
    candidate: &RetentionGroupCandidate,
    mode: RetentionMode,
    create_index: bool,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let owner = crate::api::security_context::stream_execution_context(st)?.owner_oid;
    if crate::cdc::first_rls_source_for_role(&[source_oid.to_u32()], owner)?.is_some() {
        return Err(PgTrickleError::InvalidArgument(
            "partition retention requires a stream owner that is not subject to source row-level security".into(),
        ));
    }
    let output_valid = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = $1 \
         AND attname = $2 AND attnum > 0 AND NOT attisdropped AND atttypid = $3)",
        &[
            st.pgt_relid.into(),
            candidate.output_column.clone().into(),
            pg_sys::Oid::from(candidate.source_type_oid).into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);
    if !output_valid
        || !st.storage_backend.eq_ignore_ascii_case("heap")
        || !st.st_placement.eq_ignore_ascii_case("local")
    {
        return Err(PgTrickleError::InvalidArgument(
            "retention requires local heap storage with the proven output group key".into(),
        ));
    }
    let has_identity_index = with_retention_guc_scope(|| {
        Spi::run("SET LOCAL search_path = pg_catalog, pg_temp")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_index i \
             JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid \
             JOIN pg_catalog.pg_am am ON am.oid = idx.relam \
             JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attname = '__pgt_row_id' \
             JOIN pg_catalog.pg_opclass op ON op.oid = i.indclass[0] \
             WHERE i.indrelid = $1 AND i.indisvalid AND i.indisready AND i.indpred IS NULL \
               AND am.amname = 'btree' AND op.opcdefault AND op.opcintype = a.atttypid \
               AND ((NOT $2 AND (i.indkey[0] = a.attnum OR \
                    (i.indkey[0] = 0 AND pg_catalog.pg_get_indexdef(i.indexrelid, 1, true) = \
                     'pgtrickle.row_probe_v1(__pgt_row_id)'))) OR \
                    ($2 AND i.indisunique AND i.indimmediate AND i.indnkeyatts = 1 \
                     AND i.indkey[0] = a.attnum)))",
            &[st.pgt_relid.into(), (!st.has_keyless_source && st.st_partition_key.is_none()).into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        .map(|value| value.unwrap_or(false))
    })?;
    if !has_identity_index {
        return Err(PgTrickleError::InvalidArgument(
            "partition retention requires a valid btree row-identity index on __pgt_row_id or pgtrickle.row_probe_v1(__pgt_row_id); keyed, unpartitioned streams require an immediate unique index on __pgt_row_id alone. Restore the original stream table index before retrying".into(),
        ));
    }
    if mode == RetentionMode::Retain {
        if st.st_partition_key.is_some() {
            return Err(PgTrickleError::InvalidArgument(
                "RETAIN requires unpartitioned stream storage".into(),
            ));
        }
        let durable = Spi::get_one_with_args::<bool>(
            "SELECT s.relkind = 'r' AND s.relpersistence = 'p' AND src.relpersistence = 'p' \
                    AND b.durability IN ('logged', 'sync') \
             FROM pg_catalog.pg_class s JOIN pg_catalog.pg_class src ON src.oid = $2 \
             JOIN pgtrickle.pgt_change_buffers b ON b.source_kind = 'BASE' AND b.source_id = $2::oid::bigint \
             WHERE s.oid = $1",
            &[st.pgt_relid.into(), source_oid.into()],
        ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?.unwrap_or(false);
        if !durable {
            return Err(PgTrickleError::InvalidArgument(
                "RETAIN requires logged source, stream storage, and CDC buffer".into(),
            ));
        }
        return Ok(());
    }
    let has_export = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (SELECT 1 FROM pgtrickle.pgt_output_delta_consumers WHERE pgt_id = $1 AND state <> 'DROPPED')",
        &[st.pgt_id.into()],
    ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?.unwrap_or(true);
    if has_export || crate::cdc::has_user_triggers(st.pgt_relid)? {
        return Err(PgTrickleError::InvalidArgument(
            "TRIM does not support output-delta consumers or user triggers".into(),
        ));
    }
    let has_index = || -> Result<bool, PgTrickleError> {
        Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_index i \
             JOIN pg_catalog.pg_class idx ON idx.oid = i.indexrelid \
             JOIN pg_catalog.pg_am am ON am.oid = idx.relam \
             JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attname = $2 \
             JOIN pg_catalog.pg_opclass op ON op.oid = i.indclass[0] \
             WHERE i.indrelid = $1 AND i.indisvalid AND i.indisready \
               AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indkey[0] = a.attnum \
               AND am.amname = 'btree' AND op.opcdefault AND op.opcintype = a.atttypid)",
            &[st.pgt_relid.into(), candidate.output_column.clone().into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        .map(|value| value.unwrap_or(false))
    };
    if !has_index()? && create_index {
        let table = crate::sql_builder::qualified(&st.pgt_schema, &st.pgt_name);
        let key = crate::sql_builder::ident(&candidate.output_column);
        crate::refresh::with_stream_owner(st, || {
            Spi::run(&format!("CREATE INDEX ON {table} ({key})")) // nosemgrep: rust.spi.run.dynamic-format — identifiers are quoted from catalog metadata.
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))
        })?;
    }
    if !has_index()? {
        return Err(PgTrickleError::InvalidArgument(
            "TRIM requires a btree index on the output group key".into(),
        ));
    }
    Ok(())
}

/// Drain this stream's trigger CDC under source SHARE locks before removal.
/// The caller must perform the matching DDL in the same transaction.
#[cfg(not(test))]
pub fn prepare_retention_cut(
    pgt_id: i64,
    target_oid: pgrx::pg_sys::Oid,
) -> Result<PreparedRetentionCut, crate::error::PgTrickleError> {
    use crate::catalog::{StreamTableMeta, compute_defining_query_hash};
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;

    crate::api::alter::lock_alter_target(pgt_id)?;
    let st = StreamTableMeta::get_by_id(pgt_id)?
        .ok_or_else(|| PgTrickleError::NotFound(format!("stream table {pgt_id}")))?;
    let source_oid = retention_source(&st)?;
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
    validate_retention_storage(&st, source_oid, &state.candidate, state.mode, false)?;
    if state.mode == RetentionMode::Trim {
        let table = crate::sql_builder::qualified(&st.pgt_schema, &st.pgt_name);
        crate::refresh::with_stream_owner(&st, || {
            // nosemgrep: rust.spi.run.dynamic-format — table is a quoted catalog identifier.
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
        let candidate =
            query_group_candidate(&tree, source_oid.to_u32(), &state.candidate.source_column)
                .map_err(|e| {
                    PgTrickleError::InvalidArgument(format!(
                        "retention query is no longer eligible: {e}"
                    ))
                })?;
        validate_retention_group_collations(&tree, source_oid)?;
        validate_builtin_aggregates(&st.defining_query, &tree)?;
        Ok(candidate)
    })?;
    if candidate != state.candidate {
        return Err(PgTrickleError::InvalidArgument(
            "retention group mapping changed; reconfigure before removal".into(),
        ));
    }
    ensure_no_live_sealed_overlap(source_oid, &state, None)?;
    let partition =
        resolve_retention_partition(source_oid, target_oid, &candidate, state.cutoff, None)?
            .ok_or_else(|| {
                PgTrickleError::InvalidArgument(
                    "retention target is not an aligned source partition".into(),
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
    ensure_durable_partition_tree(source_oid)?;
    ensure_retention_capture(pgt_id)?;
    let safe_bound = crate::cdc::get_current_wal_lsn()?;
    let refresh = retention_catchup(&st, source_oid, &safe_bound, &candidate)?;
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
pub(crate) fn retention_bound_sql(bound: RetentionBound) -> String {
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

/// Parse a cutoff using PostgreSQL's date and time input semantics.
#[cfg(not(test))]
pub(crate) fn parse_retention_cutoff(
    source_type_oid: u32,
    cutoff: &str,
) -> Result<RetentionBound, crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let value = match source_type_oid {
        DATE_OID => {
            let days = Spi::get_one_with_args::<i32>(
                "SELECT CASE WHEN pg_catalog.isfinite($1::date) THEN $1::date OPERATOR(pg_catalog.-) DATE '2000-01-01' END",
                &[cutoff.into()],
            ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| PgTrickleError::InvalidArgument("retention cutoff must be finite".into()))?;
            RetentionBound::Date(days)
        }
        TIMESTAMP_OID | TIMESTAMPTZ_OID => {
            let sql = if source_type_oid == TIMESTAMP_OID {
                "SELECT CASE WHEN pg_catalog.isfinite($1::timestamp) THEN \
                 (EXTRACT(EPOCH FROM ($1::timestamp OPERATOR(pg_catalog.-) TIMESTAMP '2000-01-01')) OPERATOR(pg_catalog.*) 1000000)::bigint END"
            } else {
                "SELECT CASE WHEN pg_catalog.isfinite($1::timestamptz) THEN \
                 (EXTRACT(EPOCH FROM ($1::timestamptz OPERATOR(pg_catalog.-) TIMESTAMPTZ '2000-01-01 00:00:00+00')) OPERATOR(pg_catalog.*) 1000000)::bigint END"
            };
            let micros = Spi::get_one_with_args::<i64>(sql, &[cutoff.into()])
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .ok_or_else(|| {
                    PgTrickleError::InvalidArgument("retention cutoff must be finite".into())
                })?;
            if source_type_oid == TIMESTAMP_OID {
                RetentionBound::Timestamp(micros)
            } else {
                RetentionBound::Timestamptz(micros)
            }
        }
        _ => {
            return Err(PgTrickleError::InvalidArgument(
                "unsupported retention cutoff type".into(),
            ));
        }
    };
    Ok(value)
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
            AND NOT EXISTS (WITH RECURSIVE ancestors(oid) AS ( \
                SELECT inhparent FROM pg_catalog.pg_inherits WHERE inhrelid = $2 \
                UNION ALL SELECT i.inhparent FROM pg_catalog.pg_inherits i \
                JOIN ancestors a ON i.inhrelid = a.oid \
            ) SELECT 1 FROM ancestors WHERE oid = $1)",
        &[
            pg_sys::Oid::from(cut.source_oid).into(),
            pg_sys::Oid::from(cut.target_oid).into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);
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
    with_retention_index_scans(|| {
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
        })
    })?;
    if let Some(pre) = pre {
        let schema = crate::config::pg_trickle_change_buffer_schema();
        crate::cdc::set_sync_commit_for_buffer(&format!("changes_pgt_{}", st.pgt_id))?;
        crate::cdc::validate_st_change_buffer(st.pgt_id, &schema)?;
        let buffer = crate::sql_builder::qualified(&schema, &format!("changes_pgt_{}", st.pgt_id));
        // nosemgrep: rust.spi.run.dynamic-format — buffer, columns and temporary table are quoted identifiers.
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
            AND NOT EXISTS (WITH RECURSIVE ancestors(oid) AS ( \
                SELECT inhparent FROM pg_catalog.pg_inherits WHERE inhrelid = $2 \
                UNION ALL SELECT i.inhparent FROM pg_catalog.pg_inherits i \
                JOIN ancestors a ON i.inhrelid = a.oid \
            ) SELECT 1 FROM ancestors WHERE oid = $1)",
        &[
            pg_sys::Oid::from(cut.source_oid).into(),
            pg_sys::Oid::from(cut.target_oid).into(),
        ],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);
    if !valid {
        return Err(PgTrickleError::InvalidArgument(
            "retention source lock is missing or target is still attached".into(),
        ));
    }
    let before = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT column_snapshot->'partition_retention' FROM pgtrickle.pgt_dependencies \
         WHERE pgt_id = $1 AND source_relid = $2",
        &[cut.pgt_id.into(), pg_sys::Oid::from(cut.source_oid).into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InvalidArgument(
            "retention state disappeared during partition removal".into(),
        )
    })?;
    let mut current: RetentionState = serde_json::from_value(before.0.clone())
        .map_err(|e| PgTrickleError::InvalidArgument(format!("invalid retained state: {e}")))?;
    current
        .validate()
        .map_err(|e| PgTrickleError::InvalidArgument(format!("invalid retained state: {e:?}")))?;
    let mut original = current.clone();
    original.sealed = cut.state_before_removal.sealed.clone();
    if original != cut.state_before_removal
        || !cut
            .state_before_removal
            .sealed
            .iter()
            .all(|range| current.sealed.contains(range))
    {
        return Err(PgTrickleError::InvalidArgument(
            "retention state changed during partition removal".into(),
        ));
    }
    current.seal(cut.range).map_err(|e| {
        PgTrickleError::InvalidArgument(format!("cannot seal retained range: {e:?}"))
    })?;
    let after = pgrx::JsonB(
        serde_json::to_value(current).map_err(|e| PgTrickleError::InternalError(e.to_string()))?,
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
    if state.pending.is_some() {
        return Err(PgTrickleError::InvalidArgument(
            "cannot attach a partition while a retention detach is pending".into(),
        ));
    }
    if state.mode != RetentionMode::Retain || state.sealed.is_empty() {
        return Ok(());
    }
    if !empty {
        return Err(PgTrickleError::InvalidArgument(
            "cannot attach a nonempty partition to a stream with retained history; attach an empty future partition and insert live rows normally".into(),
        ));
    }
    ensure_no_live_sealed_overlap(source_oid, &state, None).map_err(|error| {
        PgTrickleError::InvalidArgument(format!(
            "cannot attach a partition overlapping retained history or with an unprovable layout: {error}"
        ))
    })
}

/// Resolve a nested cut only through RANGE levels on the proven source key.
#[cfg(not(test))]
fn resolve_retention_partition(
    parent_oid: pg_sys::Oid,
    target_oid: pg_sys::Oid,
    candidate: &RetentionGroupCandidate,
    cutoff: RetentionBound,
    inherited: Option<SealedRange>,
) -> Result<Option<RetentionPartition>, crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let mut parent_candidate = candidate.clone();
    parent_candidate.source_oid = parent_oid.to_u32();
    let layout = validate_partition_layout_trusted(parent_oid, &parent_candidate, cutoff)?;
    for mut partition in layout {
        if let Some(range) = inherited {
            if bound_value(partition.lower) < bound_value(range.lower) {
                partition.lower = range.lower;
            }
            if bound_value(partition.upper) > bound_value(range.upper) {
                partition.upper = range.upper;
            }
        }
        if bound_value(partition.lower) >= bound_value(partition.upper) {
            continue;
        }
        partition.expired = range_is_expired(candidate, partition.lower, partition.upper, cutoff)
            .map_err(|e| {
            PgTrickleError::InvalidArgument(format!("invalid nested partition range: {e:?}"))
        })?;
        if partition.oid == target_oid.to_u32() {
            return Ok(Some(partition));
        }
        let child_oid = pg_sys::Oid::from(partition.oid);
        let contains_target = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_partition_ancestors($2::regclass) a(relid) \
             WHERE a.relid = $1::regclass)",
            &[child_oid.into(), target_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);
        if contains_target {
            return resolve_retention_partition(
                child_oid,
                target_oid,
                candidate,
                cutoff,
                Some(SealedRange {
                    lower: partition.lower,
                    upper: partition.upper,
                }),
            );
        }
    }
    Ok(None)
}

/// Prove that no still-attached leaf can contribute to a sealed range.
#[cfg(not(test))]
pub(crate) fn ensure_no_live_sealed_overlap(
    parent_oid: pg_sys::Oid,
    state: &RetentionState,
    inherited: Option<SealedRange>,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    if state.sealed.is_empty() {
        return Ok(());
    }
    let mut candidate = state.candidate.clone();
    candidate.source_oid = parent_oid.to_u32();
    let layout = validate_partition_layout_trusted(parent_oid, &candidate, state.cutoff)?;
    for mut partition in layout {
        if let Some(range) = inherited {
            if bound_value(partition.lower) < bound_value(range.lower) {
                partition.lower = range.lower;
            }
            if bound_value(partition.upper) > bound_value(range.upper) {
                partition.upper = range.upper;
            }
        }
        if bound_value(partition.lower) >= bound_value(partition.upper)
            || !state.sealed.iter().any(|range| {
                bound_value(partition.lower) < bound_value(range.upper)
                    && bound_value(range.lower) < bound_value(partition.upper)
            })
        {
            continue;
        }
        let child_oid = pg_sys::Oid::from(partition.oid);
        let partitioned = Spi::get_one_with_args::<bool>(
            "SELECT relkind = 'p' FROM pg_catalog.pg_class WHERE oid = $1",
            &[child_oid.into()],
        )
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .unwrap_or(false);
        if !partitioned {
            return Err(PgTrickleError::InvalidArgument(
                "a live partition overlaps retained history".into(),
            ));
        }
        ensure_no_live_sealed_overlap(
            child_oid,
            state,
            Some(SealedRange {
                lower: partition.lower,
                upper: partition.upper,
            }),
        )?;
    }
    Ok(())
}

/// Restore caller settings after a retention operation.
#[cfg(not(test))]
fn with_retention_guc_scope<T>(
    operation: impl FnOnce() -> Result<T, crate::error::PgTrickleError>,
) -> Result<T, crate::error::PgTrickleError> {
    // SAFETY: The GUC nest is restored even if PostgreSQL reports an error.
    unsafe {
        let level = pg_sys::NewGUCNestLevel();
        pgrx::PgTryBuilder::new(std::panic::AssertUnwindSafe(operation))
            .finally(move || pg_sys::AtEOXact_GUC(false, level))
            .execute()
    }
}

#[cfg(not(test))]
fn with_retention_index_scans<T>(
    operation: impl FnOnce() -> Result<T, crate::error::PgTrickleError>,
) -> Result<T, crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    with_retention_guc_scope(|| {
        Spi::run("SET LOCAL enable_partition_pruning = on")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL enable_indexscan = on")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL enable_bitmapscan = on")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL enable_tidscan = on")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL enable_seqscan = off")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL enable_nestloop = on")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL enable_hashjoin = off")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL enable_mergejoin = off")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        // shortcut: nested-loop parameterized lookups bound retention DML to affected keys.
        Spi::run("SET LOCAL random_page_cost = 1")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL cpu_index_tuple_cost = 0.005")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL cpu_operator_cost = 0.0025")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        Spi::run("SET LOCAL jit = off").map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        operation()
    })
}

#[cfg(not(test))]
/// Validate under a trusted catalog path without changing the caller's settings.
pub(crate) fn validate_partition_layout_trusted(
    source_oid: pg_sys::Oid,
    candidate: &RetentionGroupCandidate,
    cutoff: RetentionBound,
) -> Result<Vec<RetentionPartition>, crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    with_retention_guc_scope(|| {
        Spi::run("SET LOCAL search_path = pg_catalog, pg_temp")
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
        validate_partition_layout(source_oid, &candidate.source_column, candidate, cutoff)
    })
}

#[cfg(not(test))]
fn ensure_durable_partition_tree(
    source_oid: pg_sys::Oid,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let durable = Spi::get_one_with_args::<bool>(
        "WITH RECURSIVE descendants(relid) AS (\
             SELECT $1::oid UNION ALL \
             SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN descendants d ON i.inhparent = d.relid\
         ) SELECT NOT EXISTS (SELECT 1 FROM descendants d JOIN pg_catalog.pg_class c ON c.oid = d.relid \
             WHERE c.relkind NOT IN ('r', 'p') OR c.relpersistence <> 'p')",
        &[source_oid.into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .unwrap_or(false);
    if !durable {
        return Err(PgTrickleError::InvalidArgument(
            "retention requires permanent local tables throughout the partition tree".into(),
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
    // nosemgrep: rust.spi.run.dynamic-format — name is quoted by PostgreSQL format.
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
    ensure_durable_partition_tree(source_oid)?;
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

/// Prove aggregates resolve to PostgreSQL built-ins under a safe function path.
#[cfg(not(test))]
pub(crate) fn validate_builtin_aggregates(
    query: &str,
    tree: &OpTree,
) -> Result<(), crate::error::PgTrickleError> {
    use crate::error::PgTrickleError;
    use pgrx::prelude::*;
    let catalog_position = Spi::get_one::<i32>(
        "SELECT pg_catalog.array_position(pg_catalog.current_schemas(false), 'pg_catalog'::pg_catalog.name)",
    ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
    if catalog_position.is_some_and(|position| position != 1) {
        return Err(PgTrickleError::InvalidArgument(
            "partition retention requires pg_catalog before other schemas in the defining search path".into(),
        ));
    }
    let aggregate = match tree {
        OpTree::Project { child, .. } => child.as_ref(),
        other => other,
    };
    let OpTree::Aggregate { aggregates, .. } = aggregate else {
        return Err(PgTrickleError::InvalidArgument(
            "partition retention requires an aggregate query".into(),
        ));
    };
    let analyzed = super::parser::collect_analyzed_aggregate_locations(query)?;
    if analyzed.len() != aggregates.len() {
        return Err(PgTrickleError::InvalidArgument(
            "partition retention could not prove aggregate function identities".into(),
        ));
    }
    for oid in analyzed.values() {
        let builtin = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace \
             WHERE p.oid = $1 AND n.nspname = 'pg_catalog' AND p.prokind = 'a')",
            &[(*oid).into()],
        ).map_err(|e| PgTrickleError::SpiError(e.to_string()))?.unwrap_or(false);
        if !builtin {
            return Err(PgTrickleError::InvalidArgument(
                "partition retention requires PostgreSQL built-in aggregate identities"
                    .into(),
            ));
        }
    }
    Ok(())
}

// Floating-point addition changes with batching, so a differential SUM is not exact.
fn has_floating_sum(tree: &OpTree) -> bool {
    let aggregate = match tree {
        OpTree::Project { child, .. } => child.as_ref(),
        tree => tree,
    };
    let OpTree::Aggregate {
        aggregates, child, ..
    } = aggregate
    else {
        return false;
    };
    let OpTree::Scan { columns, .. } = child.as_ref() else {
        return false;
    };
    aggregates.iter().any(|aggregate| {
        aggregate.function == AggFunc::Sum
            && matches!(&aggregate.argument, Some(Expr::ColumnRef { column_name, .. })
                if columns.iter().any(|column| column.name == *column_name && matches!(column.type_oid, 700 | 701)))
    })
}

/// Name an operator that cannot be part of the narrow retention query shape.
fn retention_operator_name(tree: &OpTree) -> &'static str {
    match tree {
        OpTree::Scan { .. } => "table scan",
        OpTree::Project { .. } => "projection",
        OpTree::Aggregate { .. } => "grouped aggregate",
        OpTree::Filter { .. } => "WHERE or HAVING filter",
        OpTree::InnerJoin { .. }
        | OpTree::LeftJoin { .. }
        | OpTree::FullJoin { .. }
        | OpTree::SemiJoin { .. }
        | OpTree::AntiJoin { .. } => "join",
        OpTree::Distinct { .. } => "DISTINCT",
        OpTree::UnionAll { .. } | OpTree::Intersect { .. } | OpTree::Except { .. } => {
            "set operation"
        }
        OpTree::Subquery { .. }
        | OpTree::CteScan { .. }
        | OpTree::RecursiveCte { .. }
        | OpTree::RecursiveSelfRef { .. } => "subquery or CTE",
        OpTree::Window { .. } => "window function",
        OpTree::LateralFunction { .. } | OpTree::LateralSubquery { .. } => "LATERAL",
        OpTree::ScalarSubquery { .. } => "scalar subquery",
        OpTree::ConstantSelect { .. } => "constant SELECT",
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
        } => {
            if expressions.len() != aliases.len() {
                return Err(RetentionQueryError::UnsupportedShape(
                    "SELECT list has mismatched expressions and output names; retention requires plain projected columns".into(),
                ));
            }
            (Some((expressions, aliases)), child.as_ref())
        }
        OpTree::Aggregate { .. } => (None, tree),
        _ => {
            return Err(RetentionQueryError::UnsupportedShape(format!(
                "query contains a top-level {}; retention requires a single-source grouped aggregate without other query operators",
                retention_operator_name(tree)
            )));
        }
    };
    let OpTree::Aggregate {
        group_by,
        aggregates,
        child,
    } = aggregate
    else {
        return Err(RetentionQueryError::UnsupportedShape(format!(
            "SELECT input is a {}, not a grouped aggregate; retention requires GROUP BY over one source table",
            retention_operator_name(aggregate)
        )));
    };
    let OpTree::Scan {
        table_oid,
        columns,
        alias,
        ..
    } = child.as_ref()
    else {
        return Err(RetentionQueryError::UnsupportedShape(format!(
            "aggregate input contains a {}; retention requires a direct scan of a single source table",
            retention_operator_name(child)
        )));
    };
    if has_floating_sum(tree) {
        return Err(RetentionQueryError::FloatingPointSum);
    }
    let column = columns
        .iter()
        .find(|column| column.name == key && !column.is_nullable)
        .filter(|column| matches!(column.type_oid, DATE_OID | TIMESTAMP_OID | TIMESTAMPTZ_OID))
        .filter(|_| *table_oid == source_oid)
        .ok_or_else(|| {
            RetentionQueryError::InvalidSourceKey(format!(
                "source column \"{key}\" must belong to the single source table and be a non-null date, timestamp, or timestamptz partition key"
            ))
        })?;

    // Group locality follows from ONE projected group key that identifies the
    // source partition. Additional group expressions and SELECT projections
    // cannot merge groups across partitions. The normal DIFFERENTIAL validator
    // checks expression volatility, type compatibility and row identities.
    // Keep the partition-identifying projection itself strict below: deleting
    // by an arbitrary projection would not be a valid range predicate.

    if aggregates.is_empty() {
        return Err(RetentionQueryError::UnsupportedAggregate(
            "query has no aggregates; retention requires a grouped aggregate".into(),
        ));
    }
    for group in group_by {
        let Some(candidate) = bucket(group, alias, key, column.type_oid) else {
            continue;
        };
        let group_output = group.output_name();
        if candidate != RetentionBucket::Exact
            && columns.iter().any(|column| column.name == group_output)
        {
            return Err(RetentionQueryError::UnsupportedShape(format!(
                "hourly grouping key \"{}\" has the same output name \"{group_output}\" as a source column; retention group output names must be unambiguous",
                group.to_sql()
            )));
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
            return Err(RetentionQueryError::UnsupportedShape(format!(
                "grouping key \"{}\" has a duplicate output name \"{group_output}\"; retention group output names must be unique",
                group.to_sql()
            )));
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
                return Err(RetentionQueryError::UnsupportedShape(format!(
                    "projected partition key \"{group_output}\" has an ambiguous output name \"{output}\"; retention requires one uniquely named key column"
                )));
            }
            output.clone()
        } else {
            group_output
        };
        if output.is_empty() {
            return Err(RetentionQueryError::UnsupportedShape(
                "projected partition key has an empty output name; retention requires a named key column".into(),
            ));
        }
        return Ok(RetentionGroupCandidate {
            output_column: output,
            bucket: candidate,
            source_type_oid: column.type_oid,
            source_oid,
            source_column: key.to_string(),
        });
    }
    Err(RetentionQueryError::MissingProjectedGroupKey(format!(
        "the GROUP BY and select list must project the partition key \"{key}\" or an approved hourly bucket of that same key as a plain column"
    )))
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
            Err(error) => {
                let kind = match error {
                    RetentionQueryError::UnsupportedShape(_) => "UnsupportedShape",
                    RetentionQueryError::UnsupportedAggregate(_) => "UnsupportedAggregate",
                    RetentionQueryError::FloatingPointSum => "FloatingPointSum",
                    RetentionQueryError::InvalidSourceKey(_) => "InvalidSourceKey",
                    RetentionQueryError::MissingProjectedGroupKey(_) => "MissingProjectedGroupKey",
                };
                format!("Err({kind})")
            }
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
    fn output_pruning_requires_the_proven_single_group_key() {
        assert_eq!(
            retention_pruning_key(Some("HASH:bucket:16"), "bucket"),
            Some("bucket".into())
        );
        assert_eq!(
            retention_pruning_key(Some("LIST:bucket"), "bucket"),
            Some("bucket".into())
        );
        assert_eq!(
            retention_pruning_key(Some("bucket"), "bucket"),
            Some("bucket".into())
        );
        assert_eq!(retention_pruning_key(Some("HASH:account"), "bucket"), None);
        assert_eq!(
            retention_pruning_key(Some("bucket,account"), "bucket"),
            None
        );
        assert_eq!(retention_pruning_key(None, "bucket"), None);
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
        assert!(matches!(
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
            Err(RetentionQueryError::MissingProjectedGroupKey(_))
        ));
        let unqualified = Expr::FuncCall {
            func_name: "date_trunc".into(),
            args: vec![Expr::Literal("'hour'".into()), col("occurred")],
        };
        assert!(matches!(
            query_group_candidate(
                &query(unqualified.clone(), unqualified, TIMESTAMP_OID, false),
                42,
                "occurred"
            ),
            Err(RetentionQueryError::MissingProjectedGroupKey(_))
        ));
        assert!(matches!(
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
            Err(RetentionQueryError::MissingProjectedGroupKey(_))
        ));
        assert!(matches!(
            query_group_candidate(
                &query(col("occurred"), col("occurred"), DATE_OID, true),
                42,
                "occurred"
            ),
            Err(RetentionQueryError::InvalidSourceKey(_))
        ));
        assert!(matches!(
            query_group_candidate(
                &query(col("occurred"), col("occurred"), DATE_OID, false),
                43,
                "occurred"
            ),
            Err(RetentionQueryError::InvalidSourceKey(_))
        ));
        assert!(matches!(
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
            Err(RetentionQueryError::MissingProjectedGroupKey(_))
        ));
        assert!(matches!(
            query_group_candidate(
                &query(col("occurred"), col("other"), DATE_OID, false),
                42,
                "occurred"
            ),
            Err(RetentionQueryError::MissingProjectedGroupKey(_))
        ));
    }

    #[test]
    fn floating_sum_is_rejected_but_count_and_exact_sum_are_supported() {
        for (type_oid, function, expected) in [
            (
                700,
                AggFunc::Sum,
                Err(RetentionQueryError::FloatingPointSum),
            ),
            (
                701,
                AggFunc::Sum,
                Err(RetentionQueryError::FloatingPointSum),
            ),
            (701, AggFunc::Count, Ok(())),
            (1700, AggFunc::Sum, Ok(())),
            (23, AggFunc::Sum, Ok(())),
        ] {
            let mut tree = query(col("occurred"), col("occurred"), DATE_OID, false);
            let OpTree::Project { child, .. } = &mut tree else {
                unreachable!()
            };
            let OpTree::Aggregate {
                aggregates, child, ..
            } = child.as_mut()
            else {
                unreachable!()
            };
            let OpTree::Scan { columns, .. } = child.as_mut() else {
                unreachable!()
            };
            columns.push(Column {
                name: "value".into(),
                type_oid,
                is_nullable: true,
            });
            aggregates[0].function = function;
            aggregates[0].argument = Some(col("value"));
            assert_eq!(
                query_group_candidate(&tree, 42, "occurred").map(|_| ()),
                expected
            );
        }
    }

    #[test]
    fn aggregate_choice_does_not_change_locality_but_other_operators_do() {
        let mut tree = query(col("occurred"), col("occurred"), DATE_OID, false);
        let OpTree::Project { child, .. } = &mut tree else {
            unreachable!()
        };
        let OpTree::Aggregate { aggregates, .. } = child.as_mut() else {
            unreachable!()
        };
        // The aggregate's evaluation remains the normal DVM's responsibility.
        aggregates[0].function = AggFunc::Max;
        aggregates[0].argument = Some(col("occurred"));
        assert!(query_group_candidate(&tree, 42, "occurred").is_ok());

        // A top-level operator can modify groups outside the removed partition,
        // so this proof deliberately does not admit arbitrary operator trees.
        let filtered_result = OpTree::Filter {
            predicate: Expr::Literal("true".into()),
            child: Box::new(tree),
        };
        assert!(matches!(
            query_group_candidate(&filtered_result, 42, "occurred"),
            Err(RetentionQueryError::UnsupportedShape(_))
        ));

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
        assert!(matches!(
            query_group_candidate(&tree, 42, "occurred"),
            Err(RetentionQueryError::UnsupportedShape(_))
        ));
    }

    #[test]
    fn locality_ignores_other_group_expressions_and_aggregate_arguments() {
        let mut tree = query(col("occurred"), col("occurred"), DATE_OID, false);
        let OpTree::Project { child, .. } = &mut tree else {
            unreachable!()
        };
        let OpTree::Aggregate {
            group_by,
            aggregates,
            ..
        } = child.as_mut() else {
            unreachable!()
        };
        group_by.push(hour("other", None));
        aggregates[0].function = AggFunc::Avg;
        aggregates[0].argument = Some(col("value"));
        let candidate = query_group_candidate(&tree, 42, "occurred")
            .expect("a projected partition key makes other groups partition-local");
        assert_eq!(candidate.output_column, "bucket");

        // Absence of the partition-determining key is still not proof of locality.
        let only_other_group = query(
            hour("other", None),
            hour("other", None),
            DATE_OID,
            false,
        );
        assert!(matches!(
            query_group_candidate(&only_other_group, 42, "occurred"),
            Err(RetentionQueryError::MissingProjectedGroupKey(_))
        ));
    }

    #[test]
    fn accepts_constant_projection_with_an_unmodified_partition_key() {
        let mut tree = query(col("occurred"), col("occurred"), DATE_OID, false);
        let OpTree::Project {
            expressions,
            aliases,
            ..
        } = &mut tree else {
            unreachable!()
        };
        expressions.push(Expr::Literal("'x'::text".into()));
        aliases.push("tag".into());
        assert!(query_group_candidate(&tree, 42, "occurred").is_ok());

        // The WHERE predicate in the range DELETE must be on a projected
        // partition identity, not a computed replacement.
        expressions[0] = Expr::Literal("'2025-01-01'::date".into());
        assert!(matches!(
            query_group_candidate(&tree, 42, "occurred"),
            Err(RetentionQueryError::MissingProjectedGroupKey(_))
        ));
    }

    #[test]
    fn eligibility_errors_explain_missing_or_invalid_partition_key() {
        let message = query_group_candidate(
            &query(col("other"), col("occurred"), DATE_OID, false),
            42,
            "occurred",
        )
        .unwrap_err()
        .to_string();
        assert!(message.contains("select list"), "{message}");
        assert!(message.contains("partition key \"occurred\""), "{message}");

        let message = query_group_candidate(
            &query(col("occurred"), col("occurred"), DATE_OID, true),
            42,
            "occurred",
        )
        .unwrap_err()
        .to_string();
        assert!(message.contains("source column \"occurred\""), "{message}");
        assert!(message.contains("non-null"), "{message}");

        let tree = OpTree::Filter {
            predicate: Expr::Literal("true".into()),
            child: Box::new(query(col("occurred"), col("occurred"), DATE_OID, false)),
        };
        let message = query_group_candidate(&tree, 42, "occurred")
            .unwrap_err()
            .to_string();
        assert!(message.contains("WHERE or HAVING filter"), "{message}");
        assert!(
            message.contains("single-source grouped aggregate"),
            "{message}"
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
            pending: None,
        }
    }

    #[test]
    fn pending_detach_is_durable_and_blocks_rebuilds() {
        use RetentionBound::Date;
        let mut state = sample_state(RetentionMode::Trim);
        assert_eq!(serde_json::to_value(&state).unwrap().get("pending"), None);
        state.pending = Some(PendingRetentionDetach {
            parent_oid: 42,
            target_oid: 43,
            range: SealedRange {
                lower: Date(10),
                upper: Date(20),
            },
        });
        let encoded = serde_json::to_value(&state).unwrap();
        assert_eq!(
            serde_json::from_value::<RetentionState>(encoded.clone()).unwrap(),
            state
        );
        assert_eq!(state.validate(), Ok(()));
        assert_eq!(state_has_sealed_history(&encoded), Ok(true));
        state.pending.as_mut().unwrap().range.upper = Date(31);
        assert_eq!(state.validate(), Err(RetentionStateError::NotExpired));
        state.pending.as_mut().unwrap().range.upper = Date(20);
        state.pending.as_mut().unwrap().target_oid = 42;
        assert_eq!(state.validate(), Err(RetentionStateError::InvalidState));
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
