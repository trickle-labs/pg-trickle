//! Bounded differential refresh pipeline.
//!
//! The portal and its temporary batch relation stay inside the caller's outer
//! transaction. Each batch is an apply unit; frontier and refresh history are
//! still finalized once by the normal refresh finalizer.

use pgrx::prelude::*;

use crate::error::PgTrickleError;

const FETCH_CHUNK: std::os::raw::c_long = 256;

/// Stable reason for choosing the direct or portal path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineReason {
    SmallNonAmplifying,
    LargeInput,
    PotentialAmplification,
    CompatibilityFallback,
}

impl PipelineReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SmallNonAmplifying => "small_non_amplifying",
            Self::LargeInput => "large_input",
            Self::PotentialAmplification => "potential_amplification",
            Self::CompatibilityFallback => "compatibility_fallback",
        }
    }
}

/// The one pure admission decision for ordinary differential refreshes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineMode {
    Direct,
    Pipelined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineDecision {
    pub mode: PipelineMode,
    pub reason: PipelineReason,
}

pub const fn decide_pipeline(
    pending_rows: i64,
    batch_size: usize,
    proven_non_amplifying: bool,
    compatibility_exclusion: bool,
) -> PipelineDecision {
    if compatibility_exclusion {
        return PipelineDecision {
            mode: PipelineMode::Direct,
            reason: PipelineReason::CompatibilityFallback,
        };
    }
    if pending_rows >= 0 && (pending_rows as usize) <= batch_size && proven_non_amplifying {
        PipelineDecision {
            mode: PipelineMode::Direct,
            reason: PipelineReason::SmallNonAmplifying,
        }
    } else if !proven_non_amplifying {
        PipelineDecision {
            mode: PipelineMode::Pipelined,
            reason: PipelineReason::PotentialAmplification,
        }
    } else {
        PipelineDecision {
            mode: PipelineMode::Pipelined,
            reason: PipelineReason::LargeInput,
        }
    }
}

/// Counters returned by one pipelined apply.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PipelineStats {
    pub batches_completed: u64,
    pub rows_staged: u64,
    pub bytes_staged: u64,
    pub largest_batch_rows: u64,
    pub largest_batch_bytes: u64,
    pub oversize_batches: u64,
}

impl PipelineStats {
    fn add_batch(
        &mut self,
        rows: usize,
        bytes: u64,
        byte_limit: u64,
    ) -> Result<(), PgTrickleError> {
        self.batches_completed = self.batches_completed.checked_add(1).ok_or_else(|| {
            PgTrickleError::InternalError("pipeline batch counter overflow".to_string())
        })?;
        self.rows_staged = self.rows_staged.checked_add(rows as u64).ok_or_else(|| {
            PgTrickleError::InternalError("pipeline row counter overflow".to_string())
        })?;
        self.bytes_staged = self.bytes_staged.checked_add(bytes).ok_or_else(|| {
            PgTrickleError::InternalError("pipeline byte counter overflow".to_string())
        })?;
        self.largest_batch_rows = self.largest_batch_rows.max(rows as u64);
        self.largest_batch_bytes = self.largest_batch_bytes.max(bytes);
        if bytes > byte_limit {
            self.oversize_batches = self.oversize_batches.checked_add(1).ok_or_else(|| {
                PgTrickleError::InternalError("pipeline oversize counter overflow".to_string())
            })?;
        }
        Ok(())
    }
}

fn relation_name(pgt_id: i64, backend_pid: i32) -> String {
    format!("__pgt_pipeline_{pgt_id}_{backend_pid}")
}

fn replace_merge_source(merge_sql: &str, relation: &str) -> Result<String, PgTrickleError> {
    let using_start = merge_sql.find("USING ").ok_or_else(|| {
        PgTrickleError::InternalError("pipeline MERGE has no USING clause".to_string())
    })? + "USING ".len();
    let source_end = merge_sql[using_start..]
        .find(" AS d ON ")
        .map(|offset| using_start + offset)
        .ok_or_else(|| {
            PgTrickleError::InternalError("pipeline MERGE has no delta alias".to_string())
        })?;
    Ok(format!(
        "{}{}{}",
        &merge_sql[..using_start],
        relation,
        &merge_sql[source_end..]
    ))
}

#[cfg(not(test))]
fn copy_table_rows(
    table: pgrx::spi::SpiTupleTable<'_>,
    relation_oid: pg_sys::Oid,
) -> Result<usize, PgTrickleError> {
    let relation = unsafe {
        // SAFETY: relation_oid came from to_regclass for the temp relation in
        // this backend and transaction; RowExclusiveLock is valid for inserts.
        pg_sys::table_open(relation_oid, pg_sys::RowExclusiveLock as _)
    };
    if relation.is_null() {
        return Err(PgTrickleError::InternalError(
            "pipeline batch relation could not be opened".to_string(),
        ));
    }

    let tuple_desc = unsafe {
        // SAFETY: table_open returned a live relation owned by this backend.
        (*relation).rd_att
    };
    let expected_columns = unsafe {
        // SAFETY: rd_att is the relation's live tuple descriptor.
        (*tuple_desc).natts as usize
    };
    let mut copied = 0usize;
    for tuple in table {
        if tuple.columns() != expected_columns {
            unsafe {
                // SAFETY: relation was successfully opened above.
                pg_sys::table_close(relation, pg_sys::RowExclusiveLock as _);
            }
            return Err(PgTrickleError::InternalError(
                "pipeline tuple descriptor column count mismatch".to_string(),
            ));
        }
        let mut values = Vec::with_capacity(expected_columns);
        let mut nulls = Vec::with_capacity(expected_columns);
        for ordinal in 1..=expected_columns {
            let entry = tuple
                .get_datum_by_ordinal(ordinal)
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            let datum = entry
                .value::<pg_sys::Datum>()
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?;
            nulls.push(datum.is_none());
            values.push(datum.unwrap_or_else(|| pg_sys::Datum::from(0)));
            let target_oid = unsafe {
                // SAFETY: ordinal is within the validated target descriptor.
                (*pg_sys::TupleDescAttr(tuple_desc, (ordinal - 1) as _)).atttypid
            };
            if target_oid != entry.oid() {
                unsafe {
                    // SAFETY: relation was successfully opened above.
                    pg_sys::table_close(relation, pg_sys::RowExclusiveLock as _);
                }
                return Err(PgTrickleError::InternalError(format!(
                    "pipeline tuple descriptor type mismatch at column {ordinal}"
                )));
            }
        }
        let heap_tuple = unsafe {
            // SAFETY: values/nulls have exactly natts entries matching the
            // type-identical temporary relation descriptor.
            pg_sys::heap_form_tuple(tuple_desc, values.as_ptr(), nulls.as_ptr())
        };
        if heap_tuple.is_null() {
            unsafe {
                // SAFETY: relation was successfully opened above.
                pg_sys::table_close(relation, pg_sys::RowExclusiveLock as _);
            }
            return Err(PgTrickleError::InternalError(
                "pipeline tuple formation failed".to_string(),
            ));
        }
        unsafe {
            // SAFETY: heap_tuple matches relation's descriptor and is inserted
            // in the current transaction with the current command id.
            let slot = pg_sys::MakeSingleTupleTableSlot(tuple_desc, std::ptr::null());
            if slot.is_null() {
                pg_sys::heap_freetuple(heap_tuple);
                pg_sys::table_close(relation, pg_sys::RowExclusiveLock as _);
                return Err(PgTrickleError::InternalError(
                    "pipeline tuple slot allocation failed".to_string(),
                ));
            }
            pg_sys::ExecStoreHeapTuple(heap_tuple, slot, true);
            pg_sys::table_tuple_insert(
                relation,
                slot,
                pg_sys::GetCurrentCommandId(true),
                0,
                std::ptr::null_mut(),
            );
            pg_sys::ExecDropSingleTupleTableSlot(slot);
        }
        copied += 1;
    }
    unsafe {
        // SAFETY: relation was successfully opened above.
        pg_sys::CommandCounterIncrement();
        pg_sys::table_close(relation, pg_sys::RowExclusiveLock as _);
    }
    Ok(copied)
}

#[cfg(test)]
fn copy_table_rows(
    _table: pgrx::spi::SpiTupleTable<'_>,
    _relation_oid: pg_sys::Oid,
) -> Result<usize, PgTrickleError> {
    Err(PgTrickleError::InternalError(
        "pipeline row copying requires a PostgreSQL backend".to_string(),
    ))
}

/// Execute an ordinary MERGE through a detached SPI cursor and bounded temp batch relation.
pub fn execute_merge_pipeline(
    pgt_id: i64,
    delta_sql: &str,
    merge_sql: &str,
    batch_size: usize,
    byte_limit: u64,
) -> Result<(usize, PipelineStats), PgTrickleError> {
    let backend_pid = Spi::get_one::<i32>("SELECT pg_backend_pid()")
        .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
        .ok_or_else(|| PgTrickleError::InternalError("backend PID is NULL".to_string()))?;
    let relation = relation_name(pgt_id, backend_pid);
    let quoted_relation = format!("\"{relation}\"");
    Spi::run(&format!(
        "DROP TABLE IF EXISTS {quoted_relation}; CREATE TEMP TABLE {quoted_relation} ON COMMIT DROP AS SELECT * FROM ({delta_sql}) __pgt_pipeline_empty LIMIT 0"
    ))
    .map_err(|e| PgTrickleError::SpiError(format!("pipeline batch relation: {e}")))?;
    let relation_oid = Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT $1::regclass::oid",
        &[relation.as_str().into()],
    )
    .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
    .ok_or_else(|| {
        PgTrickleError::InternalError("pipeline batch relation OID is NULL".to_string())
    })?;

    let cursor_name = Spi::connect_mut(|client| {
        let cursor = client
            .try_open_cursor(delta_sql, &[])
            .map_err(|e| PgTrickleError::SpiError(format!("pipeline cursor open: {e}")))?;
        Ok::<String, PgTrickleError>(cursor.detach_into_name())
    })?;

    let mut stats = PipelineStats::default();
    let mut applied = 0usize;
    let result = (|| {
        loop {
            let fetched = Spi::connect_mut(|client| {
                let mut cursor = client
                    .find_cursor(&cursor_name)
                    .map_err(|e| PgTrickleError::SpiError(format!("pipeline cursor find: {e}")))?;
                let table = cursor
                    .fetch(FETCH_CHUNK.min(batch_size.max(1) as _))
                    .map_err(|e| PgTrickleError::SpiError(format!("pipeline cursor fetch: {e}")))?;
                if table.is_empty() {
                    return Ok::<usize, PgTrickleError>(0);
                }
                copy_table_rows(table, relation_oid)
            })?;
            if fetched == 0 {
                break;
            }
            let staged_bytes = Spi::get_one_with_args::<i64>(
                "SELECT pg_relation_size($1::oid)",
                &[relation_oid.into()],
            )
            .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
            .unwrap_or(0)
            .max(0) as u64;
            stats.add_batch(fetched, staged_bytes, byte_limit)?;
            let batch_sql = replace_merge_source(merge_sql, &quoted_relation)?;
            let changed = Spi::connect_mut(|client| {
                let result = client
                    .update(&batch_sql, None, &[])
                    .map_err(|e| PgTrickleError::SpiError(format!("pipeline batch apply: {e}")))?;
                Ok::<usize, PgTrickleError>(result.len())
            })?;
            applied = applied.checked_add(changed).ok_or_else(|| {
                PgTrickleError::InternalError("pipeline apply counter overflow".to_string())
            })?;
            Spi::run(&format!("TRUNCATE TABLE {quoted_relation}")) // nosemgrep: rust.spi.run.dynamic-format — quoted_relation is a backend-generated temporary identifier
                .map_err(|e| PgTrickleError::SpiError(format!("pipeline batch cleanup: {e}")))?;
        }
        Ok::<(), PgTrickleError>(())
    })();
    let _ = Spi::connect_mut(|client| client.find_cursor(&cursor_name).map(drop));
    result.map(|()| (applied, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decision_keeps_small_proven_delta_direct() {
        let decision = decide_pipeline(2, 4, true, false);
        assert_eq!(decision.mode, PipelineMode::Direct);
        assert_eq!(decision.reason.as_str(), "small_non_amplifying");
    }

    #[test]
    fn decision_bounds_large_and_unknown_shapes() {
        assert_eq!(
            decide_pipeline(5, 4, true, false).reason,
            PipelineReason::LargeInput
        );
        assert_eq!(
            decide_pipeline(1, 4, false, false).reason,
            PipelineReason::PotentialAmplification
        );
        assert_eq!(
            decide_pipeline(100, 4, true, true).reason,
            PipelineReason::CompatibilityFallback
        );
    }

    #[test]
    fn stats_accumulate_and_mark_oversize_batches() {
        let mut stats = PipelineStats::default();
        stats.add_batch(2, 10, 8).expect("counter fits");
        assert_eq!(stats.rows_staged, 2);
        assert_eq!(stats.oversize_batches, 1);
    }

    #[test]
    fn merge_source_replacement_keeps_the_merge_shell() {
        let sql = "MERGE INTO st AS st USING (SELECT * FROM delta) AS d ON st.id = d.id";
        assert_eq!(
            replace_merge_source(sql, "\"__pgt_pipeline_1_2\"").expect("valid merge"),
            "MERGE INTO st AS st USING \"__pgt_pipeline_1_2\" AS d ON st.id = d.id"
        );
    }
}
